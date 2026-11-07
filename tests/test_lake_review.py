"""Regression coverage for cache retention, exact module identity and assumptions."""
import json
import os
import time
from pathlib import Path

import pytest

from formal_geyser.lake_build import _recover, _workspace
from formal_geyser.lake_cache import collect, lock_file
from formal_geyser.registry import RegistryError, library_manifest
from formal_geyser.schemas import EnvironmentRecord, LibraryRoot
from formal_geyser.tooling import LeanTooling
from tests.conftest import FakeRunner
from tests.test_lake_build import real_tooling as real_tooling
from tests.test_lake_build import source


async def test_prebuilt_module_collision_in_common_build_boundary(proof_case, tmp_path):
    hub, _, _ = proof_case
    library = tmp_path / "registered"
    source(library, "Mathlib/Existing.olean", "prebuilt")
    env = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    env.library_roots = [LibraryRoot(path=str(library), manifest_sha256=hub.blobs.put_json(library_manifest(library)))]
    runner = FakeRunner()
    tooling = LeanTooling(env, hub.blobs, runner, isolate_library_checks=False)
    directory = tmp_path / "input"
    source(directory, "Mathlib/Existing.lean", "def replaced := 1")
    with pytest.raises(RegistryError, match="shadows a registered module"):
        await tooling.compile(directory, ["Mathlib.Existing"])
    assert not runner.calls
    core = Path(env.lean_executable).parent.parent / "lib/lean"
    source(core, "Lake/Existing.olean", "prebuilt toolchain module")
    with pytest.raises(RegistryError, match="shadows"):
        tooling.check_module_paths(["Lake/Existing.lean"], [])
    (directory / "Mathlib/Existing.lean").unlink()
    source(directory, "Mathlib/Extension.lean", "def fresh := 1")
    assert (await tooling.compile(directory, ["Mathlib.Extension"]))["ok"]
    with pytest.raises(RegistryError, match="shadows"):
        tooling.check_module_paths(["mathlib/existing.lean"], [])


@pytest.mark.parametrize("axiom,origin,allowed,local", [
    ("sorryAx", "Init.Core", False, False),
    ("result._native.native_decide.ax_1", "Candidate", False, True),
    ("Invented", "Candidate", True, True),
    ("Classical.choice", "Init.Classical", True, False),
])
async def test_target_assumptions(proof_case, tmp_path, axiom, origin, allowed, local):
    hub, _, _ = proof_case
    env = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    if allowed and axiom not in env.allowed_axioms:
        env.allowed_axioms.append(axiom)
    if local:
        source(tmp_path, origin.replace(".", "/") + ".olean", "synthetic")

    def metadata(value, _):
        value.update(axioms=[axiom], axiom_modules={axiom: origin})

    result = await LeanTooling(env, hub.blobs, FakeRunner(metadata)).inspect(tmp_path, "Candidate", "result")
    assert result["ok"] == (allowed and not local and axiom != "sorryAx")


async def test_slot_check_rehashes_only_a_changed_identity(proof_case, tmp_path, monkeypatch):
    hub, _, _ = proof_case
    env = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    root = tmp_path / "input"
    source(root, "One.lean", "def one := 1")
    result = await LeanTooling(env, hub.blobs, FakeRunner()).compile(root, ["One"])
    workspace = Path(result["workspace"])
    import formal_geyser.lake_build as build
    original = build.file_sha256
    calls = []

    def hashed(path):
        calls.append(path)
        return original(path)

    monkeypatch.setattr(build, "file_sha256", hashed)
    receipt = (workspace / "outputs.json").read_bytes()
    known = _recover(workspace)
    assert not calls
    build._checkpoint(workspace, known)
    assert not calls
    (workspace / "outputs.json").write_bytes(receipt)
    output = workspace / ".lake/build/lib/lean/One.olean"
    output.write_bytes(b"corrupt")
    _recover(workspace)
    assert calls and not output.exists()


def test_retention_skips_active_namespace_and_preserves_lock_paths(tmp_path):
    key = "a" * 64
    namespace = tmp_path / "workspaces" / key
    for i in range(4):
        source(namespace / str(i), "source/M.lean", "def x := 1")
        source(namespace / str(i), "environment.json", json.dumps({"key": key}))
        (namespace / str(i) / "last-used").touch()
    source(tmp_path / "cache" / key, "artifact", "cached")
    with lock_file(tmp_path / "locks" / (key + ".lock"), shared=True) as held:
        assert held
        report = collect(tmp_path, max_bytes=0, force=True)
        assert report["active_namespaces_skipped"] == 1
        assert (tmp_path / "cache" / key / "artifact").exists()
    collect(tmp_path, max_idle=2, force=True)
    assert len(list(namespace.glob("*/source/M.lean"))) == 2
    collect(tmp_path, max_bytes=0, force=True)
    assert len(list(namespace.glob("*/source/M.lean"))) == 2
    assert (tmp_path / "cache" / key / "artifact").exists()
    assert len(list(namespace.glob("*/build.lock"))) == 4


async def test_build_protects_own_namespace_over_budget(proof_case, tmp_path, monkeypatch):
    import formal_geyser.lake_build as build
    hub, _, _ = proof_case
    env = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    root = tmp_path / "input"
    source(root, "Safe.lean", "def safe := 1")
    tooling = LeanTooling(env, hub.blobs, FakeRunner())
    first = await tooling.compile(root, ["Safe"])
    output = Path(first["build_directory"]) / "Safe.olean"
    saved = output.read_bytes()
    original = build.collect
    original_run = tooling.runner.run
    async def checked_run(*args, **kwargs):
        assert output.read_bytes() == saved  # The fake compiler rewrites outputs itself.
        return await original_run(*args, **kwargs)
    monkeypatch.setattr(tooling.runner, "run", checked_run)
    monkeypatch.setattr(build, "collect", lambda root, **kwargs: original(root, max_bytes=0, force=True, **kwargs))
    second = await tooling.compile(root, ["Safe"])
    assert second["ok"]
    assert second["cache_retention"]["active_namespaces_skipped"] >= 1


async def test_build_clears_an_old_idle_slot_of_its_namespace_and_rebuilds_there(proof_case, tmp_path, monkeypatch):
    import formal_geyser.lake_build as build
    hub, _, _ = proof_case
    env = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    root = tmp_path / "input"
    source(root, "Safe.lean", "def safe := 1")
    tooling = LeanTooling(env, hub.blobs, FakeRunner())
    first = await tooling.compile(root, ["Safe"])
    slot = Path(first["workspace"])
    with lock_file(slot / "build.lock") as held:  # Two builds at once leave two slots.
        assert held
        other = Path((await tooling.compile(root, ["Safe"]))["workspace"])
    assert other != slot and (other / ".lake/build/lib/lean/Safe.olean").exists()
    stale = time.time() - 25 * 3600
    os.utime(other / "last-used", (stale, stale))
    cache = Path(first["cache_directory"])
    source(cache, "artifact", "cached")
    original = build.collect
    monkeypatch.setattr(build, "collect", lambda root, **kwargs: original(root, max_idle=0, force=True, **kwargs))
    # The only environment's build takes slot 0; its maintenance clears the idle slot.
    third = await tooling.compile(root, ["Safe"])
    assert third["ok"] and Path(third["workspace"]) == slot
    assert third["cache_retention"]["active_slots_cleared"] == 1
    assert [path.name for path in other.iterdir()] == ["build.lock"]
    assert (cache / "artifact").exists()
    # A later build in the cleared slot starts clean and builds the module again.
    with lock_file(slot / "build.lock") as held:
        assert held
        fourth = await tooling.compile(root, ["Safe"])
    assert fourth["ok"] and Path(fourth["workspace"]) == other
    assert fourth["built_modules"] == ["Safe"] and not fourth["reused_modules"]
    assert (other / ".lake/build/lib/lean/Safe.olean").read_text(encoding="utf-8") == "def safe := 1"
    assert (other / "outputs.json").is_file() and (other / "last-used").is_file()


def test_module_name_cache_is_bounded_and_keeps_collision_checks(proof_case, monkeypatch):
    from formal_geyser.tooling import _MODULE_SETS, registered_modules
    hub, _, _ = proof_case
    key = hub.blobs.put_json({"files": [{"path": "Mathlib/Existing.olean", "sha256": "a" * 64}]})
    original, calls = hub.blobs.get_json, []
    def read(digest):
        calls.append(digest)
        return original(digest)
    monkeypatch.setattr(hub.blobs, "get_json", read)
    assert registered_modules(hub.blobs, key) == frozenset({"mathlib/existing"})
    assert registered_modules(hub.blobs, key) == frozenset({"mathlib/existing"})
    assert calls == [key]
    for i in range(20):
        registered_modules(hub.blobs, hub.blobs.put_json({"files": [{"path": f"M{i}.olean"}]}))
    assert len(_MODULE_SETS) <= 16


async def test_real_cross_slot_restoration(real_tooling, tmp_path):
    root = tmp_path / "input"
    source(root, "Safe.lean", "theorem correct : True := True.intro\n")
    async with real_tooling.compilation(root, ["Safe"]) as first:
        assert first["ok"], first
        async with real_tooling.compilation(root, ["Safe"]) as second:
            assert second["ok"], second
            assert first["workspace"] != second["workspace"]
            checked = await real_tooling.inspect(Path(second["build_directory"]), "Safe", "correct")
            assert checked["ok"], checked
    assert not list(root.glob("*.olean*"))  # No copies into the frozen input.


async def test_real_build_in_a_cleared_slot(real_tooling, tmp_path):
    root = tmp_path / "input"
    source(root, "Safe.lean", "theorem correct : True := True.intro\n")
    async with real_tooling.compilation(root, ["Safe"]) as first:
        async with real_tooling.compilation(root, ["Safe"]) as second:
            assert first["ok"] and second["ok"], (first, second)
    slot, other = Path(first["workspace"]), Path(second["workspace"])
    stale = time.time() - 25 * 3600
    os.utime(other / "last-used", (stale, stale))
    report = collect(slot.parents[2], max_idle=1, force=True, active_namespace=slot.parent.name)
    assert report["active_slots_cleared"] == 1 and not report["errors"]
    assert [path.name for path in other.iterdir()] == ["build.lock"]
    async with real_tooling.compilation(root, ["Safe"]) as warm:
        assert Path(warm["workspace"]) == slot and warm["reused_modules"] == ["Safe"], warm
        async with real_tooling.compilation(root, ["Safe"]) as cold:
            assert cold["ok"] and Path(cold["workspace"]) == other, cold
            assert cold["built_modules"] == ["Safe"]
            checked = await real_tooling.inspect(Path(cold["build_directory"]), "Safe", "correct")
            assert checked["ok"], checked


@pytest.mark.parametrize("proof,prop", [("stop exact False.elim (by contradiction)", "False"),
                                        ("change True; native_decide", "True")])
async def test_real_generated_assumptions_cold_warm_and_restored(real_tooling, tmp_path, proof, prop):
    root = tmp_path / "input"
    source(root, "Probe.lean", f"import Lean\ndef target : Prop := {prop}\ntheorem result : target := by {proof}\n")
    async def check():
        async with real_tooling.compilation(root, ["Probe"]) as report:
            # A cold build may reject from its warning; the inspector must still
            # reject when restored cache output no longer replays that warning.
            inspected = await real_tooling.inspect(Path(report["build_directory"]), "Probe", "result", statement="target")
            assert not inspected["ok"], inspected
            assert inspected["disallowed_axioms"]
            return report
    first = await check()
    await check()
    with _workspace(Path(first["workspace"]).parent):
        restored = await check()
    assert restored.get("reason") != "Lake omitted a requested compiled module"


async def test_real_failure_and_removed_module_preserve_unrelated_output(real_tooling, tmp_path):
    root = tmp_path / "input"
    source(root, "Good.lean", "def good := 1\n")
    source(root, "Bad.lean", "def bad := 2\n")
    first = await real_tooling.compile(root, ["Good", "Bad"])
    assert first["ok"], first
    output = Path(first["build_directory"]) / "Good.olean"
    stamp = output.stat().st_mtime_ns
    source(root, "Bad.lean", "theorem bad : False := True.intro\n")
    failed = await real_tooling.compile(root, ["Good", "Bad"])
    assert not failed["ok"]
    assert (Path(failed["workspace"]) / "outputs.json").exists()
    source(root, "Bad.lean", "def bad := 3\n")
    fixed = await real_tooling.compile(root, ["Good", "Bad"])
    assert fixed["ok"] and output.stat().st_mtime_ns == stamp, fixed
    (root / "Bad.lean").unlink()
    deleted = await real_tooling.compile(root, ["Good"])
    assert deleted["ok"] and output.stat().st_mtime_ns == stamp, deleted
    assert not list(Path(deleted["build_directory"]).glob("Bad.*"))


def test_retention_respects_legacy_slot_lock_and_idle_expiry(tmp_path):
    key = "b" * 64
    namespace = tmp_path / "workspaces" / key
    with _workspace(namespace) as slot:
        source(slot, "source/M.lean", "def x := 1")
        source(slot, "environment.json", json.dumps({"key": key}))
        source(tmp_path / "cache" / key, "artifact", "cached")
        report = collect(tmp_path, max_bytes=0, force=True)
        assert report["active_namespaces_skipped"] == 1
        assert (tmp_path / "cache" / key / "artifact").exists()
        assert (slot / "source/M.lean").exists()
    import time
    collect(tmp_path, now=time.time() + 25 * 3600, force=True)
    assert not (slot / "source/M.lean").exists()
    assert not (tmp_path / "cache" / key).exists()
