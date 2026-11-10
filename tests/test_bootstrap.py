"""Initialization uses synthetic source fixtures and no network or model calls."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from formal_geyser import bootstrap
from formal_geyser.bootstrap_pins import LEAN_TOOLCHAIN, pinned_versions
from formal_geyser.coordination.cli import app
from formal_geyser.coordination.config import initialize, load_config
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    Command,
    ProjectConfig,
    RepositoryConfig,
    Role,
)
from formal_geyser.coordination.storage import BlobStore, atomic_json, digest
from formal_geyser.registry import (
    EnvironmentUnavailable,
    ObjectRegistry,
    file_sha256,
    library_manifest,
)
from formal_geyser.schemas import EnvironmentRecord, LibraryRoot
from tests.conftest import FakeRunner


def write(root, relative, text):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def source_tree(root):
    write(root, "Example/Base.lean", "def exampleValue : Nat := 1\n")
    write(root, "Example/Base.md", "Synthetic language fixture: a natural-number constant.\n")
    return root


def test_import_in_place_keeps_pairs_and_excludes_runtime_and_build_configuration(tmp_path):
    source_tree(tmp_path)
    for name in (".git", ".lake", ".dependencies", ".task-based", ".private"):
        write(tmp_path, name + "/hidden.lean", "sorry\n")
    write(tmp_path, "lakefile.lean", "import Lake\n")
    write(tmp_path, "lean-toolchain", LEAN_TOOLCHAIN + "\n")
    write(tmp_path, "wiki/lesson.md", "A retained public note.\n")
    original = (tmp_path / "Example/Base.lean").read_bytes()
    hub = SimpleNamespace(root=tmp_path, blobs=BlobStore(tmp_path / ".task-based/blobs"))
    sha = bootstrap.capture_library(hub, tmp_path)
    files = {item["path"] for item in hub.blobs.get_json(sha)["files"]}
    assert files == {"Example/Base.lean", "Example/Base.md", "wiki/lesson.md",
                     "lean-toolchain", "lean-environment.lock.json", "wiki/README.md", "wiki/library.md", "wiki/literature.md"}
    assert (tmp_path / "Example/Base.lean").read_bytes() == original


def test_import_keeps_the_license_and_notice_files_of_ported_code(tmp_path):
    # The pilot's move left eight behind (licenses/NYLL/NOTICE, Langlands/LocalFactor/FML/LICENSE, ...).
    source = source_tree(tmp_path / "source")
    legal = ["licenses/NYLL/LICENSE", "licenses/NYLL/NOTICE", "Example/FML/LICENSE", "third_party/COPYING",
             "licenses/Other/LICENSE-APACHE", "licenses/Other/NOTICE.txt", "licenses/Other/LICENCE.md"]
    for path in legal:
        write(source, path, "Licensed under the Apache License, Version 2.0.\n")
    for path in ("data/results.json", "Example/notes.txt", "licenses/NYLL/logo.png", ".git/LICENSE"):
        write(source, path, "not part of a Library\n")
    hub = SimpleNamespace(root=tmp_path / "project", blobs=BlobStore(tmp_path / "state/blobs"))
    files = {item["path"] for item in hub.blobs.get_json(bootstrap.capture_library(hub, source))["files"]}
    assert set(legal) <= files
    assert not files & {"data/results.json", "Example/notes.txt", "licenses/NYLL/logo.png", ".git/LICENSE"}
    # Core transfer checks a new instance's Library against its bundle by the same rule.
    from formal_geyser.coordination.core_transfer import _source
    assert all(_source(path) for path in legal) and not any(
        _source(path) for path in ("data/results.json", ".git/LICENSE", "lakefile.lean"))


@pytest.mark.parametrize("failure", ["pair", "hole", "toolchain", "mathlib"])
def test_import_rejects_incomplete_or_incompatible_sources(tmp_path, failure):
    source = source_tree(tmp_path / "source")
    if failure == "pair":
        (source / "Example/Base.md").unlink()
    elif failure == "hole":
        write(source, "Example/Base.lean", "theorem bad : False := by sorry\n")
    elif failure == "toolchain":
        write(source, "lean-toolchain", "leanprover/lean4:v0.0.0\n")
    else:
        write(source, "lake-manifest.json", json.dumps({"packages": [{"name": "mathlib", "rev": "0" * 40}]}))
    hub = SimpleNamespace(root=tmp_path / "project", blobs=BlobStore(tmp_path / "state/blobs"))
    with pytest.raises(ValueError):
        bootstrap.capture_library(hub, source)


@pytest.fixture
async def prepared_case(tmp_path, monkeypatch):
    root = tmp_path / "project"
    initialize(root, ProjectConfig(domain="lean", repository=RepositoryConfig(enabled=True)))
    hub = Hub(root, config=load_config(root))
    cache = {"pins": pinned_versions(), "cache_directory": str(root / ".dependencies/cache"),
             "receipt_path": str(root / ".dependencies/cache/ready.json"),
             "lean_executable": str(Path(sys.executable).resolve()), "library_roots": [], "source_roots": []}
    calls = {"cache": 0, "seed": 0}
    async def prepare_cache(path, *, progress=None):
        assert path == root / ".dependencies"
        calls["cache"] += 1
        return cache
    async def environment(hub, cache, previous, progress):
        if previous.get("environment_artifact"):
            return previous["environment_artifact"]
        executable = Path(sys.executable).resolve()
        record = EnvironmentRecord(name="Synthetic initialization environment",
            lean_executable=str(executable), lean_executable_sha256=file_sha256(executable), lean_version="synthetic")
        actor = Actor(role=Role.OPERATOR)
        candidate = await ObjectRegistry(hub).stage(record, actor)
        await hub.execute(Command(name="artifact.accept", key="synthetic-approval", payload={
            "artifact_id": candidate["id"], "policy": "lean.environment@1", "verdict": "accepted",
            "details": {"synthetic_fixture": True}}), actor)
        return candidate["id"]
    async def check_seed(hub, snapshot, environment_id):
        calls["seed"] += 1
        return {"ok": True, "checked_modules": ["Example.Base"], "proof_accepted": False}
    monkeypatch.setattr("formal_geyser.bootstrap_cache.prepare_cache", prepare_cache)
    monkeypatch.setattr(bootstrap, "_environment", environment)
    monkeypatch.setattr(bootstrap, "_check_seed", check_seed)
    try:
        yield SimpleNamespace(hub=hub, root=root, cache=cache, calls=calls, check_seed=check_seed)
    finally:
        await hub.close()


async def test_prepare_imports_existing_sources_without_creating_math_approvals(prepared_case, tmp_path):
    case = prepared_case
    source = source_tree(tmp_path / "existing")
    ready = await bootstrap.prepare_project(case.hub, from_library=source)
    assert ready["state"] == "ready" and ready["pins"] == pinned_versions()
    assert ready["cache"]["cache_directory"].startswith(str(case.root / ".dependencies"))
    assert (await case.hub.repository_status())["head"] == ready["initial_commit"]
    assert await case.hub.list_workers() == []
    assert await case.hub.db.call(lambda c: c.execute("SELECT COUNT(*) FROM artifacts WHERE kind<>'lean.environment'").fetchone()[0]) == 0
    assert await case.hub.db.call(lambda c: c.execute("SELECT COUNT(*) FROM runs").fetchone()[0]) == 0
    before = ready["initial_commit"]
    repeated = await bootstrap.prepare_project(case.hub)
    assert repeated["initial_commit"] == before
    assert repeated["environment_artifact"] == ready["environment_artifact"]
    assert case.calls["seed"] == 1


async def test_failed_seed_check_can_resume_using_frozen_sources(prepared_case, tmp_path, monkeypatch):
    case = prepared_case
    source = source_tree(tmp_path / "existing")
    async def fail(*args):
        raise ValueError("Synthetic compiler unavailable")
    monkeypatch.setattr(bootstrap, "_check_seed", fail)
    with pytest.raises(ValueError, match="unavailable"):
        await bootstrap.prepare_project(case.hub, from_library=source)
    failed = bootstrap.read_initialization(case.root)
    assert failed["state"] == "failed" and failed["environment_artifact"]
    assert not (await case.hub.repository_status())["initialized"]
    write(source, "Example/Base.lean", "def replaced : Nat := 2\n")
    monkeypatch.setattr(bootstrap, "_check_seed", case.check_seed)
    ready = await bootstrap.prepare_project(case.hub)
    assert ready["initial_snapshot"] == failed["initial_snapshot"]
    assert ready["environment_artifact"] == failed["environment_artifact"]
    assert ready["state"] == "ready"


async def test_preparation_refuses_replacing_a_frozen_baseline(prepared_case, tmp_path):
    case = prepared_case
    ready = await bootstrap.prepare_project(case.hub, from_library=source_tree(tmp_path / "one"))
    with pytest.raises(ValueError, match="already frozen"):
        await bootstrap.prepare_project(case.hub, from_library=source_tree(tmp_path / "two"))
    assert bootstrap.read_initialization(case.root)["state"] == "ready"
    assert (await case.hub.repository_status())["head"] == ready["initial_commit"]


async def test_retry_recovers_its_exact_initial_git_commit_before_sql_receipt(prepared_case, tmp_path):
    case = prepared_case
    snapshot = bootstrap.capture_library(case.hub, source_tree(tmp_path / "seed"))
    initial = case.hub._repository().initialize(case.hub.blobs, snapshot)
    atomic_json(bootstrap.initialization_path(case.root), {
        "format": 1, "state": "preparing", "pins": pinned_versions(), "initial_snapshot": snapshot})
    assert (await case.hub.repository_status())["reconciliation_required"]
    ready = await bootstrap.prepare_project(case.hub)
    assert ready["initial_commit"] == initial["head"]
    assert not (await case.hub.repository_status())["reconciliation_required"]


async def test_reusing_environment_rechecks_compiled_dependency_bytes(tmp_path, monkeypatch):
    library = tmp_path / "library"
    write(library, "Fixture.olean", "Original synthetic compiled bytes")
    blobs = BlobStore(tmp_path / "blobs")
    manifest = blobs.put_json(library_manifest(library))
    executable = Path(sys.executable).resolve()
    record = EnvironmentRecord(name="Synthetic content guard", lean_executable=str(executable),
        lean_executable_sha256=file_sha256(executable), lean_version="synthetic",
        local_execution="trusted-code-only", library_roots=[LibraryRoot(path=str(library), manifest_sha256=manifest)])
    asked = []
    # The registered compiler reports the recorded version; only the compiled library changed.
    monkeypatch.setattr(bootstrap, "lean_version", lambda path: asked.append(path) or "synthetic")
    write(library, "Fixture.olean", "Modified synthetic compiled bytes")
    with pytest.raises(ValueError, match="changed"):
        await bootstrap._validate_registered_environment(SimpleNamespace(blobs=blobs), record)
    assert asked == [executable]


@pytest.mark.parametrize("reported", ["another version", None])
async def test_reusing_environment_requires_the_recorded_compiler_version(tmp_path, monkeypatch, reported):
    executable = Path(sys.executable).resolve()
    record = EnvironmentRecord(name="Synthetic version guard", lean_executable=str(executable),
        lean_executable_sha256=file_sha256(executable), lean_version="synthetic", local_execution="trusted-code-only")

    def version(path):
        if reported is None:
            raise EnvironmentUnavailable("The Lean compiler could not report its version")
        return reported
    monkeypatch.setattr(bootstrap, "lean_version", version)
    with pytest.raises(ValueError, match="cannot execute the pinned compiler"):
        await bootstrap._validate_registered_environment(SimpleNamespace(blobs=BlobStore(tmp_path / "blobs")), record)


@pytest.mark.parametrize("compiles", [True, False])
async def test_registration_asks_the_compiler_and_the_seed_check_runs_in_the_lean_service(
        tmp_path, monkeypatch, lean_service, compiles):
    root = tmp_path / "project"
    initialize(root, ProjectConfig(domain="lean", repository=RepositoryConfig(enabled=True)))
    hub = Hub(root, config=load_config(root))
    try:
        lean = tmp_path / "toolchain" / "lean"  # Synthetic: the tests' Lean service answers for it.
        lean.parent.mkdir()
        lean.write_bytes(b"synthetic Lean executable")
        lean.with_name("lake").write_bytes(b"synthetic Lake executable")
        asked = []
        monkeypatch.setattr(bootstrap, "lean_version", lambda path: asked.append(path) or "synthetic-version")
        cache = {"lean_executable": str(lean), "library_roots": [],
                 "receipt_path": str(root / ".dependencies/cache/ready.json")}
        environment_id = await bootstrap._environment(hub, cache, {}, None)
        record, _ = await ObjectRegistry(hub).get(environment_id, "environment", approved=True)
        assert asked == [lean] and record.lean_version == "synthetic-version"
        snapshot = bootstrap.capture_library(hub, source_tree(tmp_path / "seed"))
        if not compiles:
            lean_service.runner = FakeRunner(exit_code=1)
            with pytest.raises(ValueError, match="does not compile"):
                await bootstrap._check_seed(hub, snapshot, environment_id)
            assert json.loads((hub.state_dir / "initialization-check.json").read_text(encoding="utf-8"))["ok"] is False
        else:
            checked = await bootstrap._check_seed(hub, snapshot, environment_id)
            # The receipt, which every admission reads, only counts the modules; the report beside it
            # names them all, however large the Library.
            assert checked["ok"] and "checked_modules" not in checked and checked["checked_module_count"] == 1
            assert checked["checked_modules_sha256"] == digest(["Example.Base"])
            report = json.loads((hub.state_dir / "initialization-check.json").read_text(encoding="utf-8"))
            assert report["ok"] and [item["module"] for item in report["modules"]] == ["Example.Base"]
            assert lean_service.runner.replays == [["Example.Base"]]  # An official check: replayed.
        [spec] = lean_service.specs
        assert (spec.lane, spec.owner, spec.label) == ("official", "operator", "Initial Library")
        assert spec.request.snapshot == snapshot and spec.request.modules == ["Example.Base"]
    finally:
        await hub.close()


def test_lean_init_prepares_and_general_init_does_not(tmp_path, monkeypatch):
    calls = []
    async def prepare(hub, *, from_library=None, progress=None):
        calls.append((hub.root, from_library))
        assert hub.config.repository.enabled
        return {"state": "ready", "pins": pinned_versions()}
    monkeypatch.setattr(bootstrap, "prepare_project", prepare)
    monkeypatch.setattr("formal_geyser.bootstrap_codex.select_bootstrap_codex",
                        lambda executable: ("synthetic-codex", "0.160.0"))
    result = CliRunner().invoke(app, ["init", str(tmp_path / "lean"), "--domain", "lean"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["initialization"]["state"] == "ready"
    general = CliRunner().invoke(app, ["init", str(tmp_path / "general")])
    assert general.exit_code == 0
    assert len(calls) == 1


def test_failed_init_preserves_config_and_reports_retry(tmp_path, monkeypatch):
    async def fail(*args, **kwargs):
        raise ValueError("Synthetic download failure")
    monkeypatch.setattr(bootstrap, "prepare_project", fail)
    monkeypatch.setattr("formal_geyser.bootstrap_codex.select_bootstrap_codex",
                        lambda executable: ("synthetic-codex", "0.160.0"))
    root = tmp_path / "project"
    result = CliRunner().invoke(app, ["init", str(root), "--domain", "lean"])
    assert result.exit_code == 1 and "prepare" in result.output
    assert (root / "project.toml").is_file()
