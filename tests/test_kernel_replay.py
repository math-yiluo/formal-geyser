"""Official validation replays compiled modules through the kernel; sources cannot bypass it."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from formal_geyser.mathematical_artifacts import (
    prohibited_constructs,
    validate_snapshot_sources,
)
from formal_geyser.registry import RegistryError
from formal_geyser.schemas import EnvironmentRecord
from formal_geyser.tooling import LeanTooling, failed_check_verdict
from tests.conftest import VERIFIED_REPLAY, FakeRunner

MISMATCH = {"finding": "while replaying declaration 'proof':\n(kernel) declaration type mismatch", "module": "Two"}


def source(root, name, body):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def tooling_for(hub, runner):
    return LeanTooling(EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"]), hub.blobs, runner)


def two_modules(directory):
    source(directory, "One.lean", "def one : Nat := 1\n")
    source(directory, "Two.lean", "import One\ntheorem proof : one = 1 := rfl\n")


async def test_official_compilation_replays_every_built_module_in_the_kernel(proof_case, tmp_path):
    hub, _, _ = proof_case
    runner = FakeRunner()
    directory = tmp_path / "source"
    two_modules(directory)
    report = await tooling_for(hub, runner).compile(directory, ["One", "Two"])
    assert report["ok"], report
    assert runner.replays == [["One", "Two"]]
    replay = next(kwargs for argv, kwargs in runner.calls if Path(argv[2]).name == "KernelReplay.lean")
    assert replay["env"]["LEAN_PATH"].split(os.pathsep)[0] == str(Path(report["build_directory"]).resolve())
    assert report["kernel_replay"]["ok"] and report["kernel_replay"]["replayed_modules"] == ["One", "Two"]


async def test_a_closure_that_passed_is_not_replayed_again_until_it_or_an_import_changes(proof_case, tmp_path):
    hub, _, _ = proof_case
    runner = FakeRunner()
    tooling = tooling_for(hub, runner)
    directory = tmp_path / "source"
    two_modules(directory)
    source(directory, "Three.lean", "def three : Nat := 3\n")

    async def replayed():
        report = await tooling.compile(directory, ["Two", "Three"])
        assert report["ok"], report
        return report["kernel_replay"]["replayed_modules"], report["kernel_replay"]["previously_replayed_modules"]

    # The imported project module One is replayed although only Two and Three were requested.
    assert await replayed() == (["One", "Two", "Three"], [])
    assert await replayed() == ([], ["One", "Three", "Two"])
    source(directory, "Two.lean", "import One\ntheorem proof : one = 1 := by rfl\n")
    assert await replayed() == (["Two"], ["One", "Three"])
    source(directory, "One.lean", "def one : Nat := 1 + 0\n")
    assert await replayed() == (["One", "Two"], ["Three"])
    source(directory, "Three.lean", "def three : Nat := 2 + 1\n")
    assert not (await tooling_for(hub, FakeRunner(kernel_replay=(1, MISMATCH))).compile(directory, ["Two", "Three"]))["ok"]
    assert await replayed() == (["Three"], ["One", "Two"])  # A failed replay recorded nothing.


async def test_development_checks_skip_the_kernel_replay(proof_case, tmp_path):
    hub, _, _ = proof_case
    runner = FakeRunner()
    directory = tmp_path / "source"
    two_modules(directory)
    report = await tooling_for(hub, runner).compile(directory, ["Two"], kernel_replay=False)
    assert report["ok"] and "kernel_replay" not in report and runner.replays == []


@pytest.mark.parametrize(("outcome", "verdict"), [
    ((1, MISMATCH), "rejected"),
    ((3, None), "unavailable"),
    ((1, {"finding": "while replaying declaration 'proof':\n(kernel) deep recursion detected", "module": "Two"}),
     "unavailable"),
    ((0, {"verdict": "unexpected"}), "unavailable"),
])
async def test_a_kernel_rejection_is_a_finding_and_a_replay_failure_is_unavailable(
        proof_case, tmp_path, outcome, verdict):
    hub, _, _ = proof_case
    directory = tmp_path / "source"
    two_modules(directory)
    report = await tooling_for(hub, FakeRunner(kernel_replay=outcome)).compile(directory, ["One", "Two"])
    assert not report["ok"] and not report["kernel_replay"]["ok"]
    assert failed_check_verdict(report) == verdict
    if verdict == "rejected":
        assert report["kernel_replay"]["problem_module"] == "Two"
        assert report["reason"] == "The kernel rejected a declaration recorded in Two"


async def test_modules_that_reach_a_name_twice_are_replayed_apart(proof_case, tmp_path):
    class Holding(FakeRunner):
        """The helper holds a name for any two modules, as it would for two that both declare it."""

        async def run(self, argv, **kwargs):
            if len(argv) > 2 and Path(argv[2]).name == "KernelReplay.lean":
                several = len(json.loads(Path(argv[-2]).read_text(encoding="utf-8"))["modules"]) > 1
                self.kernel_replay = (4, {"held": "clash", "module": "One"}) if several else VERIFIED_REPLAY
            return await super().run(argv, **kwargs)
    hub, _, _ = proof_case
    runner = Holding()
    directory = tmp_path / "source"
    two_modules(directory)
    report = await tooling_for(hub, runner).compile(directory, ["One", "Two"])
    assert report["ok"], report
    assert runner.replays == [["One", "Two"], ["One"], ["Two"]]
    assert report["kernel_replay"]["split_groups"] == [{"modules": 2, "held_name": "clash"}]
    assert report["kernel_replay"]["replayed_modules"] == ["One", "Two"]
    # Both passed apart, so neither is replayed again.
    again = await tooling_for(hub, runner).compile(directory, ["One", "Two"])
    assert again["ok"] and again["kernel_replay"]["replayed_modules"] == []


async def test_a_held_name_is_never_a_finding(proof_case, tmp_path):
    hub, _, _ = proof_case
    directory = tmp_path / "source"
    two_modules(directory)
    runner = FakeRunner(kernel_replay=(4, {"held": "clash", "module": "One"}))
    report = await tooling_for(hub, runner).compile(directory, ["One", "Two"])
    # The fake holds it for each module alone too, which the real helper never does.
    assert not report["ok"] and failed_check_verdict(report) == "unavailable"
    assert report["kernel_replay"]["held_name"] == "clash" and "problem_module" not in report["kernel_replay"]


@pytest.mark.parametrize("construct", [
    "set_option debug.skipKernelTC true in\ntheorem t : True := trivial",
    "#eval IO.FS.writeFile \"x\" \"y\"",
    "#eval! (pure () : IO Unit)",
    "run_cmd Lean.logInfo \"x\"",
    "initialize counter : IO.Ref Nat ← IO.mkRef 0",
    "@[extern \"lean_environment_add\"] opaque add : Nat → Nat",
    "@[implemented_by id] def f (n : Nat) : Nat := n",
    "theorem t : True := «Lean».«addDeclWithoutChecking»",
    "set_option «debug.skipKernelTC» true",
])
def test_submitted_sources_cannot_bypass_the_kernel_or_run_code_while_compiling(tmp_path, construct):
    from formal_geyser.coordination.storage import BlobStore
    blobs = BlobStore(tmp_path / "blobs")
    snapshot = blobs.put_json({"files": [
        {"path": "Project/Module.lean", "sha256": blobs.put(construct.encode())},
        {"path": "Project/Module.md", "sha256": blobs.put(b"Explanation.\n")}]})
    with pytest.raises(RegistryError, match="bypass the kernel"):
        validate_snapshot_sources(blobs, snapshot)


def test_ordinary_names_commands_and_comments_remain_allowed():
    allowed = ("import Mathlib.Algebra.Polynomial.Eval\n#check Polynomial.eval\n"
               "-- #eval and initialize in a comment\n/- run_cmd -/\n"
               "theorem t (p : Nat) : p = p := by rfl\nexample : \"#eval\" = \"#eval\" := rfl\n")
    assert prohibited_constructs(allowed) == (set(), set())
