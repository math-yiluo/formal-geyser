"""Persistent Lake cache tests; real tests use tiny language fixtures, never a workload."""

import asyncio
import os
from pathlib import Path

import pytest

from formal_geyser.coordination.storage import BlobStore
from formal_geyser.lake_build import (
    BUILD_OUTPUT_PER_SOURCE,
    _output_times,
    _sources,
    _synchronize,
    _workspace,
    build_output_limit,
)
from formal_geyser.process import ProcessResult, ProcessRunner
from formal_geyser.registry import EnvironmentUnavailable, file_sha256
from formal_geyser.schemas import EnvironmentRecord
from formal_geyser.tooling import LeanTooling, failed_check_verdict
from tests.conftest import FakeRunner


def source(root, name, body):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


async def test_persistent_workspace_ignores_markdown_and_recovers_partial_build(
    proof_case, tmp_path
):
    hub, _, _ = proof_case
    env = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    tooling = LeanTooling(env, hub.blobs, FakeRunner())
    directory = tmp_path / "source"
    source(directory, "Example.lean", "def exampleValue : Nat := 1\n")
    first = await tooling.compile(directory, ["Example"])
    assert first["ok"], first["process"]["stdout"] + first["process"]["stderr"]
    source(directory, "Example.md", "Only prose changed.")
    second = await tooling.compile(directory, ["Example"])
    assert first["workspace"] == second["workspace"]
    root = Path(first["workspace"])
    (root / "outputs.json").unlink()
    source(root, ".lake/build/lib/lean/Ghost.olean", "stale")
    assert (await tooling.compile(directory, ["Example"]))["ok"]
    assert not (root / ".lake/build/lib/lean/Ghost.olean").exists()


def test_workspace_locks_are_exclusive_without_a_global_limit(tmp_path):
    with _workspace(tmp_path) as one, _workspace(tmp_path) as two:
        assert one != two
    with _workspace(tmp_path) as reused:
        assert reused == one


def test_a_slot_limit_gives_no_slot_while_each_one_under_it_is_held(tmp_path):
    with _workspace(tmp_path, 2) as one, _workspace(tmp_path, 2) as two:
        assert (one.name, two.name) == ("0", "1")
        with _workspace(tmp_path, 2) as third:
            assert third is None
    assert not (tmp_path / "2").exists()  # No slot past the limit is made.
    with _workspace(tmp_path, 2) as again:
        assert again.name == "0"


async def until(condition, seconds=5.0):
    async def poll():
        while not condition():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), seconds)


async def test_builds_past_the_slot_limit_wait_for_a_slot_first_come_first_served(tmp_path):
    from formal_geyser.lake_build import _compilation_slot

    taken, releases = [], {}

    async def build(name):
        async with _compilation_slot(tmp_path, "n" * 20, 1) as slot:
            releases[name] = asyncio.Event()
            taken.append((name, slot.name))
            await releases[name].wait()

    tasks = {"first": asyncio.create_task(build("first"))}
    await until(lambda: taken)
    for name in ("second", "third"):
        tasks[name] = asyncio.create_task(build(name))
        await asyncio.sleep(0.05)
    assert taken == [("first", "0")]  # The others wait: one slot only.
    for name, after in (("first", "second"), ("second", "third")):
        releases[name].set()
        await tasks[name]
        await until(lambda: len(taken) == len(releases))
        assert taken[-1] == (after, "0")
    releases["third"].set()
    await tasks["third"]
    assert sorted(path.name for path in (tmp_path / "workspaces" / ("n" * 20)).iterdir()) == ["0"]


async def test_a_build_under_the_slot_limit_waits_for_a_slot_held_elsewhere(tmp_path, monkeypatch):
    from formal_geyser import lake_build

    monkeypatch.setattr(lake_build, "SLOT_POLL_SECONDS", 0.01)
    root = tmp_path / "workspaces" / ("m" * 20)
    taken = []

    async def build():
        async with lake_build._compilation_slot(tmp_path, "m" * 20, 1) as slot:
            taken.append(slot.name)

    with _workspace(root) as held:  # As another process would hold it.
        assert held.name == "0"
        waiting = asyncio.create_task(build())
        await asyncio.sleep(0.1)
        assert not taken and not (root / "1").exists()
    await asyncio.wait_for(waiting, 5)
    assert taken == ["0"]


async def test_changed_lake_cannot_silently_change_the_environment(proof_case, tmp_path):
    hub, _, _ = proof_case
    env = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    directory = tmp_path / "source"
    source(directory, "Example.lean", "def exampleValue : Nat := 1\n")
    first = await LeanTooling(env, hub.blobs, FakeRunner()).compile(directory, ["Example"])
    Path(env.lean_executable).with_name("lake").write_bytes(b"new Lake")
    assert first["ok"]
    with pytest.raises(EnvironmentUnavailable, match="content has changed"):
        await LeanTooling(env, hub.blobs, FakeRunner()).compile(directory, ["Example"])


async def test_successful_exit_without_outputs_is_not_acceptance(proof_case, tmp_path):
    hub, _, _ = proof_case
    env = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    directory = tmp_path / "source"
    source(directory, "Example.lean", "def exampleValue : Nat := 1\n")

    class Empty:
        async def run(self, argv, **kwargs):
            return ProcessResult(tuple(argv), 0, "", "", 0.01)

    report = await LeanTooling(env, hub.blobs, Empty()).compile(directory, ["Example"])
    assert not report["ok"] and failed_check_verdict(report) == "unavailable"


async def test_parallel_builds_use_separate_slots(proof_case, tmp_path):
    hub, _, _ = proof_case
    env = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    entered = asyncio.Event()
    release = asyncio.Event()

    class Waiting(FakeRunner):
        async def run(self, argv, **kwargs):
            entered.set()
            await release.wait()
            return await super().run(argv, **kwargs)

    one, two = tmp_path / "one", tmp_path / "two"
    for root in (one, two):
        source(root, "Example.lean", "def exampleValue : Nat := 1\n")
    first = asyncio.create_task(LeanTooling(env, hub.blobs, Waiting()).compile(one, ["Example"]))
    await entered.wait()
    second = await LeanTooling(env, hub.blobs, FakeRunner()).compile(two, ["Example"])
    release.set()
    assert (await first)["workspace"] != second["workspace"]


@pytest.fixture
async def real_tooling(tmp_path):
    location = os.environ.get("FORMAL_GEYSER_TEST_LEAN")
    if not location:
        pytest.skip("Set FORMAL_GEYSER_TEST_LEAN for real Lake tests")
    lean = Path(location).resolve()
    runner = ProcessRunner()
    version = await runner.run([str(lean), "--version"], cwd=tmp_path)
    assert version.ok
    env = EnvironmentRecord(
        name="Tiny language regression fixture",
        lean_executable=str(lean),
        lean_executable_sha256=file_sha256(lean),
        lake_executable_sha256=file_sha256(lean.with_name("lake" + lean.suffix)),
        lean_version=version.stdout.strip(),
        local_execution="trusted-code-only",
    )
    # The test stands in for the Lean service, which supplies the runner of its jobs.
    return LeanTooling(env, BlobStore(tmp_path / "state/blobs"), runner)


async def test_real_lake_incremental_build_and_deleted_import(real_tooling, tmp_path):
    root = tmp_path / "input"
    source(root, "Base.lean", "def sampleValue : Nat := 1\n")
    source(root, "Use.lean", "import Base\ndef useValue : Nat := sampleValue\n")
    source(root, "Other.lean", "def independentValue : Nat := 3\n")
    modules = ["Base", "Use", "Other"]
    first = await real_tooling.compile(root, modules)
    assert first["ok"], first["process"]["stdout"] + first["process"]["stderr"]
    second = await real_tooling.compile(root, modules)
    assert second["ok"] and set(second["reused_modules"]) == set(modules), second
    source(root, "Use.md", "A prose-only edit.")
    assert set((await real_tooling.compile(root, modules))["reused_modules"]) == set(modules)
    source(root, "Base.lean", "def sampleValue : Nat := 2\n")
    changed = await real_tooling.compile(root, modules)
    assert changed["ok"], changed
    assert "Other" in changed["reused_modules"]
    assert "Base" in changed["built_modules"] and "Use" in changed["built_modules"]
    (root / "Base.lean").unlink()
    deleted = await real_tooling.compile(root, ["Use", "Other"])
    assert not deleted["ok"], deleted


async def test_real_exact_target_inspection_without_audit(real_tooling, tmp_path):
    root = tmp_path / "input"
    source(root, "Statement.lean", "def targetStatement : Prop := True\n")
    source(
        root,
        "Proof.lean",
        "import Statement\ntheorem goodProof : targetStatement := True.intro\ntheorem wrongProof : 1 = 1 := rfl\n",
    )
    async with real_tooling.compilation(root, ["Statement", "Proof"]) as compiled:
        assert compiled["ok"], compiled
        directory = Path(compiled["build_directory"])
        good = await real_tooling.inspect(directory, "Proof", "goodProof", statement="targetStatement")
        wrong = await real_tooling.inspect(directory, "Proof", "wrongProof", statement="targetStatement")
    assert good["ok"] and good["metadata"]["target_matches"], good
    assert wrong["ok"] and not wrong["metadata"]["target_matches"], wrong
    assert good["metadata"]["axioms"] == []
    assert not {"module_axioms", "declared_axioms"}.intersection(good["metadata"])


async def test_one_lake_process_inherits_explicit_limits_and_detects_source_edits(proof_case, tmp_path):
    hub, _, _ = proof_case
    environment = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    environment.limits.timeout_seconds = 7200
    root = tmp_path / "input"
    source(root, "One.lean", "def one : Nat := 1\n")
    source(root, "Two.lean", "def two : Nat := 2\n")

    class Editing(FakeRunner):
        async def run(self, argv, **kwargs):
            assert kwargs["timeout"] == 7200
            assert kwargs["output_limit"] == environment.limits.output_bytes
            result = await super().run(argv, **kwargs)
            (kwargs["cwd"] / "source/One.lean").write_text("def one : Nat := 3\n")
            return result

    runner = Editing()
    result = await LeanTooling(environment, hub.blobs, runner).compile(root, ["One", "Two"])
    assert len(runner.calls) == 1
    assert not result["ok"] and "frozen source" in result["reason"]
    assert not (Path(result["workspace"]) / "outputs.json").exists()


async def test_a_build_may_print_more_the_more_sources_it_may_compile(proof_case, tmp_path):
    hub, _, _ = proof_case
    environment = EnvironmentRecord.model_validate(hub.artifacts["environment-1"]["data"])
    environment.limits.output_bytes = 1024
    root = tmp_path / "input"
    for index in range(3):
        source(root, f"M{index}.lean", f"def m{index} : Nat := {index}\n")
        source(root, f"M{index}.md", "Prose is not compiled.\n")
    bounds = []

    class Counting(FakeRunner):
        async def run(self, argv, **kwargs):
            if "build" in argv:
                bounds.append(kwargs["output_limit"])
            return await super().run(argv, **kwargs)

    assert (await LeanTooling(environment, hub.blobs, Counting()).compile(root, ["M0"]))["ok"]
    # A fresh build prints a line and Lean's messages for each module: stopping it at the
    # environment's bound would refuse a large Library that compiles.
    assert bounds == [3 * BUILD_OUTPUT_PER_SOURCE]
    assert build_output_limit(1048576, {"A.lean": b"", "A.md": b""}) == 1048576


def test_source_walk_skips_hidden_and_linked_directories_and_refuses_linked_files(tmp_path):
    root, outside = tmp_path / "input", tmp_path / "outside"
    for name in ("A.lean", "A/Z.lean", "lakefile.lean", ".hidden/C.lean", "A/.D.lean", "Notes.md"):
        source(root, name, name)
    source(outside, "E.lean", "outside")
    os.symlink(outside, root / "Linked", target_is_directory=True)
    sources = _sources(root)
    assert list(sources) == ["A/Z.lean", "A.lean"]  # By path parts, as a sorted walk of paths.
    assert sources["A.lean"] == b"A.lean"
    os.symlink(outside / "E.lean", root / "F.lean")
    with pytest.raises(ValueError, match="Linked"):
        _sources(root)


def test_synchronize_projects_sources_and_refuses_a_link_anywhere_below(tmp_path):
    slot = tmp_path / "slot"
    slot.mkdir()
    _synchronize(slot, {"A.lean": b"a", "Sub/B.lean": b"b"}, "key", [])
    assert (slot / "source/Sub/B.lean").read_bytes() == b"b"
    for name in ("lib/lean/Sub/B.olean", "ir/Sub/B.c", "lib/lean/Sub/Bx.olean"):
        source(slot / ".lake/build", name, "compiled")
    _synchronize(slot, {"A.lean": b"a2"}, "key", [])
    assert (slot / "source/A.lean").read_bytes() == b"a2" and not (slot / "source/Sub/B.lean").exists()
    # A deleted module's outputs go; another module's stay.
    assert sorted(path.name for path in (slot / ".lake/build").rglob("*") if path.is_file()) == ["Bx.olean"]
    os.symlink(tmp_path, slot / "source" / "Sub" / "Escape", target_is_directory=True)
    with pytest.raises(ValueError, match="Linked"):
        _synchronize(slot, {"A.lean": b"a"}, "key", [])


def test_output_times_cover_compiled_files_only(tmp_path):
    for name in ("A.olean", "Sub/B.olean.private", "Sub/B.ilean", "Sub/B.trace", "Sub/B.olean.hash"):
        source(tmp_path / ".lake/build/lib/lean", name, "compiled")
    times = _output_times(tmp_path)
    assert list(times) == ["A.olean", "Sub/B.ilean", "Sub/B.olean.hash", "Sub/B.olean.private"]
    assert times["A.olean"] == (tmp_path / ".lake/build/lib/lean/A.olean").stat().st_mtime_ns
    assert _output_times(tmp_path / "missing") == {}


async def test_cancellation_waits_for_filesystem_work_before_releasing_slot(tmp_path):
    import threading

    from formal_geyser.lake_build import _disk

    entered, release = threading.Event(), threading.Event()

    def slow():
        entered.set()
        release.wait(timeout=10)

    async def operation():
        with _workspace(tmp_path) as root:
            await _disk(slow)
            return root

    work = asyncio.create_task(operation())
    await asyncio.to_thread(entered.wait, 5)
    work.cancel()
    await asyncio.sleep(0.02)
    work.cancel()  # Cancelled again while it waits for the write: the slot stays locked.
    await asyncio.sleep(0.02)
    with _workspace(tmp_path) as other:
        assert other.name == "1"
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await work
    with _workspace(tmp_path) as free:
        assert free.name == "0"


async def test_real_build_failure_and_sorry_diagnostic(real_tooling, tmp_path):
    root = tmp_path / "input"
    source(root, "Bad.lean", "theorem bad : False := True.intro\n")
    failed = await real_tooling.compile(root, ["Bad"])
    assert not failed["ok"] and failed_check_verdict(failed) == "rejected", failed
    source(root, "Bad.lean", "theorem bad : False := by sorry\n")
    placeholder = await real_tooling.compile(root, ["Bad"])
    assert not placeholder["ok"] and "sorry" in placeholder["reason"], placeholder


async def test_real_kernel_replay_rejects_a_theorem_the_kernel_never_checked(real_tooling, tmp_path):
    root = tmp_path / "input"
    source(root, "Stmt.lean", "def Stmt : Prop := False\n")
    # Records `proof : Stmt` with value `True.intro` and no kernel check: it compiles,
    # reports no axioms, and target inspection alone would accept it.
    source(root, "Proof.lean", (
        "import Lean\nimport Stmt\nopen Lean Elab Command\n\n"
        "set_option debug.skipKernelTC true in\n"
        "run_cmd liftCoreM do\n"
        "  addDecl (.thmDecl { name := `proof, levelParams := [], type := mkConst ``Stmt, "
        "value := mkConst ``True.intro })\n"))
    development = await real_tooling.compile(root, ["Stmt", "Proof"], kernel_replay=False)
    assert development["ok"], development
    official = await real_tooling.compile(root, ["Stmt", "Proof"])
    assert not official["ok"] and failed_check_verdict(official) == "rejected", official
    assert official["kernel_replay"]["problem_module"] == "Proof", official["kernel_replay"]
    source(root, "Proof.lean", "import Stmt\ntheorem proof : ¬Stmt := id\n")
    honest = await real_tooling.compile(root, ["Stmt", "Proof"])
    assert honest["ok"] and honest["kernel_replay"]["replayed_modules"] == ["Stmt", "Proof"], honest


async def test_real_replay_of_several_modules_holds_a_name_they_reach_twice(real_tooling, tmp_path):
    root = tmp_path / "input"
    source(root, "Base.lean", "def base : Nat := 1\n")
    source(root, "Mid.lean", "import Base\ndef mid : Nat := base\n")
    source(root, "Top.lean", "import Mid\ndef top : Nat := mid\n")
    # One name in two modules that never import each other, as Lean names some instances itself.
    source(root, "Left.lean", "def clash : Nat := 1\n")
    source(root, "Right.lean", "def clash : Nat := 2\n")
    source(root, "LeftUse.lean", "import Left\ndef leftUse : Nat := clash\n")
    source(root, "RightUse.lean", "import Right\ndef rightUse : Nat := clash\n")
    # The same theorem twice is one declaration to Lean's import and to replay.
    source(root, "SameOne.lean", "theorem same : (1 : Nat) = 1 := rfl\n")
    source(root, "SameTwo.lean", "theorem same : (1 : Nat) = 1 := by rfl\n")
    modules = ["Base", "Mid", "Top", "Left", "Right", "LeftUse", "RightUse", "SameOne", "SameTwo"]
    async with real_tooling.compilation(root, modules, kernel_replay=False) as compiled:
        assert compiled["ok"], compiled
        build = Path(compiled["build_directory"])

        async def replay(*names):
            return await real_tooling.kernel_replay(build, list(names), [])
        # Top reaches Base through Mid, which is not replayed with them.
        through = await replay("Base", "Top")
        assert not through["ok"] and through["held_name"] == "base", through
        assert (await replay("Base", "Mid", "Top"))["ok"]
        for pair in (("Left", "Right"), ("Left", "RightUse"), ("LeftUse", "RightUse")):
            held = await replay(*pair)
            assert not held["ok"] and held["held_name"] == "clash" and failed_check_verdict(held) == "unavailable", held
        for alone in ("Base", "Top", "Left", "RightUse"):
            assert (await replay(alone))["ok"], alone
        assert (await replay("SameOne", "SameTwo"))["ok"]
    official = await real_tooling.compile(root, modules)
    assert official["ok"], official["kernel_replay"]
    assert official["kernel_replay"]["split_groups"][0]["held_name"] == "clash", official["kernel_replay"]
    assert sorted(official["kernel_replay"]["replayed_modules"]) == sorted(modules)


async def test_real_shared_prebuilt_mathlib_import(real_tooling, tmp_path, monkeypatch):
    """Test only Lake's prebuilt-import adapter, not dependency-manifest validation."""
    receipt = os.environ.get("FORMAL_GEYSER_TEST_INITIALIZATION_RECEIPT")
    if not receipt:
        pytest.skip("Set FORMAL_GEYSER_TEST_INITIALIZATION_RECEIPT for the shared Mathlib import test")
    value = __import__("json").loads(Path(receipt).read_text(encoding="utf-8"))
    roots = [Path(p) for p in value["cache"]["library_roots"]]

    async def paths():
        return roots

    monkeypatch.setattr(real_tooling, "library_paths", paths)
    root = tmp_path / "input"
    source(root, "Mathlib/Extension.lean", "import Mathlib.Data.Nat.Basic\ndef sampleValue : Nat := 1\n")
    first = await real_tooling.compile(root, ["Mathlib.Extension"])
    assert first["ok"], first["process"]["stdout"] + first["process"]["stderr"]
    second = await real_tooling.compile(root, ["Mathlib.Extension"])
    assert second["ok"] and second["reused_modules"] == ["Mathlib.Extension"], second
    assert list((Path(first["workspace"]) / ".lake/packages").glob("*")) == []


async def test_real_lake_preserves_existing_proposition_fingerprint(real_tooling, tmp_path):
    from formal_geyser.process import process_environment

    root = tmp_path / "input"
    source(root, "Statement.lean", "private def predicate (n : Nat) : Prop := n = n\n"
           "def expected : Prop := ∀ n : Nat, predicate n\n")
    direct = await real_tooling.runner.run(
        [str(real_tooling.executable()), "--json", "-o", str(root / "Statement.olean"),
         str(root / "Statement.lean")], cwd=root, env=process_environment())
    assert direct.ok, direct.stdout + direct.stderr
    before = await real_tooling.inspect(root, "Statement", "expected")
    assert before["ok"], before
    async with real_tooling.compilation(root, ["Statement"]) as compiled:
        assert compiled["ok"], compiled
        after = await real_tooling.inspect(Path(compiled["build_directory"]), "Statement", "expected")
    assert after["ok"], after
    assert before["metadata"]["statement_target_sha256"] == after["metadata"]["statement_target_sha256"]
