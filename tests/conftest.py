from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from formal_geyser.coordination.models import TaskContract
from formal_geyser.coordination.storage import BlobStore
from formal_geyser.registry import file_sha256, reference_ids
from formal_geyser.schemas import (
    EnvironmentRecord,
    LeanMarkdownBundle,
    LeanTaskPayload,
    ProofRecord,
    StatementRecord,
)


class FakeDatabase:
    async def call(self, function, write=False):
        return function(None)


class FakeRepository:
    def __init__(self, hub):
        self.hub = hub

    def snapshot(self, commit, blobs):
        return self.hub.library[commit]


class FakeHub:
    def __init__(self, root: Path):
        self.root = root
        self.state_dir = root / ".state"
        self.blobs = BlobStore(self.state_dir / "blobs")
        self.artifacts = {}
        self.commands = []
        self.literature = {}
        self.db = FakeDatabase()
        # Synthetic Library commits and the base each candidate was developed against.
        self.library = {}
        self.candidate_bases = {}

    def _candidate_repository(self, c, artifact_id):
        commit = self.candidate_bases.get(artifact_id)
        return None if commit is None else {"base_commit": commit}

    def _repository(self):
        return FakeRepository(self)

    async def accessible_literature(self, literature_id):
        entry = self.literature.get(literature_id)
        if entry is None:
            raise OSError("Unknown native literature: " + literature_id)
        return entry

    async def artifact(self, artifact_id):
        return self.artifacts.get(artifact_id)

    async def list_artifacts(self, kind=None):
        return [x for x in self.artifacts.values() if kind is None or x["kind"] == kind]

    async def execute(self, command, actor):
        self.commands.append((command, actor))
        if command.name == "artifact.register":
            key = "record-" + str(len(self.artifacts))
            value = {"id": key, **command.payload, "accepted": False, "status": "candidate", "evidence": []}
            self.artifacts[key] = value
            return value
        return {"id": "synthetic-task", "command": command.model_dump(mode="json")}


def approved(hub, key, record, policy):
    value = {"id": key, "kind": f"lean.{record.object_type}", "data": record.model_dump(mode="json"),
             "accepted": True, "status": "accepted", "evidence": [{"policy": policy, "verdict": "accepted"}],
             "input_refs": reference_ids(record)}
    hub.artifacts[key] = value
    return value


def cite(hub, literature_id="literature-1", body=b"Synthetic literature fixture.", locator="all"):
    """Register native literature in the fake hub and return its exact citation."""
    sha = hashlib.sha256(body).hexdigest()
    hub.literature[literature_id] = {
        "source_sha256": sha, "source_path": str(hub.root / literature_id),
        "path": str(hub.root / (literature_id + ".md")), "title": "Synthetic literature fixture"}
    return {"literature_id": literature_id, "content_sha256": sha, "locator": locator}


def snapshot(hub, file_name, body):
    entries = [{"path": file_name, "sha256": hub.blobs.put(body)}]
    if file_name.endswith(".lean"):
        entries.append({"path": file_name[:-5] + ".md", "sha256": hub.blobs.put(b"Synthetic software language fixture explanation.\n")})
    return hub.blobs.put_json({"files": entries})


def bundle(hub, snapshot_sha, module):
    entries = {item["path"]: item["sha256"] for item in hub.blobs.get_json(snapshot_sha)["files"]}
    lean_path = module.replace(".", "/") + ".lean"
    markdown_path = module.replace(".", "/") + ".md"
    return LeanMarkdownBundle(snapshot=snapshot_sha, lean_path=lean_path, lean_sha256=entries[lean_path],
                              markdown_path=markdown_path, markdown_sha256=entries[markdown_path])


@pytest.fixture(autouse=True)
def lean_service():
    """Checks run at once through FakeRunner, in this process, unless a test installs another
    Lean service; set `lean_service.runner` for other synthetic results."""
    from formal_geyser.lean_service import client
    from tests.lean_service_helpers import DirectLeanService

    service = DirectLeanService()
    previous = client.install(lambda root: service)
    try:
        yield service
    finally:
        client.install(previous)


@pytest.fixture
async def proof_case(tmp_path):
    hub = FakeHub(tmp_path)
    runtime = tmp_path / "toolchain" / "lean"
    runtime.parent.mkdir()
    runtime.write_bytes(b"synthetic Lean executable")
    runtime.with_name("lake").write_bytes(b"synthetic Lake executable")
    environment = EnvironmentRecord(name="Synthetic environment", lean_executable=str(runtime),
                                    lean_executable_sha256=file_sha256(runtime), lean_version="synthetic-version",
                                    lake_executable_sha256=file_sha256(runtime.with_name("lake")),
                                    local_execution="trusted-code-only")
    approved(hub, "environment-1", environment, "lean.environment@1")
    baseline = snapshot(hub, "Baseline.lean", b"-- Software metadata fixture only.\n")
    statement = StatementRecord(
        name="Synthetic proposition definition", statement="Software protocol fixture.",
        provenance="internal", provenance_rationale="Synthetic language protocol fixture, not a mathematical workload.",
        environment_artifact="environment-1", environment_version=1, bundle=bundle(hub, baseline, "Baseline"),
        entry_module="Baseline", declaration="Baseline.target",
    )
    approved(hub, "statement-1", statement, "lean.statement@1")
    # The candidate workspace is a checkout of its Library base plus the new proof.
    hub.library["base-commit"] = baseline
    hub.candidate_bases["candidate-1"] = "base-commit"
    added = hub.blobs.get_json(snapshot(hub, "Candidate.lean", b"-- Software metadata fixture only.\n"))["files"]
    candidate_snapshot = hub.blobs.put_json({"files": sorted(
        [*hub.blobs.get_json(baseline)["files"], *added], key=lambda item: item["path"])})
    task_payload = LeanTaskPayload(environment_artifact="environment-1")
    candidate = ProofRecord(
        name="Synthetic candidate metadata", environment_artifact="environment-1",
        task_id="task-1", task_revision=2, snapshot=candidate_snapshot,
        entry_module="Candidate", declaration="Candidate.target",
        bundle=bundle(hub, candidate_snapshot, "Candidate"), environment_version=1,
        statement_declaration="Baseline.target",
    )
    contract = TaskContract(
        title="Synthetic metadata validation", objective="Exercise software protocol only.", domain="lean",
        kind="proof", acceptance_policy="lean.proof@1", inputs=["environment-1"],
        payload=task_payload.model_dump(mode="json"),
    )
    task = {"id": "task-1", "revision": 2, "state": "candidate", "contract": contract.model_dump(mode="json")}
    artifact = {"id": "candidate-1", "kind": "lean.proof", "data": candidate.model_dump(mode="json"),
                "snapshot": candidate_snapshot, "task_id": "task-1", "task_revision": 2,
                "input_refs": reference_ids(candidate)}
    return hub, task, artifact


def fake_build_outputs(directory):
    for source in (directory / "source").rglob("*.lean"):
        relative = source.relative_to(directory / "source").with_suffix(".olean")
        target = directory / ".lake/build/lib/lean" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())


VERIFIED_REPLAY = (0, {"verdict": "verified", "constants": 1})


def fake_kernel_replay(argv, outcome=VERIFIED_REPLAY):
    """The replay helper's synthetic result, if argv runs it: an exit code and the result
    it reports (None writes no result, like an unavailable environment)."""
    from formal_geyser.process import ProcessResult
    if len(argv) <= 2 or Path(argv[2]).name != "KernelReplay.lean":
        return None
    code, result = outcome
    body = "" if result is None else json.dumps({"protocol": "task-based-lean-replay@1", **result})
    if result is not None:
        Path(argv[-1]).write_text(body, encoding="utf-8")
    return ProcessResult(tuple(argv), code, body if "finding" in (result or {}) else "", "", 0.01)


class FakeRunner:
    """Synthetic Lake/inspector responses, never claims to run Lean.

    kernel_replay is the synthetic replay helper outcome (fake_kernel_replay).
    """
    def __init__(self, mutate=None, *, exit_code=0, kernel_replay=VERIFIED_REPLAY):
        self.calls = []
        self.replays = []  # The module list of each kernel replay request.
        self.mutate = mutate
        self.exit_code = exit_code
        self.kernel_replay = kernel_replay

    async def run(self, argv, **kwargs):
        from formal_geyser.process import ProcessResult
        self.calls.append((argv, kwargs))
        if "--version" in argv:
            return ProcessResult(tuple(argv), 0, "synthetic-version\n", "", 0.01)
        if (replayed := fake_kernel_replay(argv, self.kernel_replay)) is not None:
            self.replays.append(json.loads(Path(argv[-2]).read_text(encoding="utf-8"))["modules"])
            return replayed
        if "build" in argv:
            fake_build_outputs(kwargs["cwd"])
            return ProcessResult(tuple(argv), self.exit_code, "", "", 0.01)
        module, declaration = argv[4:6]
        response = {"protocol": "task-based-lean-inspect@3", "module": module, "axioms": [], "axiom_modules": {},
                    "declaration": declaration, "kind": "theorem" if module == "Candidate" else "definition",
                    "type_repr": "synthetic type metadata", "statement_returns_prop": module != "Candidate",
                    "statement_target_repr": "synthetic type metadata", "target_matches": True,
                    "type_encoding": "repr",
                    "statement_target_encoding": "repr" if module != "Candidate" else None}
        if argv[6] != "--output":
            response["statement_module"] = argv[6].rpartition(".")[0]
        if self.mutate:
            self.mutate(response, {"directory": kwargs["cwd"], "argv": argv})
        output = Path(argv[-1])
        # The exact expressions go beside the result, as the helper writes them with --output.
        for key in ("type_repr", "statement_target_repr"):
            expression = response.pop(key, None)
            if isinstance(expression, str):
                output.with_name(output.name + "." + key).write_text(expression, encoding="utf-8")
        output.write_text(json.dumps(response), encoding="utf-8")
        return ProcessResult(tuple(argv), self.exit_code, "", "", 0.01)


@pytest.fixture
def build_case(proof_case):
    from formal_geyser.build_verification import BUILD_POLICY
    from formal_geyser.schemas import BuildCandidateRecord
    hub, _, proof_artifact = proof_case
    hub.artifacts["environment-1"]["data"]["local_execution"] = "trusted-code-only"
    contract = TaskContract(title="Compile supplied source modules", objective="Record compilation evidence only.",
                            domain="lean", acceptance_policy=BUILD_POLICY, inputs=["environment-1"],
                            payload={"environment_artifact": "environment-1", "modules": ["Candidate"]})
    task = {"id": "task-1", "revision": 2, "state": "verifying", "contract": contract.model_dump(mode="json")}
    candidate = BuildCandidateRecord(name="Build candidate", environment_artifact="environment-1", task_id="task-1",
                                     task_revision=2, modules=["Candidate"])
    artifact = {"id": "build-1", "kind": "lean.build-candidate", "task_id": "task-1", "task_revision": 2,
                "snapshot": proof_artifact["snapshot"], "data": candidate.model_dump(mode="json"),
                "input_refs": ["environment-1"]}
    return hub, task, artifact



@pytest.fixture(autouse=True, scope="session")
def operator_tokens(tmp_path_factory):
    """The tests' projects keep their operator tokens out of the user's runtime directory."""
    os.environ["FORMAL_GEYSER_RUNTIME_DIR"] = str(tmp_path_factory.mktemp("runtime"))


@pytest.fixture(autouse=True)
def local_operator_client(request, monkeypatch):
    """In-process API clients send no token; the real check has its own tests (real_operator)."""
    if request.node.get_closest_marker("real_operator") is None:
        monkeypatch.setattr("formal_geyser.coordination.api.operator_request", lambda request, token: True)


# models.py gives no role a model or effort: a real project chooses each when it is initialized.
# The tests' projects take the efforts and the Dispatcher model the code chose for every project
# before 2026-11-07, so their native turns are unchanged.
def _chosen_defaults():
    from formal_geyser.coordination.models import (
        ClaudeConfig,
        CodexConfig,
        DecisionConfig,
        ProjectConfig,
    )

    for model, field, value in ((CodexConfig, "effort", "high"), (ClaudeConfig, "effort", "high"),
                                (DecisionConfig, "model", "gpt-6.1-sol"), (DecisionConfig, "effort", "high")):
        model.model_fields[field].default = value
        model.model_rebuild(force=True)
    ProjectConfig.model_rebuild(force=True)


_chosen_defaults()


@pytest.fixture(autouse=True)
def choices_made(request):
    """Runs start without every role's model and the answer deadline chosen; the requirement
    itself has its own tests (choices_required). Its own patch outlives a test's monkeypatch.undo()."""
    if request.node.get_closest_marker("choices_required") is not None:
        yield
        return
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("formal_geyser.coordination.hub.Hub._require_choices", lambda self, c: None)
        yield
