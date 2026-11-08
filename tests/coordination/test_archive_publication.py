"""Real Git archive transitions with synthetic storage-validation receipts only."""
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.core_transfer import export_core
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    Command,
    ProjectConfig,
    RepositoryConfig,
    Role,
)
from tests.coordination.lifecycle_helpers import select_review
from tests.coordination.publication_helpers import approve

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="Native Git is required")
OPERATOR = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)
VALIDATOR = Actor(role=Role.VALIDATOR)


async def command(hub, name, payload=None, actor=OPERATOR):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


def worker(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                 fencing_token=attempt["fencing_token"])


def manifest(hub, files):
    return hub.blobs.put_json({"files": [
        {"path": path, "sha256": hub.blobs.put(body)} for path, body in sorted(files.items())
    ]})


async def quiet(hub, attempt):
    return await command(hub, "runtime.quiescent", {
        "worker_id": attempt["worker_id"],
        "snapshot": hub.blobs.snapshot(hub.workspace(attempt["worker_id"])),
    }, RUNNER)


async def original_validation(scenario, verdict="accepted", qualifications=None):
    return await command(scenario.hub, "artifact.accept", {
        "artifact_id": scenario.candidate["artifact_id"], "policy": "general.peer@1",
        "verdict": verdict, "qualifications": qualifications or [],
        "details": {"scope": "Independent original candidate fixture validation"},
    }, VALIDATOR)


@pytest.fixture
async def archive_case(tmp_path):
    initialize(tmp_path, ProjectConfig(repository=RepositoryConfig(enabled=True)))
    hub = Hub(tmp_path)
    case = SimpleNamespace(hub=hub, root=tmp_path)
    try:
        case.initial = await command(hub, "repository.initialize", {"snapshot": manifest(hub, {})})
        case.host = await command(hub, "task.publish", {"contract": {
            "title": "Archive storage fixture", "objective": "Exercise exact paired-file archive storage.",
            "acceptance_policy": "general.peer@1"}})
        case.run = await command(hub, "run.start")
        case.author = await hub.next_assignment(case.run["id"])
        lean, markdown = b"-- Storage fixture; no proof is asserted.\n", b"Storage fixture explanation.\n"
        root = hub.workspace(case.author["worker_id"])
        (root / "Fixture.lean").write_bytes(lean)
        (root / "Fixture.md").write_bytes(markdown)
        await command(hub, "artifact.submit", {"kind": "lean.statement",
            "repository_paths": ["Fixture.lean", "Fixture.md"], "data": {"version": 1,
            "bundle": {"lean_path": "Fixture.lean", "markdown_path": "Fixture.md",
                       "lean_sha256": hub.blobs.put(lean), "markdown_sha256": hub.blobs.put(markdown)}}}, worker(case.author))
        case.candidate = await quiet(hub, case.author)
        await original_validation(case)
        case.reviewer = await select_review(hub, case.candidate["review_task_id"], case.run["id"])
        case.submitted = lean
        yield case
    finally:
        await case.hub.close()


async def test_acceptance_alone_stays_staged_and_publication_archives_the_submission(archive_case):
    case = archive_case
    artifact = await case.hub.artifact(case.candidate["artifact_id"])
    staged = Path(artifact["content_file"]["path"])
    assert staged.is_relative_to(case.root / "staging")
    assert not (case.root / "artifacts").exists()
    await approve(case.hub, case.reviewer)
    artifact = await case.hub.artifact(artifact["id"])
    archived = Path(artifact["content_file"]["path"])
    assert archived == case.root / "artifacts" / artifact["id"] / "1/record.json"
    assert (archived.parent / "files/Fixture.lean").read_bytes() == case.submitted
    assert not (archived.parent / "final").exists()
    assert (archived.parent / "publication.json").is_file()
    assert (staged.parent / "relocated.json").is_file() and not staged.exists()
    assert (await case.hub.task(case.host["id"]))["state"] != "done"
    await remove_current_pair(case)
    retained = await case.hub.artifact(artifact["id"])
    assert retained["status"] == "accepted" and (archived.parent / "publication.json").is_file()
    await command(case.hub, "artifact.withdraw", {"artifact_id": artifact["id"], "reason": "Synthetic withdrawal."})
    assert (archived.parent / "files/Fixture.lean").read_bytes() == case.submitted


async def remove_current_pair(case):
    """A second real Git publication removes the pair with an authored explanation."""
    hub = case.hub
    host = await command(hub, "task.publish", {"contract": {
        "title": "Remove current storage fixture", "objective": "Test retained archive independence from the current source tree.",
        "acceptance_policy": "general.peer@1"}})
    author_id = (await command(hub, "worker.register"))["id"]
    author = await command(hub, "task.claim", {"task_id": host["id"], "revision": 1,
                                              "worker_id": author_id, "run_id": case.run["id"]})
    for name in ("Fixture.lean", "Fixture.md"):
        (hub.workspace(author_id) / name).unlink()
    (hub.workspace(author_id) / "Removal.md").write_text(
        "Remove the synthetic fixture pair from the current Library; retain its accepted archive.\n",
        encoding="utf-8")
    await command(hub, "artifact.submit", {"kind": "software.storage-fixture", "data": {"remove_fixture": True},
                  "repository_paths": ["Fixture.lean", "Fixture.md", "Removal.md"]}, worker(author))
    candidate = await quiet(hub, author)
    await original_validation(SimpleNamespace(hub=hub, candidate=candidate))
    await approve(hub, await select_review(hub, candidate["review_task_id"], case.run["id"]))


async def test_the_core_export_takes_the_last_publication_however_far_the_library_view_lags(
        archive_case, tmp_path_factory):
    case = archive_case
    bundle = tmp_path_factory.mktemp("export") / "bundle"  # Outside the project, which is tmp_path.
    initial = (await case.hub.public_content()).commit  # The view on disk names the initial commit.
    await approve(case.hub, case.reviewer)  # A publication; nothing reads the view, so it stays behind.
    published = await case.hub.db.read(lambda c: c.execute(
        "SELECT commit_sha FROM repository_publications").fetchone()[0])
    view = json.loads((case.root / "library" / "current.json").read_text(encoding="utf-8"))["commit"]
    assert view == initial != published
    summary = export_core(case.root, bundle)  # While the hub serves: one ledger snapshot.
    assert (summary["library_commit"], summary["library_view_commit"]) == (published, initial)
    library = json.loads((bundle / "library.json").read_text(encoding="utf-8"))
    assert library["commit"] == published
    assert {item["path"] for item in library["files"]} >= {"Fixture.lean", "Fixture.md"}
    assert (bundle / "library" / "Fixture.lean").read_bytes() == case.submitted
