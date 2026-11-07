"""Two-stage review, publication gates, and disposable review-worker execution."""
from __future__ import annotations

import shutil
from types import SimpleNamespace
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    Command,
    Limits,
    ProjectConfig,
    RepositoryConfig,
    Role,
)
from formal_geyser.coordination.repository import RepositoryStore
from tests.coordination.lifecycle_helpers import finish_ordinary, resolve_recruitment, select_review
from tests.coordination.publication_helpers import publish_batch

from .wait_helpers import legacy_wait

OPERATOR = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)
VALIDATOR = Actor(role=Role.VALIDATOR)
CHECKPOINT = {"summary": "Review integration can continue in the same task.", "next_action": "Resume the exact review."}


async def command(hub, name, payload=None, actor=OPERATOR):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


def worker(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                 fencing_token=attempt["fencing_token"])


async def quiet(hub, attempt):
    return await command(hub, "runtime.quiescent", {
        "worker_id": attempt["worker_id"],
        "snapshot": hub.blobs.snapshot(hub.workspace(attempt["worker_id"])),
    }, RUNNER)


def decision(case, **changes):
    review = case.reviewer["task"]["collaboration"]["review"]
    return {**{key: review[key] for key in (
        "candidate_id", "candidate_hash", "task_revision", "input_refs", "policy")},
        "rationale": "Inspected the immutable document and its evidence against the host contract.",
        "evidence": ["Checked the exact document in the candidate snapshot."], **changes}


async def validate_original(case, verdict="accepted"):
    return await command(case.hub, "artifact.accept", {
        "artifact_id": case.candidate["artifact_id"], "policy": "general.peer@1",
        "verdict": verdict, "details": {"scope": "Synthetic document validation"},
    }, VALIDATOR)


@pytest.fixture
async def review_case(tmp_path):
    hubs = []

    async def create(*, repository=True):
        if repository and shutil.which("git") is None:
            pytest.skip("Local Git is required")
        root = tmp_path / str(len(hubs))
        initialize(root, ProjectConfig(repository=RepositoryConfig(enabled=repository),
                                       limits=Limits()))
        hub = Hub(root)
        hubs.append(hub)
        case = SimpleNamespace(hub=hub, root=root)
        if repository:
            case.initial = await command(hub, "repository.initialize")
        case.host = await command(hub, "task.publish", {"contract": {
            "title": "Synthetic document team", "objective": "Review and integrate a software document.",
            "acceptance_policy": "general.peer@1",
        }})
        case.run = await command(hub, "run.start")
        case.author = await hub.next_assignment(case.run["id"])
        (hub.workspace(case.author["worker_id"]) / "document.md").write_bytes(b"Candidate document.\n")
        payload = {"kind": "software.result", "data": {"reviewed": True},
                   "repository_paths": ["document.md"]}
        await command(hub, "artifact.submit", payload, worker(case.author))
        case.candidate = await quiet(hub, case.author)
        await resolve_recruitment(hub, case.run["id"])
        await validate_original(case)  # A review starts only after machine validation.
        case.reviewer = await select_review(hub, case.candidate["review_task_id"], case.run["id"])
        assert case.reviewer["task_id"] == case.candidate["review_task_id"]
        return case

    yield create
    for hub in hubs:
        await hub.close()


async def integrated(case):
    """End the accepted reviewer's turn and publish it in one Integrator batch."""
    await case.hub.ordinary_turn_completed(case.reviewer["worker_id"], case.reviewer["id"])
    return await publish_batch(case.hub, case.run["id"])


async def test_acceptance_keeps_review_active_until_exact_quiet_publication(review_case):
    case = await review_case(repository=True)
    hub = case.hub
    original = await hub.artifact(case.candidate["artifact_id"])
    accepted = await command(hub, "review.accept", decision(case), worker(case.reviewer))
    assert accepted["state"] == "integrating" and "runtime_directive" not in accepted
    assert (await hub.worker(case.reviewer["worker_id"]))["state"] == "running"
    review = await hub.task(case.reviewer["task_id"])
    assert review["state"] == "claimed" and review["collaboration"]["review"]["state"] == "integrating"
    assert review["collaboration"]["review"]["acceptance"]["reviewer_id"] == case.reviewer["worker_id"]
    assert not (await hub.artifact(original["id"]))["accepted"]
    assert (await hub.artifact(original["id"]))["peer_reviews"] == []
    assert (await hub.repository_status())["head"] == case.initial["head"]
    await integrated(case)
    public = await hub.artifact(original["id"])
    assert public["accepted"]
    assert public["data"] == original["data"] and public["snapshot"] == original["snapshot"]
    store = RepositoryStore(hub.state_dir)
    files = hub.blobs.get_json(store.snapshot(store.head(), hub.blobs))["files"]
    assert hub.blobs.get(next(item["sha256"] for item in files if item["path"] == "document.md")) == b"Candidate document.\n"
    assert (await hub.task(case.host["id"]))["state"] != "done"
    author = await command(hub, "task.claim", {"task_id": case.host["id"], "revision": case.host["revision"],
        "worker_id": case.author["worker_id"], "run_id": case.run["id"]})
    await finish_ordinary(hub, case.host["id"], OPERATOR, artifact_ids=[original["id"]],
                          feedback_worker=worker(author))
    await quiet(hub, author)
    assert (await hub.task(case.host["id"]))["state"] == "done"
    assert await hub.next_assignment(case.run["id"]) is None
    assert len([task for task in await hub.list_tasks() if task["contract"]["kind"] != "integration"]) == 2


async def test_acceptance_ownership_survives_release_and_cannot_be_rewritten(review_case):
    case = await review_case(repository=True)
    hub = case.hub
    accepted = await command(hub, "review.accept", decision(case), worker(case.reviewer))
    assert await command(hub, "review.accept", decision(case), worker(case.reviewer)) == accepted
    with pytest.raises(HubError) as error:
        await command(hub, "review.accept", decision(case, rationale="A replacement decision."), worker(case.reviewer))
    assert error.value.code == "REVIEW_ALREADY_ACCEPTED"
    await legacy_wait(hub, {"checkpoint": CHECKPOINT}, worker(case.reviewer))
    await quiet(hub, case.reviewer)
    other = await command(hub, "worker.register")
    with pytest.raises(HubError) as error:
        await command(hub, "task.claim", {"task_id": case.reviewer["task_id"], "revision": 1,
                      "worker_id": other["id"], "run_id": case.run["id"]})
    assert error.value.code == "REVIEWER_INELIGIBLE"
    await hub.db.call(lambda c: hub._wake_for_message(c, case.reviewer["worker_id"], "resume-review"), write=True)
    resumed = await hub.next_assignment(case.run["id"])
    assert resumed["execution_mode"] == "message_only"
    assert resumed["worker_id"] == case.reviewer["worker_id"] and resumed["task_id"] == case.reviewer["task_id"]
    assert resumed["fencing_token"] != case.reviewer["fencing_token"]
    case.reviewer = resumed
    await integrated(case)


async def test_rejection_requires_suggestions_and_retires_the_review_worker(review_case):
    case = await review_case()
    hub = case.hub
    with pytest.raises(HubError) as error:
        await command(hub, "review.reject", decision(case), worker(case.reviewer))
    assert error.value.code == "SUGGESTIONS_REQUIRED"
    suggestions = ["Correct the unsupported sentence and include the source section used to verify it."]
    result = await command(hub, "review.reject", decision(case, suggestions=suggestions), worker(case.reviewer))
    assert result["runtime_directive"] == "drain_for_review"
    final = await quiet(hub, case.reviewer)
    assert final["host_work_ready"] is False
    task = await hub.task(case.host["id"])
    feedback = task["collaboration"]["review_feedback"][0]
    assert feedback["suggestions"] == suggestions and feedback["verdict"] == "request_changes"
    assert feedback["candidate_id"] == case.candidate["artifact_id"]
    assert not (await hub.artifact(case.candidate["artifact_id"]))["accepted"]
    assigned = [await hub.next_assignment(case.run["id"])]
    assert {attempt["task_id"] for attempt in assigned} == {case.host["id"]}
    assert {attempt["worker_id"] for attempt in assigned} == {case.author["worker_id"]}
    assert (await hub.worker(case.reviewer["worker_id"]))["state"] == "retired"
    assert await hub.next_assignment(case.run["id"]) is None
    for attempt in assigned:
        packet = await hub.context(attempt["worker_id"])
        collaboration = packet["task"]["collaboration"]
        if collaboration.get("requires_read"):
            collaboration = hub.blobs.get_json(collaboration["material_sha256"])
        assert collaboration["review_feedback"][0]["suggestions"] == suggestions


async def test_publication_requires_acceptance_and_original_validation(review_case):
    case = await review_case()
    hub = case.hub
    # A later check that could not run leaves the candidate without current validation.
    await validate_original(case, verdict="unavailable")
    assert await hub.db.call(hub._batch_eligible) == []
    await command(hub, "review.accept", decision(case), worker(case.reviewer))
    assert await hub.db.call(hub._batch_eligible) == []
    await validate_original(case)
    assert [item["candidate_id"] for item in await hub.db.call(hub._batch_eligible)] == [case.candidate["artifact_id"]]
    await integrated(case)
    assert (await hub.artifact(case.candidate["artifact_id"]))["accepted"]
    await command(hub, "run.pause", {"run_id": case.run["id"]})
    assert await hub.next_assignment(case.run["id"]) is None


async def test_contribution_rejects_disabled_repository_before_starting_review(review_case):
    with pytest.raises(HubError) as error:
        await review_case(repository=False)
    assert error.value.code == "SOURCE_FILES_REQUIRED"


async def test_host_revision_invalidates_accepted_review_without_publication(review_case):
    case = await review_case(repository=True)
    await command(case.hub, "review.accept", decision(case), worker(case.reviewer))
    await command(case.hub, "task.revise", {"task_id": case.host["id"], "revision": 1,
                  "contract": {**case.host["contract"], "objective": "Review the revised document objective."}, "reason": "Clarify the original document requirement."})
    review = await case.hub.task(case.reviewer["task_id"])
    assert review["collaboration"]["review"]["state"] == "invalidated"
    assert (await case.hub.worker(case.reviewer["worker_id"]))["state"] == "draining"
    await quiet(case.hub, case.reviewer)
    assert (await case.hub.repository_status())["head"] == case.initial["head"]
    assert not (await case.hub.artifact(case.candidate["artifact_id"]))["accepted"]


async def test_a_context_only_revision_keeps_the_accepted_review_and_its_evidence(review_case):
    case = await review_case(repository=True)
    hub = case.hub
    await command(hub, "review.accept", decision(case), worker(case.reviewer))
    revised = await command(hub, "task.revise", {"task_id": case.host["id"], "revision": 1,
                            "contract": {**case.host["contract"], "context": "The document is read by operators."},
                            "reason": "Add background for later readers."})
    assert revised["revision"] == 2
    review = await hub.task(case.reviewer["task_id"])
    assert review["collaboration"]["review"]["state"] == "integrating"
    await integrated(case)
    assert (await hub.artifact(case.candidate["artifact_id"]))["accepted"]
    author = await command(hub, "task.claim", {"task_id": case.host["id"], "revision": 2,
        "worker_id": case.author["worker_id"], "run_id": case.run["id"]})
    await finish_ordinary(hub, case.host["id"], OPERATOR, artifact_ids=[case.candidate["artifact_id"]],
                          feedback_worker=worker(author))
    await quiet(hub, author)
    assert (await hub.task(case.host["id"]))["state"] == "done"
    # A requirement on either revision is met by the evidence of revision 1.
    for revision in (1, 2):
        requirement = {"key": "document", "task_id": case.host["id"], "task_revision": revision,
                       "policy": "general.peer@1"}
        assert await hub.db.call(lambda c: hub._requirement(c, requirement))


async def test_active_review_cannot_execute_another_task_but_can_subscribe(review_case):
    case = await review_case()
    hub = case.hub
    other = await command(hub, "task.publish", {"contract": {"title": "Other team", "objective": "Independent software task."}})
    with pytest.raises(HubError) as error:
        await command(hub, "task.join", {"task_id": other["id"], "revision": 1}, worker(case.reviewer))
    assert error.value.code == "FORBIDDEN"  # TaskHub assigns tasks; a reviewer cannot join one.
    subscribed = await command(hub, "task.subscribe", {"task_id": other["id"]}, worker(case.reviewer))
    assert subscribed["id"] == other["id"]
    team = await hub.db.call(lambda c: hub._worker_task(c, case.reviewer["worker_id"]))
    assert team == case.reviewer["task_id"]


async def test_historical_memberships_do_not_override_current_execution(review_case):
    case = await review_case()
    hub = case.hub
    other = await command(hub, "task.publish", {"contract": {"title": "Second team", "objective": "Another software task."}})

    def earlier_membership(c):
        c.execute("INSERT INTO task_members(task_id,worker_id,executor,runnable,joined_at) VALUES(?,?,1,0,?)",
                  (other["id"], case.reviewer["worker_id"], hub.clock()))

    await hub.db.call(earlier_membership, write=True)
    with pytest.raises(HubError) as error:
        await hub.db.call(lambda c: hub._require_one_task(c, case.reviewer["worker_id"], case.host["id"]), write=True)
    assert error.value.code == "ALREADY_CLAIMED"


async def test_private_notes_are_excluded_from_candidates_and_publication_paths(review_case):
    case = await review_case()
    hub = case.hub
    author = await command(hub, "task.claim", {"task_id": case.host["id"], "revision": 1,
                           "worker_id": case.author["worker_id"], "run_id": case.run["id"]})
    notes = hub.workspace(author["worker_id"]) / "scratch" / ".PRIVATE"
    notes.mkdir(parents=True, exist_ok=True)
    (notes / "draft.md").write_bytes(b"Private reasoning that is not public source.\n")
    recorded = await command(hub, "record.private", {"kind": "tool.diagnostic", "data": {"summary": "Private progress."}}, worker(author))
    assert recorded["status"] == "private" and not recorded["public"]
    snapshot = hub._candidate_snapshot(author["worker_id"])
    assert all(".private" not in item["path"].casefold().split("/") for item in hub.blobs.get_json(snapshot)["files"])
    with pytest.raises(HubError) as error:
        await command(hub, "artifact.stage", {"kind": "software.result", "data": {"summary": "Software source."},
                      "repository_paths": ["scratch/.PRIVATE/draft.md"]}, worker(author))
    assert error.value.code == "INVALID_PATH"
    assert (notes / "draft.md").read_bytes() == b"Private reasoning that is not public source.\n"


async def test_a_candidate_size_check_measures_only_what_its_snapshot_takes(review_case, monkeypatch):
    case = await review_case()
    hub = case.hub
    author = await command(hub, "task.claim", {"task_id": case.host["id"], "revision": 1,
                           "worker_id": case.author["worker_id"], "run_id": case.run["id"]})
    files = hub.blobs.get_json(hub._candidate_snapshot(author["worker_id"]))["files"]
    monkeypatch.setattr(hub.blobs, "max_bytes", sum(len(hub.blobs.get(item["sha256"])) for item in files) + 10)
    for directory in ("drafts", "graphs"):
        (hub.workspace(author["worker_id"]) / directory).mkdir(parents=True, exist_ok=True)
        (hub.workspace(author["worker_id"]) / directory / "large.md").write_bytes(b"x" * 100)
    # Both directories stay out of the snapshot, so they count toward no limit.
    assert hub.blobs.get_json(hub._candidate_snapshot(author["worker_id"]))["files"] == files


async def test_a_rejection_survives_a_drain_that_never_finished(review_case):
    case = await review_case()
    hub = case.hub
    suggestions = ["Cite the section of the source that the second claim depends on."]
    await command(hub, "review.reject", decision(case, suggestions=suggestions), worker(case.reviewer))
    # Test 2 lost two verdicts this way: the drain timed out, and the quiet attempt was abandoned.
    await command(hub, "runtime.abandon", {"worker_id": case.reviewer["worker_id"], "quiescent": True,
                                           "reason": "RPC shutdown timed out draining callbacks"}, RUNNER)
    feedback = (await hub.task(case.host["id"]))["collaboration"]["review_feedback"][0]
    assert feedback["suggestions"] == suggestions and feedback["verdict"] == "request_changes"
    assert not (await hub.artifact(case.candidate["artifact_id"]))["accepted"]
    assert (await hub.worker(case.reviewer["worker_id"]))["state"] == "retired"
    resumed = await hub.next_assignment(case.run["id"])
    assert (resumed["task_id"], resumed["worker_id"]) == (case.host["id"], case.author["worker_id"])


async def contribution_drain(tmp_path):
    if shutil.which("git") is None:
        pytest.skip("Local Git is required")
    initialize(tmp_path, ProjectConfig(repository=RepositoryConfig(enabled=True), limits=Limits()))
    hub = Hub(tmp_path)
    await command(hub, "repository.initialize")
    await command(hub, "task.publish", {"contract": {
        "title": "Synthetic document", "objective": "Write a software document.",
        "acceptance_policy": "general.peer@1"}})
    run = await command(hub, "run.start")
    author = await hub.next_assignment(run["id"])
    (hub.workspace(author["worker_id"]) / "document.md").write_bytes(b"Candidate document.\n")
    submitted = await command(hub, "artifact.submit", {"kind": "software.result", "data": {"reviewed": True},
                                                       "repository_paths": ["document.md"]}, worker(author))
    assert submitted["runtime_directive"] == "drain_for_submission"
    return hub, author


async def test_a_contribution_survives_a_drain_that_never_finished(tmp_path):
    hub, author = await contribution_drain(tmp_path)
    try:
        result = await command(hub, "runtime.abandon", {"worker_id": author["worker_id"], "quiescent": True,
                                                        "reason": "RPC shutdown timed out draining callbacks"}, RUNNER)
        assert result["directive"] == "verify"
        assert (await hub.artifact(result["artifact_id"]))["status"] == "staged"
        assert (await hub.worker(author["worker_id"]))["active_attempt"] is None
    finally:
        await hub.close()


async def test_a_contribution_that_can_no_longer_be_staged_is_abandoned_with_its_reason(tmp_path):
    # Abandoning, as before: the worker waits for operator recovery, and the reason is recorded.
    hub, author = await contribution_drain(tmp_path)
    try:
        (hub.workspace(author["worker_id"]) / "document.md").unlink()
        result = await command(hub, "runtime.abandon", {"worker_id": author["worker_id"], "quiescent": True,
                                                        "reason": "RPC shutdown timed out draining callbacks"}, RUNNER)
        assert result["directive"] == "recovery_required"
        recorded = await hub.db.call(lambda c: c.execute(
            "SELECT json_extract(payload_json,'$.code') FROM events "
            "WHERE kind='attempt.submission_not_committed' AND object_id=?", (author["id"],)).fetchall())
        assert [row[0] for row in recorded] == ["SOURCE_FILES_REQUIRED"]
    finally:
        await hub.close()
