"""What follows when a candidate can no longer be accepted, and an accepted reviewer's recovery."""
from __future__ import annotations

import shutil
from types import SimpleNamespace
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    Command,
    Limits,
    ProjectConfig,
    RepositoryConfig,
    Role,
)
from tests.coordination.lifecycle_helpers import resolve_recruitment, select_review

from .wait_helpers import legacy_wait

OPERATOR, RUNNER, VALIDATOR = Actor(role=Role.OPERATOR), Actor(role=Role.RUNNER), Actor(role=Role.VALIDATOR)
POLICY = "general.peer@1"


async def command(hub, name, payload=None, actor=OPERATOR):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


def worker(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                 fencing_token=attempt["fencing_token"])


async def quiet(hub, attempt):
    return await command(hub, "runtime.quiescent", {
        "worker_id": attempt["worker_id"], "snapshot": hub.blobs.snapshot(hub.workspace(attempt["worker_id"]))}, RUNNER)


def verdict(review, **changes):
    return {**{key: review[key] for key in ("candidate_id", "candidate_hash", "task_revision", "input_refs", "policy")},
            "rationale": "Compared the document with its task.", "evidence": ["Read the exact document."], **changes}


@pytest.fixture
async def project(tmp_path):
    """A provider task with one submitted candidate (not yet validated)."""
    if shutil.which("git") is None:
        pytest.skip("Local Git is required")
    initialize(tmp_path, ProjectConfig(repository=RepositoryConfig(enabled=True), limits=Limits()))
    hub = Hub(tmp_path)
    case = SimpleNamespace(hub=hub)
    try:
        await command(hub, "repository.initialize")
        case.provider = await command(hub, "task.publish", {"contract": {
            "title": "Provider", "objective": "Write the provider document.", "acceptance_policy": POLICY}})
        case.run = await command(hub, "run.start")
        case.author = await hub.next_assignment(case.run["id"])
        (hub.workspace(case.author["worker_id"]) / "document.md").write_bytes(b"Provider.\n")
        await command(hub, "artifact.submit", {"kind": "software.result", "data": {"reviewed": True},
                                               "repository_paths": ["document.md"]}, worker(case.author))
        case.candidate = await quiet(hub, case.author)
        await resolve_recruitment(hub, case.run["id"])
        yield case
    finally:
        await hub.close()


async def machine(case, verdict_name):
    return await command(case.hub, "artifact.accept", {
        "artifact_id": case.candidate["artifact_id"], "policy": POLICY, "verdict": verdict_name,
        "details": {"scope": "Synthetic validation"}}, VALIDATOR)


async def waiting_consumer(case):
    """A worker of another task that waits for the provider's candidate to be accepted. Other
    assignments made meanwhile are kept in `case.assigned`."""
    consumer_task = await command(case.hub, "task.publish", {"contract": {
        "title": "Consumer", "objective": "Use the provider document.", "acceptance_policy": POLICY}})
    case.assigned = []
    for _ in range(6):
        consumer = await case.hub.next_assignment(case.run["id"])
        if consumer is not None and consumer["task_id"] == consumer_task["id"]:
            break
        if consumer is not None:
            case.assigned.append(consumer)
    else:
        raise AssertionError("The consumer task was not assigned")
    await legacy_wait(case.hub, {
        "checkpoint": {"summary": "Waiting for the provider.", "next_action": "Use its document."},
        "predicate": {"requirements": [{"key": "provider", "artifact_id": case.candidate["artifact_id"],
                                        "policy": POLICY}]}}, worker(consumer))
    await quiet(case.hub, consumer)
    return consumer


async def consumer_wait(case, consumer):
    return await case.hub.db.call(lambda c: tuple(c.execute(
        "SELECT state,wake_reason FROM continuations WHERE preferred_worker=? ORDER BY rowid DESC LIMIT 1",
        (consumer["worker_id"],)).fetchone()))


async def test_a_wait_on_a_rejected_candidate_is_reconsidered(project):
    consumer = await waiting_consumer(project)
    await machine(project, "accepted")
    reviewer = await select_review(project.hub, project.candidate["review_task_id"], project.run["id"])
    await command(project.hub, "review.reject", verdict(reviewer["task"]["collaboration"]["review"],
                                                        suggestions=["Correct the claim."]), worker(reviewer))
    await quiet(project.hub, reviewer)
    assert await consumer_wait(project, consumer) == ("reconsider", "candidate_rejected")


async def test_a_wait_on_a_withdrawn_candidate_is_reconsidered(project):
    consumer = await waiting_consumer(project)
    await command(project.hub, "artifact.withdraw", {"artifact_id": project.candidate["artifact_id"],
                                                     "reason": "operator_withdrawal"})
    assert await consumer_wait(project, consumer) == ("reconsider", "operator_withdrawal")


async def test_a_machine_rejection_rejects_the_candidate_and_closes_its_review(project):
    consumer = await waiting_consumer(project)
    result = await machine(project, "rejected")
    assert result["status"] == "rejected"
    request = await project.hub.db.call(lambda c: c.execute(
        "SELECT state FROM review_requests WHERE task_id=?", (project.candidate["review_task_id"],)).fetchone()[0])
    assert request == "invalidated"
    assert await consumer_wait(project, consumer) == ("reconsider", "validation_rejected")
    # The author corrects it with a candidate that replaces the rejected one.
    author = next((item for item in project.assigned if item["worker_id"] == project.author["worker_id"]), None)
    author = author or await project.hub.next_assignment(project.run["id"])
    assert author["worker_id"] == project.author["worker_id"]
    (project.hub.workspace(author["worker_id"]) / "document.md").write_bytes(b"Provider, corrected.\n")
    staged = await command(project.hub, "artifact.stage", {
        "kind": "software.result", "data": {"reviewed": True}, "repository_paths": ["document.md"],
        "replaces": project.candidate["artifact_id"]}, worker(author))
    assert staged["artifact_id"] != project.candidate["artifact_id"]


async def test_an_unavailable_validation_leaves_the_candidate_for_another_attempt(project):
    result = await machine(project, "unavailable")
    assert result["status"] != "rejected"
    request = await project.hub.db.call(lambda c: c.execute(
        "SELECT state FROM review_requests WHERE task_id=?", (project.candidate["review_task_id"],)).fetchone()[0])
    assert request == "pending"


async def test_an_accepted_reviewer_recovered_after_a_lost_turn_waits_for_questions(project):
    hub = project.hub
    await machine(project, "accepted")
    reviewer = await select_review(hub, project.candidate["review_task_id"], project.run["id"])
    await command(hub, "review.accept", verdict(reviewer["task"]["collaboration"]["review"]), worker(reviewer))
    # The turn then ended with an unknown outcome; the operator recovers the quarantined reviewer.
    await command(hub, "runtime.abandon", {"worker_id": reviewer["worker_id"], "quiescent": True,
                                           "reason": "Transport lost"}, RUNNER)
    await command(hub, "worker.recover", {"worker_id": reviewer["worker_id"], "process_confirmed_stopped": True,
                                          "reason": "The reviewer's processes stopped."})
    recovered = await hub.worker(reviewer["worker_id"])
    assert (recovered["state"], recovered["work_intention"]) == ("suspended", "waiting")
    # An Integrator's question wakes it for an answer.
    await hub.db.call(lambda c: hub._wake_for_message(c, reviewer["worker_id"], "question-1"), write=True)
    answer = await hub.next_assignment(project.run["id"])
    assert answer["worker_id"] == reviewer["worker_id"] and answer["execution_mode"] == "message_only"
