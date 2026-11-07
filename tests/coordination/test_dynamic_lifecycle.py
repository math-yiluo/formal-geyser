from __future__ import annotations

import asyncio
import json
from uuid import uuid4

import pytest
from pydantic import ValidationError

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import (
    Actor,
    Command,
    Limits,
    ProjectConfig,
    Role,
)
from formal_geyser.coordination.runtime import ScriptedHarness, WorkerRunner
from tests.coordination.lifecycle_helpers import (
    finish_ordinary,
    open_collaboration,
    resolve_recruitment,
    select_review,
)
from tests.coordination.publication_helpers import approve, file_payload, new_hub

from .wait_helpers import legacy_wait

OP = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)


async def command(hub, name, payload=None, actor=OP):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


def actor(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                 fencing_token=attempt["fencing_token"])


@pytest.fixture
async def case(tmp_path):
    clock = [10_000]
    hub = await new_hub(tmp_path, config=ProjectConfig(), clock=lambda: clock[0])
    root = await command(hub, "task.publish", {"contract": {"title": "Task 0", "objective": "Satisfy the original goal."}})
    run = await command(hub, "run.start")
    yield hub, root, run, clock
    await hub.close()


async def quiet(hub, attempt):
    return await command(hub, "runtime.quiescent", {"worker_id": attempt["worker_id"],
                         "snapshot": hub.blobs.snapshot(hub.workspace(attempt["worker_id"]))}, RUNNER)


async def recruitment(hub, attempt, decision=True):
    await hub.ordinary_turn_completed(attempt["worker_id"], attempt["id"])
    helpers = await hub.due_recruitment_intents(attempt["run_id"])
    assert len(helpers) == 1
    await hub.decide_recruitment(helpers[0]["id"], decision, "Check the definitions" if decision else "")
    return helpers[0]


async def publish(hub, title):
    return await command(hub, "task.publish", {"contract": {"title": title, "objective": "Produce the stated result."}})


async def source(hub):
    return await command(hub, "worker.register")


async def claim(hub, task, run, worker=None, plan=""):
    worker = worker or await source(hub)
    return await command(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"],
                                           "run_id": run["id"], "worker_id": worker["id"], "plan": plan})


async def review_candidate(hub, author, run, *, reject=False):
    candidate = await command(hub, "artifact.stage", file_payload(
        hub, author, data={"result": "verified"}), actor(author))
    await command(hub, "artifact.accept", {"artifact_id": candidate["artifact_id"], "policy": "general.peer@1",
                                          "verdict": "accepted", "details": {}, "qualifications": []})
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    basis = reviewer["task"]["collaboration"]["review"]
    report = {key: basis[key] for key in ("candidate_id", "candidate_hash", "task_revision", "input_refs", "policy")}
    report.update(rationale="Compared the exact result and dependencies.", evidence=["Exact submitted result checked."], qualifications=[])
    if reject:
        await command(hub, "review.reject", {**report, "suggestions": ["Correct the counterexample."]}, actor(reviewer))
        await quiet(hub, reviewer)
    else:
        await approve(hub, reviewer, report)
    return candidate, reviewer


def test_a_retired_limit_is_rejected():
    with pytest.raises(ValidationError):
        Limits.model_validate({"max_active": 1})


@pytest.mark.asyncio
async def test_first_turn_choice_and_atomic_one_shot_recruitment(case):
    hub, task, run, _ = case
    first = await hub.next_assignment(run["id"])
    with pytest.raises(HubError, match="no authorized collaborator"):
        await claim(hub, task, run, plan="Investigate independently")
    helper = await recruitment(hub, first)
    workers = [await source(hub), await source(hub)]
    results = await asyncio.gather(*(claim(hub, task, run, worker, "Check definitions") for worker in workers), return_exceptions=True)
    assert sum(isinstance(item, dict) for item in results) == 1
    assert sum(isinstance(item, HubError) for item in results) == 1
    assert (await hub.task(task["id"]))["collaboration"]["recruitment"]["opportunity_state"] == "consumed"
    await hub.decide_recruitment(helper["id"], True, "Check the definitions")
    assert len([worker for worker in await hub.list_workers() if worker["active_attempt"]]) == 1


@pytest.mark.asyncio
async def test_no_and_pause_before_choice_never_reopen(case):
    hub, task, run, _ = case
    first = await hub.next_assignment(run["id"])
    await recruitment(hub, first, False)
    continued = await hub.next_assignment(run["id"])
    assert continued["worker_id"] == first["worker_id"]
    await legacy_wait(hub, {"reason": "No useful immediate work"}, actor(continued))
    await quiet(hub, continued)
    lifecycle = (await hub.task(task["id"]))["collaboration"]["recruitment"]
    assert lifecycle["recruitment_decision"] == "no"
    assert lifecycle["opportunity_state"] == "cancelled"
    with pytest.raises(HubError, match="decided only once"):
        await command(hub, "task.invitation.update", {"task_id": task["id"], "revision": 1, "reason": "Late need"})


@pytest.mark.asyncio
async def test_pause_cancels_pending_first_choice(case):
    hub, task, run, _ = case
    first = await hub.next_assignment(run["id"])
    await legacy_wait(hub, {"reason": "Waiting for a new approach"}, actor(first))
    await quiet(hub, first)
    assert await hub.record_first_turn(first["worker_id"], task["id"]) is None
    assert (await hub.task(task["id"]))["collaboration"]["recruitment"]["recruitment_decision"] == "cancelled"


@pytest.mark.asyncio
async def test_paused_worker_takes_no_new_task_and_an_empty_pool_needs_no_decision(case):
    hub, _, run, clock = case
    first = await hub.next_assignment(run["id"])
    await legacy_wait(hub, {"reason": "No useful progress"}, actor(first))
    await quiet(hub, first)
    other = await publish(hub, "Independent task")
    # A worker that paused mid-task is waiting, not idle: a new worker takes the task
    # at once and starts a new conversation.
    child = await hub.next_assignment(run["id"])
    assert child["task_id"] == other["id"] and child["worker_id"] != first["worker_id"]
    assert (await hub.worker(child["worker_id"]))["thread_id"] is None
    assert (await hub.worker(first["worker_id"]))["work_intention"] == "waiting"
    clock[0] += 3_600_001
    # Nothing is open, so nothing needs a decision, however long the worker waits.
    assert await hub.next_assignment(run["id"]) is None
    assert await hub.due_assignment_intents(run["id"]) == []


@pytest.mark.asyncio
async def test_every_open_opportunity_gets_a_new_worker_at_once_without_cooldown(case):
    hub, _, run, _ = case
    first = await hub.next_assignment(run["id"])
    await recruitment(hub, first)
    continued = await hub.next_assignment(run["id"])
    assert continued["worker_id"] == first["worker_id"]
    other = await publish(hub, "Other")
    # The clock never moves: with no idle worker, the collaborator opening and the new
    # task each get a new worker at once, without an hour's wait or a shared cooldown.
    second = await hub.next_assignment(run["id"])
    third = await hub.next_assignment(run["id"])
    assert {second["task_id"], third["task_id"]} == {first["task_id"], other["id"]}
    assert len({first["worker_id"], second["worker_id"], third["worker_id"]}) == 3
    assert await hub.next_assignment(run["id"]) is None
    assert await hub.due_assignment_intents(run["id"]) == []


@pytest.mark.asyncio
async def test_unavailable_prerequisite_does_not_block_saved_independent_work(case):
    hub, _, run, _ = case
    await hub.next_assignment(run["id"])
    dependency = await command(hub, "artifact.register", {"kind": "software.input", "data": {"value": 1}})
    await command(hub, "artifact.accept", {"artifact_id": dependency["id"], "policy": "general.manual@1",
                                          "verdict": "accepted", "details": {}, "qualifications": []})
    blocked = await command(hub, "task.publish", {"contract": {
        "title": "Dependency-bound work", "objective": "Use the accepted input.", "priority": 0,
        "requirements": [{"key": "input", "artifact_id": dependency["id"], "policy": "general.manual@1"}]}})
    saved = await claim(hub, blocked, run)
    await recruitment(hub, saved, False)
    independent = await publish(hub, "Independent saved work")
    other = await claim(hub, independent, run)
    await recruitment(hub, other, False)

    await command(hub, "artifact.withdraw", {"artifact_id": dependency["id"], "reason": "The input is obsolete."})
    reconsidered = await hub.next_assignment(run["id"])
    assert reconsidered["worker_id"] == saved["worker_id"]
    assert reconsidered["task_id"] == blocked["id"]
    assert reconsidered["resume_reason"] == "artifact_withdrawn"
    resumed = await hub.next_assignment(run["id"])

    assert resumed["worker_id"] == other["worker_id"]
    assert resumed["task_id"] == independent["id"]
    assert (await hub.worker(saved["worker_id"]))["active_attempt"] == reconsidered["id"]
    assert (await hub.worker(saved["worker_id"]))["work_intention"] == "working"
    assert not await hub.db.call(lambda c: hub._requirement(c, blocked["contract"]["requirements"][0]))


@pytest.mark.asyncio
async def test_a_reviewer_is_never_the_author_and_retires_after_its_review(case):
    hub, task, run, _ = case
    author = await hub.next_assignment(run["id"])
    candidate, reviewer = await review_candidate(hub, author, run)
    assert reviewer["worker_id"] != author["worker_id"]
    assert (await hub.worker(reviewer["worker_id"]))["state"] == "retired"
    host = await hub.task(task["id"])
    assert reviewer["worker_id"] not in [item["worker_id"] for item in host["collaboration"]["members"]]
    assert host["state"] != "done"
    assert (await hub.artifact(candidate["artifact_id"]))["status"] == "accepted"


@pytest.mark.asyncio
async def test_completion_separate_gate_and_root_shutdown(case):
    hub, task, run, _ = case
    author = await hub.next_assignment(run["id"])
    candidate, _ = await review_candidate(hub, author, run)
    declaration = await command(hub, "task.complete.declare", {"task_id": task["id"], "revision": 1,
        "evidence": {"objective": {"artifact_ids": [candidate["artifact_id"]], "explanation": "The result satisfies the full original objective."}}}, actor(author))
    finalized = await command(hub, "task.complete.finalize", {"declaration_id": declaration["id"], "reason": "Every requirement is supported."}, actor(author))
    assert finalized["state"] == "finalizing"
    assert await hub.next_assignment(run["id"]) is None
    await quiet(hub, author)
    assert (await hub.task(task["id"]))["state"] == "done"
    assert (await hub.run(run["id"]))["state"] == "stopped"


@pytest.mark.asyncio
async def test_pending_required_artifact_prevents_completion(case):
    hub, task, run, _ = case
    author = await hub.next_assignment(run["id"])
    candidate, _ = await review_candidate(hub, author, run)
    await command(hub, "artifact.stage", file_payload(hub, author, data={"new": "pending"}), actor(author))
    with pytest.raises(HubError) as error:
        await command(hub, "task.complete.declare", {"task_id": task["id"], "revision": 1,
            "evidence": {"objective": {"artifact_ids": [candidate["artifact_id"]], "explanation": "Covers objective."}}}, actor(author))
    assert error.value.code == "ARTIFACT_GATE"


@pytest.mark.asyncio
async def test_shared_maintenance_cas_preserves_original_objective(case):
    hub, task, run, _ = case
    author = await hub.next_assignment(run["id"])
    await recruitment(hub, author)
    peer = await claim(hub, task, run, plan="Check conditions")
    revised = {**task["contract"], "context": "Execute in a different order."}
    result = await command(hub, "task.revise", {"task_id": task["id"], "revision": 1, "reason": "Clarify execution", "contract": revised}, actor(peer))
    assert result["revision"] == 2
    with pytest.raises(HubError) as error:
        await command(hub, "task.revise", {"task_id": task["id"], "revision": 1, "reason": "Stale edit", "contract": revised})
    assert error.value.code == "STALE_REVISION"
    assert (await hub.task(task["id"], 1))["contract"]["objective"] == task["contract"]["objective"]


@pytest.mark.asyncio
async def test_message_wake_preserves_wait_and_explicit_resume(case):
    hub, _, run, _ = case
    author = await hub.next_assignment(run["id"])
    await legacy_wait(hub, {"reason": "Blocked"}, actor(author))
    await quiet(hub, author)
    await hub.db.call(lambda c: hub._wake_for_message(c, author["worker_id"], "message-1"), write=True)
    wake = await hub.next_assignment(run["id"])
    assert wake["worker_id"] == author["worker_id"] and wake["execution_mode"] == "message"
    await hub.db.call(lambda c: hub._consume_message_wake(c, author["worker_id"], "message-1"), write=True)
    await hub.ordinary_turn_completed(wake["worker_id"], wake["id"])
    assert await hub.next_assignment(run["id"]) is None
    await hub.db.call(lambda c: hub._wake_for_message(c, author["worker_id"], "message-2"), write=True)
    wake = await hub.next_assignment(run["id"])
    await hub.db.call(lambda c: hub._consume_message_wake(c, author["worker_id"], "message-2"), write=True)
    await command(hub, "worker.resume_work", {}, actor(wake))
    await hub.ordinary_turn_completed(wake["worker_id"], wake["id"])
    assert (await hub.next_assignment(run["id"]))["worker_id"] == author["worker_id"]


@pytest.mark.asyncio
async def test_helper_usage_deduplicates_own_delta(case):
    hub, _, run, _ = case
    author = await hub.next_assignment(run["id"])
    await hub.ordinary_turn_completed(author["worker_id"], author["id"])
    helper = (await hub.due_recruitment_intents(run["id"]))[0]
    await command(hub, "helper.started", {"intent_id": helper["id"], "thread_id": "native-helper"}, RUNNER)
    payload = {"intent_id": helper["id"], "delta_tokens": 31, "sample_id": "turn:1", "sample": {"totalTokens": 90031}}
    await command(hub, "helper.usage", payload, RUNNER)
    await command(hub, "helper.usage", payload, RUNNER)
    assert (await hub.run(run["id"]))["tokens_used"] == 31
    assert json.loads((await hub.db.call(lambda c: c.execute("SELECT sample_json FROM helper_usage_samples").fetchone()))["sample_json"])["totalTokens"] == 90031


@pytest.mark.asyncio
async def test_completed_worker_message_turn_never_reopens_its_task(case):
    hub, root, run, _ = case
    await hub.next_assignment(run["id"])
    task = await publish(hub, "Finish this independent task")
    author = await claim(hub, task, run)
    candidate, _ = await review_candidate(hub, author, run)
    await finish_ordinary(hub, task["id"], actor(author), artifact_ids=[candidate["artifact_id"]])
    await quiet(hub, author)
    assert (await hub.task(task["id"]))["state"] == "done"
    await hub.db.call(lambda c: hub._wake_for_message(c, author["worker_id"], "completed-message"), write=True)
    wake = await hub.next_assignment(run["id"])
    assert wake["execution_mode"] == "message_only"
    assert (await hub.worker(author["worker_id"]))["current_task_id"] is None
    assert not (await hub.task(task["id"]))["active_attempts"]
    with pytest.raises(HubError) as forbidden:
        await command(hub, "task.pause", {"reason": "Reopen the old task"}, actor(wake))
    assert forbidden.value.code == "MESSAGE_ONLY_AUTHORITY"
    await hub.db.call(lambda c: hub._consume_message_wake(c, author["worker_id"], "completed-message"), write=True)
    await hub.ordinary_turn_completed(wake["worker_id"], wake["id"])
    assert (await hub.task(task["id"]))["state"] == "done"
    assert (await hub.task(root["id"]))["state"] == "claimed"
    assert (await hub.worker(author["worker_id"]))["work_intention"] == "idle"


@pytest.mark.asyncio
async def test_an_original_publisher_may_revise_its_task_from_a_message_turn(case):
    hub, root, run, _ = case
    await hub.next_assignment(run["id"])
    task = await publish(hub, "Finish this independent task")
    author = await claim(hub, task, run)
    published = await command(hub, "task.publish", {"contract": {
        "title": "A separate prerequisite", "objective": "Produce the prerequisite."}}, actor(author))
    candidate, _ = await review_candidate(hub, author, run)
    await finish_ordinary(hub, task["id"], actor(author), artifact_ids=[candidate["artifact_id"]])
    await quiet(hub, author)
    await hub.db.call(lambda c: hub._wake_for_message(c, author["worker_id"], "publisher-message"), write=True)
    wake = await hub.next_assignment(run["id"])
    assert wake["worker_id"] == author["worker_id"] and wake["execution_mode"] == "message_only"
    clarified = {"title": "A separate prerequisite", "objective": "Produce the prerequisite, stated exactly."}
    revision = {"task_id": published["id"], "revision": 1, "reason": "State the prerequisite exactly.",
                "contract": clarified}
    # Every other check of a revision still applies.
    for change, code in (({"revision": 2}, "STALE_REVISION"), ({"reason": " "}, "REASON_REQUIRED"),
                         ({"contract": {**clarified, "objective": "objective.md"}}, "CONTRACT_TEXT_REQUIRED"),
                         ({"contract": {**clarified, "domain": "lean"}}, "DOMAIN_MISMATCH")):
        with pytest.raises(HubError) as refused:
            await command(hub, "task.revise", {**revision, **change}, actor(wake))
        assert refused.value.code == code
    # Through the gateway, as the worker calls it: the message-only surface offers the operation.
    (hub.workspace(author["worker_id"]) / "clarified.json").write_text(json.dumps(clarified), encoding="utf-8")
    revised = await WorkerRunner(hub, ScriptedHarness)._tool(wake, "message-thread", "taskhub_call", {
        "operation": "propose_revision", "arguments": {
            **{key: revision[key] for key in ("task_id", "revision", "reason")}, "contract_path": "clarified.json"}},
        "revise-from-message")
    assert revised["revision"] == 2 and revised["contract"]["objective"] == clarified["objective"]
    # The task it executed, which an operator published, is revised only in a task turn.
    with pytest.raises(HubError) as forbidden:
        await command(hub, "task.revise", {**revision, "task_id": task["id"]}, actor(wake))
    assert forbidden.value.code == "MESSAGE_ONLY_AUTHORITY"
    assert "original publishers" in forbidden.value.message
    assert (await hub.task(task["id"]))["revision"] == 1


@pytest.mark.asyncio
async def test_never_assigned_source_handles_message_without_root_membership(case):
    hub, task, run, _ = case
    worker = await source(hub)
    await hub.db.call(lambda c: hub._wake_for_message(c, worker["id"], "new-source-message"), write=True)
    wake = await hub.next_assignment(run["id"])
    assert wake["worker_id"] == worker["id"] and wake["execution_mode"] == "message_only"
    assert (await hub.task(task["id"]))["state"] == "ready"
    assert (await hub.task(task["id"]))["collaboration"]["members"] == []
    assert (await hub.worker(worker["id"]))["current_task_id"] is None


@pytest.mark.asyncio
async def test_review_reassignment_needs_impossible_recovery_and_confirmed_quiescence(case):
    hub, _, run, _ = case
    author = await hub.next_assignment(run["id"])
    candidate = await command(hub, "artifact.stage", file_payload(hub, author, data={"candidate": 1}), actor(author))
    review = await select_review(hub, candidate["review_task_id"], run["id"])
    payload = {"task_id": review["task_id"], "worker_id": review["worker_id"], "recovery_impossible": True, "reason": "Native conversation is irrecoverably missing."}
    with pytest.raises(HubError) as unsafe:
        await command(hub, "review.reassign", payload)
    assert unsafe.value.code == "QUIESCENCE_REQUIRED"
    await command(hub, "review.reassign", {**payload, "process_confirmed_stopped": True})
    replacement = await select_review(hub, review["task_id"], run["id"])
    assert replacement["worker_id"] != review["worker_id"]
    assert (await hub.worker(review["worker_id"]))["state"] == "retired"
    assert (await hub.artifact(candidate["artifact_id"]))["status"] == "staged"
    assert len((await hub.task(review["task_id"]))["active_attempts"]) == 1


@pytest.mark.asyncio
async def test_explicit_withdrawal_closes_review_but_is_not_acceptance(case):
    hub, task, run, _ = case
    author = await hub.next_assignment(run["id"])
    accepted, _ = await review_candidate(hub, author, run)
    unnecessary = await command(hub, "artifact.stage", file_payload(hub, author, data={"optional": True}), actor(author))
    await command(hub, "artifact.withdraw", {"artifact_id": unnecessary["artifact_id"], "reason": "The accepted result already establishes the unchanged objective."}, actor(author))
    assert (await hub.artifact(unnecessary["artifact_id"]))["status"] == "withdrawn"
    assert (await hub.task(unnecessary["review_task_id"]))["state"] == "done"
    assert not (await hub.artifact(unnecessary["artifact_id"]))["peer_reviews"]
    declaration = await command(hub, "task.complete.declare", {"task_id": task["id"], "revision": 1,
        "evidence": {"objective": {"artifact_ids": [accepted["artifact_id"]], "explanation": "The exact accepted result fully establishes the original goal."}}}, actor(author))
    assert declaration["state"] == "pending"


@pytest.mark.asyncio
async def test_completion_cannot_discard_a_submission_racing_its_feedback(case):
    hub, task, run, _ = case
    author = await hub.next_assignment(run["id"])
    await open_collaboration(hub, task["id"])
    collaborator = await claim(hub, task, run, plan="Check remaining evidence")
    accepted, _ = await review_candidate(hub, author, run)
    declaration = await command(hub, "task.complete.declare", {"task_id": task["id"], "revision": 1,
        "evidence": {"objective": {"artifact_ids": [accepted["artifact_id"]], "explanation": "The reviewed result establishes the original goal."}}}, actor(author))
    await command(hub, "task.complete.feedback", {"declaration_id": declaration["id"], "conditions": [],
        "checkpoint": {"summary": "Saved collaborator work", "next_action": "Retain the remaining submitted contribution"}}, actor(collaborator))
    await command(hub, "artifact.submit", file_payload(hub, collaborator, data={"required_addendum": True}), actor(collaborator))
    with pytest.raises(HubError) as pending:
        await command(hub, "task.complete.finalize", {"declaration_id": declaration["id"], "reason": "All old references remain accepted."}, actor(author))
    assert pending.value.code == "ARTIFACT_GATE"
    submitted = await quiet(hub, collaborator)
    assert submitted["artifact_id"]
    assert (await hub.task(task["id"]))["state"] != "done"


@pytest.mark.asyncio
async def test_controlled_first_turn_contribution_creates_one_recruitment_choice(case):
    hub, task, run, _ = case
    author = await hub.next_assignment(run["id"])
    # As the runner does, bind the worker's thread before reserving a turn on it.
    await command(hub, "worker.bind_session", {"worker_id": author["worker_id"], "thread_id": "first-contribution-thread",
                  "codex_profile": hub.config.codex.model_dump(mode="json")}, RUNNER)
    intent = await command(hub, "turn.reserve", {"worker_id": author["worker_id"]}, RUNNER)
    await command(hub, "turn.bind", {"intent_id": intent["id"], "remote_turn_id": "first-contribution-turn"}, RUNNER)
    await command(hub, "artifact.submit", file_payload(hub, author, data={"summary": "A first-turn contribution"}), actor(author))
    await command(hub, "turn.finish", {"intent_id": intent["id"], "status": "interrupted",
        "terminal_observed": True, "process_quiet": True}, RUNNER)
    await quiet(hub, author)
    helpers = await hub.due_recruitment_intents(run["id"])
    assert len(helpers) == 1 and helpers[0]["source_turn_id"] == "first-contribution-turn"
    assert (await hub.task(task["id"]))["collaboration"]["recruitment"]["first_turn_id"] == "first-contribution-turn"
    await hub.record_first_turn(author["worker_id"], task["id"], "first-contribution-turn")
    assert len(await hub.due_recruitment_intents(run["id"])) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("submitted_before_shutdown", [False, True])
async def test_root_shutdown_preserves_unfinished_worker_and_committed_submission(case, submitted_before_shutdown):
    hub, root, run, _ = case
    root_worker = await hub.next_assignment(run["id"])
    unfinished = await publish(hub, "Retain independent unfinished work")
    other = await claim(hub, unfinished, run)
    path = hub.workspace(other["worker_id"]) / "saved-progress.txt"
    path.write_text("Unfinished original work survives shutdown.", encoding="utf-8")
    await command(hub, "worker.bind_session", {"worker_id": other["worker_id"], "thread_id": "retained-native-thread",
                  "codex_profile": hub.config.codex.model_dump(mode="json")}, RUNNER)
    accepted, _ = await review_candidate(hub, root_worker, run)
    if submitted_before_shutdown:
        await command(hub, "artifact.submit", file_payload(hub, other, data={"pending": "retain exact submission"}), actor(other))
    await finish_ordinary(hub, root["id"], actor(root_worker), artifact_ids=[accepted["artifact_id"]])
    await quiet(hub, root_worker)
    outcome = await quiet(hub, other)
    assert (await hub.run(run["id"]))["state"] == "stopped"
    assert (await hub.task(unfinished["id"]))["state"] != "done"
    assert path.read_text(encoding="utf-8") == "Unfinished original work survives shutdown."
    if submitted_before_shutdown:
        assert outcome["directive"] == "verify"
        assert (await hub.artifact(outcome["artifact_id"]))["status"] == "staged"
    resumed_run = await command(hub, "run.start")
    await resolve_recruitment(hub, resumed_run["id"])
    resumed = await hub.next_assignment(resumed_run["id"])
    assert resumed["worker_id"] == other["worker_id"] and resumed["task_id"] == unfinished["id"]
    assert (await hub.worker(other["worker_id"]))["thread_id"] == "retained-native-thread"
