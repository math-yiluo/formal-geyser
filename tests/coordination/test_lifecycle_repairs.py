"""Regression checks for revised waits, lost maintainers and exact provenance."""
from __future__ import annotations

from uuid import uuid4

import pytest

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Command, Role
from tests.coordination.lifecycle_helpers import (
    assignment_decision,
    idle_worker,
    open_collaboration,
    select_review,
    start_helper,
)
from tests.coordination.publication_helpers import approve, file_payload, new_hub
from tests.coordination.test_interruptions import interrupted

from .wait_helpers import legacy_wait

OP = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)


async def command(hub, name, payload=None, actor=OP):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


def worker(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                 fencing_token=attempt["fencing_token"])


async def quiet(hub, attempt):
    return await command(hub, "runtime.quiescent", {"worker_id": attempt["worker_id"],
                         "snapshot": hub.blobs.snapshot(hub.workspace(attempt["worker_id"]))}, RUNNER)


async def publish(hub, title, **fields):
    return await command(hub, "task.publish", {"contract": {
        "title": title, "objective": "Retain the verified result.", **fields}})


async def claim(hub, task, run, *, worker_id=None, plan=""):
    if worker_id is None:
        worker_id = (await command(hub, "worker.register"))["id"]
    return await command(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"],
                         "worker_id": worker_id, "run_id": run["id"], "plan": plan})


async def revise(hub, task):
    return await command(hub, "task.revise", {"task_id": task["id"], "revision": task["revision"],
                         "contract": {**task["contract"], "payload": {"approach": "Use the newly clarified approach."}},
                         "reason": "Change what the task asks for while retaining its acceptance conditions."})


async def accept_candidate(hub, candidate, run):
    await command(hub, "artifact.accept", {"artifact_id": candidate["artifact_id"], "policy": "general.peer@1",
                  "verdict": "accepted", "details": {}, "qualifications": []})
    await approve(hub, await select_review(hub, candidate["review_task_id"], run["id"]))


@pytest.fixture
async def case(tmp_path_factory):
    clock = [10_000]
    hub = await new_hub(tmp_path_factory.mktemp("life"), clock=lambda: clock[0])
    task = await publish(hub, "Task 0")
    run = await command(hub, "run.start")
    attempt = await hub.next_assignment(run["id"])
    yield hub, task, run, attempt, clock
    await hub.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("already_quiet", [True, False])
async def test_revision_reconsiders_saved_wait_without_satisfying_dependency(case, already_quiet):
    hub, task, run, attempt, _ = case
    dependency = await publish(hub, "Unfinished prerequisite")
    predicate = {"mode": "all", "requirements": [{"key": "result", "task_id": dependency["id"],
                 "task_revision": 1, "policy": "general.peer@1"}]}
    checkpoint = {"summary": "The required result is still missing.", "next_action": "Reconsider the revised task."}
    waiting = await legacy_wait(hub, {"reason": "Blocked", "checkpoint": checkpoint,
                            "predicate": predicate}, worker(attempt))
    if already_quiet:
        await quiet(hub, attempt)
    await revise(hub, task)
    if not already_quiet:
        # The unfinished prerequisite gets a new worker at once, but the saved wait is not
        # resumed before its own execution is quiet.
        other = await hub.next_assignment(run["id"])
        assert other["task_id"] == dependency["id"] and other["worker_id"] != attempt["worker_id"]
        assert await hub.next_assignment(run["id"]) is None
        await quiet(hub, attempt)
    tickets = await hub.db.call(lambda c: [dict(row) for row in c.execute(
        "SELECT * FROM continuations WHERE task_id=? ORDER BY created_at,rowid", (task["id"],))])
    assert tickets[0]["id"] == waiting["continuation_id"] and tickets[0]["state"] == "superseded"
    assert tickets[1]["task_revision"] == 2 and tickets[1]["state"] == "reconsider"
    assert tickets[1]["predicate_json"] == tickets[0]["predicate_json"]
    assert not await hub.db.call(lambda c: hub._satisfied(c, predicate, waiter=None))
    resumed = await hub.next_assignment(run["id"])
    assert resumed["worker_id"] == attempt["worker_id"] and resumed["task_revision"] == 2
    assert resumed["resume_reason"] == "task_revision_changed"
    assert resumed["checkpoint"]["summary"] == checkpoint["summary"]
    assert (await hub.worker(attempt["worker_id"]))["work_intention"] == "working"
    assert (await hub.task(task["id"]))["collaboration"]["recruitment"]["opportunity_state"] == "cancelled"


@pytest.mark.asyncio
async def test_departure_ends_continuation_and_makes_the_worker_idle_for_new_work(case):
    hub, task, run, attempt, _ = case
    await open_collaboration(hub, task["id"])
    await claim(hub, task, run, plan="Preserve the remaining execution.")
    dependency = await publish(hub, "Independent prerequisite")
    waiting = await legacy_wait(hub, {"reason": "Waiting for prerequisite", "predicate": {
        "requirements": [{"key": "result", "task_id": dependency["id"], "task_revision": 1,
                          "policy": "general.peer@1"}]}}, worker(attempt))
    await quiet(hub, attempt)
    await command(hub, "task.leave", {"task_id": task["id"], "worker_id": attempt["worker_id"],
                  "permanent": True, "reason": "This executor permanently leaves its saved work."})
    revised = await revise(hub, dependency)
    ticket = await hub.db.call(lambda c: hub._get(c, "continuations", waiting["continuation_id"]))
    assert ticket["state"] == "membership_ended"
    # The departed worker has nothing left to resume: it is idle and offered the open task.
    decision = await assignment_decision(hub, run["id"])
    assert decision["source_task_id"] == dependency["id"]
    assert [item["worker_id"] for item in decision["candidates"]["workers"]] == [attempt["worker_id"]]
    result = await hub.complete_assignment(decision["id"], attempt["worker_id"])
    assert result["outcome"] == "idle_worker"
    assert result["assignment"]["worker_id"] == attempt["worker_id"]
    assert result["assignment"]["task_id"] == dependency["id"]
    assert result["assignment"]["task_revision"] == revised["revision"]
    assert (await hub.task(task["id"]))["state"] != "retired"


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["revise", "leave"])
async def test_parked_interruption_is_reconsidered_or_terminated(tmp_path_factory, transition):
    hub = await new_hub(tmp_path_factory.mktemp("park"))
    try:
        _, task, original_worker, _, payload = await interrupted(hub)
        parked = await command(hub, "runtime.park_interrupted", payload, RUNNER)
        if transition == "revise":
            await revise(hub, task)
        else:
            await command(hub, "task.leave", {"task_id": task["id"], "worker_id": original_worker["id"],
                          "permanent": True, "reason": "The original member permanently leaves."})
        new_run = await command(hub, "run.start")
        ticket = await hub.db.call(lambda c: hub._get(c, "interruption_checkpoints", parked["interruption_id"]))
        assert ticket["state"] == ("superseded" if transition == "revise" else "membership_ended")
        if transition == "revise":
            resumed = await hub.next_assignment(new_run["id"])
            assert resumed["worker_id"] == original_worker["id"] and resumed["task_revision"] == 2
            assert resumed["resume_reason"] == "task_revision_changed"
        else:
            assert await hub.next_assignment(new_run["id"]) is None
            independent = await publish(hub, "Independent work after departure")
            # The departed member is idle again: the decision for new work offers it.
            decision = await assignment_decision(hub, new_run["id"])
            assert decision["source_task_id"] == independent["id"]
            assert [item["worker_id"] for item in decision["candidates"]["workers"]] == [original_worker["id"]]
    finally:
        await hub.close()


@pytest.mark.asyncio
async def test_prepared_decision_follows_new_run_without_replacing_intent(case):
    hub, _, run, _, _ = case
    await idle_worker(hub, run, "Earlier task")
    await publish(hub, "Other work")
    original = await assignment_decision(hub, run["id"])
    await command(hub, "run.stop", {"run_id": run["id"]})
    new_run = await command(hub, "run.start")
    due = await hub.due_assignment_intents(new_run["id"])
    assert len(due) == 1 and due[0]["id"] == original["id"]
    assert due[0]["run_id"] == new_run["id"]
    started = await start_helper(hub, original["id"], "synthetic-decision-thread")
    assert started["state"] == "running"
    assert (await hub.run(new_run["id"]))["turns_used"] == 1
    assert (await hub.run(run["id"]))["turns_used"] == 0


@pytest.mark.asyncio
async def test_catalog_hold_quarantines_one_worker_and_preserves_unrelated_execution(case):
    hub, task, run, attempt, _ = case
    await command(hub, "worker.bind_session", {"worker_id": attempt["worker_id"],
                  "thread_id": "retained-incompatible-thread", "codex_profile": hub.config.codex.model_dump(mode="json")}, RUNNER)
    progress = hub.workspace(attempt["worker_id"]) / "progress.txt"
    progress.write_text("Retained original conversation work.", encoding="utf-8")
    other = await publish(hub, "Independent modern work")
    held = await command(hub, "runtime.catalog_hold", {"worker_id": attempt["worker_id"], "attempt_id": attempt["id"],
                         "reason": "This original native catalog requires explicit migration."}, RUNNER)
    assert held["directive"] == "catalog_held"
    saved = await hub.worker(attempt["worker_id"])
    assert saved["state"] == "recovery_required" and saved["thread_id"] == "retained-incompatible-thread"
    assert saved["current_task_id"] == task["id"] and saved["active_attempt"] is None
    assert progress.read_text(encoding="utf-8") == "Retained original conversation work."
    assert (await hub.run(run["id"]))["state"] == "running"
    # The held worker is not idle: unrelated work gets a new worker at once, without a decision.
    selected = await hub.next_assignment(run["id"])
    assert selected["task_id"] == other["id"] and selected["worker_id"] != attempt["worker_id"]
    later = await publish(hub, "Further independent work")
    fresh = await hub.next_assignment(run["id"])
    assert fresh["task_id"] == later["id"]
    assert fresh["worker_id"] not in {attempt["worker_id"], selected["worker_id"]}
    assert await hub.due_assignment_intents(run["id"]) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("inference_uncertain", [False, True])
async def test_helper_hold_distinguishes_local_result_failure_from_unknown_inference(case, inference_uncertain):
    hub, _, run, _, _ = case
    await idle_worker(hub, run, "Earlier task")
    await publish(hub, "Independent assignment choice")
    helper = await assignment_decision(hub, run["id"])
    await start_helper(hub, helper["id"], "retained-helper-thread")
    held = await hub.helper_uncertain(helper["id"], "The retained helper result could not be decoded.",
                                     inference_uncertain=inference_uncertain)
    assert held["state"] == "uncertain" and held["thread_id"] == "retained-helper-thread"
    current = await hub.run(run["id"])
    assert current["state"] == "running"
    assert bool(current["usage_uncertain"]) is inference_uncertain
    if not inference_uncertain:
        # The held decision is not replayed, and no second decision replaces it.
        assert await hub.next_assignment(run["id"]) is None
        assert await hub.due_assignment_intents(run["id"]) == []
        independent = await publish(hub, "Other runnable work")
        peer = await claim(hub, independent, run)
        assert peer["task_id"] == independent["id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("control,state", [("run.pause", "paused"), ("run.stop", "stopped")])
async def test_unknown_helper_usage_is_retained_when_run_control_races(case, control, state):
    hub, _, run, _, _ = case
    await idle_worker(hub, run, "Earlier task")
    await publish(hub, "Other assignment work")
    helper = await assignment_decision(hub, run["id"])
    await start_helper(hub, helper["id"], "ambiguous-helper-thread")
    await command(hub, control, {"run_id": run["id"]})
    await hub.helper_uncertain(helper["id"], "Inference transport became uncertain after control changed.")
    result = await hub.run(run["id"])
    assert result["state"] == state and result["usage_uncertain"]


@pytest.mark.asyncio
async def test_workspace_conflict_holds_only_one_member_and_preserves_original_recovery(case):
    hub, task, run, first, _ = case
    await open_collaboration(hub, task["id"])
    second = await claim(hub, task, run, plan="Continue the independent part.")
    await command(hub, "worker.bind_session", {"worker_id": first["worker_id"], "thread_id": "retained-original-thread",
                  "codex_profile": hub.config.codex.model_dump(mode="json")}, RUNNER)
    private = hub.workspace(first["worker_id"]) / "draft.txt"
    private.write_text("Unpublished mathematical draft remains intact.", encoding="utf-8")
    await hub.ordinary_turn_completed(first["worker_id"], first["id"])
    await hub.ordinary_turn_completed(second["worker_id"], second["id"])
    report = hub.workspace(first["worker_id"]) / ".private/repository-refresh.json"
    original_report = report.read_bytes()
    report.write_text("{invalid local metadata", encoding="utf-8")
    before = await hub.db.call(lambda c: c.execute("SELECT count(*) FROM attempts WHERE worker_id=?", (first["worker_id"],)).fetchone()[0])
    admitted = await hub.next_assignment(run["id"])
    assert admitted["worker_id"] == second["worker_id"]
    held = await hub.worker(first["worker_id"])
    assert held["state"] == "recovery_required" and held["active_attempt"] is None
    assert held["thread_id"] == "retained-original-thread" and held["current_task_id"] == task["id"]
    assert (await hub.run(run["id"]))["state"] == "running"
    assert await hub.db.call(lambda c: c.execute("SELECT count(*) FROM attempts WHERE worker_id=?", (first["worker_id"],)).fetchone()[0]) == before
    assert private.read_text(encoding="utf-8") == "Unpublished mathematical draft remains intact."
    assert report.read_text(encoding="utf-8") == "{invalid local metadata"
    report.write_bytes(original_report)
    await command(hub, "worker.recover", {"worker_id": first["worker_id"], "process_confirmed_stopped": True,
                  "reason": "Repaired the local refresh report; the original native process is stopped."})
    resumed = await hub.next_assignment(run["id"])
    assert resumed["worker_id"] == first["worker_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("activation", ["continuation", "message"])
async def test_workspace_failure_preserves_resume_or_message_and_dispatches_other_work(case, activation):
    hub, task, run, first, clock = case
    await legacy_wait(hub, {"reason": "Retain saved work."}, worker(first))
    await quiet(hub, first)
    if activation == "continuation":
        await revise(hub, task)
    else:
        await hub.db.call(lambda c: hub._wake_for_message(c, first["worker_id"], "retained-message"), write=True)
    other = await publish(hub, "Unrelated work remains possible")
    report = hub.workspace(first["worker_id"]) / ".private/repository-refresh.json"
    report.write_text("bad local metadata", encoding="utf-8")
    clock[0] += 3_600_001
    admitted = await hub.next_assignment(run["id"])
    assert admitted["task_id"] == other["id"]
    assert (await hub.worker(first["worker_id"]))["state"] == "recovery_required"
    tickets = await hub.db.call(lambda c: [dict(row) for row in c.execute("SELECT * FROM continuations WHERE preferred_worker=?", (first["worker_id"],))])
    assert all(ticket["resume_attempt"] is None for ticket in tickets)
    if activation == "message":
        assert (await hub.worker(first["worker_id"]))["pending_wake"]
    assert await hub.next_assignment(run["id"]) is None
    assert (await hub.run(run["id"]))["state"] == "running"


@pytest.mark.asyncio
async def test_workspace_hold_while_assigning_an_idle_worker_recovers_without_task_membership(case):
    hub, task, run, first, _ = case
    await legacy_wait(hub, {"reason": "Save work before permanent departure."}, worker(first))
    await quiet(hub, first)
    await command(hub, "task.leave", {"task_id": task["id"], "worker_id": first["worker_id"],
                  "permanent": True, "reason": "The executor permanently leaves."})
    other = await publish(hub, "New independent task")
    helper = await assignment_decision(hub, run["id"])
    assert [item["worker_id"] for item in helper["candidates"]["workers"]] == [first["worker_id"]]
    report = hub.workspace(first["worker_id"]) / ".private/repository-refresh.json"
    original_report = report.read_bytes()
    report.write_text("bad local metadata", encoding="utf-8")
    result = await hub.complete_assignment(helper["id"], first["worker_id"])
    assert result["assignment"] is None and result["outcome"] == "claim_failed"
    assert result["reason"] == "WORKER_WORKSPACE_UNAVAILABLE"
    held = await hub.worker(first["worker_id"])
    assert held["state"] == "recovery_required" and held["current_task_id"] is None
    assert (await hub.task(other["id"]))["collaboration"]["members"] == []
    report.write_bytes(original_report)
    await command(hub, "worker.recover", {"worker_id": first["worker_id"], "process_confirmed_stopped": True,
                  "reason": "The repaired local workspace can take new work again."})
    assert (await hub.worker(first["worker_id"]))["work_intention"] == "idle"
    # The recovered worker is idle again: the next decision for the open task offers it.
    fresh = await assignment_decision(hub, run["id"])
    assert fresh["id"] != helper["id"] and fresh["source_task_id"] == other["id"]
    assert [item["worker_id"] for item in fresh["candidates"]["workers"]] == [first["worker_id"]]
    result = await hub.complete_assignment(fresh["id"], first["worker_id"])
    assert result["outcome"] == "idle_worker" and result["assignment"]["worker_id"] == first["worker_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("chosen", [True, False])
async def test_changed_candidate_claim_rolls_back_and_finishes_the_decision(case, monkeypatch, chosen):
    hub, _, run, _, _ = case
    idle = await idle_worker(hub, run, "Earlier task")
    other = await publish(hub, "Assigned work")
    helper = await assignment_decision(hub, run["id"])
    original_claim = hub._claim

    def changed(c, task_id, *args, **kwargs):
        if task_id == other["id"]:
            raise HubError("INPUTS_CHANGED", "An exact dependency changed after advertisement")
        return original_claim(c, task_id, *args, **kwargs)

    monkeypatch.setattr(hub, "_claim", changed)
    before = len(await hub.list_workers())
    # The offered idle worker, or none: then the failed claim must not leave a new worker behind.
    result = await hub.complete_assignment(helper["id"], idle["display_name"] if chosen else None)
    assert result["assignment"] is None and result["outcome"] == "claim_failed"
    if chosen:
        assert result["reason"] == "INPUTS_CHANGED"
    assert len(await hub.list_workers()) == before
    assert (await hub.db.call(lambda c: hub._helper(c, helper["id"])))["state"] == "completed"
    # The rolled-back claim also keeps the idle worker's conversation and idleness.
    current = await hub.worker(idle["id"])
    assert current["thread_id"] == idle["thread_id"] and current["work_intention"] == "idle"


@pytest.mark.asyncio
async def test_fresh_candidate_failure_does_not_block_other_work_or_consume_birth(case, monkeypatch):
    hub, _, run, _, clock = case
    stale = await publish(hub, "Stale opportunity")
    clock[0] += 1
    other = await publish(hub, "Independent opportunity")
    clock[0] += 3_600_001
    original_claim = hub._claim

    def changed(c, task_id, *args, **kwargs):
        if task_id == stale["id"]:
            raise HubError("INPUTS_CHANGED", "The input changed at admission")
        return original_claim(c, task_id, *args, **kwargs)

    monkeypatch.setattr(hub, "_claim", changed)
    before = len(await hub.list_workers())
    result = await hub.next_assignment(run["id"])
    assert result["task_id"] == other["id"]
    assert len(await hub.list_workers()) == before + 1
    assert (await hub.task(stale["id"]))["active_attempt"] is None


@pytest.mark.asyncio
async def test_revised_replacement_retains_authorship_and_required_artifact_gate(case):
    hub, task, run, first, clock = case
    await open_collaboration(hub, task["id"])
    second = await claim(hub, task, run, plan="Contribute a separate part of the result.")
    original = await command(hub, "artifact.stage", file_payload(hub, first,
        data={"result": "A retained result"}, coauthors=[second["worker_id"]]), worker(first))
    clock[0] += 1
    revised = await revise(hub, task)
    await quiet(hub, first)
    await quiet(hub, second)
    clock[0] += 1
    resumed = await claim(hub, revised, run, worker_id=second["worker_id"])
    replacement = await command(hub, "artifact.stage", file_payload(hub, resumed,
        data={"result": "The corrected retained result"}, replaces=original["artifact_id"]), worker(resumed))
    assert (await hub.artifact(original["artifact_id"]))["status"] == "superseded"
    assert (await hub.task(original["review_task_id"]))["collaboration"]["review"]["state"] == "invalidated"
    assert not await hub.db.call(lambda c: hub._can_review(c, replacement["review_task_id"], first["worker_id"]))
    with pytest.raises(HubError) as blocked:
        await hub.db.call(lambda c: hub._artifact_gate(c, task["id"]))
    assert blocked.value.code == "ARTIFACT_GATE" and replacement["artifact_id"] in blocked.value.message
    await accept_candidate(hub, replacement, run)
    await hub.db.call(lambda c: hub._artifact_gate(c, task["id"]))
    assert (await hub.task(task["id"]))["state"] != "done"


@pytest.mark.asyncio
async def test_lost_declarer_cancellation_requires_evidence_then_fresh_declaration(case):
    hub, task, run, first, _ = case
    candidate = await command(hub, "artifact.stage", file_payload(hub, first, data={"result": "Accepted result"}), worker(first))
    await accept_candidate(hub, candidate, run)
    payload = {"task_id": task["id"], "revision": 1, "evidence": {
        "objective": {"artifact_ids": [candidate["artifact_id"]], "explanation": "The independently checked result establishes the goal."}}}
    declaration = await command(hub, "task.complete.declare", payload, worker(first))
    recovery = {"declaration_id": declaration["id"], "recovery_impossible": True,
                "process_confirmed_stopped": True, "reason": "Synthetic lost conversation; no native process remains."}
    with pytest.raises(HubError) as active:
        await command(hub, "task.complete.cancel", recovery)
    assert active.value.code == "QUIESCENCE_REQUIRED"
    await legacy_wait(hub, {"reason": "Retain declarer work before recovery"}, worker(first))
    await quiet(hub, first)
    with pytest.raises(HubError) as missing:
        await command(hub, "task.complete.cancel", {"declaration_id": declaration["id"]})
    assert missing.value.code == "QUIESCENCE_REQUIRED"
    with pytest.raises(HubError):
        await command(hub, "task.complete.cancel", recovery, RUNNER)
    cancelled = await command(hub, "task.complete.cancel", recovery)
    assert cancelled["state"] == "cancelled" and cancelled["resolution"]["recovery"]["snapshot"]
    assert (await hub.task(task["id"]))["state"] != "done"
    fresh = await command(hub, "task.complete.declare", payload)
    assert fresh["id"] != declaration["id"] and fresh["state"] == "pending"
    assert fresh["evidence"] == cancelled["evidence"]


async def test_post_claim_checkout_failure_preserves_wait_and_admits_other_work(case, monkeypatch):
    hub, task, run, first, clock = case
    await legacy_wait(hub, {"reason": "Retain the original checkpoint."}, worker(first))
    await quiet(hub, first)
    await revise(hub, task)
    other = await publish(hub, "Other work remains possible during a disk fault")
    original = hub._refresh_repository_files
    def fail_one(worker_id, *args):
        if worker_id == first["worker_id"]:
            raise OSError("Synthetic checkout I/O failure")
        return original(worker_id, *args)
    monkeypatch.setattr(hub, "_refresh_repository_files", fail_one)
    clock[0] += 3_600_001
    admitted = await hub.next_assignment(run["id"])
    assert admitted["task_id"] == other["id"]
    tickets = await hub.db.read(lambda c: [dict(row) for row in c.execute(
        "SELECT * FROM continuations WHERE preferred_worker=?", (first["worker_id"],))])
    assert all(ticket["resume_attempt"] is None for ticket in tickets)
    assert (await hub.worker(first["worker_id"]))["state"] == "recovery_required"
    monkeypatch.setattr(hub, "_refresh_repository_files", original)
    clock[0] += 30_001
    await hub.db.write(lambda c: hub._retry_admission_failures(c, run["id"]))
    resumed = await hub.next_assignment(run["id"])
    assert resumed["worker_id"] == first["worker_id"]
