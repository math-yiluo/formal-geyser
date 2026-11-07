"""Infrastructure errors preserve agency and automatically continue stopped work."""
from __future__ import annotations

import asyncio
import json

import pytest

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Command, Role
from formal_geyser.coordination.runtime import (
    ScriptedHarness,
    ScriptedTurn,
    WorkerRunner,
)
from formal_geyser.coordination.runtime.codex import TurnOutcome
from tests.coordination.lifecycle_helpers import assignment_decision, idle_worker
from tests.coordination.test_fault_isolation_policy import execute
from tests.coordination.test_fault_isolation_policy import peers as peers
from tests.coordination.test_runtime_failure_isolation import runtime as runtime


async def assignment_intent(hub, run):
    """The prepared assignment decision for the open task, offering one idle worker."""
    idle = await idle_worker(hub, run, "Earlier task")
    intent = await assignment_decision(hub, run["id"])
    assert [item["worker_id"] for item in intent["candidates"]["workers"]] == [idle["id"]]
    return intent


async def recruitment_intent(hub, run):
    """The prepared recruitment decision of a new task whose first executor finished its first turn."""
    task = await execute(hub, "task.publish", {"contract": {
        "title": "Separable work", "objective": "Exercise decision recovery."}})
    worker = await execute(hub, "worker.register")
    attempt = await execute(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"],
                                                "worker_id": worker["id"], "run_id": run["id"]}, runner=True)
    await hub.ordinary_turn_completed(worker["id"], attempt["id"])
    due = await hub.due_recruitment_intents(run["id"])
    [intent] = [item for item in due if item["source_task_id"] == task["id"]]
    return intent


async def test_accounting_regression_never_removes_execution_authority(peers):
    hub, run, (attempt, _) = peers
    for total in (100, 90, 110):
        await execute(hub, "usage.record", {"worker_id": attempt["worker_id"],
            "sample": {"totalTokens": total}}, runner=True)
    assert (await hub.worker(attempt["worker_id"]))["state"] == "running"
    assert (await hub.run(run["id"]))["usage_uncertain"] == 1
    assert (await hub.run(run["id"]))["tokens_used"] == 110
    actor = Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                  fencing_token=attempt["fencing_token"])
    assert (await hub.authorize_worker(actor))["id"] == attempt["id"]
    result = await hub.execute(Command(name="task.checkpoint", key="progress-with-accounting-warning", payload={
        "checkpoint": {"summary": "Independent work continued", "next_action": "Continue investigation", "input_refs": []}}), actor)
    assert result["checkpoint"]["summary"] == "Independent work continued"


async def test_runtime_errors_cannot_issue_global_run_control(peers):
    hub, run, _ = peers
    for name in ("run.pause", "run.stop", "run.resume", "run.complete"):
        with pytest.raises(HubError) as denied:
            await execute(hub, name, {"run_id": run["id"]}, runner=True)
        assert denied.value.code == "FORBIDDEN"
    assert (await hub.run(run["id"]))["state"] == "running"
    await execute(hub, "run.pause", {"run_id": run["id"]})
    assert (await hub.run(run["id"]))["state"] == "paused"


async def test_unknown_closed_execution_continues_without_replaying_or_claiming_success(runtime, monkeypatch):
    hub, run = runtime
    first = ScriptedHarness([ScriptedTurn([], status="unknown")])
    result = await WorkerRunner(hub, lambda: first).run_once(run["id"])
    worker_id = result["worker_id"]
    worker = await hub.worker(worker_id)
    intents = await hub.db.call(lambda c: [dict(r) for r in c.execute("SELECT * FROM turn_intents")])
    assert len(intents) == 1 and intents[0]["state"] == "closed_unknown"
    assert (await hub.run(run["id"]))["usage_uncertain"] == 1
    now = hub.clock()
    monkeypatch.setattr(hub, "clock", lambda: now + 61000)
    assert await hub.recover_quiet_runtime_failures(run["id"]) == [worker_id]
    second = ScriptedHarness([ScriptedTurn([])])
    next_result = await WorkerRunner(hub, lambda: second).run_once(run["id"])
    assert next_result["worker_id"] == worker_id
    assert (await hub.worker(worker_id))["thread_id"] == worker["thread_id"]
    assert len(first.prompts) == len(second.prompts) == 1
    assert first.prompts[0] != second.prompts[0]
    assert "do not blindly repeat" in second.prompts[0]
    retained = await hub.db.call(lambda c: dict(c.execute("SELECT * FROM turn_intents WHERE id=?", (intents[0]["id"],)).fetchone()))
    assert retained["state"] == "closed_unknown"
    assert (await hub.run(run["id"]))["usage_uncertain"] == 1
    proof = await hub.db.call(lambda c: json.loads(c.execute("SELECT payload_json FROM events "
        "WHERE kind='turn.execution_closed' AND object_id=?", (intents[0]["id"],)).fetchone()[0]))
    assert proof["process_quiet"] and not proof["terminal_observed"] and not proof["previous_turn_replayed"]


@pytest.mark.parametrize("kind", ["runtime.preflight_hold", "runtime.catalog_hold"])
async def test_preflight_errors_retry_without_manual_unlock(peers, monkeypatch, kind):
    hub, run, (attempt, _) = peers
    await execute(hub, kind, {"worker_id": attempt["worker_id"], "attempt_id": attempt["id"],
                             "reason": "Temporary local preparation error"}, runner=True)
    now = hub.clock()
    monkeypatch.setattr(hub, "clock", lambda: now + 61000)
    assert await hub.recover_quiet_runtime_failures(run["id"]) == [attempt["worker_id"]]
    assert (await hub.worker(attempt["worker_id"]))["work_intention"] == "working"


async def test_a_claim_held_by_a_pause_waits_as_before_and_resumes_without_recovery(peers, monkeypatch):
    hub, run, (attempt, _) = peers
    await execute(hub, "run.pause_after_turn", {"run_id": run["id"]})
    held = await execute(hub, "runtime.preflight_hold", {"worker_id": attempt["worker_id"], "attempt_id": attempt["id"],
                         "reason": "Run control changed before any native process or model turn started"}, runner=True)
    assert held["directive"] == "preflight_held"
    worker = await hub.worker(attempt["worker_id"])
    # Not a recovery: the worker has its saved work, which waits while the run is paused.
    assert (worker["state"], worker["work_intention"], worker["active_attempt"]) == ("idle", "working", None)
    assert await hub.next_assignment(run["id"]) is None
    await execute(hub, "run.resume", {"run_id": run["id"]})
    now = hub.clock()
    monkeypatch.setattr(hub, "clock", lambda: now + 61000)
    assert await hub.recover_quiet_runtime_failures(run["id"]) == []  # Nothing failed: nothing to recover.
    resumed = await hub.next_assignment(run["id"])
    assert (resumed["worker_id"], resumed["task_id"]) == (attempt["worker_id"], attempt["task_id"])


async def test_a_waiting_workers_message_held_by_a_pause_is_handled_after_resume(peers):
    hub, run, (attempt, _) = peers
    await hub.db.call(lambda c: (
        c.execute("UPDATE attempts SET execution_mode='message' WHERE id=?", (attempt["id"],)),
        c.execute("UPDATE workers SET work_intention='waiting',execution_mode='message' WHERE id=?",
                  (attempt["worker_id"],))), write=True)
    await execute(hub, "run.pause_after_turn", {"run_id": run["id"]})
    await execute(hub, "runtime.preflight_hold", {"worker_id": attempt["worker_id"], "attempt_id": attempt["id"],
                  "reason": "Run control changed before any native process or model turn started"}, runner=True)
    worker = await hub.worker(attempt["worker_id"])
    assert (worker["state"], worker["work_intention"], worker["pending_wake"]) == ("suspended", "waiting", 1)


async def test_a_claimed_assignment_starts_while_the_driver_is_selecting(runtime):
    hub, run = runtime
    assignment = await hub.next_assignment(run["id"])
    runner = WorkerRunner(hub, lambda: ScriptedHarness([ScriptedTurn([])]))
    async with runner._admission_lock:  # The driver is selecting more work meanwhile.
        result = await asyncio.wait_for(runner.run_once(run["id"], assignment=assignment), 60)
    assert result["worker_id"] == assignment["worker_id"]


async def test_failed_decision_is_decided_again_after_quiet(runtime, monkeypatch):
    hub, run = runtime
    intent = await assignment_intent(hub, run)
    failed = ScriptedHarness([ScriptedTurn([], status="unknown")])
    with pytest.raises(Exception):
        await WorkerRunner(hub, lambda: failed).run_helper(intent)
    now = hub.clock()
    monkeypatch.setattr(hub, "clock", lambda: now + 61000)
    await hub.recover_quiet_runtime_failures(run["id"])
    retained = await hub.db.call(lambda c: hub._helper(c, intent["id"]))
    assert retained["state"] == "cancelled" and retained["result"] is None
    # The next admission decides again from current facts, with a new decision.
    replacement = await assignment_decision(hub, run["id"])
    assert replacement["id"] != intent["id"] and replacement["source_task_id"] == intent["source_task_id"]
    assert replacement["candidates"]["workers"] == intent["candidates"]["workers"]
    assert (await hub.run(run["id"]))["usage_uncertain"] == 1


async def test_failed_recruitment_retries_without_choosing_no(runtime, monkeypatch):
    hub, run = runtime
    first = await WorkerRunner(hub, lambda: ScriptedHarness([ScriptedTurn([])])).run_once(run["id"])
    intent = (await hub.due_recruitment_intents(run["id"]))[0]
    with pytest.raises(Exception):
        await WorkerRunner(hub, lambda: ScriptedHarness([ScriptedTurn([], status="unknown")])).run_helper(intent)
    now = hub.clock()
    monkeypatch.setattr(hub, "clock", lambda: now + 61000)
    await hub.recover_quiet_runtime_failures(run["id"])
    replacement = (await hub.due_recruitment_intents(run["id"]))[0]
    assert replacement["id"] != intent["id"]
    assert replacement["source_worker_id"] == first["worker_id"]
    state = await hub.db.call(lambda c: hub._lifecycle(c, intent["source_task_id"]))
    assert state["recruitment_decision"] == "pending"
    assert state["decision_helper_id"] == replacement["id"]


async def test_lost_reply_does_not_override_workers_explicit_wait(runtime, monkeypatch):
    hub, run = runtime

    class WaitingHarness(ScriptedHarness):
        async def run_turn(self, prompt, *, on_started, on_tool, **kwargs):
            await on_started("wait-before-transport-failure")
            result = await on_tool("taskhub_call", {"operation": "pause", "arguments": {"predicate": {"external_response_id": "unanswered-request"}, 
                "checkpoint": {"summary": "I am voluntarily waiting", "next_action": "Reconsider when useful"},
                "reason": "I am voluntarily waiting for a useful development"}}, "explicit-wait")
            assert result.get("runtime_directive") == "drain_for_wait", result
            await self.close()
            return TurnOutcome("wait-before-transport-failure", "unknown", False, True)

    result = await WorkerRunner(hub, WaitingHarness).run_once(run["id"])
    worker = await hub.worker(result["worker_id"])
    assert worker["state"] == "suspended" and worker["work_intention"] == "waiting"
    now = hub.clock()
    monkeypatch.setattr(hub, "clock", lambda: now + 61000)
    assert await hub.recover_quiet_runtime_failures(run["id"]) == []


@pytest.mark.parametrize("broken_kind", ["assignment", "recruitment"])
async def test_one_bad_helper_receipt_does_not_block_other_recovery(runtime, monkeypatch, broken_kind):
    hub, run = runtime
    # One assignment decision runs at a time, so the two decisions are of different kinds.
    assignment = await assignment_intent(hub, run)
    recruitment = await recruitment_intent(hub, run)
    broken, healthy = (assignment, recruitment) if broken_kind == "assignment" else (recruitment, assignment)
    for intent in (broken, healthy):
        await hub.note_helper_quiet(intent["id"])
    await hub.db.call(lambda c: c.execute("UPDATE events SET payload_json='invalid-json' "
        "WHERE kind='helper.execution_closed' AND object_id=?", (broken["id"],)), write=True)
    now = hub.clock()
    monkeypatch.setattr(hub, "clock", lambda: now + 61000)
    await hub.recover_quiet_runtime_failures(run["id"])
    states = await hub.db.call(lambda c: {r["id"]: r["state"] for r in c.execute("SELECT id,state FROM helper_intents")})
    assert states[broken["id"]] == "uncertain"
    assert states[healthy["id"]] == "cancelled"


async def test_pre_upgrade_unknown_history_can_continue_after_exact_stop_confirmation(peers):
    hub, run, (attempt, _) = peers
    intent = await execute(hub, "turn.reserve", {"worker_id": attempt["worker_id"]}, runner=True)
    await execute(hub, "runtime.abandon", {"worker_id": attempt["worker_id"],
        "quiescent": True, "reason": "Owned process shutdown was observed"}, runner=True)
    # Earlier installations recorded abandonment but did not close this row.
    await hub.db.call(lambda c: c.execute("UPDATE turn_intents SET state='uncertain' WHERE id=?", (intent["id"],)), write=True)
    with pytest.raises(HubError):
        await execute(hub, "worker.recover", {"worker_id": attempt["worker_id"], "reason": "No stop evidence"})
    result = await execute(hub, "worker.recover", {"worker_id": attempt["worker_id"],
        "process_confirmed_stopped": True, "reason": "Exact native process shutdown independently verified"})
    assert result["directive"] == "recovered"
    state = await hub.db.call(lambda c: c.execute("SELECT state FROM turn_intents WHERE id=?", (intent["id"],)).fetchone()[0])
    assert state == "closed_unknown"
    assert (await hub.run(run["id"]))["usage_uncertain"] == 1


async def test_admission_retry_receipt_cannot_release_a_later_different_failure(runtime, monkeypatch):
    hub, run = runtime
    worker = await execute(hub, "worker.register")
    await hub.db.call(lambda c: (
        c.execute("UPDATE workers SET run_id=?,state='recovery_required' WHERE id=?", (run["id"], worker["id"])),
        hub._event(c, "worker.admission_failed", worker["id"], {"attempt_admitted": False}, worker_id=worker["id"])), write=True)
    now = hub.clock()
    monkeypatch.setattr(hub, "clock", lambda: now + 61000)
    await hub.recover_quiet_runtime_failures(run["id"])
    assert (await hub.worker(worker["id"]))["state"] == "idle"
    await hub.db.call(lambda c: c.execute("UPDATE workers SET state='recovery_required' WHERE id=?", (worker["id"],)), write=True)
    await hub.recover_quiet_runtime_failures(run["id"])
    assert (await hub.worker(worker["id"]))["state"] == "recovery_required"


@pytest.mark.parametrize("kind", ["runtime.preflight_hold", "runtime.catalog_hold"])
@pytest.mark.parametrize("mode", ["work", "message_only"])
async def test_message_preflight_recovery_preserves_voluntary_wait(peers, monkeypatch, kind, mode):
    hub, run, (attempt, _) = peers
    await hub.db.call(lambda c: (
        c.execute("UPDATE attempts SET execution_mode=? WHERE id=?", (mode, attempt["id"])),
        c.execute("UPDATE workers SET work_intention='waiting',execution_mode='message' WHERE id=?", (attempt["worker_id"],))), write=True)
    await execute(hub, kind, {"worker_id": attempt["worker_id"], "attempt_id": attempt["id"],
                             "reason": "Paused before message inference"}, runner=True)
    now = hub.clock()
    monkeypatch.setattr(hub, "clock", lambda: now + 61000)
    assert await hub.recover_quiet_runtime_failures(run["id"]) == [attempt["worker_id"]]
    current = await hub.worker(attempt["worker_id"])
    assert current["state"] == "suspended" and current["work_intention"] == "waiting"
    assert current["pending_wake"] == int(mode == "message_only")
    assert (await hub.run(run["id"]))["tokens_used"] == 0
