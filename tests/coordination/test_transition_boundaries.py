"""Cross-event lifecycle regressions using synthetic local files and no models."""
import asyncio
import json
from uuid import uuid4

import pytest

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.runtime import (
    ScriptedHarness,
    TurnOutcome,
    WorkerRunner,
)
from tests.coordination.lifecycle_helpers import assignment_decision, open_collaboration
from tests.coordination.publication_helpers import file_payload, new_hub
from tests.coordination.test_audit_lifecycle_transitions import accepted, declaration, finalize
from tests.coordination.test_lifecycle_repairs import (
    RUNNER,
    claim,
    command,
    publish,
    quiet,
    revise,
    worker,
)
from tests.coordination.wait_helpers import legacy_wait

pytestmark = pytest.mark.asyncio


async def bind(hub, attempt):
    return await command(hub, "worker.bind_session", {
        "worker_id": attempt["worker_id"], "thread_id": "synthetic-" + uuid4().hex,
        "model": "scripted", "config_hash": "synthetic-lifecycle",
        "codex_profile": hub.config.codex.model_dump(mode="json")}, RUNNER)


@pytest.fixture
async def case(tmp_path_factory):
    hub = await new_hub(tmp_path_factory.mktemp("tb"))
    root = await publish(hub, "Synthetic root")
    run = await command(hub, "run.start")
    author = await hub.next_assignment(run["id"])
    try:
        yield hub, root, run, author
    finally:
        await hub.close()


@pytest.mark.parametrize("already_quiet", [False, True])
@pytest.mark.parametrize("binding", ["direct", "requirement"])
async def test_withdrawal_reconsiders_saved_executor_without_accepting_invalid_input(case, already_quiet, binding):
    hub, _, run, author = case
    artifact_id = await accepted(hub, author, run)
    fields = ({"inputs": [artifact_id]} if binding == "direct" else {"requirements": [
        {"key": "input", "artifact_id": artifact_id, "policy": "general.peer@1"}]})
    task = await publish(hub, "Consumer", **fields)
    attempt = await claim(hub, task, run)
    path = hub.workspace(attempt["worker_id"]) / "saved.txt"
    path.write_text("Preserve the original plan.", encoding="utf-8")
    if already_quiet:
        await hub.ordinary_turn_completed(attempt["worker_id"], attempt["id"])
    await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Input is invalid."})
    await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Same withdrawal observed again."})
    if not already_quiet:
        assert await hub.next_assignment(run["id"]) is None
        await hub.ordinary_turn_completed(attempt["worker_id"], attempt["id"])
    tickets = await hub.db.call(lambda c: [dict(row) for row in c.execute(
        "SELECT * FROM continuations WHERE preferred_worker=? AND state='reconsider'", (attempt["worker_id"],))])
    assert len(tickets) == 1 and tickets[0]["snapshot"]
    await command(hub, "run.pause", {"run_id": run["id"]})
    assert await hub.next_assignment(run["id"]) is None
    resumed_run = await command(hub, "run.start")
    resumed = await hub.next_assignment(resumed_run["id"])
    assert resumed["worker_id"] == attempt["worker_id"] and resumed["task_id"] == task["id"]
    assert resumed["resume_reason"] == "artifact_withdrawn"
    assert path.read_text(encoding="utf-8") == "Preserve the original plan."
    await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "The same loss is already being reconsidered."})
    assert not await hub.db.call(lambda c: c.execute("SELECT 1 FROM continuations WHERE preferred_worker=? AND state='reconsider'",
                                                   (attempt["worker_id"],)).fetchone())
    with pytest.raises(HubError) as invalid:
        await command(hub, "artifact.stage", file_payload(hub, resumed, data={"invalid": "not a valid result"}), worker(resumed))
    assert invalid.value.code in {"INPUTS_CHANGED", "PREREQUISITES_MISSING"}
    assert not await hub.db.call(lambda c: c.execute("SELECT 1 FROM artifacts WHERE task_id=?", (task["id"],)).fetchone())


async def test_withdrawal_retains_reconsideration_without_clearing_recovery_hold(case):
    hub, _, run, author = case
    artifact_id = await accepted(hub, author, run)
    task = await publish(hub, "Recoverable consumer", inputs=[artifact_id])
    attempt = await claim(hub, task, run)
    await command(hub, "runtime.abandon", {"worker_id": attempt["worker_id"], "quiescent": True,
        "reason": "The process is quiet; the workspace needs explicit recovery."}, RUNNER)
    await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Dependency lost during recovery."})
    assert (await hub.worker(attempt["worker_id"]))["state"] == "recovery_required"
    assert await hub.next_assignment(run["id"]) is None
    await command(hub, "worker.recover", {"worker_id": attempt["worker_id"], "process_confirmed_stopped": True,
        "reason": "Saved work is recoverable and the original process stopped."})
    resumed = await hub.next_assignment(run["id"])
    assert resumed["worker_id"] == attempt["worker_id"] and resumed["resume_reason"] == "artifact_withdrawn"


@pytest.mark.parametrize("wake_kind", ["ordinary", "external_response", "revision_reconsideration", "withdrawal_reconsideration"])
async def test_operator_pause_preserves_authorized_activation_through_repeated_resume(case, wake_kind):
    hub, task, run, attempt = case
    if wake_kind != "ordinary":
        await bind(hub, attempt)
    if wake_kind == "withdrawal_reconsideration":
        artifact_id = await accepted(hub, attempt, run)
        task = await publish(hub, "Consumer", inputs=[artifact_id])
        attempt = await claim(hub, task, run)
        await bind(hub, attempt)
        await hub.ordinary_turn_completed(attempt["worker_id"], attempt["id"])
        await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Reconsider invalid input"})
        attempt = await hub.next_assignment(run["id"])
    elif wake_kind != "ordinary":
        if wake_kind == "external_response":
            request = await command(hub, "message.request", {"request_key": "scope", "title": "Scope", "text": "Which scope?"}, worker(attempt))
            predicate = request["wait_predicate"]
        else:
            dependency = await publish(hub, "Unfinished dependency")
            predicate = {"mode": "all", "requirements": [{"key": "result", "task_id": dependency["id"],
                "task_revision": 1, "policy": "general.peer@1"}]}
        await legacy_wait(hub, {"reason": "Await the saved event", "predicate": predicate,
            "checkpoint": {"summary": "Saved plan", "next_action": "Reconsider the exact event"}}, worker(attempt))
        await quiet(hub, attempt)
        if wake_kind == "external_response":
            await command(hub, "request.resolve", {"request_id": request["id"], "outcome": "fulfilled",
                "text": "Keep the original scope.", "message_id": "synthetic-response"})
        else:
            task = await revise(hub, task)
        attempt = await hub.next_assignment(run["id"])
    original_worker = attempt["worker_id"]

    for index in range(2):
        interrupted_event = asyncio.Event()

        class PausedHarness(ScriptedHarness):
            async def interrupt(self):
                interrupted_event.set()

            async def run_turn(self, prompt, *, on_tool, on_usage, on_started):
                turn_id = "synthetic-turn-" + uuid4().hex
                await on_started(turn_id)
                self.workspaces[0].joinpath("progress.txt").write_text("Saved exact work.", encoding="utf-8")
                await command(hub, "run.pause", {"run_id": run["id"]})
                await interrupted_event.wait()
                await self.close()
                return TurnOutcome(turn_id, "interrupted", True, True)

        driver = WorkerRunner(hub, PausedHarness)
        parked = await driver.run_once(run["id"], assignment=attempt)
        assert parked["directive"] == "parked" and interrupted_event.is_set()
        assert (await hub.run(run["id"]))["state"] == "paused"
        assert (await hub.worker(original_worker))["state"] == "suspended"
        assert await hub.next_assignment(run["id"]) is None
        run = await command(hub, "run.start")
        attempt = await hub.next_assignment(run["id"])
        assert attempt["worker_id"] == original_worker and attempt["task_id"] == task["id"]
        assert hub.workspace(original_worker).joinpath("progress.txt").read_text(encoding="utf-8") == "Saved exact work."
        assert len(await hub.list_interruptions()) == index + 1
        if wake_kind == "withdrawal_reconsideration":
            # A maintenance activation does not authorize invalid publication.
            with pytest.raises(HubError) as invalid:
                await command(hub, "artifact.stage", file_payload(hub, attempt, data={"invalid": "blocked"}), worker(attempt))
            assert invalid.value.code in {"INPUTS_CHANGED", "PREREQUISITES_MISSING"}


@pytest.mark.parametrize("peer_state", ["active", "between_turns", "sleeping", "message", "message_then_work"])
async def test_completion_peer_follows_ongoing_work_not_attempt_timing(case, peer_state):
    hub, task, run, author = case
    await open_collaboration(hub, task["id"])
    peer = await claim(hub, task, run, plan="Check the remaining objective.")
    artifact_id = await accepted(hub, author, run)
    if peer_state == "between_turns":
        await hub.ordinary_turn_completed(peer["worker_id"], peer["id"])
    elif peer_state in {"sleeping", "message", "message_then_work"}:
        await legacy_wait(hub, {"reason": "No independent work remains."}, worker(peer))
        await quiet(hub, peer)
        if peer_state != "sleeping":
            await command(hub, "message.send", {"worker_id": peer["worker_id"], "text": "A real update.", "message_id": "update"})
            peer = await hub.next_assignment(run["id"])
            assert peer["execution_mode"] == "message"
            if peer_state == "message_then_work":
                await command(hub, "worker.resume_work", {}, worker(peer))
    completion = await declaration(hub, task, author, [artifact_id])
    ongoing = peer_state in {"active", "between_turns", "message_then_work"}
    assert completion["feedback_state"] == ("pending" if ongoing else "not_required")
    assert completion["feedback_worker_id"] == (peer["worker_id"] if ongoing else None)
    if ongoing:
        if peer_state == "between_turns":
            peer = await hub.next_assignment(run["id"])
            assert peer["worker_id"] == completion["feedback_worker_id"]
        completion = await command(hub, "task.complete.feedback", {"declaration_id": completion["id"],
            "conditions": [], "checkpoint": {"summary": "Checked the evidence", "next_action": "Finish"}}, worker(peer))
        assert completion["feedback_state"] == "responded"
    elif peer_state == "message":
        await hub.ordinary_turn_completed(peer["worker_id"], peer["id"])
        assert (await hub.worker(peer["worker_id"]))["work_intention"] == "waiting"
    await finalize(hub, completion, author)
    await quiet(hub, author)
    if (await hub.worker(peer["worker_id"]))["active_attempt"]:
        await quiet(hub, peer)
    assert (await hub.task(task["id"]))["state"] == "done"


async def test_retained_pending_feedback_reconciles_after_sleeping_message_turn(case):
    hub, task, run, author = case
    await open_collaboration(hub, task["id"])
    peer = await claim(hub, task, run, plan="Check the evidence.")
    artifact_id = await accepted(hub, author, run)
    await legacy_wait(hub, {"reason": "Work is saved."}, worker(peer))
    await quiet(hub, peer)
    await command(hub, "message.send", {"worker_id": peer["worker_id"], "text": "A real update.", "message_id": "update"})
    message_attempt = await hub.next_assignment(run["id"])
    packet = await hub.context(peer["worker_id"])
    await command(hub, "context.ack", {"worker_id": peer["worker_id"], "packet_id": packet["packet_id"]}, worker(message_attempt))
    completion = await declaration(hub, task, author, [artifact_id])
    # Simulate a durable pending declaration retained from the previous version,
    # which selected the sleeping worker solely because its message turn was active.
    await hub.db.call(lambda c: c.execute("UPDATE task_completion_declarations SET feedback_worker_id=?,feedback_state='pending' WHERE id=?",
                                        (peer["worker_id"], completion["id"])), write=True)
    await hub.ordinary_turn_completed(peer["worker_id"], message_attempt["id"])
    resolved = await hub.db.call(lambda c: hub._completion_record(c, completion["id"]))
    assert resolved["feedback_state"] == "unavailable"
    assert "voluntarily slept" in resolved["feedback"]["reason"]
    assert "recovery_impossible" not in resolved["feedback"]
    await finalize(hub, resolved, author)
    await quiet(hub, author)
    assert (await hub.task(task["id"]))["state"] == "done"


@pytest.mark.parametrize("recover_first", [False, True])
async def test_recovery_and_retirement_both_orders_make_the_worker_idle(case, recover_first):
    hub, task, run, attempt = case
    await command(hub, "runtime.abandon", {"worker_id": attempt["worker_id"], "quiescent": True,
        "reason": "Known local failure; no native outcome is unknown."}, RUNNER)
    recovery = {"worker_id": attempt["worker_id"], "process_confirmed_stopped": True,
                "reason": "Original workspace and conversation are recoverable."}
    retirement = {"task_id": task["id"], "revision": 1, "reason": "Retain history and retire this assignment."}
    if recover_first:
        await command(hub, "worker.recover", recovery)
        await command(hub, "task.retire", retirement)
    else:
        await command(hub, "task.retire", retirement)
        assert (await hub.worker(attempt["worker_id"]))["state"] == "recovery_required"
        await command(hub, "worker.recover", recovery)
    current = await hub.worker(attempt["worker_id"])
    assert current["state"] == "idle" and current["work_intention"] == "idle" and current["current_task_id"] is None
    independent = await publish(hub, "Independent work")
    # An idle worker is offered the next open task by its assignment decision.
    decision = await assignment_decision(hub, run["id"])
    assert decision["source_task_id"] == independent["id"]
    assert current["id"] in {item["worker_id"] for item in decision["candidates"]["workers"]}


async def test_confirmed_shutdown_releases_retired_membership_without_inventing_a_terminal(case):
    hub, task, _, attempt = case
    await bind(hub, attempt)
    intent = await command(hub, "turn.reserve", {"worker_id": attempt["worker_id"]}, RUNNER)
    await command(hub, "turn.bind", {"intent_id": intent["id"], "remote_turn_id": "unresolved-synthetic-turn"}, RUNNER)
    await command(hub, "runtime.abandon", {"worker_id": attempt["worker_id"], "quiescent": True,
        "reason": "Process quiet but native outcome is still unknown."}, RUNNER)
    await command(hub, "task.retire", {"task_id": task["id"], "revision": 1, "reason": "Retire saved assignment."})
    await command(hub, "worker.recover", {"worker_id": attempt["worker_id"], "process_confirmed_stopped": True,
        "reason": "The owned execution stopped; retain its unknown outcome while the worker becomes idle."})
    current = await hub.worker(attempt["worker_id"])
    assert current["state"] == "idle" and current["current_task_id"] is None
    assert current["work_intention"] == "idle"
    retained = await hub.db.call(lambda c: hub._get(c, "turn_intents", intent["id"]))
    assert retained["state"] == "closed_unknown"
    assert json.loads((await hub.db.call(lambda c: hub._get(c, "attempts", attempt["id"])))["input_refs_json"]) == []
