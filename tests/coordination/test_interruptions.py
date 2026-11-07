from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import Actor, Command, ProjectConfig, Role
from formal_geyser.coordination.runtime import (
    ScriptedAction,
    ScriptedHarness,
    ScriptedTurn,
    TurnOutcome,
    WorkerRunner,
)

OP = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)


async def command(hub, name, payload, actor=OP):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload), actor)


async def start(hub):
    return await command(hub, "run.start", {})


@pytest.fixture
async def hub(tmp_path):
    initialize(tmp_path, ProjectConfig())
    instance = Hub(tmp_path)
    yield instance
    await instance.close()


async def interrupted(hub, *, contract=None, status="interrupted", observed=True):
    run = await start(hub)
    task = await command(hub, "task.publish", {"contract": contract or {
        "title": "Interrupted software work", "objective": "Produce a configuration file."}})
    worker = await command(hub, "worker.register", {}, RUNNER)
    worker = await command(hub, "worker.bind_session", {
        "worker_id": worker["id"], "thread_id": "native-thread-" + uuid4().hex,
        "model": "scripted", "config_hash": "original-hash",
        "codex_profile": hub.config.codex.model_dump(mode="json")}, RUNNER)
    attempt = await command(hub, "task.claim", {
        "task_id": task["id"], "revision": task["revision"],
        "worker_id": worker["id"], "run_id": run["id"]}, RUNNER)
    (hub.workspace(worker["id"]) / "progress.txt").write_text("Durable progress.", encoding="utf-8")
    intent = await command(hub, "turn.reserve", {"worker_id": worker["id"]}, RUNNER)
    remote = "native-turn-" + uuid4().hex
    await command(hub, "turn.bind", {"intent_id": intent["id"], "remote_turn_id": remote}, RUNNER)
    await command(hub, "usage.record", {"worker_id": worker["id"], "sample": {
        "totalTokens": 20, "inputTokens": 17, "cachedInputTokens": 12, "outputTokens": 3}}, RUNNER)
    await command(hub, "run.pause", {"run_id": run["id"]})
    await command(hub, "turn.finish", {"intent_id": intent["id"], "status": status,
                  "terminal_observed": observed, "process_quiet": observed}, RUNNER)
    payload = {
        "attempt_id": attempt["id"], "task_id": task["id"], "task_revision": task["revision"],
        "worker_id": worker["id"], "thread_id": worker["thread_id"], "remote_turn_id": remote,
        "turn_intent_id": intent["id"], "usage_epoch": worker["usage_epoch"],
        "codex_profile": hub.config.codex.model_dump(mode="json"),
        "terminal_observed": True, "process_quiet": True, "stop_reason": "operator_pause",
        "snapshot": hub.blobs.snapshot(hub.workspace(worker["id"])),
    }
    return run, task, worker, attempt, payload


async def test_interruption_resume_preserves_identity_fence_snapshot_and_cross_run_usage(hub):
    old_run, task, worker, origin, payload = await interrupted(hub)
    parked = await command(hub, "runtime.park_interrupted", payload, RUNNER)
    assert (await hub.run(old_run["id"]))["stop_reason"] == "operator_pause"
    assert (await hub.task(task["id"]))["state"] == "waiting"
    assert (await hub.worker(worker["id"]))["state"] == "suspended"
    new_run = await start(hub)
    attempt = await command(hub, "interruption.resume", {
        "interruption_id": parked["interruption_id"], "run_id": new_run["id"]}, RUNNER)
    assert attempt["id"] != origin["id"]
    assert attempt["fencing_token"] == origin["fencing_token"] + 1
    assert attempt["worker_id"] == worker["id"]
    assert attempt["snapshot"] == payload["snapshot"]
    resumed_worker = await hub.worker(worker["id"])
    assert resumed_worker["thread_id"] == worker["thread_id"]
    assert resumed_worker["usage_epoch"] == worker["usage_epoch"]
    old_actor = Actor(role=Role.WORKER, worker_id=worker["id"],
                      attempt_id=origin["id"], fencing_token=origin["fencing_token"])
    with pytest.raises(HubError, match="not accepting mutations"):
        await hub.authorize_worker(old_actor)
    await command(hub, "usage.record", {"worker_id": worker["id"], "sample": {"totalTokens": 27}}, RUNNER)
    assert (await hub.run(old_run["id"]))["tokens_used"] == 20
    assert (await hub.run(new_run["id"]))["tokens_used"] == 7
    assert (await hub.context(worker["id"]))["attempt"]["snapshot"] == payload["snapshot"]
    assert (await hub.list_interruptions())[0]["state"] == "resumed"


async def test_parked_ticket_reserves_task_and_worker_against_claim_race(hub):
    _, task, worker, origin, payload = await interrupted(hub)
    parked = await command(hub, "runtime.park_interrupted", payload, RUNNER)
    new_run = await start(hub)
    fresh_worker = await command(hub, "worker.register", {}, RUNNER)
    other = await command(hub, "task.publish", {"contract": {
        "title": "Other software work", "objective": "Produce independent configuration."}})
    with pytest.raises(HubError) as reserved:
        await command(hub, "task.claim", {"task_id": other["id"], "revision": 1,
                      "worker_id": worker["id"], "run_id": new_run["id"]}, RUNNER)
    assert reserved.value.code == "INTERRUPTION_RESERVED"
    resumed, racing = await asyncio.gather(
        command(hub, "interruption.resume", {
            "interruption_id": parked["interruption_id"], "run_id": new_run["id"]}, RUNNER),
        command(hub, "task.claim", {"task_id": task["id"], "revision": 1,
                "worker_id": fresh_worker["id"], "run_id": new_run["id"]}, RUNNER),
        return_exceptions=True)
    assert isinstance(racing, HubError)
    assert racing.code in {"INTERRUPTION_RESERVED", "ALREADY_CLAIMED", "RECRUITMENT_CLOSED"}
    assert resumed["fencing_token"] == origin["fencing_token"] + 1
    active = await hub.db.call(lambda c: c.execute("SELECT count(*) FROM attempts WHERE ended_at IS NULL").fetchone()[0])
    assert active == 1


@pytest.mark.parametrize("change,code", [
    ("workspace", "SNAPSHOT_CHANGED"), ("profile", "PROFILE_CHANGED"),
    ("thread", "SESSION_CHANGED"), ("input", "INPUTS_CHANGED"),
])
async def test_resume_rejects_changed_original_basis(hub, change, code):
    artifact = await command(hub, "artifact.register", {"kind": "software.input", "data": {"value": 1}})
    await command(hub, "artifact.accept", {"artifact_id": artifact["id"], "policy": "general.manual@1", "verdict": "accepted"})
    _, _, worker, _, payload = await interrupted(hub, contract={
        "title": "Bound software task", "objective": "Use the exact configuration input.",
        "inputs": [artifact["id"]]})
    parked = await command(hub, "runtime.park_interrupted", payload, RUNNER)
    new_run = await start(hub)
    if change == "workspace":
        (hub.workspace(worker["id"]) / "progress.txt").write_text("Changed after parking.")
    elif change == "profile":
        hub.config.codex.effort = "low"  # Any field of the bound profile.
    elif change == "thread":
        await command(hub, "worker.bind_session", {
            "worker_id": worker["id"], "thread_id": "different-native-thread"}, RUNNER)
    else:
        await command(hub, "artifact.withdraw", {"artifact_id": artifact["id"], "reason": "Input superseded."})
    with pytest.raises(HubError) as blocked:
        await command(hub, "interruption.resume", {
            "interruption_id": parked["interruption_id"], "run_id": new_run["id"]}, RUNNER)
    assert blocked.value.code == code
    assert (await hub.list_interruptions())[0]["state"] == "parked"
    assert (await hub.worker(worker["id"]))["active_attempt"] is None


@pytest.mark.parametrize("operation", ["task.revise", "task.retire"])
async def test_task_revision_or_retirement_supersedes_interruption(hub, operation):
    _, task, worker, _, payload = await interrupted(hub)
    parked = await command(hub, "runtime.park_interrupted", payload, RUNNER)
    change = {"task_id": task["id"], "revision": task["revision"], "reason": "Reconsider the retained native work against a new task basis."}
    if operation == "task.revise":
        change["contract"] = {"title": "Revised software work", "objective": "Use a revised interface."}
    await command(hub, operation, change)
    assert (await hub.list_interruptions())[0]["state"] == "superseded"
    assert (await hub.worker(worker["id"]))["state"] == "idle"
    new_run = await start(hub)
    with pytest.raises(HubError) as blocked:
        await command(hub, "interruption.resume", {
            "interruption_id": parked["interruption_id"], "run_id": new_run["id"]}, RUNNER)
    assert blocked.value.code == "INTERRUPTION_NOT_READY"


async def test_operator_import_requires_evidence_and_never_invents_quiescence(hub):
    _, _, worker, _, payload = await interrupted(hub)
    with pytest.raises(HubError) as blocked:
        await command(hub, "runtime.park_interrupted", {**payload, "process_quiet": False}, RUNNER)
    assert blocked.value.code == "QUIESCENCE_REQUIRED"
    await command(hub, "runtime.abandon", {"worker_id": worker["id"], "quiescent": True,
                  "reason": "Observed interrupted turn and stopped process tree."}, RUNNER)
    await command(hub, "worker.recover", {"worker_id": worker["id"], "process_confirmed_stopped": True,
                  "reason": "The latest interrupted execution is independently confirmed quiet and recoverable."})
    imported = {**payload, "process_confirmed_stopped": True,
                "reason": "Import the recorded interruption after deployment.",
                "evidence": "Exact terminal event and owned-process shutdown observation were independently checked."}
    with pytest.raises(HubError) as forbidden:
        await command(hub, "interruption.import", imported, RUNNER)
    assert forbidden.value.code == "FORBIDDEN"
    for field in ("terminal_observed", "process_quiet", "process_confirmed_stopped", "evidence", "codex_profile"):
        missing = {key: value for key, value in imported.items() if key != field}
        with pytest.raises(HubError):
            await command(hub, "interruption.import", missing)
    parked = await command(hub, "interruption.import", imported)
    ticket = (await hub.list_interruptions())[0]
    assert ticket["proof"]["source"] == "operator_import"
    assert ticket["proof"]["evidence"] == imported["evidence"]
    assert ticket["snapshot"] == payload["snapshot"]
    new_run = await start(hub)
    resumed = await command(hub, "interruption.resume", {
        "interruption_id": parked["interruption_id"], "run_id": new_run["id"]}, RUNNER)
    assert resumed["worker_id"] == worker["id"]


@pytest.mark.parametrize("status", ["unknown", "failed", "completed", "cancelled"])
async def test_import_rejects_non_interrupted_or_uncertain_turn(hub, status):
    _, _, worker, _, payload = await interrupted(hub, status=status)
    await command(hub, "runtime.abandon", {"worker_id": worker["id"], "quiescent": True}, RUNNER)
    with pytest.raises(HubError) as blocked:
        await command(hub, "interruption.import", {
            **payload, "process_confirmed_stopped": True, "reason": "Inspected execution.",
            "evidence": "Process is no longer running, but no interrupted terminal was observed."})
    assert blocked.value.code in {"INTERRUPTED_TURN_REQUIRED", "USAGE_UNCERTAIN"}
    assert not await hub.list_interruptions()


async def test_parked_interruption_survives_service_reopen_without_becoming_ambiguous(hub):
    old_run, _, worker, _, payload = await interrupted(hub)
    parked = await command(hub, "runtime.park_interrupted", payload, RUNNER)
    reason = "operator_pause"
    limits_json = old_run["limits_json"]
    await hub.close()
    reopened = Hub(hub.root)
    try:
        ticket = (await reopened.list_interruptions())[0]
        assert ticket["state"] == "parked"
        assert ticket["stop_reason"] == reason
        source_run = await reopened.run(old_run["id"])
        assert not source_run["usage_uncertain"]
        assert source_run["state"] == "paused"
        assert source_run["stop_reason"] == reason
        assert await reopened.db.call(lambda c: c.execute(
            "SELECT limits_json FROM runs WHERE id=?", (old_run["id"],)).fetchone()[0]) == limits_json
        new_run = await start(reopened)
        resumed = await command(reopened, "interruption.resume", {
            "interruption_id": parked["interruption_id"], "run_id": new_run["id"]}, RUNNER)
        assert resumed["worker_id"] == worker["id"]
        assert resumed["resume_reason"] == reason
        assert resumed["snapshot"] == payload["snapshot"]
        assert await reopened.db.call(lambda c: c.execute(
            "SELECT limits_json FROM runs WHERE id=?", (old_run["id"],)).fetchone()[0]) == limits_json
    finally:
        await reopened.close()


async def test_runtime_operator_pause_then_resume_original_thread_with_incremental_usage(hub):
    run = await start(hub)
    await command(hub, "task.publish", {"contract": {
        "title": "Resumable software work", "objective": "Save a small configuration artifact."}})
    harnesses = []
    interrupted_event = asyncio.Event()
    usage_recorded = asyncio.Event()

    class PausedHarness(ScriptedHarness):
        async def interrupt(self):
            interrupted_event.set()

        async def run_turn(self, prompt, *, on_tool, on_usage, on_started):
            turn_id = uuid4().hex
            await on_started(turn_id)
            self.workspaces[0].joinpath("progress.txt").write_text("Saved before interruption.")
            await on_usage({"totalTokens": 20})
            usage_recorded.set()
            await interrupted_event.wait()
            await self.close()
            return TurnOutcome(turn_id, "interrupted", True, True)

    def factory():
        harness = PausedHarness() if not harnesses else ScriptedHarness([
            ScriptedTurn(actions=[ScriptedAction("taskhub_call", {"operation": "pause", "arguments": {"predicate": {"external_response_id": "unanswered-request"}, 
                "reason": "The recovered configuration is saved for later work.",
                "checkpoint": {"summary": "Recovered the saved configuration and collected all results.",
                               "next_action": "Continue from the saved configuration."}}})],
                         usage={"totalTokens": 27})])
        harnesses.append(harness)
        return harness

    runner = WorkerRunner(hub, factory)
    execution = asyncio.create_task(runner.run_until_idle(run["id"]))
    started = asyncio.create_task(usage_recorded.wait())
    try:
        # Runtime startup prepares native files; it is not the pause latency
        # being tested. Surface a failed driver immediately rather than masking
        # it behind an event timeout on a busy host.
        ready, _ = await asyncio.wait({started, execution}, timeout=15,
                                      return_when=asyncio.FIRST_COMPLETED)
        if execution in ready:
            await execution
        assert started in ready, "Runtime did not record usage during startup"
        assert (await hub.run(run["id"]))["state"] == "running"
        assert not interrupted_event.is_set()
        await command(hub, "run.pause", {"run_id": run["id"]})
        result = await asyncio.wait_for(execution, 3)
    finally:
        for pending in (started, execution):
            if not pending.done():
                pending.cancel()
        await asyncio.gather(started, execution, return_exceptions=True)
    parked = result["results"][0]
    assert parked["directive"] == "parked"
    assert parked["stop_reason"] == "operator_pause"
    assert parked["error"] is None and not parked["diagnostic_refs"]
    assert result["state"] == "paused"
    worker = await hub.worker(parked["worker_id"])
    native_thread, usage_epoch = worker["thread_id"], worker["usage_epoch"]
    new_run = await start(hub)
    resumed = await runner.run_once(new_run["id"])
    assert resumed["worker_id"] == parked["worker_id"]
    assert resumed["directive"] == "waiting"
    assert harnesses[1].opened_threads == [native_thread]
    assert harnesses[1].workspaces == harnesses[0].workspaces
    assert harnesses[1].workspaces[0].joinpath("progress.txt").read_text() == "Saved before interruption."
    assert (await hub.worker(worker["id"]))["usage_epoch"] == usage_epoch
    assert (await hub.run(new_run["id"]))["tokens_used"] == 7
    assert (await hub.run(run["id"]))["tokens_used"] == 20


async def test_missing_input_task_does_not_block_independent_ready_work(hub):
    run = await start(hub)
    malformed = await command(hub, "task.publish", {"contract": {
        "title": "Missing input", "objective": "Use a missing artifact.", "priority": 0,
        "inputs": ["nonexistent-artifact"]}})
    good = await command(hub, "task.publish", {"contract": {
        "title": "Independent work", "objective": "Produce a standalone software result.", "priority": 1}})
    runner = WorkerRunner(hub, lambda: ScriptedHarness([ScriptedTurn(actions=[
        ScriptedAction("taskhub_call", {"operation": "pause", "arguments": {"predicate": {"external_response_id": "unanswered-request"}, 
            "reason": "The independent task is assigned and preserved for later work.",
            "checkpoint": {
                "summary": "Took the independent work despite an unrelated missing input.",
                "next_action": "Continue the independent task."}}})])]))
    # The task with a missing input is no opportunity; the independent one gets a new worker at once.
    assigned = await hub.next_assignment(run["id"])
    assert assigned["task_id"] == good["id"]
    result = await runner.run_once(run["id"], assignment=assigned)
    assert result["task_id"] == good["id"]
    assert (await hub.task(malformed["id"]))["active_attempt"] is None
    assert result["directive"] == "waiting"


async def test_late_usage_preserves_explicit_operator_pause(hub):
    run = await start(hub)
    worker = await command(hub, "worker.register", {}, RUNNER)
    task = await command(hub, "task.publish", {"contract": {
        "title": "Paused work", "objective": "Stop on an explicit operator pause."}})
    await command(hub, "task.claim", {"task_id": task["id"], "revision": 1,
                  "worker_id": worker["id"], "run_id": run["id"]}, RUNNER)
    await command(hub, "run.pause", {"run_id": run["id"]})
    await command(hub, "usage.record", {"worker_id": worker["id"], "sample": {"totalTokens": 1000000}}, RUNNER)
    state = await hub.run(run["id"])
    assert state["stop_reason"] == "operator_pause"
    assert state["state"] == "paused"
    assert state["tokens_used"] == 1000000
    assert not any(event["kind"] == "run.budget_exhausted" for event in await hub.events())


async def test_resume_all_preserves_parked_ticket_despite_historical_preflight(hub):
    run, task, worker, origin, payload = await interrupted(hub)
    parked = await command(hub, "runtime.park_interrupted", payload, RUNNER)
    def history(c):
        row = dict(c.execute("SELECT * FROM attempts WHERE id=?", (origin["id"],)).fetchone())
        row.update(id="earlier-preflight", state="preflight_held", created_at=row["created_at"] - 1)
        c.execute("INSERT INTO attempts (" + ",".join(row) + ") VALUES (" + ",".join("?" for _ in row) + ")", tuple(row.values()))
    await hub.db.call(history, write=True)
    await command(hub, "run.resume", {"run_id": run["id"]})
    assert (await hub.worker(worker["id"]))["state"] == "suspended"
    resumed = await hub.next_assignment(run["id"])
    assert resumed["worker_id"] == worker["id"]
    assert resumed["interruption_id"] == parked["interruption_id"]
    assert (await hub.worker(worker["id"]))["thread_id"] == worker["thread_id"]
