"""A validation that outlives its submission's wait continues in the background."""
from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from contextvars import ContextVar

from formal_geyser.coordination.errors import ValidationContinues
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import Actor, Role
from formal_geyser.coordination.plugins import GeneralPlugin
from tests.coordination.test_native_runtime import call, command, worker
from tests.coordination.test_native_runtime import native_runtime as native_runtime
from tests.coordination.test_submission_recovery import prepare

RUNNER = Actor(role=Role.RUNNER)
WAIT: ContextVar[float | None] = ContextVar("test_validation_wait", default=None)


class Outliving(GeneralPlugin):
    """Validates in its own service, as the Lean plugin does: a bounded wait gives up, the work goes on."""

    validations_survive_interruption = True

    def __init__(self):
        self.release = asyncio.Event()
        self.started = 0

    @contextmanager
    def submission_wait(self, hub):
        token = WAIT.set(0.05)
        try:
            yield
        finally:
            WAIT.reset(token)

    async def validate_candidate(self, hub, task, artifact):
        self.started += 1
        try:
            await asyncio.wait_for(self.release.wait(), WAIT.get())
        except TimeoutError:
            raise ValidationContinues("The service is still checking it (queued at position 2)",
                                      {"id": "job-1", "state": "queued", "position": 2}) from None
        return await super().validate_candidate(hub, task, artifact)


def finished(hub, artifact_id):
    return hub.db.call(lambda c: hub._satisfied(c, {"mode": "all", "requirements": [], "validations": [artifact_id]}, waiter=None))


async def test_a_long_validation_is_queued_reported_and_finished_by_the_driver(native_runtime):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = plugin = Outliving()
    staged = await call(runner, attempt, "author", "stage", args, "stage-1")
    validation = staged["validation"]
    assert validation["state"] == "queued" and validation["code"] == "VALIDATION_QUEUED"
    assert validation["job"] == {"id": "job-1", "state": "queued", "position": 2}
    assert f'{{"validations": ["{staged["artifact_id"]}"]}}' in validation["instruction"]
    assert not await finished(case.hub, staged["artifact_id"])
    # The author's attempt is still active; the background driver attaches anyway.
    [pending] = await case.hub.pending_native_validations(case.run["id"])
    assert pending["validation_state"] == "queued" and pending["attempt_id"] == attempt["id"]
    assert await case.hub.pending_native_validations(case.run["id"], worker_id=attempt["worker_id"]) == []
    plugin.release.set()
    result = await runner._recover_submission(pending, background=True)
    assert result["evidence"][-1]["verdict"] == "accepted" and plugin.started == 2
    phase = await case.hub.native_validation(staged["artifact_id"])
    assert phase["validation_state"] == "completed"
    assert await finished(case.hub, staged["artifact_id"])
    events = [event for event in await case.hub.events(limit=1000) if event["kind"] == "validation.queued"]
    assert [event["object_id"] for event in events] == [staged["artifact_id"]]


async def test_a_context_only_revision_leaves_a_queued_validation_to_finish(native_runtime):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = plugin = Outliving()
    staged = await call(runner, attempt, "author", "stage", args, "stage-1")
    assert staged["validation"]["state"] == "queued"
    host = await case.hub.task(case.host["id"])
    # The author adds background to its task; its turn ends to take up the new revision.
    await command(case.hub, "task.revise", {"task_id": host["id"], "revision": host["revision"],
                  "contract": {**host["contract"], "context": "Background for later readers."},
                  "reason": "Add background for later readers."}, worker(attempt))
    await command(case.hub, "runtime.quiescent", {"worker_id": attempt["worker_id"], "snapshot":
                  case.hub.blobs.snapshot(case.hub.workspace(attempt["worker_id"]))}, RUNNER)
    [pending] = await case.hub.pending_native_validations(case.run["id"])
    plugin.release.set()
    result = await runner._recover_submission(pending, background=True)
    assert result["evidence"][-1]["verdict"] == "accepted"
    assert (await case.hub.native_validation(staged["artifact_id"]))["validation_state"] == "completed"


async def test_a_queued_validation_is_collected_whatever_its_author_does(native_runtime):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = Outliving()
    staged = await call(runner, attempt, "author", "stage", args, "stage-1")

    def paused(c):  # The operator paused this author alone.
        c.execute("INSERT INTO worker_controls(worker_id,run_id,control,created_at) VALUES(?,?,'paused',0)",
                  (attempt["worker_id"], case.run["id"]))
    await case.hub.db.call(paused, write=True)
    [pending] = await case.hub.pending_native_validations(case.run["id"])
    assert (pending["validation_artifact_id"], pending["validation_state"]) == (staged["artifact_id"], "queued")

    def quarantined(c):  # Startup recovery quarantined the author.
        c.execute("UPDATE workers SET state='recovery_required' WHERE id=?", (attempt["worker_id"],))
    await case.hub.db.call(quarantined, write=True)
    [pending] = await case.hub.pending_native_validations(case.run["id"])
    assert pending["validation_state"] == "queued"


async def test_an_author_paused_on_its_validation_wakes_when_it_finishes(native_runtime):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = plugin = Outliving()
    staged = await call(runner, attempt, "author", "stage", args, "stage-1")
    [pending] = await case.hub.pending_native_validations(case.run["id"])
    attaching = asyncio.create_task(runner._recover_submission(pending, background=True))
    for _ in range(200):
        if plugin.started == 2:  # The driver attached before the author paused.
            break
        await asyncio.sleep(0.01)
    await call(runner, attempt, "author", "pause", {
        "checkpoint": {"summary": "Staged the candidate.", "next_action": "Read its validation."},
        "reason": "Waiting for machine validation", "predicate": {"validations": [staged["artifact_id"]]}}, "pause-1")

    def state(c):
        return c.execute("SELECT state FROM continuations WHERE origin_attempt=?", (attempt["id"],)).fetchone()[0]
    assert await case.hub.db.call(state) == "waiting"
    plugin.release.set()
    assert (await asyncio.wait_for(attaching, 30))["evidence"][-1]["verdict"] == "accepted"
    assert await case.hub.db.call(state) == "eligible"


async def test_a_restart_during_a_validation_that_outlives_it_queues_it_again(native_runtime, monkeypatch):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = Outliving()
    staged = await call(runner, attempt, "author", "stage", args, "stage-1")  # Unfinished: queued.
    artifact_id = staged["artifact_id"]

    def running(c):  # As if TaskHub stopped while it waited for the result.
        c.execute("UPDATE native_call_receipts SET validation_state='running' WHERE validation_artifact_id=?",
                  (artifact_id,))
    await case.hub.db.call(running, write=True)
    await case.hub.close()
    monkeypatch.setattr(GeneralPlugin, "validations_survive_interruption", True, raising=False)
    restarted = Hub(case.hub.root)
    try:
        phase = await restarted.native_validation(artifact_id)
        assert phase["validation_state"] == "queued" and "continues outside TaskHub" in phase["validation_reason"]
    finally:
        await restarted.close()
        case.hub = Hub(case.hub.root)


async def test_a_restart_queues_again_a_validation_left_uncertain_by_a_service_timeout(native_runtime, monkeypatch):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = Outliving()
    staged = await call(runner, attempt, "author", "stage", args, "stage-1")  # Unfinished: queued.
    artifact_id = staged["artifact_id"]

    def uncertain(reason):
        def mark(c):  # As older code left a wait that timed out against the busy service.
            c.execute("UPDATE native_call_receipts SET validation_state='uncertain',validation_reason=? "
                      "WHERE validation_artifact_id=?", (reason, artifact_id))
        return mark
    monkeypatch.setattr(GeneralPlugin, "validations_survive_interruption", True, raising=False)
    for reason, expected in (("RuntimeError", "uncertain"), ("ReadTimeout", "queued")):
        await case.hub.db.call(uncertain(reason), write=True)
        await case.hub.close()
        case.hub = Hub(case.hub.root)
        phase = await case.hub.native_validation(artifact_id)
        assert phase["validation_state"] == expected, reason
    assert "did not answer in time" in phase["validation_reason"]


async def test_an_unexpected_failure_still_needs_recovery(native_runtime):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = plugin = Outliving()

    async def broken(hub, task, artifact):
        raise RuntimeError("A defect, not an interruption")

    plugin.validate_candidate = broken
    try:
        await call(runner, attempt, "author", "stage", args, "stage-1")
    except RuntimeError:
        pass
    rows = await case.hub.db.call(lambda c: [dict(row) for row in c.execute(
        "SELECT validation_state FROM native_call_receipts WHERE call_id='stage-1'")])
    assert rows == [{"validation_state": "uncertain"}]


async def test_a_withdrawn_candidates_uncertain_validation_needs_no_recovery(native_runtime):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = plugin = Outliving()

    async def broken(hub, task, artifact):
        raise RuntimeError("A defect, not an interruption")

    plugin.validate_candidate = broken
    try:
        await call(runner, attempt, "author", "stage", args, "stage-1")
    except RuntimeError:
        pass
    artifact_id = await case.hub.db.call(lambda c: c.execute(
        "SELECT validation_artifact_id FROM native_call_receipts WHERE call_id='stage-1'").fetchone()[0])
    await command(case.hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Corrected before review."})
    phase = await case.hub.native_validation(artifact_id)
    assert phase["validation_state"] == "obsolete"
    assert "Corrected before review." in phase["validation_reason"]
    assert not await case.hub.db.call(lambda c: case.hub._native_validation_held(c, attempt["worker_id"]))


async def test_an_interrupted_wait_leaves_an_outliving_validation_queued_not_uncertain(native_runtime):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = plugin = Outliving()
    plugin.submission_wait = lambda hub: contextmanager(lambda: iter([None]))()  # Waits unbounded.
    staging = asyncio.create_task(call(runner, attempt, "author", "stage", args, "stage-1"))
    for _ in range(200):
        if plugin.started:
            break
        await asyncio.sleep(0.01)
    staging.cancel()
    await asyncio.gather(staging, return_exceptions=True)
    rows = await case.hub.db.call(lambda c: [dict(row) for row in c.execute(
        "SELECT validation_state,validation_reason FROM native_call_receipts WHERE call_id='stage-1'")])
    assert rows and rows[0]["validation_state"] == "queued"
    assert json.dumps(rows[0]).count("uncertain") == 0


def receipt(hub, artifact_id):
    return hub.db.call(lambda c: dict(c.execute(
        "SELECT validation_state,validation_reason FROM native_call_receipts WHERE validation_artifact_id=?",
        (artifact_id,)).fetchone()))


async def test_a_report_that_cannot_be_kept_queues_its_validation_again(native_runtime, monkeypatch):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = plugin = Outliving()
    plugin.release.set()  # The check finishes within the submission's wait ...
    keep = case.hub.retain_native_validation_report

    async def full_disk(*_):  # ... and its report cannot be written.
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(case.hub, "retain_native_validation_report", full_disk)
    try:
        await call(runner, attempt, "author", "stage", args, "stage-1")
    except OSError:
        pass
    artifact_id = await case.hub.db.call(lambda c: c.execute(
        "SELECT validation_artifact_id FROM native_call_receipts WHERE call_id='stage-1'").fetchone()[0])
    row = await receipt(case.hub, artifact_id)
    assert row["validation_state"] == "queued" and "could not be kept" in row["validation_reason"]
    monkeypatch.setattr(case.hub, "retain_native_validation_report", keep)
    [pending] = await case.hub.pending_native_validations(case.run["id"])
    result = await runner._recover_submission(pending, background=True)
    assert result["evidence"][-1]["verdict"] == "accepted" and plugin.started == 2


async def test_a_running_validation_no_wait_holds_goes_back_as_a_restart_would_put_it(native_runtime, monkeypatch):
    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = Outliving()
    staged = await call(runner, attempt, "author", "stage", args, "stage-1")  # Unfinished: queued.
    artifact_id = staged["artifact_id"]

    def running(c):  # Its wait ended, and could not even record that it had.
        c.execute("UPDATE native_call_receipts SET validation_state='running' WHERE validation_artifact_id=?",
                  (artifact_id,))
    await case.hub.db.call(running, write=True)
    monkeypatch.setattr(GeneralPlugin, "validations_survive_interruption", True, raising=False)
    with case.hub.attached_validation(artifact_id):  # A wait holds it: it is left alone.
        assert await case.hub.pending_native_validations(case.run["id"]) == []
        assert (await receipt(case.hub, artifact_id))["validation_state"] == "running"
    [pending] = await case.hub.pending_native_validations(case.run["id"])
    assert pending["validation_state"] == "queued"
    assert "wait ended without a result" in (await receipt(case.hub, artifact_id))["validation_reason"]
    events = [event for event in await case.hub.events(limit=1000) if event["kind"] == "validation.stranded"]
    assert [(event["object_id"], event["payload"]["state"]) for event in events] == [(artifact_id, "queued")]

    await case.hub.db.call(running, write=True)
    monkeypatch.setattr(GeneralPlugin, "validations_survive_interruption", False, raising=False)
    assert await case.hub.pending_native_validations(case.run["id"]) == []
    assert (await receipt(case.hub, artifact_id))["validation_state"] == "uncertain"  # A local checker: recovery.


async def test_the_driver_finishes_a_validation_whose_wait_failed_on_a_full_disk(native_runtime, monkeypatch):
    from formal_geyser.coordination.runtime import runner as runner_module

    case, attempt, runner, _, args = await prepare(native_runtime)
    runner.plugin = plugin = Outliving()
    staged = await call(runner, attempt, "author", "stage", args, "stage-1")  # Unfinished: queued.
    artifact_id = staged["artifact_id"]
    await case.hub.ordinary_turn_completed(attempt["worker_id"], attempt["id"])
    plugin.release.set()
    monkeypatch.setattr(GeneralPlugin, "validations_survive_interruption", True, raising=False)
    monkeypatch.setattr(runner_module, "VALIDATION_RETRY_SECONDS", 0.0)
    keep, queue = case.hub.retain_native_validation_report, case.hub.native_validation_queued
    failures = {"report": 1, "queue": 1}

    async def report(*arguments):
        if failures["report"]:
            failures["report"] -= 1
            raise OSError(28, "No space left on device")
        return await keep(*arguments)

    async def queued(*arguments, **keywords):
        if failures["queue"]:
            failures["queue"] -= 1
            raise OSError(28, "database or disk is full")
        return await queue(*arguments, **keywords)

    async def nothing(*_):
        return None

    async def none(*_):
        return []

    def no_model():
        raise AssertionError("Collecting a validation starts no model turn")

    monkeypatch.setattr(case.hub, "retain_native_validation_report", report)
    monkeypatch.setattr(case.hub, "native_validation_queued", queued)
    monkeypatch.setattr(runner, "_select", nothing)
    monkeypatch.setattr(case.hub, "due_recruitment_intents", none)
    monkeypatch.setattr(case.hub, "due_assignment_intents", none)
    runner.harness_factory = no_model
    await asyncio.wait_for(runner.run_until_idle(case.run["id"]), 60)
    assert failures == {"report": 0, "queue": 0} and plugin.started == 3
    assert (await case.hub.native_validation(artifact_id))["validation_state"] == "completed"
    assert await finished(case.hub, artifact_id)
    events = [event["kind"] for event in await case.hub.events(limit=1000) if event["object_id"] == artifact_id]
    assert events.count("validation.stranded") == 1
