"""Continuous driver behavior with finite work and explicit operator controls."""
from __future__ import annotations

import asyncio
from collections import deque
from types import SimpleNamespace
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import Actor, Command, ProjectConfig, Role
from formal_geyser.coordination.runtime import (
    ScriptedAction,
    ScriptedHarness,
    ScriptedTurn,
    TurnOutcome,
    WorkerRunner,
)
from formal_geyser.coordination.runtime.rpc import JsonRpcClient, RpcClosed
from formal_geyser.coordination.storage import canonical


async def command(hub, name, payload):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload),
                             Actor(role=Role.OPERATOR))


@pytest.fixture
async def hub(tmp_path):
    initialize(tmp_path, ProjectConfig())
    instance = Hub(tmp_path)
    yield instance
    await instance.close()


async def start_run(hub):
    return await command(hub, "run.start", {})


async def test_an_operator_claim_is_launched_once_by_the_pass(hub):
    # In the Linux pilot a task.join left Worker 303's claim running for eight hours: no pass
    # launched it. A replayed command hands the same claim over again.
    task = await command(hub, "task.publish", {"contract": {"title": "Joined work", "objective": "Work on it."}})
    run = await start_run(hub)
    worker = await command(hub, "worker.register", {})
    claim = await command(hub, "task.claim", {"task_id": task["id"], "revision": 1,
                                              "worker_id": worker["id"], "run_id": run["id"]})
    assert await hub.unlaunched_claim(claim["id"])
    pause = ScriptedAction("taskhub_call", {"operation": "pause", "arguments": {"predicate": {"external_response_id": "unanswered-request"}, 
        "reason": "Nothing more to do in this test.", "checkpoint": {"summary": "Done", "next_action": "Wait"}}})
    harnesses = []

    def harness():
        harnesses.append(ScriptedHarness([ScriptedTurn([pause])]))
        return harnesses[-1]
    claims = deque([claim, claim])
    await WorkerRunner(hub, harness).run_until_idle(run["id"], claims=claims)
    assert not claims and len(harnesses) == 1 and len(harnesses[0].prompts) == 1
    turns = await hub.db.call(lambda c: c.execute(
        "SELECT count(*) FROM turn_intents WHERE attempt_id=?", (claim["id"],)).fetchone()[0])
    assert turns == 1 and not await hub.unlaunched_claim(claim["id"])
    assert (await hub.worker(worker["id"]))["work_intention"] == "waiting"


async def test_an_operator_claim_in_a_paused_run_is_held_before_start(hub):
    task = await command(hub, "task.publish", {"contract": {"title": "Joined work", "objective": "Work on it."}})
    run = await start_run(hub)
    worker = await command(hub, "worker.register", {})
    claim = await command(hub, "task.claim", {"task_id": task["id"], "revision": 1,
                                              "worker_id": worker["id"], "run_id": run["id"]})
    await command(hub, "run.pause", {"run_id": run["id"]})
    harnesses = []
    await WorkerRunner(hub, lambda: harnesses.append(ScriptedHarness()) or harnesses[-1]).run_until_idle(
        run["id"], claims=deque([claim]))
    attempt = await hub.db.call(lambda c: dict(c.execute(
        "SELECT state,ended_at FROM attempts WHERE id=?", (claim["id"],)).fetchone()))
    # Nothing ran: the claim waits as before it was made, and the run's drain is not held by it.
    assert attempt["state"] == "preflight_held" and attempt["ended_at"] is not None
    assert not any(harness.prompts for harness in harnesses)
    assert (await hub.worker(worker["id"]))["active_attempt"] is None


async def test_driver_keeps_exact_count_with_bounded_history_and_prior_failure(hub):
    run = await start_run(hub)
    calls = 0

    class ManyOutcomes(WorkerRunner):
        admitted = 0

        async def _select(self, run_id):
            self.admitted += 1
            return {"id": str(self.admitted)} if self.admitted <= 205 else None

        async def run_once(self, run_id, *, assignment=None):
            nonlocal calls
            calls += 1
            if calls > 205:
                return None
            return {"task_id": str(calls), "directive": "verify", "turn_status": "completed",
                    "error": "An earlier observed failure" if calls == 1 else None}

    result = await ManyOutcomes(hub).run_until_idle(run["id"])
    assert result["state"] == "idle"
    assert result["steps"] == 205
    assert result["results_retained"] == 100 and result["results_truncated"] == 105
    # Concurrent completions have no artificial global ordering or worker quota.
    assert len({item["task_id"] for item in result["results"]}) == 100
    assert all(1 <= int(item["task_id"]) <= 205 for item in result["results"])
    assert result["failure_observed"] is True
    assert (await hub.run(run["id"]))["state"] == "running"


async def test_driver_finishes_available_work_and_materializes_large_result(hub):
    run = await start_run(hub)
    calls = 0
    large = {"task_id": "large-result", "directive": "verify", "turn_status": "completed",
             "validation": {"details": "x" * 100000}}

    class LargeOutcomes(WorkerRunner):
        admitted = 0

        async def _select(self, run_id):
            self.admitted += 1
            return {"id": str(self.admitted)} if self.admitted <= 2 else None

        async def run_once(self, run_id, *, assignment=None):
            nonlocal calls
            calls += 1
            if calls > 2:
                return None
            return large

    runner = LargeOutcomes(hub)
    result = await runner.run_until_idle(run["id"])
    assert calls == 2 and result["steps"] == 2
    assert result["state"] == "idle"
    for retained in result["results"]:
        assert len(canonical(retained).encode()) <= runner.retained_result_bytes
        assert hub.blobs.get_json(retained["material_sha256"]) == large
        assert retained["turn_status"] == "completed" and "error" not in retained


async def test_driver_stops_admitting_after_operator_pause(hub):
    run = await start_run(hub)
    calls = 0

    class PausingRunner(WorkerRunner):
        admitted = False

        async def _select(self, run_id):
            if self.admitted:
                return None
            self.admitted = True
            return {"id": "one"}

        async def run_once(self, run_id, *, assignment=None):
            nonlocal calls
            calls += 1
            await command(hub, "run.pause", {"run_id": run_id})
            return {"directive": "parked", "stop_reason": "operator_pause",
                    "turn_status": "interrupted"}

    result = await PausingRunner(hub).run_until_idle(run["id"])
    assert calls == 1 and result["state"] == "paused"
    assert not result["failure_observed"]


async def test_turn_keeps_running_until_explicit_pause(hub):
    run = await start_run(hub)
    task = await command(hub, "task.publish", {"contract": {
        "title": "Long native operation", "objective": "Wait for the operation or user control."}})
    started, stopped = asyncio.Event(), asyncio.Event()

    class LongHarness(ScriptedHarness):
        async def interrupt(self):
            stopped.set()

        async def run_turn(self, prompt, *, on_tool, on_usage, on_started):
            turn_id = uuid4().hex
            await on_started(turn_id)
            await on_usage({"totalTokens": 20})
            self.workspaces[0].joinpath("progress.txt").write_text("Durable progress.")
            started.set()
            await stopped.wait()
            await self.close()
            return TurnOutcome(turn_id, "interrupted", True, True)

    running = asyncio.create_task(WorkerRunner(hub, LongHarness).run_until_idle(run["id"]))
    await asyncio.wait_for(started.wait(), 10)
    await asyncio.sleep(1.1)
    assert not running.done() and not stopped.is_set()
    assert "turn_seconds" not in (await hub.run(run["id"]))["limits"]
    await command(hub, "run.pause", {"run_id": run["id"]})
    result = await asyncio.wait_for(running, 10)
    assert result["steps"] == 1 and result["state"] == "paused"
    assert result["results"][0]["stop_reason"] == "operator_pause"
    assert result["results"][0]["directive"] == "parked"
    assert (await hub.task(task["id"]))["state"] != "done"
    assert (await hub.list_interruptions())[0]["state"] == "parked"


async def test_one_activation_failure_does_not_cancel_independent_worker(hub):
    run = await start_run(hub)
    peer_finished = False

    class IndependentOutcomes(WorkerRunner):
        admitted = 0

        async def _select(self, run_id):
            self.admitted += 1
            return {"id": str(self.admitted)} if self.admitted <= 2 else None

        async def run_once(self, run_id, *, assignment=None):
            nonlocal peer_finished
            if assignment["id"] == "1":
                raise OSError("Synthetic local activation failure")
            await asyncio.sleep(0.05)
            peer_finished = True
            return {"directive": "waiting", "turn_status": "completed", "task_id": "peer"}

    result = await IndependentOutcomes(hub).run_until_idle(run["id"])
    assert peer_finished and result["steps"] == 2
    assert result["state"] == "needs_attention" and result["failure_observed"]
    assert any(item.get("error_code") == "OSError" for item in result["results"])
    assert (await hub.run(run["id"]))["state"] == "running"


async def test_writer_pipe_reset_has_consistent_closed_transport_classification():
    class ResetPipe:
        def write(self, frame):
            pass

        async def drain(self):
            raise ConnectionResetError("Receiver has closed")

    client = JsonRpcClient.__new__(JsonRpcClient)
    client.host = SimpleNamespace(stdin=ResetPipe())
    client._outgoing = asyncio.Queue()
    client._failure = asyncio.get_running_loop().create_future()
    client._pending = {}
    ack = asyncio.get_running_loop().create_future()
    await client._outgoing.put((b"{}\n", ack))
    await client._writer()
    with pytest.raises(RpcClosed, match="ConnectionResetError"):
        await ack
    assert client.failure is ack.exception()
    await asyncio.wait_for(client._outgoing.join(), 1)
