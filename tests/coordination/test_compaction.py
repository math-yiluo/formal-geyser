"""Compaction through the real Hub without model inference.

Codex keeps every input across compaction and the required guides are standing
instructions, so nothing is sent again. A harness whose compaction summarizes the
inputs too, like Claude, gets its current task view with the next tool result.
"""
from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    CodexConfig,
    Command,
    ProjectConfig,
    Role,
)
from formal_geyser.coordination.runtime import (
    ScriptedAction,
    ScriptedHarness,
    ScriptedTurn,
    TurnOutcome,
    WorkerRunner,
)
from formal_geyser.coordination.runtime.codex import CodexHarness, SteerOutcome
from formal_geyser.coordination.runtime.runner import (
    COMPACTION_REFRESH,
    CONTROL_WATCH_SECONDS,
)
from formal_geyser.coordination.runtime.tools import operation_tools
from tests.bootstrap_helpers import synthetic_execution
from tests.coordination.lifecycle_helpers import resolve_recruitment


async def command(hub, name, payload=None):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}),
                             Actor(role=Role.OPERATOR))


@pytest.fixture
async def project(tmp_path):
    initialize(tmp_path, ProjectConfig(
        name="Synthetic proactive compaction", domain="lean",
        codex=CodexConfig(home=str(tmp_path / "isolated-codex-home"), stop_grace_seconds=0.5)))
    hub = synthetic_execution(Hub(tmp_path))
    task = await command(hub, "task.publish", {"contract": {
        "title": "Inspect synthetic interfaces", "domain": "lean", "acceptance_policy": "lean.peer@1",
        "objective": "Preserve this exact inspection objective and its acceptance conditions. " * 8}})
    run = await command(hub, "run.start")
    case = SimpleNamespace(root=tmp_path, hub=hub, task=task, run=run)
    try:
        yield case
    finally:
        await hub.close()


@dataclass
class PendingSteer:
    text: str
    expected_turn_id: str
    input_id: str
    reply: asyncio.Future

    @property
    def packet(self):
        return json.loads(self.text)

    def respond(self, status="accepted", *, turn_id=None, error=None):
        self.reply.set_result(SteerOutcome(status, turn_id or self.expected_turn_id, error))


class SteeringHarness(ScriptedHarness):
    """Keep receipts controllable independently of compaction and terminal events."""

    def __init__(self):
        super().__init__()
        self.started = asyncio.Event()
        self.finished = asyncio.Event()
        self.closed = asyncio.Event()
        self.calls = asyncio.Queue()
        self.active_turn_id = None
        self.turn_id = "compaction-turn-" + uuid4().hex
        self.interrupted = False
        self.inflight = 0
        self.peak_inflight = 0
        self.all_calls = []

    async def open(self, **options):
        self.instructions = options["instructions"]
        return await super().open(**options)

    def set_compaction_handler(self, callback):
        self.compacted = callback

    async def run_turn(self, prompt, *, on_tool, on_usage, on_started):
        self.prompts.append(prompt)
        self.on_tool, self.on_usage = on_tool, on_usage
        self.active_turn_id = self.turn_id
        try:
            await on_started(self.turn_id)
            self.started.set()
            await self.finished.wait()
            return TurnOutcome(self.turn_id, "interrupted" if self.interrupted else "completed",
                               True, True, visible_output="Native work finished.")
        finally:
            self.active_turn_id = None
            await self.close()

    async def steer(self, text, *, expected_turn_id, client_input_id):
        pending = PendingSteer(text, expected_turn_id, client_input_id,
                               asyncio.get_running_loop().create_future())
        self.all_calls.append(pending)
        self.inflight += 1
        self.peak_inflight = max(self.peak_inflight, self.inflight)
        self.calls.put_nowait(pending)
        try:
            return await pending.reply
        finally:
            self.inflight -= 1

    def finish(self):
        self.active_turn_id = None
        self.finished.set()

    async def interrupt(self):
        self.interrupted = True
        self.finish()

    async def close(self):
        await super().close()
        self.closed.set()


async def eventually(predicate):
    async with asyncio.timeout(10):
        while not predicate():
            await asyncio.sleep(0.01)


async def settle():
    """Long enough for any input the runtime would send: one control read and more."""
    await asyncio.sleep(CONTROL_WATCH_SECONDS + 0.2)


async def launch(project, monkeypatch, *, keeps_inputs=True):
    harness = SteeringHarness()
    # False stands for a harness whose compaction summarizes the inputs too, like Claude.
    harness.compaction_keeps_inputs = keeps_inputs
    runner = WorkerRunner(project.hub, lambda: harness)
    acks, tools = [], []
    original_ack, original_tool = runner._ack_native, runner._tool

    async def ack(packet, attempt, inbox):
        await original_ack(packet, attempt, inbox)
        acks.append(packet)

    async def tool(*args, **kwargs):
        tools.append((args, kwargs))
        return await original_tool(*args, **kwargs)

    monkeypatch.setattr(runner, "_ack_native", ack)
    monkeypatch.setattr(runner, "_tool", tool)
    task = asyncio.create_task(runner.run_once(project.run["id"]))
    started = asyncio.create_task(harness.started.wait())
    try:
        done, _ = await asyncio.wait([task, started], timeout=10,
                                     return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task
        assert started in done, "The synthetic native turn did not start"
    except BaseException:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await harness.close()
        raise
    finally:
        started.cancel()
        await asyncio.gather(started, return_exceptions=True)
    worker = (await project.hub.list_workers())[0]
    return SimpleNamespace(harness=harness, runner=runner, task=task, worker_id=worker["id"],
                           acks=acks, tools=tools)


async def finish(case):
    case.harness.finish()
    return await asyncio.wait_for(case.task, 10)


async def cleanup(case):
    if not case.task.done():
        case.task.cancel()
    await asyncio.gather(case.task, return_exceptions=True)
    await case.harness.close()


def inputs(case, runner=None):
    return [event for event in (runner or case.runner).transcripts.page(case.worker_id, limit=500)["events"]
            if event["type"] == "input"]


def standing_instructions(runner, worker_id):
    """The developer instructions recorded for the worker's latest activation."""
    return [event["instructions"]["instructions"]
            for event in runner.transcripts.page(worker_id, limit=500)["events"]
            if event["type"] == "instructions"][-1]


async def receipt(case):
    pending = asyncio.create_task(case.harness.calls.get())
    try:
        done, _ = await asyncio.wait([case.task, pending], timeout=10,
                                     return_when=asyncio.FIRST_COMPLETED)
        if pending in done:
            return pending.result()
        if case.task in done:
            await case.task
        raise AssertionError("The active turn did not receive its expected steering input")
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


async def deliveries(project, case):
    """The states of the worker's context deliveries, oldest first."""
    return await project.hub.db.call(lambda c: [row[0] for row in c.execute(
        "SELECT state FROM deliveries WHERE worker_id=? ORDER BY rowid", (case.worker_id,))])


async def compactions(project, worker_id):
    return await project.hub.db.call(lambda c: c.execute(
        "SELECT count(*) FROM events WHERE kind='worker.context_compacted' AND object_id=?",
        (worker_id,)).fetchone()[0])


def baseline(case):
    return json.loads(case.runner.continuation_prompts._path(case.worker_id).read_text(encoding="utf-8"))


def excerpt(project):
    """The objective as a task view carries it; all of it stays in paths.task."""
    return project.task["contract"]["objective"].strip()[:300] + "\u2026"


async def compact(case):
    await asyncio.wait_for(case.harness.compacted(), 10)


async def checkpoint(case, call_id):
    return await asyncio.wait_for(case.harness.on_tool("taskhub_call", {"operation": "checkpoint", "arguments": {
        "checkpoint": {"summary": "Synthetic saved state", "next_action": "Inspect the next file."}}}, call_id), 10)


def assert_reading(view, workspace, instructions):
    """The required guides are standing instructions; reading lists only the on-demand guides."""
    reading = view["reading"]
    assert "required" not in reading
    assert ".private/instructions/shared-dags.md" in reading["on_demand"]
    assert all((workspace / path).is_file() for path in reading["on_demand"])
    for path in (".private/instructions/worker.md", ".private/instructions/lean.md"):
        assert path not in reading["on_demand"]
        assert (workspace / path).read_text(encoding="utf-8").strip() in instructions
    assert not {"context_epoch", "packet_id", "execution_mode", "instruction_guidance", "native"} & view.keys()


async def test_compaction_sends_nothing_again_when_the_harness_keeps_its_inputs(project, monkeypatch):
    case = await launch(project, monkeypatch)
    try:
        initial = case.acks[0]
        assert initial["updates"] and initial["through_sequence"]
        await case.harness.on_usage({"totalTokens": 10})
        await compact(case)
        await settle()
        # The kept inputs hold the task and the guides are standing instructions: no steer, no
        # new input or delivery, and the continuation baseline stays confirmed.
        assert not case.harness.all_calls and not case.tools and len(case.acks) == 1
        assert [item["source"] for item in inputs(case)] == ["taskhub"]
        assert await deliveries(project, case) == ["delivered"]
        assert await compactions(project, case.worker_id) == 1
        assert not case.runner.continuation_prompts.compaction_pending(case.worker_id)
        assert baseline(case)["status"] == "confirmed"
        assert (await project.hub.worker(case.worker_id))["context_epoch"] == initial["context_epoch"] == 1
        # The delivery cursor continues: nothing already delivered is offered again.
        current = await project.hub.context(case.worker_id)
        assert current["context_epoch"] == 1 and current["after_sequence"] == initial["through_sequence"]
        assert not ({update["sequence"] for update in initial["updates"]}
                    & {update["sequence"] for update in current["updates"]})
        # Nor does a later tool result carry the context again.
        saved = await checkpoint(case, "operation-after-compaction")
        assert "error" not in saved and "context" not in saved and len(case.tools) == 1
        await case.harness.on_usage({"totalTokens": 15})
        result = await finish(case)
        assert result["turn_status"] == "completed" and case.harness.process_quiet
        assert not case.harness.all_calls and len(case.acks) == 1
        worker = await project.hub.worker(case.worker_id)
        assert worker["context_epoch"] == 1 and worker["usage_epoch"] == 1
        assert (await project.hub.run(project.run["id"]))["tokens_used"] == 15
    finally:
        await cleanup(case)


async def test_old_refresh_receipt_cannot_acknowledge_a_newer_compaction(project, monkeypatch):
    case = await launch(project, monkeypatch, keeps_inputs=False)
    try:
        await compact(case)
        first = await checkpoint(case, "operation-after-compaction")
        assert "error" not in first and first["context"]["instruction"] == COMPACTION_REFRESH
        # A second compaction overtakes the first view before its reply is confirmed sent.
        await compact(case)
        await first.after_send()
        assert len(case.acks) == 1
        assert await deliveries(project, case) == ["delivered", "pending"]
        second = await checkpoint(case, "operation-after-second-compaction")
        assert "error" not in second and second["context"]["instruction"] == COMPACTION_REFRESH
        assert "context_epoch" not in first["context"] and "context_epoch" not in second["context"]
        await second.after_send()
        assert len(case.acks) == 2
        assert await deliveries(project, case) == ["delivered", "pending", "delivered"]
        retried = await checkpoint(case, "operation-after-refresh")
        assert "error" not in retried and "context" not in retried and len(case.tools) == 3
        assert not case.harness.all_calls
        assert (await project.hub.worker(case.worker_id))["context_epoch"] == 1
        await finish(case)
    finally:
        await cleanup(case)


async def test_unconfirmed_refresh_keeps_working_and_rides_the_next_tool_result(project, monkeypatch):
    case = await launch(project, monkeypatch, keeps_inputs=False)
    try:
        await compact(case)
        # No steer and no blocked call: the current task view rides the next tool result.
        unconfirmed = await checkpoint(case, "refresh-unconfirmed")
        assert "error" not in unconfirmed and len(case.tools) == 1
        view = unconfirmed["context"]
        assert view["instruction"] == COMPACTION_REFRESH
        assert view["task"]["objective_excerpt"] == excerpt(project) and view["paths"]["task"]
        assert view["paths"]["knowledge_readme"] and "wiki" not in view
        assert_reading(view, project.hub.workspace(case.worker_id), case.harness.instructions)
        # Its reply is never confirmed sent, so it is not acknowledged; the worker keeps working.
        assert not case.task.done() and not case.harness.interrupted
        assert (await project.hub.run(project.run["id"]))["state"] == "running"
        assert len(case.acks) == 1 and not case.harness.all_calls
        assert await deliveries(project, case) == ["delivered", "pending"]
        refreshed = await checkpoint(case, "refresh-fallback")
        assert "error" not in refreshed and refreshed["context"]["instruction"] == COMPACTION_REFRESH
        assert refreshed.after_send is not None and len(case.tools) == 2
        await refreshed.after_send()
        assert len(case.acks) == 2
        assert await deliveries(project, case) == ["delivered", "pending", "delivered"]
        result = await checkpoint(case, "after-refresh")
        assert "error" not in result and "context" not in result and len(case.tools) == 3
        assert (await finish(case))["turn_status"] == "completed"
    finally:
        await cleanup(case)


async def test_finished_turn_leaves_the_refresh_to_the_next_full_input_without_a_new_turn(project, monkeypatch):
    case = await launch(project, monkeypatch, keeps_inputs=False)
    try:
        await compact(case)
        result = await finish(case)
        assert result["turn_status"] == "completed"
        assert (await project.hub.run(project.run["id"]))["state"] == "running"
        assert len(case.harness.prompts) == 1 and not case.harness.all_calls and not case.tools
        assert len(case.acks) == 1
        assert case.runner.continuation_prompts.compaction_pending(case.worker_id)
        await resolve_recruitment(project.hub, project.run["id"])
        resumed = ScriptedHarness([ScriptedTurn([])])
        runner = WorkerRunner(project.hub, lambda: resumed)
        await asyncio.wait_for(runner.run_once(project.run["id"]), 10)
        assert resumed.opened_threads == [case.harness.session.thread_id]
        assert inputs(case, runner)[-1]["context_mode"] == "full"
        packet = json.loads(resumed.prompts[0])
        assert packet["paths"]["knowledge_readme"] and "wiki" not in packet
        assert packet["task"]["objective_excerpt"] == excerpt(project) and packet["paths"]["task"]
        assert_reading(packet, project.hub.workspace(case.worker_id),
                       standing_instructions(runner, case.worker_id))
        assert (await project.hub.worker(case.worker_id))["context_epoch"] == 1
    finally:
        await cleanup(case)


async def test_terminal_after_compaction_sends_nothing_and_the_conversation_continues_in_deltas(
    project, monkeypatch,
):
    case = await launch(project, monkeypatch)
    try:
        await compact(case)
        case.harness.finish()
        outcome = await asyncio.wait_for(case.task, 10)
        assert outcome["turn_status"] == "completed"
        assert not case.harness.all_calls and len(case.acks) == 1
        assert (await project.hub.worker(case.worker_id))["context_epoch"] == 1
        assert (await project.hub.run(project.run["id"]))["state"] == "running"
        assert not case.runner.continuation_prompts.compaction_pending(case.worker_id)
        await resolve_recruitment(project.hub, project.run["id"])
        resumed = ScriptedHarness([ScriptedTurn([])])
        runner = WorkerRunner(project.hub, lambda: resumed)
        await asyncio.wait_for(runner.run_once(project.run["id"]), 10)
        assert resumed.opened_threads == [case.harness.session.thread_id]
        assert inputs(case, runner)[-1]["context_mode"] == "delta"
        packet = json.loads(resumed.prompts[0])
        # The kept inputs already hold the task view; the guides are standing instructions.
        assert not {"paths", "reading"} & packet.keys() and "objective_excerpt" not in packet["task"]
        assert excerpt(project).removesuffix("\u2026") not in resumed.prompts[0]
    finally:
        await cleanup(case)


@pytest.mark.parametrize("tool_result", [True, False])
async def test_compaction_refresh_preserves_waiting_message_execution_mode(project, monkeypatch, tool_result):
    pause = ScriptedHarness([ScriptedTurn([ScriptedAction("taskhub_call", {"operation": 'pause', "arguments": {"predicate": {"external_response_id": "unanswered-request"}, "reason": "Preserve progress until actionable work is available.", **({
        "reason": "Await a concrete user instruction.",
        "checkpoint": {"summary": "Inspection preserved.", "next_action": "Read a directed message."},
    })}})])])
    paused = await WorkerRunner(project.hub, lambda: pause).run_once(project.run["id"])
    await command(project.hub, "message.send", {
        "worker_id": paused["worker_id"], "message_id": "waiting-guidance",
        "text": "Keep the underlying task paused after this correspondence."})
    case = await launch(project, monkeypatch, keeps_inputs=False)
    try:
        initial = json.loads(case.harness.prompts[0])
        assert "execution_mode" not in initial
        assert initial["message_state"]["after_turn_intention"] == "waiting"
        assert case.acks[0]["execution_mode"] == "message"
        await compact(case)
        workspace = project.hub.workspace(case.worker_id)
        if tool_result:
            described = await asyncio.wait_for(
                case.harness.on_tool("taskhub_actions", {}, "describe-after-compaction"), 10)
            refresh = described["context"]
            assert "error" not in described and refresh["instruction"] == COMPACTION_REFRESH
            assert_reading(refresh, workspace, case.harness.instructions)
            assert refresh["message_state"]["after_turn_intention"] == "waiting"
            assert refresh["message_state"]["resume_operation"] == "resume_work"
            assert refresh["task"]["objective_excerpt"] == excerpt(project) and refresh["paths"]["task"]
            assert not refresh.get("messages")
            await described.after_send()
            assert len(case.acks) == 2 and case.acks[1]["execution_mode"] == "message"
        await finish(case)
        assert (await project.hub.worker(case.worker_id))["work_intention"] == "waiting"
        assert not case.harness.all_calls and len(case.tools) == (1 if tool_result else 0)
        if not tool_result:
            assert len(case.acks) == 1
            await command(project.hub, "message.send", {
                "worker_id": case.worker_id, "message_id": "following-correspondence",
                "text": "Preserve the original wait intention."})
            resumed = ScriptedHarness([ScriptedTurn([])])
            runner = WorkerRunner(project.hub, lambda: resumed)
            await asyncio.wait_for(runner.run_once(project.run["id"]), 10)
            packet = json.loads(resumed.prompts[0])
            assert packet["message_state"]["after_turn_intention"] == "waiting"
            # The task view the compaction left pending arrives with the next input.
            assert packet["task"]["objective_excerpt"] == excerpt(project) and packet["paths"]["task"]
            assert_reading(packet, workspace, standing_instructions(runner, case.worker_id))
            assert (await project.hub.worker(case.worker_id))["work_intention"] == "waiting"
    finally:
        await cleanup(case)


@pytest.mark.parametrize("keeps_inputs", [True, False])
async def test_compaction_neither_replays_nor_copies_a_steered_message(project, monkeypatch, keeps_inputs):
    case = await launch(project, monkeypatch, keeps_inputs=keeps_inputs)
    human_acks = []
    original = case.runner._resolve_inbox

    async def resolve(attempt, inbox, **kwargs):
        await original(attempt, inbox, **kwargs)
        if kwargs["accepted"] and inbox.get("_human", {}).get("delivery_id"):
            human_acks.append(inbox["_human"]["delivery_id"])

    monkeypatch.setattr(case.runner, "_resolve_inbox", resolve)
    try:
        await compact(case)
        first_line = "An exact private user instruction."
        text = first_line + "\nKeep this message out of refresh packets."
        await command(project.hub, "message.send", {
            "worker_id": case.worker_id, "message_id": "after-compaction", "text": text})
        assert not (await project.hub.human_history(case.worker_id))["messages"][0]["acknowledged_at"]
        message = await receipt(case)
        delivered = message.packet["messages"][0]
        assert text in (project.hub.workspace(case.worker_id) / delivered["file"]["path"]).read_text(encoding="utf-8")
        assert "complete" not in delivered and "text" not in delivered
        assert message.expected_turn_id == case.harness.turn_id
        message.respond()
        await eventually(lambda: len(human_acks) == 1)
        await compact(case)
        result = await checkpoint(case, "operation-after-second-compaction")
        assert "error" not in result
        if keeps_inputs:
            assert "context" not in result
        else:
            refresh = result["context"]
            assert refresh["instruction"] == COMPACTION_REFRESH
            assert first_line not in json.dumps(refresh, ensure_ascii=False)
            assert not refresh.get("messages") and "private_talk" not in refresh
            await result.after_send()
        await settle()  # A delivered message is never steered again.
        await finish(case)
        history = await project.hub.human_history(case.worker_id)
        assert len(history["messages"]) == 1 and history["messages"][0]["acknowledged_at"]
        assert len(human_acks) == 1
        assert case.harness.peak_inflight == 1
        assert len(case.harness.all_calls) == 1
        assert len(case.acks) == (1 if keeps_inputs else 2)
    finally:
        await cleanup(case)


FAKE_COMPACTING_NATIVE = r'''
import json
import sys
from pathlib import Path

log = Path(sys.argv[1])

def emit(value):
    print(json.dumps(value), flush=True)

for line in sys.stdin:
    msg = json.loads(line)
    with log.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(msg) + "\n")
    method = msg.get("method")
    if method == "initialize":
        emit({"id": msg["id"], "result": {"userAgent": "Codex Desktop/" + sys.argv[2]}})
    elif method in {"thread/start", "thread/resume"}:
        emit({"id": msg["id"], "result": {
            "thread": {"id": "synthetic-native-thread"}, "model": "synthetic-model"}})
    elif method == "turn/start":
        emit({"id": msg["id"], "result": {"turn": {"id": "native-turn", "status": "inProgress"}}})
        emit({"method": "turn/started", "params": {"threadId": "synthetic-native-thread",
             "turn": {"id": "native-turn", "status": "inProgress"}}})
        for kind in ["item/started", "item/completed"]:
            emit({"method": kind, "params": {"threadId": "synthetic-native-thread",
                 "turnId": "native-turn", "item": {"id": "compact-once", "type": "contextCompaction"}}})
        emit({"method": "thread/compacted", "params": {
             "threadId": "synthetic-native-thread", "turnId": "native-turn"}})
        emit({"method": "item/completed", "params": {"threadId": "synthetic-native-thread",
             "turnId": "native-turn", "item": {"id": "native-only", "type": "agentMessage",
             "text": "Continue using only native files and commands."}}})
        emit({"method": "turn/completed", "params": {"threadId": "synthetic-native-thread",
             "turn": {"id": "native-turn", "status": "completed"}}})
    elif method == "turn/steer":
        # Nothing is steered after a compaction; answer instead of leaving the call waiting.
        emit({"id": msg["id"], "error": {"code": -32000, "message": "No active turn"}})
'''


async def test_native_compaction_sends_nothing_and_the_turn_ends_normally(project):
    script = project.root / "compacting_native.py"
    script.write_text(FAKE_COMPACTING_NATIVE, encoding="utf-8")
    log = project.root / "native-protocol.jsonl"
    harness = CodexHarness(project.hub.config.codex,
                           command=[sys.executable, "-u", str(script), str(log),
                                    project.hub.config.codex.version])
    runner = WorkerRunner(project.hub, lambda: harness)
    try:
        result = await asyncio.wait_for(runner.run_once(project.run["id"]), 15)
        assert result["turn_status"] == "completed" and harness.process_quiet
        worker = await project.hub.worker(result["worker_id"])
        assert worker["context_epoch"] == 1 and worker["active_attempt"] is None
        # The paired compaction item and legacy notification are one compaction.
        assert await compactions(project, result["worker_id"]) == 1
        frames = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
        assert not [frame for frame in frames if frame.get("method") == "turn/steer"]
        assert sum(frame.get("method") == "turn/start" for frame in frames) == 1
        events = runner.transcripts.page(result["worker_id"], limit=500)["events"]
        assert [event["source"] for event in events if event["type"] == "input"] == ["taskhub"]
        assert (await project.hub.run(project.run["id"]))["state"] == "running"
        # The continuation baseline survives, so the next input in this conversation is a delta.
        assert not runner.continuation_prompts.compaction_pending(result["worker_id"])
        saved = json.loads(runner.continuation_prompts._path(result["worker_id"]).read_text(encoding="utf-8"))
        assert saved["status"] == "confirmed"
    finally:
        await harness.close()


def test_integration_belongs_to_the_integrator_not_reviewers():
    tools = {tool["name"]: tool for tool in operation_tools()}
    assert not any(name.startswith("taskhub_merge_") for name in tools)
    assert "integration_id" not in tools["taskhub_review"]["inputSchema"]["properties"]
