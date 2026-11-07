"""Native sessions, steering races, and visible transcripts using no model inference."""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
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
    WorkerRunner,
)
from formal_geyser.coordination.runtime.codex import (
    CodexHarness,
    SteerOutcome,
    TurnOutcome,
)
from formal_geyser.coordination.runtime.tools import dynamic_tools
from tests.coordination.communication_helpers import native_text
from tests.coordination.gateway_helpers import file_arguments
from tests.coordination.lifecycle_helpers import record_saved_turn

NATIVE_PEER = r'''
import json,sys
from pathlib import Path
log=Path(sys.argv[1]); mode=sys.argv[2]; thread='original'; active=False
def emit(value): print(json.dumps(value),flush=True)
for line in sys.stdin:
    msg=json.loads(line)
    with log.open('a',encoding='utf-8') as stream: stream.write(json.dumps(msg)+'\n')
    method=msg.get('method'); p=msg.get('params',{})
    if method=='initialize': emit({'id':msg['id'],'result':{'userAgent':'codex-cli/0.160.0'}})
    elif method in ('thread/start','thread/resume'):
        thread=p.get('threadId','original')
        emit({'id':msg['id'],'result':{'thread':{'id':thread},'model':'fake'}})
    elif method=='thread/read':
        emit({'id':msg['id'],'result':{'thread':{'id':p['threadId'],'turns':[{'id':'old','items':[{'type':'agentMessage','id':'visible','text':'Saved reply'}]}]}}})
    elif method=='turn/start':
        active=True
        emit({'id':msg['id'],'result':{'turn':{'id':'native-turn','status':'inProgress'}}})
        emit({'method':'turn/started','params':{'threadId':thread,'turn':{'id':'native-turn','status':'inProgress'}}})
    elif method=='turn/steer':
        if mode=='reject': emit({'id':msg['id'],'error':{'code':-32600,'message':'Expected active turn does not match'}})
        elif mode!='ambiguous': emit({'id':msg['id'],'result':{'turnId':'native-turn'}})
        emit({'method':'item/agentMessage/delta','params':{'threadId':thread,'turnId':'native-turn','itemId':'answer','delta':'Exact visible '}})
        emit({'method':'item/completed','params':{'threadId':thread,'turnId':'native-turn','item':{'id':'answer','type':'agentMessage','text':'Exact visible reply'}}})
        emit({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':'native-turn','status':'completed'}}})
    elif method=='turn/interrupt':
        emit({'id':msg['id'],'result':{}})
        emit({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':'native-turn','status':'interrupted'}}})
'''


def native(tmp_path: Path, mode="accept"):
    script = tmp_path / "native_peer.py"
    script.write_text(NATIVE_PEER, encoding="utf-8")
    log = tmp_path / "protocol.jsonl"
    return CodexHarness(CodexConfig(stop_grace_seconds=0.2),
        command=[sys.executable, "-u", str(script), str(log), mode]), log


async def test_native_start_has_its_own_workspace_and_history_and_runs_no_turn(tmp_path):
    harness, log = native(tmp_path)
    workspace = tmp_path / "new-workspace"
    session = await harness.open(workspace=workspace, thread_id=None, tools=dynamic_tools(),
                                 instructions="Independent review")
    await harness.close()
    assert session.thread_id == "original"
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    # A new conversation is started, never copied from another one, and no turn runs yet.
    assert [item["method"] for item in calls] == ["initialize", "initialized", "thread/start"]
    start = calls[-1]["params"]
    assert start["cwd"] == str(workspace.resolve()) and start["runtimeWorkspaceRoots"] == [str(workspace.resolve())]
    assert start["dynamicTools"] == dynamic_tools() and "threadId" not in start and "history" not in start


@pytest.mark.parametrize(("mode", "status"), [("accept", "accepted"), ("reject", "deferred"), ("ambiguous", "uncertain")])
async def test_native_steer_is_one_delivery_and_distinguishes_rejection_from_unknown(tmp_path, mode, status):
    harness, log = native(tmp_path, mode)
    await harness.open(workspace=tmp_path, thread_id=None, tools=[], instructions="test")
    begun = asyncio.Event()
    visible = []
    harness.set_native_handler(lambda method, params: capture(visible, method, params))

    async def unused(*args):
        pass

    async def started(turn_id):
        begun.set()

    turn = asyncio.create_task(harness.run_turn("Original exact prompt", on_tool=unused,
        on_usage=unused, on_started=started))
    await asyncio.wait_for(begun.wait(), 2)
    delivery = await harness.steer("Exact human text\nwith newline", expected_turn_id="native-turn", client_input_id="input-1")
    outcome = await turn
    assert delivery.status == status
    assert outcome.visible_output == "Exact visible reply"
    assert outcome.process_quiet and outcome.terminal_observed
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    steer = [item for item in calls if item.get("method") == "turn/steer"]
    assert len(steer) == 1
    assert steer[0]["params"]["expectedTurnId"] == "native-turn"
    assert steer[0]["params"]["input"][0]["text"] == "Exact human text\nwith newline"
    assert len([item for item in calls if item.get("method") == "turn/start"]) == 1
    assert (await harness.steer("late", expected_turn_id="native-turn", client_input_id="input-2")).status == "deferred"
    assert any(method == "item/completed" for method, _ in visible)


async def capture(values, method, params):
    values.append((method, params))


async def test_native_stored_history_read_never_resumes_or_runs(tmp_path):
    harness, log = native(tmp_path)
    saved = await harness.read_history("historical", workspace=tmp_path)
    assert saved["thread"]["turns"][0]["items"][0]["text"] == "Saved reply"
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    assert [item["method"] for item in calls] == ["initialize", "initialized", "thread/read"]
    assert harness.process_quiet


async def command(hub, name, payload=None):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), Actor(role=Role.OPERATOR))


async def test_success_continues_original_thread_and_recruitment_helper_never_becomes_worker(tmp_path):
    initialize(tmp_path, ProjectConfig())
    hub = Hub(tmp_path)
    try:
        task = await command(hub, "task.publish", {"contract": {"title": "Synthetic task", "objective": "Exercise coordination", "acceptance_policy": "general.peer@1", "payload": {}}})
        run = await command(hub, "run.start")
        main = ScriptedHarness([ScriptedTurn([])])
        choice = ScriptedHarness([ScriptedTurn([], visible_output='{"decision":false}')])
        continuation = ScriptedHarness([ScriptedTurn([ScriptedAction("taskhub_call", {"operation": 'pause', "arguments": {"predicate": {"external_response_id": "unanswered-request"}, "reason": "Preserve progress until actionable work is available.", **({"checkpoint": {"summary": "Saved work", "next_action": "Await new information"}})}})])])
        runners = iter([main, choice, continuation])
        runner = WorkerRunner(hub, lambda: next(runners))
        first = await runner.run_once(run["id"])
        assert first["directive"] == "continued"
        worker_id = first["worker_id"]
        first_thread = (await hub.worker(worker_id))["thread_id"]
        assert await hub.next_assignment(run["id"]) is None
        intents = await hub.due_recruitment_intents(run["id"])
        assert len(intents) == 1
        result = await runner.run_helper(intents[0])
        assert result["directive"] == "recruitment_decided"
        # The decision runs in a new conversation of its own.
        assert choice.opened_threads == [None]
        assert choice.session.thread_id != first_thread
        assert len(await hub.list_workers()) == 1
        second = await runner.run_once(run["id"])
        assert second["worker_id"] == worker_id
        assert continuation.opened_threads == [first_thread]
        assert (await hub.worker(worker_id))["thread_id"] == first_thread
        assert (await hub.task(task["id"]))["state"] not in {"done", "retired"}
        paths = list((tmp_path / ".task-based" / "transcripts" / "inputs").glob("*.json"))
        inputs = [json.loads(path.read_text()) for path in paths]
        assert len(inputs) == 3 and all(item["status"] == "delivered" for item in inputs)
        assert {item["kind"] for item in inputs} == {"initial", "recruitment", "continuation"}
    finally:
        await hub.close()


async def test_directed_reply_turn_preserves_wait_and_never_spawns_a_worker(tmp_path):
    initialize(tmp_path, ProjectConfig())
    hub = Hub(tmp_path)
    try:
        await command(hub, "task.publish", {"contract": {"title": "Waiting task", "objective": "Preserve context while waiting", "payload": {}}})
        run = await command(hub, "run.start")
        waiting = ScriptedHarness([ScriptedTurn([ScriptedAction("taskhub_call", {"operation": 'pause', "arguments": {"predicate": {"external_response_id": "unanswered-request"}, "reason": "Preserve progress until actionable work is available.", **({"checkpoint": {"summary": "Saved", "next_action": "Wait"}})}})])])
        reply = ScriptedHarness([ScriptedTurn([])])
        factory = iter([waiting, reply])
        runner = WorkerRunner(hub, lambda: next(factory))
        first = await runner.run_once(run["id"])
        worker_id = first["worker_id"]
        original = await hub.worker(worker_id)
        await command(hub, "message.send", {"worker_id": worker_id, "message_id": "human-message", "text": "Please retain this exact text.\nOnly a reply is needed."})
        second = await runner.run_once(run["id"])
        assert second["directive"] == "waiting" and second["worker_id"] == worker_id
        assert reply.opened_threads == [original["thread_id"]]
        packet = json.loads(reply.prompts[0])
        assert packet["message_state"]["task_execution_authority"] is True
        assert "execution_mode" not in packet
        assert native_text(packet["messages"][0]) == "Please retain this exact text.\nOnly a reply is needed."
        assert (await hub.worker(worker_id))["work_intention"] == "waiting"
        assert len(await hub.list_workers()) == 1
        assert await runner.run_once(run["id"]) is None
    finally:
        await hub.close()


async def test_never_assigned_worker_gets_correspondence_only_prompt_without_root_authority(tmp_path):
    initialize(tmp_path, ProjectConfig())
    hub = Hub(tmp_path)
    try:
        anchor = await command(hub, "task.publish", {"contract": {"title": "Root audit anchor",
            "objective": "Do not execute this objective in a correspondence turn", "payload": {}}})
        run = await command(hub, "run.start")
        source = await command(hub, "worker.register")
        await command(hub, "message.send", {"worker_id": source["id"], "message_id": "source-note",
            "text": "Please answer this note while no task is assigned."})

        class Reply(ScriptedHarness):
            async def run_turn(self, prompt, *, on_started, on_tool, **kwargs):
                packet = json.loads(prompt)
                assert packet["message_state"]["task_execution_authority"] is False
                assert "execution_mode" not in packet
                assert packet["task"]["kind"] == "correspondence"
                assert packet["task"].get("id") is None and "audit_anchor" not in packet
                assert "Do not execute this objective" not in canonical_task(packet["task"])
                await on_started("reply-only")
                denied = await on_tool("taskhub_call", {"operation": 'complete', "arguments": {}}, "no-root-mutation")
                assert denied["code"] == "OPERATION_NOT_ALLOWED"
                await on_tool("taskhub_call", {"operation": 'message_send', "arguments": file_arguments(self.workspaces[-1], 'message_send', {"text": "The note has been answered.", "message_id": "reply"})}, "answer")
                await self.close()
                return TurnOutcome("reply-only", "completed", True, True)

        result = await WorkerRunner(hub, Reply).run_once(run["id"])
        assert result["directive"] == "message_handled"
        worker = await hub.worker(source["id"])
        assert worker["current_task_id"] is None and worker["work_intention"] == "idle"
        assert (await hub.task(anchor["id"]))["state"] == "ready"
        assert len(await hub.list_workers()) == 1
    finally:
        await hub.close()


def canonical_task(task):
    return json.dumps(task)


async def test_a_catalog_hold_preserves_thread_and_files_without_starting_turn(tmp_path):
    initialize(tmp_path, ProjectConfig())
    hub = Hub(tmp_path)
    opened = []

    class LegacyCatalog(ScriptedHarness):
        async def stored_tool_catalog(self, thread_id, *, workspace):
            assert thread_id == "legacy-thread"  # A conversation made with a retired tool set.
            return [{"name": "taskhub_wait", "description": "Retired", "inputSchema": {"type": "object"}}]

        async def open(self, **kwargs):
            opened.append(kwargs)
            return await super().open(**kwargs)

    try:
        task = await command(hub, "task.publish", {"contract": {"title": "Retained legacy task", "objective": "Keep the original context", "payload": {}}})
        run = await command(hub, "run.start")
        attempt = await hub.next_assignment(run["id"])
        worker_id = attempt["worker_id"]
        await hub.execute(Command(name="worker.bind_session", key="legacy-bind", payload={
            "worker_id": worker_id, "thread_id": "legacy-thread", "model": "fake", "config_hash": "old"}), Actor(role=Role.RUNNER))
        await record_saved_turn(hub, worker_id)
        preserved = hub.workspace(worker_id) / "saved.md"
        preserved.write_text("Keep this draft.", encoding="utf-8")
        with pytest.raises(HubError) as failure:
            await WorkerRunner(hub, LegacyCatalog).run_once(run["id"], assignment=attempt)
        assert failure.value.code == "NATIVE_CATALOG_MIGRATION_REQUIRED"
        assert not opened
        assert preserved.read_text() == "Keep this draft."
        assert (await hub.worker(worker_id))["thread_id"] == "legacy-thread"
        assert (await hub.task(task["id"]))["state"] != "retired"
        assert (await hub.run(run["id"]))["state"] == "running"
        assert (await hub.run(run["id"]))["turns_used"] == 0
    finally:
        await hub.close()


async def test_full_human_message_reference_needs_only_one_reply_turn(tmp_path):
    initialize(tmp_path, ProjectConfig())
    hub = Hub(tmp_path)
    harnesses = []

    def factory():
        actions = [] if harnesses else [ScriptedAction("taskhub_call", {"operation": 'pause', "arguments": {"predicate": {"external_response_id": "unanswered-request"}, "reason": "Preserve progress until actionable work is available.", **({
            "checkpoint": {"summary": "Saved waiting state", "next_action": "Wait for directed input"}})}})]
        harness = ScriptedHarness([ScriptedTurn(actions)])
        harnesses.append(harness)
        return harness

    try:
        await command(hub, "task.publish", {"contract": {"title": "Long message", "objective": "Preserve message slices", "payload": {}}})
        run = await command(hub, "run.start")
        runner = WorkerRunner(hub, factory)
        initial = await runner.run_once(run["id"])
        text = "Exact multilingual text λ漢字\n" * 400
        await command(hub, "message.send", {"worker_id": initial["worker_id"], "message_id": "long-directed", "text": text})
        pieces = []
        for _ in range(20):
            result = await runner.run_once(run["id"])
            if result is None:
                break
            assert result["directive"] == "waiting"
            packet = json.loads(harnesses[-1].prompts[0])
            for piece in packet["messages"]:
                assert "offset" not in piece and "next_offset" not in piece
                assert "text" not in piece
                pieces.append(native_text(piece))
        else:
            pytest.fail("The finite directed message never finished delivering")
        assert "".join(pieces) == text
        assert len(pieces) == 1 and len(await hub.list_workers()) == 1
        assert len({harness.session.thread_id for harness in harnesses}) == 1
        assert not await hub.delivery_holds()
        assert (await hub.worker(initial["worker_id"]))["work_intention"] == "waiting"
    finally:
        await hub.close()


@pytest.mark.parametrize("delivery_status", ["accepted", "deferred", "uncertain"])
async def test_runner_steering_receipts_hold_uncertain_text_and_keep_rejected_text_pending(tmp_path, delivery_status):
    initialize(tmp_path, ProjectConfig())
    hub = Hub(tmp_path)
    entered, delivered = asyncio.Event(), asyncio.Event()

    class SteeringHarness(ScriptedHarness):
        active_turn_id = None

        async def run_turn(self, prompt, *, on_started, **kwargs):
            self.active_turn_id = "test-steer-turn"
            await on_started(self.active_turn_id)
            entered.set()
            await asyncio.wait_for(delivered.wait(), 5)
            await asyncio.sleep(0.03)
            self.active_turn_id = None
            await self.close()
            return TurnOutcome("test-steer-turn", "completed", True, True)

        async def steer(self, text, *, expected_turn_id, client_input_id):
            holds = await hub.db.call(lambda c: [dict(row) for row in c.execute("SELECT * FROM message_delivery_holds")])
            assert any(item["input_id"] == client_input_id and item["state"] == "sending" for item in holds), "A crash before the RPC receipt must retain the delivery guard"
            self.steered = text
            delivered.set()
            return SteerOutcome(delivery_status, expected_turn_id,
                                "simulated unknown transport outcome" if delivery_status == "uncertain" else None)

    harness = SteeringHarness()
    try:
        await command(hub, "task.publish", {"contract": {"title": "Working task", "objective": "Receive directed inputs", "payload": {}}})
        run = await command(hub, "run.start")
        runner = WorkerRunner(hub, lambda: harness)
        execution = asyncio.create_task(runner.run_once(run["id"]))
        await asyncio.wait_for(entered.wait(), 3)
        worker_id = (await hub.list_workers())[0]["id"]
        await command(hub, "message.send", {"worker_id": worker_id, "message_id": "while-active", "text": "Exact message arriving during work"})
        result = await asyncio.wait_for(execution, 5)
        assert result["worker_id"] == worker_id
        assert native_text(json.loads(harness.steered)["messages"][0]) == "Exact message arriving during work"
        inbox = await hub.human_unread(Actor(role=Role.OPERATOR), worker_id=worker_id)
        assert bool(inbox["messages"]) is (delivery_status == "deferred")
        rows = await hub.delivery_holds()
        assert bool(rows) is (delivery_status == "uncertain")
        if rows:
            assert rows[0]["state"] == "uncertain" and rows[0]["turn_id"] == "test-steer-turn"
            assert (await hub.run(run["id"]))["state"] == "running"
            assert not (await hub.human_unread(Actor(role=Role.OPERATOR), worker_id=worker_id))["messages"]
    finally:
        await hub.close()
