"""Model-free runtime history and Worker N routing with the actual Hub."""
from __future__ import annotations

import asyncio
import json
from uuid import uuid4

import pytest

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Command, Role
from formal_geyser.coordination.runtime import (
    ScriptedAction,
    ScriptedHarness,
    ScriptedTurn,
    SteerOutcome,
    TurnOutcome,
    WorkerRunner,
)
from formal_geyser.coordination.runtime.model_view import NOTHING_NEW
from tests.coordination.communication_helpers import native_text
from tests.coordination.gateway_helpers import file_arguments
from tests.coordination.lifecycle_helpers import open_collaboration
from tests.coordination.publication_helpers import file_payload, new_hub


async def command(hub, name, payload=None, actor=None):
    return await hub.execute(Command(name=name, payload=payload or {}, key=uuid4().hex),
                             actor or Actor(role=Role.OPERATOR))


def actor(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                 fencing_token=attempt["fencing_token"])


async def call(runner, attempt, thread_id, operation, arguments, call_id):
    return await runner._tool(attempt, thread_id, "taskhub_call", {
        "operation": operation, "arguments": arguments}, call_id)


@pytest.fixture
async def project(tmp_path):
    hub = await new_hub(tmp_path)
    task = await command(hub, "task.publish", {"contract": {
        "title": "Inspect conversation history", "objective": "Check the exact recorded inputs and outputs.",
        "acceptance_policy": "general.json@1", "payload": {"output_schema": {"type": "object"}}}})
    run = await command(hub, "run.start")
    try:
        yield hub, task, run
    finally:
        await hub.close()


async def test_runtime_initial_continuation_and_helper_sessions_keep_exact_visible_history(project):
    hub, task, run = project
    initial = ScriptedHarness([ScriptedTurn([ScriptedAction("taskhub_call", {
        "operation": "sync", "arguments": {}})], visible_output="First visible reply\n∀ x.")])
    choice = ScriptedHarness([ScriptedTurn([], visible_output='{"decision":false}')])
    continuation = ScriptedHarness([ScriptedTurn([ScriptedAction("taskhub_call", {"operation": "pause", "arguments": {"predicate": {"external_response_id": "unanswered-request"}, 
        "reason": "Keep the finished inspection available.",
        "checkpoint": {"summary": "Read the complete context.", "next_action": "Wait for a concrete request."}}})],
        visible_output="Saved; waiting.")])
    harnesses = iter([initial, choice, continuation])
    runner = WorkerRunner(hub, lambda: next(harnesses))
    attempt = await hub.next_assignment(run["id"])
    worker_id = attempt["worker_id"]
    historical = runner.transcripts.prepare_input("other-historical-worker", "old-native", "taskhub",
        "Preserve historical quotation " + worker_id)
    runner.transcripts.delivered(historical, "old-turn")
    old_page = runner.transcripts.page("other-historical-worker")

    await runner.run_once(run["id"], assignment=attempt)
    intent = (await hub.due_recruitment_intents(run["id"]))[0]
    await runner.run_helper(intent)
    await runner.run_once(run["id"])
    first_packet = json.loads(initial.prompts[0])
    assert first_packet["identity"] == "Worker 0"
    assert not {"worker_id", "worker_identity", "native", "packet_id", "context_epoch"} & first_packet.keys()
    assert first_packet["paths"]["workspace"] == str(hub.workspace(worker_id))
    # A sync adds only what is new: the Worker N name and the task came with the first input.
    assert initial.results[0] == {
        "task": {"id": task["id"], "revision": 1, "title": task["contract"]["title"], "state": "claimed"},
        "instruction": NOTHING_NEW}
    # The recruitment decision runs in its own new conversation; its prompt names no worker.
    decision_prompt = json.loads(choice.prompts[0])
    assert decision_prompt["task"]["title"] == task["contract"]["title"]
    assert "identity" not in decision_prompt
    assert worker_id not in choice.prompts[0] and "Worker 0" not in choice.prompts[0]
    assert choice.opened_threads == [None] and choice.session.thread_id != initial.session.thread_id
    assert continuation.opened_threads == [initial.session.thread_id]
    assert runner.transcripts.page("other-historical-worker") == old_page

    sessions = runner.transcripts.sessions(worker_id)["sessions"]
    assert len(sessions) == 2
    main = next(session for session in sessions if session["kind"] == "main")
    helper = next(session for session in sessions if session["kind"] == "recruitment")
    assert main["turn_count"] == 2 and helper["turn_count"] == 1
    assert helper["thread_id"] == choice.session.thread_id
    turns = runner.transcripts.session_page(worker_id, main["key"])["turns"]
    assert [turn["inputs"][0]["text"] for turn in turns] == initial.prompts + continuation.prompts
    assert [turn["replies"][0]["text"] for turn in turns] == ["First visible reply\n∀ x.", "Saved; waiting."]
    assert [turn["status"] for turn in turns] == ["completed", "interrupted"]
    assert turns[0]["task"]["id"] == task["id"]
    assert turns[0]["actions"][0]["tool"] == "taskhub_call"
    assert turns[0]["actions"][0]["arguments"]["operation"] == "sync"
    assert turns[0]["actions"][0]["status"] == "completed"
    assert turns[1]["actions"][0]["arguments"]["arguments"]["reason"] == "Keep the finished inspection available."
    helper_turn = runner.transcripts.session_page(worker_id, helper["key"])["turns"][0]
    assert helper_turn["status"] == "completed"
    assert [item["text"] for item in helper_turn["inputs"]] == choice.prompts
    assert [item["text"] for item in helper_turn["replies"]] == ['{"decision":false}']
    assert helper_turn["task"]["title"] == task["contract"]["title"]
    # The history links the decision turn to the task it decided, as for the main turns.
    assert helper_turn["task"]["id"] == task["id"]


async def test_runtime_alias_tool_arguments_and_presented_receipts_preserve_raw_fences(project):
    hub, task, run = project
    first = await hub.next_assignment(run["id"])
    await open_collaboration(hub, task["id"])
    peer = await command(hub, "worker.register")
    second = await command(hub, "task.claim", {"task_id": task["id"], "revision": 1,
        "worker_id": "Worker 1", "run_id": run["id"], "plan": "Inspect the correspondence."})
    runner = WorkerRunner(hub, ScriptedHarness)
    text = "Exact authored text includes " + first["worker_id"]
    sent = await call(runner, first, "native-original", "talk_send", {
        "recipient_id": "Worker 1", "message_id": "one", "text": text}, "send")
    assert sent["sender_id"] == "Worker 0" and sent["recipient_id"] == "Worker 1"
    packet, inbox = await runner._native_packet(await hub.context(second["worker_id"]), second)
    presented_inbox = await runner._present(packet["private_talk"])
    assert presented_inbox["messages"][0]["sender_id"] == "Worker 0"
    assert native_text(presented_inbox["messages"][0]) == text
    assert (await hub.peer_unread(actor(second)))["messages"]
    await runner._ack_native(packet, second, inbox)
    assert not (await hub.peer_unread(actor(second)))["messages"]
    arguments = file_arguments(hub.workspace(first["worker_id"]), "stage", file_payload(
        hub, first, kind="json", data={"result": text}, introduction="An exact structured record.",
        coauthors=["Worker 1"]))
    candidate = await call(runner, first, "native-original", "stage", arguments, "stage")
    artifact = await hub.artifact(candidate["artifact_id"])
    assert artifact["candidate_binding"]["coauthors"] == [peer["id"]]
    presented = await runner._present({"worker_id": first["worker_id"], "receipt": artifact["candidate_binding"]})
    assert presented["worker_id"] == "Worker 0"
    assert presented["receipt"] == artifact["candidate_binding"]
    assert first["worker_id"] == (await hub.worker("Worker 0"))["id"]


async def test_a_message_without_an_id_of_its_own_is_one_message_per_call(project):
    hub, _, run = project
    first = await hub.next_assignment(run["id"])
    await command(hub, "worker.register")
    runner = WorkerRunner(hub, ScriptedHarness)
    arguments = {"recipient_id": "Worker 1", "text": "The same words, sent twice."}
    one = await call(runner, first, "thread", "talk_send", arguments, "send-1")
    replayed = await call(runner, first, "thread", "talk_send", arguments, "send-1")  # This call again.
    two = await call(runner, first, "thread", "talk_send", arguments, "send-2")
    assert one["id"] == replayed["id"] != two["id"]
    assert two["sequence"] == one["sequence"] + 1


async def test_active_steering_uses_names_and_retains_one_exact_turn(project):
    hub, task, run = project
    first = await hub.next_assignment(run["id"])
    await open_collaboration(hub, task["id"])
    await command(hub, "worker.register")
    second = await command(hub, "task.claim", {"task_id": task["id"], "revision": 1,
        "worker_id": "Worker 1", "run_id": run["id"], "plan": "Inspect the other section."})
    entered, received = asyncio.Event(), asyncio.Event()

    class SteeringHarness(ScriptedHarness):
        active_turn_id = None

        async def run_turn(self, prompt, *, on_started, **kwargs):
            self.prompts.append(prompt)
            self.active_turn_id = "active"
            await on_started("active")
            entered.set()
            await asyncio.wait_for(received.wait(), 5)
            self.active_turn_id = None
            await self.close()
            return TurnOutcome("active", "completed", True, True, visible_output="Received the peer's message.")

        async def steer(self, text, *, expected_turn_id, client_input_id):
            self.steer_prompt = text
            received.set()
            return SteerOutcome("accepted", expected_turn_id)

    harness = SteeringHarness()
    runner = WorkerRunner(hub, lambda: harness)
    execution = asyncio.create_task(runner.run_once(run["id"], assignment=first))
    await asyncio.wait_for(entered.wait(), 5)
    await command(hub, "talk.send", {"recipient_id": "Worker 0", "message_id": "active-msg",
                                   "text": "Read this exact peer observation."}, actor(second))
    await asyncio.wait_for(execution, 7)
    packet = json.loads(harness.steer_prompt)
    message = packet["messages"][0]
    assert message["sender"] == "Worker 1"
    assert not {"packet_id", "context_epoch", "private_talk"} & packet.keys()
    assert "recipient_display_name" not in message
    path = hub.workspace(first["worker_id"]) / message["file"]["path"]
    body = path.read_text(encoding="utf-8")
    text = json.loads(body)["text"] if path.suffix == ".json" else body.partition("\n```\n\n")[2].removesuffix("\n")
    assert text == "Read this exact peer observation."
    assert not (await hub.peer_unread(Actor(role=Role.OPERATOR), worker_id=first["worker_id"]))["messages"]
    sessions = runner.transcripts.sessions(first["worker_id"])["sessions"]
    page = runner.transcripts.session_page(first["worker_id"], sessions[0]["key"])
    assert len(page["turns"]) == 1
    assert [item["text"] for item in page["turns"][0]["inputs"]] == harness.prompts + [harness.steer_prompt]
    assert all(item["status"] == "delivered" for item in page["turns"][0]["inputs"])
    assert page["turns"][0]["status"] == "completed"


async def test_stale_tool_fence_is_rejected_before_unknown_worker_alias_lookup(project):
    hub, _, run = project
    attempt = await hub.next_assignment(run["id"])
    runner = WorkerRunner(hub, ScriptedHarness)
    stale = {**attempt, "fencing_token": attempt["fencing_token"] + 1}
    with pytest.raises(HubError) as failure:
        await call(runner, stale, "thread", "talk_send", {
            "recipient_id": "Worker 999999", "message_id": "forged", "text": "No target lookup"}, "forged")
    assert failure.value.code == "STALE_BINDING"


def test_tool_action_summary_bounds_large_arguments_without_changing_exact_prompt():
    arguments = {"text": "x" * 9000, "path": "file.md"}
    summary = WorkerRunner._action_arguments(arguments)
    assert summary["arguments_omitted"] and summary["arguments_bytes"] > 9000
    assert summary["argument_keys"] == ["path", "text"]
    assert len(summary["arguments_sha256"]) == 64
    assert arguments["text"] == "x" * 9000
