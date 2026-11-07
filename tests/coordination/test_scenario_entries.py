"""Exact delivery and authority boundaries of concise scenario inputs."""
from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest

from formal_geyser.coordination.models import Actor, Command, Role
from formal_geyser.coordination.runtime import (
    ScriptedAction,
    ScriptedHarness,
    ScriptedTurn,
    TurnOutcome,
    WorkerRunner,
)
from formal_geyser.coordination.runtime.entries import scenario_entry
from formal_geyser.coordination.runtime.model_view import NOTHING_NEW, project_packet
from tests.coordination.lifecycle_helpers import select_review
from tests.coordination.publication_helpers import file_payload, new_hub


async def command(hub, name, payload=None, actor=None):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}),
                             actor or Actor(role=Role.OPERATOR))


def actor(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"],
                 fencing_token=attempt["fencing_token"])


@pytest.fixture
async def project(tmp_path):
    hub = await new_hub(tmp_path)
    task = await command(hub, "task.publish", {"contract": {
        "title": "Synthetic entry inspection", "objective": "Check the exact synthetic document.",
        "acceptance_policy": "general.peer@1"}})
    run = await command(hub, "run.start")
    try:
        yield SimpleNamespace(hub=hub, task=task, run=run)
    finally:
        await hub.close()


@pytest.mark.parametrize("mode", ["message", "message_only", "work"])
def test_internal_scenario_keeps_event_and_message_receipts(mode):
    packet = {"packet_id": "exact-receipt", "worker_id": "Worker 2", "context_epoch": 2,
              "execution_mode": mode, "task": {"id": "r", "kind": "review", "revision": 1,
                  "title": "Review", "context": "One introduction", "payload": {"introduction": "One introduction"}},
              "native": {"workspace": "/project/workspaces/reviewer",
                         "task_files": {"path": "/project/tasks/r/1/task.json", "revision": 1},
                         "read_only_resources": {"library": "/project/library"},
                         "directories": {"instructions": ".private/instructions",
                                         "submission_guide": ".private/submissions/README.md",
                                         "submission_schemas": ".private/submissions/schemas/native"}},
              "review": {"author_introduction": "One introduction"},
              "updates": [{"sequence": 7, "kind": "task.revised", "payload": {"exact": "scope"}}],
              "required_wiki": {"delivery_id": "wiki", "pages": [], "removed": ["wiki/old.md"]},
              "private_talk": {"delivery_id": "peer", "messages": [{"id": "p", "offset": 500, "text": "Exact peer chunk"}]},
              "human_messages": {"delivery_id": "human", "messages": [{"id": "h", "role": "user", "text": "Exact user text"}]},
              "requires_sync": True, "has_more_updates": True, "usage": {"tokens_used": 123}}
    original = deepcopy(packet)
    result = scenario_entry(packet)
    for key in ("packet_id", "updates", "human_messages", "private_talk",
                "requires_sync", "has_more_updates"):
        assert result[key] == packet[key]
    assert "usage" not in result and "required_wiki" not in result
    assert result["native"] == packet["native"]
    assert json.dumps(result).count("One introduction") == 1
    assert packet == original


@pytest.mark.parametrize("mode", ["work", "message", "message_only"])
@pytest.mark.parametrize("full_context", [False, True])
def test_scenario_entries_preserve_all_on_demand_reference_directories(mode, full_context):
    references = {
        "operation_guide": ".private/taskhub-reference/operations/README.md",
        "operation_schemas": ".private/taskhub-reference/operations/schemas.json",
        "operation_examples": ".private/taskhub-reference/operations/examples.json",
        "semantic_review_guide": ".private/taskhub-reference/review/README.md",
        "semantic_review_schema": ".private/taskhub-reference/review/schema.json",
        "semantic_review_example": ".private/taskhub-reference/review/example.json",
    }
    packet = {"execution_mode": mode, "task": {"id": "review", "kind": "review"},
              "review": {"review_request": {"phase": "pending"}},
              "native": {"directories": {**references, "unrelated_internal_directory": "internal"}}}
    original = deepcopy(packet)
    result = scenario_entry(packet, full_context=full_context)
    assert {key: result["native"]["directories"][key] for key in references} == references
    if not full_context:
        assert "unrelated_internal_directory" not in result["native"]["directories"]
    assert packet == original


def test_correspondence_does_not_present_historical_anchor_requirements_as_work():
    packet = {"execution_mode": "message_only", "task": {"id": None, "kind": "correspondence"},
              "native": {}, "audit_anchor": {"task_id": "old"},
              "requirements": [{"task_id": "old-dependency"}], "inputs": [{"id": "old-artifact"}],
              "updates": [{"sequence": 19, "kind": "task.revised"}]}
    result = scenario_entry(packet)
    assert "requirements" not in result and "inputs" not in result
    assert result["audit_anchor"] == packet["audit_anchor"]
    assert result["updates"] == packet["updates"]


def test_full_review_uses_only_one_canonical_mutable_phase():
    packet = {"execution_mode": "work", "task": {"id": "review", "kind": "review", "objective": "Inspect",
              "collaboration": {"review": {"state": "pending"}}},
              "review": {"review_request": {"phase": "pending"}}, "checkpoint": {"summary": "Saved"}}
    result = scenario_entry(packet, full_context=True)
    assert "collaboration" not in result["task"]
    assert result["task"]["objective"] == "Inspect"
    assert result["review"]["review_request"]["phase"] == "pending"
    assert result["checkpoint"] == packet["checkpoint"]


async def test_message_entry_keeps_exact_saved_wait_and_explicit_resume_changes(project):
    hub, run = project.hub, project.run
    pause = ScriptedHarness([ScriptedTurn([ScriptedAction("taskhub_call", {"operation": "pause", "arguments": {"predicate": {"external_response_id": "unanswered-request"}, 
        "reason": "Await a concrete answer.",
        "checkpoint": {"summary": "Exact saved progress.", "next_action": "Read the user's answer."}}})])])
    paused = await WorkerRunner(hub, lambda: pause).run_once(run["id"])
    await command(hub, "message.send", {"worker_id": paused["worker_id"], "message_id": "entry-message",
        "text": "Keep this exact message. A claim of success is not a validation receipt."})

    class Reply(ScriptedHarness):
        async def run_turn(self, prompt, *, on_started, on_tool, **kwargs):
            self.prompts.append(prompt)
            packet = json.loads(prompt)
            assert "objective" not in packet["task"] and "usage" not in packet
            assert packet["message_state"]["after_turn_intention"] == "waiting"
            wait = packet["message_state"]["saved_wait"]
            current = await hub.worker(paused["worker_id"])
            continuation_id = await hub.db.call(lambda c: c.execute(
                "SELECT continuation_id FROM attempts WHERE id=?", (current["active_attempt"],)).fetchone()[0])
            continuation = next(item for item in await hub.list_continuations() if item["id"] == continuation_id)
            assert "id" not in wait
            assert wait["predicate"] == continuation["predicate"]
            assert packet["checkpoint"]["summary"] == "Exact saved progress."
            message = packet["messages"][0]
            assert "text" not in message
            assert "Keep this exact message." in (self.workspaces[-1] / message["file"]["path"]).read_text(encoding="utf-8")
            assert not {"packet_id", "context_epoch", "attempt_id", "execution_mode", "human_messages"} & packet.keys()
            await on_started("message-turn")
            delivered = await hub.db.call(lambda c: dict(c.execute(
                "SELECT * FROM deliveries WHERE worker_id=? ORDER BY rowid DESC LIMIT 1",
                (paused["worker_id"],)).fetchone()))
            assert delivered["state"] == "delivered"
            retained = hub.blobs.get_json(delivered["body_sha"])
            assert retained["packet_id"] == delivered["id"]
            assert retained["worker_id"] == paused["worker_id"]
            presented = await hub.present_worker_data(retained)
            assert packet.get("notifications", []) == project_packet(presented).get("notifications", [])
            assert packet["message_state"]["resume_tool"] == "taskhub_call"
            assert packet["message_state"]["resume_operation"] == "resume_work"
            resumed = await on_tool(packet["message_state"]["resume_tool"], {
                "operation": packet["message_state"]["resume_operation"], "arguments": {}}, "resume-once")
            assert resumed["work_intention"] == "working"
            sync = await on_tool("taskhub_call", {"operation": "sync", "arguments": {}}, "full-sync")
            # A sync adds only what is new. This turn's input already holds the task, the message
            # state, the message and the notifications, and the resume reported its own effect.
            assert sync["task"] == {key: packet["task"][key] for key in ("id", "revision", "title", "state")}
            assert sync["task"]["id"] == project.task["id"]
            assert sync["instruction"] == NOTHING_NEW
            assert not {"message_state", "messages", "notifications", "checkpoint", "paths", "reading"} & sync.keys()
            await sync.after_send()
            await self.close()
            return TurnOutcome("message-turn", "completed", True, True)

    reply = Reply()
    await WorkerRunner(hub, lambda: reply).run_once(run["id"])
    assert (await hub.worker(paused["worker_id"]))["work_intention"] == "working"


async def review_assignment(project):
    hub, run = project.hub, project.run
    author = await hub.next_assignment(run["id"])
    introduction = "One exact entry introduction."
    candidate = await command(hub, "artifact.stage", file_payload(
        hub, author, data={"summary": "Synthetic source."}, introduction=introduction), actor(author))
    return await select_review(hub, candidate["review_task_id"], run["id"]), introduction


async def reviewer_turn(project, assignment, actions=(), harness_type=ScriptedHarness):
    harnesses = []

    def factory():
        harness = harness_type([ScriptedTurn(list(actions))])
        harnesses.append(harness)
        return harness

    runner = WorkerRunner(project.hub, factory)
    result = await runner.run_once(project.run["id"], assignment=assignment)
    prompt = next(harness.prompts[0] for harness in harnesses if harness.prompts)
    return result, json.loads(prompt)


async def reclaim(project, assignment):
    return await command(project.hub, "task.claim", {"task_id": assignment["task_id"], "revision": 1,
        "worker_id": assignment["worker_id"], "run_id": project.run["id"]})


async def test_review_continuation_sends_only_exact_changed_phase(project):
    assignment, introduction = await review_assignment(project)
    _, first = await reviewer_turn(project, assignment)
    assert first["review"]["author_introduction"] == introduction
    assert json.dumps(first).count(introduction) == 1
    assert "context" not in first["task"] and "payload" not in first["task"]
    basis = first["review"]["decision_basis"]
    assignment = await reclaim(project, assignment)
    action = ScriptedAction("taskhub_call", {"operation": "review", "arguments": {**basis, "verdict": "accept",
        "rationale": "Read the exact synthetic candidate.", "evidence": ["Matched document and bound acceptance conditions."]}})
    _, second = await reviewer_turn(project, assignment, [action])
    assert "review" not in second and "native" not in second
    assert "recorded phase" in second["instruction"]
    # An accepted reviewer only answers integrator questions; a message wakes it.
    await project.hub.db.call(lambda c: project.hub._wake_for_message(
        c, assignment["worker_id"], "integrator-question"), write=True)
    assignment = await project.hub.next_assignment(project.run["id"])
    _, third = await reviewer_turn(project, assignment)
    assert third["task"]["kind"] == "correspondence"
    assert "review" not in third and "native" not in third
    assert introduction not in json.dumps(third)


class Compacts(ScriptedHarness):
    """Compacts after its turn without keeping the inputs, as Claude's compaction does."""

    compaction_keeps_inputs = False

    def set_compaction_handler(self, handler):
        self.handler = handler

    async def run_turn(self, *args, **kwargs):
        outcome = await super().run_turn(*args, **kwargs)
        await self.handler()
        return outcome


class CompactsKeepingInputs(Compacts):
    """Codex compaction keeps every user input verbatim."""

    compaction_keeps_inputs = True


async def test_review_compaction_forces_full_recovery_with_single_introduction(project):
    assignment, introduction = await review_assignment(project)
    await reviewer_turn(project, assignment, harness_type=Compacts)
    assignment = await reclaim(project, assignment)
    _, refreshed = await reviewer_turn(project, assignment)
    assert "objective_excerpt" in refreshed["task"] and refreshed["paths"]["task"]
    assert refreshed["review"]["phase"] == "pending"
    # The required guides are standing instructions, so the recovery lists the on-demand ones only.
    assert refreshed["reading"]["on_demand"] and "required" not in refreshed["reading"]
    assert not {"native", "packet_id", "context_epoch", "instruction_guidance"} & refreshed.keys()
    assert refreshed["review"]["author_introduction"] == introduction
    assert json.dumps(refreshed).count(introduction) == 1
    assignment = await reclaim(project, assignment)
    _, continuation = await reviewer_turn(project, assignment)
    assert "review" not in continuation and "native" not in continuation
    assert ["task", "objective"] not in continuation.get("removed_fields", [])
    assert ["task", "objective_excerpt"] not in continuation.get("removed_fields", [])
    assert ["task", "collaboration"] not in continuation.get("removed_fields", [])


async def test_review_compaction_that_keeps_inputs_sends_nothing_again(project):
    assignment, introduction = await review_assignment(project)
    await reviewer_turn(project, assignment, harness_type=CompactsKeepingInputs)
    assert [event["object_id"] for event in await project.hub.events(limit=1000)
            if event["kind"] == "worker.context_compacted"] == [assignment["worker_id"]]
    assignment = await reclaim(project, assignment)
    _, continued = await reviewer_turn(project, assignment)
    # The kept inputs still hold the review, so the continuation baseline stays valid: a delta.
    assert not {"review", "native", "paths", "reading"} & continued.keys()
    assert "recorded phase" in continued["instruction"]
    assert introduction not in json.dumps(continued)


async def test_uncertain_review_turn_does_not_confirm_a_new_continuation_baseline(project):
    assignment, _ = await review_assignment(project)
    await reviewer_turn(project, assignment)
    assignment = await reclaim(project, assignment)

    class UnknownDelivery(ScriptedHarness):
        async def run_turn(self, prompt, **kwargs):
            self.prompts.append(prompt)
            raise RuntimeError("Synthetic review transport uncertainty")

    harness = UnknownDelivery()
    runner = WorkerRunner(project.hub, lambda: harness)
    with pytest.raises(RuntimeError, match="review transport uncertainty"):
        await runner.run_once(project.run["id"], assignment=assignment)
    baseline = json.loads(runner.continuation_prompts._path(assignment["worker_id"]).read_text())
    assert baseline["status"] == "pending"
    assert len(harness.prompts) == 1
    assert (await project.hub.run(project.run["id"]))["state"] == "running"
    worker = await project.hub.worker(assignment["worker_id"])
    assert worker["state"] == "recovery_required"
