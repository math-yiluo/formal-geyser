"""Compact ordinary inputs through the real Hub and model-free native harness."""
from __future__ import annotations

import json
from types import SimpleNamespace
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
from formal_geyser.coordination.runtime.files import ScopedFiles
from formal_geyser.coordination.runtime.instructions import guide_texts, standing_guides
from formal_geyser.plugin import LeanPlugin
from tests.bootstrap_helpers import synthetic_execution
from tests.coordination.lifecycle_helpers import resolve_recruitment


async def command(hub, name, payload=None, *, role=Role.OPERATOR):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}),
                             Actor(role=role))


@pytest.fixture
async def project(tmp_path):
    initialize(tmp_path, ProjectConfig(name="Synthetic continuation inputs", domain="lean"))
    hub = synthetic_execution(Hub(tmp_path))
    task = await command(hub, "task.publish", {"contract": {
        "title": "Inspect source interfaces", "domain": "lean", "acceptance_policy": "lean.peer@1",
        "objective": "Inspect the exact synthetic input; preserve the original acceptance conditions. " * 35}})
    run = await command(hub, "run.start")
    case = SimpleNamespace(root=tmp_path, hub=hub, task=task, run=run)
    try:
        yield case
    finally:
        await case.hub.close()


def scripted(*actions):
    return ScriptedHarness([ScriptedTurn(list(actions), visible_output="Continue the existing inspection.")])


async def tick(case, harness=None, *, assignment=None):
    harness = harness or scripted()
    runner = WorkerRunner(case.hub, lambda: harness)
    result = await runner.run_once(case.run["id"], assignment=assignment)
    assert result is not None
    await resolve_recruitment(case.hub, case.run["id"])
    return runner, harness, result


def inputs(runner, worker_id):
    return [item for item in runner.transcripts.page(worker_id, limit=500)["events"]
            if item["type"] == "input"]


def standing_instructions(runner, worker_id):
    """The developer instructions recorded for the worker's latest activation."""
    return [item["instructions"]["instructions"] for item in runner.transcripts.page(worker_id, limit=500)["events"]
            if item["type"] == "instructions"][-1]


def referenced_text(harness, reference):
    return (harness.workspaces[-1] / reference["path"]).read_text(encoding="utf-8")


async def latest_delivery(hub, worker_id):
    row = await hub.db.call(lambda c: c.execute(
        "SELECT body_sha FROM deliveries WHERE worker_id=? AND state='delivered' ORDER BY rowid DESC LIMIT 1",
        (worker_id,)).fetchone())
    return hub.blobs.get_json(row["body_sha"])


async def test_full_then_compact_survives_coordinator_restart_and_records_exact_inputs(project):
    _, first, result = await tick(project)
    first_packet = json.loads(first.prompts[0])
    assert first_packet["paths"]["knowledge_readme"].endswith("README.md")
    assert first_packet["paths"]["task"] and first_packet["task"]["objective_excerpt"]
    assert "wiki" not in first_packet
    assert not {"packet_id", "context_epoch", "attempt", "worker_id", "execution_mode"} & first_packet.keys()
    worker_id, original_thread = result["worker_id"], first.session.thread_id
    await project.hub.close()
    project.hub = synthetic_execution(Hub(project.root))
    runner, second, _ = await tick(project)
    next_packet = json.loads(second.prompts[0])
    assert second.opened_threads == [original_thread]
    assert not {"objective", "objective_excerpt"} & next_packet["task"].keys() and "native" not in next_packet
    assert not {"packet_id", "attempt", "usage", "checkpoint", "execution", "required_wiki"} & next_packet.keys()
    assert next_packet["task"]["id"] == project.task["id"]
    assert first_packet["task"]["objective_excerpt"][:300] not in second.prompts[0]
    assert not {"turn.finished", "task.claimed"} & {item["event"] for item in next_packet.get("notifications", [])}
    assert "first_turn_id" not in second.prompts[0] and "decision_helper_id" not in second.prompts[0]
    assert len(second.prompts[0].encode()) < len(first.prompts[0].encode()) // 2
    recorded = inputs(runner, worker_id)
    assert [item["context_mode"] for item in recorded] == ["full", "delta"]
    assert [item["text"] for item in recorded] == first.prompts + second.prompts
    session = runner.transcripts.sessions(worker_id)["sessions"][0]
    turns = runner.transcripts.session_page(worker_id, session["key"])["turns"]
    assert [turn["task"]["title"] for turn in turns] == [project.task["contract"]["title"]] * 2

    _, third, _ = await tick(project)
    quiet_packet = json.loads(third.prompts[0])
    assert "notifications" not in quiet_packet and "changes" not in quiet_packet
    assert quiet_packet["instruction"] == "Continue your current task. No new context changes or deliveries."


async def test_compact_input_omits_wiki_changes_but_keeps_message_delivery(project):
    _, _, result = await tick(project)
    guide = project.root / "wiki" / "literature.md"
    body = guide.read_text(encoding="utf-8") + "\nAn exact newly added source convention: alpha is fixed.\n"
    guide.write_text(body, encoding="utf-8", newline="")
    # Claim the ordinary continuation first; this is a message delivered with
    # ordinary work, not a separate waiting-worker message activation.
    assignment = await project.hub.next_assignment(project.run["id"])
    assert assignment["execution_mode"] == "work"
    text = "Preserve this exact user instruction.\nDo not replace it with a summary."
    await command(project.hub, "message.send", {"worker_id": result["worker_id"],
                                               "text": text, "message_id": "new-guidance"})
    runner, harness, _ = await tick(project, assignment=assignment)
    packet = json.loads(harness.prompts[0])
    assert inputs(runner, result["worker_id"])[-1]["context_mode"] == "delta"
    assert "wiki" not in packet and body not in harness.prompts[0]
    delivered = packet["messages"][0]
    assert text in referenced_text(harness, delivered["file"])
    assert "text" not in delivered and "complete" not in delivered
    exact_delivery = await latest_delivery(project.hub, result["worker_id"])
    history = await project.hub.human_history(result["worker_id"])
    assert history["messages"][0]["acknowledged_at"] is not None
    _, next_turn, _ = await tick(project)
    following = json.loads(next_turn.prompts[0])
    assert "wiki" not in following and "messages" not in following
    next_delivery = await latest_delivery(project.hub, result["worker_id"])
    assert not {update["sequence"] for update in exact_delivery.get("updates", [])} & {
        update["sequence"] for update in next_delivery.get("updates", [])}


async def test_a_sync_keeps_the_baseline_and_a_later_change_still_arrives(project, monkeypatch):
    prepare = project.hub.prepare_native_context
    value = "A"

    async def marked(worker_id):
        native = await prepare(worker_id)
        return {**native, "read_only_resources": {"synthetic_marker": value}}

    monkeypatch.setattr(project.hub, "prepare_native_context", marked)
    await tick(project)

    class SyncingHarness(ScriptedHarness):
        async def run_turn(self, prompt, *, on_tool, on_started, **kwargs):
            nonlocal value
            self.prompts.append(prompt)
            turn_id = uuid4().hex
            await on_started(turn_id)
            value = "B"
            refreshed = await on_tool("taskhub_call", {"operation": "sync", "arguments": {}}, "explicit-full-sync")
            # A sync returns only what is new: this conversation already has its task and resources.
            assert set(refreshed["task"]) == {"id", "revision", "title", "state"}
            assert "read_only_resources" not in refreshed and "paths" not in refreshed
            assert "wiki" not in refreshed
            assert "native" not in refreshed and "packet_id" not in refreshed
            await refreshed.after_send()
            value = "C"
            await self.close()
            return TurnOutcome(turn_id, "completed", True, True, visible_output="Read the refreshed state.")

    _, _, result = await tick(project, SyncingHarness())
    runner, third, _ = await tick(project)
    # The sync showed nothing of the resources, so the baseline still holds: a delta with the change.
    assert inputs(runner, result["worker_id"])[-1]["context_mode"] == "delta"
    changes = json.loads(third.prompts[0])["changes"]
    assert {"path": ["read_only_resources", "synthetic_marker"], "value": "C"} in changes


async def test_an_unconfirmed_sync_leaves_the_startup_baseline_valid(project):
    await tick(project)

    class LostSyncHarness(ScriptedHarness):
        async def run_turn(self, prompt, *, on_tool, on_started, **kwargs):
            self.prompts.append(prompt)
            turn_id = uuid4().hex
            await on_started(turn_id)
            await on_tool("taskhub_call", {"operation": "sync", "arguments": {}}, "unconfirmed-tool-delivery")
            # No after_send receipt: its deliveries stay unacknowledged and arrive again.
            await self.close()
            return TurnOutcome(turn_id, "completed", True, True)

    _, _, result = await tick(project, LostSyncHarness())
    runner, later, _ = await tick(project)
    # A sync carries no context state, so whether it arrived cannot invalidate the baseline.
    assert inputs(runner, result["worker_id"])[-1]["context_mode"] == "delta"
    assert "paths" not in json.loads(later.prompts[0])


async def test_compaction_without_a_framework_tool_forces_next_full_input(project):
    await tick(project)

    class CompactingHarness(ScriptedHarness):
        # Like Claude: compaction summarizes the inputs too. Codex keeps them, and its next input is a delta.
        compaction_keeps_inputs = False

        def set_compaction_handler(self, callback):
            self.compacted = callback

        async def run_turn(self, prompt, *, on_started, **kwargs):
            self.prompts.append(prompt)
            turn_id = uuid4().hex
            await on_started(turn_id)
            await self.compacted()
            await self.close()
            return TurnOutcome(turn_id, "completed", True, True)

    _, _, result = await tick(project, CompactingHarness())
    runner, later, _ = await tick(project)
    packet = json.loads(later.prompts[0])
    assert inputs(runner, result["worker_id"])[-1]["context_mode"] == "full"
    assert packet["paths"]["knowledge_readme"] and "wiki" not in packet
    assert packet["paths"]["task"] and packet["task"]["objective_excerpt"]
    # The required guides are standing instructions; reading lists only the on-demand guides.
    assert "required" not in packet["reading"]
    assert ".private/instructions/shared-dags.md" in packet["reading"]["on_demand"]
    standing = standing_instructions(runner, result["worker_id"])
    for path in (".private/instructions/worker.md", ".private/instructions/lean.md"):
        assert path not in packet["reading"]["on_demand"]
        assert (later.workspaces[-1] / path).read_text(encoding="utf-8").strip() in standing
    assert "context_epoch" not in packet and "instruction_guidance" not in packet


async def test_uncertain_start_does_not_advance_baseline_or_resend_the_input(project):
    await tick(project)

    class UnknownStartHarness(ScriptedHarness):
        async def run_turn(self, prompt, **kwargs):
            self.prompts.append(prompt)
            raise RuntimeError("Synthetic unknown input delivery")

    harness = UnknownStartHarness()
    runner = WorkerRunner(project.hub, lambda: harness)
    with pytest.raises(RuntimeError, match="unknown input delivery"):
        await runner.run_once(project.run["id"])
    assert len(harness.prompts) == 1
    worker = (await project.hub.list_workers())[0]
    baseline = json.loads(runner.continuation_prompts._path(worker["id"]).read_text())
    assert baseline["status"] == "pending"
    assert (await project.hub.run(project.run["id"]))["state"] == "running"
    assert runner.transcripts.page(worker["id"], limit=500)["events"][-1]["type"] in {
        "input_status", "turn"}


async def test_ack_failure_after_native_start_cannot_confirm_omission_state(project, monkeypatch):
    await tick(project)
    harness = scripted()
    runner = WorkerRunner(project.hub, lambda: harness)

    async def rejected_ack(*args, **kwargs):
        raise HubError("STALE_CONTEXT", "Synthetic epoch changed before acknowledgment")

    monkeypatch.setattr(runner, "_ack_native", rejected_ack)
    result = await runner.run_once(project.run["id"])
    assert result["turn_status"] == "completed"
    worker = (await project.hub.list_workers())[0]
    baseline = json.loads(runner.continuation_prompts._path(worker["id"]).read_text())
    assert baseline["status"] == "pending"


async def test_waiting_message_turn_is_targeted_and_does_not_silently_resume(project):
    await tick(project)
    pause = ScriptedAction("taskhub_call", {"operation": "pause", "arguments": {"predicate": {"external_response_id": "unanswered-request"}, 
        "reason": "Await human guidance.",
        "checkpoint": {"summary": "Paused with preserved work.", "next_action": "Read a concrete user message."}}})
    _, _, result = await tick(project, scripted(pause))
    await command(project.hub, "message.send", {"worker_id": result["worker_id"],
                  "message_id": "waiting-guidance", "text": "Please retain the current scope."})
    runner, resumed, _ = await tick(project)
    packet = json.loads(resumed.prompts[0])
    assert "execution_mode" not in packet and "instruction_guidance" not in packet
    assert "objective" not in packet["task"] and "paths" in packet
    assert packet["message_state"]["after_turn_intention"] == "waiting"
    assert packet["message_state"]["task_execution_authority"]
    assert "usage" not in packet and "execution" not in packet
    assert "wiki" not in packet and "native" not in packet
    assert inputs(runner, result["worker_id"])[-1]["context_mode"] == "full"
    assert (await project.hub.worker(result["worker_id"]))["work_intention"] == "waiting"


async def test_a_message_turn_keeps_the_work_baseline_and_the_workers_own_pause_is_no_news(project):
    await tick(project)
    pause = ScriptedAction("taskhub_call", {"operation": "pause", "arguments": {"predicate": {"external_response_id": "unanswered-request"}, 
        "reason": "Await human guidance.",
        "checkpoint": {"summary": "Paused with preserved work.", "next_action": "Read a concrete user message."}}})
    _, _, result = await tick(project, scripted(pause))
    worker_id = result["worker_id"]
    await command(project.hub, "message.send", {"worker_id": worker_id,
                  "message_id": "continue-guidance", "text": "Please continue within the current scope."})
    resume = ScriptedAction("taskhub_call", {"operation": "resume_work", "arguments": {}})
    _, message_turn, _ = await tick(project, scripted(resume))
    packet = json.loads(message_turn.prompts[0])
    assert packet["message_state"]["after_turn_intention"] == "waiting"
    assert not {"continuation.created", "worker.suspended"} & {
        item["event"] for item in packet.get("notifications", [])}
    # The message turn showed the current view; the next work input is a delta from the work baseline.
    runner, work_turn, _ = await tick(project)
    assert work_turn.opened_threads == message_turn.opened_threads
    assert inputs(runner, worker_id)[-1]["context_mode"] == "delta"


async def test_shared_bootstrap_carries_the_required_guides_and_survives_same_thread_resume(project):
    class InspectingHarness(ScriptedHarness):
        async def open(self, **kwargs):
            self.bootstrap = kwargs["instructions"]
            self.guide = ScopedFiles(kwargs["workspace"]).read(
                ".private/instructions/worker.md", limit=16384)
            # Instructions must be accessible even before native thread start.
            assert self.guide["text"] and not self.guide["has_more"]
            return await super().open(**kwargs)

    first = InspectingHarness([ScriptedTurn([])])
    runner, first, result = await tick(project, first)
    first_packet = json.loads(first.prompts[0])
    full = first_packet["reading"]
    # The required guides are standing instructions; reading lists only the on-demand guides.
    assert "required" not in full
    assert ".private/instructions/shared-dags.md" in full["on_demand"]
    assert all((first.workspaces[-1] / path).is_file() for path in full["on_demand"])
    for path in (".private/instructions/worker.md", ".private/instructions/lean.md"):
        assert path not in full["on_demand"]
        assert (first.workspaces[-1] / path).read_text(encoding="utf-8").strip() in first.bootstrap
    assert first.guide["text"].strip() in first.bootstrap
    # Apart from the guides and the operation cheat sheet, the shared rules stay short.
    standing = standing_guides(runner.plugin, review=False)
    sheet = runner.actions.cheat_sheet("worker")
    assert standing in first.bootstrap and sheet in first.bootstrap
    assert len(first.bootstrap.replace(standing, "").replace(sheet, "")) < 2500
    # Nothing personal, so every worker of the role shares this bootstrap as a cached prefix.
    for personal in (result["worker_id"], str(first.workspaces[-1]), first.workspaces[-1].as_posix(),
                     project.task["id"], project.task["contract"]["title"]):
        assert personal not in first.bootstrap
    assert "review.md" not in first.bootstrap
    assert guide_texts(runner.plugin, review=True)["review.md"].strip() not in first.bootstrap

    second = InspectingHarness([ScriptedTurn([])])
    _, second, _ = await tick(project, second)
    brief = json.loads(second.prompts[0])
    assert second.opened_threads == [first.session.thread_id]
    assert second.bootstrap == first.bootstrap
    # Unchanged guides need no reading entry at all.
    assert "reading" not in brief and "instruction_guidance" not in brief


async def test_an_updated_guide_continues_the_task_in_a_new_conversation_that_has_it(project, monkeypatch):
    runner, initial, result = await tick(project)
    assert "Synthetic updated source convention." not in standing_instructions(runner, result["worker_id"])
    old_worker = ScopedFiles(initial.workspaces[-1]).read(".private/instructions/worker.md", limit=16384)
    original = LeanPlugin.worker_guides

    def updated(self, **kwargs):
        guides = original(self, **kwargs)
        guides["lean.md"] += "\nSynthetic updated source convention.\n"
        return guides

    monkeypatch.setattr(LeanPlugin, "worker_guides", updated)
    runner, changed, _ = await tick(project)
    packet = json.loads(changed.prompts[0])
    # Codex keeps a conversation's instructions as they began: the task continues in a new one.
    assert changed.opened_threads == [None] and packet["rotation"]["reason"] == "instructions"
    assert ScopedFiles(changed.workspaces[-1]).read(".private/instructions/worker.md", limit=16384) == old_worker
    content = ScopedFiles(changed.workspaces[-1]).read(
        ".private/instructions/lean.md", limit=16384)
    assert content["text"].endswith("Synthetic updated source convention.\n")
    # A required guide is standing instruction text: the update arrives there, not as a reading request.
    standing = standing_instructions(runner, result["worker_id"])
    assert content["text"].strip() in standing and old_worker["text"].strip() in standing
    assert "lean.md" not in json.dumps(packet.get("reading", {}))  # Required: standing, never a reading.
    assert content["source_sha256"] not in changed.prompts[0]
    assert not any(item["path"][0] == "instruction_guidance" for item in packet.get("changes", []))
    runner, later, _ = await tick(project)
    assert later.opened_threads == [changed.session.thread_id]  # Unchanged since: no new rotation.
    assert "reading" not in json.loads(later.prompts[0])
    assert content["text"].strip() in standing_instructions(runner, result["worker_id"])
