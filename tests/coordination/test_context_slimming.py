"""Shared content is sent once, in standing instructions; afterwards only what is new."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import Actor, Command, ProjectConfig, Role
from formal_geyser.coordination.runtime import (
    ScriptedAction,
    ScriptedHarness,
    ScriptedTurn,
    WorkerRunner,
)
from formal_geyser.coordination.runtime.claude import ClaudeHarness
from formal_geyser.coordination.runtime.codex import CodexHarness
from formal_geyser.coordination.runtime.integrator import _contribution_summary
from formal_geyser.coordination.runtime.model_view import (
    _notification_summary,
    project_packet,
)
from tests.coordination.lifecycle_helpers import resolve_recruitment

OPERATOR, RUNNER = Actor(role=Role.OPERATOR), Actor(role=Role.RUNNER)
LONG_OBJECTIVE = "Prove the valuation bound for every prime. " * 400  # 17,200 characters.


async def command(hub, name, payload=None, actor=OPERATOR):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


class Recording(ScriptedHarness):
    def __init__(self, turns):
        super().__init__(turns)
        self.instructions = []

    async def open(self, **options):
        self.instructions.append(options["instructions"])
        return await super().open(**options)


@pytest.fixture
async def project(tmp_path):
    initialize(tmp_path, ProjectConfig())
    hub = Hub(tmp_path)
    await command(hub, "task.publish", {"contract": {"title": "Long task", "objective": LONG_OBJECTIVE}})
    await command(hub, "task.publish", {"contract": {"title": "Second task", "objective": "Work on it."}})
    run = await command(hub, "run.start")
    try:
        yield hub, run
    finally:
        await hub.close()


async def activate(hub, run, turn=None):
    await resolve_recruitment(hub, run["id"])
    assignment = await hub.next_assignment(run["id"])
    harness = Recording([turn or ScriptedTurn([])])
    await WorkerRunner(hub, lambda: harness).run_once(run["id"], assignment=assignment)
    return assignment, harness


async def test_standing_instructions_carry_the_guides_and_operations_and_are_shared(project):
    hub, run = project
    first, second = await hub.next_assignment(run["id"]), await hub.next_assignment(run["id"])
    assert first["worker_id"] != second["worker_id"]
    one, two = Recording([ScriptedTurn([])]), Recording([ScriptedTurn([])])
    await WorkerRunner(hub, lambda: one).run_once(run["id"], assignment=first)
    await WorkerRunner(hub, lambda: two).run_once(run["id"], assignment=second)
    [instructions] = one.instructions
    assert two.instructions == [instructions]  # The same for every worker of a role: a shared prefix.
    assert "# Worker guide" in instructions and "Operations for taskhub_call" in instructions
    assert "- sync(): Receive the notifications and directed messages not yet delivered" in instructions
    for personal in (first["worker_id"], str(hub.workspace(first["worker_id"])), "Long task"):
        assert personal not in instructions
    prompt = json.loads(one.prompts[0])
    assert "required" not in prompt["reading"] and prompt["reading"]["on_demand"]


async def test_the_task_text_stays_in_its_file_and_the_first_input_stays_small(project):
    hub, run = project
    _, harness = await activate(hub, run)
    prompt = json.loads(harness.prompts[0])
    assert prompt["task"]["objective_excerpt"].endswith("…") and len(prompt["task"]["objective_excerpt"]) == 301
    assert "objective" not in prompt["task"] and prompt["paths"]["task"]
    assert prompt["instruction"].startswith("Begin your assigned task: read paths.task for all of it")
    assert len(harness.prompts[0]) < 6000 < len(LONG_OBJECTIVE)


async def test_a_long_refresh_report_stays_in_its_file(project):
    hub, run = project
    await resolve_recruitment(hub, run["id"])
    assignment = await hub.next_assignment(run["id"])
    report = hub.workspace(assignment["worker_id"]) / ".private/repository-refresh.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    conflicts = [{"path": f"Library/File{number}.lean", "base_sha256": None, "local_sha256": "0" * 64,
                  "public_sha256": "1" * 64, "reason": "both_changed"} for number in range(500)]
    report.write_text(json.dumps({"conflicts": conflicts, "resolved_paths": []}), encoding="utf-8")
    harness = Recording([ScriptedTurn([])])
    await WorkerRunner(hub, lambda: harness).run_once(run["id"], assignment=assignment)
    refresh = json.loads(harness.prompts[0])["workspace_refresh"]
    assert refresh["conflicts"] == conflicts[:20] and refresh["conflicts_count"] == 500
    assert refresh["report_path"] == str(report) and "resolved_paths_count" not in refresh
    assert len(harness.prompts[0]) < 20_000 < len(json.dumps(conflicts))


async def test_sync_returns_only_what_is_new(project):
    hub, run = project
    sync = ScriptedAction("taskhub_call", {"operation": "sync", "arguments": {}})
    _, harness = await activate(hub, run, ScriptedTurn([sync]))
    result = harness.results[0]
    assert set(result) <= {"task", "notifications", "messages", "next_action", "workspace_refresh", "instruction"}
    assert set(result["task"]) == {"id", "name", "revision", "title", "state"}
    assert "objective" not in json.dumps(result) and "paths" not in result and "reading" not in result


async def test_compaction_keeps_the_delivery_cursor_and_the_epoch(project):
    hub, run = project
    first, _ = await activate(hub, run)
    await resolve_recruitment(hub, run["id"])
    continuing = await hub.next_assignment(run["id"])  # The worker's next turn holds an attempt.
    assert continuing["worker_id"] == first["worker_id"]
    worker = await hub.worker(first["worker_id"])
    before = await hub.context(first["worker_id"])
    await command(hub, "worker.compacted", {"worker_id": worker["id"]}, RUNNER)
    after = await hub.context(first["worker_id"])
    assert (await hub.worker(worker["id"]))["context_epoch"] == worker["context_epoch"]
    assert after["after_sequence"] == before["after_sequence"] and after["updates"] == before["updates"]
    compactions = await hub.db.call(lambda c: c.execute(
        "SELECT count(*) FROM events WHERE kind='worker.context_compacted' AND object_id=?",
        (worker["id"],)).fetchone()[0])
    assert compactions == 1


def test_a_task_state_seen_in_a_sync_is_the_baseline_so_a_reversal_arrives(tmp_path):
    from formal_geyser.coordination.runtime.continuation import ContinuationPromptStore

    store = ContinuationPromptStore(tmp_path)
    worker = {"id": "w1", "thread_id": "t1", "current_task_id": "task-1", "context_epoch": 1}

    def packet(state):
        return {"task": {"id": "task-1", "revision": 1, "title": "T", "kind": "proof", "state": state},
                "context_epoch": 1, "execution_mode": "work"}

    first = store.prepare(packet("open"), worker_id="w1", thread_id="t1")
    assert first.mode == "full" and store.confirm(first, current_worker=worker)
    store.observed("w1", {"id": "task-1", "revision": 1, "title": "T", "state": "waiting"})  # A sync showed it.
    later = store.prepare(packet("open"), worker_id="w1", thread_id="t1")
    assert later.mode == "delta"
    assert {"path": ["task", "state"], "value": "open"} in json.loads(later.text)["changes"]
    assert store.confirm(later, current_worker=worker)
    store.observed("w1", {"id": "other", "state": "done"})  # Another task's fields change nothing.
    again = store.prepare(packet("open"), worker_id="w1", thread_id="t1")
    assert again.mode == "delta" and "changes" not in json.loads(again.text)


def test_only_codex_compaction_keeps_the_inputs_that_carry_the_task():
    # Codex keeps every user input across compaction, so nothing is sent again; Claude Code
    # summarizes them, so its next tool result carries the current task view.
    assert CodexHarness.compaction_keeps_inputs is True and ScriptedHarness.compaction_keeps_inputs is True
    assert ClaudeHarness.compaction_keeps_inputs is False


async def test_an_oversized_result_lists_its_large_parts_with_outlines(project):
    hub, run = project
    first, _ = await activate(hub, run)
    runner = WorkerRunner(hub)
    stdout = "error: line\n" * 6000
    result = {"id": "c-1", "status": "staged", "data": {"name": "Small"},
              "evidence": [{"policy": "lean.proof@1", "details": {"compilation": {"stdout": stdout}}}],
              "report": {"compilation": {"stdout": stdout, "returncode": 1}, "doctor": {"ok": True}}}
    envelope = await runner._bound_tool_result(result, 32768, worker_id=first["worker_id"])
    assert envelope["requires_read"] and envelope["id"] == "c-1"
    parts = {part["field"]: part for part in envelope["parts"]}
    assert set(parts) == {"evidence", "report", "report.compilation"}
    assert parts["evidence"]["outline"] == "list of 1; each like: policy, details"
    assert parts["report.compilation"]["outline"] == "object: stdout, returncode"
    workspace = hub.workspace(first["worker_id"])
    stored = json.loads((workspace / parts["report.compilation"]["material_path"]).read_text(encoding="utf-8"))
    assert stored == result["report"]["compilation"]
    assert len(json.dumps(envelope)) < 4000


def test_notifications_are_one_line_summaries_with_their_files():
    summary, files = _notification_summary("review.submitted", {
        "verdict": "request_changes", "candidate_id": "c-1", "report_material_sha256": "ab" * 32,
        "report_material_path": ".private/material/ab.json", "rationale": "x" * 5000})
    assert summary == "review submitted: verdict request_changes; candidate id c-1"
    assert files == [".private/material/ab.json"]
    view = project_packet({"task": {"id": "t"}, "updates": [
        {"kind": "artifact.accepted", "object_id": "a-1", "payload": {"policy": "lean.proof@1", "sha256": "f" * 64}}]})
    assert view["notifications"] == [{"event": "artifact.accepted", "reference": "a-1",
                                      "summary": "artifact accepted: policy lean.proof@1"}]


def test_each_batch_contribution_has_a_one_line_summary():
    line = _contribution_summary({"kind": "lean.proof", "id": "c-9"},
                                 {"verdict": "approve", "rationale": "Checked against Serre, p. 12.\n" + "y" * 400})
    assert line.startswith("lean.proof c-9: review approve; Checked against Serre, p. 12. y") and line.endswith("…")
