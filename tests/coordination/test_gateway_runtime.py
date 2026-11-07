"""Runtime gateway reads and state transitions through real fenced actors."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import Actor, Command, ProjectConfig, Role
from formal_geyser.coordination.runtime import ScriptedHarness, WorkerRunner
from tests.bootstrap_helpers import synthetic_execution


@pytest.fixture
async def project(tmp_path):
    initialize(tmp_path, ProjectConfig())
    hub = Hub(tmp_path)
    async def command(name, payload):
        return await hub.execute(Command(name=name, payload=payload, key=uuid4().hex),
                                 Actor(role=Role.OPERATOR))
    task = await command("task.publish", {"contract": {
        "title": "Inspect software metadata", "objective": "Preserve the exact objective.",
        "constraints": ["Retain the original evidence."]}})
    run = await command("run.start", {})
    attempt = await hub.next_assignment(run["id"])
    runner = WorkerRunner(hub, ScriptedHarness)
    try:
        yield hub, task, attempt, runner
    finally:
        await hub.close()


async def call(runner, attempt, operation, arguments, key="call"):
    return await runner._tool(attempt, "gateway-thread", "taskhub_call", {
        "operation": operation, "arguments": arguments}, key)


async def test_completion_inspection_is_one_authorized_current_view(project):
    hub, task, attempt, runner = project
    detail = await call(runner, attempt, "inspect", {"task_id": task["id"], "include_completion": True})
    assert detail["completion"]["revision"] == detail["revision"]
    assert detail["completion"]["acceptance_conditions"]["objective"] == task["contract"]["objective"]
    assert detail["completion"]["acceptance_conditions"]["constraint:0"] == "Retain the original evidence."
    assert detail["completion"]["declarations"] == []
    before = await hub.db.call(lambda c: c.execute("SELECT count(*) FROM receipts").fetchone()[0])
    assert await call(runner, attempt, "inspect", {"task_id": task["id"], "include_completion": True}) == detail
    assert await hub.db.call(lambda c: c.execute("SELECT count(*) FROM receipts").fetchone()[0]) == before
    # The current revision may be named; only another one is refused.
    assert await call(runner, attempt, "inspect", {"task_id": task["id"], "revision": 1, "include_completion": True}) == detail
    with pytest.raises(HubError, match="the task is at revision 1; omit revision, or pass the current one"):
        await call(runner, attempt, "inspect", {"task_id": task["id"], "revision": 2, "include_completion": True})


async def test_a_task_file_without_a_domain_is_in_the_projects(tmp_path):
    # TaskContract's own default domain is general, which failed 81 Lean publications in a day.
    initialize(tmp_path, ProjectConfig(domain="lean"))
    hub = synthetic_execution(Hub(tmp_path))
    try:
        operator = Actor(role=Role.OPERATOR)
        await hub.execute(Command(name="task.publish", key=uuid4().hex, payload={"contract": {
            "title": "Lean root", "domain": "lean", "acceptance_policy": "lean.peer@1",
            "objective": "Formalize the stated result."}}), operator)
        run = await hub.execute(Command(name="run.start", key=uuid4().hex, payload={}), operator)
        attempt = await hub.next_assignment(run["id"])
        runner = WorkerRunner(hub, ScriptedHarness)
        workspace = hub.workspace(attempt["worker_id"])
        contract = {"title": "A prerequisite", "acceptance_policy": "lean.peer@1",
                    "objective": "Formalize the prerequisite."}
        (workspace / "task.json").write_text(json.dumps(contract), encoding="utf-8")
        published = await call(runner, attempt, "publish", {"contract_path": "task.json"}, "publish")
        assert (await hub.task(published["id"]))["contract"]["domain"] == "lean"
        # A revision's task file may omit it too; a wrong explicit domain still fails and says what to set.
        exact = {**contract, "objective": "Formalize the prerequisite exactly."}
        (workspace / "task.json").write_text(json.dumps(exact), encoding="utf-8")
        revised = await call(runner, attempt, "propose_revision", {
            "task_id": published["id"], "revision": 1, "reason": "State the prerequisite exactly.",
            "contract_path": "task.json"}, "revise")
        assert revised["revision"] == 2 and revised["contract"]["domain"] == "lean"
        (workspace / "task.json").write_text(json.dumps({**exact, "domain": "general"}), encoding="utf-8")
        for operation, arguments in (("publish", {}), ("propose_revision", {
                "task_id": published["id"], "revision": 2, "reason": "Change the domain."})):
            with pytest.raises(HubError) as mismatch:
                await call(runner, attempt, operation, {**arguments, "contract_path": "task.json"}, "wrong-" + operation)
            assert mismatch.value.code == "DOMAIN_MISMATCH"
            assert "set the contract's \"domain\" to 'lean', not 'general'" in mismatch.value.message
    finally:
        await hub.close()


async def test_completion_inspection_says_who_may_request_it(project):
    hub, _, attempt, runner = project
    other = await hub.execute(Command(name="task.publish", key=uuid4().hex, payload={"contract": {
        "title": "Another task", "objective": "Produce another result."}}), Actor(role=Role.OPERATOR))
    with pytest.raises(HubError) as refused:
        await call(runner, attempt, "inspect", {"task_id": other["id"], "include_completion": True})
    assert refused.value.code == "PARTICIPANT_REQUIRED"
    assert refused.value.message == ("include_completion is only for the task's original publishers and "
                                     "current executors; omit it to read the task")
    assert (await call(runner, attempt, "inspect", {"task_id": other["id"]}))["id"] == other["id"]
    description = runner.actions.describe(operation="inspect")["description"]
    assert "only the task's original publishers and current executors may" in description


async def test_a_missing_id_error_names_the_id_meant(project, monkeypatch):
    hub, task, attempt, runner = project
    # Workers wrote 8-character prefixes, or invented the rest of an ID.
    for written in (task["id"][:8], task["id"][:8] + "-0000-0000-0000-000000000000"):
        for completion in (False, True):
            with pytest.raises(HubError) as missing:
                await call(runner, attempt, "inspect", {"task_id": written, "include_completion": completion})
            assert missing.value.code == "NOT_FOUND"
            assert missing.value.message == (
                f"No task has ID {written!r}; IDs are full 36-character UUIDs; "
                f"the task whose ID starts with {task['id'][:8]} is {task['id']}")
    with pytest.raises(HubError) as missing:
        await call(runner, attempt, "fetch", {"artifact_id": task["id"]})
    assert missing.value.message == f"{task['id']} is a task ID, not an artifact ID: read it with inspect"
    unknown = str(uuid4())
    with pytest.raises(HubError) as missing:
        await call(runner, attempt, "fetch", {"artifact_id": unknown})
    assert missing.value.message == f"No artifact has ID {unknown!r}; IDs are full 36-character UUIDs"
    # An existing task keeps the error of what is missing.
    with pytest.raises(HubError) as missing:
        await call(runner, attempt, "inspect", {"task_id": task["id"], "revision": 5})
    assert missing.value.message == "Task revision does not exist"
    # Each lookup is a range of the IDs' index on a read connection.
    assert await hub.ids_with_prefix(task["id"][:8].upper()) == {"tasks": [task["id"]], "artifacts": []}
    assert await hub.ids_with_prefix("not-hex!") == {"tasks": [], "artifacts": []}
    artifact, twins = str(uuid4()), [task["id"][:8] + "-1111-4111-8111-111111111111", task["id"]]

    async def matches(value):
        return {"tasks": twins if value.startswith(task["id"][:8]) else [],
                "artifacts": [artifact] if value == artifact else []}
    monkeypatch.setattr(hub, "ids_with_prefix", matches)
    with pytest.raises(HubError) as missing:
        await call(runner, attempt, "inspect", {"task_id": artifact})
    assert missing.value.message == f"{artifact} is an artifact ID, not a task ID: read it with fetch"
    with pytest.raises(HubError) as missing:
        await call(runner, attempt, "inspect", {"task_id": task["id"][:8]})
    assert missing.value.message == (f"No task has ID {task['id'][:8]!r}; IDs are full 36-character UUIDs; "
                                     f"several tasks have IDs starting with {task['id'][:8]}: copy the full ID")


@pytest.mark.parametrize("predicate", [{"external_response_id": "unanswered-request"},
                                       {"mode": "any", "validations": ["candidate-under-validation"]}])
async def test_pause_preserves_exact_wait_and_duplicate_receipt_after_drain(project, predicate):
    hub, task, attempt, runner = project
    checkpoint = {"summary": "Saved the inspected metadata.", "next_action": "Wait for actionable input."}
    arguments = {"reason": "A prerequisite is unavailable.", "checkpoint": checkpoint, "predicate": predicate}
    result = await call(runner, attempt, "pause", arguments, "pause-once")
    assert result["runtime_directive"] == "drain_for_wait"
    assert await call(runner, attempt, "pause", arguments, "pause-once") == result
    continuations = await hub.list_continuations()
    assert len(continuations) == 1 and continuations[0]["state"] == "waiting"
    assert (await hub.worker(attempt["worker_id"]))["work_intention"] == "waiting"
    with pytest.raises(HubError) as stale:
        await call(runner, {**attempt, "fencing_token": attempt["fencing_token"] + 1},
                   "pause", arguments, "pause-once")
    assert stale.value.code == "IDEMPOTENCY_CONFLICT"


async def test_discovery_has_no_side_effect_or_caller_selected_authority(project):
    hub, _, attempt, runner = project
    before = await hub.list_tasks()
    described = await runner._tool(attempt, "gateway-thread", "taskhub_actions", {}, "list")
    names = {item["operation"] for item in described["operations"]}
    assert "wiki_submit" in names and "note_submit" not in names and "review" not in names
    assert all("inputSchema" not in item for item in described["operations"])
    assert (await runner._tool(attempt, "gateway-thread", "taskhub_actions", {
        "capability": "reviewer"}, "forge-role"))["code"] == "INVALID_ARGUMENTS"
    assert (await call(runner, attempt, "review", {}, "forge-operation"))["code"] == "OPERATION_NOT_ALLOWED"
    assert (await call(runner, attempt, "batch_publish", {}, "forge-integration"))["code"] in {
        "OPERATION_NOT_ALLOWED", "UNKNOWN_OPERATION"}
    assert await hub.list_tasks() == before


async def test_a_tool_call_reads_its_task_kind_on_a_read_connection(project, monkeypatch):
    hub, task, attempt, runner = project
    assert await hub.task_kind(task["id"]) == (await hub.task(task["id"]))["contract"]["kind"]
    with pytest.raises(HubError) as missing:
        await hub.task_kind("no-such-task")
    assert missing.value.code == "NOT_FOUND"

    async def unread(*args, **kwargs):
        raise AssertionError("A tool call's capability does not need the task in full")
    monkeypatch.setattr(hub, "task", unread)
    described = await runner._tool(attempt, "gateway-thread", "taskhub_actions", {}, "list")
    assert "wiki_submit" in {item["operation"] for item in described["operations"]}
