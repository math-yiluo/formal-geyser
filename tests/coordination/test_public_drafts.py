"""Public draft sharing is readable, recoverable, and never acceptance evidence."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import pytest

from formal_geyser.coordination.drafts import DraftFiles
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import Actor, Command, Role
from formal_geyser.coordination.runtime import (
    ScriptedAction,
    ScriptedHarness,
    ScriptedTurn,
    WorkerRunner,
)
from formal_geyser.coordination.runtime.entries import scenario_entry

from .test_gateway_runtime import call
from .test_gateway_runtime import project as project


def exact(result, task):
    """A draft result as its records hold it: the worker reads its task as Task N (task_names)."""
    assert result["task_id"] == "Task 0"
    return {**result, "task_id": task["id"]}


def draft_arguments(hub, task, attempt):
    source = hub.workspace(attempt["worker_id"]) / ".private/drafts/current"
    source.mkdir(parents=True, exist_ok=True)
    (source / "Higher.lean").write_text("def Higher : MissingBase := missingConstruction\n", encoding="utf-8")
    (source / "Higher.md").write_text(
        "# Intended definition\nUnreviewed draft. MissingBase and missingConstruction are not implemented.\n",
        encoding="utf-8")
    return {"task_id": task["id"], "source_dir": ".private/drafts/current", "expected_revision": 0,
            "summary": "Expose the proposed interface for prerequisite work."}


async def test_unimplemented_public_pair_is_readable_without_artifact_or_task_revision(project):
    hub, task, attempt, runner = project
    arguments = draft_arguments(hub, task, attempt)
    result = await call(runner, attempt, "draft_update", arguments)
    root = Path(result["root"])
    assert root.is_relative_to(hub.root / "drafts")
    assert "MissingBase" in (root / "Higher.lean").read_text(encoding="utf-8")
    assert result["status"] == "unreviewed" and result["revision"] == 1
    current = json.loads(Path(result["current_path"]).read_text(encoding="utf-8"))
    assert current == exact(result, task)
    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["author"] == "Worker 0" and manifest["task_revision"] == 1
    updated = await hub.task(task["id"])
    assert updated["drafts"] == exact(result, task)
    assert updated["revision"] == task["revision"] and updated["contract_hash"] == task["contract_hash"]
    assert updated["artifacts"] == [] and len(await hub.list_tasks()) == 1
    assert (await hub.worker(attempt["worker_id"]))["active_attempt"] == attempt["id"]
    snapshot = hub.blobs.snapshot(hub.workspace(attempt["worker_id"]))
    assert not any("drafts" in item["path"] for item in hub.blobs.get_json(snapshot)["files"])
    native = await hub.prepare_native_context(attempt["worker_id"])
    assert native["directories"]["drafts"] == str(hub.root / "drafts")
    assert native["task_drafts"] == exact(result, task)
    relative = (root / "Higher.lean").relative_to(hub.root / "drafts").as_posix()
    read = await call(runner, attempt, "resource_read", {"root": "drafts", "path": relative}, "read")
    assert "MissingBase" in read["text"]
    # A decision about this task has no tools: it can neither read nor change the draft.
    await hub.ordinary_turn_completed(attempt["worker_id"], attempt["id"])
    [intent] = await hub.due_recruitment_intents(attempt["run_id"])
    decision = ScriptedHarness([ScriptedTurn([
        ScriptedAction("taskhub_call", {"operation": "resource_read",
                                        "arguments": {"root": "drafts", "path": relative}}),
        ScriptedAction("taskhub_call", {"operation": "draft_update",
                                        "arguments": {**arguments, "expected_revision": 1}}),
    ], visible_output=json.dumps({"decision": False}))])
    decided = await WorkerRunner(hub, lambda: decision).run_helper(intent)
    assert decided["directive"] == "recruitment_decided"
    assert len(decision.results) == 2 and all(set(item) == {"error"} for item in decision.results)
    assert (await hub.task(task["id"]))["drafts"] == exact(result, task)
    for mode in ("message_only", "ordinary"):
        packet = {"execution_mode": mode, "task": {"id": task["id"]}, "native": native,
                  "review": {"candidate_id": "example"} if mode == "ordinary" else None}
        entry = scenario_entry(packet)
        assert entry["native"]["directories"]["drafts"] == str(hub.root / "drafts")


async def test_exact_retry_survives_changed_source_and_does_not_rewind_latest_revision(project):
    hub, task, attempt, runner = project
    arguments = draft_arguments(hub, task, attempt)
    first = await call(runner, attempt, "draft_update", arguments, "first")
    source = hub.workspace(attempt["worker_id"]) / arguments["source_dir"]
    (source / "Higher.lean").write_text("def Higher : OtherMissingBase := otherConstruction\n", encoding="utf-8")
    second = await call(runner, attempt, "draft_update", {**arguments, "expected_revision": 1}, "second")
    (source / "Higher.lean").unlink()
    assert await call(runner, attempt, "draft_update", arguments, "first") == first
    assert json.loads(Path(first["current_path"]).read_text(encoding="utf-8")) == exact(second, task)
    assert "MissingBase" in (Path(first["root"]) / "Higher.lean").read_text(encoding="utf-8")
    with pytest.raises(HubError) as error:
        await call(runner, attempt, "draft_update", arguments, "stale-update")
    assert error.value.code == "DRAFT_REVISION_CONFLICT"


async def test_simultaneous_revisions_do_not_overwrite_each_other(project):
    hub, task, attempt, runner = project
    arguments = draft_arguments(hub, task, attempt)
    replies = await asyncio.gather(*(
        call(runner, attempt, "draft_update", {**arguments, "summary": f"Writer {index}"}, str(index))
        for index in range(2)), return_exceptions=True)
    assert sum(isinstance(item, dict) for item in replies) == 1
    assert sum(isinstance(item, HubError) and item.code == "DRAFT_REVISION_CONFLICT" for item in replies) == 1
    assert (await hub.task(task["id"]))["drafts"]["revision"] == 1


async def test_receipt_recovers_navigation_failure_without_rereading_source(project, monkeypatch):
    hub, task, attempt, runner = project
    arguments = draft_arguments(hub, task, attempt)
    point = DraftFiles.point

    def fail(*args, **kwargs):
        raise OSError("Synthetic navigation write failure")

    monkeypatch.setattr(DraftFiles, "point", fail)
    with pytest.raises(OSError, match="Synthetic"):
        await call(runner, attempt, "draft_update", arguments)
    reference = (await hub.task(task["id"]))["drafts"]
    assert not Path(reference["current_path"]).exists()
    (hub.workspace(attempt["worker_id"]) / arguments["source_dir"] / "Higher.lean").unlink()
    monkeypatch.setattr(DraftFiles, "point", point)
    result = await call(runner, attempt, "draft_update", arguments)
    assert exact(result, task) == reference
    assert json.loads(Path(result["current_path"]).read_text(encoding="utf-8")) == reference


async def test_rolled_back_update_does_not_publish_a_current_head(project, monkeypatch):
    hub, task, attempt, runner = project
    arguments = draft_arguments(hub, task, attempt)
    event = hub._event

    def fail(c, kind, *args):
        if kind == "draft.updated":
            raise RuntimeError("Synthetic transaction failure")
        return event(c, kind, *args)

    monkeypatch.setattr(hub, "_event", fail)
    with pytest.raises(RuntimeError, match="Synthetic"):
        await call(runner, attempt, "draft_update", arguments)
    assert "drafts" not in await hub.task(task["id"])
    assert not (hub.root / "drafts" / task["id"] / "current.json").exists()
    monkeypatch.setattr(hub, "_event", event)
    result = await call(runner, attempt, "draft_update", arguments)
    assert result["revision"] == 1


async def test_restart_rebuilds_navigation_and_preserves_shared_source(project):
    hub, task, attempt, runner = project
    result = await call(runner, attempt, "draft_update", draft_arguments(hub, task, attempt))
    Path(result["current_path"]).unlink()
    await hub.close()
    reopened = Hub(hub.root)
    try:
        assert (await reopened.task(task["id"]))["drafts"] == exact(result, task)
        assert json.loads(Path(result["current_path"]).read_text(encoding="utf-8")) == exact(result, task)
    finally:
        await reopened.close()


@pytest.mark.parametrize("change", ["missing-md", "empty-md", "wrong-directory", "escape", "axiom"])
async def test_invalid_draft_input_has_no_public_head(project, change):
    hub, task, attempt, runner = project
    arguments = draft_arguments(hub, task, attempt)
    source = hub.workspace(attempt["worker_id"]) / arguments["source_dir"]
    if change == "missing-md":
        (source / "Higher.md").unlink()
    elif change == "empty-md":
        (source / "Higher.md").write_text("", encoding="utf-8")
    elif change == "axiom":
        (source / "Higher.lean").write_text("axiom MissingBase : Type\n", encoding="utf-8")
    else:
        arguments["source_dir"] = ".private/notes" if change == "wrong-directory" else ".private/drafts/../notes"
    with pytest.raises(HubError):
        await call(runner, attempt, "draft_update", arguments)
    assert "drafts" not in await hub.task(task["id"])


async def test_read_access_does_not_grant_an_unrelated_worker_update_authority(project):
    hub, task, attempt, runner = project
    arguments = draft_arguments(hub, task, attempt)
    result = await call(runner, attempt, "draft_update", arguments)
    other = await hub.execute(Command(name="task.publish", key=uuid4().hex, payload={"contract": {
        "title": "Independent task", "objective": "Read another task's draft."}}))
    worker = await hub.execute(Command(name="worker.register", key=uuid4().hex))
    assignment = await hub.execute(Command(name="task.claim", key=uuid4().hex, payload={
        "task_id": other["id"], "revision": 1, "worker_id": worker["id"], "run_id": attempt["run_id"]}))
    relative = (Path(result["root"]) / "Higher.md").relative_to(hub.root / "drafts").as_posix()
    read = await call(runner, assignment, "resource_read", {"root": "drafts", "path": relative}, "other-read")
    assert "Intended definition" in read["text"]
    with pytest.raises(HubError) as error:
        await call(runner, assignment, "draft_update", {**arguments, "expected_revision": 1}, "other-write")
    assert error.value.code == "PARTICIPANT_REQUIRED"


async def test_builtin_draft_root_cannot_be_shadowed(project):
    hub, _, _, _ = project
    hub.config.environment["resource_roots"] = {"drafts": str(hub.root / "other")}
    with pytest.raises(ValueError, match="reserved"):
        WorkerRunner(hub, ScriptedHarness)


@pytest.mark.parametrize("mode", ["invalid-arguments", "message-only"])
async def test_rejected_gateway_requests_remain_bounded_and_replayable(project, mode):
    hub, task, attempt, runner = project
    arguments = draft_arguments(hub, task, attempt)
    if mode == "invalid-arguments":
        arguments = {}
    else:
        attempt = {**attempt, "execution_mode": "message_only"}
    result = await call(runner, attempt, "draft_update", arguments)
    assert result["code"] == ("INVALID_ARGUMENTS" if mode == "invalid-arguments" else "OPERATION_NOT_ALLOWED")
    assert await call(runner, attempt, "draft_update", arguments) == result
    assert "drafts" not in await hub.task(task["id"])


async def test_real_reviewer_cannot_update_drafts_through_gateway_or_coordinator(tmp_path):
    from tests.coordination.lifecycle_helpers import select_review
    from tests.coordination.publication_helpers import file_payload, new_hub

    hub = await new_hub(tmp_path)
    try:
        async def command(name, payload=None, actor=None):
            return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)

        task = await command("task.publish", {"contract": {"title": "Review authority", "objective": "Inspect a candidate."}})
        run = await command("run.start")
        author = await hub.next_assignment(run["id"])
        actor = Actor(role=Role.WORKER, worker_id=author["worker_id"], attempt_id=author["id"],
                      fencing_token=author["fencing_token"])
        staged = await command("artifact.stage", file_payload(hub, author, data={"ready": True}), actor)
        reviewer = await select_review(hub, staged["review_task_id"], run["id"])
        runner = WorkerRunner(hub, ScriptedHarness)
        arguments = draft_arguments(hub, task, reviewer)
        denied = await call(runner, reviewer, "draft_update", arguments)
        assert denied["code"] == "OPERATION_NOT_ALLOWED"
        assert await call(runner, reviewer, "draft_update", arguments) == denied
        actor = Actor(role=Role.WORKER, worker_id=reviewer["worker_id"], attempt_id=reviewer["id"],
                      fencing_token=reviewer["fencing_token"])
        with pytest.raises(HubError) as error:
            await command("task.draft.update", arguments, actor)
        assert error.value.code == "ORDINARY_TASK_AUTHORITY_REQUIRED"
        assert (await hub.worker(reviewer["worker_id"]))["worker_number"] is None
    finally:
        await hub.close()
