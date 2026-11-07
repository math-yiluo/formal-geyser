"""Shared maintenance replaces publisher elections and revision-review teams."""
from __future__ import annotations

from uuid import uuid4

import pytest

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import Actor, Command, ProjectConfig, Role
from tests.coordination.lifecycle_helpers import open_collaboration

from .wait_helpers import legacy_wait

OP = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)


async def command(hub, name, payload=None, principal=OP):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), principal)


def actor(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"], fencing_token=attempt["fencing_token"])


async def publish(hub, title, principal=OP):
    return await command(hub, "task.publish", {"contract": {"title": title,
        "objective": "Coordinate the complete synthetic software document."}}, principal)


async def claim(hub, task, run):
    worker = await command(hub, "worker.register")
    return await command(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"],
        "worker_id": worker["id"], "run_id": run["id"], "plan": "Independently check the stated conditions."})


async def quiet(hub, attempt):
    return await command(hub, "runtime.quiescent", {"worker_id": attempt["worker_id"],
        "snapshot": hub.blobs.snapshot(hub.workspace(attempt["worker_id"]))}, RUNNER)


@pytest.fixture
async def hub(tmp_path):
    instance = Hub(tmp_path, config=ProjectConfig())
    yield instance
    await instance.close()


async def test_executor_revision_wakes_exact_original_publisher_continuation(hub):
    parent = await publish(hub, "Publisher's retained original task")
    run = await command(hub, "run.start")
    publisher = await hub.next_assignment(run["id"])
    child = await publish(hub, "Independent child", actor(publisher))
    checkpoint = {"summary": "Parent progress is preserved.", "next_action": "Reconsider the exact child result.", "input_refs": []}
    predicate = {"requirements": [{"key": "child", "task_id": child["id"], "task_revision": 1, "policy": "general.peer@1"}]}
    path = hub.workspace(publisher["worker_id"]) / "parent-progress.txt"
    path.write_text("Retain exact original work.", encoding="utf-8")
    wait = await legacy_wait(hub, {"checkpoint": checkpoint, "predicate": predicate}, actor(publisher))
    await quiet(hub, publisher)
    before = next(item for item in await hub.list_continuations() if item["id"] == wait["continuation_id"])
    executor = await claim(hub, child, run)
    revised = await command(hub, "task.revise", {"task_id": child["id"], "revision": 1,
        "contract": {**child["contract"], "objective": "Coordinate the full clarified software document."},
        "reason": "Clarify the execution requirement without claiming the original goal complete."}, actor(executor))
    assert revised["revision"] == 2
    assert not any(task["contract"]["kind"] == "revision_review" for task in await hub.list_tasks())
    after = next(item for item in await hub.list_continuations() if item["id"] == wait["continuation_id"])
    for key in ("task_id", "task_revision", "preferred_worker", "predicate", "checkpoint", "snapshot", "origin_attempt"):
        assert after[key] == before[key]
    assert after["state"] == "reconsider" and after["wake_reason"] == "dependency_revision_changed"
    resumed = await hub.next_assignment(run["id"])
    assert resumed["worker_id"] == publisher["worker_id"] and resumed["task_id"] == parent["id"]
    assert resumed["checkpoint"] == checkpoint
    assert path.read_text(encoding="utf-8") == "Retain exact original work."
    assert not await hub.db.call(lambda c: hub._satisfied(c, {"mode": "all", **predicate}, waiter=None))
    assert not any(member["worker_id"] == publisher["worker_id"] for member in revised["collaboration"]["members"])


async def test_original_publisher_maintains_child_without_becoming_an_executor(hub):
    parent = await publish(hub, "Publisher task")
    run = await command(hub, "run.start")
    publisher = await hub.next_assignment(run["id"])
    child = await publish(hub, "Child", actor(publisher))
    revised = await command(hub, "task.revise", {"task_id": child["id"], "revision": 1,
        "contract": {**child["contract"], "context": "Inspect source documentation before editing."},
        "reason": "Supply execution guidance."}, actor(publisher))
    assert revised["revision"] == 2 and revised["collaboration"]["members"] == []
    assert revised["collaboration"]["publishers"] == [publisher["worker_id"]]
    assert (await hub.worker(publisher["worker_id"]))["current_task_id"] == parent["id"]


async def test_current_executor_maintains_operator_published_task_without_delegation(hub):
    task = await publish(hub, "Operator's objective")
    run = await command(hub, "run.start")
    first = await hub.next_assignment(run["id"])
    await open_collaboration(hub, task["id"])
    second = await claim(hub, task, run)
    revised = await command(hub, "task.revise", {"task_id": task["id"], "revision": 1,
        "contract": {**task["contract"], "context": "Work independently on the agreed components."},
        "reason": "Clarify decomposition."}, actor(second))
    assert revised["revision"] == 2 and revised["collaboration"]["publishers"] == []
    assert (await hub.worker(first["worker_id"]))["state"] == "draining"
    assert (await hub.worker(second["worker_id"]))["state"] == "draining"
    history = await hub.db.call(lambda c: dict(c.execute("SELECT * FROM task_revision_history WHERE task_id=?", (task["id"],)).fetchone()))
    assert history["author_id"] == second["worker_id"] and history["previous_revision"] == 1 and history["material"] == 0


async def test_stale_edit_is_rejected_without_overwriting_other_executor(hub):
    task = await publish(hub, "CAS task")
    run = await command(hub, "run.start")
    first = await hub.next_assignment(run["id"])
    await command(hub, "task.revise", {"task_id": task["id"], "revision": 1,
        "contract": {**task["contract"], "context": "First exact edit."}, "reason": "Record first revision."}, actor(first))
    with pytest.raises(HubError) as error:
        await command(hub, "task.revise", {"task_id": task["id"], "revision": 1,
            "contract": {**task["contract"], "context": "Stale overwrite."}, "reason": "Attempt second edit."})
    assert error.value.code == "STALE_REVISION"
    assert (await hub.task(task["id"]))["contract"]["context"] == "First exact edit."
    assert (await hub.task(task["id"], 1))["contract"]["context"] == ""


async def test_narrowed_instructions_do_not_replace_original_acceptance_conditions(hub):
    task = await publish(hub, "General case")
    revised = await command(hub, "task.revise", {"task_id": task["id"], "revision": 1,
        "contract": {**task["contract"], "objective": "Investigate only a special case."},
        "reason": "Record a partial route while retaining the original goal."})
    conditions = await hub.db.call(lambda c: hub._conditions(c, hub._task(c, task["id"])))
    assert conditions["objective"] == task["contract"]["objective"]
    assert conditions["current-objective"] == revised["contract"]["objective"]
    assert (await hub.task(task["id"], 1))["contract"] == task["contract"]
