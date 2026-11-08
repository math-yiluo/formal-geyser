"""Work tasks are named Task N, in publication order; their ids stay their identity."""
from __future__ import annotations

import copy
import sqlite3

import pytest

from formal_geyser.coordination.core_transfer import _task_numbers
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.runtime.model_view import project_packet
from formal_geyser.coordination.task_browser import task_page
from formal_geyser.coordination.task_names import present_task_references
from tests.coordination.publication_helpers import new_hub
from tests.coordination.test_collaborative_core import (
    claim,
    command,
    publish,
    stage,
    start,
    worker_actor,
)

CHECKPOINT = {"summary": "Saved progress", "next_action": "Continue", "input_refs": []}


@pytest.fixture
async def hub(tmp_path):
    result = await new_hub(tmp_path)
    try:
        yield result
    finally:
        await result.close()


async def numbers(hub):
    return await hub.db.call(lambda c: {row[0]: row[1] for row in c.execute("SELECT id,task_number FROM tasks")})


async def test_work_tasks_are_numbered_in_publication_order_and_reviews_are_not(hub):
    first, second = await publish(hub, "First task"), await publish(hub, "Second task")
    assert (first["task_number"], first["name"]) == (0, "Task 0")
    assert ((await hub.task(second["id"]))["name"]) == "Task 1"
    run = await start(hub)
    candidate = await stage(hub, await claim(hub, second, run))
    review = await hub.task(candidate["review_task_id"])
    assert review["task_number"] is None and review["name"] is None
    assert (await publish(hub, "Third task"))["task_number"] == 2  # The review took no number.
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        await hub.db.call(lambda c: c.execute("UPDATE tasks SET task_number=7 WHERE id=?", (first["id"],)),
                          write=True)


async def test_task_n_names_a_task_wherever_its_id_is_taken(hub):
    first, second = await publish(hub, "Provider"), await publish(hub, "Consumer")
    await command(hub, "task.set_priority", {"task_id": "Task 1", "priority": 0})
    placed = await hub.db.call(lambda c: c.execute("SELECT task_id,priority FROM task_admission").fetchall())
    assert [tuple(row) for row in placed] == [(second["id"], 0)]
    # A contract's requirement and a pause's subscription take the name too, and keep the id.
    third = await command(hub, "task.publish", {"contract": {
        "title": "Needs the provider", "objective": "Use the provider's result.",
        "requirements": [{"key": "provider", "task_id": "task 0", "task_revision": 1}]}})
    assert third["contract"]["requirements"][0]["task_id"] == first["id"]
    run = await start(hub)
    attempt = await claim(hub, second, run)
    await command(hub, "task.pause", {"reason": "Wait for the provider.", "checkpoint": CHECKPOINT, "predicate": {
        "subscriptions": [{"task_id": "Task 0", "need": "The provider's result."}]}}, worker_actor(attempt))
    [wait] = await hub.list_continuations()
    assert wait["predicate"]["subscriptions"][0]["task_id"] == first["id"]
    assert await hub.resolve_task_ref("1") == second["id"]
    assert await hub.resolve_task_ref(first["id"]) == first["id"]  # An id is taken as it is.
    with pytest.raises(HubError) as missing:
        await command(hub, "task.set_priority", {"task_id": "Task 99", "priority": 0})
    assert missing.value.code == "NOT_FOUND" and "Task 99" in missing.value.message


async def test_a_project_from_before_numbering_numbers_its_work_tasks_root_first(tmp_path):
    hub = await new_hub(tmp_path)
    database = hub.state_dir / "state.sqlite3"
    try:
        tasks = [await publish(hub, title) for title in ("Older", "The root", "Newer")]
        await hub.db.call(lambda c: c.execute("UPDATE project_lifecycle SET root_task_id=? WHERE id=1",
                                              (tasks[1]["id"],)), write=True)
    finally:
        await hub.close()
    # The database as an older version left it: no number, no sequence.
    connection = sqlite3.connect(database)
    connection.executescript("DROP TRIGGER immutable_task_number; DROP INDEX tasks_number_idx; "
                             "DROP TABLE task_name_sequence; ALTER TABLE tasks DROP COLUMN task_number;")
    # An export from it gives the numbers its first start will give.
    expected = {tasks[1]["id"]: 0, tasks[0]["id"]: 1, tasks[2]["id"]: 2}
    assert _task_numbers(connection) == expected
    connection.close()
    hub = Hub(tmp_path)
    try:
        assert await numbers(hub) == expected
        assert (await publish(hub, "Published after the upgrade"))["task_number"] == 3
    finally:
        await hub.close()


def test_a_worker_reads_task_n_in_references_and_beside_an_id(tmp_path):
    task, other = "3f1c2a9e-0000-4000-8000-000000000001", "3f1c2a9e-0000-4000-8000-000000000002"
    names = {task: "Task 4"}
    value = {"task_id": task, "source_task_id": other, "task_ids": [task, other],
             "task": {"id": task, "title": "Numbered"}, "notifications": [{"event": "x", "reference": task}],
             "paths": {"handoff": f".private/tasks/{task}/handoff.md"},
             "contract": {"requirements": [{"task_id": task}]}, "note": f"about {task}"}
    original = copy.deepcopy(value)
    presented = present_task_references(value, names)
    assert value == original  # The input is never changed.
    assert presented["task_id"] == "Task 4" and presented["source_task_id"] == other
    assert presented["task_ids"] == ["Task 4", other]
    assert presented["task"] == {"id": task, "title": "Numbered", "name": "Task 4"}  # The id stays exact.
    assert presented["notifications"][0]["reference"] == "Task 4"
    # Paths, authored content and free text keep the id.
    assert presented["paths"] == value["paths"] and presented["contract"] == value["contract"]
    assert presented["note"] == value["note"]
    # The task view keeps the name, and paths built from the exact id.
    view = project_packet({"task": {"id": task, "name": "Task 4", "revision": 1, "title": "Numbered",
                                    "kind": "work", "state": "ready"}})
    assert view["task"]["name"] == "Task 4" and task in view["paths"]["handoff"]
    shown = present_task_references(view, names)
    assert shown["task"]["id"] == task and shown["paths"]["handoff"] == view["paths"]["handoff"]


async def test_the_task_list_finds_task_n(hub):
    await publish(hub, "Alpha")
    second = await publish(hub, "Beta")
    for query in ("Task 1", "task1", " TASK 1 "):
        page = await task_page(hub, query=query)
        assert [item["id"] for item in page["items"]] == [second["id"]] and page["items"][0]["name"] == "Task 1"
    # A bare number is also text: Task 1, and any task whose text holds a 1.
    assert second["id"] in {item["id"] for item in (await task_page(hub, query="1"))["items"]}
    assert [item["name"] for item in (await task_page(hub, query="Alpha"))["items"]] == ["Task 0"]
