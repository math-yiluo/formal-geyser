"""Pauses since 2026-11-07: what ends them, and the graph of tasks waiting on tasks, which a new
subscription must keep free of cycles."""
from __future__ import annotations

from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import Actor, Command, Limits, ProjectConfig, Role
from formal_geyser.coordination.wait_graph import path_between, wait_edges

OP, RUNNER = Actor(role=Role.OPERATOR), Actor(role=Role.RUNNER)
CHECKPOINT = {"summary": "Saved progress", "next_action": "Continue from the saved progress", "input_refs": []}


@pytest.fixture
async def hub(tmp_path):
    initialize(tmp_path, ProjectConfig(limits=Limits()))
    result = Hub(tmp_path)
    yield result
    await result.close()


async def command(hub, name, payload=None, actor=OP):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


async def publish(hub, title, **contract):
    return await command(hub, "task.publish", {"contract": {
        "title": title, "objective": f"Produce the {title.lower()}.", **contract}})


async def working(hub, task, run):
    """A new worker that has claimed `task`, as its actor."""
    worker = await command(hub, "worker.register")
    attempt = await command(hub, "task.claim", {
        "task_id": task["id"], "revision": task["revision"], "worker_id": worker["id"], "run_id": run["id"]})
    return Actor(role=Role.WORKER, worker_id=worker["id"], attempt_id=attempt["id"],
                 fencing_token=attempt["fencing_token"])


def subscribed(*tasks):
    return {"subscriptions": [{"task_id": task["id"], "need": f"The {task['contract']['title'].lower()}."}
                              for task in tasks]}


async def pause(hub, actor, predicate, name="task.pause"):
    return await command(hub, name, {"reason": "Wait for what ends the pause.", "checkpoint": CHECKPOINT,
                                     "predicate": predicate}, actor)


async def refused(hub, actor, predicate, code, name="task.pause"):
    with pytest.raises(HubError) as error:
        await pause(hub, actor, predicate, name)
    assert error.value.code == code
    # A refused pause changes nothing: the worker goes on working.
    assert await hub.list_continuations() == []
    assert (await hub.worker(actor.worker_id))["work_intention"] != "waiting"
    return error.value.message


def label(task):
    return f"{task['id']} ({task['contract']['title']})"


async def test_a_pause_takes_no_requirements_and_needs_a_condition(hub):
    source, task, run = await publish(hub, "Source"), await publish(hub, "Record"), await command(hub, "run.start")
    actor = await working(hub, task, run)
    requirement = {"key": "source", "task_id": source["id"], "task_revision": 1}
    for name in ("task.pause", "task.wait"):
        message = await refused(hub, actor, {"requirements": [requirement]}, "PAUSE_REQUIREMENTS_REMOVED", name)
        assert '"subscriptions"' in message and '"need"' in message
        message = await refused(hub, actor, {}, "WAIT_CONDITION_REQUIRED", name)
        assert "leave it" in message
    with pytest.raises(HubError) as error:
        await command(hub, "task.pause", {"reason": "Nothing more to do here."}, actor)
    assert error.value.code == "WAIT_CONDITION_REQUIRED"
    with pytest.raises(HubError) as error:
        await pause(hub, actor, {"subscriptions": [{"task_id": source["id"]}]})
    assert error.value.code == "INVALID_INPUT"
    # An empty requirement list, as a question's wait_predicate once carried, is no requirement.
    request = await command(hub, "message.request", {
        "request_key": "scope", "title": "Scope", "text": "Which record is meant?"}, actor)
    assert request["wait_predicate"] == {"external_response_id": request["id"]}
    await pause(hub, actor, {"requirements": [], **request["wait_predicate"]})
    [wait] = await hub.list_continuations()
    assert wait["state"] == "waiting" and wait["predicate"]["external_response_id"] == request["id"]


async def test_a_subscription_names_another_open_task(hub):
    other, closed, task = (await publish(hub, "Other"), await publish(hub, "Closed"),
                           await publish(hub, "Record"))
    run = await command(hub, "run.start")
    actor = await working(hub, task, run)
    message = await refused(hub, actor, subscribed(task), "SUBSCRIPTION_CYCLE")
    assert "validations" in message
    await command(hub, "task.retire", {"task_id": closed["id"], "revision": 1})
    message = await refused(hub, actor, subscribed(other, closed), "SUBSCRIPTION_CLOSED")
    assert label(closed) in message
    with pytest.raises(HubError) as error:
        await pause(hub, actor, {"subscriptions": [{"task_id": "no-such-task", "need": "Anything."}]})
    assert error.value.code == "NOT_FOUND"
    await pause(hub, actor, subscribed(other))
    [wait] = await hub.list_continuations()
    # Only the waker ends a subscription: no machine check does.
    assert wait["state"] == "waiting"
    assert wait["predicate"]["subscriptions"] == subscribed(other)["subscriptions"]


async def test_a_subscription_that_closes_a_cycle_is_refused_with_its_path(hub):
    first, second, third = (await publish(hub, "First"), await publish(hub, "Second"),
                            await publish(hub, "Third"))
    run = await command(hub, "run.start")
    actors = [await working(hub, task, run) for task in (first, second, third)]
    await pause(hub, actors[0], subscribed(second))
    await pause(hub, actors[1], subscribed(third))
    message = await refused_one(hub, actors[2], subscribed(first))
    assert f"{label(third)} -> {label(first)} -> {label(second)} -> {label(third)}" in message
    # Two waits on the same task are no cycle.
    await pause(hub, actors[2], subscribed(await publish(hub, "Fourth")))
    await pause(hub, await working(hub, await publish(hub, "Fifth"), run), subscribed(third))


async def refused_one(hub, actor, predicate):
    """Like `refused`, for a pause among others' waits."""
    before = await hub.list_continuations()
    with pytest.raises(HubError) as error:
        await pause(hub, actor, predicate)
    assert error.value.code == "SUBSCRIPTION_CYCLE"
    assert await hub.list_continuations() == before
    return error.value.message


async def test_a_contract_requirement_is_an_edge_of_the_graph(hub):
    first = await publish(hub, "First")
    second = await publish(hub, "Second", requirements=[{"key": "first", "task_id": first["id"], "task_revision": 1}])
    run = await command(hub, "run.start")
    actor = await working(hub, first, run)
    message = await refused(hub, actor, subscribed(second), "SUBSCRIPTION_CYCLE")
    assert f"{label(first)} -> {label(second)} -> {label(first)}" in message
    # A retired task waits on nothing.
    await command(hub, "task.retire", {"task_id": second["id"], "revision": 1})
    assert second["id"] not in await hub.db.call(lambda c: wait_edges(c))


async def test_a_wait_being_replaced_is_left_out_of_the_graph(hub):
    first, second = await publish(hub, "First"), await publish(hub, "Second")
    run = await command(hub, "run.start")
    actor = await working(hub, first, run)
    await pause(hub, actor, subscribed(second))
    edges = await hub.db.call(lambda c: wait_edges(c))
    assert edges == {first["id"]: {second["id"]}}
    assert await hub.db.call(lambda c: wait_edges(c, replacing=(actor.worker_id, first["id"]))) == {}


def test_path_between_finds_a_shortest_path():
    edges = {"a": {"b", "c"}, "b": {"d"}, "c": {"d"}, "d": {"e"}}
    assert path_between(edges, "a", "e") == ["a", "b", "d", "e"]
    assert path_between(edges, "a", "a") == ["a"]
    assert path_between(edges, "e", "a") is None


async def test_the_waker_ends_a_subscribed_wait_and_the_worker_reconsiders(hub):
    source, task = await publish(hub, "Source"), await publish(hub, "Record")
    run = await command(hub, "run.start")
    actor = await working(hub, task, run)
    other = await publish(hub, "Other")
    parked = await pause(hub, actor, subscribed(source, other))
    end = hub._end_subscription_wait
    assert not await hub.db.call(lambda c: end(c, parked["continuation_id"], task["id"]), write=True)
    assert await hub.db.call(lambda c: end(c, parked["continuation_id"], source["id"]), write=True)
    # It has ended: a second judgment changes nothing.
    assert not await hub.db.call(lambda c: end(c, parked["continuation_id"], other["id"]), write=True)
    [wait] = await hub.list_continuations()
    assert (wait["state"], wait["wake_reason"]) == ("reconsider", "subscribed_task_changed")
    [notice] = [event for event in await hub.events() if event["kind"] == "attention.required"]
    assert notice["object_id"] == source["id"] and notice["payload"] == {"reason": "subscribed_task_changed"}
    await command(hub, "runtime.quiescent", {"worker_id": actor.worker_id,
                                             "snapshot": hub.blobs.snapshot(hub.workspace(actor.worker_id))}, RUNNER)
    resumed = await command(hub, "continuation.resume", {
        "continuation_id": parked["continuation_id"], "run_id": run["id"]}, RUNNER)
    assert resumed["resume_reason"] == "subscribed_task_changed"
    assert resumed["checkpoint"] == CHECKPOINT


async def test_a_wait_on_a_task_that_closes_ends(hub):
    source, task = await publish(hub, "Source"), await publish(hub, "Record")
    run = await command(hub, "run.start")
    actor = await working(hub, task, run)
    await pause(hub, actor, subscribed(source))
    await command(hub, "task.retire", {"task_id": source["id"], "revision": 1})
    [wait] = await hub.list_continuations()
    assert (wait["state"], wait["wake_reason"]) == ("reconsider", "subscribed_task_retired")
