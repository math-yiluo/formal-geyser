"""The Task center's relations: where tasks came from, what they need, and the list's filters."""
from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from formal_geyser.coordination.api import create_app
from formal_geyser.coordination.models import Actor, Command, Role
from formal_geyser.coordination.task_graph import (
    Link,
    backfill_task_origins,
    deadlocks,
    waiting_on_operator,
)
from tests.coordination.publication_helpers import file_payload, new_hub

OPERATOR = Actor(role=Role.OPERATOR)


async def command(hub, name, payload=None, actor=OPERATOR):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


def contract(title, **fields):
    return {"contract": {"title": title, "objective": "Inspect the saved software workflow.", **fields}}


async def publish(hub, title, **fields):
    return await command(hub, "task.publish", contract(title, **fields))


async def claim(hub, task, run):
    worker = await command(hub, "worker.register")
    return await command(hub, "task.claim", {
        "task_id": task["id"], "revision": task["revision"], "worker_id": worker["id"], "run_id": run["id"]})


def worker(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"],
                 attempt_id=attempt["id"], fencing_token=attempt["fencing_token"])


@pytest.fixture
async def browser(tmp_path):
    clock = [10_000]
    hub = await new_hub(tmp_path, clock=lambda: clock[0])

    def no_models(_hub):
        raise AssertionError("Task views must never construct an execution runner")

    app = create_app(tmp_path, hub=hub, runner_factory=no_models)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                         base_url="http://testserver") as client:
                yield SimpleNamespace(hub=hub, client=client, clock=clock)
    finally:
        await hub.close()


async def read(browser, path, **params):
    response = await browser.client.get(path, params=params)
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.asyncio
async def test_a_task_hangs_under_the_task_whose_attempt_published_it(browser):
    hub = browser.hub
    root = await publish(hub, "Task 0 of the project")
    run = await command(hub, "run.start")
    author = await claim(hub, root, run)
    child = await command(hub, "task.publish", contract("Published from Task 0"), worker(author))
    candidate = await command(hub, "artifact.stage", file_payload(
        hub, author, data={"summary": "A synthetic candidate."}), worker(author))

    outline = await read(browser, "/v1/task-outline")
    by_id = {item["id"]: item for item in outline["items"]}
    assert set(by_id) == {root["id"], child["id"]}
    assert (by_id[child["id"]]["parent"], by_id[child["id"]]["relation"]) == (root["id"], "published")
    assert (by_id[root["id"]]["parent"], by_id[root["id"]]["relation"]) == (None, None)
    assert by_id[root["id"]]["reviews"]["open"] == 1

    relations = await read(browser, f"/v1/tasks/{child['id']}/relations")
    assert relations["parent"]["id"] == root["id"] and relations["relation"] == "published"
    assert relations["publisher"]["worker_id"] == author["worker_id"]
    assert relations["children"] == {"total": 0, "items": []}
    above = await read(browser, f"/v1/tasks/{root['id']}/relations")
    assert above["parent"] is None and above["publisher"] is None
    assert above["children"] == {"total": 1, "items": [
        {"id": child["id"], "task_number": 1, "title": "Published from Task 0", "state": "ready",
         "relation": "published"}]}
    assert above["reviews"]["counts"]["open"] == 1
    assert [item["id"] for item in above["reviews"]["items"]] == [candidate["review_task_id"]]
    review = await read(browser, f"/v1/tasks/{candidate['review_task_id']}/relations")
    assert review["host"]["id"] == root["id"]


@pytest.mark.asyncio
async def test_origins_recorded_before_publication_kept_them_come_from_the_publish_calls(browser):
    hub = browser.hub
    root = await publish(hub, "Task 0 of the project")
    run = await command(hub, "run.start")
    author = await claim(hub, root, run)
    child = await command(hub, "task.publish", contract("Published from Task 0"), worker(author))
    browser.clock[0] += 10
    later = await publish(hub, "Published by the operator")

    def forget_then_backfill(c):
        c.execute("DELETE FROM task_origins")
        c.execute("DELETE FROM ledger_backfills")
        # The call that published the child, and a later call that deduplicated to a task the
        # operator had already published: only the first names an origin.
        for call_id, task, after in (("publish", child, 0), ("deduplicated", later, 5)):
            created = c.execute("SELECT created_at FROM tasks WHERE id=?", (task["id"],)).fetchone()[0]
            c.execute("INSERT INTO native_call_receipts(worker_id,thread_id,call_id,attempt_id,fencing_token,"
                      "request_hash,command_json,response_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                      (author["worker_id"], "thread", call_id, author["id"], author["fencing_token"], "hash",
                       json.dumps({"name": "task.publish"}), json.dumps({"id": task["id"]}), created + after))
        added = backfill_task_origins(c, 1)
        origins = dict(c.execute("SELECT task_id,source_task_id FROM task_origins").fetchall())
        return added, origins, backfill_task_origins(c, 2)

    added, origins, again = await hub.db.call(forget_then_backfill, write=True)
    assert added == 3 and again == 0
    assert origins == {root["id"]: None, child["id"]: root["id"], later["id"]: None}

    def malformed_receipt_is_skipped(c):
        c.execute("DELETE FROM task_origins")
        c.execute("DELETE FROM ledger_backfills")
        c.execute("INSERT INTO native_call_receipts(worker_id,thread_id,call_id,attempt_id,fencing_token,"
                  "request_hash,command_json,response_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                  (author["worker_id"], "thread", "malformed", author["id"], author["fencing_token"], "hash",
                   "{not json", "{not json", 0))
        return backfill_task_origins(c, 3), dict(c.execute("SELECT task_id,source_task_id FROM task_origins").fetchall())

    added, origins = await hub.db.call(malformed_receipt_is_skipped, write=True)
    assert added == 3 and origins[child["id"]] == root["id"]


@pytest.mark.asyncio
async def test_the_dependency_view_shows_unmet_requirements_and_subscriptions(browser):
    hub = browser.hub
    provider = await publish(hub, "Provides a definition")
    needing = await publish(hub, "Requires the definition", requirements=[
        {"key": "definition", "task_id": provider["id"], "task_revision": 1}])
    waiting = await publish(hub, "Waits for the definition")
    unrelated = await publish(hub, "Needs nothing")
    run = await command(hub, "run.start")
    waiter = await claim(hub, waiting, run)
    await command(hub, "task.wait", {
        "checkpoint": {"summary": "Prepared the proof.", "next_action": "Use the definition."},
        "predicate": {"subscriptions": [{"task_id": provider["id"], "need": "The definition, published."}]},
    }, worker(waiter))

    graph = await read(browser, "/v1/task-graph")
    links = {(link["source"], link["target"], link["kind"]): link for link in graph["links"]}
    assert set(links) == {(provider["id"], needing["id"], "requirement"),
                          (provider["id"], waiting["id"], "subscription")}
    subscription = links[(provider["id"], waiting["id"], "subscription")]
    assert subscription["agent"] == waiter["worker_id"] and subscription["detail"] == "The definition, published."
    assert not any(link["met"] for link in graph["links"])
    assert {task["id"] for task in graph["tasks"]} == {provider["id"], needing["id"], waiting["id"]}
    assert unrelated["id"] not in {task["id"] for task in graph["tasks"]}
    assert graph["deadlocks"] == [] and graph["waiting_on_operator"] == []
    assert graph["agents"][waiter["worker_id"]].startswith("Worker")

    relations = await read(browser, f"/v1/tasks/{provider['id']}/relations")
    assert {link["target"] for link in relations["needed_by"]} == {needing["id"], waiting["id"]}
    assert relations["needs"] == [] and set(relations["tasks"]) == {needing["id"], waiting["id"]}
    needs = await read(browser, f"/v1/tasks/{needing['id']}/relations")
    assert [(link["source"], link["met"]) for link in needs["needs"]] == [(provider["id"], False)]


def test_unmet_cycles_are_deadlocks_and_the_operator_reaches_what_waits_on_it():
    links = [Link("a", "b", "requirement", False), Link("b", "a", "subscription", False, "w"),
             Link("c", "d", "requirement", True), Link("d", "c", "requirement", False),
             Link("operator", "e", "wait", False, "w2"), Link("e", "f", "requirement", False),
             Link("operator", "g", "wait", True, "w3")]
    assert deadlocks(links) == [["a", "b"]]
    assert waiting_on_operator(links) == ["e", "f"]


@pytest.mark.asyncio
async def test_task_pages_keep_work_or_reviews_sort_and_hold_still_while_tasks_arrive(browser):
    hub = browser.hub
    beta = await publish(hub, "Beta work")
    browser.clock[0] += 1
    alpha = await publish(hub, "Alpha work")
    run = await command(hub, "run.start")
    author = await claim(hub, alpha, run)
    candidate = await command(hub, "artifact.stage", file_payload(
        hub, author, data={"summary": "A synthetic candidate."}), worker(author))

    work = await read(browser, "/v1/task-browser", kind="work")
    assert {item["id"] for item in work["items"]} == {alpha["id"], beta["id"]}
    assert work["counts"]["all"] == 2 and work["counts"]["running"] == 1
    assert next(item for item in work["items"] if item["id"] == alpha["id"])["reviews"]["open"] == 1
    reviews = await read(browser, "/v1/task-browser", kind="review")
    assert [item["id"] for item in reviews["items"]] == [candidate["review_task_id"]] and reviews["counts"]["all"] == 1
    assert (await read(browser, "/v1/task-browser"))["counts"]["all"] == 3
    titled = await read(browser, "/v1/task-browser", kind="work", sort="title")
    assert [item["contract"]["title"] for item in titled["items"]] == ["Alpha work", "Beta work"]
    oldest = await read(browser, "/v1/task-browser", kind="work", sort="oldest")
    assert [item["id"] for item in oldest["items"]] == [beta["id"], alpha["id"]]

    anchor = work["now"]
    browser.clock[0] += 1
    gamma = await publish(hub, "Gamma work")
    held = await read(browser, "/v1/task-browser", kind="work", until=anchor)
    assert gamma["id"] not in {item["id"] for item in held["items"]} and held["newer"] == 1
    assert held["total"] == 2
    response = await browser.client.get("/v1/task-browser", params={"kind": "everything"})
    assert response.status_code == 422
