"""Task browsing observes current execution and immutable history without activation."""
from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from formal_geyser.coordination.api import create_app
from formal_geyser.coordination.models import Actor, Command, Role
from tests.coordination.lifecycle_helpers import (
    open_collaboration,
    select_review,
    validate_candidate,
)
from tests.coordination.publication_helpers import file_payload, new_hub

from .wait_helpers import legacy_wait

OPERATOR = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)


async def command(hub, name, payload=None, actor=OPERATOR):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


async def publish(hub, title, **fields):
    return await command(hub, "task.publish", {"contract": {
        "title": title, "objective": "Inspect the saved software workflow.", **fields}})


async def claim(hub, task, run):
    worker = await command(hub, "worker.register")
    return await command(hub, "task.claim", {
        "task_id": task["id"], "revision": task["revision"],
        "worker_id": worker["id"], "run_id": run["id"],
    })


def worker(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"],
                 attempt_id=attempt["id"], fencing_token=attempt["fencing_token"])


async def quiet(hub, attempt):
    return await command(hub, "runtime.quiescent", {
        "worker_id": attempt["worker_id"],
        "snapshot": hub.blobs.snapshot(hub.workspace(attempt["worker_id"])),
    }, RUNNER)


async def pause(hub, attempt):
    await legacy_wait(hub, {
        "reason": "Keep this original execution available for later work.",
        "checkpoint": {"summary": "Retained work.", "next_action": "Reconsider the saved task."},
    }, worker(attempt))
    await quiet(hub, attempt)


@pytest.fixture
async def browser(tmp_path):
    clock = [10_000]
    hub = await new_hub(tmp_path, clock=lambda: clock[0])

    def no_models(_hub):
        raise AssertionError("Task browsing must never construct an execution runner")

    app = create_app(tmp_path, hub=hub, runner_factory=no_models)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://testserver",
            ) as client:
                yield SimpleNamespace(hub=hub, app=app, client=client, clock=clock)
    finally:
        await hub.close()


async def read(browser, path, **params):
    response = await browser.client.get(path, params=params)
    assert response.status_code == 200, response.text
    return response.json()


def assert_page(page, *, total, offset, limit, size, has_more):
    assert page["total"] == total
    assert page["offset"] == offset
    assert page["limit"] == limit
    assert len(page["items"]) == size
    assert page["has_more"] is has_more
    assert page["next_offset"] == (offset + limit if has_more else None)


def member_status(task, worker_id):
    return next(member["execution_status"] for member in task["collaboration"]["members"]
                if member["worker_id"] == worker_id)


@pytest.mark.asyncio
async def test_task_browser_pages_all_history_and_case_insensitive_search(browser):
    hub = browser.hub
    archived = []
    for index in range(29):
        browser.clock[0] += 1
        task = await publish(hub, f"Archive {index:02d}")
        archived.append(task["id"])
        await command(hub, "task.retire", {
            "task_id": task["id"], "revision": 1, "reason": "Retain terminal task history."})
    for index in range(3):
        browser.clock[0] += 1
        await publish(hub, f"Unclaimed {index}")

    first = await read(browser, "/v1/task-browser", view="history")
    assert_page(first, total=29, offset=0, limit=25, size=25, has_more=True)
    assert first["counts"] == {"all": 32, "running": 0, "pool": 3, "history": 29}
    last = await read(browser, "/v1/task-browser", view="history", offset=first["next_offset"])
    assert_page(last, total=29, offset=25, limit=25, size=4, has_more=False)
    assert {item["id"] for item in first["items"] + last["items"]} == set(archived)
    assert all(item["state"] == "retired" and not item["running"]
               and item["pool_opportunity"] is None for item in first["items"] + last["items"])
    clamped = await read(browser, "/v1/task-browser", view="history", offset=999)
    assert_page(clamped, total=29, offset=25, limit=25, size=4, has_more=False)
    assert [item["id"] for item in clamped["items"]] == [item["id"] for item in last["items"]]

    searched = await read(browser, "/v1/task-browser", view="all", query="aRcHiVe 2", limit=5)
    assert_page(searched, total=9, offset=0, limit=5, size=5, has_more=True)
    assert all("Archive 2" in item["contract"]["title"] for item in searched["items"])
    exact = await read(browser, "/v1/task-browser", query=archived[12])
    assert [item["id"] for item in exact["items"]] == [archived[12]]
    empty = await read(browser, "/v1/task-browser", view="history", query="missing search", offset=999)
    assert_page(empty, total=0, offset=0, limit=25, size=0, has_more=False)


@pytest.mark.asyncio
async def test_task_browser_running_and_pool_use_actual_execution_and_admission(browser):
    hub = browser.hub
    working = await publish(hub, "Working with a collaborator opening")
    run = await command(hub, "run.start")
    active = await claim(hub, working, run)
    await open_collaboration(hub, working["id"], invitation="Check an independent software case.")
    initial = await publish(hub, "Ready for its first executor")
    await publish(hub, "Missing input cannot enter the pool", inputs=["unavailable-input"])

    finishing = await publish(hub, "Finishing a work turn")
    draining = await claim(hub, finishing, run)
    await legacy_wait(hub, {"reason": "Preserve this task."}, worker(draining))

    waiting = await publish(hub, "Waiting for a new approach")
    sleeping = await claim(hub, waiting, run)
    await pause(hub, sleeping)
    message_task = await publish(hub, "Handling a message while waiting")
    message_origin = await claim(hub, message_task, run)
    await pause(hub, message_origin)
    await command(hub, "message.send", {
        "worker_id": message_origin["worker_id"], "message_id": "browser-message",
        "text": "Keep this as correspondence while the original task is waiting.",
    })
    message = await hub.next_assignment(run["id"])
    assert message["worker_id"] == message_origin["worker_id"]
    assert message["execution_mode"] == "message"

    recovery = await publish(hub, "Execution needs recovery")
    failed = await claim(hub, recovery, run)
    await command(hub, "runtime.abandon", {
        "worker_id": failed["worker_id"], "quiescent": True,
        "reason": "Synthetic local execution failure; no native process started.",
    }, RUNNER)
    ready_task = await publish(hub, "Ready between ordinary turns")
    ready = await claim(hub, ready_task, run)
    await hub.ordinary_turn_completed(ready["worker_id"], ready["id"])

    candidate = await command(hub, "artifact.stage", file_payload(
        hub, active, data={"summary": "An independently reviewable software file."}), worker(active))
    await validate_candidate(hub, candidate["artifact_id"])  # A review opens once validated.
    pool_ids = {item["task_id"] for item in await hub.db.call(lambda c: hub._opportunities(c))}
    assert {working["id"], initial["id"], candidate["review_task_id"]} == pool_ids

    all_tasks = await read(browser, "/v1/task-browser", limit=100)
    by_id = {item["id"]: item for item in all_tasks["items"]}
    running = await read(browser, "/v1/task-browser", view="running")
    assert {item["id"] for item in running["items"]} == {working["id"], finishing["id"]}
    assert running["counts"]["running"] == 2
    pool = await read(browser, "/v1/task-browser", view="pool")
    assert {item["id"] for item in pool["items"]} == pool_ids
    assert pool["counts"]["pool"] == len(pool_ids)
    overlap = by_id[working["id"]]
    assert overlap["running"]
    assert overlap["pool_opportunity"]["opportunity"] == "collaborator"
    assert overlap["pool_opportunity"]["invitation"] == "Check an independent software case."
    assert by_id[candidate["review_task_id"]]["pool_opportunity"]["opportunity"] == "review"
    assert member_status(overlap, active["worker_id"]) == "Working"
    assert member_status(by_id[finishing["id"]], draining["worker_id"]) == "Finishing turn"
    assert member_status(by_id[waiting["id"]], sleeping["worker_id"]) == "Waiting"
    assert member_status(by_id[message_task["id"]], message["worker_id"]) == "Handling message"
    assert member_status(by_id[recovery["id"]], failed["worker_id"]) == "Recovering"
    assert member_status(by_id[ready_task["id"]], ready["worker_id"]) == "Ready"
    assert not by_id[message_task["id"]]["running"]
    assert not by_id[recovery["id"]]["running"]
    assert not by_id[ready_task["id"]]["running"]


@pytest.mark.asyncio
async def test_task_outline_lists_work_tasks_once_and_counts_reviews_on_their_task(browser):
    hub = browser.hub
    root = await publish(hub, "Task 0 of the project")
    other = await publish(hub, "A later task")
    run = await command(hub, "run.start")
    author = await claim(hub, other, run)
    candidate = await command(hub, "artifact.stage", file_payload(
        hub, author, data={"summary": "A synthetic candidate."}), worker(author))
    outline = await read(browser, "/v1/task-outline")
    by_id = {item["id"]: item for item in outline["items"]}
    assert outline["root_task_id"] == root["id"]
    assert set(by_id) == {root["id"], other["id"]}  # The review is counted, not listed.
    assert candidate["review_task_id"] not in by_id and outline["review_count"] == 1
    assert by_id[other["id"]]["reviews"] == {"total": 1, "open": 1, "accepted": 0, "changes": 0, "ended": 0}
    assert by_id[root["id"]]["reviews"]["total"] == 0
    assert by_id[root["id"]]["parent"] is None and by_id[other["id"]]["parent"] is None
    assert [item["worker_id"] for item in by_id[other["id"]]["executors"]] == [author["worker_id"]]
    assert by_id[root["id"]]["executors"] == []


@pytest.mark.asyncio
async def test_task_browser_keeps_completed_reviews_and_terminal_member_labels(browser):
    hub = browser.hub
    task = await publish(hub, "Host with a rejected candidate")
    run = await command(hub, "run.start")
    author = await claim(hub, task, run)
    candidate = await command(hub, "artifact.stage", file_payload(
        hub, author, data={"summary": "A synthetic candidate with a documented defect."}), worker(author))
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    basis = reviewer["task"]["collaboration"]["review"]
    report = {key: basis[key] for key in (
        "candidate_id", "candidate_hash", "task_revision", "input_refs", "policy")}
    await command(hub, "review.reject", {**report,
        "rationale": "The submitted software file omits the required case.",
        "evidence": ["Inspected the exact candidate file."], "qualifications": [],
        "suggestions": ["Add the missing software case."],
    }, worker(reviewer))
    await quiet(hub, reviewer)

    left_task = await publish(hub, "An executor permanently leaves")
    departed = await claim(hub, left_task, run)
    await command(hub, "task.leave", {
        "task_id": left_task["id"], "permanent": True, "reason": "Permanently end this execution."}, worker(departed))
    await quiet(hub, departed)
    retired_task = await publish(hub, "Retired during a live turn")
    ended = await claim(hub, retired_task, run)
    await command(hub, "task.retire", {
        "task_id": retired_task["id"], "revision": 1, "reason": "Retain the stopped task."})

    history = await read(browser, "/v1/task-browser", view="history")
    by_id = {item["id"]: item for item in history["items"]}
    assert set(by_id) == {candidate["review_task_id"], left_task["id"], retired_task["id"]}
    assert by_id[candidate["review_task_id"]]["state"] == "done"
    assert member_status(by_id[candidate["review_task_id"]], reviewer["worker_id"]) == "Ended"
    assert member_status(by_id[left_task["id"]], departed["worker_id"]) == "Left"
    assert member_status(by_id[retired_task["id"]], ended["worker_id"]) == "Finishing turn"
    assert all(item["pool_opportunity"] is None for item in history["items"])
    assert {item["id"] for item in history["items"] if item["running"]} == {retired_task["id"]}
    draining_tasks = await read(browser, "/v1/task-browser", view="running")
    assert retired_task["id"] in {item["id"] for item in draining_tasks["items"]}
    await quiet(hub, ended)
    finished = await read(browser, "/v1/task-browser", view="history", query=retired_task["id"])
    assert member_status(finished["items"][0], ended["worker_id"]) == "Ended"
    assert not finished["items"][0]["running"]
    running_after = await read(browser, "/v1/task-browser", view="running")
    assert retired_task["id"] not in {item["id"] for item in running_after["items"]}


@pytest.mark.asyncio
async def test_summaries_read_together_are_those_read_alone_in_eight_statements(browser):
    # The dashboard's overview summarized each of 573 tasks with its own statements every two
    # seconds in the pilot: 4,519 statements and 22 to 31 s a poll on the busy service.
    from formal_geyser.coordination.task_browser import (
        _TASK_ROWS,
        _running_ids,
        _summaries,
    )

    hub = browser.hub
    run = await command(hub, "run.start")
    host = await publish(hub, "Host with a rejected candidate")
    author = await claim(hub, host, run)
    candidate = await command(hub, "artifact.stage", file_payload(
        hub, author, data={"summary": "A synthetic candidate with a documented defect."}), worker(author))
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    basis = reviewer["task"]["collaboration"]["review"]
    await command(hub, "review.reject", {**{key: basis[key] for key in (
        "candidate_id", "candidate_hash", "task_revision", "input_refs", "policy")},
        "rationale": "The submitted software file omits the required case.",
        "evidence": ["Inspected the exact candidate file."], "qualifications": [],
        "suggestions": ["Add the missing software case."]}, worker(reviewer))
    await quiet(hub, reviewer)
    left = await publish(hub, "An executor permanently leaves")
    departed = await claim(hub, left, run)
    await command(hub, "task.leave", {"task_id": left["id"], "permanent": True,
                                      "reason": "Permanently end this execution."}, worker(departed))
    await quiet(hub, departed)
    paused = await claim(hub, await publish(hub, "Saved for later"), run)
    await pause(hub, paused)
    await publish(hub, "Nobody on it yet")

    def compare(c):
        rows = c.execute(_TASK_ROWS + " ORDER BY t.created_at,t.id").fetchall()
        running = _running_ids(c)
        alone = [_summaries(c, [row], running=running)[0] for row in rows]
        statements: list[str] = []
        c.set_trace_callback(statements.append)
        together = _summaries(c, rows, running=running)
        c.set_trace_callback(None)
        return rows, together, alone, len(statements)
    rows, together, alone, statements = await hub.db.read(compare)
    assert len(rows) == 5 and together == alone and statements == 8
    review = next(item for item in together if item["id"] == candidate["review_task_id"])
    assert review["collaboration"]["review"]["feedback"]["verdict"] == "request_changes"
    assert member_status(next(item for item in together if item["id"] == left["id"]),
                         departed["worker_id"]) == "Left"


@pytest.mark.asyncio
async def test_compact_overview_keeps_referenced_memberships_without_large_task_bodies(browser):
    hub = browser.hub
    marker = "large-private-task-context-"
    task = await publish(hub, "Current worker task", context=marker * 1400,
                         payload={"retained_detail": "payload-body-" * 3000})
    run = await command(hub, "run.start")
    active = await claim(hub, task, run)
    await pause(hub, active)
    retained = await publish(hub, "Retained executor membership")
    former = await publish(hub, "Former membership only")
    for index in range(7):
        await publish(hub, f"Unreferenced task {index}")

    # Historical saved memberships can outlive the current assignment. Reading
    # them must neither grant new execution nor discard their navigation entry.
    def retain(c):
        hub._member(c, retained["id"], active["worker_id"], executor=True)
        hub._member(c, former["id"], active["worker_id"], executor=True)
        c.execute("UPDATE task_members SET membership_state='left' WHERE task_id=? AND worker_id=?",
                  (former["id"], active["worker_id"]))

    await hub.db.call(retain, write=True)
    compact = await read(browser, "/v1/overview", compact="true")
    assert compact["task_count"] == 10
    assert {item["id"] for item in compact["tasks"]} == {task["id"], retained["id"]}
    assert {"name", "workers", "runs", "requests", "delivery_holds",
            "delivery_recovery_error", "drivers"} <= compact.keys()
    current = next(item for item in compact["tasks"] if item["id"] == task["id"])
    retained_summary = next(item for item in compact["tasks"] if item["id"] == retained["id"])
    assert member_status(retained_summary, active["worker_id"]) == "Retained"
    assert {"id", "state", "revision", "current_revision", "created_at", "collaboration"} <= current.keys()
    assert current["contract"]["title"] == "Current worker task"
    assert current["contract"]["kind"] == "work"
    assert "context" not in current["contract"] and "artifacts" not in current
    assert marker not in str(compact)
    assert "payload-body-" not in str(compact)
    page = await read(browser, "/v1/task-browser", query=task["id"])
    assert "context" not in page["items"][0]["contract"]
    assert "artifacts" not in page["items"][0]
    assert len(str(page)) < 12_000

    legacy = await read(browser, "/v1/overview")
    assert len(legacy["tasks"]) == 10
    full = next(item for item in legacy["tasks"] if item["id"] == task["id"])
    assert full["contract"]["context"] == marker * 1400
    assert "artifacts" in full

    await command(hub, "message.send", {
        "worker_id": active["worker_id"], "message_id": "resume-for-status",
        "text": "A useful new approach is available for the original task.",
    })
    wake = await hub.next_assignment(run["id"])
    assert wake["worker_id"] == active["worker_id"]
    await command(hub, "worker.resume_work", {}, worker(wake))
    await hub.ordinary_turn_completed(wake["worker_id"], wake["id"])
    resumed = await hub.next_assignment(run["id"])
    assert resumed["worker_id"] == active["worker_id"] and resumed["execution_mode"] == "work"
    elsewhere = await read(browser, "/v1/task-browser", query=retained["id"])
    assert member_status(elsewhere["items"][0], active["worker_id"]) == "Working elsewhere"


@pytest.mark.asyncio
async def test_message_only_attempt_does_not_make_its_audit_anchor_running(browser):
    hub = browser.hub
    task = await publish(hub, "Unclaimed task is only a message audit anchor")
    run = await command(hub, "run.start")
    source = await command(hub, "worker.register")
    await command(hub, "message.send", {
        "worker_id": source["id"], "message_id": "taskless-message",
        "text": "Correspondence before this worker has selected any task.",
    })
    message = await hub.next_assignment(run["id"])
    assert message["worker_id"] == source["id"] and message["execution_mode"] == "message_only"
    page = await read(browser, "/v1/task-browser")
    assert page["counts"] == {"all": 1, "running": 0, "pool": 1, "history": 0}
    assert page["items"][0]["id"] == task["id"]
    assert page["items"][0]["collaboration"]["members"] == []
    assert not page["items"][0]["running"]
    assert page["items"][0]["pool_opportunity"]["opportunity"] == "initial"
    assert (await read(browser, "/v1/task-browser", view="running"))["items"] == []


@pytest.mark.asyncio
async def test_task_revision_pages_keep_exact_old_bodies_and_current_lifecycle(browser):
    hub = browser.hub
    task = await publish(hub, "Revision 01", context="Original instructions stay immutable.")
    run = await command(hub, "run.start")
    original_worker = await claim(hub, task, run)
    await pause(hub, original_worker)
    versions = {1: task}
    for revision in range(2, 25):
        task = await command(hub, "task.revise", {
            "task_id": task["id"], "revision": revision - 1, "reason": "Clarify the current execution instructions.",
            "contract": {**task["contract"], "title": f"Revision {revision:02d}",
                         "context": f"Exact instructions for revision {revision}."},
        })
        versions[revision] = task
    await command(hub, "task.retire", {
        "task_id": task["id"], "revision": 24, "reason": "Keep the complete revision history."})

    path = f"/v1/tasks/{task['id']}/revisions"
    first = await read(browser, path)
    assert_page(first, total=24, offset=0, limit=20, size=20, has_more=True)
    assert first["current_revision"] == 24
    assert [item["revision"] for item in first["items"]] == list(range(24, 4, -1))
    last = await read(browser, path, offset=999)
    assert_page(last, total=24, offset=20, limit=20, size=4, has_more=False)
    assert [item["revision"] for item in last["items"]] == [4, 3, 2, 1]
    for item in first["items"] + last["items"]:
        assert item["title"] == versions[item["revision"]]["contract"]["title"]
        assert item["contract_hash"] == versions[item["revision"]]["contract_hash"]
        assert "contract" not in item and "context" not in item

    old = await read(browser, f"/v1/tasks/{task['id']}", revision=1)
    assert old["contract"] == versions[1]["contract"]
    assert old["contract_hash"] == versions[1]["contract_hash"]
    assert old["revision"] == 1 and old["current_revision"] == 24
    assert old["state"] == "retired"
    assert not old["running"]
    assert member_status(old, original_worker["worker_id"]) == "Ended"
    current = await read(browser, f"/v1/tasks/{task['id']}")
    assert current["contract"] == versions[24]["contract"]
    assert not current["running"]
    assert member_status(current, original_worker["worker_id"]) == "Ended"


@pytest.mark.asyncio
async def test_task_browsing_is_read_only_and_does_not_arm_or_acknowledge(browser):
    hub = browser.hub
    task = await publish(hub, "Read without activation")
    run = await command(hub, "run.start")
    attempt = await claim(hub, task, run)
    await command(hub, "artifact.stage", file_payload(
        hub, attempt, data={"summary": "A fresh review has never been inspected by the pool."}), worker(attempt))
    await pause(hub, attempt)
    await command(hub, "message.send", {
        "worker_id": attempt["worker_id"], "message_id": "unread-browser-message",
        "text": "This message remains pending until execution is explicitly authorized.",
    })
    await command(hub, "run.pause", {"run_id": run["id"]})
    before = await hub.db.call(lambda c: list(c.iterdump()))
    for view in ("all", "running", "pool", "history"):
        await read(browser, "/v1/task-browser", view=view)
    await read(browser, "/v1/overview", compact="true")
    await read(browser, f"/v1/tasks/{task['id']}/revisions")
    await read(browser, f"/v1/tasks/{task['id']}", revision=1)
    after = await hub.db.call(lambda c: list(c.iterdump()))
    assert after == before
    assert browser.app.state.driver.armed == set()
    assert browser.app.state.driver.passes == {}
    assert browser.app.state.driver.pending_wakes == set()
    assert (await hub.run(run["id"]))["state"] == "paused"


@pytest.mark.asyncio
async def test_task_browser_and_revision_pages_refuse_non_operators_and_validate_pagination(browser, monkeypatch):
    task = await publish(browser.hub, "Pagination contract")
    revisions = f"/v1/tasks/{task['id']}/revisions"
    monkeypatch.setattr("formal_geyser.coordination.api.operator_request", lambda request, token: False)
    for path in ("/v1/task-browser", revisions, "/v1/overview?compact=true"):
        response = await browser.client.get(path)
        assert response.status_code == 401
    monkeypatch.setattr("formal_geyser.coordination.api.operator_request", lambda request, token: True)
    for path in ("/v1/task-browser", revisions):
        for params in ({"offset": -1}, {"limit": 0}, {"limit": 101}, {"offset": "invalid"}):
            response = await browser.client.get(path, params=params)
            assert response.status_code == 422, response.text
    invalid_view = await browser.client.get("/v1/task-browser", params={"view": "assigned"})
    assert invalid_view.status_code == 422
    assert (await browser.client.get("/v1/tasks/missing-task/revisions")).status_code == 404
