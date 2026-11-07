"""A worker that left a task can be invited back by a current executor or the operator."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Command, Role
from tests.coordination.lifecycle_helpers import open_collaboration
from tests.coordination.publication_helpers import new_hub

OPERATOR = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)


async def command(hub, name, payload=None, actor=OPERATOR):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


async def publish(hub, title):
    return await command(hub, "task.publish", {"contract": {
        "title": title, "objective": "Inspect the saved software workflow."}})


def worker(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"],
                 attempt_id=attempt["id"], fencing_token=attempt["fencing_token"])


async def claim(hub, task, run, **extra):
    registered = await command(hub, "worker.register")
    return await command(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"],
                                            "worker_id": registered["id"], "run_id": run["id"], **extra})


async def quiet(hub, attempt):
    return await command(hub, "runtime.quiescent", {
        "worker_id": attempt["worker_id"], "snapshot": hub.blobs.snapshot(hub.workspace(attempt["worker_id"]))}, RUNNER)


async def leave(hub, attempt, task):
    await command(hub, "task.leave", {"task_id": task["id"], "permanent": True,
                                      "reason": "My part is done; my coauthor finishes the final candidate."},
                  worker(attempt))
    await quiet(hub, attempt)


@pytest.fixture
async def pair(tmp_path):
    """A task with two executors, the second of which has left it."""
    hub = await new_hub(tmp_path)
    try:
        task = await publish(hub, "A task with a collaborator")
        run = await command(hub, "run.start")
        author = await claim(hub, task, run)
        await open_collaboration(hub, task["id"])
        helper = await claim(hub, task, run, plan="Check the remaining software case.")
        await leave(hub, helper, task)
        yield hub, task, run, author, helper
    finally:
        await hub.close()


def read(hub, sql, *args):
    return hub.db.call(lambda c: [dict(row) for row in c.execute(sql, args).fetchall()])


@pytest.mark.asyncio
async def test_an_executor_invites_a_departed_worker_back_and_it_resumes_the_task(pair):
    hub, task, run, author, helper = pair
    [member] = await read(hub, "SELECT membership_state,work_reason,runnable FROM task_members WHERE task_id=? AND worker_id=?",
                          task["id"], helper["worker_id"])
    # Its own reason survives the end of its last turn, for whoever may invite it back.
    assert member == {"membership_state": "left", "runnable": 0,
                      "work_reason": "My part is done; my coauthor finishes the final candidate."}

    invited = await command(hub, "task.invite_back", {
        "task_id": task["id"], "worker_id": helper["worker_id"],
        "reason": "The reviewer asked for the case you wrote; please extend it."}, worker(author))
    assert invited["worker_id"] == helper["worker_id"] and invited["invited_by"].startswith("Worker")
    [member] = await read(hub, "SELECT membership_state,work_reason FROM task_members WHERE task_id=? AND worker_id=?",
                          task["id"], helper["worker_id"])
    assert member == {"membership_state": "member", "work_reason": "invited_back"}
    [ticket] = await read(hub, "SELECT state,wake_reason,snapshot FROM continuations WHERE id=?", invited["continuation_id"])
    assert ticket["state"] == "reconsider" and ticket["wake_reason"] == "invited_back" and ticket["snapshot"]
    [event] = await read(hub, "SELECT payload_json,worker_id FROM events WHERE kind='task.invited_back'")
    assert event["worker_id"] == helper["worker_id"]
    assert json.loads(event["payload_json"])["reason"] == "The reviewer asked for the case you wrote; please extend it."
    returned = await hub.worker(helper["worker_id"])
    assert returned["current_task_id"] == task["id"] and returned["work_intention"] == "waiting"

    # Admission resumes it as it resumes a woken wait, while the author keeps working.
    resumed = await command(hub, "continuation.resume", {"continuation_id": invited["continuation_id"],
                                                         "run_id": run["id"]}, RUNNER)
    assert resumed["worker_id"] == helper["worker_id"] and resumed["task_id"] == task["id"]
    assert resumed["resume_reason"] == "invited_back"
    executors = await read(hub, "SELECT worker_id FROM task_members WHERE task_id=? AND executor=1 "
                                "AND membership_state='member' ORDER BY worker_id", task["id"])
    assert {row["worker_id"] for row in executors} == {author["worker_id"], helper["worker_id"]}


@pytest.mark.asyncio
async def test_the_operator_may_invite_back_but_no_one_else_and_never_a_busy_worker(pair):
    hub, task, run, author, helper = pair
    reason = {"reason": "Your earlier case is needed again."}
    with pytest.raises(HubError) as missing:
        await command(hub, "task.invite_back", {"task_id": task["id"], "worker_id": helper["worker_id"], "reason": " "})
    assert missing.value.code == "REASON_REQUIRED"
    with pytest.raises(HubError) as current:
        await command(hub, "task.invite_back", {"task_id": task["id"], "worker_id": author["worker_id"], **reason})
    assert current.value.code == "FORMER_EXECUTOR_REQUIRED" and "still an executor" in str(current.value)
    stranger_task = await publish(hub, "Someone else's task")
    stranger = await claim(hub, stranger_task, run)
    with pytest.raises(HubError) as outsider:
        await command(hub, "task.invite_back", {"task_id": task["id"], "worker_id": helper["worker_id"], **reason},
                      worker(stranger))
    assert outsider.value.code == "EXECUTOR_REQUIRED"
    with pytest.raises(HubError) as never:
        await command(hub, "task.invite_back", {"task_id": task["id"], "worker_id": stranger["worker_id"], **reason},
                      worker(author))
    assert never.value.code == "FORMER_EXECUTOR_REQUIRED" and "not a former executor" in str(never.value)

    # Busy on another task: refused, and the inviter is told to message it instead.
    other = await publish(hub, "The departed worker's next task")
    await command(hub, "task.claim", {"task_id": other["id"], "revision": other["revision"],
                                      "worker_id": helper["worker_id"], "run_id": run["id"]})
    with pytest.raises(HubError) as busy:
        await command(hub, "task.invite_back", {"task_id": task["id"], "worker_id": helper["worker_id"], **reason},
                      worker(author))
    assert busy.value.code == "WORKER_BUSY" and "talk_send" in str(busy.value)
    [member] = await read(hub, "SELECT membership_state FROM task_members WHERE task_id=? AND worker_id=?",
                          task["id"], helper["worker_id"])
    assert member["membership_state"] == "left"


@pytest.mark.asyncio
async def test_the_operator_invites_back_by_name_and_a_retired_task_takes_no_one(pair):
    hub, task, run, author, helper = pair
    name = (await hub.worker(helper["worker_id"]))["display_name"]
    invited = await command(hub, "task.invite_back", {"task_id": task["id"], "worker_id": name,
                                                      "reason": "Please finish the case you began."})
    assert invited["worker_id"] == helper["worker_id"] and invited["invited_by"] == "the operator"

    alone = await publish(hub, "A task with one executor")
    solo = await claim(hub, alone, run)
    await leave(hub, solo, alone)  # Its last executor left: the task is retired.
    with pytest.raises(HubError) as retired:
        await command(hub, "task.invite_back", {"task_id": alone["id"], "worker_id": solo["worker_id"],
                                                "reason": "Come back."})
    assert retired.value.code == "TASK_TERMINAL"
