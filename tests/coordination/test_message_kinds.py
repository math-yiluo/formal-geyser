"""Peer messages since 2026-11-07: notices, questions and answers, and waits for an answer, which end
at the project's answer deadline if none comes."""
from __future__ import annotations

from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    Command,
    Limits,
    ProjectConfig,
    Role,
    WaitConfig,
)

OP, RUNNER = Actor(role=Role.OPERATOR), Actor(role=Role.RUNNER)
CHECKPOINT = {"summary": "Saved progress", "next_action": "Continue from the saved progress", "input_refs": []}


class Clock:
    def __init__(self):
        self.now = 1_000_000

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
async def hub(tmp_path, clock):
    initialize(tmp_path, ProjectConfig(limits=Limits(), waits=WaitConfig(answer_deadline_minutes=30)))
    result = Hub(tmp_path, clock=clock)
    yield result
    await result.close()


async def command(hub, name, payload=None, actor=OP, key=None):
    return await hub.execute(Command(name=name, key=key or uuid4().hex, payload=payload or {}), actor)


async def two_workers(hub):
    """Two workers, each working on a task of its own, as their actors."""
    run = await command(hub, "run.start")
    actors = []
    for title in ("Asker", "Expert"):
        task = await command(hub, "task.publish", {"contract": {"title": title, "objective": f"Do the {title} work."}})
        worker = await command(hub, "worker.register")
        attempt = await command(hub, "task.claim", {
            "task_id": task["id"], "revision": 1, "worker_id": worker["id"], "run_id": run["id"]})
        actors.append(Actor(role=Role.WORKER, worker_id=worker["id"], attempt_id=attempt["id"],
                            fencing_token=attempt["fencing_token"]))
    return actors


async def send(hub, sender, text, **fields):
    return await command(hub, "talk.send", {"message_id": uuid4().hex, "text": text, **fields}, sender)


async def pending_wake(hub, actor):
    return (await hub.worker(actor.worker_id))["pending_wake"]


async def pause(hub, actor, predicate):
    return await command(hub, "task.pause", {"reason": "Wait for the answer.", "checkpoint": CHECKPOINT,
                                             "predicate": predicate}, actor)


async def test_a_question_has_one_answerer_and_its_answer_goes_to_the_asker(hub):
    asker, expert = await two_workers(hub)
    question = await send(hub, asker, "Which normalization does the source use?",
                          recipient_id=expert.worker_id, kind="question")
    assert question["kind"] == "question"
    for fields, code in (({"kind": "answer"}, "INVALID_INPUT"),
                         ({"answers": question["id"]}, "INVALID_INPUT"),
                         ({"kind": "remark", "recipient_id": expert.worker_id}, "INVALID_INPUT"),
                         ({"kind": "answer", "answers": "no-such-question"}, "NOT_FOUND")):
        with pytest.raises(HubError) as error:
            await send(hub, expert, "The unitary one.", **fields)
        assert error.value.code == code
    with pytest.raises(HubError) as error:  # The asker cannot answer its own question.
        await send(hub, asker, "The unitary one.", kind="answer", answers=question["id"])
    assert error.value.code == "FORBIDDEN"
    with pytest.raises(HubError) as error:
        await send(hub, expert, "The unitary one.", kind="answer", answers=question["id"],
                   recipient_id=expert.worker_id)
    assert error.value.code in {"INVALID_RECIPIENT"}
    answer = await send(hub, expert, "The unitary one.", kind="answer", answers=question["id"])
    assert (answer["recipient_id"], answer["kind"], answer["answers"]) == (asker.worker_id, "answer", question["id"])
    page = await hub.peer_unread(asker, worker_id=asker.worker_id)
    assert [(item["kind"], item["answers"]) for item in page["messages"]] == [("answer", question["id"])]
    page = await hub.peer_unread(expert, worker_id=expert.worker_id)
    assert [item["kind"] for item in page["messages"]] == ["question"]


async def test_a_notice_keeps_its_earlier_receipt(hub):
    asker, expert = await two_workers(hub)
    payload = {"message_id": "stable", "recipient_id": expert.worker_id, "text": "The draft is in notes."}
    first = await command(hub, "talk.send", payload, asker)
    assert "kind" not in first
    assert await command(hub, "talk.send", {**payload, "kind": "notice"}, asker) == first
    with pytest.raises(HubError, match="different data"):
        await command(hub, "talk.send", {**payload, "kind": "question"}, asker)


async def test_a_notice_leaves_a_waiting_worker_waiting(hub):
    asker, expert = await two_workers(hub)
    question = await send(hub, asker, "Is the lemma true for p = 2?", recipient_id=expert.worker_id, kind="question")
    await send(hub, expert, "I am reading the source now.", recipient_id=asker.worker_id)
    assert await pending_wake(hub, asker)
    # Waiting on the answer, the asker no longer turns to the notice it has not read.
    await pause(hub, asker, {"answers": [question["id"]]})
    assert not await pending_wake(hub, asker)
    await send(hub, expert, "Still reading.", recipient_id=asker.worker_id)
    assert not await pending_wake(hub, asker)
    assert len((await hub.peer_unread(OP, worker_id=asker.worker_id))["messages"]) == 2
    await send(hub, expert, "Which edition do you cite?", recipient_id=asker.worker_id, kind="question")
    assert await pending_wake(hub, asker)


async def test_an_answer_ends_the_wait_for_it(hub):
    asker, expert = await two_workers(hub)
    question = await send(hub, asker, "Is the lemma true for p = 2?", recipient_id=expert.worker_id, kind="question")
    asked = await send(hub, expert, "Which edition?", recipient_id=asker.worker_id, kind="question")
    for questions in ([asked["id"]], ["no-such-question"]):  # Only its own questions.
        with pytest.raises(HubError) as error:
            await pause(hub, asker, {"answers": questions})
        assert error.value.code == "QUESTION_NOT_FOUND"
    parked = await pause(hub, asker, {"answers": [question["id"]]})
    [wait] = await hub.list_continuations()
    assert wait["state"] == "waiting"
    await send(hub, expert, "Yes, by the same argument.", kind="answer", answers=question["id"])
    [wait] = await hub.list_continuations()
    assert (wait["id"], wait["state"]) == (parked["continuation_id"], "eligible")


async def test_a_wait_for_an_answer_ends_at_the_deadline(hub, clock):
    asker, expert = await two_workers(hub)
    question = await send(hub, asker, "Is the lemma true for p = 2?", recipient_id=expert.worker_id, kind="question")
    request = await command(hub, "message.request", {
        "request_key": "edition", "title": "Edition", "text": "Which edition is meant?"}, expert)
    await pause(hub, asker, {"answers": [question["id"]]})
    await pause(hub, expert, request["wait_predicate"])
    deadlines = await hub.db.call(lambda c: [tuple(row) for row in c.execute(
        "SELECT due_at FROM wait_deadlines ORDER BY due_at")])
    assert deadlines == [(clock.now + 30 * 60_000,)] * 2
    clock.now += 30 * 60_000 - 1
    assert await hub.end_overdue_waits() == 0
    # Once a minute at most: the next pass does not read the deadlines again.
    clock.now += 1
    assert await hub.end_overdue_waits() == 0
    hub._overdue_check_at = 0.0
    assert await hub.end_overdue_waits() == 2
    assert {(wait["state"], wait["wake_reason"]) for wait in await hub.list_continuations()} == {
        ("reconsider", "answer_deadline")}


async def test_a_wait_without_an_answer_has_no_deadline(hub):
    asker, expert = await two_workers(hub)
    other = await command(hub, "task.publish", {"contract": {"title": "Other", "objective": "Do the other work."}})
    await pause(hub, asker, {"subscriptions": [{"task_id": other["id"], "need": "The other work."}]})
    assert await hub.db.call(lambda c: c.execute("SELECT count(*) FROM wait_deadlines").fetchone()[0]) == 0
