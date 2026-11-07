"""The waker: after an integration publishes from a subscribed task, it judges each subscribed
wait, and a wake ends the wait."""
from __future__ import annotations

import json

import pytest

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.runtime import ScriptedHarness, ScriptedTurn, WorkerRunner
from formal_geyser.coordination.waker import WAKE_ATTEMPTS

from .publication_helpers import new_hub
from .test_hub import accept, checkpoint, claim, command, publish, start, submit, worker

NEED = "The provider's exact result record."


@pytest.fixture
async def hub(tmp_path):
    result = await new_hub(tmp_path)
    try:
        yield result
    finally:
        await result.close()


def answering(*answers):
    """A harness factory whose decisions answer `answers` in turn, and the models it was opened with."""
    pending, opened = list(answers), []

    class Answering(ScriptedHarness):
        async def open(self, **options):
            opened.append(options)
            return await super().open(**options)

    def factory():
        answer = pending.pop(0)
        return Answering([ScriptedTurn([], visible_output=answer if isinstance(answer, str) else json.dumps(answer))])
    return factory, opened


async def subscribed_publication(hub):
    """A worker waiting on a provider task that has just published its result; the run, the
    wait and the result."""
    provider, waiting = await publish(hub, "Provider"), await publish(hub, "Waiting work")
    run = await start(hub)
    _, author = await claim(hub, provider, await worker(hub), run)
    _, waiter = await claim(hub, waiting, await worker(hub), run)
    parked = await command(hub, "task.pause", {"reason": "Wait for the provider.", "checkpoint": checkpoint(),
                                               "predicate": {"subscriptions": [{"task_id": provider["id"], "need": NEED}]}},
                           waiter)
    result = await submit(hub, author, result="ready")
    await accept(hub, result["artifact_id"])  # Reviewed and published in an Integrator batch.
    return run, provider, parked, result


async def wake_intents(hub):
    return await hub.db.call(lambda c: [hub._helper(c, row["id"]) for row in c.execute(
        "SELECT id FROM helper_intents WHERE kind='wake' ORDER BY created_at,rowid")])


async def test_a_publication_asks_the_waker_about_the_waits_subscribed_to_its_task(hub):
    run, provider, parked, result = await subscribed_publication(hub)
    [intent] = await wake_intents(hub)
    assert intent["state"] == "prepared" and intent["source_task_id"] is None
    assert intent["candidates"]["attempt"] == 1
    assert intent["candidates"]["pairs"] == [{"continuation_id": parked["continuation_id"], "task_id": provider["id"]}]
    assert (await hub.worker(intent["source_worker_id"]))["display_name"] == "Waker"
    packet = await hub.wake_packet(intent)
    [pair] = packet["pairs"]
    published = pair.pop("published")
    assert pair == {"pair": "pair_1", "need": NEED, "task_title": "Provider"}
    assert [(item["artifact_id"], item["task_title"]) for item in published] == [(result["artifact_id"], "Provider")]
    assert published[0]["kind"] and published[0]["published_at"]
    # Until a waker model is chosen, its decisions wait.
    assert await hub.due_wake_intents(run["id"]) == []
    await command(hub, "runtime.set_waker_model", {"model": "gpt-6.1-terra", "effort": "medium"})
    assert [item["id"] for item in await hub.due_wake_intents(run["id"])] == [intent["id"]]


async def test_a_wake_ends_the_wait_and_says_only_that_the_task_changed(hub):
    run, provider, parked, _ = await subscribed_publication(hub)
    await command(hub, "runtime.set_waker_model", {"model": "gpt-6.1-terra", "effort": "medium"})
    with pytest.raises(HubError):
        await command(hub, "runtime.set_waker_model", {"model": "gpt-6.1-terra", "effort": "most"})
    [intent] = await hub.due_wake_intents(run["id"])
    factory, opened = answering({"pair_1": True})
    runner = WorkerRunner(hub, factory)
    models = []
    original = runner._new_harness

    async def new_harness(backend="codex", **options):
        models.append(options)
        return await original(backend, **options)
    runner._new_harness = new_harness
    result = await runner.run_helper(intent)
    assert result["directive"] == "wake_decided"
    assert result["result"] == {"outcome": "decided", "woken": [parked["continuation_id"]]}
    assert models == [{"model": "gpt-6.1-terra", "effort": "medium", "decision": True}]
    assert opened[0]["tools"] == [] and opened[0]["read_only"] is True
    [wait] = [item for item in await hub.list_continuations() if item["id"] == parked["continuation_id"]]
    assert (wait["state"], wait["wake_reason"]) == ("reconsider", "subscribed_task_changed")
    notices = [event for event in await hub.events(limit=1000) if event["kind"] == "attention.required"
               and event["payload"] == {"reason": "subscribed_task_changed"}]
    assert [event["object_id"] for event in notices] == [provider["id"]]
    assert await hub.due_wake_intents(run["id"]) == []


async def test_a_pair_answered_false_keeps_waiting(hub):
    run, _, parked, _ = await subscribed_publication(hub)
    await command(hub, "runtime.set_waker_model", {"model": "gpt-6.1-terra", "effort": "medium"})
    [intent] = await hub.due_wake_intents(run["id"])
    result = await WorkerRunner(hub, answering({"pair_1": False})[0]).run_helper(intent)
    assert result["result"] == {"outcome": "decided", "woken": []}
    [wait] = [item for item in await hub.list_continuations() if item["id"] == parked["continuation_id"]]
    assert wait["state"] == "waiting"


async def test_a_wait_that_ended_meanwhile_is_not_judged(hub):
    run, provider, parked, _ = await subscribed_publication(hub)
    await command(hub, "runtime.set_waker_model", {"model": "gpt-6.1-terra", "effort": "medium"})
    [intent] = await hub.due_wake_intents(run["id"])
    await command(hub, "task.retire", {"task_id": provider["id"], "revision": provider["revision"]})

    def no_model():
        raise AssertionError("No wait is left to judge")
    result = await WorkerRunner(hub, no_model).run_helper(intent)
    assert result["result"] == {"outcome": "decided", "woken": []}


async def test_answers_outside_the_schema_ask_again_then_wait_for_the_operator(hub):
    run, _, parked, _ = await subscribed_publication(hub)
    await command(hub, "runtime.set_waker_model", {"model": "gpt-6.1-terra", "effort": "medium"})
    answers = ["not JSON", {"pair_1": "yes"}, {"pair_2": True}, {}, {"pair_1": True, "reason": "ready"}]
    assert len(answers) == WAKE_ATTEMPTS
    factory, _ = answering(*answers)
    for attempt in range(1, WAKE_ATTEMPTS + 1):
        [intent] = await hub.due_wake_intents(run["id"])
        assert intent["candidates"]["attempt"] == attempt
        await WorkerRunner(hub, factory).run_helper(intent)
    assert await hub.due_wake_intents(run["id"]) == []
    intents = await wake_intents(hub)
    assert [item["state"] for item in intents] == ["completed"] * (WAKE_ATTEMPTS - 1) + ["uncertain"]
    assert f"{WAKE_ATTEMPTS} times" in intents[-1]["reason"]
    [wait] = [item for item in await hub.list_continuations() if item["id"] == parked["continuation_id"]]
    assert wait["state"] == "waiting"
