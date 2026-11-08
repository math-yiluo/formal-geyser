"""A worker waits for a candidate of its own task to reach an outcome: published by the
Integrator, or rejected, deferred, withdrawn or superseded. The check is mechanical."""
from __future__ import annotations

from pathlib import Path

import pytest

from formal_geyser.coordination.batch_integration import BATCH_DELAY_MS
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Role
from tests.coordination.lifecycle_helpers import select_review
from tests.coordination.publication_helpers import new_hub
from tests.coordination.review_helpers import accept
from tests.coordination.test_collaborative_core import (
    VALIDATOR,
    claim,
    command,
    machine,
    publish,
    quiet,
    review,
    stage,
    start,
    worker_actor,
)

CHECKPOINT = {"summary": "The candidate is staged.", "next_action": "Act on its outcome.", "input_refs": []}


@pytest.fixture
async def case(tmp_path):
    hub = await new_hub(tmp_path)
    now = [1_800_000_000_000]
    hub.clock = lambda: now[0]
    await hub.db.call(hub._enable_integrator, write=True)
    run = await start(hub)
    try:
        yield hub, run, now
    finally:
        await hub.close()


async def pause(hub, attempt, *candidates):
    return await command(hub, "task.pause", {"reason": "Wait for the candidate's outcome.", "checkpoint": CHECKPOINT,
                                             "predicate": {"publications": list(candidates)}}, worker_actor(attempt))


async def wait_of(hub, attempt):
    [row] = [item for item in await hub.list_continuations() if item["origin_attempt"] == attempt["id"]]
    return row


async def notices(hub, task_id, candidate_id):
    return await hub.db.call(lambda c: [row["reason"] for row in c.execute(
        "SELECT reason FROM notifications WHERE task_id=? AND object_id=? ORDER BY created_at,id",
        (task_id, candidate_id))])


async def staged(hub, run, title):
    task = await publish(hub, title)
    author = await claim(hub, task, run)
    return task, author, await stage(hub, author)


async def accepted(hub, run, candidate):
    await machine(hub, candidate)
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    await accept(hub, reviewer)
    await hub.ordinary_turn_completed(reviewer["worker_id"], reviewer["id"])
    return reviewer


async def integrator(hub, run, now):
    now[0] += BATCH_DELAY_MS
    return await hub.db.call(lambda c: hub._batch_assignment(c, run["id"], None), write=True)


async def publish_batch(hub, assignment):
    actor = worker_actor(assignment)

    async def call(name, payload, *, actor=actor, suffix=""):
        return await command(hub, name, payload, actor)

    batch = await hub.prepare_integration_batch(actor, call)
    frozen = hub._repository().finalize(Path(batch["workspace"]), hub.blobs)
    runtime = Actor(role=Role.RUNNER, worker_id=actor.worker_id, attempt_id=actor.attempt_id,
                    fencing_token=actor.fencing_token)
    batch = await command(hub, "integrator.freeze.store", {"workspace_id": batch["workspace_id"], "frozen": frozen},
                          runtime)
    reports = {item["candidate_id"]: {"verdict": "accepted", "policy": "general.peer@1", "qualifications": [],
                                      "details": {"integration_snapshot": frozen["snapshot"],
                                                  "fresh_integrated_validation": True}}
               for item in batch["items"]}
    await command(hub, "integrator.validation", {"round_id": batch["round_id"], "reports": reports}, runtime)
    await command(hub, "integrator.publish", {"round_id": batch["round_id"]}, actor)
    await quiet(hub, assignment)


async def test_the_wait_holds_through_review_and_ends_at_publication(case):
    hub, run, now = case
    task, author, candidate = await staged(hub, run, "Published contribution")
    await pause(hub, author, candidate["artifact_id"])
    await quiet(hub, author)
    wait = await wait_of(hub, author)
    assert wait["state"] == "waiting"
    # Mechanical: no answer deadline, and no Waker decision.
    assert await hub.db.call(lambda c: c.execute("SELECT count(*) FROM wait_deadlines").fetchone()[0]) == 0
    await accepted(hub, run, candidate)
    assert (await wait_of(hub, author))["state"] == "waiting"  # Accepted, not yet published.
    await publish_batch(hub, await integrator(hub, run, now))
    wait = await wait_of(hub, author)
    assert (wait["state"], wait["wake_reason"]) == ("reconsider", "candidate_published")
    told = await notices(hub, task["id"], candidate["artifact_id"])
    assert any("was published to the Library by the Integrator" in text for text in told)
    assert await hub.db.call(lambda c: c.execute("SELECT count(*) FROM helper_intents WHERE kind='wake'").fetchone()[0]) == 0


async def test_a_review_rejection_ends_the_waits_of_the_author_and_a_co_executor(case):
    hub, run, _ = case
    task = await publish(hub, "Rejected contribution")
    author, peer = await claim(hub, task, run), await claim(hub, task, run)
    candidate = await stage(hub, author)
    await pause(hub, author, candidate["artifact_id"])
    await pause(hub, peer, candidate["artifact_id"])
    await machine(hub, candidate)
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    await review(hub, reviewer, "request_changes")
    await quiet(hub, reviewer)  # The verdict commits once the review turn is quiescent.
    for attempt in (author, peer):
        wait = await wait_of(hub, attempt)
        assert (wait["state"], wait["wake_reason"]) == ("reconsider", "candidate_rejected")
    told = await notices(hub, task["id"], candidate["artifact_id"])
    assert [text for text in told if "was rejected in review" in text] == [told[-1]]  # Told once.


async def test_a_deferral_ends_the_wait_and_says_why(case):
    hub, run, now = case
    task, author, candidate = await staged(hub, run, "Deferred contribution")
    await pause(hub, author, candidate["artifact_id"])
    await accepted(hub, run, candidate)
    assignment = await integrator(hub, run, now)
    await command(hub, "integrator.defer", {"candidate_id": candidate["artifact_id"],
                                            "reason": "Waiting for its source page."}, worker_actor(assignment))
    wait = await wait_of(hub, author)
    assert (wait["state"], wait["wake_reason"]) == ("reconsider", "candidate_deferred")
    told = await notices(hub, task["id"], candidate["artifact_id"])
    assert any("deferred" in text and "Waiting for its source page." in text for text in told)


async def test_a_machine_rejection_and_a_withdrawal_end_the_wait(case):
    hub, run, _ = case
    task, author, candidate = await staged(hub, run, "Invalid contribution")
    await pause(hub, author, candidate["artifact_id"])
    await command(hub, "artifact.accept", {"artifact_id": candidate["artifact_id"],
                                           "policy": candidate["publication_policy"], "verdict": "rejected",
                                           "details": {"scope": "test domain evidence"}, "qualifications": []},
                  VALIDATOR)
    wait = await wait_of(hub, author)
    assert (wait["state"], wait["wake_reason"]) == ("reconsider", "candidate_rejected")
    assert any("rejected by its machine validation" in text
               for text in await notices(hub, task["id"], candidate["artifact_id"]))

    other, writer, withdrawn = await staged(hub, run, "Abandoned contribution")
    await pause(hub, writer, withdrawn["artifact_id"])
    await command(hub, "artifact.withdraw", {"artifact_id": withdrawn["artifact_id"], "reason": "Route abandoned."})
    wait = await wait_of(hub, writer)
    assert (wait["state"], wait["wake_reason"]) == ("reconsider", "candidate_withdrawn")


async def test_only_an_unsettled_candidate_of_the_own_task_is_awaited(case):
    hub, run, _ = case
    _, author, candidate = await staged(hub, run, "Own contribution")
    _, stranger, foreign = await staged(hub, run, "Foreign contribution")
    for named in (foreign["artifact_id"], "no-such-candidate"):
        with pytest.raises(HubError) as refused:
            await pause(hub, author, named)
        assert refused.value.code == "PUBLICATION_NOT_OWN"
    await command(hub, "artifact.withdraw", {"artifact_id": candidate["artifact_id"], "reason": "Superseded draft."})
    with pytest.raises(HubError) as refused:
        await pause(hub, author, candidate["artifact_id"])
    assert refused.value.code == "PUBLICATION_SETTLED" and "was withdrawn" in refused.value.message
    assert [item for item in await hub.list_continuations() if item["origin_attempt"] == author["id"]] == []
    with pytest.raises(HubError) as refused:
        await command(hub, "task.pause", {"reason": "Nothing.", "checkpoint": CHECKPOINT, "predicate": {}},
                      worker_actor(stranger))
    assert refused.value.code == "WAIT_CONDITION_REQUIRED" and "publications" in refused.value.message
