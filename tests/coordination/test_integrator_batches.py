"""Dedicated integrator admission and fixed-batch publication, without models."""

import json
import threading
from pathlib import Path
from uuid import uuid4

import pytest

from formal_geyser.coordination.batch_integration import BATCH_DELAY_MS
from formal_geyser.coordination.content import NativeContentStore
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Role
from tests.coordination.publication_helpers import new_hub
from tests.coordination.review_helpers import accept, basis, reviewing
from tests.coordination.test_collaborative_core import (
    claim,
    command,
    machine,
    publish,
    quiet,
    stage,
    start,
    worker_actor,
)
from tests.coordination.test_repository import contents, manifest
from tests.coordination.test_verification_reuse import settle_tree


@pytest.fixture
async def batch_case(tmp_path):
    hub = await new_hub(tmp_path)
    now = [1_800_000_000_000]
    hub.clock = lambda: now[0]
    await hub.db.call(hub._enable_integrator, write=True)
    run = await start(hub)
    try:
        yield hub, run, now
    finally:
        await hub.close()


NOTES = {
    "overlaps": [{"existing": "Langlands.SmoothRep", "relation": "duplicate",
                  "note": "The same full subcategory of smooth representations; either can replace the other."}],
    "placement": "Langlands/Representation/Category, beside the existing smooth representation category.",
}


async def ready(hub, run, title, *, integration_notes=None):
    reviewer = await reviewing(hub, run, title)
    if integration_notes is None:
        await accept(hub, reviewer)
    else:
        await command(hub, "review.accept", {**basis(reviewer), "integration_notes": integration_notes},
                      worker_actor(reviewer))
    await hub.ordinary_turn_completed(reviewer["worker_id"], reviewer["id"])
    return reviewer


async def choose(hub, run):
    return await hub.db.call(lambda c: hub._batch_assignment(c, run["id"], None), write=True)


async def test_timer_counts_from_first_candidate_then_completed_batch(batch_case):
    hub, run, now = batch_case
    first = await ready(hub, run, "First accepted contribution")
    assert (await hub.worker(first["worker_id"]))["work_intention"] == "waiting"
    assert await choose(hub, run) is None
    now[0] += BATCH_DELAY_MS - 1
    assert await choose(hub, run) is None
    now[0] += 1
    assignment = await choose(hub, run)
    assert (await hub.worker(assignment["worker_id"]))["display_name"] == "Integrator"
    assert (await hub.worker(assignment["worker_id"]))["worker_number"] is None
    batch = await hub.db.call(lambda c: hub._batch(c))
    candidate = batch["items"][0]["candidate_id"]
    await command(
        hub,
        "integrator.defer",
        {"candidate_id": candidate, "reason": "Waiting for a precise source clarification."},
        worker_actor(assignment),
    )
    await command(hub, "integrator.finish_empty", {}, worker_actor(assignment))
    await quiet(hub, assignment)
    completed = now[0]
    second = await ready(hub, run, "Second accepted contribution")
    now[0] += BATCH_DELAY_MS - 1
    assert await choose(hub, run) is None
    now[0] += 1
    resumed = await choose(hub, run)
    assert resumed["worker_id"] == assignment["worker_id"]
    assert (
        await hub.db.call(lambda c: dict(c.execute("select * from integrator_control").fetchone()))
    )["last_finished_at"] == completed
    assert first["task_id"] != second["task_id"]


async def test_the_operator_changes_the_batch_rule_while_the_service_runs(batch_case):
    hub, run, _ = batch_case
    assert (await hub.backend_status())["integrator_batch"] == {
        "delay_minutes": 13, "threshold": 5, "source": {"delay_minutes": "config", "threshold": "config"}}
    await ready(hub, run, "First accepted contribution")
    await ready(hub, run, "Second accepted contribution")
    assert await choose(hub, run) is None  # Two of five, and no time has passed.
    await command(hub, "runtime.set_integrator_batch", {"threshold": 2})
    assert await choose(hub, run) is not None  # The next admission pass applies it.
    rule = await hub.db.read(hub._batch_rule)
    assert (rule["threshold"], rule["delay_minutes"]) == (2, 13)  # A field left out keeps its value.


async def test_a_longer_wait_holds_the_next_batch_back(batch_case):
    hub, run, now = batch_case
    await command(hub, "runtime.set_integrator_batch", {"delay_minutes": 30, "threshold": 5})
    await ready(hub, run, "Waiting contribution")
    now[0] += BATCH_DELAY_MS
    assert await choose(hub, run) is None  # Thirteen minutes no longer start it.
    now[0] += 30 * 60_000 - BATCH_DELAY_MS
    assert await choose(hub, run) is not None
    await command(hub, "runtime.set_integrator_batch", {"delay_minutes": None, "threshold": None})
    assert (await hub.db.read(hub._batch_rule))["source"] == {"delay_minutes": "config", "threshold": "config"}


@pytest.mark.parametrize("payload", [{"delay_minutes": -1}, {"delay_minutes": 10081}, {"delay_minutes": 2.5},
                                     {"threshold": 0}, {"threshold": 1001}, {"threshold": True}])
async def test_an_invalid_batch_rule_is_refused(batch_case, payload):
    hub, _, _ = batch_case
    with pytest.raises(HubError) as refused:
        await command(hub, "runtime.set_integrator_batch", payload)
    assert refused.value.code == "INVALID_INPUT"


async def test_five_starts_one_fixed_batch_and_new_arrivals_wait(batch_case):
    hub, run, _ = batch_case
    reviewers = [await ready(hub, run, f"Contribution {i}") for i in range(5)]
    first = await choose(hub, run)
    assert first is not None
    batch = await hub.db.call(lambda c: hub._batch(c))
    assert len(batch["items"]) == 5
    assert await choose(hub, run) is None
    await ready(hub, run, "Later arrival")
    assert len((await hub.db.call(lambda c: hub._batch(c)))["items"]) == 5
    assert len(await hub.db.call(hub._batch_eligible)) == 1
    assert await choose(hub, run) is None
    assert (
        await hub.db.call(
            lambda c: c.execute("select count(*) from workers where kind='integrator'").fetchone()[
                0
            ]
        )
        == 1
    )
    with pytest.raises(HubError):
        await hub.db.call(
            lambda c: hub._claim(
                c, reviewers[0]["task_id"], 1, reviewers[0]["worker_id"], run["id"]
            ),
            write=True,
        )


async def test_a_due_batch_is_admitted_before_a_waiting_message(batch_case):
    hub, run, now = batch_case
    reviewer = await ready(hub, run, "Accepted contribution")
    sender = await claim(hub, await publish(hub, "Question about the contribution"), run)
    await command(hub, "talk.send", {"recipient_id": reviewer["worker_id"], "message_id": "question",
                                     "text": "Which source paragraph fixes this normalization?"}, worker_actor(sender))
    assert (await hub.worker(reviewer["worker_id"]))["pending_wake"]
    now[0] += BATCH_DELAY_MS
    first = await hub.next_assignment(run["id"])
    assert (await hub.worker(first["worker_id"]))["kind"] == "integrator"
    second = await hub.next_assignment(run["id"])
    assert second["worker_id"] == reviewer["worker_id"]
    assert second["execution_mode"] == "message_only"


async def test_a_validated_review_is_admitted_before_a_waiting_message(batch_case):
    hub, run, _ = batch_case
    reviewer = await ready(hub, run, "Accepted contribution")
    sender = await claim(hub, await publish(hub, "Question about the contribution"), run)
    await command(hub, "talk.send", {"recipient_id": reviewer["worker_id"], "message_id": "question",
                                     "text": "Which source paragraph fixes this normalization?"}, worker_actor(sender))
    author = await claim(hub, await publish(hub, "Contribution awaiting review"), run)
    candidate = await stage(hub, author)
    await machine(hub, candidate)
    first = await hub.next_assignment(run["id"])
    assert first["task_id"] == candidate["review_task_id"]
    assert (await hub.worker(first["worker_id"]))["kind"] == "review"
    second = await hub.next_assignment(run["id"])
    assert second["worker_id"] == reviewer["worker_id"]
    assert second["execution_mode"] == "message_only"


async def test_pause_and_individual_scope_do_not_create_batch(batch_case):
    hub, run, now = batch_case
    await ready(hub, run, "Accepted contribution")
    now[0] += BATCH_DELAY_MS
    await command(hub, "run.pause", {"run_id": run["id"]})
    assert await hub.next_assignment(run["id"]) is None
    assert (
        await hub.db.call(lambda c: hub._batch_assignment(c, run["id"], set()), write=True) is None
    )
    assert not await hub.db.call(
        lambda c: c.execute("select 1 from integration_batches").fetchone()
    )


async def test_a_resumed_integrator_receives_the_next_batch_while_the_run_is_paused(batch_case):
    hub, run, now = batch_case
    await ready(hub, run, "Earlier contribution")
    now[0] += BATCH_DELAY_MS
    first = await choose(hub, run)
    batch = await hub.db.call(lambda c: hub._batch(c))
    await command(hub, "integrator.defer", {"candidate_id": batch["items"][0]["candidate_id"],
                                            "reason": "Waiting for a later batch."}, worker_actor(first))
    await command(hub, "integrator.finish_empty", {}, worker_actor(first))
    await quiet(hub, first)
    reviewer = await ready(hub, run, "Next contribution")
    await command(hub, "run.pause", {"run_id": run["id"]})
    now[0] += BATCH_DELAY_MS
    open_batches = "SELECT count(*) FROM integration_batches WHERE ended_at IS NULL"
    # Another person resumed alone still creates no new work.
    await command(hub, "worker.resume", {"worker_id": reviewer["worker_id"], "run_id": run["id"]})
    await hub.next_assignment(run["id"])
    assert await hub.db.call(lambda c: c.execute(open_batches).fetchone()[0]) == 0
    # A resumed Integrator receives the next batch under the usual rules.
    await command(hub, "worker.resume", {"worker_id": first["worker_id"], "run_id": run["id"]})
    assignment = await hub.next_assignment(run["id"])
    assert assignment["worker_id"] == first["worker_id"]
    batch = await hub.db.call(lambda c: hub._batch(c))
    assert batch["worker_id"] == first["worker_id"] and len(batch["items"]) == 1


async def test_withdrawn_item_must_be_removed_before_batch_freeze(batch_case):
    hub, run, now = batch_case
    await ready(hub, run, "Withdrawable contribution")
    now[0] += BATCH_DELAY_MS
    assignment = await choose(hub, run)
    batch = await hub.db.call(lambda c: hub._batch(c))
    candidate = batch["items"][0]["candidate_id"]
    await command(
        hub,
        "artifact.withdraw",
        {"artifact_id": candidate, "reason": "The original route was abandoned."},
    )
    with pytest.raises(HubError) as caught:
        await command(hub, "integrator.prepare.reserve", {}, worker_actor(assignment))
    assert caught.value.code == "BATCH_INPUT_CHANGED"
    await command(
        hub,
        "integrator.defer",
        {"candidate_id": candidate, "reason": "Withdrawn by the maintainer."},
        worker_actor(assignment),
    )
    assert (await hub.artifact(candidate))["status"] == "withdrawn"


async def test_batch_publishes_one_commit_and_retains_each_independent_review(batch_case, monkeypatch):
    hub, run, now = batch_case
    for i in range(2):
        await ready(hub, run, f"Accepted note {i}")
    now[0] += BATCH_DELAY_MS
    assignment = await choose(hub, run)
    actor = worker_actor(assignment)
    serial = [0]

    async def call(name, payload, *, actor=actor, suffix=""):
        serial[0] += 1
        return await command(hub, name, payload, actor)

    batch = await hub.prepare_integration_batch(actor, call)
    from pathlib import Path

    frozen = hub._repository().finalize(Path(batch["workspace"]), hub.blobs)
    runtime = Actor(
        role=Role.RUNNER,
        worker_id=actor.worker_id,
        attempt_id=actor.attempt_id,
        fencing_token=actor.fencing_token,
    )
    batch = await command(
        hub,
        "integrator.freeze.store",
        {"workspace_id": batch["workspace_id"], "frozen": frozen},
        runtime,
    )
    reports = {
        item["candidate_id"]: {
            "verdict": "accepted",
            "policy": "general.peer@1",
            "qualifications": [],
            "details": {
                "integration_snapshot": frozen["snapshot"],
                "fresh_integrated_validation": True,
            },
        }
        for item in batch["items"]
    }
    await command(
        hub, "integrator.validation", {"round_id": batch["round_id"], "reports": reports}, runtime
    )
    before = hub._repository().head()
    await command(
        hub,
        "integrator.publish",
        {"round_id": batch["round_id"]},
        actor,
    )
    assert hub._repository().head() == before
    def no_tree_copy(*args, **kwargs):
        raise AssertionError("Publication must reuse the prepared prior snapshot")
    from formal_geyser.coordination.repository import RepositoryStore
    monkeypatch.setattr(RepositoryStore, "snapshot", no_tree_copy)
    await quiet(hub, assignment)
    assert hub._repository().head() == frozen["commit"]
    publications = await hub.db.call(
        lambda c: [dict(r) for r in c.execute("select * from repository_publications")]
    )
    assert len(publications) == 2
    assert {p["commit_sha"] for p in publications} == {frozen["commit"]}
    assert {p["reviewer_id"] for p in publications} == {x["reviewer_id"] for x in batch["items"]}
    for publication in publications:
        assert json.loads(publication["review_json"])["integrator_id"] == assignment["worker_id"]
        assert (await hub.artifact(publication["candidate_id"]))["status"] == "accepted"
    assert (await hub.worker(assignment["worker_id"]))["work_intention"] == "waiting"
    # The ended batch lets its checkout go at once; its frozen snapshot and commit remain.
    assert hub.integration_workspaces_due()
    report = await hub.reclaim_integration_workspaces()
    assert [item["state"] for item in report["workspaces"]] == ["reclaimed"]
    assert not Path(batch["workspace"]).exists() and report["workspaces"][0]["commit"] == frozen["commit"]


async def test_public_notes_batch_alone_and_never_mix_with_waiting_mathematics(batch_case):
    hub, run, now = batch_case
    mathematics = await ready(hub, run, "Waiting mathematical contribution")
    note = await ready(hub, run, "Waiting public note")
    note_id = note["task"]["collaboration"]["review"]["candidate_id"]
    # Synthetic kind for the admission rule only; validation still inspects the real diff.
    await hub.db.call(lambda c: c.execute("UPDATE artifacts SET kind='note.wiki' WHERE id=?", (note_id,)), write=True)
    assignment = await choose(hub, run)  # No five-item threshold or 13-minute timer for notes.
    assert assignment is not None
    batch = await hub.db.call(lambda c: hub._batch(c))
    assert batch["content_kind"] == "public-notes"
    assert [item["candidate_id"] for item in batch["items"]] == [note_id]
    waiting = await hub.db.call(hub._batch_eligible)
    assert [item["review_task_id"] for item in waiting] == [mathematics["task_id"]]
    assert await choose(hub, run) is None  # One open batch at a time.


async def test_a_batch_of_public_notes_does_not_restart_the_mathematics_delay(batch_case):
    hub, run, now = batch_case
    mathematics = await ready(hub, run, "Waiting mathematical contribution")
    note = await ready(hub, run, "Waiting public note")
    note_id = note["task"]["collaboration"]["review"]["candidate_id"]
    await hub.db.call(lambda c: c.execute("UPDATE artifacts SET kind='note.wiki' WHERE id=?", (note_id,)), write=True)
    now[0] += BATCH_DELAY_MS - 1
    assignment = await choose(hub, run)  # The note goes at once.
    await command(hub, "integrator.defer", {"candidate_id": note_id, "reason": "Waiting for its source page."},
                  worker_actor(assignment))
    await command(hub, "integrator.finish_empty", {}, worker_actor(assignment))
    await quiet(hub, assignment)
    assert await choose(hub, run) is None
    now[0] += 1  # Thirteen minutes since the mathematics waited, not since the notes' batch ended.
    assert await choose(hub, run) is not None
    batch = await hub.db.call(lambda c: hub._batch(c))
    assert [item["review_task_id"] for item in batch["items"]] == [mathematics["task_id"]]


async def test_admitting_a_batch_writes_its_task_files_off_the_coordination_thread(batch_case, monkeypatch):
    hub, run, now = batch_case
    await ready(hub, run, "Accepted contribution")
    now[0] += BATCH_DELAY_MS
    threads = []
    original = NativeContentStore.task_files

    def observed(self, *args, **kwargs):
        threads.append(threading.current_thread().name)
        return original(self, *args, **kwargs)
    monkeypatch.setattr(NativeContentStore, "task_files", observed)
    assignment = await hub.next_assignment(run["id"])
    assert (await hub.worker(assignment["worker_id"]))["kind"] == "integrator"
    assert threads and not any(name.startswith("taskhub-db") for name in threads)
    task = await hub.task(assignment["task_id"])
    assert (Path(task["files"]["root"]) / "task.json").is_file()
    assert not await hub.db.read(lambda c: c.execute("SELECT 1 FROM task_tree_intents").fetchone())


async def test_a_batch_view_checks_its_task_files_before_the_coordination_call(batch_case, monkeypatch):
    hub, run, now = batch_case
    reviewer = await ready(hub, run, "Accepted contribution")
    now[0] += BATCH_DELAY_MS
    assignment = await choose(hub, run)
    candidate = (await hub.db.call(lambda c: hub._batch(c)))["items"][0]["candidate_id"]
    for task_id in (assignment["task_id"], (await hub.artifact(candidate))["task_id"], reviewer["task_id"]):
        settle_tree(Path((await hub.task(task_id))["files"]["root"]).parents[2])
    trees = []
    original = NativeContentStore._verify_tree
    monkeypatch.setattr(NativeContentStore, "_verify_tree", staticmethod(
        lambda folder, refs: trees.append(threading.current_thread().name) or original(folder, refs)))
    entries = await hub.batch_contributions(worker_actor(assignment))
    assert [entry["artifact"]["id"] for entry in entries] == [candidate]
    # Each task revision was checked before the coordination call, which reused the checks.
    assert trees and not any(name.startswith("taskhub-db") for name in trees)


async def test_an_acceptance_keeps_the_reviewers_integration_notes(batch_case):
    hub, run, _ = batch_case
    reviewer = await reviewing(hub, run, "Noted contribution")
    actor = worker_actor(reviewer)
    for notes in ({}, {"placement": " "}, {"layout": "Elsewhere."},
                  {"overlaps": [{"existing": "Langlands.SmoothRep", "relation": "same", "note": "Equal."}]},
                  {"overlaps": [{"existing": "Langlands.SmoothRep", "relation": "duplicate"}]}):
        with pytest.raises(HubError) as caught:
            await command(hub, "review.accept", {**basis(reviewer), "integration_notes": notes}, actor)
        assert caught.value.code == "INVALID_INPUT"
    # A rejected author receives suggestions; notes accompany an acceptance only.
    with pytest.raises(HubError, match="accompany an acceptance"):
        await command(hub, "review.reject", {**basis(reviewer), "integration_notes": NOTES,
                                             "suggestions": ["Reuse the existing category."]}, actor)
    await command(hub, "review.accept", {**basis(reviewer), "integration_notes": NOTES}, actor)
    acceptance = await hub.db.call(lambda c: hub._review_acceptance(c, reviewer["task_id"]))
    assert acceptance["report"]["integration_notes"] == NOTES


async def test_a_deferral_lets_its_checkout_go_to_be_recorded_and_reclaimed(batch_case):
    hub, run, now = batch_case
    await ready(hub, run, "Contribution waiting for a source page")
    now[0] += BATCH_DELAY_MS
    assignment = await choose(hub, run)
    actor = worker_actor(assignment)

    async def call(name, payload, *, actor=actor, suffix=""):
        return await command(hub, name, payload, actor)

    prepared = await hub.prepare_integration_batch(actor, call)
    location = Path(prepared["workspace"])
    (location / "Integrator-note.md").write_text("A link fixed while preparing.", encoding="utf-8")
    report = await hub.reclaim_integration_workspaces()  # The open batch uses it.
    assert location.is_dir() and report["workspaces"] == [] and not hub.integration_workspaces_due()
    await command(hub, "integrator.defer", {"candidate_id": prepared["items"][0]["candidate_id"],
                                            "reason": "Waiting for its source page."}, actor)
    assert hub.integration_workspaces_due()
    report = await hub.reclaim_integration_workspaces()
    [item] = report["workspaces"]
    assert item["state"] == "reclaimed_unfrozen" and not location.exists()
    assert contents(hub.blobs, item["snapshot"])["Integrator-note.md"] == b"A link fixed while preparing."
    events = [e for e in await hub.events(limit=1000) if e["kind"] == "integration.workspace_retention"]
    assert events[-1]["payload"]["workspaces"][0]["snapshot"] == item["snapshot"]
    assert (await hub.reclaim_integration_workspaces())["workspaces"] == []


async def test_a_checkout_no_batch_or_round_recorded_is_reclaimed(batch_case):
    hub, run, now = batch_case
    await ready(hub, run, "Contribution")
    now[0] += BATCH_DELAY_MS
    assignment = await choose(hub, run)
    repository = hub._repository()
    # As a preparation let go before its round was recorded left it (Linux pilot, 2026-11-07).
    orphan = hub.workspace(assignment["worker_id"]) / ".taskhub-integration" / uuid4().hex
    repository.prepare(hub.blobs, manifest(hub.blobs, {"Orphan.md": b"left behind"}), repository.head(),
                       orphan, ["Orphan.md"])
    report = await hub.reclaim_integration_workspaces()
    assert [item["state"] for item in report["workspaces"]] == ["reclaimed_unfrozen"]
    assert not orphan.exists()
    assert contents(hub.blobs, report["workspaces"][0]["snapshot"])["Orphan.md"] == b"left behind"
