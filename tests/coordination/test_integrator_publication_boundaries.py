"""Current integrator gates, complete-tree publication and recovery, without models."""

import json
from pathlib import Path
from uuid import uuid4

import pytest

from formal_geyser.coordination.batch_integration import BATCH_DELAY_MS
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Role
from formal_geyser.coordination.runtime import (
    ScriptedHarness,
    ScriptedTurn,
    WorkerRunner,
)
from tests.coordination.lifecycle_helpers import select_review
from tests.coordination.review_helpers import accept
from tests.coordination.test_archive_publication import archive_case as archive_case
from tests.coordination.test_collaborative_core import (
    claim,
    command,
    machine,
    publish,
    quiet,
    stage,
    wait_for,
    worker_actor,
)
from tests.coordination.test_integrator_batches import (
    NOTES,
    choose,
    ready,
)
from tests.coordination.test_integrator_batches import (
    batch_case as batch_case,
)


async def prepare(hub, assignment):
    actor = worker_actor(assignment)

    async def call(name, payload, *, actor=actor, suffix=""):
        return await command(hub, name, payload, actor)

    return await hub.prepare_integration_batch(actor, call)


def runner(assignment):
    return Actor(
        role=Role.RUNNER,
        worker_id=assignment["worker_id"],
        attempt_id=assignment["id"],
        fencing_token=assignment["fencing_token"],
    )


async def freeze(hub, assignment):
    batch = await hub.db.call(lambda c: hub._batch(c))
    frozen = hub._repository().finalize(Path(batch["workspace"]), hub.blobs)
    return await command(
        hub,
        "integrator.freeze.store",
        {"workspace_id": batch["workspace_id"], "frozen": frozen},
        runner(assignment),
    )


async def validate(hub, assignment, *, verdict="accepted", qualifications=None):
    batch = await freeze(hub, assignment)
    reports = {
        item["candidate_id"]: {
            "verdict": verdict,
            "policy": "general.peer@1",
            "qualifications": qualifications or [],
            "details": {
                "integration_snapshot": batch["round"]["snapshot_sha"],
                "fresh_integrated_validation": True,
                "scope": "Synthetic protocol evidence, no mathematical assertion.",
            },
        }
        for item in batch["items"]
        if item["state"] == "included"
    }
    return await command(
        hub,
        "integrator.validation",
        {"round_id": batch["round_id"], "reports": reports},
        runner(assignment),
    )


async def submit(hub, assignment):
    batch = await hub.db.call(lambda c: hub._batch(c))
    return await command(
        hub,
        "integrator.publish",
        {"round_id": batch["round_id"]},
        worker_actor(assignment),
    )


async def test_wait_for_artifact_and_task_completion_require_actual_publication(batch_case):
    hub, run, now = batch_case
    host = await publish(hub)
    author = await claim(hub, host, run)
    candidate = await stage(hub, author)
    await machine(hub, candidate)
    await wait_for(hub, author, candidate["artifact_id"])
    await quiet(hub, author)
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    await accept(hub, reviewer)
    await hub.ordinary_turn_completed(reviewer["worker_id"], reviewer["id"])
    requirement = {
        "key": "published",
        "artifact_id": candidate["artifact_id"],
        "policy": "general.peer@1",
    }

    async def still_waiting():
        assert not await hub.db.call(lambda c: hub._requirement(c, requirement))
        with pytest.raises(HubError, match="still needs independent acceptance and publication"):
            await hub.db.call(lambda c: hub._artifact_gate(c, host["id"]))
        waits = await hub.db.call(
            lambda c: [
                dict(x)
                for x in c.execute(
                    "SELECT * FROM continuations WHERE preferred_worker=?", (author["worker_id"],)
                )
            ]
        )
        assert waits[-1]["state"] == "waiting"

    await still_waiting()
    now[0] += BATCH_DELAY_MS
    integrator = await choose(hub, run)
    await prepare(hub, integrator)
    await validate(hub, integrator)
    await still_waiting()
    await submit(hub, integrator)
    await still_waiting()
    await quiet(hub, integrator)
    assert await hub.db.call(lambda c: hub._requirement(c, requirement))
    await hub.db.call(lambda c: hub._artifact_gate(c, host["id"]))
    assert (await hub.task(host["id"]))["state"] != "done"
    assert not await hub.db.call(
        lambda c: hub._requirement(
            c,
            {
                "key": "task_done",
                "task_id": host["id"],
                "task_revision": 1,
                "artifact_id": candidate["artifact_id"],
                "policy": "general.peer@1",
            },
        )
    )


@pytest.mark.parametrize(
    "verdict,qualifications",
    [
        ("rejected", []),
        ("unavailable", []),
        ("needs_operator", []),
        ("accepted", ["Unresolved evidence"]),
    ],
)
async def test_no_failed_or_qualified_batch_can_publish(batch_case, verdict, qualifications):
    hub, run, now = batch_case
    await ready(hub, run, "Independent contribution")
    now[0] += BATCH_DELAY_MS
    assignment = await choose(hub, run)
    await prepare(hub, assignment)
    await validate(hub, assignment, verdict=verdict, qualifications=qualifications)
    head = hub._repository().head()
    with pytest.raises(HubError) as failure:
        await submit(hub, assignment)
    assert failure.value.code == "MERGE_VALIDATION_REQUIRED"
    assert hub._repository().head() == head


async def test_deferral_rebuilds_without_isolated_candidate_bytes(batch_case):
    hub, run, now = batch_case
    a, b = await ready(hub, run, "First contribution"), await ready(hub, run, "Second contribution")
    now[0] += BATCH_DELAY_MS
    assignment = await choose(hub, run)
    original = await prepare(hub, assignment)
    aid = a["task"]["collaboration"]["review"]["candidate_id"]
    bid = b["task"]["collaboration"]["review"]["candidate_id"]
    apaths = (await hub.artifact(aid))["repository"]["paths"]
    await command(
        hub,
        "integrator.defer",
        {"candidate_id": aid, "reason": "Missing exact source clarification."},
        worker_actor(assignment),
    )
    rebuilt = await prepare(hub, assignment)
    assert rebuilt["workspace"] != original["workspace"]
    # The deferral let the first checkout go: the rebuild's preparation recorded and removed it.
    assert not Path(original["workspace"]).exists()
    retention = [e["payload"] for e in await hub.events(limit=1000) if e["kind"] == "integration.workspace_retention"]
    assert [(w["workspace"], w["state"]) for w in retention[-1]["workspaces"]] == [
        (original["workspace"], "reclaimed_unfrozen")]
    assert all(not (Path(rebuilt["workspace"]) / p).exists() for p in apaths)
    await validate(hub, assignment)
    await submit(hub, assignment)
    await quiet(hub, assignment)
    assert (await hub.artifact(aid))["status"] == "staged"
    assert (await hub.artifact(bid))["status"] == "accepted"
    assert await hub.db.call(hub._batch_eligible) == []


@pytest.mark.parametrize("phase", ["before_git", "after_git", "after_archive"])
async def test_durable_publication_recovers_once_without_another_model_turn(
    archive_case, monkeypatch, phase
):
    case = archive_case
    hub = case.hub
    await accept(hub, case.reviewer)
    await hub.ordinary_turn_completed(case.reviewer["worker_id"], case.reviewer["id"])
    clock = hub.clock()
    hub.clock = lambda: clock + BATCH_DELAY_MS
    assignment = await choose(hub, case.run)
    await prepare(hub, assignment)
    checked = await validate(hub, assignment)
    await submit(hub, assignment)
    if phase != "before_git":
        original = hub._archive_publication if phase == "after_git" else hub._event

        def fail(*args, **kwargs):
            if phase == "after_git" or args[1] == "repository.published":
                raise OSError("Simulated process loss after Git advanced")
            return original(*args, **kwargs)

        name = "_archive_publication" if phase == "after_git" else "_event"
        monkeypatch.setattr(hub, name, fail)
        with pytest.raises(OSError, match="process loss"):
            await quiet(hub, assignment)
        monkeypatch.setattr(hub, name, original)
        assert hub._repository().head() == checked["round"]["commit_sha"]
    # Recovery boundary already independently stopped owned processes.
    recovered = await command(
        hub,
        "runtime.abandon",
        {
            "worker_id": assignment["worker_id"],
            "quiescent": True,
            "reason": "Verified process shutdown after the publication response was lost.",
        },
        Actor(role=Role.RUNNER),
    )
    assert recovered["directive"] == "batch_finished"
    publications = (await hub.repository_status())["publications"]
    assert len(publications) == 1
    artifact = await hub.artifact(case.candidate["artifact_id"])
    assert artifact["status"] == "accepted"
    assert (
        Path(artifact["content_file"]["path"]).parent / "files/Fixture.lean"
    ).read_bytes() == case.submitted
    again = await command(
        hub,
        "runtime.abandon",
        {"worker_id": assignment["worker_id"], "quiescent": True},
        Actor(role=Role.RUNNER),
    )
    assert again["directive"] == "already_quiet"


async def test_withdrawal_after_submit_never_becomes_acceptance_during_recovery(batch_case):
    hub, run, now = batch_case
    await ready(hub, run, "Withdraw before publication")
    now[0] += BATCH_DELAY_MS
    assignment = await choose(hub, run)
    await prepare(hub, assignment)
    checked = await validate(hub, assignment)
    await submit(hub, assignment)
    aid = checked["items"][0]["candidate_id"]
    await command(
        hub, "artifact.withdraw", {"artifact_id": aid, "reason": "Withdraw this abandoned route."}
    )
    result = await command(
        hub,
        "runtime.abandon",
        {"worker_id": assignment["worker_id"], "quiescent": True},
        Actor(role=Role.RUNNER),
    )
    assert result["directive"] == "recovery_required"
    assert result.get("reason") != "repository_reconciliation_required"
    assert hub._repository().head() == checked["target_head"]
    assert (await hub.artifact(aid))["status"] == "withdrawn"
    assert (await hub.db.call(lambda c: hub._batch(c)))["state"] == "collecting"


@pytest.mark.parametrize("notes", [None, NOTES])
async def test_runtime_gateway_runs_dedicated_integrator_and_publishes_after_drain(batch_case, notes):
    hub, run, now = batch_case
    await ready(hub, run, "Runtime contribution", integration_notes=notes)
    now[0] += BATCH_DELAY_MS

    class IntegratorHarness(ScriptedHarness):
        async def run_turn(self, prompt, **callbacks):
            async def call(operation, arguments=None):
                result = await callbacks["on_tool"](
                    "taskhub_call",
                    {"operation": operation, "arguments": arguments or {}},
                    uuid4().hex,
                )
                if getattr(result, "after_send", None):
                    await result.after_send()
                assert "error" not in result, result
                return result

            assert "Integrator" in prompt
            # The batch starts in a new conversation, so nothing is "continued".
            assert json.loads(prompt)["instruction"].startswith("Integrate the assigned fixed batch: inspect")
            context = await call("batch_status")
            assert len(context["contributions"]) == 1
            assert context["contributions"][0]["independent_review"]
            # The reviewer's integration notes are listed with the contribution.
            assert context["contributions"][0].get("integration_notes") == notes
            await call("batch_prepare")
            validated = await call("batch_validate")
            before = hub._repository().head()
            await call("batch_publish", {"round_id": validated["round_id"]})
            assert hub._repository().head() == before
            return await super().run_turn(prompt, **callbacks)

    result = await WorkerRunner(hub, lambda: IntegratorHarness([ScriptedTurn([])])).run_once(
        run["id"]
    )
    assert result["directive"] == "batch_finished"
    assert len((await hub.repository_status())["publications"]) == 1


async def test_a_batch_begins_in_a_new_conversation_and_continues_in_it(batch_case):
    hub, run, now = batch_case
    await ready(hub, run, "Two-turn contribution")
    now[0] += BATCH_DELAY_MS
    instructions = []

    class Recording(ScriptedHarness):
        async def run_turn(self, prompt, **callbacks):
            instructions.append(json.loads(prompt).get("instruction", ""))
            return await super().run_turn(prompt, **callbacks)

    harness = Recording([ScriptedTurn([]), ScriptedTurn([])])
    first = await WorkerRunner(hub, lambda: harness).run_once(run["id"])
    second = await WorkerRunner(hub, lambda: harness).run_once(run["id"])
    assert first["worker_id"] == second["worker_id"]
    assert instructions[0].startswith("Integrate the assigned fixed batch: inspect")
    # The next turn continues the same conversation, from what it was already given.
    assert instructions[1].startswith("Continue your current task.")


async def test_one_batch_archives_each_mathematical_contribution_as_submitted(batch_case):
    hub, run, now = batch_case
    candidates = []
    for module in ("First", "Second"):
        author = await claim(hub, await publish(hub, module), run)
        lean, md = b"-- Archive protocol fixture.\n", b"Paired archive protocol fixture.\n"
        root = hub.workspace(author["worker_id"])
        (root / (module + ".lean")).write_bytes(lean)
        (root / (module + ".md")).write_bytes(md)
        candidate = await command(
            hub,
            "artifact.stage",
            {
                "kind": "lean.statement",
                "repository_paths": [module + ".lean", module + ".md"],
                "data": {
                    "version": 1,
                    "bundle": {
                        "lean_path": module + ".lean",
                        "markdown_path": module + ".md",
                        "lean_sha256": hub.blobs.put(lean),
                        "markdown_sha256": hub.blobs.put(md),
                    },
                },
            },
            worker_actor(author),
        )
        await machine(hub, candidate)
        reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
        await accept(hub, reviewer)
        await hub.ordinary_turn_completed(reviewer["worker_id"], reviewer["id"])
        candidates.append((module, candidate["artifact_id"]))
    now[0] += BATCH_DELAY_MS
    assignment = await choose(hub, run)
    await prepare(hub, assignment)
    await validate(hub, assignment)
    await submit(hub, assignment)
    await quiet(hub, assignment)
    await hub.archive_moves_settled()
    for module, aid in candidates:
        artifact = await hub.artifact(aid)
        archive = Path(artifact["content_file"]["path"]).parent
        receipt = json.loads((archive / "publication.json").read_text())
        assert artifact["status"] == "accepted" and "source_binding" not in receipt
        assert (archive / "files" / (module + ".lean")).read_bytes() == b"-- Archive protocol fixture.\n"
        assert not (archive / "final").exists()


async def test_publication_records_the_original_review_without_reconfirmation(batch_case):
    hub, run, now = batch_case
    await ready(hub, run, "Keep original review")
    now[0] += BATCH_DELAY_MS
    assignment = await choose(hub, run)
    await prepare(hub, assignment)
    checked = await validate(hub, assignment)
    item = checked["items"][0]
    accepted = await hub.db.call(lambda c: hub._review_acceptance(c, item["review_task_id"]))
    await submit(hub, assignment)
    await quiet(hub, assignment)
    recorded = await hub.db.call(lambda c: c.execute(
        "SELECT review_json FROM repository_publications WHERE candidate_id=?",
        (item["candidate_id"],)).fetchone()[0])
    report = json.loads(recorded)
    assert {key: report[key] for key in accepted["report"]} == accepted["report"]
    assert "final_semantic_checks" not in report
    columns = await hub.db.call(lambda c: [row[1] for row in c.execute("PRAGMA table_info(integration_batches)")])
    assert "publish_json" not in columns


async def test_quiet_review_handoff_preserves_original_thread_and_checkpoint(batch_case):
    hub, run, _ = batch_case
    review = await ready(hub, run, "Retained review")
    before = await hub.worker(review["worker_id"])
    await hub.db.call(hub._enable_integrator, write=True)
    after = await hub.worker(review["worker_id"])
    assert after["thread_id"] == before["thread_id"]
    assert after["state"] == "suspended" and after["work_intention"] == "waiting"
    assert (await hub.task(review["task_id"]))["collaboration"]["review"]["state"] == "integrating"


async def test_integrator_can_read_original_pending_math_pair_without_authority_to_review(
    archive_case,
):
    from formal_geyser.coordination.runtime.files import ResourceRoots
    from formal_geyser.coordination.runtime.native_files import native_file

    case = archive_case
    await accept(case.hub, case.reviewer)
    await case.hub.ordinary_turn_completed(case.reviewer["worker_id"], case.reviewer["id"])
    now = case.hub.clock()
    case.hub.clock = lambda: now + BATCH_DELAY_MS
    assignment = await choose(case.hub, case.run)
    actor = worker_actor(assignment)
    artifact = await case.hub.native_record(case.candidate["artifact_id"], actor)
    path = str(Path(artifact["content_file"]["path"]).parent / "files/Fixture.lean")
    reader, relative = await native_file(case.hub, actor, ResourceRoots({}), path)
    assert reader.read(relative)["text"] == case.submitted.decode()
    with pytest.raises(HubError) as denied:
        await command(case.hub, "review.accept", {}, actor)
    assert denied.value.code == "INTEGRATOR_SCOPE"



async def test_large_batch_status_is_a_readable_success_not_an_error(batch_case):
    hub, run, now = batch_case
    for n in range(5):
        await ready(hub, run, f"Contribution {n}")
    assignment = await choose(hub, run)
    raw = await command(hub, "integrator.status", {}, worker_actor(assignment))
    assert "error" not in raw and "last_error" not in raw
    runtime = WorkerRunner(hub, lambda: ScriptedHarness())
    bounded = await runtime._bound_tool_result(raw, 512, worker_id=assignment["worker_id"])
    assert "error" not in bounded
    assert bounded["requires_read"]
    material = json.loads((hub.workspace(assignment["worker_id"]) / bounded["material_path"]).read_text())
    assert len(material["items"]) == 5
