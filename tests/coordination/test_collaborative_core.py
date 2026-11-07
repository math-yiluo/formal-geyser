from __future__ import annotations

import json
from uuid import uuid4

import pytest

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import (
    Actor,
    Command,
    Limits,
    ProjectConfig,
    Role,
)
from tests.coordination.lifecycle_helpers import finish_ordinary, open_collaboration, select_review
from tests.coordination.publication_helpers import approve, file_payload, new_hub

from .wait_helpers import legacy_wait

OP = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)
VALIDATOR = Actor(role=Role.VALIDATOR)


async def command(hub, name, payload=None, actor=OP, key=None):
    return await hub.execute(Command(name=name, payload=payload or {}, key=key or uuid4().hex), actor)


def contract(title="Collaborative task", **values):
    return {"title": title, "objective": "Produce independently reviewed software evidence.", **values}


async def publish(hub, title="Collaborative task", actor=OP, **values):
    return await command(hub, "task.publish", {"contract": contract(title, **values)}, actor)


async def start(hub):
    return await command(hub, "run.start")


def worker_actor(attempt):
    return Actor(role=Role.WORKER, worker_id=attempt["worker_id"], attempt_id=attempt["id"], fencing_token=attempt["fencing_token"])


async def claim(hub, task, run, worker_id=None):
    if worker_id is None and task["contract"]["kind"] == "review":
        return await select_review(hub, task["id"], run["id"])
    state = (await hub.task(task["id"]))["collaboration"].get("recruitment", {})
    if worker_id is None and state.get("initial_worker_id"):
        await open_collaboration(hub, task["id"])
    if worker_id is None:
        worker_id = (await command(hub, "worker.register"))["id"]
    return await command(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"], "run_id": run["id"], "worker_id": worker_id,
                                            "plan": "Independently check the remaining conditions."})


async def quiet(hub, attempt):
    return await command(hub, "runtime.quiescent", {"worker_id": attempt["worker_id"], "snapshot": hub.blobs.snapshot(hub.workspace(attempt["worker_id"]))}, RUNNER)


async def stage(hub, attempt, *, coauthors=None):
    payload = file_payload(hub, attempt, data={"result": uuid4().hex},
                           coauthors=coauthors or [])
    return await command(hub, "artifact.stage", payload, worker_actor(attempt))


async def machine(hub, candidate, policy=None):
    return await command(hub, "artifact.accept", {"artifact_id": candidate["artifact_id"], "policy": policy or candidate["publication_policy"], "verdict": "accepted", "details": {"scope": "test domain evidence"}, "qualifications": []}, VALIDATOR)


async def review(hub, attempt, verdict="approve", **changes):
    binding = attempt["task"]["collaboration"]["review"]
    payload = {key: binding[key] for key in ("candidate_id", "candidate_hash", "task_revision", "input_refs", "policy")}
    payload.update(verdict=verdict, rationale="Inspected the candidate against its immutable task and input basis.", evidence=["Exact candidate material and task contract were compared."], qualifications=[])
    payload.update(changes)
    if payload["verdict"] == "approve" and not payload["qualifications"]:
        return await approve(hub, attempt, payload)
    if payload["verdict"] == "request_changes":
        return await command(hub, "review.reject", {**payload, "suggestions": ["Correct the unsupported result."]}, worker_actor(attempt))
    return await legacy_wait(hub, {
        "reason": "Resolve the explicit verification uncertainty before deciding the review.",
        "checkpoint": {"summary": json.dumps(payload), "next_action": "Resolve the missing evidence and retain this candidate binding."},
    }, worker_actor(attempt))


async def wait_for(hub, attempt, artifact_id):
    return await legacy_wait(hub, {
        "checkpoint": {"summary": "Candidate staged; awaiting independent publication.", "next_action": "Use peer feedback before continuing.", "input_refs": []},
        "predicate": {"requirements": [{"key": "candidate", "artifact_id": artifact_id, "policy": "general.peer@1"}]},
    }, worker_actor(attempt))


@pytest.fixture
async def hub(tmp_path):
    result = await new_hub(tmp_path, config=ProjectConfig(limits=Limits()))
    yield result
    await result.close()


async def test_first_admission_one_worker_then_each_review_gets_a_new_reviewer(hub):
    task, run = await publish(hub), await start(hub)
    author = await hub.next_assignment(run["id"])
    assert author["task_id"] == task["id"]
    assert await hub.next_assignment(run["id"]) is None
    candidate = await stage(hub, author)
    assert (await machine(hub, candidate))["status"] == "staged"
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    assert reviewer["worker_id"] != author["worker_id"]
    assert reviewer["task"]["contract"]["kind"] == "review"
    await review(hub, reviewer)
    assert (await hub.artifact(candidate["artifact_id"]))["status"] == "accepted"
    assert (await hub.task(task["id"]))["state"] == "claimed"
    second = await stage(hub, author)
    await machine(hub, second)
    again = await select_review(hub, second["review_task_id"], run["id"])
    assert again["worker_id"] != reviewer["worker_id"]
    assert (await hub.worker(reviewer["worker_id"]))["state"] == "retired"
    await review(hub, again)
    assert len([item for item in await hub.list_tasks() if item["contract"]["kind"] != "integration"]) == 3
    assert not any(item["task_id"] == again["task_id"] for item in await hub.list_artifacts())


async def test_two_attempts_keep_fences_and_independent_wait(hub):
    task, run = await publish(hub), await start(hub)
    a, b = await claim(hub, task, run), await claim(hub, task, run)
    assert b["fencing_token"] > a["fencing_token"]
    checkpoint = {"summary": "A still owns its lease.", "next_action": "Continue.", "input_refs": []}
    await command(hub, "task.checkpoint", {"checkpoint": checkpoint}, worker_actor(a))
    with pytest.raises(HubError, match="Worker already"):
        await claim(hub, task, run, a["worker_id"])
    dep = await command(hub, "artifact.register", {"kind": "software.input", "data": {"ready": False}})
    await wait_for(hub, a, dep["id"])
    await quiet(hub, a)
    state = await hub.task(task["id"])
    assert state["state"] == "claimed" and state["active_attempt"] == b["id"]
    assert [entry["id"] for entry in state["active_attempts"]] == [b["id"]]
    assert len(state["collaboration"]["members"]) == 2
    await command(hub, "task.checkpoint", {"checkpoint": checkpoint}, worker_actor(b))
    await command(hub, "artifact.accept", {"artifact_id": dep["id"], "policy": "general.peer@1", "verdict": "accepted"}, VALIDATOR)
    resumed = await hub.next_assignment(run["id"])
    assert resumed["worker_id"] == a["worker_id"]
    assert len((await hub.task(task["id"]))["active_attempts"]) == 2


async def test_exact_review_binding_author_exclusion_and_no_recursion(hub):
    task, run = await publish(hub), await start(hub)
    a = await claim(hub, task, run)
    coauthor = (await command(hub, "worker.register"))["id"]
    await open_collaboration(hub, task["id"])
    await command(hub, "task.join", {"task_id": task["id"], "revision": 1, "worker_id": coauthor, "plan": "Check the statement."})
    candidate = await stage(hub, a, coauthors=[coauthor])
    review_task = await hub.task(candidate["review_task_id"])
    for wid in [a["worker_id"], coauthor]:
        with pytest.raises(HubError) as error:
            await claim(hub, review_task, run, wid)
        assert error.value.code == "REVIEWER_INELIGIBLE"
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    assert reviewer["worker_id"] not in {a["worker_id"], coauthor}
    with pytest.raises(HubError) as error:
        await review(hub, reviewer, candidate_hash="0" * 64)
    assert error.value.code == "REVIEW_BINDING_MISMATCH"
    with pytest.raises(HubError) as error:
        await stage(hub, reviewer)
    assert error.value.code in {"REVIEW_PROTOCOL", "RECRUITMENT_CLOSED"}
    with pytest.raises(HubError) as error:
        await command(hub, "task.invitation.update", {"task_id": review_task["id"], "revision": 1, "reason": "Add a review team"}, worker_actor(reviewer))
    assert error.value.code == "REVIEW_PROTOCOL"


@pytest.mark.parametrize("verdict,qualifications", [("request_changes", []), ("uncertain", []), ("approve", ["Evidence scope remains uncertain"])])
async def test_unqualified_or_negative_reviews_never_accept(hub, verdict, qualifications):
    task, run = await publish(hub), await start(hub)
    a = await claim(hub, task, run)
    candidate = await stage(hub, a)
    await machine(hub, candidate)
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    await review(hub, reviewer, verdict, qualifications=qualifications)
    await quiet(hub, reviewer)
    assert (await hub.artifact(candidate["artifact_id"]))["status"] == ("rejected" if verdict == "request_changes" else "staged")
    assert (await hub.task(task["id"]))["state"] != "done"


async def test_private_and_staged_candidates_cannot_supply_public_dependencies(hub):
    task, run = await publish(hub), await start(hub)
    a = await claim(hub, task, run)
    private = await command(hub, "record.private", {"kind": "software.diagnostic", "data": {"check": "passed"}}, worker_actor(a))
    assert private["status"] == "private" and not private["public"]
    with pytest.raises(HubError) as error:
        await command(hub, "artifact.accept", {"artifact_id": private["id"], "policy": "general.peer@1", "verdict": "accepted"}, VALIDATOR)
    assert error.value.code == "PRIVATE_RECORD"
    candidate = await stage(hub, a)
    await machine(hub, candidate)
    for artifact in [private["id"], candidate["artifact_id"]]:
        child = await publish(hub, artifact, inputs=[artifact])
        with pytest.raises(HubError) as error:
            await claim(hub, child, run)
        assert error.value.code in {"INPUTS_CHANGED", "PRIVATE_RECORD"}
    assert not any(row["object_id"] == private["id"] for row in await hub.search("software.diagnostic"))


async def test_completion_drains_every_peer_and_waits_for_quiescence(hub):
    task, run = await publish(hub), await start(hub)
    a, b = await claim(hub, task, run), await claim(hub, task, run)
    candidate = await stage(hub, a, coauthors=[b["worker_id"]])
    await machine(hub, candidate)
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    await review(hub, reviewer)
    state = await hub.task(task["id"])
    assert state["state"] == "claimed"
    await finish_ordinary(hub, task["id"], worker_actor(a), artifact_ids=[candidate["artifact_id"]], feedback_worker=worker_actor(b))
    state = await hub.task(task["id"])
    assert state["state"] == "draining"
    for peer in [a, b]:
        worker = await hub.worker(peer["worker_id"])
        assert worker["state"] == "draining" and worker["runtime_stop_reason"] == "task_completion"
        with pytest.raises(HubError):
            await stage(hub, peer)
    await quiet(hub, a)
    assert (await hub.task(task["id"]))["active_attempt"] == b["id"]
    assert (await hub.task(task["id"]))["state"] == "draining"
    await quiet(hub, b)
    assert (await hub.task(task["id"]))["state"] == "done"
    assert await hub.next_assignment(run["id"]) is None


async def wait_on_declaration(hub, attempt, declaration_id):
    return await command(hub, "task.wait", {
        "checkpoint": {"summary": "Completion is declared; nothing else remains.", "next_action": "Act on its outcome.", "input_refs": []},
        "predicate": {"completion_declaration_id": declaration_id},
    }, worker_actor(attempt))


def continuation_state(hub, continuation_id):
    return hub.db.call(lambda c: c.execute("SELECT state FROM continuations WHERE id=?", (continuation_id,)).fetchone()["state"])


async def test_declarer_waits_for_the_feedback_and_the_feedback_giver_for_the_decision(hub):
    task, run = await publish(hub), await start(hub)
    a, b = await claim(hub, task, run), await claim(hub, task, run)
    candidate = await stage(hub, a, coauthors=[b["worker_id"]])
    await machine(hub, candidate)
    await review(hub, await select_review(hub, candidate["review_task_id"], run["id"]))
    conditions = await hub.db.call(lambda c: hub._conditions(c, hub._task(c, task["id"])))
    declaration = await command(hub, "task.complete.declare", {"task_id": task["id"], "revision": task["revision"], "evidence": {
        key: {"artifact_ids": [candidate["artifact_id"]], "explanation": "The exact accepted result satisfies: " + value}
        for key, value in conditions.items()}}, worker_actor(a))
    assert declaration["feedback_worker_id"] == b["worker_id"]
    declarer = await wait_on_declaration(hub, a, declaration["id"])
    await quiet(hub, a)
    assert await continuation_state(hub, declarer["continuation_id"]) == "waiting"
    await command(hub, "task.complete.feedback", {"declaration_id": declaration["id"], "conditions": [],
        "checkpoint": {"summary": "No unmet condition found.", "next_action": "Wait for the decision."}}, worker_actor(b))
    assert await continuation_state(hub, declarer["continuation_id"]) == "eligible"
    # Worker 1098 in the Linux pilot: its own feedback must not end its wait.
    giver = await wait_on_declaration(hub, b, declaration["id"])
    await quiet(hub, b)
    assert await continuation_state(hub, giver["continuation_id"]) == "waiting"
    resumed = await command(hub, "continuation.resume", {"continuation_id": declarer["continuation_id"], "run_id": run["id"]}, RUNNER)
    await command(hub, "task.complete.finalize", {"declaration_id": declaration["id"], "decision": "continue",
        "reason": "One condition deserves another look."}, worker_actor(resumed))
    assert await continuation_state(hub, giver["continuation_id"]) == "eligible"


async def test_both_current_executors_maintain_task_with_version_cas(hub):
    host, run = await publish(hub), await start(hub)
    a, b = await claim(hub, host, run), await claim(hub, host, run)
    child = await publish(hub, "Published jointly", actor=worker_actor(a))
    assert set(child["collaboration"]["publishers"]) == {a["worker_id"], b["worker_id"]}
    c, d = await claim(hub, child, run), await claim(hub, child, run)
    revised = await command(hub, "task.revise", {"task_id": child["id"], "revision": 1, "contract": contract("Refined child"), "reason": "Clarify execution expectations."}, worker_actor(d))
    assert revised["revision"] == 2 and revised["state"] == "draining"
    with pytest.raises(HubError) as error:
        await command(hub, "task.revise", {"task_id": child["id"], "revision": 1, "contract": contract("Stale overwrite"), "reason": "Stale edit."}, worker_actor(a))
    assert error.value.code == "STALE_REVISION"
    await quiet(hub, c)
    assert (await hub.task(child["id"]))["active_attempt"] == d["id"]
    await quiet(hub, d)
    assert (await hub.task(child["id"]))["state"] == "ready"
    original = await hub.task(child["id"], 1)
    assert original["contract"]["title"] == "Published jointly"


async def test_candidate_revision_change_cannot_promote_and_preserves_pending_evidence(hub):
    task, run = await publish(hub), await start(hub)
    a = await claim(hub, task, run)
    candidate = await stage(hub, a)
    reviewer = await select_review(hub, candidate["review_task_id"], run["id"])
    await review(hub, reviewer, qualifications=["Original domain validation is still pending."])
    waiting = await quiet(hub, reviewer)
    assert waiting["directive"] == "waiting"
    revised = await command(hub, "task.revise", {"task_id": task["id"], "revision": 1, "contract": contract(
        payload={"basis": "New basis"}), "reason": "Change the task basis."})
    assert revised["revision"] == 2
    with pytest.raises(HubError) as error:
        await machine(hub, candidate)
    assert error.value.code == "INPUTS_CHANGED"
    artifact = await hub.artifact(candidate["artifact_id"])
    assert artifact["status"] == "superseded" and artifact["peer_reviews"] == []
    pending = await hub.db.call(lambda c: c.execute(
        "SELECT checkpoint_json FROM attempts WHERE id=?", (reviewer["id"],)).fetchone()[0])
    report = json.loads(json.loads(pending)["summary"])
    assert report["candidate_id"] == candidate["artifact_id"]
    assert report["candidate_hash"] == candidate["body_sha"] and report["task_revision"] == 1
    assert report["qualifications"] == ["Original domain validation is still pending."]


async def test_negative_feedback_resumes_original_author_and_retires_reviewer(hub):
    task, run = await publish(hub), await start(hub)
    a = await claim(hub, task, run)
    candidate_a = await stage(hub, a)
    wait_a = await wait_for(hub, a, candidate_a["artifact_id"])
    await quiet(hub, a)
    b = await select_review(hub, candidate_a["review_task_id"], run["id"])
    await review(hub, b, "request_changes")
    await quiet(hub, b)
    resumed = await hub.next_assignment(run["id"])
    assert resumed["worker_id"] == a["worker_id"]
    assert resumed["resume_reason"] == "peer_review_received"
    packet = await hub.context(a["worker_id"])
    assert packet["resume_requirements"][0]["satisfied"] is False
    assert (await hub.artifact(candidate_a["artifact_id"]))["status"] == "rejected"
    assert next(item for item in await hub.list_continuations() if item["id"] == wait_a["continuation_id"])["state"] == "resumed"
    await wait_for(hub, resumed, candidate_a["artifact_id"])
    await quiet(hub, resumed)
    with pytest.raises(HubError) as retired:
        await claim(hub, await hub.task(task["id"]), run, b["worker_id"])
    assert retired.value.code == "WORKER_RETIRED"
    assert (await hub.worker(a["worker_id"]))["state"] == "suspended"
    assert (await hub.worker(b["worker_id"]))["state"] == "retired"
    assert await hub.next_assignment(run["id"]) is None


async def test_stage_idempotency_keeps_one_candidate_review_and_private_snapshot(hub):
    task, run = await publish(hub), await start(hub)
    a = await claim(hub, task, run)
    path = hub.workspace(a["worker_id"]) / "draft.txt"
    path.write_text("First immutable bytes.", encoding="utf-8")
    payload = {"kind": "software.result", "data": {"text": "immutable"},
               "repository_paths": ["draft.txt"]}
    first = await command(hub, "artifact.stage", payload, worker_actor(a), key="one-stage")
    path.write_text("Later private draft.", encoding="utf-8")
    second = await command(hub, "artifact.stage", payload, worker_actor(a), key="one-stage")
    assert first == second
    assert len(await hub.list_tasks()) == 2
    manifest = hub.blobs.get_json(first["snapshot"])
    assert hub.blobs.get(manifest["files"][0]["sha256"]) == b"First immutable bytes."


async def test_affiliated_review_gets_a_new_reviewer_and_preserves_the_peer_conversation_and_cursor(hub):
    task, run = await publish(hub), await start(hub)
    peer, author = await claim(hub, task, run), await claim(hub, task, run)
    await command(hub, "worker.bind_session", {
        "worker_id": peer["worker_id"], "thread_id": "persistent-peer-thread",
        "codex_profile": hub.config.codex.model_dump(mode="json"),
    }, RUNNER)
    candidate = await stage(hub, author)
    await machine(hub, candidate)
    host_packet = await hub.context(peer["worker_id"])
    assert host_packet["through_sequence"] > 0
    await command(hub, "context.ack", {
        "worker_id": peer["worker_id"], "packet_id": host_packet["packet_id"],
    }, worker_actor(peer))
    turn = await command(hub, "turn.reserve", {"worker_id": peer["worker_id"]}, RUNNER)
    await command(hub, "turn.bind", {"intent_id": turn["id"], "remote_turn_id": "completed-peer-turn"}, RUNNER)
    await command(hub, "turn.finish", {"intent_id": turn["id"], "status": "completed", "terminal_observed": True, "process_quiet": True}, RUNNER)
    reviewer = await hub.next_assignment(run["id"])
    assert reviewer["worker_id"] != peer["worker_id"]
    # A teammate's conversation is never reused for the review: its new reviewer has no
    # source worker and no thread, so it starts a new conversation.
    assert (await hub.worker(reviewer["worker_id"]))["kind"] == "review"
    assert (await hub.worker(reviewer["worker_id"]))["thread_id"] is None
    assert reviewer["task_id"] == candidate["review_task_id"]
    review_packet = await hub.context(reviewer["worker_id"])
    assert review_packet["after_sequence"] == 0
    assert review_packet["task"]["id"] == candidate["review_task_id"]
    assert review_packet["task"]["kind"] == "review"
    assert review_packet["task"]["contract_hash"] == reviewer["task"]["contract_hash"]
    binding = review_packet["task"]["collaboration"]["review"]
    assert binding["candidate_id"] == candidate["artifact_id"]
    assert binding["candidate_hash"] == candidate["body_sha"]
    assert binding["task_revision"] == task["revision"]
    await command(hub, "context.ack", {
        "worker_id": reviewer["worker_id"], "packet_id": review_packet["packet_id"],
    }, worker_actor(reviewer))
    await review(hub, reviewer)
    resumed_packet = await hub.context(peer["worker_id"])
    assert resumed_packet["context_epoch"] == host_packet["context_epoch"]
    assert resumed_packet["after_sequence"] == host_packet["through_sequence"]
    assert resumed_packet["task"]["id"] == task["id"]
    assert resumed_packet["task"]["contract_hash"] == task["contract_hash"]
    assert (await hub.worker(peer["worker_id"]))["thread_id"] == "persistent-peer-thread"
    assert (await hub.worker(peer["worker_id"]))["active_attempt"] == peer["id"]
    assert (await hub.worker(reviewer["worker_id"]))["state"] == "retired"

    unrelated = await publish(hub, "An independent task team")
    with pytest.raises(HubError) as error:
        await command(hub, "task.join", {"task_id": unrelated["id"], "revision": 1}, worker_actor(peer))
    assert error.value.code == "FORBIDDEN"  # TaskHub assigns tasks; a worker cannot join one.
    await command(hub, "task.subscribe", {"task_id": unrelated["id"]}, worker_actor(peer))
    same_team_packet = await hub.context(peer["worker_id"])
    assert same_team_packet["context_epoch"] == host_packet["context_epoch"]
    assert same_team_packet["task"]["id"] == task["id"]
    await legacy_wait(hub, {"reason": "Preserve original work while considering an independent task."}, worker_actor(peer))
    await quiet(hub, peer)
    # A worker paused mid-task is not idle: the independent task gets a new worker at once.
    joined = await hub.next_assignment(run["id"])
    assert joined["worker_id"] != peer["worker_id"] and joined["task_id"] == unrelated["id"]
    assert await hub.due_assignment_intents(run["id"]) == []
    assert (await hub.worker(peer["worker_id"]))["thread_id"] == "persistent-peer-thread"
    assert (await hub.worker(peer["worker_id"]))["work_intention"] == "waiting"


async def test_publisher_maintenance_preserves_its_current_conversation(hub):
    host, run = await publish(hub), await start(hub)
    publisher = await claim(hub, host, run)
    child = await publish(hub, "A separate published task", actor=worker_actor(publisher))
    revised = await command(hub, "task.revise", {
        "task_id": child["id"], "revision": 1,
        "contract": contract("Proposed child contract"), "reason": "Clarify the independent task.",
    }, worker_actor(publisher))
    packet = await hub.context(publisher["worker_id"])
    await command(hub, "context.ack", {
        "worker_id": publisher["worker_id"], "packet_id": packet["packet_id"],
    }, worker_actor(publisher))
    assert revised["revision"] == 2
    assert len(await hub.list_tasks()) == 2
    new_packet = await hub.context(publisher["worker_id"])
    assert new_packet["context_epoch"] == packet["context_epoch"]
    assert new_packet["after_sequence"] == packet["through_sequence"]
    assert new_packet["task"]["id"] == host["id"]
    assert await hub.db.call(lambda c: hub._worker_task(c, publisher["worker_id"])) == host["id"]
    assert await hub.db.call(lambda c: c.execute("SELECT count(*) FROM task_members WHERE task_id=? AND worker_id=?",
                            (child["id"], publisher["worker_id"])).fetchone()[0]) == 0
