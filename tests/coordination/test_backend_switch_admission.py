"""A worker that asked to move to the other backend is admitted only on that account, and its next
activation continues its task in a new conversation there, not first in its old one. It takes no
further session on the account it leaves, for its waiting work or for a new task."""
from __future__ import annotations

from uuid import uuid4

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    AssignmentConfig,
    ClaudeConfig,
    CodexConfig,
    Command,
    ConcurrencyConfig,
    ProjectConfig,
    Role,
    RoleModelConfig,
)
from formal_geyser.coordination.runtime import claude as claude_runtime
from tests.coordination.lifecycle_helpers import assignment_decision, idle_worker, record_saved_turn

CONFIG = ProjectConfig(codex=CodexConfig(model="gpt-6-astra", effort="xhigh", review_effort="max", integrator_effort="max"),
                       claude=ClaudeConfig(model="claude-opus-5-5", effort="xhigh"),
                       reviewers=RoleModelConfig(model="gpt-6-astra"), integrator=RoleModelConfig(model="gpt-6-astra"),
                       assignment=AssignmentConfig(worker_backends=["codex"], reviewer_backend="codex"),
                       concurrency=ConcurrencyConfig(codex=60, claude=12))
RUNNER = Actor(role=Role.RUNNER)
CHECKPOINT = {"summary": "Saved progress", "next_action": "Continue from the saved progress", "input_refs": []}


async def command(hub, name, payload=None, actor=None):
    return await hub.execute(Command(name=name, payload=payload or {}, key=uuid4().hex),
                             actor or Actor(role=Role.OPERATOR))


@pytest.fixture
async def hub(tmp_path, monkeypatch):
    # Selecting Claude checks this machine; the tests exercise the hub, not the platform.
    monkeypatch.setattr(claude_runtime, "claude_unavailable", lambda config: None)
    initialize(tmp_path, CONFIG)
    result = Hub(tmp_path)
    try:
        yield result
    finally:
        await result.close()


async def paused_on_codex(hub, run, title):
    """A Codex worker paused in its task on a question to the operator, its conversation open."""
    task = await command(hub, "task.publish", {"contract": {"title": title, "objective": f"Work on {title}."}})
    worker = await command(hub, "worker.register")
    attempt = await command(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"],
                                                "run_id": run["id"], "worker_id": worker["id"]})
    await command(hub, "worker.bind_session", {
        "worker_id": worker["id"], "thread_id": "thread-" + uuid4().hex, "model": "gpt-6-astra", "codex_home": None,
        "codex_profile": {}, "settings": {"model": "gpt-6-astra", "effort": "xhigh"}}, RUNNER)
    await record_saved_turn(hub, worker["id"])
    actor = Actor(role=Role.WORKER, worker_id=worker["id"], attempt_id=attempt["id"],
                  fencing_token=attempt["fencing_token"])
    request = await command(hub, "message.request", {
        "request_key": "scope-" + uuid4().hex, "title": "Scope", "text": "Which record is meant?"}, actor)
    await command(hub, "task.pause", {"reason": "Wait for the operator's answer.", "checkpoint": CHECKPOINT,
                                      "predicate": request["wait_predicate"]}, actor)
    await command(hub, "runtime.quiescent", {"worker_id": worker["id"],
                                             "snapshot": hub.blobs.snapshot(hub.workspace(worker["id"]))}, RUNNER)
    return await hub.worker(worker["id"]), attempt, request


async def answer(hub, request):
    await command(hub, "request.resolve", {"request_id": request["id"], "outcome": "fulfilled",
                                           "text": "The first record.", "message_id": "answer-" + uuid4().hex})


async def rotations(hub, worker_id):
    return await hub.db.read(lambda c: [tuple(row) for row in c.execute(
        "SELECT reason,state FROM rotations WHERE worker_id=? ORDER BY requested_at,rowid", (worker_id,))])


async def requested(hub, worker_id):
    return {item["id"]: item for item in await hub.list_workers()}[worker_id]["requested_backend"]


async def admitted_workers(hub, run):
    """Every admission now possible, and the workers it admitted."""
    workers = []
    for _ in range(6):
        assignment = await hub.next_assignment(run["id"])
        if assignment is None:
            break
        workers.append(assignment["worker_id"])
    return workers


async def test_a_waiting_worker_moves_at_its_next_turn_without_a_session_on_its_old_account(hub):
    run = await command(hub, "run.start")
    worker, _, request = await paused_on_codex(hub, run, "Valuation estimates")
    pending = await command(hub, "worker.select_backend", {"worker_id": worker["id"], "backend": "claude"})
    assert (pending["backend"], pending["requested_backend"]) == ("codex", "claude")
    # Codex starts no worker session at all; the worker needs none there.
    await command(hub, "runtime.set_concurrency", {"backend": "codex", "sessions": 0})
    await answer(hub, request)
    assignment = await hub.next_assignment(run["id"])
    assert assignment is not None and assignment["worker_id"] == worker["id"]
    # Its session counts toward Claude from admission, and the activation is to move first.
    assert await rotations(hub, worker["id"]) == [("backend", "ready")]
    sessions = (await hub.backend_status())["sessions"]
    assert (sessions["claude"]["live"], sessions["codex"]["live"]) == (1, 0)
    # The runner's step before the conversation opens: the old Codex conversation is not
    # continued, whatever its idle time; TaskHub releases it and the worker moves.
    rotation = await hub.rotation_due(worker["id"], assignment)
    assert rotation is not None and rotation["reason"] == "backend"
    began = await hub.begin_rotation(rotation["id"], handoff={"valid": False}, state_path="state.md")
    assert began is not None and began["state"] == "started"
    moved = await hub.worker(worker["id"])
    assert (moved["backend"], moved["thread_id"]) == ("claude", None)
    assert await requested(hub, worker["id"]) is None
    settings = await hub.session_settings(worker["id"])
    assert (settings["backend"], settings["model"], settings["new"]) == ("claude", "claude-opus-5-5", True)
    assert (await hub.backend_status())["sessions"]["claude"]["live"] == 1


async def test_a_worker_that_moves_waits_for_a_session_on_its_new_account(hub):
    run = await command(hub, "run.start")
    worker, _, request = await paused_on_codex(hub, run, "Valuation estimates")
    await command(hub, "worker.select_backend", {"worker_id": worker["id"], "backend": "claude"})
    await answer(hub, request)
    # Claude is held: Codex has room, but the worker does not run there first.
    assert await hub.hold_dispatch("claudeUsageLimit", "Limit reached", subject_id="turn") is True
    assert worker["id"] not in await admitted_workers(hub, run)
    # Claude is full: the same.
    await command(hub, "run.resume", {"run_id": run["id"]})
    await hub.finish_dispatch_release()
    await command(hub, "runtime.set_concurrency", {"backend": "claude", "sessions": 0})
    assert worker["id"] not in await admitted_workers(hub, run)
    waiting = await hub.worker(worker["id"])
    assert (waiting["backend"], waiting["active_attempt"], await requested(hub, worker["id"])) == ("codex", None, "claude")
    assert await rotations(hub, worker["id"]) == []
    # With a free Claude session it is admitted, there.
    await command(hub, "runtime.set_concurrency", {"backend": "claude", "sessions": 1})
    assert worker["id"] in await admitted_workers(hub, run)
    assert (await hub.backend_status())["sessions"]["claude"]["live"] == 1


async def test_a_move_held_since_admission_keeps_the_old_conversation(hub):
    run = await command(hub, "run.start")
    worker, _, request = await paused_on_codex(hub, run, "Valuation estimates")
    await command(hub, "worker.select_backend", {"worker_id": worker["id"], "backend": "claude"})
    await answer(hub, request)
    assignment = await hub.next_assignment(run["id"])
    assert assignment["worker_id"] == worker["id"]
    rotation = await hub.rotation_due(worker["id"], assignment)
    assert await hub.hold_dispatch("claudeUsageLimit", "Limit reached", subject_id="turn") is True
    # No new conversation on Codex: this activation continues the old one, and counts there.
    assert await hub.begin_rotation(rotation["id"], handoff={"valid": False}, state_path="state.md") is None
    kept = await hub.worker(worker["id"])
    assert (kept["backend"], kept["thread_id"]) == ("codex", worker["thread_id"])
    assert await requested(hub, worker["id"]) == "claude"
    assert await rotations(hub, worker["id"]) == [("backend", "cancelled")]
    assert (await hub.backend_status())["sessions"]["codex"]["live"] == 1


async def test_a_worker_waiting_to_move_waits_while_its_old_conversation_has_open_records(hub):
    run = await command(hub, "run.start")
    worker, attempt, request = await paused_on_codex(hub, run, "Valuation estimates")
    await command(hub, "worker.select_backend", {"worker_id": worker["id"], "backend": "claude"})
    # A validation of a call in its Codex conversation is still open.
    await hub.db.call(lambda c: c.execute(
        "INSERT INTO native_call_receipts(worker_id,thread_id,call_id,attempt_id,fencing_token,request_hash,"
        "validation_state,created_at) VALUES(?,?,?,?,?,?,'running',?)",
        (worker["id"], worker["thread_id"], "call-1", attempt["id"], attempt["fencing_token"], "hash", hub.clock())),
        write=True)
    await answer(hub, request)
    # Codex has room and so does Claude, but it takes no session on the account it leaves.
    assert worker["id"] not in await admitted_workers(hub, run)
    sessions = (await hub.backend_status())["sessions"]
    assert (sessions["codex"]["live"], sessions["claude"]["live"]) == (0, 0)
    # Once the record settles it moves, at its next turn.
    await hub.db.call(lambda c: c.execute(
        "UPDATE native_call_receipts SET validation_state='completed' WHERE worker_id=?", (worker["id"],)), write=True)
    assert worker["id"] in await admitted_workers(hub, run)
    assert await rotations(hub, worker["id"]) == [("backend", "ready")]
    assert (await hub.backend_status())["sessions"]["claude"]["live"] == 1


async def test_a_new_task_goes_to_a_worker_waiting_to_move_only_on_its_new_account(hub):
    run = await command(hub, "run.start")
    worker = await idle_worker(hub, run, "Finished lemma")
    await command(hub, "worker.select_backend", {"worker_id": worker["id"], "backend": "claude"})
    # Claude is full; Codex has room. The worker is not offered for new work.
    await command(hub, "runtime.set_concurrency", {"backend": "claude", "sessions": 0})
    assert worker["id"] not in [item["id"] for item in await hub.db.read(lambda c: hub._idle_workers(c, run["id"]))]
    # With a free Claude session it is offered, and the new task starts it on Claude.
    await command(hub, "runtime.set_concurrency", {"backend": "claude", "sessions": 1})
    assert worker["id"] in [item["id"] for item in await hub.db.read(lambda c: hub._idle_workers(c, run["id"]))]
    await command(hub, "task.publish", {"contract": {"title": "Valuation bounds", "objective": "Bound valuations."}})
    intent = await assignment_decision(hub, run["id"])
    assert [item["worker_id"] for item in intent["candidates"]["workers"]] == [worker["id"]]
    chosen = await hub.complete_assignment(intent["id"], worker["display_name"])
    assert chosen["outcome"] == "idle_worker" and chosen["assignment"]["worker_id"] == worker["id"]
    moved = await hub.worker(worker["id"])
    assert (moved["backend"], moved["thread_id"]) == ("claude", None)
    assert await requested(hub, worker["id"]) is None
    sessions = (await hub.backend_status())["sessions"]
    assert (sessions["claude"]["live"], sessions["codex"]["live"]) == (1, 0)


async def test_a_decision_completed_after_the_new_account_filled_takes_no_session_on_the_old(hub):
    run = await command(hub, "run.start")
    worker = await idle_worker(hub, run, "Finished lemma")
    await command(hub, "worker.select_backend", {"worker_id": worker["id"], "backend": "claude"})
    await command(hub, "task.publish", {"contract": {"title": "Valuation bounds", "objective": "Bound valuations."}})
    intent = await assignment_decision(hub, run["id"])
    # Claude fills while the decision runs: the chosen worker is no longer offered.
    await command(hub, "runtime.set_concurrency", {"backend": "claude", "sessions": 0})
    chosen = await hub.complete_assignment(intent["id"], worker["display_name"])
    assert chosen.get("worker_id") != worker["id"]
    kept = await hub.worker(worker["id"])
    assert (kept["backend"], kept["active_attempt"]) == ("codex", None)
    assert await requested(hub, worker["id"]) == "claude"


async def test_a_message_turn_about_no_task_moves_at_once(hub):
    run = await command(hub, "run.start")
    worker = await idle_worker(hub, run, "Finished lemma")
    assert worker["thread_id"] is not None and worker["current_task_id"] is None
    pending = await command(hub, "worker.select_backend", {"worker_id": worker["id"], "backend": "claude"})
    assert pending["requested_backend"] == "claude"
    await command(hub, "runtime.set_concurrency", {"backend": "codex", "sessions": 0})
    await command(hub, "message.send", {"worker_id": worker["id"], "text": "A question about your old task.",
                                        "message_id": "note-" + uuid4().hex})
    assignment = await hub.next_assignment(run["id"])
    assert assignment is not None and assignment["worker_id"] == worker["id"]
    moved = await hub.worker(worker["id"])
    # There is no task to continue: the old conversation is released at admission.
    assert (moved["backend"], moved["thread_id"]) == ("claude", None)
    assert await requested(hub, worker["id"]) is None
    assert (await hub.backend_status())["sessions"]["claude"]["live"] == 1
