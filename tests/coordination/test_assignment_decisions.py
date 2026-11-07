"""Each open task gets a worker at once: a new one, or an idle one a cheap decision chose."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Command, Role
from formal_geyser.coordination.runtime import (
    ScriptedHarness,
    ScriptedTurn,
    WorkerRunner,
)
from formal_geyser.coordination.runtime import claude as claude_runtime
from tests.coordination.publication_helpers import new_hub

from .wait_helpers import legacy_wait

OPERATOR, RUNNER = Actor(role=Role.OPERATOR), Actor(role=Role.RUNNER)


async def command(hub, name, payload=None, actor=OPERATOR):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


async def publish(hub, title):
    return await command(hub, "task.publish", {"contract": {"title": title, "objective": f"Work on {title}."}})


async def idle_after(hub, run, title):
    """A worker that executed a task titled `title` and then left it: idle, with that history."""
    task = await publish(hub, title)
    worker = await command(hub, "worker.register")
    attempt = await command(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"],
                                                "run_id": run["id"], "worker_id": worker["id"]})
    await command(hub, "worker.bind_session", {"worker_id": worker["id"], "thread_id": "thread-" + uuid4().hex,
                  "codex_profile": hub.config.codex.model_dump(mode="json")}, RUNNER)
    await hub.ordinary_turn_completed(worker["id"], attempt["id"])
    await command(hub, "task.leave", {"task_id": task["id"], "worker_id": worker["id"], "permanent": True,
                                      "reason": "The synthetic executor finished its part and leaves."})
    current = await hub.worker(worker["id"])
    assert current["work_intention"] == "idle" and current["active_attempt"] is None
    return current


class Recording(ScriptedHarness):
    opened: list[dict] = []

    async def open(self, **options):
        Recording.opened.append(options)
        return await super().open(**options)


def answering(answer):
    harnesses = []

    def factory():
        harness = Recording([ScriptedTurn([], visible_output=json.dumps(answer) if not isinstance(answer, str)
                                          else answer)])
        harnesses.append(harness)
        return harness
    return factory, harnesses


async def assignment_intent(hub, run):
    """The one prepared decision that admission creates while idle workers exist."""
    assert await hub.next_assignment(run["id"]) is None
    [intent] = await hub.due_assignment_intents(run["id"])
    return intent


async def decisions(hub):
    return await hub.db.call(lambda c: [dict(row) for row in c.execute(
        "SELECT * FROM helper_intents WHERE kind='assignment' ORDER BY created_at,id")])


@pytest.fixture
async def hub(tmp_path):
    project = await new_hub(tmp_path / "project")
    Recording.opened = []
    yield project
    await project.close()


async def test_without_idle_workers_each_open_task_gets_a_new_worker_at_once(hub):
    first, second = await publish(hub, "First"), await publish(hub, "Second")
    run = await command(hub, "run.start")
    assigned = [await hub.next_assignment(run["id"]), await hub.next_assignment(run["id"])]
    assert {item["task_id"] for item in assigned} == {first["id"], second["id"]}
    assert len({item["worker_id"] for item in assigned}) == 2
    assert await hub.next_assignment(run["id"]) is None
    assert await decisions(hub) == []  # No idle worker, so nothing to decide.


async def test_a_carried_over_worker_is_offered_with_the_tasks_it_brought(hub):
    run = await command(hub, "run.start")
    local = await idle_after(hub, run, "Lemma on valuations")
    carried = await command(hub, "worker.register")
    imported = await command(hub, "worker.import_history", {
        "worker_id": carried["id"], "tasks": ["Henselian fields", "  Local class field theory  "]})
    assert imported["tasks"] == ["Henselian fields", "Local class field theory"]
    # Its own executions come first; an imported title it executed here again is not repeated.
    await command(hub, "worker.import_history", {"worker_id": local["id"], "tasks": ["Lemma on valuations", "Older work"]})
    await publish(hub, "Valuation estimates")
    intent = await assignment_intent(hub, run)
    offered = {item["worker_id"]: item["previous_tasks"] for item in intent["candidates"]["workers"]}
    assert offered == {local["id"]: ["Lemma on valuations", "Older work"],
                       carried["id"]: ["Henselian fields", "Local class field theory"]}
    for tasks in (["Too many"] * 11, [""], "not a list"):
        with pytest.raises(HubError) as refused:
            await command(hub, "worker.import_history", {"worker_id": carried["id"], "tasks": tasks})
        assert refused.value.code == "INVALID_INPUT"


async def test_one_decision_chooses_an_idle_worker_who_starts_a_new_conversation(hub, monkeypatch):
    run = await command(hub, "run.start")
    expert = await idle_after(hub, run, "Lemma on valuations")
    other = await idle_after(hub, run, "Unrelated combinatorics")
    task = await publish(hub, "Valuation estimates")
    assert await hub.next_assignment(run["id"]) is None  # The task waits for its decision.
    [intent] = await hub.due_assignment_intents(run["id"])
    assert intent["source_task_id"] == task["id"]
    assert [(item["name"], item["previous_tasks"]) for item in intent["candidates"]["workers"]] == [
        (expert["display_name"], ["Lemma on valuations"]), (other["display_name"], ["Unrelated combinatorics"])]
    assert (await hub.worker(intent["source_worker_id"]))["display_name"] == "Dispatcher"

    factory, harnesses = answering({"worker": expert["display_name"]})
    runner = WorkerRunner(hub, factory)
    models = []
    original = runner._new_harness

    async def new_harness(backend="codex", **options):
        models.append((backend, options))
        return await original(backend, **options)
    monkeypatch.setattr(runner, "_new_harness", new_harness)
    result = await runner.run_helper(intent)

    assert models == [("codex", {"model": "gpt-6.1-sol", "effort": "high", "decision": True})]
    [opened] = Recording.opened
    assert opened["tools"] == [] and opened["thread_id"] is None  # A new conversation without tools.
    prompt = json.loads(harnesses[0].prompts[0])
    assert prompt["task"]["title"] == "Valuation estimates"
    assert prompt["idle_workers"] == [{"name": expert["display_name"], "previous_tasks": ["Lemma on valuations"]},
                                      {"name": other["display_name"], "previous_tasks": ["Unrelated combinatorics"]}]
    assert result["directive"] == "assigned" and result["outcome"] == "idle_worker"
    assignment = result["assignment"]
    assert assignment["worker_id"] == expert["id"] and assignment["task_id"] == task["id"]
    worker = await hub.worker(expert["id"])
    assert worker["thread_id"] is None and worker["current_task_id"] == task["id"]  # A new conversation.
    assert (await hub.worker(other["id"]))["work_intention"] == "idle"


@pytest.mark.parametrize(("answer", "reason"), [
    ({"worker": None}, "none_suitable"),
    ({"worker": "Worker 99"}, "none_suitable"),  # Not an offered name: outside the schema.
    ("not a decision", "none_suitable"),
])
async def test_none_unknown_or_invalid_answers_give_the_task_a_new_worker(hub, answer, reason):
    run = await command(hub, "run.start")
    idle = await idle_after(hub, run, "Earlier task")
    task = await publish(hub, "Next task")
    assert await hub.next_assignment(run["id"]) is None
    [intent] = await hub.due_assignment_intents(run["id"])
    factory, _ = answering(answer)
    result = await WorkerRunner(hub, factory).run_helper(intent)
    assert result["outcome"] == "new_worker" and result["reason"] == reason
    assert result["assignment"]["task_id"] == task["id"] and result["assignment"]["worker_id"] != idle["id"]
    assert (await hub.worker(idle["id"]))["work_intention"] == "idle"
    if answer != {"worker": None}:
        stored = await hub.db.call(lambda c: c.execute(
            "SELECT raw_result_json FROM helper_intents WHERE id=?", (intent["id"],)).fetchone()[0])
        assert json.loads(stored)["invalid_output"] is True


async def test_an_operator_claim_of_another_task_also_starts_a_new_conversation(hub):
    run = await command(hub, "run.start")
    worker = await idle_after(hub, run, "Lemma on valuations")
    task = await publish(hub, "Valuation estimates")
    await command(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"],
                                      "run_id": run["id"], "worker_id": worker["id"]})
    claimed = await hub.worker(worker["id"])
    assert claimed["thread_id"] is None and claimed["current_task_id"] == task["id"]
    [released] = [event for event in await hub.events() if event["kind"] == "worker.conversation_released"]
    assert released["payload"] == {"thread_id": worker["thread_id"], "reason": "new_task"}


async def test_a_paused_worker_is_not_idle_and_one_decision_runs_at_a_time(hub):
    run = await command(hub, "run.start")
    waiting_task = await publish(hub, "Paused work")
    waiting = await command(hub, "worker.register")
    attempt = await command(hub, "task.claim", {"task_id": waiting_task["id"], "revision": 1,
                                                "run_id": run["id"], "worker_id": waiting["id"]})
    await legacy_wait(hub, {"reason": "Waiting for an answer"}, Actor(
        role=Role.WORKER, worker_id=waiting["id"], attempt_id=attempt["id"], fencing_token=attempt["fencing_token"]))
    await command(hub, "runtime.quiescent", {"worker_id": waiting["id"],
                  "snapshot": hub.blobs.snapshot(hub.workspace(waiting["id"]))}, RUNNER)
    assert (await hub.worker(waiting["id"]))["work_intention"] == "waiting"
    first = await publish(hub, "First new task")
    # Only the paused worker exists: it is not offered, so the task gets a new worker at once.
    fresh = await hub.next_assignment(run["id"])
    assert fresh["task_id"] == first["id"] and fresh["worker_id"] != waiting["id"]
    assert await decisions(hub) == []
    idle = await idle_after(hub, run, "Finished task")
    second, third = await publish(hub, "Second new task"), await publish(hub, "Third new task")
    assert await hub.next_assignment(run["id"]) is None
    [intent] = await hub.due_assignment_intents(run["id"])
    assert intent["source_task_id"] == second["id"]
    assert [item["worker_id"] for item in intent["candidates"]["workers"]] == [idle["id"]]
    # The third task waits while the one decision runs: its idle candidate is the same worker.
    assert await hub.next_assignment(run["id"]) is None and len(await decisions(hub)) == 1
    factory, _ = answering({"worker": idle["display_name"]})
    assigned = await WorkerRunner(hub, factory).run_helper(intent)
    assert assigned["assignment"]["worker_id"] == idle["id"]
    # Now no worker is idle: the third task gets a new worker without a decision.
    later = await hub.next_assignment(run["id"])
    assert later["task_id"] == third["id"] and len(await decisions(hub)) == 1


async def test_failed_decisions_give_way_to_a_new_worker(hub):
    run = await command(hub, "run.start")
    await idle_after(hub, run, "Earlier task")
    task = await publish(hub, "Next task")
    for _ in range(2):
        assert await hub.next_assignment(run["id"]) is None
        [intent] = await hub.due_assignment_intents(run["id"])
        await hub.db.call(lambda c: c.execute(
            "UPDATE helper_intents SET state='cancelled',reason='automatic_retry_after_error' WHERE id=?",
            (intent["id"],)), write=True)
    assignment = await hub.next_assignment(run["id"])
    assert assignment["task_id"] == task["id"] and await hub.due_assignment_intents(run["id"]) == []


async def test_the_operator_chooses_the_decision_model(hub):
    assert await hub.decision_preferences() == {"model": "gpt-6.1-sol", "effort": "high", "source": "config"}
    chosen = await command(hub, "runtime.set_decision_model", {"model": "gpt-6.1-mini", "effort": "medium"})
    assert chosen == {"model": "gpt-6.1-mini", "effort": "medium", "source": "operator"}
    assert (await hub.backend_status())["decision_model"] == chosen
    for invalid in ({"model": ""}, {"model": "two words"}, {"model": "gpt", "effort": "huge"}):
        with pytest.raises(HubError):
            await command(hub, "runtime.set_decision_model", invalid)


async def test_recruitment_is_one_yes_or_no_of_the_decision_model_in_its_own_conversation(hub):
    run = await command(hub, "run.start")
    task = await publish(hub, "Large task")
    attempt = await hub.next_assignment(run["id"])
    await command(hub, "worker.bind_session", {"worker_id": attempt["worker_id"], "thread_id": "first-thread",
                  "codex_profile": hub.config.codex.model_dump(mode="json")}, RUNNER)
    await hub.ordinary_turn_completed(attempt["worker_id"], attempt["id"])
    [intent] = await hub.due_recruitment_intents(run["id"])
    factory, harnesses = answering({"decision": True})
    result = await WorkerRunner(hub, factory).run_helper(intent)
    assert result["directive"] == "recruitment_decided"
    assert Recording.opened[0]["thread_id"] is None and Recording.opened[0]["tools"] == []
    assert json.loads(harnesses[0].prompts[0])["task"]["title"] == "Large task"
    lifecycle = (await hub.task(task["id"]))["collaboration"]["recruitment"]
    assert lifecycle["opportunity_state"] == "open" and lifecycle["invitation"]


async def test_decisions_use_the_codex_account_whatever_the_worker_backend(hub, monkeypatch):
    monkeypatch.setattr(claude_runtime, "claude_unavailable", lambda config: None)
    await command(hub, "runtime.select_backend", {"backend": "claude"})
    run = await command(hub, "run.start")
    await publish(hub, "Large task")
    attempt = await hub.next_assignment(run["id"])
    assert (await hub.worker(attempt["worker_id"]))["backend"] == "claude"
    await command(hub, "worker.bind_session", {"worker_id": attempt["worker_id"], "thread_id": "first-thread",
                  "codex_profile": hub.config.codex.model_dump(mode="json")}, RUNNER)
    await hub.ordinary_turn_completed(attempt["worker_id"], attempt["id"])
    # A Claude hold does not stop the decision about a Claude worker's task; a Codex hold does.
    assert await hub.hold_dispatch("claudeUsageLimit", "Limit reached", subject_id="turn") is True
    assert len(await hub.due_recruitment_intents(run["id"])) == 1
    assert await hub.hold_dispatch("usageLimitExceeded", "Codex limit", subject_id="turn") is True
    assert await hub.due_recruitment_intents(run["id"]) == []
    await command(hub, "run.resume", {"run_id": run["id"]})
    await hub.finish_dispatch_release()
    # The Codex account pays for the decision, not the worker's Claude account.
    [intent] = await hub.due_recruitment_intents(run["id"])
    harness = ScriptedHarness([ScriptedTurn([], usage={"totalTokens": 50, "lastTokenUsage": {"totalTokens": 50}},
                                            visible_output=json.dumps({"decision": False}))])
    await WorkerRunner(hub, lambda: harness).run_helper(intent)
    record = await hub.run(run["id"])
    assert (record["codex_tokens_used"], record["claude_tokens_used"]) == (50, 0)

    # While Codex is held, a task that would need a decision gets a new Claude worker instead.
    idle = await idle_after(hub, run, "Earlier task")
    assert idle["backend"] == "claude"
    assert await hub.hold_dispatch("usageLimitExceeded", "Codex limit", subject_id="turn") is True
    task = await publish(hub, "Next task")
    continuing = await hub.next_assignment(run["id"])  # The first Claude worker's own task comes first.
    assert continuing["worker_id"] == attempt["worker_id"] and continuing["task_id"] == attempt["task_id"]
    assignment = await hub.next_assignment(run["id"])
    assert assignment["task_id"] == task["id"] and assignment["worker_id"] != idle["id"]
    assert (await hub.worker(assignment["worker_id"]))["backend"] == "claude"
    assert await decisions(hub) == []


async def test_a_decision_of_unknown_outcome_holds_only_its_own_task(hub):
    run = await command(hub, "run.start")
    await idle_after(hub, run, "Earlier task")
    first = await publish(hub, "First task")
    held = await assignment_intent(hub, run)
    await hub.db.call(lambda c: c.execute("UPDATE helper_intents SET state='uncertain' WHERE id=?", (held["id"],)),
                      write=True)
    second = await publish(hub, "Second task")
    # The other task still gets its decision; the held task gets neither a second one nor a new worker.
    decision = await assignment_intent(hub, run)
    assert decision["source_task_id"] == second["id"]
    assert [item["source_task_id"] for item in await decisions(hub)].count(first["id"]) == 1


async def test_one_task_that_cannot_be_assigned_does_not_hold_up_the_others(hub, monkeypatch):
    run = await command(hub, "run.start")
    broken, healthy = await publish(hub, "Broken task"), await publish(hub, "Healthy task")
    original = hub._new_worker_claim

    def failing(c, candidate, run_id):
        if candidate["task_id"] == broken["id"]:
            raise ValueError("unreadable record")
        return original(c, candidate, run_id)
    monkeypatch.setattr(hub, "_new_worker_claim", failing)
    assignment = await hub.next_assignment(run["id"])
    assert assignment["task_id"] == healthy["id"]
    await hub.next_assignment(run["id"])
    skipped = await hub.db.call(lambda c: c.execute(
        "SELECT count(*) FROM events WHERE kind='admission.item_skipped' AND object_id=?", (broken["id"],)).fetchone()[0])
    assert skipped == 1  # Reported once, however often admission looks again.
    assert await hub.db.call(lambda c: c.execute("SELECT count(*) FROM workers WHERE kind='ordinary'").fetchone()[0]) == 1


async def test_a_long_invitation_still_lets_a_new_worker_join(hub):
    run = await command(hub, "run.start")
    task = await publish(hub, "Large task")
    attempt = await hub.next_assignment(run["id"])
    await command(hub, "worker.bind_session", {"worker_id": attempt["worker_id"], "thread_id": "first-thread",
                  "codex_profile": hub.config.codex.model_dump(mode="json")}, RUNNER)
    await hub.ordinary_turn_completed(attempt["worker_id"], attempt["id"])
    [intent] = await hub.due_recruitment_intents(run["id"])
    await hub.decide_recruitment(intent["id"], True, "Prove the second case. " + "x" * 4977)
    continuing = await hub.next_assignment(run["id"])  # The first executor continues first.
    assert continuing["worker_id"] == attempt["worker_id"]
    joined = await hub.next_assignment(run["id"])
    assert joined["task_id"] == task["id"] and joined["worker_id"] != attempt["worker_id"]
    plan = await hub.db.call(lambda c: c.execute(
        "SELECT contribution_plan FROM task_members WHERE task_id=? AND worker_id=?",
        (task["id"], joined["worker_id"])).fetchone()[0])
    assert len(plan) == 5000 and plan.startswith("Undertake the advertised contribution: Prove") and plan.endswith("…")


async def test_failed_claims_count_as_failed_decisions(hub, monkeypatch):
    run = await command(hub, "run.start")
    idle = await idle_after(hub, run, "Earlier task")
    task = await publish(hub, "Next task")
    original = hub._claim_opportunity

    def refusing(c, worker_id, candidate, run_id):
        if worker_id == idle["id"]:
            raise HubError("CONTRIBUTION_PLAN_REQUIRED", "A collaborator must state the part it will undertake", 422)
        return original(c, worker_id, candidate, run_id)
    monkeypatch.setattr(hub, "_claim_opportunity", refusing)
    for _ in range(2):
        intent = await assignment_intent(hub, run)
        result = await hub.complete_assignment(intent["id"], idle["display_name"])
        assert result["outcome"] == "claim_failed" and result["reason"] == "CONTRIBUTION_PLAN_REQUIRED"
    # Two failed decisions: the task now gets a new worker without a third decision.
    assignment = await hub.next_assignment(run["id"])
    assert assignment["task_id"] == task["id"] and assignment["worker_id"] != idle["id"]
    assert len(await decisions(hub)) == 2


async def test_the_dispatcher_is_a_named_identity_and_replies_count_offered_workers(hub):
    run = await command(hub, "run.start")
    await idle_after(hub, run, "First earlier task")
    await idle_after(hub, run, "Second earlier task")
    await publish(hub, "Next task")
    intent = await assignment_intent(hub, run)
    dispatcher = await hub.db.call(lambda c: hub._resolve_worker_id(c, "Dispatcher"))
    assert dispatcher == intent["source_worker_id"]
    reply = await hub.db.call(lambda c: hub._helper_reply(c, intent["id"]))
    assert reply["candidate_count"] == 2
