"""Cross-event lifecycle regressions with synthetic files and no model execution."""
import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import Actor, ProjectConfig, Role
from tests.coordination.lifecycle_helpers import (
    assignment_decision,
    finish_ordinary,
    open_collaboration,
    start_helper,
)
from tests.coordination.publication_helpers import file_payload, new_hub
from tests.coordination.test_lifecycle_repairs import (
    RUNNER,
    accept_candidate,
    claim,
    command,
    publish,
    quiet,
    worker,
)
from tests.coordination.wait_helpers import legacy_wait


@pytest.fixture
async def case(tmp_path_factory):
    hub = await new_hub(tmp_path_factory.mktemp("af"))
    root = await publish(hub, "Synthetic root")
    run = await command(hub, "run.start")
    author = await hub.next_assignment(run["id"])
    yield hub, root, run, author
    await hub.close()


async def accepted(hub, attempt, run):
    candidate = await command(hub, "artifact.stage", file_payload(
        hub, attempt, data={"synthetic": "independently reviewed file fixture"}), worker(attempt))
    await accept_candidate(hub, candidate, run)
    return candidate["artifact_id"]


async def declaration(hub, task, attempt, artifact_ids):
    return await command(hub, "task.complete.declare", {"task_id": task["id"], "revision": task["revision"],
        "evidence": {"objective": {"artifact_ids": artifact_ids, "explanation": "Exact synthetic accepted results"}}}, worker(attempt))


async def finalize(hub, completion, attempt):
    return await command(hub, "task.complete.finalize", {
        "declaration_id": completion["id"], "reason": "Every original condition has exact accepted evidence",
    }, worker(attempt))


async def project(hub):
    return await hub.db.call(lambda c: dict(c.execute("SELECT * FROM project_lifecycle").fetchone()))


@pytest.mark.asyncio
@pytest.mark.parametrize("operator_pause", [False, True])
async def test_failed_root_finalization_rolls_back_only_its_own_drain(case, operator_pause):
    hub, root, run, author = case
    other_task = await publish(hub, "Retain other work")
    other = await claim(hub, other_task, run)
    path = hub.workspace(other["worker_id"]) / "unfinished.txt"
    path.write_text("Preserve unfinished work", encoding="utf-8")
    artifact_id = await accepted(hub, author, run)
    completion = await declaration(hub, root, author, [artifact_id])
    await finalize(hub, completion, author)
    if operator_pause:
        await command(hub, "run.pause", {"run_id": run["id"]})
    await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Evidence invalidated during drain"})
    await quiet(hub, author)
    assert (await project(hub))["state"] == "shutting_down"
    await quiet(hub, other)
    assert (await project(hub))["state"] == "running"
    assert (await hub.run(run["id"]))["state"] == ("paused" if operator_pause else "running")
    assert (await hub.task(root["id"]))["state"] != "done"
    assert path.read_text(encoding="utf-8") == "Preserve unfinished work"
    for attempt in (author, other):
        current = await hub.worker(attempt["worker_id"])
        assert current["state"] == "idle" and current["work_intention"] == "working"
        task = await hub.task(attempt["task_id"])
        assert any(member["worker_id"] == current["id"] and member["runnable"] for member in task["collaboration"]["members"])
    resumed = await hub.next_assignment(run["id"])
    assert (resumed is None) == operator_pause


@pytest.mark.asyncio
async def test_invalidated_nonroot_completion_restores_its_quiet_worker(case):
    hub, _, run, _ = case
    task = await publish(hub, "Complete an ordinary task")
    attempt = await claim(hub, task, run)
    artifact_id = await accepted(hub, attempt, run)
    completion = await declaration(hub, task, attempt, [artifact_id])
    await finalize(hub, completion, attempt)
    await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Invalidate local finalization"})
    await quiet(hub, attempt)
    current = await hub.worker(attempt["worker_id"])
    assert current["state"] == "idle" and current["work_intention"] == "working"
    assert (await hub.task(task["id"]))["collaboration"]["members"][0]["runnable"]
    assert (await hub.run(run["id"]))["state"] == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("already_quiet", [False, True])
async def test_clean_departure_truthfully_closes_pending_feedback(case, already_quiet):
    hub, task, run, author = case
    await open_collaboration(hub, task["id"])
    peer = await claim(hub, task, run, plan="Inspect the remaining condition")
    artifact_id = await accepted(hub, author, run)
    completion = await declaration(hub, task, author, [artifact_id])
    if already_quiet:
        await hub.ordinary_turn_completed(peer["worker_id"], peer["id"])
    await command(hub, "task.leave", {"task_id": task["id"], "worker_id": peer["worker_id"],
                  "permanent": True, "reason": "Permanently leave the collaboration"})
    if not already_quiet:
        pending = await hub.db.call(lambda c: hub._completion_record(c, completion["id"]))
        assert pending["feedback_state"] == "pending"
        await quiet(hub, peer)
    resolved = await hub.db.call(lambda c: hub._completion_record(c, completion["id"]))
    assert resolved["feedback_state"] == "unavailable"
    assert "permanently left" in resolved["feedback"]["reason"]
    assert "recovery_impossible" not in resolved["feedback"]
    await finalize(hub, completion, author)
    await quiet(hub, author)
    assert (await hub.task(task["id"]))["state"] == "done"
    assert (await hub.worker(peer["worker_id"]))["work_intention"] == "idle"


@pytest.mark.asyncio
@pytest.mark.parametrize("already_quiet", [False, True])
async def test_retirement_returns_quiet_or_draining_worker_to_idle(tmp_path_factory, already_quiet):
    root = tmp_path_factory.mktemp("rt")
    initialize(root, ProjectConfig())
    hub = Hub(root)
    try:
        task = await publish(hub, "Retire this task")
        run = await command(hub, "run.start")
        attempt = await hub.next_assignment(run["id"])
        if already_quiet:
            await hub.ordinary_turn_completed(attempt["worker_id"], attempt["id"])
        await command(hub, "task.retire", {"task_id": task["id"], "revision": 1, "reason": "Task no longer needed"})
        if not already_quiet:
            await quiet(hub, attempt)
        eligible = await publish(hub, "Eligible next task")
        current = await hub.worker(attempt["worker_id"])
        assert current["state"] == "idle" and current["work_intention"] == "idle"
        assert current["current_task_id"] is None
        # The idle worker is offered the next task in an assignment decision.
        intent = await assignment_decision(hub, run["id"])
        assert intent["source_task_id"] == eligible["id"]
        assert [item["worker_id"] for item in intent["candidates"]["workers"]] == [current["id"]]
    finally:
        await hub.close()


async def pending_decision(hub, run, title):
    """An open task and an idle worker, so the task waits for one prepared assignment decision."""
    task = await publish(hub, title)
    idle = await command(hub, "worker.register")
    intent = await assignment_decision(hub, run["id"])
    assert intent["source_task_id"] == task["id"]
    assert [item["worker_id"] for item in intent["candidates"]["workers"]] == [idle["id"]]
    return intent


@pytest.mark.asyncio
@pytest.mark.parametrize("recover_result", [False, True])
async def test_last_helper_reconciliation_finishes_valid_root_shutdown(case, recover_result):
    hub, task, run, author = case
    artifact_id = await accepted(hub, author, run)
    helper = await pending_decision(hub, run, "Pool opportunity")
    await start_helper(hub, helper["id"], "synthetic-assignment-thread")
    completion = await declaration(hub, task, author, [artifact_id])
    await finalize(hub, completion, author)
    await quiet(hub, author)
    await hub.helper_uncertain(helper["id"], "Interrupted during drain", inference_uncertain=False)
    assert (await project(hub))["state"] == "shutting_down"
    payload = {"intent_id": helper["id"], "process_confirmed_stopped": True, "reason": "Synthetic helper has stopped"}
    if recover_result:
        payload["result"] = {"decision": {"worker": None}}
    await command(hub, "helper.reconcile", payload)
    assert (await project(hub))["state"] == "stopped"
    assert (await hub.run(run["id"]))["state"] == "stopped"


@pytest.mark.asyncio
@pytest.mark.parametrize("root_already_closed", [False, True])
async def test_failed_root_drain_preserves_helper_accounting_hold(case, root_already_closed):
    hub, task, run, author = case
    artifact_id = await accepted(hub, author, run)
    helper = await pending_decision(hub, run, "Available independent work")
    await start_helper(hub, helper["id"], "synthetic-uncertain-helper")
    completion = await declaration(hub, task, author, [artifact_id])
    await finalize(hub, completion, author)
    if root_already_closed:
        await quiet(hub, author)
        assert (await hub.task(task["id"]))["state"] == "done"
    await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Evidence lost before global quiet"})
    if not root_already_closed:
        await quiet(hub, author)
    await hub.helper_uncertain(helper["id"], "Native usage outcome is uncertain", inference_uncertain=True)
    assert (await project(hub))["state"] == "shutting_down"
    reconciled = await command(hub, "helper.reconcile", {"intent_id": helper["id"],
                               "process_confirmed_stopped": True, "reason": "Execution stopped; accounting remains held"})
    assert not reconciled["inference_uncertain"]
    current = await hub.run(run["id"])
    assert current["state"] == "running" and current["usage_uncertain"]
    assert (await project(hub))["state"] == "running"
    if root_already_closed:
        original = await hub.task(task["id"])
        assert original["state"] == "done" and original["current_evidence"]["state"] == "invalid"


@pytest.mark.asyncio
async def test_failed_root_drain_preserves_committed_voluntary_wait(case):
    hub, task, run, author = case
    waiting_task = await publish(hub, "Retain its voluntary wait")
    waiting = await claim(hub, waiting_task, run)
    wait = await legacy_wait(hub, {"reason": "Save work until prerequisites arrive"}, worker(waiting))
    artifact_id = await accepted(hub, author, run)
    completion = await declaration(hub, task, author, [artifact_id])
    await finalize(hub, completion, author)
    await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Invalidate completion"})
    await quiet(hub, waiting)
    await quiet(hub, author)
    current = await hub.worker(waiting["worker_id"])
    ticket = await hub.db.call(lambda c: hub._get(c, "continuations", wait["continuation_id"]))
    assert current["work_intention"] == "waiting" and current["state"] == "suspended"
    assert ticket["state"] == "waiting" and ticket["snapshot"]
    assert (await hub.run(run["id"]))["state"] == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("initially_held", [False, True])
async def test_failed_root_drain_retains_known_quiet_helper_decision(case, initially_held):
    hub, task, run, author = case
    artifact_id = await accepted(hub, author, run)
    helper = await pending_decision(hub, run, "Another opportunity")
    [idle] = helper["candidates"]["workers"]
    await start_helper(hub, helper["id"], "synthetic-known-helper")
    if initially_held:
        await hub.helper_uncertain(helper["id"], "Local decision needs operator recovery", inference_uncertain=False)
    completion = await declaration(hub, task, author, [artifact_id])
    await finalize(hub, completion, author)
    await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Root completion no longer valid"})
    await quiet(hub, author)
    payload = {"intent_id": helper["id"], "result": {"decision": {"worker": None}}}
    if initially_held:
        result = await command(hub, "helper.reconcile", {**payload, "process_confirmed_stopped": True,
                               "reason": "Recover the exact quiet helper result"})
    else:
        result = await command(hub, "helper.result", {**payload, "process_quiet": True}, RUNNER)
    assert result["state"] == "running" and result["raw_result_json"]
    assert (await project(hub))["state"] == "running"
    due = await hub.due_assignment_intents(run["id"])
    assert any(item["id"] == helper["id"] for item in due)
    committed = await hub.complete_assignment(helper["id"], None)
    # The retained answer is committed as given: no idle worker suits the task, so a new one takes it.
    assert committed["choice"] == {"worker": None} and committed["outcome"] == "new_worker"
    assert committed["assignment"]["task_id"] == helper["source_task_id"]
    assert committed["assignment"]["worker_id"] != idle["worker_id"]


@pytest.mark.asyncio
async def test_failed_root_drain_does_not_override_new_preflight_hold(case):
    hub, task, run, author = case
    other_task = await publish(hub, "Independently held execution")
    other = await claim(hub, other_task, run)
    artifact_id = await accepted(hub, author, run)
    completion = await declaration(hub, task, author, [artifact_id])
    await finalize(hub, completion, author)
    await command(hub, "runtime.preflight_hold", {"worker_id": other["worker_id"], "attempt_id": other["id"],
                  "reason": "Independent local activation hold"}, RUNNER)
    await command(hub, "artifact.withdraw", {"artifact_id": artifact_id, "reason": "Invalidate provisional root completion"})
    await quiet(hub, author)
    held = await hub.worker(other["worker_id"])
    # Held while the completed root paused the run: no failure, so its saved work simply waits,
    # and the reopened run admits it again.
    assert held["state"] == "idle" and held["work_intention"] == "working" and held["active_attempt"] is None
    assert (await hub.run(run["id"]))["state"] == "running"
    assert (await project(hub))["state"] == "running"


@pytest.mark.asyncio
async def test_retained_uncertain_helper_is_not_due(tmp_path_factory):
    root = tmp_path_factory.mktemp("hl")
    initialize(root, ProjectConfig())
    hub = Hub(root)
    try:
        run = await command(hub, "run.start")
        helper = await pending_decision(hub, run, "Available task")
        await start_helper(hub, helper["id"], "synthetic-held-thread")
        await command(hub, "helper.result", {"intent_id": helper["id"], "process_quiet": True,
                      "result": {"decision": {"worker": None}}}, RUNNER)
        await hub.helper_uncertain(helper["id"], "Known invalid local decision", inference_uncertain=False)
        assert await hub.due_assignment_intents(run["id"]) == []
        # Its task waits for the held decision's recovery: no second decision and no new worker.
        assert await hub.next_assignment(run["id"]) is None
        assert await hub.db.call(lambda c: c.execute(
            "SELECT count(*) FROM helper_intents WHERE kind='assignment'").fetchone()[0]) == 1
        assert (await hub.run(run["id"]))["state"] == "running"
        held = await hub.db.call(lambda c: hub._helper(c, helper["id"]))
        assert held["raw_result_json"] and not held["inference_uncertain"]
    finally:
        await hub.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("completed_root", [False, True])
async def test_completed_evidence_loss_keeps_history_and_creates_claimable_repair(case, completed_root):
    hub, root, run, author = case
    task = root if completed_root else await publish(hub, "Dependent completion")
    attempt = author if completed_root else await claim(hub, task, run)
    artifact_id = await accepted(hub, attempt, run)
    await finish_ordinary(hub, task["id"], worker(attempt), artifact_ids=[artifact_id])
    await quiet(hub, attempt)
    before = await hub.task(task["id"])
    payload = {"artifact_id": artifact_id, "reason": "Published evidence is no longer valid"}
    await command(hub, "artifact.withdraw", payload)
    await command(hub, "artifact.withdraw", payload)
    after = await hub.task(task["id"])
    assert after["state"] == "done" and after["contract_hash"] == before["contract_hash"]
    assert after["collaboration"]["members"] == before["collaboration"]["members"]
    assert after["current_evidence"]["state"] == "invalid"
    invalidations = after["current_evidence"]["invalidations"]
    assert len(invalidations) == 1 and invalidations[0]["artifact_ids"] == [artifact_id]
    assert len(invalidations[0]["observations"]) == 1
    repair = await hub.task(invalidations[0]["repair_task_id"])
    assert repair["state"] == "ready" and repair["repair_of"]["task_id"] == task["id"]
    assert repair["repair_of"]["origin"] == "system_generated"
    assert repair["repair_of"]["acceptance_condition_keys"] == ["objective"]
    assert repair["acceptance_conditions"]["repair-original:objective"] == task["contract"]["objective"]
    assert repair["contract"]["inputs"] == repair["contract"]["requirements"] == []
    assert not await hub.db.call(lambda c: hub._available_artifact(c, artifact_id))
    if completed_root:
        assert (await project(hub))["state"] == "stopped"
        assert (await hub.run(run["id"]))["state"] == "stopped"
        run = await command(hub, "run.start")
    assigned = await claim(hub, repair, run)
    assert assigned["task_id"] == repair["id"]
    with pytest.raises(HubError) as incomplete:
        await declaration(hub, repair, assigned, [artifact_id])
    assert incomplete.value.code == "ACCEPTANCE_MAP_REQUIRED"


@pytest.mark.asyncio
async def test_new_loss_after_terminal_repair_creates_new_obligation(case):
    hub, _, run, _ = case
    task = await publish(hub, "Complete with two independent artifacts")
    attempt = await claim(hub, task, run)
    first = await accepted(hub, attempt, run)
    second = await accepted(hub, attempt, run)
    # The second artifact is required by the contribution gate even though the
    # explicit acceptance-condition mapping cites the first result only.
    await finish_ordinary(hub, task["id"], worker(attempt), artifact_ids=[first])
    await quiet(hub, attempt)
    first_payload = {"artifact_id": first, "reason": "First invalidation"}
    await command(hub, "artifact.withdraw", first_payload)
    original = await hub.task(task["id"])
    requirement = {"task_id": task["id"], "task_revision": 1, "policy": "general.peer@1"}
    assert not await hub.db.call(lambda c: hub._requirement(c, requirement))
    assert await hub.db.call(lambda c: hub._requirement_artifacts(c, [requirement])) == []
    repair_id = original["current_evidence"]["invalidations"][0]["repair_task_id"]
    await command(hub, "task.retire", {"task_id": repair_id, "revision": 1, "reason": "Repair attempt abandoned"})
    await command(hub, "artifact.withdraw", first_payload)
    assert len((await hub.task(task["id"]))["current_evidence"]["invalidations"]) == 1
    await command(hub, "artifact.withdraw", {"artifact_id": second, "reason": "Distinct later invalidation"})
    current = await hub.task(task["id"])
    records = current["current_evidence"]["invalidations"]
    assert len(records) == 2 and records[0]["repair_task_id"] != records[1]["repair_task_id"]
    assert (await hub.task(records[1]["repair_task_id"]))["state"] == "ready"
    assert current["state"] == "done" and current["current_evidence"]["state"] == "invalid"


@pytest.mark.asyncio
async def test_manual_task_policy_cannot_be_published(tmp_path_factory):
    path = tmp_path_factory.mktemp("mp")
    initialize(path, ProjectConfig())
    hub = Hub(path)
    try:
        with pytest.raises(HubError, match="general.manual@1 is retired"):
            await publish(hub, "Rejected new manual task", acceptance_policy="general.manual@1")
        run = await command(hub, "run.start")
        current = await publish(hub, "Current peer task")
        assert current["contract"]["acceptance_policy"] == "general.peer@1"
        assert (await claim(hub, current, run))["task_id"] == current["id"]
    finally:
        await hub.close()


@pytest.mark.asyncio
async def test_repair_preserves_maximum_original_conditions_without_admission_dependencies(case):
    hub, _, run, author = case
    prerequisite = await accepted(hub, author, run)
    task = await publish(hub, "Maximum original acceptance conditions",
                         constraints=["Original constraint " + str(index) for index in range(100)],
                         requirements=[{"key": "original-" + str(index), "artifact_id": prerequisite,
                                        "policy": "general.peer@1"} for index in range(128)])
    attempt = await claim(hub, task, run)
    result = await accepted(hub, attempt, run)
    original_conditions = await hub.db.call(lambda c: hub._conditions(c, hub._task(c, task["id"])))
    assert len(original_conditions) == 229
    await finish_ordinary(hub, task["id"], worker(attempt), artifact_ids=[result])
    await quiet(hub, attempt)
    await command(hub, "artifact.withdraw", {"artifact_id": prerequisite, "reason": "Invalidate the old admission basis"})
    original = await hub.task(task["id"])
    repair = await hub.task(original["current_evidence"]["invalidations"][0]["repair_task_id"])
    actual_conditions = await hub.db.call(lambda c: hub._conditions(c, hub._task(c, repair["id"])))
    assert {key.removeprefix("repair-original:"): value for key, value in actual_conditions.items()
            if key.startswith("repair-original:")} == original_conditions
    assert repair["contract"]["inputs"] == repair["contract"]["requirements"] == []
    assert (await claim(hub, repair, run))["task_id"] == repair["id"]


@pytest.mark.asyncio
async def test_original_requirement_loss_invalidates_later_completed_revision(case):
    hub, _, run, author = case
    original_input = await accepted(hub, author, run)
    current_input = await accepted(hub, author, run)
    task = await publish(hub, "Maintain original and current acceptance dependencies", requirements=[{
        "key": "original-input", "artifact_id": original_input, "policy": "general.peer@1"}])
    revised = await command(hub, "task.revise", {"task_id": task["id"], "revision": 1,
        "reason": "Change execution dependency while retaining original acceptance",
        "contract": {**task["contract"], "requirements": [{"key": "current-input",
                     "artifact_id": current_input, "policy": "general.peer@1"}]}})
    attempt = await claim(hub, revised, run)
    result = await accepted(hub, attempt, run)
    await finish_ordinary(hub, task["id"], worker(attempt), artifact_ids=[result])
    await quiet(hub, attempt)
    await command(hub, "artifact.withdraw", {"artifact_id": original_input, "reason": "Original acceptance dependency lost"})
    assert (await hub.artifact(result))["status"] == "accepted"
    current = await hub.task(task["id"])
    assert current["state"] == "done" and current["revision"] == 2
    assert current["current_evidence"]["invalidations"][0]["artifact_ids"] == [original_input]


@pytest.mark.asyncio
async def test_completion_requirements_propagate_without_artifact_dependency_edges(case):
    hub, _, run, _ = case

    async def completed(title, requirements):
        task = await publish(hub, title, requirements=requirements)
        attempt = await claim(hub, task, run)
        artifact_id = await accepted(hub, attempt, run)
        await finish_ordinary(hub, task["id"], worker(attempt), artifact_ids=[artifact_id])
        await quiet(hub, attempt)
        return task, artifact_id

    first, first_artifact = await completed("Completed source", [])
    second, second_artifact = await completed("Completion depends on source", [{
        "key": "source-task", "task_id": first["id"], "task_revision": 1, "policy": "general.peer@1"}])
    third, third_artifact = await completed("Completion depends on second task", [{
        "key": "second-task", "task_id": second["id"], "task_revision": 1, "policy": "general.peer@1"}])
    # Isolate retained task-level dependency metadata from the artifact graph.
    # These generic storage fixtures assert no mathematical validity.
    await hub.db.call(lambda c: c.execute("UPDATE artifacts SET input_refs_json='[]' WHERE id IN (?,?)",
                                        (second_artifact, third_artifact)), write=True)
    await command(hub, "artifact.withdraw", {"artifact_id": first_artifact, "reason": "Invalidate original source"})
    assert (await hub.artifact(second_artifact))["status"] == "accepted"
    assert (await hub.artifact(third_artifact))["status"] == "accepted"
    for original in (first, second, third):
        current = await hub.task(original["id"])
        assert current["state"] == "done"
        invalidation = current["current_evidence"]["invalidations"][0]
        assert invalidation["artifact_ids"] == [first_artifact]
        repair = await hub.task(invalidation["repair_task_id"])
        assert repair["state"] == "ready" and repair["contract"]["requirements"] == []
        assert not await hub.db.call(lambda c: hub._requirement(c, {
            "task_id": original["id"], "task_revision": 1, "policy": "general.peer@1"}))


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["artifact.accept", "task.retire", "task.complete.feedback_unavailable",
                                      "task.complete.cancel", "review.reassign", "continuation.resume", "interruption.resume"])
async def test_forbidden_role_is_rejected_before_inspecting_mutation_target(tmp_path_factory, operation):
    path = tmp_path_factory.mktemp("az")
    initialize(path, ProjectConfig())
    hub = Hub(path)
    try:
        with pytest.raises(HubError) as failure:
            await command(hub, operation, {"task_id": "invented", "artifact_id": "invented",
                          "declaration_id": "invented"}, Actor(role=Role.WORKER))
        assert failure.value.status == 403 and failure.value.code == "FORBIDDEN"
    finally:
        await hub.close()
