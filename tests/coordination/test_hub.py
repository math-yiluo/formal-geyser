from __future__ import annotations

import asyncio
import hashlib
import json
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.hub import Hub
from formal_geyser.coordination.models import (
    Actor,
    Command,
    Limits,
    ProjectConfig,
    Requirement,
    Role,
    TaskContract,
)

from .lifecycle_helpers import select_review
from .publication_helpers import approve as publish_review
from .publication_helpers import file_payload, new_hub
from .wait_helpers import legacy_wait

OP = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)
VALIDATOR = Actor(role=Role.VALIDATOR)


async def command(hub, name, payload=None, actor=OP, key=None):
    if name == "task.revise":
        payload = {"reason": "Update the synthetic execution contract.", **(payload or {})}
        payload.setdefault("revision", (await hub.task(payload["task_id"]))["revision"])
    if name == "artifact.withdraw":
        payload = {"reason": "Revoke the synthetic dependency evidence.", **(payload or {})}
    return await hub.execute(Command(name=name, key=key or uuid4().hex, payload=payload or {}), actor)


def contract(title="Independent task", **kwargs):
    return {"title": title, "objective": "Produce an auditable software artifact.", **kwargs}


async def publish(hub, title="Independent task", **kwargs):
    return await command(hub, "task.publish", {"contract": contract(title, **kwargs)})


async def start(hub, **kwargs):
    kwargs.pop("max_active", None)
    return await command(hub, "run.start", {"limits": kwargs})


async def worker(hub):
    return await command(hub, "worker.register")


async def claim(hub, task, w, run):
    attempt = await command(hub, "task.claim", {
        "task_id": task["id"], "revision": task["revision"], "worker_id": w["id"], "run_id": run["id"]})
    actor = Actor(role=Role.WORKER, worker_id=w["id"], attempt_id=attempt["id"],
                  fencing_token=attempt["fencing_token"])
    return attempt, actor


def checkpoint():
    return {"summary": "Durable checkpoint", "next_action": "Inspect the delivered result", "input_refs": []}


def predicate(task, policy=None):
    policy = policy or task["contract"]["acceptance_policy"]
    return {"mode": "all", "requirements": [
        {"key": "result", "task_id": task["id"], "task_revision": task["revision"], "policy": policy}]}


async def quiet(hub, wid):
    snapshot = hub.blobs.snapshot(hub.workspace(wid))
    return await command(hub, "runtime.quiescent", {"worker_id": wid, "snapshot": snapshot}, RUNNER)


async def submit(hub, actor, *, replaces=None, **data):
    payload = file_payload(hub, actor, data=data)
    if replaces:
        payload["replaces"] = replaces
    await command(hub, "artifact.submit", payload, actor)
    return await quiet(hub, actor.worker_id)


async def accept(hub, artifact_id, *, review="approve", **kwargs):
    """Record a machine verdict; an accepted candidate is then reviewed with the given verdict."""
    original = await hub.artifact(artifact_id)
    policy = kwargs.get("policy", original.get("candidate_binding", {}).get("policy", "general.manual@1"))
    response = await command(hub, "artifact.accept", {
        "artifact_id": artifact_id, "policy": policy, "verdict": "accepted",
        "details": {}, "qualifications": [], **kwargs}, VALIDATOR)
    artifact = await hub.artifact(artifact_id)
    binding = artifact.get("candidate_binding")
    # Only an accepted machine validation starts the candidate's review.
    if (binding and policy == binding["policy"] and kwargs.get("verdict", "accepted") == "accepted"
            and not kwargs.get("qualifications")):
        request = await hub.db.call(lambda c: dict(c.execute(
            "SELECT * FROM review_requests WHERE candidate_id=?", (artifact_id,)).fetchone()))
        if request["state"] == "pending":
            run_id = (await hub.worker(binding["author_id"]))["run_id"]
            reviewer = await select_review(hub, binding["review_task_id"], run_id)
            if review == "approve":
                await publish_review(hub, reviewer)
            else:
                basis = hub._review_basis(binding)
                await command(hub, "review.reject", {**basis, "rationale": "The synthetic reviewer found a defect.",
                    "evidence": ["Inspected the frozen JSON source and failed validation."],
                    "suggestions": ["Correct the rejected source and submit a replacement."]},
                    Actor(role=Role.WORKER, worker_id=reviewer["worker_id"],
                          attempt_id=reviewer["id"], fencing_token=reviewer["fencing_token"]))
                await quiet(hub, reviewer["worker_id"])
    return response


async def close_task(hub, task_id, artifact_id):
    current = await command(hub, "task.complete.inspect", {"task_id": task_id})
    declaration = await command(hub, "task.complete.declare", {
        "task_id": task_id, "revision": current["revision"],
        "reason": "The independent publication fulfills the original synthetic objective.",
        "evidence": {key: {"explanation": "The accepted source satisfies this unchanged condition.",
                           "artifact_ids": [artifact_id]} for key in current["acceptance_conditions"]}})
    if declaration["feedback_state"] == "pending":
        # An operator declaration still gives the ongoing author its opportunity,
        # including when that author is between synthetic native turns.
        participant = await hub.worker(declaration["feedback_worker_id"])
        attempt, actor = await claim(hub, await hub.task(task_id), participant,
                                     await hub.run(participant["run_id"]))
        await command(hub, "task.complete.feedback", {
            "declaration_id": declaration["id"], "conditions": [],
            "checkpoint": checkpoint()}, actor)
        await hub.ordinary_turn_completed(participant["id"], attempt["id"])
    return await command(hub, "task.complete.finalize", {
        "declaration_id": declaration["id"], "decision": "close", "reason": "All required results are published."})


@pytest.fixture
async def hub(tmp_path):
    initialize(tmp_path, ProjectConfig(limits=Limits()))
    result = Hub(tmp_path)
    yield result
    await result.close()


@pytest.fixture
async def file_hub(tmp_path):
    result = await new_hub(tmp_path)
    try:
        await publish(result, "Keep the enclosing test objective open")
        yield result
    finally:
        await result.close()


async def test_atomic_claim_race(hub):
    task, run = await publish(hub), await start(hub)
    workers = [await worker(hub), await worker(hub)]
    outcomes = await asyncio.gather(*(claim(hub, task, w, run) for w in workers), return_exceptions=True)
    assert sum(isinstance(value, tuple) for value in outcomes) == 1
    assert sum(isinstance(value, HubError) for value in outcomes) == 1
    assert (await hub.task(task["id"]))["fencing_counter"] == 1


async def test_idempotent_receipt_and_payload_conflict_survive_reopen(hub):
    payload = {"contract": contract()}
    first = await command(hub, "task.publish", payload, key="publish-once")
    replay = await command(hub, "task.publish", payload, key="publish-once")
    assert replay == first
    with pytest.raises(HubError, match="different data"):
        await command(hub, "task.publish", {"contract": contract("Changed")}, key="publish-once")
    root = hub.root
    await hub.close()
    reopened = Hub(root)
    try:
        assert await command(reopened, "task.publish", payload, key="publish-once") == first
        assert len(await reopened.list_tasks()) == 1
    finally:
        await reopened.close()


async def test_wait_registers_before_slot_is_freed_and_early_result_is_not_lost(file_hub):
    hub = file_hub
    parent, dependency = await publish(hub, "Parent"), await publish(hub, "Dependency")
    run = await start(hub, max_active=2)
    first, second = await worker(hub), await worker(hub)
    _, parent_actor = await claim(hub, parent, first, run)
    _, helper_actor = await claim(hub, dependency, second, run)
    parked = await legacy_wait(hub, {"predicate": predicate(dependency), "checkpoint": checkpoint()}, parent_actor)
    result = await submit(hub, helper_actor, result="ready")
    await accept(hub, result["artifact_id"])
    await close_task(hub, dependency["id"], result["artifact_id"])
    continuation = (await hub.list_continuations())[0]
    assert continuation["state"] == "eligible"
    assert (await hub.worker(first["id"]))["state"] == "draining"
    with pytest.raises(HubError, match="quiescent"):
        await command(hub, "continuation.resume", {"continuation_id": parked["continuation_id"], "run_id": run["id"]}, RUNNER)
    await quiet(hub, first["id"])
    resumed = await command(hub, "continuation.resume", {
        "continuation_id": parked["continuation_id"], "run_id": run["id"]}, RUNNER)
    assert resumed["worker_id"] == first["id"]
    assert resumed["fencing_token"] == 2
    assert resumed["checkpoint"] == checkpoint()


async def test_dependency_handoff_preserves_parent_without_global_slot_gate(file_hub):
    hub = file_hub
    parent, helper = await publish(hub, "Parent"), await publish(hub, "Helper")
    run = await start(hub, max_active=1)
    first, second = await worker(hub), await worker(hub)
    _, original = await claim(hub, parent, first, run)
    path = hub.workspace(first["id"]) / "progress.txt"
    path.write_text("preserve this", encoding="utf-8")
    wait = await legacy_wait(hub, {"predicate": predicate(helper), "checkpoint": checkpoint()}, original)
    _, helper_actor = await claim(hub, helper, second, run)
    await quiet(hub, first["id"])
    output = await submit(hub, helper_actor, result="done")
    await accept(hub, output["artifact_id"])
    await close_task(hub, helper["id"], output["artifact_id"])
    resumed = await command(hub, "continuation.resume", {
        "continuation_id": wait["continuation_id"], "run_id": run["id"]}, RUNNER)
    assert resumed["worker_id"] == original.worker_id
    assert path.read_text(encoding="utf-8") == "preserve this"
    assert hub.blobs.get_json(resumed["snapshot"])["files"][0]["path"] == "progress.txt"


async def test_wait_accepts_preexisting_result_without_missing_wake(file_hub):
    hub = file_hub
    dep, parent = await publish(hub, "Dependency"), await publish(hub, "Parent")
    run, first, second = await start(hub), await worker(hub), await worker(hub)
    _, dep_actor = await claim(hub, dep, first, run)
    result = await submit(hub, dep_actor)
    await accept(hub, result["artifact_id"])
    await close_task(hub, dep["id"], result["artifact_id"])
    _, parent_actor = await claim(hub, parent, second, run)
    await legacy_wait(hub, {"predicate": predicate(dep), "checkpoint": checkpoint()}, parent_actor)
    assert (await hub.list_continuations())[0]["state"] == "eligible"


async def test_a_new_wait_replaces_the_workers_earlier_waits_on_the_task(file_hub):
    # In the Linux pilot a worker woken by one wait paused again while its older waits stayed
    # open; once they held, each new pause woke it at once.
    hub = file_hub
    parent, first, second = await publish(hub, "Parent"), await publish(hub, "First"), await publish(hub, "Second")
    run, author, other = await start(hub), await worker(hub), await worker(hub)
    attempt, actor = await claim(hub, parent, author, run)
    earlier, others = str(uuid4()), str(uuid4())  # The author's wait from an earlier pause, and another worker's.
    for continuation, worker_id in ((earlier, author["id"]), (others, other["id"])):
        await hub.db.call(lambda c, continuation=continuation, worker_id=worker_id: c.execute(
            "INSERT INTO continuations(id,task_id,task_revision,preferred_worker,predicate_json,checkpoint_json,state,"
            "origin_attempt,created_at) VALUES(?,?,?,?,?,?,'waiting',?,0)",
            (continuation, parent["id"], parent["revision"], worker_id, json.dumps(predicate(first)),
             json.dumps(checkpoint()), attempt["id"])))
    later = await legacy_wait(hub, {"predicate": predicate(second), "checkpoint": checkpoint()}, actor)
    states = {item["id"]: item["state"] for item in await hub.list_continuations()}
    assert states == {earlier: "superseded_by_worker", others: "waiting", later["continuation_id"]: "waiting"}


async def test_startup_keeps_only_each_workers_newest_wait_on_a_task(file_hub):
    hub = file_hub
    parent, dependency = await publish(hub, "Parent"), await publish(hub, "Dependency")
    run, author = await start(hub), await worker(hub)
    _, actor = await claim(hub, parent, author, run)
    kept = await legacy_wait(hub, {"predicate": predicate(dependency), "checkpoint": checkpoint()}, actor)
    await quiet(hub, author["id"])
    older = str(uuid4())  # Left by code that kept a worker's earlier waits.
    await hub.db.call(lambda c: c.execute(
        "INSERT INTO continuations(id,task_id,task_revision,preferred_worker,predicate_json,checkpoint_json,state,"
        "origin_attempt,created_at) SELECT ?,task_id,task_revision,preferred_worker,predicate_json,checkpoint_json,"
        "'eligible',origin_attempt,created_at-1 FROM continuations WHERE id=?", (older, kept["continuation_id"])))
    root = hub.root
    await hub.close()
    reopened = Hub(root)
    try:
        states = {item["id"]: item["state"] for item in await reopened.list_continuations()}
        assert states == {older: "superseded_by_worker", kept["continuation_id"]: "waiting"}
    finally:
        await reopened.close()


async def test_candidate_wrong_policy_and_qualified_acceptance_do_not_wake(file_hub):
    hub = file_hub
    dep, parent = await publish(hub, "Dependency"), await publish(hub, "Parent")
    run, first, second = await start(hub), await worker(hub), await worker(hub)
    _, parent_actor = await claim(hub, parent, first, run)
    await legacy_wait(hub, {"predicate": predicate(dep), "checkpoint": checkpoint()}, parent_actor)
    await quiet(hub, first["id"])
    _, dep_actor = await claim(hub, dep, second, run)
    output = await submit(hub, dep_actor)
    assert (await hub.list_continuations())[0]["state"] == "waiting"
    await accept(hub, output["artifact_id"], policy="different.policy@1")
    assert (await hub.list_continuations())[0]["state"] == "waiting"
    await accept(hub, output["artifact_id"], qualifications=["Unresolved condition"])
    assert (await hub.list_continuations())[0]["state"] == "waiting"
    await accept(hub, output["artifact_id"])
    assert (await hub.list_continuations())[0]["state"] == "waiting"
    await close_task(hub, dep["id"], output["artifact_id"])
    assert (await hub.list_continuations())[0]["state"] == "eligible"


async def test_a_wake_up_reads_only_the_requirements_that_may_hold(file_hub):
    # Every suspension, completion and acceptance wakes waiting work: in the Linux pilot 634
    # waiting requirements were read one by one, 3 s a wake-up, though almost none could hold.
    hub = file_hub
    dependencies = [await publish(hub, f"Dependency {number}") for number in range(4)]
    requirements = [{**predicate(task)["requirements"][0], "key": f"result-{number}"}
                    for number, task in enumerate(dependencies)]
    run, author = await start(hub), await worker(hub)
    parents = {}
    for mode in ("all", "any"):
        parents[mode], waiter = await publish(hub, f"Parent waiting for {mode}"), await worker(hub)
        _, parent_actor = await claim(hub, parents[mode], waiter, run)
        await legacy_wait(hub, {"predicate": {"mode": mode, "requirements": requirements},
                                         "checkpoint": checkpoint()}, parent_actor)
        await quiet(hub, waiter["id"])
    _, dependency_actor = await claim(hub, dependencies[0], author, run)
    output = await submit(hub, dependency_actor)
    await accept(hub, output["artifact_id"])
    await close_task(hub, dependencies[0]["id"], output["artifact_id"])
    states = {item["task_id"]: item["state"] for item in await hub.list_continuations()}
    assert states == {parents["all"]["id"]: "waiting", parents["any"]["id"]: "eligible"}
    statements: list[str] = []

    def wake(c):
        c.set_trace_callback(statements.append)
        try:
            hub._wake(c)
        finally:
            c.set_trace_callback(None)
    await hub.db.call(wake, write=True)
    # One read for every requirement at once: three of the four cannot hold, so neither can "all".
    assert len([sql for sql in statements if "JOIN evidence" in sql]) == 1
    assert {item["task_id"]: item["state"] for item in await hub.list_continuations()} == states


async def test_stale_worker_cannot_mutate_after_revision_change(hub):
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    revised = await command(hub, "task.revise", {"task_id": task["id"], "revision": 1,
                                              "contract": contract("Revised")})
    assert revised["revision"] == 2
    with pytest.raises(HubError):
        await command(hub, "artifact.submit", {"kind": "result", "data": {}}, actor)
    assert (await hub.worker(w["id"]))["state"] == "draining"
    assert (await quiet(hub, w["id"]))["directive"] == "superseded"
    assert (await hub.task(task["id"]))["state"] == "ready"


async def test_elapsed_time_neither_expires_nor_frees_mutable_workspace(hub):
    now = [100000]
    hub.clock = lambda: now[0]
    task, run, first, second = await publish(hub), await start(hub), await worker(hub), await worker(hub)
    _, actor = await claim(hub, task, first, run)
    now[0] += 1000000
    await command(hub, "task.checkpoint", {"checkpoint": checkpoint()}, actor)
    with pytest.raises(HubError, match="authorized collaborator"):
        await claim(hub, task, second, run)
    with pytest.raises(HubError, match="stopped"):
        await command(hub, "runtime.abandon", {"worker_id": first["id"]}, RUNNER)
    await command(hub, "runtime.abandon", {"worker_id": first["id"], "quiescent": True}, RUNNER)
    with pytest.raises(HubError):
        await claim(hub, task, second, run)
    await command(hub, "worker.recover", {"worker_id": first["id"], "process_confirmed_stopped": True,
                  "reason": "The exact original workspace and stopped synthetic execution are recoverable."})
    attempt, _ = await claim(hub, task, first, run)
    assert attempt["fencing_token"] == 2


async def test_worker_cannot_accept_results_or_start_runs(hub):
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    for name, payload in [("run.start", {}),
                          ("artifact.accept", {"artifact_id": "invented"})]:
        with pytest.raises(HubError) as caught:
            await command(hub, name, payload, actor)
        assert caught.value.status == 403


async def test_withdrawal_propagates_through_declared_input_graph(hub):
    first = await command(hub, "artifact.register", {"kind": "source", "data": {"v": 1}})
    second = await command(hub, "artifact.register", {"kind": "derived", "data": {},
                                                    "input_refs": [first["id"]]})
    third = await command(hub, "artifact.register", {"kind": "derived", "data": {},
                                                   "input_refs": [second["id"]]})
    for a in (first, second, third):
        await accept(hub, a["id"])
    response = await command(hub, "artifact.withdraw", {"artifact_id": first["id"], "reason": "basis changed"})
    assert set(response["withdrawn"]) == {first["id"], second["id"], third["id"]}
    for a in (first, second, third):
        record = await hub.artifact(a["id"])
        assert record["status"] == "withdrawn"
        assert record["evidence"][0]["verdict"] == "accepted"
    with pytest.raises(HubError, match="cannot be accepted"):
        await accept(hub, first["id"])


async def test_withdrawal_makes_a_wait_on_the_artifact_reconsider(hub):
    source = await command(hub, "artifact.register", {"kind": "source", "data": {}})
    await accept(hub, source["id"])
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    await legacy_wait(hub, {
        "predicate": {"mode": "all", "requirements": [{"key": "input", "artifact_id": source["id"],
                                                    "policy": "general.manual@1"}]},
        "checkpoint": checkpoint()}, actor)
    await quiet(hub, w["id"])
    await command(hub, "artifact.withdraw", {"artifact_id": source["id"]})
    # The artifact can never be accepted again: waiting for it is pointless.
    [wait] = await hub.list_continuations()
    assert wait["state"] == "reconsider" and wait["wake_reason"]


async def test_a_recorded_replacement_meets_requirements_naming_the_artifact_it_replaces(hub):
    # In the pilot a statement was rejected for one Markdown remark and resubmitted with the same
    # Lean: the task that had required the first could never be completed.
    original = await command(hub, "artifact.register", {"kind": "statement", "data": {"lean": 1}})
    replacement = await command(hub, "artifact.register", {"kind": "statement", "data": {"lean": 1, "notes": 2}})
    await accept(hub, original["id"])
    await command(hub, "artifact.withdraw", {"artifact_id": original["id"]})
    requirement = {"key": "p1-statement", "artifact_id": original["id"], "policy": "general.manual@1"}

    def met(c):
        return hub._requirement(c, requirement), hub._possible_requirements(c, [requirement])

    def record(original_id, replacement_id, reason="Resubmitted with the same Lean", actor=OP):
        return command(hub, "artifact.record_replacement",
                       {"original_id": original_id, "replacement_id": replacement_id, "reason": reason}, actor)
    assert await hub.db.call(met) == (False, set())
    with pytest.raises(HubError) as error:
        await record(original["id"], replacement["id"])
    assert error.value.code == "REPLACEMENT_NOT_ACCEPTED"
    await accept(hub, replacement["id"])
    for args, code in (((replacement["id"], original["id"]), "ORIGINAL_NOT_REPLACEABLE"),
                       ((original["id"], replacement["id"], " "), "INVALID_INPUT"),
                       ((original["id"], original["id"]), "INVALID_INPUT")):
        with pytest.raises(HubError) as error:
            await record(*args)
        assert error.value.code == code
    with pytest.raises(HubError):
        await record(original["id"], replacement["id"], actor=RUNNER)
    assert await record(original["id"], replacement["id"]) == {
        "original_id": original["id"], "replacement_id": replacement["id"]}
    assert await hub.db.call(met) == (True, {("artifact", original["id"])})


async def test_a_requirement_names_no_policy_and_a_former_policy_is_ignored(hub):
    # In the Linux pilot workers named the task's acceptance_policy for every requirement, while
    # a proof task's outputs were mostly definitions and statements: such a wait never held.
    artifact = await command(hub, "artifact.register", {"kind": "statement", "data": {"lean": 1}})
    plain = {"key": "statement", "artifact_id": artifact["id"]}
    wrong = {**plain, "policy": "lean.proof@1"}  # As a stored predicate, or a habit, still names one.
    assert Requirement.model_validate(wrong).model_dump() == Requirement.model_validate(plain).model_dump()
    assert "policy" not in Requirement.model_validate(wrong).model_dump()

    def met(c):
        return [hub._requirement(c, r) for r in (plain, wrong)], hub._possible_requirements(c, [plain, wrong])
    assert await hub.db.call(met) == ([False, False], set())
    await accept(hub, artifact["id"])  # Under its own policy, general.manual@1.
    assert await hub.db.call(met) == ([True, True], {("artifact", artifact["id"])})
    await accept(hub, artifact["id"], policy="other.policy@1", verdict="rejected")
    assert await hub.db.call(met) == ([True, True], {("artifact", artifact["id"])})  # Unrelated: still accepted.
    # A task contract takes the former field and ignores it, in its schema too.
    contract = {"title": "T", "objective": "O", "requirements": [wrong]}
    Draft202012Validator(TaskContract.model_json_schema()).validate(contract)
    assert TaskContract.model_validate(contract).requirements[0].model_dump() == {
        "key": "statement", "task_id": None, "task_revision": None, "artifact_id": artifact["id"]}


def test_a_former_requirement_policy_is_no_change_of_substance():
    import sqlite3

    from formal_geyser.coordination.collaboration import (
        AT_LEAST_SUBSTANCE_SQL,
        SAME_SUBSTANCE_SQL,
        contract_substance,
    )
    stored = {"title": "T", "objective": "O", "requirements": [
        {"key": "k", "task_id": "t1", "task_revision": 1, "artifact_id": None, "policy": "lean.proof@1"}]}
    revised = {"title": "T2", "objective": "O", "requirements": [
        {"key": "k", "task_id": "t1", "task_revision": 1, "artifact_id": None}]}
    more = {**revised, "requirements": [*revised["requirements"], {"key": "j", "task_id": "t2", "task_revision": 1,
                                                                   "artifact_id": None}]}
    assert contract_substance(stored) == contract_substance(revised) != contract_substance(more)
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE r(id INTEGER, contract_json TEXT)")
    for number, contract in enumerate((stored, revised, more)):
        c.execute("INSERT INTO r VALUES(?,?)", (number, json.dumps(contract, sort_keys=True)))

    def holds(sql, a, b):
        return c.execute("SELECT " + sql.format(a="a", b="b") + " FROM r a, r b WHERE a.id=? AND b.id=?",
                         (a, b)).fetchone()[0] == 1
    assert holds(SAME_SUBSTANCE_SQL, 1, 0) and holds(SAME_SUBSTANCE_SQL, 0, 1)
    assert not holds(SAME_SUBSTANCE_SQL, 2, 0)
    assert holds(AT_LEAST_SUBSTANCE_SQL, 1, 0) and holds(AT_LEAST_SUBSTANCE_SQL, 2, 0)
    assert not holds(AT_LEAST_SUBSTANCE_SQL, 0, 2)


async def rejected_candidate(hub, run):
    """A task, its author and the author's first candidate for it, rejected by review."""
    task, author = await publish(hub), await worker(hub)
    _, actor = await claim(hub, task, author, run)
    first = (await submit(hub, actor, attempt=1))["artifact_id"]
    await accept(hub, first, review="reject")
    policy = (await hub.artifact(first))["candidate_binding"]["policy"]
    return task, author, first, {"key": "result", "artifact_id": first, "policy": policy}


async def test_a_requirement_naming_a_corrected_candidate_is_met_by_its_newest_version(file_hub):
    # In the Linux pilot 189 of 207 rejected or withdrawn candidates were corrected with
    # `replaces`, yet a task that had required one of them could never be claimed or completed.
    hub = file_hub
    run = await start(hub)
    task, author, first, requirement = await rejected_candidate(hub, run)

    def met(c):
        return hub._requirement(c, requirement), hub._possible_requirements(c, [requirement])
    _, actor = await claim(hub, task, author, run)
    second = (await submit(hub, actor, replaces=first, attempt=2))["artifact_id"]
    assert await hub.db.call(met) == (False, set())  # The correction still awaits its review.
    await accept(hub, second)
    assert await hub.db.call(met) == (True, {("artifact", first)})
    dependent, other = await publish(hub, "Dependent task", requirements=[requirement]), await worker(hub)
    _, actor = await claim(hub, dependent, other, run)
    result = (await submit(hub, actor, attempt=3))["artifact_id"]
    assert second in (await hub.artifact(result))["input_refs"]
    await accept(hub, result)
    await close_task(hub, dependent["id"], result)
    assert (await hub.task(dependent["id"]))["state"] == "done"


async def test_a_requirement_on_a_revision_is_met_by_one_that_only_adds_requirements(file_hub):
    # In the Linux pilot a task's revision 3 only added a requirement to revision 1, and a task
    # whose original revision required revision 1 could never be completed with revision 3's proof.
    hub = file_hub
    source = await command(hub, "artifact.register", {"kind": "source", "data": {}})
    await accept(hub, source["id"])
    added = {"key": "source", "artifact_id": source["id"], "policy": "general.manual@1"}
    run = await start(hub)

    async def proved(title, first, second):
        """A task published as `first`, revised to `second`, proved and completed; the requirement
        on its first revision."""
        task = await publish(hub, title, **first)
        await command(hub, "task.revise", {"task_id": task["id"], "contract": contract(title, **second)})
        _, actor = await claim(hub, await hub.task(task["id"]), await worker(hub), run)
        result = await submit(hub, actor)
        await accept(hub, result["artifact_id"])
        await close_task(hub, task["id"], result["artifact_id"])
        assert (await hub.task(task["id"]))["state"] == "done"
        return {"key": "result", "task_id": task["id"], "task_revision": 1,
                "policy": task["contract"]["acceptance_policy"]}
    stronger = await proved("Strengthened provider", {}, {"requirements": [added]})
    weaker = await proved("Weakened provider", {"requirements": [added]}, {})
    assert await hub.db.call(lambda c: (hub._requirement(c, stronger), hub._requirement(c, weaker))) == (True, False)


async def test_a_wait_naming_a_rejected_candidate_follows_its_corrections(file_hub):
    hub = file_hub
    run = await start(hub)
    task, author, first, requirement = await rejected_candidate(hub, run)

    async def wait_on_first(objective):
        dependent, waiter = await publish(hub, "Dependent task", objective=objective), await worker(hub)
        _, actor = await claim(hub, dependent, waiter, run)
        await legacy_wait(hub, {"predicate": {"mode": "all", "requirements": [requirement]},
                                         "checkpoint": checkpoint()}, actor)
        await quiet(hub, waiter["id"])
        return dependent["id"]

    async def wait_of(task_id):
        [wait] = [item for item in await hub.list_continuations() if item["task_id"] == task_id]
        return wait["state"], wait["wake_reason"]

    async def correct(predecessor, review):
        _, actor = await claim(hub, task, author, run)
        corrected = (await submit(hub, actor, replaces=predecessor))["artifact_id"]
        await accept(hub, corrected, review=review)
        return corrected
    rejected_wait = await wait_on_first("Use the first correction of the software artifact.")
    second = await correct(first, "reject")
    # The version the wait now stands for was rejected: its worker reconsiders it.
    assert await wait_of(rejected_wait) == ("reconsider", "candidate_rejected")
    accepted_wait = await wait_on_first("Use the second correction of the software artifact.")
    assert await wait_of(accepted_wait) == ("waiting", None)
    await correct(second, "approve")
    assert await wait_of(accepted_wait) == ("eligible", "requirements_satisfied")


async def test_a_resubmission_of_the_same_files_corrects_the_rejected_candidate_it_names_none(file_hub):
    # In the Linux pilot 5 corrections of the same files by the same author named no `replaces`;
    # of the 192 that named one, 176 had such a candidate, always the one named.
    hub = file_hub
    run = await start(hub)
    task, author = await publish(hub), await worker(hub)

    async def contribute(path, attempt):
        _, actor = await claim(hub, task, author, run)
        await command(hub, "artifact.submit", file_payload(hub, actor, data={"attempt": attempt}, path=path), actor)
        return (await quiet(hub, author["id"]))["artifact_id"]

    def predecessor(c, artifact_id):
        return c.execute("SELECT predecessor_id FROM artifact_obligations WHERE artifact_id=?", (artifact_id,)).fetchone()[0]
    first = await contribute("results/statement.json", 1)
    await accept(hub, first, review="reject")
    other = await contribute("results/other.json", 2)
    assert await hub.db.call(lambda c: predecessor(c, other)) is None
    _, actor = await claim(hub, task, author, run)
    second = await command(hub, "artifact.stage", file_payload(
        hub, actor, data={"attempt": 3}, path="results/statement.json"), actor)
    assert second["replaces"] == first and first in second["replaces_note"]
    assert await hub.db.call(lambda c: predecessor(c, second["artifact_id"])) == first
    [event] = [item for item in await hub.events(limit=1000) if item["kind"] == "artifact.replacement_inferred"]
    assert (event["object_id"], event["payload"]) == (second["artifact_id"], {"replaces": first})
    await accept(hub, second["artifact_id"], review="reject")
    payload = {"kind": "software.result", "data": {}, "repository_paths": ["results/statement.json"]}

    def corrected(c, worker_id):
        return hub._corrected_candidate(c, {"worker_id": worker_id}, hub._task(c, task["id"]), payload)
    assert await hub.db.call(lambda c: corrected(c, author["id"])) == second["artifact_id"]
    # Only a candidate its author contributed to, and only when there is one: never a guess.
    assert await hub.db.call(lambda c: corrected(c, "another-worker")) is None
    await hub.db.call(lambda c: c.execute("UPDATE artifact_obligations SET predecessor_id=NULL WHERE artifact_id=?",
                                          (second["artifact_id"],)))
    assert await hub.db.call(lambda c: corrected(c, author["id"])) is None


async def test_turn_admission_and_monotonic_usage(hub):
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    await claim(hub, task, w, run)
    await command(hub, "worker.bind_session", {"worker_id": w["id"], "thread_id": "thread"}, RUNNER)
    intent = await command(hub, "turn.reserve", {"worker_id": w["id"]}, RUNNER)
    await command(hub, "turn.bind", {"intent_id": intent["id"], "remote_turn_id": "turn"}, RUNNER)
    for total in (100, 100, 150):
        await command(hub, "usage.record", {"worker_id": w["id"], "sample": {
            "total": {"totalTokens": total, "inputTokens": 100, "cachedInputTokens": 80,
                      "outputTokens": total-100, "reasoningOutputTokens": 10}}}, RUNNER)
    await command(hub, "worker.compacted", {"worker_id": w["id"]}, RUNNER)
    await command(hub, "usage.record", {"worker_id": w["id"], "sample": {"totalTokens": 160}}, RUNNER)
    await command(hub, "turn.finish", {"intent_id": intent["id"], "status": "completed"}, RUNNER)
    state = await hub.run(run["id"])
    assert state["tokens_used"] == 160
    assert state["turns_used"] == 1
    for number in range(1, 25):
        intent = await command(hub, "turn.reserve", {"worker_id": w["id"]}, RUNNER)
        await command(hub, "turn.bind", {
            "intent_id": intent["id"], "remote_turn_id": f"turn-{number}"}, RUNNER)
        await command(hub, "usage.record", {
            "worker_id": w["id"], "sample": {"totalTokens": number * 100000}}, RUNNER)
        await command(hub, "turn.finish", {"intent_id": intent["id"], "status": "completed"}, RUNNER)
    state = await hub.run(run["id"])
    assert state["state"] == "running"
    assert state["turns_used"] == 25
    assert state["tokens_used"] == 2400000
    packet = await hub.context(w["id"])
    assert packet["execution"] == {"active_workers": 1}
    assert packet["usage"] == {"turns_used": 25, "tokens_used": 2400000}
    assert "budget" not in packet
    assert not any(event["kind"] == "run.budget_exhausted" for event in await hub.events())


async def test_uncertain_turn_holds_execution_until_operator_reconciliation(hub):
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    await claim(hub, task, w, run)
    intent = await command(hub, "turn.reserve", {"worker_id": w["id"]}, RUNNER)
    await command(hub, "turn.finish", {"intent_id": intent["id"], "status": "unknown"}, RUNNER)
    await command(hub, "runtime.abandon", {"worker_id": w["id"], "quiescent": True}, RUNNER)
    assert (await hub.run(run["id"]))["usage_uncertain"] == 1
    resumed = await command(hub, "run.resume", {"run_id": run["id"]})
    assert resumed["state"] == "running" and resumed["usage_uncertain"]
    with pytest.raises(HubError):
        await claim(hub, task, w, run)
    await command(hub, "run.reconcile_usage", {"run_id": run["id"], "tokens_used": 125,
                                            "reason": "Operator reconciled provider usage"})
    await command(hub, "run.resume", {"run_id": run["id"]})
    assert (await hub.run(run["id"]))["tokens_used"] == 125


async def test_usage_decrease_does_not_refund_or_silently_reset(hub):
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    await claim(hub, task, w, run)
    for total in (100, 50):
        result = await command(hub, "usage.record", {"worker_id": w["id"], "sample": {"totalTokens": total}}, RUNNER)
    assert result["uncertain"] is True
    assert (await hub.run(run["id"]))["tokens_used"] == 100


async def test_context_delivery_replays_until_ack_and_is_task_scoped(hub):
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    unrelated = await publish(hub, "Unrelated")
    packet = await hub.context(w["id"])
    replay = await hub.context(w["id"])
    assert replay["updates"] == packet["updates"]
    assert all(event["object_id"] != unrelated["id"] for event in packet["updates"])
    await command(hub, "context.ack", {"worker_id": w["id"], "packet_id": packet["packet_id"]}, actor)
    assert (await hub.context(w["id"]))["updates"] == []
    # Compaction keeps the conversation: its epoch, earlier receipts and the delivery cursor
    # continue, so nothing delivered is sent again. The compaction is recorded, not delivered.
    await command(hub, "worker.compacted", {"worker_id": w["id"]}, RUNNER)
    assert (await hub.worker(w["id"]))["context_epoch"] == packet["context_epoch"]
    assert [event["object_id"] for event in await hub.events(limit=1000)
            if event["kind"] == "worker.context_compacted"] == [w["id"]]
    await command(hub, "context.ack", {"worker_id": w["id"], "packet_id": replay["packet_id"]}, actor)
    assert (await hub.context(w["id"]))["updates"] == []
    # A new conversation starts a new epoch: an earlier receipt is stale, and what was already
    # delivered is not sent again (its cursor carries over; its view shows the current state).
    await command(hub, "worker.bind_session", {"worker_id": w["id"], "thread_id": "new-thread"}, RUNNER)
    with pytest.raises(HubError, match="epoch"):
        await command(hub, "context.ack", {"worker_id": w["id"], "packet_id": replay["packet_id"]}, actor)
    assert (await hub.context(w["id"]))["updates"] == []


async def test_context_pages_all_unacknowledged_updates_without_silent_drop(hub):
    task, run, w = await publish(hub), await start(hub, delta_bytes=1024), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    for number in range(30):
        await hub.db.call(lambda c, number=number: hub._event(c, "task.observation", task["id"],
            {"reason": f"Outstanding observation {number}"}, task["id"], w["id"]), write=True)
    delivered = []
    for _ in range(100):
        packet = await hub.context(w["id"])
        delivered.extend(packet["updates"])
        await command(hub, "context.ack", {"worker_id": w["id"], "packet_id": packet["packet_id"]}, actor)
        if not packet["has_more_updates"]:
            break
    reasons = {event["payload"].get("reason") for event in delivered}
    assert {f"Outstanding observation {number}" for number in range(30)} <= reasons
    assert len({event["sequence"] for event in delivered}) == len(delivered)


async def test_large_optional_context_is_referenced_and_readable(hub):
    task, run, w = await publish(hub, context="A" * 15000), await start(hub), await worker(hub)
    await claim(hub, task, w, run)
    packet = await hub.context(w["id"])
    sha = packet["task"]["context_material_sha256"]
    path = hub.workspace(w["id"]) / packet["task"]["context_material_path"]
    body = path.read_bytes()
    assert hashlib.sha256(body).hexdigest() == sha
    assert json.loads(body) == "A" * 15000


async def test_impossible_mandatory_context_fails_explicitly(hub):
    task, run, w = await publish(hub, objective="x" * 2000), await start(hub, packet_bytes=512), await worker(hub)
    await claim(hub, task, w, run)
    with pytest.raises(HubError) as caught:
        await hub.context(w["id"])
    assert caught.value.code == "CONTEXT_TOO_LARGE"


async def test_domain_cannot_be_changed_by_worker_contract(hub):
    with pytest.raises(HubError) as caught:
        await publish(hub, domain="lean")
    assert caught.value.code == "DOMAIN_MISMATCH"


async def test_completion_requires_no_unfinished_tasks(hub):
    run = await start(hub)
    task = await publish(hub)
    with pytest.raises(HubError, match="unfinished"):
        await command(hub, "run.complete", {"run_id": run["id"]})
    await command(hub, "task.retire", {"task_id": task["id"], "revision": task["revision"]})
    assert (await command(hub, "run.complete", {"run_id": run["id"]}))["state"] == "complete"


@pytest.mark.parametrize("terminal", ["run.stop", "run.complete"])
async def test_terminal_run_cannot_be_reactivated_via_pause(hub, terminal):
    run = await start(hub)
    await command(hub, terminal, {"run_id": run["id"]})
    for name in ("run.pause", "run.resume"):
        with pytest.raises(HubError):
            await command(hub, name, {"run_id": run["id"]})


async def test_retracted_acceptance_invalidates_transitive_results(hub):
    source = await command(hub, "artifact.register", {"kind": "source", "data": {}})
    derived = await command(hub, "artifact.register", {
        "kind": "derived", "data": {}, "input_refs": [source["id"]]})
    await accept(hub, source["id"])
    await accept(hub, derived["id"])
    await accept(hub, source["id"], verdict="rejected")
    assert (await hub.artifact(source["id"]))["status"] == "withdrawn"
    assert (await hub.artifact(derived["id"]))["status"] == "withdrawn"


async def test_old_rejected_candidate_cannot_reopen_task_with_an_accepted_result(file_hub):
    hub = file_hub
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    old = await submit(hub, actor, attempt=1)
    await accept(hub, old["artifact_id"], review="reject")
    _, actor = await claim(hub, task, w, run)
    current = await submit(hub, actor, replaces=old["artifact_id"], attempt=2)
    await accept(hub, current["artifact_id"])
    await close_task(hub, task["id"], current["artifact_id"])
    await accept(hub, old["artifact_id"], verdict="rejected")
    assert (await hub.task(task["id"]))["state"] == "done"


async def test_prerequisite_ids_are_frozen_into_attempt_and_result(file_hub):
    hub = file_hub
    source = await command(hub, "artifact.register", {"kind": "source", "data": {}})
    await accept(hub, source["id"])
    requirements = [{"key": "precondition", "artifact_id": source["id"], "policy": "general.manual@1"}]
    task, run, w = await publish(hub, requirements=requirements), await start(hub), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    result = await submit(hub, actor)
    await accept(hub, result["artifact_id"])
    assert source["id"] in (await hub.artifact(result["artifact_id"]))["input_refs"]
    await command(hub, "artifact.withdraw", {"artifact_id": source["id"]})
    assert (await hub.artifact(result["artifact_id"]))["status"] == "withdrawn"
    assert (await hub.task(task["id"]))["state"] != "done"


async def test_stale_actor_cannot_ack_new_attempt_context(hub):
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    _, old_actor = await claim(hub, task, w, run)
    await command(hub, "runtime.abandon", {"worker_id": w["id"], "quiescent": True}, RUNNER)
    await command(hub, "worker.recover", {"worker_id": w["id"], "process_confirmed_stopped": True,
                  "reason": "Recover the original saved execution after known quiescence."})
    _, new_actor = await claim(hub, task, w, run)
    packet = await hub.context(w["id"])
    with pytest.raises(HubError):
        await command(hub, "context.ack", {"worker_id": w["id"], "packet_id": packet["packet_id"]}, old_actor)
    await command(hub, "context.ack", {"worker_id": w["id"], "packet_id": packet["packet_id"]}, new_actor)


async def test_late_usage_cannot_resurrect_stopped_run(hub):
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    await claim(hub, task, w, run)
    intent = await command(hub, "turn.reserve", {"worker_id": w["id"]}, RUNNER)
    await command(hub, "run.stop", {"run_id": run["id"]})
    await command(hub, "turn.finish", {"intent_id": intent["id"], "status": "unknown"}, RUNNER)
    for total in (50000, 1):
        await command(hub, "usage.record", {"worker_id": w["id"], "sample": {"totalTokens": total}}, RUNNER)
        assert (await hub.run(run["id"]))["state"] == "stopped"
    await command(hub, "run.reconcile_usage", {
        "run_id": run["id"], "tokens_used": 50000, "reason": "Provider accounting inspected"})
    with pytest.raises(HubError):
        await command(hub, "run.resume", {"run_id": run["id"]})


async def test_unrelated_policy_rejection_preserves_other_acceptance(hub):
    artifact = await command(hub, "artifact.register", {"kind": "input", "data": {}})
    await accept(hub, artifact["id"])
    await accept(hub, artifact["id"], policy="other.policy@1", verdict="rejected")
    record = await hub.artifact(artifact["id"])
    assert record["accepted"]
    task = await publish(hub, requirements=[{
        "key": "input", "artifact_id": artifact["id"], "policy": "general.manual@1"}])
    await claim(hub, task, await worker(hub), await start(hub))


async def test_late_machine_approval_does_not_override_peer_rejection(file_hub):
    hub = file_hub
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    candidate = await submit(hub, actor)
    await accept(hub, candidate["artifact_id"], review="reject")
    await claim(hub, task, w, run)
    await accept(hub, candidate["artifact_id"])
    assert (await hub.task(task["id"]))["state"] == "claimed"
    await command(hub, "runtime.abandon", {"worker_id": w["id"], "quiescent": True}, RUNNER)
    assert not (await hub.artifact(candidate["artifact_id"]))["accepted"]
    assert (await hub.task(task["id"]))["state"] != "done"


async def test_other_task_requires_new_worker_and_fresh_context_baseline(hub):
    task_a, task_b = await publish(hub, "A"), await publish(hub, "B")
    run, w = await start(hub), await worker(hub)
    _, actor = await claim(hub, task_a, w, run)
    packet = await hub.context(w["id"])
    await command(hub, "context.ack", {"worker_id": w["id"], "packet_id": packet["packet_id"]}, actor)
    await command(hub, "runtime.abandon", {"worker_id": w["id"], "quiescent": True}, RUNNER)
    await command(hub, "worker.recover", {"worker_id": w["id"], "process_confirmed_stopped": True,
                  "reason": "Preserve and recover the original unfinished conversation."})
    with pytest.raises(HubError) as error:
        await claim(hub, task_b, w, run)
    assert error.value.code == "CONTINUATION_RESERVED"
    independent = await worker(hub)
    await claim(hub, task_b, independent, run)
    second = await hub.context(independent["id"])
    assert second["after_sequence"] == 0
    assert (await hub.worker(w["id"]))["context_epoch"] == packet["context_epoch"]
    assert any(event["kind"] == "task.published" and event["object_id"] == task_b["id"]
               for event in second["updates"])


async def test_startup_quarantines_unknown_turn_and_preserves_recovery_snapshot(hub):
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    await command(hub, "task.checkpoint", {"checkpoint": checkpoint()}, actor)
    (hub.workspace(w["id"]) / "work.txt").write_text("unpublished work", encoding="utf-8")
    await command(hub, "turn.reserve", {"worker_id": w["id"]}, RUNNER)
    root = hub.root
    await hub.close()
    recovered = Hub(root)
    try:
        assert (await recovered.worker(w["id"]))["state"] == "recovery_required"
        assert (await recovered.run(run["id"]))["state"] == "running"
        assert (await recovered.run(run["id"]))["usage_uncertain"]
        with pytest.raises(HubError):
            await command(recovered, "worker.recover", {"worker_id": w["id"]})
        receipt = await command(recovered, "worker.recover", {
            "worker_id": w["id"], "process_confirmed_stopped": True,
            "reason": "Synthetic fixture has no owned processes"})
        assert receipt["checkpoint"] == checkpoint()
        assert recovered.blobs.get_json(receipt["snapshot"])["files"][0]["path"] == "work.txt"
        assert (await recovered.worker(w["id"]))["active_attempt"] is None
        assert (await recovered.run(run["id"]))["usage_uncertain"]
        resumed = await command(recovered, "run.resume", {"run_id": run["id"]})
        assert resumed["state"] == "running" and resumed["usage_uncertain"]
        assert (await claim(recovered, task, w, run))[0]["worker_id"] == w["id"]
    finally:
        await recovered.close()


async def test_paused_run_rejects_worker_mutation_and_turn_admission(hub):
    task, run, w = await publish(hub), await start(hub), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    await command(hub, "run.pause", {"run_id": run["id"]})
    with pytest.raises(HubError):
        await command(hub, "task.publish", {"contract": contract("After pause")}, actor)
    with pytest.raises(HubError):
        await command(hub, "turn.reserve", {"worker_id": w["id"]}, RUNNER)


async def test_revocation_during_drain_cannot_be_validated_on_a_new_basis(file_hub):
    hub = file_hub
    source = await command(hub, "artifact.register", {"kind": "input", "data": {}})
    await accept(hub, source["id"])
    task, run, w = await publish(hub, inputs=[source["id"]]), await start(hub), await worker(hub)
    _, actor = await claim(hub, task, w, run)
    await command(hub, "artifact.submit", file_payload(hub, actor, kind="output", data={}), actor)
    await command(hub, "artifact.withdraw", {"artifact_id": source["id"]})
    transition = await quiet(hub, w["id"])
    assert transition["directive"] == "needs_attention"
    assert transition["reason"] == "INPUTS_CHANGED"
    assert not [item for item in await hub.list_artifacts() if item.get("task_id") == task["id"]]


async def test_resumed_context_exposes_exact_artifact_from_old_revision(file_hub):
    hub = file_hub
    provider, consumer = await publish(hub, "Provider"), await publish(hub, "Consumer")
    run, w1, w2 = await start(hub), await worker(hub), await worker(hub)
    _, actor = await claim(hub, provider, w1, run)
    result = await submit(hub, actor)
    await accept(hub, result["artifact_id"])
    _, actor = await claim(hub, consumer, w2, run)
    initial = await hub.context(w2["id"])
    await command(hub, "context.ack", {"worker_id": w2["id"], "packet_id": initial["packet_id"]}, actor)
    await command(hub, "task.revise", {"task_id": provider["id"], "contract": contract("Provider revised")})
    waiting = await legacy_wait(hub, {
        "predicate": {"requirements": [{"key": "result", "artifact_id": result["artifact_id"],
                                        "policy": provider["contract"]["acceptance_policy"]}]},
        "checkpoint": checkpoint()}, actor)
    await quiet(hub, w2["id"])
    await command(hub, "continuation.resume", {
        "continuation_id": waiting["continuation_id"], "run_id": run["id"]}, RUNNER)
    packet = await hub.context(w2["id"])
    assert result["artifact_id"] in [item["id"] for item in packet["inputs"]]
    assert packet["resume_requirements"][0]["artifact_ids"] == [result["artifact_id"]]
    old = await hub.task(provider["id"], revision=1)
    assert old["contract"]["title"] == "Provider"
    assert old["artifacts"][0]["id"] == result["artifact_id"]
    assert (await hub.task(provider["id"]))["revision"] == 2


async def test_withdrawn_direct_input_marks_task_for_reconciliation(hub):
    source = await command(hub, "artifact.register", {"kind": "input", "data": {}})
    task = await publish(hub, inputs=[source["id"]])
    other = await publish(hub, "Independent")
    await command(hub, "artifact.withdraw", {"artifact_id": source["id"]})
    assert (await hub.task(task["id"]))["state"] == "needs_attention"
    run, w = await start(hub), await worker(hub)
    await claim(hub, other, w, run)
    revised = await command(hub, "task.revise", {
        "task_id": task["id"], "contract": contract("Reconciled", inputs=[])})
    assert revised["state"] == "ready"


async def test_task_set_aside_for_a_candidate_input_returns_once_it_is_published(file_hub):
    # Task 06e70c66 in the Linux pilot: revised to read a candidate under review, it stayed set
    # aside after the publication, and every claim was refused for three days.
    hub = file_hub
    provider, consumer = await publish(hub, "Provider"), await publish(hub, "Consumer")
    run, author, other = await start(hub), await worker(hub), await worker(hub)
    _, actor = await claim(hub, provider, author, run)
    candidate = (await submit(hub, actor))["artifact_id"]
    revised = await command(hub, "task.revise", {
        "task_id": consumer["id"], "contract": contract("Consumer", inputs=[candidate])})
    assert revised["state"] == "needs_attention"
    with pytest.raises(HubError) as refused:
        await claim(hub, revised, other, run)
    assert refused.value.code == "ALREADY_CLAIMED"
    await accept(hub, candidate)
    returned = await hub.task(consumer["id"])
    assert returned["state"] == "ready"
    attempt, _ = await claim(hub, returned, other, run)
    assert attempt["task_id"] == consumer["id"]


async def test_task_whose_input_depends_on_an_unaccepted_artifact_stays_set_aside(hub):
    base = await command(hub, "artifact.register", {"kind": "input", "data": {}})
    derived = await command(hub, "artifact.register", {"kind": "input", "data": {}, "input_refs": [base["id"]]})
    consumer = await publish(hub, "Consumer")
    revised = await command(hub, "task.revise", {
        "task_id": consumer["id"], "contract": contract("Consumer", inputs=[derived["id"]])})
    assert revised["state"] == "needs_attention"
    await accept(hub, derived["id"])
    assert (await hub.task(consumer["id"]))["state"] == "needs_attention"
    await accept(hub, base["id"])
    assert (await hub.task(consumer["id"]))["state"] == "ready"


async def test_task_with_a_withdrawn_input_stays_set_aside(hub):
    source = await command(hub, "artifact.register", {"kind": "input", "data": {}})
    other = await command(hub, "artifact.register", {"kind": "input", "data": {}})
    await accept(hub, source["id"])
    task = await publish(hub, inputs=[source["id"]])
    await command(hub, "artifact.withdraw", {"artifact_id": source["id"]})
    await accept(hub, other["id"])
    assert (await hub.task(task["id"]))["state"] == "needs_attention"


async def test_unreadable_set_aside_task_does_not_stop_a_publication(hub, monkeypatch):
    source = await command(hub, "artifact.register", {"kind": "input", "data": {}})
    unreadable, returning = await publish(hub, "Unreadable"), await publish(hub, "Returning")
    for task in (unreadable, returning):
        await command(hub, "task.revise", {
            "task_id": task["id"], "contract": contract(task["contract"]["title"], inputs=[source["id"]])})
    recompute = hub._recompute_task

    def fails_on_one(c, task_id):
        if task_id == unreadable["id"]:
            raise OSError("The task's files cannot be read")
        return recompute(c, task_id)

    monkeypatch.setattr(hub, "_recompute_task", fails_on_one)
    await accept(hub, source["id"])
    assert (await hub.task(unreadable["id"]))["state"] == "needs_attention"
    assert (await hub.task(returning["id"]))["state"] == "ready"
    assert (await hub.artifact(source["id"]))["status"] == "accepted"


async def test_withdrawal_and_revision_notify_only_the_open_tasks_naming_them(hub):
    # Both read only the open tasks whose ledger contracts name the artifact or the task.
    source = await command(hub, "artifact.register", {"kind": "input", "data": {}})
    loose = await command(hub, "artifact.register", {"kind": "input", "data": {}})
    provider = await publish(hub, "Provider")
    direct = await publish(hub, "Direct input", inputs=[source["id"]])
    named = await publish(hub, "Requires the provider", requirements=[
        {"key": "result", "task_id": provider["id"], "task_revision": 1,
         "policy": provider["contract"]["acceptance_policy"]}])
    unrelated = await publish(hub, "Requires another artifact", requirements=[
        {"key": "other", "artifact_id": loose["id"], "policy": provider["contract"]["acceptance_policy"]}])

    async def notified(reason):
        return await hub.db.call(lambda c: sorted(row[0] for row in c.execute(
            "SELECT task_id FROM notifications WHERE reason=?", (reason,))))

    await command(hub, "artifact.withdraw", {"artifact_id": source["id"], "reason": "Source revoked"})
    assert await notified("Source revoked") == [direct["id"]]
    assert (await hub.task(direct["id"]))["state"] == "needs_attention"
    assert (await hub.task(unrelated["id"]))["state"] == "ready"
    await command(hub, "task.revise", {"task_id": provider["id"],
                                       "contract": contract("Provider", objective="Produce a changed artifact.")})
    assert await notified("dependency_requirement_changed") == [named["id"]]


@pytest.mark.parametrize("verdict", ["unavailable", "needs_operator"])
async def test_inconclusive_recheck_does_not_withdraw_but_later_rejection_does(hub, verdict):
    source = await command(hub, "artifact.register", {"kind": "source", "data": {}})
    derived = await command(hub, "artifact.register", {
        "kind": "derived", "data": {}, "input_refs": [source["id"]]})
    await accept(hub, source["id"])
    await accept(hub, derived["id"])
    await accept(hub, source["id"], verdict=verdict)
    assert (await hub.artifact(source["id"]))["status"] != "withdrawn"
    assert (await hub.artifact(derived["id"]))["status"] == "accepted"
    await accept(hub, source["id"], verdict="rejected")
    assert (await hub.artifact(source["id"]))["status"] == "withdrawn"
    assert (await hub.artifact(derived["id"]))["status"] == "withdrawn"


async def test_unavailable_revalidation_can_be_retried_without_resubmission(hub):
    source = await command(hub, "artifact.register", {"kind": "source", "data": {}})
    await accept(hub, source["id"])
    await accept(hub, source["id"], verdict="unavailable")
    await accept(hub, source["id"])
    artifact = await hub.artifact(source["id"])
    assert artifact["status"] == "accepted"
    assert [e["verdict"] for e in artifact["evidence"]] == ["accepted", "unavailable", "accepted"]
