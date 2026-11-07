"""Model-free runtime coverage for temporary teams and reviews in their own conversations."""
from __future__ import annotations

import asyncio
import json
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator, ValidationError

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Command, Role
from formal_geyser.coordination.runtime import (
    ScriptedAction,
    ScriptedHarness,
    ScriptedTurn,
    TurnOutcome,
    WorkerRunner,
)
from formal_geyser.coordination.runtime.helpers import INVITATION
from formal_geyser.coordination.runtime.tools import operation_tools
from tests.coordination.gateway_helpers import file_arguments
from tests.coordination.lifecycle_helpers import record_saved_turn, select_review
from tests.coordination.publication_helpers import (
    FileHarness,
    file_payload,
    new_hub,
    publish_batch,
    source_payload,
)


def contract(title="Shared record"):
    return {"title": title, "objective": "Produce a complete structured record.",
            "acceptance_policy": "general.json@1", "payload": {"output_schema": {"type": "object"}}}


async def command(hub, name, payload=None):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), Actor(role=Role.OPERATOR))


async def call(on_tool, name, arguments=None):
    result = await on_tool("taskhub_call", {"operation": (name).removeprefix("taskhub_"), "arguments": arguments or {}}, uuid4().hex)
    admitted = getattr(result, "after_send", None)
    if admitted:
        await admitted()
    return result


def review_arguments(binding):
    return {**{key: binding[key] for key in ("candidate_id", "candidate_hash", "task_revision", "input_refs", "policy")},
            "verdict": "accept", "rationale": "The exact structured candidate matches the task.",
            "evidence": ["Compared the candidate bytes with the bound task revision."]}


@pytest.fixture
async def collaborative(tmp_path):
    hub = await new_hub(tmp_path)
    task = await command(hub, "task.publish", {"contract": contract()})
    run = await command(hub, "run.start")
    yield hub, task, run
    await hub.close()


async def test_review_starts_in_its_own_conversation_and_retires_without_source_reuse(collaborative):
    hub, host, run = collaborative
    author = FileHarness([ScriptedTurn([ScriptedAction("taskhub_call", {"operation": 'contribute', "arguments": {
        "kind": "json", "data": {"ready": True}, "repository_paths": ["result.json"],
    }})])], files={"result.json": b'{"ready":true}\n'})
    submitted = await WorkerRunner(hub, lambda: author).run_once(run["id"])
    review = next(task for task in await hub.list_tasks() if task["contract"]["kind"] == "review")
    source = await command(hub, "worker.register")
    await hub.execute(Command(name="worker.bind_session", key=uuid4().hex, payload={
        "worker_id": source["id"], "thread_id": "existing-peer-conversation",
        **WorkerRunner(hub, ScriptedHarness)._catalog_binding()}), Actor(role=Role.RUNNER))
    await record_saved_turn(hub, source["id"])
    # Reviews are never offered to an idle worker; admission gives each its own new reviewer.
    assignment = await hub.next_assignment(run["id"])
    assert assignment["task_id"] == review["id"] and assignment["worker_id"] != source["id"]
    assert await hub.due_assignment_intents(run["id"]) == []
    idle = await hub.worker(source["id"])
    assert idle["work_intention"] == "idle" and idle["active_attempt"] is None
    source_workspace = hub.workspace(source["id"])
    (source_workspace / "private-progress.txt").write_text("Retain source progress", encoding="utf-8")
    peers = []

    class Reviewer(ScriptedHarness):
        async def open(self, **options):
            self.instructions = options["instructions"]
            return await super().open(**options)

        async def run_turn(self, prompt, *, on_started, on_tool, **kwargs):
            packet = json.loads(prompt)
            assert packet["task"]["kind"] == "review"
            guidance = packet["reading"]
            # The required guides are the reviewer's standing instructions; reading lists on-demand ones.
            assert "required" not in guidance
            assert ".private/instructions/shared-dags.md" in guidance["on_demand"]
            for path in (".private/instructions/worker.md", ".private/instructions/review.md"):
                assert path not in guidance["on_demand"]
                assert (self.workspaces[-1] / path).read_text(encoding="utf-8").strip() in self.instructions
            assert self.workspaces[-1] != source_workspace
            await on_started("review-turn")
            denied = await on_tool("taskhub_call", {"operation": "talk_send", "arguments": {
                "recipient_id": submitted["worker_id"], "message_id": "forbidden", "text": "No author consultation"}}, "private-call")
            assert "error" not in denied  # Clarification does not grant authors approval authority.
            binding = (await hub.task(review["id"]))["collaboration"]["review"]
            accepted = await call(on_tool, 'review', review_arguments(binding))
            assert accepted["state"] == "integrating"
            await self.close()
            return TurnOutcome("review-turn", "completed", True, True)

    def factory():
        peer = Reviewer()
        peers.append(peer)
        return peer

    runner = WorkerRunner(hub, factory)
    reviewing = await runner.run_once(run["id"], assignment=assignment)
    await publish_batch(hub, run["id"])
    reviewer = await hub.worker(reviewing["worker_id"])
    source_record = await hub.worker(source["id"])
    assert reviewer["kind"] == "review" and reviewer["work_intention"] == "retired"
    assert reviewer["thread_id"] != source_record["thread_id"] != author.session.thread_id
    assert (source_workspace / "private-progress.txt").read_text() == "Retain source progress"
    assert (await hub.task(host["id"]))["state"] != "done"
    assert (await hub.artifact(submitted["artifact_id"]))["accepted"]
    assert peers and all(peer.opened_threads == [None] for peer in peers)  # A new conversation.
    assert (await hub.run(run["id"]))["turns_used"] == 2


async def test_recruitment_helper_opens_once_and_successful_admission_consumes_it(collaborative):
    hub, host, run = collaborative
    first_harness = ScriptedHarness([ScriptedTurn([])])
    first = await WorkerRunner(hub, lambda: first_harness).run_once(run["id"])
    intent = (await hub.due_recruitment_intents(run["id"]))[0]
    choice = ScriptedHarness([ScriptedTurn([], visible_output='{"decision":true}')])
    await WorkerRunner(hub, lambda: choice).run_helper(intent)
    assert "instruction_guidance" not in json.loads(choice.prompts[0])
    # The runtime writes the standard invitation for an opening the model approved.
    assert (await hub.task(host["id"]))["collaboration"]["recruitment"]["invitation"] == INVITATION
    second = await command(hub, "worker.register")
    joined = await command(hub, "task.join", {"task_id": host["id"], "revision": 1,
        "worker_id": second["id"], "run_id": run["id"], "plan": "Check the disjoint input section."})
    assert joined["worker_id"] != first["worker_id"]
    assert (await hub.task(host["id"]))["collaboration"]["recruitment"]["opportunity_state"] == "consumed"
    third = await command(hub, "worker.register")
    with pytest.raises(HubError):
        await command(hub, "task.join", {"task_id": host["id"], "revision": 1,
            "worker_id": third["id"], "run_id": run["id"], "plan": "No third admission."})
    assert choice.opened_threads == [None]


async def test_paused_worker_stays_asleep_without_announcement_turn(collaborative):
    hub, host, run = collaborative
    sleeping = ScriptedHarness([ScriptedTurn([ScriptedAction("taskhub_call", {"operation": 'pause', "arguments": {"predicate": {"external_response_id": "unanswered-request"}, "reason": "Preserve progress until actionable work is available.", **({
        "checkpoint": {"summary": "Saved context", "next_action": "Wait for suitable work"}})}})])])
    first = await WorkerRunner(hub, lambda: sleeping).run_once(run["id"])
    other = await command(hub, "task.publish", {"contract": contract("Unrelated record")})
    # A paused worker is waiting, not idle: it is offered no new task and gets no
    # announcement turn, so the new task gets a new worker without any decision.
    newcomer = ScriptedHarness([ScriptedTurn([])])
    started = await WorkerRunner(hub, lambda: newcomer).run_once(run["id"])
    assert started["worker_id"] != first["worker_id"]
    assert (await hub.worker(started["worker_id"]))["current_task_id"] == other["id"]
    assert (await hub.worker(first["worker_id"]))["work_intention"] == "waiting"
    assert len(await hub.list_workers()) == 2
    assert len(sleeping.prompts) == 1 and len(newcomer.prompts) == 1
    assert not await hub.due_assignment_intents(run["id"])
    assert await hub.db.call(lambda c: c.execute(
        "SELECT count(*) FROM helper_intents WHERE kind='assignment'").fetchone()[0]) == 0
    assert (await hub.run(run["id"]))["turns_used"] == 2


async def test_executor_changes_native_contract_with_version_and_reason_without_publisher_vote(collaborative):
    hub, task, run = collaborative
    attempt = await hub.next_assignment(run["id"])
    runner = WorkerRunner(hub, ScriptedHarness)
    revised = {**task["contract"], "context": "Precise additional execution guidance."}
    result = await runner._tool(attempt, "synthetic", "taskhub_call", {"operation": 'propose_revision', "arguments": file_arguments(runner.hub.workspace(attempt["worker_id"]), 'propose_revision', {
        "task_id": task["id"], "revision": 1, "contract": revised,
        "reason": "Clarify the execution guidance without changing the objective."})}, "revision-1")
    assert result["revision"] == 2
    assert (await hub.task(task["id"]))["contract"]["context"] == revised["context"]


async def test_native_withdrawal_retains_candidate_history_and_rechecks_acceptance(collaborative):
    hub, task, run = collaborative
    attempt = await hub.next_assignment(run["id"])
    runner = WorkerRunner(hub, ScriptedHarness)
    candidate = await runner._tool(attempt, "author-thread", "taskhub_call", {"operation": 'stage', "arguments": file_arguments(runner.hub.workspace(attempt["worker_id"]), 'stage', file_payload(
        hub, attempt, kind="json", data={"optional": True},
        introduction="An optional candidate that may be unnecessary."))}, "stage-optional")
    before = await hub.artifact(candidate["artifact_id"])
    reason = "This optional result is not needed for the unchanged objective; reevaluate the original acceptance conditions."
    result = await runner._tool(attempt, "author-thread", "taskhub_call", {"operation": 'withdraw', "arguments": file_arguments(runner.hub.workspace(attempt["worker_id"]), 'withdraw', {
        "artifact_id": before["id"], "reason": reason})}, "withdraw-optional")
    retained = await hub.artifact(before["id"])
    assert before["id"] in result["withdrawn"]
    assert retained["status"] == "withdrawn" and not retained["accepted"]
    assert retained["data"] == before["data"] and retained["body_sha"] == before["body_sha"]
    assert (await hub.worker(attempt["worker_id"]))["active_attempt"] == attempt["id"]
    assert (await hub.task(task["id"]))["state"] != "done"
    obligation = await hub.db.call(lambda c: dict(c.execute(
        "SELECT * FROM artifact_obligations WHERE artifact_id=?", (before["id"],)).fetchone()))
    assert obligation["withdrawn_reason"] == reason
    notifications = await hub.db.call(lambda c: [row[0] for row in c.execute(
        "SELECT reason FROM notifications WHERE task_id=?", (task["id"],))])
    assert "withdrawal_requires_acceptance_recheck" in notifications


async def test_native_permanent_leave_drains_and_preserves_checkpoint_and_files(collaborative):
    hub, task, run = collaborative

    class Leaving(ScriptedHarness):
        async def run_turn(self, prompt, *, on_started, on_tool, **kwargs):
            await on_started("permanent-leave-turn")
            (self.workspaces[-1] / "saved-work.md").write_text("Recoverable unfinished work.", encoding="utf-8")
            await call(on_tool, 'checkpoint', {
                "checkpoint": {"summary": "Preserve unfinished work", "next_action": "Retain for inspection"}})
            result = await call(on_tool, 'leave_task', {
                "task_id": task["id"], "reason": "I permanently abandon this execution relationship."})
            assert result["runtime_directive"] == "drain_for_release"
            assert not self.process_quiet
            await self.close()
            return TurnOutcome("permanent-leave-turn", "interrupted", True, True, result["runtime_directive"])

    result = await WorkerRunner(hub, Leaving).run_once(run["id"])
    worker = await hub.worker(result["worker_id"])
    assert worker["active_attempt"] is None
    assert (hub.workspace(worker["id"]) / "saved-work.md").read_text() == "Recoverable unfinished work."
    members = (await hub.task(task["id"]))["collaboration"]["members"]
    assert next(item for item in members if item["worker_id"] == worker["id"])["membership_state"] == "left"
    assert (await hub.task(task["id"]))["state"] == "retired"
    saved = await hub.db.call(lambda c: dict(c.execute(
        "SELECT * FROM attempts WHERE worker_id=? ORDER BY created_at DESC LIMIT 1", (worker["id"],)).fetchone()))
    assert saved["snapshot"] and json.loads(saved["checkpoint_json"])["summary"] == "Preserve unfinished work"


async def test_native_leave_requires_membership_and_reviewer_cannot_withdraw_or_leave(collaborative):
    hub, task, run = collaborative
    author = await hub.next_assignment(run["id"])
    runner = WorkerRunner(hub, ScriptedHarness)
    helper_task = await runner._tool(author, "author-thread", "taskhub_call", {"operation": 'publish', "arguments": file_arguments(runner.hub.workspace(author["worker_id"]), 'publish', {
        "contract": contract("Published but not joined")})}, "publish-independent")
    with pytest.raises(HubError) as not_joined:
        await runner._tool(author, "author-thread", "taskhub_call", {"operation": 'leave_task', "arguments": file_arguments(runner.hub.workspace(author["worker_id"]), 'leave_task', {
            "task_id": helper_task["id"], "reason": "Publishing alone is not executor membership."})}, "leave-unjoined")
    assert not_joined.value.code == "EXECUTOR_MEMBERSHIP_REQUIRED"
    assert (await hub.task(helper_task["id"]))["state"] == "ready"
    staged = await runner._tool(author, "author-thread", "taskhub_call", {"operation": 'stage', "arguments": file_arguments(runner.hub.workspace(author["worker_id"]), 'stage', file_payload(
        hub, author, kind="json", data={"ready": True}))}, "review-candidate")
    reviewer = await select_review(hub, staged["review_task_id"], run["id"])
    for tool_name, payload in (
        ("taskhub_withdraw", {"artifact_id": staged["artifact_id"], "reason": "Must not bypass review."}),
        ("taskhub_leave_task", {"task_id": reviewer["task_id"], "reason": "Use review recovery instead."}),
    ):
        denied = await runner._tool(reviewer, "review-thread", "taskhub_call", {
            "operation": tool_name.removeprefix("taskhub_"), "arguments": payload}, tool_name)
        assert denied["code"] == "OPERATION_NOT_ALLOWED"
    assert (await hub.artifact(staged["artifact_id"]))["status"] != "withdrawn"
    assert (await hub.worker(reviewer["worker_id"]))["active_attempt"] == reviewer["id"]


async def test_busy_team_review_gets_a_new_reviewer_without_interrupting_its_work(collaborative):
    hub, task, run = collaborative
    initial = await WorkerRunner(hub, lambda: ScriptedHarness([ScriptedTurn([])])).run_once(run["id"])
    author_id = initial["worker_id"]
    intent = (await hub.due_recruitment_intents(run["id"]))[0]
    await hub.decide_recruitment(intent["id"], True, "Inspect an independent record field.")
    peer = await command(hub, "worker.register")
    peer_attempt = await command(hub, "task.join", {"task_id": task["id"], "revision": 1,
        "worker_id": peer["id"], "run_id": run["id"], "plan": "Inspect the independent field."})
    first_peer = ScriptedHarness([ScriptedTurn([])])
    first_peer_result = await WorkerRunner(hub, lambda: first_peer).run_once(run["id"], assignment=peer_attempt)
    assert first_peer_result["directive"] == "continued"
    peer_attempt = await command(hub, "task.claim", {"task_id": task["id"], "revision": 1,
        "worker_id": peer["id"], "run_id": run["id"]})
    peer_running, accepted = asyncio.Event(), asyncio.Event()
    observed = {}

    class ConcurrentHarness(ScriptedHarness):
        async def run_turn(self, prompt, *, on_started, on_tool, **kwargs):
            try:
                packet = json.loads(prompt)
                # Ordinary continuations omit already-known worker identity. The
                # harness is still bound to the same native conversation.
                worker_id = next(worker["id"] for worker in await hub.list_workers()
                                 if worker["thread_id"] == self.session.thread_id)
                turn_id = uuid4().hex
                await on_started(turn_id)
                if packet["task"]["kind"] == "review":
                    source = await hub.worker(peer["id"])
                    assert source["active_attempt"] == peer_attempt["id"]
                    # The review starts a new conversation, not the busy teammate's.
                    assert self.opened_threads == [None]
                    assert worker_id not in (peer["id"], author_id)
                    assert self.workspaces[-1] != hub.workspace(peer["id"])
                    observed["review"] = worker_id
                    binding = (await hub.task(packet["task"]["id"]))["collaboration"]["review"]
                    await call(on_tool, 'review', review_arguments(binding))
                    assert not peer_execution.done()
                    assert (await hub.worker(peer["id"]))["active_attempt"] == peer_attempt["id"]
                    accepted.set()
                    return TurnOutcome(turn_id, "completed", True, True)
                else:
                    if worker_id == peer["id"]:
                        observed["source_thread"] = self.session.thread_id
                        observed["source_active_turn"] = turn_id
                        assert self.session.thread_id == first_peer.session.thread_id
                        peer_running.set()
                    else:
                        assert worker_id == author_id and peer_running.is_set()
                        staged = await call(on_tool, 'stage', file_arguments(self.workspaces[-1], "stage", source_payload(
                            self.workspaces[-1], kind="json", data={"value": 1})))
                        assert staged["review_task_id"]
                    # The review must accept before either ordinary turn
                    # releases. The test below owns the acceptance watchdog.
                    await accepted.wait()
                    result = await call(on_tool, 'pause', {"predicate": {"external_response_id": "unanswered-request"}, 
                        "reason": "Save the independent work after acceptance.",
                        "checkpoint": {"summary": "Saved independent work", "next_action": "Await new useful work"}})
                return TurnOutcome(turn_id, "interrupted", True, True, result["runtime_directive"])
            finally:
                await self.close()

    runner = WorkerRunner(hub, ConcurrentHarness)
    peer_execution = asyncio.create_task(runner.run_once(run["id"], assignment=peer_attempt))
    peer_ready = asyncio.create_task(peer_running.wait())
    driver_execution = acceptance_ready = None
    try:
        finished, _ = await asyncio.wait({peer_execution, peer_ready}, timeout=10,
                                         return_when=asyncio.FIRST_COMPLETED)
        if peer_execution in finished:
            peer_execution.result()
            pytest.fail("The source execution ended before the overlap check began")
        assert peer_ready in finished, "The source worker did not reach its ready boundary"
        driver_execution = asyncio.create_task(runner.run_until_idle(run["id"]))
        acceptance_ready = asyncio.create_task(accepted.wait())
        finished, _ = await asyncio.wait({peer_execution, driver_execution, acceptance_ready},
                                         timeout=60, return_when=asyncio.FIRST_COMPLETED)
        if not accepted.is_set():
            for execution in (peer_execution, driver_execution):
                if execution in finished:
                    outcome = execution.result()
                    pytest.fail(f"Execution ended before the review accepted: {outcome}")
        assert accepted.is_set(), "The overlapping review did not accept the candidate"
        result = await asyncio.wait_for(driver_execution, 30)
        await asyncio.wait_for(peer_execution, 5)
        assert not result["failure_observed"]
        assert (await hub.worker(peer["id"]))["thread_id"] == observed["source_thread"]
        assert all(worker["active_attempt"] is None for worker in await hub.list_workers())
        await publish_batch(hub, run["id"])
        assert (await hub.worker(observed["review"]))["work_intention"] == "retired"
    finally:
        executions = (peer_ready, acceptance_ready, driver_execution, peer_execution)
        for execution in executions:
            if execution is not None and not execution.done():
                execution.cancel()
        await asyncio.gather(*(execution for execution in executions if execution is not None),
                             return_exceptions=True)

def test_collaboration_tool_schemas_keep_submission_intent_and_review_binding_explicit():
    tools = {item["name"]: item["inputSchema"] for item in operation_tools()}
    required = {"taskhub_stage", "taskhub_contribute", "taskhub_complete", "taskhub_review",
                "taskhub_withdraw", "taskhub_leave_task", "taskhub_pause",
                "taskhub_invitation_update", "taskhub_invitation_withdraw",
                "taskhub_propose_revision", "taskhub_completion_feedback", "taskhub_completion_finalize"}
    assert required <= tools.keys()
    assert not any(name.startswith("taskhub_merge_") for name in tools)
    assert {"taskhub_submit", "task_submit", "taskhub_join", "taskhub_private"}.isdisjoint(tools)
    assert {"taskhub_request_assistance", "taskhub_approve_revision", "taskhub_reject_revision", "taskhub_yield"}.isdisjoint(tools)
    for schema in tools.values():
        Draft202012Validator.check_schema(schema)
    Draft202012Validator(tools["taskhub_review"]).validate(review_arguments({
        "candidate_id": "candidate", "candidate_hash": "a" * 64, "task_revision": 1,
        "input_refs": [], "policy": "general.json@1"}))
    assert set(tools["taskhub_publish"]["properties"]) == {"contract_path"}
    assert tools["taskhub_publish"]["required"] == ["contract_path"]
    Draft202012Validator(tools["taskhub_withdraw"]).validate({"artifact_id": "pending-artifact", "reason": "Unnecessary for the original objective."})
    Draft202012Validator(tools["taskhub_leave_task"]).validate({"task_id": "ordinary-task", "reason": "Permanently abandon my execution relationship."})
    assert "permanent" not in tools["taskhub_leave_task"]["properties"]
    with pytest.raises(ValidationError):
        Draft202012Validator(tools["taskhub_leave_task"]).validate({"task_id": "ordinary-task"})


def test_candidate_tools_use_files_and_keep_completion_and_review_versions_explicit():
    tools = {item["name"]: item["inputSchema"] for item in operation_tools()}
    for name in ("taskhub_stage", "taskhub_contribute"):
        Draft202012Validator(tools[name]).validate({"manifest_path": "submission.json"})
        assert tools[name]["required"] == ["manifest_path"]
        assert set(tools[name]["properties"]) == {"manifest_path"}
        with pytest.raises(ValidationError):
            Draft202012Validator(tools[name]).validate({"kind": "json", "data": {"ready": True}})
    Draft202012Validator(tools["taskhub_complete"]).validate({
        "task_id": "task", "revision": 1, "evidence": {"objective": {"artifact_ids": ["accepted-result"],
        "explanation": "Published evidence satisfies the original objective."}}, "reason": "All conditions hold."})
    assert "taskhub_completion_declare" not in tools
    with pytest.raises(ValidationError):
        Draft202012Validator(tools["taskhub_complete"]).validate({"kind": "json", "data": {}})
    with pytest.raises(ValidationError):
        Draft202012Validator(tools["taskhub_review"]).validate({
            **review_arguments({"candidate_id": "candidate", "candidate_hash": "a" * 64,
                                "task_revision": 1, "input_refs": [], "policy": "general.json@1"}),
            "verdict": "approve",
        })
