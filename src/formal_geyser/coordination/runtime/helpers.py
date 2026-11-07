"""One-use decisions of the cheap decision model, each in a new conversation without tools."""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any

from jsonschema import ValidationError as SchemaError
from jsonschema import validate

from ..errors import HubError
from ..faults import ACCOUNT_HOLDS
from ..storage import canonical
from ..waker import wake_schema
from .diagnostics import normalize_error
from .handoff import handoff_path, sections
from .helper_entries import assignment_entry, recruitment_entry
from .model_view import render_prompt

logger = logging.getLogger(__name__)

ASSIGNMENT_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "properties": {"worker": {"type": ["string", "null"]}},
    "required": ["worker"],
}
RECRUITMENT_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "properties": {"decision": {"type": "boolean"}},
    "required": ["decision"],
}
# A wake decision's schema names the pairs it is offered (waker.wake_schema).
DECISION_SCHEMAS = {"assignment": ASSIGNMENT_SCHEMA, "recruitment": RECRUITMENT_SCHEMA, "wake": wake_schema([])}
# A pause stopped the decision before it answered: it is asked again, and it is no failure.
RUN_CONTROL = "run_control"
# The handoff sections a recruitment decision reads, when the first executor wrote them.
RECRUITMENT_HANDOFF = ("Current state", "Next step", "Remaining plan")
# The usage limits that move a backend to its next profile (account_profiles): the backend, and
# the activation's attribute naming its profile.
PROFILE_LIMITS = {"claudeUsageLimit": ("claude", "claude_profile"), "usageLimitExceeded": ("codex", "codex_profile")}


def decision_schema(intent: dict) -> dict:
    """The answer's schema; an assignment names one of the offered workers, or null."""
    if intent["kind"] != "assignment":
        return DECISION_SCHEMAS[intent["kind"]]
    names = [item["name"] for item in intent["candidates"]["workers"]]
    return {**ASSIGNMENT_SCHEMA, "properties": {"worker": {"type": ["string", "null"], "enum": [*names, None]}}}
# An answer outside its schema, or none within the time limit, means a new worker, or no collaborator;
# a wake decision has no default: the waker is asked again (waker.Waker._complete_wake).
DEFAULT_DECISIONS: dict[str, dict[str, Any]] = {"assignment": {"worker": None}, "recruitment": {"decision": False}}
# How long a decision turn may run. A pause stops the turn and its clock; it is asked again later.
DECISION_SECONDS = 600.0
DECISION_INSTRUCTIONS = (
    "You make one decision for a task coordinator. Everything you need is in the message: do not "
    "run commands, read files or use tools. Answer only with the JSON object your output schema asks for.")
INVITATION = ("Join this task as its second executor: take on a part that can proceed independently of "
              "the first executor's work, and agree on it with them.")


class DecisionHelpers:
    """Runner mixin; persistent intent state belongs to TaskHub."""

    async def _note_account_hold(self, record, *, subject_id, worker_id, harness=None):
        """An explicit, final Codex account error stops new dispatch until the operator resumes.
        A sign-in error of a process older than the current sign-in is its own: each sign-in
        revokes the previous one's tokens, which running processes keep. That turn alone ends,
        and its next activation starts with the current sign-in. In the pilot every one of nine
        holds in 40 minutes came from such a process, one after each sign-in."""
        hold = getattr(self.hub, "hold_dispatch", None)
        if record.get("code") not in ACCOUNT_HOLDS or record.get("will_retry") or not callable(hold):
            return
        if record["code"] == "unauthorized" and getattr(harness, "signed_in_again", lambda: False)():
            logger.warning("Codex sign-in error of %s, whose process predates the current sign-in: "
                           "Codex dispatch is not held", subject_id)
            return
        backend, profile_attribute = PROFILE_LIMITS.get(record["code"], (None, None))
        profile = getattr(harness, profile_attribute, None) if profile_attribute else None
        limited = getattr(self.hub, f"{backend}_limit_reached", None) if backend else None
        if profile and callable(limited):
            # A profile's usage limit moves its backend to the next profile; dispatch waits only
            # while every profile is at its limit, and resumes by itself at the first reset.
            try:
                await limited(profile, record.get("message", ""), subject_id=subject_id, worker_id=worker_id)
                return
            except Exception as error:
                # Unrecorded, the limit holds the backend's dispatch as before, until Resume.
                logger.warning("%s profile limit not recorded, so its dispatch is held: %s", backend, type(error).__name__)
        try:
            await hold(record["code"], record.get("message", ""), subject_id=subject_id, worker_id=worker_id)
        except Exception as error:
            # The hold already stops dispatch in this process; the write is retried.
            logger.warning("Dispatch hold write deferred: %s", type(error).__name__)

    if TYPE_CHECKING:
        hub: Any
        harness_factory: Any
        transcripts: Any
        _command: Any
        _helpers: Any
        _interrupt_for_control: Any
        _present: Any
        _new_harness: Any
        _action_arguments: Any
        activity: Any

    async def _commit_helper(self, intent: dict, decision: dict | None) -> dict:
        if intent["kind"] == "wake":
            result = await self.hub.complete_wake(intent["id"], decision)
            return {"helper_id": intent["id"], "directive": "wake_decided", "result": result}
        assert decision is not None
        if intent["kind"] == "recruitment":
            result = await self.hub.decide_recruitment(
                intent["id"], decision["decision"], INVITATION if decision["decision"] else "")
            return {"helper_id": intent["id"], "directive": "recruitment_decided", "result": result}
        assigned = await self.hub.complete_assignment(intent["id"], decision["worker"])
        return {"helper_id": intent["id"], "directive": "assigned", **assigned}

    async def _recruitment_progress(self, intent: dict) -> dict | None:
        def read(c):
            row = c.execute(
                "SELECT id,task_revision,checkpoint_json FROM attempts WHERE worker_id=? AND task_id=? "
                "AND checkpoint_json IS NOT NULL ORDER BY created_at DESC,rowid DESC LIMIT 1",
                (intent["source_worker_id"], intent["source_task_id"])).fetchone()
            if row is None:
                return None
            return {"attempt_id": row["id"], "task_revision": row["task_revision"],
                    "checkpoint": json.loads(row["checkpoint_json"])}
        progress = await self.hub.db.read(read) or {}
        path = self.hub.workspace(intent["source_worker_id"]) / handoff_path(intent["source_task_id"])
        try:
            found = sections(await asyncio.to_thread(path.read_text, encoding="utf-8"))
        except (OSError, UnicodeDecodeError):
            found = {}
        handoff = {heading: found[heading.casefold()] for heading in RECRUITMENT_HANDOFF if found.get(heading.casefold())}
        if handoff:
            progress["handoff"] = handoff
        return progress or None

    async def _helper_may_run(self, intent: dict, *, finishing: bool = False) -> bool:
        """A recruitment decision is part of its worker's own task; an assignment decision needs
        the whole run, or a group of workers resumed while it is paused. Neither runs for a
        paused worker, except that a decision turn already under way finishes under Pause after
        turn (``finishing``)."""
        control = await self.hub.run(intent["run_id"], worker_id=intent["source_worker_id"])
        if finishing and (control.get("pause_mode") == "after_turn" or (
                control.get("pause_mode") is None and control.get("run_state") == "paused"
                and control.get("pause_after_turn"))):
            return True
        if intent["kind"] == "recruitment":
            return control["state"] == "running"
        return control["assignment_open"] and intent["source_worker_id"] not in control["paused_workers"]

    async def _commit_saved(self, intent: dict, decision: dict | None) -> dict:
        """Commit a decision whose native turn has ended. While its worker may not run, the
        saved decision waits and is committed once it may, without another model call."""
        deferred = {"helper_id": intent["id"], "directive": "helper_deferred"}
        if not await self._helper_may_run(intent):
            return deferred
        try:
            return await self._commit_helper(intent, decision)
        except HubError as error:
            if error.code in {"RUN_PAUSED", "WORKER_PAUSED"}:
                return deferred
            raise

    async def _helper_cancellation(self, intent: dict) -> str | None:
        if intent["kind"] != "recruitment":
            return None

        def read(c):
            lifecycle = self.hub._lifecycle(c, intent["source_task_id"])
            if lifecycle["opportunity_state"] == "cancelled":
                return lifecycle["closed_reason"] or "recruitment_cancelled"
            return None
        return await self.hub.db.read(read)  # Polled while the decision runs: off the writer.

    async def _decision_packet(self, intent: dict) -> tuple[dict | None, dict]:
        if intent["kind"] == "wake":
            return None, await self.hub.wake_packet(intent)
        task = await self.hub.task(intent["source_task_id"])
        if intent["kind"] == "assignment":
            return task, assignment_entry(task, intent["candidates"])
        return task, recruitment_entry(task, await self._recruitment_progress(intent))

    async def run_helper(self, intent: dict) -> dict:
        # A caller may retain an old prepared/running object after a failed
        # commit. Durable uncertainty must win over retained decision output.
        intent = await self.hub.db.call(lambda c: self.hub._helper(c, intent["id"]))
        if intent["state"] not in {"prepared", "running"} or (
                intent["state"] == "running" and not intent.get("raw_result_json")):
            raise HubError("HELPER_RECONCILIATION_REQUIRED", "Do not replay a running or held helper")
        if intent["kind"] not in DECISION_SCHEMAS:
            raise HubError("INVALID_INPUT", "Only assignment, recruitment and wake decisions run", 422)
        self._helpers.add(intent["id"])
        return await DecisionTurn(self, intent).run()


class DecisionTurn:
    """One decision's model turn, from its prepared intent to its committed answer: what
    DecisionHelpers.run_helper runs. Its attributes are the state its native callbacks share."""

    def __init__(self, runner: DecisionHelpers, intent: dict):
        self.runner, self.hub, self.intent = runner, runner.hub, intent
        self.helper_id, self.worker_id = intent["id"], intent["source_worker_id"]
        self.identity = {"run_id": intent["run_id"], "helper_id": self.helper_id,
                         "helper_kind": intent["kind"], "source_worker_id": self.worker_id}
        self.harness: Any = None
        self.session: Any = None
        self.input_id: str | None = None
        self.saved_result = False
        self.visible_saved = False
        self.terminal_saved = False
        self.native_turn_id: str | None = None
        self.outcome: Any = None
        self.inference_dispatched = False
        self.accounting_uncertain = False
        self.stopped_by_control = False
        self.timed_out = False
        self.previous_total: int | None = None
        self.charged_total = 0
        self.packet: tuple[dict | None, dict] | None = None  # A wake decision's, read before its schema.
        # Decisions use the cheap decision model on the Codex account, whatever the worker's backend.
        self.backend = "codex"

    async def run(self) -> dict:
        runner, intent = self.runner, self.intent
        try:
            await runner.activity.emit("activation_started", **self.identity)
            raw = intent.get("raw_result_json")
            if raw:
                result = json.loads(raw) if isinstance(raw, str) else raw
                return await runner._commit_saved(intent, result["decision"])
            if intent["state"] != "prepared":
                raise HubError("HELPER_RECONCILIATION_REQUIRED", "Do not replay an uncertain helper")
            if intent["kind"] == "assignment" and not await self.hub.assignment_offer_open(intent):
                # Its task changed meanwhile: the decision ends without a model call.
                return await runner._commit_helper(intent, {"worker": None})
            if intent["kind"] == "wake":
                self.packet = await runner._decision_packet(intent)
                names = [pair["pair"] for pair in self.packet[1]["pairs"]]
                if not names:  # Every wait it was to judge ended meanwhile.
                    return await runner._commit_helper(intent, {})
                schema = wake_schema(names)
            else:
                schema = decision_schema(intent)
            prompt = await self._open(schema)
            cancelled = await self._cancelled_before_input()
            if cancelled is not None:
                return cancelled
            cancellation = await self._run_turn(prompt)
            ended = await self._ended_without_answer(cancellation)
            if ended is not None:
                return ended
            decision = await self._answer(schema)
            return await runner._commit_saved(intent, decision)
        except BaseException as exc:
            await self._failed(exc)
            raise
        finally:
            await self._close()

    async def _open(self, schema: dict) -> str:
        """Start the decision's own new conversation and prepare its single input."""
        runner, hub, intent, helper_id, worker_id = self.runner, self.hub, self.intent, self.helper_id, self.worker_id
        preferences = await (hub.waker_preferences() if intent["kind"] == "wake" else hub.decision_preferences())
        self.harness = harness = await runner._new_harness(self.backend, model=preferences["model"],
                                                           effort=preferences["effort"], decision=True)
        diagnostic_setter = getattr(harness, "set_diagnostic_handler", None)
        if diagnostic_setter is not None:
            diagnostic_setter(self._diagnostic)
        control_setter = getattr(harness, "set_control_handler", None)
        if control_setter is not None:
            control_setter(self._account_control)
        activity_setter = getattr(harness, "set_activity_handler", None)
        if activity_setter is not None:
            activity_setter(self._activity)
        task, packet = self.packet or await runner._decision_packet(intent)
        turn_options = getattr(harness, "set_turn_options", None)
        if turn_options:
            turn_options(output_schema=schema)  # The answer is one JSON object of this schema.
        # A new conversation without tools, history or files beyond the message: it runs
        # read-only over an empty directory, never in a worker's workspace.
        workspace = hub.state_dir / "decision-workspace"
        await asyncio.to_thread(workspace.mkdir, parents=True, exist_ok=True)
        self.session = session = await harness.open(
            workspace=workspace, thread_id=None,
            tools=[], instructions=DECISION_INSTRUCTIONS, read_only=True)
        await hub.note_owned_process("helper", helper_id, getattr(harness, "ownership_receipt", None))
        runner.transcripts.record_instructions(worker_id, session.thread_id, {
            "instructions": DECISION_INSTRUCTIONS, "sandbox": "read-only", "tools": [],
            "codex_profile": {**hub.config.runtime_profile(self.backend),
                              "model": preferences["model"], "effort": preferences["effort"]}},
            helper_id=helper_id)
        await runner._command("helper.started", {"intent_id": helper_id, "thread_id": session.thread_id})
        await runner.activity.emit("session_ready", **self.identity, thread_id=session.thread_id)
        native_setter = getattr(harness, "set_native_handler", None)
        if native_setter:
            native_setter(self._native)
        prompt = render_prompt(await runner._present(packet))
        # The prompt carries no identifiers; the transcript still links the decision to its task.
        self.input_id = runner.transcripts.prepare_input(worker_id, session.thread_id, "taskhub",
            prompt, helper_id=helper_id, kind=intent["kind"],
            **({"task": {"id": task["id"], "revision": task["revision"], "title": task["contract"]["title"],
                         "kind": task["contract"]["kind"]}} if task is not None else {}))
        if turn_options:
            turn_options(output_schema=schema, client_input_id=self.input_id)
        return prompt

    async def _cancelled_before_input(self) -> dict | None:
        """The last checks before the input goes: run control, a cancelled decision, account holds."""
        runner, harness = self.runner, self.harness
        if not await runner._helper_may_run(self.intent):
            raise HubError("RUN_PAUSED", "Project control superseded the helper")
        cancellation = await runner._helper_cancellation(self.intent)
        if cancellation:
            await harness.close()
            if not harness.process_quiet:
                raise HubError("HELPER_RECONCILIATION_REQUIRED", "Cancelled helper transport is not confirmed quiet")
            await runner._command("helper.cancelled", {"intent_id": self.helper_id,
                "process_quiet": True, "terminal_observed": False,
                "inference_dispatched": False, "reason": cancellation})
            return {"helper_id": self.helper_id, "directive": "helper_cancelled", "reason": cancellation}
        require_dispatch = getattr(self.hub, "require_dispatch", None)
        if callable(require_dispatch):
            # Last check before model input is sent; nothing is awaited after it.
            await require_dispatch(self.backend)
        return None

    async def _run_turn(self, prompt: str) -> str | None:
        """Send the input and wait for the turn's outcome; a cancellation, run control or the time
        limit stops it. Returns the decision's cancellation reason, if any."""
        runner, harness, intent = self.runner, self.harness, self.intent
        cancellation = None
        turn = asyncio.create_task(harness.run_turn(prompt, on_tool=self._tool, on_usage=self._usage,
            on_started=self._started))
        self.inference_dispatched = True
        deadline = asyncio.get_running_loop().time() + DECISION_SECONDS
        try:
            while not turn.done():
                done, _ = await asyncio.wait([turn], timeout=0.2)
                if done:
                    break
                cancellation = await runner._helper_cancellation(intent)
                if cancellation or not await runner._helper_may_run(intent, finishing=True):
                    self.stopped_by_control = cancellation is None
                    self.outcome = await runner._interrupt_for_control(
                        harness, turn, cancellation or "Run control stopped decision helper")
                    break
                if asyncio.get_running_loop().time() >= deadline:
                    self.timed_out = True
                    self.outcome = await runner._interrupt_for_control(
                        harness, turn, "The decision reached its time limit")
                    break
            if self.outcome is None:
                self.outcome = await turn
            await runner.activity.emit("turn_finished", **self.identity, turn_id=self.outcome.turn_id,
                                       status=self.outcome.status, process_quiet=self.outcome.process_quiet)
        finally:
            if not turn.done():
                turn.cancel()
                await asyncio.gather(turn, return_exceptions=True)
        outcome = self.outcome
        if outcome.terminal_observed and not self.terminal_saved:
            runner.transcripts.capture_native(self.session.thread_id, self.worker_id, "turn/completed", {
                "turn": {"id": outcome.turn_id, "status": outcome.status}}, helper_id=self.helper_id,
                source="runtime_terminal")
            self.terminal_saved = True
        return cancellation or await runner._helper_cancellation(intent)

    async def _ended_without_answer(self, cancellation: str | None) -> dict | None:
        """A turn stopped by its cancellation or by run control ends the decision for now; one
        stopped at its time limit goes on to the default decision; any other turn that did not
        complete cleanly needs reconciliation."""
        outcome = self.outcome
        # Its end was observed, its processes are quiet and its usage was recorded.
        quiet = (not self.accounting_uncertain and outcome.terminal_observed
                 and outcome.process_quiet and self.harness.process_quiet)
        cancelled = (cancellation if cancellation and quiet
                     and outcome.status in {"completed", "failed", "interrupted", "cancelled"}
                     else RUN_CONTROL if self.stopped_by_control and quiet and outcome.status != "completed"
                     else None)
        if cancelled is not None:
            await self.runner._command("helper.cancelled", {"intent_id": self.helper_id, "process_quiet": True,
                "terminal_observed": True, "inference_dispatched": True, "reason": cancelled})
            return {"helper_id": self.helper_id, "directive": "helper_cancelled", "reason": cancelled}
        if not (outcome.process_quiet and self.harness.process_quiet and outcome.terminal_observed
                and (outcome.status == "completed" or self.timed_out)):
            raise HubError("HELPER_RECONCILIATION_REQUIRED", outcome.error or "Helper did not complete cleanly")
        return None

    async def _answer(self, schema: dict) -> dict:
        """The decision the turn answered, saved before it is committed. An answer outside the
        schema, or a turn stopped at its time limit, counts as the kind's default."""
        runner, intent, outcome = self.runner, self.intent, self.outcome
        if self.accounting_uncertain:
            await self.hub.note_helper_accounting(self.helper_id, "Native helper usage could not be fully recorded")
        # Only a turn stopped at its time limit gets here unfinished (see _ended_without_answer).
        decision: Any = None
        default = "timed_out" if outcome.status != "completed" else None
        if default is None:
            try:
                decision = json.loads(outcome.final_output or outcome.visible_output)
                validate(decision, schema)
            except (ValueError, TypeError, SchemaError):
                default = "invalid_output"
        if default is not None:
            decision = None if intent["kind"] == "wake" else dict(DEFAULT_DECISIONS[intent["kind"]])
            logger.warning("Decision %s: %s; using %s", self.helper_id, default.replace("_", " "), decision)
            await runner.activity.emit("decision_timed_out" if default == "timed_out" else "decision_invalid",
                                       **self.identity, turn_id=outcome.turn_id)
        # A synthetic harness may omit native notifications; retain its exact
        # visible result too, with native turn/thread identities.
        if not self.visible_saved:
            runner.transcripts.capture_native(self.session.thread_id, self.worker_id, "item/completed", {
                "turnId": outcome.turn_id, "item": {"id": "helper-result:" + self.helper_id,
                "type": "agentMessage", "text": outcome.visible_output}}, helper_id=self.helper_id)
        await runner._command("helper.result", {"intent_id": self.helper_id, "process_quiet": True,
            "result": {"decision": decision, "visible_output": outcome.visible_output,
                       **({default: True} if default else {})}})
        self.saved_result = True
        return decision

    async def _failed(self, exc: BaseException) -> None:
        runner, outcome, harness = self.runner, self.outcome, self.harness
        await self._diagnostic(normalize_error(exc, source="decision_helper"))
        if self.saved_result or self.intent.get("raw_result_json"):
            # The native turn and its output are known. A failed decision
            # commit is a local recovery hold, never another inference call.
            await self.hub.helper_uncertain(self.helper_id, str(exc), inference_uncertain=False)
            return
        terminal_known = bool(outcome and outcome.terminal_observed
                              and outcome.status in {"completed", "failed", "interrupted", "cancelled"}
                              and outcome.process_quiet and harness is not None and harness.process_quiet)
        inference_uncertain = self.accounting_uncertain or (self.inference_dispatched and not terminal_known)
        if self.input_id and self.inference_dispatched and not terminal_known:
            runner.transcripts.ambiguous(self.input_id, exc)
            if self.session is not None and not self.terminal_saved:
                runner.transcripts.record_uncertain_turn(self.worker_id, self.session.thread_id,
                    self.native_turn_id, self.input_id, helper_id=self.helper_id)
        await self.hub.helper_uncertain(self.helper_id, str(exc), inference_uncertain=inference_uncertain)

    async def _close(self) -> None:
        harness = self.harness
        try:
            if harness is not None:
                await harness.close()
            if harness is None or harness.process_quiet:
                await self.hub.note_helper_quiet(self.helper_id)
            await self.runner.activity.emit("activation_finished", **self.identity,
                                            process_quiet=harness.process_quiet if harness is not None else True)
        finally:
            self.runner._helpers.discard(self.helper_id)

    # -- Callbacks while the turn runs --------------------------------------------------------

    async def _diagnostic(self, record):
        sha = await asyncio.to_thread(self.hub.blobs.put_json, {**self.identity, **record})
        await self.runner.activity.emit("runtime_error", **self.identity, diagnostic_sha256=sha,
                                        status="retrying" if record.get("will_retry") else "error",
                                        error_type=str(record.get("code", "native_error")))

    async def _account_control(self, record):
        await self.runner._note_account_hold(record, subject_id=self.helper_id, worker_id=self.worker_id,
                                             harness=self.harness)

    async def _activity(self, event, fields):
        await self.runner.activity.emit(event, **{**fields, **self.identity})

    async def _native(self, method, params):
        try:
            self.runner.transcripts.capture_native(self.session.thread_id, self.worker_id, method, params,
                                                   helper_id=self.helper_id)
        except Exception as error:
            logger.warning("Helper transcript observation deferred: %s", type(error).__name__)
            return
        self.visible_saved |= method == "item/completed" and params.get("item", {}).get("type") == "agentMessage"
        self.terminal_saved |= method == "turn/completed"

    async def _usage(self, sample):
        total = sample.get("totalTokens")
        last = sample.get("lastTokenUsage")
        if type(total) is not int or not isinstance(last, dict) or type(last.get("totalTokens")) is not int:
            self.accounting_uncertain = True
            return
        delta = last["totalTokens"] if self.previous_total is None else total - self.previous_total
        if delta < 0:
            self.accounting_uncertain = True
            return
        if self.previous_total == total:
            return
        self.previous_total = total
        sample_id = hashlib.sha256((self.session.thread_id + canonical(sample)).encode()).hexdigest()
        try:
            await self.runner._command("helper.usage", {"intent_id": self.helper_id, "sample_id": sample_id,
                                                        "sample": sample, "delta_tokens": delta},
                                       key="helper-usage:" + sample_id)
            self.charged_total += delta
            await self.runner.activity.emit("usage", **self.identity, thread_id=self.session.thread_id,
                                            turn_id=self.native_turn_id, total_tokens=self.charged_total)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.accounting_uncertain = True

    async def _started(self, turn_id):
        self.native_turn_id = turn_id
        await self.runner._command("helper.turn.bind", {"intent_id": self.helper_id, "remote_turn_id": turn_id})
        self.runner.transcripts.delivered(self.input_id, turn_id)
        await self.runner.activity.emit("turn_started", **self.identity, thread_id=self.session.thread_id,
                                        turn_id=turn_id)

    async def _tool(self, name, args, call_id):
        # A decision has no tools; any call is answered without effect.
        self.runner.transcripts.record_action(self.worker_id, self.session.thread_id, self.native_turn_id,
            call_id, name, "error", helper_id=self.helper_id, **self.runner._action_arguments(args))
        return {"error": "A decision has no tools; answer with the JSON object only"}
