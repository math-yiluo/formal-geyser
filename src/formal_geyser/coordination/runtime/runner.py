"""Worker activations with control checks and quiescent transitions."""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import time
import traceback
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4
from weakref import WeakValueDictionary

from ...bootstrap_runtime import NOT_READY
from ...bootstrap_runtime import resource_roots as prepared_resource_roots
from ..commands import CommandTable
from ..errors import HubError, ValidationContinues
from ..json_io import read_json
from ..material_files import MaterialFiles
from ..models import Actor, Command, Role, ValidationReport, file_name_fields
from ..plugins import load_plugin
from ..shared_dags import prepare_dag_checker
from ..storage import canonical, collected
from ..transcript_digest import digest as transcript_digest
from ..transcript_digest import search as search_history
from ..transcripts import TranscriptStore
from . import review_report, state_packet
from .actions import ActionCatalog, ActionError, validate_gateway
from .codex import CodexHarness, DecisionHarness, Harness, ToolResult, TurnOutcome
from .continuation import ContinuationPromptStore, PreparedPrompt
from .control_monitor import ControlMonitor
from .diagnostics import normalize_error
from .entries import MESSAGE_MODES, message_state, scenario_entry
from .files import ResourceRoots, ScopedFiles, resource_tools, workspace_tools
from .handoff import (
    JOURNAL_PATH,
    HandoffCheck,
    check_handoff,
    handoff_path,
    handoff_sha256,
    handoff_template,
)
from .helpers import DecisionHelpers
from .instructions import prepare_guidance, standing_anchor, standing_guides
from .model_view import project_packet, render_prompt, sync_view, task_file, tool_view
from .native_files import native_file, native_resource
from .operation_reference import prepare_operation_reference
from .process import ProcessHostError
from .review_entry import prepare_review_entry
from .rpc import RpcClosed, RpcTimeout
from .telemetry import ActivityLog
from .tools import WORKER_INSTRUCTIONS, dynamic_tools, operation_tools

# Polling intervals on the shared coordination database. A finished execution
# still wakes admission immediately; these bound only the periodic checks.
ADMISSION_INTERVAL_SECONDS = 1.0
# One pass admits for at most this long before recovery, validation retries and decision
# helpers get their turn; the next pass starts at once.
ADMISSION_PASS_SECONDS = 30.0
# Before the driver attaches again to a validation whose attempt failed.
VALIDATION_RETRY_SECONDS = 30.0
# Library checkouts prepared at once by activations; more would only queue on the thread pool.
WORKSPACE_PREPARATIONS = 4
# How often the control checks of turns and validations under way are read, all at once.
CONTROL_WATCH_SECONDS = 0.5
# How often a conversation's timed-out steers are looked up in its native history.
STEER_SETTLE_SECONDS = 15.0
# How long a finished turn waits for its queued usage samples to be recorded before
# `turn.finish`, which takes the turn's total from them.
USAGE_FLUSH_SECONDS = 300.0

NATIVE_COMMAND_OPERATIONS = frozenset({
    "batch_publish", "batch_finish_empty", "batch_defer", "batch_retry",
    "publish", "checkpoint", "stage", "contribute", "complete", "talk_send",
    "propose_revision", "subscribe",
    "resume_work", "leave_task", "withdraw", "invitation_update", "invitation_withdraw",
    "completion_feedback", "completion_finalize", "message_send", "literature_register",
    "request_resolve", "pause", "request", "review", "wiki_submit", "draft_update", "dag_update",
})
VALIDATION_HOLDS = frozenset({
    NOT_READY,
    "RUN_PAUSED", "WORKER_PAUSED", "RUN_STOPPED", "PROJECT_STOPPED", "USAGE_UNCERTAIN", "WORKER_DRAINING",
    "STALE_BINDING", "STALE_REVISION", "INPUTS_CHANGED", "ARTIFACT_WITHDRAWN", "VALIDATION_RECONCILIATION_REQUIRED",
    "WORKER_RECOVERY_REQUIRED", "VALIDATION_QUEUED",
})
VALIDATION_QUEUED = ("Machine validation of {artifact_id} continues in the background and its result arrives as a "
                     "notification. Keep working on something else, or pause with predicate "
                     '{{"validations": ["{artifact_id}"]}} to wait for it.')


logger = logging.getLogger(__name__)
PART_BYTES = 4096  # A field at least this large gets its own material file.


def _steer_scope(run: dict) -> str | None:
    """Which directed messages are steered into a turn under way: all while the run runs for
    the person. Under Pause after turn, which lets that turn finish, only a person's (such as
    an operator asking a draining worker to wrap up); a peer's waits for a later turn, so peers
    do not keep each other's turns going. None under any other pause."""
    if run["state"] == "running":
        return "all"
    return "human" if run.get("pause_mode") == "after_turn" else None


def _outline(value: Any) -> str:
    """One line on what a JSON value holds."""
    if isinstance(value, dict):
        keys = list(value)
        return "object: " + ", ".join(map(str, keys[:12])) + (", …" if len(keys) > 12 else "")
    if isinstance(value, list):
        first = value[0] if value else None
        return f"list of {len(value)}" + (
            "; each like: " + ", ".join(map(str, list(first)[:8])) if isinstance(first, dict) else "")
    if isinstance(value, str):
        text = " ".join(value[:160].split())
        return "text: " + text + ("…" if len(value) > 160 else "")
    return type(value).__name__


def _material_parts(files: MaterialFiles, value: Any, path: tuple[str, ...] = (),
                    limit: int = 24) -> list[dict[str, Any]]:
    """The large fields of an oversized result, two levels deep, each in its own file."""
    parts: list[dict[str, Any]] = []
    if not isinstance(value, dict):
        return parts
    for key, item in value.items():
        body = canonical(item).encode("utf-8")
        if len(body) < PART_BYTES:
            continue
        field = (*path, str(key))
        reference = files.write(body)
        parts.append({"field": ".".join(field), "material_path": reference["material_path"],
                      "bytes": len(body), "outline": _outline(item)})
        if len(field) < 2:
            parts.extend(_material_parts(files, item, field, limit))
    return sorted(parts, key=lambda part: (part["field"].count("."), -part["bytes"]))[:limit]

ROTATION_REMINDER = ("This conversation's context is past {limit} tokens. When the command you are running "
                     "finishes, update your handoff file {path} and your journal {journal}, then end this turn: "
                     "the task continues in a new conversation that starts from that file.")
ROTATION_START = ("This task continues from an earlier conversation of yours, which was replaced because {reason}. "
                  "First read rotation.handoff, your own handoff file, then rotation.state, TaskHub's record of the "
                  "task, and the files your handoff's Next step names; the other files listed there are for lookup. "
                  "Check the current state before you continue: recompile the modules you were changing and "
                  "inspect the candidates you submitted. Your journal is " + JOURNAL_PATH + "; history_search finds "
                  "anything else from earlier conversations.")
REVIEW_CLARIFICATION = ("Answer the directed messages about your review, then finish. This turn runs in a new "
                        "conversation{report}; history_search finds anything else from your earlier conversation. "
                        "This is a clarification turn, not a new review.")
ROTATION_REPAIR = ("This task is about to continue in a new conversation, but your handoff file {path} is not ready: "
                   "{problems}. Fix it, then end this turn.")
# Each batch starts in a new conversation; a batch that takes more turns continues in it.
INTEGRATION_RULE = ("inspect batch_status and its files. Publish only a freshly validated final tree, or isolate "
                    "blocked items. Receive new arrivals in the next batch.")
INTEGRATION_START = "Integrate the assigned fixed batch: " + INTEGRATION_RULE
INTEGRATION_CONTINUE = "Continue the assigned fixed integration batch; " + INTEGRATION_RULE


def _with_field(result: Any, key: str, value: Any) -> Any:
    """A tool result with one more field, keeping its delivery acknowledgement."""
    after = getattr(result, "after_send", None)
    merged = {**result, key: value}
    return ToolResult(merged, after_send=after) if after is not None else merged


COMPACTION_REFRESH = (
    "Context was compacted. Here is your current task view; your standing instructions, with the required "
    "guides, are unchanged. Preserve the supplied assignment and any message-only authority.")


# Operations that are one TaskHub command, by tool name; hub_command chooses the command of the
# operations whose arguments decide it.
HUB_COMMANDS = {
    "taskhub_publish": "task.publish", "taskhub_checkpoint": "task.checkpoint",
    "taskhub_stage": "artifact.stage", "taskhub_contribute": "artifact.submit",
    "taskhub_complete": "task.complete.declare",
    "taskhub_talk_send": "talk.send",
    "taskhub_propose_revision": "task.revise",
    "taskhub_subscribe": "task.subscribe",
    "taskhub_resume_work": "worker.resume_work",
    "taskhub_leave_task": "task.leave",
    "taskhub_invite_back": "task.invite_back",
    "taskhub_withdraw": "artifact.withdraw",
    "taskhub_invitation_update": "task.invitation.update",
    "taskhub_invitation_withdraw": "task.invitation.withdraw",
    "taskhub_completion_feedback": "task.complete.feedback",
    "taskhub_completion_finalize": "task.complete.finalize",
    "taskhub_message_send": "message.send",
    "taskhub_literature_register": "literature.register",
    "taskhub_request_resolve": "request.resolve",
    "taskhub_draft_update": "task.draft.update",
    "taskhub_dag_update": "dag.update",
}


def hub_command(name: str, args: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """The TaskHub command an operation is, with its arguments; None for the other tools."""
    if name == "taskhub_pause":
        return ("task.wait" if args.get("predicate") else "task.pause"), args
    if name == "taskhub_request":
        args = dict(args)
        return ("literature.request" if args.pop("kind") == "literature" else "message.request"), args
    if name == "taskhub_review":
        if args.get("verdict") not in {"accept", "reject"}:
            raise HubError("INVALID_INPUT", "A review decision must accept or reject the candidate", 422)
        return ("review.accept" if args["verdict"] == "accept" else "review.reject"), args
    command = HUB_COMMANDS.get(name)
    return None if command is None else (command, args)


# The other framework tools' handlers, by tool name.
OPERATIONS = CommandTable()


@dataclass(frozen=True)
class _Call:
    """One operation call: whose it is, its idempotency key, and the room its result has."""
    attempt: dict[str, Any]
    thread_id: str
    actor: Actor
    key: str
    packet_bytes: int
    native_call: dict | None


class WorkerRunner(DecisionHelpers):
    recent_result_limit = 100
    retained_result_bytes = 16384

    def __init__(self, hub, harness_factory: Callable[[], Harness] | None = None):
        self.hub = hub
        self.harness_factory = harness_factory or (lambda: CodexHarness(hub.config.codex))
        self._native_harness = harness_factory is None
        self._access_checked = False
        self._access_lock = asyncio.Lock()
        self._prepared_environment: dict[str, Any] | None = None
        self._admission_lock = asyncio.Lock()
        self._preparations = asyncio.Semaphore(WORKSPACE_PREPARATIONS)
        self._executing: set[str] = set()
        self._helpers: set[str] = set()
        self._steering_attempts: set[str] = set()
        self._delivery_locks: dict[str, asyncio.Lock] = {}
        self.controls = ControlMonitor(hub, CONTROL_WATCH_SECONDS)
        self._native_call_locks: WeakValueDictionary[tuple[str, str, str], asyncio.Lock] = WeakValueDictionary()
        self.transcripts = TranscriptStore(hub.state_dir.parent)
        self.continuation_prompts = ContinuationPromptStore(hub.state_dir.parent)
        self.activity = ActivityLog(hub.state_dir)
        self.plugin = load_plugin(hub.config.domain)
        resource_roots = dict(hub.config.environment.get("resource_roots", {}))
        draft_root = hub.draft_files.ensure()
        for resource_name, resource_path in resource_roots.items():
            if resource_name.casefold() == "drafts" and (
                    resource_name != "drafts" or Path(resource_path).resolve() != draft_root.resolve()):
                raise ValueError("The drafts resource name is reserved for this project's public draft directory")
        resource_roots["drafts"] = str(draft_root)
        graph_root = hub.dag_files.ensure()
        for resource_name, resource_path in resource_roots.items():
            if resource_name.casefold() == "graphs" and (
                    resource_name != "graphs" or Path(resource_path).resolve() != graph_root.resolve()):
                raise ValueError("The graphs resource name is reserved for this project's shared DAG directory")
        resource_roots["graphs"] = str(graph_root)
        self._base_resource_roots = dict(resource_roots)
        self.resources = ResourceRoots(resource_roots)
        self.tools = dynamic_tools()
        from .integrator import integrator_operations
        operations = operation_tools() + workspace_tools() + integrator_operations()
        if self.resources.roots:
            operations.extend(resource_tools())
        names = {tool["name"] for tool in operations}
        domain_tools: list[dict[str, Any]] = getattr(self.plugin, "worker_tools", lambda: [])()
        self.domain_names: set[str] = set()
        for tool in domain_tools:
            if (not isinstance(tool, dict) or tool.get("type") != "function"
                    or not isinstance(tool.get("name"), str)
                    or not isinstance(tool.get("description"), str)
                    or not isinstance(tool.get("inputSchema"), dict)):
                raise ValueError("Domain worker tools must be function DynamicToolSpec objects")
            name = tool["name"]
            if name in names:
                raise ValueError(f"Duplicate worker tool name: {name}")
            names.add(name)
            self.domain_names.add(name)
            operations.append(tool)
        self.actions = ActionCatalog(operations)
        if self.domain_names and not callable(getattr(self.plugin, "handle_worker_tool", None)):
            raise ValueError("Domain worker tools require handle_worker_tool")
        instructions = getattr(self.plugin, "worker_instructions", lambda: "")()
        if not isinstance(instructions, str):
            raise ValueError("Domain worker instructions must be a string")
        self.instructions = WORKER_INSTRUCTIONS + ("\n" + instructions if instructions else "")
        self.instructions += (
            "\nPrefer native file tools and commands; TaskHub workspace and resource operations are "
            "fallbacks. Shared reading locations are in paths and paths.reference_index.")
        # Each backend's own tool surface; the shared instructions stay backend-neutral.
        from .claude import CLAUDE_TOOL_NOTE
        self._backend_notes = {
            "codex": ("\nLong code-mode exec or wait calls use yield_time_ms of at least 60000. Call the shell, "
                      "taskhub_actions and taskhub_call directly, never from inside an exec cell: there, "
                      "tools.exec_command, tools.taskhub_call and ALL_TOOLS are unavailable. Your web tool, "
                      "web__run, searches the web."),
            "claude": CLAUDE_TOOL_NOTE}

    async def _new_harness(self, backend: str = "codex", *, kind: str | None = None, model: str | None = None,
                           effort: str | None = None, decision: bool = False,
                           claude_profile: dict[str, str] | None = None) -> Harness:
        """A model and effort override the backend's own: a conversation's own settings, or the
        cheap decision model; otherwise the agent's role picks its effort. A decision gets no
        native tools. A Claude activation signs in with the given profile's directory; a Codex
        activation, a worker's, a reviewer's, the Integrator's or a decision's, with the Codex
        profile in use now."""
        # Recheck previously selected work before starting any native process.
        prepared = await self.hub.execution_environment()
        self._prepared_environment = prepared
        self.resources = ResourceRoots({**self._base_resource_roots, **prepared_resource_roots(prepared)})
        from ..claude_profiles import profile_homes
        from ..codex_profiles import sign_in_homes
        from ..native_permissions import ensure_native_access, protected_reads

        # Neither backend's sandboxed commands may read the operator tokens or any sign-in.
        protected = protected_reads(sign_in_homes(self.hub.config.codex), profile_homes(self.hub.config.claude))
        if self._native_harness and backend == "claude":
            # Claude commands run in Claude Code's own OS sandbox; there is no Codex probe.
            from .claude import ClaudeHarness
            claude = self.hub.config.claude
            if kind is not None:
                claude = claude.model_copy(update={"effort": claude.effort_for(kind)})
            if model is not None or effort is not None:
                claude = claude.model_copy(update={key: value for key, value in (
                    ("model", model), ("effort", effort)) if value is not None})
            if claude_profile is not None:
                claude = claude.model_copy(update={"home": claude_profile["home"]})
            harness = ClaudeHarness(claude, prepared_environment=prepared, protected=protected)
            harness.claude_profile = claude_profile["name"] if claude_profile is not None else None
            return harness
        if self._native_harness:
            async with self._access_lock:
                if not self._access_checked:
                    await ensure_native_access(self.hub.root, self.hub.state_dir / "native-access-probe/work",
                                               self.hub.config.codex, protected)
                    self._access_checked = True
            config = self.hub.config.codex
            if effort is None and kind is not None:
                effort = config.effort_for(kind)
            if model is not None or effort is not None:
                config = config.model_copy(update={key: value for key, value in (
                    ("model", model), ("effort", effort)) if value is not None})
            chooser = getattr(self.hub, "codex_session_profile", None)
            profile = await chooser() if callable(chooser) else None
            codex = (DecisionHarness if decision else CodexHarness)(config, prepared_environment=prepared,
                                                                    protected=protected, account=profile)
            observed = getattr(self.hub, "codex_usage_observed", None)
            if profile is not None and callable(observed):
                name = profile["name"]

                async def report(snapshot: Any) -> None:
                    # What Codex reports of the profile's usage windows, shown in the Codex tab.
                    await observed(name, snapshot)
                codex.set_rate_limit_handler(report)
            return codex
        return self.harness_factory()

    async def _command(
        self, name: str, payload: dict[str, Any], *, actor: Actor | None = None,
        key: str | None = None, native_call: dict | None = None,
    ) -> dict[str, Any]:
        actor = actor or Actor(role=Role.RUNNER)
        command = Command(name=name, payload=payload, key=key or "runtime:" + uuid4().hex)
        if native_call is not None:
            command = await self.hub.prepare_native_command(native_call, actor, command)
            return await self.hub.execute(command, actor, native_call=native_call)
        return await self.hub.execute(command, actor)

    @staticmethod
    def _worker_actor(attempt: dict[str, Any]) -> Actor:
        return Actor(role=Role.WORKER, worker_id=attempt["worker_id"],
                     attempt_id=attempt["id"], fencing_token=attempt["fencing_token"])

    def _catalog_binding(self) -> dict[str, Any]:
        from .catalog import catalog_binding

        return catalog_binding(self.tools)

    def _catalog_transcript(self, binding: dict[str, Any], *, thread_id: str | None = None) -> dict[str, Any]:
        if thread_id:
            return {"tool_catalog": {
                "mode": "persisted", "source_thread_id": thread_id,
                "schemas": ("verified" if binding.get("tool_catalog_verified_thread_id") == thread_id
                            else "not_loaded"), **binding,
            }}
        return {"tools": self.tools, "tool_catalog": {"mode": "installed", **binding}}

    async def _ensure_catalog(self, worker: dict[str, Any], *, harness: Harness | None = None) -> dict[str, Any]:
        from .catalog import (
            catalog_binding,
            catalog_difference,
            migration_required,
            normalize_catalog,
        )
        from .codex import ScriptedHarness

        target_thread = worker.get("thread_id")
        if not target_thread:
            return self._catalog_binding()
        try:
            profile = json.loads(worker.get("profile_json") or "{}")
        except (TypeError, ValueError):
            profile = {}
        if not isinstance(profile, dict):
            profile = {}
        binding = {key: deepcopy(profile[key]) for key in (
            "tool_catalog_version", "tool_catalog_names", "tool_catalog_hash") if key in profile}
        if harness is None and not self._native_harness:
            # Source discovery alone opens no existing conversation. The actual
            # activation harness must pass the check below before open/turn.
            return binding
        if isinstance(harness, ScriptedHarness) and not callable(getattr(harness, "stored_tool_catalog", None)):
            return binding  # Only the model-free scripted adapter has no native catalog.
        if harness is not None and not callable(getattr(harness, "stored_tool_catalog", None)):
            raise migration_required(target_thread, "Full persisted tool definitions cannot be inspected")
        try:
            probe = await self._new_harness(worker.get("backend", "codex"))
            try:
                inspect = getattr(probe, "stored_tool_catalog", None)
                if not callable(inspect):
                    raise ValueError("Full persisted tool definitions cannot be inspected")
                saved = normalize_catalog(await inspect(
                    target_thread, workspace=self.hub.workspace(worker["id"])))
            finally:
                await probe.close()
            expected = normalize_catalog(self.tools)
            difference = catalog_difference(expected, saved)
        except HubError as exc:
            if exc.code == NOT_READY:
                raise
            raise migration_required(target_thread, str(exc)) from exc
        except (RpcTimeout, RpcClosed, ProcessHostError, ConnectionError, TimeoutError):
            # Codex could not be reached, which says nothing about the stored tools: the
            # ordinary infrastructure path backs off and retries. Nothing is opened meanwhile.
            raise
        except Exception as exc:
            raise migration_required(target_thread, str(exc)) from exc
        if difference:
            raise migration_required(target_thread, difference)
        # The digest comes from the native definitions just read, never the
        # saved profile cache or the catalog supplied to resume.
        return {**catalog_binding(saved), "tool_catalog_verified_thread_id": target_thread}

    def _start_handoff(self, worker_id: str, task_id: str, title: str, contract_path: str | None,
                       *, review: bool = False) -> None:
        """Write the task's (or the review's) handoff file from its template, unless the worker
        already has one."""
        relative = handoff_path(task_id)
        files = ScopedFiles(self.hub.workspace(worker_id))
        if not files.path(relative, must_exist=False).exists():
            files.write(relative, handoff_template(title, contract_path, review=review))

    @staticmethod
    def _capability(worker: dict[str, Any], attempt: dict[str, Any], task_kind: str | None = None) -> str:
        """The operations this activation may call, by role and execution mode."""
        message = attempt.get("execution_mode") == "message_only"
        kind = worker.get("kind")
        if message:
            return "review_answer" if kind == "review" else "integrator_answer" if kind == "integrator" else "message_only"
        if kind == "integrator":
            return "integrator"
        return "reviewer" if kind == "review" or task_kind == "review" else "worker"

    async def _standing_instructions(self, worker: dict[str, Any], attempt: dict[str, Any], backend: str) -> str:
        """The conversation's standing instructions. They are the same for every worker of a
        role, so they can be a shared cached prefix, and for every mode of one conversation: an
        ordinary worker's message turns use its work text, and the input says what the turn is."""
        kind, review = worker.get("kind", "ordinary"), worker.get("kind") == "review"
        mode = "work" if kind == "ordinary" else attempt.get("execution_mode", "work")
        standing = await asyncio.to_thread(standing_guides, self.plugin, review=review, worker_kind=kind,
                                           execution_mode=mode)
        anchor = await asyncio.to_thread(standing_anchor, self.plugin, review=review, worker_kind=kind,
                                         execution_mode=mode)
        capability = self._capability(worker, {**attempt, "execution_mode": mode})
        return "\n\n".join((self.instructions + self._backend_notes[backend], standing,
                            self.actions.cheat_sheet(capability), anchor))

    async def _rotate_if_due(self, worker: dict[str, Any], attempt: dict[str, Any],
                             instructions_sha256: str | None = None) -> dict[str, Any] | None:
        """Replace the conversation before this activation if a rotation is due.

        The runtime writes its own state packet first and checks the agent's handoff file;
        TaskHub then releases the old conversation, so this activation starts a new one.
        """
        due = getattr(self.hub, "rotation_due", None)
        if not worker.get("thread_id") or not callable(due):
            return None
        rotation = await due(worker["id"], attempt, instructions_sha256)
        if rotation is None:
            return None
        workspace, task_id = self.hub.workspace(worker["id"]), attempt["task_id"]
        handoff = rotation.get("handoff") or (await asyncio.to_thread(
            check_handoff, workspace, task_id, review=worker.get("kind") == "review")).record()
        state = await self.hub.rotation_state(worker["id"], task_id)
        history = await asyncio.to_thread(
            transcript_digest, self.transcripts.events_path(worker["id"]), rotation["thread_id"])
        text = state_packet.render(state=state, history=history, reason=rotation["reason"], mode=rotation["mode"],
                                   handoff=HandoffCheck(
                                       handoff["path"], handoff.get("exists", False), handoff.get("sha256"),
                                       handoff.get("characters", 0), tuple(handoff.get("problems", [])),
                                       tuple(handoff.get("notes", []))))
        path = await asyncio.to_thread(state_packet.write, workspace, task_id, text)
        return await self.hub.begin_rotation(rotation["id"], handoff=handoff, state_path=path)

    async def _saved_handoff(self, worker: dict[str, Any], attempt: dict[str, Any]) -> dict[str, Any] | None:
        """For a new conversation on a task whose handoff file is already filled in, the start a
        rotation would give it: the handoff file and a state packet written now. Else None."""
        workspace, task_id = self.hub.workspace(worker["id"]), attempt["task_id"]
        check = await asyncio.to_thread(check_handoff, workspace, task_id)
        if not check.valid:
            return None
        state = await self.hub.rotation_state(worker["id"], task_id)
        history = await asyncio.to_thread(transcript_digest, self.transcripts.events_path(worker["id"]), None)
        mode = worker.get("effective_rotation_mode") or "conservative"
        text = state_packet.render(state=state, history=history, reason="handoff", mode=mode, handoff=check)
        path = await asyncio.to_thread(state_packet.write, workspace, task_id, text)
        return {"reason": "handoff", "mode": mode, "handoff": check.record(), "state_path": path}

    @staticmethod
    def _rotation_view(rotation: dict[str, Any]) -> dict[str, Any]:
        """The new conversation's start: always the handoff file, with what is wrong with it."""
        handoff = rotation.get("handoff") or {}
        reason = state_packet.REASONS.get(rotation["reason"], rotation["reason"])
        view = {"reason": rotation["reason"], "mode": rotation["mode"],
                "handoff": handoff.get("path") or None, "state": rotation["state_path"],
                "instruction": ROTATION_START.format(reason=reason)}
        if not handoff.get("valid"):
            view["handoff_problems"] = handoff.get("problems") or ["The handoff file does not exist"]
            view["instruction"] += " Your handoff file needs attention (handoff_problems): bring it up to date first."
        return view

    async def _request_rotation(self, worker_id: str, thread_id: str, attempt: dict[str, Any],
                                context: int) -> str | None:
        """Ask the agent to prepare its handoff once its context passes the aggressive limit."""
        task_id = attempt.get("task_id")
        if attempt.get("execution_mode") == "message_only" or not task_id:
            return None
        sha = await asyncio.to_thread(handoff_sha256, self.hub.workspace(worker_id), task_id)
        requested = await self.hub.request_rotation(worker_id, thread_id, context, sha)
        if requested is None:
            return None
        limit = (await self.hub.rotation_settings())["context_tokens"]
        return ROTATION_REMINDER.format(limit=limit, path=handoff_path(task_id), journal=JOURNAL_PATH)

    async def _candidate_files(self, candidate_id: str | None) -> str | None:
        """Where a candidate's frozen files are, for the report a later question starts from."""
        if not candidate_id:
            return None
        try:
            record = (await self.hub.artifact(candidate_id)).get("content_file") or {}
        except HubError:
            return None
        return str(Path(record["root"]) / "files") if record.get("root") else None

    async def _write_review_report(self, attempt: dict[str, Any], args: dict[str, Any], result: dict[str, Any]) -> None:
        """Keep the verdict the reviewer just gave as a report file, recorded with the review."""
        candidate_id = result.get("candidate_id") or args.get("candidate_id")
        text = review_report.render(
            review_task_id=attempt["task_id"], candidate_id=candidate_id,
            verdict="approve" if args.get("verdict") == "accept" else "request_changes", fields=args,
            files=await self._candidate_files(candidate_id))
        try:
            path, sha = await asyncio.to_thread(review_report.write, self.hub.workspace(attempt["worker_id"]),
                                                attempt["task_id"], text)
            await self.hub.record_review_report(attempt["worker_id"], attempt["task_id"], path, sha)
        except Exception as error:
            # The verdict is recorded either way; a later question writes the report from it.
            logger.warning("Review report file deferred for %s: %s", attempt["task_id"], type(error).__name__)

    async def _review_report_file(self, worker: dict[str, Any]) -> str | None:
        """The reviewer's report file, written from its recorded verdict when it does not exist."""
        recorded = await self.hub.review_report(worker["id"])
        workspace = self.hub.workspace(worker["id"])
        if recorded is not None and (workspace / recorded["path"]).is_file():
            return recorded["path"]
        record = await self.hub.review_record(worker["id"])
        if record is None:
            return None
        text = review_report.render(review_task_id=record["review_task_id"], candidate_id=record["candidate_id"],
                                    verdict=record["verdict"], fields=record["fields"],
                                    files=await self._candidate_files(record["candidate_id"]))
        path, sha = await asyncio.to_thread(review_report.write, workspace, record["review_task_id"], text)
        await self.hub.record_review_report(worker["id"], record["review_task_id"], path, sha)
        return path

    async def _settle_rotation_request(self, worker_id: str, thread_id: str) -> None:
        """After the reminded turn ends, judge the handoff file the agent prepared for the
        rotation of this conversation (`thread_id`)."""
        requested = getattr(self.hub, "requested_rotation", None)
        if not callable(requested):
            return
        rotation = await requested(worker_id, thread_id)
        if rotation is None:
            return
        if rotation["task_id"] is None:
            check = {"valid": False, "problems": ["There is no current task to hand off"]}
        else:
            review = (await self.hub.worker(worker_id)).get("kind") == "review"
            check = (await asyncio.to_thread(
                check_handoff, self.hub.workspace(worker_id), rotation["task_id"],
                previous_sha256=rotation["reminder_sha256"], review=review)).record()
        await self.hub.decide_rotation(rotation["id"], check)

    async def _select(self, run_id: str) -> dict[str, Any] | None:
        # Membership, review eligibility, and resumptions are durable Hub decisions. The Library
        # checkout is left to the activation (run_once), so admission does not wait for it.
        return await self.hub.next_assignment(run_id, prepare=False)

    async def _watch_run(
        self, run_id: str, *, attempt: dict[str, Any],
    ) -> tuple[str, str]:
        async with self.controls.watch(run_id, attempt["worker_id"]) as watch:
            while True:
                try:
                    view = await watch.next()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # Unavailable observation is not a stop instruction. Durable
                    # authority is checked when it becomes readable again.
                    await asyncio.sleep(1)
                    continue
                run, worker = view["run"], view["worker"]
                # Pause after turn lets this turn finish; Pause, Stop and shutdown still end it.
                if run["state"] != "running" and run.get("pause_mode") != "after_turn":
                    return "Run changed to " + run["state"], run.get("stop_reason") or "run_state_change"
                if worker["active_attempt"] != attempt["id"]:
                    return "The active execution binding changed", "assignment_changed"
                if worker.get("runtime_stop_reason"):
                    return "The task assignment was superseded", worker["runtime_stop_reason"]
                if worker["state"] == "draining":
                    task = await self.hub.task(attempt["task_id"])
                    if (task["revision"] != attempt["task_revision"]
                            or task["state"] in {"done", "retired"}):
                        # Peer completion and publisher revision may retire this
                        # execution without a tool call. Ordinary tool drainage is
                        # handled by the harness after its reply is delivered.
                        return "The task assignment was superseded", "assignment_changed"

    async def _ack(self, packet: dict[str, Any], attempt: dict[str, Any]) -> None:
        if packet.get("packet_id"):
            await self._command("context.ack", {
                "worker_id": attempt["worker_id"], "packet_id": packet["packet_id"],
            }, actor=self._worker_actor(attempt),
                key="context:" + hashlib.sha256(
                    (attempt["id"] + packet["packet_id"]).encode()).hexdigest())

    async def _bound_tool_result(self, result: dict[str, Any], packet_bytes: int,
                                 *, worker_id: str | None = None) -> dict[str, Any]:
        # Context packets already obey their own limit. Their acknowledgment must
        # correspond to the actual packet, never to a replacement blob pointer.
        if isinstance(result, ToolResult):
            return result
        material = ({key: value for key, value in result.items() if key != "runtime_directive"}
                    if worker_id is not None else result)
        body = canonical(material).encode("utf-8")
        if len(body) <= packet_bytes:
            return result
        envelope: dict[str, Any]
        if worker_id is not None:
            # File reads and development diagnostics may be private. Do not
            # copy worker-visible overflow into the shared material namespace.
            files = MaterialFiles(self.hub.workspace(worker_id))
            envelope = await asyncio.to_thread(files.write, body)
            # Its large fields also get their own files with an outline, so that a reader can
            # open the part it needs instead of the whole record.
            parts = await asyncio.to_thread(_material_parts, files, material)
            if parts and len(canonical({**envelope, "parts": parts}).encode("utf-8")) <= packet_bytes // 2:
                envelope["parts"] = parts
        else:
            # Internal retained outcomes keep their existing operational store.
            sha = await asyncio.to_thread(self.hub.blobs.put, body)
            envelope = {"material_sha256": sha, "material_bytes": len(body), "requires_read": True}
        if "error" in result:
            # Preserve unsuccessful-tool semantics without copying an unbounded error.
            envelope["error"] = True
        for key in (
            "runtime_directive", "continuation_id", "attempt_id",
            "id", "task_id", "revision", "kind", "status",
            "artifact_id", "candidate_id", "candidate_hash", "review_task_id",
            "review_receipt_id", "proposal_id", "decision_id", "publication_policy", "snapshot",
            "integration_id", "workspace_relative_path", "base_commit", "target_head", "commit",
        ):
            value = result.get(key)
            if key not in result or not isinstance(value, (str, int, float, bool, type(None))):
                continue
            candidate = {**envelope, key: value}
            if len(canonical(candidate).encode("utf-8")) <= packet_bytes:
                envelope = candidate
        if len(canonical(envelope).encode("utf-8")) > packet_bytes:
            raise ValueError("Tool material reference exceeds the packet byte limit")
        return envelope

    @staticmethod
    def _fit_text_page(page: dict[str, Any], packet_bytes: int) -> dict[str, Any]:
        if len(canonical(page).encode("utf-8")) <= packet_bytes:
            return page
        text = page["text"]
        offset = page["offset"]

        def prefix(length: int) -> dict[str, Any]:
            return {
                **page, "text": text[:length], "next_offset": offset + length,
                "has_more": page["has_more"] or length < len(text),
            }

        low, high = 0, len(text)
        while low < high:
            middle = (low + high + 1) // 2
            if len(canonical(prefix(middle)).encode("utf-8")) <= packet_bytes:
                low = middle
            else:
                high = middle - 1
        fitted = prefix(low)
        if (len(canonical(fitted).encode("utf-8")) > packet_bytes
                or (text and not low)):
            raise ValueError("File pagination metadata exceeds the packet byte limit")
        return fitted

    async def _tool(
        self, attempt: dict[str, Any], thread_id: str, name: str, args: dict[str, Any],
        call_id: str,
    ) -> dict[str, Any]:
        key = (attempt["worker_id"], thread_id, call_id)
        lock = self._native_call_locks.setdefault(key, asyncio.Lock())
        async with lock:
            try:
                return await self._tool_result(attempt, thread_id, name, args, call_id)
            except FileNotFoundError as exc:
                raise HubError("NOT_FOUND", "The requested file is missing; refresh its parent listing.", 404) from exc

    async def _tool_result(self, attempt, thread_id, name, args, call_id):
        # Only the packet size: each tool call working out the run's controls cost a read each.
        packet_bytes = (await self.hub.run_limits(attempt["run_id"]))["packet_bytes"]
        native_call = await self.hub.begin_native_call(
            self._worker_actor(attempt), thread_id, call_id, {"name": name, "arguments": args},
            create=name == "taskhub_call" and isinstance(args, dict)
            and isinstance(args.get("operation"), str) and args["operation"] in NATIVE_COMMAND_OPERATIONS)
        if native_call and native_call["response_json"] is not None:
            saved = json.loads(native_call["response_json"])
            if args.get("operation") == "draft_update" and saved.get("status") == "unreviewed":
                await self.hub.reconcile_drafts(saved["task_id"])
            if args.get("operation") == "dag_update" and saved.get("kind") == "shared-dags":
                await self.hub.reconcile_dags()
            if (not native_call.get("validation_artifact_id")
                    or native_call.get("validation_state") == "completed"):
                return await self._model_result(saved, attempt)
        if native_call and native_call["command_json"] is not None:
            if native_call["core_response_json"] is not None:
                result = json.loads(native_call["core_response_json"])
                if args.get("operation") == "draft_update" and result.get("status") == "unreviewed":
                    await self.hub.reconcile_drafts(result["task_id"])
                if args.get("operation") == "dag_update" and result.get("kind") == "shared-dags":
                    await self.hub.reconcile_dags()
            else:
                command = Command.model_validate_json(native_call["command_json"])
                result = await self.hub.execute(command, self._worker_actor(attempt), native_call=native_call)
            # Recover the exact action without rereading its mutable request.
            # Original validation has its own durable phase below.
            if args["operation"] == "leave_task" and args["arguments"]["task_id"] == attempt["task_id"]:
                result = {**result, "runtime_directive": "drain_for_release"}
            elif args["operation"] == "wiki_submit":
                result = {**result, "note_id": result["artifact_id"], "category": "note"}
        else:
            result = await self._dispatch_tool(
                attempt, thread_id, name, args, call_id, packet_bytes=packet_bytes, native_call=native_call)
        if (native_call is not None and args.get("operation") in {"stage", "wiki_submit"}
                and result.get("artifact_id")):
            result = {**result, "validation": await self._recover_submission({
                "task_id": attempt["task_id"], "task_revision": attempt["task_revision"],
                "validation_artifact_id": result["artifact_id"]})}
        result = await self._model_result(result, attempt)
        result = await self._bound_tool_result(result, packet_bytes, worker_id=attempt["worker_id"])
        if native_call is not None:
            return await self.hub.finish_native_call(native_call, self._worker_actor(attempt), result)
        return result

    async def _present(self, value):
        """Model-facing copy; operational actors, receipts and paths stay exact."""
        present = getattr(self.hub, "present_worker_data", None)
        projected = await present(value) if callable(present) else deepcopy(dict(value))
        if isinstance(value, ToolResult):
            return ToolResult(projected, after_send=value.after_send)
        return projected

    async def _model_result(self, value, attempt):
        """Project only the response; callbacks retain their original backend envelopes."""
        named = await self._present(value)
        projected = await asyncio.to_thread(tool_view, named,
                                            workspace=self.hub.workspace(attempt["worker_id"]))
        if isinstance(value, ToolResult):
            return ToolResult(projected, after_send=value.after_send)
        return projected

    @staticmethod
    def _action_arguments(arguments):
        body = canonical(arguments).encode("utf-8")
        operation = arguments.get("operation")
        label = ({"operation": operation} if isinstance(operation, str) and len(operation) <= 80 else {})
        if len(body) <= 8192:
            return {**label, "arguments": arguments}
        return {**label, "argument_keys": sorted(arguments), "arguments_bytes": len(body),
                "arguments_sha256": hashlib.sha256(body).hexdigest(), "arguments_omitted": True}

    @staticmethod
    def _action_result(result):
        # The conversation already retains replies and submitted prompt packets.
        # Keep boundary outcomes useful without copying large source/tool streams.
        return {key: result[key] for key in (
            "error", "code", "status", "runtime_directive", "id", "task_id", "worker_id",
            "artifact_id", "candidate_id", "review_task_id", "integration_id", "message_id",
            "request_id", "declaration_id", "directive", "commit")
            if key in result and isinstance(result[key], (str, bool, int, type(None)))
            and len(str(result[key])) <= 2000}

    async def _dispatch_tool(
        self, attempt: dict[str, Any], thread_id: str, name: str, args: dict[str, Any],
        call_id: str, *, packet_bytes: int,
        native_call: dict | None = None,
    ) -> dict[str, Any]:
        # The native catalog never depends on roles or operation definitions.
        # Every invocation resolves authority from the current activation; a
        # previously discovered schema is not a capability grant.
        try:
            args = validate_gateway(name, args)
            worker = await self.hub.worker(attempt["worker_id"])
            capability = self._capability(worker, attempt, await self.hub.task_kind(attempt["task_id"]))
            if name == "taskhub_actions":
                await self.hub.authorize_worker(self._worker_actor(attempt))
                return self.actions.describe(capability=capability, **args)
            operation, arguments = self.actions.resolve(
                args["operation"], args["arguments"], capability=capability)
        except ActionError as exc:
            return exc.as_result()
        return await self._dispatch_operation(
            attempt, thread_id, operation, arguments, call_id, packet_bytes=packet_bytes, native_call=native_call)

    async def _dispatch_operation(
        self, attempt: dict[str, Any], thread_id: str, name: str, args: dict[str, Any],
        call_id: str, *, packet_bytes: int,
        native_call: dict | None = None,
    ) -> dict[str, Any]:
        actor = self._worker_actor(attempt)
        if name in {"taskhub_withdraw", "taskhub_leave_task"}:
            args = await self._ordinary_task_authority(attempt, name, args)
        key = ("native-tool:" + hashlib.sha256(canonical({
            "worker_id": attempt["worker_id"], "thread_id": thread_id, "call_id": call_id,
        }).encode("utf-8")).hexdigest() if native_call is not None else
            "tool:" + hashlib.sha256((thread_id + ":" + call_id).encode()).hexdigest())
        if (name in {"taskhub_talk_send", "taskhub_message_send", "taskhub_request_resolve"}
                and not args.get("message_id")):
            # A call without an ID of its own is its own message; this call's replay is the same one.
            args = {**args, "message_id": key}
        if name.startswith("taskhub_batch_"):
            from .integrator import dispatch_integrator
            return await dispatch_integrator(self, attempt, name, args, key, native_call)
        if name in {"taskhub_publish", "taskhub_propose_revision"}:
            args = await self._contract_arguments(attempt, args)
        if name in {"taskhub_stage", "taskhub_contribute"}:
            args = await self._submission_arguments(attempt, args)
        command = hub_command(name, args)
        if command is not None:
            # Hub command handlers validate authority before resolving aliases.
            # Keep their exact receipt fast path: a delivered draining command
            # may be replayed without executing or validating its work again.
            command_name, args = command
            result = await self._command(command_name, args, actor=actor, key=key, native_call=native_call)
            if name == "taskhub_review" and "error" not in result:
                await self._write_review_report(attempt, args, result)
            if name == "taskhub_leave_task" and args["task_id"] == attempt["task_id"]:
                result = {**result, "runtime_directive": "drain_for_release"}
            if name == "taskhub_stage" and result.get("artifact_id") and native_call is None:
                result = {**result, "validation": await self._validate_submission(
                    attempt, result["artifact_id"])}
            return result
        # Read tools and application tools obey the same execution ownership and fencing.
        await self.hub.authorize_worker(actor)
        resolve = getattr(self.hub, "resolve_worker_refs", None)
        if callable(resolve):
            args = await resolve(args)
        operation = OPERATIONS.get(name)
        if operation is not None:
            return await operation(self, _Call(attempt, thread_id, actor, key, packet_bytes, native_call), name, args)
        if name in self.domain_names:
            handler = getattr(self.plugin, "handle_worker_tool")
            result = await handler(self.hub, name, args, actor, key=key)
            if not isinstance(result, dict):
                raise TypeError("Domain worker tool responses must be JSON objects")
            if result.get("artifact_id") and result.get("review_task_id"):
                result = {**result, "validation": await self._validate_submission(
                    attempt, result["artifact_id"])}
            return result
        raise ValueError(f"Unknown framework tool: {name}")

    async def _ordinary_task_authority(self, attempt: dict[str, Any], name: str,
                                       args: dict[str, Any]) -> dict[str, Any]:
        """Withdrawal and departure need ordinary task authority and a reason; departure, the
        caller's own executor membership."""
        worker = await self.hub.worker(attempt["worker_id"])
        current_task = await self.hub.task(attempt["task_id"])
        if (worker.get("kind") != "ordinary" or attempt.get("execution_mode") == "message_only"
                or current_task["contract"]["kind"] in {"review"}):
            raise HubError("ORDINARY_TASK_AUTHORITY_REQUIRED", "Withdrawal and permanent departure require ordinary task execution authority", 403)
        reason = args.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 4000:
            raise HubError("REASON_REQUIRED", "Supply a concrete reason of 1 to 4000 characters", 422)
        if name != "taskhub_leave_task":
            return args
        task = (current_task if args["task_id"] == current_task["id"]
                else await self.hub.task(args["task_id"]))
        if not any(member["worker_id"] == attempt["worker_id"] and member["executor"]
                   and member.get("membership_state", "member") == "member"
                   for member in task["collaboration"]["members"]):
            raise HubError("EXECUTOR_MEMBERSHIP_REQUIRED", "Only your current executor membership can be permanently left", 403)
        return {**args, "permanent": True}

    @OPERATIONS.handles("taskhub_wiki_submit")
    async def _op_wiki_submit(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        paths = args.get("paths")
        if (not isinstance(paths, list) or not 1 <= len(paths) <= 200
                or not all(isinstance(path, str) and path.startswith(("wiki/", "notes/")) and path.endswith(".md") for path in paths)
                or not isinstance(args.get("summary"), str) or not args["summary"].strip()):
            raise HubError("INVALID_INPUT", "Public notes require wiki/ or notes/ Markdown paths and a concrete summary")
        if "replaces" in args and (not isinstance(args["replaces"], str)
                                   or not args["replaces"].strip()):
            raise HubError("INVALID_REPLACEMENT", "Supply the exact rejected, withdrawn, or review-invalidated candidate ID")
        workspace = ScopedFiles(self.hub.workspace(call.attempt["worker_id"]))
        for path in paths:
            content = workspace.path(path).read_text(encoding="utf-8")
            if not content.strip():
                raise HubError("INVALID_INPUT", "Public knowledge pages must not be empty")
        policy = "lean.peer@1" if self.plugin.name == "lean" else "general.peer@1"
        staged = await self._command("artifact.stage", {
            "kind": "note.wiki", "data": {"summary": args["summary"], "paths": paths},
            "repository_paths": paths, "input_refs": args.get("input_refs", []), "policy": policy,
            **{field: args[field] for field in ("coauthors", "replaces") if field in args},
        }, actor=call.actor, key=call.key, native_call=call.native_call)
        result = {**staged, "note_id": staged["artifact_id"], "category": "note"}
        if call.native_call is None:
            result["validation"] = await self._validate_submission(call.attempt, staged["artifact_id"])
        return result

    @OPERATIONS.handles("taskhub_talk_read")
    async def _op_talk_read(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        maximum = max(512, min(call.packet_bytes, 8192))
        if "conversation_id" not in args:
            value = await self.hub.peer_conversations(call.actor, after_id=args.get("after_id", ""), max_bytes=maximum)
        else:
            value = await self.hub.peer_history(args["conversation_id"], call.actor,
                after_sequence=args.get("after_sequence", 0), offset=args.get("offset", 0), max_bytes=maximum)

        async def delivered():
            pass  # History reads never acknowledge automatic message delivery.
        # Private text must never enter shared material blobs on overflow.
        return ToolResult(value, after_send=delivered)

    @OPERATIONS.handles("taskhub_dag_status")
    async def _op_dag_status(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        return await self.hub.dag_status()

    @OPERATIONS.handles("taskhub_public_read")
    async def _op_public_read(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        repository = self.hub._repository()
        recorded = await self.hub.db.call(lambda c: self.hub._repository_head_recorded(c, args["commit"]))
        if not recorded:
            raise HubError("UNPUBLISHED_COMMIT", "Public reads require a recorded Library commit")
        view = await asyncio.to_thread(self.hub.content.public_view, repository, self.hub.blobs, args["commit"])
        page = await asyncio.to_thread(ScopedFiles(view.root).read, args["path"], args.get("offset", 0), args.get("limit", 4096))
        return self._fit_text_page(page, call.packet_bytes)

    @OPERATIONS.handles("taskhub_workspace_list")
    async def _op_workspace_list(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        reader, source_path = await native_file(self.hub, call.actor, self.resources, args.get("path", ""))
        return await asyncio.to_thread(reader.list, source_path, args.get("cursor", 0), args.get("limit", 50))

    @OPERATIONS.handles("taskhub_workspace_read")
    async def _op_workspace_read(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        reader, path = await native_file(self.hub, call.actor, self.resources, args["path"])
        page = await asyncio.to_thread(reader.read, path, args.get("offset", 0), args.get("limit", 4096))
        page["path"] = args["path"]
        return self._fit_text_page(page, call.packet_bytes)

    @OPERATIONS.handles("taskhub_workspace_write", "taskhub_workspace_copy")
    async def _op_workspace_write(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        if (call.attempt.get("execution_mode") == "message_only"
                and not args["path"].startswith(".private/dags/")):
            raise HubError("MESSAGE_ONLY_AUTHORITY", "Correspondence turns may write only proposed DAG files under .private/dags/", 403)
        workspace = ScopedFiles(self.hub.workspace(call.attempt["worker_id"]))
        if name == "taskhub_workspace_copy":
            source, source_path = await native_resource(
                self.hub, call.actor, self.resources, args["root"], args["source_path"])
            return await asyncio.to_thread(
                workspace.copy_from, source, source_path, args["path"],
                expected_source_sha256=args.get("expected_source_sha256"), expected_sha256=args.get("expected_sha256"))
        return await asyncio.to_thread(
            workspace.write, args["path"], args["text"], args.get("expected_sha256"),
            offset=args.get("offset"), delete_count=args.get("delete_count", 0))

    @OPERATIONS.handles("taskhub_resource_list", "taskhub_resource_read", "taskhub_resource_search",
                        "taskhub_resource_pdf")
    async def _op_resource(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        resource, path = await native_resource(
            self.hub, call.actor, self.resources, args["root"], args.get("path", ""))
        if name == "taskhub_resource_list":
            result = await asyncio.to_thread(
                resource.list, path, args.get("cursor", 0), args.get("limit", 50))
        elif name == "taskhub_resource_read":
            result = await asyncio.to_thread(
                resource.read, path, args.get("offset", 0), args.get("limit", 4096))
        elif name == "taskhub_resource_search":
            result = await asyncio.to_thread(
                resource.search, args["query"], path,
                args.get("mode", "path"), args.get("cursor", 0), args.get("limit", 20))
        else:
            workspace = self.hub.workspace(call.attempt["worker_id"])
            pages = workspace / ".private" / "tmp" / "pdf-pages" if args.get("render") is True else None
            result = await asyncio.to_thread(
                resource.pdf, path, args.get("page", 1), args.get("count", 1), render=pages)
            for image in result.get("images", []):
                image["absolute_path"] = image["path"]
                image["path"] = Path(image["path"]).relative_to(workspace).as_posix()
        if "path" in result:
            result["path"] = args.get("path", "")
        page = {"root": args["root"], **result}
        return self._fit_text_page(page, call.packet_bytes) if name == "taskhub_resource_read" else page

    @OPERATIONS.handles("taskhub_sync")
    async def _op_sync(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        attempt = call.attempt
        packet = await self.hub.context(attempt["worker_id"])
        packet, inbox = await self._native_packet(
            packet, attempt, include_messages=attempt["id"] not in self._steering_attempts)
        # Only what is new: the conversation already has its task. What the sync showed of
        # the task becomes the continuation baseline, so the next input stays a delta.
        view = sync_view(packet)

        async def sent() -> None:
            await self._ack_native(packet, attempt, inbox)
            await asyncio.to_thread(self.continuation_prompts.observed, attempt["worker_id"], view["task"])

        return ToolResult(view, after_send=sent)

    @OPERATIONS.handles("taskhub_history_search")
    async def _op_history_search(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        return await asyncio.to_thread(
            search_history, self.transcripts.events_path(call.attempt["worker_id"]), args["query"],
            limit=args.get("limit", 10), exclude_thread=call.thread_id)

    @OPERATIONS.handles("taskhub_search")
    async def _op_search(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        return {"matches": await self.hub.search(
            args.get("query", ""), limit=args.get("limit", 10), actor=call.actor)}

    async def _not_found(self, error: HubError, kind: str, value: str) -> HubError:
        """A missing task or artifact ID's error, naming the ID meant when that is clear. Workers
        wrote 8-character prefixes, or invented the rest of an ID, in about a hundred calls a day."""
        matches = await self.hub.ids_with_prefix(value)
        own, other = (matches["tasks"], matches["artifacts"]) if kind == "task" else (matches["artifacts"], matches["tasks"])
        if value in own:
            return error  # The ID exists: something else, such as the revision asked for, does not.
        if value in other:
            meant, operation = ("an artifact", "fetch") if kind == "task" else ("a task", "inspect")
            return HubError("NOT_FOUND", f"{value} is {meant} ID, not {'a task' if kind == 'task' else 'an artifact'} "
                            f"ID: read it with {operation}", 404)
        message = f"No {kind} has ID {value[:80]!r}; IDs are full 36-character UUIDs"
        if len(own) == 1:
            message += f"; the {kind} whose ID starts with {value[:8]} is {own[0]}"
        elif own:
            message += f"; several {kind}s have IDs starting with {value[:8]}: copy the full ID"
        return HubError("NOT_FOUND", message, 404)

    @OPERATIONS.handles("taskhub_inspect")
    async def _op_inspect(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        try:
            if not args.get("include_completion"):
                return await self.hub.task(args["task_id"], revision=args.get("revision"))
            result = await self.hub.inspect_task_completion(args["task_id"], call.actor)
        except HubError as error:
            if error.code != "NOT_FOUND":
                raise
            raise await self._not_found(error, "task", args["task_id"]) from error
        if args.get("revision", result["revision"]) != result["revision"]:
            raise HubError("INVALID_INPUT", f"Completion is current-state evidence: the task is at revision "
                           f"{result['revision']}; omit revision, or pass the current one, to inspect it", 422)
        return result

    @OPERATIONS.handles("taskhub_fetch")
    async def _op_fetch(self, call: _Call, name: str, args: dict[str, Any]) -> Any:
        try:
            record = await self.hub.native_record(args["artifact_id"], call.actor)
        except HubError as error:
            if error.code != "NOT_FOUND":
                raise
            raise await self._not_found(error, "artifact", args["artifact_id"]) from error
        if record["status"] != "private":
            return record
        if len(canonical(record).encode("utf-8")) > call.packet_bytes:
            record = {key: record[key] for key in ("id", "kind", "category", "status", "content_file")}

        async def no_shared_copy():
            pass
        return ToolResult(record, after_send=no_shared_copy)

    async def _submission_arguments(self, attempt, arguments):
        """Read a native submission manifest only at the review boundary."""
        await self.hub.authorize_worker(self._worker_actor(attempt))
        if "manifest_path" not in arguments:
            raise HubError("INVALID_INPUT", "Provide a workspace-relative manifest_path", 422)
        if set(arguments) != {"manifest_path"}:
            raise HubError("INVALID_INPUT", "Do not mix a native submission file with inline fields")
        workspace = ScopedFiles(self.hub.workspace(attempt["worker_id"]))
        path = workspace.path(arguments["manifest_path"])
        if path.stat().st_size > 1048576:
            raise HubError("INVALID_INPUT", "Submission metadata exceeds one MiB; keep source in files")
        value = read_json(await asyncio.to_thread(path.read_bytes))
        allowed = {"kind", "data", "repository_paths", "input_refs", "policy", "coauthors", "introduction", "replaces"}
        if (set(value) - allowed or not isinstance(value.get("kind"), str) or not value["kind"].strip()
                or not isinstance(value.get("data"), dict)):
            raise HubError("INVALID_INPUT", "Native submission requires kind/data and supported publication fields")
        introduction = value.get("introduction")
        if introduction is not None and (
                not isinstance(introduction, str) or not introduction.strip() or len(introduction) > 4000):
            raise HubError("INVALID_INPUT", "A submission introduction must contain 1 to 4000 characters")
        normalize = getattr(self.plugin, "normalize_submission", None)
        if callable(normalize):
            task = await self.hub.task(attempt["task_id"], attempt["task_revision"])
            snapshot = await asyncio.to_thread(self.hub._candidate_snapshot, attempt["worker_id"])
            normalized = await normalize(self.hub, task, value, snapshot)
            value.update(normalized)
        return value

    async def _contract_arguments(self, attempt, arguments):
        await self.hub.authorize_worker(self._worker_actor(attempt))
        if "contract_path" not in arguments:
            raise HubError("INVALID_INPUT", "Provide a workspace-relative contract_path", 422)
        if "contract" in arguments:
            raise HubError("INVALID_INPUT", "Do not mix task file and inline contract")
        workspace = ScopedFiles(self.hub.workspace(attempt["worker_id"]))
        path = workspace.path(arguments["contract_path"])
        if path.stat().st_size > 1048576:
            raise HubError("INVALID_INPUT", "Task metadata exceeds one MiB")
        value = read_json(await asyncio.to_thread(path.read_bytes))
        if isinstance(value.get("contract"), dict):
            contract = dict(value["contract"])
            references = {field: value[field] for field in ("objective", "context") if field in value}
        else:
            # A contract's objective and context may also be adjacent Markdown files, named in place
            # of their text (submission guide). Taken as text, such a name made a task with no target.
            contract = dict(value)
            references = {field: contract[field] for field in file_name_fields(contract)}
        for field, reference in references.items():
            if not isinstance(reference, str):
                raise HubError("INVALID_INPUT", "Task body references must be relative file paths")
            relative = (path.parent / reference.strip()).relative_to(workspace.root).as_posix()
            referenced = workspace.path(relative)
            if not referenced.is_file():
                raise HubError("INVALID_INPUT", f"The task's {field} names {reference.strip()}, which is not a file "
                               "beside the task file: write the Markdown text itself, or create that file", 422)
            if referenced.stat().st_size > 262144:
                raise HubError("INVALID_INPUT", "Task body exceeds its file size limit")
            contract[field] = await asyncio.to_thread(referenced.read_text, encoding="utf-8")
        # A task file without a domain is in the project's: the contract model's own default is
        # general, which failed 81 Lean publications in a day. A wrong explicit domain still fails.
        contract.setdefault("domain", self.hub.config.domain)
        return {**{key: value for key, value in arguments.items() if key != "contract_path"},
                "contract": contract}

    async def _native_packet(self, packet, attempt, *, include_messages=True, compacted=False, full_context=False):
        compacted = compacted or await asyncio.to_thread(
            self.continuation_prompts.compaction_pending, attempt["worker_id"])
        native = await self.hub.prepare_native_context(attempt["worker_id"])
        operation_paths = await asyncio.to_thread(
            prepare_operation_reference, self.hub.workspace(attempt["worker_id"]))
        native.setdefault("directories", {}).update(operation_paths)
        native["directories"]["dag_checker"] = await asyncio.to_thread(
            prepare_dag_checker, self.hub.workspace(attempt["worker_id"]))
        reference = getattr(self.plugin, "prepare_worker_reference", None)
        if callable(reference):
            paths = await asyncio.to_thread(reference, self.hub.workspace(attempt["worker_id"]))
            native.setdefault("directories", {}).update(paths)
        actor = self._worker_actor(attempt)
        worker = await self.hub.worker(attempt["worker_id"])
        guidance = await asyncio.to_thread(
            prepare_guidance, self.hub.workspace(attempt["worker_id"]), self.plugin,
            review=worker.get("kind") == "review", worker_kind=worker.get("kind", "ordinary"), context_epoch=worker["context_epoch"],
            compacted=compacted, execution_mode=attempt.get("execution_mode", "work"))
        native.setdefault("directories", {})["instructions"] = guidance["directory"]
        inbox = await self.hub.peer_unread(actor, max_bytes=4096, reference_only=True) if include_messages else {}
        human = await self.hub.human_unread(actor, max_bytes=4096, reference_only=True) if include_messages else {}
        packet = {**packet, "native": native, "instruction_guidance": guidance}
        packet["worker_identity"] = {"worker_id": attempt["worker_id"],
                                     "display_name": worker.get("display_name"),
                                     "worker_number": worker.get("worker_number"),
                                     "reviewer_number": worker.get("reviewer_number"),
                                     "kind": worker.get("kind")}
        packet["execution_mode"] = attempt.get("execution_mode", "work")
        if packet["execution_mode"] == "message_only":
            packet["audit_anchor"] = {"task_id": attempt["task_id"],
                                      "task_revision": attempt["task_revision"],
                                      "files": native.get("task_files")}
            packet["task"] = {"id": None, "kind": "correspondence", "title": "Directed correspondence",
                "objective": "Handle the directed messages in this conversation for one turn. You have no task execution assignment."}
            packet["native"] = {key: value for key, value in native.items()
                                 if key not in {"task_files", "review"}}
        if worker.get("kind") == "review" and packet["execution_mode"] != "message_only":
            packet["review"] = await prepare_review_entry(self.hub, attempt)
        if packet["execution_mode"] in MESSAGE_MODES:
            packet["message_state"] = await message_state(self.hub, attempt)
            packet["continuation_instruction"] = (
                "Handle the directed messages. Finish to retain message_state.after_turn_intention "
                "unless you explicitly invoke the resume_work operation. Its effect is stated in message_state. "
                "Before task work, read the task file and saved checkpoint/requirements; use the sync operation "
                "if context is missing. A message is not verified dependency evidence.")
        elif worker.get("kind") == "integrator":
            packet["continuation_instruction"] = INTEGRATION_CONTINUE
        elif worker.get("kind") == "review":
            packet["continuation_instruction"] = (
                "Continue the review from review.phase; follow its exact candidate and acceptance references.")
        else:
            packet["continuation_instruction"] = (
                "Continue the current task in this original conversation. A normal turn end starts "
                "the next turn at once; when you cannot progress until an event, pause with a predicate for it.")
        if packet["execution_mode"] == "message_only" and worker.get("kind") in {"review", "integrator"}:
            packet["message_state"] = {"task_execution_authority": False, "after_turn_intention": "waiting"}
            packet["continuation_instruction"] = "Answer the directed artifact-specific messages in this retained conversation, then finish. This is a clarification turn, not a new task or integration assignment."
            reader = getattr(self.hub, "review_report", None)
            if worker.get("kind") == "review" and callable(reader):
                # A reviewer answers from its recorded report in a new conversation.
                report = await reader(attempt["worker_id"])
                if report is not None:
                    packet["review_report"] = report["path"]
                packet["continuation_instruction"] = REVIEW_CLARIFICATION.format(
                    report=": first read review_report, your verdict and its evidence" if report else "")
        warning_reader = getattr(self.hub, "prior_execution_warning", None)
        if callable(warning_reader):
            warning = await warning_reader(attempt["worker_id"], attempt["id"])
            if warning:
                packet["continuation_instruction"] += " " + warning
                packet["runtime_recovery"] = True
        if inbox.get("messages"):
            packet["private_talk"] = inbox
        if human.get("messages"):
            packet["human_messages"] = human
        packet = scenario_entry(packet, full_context=full_context or compacted)
        frame_size = len(canonical({"input": [{"type": "text", "text": canonical(packet)}]}).encode("utf-8"))
        if frame_size > min(self.hub.config.codex.max_frame_bytes, self.hub.config.claude.max_frame_bytes) - 4096:
            raise HubError("INPUT_FRAME_TOO_LARGE", "The prepared task input exceeds the configured native frame size; "
                           "use file references for large material. No content was omitted.")
        return packet, {**inbox, "_human": human}

    async def _ack_native(self, packet, attempt, inbox):
        await self._ack(packet, attempt)
        actor = self._worker_actor(attempt)
        if inbox.get("delivery_id"):
            await self._command("talk.ack", {"delivery_id": inbox["delivery_id"]}, actor=actor,
                                key="talk-delivery:" + inbox["delivery_id"])
        human = inbox.get("_human", {})
        if human.get("delivery_id"):
            await self._command("message.ack", {"delivery_id": human["delivery_id"]}, actor=actor,
                                key="human-delivery:" + human["delivery_id"])

    async def _hold_inbox(self, attempt, inbox, *, thread_id, turn_id, input_id, pending=False):
        for kind, delivery in (("peer", inbox), ("human", inbox.get("_human", {}))):
            if delivery.get("delivery_id"):
                await self.hub.mark_delivery_uncertain(
                    Actor(role=Role.RUNNER), kind, delivery["delivery_id"],
                    thread_id, turn_id, input_id, pending=pending)

    async def _resolve_inbox(self, attempt, inbox, *, accepted, thread_id, turn_id, input_id):
        for kind, delivery in (("peer", inbox), ("human", inbox.get("_human", {}))):
            if delivery.get("delivery_id"):
                await self.hub.confirm_delivery_transport(Actor(role=Role.RUNNER), kind,
                    delivery["delivery_id"], accepted, thread_id=thread_id, turn_id=turn_id,
                    input_id=input_id)

    async def _settle_steered_inputs(self, harness, attempt, session):
        """Confirm a steer whose RPC timed out once the native history records its input.

        Codex adds a steered input when the running tool call returns, often after the RPC
        timeout. A missing record proves nothing, so that delivery stays held for the operator.
        """
        recorded = getattr(harness, "recorded_input", None)
        if recorded is None:
            return
        for hold in await self.hub.uncertain_deliveries(attempt["worker_id"], session.thread_id):
            if not hold["input_id"]:
                continue
            found = await asyncio.to_thread(recorded, hold["input_id"])
            if found is None:
                continue
            await self.hub.confirm_delivery_transport(
                Actor(role=Role.RUNNER), hold["kind"], hold["delivery_id"], True,
                thread_id=session.thread_id, turn_id=hold["turn_id"], input_id=hold["input_id"],
                reason=f"The native history records this input at {found[0]}; its steer RPC had timed out.")

    async def _steer_messages(self, harness, attempt, session, started_event):
        await started_event.wait()
        worker_id = attempt["worker_id"]
        lock = self._delivery_locks.setdefault(worker_id, asyncio.Lock())
        actor = self._worker_actor(attempt)
        settled_at: float | None = None
        async with self.controls.watch(attempt["run_id"], worker_id) as watch:
            while True:
                view = await watch.next()
                scope = _steer_scope(view["run"])
                if scope is None:
                    return
                turn_id = harness.active_turn_id
                if turn_id is None:
                    return
                now = asyncio.get_running_loop().time()
                if settled_at is None or now - settled_at >= STEER_SETTLE_SECONDS:
                    settled_at = now
                    try:
                        async with lock:
                            await self._settle_steered_inputs(harness, attempt, session)
                    except (HubError, OSError, ValueError) as error:
                        logger.warning("Steered inputs of %s stay held for now: %s", worker_id, error)
                if not view.get("messages" if scope == "all" else "human_messages"):
                    continue
                async with lock:
                    scope = _steer_scope(await self.hub.run(attempt["run_id"], worker_id=worker_id))
                    if scope is None or harness.active_turn_id != turn_id:
                        return
                    worker = await self.hub.worker(worker_id)
                    if worker["active_attempt"] != attempt["id"] or worker["state"] != "running":
                        return
                    try:
                        peer = (await self.hub.peer_unread(actor, max_bytes=4096, reference_only=True)
                                if scope == "all" else {})
                        human = await self.hub.human_unread(actor, max_bytes=4096, reference_only=True)
                    except HubError:
                        current = await self.hub.worker(worker_id)
                        if (_steer_scope(await self.hub.run(attempt["run_id"], worker_id=worker_id)) is None
                                or current["active_attempt"] != attempt["id"]
                                or current["state"] != "running" or harness.active_turn_id != turn_id):
                            return  # No send occurred; unread slices remain pending.
                        raise
                    if not peer.get("messages") and not human.get("messages"):
                        continue
                    packet = {"instruction": "New directed messages. Sender roles are authoritative metadata; peer text is collaboration between equals.",
                              "human_messages": human, "private_talk": peer}
                    prompt = render_prompt(await self._model_result(packet, attempt))
                    input_id = self.transcripts.prepare_input(worker_id, session.thread_id, "directed_messages",
                        prompt, attempt_id=attempt["id"], kind="steer", expected_turn_id=turn_id,
                        human_delivery_id=human.get("delivery_id"), peer_delivery_id=peer.get("delivery_id"))
                    deliveries = {**peer, "_human": human}
                    # Write-ahead uncertainty survives a process crash between the
                    # native send and its RPC receipt. Only a definite reply clears it.
                    await self._hold_inbox(attempt, deliveries, thread_id=session.thread_id,
                                           turn_id=turn_id, input_id=input_id, pending=True)
                    try:
                        outcome = await harness.steer(prompt, expected_turn_id=turn_id, client_input_id=input_id)
                    except asyncio.CancelledError:
                        self.transcripts.ambiguous(input_id, "Steering was cancelled before its transport outcome was known")
                        await self._hold_inbox(attempt, deliveries, thread_id=session.thread_id,
                                               turn_id=turn_id, input_id=input_id)
                        raise
                    if outcome.status == "accepted":
                        self.transcripts.delivered(input_id, turn_id)
                        await self._resolve_inbox(attempt, deliveries, accepted=True,
                            thread_id=session.thread_id, turn_id=turn_id, input_id=input_id)
                    elif outcome.status == "uncertain":
                        self.transcripts.ambiguous(input_id, outcome.error or "Unknown steering outcome")
                        await self._hold_inbox(attempt, deliveries, thread_id=session.thread_id,
                                               turn_id=turn_id, input_id=input_id)
                        # Keep this input's delivery uncertain without replaying it.
                        # It has no authority to stop the worker's native turn.
                        continue
                    else:
                        # Definitive rejection/turn-end race: pending for next input,
                        # never start a new turn merely to make steering succeed.
                        await self._resolve_inbox(attempt, deliveries, accepted=False,
                            thread_id=session.thread_id, turn_id=turn_id, input_id=input_id)
                        return

    async def _validate_submission(self, attempt: dict, artifact_id: str, *, background: bool = False) -> dict:
        """Validate a staged candidate. Inside a worker's activation the plugin may bound the
        wait (submission_wait): a longer validation continues and its result arrives later."""
        task = attempt.get("task")
        if task is None:
            task = await self.hub.task(attempt["task_id"], revision=attempt["task_revision"])
        bounded = None if background else getattr(self.plugin, "submission_wait", None)
        if bounded is None:
            return await self._validate(task, artifact_id)
        with bounded(self.hub):
            return await self._validate(task, artifact_id)

    async def _validate(self, task: dict, artifact_id: str) -> dict:
        # While it is attached, no pass of the driver takes its validation for stranded.
        attached = getattr(self.hub, "attached_validation", None)
        with attached(artifact_id) if attached is not None else contextlib.nullcontext():
            return await self._validate_attached(task, artifact_id)

    async def _validate_attached(self, task: dict, artifact_id: str) -> dict:
        phase = await self.hub.native_validation(artifact_id)
        if phase and phase["validation_result_json"] is not None:
            return json.loads(phase["validation_result_json"])
        if phase and phase["validation_state"] in {"obsolete", "blocked"}:
            raise HubError("INPUTS_CHANGED", phase["validation_reason"])
        artifact = await self.hub.artifact(artifact_id)
        if task.get("contract", {}).get("kind") in {"review"}:
            raise HubError("REVIEW_PROTOCOL_REQUIRED", "Review tasks submit protocol receipts")
        if artifact.get("publication_policy"):
            task = {**task, "contract": {
                **task["contract"], "acceptance_policy": artifact["publication_policy"]}}
        if phase:
            phase = await self.hub.native_validation(artifact_id, start=True)
            if phase["validation_result_json"] is not None:
                return json.loads(phase["validation_result_json"])
            if phase["validation_state"] in {"obsolete", "blocked"}:
                raise HubError("INPUTS_CHANGED", phase["validation_reason"])
        if phase and phase["validation_report_json"] is not None:
            report = ValidationReport.model_validate_json(phase["validation_report_json"])
        else:
            try:
                report = await self._original_validation(task, artifact, phase)
            except ValidationContinues as continues:
                # The plugin's own service goes on with it; asking again attaches to it.
                if phase:
                    await asyncio.shield(self.hub.native_validation_queued(artifact_id, str(continues), continues.details))
                raise HubError("VALIDATION_QUEUED", str(continues), details=continues.details) from None
            except BaseException as exc:
                if phase:
                    # Only an interrupted wait (a pause, a shutdown) leaves the plugin's own work
                    # going; an unexpected failure still needs confirmed recovery.
                    interrupted = (isinstance(exc, asyncio.CancelledError)
                                   or isinstance(exc, HubError) and exc.code in VALIDATION_HOLDS)
                    if interrupted and getattr(self.plugin, "validations_survive_interruption", False):
                        await asyncio.shield(self.hub.native_validation_queued(
                            artifact_id, "Waiting for the validation stopped; it continues outside TaskHub"))
                    else:
                        # No report does not prove that a local checker stopped.
                        await asyncio.shield(self.hub.native_validation_uncertain(artifact_id, str(exc) or type(exc).__name__))
                raise
            if phase:
                try:
                    await self.hub.retain_native_validation_report(artifact_id, report.model_dump(mode="json"))
                except BaseException as exc:
                    await self._report_not_kept(artifact_id, exc)
                    raise
        try:
            return await self._command(
                "artifact.accept", {"artifact_id": artifact_id, **report.model_dump()},
                actor=Actor(role=Role.VALIDATOR),
                key=("native-validation:" + artifact_id +
                     (":retry:" + str(phase["validation_generation"]) if phase["validation_generation"] else "")) if phase else None,
            )
        except HubError as exc:
            if phase and exc.code in {"INPUTS_CHANGED", "ARTIFACT_WITHDRAWN"}:
                # A known finished checker and an obsolete publication basis are
                # not an unknown process, and cannot pause unrelated work.
                await self.hub.block_native_validation(artifact_id, exc.message)
            raise

    async def _report_not_kept(self, artifact_id: str, exc: BaseException) -> None:
        """The check ended, but its report could not be kept (a full disk, say). A plugin's own
        service still has the result, so asking again attaches to it; a local checker needs
        confirmed recovery, as after any unexpected failure. If even that cannot be written, the
        driver finds the validation stranded once the database takes writes again."""
        try:
            if getattr(self.plugin, "validations_survive_interruption", False):
                await asyncio.shield(self.hub.native_validation_queued(
                    artifact_id, "Its report could not be kept (" + (str(exc) or type(exc).__name__)[:300]
                    + "); asking again attaches to the check"))
            else:
                await asyncio.shield(self.hub.native_validation_uncertain(artifact_id, str(exc) or type(exc).__name__))
        except Exception as error:
            logger.warning("Validation %s stays running until the driver finds it stranded: %s: %s",
                           artifact_id, type(error).__name__, str(error)[:300])

    async def _original_validation(self, task, artifact, phase):
        if phase is None:
            return await self.plugin.validate_candidate(self.hub, task, artifact)
        validation = asyncio.create_task(self.plugin.validate_candidate(self.hub, task, artifact))
        try:
            async with self.controls.watch(phase["validation_run_id"], phase.get("worker_id")) as watch:
                while not validation.done():
                    done, _ = await asyncio.wait([validation], timeout=0.2)
                    if done:
                        break
                    control = watch.latest()
                    if control is not None and control["run"]["state"] != "running":
                        raise HubError("RUN_PAUSED", "Run control stopped original validation; retain its recovery phase")
            return await validation
        finally:
            async def collect():
                if not validation.done():
                    validation.cancel()
                # Domain ProcessRunner cancellation awaits its owned process-tree
                # cleanup. Keep that await alive even across repeated cancellation.
                await asyncio.gather(validation, return_exceptions=True)

            await collected(asyncio.create_task(collect()))

    async def _recover_submission(self, pending: dict, *, background: bool = False) -> dict:
        phase = None
        try:
            phase = await self.hub.native_validation(pending["validation_artifact_id"])
            if phase and phase["validation_result_json"] is not None:
                return json.loads(phase["validation_result_json"])
            return await self._validate_submission(pending, pending["validation_artifact_id"], background=background)
        except HubError as exc:
            if exc.code not in VALIDATION_HOLDS:
                raise
            phase = await self.hub.native_validation(pending["validation_artifact_id"])
            held = {"state": (phase or {}).get("validation_state", "pending"),
                    "code": exc.code, "reason": exc.message, "validation_pending": True}
            if exc.code == "VALIDATION_QUEUED":
                held.update(job=exc.details, instruction=VALIDATION_QUEUED.format(
                    artifact_id=pending["validation_artifact_id"]))
            return held

    async def _interrupt_for_control(self, harness: Harness, turn_task, reason: str) -> TurnOutcome:
        try:
            interrupt = getattr(harness, "interrupt", None)
            if interrupt is None:
                raise RuntimeError("Harness does not support interruption")
            grace = getattr(getattr(harness, "config", None), "stop_grace_seconds",
                            self.hub.config.codex.stop_grace_seconds)
            await asyncio.wait_for(interrupt(), grace)
            # The turn's own close may finish received messages after its process exits.
            received = getattr(harness, "received_drain_seconds", 0.0)
            return await asyncio.wait_for(asyncio.shield(turn_task), grace * 5 + received + 1)
        except Exception as exc:
            turn_task.cancel()
            await asyncio.gather(turn_task, return_exceptions=True)
            await harness.close()
            return TurnOutcome(None, "unknown", False, harness.process_quiet,
                               error=reason + ": " + str(exc))

    async def run_once(self, run_id: str, *, assignment: dict | None = None) -> dict[str, Any] | None:
        # Only selection is serialized. An assignment already claimed starts at once, even while
        # the driver is still selecting others; claimed workers run concurrently.
        if assignment is None:
            async with self._admission_lock:
                run = await self.hub.run(run_id)
                if run["state"] != "running" and not (run.get("resumed_workers") or run.get("resumed_groups")):
                    return None
                assignment = await self._select(run_id)
            if assignment is None:
                return None
        else:
            hold = {"worker_id": assignment["worker_id"], "attempt_id": assignment["id"]}
            try:
                run = await self.hub.run(run_id, worker_id=assignment["worker_id"])
            except BaseException as error:
                # No activation exists yet to end this claim: hold it, as a stopping driver holds
                # the claims it selected, so that it does not stay running with nothing behind it.
                cause = "cancelled" if isinstance(error, asyncio.CancelledError) else str(error) or type(error).__name__
                try:
                    await collected(asyncio.ensure_future(self._command("runtime.preflight_hold", {
                        **hold, "reason": "The claim could not be checked before native activation: " + cause})))
                except Exception as failure:
                    logger.warning("Unstarted claim %s is left to recovery: %s", assignment["id"], failure)
                raise
            if run["state"] != "running":
                await self._command("runtime.preflight_hold", {
                    **hold, "reason": "Run control changed before any native process or model turn started"})
                return None
        self._executing.add(assignment["id"])
        prepare = getattr(self.hub, "prepare_assignment_workspace", None)
        if prepare is not None:
            # A failed checkout releases its claim (prepare_assignment_workspace); a prepared one
            # is only recognized again.
            try:
                async with self._preparations:
                    prepared = await prepare(assignment, tolerate_failure=True)
            except BaseException:
                self._executing.discard(assignment["id"])
                raise
            if prepared is None:
                self._executing.discard(assignment["id"])
                return None
            assignment = prepared
        return await Activation(self, run_id, assignment).run()

    async def _retain_outcome(self, result: dict[str, Any]) -> dict[str, Any]:
        retained = await self._bound_tool_result(result, self.retained_result_bytes)
        if retained is result:
            return result
        if not result.get("error"):
            retained.pop("error", None)
        for key in ("worker_id", "directive", "turn_status", "stop_reason", "artifact_id",
                    "candidate_id", "review_receipt_id", "interruption_id", "snapshot",
                    "integration_id", "commit", "target_head", "reason",
                    "diagnostic_refs"):
            if key not in result:
                continue
            value = result[key]
            if key == "diagnostic_refs":
                if not isinstance(value, list):
                    continue
                value = value[:32]
            elif not isinstance(value, (str, int, float, bool, type(None))):
                continue
            candidate = {**retained, key: value}
            if len(canonical(candidate).encode("utf-8")) <= self.retained_result_bytes:
                retained = candidate
        return retained

    async def run_until_idle(self, run_id: str, *, claims: deque[dict] | None = None) -> dict[str, Any]:
        """Drive every admissible execution; the Hub enforces task-local admission.

        The deterministic service timer invokes this again when sleeping checks
        are due. No active-worker count is used as a project admission quota.
        `claims` holds claims the operator made (task.join), shared with the run
        driver, which go on arriving while this pass runs.
        """
        claims = claims if claims is not None else deque()
        results: deque[dict[str, Any]] = deque(maxlen=self.recent_result_limit)
        completed_steps = 0
        failure_observed = False
        active: set[asyncio.Task] = set()
        identities: dict[asyncio.Task, dict[str, Any]] = {}
        assignments: deque[dict] = deque()
        validation_attempts: set[tuple[str, int]] = set()
        # A queued validation the driver could not attach to yet (its author drains, say) is
        # tried again after a while, and so is one whose attempt failed once it is pending or
        # queued again; every other generation is attempted once per drive.
        queued_retry_at: dict[tuple[str, int], float] = {}
        state = "idle"

        def launch(coroutine, identity: dict[str, Any]) -> None:
            task = asyncio.create_task(coroutine)
            active.add(task)
            identities[task] = identity

        try:
            while True:
                more = False  # This pass stopped admitting at its time limit, with work left.
                try:
                    persist_hold = getattr(self.hub, "persist_pending_dispatch_hold", None)
                    if persist_hold is not None:
                        try:
                            await persist_hold()
                        except Exception as error:
                            # The hold already applies in memory; only its record waits.
                            logger.warning("Dispatch hold write will retry: %s", type(error).__name__)
                    run = await self.hub.run(run_id)
                    admitting = run["state"] == "running" or bool(run.get("resumed_workers") or run.get("resumed_groups"))
                    if not admitting:
                        state = run["state"]
                    while claims:
                        # An operator's claim starts as an admitted one does; one the run no longer
                        # admits is held before start (run_once). A replayed command returns its claim
                        # again: only the worker's running attempt with no turn yet is launched.
                        claim = claims.popleft()
                        if (claim["id"] in self._executing
                                or any(item.get("attempt_id") == claim["id"] for item in identities.values())
                                or not await self.hub.unlaunched_claim(claim["id"])):
                            continue
                        launch(self.run_once(run_id, assignment=claim), {
                            "attempt_id": claim["id"], "worker_id": claim.get("worker_id"),
                            "task_id": claim.get("task_id")})
                    if admitting:
                        overdue = getattr(self.hub, "end_overdue_waits", None)
                        if overdue is not None:
                            try:
                                await overdue()  # Waits for an answer past their deadline end.
                            except Exception as error:
                                logger.warning("Answer deadlines deferred: %s", type(error).__name__)
                        recover_quiet = getattr(self.hub, "recover_quiet_runtime_failures", None)
                        if recover_quiet is not None:
                            try:
                                await recover_quiet(run_id, exclude_attempts=set(self._executing),
                                                    exclude_helpers=set(self._helpers))
                            except Exception as error:
                                # Recovery of earlier executions retries next pass; new work still starts.
                                logger.warning("Automatic recovery deferred: %s: %s", type(error).__name__, str(error)[:500])
                        for pending in await self.hub.pending_native_validations(run_id):
                            artifact_id = pending["validation_artifact_id"]
                            generation = (artifact_id, pending.get("validation_generation", 0))
                            if queued_retry_at.get(generation, float("inf")) <= time.monotonic():
                                validation_attempts.discard(generation)
                                del queued_retry_at[generation]
                            # A queued validation continues while its author keeps working.
                            if (generation not in validation_attempts
                                    and not any(item.get("validation_artifact_id") == artifact_id for item in identities.values())
                                    and (pending.get("validation_state") == "queued"
                                         or pending["attempt_id"] not in self._executing)):
                                validation_attempts.add(generation)
                                launch(self._recover_submission(pending, background=True), {
                                    "validation_artifact_id": artifact_id, "validation_generation": generation[1],
                                    "validation_state": pending.get("validation_state"), "task_id": pending["task_id"]})
                        while assignments:
                            assignment = assignments.popleft()
                            launch(self.run_once(run_id, assignment=assignment), {
                                "attempt_id": assignment["id"], "worker_id": assignment.get("worker_id"),
                                "task_id": assignment.get("task_id")})
                        async with self._admission_lock:
                            pass_ends = time.monotonic() + ADMISSION_PASS_SECONDS
                            while True:
                                selected = await self._select(run_id)
                                if selected is None:
                                    break
                                launch(self.run_once(run_id, assignment=selected), {
                                    "attempt_id": selected["id"], "worker_id": selected.get("worker_id"),
                                    "task_id": selected.get("task_id")})
                                if time.monotonic() >= pass_ends:
                                    # Recovery, validation retries and decision helpers run between
                                    # passes. On a busy run one pass admitted from 21:10 for over 40
                                    # minutes, and none of them ran (Linux pilot).
                                    more = True
                                    break
                        for intent in [*(await self.hub.due_recruitment_intents(run_id)),
                                       *(await self.hub.due_assignment_intents(run_id)),
                                       *(await self.hub.due_wake_intents(run_id))]:
                            if intent["id"] not in self._helpers:
                                self._helpers.add(intent["id"])
                                launch(self.run_helper(intent), {"helper_id": intent["id"],
                                    "worker_id": intent.get("source_worker_id"), "task_id": intent.get("task_id")})
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # A scheduler/storage fault does not cancel already admitted
                    # peers. Retry only coordination; unresolved model turns remain
                    # fenced by their existing durable attempt/turn records.
                    state, failure_observed = "needs_attention", True
                    logger.warning("Coordination admission will retry: %s: %s", type(exc).__name__, str(exc))
                    if not active:
                        results.append({"directive": "coordination_retry", "error_code": type(exc).__name__,
                                        "error": str(exc)[:2000]})
                        completed_steps += 1
                        break
                    await asyncio.sleep(1)
                if not active:
                    break
                done, _ = await asyncio.wait(active, timeout=0 if more else ADMISSION_INTERVAL_SECONDS,
                                             return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    active.remove(task)
                    identity = identities.pop(task)
                    try:
                        result = await task
                    except asyncio.CancelledError:
                        driver = asyncio.current_task()
                        if driver is not None and driver.cancelling():
                            raise
                        result = {**identity, "directive": "recovery_required", "turn_status": "cancelled",
                                  "error": "The independent activation was cancelled; its recovery evidence is retained.",
                                  "error_code": "ActivationCancelled"}
                    except HubError as exc:
                        if exc.code == "WORKER_PAUSED":
                            continue  # Only this worker is held; the run continues.
                        if exc.code in {"RUN_PAUSED", "RUN_STOPPED", "PROJECT_SHUTDOWN"}:
                            state = "stopped" if exc.code in {"RUN_STOPPED", "PROJECT_SHUTDOWN"} else "paused"
                            continue
                        if exc.code == "HELPER_RECONCILIATION_REQUIRED":
                            state, failure_observed = "needs_attention", True
                            continue
                        result = {**identity, "directive": "recovery_required", "turn_status": "failed",
                                  "error": exc.message, "error_code": exc.code}
                    except Exception as exc:
                        # run_once/run_helper finish owned-process cleanup and persist
                        # their recovery hold before raising. A local failure must
                        # neither cancel peers nor replay the uncertain activation.
                        result = {**identity, "directive": "recovery_required", "turn_status": "failed",
                                  "error": str(exc), "error_code": type(exc).__name__}
                    if result is None:
                        continue
                    completed_steps += 1
                    if identity.get("validation_artifact_id") is not None:
                        generation = (identity["validation_artifact_id"], identity.get("validation_generation", 0))
                        if identity.get("validation_state") == "queued" and result.get("validation_pending"):
                            queued_retry_at[generation] = time.monotonic() + 10
                        elif result.get("directive") == "recovery_required":
                            # It failed before its validation could record why (a full disk, say):
                            # attached again once it is pending or queued again.
                            queued_retry_at[generation] = time.monotonic() + VALIDATION_RETRY_SECONDS
                    if result.get("assignment") is not None:
                        assignments.append(result.pop("assignment"))
                    queued = "VALIDATION_QUEUED" in {result.get("code"), result.get("validation", {}).get("code")}
                    failure_observed |= bool(result.get("error") or result.get("diagnostic_refs")
                        or result.get("turn_status") in {"failed", "unknown"}
                        or result.get("directive") in {"abandoned", "recovery_required"}
                        or (not queued and (result.get("validation_pending")
                                            or result.get("validation", {}).get("validation_pending"))))
                    try:
                        retained = await self._retain_outcome(result)
                    except Exception as exc:
                        # Observation storage failure cannot invalidate a known
                        # terminal or cancel other workers.
                        logger.warning("Outcome observation could not be saved: %s", type(exc).__name__)
                        retained = {key: result.get(key) for key in
                                    ("worker_id", "attempt_id", "directive", "turn_status", "error_code")}
                        retained["observation_error"] = str(exc)[:2000]
                        failure_observed = True
                    results.append(retained)
                    if result.get("directive") in {"abandoned", "recovery_required"}:
                        # Failed/ambiguous executions are quarantined by the Hub.
                        # A service failure never becomes a voluntary task pause.
                        state = "needs_attention"
        finally:
            for task in active:
                task.cancel()
            if active:
                await asyncio.gather(*active, return_exceptions=True)
            for assignment in assignments:
                await self._command("runtime.preflight_hold", {
                    "worker_id": assignment["worker_id"], "attempt_id": assignment["id"],
                    "reason": "A selected assignment was preserved before native activation stopped"})
        return {"run_id": run_id, "state": state, "steps": completed_steps, "results": list(results),
                "results_retained": len(results), "results_truncated": completed_steps - len(results),
                "failure_observed": failure_observed}


class Activation:
    """One worker activation, from its claim to its cleanup: what WorkerRunner.run_once runs.

    Its attributes are the activation's state, which the native callbacks share: they are
    called while the turn runs and read the state current then.
    """

    def __init__(self, runner: WorkerRunner, run_id: str, attempt: dict[str, Any]):
        self.runner, self.hub = runner, runner.hub
        self.run_id, self.attempt = run_id, attempt
        self.worker_id = attempt["worker_id"]
        self.identity = {"run_id": run_id, "worker_id": self.worker_id,
                         "task_id": attempt["task_id"], "attempt_id": attempt["id"]}
        self.harness: Harness | None = None
        self.watcher: asyncio.Task | None = None
        self.turn_task: asyncio.Task | None = None
        self.steering: asyncio.Task | None = None
        self.preparing: asyncio.Task | None = None
        self.preparation_cancellable = True
        self.intent: dict[str, Any] | None = None
        self.input_id: str | None = None
        self.input_delivered = False
        self.talk_delivery: dict[str, Any] = {}
        self.session: Any = None
        self.finalized = False
        self.diagnostic_refs: list[str] = []
        self.observed_background_failures: set[int] = set()
        self.failure_reason: str | None = None
        self.stop_reason: str | None = None
        self.visible_saved = False
        self.terminal_saved = False
        self.native_turn_id: str | None = None
        self.inference_dispatched = False
        self.prepared_prompt: PreparedPrompt | None = None
        self.preflight_hold: tuple[str, str] | None = None
        self.outcome: TurnOutcome | None = None
        self.phase = "preflight"
        # Set while preparing, for the callbacks.
        self.worker: dict[str, Any] = {}
        self.backend = "codex"
        self.instructions_sha256 = ""
        self.rotation: dict[str, Any] | None = None
        self.context_limit: int | None = None
        self.rotation_notice: list[str] = []
        self.packet: dict[str, Any] = {}
        self.intent_id = ""
        self.context_lock = asyncio.Lock()
        self.started_event = asyncio.Event()
        self.context_generation = 0
        self.acknowledged_generation = 0
        self.keeps_inputs = False
        # Native usage samples, recorded in their order by one writer (`_write_usage`).
        self.usage_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.usage_writer: asyncio.Task | None = None

    async def run(self) -> dict[str, Any]:
        try:
            held = await self._prepare()
            if held is not None:
                return held
            prompt = await self._compose_input()
            await self._run_turn(prompt)
            return await self._settle()
        except BaseException as exc:
            if isinstance(exc, HubError) and exc.code == "NATIVE_CATALOG_MIGRATION_REQUIRED":
                self.preflight_hold = ("runtime.catalog_hold", exc.message)
            elif (isinstance(exc, HubError) and exc.code in {NOT_READY, "RUN_PAUSED", "WORKER_PAUSED"}
                  and not self.inference_dispatched and self.intent is None):
                self.preflight_hold = ("runtime.preflight_hold", exc.message)
            if not isinstance(exc, asyncio.CancelledError):
                record = normalize_error(exc, source="activation")
                record["phase"] = self.phase
                # Private diagnostics only: bounded locations, without source
                # lines, local variables, prompt bodies, or absolute paths.
                record["frames"] = [
                    {"file": Path(frame.filename).name[:160], "line": frame.lineno,
                     "function": frame.name[:120]}
                    for frame in traceback.extract_tb(exc.__traceback__)[-8:]]
                self.failure_reason = record["message"]
                await self._diagnostic(record)
            raise
        finally:
            try:
                await collected(asyncio.create_task(self._cleanup()))
            finally:
                self.runner._executing.discard(self.attempt["id"])
                self.runner._steering_attempts.discard(self.attempt["id"])

    # -- Preparation ------------------------------------------------------------------------

    async def _preflight(self, operation, *, cancellable=True):
        # Retain the current task for the shielded outer cleanup, including
        # native open/probe cleanup after repeated cancellation.
        self.preparation_cancellable = cancellable
        self.preparing = asyncio.create_task(operation, name="worker-preflight")
        try:
            await asyncio.wait([self.preparing])
            result = self.preparing.result()
        except BaseException:
            if cancellable and not self.preparing.done():
                self.preparing.cancel()
            raise
        self.preparing = None
        return result

    async def _reserve_turn(self) -> None:
        # A DB command cannot be cancelled once its transaction is running.
        # Retain its result even if the activation is cancelled, so cleanup
        # can mark the reservation not_sent instead of losing its identity.
        reserved = await self.runner._command("turn.reserve", {"worker_id": self.worker_id})
        self.intent = reserved.get("intent", reserved)

    async def _session_settings(self) -> dict[str, Any]:
        reader = getattr(self.hub, "session_settings", None)
        if callable(reader):
            return await reader(self.worker_id)
        backend, kind = await self.hub.worker_runtime(self.worker_id)
        return {"backend": backend, "kind": kind, "model": None, "effort": None, "new": True}

    async def _open_harness(self, settings: dict[str, Any]) -> Harness:
        profile = None
        if settings["backend"] == "claude":
            # A Claude conversation, new or continued, runs on the profile in use now.
            chooser = getattr(self.hub, "claude_session_profile", None)
            profile = await chooser() if callable(chooser) else None
        self.harness = harness = await self.runner._new_harness(
            settings["backend"], kind=settings["kind"], model=settings["model"], effort=settings["effort"],
            **({"claude_profile": profile} if profile is not None else {}))
        diagnostic_setter = getattr(harness, "set_diagnostic_handler", None)
        if diagnostic_setter is not None:
            diagnostic_setter(self._diagnostic)
        control_setter = getattr(harness, "set_control_handler", None)
        if control_setter is not None:
            control_setter(self._account_control)
        activity_setter = getattr(harness, "set_activity_handler", None)
        if activity_setter is not None:
            activity_setter(self._activity)
        usage_setter, observed = getattr(harness, "set_rate_limit_handler", None), getattr(self.hub, "claude_usage_observed", None)
        if usage_setter is not None and callable(observed) and profile is not None:
            # What Claude Code reports of the profile's usage windows, shown in the Claude tab.
            usage_setter(lambda info, name=profile["name"]: observed(name, info))
        return harness

    async def _prepare(self) -> dict[str, Any] | None:
        """Open the native session and reserve the turn; the result of a hold instead, if the run
        stopped dispatching meanwhile."""
        runner, hub, attempt, worker_id, preflight = self.runner, self.hub, self.attempt, self.worker_id, self._preflight
        await preflight(runner.activity.emit("activation_started", **self.identity))
        for pending in await preflight(
                hub.pending_native_validations(self.run_id, worker_id=worker_id)):
            # Finish interrupted original submissions before continuing their
            # author. Later unavailable-check retries belong to the driver;
            # an old candidate must not delay or fail a new model activation.
            if pending["validation_generation"] == 0:
                await preflight(runner._recover_submission(pending))
        control = await preflight(hub.run(self.run_id, worker_id=worker_id))
        if control["state"] != "running" or control.get("dispatch_hold"):
            # Nothing is launched: the claim waits like a paused one until admission resumes.
            transition = await runner._command("runtime.preflight_hold", {
                "worker_id": worker_id, "attempt_id": attempt["id"],
                "reason": (control["dispatch_hold"]["message"] if control.get("dispatch_hold") else
                           "Run control changed while recovering original validation before native dispatch")})
            self.finalized = True
            return {"worker_id": worker_id, **transition}
        # A conversation keeps the backend, model and effort it began with; Codex threads
        # never move to Claude. A new one starts with the current choices.
        settings = await preflight(self._session_settings())
        backend = settings["backend"]
        self.backend = backend
        harness = await preflight(self._open_harness(settings))
        worker = await preflight(hub.worker(worker_id))
        release = getattr(hub, "release_empty_thread", None)
        if (worker.get("thread_id") and callable(release)
                and await preflight(release(worker_id), cancellable=False)):
            # Codex never saved a thread without a turn: this activation starts a new one.
            worker = await preflight(hub.worker(worker_id))
        if (worker.get("kind") == "review" and attempt.get("execution_mode") == "message_only"
                and worker.get("thread_id") and callable(getattr(hub, "review_record", None))):
            # A question about a finished review starts a new conversation from its report.
            await preflight(runner._review_report_file(worker), cancellable=False)
            await preflight(hub.release_conversation(worker_id, "review_clarification"), cancellable=False)
            worker = await preflight(hub.worker(worker_id))
        activation_instructions = await preflight(runner._standing_instructions(worker, attempt, backend),
                                                  cancellable=False)
        self.instructions_sha256 = hashlib.sha256(activation_instructions.encode("utf-8")).hexdigest()
        self.rotation = await preflight(runner._rotate_if_due(worker, attempt, self.instructions_sha256),
                                        cancellable=False)
        if self.rotation is not None:
            worker = await preflight(hub.worker(worker_id))
        if not worker.get("thread_id") and not settings.get("new", True):
            # The old conversation was released: the new one starts with the current choices,
            # and its worker may move to the backend the operator requested.
            fresh = await preflight(self._session_settings())
            if any(fresh[key] != settings[key] for key in ("backend", "model", "effort")):
                await harness.close()
                if fresh["backend"] != backend:
                    backend = self.backend = fresh["backend"]
                    activation_instructions = await preflight(
                        runner._standing_instructions(worker, attempt, backend), cancellable=False)
                    self.instructions_sha256 = hashlib.sha256(activation_instructions.encode("utf-8")).hexdigest()
                harness = await preflight(self._open_harness(fresh))
            settings = fresh
        settings_reader = getattr(hub, "rotation_settings", None)
        rotation_settings = await preflight(settings_reader()) if callable(settings_reader) else None
        self.context_limit = (rotation_settings["context_tokens"] if rotation_settings
                              and worker.get("effective_rotation_mode") == "aggressive" else None)
        catalog = await preflight(runner._ensure_catalog(worker, harness=harness))
        # The guides' copies in the workspace, where the on-demand ones are read.
        await preflight(asyncio.to_thread(
            prepare_guidance, hub.workspace(worker_id), runner.plugin,
            review=worker.get("kind") == "review", worker_kind=worker.get("kind", "ordinary"), context_epoch=worker["context_epoch"],
            execution_mode=attempt.get("execution_mode", "work")), cancellable=False)
        note_launch = getattr(hub, "note_launch", None)
        if callable(note_launch):
            # Durable before any process exists: recovery after a crash in this window
            # reads it instead of an ownership record that was never written.
            plan = getattr(harness, "plan_process", None)
            await preflight(note_launch("worker", attempt["id"], plan() if callable(plan) else None),
                            cancellable=False)
        self.session = session = await preflight(harness.open(
            workspace=hub.workspace(worker_id), thread_id=worker.get("thread_id"),
            tools=runner.tools, instructions=activation_instructions,
        ))
        await preflight(hub.note_owned_process(
            "worker", attempt["id"], getattr(harness, "ownership_receipt", None)), cancellable=False)
        await preflight(runner._command("worker.bind_session", {
            "worker_id": worker_id, "thread_id": session.thread_id,
            "model": session.model, "config_hash": session.config_hash,
            # Historical key names; the values are this worker's own backend home and profile.
            "codex_home": hub.config.backend_config(backend).home,
            "codex_profile": hub.config.runtime_profile(backend),
            "settings": {"model": settings["model"], "effort": settings["effort"]},
            "instructions_sha256": self.instructions_sha256,
            **catalog,
        }))
        runner.transcripts.record_instructions(worker_id, session.thread_id, {
            "instructions": activation_instructions, "codex_profile": hub.config.runtime_profile(backend),
            **runner._catalog_transcript(catalog, thread_id=worker.get("thread_id"))})
        self.worker = worker
        native_setter = getattr(harness, "set_native_handler", None)
        if native_setter:
            native_setter(self._native)
        if callable(getattr(harness, "steer", None)):
            runner._steering_attempts.add(attempt["id"])
        await preflight(runner.activity.emit("session_ready", **self.identity, thread_id=session.thread_id))
        packet = await preflight(hub.context(worker_id))
        self.packet, self.talk_delivery = await preflight(runner._native_packet(
            packet, attempt,
            full_context=bool(attempt.get("continuation_id") or attempt.get("interruption_id"))
            and attempt.get("execution_mode", "work") == "work"))
        await preflight(self._reserve_turn(), cancellable=False)
        assert self.intent is not None
        self.intent_id = self.intent["id"]
        # Codex keeps every input across compaction; Claude's compaction summarizes them.
        self.keeps_inputs = bool(getattr(harness, "compaction_keeps_inputs", False))
        compaction_setter = getattr(harness, "set_compaction_handler", None)
        if compaction_setter is not None:
            compaction_setter(self._compacted)
        return None

    async def _compose_input(self) -> str:
        """This turn's input. For task work it is also the continuation prepared from the last."""
        runner, hub, attempt, worker, preflight = self.runner, self.hub, self.attempt, self.worker, self._preflight
        worker_id, packet, rotation, session = self.worker_id, self.packet, self.rotation, self.session
        presented = await preflight(runner._present(packet))
        if rotation is not None:
            presented["rotation"] = runner._rotation_view(rotation)
            if attempt.get("execution_mode", "work") == "work" and worker.get("kind", "ordinary") == "ordinary":
                # Its own instruction would continue "in this original conversation".
                presented["continuation_instruction"] = presented["rotation"].pop("instruction")
            # Otherwise the rotation adds its note, and the mode's or role's instruction stays.
        elif (not worker.get("thread_id") and attempt.get("execution_mode", "work") == "work"
              and not presented.get("review") and worker.get("kind") != "integrator"):
            saved = await preflight(runner._saved_handoff(worker, attempt))
            if saved is not None:
                # The task's handoff file is already written (an earlier conversation's, or one
                # carried over from another project): the conversation starts from it.
                presented["rotation"] = runner._rotation_view(saved)
                presented["continuation_instruction"] = presented["rotation"].pop("instruction")
            else:
                presented["continuation_instruction"] = (
                    "Begin your assigned task: read paths.task for all of it, and paths.knowledge_readme for "
                    "essential guidance and relevant public notes.")
                # The handoff file's fixed sections, ready to fill in as the work goes.
                await preflight(asyncio.to_thread(
                    runner._start_handoff, worker_id, attempt["task_id"],
                    str(packet.get("task", {}).get("title", "")), task_file(presented)))
        elif (not worker.get("thread_id") and attempt.get("execution_mode", "work") == "work"
              and presented.get("review")):
            # A review's handoff file, ready to fill in as the review goes.
            await preflight(asyncio.to_thread(
                runner._start_handoff, worker_id, attempt["task_id"],
                str(packet.get("task", {}).get("title", "")), task_file(presented), review=True))
        elif (not worker.get("thread_id") and attempt.get("execution_mode", "work") == "work"
              and worker.get("kind") == "integrator"):
            # A warning about an earlier attempt, appended to the instruction, stays.
            presented["continuation_instruction"] = presented.get("continuation_instruction", "").replace(
                INTEGRATION_CONTINUE, INTEGRATION_START, 1) or INTEGRATION_START
        elif callable(getattr(hub, "requested_rotation", None)):
            repair = await preflight(hub.requested_rotation(worker_id))
            if repair is not None and repair["repairs"] and repair.get("handoff"):
                presented["rotation"] = {"handoff": handoff_path(repair["task_id"]), "instruction": ROTATION_REPAIR.format(
                    path=handoff_path(repair["task_id"]), problems="; ".join(repair["handoff"].get("problems", [])))}
                presented.pop("continuation_instruction", None)  # This turn only repairs the file.
        resumable_work = (worker.get("kind", "ordinary") in {"ordinary", "review", "integrator"}
                          and attempt.get("execution_mode", "work") == "work")
        if resumable_work:
            built_prompt = await preflight(asyncio.to_thread(
                runner.continuation_prompts.prepare, presented, worker_id=worker_id,
                thread_id=session.thread_id,
                snapshot_packet=scenario_entry(presented) if worker.get("kind") == "review" else None,
                allow_delta=bool(worker.get("thread_id") and not packet.get("runtime_recovery"))),
                cancellable=False)
            self.prepared_prompt = built_prompt
            prompt = built_prompt.text
        else:
            # A message input does not become the task-work baseline, and the baseline stays:
            # the next work input is a delta from it unless a compaction dropped it.
            if presented.get("instruction_guidance", {}).get("reason") == "context_compacted":
                await preflight(asyncio.to_thread(runner.continuation_prompts.invalidate, worker_id,
                                                  reason="context_compacted"), cancellable=False)
            prompt = render_prompt(project_packet(presented))
        prepared_prompt = self.prepared_prompt
        self.input_id = runner.transcripts.prepare_input(worker_id, session.thread_id, "taskhub", prompt,
            attempt_id=attempt["id"], kind="message" if attempt.get("execution_mode") in {"message", "message_only"}
            else "continuation" if worker.get("thread_id") else "review" if worker.get("kind") == "review" else "initial",
            context_mode=prepared_prompt.mode if prepared_prompt else "full",
            context_reason=prepared_prompt.reason if prepared_prompt else "special_activation")
        turn_options = getattr(self.harness, "set_turn_options", None)
        if turn_options:
            turn_options(client_input_id=self.input_id)
        await preflight(runner._hold_inbox(attempt, self.talk_delivery, thread_id=session.thread_id,
                                          turn_id=None, input_id=self.input_id, pending=True))
        # Preparation can await local I/O after admission. Recheck control (every
        # pause, including Pause after turn), ownership and task bindings before any
        # native input is dispatched.
        await preflight(hub.authorize_worker(runner._worker_actor(attempt), dispatch=True))
        require_dispatch = getattr(hub, "require_dispatch", None)
        if callable(require_dispatch):
            # An account hold stops new input; the reserved turn is recorded as not sent.
            await preflight(require_dispatch(self.backend))
        return prompt

    # -- The native turn ----------------------------------------------------------------------

    async def _run_turn(self, prompt: str) -> None:
        """Send the input and wait for the turn's outcome, or interrupt it for run control."""
        runner, attempt, harness, session = self.runner, self.attempt, self.harness, self.session
        assert harness is not None
        self.phase = "native_turn"
        self.watcher = asyncio.create_task(
            runner._watch_run(self.run_id, attempt=attempt),
            name="worker-run-state")
        self.turn_task = asyncio.create_task(harness.run_turn(
            prompt, on_tool=self._tool, on_usage=self._usage, on_started=self._started,
        ))
        self.inference_dispatched = True
        if attempt["id"] in runner._steering_attempts:
            self.steering = asyncio.create_task(self._resilient_steering(harness, attempt, session, self.started_event))
        waits = [self.turn_task, self.watcher]
        backgrounds = {task for task in (self.steering,) if task is not None}
        while True:
            done, _ = await asyncio.wait([*waits, *backgrounds], return_when=asyncio.FIRST_COMPLETED)
            for background in done & backgrounds:
                try:
                    await background
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    # Delivery/observation is independent of the active model
                    # turn. Keep its journal and let the worker continue.
                    logger.warning("Worker background channel deferred: %s", type(error).__name__)
                    await self._background_failure(error, "native_turn_background")
                backgrounds.remove(background)
            if done.intersection(waits):
                break
        if self.turn_task in done:
            self.phase = "native_turn_result"
            self.outcome = outcome = await self.turn_task
        else:
            reason, self.stop_reason = self.watcher.result()
            self.outcome = outcome = await runner._interrupt_for_control(harness, self.turn_task, reason)
        if not outcome.process_quiet or not harness.process_quiet:
            raise RuntimeError("Harness returned before its owned process tree was quiet")
        if outcome.visible_output and not self.visible_saved:
            runner.transcripts.capture_native(session.thread_id, self.worker_id, "item/completed", {
                "turnId": outcome.turn_id, "item": {"id": "turn-result:" + attempt["id"],
                "type": "agentMessage", "text": outcome.visible_output}})
        if outcome.terminal_observed and not self.terminal_saved:
            runner.transcripts.capture_native(session.thread_id, self.worker_id, "turn/completed", {
                "turn": {"id": outcome.turn_id, "status": outcome.status}}, source="runtime_terminal")
            self.terminal_saved = True
        elif not outcome.terminal_observed and self.input_id is not None:
            runner.transcripts.record_uncertain_turn(self.worker_id, session.thread_id, outcome.turn_id,
                                                     self.input_id, attempt_id=attempt["id"])
        # The owned native process is already quiet. These tasks finish local
        # receipt/DB work; an elapsed shutdown deadline must not erase a
        # completed, fully drained native outcome while that work progresses.
        self.phase = "post_turn_steering"
        if self.steering is not None:
            await self._collect_background(self.steering, self.phase)
        if self.input_id is not None and not self.input_delivered:
            runner.transcripts.ambiguous(self.input_id, outcome.error or "Native input delivery is uncertain")
            await runner._hold_inbox(attempt, self.talk_delivery, thread_id=session.thread_id,
                                     turn_id=outcome.turn_id, input_id=self.input_id)
        self.phase = "turn_finish"
        await self._flush_usage()
        await runner._command("turn.finish", {
            "intent_id": self.intent_id, "status": outcome.status,
            "terminal_observed": outcome.terminal_observed, "process_quiet": True})
        self.intent = None
        await runner.activity.emit(
            "turn_finished", **self.identity, status=outcome.status,
            turn_id=outcome.turn_id, process_quiet=harness.process_quiet,
            directive=outcome.directive)

    async def _settle(self) -> dict[str, Any]:
        """Record what the finished turn leaves: its committed transition, a parked interruption,
        a completed turn, or an abandoned attempt."""
        runner, hub, attempt, worker_id, outcome = self.runner, self.hub, self.attempt, self.worker_id, self.outcome
        assert outcome is not None
        stop_reason = self.stop_reason
        current_worker = await hub.worker(worker_id)
        if (outcome.terminal_observed and outcome.status != "unknown"
                and (outcome.directive is not None or current_worker["state"] == "draining")):
            # A tool command may commit before cancellation prevents its reply.
            # Finalize that durable transition instead of discarding it.

            snapshot = await asyncio.to_thread(
                hub.blobs.snapshot, hub.workspace(worker_id))
            await self._judge_rotation()
            transition = await runner._command("runtime.quiescent", {
                "worker_id": worker_id, "snapshot": snapshot,
            })
            self.finalized = True
            if transition.get("artifact_id") and transition.get("directive") == "verify":
                transition["validation"] = await runner._recover_submission({
                    "task_id": attempt["task_id"], "task_revision": attempt["task_revision"],
                    "validation_artifact_id": transition["artifact_id"]})
            if attempt.get("execution_mode") != "message_only":
                await hub.record_first_turn(worker_id, attempt["task_id"], outcome.turn_id)
            return {"worker_id": worker_id, "task_id": attempt["task_id"],
                    "turn_status": outcome.status, "diagnostic_refs": self.diagnostic_refs,
                    "error": outcome.error, **transition}
        if (outcome.terminal_observed and outcome.status == "interrupted"
                and stop_reason in {"operator_pause", "operator_stop", "task0_complete", "project_shutdown"}):
            snapshot = await asyncio.to_thread(
                hub.blobs.snapshot, hub.workspace(worker_id))
            current_worker = await hub.worker(worker_id)
            transition = await runner._command("runtime.park_interrupted", {
                "attempt_id": attempt["id"], "task_id": attempt["task_id"],
                "task_revision": attempt["task_revision"], "worker_id": worker_id,
                "thread_id": self.session.thread_id, "remote_turn_id": outcome.turn_id,
                "turn_intent_id": self.intent_id, "usage_epoch": current_worker["usage_epoch"],
                "codex_profile": hub.config.runtime_profile(self.backend),
                "terminal_observed": True, "process_quiet": True,
                "snapshot": snapshot, "stop_reason": stop_reason})
            self.finalized = True
            return {"worker_id": worker_id, "task_id": attempt["task_id"],
                    "turn_status": outcome.status, "error": outcome.error,
                    "diagnostic_refs": self.diagnostic_refs, **transition}
        if outcome.terminal_observed and outcome.status == "completed":
            if attempt.get("execution_mode") != "message_only":
                await hub.record_first_turn(worker_id, attempt["task_id"], outcome.turn_id)
            await self._judge_rotation()
            transition = await hub.ordinary_turn_completed(worker_id, attempt["id"])
            self.finalized = True
            return {"worker_id": worker_id, "task_id": attempt["task_id"],
                    "turn_status": outcome.status, "diagnostic_refs": self.diagnostic_refs, **transition}
        transition = await runner._command("runtime.abandon", {
            "worker_id": worker_id, "quiescent": True,
            "reason": outcome.error or "Turn ended without a wait or submission",
        })
        self.finalized = True
        return {"worker_id": worker_id, "task_id": attempt["task_id"],
                **transition, "turn_status": outcome.status, "error": outcome.error,
                "diagnostic_refs": self.diagnostic_refs,
                "stop_reason": (stop_reason if outcome.terminal_observed
                                and outcome.status in {"interrupted", "cancelled"} else None)}

    async def _judge_rotation(self) -> None:
        """Judge this conversation's requested rotation before the command that frees the worker,
        so its next activation finds the rotation ready. Judged after it, the next activation could
        start first and run a turn on the conversation past its limit (Linux pilot, 2026-10-22,
        while a settle waited ten minutes). A request not judged here stays requested; the next
        settle of this conversation judges it."""
        try:
            await self.runner._settle_rotation_request(self.worker_id, self.session.thread_id)
        except Exception as error:
            await self._diagnostic({"code": "RotationJudgementDeferred", "message": str(error)[:2000]})

    # -- Callbacks while the turn runs --------------------------------------------------------

    async def _diagnostic(self, record: dict[str, Any]) -> None:
        try:
            sha = await asyncio.to_thread(self.hub.blobs.put_json, {**self.identity, **record})
            self.diagnostic_refs.append(sha)
            await self.runner.activity.emit(
                "runtime_error", **self.identity, diagnostic_sha256=sha,
                status="retrying" if record.get("will_retry") else "error",
                error_type=str(record.get("code", "native_error")))
        except Exception as error:
            logger.warning("Worker diagnostics deferred: %s", type(error).__name__)

    async def _background_failure(self, error: Exception, failure_phase: str) -> None:
        if id(error) in self.observed_background_failures:
            return
        self.observed_background_failures.add(id(error))
        record = normalize_error(error, source="worker_background")
        record["phase"] = failure_phase
        record["frames"] = [{"file": Path(frame.filename).name[:160], "line": frame.lineno,
                             "function": frame.name[:120]}
                            for frame in traceback.extract_tb(error.__traceback__)[-8:]]
        await self._diagnostic(record)

    async def _collect_background(self, task, failure_phase):
        results = await asyncio.gather(asyncio.shield(task), return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                await self._background_failure(result, failure_phase)

    async def _resilient_steering(self, harness, attempt, session, started_event):
        failures = 0
        while True:
            try:
                await self.runner._steer_messages(harness, attempt, session, started_event)
                return
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._background_failure(error, self.phase)
                if harness.process_quiet:
                    return
                failures += 1
                await asyncio.sleep(min(30, 2 ** min(failures - 1, 5)))

    async def _account_control(self, record: dict[str, Any]) -> None:
        await self.runner._note_account_hold(record, subject_id=self.attempt["id"], worker_id=self.worker_id,
                                             harness=self.harness)

    async def _activity(self, event: str, fields: dict[str, Any]) -> None:
        try:
            await self.runner.activity.emit(event, **self.identity, **fields)
        except Exception as error:
            logger.warning("Worker activity observation deferred: %s", type(error).__name__)

    async def _native(self, method: str, params: dict) -> None:
        try:
            self.runner.transcripts.capture_native(self.session.thread_id, self.worker_id, method, params)
        except Exception as error:
            logger.warning("Native transcript observation deferred: %s", type(error).__name__)
            return
        self.visible_saved |= method == "item/completed" and params.get("item", {}).get("type") == "agentMessage"
        self.terminal_saved |= method == "turn/completed"

    async def _compacted(self) -> None:
        async with self.context_lock:
            await self.runner._command("worker.compacted", {"worker_id": self.worker_id,
                                                            "instructions_sha256": self.instructions_sha256})
            if self.keeps_inputs:
                # The inputs that carried the task context are kept and the guides are
                # standing instructions: sending the context again would only grow it.
                return
            await asyncio.to_thread(self.runner.continuation_prompts.invalidate, self.worker_id,
                                    reason="context_compacted")
            # The next tool result carries the current task view.
            self.context_generation += 1

    async def _started(self, turn_id: str) -> None:
        runner, attempt, session, input_id = self.runner, self.attempt, self.session, self.input_id
        self.native_turn_id = turn_id
        await runner._command("turn.bind", {
            "intent_id": self.intent_id, "remote_turn_id": turn_id,
        }, key="bind:" + self.intent_id + ":" + turn_id)
        self.input_delivered = True
        try:
            if input_id is not None:
                runner.transcripts.delivered(input_id, turn_id)
            await runner._resolve_inbox(attempt, self.talk_delivery, accepted=True,
                thread_id=session.thread_id, turn_id=turn_id, input_id=input_id)
            await runner._ack_native(self.packet, attempt, {})
            if self.prepared_prompt is not None:
                current_worker = await self.hub.worker(self.worker_id)
                await asyncio.to_thread(runner.continuation_prompts.confirm, self.prepared_prompt,
                                        current_worker=current_worker)
            elif self.packet.get("instruction_guidance", {}).get("reason") == "context_compacted":
                await asyncio.to_thread(runner.continuation_prompts.invalidate, self.worker_id)
        except Exception as error:
            await self._diagnostic({"code": "InputAcknowledgementDeferred", "message": str(error)[:2000],
                                    "input_id": input_id, "turn_id": turn_id, "input_delivered": True})
        self.started_event.set()
        await self._activity("turn_started", {"turn_id": turn_id, "thread_id": session.thread_id})

    async def _usage(self, sample: dict[str, Any]) -> None:
        """Queue a native usage sample for `_write_usage`, which records the samples in order.
        The native notifications, among them the turn's terminal, are handled one at a time: in
        the Linux pilot each sample waited 12-58 s there for the congested coordination
        database, and terminals queued behind them were lost when the harness closed."""
        self.usage_queue.put_nowait(sample)
        if self.usage_writer is None or self.usage_writer.done():
            self.usage_writer = asyncio.create_task(self._write_usage(), name="worker-usage")

    async def _write_usage(self) -> None:
        while not self.usage_queue.empty():
            sample = self.usage_queue.get_nowait()
            try:
                await self._record_usage(sample)
            finally:
                self.usage_queue.task_done()

    async def _flush_usage(self, timeout: float = USAGE_FLUSH_SECONDS) -> None:
        """Record every queued sample: `turn.finish` takes the turn's total from them."""
        writer = self.usage_writer
        if writer is None or writer.done():
            return
        try:
            await asyncio.wait_for(asyncio.shield(writer), timeout)
        except TimeoutError:
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)
            # Raw native session history retains the cumulative samples.
            await self._diagnostic({"code": "UsageRecordingDeferred", "pending": self.usage_queue.qsize(),
                                    "message": f"Usage samples were not recorded within {timeout:g} s"})
            try:
                await self.hub.note_worker_accounting(self.worker_id, "Native usage samples could not be recorded")
            except Exception:
                logger.warning("Accounting warning persistence is also unavailable for %s", self.worker_id)

    async def _record_usage(self, sample: dict[str, Any]) -> None:
        runner, worker_id = self.runner, self.worker_id
        try:
            recorded = await runner._command("usage.record", {
                "worker_id": worker_id, "sample": sample,
            })
        except Exception as error:
            # Raw native session history retains cumulative samples.
            # Observation trouble cannot interrupt the model's work.
            await self._diagnostic({"code": "UsageRecordingDeferred", "message": str(error)[:2000],
                                    "sample": sample})
            try:
                await self.hub.note_worker_accounting(worker_id, "A native usage sample could not be recorded")
            except Exception:
                logger.warning("Accounting warning persistence is also unavailable for %s", worker_id)
            return
        await self._activity("usage", {"total_tokens": sample.get("totalTokens")})
        if recorded.get("uncertain"):
            logger.warning("Usage remains uncertain for %s; native work continues", worker_id)
        last = sample.get("lastTokenUsage")
        context = last.get("inputTokens") if isinstance(last, dict) else None
        if self.context_limit is not None and isinstance(context, int) and context > self.context_limit:
            try:
                notice = await runner._request_rotation(worker_id, self.session.thread_id, self.attempt, context)
            except Exception as error:
                await self._diagnostic({"code": "RotationRequestDeferred", "message": str(error)[:2000]})
            else:
                if notice:
                    self.rotation_notice.append(notice)

    async def _invoke_tool(self, name: str, args: dict[str, Any], call_id: str) -> dict[str, Any]:
        runner, attempt, worker_id = self.runner, self.attempt, self.worker_id
        refresh = None
        admitted = None
        async with self.context_lock:
            if self.context_generation != self.acknowledged_generation:
                refreshed = await self.hub.context(worker_id)
                refreshed, refreshed_talk = await runner._native_packet(
                    refreshed, attempt, include_messages=attempt["id"] not in runner._steering_attempts,
                    compacted=True)
                generation = self.context_generation
                # The refresh arrives with this tool result; say why.
                refreshed["context_refresh_instruction"] = COMPACTION_REFRESH
                refresh = await runner._model_result({"context": refreshed}, attempt)

                async def admitted() -> None:
                    async with self.context_lock:
                        if generation == self.context_generation:
                            try:
                                await runner._ack_native(refreshed, attempt, refreshed_talk)
                            except HubError as error:
                                if error.code not in {"WORKER_DRAINING", "STALE_BINDING", "RUN_PAUSED",
                                                      "WORKER_PAUSED", "STALE_REVISION", "STALE_CONTEXT"}:
                                    raise
                                # A successful draining action, a pause or a newer revision
                                # needs no second mutation. Keep the next-turn full refresh
                                # pending instead of turning successful work into a failed
                                # tool reply.
                                return
                            await asyncio.to_thread(runner.continuation_prompts.invalidate, worker_id)
                            self.acknowledged_generation = generation
        # Every operation still uses current fencing, revisions and input
        # checks. Compaction alone is neither a revocation nor a failed call.
        result = await runner._tool(attempt, self.session.thread_id, name, args, call_id)
        if self.rotation_notice:
            # The reminder rides on the next tool result, on every backend.
            result = _with_field(result, "rotation_notice", self.rotation_notice.pop())
        if refresh is None:
            return result
        after = getattr(result, "after_send", None)

        async def delivered():
            if after is not None:
                await after()
            if admitted is not None:
                await admitted()

        return ToolResult({**result, **refresh}, after_send=delivered)

    async def _tool(self, name: str, args: dict[str, Any], call_id: str) -> dict[str, Any]:
        runner = self.runner
        safe_name = name if name in {item["name"] for item in runner.tools} else "unknown"

        async def observe(event, state, *, directive=None, **fields):
            observed = {"tool": safe_name}
            if event == "tool_finished":
                observed["status"] = "ok" if state == "completed" else state
                observed["directive"] = directive
            await self._activity(event, observed)
            try:
                runner.transcripts.record_action(self.worker_id, self.session.thread_id, self.native_turn_id,
                    call_id, safe_name, state, **fields)
            except Exception as error:
                logger.warning("Tool observation deferred: %s", type(error).__name__)

        await observe("tool_started", "running", **runner._action_arguments(args))
        try:
            result = await self._invoke_tool(name, args, call_id)
        except BaseException as exc:
            fields = ({"status": "cancelled"} if isinstance(exc, asyncio.CancelledError)
                      else {"status": "error", "error_type": type(exc).__name__})
            await observe("tool_finished", fields["status"], error_type=type(exc).__name__)
            raise
        await observe("tool_finished", "error" if "error" in result else "completed",
                      directive=result.get("runtime_directive"), result=runner._action_result(result))
        return result

    # -- Cleanup ------------------------------------------------------------------------------

    async def _cleanup(self) -> None:
        runner, attempt, worker_id, harness, session = self.runner, self.attempt, self.worker_id, self.harness, self.session
        pending = [task for task in (self.watcher, self.turn_task, self.steering, self.preparing)
                   if task is not None]
        for task in pending:
            if task is self.preparing and (not self.preparation_cancellable or task.cancelling()):
                # Its first cancellation may already be closing owned
                # native processes. Collect that cleanup without aborting it.
                continue
            if not task.done():
                task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        turn_task = self.turn_task
        if self.outcome is None and turn_task is not None and not turn_task.cancelled():
            # A background exception can win the wait in the same tick
            # as a successful native return. Retrieve its completed proof
            # of terminal, usage drainage and process quiescence first.
            if turn_task.exception() is None:
                self.outcome = turn_task.result()
        outcome = self.outcome
        close_error = None
        if harness is not None:
            try:
                await harness.close()
            except Exception as exc:
                close_error = exc
        # Samples the close received are recorded before an unfinished turn is finished.
        await self._flush_usage()
        # A concurrent reservation may have committed during cancellation.
        # Decide only after its retained result and all owned
        # preparation have settled; an admitted intent needs not_sent.
        intent, input_id = self.intent, self.input_id
        if (self.preflight_hold is not None and intent is None and not self.inference_dispatched
                and (harness is None or harness.process_quiet)):
            name, reason = self.preflight_hold
            await runner._command(name, {
                "worker_id": worker_id, "attempt_id": attempt["id"], "reason": reason})
            self.finalized = True
        if not self.finalized:
            if (self.inference_dispatched and input_id is not None
                    and session is not None and not self.terminal_saved):
                runner.transcripts.record_uncertain_turn(worker_id, session.thread_id,
                    self.native_turn_id, input_id, attempt_id=attempt["id"])
            if self.inference_dispatched and input_id is not None and not self.input_delivered:
                runner.transcripts.ambiguous(input_id, self.failure_reason or "Native start delivery is uncertain")
                if session is not None:
                    await runner._hold_inbox(attempt, self.talk_delivery, thread_id=session.thread_id,
                                             turn_id=None, input_id=input_id)
            if intent is not None and self.inference_dispatched:
                completion: dict[str, Any] = {
                    "intent_id": intent["id"], "status": "unknown",
                    "process_quiet": harness is None or harness.process_quiet}
                if (outcome is not None and outcome.terminal_observed
                        and outcome.status in {"completed", "interrupted", "failed", "cancelled"}
                        and outcome.process_quiet and harness is not None and harness.process_quiet):
                    # Do not replace an observed, fully returned outcome
                    # with uncertainty because later local work failed.
                    # This does not clear delivery or accounting holds.
                    completion.update(status=outcome.status, terminal_observed=True,
                                      process_quiet=True)
                try:
                    await runner._command("turn.finish", completion)
                except HubError as exc:
                    if exc.code != "TURN_FINISHED":
                        raise
            if harness is None or harness.process_quiet:
                if intent is not None and not self.inference_dispatched:
                    reason = "Native input was not dispatched. " + (self.failure_reason or "Activation stopped during local preparation.")
                    await runner._command("turn.not_sent", {
                        "intent_id": intent["id"], "worker_id": worker_id, "attempt_id": attempt["id"],
                        "thread_id": session.thread_id if session is not None else None,
                        "input_id": input_id, "inference_dispatched": False, "process_quiet": True,
                        "reason": reason[:8192], "deliveries": [
                            {"kind": kind, "delivery_id": delivery["delivery_id"]}
                            for kind, delivery in (("peer", self.talk_delivery), ("human", self.talk_delivery.get("_human", {})))
                            if delivery.get("delivery_id")],
                    })
                    if input_id is not None:
                        runner.transcripts.reconciled(input_id, delivered=False, turn_id=None, reason=reason)
                await runner._command("runtime.abandon", {
                    "worker_id": worker_id, "quiescent": True,
                    "reason": self.failure_reason or
                              "Activation interrupted or failed after process shutdown",
                })
        await runner.activity.emit(
            "activation_finished", **self.identity,
            status="finalized" if self.finalized else "interrupted",
            process_quiet=harness is None or harness.process_quiet)
        if close_error is not None:
            raise close_error
