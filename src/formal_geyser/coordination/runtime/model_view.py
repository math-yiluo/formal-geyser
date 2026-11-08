"""Worker-facing task views, separate from delivery receipts and execution state.

Only coordinator-owned structures are projected. Authored task prose, evidence,
message files and domain records are never recursively redacted by key name.
"""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from ..material_files import MaterialFiles
from ..worker_names import OPAQUE_FIELDS, is_reserved_identity
from .handoff import JOURNAL_PATH, handoff_path

_OPERATIONAL = frozenset({
    "packet_id", "delivery_id", "context_epoch", "fencing_token", "attempt_id",
    "helper_id", "thread_id", "remote_turn_id", "turn_id", "run_id", "continuation_id",
    "after_sequence", "through_sequence",
    "created_at", "updated_at", "worker_number", "reviewer_number", "active_attempt",
    "prepare_attempt", "publication_attempt",
    "source_thread_id", "source_turn_id",
    "first_turn_id", "decision_helper_id", "helper_intent_id", "input_id", "intent_id",
    "tokens_used", "previous_tokens_used", "previous_cumulative_total_tokens",
    "cumulative_total_tokens", "native_usage_delta", "pid", "process_id",
})
_QUIET_EVENTS = frozenset({
    "project.setup", "worker.fresh_created", "worker.registered",
    "turn.started", "run.started", "repository.initialized",
    "assignment.requested", "assignment.decided", "worker.conversation_released",
    "rotation.requested", "rotation.ready", "rotation.completed", "rotation.handoff_repair_requested",
    "review.report_recorded", "worker.rotation_mode_selected", "worker.context_compacted",
    "execution.process_owned", "execution.quiescence_verified", "turn.execution_closed",
    "worker.accounting_held", "worker.automatically_recovered", "worker.admission_retry", "helper.accounting_warning",
    "helper.execution_closed", "helper.retry_ready",
})


_SUMMARY_FACTS = ("title", "verdict", "decision", "outcome", "state", "status", "policy", "kind", "revision",
                  "task_revision", "reason", "candidate_id", "artifact_id", "task_id", "review_task_id")


def _notification_summary(kind: str, payload: dict) -> tuple[str, list[str]]:
    """One line for a notification, and the material files that hold anything longer.

    Hashes, registry paths and other bookkeeping stay out; the reference and fetch
    locate the full record.
    """
    facts = []
    for key in _SUMMARY_FACTS:
        value = payload.get(key)
        if isinstance(value, bool):  # A recruitment's decision, for one.
            facts.append(f"{key.replace('_', ' ')} {'yes' if value else 'no'}")
        elif isinstance(value, (str, int)) and str(value).strip():
            text = str(value).strip()
            facts.append(f"{key.replace('_', ' ')} {text if len(text) <= 160 else text[:160] + '…'}")
    summary = kind.replace(".", " ").replace("_", " ") + (": " + "; ".join(facts[:5]) if facts else "")
    files = [value for key, value in payload.items()
             if (key == "material_path" or key.endswith("_material_path")) and isinstance(value, str)]
    return summary, files


def _notification(event: dict, task_id: Any) -> dict | None:
    """What a worker hears of an update: its kind, reference and one-line summary. None for
    bookkeeping, and for what the input already shows (its own task's publication, the
    environment's acceptance)."""
    kind = str(event.get("kind", ""))
    if (event.get("kind") in _QUIET_EVENTS
            or kind == "task.published" and event.get("object_id") == task_id
            or kind == "artifact.accepted" and event.get("payload", {}).get("policy") == "lean.environment@1"):
        return None
    notification = {"event": event.get("kind")}
    if event.get("object_id") and not kind.startswith(("worker.", "turn.", "continuation.", "helper.", "run.")):
        notification["reference"] = event["object_id"]
    elif is_reserved_identity(event.get("object_id")):
        notification["worker"] = event["object_id"]
    payload = event.get("payload", {})
    summary, files = _notification_summary(kind, payload if isinstance(payload, dict) else {})
    notification["summary"] = summary
    if files:
        notification["files"] = files
    return notification


NOTHING_NEW = "Nothing new since your last input. To wait for something, pause rather than calling sync again."


def sync_view(packet: dict) -> dict:
    """What a sync adds to a conversation that already has its task: only what is new."""
    view = project_packet(packet)
    result: dict[str, Any] = {"task": {key: view["task"][key] for key in ("id", "name", "revision", "title", "state")
                                       if key in view.get("task", {})}}
    for key in ("notifications", "messages", "next_action", "workspace_refresh"):
        if view.get(key):
            result[key] = view[key]
    if len(result) == 1:
        result["instruction"] = NOTHING_NEW
    return result


def render_prompt(view: dict, *, opening: str | None = None) -> str:
    """Readable exact JSON; the dictionary already contains only task-facing data."""
    if opening:
        view = {**view, "instruction": opening}
    return json.dumps(view, ensure_ascii=False, indent=2, allow_nan=False)


def file_reference(value: Any) -> Any:
    if not isinstance(value, dict):
        return deepcopy(value)
    path = value.get("workspace_path") or value.get("material_path") or value.get("path") or value.get("absolute_path")
    if path is None:
        return deepcopy(value)
    return {"path": path, **({"required": value["requires_read"]} if "requires_read" in value else {})}


def _details(value: Any, workspace: Path | None) -> Any:
    if workspace is None:
        return deepcopy(value)
    return file_reference(MaterialFiles(workspace).json(value))


def _review_details(value: Any, workspace: Path | None) -> Any:
    # Only these coordinator-owned review fields may already be material files.
    if isinstance(value, dict) and (value.get("material_path") or value.get("workspace_path")):
        return file_reference(value)
    return _details(value, workspace)


def _detail_path(value: Any, workspace: Path | None) -> Any:
    reference = _details(value, workspace)
    return reference["path"] if workspace is not None else reference


def reading_view(guidance: dict) -> dict:
    """The on-demand guides; the required ones are part of the standing instructions."""
    directory = guidance.get("directory", ".private/instructions")
    result: dict[str, Any] = {}
    if guidance.get("instruction"):
        result["instruction"] = guidance["instruction"]
    optional = guidance.get("optional_files", {})
    if optional:
        result["on_demand"] = [f"{directory}/{name}" for name in optional]
    if guidance.get("removed_guides"):
        result["removed"] = [f"{directory}/{name}" for name in guidance["removed_guides"]]
    return result


def message_view(envelope: dict) -> list[dict]:
    result = []
    for message in envelope.get("messages", []):
        item = {key: deepcopy(message[key]) for key in
                ("sender_role", "sender", "sender_name", "from", "request_id", "request_kind", "title")
                if key in message}
        if message.get("sender_display_name"):
            item["sender"] = message["sender_display_name"]
        for key in ("sender_worker_id", "sender_id", "worker_id"):
            name = message.get(key)
            if is_reserved_identity(name):
                item.setdefault("sender", name)
        if message.get("sender_role") == "operator":
            item.setdefault("sender", "User")
        if message.get("kind") == "question":  # Answered with talk_send kind answer, answers this id.
            item.update(kind="question", question_id=message["id"])
        elif message.get("kind") == "answer":
            item.update(kind="answer", answers=message["answers"])
        if message.get("native_message"):
            item["file"] = file_reference(message["native_message"])
        elif "text" in message:
            item["text"] = message["text"]  # Exact authored text, never inferred authority.
        if item:
            result.append(item)
    return result


def review_view(review: dict, workspace: Path | None) -> dict:
    result: dict[str, Any] = {}
    if "author_introduction" in review:
        result["author_introduction"] = file_reference(review["author_introduction"])
    request = review.get("review_request", {})
    result["phase"] = request.get("phase", "pending")
    if request.get("handoff"):
        result["next_action"] = request["handoff"]
    # Decision bases are business references required by review operations.
    if request.get("basis"):
        result["decision_basis"] = deepcopy(request["basis"])
    candidate = review.get("candidate", {})
    result["candidate"] = {key: deepcopy(candidate[key]) for key in ("id", "kind") if key in candidate}
    if candidate.get("record_file"):
        result["candidate"]["record"] = file_reference(candidate["record_file"])
    for name, pair in candidate.get("primary_pair", {}).items():
        result["candidate"][name] = file_reference(pair)
    for key in ("source_files", "removed_paths", "repository"):
        if candidate.get(key):
            result["candidate"][key] = _review_details(candidate[key], workspace)
    if review.get("validation"):
        result["validation"] = deepcopy(review["validation"])
    host = review.get("host_task", {})
    # A long text is a file to read; the reference says whether it is required.
    result["acceptance"] = {key: file_reference(host[key]) for key in
                             ("title", "acceptance_instructions", "acceptance_conditions") if key in host}
    if host.get("files"):
        result["acceptance"]["task_file"] = file_reference(host["files"])
    if review.get("exact_details_file"):
        result["exact_details_file"] = file_reference(review["exact_details_file"])
    return result


def task_file(packet: dict) -> str | None:
    """The file holding the task's full text, when it has one."""
    return ((packet.get("native", {}).get("task_files") or {}).get("path")
            or packet.get("task", {}).get("contract_material_path"))


def project_packet(packet: dict, *, workspace: Path | None = None) -> dict:
    """Project an internal packet without changing its receipt, cursor or binding."""
    native = packet.get("native", {})
    if workspace is None and native.get("workspace") and Path(native["workspace"]).is_absolute():
        workspace = Path(native["workspace"])
    identity = packet.get("worker_identity", {})
    name = identity.get("display_name") or packet.get("display_name") or packet.get("worker_id")
    result: dict[str, Any] = {}
    if is_reserved_identity(name):
        result["identity"] = name
    if packet.get("context_refresh_instruction") or packet.get("continuation_instruction"):
        result["instruction"] = packet.get("context_refresh_instruction") or packet["continuation_instruction"]
    task = packet.get("task", {})
    # Task IDs/revisions remain actionable entity references, never execution authority.
    paths = {}
    task_path = task_file(packet)
    # The task's text lives in its file; inputs keep only an excerpt, so they stay small when
    # Codex keeps every input across compaction.
    keys = ("id", "name", "revision", "title", "kind", "state") + (() if task_path else ("objective", "context", "constraints"))
    result["task"] = {key: deepcopy(task[key]) for key in keys if key in task and task[key] not in (None, "", [], {})}
    if task_path and isinstance(task.get("objective"), str) and task["objective"].strip():
        objective = task["objective"].strip()
        result["task"]["objective_excerpt"] = objective if len(objective) <= 300 else objective[:300] + "…"
    if task_path:
        paths["task"] = task_path
    if task.get("id") and task.get("kind") != "integration":
        # The agent's own memory of the task, kept current as it works (rotation may replace
        # the conversation at any time).
        paths["handoff"] = handoff_path(str(task["id"]))
        paths["journal"] = JOURNAL_PATH
    for key in ("workspace", "private_notes"):
        if native.get(key):
            paths[key] = native[key]
    directories = native.get("directories", {})
    for key in ("library", "wiki", "knowledge_readme", "public_notes", "literature", "literature_index", "central_literature", "drafts", "graphs",
                "operation_guide", "submission_guide"):
        if directories.get(key):
            paths[key] = directories[key]
    if native.get("library", {}).get("root"):
        paths["library"] = native["library"]["root"]
    if directories:
        paths["reference_index"] = _detail_path(directories, workspace)
    # Full environment and resource maps are useful on demand, not every turn.
    environment = native.get("lean_environment")
    if environment:
        paths["lean_environment"] = _detail_path(environment, workspace)
    resources = native.get("read_only_resources", {})
    if resources:
        result["read_only_resources"] = {key: value for key, value in resources.items()
                                         if value not in paths.values() and key not in {"shared_mathlib", "lean_toolchain"}}
        if not result["read_only_resources"]:
            result.pop("read_only_resources")
    if native.get("shared_dags"):
        paths["shared_dags"] = native["shared_dags"].get("current_path") or native["shared_dags"].get("root")
    if paths:
        result["paths"] = paths
    if native.get("repository_refresh"):
        result["workspace_refresh"] = deepcopy(native["repository_refresh"])
    if task.get("payload") and not task_path:
        result["task"]["details"] = _details(task["payload"], workspace)
    for field in ("checkpoint", "inputs", "requirements", "resume_requirements"):
        if packet.get(field):
            entries = packet[field]
            if field == "inputs":
                entries = [item for item in entries if item.get("kind") != "lean.environment"]
            if entries:
                result[field] = deepcopy(entries)
    if packet.get("message_state"):
        state = packet["message_state"]
        result["message_state"] = {key: deepcopy(state[key]) for key in
            ("task_execution_authority", "after_turn_intention", "resume_tool", "resume_operation", "resume_effect") if key in state}
        if state.get("saved_wait"):
            wait = state["saved_wait"]
            result["message_state"]["saved_wait"] = {key: deepcopy(wait[key]) for key in ("state", "predicate", "material_path") if key in wait}
    if packet.get("rotation"):
        result["rotation"] = deepcopy(packet["rotation"])
    if packet.get("review_report"):
        result["review_report"] = packet["review_report"]
    if packet.get("review"):
        result["review"] = review_view(packet["review"], workspace)
    if packet.get("instruction_guidance"):
        result["reading"] = reading_view(packet["instruction_guidance"])
    messages = [*message_view(packet.get("human_messages", {})), *message_view(packet.get("private_talk", {}))]
    if messages:
        result["messages"] = messages
    notifications = [notification for event in packet.get("updates", [])
                     if (notification := _notification(event, task.get("id"))) is not None]
    if notifications:
        result["notifications"] = notifications
    if (packet.get("requires_sync") or packet.get("has_more_updates")
            or packet.get("human_messages", {}).get("has_more")
            or packet.get("private_talk", {}).get("has_more")):
        result["next_action"] = "More updates or messages are available; use the sync operation to receive them."
    return result


def _tool_metadata(value: Any) -> Any:
    if isinstance(value, list):
        return [_tool_metadata(item) for item in value]
    if not isinstance(value, dict):
        return value
    authored = OPAQUE_FIELDS | {"inputSchema", "arguments", "schema", "text", "qualifications"}
    result = {}
    for key, item in value.items():
        if key in _OPERATIONAL or key in {"attempts", "usage", "execution", "codex_profile", "helper_intents"}:
            continue
        result[key] = deepcopy(item) if key in authored else _tool_metadata(item)
    return result


def tool_view(value: dict, *, workspace: Path | None = None) -> dict:
    """Remove transport envelopes while retaining exact actionable domain evidence."""
    if "packet_id" in value and isinstance(value.get("task"), dict):
        return project_packet(value, workspace=workspace)
    result = _tool_metadata(value)
    if "error" in value and isinstance(value.get("details"), dict):
        result["details"] = _tool_metadata(value["details"])
    if isinstance(value.get("context"), dict) and "packet_id" in value["context"]:
        result["context"] = project_packet(value["context"], workspace=workspace)
    if "native_message" in result:
        result["native_message"] = file_reference(result["native_message"])
    if "human_messages" in result or "private_talk" in result:
        result["messages"] = [*message_view(result.pop("human_messages", {})),
                              *message_view(result.pop("private_talk", {}))]
    return result
