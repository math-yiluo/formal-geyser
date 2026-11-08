"""The runtime's own record of a task's state, written for a conversation that replaces another.

It does not depend on the agent remembering anything: TaskHub's records, the files the old
conversation touched, its last replies and its recent errors. The agent's handoff file
complements it and is the agent's own memory.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .files import ScopedFiles
from .handoff import TASK_DIRECTORY, HandoffCheck

REASONS = {"idle": "the conversation had been idle", "context": "the conversation's context grew large",
           "instructions": "your standing instructions changed",
           "handoff": "an earlier conversation ended and left your handoff file",
           "backend": "you moved to another model backend, as the operator asked"}


def state_path(task_id: str) -> str:
    return f"{TASK_DIRECTORY}/{task_id}/state.md"


def _quote(text: str) -> str:
    return "\n".join("> " + line for line in text.splitlines()) or "> "


def wait_text(predicate: dict[str, Any]) -> str:
    """A pause's predicate in words."""
    parts = []
    for item in predicate.get("requirements", []):  # Recorded before pauses took subscriptions instead.
        if item.get("artifact_id"):
            parts.append(f"artifact `{item['artifact_id']}` accepted")
        elif item.get("task_id"):
            parts.append(f"task `{item['task_id']}` (revision {item.get('task_revision')}) completed")
    parts += [f"a change to task `{item['task_id']}` that meets: {item['need']}"
              for item in predicate.get("subscriptions", [])]
    parts += [f"the machine validation of `{artifact}`" for artifact in predicate.get("validations", [])]
    parts += [f"the outcome of candidate `{candidate}`: published, rejected, deferred or withdrawn"
              for candidate in predicate.get("publications", [])]
    parts += [f"the answer to your question `{question}`" for question in predicate.get("answers", [])]
    if predicate.get("external_response_id"):
        parts.append(f"the answer to request `{predicate['external_response_id']}`")
    if predicate.get("completion_declaration_id"):
        parts.append(f"completion declaration `{predicate['completion_declaration_id']}`: its feedback if you "
                     "declared it, otherwise its decision")
    joined = (" or " if predicate.get("mode") == "any" else " and ").join(parts)
    return joined or "the condition recorded with the pause"


def render(*, state: dict[str, Any], history: dict[str, Any], reason: str, mode: str,
           handoff: HandoffCheck | None) -> str:
    task = state["task"]
    lines = [f"# State of task: {task['title']}", "",
             f"TaskHub wrote this file when a new conversation replaced an earlier one "
             f"({REASONS.get(reason, reason)}; rotation mode: {mode}). It is rewritten at every such "
             "change: keep your own notes in the handoff file and the journal.", ""]
    if handoff is not None and not handoff.valid:
        problems = "; ".join(handoff.problems) if handoff.problems else "it is missing"
        lines += [f"**Your handoff file `{handoff.path}` needs attention:** {problems}.",
                  "Bring it up to date from this file and your notes before continuing.", ""]
    if handoff is not None and handoff.notes:
        lines += ["Notes on your handoff file: " + "; ".join(handoff.notes) + ".", ""]
    lines += ["## Task", "", f"- {task['title']} (`{task['id']}`), revision {task['revision']}, {task['state']}."]
    if task.get("contract_file"):
        lines.append(f"- Contract: `{task['contract_file']}`")
    lines.append("")
    if state.get("members"):
        lines += ["## Team", ""]
        lines += [f"- {member['name']}: {member['role']}" + ("" if member.get("current", True) else " (left)")
                  for member in state["members"]]
        lines.append("")
    lines += ["## Your candidates for this task", ""]
    if state.get("candidates"):
        for item in state["candidates"]:
            parts = [item["status"]]
            if item.get("validation"):
                parts.append("machine validation " + item["validation"])
            if item.get("review"):
                parts.append("review " + item["review"])
            lines.append(f"- `{item['id']}` {item['kind']}: " + "; ".join(parts))
    else:
        lines.append("- None.")
    lines.append("")
    messages = state.get("messages") or {}
    unread = [f"{messages[key]} unread {label}" for key, label in (("human", "human messages"),
                                                                    ("peer", "peer messages")) if messages.get(key)]
    if unread:
        lines += ["## Messages", "", "- " + ", ".join(unread) + "; they arrive with your next input.", ""]
    if state.get("wait"):
        lines += ["## Waiting for", "", f"- {wait_text(state['wait'])}", ""]
    checkpoint = state.get("checkpoint") or {}
    if checkpoint.get("summary") or checkpoint.get("next_action"):
        lines += ["## Last checkpoint", ""]
        if checkpoint.get("summary"):
            lines += [_quote(checkpoint["summary"]), ""]
        if checkpoint.get("next_action"):
            lines += ["Next action: " + checkpoint["next_action"], ""]
    if history.get("files"):
        lines += ["## Files the earlier conversation worked with (most recent first)", ""]
        lines += [f"- `{item['path']}` ({item['use']})" for item in history["files"]]
        lines.append("")
    if history.get("replies"):
        lines += ["## Its last replies", ""]
        for reply in history["replies"]:
            lines += [_quote(reply["text"]), ""]
    if history.get("errors"):
        lines += ["## Its recent errors", ""]
        for error in history["errors"]:
            what = error.get("operation") or error.get("tool") or "command"
            detail = error.get("code") or (f"exit code {error['exit_code']}" if "exit_code" in error else "")
            message = error.get("error") or error.get("command") or ""
            lines.append(f"- {what}: {detail}" + (f" ({message})" if message else ""))
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write(workspace: Path, task_id: str, text: str) -> str:
    """Write the state file inside the private task directory and return its relative path."""
    relative = state_path(task_id)
    ScopedFiles(workspace).write(relative, text)
    return relative
