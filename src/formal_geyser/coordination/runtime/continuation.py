"""Delivery-bound, disposable baselines for ordinary and review continuation inputs.

The Hub still constructs and acknowledges its full authoritative packet. This
file-backed cache only decides how much of an already delivered state the next
native input needs to repeat. Losing the cache always falls back to full input.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import portalocker

from ..storage import atomic_json, canonical
from ..task_names import present_task_references
from .instructions import continuation_guidance
from .model_view import project_packet, reading_view, render_prompt

_FORMAT = 1
# Recovery checkpoints and execution bookkeeping belong to the full/recovery
# input, not routine same-conversation continuation. Transient deliveries have
# their own durable Hub receipts and must never be deduplicated by this cache.
_NON_STATE = frozenset({
    "packet_id", "attempt", "checkpoint", "execution", "usage", "after_sequence",
    "through_sequence", "has_more_updates", "requires_sync", "material_access",
    "continuation_instruction", "updates", "private_talk", "human_messages",
    "instruction", "reading", "messages", "notifications", "next_action", "rotation",
})


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _changes(previous: Any, current: Any, path: list[str], replacements: list[dict],
             removed: list[list[str]]) -> None:
    if isinstance(previous, dict) and isinstance(current, dict):
        for key in previous.keys() - current.keys():
            removed.append([*path, key])
        for key, value in current.items():
            if key not in previous:
                replacements.append({"path": [*path, key], "value": deepcopy(value)})
            else:
                _changes(previous[key], value, [*path, key], replacements, removed)
    elif canonical(previous) != canonical(current):
        # Arrays are complete replacements, never positional patches. Empty,
        # missing, null, false and zero must remain different states.
        replacements.append({"path": path, "value": deepcopy(current)})


@dataclass(frozen=True)
class PreparedPrompt:
    worker_id: str
    thread_id: str
    task_id: str
    context_epoch: int
    token: str
    snapshot_json: str
    text: str
    mode: Literal["full", "delta"]
    reason: str

    @property
    def binding(self) -> dict:
        return {"worker_id": self.worker_id, "thread_id": self.thread_id,
                "task_id": self.task_id, "context_epoch": self.context_epoch}


class ContinuationPromptStore:
    def __init__(self, root: Path):
        self.root = Path(root) / ".task-based" / "prompt-contexts"

    def _path(self, worker_id: str) -> Path:
        return self.root / (_sha(worker_id) + ".json")

    @contextmanager
    def _locked(self, worker_id: str) -> Iterator[Path]:
        """The worker's record, held under its lock."""
        path = self._path(worker_id)
        self.root.mkdir(parents=True, exist_ok=True)
        with portalocker.Lock(str(path.with_suffix(".lock")), timeout=10):
            yield path

    @staticmethod
    def _read(path: Path) -> dict | None:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError, RecursionError):
            return None
        if not isinstance(record, dict) or record.get("format") != _FORMAT:
            return None
        if record.get("status") == "confirmed":
            state = record.get("snapshot")
            try:
                valid = isinstance(state, dict) and record.get("snapshot_sha256") == _sha(canonical(state))
            except (TypeError, ValueError, RecursionError):
                valid = False
            if not valid:
                return None
        return record

    def prepare(self, packet: dict, *, worker_id: str, thread_id: str,
                allow_delta: bool = True, snapshot_packet: dict | None = None,
                task_names: dict[str, str] | None = None) -> PreparedPrompt:
        task = packet.get("task", {})
        epoch = packet.get("context_epoch")
        if (not isinstance(task, dict) or not isinstance(task.get("id"), str)
                or not task["id"] or type(epoch) is not int or epoch < 0
                or not worker_id or not thread_id):
            raise ValueError("Work context requires exact worker, thread, task and epoch identities")
        binding = {"worker_id": worker_id, "thread_id": thread_id,
                   "task_id": task["id"], "context_epoch": epoch}
        state_packet = packet if snapshot_packet is None else snapshot_packet
        if (state_packet.get("context_epoch") != epoch
                or any(state_packet.get("task", {}).get(key) != task.get(key)
                       for key in ("id", "revision", "kind"))
                or state_packet.get("execution_mode") != packet.get("execution_mode")):
            raise ValueError("Continuation state must match the delivered task, mode and epoch")
        # Recovery may send additional full context. Keep the normal scenario
        # projection as the baseline, so its later omission never masquerades
        # as removal of still-valid task conditions or other recovery facts.
        view = project_packet(packet)
        state_view = project_packet(state_packet)
        snapshot = {key: deepcopy(value) for key, value in state_view.items() if key not in _NON_STATE}
        if isinstance(state_packet.get("instruction_guidance"), dict):
            snapshot["instruction_guidance"] = deepcopy(state_packet["instruction_guidance"])
        snapshot_json = canonical(snapshot)
        token = uuid4().hex
        with self._locked(worker_id) as path:
            previous = self._read(path)
            mode: Literal["full", "delta"]
            rendered: dict[str, Any]
            eligible = (allow_delta and previous is not None
                        and previous.get("status") == "confirmed" and previous.get("binding") == binding
                        and previous.get("projection") == "native-files")
            if eligible:
                assert previous is not None
                replacements: list[dict] = []
                removed: list[list[str]] = []
                # Guide metadata is rendered separately, as a brief path reminder;
                # keep its full versions in this same delivery baseline without a
                # second reading ledger.
                guidance = snapshot.get("instruction_guidance")
                before, after = dict(previous["snapshot"]), dict(snapshot)
                if isinstance(guidance, dict):
                    before.pop("instruction_guidance", None)
                    after.pop("instruction_guidance", None)
                _changes(before, after, [], replacements, removed)
                removed.sort()
                rendered = {
                    "instruction": (
                        "Continue the review from its recorded phase."
                        if task.get("kind") in {"review"} else "Continue your current task."),
                    "task": {key: deepcopy(task[key]) for key in ("id", "name", "revision", "title", "kind")
                             if key in task},
                }
                if isinstance(guidance, dict):
                    changed = continuation_guidance(guidance, previous["snapshot"].get("instruction_guidance"))
                    if changed is not None:
                        rendered["reading"] = reading_view(changed)
                if replacements or removed:
                    rendered["instruction"] += (
                        " Apply these changes to your previously supplied context; "
                        "each path's value replaces that field, and removed fields no longer apply.")
                    if replacements:
                        rendered["changes"] = replacements
                    if removed:
                        rendered["removed_fields"] = removed
                for key in ("notifications", "messages", "rotation"):
                    if view.get(key):
                        rendered[key] = deepcopy(view[key])
                if view.get("next_action"):
                    rendered["next_action"] = view["next_action"]
                if not any(key in rendered for key in (
                        "changes", "removed_fields", "notifications", "messages", "next_action", "rotation", "reading")):
                    rendered["instruction"] += " No new context changes or deliveries."
                mode, reason = "delta", "confirmed_same_context"
            else:
                rendered = deepcopy(view)
                mode = "full"
                reason = ("explicit_full" if not allow_delta else "missing_baseline" if previous is None
                          else "unconfirmed_baseline" if previous.get("status") != "confirmed"
                          else "context_binding_changed")
            # Only the rendered text names tasks (Task N): the baseline and the binding keep ids.
            text = render_prompt(present_task_references(rendered, task_names) if task_names else rendered)
            # This write must succeed before any transport. A crash/uncertain
            # send cannot leave an older confirmed state eligible for omission.
            atomic_json(path, {"format": _FORMAT, "status": "pending", "token": token,
                               "binding": binding, "projection": "native-files",
                               "reason": packet.get("instruction_guidance", {}).get("reason")})
        return PreparedPrompt(worker_id, thread_id, task["id"], epoch, token,
                              snapshot_json, text, mode, reason)

    def compaction_pending(self, worker_id: str) -> bool:
        """Read the existing cache marker; no reading receipt or new ledger."""
        record = self._read(self._path(worker_id))
        return record is not None and record.get("reason") == "context_compacted"

    def observed(self, worker_id: str, task: dict) -> None:
        """A tool result showed the conversation these task fields: they become its baseline,
        so the next delta reports any later change, a reversal included."""
        with self._locked(worker_id) as path:
            record = self._read(path)
            if record is None or record.get("status") != "confirmed":
                return
            snapshot = record["snapshot"]
            current = snapshot.get("task")
            if not isinstance(current, dict) or current.get("id") != task.get("id"):
                return
            snapshot["task"] = {**current, **{key: deepcopy(task[key]) for key in ("revision", "title", "state")
                                              if key in task}}
            atomic_json(path, {**record, "snapshot": snapshot, "snapshot_sha256": _sha(canonical(snapshot))})

    def invalidate(self, worker_id: str, *, reason: str | None = None) -> None:
        with self._locked(worker_id) as path:
            atomic_json(path, {"format": _FORMAT, "status": "invalidated", "token": uuid4().hex,
                               "reason": reason})

    def confirm(self, prepared: PreparedPrompt, *, current_worker: dict) -> bool:
        current = {"worker_id": current_worker.get("id"), "thread_id": current_worker.get("thread_id"),
                   "task_id": current_worker.get("current_task_id"),
                   "context_epoch": current_worker.get("context_epoch")}
        with self._locked(prepared.worker_id) as path:
            record = self._read(path)
            same = (record is not None and record.get("token") == prepared.token
                    and record.get("binding") == prepared.binding and current == prepared.binding)
            if same and record is not None and record.get("status") == "confirmed":
                return True  # The same receipt may be delivered twice.
            if not same or record is None or record.get("status") != "pending":
                # A stale acknowledgement must not restore an earlier snapshot,
                # even if a later full tool result is already known to be sent.
                atomic_json(path, {"format": _FORMAT, "status": "invalidated", "token": uuid4().hex})
                return False
            atomic_json(path, {"format": _FORMAT, "status": "confirmed", "token": prepared.token,
                               "binding": prepared.binding, "projection": "native-files",
                               "snapshot": json.loads(prepared.snapshot_json),
                               "snapshot_sha256": _sha(prepared.snapshot_json)})
            return True
