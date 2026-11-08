"""Whether a conversation that replaced another by rotation kept the old one's working memory.

The measures come from TOKEN-PLAN §9.7. For every completed rotation, the worker's own
transcript, the Lean service's development checks and the review receipts are compared before
and after the switch:

- how many actions the new conversation took before its first work (editing, checking,
  submitting, or reading the task's own material), and how many of those only oriented it
  (operation schema lookups, guide reads, listings, Git and environment probes);
- the share of files the new conversation touched in its first actions that the old one had
  touched in its last;
- operation error codes the old conversation hit that the new one hit again;
- Lean error messages the worker's development checks reported both before and after;
- the share of failed checks, and of reviews returned (not approved), before and after.

Before a switch means since the worker's previous switch; after it, until its next one. The
baselines are Test 2's 555 compactions measured the same way from its transcripts (370 for
workers, 185 for reviewers). TOKEN-PLAN's own figures (4 and 6 requests, 25% and 46% of files)
were counted per model request in Codex's session files, so they are not comparable.
"""
from __future__ import annotations

import json
import re
import sqlite3
import statistics
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from ..lean_service.store import unpack
from .transcript_digest import events

WINDOW = 25  # Actions compared on each side of a switch, as in Test 2's analysis.
BASELINES: dict[str, dict[str, float]] = {"actions_before_first_work": {"ordinary": 2, "review": 2},
                                           "orientation_before_first_work": {"ordinary": 2, "review": 2},
                                           "reopened_share": {"ordinary": 0.265, "review": 0.334}}
_ORIENTING_OPERATIONS = {"workspace_list", "resource_list"}
_MATERIAL_OPERATIONS = {"workspace_read", "resource_read", "resource_search", "resource_pdf", "public_read"}
_OTHER_OPERATIONS = {"sync", "inspect", "fetch", "search", "talk_read", "dag_status", "history_search",
                     "lean_registry_list", "message_send", "talk_send", "request", "request_resolve",
                     "checkpoint", "pause", "resume_work", "subscribe", "literature_register"}
_ARGUMENT_PATHS = ("path", "source_path", "directory", "paths", "entry_files", "files")
_GUIDES = re.compile(r"\.private/+(?:instructions|taskhub-reference)")
_GIT = re.compile(r"(?:^|[\s;&|'\"(])git\s")
_LISTING = re.compile(r"(?:^|[\s;&|'\"(])(?:ls|dir|tree)(?:[\s'\");]|$)|rg\s+--files")
_PROBE = re.compile(r"(?:^|[\s;&|'\"(])(?:which|whoami|hostname|uname|command\s+-v)\b")
_FILE = re.compile(r"[\w.\-]+(?:/[\w.\-]+)*\.[A-Za-z]\w{0,7}")


def _normal(path: str) -> str:
    return path.removeprefix("./").strip("/")


def actions(path: Path, thread_id: str) -> list[dict[str, Any]]:
    """The thread's tool calls in order: each call's first record, with its final status and result."""
    merged: dict[str, dict[str, Any]] = {}
    for event in events(path):
        if event.get("type") != "action" or event.get("thread_id") != thread_id:
            continue
        key = str(event.get("action_id") or event.get("id"))
        if key not in merged:
            merged[key] = dict(event)
        else:
            merged[key].update({name: event[name] for name in ("status", "result", "error_type", "details")
                                if event.get(name)})
    return list(merged.values())


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def touched(action: dict[str, Any]) -> list[str]:
    """The files a tool call names: edited, read, or used in a command."""
    details, arguments = _mapping(action.get("details")), _mapping(action.get("arguments"))
    if action.get("tool") == "fileChange":
        found = [item for item in details.get("paths", []) if isinstance(item, str)]
    elif action.get("tool") == "commandExecution":
        found = _FILE.findall(str(details.get("command", "")))
    else:
        nested = _mapping(arguments.get("arguments")) or arguments
        found = []
        for key in _ARGUMENT_PATHS:
            value = nested.get(key)
            found += [item for item in (value if isinstance(value, list) else [value]) if isinstance(item, str)]
    return [_normal(item) for item in found if item.strip()]


def classify(action: dict[str, Any]) -> str:
    """`work`, `orientation` (schemas, guides, listings, Git, probes) or `other` (TaskHub reads
    and messages)."""
    tool = action.get("tool")
    operation = action.get("operation") or _mapping(action.get("arguments")).get("operation")
    if tool == "fileChange":
        return "work"
    if tool == "taskhub_actions":
        return "orientation"
    if tool == "taskhub_call":
        if operation in _ORIENTING_OPERATIONS:
            return "orientation"
        if operation in _MATERIAL_OPERATIONS:
            paths = touched(action)
            return "orientation" if paths and all(_GUIDES.search(item) for item in paths) else "work"
        return "other" if operation in _OTHER_OPERATIONS else "work"  # Checks, submissions, verdicts, writes.
    if tool == "commandExecution":
        command = str(_mapping(action.get("details")).get("command", ""))
        if _GUIDES.search(command) or _GIT.search(command) or _PROBE.search(command):
            return "orientation"
        files = touched(action)
        if not files:
            return "orientation" if _LISTING.search(command) else "other"
        return "work"
    return "work" if tool in {"imageView", "view_image"} else "other"


def first_work(calls: list[dict[str, Any]]) -> tuple[int | None, int | None]:
    """How many calls, and how many orienting ones, came before the first work."""
    orienting = 0
    for index, call in enumerate(calls):
        kind = classify(call)
        if kind == "work":
            return index, orienting
        orienting += kind == "orientation"
    return None, None


def _error_codes(calls: Iterable[dict[str, Any]]) -> set[str]:
    return {call["result"]["code"] for call in calls if call.get("status") == "error"
            and isinstance(_mapping(call.get("result")).get("code"), str)}


def _compile_errors(result: dict[str, Any]) -> set[str]:
    messages = _mapping(_mapping(result.get("compilation")).get("diagnostics")).get("messages")
    if not isinstance(messages, list):
        return set()
    return {" ".join(str(item.get("data", "")).split())[:300] for item in messages
            if isinstance(item, dict) and item.get("severity") == "error" and item.get("data")}


def checks(store: Path | None, owner: str, start: float, end: float) -> dict[str, Any]:
    """The owner's finished development checks submitted in [start, end)."""
    found: dict[str, Any] = {"count": 0, "failed": 0, "errors": set()}
    if store is None or not store.is_file():
        return found
    connection = sqlite3.connect(f"file:{store.as_posix()}?mode=ro", uri=True)
    try:
        rows = connection.execute("SELECT outcome,result_json FROM jobs WHERE owner=? AND lane='interactive' "
                                  "AND state='done' AND submitted_at>=? AND submitted_at<?",
                                  (owner, start, end)).fetchall()
    except sqlite3.DatabaseError:
        rows = []
    finally:
        connection.close()
    for outcome, body in rows:
        text = unpack(body)
        result = json.loads(text) if text else {}
        found["count"] += 1
        found["failed"] += (outcome != "completed" or "error" in result
                            or _mapping(result.get("compilation")).get("ok") is not True)
        found["errors"] |= _compile_errors(result)
    return found


def _share(part: int, whole: int) -> float | None:
    return round(part / whole, 3) if whole else None


def rotation_record(rotation: dict[str, Any], transcript: Path, store: Path | None,
                    reviews: list[dict[str, Any]], *, start: float, end: float) -> dict[str, Any]:
    """The measures for one completed rotation; `start` and `end` bound the worker's time before and
    after this switch (its previous and next switches)."""
    before, after = actions(transcript, rotation["thread_id"]), actions(transcript, rotation["new_thread_id"])
    switch = rotation["completed_at"] / 1000
    index, orienting = first_work(after)
    old_files = {item for call in before[-WINDOW:] for item in touched(call)}
    new_files = {item for call in after[:WINDOW] for item in touched(call)}
    old_checks, new_checks = checks(store, rotation["worker_id"], start, switch), checks(
        store, rotation["worker_id"], switch, end)
    old_reviews = [item for item in reviews if start <= item["submitted_at"] / 1000 < switch]
    new_reviews = [item for item in reviews if switch <= item["submitted_at"] / 1000 < end]
    return {
        "rotation_id": rotation["id"], "worker_id": rotation["worker_id"],
        "worker_kind": rotation.get("worker_kind") or "ordinary", "mode": rotation["mode"],
        "reason": rotation["reason"], "state_file_only": bool(rotation.get("automatic")),
        "actions_before_first_work": index, "orientation_before_first_work": orienting,
        "reopened": {"files": len(new_files), "touched_before": len(new_files & old_files),
                     "share": _share(len(new_files & old_files), len(new_files))},
        "repeated_error_codes": sorted(_error_codes(before) & _error_codes(after)),
        "checks": {"before": {"count": old_checks["count"], "failed": old_checks["failed"]},
                   "after": {"count": new_checks["count"], "failed": new_checks["failed"]}},
        "repeated_compile_errors": sorted(old_checks["errors"] & new_checks["errors"])[:5],
        "reviews": {side: {"verdicts": len(items), "returned": sum(item["verdict"] != "approve" for item in items)}
                    for side, items in (("before", old_reviews), ("after", new_reviews))},
    }


def summary(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Per rotation mode and worker kind: the medians and shares to compare with the baselines."""
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in records:
        groups.setdefault((record["mode"], record["worker_kind"]), []).append(record)
    result = []
    for (mode, kind), items in sorted(groups.items()):
        acting = [item["actions_before_first_work"] for item in items if item["actions_before_first_work"] is not None]
        orienting = [item["orientation_before_first_work"] for item in items
                     if item["orientation_before_first_work"] is not None]
        reopened = [item["reopened"]["share"] for item in items if item["reopened"]["share"] is not None]

        def share(section: str, side: str, part: str, whole: str) -> float | None:
            return _share(sum(item[section][side][part] for item in items),
                          sum(item[section][side][whole] for item in items))
        result.append({
            "mode": mode, "worker_kind": kind, "rotations": len(items),
            "actions_before_first_work": {"median": statistics.median(acting) if acting else None,
                                          "baseline": BASELINES["actions_before_first_work"].get(kind)},
            "orientation_before_first_work": {
                "median": statistics.median(orienting) if orienting else None,
                "baseline": BASELINES["orientation_before_first_work"].get(kind)},
            "reopened_share": {"mean": round(statistics.fmean(reopened), 3) if reopened else None,
                               "baseline": BASELINES["reopened_share"].get(kind)},
            "with_repeated_error_codes": sum(bool(item["repeated_error_codes"]) for item in items),
            "with_repeated_compile_errors": sum(bool(item["repeated_compile_errors"]) for item in items),
            "check_failure_share": {side: share("checks", side, "failed", "count") for side in ("before", "after")},
            "review_returned_share": {side: share("reviews", side, "returned", "verdicts")
                                      for side in ("before", "after")},
        })
    return result


def memory_report(rotations: list[dict[str, Any]], reviews: list[dict[str, Any]],
                  transcript: Callable[[str], Path], store: Path | None, *, now: float) -> dict[str, Any]:
    """Every completed rotation's measures (`rotations` ordered by completion), and their summary by
    rotation mode and worker kind."""
    switches: dict[str, list[float]] = {}
    for rotation in rotations:
        switches.setdefault(rotation["worker_id"], []).append(rotation["completed_at"] / 1000)
    records = []
    for rotation in rotations:
        own = switches[rotation["worker_id"]]
        position = own.index(rotation["completed_at"] / 1000)
        records.append(rotation_record(
            rotation, transcript(rotation["worker_id"]), store,
            [item for item in reviews if item["producer"] == rotation["worker_id"]],
            start=own[position - 1] if position else 0.0,
            end=own[position + 1] if position + 1 < len(own) else now))
    return {"window": WINDOW, "baselines": BASELINES, "summary": summary(records), "rotations": records}
