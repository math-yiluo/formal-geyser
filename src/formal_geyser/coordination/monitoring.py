"""Read-only operational snapshots and deterministic watchdog incidents.

This module never opens a Hub, takes ownership, mutates project state, drives a
run, retries a model request, recovers a worker, or validates an artifact.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import time
from collections import Counter
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from .config import codex_profiles_equal, load_config
from .content import NativeContentStore
from .local_operator import bearer, loopback_url
from .models import Limits
from .runtime.diagnostics import safe_message
from .schema import LATEST_VERSION_SQL
from .storage import atomic_write

BUSY_WORKERS = {"preparing", "running", "draining", "recovery_required"}
SEVERITIES = {"info": 0, "warning": 1, "error": 2, "critical": 3}
TELEMETRY_GAPS = [
    "Raw tool arguments, tool output, and native shell output are intentionally absent from telemetry.",
    "Worker process liveness is unknown unless an explicit worker PID is supplied.",
]


@dataclass(frozen=True)
class MonitorPolicy:
    stalled_seconds: float = 300
    validation_seconds: float = 300
    idle_grace_seconds: float = 3
    stage_seconds: float = 120

    def __post_init__(self):
        for value in (
            self.stalled_seconds,
            self.validation_seconds, self.idle_grace_seconds, self.stage_seconds,
        ):
            if not math.isfinite(value) or value < 0:
                raise ValueError("Monitor thresholds must be finite and nonnegative")


def process_identity(pid: int) -> dict[str, Any]:
    """Read PID liveness and identity without signaling or controlling a process."""
    if type(pid) is not int or pid <= 0:
        raise ValueError("A process ID must be a positive integer")
    result: dict[str, Any] = {"alive": None, "started_at_ms": None, "executable": None}
    try:
        os.kill(pid, 0)
        result["alive"] = True
    except ProcessLookupError:
        result["alive"] = False
        return result
    except PermissionError:
        return result
    proc = Path("/proc") / str(pid)
    try:
        stat = (proc / "stat").read_text()
        start_ticks = int(stat[stat.rfind(")") + 2:].split()[19])
        boot = next(int(line.split()[1]) for line in Path("/proc/stat").read_text().splitlines()
                    if line.startswith("btime "))
        result["started_at_ms"] = int(boot * 1000 + start_ticks * 1000 / os.sysconf("SC_CLK_TCK"))
        result["executable"] = str((proc / "exe").resolve(strict=True))
    except (OSError, ValueError, IndexError, StopIteration):
        pass
    return result


def _rows(connection: sqlite3.Connection, sql: str, values=()) -> list[dict]:
    return [dict(row) for row in connection.execute(sql, values)]


def _collaboration_rows(connection: sqlite3.Connection) -> dict[str, dict]:
    return {row["task_id"]: dict(row) for row in connection.execute("SELECT * FROM task_collaboration")}


def _requirement_holds(connection: sqlite3.Connection, requirement: dict) -> bool:
    completion = ""
    if requirement.get("artifact_id"):
        # The artifact's newest version, as the hub reads it.
        where, arguments = "a.id=(" + LATEST_VERSION_SQL + ")", [requirement["artifact_id"]]
    else:
        where, arguments = "a.task_id=? AND a.task_revision=?", [
            requirement.get("task_id"), requirement.get("task_revision")
        ]
        completion = (
            " AND EXISTS(SELECT 1 FROM candidate_bindings cb WHERE cb.artifact_id=a.id AND cb.completion=1)"
            " AND NOT EXISTS(SELECT 1 FROM attempts active WHERE active.task_id=a.task_id "
            "AND active.task_revision=a.task_revision AND active.ended_at IS NULL)"
        )
    return connection.execute(
        "SELECT 1 FROM artifacts a JOIN evidence e ON e.artifact_id=a.id WHERE "
        + where + completion + " AND a.status='accepted' AND e.verdict='accepted' "
        "AND e.qualifications_json='[]' AND e.sequence=(SELECT MAX(e2.sequence) "
        "FROM evidence e2 WHERE e2.artifact_id=a.id AND e2.policy=e.policy) LIMIT 1",
        arguments,
    ).fetchone() is not None


def _blocked_requirement(connection: sqlite3.Connection, requirement: dict) -> dict | None:
    if _requirement_holds(connection, requirement):
        return None
    reason = {key: requirement.get(key) for key in ("key", "task_id", "task_revision", "artifact_id")}
    reason["code"] = "acceptance_missing"
    if requirement.get("artifact_id") is not None:
        row = connection.execute(
            "SELECT status FROM artifacts WHERE id=?", (requirement["artifact_id"],)
        ).fetchone()
        reason["provider_state"] = row["status"] if row else "missing"
        if row is None:
            reason["code"] = "provider_artifact_missing"
        elif row["status"] == "withdrawn":
            reason["code"] = "provider_artifact_withdrawn"
    else:
        row = connection.execute(
            "SELECT state,current_revision FROM tasks WHERE id=?", (requirement.get("task_id"),)
        ).fetchone()
        reason["provider_state"] = row["state"] if row else "missing"
        reason["provider_current_revision"] = row["current_revision"] if row else None
        if row is None:
            reason["code"] = "provider_task_missing"
        elif connection.execute(
            "SELECT 1 FROM task_revisions WHERE task_id=? AND revision=?",
            (requirement.get("task_id"), requirement.get("task_revision")),
        ).fetchone() is None:
            reason["code"] = "provider_revision_missing"
        elif row["current_revision"] != requirement.get("task_revision"):
            reason["code"] = "historical_provider_receipt_missing"
        elif row["state"] == "retired":
            reason["code"] = "provider_retired"
        else:
            reason["code"] = "provider_receipt_pending"
    return reason


def _dependency_cycles(edges: dict[str, set[str]]) -> list[list[str]]:
    """Report deterministic DFS cycles without recursive traversal or claiming unresolvability."""
    color: dict[str, int] = {}
    cycles: set[tuple[str, ...]] = set()
    for first in sorted(edges):
        if color.get(first):
            continue
        path = [first]
        positions = {first: 0}
        color[first] = 1
        stack = [(first, iter(sorted(edges.get(first, set()))))]
        while stack:
            node, neighbors = stack[-1]
            next_node = next(neighbors, None)
            if next_node is None:
                color[node] = 2
                stack.pop()
                positions.pop(node, None)
                path.pop()
            elif color.get(next_node) == 1:
                cycles.add(tuple(sorted(path[positions[next_node]:])))
            elif not color.get(next_node):
                color[next_node] = 1
                positions[next_node] = len(path)
                path.append(next_node)
                stack.append((next_node, iter(sorted(edges.get(next_node, set())))))
    return [list(cycle) for cycle in sorted(cycles)[:20]]


def _contract_body(connection: sqlite3.Connection, project: Path, task_id: str,
                   revision: int, cached: str) -> dict:
    native = connection.execute("SELECT contract_sha256 FROM native_task_files WHERE task_id=? AND revision=?",
                                (task_id, revision)).fetchone()
    if native:
        return NativeContentStore(project / ".task-based").read_task(task_id, revision, native["contract_sha256"])
    return json.loads(cached)  # Its files are written just after the revision commits.


def _read_interruptions(connection: sqlite3.Connection, project: Path) -> dict:
    """Check inexpensive resume prerequisites; never hash a workspace or read proof bodies."""
    total = connection.execute(
        "SELECT COUNT(*) FROM interruption_checkpoints WHERE state='parked'"
    ).fetchone()[0]
    try:
        profile = load_config(project).codex.model_dump(mode="json")
    except (OSError, ValueError):
        profile = None
    tickets = []
    rows = _rows(connection,
        "SELECT i.id,i.task_id,i.task_revision,i.worker_id,i.origin_attempt,i.thread_id,"
        "i.usage_epoch,i.workspace,i.codex_profile_json,i.snapshot,i.input_refs_json,"
        "i.stop_reason,i.created_at,i.state,w.state AS worker_state,w.active_attempt AS worker_active,"
        "w.thread_id AS current_thread,w.usage_epoch AS current_epoch,w.profile_json,"
        "t.current_revision,t.state AS task_state,t.active_attempt AS task_active,"
        "a.run_id AS source_run_id,a.input_refs_json AS original_refs,a.continuation_id,"
        "EXISTS(SELECT 1 FROM worker_usage_holds u WHERE u.worker_id=i.worker_id) AS source_usage_uncertain "
        "FROM interruption_checkpoints i "
        "LEFT JOIN workers w ON w.id=i.worker_id LEFT JOIN tasks t ON t.id=i.task_id "
        "LEFT JOIN attempts a ON a.id=i.origin_attempt "
        "WHERE i.state='parked' ORDER BY i.created_at,i.id LIMIT 1000")
    for row in rows:
        reasons = []
        if row["current_revision"] != row["task_revision"] or row["task_state"] == "retired":
            reasons.append("task_revision_unavailable")
        if row["worker_active"] or row["worker_state"] != "suspended":
            reasons.append("worker_or_task_unavailable")
        if row["current_thread"] != row["thread_id"] or row["current_epoch"] != row["usage_epoch"]:
            reasons.append("session_identity_changed")
        saved_profile = json.loads(row["codex_profile_json"])
        worker_profile = json.loads(row["profile_json"] or "{}").get("codex_profile")
        if profile is None:
            reasons.append("configured_profile_unavailable")
        elif not codex_profiles_equal(profile, saved_profile) or not codex_profiles_equal(worker_profile, saved_profile):
            reasons.append("profile_changed")
        if row["source_run_id"] is None:
            reasons.append("source_run_unavailable")
        refs = json.loads(row["input_refs_json"])
        original_refs = json.loads(row["original_refs"] or "[]")
        if refs != sorted(set(original_refs)):
            reasons.append("input_binding_changed")
        artifacts = {}
        for ref in refs:
            item = connection.execute(
                "SELECT id,status,task_id,task_revision FROM artifacts WHERE id=?", (ref,)
            ).fetchone()
            if item is None:
                reasons.append("input_missing")
            else:
                artifacts[ref] = dict(item)
                if item["status"] == "withdrawn":
                    reasons.append("input_withdrawn")

        def bound(requirement):
            for ref, artifact in artifacts.items():
                matches = (
                    ref == connection.execute(LATEST_VERSION_SQL, (requirement["artifact_id"],)).fetchone()[0]
                    if requirement.get("artifact_id")
                    else artifact["task_id"] == requirement.get("task_id")
                    and artifact["task_revision"] == requirement.get("task_revision")
                )
                if matches and _requirement_holds(connection, {"artifact_id": ref}):
                    return True
            return False

        contract_row = connection.execute(
            "SELECT contract_json FROM task_revisions WHERE task_id=? AND revision=?",
            (row["task_id"], row["task_revision"]),
        ).fetchone()
        if contract_row:
            contract = _contract_body(connection, project, row["task_id"], row["task_revision"], contract_row["contract_json"])
            if not set(contract["inputs"]).issubset(refs):
                reasons.append("input_binding_changed")
            if not all(bound(req) for req in contract["requirements"]):
                reasons.append("original_requirement_evidence_changed")
        else:
            reasons.append("task_revision_unavailable")
        if row["continuation_id"]:
            prior = connection.execute(
                "SELECT predicate_json FROM continuations WHERE id=?", (row["continuation_id"],)
            ).fetchone()
            if prior:
                predicate = json.loads(prior["predicate_json"])
                # A wait on subscriptions, validations or an answer has no requirement to re-check.
                checks = [bound(req) for req in predicate.get("requirements", [])]
                if checks and not (all(checks) if predicate.get("mode", "all") == "all" else any(checks)):
                    reasons.append("original_wait_evidence_changed")
            else:
                reasons.append("original_wait_unavailable")
        snapshot = row["snapshot"]
        if not isinstance(snapshot, str) or re.fullmatch(r"[0-9a-f]{64}", snapshot) is None:
            reasons.append("snapshot_reference_invalid")
        elif not (project / ".task-based" / "objects" / "sha256" / snapshot[:2] / snapshot).is_file():
            reasons.append("snapshot_reference_missing")
        if re.fullmatch(r"[A-Za-z0-9_-]+", row["worker_id"] or ""):
            workspace = project / ".task-based" / "workers" / row["worker_id"] / "work"
            if str(workspace.resolve()) != row["workspace"]:
                reasons.append("workspace_identity_changed")
            elif not workspace.is_dir():
                reasons.append("workspace_unavailable")
        else:
            reasons.append("worker_identity_invalid")
        tickets.append({
            **{key: row[key] for key in
               ("id", "task_id", "task_revision", "worker_id", "origin_attempt",
                "source_run_id", "state", "stop_reason", "created_at", "snapshot")},
            "readiness": "blocked" if reasons else "candidate",
            "blocked_reasons": sorted(set(reasons)),
            "accounting_uncertain": bool(row["source_usage_uncertain"]),
            "snapshot_content_check": "deferred_to_runtime",
            "profile_source": "project.toml",
        })
    return {"available": True, "parked_count": total, "truncated": total > len(tickets),
            "tickets": tickets}


def _read_helpers(connection, run_id):
    result = []
    for row in _rows(connection, "SELECT * FROM helper_intents WHERE state IN ('prepared','running','uncertain') "
                     "AND (? IS NULL OR run_id=?) ORDER BY created_at,id", (run_id, run_id)):
        helper = {key: row.get(key) for key in (
            "id", "kind", "source_worker_id", "source_task_id", "run_id", "state", "thread_id",
            "remote_turn_id", "created_at", "ended_at", "inference_uncertain")}
        helper["reason"] = safe_message(row["reason"]) if row.get("reason") else None
        helper["has_retained_result"] = row.get("raw_result_json") is not None
        usage = connection.execute("SELECT COALESCE(SUM(delta_tokens),0) AS tokens_used,COUNT(*) AS samples,"
                                   "MAX(created_at) AS last_at FROM helper_usage_samples WHERE helper_id=?",
                                   (row["id"],)).fetchone()
        helper["usage"] = dict(usage)
        result.append(helper)
    return result


def read_database(project: Path, run_id: str | None = None) -> dict[str, Any]:
    """Read one consistent SQLite snapshot without running schema or recovery code."""
    path = project.resolve() / ".task-based" / "state.sqlite3"
    if not path.is_file():
        return {"available": False, "error": "database_missing"}
    try:
        uri = path.as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=1)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            versions = [row[0] for row in connection.execute("SELECT version FROM schema_info")]
            if versions != [1]:
                return {"available": False, "error": "unsupported_schema", "versions": versions}
            selected = _select_run(connection, run_id)
            selected_id = selected["id"] if selected else run_id
            dependency_edges: dict[str, set[str]] = {}
            tasks = _read_tasks(connection, project, dependency_edges)
            attempts = _rows(connection,
                "SELECT id,task_id,task_revision,worker_id,run_id,state,created_at,"
                "ended_at,checkpoint_json IS NOT NULL AS has_checkpoint,"
                "submission_json IS NOT NULL AS has_submission "
                "FROM attempts WHERE ended_at IS NULL AND (? IS NULL OR run_id=?) ORDER BY id",
                (selected_id, selected_id))
            workers = _read_workers(connection, selected_id)
            turns = _rows(connection,
                "SELECT * FROM turn_intents WHERE state IN ('prepared','sending','bound','uncertain') "
                "AND (? IS NULL OR run_id=?) ORDER BY created_at,id", (selected_id, selected_id))
            continuations = _read_continuations(connection, tasks, dependency_edges)
            events = _rows(connection,
                "SELECT sequence,kind,object_id,task_id,worker_id,created_at "
                "FROM events ORDER BY sequence DESC LIMIT 30")
            return {
                "available": True, "schema_version": 1, "run": selected,
                "requested_run_missing": run_id is not None and selected is None,
                "tasks": tasks, "workers": workers, "attempts": attempts,
                "turns": turns, "continuations": continuations,
                "helpers": _read_helpers(connection, selected_id),
                "dependency_cycles": _dependency_cycles(dependency_edges),
                "interruptions": _read_interruptions(connection, project.resolve()),
                "events": list(reversed(events)),
            }
    except (sqlite3.Error, OSError, ValueError, KeyError) as error:
        return {"available": False, "error": "database_read_failed",
                "error_type": type(error).__name__}


def _select_run(connection: sqlite3.Connection, run_id: str | None) -> dict | None:
    """The requested run, or else the running, the paused or the newest one, with its limits and
    each backend's dispatch hold."""
    runs = _rows(connection, "SELECT * FROM runs ORDER BY created_at DESC,id DESC")
    selected = next((row for row in runs if row["id"] == run_id), None) if run_id else (
        next((row for row in runs if row["state"] == "running"), None)
        or next((row for row in runs if row["state"] == "paused"), None)
        or (runs[0] if runs else None)
    )
    if selected:
        selected["limits"] = Limits.model_validate(
            json.loads(selected.pop("limits_json"))
        ).model_dump()
        # Codex and Claude bill separate accounts, so each backend has its own hold.
        by_backend = {}
        for backend, table in (("codex", "dispatch_hold"), ("claude", "claude_dispatch_hold")):
            rows = _rows(connection, f"SELECT code,message,created_at FROM {table}")
            if rows:
                by_backend[backend] = {**rows[0], "backend": backend}
        selected["dispatch_holds"] = by_backend
        selected["dispatch_hold"] = next(iter(by_backend.values()), None)
    return selected


def _blocked_reasons(connection: sqlite3.Connection, requirements: list) -> list[dict]:
    return [reason for item in requirements
            if (reason := _blocked_requirement(connection, item)) is not None]


def _dependency_targets(reasons: list[dict]) -> set[str]:
    """The tasks whose current revision the blocked requirements wait for."""
    return {reason["task_id"] for reason in reasons if reason.get("task_id")
            and reason.get("provider_current_revision") == reason.get("task_revision")}


def _read_tasks(connection: sqlite3.Connection, project: Path, dependency_edges: dict[str, set[str]]) -> list[dict]:
    """Every task at its current revision: what blocks it, whether it is actionable, its team and
    its latest validation. Adds the tasks a ready or waiting task depends on to `dependency_edges`."""
    tasks = []
    collaborative = _collaboration_rows(connection)
    for row in _rows(connection,
        "SELECT t.*,r.contract_json FROM tasks t JOIN task_revisions r "
        "ON r.task_id=t.id AND r.revision=t.current_revision ORDER BY t.id"
    ):
        contract = _contract_body(connection, project, row["id"], row["current_revision"], row.pop("contract_json"))
        row["acceptance_policy"] = contract["acceptance_policy"]
        requirements = _blocked_reasons(connection, contract.get("requirements", []))
        inputs = []
        for ref in contract.get("inputs", []):
            artifact = connection.execute(
                "SELECT status FROM artifacts WHERE id=?", (ref,)
            ).fetchone()
            if artifact is None or artifact["status"] == "withdrawn":
                inputs.append({
                    "code": "input_missing" if artifact is None else "input_withdrawn",
                    "artifact_id": ref,
                })
        row["blocked_reasons"] = [*inputs, *requirements]
        row["requirements_satisfied"] = not requirements
        row["inputs_current"] = not inputs
        if row["state"] in {"ready", "waiting"}:
            dependency_edges[row["id"]] = _dependency_targets(requirements)
        row["actionable"] = (
            row["state"] == "ready" and row["requirements_satisfied"]
            and row["inputs_current"] and row["active_attempt"] is None
        )
        row["active_attempts"] = [attempt["id"] for attempt in connection.execute(
            "SELECT id FROM attempts WHERE task_id=? AND ended_at IS NULL ORDER BY id",
            (row["id"],),
        )]
        if team := collaborative.get(row["id"]):
            members = _rows(connection,
                "SELECT m.worker_id,m.executor,m.runnable,m.work_reason,w.state,"
                "w.active_attempt FROM task_members m JOIN workers w ON w.id=m.worker_id "
                "WHERE m.task_id=? AND m.membership_state='member' ORDER BY m.joined_at,m.worker_id",
                (row["id"],))
            pending_reviews = _rows(connection,
                "SELECT task_id,candidate_id,state FROM review_requests "
                "WHERE host_task_id=? AND state IN ('pending','integrating') ORDER BY created_at,task_id",
                (row["id"],))
            opportunity = connection.execute(
                "SELECT opportunity_state,opportunity_opened_at FROM task_lifecycle WHERE task_id=?",
                (row["id"],)).fetchone()
            accepting_collaborator = bool(opportunity and opportunity["opportunity_state"] == "open")
            runnable = sum(bool(m["executor"] and m["runnable"] and m["state"] == "idle"
                                and m["active_attempt"] is None) for m in members)
            row["collaboration"] = {
                "host_task_id": team["host_task_id"], "governance": team["governance"],
                "member_count": len(members), "runnable_members": runnable,
                "members": members[:100], "members_truncated": len(members) > 100,
                "pending_reviews": pending_reviews[:100],
                "pending_review_count": len(pending_reviews),
                "collaborator_opportunity": dict(opportunity) if opportunity else None,
            }
            if team["host_task_id"] is None:
                initial = not any(m["executor"] for m in members)
                row["actionable"] = bool(
                    row["state"] not in {"done", "retired", "draining", "recovery_required", "needs_attention"}
                    and row["requirements_satisfied"] and row["inputs_current"]
                    and (initial or runnable or accepting_collaborator)
                )
        latest = connection.execute(
            "SELECT a.id,a.created_at,e.verdict,e.created_at AS reviewed_at "
            "FROM artifacts a LEFT JOIN evidence e ON e.artifact_id=a.id AND e.policy=? "
            "AND e.sequence=(SELECT MAX(e2.sequence) FROM evidence e2 "
            "WHERE e2.artifact_id=a.id AND e2.policy=e.policy) "
            "WHERE a.task_id=? AND a.task_revision=? AND a.status<>'withdrawn' "
            "ORDER BY a.created_at DESC,a.rowid DESC LIMIT 1",
            (contract["acceptance_policy"], row["id"], row["current_revision"]),
        ).fetchone()
        row["latest_validation"] = dict(latest) if latest else None
        tasks.append(row)
    return tasks


def _read_workers(connection: sqlite3.Connection, run_id: str | None) -> list[dict]:
    """The run's workers, each with its last activity and a key that changes with its progress."""
    workers = _rows(connection,
        "SELECT id,state,thread_id,active_attempt,run_id,created_at FROM workers "
        "WHERE (? IS NULL OR run_id=?) ORDER BY id", (run_id, run_id))
    for worker in workers:
        usage = connection.execute(
            "SELECT MAX(created_at) AS last_at,COALESCE(SUM(total),0) AS samples "
            "FROM usage_samples WHERE worker_id=?", (worker["id"],)
        ).fetchone()
        event = connection.execute(
            "SELECT MAX(sequence) AS sequence,MAX(created_at) AS last_at FROM events "
            "WHERE worker_id=?",
            (worker["id"],),
        ).fetchone()
        receipt = connection.execute(
            "SELECT MAX(rowid) AS sequence,COUNT(*) AS count FROM receipts "
            "WHERE principal=? AND command <>'usage.record'",
            ("worker:" + worker["id"],),
        ).fetchone()
        worker["last_activity_at"] = max(
            usage["last_at"] or 0, event["last_at"] or 0, worker["created_at"]
        )
        worker["progress_key"] = [
            event["sequence"] or 0, usage["samples"], receipt["sequence"] or 0
        ]
        worker["recorded_tool_commands"] = receipt["count"]
        worker["recent_commands"] = _rows(connection,
            "SELECT rowid AS sequence,command FROM receipts WHERE principal=? "
            "AND command <>'usage.record' ORDER BY rowid DESC LIMIT 8",
            ("worker:" + worker["id"],))
    return workers


def _read_continuations(connection: sqlite3.Connection, tasks: list[dict],
                        dependency_edges: dict[str, set[str]]) -> list[dict]:
    """Open waits and what blocks them, also recorded on their tasks."""
    continuations = _rows(connection,
        "SELECT id,task_id,task_revision,preferred_worker,state,wake_reason,created_at,predicate_json "
        "FROM continuations WHERE state IN ('waiting','eligible','reconsider') ORDER BY created_at,id")
    task_map = {task["id"]: task for task in tasks}
    for continuation in continuations:
        predicate = json.loads(continuation.pop("predicate_json"))
        reasons = _blocked_reasons(connection, predicate.get("requirements", []))
        continuation["wait_mode"] = predicate.get("mode", "all")
        if continuation["wait_mode"] == "any" and len(reasons) < len(predicate.get("requirements", [])):
            reasons = []
        continuation["blocked_reasons"] = reasons
        task = task_map.get(continuation["task_id"])
        if (task and task.get("collaboration")
                and task["current_revision"] == continuation["task_revision"]):
            task.setdefault("member_waits", []).append({
                "worker_id": continuation["preferred_worker"], "continuation_id": continuation["id"],
                "state": continuation["state"], "wait_mode": continuation["wait_mode"],
                "blocked_reasons": reasons,
            })
        if (task and task["current_revision"] == continuation["task_revision"]
                and not task.get("collaboration")):
            task["blocked_reasons"].extend(reasons)
            if continuation["wait_mode"] == "all" and task["state"] in {"ready", "waiting"}:
                dependency_edges.setdefault(task["id"], set()).update(_dependency_targets(reasons))
    return continuations


RUNTIME_EVENTS = frozenset({
    "activation_started", "session_ready", "turn_started", "model_activity",
    "tool_started", "tool_finished", "usage", "turn_finished", "activation_finished",
    "runtime_error",
})
RUNTIME_FIELDS = frozenset({
    "run_id", "worker_id", "task_id", "attempt_id", "thread_id", "turn_id", "tool",
    "status", "directive", "method", "error_type", "total_tokens", "process_quiet",
    "diagnostic_sha256", "stop_reason",
    "helper_id", "helper_kind", "source_worker_id",
})


class RuntimeTail:
    """Consume complete allowlisted lines with bounded reads and retained worker summaries."""

    def __init__(self, path: Path, max_bytes: int = 262144, max_workers: int = 128):
        if max_bytes < 256 or max_workers < 1:
            raise ValueError("Runtime tail limits must be positive and at least 256 bytes")
        self.path = path
        self.max_bytes = max_bytes
        self.max_workers = max_workers
        self.offset = 0
        self.identity: tuple[int, int] | None = None
        self.anchor = b""
        self.workers: dict[tuple[str, str], dict] = {}
        self.helpers: dict[tuple[str, str], dict] = {}
        self.recent: list[dict] = []
        self.history_incomplete = False

    @staticmethod
    def _record(line: bytes, now_ms: int) -> dict:
        if len(line) > 4096:
            raise ValueError("Runtime record is too long")
        raw = json.loads(line)
        if not isinstance(raw, dict) or raw.get("event") not in RUNTIME_EVENTS:
            raise ValueError("Unsupported runtime record")
        stamp = raw.get("timestamp")
        if not isinstance(stamp, str) or len(stamp) > 64:
            raise ValueError("Missing runtime timestamp")
        parsed = datetime.fromisoformat(stamp)
        if parsed.tzinfo is None:
            raise ValueError("Runtime timestamps require a timezone")
        timestamp = int(parsed.timestamp() * 1000)
        if timestamp > now_ms + 5000:
            raise ValueError("Runtime timestamp is ahead of the observer clock")
        record = {"event": raw["event"], "timestamp": stamp, "timestamp_ms": min(timestamp, now_ms)}
        for key in RUNTIME_FIELDS:
            value = raw.get(key)
            if type(value) in {int, bool} or value is None:
                if key in raw:
                    record[key] = value
            elif isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:/-]{1,160}", value):
                if key != "diagnostic_sha256" or re.fullmatch(r"[0-9a-f]{64}", value):
                    record[key] = value
        if (not isinstance(record.get("run_id"), str)
                or not isinstance(record.get("helper_id") or record.get("worker_id"), str)):
            raise ValueError("Runtime progress requires run and worker or helper identities")
        return record

    def _apply(self, record: dict) -> None:
        entities = self.helpers if record.get("helper_id") else self.workers
        key = (record["run_id"], record.get("helper_id") or record["worker_id"])
        previous = entities.get(key, {})
        if record["event"] == "activation_started" or (
            record.get("attempt_id") and previous.get("attempt_id")
            and record["attempt_id"] != previous["attempt_id"]
        ):
            previous = {}
        state = previous.copy()
        state.setdefault("active_tools", {})
        for name in ("run_id", "worker_id", "task_id", "attempt_id", "thread_id", "turn_id",
                     "helper_id", "helper_kind", "source_worker_id"):
            if record.get(name) is not None:
                state[name] = record[name]
        state["last_activity_at_ms"] = max(
            state.get("last_activity_at_ms", 0), record["timestamp_ms"]
        )
        state["last_event"] = record["event"]
        if record["event"] == "activation_started":
            state["activation_started_at_ms"] = record["timestamp_ms"]
        if record["event"] == "runtime_error":
            state["last_runtime_error"] = {
                name: record.get(name) for name in
                ("status", "error_type", "diagnostic_sha256", "timestamp_ms")
            }
        if record["event"] in {"turn_started", "model_activity", "usage"} or (
            record["event"] == "tool_finished" and record.get("status") == "ok"
        ):
            state["last_success_at_ms"] = record["timestamp_ms"]
        if record["event"] == "tool_started" and record.get("tool"):
            if record["tool"] not in state["active_tools"] and len(state["active_tools"]) >= 64:
                state["active_tools"].pop(next(iter(state["active_tools"])))
                state["tool_history_incomplete"] = True
                self.history_incomplete = True
            tool = state["active_tools"].setdefault(
                record["tool"], {"started_at_ms": record["timestamp_ms"], "count": 0}
            )
            tool["count"] += 1
        elif record["event"] == "tool_finished":
            tool = state["active_tools"].get(record.get("tool"))
            if tool:
                tool["count"] -= 1
                if tool["count"] <= 0:
                    state["active_tools"].pop(record["tool"], None)
            state["last_tool_result"] = {
                name: record.get(name) for name in
                ("tool", "status", "error_type", "directive", "timestamp_ms")
            }
        elif record["event"] == "activation_finished":
            state["active_tools"] = {}
            state["process_quiet_reported"] = record.get("process_quiet")
        entities.pop(key, None)
        entities[key] = state
        while len(entities) > self.max_workers:
            entities.pop(next(iter(entities)))
            self.history_incomplete = True
        self.recent.append(record)
        self.recent = self.recent[-30:]

    def read(self, run_id: str | None, now_ms: int) -> dict:
        try:
            with self.path.open("rb") as stream:
                stat = os.fstat(stream.fileno())
                identity = (stat.st_dev, stat.st_ino)
                reset = self.identity is not None and (
                    identity != self.identity or stat.st_size < self.offset
                )
                if self.anchor and not reset:
                    stream.seek(self.offset - len(self.anchor))
                    reset = stream.read(len(self.anchor)) != self.anchor
                if reset:
                    self.offset = 0
                    self.workers.clear()
                    self.helpers.clear()
                    self.recent.clear()
                    self.anchor = b""
                    self.history_incomplete = True
                self.identity = identity
                start = self.offset
                skipped = stat.st_size - start > self.max_bytes
                if skipped:
                    start = stat.st_size - self.max_bytes
                    self.workers.clear()
                    self.helpers.clear()
                    self.recent.clear()
                    self.history_incomplete = True
                stream.seek(start)
                body = stream.read(self.max_bytes)
                bytes_read = len(body)
                if skipped:
                    newline = body.find(b"\n")
                    if newline >= 0:
                        start += newline + 1
                        body = body[newline + 1:]
                    else:
                        start += len(body)
                        body = b""
                complete = body.rfind(b"\n") + 1
                invalid = 0
                for line in body[:complete].splitlines():
                    try:
                        self._apply(self._record(line, now_ms))
                    except (ValueError, TypeError, OverflowError, OSError):
                        invalid += 1
                self.offset = start + complete
                stream.seek(max(0, self.offset - 64))
                self.anchor = stream.read(min(64, self.offset))
                return {
                    "available": True, "offset": self.offset, "file_bytes": stat.st_size,
                    "bytes_read": bytes_read, "partial_bytes": len(body) - complete,
                    "reset_detected": reset, "tail_skipped": skipped,
                    "invalid_records": invalid, "history_incomplete": self.history_incomplete,
                    "workers": [state for (rid, _), state in self.workers.items()
                                if run_id is None or rid == run_id],
                    "helpers": [state for (rid, _), state in self.helpers.items()
                                if run_id is None or rid == run_id],
                    "recent_events": [record for record in self.recent
                                      if run_id is None or record["run_id"] == run_id],
                }
        except OSError as error:
            return {"available": False,
                    "error": "missing" if isinstance(error, FileNotFoundError) else "unreadable",
                    "workers": [], "helpers": [], "recent_events": []}


def _diagnostic_refs(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)
            and re.fullmatch(r"[0-9a-f]{64}", item)][-8:]


def _failure_evidence(value: dict) -> bool:
    return bool(
        value.get("error") or value.get("error_present") or value.get("diagnostic_refs")
        or value.get("diagnostic_sha256") or value.get("has_failure_evidence")
        or value.get("failure_observed")
        or value.get("turn_status") in {"failed", "unknown"}
        or value.get("state") in {"failed", "unknown"}
    )


def _driver_summary(value: Any) -> dict | None:
    if not isinstance(value, dict):
        return None
    summary: dict[str, Any] = {
        "state": value.get("state"), "stop_reason": value.get("stop_reason"),
        "error_present": bool(value.get("error")),
        "diagnostic_refs": _diagnostic_refs(value.get("diagnostic_refs")),
    }
    if value.get("run_state") is not None:
        summary["run_state"] = value["run_state"]
    error = value.get("error")
    if isinstance(error, dict):
        summary["error"] = {
            "code": error.get("code"),
            "message": str(error.get("message", ""))[:2000],
        }
    elif error:
        summary["error"] = {"message": str(error)[:2000]}
    result = value.get("result")
    if isinstance(result, dict):
        outcomes = result.get("results", [])
        outcomes = outcomes if isinstance(outcomes, list) else []
        summary["result"] = {
            key: result.get(key) for key in (
                "run_id", "state", "steps", "stop_reason", "results_truncated",
                "results_retained", "failure_observed",
            )
        }
        summary["result"]["diagnostic_refs"] = _diagnostic_refs(result.get("diagnostic_refs"))
        summary["result"]["has_failure_evidence"] = (
            _failure_evidence(value) or _failure_evidence(result)
            or any(_failure_evidence(row) for row in outcomes if isinstance(row, dict))
        )
        summary["result"]["recent_outcomes"] = [
            {
                **{key: row.get(key) for key in (
                    "task_id", "worker_id", "attempt_id", "directive", "turn_status",
                    "artifact_id", "stop_reason", "process_quiet",
                )},
                "error_present": bool(row.get("error")),
                "diagnostic_refs": _diagnostic_refs(row.get("diagnostic_refs")),
                "has_failure_evidence": _failure_evidence(row),
            }
            for row in outcomes[-8:] if isinstance(row, dict)
        ]
    return summary


class SnapshotReader:
    def __init__(
        self, project: Path, *, url: str | None = None, run_id: str | None = None,
        service_pid: int | None = None, worker_pids: dict[str, int] | None = None,
        timeout: float = 2, transport=None,
        service_start_ms: int | None = None, service_executable: str | None = None,
        worker_start_ms: dict[str, int] | None = None,
        probe: Callable[[int], Any] = process_identity,
    ):
        self.project = project.resolve()
        self.url = loopback_url(url) if url else None
        self.run_id = run_id
        self.service_pid = service_pid
        self.worker_pids = worker_pids or {}
        self.timeout = timeout
        self.service_start_ms = service_start_ms
        self.service_executable = service_executable
        self.worker_start_ms = worker_start_ms or {}
        self.transport = transport
        self.probe = probe
        self.runtime_tail = RuntimeTail(self.project / ".task-based" / "runtime-events.jsonl")

    def _process(self, pid: int, start_ms=None, executable=None) -> dict:
        value = self.probe(pid)
        result = {"pid": pid, **(value if isinstance(value, dict) else {"alive": value})}
        matches = []
        if start_ms is not None:
            result["expected_started_at_ms"] = start_ms
            actual = result.get("started_at_ms")
            matches.append(None if actual is None else actual == start_ms)
        if executable is not None:
            result["expected_executable"] = executable
            actual = result.get("executable")
            matches.append(None if actual is None else
                           os.path.abspath(actual) ==
                           os.path.abspath(executable))
        result["identity_matches"] = (
            False if False in matches else True if matches and None not in matches else None
        )
        return result

    def collect(self, now_ms: int | None = None) -> dict[str, Any]:
        database = read_database(self.project, self.run_id)
        service: dict[str, Any] = {"observed": self.url is not None, "reachable": None}
        run = database.get("run")
        if self.url:
            try:
                with httpx.Client(
                    base_url=self.url, timeout=self.timeout, trust_env=False,
                    follow_redirects=False, transport=self.transport,
                    headers=bearer(self.project, "taskhub"),
                ) as client:
                    response = client.get("/v1/status")
                    response.raise_for_status()
                    status = response.json()
                    service.update(reachable=True, armed_runs=status.get("armed_runs", []),
                                   running_drivers=status.get("running_drivers", []))
                    if run:
                        response = client.get("/v1/runs/" + run["id"])
                        response.raise_for_status()
                        detail = response.json()
                        service["run"] = {
                            key: detail.get(key)
                            for key in ("id", "state", "driving", "armed", "wake_pending")
                        }
                        service["run"]["driver_result"] = _driver_summary(
                            detail.get("driver_result")
                        )
            except httpx.HTTPStatusError as error:
                service.update(reachable=False, error="http_status",
                               status_code=error.response.status_code)
            except (httpx.HTTPError, OSError, ValueError, KeyError, TypeError) as error:
                service.update(reachable=False, error="probe_failed",
                               error_type=type(error).__name__)
        if self.service_pid is not None:
            service["pid"] = self.service_pid
            service["process"] = self._process(
                self.service_pid, self.service_start_ms, self.service_executable
            )
            service["process_alive"] = service["process"]["alive"]
        worker_processes = {
            worker_id: self._process(pid, self.worker_start_ms.get(worker_id))
            for worker_id, pid in sorted(self.worker_pids.items())
        }
        observed_at = now_ms if now_ms is not None else int(time.time() * 1000)
        runtime = self.runtime_tail.read((run or {}).get("id") or self.run_id, observed_at)
        attempts = {row["id"]: row for row in database.get("attempts", [])}
        activity = {row["worker_id"]: row for row in runtime["workers"]}
        for worker in database.get("workers", []):
            item = activity.get(worker["id"])
            attempt = attempts.get(worker.get("active_attempt"))
            if item and attempt and (
                item.get("attempt_id") == attempt["id"] or (
                    item.get("attempt_id") is None
                    and item.get("task_id") == attempt.get("task_id")
                    and item["last_activity_at_ms"] >= attempt["created_at"]
                )
            ):
                worker["runtime_activity"] = item
                worker["last_activity_at"] = max(
                    worker["last_activity_at"], item["last_activity_at_ms"]
                )
        helper_activity = {row["helper_id"]: row for row in runtime.get("helpers", [])}
        for helper in database.get("helpers", []):
            item = helper_activity.get(helper["id"])
            if item and (not helper["thread_id"] or item.get("thread_id") == helper["thread_id"]):
                helper["runtime_activity"] = item
        gaps = TELEMETRY_GAPS.copy()
        if not runtime["available"]:
            gaps.append("Runtime activity log is missing or unreadable; progress falls back to database metadata.")
        if runtime.get("history_incomplete"):
            gaps.append("Runtime activity history is partial because only a bounded tail is retained or the file changed.")
        if runtime.get("invalid_records"):
            gaps.append("Malformed or unsupported runtime records were ignored.")
        return {
            "observed_at_ms": observed_at,
            "database": database, "service": service, "worker_processes": worker_processes,
            "runtime_telemetry": runtime, "telemetry_gaps": gaps,
        }


# Incident codes by category; any other code asks the operator to act.
EXPECTED_HOLDS = frozenset({"run_paused", "operator_review_required", "dispatch_held"})
RUNTIME_FAILURES = frozenset({
    "service_unreachable", "service_process_exited", "service_process_replaced",
    "worker_process_exited", "worker_process_replaced", "turn_progress_stalled", "driver_needs_attention",
    "worker_stage_stalled", "runtime_error", "runtime_retrying", "helper_recovery_required", "helper_runtime_error",
})
# Incidents that put the project in the needs_attention phase.
ATTENTION_CODES = frozenset({
    "driver_needs_attention", "task_needs_attention", "worker_process_exited", "worker_process_replaced",
    "worker_stage_stalled", "runtime_error", "dependency_blocked", "dependency_cycle",
    "interruption_blocked", "interruption_resume_failed", "helper_recovery_required", "helper_runtime_error",
})
# Driver error codes that mean an interruption ticket could not resume.
RESUME_ERROR_CODES = frozenset({
    "SNAPSHOT_CHANGED", "PROFILE_CHANGED", "SESSION_CHANGED", "INPUTS_CHANGED",
    "INTERRUPTION_NOT_READY", "ALREADY_CLAIMED", "STALE_REVISION",
    "PREREQUISITES_MISSING", "USAGE_UNCERTAIN", "NOT_FOUND",
})


def incident_category(code: str) -> str:
    return ("expected_hold" if code in EXPECTED_HOLDS else "validation" if code.startswith("validation")
            else "runtime_failure" if code in RUNTIME_FAILURES else "operator_action")


class Incidents:
    """The incidents one observation finds, each under its code and subject."""

    def __init__(self) -> None:
        self.found: dict[str, dict] = {}

    def __call__(self, code: str, severity: str, subject: Any, message: str, action: str, **details: Any) -> None:
        key = f"{code}:{subject}"
        self.found[key] = {
            "incident_id": hashlib.sha256(key.encode()).hexdigest()[:20],
            "code": code, "severity": severity, "subject_id": subject, "category": incident_category(code),
            "message": message, "action": action, "details": details,
        }


class Watchdog:
    """Convert deterministic observations into deduplicated incident transitions."""

    def __init__(self, policy: MonitorPolicy | None = None):
        self.policy = policy or MonitorPolicy()
        self.active: dict[str, dict] = {}
        self.progress: dict[str, tuple[Any, int]] = {}
        self.idle_since: dict[str | None, int] = {}
        self.stage_since: dict[tuple[str, str, str], int] = {}

    def observe(self, observation: dict[str, Any]) -> list[dict]:
        now = observation["observed_at_ms"]
        database, service = observation["database"], observation["service"]
        run = database.get("run") or {}
        run_id = run.get("id")
        issue = Incidents()
        driver = (service.get("run") or {}).get("driver_result") or {}
        self._service_incidents(issue, database, service, run, driver)
        self._telemetry_incidents(issue, observation, database, run_id)
        workers = {row["id"]: row for row in database.get("workers", [])}
        attempts = {row["id"]: row for row in database.get("attempts", [])}
        self._worker_incidents(issue, observation, database, workers, attempts, now)
        self._turn_incidents(issue, database, workers, now)
        helpers = database.get("helpers", [])
        self._helper_incidents(issue, helpers)
        tasks = database.get("tasks", [])
        self._task_incidents(issue, tasks, now)
        work = self._work_incidents(issue, database, service, run, driver, tasks, now)
        found = issue.found
        self._keep_unobserved(found, database, service)
        transitions = self._transitions(found, run_id, now)
        counts = dict(sorted(Counter(row["state"] for row in tasks).items()))
        phase = self._phase(found, service, run, workers, counts, work)
        interruptions, tickets = work["interruptions"], work["tickets"]
        snapshot = {
            "schema_version": 1, "type": "snapshot", "observed_at_ms": now,
            "run_id": run_id, "read_only": True, "phase": phase,
            "service": service, "database_available": database.get("available", False),
            "run": run or None, "task_counts": counts, "actionable_tasks": work["actionable"],
            "eligible_continuations": work["eligible"],
            "interruption_candidates": work["resume_candidates"],
            "interruption_metadata_candidates": work["metadata_candidates"],
            "interruption_resume_error": work["resume_error"],
            "interruptions": {
                **{key: interruptions.get(key) for key in ("available", "parked_count", "truncated", "error")},
                "tickets": tickets[:100],
            },
            "blocked_tasks": work["blocked_tasks"][:100],
            "dependency_cycles": database.get("dependency_cycles", [])[:20],
            "workers": list(workers.values()),
            "helpers": helpers,
            "attempts": list(attempts.values()), "turns": database.get("turns", []),
            "worker_processes": observation.get("worker_processes", {}),
            "runtime_telemetry": observation.get("runtime_telemetry"),
            "recent_events": database.get("events", []),
            "telemetry_gaps": observation.get("telemetry_gaps", []),
            "active_incidents": [{key: item[key] for key in
                ("incident_id", "code", "severity", "subject_id", "since_ms")}
                for item in found.values()],
        }
        return [snapshot, *transitions]

    @staticmethod
    def _service_incidents(issue: Incidents, database: dict, service: dict, run: dict, driver: dict) -> None:
        """The project state, the service process, the run and its driver."""
        run_id = run.get("id")
        if not database.get("available"):
            issue("database_unavailable", "error", "project",
                  "Project state could not be read safely.",
                  "Check the project path, schema, and service logs without initializing another Hub.",
                  error=database.get("error"))
        if database.get("requested_run_missing"):
            issue("run_not_found", "error", "project", "The requested run was not found.",
                  "Verify the run ID and project path.")
        if service.get("reachable") is False:
            authentication = service.get("status_code") in {401, 403}
            issue("service_authentication_failed" if authentication else "service_unreachable",
                  "error", "service", "The service status endpoint is unavailable.",
                  "Check the service and its logs; do not retry unknown model calls.",
                  error=service.get("error"), status_code=service.get("status_code"))
        if service.get("process_alive") is False:
            issue("service_process_exited", "critical", "service",
                  "The supplied service PID is no longer running.",
                  "Inspect the exit log and old owned processes before explicit crash recovery.",
                  pid=service.get("pid"))
        if (service.get("process") or {}).get("identity_matches") is False:
            issue("service_process_replaced", "critical", "service",
                  "The supplied PID belongs to a different process identity.",
                  "Refresh the observation binding from verified launch evidence; do not stop that PID.",
                  process=service["process"])
        if run.get("usage_uncertain"):
            issue("usage_uncertain", "error", run_id, "Run totals include unresolved execution usage; unaffected workers may continue.",
                  "Reconcile the affected execution explicitly; never replay its unknown model call.")
        if run.get("state") == "paused":
            issue("run_paused", "info", run_id, "The run is paused.",
                  "Inspect any usage or recovery hold before deciding whether to resume.")
        for backend, hold in (run.get("dispatch_holds") or {}).items():
            account = "Claude subscription" if backend == "claude" else "Codex account"
            action = (f"Fix the {account}, then Resume; only {backend} workers wait, and in-flight turns "
                      "finish on their own.")
            label = "Claude" if backend == "claude" else "Codex"
            if hold.get("code") == f"{backend}ProfilesLimited":
                action = (f"{label} sessions start again by themselves at the first profile's reset; only {label} "
                          "sessions wait. Another signed-in profile can be selected by hand.")
            elif hold.get("code") == f"{backend}ProfilesUnavailable":
                action = (f"Tick a signed-in profile in the {label} tab, or sign in to one in the rotation; {label} "
                          f"sessions then start without Resume. Only {label} sessions wait.")
            issue("dispatch_held", "warning", run_id, hold["message"], action, error_code=hold["code"])
        driver_state = driver.get("state")
        result = driver.get("result") or {}
        outcome_state = result.get("state")
        if driver_state in {"failed", "retrying"} or outcome_state == "needs_attention" or result.get("has_failure_evidence"):
            issue("driver_needs_attention", "error", run_id,
                  "Coordination is retrying or an individual execution needs attention.",
                  "Inspect driver details and owned process state before any engineering intervention.",
                  driver_state=driver_state, outcome_state=outcome_state,
                  error=driver.get("error"),
                  diagnostic_refs=result.get("diagnostic_refs", []),
                  recent_outcomes=result.get("recent_outcomes", []))

    @staticmethod
    def _telemetry_incidents(issue: Incidents, observation: dict, database: dict, run_id: str | None) -> None:
        runtime = observation.get("runtime_telemetry")
        if runtime is not None and not runtime["available"] and database.get("turns"):
            issue("runtime_telemetry_unavailable", "warning", run_id,
                  "An active turn has no readable runtime activity log.",
                  "Inspect the runtime logger and service log; do not infer process death from missing telemetry.",
                  error=runtime.get("error"))
        if runtime and (runtime.get("invalid_records") or runtime.get("reset_detected")):
            issue("runtime_telemetry_degraded", "warning", run_id,
                  "Runtime activity history was reset or contained unreadable records.",
                  "Inspect logging continuity; use confirmed database and process evidence while history is partial.",
                  reset=runtime.get("reset_detected"), invalid_records=runtime.get("invalid_records"))

    def _worker_incidents(self, issue: Incidents, observation: dict, database: dict, workers: dict,
                          attempts: dict, now: int) -> None:
        """Quarantined workers, their processes, runtime errors, and lifecycle stages that stall."""
        live_worker_ids = {turn["worker_id"] for turn in database.get("turns", [])}
        observed_stages = set()
        for worker in workers.values():
            worker_id = worker["id"]
            if worker["state"] == "recovery_required":
                issue("worker_recovery_required", "critical", worker_id,
                      "Startup recovery has quarantined this worker.",
                      "Confirm the old process tree is stopped before explicit worker recovery.")
            process = observation.get("worker_processes", {}).get(worker_id, {})
            if worker["state"] in BUSY_WORKERS and process.get("alive") is False:
                issue("worker_process_exited", "critical", worker_id,
                      "A supplied worker PID exited while the worker still owns work.",
                      "Inspect the attempt and remaining process tree; preserve uncertain usage.",
                      pid=process.get("pid"))
            if process.get("identity_matches") is False:
                issue("worker_process_replaced", "critical", worker_id,
                      "The supplied worker PID no longer matches its expected start time.",
                      "Verify ownership from launch evidence; do not signal the replacement process.",
                      process=process)
            attempt = attempts.get(worker.get("active_attempt"))
            activity = worker.get("runtime_activity") or {}
            runtime_error = activity.get("last_runtime_error") or {}
            if runtime_error.get("timestamp_ms", 0) > activity.get("last_success_at_ms", 0):
                retrying = runtime_error.get("status") == "retrying"
                issue("runtime_retrying" if retrying else "runtime_error",
                      "warning" if retrying else "error", worker_id,
                      "The runtime reported a provider or tool execution error.",
                      "Inspect the immutable diagnostic reference; the monitor never retries model calls.",
                      error_type=runtime_error.get("error_type"),
                      diagnostic_sha256=runtime_error.get("diagnostic_sha256"),
                      status=runtime_error.get("status"))
            stage = "draining" if worker["state"] == "draining" else (
                "preparing" if worker["state"] in {"preparing", "running"}
                and worker_id not in live_worker_ids else None
            )
            if attempt and stage:
                stage_key = (worker_id, attempt["id"], stage)
                observed_stages.add(stage_key)
                marker_at = activity.get("activation_started_at_ms") if stage == "preparing" else None
                last_tool = activity.get("last_tool_result") or {}
                if stage == "draining" and str(last_tool.get("directive", "")).startswith("drain_"):
                    marker_at = last_tool.get("timestamp_ms")
                since = self.stage_since.setdefault(
                    stage_key, min(now, marker_at) if isinstance(marker_at, int) else now
                )
                if now - since >= self.policy.stage_seconds * 1000:
                    issue("worker_stage_stalled", "warning", worker_id,
                          "A worker still owns an execution slot without completing its lifecycle stage.",
                          "Inspect process drainage, workspace snapshotting, or startup.",
                          stage=stage, since_ms=since, attempt_id=attempt["id"],
                          threshold_seconds=self.policy.stage_seconds,
                          active_turn=worker_id in live_worker_ids)
        self.stage_since = {key: value for key, value in self.stage_since.items()
                            if key in observed_stages}

    def _turn_incidents(self, issue: Incidents, database: dict, workers: dict, now: int) -> None:
        """Turns whose outcome is unknown, and turns without progress."""
        live_ids = set()
        for turn in database.get("turns", []):
            live_ids.add(turn["id"])
            worker = workers.get(turn["worker_id"], {})
            key = worker.get("progress_key", [])
            progress_previous = self.progress.get(turn["id"])
            last = max(turn["created_at"], worker.get("last_activity_at", turn["created_at"]))
            if progress_previous:
                last = now if progress_previous[0] != key else max(last, progress_previous[1])
            self.progress[turn["id"]] = (key, last)
            if turn["state"] == "uncertain":
                issue("turn_outcome_unknown", "error", turn["id"],
                      "The outcome of an admitted model turn is unknown.",
                      "Inspect provider and runtime evidence; do not retry the turn automatically.")
                continue
            quiet = max(0, now - last) / 1000
            if quiet >= self.policy.stalled_seconds:
                issue("turn_progress_stalled", "warning", turn["id"],
                      "No model, tool, or persisted turn progress has been observed within the monitoring threshold.",
                      "Check the active tool and transport logs.",
                      worker_id=turn["worker_id"], last_activity_at=last,
                      active_tools=(worker.get("runtime_activity") or {}).get("active_tools", {}),
                      threshold_seconds=self.policy.stalled_seconds)
        self.progress = {key: value for key, value in self.progress.items() if key in live_ids}

    @staticmethod
    def _helper_incidents(issue: Incidents, helpers: list[dict]) -> None:
        for helper in helpers:
            if helper["state"] == "uncertain":
                issue("helper_recovery_required", "error", helper["id"],
                      "A decision helper is held for explicit reconciliation.",
                      "Inspect the retained decision, native identities, and owned process evidence; never replay unknown inference.",
                      kind=helper["kind"], thread_id=helper["thread_id"], turn_id=helper["remote_turn_id"],
                      reason=helper.get("reason"), inference_uncertain=helper.get("inference_uncertain"),
                      usage=helper.get("usage"), has_retained_result=helper.get("has_retained_result"))
            activity = helper.get("runtime_activity") or {}
            error = activity.get("last_runtime_error") or {}
            if error.get("timestamp_ms", 0) > activity.get("last_success_at_ms", 0):
                issue("helper_runtime_error", "error", helper["id"],
                      "A decision helper reported a native or local runtime error.",
                      "Inspect the diagnostic reference and helper hold before explicit reconciliation.",
                      diagnostic_sha256=error.get("diagnostic_sha256"), error_type=error.get("error_type"))

    def _task_incidents(self, issue: Incidents, tasks: list[dict], now: int) -> None:
        for task in tasks:
            if task["state"] in {"needs_attention", "recovery_required"}:
                issue("task_" + task["state"], "error", task["id"],
                      "A task requires engineering or operator attention.",
                      "Inspect its contract, latest evidence, and events without granting acceptance.")
            if task["state"] == "verifying":
                validation = task.get("latest_validation") or {}
                verdict = validation.get("verdict")
                age = max(0, now - validation.get("created_at", now)) / 1000
                code = "operator_review_required" if verdict == "needs_operator" else (
                    "validation_unavailable" if verdict == "unavailable" else "validation_pending"
                )
                issue(code, "error" if verdict == "unavailable" else "warning", task["id"],
                      "A candidate is waiting for its exact acceptance policy.",
                      "Inspect validation evidence and use the authorized review workflow; the monitor never accepts artifacts.",
                      artifact_id=validation.get("id"), verdict=verdict,
                      policy=task.get("acceptance_policy"),
                      observation_threshold_exceeded=age >= self.policy.validation_seconds)

    def _work_incidents(self, issue: Incidents, database: dict, service: dict, run: dict, driver: dict,
                        tasks: list[dict], now: int) -> dict:
        """Runnable work the run does not drive, and work blocked on dependencies or on
        interruption tickets; the counts that tell, for the snapshot."""
        run_id = run.get("id")
        actionable = sum(bool(row.get("actionable")) for row in tasks)
        eligible = sum(row["state"] in {"eligible", "reconsider"} for row in database.get("continuations", []))
        interruptions = database.get("interruptions") or {"available": False, "tickets": []}
        tickets = interruptions.get("tickets", [])
        metadata_candidates = sum(ticket["readiness"] == "candidate" for ticket in tickets)
        resume_error = (driver.get("error") or {}).get("code")
        resume_error = resume_error if tickets and resume_error in RESUME_ERROR_CODES else None
        resume_candidates = 0 if resume_error else metadata_candidates
        blocked_tickets = [ticket for ticket in tickets if ticket["readiness"] == "blocked"]
        driving = (service.get("run") or {}).get("driving")
        idle_work = (run.get("state") == "running" and driving is False and not run.get("dispatch_hold")
                     and actionable + eligible + resume_candidates > 0)
        if idle_work:
            since = self.idle_since.setdefault(run_id, now)
            if now - since >= self.policy.idle_grace_seconds * 1000:
                armed = (service.get("run") or {}).get("armed")
                issue("idle_actionable_work" if armed else "execution_unarmed", "warning", run_id,
                      "Actionable work exists while the run has no active driver.",
                      "Inspect wake, dependency, and execution state; an unarmed run needs an explicit drive.",
                      ready_tasks=actionable, eligible_continuations=eligible,
                      interruption_candidates=resume_candidates, armed=armed)
        else:
            self.idle_since.clear()

        blocked_tasks = [
            {"id": task["id"], "state": task["state"],
             "blocked_reasons": task.get("blocked_reasons", [])[:16]}
            for task in tasks if task["state"] in {"ready", "waiting", "needs_attention"}
            and task.get("blocked_reasons")
        ]
        if (run.get("state") == "running" and driving is False
                and actionable + eligible + resume_candidates == 0):
            if blocked_tasks:
                issue("dependency_blocked", "warning", run_id,
                      "No runnable work remains; tasks are waiting on explicit dependency conditions.",
                      "Inspect provider IDs, exact revisions, and policy receipts. Do not bypass validation or retry model calls.",
                      blocked_task_count=len(blocked_tasks), blocked_tasks=blocked_tasks[:20])
            if database.get("dependency_cycles"):
                issue("dependency_cycle", "warning", run_id,
                      "The currently unsatisfied dependency graph contains a cycle.",
                      "Review task decomposition and external evidence. A cycle is not authorization to accept a result.",
                      cycles=database["dependency_cycles"][:20])

        no_other_work = actionable + eligible == 0 and not database.get("attempts")
        if run.get("state") == "running" and driving is False and no_other_work:
            if blocked_tickets:
                issue("interruption_blocked", "warning", run_id,
                      "Parked interruption tickets have observable resume blockers.",
                      "Inspect the original worker, profile, frozen inputs, and snapshot reference; do not bypass resume checks.",
                      blocked_tickets=blocked_tickets[:20])
            if resume_error:
                issue("interruption_resume_failed", "error", run_id,
                      "The driver reported a failed interruption resume check.",
                      "Inspect the referenced driver diagnostic and original ticket. The monitor does not hash or repair the workspace.",
                      driver_code=resume_error,
                      candidate_ticket_ids=[ticket["id"] for ticket in tickets[:20]],
                      ticket_attribution="not_available_in_driver_error")
        return {"actionable": actionable, "eligible": eligible, "interruptions": interruptions,
                "tickets": tickets, "metadata_candidates": metadata_candidates, "resume_error": resume_error,
                "resume_candidates": resume_candidates, "blocked_tasks": blocked_tasks, "driving": driving,
                "idle_work": idle_work}

    def _keep_unobserved(self, found: dict, database: dict, service: dict) -> None:
        """Missing telemetry does not prove that an existing condition recovered."""
        for key, previous_incident in self.active.items():
            code = previous_incident["code"]
            database_unknown = not database.get("available") and not code.startswith("service_")
            driver_unknown = service.get("reachable") is False and code in {
                "driver_needs_attention", "idle_actionable_work", "execution_unarmed",
            }
            if (database_unknown or driver_unknown) and key not in found:
                found[key] = {**previous_incident,
                              "details": {**previous_incident["details"], "observation_stale": True}}

    def _transitions(self, found: dict, run_id: str | None, now: int) -> list[dict]:
        """Incidents opened, updated (in severity) or resolved since the last observation."""
        transitions = []
        for key, incident in found.items():
            previous = self.active.get(key)
            incident["since_ms"] = previous["since_ms"] if previous else now
            if previous is None or previous["severity"] != incident["severity"]:
                transitions.append({
                    "schema_version": 1, "type": "incident", "observed_at_ms": now,
                    "run_id": run_id, "event": "opened" if previous is None else "updated", **incident,
                })
        for key, previous in self.active.items():
            if key not in found:
                transitions.append({
                    "schema_version": 1, "type": "incident", "observed_at_ms": now,
                    "run_id": run_id, "event": "resolved", **previous,
                })
        self.active = found
        return transitions

    @staticmethod
    def _phase(found: dict, service: dict, run: dict, workers: dict, counts: dict, work: dict) -> str:
        """The one-word state of the project, for the snapshot."""
        if service.get("reachable") is False or service.get("process_alive") is False or (
            (service.get("process") or {}).get("identity_matches") is False
        ):
            return "service_unavailable"
        if any(row["state"] == "recovery_required" for row in workers.values()) or run.get("usage_uncertain"):
            return "running_with_issues" if run.get("state") == "running" else "needs_attention"
        if run.get("state") in {"complete", "stopped"}:
            return run["state"]
        if any(item["code"] in ATTENTION_CODES for item in found.values()):
            return "needs_attention"
        if work["driving"]:
            return "running"
        if counts.get("verifying"):
            return "validation_pending"
        if counts.get("waiting"):
            return "waiting"
        if work["idle_work"]:
            return "ready_idle"
        return run.get("state", "no_run")


def _positive(value: str) -> float:
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("Value must be finite and positive")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Observe a FormalGeyser project without changing work or retrying model calls."
    )
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--url", help="Optional loopback service URL for driver health.")
    parser.add_argument("--run-id", help="Run to observe; defaults to the current or latest run.")
    parser.add_argument("--service-pid", type=int, help="Known service PID to check without signaling.")
    parser.add_argument("--worker-pid", action="append", default=[], metavar="WORKER_ID=PID")
    parser.add_argument("--service-start-ms", type=int, help="Expected process creation Unix milliseconds.")
    parser.add_argument("--service-executable", help="Expected service executable path.")
    parser.add_argument("--worker-start-ms", action="append", default=[], metavar="WORKER_ID=UNIX_MS")
    parser.add_argument("--interval", type=_positive, default=2)
    parser.add_argument("--timeout", type=_positive, default=2)
    parser.add_argument("--stalled-seconds", type=_positive, default=300)
    parser.add_argument("--validation-seconds", type=_positive, default=300)
    parser.add_argument("--idle-grace-seconds", type=_positive, default=3)
    parser.add_argument("--stage-seconds", type=_positive, default=120)
    parser.add_argument("--jsonl", type=Path, help="Append records to this file as well as stdout.")
    parser.add_argument("--latest", type=Path, help="Atomically replace this file with the latest snapshot.")
    samples = parser.add_mutually_exclusive_group()
    samples.add_argument("--once", action="store_true")
    samples.add_argument("--samples", type=int, help="Stop after this many snapshots.")
    args = parser.parse_args(argv)
    if args.samples is not None and args.samples < 1:
        parser.error("--samples must be positive")
    worker_pids = {}
    worker_starts = {}
    try:
        if args.latest and args.jsonl and args.latest.resolve() == args.jsonl.resolve():
            raise ValueError("--latest and --jsonl must use different files")
        if (args.service_start_ms is not None or args.service_executable) and args.service_pid is None:
            raise ValueError("Service identity checks require --service-pid")
        for item in args.worker_start_ms:
            worker_id, stamp = item.rsplit("=", 1)
            worker_starts[worker_id] = int(stamp)
        for item in args.worker_pid:
            worker_id, raw_pid = item.rsplit("=", 1)
            pid = int(raw_pid)
            if not worker_id or pid <= 0:
                raise ValueError
            worker_pids[worker_id] = pid
        if not worker_starts.keys() <= worker_pids.keys():
            raise ValueError("Each --worker-start-ms needs a matching --worker-pid")
        if args.service_pid is not None and args.service_pid <= 0:
            raise ValueError
        reader = SnapshotReader(
            args.project, url=args.url, run_id=args.run_id, service_pid=args.service_pid,
            worker_pids=worker_pids, timeout=args.timeout,
            service_start_ms=args.service_start_ms, service_executable=args.service_executable,
            worker_start_ms=worker_starts,
        )
    except ValueError as error:
        parser.error(str(error) or "Process bindings must use WORKER_ID=POSITIVE_PID")
    watchdog = Watchdog(MonitorPolicy(
        args.stalled_seconds,
        args.validation_seconds, args.idle_grace_seconds, args.stage_seconds,
    ))
    target = None
    if args.jsonl:
        args.jsonl.parent.mkdir(parents=True, exist_ok=True)
        target = args.jsonl.open("a", encoding="utf-8")
    maximum = 1 if args.once else args.samples
    count = 0
    try:
        while maximum is None or count < maximum:
            started = time.monotonic()
            for record in watchdog.observe(reader.collect()):
                line = json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
                print(line, flush=True)
                if args.latest and record["type"] == "snapshot":
                    args.latest.parent.mkdir(parents=True, exist_ok=True)
                    atomic_write(args.latest, (line + "\n").encode("utf-8"), prefix=".snapshot-")
                if target:
                    target.write(line + "\n")
                    target.flush()
            count += 1
            if maximum is not None and count >= maximum:
                break
            time.sleep(max(0, args.interval - (time.monotonic() - started)))
    except KeyboardInterrupt:
        return 130
    finally:
        if target:
            target.close()
    return 2 if any(SEVERITIES[item["severity"]] >= SEVERITIES["warning"]
                    for item in watchdog.active.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
