from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import threading
import time
from collections.abc import Iterable
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, get_args
from uuid import uuid4

from pydantic import ValidationError

from ..bootstrap_runtime import bind_contract, require_contract_environment, require_ready
from .batch_integration import BATCH_SCHEMA, INTEGRATOR_GROUP, BatchIntegration
from .claude_profiles import CLAUDE_PROFILES_SCHEMA, ClaudeProfiles
from .codex_profiles import CODEX_PROFILES_SCHEMA, CodexProfiles
from .collaboration import AT_LEAST_SUBSTANCE_SQL, SAME_SUBSTANCE_SQL
from .commands import CommandTable
from .communications import (
    COMMUNICATION_SCHEMA,
    Communications,
    waiting_human_message,
    waiting_peer_message,
)
from .config import codex_profiles_equal, load_config, resolve_config
from .context_events import WORKER_UPDATE_LIMIT, routine_for_recipient, worker_collaboration
from .conversation_archive import ConversationArchive
from .errors import HubError, WorkerWorkspaceError
from .faults import FaultIsolation
from .integration import RepositoryCoordination
from .lifecycle import Lifecycle
from .lifecycle_schema import LIFECYCLE_SCHEMA
from .material_files import MaterialFiles
from .models import (
    BACKENDS,
    Actor,
    Checkpoint,
    Command,
    Effort,
    Limits,
    Predicate,
    ProjectConfig,
    Role,
    TaskContract,
)
from .native import MATHEMATICAL_KINDS, NativeWorkflow
from .rotation import ROTATION_SCHEMA, Rotation
from .schema import LATEST_VERSION_SQL, NEXT_VERSION_SQL, SCHEMA
from .session_settings import (
    SESSION_BACKEND,
    SESSION_SETTINGS_SCHEMA,
    SessionSettings,
    bound_settings,
)
from .shared_dags import SharedDags
from .storage import BlobStore, Database, canonical, collected_thread_call, digest
from .talk import PeerTalk
from .task_names import (
    INTERNAL_KINDS,
    TaskNames,
    allocate_task_number,
    initialize_task_names,
    task_name,
)
from .wait_graph import path_between, wait_edges
from .waker import WAKER_PREFERENCES_SCHEMA, Waker
from .worker_names import (
    WorkerNames,
    allocate_reviewer_number,
    allocate_worker_number,
    initialize_agent_names,
    is_reserved_identity,
    kept_worker_number,
    next_worker_number,
    worker_identity,
)
from .workspace_preparation import WorkspacePreparation
from .workspace_retention import WORKSPACE_RETENTION_SCHEMA, WorkspaceRetention

logger = logging.getLogger(__name__)

# Runtime usage samples use one-off keys that are never replayed;
# their responses would only grow the receipt table.
UNRETAINED_RUNTIME_COMMANDS = frozenset({"usage.record"})
# The first 8 characters of a task or artifact ID (a UUID).
HEX_PREFIX = re.compile(r"[0-9a-f]{8}")
TASK_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


# The core commands' handlers; the lifecycle's own table goes first (Hub._dispatch).
CORE = CommandTable()


def uid() -> str:
    return str(uuid4())


def _requirement_key(requirement: dict) -> tuple[str, str] | None:
    """What Hub._possible_requirements finds for a requirement: its artifact, else its task (None:
    neither is named, so it is always checked). A stored requirement's former `policy` is ignored."""
    if requirement.get("artifact_id"):
        return "artifact", requirement["artifact_id"]
    if requirement.get("task_id"):
        return "task", requirement["task_id"]
    return None


# The evidence that decides whether accepted artifact `a` meets a requirement: the newest of each
# policy it has, of which one accepted without qualifications suffices, as for the artifact's own
# acceptance (artifact.accept): a rejection under an unrelated policy leaves it accepted. In the
# Linux pilot no accepted artifact had evidence under two policies.
LATEST_EVIDENCE_SQL = ("e.sequence=(SELECT MAX(e2.sequence) FROM evidence e2 "
                       "WHERE e2.artifact_id=a.id AND e2.policy=e.policy)")


def control_applies(alias: str, group: str = "NULL") -> str:
    """SQL: the individual control row ``alias``, joined to its run as ``r``, differs from the run
    and from the person's group control (``group``, SQL, see group_control). A resume applies
    while the run or the group is paused. A pause applies while the run runs or the group is
    resumed; one that interrupts also while the run is paused after turns, and one after turn
    while it is paused at all, so a person resumed under Pause all can still finish its turn."""
    return (f"(({alias}.control='resumed' AND (r.state='paused' OR {group}='paused')) OR ({alias}.control='paused' "
            f"AND (r.state='running' OR {group}='resumed' OR (r.state='paused' AND ({alias}.after_turn=1 "
            "OR r.pause_after_turn=1))))) AND (SELECT state FROM project_lifecycle WHERE id=1)='running'")


# The groups the operator pauses after turn or resumes together: all of a backend's workers and
# reviewers, or Codex's workers or reviewers alone; and the Integrator, a group of its own on any
# backend (None) that no other group includes. The Dispatcher and the Waker are in none.
CONTROL_GROUPS: dict[str, tuple[str | None, tuple[str, ...]]] = {
    "codex": ("codex", ("ordinary", "review")), "codex.workers": ("codex", ("ordinary",)),
    "codex.reviewers": ("codex", ("review",)), "claude": ("claude", ("ordinary", "review")),
    "integrator": (None, ("integrator",))}


def upgrade_group_controls(connection) -> None:
    """Rebuild group_controls when its scope check predates the Integrator's group (2026-11-08):
    SQLite cannot change a CHECK constraint in place. Its rows are kept."""
    row = connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='group_controls'").fetchone()
    if row is None or "'integrator'" in row[0]:
        return
    scopes = ",".join(f"'{scope}'" for scope in CONTROL_GROUPS)
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(f"CREATE TABLE group_controls_upgraded(scope TEXT PRIMARY KEY CHECK(scope IN ({scopes})),"
                           "run_id TEXT NOT NULL REFERENCES runs(id),"
                           "control TEXT NOT NULL CHECK(control IN ('paused','resumed')),created_at INTEGER NOT NULL)")
        connection.execute("INSERT INTO group_controls_upgraded(scope,run_id,control,created_at) "
                           "SELECT scope,run_id,control,created_at FROM group_controls")
        connection.execute("DROP TABLE group_controls")
        connection.execute("ALTER TABLE group_controls_upgraded RENAME TO group_controls")
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def group_control(worker: str, run: str | None) -> str:
    """SQL: the group control that applies to the worker row ``worker`` in run ``run`` (SQL; None
    for any run): its role's group (Codex workers or reviewers) before its backend's, and the
    Integrator's for the Integrator. A group control is kept only while it differs from what
    applies around it (Hub._on_group_control)."""
    where = f"g.run_id={run} AND " if run else ""
    return (f"(SELECT g.control FROM group_controls g WHERE {where}(({worker}.kind IN ('ordinary','review') "
            f"AND g.scope IN ({worker}.backend,{worker}.backend||CASE {worker}.kind WHEN 'ordinary' "
            f"THEN '.workers' ELSE '.reviewers' END)) OR ({worker}.kind='integrator' AND g.scope='integrator')) "
            "AND (SELECT state FROM project_lifecycle WHERE id=1)='running' "
            "ORDER BY length(g.scope) DESC LIMIT 1)")


# SQL: the run controlled now; the group control of worker `w` in the run of its own control `wc`.
CURRENT_RUN = "(SELECT id FROM runs WHERE state IN ('running','paused') ORDER BY created_at DESC,id DESC LIMIT 1)"
WORKER_GROUP = group_control("w", "wc.run_id")


class AdmissionScope(frozenset):
    """What a paused run admits: the workers resumed on their own or with their group, and the
    groups resumed (`groups`, as backend and kind), whose new workers or reviewers start too."""
    groups: frozenset[tuple[str, str]]

    def __new__(cls, workers=(), groups=()):
        scope = super().__new__(cls, workers)
        scope.groups = frozenset(groups)
        return scope


# The events that set a run's control, and why a run that is not running stopped after each.
RUN_CONTROL_EVENTS = "('run.pause','run.pause_after_turn','run.stop','run.resume')"
RUN_STOP_REASONS = {"run.pause": "operator_pause", "run.pause_after_turn": "operator_pause",
                    "run.stop": "operator_stop"}


class Hub(WorkspacePreparation, WorkspaceRetention, ConversationArchive, BatchIntegration, FaultIsolation, WorkerNames, TaskNames, SharedDags, NativeWorkflow, Communications, PeerTalk, RepositoryCoordination, Rotation, ClaudeProfiles, CodexProfiles, SessionSettings, Waker, Lifecycle):
    def __init__(
        self, root: Path, config: ProjectConfig | None = None,
        clock: Callable[[], int] | None = None,
    ):
        self.root = Path(root).resolve()
        self.config = resolve_config(self.root, config) if config is not None else load_config(self.root)
        self.state_dir = self.root / ".task-based"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.blobs = BlobStore(self.state_dir / "objects" / "sha256")
        from .reports import ReportStore
        self.reports = ReportStore(self.state_dir)
        self._native_read_caches = threading.local()
        # Kept across calls; each entry is reused only while the files it checked are unchanged.
        self._verified_record_bodies = {}
        self._pending_dispatch_holds = {}
        # Launch spacing: backends whose concurrent sessions have filled since dispatch last began
        # to ramp up, and when that was (the service's first admission, or a run start or resume).
        self._saturated: set[str] = set()
        self._ramp_started: int | None = None
        # Set on the coordination thread while a command applies: its archive moves and task
        # revision files are recorded and written after commit, never inside the transaction.
        self._defer_archive_moves = threading.local()
        self.draft_files.ensure()
        self.db = Database(self.state_dir / "state.sqlite3", SCHEMA + COMMUNICATION_SCHEMA + LIFECYCLE_SCHEMA + BATCH_SCHEMA + ROTATION_SCHEMA + SESSION_SETTINGS_SCHEMA + CLAUDE_PROFILES_SCHEMA + CODEX_PROFILES_SCHEMA + WORKSPACE_RETENTION_SCHEMA + WAKER_PREFERENCES_SCHEMA, on_open=self._recover_startup,
                           before_call=self._clear_native_read_cache, before_read=self._clear_native_read_cache)
        self.clock = clock or (lambda: int(time.time() * 1000))
        self._overdue_check_at = 0.0  # end_overdue_waits: when it next reads the database.

    @property
    def _native_read_cache(self) -> dict:
        """Native file reads reused within one ledger call: on the coordination thread, or on a read
        connection (NativeWorkflow._native_view); each thread keeps its own, cleared before each call."""
        cache = getattr(self._native_read_caches, "value", None)
        if cache is None:
            cache = self._native_read_caches.value = {}
        return cache

    def _clear_native_read_cache(self) -> None:
        self._native_read_cache.clear()

    def _recover_startup(self, c):
        """Quarantine unfinished local execution until its owned processes are confirmed quiet."""
        initialize_agent_names(c)  # Manages its own transaction.
        initialize_task_names(c)  # So does this; it numbers work tasks that have no number.
        upgrade_group_controls(c)  # And this; it lets the Integrator's group be recorded.
        c.execute("BEGIN IMMEDIATE")
        try:
            self._restore_dispatch_hold(c)
            self._recover_native_validations(c)
            c.execute("UPDATE message_delivery_holds SET state='uncertain',reason='Transport was in flight at coordinator restart' WHERE state='sending'")
            attempts = c.execute("SELECT * FROM attempts WHERE ended_at IS NULL").fetchall()
            for attempt in attempts:
                c.execute("UPDATE attempts SET state='recovery_required' WHERE id=?", (attempt["id"],))
                c.execute("UPDATE workers SET state='recovery_required' WHERE id=?", (attempt["worker_id"],))
                if attempt["execution_mode"] != "message_only":
                    c.execute("UPDATE tasks SET state=CASE WHEN state IN ('done','retired') THEN state ELSE 'recovery_required' END "
                              "WHERE id=?", (attempt["task_id"],))
                self._notify(c, attempt["task_id"], attempt["id"], "startup_recovery_required")
            live = c.execute(
                "SELECT DISTINCT run_id FROM turn_intents WHERE state IN ('prepared','sending','bound','uncertain')"
            ).fetchall()
            c.execute("UPDATE turn_intents SET state='uncertain' WHERE state IN ('prepared','sending','bound')")
            for row in live:
                c.execute("UPDATE runs SET usage_uncertain=1 WHERE id=?", (row["run_id"],))
            for helper in c.execute("SELECT id,run_id FROM helper_intents WHERE state='running' AND raw_result_json IS NULL").fetchall():
                c.execute("UPDATE helper_intents SET state='uncertain',inference_uncertain=1,reason='startup_recovery_required' WHERE id=?", (helper["id"],))
                self._hold_helper_usage(c, helper, "Coordinator restarted before helper terminal accounting was known")
            self._reconcile_drafts(c)
            self._reconcile_dags(c)
            self._supersede_stacked_waits(c)
            from .task_graph import backfill_task_origins
            backfill_task_origins(c, self.clock())
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise

    @staticmethod
    def _supersede_stacked_waits(c):
        """Keep only each worker's newest open wait on a task (task.wait now replaces the earlier
        ones). Before, a worker woken by one wait that paused again kept its older waits: in the
        Linux pilot 105 workers held up to 13 on one task, and once older ones held, each new pause
        woke the worker at once (44 of 246 wakes on 2026-10-30)."""
        c.execute("UPDATE continuations SET state='superseded_by_worker' WHERE state IN ('waiting','eligible') "
                  "AND EXISTS(SELECT 1 FROM continuations newer WHERE newer.preferred_worker=continuations.preferred_worker "
                  "AND newer.task_id=continuations.task_id AND newer.state IN ('waiting','eligible') "
                  "AND (newer.created_at>continuations.created_at "
                  "OR newer.created_at=continuations.created_at AND newer.rowid>continuations.rowid))")

    def _finish_drained_submission(self, c, worker, attempt, actor):
        """Commit a quiet attempt's accepted submission as its drain would have, or None when it
        can no longer be committed (the attempt is then abandoned and the reason recorded)."""
        c.execute("SAVEPOINT drained_submission")
        try:
            result = self._dispatch(c, "runtime.quiescent", {
                "worker_id": worker["id"], "snapshot": self._workspace_snapshot(worker["id"])}, actor)
        except HubError as error:
            c.execute("ROLLBACK TO drained_submission")
            c.execute("RELEASE drained_submission")
            self._event(c, "attempt.submission_not_committed", attempt["id"],
                        {"code": error.code, "message": error.message}, attempt["task_id"], worker["id"])
            return None
        c.execute("RELEASE drained_submission")
        self._event(c, "attempt.drain_completed_at_recovery", attempt["id"], {"directive": result.get("directive")},
                    attempt["task_id"], worker["id"])
        return result

    def workspace(self, worker_id: str) -> Path:
        if not worker_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for c in worker_id):
            raise HubError("INVALID_ID", "Invalid worker ID", 422)
        path = self.state_dir / "workers" / worker_id / "work"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def discard_workspace(self, worker_id: str) -> None:
        """Remove the workspace of a worker whose registration was rolled back, if still empty.
        Registration creates it first: in the pilot, rolled-back registrations left 178,000 empty
        ones in three hours. Anything else stays, and `workspace` makes it again if the worker
        exists after all."""
        folder = self.state_dir / "workers" / worker_id
        for path in (folder / "work", folder):
            try:
                path.rmdir()
            except OSError:
                return

    async def close(self):
        await self.stop_archive_moves()
        await self.db.close()

    def _event(self, c, kind, object_id, payload=None, task_id=None, worker_id=None):
        return c.execute(
            "INSERT INTO events(kind,object_id,task_id,worker_id,payload_json,created_at) VALUES(?,?,?,?,?,?)",
            (kind, object_id, task_id, worker_id, canonical(payload or {}), self.clock()),
        ).lastrowid

    def _get(self, c, table, key):
        if table not in {"tasks", "workers", "attempts", "continuations", "artifacts", "runs", "turn_intents", "deliveries", "interruption_checkpoints"}:
            raise ValueError(table)
        row = c.execute(f"SELECT * FROM {table} WHERE id=?", (key,)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", f"{table} object does not exist", 404)
        return worker_identity(dict(row)) if table == "workers" else dict(row)

    def _ledger_task(self, c, task_id, revision):
        """A task revision's identity and contract from the ledger alone, for scans over many
        tasks in one transaction: `_task` also reads the revision's files and its collaboration,
        attempts and evidence (about 50 ms a task in the Linux pilot). The files hold the same
        contract, bound by its hash."""
        row = c.execute("SELECT contract_json,contract_hash FROM task_revisions WHERE task_id=? AND revision=?",
                        (task_id, revision)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "Task revision does not exist", 404)
        return {"id": task_id, "revision": revision, "contract": json.loads(row["contract_json"]),
                "contract_hash": row["contract_hash"]}

    def _task(self, c, key, revision=None):
        result = self._get(c, "tasks", key)
        revision = revision or result["current_revision"]
        row = c.execute(
            "SELECT * FROM task_revisions WHERE task_id=? AND revision=?", (key, revision)
        ).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "Task revision does not exist", 404)
        result.update(revision=revision, contract=self._task_contract(c, row), contract_hash=row["contract_hash"],
                      name=task_name(result.get("task_number")))
        result["files"] = self._task_file_reference(c, key, revision)
        drafts = self._draft_reference(c, key)
        if drafts is not None:
            result["drafts"] = drafts
        result["artifacts"] = [dict(a) for a in c.execute(
            "SELECT id,kind,status,task_revision FROM artifacts WHERE task_id=? AND task_revision=? ORDER BY created_at,id",
            (key, revision))]
        result["collaboration"] = self._collaboration(c, key)
        invalidations = self._completion_invalidations(c, key, revision)
        if invalidations:
            result["current_evidence"] = {"state": "invalid", "invalidations": invalidations}
        repair = c.execute("SELECT task_id,task_revision,declaration_id,id FROM completion_evidence_invalidations "
                           "WHERE repair_task_id=?", (key,)).fetchone()
        if repair:
            result["repair_of"] = {**dict(repair), "origin": "system_generated",
                                   "acceptance_condition_keys": list(self._repair_conditions(c, key))}
        result["active_attempts"] = self._active_attempts(c, key)
        if result["contract"]["kind"] not in {"review"} and revision == 1:
            result["acceptance_conditions"] = {"objective": result["contract"]["objective"],
                **{f"constraint:{index}": value for index, value in enumerate(result["contract"]["constraints"])},
                **{f"requirement:{item['key']}": canonical(item) for item in result["contract"]["requirements"]}}
            result["acceptance_conditions"].update({"repair-original:" + key: value
                                                    for key, value in self._repair_conditions(c, result["id"]).items()})
        return result

    def _artifact(self, c, key):
        result = self._get(c, "artifacts", key)
        result["data"] = self._record_body(c, result)
        result["category"] = "artifact" if result["kind"] in MATHEMATICAL_KINDS else "note"
        result["content_file"] = self._record_files(c, result)
        result["input_refs"] = json.loads(result.pop("input_refs_json"))
        result["dependency_refs"] = self._artifact_dependency_refs(c, key)
        rows = c.execute("SELECT * FROM evidence WHERE artifact_id=? ORDER BY sequence", (key,)).fetchall()
        result["evidence"] = [
            {**dict(r), "details": json.loads(r["details_json"]),
             "qualifications": json.loads(r["qualifications_json"])} for r in rows
        ]
        result["accepted"] = result["status"] == "accepted"
        result["public"] = result["accepted"]
        binding = c.execute("SELECT 1 FROM candidate_bindings WHERE artifact_id=?", (key,)).fetchone()
        if binding:
            result["candidate_binding"] = self._candidate_binding(c, key)
            result["publication_policy"] = result["candidate_binding"]["policy"]
            result["review_task_id"] = result["candidate_binding"]["review_task_id"]
            result["review_feedback"] = self._review_feedback(c, result["review_task_id"])
            result["review_acceptance"] = self._review_acceptance(c, result["review_task_id"])
            result["peer_reviews"] = [{**dict(row), "binding": json.loads(row["binding_json"]),
                                       "evidence": json.loads(row["evidence_json"]),
                                       "qualifications": json.loads(row["qualifications_json"])}
                                      for row in c.execute("SELECT * FROM peer_review_receipts WHERE candidate_id=? ORDER BY created_at,id", (key,))]
            repository = self._candidate_repository(c, key)
            if repository:
                result["repository"] = repository
                publication = c.execute("SELECT integration_id FROM repository_publications WHERE candidate_id=? ORDER BY created_at DESC LIMIT 1", (key,)).fetchone()
                if publication:
                    result["integration"] = self._integration(c, publication["integration_id"])
        return result

    def _role(self, actor, *allowed):
        if actor.role not in allowed:
            raise HubError("FORBIDDEN", "This principal cannot perform this operation", 403)

    def _execution_environment(self, c):
        # Checked once per ledger call; admission may consult it for many tasks.
        if "readiness" not in self._native_read_cache:
            self._native_read_cache["readiness"] = require_ready(self, c)
        return deepcopy(self._native_read_cache["readiness"])

    async def execution_environment(self) -> dict | None:
        return await self.db.call(self._execution_environment)

    def _active(self, c, actor, *, dispatch=False):
        """The live attempt behind a worker's operation. Under Pause after turn the turn
        already under way may still act; before a turn's input is dispatched every pause applies."""
        self._role(actor, Role.WORKER, Role.RUNNER, Role.OPERATOR)
        if not actor.attempt_id or not actor.worker_id:
            raise HubError("STALE_BINDING", "An active attempt binding is required")
        a = self._get(c, "attempts", actor.attempt_id)
        if a["worker_id"] != actor.worker_id or a["fencing_token"] != actor.fencing_token:
            raise HubError("STALE_BINDING", "Attempt identity or fencing token does not match")
        if a["state"] != "running" or a["ended_at"] is not None:
            raise HubError("WORKER_DRAINING", "The attempt is not accepting mutations")
        run = self._get(c, "runs", a["run_id"])
        if (self._effective_run_state(c, run, actor.worker_id) != "running"
                and (dispatch or not self._finishing_turn(c, run, actor.worker_id, a["id"]))):
            raise HubError("RUN_PAUSED", "Run no longer permits worker operations")
        if self._get(c, "workers", actor.worker_id)["state"] == "recovery_required":
            raise HubError("WORKER_RECOVERY_REQUIRED", "This execution requires recovery; other workers may continue")
        task = self._task(c, a["task_id"])
        if self._get(c, "workers", actor.worker_id)["active_attempt"] != a["id"] or (a["execution_mode"] != "message_only" and task["revision"] != a["task_revision"]):
            raise HubError("STALE_REVISION", "Task basis changed")
        self._execution_environment(c)
        return a, task

    def _valid_inputs(self, c, contract):
        if any(not self._requirement(c, req) for req in contract.get("requirements", [])):
            raise HubError("PREREQUISITES_MISSING", "Execution requirements no longer hold")
        for ref in contract.get("inputs", []):
            artifact = self._get(c, "artifacts", ref)
            if artifact["status"] == "private":
                raise HubError("PRIVATE_RECORD", "Private diagnostics cannot acquire public acceptance")
            if artifact["status"] == "withdrawn":
                raise HubError("INPUTS_CHANGED", "A required input was withdrawn")
            if not self._available_artifact(c, ref):
                raise HubError("INPUTS_CHANGED", "Execution inputs must be accepted artifacts")

    def _available_artifact(self, c, artifact_id):
        """An accepted artifact whose recorded inputs are accepted as well."""
        pending, seen = [artifact_id], set()
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            if self._get(c, "artifacts", current)["status"] != "accepted":
                return False
            pending.extend(self._artifact_dependency_refs(c, current))
        return True

    @staticmethod
    def _requirement_where(req):
        """Which accepted artifacts satisfy one requirement: the newest version of the named
        artifact (LATEST_VERSION_SQL), or the evidence of the named revision or of one asking for
        at least the same (AT_LEAST_SUBSTANCE_SQL); a requirement naming a task also needs that
        task completed."""
        if req.get("artifact_id"):
            where, args = "a.id=(" + LATEST_VERSION_SQL + ")", [req["artifact_id"]]
        else:
            where, args = ("a.task_id=? AND EXISTS(SELECT 1 FROM task_revisions ar JOIN task_revisions rr "
                           "ON rr.task_id=ar.task_id WHERE ar.task_id=a.task_id AND ar.revision=a.task_revision "
                           "AND rr.revision=? AND " + AT_LEAST_SUBSTANCE_SQL.format(a="ar", b="rr") + ")",
                           [req["task_id"], req["task_revision"]])
        if req.get("task_id"):
            where += (" AND EXISTS(SELECT 1 FROM candidate_bindings cb WHERE cb.artifact_id=a.id)"
                      " AND EXISTS(SELECT 1 FROM tasks completed JOIN task_revisions cr ON cr.task_id=completed.id "
                      "AND cr.revision=completed.current_revision JOIN task_revisions ar ON ar.task_id=a.task_id "
                      "AND ar.revision=a.task_revision WHERE completed.id=a.task_id AND completed.state='done' AND "
                      + SAME_SUBSTANCE_SQL.format(a="cr", b="ar") + ")"
                      " AND NOT EXISTS(SELECT 1 FROM completion_evidence_invalidations invalid "
                      "WHERE invalid.task_id=a.task_id AND invalid.task_revision=a.task_revision)")
        return where, args

    def _requirement(self, c, req):
        where, args = self._requirement_where(self._named_requirement(c, req))
        rows = c.execute(
            "SELECT a.id,e.verdict,e.qualifications_json FROM artifacts a JOIN evidence e "
            "ON e.artifact_id=a.id WHERE " + where + " AND a.status='accepted' AND " + LATEST_EVIDENCE_SQL,
            args,
        ).fetchall()
        return any(r["verdict"] == "accepted" and not json.loads(r["qualifications_json"])
                   and self._available_artifact(c, r["id"]) for r in rows)

    def _requirement_artifacts(self, c, requirements):
        found: list[str] = []
        for req in requirements:
            where, args = self._requirement_where(self._named_requirement(c, req))
            rows = c.execute(
                "SELECT a.id,e.verdict,e.qualifications_json FROM artifacts a JOIN evidence e ON e.artifact_id=a.id WHERE "
                + where + " AND a.status='accepted' AND " + LATEST_EVIDENCE_SQL, args).fetchall()
            found.extend(r["id"] for r in rows
                         if r["verdict"] == "accepted" and not json.loads(r["qualifications_json"])
                         and self._available_artifact(c, r["id"]))
        return sorted(set(found))

    def _satisfied(self, c, predicate, *, waiter, frozen_refs=None, possible=None):
        """Whether `waiter`'s wait predicate holds; its checks stop once the answer is known. `possible`
        (_possible_requirements): the requirements that may hold; the others fail unread."""
        if possible is not None and predicate["mode"] == "all" and any(
                key is not None and key not in possible for key in map(_requirement_key, predicate.get("requirements", []))):
            return False

        def requirement(r):
            if frozen_refs is not None:
                return bool(frozen_refs.intersection(self._requirement_artifacts(c, [r])))
            key = _requirement_key(r)
            if possible is not None and key is not None and key not in possible:
                return False
            return self._requirement(c, r)

        def results():
            # Subscriptions are not among them: only the waker ends one (_end_subscription_wait),
            # whatever the mode. These combine by the mode.
            yield from map(requirement, predicate.get("requirements", []))
            for question_id in predicate.get("answers", []):
                yield self._answered(c, question_id)
            if predicate.get("external_response_id"):
                yield self._external_wait_satisfied(c, predicate)
            for artifact_id in predicate.get("validations", []):
                yield self._validation_finished(c, artifact_id)
            for candidate_id in predicate.get("publications", []):
                yield self._publication_outcome(c, candidate_id) is not None
            if predicate.get("completion_declaration_id"):
                declaration = c.execute("SELECT state,feedback_state,declarer_id FROM task_completion_declarations WHERE id=?",
                                        (predicate["completion_declaration_id"],)).fetchone()
                # Its declarer waits for the feedback (or the decision); anyone else, the feedback
                # giver included, for the decision alone: a given feedback would wake it at once.
                yield bool(declaration and (declaration["state"] != "pending" or (
                    declaration["feedback_state"] != "pending" and declaration["declarer_id"] == waiter)))
        if not (predicate.get("requirements", []) or predicate.get("answers") or predicate.get("external_response_id")
                or predicate.get("validations") or predicate.get("publications")
                or predicate.get("completion_declaration_id")):
            return False
        return all(results()) if predicate["mode"] == "all" else any(results())

    @staticmethod
    def _possible_requirements(c, requirements) -> set[tuple[str, str]]:
        """The requirements that may hold now, as _requirement_key: a version of the named
        artifact (NEXT_VERSION_SQL), or a reviewed one of the named task once it is done, is
        accepted and has accepted evidence (what _requirement_where asks, but for the newest
        version and the revisions). One statement for all of them; Hub._requirement is several
        for each."""
        artifacts = sorted({r["artifact_id"] for r in requirements if r.get("artifact_id")})
        tasks = sorted({r["task_id"] for r in requirements if not r.get("artifact_id") and r.get("task_id")})
        if not artifacts and not tasks:
            return set()
        return {tuple(row) for row in c.execute(
            # CROSS JOIN keeps the versions outermost: planned freely, it read all evidence.
            "WITH RECURSIVE chain(named,id,depth) AS (SELECT value,value,0 FROM json_each(?) "
            "UNION ALL SELECT chain.named," + NEXT_VERSION_SQL + ") "
            "SELECT 'artifact',chain.named FROM chain CROSS JOIN artifacts a ON a.id=chain.id "
            "CROSS JOIN evidence e ON e.artifact_id=a.id WHERE a.status='accepted' AND e.verdict='accepted' "
            "UNION SELECT 'task',a.task_id FROM tasks t JOIN artifacts a ON a.task_id=t.id "
            "JOIN evidence e ON e.artifact_id=a.id WHERE t.id IN (SELECT value FROM json_each(?)) AND t.state='done' "
            "AND a.status='accepted' AND e.verdict='accepted' "
            "AND EXISTS(SELECT 1 FROM candidate_bindings cb WHERE cb.artifact_id=a.id)",
            (json.dumps(artifacts), json.dumps(tasks)))}

    def _versions_named_by(self, c, artifact_id) -> set[str]:
        """The artifacts `artifact_id` stands for when a requirement names one: itself, and each
        earlier version whose newest version it is (LATEST_VERSION_SQL)."""
        found, pending = {artifact_id}, [artifact_id]
        while pending:
            current = pending.pop()
            for (earlier,) in c.execute(
                    "SELECT predecessor_id FROM artifact_obligations WHERE artifact_id=? AND predecessor_id IS NOT NULL "
                    "UNION SELECT original_id FROM artifact_replacements WHERE replacement_id=?", (current, current)).fetchall():
                if earlier not in found and c.execute(LATEST_VERSION_SQL, (earlier,)).fetchone()[0] == artifact_id:
                    found.add(earlier)
                    pending.append(earlier)
        return found

    def _validation_finished(self, c, artifact_id):
        """The candidate's latest machine validation has a result, or cannot finish without help."""
        phase = c.execute("SELECT validation_state,validation_after_sequence FROM native_call_receipts "
                          "WHERE validation_artifact_id=?", (artifact_id,)).fetchone()
        if phase is not None and phase["validation_state"] in {"obsolete", "blocked", "uncertain"}:
            return True
        return c.execute("SELECT 1 FROM evidence e JOIN candidate_bindings b ON b.artifact_id=e.artifact_id "
                         "WHERE e.artifact_id=? AND e.policy=b.policy AND e.sequence>? LIMIT 1",
                         (artifact_id, phase["validation_after_sequence"] if phase else 0)).fetchone() is not None

    def _publication_outcome(self, c, candidate_id) -> tuple[str, str] | None:
        """How candidate `candidate_id` ended, as (outcome, what a worker waiting on it is told),
        or None while it is on its way to the Library: staged, under review, or accepted and
        waiting for the Integrator. A deferral ends it too, though a retry (integrator_retry)
        queues it again."""
        row = c.execute(
            "SELECT a.status,r.state AS review,d.reason AS deferral,(SELECT verdict FROM peer_review_receipts "
            "WHERE candidate_id=a.id ORDER BY created_at DESC,id DESC LIMIT 1) AS verdict FROM artifacts a "
            "LEFT JOIN review_requests r ON r.candidate_id=a.id LEFT JOIN integration_deferred d "
            "ON d.candidate_id=a.id WHERE a.id=?", (candidate_id,)).fetchone()
        if row is None:
            return None
        rejected_in_review = row["verdict"] == "request_changes"
        if row["status"] == "accepted":
            return "published", f"Candidate {candidate_id} was published to the Library by the Integrator."
        if row["status"] == "rejected" or (rejected_in_review and row["review"] not in {"pending", "integrating"}):
            return "rejected", (f"Candidate {candidate_id} was rejected in review; the review report gives the "
                                "suggestions." if rejected_in_review else
                                f"Candidate {candidate_id} was rejected by its machine validation.")
        if row["status"] == "withdrawn":
            return "withdrawn", f"Candidate {candidate_id} was withdrawn."
        if row["status"] == "superseded":
            return "superseded", (f"Candidate {candidate_id} was superseded: its review basis changed, and it "
                                  "will not be published. Submit a corrected candidate.")
        if row["review"] == "integrating" and row["deferral"] is not None:
            return "deferred", f"The Integrator deferred candidate {candidate_id}: {row['deferral']}"
        # An approved candidate whose machine validation is still running is promoted when it
        # finishes (_on_artifact_validate).
        if row["review"] in {"pending", "integrating"} or (row["review"] == "resolved" and row["verdict"] == "approve"):
            return None
        return "review_ended", (f"The review of candidate {candidate_id} ended without accepting it; it will not "
                                "be published.")

    def _end_wait(self, c, row, predicate, told: set[tuple[str, str]] | None = None):
        """End the wait `row`, whose predicate holds. One ended by its candidates' outcomes is
        reconsidered, and its task is told each outcome (once per task and candidate in `told`);
        any other becomes eligible."""
        outcomes = {candidate_id: outcome for candidate_id in predicate.get("publications", [])
                    if (outcome := self._publication_outcome(c, candidate_id)) is not None}
        if not outcomes:
            c.execute("UPDATE continuations SET state='eligible',wake_reason='requirements_satisfied' WHERE id=?",
                      (row["id"],))
            self._event(c, "continuation.eligible", row["id"], task_id=row["task_id"], worker_id=row["preferred_worker"])
            return
        kinds = {outcome for outcome, _ in outcomes.values()}
        reason = "candidate_" + kinds.pop() if len(kinds) == 1 else "candidates_settled"
        c.execute("UPDATE continuations SET state='reconsider',wake_reason=? WHERE id=?", (reason, row["id"]))
        told = set() if told is None else told
        for candidate_id, (_, text) in outcomes.items():
            if (row["task_id"], candidate_id) not in told:
                told.add((row["task_id"], candidate_id))
                self._notify(c, row["task_id"], candidate_id, text)

    def _wake_publication_waits(self, c, candidate_id):
        """Candidate `candidate_id` may have reached an outcome: the waits naming it that now hold
        end, as _wake ends them, without reading every other wait."""
        told: set[tuple[str, str]] = set()
        for row in c.execute("SELECT DISTINCT k.* FROM continuations k, json_each(k.predicate_json,'$.publications') p "
                             "WHERE k.state='waiting' AND p.value=?", (candidate_id,)).fetchall():
            predicate = json.loads(row["predicate_json"])
            if self._satisfied(c, predicate, waiter=row["preferred_worker"]):
                self._end_wait(c, row, predicate, told)

    def _reconsidering_continuation(self, c, continuation_id):
        if not continuation_id:
            return False
        continuation = self._get(c, "continuations", continuation_id)
        # The wake reason survives admission and repeated interruption/resume.
        # Reconsideration permits inspecting an invalid basis, never publishing it.
        return (continuation["state"] == "reconsider" or
                (continuation["state"] == "resumed" and continuation["wake_reason"] not in
                 {None, "requirements_satisfied", "evidence_changed"}))

    def _wake(self, c):
        # After every change that may end a wait (each suspension, completion and acceptance):
        # in the Linux pilot 437 waiting continuations with 634 requirements, about 1,300
        # statements and 3 s a call, though almost none of them could hold.
        rows = c.execute("SELECT * FROM continuations WHERE state='waiting'").fetchall()
        predicates = [json.loads(row["predicate_json"]) for row in rows]
        possible = self._possible_requirements(c, [r for predicate in predicates for r in predicate.get("requirements", [])])
        told: set[tuple[str, str]] = set()
        for row, predicate in zip(rows, predicates):
            if self._satisfied(c, predicate, waiter=row["preferred_worker"], possible=possible):
                self._end_wait(c, row, predicate, told)
        self._return_set_aside_tasks(c)

    def _return_set_aside_tasks(self, c):
        """A task set aside for an unavailable input returns once all its inputs are available.
        Nothing else reads it again, and every claim is refused meanwhile: in the Linux pilot task
        06e70c66, revised to read a candidate under review, stayed so for three days after its
        publication, and neither a decision nor a new worker could take it."""
        def recompute(task_id, inputs):
            if all(self._available_artifact(c, ref) for ref in inputs):
                self._recompute_task(c, task_id)
        for row in c.execute("SELECT t.id,r.contract_json FROM tasks t JOIN task_revisions r ON r.task_id=t.id "
                             "AND r.revision=t.current_revision WHERE t.state='needs_attention'").fetchall():
            # One unreadable task must not stop the change that woke the others.
            self._admit_item(c, "set_aside_task", row["id"], row["id"], (),
                             lambda: recompute(row["id"], json.loads(row["contract_json"]).get("inputs", [])))

    def _control_of(self, c, run_id, worker_id):
        """The operator's override for one person in a run, and whether it is its own: its own
        ('resumed' while its run or group is paused, 'paused' while either runs), else its
        group's. (None, False) while the person follows the run."""
        if not worker_id:
            return None, False
        group = group_control("w", "r.id")
        row = c.execute(f"SELECT CASE WHEN wc.worker_id IS NOT NULL AND {control_applies('wc', group)} "
                        f"THEN wc.control END AS own,{group} AS group_control FROM workers w JOIN runs r ON r.id=? "
                        "LEFT JOIN worker_controls wc ON wc.worker_id=w.id AND wc.run_id=r.id WHERE w.id=?",
                        (run_id, worker_id)).fetchone()
        if row is None:
            return None, False
        return (row["own"], True) if row["own"] else (row["group_control"], False)

    def _worker_control(self, c, run_id, worker_id):
        """An operator's override for one person, its own or its group's: 'resumed' while its run
        is paused, or 'paused' while it runs. Without one, the person follows the run."""
        return self._control_of(c, run_id, worker_id)[0]

    def _controlled_workers(self, c, run_id, control):
        """The people `control` applies to, their own or their group's; a group's retired members
        are left out."""
        if not c.execute("SELECT 1 FROM group_controls WHERE run_id=?", (run_id,)).fetchone():
            return {row["worker_id"] for row in c.execute(
                "SELECT w.worker_id FROM worker_controls w JOIN runs r ON r.id=w.run_id WHERE w.run_id=? "
                "AND w.control=? AND " + control_applies("w"), (run_id, control))}
        group = group_control("w", "r.id")
        own = {row["id"]: row["control"] for row in c.execute(
            f"SELECT w.id,CASE WHEN {control_applies('wc', group)} THEN wc.control ELSE {group} END AS control "
            "FROM worker_controls wc JOIN workers w ON w.id=wc.worker_id JOIN runs r ON r.id=wc.run_id "
            "WHERE wc.run_id=?", (run_id,))}
        found = {worker_id for worker_id, applied in own.items() if applied == control}
        # Everyone else follows their group, the same for each backend and role: worked out per
        # person, this read all 4,500 workers in the Linux pilot, 0.4 s on every Hub.run.
        for backend in BACKENDS:
            for kind in ("ordinary", "review"):
                applied = c.execute("SELECT " + group_control("w", "?") + " FROM (SELECT ? AS backend,? AS kind) w",
                                    (run_id, backend, kind)).fetchone()[0]
                if applied == control:
                    found.update(row["id"] for row in c.execute(
                        "SELECT id FROM workers WHERE backend=? AND kind=? AND work_intention<>'retired' "
                        "AND state<>'retired'", (backend, kind)) if row["id"] not in own)
        return found

    def _effective_run_state(self, c, run, worker_id=None):
        control = self._worker_control(c, run["id"], worker_id)
        return {"resumed": "running", "paused": "paused"}.get(control, run["state"])

    def _pause_mode(self, c, run, worker_id=None):
        """How the pause that applies to a person, or to the run, treats turns under way:
        'now' interrupts them, 'after_turn' lets them finish. None while it may run."""
        if self._effective_run_state(c, run, worker_id) == "running":
            return None
        if c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running":
            return "now"
        control, own = self._control_of(c, run["id"], worker_id)
        if control == "paused" and own:
            row = c.execute("SELECT after_turn FROM worker_controls WHERE worker_id=?", (worker_id,)).fetchone()
            return "after_turn" if row["after_turn"] else "now"
        if control == "paused":
            return "after_turn"  # A group pauses only after turns.
        return "after_turn" if run["state"] == "paused" and run["pause_after_turn"] else "now"

    def _group_controls(self, c, run_id):
        """The group controls recorded for a run, by group, while Task 0 runs."""
        if c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running":
            return {}
        return {row["scope"]: row["control"] for row in c.execute(
            "SELECT scope,control FROM group_controls WHERE run_id=?", (run_id,))}

    @staticmethod
    def _group_of(controls, backend, kind):
        """The group control that applies to a backend's workers or reviewers (kind): their
        role's group's, else their backend's; the Integrator's own group's, on any backend. The
        Dispatcher and the Waker have none."""
        if kind == "integrator":
            return controls.get("integrator")
        if kind not in {"ordinary", "review"}:
            return None
        return controls.get(backend + (".workers" if kind == "ordinary" else ".reviewers")) or controls.get(backend)

    def _group_state(self, run, controls, backend, kind):
        """'running' or 'paused': whether new turns start for this backend's agents of this kind
        without a control of their own, by their group's control, else the run's state."""
        return {"paused": "paused", "resumed": "running"}.get(self._group_of(controls, backend, kind), run["state"])

    def _group_pause_mode(self, run, controls, backend, kind):
        """How the pause that applies to such agents without a control of their own treats a
        turn under way, as _pause_mode says: a group pauses only after turns."""
        if self._group_state(run, controls, backend, kind) == "running":
            return None
        if self._group_of(controls, backend, kind) == "paused" or run["pause_after_turn"]:
            return "after_turn"
        return "now"

    def _assignment_open(self, run, controls):
        """Whether new work may go to workers: the run runs, or, while it is paused, a group of
        workers does (_assignment_scope). A group of reviewers alone takes no new work."""
        return run["state"] == "running" or any(
            self._group_state(run, controls, backend, "ordinary") == "running" for backend in BACKENDS)

    def _paused_backends(self, c, kind, run_id=None):
        """The backends on which no new agent of this kind starts now, by its group or the run.
        Without a run, the current one."""
        run = (self._get(c, "runs", run_id) if run_id else c.execute(
            "SELECT * FROM runs WHERE state IN ('running','paused') ORDER BY created_at DESC,id DESC LIMIT 1").fetchone())
        if run is None:
            return set()
        controls = self._group_controls(c, run["id"])
        return {backend for backend in BACKENDS if self._group_state(run, controls, backend, kind) != "running"}

    def _resumed_individually(self, c, run_id):
        """The backends of people resumed on their own while their run or group is paused."""
        group = group_control("w", "r.id")
        return {row["backend"] for row in c.execute(
            "SELECT DISTINCT w.backend FROM worker_controls wc JOIN workers w ON w.id=wc.worker_id "
            f"JOIN runs r ON r.id=wc.run_id WHERE wc.run_id=? AND wc.control='resumed' AND {control_applies('wc', group)}",
            (run_id,))}

    def _finishing_turn(self, c, run, worker_id, attempt_id):
        """Pause after turn: the turn this attempt has under way may still act until it ends."""
        return self._pause_mode(c, run, worker_id) == "after_turn" and c.execute(
            "SELECT 1 FROM turn_intents WHERE attempt_id=? AND state IN ('prepared','sending','bound')",
            (attempt_id,)).fetchone() is not None

    def _run_controls(self, c, run_id, worker_id=None):
        run = self._get(c, "runs", run_id)
        limits = Limits.model_validate(json.loads(run["limits_json"]))
        if c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running":
            raise HubError("PROJECT_STOPPED", "Task 0 shutdown requires an explicit operator resumption")
        if self._effective_run_state(c, run, worker_id) != "running":
            if run["state"] == "running":
                raise HubError("WORKER_PAUSED", "The operator paused this worker")
            raise HubError("RUN_PAUSED", "Run is not active")
        self._execution_environment(c)
        return run, limits

    def _admission_scope(self, c, run_id):
        """The workers a run admits now: None means every worker that is not
        paused on its own or with its group; while the run is paused, an AdmissionScope of
        those resumed on their own or with their group, and of the groups resumed. Raises the
        run's control error when it admits nobody."""
        try:
            self._run_controls(c, run_id)
            return None
        except HubError as error:
            if error.code != "RUN_PAUSED":
                raise
            resumed = self._controlled_workers(c, run_id, "resumed")
            run, controls = self._get(c, "runs", run_id), self._group_controls(c, run_id)
            groups: set[tuple[str | None, str]] = {
                (backend, kind) for backend in BACKENDS for kind in ("ordinary", "review")
                if self._group_state(run, controls, backend, kind) == "running"}
            if self._group_state(run, controls, None, "integrator") == "running":
                groups.add(INTEGRATOR_GROUP)  # The Integrator resumed with its group: its batches run.
            if not resumed and not groups:
                raise
        if resumed:
            self._run_controls(c, run_id, min(resumed))
        else:
            self._execution_environment(c)
        return AdmissionScope(resumed, groups)

    def _assignment_scope(self, c, run_id):
        """The admission scope for new work given to idle or new workers (assignment decisions
        and their claims): while the run is paused, only with a group of workers resumed."""
        scope = self._admission_scope(c, run_id)
        if scope is not None and not any(kind == "ordinary" for _, kind in scope.groups):
            raise HubError("RUN_PAUSED", "Run is not active")
        return scope

    def _control_scope(self, c, run_id):
        """The same scope from run controls alone, for recovery bookkeeping. The Dispatcher is in
        no group, but while a group of workers runs under a paused run its decisions run for them."""
        run = self._get(c, "runs", run_id)
        if run["state"] == "running":
            return None
        scope = self._controlled_workers(c, run_id, "resumed")
        if self._assignment_open(run, self._group_controls(c, run_id)):
            scope |= {row["id"] for row in c.execute("SELECT id FROM workers WHERE kind IN ('dispatcher','waker')")}
        return scope

    def _scope_admits(self, c, run_id, scope, worker_id):
        if scope is not None:
            return worker_id in scope
        return self._worker_control(c, run_id, worker_id) != "paused"

    def _claim(self, c, task_id, revision, worker_id, run_id, continuation_id=None,
               interruption_id=None, frozen_inputs=None, contribution_plan="", message_wake=False):
        task, worker = self._task(c, task_id), self._get(c, "workers", worker_id)
        if task["revision"] != revision:
            raise HubError("STALE_REVISION", "Task revision changed")
        reserved = c.execute("SELECT id FROM interruption_checkpoints WHERE state='parked' AND worker_id=?",
                             (worker_id,)).fetchall()
        if any(row["id"] != interruption_id for row in reserved):
            raise HubError("INTERRUPTION_RESERVED", "Resume the parked interruption before claiming this task or worker")
        allowed = {"ready", "claimed", "waiting", "verifying"}
        if task["state"] not in allowed:
            raise HubError("ALREADY_CLAIMED", "Task is not ready for this attempt")
        if task["contract"]["kind"] in {"review"}:
            if self._active_attempts(c, task_id):
                raise HubError("ALREADY_CLAIMED", "A review already has its independent executor")
            if not self._can_review(c, task_id, worker_id):
                raise HubError("REVIEWER_INELIGIBLE", "This worker cannot review this exact candidate or revision")
        if worker["active_attempt"] or worker["state"] in {"preparing", "running", "draining"}:
            raise HubError("ALREADY_CLAIMED", "Worker already has active work")
        if worker["work_intention"] == "retired" or worker["state"] == "retired":
            raise HubError("WORKER_RETIRED", "A retired worker conversation cannot execute again")
        if worker["state"] == "recovery_required":
            raise HubError("RECOVERY_REQUIRED", "Reconcile the previous execution before admitting another turn")
        if worker["state"] == "suspended" and not (continuation_id or interruption_id or message_wake):
            raise HubError("CONTINUATION_RESERVED", "A parked worker is reserved for its continuation")
        if worker["thread_id"]:
            binding = json.loads(worker["profile_json"])
            # The codex_* binding keys are historical names for the worker's own backend.
            if binding.get("codex_home") != self.config.backend_config(worker["backend"]).home:
                raise HubError("PROFILE_HOME_CHANGED", "The native session belongs to another runtime home")
        self._run_controls(c, run_id, worker_id)
        require_contract_environment(self, c, task["contract"])
        reconsider = self._reconsidering_continuation(c, continuation_id)
        if not (reconsider or message_wake):
            self._valid_inputs(c, task["contract"])
        if not (reconsider or message_wake) and any(not self._requirement(c, r) for r in task["contract"].get("requirements", [])):
            raise HubError("PREREQUISITES_MISSING", "Execution requirements are not satisfied")
        if task["contract"]["kind"] in {"review"} and not message_wake:
            request = c.execute("SELECT state FROM review_requests WHERE task_id=?", (task_id,)).fetchone()
            if request is not None and request["state"] == "integrating":
                raise HubError("INTEGRATION_QUEUED", "An accepted review only answers integrator questions")
        if worker["current_task_id"] and worker["current_task_id"] != task_id and self._get(c, "tasks", worker["current_task_id"])["state"] not in {"done", "retired"}:
            raise HubError("CONTINUATION_RESERVED", "A worker with unfinished work takes no other task")
        kept = kept_worker_number(c, task_id)
        if (kept is not None and worker["worker_number"] != kept
                and not c.execute("SELECT 1 FROM task_members WHERE task_id=? AND executor=1", (task_id,)).fetchone()):
            raise HubError("WORKER_NUMBER_KEPT", f"Task {kept} had a worker in the instance it was carried from, so "
                           f"its first executor here is Worker {kept}, a new worker made when the task is first "
                           "assigned", 409)
        if self.config.repository.enabled:
            # This tiny metadata read preserves failure isolation before taking
            # membership or consuming a ticket; the actual checkout is async.
            try:
                self._repository_refresh_report(worker_id)
            except (HubError, OSError, ValueError) as error:
                raise WorkerWorkspaceError(worker_id, task_id, str(error)) from error
        self._admit_member(c, task, worker, plan=contribution_plan, resume=bool(continuation_id or interruption_id))
        self._require_one_task(c, worker_id, task_id)
        # Ownership is reserved here; the async admission boundary prepares files.
        previous = c.execute("SELECT task_id FROM attempts WHERE worker_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
                             (worker_id,)).fetchone()
        if (previous is not None
                and self._task_family(c, previous["task_id"]) != self._task_family(c, task_id)):
            # A new task starts a new conversation, whoever assigns it; binding the new
            # conversation starts the next context epoch.
            self._release_conversation(c, worker_id, "new_task")
        self._adopt_requested_backend(c, worker_id)  # Only before a new conversation.
        aid, fence = uid(), task["fencing_counter"] + 1
        c.execute(
            "INSERT INTO attempts(id,task_id,task_revision,worker_id,run_id,fencing_token,state,continuation_id,created_at)"
            " VALUES(?,?,?,?,?,?,'running',?,?)",
            (aid, task_id, revision, worker_id, run_id, fence, continuation_id, self.clock()),
        )
        input_refs = set(task["contract"]["inputs"])
        input_refs.update(self._requirement_artifacts(c, task["contract"]["requirements"]))
        if continuation_id:
            continuation = self._get(c, "continuations", continuation_id)
            input_refs.update(self._requirement_artifacts(c, json.loads(continuation["predicate_json"]).get("requirements", [])))
        if frozen_inputs is not None:
            input_refs = set(frozen_inputs)
        c.execute("UPDATE attempts SET input_refs_json=? WHERE id=?", (canonical(sorted(input_refs)), aid))
        c.execute("UPDATE tasks SET state='claimed',active_attempt=COALESCE(active_attempt,?),fencing_counter=?,state_version=state_version+1 WHERE id=?", (aid, fence, task_id))
        self._member(c, task_id, worker_id, executor=True)
        self._set_member_work(c, task_id, worker_id, False, "executing")
        member = c.execute("SELECT checkpoint_json,snapshot FROM task_members WHERE task_id=? AND worker_id=?", (task_id, worker_id)).fetchone()
        if member["checkpoint_json"]:
            c.execute("UPDATE attempts SET checkpoint_json=?,snapshot=? WHERE id=?", (member["checkpoint_json"], member["snapshot"], aid))
        c.execute("UPDATE workers SET state='running',active_attempt=?,run_id=?,current_task_id=?,work_intention=?,execution_mode=? WHERE id=?",
                  (aid, run_id, task_id, "waiting" if message_wake else "working", "message" if message_wake else "work", worker_id))
        self._event(c, "task.claimed", task_id, {"attempt_id": aid, "fencing_token": fence}, task_id, worker_id)
        result = self._get(c, "attempts", aid)
        result["task"] = self._task(c, task_id)
        if result["checkpoint_json"]:
            result["checkpoint"] = json.loads(result["checkpoint_json"])
        return result

    def _interruption_inputs(self, c, attempt, task):
        """Validate the original binding without selecting replacement evidence."""
        refs = set(json.loads(attempt["input_refs_json"]))
        if self._reconsidering_continuation(c, attempt["continuation_id"]):
            return sorted(refs)
        if not set(task["contract"]["inputs"]).issubset(refs):
            raise HubError("INPUTS_CHANGED", "Original attempt did not bind the current direct inputs")
        for ref in refs:
            if self._get(c, "artifacts", ref)["status"] == "withdrawn":
                raise HubError("INPUTS_CHANGED", "An original interruption input was withdrawn")
        def bound(req):
            return bool(refs.intersection(self._requirement_artifacts(c, [req])))
        if not all(bound(req) for req in task["contract"]["requirements"]):
            raise HubError("PREREQUISITES_MISSING", "Original requirement evidence no longer holds")
        if attempt["continuation_id"]:
            prior = self._get(c, "continuations", attempt["continuation_id"])
            predicate = json.loads(prior["predicate_json"])
            if not self._satisfied(c, predicate, waiter=prior["preferred_worker"], frozen_refs=refs):
                raise HubError("PREREQUISITES_MISSING", "Original wait evidence no longer holds")
        return sorted(refs)

    def _park_interruption(self, c, p, *, imported):
        attempt = self._get(c, "attempts", p["attempt_id"])
        worker = self._get(c, "workers", attempt["worker_id"])
        task = self._task(c, attempt["task_id"])
        if task["revision"] != attempt["task_revision"]:
            raise HubError("STALE_REVISION", "An interruption cannot cross task revisions")
        if not worker["thread_id"]:
            raise HubError("SESSION_REQUIRED", "A durable native thread is required")
        if p.get("terminal_observed") is not True or p.get("process_quiet") is not True:
            raise HubError("QUIESCENCE_REQUIRED", "Observed interrupted terminal and stopped process tree are required")
        intent = self._get(c, "turn_intents", p["turn_intent_id"])
        if (intent["attempt_id"] != attempt["id"] or intent["worker_id"] != worker["id"]
                or intent["state"] != "interrupted" or not intent["remote_turn_id"]):
            raise HubError("INTERRUPTED_TURN_REQUIRED", "Only a known interrupted native turn is resumable")
        if c.execute("SELECT 1 FROM turn_intents WHERE worker_id=? "
                     "AND state IN ('prepared','sending','bound','uncertain')", (worker["id"],)).fetchone():
            raise HubError("TURN_UNRESOLVED", "A worker with an unresolved turn cannot be parked")
        latest = c.execute("SELECT id FROM attempts WHERE worker_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
                           (worker["id"],)).fetchone()
        if latest["id"] != attempt["id"]:
            raise HubError("SESSION_CHANGED", "The original worker has already run another attempt")
        expected = {"worker_id": worker["id"], "task_id": task["id"],
                    "task_revision": attempt["task_revision"], "thread_id": worker["thread_id"],
                    "remote_turn_id": intent["remote_turn_id"], "usage_epoch": worker["usage_epoch"]}
        if any(p.get(key) != value for key, value in expected.items()):
            raise HubError("SESSION_CHANGED", "Explicit interruption identity does not match the durable binding")
        profile = self.config.runtime_profile(worker["backend"])
        stored_profile = json.loads(worker["profile_json"])
        if (not codex_profiles_equal(p.get("codex_profile"), profile)
                or not codex_profiles_equal(stored_profile.get("codex_profile"), profile)):
            raise HubError("PROFILE_CHANGED", "The interrupted native profile must match the configured runtime")
        # The conversation's own model, which may differ from the project's.
        bound = bound_settings(worker)
        expected_model = bound["model"] if bound is not None else profile["model"]
        if expected_model is not None and stored_profile.get("model") != expected_model:
            raise HubError("PROFILE_CHANGED", "The bound native model does not match the interrupted profile")
        stop_reason = p["stop_reason"]
        if stop_reason not in {"operator_pause", "operator_stop"}:
            raise HubError("INVALID_INPUT", "An explicit operator pause or stop reason is required", 422)
        if imported:
            if (attempt["state"] != "abandoned" or attempt["ended_at"] is None
                    or worker["active_attempt"] is not None or worker["state"] != "idle"):
                raise HubError("IMPORT_NOT_SAFE", "Import requires the latest abandoned attempt and an idle original task and worker")
            if p.get("process_confirmed_stopped") is not True:
                raise HubError("QUIESCENCE_REQUIRED", "The operator must independently confirm process shutdown")
            if (not isinstance(p.get("reason"), str) or not p["reason"].strip()
                    or not isinstance(p.get("evidence"), str) or not p["evidence"].strip()
                    or len(p["reason"]) > 8192 or len(p["evidence"]) > 8192):
                raise HubError("EVIDENCE_REQUIRED", "Record the observed terminal and process-shutdown evidence", 422)
            snapshot = self._workspace_snapshot(worker["id"])
        else:
            if (attempt["state"] != "running" or attempt["ended_at"] is not None
                    or worker["active_attempt"] != attempt["id"]):
                raise HubError("NOT_RUNNING", "Only the current interrupted attempt can be parked")
            observation = c.execute("SELECT payload_json FROM events WHERE kind='turn.finished' AND object_id=? "
                                    "ORDER BY sequence DESC LIMIT 1", (intent["id"],)).fetchone()
            recorded = json.loads(observation["payload_json"]) if observation else {}
            if recorded.get("terminal_observed") is not True or recorded.get("process_quiet") is not True:
                raise HubError("QUIESCENCE_REQUIRED", "A durable terminal and process-quiescence observation is required")
            snapshot = p["snapshot"]
            if self._workspace_snapshot(worker["id"]) != snapshot:
                raise HubError("SNAPSHOT_CHANGED", "The supplied snapshot does not match the quiet workspace")
        refs = self._interruption_inputs(c, attempt, task)
        checkpoint = (json.loads(attempt["checkpoint_json"]) if attempt["checkpoint_json"] else {
            "summary": "The runtime preserved an interrupted native turn and its workspace. No worker-authored progress summary was available.",
            "next_action": "Continue from the existing conversation and workspace after inspecting the current context packet.",
            "input_refs": refs})
        proof = {**expected, "terminal_status": "interrupted", "terminal_observed": True,
                 "process_quiet": True, "turn_intent_id": intent["id"], "observed_at": self.clock(),
                 "source": "operator_import" if imported else "runtime",
                 "session_profile": stored_profile,
                 "reason": p.get("reason"), "evidence": p.get("evidence")}
        iid = uid()
        c.execute("INSERT INTO interruption_checkpoints(id,task_id,task_revision,worker_id,origin_attempt,"
                  "turn_intent_id,thread_id,usage_epoch,workspace,codex_profile_json,snapshot,input_refs_json,"
                  "checkpoint_json,proof_json,stop_reason,state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'parked',?)",
                  (iid, task["id"], attempt["task_revision"], worker["id"], attempt["id"], intent["id"],
                   worker["thread_id"], worker["usage_epoch"], str(self.workspace(worker["id"]).resolve()),
                   canonical(profile), snapshot, canonical(refs), canonical(checkpoint), canonical(proof), stop_reason, self.clock()))
        if not imported:
            c.execute("UPDATE attempts SET state='interrupted',ended_at=?,snapshot=? WHERE id=?",
                      (self.clock(), snapshot, attempt["id"]))
        c.execute("UPDATE workers SET state='suspended',active_attempt=NULL WHERE id=?", (worker["id"],))
        self._preserve_member(c, attempt, snapshot, reason="interrupted")
        self._refresh_aggregate(c, task["id"])
        c.execute("UPDATE tasks SET state='waiting',state_version=state_version+1 WHERE id=? AND active_attempt IS NULL", (task["id"],))
        self._recompute_collaborative(c, task["id"])
        self._event(c, "interruption.imported" if imported else "interruption.parked", iid,
                    {"origin_attempt": attempt["id"], "snapshot": snapshot, "stop_reason": stop_reason,
                     "proof": proof}, task["id"], worker["id"])
        return {"directive": "parked", "interruption_id": iid, "snapshot": snapshot,
                "worker_id": worker["id"], "task_id": task["id"], "stop_reason": stop_reason}

    def _reconcile_terminal(self, c, p):
        """Resolve one externally verified terminal without admitting further execution."""
        intent = self._get(c, "turn_intents", p["intent_id"])
        if intent["state"] not in {"uncertain", "reconciled", "closed_unknown"}:
            raise HubError("TURN_NOT_UNCERTAIN", "Only an unresolved native terminal can receive exact terminal reconciliation")
        attempt = self._get(c, "attempts", intent["attempt_id"])
        worker = self._get(c, "workers", intent["worker_id"])
        run = self._get(c, "runs", intent["run_id"])
        task = self._task(c, attempt["task_id"])
        expected = {
            "intent_id": intent["id"], "attempt_id": attempt["id"], "run_id": run["id"],
            "task_id": task["id"], "task_revision": attempt["task_revision"],
            "worker_id": worker["id"], "thread_id": worker["thread_id"],
            "remote_turn_id": intent["remote_turn_id"], "usage_epoch": worker["usage_epoch"],
        }
        if (any(type(p.get(key)) is not type(value) or p.get(key) != value
                for key, value in expected.items())
                or not worker["thread_id"] or not intent["remote_turn_id"]
                or attempt["worker_id"] != worker["id"] or attempt["run_id"] != run["id"]
                or worker["run_id"] != run["id"]):
            raise HubError("SESSION_CHANGED", "Exact original turn, attempt, worker, thread, and usage bindings are required")
        if task["revision"] != attempt["task_revision"]:
            raise HubError("STALE_REVISION", "The original task revision is no longer current")
        recovered = (attempt["state"] == "abandoned" and attempt["ended_at"] is not None
                     and worker["state"] in {"idle", "recovery_required"} and worker["active_attempt"] is None)
        publication_hold = (attempt["state"] == "recovery_required" and attempt["ended_at"] is None
                            and worker["state"] == "recovery_required" and worker["active_attempt"] == attempt["id"]
                            and self._unrecorded_publication(c, worker, attempt) is not None)
        if (not (recovered or publication_hold)
                or c.execute("SELECT 1 FROM attempts WHERE worker_id=? AND ended_at IS NULL AND id<>?",
                             (worker["id"], attempt["id"])).fetchone()):
            raise HubError("RECONCILIATION_NOT_SAFE", "Recover or quarantine this stopped original attempt before reconciliation")
        latest_attempt = c.execute(
            "SELECT id FROM attempts WHERE worker_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
            (worker["id"],),
        ).fetchone()
        latest_intent = c.execute(
            "SELECT id FROM turn_intents WHERE worker_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1",
            (worker["id"],),
        ).fetchone()
        if latest_attempt["id"] != attempt["id"] or latest_intent["id"] != intent["id"]:
            raise HubError("SESSION_CHANGED", "The original worker has admitted newer execution")
        if c.execute(
            "SELECT 1 FROM turn_intents WHERE id<>? AND worker_id=? "
            "AND state IN ('prepared','sending','bound','uncertain')",
            (intent["id"], worker["id"]),
        ).fetchone():
            raise HubError("TURN_UNRESOLVED", "Other unresolved turns must not be cleared by this reconciliation")
        status = p.get("status")
        if status not in {"interrupted", "completed", "failed", "cancelled"}:
            raise HubError("TERMINAL_REQUIRED", "A known externally observed terminal status is required", 422)
        if p.get("terminal_observed") is not True or p.get("process_confirmed_stopped") is not True:
            raise HubError("QUIESCENCE_REQUIRED", "Independently verify the terminal and all owned process shutdown")
        reason = p.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 8192:
            raise HubError("EVIDENCE_REQUIRED", "Record a bounded explanation of the terminal and usage verification", 422)
        total, cumulative = p.get("tokens_used"), p.get("cumulative_total_tokens")
        if (type(total) is not int or type(cumulative) is not int
                or not 0 <= total <= 9223372036854775807
                or not 0 <= cumulative <= 9223372036854775807):
            raise HubError("INVALID_USAGE", "Explicit nonnegative run and cumulative native token totals are required", 422)
        previous = c.execute(
            "SELECT MAX(total) FROM usage_samples WHERE worker_id=? AND epoch=?",
            (worker["id"], worker["usage_epoch"]),
        ).fetchone()[0] or 0
        delta = cumulative - previous
        if delta < 0 or total < run["tokens_used"] + delta:
            raise HubError("INVALID_USAGE", "Reconciliation cannot refund tokens or omit newly observed cumulative usage", 422)
        evidence = p.get("evidence")
        if (not isinstance(evidence, dict) or set(evidence) != {"path", "sha256"}
                or not isinstance(evidence["path"], str) or not evidence["path"]
                or not isinstance(evidence["sha256"], str)
                or len(evidence["sha256"]) != 64
                or any(char not in "0123456789abcdef" for char in evidence["sha256"])):
            raise HubError("EVIDENCE_REQUIRED", "Supply a project-relative evidence path and exact lowercase SHA-256", 422)
        relative = Path(evidence["path"])
        if relative.anchor or ".." in relative.parts:
            raise HubError("EVIDENCE_REQUIRED", "Evidence must remain within the project directory", 422)
        source = (self.root / relative).resolve()
        if not source.is_relative_to(self.root):
            raise HubError("EVIDENCE_REQUIRED", "Evidence must remain within the project directory", 422)
        try:
            with source.open("rb") as stream:
                body = stream.read(1048577)
        except OSError as exc:
            raise HubError("EVIDENCE_REQUIRED", "The referenced evidence file is unavailable", 422) from exc
        if not body or len(body) > 1048576:
            raise HubError("EVIDENCE_REQUIRED", "Evidence must contain between one byte and one MiB", 422)
        if hashlib.sha256(body).hexdigest() != evidence["sha256"]:
            raise HubError("EVIDENCE_MISMATCH", "Evidence bytes do not match the reviewed SHA-256", 422)
        evidence_sha = self.blobs.put(body)
        c.execute("INSERT INTO usage_samples VALUES(?,?,?,?,?) ON CONFLICT(worker_id,epoch) DO UPDATE SET "
                  "total=excluded.total,sample_json=excluded.sample_json,created_at=excluded.created_at "
                  "WHERE usage_samples.total<>excluded.total OR usage_samples.sample_json<>excluded.sample_json", (
            worker["id"], worker["usage_epoch"], cumulative,
            canonical({"totalTokens": cumulative, "source": "operator_terminal_reconciliation",
                       "intent_id": intent["id"], "evidence_sha256": evidence_sha}), self.clock(),
        ))
        c.execute("UPDATE turn_intents SET state=? WHERE id=?", (status, intent["id"]))
        c.execute("UPDATE turn_usage_totals SET end_total=?,updated_at=? WHERE intent_id=?",
                  (cumulative, self.clock(), intent["id"]))
        # The reconciled tokens were spent by this worker's backend account.
        self._charge_backend(c, run["id"], worker["backend"], total - run["tokens_used"])
        c.execute("DELETE FROM worker_usage_holds WHERE worker_id=?", (worker["id"],))
        self._refresh_usage_uncertainty(c, run["id"])
        if recovered:
            # Proof permits a later explicit import/recovery, not automatic work.
            c.execute("UPDATE workers SET state='idle' WHERE id=?", (worker["id"],))
        self._event(c, "turn.terminal_reconciled", intent["id"], {
            **expected, "previous_state": intent["state"], "status": status,
            "terminal_observed": True, "process_quiet": True, "process_confirmed_stopped": True,
            "source": "operator", "reason": reason, "evidence_sha256": evidence_sha,
            "evidence_path": relative.as_posix(), "previous_tokens_used": run["tokens_used"],
            "tokens_used": total, "previous_cumulative_total_tokens": previous,
            "cumulative_total_tokens": cumulative, "native_usage_delta": delta,
        }, task["id"], worker["id"])
        return {
            "turn_intent": self._get(c, "turn_intents", intent["id"]),
            "run": self._get(c, "runs", run["id"]), "evidence_sha256": evidence_sha,
            "native_usage_delta": delta, "added_tokens": total - run["tokens_used"],
        }

    def _invalidate_interruptions(self, c, task_id, state):
        c.execute("UPDATE workers SET state='idle' WHERE state='suspended' AND active_attempt IS NULL AND id IN "
                  "(SELECT worker_id FROM interruption_checkpoints WHERE task_id=? AND state='parked')", (task_id,))
        c.execute("UPDATE interruption_checkpoints SET state=? WHERE task_id=? AND state='parked'", (state, task_id))

    async def execute(self, command: Command, actor: Actor | None = None, *, native_call: dict | None = None):
        actor = actor or Actor(role=Role.OPERATOR)
        try:
            # Normalize machine output outside the single coordination thread.
            # Replayed raw and retained compact reports produce the same request hash.
            if command.name == "artifact.accept":
                details = await collected_thread_call(self.reports.compact, command.payload.get("details", {}))
                command = command.model_copy(update={"payload": {**command.payload, "details": details}})
            elif command.name == "integrator.validation":
                reports = {key: await collected_thread_call(self.reports.report, value)
                           for key, value in command.payload["reports"].items()}
                command = command.model_copy(update={"payload": {**command.payload, "reports": reports}})
            principal = f"{actor.role}:{actor.worker_id or 'operator'}"
            request_hash = digest({"payload": command.payload, "attempt": actor.attempt_id, "fence": actor.fencing_token})
            prepared_dags = prepared_draft = None
            prepared_snapshots = await self.prepare_command_snapshots(command, actor, principal)
            await self.prepare_batch_publication(command)
            prepared_record = await self.prepare_private_record(command, actor)

            def mutate(c):
                prior = c.execute("SELECT * FROM receipts WHERE principal=? AND command=? AND key=?", (principal, command.name, command.key)).fetchone()
                if prior:
                    if prior["request_hash"] != request_hash:
                        raise HubError("IDEMPOTENCY_CONFLICT", "Idempotency key was reused with different data")
                    response = json.loads(prior["response_json"])
                else:
                    if command.name == "dag.update":
                        response = self._commit_dag_update(c, command.payload, actor, prepared_dags)
                    elif command.name == "task.draft.update":
                        response = self._commit_draft_update(c, command.payload, actor, prepared_draft)
                    else:
                        response = self._dispatch(c, command.name, command.payload, actor)
                    if not (command.name in UNRETAINED_RUNTIME_COMMANDS and command.key.startswith("runtime:")):
                        c.execute("INSERT INTO receipts VALUES(?,?,?,?,?)", (principal, command.name, command.key, request_hash, canonical(response)))
                if command.name == "artifact.accept":
                    self._record_native_validation(c, command, actor, response)
                if native_call is not None:
                    self._record_native_core(c, native_call, actor, command, response)
                return response
            if command.name == "dag.update":
                prepared_dags = await self._prepare_dag_update(command, actor, principal, request_hash)
            if command.name == "task.draft.update":
                prepared_draft = await self._prepare_draft_update(command, actor, principal, request_hash)
            def apply(c):
                return self._with_deferred_files(c, lambda c: self.with_workspace_snapshots(
                    c, prepared_snapshots, lambda c: self._with_prepared_record(c, prepared_record, mutate)))
            result = None
            try:
                result = await self.db.call(apply, write=True, label="command:" + command.name)
            finally:
                await self.discard_prepared_sources(prepared_snapshots)
                await self.discard_private_record(prepared_record, result)
            await self.finish_deferred_files(command.name)
            if command.name == "task.draft.update":
                await self.reconcile_drafts(command.payload["task_id"])
            if command.name == "dag.update":
                await self.reconcile_dags()
            if command.name.startswith("talk."):
                await self.reconcile_peer_talk()
            if command.name in {"message.delivery.resolve", "turn.not_sent"}:
                await self.reconcile_delivery_transcripts()
            if command.name == "run.resume":
                await self.finish_dispatch_release()
            return await self.prepare_assignment_workspace(result)
        except ValidationError as exc:
            raise HubError("INVALID_INPUT", str(exc), 422) from exc
        except (KeyError, ValueError, TypeError) as exc:
            if isinstance(exc, (KeyError, TypeError)):
                # Usually a malformed payload, but possibly an internal defect:
                # keep the traceback for the operator instead of hiding it.
                logger.warning("Command %s rejected after %s", command.name, type(exc).__name__, exc_info=True)
            raise HubError("INVALID_INPUT", str(exc), 422) from exc
        except sqlite3.IntegrityError as exc:
            raise HubError("CONFLICT", "A uniqueness or relationship constraint prevented this operation") from exc

    def _dispatch(self, c, name, p, actor):
        p = self._resolve_task_refs(c, p)  # Task N names a task wherever its id is taken.
        self._require_current_mutation(c, name, p, actor)
        if actor.role == Role.WORKER and actor.attempt_id:
            attempt = self._get(c, "attempts", actor.attempt_id)
            kind = self._get(c, "workers", actor.worker_id)["kind"]
            # The Integrator waits for an answer by deferring the item, never by pausing.
            if kind == "integrator" and not (name.startswith(("integrator.", "talk.", "message.", "request."))
                    or name in {"context.ack", "record.private", "task.checkpoint"}):
                raise HubError("INTEGRATOR_SCOPE", "The integrator maintains accepted batches, not ordinary task work", 403)
            if kind == "review" and attempt["execution_mode"] == "message_only" and not name.startswith(("talk.", "message.", "request.", "context.")):
                raise HubError("REVIEW_ANSWER_SCOPE", "This clarification turn only answers questions about your review", 403)
            if attempt["execution_mode"] == "message_only" and name == "task.revise":
                # An original publisher may revise its task from a correspondence turn; a current
                # executor revises in its own task turn. task.revise makes every other check.
                if not c.execute("SELECT 1 FROM task_publishers WHERE task_id=? AND worker_id=?",
                                 (p.get("task_id"), actor.worker_id)).fetchone():
                    raise HubError("MESSAGE_ONLY_AUTHORITY", "In a message-only turn only the task's original "
                                   "publishers may revise it; its executors revise it in their own task turn", 403)
            elif attempt["execution_mode"] == "message_only" and not (
                    name.startswith(("message.", "literature.", "request.")) or name in {"dag.update", "talk.send", "talk.inbox", "talk.history", "talk.list", "talk.ack", "integrator.retry", "context.ack", "record.private", "task.publish", "worker.resume_work", "task.resume_work"}):
                raise HubError("MESSAGE_ONLY_AUTHORITY", "This one-off communication turn has no task execution or peer-send authority", 403)
        if name.startswith("integrator."):
            return self._integrator_command(c, name, p, actor)
        result = self._lifecycle_command(c, name, p, actor)
        if result is not None:
            return result
        result = self._dispatch_core(c, name, p, actor)
        return self._lifecycle_after(c, name, p, actor, result)

    def _dispatch_core(self, c, name, p, actor):
        handler = CORE.get(name)
        if handler is None:
            raise HubError("UNKNOWN_COMMAND", f"Unknown command: {name}", 404)
        return handler(self, c, name, p, actor)

    @CORE.handles("validation.recover")
    def _on_validation_recover(self, c, name, p, actor):
        return self._recover_candidate_validation(c, p, actor)

    @CORE.handles("validation.retry")
    def _on_validation_retry(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        if not isinstance(p.get("reason"), str) or not p["reason"].strip():
            raise HubError("REASON_REQUIRED", "Record why this unavailable check should run again", 422)
        result = self._retry_unavailable_validation(c, p["artifact_id"])
        self._event(c, "validation.retry_requested", p["artifact_id"], {"reason": p["reason"]})
        return result

    @CORE.handles("task.draft.update")
    def _on_task_draft_update(self, c, name, p, actor):
        raise HubError("DRAFT_PREPARATION_REQUIRED", "Draft files must be prepared outside the coordination transaction")

    @CORE.handles("message.", "literature.", "request.")
    def _on_communication(self, c, name, p, actor):
        if name == "message.send":
            self._role(actor, Role.OPERATOR, Role.WORKER)
            if actor.role == Role.WORKER:
                self._active(c, actor)
            self._normalize_worker_fields(c, p, "worker_id")
        return self._communication_command(c, name, p, actor)

    @CORE.handles("talk.")
    def _on_peer_talk(self, c, name, p, actor):
        return self._peer_talk_command(c, name, p, actor)

    @CORE.handles("repository.")
    def _on_repository(self, c, name, p, actor):
        result = self._repository_command(c, name, p, actor)
        if result is None:
            raise HubError("UNKNOWN_COMMAND", "Unknown repository operation", 422)
        return result

    @CORE.handles("artifact.stage", "record.private", "review.accept", "review.reject", "task.subscribe")
    def _on_collaboration(self, c, name, p, actor):
        return self._collaboration_command(c, name, p, actor)

    @CORE.handles("run.start")
    def _on_run_start(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        self._execution_environment(c)
        self._require_choices(c)
        if c.execute("SELECT 1 FROM runs WHERE state='running'").fetchone():
            raise HubError("RUN_ACTIVE", "Pause or stop the active run before creating another")
        limits = Limits.model_validate({**self.config.limits.model_dump(), **p.get("limits", {})})
        rid = uid()
        c.execute("INSERT INTO runs(id,state,limits_json,created_at) VALUES(?,'running',?,?)", (rid, canonical(limits.model_dump()), self.clock()))
        self._event(c, "run.started", rid, {"limits": limits.model_dump(mode="json")})
        self._restart_ramp()
        return {**self._get(c, "runs", rid), "limits": limits.model_dump()}

    @CORE.handles("run.complete", "run.pause", "run.pause_after_turn", "run.resume", "run.stop")
    def _on_run_control(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        if name == "run.resume":
            self._execution_environment(c)
            self._require_choices(c)
        rid = p["run_id"]
        run = self._get(c, "runs", rid)
        transitions = {
            "run.pause": {"running", "paused"}, "run.pause_after_turn": {"running", "paused"},
            "run.resume": {"paused", "running"},
            "run.stop": {"running", "paused", "stopped"},
            "run.complete": {"running", "paused", "complete"},
        }
        if run["state"] not in transitions[name]:
            raise HubError("INVALID_RUN_TRANSITION", "A terminal run cannot return to active use")
        if name == "run.complete" and (
            c.execute("SELECT 1 FROM tasks WHERE state NOT IN ('done','retired')").fetchone()
            or c.execute("SELECT 1 FROM attempts WHERE ended_at IS NULL").fetchone()
        ):
            raise HubError("WORK_REMAINS", "Cannot complete a run with unfinished tasks")
        if name == "run.resume" and run["state"] in {"complete", "stopped"}:
            raise HubError("RUN_CANNOT_RESUME", "A terminal run cannot be resumed")
        state = {"run.pause": "paused", "run.pause_after_turn": "paused", "run.resume": "running",
                 "run.stop": "stopped", "run.complete": "complete"}[name]
        if state == "running" and c.execute("SELECT 1 FROM runs WHERE state='running' AND id<>?", (rid,)).fetchone():
            raise HubError("RUN_ACTIVE", "Another run is active")
        # Pause all after turn lets the turns under way finish; every other control ends that.
        c.execute("UPDATE runs SET state=?,pause_after_turn=? WHERE id=?",
                  (state, int(name == "run.pause_after_turn"), rid))
        # Pause all and Resume all apply to everyone, ending individual and group controls.
        c.execute("DELETE FROM worker_controls")
        c.execute("DELETE FROM group_controls")
        self._event(c, name, rid)
        if name == "run.resume":
            self._restart_ramp()
            released = self._release_dispatch_hold(c)
            if released:
                # The operator authorizes another attempt; a still-exhausted account holds again.
                self._event(c, "dispatch.resumed", rid, {"hold_ids": released})
        return self._get(c, "runs", rid)

    @CORE.handles("worker.pause", "worker.pause_after_turn", "worker.resume")
    def _on_worker_control(self, c, name, p, actor):
        # One person against its run: resumed while the run is paused (its own
        # current work only), or paused while it runs. Pause interrupts a running
        # turn and parks it exactly as Pause all does; Pause after turn lets the
        # turn under way finish and starts nothing after it.
        self._role(actor, Role.OPERATOR)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        if not p.get("run_id"):
            current = c.execute("SELECT id FROM runs WHERE state IN ('running','paused') "
                                "ORDER BY created_at DESC,id DESC LIMIT 1").fetchone()
            if current is None:
                raise HubError("INVALID_RUN_TRANSITION", "No running or paused run")
            p["run_id"] = current["id"]
        run = self._get(c, "runs", p["run_id"])
        if run["state"] not in {"running", "paused"}:
            raise HubError("INVALID_RUN_TRANSITION", "Only a running or paused run can pause or resume one person")
        wanted = "resumed" if name == "worker.resume" else "paused"
        after_turn = name == "worker.pause_after_turn"
        if wanted == "resumed":
            if worker["work_intention"] == "retired" or worker["state"] == "retired":
                raise HubError("WORKER_RETIRED", "A retired worker conversation cannot execute again")
            self._execution_environment(c)
            self._require_choices(c)
        # A person already paused keeps the pause it has when asked to pause after its turn.
        if not after_turn or self._effective_run_state(c, run, worker["id"]) == "running":
            c.execute("DELETE FROM worker_controls WHERE worker_id=?", (worker["id"],))
            # Recorded only where it differs from what applies without it, its group's control or
            # else the run. A pause that interrupts also differs from a pause after turns (a
            # group's always is); one after turn differs from any.
            mode = self._group_pause_mode(run, self._group_controls(c, run["id"]), worker["backend"], worker["kind"])
            if mode is not None if wanted == "resumed" else after_turn or mode != "now":
                c.execute("INSERT INTO worker_controls(worker_id,run_id,control,created_at,after_turn) "
                          "VALUES(?,?,?,?,?)", (worker["id"], run["id"], wanted, self.clock(), int(after_turn)))
        if wanted == "resumed":
            # As on Resume all, a claim held before dispatch becomes admissible again.
            c.execute("UPDATE workers SET state='idle' WHERE id=? AND state='suspended' AND work_intention='working' "
                      "AND active_attempt IS NULL AND id IN (SELECT worker_id FROM attempts WHERE state='preflight_held')",
                      (worker["id"],))
        self._event(c, name, worker["id"], {"run_id": run["id"]}, worker_id=worker["id"])
        return {"worker_id": worker["id"], "run_id": run["id"], "run_state": run["state"],
                "control": self._worker_control(c, run["id"], worker["id"]),
                "state": self._effective_run_state(c, run, worker["id"]),
                "pause_mode": self._pause_mode(c, run, worker["id"])}

    @CORE.handles("group.pause_after_turn", "group.resume")
    def _on_group_control(self, c, name, p, actor):
        # A group against its run: all of Codex's or Claude's workers and reviewers, Codex's
        # workers or reviewers alone, or the Integrator, which no other group includes and which
        # may be paused before it exists (CONTROL_GROUPS). Pause after turn lets each member's turn
        # under way finish and starts nothing after it, for members old and new. Resume lets
        # them run, new members and new work included, also while the run is paused.
        self._role(actor, Role.OPERATOR)
        group = p.get("group")
        if group not in CONTROL_GROUPS:
            raise HubError("INVALID_INPUT", "Expected one of the groups " + ", ".join(CONTROL_GROUPS), 422)
        if not p.get("run_id"):
            current = c.execute("SELECT id FROM runs WHERE state IN ('running','paused') "
                                "ORDER BY created_at DESC,id DESC LIMIT 1").fetchone()
            if current is None:
                raise HubError("INVALID_RUN_TRANSITION", "No running or paused run")
            p["run_id"] = current["id"]
        run = self._get(c, "runs", p["run_id"])
        if run["state"] not in {"running", "paused"}:
            raise HubError("INVALID_RUN_TRANSITION", "Only a running or paused run can pause or resume a group")
        wanted = "resumed" if name == "group.resume" else "paused"
        if wanted == "resumed":
            self._execution_environment(c)
            self._require_choices(c)
        backend, kinds = CONTROL_GROUPS[group]
        # The Integrator's group is on any backend (None): its members are its kind's alone.
        members = ("SELECT id FROM workers WHERE (? IS NULL OR backend=?) AND kind IN ("
                   + ",".join("?" * len(kinds)) + ")")
        member_args = (backend, backend, *kinds)
        narrower = [item for item, (other, part) in CONTROL_GROUPS.items() if other == backend and set(part) < set(kinds)]
        wider = [item for item, (other, part) in CONTROL_GROUPS.items() if other == backend and set(kinds) < set(part)]
        # As everyone's do to Pause all and Resume all, the controls of its narrower groups and
        # its members give way to it; a member already paused keeps its pause when the group
        # pauses after turns.
        c.executemany("DELETE FROM group_controls WHERE scope=?", [(item,) for item in (group, *narrower)])
        c.execute("DELETE FROM worker_controls WHERE worker_id IN (" + members + ")"
                  + ("" if wanted == "resumed" else " AND control='resumed'"), member_args)
        # Recorded only where it differs from what applies around it, its wider group's control
        # or else the run; a pause after turns differs from any, as a person's does.
        around = next((control for item, control in self._group_controls(c, run["id"]).items() if item in wider), None)
        if wanted == "paused" or around == "paused" or (around is None and run["state"] == "paused"):
            c.execute("INSERT INTO group_controls(scope,run_id,control,created_at) VALUES(?,?,?,?)",
                      (group, run["id"], wanted, self.clock()))
        if wanted == "resumed":
            # As on Resume, a member's claim held before dispatch becomes admissible again.
            c.execute("UPDATE workers SET state='idle' WHERE state='suspended' AND work_intention='working' "
                      "AND active_attempt IS NULL AND id IN (SELECT worker_id FROM attempts WHERE state='preflight_held') "
                      "AND id IN (" + members + ")", member_args)
            self._restart_ramp()
        self._event(c, name, group, {"run_id": run["id"]})
        return {"group": group, "run_id": run["id"], "run_state": run["state"], "groups": self._groups_view(c, run)}

    def _groups_view(self, c, run):
        """Each group's own control, and whether its members run: 'running', 'paused' (with its
        pause_mode) or, for all of a backend with its workers and reviewers apart, 'mixed'."""
        controls, result = self._group_controls(c, run["id"]), {}
        for group, (backend, kinds) in CONTROL_GROUPS.items():
            states = {self._group_state(run, controls, backend, kind) for kind in kinds}
            modes = {self._group_pause_mode(run, controls, backend, kind) for kind in kinds}
            state = states.pop() if len(states) == 1 else "mixed"
            result[group] = {"control": controls.get(group), "state": state,
                             "pause_mode": modes.pop() if state == "paused" and len(modes) == 1 else None}
        return result

    @CORE.handles("worker.register")
    def _on_worker_register(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR, Role.RUNNER)
        wid = p.get("worker_id") or uid()
        if is_reserved_identity(wid):
            raise HubError("INVALID_ID", "Worker N and Reviewer N are reserved for generated display names", 422)
        kind = p.get("kind", "ordinary")
        if not isinstance(kind, str) or kind not in {"ordinary", "review", "integrator", "dispatcher", "waker"}:
            raise HubError("INVALID_INPUT", "Expected ordinary worker or review actor", 422)
        kept = p.get("worker_number")
        if kept is not None:
            # The first executor of a task that keeps its number (worker_names.kept_worker_number).
            self._role(actor, Role.RUNNER)
            if kind != "ordinary" or not isinstance(kept, int) or isinstance(kept, bool) or not c.execute(
                    "SELECT 1 FROM kept_worker_numbers WHERE worker_number=? AND worker_id IS NULL", (kept,)).fetchone():
                raise HubError("INVALID_INPUT", "Only a task's kept number, not yet taken, is given to a new worker", 422)
        number = (kept if kept is not None else allocate_worker_number(c)) if kind == "ordinary" else None
        reviewer_number = allocate_reviewer_number(c) if kind == "review" else None
        # A worker keeps its backend for life: its native session cannot move between them.
        backend = p.get("backend") or self._creation_backend(c, kind, number)
        if backend not in BACKENDS:
            raise HubError("INVALID_INPUT", "Expected the codex or claude backend", 422)
        if kind != "ordinary" and backend != "codex":
            raise HubError("INVALID_INPUT", "Reviewers, the Integrator and the decision helpers run on Codex", 422)
        self.workspace(wid)
        c.execute("INSERT INTO workers(id,worker_number,reviewer_number,kind,created_at,backend,work_intention,"
                  "rotation_mode) VALUES(?,?,?,?,?,?,'idle',?)",
                  (wid, number, reviewer_number, kind, self.clock(), backend,
                   self.config.assignment.rotation(kind, number)))
        if kept is not None:
            c.execute("UPDATE kept_worker_numbers SET worker_id=? WHERE worker_number=?", (wid, kept))
        return self._get(c, "workers", wid)

    @CORE.handles("runtime.select_backend")
    def _on_runtime_select_backend(self, c, name, p, actor):
        # Applies to workers created from now on, over the assignment plan; existing workers
        # keep theirs (worker.select_backend moves one), and reviewers follow the plan.
        self._role(actor, Role.OPERATOR)
        backend = p.get("backend")
        if backend not in BACKENDS:
            raise HubError("INVALID_INPUT", "Expected the codex or claude backend", 422)
        if backend == "claude":
            from .runtime.claude import claude_unavailable
            problem = claude_unavailable(self.config.claude)
            if problem:
                raise HubError("BACKEND_UNAVAILABLE", problem, 409)
        c.execute("INSERT INTO runtime_preferences(id,new_worker_backend,updated_at) VALUES(1,?,?) "
                  "ON CONFLICT(id) DO UPDATE SET new_worker_backend=excluded.new_worker_backend,"
                  "updated_at=excluded.updated_at", (backend, self.clock()))
        self._event(c, "runtime.backend_selected", "runtime", {"new_worker_backend": backend})
        return self._backend_status(c)

    @CORE.handles("runtime.set_role_model", "runtime.set_concurrency", "worker.select_backend")
    def _on_session_settings(self, c, name, p, actor):
        # Each applies to conversations that begin from now on; running ones keep theirs.
        return self._session_settings_command(c, name, p, actor)

    @CORE.handles("runtime.select_claude_profile", "runtime.set_claude_profile_rotation",
                  "runtime.set_claude_profile_priority")
    def _on_claude_profile(self, c, name, p, actor):
        # Claude sessions, new or continued, use the profile from their next activation.
        return self._claude_profile_command(c, name, p, actor)

    @CORE.handles("runtime.select_codex_profile", "runtime.set_codex_profile_rotation",
                  "runtime.set_codex_profile_priority")
    def _on_codex_profile(self, c, name, p, actor):
        # Codex sessions, new or continued, use the profile from their next activation.
        return self._codex_profile_command(c, name, p, actor)

    @CORE.handles("runtime.set_rotation_mode", "runtime.set_rotation_idle", "worker.set_rotation_mode")
    def _on_rotation_mode(self, c, name, p, actor):
        return self._rotation_command(c, name, p, actor)

    @CORE.handles("runtime.set_integrator_batch")
    def _on_integrator_batch(self, c, name, p, actor):
        return self._batch_rule_command(c, name, p, actor)

    @CORE.handles("runtime.set_answer_deadline")
    def _on_runtime_set_answer_deadline(self, c, name, p, actor):
        # Applies to waits for an answer recorded from now on.
        self._role(actor, Role.OPERATOR)
        minutes = p.get("minutes")
        if type(minutes) is not int or not 1 <= minutes <= 30 * 24 * 60:
            raise HubError("INVALID_INPUT", "Expected a whole number of minutes from 1 to 43200", 422)
        c.execute("INSERT INTO answer_deadline_preferences(id,minutes,updated_at) VALUES(1,?,?) "
                  "ON CONFLICT(id) DO UPDATE SET minutes=excluded.minutes,updated_at=excluded.updated_at",
                  (minutes, self.clock()))
        self._event(c, "runtime.answer_deadline_selected", "runtime", {"minutes": minutes})
        return self._answer_deadline(c)

    def _answer_deadline(self, c) -> dict:
        """How many minutes a wait for an answer lasts: the operator's choice, else project.toml;
        None while neither chose."""
        row = c.execute("SELECT minutes FROM answer_deadline_preferences WHERE id=1").fetchone()
        if row is not None:
            return {"minutes": row["minutes"], "source": "operator"}
        minutes = self.config.waits.answer_deadline_minutes
        return {"minutes": minutes, "source": "config" if minutes is not None else None}

    def _missing_choices(self, c) -> list[str]:
        """What must still be chosen before a run starts, in words: every role's model and effort
        and the answer deadline. None has a default."""
        missing = self._missing_role_choices(c)
        for label, chosen in (("The Dispatcher", self._decision_preferences(c)),
                              ("The Waker", self._waker_preferences(c))):
            missing += [f"{label}: {field}" for field in ("model", "effort") if not chosen[field]]
        if self._answer_deadline(c)["minutes"] is None:
            missing.append("The answer deadline")
        return missing

    def _require_choices(self, c) -> None:
        missing = self._missing_choices(c)
        if missing:
            raise HubError("CHOICES_REQUIRED", "Choose these before a run starts or resumes, in Project setup's "
                           "Agents and sessions panel or in project.toml: " + "; ".join(missing) + ".", 409)

    @CORE.handles("runtime.set_waker_model")
    def _on_runtime_set_waker_model(self, c, name, p, actor):
        # Applies to wake decisions started from now on.
        return self._set_waker_model(c, p, actor)

    @CORE.handles("runtime.set_decision_model")
    def _on_runtime_set_decision_model(self, c, name, p, actor):
        # Applies to assignment and recruitment decisions started from now on.
        self._role(actor, Role.OPERATOR)
        model, effort = p.get("model"), p.get("effort")  # Both chosen: neither has a default.
        if (not isinstance(model, str) or not model.strip() or len(model) > 200
                or any(ch.isspace() for ch in model)):
            raise HubError("INVALID_INPUT", "Expected a model name", 422)
        if effort not in get_args(Effort):
            raise HubError("INVALID_INPUT", "Expected effort low, medium, high, xhigh or max", 422)
        c.execute("INSERT INTO decision_preferences(id,model,effort,updated_at) VALUES(1,?,?,?) "
                  "ON CONFLICT(id) DO UPDATE SET model=excluded.model,effort=excluded.effort,"
                  "updated_at=excluded.updated_at", (model, effort, self.clock()))
        self._event(c, "runtime.decision_model_selected", "runtime", {"model": model, "effort": effort})
        return self._decision_preferences(c)

    @CORE.handles("worker.bind_session")
    def _on_worker_bind_session(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        wid = p["worker_id"]
        worker = self._get(c, "workers", wid)
        changed = int(worker["thread_id"] != p["thread_id"])
        profile = {k: v for k, v in p.items() if k not in {"worker_id", "thread_id", "instructions_sha256"}}
        c.execute("UPDATE workers SET thread_id=?,profile_json=?,context_epoch=context_epoch+?,usage_epoch=usage_epoch+? WHERE id=?", (p["thread_id"], canonical(profile), changed, changed, wid))
        if changed:
            # The instructions a new conversation begins with; a resumed one keeps its own.
            c.execute("UPDATE workers SET instructions_sha256=? WHERE id=?", (p.get("instructions_sha256"), wid))
        delivered = c.execute("SELECT MAX(sequence) FROM deliveries WHERE worker_id=? AND state='delivered'",
                              (wid,)).fetchone()[0] if changed else None
        if delivered:
            # A new conversation starts from the current state (its view, the task's state file),
            # not from the updates its worker was already sent: its cursor carries over.
            c.execute("INSERT INTO deliveries VALUES(?,?,?,?,?,?,?)",
                      (uid(), wid, worker["context_epoch"] + 1, delivered,
                       self.blobs.put_json({"carried_over_through": delivered}), "delivered", self.clock()))
        return self._get(c, "workers", wid)

    @CORE.handles("task.publish")
    def _on_task_publish(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR, Role.WORKER)
        if actor.role == Role.WORKER:
            self._active(c, actor)
        raw_contract = dict(p["contract"])
        if (self.config.collaboration_mode == "collaborative" and self.config.domain == "general"
                and "acceptance_policy" not in raw_contract):
            raw_contract["acceptance_policy"] = "general.peer@1"
        raw_contract = bind_contract(self, c, raw_contract)
        contract = TaskContract.model_validate(raw_contract)
        self._require_contract_text(contract)
        if contract.kind in {"review", "integration"}:
            raise HubError("REVIEW_PROTOCOL", "Review tasks are created only by the immutable review protocol", 422)
        if contract.domain != self.config.domain:
            raise HubError("DOMAIN_MISMATCH", f"Task must use the configured project domain: set the contract's "
                           f"\"domain\" to {self.config.domain!r}, not {contract.domain[:80]!r}", 422)
        from .plugins import load_plugin
        load_plugin(contract.domain).validate_contract(contract)
        body, sha = canonical(contract.model_dump()), digest(contract.model_dump())
        if p.get("deduplicate", True):
            row = c.execute("SELECT t.id FROM tasks t JOIN task_revisions r ON r.task_id=t.id AND r.revision=t.current_revision WHERE r.contract_hash=? AND t.state<>'retired'", (sha,)).fetchone()
            if row:
                return self._task(c, row["id"])
        tid = uid()
        if p.get("task_id") is not None:
            # The operator keeps a task's identity when carrying it from another instance
            # (core_transfer): its references there, in words and in graphs, name it still.
            if actor.role != Role.OPERATOR:
                raise HubError("FORBIDDEN", "Only the operator chooses a new task's id", 403)
            if not isinstance(p["task_id"], str) or not TASK_ID.fullmatch(p["task_id"]):
                raise HubError("INVALID_INPUT", "A task id is a lowercase UUID", 422)
            if c.execute("SELECT 1 FROM tasks WHERE id=?", (p["task_id"],)).fetchone():
                raise HubError("TASK_EXISTS", "A task with this id already exists", 409)
            tid = p["task_id"]
        if p.get("task_number") is not None and actor.role != Role.OPERATOR:
            raise HubError("FORBIDDEN", "Only the operator chooses a new task's number", 403)
        number = (None if contract.kind in INTERNAL_KINDS
                  else allocate_task_number(c, p.get("task_number")))  # core_transfer keeps its own.
        c.execute("INSERT INTO tasks(id,current_revision,state,created_at,task_number) VALUES(?,1,'ready',?,?)",
                  (tid, self.clock(), number))
        c.execute("INSERT INTO task_revisions VALUES(?,1,?,?)", (tid, body, sha))
        origin = self._active(c, actor) if actor.role == Role.WORKER else None
        c.execute("INSERT INTO task_origins VALUES(?,?,?,?,?)", (
            tid, origin[1]["id"] if origin else None, origin[0]["worker_id"] if origin else None,
            origin[0]["id"] if origin else None, self.clock()))
        self._persist_task_content(c, tid, 1, contract.model_dump())
        self._init_collaboration(c, tid, actor)
        c.execute("INSERT INTO search_index VALUES(?,?,?,?)", (tid, "task", contract.title, contract.objective))
        self._event(c, "task.published", tid, task_id=tid)
        return self._task(c, tid)

    @CORE.handles("task.claim")
    def _on_task_claim(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        return self._claim(c, p["task_id"], p["revision"], p["worker_id"], p["run_id"],
                           contribution_plan=p.get("plan", ""))

    @CORE.handles("task.checkpoint")
    def _on_task_checkpoint(self, c, name, p, actor):
        a, _ = self._active(c, actor)
        checkpoint = Checkpoint.model_validate(p["checkpoint"])
        c.execute("UPDATE attempts SET checkpoint_json=? WHERE id=?", (canonical(checkpoint.model_dump()), a["id"]))
        return {"attempt_id": a["id"], "checkpoint": checkpoint.model_dump()}

    @CORE.handles("task.wait")
    def _on_task_wait(self, c, name, p, actor):
        a, task = self._active(c, actor)
        raw = p.get("predicate")
        if isinstance(raw, dict) and raw.get("requirements"):
            raise HubError("PAUSE_REQUIREMENTS_REMOVED", "A pause no longer takes requirements. To wait for another "
                           "task's results, subscribe to it: {\"subscriptions\": [{\"task_id\": \"<id>\", \"need\": "
                           "\"<the results you await>\"}]}", 422)
        if isinstance(raw, dict) and "requirements" in raw:
            raw = {key: value for key, value in raw.items() if key != "requirements"}
        checkpoint, predicate = Checkpoint.model_validate(p["checkpoint"]), Predicate.model_validate(raw or {})
        if not predicate.has_condition():
            raise HubError("WAIT_CONDITION_REQUIRED", "A pause needs what ends it: a subscription to another task "
                           "(subscriptions), your own submission's result (validations, publications, "
                           "completion_declaration_id), or "
                           "an answer to your question (answers: a worker's; external_response_id: the operator's). If "
                           "nothing is left for you to do in this task, leave it (leave_task) instead; while another "
                           "executor remains, it can invite you back (invite_back).", 422)
        self._check_subscriptions(c, task, a["worker_id"], predicate.subscriptions)
        self._check_questions(c, a["worker_id"], predicate.answers)
        self._check_publications(c, task, predicate.publications)
        return self._record_wait(c, a, task, checkpoint.model_dump(), predicate.model_dump())

    def _record_wait(self, c, a, task, checkpoint: dict, predicate: dict) -> dict:
        """Record attempt `a`'s wait on `predicate`, already checked (stored waits recorded before
        2026-11-07 may name requirements, or nothing)."""
        cid = uid()
        state = "eligible" if self._satisfied(c, predicate, waiter=a["worker_id"]) else "waiting"
        # The new wait states the whole wake condition: it replaces the worker's earlier ones here.
        c.execute("UPDATE continuations SET state='superseded_by_worker' WHERE preferred_worker=? AND task_id=? "
                  "AND state IN ('waiting','eligible')", (a["worker_id"], a["task_id"]))
        c.execute(
            "INSERT INTO continuations(id,task_id,task_revision,preferred_worker,predicate_json,checkpoint_json,state,origin_attempt,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (cid, a["task_id"], a["task_revision"], a["worker_id"], canonical(predicate), canonical(checkpoint), state, a["id"], self.clock()),
        )
        c.execute("UPDATE attempts SET state='draining',checkpoint_json=?,continuation_id=? WHERE id=?", (canonical(checkpoint), cid, a["id"]))
        c.execute("UPDATE workers SET state='draining' WHERE id=?", (a["worker_id"],))
        deadline = self._answer_deadline(c)["minutes"]
        if deadline is not None and (predicate.get("answers") or predicate.get("external_response_id")):
            c.execute("INSERT INTO wait_deadlines VALUES(?,?)", (cid, self.clock() + deadline * 60_000))
        self._event(c, "continuation.created", cid, {"state": state}, task["id"], a["worker_id"])
        return {"runtime_directive": "drain_for_wait", "continuation_id": cid, "attempt_id": a["id"]}

    @staticmethod
    def _check_questions(c, worker_id, question_ids):
        """Each awaited answer is to a question this worker asked."""
        for question_id in question_ids:
            if c.execute("SELECT 1 FROM peer_messages m JOIN peer_message_kinds k ON k.message_id=m.id "
                         "AND k.kind='question' WHERE m.id=? AND m.sender_id=?", (question_id, worker_id)).fetchone() is None:
                raise HubError("QUESTION_NOT_FOUND", f"{question_id} is not a question you asked. Ask it with "
                               "talk_send (kind question), then wait on the id its receipt returns.", 422)

    def _check_publications(self, c, task, candidate_ids):
        """Each awaited candidate belongs to the waiter's own task, whose author or co-executor it
        is, and is still on its way to the Library."""
        for candidate_id in candidate_ids:
            binding = c.execute("SELECT task_id FROM candidate_bindings WHERE artifact_id=?", (candidate_id,)).fetchone()
            if binding is None or binding["task_id"] != task["id"]:
                raise HubError("PUBLICATION_NOT_OWN", f"{candidate_id} is not a candidate of this task. Wait only on "
                               "a candidate staged in your own task; for another task's results, subscribe to it.", 422)
            outcome = self._publication_outcome(c, candidate_id)
            if outcome is not None:
                raise HubError("PUBLICATION_SETTLED", outcome[1] + " Its wait would end at once.", 409)

    @staticmethod
    def _answered(c, question_id) -> bool:
        return c.execute("SELECT 1 FROM peer_message_kinds WHERE answers=? LIMIT 1", (question_id,)).fetchone() is not None

    def _wake_answered(self, c, question_id):
        """The question `question_id` has an answer: the waits on it that now hold end, as _wake ends
        them, without reading every other wait."""
        for row in c.execute("SELECT DISTINCT k.* FROM continuations k, json_each(k.predicate_json,'$.answers') a "
                             "WHERE k.state='waiting' AND a.value=?", (question_id,)).fetchall():
            predicate = json.loads(row["predicate_json"])
            if self._satisfied(c, predicate, waiter=row["preferred_worker"]):
                self._end_wait(c, row, predicate)

    async def end_overdue_waits(self) -> int:
        """Waits for an answer still unanswered at their deadline end, and their workers reconsider.
        The driver asks on every pass; this reads the database at most once a minute."""
        if time.monotonic() < self._overdue_check_at:
            return 0
        self._overdue_check_at = time.monotonic() + 60

        def end(c):
            rows = c.execute("SELECT k.id,k.task_id FROM wait_deadlines d JOIN continuations k ON k.id=d.continuation_id "
                             "WHERE d.due_at<=? AND k.state='waiting'", (self.clock(),)).fetchall()
            for row in rows:
                c.execute("UPDATE continuations SET state='reconsider',wake_reason='answer_deadline' WHERE id=?", (row["id"],))
                self._notify(c, row["task_id"], row["id"], "answer_deadline")
            return len(rows)
        return await self.db.call(end, write=True)

    def _check_subscriptions(self, c, task, worker_id, subscriptions):
        """Each subscribed task exists and can still publish, and no subscription closes a cycle of
        tasks waiting on each other (wait_graph)."""
        if not subscriptions:
            return
        titles = {}

        def title(task_id):
            if task_id not in titles:
                row = c.execute("SELECT json_extract(v.contract_json,'$.title') FROM tasks t JOIN task_revisions v "
                                "ON v.task_id=t.id AND v.revision=t.current_revision WHERE t.id=?", (task_id,)).fetchone()
                titles[task_id] = row[0] if row else "?"
            return f"{task_id} ({titles[task_id]})"
        edges = None
        for subscription in subscriptions:
            target = self._task(c, subscription.task_id)
            if target["id"] == task["id"]:
                raise HubError("SUBSCRIPTION_CYCLE", "A task cannot wait on itself: for your own submission's result "
                               "wait on its validations or completion declaration, and ask a collaborator a question "
                               "for anything else.", 422)
            if target["state"] in {"done", "retired"}:
                raise HubError("SUBSCRIPTION_CLOSED", f"Task {title(target['id'])} is {target['state']} and publishes "
                               "nothing more: its results are in the Library or nowhere.", 422)
            if edges is None:
                edges = wait_edges(c, replacing=(worker_id, task["id"]))
            back = path_between(edges, target["id"], task["id"])
            if back is not None:
                cycle = " -> ".join(title(node) for node in [task["id"], *back])
                raise HubError("SUBSCRIPTION_CYCLE", "This subscription would close a cycle of tasks waiting on each "
                               f"other, so none of them could go on: {cycle}. Ask the workers involved instead, or "
                               "work on another part.", 422)
            edges.setdefault(task["id"], set()).add(target["id"])

    @CORE.handles("artifact.submit")
    def _on_artifact_submit(self, c, name, p, actor):
        a, task = self._active(c, actor)
        if "completion" in p:
            raise HubError("INVALID_INPUT", "Contribution does not declare completion; use the task completion protocol", 422)
        self._normalize_worker_fields(c, p, "coauthors")
        self._valid_inputs(c, task["contract"])
        for ref in json.loads(a["input_refs_json"]):
            if self._get(c, "artifacts", ref)["status"] == "withdrawn":
                raise HubError("INPUTS_CHANGED", "A bound attempt input was withdrawn")
        if task["contract"]["kind"] in {"review"}:
            raise HubError("REVIEW_PROTOCOL", "Review tasks must submit protocol receipts")
        from .plugins import load_plugin
        publication_paths = self._publication_paths(
            task, p, load_plugin(task["contract"]["domain"]))
        self._require_source_files(self._candidate_snapshot(a["worker_id"]), publication_paths)
        submission = dict(p)
        c.execute("UPDATE attempts SET state='draining',submission_json=? WHERE id=?", (canonical(submission), a["id"]))
        c.execute("UPDATE workers SET state='draining' WHERE id=?", (a["worker_id"],))
        return {"runtime_directive": "drain_for_submission", "attempt_id": a["id"]}

    @CORE.handles("runtime.quiescent")
    def _on_runtime_quiescent(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        w = self._get(c, "workers", p["worker_id"])
        a = self._get(c, "attempts", w["active_attempt"])
        if a["state"] != "draining" or a["ended_at"] is not None:
            raise HubError("NOT_DRAINING", "Only a draining attempt can surrender its slot")
        snapshot = p["snapshot"]
        manifest = self.blobs.get_json(snapshot)
        if not isinstance(manifest, dict) or "files" not in manifest:
            raise HubError("INVALID_SNAPSHOT", "Workspace manifest required", 422)
        if self._workspace_snapshot(w["id"]) != snapshot:
            raise HubError("SNAPSHOT_CHANGED", "Snapshot must match the quiet worker workspace")
        task = self._task(c, a["task_id"])
        current = task["revision"] == a["task_revision"] and task["state"] != "retired"
        c.execute("UPDATE attempts SET snapshot=?,ended_at=? WHERE id=?", (snapshot, self.clock(), a["id"]))
        c.execute("UPDATE workers SET active_attempt=NULL,state='idle' WHERE id=?", (w["id"],))
        self._refresh_aggregate(c, task["id"])
        stop = c.execute("SELECT reason FROM attempt_stop_requests WHERE attempt_id=?", (a["id"],)).fetchone()
        if not current or stop:
            reason = stop["reason"] if stop else "superseded"
            c.execute("UPDATE attempts SET state='superseded' WHERE id=?", (a["id"],))
            self._preserve_member(c, a, snapshot, runnable=reason == "task_revision_changed", reason=reason)
            if task["state"] != "retired":
                self._recompute_task(c, task["id"])
            return {"directive": "superseded", "snapshot": snapshot, "reason": reason}
        if a["submission_json"]:
            submission = json.loads(a["submission_json"])
            if "integration_batch" in submission:
                return {**self._finish_integration_batch(c, a, submission["integration_batch"]), "snapshot": snapshot}
            if "review" in submission:
                self._preserve_member(c, a, snapshot, reason="review_submitted")
                return {**self._commit_review(c, a, task, submission["review"]), "snapshot": snapshot}
            self._preserve_member(c, a, snapshot, reason="awaiting_peer_review")
            try:
                staged = self._stage_candidate(c, a, task, submission, snapshot)
            except HubError as error:
                if error.code not in {"INPUTS_CHANGED", "PREREQUISITES_MISSING"}:
                    raise
                c.execute("UPDATE attempts SET state='blocked' WHERE id=?", (a["id"],))
                self._notify(c, task["id"], a["id"], "basis_changed_during_drain")
                self._recompute_task(c, task["id"])
                return {"directive": "needs_attention", "snapshot": snapshot, "reason": error.code}
            c.execute("UPDATE attempts SET state='submitted' WHERE id=?", (a["id"],))
            held = self._record_contribution_validation(c, a["id"], staged["artifact_id"])
            self._recompute_task(c, task["id"])
            result = {"directive": "verify", "artifact_id": staged["artifact_id"],
                      "review_task_id": staged["review_task_id"], "snapshot": snapshot,
                      "attempt_id": a["id"]}
            if held:
                result.update(directive="validation_held", validation_pending=True, reason=held)
            return result
        if not a["continuation_id"]:
            raise HubError("INVALID_TRANSITION", "No durable continuation or submission")
        c.execute("UPDATE continuations SET snapshot=? WHERE id=?", (snapshot, a["continuation_id"]))
        c.execute("UPDATE attempts SET state='suspended' WHERE id=?", (a["id"],))
        c.execute("UPDATE workers SET state='suspended' WHERE id=?", (w["id"],))
        self._preserve_member(c, a, snapshot, reason="dependency_wait")
        c.execute("UPDATE tasks SET state='waiting' WHERE id=? AND active_attempt IS NULL", (task["id"],))
        self._recompute_task(c, task["id"])
        self._wake(c)
        self._event(c, "worker.suspended", w["id"], task_id=task["id"], worker_id=w["id"])
        return {"directive": "waiting", "continuation_id": a["continuation_id"], "snapshot": snapshot}

    @CORE.handles("continuation.resume")
    def _on_continuation_resume(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        continuation = self._get(c, "continuations", p["continuation_id"])
        origin = self._get(c, "attempts", continuation["origin_attempt"])
        if continuation["state"] not in {"eligible", "reconsider"} or origin["ended_at"] is None or not continuation["snapshot"]:
            raise HubError("CONTINUATION_NOT_READY", "Continuation is not eligible and quiescent")
        task = self._task(c, continuation["task_id"])
        if task["revision"] != continuation["task_revision"]:
            raise HubError("STALE_REVISION", "Continuation belongs to an earlier task revision")
        if continuation["state"] != "reconsider" and not self._satisfied(
                c, json.loads(continuation["predicate_json"]), waiter=continuation["preferred_worker"]):
            raise HubError("PREREQUISITES_MISSING", "Wait predicate no longer holds")
        wid = p.get("worker_id") or continuation["preferred_worker"]
        if wid != continuation["preferred_worker"]:
            raise HubError("RESTORE_REQUIRED", "Version 0.1 resumes on the original durable worker")
        c.execute("UPDATE tasks SET state='ready' WHERE id=?", (task["id"],))
        result = self._claim(c, task["id"], task["revision"], wid, p["run_id"], continuation["id"])
        c.execute("UPDATE continuations SET state='resumed',resume_attempt=? WHERE id=?",
                  (result["id"], continuation["id"]))
        c.execute("UPDATE attempts SET checkpoint_json=?,snapshot=? WHERE id=?",
                  (continuation["checkpoint_json"], continuation["snapshot"], result["id"]))
        result.update(checkpoint=json.loads(continuation["checkpoint_json"]),
                      snapshot=continuation["snapshot"], resume_reason=(continuation["wake_reason"] if continuation["state"] == "reconsider" else "requirements_satisfied"))
        self._event(c, "continuation.resumed", continuation["id"], task_id=task["id"], worker_id=wid)
        return result

    @CORE.handles("interruption.import", "runtime.park_interrupted")
    def _on_interruption_park(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR if name == "interruption.import" else Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        return self._park_interruption(c, p, imported=name == "interruption.import")

    @CORE.handles("interruption.resume")
    def _on_interruption_resume(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        ticket = self._get(c, "interruption_checkpoints", p["interruption_id"])
        if ticket["state"] != "parked":
            raise HubError("INTERRUPTION_NOT_READY", "Interruption ticket is not parked")
        origin = self._get(c, "attempts", ticket["origin_attempt"])
        worker = self._get(c, "workers", ticket["worker_id"])
        task = self._task(c, ticket["task_id"])
        if task["revision"] != ticket["task_revision"] or task["state"] == "retired":
            raise HubError("STALE_REVISION", "Interrupted task revision is no longer current")
        if worker["active_attempt"] or worker["state"] != "suspended":
            raise HubError("ALREADY_CLAIMED", "Original task or worker is not available")
        if worker["thread_id"] != ticket["thread_id"] or worker["usage_epoch"] != ticket["usage_epoch"]:
            raise HubError("SESSION_CHANGED", "The original session or usage epoch changed")
        if (not codex_profiles_equal(self.config.runtime_profile(worker["backend"]),
                                     json.loads(ticket["codex_profile_json"]))
                or not codex_profiles_equal(json.loads(worker["profile_json"]).get("codex_profile"),
                                            json.loads(ticket["codex_profile_json"]))):
            raise HubError("PROFILE_CHANGED", "Resume requires the interrupted native version and profile")
        self._run_controls(c, p["run_id"], worker["id"])
        refs = self._interruption_inputs(c, origin, task)
        if refs != json.loads(ticket["input_refs_json"]):
            raise HubError("INPUTS_CHANGED", "The original frozen input binding changed")
        workspace = self.workspace(worker["id"]).resolve()
        if str(workspace) != ticket["workspace"] or self._workspace_snapshot(worker["id"]) != ticket["snapshot"]:
            raise HubError("SNAPSHOT_CHANGED", "Original workspace differs from its interruption snapshot")
        c.execute("UPDATE tasks SET state='ready' WHERE id=?", (task["id"],))
        result = self._claim(c, task["id"], task["revision"], worker["id"], p["run_id"],
                             origin["continuation_id"], ticket["id"], refs)
        c.execute("UPDATE attempts SET checkpoint_json=?,snapshot=? WHERE id=?",
                  (ticket["checkpoint_json"], ticket["snapshot"], result["id"]))
        c.execute("UPDATE interruption_checkpoints SET state='resumed',resume_attempt=? WHERE id=?",
                  (result["id"], ticket["id"]))
        result.update(snapshot=ticket["snapshot"], checkpoint=json.loads(ticket["checkpoint_json"]),
                      interruption_id=ticket["id"], resume_reason=ticket["stop_reason"])
        self._event(c, "interruption.resumed", ticket["id"], {"attempt_id": result["id"], "run_id": p["run_id"]},
                    task["id"], worker["id"])
        return result

    @CORE.handles("artifact.register")
    def _on_artifact_register(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR, Role.WORKER)
        if actor.role == Role.WORKER:
            return self._collaboration_command(c, "artifact.stage", p, actor)
        kind = p.get("kind", "")
        if isinstance(kind, str) and any(part in {"note", "notes", "wiki"} for part in kind.lower().split(".")):
            raise HubError("NOTE_FILES_REQUIRED", "Write notes in files; public notes require a source-file submission and independent review", 422)
        if self.config.domain == "lean":
            # Direct registration is for schema-checked operator records,
            # such as execution environments, not a second note interface.
            from ..schemas import OBJECT_ADAPTER
            record = OBJECT_ADAPTER.validate_python(p["data"])
            if record.object_type != "environment":
                raise HubError("SOURCE_SUBMISSION_REQUIRED", "Operator registration is limited to execution environments; authored records require source-file peer review", 422)
            if kind != "lean." + record.object_type:
                raise HubError("NOTE_FILES_REQUIRED", "Authored notes must use the file submission workflow", 422)
        return self._insert_artifact(c, p["kind"], p["data"],
                                     producer=actor.worker_id or "operator",
                                     input_refs=p.get("input_refs", []))

    @CORE.handles("artifact.record_replacement")
    def _on_artifact_record_replacement(self, c, name, p, actor):
        """The operator records that an accepted artifact replaces a rejected or withdrawn one of
        the same content: a requirement naming the original is then met by the replacement. In the
        Linux pilot a statement rejected for one Markdown remark was resubmitted with the same Lean
        without naming the one it replaced; the task that had required the first could never be
        completed, since its original requirements must stay satisfied."""
        self._role(actor, Role.OPERATOR)
        original, replacement = self._get(c, "artifacts", p.get("original_id")), self._get(c, "artifacts", p.get("replacement_id"))
        reason = p.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 2000:
            raise HubError("INVALID_INPUT", "Record why the replacement stands for the original", 422)
        if original["id"] == replacement["id"] or original["kind"] != replacement["kind"]:
            raise HubError("INVALID_INPUT", "The replacement must be another artifact of the same kind", 422)
        if original["status"] not in {"rejected", "withdrawn"}:
            raise HubError("ORIGINAL_NOT_REPLACEABLE", "Only a rejected or withdrawn artifact can be replaced", 409)
        if replacement["status"] != "accepted":
            raise HubError("REPLACEMENT_NOT_ACCEPTED", "The replacement needs its own independent acceptance", 409)
        c.execute("INSERT INTO artifact_replacements(original_id,replacement_id,reason,recorded_at) VALUES(?,?,?,?) "
                  "ON CONFLICT(original_id) DO UPDATE SET replacement_id=excluded.replacement_id,"
                  "reason=excluded.reason,recorded_at=excluded.recorded_at",
                  (original["id"], replacement["id"], reason.strip(), self.clock()))
        self._event(c, "artifact.replacement_recorded", original["id"],
                    {"replacement_id": replacement["id"], "reason": reason.strip()}, original["task_id"])
        self._wake(c)  # Waits on the original may now be met.
        return {"original_id": original["id"], "replacement_id": replacement["id"]}

    @CORE.handles("artifact.accept")
    def _on_artifact_accept(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR, Role.VALIDATOR)
        from .models import ValidationReport
        report = ValidationReport.model_validate({
            k: p[k] for k in ("policy", "verdict", "details", "qualifications") if k in p})
        artifact = self._artifact(c, p["artifact_id"])
        if artifact["status"] == "private":
            raise HubError("PRIVATE_RECORD", "Private diagnostics cannot acquire public acceptance")
        if artifact["status"] == "withdrawn":
            raise HubError("ARTIFACT_WITHDRAWN", "Withdrawn objects cannot be accepted again")
        if any(self._get(c, "artifacts", ref)["status"] == "withdrawn" for ref in artifact["input_refs"]):
            raise HubError("INPUTS_CHANGED", "Candidate uses withdrawn inputs")
        binding = (self._candidate_binding(c, artifact["id"])
                   if "candidate_binding" in artifact else None)
        if binding:
            if not self._basis_valid(c, binding):
                raise HubError("INPUTS_CHANGED", "Candidate basis or task revision changed")
            report.details = {**report.details, "candidate_binding": self._review_basis(binding)}
        eid = uid()
        c.execute(
            "INSERT INTO evidence(id,artifact_id,policy,verdict,details_json,qualifications_json,issuer,created_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (eid, artifact["id"], report.policy, report.verdict, canonical(report.details),
             canonical(report.qualifications), str(actor.role), self.clock()))
        accepted = report.verdict == "accepted" and not report.qualifications
        # A failed attempt to obtain evidence does not retract a prior
        # result or permanently withdraw the unchanged candidate. Keep the
        # unavailable receipt for inspection and allow a fresh validation.
        # A later genuine rejection must still invalidate dependencies.
        inconclusive = {"unavailable", "needs_operator"}
        previous = next((item for item in reversed(artifact["evidence"])
                         if item["policy"] == report.policy
                         and item["verdict"] not in inconclusive), None)
        retracted = (previous is not None and previous["verdict"] == "accepted"
                     and not previous["qualifications"] and not accepted
                     and report.verdict not in inconclusive)
        if retracted:
            self._withdraw(c, artifact["id"], "validation_receipt_retracted")
        elif binding and report.verdict == "rejected":
            self._close_rejected_candidate(c, artifact["id"])
        elif binding:
            self._promote_candidate(c, artifact["id"])
        else:
            evidence_valid = c.execute(
                "SELECT 1 FROM evidence e WHERE e.artifact_id=? AND e.verdict='accepted' "
                "AND e.qualifications_json='[]' AND e.sequence=(SELECT MAX(e2.sequence) FROM evidence e2 "
                "WHERE e2.artifact_id=e.artifact_id AND e2.policy=e.policy) LIMIT 1", (artifact["id"],)).fetchone()
            c.execute("UPDATE artifacts SET status=? WHERE id=?",
                      ("accepted" if evidence_valid else "candidate", artifact["id"]))
        if artifact["task_id"]:
            task = self._task(c, artifact["task_id"])
            if (self._same_substance(c, task, artifact["task_revision"], artifact["contract_hash"])
                    and report.policy == task["contract"]["acceptance_policy"]
                    and task["state"] != "retired"):
                self._recompute_task(c, task["id"])
                if not accepted:
                    self._notify(c, task["id"], artifact["id"], "validation_" + report.verdict)
        self._event(c, "artifact." + report.verdict, artifact["id"],
                    {"policy": report.policy, "evidence_id": eid}, artifact["task_id"])
        # A wait for this validation's result ends whatever the verdict.
        self._wake(c)
        if not accepted:
            self._refresh_waits(c)
        state = self._get(c, "artifacts", artifact["id"])["status"]
        return {"id": artifact["id"], "artifact_id": artifact["id"], "status": state,
                "accepted": state == "accepted", "evidence_id": eid,
                "evidence": [{"id": eid, "policy": report.policy, "verdict": report.verdict,
                              "details": report.details, "qualifications": report.qualifications}]}

    @CORE.handles("task.retire")
    def _on_task_retire(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        task = self._task(c, p["task_id"])
        if "revision" in p and p["revision"] != task["revision"]:
            raise HubError("STALE_REVISION", "Expected current revision")
        self._expected_revision(task, p)
        c.execute("UPDATE tasks SET state='retired',state_version=state_version+1 WHERE id=?", (task["id"],))
        if self._active_attempts(c, task["id"]):
            self._drain_task(c, task["id"], name)
        self._invalidate_reviews(c, task["id"], name)
        c.execute("UPDATE workers SET state='idle' WHERE state='suspended' AND id IN "
                  "(SELECT preferred_worker FROM continuations WHERE task_id=? AND state IN ('waiting','eligible','reconsider'))",
                  (task["id"],))
        c.execute("UPDATE continuations SET state='superseded' WHERE task_id=? AND state IN ('waiting','eligible','reconsider')",
                  (task["id"],))
        self._invalidate_interruptions(c, task["id"], "superseded")
        self._notify(c, task["id"], task["id"], name)
        self._event(c, name, task["id"], task_id=task["id"])
        return self._task(c, task["id"])

    @CORE.handles("turn.reserve")
    def _on_turn_reserve(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        a = self._get(c, "attempts", worker["active_attempt"])
        if a["state"] != "running" or a["ended_at"] is not None:
            raise HubError("NOT_RUNNING", "No running attempt to admit")
        if self.config.repository.enabled and not c.execute(
                "SELECT 1 FROM attempt_workspaces WHERE attempt_id=?", (a["id"],)).fetchone():
            raise HubError("WORKSPACE_NOT_PREPARED", "Finish the reserved workspace checkout before starting a model turn")
        self._active(c, Actor(role=Role.WORKER, worker_id=worker["id"],
                              attempt_id=a["id"], fencing_token=a["fencing_token"]))
        self._run_controls(c, a["run_id"], worker["id"])
        intent = uid()
        # The mode in force is recorded with every turn, for comparing the modes over time.
        c.execute("INSERT INTO turn_intents(id,worker_id,attempt_id,run_id,state,created_at,thread_id,rotation_mode)"
                  " VALUES(?,?,?,?,'prepared',?,?,?)",
                  (intent, worker["id"], a["id"], a["run_id"], self.clock(), worker["thread_id"],
                   self._rotation_mode(c, worker)))
        current_usage = c.execute("SELECT total FROM usage_samples WHERE worker_id=? AND epoch=?",
                                  (worker["id"], worker["usage_epoch"])).fetchone()
        c.execute("INSERT INTO turn_usage_totals(intent_id,worker_id,epoch,start_total,end_total,updated_at) "
                  "VALUES(?,?,?,?,NULL,?)",
                  (intent, worker["id"], worker["usage_epoch"], current_usage[0] if current_usage else 0, self.clock()))
        c.execute("UPDATE runs SET turns_used=turns_used+1 WHERE id=?", (a["run_id"],))
        return self._get(c, "turn_intents", intent)

    @CORE.handles("turn.bind")
    def _on_turn_bind(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        intent = self._get(c, "turn_intents", p["intent_id"])
        if intent["state"] != "prepared":
            raise HubError("TURN_ALREADY_BOUND", "Turn admission is not waiting for binding")
        c.execute("UPDATE turn_intents SET remote_turn_id=?,state='bound' WHERE id=?",
                  (p["remote_turn_id"], intent["id"]))
        return self._get(c, "turn_intents", intent["id"])

    @CORE.handles("turn.not_sent")
    def _on_turn_not_sent(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        intent = self._get(c, "turn_intents", p["intent_id"])
        if p.get("inference_dispatched") is not False or p.get("process_quiet") is not True:
            raise HubError("UNSENT_EVIDENCE_REQUIRED", "Confirm no native input was dispatched and owned processes are quiet")
        reason = p.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 8192:
            raise HubError("EVIDENCE_REQUIRED", "Record the exact local failure before native dispatch", 422)
        if intent["worker_id"] != p.get("worker_id") or intent["attempt_id"] != p.get("attempt_id"):
            raise HubError("TURN_BINDING_MISMATCH", "Unsent evidence must identify the exact worker and attempt")
        if intent["state"] == "not_sent":
            return intent
        worker = self._get(c, "workers", intent["worker_id"])
        attempt = self._get(c, "attempts", intent["attempt_id"])
        if (intent["state"] != "prepared" or intent["remote_turn_id"] is not None
                or worker["active_attempt"] != attempt["id"] or attempt["ended_at"] is not None
                or worker["thread_id"] != p.get("thread_id")):
            raise HubError("TURN_ALREADY_DISPATCHED", "Only the original active, unbound preparation may be marked not sent")
        deliveries = p.get("deliveries", [])
        if (not isinstance(deliveries, list) or len(deliveries) > 2
                or any(not isinstance(item, dict) or item.get("kind") not in {"peer", "human"}
                       or not isinstance(item.get("delivery_id"), str) for item in deliveries)):
            raise HubError("INVALID_INPUT", "Unsent delivery evidence must identify at most two exact inbox packets", 422)
        for delivery in deliveries:
            hold = c.execute("SELECT * FROM message_delivery_holds WHERE kind=? AND delivery_id=?",
                             (delivery["kind"], delivery["delivery_id"])).fetchone()
            if hold is None:
                continue  # Preparation can fail before this packet's guard is persisted.
            if (hold["worker_id"] != intent["worker_id"] or hold["input_id"] != p.get("input_id")
                    or hold["thread_id"] != p.get("thread_id") or hold["turn_id"] is not None
                    or hold["state"] not in {"sending", "not_delivered"}):
                raise HubError("DELIVERY_CONFLICT", "Unsent evidence cannot resolve another input or an uncertain native send")
            c.execute("UPDATE message_delivery_holds SET state='not_delivered',reason=? WHERE kind=? AND delivery_id=?",
                      (reason, delivery["kind"], delivery["delivery_id"]))
            if hold["input_id"]:
                c.execute("INSERT OR REPLACE INTO delivery_transcript_updates VALUES(?,?,?,?,?,?,0)",
                          (delivery["kind"], delivery["delivery_id"], hold["input_id"], "not_delivered", None, reason))
        c.execute("UPDATE turn_intents SET state='not_sent' WHERE id=?", (intent["id"],))
        self._refresh_message_wake(c, intent["worker_id"])
        self._event(c, "turn.not_sent", intent["id"], {"reason": reason,
                    "attempt_id": intent["attempt_id"], "input_id": p.get("input_id"),
                    "process_quiet": True, "inference_dispatched": False}, worker_id=intent["worker_id"])
        return self._get(c, "turn_intents", intent["id"])

    @CORE.handles("turn.finish")
    def _on_turn_finish(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        intent = self._get(c, "turn_intents", p["intent_id"])
        status = p["status"]
        if status not in {"completed", "interrupted", "failed", "cancelled", "unknown"}:
            raise HubError("INVALID_INPUT", "Unknown turn status", 422)
        if intent["state"] not in {"prepared", "bound", "sending", "uncertain"}:
            raise HubError("TURN_FINISHED", "Turn is already finished")
        state = "uncertain" if status == "unknown" else status
        c.execute("UPDATE turn_intents SET state=? WHERE id=?", (state, intent["id"]))
        c.execute("UPDATE turn_usage_totals SET end_total=COALESCE((SELECT total FROM usage_samples u "
                  "WHERE u.worker_id=turn_usage_totals.worker_id AND u.epoch=turn_usage_totals.epoch),0),updated_at=? "
                  "WHERE intent_id=?", (self.clock(), intent["id"]))
        self._event(c, "turn.finished", intent["id"], {
            "status": status, "terminal_observed": p.get("terminal_observed") is True,
            "process_quiet": p.get("process_quiet") is True}, worker_id=intent["worker_id"])
        if state == "uncertain":
            self._hold_worker_usage(c, intent["worker_id"], "Native turn outcome is unknown")
        return self._get(c, "turn_intents", intent["id"])

    @CORE.handles("usage.record")
    def _on_usage_record(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        if not worker["run_id"]:
            raise HubError("NO_RUN", "Worker is not associated with a run")
        sample = p["sample"]
        if isinstance(sample, dict) and "tokenUsage" in sample:
            sample = sample["tokenUsage"]
        total_sample = sample.get("total", sample)
        total = total_sample.get("totalTokens")
        if type(total) is not int or total < 0:
            raise HubError("INVALID_USAGE", "A nonnegative cumulative totalTokens is required", 422)
        epoch = p.get("epoch", worker["usage_epoch"])
        if epoch != worker["usage_epoch"]:
            raise HubError("STALE_USAGE", "Usage belongs to an old thread binding")
        prior = c.execute("SELECT total,sample_json FROM usage_samples WHERE worker_id=? AND epoch=?",
                          (worker["id"], epoch)).fetchone()
        previous = prior["total"] if prior else None
        if previous is not None and total < previous:
            self._hold_worker_usage(c, worker["id"], "Native cumulative usage regressed")
            return {"uncertain": True, "charged_tokens": 0, "observed_total": total, "previous_total": previous}
        delta = total - (previous or 0)
        c.execute("INSERT INTO usage_samples VALUES(?,?,?,?,?) ON CONFLICT(worker_id,epoch) DO UPDATE SET "
                  "total=excluded.total,sample_json=excluded.sample_json,created_at=excluded.created_at "
              "WHERE usage_samples.total<>excluded.total OR usage_samples.sample_json<>excluded.sample_json",
                  (worker["id"], epoch, total, canonical(sample), self.clock()))
        self._charge_backend(c, worker["run_id"], worker["backend"], delta)
        self._record_turn_usage(c, worker["id"], sample, json.loads(prior["sample_json"]) if prior else None)
        run = self._get(c, "runs", worker["run_id"])
        return {"uncertain": False, "charged_tokens": delta, "total_tokens": run["tokens_used"],
                "backend": worker["backend"], "backend_tokens": run[worker["backend"] + "_tokens_used"]}

    @CORE.handles("runtime.abandon")
    def _on_runtime_abandon(self, c, name, p, actor):
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        if p.get("quiescent") is not True:
            raise HubError("QUIESCENCE_REQUIRED", "Owned processes must be stopped before recovery")
        worker = self._get(c, "workers", p["worker_id"])
        if not worker["active_attempt"]:
            return {"worker_id": worker["id"], "directive": "already_quiet"}
        attempt = self._get(c, "attempts", worker["active_attempt"])
        self._close_failed_native_turns(c, worker["id"], attempt["id"], str(p.get("reason", "Execution stopped")))
        if (worker["work_intention"] == "waiting" and attempt["continuation_id"]
                and not attempt["submission_json"]):
            # A committed worker choice survives a lost transport receipt.
            # Quiet failure recovery must not turn a voluntary wait into work.
            c.execute("UPDATE attempts SET state='draining' WHERE id=?", (attempt["id"],))
            c.execute("UPDATE workers SET state='draining' WHERE id=?", (worker["id"],))
            snapshot = self._workspace_snapshot(worker["id"])
            return self._dispatch(c, "runtime.quiescent", {"worker_id": worker["id"], "snapshot": snapshot}, actor)
        submission = json.loads(attempt["submission_json"]) if attempt["submission_json"] else {}
        if attempt["state"] == "draining" and submission and "integration_batch" not in submission:
            # A review verdict or a contribution TaskHub accepted survives a drain that never
            # finished: the processes are quiet, so it is committed as the drain would have.
            finished = self._finish_drained_submission(c, worker, attempt, actor)
            if finished is not None:
                return finished
        reconciled = self._recover_integration_batch(c, worker, attempt)
        if reconciled is not None:
            return reconciled
        attempt = self._get(c, "attempts", attempt["id"])
        held = self._hold_unrecorded_publication(c, worker, attempt)
        if held is not None:
            return held
        task = self._task(c, attempt["task_id"])
        c.execute("UPDATE attempts SET state='abandoned',ended_at=? WHERE id=?", (self.clock(), attempt["id"]))
        c.execute("UPDATE workers SET state='idle',active_attempt=NULL WHERE id=?", (worker["id"],))
        self._refresh_aggregate(c, task["id"])
        c.execute("UPDATE tasks SET state=?,state_version=state_version+1 WHERE id=? AND active_attempt IS NULL",
                  ("retired" if task["state"] == "retired" else "ready", task["id"]))
        self._preserve_member(c, attempt, attempt["snapshot"], reason="abandoned")
        if attempt["continuation_id"]:
            c.execute("UPDATE continuations SET state='abandoned' WHERE id=? AND state<>'resumed'",
                      (attempt["continuation_id"],))
        self._notify(c, task["id"], attempt["id"], str(p.get("reason", "abandoned")))
        self._event(c, "attempt.abandoned", attempt["id"], {"process_quiet": True, "reason": p.get("reason")},
                    task_id=task["id"], worker_id=worker["id"])
        self._recompute_task(c, task["id"])
        return {"worker_id": worker["id"], "directive": "abandoned"}

    @CORE.handles("worker.recover")
    def _on_worker_recover(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        if worker["state"] != "recovery_required" or not worker["active_attempt"]:
            raise HubError("RECOVERY_NOT_REQUIRED", "Worker is not quarantined by startup recovery")
        if p.get("process_confirmed_stopped") is not True or not p.get("reason"):
            raise HubError("QUIESCENCE_REQUIRED", "Confirm the old processes are stopped and record a reason")
        attempt = self._get(c, "attempts", worker["active_attempt"])
        snapshot = self._workspace_snapshot(worker["id"])
        c.execute("UPDATE attempts SET snapshot=? WHERE id=?", (snapshot, attempt["id"]))
        result = self._dispatch(c, "runtime.abandon",
            {"worker_id": worker["id"], "quiescent": True, "reason": "operator_recovery: " + p["reason"]},
            Actor(role=Role.RUNNER))
        if result.get("directive") == "recovery_required":
            return {**result, "snapshot": snapshot, "run_id": attempt["run_id"]}
        self._event(c, "worker.recovered", worker["id"], {"reason": p["reason"], "snapshot": snapshot},
                    attempt["task_id"], worker["id"])
        return {**result, "snapshot": snapshot, "checkpoint": (
            json.loads(attempt["checkpoint_json"]) if attempt["checkpoint_json"] else None),
            "run_id": attempt["run_id"]}

    @CORE.handles("context.ack")
    def _on_context_ack(self, c, name, p, actor):
        self._role(actor, Role.WORKER, Role.RUNNER)
        if actor.role == Role.WORKER:
            self._active(c, actor)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        if actor.role == Role.WORKER and actor.worker_id != worker["id"]:
            raise HubError("FORBIDDEN", "Cannot acknowledge another worker's context", 403)
        if actor.role == Role.WORKER:
            self._active(c, actor)
        delivery = self._get(c, "deliveries", p["packet_id"])
        if delivery["worker_id"] != worker["id"] or delivery["context_epoch"] != worker["context_epoch"]:
            raise HubError("STALE_CONTEXT", "Context receipt belongs to another worker or epoch")
        c.execute("UPDATE deliveries SET state='delivered' WHERE id=?", (delivery["id"],))
        return {"packet_id": delivery["id"], "state": "delivered"}

    @CORE.handles("worker.compacted")
    def _on_worker_compacted(self, c, name, p, actor):
        # The delivery cursor continues: what was delivered is not sent again after compaction.
        self._role(actor, Role.RUNNER)
        self._normalize_worker_fields(c, p, "worker_id")
        worker = self._get(c, "workers", p["worker_id"])
        if p.get("instructions_sha256"):
            # Codex rebuilds a compacted conversation under the instructions of the turn.
            c.execute("UPDATE workers SET instructions_sha256=? WHERE id=?", (p["instructions_sha256"], worker["id"]))
        self._event(c, "worker.context_compacted", worker["id"], {"thread_id": worker["thread_id"]},
                    worker_id=worker["id"])
        return self._get(c, "workers", worker["id"])

    @CORE.handles("turn.reconcile_terminal")
    def _on_turn_reconcile_terminal(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        return self._reconcile_terminal(c, p)

    @CORE.handles("run.reconcile_usage")
    def _on_run_reconcile_usage(self, c, name, p, actor):
        self._role(actor, Role.OPERATOR)
        run = self._get(c, "runs", p["run_id"])
        total = p["tokens_used"]
        if type(total) is not int or total < run["tokens_used"]:
            raise HubError("INVALID_USAGE", "Reconciliation cannot refund observed tokens", 422)
        if not p.get("reason"):
            raise HubError("REASON_REQUIRED", "Record the external reconciliation evidence", 422)
        backend = p.get("backend", "codex")
        if backend not in BACKENDS:
            raise HubError("INVALID_INPUT", "Name the codex or claude account that spent the tokens", 422)
        self._charge_backend(c, run["id"], backend, total - run["tokens_used"])
        c.execute("DELETE FROM worker_usage_holds WHERE run_id=?", (run["id"],))
        c.execute("DELETE FROM helper_usage_holds WHERE run_id=?", (run["id"],))
        c.execute("UPDATE turn_intents SET state='reconciled' WHERE run_id=? AND state='uncertain'", (run["id"],))
        self._refresh_usage_uncertainty(c, run["id"])
        self._event(c, name, run["id"], {"reason": p["reason"], "tokens_used": total})
        return self._get(c, "runs", run["id"])

    def _insert_artifact(self, c, kind, data, *, snapshot=None, task=None, producer=None, input_refs=None,
                         private=False, source_paths=None):
        if not isinstance(kind, str) or not kind or len(kind) > 128:
            raise HubError("INVALID_INPUT", "Artifact kind must be a short nonempty string", 422)
        if not isinstance(data, dict):
            raise HubError("INVALID_INPUT", "Artifact data must be an object", 422)
        input_refs = input_refs or []
        for ref in input_refs:
            self._get(c, "artifacts", ref)
        # A private record whose file the command wrote before the transaction keeps that ID.
        aid, sha = (self._prepared_record_id(producer) if private else None) or uid(), digest(data)
        c.execute(
            "INSERT INTO artifacts(id,kind,body_sha,snapshot,task_id,task_revision,contract_hash,producer,input_refs_json,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (aid, kind, sha, snapshot, task["id"] if task else None, task["revision"] if task else None,
             task["contract_hash"] if task else None, producer, canonical(input_refs), self.clock()))
        self._persist_record_content(c, self._get(c, "artifacts", aid), data, private=private,
                                     source_paths=source_paths)
        c.execute("INSERT INTO search_index VALUES(?,?,?,?)", (aid, kind, kind, canonical(data)))
        if source_paths is not None:
            # Staging installs its source/review bindings next and performs the
            # complete native read before the same transaction can return.
            return self._get(c, "artifacts", aid)
        return self._artifact(c, aid)

    def _notify(self, c, task_id, object_id, reason):
        c.execute("INSERT INTO notifications(id,task_id,object_id,reason,created_at) VALUES(?,?,?,?,?)",
                  (uid(), task_id, object_id, reason, self.clock()))
        self._event(c, "attention.required", object_id, {"reason": reason}, task_id)

    def _end_subscription_wait(self, c, continuation_id, task_id) -> bool:
        """The waker found that what `task_id` published meets a subscription of this wait: the
        wait ends, and its worker reconsiders, told only that its subscribed task changed. False
        when the wait has ended or does not subscribe to `task_id`."""
        row = c.execute("SELECT * FROM continuations WHERE id=? AND state='waiting'", (continuation_id,)).fetchone()
        if row is None or task_id not in {item["task_id"] for item in
                                          json.loads(row["predicate_json"]).get("subscriptions", [])}:
            return False
        c.execute("UPDATE continuations SET state='reconsider',wake_reason='subscribed_task_changed' WHERE id=?",
                  (continuation_id,))
        self._notify(c, row["task_id"], task_id, "subscribed_task_changed")
        return True

    def _end_closed_subscriptions(self, c, task_id, reason):
        """`task_id` is done or retired and publishes nothing more: the waits subscribed to it end,
        and their workers reconsider."""
        for row in c.execute("SELECT DISTINCT k.id,k.task_id FROM continuations k, json_each(k.predicate_json,"
                             "'$.subscriptions') s WHERE k.state IN ('waiting','eligible') "
                             "AND json_extract(s.value,'$.task_id')=?", (task_id,)).fetchall():
            c.execute("UPDATE continuations SET state='reconsider',wake_reason=? WHERE id=?", (reason, row["id"]))
            self._notify(c, row["task_id"], task_id, reason)

    def _reconsider_artifact_waits(self, c, artifact_id, reason):
        """Whoever waits on a candidate that can no longer be accepted reconsiders its wait (one
        naming an earlier version it corrects as well), and a validation of it that waits for
        an operator ends."""
        self._end_held_validation(c, artifact_id, reason)
        self._wake_publication_waits(c, artifact_id)  # Told the candidate's outcome, not only `reason`.
        versions = self._versions_named_by(c, artifact_id)
        for row in c.execute("SELECT * FROM continuations WHERE state IN ('waiting','eligible')").fetchall():
            predicate = json.loads(row["predicate_json"])
            named = {item.get("artifact_id") for item in predicate.get("requirements", [])}
            if named & versions or artifact_id in predicate.get("validations", []):
                c.execute("UPDATE continuations SET state='reconsider',wake_reason=? WHERE id=?", (reason, row["id"]))
                self._notify(c, row["task_id"], artifact_id, reason)

    def _refresh_waits(self, c):
        for row in c.execute("SELECT * FROM continuations WHERE state='eligible'").fetchall():
            if not self._satisfied(c, json.loads(row["predicate_json"]), waiter=row["preferred_worker"]):
                c.execute("UPDATE continuations SET state='waiting',wake_reason='evidence_changed' WHERE id=?",
                          (row["id"],))
                self._notify(c, row["task_id"], row["id"], "wait_evidence_changed")

    def _withdraw(self, c, artifact_id, reason):
        pending, withdrawn, newly_withdrawn = [artifact_id], [], []
        affected_tasks = set()
        while pending:
            current = pending.pop()
            if current in withdrawn:
                continue
            artifact = self._get(c, "artifacts", current)
            fresh = artifact["status"] != "withdrawn"
            if fresh:
                newly_withdrawn.append(current)
            c.execute("UPDATE artifacts SET status='withdrawn' WHERE id=?", (current,))
            withdrawn.append(current)
            if fresh:
                self._reconsider_artifact_waits(c, current, reason)
            if artifact["task_id"]:
                self._notify(c, artifact["task_id"], current, reason)
            # Only the rows whose text names the artifact or its task are read: reading every
            # artifact and every open task in full for each withdrawn artifact held the
            # coordination database for 40-47 s in the Linux pilot. A task's ledger contract
            # is the one its files hold (both are bound by its contract hash).
            for row in c.execute("SELECT id FROM artifacts WHERE status<>'withdrawn' AND instr(input_refs_json,?)>0",
                                 (current,)).fetchall():
                if current in self._artifact_dependency_refs(c, row["id"]):
                    pending.append(row["id"])
            # A requirement naming an earlier version that this one corrects stands for this one.
            versions = self._versions_named_by(c, current)
            names = [*sorted(versions), *([artifact["task_id"]] if artifact["task_id"] else [])]
            for row in c.execute(
                "SELECT t.id,r.contract_json FROM tasks t JOIN task_revisions r "
                "ON r.task_id=t.id AND r.revision=t.current_revision WHERE t.state NOT IN ('done','retired') "
                "AND (" + " OR ".join("instr(r.contract_json,?)>0" for _ in names) + ")", names).fetchall():
                contract = json.loads(row["contract_json"])
                if current in contract.get("inputs", []) or any(
                    req.get("artifact_id") in versions or
                    (artifact["task_id"] is not None and req.get("task_id") == artifact["task_id"]
                     and req.get("task_revision") == artifact["task_revision"])
                    for req in contract.get("requirements", [])
                ):
                    if fresh:
                        affected_tasks.add(row["id"])
                    self._notify(c, row["id"], current, reason)
                    if current in contract.get("inputs", []):
                        c.execute("UPDATE tasks SET state='needs_attention',state_version=state_version+1 "
                                  "WHERE id=? AND active_attempt IS NULL AND state<>'retired'", (row["id"],))
            self._event(c, "artifact.withdrawn", current, {"reason": reason}, artifact["task_id"])
        for current in withdrawn:
            artifact = self._get(c, "artifacts", current)
            if artifact["task_id"]:
                self._recompute_task(c, artifact["task_id"])
        for task_id in sorted(affected_tasks):
            self._queue_dependency_reconsideration(c, task_id, artifact_id)
        self._invalidate_completed_evidence(c, newly_withdrawn, reason, artifact_id)
        return withdrawn


    def _recompute_task(self, c, task_id):
        return self._recompute_collaborative(c, task_id)

    async def authorize_worker(self, actor: Actor, *, dispatch: bool = False) -> dict:
        """Validate the exact live worker binding before a trusted plugin tool executes, or
        before a turn's input is dispatched (``dispatch``), when every pause applies."""
        self._role(actor, Role.WORKER)
        return await self.db.call(lambda c: self._active(c, actor, dispatch=dispatch)[0])

    async def task(self, task_id: str, revision: int | None = None) -> dict:
        if revision is not None and (type(revision) is not int or revision < 1):
            raise HubError("INVALID_INPUT", "Revision must be a positive integer", 422)
        # A worker's taskhub_inspect, and the dashboard: on a read connection, files included.
        return await self._native_view(lambda c: self._task(c, task_id, revision))

    async def task_kind(self, task_id: str) -> str:
        """The kind of a task's current contract, on a read connection. Each tool call needs it
        for its capability; reading the task in full took the coordination thread about 50 ms
        a call in the pilot, more than once a second."""
        def read(c):
            row = c.execute("SELECT json_extract(v.contract_json,'$.kind') FROM tasks t JOIN task_revisions v "
                            "ON v.task_id=t.id AND v.revision=t.current_revision WHERE t.id=?", (task_id,)).fetchone()
            if row is None:
                raise HubError("NOT_FOUND", "tasks object does not exist", 404)
            return row[0]
        return await self.db.read(read)

    async def ids_with_prefix(self, value: str) -> dict[str, list[str]]:
        """The task and artifact IDs a worker may have meant by `value`, on a read connection:
        `value` itself if it exists, and up to three IDs starting with its first 8 characters."""
        prefix = value[:8].lower()
        # The IDs sharing the prefix are a range of each primary key's index.
        bounds = (prefix, prefix[:7] + chr(ord(prefix[7]) + 1)) if HEX_PREFIX.fullmatch(prefix) else None

        def read(c):
            found = {}
            for table in ("tasks", "artifacts"):
                ids = [row[0] for row in c.execute(f"SELECT id FROM {table} WHERE id=?", (value,))]
                if bounds is not None:
                    ids += [row[0] for row in c.execute(
                        f"SELECT id FROM {table} WHERE id>=? AND id<? AND id<>? ORDER BY id LIMIT 3", (*bounds, value))]
                found[table] = ids
            return found
        return await self.db.read(read)

    async def inspect_task_completion(self, task_id: str, actor: Actor) -> dict:
        """Read the contract and its completion evidence at one serialized boundary."""
        def read(c):
            self._active(c, actor)
            task = self._task(c, task_id)
            try:
                self._authorize_participant(c, task, actor)
            except HubError as error:
                if error.code != "PARTICIPANT_REQUIRED":
                    raise
                raise HubError("PARTICIPANT_REQUIRED", "include_completion is only for the task's original "
                               "publishers and current executors; omit it to read the task", 403) from error
            completion = {
                "task_id": task["id"], "revision": task["revision"],
                "acceptance_conditions": self._conditions(c, task),
                "declarations": [self._completion_record(c, row["id"]) for row in c.execute(
                    "SELECT id FROM task_completion_declarations WHERE task_id=? ORDER BY created_at,id",
                    (task["id"],))],
            }
            return {**task, "completion": completion}
        return await self._native_view(read)

    async def worker(self, worker_id: str) -> dict:
        def read(c):
            resolved = self._resolve_worker_id(c, worker_id)
            worker = self._get(c, "workers", resolved)
            stop = c.execute("SELECT reason FROM attempt_stop_requests WHERE attempt_id=?", (worker["active_attempt"],)).fetchone()
            worker["runtime_stop_reason"] = stop["reason"] if stop else None
            control = c.execute("SELECT run_id,after_turn FROM worker_controls WHERE worker_id=?", (worker["id"],)).fetchone()
            own, individual = self._control_of(c, control["run_id"], worker["id"]) if control else (None, False)
            worker["run_control"] = own if individual else None
            worker["run_control_after_turn"] = bool(worker["run_control"] == "paused" and control["after_turn"])
            # Its group's control in the current run (Pause Codex workers after turn, ...).
            current = c.execute("SELECT id FROM runs WHERE state IN ('running','paused') "
                                "ORDER BY created_at DESC,id DESC LIMIT 1").fetchone()
            worker["group_control"] = self._group_of(
                self._group_controls(c, current["id"]), worker["backend"], worker["kind"]) if current else None
            worker["effective_rotation_mode"] = self._rotation_mode(c, worker)
            return worker
        return await self.db.read(read)

    async def artifact(self, artifact_id: str) -> dict:
        return await self.db.call(lambda c: self._artifact(c, artifact_id))

    def _new_worker_backend(self, c) -> str:
        """The operator's backend for new agents that the assignment plan leaves open."""
        row = c.execute("SELECT new_worker_backend FROM runtime_preferences WHERE id=1").fetchone()
        return row["new_worker_backend"] if row else self.config.new_worker_backend

    def _creation_backend(self, c, kind: str = "ordinary", number: int | None = None) -> str:
        """The backend a new agent of this kind gets. A new worker: the operator's choice, else
        the assignment plan's, else the project's. Every other agent (a reviewer, the Integrator,
        a decision helper) runs on Codex, whatever backend workers take. Until 2026-11-08 a
        reviewer and the Integrator followed the project's new_worker_backend: a project whose
        workers were on Claude made Claude reviewers, which no model is chosen for. Without a
        number, that of the next ordinary worker."""
        if kind != "ordinary":
            return "codex"
        if self._operator_worker_backend(c):
            return self._new_worker_backend(c)
        if number is None:
            number = next_worker_number(c)
        return self.config.assignment.backend(kind, number) or self.config.new_worker_backend

    @staticmethod
    def _operator_worker_backend(c) -> bool:
        return c.execute("SELECT 1 FROM runtime_preferences WHERE id=1").fetchone() is not None

    def _worker_plan(self, c) -> list:
        """The assignment plan's backends for new workers, unless the operator chose one."""
        return [] if self._operator_worker_backend(c) else self.config.assignment.worker_backends

    @staticmethod
    def _charge_backend(c, run_id, backend, delta):
        """Usage is charged to the run and, separately, to the account of the backend that spent it."""
        column = {"codex": "codex_tokens_used", "claude": "claude_tokens_used"}[backend]
        c.execute(f"UPDATE runs SET tokens_used=tokens_used+?,{column}={column}+? WHERE id=?", (delta, delta, run_id))

    def _backend_status(self, c) -> dict:
        workers = {name: 0 for name in BACKENDS}
        for row in c.execute("SELECT backend,COUNT(*) AS count FROM workers WHERE work_intention<>'retired' "
                             "GROUP BY backend"):
            workers[row["backend"]] = row["count"]
        usage = c.execute("SELECT COALESCE(SUM(codex_tokens_used),0) AS codex,"
                          "COALESCE(SUM(claude_tokens_used),0) AS claude FROM runs").fetchone()
        live = dict.fromkeys(BACKENDS, 0)
        for row in c.execute("SELECT " + SESSION_BACKEND + " AS backend,COUNT(*) AS count FROM attempts a "
                             "JOIN workers w ON w.id=a.worker_id WHERE a.ended_at IS NULL GROUP BY 1"):
            live[row["backend"]] = row["count"]
        limits = self._session_limits_view(c)
        return {"new_worker_backend": self._new_worker_backend(c), "workers": workers,
                "new_worker_source": "operator" if self._operator_worker_backend(c)
                else "plan" if self.config.assignment.worker_backends else "config",
                "role_models": self._role_models(c),
                "assignment": self.config.assignment.model_dump(mode="json"),
                "next_worker": {"number": (number := self._free_number(c)),
                                "backend": self._creation_backend(c, "ordinary", number)},
                # A limit of None is no limit; 0 starts no worker session.
                "sessions": {name: {"live": live[name],
                                    "limit": None if limits[name]["limit"] < 0 else limits[name]["limit"],
                                    "source": limits[name]["source"]}
                             for name in BACKENDS},
                "tokens_used": {name: usage[name] for name in BACKENDS},
                "dispatch_holds": self._dispatch_holds(c),
                "claude_profiles": self._claude_profiles_view(c),
                "codex_profiles": self._codex_profiles_view(c),
                "decision_model": self._decision_preferences(c),
                "waker_model": self._waker_preferences(c),
                "answer_deadline": self._answer_deadline(c),
                "missing_choices": self._missing_choices(c),
                "rotation": self._rotation_settings(c),
                "integrator_batch": self._batch_rule(c)}

    def _decision_preferences(self, c) -> dict:
        """The Dispatcher's model and effort for assignment and recruitment decisions: the
        operator's recorded choice, else the project configuration; None while neither chose."""
        row = c.execute("SELECT model,effort FROM decision_preferences WHERE id=1").fetchone()
        if row is not None:
            return {"model": row["model"], "effort": row["effort"], "source": "operator"}
        decisions = self.config.decisions
        return {"model": decisions.model, "effort": decisions.effort,
                "source": "config" if decisions.model or decisions.effort else None}

    async def decision_preferences(self) -> dict:
        return await self.db.read(self._decision_preferences)

    async def backend_status(self) -> dict:
        """Which backend fresh workers use, and each backend's own workers, usage, and hold."""
        if self.config.claude.home:
            await self.refresh_claude_profiles()  # Their sign-ins and directories, read now.
        if self.config.codex.home and self.config.codex.profiles:
            await self.refresh_codex_profiles()
        return await self.db.read(self._backend_status)

    async def worker_runtime(self, worker_id: str) -> tuple[str, str]:
        """The backend an agent was created on, and its role (which may set its effort)."""
        def read(c):
            worker = self._get(c, "workers", worker_id)
            return worker["backend"], worker["kind"]
        return await self.db.read(read)

    async def run_limits(self, run_id: str) -> dict:
        """The run's limits alone, without working out its controls (as `run` does)."""
        def read(c):
            row = c.execute("SELECT limits_json FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise HubError("NOT_FOUND", "runs object does not exist", 404)
            return Limits.model_validate(json.loads(row["limits_json"])).model_dump()
        return await self.db.read(read)

    async def run(self, run_id: str, *, worker_id: str | None = None) -> dict:
        """The run's control state; with a worker, the state as it applies to that person."""
        def read(c):
            result = self._get(c, "runs", run_id)
            result["limits"] = Limits.model_validate(json.loads(result.pop("limits_json"))).model_dump()
            # A held account stops only its own backend's workers.
            backend = self._get(c, "workers", worker_id)["backend"] if worker_id is not None else None
            result["dispatch_hold"] = self._dispatch_hold(c, backend)
            result["dispatch_holds"] = self._dispatch_holds(c)
            result["resumed_workers"] = sorted(self._controlled_workers(c, run_id, "resumed"))
            result["paused_workers"] = sorted(self._controlled_workers(c, run_id, "paused"))
            # The groups' controls; while the run is paused, those resumed, as a whole or in part.
            result["groups"] = self._groups_view(c, result)
            result["resumed_groups"] = sorted(group for group, view in result["groups"].items()
                                              if result["state"] == "paused" and view["state"] != "paused")
            result["assignment_open"] = self._assignment_open(result, self._group_controls(c, run_id))
            control = c.execute("SELECT kind FROM events WHERE object_id=? AND kind IN " + RUN_CONTROL_EVENTS
                                + " ORDER BY sequence DESC LIMIT 1", (run_id,)).fetchone()
            result["stop_reason"] = (RUN_STOP_REASONS.get(control["kind"])
                                     if control and result["state"] != "running" else None)
            result["run_state"] = result["state"]
            if worker_id is not None:
                individual = self._worker_control(c, run_id, worker_id)
                if individual is not None:
                    result["state"] = "running" if individual == "resumed" else "paused"
                    result["stop_reason"] = None if individual == "resumed" else "operator_pause"
            if c.execute("SELECT state FROM project_lifecycle WHERE id=1").fetchone()["state"] != "running":
                result["stop_reason"] = "project_shutdown"
            # 'after_turn' while the pause lets this person's turn under way finish.
            result["pause_mode"] = self._pause_mode(c, self._get(c, "runs", run_id), worker_id)
            return result
        return await self.db.read(read)

    async def activation_controls(self, watches: Iterable[tuple[str, str | None]]) -> dict[tuple[str, str | None], dict]:
        """What turns and validations under way check while they run, for all of them in one read.
        A watch is a run and a worker, or None for the run alone. Its view has the run's state,
        stop reason and pause mode as run(run_id, worker_id=...) gives them; the worker's active
        attempt, state and stop request as worker() gives them; and whether a directed message
        waits, as pending_message_signal() says (`messages`), and one from a person
        (`human_messages`). Two statements whatever the number of watches,
        and one row each: each statement and each row gives up the interpreter lock. A watch
        whose run or worker is missing has no view."""
        watches = set(watches)

        def read(c):
            runs = {row["id"]: row for row in c.execute(
                "SELECT id,state,pause_after_turn,CASE WHEN state<>'running' THEN (SELECT kind FROM events "
                "WHERE object_id=runs.id AND kind IN " + RUN_CONTROL_EVENTS + " ORDER BY sequence DESC LIMIT 1) "
                "END AS control_event,(SELECT state FROM project_lifecycle WHERE id=1) AS lifecycle,"
                "(SELECT json_group_object(scope,control) FROM group_controls WHERE run_id=runs.id) AS groups "
                "FROM runs WHERE id IN (SELECT value FROM json_each(?))",
                (json.dumps(sorted({run_id for run_id, _ in watches})),))}
            workers = {row["id"]: row for row in c.execute(
                "SELECT w.id,w.backend,w.kind,w.active_attempt,w.state,(SELECT reason FROM attempt_stop_requests "
                "WHERE attempt_id=w.active_attempt) AS runtime_stop_reason,wc.run_id AS control_run,"
                "wc.after_turn,CASE WHEN " + control_applies("wc", group_control("w", "wc.run_id"))
                + " THEN wc.control END AS control," + waiting_human_message("w.id") + " AS human_messages,"
                + waiting_peer_message("w.id") + " AS peer_messages FROM workers w "
                "LEFT JOIN worker_controls wc ON wc.worker_id=w.id LEFT JOIN runs r ON r.id=wc.run_id "
                "WHERE w.id IN (SELECT value FROM json_each(?))",
                (json.dumps(sorted({worker_id for _, worker_id in watches if worker_id is not None})),))}
            views = {}
            for run_id, worker_id in watches:
                run, worker = runs.get(run_id), workers.get(worker_id) if worker_id is not None else None
                if run is None or (worker_id is not None and worker is None):
                    continue
                control, after_turn = ((worker["control"], worker["after_turn"])
                                       if worker is not None and worker["control_run"] == run_id else (None, 0))
                if control is None and worker is not None and run["lifecycle"] == "running":
                    # Else its group's control, which pauses only after turns.
                    control = self._group_of(json.loads(run["groups"] or "{}"), worker["backend"], worker["kind"])
                    after_turn = 1
                state = run["state"] if control is None else "running" if control == "resumed" else "paused"
                stop_reason = RUN_STOP_REASONS.get(run["control_event"]) if run["state"] != "running" else None
                if control is not None:
                    stop_reason = None if control == "resumed" else "operator_pause"
                stopping = run["lifecycle"] != "running"
                if state == "running":
                    pause_mode = None
                elif stopping:
                    pause_mode = "now"
                elif control == "paused":
                    pause_mode = "after_turn" if after_turn else "now"
                else:
                    pause_mode = "after_turn" if run["state"] == "paused" and run["pause_after_turn"] else "now"
                views[run_id, worker_id] = {
                    "run": {"state": state, "pause_mode": pause_mode,
                            "stop_reason": "project_shutdown" if stopping else stop_reason},
                    "worker": None if worker is None else {
                        key: worker[key] for key in ("active_attempt", "state", "runtime_stop_reason")},
                    "messages": bool(worker is not None and (worker["human_messages"] or worker["peer_messages"])),
                    "human_messages": bool(worker is not None and worker["human_messages"])}
            return views
        return await self.db.read(read)

    async def list_runs(self) -> list[dict]:
        def read(c):
            hold, holds = self._dispatch_hold(c), self._dispatch_holds(c)
            return [{**dict(r), "limits": Limits.model_validate(json.loads(r["limits_json"])).model_dump(),
                     "dispatch_hold": hold, "dispatch_holds": holds,
                     "groups": self._groups_view(c, r) if r["state"] in {"running", "paused"} else {}}
                    for r in c.execute("SELECT * FROM runs ORDER BY created_at,id")]
        return await self.db.read(read)

    async def mark_run_resumable(self, run_id: str) -> None:
        """Remember that the service drives this run, so a service restart resumes it."""
        def mark(c):
            c.execute("UPDATE runs SET resume_on_start=1 WHERE id=?", (run_id,))
        await self.db.call(mark, write=True)

    async def resumable_runs(self) -> list[str]:
        """Runs to re-arm when the service starts; stopped and completed runs never resume."""
        return await self.db.read(lambda c: [row["id"] for row in c.execute(
            "SELECT id FROM runs WHERE resume_on_start=1 AND state IN ('running','paused') ORDER BY created_at,id")])

    async def list_tasks(self) -> list[dict]:
        return await self.db.call(lambda c: [
            self._task(c, row["id"]) for row in c.execute("SELECT id FROM tasks ORDER BY created_at,id")])

    async def task_page(self, *, view="all", query="", offset=0, limit=25, kind="both", sort="newest", until=None):
        from .task_browser import task_page
        return await task_page(self, view=view, query=query, offset=offset, limit=limit,
                               kind=kind, sort=sort, until=until)

    async def task_revision_page(self, task_id, *, offset=0, limit=20):
        from .task_browser import task_revision_page
        return await task_revision_page(self, task_id, offset=offset, limit=limit)

    async def task_overview(self):
        from .task_browser import task_overview
        return await task_overview(self)

    async def task_outline(self):
        from .task_graph import task_outline
        return await task_outline(self)

    async def task_graph(self, *, include_met=False):
        from .task_graph import task_graph
        return await task_graph(self, include_met=include_met)

    async def task_relations(self, task_id):
        from .task_graph import task_relations
        return await task_relations(self, task_id)

    async def list_workers(self, *, retired: bool = True) -> list[dict]:
        """Every worker with its controls; without `retired`, only those not retired (the
        dashboard's poll: in the Linux pilot 3,424 of 4,109 workers were retired)."""
        return await self.db.read(lambda c: [
            worker_identity(dict(row))
            for row in c.execute("SELECT w.*,s.reason AS runtime_stop_reason,a.created_at AS active_since,"
                                 # An individual control applies only while it differs from its run
                                 # and its group's control; the latter is listed apart.
                                 "CASE WHEN " + control_applies("wc", WORKER_GROUP) + " THEN wc.control END AS run_control,"
                                 "CASE WHEN wc.control='paused' AND " + control_applies("wc", WORKER_GROUP)
                                 + " THEN wc.after_turn=1 ELSE 0 END AS run_control_after_turn,"
                                 + group_control("w", CURRENT_RUN) + " AS group_control,br.backend AS requested_backend "
                                 "FROM workers w LEFT JOIN attempt_stop_requests s ON s.attempt_id=w.active_attempt "
                                 "LEFT JOIN worker_backend_requests br ON br.worker_id=w.id "
                                 "LEFT JOIN attempts a ON a.id=w.active_attempt "
                                 "LEFT JOIN worker_controls wc ON wc.worker_id=w.id LEFT JOIN runs r ON r.id=wc.run_id "
                                 + ("" if retired else "WHERE w.state<>'retired' ") +
                                 "ORDER BY w.kind='review',CASE WHEN w.kind='review' "
                                 "THEN w.reviewer_number ELSE w.worker_number END")])

    async def retired_workers(self) -> dict:
        """The retired workers, enough to name them: [id, display name, kind, backend], and their count."""
        def read(c):
            rows = [worker_identity(dict(row)) for row in c.execute(
                "SELECT id,kind,backend,worker_number,reviewer_number FROM workers WHERE state='retired' "
                "ORDER BY kind='review',CASE WHEN kind='review' THEN reviewer_number ELSE worker_number END")]
            return {"count": len(rows),
                    "workers": [[row["id"], row.get("display_name") or row["id"], row["kind"], row["backend"]] for row in rows]}
        return await self.db.read(read)

    async def retired_worker_count(self) -> int:
        return await self.db.read(lambda c: c.execute("SELECT COUNT(*) FROM workers WHERE state='retired'").fetchone()[0])

    async def list_continuations(self) -> list[dict]:
        def read(c):
            result = []
            for row in c.execute("SELECT * FROM continuations ORDER BY created_at,id"):
                item = dict(row)
                item["predicate"] = json.loads(item.pop("predicate_json"))
                item["checkpoint"] = json.loads(item.pop("checkpoint_json"))
                result.append(item)
            return result
        return await self.db.call(read)

    async def list_interruptions(self) -> list[dict]:
        def read(c):
            result = []
            for row in c.execute("SELECT * FROM interruption_checkpoints ORDER BY created_at,id"):
                item = dict(row)
                for field in ("input_refs", "checkpoint", "proof", "codex_profile"):
                    item[field] = json.loads(item.pop(field + "_json"))
                result.append(item)
            return result
        return await self.db.call(read)

    async def list_artifacts(self, kind: str | None = None) -> list[dict]:
        def read(c):
            rows = c.execute("SELECT id FROM artifacts WHERE (? IS NULL OR kind=?) ORDER BY created_at,id", (kind, kind))
            return [self._artifact(c, row["id"]) for row in rows]
        return await self.db.call(read)

    async def events(self, after: int = 0, limit: int = 200) -> list[dict]:
        if type(after) is not int or after < 0 or not 1 <= limit <= 1000:
            raise HubError("INVALID_INPUT", "Invalid event pagination", 422)
        return await self.db.call(lambda c: [
            {**dict(r), "payload": json.loads(r["payload_json"])} for r in c.execute(
                "SELECT * FROM events WHERE sequence>? ORDER BY sequence LIMIT ?", (after, limit))])

    async def search(self, query: str, limit: int = 20, *, actor: Actor | None = None) -> list[dict]:
        if not query.strip() or len(query) > 1000 or not 1 <= limit <= 100:
            raise HubError("INVALID_INPUT", "Invalid search request", 422)
        # Literal phrase search prevents an incidental FTS expression from expanding the query.
        phrase = '"' + query.replace('"', '""') + '"'
        def read(c):
            if actor is not None:
                self._record_reader(c, actor)
            result = []
            for row in c.execute(
                "SELECT object_id,kind,title,snippet(search_index,3,'','', ' ... ',24) AS excerpt "
                    "FROM search_index WHERE search_index MATCH ? ORDER BY rowid", (phrase,)):
                if actor is not None and actor.role == Role.WORKER:
                    record = c.execute(
                        "SELECT id,kind,status,task_id,producer FROM artifacts WHERE id=?",
                        (row["object_id"],)).fetchone()
                    if record is not None and self._record_private(c, record):
                        continue
                result.append(dict(row))
                if len(result) == limit:
                    break
            return result
        return await self.db.call(read)

    async def context(self, worker_id: str) -> dict:
        worker_id = await self.resolve_worker_id(worker_id)
        material = MaterialFiles(self.workspace(worker_id))
        # Blobs and material files are content-addressed: the transaction records their hashes
        # and paths, and they are written after commit, off its thread, before the packet is
        # returned. A packet that was never returned needs none of them.
        writes: list[tuple[str, Any]] = []

        def blob(value) -> str:
            body = canonical(value).encode("utf-8")
            writes.append(("blob", body))
            return hashlib.sha256(body).hexdigest()

        def material_file(value) -> dict:
            body = canonical(value).encode("utf-8")
            writes.append(("material", body))
            return MaterialFiles.reference(body)

        def assemble(c):
            writes.clear()
            worker = self._get(c, "workers", worker_id)
            if not worker["active_attempt"]:
                raise HubError("NO_ACTIVE_TASK", "Worker has no active attempt")
            attempt = self._get(c, "attempts", worker["active_attempt"])
            task = self._task(c, attempt["task_id"])
            run = self._get(c, "runs", attempt["run_id"])
            limits = Limits.model_validate(json.loads(run["limits_json"]))
            baseline = c.execute(
                "SELECT MAX(sequence) FROM deliveries WHERE worker_id=? AND context_epoch=? AND state='delivered'",
                (worker_id, worker["context_epoch"])).fetchone()[0] or 0
            contract = task["contract"]
            reference = blob(contract)
            contract_file = material_file(contract)["material_path"]
            checkpoint = json.loads(attempt["checkpoint_json"]) if attempt["checkpoint_json"] else None
            if checkpoint and len(canonical(checkpoint).encode("utf-8")) > 4096:
                blob(checkpoint)
                checkpoint = material_file(checkpoint)
            input_refs = []
            for ref in sorted(set(contract["inputs"]) | set(json.loads(attempt["input_refs_json"]))):
                artifact = self._get(c, "artifacts", ref)
                input_refs.append({key: artifact[key] for key in
                                   ("id", "kind", "status", "task_id", "task_revision")})
            requirements = [
                {**req, "satisfied": self._requirement(c, req), "artifact_ids": self._requirement_artifacts(c, [req])}
                for req in contract["requirements"]]
            if attempt["continuation_id"]:
                continuation = self._get(c, "continuations", attempt["continuation_id"])
                predicate = json.loads(continuation["predicate_json"])
                resume_requirements = [
                    {**req, "satisfied": self._requirement(c, req), "artifact_ids": self._requirement_artifacts(c, [req])}
                    for req in predicate.get("requirements", [])]
            else:
                resume_requirements = []
            packet = {
                "packet_id": uid(), "worker_id": worker_id, "context_epoch": worker["context_epoch"],
                "worker_number": worker["worker_number"], "display_name": worker["display_name"],
                "reviewer_number": worker["reviewer_number"], "worker_kind": worker["kind"],
                "task": {"id": task["id"], "revision": task["revision"], "name": task["name"],
                         "contract_hash": task["contract_hash"], "state": task["state"],
                         "title": contract["title"], "objective": contract["objective"],
                         "domain": contract["domain"], "kind": contract["kind"],
                         "acceptance_policy": contract["acceptance_policy"],
                         "constraints": contract["constraints"], "contract_material_sha256": reference,
                         "contract_material_path": contract_file},
                "attempt": {"id": attempt["id"], "fencing_token": attempt["fencing_token"], "snapshot": attempt["snapshot"]},
                "checkpoint": checkpoint, "inputs": input_refs, "requirements": requirements,
                "resume_requirements": resume_requirements,
                "execution": {"active_workers": c.execute("SELECT count(*) FROM workers WHERE state IN ('preparing','running','draining','recovery_required')").fetchone()[0]},
                "usage": {"turns_used": run["turns_used"], "tokens_used": run["tokens_used"]},
                "updates": [], "after_sequence": baseline, "through_sequence": baseline,
                "has_more_updates": False, "requires_sync": False,
                "material_access": "Read *_material_path or material_path as workspace-relative UTF-8 files with native file tools or workspace_read operation. Hashes identify exact bytes; fetch operation locates records and receipts.",
            }
            # Large optional material is explicitly referenced, never silently summarized or cut.
            for field in ("context", "payload"):
                value = contract[field]
                if len(canonical(value).encode("utf-8")) <= 1024:
                    packet["task"][field] = value
                else:
                    packet["task"][field + "_material_sha256"] = blob(value)
                    packet["task"][field + "_material_path"] = material_file(value)["material_path"]
            collaboration = worker_collaboration(task["collaboration"])
            if collaboration["mode"] == "collaborative":
                compact = {key: value for key, value in collaboration.items() if key not in {"members", "publishers"}}
                for field in ("members", "publishers"):
                    values = collaboration[field]
                    compact[field] = values[:16]
                    compact[field + "_count"] = len(values)
                    if len(values) > 16:
                        compact[field + "_material_sha256"] = blob(values)
                        compact[field + "_material_path"] = material_file(values)["material_path"]
                if len(canonical(compact).encode("utf-8")) <= min(4096, limits.packet_bytes // 3):
                    packet["task"]["collaboration"] = compact
                else:
                    blob(collaboration)
                    packet["task"]["collaboration"] = {
                        "mode": "collaborative", "host_task_id": collaboration.get("host_task_id"),
                        **material_file(collaboration)}
            dependencies = {req["task_id"] for req in contract["requirements"] + resume_requirements if req.get("task_id")}
            task_ids_set = {task["id"], *dependencies}
            artifact_ids_set = set(contract["inputs"]) | set(json.loads(attempt["input_refs_json"])) | {
                req["artifact_id"] for req in contract["requirements"] + resume_requirements if req.get("artifact_id")}
            subscriptions = []
            for subscribed in c.execute(
                "SELECT t.id,t.current_revision,t.state,s.task_revision,s.reason,r.contract_json FROM task_subscriptions s "
                "JOIN tasks t ON t.id=s.task_id JOIN task_revisions r ON r.task_id=t.id AND r.revision=t.current_revision "
                "WHERE s.worker_id=? ORDER BY s.created_at,t.id", (worker_id,)):
                subscribed_contract = self._task(c, subscribed["id"])["contract"]
                task_ids_set.add(subscribed["id"])
                for requirement in subscribed_contract["requirements"]:
                    if requirement.get("task_id"):
                        task_ids_set.add(requirement["task_id"])
                    elif requirement.get("artifact_id"):
                        artifact_ids_set.add(requirement["artifact_id"])
                artifact_ids_set.update(subscribed_contract["inputs"])
                subscriptions.append({key: subscribed[key] for key in ("id", "current_revision", "state", "task_revision", "reason")})
            if subscriptions:
                # Current snapshots make newly subscribed state visible even past an acknowledged event cursor.
                packet["subscriptions"] = {"count": len(subscriptions), "tasks": subscriptions[:8]}
                if len(subscriptions) > 8:
                    packet["subscriptions"]["material_sha256"] = blob(subscriptions)
                    packet["subscriptions"]["material_path"] = material_file(subscriptions)["material_path"]
            task_ids = sorted(task_ids_set)
            marks = ",".join("?" for _ in task_ids)
            artifact_ids = sorted(artifact_ids_set)
            conditions = f"(task_id IN ({marks}) OR worker_id=?"
            args = [baseline, *task_ids, worker_id]
            if artifact_ids:
                conditions += " OR object_id IN (" + ",".join("?" for _ in artifact_ids) + ")"
                args.extend(artifact_ids)
            conditions += ")"
            # Stream past known routine rows without making the model page
            # through them. The budget counts worker-visible events. The scan
            # cursor is committed only by the existing delivery acknowledgment;
            # it must never cross a visible event that did not fit this packet.
            rows = c.execute(
                "SELECT * FROM events WHERE sequence>? AND " + conditions + " ORDER BY sequence", args)
            blocked_by_size = False
            for row in rows:
                payload = json.loads(row["payload_json"])
                if routine_for_recipient(row["kind"], payload, row["worker_id"], worker_id):
                    packet["through_sequence"] = row["sequence"]
                    continue
                if len(packet["updates"]) >= WORKER_UPDATE_LIMIT:
                    packet["has_more_updates"] = True
                    packet["requires_sync"] = True
                    break
                item = {"sequence": row["sequence"], "kind": row["kind"],
                        "object_id": row["object_id"], "payload": payload}
                if item["kind"] == "review.submitted" and item["payload"].get("report_material_sha256"):
                    # This field is created by the review protocol, never by
                    # traversing arbitrary user content for hash-like strings.
                    retained = item["payload"]["report_material_sha256"]
                    writes.append(("retained", retained))
                    item["payload"]["report_material_path"] = MaterialFiles.path_for(retained)
                if len(canonical(item).encode("utf-8")) > limits.delta_bytes // 2:
                    blob(item["payload"])
                    item["payload"] = material_file(item["payload"])
                packet["updates"].append(item)
                if (len(canonical(packet["updates"]).encode("utf-8")) > limits.delta_bytes
                        or len(canonical(packet).encode("utf-8")) > limits.packet_bytes - 128):
                    packet["updates"].pop()
                    blocked_by_size = True
                    packet["has_more_updates"] = True
                    packet["requires_sync"] = True
                    break
                packet["through_sequence"] = row["sequence"]
            if len(canonical(packet).encode("utf-8")) > limits.packet_bytes:
                raise HubError("CONTEXT_TOO_LARGE", "Mandatory context exceeds packet_bytes; revise the task or packet size", 413)
            if blocked_by_size and not packet["updates"]:
                raise HubError("CONTEXT_TOO_LARGE", "The next update cannot fit; increase packet_bytes or delta_bytes", 413)
            sha = blob(packet)
            c.execute("INSERT INTO deliveries VALUES(?,?,?,?,?,?,?)",
                      (packet["packet_id"], worker_id, worker["context_epoch"],
                       packet["through_sequence"], sha, "pending", self.clock()))
            return packet
        packet = await self.db.call(assemble, write=True)

        def install():
            for kind, value in writes:
                if kind == "blob":
                    self.blobs.put(value)
                elif kind == "material":
                    material.write(value)
                else:
                    material.retained(self.blobs, value)
        await collected_thread_call(install)
        return packet
