"""Managed repository bases, candidate source bindings and integration records.

The dedicated Integrator publishes batches (see ``batch_integration``); this
module keeps the worker repository checkouts and shared integration records.
"""
from __future__ import annotations

import asyncio
import json
import logging
import stat
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .content import TreeView, _path, _relative
from .draft_paths import is_draft_path
from .errors import HubError, WorkerWorkspaceError
from .graph_paths import is_graph_path
from .library_rest import at_rest, restore_workspace
from .models import Role
from .repository import RepositoryStore
from .storage import canonical, collected, collected_thread_call, snapshot_identity

logger = logging.getLogger(__name__)


def _writable(path: Path) -> None:
    """Let the owner write a workspace file a refresh is about to update. A worker that copies a
    file out of the published Library, whose files are read-only, with `cp` keeps that mode; the
    refresh then failed on it and refused every admission of the worker (Linux pilot: Worker 281,
    61 times in three hours; 23 workspaces held such files)."""
    try:
        mode = path.stat(follow_symlinks=False).st_mode
    except FileNotFoundError:
        return
    if stat.S_ISREG(mode) and not mode & stat.S_IWUSR:
        path.chmod(stat.S_IMODE(mode) | stat.S_IWUSR)


class RepositoryCoordination:
    if TYPE_CHECKING:
        _validation_queue_status: Any
        _batch: Any
        _batch_rule: Any
        db: Any
        config: Any
        state_dir: Any
        blobs: Any
        clock: Any
        workspace: Any
        _get: Any
        _role: Any
        _event: Any
        _notify: Any
        _snapshot_binding: Any
        _workspace_lock: Any
        _quarantine_workspace_failure: Any
        _refresh_aggregate: Any
        _refresh_message_wake: Any

    def _repository(self):
        config = self.config.repository
        if not config.enabled:
            raise HubError("REPOSITORY_DISABLED", "Enable the project's managed repository first")
        return RepositoryStore(self.state_dir, branch=config.branch, executable=config.executable)

    @staticmethod
    def _repository_head_recorded(c, head):
        return bool(c.execute("SELECT 1 FROM events WHERE kind='repository.initialized' AND object_id=?",
                              (head,)).fetchone()
                    or c.execute("SELECT 1 FROM repository_publications WHERE commit_sha=?", (head,)).fetchone())

    def _require_recorded_repository_head(self, c, repository):
        head = repository.head()
        if not self._repository_head_recorded(c, head):
            raise HubError("REPOSITORY_RECONCILIATION_REQUIRED",
                           "The public Git head has no durable publication receipt; reconcile its approved review first")
        return head

    def _repository_summary(self, c, head=None):
        if not self.config.repository.enabled:
            return {"enabled": False}
        if head is None:
            try:
                head = self._repository().head()
            except ValueError as error:
                return {"enabled": True, "initialized": False, "branch": self.config.repository.branch,
                        "error": str(error)}
        rows = c.execute("SELECT * FROM repository_publications ORDER BY created_at DESC LIMIT 20").fetchall()
        return {"enabled": True, "initialized": True, "head": head,
                "reconciliation_required": not self._repository_head_recorded(c, head),
                "branch": self.config.repository.branch,
                "publications": [{key: row[key] for key in
                                  ("integration_id", "candidate_id", "reviewer_id", "commit_sha", "snapshot_sha", "previous_head", "created_at")}
                                 for row in rows]}

    async def repository_status(self):
        # The head read takes the repository lock and checks the whole tree; do it off the
        # coordination thread, then read the publication ledger on a read connection.
        if not self.config.repository.enabled:
            return {"enabled": False}
        try:
            head = await asyncio.to_thread(lambda: self._repository().head())
        except ValueError as error:
            return {"enabled": True, "initialized": False, "branch": self.config.repository.branch,
                    "error": str(error)}
        return await self.db.read(lambda c: self._repository_summary(c, head))

    def _repository_refresh_report(self, worker_id):
        root = self.workspace(worker_id).resolve()
        path = _path(root, ".private/repository-refresh.json", private=True)
        if not path.exists():
            return {"conflicts": [], "resolved_paths": []}
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
            if (not isinstance(report, dict) or not isinstance(report.get("conflicts"), list)
                    or not isinstance(report.get("resolved_paths", []), list)):
                raise ValueError("Invalid refresh report")
            for conflict in report["conflicts"]:
                _relative(conflict["path"])
            if any(not isinstance(path, str) for path in report.get("resolved_paths", [])):
                raise ValueError("Invalid resolved paths")
            return report
        except (ValueError, KeyError, TypeError) as error:
            raise HubError("WORKSPACE_CONFLICT", "Repair the readable .private/repository-refresh.json conflict report before refreshing") from error

    def _restore_library_copy(self, worker_id):
        """Write back the Library files a resting worker's workspace left out (library_rest), before
        anything reads it; nothing for a workspace not at rest. Under its workspace lock, off the
        coordination thread."""
        written = restore_workspace(self.blobs, self.workspace(worker_id).resolve())
        if written is not None:
            logger.info("Restored the Library copy of worker %s: %d files", worker_id, written)

    def _refresh_repository_files(self, worker_id, base_commit, head):
        """Three-way file refresh outside the SQLite executor; private edits survive. A workspace at
        rest gets its Library files back first, so the refresh sees it as it was."""
        self._restore_library_copy(worker_id)
        repository = self._repository()
        report = self._repository_refresh_report(worker_id)
        resolved = set(report.get("resolved_paths", []))
        pending = {item["path"]: item for item in report["conflicts"] if item["path"] not in resolved}
        if base_commit == head and not resolved:
            return report
        current = {item["path"]: item["sha256"] for item in self.blobs.get_json(repository.snapshot(head, self.blobs))["files"]}
        baseline = ({item["path"]: item["sha256"] for item in self.blobs.get_json(repository.snapshot(base_commit, self.blobs))["files"]}
                    if base_commit is not None else {})
        root = self.workspace(worker_id).resolve()
        changes = []
        # Preflight all paths before any write. File/directory collisions become
        # explicit conflicts; linked paths remain forbidden. One listing per directory, and a
        # file unchanged since the workspace's last snapshot keeps the hash that snapshot read.
        # Only paths the publication changed, and open conflicts, are looked at: for any other
        # path the steps below change nothing. In the Linux pilot reading every Library file of
        # each of thirty workers back after a day took 44% of the service's CPU.
        view, known = TreeView(root), self.blobs.known_hashes(root)
        changed = {name for name in baseline.keys() | current.keys() if baseline.get(name) != current.get(name)}
        for name in sorted(changed | set(pending)):
            state, entry = view.state(name)
            target = root / name
            before, incoming = baseline.get(name), current.get(name)
            obstructed = state in {"directory", "other", "under_file"}
            local = None
            if state == "file":
                assert entry is not None
                cached = known.get(name)
                if cached is not None and cached[0] == snapshot_identity(entry.stat(follow_symlinks=False)):
                    local = cached[1]
                else:
                    local = self.blobs.put(target.read_bytes())
            if not obstructed and local == incoming:
                pending.pop(name, None)
                continue
            if name in resolved and before == incoming:
                continue
            if name not in pending and before == incoming:
                continue  # Public bytes did not change; this is private work.
            if not obstructed and name not in pending and local == before:
                changes.append((target, incoming))
                continue
            conflict = pending.get(name, {})
            conflict = {**conflict, "path": name, "base_sha256": conflict.get("base_sha256", before),
                        "local_sha256": local, "public_sha256": incoming,
                        "reason": "path_obstructed" if obstructed else "both_changed"}
            if incoming is not None:
                conflict["public_file"] = ".private/repository-incoming/" + incoming
            else:
                conflict.pop("public_file", None)
            pending[name] = conflict
        for target, incoming in changes:
            if incoming is None:
                target.unlink(missing_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                _writable(target)
                target.write_bytes(self.blobs.get(incoming))
        for conflict in pending.values():
            if conflict.get("public_file"):
                destination = _path(root, conflict["public_file"], private=True)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(self.blobs.get(conflict["public_sha256"]))
        report = {"base_commit": head, "previous_base_commit": base_commit,
                  "conflicts": list(pending.values()), "resolved_paths": [],
                  "guidance": "Private edits are preserved. Merge each conflict using its base/local/public hashes and public_file (null means deletion), then add its path to resolved_paths. The next refresh acknowledges those paths. Selected unresolved paths cannot be submitted."}
        report_path = _path(root, ".private/repository-refresh.json", private=True)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(canonical(report) + "\n", encoding="utf-8")
        return report

    async def refresh_repository_worker(self, worker_id, *, admission_attempt=None):
        """Bring a worker's Library checkout to the current head (three-way), and record the
        attempt's admission. A reviewer's claims leave out the checkout until it needs one
        (`ensure_library_copy`): it reads the published Library. In the Linux pilot a full copy
        for each new reviewer, some thirty an hour, was much of the eighth of the service's CPU
        that refreshing checkouts took, while only 29% of reviewers ever compiled anything."""
        if not self.config.repository.enabled:
            return None
        async with self._workspace_lock(worker_id):
            if admission_attempt is not None:
                ready = await self.db.read(lambda c: c.execute(
                    "SELECT 1 FROM attempt_workspaces WHERE attempt_id=?", (admission_attempt,)).fetchone())
                if ready:
                    return None  # Exact claim replay cannot refresh an already admitted workspace.
            repository = self._repository()
            head = await collected_thread_call(repository.head)
            def prepare(c):
                if not self._repository_head_recorded(c, head):
                    raise HubError("REPOSITORY_RECONCILIATION_REQUIRED", "Library publication is not yet reconciled")
                row = c.execute("SELECT base_commit FROM repository_bases WHERE worker_id=?", (worker_id,)).fetchone()
                binding = self._snapshot_binding(c, worker_id)
                if admission_attempt is not None and binding["active_attempt"] != admission_attempt:
                    raise HubError("STALE_BINDING", "Workspace admission no longer owns this attempt")
                return binding, row[0] if row else None, self._get(c, "workers", worker_id)["kind"]
            binding, previous, kind = await self.db.call(prepare)
            if admission_attempt is not None and kind == "review" and previous is None:
                def admit(c):
                    if self._snapshot_binding(c, worker_id) != binding:
                        raise HubError("STALE_BINDING", "Workspace ownership changed while its admission was recorded")
                    c.execute("INSERT INTO attempt_workspaces VALUES(?,?,?)", (admission_attempt, head, self.clock()))
                await self.db.write(admit)
                return None
            report = await collected_thread_call(self._refresh_repository_files, worker_id, previous, head)
            def commit(c):
                row = c.execute("SELECT base_commit FROM repository_bases WHERE worker_id=?", (worker_id,)).fetchone()
                if self._snapshot_binding(c, worker_id) != binding or (row[0] if row else None) != previous:
                    raise HubError("STALE_BINDING", "Workspace ownership changed while its Library checkout was prepared")
                c.execute("INSERT INTO repository_bases VALUES(?,?,?) ON CONFLICT(worker_id) DO UPDATE SET base_commit=excluded.base_commit",
                          (worker_id, head, self.clock()))
                if admission_attempt is not None:
                    c.execute("INSERT INTO attempt_workspaces VALUES(?,?,?)", (admission_attempt, head, self.clock()))
                return report
            return await self.db.write(commit)

    async def ensure_library_copy(self, worker_id):
        """Write the Library checkout of a worker that has none yet (a reviewer before its first
        `lean_check`), or whose checkout is at rest; the refresh report, or None when it already
        has one."""
        if not self.config.repository.enabled:
            return None
        if await self.db.read(lambda c: c.execute(
                "SELECT 1 FROM repository_bases WHERE worker_id=?", (worker_id,)).fetchone()) and not at_rest(
                    self.workspace(worker_id).resolve()):
            return None
        return await self.refresh_repository_worker(worker_id)

    async def prepare_assignment_workspace(self, result, *, tolerate_failure=False):
        """A claimed attempt cannot reach its caller/model until checkout finishes.

        The existing attempt/fence reserves ownership during I/O. A crash leaves
        that attempt for the existing no-model-dispatched recovery path.
        Replaying an ended claim returns its historical receipt without editing
        the worker's newer workspace.
        """
        if not self.config.repository.enabled or not isinstance(result, dict):
            return result
        if isinstance(result.get("assignment"), dict):
            prepared = await self.prepare_assignment_workspace(result["assignment"], tolerate_failure=tolerate_failure)
            return {**result, "assignment": prepared,
                    **({"reason": "WORKER_WORKSPACE_UNAVAILABLE"} if prepared is None else {})}
        if not {"id", "worker_id", "task_id", "run_id", "fencing_token"} <= result.keys():
            return result
        def current(c):
            attempt = c.execute("SELECT * FROM attempts WHERE id=?", (result["id"],)).fetchone()
            return bool(attempt and attempt["ended_at"] is None
                        and self._get(c, "workers", attempt["worker_id"])["active_attempt"] == attempt["id"])
        try:
            if not await self.db.read(current):
                return result
            await self.refresh_repository_worker(result["worker_id"], admission_attempt=result["id"])
        except BaseException as error:
            # Every failure releases the claim, a lock that stayed busy and a cancellation as much
            # as a damaged checkout: its caller never receives it, so nothing would start or end
            # it, and it would count as a live session until a restart (Linux pilot).
            if isinstance(error, asyncio.CancelledError):
                failure_reason = "Workspace preparation was cancelled before the claim was handed over"
            elif isinstance(error, (HubError, OSError, ValueError)):
                failure_reason = str(error)
            else:
                failure_reason = f"{type(error).__name__}: {error}"
            if not isinstance(error, HubError):
                logger.warning("Claim %s is released after its workspace preparation failed: %s",
                               result["id"], failure_reason)
            def fail(c):
                if not current(c):
                    return
                # No model has been admitted by this preparation. Never abandon
                # an attempt that another legitimate owner has already dispatched.
                if c.execute("SELECT 1 FROM turn_intents WHERE attempt_id=?", (result["id"],)).fetchone():
                    raise HubError("PREPARATION_ALREADY_DISPATCHED", "Recover the active native execution before workspace repair")
                c.execute("UPDATE attempts SET state='preparation_failed',ended_at=? WHERE id=?", (self.clock(), result["id"]))
                c.execute("UPDATE workers SET active_attempt=NULL,state='idle' WHERE id=?", (result["worker_id"],))
                # No turn was sent: retain the source continuation/interruption.
                c.execute("UPDATE continuations SET state=CASE WHEN wake_reason IN ('requirements_satisfied','evidence_changed') "
                          "THEN 'eligible' ELSE 'reconsider' END,resume_attempt=NULL WHERE resume_attempt=?", (result["id"],))
                c.execute("UPDATE interruption_checkpoints SET state='parked',resume_attempt=NULL WHERE resume_attempt=?", (result["id"],))
                self._refresh_message_wake(c, result["worker_id"])
                self._refresh_aggregate(c, result["task_id"])
                failure = WorkerWorkspaceError(result["worker_id"], result["task_id"], failure_reason)
                self._quarantine_workspace_failure(c, failure)
            # A cancelled caller still waits for the release, which no cancellation interrupts.
            await collected(asyncio.ensure_future(self.db.write(fail)))
            if tolerate_failure and isinstance(error, Exception):
                return None
            raise
        return result

    def _publication_paths(self, task, payload, plugin):
        hook = getattr(plugin, "publication_paths", None)
        inferred = hook(task, payload) if callable(hook) else []
        explicit = payload.get("repository_paths", [])
        if not isinstance(explicit, list) or not isinstance(inferred, list):
            raise HubError("INVALID_INPUT", "Repository paths must be an explicit list", 422)
        paths = [*inferred, *explicit]
        if len(paths) > 10000 or any(not isinstance(path, str) or not path for path in paths):
            raise HubError("INVALID_INPUT", "Repository publication paths must be bounded nonempty strings", 422)
        for path in paths:
            relative = Path(path)
            if (relative.is_absolute() or relative.drive or ":" in path or "\\" in path
                    or is_draft_path(path) or is_graph_path(path)
                    or any(part.upper().casefold() in {".", "..", ".git", ".task-based", ".taskhub-integration", ".private"} for part in path.split("/"))
                    or "" in path.split("/")):
                raise HubError("INVALID_PATH", "Repository paths must be safe relative source paths", 422)
        return sorted(set(paths))

    def _require_source_files(self, snapshot, paths):
        if not self.config.repository.enabled or not paths:
            raise HubError("SOURCE_FILES_REQUIRED", "Public contributions require selected source files and an initialized managed repository; private notes stay in workspace files", 422)
        manifest = self.blobs.get_json(snapshot)
        if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
            raise HubError("INVALID_SNAPSHOT", "An immutable workspace snapshot is required", 422)
        present = {item["path"] for item in manifest["files"]}
        if not any(path in present for path in paths):
            raise HubError("SOURCE_FILES_REQUIRED", "Select at least one authored file from the frozen workspace, including an explanation file for deletion-only changes", 422)

    def _bind_publication_repository(self, c, attempt, artifact, paths):
        if not paths or not self.config.repository.enabled:
            raise HubError("REPOSITORY_REQUIRED", "Public contributions require selected source files and an initialized managed repository")
        base = c.execute("SELECT base_commit FROM repository_bases WHERE worker_id=?", (attempt["worker_id"],)).fetchone()
        if base is None:
            raise HubError("REPOSITORY_BASE_REQUIRED", "The contributor needs a recorded repository base")
        report = self._repository_refresh_report(attempt["worker_id"])
        unresolved = {item["path"] for item in report["conflicts"]} - set(report.get("resolved_paths", []))
        if unresolved.intersection(paths):
            raise HubError("WORKSPACE_CONFLICT", "Resolve selected public-base conflicts in .private/repository-refresh.json before submitting")
        c.execute("INSERT INTO candidate_repositories VALUES(?,?,?,?)",
                  (artifact["id"], base["base_commit"], canonical(paths), self.clock()))

    def _candidate_repository(self, c, candidate_id):
        row = c.execute("SELECT * FROM candidate_repositories WHERE artifact_id=?", (candidate_id,)).fetchone()
        if row is None:
            return None
        return {"required": True, "base_commit": row["base_commit"], "paths": json.loads(row["paths_json"]),
                "branch": self.config.repository.branch}

    def _integration(self, c, integration_id):
        row = c.execute("SELECT * FROM review_integrations WHERE id=?", (integration_id,)).fetchone()
        if row is None:
            raise HubError("NOT_FOUND", "Unknown integration or validation round", 404)
        result = dict(row)
        owner = c.execute("SELECT b.worker_id FROM integration_batch_projections p "
                          "JOIN integration_batch_rounds r ON r.id=p.round_id "
                          "JOIN integration_batches b ON b.id=r.batch_id "
                          "WHERE p.integration_id=?", (integration_id,)).fetchone()
        if owner:
            result["integration_owner_id"] = owner[0]
        result.update(integration_id=result["id"], commit=result["commit_sha"], snapshot=result["snapshot_sha"])
        result["validation"] = json.loads(result.pop("validation_json")) if result["validation_json"] else None
        return result

    def _integration_queue(self, c):
        """Accepted reviews waiting for or included in an integration batch, in acceptance order."""
        return c.execute(
            "SELECT r.task_id AS review_task_id,a.reviewer_id,a.created_at AS accepted_at "
            "FROM review_requests r JOIN review_acceptances a ON a.review_task_id=r.task_id "
            "WHERE r.state='integrating' ORDER BY a.created_at,r.task_id").fetchall()

    async def integration_queue(self):
        """The current batch and each accepted review waiting for integration, for operators."""
        def read(c):
            batch = c.execute("SELECT * FROM integration_batches WHERE ended_at IS NULL").fetchone()
            selected = ({row["review_task_id"] for row in c.execute(
                "SELECT review_task_id FROM integration_batch_items WHERE batch_id=? AND state='included'", (batch["id"],))} if batch else set())
            items, position = [], 0
            deferred = {row["candidate_id"]: dict(row) for row in c.execute("SELECT * FROM integration_deferred")}
            for row in self._integration_queue(c):
                candidate = c.execute("SELECT candidate_id FROM review_requests WHERE task_id=?", (row["review_task_id"],)).fetchone()[0]
                holding = row["review_task_id"] in selected
                validation = self._validation_queue_status(c, candidate)
                position += 0 if holding else 1
                items.append({**dict(row), "holding": holding, "candidate_id": candidate,
                              "position": None if holding else position,
                              "can_run": candidate not in deferred and validation["validation_ready"], **validation,
                              "deferred_reason": deferred.get(candidate, {}).get("reason"),
                              "deferred_at": deferred.get(candidate, {}).get("created_at"), "integrator_id": batch["worker_id"] if holding else None})
            control = c.execute("SELECT * FROM integrator_control WHERE id=1").fetchone()
            rule = self._batch_rule(c)
            return {"mode": "batch", "holder": {"worker_id": batch["worker_id"], "batch_id": batch["id"],
                    "review_task_ids": sorted(selected), "granted_at": batch["created_at"], "state": batch["state"]} if batch else None,
                    "reviews": items, "last_finished_at": control["last_finished_at"] if control else None,
                    "delay_minutes": rule["delay_minutes"], "threshold": rule["threshold"]}
        return await self.db.read(read)

    def _unrecorded_publication(self, c, worker, attempt):
        """Identify a durable approval whose exact Git commit lacks a database receipt."""
        submission = json.loads(attempt["submission_json"] or "{}")
        if not submission.get("integration_batch"):
            return None
        batch = self._batch(c, submission["integration_batch"])
        if (batch["worker_id"] == worker["id"] and batch["state"] == "publishing"
                and batch.get("round") and self._repository().head() == batch["round"]["commit_sha"]):
            return {"id": batch["id"], "commit": batch["round"]["commit_sha"],
                    "snapshot": batch["round"]["snapshot_sha"]}
        return None

    def _hold_unrecorded_publication(self, c, worker, attempt):
        """Keep an approved intent when Git committed but the database rolled back.

        This only quarantines the unfinished attempt. Head equality is not proof
        of successful review completion; explicit reconciliation still verifies
        the recorded terminal, usage, original validation, and approval basis.
        """
        record = self._unrecorded_publication(c, worker, attempt)
        if record is None:
            return None
        c.execute("UPDATE attempts SET state='recovery_required' WHERE id=?", (attempt["id"],))
        c.execute("UPDATE workers SET state='recovery_required' WHERE id=?", (worker["id"],))
        c.execute("UPDATE tasks SET state=CASE WHEN state='retired' THEN state ELSE 'recovery_required' END "
                  "WHERE id=?", (attempt["task_id"],))
        self._notify(c, attempt["task_id"], record["id"], "repository_reconciliation_required")
        self._event(c, "repository.recovery_required", record["id"], {
            "attempt_id": attempt["id"], "commit": record["commit"],
            "reason": "published_commit_without_database_receipt",
        }, attempt["task_id"], worker["id"])
        return {"worker_id": worker["id"], "directive": "recovery_required",
                "integration_id": record["id"], "reason": "repository_reconciliation_required"}

    def _merge_published(self, c, candidate_id):
        record = self._get(c, "artifacts", candidate_id)
        if (record["kind"] in {"lean.definition", "lean.statement", "lean.proof"}
                or c.execute("SELECT 1 FROM candidate_bindings WHERE artifact_id=?", (candidate_id,)).fetchone()):
            return c.execute("SELECT 1 FROM repository_publications WHERE candidate_id=?", (candidate_id,)).fetchone() is not None
        # Operator configuration and machine evidence are native records, not
        # reviewed public notes. They do not masquerade as published sources.
        return True

    def _repository_command(self, c, name, p, actor):
        if name == "repository.initialize":
            self._role(actor, Role.OPERATOR)
            repository = self._repository()
            if c.execute("SELECT 1 FROM events WHERE kind='repository.initialized'").fetchone():
                self._require_recorded_repository_head(c, repository)
            if c.execute("SELECT 1 FROM attempts WHERE ended_at IS NULL").fetchone():
                raise HubError("EXECUTION_ACTIVE", "Initialize the public repository before admitting workers")
            if p.get("snapshot") is not None:
                from .plugins import load_plugin

                validate = getattr(load_plugin(self.config.domain), "validate_repository_snapshot", None)
                if callable(validate):
                    validate(self, p["snapshot"])
            result = repository.initialize(self.blobs, p.get("snapshot"))
            self._event(c, "repository.initialized", result["head"], result)
            return result
        return None
