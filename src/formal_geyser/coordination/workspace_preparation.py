"""Prepare immutable workspace bytes before the short coordination transaction."""
from __future__ import annotations

import asyncio
import json
import logging
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from .artifact_files import ArtifactFiles
from .content import ContentError
from .errors import HubError
from .models import Role
from .storage import collected_thread_call

logger = logging.getLogger(__name__)
_SNAPSHOTS: ContextVar[dict | None] = ContextVar("prepared_workspace_snapshots", default=None)
SNAPSHOT_COMMANDS = frozenset({
    "artifact.stage", "artifact.submit", "artifact.register", "runtime.quiescent", "runtime.abandon",
    "runtime.park_interrupted", "interruption.import", "interruption.resume",
    "runtime.catalog_hold", "runtime.preflight_hold", "worker.recover",
    "review.reassign", "task.complete.cancel",
})


class SnapshotRequired(Exception):
    """Internal admission request; the transaction is rolled back before capture."""
    def __init__(self, worker_id):
        self.worker_id = worker_id
        super().__init__("Workspace snapshot must be prepared outside the transaction")


class WorkspacePreparation:
    if TYPE_CHECKING:
        db: Any
        blobs: Any
        workspace: Any
        _get: Any
        _resolve_worker_id: Any
        _active: Any
        _role: Any
        _require_current_mutation: Any
        _candidate_snapshot: Any
        _completion_record: Any
        content: Any
        config: Any
        _repository: Any
        _restore_library_copy: Any

    def _workspace_lock(self, worker_id):
        locks = self.__dict__.setdefault("_workspace_io_locks", {})
        return locks.setdefault(worker_id, asyncio.Lock())

    def _workspace_snapshot(self, worker_id):
        prepared = _SNAPSHOTS.get()
        if prepared is not None:
            if worker_id not in prepared:
                raise SnapshotRequired(worker_id)
            return prepared[worker_id]["snapshot"]
        self._restore_library_copy(worker_id)
        return self.blobs.snapshot(self.workspace(worker_id))

    def _prepared_candidate(self, worker_id):
        prepared = _SNAPSHOTS.get()
        return prepared[worker_id]["snapshot"] if prepared is not None and worker_id in prepared else None

    def _prepared_sources(self, snapshot):
        """Submission files this command wrote for `snapshot` before its transaction, if any."""
        for value in (_SNAPSHOTS.get() or {}).values():
            sources = value.get("sources")
            if sources is not None and sources.snapshot == snapshot and not sources.consumed:
                return sources
        return None

    def _staged_snapshot(self, c, command, worker_id):
        """Which snapshot this command will freeze as a mathematical submission archive, if any."""
        from .native import MATHEMATICAL_KINDS
        if command.name == "artifact.stage":
            return "candidate" if command.payload.get("kind") in MATHEMATICAL_KINDS else None
        if command.name != "runtime.quiescent":
            return None
        row = c.execute("SELECT a.submission_json FROM workers w JOIN attempts a ON a.id=w.active_attempt "
                        "WHERE w.id=?", (worker_id,)).fetchone()
        submission = json.loads(row[0]) if row and row[0] else {}
        snapshot = command.payload.get("snapshot")
        if (isinstance(submission, dict) and submission.get("kind") in MATHEMATICAL_KINDS
                and isinstance(snapshot, str) and len(snapshot) == 64
                and all(char in "0123456789abcdef" for char in snapshot)):
            return snapshot
        return None

    async def _prepare_submission_sources(self, command, prepared, worker_id, staged):
        """Write a submission's snapshot files off the coordination thread before staging: those
        that differ from the Library base of the worker's checkout, which the transaction
        rechecks with the rest of the workspace binding."""
        snapshot = prepared[worker_id]["snapshot"] if staged == "candidate" else staged
        base_commit = prepared[worker_id]["binding"].get("repository_base")

        def write():
            base = None
            if base_commit is not None and self.config.repository.enabled:
                base = (base_commit, self._repository().snapshot(base_commit, self.blobs))
            return ArtifactFiles(self.content).prepare_sources(self.blobs, snapshot, base)
        try:
            prepared[worker_id]["sources"] = await collected_thread_call(write)
        except (ContentError, HubError, OSError, ValueError, KeyError, TypeError) as error:
            # Staging then writes the files itself and reports any real defect.
            logger.warning("Submission files for %s were not prepared: %s", command.name, error)

    async def discard_prepared_sources(self, prepared):
        """Remove prepared submission files that the transaction did not use."""
        for value in (prepared or {}).values():
            sources = value.get("sources")
            if sources is not None and not sources.consumed:
                await collected_thread_call(ArtifactFiles(self.content).discard, sources)

    def _snapshot_binding(self, c, worker_id):
        worker = self._get(c, "workers", worker_id)
        binding = {key: worker[key] for key in ("id", "active_attempt", "thread_id", "usage_epoch", "current_task_id")}
        task = worker["current_task_id"]
        binding["task_revision"] = self._get(c, "tasks", task)["current_revision"] if task else None
        base = c.execute("SELECT base_commit FROM repository_bases WHERE worker_id=?", (worker_id,)).fetchone()
        binding["repository_base"] = base[0] if base else None
        return binding

    async def prepare_workspace_snapshots(self, worker_ids, *, candidate=False):
        result = {}
        for worker_id in dict.fromkeys(worker_ids):
            async with self._workspace_lock(worker_id):
                binding = await self.db.read(lambda c: self._snapshot_binding(c, worker_id))
                def capture():
                    self._restore_library_copy(worker_id)
                    if candidate:
                        return self._candidate_snapshot(worker_id)
                    first = self.blobs.snapshot(self.workspace(worker_id))
                    if self.blobs.snapshot(self.workspace(worker_id)) != first:
                        raise HubError("SNAPSHOT_CHANGED", "Workspace changed during snapshot preparation")
                    return first
                snapshot = await collected_thread_call(capture)
                result[worker_id] = {"binding": binding, "snapshot": snapshot}
        return result

    async def prepare_command_snapshots(self, command, actor, principal):
        if command.name not in SNAPSHOT_COMMANDS:
            return {}
        def select(c):
            if c.execute("SELECT 1 FROM receipts WHERE principal=? AND command=? AND key=?",
                         (principal, command.name, command.key)).fetchone():
                return []  # An exact replay never rereads a mutable workspace.
            self._require_current_mutation(c, command.name, command.payload, actor)
            if command.name.startswith("runtime."):
                self._role(actor, Role.RUNNER)
            if command.name == "worker.recover":
                self._role(actor, Role.OPERATOR)
            if command.name == "artifact.register" and actor.role != Role.WORKER:
                return []
            if actor.role == Role.WORKER:
                self._active(c, actor)
            worker_id = actor.worker_id or command.payload.get("worker_id")
            if command.name == "interruption.resume":
                worker_id = self._get(c, "interruption_checkpoints", command.payload["interruption_id"])["worker_id"]
            if command.name == "task.complete.cancel":
                worker_id = self._completion_record(c, command.payload["declaration_id"])["declarer_id"]
            if command.name == "review.reassign" and not worker_id:
                acceptance = c.execute("SELECT reviewer_id FROM review_acceptances WHERE review_task_id=?",
                                       (command.payload["task_id"],)).fetchone()
                worker_id = acceptance[0] if acceptance else None
            if not worker_id or worker_id == "operator":
                return []
            resolved = self._resolve_worker_id(c, worker_id)
            staging["snapshot"] = self._staged_snapshot(c, command, resolved)
            return [resolved]
        staging: dict[str, str | None] = {}
        identities = await self.db.call(select, label="command-prepare:" + command.name)
        prepared = await self.prepare_workspace_snapshots(identities, candidate=command.name in {"artifact.stage", "artifact.submit", "artifact.register"})
        if staging.get("snapshot") and identities:
            await self._prepare_submission_sources(command, prepared, identities[0], staging["snapshot"])
        return prepared

    def with_workspace_snapshots(self, c, prepared, callback):
        for worker_id, value in prepared.items():
            if self._snapshot_binding(c, worker_id) != value["binding"]:
                raise HubError("STALE_BINDING", "Execution changed during workspace preparation; retry from current state")
        token = _SNAPSHOTS.set(prepared)
        try:
            return callback(c)
        finally:
            _SNAPSHOTS.reset(token)
