"""File-authoritative task/record bodies and public knowledge locations.

The ledger stores operational identities and immutable file references. New
content is written before its transactional reference: rollback can leave an
unreferenced immutable file, never a partially changed live document.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from contextvars import ContextVar
from copy import deepcopy
from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .artifact_files import BASE_FILE, ArtifactFiles
from .content import (
    ContentError,
    FileReference,
    NativeContentStore,
    _file_identity,
    _normal_path,
    _path,
    _read,
    _write_new,
    verify_references,
)
from .drafts import DraftFiles, PreparedDraftUpdate
from .errors import HubError
from .literature import INDEX, LiteratureFiles
from .models import Actor, Role
from .storage import canonical, collected_thread_call, digest
from .wiki import KNOWLEDGE_README, ensure_guides

MATHEMATICAL_KINDS = frozenset({"lean.definition", "lean.statement", "lean.proof"})
RELOCATION_RETRY_SECONDS = 60.0
REFRESH_ENTRIES_SHOWN = 20
logger = logging.getLogger(__name__)
# The private record whose file a command wrote before its transaction (prepare_private_record):
# its ID, file and bytes, and whether the transaction took it.
_PREPARED_RECORD: ContextVar[dict | None] = ContextVar("prepared_private_record", default=None)


def _record_file_bytes(data: Any) -> bytes:
    """A record's file: its data as indented JSON."""
    return (json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")


def refresh_view(report: dict, report_path: Path) -> dict:
    """The refresh report as a turn input carries it: a long list keeps its first
    REFRESH_ENTRIES_SHOWN entries and states its length; the report file holds it whole.
    In the Linux pilot 6,270 Library files a crash had left empty put 3 MB of conflicts in
    each of one worker's inputs, past Claude's 1M-token limit, on every turn."""
    view = {**report, "report_path": str(report_path)}
    for key in ("conflicts", "resolved_paths"):
        entries = report.get(key) or []
        if len(entries) > REFRESH_ENTRIES_SHOWN:
            view[key] = entries[:REFRESH_ENTRIES_SHOWN]
            view[key + "_count"] = len(entries)
            view["shown"] = (f"Each list shows its first {REFRESH_ENTRIES_SHOWN} entries and its "
                             "length (*_count); report_path lists them all.")
    return view


class NativeWorkflow:
    if TYPE_CHECKING:
        root: Any
        state_dir: Any
        config: Any
        db: Any
        blobs: Any
        clock: Any
        workspace: Any
        _get: Any
        _active: Any
        _execution_environment: Any
        _task: Any
        _artifact: Any
        _repository: Any
        _require_recorded_repository_head: Any
        _repository_head_recorded: Any
        _worker_task: Any
        _integration: Any
        _candidate_repository: Any
        _repository_refresh_report: Any

        @property
        def _native_read_cache(self) -> dict: ...

        _verified_record_bodies: dict
        dag_files: Any
        _dag_reference: Any
        _role: Any
        _authorize_participant: Any
        _event: Any

    def _artifact_dependency_refs(self, c, artifact_id):
        return json.loads(self._get(c, "artifacts", artifact_id)["input_refs_json"])

    @property
    def content(self) -> NativeContentStore:
        store = getattr(self, "_content_store", None)
        if store is None:
            store = NativeContentStore(self.state_dir)
            self._content_store = store
        return store

    def _persist_task_content(self, c, task_id, revision, contract):
        if getattr(getattr(self, "_defer_archive_moves", None), "active", False):
            # Writing the revision's files (four fsyncs, under the content lock) must not hold
            # the coordination transaction. A command records them; Hub.execute writes them
            # after commit (reconcile_task_trees). Until then reads use the ledger's
            # hash-bound contract, as for historical tasks without files.
            c.execute("INSERT OR IGNORE INTO task_tree_intents VALUES(?,?,?)", (task_id, revision, self.clock()))
            self._task_trees_pending = True
            return None
        view = self.content.task_files(task_id, revision, contract)
        c.execute("INSERT INTO native_task_files VALUES(?,?,?)",
                  (task_id, revision, view.contract_sha256))
        return view

    def _with_deferred_files(self, c, callback):
        """Run a write callback whose new task files and archive moves are only recorded;
        the caller runs finish_deferred_files after the transaction commits."""
        state = getattr(self, "_defer_archive_moves", None)
        if state is None:
            return callback(c)
        state.active = True
        try:
            return callback(c)
        finally:
            state.active = False

    async def finish_deferred_files(self, label: str = "", *, archive_moves: bool = True) -> None:
        """Write the task files that committed transactions recorded, and have their archives moved.

        The archives are moved by the Hub's own background task (`_move_archives`), never in the
        command that recorded them; reads follow a move's journal or the old folder meanwhile. In
        the Linux pilot every command waited for the whole queue of moves once one recorded a move
        during a pass: from 05:07 to 05:17 on 2026-10-23 nothing else was done, and the pass ran
        for 56 minutes inside the Integrator's settling of its turn. Admission and the driver's
        recovery leave starting it to the next command (`archive_moves=False`).
        """
        if self._task_trees_due():
            try:
                await self.reconcile_task_trees()
            except Exception:
                # Committed: reads use the ledger contract until a retry writes the files.
                logger.exception("Task files after %s will be retried", label)
        if archive_moves and self._archive_moves_due():
            mover = getattr(self, "_archive_mover", None)
            if mover is None or mover.done():
                self._archive_mover = asyncio.create_task(self._move_archives(), name="archive-moves")

    async def _move_archives(self) -> None:
        """Move recorded archives until none is due, including those recorded meanwhile."""
        while self._archive_moves_due():
            try:
                await self.reconcile_archive_relocations()
            except Exception:
                # Committed: reads follow the journal or the old folder until a later command retries.
                logger.exception("Archive moves will be retried")
                self._archive_relocations_failed = True
                self._archive_relocation_retry_at = monotonic() + RELOCATION_RETRY_SECONDS
                return

    async def archive_moves_settled(self) -> None:
        """Wait until the background archive mover, if one runs, has nothing due."""
        mover = getattr(self, "_archive_mover", None)
        if mover is not None:
            await asyncio.shield(mover)

    async def stop_archive_moves(self) -> None:
        """Stop the background archive mover at shutdown. A move it was making finishes its
        file work first; what is left stays recorded and is finished at the next start."""
        mover = getattr(self, "_archive_mover", None)
        if mover is not None and not mover.done():
            mover.cancel()
            await asyncio.gather(mover, return_exceptions=True)

    def _task_trees_due(self) -> bool:
        return getattr(self, "_task_trees_pending", False) or (
            getattr(self, "_task_trees_failed", False)
            and monotonic() >= getattr(self, "_task_tree_retry_at", 0.0))

    async def reconcile_task_trees(self) -> list[tuple[str, int]]:
        """Write task revision files recorded by committed transactions, off the coordination thread."""
        lock = self.__dict__.setdefault("_task_tree_lock", asyncio.Lock())
        written: list[tuple[str, int]] = []
        async with lock:
            self._task_trees_pending = self._task_trees_failed = False
            pending = await self.db.read(lambda c: [tuple(row) for row in c.execute(
                "SELECT i.task_id,i.revision,r.contract_json,r.contract_hash FROM task_tree_intents i "
                "JOIN task_revisions r ON r.task_id=i.task_id AND r.revision=i.revision "
                "ORDER BY i.created_at,i.task_id,i.revision")])
            for task_id, revision, contract_json, contract_hash in pending:
                try:
                    view = await collected_thread_call(self.content.task_files, task_id, revision,
                                                       json.loads(contract_json))
                    if view.contract_sha256 != contract_hash:
                        raise ContentError("Task files differ from the recorded contract")

                    def finish(c, task_id=task_id, revision=revision, sha=view.contract_sha256):
                        if c.execute("DELETE FROM task_tree_intents WHERE task_id=? AND revision=?",
                                     (task_id, revision)).rowcount:
                            c.execute("INSERT OR IGNORE INTO native_task_files VALUES(?,?,?)", (task_id, revision, sha))
                    await self.db.call(finish, write=True, label="task:files-finish")
                except (ContentError, HubError, OSError, ValueError) as error:
                    # Still recorded: reads keep using the ledger contract until a retry writes them.
                    logger.warning("Files of task %s revision %s stay recorded for retry: %s", task_id, revision, error)
                    await self.db.call(lambda c, item=task_id, number=revision, reason=str(error)[:2000]: self._event(
                        c, "task.files_failed", item, {"revision": number, "reason": reason}), write=True,
                        label="task:files-failed")
                    self._task_trees_failed = True
                    self._task_tree_retry_at = monotonic() + RELOCATION_RETRY_SECONDS
                    continue
                written.append((task_id, revision))
        return written

    @property
    def draft_files(self) -> DraftFiles:
        return DraftFiles(self.content)

    def _draft_reference(self, c, task_id):
        row = c.execute("SELECT * FROM native_draft_heads WHERE task_id=?", (task_id,)).fetchone()
        return self.draft_files.reference(**dict(row)) if row is not None else None

    def _draft_update_base(self, c, p, actor):
        self._role(actor, Role.WORKER)
        attempt, assigned = self._active(c, actor)
        worker = self._get(c, "workers", actor.worker_id)
        if (worker.get("kind") != "ordinary" or attempt["execution_mode"] == "message_only"
                or assigned["contract"]["kind"] in {"review"}):
            raise HubError("ORDINARY_TASK_AUTHORITY_REQUIRED", "Only ordinary task execution may update public drafts", 403)
        task = self._task(c, p["task_id"])
        self._authorize_participant(c, task, actor)
        if task["state"] in {"done", "retired"} or task["contract"]["kind"] in {"review"}:
            raise HubError("DRAFT_TASK_CLOSED", "Draft updates require an unfinished ordinary task")
        expected = p.get("expected_revision")
        summary = p.get("summary")
        if type(expected) is not int or expected < 0:
            raise HubError("INVALID_INPUT", "Supply the current draft revision, or zero for the first revision", 422)
        if not isinstance(summary, str) or not summary.strip() or len(summary) > 4000:
            raise HubError("INVALID_INPUT", "Supply a concrete draft update summary of 1 to 4000 characters", 422)
        head = self._draft_reference(c, task["id"])
        if expected != (head["revision"] if head is not None else 0):
            raise HubError("DRAFT_REVISION_CONFLICT", "Read the task's current public draft before updating it", 409)
        return worker, task

    async def _prepare_draft_update(self, command, actor, principal, request_hash):
        def preflight(c):
            prior = c.execute("SELECT request_hash FROM receipts WHERE principal=? AND command=? AND key=?",
                              (principal, command.name, command.key)).fetchone()
            if prior:
                if prior["request_hash"] != request_hash:
                    raise HubError("IDEMPOTENCY_CONFLICT", "Idempotency key was reused with different data")
                return None
            return self._draft_update_base(c, command.payload, actor)

        base = await self.db.call(preflight)
        if base is None:
            return None
        worker, task = base
        p = command.payload

        def prepare():
            files = self.draft_files.capture(self.workspace(actor.worker_id), p["source_dir"])
            reference = self.draft_files.install(task["id"], p["expected_revision"] + 1,
                task["revision"], actor.worker_id, worker["display_name"], p["summary"], files)
            return PreparedDraftUpdate(reference, task["revision"])

        return await collected_thread_call(prepare)

    def _commit_draft_update(self, c, p, actor, prepared):
        _, task = self._draft_update_base(c, p, actor)
        if not isinstance(prepared, PreparedDraftUpdate) or prepared.task_revision != task["revision"]:
            raise HubError("STALE_REVISION", "The prepared draft's target task revision is no longer current")
        reference = prepared.reference
        c.execute("INSERT INTO native_draft_heads VALUES(?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET "
                  "revision=excluded.revision,content_key=excluded.content_key,manifest_sha256=excluded.manifest_sha256",
                  (task["id"], reference["revision"], reference["content_key"], reference["manifest_sha256"]))
        self._event(c, "draft.updated", task["id"], {"draft": reference, "summary": p["summary"]},
                    task["id"], actor.worker_id)
        return reference

    def _reconcile_drafts(self, c, task_id=None):
        query = "SELECT * FROM native_draft_heads"
        rows = c.execute(query + (" WHERE task_id=?" if task_id else ""),
                         (task_id,) if task_id else ()).fetchall()
        for row in rows:
            try:
                self.draft_files.point(self.draft_files.reference(**dict(row)))
            except (OSError, ValueError, KeyError, TypeError) as exc:
                # Rebuildable unreviewed navigation cannot prevent service startup.
                logging.getLogger(__name__).warning("Draft navigation needs repair for %s: %s",
                                                    row["task_id"], str(exc))

    async def reconcile_drafts(self, task_id=None):
        def heads(c):
            rows = c.execute("SELECT * FROM native_draft_heads" + (" WHERE task_id=?" if task_id else ""),
                             (task_id,) if task_id else ()).fetchall()
            return [self.draft_files.reference(**dict(row)) for row in rows]

        for reference in await self.db.call(heads):
            await collected_thread_call(self.draft_files.verify, reference)

            def point_current(c, verified=reference):
                # A concurrent newer commit owns navigation. An older replay
                # must never rewind the pointer, even if verification finishes late.
                if self._draft_reference(c, verified["task_id"]) == verified:
                    self.draft_files.point(verified, verified=True)

            await self.db.call(point_current, write=True)

    def _task_contract(self, c, row):
        native = c.execute("SELECT * FROM native_task_files WHERE task_id=? AND revision=?",
                           (row["task_id"], row["revision"])).fetchone()
        if native is None:
            return json.loads(row["contract_json"])  # Its files are written just after the revision commits.
        if native["contract_sha256"] != row["contract_hash"]:
            raise HubError("CONTENT_REFERENCE_CHANGED", "Task file identity differs from its active revision")
        key = ("task", row["task_id"], row["revision"], native["contract_sha256"])
        if key not in self._native_read_cache:
            self._native_read_cache[key] = self.content.read_task(
                row["task_id"], row["revision"], native["contract_sha256"])
        # Reuse only inside one serialized ledger operation; the next call
        # rechecks file bytes. Callers cannot mutate another reader's value.
        return deepcopy(self._native_read_cache[key])

    def _task_file_reference(self, c, task_id, revision):
        native = c.execute("SELECT contract_sha256 FROM native_task_files WHERE task_id=? AND revision=?",
                           (task_id, revision)).fetchone()
        if native is None:
            # Files recorded for writing after their commit are named already: the command
            # that recorded them answers only after reconcile_task_trees has written them.
            native = c.execute("SELECT r.contract_hash FROM task_tree_intents i JOIN task_revisions r "
                               "ON r.task_id=i.task_id AND r.revision=i.revision "
                               "WHERE i.task_id=? AND i.revision=?", (task_id, revision)).fetchone()
        if native is None:
            return None
        root = self.root / "tasks" / task_id / "revisions" / f"{revision}-{native[0]}" / "files"
        return {"root": str(root), "path": str(root / "task.json"), "task_id": task_id,
                "revision": revision, "contract_sha256": native[0]}

    async def prepare_private_record(self, command, actor) -> dict | None:
        """Write the file of a worker's private record (record.private) before its transaction, off
        the coordination thread: the transaction then only finds it. In the Linux pilot the fsync
        of this small file, made inside the transaction while the disk wrote back other work,
        held the coordination thread for up to 113 s. None when the command is not one, or its
        data cannot be written (the transaction then reports why)."""
        if command.name != "record.private" or actor.role != Role.WORKER or not actor.worker_id:
            return None
        data = command.payload.get("data")
        if not isinstance(data, dict):
            return None
        try:
            body = _record_file_bytes(data)
        except (TypeError, ValueError):
            return None
        record_id, owner = str(uuid4()), actor.worker_id

        def write():
            root = self.content.worker_notes(owner, self.workspace(owner))
            target = _path(root, f"records/{record_id}.json")
            _write_new(target, body)
            return target
        try:
            target = await collected_thread_call(write)
        except (ContentError, HubError, OSError, ValueError):
            return None
        return {"id": record_id, "owner": owner, "target": target, "used": False}

    def _with_prepared_record(self, c, prepared, callback):
        """Run a transaction in which _insert_artifact may take the prepared private record's ID."""
        token = _PREPARED_RECORD.set(prepared)
        try:
            return callback(c)
        finally:
            _PREPARED_RECORD.reset(token)

    def _prepared_record_id(self, producer) -> str | None:
        """The ID of the private record written before this transaction, once, for its producer."""
        prepared = _PREPARED_RECORD.get()
        if prepared is None or prepared["used"] or prepared["owner"] != producer:
            return None
        prepared["used"] = True
        return prepared["id"]

    async def discard_private_record(self, prepared, result) -> None:
        """Remove a prepared private record's file that no committed record took (the command
        failed, or replayed an earlier record)."""
        if prepared is None or (prepared["used"] and isinstance(result, dict) and result.get("id") == prepared["id"]):
            return
        try:
            await collected_thread_call(lambda: _normal_path(prepared["target"]).unlink(missing_ok=True))
        except OSError as error:
            # An unreferenced file in the worker's private notes; the command's own outcome stands.
            logger.warning("Unused private record file was not removed: %s", type(error).__name__)

    def _persist_record_content(self, c, record, data, *, private=False, source_paths=None):
        category = "artifact" if record["kind"] in MATHEMATICAL_KINDS else "note"
        owner = record.get("producer") if private else None
        if category == "artifact" and owner is None:
            bundle = data.get("bundle")
            sources = {}
            snapshot = record.get("snapshot") or (bundle.get("snapshot") if isinstance(bundle, dict) else None)
            # A command that stages a submission writes its snapshot files before the
            # transaction (prepare_command_snapshots); only the record and manifest remain.
            lookup = getattr(self, "_prepared_sources", None)
            prepared = lookup(snapshot) if snapshot and callable(lookup) else None
            if snapshot and prepared is None:
                sources = {item["path"]: self.blobs.get(item["sha256"])
                           for item in self.blobs.get_json(snapshot)["files"]}
            if isinstance(bundle, dict):
                for field in ("lean", "markdown"):
                    path, sha = bundle.get(field + "_path"), bundle.get(field + "_sha256")
                    if isinstance(path, str) and isinstance(sha, str):
                        known = prepared.refs.get("files/" + path) if prepared is not None else None
                        present = (known.sha256 if known is not None
                                   else hashlib.sha256(sources[path]).hexdigest() if path in sources else None)
                        if present is not None and present != sha:
                            raise HubError("CONTENT_CHANGED", "The paired source hashes differ from the frozen submission snapshot")
                        if known is None:
                            sources[path] = self.blobs.get(sha)
            target = ArtifactFiles(self.content).stage(record["id"], data, sources, prepared=prepared)
            c.execute("INSERT INTO native_record_files VALUES(?,?,?,?,?)",
                      (record["id"], target.relative_to(self.root).as_posix(),
                       hashlib.sha256(_read(target)).hexdigest(), category, None))
            self._pin_artifact_manifest(c, record["id"], target.parent)
            return
        if private and owner:
            root = self.content.worker_notes(owner, self.workspace(owner))
            target = _path(root, f"records/{record['id']}.json")
        else:
            folder = "artifacts" if category == "artifact" else "notes"
            target = _path(self.root, f"{folder}/{record['id']}/record.json")
        body = _record_file_bytes(data)
        if _normal_path(target).exists():
            if _read(target) != body:
                raise HubError("CONTENT_CONFLICT", "An immutable content record already differs")
        else:
            _write_new(target, body)
        if source_paths and category == "note" and not private:
            snapshot = {item["path"]: item["sha256"]
                        for item in self.blobs.get_json(record["snapshot"])["files"]}
            for relative in source_paths:
                if relative not in snapshot:
                    continue  # A selected deletion is interpreted against its recorded Git base.
                destination = _path(target.parent, "files/" + relative)
                source = self.blobs.get(snapshot[relative])
                if hashlib.sha256(source).hexdigest() != snapshot[relative]:
                    raise HubError("CONTENT_CHANGED", "A note source differs from its frozen snapshot")
                if _normal_path(destination).exists():
                    if _read(destination) != source:
                        raise HubError("CONTENT_CONFLICT", "A frozen note source already differs")
                else:
                    _write_new(destination, source)
        c.execute("INSERT INTO native_record_files VALUES(?,?,?,?,?)",
                  (record["id"], str(target.relative_to(self.root).as_posix()),
                   hashlib.sha256(body).hexdigest(), category, owner))
        # Mathematical candidate files are directly inspectable while the
        # authoritative evidence retains its immutable snapshot and bundle.
        bundle = data.get("bundle") if category == "artifact" else None
        if isinstance(bundle, dict) and record.get("snapshot"):
            for field in ("lean", "markdown"):
                path, sha = bundle.get(field + "_path"), bundle.get(field + "_sha256")
                if isinstance(path, str) and isinstance(sha, str):
                    source = self.blobs.get(sha)
                    destination = _path(target.parent, "files/" + path)
                    if _normal_path(destination).exists():
                        if _read(destination) != source:
                            raise HubError("CONTENT_CONFLICT", "Frozen candidate source differs")
                    else:
                        _write_new(destination, source)

    def _pin_artifact_manifest(self, c, artifact_id, directory):
        sha = hashlib.sha256(_read(directory / "manifest.json")).hexdigest()
        c.execute("INSERT INTO native_artifact_manifests VALUES(?,?) ON CONFLICT(record_id) "
                  "DO UPDATE SET manifest_sha256=excluded.manifest_sha256", (artifact_id, sha))

    def _record_body(self, c, record, *, allow_relocation=False, metadata_only=False):
        try:
            return self._read_record_body(c, record, allow_relocation=allow_relocation, metadata_only=metadata_only)
        except (OSError, ContentError):
            # A background move journals the new folder before it removes the old one, so a
            # read that found the old folder just before the removal succeeds when repeated.
            if not c.execute("SELECT 1 FROM archive_relocations WHERE record_id=?", (record["id"],)).fetchone():
                raise
            return self._read_record_body(c, record, allow_relocation=allow_relocation, metadata_only=metadata_only)

    def _read_record_body(self, c, record, *, allow_relocation=False, metadata_only=False):
        native = c.execute("SELECT * FROM native_record_files WHERE record_id=?",
                           (record["id"],)).fetchone()
        if native is None:
            raise HubError("CONTENT_REFERENCE_MISSING", "A record requires its authoritative native file; content-only records are unsupported")
        pinned, relocating = self._archive_ledger(c, record)
        value, target = self._checked_record_body(record, native, pinned, relocating, self._native_read_cache,
                                                  allow_relocation=allow_relocation, metadata_only=metadata_only)
        if record["kind"] not in MATHEMATICAL_KINDS:
            self._note_source_files(c, record, target.parent)
        return value

    def _archive_ledger(self, c, record):
        """An artifact archive's pinned manifest hash, and whether a committed move of it is pending."""
        if record["kind"] not in MATHEMATICAL_KINDS:
            return None, False
        pinned = c.execute("SELECT manifest_sha256 FROM native_artifact_manifests WHERE record_id=?",
                           (record["id"],)).fetchone()
        relocating = c.execute("SELECT 1 FROM archive_relocations WHERE record_id=?",
                               (record["id"],)).fetchone() is not None
        return (pinned[0] if pinned else None), relocating

    def _checked_record_body(self, record, native, pinned, relocating, shared, *,
                             allow_relocation=False, metadata_only=False):
        """Check a record file, and an artifact's archive, against the ledger facts its caller read.
        This reads no database, so the same check can also run before a coordination call;
        `shared` is the cache of that call, or None outside one."""
        was_relocated = False
        if native["owner_id"]:
            target = _path(self.content.worker_notes(native["owner_id"]), f"records/{record['id']}.json")
        else:
            target = _path(self.root, native["path"])
            if record["kind"] in MATHEMATICAL_KINDS:
                original = target
                target = ArtifactFiles(self.content).resolve(target, record["id"], record["body_sha"])
                was_relocated = target != original
        cache_key = None
        if record["kind"] in MATHEMATICAL_KINDS:
            cache_key = ("verified_record_body", record["id"], record["body_sha"],
                         native["sha256"], str(target), pinned, allow_relocation, metadata_only)
            if shared is not None and cache_key in shared:
                return deepcopy(shared[cache_key]), target
            # Across calls, a read of a pinned archive is reused while the record, manifest and
            # pair (or publication) files it checked keep their size, modification time and ID.
            remembered = self._verified_record_bodies.get(cache_key)
            if remembered is not None and _file_identity(remembered[0]) == remembered[1]:
                if shared is not None:
                    shared[cache_key] = deepcopy(remembered[2])
                return deepcopy(remembered[2]), target
        watched: list[Path] = []
        identity: tuple | None = () if cache_key is not None else None

        def watch(paths):
            # Identities are taken before the files are read, so a later change is never missed.
            nonlocal identity
            checked = _file_identity(paths) if identity is not None else None
            identity = identity + checked if identity is not None and checked is not None else None
            watched.extend(paths)
        if cache_key is not None:
            watch([target, target.parent / "manifest.json"])
        body = _read(target)
        if hashlib.sha256(body).hexdigest() != native["sha256"]:
            raise HubError("CONTENT_CHANGED", "An immutable record file was modified; submit a new revision")
        value = json.loads(body)
        if digest(value) != record["body_sha"]:
            raise HubError("CONTENT_CHANGED", "Record file does not match its immutable review identity")
        if record["kind"] in MATHEMATICAL_KINDS:
            verified_metadata = False
            if _normal_path(target.parent / "manifest.json").exists():
                manifest_body = _read(target.parent / "manifest.json")
                observed = hashlib.sha256(manifest_body).hexdigest()
                if (pinned is not None and pinned != observed and was_relocated
                        and not allow_relocation and relocating):
                    allow_relocation = True  # A committed move is finishing after its transaction.
                if pinned is not None and pinned != observed and not (allow_relocation and was_relocated):
                    raise HubError("ARCHIVE_RECONCILIATION_REQUIRED", "The frozen archive manifest changed or its relocation needs reconciliation")
                if metadata_only and pinned is not None and pinned == observed:
                    # Origin discovery needs pinned ownership metadata, not the
                    # contents of every unrelated historical source tree. Actual
                    # imported dependencies still pass full archive verification.
                    manifest_data = json.loads(manifest_body)
                    expected_identity = {"schema_version": 1, "artifact_id": record["id"],
                        "version": ArtifactFiles.version(value), "body_sha256": record["body_sha"]}
                    if any(manifest_data.get(k) != v for k, v in expected_identity.items()):
                        raise HubError("CONTENT_CHANGED", "Publication metadata identity differs from its archive")
                    refs = [FileReference.from_dict(item) for item in manifest_data["files"]
                            if item["path"] in {"record.json", "publication.json"}]
                    if sorted(ref.path for ref in refs) != ["publication.json", "record.json"]:
                        raise HubError("CONTENT_CHANGED", "Published ownership metadata is missing or duplicated")
                    watch([target.parent / "publication.json"])
                    verify_references(target.parent, refs)
                    verified_metadata = True
                elif pinned is None or pinned != observed:
                    # Without a pinned identity, or across an allowed relocation, check every file.
                    identity = None  # Such a full check is not reused across calls.
                    ArtifactFiles(self.content).verify(target.parent, record["id"],
                                                       ArtifactFiles.version(value), record["body_sha"])
                # Otherwise the manifest is the pinned one. The record file was checked above
                # and the Lean/Markdown pair is checked below; each other archived file is
                # checked against the manifest when it is read and when the archive moves.
                # Re-reading the whole archive on every record read protects nothing more.
            else:
                identity = None  # Historical layouts without a manifest are checked on every read.
            bundle = value.get("bundle")
            if isinstance(bundle, dict) and not verified_metadata:
                for field in ("lean", "markdown"):
                    relative, expected = bundle.get(field + "_path"), bundle.get(field + "_sha256")
                    if isinstance(relative, str) and isinstance(expected, str):
                        pair = _path(target.parent, "files/" + relative)
                        watch([pair])
                        frozen = _read(pair)
                        if hashlib.sha256(frozen).hexdigest() != expected:
                            raise HubError("CONTENT_CHANGED", "A frozen mathematical artifact file was modified")
        if cache_key is not None:
            # Clear at every serialized DB call. Every publication boundary
            # rehashes bytes; repeated dependency visits within it share that
            # verified immutable view. Relocation/manifest changes get new keys.
            if shared is not None:
                shared[cache_key] = deepcopy(value)
            if identity is not None:
                if len(self._verified_record_bodies) >= 8192:
                    self._verified_record_bodies.clear()
                self._verified_record_bodies[cache_key] = (watched, identity, deepcopy(value))
        return value, target

    def _note_source_files(self, c, record, root):
        """Resolve source files from their exact recorded selection, never from old bodies."""
        repository = self._candidate_repository(c, record["id"])
        if repository is None:
            authored_note = any(part in {"note", "notes", "wiki"}
                                for part in record["kind"].lower().split("."))
            reviewed = c.execute("SELECT 1 FROM candidate_bindings WHERE artifact_id=?",
                                 (record["id"],)).fetchone()
            if authored_note or reviewed is not None:
                raise HubError("CONTENT_REFERENCE_MISSING", "A note requires its authoritative source-file publication binding; content-only note records are unsupported")
            return None  # Operator configuration and private machine evidence are records.
        snapshot = {item["path"]: item["sha256"]
                    for item in self.blobs.get_json(record["snapshot"])["files"]}
        files, removed = [], []
        for relative in repository["paths"]:
            target = _path(root, "files/" + relative)
            if relative not in snapshot:
                removed.append(relative)
                if _normal_path(target).exists():
                    raise HubError("CONTENT_CHANGED", "A deleted note source unexpectedly exists")
                continue
            if hashlib.sha256(_read(target)).hexdigest() != snapshot[relative]:
                raise HubError("CONTENT_CHANGED", "An authoritative note source is missing or modified")
            files.append({"source_path": relative, "path": str(target), "sha256": snapshot[relative]})
        if not files:
            raise HubError("SOURCE_FILES_REQUIRED", "A public note must retain its selected source files")
        return {"source_files": files, "removed_paths": removed}

    def _record_files(self, c, record):
        native = c.execute("SELECT * FROM native_record_files WHERE record_id=?",
                           (record["id"],)).fetchone()
        if native is None:
            raise HubError("CONTENT_REFERENCE_MISSING", "A record requires its authoritative native file")
        target = (_path(self.content.worker_notes(native["owner_id"]), f"records/{record['id']}.json")
                  if native["owner_id"] else _path(self.root, native["path"]))
        if record["kind"] in MATHEMATICAL_KINDS and not native["owner_id"]:
            target = ArtifactFiles(self.content).resolve(target, record["id"], record["body_sha"])
        result = {"path": str(target), "sha256": native["sha256"],
                "root": str(target.parent), "artifact_id": record["id"],
                "category": native["category"], "owner_id": native["owner_id"]}
        if record["kind"] not in MATHEMATICAL_KINDS:
            result.update(self._note_source_files(c, record, target.parent) or {})
        return result

    def _relocate_artifact(self, c, artifact_id, area, receipt, extra_files=None, *, recovery=False):
        record = self._get(c, "artifacts", artifact_id)
        if record["kind"] not in MATHEMATICAL_KINDS:
            return None
        native = c.execute("SELECT * FROM native_record_files WHERE record_id=?", (artifact_id,)).fetchone()
        if native is None or native["owner_id"]:
            raise HubError("NATIVE_RECORD_REQUIRED", "Only a submitted mathematical record has an archive to move")
        store = ArtifactFiles(self.content)
        deferred = extra_files is None and getattr(getattr(self, "_defer_archive_moves", None), "active", False)
        pending = c.execute("SELECT area,receipt_json FROM archive_relocations WHERE record_id=?",
                            (artifact_id,)).fetchone()
        repeated = (pending is not None and deferred and pending["area"] == area
                    and pending["receipt_json"] == canonical(receipt))
        if pending is not None and not repeated:
            # An earlier move of this archive has not finished; complete it first, in order.
            self._finish_pending_relocation(c, artifact_id)
            native = c.execute("SELECT * FROM native_record_files WHERE record_id=?", (artifact_id,)).fetchone()
        original = _path(self.root, native["path"])
        source = store.resolve(original, artifact_id, record["body_sha"])
        data = self._record_body(c, record, allow_relocation=recovery and source != original)
        if repeated:
            # Exactly this move is already recorded; it finishes after its transaction.
            return str(store.location(artifact_id, store.version(data), area) / "record.json")
        if deferred and _normal_path(source.parent / "manifest.json").exists():
            # Copying and re-verifying every archived file must not hold the coordination
            # transaction. A command records the move; Hub.execute finishes it after commit
            # (reconcile_archive_relocations). Other callers still move the archive here.
            c.execute("INSERT INTO archive_relocations VALUES(?,?,?,?)",
                      (artifact_id, area, canonical(receipt), self.clock()))
            self._archive_relocations_pending = True
            return str(store.location(artifact_id, store.version(data), area) / "record.json")
        target = store.relocate(source.parent, artifact_id, data, area, receipt, extra_files)
        return self._record_relocation(c, record, data, area, target, extra_files)

    def _record_relocation(self, c, record, data, area, target, extra_files=None):
        """Bind a moved archive: check its exact file set, then point the ledger at it."""
        artifact_id = record["id"]
        bundle = data.get("bundle", {})
        snapshot = record.get("snapshot") or bundle.get("snapshot")
        source_entries = self.blobs.get_json(snapshot)["files"] if snapshot else []
        if all(isinstance(bundle.get(field + "_path"), str) for field in ("lean", "markdown")):
            manifest = json.loads(_read(target.parent / "manifest.json"))
            listed = {item["path"]: item["sha256"] for item in manifest["files"]}
            expected = {"record.json"}
            if BASE_FILE in listed:
                # Only what differs from its Library base: the base must be the candidate's.
                body = _read(target.parent / BASE_FILE)
                base = json.loads(body)
                row = c.execute("SELECT base_commit FROM candidate_repositories WHERE artifact_id=?",
                                (artifact_id,)).fetchone()
                if (hashlib.sha256(body).hexdigest() != listed[BASE_FILE] or base.get("snapshot") != snapshot
                        or row is None or base.get("base_commit") != row[0]):
                    raise HubError("CONTENT_CHANGED", "The frozen archive's Library base is not its candidate's")
                kept = {item["path"]: item["sha256"] for item in self.blobs.get_json(base["base_snapshot"])["files"]}
                source_entries = [item for item in source_entries if kept.get(item["path"]) != item["sha256"]]
                expected.add(BASE_FILE)
            expected.update({*["files/" + item["path"] for item in source_entries],
                             *["files/" + bundle[field + "_path"] for field in ("lean", "markdown")]})
            if area == "artifacts":
                expected.update({"publication.json", *[name for name in extra_files or {} if name.startswith("final/")]})
            elif area == "submission-history":
                expected.add("history.json")
            if set(listed) != expected:
                raise HubError("CONTENT_CHANGED", "Unexpected files appeared in the frozen mathematical archive")
        c.execute("UPDATE native_record_files SET path=? WHERE record_id=?",
                  (target.relative_to(self.root).as_posix(), artifact_id))
        self._pin_artifact_manifest(c, artifact_id, target.parent)
        return str(target)

    def _relocation_plan(self, c, artifact_id):
        row = c.execute("SELECT * FROM archive_relocations WHERE record_id=?", (artifact_id,)).fetchone()
        if row is None:
            return None
        record = self._get(c, "artifacts", artifact_id)
        native = c.execute("SELECT * FROM native_record_files WHERE record_id=?", (artifact_id,)).fetchone()
        source = ArtifactFiles(self.content).resolve(_path(self.root, native["path"]), artifact_id, record["body_sha"])
        return {"source": source.parent, "data": self._record_body(c, record, allow_relocation=True),
                "area": row["area"], "receipt": json.loads(row["receipt_json"]), "created_at": row["created_at"]}

    def _finish_relocation(self, c, artifact_id, plan, target):
        row = c.execute("SELECT * FROM archive_relocations WHERE record_id=?", (artifact_id,)).fetchone()
        if row is None or row["created_at"] != plan["created_at"] or row["area"] != plan["area"]:
            return None  # Another pass recorded this move first.
        result = self._record_relocation(c, self._get(c, "artifacts", artifact_id), plan["data"], plan["area"], target)
        c.execute("DELETE FROM archive_relocations WHERE record_id=?", (artifact_id,))
        return result

    def _finish_pending_relocation(self, c, artifact_id):
        """Complete a recorded move inside this transaction (only when another move must follow)."""
        plan = self._relocation_plan(c, artifact_id)
        target = ArtifactFiles(self.content).relocate(plan["source"], artifact_id, plan["data"],
                                                      plan["area"], plan["receipt"], None)
        return self._finish_relocation(c, artifact_id, plan, target)

    def _relocation_failed(self, c, artifact_id, plan, reason):
        row = c.execute("SELECT created_at FROM archive_relocations WHERE record_id=?", (artifact_id,)).fetchone()
        if row is None or row["created_at"] != plan["created_at"]:
            return False  # A command finished or replaced this move meanwhile.
        self._event(c, "archive.relocation_failed", artifact_id, {"reason": reason})
        return True

    def _archive_moves_due(self) -> bool:
        """Recorded moves are made right after their command, in the background; a failed one is
        retried after a later command at most once a minute, and at startup."""
        return getattr(self, "_archive_relocations_pending", False) or (
            getattr(self, "_archive_relocations_failed", False)
            and monotonic() >= getattr(self, "_archive_relocation_retry_at", 0.0))

    async def reconcile_archive_relocations(self) -> list[str]:
        """Finish archive moves recorded by committed transactions, off the coordination thread.

        Each move copies and verifies the archive, journals it and removes the old folder
        (ArtifactFiles.relocate), then a short transaction binds the new path and manifest.
        Reads follow the journal meanwhile. A failed move stays recorded and is retried.
        """
        lock = self.__dict__.setdefault("_relocation_lock", asyncio.Lock())
        moved: list[str] = []

        def recorded(c):
            # On the coordination thread, between transactions: a move recorded by a transaction
            # still open is either seen here or sets the flag again for another pass.
            self._archive_relocations_pending = self._archive_relocations_failed = False
            return [row[0] for row in c.execute(
                "SELECT record_id FROM archive_relocations ORDER BY created_at,record_id")]

        async with lock:
            pending = await self.db.call(recorded, label="archive:relocation-queue")
            store = ArtifactFiles(self.content)
            for artifact_id in pending:
                plan = await self.db.call(lambda c, item=artifact_id: self._relocation_plan(c, item),
                                          label="archive:relocation-plan")
                if plan is None:
                    continue
                try:
                    target = await collected_thread_call(store.relocate, plan["source"], artifact_id,
                                                         plan["data"], plan["area"], plan["receipt"], None)
                    finished = await self.db.call(lambda c, item=artifact_id, step=plan, path=target: self._finish_relocation(
                        c, item, step, path), write=True, label="archive:relocation-finish")
                except (ContentError, HubError, OSError, ValueError) as error:
                    # The move is still recorded; reads follow its journal or the old folder.
                    if await self.db.call(lambda c, item=artifact_id, step=plan, reason=str(error)[:2000]:
                                          self._relocation_failed(c, item, step, reason),
                                          write=True, label="archive:relocation-failed"):
                        logger.warning("Archive relocation of %s stays recorded for retry: %s", artifact_id, error)
                        self._archive_relocations_failed = True
                        self._archive_relocation_retry_at = monotonic() + RELOCATION_RETRY_SECONDS
                    continue
                if finished is not None:
                    moved.append(artifact_id)
        return moved

    def _archive_submission(self, c, artifact_id, disposition, reason):
        if disposition not in {"rejected", "withdrawn"} or not isinstance(reason, str) or not reason.strip():
            raise HubError("INVALID_INPUT", "Submission history requires an exact outcome and reason")
        # Removal from the current Library never destroys historical acceptance.
        if c.execute("SELECT 1 FROM repository_publications WHERE candidate_id=?", (artifact_id,)).fetchone():
            return None
        record = self._get(c, "artifacts", artifact_id)
        if record["kind"] not in MATHEMATICAL_KINDS:
            return None
        native = c.execute("SELECT * FROM native_record_files WHERE record_id=?", (artifact_id,)).fetchone()
        if native is None:
            raise HubError("NATIVE_RECORD_REQUIRED", "This mathematical record has no submitted files")
        store = ArtifactFiles(self.content)
        original = _path(self.root, native["path"])
        resolved = store.resolve(original, artifact_id, record["body_sha"])
        data = self._record_body(c, record, allow_relocation=resolved != original)
        receipt = {"artifact_id": artifact_id, "disposition": disposition, "reason": reason}
        history = store.location(artifact_id, store.version(data), "submission-history")
        pending = c.execute("SELECT area,receipt_json FROM archive_relocations WHERE record_id=?",
                            (artifact_id,)).fetchone()
        if pending is not None and pending["area"] == "submission-history":
            # A recorded move is filing this version in its history; keep that first outcome.
            target = self._relocate_artifact(c, artifact_id, "submission-history",
                                             json.loads(pending["receipt_json"]), recovery=True)
        elif resolved.parent == history and _normal_path(history / "history.json").exists():
            # A later withdrawal of rejected work retains the original refusal;
            # append the new disposition beside the immutable version directory.
            old_receipt = json.loads(_read(history / "history.json"))
            target = self._relocate_artifact(c, artifact_id, "submission-history", old_receipt, recovery=True)
        else:
            target = self._relocate_artifact(c, artifact_id, "submission-history", receipt, recovery=True)
        event_body = canonical({"version": store.version(data), **receipt}).encode("utf-8")
        event = _path(self.root, f"submission-history/{artifact_id}/events/{hashlib.sha256(event_body).hexdigest()}.json")
        if _normal_path(event).exists():
            if _read(event) != event_body:
                raise HubError("CONTENT_CHANGED", "Submission history event was modified")
        else:
            _write_new(event, event_body)
        return target

    def _archive_publication(self, c, candidate_id, publication):
        """Move an accepted, published contribution from staging into its archive."""
        record = self._get(c, "artifacts", candidate_id)
        if record["kind"] not in MATHEMATICAL_KINDS:
            return None
        integration = self._integration(c, publication["integration_id"])
        acceptance = c.execute("SELECT * FROM review_acceptances WHERE candidate_id=?", (candidate_id,)).fetchone()
        report = json.loads(acceptance["report_json"]) if acceptance is not None else {}
        if (acceptance is None or acceptance["reviewer_id"] != publication["reviewer_id"]
                or report.get("verdict") != "approve" or report.get("qualifications")):
            raise HubError("INDEPENDENT_ACCEPTANCE_REQUIRED", "Archive promotion requires the publication's unqualified independent acceptance")
        if (integration["state"] != "published" or integration["commit"] != publication["commit_sha"]
                or integration["snapshot"] != publication["snapshot_sha"]
                or not self._repository_head_recorded(c, publication["commit_sha"])):
            raise HubError("PUBLICATION_REQUIRED", "Only a successfully published integration enters the accepted archive")
        if c.execute("SELECT 1 FROM native_record_files WHERE record_id=?", (candidate_id,)).fetchone() is None:
            raise HubError("NATIVE_RECORD_REQUIRED", "This mathematical record has no submitted files")
        evidence = [dict(row) for row in c.execute("SELECT * FROM evidence WHERE artifact_id=? ORDER BY sequence", (candidate_id,))]
        receipt = {"artifact_id": candidate_id, "disposition": "published",
                   "publication": {key: value for key, value in dict(publication).items() if key != "created_at"},
                   "independent_acceptance": dict(acceptance), "validation": integration["validation"],
                   "submitted_snapshot": record["snapshot"], "evidence": evidence}
        return self._relocate_artifact(c, candidate_id, "artifacts", receipt, recovery=True)

    async def public_content(self):
        if not self.config.repository.enabled:
            return None
        repository = self._repository()
        try:
            # A due ownership walk runs here, off the coordination thread; the reads below reuse it.
            await collected_thread_call(repository.head)
            head = await self.db.call(lambda c: self._require_recorded_repository_head(c, repository))
        except (FileNotFoundError, ValueError) as error:
            raise HubError("LIBRARY_NOT_INITIALIZED", "Initialize the managed Library before starting workers") from error
        view = await asyncio.to_thread(self.content.public_view, repository, self.blobs, head)

        def current():
            target = self.content.library_root / "current.json"
            return json.loads(_read(target)) if _normal_path(target).exists() else None
        previous = await collected_thread_call(current)
        expected = {"schema_version": 1, "commit": view.commit,
                    "root": f"{view.commit}/files", "manifest_sha256": view.manifest_sha256}
        if previous == expected:
            return view
        old_commit = previous.get("commit") if isinstance(previous, dict) else None

        def navigation(c):
            if self._require_recorded_repository_head(c, repository) != view.commit:
                return False  # A newer publication owns the next navigation update.
            if previous is not None and not self._repository_head_recorded(c, old_commit):
                raise HubError("CONTENT_CONFLICT", "Library navigation no longer names a recorded public commit")
            return True
        if await self.db.call(navigation):
            # The view check and file replacement run off the coordination thread; the
            # replacement requires the pointer still to name the commit read above.
            await collected_thread_call(lambda: self.content.publish_current_view(view, expected_commit=old_commit))
            try:
                removed = await collected_thread_call(self.content.prune_public_views)
                if removed:
                    logger.info("Removed %d superseded Library views", len(removed))
            except (OSError, ValueError) as error:  # ContentError is a ValueError; the next publication retries.
                logger.warning("Superseded Library views were not removed: %s", error)
        return view

    async def library_status(self):
        view = await self.public_content()
        if view is None:
            return {"enabled": False, "files": [], "wiki": [], "knowledge": []}
        return {"enabled": True, **view.as_dict()}

    async def prepare_native_context(self, worker_id):
        """Supply file locations only; knowledge reading has no delivery ledger."""
        view = await self.public_content()
        notes = await asyncio.to_thread(self.content.worker_notes, worker_id, self.workspace(worker_id))
        guides = await asyncio.to_thread(ensure_guides, self.root)
        if self.config.domain == "lean":
            literature = LiteratureFiles(self.root)
            await asyncio.to_thread(literature.ensure_index)

        def prepare(c):
            from ..bootstrap_runtime import native_environment, resource_roots
            from .runtime.native_files import native_resource_paths

            prepared = self._execution_environment(c)
            worker = self._get(c, "workers", worker_id)
            if not worker["active_attempt"]:
                raise HubError("NO_ACTIVE_TASK", "Native preparation requires an active worker")
            attempt = self._get(c, "attempts", worker["active_attempt"])
            task = self._task(c, attempt["task_id"])
            wiki_root = view.root / "wiki" if view is not None and view.wiki else self.root / "wiki"
            published_readme = view.root / KNOWLEDGE_README if view is not None else None
            readme = published_readme if published_readme is not None and _normal_path(published_readme).is_file() else guides[KNOWLEDGE_README]
            native = {"mode": "native-files", "workspace": str(self.workspace(worker_id)),
                      "private_notes": str(notes), "current_task_id": self._worker_task(c, worker_id),
                      "task_files": self._task_file_reference(c, task["id"], task["revision"])}
            refresh = self._repository_refresh_report(worker_id)
            if refresh.get("conflicts"):
                native["repository_refresh"] = refresh_view(
                    refresh, self.workspace(worker_id) / ".private/repository-refresh.json")
            native["directories"] = {
                "tasks": str(self.root / "tasks"),
                "drafts": str(self.draft_files.root),
                "graphs": str(self.dag_files.root),
                "library": str(view.root if view is not None else self.root / "library"),
                "wiki": str(wiki_root), "knowledge_readme": str(readme),
                "public_notes": str(view.root / "notes" if view is not None else self.root / "notes"),
                "literature": str(self.root / "literature"),
                "literature_index": str(self.root / "literature" / INDEX),
                "staging": str(self.root / "staging"),
                "artifacts": str(self.root / "artifacts"),
                "submission_history": str(self.root / "submission-history"),
                "correspondence": str(self.root / "correspondence"),
                "workspace": str(self.workspace(worker_id)), "private_notes": str(notes),
            }
            native["read_only_resources"] = dict(self.config.environment.get("resource_roots", {}))
            native["read_only_resources"].update(resource_roots(prepared))
            native["read_only_resources"].update(native_resource_paths(self))
            if prepared is not None:
                native["lean_environment"] = native_environment(prepared)
            native["read_only_resources"]["drafts"] = str(self.draft_files.root)
            native["read_only_resources"]["graphs"] = str(self.dag_files.root)
            native["shared_dags"] = self._dag_reference(c)
            if task.get("drafts"):
                native["task_drafts"] = task["drafts"]
            central = native["read_only_resources"].get("central_literature")
            if central is not None:
                native["directories"]["central_literature"] = central
            if view is not None:
                native["library"] = {"root": str(view.root), "commit": view.commit,
                                     "wiki_root": str(wiki_root)}
            for relative, fallback in guides.items():
                if relative == KNOWLEDGE_README:
                    continue
                published = view.root / relative if view is not None else None
                location = published if published is not None and _normal_path(published).is_file() else fallback
                native["directories"][Path(relative).stem + "_wiki"] = str(location)
            return native
        return await self.db.call(prepare)

    def _record_reader(self, c, actor: Actor) -> None:
        if actor.role == Role.WORKER:
            self._active(c, actor)
        elif actor.role == Role.RUNNER and actor.worker_id:
            self._get(c, "workers", actor.worker_id)
        elif actor.role != Role.OPERATOR:
            raise HubError("FORBIDDEN", "Record reads require a project identity", 403)

    @staticmethod
    def _record_private(c, record: dict) -> bool:
        """A private note or diagnostic. Project members may read one another's by ID, but shared
        listings leave them out; reading a record never grants review or publication rights."""
        return record["status"] == "private" or c.execute(
            "SELECT 1 FROM private_records WHERE artifact_id=? "
            "UNION SELECT 1 FROM native_record_files WHERE record_id=? AND owner_id IS NOT NULL",
            (record["id"], record["id"])).fetchone() is not None

    async def _native_view(self, read):
        """A read that also opens native files, on a read connection: its files are read and
        checked there, never on the coordination thread (in the Linux pilot a worker's fetch of a
        record took the coordination thread 2 s on average while the disk was busy). A record the
        background archive mover moved between this snapshot and its file read is found by a
        second read, on a newer snapshot."""
        try:
            return await self.db.read(read)
        except (OSError, ContentError):
            return await self.db.read(read)

    async def native_record(self, record_id, actor):
        def read(c):
            self._record_reader(c, actor)
            self._get(c, "artifacts", record_id)
            return self._artifact(c, record_id)
        return await self._native_view(read)

    async def list_visible_records(self, actor: Actor, *, kind: str | None = None,
                                   kind_prefix: str | None = None, offset: int = 0,
                                   limit: int = 25, exclude_private: bool = False) -> dict:
        """Filter raw rows before paging or reading bodies.

        Shared discovery tools exclude private notes so large responses cannot
        copy private content into shared material blobs. Owners use native_record.
        """
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise HubError("INVALID_INPUT", "Invalid record pagination", 422)
        def read(c):
            self._record_reader(c, actor)
            visible = []
            for row in c.execute(
                    "SELECT id,kind,status,task_id,producer FROM artifacts "
                    "WHERE (? IS NULL OR kind=?) ORDER BY created_at,id", (kind, kind)):
                if kind_prefix is not None and not row["kind"].startswith(kind_prefix):
                    continue
                if not (exclude_private and self._record_private(c, row)):
                    visible.append(row["id"])
            page = visible[offset:offset + limit]
            items, unavailable = [], []
            for key in page:
                try:
                    items.append(self._artifact(c, key))
                except (OSError, ValueError, HubError) as error:
                    unavailable.append({"id": key, "code": getattr(error, "code", "CONTENT_UNAVAILABLE")})
            return {"items": items, "total": len(visible),
                    **({"unavailable": unavailable} if unavailable else {}),
                    "next_offset": offset + len(page) if offset + len(page) < len(visible) else None}
        return await self.db.call(read)

    async def list_notes(self, kind=None):
        def read(c):
            records = [self._artifact(c, row["id"]) for row in c.execute(
                "SELECT id,kind,status,task_id,producer FROM artifacts "
                "WHERE (? IS NULL OR kind=?) ORDER BY created_at,id", (kind, kind))
                if row["kind"] not in MATHEMATICAL_KINDS
                and not self._record_private(c, row)]
            return records
        return await self.db.call(read)

    async def list_math_artifacts(self):
        """List shared mathematical identities across staging and both archives."""
        return await self.db.call(lambda c: [self._artifact(c, row["id"]) for row in c.execute(
            "SELECT id,kind,status,task_id,producer FROM artifacts "
            "WHERE kind IN ('lean.definition','lean.statement','lean.proof') ORDER BY created_at,id")
            if not self._record_private(c, row)])
