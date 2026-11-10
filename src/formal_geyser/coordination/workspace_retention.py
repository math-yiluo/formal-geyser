"""Retiring long-idle workers, and reclaiming the workspaces of retired ones.

A reviewer is a fresh worker for one review, retired once that review ends, and the hub never
runs a retired worker again. In the Linux pilot their workspaces held 41 of the 51 GB of all
workspaces after three days: each kept a full copy of the Library, and many had installed
Python packages to read PDFs. Ordinary workers were never retired: new tasks start with new
workers, about 11 an hour, and three days later 723 of them held 44 GB. So an ordinary worker
with nothing of its own left (no task, no membership of an open task, no continuation, saved
turn, message or feedback owed, and no open task it published) is retired once it has had no
attempt for IDLE_RETIREMENT_MS.

A retired worker's workspace snapshot holds everything in its workspace but `.private`. Once
the workspace is shown to be exactly that stored snapshot (read again in full: every file
stored, nothing changed or added since), the rest of the workspace is removed, and so are the
Python environments, caches and scratch in `.private`. Its notes, its records, its material and
everything else in `.private` stay, as does any file registered literature points to.

Registering a worker creates its workspace before the transaction commits, so a registration
rolled back leaves an empty `workers/<id>/work` with no worker. Each pass also removes those,
once an hour old: any still empty, for a worker that does not exist.

Last, each pass rests the Library copies of up to REST_BATCH workers that have had no attempt
for REST_AFTER_MS (library_rest): their workspaces keep only what differs from their Library
base until something reads them again.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .library_rest import at_rest, read_record, record_path, rest_workspace, rested_snapshot
from .storage import collected_thread_call

logger = logging.getLogger(__name__)

RECLAIM_BATCH = 20  # Workspaces reclaimed in one pass: each is read in full first.
IDLE_RETIREMENT_MS = 48 * 3_600_000  # Without an attempt for this long, an idle worker retires.
RETIREMENT_BATCH = 100
# An empty workspace without a worker is removed once this old: its registration rolled back.
UNREGISTERED_SECONDS = 3600
# What stays in the workspace itself: what its snapshot leaves out and is not a cache.
KEPT_ENTRIES = frozenset({".private", ".git", ".taskhub-integration"})
# A directory in `.private` holding one of these at its top is a Python environment or cache:
# a virtual environment, a uv or pip cache; `--target` installs are known by their *.dist-info.
PYTHON_MARKERS = ("pyvenv.cfg", "CACHEDIR.TAG")
# Scratch in `.private`: the shells' temporary directory and rendered PDF pages.
SCRATCH = frozenset({"tmp"})
# A worker without an attempt for this long has its Library copy rested (library_rest). Each
# worker resumed after a rest has it written back (about 42k files in the Linux pilot), so a
# worker between turns of ordinary work keeps it.
REST_AFTER_MS = 2 * 3_600_000
# Workspaces rested in one pass: each is read in full first (about 270 MB in the Linux pilot).
REST_BATCH = 30

WORKSPACE_RETENTION_SCHEMA = """
CREATE TABLE IF NOT EXISTS workspace_reclamations(
 worker_id TEXT PRIMARY KEY REFERENCES workers(id),reclaimed_at INTEGER NOT NULL,bytes INTEGER NOT NULL);
"""
LAST_ACTIVE = ("COALESCE((SELECT MAX(COALESCE(a.ended_at,a.created_at)) FROM attempts a WHERE a.worker_id=w.id),"
               "w.created_at)")
# An ordinary worker with nothing of its own left; the caller adds the idle cutoff.
RETIRABLE = (
    "w.kind='ordinary' AND w.state='idle' AND w.work_intention='idle' AND w.active_attempt IS NULL "
    "AND w.current_task_id IS NULL AND w.pending_wake=0 AND " + LAST_ACTIVE + "<? "
    "AND NOT EXISTS(SELECT 1 FROM continuations k WHERE k.preferred_worker=w.id "
    "AND k.state IN ('eligible','reconsider','waiting')) "
    "AND NOT EXISTS(SELECT 1 FROM interruption_checkpoints i WHERE i.worker_id=w.id AND i.state='parked') "
    "AND NOT EXISTS(SELECT 1 FROM message_delivery_holds h WHERE h.worker_id=w.id AND h.state IN ('sending','uncertain')) "
    "AND NOT EXISTS(SELECT 1 FROM turn_intents t WHERE t.worker_id=w.id "
    "AND t.state IN ('prepared','sending','bound','uncertain')) "
    "AND NOT EXISTS(SELECT 1 FROM task_members m JOIN tasks t ON t.id=m.task_id WHERE m.worker_id=w.id "
    "AND m.membership_state='member' AND t.state NOT IN ('done','retired')) "
    "AND NOT EXISTS(SELECT 1 FROM task_completion_declarations d WHERE d.feedback_worker_id=w.id "
    "AND d.state='pending' AND d.feedback_state='pending') "
    # It may still hear about a task it published.
    "AND NOT EXISTS(SELECT 1 FROM task_publishers p JOIN tasks t ON t.id=p.task_id WHERE p.worker_id=w.id "
    "AND t.state NOT IN ('done','retired'))")
RECLAIMABLE = (
    "SELECT w.id,(SELECT a.snapshot FROM attempts a WHERE a.worker_id=w.id AND a.snapshot IS NOT NULL "
    "ORDER BY a.created_at DESC LIMIT 1) "
    "AS snapshot FROM workers w WHERE w.kind IN ('review','ordinary') AND w.state='retired' AND w.work_intention='retired' "
    "AND w.active_attempt IS NULL "
    "AND NOT EXISTS(SELECT 1 FROM workspace_reclamations r WHERE r.worker_id=w.id) "
    "AND NOT EXISTS(SELECT 1 FROM interruption_checkpoints i WHERE i.worker_id=w.id AND i.state='parked') "
    "AND NOT EXISTS(SELECT 1 FROM continuations k WHERE k.preferred_worker=w.id "
    "AND k.state IN ('waiting','eligible','reconsider')) "
    "AND snapshot IS NOT NULL ORDER BY w.created_at LIMIT ?")
# A worker whose Library copy may rest: one with a checkout and no attempt, turn, helper, message
# delivery or parked interruption (whose ticket names the workspace's exact snapshot), and no
# attempt since the cutoff that follows. The Integrator keeps its copy.
RESTABLE = (
    "w.kind IN ('ordinary','review') AND w.state IN ('idle','suspended') AND w.active_attempt IS NULL "
    "AND EXISTS(SELECT 1 FROM repository_bases b WHERE b.worker_id=w.id) AND " + LAST_ACTIVE + "<? "
    "AND NOT EXISTS(SELECT 1 FROM interruption_checkpoints i WHERE i.worker_id=w.id AND i.state='parked') "
    "AND NOT EXISTS(SELECT 1 FROM message_delivery_holds h WHERE h.worker_id=w.id AND h.state IN ('sending','uncertain')) "
    "AND NOT EXISTS(SELECT 1 FROM turn_intents t WHERE t.worker_id=w.id "
    "AND t.state IN ('prepared','sending','bound','uncertain')) "
    "AND NOT EXISTS(SELECT 1 FROM helper_intents h WHERE h.source_worker_id=w.id "
    "AND h.state IN ('prepared','running','uncertain'))")


def python_install(path: Path) -> bool:
    """A directory that is a Python environment, cache or `--target` install."""
    if path.is_symlink() or not path.is_dir():
        return False
    if any((path / marker).is_file() for marker in PYTHON_MARKERS):
        return True
    return any(child.name.endswith(".dist-info") and child.is_dir() for child in path.iterdir())


def _remove(path: Path, seen: set) -> int:
    """Remove a file or directory tree; the bytes it held on disk, each inode counted once."""
    tree = path.is_dir() and not path.is_symlink()
    freed = 0
    for entry in ([Path(directory) / name for directory, _, names in os.walk(path) for name in names]
                  if tree else [path]):
        try:
            stat = entry.lstat()
        except OSError:
            continue
        if (stat.st_dev, stat.st_ino) not in seen:
            seen.add((stat.st_dev, stat.st_ino))
            freed += stat.st_blocks * 512
    if tree:
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)
    return freed


def reclaim_workspace(work: Path, kept: list[Path]) -> int:
    """Remove a retired worker's snapshotted workspace entries and its Python environments
    and caches, but nothing that contains a path in `kept`; the bytes freed. An entry that
    cannot be removed is reported and left."""
    if not work.is_dir():
        return 0
    entries = [entry for entry in work.iterdir() if entry.name not in KEPT_ENTRIES]
    private = work / ".private"
    if private.is_dir() and not private.is_symlink():
        for entry in private.iterdir():
            try:
                if entry.name in SCRATCH or python_install(entry):
                    entries.append(entry)
            except OSError as error:
                logger.warning("Reclaiming %s: %s cannot be read: %s", work, entry.name, error)
    freed, seen = 0, set[tuple[int, int]]()
    for entry in entries:
        if any(path == entry or path.is_relative_to(entry) for path in kept):
            continue
        try:
            freed += _remove(entry, seen)
        except OSError as error:
            logger.warning("Reclaiming %s: %s cannot be removed: %s", work, entry.name, error)
    return freed


class WorkspaceRetention:
    if TYPE_CHECKING:
        db: Any
        blobs: Any
        config: Any
        state_dir: Path
        clock: Any
        _event: Any
        _repository: Any
        _workspace_lock: Any
        workspace: Any

    async def retire_idle_workers(self, limit: int = RETIREMENT_BATCH) -> list[str]:
        """Retire up to `limit` ordinary workers with nothing of their own left and no attempt for
        IDLE_RETIREMENT_MS; found on a reader, each checked again as it is retired."""
        cutoff = self.clock() - IDLE_RETIREMENT_MS
        found = await self.db.read(lambda c: [row[0] for row in c.execute(
            "SELECT w.id FROM workers w WHERE " + RETIRABLE + " ORDER BY w.created_at LIMIT ?", (cutoff, limit))])
        if not found:
            return []

        def retire(c):
            retired = []
            for worker_id in found:
                row = c.execute("SELECT " + LAST_ACTIVE + " FROM workers w WHERE w.id=? AND " + RETIRABLE,
                                (worker_id, cutoff)).fetchone()
                if row is None:
                    continue
                c.execute("UPDATE workers SET state='retired',work_intention='retired' WHERE id=?", (worker_id,))
                self._event(c, "worker.retired_idle", worker_id,
                            {"idle_hours": round((self.clock() - row[0]) / 3_600_000, 1)}, worker_id=worker_id)
                retired.append(worker_id)
            return retired
        return await self.db.write(retire)

    def _literature_in_workspaces(self) -> list[Path]:
        """The registered literature sources that lie in a worker's workspace (registered
        without a copy, they keep an absolute path)."""
        workers = (self.state_dir / "workers").resolve()
        found = []
        for record in (self.state_dir / "literature").glob("source-*.json"):
            try:
                source = json.loads(record.read_text(encoding="utf-8")).get("source_path") or ""
            except (OSError, ValueError):
                continue
            if os.path.isabs(source) and Path(os.path.realpath(source)).is_relative_to(workers):
                found.append(Path(os.path.realpath(source)))
        return found

    def _reclaim(self, rows: list[tuple[str, str]]) -> list[tuple[str, int]]:
        kept = self._literature_in_workspaces()
        done = []
        for worker_id, snapshot in rows:
            if not self.blobs.has(snapshot):
                # Without its snapshot the workspace's Library copy is its only record.
                logger.warning("Retired worker %s keeps its workspace: snapshot %s is missing", worker_id, snapshot)
                continue
            work = (self.state_dir / "workers" / worker_id / "work").resolve()
            if work.is_dir():
                # Read in full: every file stored again, and nothing changed or added since. A
                # workspace at rest counts the Library files a restore would write back.
                try:
                    record = read_record(work)
                    current = (self.blobs.snapshot(work, exact=True, remember=False) if record is None
                               else rested_snapshot(self.blobs, work, record))
                except (OSError, ValueError, KeyError, TypeError) as error:
                    logger.warning("Retired worker %s keeps its workspace: it cannot be read: %s", worker_id, error)
                    continue
                if current != snapshot:
                    logger.warning("Retired worker %s keeps its workspace: it differs from its last snapshot", worker_id)
                    continue
            done.append((worker_id, reclaim_workspace(work, kept)))
            self.blobs.forget(work)  # Its files have gone.
            record_path(work).unlink(missing_ok=True)  # Never run again: nothing is restored.
        return done

    def _remove_unregistered(self, registered: set[str]) -> int:
        """Remove each empty workspace, UNREGISTERED_SECONDS old, of a worker not in `registered`
        (read before this listing, so a worker registered since is new and kept)."""
        cutoff = time.time() - UNREGISTERED_SECONDS
        try:
            with os.scandir(self.state_dir / "workers") as listing:
                entries = [entry for entry in listing if entry.name not in registered]
        except FileNotFoundError:
            return 0
        removed = 0
        for entry in entries:
            folder = Path(entry.path)
            try:
                if (not entry.is_dir(follow_symlinks=False) or entry.stat(follow_symlinks=False).st_mtime > cutoff
                        or os.listdir(folder) != ["work"]
                        or os.stat(folder / "work", follow_symlinks=False).st_mtime > cutoff):
                    continue
                (folder / "work").rmdir()  # Refused unless empty.
                folder.rmdir()
            except OSError:
                continue
            removed += 1
        return removed

    async def reclaim_retired_workspaces(self, limit: int = RECLAIM_BATCH) -> list[tuple[str, int]]:
        """Retire long-idle workers, remove the empty workspaces rolled-back registrations left,
        then reclaim the workspaces of up to `limit` retired ones, off the coordination thread;
        each one's worker ID and the bytes freed."""
        retired = await self.retire_idle_workers()
        if retired:
            logger.info("Retired %d workers idle for %d hours or more", len(retired), IDLE_RETIREMENT_MS // 3_600_000)
        registered = await self.db.read(lambda c: {row[0] for row in c.execute("SELECT id FROM workers")})
        if removed := await asyncio.to_thread(self._remove_unregistered, registered):
            logger.info("Removed %d empty workspaces left by rolled-back worker registrations", removed)
        rows = await self.db.read(lambda c: [(row[0], row[1]) for row in c.execute(RECLAIMABLE, (limit,))])
        done = await asyncio.to_thread(self._reclaim, rows) if rows else []
        if done:
            now = self.clock()
            await self.db.call(lambda c: c.executemany(
                "INSERT OR IGNORE INTO workspace_reclamations(worker_id,reclaimed_at,bytes) VALUES(?,?,?)",
                [(worker_id, now, freed) for worker_id, freed in done]), write=True)
        if rested := await self.rest_library_copies():
            logger.info("Rested %d workers' Library copies: %d files, %.0f MiB", len(rested),
                        sum(files for _, files, _ in rested), sum(freed for _, _, freed in rested) / 2**20)
        return done

    async def rest_library_copies(self, limit: int = REST_BATCH) -> list[tuple[str, int, int]]:
        """Rest the Library copies of up to `limit` workers without an attempt for REST_AFTER_MS,
        longest at rest first (library_rest); each one's worker ID, files removed and bytes freed.
        An error leaves that worker's copy, and the others are rested. A workspace that could not
        be rested, or had nothing to remove, is not read again until its worker has run again."""
        if not self.config.repository.enabled:
            return []
        cutoff = self.clock() - REST_AFTER_MS
        found = await self.db.read(lambda c: [(row[0], row[1]) for row in c.execute(
            "SELECT w.id," + LAST_ACTIVE + " FROM workers w WHERE " + RESTABLE + " ORDER BY " + LAST_ACTIVE, (cutoff,))])
        workers = self.state_dir / "workers"
        # Per worker, its last activity when its workspace was last read without being rested.
        unrested: dict[str, int] = self.__dict__.setdefault("_unrested_workspaces", {})
        found = await asyncio.to_thread(lambda: [
            (worker_id, active) for worker_id, active in found
            if unrested.get(worker_id) != active and not at_rest(workers / worker_id / "work")][:limit])
        if not found:
            return []
        kept = await asyncio.to_thread(self._literature_in_workspaces)
        done = []
        for worker_id, active in found:
            try:
                files, freed = await self._rest_library_copy(worker_id, cutoff, kept)
            except Exception as error:
                logger.warning("Worker %s keeps its Library copy: %s: %s", worker_id, type(error).__name__, error)
                unrested[worker_id] = active
                continue
            if files:
                unrested.pop(worker_id, None)
                done.append((worker_id, files, freed))
            else:
                unrested[worker_id] = active
        return done

    async def _rest_library_copy(self, worker_id: str, cutoff: int, kept: list[Path]) -> tuple[int, int]:
        # Under the workspace lock, which every reader of the workspace takes and which restores
        # it first: a claim made meanwhile has its checkout prepared only after this.
        async with self._workspace_lock(worker_id):
            row = await self.db.read(lambda c: c.execute(
                "SELECT b.base_commit FROM workers w JOIN repository_bases b ON b.worker_id=w.id "
                "WHERE w.id=? AND " + RESTABLE, (worker_id, cutoff)).fetchone())
            if row is None:
                return 0, 0
            base_commit = row[0]
            base_snapshot = await collected_thread_call(self._repository().snapshot, base_commit, self.blobs)
            work = self.workspace(worker_id).resolve()
            return await collected_thread_call(rest_workspace, self.blobs, work, base_commit, base_snapshot,
                                               kept, self.clock())
