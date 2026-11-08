from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import tempfile
import threading
import time
from collections import OrderedDict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Callable, TypeVar

from .draft_paths import is_draft_path, is_shared_draft_directory
from .graph_paths import is_graph_path, is_shared_graph_directory
from .snapshot_limits import MAX_SNAPSHOT_BYTES, MAX_SNAPSHOT_FILES, snapshot_bounds

T = TypeVar("T")
logger = logging.getLogger(__name__)
# A call is reported when it waited or ran this long; every call enters the window statistics.
SLOW_QUEUE_SECONDS = 10.0
SLOW_EXECUTE_SECONDS = 2.0
STATISTICS_WINDOW_SECONDS = 300.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


_COORDINATION = threading.local()


def on_coordination_thread() -> bool:
    """True inside a coordination database callback: periodic slow checks must wait for
    another thread, while changed or unverified content is still checked here."""
    return getattr(_COORDINATION, "active", False)


class CallStatistics:
    """Per-label counts and timings of coordination calls, in fixed windows; never payloads."""

    def __init__(self, window: float = STATISTICS_WINDOW_SECONDS, top: int = 10):
        self.window = window
        self.top = top
        self._lock = threading.Lock()
        self._started: float | None = None
        self._labels: dict[str, list[float]] = {}
        self.previous: dict[str, Any] | None = None

    def record(self, label: str, queue: float, execute: float, finished: float) -> dict | None:
        """Add one finished call; return the closed window's summary when this call ends it."""
        with self._lock:
            if self._started is None:
                self._started = finished
            entry = self._labels.setdefault(label, [0, 0.0, 0.0, 0.0, 0.0])
            entry[0] += 1
            entry[1] += execute
            entry[2] = max(entry[2], execute)
            entry[3] += queue
            entry[4] = max(entry[4], queue)
            if finished - self._started < self.window:
                return None
            summary = self._summary(finished)
            self.previous = summary
            self._started, self._labels = None, {}
            return summary

    def _summary(self, now: float) -> dict[str, Any]:
        labels = sorted(self._labels.items(), key=lambda item: (-item[1][1], item[0]))
        return {
            "ended_at": _utc_now(), "window_seconds": round(now - (self._started or now), 3),
            "calls": sum(int(entry[0]) for _, entry in labels),
            "execute_seconds": round(sum(entry[1] for _, entry in labels), 3),
            "max_queue_seconds": round(max((entry[4] for _, entry in labels), default=0.0), 3),
            "top": [{"label": label, "calls": int(entry[0]), "execute_seconds": round(entry[1], 3),
                     "max_execute_seconds": round(entry[2], 3), "queue_seconds": round(entry[3], 3),
                     "max_queue_seconds": round(entry[4], 3)} for label, entry in labels[:self.top]],
        }

    def snapshot(self, now: float) -> dict[str, Any]:
        with self._lock:
            return {"current": self._summary(now) if self._labels else None, "previous": self.previous}


async def collected(task: asyncio.Future) -> Any:
    """Wait for a cleanup task to end, however often the caller is cancelled meanwhile: the
    task's own failure is raised first, then the caller's cancellation."""
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


def atomic_write(path: Path, body: bytes, *, prefix: str = "tmp") -> None:
    """Replace `path` with `body` in one step: a reader sees the old file or all of the new one,
    and the bytes are on disk before the name points at them."""
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=prefix)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_json(path: Path, value: dict) -> None:
    """Publish one complete UTF-8 JSON file, creating its directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(path, (canonical(value) + "\n").encode("utf-8"), prefix=path.name + ".")


async def collected_thread_call(function, *args):
    """Never leave filesystem writes running after the caller is cancelled."""
    pending = asyncio.create_task(asyncio.to_thread(function, *args))
    cancelled = False
    while not pending.done():
        try:
            await asyncio.shield(pending)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            if not cancelled:
                raise
    if cancelled:
        if not pending.cancelled():
            pending.exception()
        raise asyncio.CancelledError
    return pending.result()


# Directories verified to contain no link in themselves or their ancestors. Project
# roots are registered once; workers cannot write above them.
_TRUSTED_DIRECTORIES: set[str] = set()


def _trust_key(path) -> str:
    return os.path.abspath(path)


def trusted_directory(path) -> bool:
    return _trust_key(path) in _TRUSTED_DIRECTORIES


def trust_directories(path) -> None:
    """Record an already verified link-free directory and all of its ancestors."""
    path = Path(os.path.abspath(path))
    _TRUSTED_DIRECTORIES.update(_trust_key(item) for item in (path, *path.parents))


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def digest(value) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


# Directories no source snapshot enters, by case-folded name.
SKIPPED_DIRECTORIES = frozenset({".git", ".lake", ".venv", "__pycache__", ".taskhub-integration", ".private"})
# A file changed this recently is read again: its metadata might not yet identify its content.
SNAPSHOT_SETTLED_NANOSECONDS = 2_000_000_000
# Workspaces whose files' hashes a BlobStore remembers between snapshots (about 0.5 MB each).
# 64 were fewer than the Linux pilot's workspaces active at once (about 50 live sessions, new
# reviewers and the workers between turns), so many turns read and hashed every file again.
SNAPSHOT_DIRECTORIES = 256


# Empty files Claude Code's sandbox creates at a workspace's top while a command runs, to mount its
# protected configuration paths read-only, and removes afterwards. A command cut off by a crash
# leaves them, and staging then refused the hidden path (Worker 254 in the Linux pilot, 2026-10-23
# and 10-30). Empty, they hold nothing to publish; a nonempty file of such a name is still refused.
SANDBOX_PLACEHOLDERS = frozenset({".bash_profile", ".bashrc", ".gitconfig", ".gitmodules", ".idea", ".mcp.json",
                                  ".profile", ".ripgreprc", ".vscode", ".zprofile", ".zshrc"})


def _sandbox_placeholder(entry: os.DirEntry) -> bool:
    try:
        return (entry.name in SANDBOX_PLACEHOLDERS and entry.is_file(follow_symlinks=False)
                and entry.stat(follow_symlinks=False).st_size == 0)
    except OSError:
        return False


def _is_directory(entry: os.DirEntry) -> bool:
    try:
        return entry.is_dir()  # Follows a link, as os.walk does to sort names from directories.
    except OSError:
        return False


def snapshot_entries(directory: Path) -> Iterator[tuple[str, os.DirEntry]]:
    """The files a source snapshot of `directory` covers, with their paths relative to it: none in
    a skipped directory, no private file, and no shared drafts or graphs or empty sandbox
    placeholders (SANDBOX_PLACEHOLDERS) at its top. A linked
    directory is an error; a linked file is returned for the caller to refuse. Like os.walk, an
    unreadable directory is passed over."""
    pending = [("", str(directory))]
    while pending:
        prefix, base = pending.pop()
        try:
            with os.scandir(base) as scan:
                entries = sorted(scan, key=lambda entry: entry.name)
        except OSError:
            continue
        top = not prefix
        for entry in entries:
            name = entry.name
            if _is_directory(entry):
                if top and (is_draft_path(name) or is_graph_path(name)) or name.upper().casefold() in SKIPPED_DIRECTORIES:
                    continue
                if entry.is_symlink():
                    raise ValueError("Linked directories are not snapshot inputs")
                pending.append((prefix + name + "/", entry.path))
            elif name.upper().casefold() != ".private" and not (
                    top and (is_draft_path(name) or is_graph_path(name) or _sandbox_placeholder(entry))):
                yield prefix + name, entry


def snapshot_identity(info: os.stat_result) -> tuple[int, ...]:
    """What identifies a file's content between snapshots, with its settling time."""
    return (info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_ino, info.st_dev)


def snapshot_files(directory: Path) -> Iterator[Path]:
    """The files a source snapshot of `directory` covers (snapshot_entries)."""
    for _, entry in snapshot_entries(directory):
        yield Path(entry.path)


class BlobStore:
    # A blob's name is its SHA-256. Bytes read and verified once are kept within this budget
    # and reused while the stored file keeps its size, modification time and ID.
    CACHE_BYTES = 64 * 1024 * 1024
    CACHE_ITEM_BYTES = 1024 * 1024

    def __init__(self, root: Path, *, max_files: int = MAX_SNAPSHOT_FILES,
                 max_bytes: int = MAX_SNAPSHOT_BYTES):
        self.root = root
        self.max_files, self.max_bytes = snapshot_bounds(max_files, max_bytes)
        self._lock = threading.RLock()
        self._cache: OrderedDict[str, tuple[tuple[int, int, int], bytes]] = OrderedDict()
        self._cached_bytes = 0
        # Per snapshot directory, each settled file's identity and hash (snapshot).
        self._file_hashes: OrderedDict[str, dict[str, tuple[tuple[int, ...], str]]] = OrderedDict()
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, body: bytes) -> str:
        with self._lock:
            return self._put(body)

    def _put(self, body: bytes) -> str:
        sha = hashlib.sha256(body).hexdigest()
        target = self.root / sha[:2] / sha
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            atomic_write(target, body)
        return sha

    def get(self, sha: str) -> bytes:
        with self._lock:
            return self._get(sha)

    def has(self, sha: str) -> bool:
        if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
            return False
        return (self.root / sha[:2] / sha).is_file()

    def _get(self, sha: str) -> bytes:
        if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
            raise ValueError("Invalid content hash")
        path = self.root / sha[:2] / sha
        info = os.stat(path)
        identity = (info.st_size, info.st_mtime_ns, info.st_ino)
        cached = self._cache.get(sha)
        if cached is not None and cached[0] == identity:
            self._cache.move_to_end(sha)
            return cached[1]
        body = path.read_bytes()
        if hashlib.sha256(body).hexdigest() != sha:
            raise ValueError("Blob integrity failure")
        settled = time.time_ns() - info.st_mtime_ns >= 2_000_000_000
        if settled and len(body) <= self.CACHE_ITEM_BYTES and len(body) == info.st_size:
            if cached is not None:
                self._cached_bytes -= len(cached[1])
            self._cache[sha] = (identity, body)
            self._cached_bytes += len(body)
            while self._cached_bytes > self.CACHE_BYTES:
                _, (_, evicted) = self._cache.popitem(last=False)
                self._cached_bytes -= len(evicted)
        return body

    def put_json(self, value) -> str:
        return self.put(canonical(value).encode("utf-8"))

    def get_json(self, sha: str):
        return json.loads(self.get(sha))

    def read(self, sha: str) -> bytes:
        """A blob's bytes, verified, without the store's lock or cache: for copying many blobs
        out (library_rest), which would otherwise hold the lock and evict the cache."""
        if not self.has(sha):
            raise ValueError("Missing or invalid content hash")
        body = (self.root / sha[:2] / sha).read_bytes()
        if hashlib.sha256(body).hexdigest() != sha:
            raise ValueError("Blob integrity failure")
        return body

    def snapshot(self, directory: Path, *, max_files: int | None = None,
                 max_bytes: int | None = None, exact: bool = False,
                 record: dict[str, tuple[tuple[int, ...], str]] | None = None) -> str:
        """The directory's files by content. An exact snapshot reads every file: content evidence,
        such as a candidate. Otherwise a settled file unchanged since the last snapshot keeps its
        hash. `record`, when given, receives each file's identity and hash."""
        max_files, max_bytes = snapshot_bounds(
            self.max_files if max_files is None else max_files,
            self.max_bytes if max_bytes is None else max_bytes,
        )
        project_root = next((parent.parent for parent in self.root.resolve().parents
                             if parent.name.upper().casefold() == ".task-based"), None)
        if project_root is not None and is_shared_draft_directory(directory, project_root):
            raise ValueError("Shared drafts cannot be selected as a source snapshot")
        if project_root is not None and is_shared_graph_directory(directory, project_root):
            raise ValueError("Shared graphs cannot be selected as a source snapshot")
        directory = directory.resolve()
        if any(part.upper().casefold() == ".private" for part in directory.parts):
            raise ValueError("Private draft/notes directories cannot be selected as a source snapshot")
        # A file whose size, times, inode and device are those of the last snapshot, settled
        # since, is not read again: blobs are never removed. In the pilot every turn's end
        # read and hashed a worker's ~1,400 files twice, holding the interpreter lock that the
        # coordination thread waits for after each SQLite statement.
        key = str(directory)
        with self._lock:
            known = {} if exact else self._file_hashes.get(key, {})
        current: dict[str, tuple[tuple[int, ...], str]] = {}
        files: list[dict[str, str]] = []
        total = 0
        for relative, entry in snapshot_entries(directory):
            if entry.is_symlink():  # No linked directory is entered, so only a linked file escapes.
                raise ValueError("Snapshot path escapes workspace")
            if len(files) >= max_files:
                raise ValueError("Snapshot file limit exceeded")
            info = entry.stat(follow_symlinks=False)
            size = info.st_size
            if size > max_bytes - total:
                raise ValueError("Snapshot byte limit exceeded")
            identity = snapshot_identity(info)
            cached = known.get(relative)
            if cached is not None and cached[0] == identity:
                sha = cached[1]
            else:
                with open(entry.path, "rb") as stream:
                    body = stream.read(size + 1)
                if len(body) != size:
                    raise ValueError("Snapshot file changed during capture")
                sha = self.put(body)
            if time.time_ns() - max(info.st_mtime_ns, info.st_ctime_ns) >= SNAPSHOT_SETTLED_NANOSECONDS:
                current[relative] = (identity, sha)
            if record is not None:
                record[relative] = (identity, sha)
            total += size
            files.append({"path": relative, "sha256": sha})
        with self._lock:
            self._file_hashes[key] = current
            self._file_hashes.move_to_end(key)
            while len(self._file_hashes) > SNAPSHOT_DIRECTORIES:
                self._file_hashes.popitem(last=False)
        return self.put_json({"files": sorted(files, key=lambda x: x["path"])})

    def known_hashes(self, directory: Path) -> dict[str, tuple[tuple[int, ...], str]]:
        """Each settled file the last snapshot of `directory` read: its identity and hash."""
        with self._lock:
            return self._file_hashes.get(str(Path(directory).resolve()), {})

    def restore(self, sha: str, destination: Path) -> None:
        destination.mkdir(parents=True, exist_ok=False)
        root = destination.resolve()
        for item in self.get_json(sha)["files"]:
            relative = Path(item["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Invalid snapshot path")
            target = root / relative
            if not target.resolve().is_relative_to(root):
                raise ValueError("Snapshot path escapes destination")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(self.get(item["sha256"]))


class ReadConnection:
    """A thread-affine, short-lived WAL snapshot for audited SQL-only queries."""

    def __init__(self, path: Path, number: int):
        self.path = path
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"taskhub-read-{number}")
        self.connection: sqlite3.Connection | None = None

    def query(self, function):
        if self.connection is None:
            connection = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True, autocommit=True)
            connection.row_factory = sqlite3.Row
            for pragma in ("query_only=ON", "busy_timeout=5000", "cache_size=-8192"):
                connection.execute("PRAGMA " + pragma)
            self.connection = connection
        connection = self.connection
        connection.execute("BEGIN")
        try:
            return function(connection)
        finally:
            # Release even a failed/cancelled read's snapshot; never pin the WAL.
            if connection.in_transaction:
                connection.execute("ROLLBACK")

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None


class Database:
    def __init__(self, path: Path, migration: str, on_open: Callable | None = None,
                 before_call: Callable | None = None, before_read: Callable | None = None):
        """`before_call` and `before_read` run on the thread of each coordination call and each
        read before it starts (the Hub clears that thread's per-call cache of native reads)."""
        self.path = path
        self.migration = migration
        self.on_open = on_open
        self.before_call = before_call
        self.before_read = before_read
        self._queue_lock = threading.Lock()
        self._pending_operations = 0
        self.statistics = CallStatistics()
        # The read connections' calls, summarized like the coordination thread's: each row a read
        # returns gives up the interpreter lock, so frequent wide reads slow every other thread.
        self.read_statistics = CallStatistics()
        self._maintenance_at = monotonic() + 1800
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="taskhub-db")
        self.connection: sqlite3.Connection | None = None
        self.closed = False
        self.readers = [ReadConnection(path, number) for number in range(4)]
        self._next_reader = 0

    def _connect(self) -> sqlite3.Connection:
        if self.connection is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fresh = not self.path.exists()
            connection = sqlite3.connect(self.path, autocommit=True)
            if fresh:
                connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
            try:
                connection.row_factory = sqlite3.Row
                for pragma in ("foreign_keys=ON", "journal_mode=WAL", "synchronous=FULL", "busy_timeout=5000", "cache_size=-65536"):
                    connection.execute("PRAGMA " + pragma)
                connection.executescript(self.migration)
                if self.on_open is not None:
                    self.on_open(connection)
            except BaseException:
                connection.close()
                raise
            self.connection = connection
        return self.connection

    async def call(self, function: Callable[[sqlite3.Connection], T], write: bool = False,
                   *, label: str | None = None) -> T:
        """Run on the single coordination thread. `label` names the operation (for example a
        command name) in diagnostics; it never carries a payload."""
        if self.closed:
            raise RuntimeError("Database is closed")
        queued_at = monotonic()
        name: str = label or str(getattr(function, "__qualname__", type(function).__name__))
        depth = 0

        def perform():
            started_at = monotonic()
            try:
                connection = self._connect()
                if self.before_call is not None:
                    self.before_call()
                if write:
                    connection.execute("BEGIN IMMEDIATE")
                _COORDINATION.active = True
                try:
                    value = function(connection)
                    if write:
                        connection.execute("COMMIT")
                    return value
                except BaseException:
                    if write and connection.in_transaction:
                        connection.execute("ROLLBACK")
                    raise
                finally:
                    _COORDINATION.active = False
            finally:
                finished = monotonic()
                queue_seconds = started_at - queued_at
                execute_seconds = finished - started_at
                if queue_seconds >= SLOW_QUEUE_SECONDS or execute_seconds >= SLOW_EXECUTE_SECONDS:
                    # Record only the operation's name and timing, never command bodies.
                    logger.warning(
                        "Slow coordination database call: at=%s callback=%s write=%s queue_depth=%d "
                        "queue_seconds=%.3f execute_seconds=%.3f",
                        _utc_now(), name, write, depth, queue_seconds, execute_seconds,
                    )
                summary = self.statistics.record(name, queue_seconds, execute_seconds, finished)
                if summary is not None:
                    logger.info("Coordination database summary: %s",
                                json.dumps(summary, separators=(",", ":"), sort_keys=True))

        with self._queue_lock:
            self._pending_operations += 1
            depth = self._pending_operations
        def finished(_):
            with self._queue_lock:
                self._pending_operations -= 1
        pending = self.executor.submit(perform)
        pending.add_done_callback(finished)
        return await asyncio.wrap_future(pending)

    async def read(self, function: Callable[[sqlite3.Connection], T]) -> T:
        """Only explicit readers bypass the coordination queue: SQL-only ones, and views of
        native files that verify what they read against this snapshot (NativeWorkflow._native_view).

        Authorization-and-action operations, delivery receipt creation and recovery remain on
        call(). Never route by write=False.
        """
        if self.closed:
            raise RuntimeError("Database is closed")
        if self.connection is None:
            await self.call(lambda connection: None)  # Schema/recovery runs on the writer only.
        if self.closed:
            raise RuntimeError("Database is closed")
        reader = self.readers[self._next_reader]
        self._next_reader = (self._next_reader + 1) % len(self.readers)
        queued_at, name = monotonic(), str(getattr(function, "__qualname__", type(function).__name__))

        def perform():
            started_at = monotonic()
            try:
                if self.before_read is not None:
                    self.before_read()
                return reader.query(function)
            finally:
                finished = monotonic()
                summary = self.read_statistics.record(name, started_at - queued_at, finished - started_at, finished)
                if summary is not None:
                    logger.info("Read database summary: %s", json.dumps(summary, separators=(",", ":"), sort_keys=True))
        return await asyncio.get_running_loop().run_in_executor(reader.executor, perform)

    async def write(self, function: Callable[[sqlite3.Connection], T], *, label: str | None = None) -> T:
        return await self.call(function, write=True, label=label)

    def diagnostics(self) -> dict[str, Any]:
        """Queue depth and windowed per-operation timings, readable without the coordination thread."""
        with self._queue_lock:
            pending = self._pending_operations
        return {"pending": pending, **self.statistics.snapshot(monotonic())}

    async def maintain(self, pages: int = 256) -> dict:
        """Bounded explicit maintenance, not a full vacuum on an active database."""
        if type(pages) is not int or not 1 <= pages <= 4096:
            raise ValueError("Maintenance pages must be between 1 and 4096")
        def maintain(connection):
            before = connection.execute("PRAGMA freelist_count").fetchone()[0]
            connection.execute(f"PRAGMA incremental_vacuum({pages})").fetchall()
            checkpoint = tuple(connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone())
            return {"free_pages_before": before,
                    "free_pages_after": connection.execute("PRAGMA freelist_count").fetchone()[0],
                    "checkpoint": checkpoint}
        return await self.call(maintain)

    async def maintain_if_idle(self) -> dict | None:
        """A bounded half-hourly pass; never queue behind application writes."""
        if self.closed or self.connection is None or monotonic() < self._maintenance_at:
            return None
        with self._queue_lock:
            if self._pending_operations:
                return None
        self._maintenance_at = monotonic() + 1800
        return await self.maintain()

    async def backup(self, target: Path) -> None:
        def copy(connection):
            if target.exists():
                raise FileExistsError(target)
            target.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(target) as output:
                connection.backup(output)
        await self.call(copy)

    async def close(self):
        if not self.closed:
            self.closed = True
            loop = asyncio.get_running_loop()
            await asyncio.gather(*(loop.run_in_executor(reader.executor, reader.close) for reader in self.readers))
            for reader in self.readers:
                reader.executor.shutdown(wait=True)
            def close_connection():
                if self.connection is not None:
                    self.connection.close()
                    self.connection = None
            await asyncio.get_running_loop().run_in_executor(self.executor, close_connection)
            self.executor.shutdown(wait=True)
