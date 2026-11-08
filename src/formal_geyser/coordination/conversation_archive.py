"""Compress the native files of conversations TaskHub has let go, and trim Codex's debug log.

Codex keeps each conversation in `codex.home/sessions/YYYY/MM/DD/rollout-<time>-<thread>.jsonl`,
Claude Code in `claude.home/projects/<folder>/<session>.jsonl`. TaskHub resumes a conversation only
while a worker, a live turn, a parked interruption, a live rotation or a running decision is bound
to it; once released (a rotation, a new task, a new batch, retirement, a backend switch) it never
opens it again, and its own transcript keeps what the conversation said. In the Linux pilot these
files held 96 GB (Codex 82.6, Claude 13.4), about 2,400 Codex files a day. A file of a conversation
no one is bound to, unchanged for ARCHIVE_AFTER_SECONDS, is compressed in place
(`<name>.jsonl.gz`): kept for the operator, a fifth or less of its size.

Codex also logs to `codex.home/logs_2.sqlite` for its own diagnostics, about 3 days of rows: 11.5 GB
there, growing. Rows older than CODEX_LOG_DAYS are deleted in bounded steps, and the pages given
back (the database frees pages as it goes).
"""
from __future__ import annotations

import gzip
import os
import shutil
import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

ARCHIVE_AFTER_SECONDS = 3600
ARCHIVE_BYTES_PER_PASS = 4 * 1024**3  # Uncompressed bytes compressed in one pass.
CODEX_LOG_DAYS = 1
LOG_ROWS = 5000  # Log rows deleted in one transaction: Codex writes meanwhile.
LOG_VACUUM_PAGES = 25_000
_ID = 36  # A conversation id: a UUID in its text form.


def bound_conversations(c) -> set[str]:
    """Every conversation TaskHub may still open: bound to a worker, a turn not yet ended, a parked
    interruption, a live rotation (both its old and its new one) or a decision not yet completed."""
    found: set[str] = set()
    for sql in ("SELECT thread_id FROM workers WHERE thread_id IS NOT NULL",
                "SELECT thread_id FROM turn_intents WHERE thread_id IS NOT NULL AND state IN "
                "('prepared','sending','bound','uncertain')",
                "SELECT thread_id FROM interruption_checkpoints WHERE state='parked'",
                "SELECT thread_id FROM rotations WHERE state IN ('requested','ready','started')",
                "SELECT new_thread_id FROM rotations WHERE new_thread_id IS NOT NULL AND state IN "
                "('requested','ready','started')",
                "SELECT thread_id FROM helper_intents WHERE thread_id IS NOT NULL AND state NOT IN "
                "('completed','cancelled','failed')"):
        try:
            found.update(row[0] for row in c.execute(sql) if row[0])
        except sqlite3.OperationalError:
            continue  # A table an older or smaller project lacks.
    return found


def conversation_files(codex_homes: list[Path], claude_homes: list[Path]) -> Iterator[tuple[str, Path]]:
    """(conversation id, file) of every uncompressed native conversation file."""
    for home in codex_homes:
        for directory, _, names in os.walk(home / "sessions"):
            for name in names:
                if name.startswith("rollout-") and name.endswith(".jsonl") and len(name) > _ID + 6:
                    yield name[-(_ID + 6):-6], Path(directory) / name
    for home in claude_homes:
        projects = home / "projects"
        if not projects.is_dir():
            continue
        for folder in projects.iterdir():
            if folder.is_dir() and not folder.is_symlink():
                for path in folder.glob("*.jsonl"):
                    if len(path.stem) == _ID:
                        yield path.stem, path


def compress(path: Path) -> int:
    """Replace `path` by `path.gz`, keeping its modification time; the bytes it held."""
    size = path.stat().st_size
    target = path.with_name(path.name + ".gz")
    temporary = path.with_name("." + path.name + ".gz.tmp")
    with open(path, "rb") as source, gzip.open(temporary, "wb", compresslevel=6) as sink:
        shutil.copyfileobj(source, sink, 1024 * 1024)
    with open(temporary, "rb") as written:
        os.fsync(written.fileno())
    stat = path.stat()
    os.utime(temporary, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    os.replace(temporary, target)
    path.unlink()
    return size


def archive(codex_homes: list[Path], claude_homes: list[Path], bound: set[str], *, now: float | None = None,
            budget: int = ARCHIVE_BYTES_PER_PASS) -> dict[str, int]:
    """Compress the files of released conversations unchanged for ARCHIVE_AFTER_SECONDS, at most
    `budget` bytes of them; what it did."""
    now = time.time() if now is None else now
    done = {"files": 0, "bytes": 0, "compressed_bytes": 0}
    for conversation, path in conversation_files(codex_homes, claude_homes):
        if conversation in bound or done["bytes"] >= budget:
            continue
        try:
            if now - path.stat().st_mtime < ARCHIVE_AFTER_SECONDS:
                continue
            size = compress(path)
        except FileNotFoundError:
            continue  # Moved or removed meanwhile.
        done["files"] += 1
        done["bytes"] += size
        done["compressed_bytes"] += path.with_name(path.name + ".gz").stat().st_size
    return done


def trim_codex_log(path: Path, *, now: float | None = None, days: float = CODEX_LOG_DAYS) -> dict[str, int]:
    """Delete Codex's debug-log rows older than `days`, a bounded step at a time, then give the
    freed pages back; the rows deleted and pages freed."""
    if not path.is_file():
        return {"rows": 0, "pages_freed": 0}
    now = time.time() if now is None else now
    db = sqlite3.connect(str(path), timeout=30, isolation_level=None)
    try:
        rows = 0
        while True:
            count = db.execute("DELETE FROM logs WHERE id IN (SELECT id FROM logs WHERE ts<? LIMIT ?)",
                               (int(now - days * 86400), LOG_ROWS)).rowcount
            rows += count
            if count < LOG_ROWS:
                break
        freed = 0
        if db.execute("PRAGMA auto_vacuum").fetchone()[0] == 2:
            while free := db.execute("PRAGMA freelist_count").fetchone()[0]:
                db.execute(f"PRAGMA incremental_vacuum({min(free, LOG_VACUUM_PAGES)})").fetchall()
                freed += min(free, LOG_VACUUM_PAGES)
        return {"rows": rows, "pages_freed": freed}
    finally:
        db.close()


class ConversationArchive:
    if TYPE_CHECKING:
        config: Any
        db: Any

    def _native_homes(self) -> tuple[list[Path], list[Path]]:
        homes = []
        for backend in ("codex", "claude"):
            home = getattr(getattr(self.config, backend, None), "home", None)
            homes.append([Path(home).expanduser()] if home else [])
        return homes[0], homes[1]

    async def archive_native_conversations(self) -> dict[str, Any]:
        """One pass: compress released conversations' files and trim Codex's debug log."""
        import asyncio

        codex, claude = self._native_homes()
        if not codex and not claude:
            return {"files": 0, "bytes": 0, "compressed_bytes": 0}
        bound = await self.db.read(bound_conversations)
        report: dict[str, Any] = await asyncio.to_thread(archive, codex, claude, bound)
        if codex:
            report["codex_log"] = await asyncio.to_thread(trim_codex_log, codex[0] / "logs_2.sqlite")
        return report
