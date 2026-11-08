"""Library copies of workers at rest.

A worker's workspace holds a full checkout of the Library, but only an attempt reads it: every
process of an attempt is quiet before the attempt ends, no model turn starts before the claim's
checkout is prepared, and every snapshot is captured under the workspace lock. In the Linux
pilot on 2026-11-07, 945 workspaces held a copy (263 of 912 GB), almost all of workers waiting
or idle between attempts.

A worker at rest therefore keeps only what differs from its Library base. Its workspace is read
in full, the record beside it (`workers/<id>/library-at-rest.json`, outside the sandbox's
writable workspace) names the base and the workspace's exact manifest, and each file whose path
and bytes are the base's is removed. Before anything reads the workspace again, those files are
written back from the blob store, and the workspace is byte for byte what it was.
"""
from __future__ import annotations

import json
import os
import stat
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any

from .storage import atomic_json, snapshot_identity

RECORD = "library-at-rest.json"
# A restored file is written under this name in its directory, then renamed into place, so a
# path is either absent or complete. A crash can leave one; the next restore of a file in that
# directory overwrites it.
RESTORING = ".formal-geyser-restoring"


def record_path(work: Path) -> Path:
    """Where a rested workspace's record lives: beside `work/`, where no sandbox writes."""
    return work.parent / RECORD


def at_rest(work: Path) -> bool:
    return record_path(work).exists()


def _sha(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def read_record(work: Path) -> dict | None:
    """The workspace's rest record, or None when it is not at rest."""
    try:
        record = json.loads(record_path(work).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if (not isinstance(record, dict) or record.get("schema_version") != 1
            or not _sha(record.get("manifest")) or not _sha(record.get("base_snapshot"))):
        raise ValueError(f"Unreadable Library rest record {record_path(work)}")
    return record


def _manifest(blobs, sha: str) -> dict[str, str]:
    return {item["path"]: item["sha256"] for item in json.loads(blobs.read(sha))["files"]}


def restorable(blobs, record: dict) -> dict[str, str]:
    """The files resting may have removed: those of the recorded workspace that are its base's."""
    base = _manifest(blobs, record["base_snapshot"])
    return {path: sha for path, sha in _manifest(blobs, record["manifest"]).items() if base.get(path) == sha}


def _removable(info: os.stat_result) -> bool:
    """A plain file a restore writes back the same: one link, writable, not executable."""
    mode = info.st_mode
    return bool(stat.S_ISREG(mode) and info.st_nlink == 1 and mode & stat.S_IWUSR
                and not mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))


def rest_workspace(blobs, work: Path, base_commit: str, base_snapshot: str, kept: Iterable[Path],
                   now: int) -> tuple[int, int]:
    """Remove the files of `work` that are its Library base's, unchanged, and the directories that
    leaves empty; the files removed and the bytes freed. The workspace is read in full and the
    record written before anything is removed. A path in `kept` (registered literature) stays, and
    so does a file whose metadata changed since it was read."""
    if at_rest(work):
        return 0, 0
    files: dict[str, tuple[tuple[int, ...], str]] = {}
    manifest = blobs.snapshot(work, exact=True, record=files)
    base = _manifest(blobs, base_snapshot)
    keep = {path.relative_to(work).as_posix() for path in kept if path.is_relative_to(work)}
    candidates = sorted(path for path, (_, sha) in files.items() if base.get(path) == sha and path not in keep)
    if not candidates:
        return 0, 0
    atomic_json(record_path(work), {"schema_version": 1, "base_commit": base_commit,
                                    "base_snapshot": base_snapshot, "manifest": manifest, "rested_at": now})
    removed, freed, parents = 0, 0, set[Path]()
    for path in candidates:
        target = work / path
        try:
            info = os.lstat(target)
        except FileNotFoundError:
            continue
        if not _removable(info) or snapshot_identity(info) != files[path][0]:
            continue
        target.unlink()
        removed += 1
        freed += info.st_blocks * 512
        for parent in target.parents:
            if parent == work:
                break
            parents.add(parent)
    for directory in sorted(parents, key=lambda item: len(item.parts), reverse=True):
        try:
            directory.rmdir()  # Refused unless empty: only directories resting emptied go.
        except OSError:
            pass
    if not removed:
        record_path(work).unlink()
    return removed, freed


def restore_workspace(blobs, work: Path) -> int | None:
    """Write back the files resting removed from `work`, then remove its record; the files
    written, or None when it is not at rest. A path that exists is never overwritten."""
    record = read_record(work)
    if record is None:
        return None
    root = work.resolve()
    written, checked = 0, set[Path]()
    for path, sha in sorted(restorable(blobs, record).items()):
        relative = PurePosixPath(path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Invalid snapshot path")
        target = root / path
        if os.path.lexists(target):
            continue
        parent = target.parent
        if parent not in checked:
            existing = parent
            while not os.path.lexists(existing):
                existing = existing.parent
            if not Path(os.path.realpath(existing)).is_relative_to(root):
                raise ValueError("A restored path escapes the workspace")
            parent.mkdir(parents=True, exist_ok=True)
            checked.add(parent)
        body = blobs.read(sha)
        temporary = parent / RESTORING
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o666)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
        os.replace(temporary, target)
        written += 1
    record_path(work).unlink()
    return written


def rested_snapshot(blobs, work: Path, record: dict) -> str:
    """The snapshot `work` would have once restored: its files now, with each restorable file it
    lacks. Equal to the recorded manifest when nothing changed while it rested."""
    present = _manifest(blobs, blobs.snapshot(work, exact=True))
    for path, sha in restorable(blobs, record).items():
        present.setdefault(path, sha)
    return blobs.put_json({"files": [{"path": path, "sha256": sha} for path, sha in sorted(present.items())]})
