from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from formal_geyser.coordination.config import initialize
from formal_geyser.coordination.models import ProjectConfig
from formal_geyser.coordination.storage import BlobStore, Database, canonical


def test_blob_round_trip_and_hash_integrity(tmp_path):
    store = BlobStore(tmp_path / "objects")
    first = store.put(b"immutable")
    assert store.put(b"immutable") == first
    assert store.get(first) == b"immutable"
    (store.root / first[:2] / first).write_bytes(b"altered")
    with pytest.raises(ValueError, match="integrity"):
        store.get(first)


def test_snapshot_restores_exact_files_without_shared_mutable_workspace(tmp_path):
    store = BlobStore(tmp_path / "objects")
    source = tmp_path / "source"
    (source / "nested").mkdir(parents=True)
    (source / "nested" / "data.txt").write_text("Version one", encoding="utf-8")
    (source / ".git").mkdir()
    (source / ".git" / "ignored").write_text("excluded", encoding="utf-8")
    snapshot = store.snapshot(source)
    (source / "nested" / "data.txt").write_text("Version two", encoding="utf-8")
    target = tmp_path / "restored"
    store.restore(snapshot, target)
    assert (target / "nested" / "data.txt").read_text(encoding="utf-8") == "Version one"
    assert not (target / ".git").exists()
    with pytest.raises(FileExistsError):
        store.restore(snapshot, target)


def test_a_snapshot_reads_again_only_files_that_changed(tmp_path, monkeypatch):
    from formal_geyser.coordination import storage

    reads = []

    def counted(path, *args, **kwargs):
        reads.append(Path(path).name)
        return open(path, *args, **kwargs)
    monkeypatch.setattr(storage, "open", counted, raising=False)
    store = BlobStore(tmp_path / "objects")
    source = tmp_path / "source"
    (source / "nested").mkdir(parents=True)
    for name in ("a.lean", "b.lean", "nested/c.md"):
        (source / name).write_text("content of " + name, encoding="utf-8")
    # Files changed within the settling time are always read: their metadata may not yet
    # identify their content.
    first = store.snapshot(source)
    assert store.snapshot(source) == first and len(reads) == 6
    monkeypatch.setattr(storage, "SNAPSHOT_SETTLED_NANOSECONDS", 0)
    reads.clear()
    assert store.snapshot(source) == first and len(reads) == 3  # Remembered once settled.
    reads.clear()
    assert store.snapshot(source) == first and reads == []
    time.sleep(0.05)  # A later write gets a later change time, even at the same size.
    (source / "b.lean").write_text("CONTENT OF b.lean", encoding="utf-8")
    (source / "nested" / "d.md").write_text("new", encoding="utf-8")
    second = store.snapshot(source)
    assert sorted(reads) == ["b.lean", "d.md"]
    target = tmp_path / "restored"
    store.restore(second, target)
    assert (target / "b.lean").read_text(encoding="utf-8") == "CONTENT OF b.lean"
    assert (target / "nested" / "d.md").read_text(encoding="utf-8") == "new"
    (source / "a.lean").unlink()
    assert [item["path"] for item in store.get_json(store.snapshot(source))["files"]] == [
        "b.lean", "nested/c.md", "nested/d.md"]


def test_an_exact_snapshot_reads_every_file(tmp_path, monkeypatch):
    from formal_geyser.coordination import storage

    store = BlobStore(tmp_path / "objects")
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.lean").write_text("first", encoding="utf-8")
    monkeypatch.setattr(storage, "SNAPSHOT_SETTLED_NANOSECONDS", 0)
    # Metadata that cannot tell the two versions apart: only an exact snapshot sees the change.
    monkeypatch.setattr(storage, "snapshot_identity", lambda info: (info.st_size,))
    first = store.snapshot(source)
    (source / "a.lean").write_text("again", encoding="utf-8")
    assert store.snapshot(source) == first
    exact = store.snapshot(source, exact=True)
    assert exact != first and store.snapshot(source) == exact  # What it read is remembered.


def test_remembered_files_are_bounded_over_all_workspaces_and_share_their_paths(tmp_path, monkeypatch):
    # On the pilot on 2026-11-09 each of up to 256 remembered workspaces held the Library's
    # 42,700 files, about 21 MB each, and TaskHub grew to 11.8 GiB.
    from formal_geyser.coordination import storage

    monkeypatch.setattr(storage, "SNAPSHOT_SETTLED_NANOSECONDS", 0)
    monkeypatch.setattr(storage, "SNAPSHOT_CACHED_FILES", 5)
    store = BlobStore(tmp_path / "objects")
    workspaces = []
    for name in ("one", "two", "three"):
        work = tmp_path / name
        work.mkdir()
        for file in ("a.lean", "b.lean"):
            (work / file).write_text(name + file, encoding="utf-8")
        store.snapshot(work)
        workspaces.append(work)
    # Six files would pass five: the least recently snapshotted workspace is forgotten.
    assert len(store.known_hashes(workspaces[0])) == 0
    two, three = store.known_hashes(workspaces[1]), store.known_hashes(workspaces[2])
    assert len(two) == len(three) == 2 and store._cached_files == 4
    identity, sha = three["a.lean"]
    assert identity == storage.snapshot_identity((workspaces[2] / "a.lean").stat())
    assert sha == store.put(b"threea.lean")
    assert next(name for name in store._file_hashes[str(workspaces[1].resolve())] if name == "a.lean") is next(
        name for name in store._file_hashes[str(workspaces[2].resolve())] if name == "a.lean")
    # A workspace whose files are about to go is not remembered, nor kept from before.
    store.snapshot(workspaces[1], exact=True, remember=False)
    assert len(store.known_hashes(workspaces[1])) == 0 and store._cached_files == 2
    store.forget(workspaces[2])
    assert len(store.known_hashes(workspaces[2])) == 0 and store._cached_files == 0


def test_a_snapshot_still_refuses_links(tmp_path):
    store = BlobStore(tmp_path / "objects")
    source = tmp_path / "source"
    (source / "real").mkdir(parents=True)
    (source / "real" / "file.txt").write_text("x", encoding="utf-8")
    (source / "linked").symlink_to(source / "real", target_is_directory=True)
    with pytest.raises(ValueError, match="Linked directories"):
        store.snapshot(source)
    (source / "linked").unlink()
    (source / "alias.txt").symlink_to(source / "real" / "file.txt")
    with pytest.raises(ValueError, match="escapes workspace"):
        store.snapshot(source)


@pytest.mark.parametrize("path", ["../escape", "/absolute", "a/../../escape"])
def test_restore_rejects_path_escape(tmp_path, path):
    store = BlobStore(tmp_path / "objects")
    body = store.put(b"outside")
    manifest = store.put_json({"files": [{"path": path, "sha256": body}]})
    with pytest.raises(ValueError):
        store.restore(manifest, tmp_path / "restored")


def test_canonical_json_refuses_nan_and_is_deterministic():
    assert canonical({"b": 1, "a": 2}) == canonical({"a": 2, "b": 1})
    with pytest.raises(ValueError):
        canonical({"bad": float("nan")})


async def test_transaction_rolls_back_without_partial_state(tmp_path):
    db = Database(tmp_path / "data.sqlite3", "CREATE TABLE IF NOT EXISTS values_(n INTEGER UNIQUE);")
    try:
        def invalid(c):
            c.execute("INSERT INTO values_ VALUES(1)")
            c.execute("INSERT INTO values_ VALUES(1)")
        with pytest.raises(sqlite3.IntegrityError):
            await db.call(invalid, write=True)
        assert await db.call(lambda c: c.execute("SELECT count(*) FROM values_").fetchone()[0]) == 0
    finally:
        await db.close()


async def test_actor_serializes_concurrent_transactions_and_backup(tmp_path):
    db = Database(tmp_path / "data.sqlite3", "CREATE TABLE IF NOT EXISTS values_(n INTEGER UNIQUE);")
    try:
        await asyncio.gather(*(
            db.call(lambda c, value=n: c.execute("INSERT INTO values_ VALUES(?)", (value,)), write=True)
            for n in range(50)))
        target = tmp_path / "backup.sqlite3"
        await db.backup(target)
        with sqlite3.connect(target) as c:
            assert c.execute("SELECT count(*) FROM values_").fetchone()[0] == 50
        with pytest.raises(FileExistsError):
            await db.backup(target)
    finally:
        await db.close()


def test_initializer_preserves_existing_config(tmp_path):
    initialize(tmp_path, ProjectConfig())
    config = (tmp_path / "project.toml").read_bytes()
    with pytest.raises(FileExistsError):
        initialize(tmp_path, ProjectConfig(name="Different"))
    assert (tmp_path / "project.toml").read_bytes() == config
    assert not (tmp_path / ".task-based" / "operator-token").exists()


def test_initializer_refuses_partial_existing_state(tmp_path):
    (tmp_path / ".task-based").mkdir()
    existing = tmp_path / ".task-based" / "existing.txt"
    existing.write_text("existing state", encoding="ascii")
    with pytest.raises(FileExistsError):
        initialize(tmp_path, ProjectConfig())
    assert existing.read_text(encoding="ascii") == "existing state"
    assert not (tmp_path / "project.toml").exists()


async def test_failed_startup_hook_is_retried_before_any_query(tmp_path):
    calls = []
    def recover(_connection):
        calls.append(1)
        if len(calls) == 1:
            raise sqlite3.OperationalError("Simulated recovery failure")
    db = Database(tmp_path / "data.sqlite3", "CREATE TABLE IF NOT EXISTS values_(n INTEGER);", on_open=recover)
    try:
        with pytest.raises(sqlite3.OperationalError):
            await db.call(lambda c: c.execute("SELECT 1").fetchone()[0])
        assert db.connection is None
        assert await db.call(lambda c: c.execute("SELECT 1").fetchone()[0]) == 1
        assert len(calls) == 2
    finally:
        await db.close()


async def test_close_does_not_initialize_an_unused_database(tmp_path):
    db = Database(tmp_path / "data.sqlite3", "Invalid migration must never run")
    await db.close()
    assert not db.path.exists()


async def test_concurrent_identical_blob_installs_are_readable(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    store = BlobStore(tmp_path / "objects")
    with ThreadPoolExecutor(max_workers=8) as pool:
        def operation(_):
            sha = store.put(b"same immutable object")
            assert store.get(sha) == b"same immutable object"
            return sha
        results = list(pool.map(operation, range(100)))
    assert len(set(results)) == 1


async def test_file_reads_cannot_observe_half_committed_publication(tmp_path):
    """WAL snapshots alone do not synchronize the authoritative native files."""
    db = Database(tmp_path / "ledger.sqlite3", "CREATE TABLE refs(body TEXT);")
    native = tmp_path / "record.txt"
    native.write_text("old", encoding="utf-8")
    await db.call(lambda c: c.execute("INSERT INTO refs VALUES('old')"), write=True)
    entered, release = threading.Event(), threading.Event()
    def publish(c):
        native.write_text("new", encoding="utf-8")
        entered.set()
        assert release.wait(15), "Release the synthetic publication"
        c.execute("UPDATE refs SET body='new'")
    def read(c):
        return c.execute("SELECT body FROM refs").fetchone()[0], native.read_text(encoding="utf-8")
    pending = asyncio.create_task(db.call(publish, write=True))
    reading = None
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        reading = asyncio.create_task(db.call(read))
        await asyncio.sleep(0)
        assert not reading.done()
    finally:
        release.set()
        await pending
        if reading is not None:
            assert await reading == ("new", "new")
        await db.close()
