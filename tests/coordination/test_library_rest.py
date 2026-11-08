"""Library copies of workers at rest (library_rest), without models or Lean."""
from __future__ import annotations

import os
import stat
from pathlib import Path
from uuid import uuid4

import pytest

from formal_geyser.coordination.library_rest import (
    RESTORING,
    at_rest,
    read_record,
    record_path,
    rest_workspace,
    rested_snapshot,
    restore_workspace,
)
from formal_geyser.coordination.models import Actor, Command, Role
from formal_geyser.coordination.storage import BlobStore
from formal_geyser.coordination.workspace_retention import IDLE_RETIREMENT_MS, REST_AFTER_MS
from formal_geyser.maintenance import restore_all_library_copies
from tests.coordination.lifecycle_helpers import idle_worker
from tests.coordination.publication_helpers import new_hub
from tests.coordination.test_publication_repairs import FixtureRepository, seed_files

OP = Actor(role=Role.OPERATOR)
RUNNER = Actor(role=Role.RUNNER)
LIBRARY = {"Library/Basic.lean": b"def one := 1\n", "Library/Basic.md": b"# Basic\n",
           "Library/Deep/Nested/Lemma.lean": b"theorem t : True := trivial\n", "wiki/README.md": b"# Library\n"}


def write(root: Path, files: dict[str, bytes]) -> None:
    for path, body in files.items():
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_bytes(body)


def tree(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def base_snapshot(blobs: BlobStore, files: dict[str, bytes]) -> str:
    return blobs.put_json({"files": [{"path": path, "sha256": blobs.put(body)} for path, body in sorted(files.items())]})


@pytest.fixture
def checkout(tmp_path):
    """A worker's checkout of a base, with its own work: a change, a deletion and new files."""
    blobs = BlobStore(tmp_path / "blobs")
    base = {"a.lean": b"a", "dir/b.lean": b"b", "dir/sub/c.md": b"c", "changed.lean": b"base",
            "deleted.lean": b"gone", "tool.sh": b"#!/bin/sh\n", "readonly.md": b"r", "linked.md": b"l",
            "literature/paper.md": b"p"}
    work = tmp_path / "workers" / "w1" / "work"
    write(work, base)
    (work / "changed.lean").write_bytes(b"worker's change")
    (work / "deleted.lean").unlink()
    write(work, {"own/new.lean": b"new", ".private/notes/journal.md": b"notes", ".lake/build/x.olean": b"o"})
    (work / "own/empty").mkdir()
    (work / "tool.sh").chmod(0o755)
    (work / "readonly.md").chmod(stat.S_IREAD)
    os.link(work / "linked.md", tmp_path / "elsewhere.md")
    return blobs, work.resolve(), base_snapshot(blobs, base)


def test_resting_keeps_only_what_differs_from_the_base_and_a_restore_brings_back_the_same_tree(checkout):
    blobs, work, base = checkout
    before, files_before = blobs.snapshot(work, exact=True), tree(work)
    removed, freed = rest_workspace(blobs, work, "1" * 40, base, [work / "literature/paper.md"], 7)
    assert removed == 3 and freed > 0 and at_rest(work)
    assert read_record(work) == {"schema_version": 1, "base_commit": "1" * 40, "base_snapshot": base,
                                 "manifest": before, "rested_at": 7}
    # Executable, read-only and linked files stay, as does registered literature.
    assert sorted(tree(work)) == [".lake/build/x.olean", ".private/notes/journal.md", "changed.lean", "linked.md",
                                  "literature/paper.md", "own/new.lean", "readonly.md", "tool.sh"]
    assert not (work / "dir").exists() and (work / "own/empty").is_dir()  # Only directories it emptied go.
    assert rested_snapshot(blobs, work, read_record(work)) == before
    assert rest_workspace(blobs, work, "1" * 40, base, [], 8) == (0, 0)  # Already at rest.
    assert restore_workspace(blobs, work) == 3
    assert not at_rest(work) and tree(work) == files_before and blobs.snapshot(work, exact=True) == before
    assert restore_workspace(blobs, work) is None


def test_a_file_changed_after_it_was_read_is_not_removed(checkout, monkeypatch):
    blobs, work, base = checkout
    snapshot = blobs.snapshot

    def read_then_changed(directory, **options):
        result = snapshot(directory, **options)
        (work / "a.lean").write_bytes(b"changed")
        return result
    monkeypatch.setattr(blobs, "snapshot", read_then_changed)
    assert rest_workspace(blobs, work, "1" * 40, base, [work / "literature/paper.md"], 1)[0] == 2
    monkeypatch.undo()
    assert (work / "a.lean").read_bytes() == b"changed"
    restore_workspace(blobs, work)
    assert (work / "a.lean").read_bytes() == b"changed"  # Never overwritten.


def test_a_restore_cut_short_finishes_and_a_rest_with_nothing_to_remove_leaves_no_record(checkout):
    blobs, work, base = checkout
    before = blobs.snapshot(work, exact=True)
    rest_workspace(blobs, work, "1" * 40, base, [work / "literature/paper.md"], 1)
    # A crash after one file was written and while the next was being written.
    (work / "a.lean").write_bytes(b"a")
    (work / "dir/sub").mkdir(parents=True)
    (work / "dir/sub" / RESTORING).write_bytes(b"partial")
    assert restore_workspace(blobs, work) == 2
    assert not (work / "dir/sub" / RESTORING).exists() and blobs.snapshot(work, exact=True) == before
    unrelated = base_snapshot(blobs, {"other.lean": b"o"})
    assert rest_workspace(blobs, work, "2" * 40, unrelated, [], 2) == (0, 0) and not at_rest(work)


@pytest.mark.parametrize("rested", [False, True])
def test_a_refresh_after_a_rest_is_the_refresh_without_one(tmp_path, rested):
    store = FixtureRepository(tmp_path)
    try:
        before = {"updated.md": b"base", "private.md": b"base", "conflict.md": b"base", "deleted.md": b"base",
                  "kept.md": b"base"}
        store.snapshots[store.head] = store.snapshot(before)
        seed_files(store, store.connection, "worker")
        root = store.workspace("worker").resolve()
        (root / "private.md").write_bytes(b"private edit")
        (root / "conflict.md").write_bytes(b"local edit")
        if rested:
            assert rest_workspace(store.blobs, root, store.head, store.snapshots[store.head], [], 1)[0] == 3
            assert not (root / "updated.md").exists()
        store.head = "2" * 40
        store.snapshots[store.head] = store.snapshot({"updated.md": b"published", "private.md": b"base",
                                                      "conflict.md": b"incoming", "new.md": b"new", "kept.md": b"base"})
        report = seed_files(store, store.connection, "worker")
        assert [item["path"] for item in report["conflicts"]] == ["conflict.md"]
        assert {path: body for path, body in tree(root).items() if not path.startswith(".private/")} == {
            "updated.md": b"published", "private.md": b"private edit", "conflict.md": b"local edit",
            "new.md": b"new", "kept.md": b"base"}
        assert not at_rest(root)
    finally:
        store.connection.close()


async def command(hub, name, payload=None, actor=OP):
    return await hub.execute(Command(name=name, key=uuid4().hex, payload=payload or {}), actor)


async def rested_worker(hub):
    """An idle worker with a Library checkout and work of its own, its copy rested."""
    run = await command(hub, "run.start")
    worker = await idle_worker(hub, run)
    work = hub.workspace(worker["id"]).resolve()
    assert {path for path in LIBRARY if (work / path).is_file()} == set(LIBRARY)
    write(work, {"Mine/Draft.lean": b"-- mine\n", ".private/notes/journal.md": b"notes"})
    (work / "Library/Basic.md").write_bytes(b"# Basic, revised\n")
    before = hub.blobs.snapshot(work, exact=True)
    now = [hub.clock() + REST_AFTER_MS + 1]
    hub.clock = lambda: now[0]
    [(worker_id, files, freed)] = await hub.rest_library_copies()
    assert (worker_id, files) == (worker["id"], 3) and freed > 0
    assert not any((work / path).exists() for path in LIBRARY if path != "Library/Basic.md")
    assert all((work / path).is_file() for path in ("Library/Basic.md", "Mine/Draft.lean", ".private/notes/journal.md"))
    assert await hub.rest_library_copies() == []  # At rest already.
    return run, worker, work, before, now


async def test_a_worker_at_rest_has_its_library_back_before_any_snapshot(tmp_path):
    hub = await new_hub(tmp_path / "project", files=LIBRARY)
    try:
        _, worker, work, before, _ = await rested_worker(hub)
        prepared = await hub.prepare_workspace_snapshots([worker["id"]])
        assert prepared[worker["id"]]["snapshot"] == before and not at_rest(work)
        assert (work / "Library/Basic.md").read_bytes() == b"# Basic, revised\n"
    finally:
        await hub.close()


async def test_a_worker_at_rest_has_its_library_back_when_admitted_and_when_it_checks(tmp_path):
    hub = await new_hub(tmp_path / "project", files=LIBRARY)
    try:
        run, worker, work, before, _ = await rested_worker(hub)
        task = await command(hub, "task.publish", {"contract": {"title": "Next task", "objective": "Work on it."}})
        attempt = await command(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"],
                                                    "run_id": run["id"], "worker_id": worker["id"]}, RUNNER)
        assert not at_rest(work) and hub.blobs.snapshot(work, exact=True) == before
        assert await hub.db.read(lambda c: c.execute("SELECT 1 FROM attempt_workspaces WHERE attempt_id=?",
                                                     (attempt["id"],)).fetchone())
        assert await hub.rest_library_copies() == []  # It has an attempt.
        assert await hub.ensure_library_copy(worker["id"]) is None
    finally:
        await hub.close()
    hub = await new_hub(tmp_path / "second", files=LIBRARY)
    try:
        _, worker, work, before, _ = await rested_worker(hub)
        assert await hub.ensure_library_copy(worker["id"]) is not None
        assert not at_rest(work) and hub.blobs.snapshot(work, exact=True) == before
    finally:
        await hub.close()


async def test_only_workers_without_an_attempt_for_two_hours_rest(tmp_path):
    hub = await new_hub(tmp_path / "project", files=LIBRARY)
    try:
        run = await command(hub, "run.start")
        resting = await idle_worker(hub, run)
        now = [hub.clock() + REST_AFTER_MS + 1]
        hub.clock = lambda: now[0]
        recent = await idle_worker(hub, run, title="Recent task")
        task = await command(hub, "task.publish", {"contract": {"title": "Held", "objective": "Hold it."}})
        busy = await command(hub, "worker.register", {}, RUNNER)
        await command(hub, "task.claim", {"task_id": task["id"], "revision": task["revision"],
                                          "run_id": run["id"], "worker_id": busy["id"]}, RUNNER)
        await hub.db.write(lambda c: c.execute("UPDATE attempts SET created_at=0 WHERE worker_id=?", (busy["id"],)))
        assert [worker_id for worker_id, _, _ in await hub.rest_library_copies()] == [resting["id"]]
        assert not at_rest(hub.workspace(recent["id"]).resolve()) and not at_rest(hub.workspace(busy["id"]).resolve())
    finally:
        await hub.close()


async def test_a_workspace_with_nothing_to_remove_is_read_again_only_after_its_worker_ran(tmp_path):
    hub = await new_hub(tmp_path / "project", files=LIBRARY)
    try:
        run = await command(hub, "run.start")
        worker = await idle_worker(hub, run)
        work = hub.workspace(worker["id"]).resolve()
        for path in LIBRARY:
            (work / path).chmod(stat.S_IREAD)  # Copied with `cp` from the read-only Library.
        now = [hub.clock() + REST_AFTER_MS + 1]
        hub.clock = lambda: now[0]
        assert await hub.rest_library_copies() == [] and not at_rest(work)
        for path in LIBRARY:
            (work / path).chmod(stat.S_IREAD | stat.S_IWRITE)
        assert await hub.rest_library_copies() == []  # Not read again.
        await hub.db.write(lambda c: c.execute(
            "UPDATE attempts SET ended_at=ended_at+1 WHERE worker_id=?", (worker["id"],)))
        assert [worker_id for worker_id, _, _ in await hub.rest_library_copies()] == [worker["id"]]
    finally:
        await hub.close()


async def test_a_retired_worker_at_rest_is_reclaimed_against_its_last_snapshot(tmp_path):
    hub = await new_hub(tmp_path / "project", files=LIBRARY)
    try:
        run = await command(hub, "run.start")
        worker = await idle_worker(hub, run)
        work = hub.workspace(worker["id"]).resolve()
        now = [hub.clock() + REST_AFTER_MS + 1]
        hub.clock = lambda: now[0]
        assert await hub.rest_library_copies()
        now[0] += IDLE_RETIREMENT_MS
        [(worker_id, freed)] = await hub.reclaim_retired_workspaces()
        assert worker_id == worker["id"] and not record_path(work).exists()
        assert not any(path.suffix == ".lean" for path in work.rglob("*"))
    finally:
        await hub.close()


async def test_every_resting_copy_is_restored_for_a_rollback(tmp_path):
    hub = await new_hub(tmp_path / "project", files=LIBRARY)
    try:
        _, worker, work, before, _ = await rested_worker(hub)
        assert await restore_all_library_copies(hub) == {"workers": 1, "files": 3, "restored": {worker["id"]: 3}}
        assert not at_rest(work) and hub.blobs.snapshot(work, exact=True) == before
    finally:
        await hub.close()
