"""A task carried from an instance where it had a worker keeps its number for its first executor:
Task X's is Worker X. Other workers take the numbers no task keeps."""
from __future__ import annotations

import json
import sqlite3

import pytest

from formal_geyser.coordination.core_transfer import (
    export_core,
    import_core,
    keep_carried_worker_numbers,
)
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.worker_names import kept_worker_number, next_worker_number
from tests.coordination.publication_helpers import _command, new_hub
from tests.coordination.test_core_transfer import LIBRARY, publish


@pytest.fixture
async def bundle(tmp_path):
    """An old instance whose root (Task 0) had a worker and whose second task (Task 1) never had one."""
    source = await new_hub(tmp_path / "old", files=LIBRARY)
    try:
        root = await publish(source, "Program root")
        other = await publish(source, "Never started")
        run = await _command(source, "run.start")
        worker = await _command(source, "worker.register")
        await _command(source, "task.join", {"task_id": root["id"], "worker_id": worker["id"], "run_id": run["id"],
                                              "revision": 1})
        await source.public_content()
        export_core(tmp_path / "old", tmp_path / "bundle")
    finally:
        await source.close()
    return tmp_path / "bundle", root["id"], other["id"]


async def numbers_by_task(hub):
    return await hub.db.call(lambda c: {row[0]: row[1] for row in c.execute(
        "SELECT m.task_id,w.worker_number FROM task_members m JOIN workers w ON w.id=m.worker_id WHERE m.executor=1")})


async def test_the_export_says_which_tasks_had_a_worker(bundle):
    path, root, other = bundle
    entries = {entry["id"]: entry for entry in json.loads((path / "tasks.json").read_text(encoding="utf-8"))}
    assert (entries[root]["had_worker"], entries[other]["had_worker"]) == (True, False)


async def test_task_x_keeps_worker_x_for_its_first_executor(bundle, tmp_path):
    path, root, other = bundle
    target = await new_hub(tmp_path / "new", files=LIBRARY)
    try:
        summary = await import_core(target, path)
        assert summary["kept_worker_numbers"] == 1
        assert await target.db.call(lambda c: (kept_worker_number(c, root), kept_worker_number(c, other),
                                               next_worker_number(c))) == (0, None, 1)
        run = await _command(target, "run.start")
        # The never-started task's worker comes first here, yet takes 1: 0 stays Task 0's.
        await _command(target, "task.set_pinned", {"task_id": other, "pinned": True})
        first = await target.next_assignment(run["id"])
        second = await target.next_assignment(run["id"])
        assert {first["task_id"], second["task_id"]} == {root, other}
        assert await numbers_by_task(target) == {root: 0, other: 1}
        assert await target.db.call(lambda c: c.execute(
            "SELECT worker_id FROM kept_worker_numbers WHERE task_id=?", (root,)).fetchone()[0]) == (
            first if first["task_id"] == root else second)["worker_id"]
    finally:
        await target.close()


async def test_no_existing_worker_is_a_kept_tasks_first_executor(bundle, tmp_path):
    path, root, _ = bundle
    target = await new_hub(tmp_path / "new", files=LIBRARY)
    try:
        await import_core(target, path)
        run = await _command(target, "run.start")
        idle = await _command(target, "worker.register")
        assert idle["worker_number"] == 1
        with pytest.raises(HubError) as refused:
            await _command(target, "task.join", {"task_id": root, "worker_id": idle["id"], "run_id": run["id"],
                                             "revision": 1})
        assert refused.value.code == "WORKER_NUMBER_KEPT" and "Worker 0" in refused.value.message
        # Only the runner gives a kept number, and only one not yet taken.
        with pytest.raises(HubError):
            await _command(target, "worker.register", {"worker_number": 0})
    finally:
        await target.close()


async def test_an_instance_seeded_before_is_renumbered(bundle, tmp_path):
    path, root, other = bundle
    target = await new_hub(tmp_path / "new", files=LIBRARY)
    try:
        await import_core(target, path)
        # As an import before this change left it: no number kept, Worker 0 on the other task.
        await target.db.call(lambda c: c.execute("DELETE FROM kept_worker_numbers"), write=True)
        run = await _command(target, "run.start")
        for task_id in (other, root):
            worker = await _command(target, "worker.register")
            await _command(target, "task.join", {"task_id": task_id, "worker_id": worker["id"], "run_id": run["id"],
                                              "revision": 1})
        assert await numbers_by_task(target) == {other: 0, root: 1}
        result = await keep_carried_worker_numbers(target, path)
        assert (result["kept"], result["taken"]) == (1, 1)
        assert {(change["from"], change["to"]) for change in result["renumbered"]} == {(1, 0), (0, 2)}
        assert await numbers_by_task(target) == {root: 0, other: 2}
        events = await target.db.call(lambda c: c.execute(
            "SELECT count(*) FROM events WHERE kind='worker.renumbered'").fetchone()[0])
        assert events == 2
        assert (await _command(target, "worker.register"))["worker_number"] == 3
        # Numbers are immutable again.
        with pytest.raises(sqlite3.IntegrityError):
            await target.db.call(lambda c: c.execute("UPDATE workers SET worker_number=99 WHERE worker_number=0"),
                                 write=True)
        # Run twice, it changes nothing more.
        again = await keep_carried_worker_numbers(target, path)
        assert again["renumbered"] == []
    finally:
        await target.close()


async def test_a_bundle_that_does_not_say_who_had_a_worker_is_refused(bundle, tmp_path):
    path, _, _ = bundle
    entries = json.loads((path / "tasks.json").read_text(encoding="utf-8"))
    for entry in entries:
        del entry["had_worker"]
    (path / "tasks.json").write_text(json.dumps(entries), encoding="utf-8")
    target = await new_hub(tmp_path / "new", files=LIBRARY)
    try:
        assert (await import_core(target, path))["kept_worker_numbers"] == 0
        with pytest.raises(HubError) as refused:
            await keep_carried_worker_numbers(target, path)
        assert refused.value.code == "CORE_FORMAT"
    finally:
        await target.close()
