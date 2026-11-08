"""Reclaiming retired reviewers' workspaces, without models."""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.run_driver import RunDriver
from formal_geyser.coordination.workspace_retention import (
    UNREGISTERED_SECONDS,
    reclaim_workspace,
)
from tests.coordination.publication_helpers import file_payload
from tests.coordination.test_collaboration_boundaries import (  # noqa: F401  (the hub fixture)
    claim,
    command,
    hub,
    machine_accept,
    peer_accept,
    publish,
    start,
)


def files(root: Path, *paths: str) -> None:
    for path in paths:
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_text(path, encoding="utf-8")


def test_a_retired_workspace_keeps_its_private_notes_and_registered_literature(tmp_path):
    work = tmp_path / "work"
    files(work, "Langlands/Basic.lean", "notes/public.md", "README.md", ".lake/build/x.olean", ".git/HEAD",
          ".private/notes/journal.md", ".private/notes/records/r.json", ".private/material/m.json",
          ".private/downloads/paper.pdf", ".private/uv-cache/CACHEDIR.TAG", ".private/uv-cache/archive/a.so",
          ".private/pdf-tools/pymupdf-1.26.dist-info/METADATA", ".private/pdf-tools/pymupdf/libmupdf.so",
          ".private/venv/pyvenv.cfg", ".private/venv/lib/site.py", ".private/tmp/pdf-pages/k-p1.png",
          ".private/ocr/rapidocr-1.4.dist-info/METADATA", ".private/ocr/source.pdf")
    kept_source = work / ".private/ocr/source.pdf"  # Registered literature inside a tool directory.
    assert reclaim_workspace(work, [kept_source]) > 0
    remaining = sorted(str(path.relative_to(work)).replace("\\", "/") for path in work.rglob("*") if path.is_file())
    assert remaining == [".git/HEAD", ".private/downloads/paper.pdf", ".private/material/m.json",
                         ".private/notes/journal.md", ".private/notes/records/r.json",
                         ".private/ocr/rapidocr-1.4.dist-info/METADATA", ".private/ocr/source.pdf"]


async def test_only_retired_workspaces_exactly_as_last_snapshotted_are_reclaimed_once(hub):  # noqa: F811
    host = await publish(hub, "Producer")
    run = await start(hub)
    author = await claim(hub, host, run)
    candidate = await command(hub, "artifact.stage", file_payload(hub, author, kind="record", data={"part": "One"}),
                              author)
    await machine_accept(hub, candidate)
    reviewer = await peer_accept(hub, candidate, run)  # Accepted, published, and so retired.
    worker = await hub.worker(reviewer.worker_id)
    assert (worker["state"], worker["work_intention"]) == ("retired", "retired")
    work = hub.workspace(reviewer.worker_id)
    snapshotted = [path.relative_to(work) for path in work.iterdir() if path.name not in {".private", ".git"}]
    for actor in (reviewer, author):
        # `.private` is outside the snapshot: these change nothing it holds.
        files(hub.workspace(actor.worker_id), ".private/notes/journal.md", ".private/uv-cache/CACHEDIR.TAG")
    # A file written after the last snapshot exists nowhere else: the workspace stays.
    files(work, "Langlands/Late.lean")
    assert await hub.reclaim_retired_workspaces() == []
    assert (work / "Langlands/Late.lean").is_file() and (work / ".private/uv-cache").is_dir()
    (work / "Langlands/Late.lean").unlink()
    (work / "Langlands").rmdir()
    [(worker_id, freed)] = await hub.reclaim_retired_workspaces()
    assert worker_id == reviewer.worker_id and freed > 0
    assert not any((work / path).exists() for path in snapshotted) and not (work / ".private/uv-cache").exists()
    assert (work / ".private/notes/journal.md").is_file()
    assert (hub.workspace(author.worker_id) / ".private/uv-cache").is_dir()  # A working author keeps all.
    assert await hub.reclaim_retired_workspaces() == []  # Recorded: never walked again.


async def test_a_worker_idle_for_48_hours_with_nothing_of_its_own_retires(hub):  # noqa: F811
    now = [hub.clock()]
    hub.clock = lambda: now[0]
    host = await publish(hub, "Producer")
    run = await start(hub)
    author = await claim(hub, host, run)  # Holds a task, however long it waits.
    idle = await command(hub, "worker.register")
    now[0] += 24 * 3_600_000
    recent = await command(hub, "worker.register")
    now[0] += 24 * 3_600_000 + 1
    assert await hub.retire_idle_workers() == [idle["id"]]
    states = {w: (await hub.worker(w))["state"] for w in (idle["id"], recent["id"], author.worker_id)}
    assert states == {idle["id"]: "retired", recent["id"]: "idle", author.worker_id: states[author.worker_id]}
    assert states[author.worker_id] != "retired"
    assert await hub.retire_idle_workers() == []
    now[0] += 24 * 3_600_000
    assert await hub.retire_idle_workers() == [recent["id"]]


async def registered_ids(hub) -> set[str]:  # noqa: F811
    return await hub.db.read(lambda c: {row[0] for row in c.execute("SELECT id FROM workers")})


async def test_claims_that_fail_again_and_again_leave_no_workspaces_behind(hub, monkeypatch):  # noqa: F811
    run = await start(hub)
    await publish(hub, "Contested task")

    def taken(*args, **kwargs):
        raise HubError("ALREADY_CLAIMED", "Another worker took it first")
    monkeypatch.setattr(hub, "_claim_opportunity", taken)
    for _ in range(100):
        assert await hub.next_assignment(run["id"]) is None
    assert {path.name for path in (hub.state_dir / "workers").iterdir()} <= await registered_ids(hub)


async def test_empty_workspaces_of_rolled_back_registrations_are_removed_once_an_hour_old(hub):  # noqa: F811
    worker = await command(hub, "worker.register")
    hour_ago = time.time() - UNREGISTERED_SECONDS - 60

    def workspace(name, *paths, modified=hour_ago):
        work = hub.state_dir / "workers" / name / "work"
        work.mkdir(parents=True, exist_ok=True)
        files(work, *paths)
        for path in (work, work.parent):
            os.utime(path, (modified, modified))
        return work.parent
    abandoned = workspace("abandoned")
    recent = workspace("recent", modified=time.time())  # Its registration may still commit.
    holding = workspace("holding", "notes/kept.md")
    own = workspace(worker["id"])
    assert await hub.reclaim_retired_workspaces() == []
    assert not abandoned.exists()
    assert recent.is_dir() and (holding / "work/notes/kept.md").is_file() and (own / "work").is_dir()
    assert worker["id"] in await registered_ids(hub)


async def test_the_driver_reclaims_in_the_background_one_pass_at_a_time():
    release, passes = asyncio.Event(), []

    class Hub:
        async def reclaim_retired_workspaces(self):
            passes.append(1)
            await release.wait()
            return [("reviewer", 4096)]

    driver = RunDriver(Hub(), make_runner=lambda *args: None, resume_runs=False)
    driver._reclaim_retired()
    assert driver.reclaim_task is None  # Not due in the first ten minutes.
    driver.reclaim_at = 0
    driver._reclaim_retired()
    first = driver.reclaim_task
    await asyncio.sleep(0)
    driver.reclaim_at = 0
    driver._reclaim_retired()  # A pass still runs: no second one.
    assert driver.reclaim_task is first and passes == [1]
    release.set()
    await first
    assert driver.stop() == [first]


async def test_the_driver_reclaims_integration_checkouts_as_soon_as_a_batch_lets_one_go():
    release, passes = asyncio.Event(), []

    class Hub:
        due = False

        def integration_workspaces_due(self):
            return self.due

        async def reclaim_integration_workspaces(self):
            passes.append(1)
            self.due = False
            await release.wait()
            return {"workspaces": [{"workspace": "w", "state": "reclaimed_unfrozen", "bytes": 1}],
                    "reclaimed_bytes": 1, "remaining": 0}

    fake = Hub()
    driver = RunDriver(fake, make_runner=lambda *args: None, resume_runs=False)
    driver._reclaim_integration()
    assert driver.integration_reclaim_task is None  # Nothing let go, and not ten minutes yet.
    fake.due = True
    driver._reclaim_integration()
    first = driver.integration_reclaim_task
    await asyncio.sleep(0)
    fake.due = True
    driver._reclaim_integration()  # A pass still runs: no second one.
    assert driver.integration_reclaim_task is first and passes == [1]
    release.set()
    await first
    assert first in driver.stop()
