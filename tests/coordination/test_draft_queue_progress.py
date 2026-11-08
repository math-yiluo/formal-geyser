"""Slow shared draft I/O must not starve coordination or leave background writes."""
from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path

import pytest

from formal_geyser.coordination.drafts import DraftFiles
from formal_geyser.coordination.errors import HubError
from formal_geyser.coordination.models import Actor, Command, Role

from .test_gateway_runtime import call
from .test_gateway_runtime import project as project
from .test_public_drafts import draft_arguments


@pytest.mark.parametrize("phase", ["install", "verify"])
async def test_draft_io_does_not_block_task_reads_or_worker_authority(project, monkeypatch, phase):
    hub, task, attempt, runner = project
    entered, release = threading.Event(), threading.Event()
    original = getattr(DraftFiles, phase)

    def blocked(self, *args, **kwargs):
        entered.set()
        assert release.wait(10), "Test did not release draft I/O"
        return original(self, *args, **kwargs)

    monkeypatch.setattr(DraftFiles, phase, blocked)
    pending = asyncio.create_task(call(runner, attempt, "draft_update", draft_arguments(hub, task, attempt)))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        async with asyncio.timeout(2):
            assert (await hub.task(task["id"]))["revision"] == 1
            await hub.authorize_worker(Actor(role=Role.WORKER, worker_id=attempt["worker_id"],
                                             attempt_id=attempt["id"], fencing_token=attempt["fencing_token"]))
        assert not pending.done()
    finally:
        release.set()
        await pending


@pytest.mark.parametrize("action", ["pause", "cancel"])
async def test_draft_preparation_rechecks_control_and_collects_cancelled_io(project, monkeypatch, action):
    hub, task, attempt, runner = project
    entered, release, stopped = threading.Event(), threading.Event(), threading.Event()
    original = DraftFiles.install

    def blocked(self, *args, **kwargs):
        entered.set()
        assert release.wait(10)
        try:
            return original(self, *args, **kwargs)
        finally:
            stopped.set()

    monkeypatch.setattr(DraftFiles, "install", blocked)
    pending = asyncio.create_task(call(runner, attempt, "draft_update", draft_arguments(hub, task, attempt)))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        if action == "pause":
            await hub.execute(Command(name="run.pause", key="operator-pause", payload={"run_id": attempt["run_id"]}))
        else:
            pending.cancel()
            await asyncio.sleep(0.02)
            assert not pending.done()
        release.set()
        with pytest.raises(HubError if action == "pause" else asyncio.CancelledError):
            await pending
        assert stopped.is_set()
        assert "drafts" not in await hub.task(task["id"])
    finally:
        release.set()
        if not pending.done():
            await asyncio.gather(pending, return_exceptions=True)


async def test_old_verification_cannot_rewind_a_new_draft_head(project, monkeypatch):
    hub, task, attempt, runner = project
    args = draft_arguments(hub, task, attempt)
    first = await call(runner, attempt, "draft_update", args, "first")
    entered, release = threading.Event(), threading.Event()
    original = DraftFiles.verify

    def blocked(self, reference):
        if reference["revision"] == 1:
            entered.set()
            assert release.wait(10)
        return original(self, reference)

    monkeypatch.setattr(DraftFiles, "verify", blocked)
    old = asyncio.create_task(hub.reconcile_drafts(task["id"]))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        second = await call(runner, attempt, "draft_update", {**args, "expected_revision": 1,
            "summary": "A newer revision while old navigation is being verified."}, "second")
        assert second["revision"] == 2
    finally:
        release.set()
        await old
    # The worker reads its task as Task 0; the file keeps the id.
    assert second["task_id"] == "Task 0"
    assert json.loads(Path(first["current_path"]).read_bytes()) == {**second, "task_id": task["id"]}
