"""The Lean service's bookkeeping processes (offload)."""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pytest

from formal_geyser import offload
from formal_geyser.registry import RegistryError


def pid() -> int:
    return os.getpid()


def refuse(message: str) -> None:
    raise RegistryError(message)


def slow_write(path: str) -> None:
    time.sleep(0.5)
    Path(path).write_text("done")


@pytest.fixture
def pool():
    offload.configure(2)
    yield
    offload.shutdown()


async def test_a_call_runs_in_a_worker_process_with_the_same_result_and_error(pool):
    assert await offload.call(pid) != os.getpid()
    with pytest.raises(RegistryError, match="Missing source module: M"):
        await offload.call(refuse, "Missing source module: M")


async def test_a_cancelled_caller_waits_for_the_call_to_end(pool, tmp_path):
    target = tmp_path / "written"
    task = asyncio.create_task(offload.call(slow_write, str(target)))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert target.read_text() == "done"  # The filesystem change ended before the caller did.


async def test_without_a_pool_a_call_runs_in_a_thread():
    offload.shutdown()
    assert await offload.call(pid) == os.getpid()
