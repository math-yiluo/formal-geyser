"""CPU-bound file bookkeeping in worker processes, off the Lean service's interpreter lock.

Each check reads, compares, syncs and hashes the whole Library several times. In the Linux
pilot on 2026-11-03 that Python work, done in the Lean service's own threads, held its one
interpreter at 120-130% CPU while the machine was 80% idle, and a check's 3.5 s Lake build
took 71 s. With `configure`, `call` runs such a function in a pool of worker processes;
without it, in a thread as before. Either way the result is the same function's result.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import multiprocessing
import threading
from concurrent.futures.process import BrokenProcessPool
from typing import Any, Callable

from .coordination.storage import collected_thread_call

logger = logging.getLogger(__name__)
_LOCK = threading.Lock()
_POOL: concurrent.futures.ProcessPoolExecutor | None = None
_PROCESSES = 0


def configure(processes: int) -> None:
    """Use `processes` worker processes from now on; 0 runs calls in threads."""
    global _POOL, _PROCESSES
    with _LOCK:
        previous, _POOL, _PROCESSES = _POOL, None, max(0, processes)
        if _PROCESSES:
            # A worker process starts from a clean interpreter, never as a fork of a service
            # that has threads running.
            _POOL = concurrent.futures.ProcessPoolExecutor(
                max_workers=_PROCESSES, mp_context=multiprocessing.get_context("forkserver"))
    if previous is not None:
        previous.shutdown(wait=True)


def shutdown() -> None:
    configure(0)


def _replace_broken(pool: concurrent.futures.ProcessPoolExecutor) -> None:
    global _POOL
    with _LOCK:
        if _POOL is not pool:
            return
        _POOL = concurrent.futures.ProcessPoolExecutor(
            max_workers=_PROCESSES, mp_context=multiprocessing.get_context("forkserver"))
    pool.shutdown(wait=False)


async def call(function: Callable[..., Any], *args: Any) -> Any:
    """`function(*args)` in a worker process, or in a thread when no pool is configured. Like
    collected_thread_call, a caller cancelled meanwhile waits for the call to end, so a
    filesystem mutation never outlives the lock that protects it. `function` and its arguments
    must be picklable: module-level functions and plain data."""
    pool = _POOL
    if pool is None:
        return await collected_thread_call(function, *args)
    loop = asyncio.get_running_loop()
    try:
        pending = asyncio.ensure_future(loop.run_in_executor(pool, function, *args))
    except (BrokenProcessPool, RuntimeError):
        _replace_broken(pool)
        return await collected_thread_call(function, *args)
    cancelled = False
    while not pending.done():
        try:
            await asyncio.shield(pending)
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            if not cancelled:
                break
    if cancelled:
        if not pending.cancelled():
            pending.exception()
        raise asyncio.CancelledError
    error = pending.exception()
    if isinstance(error, BrokenProcessPool):
        # A worker died (its memory was reclaimed, say): the call is made again in a thread,
        # and later calls use a new pool. Every offloaded function may be run again.
        logger.warning("A bookkeeping worker process ended; %s runs in a thread", getattr(function, "__name__", function))
        _replace_broken(pool)
        return await collected_thread_call(function, *args)
    return pending.result()
