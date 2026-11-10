"""glibc's memory allocator in TaskHub, a long-running process with many threads.

glibc gives each thread that allocates at once an arena of its own (up to eight per core) and
keeps what is freed in it for reuse. On the pilot on 2026-11-09 TaskHub's twenty-odd worker
threads held about 90 arenas, 4.3 GiB in all, most of it freed memory from reading whole Library
trees, which the process never gave back. TaskHub limits the arenas at its start, before its
threads exist, and gives freed memory back every few minutes. Elsewhere both do nothing.
"""
from __future__ import annotations

import ctypes
import sys
from pathlib import Path

M_ARENA_MAX = -8  # mallopt's parameter for the most arenas (malloc.h).
ARENAS = 2  # The interpreter lock already lets one thread run Python at a time.


def _libc() -> ctypes.CDLL | None:
    if not sys.platform.startswith("linux"):
        return None
    try:
        return ctypes.CDLL(None)
    except OSError:
        return None


def limit_arenas(count: int = ARENAS) -> bool:
    """Allow at most `count` arenas from now on; whether the allocator took it."""
    libc = _libc()
    function = getattr(libc, "mallopt", None) if libc is not None else None
    return bool(function is not None and function(M_ARENA_MAX, count))


def trim() -> bool:
    """Give the system back the freed memory the allocator holds; whether it gave any."""
    libc = _libc()
    function = getattr(libc, "malloc_trim", None) if libc is not None else None
    return bool(function is not None and function(0))


def private_memory() -> int | None:
    """This process's private memory, resident or swapped out, in bytes."""
    try:
        fields = dict(line.split(":", 1) for line in Path("/proc/self/status").read_text().splitlines() if ":" in line)
        return sum(int(fields[name].split()[0]) for name in ("RssAnon", "VmSwap")) * 1024
    except (OSError, KeyError, ValueError, IndexError):
        return None
