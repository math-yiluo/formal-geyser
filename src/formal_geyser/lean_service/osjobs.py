"""The operating-system processes of one Lean service job: suspended, resumed, ended and
measured together.

A job's processes run below one cgroup (cgroups.py), each process tree in a cgroup of its own:
freezing the job's cgroup stops every process in it, including one that starts later; its
accounting keeps what ended processes used; and the service's unit ends whatever still runs
when the service itself ends (server.py).
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from uuid import uuid4

from .. import cgroups


@dataclass(frozen=True)
class Usage:
    cpu_seconds: float
    private_memory: int  # Now: physical memory private to the job's processes.
    committed_memory: int  # Now: private memory, resident or swapped out.
    peak_committed_memory: int
    processes: int


def process_memory(pid: int) -> int:
    """A process's private memory, resident or swapped out, in bytes; 0 once it has ended."""
    try:
        with open(f"/proc/{pid}/status", encoding="ascii", errors="replace") as status:
            fields = dict(line.split(":", 1) for line in status if ":" in line)
    except OSError:
        return 0
    return sum(int(fields.get(name, "0 kB").split()[0]) for name in ("RssAnon", "VmSwap")) * 1024


class JobGroup:
    """All processes of one service job.

    Its processes are the first the kernel ends when memory runs out (`oom_score_adj`): on
    the pilot on 2026-11-09 the machine's memory ran out while every process had the same score,
    and the kernel ended TaskHub, the largest, and with it every worker. `memory_max` and
    `swap_max` bound what the job's processes may hold together (cgroups.limit_memory)."""

    oom_score_adj = 1000

    def __init__(self, *, memory_max: int | None = None, swap_max: int | None = None) -> None:
        self.path = cgroups.root() / ("job-" + uuid4().hex)
        self.path.mkdir()
        try:
            cgroups.limit_memory(self.path, memory_max, swap_max)
        except BaseException:
            cgroups.remove(self.path)
            raise
        self.suspended = False
        self._peak_private = 0

    @property
    def peak_private_memory(self) -> int:
        return self._peak_private

    def pids(self) -> list[int]:
        return cgroups.processes(self.path)

    def suspend(self) -> int:
        """Stop every process, including any that starts later; returns how many there are."""
        self.suspended = True
        cgroups.freeze(self.path, True)
        return len(self.pids())

    def resume(self) -> int:
        self.suspended = False
        cgroups.freeze(self.path, False)
        return len(self.pids())

    def largest_process(self) -> int:
        """The private memory, resident or swapped out, of the group's largest process."""
        return max((process_memory(pid) for pid in self.pids()), default=0)

    def oom_kills(self) -> int:
        """How many of the group's processes the kernel has ended for want of memory."""
        return cgroups.oom_kills(self.path)

    def usage(self) -> Usage:
        cpu, private, swapped, peak = cgroups.usage(self.path)
        self._peak_private = max(self._peak_private, private)
        return Usage(cpu, private, private + swapped, peak, len(self.pids()))

    def terminate(self) -> None:
        cgroups.kill(self.path)

    def close(self) -> None:
        """End anything left and remove the job's cgroup; each process's owner removes its own."""
        if cgroups.populated(self.path):
            self.terminate()
            deadline = time.monotonic() + 10
            while cgroups.populated(self.path) and time.monotonic() < deadline:
                time.sleep(0.02)
        cgroups.remove(self.path)
