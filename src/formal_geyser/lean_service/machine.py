"""What this machine has free now: logical cores, physical and committed memory, CPU load."""
from __future__ import annotations

import os
from pathlib import Path

from .. import cgroups

# Unless configured, the Lean service and every process it starts may hold this share of the
# machine's memory together; the rest stays for TaskHub, its workers and the system. On the pilot
# (46 GiB) on 2026-11-09 Lean held 16-22 GiB, TaskHub 12 and the workers' processes 13 when the
# memory ran out.
SERVICE_MEMORY_SHARE = 0.6


def logical_cores() -> int:
    return os.cpu_count() or 1


def memory(cgroup: Path | None = None) -> tuple[int, int] | None:
    """Available physical and committed bytes, not installed RAM. With `cgroup`, whose memory.max
    bounds every process the caller starts (the Lean service's own scope), no more than that
    bound still leaves: past it the kernel reclaims and then ends the cgroup's own processes,
    while the machine may still have memory free."""
    try:
        fields = {key: int(value.strip().split()[0]) * 1024 for key, value in
                  (line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())}
        available, swap = fields["MemAvailable"], fields.get("SwapFree", 0)
    except (OSError, ValueError, KeyError):
        return None
    headroom = cgroups.memory_headroom(cgroup) if cgroup is not None else None
    if headroom is not None:
        available = min(available, headroom)
    return available, available + swap


def memory_limit(gib: float | None) -> int | None:
    """The Lean service's memory bound in bytes (LeanServiceConfig.service_memory_gib): that many
    GiB, or by default (None) SERVICE_MEMORY_SHARE of this machine's memory; 0: no bound."""
    if gib is not None:
        return int(gib * 2**30) if gib > 0 else None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return int(int(line.split()[1]) * 1024 * SERVICE_MEMORY_SHARE)
    except (OSError, ValueError, IndexError):
        pass
    return None


class CpuMeter:
    """The share of all cores that was busy since the previous reading."""

    def __init__(self) -> None:
        self._previous = self._times()

    @staticmethod
    def _times() -> tuple[float, float] | None:
        """Idle and total time of all cores, from /proc/stat."""
        try:
            parts = [float(part) for part in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
            return parts[3] + parts[4], sum(parts)
        except (OSError, ValueError, IndexError):
            return None

    def busy(self) -> float | None:
        current = self._times()
        previous, self._previous = self._previous, current
        if current is None or previous is None or current[1] <= previous[1]:
            return None
        return max(0.0, min(1.0, 1 - (current[0] - previous[0]) / (current[1] - previous[1])))
