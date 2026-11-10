"""Process trees in the cgroups of a cgroup v2 subtree that this process owns.

A process that starts process trees (TaskHub, the Lean service, a command-line tool) owns a
delegated cgroup: its own systemd user unit's when that unit has Delegate=yes, or else a new
delegated scope of the user's systemd manager that it moves itself into. It moves itself into
the leaf `owner`, so that the subtree's other cgroups can account memory, and starts each
process tree in a new cgroup of its own: the tree's first process moves itself in before it
runs the command, so no process of the tree runs outside it. A tree is stopped and continued
as a whole (cgroup.freeze), ended as a whole (cgroup.kill), and has ended once its cgroup is
empty; what it used stays readable until its owner removes the cgroup.

This holds while sandboxed commands cannot reach the user's systemd manager, which would let
them start processes elsewhere.
"""
from __future__ import annotations

import errno
import os
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Sequence
from uuid import uuid4

MOUNT = Path("/sys/fs/cgroup")
CONTROLLERS = ("cpu", "memory", "pids")
TREE = re.compile(r"(?:/[\w.@-]+)+/tree-[0-9a-f]{32}")
_ROOT: Path | None = None
_LOCK = threading.Lock()


class Unavailable(OSError):
    """This system cannot give a process a cgroup subtree of its own."""


def _own() -> str:
    """This process's cgroup, as /proc shows it."""
    for line in Path("/proc/self/cgroup").read_text().splitlines():
        if line.startswith("0::"):
            return line[3:]
    raise Unavailable("This system has no unified cgroup v2 hierarchy")


def _manager() -> str:
    """The cgroup of this user's systemd manager."""
    uid = os.getuid()
    return f"/user.slice/user-{uid}.slice/user@{uid}.service"


def bus_environment() -> dict[str, str]:
    """This process's environment, with the user's runtime directory, where systemd's user tools
    find the manager, even if the environment has lost it."""
    return {"XDG_RUNTIME_DIR": f"/run/user/{os.getuid()}", **os.environ}


def own_unit() -> str | None:
    """The systemd user unit this process runs in directly, if it does."""
    relative = _own()
    unit = relative.rsplit("/", 1)[1]
    if relative.startswith(_manager() + "/") and unit.endswith((".scope", ".service")):
        return unit
    return None


def _delegated() -> bool:
    unit = own_unit()
    if unit is None:
        return False
    try:
        shown = subprocess.run(["systemctl", "--user", "show", "--property=Delegate", "--value", unit],
                               capture_output=True, text=True, timeout=30, env=bus_environment())
    except (OSError, subprocess.SubprocessError):
        return False
    return shown.returncode == 0 and shown.stdout.strip() == "yes"


def _new_scope() -> str:
    """Move this process into a new delegated scope of the user's systemd manager."""
    unit = f"formal-geyser-{uuid4().hex}.scope"
    command = ["busctl", "--user", "call", "org.freedesktop.systemd1", "/org/freedesktop/systemd1",
               "org.freedesktop.systemd1.Manager", "StartTransientUnit", "ssa(sv)a(sa(sv))", unit, "fail",
               "3", "PIDs", "au", "1", str(os.getpid()), "Delegate", "b", "true",
               "CollectMode", "s", "inactive-or-failed", "0"]
    try:
        started = subprocess.run(command, capture_output=True, text=True, timeout=30, env=bus_environment())
    except (OSError, subprocess.SubprocessError) as error:
        raise Unavailable(f"The user's systemd manager is unreachable: {error}") from error
    if started.returncode != 0:
        raise Unavailable("The user's systemd manager started no scope for this process: "
                          + started.stderr.strip()[-500:])
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        relative = _own()
        if relative.endswith("/" + unit):
            return relative
        time.sleep(0.01)
    raise Unavailable("This process did not enter its new scope")


def root() -> Path:
    """This process's owned cgroup, set up on first use."""
    global _ROOT
    with _LOCK:
        if _ROOT is None:
            relative = _own() if _delegated() else _new_scope()
            path = MOUNT / relative.lstrip("/")
            leaf = path / "owner"
            leaf.mkdir(exist_ok=True)
            # A cgroup that holds processes cannot account its children: everything in the
            # unit moves to the leaf, this process included.
            for _ in range(100):
                pids = _pids(path)
                if not pids:
                    break
                for pid in pids:
                    try:
                        (leaf / "cgroup.procs").write_text(str(pid))
                    except ProcessLookupError:
                        pass
            available = (path / "cgroup.controllers").read_text().split()
            enabled = (path / "cgroup.subtree_control").read_text().split()
            wanted = [name for name in CONTROLLERS if name in available and name not in enabled]
            if wanted:
                (path / "cgroup.subtree_control").write_text(" ".join("+" + name for name in wanted))
            _ROOT = path
        return _ROOT


def current_root() -> Path | None:
    """This process's owned cgroup if it has been set up (root), without setting it up."""
    return _ROOT


def scope_command(unit: str, command: Sequence[str], *, delegate: bool = False,
                  bound_to: str | None = None, properties: Sequence[str] = ()) -> list[str]:
    """`command` in a new scope `unit` of the user's systemd manager, which outlives this
    process's own unit; with `bound_to`, it ends when that unit does. `properties` are further
    unit properties, such as "MemoryMax=…"."""
    argv = ["systemd-run", "--user", "--scope", "--quiet", "--collect", "--unit=" + unit]
    if delegate:
        argv.append("--property=Delegate=yes")
    if bound_to is not None:
        argv += ["--property=BindsTo=" + bound_to, "--property=After=" + bound_to]
    argv += ["--property=" + item for item in properties]
    return [*argv, "--", *command]


def end_unit(unit: str) -> None:
    """Kill whatever still runs in a scope of the user's systemd manager."""
    try:
        subprocess.run(["systemctl", "--user", "kill", "--signal=SIGKILL", unit],
                       capture_output=True, timeout=30, env=bus_environment())
    except (OSError, subprocess.SubprocessError):
        pass


def enter(cgroup: Path, command: Sequence[str], *, oom_score_adj: int | None = None) -> list[str]:
    """`command`, run by a process that first moves itself into `cgroup`. With `oom_score_adj`,
    that process, and so every process of its tree, is first in line when the kernel ends a
    process because memory ran out: an unprivileged process may raise its own value, never lower
    it, and a failure to raise it does not stop the command."""
    if oom_score_adj is None:
        return ["/bin/sh", "-c", 'echo 0 > "$0" && exec "$@"', str(cgroup / "cgroup.procs"), *command]
    script = 'echo 0 > "$0" || exit 1; { echo "$1" > /proc/self/oom_score_adj; } 2>/dev/null; shift; exec "$@"'
    return ["/bin/sh", "-c", script, str(cgroup / "cgroup.procs"), str(oom_score_adj), *command]


def relative(cgroup: Path) -> str:
    return "/" + cgroup.relative_to(MOUNT).as_posix()


def located(value: object) -> Path | None:
    """The process tree's cgroup that a receipt names, if it names one."""
    if not isinstance(value, str) or not TREE.fullmatch(value) or {".", ".."} & set(value.split("/")):
        return None
    return MOUNT / value.lstrip("/")


def _pids(cgroup: Path) -> list[int]:
    return [int(line) for line in (cgroup / "cgroup.procs").read_text().split()]


def processes(cgroup: Path) -> list[int]:
    """Every process in the cgroup and below it."""
    found: list[int] = []
    for current, _, _ in os.walk(cgroup):
        try:
            found += _pids(Path(current))
        except OSError as error:
            # Removed meanwhile: its cgroup.procs is gone (ENOENT), or no longer readable while
            # the kernel removes it (ENODEV), as for each Lean process's own cgroup as it ends.
            if error.errno not in (errno.ENOENT, errno.ENODEV):
                raise
    return found


def _keys(path: Path) -> dict[str, str]:
    return dict(line.split(" ", 1) for line in path.read_text().splitlines() if " " in line)


def populated(cgroup: Path) -> bool:
    """Whether any process runs in the cgroup or below it."""
    try:
        return _keys(cgroup / "cgroup.events").get("populated") == "1"
    except FileNotFoundError:
        return False


def _write(path: Path, value: str) -> None:
    try:
        path.write_text(value)
    except FileNotFoundError:
        pass  # The cgroup has been removed: nothing runs in it.


def freeze(cgroup: Path, frozen: bool) -> None:
    """Stop or continue every process in the cgroup, including any that enters it later."""
    _write(cgroup / "cgroup.freeze", "1" if frozen else "0")


def kill(cgroup: Path) -> None:
    """SIGKILL every process in the cgroup, frozen ones included."""
    _write(cgroup / "cgroup.kill", "1")


def signal(cgroup: Path, number: int) -> None:
    for pid in processes(cgroup):
        try:
            os.kill(pid, number)
        except ProcessLookupError:
            pass


def limit_memory(cgroup: Path, memory_max: int | None, swap_max: int | None = None) -> None:
    """Bound the memory, and the swap, that the cgroup's processes may hold together: past
    `memory_max` the kernel reclaims the cgroup's own pages and swaps them out, and past both it
    ends one of the cgroup's processes, never a process elsewhere. A system that does not account
    memory or swap leaves that bound out."""
    for name, value in (("memory.max", memory_max), ("memory.swap.max", swap_max)):
        if value is not None and (cgroup / name).exists():
            (cgroup / name).write_text(str(max(0, int(value))))


def oom_kills(cgroup: Path) -> int:
    """How many processes of the cgroup, or below it, the kernel has ended for want of memory,
    whether the cgroup's own limit, an enclosing one or the machine's memory ran out."""
    try:
        return int(_keys(cgroup / "memory.events").get("oom_kill", 0))
    except (FileNotFoundError, ValueError):
        return 0


def memory_headroom(cgroup: Path) -> int | None:
    """What the cgroup may still take before its memory.max, counting its page cache as free
    (the kernel reclaims it first); None when it has no limit or none is readable."""
    try:
        limit = (cgroup / "memory.max").read_text().strip()
        if limit == "max":
            return None
        current = int((cgroup / "memory.current").read_text())
        cached = int(_keys(cgroup / "memory.stat").get("file", 0))
        return max(0, int(limit) - (current - cached))
    except (FileNotFoundError, ValueError):
        return None


def remove(cgroup: Path) -> None:
    """Remove an empty cgroup and the cgroups below it."""
    for current, _, _ in os.walk(cgroup, topdown=False):
        try:
            os.rmdir(current)
        except FileNotFoundError:
            pass


def usage(cgroup: Path) -> tuple[float, int, int, int]:
    """CPU seconds, and private memory now, swap now and peak memory, in bytes: of the
    cgroup's processes, the ended ones included. What is not accounted reads as zero."""
    def read(name: str, key: str | None = None) -> int:
        try:
            if key is None:
                return int((cgroup / name).read_text())
            return int(_keys(cgroup / name).get(key, 0))
        except (FileNotFoundError, ValueError):
            return 0

    return (read("cpu.stat", "usage_usec") / 1e6, read("memory.stat", "anon"),
            read("memory.swap.current"), read("memory.peak"))
