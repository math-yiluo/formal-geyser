"""Owned subprocess trees with confirmed shutdown before resource release."""
from __future__ import annotations

import asyncio
import json
import os
import resource
import shutil
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence
from uuid import uuid4

from ... import cgroups


def raise_open_files_limit() -> None:
    """Raise this process's soft open-file limit to the hard one. Each owned process holds
    three pipes: in the Linux pilot on 2026-11-02 TaskHub's 158 worker processes kept about
    750 descriptors open, and every stage failed at the default soft limit of 1,024."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < hard and hard != resource.RLIM_INFINITY:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))


class ProcessHostError(RuntimeError):
    pass


class ProcessTreeNotQuiet(ProcessHostError):
    pass


@dataclass(frozen=True)
class ProcessExit:
    returncode: int | None
    forced: bool
    tree_quiet: bool


def _require_executable(program: str, cwd: Path, env: Mapping[str, str] | None) -> None:
    """Fail as starting a missing program would, before anything is created for it."""
    if "/" in program:
        found = shutil.which(str(cwd / program))
    else:
        found = shutil.which(program, path=(env if env is not None else os.environ).get("PATH", os.defpath))
    if found is None:
        raise FileNotFoundError(2, "No such executable", program)


class OwnedProcessHost:
    """Own one process tree in a cgroup of its own (cgroups.py), which exists before the tree's
    first process runs and is removed once the tree has ended."""

    def __init__(self, process: asyncio.subprocess.Process, *, cgroup: Path | None = None,
                 receipt: Path | None = None):
        self.process = process
        assert process.stdin is not None and process.stdout is not None and process.stderr is not None
        self.stdin = process.stdin
        self.stdout = process.stdout
        self.stderr = process.stderr
        self.cgroup = cgroup
        self._ownership_path = receipt
        self._stop_lock = asyncio.Lock()
        self._exit: ProcessExit | None = None
        self.tree_quiet = False
        self.child_pid: int | None = None

    @classmethod
    async def start(
        cls, command: Sequence[str], *, cwd: Path, env: Mapping[str, str] | None = None,
        max_frame_bytes: int = 8 * 1024 * 1024, startup_timeout: float = 10.0,
        cgroup: Path | None = None, group=None, register: bool = True,
    ) -> OwnedProcessHost:
        """`cgroup` is a planned cgroup path (ownership.planned_receipt); `group` is a lean_service
        JobGroup, below whose cgroup this tree runs; `register` records a TaskHub ownership
        receipt, which processes owned by another service do not need."""
        if not command or any(not isinstance(part, str) for part in command):
            raise ValueError("command must be a nonempty sequence of strings")
        _require_executable(command[0], cwd, env)
        if cgroup is None:
            parent = group.path if group is not None else await asyncio.to_thread(cgroups.root)
            cgroup = parent / ("tree-" + uuid4().hex)
        cgroup.mkdir()
        receipt = None
        try:
            if register:
                from .ownership import register_tree
                receipt = register_tree(cwd.resolve(), cgroup)
            # A Lean service job's processes are the first the kernel ends when memory runs out.
            adjust = getattr(group, "oom_score_adj", None) if group is not None else None
            process = await asyncio.create_subprocess_exec(
                *cgroups.enter(cgroup, command, oom_score_adj=adjust), cwd=str(cwd),
                env=dict(env) if env is not None else None,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE, limit=max_frame_bytes + 1, start_new_session=True,
            )
        except BaseException:
            cgroups.remove(cgroup)
            if receipt is not None:
                from .ownership import record_quiet
                record_quiet(receipt)
            raise
        host = cls(process, cgroup=cgroup, receipt=receipt)
        try:
            await host._entered(startup_timeout)
            host.child_pid = process.pid
            return host
        except BaseException:
            cleanup = asyncio.create_task(host.stop(grace=0.1))
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            cleanup.result()
            raise

    async def _entered(self, timeout: float) -> None:
        """Wait until the first process is in the tree's cgroup, where it runs the command."""
        assert self.cgroup is not None
        deadline = time.monotonic() + timeout
        while self.process.pid not in cgroups.processes(self.cgroup):
            if self.process.returncode is not None:
                if cgroups.usage(self.cgroup)[0] > 0:
                    return  # It ran in its cgroup and has already ended.
                raise ProcessHostError("The process could not enter its cgroup")
            if time.monotonic() >= deadline:
                raise ProcessHostError("The process did not enter its cgroup in time")
            await asyncio.sleep(0.002)

    @property
    def ownership_receipt(self) -> dict | None:
        return json.loads(self._ownership_path.read_text(encoding="utf-8")) if self._ownership_path else None

    def _active_processes(self) -> bool:
        # The first process runs outside the cgroup only until it moves itself in.
        return self.process.returncode is None or (self.cgroup is not None and cgroups.populated(self.cgroup))

    async def _settled(self, deadline: float) -> bool:
        """Wait until the owned tree has exited or the monotonic `deadline` passes; whether it exited."""
        while self._active_processes() and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        return not self._active_processes()

    def _kill_tree(self, *, hard: bool) -> None:
        if self.cgroup is not None:
            if hard:
                cgroups.kill(self.cgroup)
            else:
                cgroups.signal(self.cgroup, signal.SIGTERM)
        try:
            self.process.send_signal(signal.SIGKILL if hard else signal.SIGTERM)
        except ProcessLookupError:
            pass

    async def stop(self, *, grace: float = 5.0) -> ProcessExit:
        """Close stdin, then terminate all remaining descendants and verify."""
        async with self._stop_lock:
            if self._exit is not None:
                return self._exit
            forced = False
            if self.stdin is not None and not self.stdin.is_closing():
                self.stdin.close()
            natural_deadline = time.monotonic() + max(grace, 0.01)
            try:
                await asyncio.wait_for(asyncio.shield(self.process.wait()), max(grace, 0.01))
            except TimeoutError:
                pass
            # Descendants may still be ending when the first process exits. Give the whole
            # tree the remaining natural-exit grace before classifying it as work that
            # actually required termination.
            if not await self._settled(natural_deadline):
                forced = True
                self._kill_tree(hard=False)
                if not await self._settled(time.monotonic() + max(grace, 0.2)):
                    self._kill_tree(hard=True)
                    # SIGKILL cannot be refused; a process holding gigabytes of mapped .olean
                    # files on a loaded machine can still take seconds to be torn down.
                    await self._settled(time.monotonic() + max(grace, 10.0))
            self.tree_quiet = not self._active_processes()
            if not self.tree_quiet:
                raise ProcessTreeNotQuiet("Owned process tree is still active")
            # Drain subprocess pipes in the transport; waiting cannot deadlock once dead.
            try:
                await asyncio.wait_for(self.process.wait(), max(grace, 1.0))
            except TimeoutError:
                pass
            if self.cgroup is not None:
                cgroups.remove(self.cgroup)  # What the tree used stays counted in a job's cgroup.
            self._exit = ProcessExit(self.process.returncode, forced, True)
            if self._ownership_path is not None:
                from .ownership import record_quiet
                try:
                    record_quiet(self._ownership_path)
                except (OSError, ValueError):
                    # The unique cgroup's absence remains independently queryable.
                    pass
            return self._exit
