"""Owned process trees, each in a cgroup of its own."""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

from formal_geyser import cgroups
from formal_geyser.coordination.runtime import process as process_module
from formal_geyser.coordination.runtime.ownership import tree_quiet
from formal_geyser.coordination.runtime.process import (
    OwnedProcessHost,
    ProcessHostError,
)


async def wait_path(path: Path, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not path.exists():
            await asyncio.sleep(0.01)


def ended(pid: int) -> bool:
    """Whether a process has ended; a zombie has."""
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] == "Z"
    except FileNotFoundError:
        return True


async def until(condition, seconds: float = 5.0) -> bool:
    for _ in range(int(seconds / 0.02)):
        if condition():
            return True
        await asyncio.sleep(0.02)
    return condition()


async def test_tree_shutdown_reaps_a_descendant_after_parent_exits(tmp_path):
    descendant = tmp_path / "descendant.pid"
    script = tmp_path / "parent.py"
    script.write_text(
        "import subprocess,sys\n"
        "from pathlib import Path\n"
        "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(120)'])\n"
        f"pid_file=Path({str(descendant)!r})\n"
        "pending=pid_file.with_suffix('.tmp');pending.write_text(str(p.pid));pending.replace(pid_file)\n"
        "sys.stdin.read()\n",
        encoding="utf-8",
    )
    host = await OwnedProcessHost.start([sys.executable, str(script)], cwd=tmp_path)
    drains = [
        asyncio.create_task(host.stdout.read()),
        asyncio.create_task(host.stderr.read()),
    ]
    try:
        await wait_path(descendant)
        pid = int(descendant.read_text())
        assert host.cgroup is not None and pid in cgroups.processes(host.cgroup)
        result = await host.stop(grace=0.2)
        assert result.tree_quiet
        assert host.tree_quiet
        assert await host.stop(grace=0.1) == result
        assert await until(lambda: ended(pid))
        assert not host.cgroup.exists()  # Removed once the tree had ended.
        await asyncio.wait_for(asyncio.gather(*drains), 2)
    finally:
        await host.stop(grace=0.1)
        await asyncio.gather(*drains)


async def test_a_missing_executable_fails_before_anything_is_created(tmp_path):
    root = await asyncio.to_thread(cgroups.root)
    before = sorted(path.name for path in root.iterdir() if path.is_dir())
    with pytest.raises(FileNotFoundError):
        await OwnedProcessHost.start(
            [str(tmp_path / "definitely-missing-executable")], cwd=tmp_path,
            startup_timeout=1,
        )
    assert sorted(path.name for path in root.iterdir() if path.is_dir()) == before


async def test_caller_environment_is_passed_without_shell_expansion(tmp_path):
    host = await OwnedProcessHost.start(
        [sys.executable, "-c",
         "import os,sys;print(os.environ['FORMAL_GEYSER_TEST']);sys.stdout.flush()"],
        cwd=tmp_path, env={**os.environ, "FORMAL_GEYSER_TEST": "literal $() & | value"},
    )
    try:
        assert (await host.stdout.readline()).strip() == b"literal $() & | value"
    finally:
        await host.stop(grace=0.2)


async def test_a_process_that_cannot_enter_its_cgroup_never_runs_the_command(tmp_path, monkeypatch):
    marker = tmp_path / "ran"
    enter = cgroups.enter
    monkeypatch.setattr(process_module.cgroups, "enter", lambda cgroup, command, **options: enter(cgroup / "absent", command, **options))
    with pytest.raises(ProcessHostError, match="could not enter"):
        await OwnedProcessHost.start([sys.executable, "-c", f"open({str(marker)!r}, 'w')"], cwd=tmp_path)
    assert not marker.exists()


async def test_recovery_ends_a_tree_whose_owner_has_died(tmp_path):
    workspace = tmp_path / ".task-based" / "workers" / "example" / "work"
    workspace.mkdir(parents=True)
    written = tmp_path / "receipt.json"
    owner = tmp_path / "owner.py"
    owner.write_text(
        "import asyncio,json,os,sys\n"
        "from pathlib import Path\n"
        "from formal_geyser.coordination.runtime.process import OwnedProcessHost\n"
        "async def main():\n"
        "    host=await OwnedProcessHost.start([sys.executable,'-c','import time;time.sleep(120)'],"
        f"cwd=Path({str(workspace)!r}))\n"
        f"    Path({str(written)!r}).write_text(json.dumps(host.ownership_receipt))\n"
        "    os._exit(0)\n"
        "asyncio.run(main())\n",
        encoding="utf-8",
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable, str(owner), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    _, stderr = await asyncio.wait_for(process.communicate(), 60)
    assert process.returncode == 0, stderr.decode()
    receipt = json.loads(written.read_text())
    cgroup = cgroups.located(receipt["cgroup"])
    assert cgroup is not None
    try:
        # Its owner has died, and nothing else ended the tree.
        assert cgroups.populated(cgroup) and not tree_quiet(receipt)
        assert tree_quiet(receipt, end_orphan=True) and not cgroups.populated(cgroup)
    finally:
        cgroups.kill(cgroup)


async def test_repeated_startup_cancellation_awaits_owned_tree_cleanup(tmp_path, monkeypatch):
    original_stop = OwnedProcessHost.stop
    marker = tmp_path / "child-started"
    cleaning, release = asyncio.Event(), asyncio.Event()
    captured = []

    async def delayed_stop(host, *, grace=5.0):
        captured.append(host)
        cleaning.set()
        await release.wait()
        return await original_stop(host, grace=grace)

    # The first process is never seen in its cgroup, so its startup keeps waiting.
    monkeypatch.setattr(process_module.cgroups, "processes", lambda cgroup: [])
    monkeypatch.setattr(OwnedProcessHost, "stop", delayed_stop)
    child = f"import time;from pathlib import Path;Path({str(marker)!r}).write_text('started');time.sleep(120)"
    startup = asyncio.create_task(OwnedProcessHost.start([sys.executable, "-c", child], cwd=tmp_path))
    await wait_path(marker)
    startup.cancel()
    await asyncio.wait_for(cleaning.wait(), 2)
    startup.cancel()
    await asyncio.sleep(0)
    assert not startup.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await startup
    assert len(captured) == 1 and captured[0].tree_quiet


def test_a_service_raises_its_open_file_limit_to_the_hard_one(monkeypatch):
    limits = {process_module.resource.RLIMIT_NOFILE: (1024, 524288)}
    monkeypatch.setattr(process_module.resource, "getrlimit", lambda kind: limits[kind])
    monkeypatch.setattr(process_module.resource, "setrlimit", lambda kind, value: limits.__setitem__(kind, value))
    process_module.raise_open_files_limit()
    assert limits[process_module.resource.RLIMIT_NOFILE] == (524288, 524288)
    # An unlimited hard limit is left alone: the soft one cannot be set to it everywhere.
    limits[process_module.resource.RLIMIT_NOFILE] = (1024, process_module.resource.RLIM_INFINITY)
    process_module.raise_open_files_limit()
    assert limits[process_module.resource.RLIMIT_NOFILE] == (1024, process_module.resource.RLIM_INFINITY)
