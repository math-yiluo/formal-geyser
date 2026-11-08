"""A long kernel replay runs in several processes at once, each module once; a group that passes keeps its markers
even when another fails, and the report covers every pending module."""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from formal_geyser import lake_build
from formal_geyser.lake_build import REPLAY_GROUP_MODULES, _replay_groups


class Tooling:
    def __init__(self, fail=None, delay=0.0):
        self.calls, self.fail, self.delay, self.running, self.most = [], fail or set(), delay, 0, 0

    async def kernel_replay(self, build_directory, modules, libraries):
        self.calls.append(list(modules))
        self.running += 1
        self.most = max(self.most, self.running)
        try:
            await asyncio.sleep(self.delay)
        finally:
            self.running -= 1
        bad = sorted(self.fail & set(modules))
        if bad:
            return {"ok": False, "replayed_modules": list(modules), "reason": "The kernel rejected a declaration recorded "
                    "in " + bad[0], "problem_module": bad[0], "process": {"returncode": 1}}
        return {"ok": True, "replayed_modules": list(modules), "constants": len(modules), "process": {"returncode": 0}}


def modules(count):
    return [f"Langlands.M{index:05d}" for index in range(count)]


async def run(tooling, pending, threads):
    recorded = []

    async def record(passed):
        recorded.append(list(passed))
    report = await _replay_groups(tooling, Path("build"), pending, [], threads, record)
    return report, recorded


async def test_a_short_replay_stays_in_one_process():
    tooling = Tooling()
    report, recorded = await run(tooling, modules(REPLAY_GROUP_MODULES + 10), threads=8)
    assert len(tooling.calls) == 1 and report["ok"] and "groups" not in report
    assert recorded == [modules(REPLAY_GROUP_MODULES + 10)]


async def test_a_long_replay_runs_in_up_to_threads_processes_each_module_once():
    tooling = Tooling(delay=0.05)
    pending = modules(REPLAY_GROUP_MODULES * 10)
    report, recorded = await run(tooling, pending, threads=4)
    assert len(tooling.calls) == 4 and tooling.most == 4
    assert sorted(module for call in tooling.calls for module in call) == pending
    assert report["ok"] and report["replayed_modules"] == pending and report["constants"] == len(pending)
    assert [group["modules"] for group in report["groups"]] == [250, 250, 250, 250]
    assert sorted(module for part in recorded for module in part) == pending


async def test_a_failing_group_fails_the_replay_but_the_others_keep_their_markers():
    pending = modules(REPLAY_GROUP_MODULES * 3)
    tooling = Tooling(fail={pending[1]})
    report, recorded = await run(tooling, pending, threads=3)
    assert not report["ok"] and report["problem_module"] == pending[1]
    assert report["replayed_modules"] == pending and [group["ok"] for group in report["groups"]] == [True, False, True]
    assert pending[1] not in {module for part in recorded for module in part} and len(recorded) == 2


async def test_an_error_in_one_group_stops_the_others_and_is_raised_as_it_was():
    class Broken(Tooling):
        async def kernel_replay(self, build_directory, modules, libraries):
            if modules[0] == "Langlands.M00000":
                raise OSError("helper missing")
            return await super().kernel_replay(build_directory, modules, libraries)
    tooling = Broken(delay=5)
    with pytest.raises(OSError):
        await asyncio.wait_for(run(tooling, modules(REPLAY_GROUP_MODULES * 2), threads=2), 2)


def test_the_group_size_is_documented_in_the_module():
    assert lake_build.REPLAY_GROUP_MODULES == 100
