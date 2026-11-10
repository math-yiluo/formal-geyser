"""A long kernel replay runs in several processes at once, each module once, each process a run of the import order;
a group that reaches one name twice is replayed again in halves; a group that passes keeps its markers even when
another fails, and the report covers every pending module."""
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
    bad = pending[REPLAY_GROUP_MODULES + 1]
    tooling = Tooling(fail={bad})
    report, recorded = await run(tooling, pending, threads=3)
    assert not report["ok"] and report["problem_module"] == bad
    assert report["replayed_modules"] == pending and [group["ok"] for group in report["groups"]] == [True, False, True]
    assert bad not in {module for part in recorded for module in part} and len(recorded) == 2


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
    assert lake_build.REPLAY_GROUP_MODULES == 100 and lake_build.REPLAY_GROUP_MOST_MODULES == 250


async def test_a_replay_longer_than_its_processes_hold_runs_in_bounded_groups_in_turn():
    pending = modules(lake_build.REPLAY_GROUP_MOST_MODULES * 6)
    tooling = Tooling(delay=0.02)
    report, recorded = await run(tooling, pending, threads=2)
    assert report["ok"] and len(tooling.calls) == 6 and tooling.most == 2
    assert [group["modules"] for group in report["groups"]] == [lake_build.REPLAY_GROUP_MOST_MODULES] * 6
    assert sorted(recorded) == [pending[index * 250:(index + 1) * 250] for index in range(6)]


class Helper(Tooling):
    """Holds a name as KernelReplay.lean does for several modules: a member that the group reaches through an import
    outside it, or two modules the group reaches that declare one name (`clashes`)."""

    def __init__(self, imports, clashes=(), **kwargs):
        super().__init__(**kwargs)
        self.imports, self.clashes = imports, clashes

    def loaded(self, modules):
        group, seen = set(modules), set()
        todo = [item for module in modules for item in self.imports.get(module, ()) if item not in group]
        while todo:
            if (module := todo.pop()) not in seen:
                seen.add(module)
                todo.extend(self.imports.get(module, ()))
        return seen

    async def kernel_replay(self, build_directory, modules, libraries):
        loaded = self.loaded(modules)
        reached = loaded | set(modules)
        held = sorted(loaded & set(modules)) + [left for left, right in self.clashes if {left, right} <= reached]
        if len(modules) > 1 and held:
            self.calls.append(list(modules))
            return {"ok": False, "replayed_modules": list(modules), "held_name": held[0],
                    "reason": "Modules replayed together reach two declarations named " + held[0],
                    "process": {"returncode": 4}}
        return await super().kernel_replay(build_directory, modules, libraries)


async def test_groups_are_runs_of_the_import_order_so_none_reaches_a_member_through_another_group():
    # Each module imports the two before it; the pending list is in import order (_replay_keys).
    pending = modules(REPLAY_GROUP_MODULES * 8)
    imports = {module: pending[max(0, index - 2):index] for index, module in enumerate(pending)}
    tooling = Helper(imports)
    report, recorded = await run(tooling, pending, threads=4)
    assert report["ok"] and "split_groups" not in report and len(tooling.calls) == 4
    assert sorted(tooling.calls) == [pending[index * 200:(index + 1) * 200] for index in range(4)]
    assert sorted(module for part in recorded for module in part) == pending
    # Round-robin groups, as on 11-08, reach members through the other groups.
    assert not (await Helper(imports).kernel_replay(Path("build"), pending[1::4], []))["ok"]


async def test_a_group_that_reaches_a_name_twice_is_split_until_no_group_does():
    pending = modules(REPLAY_GROUP_MODULES * 4)
    imports = {pending[10]: ["Langlands.Left"], pending[20]: ["Langlands.Right"]}
    # Two imports declare one name, and two requested modules declare another.
    clashes = [("Langlands.Left", "Langlands.Right"), (pending[150], pending[160])]
    tooling = Helper(imports, clashes, delay=0.01)
    report, recorded = await run(tooling, pending, threads=4)
    assert report["ok"] and report["replayed_modules"] == pending, report
    assert sorted(module for part in recorded for module in part) == pending
    assert not any({pending[10], pending[20]} <= set(part) or {pending[150], pending[160]} <= set(part)
                   for part in recorded)
    assert {item["held_name"] for item in report["split_groups"]} == {"Langlands.Left", pending[150]}
    assert sum(group["modules"] for group in report["groups"]) == len(pending) and tooling.most <= 4
    assert report["constants"] == len(pending)


async def test_a_single_module_is_never_split_again():
    class Holding(Tooling):
        async def kernel_replay(self, build_directory, modules, libraries):
            self.calls.append(list(modules))
            return {"ok": False, "replayed_modules": list(modules), "held_name": "clash", "reason": "held",
                    "process": {"returncode": 4}}
    tooling = Holding()
    report, recorded = await run(tooling, modules(2), threads=1)
    assert tooling.calls == [modules(2), modules(1), modules(2)[1:]]
    assert not report["ok"] and recorded == [] and [group["ok"] for group in report["groups"]] == [False, False]
    assert report["split_groups"] == [{"modules": 2, "held_name": "clash"}]


def test_replay_keys_list_each_module_after_the_project_modules_it_imports(tmp_path):
    sources = {"A.lean": b"import B\nimport C\n", "B.lean": b"import C\n", "C.lean": b"",
               "D.lean": b"import A\nimport Mathlib.Data.Nat\n", "E.lean": b""}
    order = list(lake_build._replay_keys(tmp_path, sources, ["D", "E", "B"], "identity"))
    assert sorted(order) == ["A", "B", "C", "D", "E"]
    for module, body in sources.items():
        for line in body.decode().splitlines():
            imported = line.removeprefix("import ")
            if imported + ".lean" in sources:
                assert order.index(imported) < order.index(module[:-5])
