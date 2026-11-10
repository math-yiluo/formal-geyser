"""Resident incremental sessions for development checks, with a stand-in Lean server."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from formal_geyser.coordination.models import LeanServiceConfig
from formal_geyser.lean_service import client
from formal_geyser.lean_service.executor import stopped_at
from formal_geyser.lean_service.osjobs import JobGroup
from formal_geyser.lean_service.scheduler import GIB
from formal_geyser.lean_service.service import JobRunner, LeanService
from tests.conftest import FakeRunner
from tests.lean_service_helpers import LocalClient
from tests.test_lean_service import spec, workspace

SERVER = Path(__file__).with_name("fake_lean_server.py")


async def started(hub, log: Path, command=None, **config):
    service = LeanService(hub.root, LeanServiceConfig(**config), runner=FakeRunner(),
                          measure=lambda: (64 * GIB, 128 * GIB), threads=8,
                          session_command=command or (lambda lean: [sys.executable, str(SERVER), str(log)]))
    await service.start()
    return service


def events(log: Path) -> list[str]:
    return [json.loads(line)["event"] for line in log.read_text(encoding="utf-8").splitlines()]


async def check(service, hub, work, module="Probe", **kwargs):
    return await client.run(LocalClient(service), spec(hub, lane="interactive", directory=work, modules=[module],
                                                       **kwargs))


async def test_a_session_stays_resident_and_receives_only_changes(proof_case, tmp_path):
    hub, _, _ = proof_case
    log = tmp_path / "server.jsonl"
    work = workspace(tmp_path, files={"Probe.lean": "def probe : Nat := 1\n", "Other.lean": "def other : Nat := 2\n"})
    service = await started(hub, log)
    try:
        first = (await check(service, hub, work))["compilation"]
        assert first["validation_method"] == "lean-server" and first["ok"]
        assert first["session"]["reopened"] == "opened" and not first["session"]["incremental"]
        (work / "Probe.lean").write_text("def probe : Nat := 3\n", encoding="utf-8")
        second = (await check(service, hub, work))["compilation"]
        assert second["session"]["incremental"] and second["ok"]
        (work / "Probe.lean").write_text("def probe : Nat := 3\n-- error: unknown identifier 'x'\n", encoding="utf-8")
        third = (await check(service, hub, work))["compilation"]
        assert not third["ok"] and third["diagnostics"]["errors"] == 1
        [message] = third["diagnostics"]["messages"]
        assert (message["severity"], message["pos"], message["data"]) == ("error", {"line": 2, "column": 0},
                                                                          "unknown identifier 'x'")
        (work / "Probe.lean").write_text("def probe : Nat := 4\n", encoding="utf-8")
        fourth = (await check(service, hub, work))["compilation"]  # Nothing published: the old error is gone.
        assert fourth["ok"] and fourth["diagnostics"]["messages"] == []
        (work / "Probe.lean").write_text("import Other\ndef probe : Nat := other\n", encoding="utf-8")
        assert (await check(service, hub, work))["compilation"]["session"]["reopened"] == "imports changed"
        (work / "Other.lean").write_text("def other : Nat := 5\n", encoding="utf-8")
        assert (await check(service, hub, work))["compilation"]["session"]["reopened"] == "a project dependency changed"
        (work / "Probe.lean").write_text("import Other\ntheorem t : 1 = 2 := sorry\n", encoding="utf-8")
        holed = (await check(service, hub, work))["compilation"]
        assert not holed["ok"] and holed["reason"] == "Lean reported a declaration using sorry"
        assert events(log) == ["initialize", "open", "change", "change", "change", "close", "open", "close", "open",
                               "change"]
        [state] = service.status()["sessions"]
        assert state["checks"] == 7 and not state["busy"]
        assert not (work / ".lake").exists()  # Outputs stay in the session's own directory.
    finally:
        await service.close()
    assert events(log)[-1] == "shutdown"


async def test_checks_a_session_cannot_serve_use_the_build(proof_case, tmp_path):
    hub, _, _ = proof_case
    log = tmp_path / "server.jsonl"
    work = workspace(tmp_path, files={"Probe.lean": "def probe : Nat := 1\n", "Other.lean": "def other : Nat := 2\n"})
    service = await started(hub, log)
    try:
        inspected = await check(service, hub, work, inspections=[{"module": "Probe", "declaration": "probe"}])
        assert inspected["compilation"]["validation_method"] == "lake-build"
        both = await client.run(LocalClient(service), spec(hub, lane="interactive", directory=work,
                                                            modules=["Other", "Probe"]))
        assert both["compilation"]["validation_method"] == "lake-build"
        assert not log.exists()  # No session was started for them.
    finally:
        await service.close()


async def test_a_failing_session_is_replaced_by_the_build(proof_case, tmp_path):
    hub, _, _ = proof_case
    work = workspace(tmp_path, files={"Probe.lean": "def probe : Nat := 1\n"})
    service = await started(hub, tmp_path / "unused.jsonl", command=lambda lean: [sys.executable, "-c", "pass"])
    try:
        result = await check(service, hub, work)
        assert result["compilation"]["validation_method"] == "lake-build" and result["compilation"]["ok"]
        assert result["session_fallback"] and service.status()["sessions"] == []
    finally:
        await service.close()


async def test_a_server_that_cannot_start_is_replaced_by_the_build(proof_case, tmp_path):
    hub, _, _ = proof_case
    work = workspace(tmp_path, files={"Probe.lean": "def probe : Nat := 1\n"})
    not_a_program = tmp_path / "lean-server"
    not_a_program.write_text("not a program", encoding="utf-8")
    for command in ([str(tmp_path / "missing-lean")], [str(not_a_program)]):
        service = await started(hub, tmp_path / "unused.jsonl", command=lambda lean, command=command: command)
        try:
            result = await check(service, hub, work)
            assert result["compilation"]["validation_method"] == "lake-build" and result["compilation"]["ok"]
            assert result["session_fallback"].startswith("The Lean server did not start")
            assert service.status()["sessions"] == []
        finally:
            await service.close()


async def test_sessions_can_be_turned_off(proof_case, tmp_path):
    hub, _, _ = proof_case
    log = tmp_path / "server.jsonl"
    service = await started(hub, log, sessions=0)
    try:
        result = await check(service, hub, workspace(tmp_path, files={"Probe.lean": "def probe : Nat := 1\n"}))
        assert result["compilation"]["validation_method"] == "lake-build" and not log.exists()
    finally:
        await service.close()


async def test_idle_sessions_are_closed(proof_case, tmp_path):
    hub, _, _ = proof_case
    log = tmp_path / "server.jsonl"
    service = await started(hub, log)
    try:
        await check(service, hub, workspace(tmp_path, files={"Probe.lean": "def probe : Nat := 1\n"}))
        [session] = service.sessions.sessions.values()
        assert session.root.is_dir()
        assert await service.sessions.evict(idle_seconds=0) == 1
        assert service.status()["sessions"] == [] and session.group.pids() == []
        assert events(log)[-1] == "shutdown"
        assert list(session.root.parent.iterdir()) == []  # Its directory went with it.
    finally:
        await service.close()


def test_a_suspended_job_suspends_the_session_it_borrows():
    class Job:
        def __init__(self):
            self.borrowed = []
            self.sched = type("Sched", (), {"state": "suspended"})()

    job = Job()
    runner = JobRunner(None, job)  # It needs no service to borrow and give back.
    group = JobGroup()
    try:
        runner.borrow(group)
        assert job.borrowed == [group] and group.suspended
        runner.give_back(group)
        assert job.borrowed == [] and not group.suspended
    finally:
        group.close()


async def test_a_session_check_that_does_not_end_is_stopped_and_not_built_again(proof_case, tmp_path):
    hub, _, _ = proof_case
    work = workspace(tmp_path, files={"Probe.lean": "theorem probe : True := by\n  exact trivial -- never ends\n"})
    log = tmp_path / "server.jsonl"
    service = await started(hub, log, check_minutes=0.02)  # 1.2 seconds.
    try:
        result = await check(service, hub, work)
        compilation = result["compilation"]
        assert not compilation["ok"] and compilation["process"] == {"timed_out": True}
        assert compilation["reason"].startswith("The development check did not end within")
        assert compilation["reason"].endswith(
            "When it was stopped, Lean was still working on line 2 of Probe.lean: `exact trivial -- never ends`.")
        assert compilation["diagnostics"]["messages"] == []
        assert not result["definitive"] and "session_fallback" not in result
        # The session that went on elaborating is closed, and no build repeats the check.
        assert service.status()["sessions"] == [] and "shutdown" in events(log)
        assert not any("build" in argv for argv, _ in service.process_runner.calls)
        (work / "Probe.lean").write_text("def probe : Nat := 1\n", encoding="utf-8")
        assert (await check(service, hub, work))["compilation"]["ok"]  # In a new session.
    finally:
        await service.close()


def test_where_a_stopped_check_was_names_every_range_and_quotes_the_first():
    text = "import A\n\ntheorem one : True := by\n  trivial\ntheorem two : True := by\n  simp\n  trivial\n"
    assert stopped_at("B.lean", text, ((3, 3), (5, 7)), {}) == (
        "When it was stopped, Lean was still working on lines 3, 5–7 of B.lean: `theorem one : True := by`.")
    assert stopped_at("B.lean", text, (), {"errors": 0, "warnings": 2}) == (
        "By then it had reported 0 errors and 2 warnings, which are in the diagnostics.")
    assert stopped_at("B.lean", text, (), {}) == ""


async def test_a_stopped_session_check_reports_what_lean_had_found(proof_case, tmp_path):
    hub, _, _ = proof_case
    work = workspace(tmp_path, files={"Probe.lean": (
        "theorem first : True := by\n  exact trivial -- error: rewrite failed: motive is not type correct\n\n"
        "theorem slow : True := by -- never ends\n  trivial\n")})
    service = await started(hub, tmp_path / "server.jsonl", check_minutes=0.02)
    try:
        result = await check(service, hub, work)
        compilation = result["compilation"]
        assert compilation["process"] == {"timed_out": True} and not result["definitive"]
        [message] = compilation["diagnostics"]["messages"]
        assert (message["severity"], message["pos"]["line"], message["data"]) == (
            "error", 2, "rewrite failed: motive is not type correct")
        assert (compilation["diagnostics"]["errors"], compilation["diagnostics"]["warnings"]) == (1, 0)
        assert compilation["reason"].endswith(
            "When it was stopped, Lean was still working on line 4 of Probe.lean: "
            "`theorem slow : True := by -- never ends`. By then it had reported 1 error and 0 warnings, "
            "which are in the diagnostics.")
    finally:
        await service.close()
