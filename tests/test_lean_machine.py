"""What the Lean service reads about the machine, and the sandbox command it wraps Lean in."""
from __future__ import annotations

import pytest

from formal_geyser.lean_service import machine, sandbox

MEMINFO = """MemTotal:       32000000 kB
MemFree:         1000000 kB
MemAvailable:   20000000 kB
SwapTotal:       8000000 kB
SwapFree:        6000000 kB
"""


def test_free_memory_is_what_the_kernel_reports_available(monkeypatch, tmp_path):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(MEMINFO)
    real = machine.Path
    monkeypatch.setattr(machine, "Path", lambda value: meminfo if value == "/proc/meminfo" else real(value))
    assert machine.memory() == (20000000 * 1024, (20000000 + 6000000) * 1024)
    meminfo.write_text("MemTotal: 1 kB\n")
    assert machine.memory() is None


def test_the_cpu_meter_reports_the_busy_share_since_its_previous_reading(monkeypatch):
    readings = iter([(100.0, 1000.0), (150.0, 1200.0), (150.0, 1200.0)])
    monkeypatch.setattr(machine.CpuMeter, "_times", staticmethod(lambda: next(readings)))
    meter = machine.CpuMeter()
    assert meter.busy() == pytest.approx(0.75)  # 50 of 200 ticks idle.
    assert meter.busy() is None  # No time passed.


def test_the_codex_sandbox_wraps_a_command_in_its_lean_profile(tmp_path):
    wrapper = sandbox.CodexSandbox.__new__(sandbox.CodexSandbox)
    wrapper.executable, wrapper.home, wrapper.protected = "/usr/local/bin/codex", tmp_path / "home", []
    command, env = wrapper.wrap(["lean", "--version"], cwd=tmp_path, env={"A": "1"}, writable=[tmp_path])
    assert command[0] == "/usr/local/bin/codex" and command[-3:] == ["--", "lean", "--version"]
    assert command[command.index("sandbox"):command.index("sandbox") + 5] == ["sandbox", "-P", "lean", "-C", str(tmp_path)]
    assert env == {"A": "1", "CODEX_HOME": str(tmp_path / "home")}


def test_lean_code_in_the_sandbox_cannot_read_the_protected_paths_or_write_tmp(tmp_path):
    import tomllib

    wrapper = sandbox.CodexSandbox.__new__(sandbox.CodexSandbox)
    wrapper.executable, wrapper.home, wrapper.protected = "/usr/local/bin/codex", tmp_path / "home", ["/run/user/1000"]
    command, _ = wrapper.wrap(["lean"], cwd=tmp_path, env={}, writable=[tmp_path])
    table = next(item for item in command if item.startswith("permissions.lean.filesystem="))
    assert tomllib.loads("value = " + table.split("=", 1)[1])["value"] == {
        ":slash_tmp": "read", "/run/user/1000": "deny", str(tmp_path.resolve()): "write"}


def test_free_memory_stays_within_the_services_own_memory_bound(monkeypatch, tmp_path):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(MEMINFO)
    real = machine.Path
    monkeypatch.setattr(machine, "Path", lambda value: meminfo if value == "/proc/meminfo" else real(value))
    scope = tmp_path / "scope"
    scope.mkdir()
    (scope / "memory.max").write_text("max\n")
    assert machine.memory(scope) == machine.memory()  # Unbounded: the machine's.
    (scope / "memory.max").write_text(str(4 * 2**30))
    (scope / "memory.current").write_text(str(5 * 2**30))
    (scope / "memory.stat").write_text(f"anon {2**30}\nfile {3 * 2**30}\n")
    # 2 GiB of its 4 are taken (its page cache counts as free): 2 GiB remain, with swap beside.
    assert machine.memory(scope) == (2 * 2**30, 2 * 2**30 + 6000000 * 1024)


def test_the_services_memory_bound_is_a_share_of_the_machine_unless_configured(monkeypatch, tmp_path):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(MEMINFO)
    real = machine.Path
    monkeypatch.setattr(machine, "Path", lambda value: meminfo if value == "/proc/meminfo" else real(value))
    assert machine.memory_limit(None) == int(32000000 * 1024 * machine.SERVICE_MEMORY_SHARE)
    assert machine.memory_limit(12) == 12 * 2**30
    assert machine.memory_limit(0) is None  # No bound.
