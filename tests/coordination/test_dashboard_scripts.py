"""Dashboard behavior checks that run in Node without a browser, service, or model."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("script", ["dashboard_refresh", "dashboard_setup", "dashboard_tasks", "dashboard_status",
                                    "dashboard_stream", "dashboard_task_center"])
def test_dashboard_script(script):
    executable = shutil.which("node")
    if executable is None:
        pytest.skip("Node is an optional development check, never a runtime dependency")
    path = Path(__file__).with_name(script + ".test.cjs")
    result = subprocess.run([executable, "--test", str(path)], capture_output=True,
                            text=True, encoding="utf-8", timeout=60, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
