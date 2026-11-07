"""Run the standalone demos as users do, with assertions enabled."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

PYTHON_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("name", "milestones"),
    [
        (
            "interruptible_viewport_grid.py",
            ("render Dashboard now", "scroll ->", "edit price:", "all cells match"),
        ),
        (
            "interruptible_async_service.py",
            ("Superseded:", "installed results preserved", "heartbeat ticks=", "revenue=1500"),
        ),
        (
            "interruptible_pause_inspect.py",
            ("Watch triggered:", "Paused: job active=True", "Repair cost:", "repaired margin=20"),
        ),
    ],
)
def test_interruptible_example(name, milestones):
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(PYTHON_ROOT), env.get("PYTHONPATH")))
    )
    # Do not let a caller's optimization setting disable the demos' assertions.
    env.pop("PYTHONOPTIMIZE", None)
    result = subprocess.run(
        [sys.executable, str(PYTHON_ROOT / "examples" / name)],
        cwd=PYTHON_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Finished:" in result.stdout
    for milestone in milestones:
        assert milestone in result.stdout, result.stdout
