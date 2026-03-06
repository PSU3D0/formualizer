from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any


def run_scenario(*, scenario: dict[str, Any], mode: str, repo_root: Path, scenarios_path: Path) -> dict[str, Any]:
    cmd = [
        "cargo",
        "run",
        "--release",
        "-p",
        "formualizer-bench-core",
        "--features",
        "ironcalc_runner",
        "--bin",
        "run-ironcalc-native",
        "--",
        "--scenarios",
        str(scenarios_path),
        "--scenario",
        scenario["id"],
        "--root",
        str(repo_root),
        "--mode",
        mode,
    ]

    proc = subprocess.run(
        cmd,
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    if proc.returncode != 0:
        return {
            "status": "failed",
            "metrics": {"load_ms": None, "full_eval_ms": None, "incremental_us": None, "peak_rss_mb": None},
            "correctness": {"passed": False, "mismatches": 1, "details": ["adapter command failed"]},
            "notes": [proc.stderr.strip() or proc.stdout.strip() or f"exit={proc.returncode}"],
        }

    try:
        payload = json.loads(proc.stdout.strip().splitlines()[-1])
    except Exception as exc:
        return {
            "status": "failed",
            "metrics": {"load_ms": None, "full_eval_ms": None, "incremental_us": None, "peak_rss_mb": None},
            "correctness": {"passed": False, "mismatches": 1, "details": ["invalid adapter json output"]},
            "notes": [f"parse_error={exc}", proc.stdout[-4000:]],
        }

    return {
        "status": payload.get("status", "failed"),
        "metrics": payload.get("metrics", {}),
        "correctness": payload.get("correctness", {"passed": False, "mismatches": 1, "details": ["missing correctness"]}),
        "notes": payload.get("notes", []),
    }
