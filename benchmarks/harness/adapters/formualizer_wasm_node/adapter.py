from __future__ import annotations

from pathlib import Path
from typing import Any


def run_scenario(*, scenario: dict[str, Any], mode: str, repo_root: Path, scenarios_path: Path) -> dict[str, Any]:
    _ = (scenario, mode, repo_root, scenarios_path)
    return {
        "status": "not_implemented",
        "metrics": {"load_ms": None, "full_eval_ms": None, "incremental_us": None, "peak_rss_mb": None},
        "correctness": {"passed": False, "mismatches": 1, "details": ["formualizer_wasm_node adapter not implemented"]},
        "notes": [],
    }
