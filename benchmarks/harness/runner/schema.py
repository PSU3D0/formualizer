from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


REQUIRED_RESULT_KEYS = {
    "engine",
    "scenario",
    "mode",
    "status",
    "metrics",
    "correctness",
    "timestamp",
}


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def get_scenario(doc: dict[str, Any], scenario_id: str) -> dict[str, Any] | None:
    for s in doc.get("scenarios", []):
        if s.get("id") == scenario_id:
            return s
    return None


def validate_result_doc(result: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    missing = REQUIRED_RESULT_KEYS - set(result.keys())
    if missing:
        errors.append(f"missing result keys: {sorted(missing)}")
    if not isinstance(result.get("metrics", {}), dict):
        errors.append("metrics must be object")
    if not isinstance(result.get("correctness", {}), dict):
        errors.append("correctness must be object")
    return errors


def save_result(path: Path, result: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
