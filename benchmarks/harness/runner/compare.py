from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_results(raw_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for f in sorted(raw_dir.glob("*.json")):
        rows.append(json.loads(f.read_text(encoding="utf-8")))
    return rows


def write_markdown(results: list[dict[str, Any]], out: Path) -> None:
    lines = [
        "# Benchmark Summary",
        "",
        "| Scenario | Engine | Mode | Status | Incremental (us) | Full eval (ms) | Correctness |",
        "|---|---|---|---|---:|---:|---|",
    ]
    for r in results:
        m = r.get("metrics", {})
        c = r.get("correctness", {})
        lines.append(
            f"| {r.get('scenario','-')} | {r.get('engine','-')} | {r.get('mode','-')} | {r.get('status','-')} | "
            f"{m.get('incremental_us','-')} | {m.get('full_eval_ms','-')} | "
            f"{'pass' if c.get('passed') else 'fail'} |"
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
