from __future__ import annotations

import argparse
import importlib
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HARNESS_ROOT = Path(__file__).resolve().parents[1]
BENCH_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(HARNESS_ROOT))

from runner.compare import load_results, write_markdown
from runner.schema import get_scenario, load_yaml, save_result, validate_result_doc

SCENARIOS = BENCH_ROOT / "scenarios.yaml"
RAW = HARNESS_ROOT / "results" / "raw"
REPORTS = HARNESS_ROOT / "results" / "reports"


def _git_sha() -> str | None:
    try:
        out = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, text=True)
        return out.strip()
    except Exception:
        return None


def _load_adapter(engine: str):
    return importlib.import_module(f"adapters.{engine}.adapter")


def cmd_list_engines(_: argparse.Namespace) -> int:
    doc = load_yaml(SCENARIOS)
    for e in doc.get("engines", []):
        print(e.get("id"))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    doc = load_yaml(SCENARIOS)
    scenario = get_scenario(doc, args.scenario)
    if scenario is None:
        print(f"unknown scenario: {args.scenario}")
        return 1

    adapter = _load_adapter(args.engine)
    adapter_result: dict[str, Any] = adapter.run_scenario(
        scenario=scenario,
        mode=args.mode,
        repo_root=REPO_ROOT,
        scenarios_path=SCENARIOS,
    )

    result = {
        "engine": args.engine,
        "scenario": args.scenario,
        "mode": args.mode,
        "status": adapter_result.get("status", "failed"),
        "metrics": adapter_result.get("metrics", {}),
        "correctness": adapter_result.get("correctness", {"passed": False, "mismatches": 1, "details": ["missing correctness"]}),
        "notes": adapter_result.get("notes", []),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "meta": {"git_sha": _git_sha()},
    }

    errs = validate_result_doc(result)
    if errs:
        print("result validation failed:")
        for e in errs:
            print(f"- {e}")
        return 2

    out = RAW / f"{args.scenario}__{args.engine}__{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    save_result(out, result)
    print(f"wrote: {out}")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    results = load_results(RAW)
    out = REPORTS / args.output
    write_markdown(results, out)
    print(f"wrote: {out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Comparative benchmark harness")
    sp = p.add_subparsers(dest="cmd", required=True)

    p_eng = sp.add_parser("list-engines", help="List scenario-declared engines")
    p_eng.set_defaults(func=cmd_list_engines)

    p_run = sp.add_parser("run", help="Run one engine against one scenario")
    p_run.add_argument("--engine", required=True)
    p_run.add_argument("--scenario", required=True)
    p_run.add_argument("--mode", default="native_best")
    p_run.set_defaults(func=cmd_run)

    p_rep = sp.add_parser("report", help="Generate markdown report from raw JSON")
    p_rep.add_argument("--output", default="summary.md")
    p_rep.set_defaults(func=cmd_report)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
