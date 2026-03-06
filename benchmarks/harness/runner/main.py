from __future__ import annotations

import argparse
import importlib
import json
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HARNESS_ROOT = Path(__file__).resolve().parents[1]
BENCH_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(HARNESS_ROOT) not in sys.path:
    sys.path.insert(0, str(HARNESS_ROOT))

from runner.compare import annotate_results, filter_results, load_results, write_markdown
from runner.schema import (
    build_scenario_index,
    get_plan,
    get_scenario,
    load_yaml,
    save_result,
    validate_plan_contract,
    validate_result_doc,
    validate_suite_contract,
)

SCENARIOS = BENCH_ROOT / "scenarios.yaml"
FUNCTION_MATRIX = BENCH_ROOT / "function_matrix.yaml"
PLANS = HARNESS_ROOT / "plans.yaml"
RAW = HARNESS_ROOT / "results" / "raw"
REPORTS = HARNESS_ROOT / "results" / "reports"
VALID_GROUP_FIELDS = {
    "family",
    "tier",
    "profile",
    "mode",
    "claim_class",
    "support_policy",
    "comparison_profile",
}
SUCCESS_STATUSES = {"ok", "success", "passed"}


def _git_sha() -> str | None:
    try:
        out = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, text=True)
        return out.strip()
    except Exception:
        return None


def _load_adapter(engine: str):
    return importlib.import_module(f"adapters.{engine}.adapter")


def _validate_suite() -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    scenarios_doc = load_yaml(SCENARIOS)
    matrix_doc = load_yaml(FUNCTION_MATRIX)
    errors = validate_suite_contract(scenarios_doc, matrix_doc)
    return scenarios_doc, matrix_doc, errors


def _validate_plans() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[str]]:
    scenarios_doc, matrix_doc, errors = _validate_suite()
    if errors:
        return scenarios_doc, matrix_doc, {}, errors
    plans_doc = load_yaml(PLANS)
    errors = validate_plan_contract(plans_doc, scenarios_doc)
    return scenarios_doc, matrix_doc, plans_doc, errors


def _parse_group_by(raw_value: str | list[str] | None) -> list[str]:
    if raw_value is None:
        return ["family", "tier"]
    if isinstance(raw_value, list):
        fields = [str(part).strip() for part in raw_value if str(part).strip()]
    else:
        fields = [part.strip() for part in str(raw_value).split(",") if part.strip()]
    invalid = [field for field in fields if field not in VALID_GROUP_FIELDS]
    if invalid:
        raise ValueError(f"unsupported group-by field(s): {', '.join(invalid)}")
    return fields


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def _result_ok(result: dict[str, Any]) -> bool:
    status = str(result.get("status", "")).lower()
    correctness = result.get("correctness", {}) or {}
    return status in SUCCESS_STATUSES and bool(correctness.get("passed", False))


def _load_all_raw_results() -> list[dict[str, Any]]:
    return load_results(RAW)


def _existing_run_keys(git_sha: str | None) -> set[tuple[str, str, str, str | None]]:
    keys: set[tuple[str, str, str, str | None]] = set()
    for row in _load_all_raw_results():
        meta = row.get("meta", {}) or {}
        keys.add((str(row.get("scenario", "")), str(row.get("engine", "")), str(row.get("mode", "")), meta.get("git_sha")))
    return keys


def _plan_results(plan_name: str, git_sha: str | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in _load_all_raw_results():
        meta = row.get("meta", {}) or {}
        if meta.get("run_plan") != plan_name:
            continue
        if git_sha is not None and meta.get("git_sha") != git_sha:
            continue
        rows.append(row)
    return rows


def _execute_run(
    *,
    scenario: dict[str, Any],
    engine: str,
    mode: str,
    meta_extra: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], Path]:
    adapter = _load_adapter(engine)
    adapter_result: dict[str, Any] = adapter.run_scenario(
        scenario=scenario,
        mode=mode,
        repo_root=REPO_ROOT,
        scenarios_path=SCENARIOS,
    )

    meta = {"git_sha": _git_sha()}
    if meta_extra:
        meta.update(meta_extra)

    result = {
        "engine": engine,
        "scenario": scenario["id"],
        "mode": mode,
        "status": adapter_result.get("status", "failed"),
        "metrics": adapter_result.get("metrics", {}),
        "correctness": adapter_result.get("correctness", {"passed": False, "mismatches": 1, "details": ["missing correctness"]}),
        "notes": adapter_result.get("notes", []),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "meta": meta,
    }

    errs = validate_result_doc(result)
    if errs:
        raise ValueError("; ".join(errs))

    out = RAW / (
        f"{_slug(scenario['id'])}__{_slug(engine)}__{_slug(mode)}__"
        f"{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
    )
    save_result(out, result)
    return result, out


def _scenario_matches_batch(scenario: dict[str, Any], batch: dict[str, Any]) -> bool:
    scenario_id = str(scenario.get("id", ""))
    if batch.get("scenarios") and scenario_id not in set(batch.get("scenarios", [])):
        return False
    if batch.get("comparison_profiles") and not (set(batch.get("comparison_profiles", [])) & set(scenario.get("comparison_profiles", []))):
        return False
    if batch.get("tiers") and scenario.get("tier") not in set(batch.get("tiers", [])):
        return False
    if batch.get("families") and scenario.get("family") not in set(batch.get("families", [])):
        return False
    if batch.get("profiles") and scenario.get("profile") not in set(batch.get("profiles", [])):
        return False
    if batch.get("regression_gate_only") and not bool(scenario.get("regression_gate", False)):
        return False
    if not set(batch.get("modes", [])) & set(scenario.get("runtime_modes", [])):
        return False
    return True


def _resolve_plan_tasks(plan_name: str, plan: dict[str, Any], scenarios_doc: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    tasks: list[dict[str, Any]] = []
    errors: list[str] = []
    seen: set[tuple[str, str, str]] = set()

    for batch in plan.get("batches", []):
        batch_name = str(batch.get("name", "unnamed-batch"))
        matched = [scenario for scenario in scenarios_doc.get("scenarios", []) if _scenario_matches_batch(scenario, batch)]
        if not matched:
            errors.append(f"plan {plan_name} batch {batch_name}: selectors matched no scenarios")
            continue

        batch_tasks = 0
        for scenario in matched:
            supported_modes = set(scenario.get("runtime_modes", []))
            for mode in batch.get("modes", []):
                if mode not in supported_modes:
                    continue
                for engine in batch.get("engines", []):
                    key = (str(scenario.get("id", "")), str(engine), str(mode))
                    if key in seen:
                        continue
                    seen.add(key)
                    tasks.append(
                        {
                            "scenario_id": str(scenario.get("id", "")),
                            "engine": str(engine),
                            "mode": str(mode),
                            "batch": batch_name,
                        }
                    )
                    batch_tasks += 1

        if batch_tasks == 0:
            errors.append(f"plan {plan_name} batch {batch_name}: selectors resolved scenarios but no runnable mode/task pairs")

    return tasks, errors


def _generated_scenarios_for_tasks(tasks: list[dict[str, Any]], scenarios_doc: dict[str, Any]) -> list[str]:
    generated: list[str] = []
    seen: set[str] = set()
    for task in tasks:
        scenario = get_scenario(scenarios_doc, task["scenario_id"])
        if scenario is None:
            continue
        if scenario.get("source", {}).get("kind") != "generated":
            continue
        scenario_id = str(scenario.get("id"))
        if scenario_id in seen:
            continue
        seen.add(scenario_id)
        generated.append(scenario_id)
    return generated


def _generate_corpus_for_scenarios(scenario_ids: list[str], *, dry_run: bool = False) -> None:
    if not scenario_ids:
        return
    cmd = [
        "cargo",
        "run",
        "-p",
        "formualizer-bench-core",
        "--features",
        "xlsx",
        "--bin",
        "generate-corpus",
        "--",
        "--scenarios",
        str(SCENARIOS),
    ]
    for scenario_id in scenario_ids:
        cmd.extend(["--only", scenario_id])

    if dry_run:
        print("[dry-run] generate corpus:", " ".join(cmd))
        return

    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def _blocking_failure(result: dict[str, Any], required_engines: set[str]) -> bool:
    return result.get("engine") in required_engines and not _result_ok(result)


def _status_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(row.get("status", "unknown")) for row in results)
    return dict(sorted(counts.items()))


def _print_plan_summary(plan_name: str, results: list[dict[str, Any]], blocking_failures: list[dict[str, Any]], skipped: int) -> None:
    print(f"plan: {plan_name}")
    print(f"- results: {len(results)}")
    print(f"- skipped existing: {skipped}")
    print(f"- status counts: {_status_counts(results)}")
    print(f"- blocking failures: {len(blocking_failures)}")


def cmd_list_engines(_: argparse.Namespace) -> int:
    doc = load_yaml(SCENARIOS)
    for e in doc.get("engines", []):
        print(e.get("id"))
    return 0


def cmd_list_plans(_: argparse.Namespace) -> int:
    _, _, plans_doc, errors = _validate_plans()
    if errors:
        print("plan validation failed:")
        for error in errors:
            print(f"- {error}")
        return 2

    for name, plan in (plans_doc.get("plans") or {}).items():
        description = str((plan or {}).get("description", "")).strip()
        batches = len((plan or {}).get("batches", []) or [])
        print(f"{name}\t{batches} batch(es)\t{description}")
    return 0


def cmd_validate_suite(_: argparse.Namespace) -> int:
    _, _, errors = _validate_suite()
    if errors:
        print("suite validation failed:")
        for error in errors:
            print(f"- {error}")
        return 2
    print("benchmark metadata contract OK")
    return 0


def cmd_validate_plans(_: argparse.Namespace) -> int:
    scenarios_doc, _, plans_doc, errors = _validate_plans()
    if errors:
        print("plan validation failed:")
        for error in errors:
            print(f"- {error}")
        return 2

    plans = plans_doc.get("plans") or {}
    for name, plan in plans.items():
        tasks, resolve_errors = _resolve_plan_tasks(name, plan, scenarios_doc)
        if resolve_errors:
            print("plan validation failed:")
            for error in resolve_errors:
                print(f"- {error}")
            return 2
        print(f"{name}: {len(tasks)} runnable task(s)")

    return 0


def cmd_run(args: argparse.Namespace) -> int:
    doc = load_yaml(SCENARIOS)
    scenario = get_scenario(doc, args.scenario)
    if scenario is None:
        print(f"unknown scenario: {args.scenario}")
        return 1

    try:
        result, out = _execute_run(scenario=scenario, engine=args.engine, mode=args.mode)
    except ValueError as exc:
        print(f"result validation failed: {exc}")
        return 2

    print(f"wrote: {out}")
    return 0 if _result_ok(result) else 1


def cmd_run_plan(args: argparse.Namespace) -> int:
    scenarios_doc, matrix_doc, plans_doc, errors = _validate_plans()
    if errors:
        print("plan validation failed:")
        for error in errors:
            print(f"- {error}")
        return 2

    plan = get_plan(plans_doc, args.plan)
    if plan is None:
        print(f"unknown plan: {args.plan}")
        return 1

    tasks, resolve_errors = _resolve_plan_tasks(args.plan, plan, scenarios_doc)
    if resolve_errors:
        print("plan resolution failed:")
        for error in resolve_errors:
            print(f"- {error}")
        return 2

    git_sha = _git_sha()
    if args.skip_existing:
        existing_keys = _existing_run_keys(git_sha)
    else:
        existing_keys = set()

    generated_scenarios = _generated_scenarios_for_tasks(tasks, scenarios_doc)
    if plan.get("generate_corpus", False) and not args.no_generate:
        _generate_corpus_for_scenarios(generated_scenarios, dry_run=args.dry_run)

    if args.dry_run:
        print(f"plan: {args.plan}")
        print(f"- tasks: {len(tasks)}")
        print(f"- generated scenarios: {len(generated_scenarios)}")
        for task in tasks:
            print(f"- {task['batch']}: {task['scenario_id']} :: {task['engine']} :: {task['mode']}")
        return 0

    scenario_index = build_scenario_index(scenarios_doc, matrix_doc)
    required_engines = set(plan.get("required_engines", []))
    continue_on_error = bool(plan.get("continue_on_error", False))

    results_new: list[dict[str, Any]] = []
    blocking_failures: list[dict[str, Any]] = []
    skipped_existing = 0

    for idx, task in enumerate(tasks, start=1):
        key = (task["scenario_id"], task["engine"], task["mode"], git_sha)
        if args.skip_existing and key in existing_keys:
            skipped_existing += 1
            print(f"[skip {idx}/{len(tasks)}] {task['batch']} :: {task['scenario_id']} :: {task['engine']} :: {task['mode']} (existing current-sha result)")
            continue

        scenario = get_scenario(scenarios_doc, task["scenario_id"])
        if scenario is None:
            print(f"missing scenario during run: {task['scenario_id']}")
            return 2

        print(f"[run {idx}/{len(tasks)}] {task['batch']} :: {task['scenario_id']} :: {task['engine']} :: {task['mode']}")
        try:
            result, out = _execute_run(
                scenario=scenario,
                engine=task["engine"],
                mode=task["mode"],
                meta_extra={"run_plan": args.plan, "run_batch": task["batch"]},
            )
        except ValueError as exc:
            print(f"result validation failed: {exc}")
            return 2

        print(f"  wrote: {out}")
        print(f"  status: {result['status']} correctness={'pass' if result.get('correctness', {}).get('passed') else 'fail'}")
        results_new.append(result)

        if _blocking_failure(result, required_engines):
            blocking_failures.append(result)
            if not continue_on_error:
                print("stopping early because a blocking run failed")
                break

    plan_results = _plan_results(args.plan, git_sha)
    annotated = annotate_results(plan_results, scenario_index)

    report_cfg = plan.get("report") or {}
    try:
        group_by = _parse_group_by(report_cfg.get("group_by"))
    except ValueError as exc:
        print(str(exc))
        return 2
    report_name = str(report_cfg.get("output") or f"{args.plan}.md")
    report_path = REPORTS / report_name
    write_markdown(
        annotated,
        report_path,
        group_by=group_by,
        filters={"plan": [args.plan], "git_sha": [git_sha or "unknown"]},
    )

    manifest = {
        "plan": args.plan,
        "git_sha": git_sha,
        "generated_scenarios": generated_scenarios,
        "selected_task_count": len(tasks),
        "executed_task_count": len(results_new),
        "skipped_existing_count": skipped_existing,
        "blocking_failure_count": len(blocking_failures),
        "status_counts": _status_counts(plan_results),
        "report": str(report_path.relative_to(HARNESS_ROOT)),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = REPORTS / f"{args.plan}.manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    _print_plan_summary(args.plan, plan_results, blocking_failures, skipped_existing)
    print(f"- report: {report_path}")
    print(f"- manifest: {manifest_path}")
    return 1 if blocking_failures else 0


def cmd_report(args: argparse.Namespace) -> int:
    scenarios_doc, matrix_doc, errors = _validate_suite()
    if errors:
        print("suite validation failed:")
        for error in errors:
            print(f"- {error}")
        return 2

    try:
        group_by = _parse_group_by(args.group_by)
    except ValueError as exc:
        print(str(exc))
        return 2

    scenario_index = build_scenario_index(scenarios_doc, matrix_doc)
    results = annotate_results(load_results(RAW), scenario_index)
    filtered = filter_results(
        results,
        families=args.family,
        tiers=args.tier,
        profiles=args.profile,
        modes=args.mode,
        comparison_profiles=args.comparison_profile,
        claim_classes=args.claim_class,
        regression_gate_only=args.regression_gate,
    )

    out = REPORTS / args.output
    write_markdown(
        filtered,
        out,
        group_by=group_by,
        filters={
            "family": args.family or [],
            "tier": args.tier or [],
            "profile": args.profile or [],
            "mode": args.mode or [],
            "comparison_profile": args.comparison_profile or [],
            "claim_class": args.claim_class or [],
            "regression_gate": args.regression_gate,
        },
    )
    print(f"wrote: {out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Comparative benchmark harness")
    sp = p.add_subparsers(dest="cmd", required=True)

    p_eng = sp.add_parser("list-engines", help="List scenario-declared engines")
    p_eng.set_defaults(func=cmd_list_engines)

    p_plans = sp.add_parser("list-plans", help="List configured benchmark execution plans")
    p_plans.set_defaults(func=cmd_list_plans)

    p_val = sp.add_parser("validate-suite", help="Validate benchmark metadata and support contracts")
    p_val.set_defaults(func=cmd_validate_suite)

    p_val_plans = sp.add_parser("validate-plans", help="Validate execution plans and resolve runnable task matrices")
    p_val_plans.set_defaults(func=cmd_validate_plans)

    p_run = sp.add_parser("run", help="Run one engine against one scenario")
    p_run.add_argument("--engine", required=True)
    p_run.add_argument("--scenario", required=True)
    p_run.add_argument("--mode", default="native_best")
    p_run.set_defaults(func=cmd_run)

    p_run_plan = sp.add_parser("run-plan", help="Run a named execution plan from plans.yaml")
    p_run_plan.add_argument("--plan", required=True)
    p_run_plan.add_argument("--dry-run", action="store_true")
    p_run_plan.add_argument("--no-generate", action="store_true", help="Skip corpus generation even if plan requests it")
    p_run_plan.add_argument(
        "--skip-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip results already present for the current git SHA (default: true)",
    )
    p_run_plan.set_defaults(func=cmd_run_plan)

    p_rep = sp.add_parser("report", help="Generate markdown report from raw JSON")
    p_rep.add_argument("--output", default="summary.md")
    p_rep.add_argument("--group-by", default="family,tier")
    p_rep.add_argument("--family", action="append")
    p_rep.add_argument("--tier", action="append")
    p_rep.add_argument("--profile", action="append")
    p_rep.add_argument("--mode", action="append")
    p_rep.add_argument("--comparison-profile", action="append")
    p_rep.add_argument("--claim-class", action="append")
    p_rep.add_argument("--regression-gate", action="store_true")
    p_rep.set_defaults(func=cmd_report)

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
