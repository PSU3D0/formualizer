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

REQUIRED_SCENARIO_KEYS = {
    "family",
    "tier",
    "comparison_profiles",
    "runtime_modes",
    "regression_gate",
}

REQUIRED_MATRIX_KEYS = {
    "functions",
    "features",
    "support_policy",
    "claim_class",
    "caveat_labels",
}

REQUIRED_PLAN_KEYS = {
    "batches",
}

REQUIRED_PLAN_BATCH_KEYS = {
    "name",
    "engines",
    "modes",
}

PLAN_SELECTOR_KEYS = {
    "scenarios",
    "comparison_profiles",
    "tiers",
    "families",
    "profiles",
    "regression_gate_only",
}


def load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def get_scenario(doc: dict[str, Any], scenario_id: str) -> dict[str, Any] | None:
    for s in doc.get("scenarios", []):
        if s.get("id") == scenario_id:
            return s
    return None


def get_plan(doc: dict[str, Any], plan_name: str) -> dict[str, Any] | None:
    plans = doc.get("plans") or {}
    if not isinstance(plans, dict):
        return None
    plan = plans.get(plan_name)
    return plan if isinstance(plan, dict) else None


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


def build_scenario_index(scenarios_doc: dict[str, Any], matrix_doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    matrix_scenarios = matrix_doc.get("scenarios", {})
    index: dict[str, dict[str, Any]] = {}

    for scenario in scenarios_doc.get("scenarios", []):
        scenario_id = scenario.get("id")
        if not scenario_id:
            continue
        matrix_entry = matrix_scenarios.get(scenario_id, {})
        index[scenario_id] = {
            "name": scenario.get("name", scenario_id),
            "profile": scenario.get("profile", "-"),
            "family": scenario.get("family", "-"),
            "tier": scenario.get("tier", "-"),
            "comparison_profiles": list(scenario.get("comparison_profiles", [])),
            "runtime_modes": list(scenario.get("runtime_modes", [])),
            "regression_gate": bool(scenario.get("regression_gate", False)),
            "support_policy": matrix_entry.get("support_policy", "-"),
            "claim_class": matrix_entry.get("claim_class", "-"),
            "caveat_labels": list(matrix_entry.get("caveat_labels", [])),
            "functions": list(matrix_entry.get("functions", [])),
            "features": list(matrix_entry.get("features", [])),
        }

    return index


def validate_suite_contract(scenarios_doc: dict[str, Any], matrix_doc: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    scenario_defs = scenarios_doc.get("scenarios", [])
    matrix_scenarios = matrix_doc.get("scenarios", {})

    families = set((scenarios_doc.get("families") or {}).keys())
    tiers = set((scenarios_doc.get("tiers") or {}).keys())
    profiles = set((scenarios_doc.get("profiles") or {}).keys())
    comparison_profiles = set((scenarios_doc.get("comparison_profiles") or {}).keys())
    runtime_modes = set((scenarios_doc.get("runtime_modes") or {}).keys())
    regression_gates = set(scenarios_doc.get("regression_gates", []))

    support_policies = set((matrix_doc.get("support_policies") or {}).keys())
    claim_classes = set((matrix_doc.get("claim_classes") or {}).keys())
    caveat_labels = set((matrix_doc.get("caveat_labels") or {}).keys())

    scenario_ids: set[str] = set()
    for scenario in scenario_defs:
        scenario_id = scenario.get("id")
        if not scenario_id:
            errors.append("scenario missing id")
            continue
        scenario_ids.add(scenario_id)

        missing = REQUIRED_SCENARIO_KEYS - set(scenario.keys())
        if missing:
            errors.append(f"scenario {scenario_id}: missing keys {sorted(missing)}")

        family = scenario.get("family")
        if family not in families:
            errors.append(f"scenario {scenario_id}: unknown family {family!r}")

        tier = scenario.get("tier")
        if tier not in tiers:
            errors.append(f"scenario {scenario_id}: unknown tier {tier!r}")

        profile = scenario.get("profile")
        if profile not in profiles:
            errors.append(f"scenario {scenario_id}: unknown profile {profile!r}")

        scenario_profiles = scenario.get("comparison_profiles", [])
        if not isinstance(scenario_profiles, list) or not scenario_profiles:
            errors.append(f"scenario {scenario_id}: comparison_profiles must be a non-empty list")
        else:
            unknown = sorted(set(scenario_profiles) - comparison_profiles)
            if unknown:
                errors.append(f"scenario {scenario_id}: unknown comparison_profiles {unknown}")
            if "core_smoke" in scenario_profiles and tier != "pr_smoke":
                errors.append(f"scenario {scenario_id}: core_smoke requires tier=pr_smoke")
            if "runtime_parity_core" in scenario_profiles and "runtime_parity" not in scenario.get("runtime_modes", []):
                errors.append(f"scenario {scenario_id}: runtime_parity_core requires runtime_modes to include runtime_parity")

        scenario_runtime_modes = scenario.get("runtime_modes", [])
        if not isinstance(scenario_runtime_modes, list) or not scenario_runtime_modes:
            errors.append(f"scenario {scenario_id}: runtime_modes must be a non-empty list")
        else:
            unknown = sorted(set(scenario_runtime_modes) - runtime_modes)
            if unknown:
                errors.append(f"scenario {scenario_id}: unknown runtime_modes {unknown}")

        if scenario_id in regression_gates and not bool(scenario.get("regression_gate", False)):
            errors.append(f"scenario {scenario_id}: listed in regression_gates but regression_gate=false")
        if bool(scenario.get("regression_gate", False)) and scenario_id not in regression_gates:
            errors.append(f"scenario {scenario_id}: regression_gate=true but scenario missing from top-level regression_gates")

        matrix_entry = matrix_scenarios.get(scenario_id)
        if matrix_entry is None:
            errors.append(f"scenario {scenario_id}: missing function_matrix entry")
            continue

        missing_matrix = REQUIRED_MATRIX_KEYS - set(matrix_entry.keys())
        if missing_matrix:
            errors.append(f"function_matrix {scenario_id}: missing keys {sorted(missing_matrix)}")

        support_policy = matrix_entry.get("support_policy")
        if support_policy not in support_policies:
            errors.append(f"function_matrix {scenario_id}: unknown support_policy {support_policy!r}")

        claim_class = matrix_entry.get("claim_class")
        if claim_class not in claim_classes:
            errors.append(f"function_matrix {scenario_id}: unknown claim_class {claim_class!r}")

        entry_caveats = matrix_entry.get("caveat_labels", [])
        if not isinstance(entry_caveats, list):
            errors.append(f"function_matrix {scenario_id}: caveat_labels must be a list")
            entry_caveats = []
        unknown_caveats = sorted(set(entry_caveats) - caveat_labels)
        if unknown_caveats:
            errors.append(f"function_matrix {scenario_id}: unknown caveat_labels {unknown_caveats}")

        if support_policy in {"profile_subset", "explicit_caveat"} and not entry_caveats:
            errors.append(f"function_matrix {scenario_id}: {support_policy} requires at least one caveat label")
        if claim_class == "claim_safe_with_caveats" and not entry_caveats:
            errors.append(f"function_matrix {scenario_id}: claim_safe_with_caveats requires caveat labels")
        if claim_class == "claim_safe_now" and support_policy == "explicit_caveat":
            errors.append(f"function_matrix {scenario_id}: claim_safe_now cannot use support_policy=explicit_caveat")
        if claim_class == "internal_only" and support_policy == "all_engines":
            errors.append(f"function_matrix {scenario_id}: internal_only should not use support_policy=all_engines")

    extra_matrix = sorted(set(matrix_scenarios.keys()) - scenario_ids)
    for scenario_id in extra_matrix:
        errors.append(f"function_matrix {scenario_id}: no matching scenario entry")

    missing_gates = sorted(regression_gates - scenario_ids)
    for scenario_id in missing_gates:
        errors.append(f"regression_gates references unknown scenario {scenario_id}")

    return errors


def validate_plan_contract(plans_doc: dict[str, Any], scenarios_doc: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    plans = plans_doc.get("plans")
    if not isinstance(plans, dict) or not plans:
        return ["plans: expected a non-empty mapping"]

    known_scenarios = {scenario.get("id") for scenario in scenarios_doc.get("scenarios", []) if scenario.get("id")}
    known_engines = {engine.get("id") for engine in scenarios_doc.get("engines", []) if engine.get("id")}
    known_families = set((scenarios_doc.get("families") or {}).keys())
    known_tiers = set((scenarios_doc.get("tiers") or {}).keys())
    known_profiles = set((scenarios_doc.get("profiles") or {}).keys())
    known_comparison_profiles = set((scenarios_doc.get("comparison_profiles") or {}).keys())
    known_runtime_modes = set((scenarios_doc.get("runtime_modes") or {}).keys())

    for plan_name, plan in plans.items():
        if not isinstance(plan, dict):
            errors.append(f"plan {plan_name}: expected object")
            continue

        missing = REQUIRED_PLAN_KEYS - set(plan.keys())
        if missing:
            errors.append(f"plan {plan_name}: missing keys {sorted(missing)}")

        required_engines = plan.get("required_engines", [])
        if required_engines:
            if not isinstance(required_engines, list):
                errors.append(f"plan {plan_name}: required_engines must be a list")
            else:
                unknown = sorted(set(required_engines) - known_engines)
                if unknown:
                    errors.append(f"plan {plan_name}: unknown required_engines {unknown}")

        report = plan.get("report")
        if report is not None:
            if not isinstance(report, dict):
                errors.append(f"plan {plan_name}: report must be an object")
            else:
                output = report.get("output")
                if output is not None and not isinstance(output, str):
                    errors.append(f"plan {plan_name}: report.output must be a string")
                group_by = report.get("group_by")
                if group_by is not None and not isinstance(group_by, (str, list)):
                    errors.append(f"plan {plan_name}: report.group_by must be a string or list")

        batches = plan.get("batches")
        if not isinstance(batches, list) or not batches:
            errors.append(f"plan {plan_name}: batches must be a non-empty list")
            continue

        batch_engine_union: set[str] = set()

        for idx, batch in enumerate(batches):
            if not isinstance(batch, dict):
                errors.append(f"plan {plan_name} batch[{idx}]: expected object")
                continue

            batch_name = batch.get("name", f"batch[{idx}]")
            prefix = f"plan {plan_name} batch {batch_name}"

            missing_batch = REQUIRED_PLAN_BATCH_KEYS - set(batch.keys())
            if missing_batch:
                errors.append(f"{prefix}: missing keys {sorted(missing_batch)}")

            engines = batch.get("engines", [])
            if not isinstance(engines, list) or not engines:
                errors.append(f"{prefix}: engines must be a non-empty list")
            else:
                batch_engine_union.update(str(engine) for engine in engines)
                unknown = sorted(set(engines) - known_engines)
                if unknown:
                    errors.append(f"{prefix}: unknown engines {unknown}")

            modes = batch.get("modes", [])
            if not isinstance(modes, list) or not modes:
                errors.append(f"{prefix}: modes must be a non-empty list")
            else:
                unknown = sorted(set(modes) - known_runtime_modes)
                if unknown:
                    errors.append(f"{prefix}: unknown modes {unknown}")

            scenarios = batch.get("scenarios")
            if scenarios is not None:
                if not isinstance(scenarios, list) or not scenarios:
                    errors.append(f"{prefix}: scenarios must be a non-empty list when provided")
                else:
                    unknown = sorted(set(scenarios) - known_scenarios)
                    if unknown:
                        errors.append(f"{prefix}: unknown scenarios {unknown}")

            batch_profiles = batch.get("comparison_profiles")
            if batch_profiles is not None:
                if not isinstance(batch_profiles, list) or not batch_profiles:
                    errors.append(f"{prefix}: comparison_profiles must be a non-empty list when provided")
                else:
                    unknown = sorted(set(batch_profiles) - known_comparison_profiles)
                    if unknown:
                        errors.append(f"{prefix}: unknown comparison_profiles {unknown}")

            tiers = batch.get("tiers")
            if tiers is not None:
                if not isinstance(tiers, list) or not tiers:
                    errors.append(f"{prefix}: tiers must be a non-empty list when provided")
                else:
                    unknown = sorted(set(tiers) - known_tiers)
                    if unknown:
                        errors.append(f"{prefix}: unknown tiers {unknown}")

            families = batch.get("families")
            if families is not None:
                if not isinstance(families, list) or not families:
                    errors.append(f"{prefix}: families must be a non-empty list when provided")
                else:
                    unknown = sorted(set(families) - known_families)
                    if unknown:
                        errors.append(f"{prefix}: unknown families {unknown}")

            profiles = batch.get("profiles")
            if profiles is not None:
                if not isinstance(profiles, list) or not profiles:
                    errors.append(f"{prefix}: profiles must be a non-empty list when provided")
                else:
                    unknown = sorted(set(profiles) - known_profiles)
                    if unknown:
                        errors.append(f"{prefix}: unknown profiles {unknown}")

            regression_gate_only = batch.get("regression_gate_only")
            if regression_gate_only is not None and not isinstance(regression_gate_only, bool):
                errors.append(f"{prefix}: regression_gate_only must be a boolean")

            if not any(batch.get(key) for key in PLAN_SELECTOR_KEYS if key != "regression_gate_only") and not bool(regression_gate_only):
                errors.append(f"{prefix}: define at least one scenario selector")

        if isinstance(required_engines, list):
            missing_required = sorted(set(required_engines) - batch_engine_union)
            if missing_required:
                errors.append(f"plan {plan_name}: required_engines not referenced by any batch {missing_required}")

    return errors
