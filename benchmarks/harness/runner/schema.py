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
