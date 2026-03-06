from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


GROUP_FIELD_NAMES = {
    "family": "family",
    "tier": "tier",
    "profile": "profile",
    "mode": "mode",
    "claim_class": "claim_class",
    "support_policy": "support_policy",
    "comparison_profile": "comparison_profile",
}


def load_results(raw_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for f in sorted(raw_dir.glob("*.json")):
        rows.append(json.loads(f.read_text(encoding="utf-8")))
    return rows


def annotate_results(results: list[dict[str, Any]], scenario_index: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    annotated: list[dict[str, Any]] = []
    for row in results:
        scenario_id = row.get("scenario", "-")
        meta = scenario_index.get(scenario_id, {})
        combined = dict(row)
        combined["scenario_name"] = meta.get("name", scenario_id)
        combined["profile"] = meta.get("profile", "-")
        combined["family"] = meta.get("family", "-")
        combined["tier"] = meta.get("tier", "-")
        combined["comparison_profiles"] = list(meta.get("comparison_profiles", []))
        combined["runtime_modes"] = list(meta.get("runtime_modes", []))
        combined["regression_gate"] = bool(meta.get("regression_gate", False))
        combined["support_policy"] = meta.get("support_policy", "-")
        combined["claim_class"] = meta.get("claim_class", "-")
        combined["declared_caveats"] = list(meta.get("caveat_labels", []))
        combined["caveats"] = _combined_caveats(combined)
        annotated.append(combined)
    return annotated


def filter_results(
    results: list[dict[str, Any]],
    *,
    families: Iterable[str] | None = None,
    tiers: Iterable[str] | None = None,
    profiles: Iterable[str] | None = None,
    modes: Iterable[str] | None = None,
    comparison_profiles: Iterable[str] | None = None,
    claim_classes: Iterable[str] | None = None,
    regression_gate_only: bool = False,
) -> list[dict[str, Any]]:
    family_set = set(families or [])
    tier_set = set(tiers or [])
    profile_set = set(profiles or [])
    mode_set = set(modes or [])
    comparison_profile_set = set(comparison_profiles or [])
    claim_class_set = set(claim_classes or [])

    filtered: list[dict[str, Any]] = []
    for row in results:
        if family_set and row.get("family") not in family_set:
            continue
        if tier_set and row.get("tier") not in tier_set:
            continue
        if profile_set and row.get("profile") not in profile_set:
            continue
        if mode_set and row.get("mode") not in mode_set:
            continue
        if claim_class_set and row.get("claim_class") not in claim_class_set:
            continue
        if comparison_profile_set and not (comparison_profile_set & set(row.get("comparison_profiles", []))):
            continue
        if regression_gate_only and not row.get("regression_gate", False):
            continue
        filtered.append(row)
    return filtered


def write_markdown(
    results: list[dict[str, Any]],
    out: Path,
    *,
    group_by: list[str] | None = None,
    filters: dict[str, list[str] | bool] | None = None,
) -> None:
    group_by = group_by or ["family", "tier"]
    lines = ["# Benchmark Summary", ""]
    lines.append(f"- Rows: {len(results)}")
    lines.append(f"- Grouped by: {', '.join(group_by) if group_by else 'none'}")
    if filters:
        active_filters = []
        for key, value in filters.items():
            if value is False:
                continue
            if value in (None, [], ()):
                continue
            if value is True:
                active_filters.append(key)
            else:
                active_filters.append(f"{key}={','.join(str(v) for v in value)}")
        lines.append(f"- Filters: {', '.join(active_filters) if active_filters else 'none'}")
    lines.append("")

    if not results:
        lines.extend([
            "No results matched the current filters.",
            "",
            "Rows inherit scenario/reporting metadata from `benchmarks/scenarios.yaml` and `benchmarks/function_matrix.yaml`.",
        ])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    grouped = _group_results(results, group_by)
    for group_key in sorted(grouped.keys()):
        heading = _format_group_heading(group_by, group_key)
        lines.extend([
            f"## {heading}",
            "",
            "| Scenario | Profile | Support | Claim | Engine | Mode | Status | Incremental (us) | Full eval (ms) | Correctness | Caveats |",
            "|---|---|---|---|---|---|---|---:|---:|---|---|",
        ])
        for row in sorted(grouped[group_key], key=_result_sort_key):
            metrics = row.get("metrics", {})
            correctness = row.get("correctness", {})
            caveats = row.get("caveats", [])
            lines.append(
                f"| {row.get('scenario', '-')} | {row.get('profile', '-')} | {row.get('support_policy', '-')} | "
                f"{row.get('claim_class', '-')} | {row.get('engine', '-')} | {row.get('mode', '-')} | {row.get('status', '-')} | "
                f"{_fmt_metric(metrics.get('incremental_us'))} | {_fmt_metric(metrics.get('full_eval_ms'))} | "
                f"{'pass' if correctness.get('passed') else 'fail'} | {', '.join(caveats) if caveats else '-'} |"
            )
        lines.append("")

    lines.append("Rows inherit scenario/reporting metadata from `benchmarks/scenarios.yaml` and `benchmarks/function_matrix.yaml`.")

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _group_results(results: list[dict[str, Any]], group_by: list[str]) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    if not group_by:
        grouped[("All results",)] = list(results)
        return grouped
    for row in results:
        key = tuple(_group_value(row, field) for field in group_by)
        grouped[key].append(row)
    return grouped


def _group_value(row: dict[str, Any], field: str) -> str:
    if field == "comparison_profile":
        profiles = row.get("comparison_profiles", [])
        return ",".join(profiles) if profiles else "-"
    value = row.get(field)
    if isinstance(value, list):
        return ",".join(str(v) for v in value) if value else "-"
    if value in (None, ""):
        return "-"
    return str(value)


def _format_group_heading(group_by: list[str], group_key: tuple[str, ...]) -> str:
    if not group_by:
        return "All results"
    parts = []
    for field, value in zip(group_by, group_key, strict=False):
        label = GROUP_FIELD_NAMES.get(field, field)
        parts.append(f"{label}={value}")
    return " / ".join(parts)


def _result_sort_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("scenario", "")),
        str(row.get("engine", "")),
        str(row.get("mode", "")),
    )


def _combined_caveats(row: dict[str, Any]) -> list[str]:
    caveats = set(str(label) for label in row.get("declared_caveats", []))
    status = str(row.get("status", "")).lower()
    correctness = row.get("correctness", {}) or {}
    notes = row.get("notes", []) or []

    if status == "not_implemented":
        caveats.add("unsupported_path")
    elif status and status not in {"ok", "success", "passed"}:
        caveats.add("execution_failure")

    if not bool(correctness.get("passed", False)):
        caveats.add("correctness_failure")

    for note in notes:
        note_text = str(note).lower()
        if "fallback" in note_text:
            caveats.add("fallback_path")
        if "not implemented" in note_text or "unsupported" in note_text:
            caveats.add("unsupported_path")

    return sorted(caveats)


def _fmt_metric(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)
