#!/usr/bin/env python3
"""Validate and summarize four recalc_reuse stdout logs; never run a benchmark.

Usage: uv run --no-project python3 benchmarks/summarize-perf-recalc.py run-*.txt
Names must be run-1-baseline.txt, run-2-fixed.txt, run-3-fixed.txt,
and run-4-baseline.txt. Each contains seven samples per scenario/depth.
"""

import argparse
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

SCENARIOS = (
    "cold", "same", "alternating", "late-target", "late-plan", "clean-target", "noop",
)
CASES = tuple((scenario, depth) for depth in (1, 32, 256) for scenario in SCENARIOS)
SIDES = {1: "baseline", 2: "fixed", 3: "fixed", 4: "baseline"}
SAMPLES = 7
ITERATIONS = 1024
FILENAME = re.compile(r"run-([1-4])-(baseline|fixed)\.txt")
ROW = re.compile(
    r"scenario=([\w-]+) depth=(\d+) repeats=(\d+) computed=(\d+) "
    r"result=Some\(Number\(([-+\d.eE]+)\)\) edit_eval_ns=Some\((\d+)\)"
)


def validate_row(line):
    match = ROW.fullmatch(line)
    if match is None:
        raise ValueError("expected one counter-free timed recalc_reuse output line")
    scenario, depth, count, computed, value, elapsed = match.groups()
    depth, count, computed, elapsed = map(int, (depth, count, computed, elapsed))
    value = float(value)
    if (scenario, depth) not in CASES:
        raise ValueError("scenario/depth is outside the documented matrix")
    expected_count = 1 if scenario == "cold" else ITERATIONS
    if scenario in ("noop", "clean-target"):
        expected_computed = 0
    elif scenario in ("late-target", "late-plan"):
        expected_computed = 2 * expected_count
    else:
        expected_computed = depth * expected_count
    expected_value = depth + 1
    if scenario == "same":
        expected_value = depth + 2 + (expected_count - 1) % 2
    elif scenario == "alternating":
        # The captured value is the first branch's terminal, not both branches.
        last_first_branch_edit = (expected_count - 1) // 2 * 2
        expected_value = depth + 2 + (last_first_branch_edit // 2) % 2
    elif scenario in ("late-target", "late-plan"):
        expected_value = depth + 4 + (expected_count - 1) % 2
    elif scenario == "clean-target":
        expected_value = depth + 12
    if (
        count != expected_count
        or computed != expected_computed
        or not math.isfinite(value)
        or value != expected_value
        or elapsed <= 0
    ):
        raise ValueError("iteration/computed/value/elapsed control failed")
    return (scenario, depth), count, computed, elapsed / count / 1000


def distribution(values):
    q1, _, q3 = statistics.quantiles(values, n=4, method="inclusive")
    return dict(
        n=len(values), median_us=statistics.median(values), q1_us=q1, q3_us=q3,
        min_us=min(values), max_us=max(values),
    )


def summarize(paths):
    runs = {}
    requests = computed_total = 0
    for path in paths:
        match = FILENAME.fullmatch(path.name)
        if match is None:
            raise ValueError(f"{path.name}: invalid run filename")
        run, side = int(match[1]), match[2]
        if run in runs or SIDES[run] != side:
            raise ValueError(f"{path.name}: duplicate run or wrong baseline/fixed order")
        cases = defaultdict(list)
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if not line.strip():
                continue
            try:
                key, count, computed, elapsed_us = validate_row(line.strip())
            except ValueError as error:
                raise ValueError(f"{path.name}:{lineno}: {error}") from error
            cases[key].append(elapsed_us)
            requests += count
            computed_total += computed
        if set(cases) != set(CASES) or any(len(v) != SAMPLES for v in cases.values()):
            raise ValueError(f"{path.name}: expected seven samples for each of 21 cases")
        runs[run] = cases
    if set(runs) != set(SIDES):
        raise ValueError("expected all four baseline/fixed/fixed/baseline runs")

    results = []
    for scenario, depth in CASES:
        case = (scenario, depth)
        blocks = {str(run): distribution(runs[run][case]) for run in SIDES}
        baseline = (blocks["1"]["median_us"] + blocks["4"]["median_us"]) / 2
        fixed = (blocks["2"]["median_us"] + blocks["3"]["median_us"]) / 2
        results.append(dict(
            scenario=scenario, depth=depth,
            iterations_per_sample=1 if scenario == "cold" else ITERATIONS,
            runs=blocks,
            baseline=distribution(runs[1][case] + runs[4][case]),
            fixed=distribution(runs[2][case] + runs[3][case]),
            balanced_baseline_us=baseline, balanced_fixed_us=fixed,
            balanced_change_pct=100 * (fixed / baseline - 1),
            first_pair_change_pct=100 * (blocks["2"]["median_us"] / blocks["1"]["median_us"] - 1),
            second_pair_change_pct=100 * (blocks["3"]["median_us"] / blocks["4"]["median_us"] - 1),
        ))
    return dict(
        method="Average each side's two seven-sample block medians; report F/B-1. "
               "Each sample is a process-level batch mean, not 1024 independent latencies.",
        completed_samples=len(CASES) * SAMPLES * len(SIDES),
        actual_requests=requests, computed_vertices=computed_total,
        control_checks_passed=True, results=results,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs=4, type=Path)
    args = parser.parse_args()
    try:
        result = summarize(args.logs)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    json.dump(result, sys.stdout, indent=2, allow_nan=False)
    print()


if __name__ == "__main__":
    main()
