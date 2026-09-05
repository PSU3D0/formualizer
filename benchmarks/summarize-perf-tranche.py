#!/usr/bin/env python3
"""Summarize standalone probe logs: uv run python3 benchmarks/summarize-perf-tranche.py LOG..."""
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

FIELDS = (
    "chunks", "p", "text", "formulas", "threads", "mode", "calls", "axis_edit",
    "whole_column", "active_spans", "execution",
)
METRICS = (
    "ns", "edit_ns", "recalc_ns", "allocs", "allocated_bytes",
    "allocs_main_thread", "allocated_bytes_main_thread", "lookups",
    "mask_calls", "mask_logical_rows", "builds", "hits", "misses",
    "skipped_cap", "skipped_below_threshold", "bytes_in_cache", "entries_count",
)
# First collapse lookup cycles within each run/sample/position to medians;
# the resulting distribution does not treat correlated cycles as independent samples.
groups = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
for filename in sys.argv[1:]:
    path = Path(filename)
    variant = "baseline" if "baseline" in path.name else "fixed"
    for line in path.read_text().splitlines():
        line = re.sub(r"^test .*? \.\.\. ", "", line)
        if not line.startswith(("criteria", "registry", "lookup")):
            continue
        event = line.split()[0]
        fields = dict(re.findall(r"(\w+)=(\S+)", line))
        fields.update(re.findall(r"(\w+): (\d+)", line))
        params = [f"{key}={fields[key]}" for key in FIELDS if key in fields]
        if "cycle" in fields:
            cycle = int(fields["cycle"])
            phase = "initial" if cycle == 0 else "early" if cycle < 4 else "late"
            position = "first" if fields["axis_edit"] == "true" and cycle % 2 else "last"
            params.extend((f"phase={phase}", f"position={position}"))
        elif event in ("criteria", "criteria_floor", "registry_engine"):
            params.append("phase=cold" if fields["sample"] == "0" else "phase=warm")
        sample = (filename, fields.get("sample", "setup"))
        group = groups[(variant, event, " ".join(params))]
        for metric in METRICS:
            if metric in fields:
                group[metric][sample].append(int(fields[metric]))

output = []
for (variant, event, params), metrics in sorted(groups.items()):
    summary = {}
    for metric, samples in metrics.items():
        values = sorted(statistics.median(v) for v in samples.values())
        if len(values) > 1:
            p25, _, p75 = statistics.quantiles(values, n=4, method="inclusive")
        else:
            p25 = p75 = values[0]
        summary[metric] = dict(n=len(values), median=statistics.median(values),
                               p25=p25, p75=p75, min=values[0], max=values[-1])
    output.append(dict(variant=variant, event=event, params=params, metrics=summary))
json.dump(output, sys.stdout, indent=2)
print()
