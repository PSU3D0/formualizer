#!/usr/bin/env python3
"""Print TSV summaries of local range probe captures; never run a benchmark."""
import argparse
import collections
import json
from pathlib import Path
import re
import statistics


def quantile(values, fraction):
    values = sorted(values)
    position = (len(values) - 1) * fraction
    lo = int(position)
    hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (position - lo)


def load(paths):
    groups = collections.defaultdict(list)
    for path in paths:
        match = re.fullmatch(r"(instrumented|production)-(baseline|a|ab|fixed)-(\d+)\.txt", path.name)
        if match is None:
            raise ValueError("unexpected capture filename: " + path.name)
        kind, variant, _run = match.groups()
        count = 0
        for line in path.read_text().splitlines():
            brace = line.find("{")
            if brace < 0:
                continue
            # Libtest can prefix the first complete JSON record with the test name.
            record = json.loads(line[brace:])
            if record["family"] == "direct":
                case = record["case"] + "/" + record["mode"]
                phase = record["phase"]
            else:
                case = record["case"] + "/" + str(record["chunk_rows"])
                if kind == "instrumented":
                    case += "/index=" + str(record["index_enabled"])
                    phase = "cold" if record["sample"] == 0 else "warm-dirty"
                else:
                    phase = record["phase"]
            groups[(kind, variant, record["family"], case, phase)].append(record)
            count += 1
        if count == 0:
            raise ValueError("no JSON records: " + path.name)
    return groups


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", action="store_true", help="emit median instrumented work per iteration")
    parser.add_argument("captures", nargs="+", type=Path)
    args = parser.parse_args()
    groups = load(args.captures)
    keys = ["kind", "variant", "family", "case", "phase", "samples"]
    work_fields = ["candidates", "search_probes", "segments", "segment_rows",
                   "generic_columns", "selector_searches", "null_arrays"]
    fields = (["allocations", "allocated_bytes"] + work_fields if args.work
              else ["median_us", "p25_us", "p75_us", "p90_us"])
    print("\t".join(keys + fields))
    for key, records in sorted(groups.items()):
        if args.work:
            if key[0] != "instrumented":
                continue
            values = []
            for field in fields:
                values.append(statistics.median([
                    (r[field] if field in r else r["work"][field]) / r.get("iterations", 1)
                    for r in records
                ]))
        else:
            timings = [r["ns"] / r.get("iterations", 1) / 1000 for r in records]
            values = [statistics.median(timings)] + [quantile(timings, q) for q in (0.25, 0.75, 0.9)]
        print("\t".join(list(key) + [str(len(records))] + [format(v, ".6f") for v in values]))


if __name__ == "__main__":
    main()
