#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = []
# ///
"""Interleave prebuilt binaries; never build, mutate arms, or bless wrong answers.

uv run benchmarks/harness/scripts/error-guard-campaign.py CONFIG.json OUTDIR
See benchmarks/error-guards.md for the config contract and build instructions.
"""
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def schedule(arms, repeats):
    for repeat in range(repeats):
        order = arms[repeat % len(arms):] + arms[:repeat % len(arms)]
        if repeat % 2:
            order = order[::-1]
        for arm in order:
            yield repeat, arm


def main():
    config_path, output = map(Path, sys.argv[1:])
    cfg = json.loads(config_path.read_text())
    repeats = cfg.get("repeats", 3)
    if repeats < 3:
        raise ValueError("at least three repetitions required")
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    arms = cfg["arms"]
    if len({arm["id"] for arm in arms}) != len(arms):
        raise ValueError("duplicate arm ids")
    hashes = {}
    for arm in arms:
        if len(arm["commit"]) != 40 or not arm["build_notes"]:
            raise ValueError("full commit SHA and compiler/profile/features build_notes required")
        for kind in {case["kind"] for case in cfg["cases"]}:
            path = Path(arm[kind]).resolve(strict=True)
            hashes[str(path)] = sha(path)
            arm[kind] = str(path)
    inputs = {}
    for case in cfg["cases"]:
        if case["kind"] == "program1":
            path = Path(case["xlsx"]).resolve(strict=True)
            inputs[str(path)] = sha(path)
            case["xlsx"] = str(path)
        elif case["kind"] != "corpus":
            raise ValueError("case kind must be corpus or program1")
    provenance = {"config": cfg, "binary_sha256": hashes, "input_sha256": inputs,
                  "host": platform.uname()._asdict(), "started_ns": time.time_ns(),
                  "environment": {k: os.environ.get(k) for k in
                                  ["RAYON_NUM_THREADS", "OMP_NUM_THREADS", "RUSTFLAGS"]},
                  "script_sha256": sha(__file__)}
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    samples = []
    for repeat, arm in schedule(arms, repeats):
        for case in cfg["cases"]:
            run_dir = output / f"r{repeat}-{arm['id']}-{case['id']}"
            run_dir.mkdir()
            if case["kind"] == "corpus":
                cmd = [arm["corpus"], "--label", "campaign", "--include", case["include"],
                       "--scale", case.get("scale", "small"), "--modes", "off",
                       "--backend", "calamine", "--enable-parallel", "false",
                       "--output-dir", str(run_dir / "reports"),
                       "--phase-timeout-ms", str(case.get("phase_timeout_ms", 60000))]
            else:
                cmd = [arm["program1"], "--xlsx", case["xlsx"], "--mode", "interactive",
                       "--edits", str(case.get("edits", 10)), "--no-alloc-count"]
            for path, expected in (hashes | inputs).items():
                if sha(path) != expected:
                    raise RuntimeError(f"immutable artifact changed: {path}")
            started = time.time_ns()
            with (run_dir / "stdout.txt").open("w") as stdout, (run_dir / "stderr.txt").open("w") as stderr:
                try:
                    returncode = subprocess.run(cmd, stdout=stdout, stderr=stderr,
                                                timeout=case.get("timeout_seconds", 600), check=False).returncode
                except subprocess.TimeoutExpired:
                    returncode = 124
            record = {"repeat": repeat, "arm": arm["id"], "case": case["id"],
                      "command": cmd, "started_ns": started, "returncode": returncode,
                      "status": "FAILED_OR_WRONG_ANSWER" if returncode else "UNVERIFIED"}
            metrics = {}
            if case["kind"] == "corpus":
                reports = list((run_dir / "reports").glob("s*-off.json"))
                report_data = [json.loads(p.read_text()) for p in reports]
                record["fixture_sha256"] = {d["fixture_path"]: sha(d["fixture_path"])
                    for d in report_data if d.get("fixture_path") and Path(d["fixture_path"]).exists()}
                # probe-corpus also sets this flag for known/expected failures.
                # Such classification never establishes correct workbook answers.
                invalid_prefixes = ("invariant failure", "expected invariant failure", "expected_failure_reason:")
                valid = bool(reports) and all(
                    d["final_invariants_passed"]
                    and not any(note.startswith(invalid_prefixes) for note in d.get("notes", []))
                    for d in report_data
                )
                record["status"] = "CORRECT" if valid and returncode == 0 else "FAILED_OR_WRONG_ANSWER"
                csv_path = run_dir / "reports" / "summary.csv"
                if csv_path.exists():
                    with csv_path.open() as stream:
                        for row in csv.DictReader(stream):
                            phase = row["phase"]
                            if phase in ("phase_load", "phase_first_eval") or phase.startswith("phase_recalc_"):
                                phase = "phase_recalc" if phase.startswith("phase_recalc_") else phase
                                key = f"{row['scenario_id']}:{phase}"
                                metrics.setdefault(key, []).append(float(row["wall_ms"]))
            elif returncode == 0:
                data = json.loads((run_dir / "stdout.txt").read_text())
                expected = case.get("expected_digests")
                record["digests"] = {k: data[k] for k in ("digest_first", "digest_end")}
                ok = data["first_ok"] and not data["value_edit_errors"] and not data["formula_edit_errors"]
                record["status"] = ("CORRECT" if ok and expected == record["digests"] else
                                    "UNVERIFIED" if ok and expected is None else "FAILED_OR_WRONG_ANSWER")
                metrics = {k: [data[k]] for k in ("load_ms", "first_eval_ms", "value_recalc_p50_ms") if data[k] is not None}
            for key, values in metrics.items():
                samples.append({**record, "metric": key, "ms": statistics.median(values)})
            (run_dir / "run.json").write_text(json.dumps(record, indent=2) + "\n")
            print(f"r{repeat} {arm['id']} {case['id']}: {record['status']}", flush=True)
    for path, expected in (hashes | inputs).items():
        if sha(path) != expected:
            raise RuntimeError(f"immutable artifact changed: {path}")
    (output / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
    # No speedup ratios from UNVERIFIED/wrong-answer runs. Keep raw timings for
    # diagnostic use, but publish medians only if every repetition is correct.
    summary = []
    for arm in arms:
        for case in cfg["cases"]:
            group = [s for s in samples if s["arm"] == arm["id"] and s["case"] == case["id"]]
            for metric in sorted({s["metric"] for s in group}):
                rows = [s for s in group if s["metric"] == metric]
                eligible = len(rows) == repeats and all(s["status"] == "CORRECT" for s in rows)
                summary.append({"arm": arm["id"], "case": case["id"], "metric": metric,
                                "comparison_eligible": eligible,
                                "median_ms": statistics.median(s["ms"] for s in rows) if eligible else None})
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
