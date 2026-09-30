# Error-guard correctness and performance campaign

The permanent **ScenarioRegistry** workloads `s088-error-guards-clean`, `s089-error-guards-sparse` (1% zero divisors), and `s090-error-guards-dense` (50%) run through the existing `probe-corpus` lifecycle/timing harness and its unified testkit adapter. They are original fixtures, not reference-fork fixtures. `small` = 256 rows, `medium` = 100,000, `large` = 1,000,000. Start small; large is optional, not a PR gate.

Each fixture contains clean scalar IFERROR/IFNA families with an erroring dormant fallback, clean range passthrough, reciprocal arrays with scalar and positionally paired range fallbacks, IFNA over #N/A/#DIV/0 mixtures, direct error-range IFERROR, and four scalar reductions including `SUM(IFERROR(1/range,0))` and `SUM(IFNA(error_range,0))` (which must still propagate non-#N/A errors). Five deterministic edits recover an initially erroneous cell, change its denominator, introduce an error of the other kind, change a fallback member, and change a clean input. Clean workloads never introduce errors, so their timings remain comparable to correct released behavior.

The independent host-arithmetic oracle checks **all six output columns, every spill member, and every reduction**, after first calculation and all five recalculations. It checks corrected/edited members, not merely unaffected sentinels. IFNA intentionally leaves #DIV/0 intact: `NoErrorCells` would be an invalid oracle. Array shape is paired 1-column here; dormant fallback stays lazy. Numeric results use exact binary fractions, with the runner's existing numeric tolerance. Load/edit-before-recalc cached values are not claimed as fresh answers.

`benchmarks/scenarios.yaml` / `function_matrix.yaml` belong to the cross-engine YAML suite (`generate-corpus`, Python/Node adapters, fixed expected cell maps). This richer lifecycle corpus lives in `crates/formualizer-bench-core/src/scenarios/`, not that separate generator dispatcher; adding unknown YAML IDs would silently create unrunnable catalog entries. Use the Rust registry driver for these workloads. They are internal correctness/performance guards, not cross-engine claim-safe comparisons. Existing YAML real-workbook profiles can still be run separately through their adapters and expected maps.

## Build and quick correctness

Use the wrapper's absolute cargo helper (invoke via `bash` if not executable):

```bash
CARGO_HELPER=/home/psu3d0/Projects/psu3d0/coltec-codespaces/nexus/codespaces/formualizer/platform-dev/scratch/formualizer-forkfix/cargo-shared.sh
bash "$CARGO_HELPER" test -p formualizer-bench-core --features formualizer_runner --lib error_guard_generator_oracle
bash "$CARGO_HELPER" build --release --locked -p formualizer-bench-core --features formualizer_runner --bin probe-corpus --bin program1-perf
# Copy binaries from the helper's per-worktree target/release to a NEW immutable arm directory.
# Do not build arms concurrently with measurement or run binaries from a live build directory.
/path/to/immutable/D/probe-corpus --label quick-D --include 's088*,s089*,s090*' --scale small --modes off --backend calamine --enable-parallel false --output-dir /new/output/quick-D
uv run benchmarks/harness/scripts/test_error_guard_campaign.py
```

The generator/oracle unit test passes before the implementation exists. The executable lifecycle oracle is expected to fail on released main for sparse/dense scenarios: leaked errors are **wrong answers**, not improved performance. `probe-corpus` writes phase metrics and `final_invariants_passed` plus concrete cell mismatch notes, then returns nonzero on failure. Its phase timing excludes fixture generation and oracle checks. Use fresh output directories. Avoid `probe-unified-registry` as a campaign driver: it is a structural smoke tool, not a timed failure-gated CLI.

## Four immutable arms

Build identical optimized profiles, Rust toolchain, Cargo.lock, features and evaluator settings:

- A: released main `5d4354d3`.
- B: six fixes `970b5c62`.
- C: IFERROR/IFNA + spill admission alone atop A.
- D: six fixes + new work.

Apply **only this benchmark/harness commit** to each arm before compiling; A/B must not receive production fixes from C/D. Record both the semantic base commit and the final full source commit after harness application in build notes. Record `rustc -Vv`, feature list, release profile/RUSTFLAGS, target, Cargo.lock hash, dirty state, and scenario commit. The script hashes binaries and real XLSX inputs, snapshots the config/host/environment, and verifies artifacts before every invocation and after the campaign. It never builds or changes the binaries. Allocate enough disk for isolated builds, but do not require a new full rebuild between repetitions.

Create `campaign.json` with **absolute binary paths**, 40-character source commits, and build notes (replace placeholders):

```json
{
  "repeats": 3,
  "arms": [
    {"id":"A","commit":"FULL_A_SOURCE_COMMIT_AFTER_HARNESS_APPLIED","build_notes":"base=5d4354d3; rustc/profile/features/lock recorded separately","corpus":"/immutable/A/probe-corpus","program1":"/immutable/A/program1-perf"},
    {"id":"B","commit":"FULL_B_SOURCE_COMMIT_AFTER_HARNESS_APPLIED","build_notes":"base=970b5c62; identical build settings","corpus":"/immutable/B/probe-corpus","program1":"/immutable/B/program1-perf"},
    {"id":"C","commit":"FULL_C_SOURCE_COMMIT_AFTER_HARNESS_APPLIED","build_notes":"base=A + guards/spill; identical build settings","corpus":"/immutable/C/probe-corpus","program1":"/immutable/C/program1-perf"},
    {"id":"D","commit":"FULL_D_SOURCE_COMMIT_AFTER_HARNESS_APPLIED","build_notes":"combined; identical build settings","corpus":"/immutable/D/probe-corpus","program1":"/immutable/D/program1-perf"}
  ],
  "cases": [
    {"id":"guards-clean","kind":"corpus","include":"s088*"},
    {"id":"guards-sparse","kind":"corpus","include":"s089*"},
    {"id":"guards-dense","kind":"corpus","include":"s090*"},
    {"id":"existing","kind":"corpus","include":"s002*,s003*,s013*,s031*,s045*,s075*","timeout_seconds":1200},
    {"id":"finance","kind":"program1","xlsx":"/absolute/finance.xlsx","edits":10,"expected_digests":{"digest_first":"TRUSTED_INITIAL_DIGEST","digest_end":"TRUSTED_EDITED_DIGEST"}}
  ]
}
```

A smaller first campaign can omit finance and restrict `existing` to `s002*,s003*,s045*`. Once correctness passes, add a `guards-100k` corpus case with `include: "s088*,s089*,s090*"`, `scale: "medium"`, `timeout_seconds: 1800`, `phase_timeout_ms: 180000`; large/1m is optional. For large mixed-error cases use C/D only after small correctness passes; keep A/B to clean/baseline-correct cases to avoid generating millions of expected mismatch notes. Do not turn the first campaign into an eight-hour sweep.

```bash
uv run benchmarks/harness/scripts/error-guard-campaign.py /absolute/campaign.json /new/output/campaign-quick
# Real workbook timing: counting allocator disabled (automatically by campaign script).
/immutable/D/program1-perf --xlsx /absolute/finance.xlsx --mode interactive --edits 10 --no-alloc-count
# Separate process / separate evidence directory for allocation + RSS (not timing comparisons).
/usr/bin/time -v /immutable/D/program1-perf --xlsx /absolute/finance.xlsx --mode interactive --edits 10
```

`program1-perf` records first/end all-formula digests, edit errors, load/first/recalc timings and RSS. A digest is **not an independent oracle**; use independently established expected outputs (e.g. the YAML finance verify map or prior accepted workbook authority/correctness campaign) to approve and pin first AND post-edit digests for this exact deterministic edit schedule. Equality across arms alone does not prove correctness. If `expected_digests` is omitted, the script records `UNVERIFIED`, never comparison-eligible. Changing edits invalidates the post-edit pin. Neither driver supplies independent validation of arbitrary real XLSX cached answers.

Each repetition interleaves arm order; raw stdout/stderr, exact commands, return codes and reports are retained. `samples.json` retains diagnostic timings even for wrong answers; `summary.json` publishes medians only when **all repetitions are correct**. Corpus mismatch details stay in the JSON phase reports. Compare load, first evaluation and median edit/recalc separately, using same case/mode/scale/settings. A/B sparse/dense are expected to be wrong, so only C/D are performance-comparable there. A/B/C/D clean guards and correct existing cases are comparable. Allocation/RSS must be investigated separately: counting instrumentation serializes allocations, and full-cell oracle allocations are outside the timed phase but can inflate process high-water RSS.

Shared-host numbers are indicative, not release-quality claims. Investigate a sustained >10% regression on correct baseline workloads with more interleaved repetitions, isolated processes and relevant profiling; do not average away workload-specific regressions. Keep wrong-answer and unverified runs visible rather than labeling them speedups.
