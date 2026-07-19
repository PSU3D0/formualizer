# C6 Target-Locality Calibration

Status: focused Phase A harness; measurement only

This harness compares four native public evaluation paths in fresh processes:

- `full`: interactive/deferred XLSX load, explicit `prepare_graph_all`, `evaluate_all`, and direct output reads.
- `cells`: ordinary `evaluate_cells`; its public call necessarily combines target preparation, ephemeral target-plan construction, evaluation, and returned outputs.
- `plan`: a revision-bound target plan built once with `build_recalc_plan_for_targets`, followed by `evaluate_with_plan` and direct output reads.
- `sheetport`: a parsed and validated FIO manifest bound with `SheetPort::new`; one-shot output is read by `evaluate_once`, then warm edits use a SheetPort batch and its reusable target plan.

The child probe and matrix runner require the `c6_calibration` feature. Fixtures are generated before timed child samples. Every sample loads the same immutable XLSX in a new process using `WorkbookConfig::interactive()`, so graph preparation remains deferred and request-time locality is visible.

## Fixture

The deterministic fixture contains exactly the requested formula count split across independent finance-shaped chains:

- `Tiny`: 0.5% of formulas, providing the less-than-1% target.
- `Medium`: 10% of formulas.
- `Large`: the remaining primary formulas.
- `Dirty`: eight formulas used to establish an unrelated prepared and dirty branch.

A target is the terminal balance in a branch. The 100% scope requests every terminal balance. Before the measured path, every child prepares and evaluates `Dirty`, edits its input, and reports that common setup separately. Tiny and medium runs therefore begin with both unrelated staged formulas and unrelated dirty prepared work. Target-local runs must retain both. The prepared-formula locality gate subtracts the eight-formula common setup and allows at most the reachable oracle plus 1%.

Repeated value edits change `A1` inside the selected closure. These rows report public API cost after that edit, not a claim that the full requested scope is dirty or recomputed. In particular, `full_100pct` always edits the fixed 0.5% `Tiny` branch while requesting all four terminals. Direct plan runs reuse the same revision-bound plan. SheetPort repeated runs go through `BatchExecutor`; its progress clock is reset immediately before `batch.run`, each edit/evaluate/output iteration is separated, and checked nonnegative baseline-restoration accounting is reported independently.

## Commands

Build and run the focused tests:

```bash
cargo test -p formualizer-bench-core --features c6_calibration \
  --lib \
  --bin probe-c6-target-locality \
  --bin probe-c6-target-locality-matrix
cargo check -p formualizer-bench-core --features c6_calibration --bins
cargo clippy -p formualizer-bench-core --features c6_calibration --bins --tests -- -D warnings
```

Run a one-sample native smoke matrix:

```bash
cargo run --release -p formualizer-bench-core --features c6_calibration \
  --bin probe-c6-target-locality-matrix -- \
  --formulas 1000 --samples 1 --warm-repeats 1 --timeout-seconds 60 \
  --output-dir target/c6-calibration/smoke
```

Run the required randomized seven-sample 50k matrix. The runner builds the release child once; use `--skip-build` only when that exact child is already built:

```bash
cargo run --release -p formualizer-bench-core --features c6_calibration \
  --bin probe-c6-target-locality-matrix -- \
  --formulas 50000 --samples 7 --warm-repeats 3 --timeout-seconds 600 \
  --output-dir target/c6-calibration/native-50k
```

The child can also generate and inspect one case directly:

```bash
cargo run --release -p formualizer-bench-core --features c6_calibration \
  --bin probe-c6-target-locality -- generate \
  --fixture target/c6-calibration/manual-50000.xlsx --formulas 50000
cargo run --release -p formualizer-bench-core --features c6_calibration \
  --bin probe-c6-target-locality -- sample \
  --fixture target/c6-calibration/manual-50000.xlsx --formulas 50000 \
  --path plan --scope tiny --warm-repeats 3
```

## Outputs and interpretation

The output directory contains:

- `matrix-raw.json`: identity, randomized job order, per-child raw phase data, typed output/error fields, graph snapshots, request telemetry, RSS/HWM, and `/usr/bin/time -v` maximum RSS where available.
- `matrix-summary.md`: median, nearest-rank p95, median absolute deviation, and maximum over successful samples.
- the immutable fixture and its SHA-256 plus stderr and external-time logs.

Machine-specific output stays under `target/c6-calibration/` and is not committed. Sample summaries include only `status: ok` children. The runner drains child stdout and stderr concurrently; on timeout it kills the complete child process group, including the `/usr/bin/time` wrapper and probe. Runs at 50,000 formulas or above enforce at least a 600-second per-child timeout.

A summary timing cell is `median / p95 / MAD / max`. Percentiles use nearest rank; with seven values a per-child p95 equals the maximum, while the pooled repeated-API cells contain 21 observations. Every child is process-cold, but fixture reads are normally OS-page-cache-warm. Cold total includes XLSX open/load, the common dirty-branch setup, binding/target resolution, first evaluation, and preparation/plan build plus output read when those precede the first evaluation. SheetPort cold total is its one-shot path; the subsequent reusable batch-plan build is reported separately in the `prepare/plan` column and raw phases rather than double-counted into one-shot cold latency. The raw `includes` list is authoritative when a public API combines phases. In particular, `cells` cannot split preparation from evaluation/output, and SheetPort one-shot and batch calls cannot split all selector, edit, evaluation, restoration, and output-read subphases.

Correctness does not rely only on agreement between APIs. For the finance recurrence, the harness computes every requested terminal after the initial seeds and every repeated edit from the closed form `B_n = B_1*r^(n-1) + A1*0.00001*(r^(n-1)-1)/(r-1)`, independently of the engine, and fails a sample on mismatch. It also asserts exact deferred, staged, prepared, and dirty counts for this deterministic fixture. Path shape is supported by the preparation scope/widening bits, requested/normalized targets, selected/retained staging, graph formula/edge deltas, target commit work, charged work, topology strategy/candidates/edges, materialization counts, dirty-lease outcome, and retained/scratch accounting. Formula count and target commit work are different units and are reported separately; the locality tolerance gate uses prepared formula deltas and selected staged formula counts only.

SheetPort records three non-overwriting telemetry stages: `first_evaluation` immediately after one-shot evaluation, `sheetport_batch_plan_build` immediately after batch construction, and `sheetport_batch_execution` immediately after `batch.run` completes restoration. The summary identifies the latter two snapshots explicitly.

## Current limitations

This focused first deliverable covers deterministic independent scalar finance branches with Calamine native loading and the current default FormulaPlane mode. The SheetPort sample deliberately records both public modes in sequence: one-shot first, then batch creation/reuse on the already prepared workbook. Its separately reported batch-plan build is therefore not a second cold-path latency and is not directly setup-equivalent to the direct plan build on deferred staging. It does not yet add the remaining Phase A cross-sheet, name, bounded-layout, native-table, or dynamic/opaque widening fixture families. It also does not run 250k/largest-safe tiers, historical pre-C5 binaries, the broader native mode/budget matrix, WASM/no-disk, or recommend defaults. No algorithm, semantic behavior, cap, or default changes are part of this harness.

The engine does not expose a stable allocator-specific counter. The harness therefore records process RSS/HWM, external maximum RSS, and existing request-ledger retained/scratch values. Failed child processes are preserved as typed matrix statuses with stderr; successful engine/SheetPort errors have a raw `typed_error` slot for parity, but the initial scalar fixture is expected to succeed and is not an error-injection suite.
