# Benchmarks

This directory contains benchmark suite inputs, governance metadata, and harness docs.

## Layout

- `scenarios.yaml` — canonical scenario definitions plus family/tier/profile/runtime metadata
- `function_matrix.yaml` — scenario function/features plus support policy, claim class, and caveat labels
- `reporting.md` — reporting contract, claim matrix, nightly/runtime-parity policy, and regression-gate shortlist
- `corpus/` — generated and curated `.xlsx` benchmark artifacts
- `expected/` — expected outputs for verification checks
- `harness/` — runner/adapters documentation and implementation notes

## Generate synthetic corpus

From repository root:

```bash
cargo run -p formualizer-bench-core --features xlsx --bin generate-corpus -- \
  --scenarios benchmarks/scenarios.yaml
```

Optional filters:

```bash
cargo run -p formualizer-bench-core --features xlsx --bin generate-corpus -- \
  --scenarios benchmarks/scenarios.yaml \
  --only headline_100k_single_edit --only chain_100k
```

## Governance highlights

- Scenario families are normalized into `incremental_locality`, `chain_topology`, `lookup_dimension_join`, `aggregate_analytics`, `structural_edit`, `real_world_anchor`, and `nightly_stress`.
- Tiering distinguishes `pr_smoke`, `comparative`, and `nightly_heavy` workloads so routine reports do not silently absorb heavy/stress scenarios.
- Support policy and claim safety live alongside the function matrix so public comparison tables can distinguish all-engine rows from profile-subset or caveated rows.
- Runtime-parity reporting is intentionally opt-in per scenario; nightly-scale and native-strength scenarios stay out of parity views unless reclassified later.

## Large-workbook envelope probing

For opt-in large-sheet load/evaluate exploration (outside normal PR CI), use the dedicated Rust probe binary:

```bash
cargo run --release -p formualizer-bench-core --features formualizer_runner --bin probe-load-envelope -- \
  --scenario linear-rollup \
  --rows 700000 \
  --logical-cols 20 \
  --logical-cell-budget 256000000
```

To run the curated matrix with a hard per-case subprocess timeout, use the Rust matrix runner:

```bash
cargo run --release -p formualizer-bench-core --features formualizer_runner --bin probe-load-envelope-matrix -- \
  --preset targeted \
  --timeout-seconds 60 \
  --logical-cell-budget 256000000
```

Use `--preset envelope` for the broader sweep once the targeted set has been characterized.

For fine-grained hot-path timing (compiled out of normal release builds), add the perf feature:

```bash
cargo run --release -p formualizer-bench-core --features formualizer_runner,perf_instrumentation --bin probe-load-envelope-matrix -- \
  --preset iteration \
  --debug-load \
  --perf-instrumentation
```

The probe generates realistic-ish large XLSX workbooks for three scenario families:
- `linear_rollup` — row-local formulas plus a wide/tall logical envelope
- `sumifs_report` — fact-table + report-sheet `SUMIFS` workload
- `whole_column_summary` — `A:A` / whole-column `SUMIFS` / `COUNTIF` workload

This tooling is intended to help choose sane default ingest limits and identify when load or full-workbook evaluation crosses the 60-second threshold.

## Notable scenarios

- `inc_sparse_dirty_region_1m` is the nightly-scale sparse-locality watchlist scenario.
- `inc_cross_sheet_mesh_3x25k` is the comparative/runtime-parity selective-propagation scenario.
- `agg_countifs_multi_criteria_100k` is the claim-safe aggregate/report regression scenario.
- `agg_mixed_rollup_grid_2k_reports` is the broader native-best report-grid aggregate scenario.
- `struct_row_insert_middle_50k_refs` and `struct_sheet_rename_rebind` anchor structural-edit reporting with explicit caveats.
- `real_finance_model_v1` is the real-world finance anchor used for scheduled realism checks.
- `real_ops_model_v1` adds a service-operations anchor with lookup-driven work orders and dashboard rollups.

## Design split

- Rust-native contract and corpus tooling live in `crates/formualizer-bench-core`.
- Reusable fixture generation helpers live in `crates/formualizer-testkit`.
- Polyglot comparative runners and reporting utilities live in `benchmarks/harness`.
