# FP1 baseline report

Date: 2026-04-29  
Branch: `formula-plane/bridge`  
Raw artifact directory: `target/fp1-baseline/b01e3e7`  
Baseline hook commit: `78d14c7` (`feat(formula-plane): add fp1 baseline stats hooks`)

## Scope

This is a bounded FP1 baseline for FormulaPlane/span work. It uses the governed synthetic scenarios requested for FP1 and records only read-only, accuracy-preserving counters. It does not implement FormulaPlane scheduling or template execution.

The governed runner data is **Umya-only**. Backend comparison is limited to `probe-load-envelope-matrix --preset smoke` for Umya and Calamine.

## Commands run

Validation:

```bash
RUN_DIR=target/fp1-baseline/$(git rev-parse --short HEAD)
mkdir -p "$RUN_DIR"
timeout 60s uv run --project benchmarks/harness python benchmarks/harness/runner/main.py validate-suite | tee "$RUN_DIR/validate-suite.log"
timeout 60s uv run --project benchmarks/harness python benchmarks/harness/runner/main.py validate-plans | tee "$RUN_DIR/validate-plans.log"
```

Corpus generation:

```bash
timeout 20m cargo run -p formualizer-bench-core --features xlsx --bin generate-corpus -- \
  --scenarios benchmarks/scenarios.yaml \
  --only headline_100k_single_edit \
  --only chain_100k \
  --only fanout_100k \
  --only inc_cross_sheet_mesh_3x25k \
  --only agg_countifs_multi_criteria_100k \
  --only agg_mixed_rollup_grid_2k_reports \
  > "$RUN_DIR/generate-corpus.stdout.log" \
  2> "$RUN_DIR/generate-corpus.stderr.log"
```

Build/timing:

```bash
timeout 10m cargo build --release -p formualizer-bench-core --features formualizer_runner --bin run-formualizer-native

for s in headline_100k_single_edit chain_100k fanout_100k inc_cross_sheet_mesh_3x25k agg_countifs_multi_criteria_100k agg_mixed_rollup_grid_2k_reports; do
  /usr/bin/time -v timeout 15m env FZ_DEBUG_LOAD=1 target/release/run-formualizer-native \
    --scenarios benchmarks/scenarios.yaml \
    --scenario "$s" \
    --root . \
    --mode native_best \
    > "$RUN_DIR/$s.umya.native_best.json" \
    2> "$RUN_DIR/$s.umya.native_best.stderr.log"
done

for s in chain_100k fanout_100k; do
  /usr/bin/time -v timeout 15m env FZ_DEBUG_LOAD=1 target/release/run-formualizer-native \
    --scenarios benchmarks/scenarios.yaml \
    --scenario "$s" \
    --root . \
    --mode native_best_cached_plan \
    > "$RUN_DIR/$s.umya.native_best_cached_plan.json" \
    2> "$RUN_DIR/$s.umya.native_best_cached_plan.stderr.log"
done
```

Backend smoke probe:

```bash
for b in umya calamine; do
  timeout 20m cargo run --release -p formualizer-bench-core --features formualizer_runner --bin probe-load-envelope-matrix -- \
    --preset smoke \
    --backend "$b" \
    --timeout-seconds 60 \
    --logical-cell-budget 256000000 \
    --debug-load \
    --output-dir "$RUN_DIR/load-envelope-$b" \
    --json-out "$RUN_DIR/load-envelope-$b.json" \
    --markdown-out "$RUN_DIR/load-envelope-$b.md" \
    > "$RUN_DIR/load-envelope-$b.stdout.md" \
    2> "$RUN_DIR/load-envelope-$b.stderr.log"
done
```

Sidecar formula scan:

- Script artifact: `target/fp1-baseline/b01e3e7/fp1_formula_scan.py`
- Output: `target/fp1-baseline/b01e3e7/formula-scan.json`
- Nature: passive OOXML scan; formula normalization is heuristic and does not parse Excel grammar.

## Governed Umya baseline

`load_ms`, `full_eval_ms`, and `incremental_ms` are runner wall-clock metrics. RSS is parsed from `/usr/bin/time -v` stderr. Formula/run/shared-formula metrics come from the sidecar OOXML scan unless explicitly named as graph/AST metrics.

| Scenario | Load ms | Full eval ms | Incremental ms | RSS MB | Formula cells | AST roots | AST nodes | Graph formula vertices | Graph edges | Pending eval vertices | Incremental computed | Repeated templates | Repeated-template cells | Col runs | Row runs | Holes | Shared `<f t=shared>` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `agg_countifs_multi_criteria_100k` | 2180.2 | 436.2 | 9.454 | 323.6 | 1000 | 1000 | 7006 | 1000 | 5000 | 1 | 1 | 1 | 1000 | 1 | 0 | 0 | 0 |
| `agg_mixed_rollup_grid_2k_reports` | 327.8 | 376.0 | 1.313 | 75.9 | 12000 | 12000 | 34006 | 12000 | 28000 | 4 | 4 | 5 | 12000 | 5 | 0 | 0 | 0 |
| `chain_100k` | 1010.1 | 78.3 | 60.443 | 207.6 | 99999 | 99999 | 299997 | 99999 | 99999 | 99999 | 99999 | 1 | 99999 | 1 | 0 | 0 | 0 |
| `fanout_100k` | 966.0 | 66.3 | 39.231 | 188.4 | 100000 | 100000 | 200001 | 100000 | 100000 | 100000 | 100000 | 0 | 0 | 0 | 0 | 0 | 0 |
| `headline_100k_single_edit` | 1559.0 | 105.2 | 24.394 | 253.9 | 100001 | 100001 | 300002 | 100001 | 100000 | 2 | 2 | 1 | 100000 | 1 | 0 | 0 | 0 |
| `inc_cross_sheet_mesh_3x25k` | 807.0 | 30.7 | 0.024 | 143.3 | 50000 | 50000 | 150000 | 50000 | 100000 | 2 | 2 | 2 | 50000 | 2 | 0 | 0 | 0 |

### Cached-plan comparison

Only stable-topology `chain_100k` and `fanout_100k` were measured in `native_best_cached_plan` mode.

| Scenario | Load ms | Full eval ms | Incremental ms | RSS MB | Pending eval vertices | Incremental computed | Plan builds | Plan reuses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `chain_100k` | 998.3 | 78.4 | 62.008 | 211.3 | 99999 | 99999 | 1 | 1 |
| `fanout_100k` | 950.3 | 60.0 | 38.743 | 189.4 | 100000 | 100000 | 1 | 1 |

## Backend smoke probe

Smoke-only matrix from `probe-load-envelope-matrix --preset smoke`; this probe is not the governed scenario runner and does not measure incremental recalc.

| Backend | Scenario | Shape | Logical cells | Status | Gen ms | Load ms | Eval ms | Load <=60s | Eval <=60s |
|---|---|---:|---:|---|---:|---:|---:|---|---|
| umya | linear_rollup | 10000x20 | 200000 | ok | 115.2 | 271.3 | 19.0 | yes | yes |
| umya | sumifs_report | 10000x20 | 200000 | ok | 178.1 | 277.5 | 38.6 | yes | yes |
| umya | whole_column_summary | 10000x20 | 200000 | ok | 52.6 | 75.3 | 1.5 | yes | yes |
| calamine | linear_rollup | 10000x20 | 200000 | ok | 119.7 | 272.7 | 20.0 | yes | yes |
| calamine | sumifs_report | 10000x20 | 200000 | ok | 178.1 | 163.8 | 40.3 | yes | yes |
| calamine | whole_column_summary | 10000x20 | 200000 | ok | 54.1 | 42.6 | 2.1 | yes | yes |

## Interpretation notes

- `load_ms` is combined open/read + workbook ingest/build time. A separated ingest/build metric is still unavailable.
- The governed runner uses `UmyaAdapter`. Calamine appears only in the smoke backend probe.
- Runner JSON still reports `metrics.peak_rss_mb = null`; RSS values above are from `/usr/bin/time -v` stderr.
- Default `native_best` incremental op currently calls `evaluate_all`, so `incremental_ms` is a correctness-preserving benchmark operation timing, not necessarily a minimal dirty-frontier scheduler timing.
- `graph_edge_count` is a read-only exact count of logical outgoing graph edges in `CsrMutableEdges`; it is not a separate dependency-row or compressed range-row count.
- Template/run/shared-formula metrics are from a passive OOXML sidecar scan and are suitable as FP1 comparison signals, not as stable production FormulaPlane template IDs.
- Raw generated OOXML corpus had no `<f t="shared" ...>` tags in these six scenarios.

## Explicit gaps

| Gap | Impact on FP2 comparison | Recommended follow-up |
|---|---|---|
| Load phase not split into open/read vs engine ingest/build | Cannot attribute FormulaPlane ingest wins separately from adapter IO/parse changes | FP1.B: add read-only phase timing split in runner/workbook loader |
| Calamine governed runner absent | Backend comparison lacks incremental/correctness scenario parity | FP1.B: add governed `run-formualizer-native --backend calamine` or equivalent runner |
| Template/run scan is heuristic sidecar | Future parser/template changes need stable canonical IDs | FP1.B: add parser-backed bench-only formula template scanner |
| No adapter/materialization counters | Cannot quantify formula/value materialization avoidance | FP1.B: reader metadata counters for formula/value/shared-formula handoff |
| Graph edge count is not dependency-row taxonomy | Cannot distinguish scalar cell deps, range deps, stripes, names, virtual deps from one count | FP2: add dependency taxonomy counters when span/partition representation lands |
| Shared formulas only raw OOXML visibility | Cannot measure whether adapters preserve or expand shared formulas | FP1.B/FP2: adapter shared-formula visibility and expansion counters |

## Status

**PARTIAL PASS.** FP1 now has bounded timing, RSS, correctness, formula, AST, graph, dirty/evaluation, passive template/run, and raw shared-formula visibility for the requested synthetic scenarios. Remaining gaps are explicit and should be addressed by FP1.B before using this as the only comparison point for production FormulaPlane scheduling changes.
