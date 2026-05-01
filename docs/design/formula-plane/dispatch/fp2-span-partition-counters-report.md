# FP2.A span partition counters report

Date: 2026-04-29  
Branch: `formula-plane/bridge`  
Raw artifact directory: `target/fp2-span-counters/75caa35`  
Implementation commit: `75caa35` (`feat(formula-plane): add passive span partition counters`)

## Scope

FP2.A adds passive, read-only FormulaPlane candidate counters behind the FP1.B scanner path. It does not implement a scheduler, does not create runtime authority, and does not change public/default workbook behavior, production formula parsing, dependency graph construction, or formula evaluation semantics.

The scanner still reads XLSX OOXML formulas and parser-backed template IDs as in FP1.B. The new `formula_plane_candidates` JSON section models candidate dependent formula placements only:

```text
precedent region -> dependent formula placement -> result region
```

FP2.A observes the dependent formula placement span/partition shape. It does not infer authoritative precedent regions, result regions, dirty propagation, or scheduler partitions.

## Counter contract

- FormulaPlane bridge counter primitives live in `crates/formualizer-eval/src/formula_plane/span_counters.rs` and are consumed by the bench-only `scan-formula-templates` binary.
- Candidate partitioning uses a fixed diagnostic row block size of 4096 rows (`row_block_size: 4096`).
- `candidate_row_block_partition_count` counts distinct `(sheet, row_block)` buckets touched by candidate formula runs.
- `candidate_formula_run_to_partition_edge_estimate` sums candidate run -> row-block touches; it is not a real dependency graph edge count.
- `estimated_materialization_avoidable_cell_count` is a rough estimate equal to formula cells represented by repeated candidate runs.
- `dense_run_coverage_percent` is `formula_cells_represented_by_runs / formula_cell_count * 100`.

## Commands run

Build and scanner baseline:

```bash
RUN_DIR=target/fp2-span-counters/$(git rev-parse --short HEAD)
mkdir -p "$RUN_DIR"
timeout 10m cargo build --release -p formualizer-bench-core --features formualizer_runner --bin scan-formula-templates --bin run-formualizer-native \
  > "$RUN_DIR/build-release.stdout.log" \
  2> "$RUN_DIR/build-release.stderr.log"

for s in headline_100k_single_edit chain_100k fanout_100k inc_cross_sheet_mesh_3x25k agg_countifs_multi_criteria_100k agg_mixed_rollup_grid_2k_reports; do
  timeout 2m target/release/scan-formula-templates \
    --scenarios benchmarks/scenarios.yaml \
    --scenario "$s" \
    --root . \
    > "$RUN_DIR/$s.formula-plane-candidates.json"
done
```

Validation:

```bash
timeout 10m cargo fmt --all -- --check
timeout 10m cargo test -p formualizer-common --quiet
timeout 15m cargo test -p formualizer-eval --quiet
timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet
```

Result: passed.

## Scanner results

| Scenario | Formula cells | Templates | Repeated templates | Cells in candidate runs | Row runs | Column runs | Max run length | Singletons | Holes | Exceptions | Parse errors | Dynamic | Volatile | Unsupported | Est. avoidable cells | Row-block partitions | Run->partition edge est. | Max partitions/run | Dense run coverage |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `agg_countifs_multi_criteria_100k` | 1000 | 1 | 1 | 1000 | 0 | 1 | 1000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1000 | 1 | 1 | 1 | 100.000% |
| `agg_mixed_rollup_grid_2k_reports` | 12000 | 5 | 5 | 12000 | 0 | 5 | 10000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 12000 | 4 | 7 | 3 | 100.000% |
| `chain_100k` | 99999 | 1 | 1 | 99999 | 0 | 1 | 99999 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 99999 | 25 | 25 | 25 | 100.000% |
| `fanout_100k` | 100000 | 1 | 1 | 100000 | 0 | 1 | 100000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 100000 | 25 | 25 | 25 | 100.000% |
| `headline_100k_single_edit` | 100001 | 2 | 1 | 100000 | 0 | 1 | 100000 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 100000 | 25 | 25 | 25 | 99.999% |
| `inc_cross_sheet_mesh_3x25k` | 50000 | 2 | 2 | 50000 | 0 | 2 | 25000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 50000 | 14 | 14 | 7 | 100.000% |

## Comparison to FP1.B

| Scenario | FP1.B formula cells | FP2.A formula cells | FP1.B templates | FP2.A templates | FP1.B row/column runs | FP2.A row/column runs | Interpretation |
|---|---:|---:|---:|---:|---:|---:|---|
| `agg_countifs_multi_criteria_100k` | 1000 | 1000 | 1 | 1 | 0 / 1 | 0 / 1 | Matches FP1.B; one dependent formula placement touches one row block. |
| `agg_mixed_rollup_grid_2k_reports` | 12000 | 12000 | 5 | 5 | 0 / 5 | 0 / 5 | Matches FP1.B; five placements touch four unique row blocks with seven run->partition touches. |
| `chain_100k` | 99999 | 99999 | 1 | 1 | 0 / 1 | 0 / 1 | Matches FP1.B; one long dependent placement spans 25 row blocks. |
| `fanout_100k` | 100000 | 100000 | 1 | 1 | 0 / 1 | 0 / 1 | Matches FP1.B; one long dependent placement spans 25 row blocks. |
| `headline_100k_single_edit` | 100001 | 100001 | 2 | 2 | 0 / 1 | 0 / 1 | Matches FP1.B; one singleton formula remains outside the candidate run. |
| `inc_cross_sheet_mesh_3x25k` | 50000 | 50000 | 2 | 2 | 0 / 2 | 0 / 2 | Matches FP1.B; two dependent placements touch 14 row blocks total. |

The FP2.A scanner preserves the FP1.B template view for the bounded synthetic corpus and adds candidate partition fanout estimates. No governed runner timing was rerun; FP1.B timing remains the comparison baseline.

## Interpretation notes

- The six synthetic scenarios are highly dense vertical fill-down families, so row runs remain zero and column runs dominate.
- All six scenarios have parser-supported, non-dynamic, non-volatile formulas in this scanner pass.
- `headline_100k_single_edit` has one singleton template and one 100k-cell candidate run, producing 99.999% dense coverage.
- Partition fanout is driven only by dependent formula placement rows. A run spanning rows 1..100000 touches 25 fixed 4096-row diagnostic partitions.
- These counters are intentionally conservative. Unsupported structured, external, 3D, dynamic, volatile, and parse-error formulas remain labels and counts rather than normalized FormulaPlane authority.

## Code surfaces added

- `SpanPartitionCounterOptions`, `FormulaPlaneCandidateCell`, `CandidateFormulaRun`, and `SpanPartitionCounters` under `crates/formualizer-eval/src/formula_plane/`.
- Bench-only scanner JSON field `formula_plane_candidates` with candidate run, partition, unsupported/dynamic/volatile, singleton, hole, exception, and dense coverage counters.
- Unit tests for vertical run partition fanout and conservative hole/exception/singleton accounting.

## Remaining gaps

| Gap | Impact | Recommended owner |
|---|---|---|
| Counters consume scanner cells, not loader-preserved shared-formula metadata | Cannot yet compare backend shared-formula preservation against candidate run detection | FP4.D loader/shared-formula metadata bridge |
| Candidate runs are diagnostic row/column spans, not a stored FormulaRunStore | No reusable span store or placement ID exists yet | FP2.B first safe span-store implementation |
| Candidate row-block partitions are placement partitions only | No precedent summary or result-region summary is represented | FP4.A passive dependency-template summaries |
| Dense rectangles are represented as row and column run observations where present | Future run-store builder still needs deterministic orientation/rectangle policy | FP2.B/FP3 run model tests |
| No runner `metrics.extra` integration yet | FP2.A artifacts live in scanner JSON/report only | Optional future runner low-risk metrics thread-through |

## Status

**PASS for FP2.A scope.** Passive FormulaPlane span/partition counters are available in the scanner baseline, match FP1.B formula/template/run counts for the bounded six-scenario suite, and add diagnostic row-block partition fanout without changing scheduler or evaluation behavior.
