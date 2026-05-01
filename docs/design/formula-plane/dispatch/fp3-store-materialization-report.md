# FP3 span-store materialization report

Date: 2026-04-30  
Branch: `formula-plane/bridge`  
Raw artifact directory: `target/fp3-materialization/1e4b01d`  
Implementation commit: `1e4b01d` (`feat(formula-plane): report passive span store materialization`)

## Scope

FP3 connects the passive `FormulaRunStore` from FP2.B to the existing read-only `scan-formula-templates` path and emits materialization accounting in scanner JSON. It does not change workbook loading, graph construction, formula evaluation, scheduling, dirty propagation, dependency graph bypass, public API, or any Core+Overlay work.

The scanner now emits two additional top-level sections:

- `formula_run_store`: deterministic passive store report, template arena summary, run samples, gap samples, rejected-cell samples, and FP2.A reconciliation.
- `materialization_accounting`: compact-store counters plus optional runner graph materialization stats parsed from `--runner-json`.

No table in this report claims an actual runtime win. Avoidable counts are representation/materialization opportunity estimates only.

## Commands run

Build and scanner materialization artifacts:

```bash
RUN_DIR=target/fp3-materialization/$(git rev-parse --short HEAD)
mkdir -p "$RUN_DIR"
timeout 10m cargo build --release -p formualizer-bench-core --features formualizer_runner --bin scan-formula-templates --bin run-formualizer-native \
  > "$RUN_DIR/build-release.stdout.log" \
  2> "$RUN_DIR/build-release.stderr.log"

for s in headline_100k_single_edit chain_100k fanout_100k inc_cross_sheet_mesh_3x25k agg_countifs_multi_criteria_100k agg_mixed_rollup_grid_2k_reports; do
  timeout 2m target/release/scan-formula-templates \
    --scenarios benchmarks/scenarios.yaml \
    --scenario "$s" \
    --root . \
    --runner-json "target/fp1b-baseline/6322615/$s.calamine.native_best.json" \
    > "$RUN_DIR/$s.formula-run-store.json"
done
```

Graph materialization stats came from the existing FP1.B Calamine governed runner artifacts in `target/fp1b-baseline/6322615/*.calamine.native_best.json`. The governed runner was not rerun for FP3 because the existing bounded artifacts already contain `load_graph_formula_vertex_count`, `load_formula_ast_root_count`, `load_formula_ast_node_count`, and `load_graph_edge_count` for the six scenarios.

Validation:

```bash
timeout 30s git status --short
timeout 10m cargo fmt --all -- --check
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet
```

Result: passed.

## Materialization accounting

`compact_representation_ratio` is `formula_cells / max(1, run_count + template_count + exception_count + rejected_count)`. Formula-vertex and AST-root avoided counts use FP1.B runner graph counters when available. Graph-edge avoided counts are rough estimates: `min(runner_graph_edges, formula_cells_represented_by_runs - run_count)`, intended only as an upper-bound opportunity signal for dense run materialization.

| Scenario | Formula cells | Graph formula vertices | AST roots | AST nodes | Graph edges | Templates | Runs | Rejected | Holes | Exceptions | Dense run coverage | Est. avoidable formula vertices | Est. avoidable AST roots | Est. avoidable graph edges | Compact ratio |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `agg_countifs_multi_criteria_100k` | 1000 | 1000 | 1000 | 7006 | 5000 | 1 | 1 | 0 | 0 | 0 | 100.000000% | 999 | 999 | 999 | 500.000x |
| `agg_mixed_rollup_grid_2k_reports` | 12000 | 12000 | 12000 | 34006 | 28000 | 5 | 5 | 0 | 0 | 0 | 100.000000% | 11995 | 11995 | 11995 | 1200.000x |
| `chain_100k` | 99999 | 99999 | 99999 | 299997 | 99999 | 1 | 1 | 0 | 0 | 0 | 100.000000% | 99998 | 99998 | 99998 | 49999.500x |
| `fanout_100k` | 100000 | 100000 | 100000 | 200001 | 100000 | 1 | 1 | 0 | 0 | 0 | 100.000000% | 99999 | 99999 | 99999 | 50000.000x |
| `headline_100k_single_edit` | 100001 | 100001 | 100001 | 300002 | 100000 | 2 | 2 | 0 | 0 | 0 | 99.999000% | 99999 | 99999 | 99999 | 25000.250x |
| `inc_cross_sheet_mesh_3x25k` | 50000 | 50000 | 50000 | 150000 | 100000 | 2 | 2 | 0 | 0 | 0 | 100.000000% | 49998 | 49998 | 49998 | 12500.000x |

## Passive store counters

The FP2.B store stores supported singleton formulas as singleton runs. That is why `headline_100k_single_edit` has one extra run and a store-vs-FP2.A reconciliation delta; FP2.A counted only repeated candidate runs.

| Scenario | Row runs | Column runs | Singleton runs | Cells represented by all runs | Row-block partitions | Run->partition edge est. | Max partitions/run | FP2.A reconciliation |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `agg_countifs_multi_criteria_100k` | 0 | 1 | 0 | 1000 | 1 | 1 | 1 | matched |
| `agg_mixed_rollup_grid_2k_reports` | 0 | 5 | 0 | 12000 | 4 | 7 | 3 | matched |
| `chain_100k` | 0 | 1 | 0 | 99999 | 25 | 25 | 25 | matched |
| `fanout_100k` | 0 | 1 | 0 | 100000 | 25 | 25 | 25 | matched |
| `headline_100k_single_edit` | 0 | 1 | 1 | 100001 | 25 | 26 | 25 | singleton delta only |
| `inc_cross_sheet_mesh_3x25k` | 0 | 2 | 0 | 50000 | 14 | 14 | 7 | matched |

## Interpretation notes

- The six-scenario corpus remains highly dense and vertical: all repeated templates materialize as column runs.
- `rejected_cell_count`, `hole_count`, and `exception_count` are zero for this bounded corpus, so compact ratios are driven only by templates and runs.
- `headline_100k_single_edit` has one singleton template plus one 100k-cell column run; dense-run coverage excludes the singleton and remains 99.999%.
- The estimated avoidable formula vertices are current runner formula vertices minus a compact run/exception/rejected proxy; this is not a runtime graph-bypass implementation.
- The estimated avoidable AST roots are current runner AST roots minus compact template/rejected-root proxy; AST node sharing remains unimplemented.
- The estimated avoidable graph edges are deliberately rough because the passive store does not yet know dependency summaries or precedent regions.

## Code surfaces added

- `scan-formula-templates --runner-json <path>` optionally parses governed runner JSON graph materialization counters.
- Scanner JSON now includes `formula_run_store` built by `FormulaRunStore::build` from the existing scanner candidate cells.
- Scanner JSON now includes `materialization_accounting` with formula cells, runner graph stats when provided, run/template/rejected/hole/exception counts, compact ratio, and clearly labeled estimates.
- The integration is read-only and bench-local; FormulaPlane primitives remain in `crates/formualizer-eval/src/formula_plane/`.

## Remaining gaps

| Gap | Impact | Recommended owner |
|---|---|---|
| The passive store is not consumed by graph build | Current runtime still materializes per-formula graph vertices, AST roots, and edges | FP4/FP5 graph-build optimization design |
| Graph-edge opportunity estimate lacks dependency summaries | Edge savings are rough and cannot distinguish shared precedent regions from dense dependent placement | FP5 dependency summary work |
| Runner graph stats are joined via scanner `--runner-json` | Scanner artifacts need a matching runner artifact for live graph comparison | Future harness-level join or runner-side passive scan hook |
| Shared-formula OOXML preservation is still not loader-integrated | Scanner cannot yet compare compact spans against backend-preserved shared formula groups | FP4 loader hint bridge |

## Status

**PASS for FP3 scope.** The passive FormulaRunStore is now reported by the scanner, FP1.B runner graph materialization counters are joined into the bounded scanner artifacts, and the report quantifies representation/materialization opportunity without changing runtime behavior or claiming a performance win.
