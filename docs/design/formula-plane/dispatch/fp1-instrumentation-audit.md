# FP1 instrumentation audit

Date: 2026-04-29  
Branch: `formula-plane/bridge`  
Seed before FP1 hook commit: `b01e3e7` (`feat(formula-plane): seed bridge primitives and plan`)  
FP1 hook commit: `78d14c7` (`feat(formula-plane): add fp1 baseline stats hooks`)

## Summary

Existing instrumentation was sufficient to produce timing/correctness data, but not sufficient to produce an accuracy-preserving FormulaPlane/span baseline with formula, AST, and graph shape counters. I added one small read-only stats surface in `formualizer-eval` and wired those counters into the bench runner. Candidate template/run/shared-formula metrics remain out-of-band sidecar measurements rather than engine behavior.

The result is a useful **partial FP1 baseline**. It is not a complete span-scheduler observability stack.

## Audit results

| Metric requested by FP1 | Existing status before FP1 | FP1 action | Current status |
|---|---|---|---|
| Load/open/read time | `run-formualizer-native` emitted `load_ms`; `probe-load-envelope-matrix` emitted backend load times | No semantic change | Available for Umya governed scenarios; smoke-only Umya/Calamine backend probe available |
| Engine ingest/build time separable from open/read | Not separated in workbook runner | No broad loader refactor | Gap: `load_ms` includes open + workbook load + engine graph ingest/build |
| Full eval time | `run-formualizer-native` emitted wall-clock `full_eval_ms`; `EvalResult` had engine elapsed/computed vertices but runner did not report it | Runner now emits `full_eval_computed_vertices` and `full_eval_engine_elapsed_ms` in `metrics.extra` | Available for Umya governed scenarios |
| Incremental recalc time | Runner emitted one `incremental_us` but implemented benchmark op as `evaluate_all` or cached recalc plan mode | Runner now emits pending dirty/evaluation stats and incremental computed vertices/engine elapsed | Available, but semantic caveat remains: default mode uses `evaluate_all` for the incremental op |
| RSS | Runner JSON left `peak_rss_mb = null` | Used `/usr/bin/time -v` stderr parsing in report | Available in report artifacts, not embedded in runner JSON |
| Formula cell count | Missing from runner | Added read-only engine graph formula count; sidecar OOXML scan also counts raw formula cells | Available. For generated corpus, engine and sidecar counts agree |
| Formula AST/root count | Missing from runner | Added `formula_ast_root_count` and `formula_ast_node_count` from graph formula map + `DataStore` stats | Available from runner extras |
| Graph formula vertex count | Missing from runner | Added `graph_formula_vertex_count` | Available from runner extras |
| Graph edge/dependency row count | Missing from runner | Added exact read-only edge count over `CsrMutableEdges` | Available as logical graph edge count. It is not a separate dependency-row table count |
| Dirty/evaluation vertex counts | `engine.evaluation_vertices().len()` existed but runner did not report it | Added load/final/incremental-pending dirty and evaluation counts | Available from runner extras |
| Repeated formula/template candidate counts | Missing | Used passive sidecar OOXML scan with relative-reference normalization heuristic | Available as report-side heuristic, not engine counter |
| Row/column run candidate counts, holes/exceptions | Missing | Used passive sidecar scan | Available as report-side heuristic, not engine counter |
| Raw OOXML shared-formula visibility | Missing | Used passive sidecar scan over sheet XML `<f t="shared" ...>` | Available as report-side raw XML count |
| Backend mode: Umya vs Calamine | Runner is Umya-only; probe matrix supports both | Kept governed runner Umya-only; ran smoke probe for both backends | Available for smoke load/eval only; not incremental/governed scenarios |
| Adapter/materialization counters | Missing | No broad instrumentation added | Gap |
| Formula parser/template canonicalization | Missing | No engine/parser changes; sidecar heuristic only | Gap for production-grade template ID stability |

## Code added

Read-only, additive hooks only:

- `formualizer_eval::engine::GraphBaselineStats`
- `formualizer_eval::engine::EngineBaselineStats`
- `DependencyGraph::baseline_stats()`
- `Engine::baseline_stats()`
- `Engine::staged_formula_count()`
- `CsrMutableEdges::num_edges_exact()` for observability
- `run-formualizer-native` `metrics.extra` fields:
  - `load_*` engine/graph counters
  - `final_*` engine/graph counters
  - `incremental_pending_*` engine/graph counters when an incremental op occurs
  - `full_eval_computed_vertices`
  - `full_eval_engine_elapsed_ms`
  - `incremental_computed_vertices`
  - `incremental_engine_elapsed_ms`

These hooks do not change public/default workbook behavior, formula parsing, dependency construction, or evaluation semantics.

## Validation run after code changes

```bash
timeout 10m cargo fmt --all -- --check
timeout 10m cargo test -p formualizer-common --quiet
timeout 15m cargo test -p formualizer-eval --quiet
```

Result: passed (`formualizer-common`: 26 tests; `formualizer-eval`: 1169 passed, 4 ignored, doctest pass).

## FP1.B additions

FP1.B implemented the recommended bounded follow-up without changing default workbook behavior or formula evaluation semantics.

| Metric requested by FP1.B | FP1.B action | Current status |
|---|---|---|
| Load/open/read split | Added `open_read_ms` and `workbook_ingest_ms` under `metrics.extra`; retained existing `metrics.load_ms` | Available in governed runner for Umya and Calamine |
| Backend mode | Added `run-formualizer-native --backend umya|calamine`; output includes backend in `metrics.extra.backend` and `meta.backend` | Available for full eval, incremental op, scenario metadata, and correctness checks |
| Adapter formula counters | Added read-only `AdapterLoadStats` and workbook loader path that returns stats after ingest | Formula cells observed and handed to engine available for Umya/Calamine |
| Adapter value counters | Counted backend-observed non-empty values and dense value slots handed to Arrow ingest | Available for Umya/Calamine; value slots are intentionally dense materialization slots |
| Parser-backed template scan | Added bench-only `scan-formula-templates` binary reading OOXML formulas and parsing through `formualizer_parse` | Emits stable template IDs, canonical AST text, labels, run counts, holes, exceptions, and raw shared formula visibility |
| Raw OOXML shared formulas | Scanner counts `<f t="shared" ...>`, anchor refs, and shared indices | Available in scan JSON; generated six-scenario corpus has zero shared tags |

FP1.B report: `docs/design/formula-plane/dispatch/fp1b-baseline-report.md`  
FP1.B raw artifacts: `target/fp1b-baseline/6322615`

## Remaining instrumentation gaps

1. **Shared-formula adapter counters.** The bench scanner reports raw OOXML shared tags, but Umya/Calamine runner stats still leave `adapter_shared_formula_tags_observed` absent because that metadata is not currently carried through adapter ingest.
2. **Dependency edge taxonomy.** `graph_edge_count` is a logical outgoing graph-edge count, not a dependency-row taxonomy split by scalar cell, range, stripe, virtual, name, or sheet dependency.
3. **Template scanner is conservative.** It is parser-backed and stable enough for baseline reports, but structured/external/3D references and parse failures are labeled rather than normalized into production FormulaPlane template IDs.
4. **Calamine open/read timing is lazy-open shaped.** `open_read_ms` is near zero for current Calamine XLSX open; sheet IO/materialization is attributed to `workbook_ingest_ms`.
5. **Dense value handoff remains coarse.** `adapter_value_slots_handed_to_engine` measures current dense Arrow sheet materialization slots, not a future sparse/span representation.
6. **No production FormulaPlane partition/span authority yet.** FP2.A adds scanner-only candidate span and row-block partition counters, but they are passive diagnostics and do not route dirty propagation, scheduler behavior, dependency graph construction, or evaluation.

## Next patch plan

Recommended next step for FP2:

1. Implement the first in-memory passive span-store builder from the FP2.A candidate cells/runs, with deterministic placement IDs and no evaluator integration.
2. Thread raw/shared formula metadata into loader observability if FP2 loader work needs to distinguish preserved shared formulas from expanded formula cells.
3. Keep `open_read_ms`, `workbook_ingest_ms`, backend mode, adapter counters, and scanner JSON in all FP2 reports so regressions can be attributed to IO, ingest, graph build, or evaluation separately.
