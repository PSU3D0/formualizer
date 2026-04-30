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

## Remaining instrumentation gaps

1. **Open/read vs engine ingest/build split.** `run-formualizer-native` measures a single `load_ms` around `UmyaAdapter::open_path` plus `Workbook::from_reader`. This is enough for a baseline but not enough to attribute loader vs graph ingest.
2. **Runner remains Umya-only.** Calamine data comes from `probe-load-envelope-matrix` smoke, which is not governed by the same scenario operation/correctness contract and does not run incremental recalc.
3. **Dependency edge meaning is graph-edge count, not source dependency rows.** It counts logical outgoing edges in `CsrMutableEdges`, including pending delta mutations. It does not expose decomposed dependency records or compressed range-dependency rows.
4. **Template/run metrics are heuristic sidecar data.** They are based on raw OOXML formula text and simple relative-reference normalization, not parser-backed canonical templates.
5. **No adapter/materialization counters.** Calamine/Umya adapter materialization counts, formula text handoff counts, and range materialization counts are not yet exposed.
6. **Shared formulas are only raw XML visibility.** The baseline records `<f t="shared" ...>` tags, anchors, and shared-index count, but not how an adapter expands or preserves shared formulas.
7. **No production FormulaPlane partition/span counters yet.** FP1 intentionally avoids implementing span scheduler behavior; it only establishes baseline counters for comparison.

## Minimal next patch plan

Recommended FP1.B before FP2 implementation:

1. Add a tiny bench-facing load-phase timing split in `formualizer-workbook` or the runner: `open_read_ms`, `workbook_ingest_ms`, and current combined `load_ms`. Keep existing `load_ms` for compatibility.
2. Add parser-backed, read-only formula template scan in bench tooling (not engine evaluation): parse formula text, canonicalize references relative to anchor, and emit stable template IDs plus unsupported/volatile/dynamic labels.
3. Add optional adapter metadata counters for Umya/Calamine workbook readers: formula cells observed, shared formula tags observed, formula cells handed to engine, values handed to engine.
4. Extend `probe-load-envelope-matrix` or add a parallel governed runner mode for Calamine so backend comparisons include scenario metadata and incremental recalc.
