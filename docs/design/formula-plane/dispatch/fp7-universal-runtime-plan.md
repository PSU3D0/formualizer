# FP7 Universal FormulaPlane Runtime Plan

Date: 2026-05-04  
Status: architectural plan; production source unchanged.

## Purpose

Make `FormulaPlaneMode::AuthoritativeExperimental` use one FormulaPlane-aware evaluation coordinator for every public `evaluate_*` entry point. `SingletonUnique` formulas remain legacy graph vertices as a representation choice; the runtime must still schedule them as `FormulaProducerId::Legacy`, not fall back to the old public legacy evaluation paths.

Current hedge to remove:

```text
if graph.formula_authority().active_span_count() > 0 { FormulaPlane runtime } else { legacy runtime }
```

Call sites to remove by the final phase:

```text
crates/formualizer-eval/src/engine/eval.rs:
  evaluate_vertex, evaluate_until, evaluate_recalc_plan, evaluate_all,
  evaluate_all_with_delta, evaluate_cell, evaluate_cells,
  evaluate_cells_with_delta, evaluate_cells_cancellable,
  evaluate_until_cancellable, evaluate_all_logged, delta/evaluate_all variant near 10305
```

## Non-Goals

```text
Do not make singletons point spans in FP7.
Do not add graph proxy vertices.
Do not bypass ComputedWriteBuffer for span writes.
Do not remove graph ownership of legacy dependency/cycle/dirty semantics.
```

## Phase 1 - Central Coordinator With Legacy-Only Parity

Capability:

- Add one internal FormulaPlane evaluation coordinator selected by `config.formula_plane_mode == AuthoritativeExperimental`.
- Keep `SingletonUnique` fallback; when no active spans exist, the coordinator schedules `FormulaProducerId::Legacy` producers but executes through the existing graph schedule primitives.
- Remove `active_span_count()` gates only where the coordinator is proven parity-safe for legacy-only work.

Touch points:

- `crates/formualizer-eval/src/engine/eval.rs`
  - Introduce helpers such as `uses_formula_plane_runtime()`, `evaluate_formula_plane_all(...)`, and a shared legacy schedule loop extracted from `evaluate_all` / `evaluate_all_cancellable` / `evaluate_all_logged`.
  - Keep using `create_evaluation_schedule`, `changed_virtual_dep_vertices`, `evaluate_layer_*`, `mirror_vertex_value_to_overlay`, and `graph.clear_dirty_flags` for all-legacy work.
- `crates/formualizer-eval/src/formula_plane/scheduler.rs`
  - No semantic change; this phase can skip mixed scheduler when there are no spans.

Expected tests restored/protected:

- Most singleton-only tests should remain green, including:
  - `date_math_parity::math_functions_mround_roman_arabic_sumsq_in_engine`
  - `evaluation::test_evaluation_of_empty_placeholders`
  - `recalc_plan::*` when no spans are present
  - `schedule_cache::schedule_cache_hits_on_repeated_value_only_chain_recalc`

Risks:

- If the coordinator literally calls `evaluate_all`, it is still a fallback. Extract the loop into private primitives instead.
- Legacy-only coordinator must preserve telemetry, static schedule cache hits, virtual-dep replan iterations, volatile redirty, and `recalc_epoch` increments exactly.

## Phase 2 - Mixed Dirty Seed and Closure

Capability:

- Build mixed work from dirty state instead of whole-authority.
- Inputs:
  - dirty/volatile legacy formula vertices from `graph.get_evaluation_vertices()`;
  - changed cell/region queue recorded by edits, spills, source invalidation, and structural operations;
  - FormulaAuthority span indexes.
- Use `compute_dirty_closure(...)` to project changed regions through `FormulaConsumerReadIndex`, producing dirty span work and downstream legacy work.
- Initial span work may be whole-span only for newly ingested active spans, but edits must use projected dirty domains when available.

Touch points:

- `crates/formualizer-eval/src/engine/eval.rs`
  - Replace `build_formula_plane_mixed_schedule()` whole-work construction with a dirty-work builder.
  - Capture changed regions in `set_cell_value`, spill writeback/clear, source invalidation, table invalidation, and structural-edit hooks.
- `crates/formualizer-eval/src/formula_plane/authority.rs`
  - Add a small pending dirty-region queue and clear/merge API.
  - Continue `rebuild_indexes()` after authoritative ingest.
- `crates/formualizer-eval/src/formula_plane/producer.rs`
  - Reuse `compute_dirty_closure`, `ProducerDirtyDomain`, `ProjectionResult`.
- `crates/formualizer-eval/src/formula_plane/scheduler.rs`
  - Schedule only the dirty closure work.

Expected tests restored:

- `formula_edit_propagation::*` (4)
- `range_dependencies::test_healed_formula_recomputes_downstream_dependents`
- `range_dependencies::test_partial_range_overlap_dependency_propagation`
- `range_dependencies::test_nested_formula_within_range_propagation`
- `eval_flush_recalc_probe::repeated_edit_recalc_keeps_computed_overlays_bounded_and_correct`
- `sources::scalar_source_invalidate_marks_multiple_dependents_dirty`
- `sources::table_source_invalidate_marks_multiple_dependents_dirty`
- `spill_edges::spill_values_update_dependents`

Risks:

- Existing graph dirty propagation marks legacy vertices, not spans. Span dirty requires explicit changed-region capture.
- `WholeResult` legacy consumers are safe but coarse; they may overcompute until legacy dependency extraction improves.
- Dirty queues must be cleared only after successful flush/evaluation.

## Phase 3 - Cycle and Error Semantics

Capability:

- Mixed runtime detects and mirrors cycles the same way the graph scheduler does.
- For legacy-only and legacy-involved cycles, continue using `Scheduler::create_schedule[_with_virtual]` cycle output.
- For span-involved cycles, fail closed or mark affected span cells with `#CIRC!` only when the participating result cells are known exactly.

Touch points:

- `crates/formualizer-eval/src/engine/scheduler.rs`
  - Reuse existing `Schedule { layers, cycles }` for legacy producer subsets.
- `crates/formualizer-eval/src/formula_plane/scheduler.rs`
  - Expose cycle producer sets rather than only `is_authoritative_safe() == false`.
- `crates/formualizer-eval/src/engine/eval.rs`
  - Add a shared cycle application helper for legacy vertices and span result cells.

Expected tests restored:

- `arrow_canonical_606::error_mirroring_cycle_is_visible_under_canonical_reads`
- `layer_evaluation::test_evaluation_with_cycles`
- `demand_driven::test_evaluate_until_precedents_include_a_cycle`

Risks:

- Span cycles are not graph SCCs. If exact result cells cannot be identified, the correct FP7 behavior is fail closed with `NImpl`, not silent omission.
- Graph cycle count semantics must match existing `EvalResult.cycle_errors`.

## Phase 4 - Virtual Dependencies and Demand-Driven Targets

Capability:

- Demand-driven APIs build target-bounded mixed work instead of whole-authority work.
- Dynamic/virtual refs continue using `VirtualDepBuilder`, `build_demand_subgraph`, and `changed_virtual_dep_vertices` for legacy producers.
- Span producers are included when target cells intersect span result regions or when legacy/dirty closure reaches them.

Touch points:

- `crates/formualizer-eval/src/engine/eval.rs`
  - Factor target parsing from `evaluate_until`, `evaluate_cell`, `evaluate_cells`, and cancellable variants into the coordinator.
  - Convert target result regions to `FormulaProducerWork` seeds.
  - Preserve virtual-dep replan loop for legacy producers.
- `crates/formualizer-eval/src/formula_plane/producer.rs`
  - Use result/read indexes to project target demand backward/forward where possible; otherwise conservative whole producer.
- `crates/formualizer-eval/src/formula_plane/scheduler.rs`
  - Schedule mixed target-bounded work.

Expected tests restored:

- `demand_driven::test_evaluate_until_multiple_targets`
- `cancellation::test_cancellation_in_demand_driven_evaluation` after Phase 6 cancellation hooks
- `offset_dynamic::offset_dynamic_ordering_with_dirty_formula_target`
- `offset_dynamic::offset_entrypoint_parity`
- `offset_dynamic::recalc_plan_with_offset_falls_back_to_dynamic_recalc`
- `infinite_ranges::partial_ranges_column_tail_and_head_bounds`

Risks:

- Current span read summaries support static pointwise formulas only. Dynamic refs, names, structured refs, and arrays should remain legacy or explicit fail-closed.
- Demand backward projection for spans is harder than dirty forward projection; first cut can conservatively evaluate whole intersecting spans.

## Phase 5 - RecalcPlan and Schedule Cache Parity

Capability:

- `RecalcPlan` can represent mixed plans or explicitly records that it is legacy-only.
- `evaluate_recalc_plan` under AuthoritativeExperimental no longer ignores the supplied plan.
- Static schedule cache remains active for legacy-only and mixed plans with stable authority/index epochs.

Touch points:

- `crates/formualizer-eval/src/engine/eval.rs`
  - Extend `RecalcPlan` with mixed schedule metadata or a `FormulaPlanePlan` sidecar.
  - Include `FormulaAuthority::indexes_epoch()` and graph topology epoch in cache keys.
- `crates/formualizer-eval/src/formula_plane/authority.rs`
  - Expose stable epochs needed for mixed plan invalidation.
- `crates/formualizer-eval/src/engine/plan.rs`
  - Keep dependency-plan build unchanged unless plan serialization needs producer ids.

Expected tests restored:

- `recalc_plan::recalc_plan_matches_evaluate_all`
- `recalc_plan::recalc_plan_reused_for_multiple_runs`
- `schedule_cache::schedule_cache_hits_on_repeated_value_only_chain_recalc`

Risks:

- Cached mixed schedules must be invalidated by formula authority rebuilds, dirty read-index rebuilds, graph topology edits, and virtual-dep changes.
- Dynamic refs should keep existing fallback-to-replan behavior.

## Phase 6 - Delta, Logged, and Cancellable Semantics

Capability:

- FormulaPlane coordinator has execution options:
  - optional `DeltaCollector`;
  - optional `ChangeLog`;
  - optional cancellation flag.
- Span writes report changed cells before/after flush so deltas/logs include FormulaPlane output.
- Cancellation is checked before schedule build, between layers, between producers, and in span placement loops.

Touch points:

- `crates/formualizer-eval/src/engine/eval.rs`
  - Thread `DeltaCollector`, `ChangeLog`, and cancellation into the coordinator.
  - Reuse `record_cell_if_changed` for legacy and span outputs.
- `crates/formualizer-eval/src/formula_plane/span_eval.rs`
  - Add cancellation-aware placement iteration and expose per-cell changed output metadata.
- `crates/formualizer-eval/src/formula_plane/runtime.rs`
  - No ownership change; only task/report vocabulary if needed.

Expected tests restored:

- `cancellation::test_cancellation_between_layers`
- `cancellation::test_cancellation_in_demand_driven_evaluation`
- Any existing delta/log parity assertions that become active once gates are removed.

Risks:

- `ComputedWriteBuffer` flush currently owns span output application. Delta/log capture must observe old values before flush and new values after flush without per-cell write bypass.
- Partial cancellation after a layer must leave dirty state conservative and recoverable.

## Phase 7 - Structural, Source, Table, and Spill Integration

Capability:

- Structural edits and semantic producers invalidate FormulaPlane authority and/or dirty regions precisely enough to avoid stale spans.
- Unsupported structural transformations demote affected spans or fail closed, then rebuild indexes and dirty downstream consumers.

Touch points:

- `crates/formualizer-eval/src/engine/eval.rs`
  - Sheet remove/rename/move, row/column visibility, source/table invalidation, spill apply/clear hooks.
- `crates/formualizer-eval/src/formula_plane/authority.rs`
  - Add demotion/invalidation reports and changed-region emission.
- `crates/formualizer-eval/src/formula_plane/runtime.rs`
  - Add overlay/punchout state only if needed to represent demotions.

Expected tests restored:

- `arrow_canonical_611::canonical_remove_sheet_marks_ref_and_propagates_to_downstream_dependents`
- `tables::structured_ref_this_row_column_rewrites_to_concrete_cell`
- remaining `sources::*` / `spill_edges::*` cases if not fixed in Phase 2

Risks:

- FormulaPlane spans have no graph vertices, so existing structural #REF! marking does not automatically reach span cells.
- Table/structured refs should not be accepted as spans until dependency summaries can represent them; legacy producer extraction must be explicit.

## Final Cutover Gate

After Phases 1-7 are green:

```text
Replace all 12 active_span_count() runtime gates with:
  if self.config.formula_plane_mode == FormulaPlaneMode::AuthoritativeExperimental

Then remove any remaining public legacy evaluate_* branch reachable in AuthoritativeExperimental.
```

Required validation:

```bash
cargo fmt --all -- --check
cargo clippy -p formualizer-eval --all-targets -- -D warnings
cargo test -p formualizer-eval formula_plane_ingest_shadow --quiet
cargo test -p formualizer-eval formula_plane --quiet
cargo test -p formualizer-eval computed_flush --quiet
cargo test -p formualizer-eval rangeview_ --quiet
cargo test -p formualizer-eval --quiet
```

## Failure Allocation Summary

```text
Phase 1: singleton legacy parity, date math, empty placeholders, basic recalc/schedule cache no-span cases
Phase 2: edit dirty propagation, range dependencies, eval-flush bounded overlays, sources/spills dirtying
Phase 3: cycle/error visibility
Phase 4: demand-driven, dynamic OFFSET/INDIRECT, infinite/open range target parity
Phase 5: RecalcPlan and schedule cache mixed-plan semantics
Phase 6: delta/log/cancellation semantics
Phase 7: structural #REF!, tables, source/table/spill edge cases
```
