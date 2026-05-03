# FP6 Implementation/Test Plan Review

Date: 2026-05-03
Branch: `formula-plane/bridge`
Reviewed docs:

- `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_ARCHITECTURE.md`
- `docs/design/formula-plane/FORMULA_PLANE_IMPLEMENTATION_PLAN.md`
- `docs/design/formula-plane/REPHASE_PLAN.md`
- `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_CONTRACT.md`

## Verdict

Overall verdict: WARN.

FP6.1-FP6.4 are directionally testable and incrementally scoped, but they are not
ready to change runtime behavior until the missing tests and counters below are
written first. The plan's phase boundaries are mostly narrow: FP6.1 can be pure
storage vocabulary, FP6.2 can be default-off placement, FP6.3 can be sidecar
lookup and conservative dirty projection, and FP6.4 can be a test-driven direct
span evaluator. The WARN is because the current worktree still contains passive
FormulaPlane primitives only, while the new runtime path depends on unimplemented
runtime stores, FormulaOverlay, sidecar indexes, scheduler seams, and the
ComputedWriteBuffer/fragment write path.

| Phase | Verdict | Reason |
|---|---|---|
| FP6.1 core runtime stores and handles | PASS | Testable without behavior change if it stays under `crates/formualizer-eval/src/formula_plane/` and asserts store ownership, generational IDs, and `FormulaResolution` authority order. |
| FP6.2 authority-grade span placement | WARN | Testable, but must add exact-family, fallback, default-off, and materialization-avoidance tests before any ingest/graph-build behavior changes. |
| FP6.3 sidecar region indexes and dirty routing | WARN | Testable, but high correctness risk. Region indexes, dirty projection, exact filtering, and no-under-approx oracle tests must land before connecting to graph dirty routing. |
| FP6.4 span evaluator and computed fragment writes | WARN | Testable only as a gated/test-only evaluator until scheduling is proven. The source inspected has computed overlays but no discoverable `ComputedWriteBuffer`/fragment API symbol, so write-path tests must lock the intended buffer/fragment seam before implementation. |

## Current Source Posture

- `crates/formualizer-eval/src/formula_plane/` currently holds passive primitives: `ids.rs`, `span_counters.rs`, `span_store.rs`, `template_canonical.rs`, `dependency_summary.rs`, diagnostics, grid, partition, and virtual refs.
- `crates/formualizer-eval/src/formula_plane/ids.rs` only exposes `FormulaTemplateId` and `FormulaRunId`; runtime `FormulaSpanId`, overlay IDs, mask IDs, generations, and epochs are not present yet.
- `crates/formualizer-eval/src/formula_plane/span_store.rs` is the passive `FormulaRunStore` builder, not the runtime `FormulaPlane`/`TemplateStore`/`SpanStore` described in FP6.1.
- `crates/formualizer-eval/src/formula_plane/dependency_summary.rs` already has useful passive summary and reverse-query tests; these are good oracles for FP6.3 but are not sidecar runtime indexes.
- `crates/formualizer-eval/src/engine/eval.rs` and `crates/formualizer-eval/src/engine/scheduler.rs` are vertex-oriented. `Scheduler` schedules `VertexId` layers only, with optional virtual vertex dependencies.
- `crates/formualizer-eval/src/arrow_store/mod.rs` has `overlay -> computed_overlay -> base` precedence and computed overlay compaction, but the searched source did not expose a named `ComputedWriteBuffer`, `DenseRange`, `RunRange`, or computed fragment write API.
- `crates/formualizer-eval/src/interpreter.rs` already supports `Interpreter::new_with_cell`, which is the likely scalar-semantics-preserving hook for span placement evaluation.
- `crates/formualizer-eval/src/function_contract.rs` contains passive function dependency contracts and tests; FP6.4 should not expand this into span kernels.

## Missing Tests Before Code

### Stores And Handles

Add these as unit tests near the new runtime store modules, likely under
`crates/formualizer-eval/src/formula_plane/`:

- `template_store_interns_equivalent_templates_once`
- `template_store_distinguishes_literal_values_in_authority_key`
- `template_store_compiled_once_cell_is_shared_by_spans`
- `span_store_allocates_generational_span_ids`
- `span_store_rejects_stale_generation_after_remove`
- `span_record_stores_ids_domain_state_version_not_ast_or_exceptions`
- `placement_domain_row_run_iteration_is_correct`
- `placement_domain_col_run_iteration_is_correct`
- `placement_domain_rect_iteration_is_row_major_and_bounded`
- `formula_plane_epoch_increments_when_store_mutates`

These tests should fail if a span owns an AST, exception map, dependency index,
or per-placement graph materialization state inline.

### FormulaResolution

Add unit tests for the formula authority cascade before any lookup path is wired
into `Engine`:

- `formula_resolution_prefers_overlay_formula_override_over_span`
- `formula_resolution_prefers_overlay_cleared_over_span`
- `formula_resolution_prefers_overlay_value_override_over_span_formula`
- `formula_resolution_returns_span_placement_without_legacy_materialization`
- `formula_resolution_falls_back_to_legacy_vertex_outside_span`
- `formula_resolution_returns_empty_for_no_formula_authority`

The key assertion is that span lookup returns a virtual placement/template ID and
placement coordinate; it must not allocate or require a scalar formula vertex.

### FormulaOverlay

Add tests in a dedicated FormulaPlane overlay module before edits are integrated:

- `formula_overlay_masks_span_resolution`
- `formula_overlay_epoch_invalidates_projection_cache`
- `formula_overlay_value_override_masks_formula_but_not_user_value_overlay`
- `formula_overlay_cleared_masks_computed_and_base_formula_authority`
- `formula_overlay_legacy_owned_records_vertex_escape_hatch`
- `formula_overlay_bulk_region_query_returns_intersecting_entries_only`
- `formula_overlay_projection_excludes_punched_out_placements`

These should keep FormulaOverlay separate from Arrow's value/computed overlays.
The formula-plane overlay controls formula authority; Arrow overlays control
value precedence.

### Region Indexes

Add unit tests for a generic spreadsheet-shaped index and then wrapper tests for
the three role-specific indexes:

- `sheet_region_index_finds_point_interval_rect_and_whole_axis_entries`
- `sheet_region_index_may_overreturn_rect_bucket_but_never_misses_intersection`
- `span_domain_index_finds_row_run_owner`
- `span_domain_index_finds_col_run_owner`
- `span_domain_index_finds_rect_intersections`
- `span_domain_index_does_not_apply_formula_overlay_semantics`
- `span_dependency_index_indexes_same_row_static_precedent_regions`
- `span_dependency_index_indexes_absolute_precedent_regions`
- `span_dependency_index_keeps_whole_column_bucket_separate`
- `formula_overlay_index_finds_cell_punchout`
- `formula_overlay_index_finds_region_punchouts_without_domain_entries`

Use `crates/formualizer-eval/src/engine/interval_tree.rs` only as a minimal 1D
helper if needed. Tests should reject a single ambiguous map that handles domain,
dependency, and overlay questions together.

### Dirty Projection

Add unit tests around `SpanDependencyIndex -> exact filter -> DirtyProjection ->
SpanDirtyStore` before graph dirty routing calls it:

- `same_row_dependency_edit_marks_candidate_span_dirty_whole`
- `absolute_dependency_edit_marks_candidate_span_dirty_whole`
- `whole_column_dependency_query_marks_candidate_span`
- `rect_dependency_query_exact_filters_bucket_candidates`
- `unrelated_edit_does_not_mark_span_dirty`
- `unsupported_dependency_prevents_span_index_entry`
- `multiple_changed_regions_union_whole_span_dirty_once`
- `sidecar_dirty_is_no_under_approx_against_legacy_fixture`

For FP6.3, whole-span dirty is acceptable. The tests should not require partial
projection, but they must prove no changed precedent that legacy would route is
missed by the sidecar.

### Scheduler Seam

Although the implementation plan places graph/proxy scheduling in FP6.5, add the
seam tests before any FP6.4 evaluator is invoked from normal recalculation. Good
locations are `crates/formualizer-eval/src/engine/tests/formula_plane_scheduler_seam.rs`
and narrow unit tests near any new task scheduler type:

- `formula_plane_disabled_schedule_is_unchanged`
- `legacy_precedent_dirty_schedules_one_span_task_not_n_placements`
- `span_result_region_dirty_schedules_legacy_dependent`
- `span_to_span_dependency_orders_precedent_before_dependent`
- `span_proxy_cycle_demotes_or_reports_conservative_cycle`
- `scheduler_reports_span_work_items_separately_from_vertex_work_items`

If these are not ready in FP6.4, the evaluator should remain direct/test-only and
must not participate in `evaluate_all` or demand-driven evaluation.

### Span Evaluator

Add evaluator tests in `crates/formualizer-eval/src/formula_plane/span_eval.rs`
and integration tests under `crates/formualizer-eval/src/engine/tests/`:

- `span_eval_row_run_matches_legacy_outputs`
- `span_eval_col_run_matches_legacy_outputs`
- `span_eval_rect_matches_legacy_outputs`
- `span_eval_uses_interpreter_current_cell_for_relative_refs`
- `span_eval_preserves_explicit_empty_outputs`
- `span_eval_propagates_scalar_errors_like_legacy`
- `span_eval_effective_domain_skips_overlay_punchouts`
- `span_eval_fallback_for_unsupported_template_matches_legacy`
- `span_eval_does_not_call_set_cell_formula_per_placement`
- `span_eval_does_not_allocate_per_placement_ast_roots_or_edges`

The evaluator can use the scalar interpreter per placement, but it must reuse the
one stored template and placement context. It must not create a temporary graph
formula per placement to get a value.

### ComputedWriteBuffer Writes

Before FP6.4 writes span outputs, add write-buffer tests in the actual eval-flush
module. If no module exists yet in this worktree, create the tests with the new
buffer rather than writing directly to `computed_overlay` maps:

- `computed_write_buffer_coalesces_varying_row_run_to_dense_range_fragment`
- `computed_write_buffer_coalesces_constant_row_run_to_run_range_fragment`
- `computed_write_buffer_preserves_sparse_holes_as_sparse_offsets_fragment`
- `computed_write_buffer_explicit_empty_masks_base_value`
- `computed_write_buffer_user_overlay_precedence_survives_fragment_flush`
- `span_eval_writes_through_computed_write_buffer_not_direct_overlay`
- `range_view_reads_flushed_computed_fragments`

The existing `crates/formualizer-eval/src/engine/tests/formula_overlay_writeback.rs`
and `crates/formualizer-eval/src/arrow_store/mod.rs` overlay tests are useful
precedence oracles, but they are not sufficient proof of fragment-backed span
writes.

## Recommended Test-First Sequence

1. Add a default-off behavior guard before runtime work: `formula_plane_default_config_preserves_current_outputs`, `formula_plane_default_config_preserves_graph_counts`, and `formula_plane_enabled_requires_explicit_config`.
2. Add FP6.1 store/resolution/overlay unit tests, then implement only data structures and lookup vocabulary. No graph, scheduler, dirty, or evaluation behavior should change.
3. Add FP6.2 placement tests for exact family identity, row-run/col-run/rect promotion, unsupported fallback reasons, and materialization counters. Then wire placement behind an internal opt-in path.
4. Add compact-authority counter tests before FP6.2 can claim success: `dense_row_run_placement_counts_one_template_one_span`, `unique_formulas_remain_legacy_and_counted`, and `accepted_span_does_not_create_per_placement_vertices_ast_or_edges`.
5. Add FP6.3 region-index unit tests and dirty no-under-approx integration tests. Then wire sidecar dirty projection alongside graph dirty routing, still whole-span only.
6. Add scheduler seam tests before any normal recalc path invokes spans. If the seam is deferred to FP6.5, keep FP6.4 evaluator direct/test-only.
7. Add ComputedWriteBuffer coalescing and RangeView fragment-read tests. Then implement FP6.4 span evaluator writes through the buffer.
8. Add FP6.4 parity tests that run the same synthetic workbook through legacy and FormulaPlane-enabled engines, comparing outputs and counter invariants in the same assertion block.
9. Add manual perf probes only after the parity and counter gates pass. Do not use wall time as an acceptance gate before compact-authority counters are nonzero.

## Phase-Boundary Risks

- FP6.2 could accidentally treat passive scanner `source_template_id` strings as authority keys. Tests must force literal values, mixed anchors, function identity, and sheet binding into the authority key.
- FP6.2 could promote a dense family but still call `set_cell_formula` once per placement. Counter tests must fail if accepted span cells have matching formula vertices, AST roots, or edge rows.
- FP6.1 store code could put exception maps, ASTs, dependency summaries, or indexes inline in `FormulaSpan`. Unit tests and debug-size checks should keep spans as compact arena rows.
- FP6.3 could collapse `SpanDomainIndex`, `SpanDependencyIndex`, and `FormulaOverlayIndex` into one map. This risks wrong authority answers and stale dirty routing; role-specific tests should prevent it.
- FP6.3 exact filtering could be skipped after rect-bucket over-return. Add a test where the bucket returns two spans but only one intersects after exact filtering.
- FP6.3 could under-approximate dirty by implementing partial projection too early. Initial dirty should be whole span unless a projection is proven exact.
- FP6.4 could use the scalar interpreter correctly but write through per-cell computed overlay calls instead of the buffer/fragment substrate. Buffer-spy or counters should fail that path.
- FP6.4 before scheduler seam could evaluate spans in the wrong order relative to legacy formulas. Keep it direct/test-only until the seam tests pass.
- FormulaOverlay and user value overlay can be confused. A value edit inside a span must punch out formula authority and write the value plane; recomputation must not overwrite it.
- Default behavior can drift if the config gate is added late. Default-off tests should be the first integration tests and should remain in the main suite.

## Fixture And Oracle Strategy

- Unit fixtures: pure FormulaPlane data fixtures for stores, placement domains, overlays, indexes, dirty projection, and buffer coalescing. These should not instantiate `Engine` unless the component requires engine types.
- Integration fixtures: dual-engine fixtures using `TestWorkbook`, one legacy/default engine and one opt-in FormulaPlane engine. Compare values, errors, formula lookup, dirty sets, and counter invariants.
- Dependency oracles: use existing graph dependency planning and current `dependency_summary.rs` comparison helpers for no-under-approx checks. Over-approximation is acceptable only when counted.
- Writeback oracles: use `RangeView` reads plus direct fragment/buffer counters. Value equality alone is not enough; tests must also assert the fragment shape and user-overlay precedence.
- Manual perf probes: keep 100k copied row-run, dense col-run, simple rect, mostly unique formulas, unsupported/dynamic fallback, and small-workbook overhead probes as manual or nightly characterization. Acceptance should use counters first, then wall time/RSS only as supporting evidence.

## Mandatory Observability Assertions

Every runtime acceptance test that claims FormulaPlane authority should assert the
relevant counters in addition to values:

- `formula_plane_enabled == true` for opt-in tests; disabled/default tests should show no active span authority.
- `formula_cells_seen > 0`, `accepted_span_cells > 0`, `spans_created > 0`, and `templates_interned > 0` for promoted fixtures.
- `legacy_fallback_cells` and `fallback_reasons` are present and exact for unsupported fixtures.
- `formula_vertices_avoided > 0`, `ast_roots_avoided > 0`, and `edge_rows_avoided > 0` for dense accepted spans.
- `per_placement_formula_vertices_created == 0`, `per_placement_ast_roots_created == 0`, and `per_placement_edge_rows_created == 0` for accepted spans.
- `span_work_item_count <= spans_created` for whole-span scheduling; it must not equal accepted placement count in dense fixtures.
- `span_domain_index_entries`, `span_dependency_index_entries`, and `formula_overlay_index_entries` are separately reported.
- `region_query_candidate_count` and `region_query_exact_filter_drop_count` are asserted on rect-bucket tests.
- `dirty_span_count`, `dirty_whole_span_count`, and `dirty_under_approx_oracle_misses == 0` are asserted for FP6.3.
- `formula_overlay_exception_count` and projection-cache epoch counters are asserted for punchout/effective-domain tests.
- `span_eval_task_count`, `span_eval_placement_count`, `span_eval_ms`, and scalar-interpreter fallback counts are reported for FP6.4.
- `computed_write_buffer_push_count`, `computed_write_buffer_flush_count`, `computed_fragment_shape_counts`, and `computed_fragment_cell_count` are asserted for span writeback.
- `range_view_fragment_read_count` or equivalent is asserted where fragment-backed reads are part of the claim.
- Any lazy legacy materialization escape hatch reports `legacy_materialized_cells` and reason labels.

A dense row-run acceptance fixture should have an assertion shape like:

```text
formula_cells_seen = 100000
accepted_span_cells = 100000
templates_interned = 1
spans_created = 1
span_work_item_count = 1
legacy_fallback_cells = 0
formula_vertices_avoided > 0
ast_roots_avoided > 0
edge_rows_avoided > 0
per_placement_formula_vertices_created = 0
computed_fragment_shape_counts.dense_range >= 1
```

A fallback fixture should assert the opposite authority shape:

```text
accepted_span_cells = 0
legacy_fallback_cells = formula_cells_seen
fallback_reasons.dynamic_or_unsupported > 0
formula_vertices_avoided = 0
```

## Concrete Acceptance Tests And Locations

| Area | Suggested location | Test names |
|---|---|---|
| Runtime stores | `crates/formualizer-eval/src/formula_plane/runtime_store.rs` | `template_store_interns_equivalent_templates_once`, `span_store_allocates_generational_span_ids`, `span_record_stores_ids_domain_state_version_not_ast_or_exceptions` |
| Formula resolution | `crates/formualizer-eval/src/formula_plane/formula_resolution.rs` | `formula_resolution_prefers_overlay_over_span_over_legacy`, `formula_resolution_returns_span_placement_without_legacy_materialization`, `formula_resolution_returns_empty_for_no_formula_authority` |
| FormulaOverlay | `crates/formualizer-eval/src/formula_plane/formula_overlay.rs` | `formula_overlay_masks_span_resolution`, `formula_overlay_epoch_invalidates_projection_cache`, `formula_overlay_projection_excludes_punched_out_placements` |
| Placement | `crates/formualizer-eval/src/engine/tests/formula_plane_placement.rs` | `row_run_same_template_promotes_to_span`, `col_run_same_template_promotes_to_span`, `rect_same_template_promotes_to_span`, `unsupported_dynamic_formula_remains_legacy_with_reason` |
| Materialization counters | `crates/formualizer-eval/src/engine/tests/formula_plane_observability.rs` | `accepted_row_run_avoids_per_placement_vertices_ast_and_edges`, `unique_formulas_remain_legacy_and_counted`, `formula_plane_disabled_counters_stay_zero` |
| Region indexes | `crates/formualizer-eval/src/formula_plane/region_index.rs` | `sheet_region_index_finds_point_interval_rect_and_whole_axis_entries`, `span_domain_index_finds_rect_intersections`, `formula_overlay_index_finds_cell_punchout` |
| Dirty projection | `crates/formualizer-eval/src/formula_plane/dirty.rs` and `crates/formualizer-eval/src/engine/tests/formula_plane_dirty.rs` | `same_row_dependency_edit_marks_candidate_span_dirty_whole`, `absolute_dependency_edit_marks_candidate_span_dirty_whole`, `sidecar_dirty_is_no_under_approx_against_legacy_fixture` |
| Scheduler seam | `crates/formualizer-eval/src/engine/tests/formula_plane_scheduler_seam.rs` | `legacy_precedent_dirty_schedules_one_span_task_not_n_placements`, `span_result_region_dirty_schedules_legacy_dependent`, `span_to_span_dependency_orders_precedent_before_dependent` |
| Span evaluator | `crates/formualizer-eval/src/formula_plane/span_eval.rs` and `crates/formualizer-eval/src/engine/tests/formula_plane_span_eval.rs` | `span_eval_row_run_matches_legacy_outputs`, `span_eval_rect_matches_legacy_outputs`, `span_eval_effective_domain_skips_overlay_punchouts`, `span_eval_does_not_allocate_per_placement_ast_roots_or_edges` |
| Computed writes | actual eval-flush/buffer module plus `crates/formualizer-eval/src/engine/tests/formula_plane_computed_writes.rs` | `computed_write_buffer_coalesces_varying_row_run_to_dense_range_fragment`, `computed_write_buffer_explicit_empty_masks_base_value`, `span_eval_writes_through_computed_write_buffer_not_direct_overlay`, `range_view_reads_flushed_computed_fragments` |

## Proceed/Stop Guidance

Proceed with FP6.1 after adding the store, FormulaResolution, and FormulaOverlay
unit tests. Proceed with FP6.2 only after default-off parity and materialization
counter tests exist. Proceed with FP6.3 only after no-under-approx dirty oracle
tests exist. Proceed with FP6.4 only after ComputedWriteBuffer/fragment tests and
the scheduler-seam boundary decision are in place.

Stop and replan if an implementation claims compact authority while allocating
one formula AST, one formula vertex, or one scalar edge set per accepted span
placement, or if span output bypasses the computed write buffer/fragment path.
