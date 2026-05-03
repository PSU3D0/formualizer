# FP6 Oracle, Fixtures, Observability, And Compact-Authority Proof Shore-Up

Date: 2026-05-03  
Branch: `formula-plane/bridge`  
Scope: report-only shore-up for FormulaPlane parity/oracle fixtures and counters. No production code changes.

## Verdict

FormulaPlane runtime work should not use wall-time wins as its first acceptance
signal. The first acceptance signal must be a combined proof:

```text
legacy outputs match
FormulaPlane authority is actually active
per-placement formula graph materialization did not happen
span results used the contracted computed-write/fragment path
fallbacks are explicit and counted
```

The current source already has useful pieces:

- `Engine::baseline_stats()` and `DependencyGraph::baseline_stats()` expose graph
  vertices, formula vertices, graph edges, dirty vertices, evaluation vertices,
  AST roots, AST nodes, and staged formula counts.
- `formualizer-eval/src/formula_plane/span_counters.rs` has passive candidate
  counters and materialization-avoidable estimates.
- `formualizer-eval/src/formula_plane/diagnostics.rs` has passive authority and
  dependency-summary diagnostics, including under-approximation accounting.
- `formualizer-bench-core/src/bin/run-formualizer-native.rs` already emits engine
  baseline stats into benchmark JSON.
- `formualizer-bench-core/src/bin/scan-formula-templates.rs` already joins scan
  results with runner materialization stats.
- `formualizer-testkit` can generate XLSX fixtures for workbook/loader probes.

But the runtime proof substrate is not present yet. There is no runtime
`FormulaPlaneStats`, no span-eval counters, no sidecar index counters, and this
worktree does not expose the `ComputedWriteBuffer` / overlay-fragment API named
by the active docs. Those must be explicit phase gates before FP6.4 claims
fragment-backed span evaluation.

## 1. Source Reality Check

### 1.1 Existing unit/integration test substrate

Relevant current tests under `crates/formualizer-eval/src/engine/tests/`:

| Existing area | Useful for FP6 oracle work |
|---|---|
| `common.rs` | Helpers for AST refs, graph-truth config, Arrow-enabled config. |
| `formula_overlay_writeback.rs` | Value-plane precedent for formula result writeback through Arrow computed overlay. This is not FormulaOverlay formula authority. |
| `range_dependencies.rs`, `dirty_propagation.rs`, `dirty_propagation_precision.rs`, `striped_dirty_propagation.rs` | Dirty/range routing behavior and graph may-affect oracles. |
| `schedule_cache.rs`, `schedule_integration.rs`, `topo_layers.rs`, `compressed_range_scheduler.rs` | Schedule/cache/layering parity for later span work-item scheduling. |
| `arrow_sparse_compaction.rs`, `overlay_compaction.rs`, `arrow_sparse_structural_ops.rs`, `range_view`-style tests | Value overlay/base precedence and structural/value-store behavior. |
| `engine_action_rollback_615.rs`, `engine_atomic_actions_618.rs`, `transactions.rs` | Rollback/action patterns needed when FormulaOverlay becomes transactional. |
| `arrow_canonical_611.rs` | Formula lookup/writeback and Arrow-canonical behavior examples. |
| `criteria_overlay_parity.rs`, `sumifs_arrow_fastpath.rs`, `countifs_arrow_overlay.rs` | Examples of value equality plus fast-path/overlay parity tests. |
| `indirect.rs`, `offset_dynamic.rs`, `volatile_rng.rs`, `spill_*`, `let_lambda.rs` | Unsupported/dynamic/volatile/spill/local-env fallback fixture sources. |

Good new test modules should be explicit rather than hidden in existing files:

```text
crates/formualizer-eval/src/formula_plane/runtime_store.rs
crates/formualizer-eval/src/formula_plane/formula_resolution.rs
crates/formualizer-eval/src/formula_plane/formula_overlay.rs
crates/formualizer-eval/src/formula_plane/region_index.rs
crates/formualizer-eval/src/formula_plane/dirty.rs
crates/formualizer-eval/src/formula_plane/span_eval.rs
crates/formualizer-eval/src/engine/tests/formula_plane_default_off.rs
crates/formualizer-eval/src/engine/tests/formula_plane_placement.rs
crates/formualizer-eval/src/engine/tests/formula_plane_observability.rs
crates/formualizer-eval/src/engine/tests/formula_plane_dirty.rs
crates/formualizer-eval/src/engine/tests/formula_plane_scheduler_seam.rs
crates/formualizer-eval/src/engine/tests/formula_plane_span_eval.rs
crates/formualizer-eval/src/engine/tests/formula_plane_computed_writes.rs
```

### 1.2 Existing observability substrate

`EngineBaselineStats` currently exposes:

```rust
graph_vertex_count
graph_formula_vertex_count
graph_edge_count
dirty_vertex_count
evaluation_vertex_count
formula_ast_root_count
formula_ast_node_count
staged_formula_count
```

These are the minimum baseline for materialization proof. Runtime FormulaPlane
must add a separate stats surface rather than overloading passive diagnostics:

```rust
pub(crate) struct FormulaPlaneRuntimeStats { ... }
```

It should be read-only and observational, like `EngineBaselineStats`. Collecting
it must not mutate engine, graph, Arrow, or FormulaPlane state.

### 1.3 Existing passive diagnostics

`span_counters.rs` has passive counts such as:

```text
formula_cell_count
repeated_template_count
candidate_formula_run_count
formula_cells_represented_by_runs
estimated_materialization_avoidable_cell_count
candidate_row_block_partition_count
```

`diagnostics.rs` has dependency-summary comparison counts:

```text
exact_match_count
over_approximation_count
under_approximation_count
rejection_count
fallback_reason_histogram
```

These should remain useful scanner/planning oracles, but runtime acceptance tests
must not treat them as runtime authority counters. Passive candidate coverage is
not proof that graph vertices/ASTs/edges were avoided.

### 1.4 Computed-write source mismatch

This branch does not contain discoverable symbols named:

```text
ComputedWriteBuffer
OverlayFragment
DenseRange
RunRange
SparseOffsets
```

The active architecture requires the eval-flush Phase 5 substrate. Therefore:

```text
FP6.4 cannot claim fragment-backed span writes in this worktree until eval-flush
is merged or an explicit integration branch provides those APIs.
```

Unit tests may be specified now, but implementation must gate on the concrete
computed-write API path.

## 2. Fixture Matrix

Use four fixture tiers. Unit tests prove local contracts. Engine integration
tests prove default behavior and opt-in parity. Oracle/parity fixtures compare
legacy/default to FormulaPlane-enabled engines and assert counters. Manual/nightly
probes characterize scale after counters prove authority.

### 2.1 Unit fixtures

| Fixture | Component | Purpose | Required assertions |
|---|---|---|---|
| `dense_row_run_domain_10` | `PlacementDomain`, `SpanDomainIndex` | Basic row-run iteration/lookup. | 10 placements, one span, no overlay semantics applied. |
| `dense_col_run_domain_10` | `PlacementDomain`, `SpanDomainIndex` | Period-style horizontal/column-run coverage. | 10 placements, deterministic order. |
| `rect_domain_4x3` | `PlacementDomain`, `SpanDomainIndex` | Rect iteration and exact filtering. | 12 placements, no under-return. |
| `formula_overlay_single_punchout` | `FormulaOverlay`, `FormulaOverlayIndex` | One edit masks one placement. | Effective domain excludes punchout; overlay epoch increments. |
| `formula_overlay_region_paste` | `FormulaOverlay`, `FormulaOverlayIndex` | Bulk paste/clear punchout path. | One region query, counted overlay entries, no span splitting required. |
| `span_dependency_same_row` | `SpanDependencyIndex`, `DirtyProjection` | Same-row precedent discovery. | Candidate found; FP6.3 whole-span dirty initially. |
| `span_dependency_absolute_ref` | `SpanDependencyIndex`, `DirtyProjection` | Absolute cell dependency. | Candidate found; whole span dirty. |
| `rect_bucket_overreturn` | `SheetRegionIndex` | Bucket over-return exact filter. | Candidate count > final count; final set exact. |
| `unsupported_dynamic_summary` | dependency adapter | Reject dynamic/volatile/opaque authority. | No index entry; fallback reason counted. |
| `computed_write_run_dense_sparse_shapes` | eval-flush seam | Coalescing contract before span eval. | Fragment shape counters match expected outputs. |

Suggested locations:

```text
crates/formualizer-eval/src/formula_plane/region_index.rs
crates/formualizer-eval/src/formula_plane/dirty.rs
crates/formualizer-eval/src/formula_plane/formula_overlay.rs
crates/formualizer-eval/src/formula_plane/span_eval.rs
crates/formualizer-eval/src/engine/tests/formula_plane_computed_writes.rs
```

### 2.2 Engine integration fixtures

| Fixture | Shape | Purpose | Suggested location |
|---|---|---|---|
| `formula_plane_default_off_preserves_outputs` | small workbook with formulas | Default behavior guard. | `engine/tests/formula_plane_default_off.rs` |
| `formula_plane_default_off_preserves_graph_counts` | dense copied formulas | Enabling code without opt-in must not alter graph materialization. | `engine/tests/formula_plane_default_off.rs` |
| `dense_copied_arithmetic_row_run_100` | `C_r = A_r + B_r` | First accepted row-run parity and counters. | `engine/tests/formula_plane_span_eval.rs` |
| `same_row_dependencies_100` | `C_r = A_r * B_r` | Dirty routing same-row source changes. | `engine/tests/formula_plane_dirty.rs` |
| `absolute_ref_fanout_100` | `C_r = A_r * $F$1` | Whole-span dirty from absolute edit. | `engine/tests/formula_plane_dirty.rs` |
| `fixed_reduction_100` | `C_r = SUM($A$1:$A$10)` | Conservative whole-span policy; may be fallback until range summaries exist. | `engine/tests/formula_plane_placement.rs` |
| `prefix_range_100` | `C_r = SUM(A$1:A_r)` | Future partial projection; initial fallback or conservative whole if exact. | `engine/tests/formula_plane_dirty.rs` |
| `cross_sheet_static_ref_100` | `Calc!C_r = Inputs!A_r + 1` | Stable `SheetId` and cross-sheet parity. | `engine/tests/formula_plane_span_eval.rs` |
| `unsupported_dynamic_fallback` | `INDIRECT`, `OFFSET`, volatile, spill/local-env | Fallback reason exactness. | `engine/tests/formula_plane_fallback.rs` |
| `edit_value_inside_span` | value edit into accepted span cell | FormulaOverlay punchout and value overlay precedence. | `engine/tests/formula_plane_edits.rs` |
| `clear_inside_span` | clear accepted span cell | Cleared formula authority and explicit empty semantics. | `engine/tests/formula_plane_edits.rs` |
| `paste_block_over_span` | paste values/formulas over subregion | Bulk punchouts and local re-pattern hooks. | `engine/tests/formula_plane_edits.rs` |
| `insert_rows_before_span` | structural shift | Shift or demote and rebuild indexes. | `engine/tests/formula_plane_structural.rs` |
| `insert_rows_inside_span` | structural edit inside domain | Hole/split/demote policy. | `engine/tests/formula_plane_structural.rs` |
| `small_workbook_overhead` | 5-20 formulas | Opt-in overhead and fallback correctness. | `engine/tests/formula_plane_observability.rs` |

### 2.3 Oracle/parity fixtures

These compare two engines in the same test:

```text
legacy/default engine
FormulaPlane-enabled engine
```

For each fixture, compare:

```text
values and errors for all observed cells
formula lookup results or virtual formula text where relevant
dirty/recalc effects after edits
FormulaPlane runtime counters
EngineBaselineStats deltas
computed-write/fragment counters when span eval runs
fallback/demotion reason maps
```

Recommended parity fixtures:

| Fixture | Rows | Formula | Primary proof |
|---|---:|---|---|
| `oracle_dense_copied_arithmetic` | 100 / 10_000 | `C_r = A_r + B_r` | Values equal; one template/span; avoided materialization. |
| `oracle_same_row_edit_recalc` | 100 / 10_000 | `C_r = A_r * B_r` | Edit `A50`; dirty candidate includes span; outputs equal. |
| `oracle_absolute_ref_recalc` | 100 / 10_000 | `C_r = A_r * $F$1` | Edit `F1`; whole span dirty; outputs equal. |
| `oracle_fixed_reduction_or_fallback` | 100 | `C_r = SUM($A$1:$A$10)` | Either safe whole-span authority or exact fallback count. |
| `oracle_prefix_range_or_fallback` | 100 | `C_r = SUM(A$1:A_r)` | No under-approx; fallback until exact projection exists. |
| `oracle_cross_sheet_static` | 100 | `Calc!C_r = Inputs!A_r + 1` | Stable sheet identity and output parity. |
| `oracle_unsupported_dynamic` | 50 | `INDIRECT`, `OFFSET`, `RAND`, spill/local env | `accepted_span_cells = 0`; legacy outputs equal. |
| `oracle_edit_punchout` | 100 | edit/clear/paste inside span | FormulaOverlay and value overlay parity. |
| `oracle_structural_common` | 50 | insert/delete rows/cols | Shift/demote parity and index rebuild counters. |

### 2.4 Manual/nightly perf probes

Manual/nightly probes should run only after parity and counters pass. They should
write JSON/Markdown reports under `target/` and must include the same compact
proof counters as integration tests.

| Probe | Rows/shape | Purpose | Suggested binary/report |
|---|---:|---|---|
| `formula_plane_100k_copied_arithmetic` | 100k row-run | M1 headline copied formula. | `formualizer-bench-core` probe, `target/formula-plane-runtime/` |
| `formula_plane_100k_same_row_recalc` | 100k, repeated edits | Dirty/recalc materialization win. | `target/formula-plane-runtime/recalc-100k.json` |
| `formula_plane_finance_recalc` | 50k+, multi-column formulas | Finance-shaped workload. | `target/formula-plane-runtime/finance-recalc.md` |
| `formula_plane_small_overhead` | 10-100 cells | Opt-in overhead guard. | `target/formula-plane-runtime/small-overhead.json` |
| `formula_plane_mostly_unique` | many unique formulas | Fallback overhead and no false promotion. | `target/formula-plane-runtime/unique-fallback.json` |
| `formula_plane_unsupported_dynamic` | dynamic/volatile/spill | Fallback/counter exactness. | `target/formula-plane-runtime/unsupported.json` |
| `formula_plane_loader_shared_formula` | XLSX shared groups | Later FP6.11 loader metadata bridge. | `scan-formula-templates` plus runtime probe. |

Manual probes can reuse `formualizer-testkit::write_workbook` and existing
`run-formualizer-native`/`scan-formula-templates` patterns. Wall time and RSS are
supporting evidence only; a probe without compact-authority counters is not an
acceptance result.

## 3. Oracle Strategy

### 3.1 Dual-engine parity harness

Create a small internal test harness, not a new public API:

```rust
struct FormulaPlaneOracleFixture {
    name: &'static str,
    rows: u32,
    setup: fn(&mut Engine<TestWorkbook>),
    edits: &'static [OracleEdit],
    observed_cells: &'static [ObservedCell],
    expected_authority: ExpectedAuthorityShape,
}
```

Execution shape:

```text
1. Build legacy/default engine.
2. Build FormulaPlane-enabled engine from the same fixture.
3. Assert FormulaPlane disabled/default mode remains identical to legacy.
4. Evaluate both.
5. Compare observed values/errors.
6. Compare formula lookup for observed formula cells.
7. Assert FormulaPlane counters match expected authority shape.
8. Apply edits in identical order.
9. Re-evaluate both.
10. Compare outputs, dirty/recalc counters, FormulaOverlay counters, and graph/materialization counters.
```

The harness should be deterministic and small enough for normal tests. Large
100k/finance probes stay ignored/manual/nightly.

### 3.2 Value and error comparison

Compare observable results, not internal representation, for semantic parity:

```text
LiteralValue equality for values
ExcelError kind for errors
blank vs explicit Empty according to current public read semantics
formula text or canonical AST rendering where formula lookup is part of the fixture
```

A FormulaPlane-enabled engine may have fewer graph vertices and different
internal FormulaResolution, but observable values/errors must match legacy.

### 3.3 Formula lookup comparison

For formula lookup fixtures:

```text
legacy: graph formula AST/text
FormulaPlane: virtual relocated span formula unless overlay/legacy-owned punchout
```

Comparison should use canonical formula text or canonical AST equivalence,
not pointer identity. Do not materialize a legacy graph vertex just to satisfy the
comparison unless the fixture explicitly tests lazy materialization.

### 3.4 Dirty and dependency oracle

Dirty correctness should use two layers:

1. Legacy/default graph engine as output oracle after edit/recalc.
2. Dependency-summary comparison helpers as no-under-approx checks where
   available.

Assertions:

```text
dirty_under_approx_oracle_misses = 0
sidecar may over-dirty only when counted
legacy outputs == FormulaPlane outputs after each edit
```

### 3.5 Counter oracle

Every authority fixture should assert counters in the same assertion block as
value equality. This prevents false wins where a test passes because it silently
fell back to legacy.

## 4. Mandatory Runtime Counters

Runtime counters should be read-only and grouped by subsystem. Passive
FormulaPlane diagnostics should not be reused as runtime authority counters.

### 4.1 Configuration and phase counters

```text
formula_plane_enabled
formula_plane_runtime_epoch
formula_plane_runtime_mode
formula_plane_default_off_bypass_count
```

Default tests assert:

```text
formula_plane_enabled = false
accepted_span_cells = 0
span_eval_task_count = 0
formula_plane_default_off_bypass_count >= 1
legacy/default outputs unchanged
```

### 4.2 Placement and authority counters

```text
formula_cells_seen
candidate_formula_cells
accepted_span_cells
legacy_fallback_cells
templates_interned
spans_created
spans_active
span_cells_covered
span_cells_effective
span_cells_masked_by_intrinsic_mask
span_cells_masked_by_formula_overlay
span_rect_count
span_row_run_count
span_col_run_count
span_sparse_count
fallback_reasons{reason}
demotion_reasons{reason}
legacy_materialized_cells
```

Dense accepted row-run shape:

```text
formula_cells_seen = N
candidate_formula_cells = N
accepted_span_cells = N
legacy_fallback_cells = 0
templates_interned = 1
spans_created = 1
span_row_run_count = 1
span_cells_covered = N
span_cells_effective = N
fallback_reasons = {}
legacy_materialized_cells = 0
```

Fallback fixture shape:

```text
formula_cells_seen = N
accepted_span_cells = 0
legacy_fallback_cells = N
spans_created = 0
fallback_reasons.dynamic_or_unsupported > 0
legacy_materialized_cells = 0 unless the fixture explicitly requests materialization
```

### 4.3 Graph/materialization counters

Use existing `EngineBaselineStats` as the baseline and add FormulaPlane-specific
created/avoided counters.

```text
baseline_graph_formula_vertex_count
formula_plane_graph_formula_vertex_count
formula_vertices_avoided
formula_vertices_created_for_accepted_spans
per_placement_formula_vertices_created
baseline_formula_ast_root_count
formula_plane_formula_ast_root_count
ast_roots_avoided
per_placement_ast_roots_created
baseline_graph_edge_count
formula_plane_graph_edge_count
edge_rows_avoided
per_placement_edge_rows_created
```

Accepted dense row-run assertions:

```text
formula_vertices_avoided > 0
ast_roots_avoided > 0
edge_rows_avoided > 0
formula_vertices_created_for_accepted_spans <= spans_created
per_placement_formula_vertices_created = 0
per_placement_ast_roots_created = 0
per_placement_edge_rows_created = 0
```

If a future scheduler uses proxy vertices, proxy counts must be reported
separately:

```text
span_proxy_vertex_count <= spans_created
span_proxy_edge_count = O(spans + span deps), not O(placements)
```

### 4.4 Index and dirty counters

```text
span_domain_index_entries
span_dependency_index_entries
formula_overlay_index_entries
region_query_count
region_query_candidate_count
region_query_exact_filter_drop_count
region_query_final_candidate_count
index_rebuild_count
index_incremental_update_count
index_stale_epoch_count
dirty_span_count
dirty_whole_span_count
dirty_interval_count
dirty_sparse_count
dirty_effective_placement_count
dirty_under_approx_oracle_misses
dirty_over_approx_count
```

Rect-bucket over-return shape:

```text
region_query_candidate_count > region_query_final_candidate_count
region_query_exact_filter_drop_count > 0
dirty_under_approx_oracle_misses = 0
```

FP6.3 whole-span dirty shape:

```text
dirty_span_count > 0
dirty_whole_span_count = dirty_span_count
dirty_interval_count = 0
dirty_sparse_count = 0
dirty_under_approx_oracle_misses = 0
```

### 4.5 FormulaOverlay counters

```text
formula_overlay_entry_count
formula_overlay_value_override_count
formula_overlay_formula_override_count
formula_overlay_cleared_count
formula_overlay_legacy_owned_count
formula_overlay_unsupported_count
formula_overlay_projection_cache_hit_count
formula_overlay_projection_cache_miss_count
formula_overlay_epoch
formula_overlay_projection_epoch
bulk_overlay_operation_count
```

Edit/punchout shape:

```text
formula_overlay_value_override_count = edited_value_cells
span_cells_masked_by_formula_overlay >= edited_value_cells
user value overlay still wins over computed overlay
legacy outputs == FormulaPlane outputs after recalc
```

### 4.6 Span eval and computed-write counters

```text
span_work_item_count
span_eval_task_count
span_eval_placement_count
span_eval_effective_placement_count
span_eval_scalar_interpreter_count
span_eval_fallback_count
span_eval_error_count
span_eval_ms
computed_write_buffer_push_count
computed_write_buffer_flush_count
computed_write_buffer_cell_count
computed_fragment_shape_counts.dense_range
computed_fragment_shape_counts.run_range
computed_fragment_shape_counts.sparse_offsets
computed_fragment_cell_count
computed_fragment_estimated_bytes
computed_write_direct_point_fallback_count
computed_write_direct_overlay_bypass_count
range_view_fragment_read_count
range_view_zip_select_fallback_count
```

Accepted constant row-run shape:

```text
span_eval_task_count = 1
span_eval_placement_count = N
computed_write_buffer_push_count = N
computed_write_buffer_flush_count >= 1
computed_fragment_shape_counts.run_range >= 1
computed_write_direct_overlay_bypass_count = 0
range_view_fragment_read_count > 0 when readback is asserted
```

Accepted varying row-run shape:

```text
span_eval_task_count = 1
span_eval_placement_count = N
computed_fragment_shape_counts.dense_range >= 1
computed_write_direct_overlay_bypass_count = 0
```

Sparse/punchout shape:

```text
span_eval_effective_placement_count = N - punchouts
computed_fragment_shape_counts.sparse_offsets >= 1 or exact cheaper point fallback counted
computed_write_direct_overlay_bypass_count = 0
```

## 5. Exact Assertion Shapes

### 5.1 Default-off guard

```text
legacy_values == default_values
legacy_stats == default_stats for graph/materialization-sensitive fields
formula_plane_enabled = false
accepted_span_cells = 0
span_eval_task_count = 0
computed_write_buffer_push_count = 0
formula_overlay_entry_count = 0
```

This must be the first engine integration gate. FormulaPlane code can exist, but
current public/default behavior must not change.

### 5.2 Dense copied arithmetic accepted fixture

For `N = 100` in normal tests, and `N = 100_000` in manual probes:

```text
formula_cells_seen = N
accepted_span_cells = N
templates_interned = 1
spans_created = 1
span_row_run_count = 1
legacy_fallback_cells = 0
span_work_item_count = 1 after scheduler seam exists
formula_vertices_avoided >= N - allowed_proxy_vertices
ast_roots_avoided >= N - 1
edge_rows_avoided > 0
per_placement_formula_vertices_created = 0
per_placement_ast_roots_created = 0
per_placement_edge_rows_created = 0
computed_fragment_cell_count = N
computed_write_direct_overlay_bypass_count = 0
legacy outputs == FormulaPlane outputs
```

### 5.3 Unsupported/dynamic fallback fixture

```text
formula_cells_seen = N
accepted_span_cells = 0
legacy_fallback_cells = N
spans_created = 0
span_eval_task_count = 0
fallback_reasons.dynamic_or_unsupported > 0
formula_vertices_avoided = 0
per_placement_formula_vertices_created = 0 for FormulaPlane accepted spans
legacy outputs == FormulaPlane outputs
```

This fixture proves safe non-optimization, not speed.

### 5.4 Edit/punchout fixture

```text
initial accepted_span_cells = N
value edit count = K
formula_overlay_value_override_count = K
span_cells_masked_by_formula_overlay >= K
span_eval_effective_placement_count = N - K for full-span recalc
user overlay values visible after recalc
computed overlay does not resurface under user edits
legacy outputs == FormulaPlane outputs
```

### 5.5 Dirty no-under-approx fixture

```text
changed_region_count > 0
span_dependency_index_entries > 0
region_query_candidate_count >= region_query_final_candidate_count
region_query_exact_filter_drop_count counted when over-returning fixture is used
dirty_span_count > 0
dirty_under_approx_oracle_misses = 0
legacy outputs == FormulaPlane outputs after edit/recalc
```

### 5.6 Structural fallback fixture

For M1/M2 unsupported structural cases, demotion is acceptable:

```text
structural_edit_count = 1
span_demoted_count >= affected_spans unless exact transform is implemented
index_rebuild_count >= 1 or index_stale_epoch_count > 0 followed by rebuild
legacy_fallback_cells increases by demoted effective placements
stale_index_query_count = 0 after rebuild
legacy outputs == FormulaPlane outputs
```

## 6. Where To Implement Tests And Reports

### 6.1 Unit tests

```text
crates/formualizer-eval/src/formula_plane/runtime_store.rs
crates/formualizer-eval/src/formula_plane/formula_resolution.rs
crates/formualizer-eval/src/formula_plane/formula_overlay.rs
crates/formualizer-eval/src/formula_plane/region_index.rs
crates/formualizer-eval/src/formula_plane/dirty.rs
crates/formualizer-eval/src/formula_plane/span_eval.rs
```

Keep these tests engine-free unless they need engine types like `SheetId` or
`VertexId`. They should be fast and run under:

```bash
cargo test -p formualizer-eval formula_plane --quiet
```

### 6.2 Engine integration tests

Add dedicated modules and register them in `engine/tests/mod.rs`:

```text
formula_plane_default_off.rs
formula_plane_placement.rs
formula_plane_observability.rs
formula_plane_dirty.rs
formula_plane_scheduler_seam.rs
formula_plane_span_eval.rs
formula_plane_computed_writes.rs
formula_plane_edits.rs
formula_plane_structural.rs
formula_plane_fallback.rs
```

These should use `TestWorkbook` for in-memory deterministic fixtures. Keep normal
suite sizes small, e.g. 10, 50, 100, 1_000 rows depending on test cost.

### 6.3 Testkit/XLSX fixtures

Use `formualizer-testkit` for XLSX fixture generation when loader/shared-formula
or workbook IO behavior is part of the test. Do not require XLSX IO for unit or
core engine parity tests.

Suggested future helpers:

```text
formualizer_testkit::formula_plane::dense_copied_arithmetic_xlsx(rows)
formualizer_testkit::formula_plane::same_row_deps_xlsx(rows)
formualizer_testkit::formula_plane::cross_sheet_static_xlsx(rows)
formualizer_testkit::formula_plane::unsupported_dynamic_xlsx(rows)
```

These can be added later when loader/FP6.11 work starts.

### 6.4 Benchmark/manual reports

Reuse existing patterns:

```text
crates/formualizer-bench-core/src/bin/run-formualizer-native.rs
crates/formualizer-bench-core/src/bin/scan-formula-templates.rs
crates/formualizer-bench-core/src/bin/generate-corpus.rs
```

Add future FormulaPlane runtime probes only after unit/integration gates pass:

```text
crates/formualizer-bench-core/src/bin/probe-formula-plane-runtime.rs
crates/formualizer-bench-core/src/bin/probe-formula-plane-recalc.rs
```

Suggested artifact paths:

```text
target/formula-plane-runtime/<git-sha>/copied-100k.json
target/formula-plane-runtime/<git-sha>/copied-100k.md
target/formula-plane-runtime/<git-sha>/finance-recalc-50k-10.json
target/formula-plane-runtime/<git-sha>/finance-recalc-50k-10.md
```

Reports must include compact-authority counters, not only timings.

## 7. Detecting Accidental Per-Placement Materialization

### 7.1 Graph vertices

Use `EngineBaselineStats` before and after enabling FormulaPlane for the same
fixture.

Failure signals:

```text
graph_formula_vertex_count ~= accepted_span_cells
formula_vertices_created_for_accepted_spans > allowed proxy/span count
per_placement_formula_vertices_created > 0
```

For first accepted row-run authority, allowed formula graph materialization should
be:

```text
0 per-placement formula vertices
0 or 1 proxy/work item records, counted separately if introduced
```

### 7.2 AST roots and AST nodes

Failure signals:

```text
formula_ast_root_count ~= accepted_span_cells
per_placement_ast_roots_created > 0
formula_ast_node_count scales linearly with accepted placements when template count is constant
```

Required proof:

```text
templates_interned = 1 for copied row-run
formula_ast_root_count increase <= templates_interned + allowed overrides/proxies
```

### 7.3 Graph edges

Failure signals:

```text
graph_edge_count scales with accepted placements for a span fixture
per_placement_edge_rows_created > 0
```

Required proof:

```text
edge_rows_avoided > 0
span_dependency_index_entries = O(spans * precedent_summary_entries)
```

Do not hide graph edges by removing necessary legacy downstream edges. Span
result regions must still dirty downstream legacy formulas through graph/range
routing or a defined sidecar/proxy seam.

### 7.4 Direct/non-fragment computed writes

Failure signals:

```text
computed_write_direct_overlay_bypass_count > 0
computed_write_buffer_push_count = 0 while span_eval_placement_count > 0
computed_fragment_cell_count = 0 for dense accepted span output
range_view_fragment_read_count = 0 in a fragment-read assertion fixture
```

Until eval-flush Phase 5 APIs are present in this branch, these counters/tests
should be treated as a blocking prerequisite for FP6.4 normal runtime authority.

### 7.5 Hidden fallback

Failure signals:

```text
values match but accepted_span_cells = 0 in an accepted fixture
values match but legacy_fallback_cells = formula_cells_seen without an expected fallback reason
fallback_reasons missing or total fallback count does not sum to legacy_fallback_cells
legacy_materialized_cells > 0 without materialization_request reason
```

Every fixture should declare its expected authority shape before evaluation.

## 8. Phase Gate Checklist

### FP6.1 - Core stores/handles

Required before proceeding:

```text
store/resolution/overlay unit tests pass
runtime stats struct exists or test-only placeholder exists
FormulaPlane stats collection is read-only
FormulaPlane default-off integration test passes
no Engine public/default behavior changes
no graph dirty/scheduler/evaluator changes
passive FormulaRunStore counters are not reused as runtime authority counters
```

Minimum counter assertions:

```text
formula_plane_enabled = false by default
accepted_span_cells = 0 by default
formula_plane_runtime_epoch increments on store mutation in unit tests
```

### FP6.2 - Authority-grade placement

Required before claiming placement authority:

```text
exact canonical template grouping tests pass
fallback reason tests pass
row-run/col-run/rect placement tests pass
materialization counter tests exist before graph-build path changes
accepted fixture asserts one template + one span for dense copied formula
unsupported fixture asserts accepted_span_cells = 0 and exact fallback reasons
```

Minimum counter assertions:

```text
formula_cells_seen = N
accepted_span_cells = N for accepted row-run
legacy_fallback_cells = 0 for accepted row-run
spans_created = 1
templates_interned = 1
per_placement_formula_vertices_created = 0
per_placement_ast_roots_created = 0
per_placement_edge_rows_created = 0
```

If the phase has not yet bypassed graph materialization, it must not claim
runtime compact-authority wins. It may claim exact placement discovery only.

### FP6.3 - Region indexes and dirty routing

Required before graph-adjacent dirty integration:

```text
SpanDomainIndex/SpanDependencyIndex/FormulaOverlayIndex counters exist separately
no-under-return tests pass for point, row interval, col interval, rect bucket, whole axis
rect over-return exact-filter tests pass
stale epoch/rebuild tests pass
changed-region extraction tests exist for value edit, formula edit, clear, paste
sidecar dirty oracle reports dirty_under_approx_oracle_misses = 0
no scheduler/evaluator behavior changes except test-only plumbing
```

Minimum counter assertions:

```text
span_dependency_index_entries > 0 for accepted dirty fixtures
region_query_candidate_count >= region_query_final_candidate_count
dirty_whole_span_count = dirty_span_count for FP6.3 initial policy
dirty_under_approx_oracle_misses = 0
```

### FP6.4 - Span evaluator and computed writes

Required before normal recalc invokes span tasks:

```text
computed-write buffer/fragment API is present in this branch
computed-write coalescing tests pass
RangeView fragment-read tests pass
span evaluator parity tests pass as direct/test-only first
scheduler seam tests pass or evaluator remains direct/test-only
span eval writes through buffer; direct overlay bypass counter is zero
legacy outputs match FormulaPlane outputs for accepted fixtures
```

Minimum counter assertions:

```text
span_eval_task_count > 0
span_eval_placement_count = accepted effective placement count
computed_write_buffer_push_count = span_eval_effective_placement_count
computed_fragment_cell_count = span_eval_effective_placement_count
computed_write_direct_overlay_bypass_count = 0
per_placement_formula_vertices_created = 0
per_placement_ast_roots_created = 0
per_placement_edge_rows_created = 0
```

### Default-off beta

Required before broader fixture testing:

```text
explicit opt-in config only
default-off tests remain in normal suite
fixture matrix covers accepted, fallback, edits, dirty, cross-sheet, small-overhead
manual probes produce JSON/Markdown with compact-authority counters
fallback/demotion reason taxonomy is stable enough for assertions
```

Minimum beta acceptance:

```text
all beta fixtures match legacy outputs
accepted fixtures prove compact authority with counters
fallback fixtures prove safe non-optimization with counters
manual 100k/finance probes include timing, RSS where available, and all compact counters
```

## 9. Recommended Test Naming Inventory

Use these as concrete names for implementation briefs.

### Default/off and observability

```text
formula_plane_default_config_preserves_current_outputs
formula_plane_default_config_preserves_graph_counts
formula_plane_enabled_requires_explicit_config
formula_plane_stats_collection_is_read_only
accepted_row_run_reports_compact_authority_counters
unsupported_formulas_report_exact_fallback_counters
```

### Placement/materialization

```text
dense_row_run_placement_counts_one_template_one_span
col_run_placement_counts_one_template_one_span
rect_placement_counts_one_template_one_span
accepted_span_does_not_create_per_placement_vertices_ast_or_edges
unique_formulas_remain_legacy_and_counted
dynamic_formula_remains_legacy_with_reason
```

### Oracle/parity

```text
oracle_dense_copied_arithmetic_matches_legacy
oracle_same_row_edit_recalc_matches_legacy
oracle_absolute_ref_edit_recalc_matches_legacy
oracle_fixed_reduction_fallback_or_whole_span_matches_legacy
oracle_prefix_range_fallback_or_projection_matches_legacy
oracle_cross_sheet_static_refs_match_legacy
oracle_unsupported_dynamic_fallback_matches_legacy
oracle_edit_punchout_matches_legacy_after_recalc
oracle_structural_demote_or_transform_matches_legacy
```

### Writeback/fragment proof

```text
span_eval_constant_outputs_emit_run_fragment
span_eval_varying_outputs_emit_dense_fragment
span_eval_sparse_effective_domain_emits_sparse_or_counted_point_fallback
span_eval_explicit_empty_masks_base_value
span_eval_user_overlay_precedence_survives_fragment_flush
span_eval_writes_through_computed_write_buffer_not_direct_overlay
range_view_reads_span_eval_computed_fragments
```

### Manual probes

```text
probe_formula_plane_copied_100k_reports_compact_authority
probe_formula_plane_same_row_recalc_100k_reports_dirty_and_fragments
probe_formula_plane_finance_recalc_reports_parity_and_compact_authority
probe_formula_plane_small_workbook_overhead_reports_no_false_promotion
probe_formula_plane_unsupported_fallback_reports_zero_accepted_cells
```

## 10. Non-Goals

- Do not use wall time or RSS as the first acceptance criterion for FP6.1-FP6.4.
- Do not create public/runtime metrics APIs before internal counters and tests are
  proven.
- Do not promote passive diagnostic counters to runtime authority counters.
- Do not require XLSX loader/shared-formula metadata for M1 runtime authority.
- Do not require span-aware function kernels for scalar span-eval parity.
- Do not put long-running 100k/finance probes in the normal test suite.
- Do not treat exact output equality alone as proof that FormulaPlane ran.
- Do not hide fallback/materialization behind successful values.

## 11. Circuit Breakers

Stop and replan immediately if any implementation or test plan does this:

- Claims FormulaPlane compact authority without asserting `accepted_span_cells`,
  `spans_created`, `templates_interned`, avoided vertices/ASTs/edges, and
  fallback reasons.
- Claims a performance win while `per_placement_formula_vertices_created`,
  `per_placement_ast_roots_created`, or `per_placement_edge_rows_created` is
  nonzero for accepted spans.
- Uses passive `SpanPartitionCounters` or `FormulaRunStoreBuildReport` as proof
  of runtime materialization avoidance.
- Lets an accepted span fixture pass only because it silently fell back to legacy.
- Allows default/public behavior changes without an explicit opt-in test.
- Evaluates span tasks in normal recalc before scheduler/proxy ordering tests
  pass.
- Writes span results through direct Arrow base mutation, user value overlay,
  graph value cache, or uncounted per-cell computed overlay calls.
- Proceeds with FP6.4 before the eval-flush computed-write/fragment API is
  present and tested in this branch.
- Omits exact-filter/drop counters for over-returning region-index tests.
- Adds broad span-aware function kernels before scalar span evaluator parity and
  compact-authority counters pass.
- Suppresses fallback/demotion/materialization counters from reports because
  outputs happen to match.

## 12. Recommended Immediate Additions To Active Docs

When synthesizing this report into the active plan, add:

1. A `FormulaPlaneRuntimeStats` requirement in FP6.1 with read-only collection
   semantics matching `EngineBaselineStats`.
2. A default-off test gate before any runtime behavior changes.
3. A dense accepted fixture assertion block and unsupported fallback assertion
   block in the implementation plan.
4. A hard FP6.4 prerequisite that names the concrete computed-write/fragment API
   path after eval-flush is merged.
5. A manual-probe rule: timing/RSS reports are valid only when they include
   compact-authority counters and legacy parity status.

These additions should keep implementation agents from proving only that the
workbook still calculates, when the real question is whether FormulaPlane
calculated it with compressed authority.
