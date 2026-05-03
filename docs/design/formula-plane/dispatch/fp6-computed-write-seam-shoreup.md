# FP6 Computed-Write Seam Shore-Up

Date: 2026-05-03  
Branch: `formula-plane/bridge`  
Scope: design shore-up only; no production code changes.

## Verdict

**WARN / prerequisite-gated for FP6.4.**

The FormulaPlane span evaluator must use the eval-flush result substrate:

```text
ComputedWriteBuffer
  -> coalesced chunk plan
  -> computed overlay fragments
  -> RangeView fragment-aware reads
```

The current `formula-plane/bridge` worktree does **not** contain that substrate.
A source search found no symbols named:

```text
ComputedWriteBuffer
ComputedWrite
OverlayFragment
DenseRange
RunRange
SparseOffsets
OverlayScalar
OverlayCascade
plan_owned_computed_write_coalescing
flush_computed_write_buffer
```

Current source still has map-backed computed overlays and per-cell mirror paths.
Therefore FP6.4 must not claim fragment-backed span evaluation on this branch
until PR #95 / eval-flush Phase 5 is merged or explicitly integrated into the
FormulaPlane branch.

A test-only span evaluator can be designed now, but any production or benchmark
claim must be gated on the concrete computed-write API being present.

## 1. Current source reality check

### 1.1 Present in this branch

`crates/formualizer-eval/src/arrow_store/mod.rs` contains:

```rust
pub struct ColumnChunk {
    pub overlay: Overlay,
    pub computed_overlay: Overlay,
}

pub struct Overlay {
    map: HashMap<usize, OverlayValue>,
    estimated_bytes: usize,
}
```

`Overlay` currently supports point operations only:

```rust
get(off) -> Option<&OverlayValue>
set(off, OverlayValue) -> isize
remove(off) -> isize
clear() -> usize
any_in_range(range) -> bool
iter()
```

The value cascade is already correct and load-bearing:

```text
ColumnChunk.overlay -> ColumnChunk.computed_overlay -> base lanes
```

`crates/formualizer-eval/src/engine/eval.rs` contains point-oriented helpers:

```rust
set_computed_overlay_cell_raw(sheet, row, col, value)
mirror_value_to_computed_overlay(sheet, row, col, value)
read_computed_overlay_cell(sheet, row, col)
clear_all_computed_overlays()
compact_all_computed_overlays()
```

`mirror_value_to_computed_overlay`:

- writes one cell at a time;
- converts `LiteralValue` to `OverlayValue`;
- inserts into `ch.computed_overlay.set(in_off, ov)`;
- updates `computed_overlay_bytes_estimate`;
- may compact overlays when `EvalConfig.max_overlay_memory_bytes` is exceeded.

Existing tests prove current point-overlay behavior:

```text
engine/tests/formula_overlay_writeback.rs
engine/tests/spill_overlay_writeback.rs
engine/tests/hardening_503.rs
engine/tests/arrow_canonical_606.rs
engine/tests/arrow_canonical_601.rs
```

Those tests are useful value-precedence oracles, but they are not proof of
fragment-backed FormulaPlane result writes.

### 1.2 Absent in this branch

The active docs reference eval-flush Phase 5 concepts, but this branch lacks the
implementation:

```text
ComputedWriteBuffer absent
ComputedWrite::{Cell, Rect} absent
computed-write coalescing plan absent
OverlayFragment absent
fragment variants DenseRange / RunRange / SparseOffsets absent
fragment-aware OverlayCascade absent
fragment-aware RangeView selector stats absent
```

`crates/formualizer-eval/Cargo.toml` still pins Arrow to the 56.x series. The
Phase 5 eval-flush branch is expected to carry the Arrow 58.2.0 bump and the
fragment substrate. This branch should not reimplement or fork that substrate.
It should merge/rebase onto it before FP6.4 starts.

### 1.3 Current RangeView posture

`crates/formualizer-eval/src/engine/range_view.rs` currently resolves overlays by
checking:

```rust
ch.overlay.any_in_range(seg_range)
ch.computed_overlay.any_in_range(seg_range)
```

and then building per-row overlay masks with:

```rust
ch.overlay.get(rel_off + i).or_else(|| ch.computed_overlay.get(rel_off + i))
```

This preserves user overlay precedence and explicit overlay masking. It does not
contain the Phase 4 fragment-aware direct dense/run/sparse selector paths named
by the active runtime docs.

## 2. Required API seam for FormulaPlane span eval

FormulaPlane should not write directly to `ArrowSheet`, `ColumnChunk`,
`computed_overlay`, graph value caches, user overlays, or base lanes.

The span evaluator should depend on a narrow writer interface under
`crates/formualizer-eval/src/formula_plane/` that is backed in production by the
eval-flush buffer:

```rust
pub(crate) trait SpanComputedWriteSink {
    fn push_cell(
        &mut self,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        value: LiteralValue,
    ) -> Result<(), ExcelError>;

    fn push_rect(
        &mut self,
        sheet_id: SheetId,
        sr0: u32,
        sc0: u32,
        rows: u32,
        cols: u32,
        values_row_major: impl IntoIterator<Item = LiteralValue>,
    ) -> Result<(), ExcelError>;

    fn flush(self, engine: &mut Engine) -> Result<SpanComputedFlushStats, ExcelError>;
    fn discard(self);
}
```

Production backing:

```text
SpanComputedWriteSink
  -> ComputedWriteBuffer
  -> plan_owned_computed_write_coalescing(...)
  -> flush_computed_write_plan(...)
  -> OverlayFragment::{DenseRange, RunRange, SparseOffsets} or points
```

If the eval-flush API names differ after PR #95 lands, the FormulaPlane adapter
should absorb the naming difference. FormulaPlane agents should not reach below
this seam into `arrow_store` internals.

### 2.1 Inputs and ownership

Each pushed write must carry:

```text
sheet_id
row0 / col0 in internal 0-based coordinates
LiteralValue output
write sequence within the task
optional span_id/task_id for observability only
```

The buffer owns staged values until flush or discard. It must not borrow
placement-local `LiteralValue` data beyond the call. Text values should be
converted/cloned once into the buffer payload or into overlay lanes during flush.

The writer must represent two distinct states:

```text
no write for this cell      -> sparse gap; lower layers remain visible
write LiteralValue::Empty   -> explicit Empty masks computed/base lower layers
```

For span dirty domains with holes/punchouts, skipped placements are sparse gaps,
not explicit `Empty` writes, unless the formula result is actually empty.

### 2.2 Sheet/column/chunk grouping

The production flush must group by:

```text
sheet_id -> column -> Arrow chunk -> row offset within chunk
```

Then it applies same-cell last-write-wins by sequence number and chooses the
physical representation:

```text
single isolated write                  -> point
contiguous varied writes               -> DenseRange
contiguous repeated/result-run writes  -> RunRange
sparse offsets cheaper than points     -> SparseOffsets
sparse offsets not cheaper             -> points
```

FormulaPlane should not make representation decisions directly. It should push
logical computed writes and let eval-flush coalescing select fragments.

### 2.3 Flush boundaries

Span writes must flush at deterministic scheduler boundaries:

```text
before downstream legacy or span tasks can read the span result region
before any RangeView over the written result region is evaluated
before returning from evaluate_all / evaluate_until for observed targets
before cancellation/rollback can expose mixed staged state
```

For M1, the safest rule is:

```text
one SpanEvalTask stages all writes, then flushes before the scheduler advances
past that span task/layer
```

If multiple independent span tasks run in the same scheduler layer, the engine
may either flush each task independently or flush the layer as one buffer, but the
ordering contract must still ensure downstream reads see all precedent writes.

### 2.4 Value semantics

The flush path must preserve current value semantics:

```text
user/edit overlay > computed overlay fragment/point > base lanes
```

Required semantics:

- user overlay wins over computed fragments;
- computed explicit `Empty` masks base lanes;
- computed `Pending` masks base lanes as pending/null in typed selectors;
- wrong-type computed values still mask base for selector presence semantics;
- sparse gaps do not mask base;
- errors preserve error codes;
- date/time/duration preserve serial/type-tag semantics;
- array results are not valid single-cell lane values except via existing spill
  semantics or fallback.

The span evaluator should push scalar single-cell results only for M1. Spill or
multi-cell array results should remain legacy until a separate spill-span result
contract exists.

### 2.5 User overlay precedence

FormulaPlane result writes go into the computed layer only. They must never clear
or rewrite user value overlays. A value edit inside a span should be handled by
FormulaOverlay plus the existing user/delta overlay path; recomputing the span
must skip that effective-domain placement or write beneath it without surfacing.

Acceptance tests must cover both cases:

```text
computed fragment exists under user overlay -> user value is read
user overlay removed later -> current formula authority decides whether computed
  fragment may resurface or must be cleared by punchout semantics
```

The second case requires FormulaOverlay state-machine alignment. If a cell is
`ValueOverride` or `Cleared`, stale computed fragments underneath must not become
observable after compaction or undo unless formula authority is reabsorbed.

## 3. Tests needed before span eval writes through it

Add these tests before FP6.4 writes span results through the production path.
Locations assume eval-flush modules exist after PR #95 integration; if not, add
these with the imported buffer module first.

### 3.1 Buffer and coalescing tests

Suggested location:

```text
crates/formualizer-eval/src/engine/tests/computed_flush.rs
```

Required tests:

```text
computed_write_buffer_coalesces_varying_row_run_to_dense_range_fragment
computed_write_buffer_coalesces_constant_row_run_to_run_range_fragment
computed_write_buffer_preserves_sparse_holes_as_sparse_offsets_fragment
computed_write_buffer_falls_back_to_points_when_sparse_offsets_are_larger
computed_write_buffer_last_write_wins_within_same_cell
computed_write_buffer_groups_by_sheet_column_and_chunk
computed_write_buffer_flush_boundary_does_not_cross_sheet_or_column
```

### 3.2 Explicit Empty and presence-mask tests

Suggested location:

```text
crates/formualizer-eval/src/engine/tests/formula_plane_computed_writes.rs
```

Required tests:

```text
computed_write_buffer_explicit_empty_masks_base_value
computed_write_buffer_sparse_gap_does_not_mask_base_value
computed_write_buffer_pending_masks_base_value
computed_write_buffer_wrong_type_masks_base_in_numeric_selector
computed_write_buffer_errors_roundtrip_through_fragment_lanes
computed_write_buffer_temporal_values_preserve_type_tags
```

### 3.3 User overlay precedence tests

Suggested location:

```text
crates/formualizer-eval/src/engine/tests/formula_plane_computed_writes.rs
```

Required tests:

```text
computed_fragment_user_overlay_precedence_survives_rangeview_read
computed_fragment_user_overlay_precedence_survives_scalar_get_cell
span_eval_skips_value_override_punchout_even_with_stale_computed_fragment
cleared_formula_overlay_prevents_stale_computed_fragment_resurfacing
```

### 3.4 RangeView fragment readback tests

Suggested location:

```text
crates/formualizer-eval/src/engine/tests/formula_plane_computed_writes.rs
```

Required tests:

```text
range_view_reads_dense_computed_fragment_without_zip_select
range_view_reads_run_computed_fragment_without_zip_select
range_view_reads_sparse_computed_fragment_with_exact_offsets
range_view_lowered_text_reads_computed_text_fragment
range_view_type_tags_reflect_computed_fragment_presence
```

These tests should assert both values and fragment/read-path counters. Value
parity alone is not enough.

### 3.5 Span-eval write-through tests

Suggested location:

```text
crates/formualizer-eval/src/formula_plane/span_eval.rs
crates/formualizer-eval/src/engine/tests/formula_plane_span_eval.rs
```

Required tests:

```text
span_eval_writes_through_computed_write_buffer_not_direct_overlay
span_eval_constant_row_run_emits_run_fragment
span_eval_varying_row_run_emits_dense_fragment
span_eval_sparse_effective_domain_emits_sparse_offsets_or_points_by_cost
span_eval_explicit_empty_result_masks_base
span_eval_user_value_override_remains_visible_after_span_flush
span_eval_flushes_before_downstream_rangeview_read
span_eval_discarded_buffer_on_cancellation_writes_nothing
```

Use a writer spy where possible:

```text
push_count > 0
flush_count = 1 for one task/layer
computed_overlay_direct_set_count = 0 for FormulaPlane path
fragment_shape_counts expected
```

## 4. Rollback, cancellation, and flush-boundary safety

### 4.1 Current rollback posture

This branch has `ArrowUndoBatch` operations for point computed cells and computed
rect restore:

```rust
ArrowOp::SetComputedCell
ArrowOp::RestoreComputedRect
```

Those operations replay through `set_computed_overlay_cell_raw`, which is
point-oriented. After eval-flush fragments land, rollback must either:

```text
1. snapshot/restore logical computed values over the affected region, then flush
   through the same computed-write buffer/coalescer; or
2. support fragment-level restore with exact old/new logical semantics.
```

FormulaPlane should not invent a separate rollback path. It must reuse the
eval-flush rollback/flush contract once present.

### 4.2 Span task atomicity

A span eval task should behave as:

```text
stage writes in memory
if cancelled before flush: discard buffer, no computed overlay mutation
if error before flush: discard buffer or write only legacy-compatible error state
if stale epoch before flush: discard buffer and count stale task
if flush succeeds: expose all staged writes at the scheduled boundary
if flush fails: abort task and leave no partial visible state unless the engine's
  existing transaction boundary can roll it back
```

If the computed-write flush API cannot guarantee all-or-nothing mutation, the
scheduler must treat flush as the transaction boundary and register undo/restore
metadata before mutation.

### 4.3 Cancellation

Cancellation checks should occur:

```text
before starting the span task
periodically during placement iteration
before flush
inside flush if coalescing/fragment construction can be long-running
```

On cancellation before flush, `SpanComputedWriteSink::discard()` must be called.
Partial staged buffers must not leak into `computed_overlay`.

### 4.4 Existing flush boundaries to preserve

FormulaPlane must not weaken conservative eval-flush boundaries around:

```text
range-dependent vertices
spills and spill clears
delta old-value reads
logged snapshots / rollback records
cancellation or error exits
evaluate_until observed target reads
layer transitions where downstream tasks may read computed results
```

If the Phase 5 eval-flush API already encodes these boundaries, FormulaPlane
should call into that API rather than duplicating policy.

## 5. Observability counters

A FormulaPlane runtime test or benchmark may claim fragment-backed span writes
only if counters prove it.

Required computed-write counters:

```text
computed_write_buffer_push_count
computed_write_buffer_rect_push_count
computed_write_buffer_flush_count
computed_write_buffer_discard_count
computed_write_buffer_input_cell_count
computed_write_buffer_same_cell_overwrite_count
computed_write_chunk_plan_count
computed_write_chunk_plan_cell_count
computed_write_fragment_cell_count
computed_write_point_cell_count
computed_write_dense_range_fragment_count
computed_write_dense_range_cell_count
computed_write_run_range_fragment_count
computed_write_run_range_cell_count
computed_write_sparse_offsets_fragment_count
computed_write_sparse_offsets_cell_count
computed_write_sparse_offsets_fallback_point_count
computed_write_explicit_empty_count
computed_write_pending_count
computed_write_error_count
computed_write_estimated_bytes
```

Required FormulaPlane span-write counters:

```text
span_eval_task_count
span_eval_placement_count
span_eval_write_push_count
span_eval_flush_count
span_eval_cancelled_discard_count
span_eval_stale_discard_count
span_eval_writer_fallback_count
span_eval_direct_computed_overlay_write_count
```

Required assertions for accepted span fixtures:

```text
span_eval_write_push_count = span_eval_placement_count for scalar M1
computed_write_buffer_push_count = span_eval_write_push_count
computed_write_buffer_flush_count > 0
computed_fragment_cell_count > 0
span_eval_direct_computed_overlay_write_count = 0
per_placement_formula_vertices_created = 0
per_placement_ast_roots_created = 0
per_placement_edge_rows_created = 0
```

For constant copied formulas:

```text
computed_write_run_range_fragment_count > 0
computed_write_dense_range_fragment_count = 0 or justified by representation policy
```

For varied copied formulas:

```text
computed_write_dense_range_fragment_count > 0
```

For sparse effective domains:

```text
computed_write_sparse_offsets_fragment_count > 0
  or computed_write_sparse_offsets_fallback_point_count > 0 by explicit cost policy
```

RangeView read counters should distinguish:

```text
range_view_direct_dense_fragment_reads
range_view_direct_run_fragment_reads
range_view_sparse_fragment_intersections
range_view_zip_select_calls
range_view_row_scalar_fallbacks
```

These are not just performance counters; they guard against a false win where
values are correct but every read/write remains point-backed.

## 6. Integration dependency ordering

### 6.1 Required ordering

Recommended ordering:

```text
1. Merge/rebase eval-flush PR #95 into FormulaPlane branch.
2. Verify concrete symbols/API paths for:
   - ComputedWriteBuffer
   - ComputedWrite::{Cell, Rect}
   - coalescing plan
   - fragment flush
   - OverlayFragment variants
   - RangeView fragment-aware selectors
3. Add FormulaPlane-facing SpanComputedWriteSink adapter around the eval-flush API.
4. Add computed-write seam tests and RangeView fragment readback tests.
5. Only then implement FP6.4 span evaluator writes through the adapter.
```

### 6.2 PR #95 / branch mismatch gate

Until PR #95 is present, FP6.4 must be limited to:

```text
report/design work
store/evaluator shape tests using a mock writer
no normal recalc integration
no performance claims
no fragment-backed result claims
```

A temporary point writer may be used only for negative/control tests:

```text
span_eval_direct_computed_overlay_write_count > 0 -> test should fail for
accepted compact-authority path
```

It must not be hidden behind the production FormulaPlane evaluator.

### 6.3 Adapter boundary after merge

After eval-flush lands, FormulaPlane should depend on one internal adapter rather
than many eval-flush internals:

```rust
pub(crate) struct FormulaPlaneComputedWriter {
    buffer: ComputedWriteBuffer,
    stats: SpanComputedFlushStats,
}
```

The adapter should be the only FormulaPlane code allowed to import eval-flush
write-buffer/coalescing types. This keeps later direct-fragment writer changes
localized.

## 7. Non-goals

This shore-up does not propose:

- implementing eval-flush Phase 5 in this branch;
- changing current public/default evaluation behavior;
- changing Arrow base-lane write semantics;
- using FormulaOverlay formula punchouts as a value-write layer;
- adding span-aware function kernels;
- adding spill/array FormulaPlane result support;
- relaxing rollback/cancellation guarantees;
- accepting per-cell point writes as proof of FormulaPlane compact authority.

## 8. Circuit breakers

Stop and replan if any implementation:

- starts FP6.4 production span eval before eval-flush PR #95 / equivalent
  computed-write substrate is integrated;
- writes FormulaPlane span results through `mirror_value_to_computed_overlay`,
  `set_computed_overlay_cell_raw`, `ch.computed_overlay.set`, graph value caches,
  user/delta overlays, or Arrow base lanes for a path claiming compact authority;
- treats sparse gaps as explicit `Empty` writes or treats explicit `Empty` writes
  as sparse gaps;
- lets a user value overlay be overwritten or cleared by computed span flush;
- lets stale computed fragments resurface through a `ValueOverride` or `Cleared`
  FormulaOverlay punchout;
- allows downstream tasks or `RangeView` reads to observe staged but unflushed
  span results;
- ignores cancellation after staging writes but before flush;
- flushes without undo/rollback metadata where the surrounding engine action is
  transactional;
- hides direct per-cell writes behind correct values without exposing counters;
- reports a FormulaPlane performance win without nonzero fragment-shape counters
  and zero direct computed-overlay write counters;
- forks a second computed-write/coalescing implementation under FormulaPlane
  instead of using the eval-flush substrate.

## 9. Doc-update recommendations

When synthesizing this report into the active docs, add these gates:

1. FP6.4 has a hard prerequisite on eval-flush PR #95 or equivalent concrete
   computed-write/fragment API integration.
2. FormulaPlane span eval writes through a single `SpanComputedWriteSink` adapter;
   no other FormulaPlane module writes computed results.
3. The implementation plan should distinguish:

```text
mock-writer span evaluator tests -> allowed before eval-flush integration
production writer / recalc integration -> blocked until eval-flush integration
```

4. Mandatory counters must include direct-write guard counters and fragment-shape
   counters.
5. Existing map-backed computed overlay tests remain parity oracles, not compact
   authority proof.
