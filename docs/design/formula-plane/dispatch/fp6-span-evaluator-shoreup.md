# FP6 Span Evaluator + Placement Context Shore-Up

Date: 2026-05-03  
Branch: `formula-plane/bridge`  
Scope: design shore-up only; no production code changes.

## Verdict

**WARN** for FP6.4 implementation readiness.

A first scalar span evaluator is feasible, but it is not safe to hook into normal
recalculation until two prerequisites are explicit:

1. The scheduler/proxy seam must order span tasks relative to legacy vertices and
   flush span results before downstream reads.
2. The eval-flush `ComputedWriteBuffer` / fragment write API must be present in
   this branch, or FP6.4 must add it as a prerequisite before span eval claims
   fragment-backed writes.

A direct/test-only evaluator is startable once FP6.1 stores, FormulaResolution,
FormulaOverlay effective-domain projection, and FP6.3 region indexes exist. It
must reuse one template and synthesize placement context per output cell without
creating per-placement graph vertices, persistent AST roots, or scalar edge sets.

## Source Reality Check

Relevant current source posture:

- `crates/formualizer-eval/src/interpreter.rs` has
  `Interpreter::new_with_cell(context, current_sheet, cell)` and passes
  `current_cell`/`current_sheet` into `DefaultFunctionContext` for function
  dispatch.
- `FunctionContext` exposes `current_sheet`, `current_cell`, locale, timezone,
  clock, thread pool, cancellation token, chunk hint, volatile level,
  workbook seed, recalc epoch, date system, criteria mask, and row visibility
  masks via `crates/formualizer-eval/src/traits.rs`.
- `Interpreter` resolves references through `EvaluationContext::resolve_range_view`
  and scalar cell refs through `resolve_cell_reference_value`, so a span evaluator
  should reuse the same `Engine` context instead of inventing a separate value
  reader.
- Current graph/eval paths are `VertexId`-centric. `evaluate_vertex_impl` obtains
  a vertex's `CellRef`, builds `Interpreter::new_with_cell`, evaluates the graph
  arena AST, updates the graph value, and mirrors through computed overlay calls.
- Current `Scheduler` schedules only `VertexId` layers, plus `VertexId -> VertexId`
  virtual dependencies. Span work items do not exist yet.
- Current `RangeView` reads `overlay -> computed_overlay -> base`, preserving user
  overlay precedence over computed values.
- This worktree does **not** expose named `ComputedWriteBuffer`, `ComputedWrite`,
  `DenseRange`, `RunRange`, `SparseOffsets`, or `OverlayFragment` symbols in
  `crates/formualizer-eval/src`. It still has a map-backed `Overlay` and
  per-cell `mirror_value_to_computed_overlay` / `set_computed_overlay_cell_raw`
  paths. The active docs therefore depend on eval-flush Phase 5 / PR #95, or on a
  prerequisite port of that substrate, before FP6.4 can claim fragment-backed
  span result writes.
- Existing `formula_plane/` modules are passive. `template_canonical.rs` has the
  useful `AxisRef::RelativeToPlacement` / `AbsoluteVc` model, but there is no
  runtime `CompiledTemplate`, `SpanEvalTask`, or placement-aware interpreter
  bridge yet.

## Proposed Internal API Contract

The first evaluator should live under:

```text
crates/formualizer-eval/src/formula_plane/span_eval.rs
```

All types should be `pub(crate)` unless a later stable-contract decision says
otherwise.

### SpanEvalTask

```rust
pub(crate) struct SpanEvalTask {
    pub span_id: FormulaSpanId,
    pub span_generation: u32,
    pub span_version: u32,
    pub template_id: FormulaTemplateId,
    pub sheet_id: SheetId,
    pub dirty: DirtyDomain,
    pub plane_epoch: FormulaPlaneEpoch,
    pub overlay_epoch: u64,
    pub dependency_epoch: u64,
    pub schedule_epoch: u64,
}
```

Required invariants:

- `span_generation`, `span_version`, and epochs are validated before evaluation.
- `dirty` is already projected and unioned by `SpanDirtyStore`.
- `dirty` is later intersected with the effective span domain:

```text
span domain - intrinsic mask - FormulaOverlay projection
```

- A stale task is discarded or demotes with a counted reason; it must not write.

### SpanPlacementContext

```rust
pub(crate) struct SpanPlacementContext<'a> {
    pub span_id: FormulaSpanId,
    pub template_id: FormulaTemplateId,
    pub placement_index: u32,
    pub placement: PlacementCoord,      // 0-based physical/grid coordinate
    pub result_cell: CellRef,           // sheet id + 0-based Coord
    pub sheet_name: &'a str,
    pub template_anchor: TemplateAnchor, // original placement used to build the template
}
```

Required invariants:

- `result_cell.coord` is the `current_cell` passed to the interpreter and
  functions.
- `sheet_name` comes from the graph/sheet registry for `sheet_id`; display names
  are not authority keys.
- For first scalar spans, placement and result coordinates are the same. If later
  spans support non-scalar result regions, this contract must split
  `formula_placement` from `result_cell`.

### SpanEvaluator

```rust
pub(crate) struct SpanEvaluator<'a> {
    engine_ctx: &'a dyn EvaluationContext,
    sheet_registry: &'a SheetRegistry,
    templates: &'a TemplateStore,
    spans: &'a SpanStore,
    formula_overlay: &'a FormulaOverlay,
    counters: &'a mut FormulaPlaneRuntimeCounters,
}

pub(crate) enum SpanEvalOutcome {
    Completed { placements: u64, writes: u64 },
    SkippedStale { reason: SpanEvalFallbackReason },
    Fallback { reason: SpanEvalFallbackReason },
    Demoted { reason: SpanEvalFallbackReason },
    Cancelled,
}
```

The evaluator owns no persistent ASTs, graph vertices, or dependency edges. It
borrows template/span stores and pushes computed values into a staged writer.

### SpanResultWriter

The evaluator should write through an interface narrow enough to spy on in tests:

```rust
pub(crate) trait SpanResultWriter {
    fn push_cell(
        &mut self,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        value: LiteralValue,
    ) -> Result<(), ExcelError>;

    fn flush(self, engine: &mut Engine) -> Result<SpanWriteFlushStats, ExcelError>;
    fn discard(self);
}
```

Production implementation must be backed by:

```text
ComputedWriteBuffer -> coalesced chunk plan -> computed overlay fragments
```

If `ComputedWriteBuffer` is not present, FP6.4 must first add or import that
substrate. A per-cell `mirror_value_to_computed_overlay` writer may exist only as
an explicitly failing test spy or temporary non-claiming fallback; it must not be
used for any path that reports compact FormulaPlane evaluation wins.

## Template Relocation and Placement Semantics

### Current interpreter is necessary but not sufficient

`Interpreter::new_with_cell` preserves scalar semantics for:

- `FunctionContext::current_cell`;
- implicit intersection;
- deterministic RNG seeding;
- locale/timezone/date system;
- cancellation and chunk hints;
- criteria and row visibility masks through `EvaluationContext`.

It does **not** by itself make one anchor AST act like N copied formulas. Current
graph formulas are already stored per concrete formula cell. A span-owned
runtime template needs a placement-aware reference representation.

### Required template representation

The runtime template should have one stored body plus placement-aware reference
sites. Acceptable first shapes:

```rust
pub(crate) struct CompiledTemplate {
    ast: Arc<ASTNode>,
    ref_sites: Arc<[TemplateReferenceSite]>,
    unsupported_eval_flags: TemplateEvalFlags,
}

pub(crate) enum TemplateAxis {
    RelativeToPlacement { offset: i64 },
    AbsoluteVc { index: u32 },
}
```

or an equivalent arena form under `formula_plane/`.

The already-passive `template_canonical::AxisRef` model is the right source of
truth for relative/absolute axes, but runtime eval should not reuse passive
scanner IDs as authority.

### Relocation rule

For each placement, reference endpoints are produced from template axes:

```text
RelativeToPlacement(offset) -> placement_axis + offset
AbsoluteVc(index)           -> index
```

Then convert to parser/interpreter `ReferenceType` using current conventions:

- internal placement/result coordinates are 0-based;
- parser `ReferenceType` rows/cols are 1-based for ordinary cell/range refs;
- `CellRef` uses 0-based `Coord`;
- conversion boundaries must be explicit and tested.

### No persistent per-placement ASTs

The preferred implementation is a placement-aware compiled template evaluator or
argument handle adapter that materializes references lazily for the current
placement. It should not clone or store a full AST per placement.

A transient debug-only AST reification helper may be useful for oracle tests, but
must be counted separately and must not be the production M1 evaluator path if
M1 claims allocation wins.

## Invocation of Existing Scalar Semantics

The first evaluator should be scalar, not vectorized:

```text
for placement in task.dirty ∩ effective_span_domain:
  validate cancellation/stale epochs
  construct SpanPlacementContext
  construct placement-aware template view
  invoke existing Interpreter/Function dispatch semantics
  stage one computed output cell
flush staged writes at the task/layer boundary
```

Required call shape:

```rust
let interpreter = Interpreter::new_with_cell(engine_ctx, sheet_name, result_cell);
let value = template_eval.evaluate_with(&interpreter, &placement_ctx)?;
```

`template_eval.evaluate_with` may internally use existing `Interpreter` entry
points, but reference nodes must be placement-aware. Function calls should still
flow through `Function::dispatch` with `DefaultFunctionContext` so existing
argument validation, short-circuit behavior, lazy references, criteria masks,
row visibility, RNG, and cancellation are preserved.

### Constructs that force fallback or demotion initially

Reject at promotion time where possible. If discovered during eval, stop the span
path and record a fallback/demotion reason.

Initial fallback/demotion set:

- volatile functions (`FnCaps::VOLATILE`) unless a later exact volatility policy
  accepts them;
- dynamic dependencies (`FnCaps::DYNAMIC_DEPENDENCY`, `INDIRECT`, `OFFSET`, etc.);
- reference-returning functions (`FnCaps::RETURNS_REFERENCE`) outside explicitly
  supported scalar semantics;
- dynamic arrays / spill outputs (`LiteralValue::Array` from a scalar span cell);
- internal span dependencies or self-dependencies;
- names, tables, local environments, structured refs, 3D refs, external refs
  unless exact dependency and evaluation contracts exist;
- open/whole-axis ranges not covered by the active dependency and index policy;
- formulas whose dependency summary cannot be bounded without
  under-approximation;
- unsupported function contracts or unknown functions;
- stale template/span/index/overlay epochs.

Scalar errors are not fallback. They are ordinary computed values and should be
written as `LiteralValue::Error` exactly like legacy evaluation.

## Result Write Contract

### Staging

A span task must stage all writes before flushing:

```text
placement evaluation results -> SpanResultWriter/ComputedWriteBuffer -> flush
```

Required semantics:

- `LiteralValue::Empty` is written as an explicit computed empty, not as an absent
  write/removal.
- Skipped holes/punchouts produce no computed write for that placement; their
  existing user/formula overlay authority decides the visible value.
- Errors are staged as computed error values.
- Arrays/spills are unsupported for scalar M1 spans and force fallback/demotion.
- User value overlay precedence is preserved because writes target the computed
  overlay layer only.

### Flush boundary

Flush must happen before downstream reads observe span results:

```text
span task complete -> flush computed writes -> schedule downstream legacy/span readers
```

For direct/test-only FP6.4 evaluation, flush before reading through `RangeView` in
assertions. For normal recalc, the FP6.5 scheduler/proxy seam must define whether
flush happens per span task, per scheduler layer, or at a conservative barrier.
The first safe policy is per-task or per-layer flush before any dependent task.

### Cancellation

Cancellation must be checked:

- before scheduling/evaluating the task;
- between placement batches, with a small bounded batch size;
- before flush.

If cancellation fires before flush, discard staged writes and return `Cancelled`.
If cancellation fires during flush, the flush API must either be atomic at the
observable task boundary or must provide rollback/undo records. Until that
contract exists, cancellation during flush should not be exposed for normal
FormulaPlane recalc.

### Rollback and delta logging

Current engine paths have undo/delta concepts for graph and Arrow overlays.
FormulaPlane span writes need one of these contracts before public/default edit
integration:

```text
flush records old/new computed cells/fragments into ArrowUndoBatch/DeltaCollector
or
flush is only used in non-transactional full recalc paths where rollback is not required
```

Transactional edit paths must not use span writes until the rollback contract is
implemented and tested.

### ComputedWriteBuffer prerequisite

Current branch lacks the required buffer/fragment API. FP6.4 therefore has an
explicit prerequisite:

```text
merge/import eval-flush Phase 5 substrate
or add an internal computed write buffer with tests equivalent to Phase 5
```

Minimum API behavior required by span eval:

```text
push cell writes by sheet/row/col
preserve same-cell last-write-wins
coalesce by sheet/column/chunk
emit DenseRange/RunRange/SparseOffsets/Point as appropriate
preserve explicit Empty
preserve user overlay > computed overlay > base reads
expose shape/count/byte observability
```

## Interaction With RangeView and Downstream Reads

RangeView remains the semantic read abstraction. A span evaluator should read
precedents through the existing `EvaluationContext` / `RangeView` path so it sees:

```text
user overlay > computed overlay > base
```

Implications:

- A span task must not write direct base lanes.
- A span task must not write user/delta overlays.
- Flushed computed fragments must be visible to `RangeView` before any dependent
  legacy formula or span task runs.
- If a formula references its own result region or another still-dirty span
  result without an ordering proof, the span must be rejected/demoted.
- Internal span dependencies are unsupported in M1. This prevents the evaluator
  from depending on old/new mixed values within its own output region.
- FormulaOverlay effective-domain projection must run before evaluation so
  punched-out cells are skipped and not overwritten.

Downstream dirty propagation is not the evaluator's responsibility, but the
evaluator must report written result regions so the scheduler/dirty bridge can
route legacy and span dependents.

## Performance and Counter Requirements

A span evaluator acceptance test may claim compact authority only when counters
prove it.

Required counters:

```text
span_eval_task_count
span_eval_placement_count
span_eval_effective_placement_count
span_eval_skipped_overlay_punchout_count
span_eval_template_count
span_eval_template_arc_clone_count
span_eval_compiled_template_reuse_count
span_eval_transient_ast_reification_count
span_eval_graph_vertices_created
span_eval_ast_roots_created
span_eval_edge_rows_created
span_eval_set_cell_formula_calls
span_eval_scalar_interpreter_calls
span_eval_fallback_reason_counts
span_eval_demote_reason_counts
computed_write_buffer_push_count
computed_write_buffer_flush_count
computed_fragment_shape_counts
computed_fragment_cell_count
computed_write_direct_overlay_fallback_count
range_view_fragment_read_count_or_equivalent
```

Required assertions for dense accepted spans:

```text
span_eval_task_count = span count or dirty span count
span_eval_placement_count = accepted effective dirty placements
span_eval_graph_vertices_created = 0
span_eval_ast_roots_created = 0
span_eval_edge_rows_created = 0
span_eval_set_cell_formula_calls = 0
span_eval_transient_ast_reification_count = 0 for production M1 path
computed_write_direct_overlay_fallback_count = 0
computed_fragment_cell_count = span_eval_effective_placement_count
computed_fragment_shape_counts.dense_range/run_range/sparse_offsets > 0 as fixture-appropriate
```

If a fallback path materializes legacy formulas, it must increment materialized
cell/vertex/AST/edge counters with a reason label.

## Acceptance Tests

### Unit tests under `crates/formualizer-eval/src/formula_plane/span_eval.rs`

```text
span_eval_task_rejects_stale_span_generation
span_eval_task_rejects_stale_template_epoch
span_placement_context_uses_result_cell_as_current_cell
span_placement_context_uses_sheet_registry_name_not_display_key
span_template_relative_ref_relocates_by_placement_row
span_template_relative_ref_relocates_by_placement_col
span_template_absolute_ref_does_not_relocate
span_template_mixed_anchor_ref_relocates_only_relative_axes
span_template_cross_sheet_static_ref_uses_stable_sheet_binding
span_eval_effective_domain_skips_formula_overlay_punchouts
span_eval_internal_dependency_is_rejected_for_m1
span_eval_volatile_function_forces_fallback
span_eval_dynamic_dependency_function_forces_fallback
span_eval_reference_returning_function_forces_fallback
span_eval_array_result_forces_spill_fallback_or_demote
span_eval_scalar_error_is_written_not_fallback
span_eval_explicit_empty_is_staged_as_explicit_write
span_eval_cancel_before_flush_discards_staged_writes
span_eval_does_not_reify_ast_per_placement_in_production_path
```

### Computed write tests in the eval-flush/buffer module

If the eval-flush module is imported, place these near the buffer. If it is added
fresh, these tests should define the buffer contract before span eval uses it.

```text
computed_write_buffer_coalesces_varying_row_run_to_dense_range_fragment
computed_write_buffer_coalesces_constant_row_run_to_run_range_fragment
computed_write_buffer_preserves_sparse_holes_as_sparse_offsets_fragment
computed_write_buffer_explicit_empty_masks_base_value
computed_write_buffer_user_overlay_precedence_survives_fragment_flush
computed_write_buffer_same_cell_last_write_wins
computed_write_buffer_flush_reports_fragment_shape_counts
```

### Integration tests under `crates/formualizer-eval/src/engine/tests/`

```text
formula_plane_span_eval_row_run_matches_legacy_outputs
formula_plane_span_eval_col_run_matches_legacy_outputs
formula_plane_span_eval_rect_matches_legacy_outputs
formula_plane_span_eval_uses_interpreter_current_cell_for_rng_seed
formula_plane_span_eval_implicit_intersection_matches_legacy
formula_plane_span_eval_cross_sheet_static_ref_matches_legacy
formula_plane_span_eval_error_outputs_match_legacy
formula_plane_span_eval_empty_outputs_mask_old_base_values
formula_plane_span_eval_user_overlay_precedence_survives_recalc
formula_plane_span_eval_punchout_cell_is_not_overwritten
formula_plane_span_eval_rangeview_reads_flushed_computed_fragments
formula_plane_span_eval_does_not_create_per_placement_vertices_ast_or_edges
formula_plane_span_eval_direct_test_only_mode_does_not_touch_scheduler
formula_plane_span_eval_normal_recalc_requires_scheduler_seam_gate
```

### Manual/nightly probes after parity gates

```text
formula_plane_100k_row_run_scalar_eval_probe
formula_plane_100k_constant_run_fragment_probe
formula_plane_sparse_punchout_fragment_probe
formula_plane_finance_recalc_span_eval_probe
formula_plane_small_workbook_overhead_probe
```

Wall time/RSS are supporting evidence only. Counters proving compact authority
come first.

## Integration Ordering

Recommended dependency order:

1. FP6.1 stores/IDs/FormulaResolution/FormulaOverlay vocabulary.
2. FP6.3 effective-domain projection and sidecar indexes.
3. Eval-flush Phase 5 buffer/fragment substrate available in this branch.
4. Test-only `SpanEvaluator` direct API with no normal recalc integration.
5. FP6.5 scheduler/proxy seam decides normal ordering and flush barriers.
6. Normal FormulaPlane-enabled recalc path behind an explicit opt-in gate.

FP6.4 should not block on optional span-aware function kernels. Scalar semantics
via existing interpreter/function dispatch are sufficient for the first runtime
win once template reference relocation and computed write buffering exist.

## Non-Goals

- No public/default behavior changes.
- No graph amputation.
- No graph-native `SpanProxy` requirement for the test-only evaluator.
- No span-aware function kernels.
- No dynamic array/spill support for M1 span tasks.
- No volatile/dynamic/reference-returning function optimization.
- No direct base-lane writes.
- No user/delta overlay writes for computed span outputs.
- No per-placement formula graph vertices, persistent AST roots, or scalar edge
  sets for accepted spans.
- No FormulaPlane runtime types outside `crates/formualizer-eval/src/formula_plane/`.

## Circuit Breakers

Stop and replan if any implementation:

- evaluates a span by calling `set_cell_formula` or creating one graph formula
  vertex per placement;
- stores or clones persistent AST roots per placement;
- builds scalar dependency edges per placement for an accepted compact span;
- assumes `Interpreter::new_with_cell` alone relocates an anchor formula's
  references for copied placements;
- writes span outputs through `mirror_value_to_computed_overlay`,
  `set_computed_overlay_cell_raw`, graph value caches, user/delta overlays, or
  Arrow base lanes for a path claiming compact eval wins;
- allows a span task into normal recalc before scheduler ordering and flush
  barriers are defined;
- evaluates formulas with internal span dependencies under M1;
- treats `LiteralValue::Array` as a scalar span output;
- drops explicit `Empty` writes as absent holes;
- overwrites FormulaOverlay punchouts or user value overrides;
- suppresses fallback, demotion, materialization, direct-write, or transient-AST
  counters;
- changes public/default behavior without an explicit scoped opt-in.
