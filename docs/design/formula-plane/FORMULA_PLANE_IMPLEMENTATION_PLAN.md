# FormulaPlane Runtime Implementation Plan

Status: **active phased implementation plan**  
Branch: `formula-plane/bridge`  
Date: 2026-05-03

This plan implements `FORMULA_PLANE_RUNTIME_ARCHITECTURE.md`. It supersedes the
forward-looking FP5-FP7 portion of `REPHASE_PLAN.md` while preserving completed
passive phases FP1-FP4 as prerequisites and source material.

Runtime implementation should be based on, or explicitly integrated with,
eval-flush PR #95 / Phase 5. FP6.4+ assumes `ComputedWriteBuffer`, coalesced
computed write plans, overlay fragments, and fragment-aware `RangeView` are
available in the implementation branch.

## 1. Program goal

Land FormulaPlane as an opt-in compressed formula authority for accepted repeated
formula spans while preserving existing observable workbook semantics.

The first production target is not broad Excel feature replacement. It is:

```text
safe copied/shared formula spans
  -> one/few templates
  -> one/few span work items
  -> sidecar dirty projection
  -> fragment-backed computed results
  -> legacy fallback for unsupported cases
```

The graph remains the correctness and may-affect routing backbone. FormulaPlane
owns span projection, punchouts, and compressed evaluation.

## 2. Program constraints

All phases must preserve:

- public/default behavior unless an explicit opt-in flag is introduced;
- legacy fallback correctness;
- no under-approximated dirty propagation;
- `user value overlay > computed overlay > base` value precedence;
- `FormulaOverlay > span/template > legacy formula` formula precedence;
- explicit `Empty` masking;
- bounded validation gates;
- no FormulaPlane type promotion into `formualizer-common` without a separate
  stable-contract decision.

FormulaPlane primitives live under:

```text
crates/formualizer-eval/src/formula_plane/
```

Evaluation/writeback phases also require the eval-flush substrate from PR #95:

```text
ComputedWriteBuffer -> coalesced chunk plan -> computed overlay fragments
```

If an implementation worktree lacks that substrate, FP6.4 may not claim
fragment-backed span evaluation.

## 3. Definitions of done for runtime authority

A FormulaPlane runtime phase is not complete unless it can show:

```text
accepted span cells > 0
legacy fallback cells counted
formula vertices avoided counted
AST roots avoided counted
edge rows avoided counted
computed result fragment shapes counted
per-placement formula vertices/AST roots/edge rows created = 0 for accepted spans
legacy parity tests pass
fallback/demotion/materialization reasons explicit
```

No phase may claim a compact runtime win if it still allocates one formula
AST/vertex/edge set per accepted span placement or writes accepted span results
through an unbatched/direct per-cell bypass instead of the eval-flush substrate.

## 4. Phase FP6.0 — Runtime architecture closeout

Status: **this document set**.

Deliverables:

- `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_ARCHITECTURE.md`
- `docs/design/formula-plane/FORMULA_PLANE_IMPLEMENTATION_PLAN.md`
- `REPHASE_PLAN.md` supersession note marking forward FP5-FP7 as controlled by
  these documents.

Gate:

- design reviewed for ownership, graph integration, dirty projection,
  FormulaOverlay punchouts, and eval-flush result-plane integration;
- shore-up reports recorded under `docs/design/formula-plane/dispatch/fp6-*.md`;
- `REPHASE_PLAN.md` marks forward FP5-FP7 as superseded.

No source behavior changes.

## 5. Phase FP6.1 — Core runtime stores and handles

Goal:

```text
Create FormulaPlane as an internal runtime store with compact IDs and formula
resolution semantics, but no scheduling/evaluation authority yet.
```

Deliverables:

```rust
FormulaPlane
TemplateStore
SpanStore
FormulaOverlay
SpanDirtyStore
SpanProjectionCache
FormulaTemplateId
FormulaSpanId
FormulaOverlayEntryId
SpanMaskId
PlacementDomain
ResultRegion
FormulaHandle
FormulaResolution
```

Implementation notes:

- Use store-owned data and generational IDs.
- Runtime coordinates are stable `SheetId` plus 0-based row/column; passive
  sheet-name/1-based records convert at boundaries only.
- `TemplateStore` interns `Arc<ASTNode>` and canonical template keys.
- `FormulaSpan` stores IDs, domain, result region, state, and version only.
- Formula exceptions are stored in `FormulaOverlay`, not inline in spans.
- `FormulaOverlay` is formula-definition authority only; Arrow user/computed/base
  overlays remain value authority.
- Add transient borrowed views only for queries/eval planning.
- Keep new runtime types internal/`pub(crate)` unless a stable-contract decision
  promotes them.
- Existing passive `FormulaRunStore` and scanner IDs are inputs, not runtime
  authority IDs.

Acceptance tests:

```text
template_store_interns_equivalent_templates
span_store_allocates_generational_span_ids
formula_overlay_masks_span_resolution
formula_resolution_prefers_overlay_over_span_over_legacy
formula_resolution_returns_span_placement_without_legacy_materialization
legacy_owned_overlay_prevents_span_resolution
span_store_rejects_stale_generation_after_remove
placement_domain_row_run_iteration_is_correct
placement_domain_rect_iteration_is_correct
formula_plane_epoch_increments_when_store_mutates
```

Validation:

```bash
cargo fmt --all -- --check
cargo test -p formualizer-eval formula_plane --quiet
cargo test -p formualizer-eval --quiet
```

Exit claim:

```text
FormulaPlane has internal runtime storage and formula-resolution vocabulary.
No scheduler, graph bypass, dirty authority, or evaluation behavior changes yet.
```

## 6. Phase FP6.2 — Authority-grade span placement

Goal:

```text
Accept safe repeated formula families into FormulaPlane from existing
formula-ingest/formula-build pathways while unsupported formulas remain legacy.
```

Deliverables:

- runtime span placement builder using existing canonical template logic;
- row-run, col-run, and simple rectangular span placement;
- exact family identity check;
- fallback reasons for unsupported formulas;
- `FormulaPlacementResult` internal return type:

```rust
pub enum FormulaPlacementResult {
    Legacy { vertex_id: VertexId },
    Span { span_id: FormulaSpanId, placement: PlacementCoord },
    SpanException { span_id: FormulaSpanId, entry_id: FormulaOverlayEntryId },
}
```

Rules:

- Do not optimize formulas with dynamic/volatile/opaque dependency authority.
- Do not create spans from lossy scanner fingerprints.
- Template authority keys must include canonical AST structure, anchors,
  literals, function identity/contracts, stable sheet binding, and dependency
  summary compatibility.
- Do not reuse passive scanner `source_template_id` strings as runtime template
  authority.
- Do not yet skip graph materialization in broad paths unless the phase
  explicitly controls that path and counts avoided materialization.

Acceptance tests:

```text
row_run_same_template_promotes_to_span
col_run_same_template_promotes_to_span
rect_same_template_promotes_to_span
unique_formulas_remain_legacy
unsupported_dynamic_formula_remains_legacy_with_reason
span_virtual_formula_matches_legacy_formula_text_or_ast_relocation
```

Observability:

```text
formula_cells_seen
templates_interned
spans_created
span_cells_covered
legacy_cells
fallback_reasons
```

Exit claim:

```text
FormulaPlane can accept and resolve safe formula spans exactly, but runtime eval
may still use legacy authority until later phases.
```

## 7. Phase FP6.3 — Sidecar region indexes and span dependency routing

Goal:

```text
FormulaPlane can find span owners, formula punchouts, and candidate dirty spans
quickly without per-cell graph edges or deep graph span awareness.
```

Deliverables:

```rust
RegionKey / RegionSet / RegionPattern helpers, if existing types are insufficient
SheetRegionIndex<T>
SpanDomainIndex
SpanDependencyIndex
FormulaOverlayIndex
SpanDependencySummary runtime adapter
DirtyProjection::WholeTarget
DirtyDomain::Whole
```

The first implementation should be spreadsheet-shaped:

```rust
pub struct SheetRegionIndex<T> {
    points: FxHashMap<(u32, u32), SmallVec<[T; 2]>>,
    col_intervals: FxHashMap<u32, IntervalTree<u32, T>>,
    row_intervals: FxHashMap<u32, IntervalTree<u32, T>>,
    rect_buckets: RectBucketIndex<T>,
    whole_cols: FxHashMap<u32, Vec<T>>,
    whole_rows: FxHashMap<u32, Vec<T>>,
    whole_sheet: Vec<T>,
}
```

Use the existing `engine::interval_tree` for one-dimensional row/column interval
queries if possible. If it is too graph-specific, factor only the minimal
generic interval primitive. Do not introduce a broad R-tree/geometry dependency
for the first runtime path.

### 7.1 Index responsibilities

`SpanDomainIndex`:

```text
find_span_at(sheet,row,col)
find_spans_intersecting(region)
```

Used by virtual formula lookup, edits, structural transforms, and local
repatterning. It answers geometric coverage only; FormulaOverlay still wins on
punchouts.

`SpanDependencyIndex`:

```text
changed precedent region -> candidate span dependency entries
```

Used by dirty routing. It returns candidate entries, which are exact-filtered and
then projected into dirty placement domains.

`FormulaOverlayIndex`:

```text
cell/region -> overlay entries
```

Used for punchout lookup and bulk edit/structural operations.

### 7.2 Initial dirty policy

```text
any overlap with an accepted span dependency summary dirties the whole span
```

This is intentionally conservative and correct. Partial dirty projections come
in FP6.7.

### 7.3 Implementation notes

- Build sidecar indexes from `SpanStore`, `TemplateStore`, dependency summaries,
  and `FormulaOverlay`.
- Query the span dependency sidecar alongside existing graph dirty routing.
- Exact-filter candidates after index lookup; the index may over-return but must
  not under-return.
- Store true rectangular dependencies in coarse rect buckets or a simple exact
  fallback list initially.
- Store whole-row, whole-column, whole-sheet, and structural dependencies in
  explicit side buckets.
- Track index epochs; rebuild derived indexes after complex structural or
  normalization operations rather than over-optimizing incremental maintenance.
- Do not modify graph internals unless required by a narrow seam.
- If a dependency summary cannot bound the precedent region, do not promote the
  span.

Acceptance tests:

```text
span_domain_index_finds_row_run_owner
span_domain_index_finds_rect_intersections
formula_overlay_index_finds_cell_punchout
same_row_dependency_edit_marks_candidate_span_dirty_whole
absolute_dependency_edit_marks_candidate_span_dirty_whole
unrelated_edit_does_not_mark_span_dirty
rect_dependency_query_exact_filters_bucket_candidates
whole_column_dependency_query_marks_candidate_span
unsupported_dependency_prevents_span_index_entry
sidecar_dirty_is_no_under_approx_against_legacy_fixture
```

Observability:

```text
span_domain_index_entries
span_dependency_index_entries
formula_overlay_index_entries
region_query_candidate_count
region_query_exact_filter_drop_count
index_rebuild_count
index_stale_epoch_count
```

Exit claim:

```text
FormulaPlane has fast sidecar lookup for ownership, punchouts, and conservative
may-affect dirty routing. No partial dirty or span eval required yet.
```

## 8. Phase FP6.4 — Span work item evaluator with computed fragment writes

Prerequisite:

```text
implementation branch includes eval-flush PR #95 / Phase 5 substrate
```

FP6.4 assumes these concrete APIs or equivalent internal adapters exist:

```text
ComputedWriteBuffer
ComputedWrite::{Cell, Rect}
plan/flush coalesced computed writes
OverlayFragment::{DenseRange, RunRange, SparseOffsets}
fragment-aware RangeView selectors
```

Goal:

```text
Evaluate accepted dirty spans as compressed work items and write results through
ComputedWriteBuffer / eval-flush fragment storage.
```

Deliverables:

```rust
SpanEvalTask
SpanEvaluator
EffectiveSpanDomain view
SpanComputedWriteSink adapter over ComputedWriteBuffer
scalar placement loop using existing interpreter/function semantics
ComputedWriteBuffer output path
span eval observability
```

Initial evaluator:

```text
for placement in effective dirty span domain:
  evaluate template using existing scalar semantics and placement context
  push result into ComputedWriteBuffer
flush -> DenseRange / RunRange / SparseOffsets computed fragments
```

No span-aware function kernels in this phase.

Acceptance tests:

```text
span_eval_row_run_matches_legacy_outputs
span_eval_col_run_matches_legacy_outputs
span_eval_rect_matches_legacy_outputs
span_eval_writes_dense_fragment_for_varying_outputs
span_eval_writes_run_fragment_for_constant_outputs
span_eval_writes_through_computed_write_buffer_not_direct_overlay
rangeview_reads_flushed_computed_fragments
span_eval_preserves_explicit_empty_outputs
span_eval_preserves_user_overlay_precedence
span_eval_fallback_for_unsupported_template_matches_legacy
span_eval_does_not_allocate_per_placement_ast_roots_or_edges
```

Benchmark/probe:

```text
100k copied formula row-run fixture
report graph vertices avoided, AST roots avoided, result fragment shape,
full eval ms, RSS if available
```

Exit claim:

```text
First non-spike FormulaPlane runtime path evaluates accepted spans without
per-cell formula materialization and writes fragment-backed computed results.
```

## 9. Phase FP6.5 — Graph/proxy scheduling seam

Goal:

```text
Schedule legacy vertices and FormulaPlane span work items together without making
the graph deeply span-aware.
```

First implementation decision: **sidecar mixed work-item scheduler**.

```rust
pub enum FormulaPlaneWorkItem {
    Legacy(VertexId),
    Span(FormulaSpanId),
}
```

The engine/FormulaPlane adapter builds a temporary recalc work graph for the
current dirty set. The legacy graph remains the may-affect router and legacy
formula dependency source; FormulaPlane contributes span dependency/result-domain
edges.

Graph-native proxy vertices remain deferred:

```rust
// later only if needed
VertexKind::FormulaSpanProxy(FormulaSpanId)
```

Rules:

- One work item per dirty span, not per placement.
- Downstream consumers see changed result regions.
- Existing range dependency machinery remains the may-affect backbone.
- Static graph-only schedule caches are disabled or keyed by FormulaPlane epochs
  whenever span work exists.
- Internal span dependencies/cycles demote unless an explicit recurrence policy
  accepts them.

Acceptance tests:

```text
formula_plane_disabled_schedule_is_unchanged
legacy_precedent_dirty_schedules_one_span_task_not_n_placements
span_result_region_dirty_schedules_legacy_dependent
span_to_span_dependency_orders_precedent_before_dependent
span_work_item_cycle_demotes_or_reports_conservative_cycle
legacy_and_span_outputs_match_legacy_oracle
```

Exit claim:

```text
FormulaPlane spans participate in normal recalculation ordering with legacy
formulas through a sidecar mixed work-item schedule.
```

## 10. Phase FP6.6 — FormulaOverlay punchouts and edit semantics

Goal:

```text
Edits inside and around spans are correct without eagerly materializing every
placement as legacy graph vertices.
```

Deliverables:

- `FormulaOverlayEntryKind::{FormulaOverride, ValueOverride, Cleared,
  LegacyOwned, Unsupported}` with generation/epoch metadata;
- value edit into span cell => `ValueOverride` plus user value overlay write;
- clear inside span => `Cleared` punchout plus explicit Empty/current clear
  value semantics;
- compatible formula edit => reabsorb/remove exception;
- incompatible supported formula edit => `FormulaOverride(template_id)` or new
  local span;
- unsupported formula edit => explicit `LegacyOwned(vertex_id)` or
  `Unsupported(reason)`;
- bulk paste/clear path that creates region punchouts without eager per-cell span
  splitting;
- virtual formula lookup for span placements;
- lazy materialization escape hatch.

Acceptance tests:

```text
value_edit_inside_span_punches_out_and_masks_computed
clear_inside_span_punches_out_and_returns_empty
same_template_formula_edit_reabsorbs_into_span
different_formula_edit_creates_exception
unsupported_formula_edit_materializes_legacy_owned_cell
get_formula_on_span_cell_returns_virtual_relocated_formula
edit_inside_span_dirties_downstream_dependent
paste_region_creates_bulk_punchouts_not_per_cell_span_splits
rollback_action_restores_formula_overlay_and_value_overlay
formula_overlay_epoch_invalidates_projection_cache
```

Exit claim:

```text
FormulaOverlay is the semantic edit layer for spans. Spans remain stable under
small edits; normalization is separate.
```

## 11. Phase FP6.7 — Partial dirty projection

Goal:

```text
Narrow dirty domains for common dependency shapes while preserving no
under-approximation.
```

Deliverables:

```rust
DirtyProjection::SameRow
DirtyProjection::SameCol
DirtyProjection::Shifted
DirtyProjection::FixedRangeToWhole
DirtyProjection::PrefixFromSource
DirtyProjection::SuffixFromSource
DirtyDomain::Intervals
DirtyDomain::Sparse
```

Rules:

- Only exact projection families may narrow dirty domains.
- Unknown projection => whole span or legacy fallback.
- Dynamic dependency footprint => legacy, not approximate optimization.

Acceptance tests:

```text
same_row_edit_dirties_matching_placement_only
same_col_edit_dirties_matching_placement_only
absolute_ref_edit_dirties_whole_span
fixed_range_edit_dirties_whole_span
prefix_range_edit_dirties_suffix_interval
multiple_edits_union_dirty_domains
partial_dirty_outputs_match_legacy_after_recalc
```

Observability:

```text
dirty_whole_span_count
dirty_interval_count
dirty_sparse_count
average_dirty_coverage_ratio
```

Exit claim:

```text
FormulaPlane avoids unnecessary full-span recalc for common exact dependency
projections.
```

## 12. Phase FP6.8 — Pattern normalization and local repatterning

Goal:

```text
Keep spans compact and useful after edits/pastes without making edits eagerly
split every span.
```

Deliverables:

- local absorb into neighboring compatible span;
- adjacent compatible span merge;
- interval-hole split when cheaper;
- exception-density demotion threshold;
- local repattern after paste;
- background/global optimize hook as manual/internal maintenance.

Acceptance tests:

```text
single_hole_keeps_large_span_with_projection
interval_holes_can_split_span
adjacent_compatible_spans_merge
many_exceptions_demote_region_to_legacy
paste_repeated_formula_block_forms_new_span
local_repattern_preserves_outputs
```

Exit claim:

```text
FormulaPlane spans remain representation-efficient under realistic edit/paste
patterns while preserving formula overlay semantics.
```

## 13. Phase FP6.9 — Structural edit support phase 1

Goal:

```text
Common row/column insert/delete operations over spans are correct.
```

MVP policy before exact transforms are implemented:

```text
structural edit intersects span/domain/precedent/overlay in an unsupported way
  -> demote affected span or region
  -> rebuild sidecar indexes
  -> dirty downstream dependents
```

Deliverables:

- shift span domains before/after insertion/deletion;
- shrink/expand domains where mathematically exact;
- split/hole/demote affected regions inside spans;
- relocate template references;
- recompute dependency summaries;
- invalidate/rebuild sidecar dirty index entries;
- local repattern after structural edits.

Acceptance tests:

```text
insert_rows_before_span_shifts_domain
insert_rows_inside_span_holes_or_splits_correctly
delete_rows_inside_span_shrinks_or_demotes_correctly
insert_cols_before_precedent_relocates_template_summary
delete_precedent_region_demotes_or_recomputes_correctly
structural_edit_outputs_match_legacy_oracle
```

Exit claim:

```text
FormulaPlane can survive common structural edits through exact transforms or
explicit demotion.
```

## 14. Phase FP6.10 — Optional span-aware function kernels

Goal:

```text
Allow functions to opt into span/batch evaluation while scalar semantics remain
the default.
```

Deliverables:

- minimal defaulted span-eval hook or internal adapter;
- arithmetic/comparison kernels;
- mask-aware `IF` if semantics are exact;
- static reductions/criteria aggregations only where existing function-owned
  dependency contracts are sufficient;
- fallback to scalar placement loop.

Acceptance tests:

```text
span_kernel_arithmetic_matches_scalar
span_kernel_comparison_matches_scalar
span_kernel_if_matches_scalar_short_circuit_semantics
unsupported_function_uses_scalar_or_legacy_fallback
kernel_result_fragments_match_scalar_result_fragments
```

Exit claim:

```text
Function-level span execution is an optional acceleration layer, not a semantic
requirement for FormulaPlane spans.
```

## 15. Phase FP6.11 — Loader/shared-formula metadata bridge

Goal:

```text
Use XLSX shared-formula metadata to seed FormulaPlane spans when available.
```

This phase can run in parallel with FP6.4-FP6.10.

Deliverables:

- preserve backend shared-formula hints where available;
- map shared groups to template/span candidates;
- scanner/canonicalizer remains fallback;
- absence of metadata does not change behavior.

Acceptance tests:

```text
xlsx_shared_formula_group_maps_to_span_candidate
loader_hint_absence_falls_back_to_scanner
loader_hint_mismatch_rejects_or_falls_back
loaded_workbook_outputs_match_legacy
```

Exit claim:

```text
Real workbook shared-formula groups can enter FormulaPlane without full workbook
formula rediscovery when backend metadata is available.
```

## 16. Phase FP6.12 — Default-off beta and runtime guardrails

Goal:

```text
Expose FormulaPlane runtime as an internal/default-off engine mode for broad
fixture testing.
```

Deliverables:

- config gate, e.g. `EvalConfig::formula_plane_enabled`;
- counters/report API for debug;
- fixture corpus:
  - dense copied arithmetic;
  - absolute refs;
  - same-row dependencies;
  - fixed ranges;
  - prefix ranges;
  - cross-sheet static refs;
  - unsupported/dynamic formulas;
  - edits/punchouts;
  - structural common cases;
- compare outputs against legacy default engine.

Acceptance:

```text
all beta fixtures match legacy outputs
fallbacks counted
no silent per-cell materialization on accepted spans
manual perf probe shows materialization and recalc wins
```

Exit claim:

```text
FormulaPlane is usable as a default-off beta for safe copied-formula workloads.
```

## 17. Agent workstream plan

Use agents in parallel only when interfaces are explicit.

### Lane A — Stores and authority

Owns FP6.1 data structures, IDs, formula resolution, and FormulaOverlay base
semantics.

### Lane B — Patterning

Owns FP6.2 canonical grouping, row/col/rect span construction, fallback reasons,
and local repatterning later.

### Lane C — Region indexes and dirty routing

Owns FP6.3 and FP6.7 sidecar region indexes, span dependency index, FormulaOverlay
index, dirty projections, exact-filtering, epoch/rebuild policy, and
no-under-approx tests.

### Lane D — Evaluation bridge

Owns FP6.4 span evaluator, placement context, `SpanComputedWriteSink`, and
`ComputedWriteBuffer`/fragment result output. Must be based on the eval-flush PR
#95 substrate.

### Lane E — Scheduling/proxy

Owns FP6.5 graph/scheduler seam and downstream propagation.

### Lane F — Edits/punchouts

Owns FP6.6 FormulaOverlay edit semantics and lazy materialization.

### Lane G — Structural transforms

Owns FP6.9 shift/split/demote/repattern operations.

### Lane H — Function kernels

Owns FP6.10 optional span-aware function kernels. Must not block scalar span
evaluator.

### Lane I — Oracle/benchmark/red-team

Owns legacy parity harnesses, benchmark probes, and regression reports.

## 18. Suggested first dispatch sequence

1. **Architecture review and shore-up synthesis** of the two design docs.
2. **FP6.1 test-first build agent** for core stores/handles/resolution/overlay
   vocabulary. No Engine behavior changes.
3. **FP6.1 review agent** before any runtime authority.
4. In parallel only after FP6.1 interfaces stabilize:
   - FP6.2 pattern placement behind internal/default-off gates;
   - FP6.3 sidecar region indexes and dirty-routing skeleton;
   - FP6.4 evaluator skeleton only on a branch with eval-flush PR #95 substrate.
5. FP6.5 sidecar mixed scheduler before normal recalc invokes span tasks.
6. Integration checkpoint: accepted row-run formula span evaluates through
   `ComputedWriteBuffer`, writes fragments, and matches legacy outputs with
   compact-authority counters.
7. Then proceed to edits/punchouts and partial dirty.

## 19. Safe kickoff checklist

Before dispatching production build agents beyond FP6.1/FP6.3 inert substrate,
answer these in the active docs or phase brief:

```text
1. How does formula lookup resolve staged/overlay/span/legacy/empty authority?
2. What exact state transition occurs for value edit, formula edit, clear, paste,
   undo, unsupported formula, and demotion?
3. Which region indexes exist, and which may over-return?
4. Where is exact filtering mandatory after an over-returning query?
5. Which dirty projections are exact and which force whole-span dirty or legacy?
6. How does a span task order against legacy vertices and other spans?
7. How does the evaluator set current_sheet/current_cell without cloning ASTs or
   creating graph vertices per placement?
8. How do span results flow through ComputedWriteBuffer and fragments?
9. What counters prove compact authority and what counters prove fallback?
10. What demotes to legacy, and which indexes/dirty/cache entries are invalidated?
```

## 20. Circuit breakers

Stop and replan if any implementation attempts to:

- silently allocate per-cell graph formula vertices for accepted spans;
- make the graph deeply inspect placement internals as a prerequisite;
- invent an unreviewed broad geometry/R-tree subsystem instead of the specified
  spreadsheet-shaped sidecar indexes;
- conflate span domain lookup, dependency lookup, and FormulaOverlay punchout
  lookup into one ambiguous map;
- under-approximate dirty dependencies or skip exact-filtering after an
  over-returning region-index query;
- start FP6.4 runtime claims on a branch that does not include eval-flush PR #95
  substrate;
- bypass `ComputedWriteBuffer`/fragment result storage for span output;
- implement broad span-aware function APIs before scalar span evaluator works;
- eagerly split spans on every edit instead of using FormulaOverlay punchouts;
- optimize dynamic/volatile/opaque formulas without a stable dependency contract;
- change public behavior without an explicit scoped decision.

## 21. Validation ladder

Minimum recurring gates:

```bash
cargo fmt --all -- --check
cargo test -p formualizer-eval formula_plane --quiet
cargo test -p formualizer-eval computed_flush --quiet
cargo test -p formualizer-eval rangeview_ --quiet
cargo test -p formualizer-eval --quiet
cargo test -p formualizer-bench-core --features formualizer_runner --quiet
cargo test -p formualizer-workbook --features umya,calamine --quiet
```

Manual/nightly characterization only after runtime authority changes:

```text
100k copied formula run
finance-shaped repeated edit/recalc fixture
small-workbook overhead fixture
unsupported-formula fallback fixture
structural edit parity fixture
```

## 22. Target milestones

### Milestone M1 — Non-spike row-run authority

```text
accepted row-run span
one template
one span task
no per-cell formula graph materialization
computed result fragments
legacy output parity
```

### Milestone M2 — Dirty/edit beta

```text
sidecar dirty routing
whole-span and partial dirty projections
FormulaOverlay punchouts
edit/clear/formula override correctness
```

### Milestone M3 — Default-off FormulaPlane beta

```text
config-gated runtime path
safe copied-formula workload coverage
explicit fallback reasons
manual perf wins on real finance-shaped fixtures
```

### Milestone M4 — Broader finance workload coverage

```text
loader shared-formula hints
structural common cases
local repatterning
optional span-aware kernels for high-value functions
```

This plan should get to M1 quickly, then expand correctness and coverage without
losing the validated performance path.
