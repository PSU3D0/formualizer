# FP6.5R Tranche 3/4 Implementation Plan

Date: 2026-05-04  
Status: dispatch-ready planning brief; no runtime cut-over authorized.

## 1. Purpose

This brief turns `FORMULA_PRODUCER_PLANNING_V1.md` into concrete implementation
tranches for the next two steps:

```text
Tranche 3 — graph-owned FormulaAuthority + formula ingest shadow seam
Tranche 4 — pure/inert partial dirty closure over producer/read indexes
```

The goal is to advance FormulaPlane toward runtime integration without changing
public/default behavior and without entering normal `evaluate_all` span runtime
authority yet.

Non-negotiable constraints:

```text
FormulaPlaneMode::Off remains default.
Shadow mode graph-materializes every formula.
Backends never decide FormulaPlane authority.
Dirty closure is pure/inert in Tranche 4.
No scheduler/evaluate_all/RecalcPlan/evaluate_cells cut-over in Tranche 3/4.
Partial dirty precision is V1 architecture, not deferred.
Graph proxy nodes are out of scope.
```

## 2. Tranche 3 — Formula ingest shadow seam

### 2.1 Goal

Centralize formula ingest through an Engine coordinator and add graph-owned
FormulaPlane authority scaffolding, while keeping all runtime behavior unchanged.

```text
Off:
  current BulkIngestBuilder behavior

Shadow:
  analyze would-accept/would-fallback spans
  still materialize all formulas through BulkIngestBuilder
  do not install active runtime spans
```

### 2.2 Concrete ownership shape

Add a graph-owned authority shell. Recommended location:

```text
crates/formualizer-eval/src/formula_plane/authority.rs
```

Initial type:

```rust
#[derive(Debug, Default)]
pub(crate) struct FormulaAuthority {
    pub(crate) plane: FormulaPlane,
    // Filled in later tranches as they become authoritative:
    // producer_results: FormulaProducerResultIndex,
    // consumer_reads: FormulaConsumerReadIndex,
    // dirty: FormulaDirtyStore,
}
```

Add to `DependencyGraph`:

```rust
formula_authority: FormulaAuthority,
```

Accessors:

```rust
pub(crate) fn formula_authority(&self) -> &FormulaAuthority;
pub(crate) fn formula_authority_mut(&mut self) -> &mut FormulaAuthority;
```

Tranche 3 shadow analysis should use a scratch `FormulaPlane` / scratch
`FormulaAuthority` and must not populate active graph-owned runtime authority.
This avoids dual formula authority while every formula is still graph-materialized.

### 2.3 Config surface

File:

```text
crates/formualizer-eval/src/engine/mod.rs
```

Add:

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FormulaPlaneMode {
    #[default]
    Off,
    Shadow,
}
```

Add to `EvalConfig`:

```rust
pub formula_plane_mode: FormulaPlaneMode,
```

Builder:

```rust
pub fn with_formula_plane_mode(mut self, mode: FormulaPlaneMode) -> Self;
```

Do **not** add `Authoritative` yet unless it is private/test-only and
unreachable from normal code paths. Prefer only `Off | Shadow` for Tranche 3.

### 2.4 Formula ingest records

Add a central ingest module:

```text
crates/formualizer-eval/src/engine/formula_ingest.rs
```

Public enough for `formualizer-workbook` backends to call Engine without reaching
into FormulaPlane internals:

```rust
#[derive(Clone, Debug)]
pub struct FormulaIngestRecord {
    pub row: u32, // 1-based public/ingest coordinate
    pub col: u32, // 1-based public/ingest coordinate
    pub ast: ASTNode,
    pub formula_text: Option<Arc<str>>,
}

#[derive(Clone, Debug)]
pub struct FormulaIngestBatch {
    pub sheet_name: String,
    pub formulas: Vec<FormulaIngestRecord>,
}
```

These are formula-ingest records, not FormulaPlane authority decisions.

### 2.5 Formula ingest report

Add:

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FormulaIngestReport {
    pub mode: FormulaPlaneMode,
    pub formula_cells_seen: u64,
    pub graph_formula_cells_materialized: u64,

    pub shadow_candidate_cells: u64,
    pub shadow_accepted_span_cells: u64,
    pub shadow_fallback_cells: u64,
    pub shadow_templates_interned: u64,
    pub shadow_spans_created: u64,

    pub graph_formula_vertices_avoided_shadow: u64,
    pub ast_roots_avoided_shadow: u64,
    pub edge_rows_avoided_shadow: u64,

    pub graph_vertices_created: u64,
    pub graph_edges_created: u64,

    pub fallback_reasons: BTreeMap<String, u64>,
}
```

Engine fields:

```rust
last_formula_ingest_report: Option<FormulaIngestReport>,
formula_ingest_report_total: FormulaIngestReport,
```

Accessors:

```rust
pub fn last_formula_ingest_report(&self) -> Option<&FormulaIngestReport>;
pub fn formula_ingest_report_total(&self) -> &FormulaIngestReport;
```

`AdapterLoadStats` remains backend-only. Do not add FormulaPlane accepted/fallback
fields there.

### 2.6 Engine formula ingest coordinator

Add to `Engine`:

```rust
pub fn ingest_formula_batches(
    &mut self,
    batches: Vec<FormulaIngestBatch>,
) -> Result<FormulaIngestReport, ExcelError>;
```

Implementation order:

1. Count `formula_cells_seen`.
2. If `formula_plane_mode == Shadow`, run shadow analysis using scratch authority.
3. In all modes for Tranche 3, pass **all** formulas to `BulkIngestBuilder`.
4. Store last/aggregate report.
5. Return report.

Off-mode behavior:

```text
formula_cells_seen == graph_formula_cells_materialized
graph path equivalent to current direct BulkIngestBuilder usage
```

Shadow-mode behavior:

```text
shadow_* counters may be nonzero
graph_formula_cells_materialized == formula_cells_seen
baseline graph formula counts match Off control
```

### 2.7 Shadow candidate analysis

Add a helper, preferably under `formula_plane/authority.rs` or as a graph method:

```rust
pub(crate) fn analyze_formula_plane_shadow_candidates(
    graph: &mut DependencyGraph,
    batches: &[FormulaIngestBatch],
) -> FormulaIngestReport;
```

Coordinate conversion:

```text
FormulaIngestRecord row/col are 1-based.
FormulaPlacementCandidate row/col are 0-based.
```

Candidate creation:

```rust
let sheet_id = graph.sheet_id_mut(&batch.sheet_name);
FormulaPlacementCandidate::new(
    sheet_id,
    row.saturating_sub(1),
    col.saturating_sub(1),
    Arc::new(ast.clone()),
    formula_text.clone(),
)
```

Grouping policy is Engine/graph-owned, not backend-owned:

1. Canonicalize each candidate with its placement anchor.
2. Reject unsupported canonical templates into fallback counts.
3. Group remaining by `(sheet_id, canonical_payload)`.
4. Split each group into connected components by Manhattan adjacency.
5. Call existing `place_candidate_family(&mut scratch_plane, component)` for each component.
6. Aggregate counters from `FormulaPlacementReport`.

This grouping is intentionally conservative. It avoids causing two disjoint runs
with the same payload to be rejected as one gapped family. L-shaped components
may still fallback via `UnsupportedShapeOrGaps`, which is acceptable in Shadow.

### 2.8 Ingest call sites to route

Route all current formula-batch sinks through `Engine::ingest_formula_batches`.

Required call sites:

```text
crates/formualizer-eval/src/engine/eval.rs
  Engine::build_graph_all
  Engine::build_graph_for_sheets

crates/formualizer-workbook/src/backends/calamine.rs
  stream_into_engine eager_formula_batches finalization

crates/formualizer-workbook/src/backends/umya.rs
  stream_into_engine eager_formula_batches finalization

crates/formualizer-workbook/src/backends/json.rs
  stream_into_engine eager_formula_batches finalization, if present
```

If JSON is not routed in the same patch, explicitly document the deferral. Do not
silently leave a third eager formula path bypassing the coordinator.

Deferred staged formula paths should preserve formula text:

```rust
formula_text: Some(Arc::<str>::from(key.clone()))
```

Eager backend paths may initially pass `None` if formula text is not retained in
the current `eager_formula_batches` shape, but prefer retaining text where cheap.

### 2.9 Workbook diagnostics

Add a thin optional diagnostic wrapper:

```rust
pub fn last_formula_ingest_report(&self) -> Option<formualizer_eval::engine::FormulaIngestReport>;
pub fn formula_ingest_report_total(&self) -> formualizer_eval::engine::FormulaIngestReport;
```

Clone-returning accessors are easier across crate boundaries. Keep this clearly
diagnostic.

### 2.10 Tranche 3 tests

Engine tests, suggested file:

```text
crates/formualizer-eval/src/engine/tests/formula_plane_ingest_shadow.rs
```

Required:

```text
formula_plane_off_ingest_matches_existing_bulk_outputs
formula_plane_off_report_counts_graph_materialized_formulas
formula_plane_shadow_reports_row_run_candidates_but_materializes_graph
formula_plane_shadow_outputs_match_off_control
formula_plane_shadow_fallback_reasons_report_unsupported_formulas
formula_plane_shadow_deferred_build_graph_all_reports_candidates
formula_plane_shadow_build_graph_for_sheets_reports_only_selected_sheet
shadow_get_formula_still_uses_staged_or_graph_ast
```

Backend/workbook tests if fixtures are practical:

```text
workbook_load_shadow_outputs_match_control
workbook_shadow_adapter_stats_remain_backend_only
```

### 2.11 Tranche 3 exit criteria

```text
FormulaPlaneMode::Off is default and behavior-equivalent.
Engine formula ingest paths are centralized through ingest_formula_batches.
Calamine/umya/json eager formulas call the coordinator or have explicit deferral.
Shadow mode reports accepted/fallback candidates.
Shadow mode still graph-materializes every formula.
Shadow outputs and graph formula counts match Off controls.
AdapterLoadStats remains backend-only.
Graph-owned FormulaAuthority exists but is not runtime authority yet.
```

## 3. Tranche 4 — Pure partial dirty closure

### 3.1 Goal

Implement changed-region fixed-point closure over producer/read indexes, without
mutating graph dirty flags, scheduling work, or evaluating formulas.

Input:

```text
changed regions
FormulaConsumerReadIndex
producer result-region provider
```

Output:

```text
FormulaProducerWork with dirty domains
changed result regions emitted by dirty producers
stats/fallbacks
```

### 3.2 Add WholeResult projection

Tranche 1/2 only added affine cell projection. Tranche 4 needs a way to model
legacy/range consumers where any source change in a read region dirties the
consumer's whole result.

Extend:

```rust
pub(crate) enum DirtyProjectionRule {
    AffineCell { row: AxisProjection, col: AxisProjection },
    WholeResult,
}
```

`WholeResult` semantics:

```text
if changed intersects read_region:
  ProjectionResult::Exact(ProducerDirtyDomain::Whole)
else:
  ProjectionResult::NoIntersection
```

This enables pure tests for:

```text
Span(B) -> Legacy(D1 = SUM(B1:B100))
```

without graph integration.

### 3.3 Dirty-domain merge API

Current `ProducerDirtyDomain::merge` mutates without reporting growth. Add:

```rust
pub(crate) fn merge_changed(&mut self, other: Self) -> bool;
```

Semantics:

```text
returns true iff the represented dirty domain grew or changed
Whole absorbs all
Cells + Cells dedup sparse cells without widening
Regions + Regions dedup regions
Cells + Regions converts to Regions only when mixed representation is necessary
```

Keep `merge` as a wrapper if useful.

### 3.4 Dirty closure types

Add under `producer.rs` or split to:

```text
crates/formualizer-eval/src/formula_plane/dirty.rs
```

Recommended types:

```rust
pub(crate) struct FormulaDirtyClosure {
    pub(crate) work: Vec<FormulaProducerWork>,
    pub(crate) changed_result_regions: Vec<Region>,
    pub(crate) stats: FormulaDirtyClosureStats,
    pub(crate) fallbacks: Vec<FormulaDirtyFallback>,
}

pub(crate) struct FormulaDirtyClosureStats {
    pub(crate) input_changed_regions: usize,
    pub(crate) read_index_query_count: usize,
    pub(crate) read_index_candidate_count: usize,
    pub(crate) exact_filter_drop_count: usize,
    pub(crate) projection_exact_count: usize,
    pub(crate) projection_conservative_count: usize,
    pub(crate) projection_no_intersection_count: usize,
    pub(crate) projection_unsupported_count: usize,
    pub(crate) merged_dirty_domains: usize,
    pub(crate) emitted_changed_regions: usize,
    pub(crate) duplicate_changed_regions_skipped: usize,
    pub(crate) fixed_point_iterations: usize,
}

pub(crate) struct FormulaDirtyFallback {
    pub(crate) consumer: FormulaProducerId,
    pub(crate) changed_region: Region,
    pub(crate) reason: ProjectionFallbackReason,
}
```

### 3.5 Producer result-region provider

Dirty closure needs a producer result region to emit downstream changed regions.
Keep this pure with a trait or closure callback:

```rust
pub(crate) trait FormulaProducerResultProvider {
    fn producer_result_region(&self, producer: FormulaProducerId) -> Option<Region>;
}
```

For tests, implement the trait for a fixture map or use a callback:

```rust
compute_dirty_closure(
    consumer_reads: &FormulaConsumerReadIndex,
    changed_regions: impl IntoIterator<Item = Region>,
    result_region: impl Fn(FormulaProducerId) -> Option<Region>,
) -> FormulaDirtyClosure
```

Do not require `DependencyGraph` in Tranche 4.

### 3.6 Dirty closure algorithm

```text
queue = changed regions
seen_changed = set(changed regions)
dirty_by_producer = Map<FormulaProducerId, ProducerDirtyDomain>

while queue not empty:
  changed = pop_front(queue)
  result = consumer_reads.query_changed_region(changed)
  update query stats

  for candidate in result.matches:
    match candidate.dirty:
      Exact(dirty):
        merge dirty into dirty_by_producer[candidate.consumer]
        if merge changed:
          emit result regions from dirty domain

      Conservative { dirty, reason }:
        record/count conservative fallback
        merge and emit as above

      NoIntersection:
        count and ignore

      Unsupported(reason):
        count and record FormulaDirtyFallback
        do not silently dirty
```

Emitting downstream changed regions:

```rust
let result_region = result_region_provider(candidate.consumer)
    .ok_or_else(|| fallback MissingProducerResultRegion)
for region in dirty.result_regions(result_region) {
    if seen_changed.insert(region) {
        queue.push_back(region)
        changed_result_regions.push(region)
    }
}
```

Add an iteration cap as a defensive guard. Hitting it should produce an
unsupported/fail-closed report in runtime callers later. In pure tests it should
never trigger.

### 3.7 Projection filtering invariant

`FormulaConsumerReadIndex::query_changed_region` returns candidates with a
projection result attached. Dirty closure must filter:

```text
Exact -> dirty work
Conservative -> dirty work + reason count
NoIntersection -> ignore
Unsupported -> fallback record, no silent dirty omission
```

Do not treat `NoIntersection` as dirty work. Do not silently ignore
`Unsupported` in runtime-adjacent APIs.

### 3.8 No value-range expansion guardrail

Dirty closure must query formula-consumer indexes only. It must not enumerate raw
value cells in large ranges.

Rules:

```text
No SheetIndex::vertices_in_col_range in dirty closure.
No graph collect_range_dependents_for_rect in dirty closure.
No per-cell expansion of whole/open value ranges.
Output size is producer-bound, not value-range-size-bound.
```

Whole/open regions may appear as `Region::WholeCol`, `WholeRow`, or
`WholeSheet`; closure should query indexes with those regions, not expand them.

### 3.9 Tranche 4 tests

Suggested pure tests in `producer.rs` or `dirty.rs`:

```text
dirty_closure_same_row_source_dirties_single_span_cell
dirty_closure_shifted_ref_dirties_neighbor_result_cell
dirty_closure_absolute_ref_dirties_whole_span_exactly
dirty_closure_composes_span_to_span_single_cell
dirty_closure_reaches_legacy_range_consumer_after_span_cell_dirty
dirty_closure_merges_sparse_cells_without_widening
dirty_closure_filters_no_intersection_candidates
dirty_closure_records_unsupported_projection_without_silent_dirty
dirty_closure_dedups_fixed_point_regions
dirty_closure_whole_col_changed_region_is_producer_bounded_not_value_bounded
dirty_closure_affine_projection_no_under_return_bruteforce_small_grid
```

The key regression guard is:

```text
A50 -> B50 -> C50
```

not:

```text
A50 -> whole B span -> whole C span
```

### 3.10 Tranche 4 exit criteria

```text
changed regions produce dirty-domain FormulaProducerWork through FormulaConsumerReadIndex
exact affine projections do not under-dirty
span-to-span dirty domains compose without widening
legacy whole-result consumers can be represented via WholeResult projection
NoIntersection and Unsupported are handled explicitly
closure reaches fixed point with dedupe
whole/open range queries do not expand raw value cells
no Engine/graph/scheduler/eval behavior changes occur
```

## 4. Recommended implementation sequence

Do not combine all work in one large runtime patch. Use four subtranches:

### Tranche 3A — Off-mode coordinator

- Add `FormulaPlaneMode::{Off, Shadow}` default Off.
- Add `FormulaIngestRecord`, `FormulaIngestBatch`, `FormulaIngestReport`.
- Add `Engine::ingest_formula_batches` Off-only delegation.
- Route `Engine::build_graph_all` / `build_graph_for_sheets`.
- Tests prove Off behavior and report counts.

### Tranche 3B — Shadow analysis and backend routing

- Add `FormulaAuthority` shell in graph.
- Add scratch shadow analyzer.
- Route calamine/umya/json eager batches through coordinator.
- Tests prove Shadow reports candidates but graph/output behavior matches Off.

### Tranche 4A — Dirty closure substrate

- Add `WholeResult` projection.
- Add `merge_changed`.
- Add dirty closure report/stats/fallback types.
- Implement pure fixed-point closure.
- Basic same-row/span-to-span/legacy-consumer tests.

### Tranche 4B — Dirty closure hardening

- Brute-force affine no-under-return tests.
- Whole/open range no-expansion tests.
- Unsupported/no-intersection tests.
- Sparse merge and fixed-point dedupe tests.

## 5. Validation gates

Each code subtranche should pass:

```bash
cargo fmt --all -- --check
cargo clippy -p formualizer-eval --all-targets -- -D warnings
cargo test -p formualizer-eval formula_plane --quiet
cargo test -p formualizer-eval computed_flush --quiet
cargo test -p formualizer-eval rangeview_ --quiet
cargo test -p formualizer-eval --quiet
```

If workbook backend routing is changed, also run relevant workbook tests or the
narrowest available `formualizer-workbook` test target.

## 6. Stop conditions

Stop and replan if an implementation attempts to:

```text
make Shadow skip graph formula materialization
let calamine/umya/json decide span acceptance
add graph proxy span vertices
wire spans into Scheduler/evaluate_all
make whole-span dirty the default exact path
silently ignore ProjectionResult::Unsupported
expand whole/open value ranges into per-cell dirty work
change AdapterLoadStats semantics to include FormulaPlane authority
```
