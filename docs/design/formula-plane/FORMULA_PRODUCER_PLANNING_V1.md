# Formula Producer Planning V1

Date: 2026-05-04  
Status: design gate for FP6.5R; no runtime cut-over implied by this document.

## 1. Purpose

This document supersedes the earlier loose FP6.5 framing of "schedule
`Legacy(VertexId)` + `Span(FormulaSpanId)` work items" with a more precise
planning model:

```text
formula producers own result regions
formula consumers read regions
dirty propagation projects changed regions into producer dirty domains
mixed scheduling orders formula producers by region-derived dependencies
span evaluation receives dirty-domain-bounded work
```

The core requirement is **partial dirty from V1**. Whole-span dirty is allowed
only when it is semantically exact, or as an explicitly counted conservative
fallback. V1 must not bake in whole-span recomputation as the default runtime
shape.

This gate exists because the current engine scheduler is `VertexId`-only:

```text
Engine::evaluate_all
  -> graph.get_evaluation_vertices()              // Vec<VertexId>
  -> Engine::create_evaluation_schedule(...)
  -> Scheduler::create_schedule(&[VertexId])
  -> Schedule { layers: Vec<Layer { vertices }> }
  -> evaluate_layer_*_effects(VertexId)
```

FormulaPlane spans cannot safely enter normal recalc until we add a first-class
mixed producer planning layer.

## 2. Non-goals for V1

V1 does not attempt to support every spreadsheet feature as FormulaPlane span
authority. The following remain disabled or legacy-only initially:

- graph proxy vertices for spans;
- parallel span evaluation;
- cached mixed `RecalcPlan`;
- `evaluate_cells` / demand-driven mixed span execution;
- array/spill-producing spans;
- dynamic/`INDIRECT`/`OFFSET` span formulas;
- volatile span formulas;
- names/tables/structured references inside accepted spans;
- post-hoc graph-to-span optimize;
- structural edit exact span transforms;
- function-level span kernels.

Important nuance:

```text
partial dirty is not disabled
```

It is V1 architecture.

## 3. Ownership and layering

FormulaPlane is formula-authority infrastructure, not evaluation/writeback
infrastructure. The intended ownership split is:

```text
DependencyGraph / graph-owned FormulaAuthority
  owns formula definition authority:
    legacy formula vertices/AST ids
    FormulaPlane templates/spans/overlays
    producer result indexes
    consumer read indexes
    dirty metadata sufficient for planning

Engine
  owns evaluation mechanics:
    interpreter/evaluation context
    ComputedWriteBuffer
    computed overlay flush
    Arrow SheetStore / RangeView readback
    evaluate_all orchestration
```

This document intentionally does **not** propose graph proxy vertices. Proxy nodes
hide the regional structure needed for dirty-domain planning and future
span/chunk/function-level optimization.

Recommended internal shape:

```rust
pub(crate) struct FormulaAuthority {
    pub(crate) plane: FormulaPlane,
    pub(crate) producer_results: FormulaProducerResultIndex,
    pub(crate) consumer_reads: FormulaConsumerReadIndex,
    pub(crate) dirty: FormulaDirtyStore,
}
```

The exact module boundary may be under `formula_plane/` or `engine/`, but the
conceptual authority belongs with graph formula authority, not with loader
backends.

## 4. Core concepts

### 4.1 Formula producer

A formula producer is any formula authority that can produce one or more result
cells.

```rust
pub(crate) enum FormulaProducerId {
    Legacy(VertexId),
    Span(FormulaSpanId),
}
```

V1 result regions:

```text
Legacy(VertexId) -> singleton formula result cell
Span(FormulaSpanId) -> span ResultRegion / PlacementDomain
```

Longer term, singleton formulas may also become size-1 spans, but V1 does not
require that migration.

### 4.2 Producer work

Scheduling identity and dirty payload are separate:

```rust
pub(crate) struct FormulaProducerWork {
    pub(crate) producer: FormulaProducerId,
    pub(crate) dirty: ProducerDirtyDomain,
}
```

The topological node identity is `FormulaProducerId`; merged work carries the
combined dirty domain.

### 4.3 Dirty domains

V1 dirty domains must express subsets of producer result space:

```rust
pub(crate) enum ProducerDirtyDomain {
    Whole,
    Cells(Vec<RegionKey>),
    Regions(Vec<RegionPattern>),
}
```

Rules:

- `Cells` and `Regions` are result-space dirty regions at planner boundaries.
- Span eval maps result-space dirty regions to placements.
- Dirty domains merge without widening unless required by representation limits.
- Whole-producer dirty is allowed for exact fanout (for example `$A$1` used by
  all placements) or counted conservative fallback.

## 5. Index model

V1 needs two no-under-return region indexes.

### 5.1 Producer result index

Maps produced result regions to formula producers:

```rust
pub(crate) struct FormulaProducerResultIndex {
    index: SheetRegionIndex<FormulaProducerResultEntryId>,
    entries: Vec<FormulaProducerResultEntry>,
}

pub(crate) struct FormulaProducerResultEntry {
    pub(crate) producer: FormulaProducerId,
    pub(crate) result_region: RegionPattern,
}
```

Used by scheduling:

```text
consumer read region -> upstream formula producers whose outputs intersect it
```

This is the generalized form of existing range virtual dependency discovery in
`engine/virtual_deps.rs`, which currently finds dirty/volatile formula
`VertexId`s inside legacy range reads.

### 5.2 Consumer read index

Maps formula read/precedent regions to consuming formula producers:

```rust
pub(crate) struct FormulaConsumerReadIndex {
    index: SheetRegionIndex<FormulaConsumerReadEntryId>,
    entries: Vec<FormulaConsumerReadEntry>,
}

pub(crate) struct FormulaConsumerReadEntry {
    pub(crate) consumer: FormulaProducerId,
    pub(crate) read_region: RegionPattern,
    pub(crate) projection: DirtyProjectionRule,
}
```

Used by dirty propagation:

```text
changed region -> downstream consumers + projection metadata
```

V1 sources:

```text
Legacy(VertexId) read regions:
  existing graph dependency/range planning records

Span(FormulaSpanId) read regions:
  retained FormulaPlane span read summaries from accepted static pointwise templates
```

## 6. Dirty projection model

Dirty projection maps a changed precedent/source region into the subset of a
consumer's result region that must be recomputed.

```rust
pub(crate) enum ProjectionResult {
    Exact(ProducerDirtyDomain),
    Conservative {
        dirty: ProducerDirtyDomain,
        reason: ProjectionFallbackReason,
    },
    NoIntersection,
    Unsupported(ProjectionFallbackReason),
}
```

For accepted V1 spans, dependencies are affine cell precedents:

```text
source_row = placement_row + row_offset
source_col = placement_col + col_offset
```

or absolute axes:

```text
source_row = fixed_row
source_col = fixed_col
```

### Required V1 projection cases

#### Same-row / same-col relative references

```excel
B_r = A_r * 2
```

Edit:

```text
A50 -> dirty B50
```

#### Shifted references

```excel
B_r = A_{r-1} + A_r
```

Edit:

```text
A50 -> dirty B50 and B51
```

Projection must clip to the dependent span result domain.

#### Absolute references

```excel
B_r = $A$1 * A_r
```

Edits:

```text
A50 -> dirty B50
A1  -> dirty whole B span
```

The `$A$1` fanout is exact whole-span dirty, not a lazy default.

#### Mixed absolute/relative axes

```excel
C_{r,c} = $A_r + B$1
```

Projection may produce row or column regions. If a mixed-axis projection cannot
be represented without under-approximation, the candidate must remain legacy or
the projection must be an explicitly counted conservative result.

## 7. Dirty closure algorithm

V1 dirty closure is a fixed-point over changed regions and formula producers.

```text
queue: changed regions
dirty_work: Map<FormulaProducerId, ProducerDirtyDomain>

while queue not empty:
  changed = pop(queue)
  for read_hit in consumer_read_index.query(changed):
    projected = read_hit.projection.project(changed)
    if projected is Exact/Conservative dirty domain:
      merge dirty_work[read_hit.consumer]
      changed_result = result_region_for_dirty(read_hit.consumer, dirty domain)
      push changed_result if it contributes new changed cells/regions
    if projected is Unsupported:
      demote/fallback/fail closed according to policy
```

Example:

```excel
B_r = A_r * 2
C_r = B_r + 1
D1 = SUM(C1:C100000)
```

Edit:

```text
A50000
```

Expected V1 closure:

```text
A50000 changed
  -> B_span dirty B50000
  -> B50000 changed
  -> C_span dirty C50000
  -> C50000 changed
  -> Legacy(D1) dirty
```

This closure must not expand large raw value ranges. Raw values are not formula
producers; only formula producer result regions participate in scheduling
edges.

## 8. Mixed scheduling algorithm

Input:

```text
dirty_work: Map<FormulaProducerId, ProducerDirtyDomain>
```

For each dirty consumer, derive read regions required by its dirty domain. Then
query the producer result index:

```text
for consumer in dirty_work.keys():
  for read_region in required_read_regions(consumer, dirty_work[consumer]):
    for producer in producer_result_index.query(read_region):
      if producer in dirty_work:
        add edge producer -> consumer
```

Then run Tarjan/topological layering over `FormulaProducerId`.

```rust
pub(crate) struct MixedSchedule {
    pub(crate) layers: Vec<MixedLayer>,
    pub(crate) cycles: Vec<Vec<FormulaProducerId>>,
}

pub(crate) struct MixedLayer {
    pub(crate) work: Vec<FormulaProducerWork>,
}
```

V1 should implement a separate mixed planner first. Do not immediately rewrite
`engine/scheduler.rs` generically; the existing `Scheduler` remains the legacy
`VertexId` path until mixed execution is proven.

## 9. Evaluation integration scope

The first runtime integration is limited to sequential `Engine::evaluate_all`.

Initial mixed eval path:

```text
Engine::evaluate_all_mixed_sequential
  -> build mixed dirty closure
  -> build mixed schedule
  -> for each mixed layer:
       for each FormulaProducerWork:
         Legacy(VertexId): existing vertex/effects path
         Span(FormulaSpanId): span descriptor + dirty domain -> SpanEvaluator
       flush ComputedWriteBuffer at layer boundary
  -> clear graph dirty flags and FormulaPlane dirty work
```

Legacy work should reuse existing effects/writeback machinery:

```text
evaluate_vertex_immutable
plan_vertex_effects_with_computed_flush
apply_effect_with_computed_writes
flush_computed_write_buffer
```

Span work should write only through:

```text
SpanEvaluator -> ComputedWriteBuffer -> flush_computed_write_buffer
```

V1 flush policy is conservative:

- flush at mixed layer boundaries;
- flush before legacy range-dependent vertices when required by existing
  `flush_before_range_dependent_vertex` semantics;
- flush before any path that needs old/new value comparison for delta reporting.

## 10. Ingest and authority creation

Do not put FormulaPlane authority decisions in calamine or umya.

Backends may provide:

```text
sheet names
formula text
parsed ASTs if already parsed
cell coordinates
future shared-formula hints
```

Only graph/FormulaAuthority accepts or rejects span authority.

Introduce an Engine-level formula ingest coordinator seam even if the authority
lives in graph:

```rust
Engine::ingest_formula_batches(...) -> FormulaIngestReport
```

Modes:

```text
Off:
  current BulkIngestBuilder behavior

Shadow:
  run FormulaPlane candidate analysis/reporting
  still materialize all formulas through graph

Authoritative:
  accepted spans enter FormulaPlane authority
  fallback formulas go to BulkIngestBuilder
```

The coordinator should cover:

```text
Engine::build_graph_all
Engine::build_graph_for_sheets
calamine eager formula batches
umya eager formula batches
```

`AdapterLoadStats` remains backend-observation-only. FormulaPlane accepted/fallback
counts belong in Engine/graph diagnostics.

## 11. Public formula access

Before authoritative cut-over, add or route through an Engine formula-resolution
API:

```rust
Engine::get_formula_text(sheet, row, col) -> Option<String>
```

Resolution order:

```text
staged formula text
FormulaOverlay / FormulaPlane virtual formula
legacy graph AST
```

`Workbook::get_formula` should eventually call this API instead of manually
checking staged text and graph ASTs. This keeps enclosing-interface tests clean.

## 12. V1 acceptance scope

Accepted as FormulaPlane span producers:

- scalar static pointwise formulas;
- literals;
- cell references;
- supported unary/binary operators;
- relative/absolute/mixed cell refs with exact or explicitly conservative dirty
  projection;
- same-sheet/current-sheet only unless stable `SheetId` binding is explicitly
  implemented.

Rejected/remain legacy as span producers:

- `INDIRECT`, `OFFSET`, and other dynamic dependencies;
- volatile functions such as `RAND`, `NOW`, `TODAY`;
- array/spill-producing formulas/functions and spill refs;
- range-consuming formulas, including `SUM(A1:A10)`, whole/open ranges;
- named ranges;
- tables / structured refs;
- external and 3D references;
- unknown/custom/reference-returning functions without exact contracts;
- unsupported implicit intersection;
- internal/self span dependencies not explicitly proven safe.

Legacy formulas may consume span outputs, but the mixed planner must discover
those dependencies or demote/materialize the relevant span. Stale reads are not
allowed.

## 13. Special semantic hazards

### 13.1 Dynamic legacy consumers

Dynamic span producers are rejected. Legacy dynamic formulas may still read span
outputs:

```excel
B1:B100 = A_r * 2
C1 = INDIRECT("B50")
```

V1 must either:

1. extend dynamic virtual dependency collection to return `FormulaProducerId`,
   including spans; or
2. demote/materialize spans intersected by dynamic legacy refs.

It must not ignore the dependency.

### 13.2 Array/spill producers

Array/spill span producers are rejected. Legacy spill producers have multi-cell
result regions; V1 should either index current spill result regions as producer
outputs or demote/reject spans whose reads intersect spill regions.

Do not pretend a spill producer is only its anchor if downstream span reads can
observe spill children.

### 13.3 Names, tables, and structured refs

Span producers using names/tables/structured refs remain legacy in V1. Legacy
consumers that read span outputs through simple resolved range-backed names may
be supported if the graph can expose concrete read regions. Otherwise, demote or
fallback.

### 13.4 Whole/open legacy range consumers

Legacy formulas such as:

```excel
C1 = SUM(B:B)
```

must discover span producers intersecting the normalized read range without
expanding raw value cells. Existing used-bounds/open-range logic can bound the
query, but the query target is formula producers, not every value cell.

### 13.5 Formula overlays / punchouts

`FormulaOverlay` masks span authority. Dirty projection and span eval must
subtract overlay/punchout regions before evaluating span placements.

V1 rules:

- value override masks span output;
- cleared masks span formula authority;
- formula override becomes separate formula authority or fallback;
- `LegacyOwned(VertexId)` remains graph-owned authority;
- removing overlay restores span authority if the span remains active.

### 13.6 Cycles and false cycles

Region-derived planning may produce real cycles or coarse false cycles. V1
policy is fail-closed:

```text
if an SCC contains a span and cannot be proven safe at dirty-domain precision:
  demote involved spans, fallback/materialize, or reject authoritative scheduling
```

Do not report `#CIRC!` for possible coarse false cycles unless verified.

## 14. Current scaffold alignment

Already aligned:

```text
formula_plane/runtime.rs
  FormulaPlane, TemplateStore, SpanStore, FormulaOverlay, ResultRegion

formula_plane/placement.rs
  exact canonical family placement and StaticPointwise gate

formula_plane/dependency_summary.rs
  affine static pointwise summaries and rejection reasons

formula_plane/region_index.rs
  SheetRegionIndex, RegionPattern, DirtyDomain, DirtyProjection vocabulary

formula_plane/span_eval.rs
  dirty-domain-aware direct/test-only scalar evaluator
  ComputedWriteBuffer/fragment proof
```

Main gaps:

```text
retained span read summaries
producer result index covering legacy + spans
consumer read index covering legacy + spans
executable dirty projection
mixed dirty closure
mixed topological schedule
Engine evaluate_all mixed sequential path
ingest coordinator shadow path
public formula lookup via FormulaPlane resolution
```

## 15. Implementation tranches

### Tranche 0 — design gate

This document plus review. No runtime changes.

Exit criteria:

- `FormulaProducerId` model accepted;
- dirty-domain representation accepted;
- result/read index semantics accepted;
- projection semantics for relative/absolute/shifted refs accepted;
- reject/demote matrix accepted;
- cycle policy accepted;
- cache/RecalcPlan/evaluate_cells disablement accepted;
- acceptance tests accepted.

### Tranche 1 — pure producer/dirty substrate

Add inert internal types and pure tests:

```text
FormulaProducerId
ProducerDirtyDomain
FormulaProducerWork
FormulaProducerResultIndex
FormulaConsumerReadIndex
ProjectionResult
```

No behavior change.

### Tranche 2 — retained span read summaries and projections

Extend accepted spans to retain read summaries and projection metadata.

Implement projections for:

```text
same-row
same-col
shifted relative refs
absolute fanout
mixed absolute/relative axes where representable
```

No normal runtime scheduling yet.

### Tranche 3 — graph-owned authority + ingest shadow seam

Add graph-owned FormulaAuthority or equivalent wrapper.

Route formula ingest through an Engine coordinator in `Off` and `Shadow` modes.

Shadow mode reports would-accept/fallback spans while graph materializes all
formulas.

### Tranche 4 — partial dirty closure, pure/inert

Implement changed-region fixed-point dirty closure over producer/read indexes.

No evaluation yet.

### Tranche 5 — mixed scheduler, pure/inert

Implement Tarjan/topological layers over `FormulaProducerId` with dirty-domain
work payloads.

No evaluation yet.

### Tranche 6 — authoritative ingest split, guarded

Accepted spans skip graph formula materialization; fallback formulas remain graph
materialized. Runtime mixed eval remains guarded until Tranche 7.

### Tranche 7 — sequential `evaluate_all` mixed runtime

First actual runtime bridge:

```text
Engine::evaluate_all only
sequential only
schedule cache disabled
RecalcPlan disabled/fallback
no evaluate_cells mixed path
```

## 16. Mandatory acceptance tests

### Partial dirty precision

```text
edit_same_row_source_dirties_single_span_cell
edit_shifted_source_dirties_neighbor_result_cells
edit_absolute_source_dirties_whole_span_with_counted_reason
dirty_domain_composes_span_to_span_single_cell
dirty_domain_merge_keeps_sparse_cells_without_widening
dirty_domain_region_intersection_limits_span_eval_count
```

### Mixed producer planning

```text
producer_result_index_legacy_and_span_no_under_return
consumer_read_index_legacy_and_span_no_under_return
mixed_schedule_orders_legacy_before_dependent_span
mixed_schedule_orders_span_before_legacy_range_consumer
mixed_schedule_orders_span_before_span_consumer
mixed_schedule_detects_span_involved_cycles
```

### Engine integration

```text
formula_plane_disabled_evaluate_all_unchanged
formula_plane_active_spans_disable_graph_static_schedule_cache
formula_plane_recalc_plan_rejects_or_fallbacks_when_span_work_exists
edit_A50_updates_B_span_C_span_and_D1_with_single_cell_span_eval
span_result_region_dirties_legacy_direct_dependent
span_result_region_dirties_legacy_range_dependent
legacy_formula_result_dirties_dependent_span_region
outputs_match_legacy_control_for_static_pointwise_chain
```

### Unsupported semantics

```text
indirect_span_candidate_remains_legacy
offset_span_candidate_remains_legacy
volatile_span_candidate_remains_legacy
array_spill_span_candidate_remains_legacy
range_formula_span_candidate_remains_legacy
named_range_span_candidate_remains_legacy
structured_ref_span_candidate_remains_legacy
internal_span_dependency_rejects_promotion
```

### Public/enclosing interface tests

```text
workbook_load_shadow_outputs_match_control
workbook_get_formula_inside_accepted_span_returns_expected_virtual_formula_or_guarded_fallback
engine_baseline_stats_report_avoided_formula_vertices_when_authoritative_enabled
adapter_stats_remain_backend_observation_not_formula_plane_authority
```

## 17. First safe implementation target

The next code tranche after this design gate should not be runtime cut-over. It
should be:

```text
Tranche 1 + Tranche 2:
  FormulaProducerId / dirty-domain substrate
  producer/read indexes
  retained span read summaries
  executable partial dirty projection for V1 affine cell refs
```

Only after those are tested should mixed scheduling and `evaluate_all` integration
begin.
