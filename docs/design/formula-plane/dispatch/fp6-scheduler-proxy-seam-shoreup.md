# FP6 Scheduler/Proxy Seam Shore-Up

Date: 2026-05-03  
Branch: `formula-plane/bridge`  
Scope: FormulaPlane span work scheduling relative to legacy graph vertices. Report only; no production code changes.

## Verdict

Recommendation: **sidecar mixed work-item scheduler first; graph proxy vertex later only if the sidecar seam proves insufficient**.

The first normal-runtime seam should not add `VertexKind::FormulaSpanProxy` or `DepTarget::SpanProxy` to the graph. Instead, FormulaPlane should build a temporary recalc work graph containing:

```rust
pub enum FormulaPlaneWorkItem {
    Legacy(VertexId),
    Span(FormulaSpanId),
}
```

and schedule those work items in FormulaPlane/engine-adapter code. The legacy graph remains the may-affect router and source of legacy vertex dependencies; FormulaPlane supplies span dependency/result-domain edges. This preserves the architecture preference that the graph remains mostly span-agnostic while still giving FP6.5 a concrete ordering model for `legacy -> span`, `span -> legacy`, and `span -> span` dependencies.

Important staging constraint: FP6.4 span evaluation may remain direct/test-only until this seam exists. No normal `evaluate_all` path should execute span tasks until a mixed work-item schedule or equivalent proxy seam has passing ordering tests.

## 1. Current Scheduler And Graph Reality Check

### VertexId assumptions

Current scheduling is `VertexId`-only:

- `Scheduler::create_schedule(&[VertexId])` returns `Schedule { layers: Vec<Layer>, cycles: Vec<Vec<VertexId>> }`.
- `Layer` is `Vec<VertexId>` only.
- `Scheduler::create_schedule_with_virtual` accepts extra dependencies as `FxHashMap<VertexId, Vec<VertexId>>`, still only `VertexId -> VertexId`.
- The scheduler reads dependencies through `DependencyGraph::dependencies_slice` / `get_dependencies` and dependents through `dependents_slice` / `get_dependents`.
- `Engine::evaluate_all`, `evaluate_all_with_delta`, cancellation paths, demand-driven paths, and `RecalcPlan` all evaluate graph vertex layers.

There is no existing span task identity, span layer, span proxy vertex, span dirty set, or FormulaPlane work item in source.

### Layer and cycle behavior

Current scheduler behavior:

- Tarjan SCC detects cycles only among scheduled `VertexId`s.
- A singleton SCC is cyclic only if `graph.has_self_loop(vertex)`.
- Acyclic SCCs are topologically layered by graph dependencies and graph dependents.
- `create_schedule_with_virtual` adds temporary incoming dependencies for existing vertices, but cannot represent a non-vertex span task.
- `Engine::evaluate_all` handles cycles by writing `#CIRC!` to cycle vertices and mirroring those vertex values to the computed overlay path.

FormulaPlane spans will be invisible to existing cycle detection unless FP6.5 creates either a sidecar mixed scheduler or graph proxy vertices.

### Dirty entry points

Current dirty propagation is graph-local:

- `DependencyGraph::mark_dirty(vertex_id)` marks formula dependents dirty and uses range dependency collection for changed cell/rect effects.
- Value cells are affected sources but are not scheduled; dirty evaluation vertices are filtered to formula/name vertex kinds.
- `DependencyGraph::get_evaluation_vertices()` returns dirty or volatile graph formulas only.
- Bulk spill clearing has a graph helper that marks many value cells and range dependents in one pass.
- Current range-dependent dirty routing uses stripe candidates followed by exact overlap checks against `formula_to_range_deps`.

FormulaPlane needs a changed-region seam, not just a changed-vertex seam:

```text
value edit / formula edit / clear / paste / span result flush / structural edit
  -> changed RegionSet
  -> graph legacy dependents
  -> FormulaPlane span dependents
```

The graph already has private range-dependent collection for rects; FP6.5 should add a narrow internal adapter rather than expose FormulaPlane internals to graph core.

### Static schedule cache and plan behavior

Current schedule caching is graph-only:

- `Engine::cached_static_schedule` stores `topology_epoch`, candidate `VertexId`s, and a `Schedule`.
- `can_use_static_schedule_cache` allows cache use only when candidate graph vertices are not dynamic and have no graph range dependencies.
- `RecalcPlan` stores a graph `Schedule` plus `has_dynamic_refs`.

Any normal runtime FormulaPlane path must either disable these caches when span work exists or extend cache keys with FormulaPlane epochs and work items. The first implementation should disable graph-only schedule reuse for FormulaPlane-active recalc.

## 2. M1 Options

### Option A: sidecar mixed work-item scheduler

Shape:

```rust
pub enum FormulaPlaneWorkItem {
    Legacy(VertexId),
    Span(FormulaSpanId),
}

pub struct FormulaPlaneSchedule {
    pub layers: Vec<FormulaPlaneWorkLayer>,
    pub cycles: Vec<Vec<FormulaPlaneWorkItem>>,
}

pub struct FormulaPlaneWorkLayer {
    pub items: Vec<FormulaPlaneWorkItem>,
}
```

The builder constructs a temporary dependency graph over only the current recalc work set:

```text
Legacy(VertexId) -> Legacy(VertexId) from graph dependencies
Span(id) -> Legacy(v) when span depends on a dirty legacy formula result
Legacy(v) -> Span(id) when legacy formula depends on a dirty span result
Span(dst) -> Span(src) when dst span precedents intersect src span result region
```

The edge direction follows the current scheduler convention: the left item depends on the right item and must run after it.

Pros:

- Preserves graph-as-router/backbone without making graph span-native.
- Avoids introducing `VertexKind::FormulaSpanProxy` before span authority, dirty projection, and punchout semantics are proven.
- Keeps span-specific projection, overlay holes, effective domains, demotion, and counters inside FormulaPlane.
- Can be test-driven as an internal/default-off path with no public behavior changes.
- Lets the no-span/default path keep the existing scheduler unchanged.

Cons:

- Requires a small generic Tarjan/topological scheduler over `FormulaPlaneWorkItem` or an adapter that builds an explicit adjacency map.
- Must carefully mirror graph scheduler semantics around cycles, cancellation, parallel layer barriers, and schedule cache invalidation.
- Needs narrow graph accessors for dependency coordinates and region-dependent dirty routing.

### Option B: graph proxy vertex

Shape:

```rust
VertexKind::FormulaSpanProxy
FormulaSpanProxy { vertex_id: VertexId, span_id: FormulaSpanId, result_region: RegionSet }
```

The graph would schedule one proxy vertex per accepted span. The evaluator would dispatch proxy vertices to the FormulaPlane span evaluator.

Pros:

- Reuses existing `Scheduler`, cycle detection, layer evaluation entry points, and some cache invalidation semantics.
- Existing virtual dependency machinery remains `VertexId -> VertexId`.
- Downstream legacy formulas can depend on a graph object.

Cons:

- Forces graph core to understand span-owned formula authority earlier than necessary.
- Risks converting a sidecar architecture into graph-native span targets before FormulaOverlay, partial dirty, demotion, and structural rules are stable.
- Requires graph edge construction from spans to legacy precedents and legacy dependents to spans, which is close to a native `DepTarget::SpanProxy` design.
- Makes span punchouts/holes awkward: one proxy covers a region, but effective domain changes with FormulaOverlay epochs.
- Can hide accidental per-placement materialization if agents add proxy plus per-cell graph formula vertices.

### Recommendation

Use **Option A: sidecar mixed work-item scheduler** for first implementation.

The graph proxy vertex should remain a later optimization or simplification candidate after FP6.5 proves which edges are actually needed and after FormulaOverlay/edit semantics are stable. The sidecar scheduler is more new code, but it keeps ownership boundaries correct and avoids prematurely making the graph span-native.

## 3. Ordering Model

### Work item dependency convention

Use the current scheduler's mental model:

```text
item -> dependency item
```

If `A` reads `B`, store an edge:

```text
A depends on B
A must be scheduled after B
```

### Legacy -> legacy

For `Legacy(v)` items, reuse graph dependencies among dirty legacy work:

```text
Legacy(v) -> Legacy(dep)
```

where `dep` is in the mixed work set and `dep` is a dirty/volatile formula vertex.

Do not schedule value cells. Value cells are precedent data only.

### Legacy -> span

A span task depends on legacy formulas when the span's precedent regions intersect dirty legacy formula result cells or result ranges:

```text
Span(S) -> Legacy(V)
```

Example:

```excel
B_r = legacy formula
C_r = B_r * 2       // accepted span
```

If `B10` is dirty and `C` span reads `B10`, schedule `Legacy(B10)` before `Span(C_span)`.

Required discovery:

- Start with the span's accepted dependency summary/precedent regions.
- Use graph sheet indexes and vertex coordinates to find dirty/volatile legacy formula vertices intersecting those precedent regions.
- Exact-filter coordinates/regions. Over-return is safe; under-return is not.
- Reject or demote if dependency summary is unbounded/dynamic.

### Span -> legacy

A legacy formula depends on a span when its dependency regions intersect a dirty span result region:

```text
Legacy(V) -> Span(S)
```

Example:

```excel
C_r = accepted span
D1 = SUM(C1:C100000)   // legacy formula
```

When `C` span is dirty, `D1` must be marked dirty before schedule construction and ordered after `Span(C_span)`.

Required discovery:

- Convert dirty span placement domains to dirty result regions.
- Ask graph for legacy dependents of those changed regions using a narrow internal changed-region API.
- Query direct dependents through existing graph vertices when placeholders exist.
- Query range dependents through existing range/stripe machinery.
- Do not require one graph vertex per span placement merely to dirty dependents; placeholders that already exist for legacy references are acceptable, but compact authority cannot depend on materializing the entire span.

### Span -> span

A span depends on another span when its accepted precedent region intersects the other span's dirty result region:

```text
Span(dst) -> Span(src)
```

Example:

```excel
C_r = accepted span reading A_r
D_r = accepted span reading C_r
```

If `C` is dirty and `D` reads `C`, schedule `Span(C)` before `Span(D)`.

Discovery uses FormulaPlane indexes only:

```text
dirty span result region
  -> SpanDependencyIndex candidate entries
  -> exact filter
  -> dirty projection
  -> SpanDirtyStore
```

Then the schedule builder adds `Span(dst) -> Span(src)` if both spans are in the work set.

### Same-layer execution

A mixed layer may contain legacy vertices and span tasks only if no dependency edge exists between them. For first implementation:

- Sequential mixed layer execution is acceptable.
- Parallel legacy vertex evaluation can remain inside legacy-only groups if existing layer semantics allow it.
- Span tasks should flush their `ComputedWriteBuffer` output before the next mixed layer begins.
- Downstream tasks must never read stale computed overlay/base lanes from a preceding span task.

## 4. Downstream Dirty Propagation From Span Results

Span result writes are changed regions. They must route exactly like value changes for downstream dependency purposes.

### Pre-schedule closure

Before constructing the mixed schedule, build a conservative closed work set:

```text
initial changed regions / dirty graph vertices / dirty spans
  -> graph legacy dependents
  -> span dependents
  -> repeat until no new dirty legacy vertices or spans
```

This prevents a downstream legacy formula from being omitted simply because the span result has not been written yet in this recalc pass.

### Narrow graph adapter

Add a narrow internal graph/engine seam later, conceptually:

```rust
fn mark_legacy_dependents_dirty_for_region(
    &mut self,
    region: RegionKey,
    reason: DirtyReason,
) -> Vec<VertexId>;
```

Requirements:

- Mark only formula/name dependents dirty, not every cell in the span result region.
- Cover direct existing cell vertices and graph range dependents.
- Exact-filter range candidate hits as current `collect_range_dependents_for_rect` does.
- Do not create formula vertices for every span placement.
- Count `legacy_dependents_dirtied_from_span_region` and `range_dependents_dirtied_from_span_region`.

### FormulaPlane sidecar routing

For span dependents:

```rust
fn mark_span_dependents_dirty_for_region(
    &mut self,
    changed: RegionKey,
    reason: DirtyReason,
) -> Vec<FormulaSpanId>;
```

Requirements:

- Query `SpanDependencyIndex`.
- Exact-filter over-returned candidates.
- Project into dirty domains.
- Union dirty domains by span id/generation.
- Ignore stale span generations and stale index epochs with counted rebuild/fallback.

### Post-eval dirtying

After a span task evaluates:

- The task writes through the computed write buffer/fragment path.
- The task reports dirty result regions actually written or conservatively the scheduled dirty result domain.
- Those regions can be used to mark downstream graph/span dependents for a follow-up loop only if the pre-schedule closure did not already include them or if dynamic/fallback behavior changed.

Initial static spans should prefer pre-schedule closure plus one mixed schedule. A follow-up loop is acceptable as a safety valve, but it must be bounded and counted.

## 5. Schedule Cache And Epoch Policy

### Existing graph cache

The existing `cached_static_schedule` is keyed by:

```text
topology_epoch + candidate VertexId list
```

It cannot safely cache a mixed legacy/span schedule.

### First FormulaPlane policy

For the first normal FormulaPlane runtime path:

```text
if formula_plane_enabled && active_span_work_exists:
  do not use cached_static_schedule
  do not use graph-only RecalcPlan
```

Counters:

```text
formula_plane_schedule_cache_disabled_count
formula_plane_mixed_schedule_build_count
formula_plane_work_item_count
formula_plane_span_work_item_count
formula_plane_legacy_work_item_count
```

### Later cache key

A later mixed schedule cache may key on:

```rust
struct FormulaPlaneScheduleCacheKey {
    graph_topology_epoch: u64,
    formula_plane_epoch: u64,
    span_index_epoch: u64,
    formula_overlay_epoch: u64,
    candidate_legacy_vertices_hash: u64,
    candidate_span_ids_with_generation_hash: u64,
    dirty_domain_shape_hash: u64,
}
```

Do not build this in M1. M1 should prefer correctness and explicit cache disablement.

### RecalcPlan

`RecalcPlan` currently stores graph `Schedule` only. First policy:

- If FormulaPlane is disabled or no accepted span authority exists, current `RecalcPlan` behavior remains unchanged.
- If FormulaPlane is enabled and active spans can be dirtied, `build_recalc_plan` should either return a graph-only plan marked FormulaPlane-inactive or reject/fallback to `evaluate_all` with a counted reason.
- Do not let `evaluate_recalc_plan` silently skip dirty spans.

Suggested counter:

```text
formula_plane_recalc_plan_fallback_count{reason="span_work_not_represented"}
```

## 6. Cycle And Internal-Dependency Policy

### Internal span dependencies

Initial runtime must reject or demote spans with internal dependencies before scheduling.

Reject if a span's accepted precedent summary can intersect its own result/placement domain in a way that is not classified as safe. Examples:

```text
C_r = C_r + 1        -> reject/demote
C_r = C_{r-1} + A_r  -> reject initially, maybe recurrence later
prefix/suffix self references -> reject initially unless explicitly classified
```

Do not depend on the mixed scheduler to discover every internal placement-level cycle; it schedules coarse spans, not per-placement recurrences.

### Mixed cycles

The mixed scheduler must detect cycles among `FormulaPlaneWorkItem`s.

Cycle types:

```text
legacy <-> legacy  -> existing behavior, #CIRC! on legacy vertices
span <-> legacy    -> demote span(s) in SCC and replan, or fallback before authority claim
span <-> span      -> demote involved spans and replan, unless an explicit safe recurrence class exists
span self-cycle    -> demote/reject; do not produce optimized output
```

For M1, any cycle involving a span should trigger demotion/fallback rather than attempting to evaluate the span. The demotion path must:

- remove or stale span sidecar index entries;
- clear span dirty state for stale generation;
- materialize or preserve legacy authority for affected placements only through the approved fallback path;
- mark downstream dependents dirty;
- increment `formula_plane_demotions{reason="cycle"}` or `fallback_reasons.conservative_cycle`.

If demotion is not implemented yet, normal-runtime span scheduling must remain disabled for fixtures that can form such cycles.

## 7. Acceptance Tests

Suggested primary location:

```text
crates/formualizer-eval/src/engine/tests/formula_plane_scheduler_seam.rs
```

Suggested unit-test locations for pure work-item scheduling:

```text
crates/formualizer-eval/src/formula_plane/scheduler.rs
crates/formualizer-eval/src/formula_plane/dirty.rs
```

### Default and no-span guards

```text
formula_plane_disabled_schedule_is_unchanged
formula_plane_enabled_without_dirty_spans_uses_existing_graph_schedule
formula_plane_active_spans_disable_graph_static_schedule_cache
formula_plane_recalc_plan_rejects_or_fallbacks_when_span_work_exists
```

### Mixed work-item ordering

```text
legacy_precedent_dirty_schedules_span_after_legacy
span_result_region_dirties_legacy_dependent_before_schedule
span_result_region_routes_range_dependent_legacy_formula
span_to_span_dependency_orders_precedent_before_dependent
mixed_scheduler_keeps_independent_legacy_and_span_items_in_same_or_parallelizable_layer
dirty_span_task_runs_once_per_span_not_once_per_placement
```

Expected assertion shape for the dense row-run case:

```text
accepted_span_cells = 100000
span_work_item_count = 1
per_placement_formula_vertices_created = 0
per_placement_ast_roots_created = 0
per_placement_edge_rows_created = 0
```

### Downstream propagation

```text
span_flush_marks_direct_legacy_dependent_dirty
span_flush_marks_range_legacy_dependent_dirty
span_flush_marks_downstream_span_dirty
pre_schedule_dirty_closure_includes_legacy_after_dirty_span
pre_schedule_dirty_closure_includes_span_after_dirty_span_result_region
```

### Correct read ordering

```text
computed_span_flush_happens_before_downstream_legacy_read
legacy_formula_reads_new_span_result_in_same_recalc
span_formula_reads_new_legacy_result_in_same_recalc
span_formula_reads_new_upstream_span_result_in_same_recalc
```

### Cycle and demotion tests

```text
internal_span_dependency_rejects_promotion
span_to_span_cycle_demotes_or_reports_conservative_cycle
legacy_span_cycle_demotes_span_and_replans
span_cycle_does_not_write_partial_computed_results
```

### Cache and epoch tests

```text
mixed_schedule_rebuilds_after_formula_plane_epoch_change
mixed_schedule_rebuilds_after_span_index_epoch_change
mixed_schedule_rebuilds_after_formula_overlay_epoch_change
stale_span_generation_dirty_entry_is_ignored_or_rebuilt
```

### Cancellation and delta parity

```text
formula_plane_mixed_schedule_cancellation_between_layers_is_safe
formula_plane_delta_includes_span_written_result_regions
formula_plane_default_delta_behavior_unchanged_when_disabled
```

## 8. Non-Goals

- Do not add public FormulaPlane scheduling APIs in FP6.5.
- Do not add `VertexKind::FormulaSpanProxy` or `DepTarget::SpanProxy` for M1.
- Do not make the graph inspect FormulaOverlay holes, span masks, or placement internals.
- Do not implement span-aware function kernels as part of scheduling.
- Do not support internal span recurrences in the first runtime path.
- Do not require loader-preserved shared-formula metadata for scheduler correctness.
- Do not optimize schedule caching before mixed scheduling correctness is proven.
- Do not claim performance wins from wall time until counters prove compact authority and one work item per span.

## 9. Circuit Breakers

Stop and replan if an implementation:

- executes a span task from normal `evaluate_all` before mixed ordering tests pass;
- evaluates all legacy vertices before spans or all spans before legacy vertices without dependency classification;
- represents accepted spans by creating one graph formula vertex per placement;
- adds graph-native span proxy vertices before a deliberate architecture decision;
- allows graph-only `cached_static_schedule` or `RecalcPlan` to skip dirty span work;
- routes span result dirty propagation only through per-cell graph vertices for every placement;
- fails to dirty downstream legacy range dependents after a span result changes;
- schedules a downstream legacy formula in the same or earlier layer than a span it reads;
- schedules a span before a dirty legacy formula result it reads;
- permits cycles involving spans to produce optimized outputs instead of demoting/falling back;
- evaluates internal span dependencies as if they were independent placements;
- writes span results without flushing before downstream reads;
- hides fallback/demotion/cache-disablement behind missing counters.

## 10. Implementation Staging Recommendation

1. Keep FP6.4 span evaluator direct/test-only until FP6.5 scheduling seam tests exist.
2. Add pure unit tests for `FormulaPlaneWorkItem` DAG scheduling under `formula_plane/`.
3. Add a narrow graph changed-region dependent adapter for legacy downstream dirty routing.
4. Build pre-schedule dirty closure over graph legacy vertices plus FormulaPlane spans.
5. Build mixed `FormulaPlaneSchedule` only when FormulaPlane is enabled and dirty span work exists; otherwise use the existing graph path unchanged.
6. Disable graph-only schedule cache and graph-only `RecalcPlan` when active span work exists.
7. Execute mixed layers with a barrier between layers; span result writes must flush before the next layer.
8. Treat span-involved cycles and internal span dependencies as demotion/fallback until explicit recurrence support is designed.

This path keeps the graph as the may-affect backbone, gives FormulaPlane enough authority to schedule compressed work items correctly, and avoids prematurely committing to graph-native span proxy architecture.
