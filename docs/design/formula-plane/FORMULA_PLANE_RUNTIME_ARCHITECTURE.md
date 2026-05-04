# FormulaPlane Runtime Architecture: Span Authority, Dirty Projection, and Punchouts

Status: **active runtime architecture draft**  
Branch: `formula-plane/bridge`  
Date: 2026-05-03

This document supersedes the forward-looking runtime portions of
`REPHASE_PLAN.md` FP5-FP7. Historical passive phases and reports remain valid;
this document is the new target architecture for moving FormulaPlane from
passive representation into opt-in runtime authority.

Runtime implementation that evaluates or writes span results is assumed to be
based on, or explicitly integrated with, eval-flush PR #95 / Phase 5. In
particular, FormulaPlane span evaluation must have access to:

```text
ComputedWriteBuffer
  -> coalesced chunk plans
  -> DenseRange / RunRange / SparseOffsets computed overlay fragments
  -> fragment-aware RangeView reads
```

If a worktree lacks those APIs, FP6.4+ may only do design/test scaffolding; it
must not claim fragment-backed runtime evaluation.

## 1. Core thesis

FormulaPlane is an opt-in compressed formula authority for repeated formula
families. It changes formula representation, dependency routing, scheduling
unit, and result writeback. It must not change observable spreadsheet semantics.

```text
FormulaPlane changes representation and batching.
It does not change formula semantics.
```

The graph remains the correctness/router/scheduler backbone for legacy formulas
and for conservative may-affect propagation. FormulaPlane plugs into that
backbone through graph-owned formula authority and sidecar region indexes: it
projects dirty regions into producer dirty domains and enables fragment-backed
computed results without graph proxy nodes in V1.

The required result/value substrate is supplied by eval-flush Phase 1-5:

```text
ComputedWriteBuffer
  -> coalesced DenseRange / RunRange / SparseOffsets computed fragments
  -> RangeView fragment-aware reads
```

The FPX bridge rerun with eval-flush Phase 5 validated the combined direction on
100k copied formulas:

```text
baseline full eval:      ~54.5 ms
spike + eval-flush eval:  ~5.7 ms
baseline RSS:           ~240 MB
spike + eval-flush RSS:  ~19 MB
```

That validation moves the next FormulaPlane work from passive exploration into
productionizing the opt-in compressed authority path.

## 2. Non-goals

The runtime architecture does not require or permit:

- public API behavior changes;
- hard `DependencyGraph` amputation;
- per-cell graph/AST/edge materialization for spans that claim compact
  authority;
- mandatory span-aware function kernels;
- loader/shared-formula preservation as a prerequisite for runtime authority;
- hidden fallback that is not counted;
- under-approximated dirty propagation;
- broad optimization of volatile/dynamic/opaque formulas in the first runtime
  path;
- promotion of FormulaPlane experimental types to `formualizer-common`.

Unsupported formulas stay on the legacy path. Supported formulas enter
FormulaPlane only when the template identity and dependency summary are exact
under the active contract.

## 3. Ownership model

Persistent FormulaPlane state should be ID-addressed and store-owned. Avoid
long-lived Rust lifetimes in persistent records.

```text
persistent data: owned stores + compact IDs
large immutable payloads: Arc-backed records
hot path: borrowed transient views
deltas/edits: overlay-owned punchouts
```

### 3.1 FormulaPlane store

```rust
pub struct FormulaPlane {
    templates: TemplateStore,
    spans: SpanStore,
    formula_overlay: FormulaOverlay,
    projection_cache: SpanProjectionCache,
    dep_index: SpanDependencyIndex,
    dirty: SpanDirtyStore,
    epoch: FormulaPlaneEpoch,
}
```

`FormulaPlane` owns the stores. It does not store references between stores.
Cross-store relationships use generational IDs.

### 3.2 TemplateStore

```rust
pub struct TemplateStore {
    records: Vec<TemplateRecord>,
    intern: FxHashMap<TemplateKeyHash, FormulaTemplateId>,
}

pub struct TemplateRecord {
    id: FormulaTemplateId,
    ast: Arc<ASTNode>,
    canonical_key: Arc<TemplateKey>,
    formula_text: Option<Arc<str>>,
    dependency_summary: Arc<TemplateDependencySummary>,
    compiled: OnceCell<Arc<CompiledTemplate>>,
}
```

Many spans can point to one template. Formula overrides should also intern into
`TemplateStore`; a one-off override still receives a `FormulaTemplateId` rather
than owning an inline AST in an exception record.

### 3.3 SpanStore

```rust
pub struct SpanStore {
    spans: Vec<SpanSlot>,
}

pub struct SpanSlot {
    generation: u32,
    span: Option<FormulaSpan>,
}

pub struct FormulaSpan {
    id: FormulaSpanId,
    generation: u32,
    sheet_id: SheetId,
    template_id: FormulaTemplateId,
    domain: PlacementDomain,
    result_region: ResultRegion,
    intrinsic_mask_id: Option<SpanMaskId>,
    state: SpanState,
    version: u32,
}
```

`FormulaSpan` is a compact arena row: IDs, domain, state. It must not own large
exception maps, ASTs, or dependency indexes inline.

### 3.4 FormulaOverlay

FormulaOverlay is the edit/punchout authority over spans.

```rust
pub struct FormulaOverlay {
    entries: FormulaOverlayStore,
    index: FormulaOverlayIndex,
    epoch: u64,
}

pub struct FormulaOverlayEntryRecord {
    id: FormulaOverlayEntryId,
    generation: u32,
    sheet_id: SheetId,
    domain: PlacementDomain,
    source_span: Option<(FormulaSpanId, u32 /* generation */)>,
    kind: FormulaOverlayEntryKind,
    created_epoch: u64,
}

pub enum FormulaOverlayEntryKind {
    FormulaOverride(FormulaTemplateId),
    ValueOverride,
    Cleared,
    LegacyOwned(VertexId),
    Unsupported(UnsupportedReason),
}
```

A value edit into a formula span writes both planes:

```text
formula plane: FormulaOverlay tombstone / ValueOverride
value plane:   user value overlay masks computed/base value
```

This prevents the span from recomputing over a user edit. FormulaOverlay is the
primary punchout mechanism; eager span splitting is a normalization choice, not
the edit semantic.

### 3.5 Masks and projections

```rust
pub enum SpanMask {
    Empty,
    SparseOffsets(Arc<[u32]>),
    Intervals(Arc<[Range<u32>]>),
    Bitmap(Arc<[u64]>),
}

pub struct SpanProjectionCache {
    entries: FxHashMap<(FormulaSpanId, u64 /* overlay_epoch */), SpanProjectionId>,
}
```

A span's effective domain is:

```text
PlacementDomain - intrinsic_mask - FormulaOverlay projection
```

Evaluation should work over borrowed effective-domain views. Normalization may
later split/merge/demote spans without changing observable formula authority.

## 4. Authority cascades

Formula definition authority:

```text
FormulaOverlay / user override
  > FormulaPlane span/template
  > legacy graph formula
```

Value authority remains:

```text
user/edit overlay
  > computed overlay
  > base lanes
```

These cascades are separate. A user edit to a formula span cell affects both
cascades: it creates a formula punchout and a value-plane edit.

Formula lookup uses a slightly more explicit resolution order because staged
edits may exist before they are committed to either graph or FormulaPlane:

```text
staged formula text / in-flight transaction
  > FormulaOverlay entry
  > FormulaPlane span/template virtual placement
  > legacy graph formula vertex
  > empty/no formula authority
```

Runtime FormulaPlane coordinates are internal engine coordinates:

```text
stable SheetId + 0-based row + 0-based column
```

User-facing A1/Excel coordinates and passive scanner reports convert at API or
adapter boundaries. Runtime authority must not depend on sheet display names;
sheet rename/delete either updates stable sheet mappings under an epoch or
demotes affected spans.

## 5. Placement domains

`PlacementDomain` is owned and compact for dense shapes; irregular domains point
to offset/mask stores.

```rust
pub enum PlacementDomain {
    RowRun { row_start: u32, row_end: u32, col: u32 },
    ColRun { row: u32, col_start: u32, col_end: u32 },
    Rect { row_start: u32, row_end: u32, col_start: u32, col_end: u32 },
    SparseOffsets { anchor_row: u32, anchor_col: u32, offsets_id: OffsetSetId },
}
```

Initial runtime support should implement `RowRun`, `ColRun`, and simple `Rect`.
The abstraction must not hard-code row runs everywhere; later periodic and
sparse domains should be representable without rewriting dirty/eval plumbing.

## 6. Graph integration model

The graph should remain mostly span-agnostic. It should provide conservative
may-affect routing and scheduling, not inspect every placement.

### 6.1 Initial integration: producer-region sidecar first

The first runtime path should use graph-owned FormulaPlane authority plus sidecar
region indexes:

```text
changed region
  -> legacy graph dependents / read summaries
  -> FormulaConsumerReadIndex entries
  -> projected FormulaProducerWork dirty domains
```

This avoids invasive graph rewrites while still making spans first-class formula
producers. The graph remains the legacy formula vertex authority; FormulaPlane
handles accepted span authority through producer/read-region indexes.

### 6.2 Deferred/non-V1 option: graph proxy targets

Graph proxy nodes are intentionally out of scope for V1. If a future design needs
proxy targets, it must be re-approved separately and must not replace the
producer-region planning contract in `FORMULA_PRODUCER_PLANNING_V1.md`.

Historical deferred shape:

```rust
enum DepTarget {
    Vertex(VertexId),
    SpanProxy(FormulaSpanId),
}
```

or one proxy vertex per span. This remains a post-V1 escape hatch only, not the
active implementation path.

### 6.3 Sidecar region-index substrate

FormulaPlane needs fast lookup for three distinct questions:

```text
cell/region -> spans whose placement/result domains intersect
changed precedent region -> span dependency entries that may be affected
cell/region -> formula overlay punchouts/exceptions
```

These are related but should not be collapsed into one map. The runtime should
own sidecar indexes that share region-index primitives:

```rust
pub struct SpanDomainIndex { /* placement/result ownership */ }
pub struct SpanDependencyIndex { /* precedent may-affect routing */ }
pub struct FormulaOverlayIndex { /* punchouts/exceptions */ }
```

The first implementation should be spreadsheet-shaped rather than a generic 2D
spatial tree:

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

Use the existing engine `IntervalTree` for one-dimensional row/column interval
queries if possible. If it is too graph-specific, factor only the small generic
interval primitive needed by FormulaPlane. Avoid inventing a broad geometry
engine before profiling demands it.

### 6.4 Index roles

`SpanDomainIndex` answers ownership/intersection questions:

```text
find_span_at(sheet,row,col)
find_spans_intersecting(region)
```

It is used for virtual formula lookup, edits/punchouts, structural transforms,
and local repatterning. It answers geometric span coverage only; FormulaOverlay
still decides whether a covered placement is punched out.

`SpanDependencyIndex` answers dirty may-affect questions:

```text
changed precedent region -> candidate span dependency entries
```

It indexes precedent regions instantiated from accepted span dependency
summaries. The index returns candidate entries; the entry's `DirtyProjection`
then maps the changed region to a dirty placement domain.

`FormulaOverlayIndex` answers edit-overlay questions:

```text
cell/region -> overlay exceptions/punchouts
```

It keeps formula punchout lookup fast and separates edit authority from span
normalization.

### 6.5 Query pipeline

Dirty routing is explicitly split into index lookup and projection:

```text
changed region
  -> SpanDependencyIndex candidate entries
  -> exact region-intersection filter if index over-returned
  -> DirtyProjection projects to DirtyDomain
  -> SpanDirtyStore unions dirty domains by span
```

The index may over-return. It must not under-return. Projection may be
conservative. Unsupported/unbounded summaries must not enter compact dependency
authority.

### 6.6 Rectangular and whole-axis dependencies

Most accepted spans should initially index into point maps or one-dimensional
interval trees:

```text
same-row / same-column refs
absolute cell refs
fixed column ranges
row ranges for period-style sheets
```

True 2D rectangular dependencies should initially use a coarse bucket index with
exact filtering, or a simple fallback list when the accepted-span count is small.
Whole-column, whole-row, whole-sheet, and structural dependencies should be
stored in explicit side buckets rather than forced into bounded interval trees.

### 6.7 Epochs and rebuild policy

FormulaPlane sidecar indexes are derived data. They should track epochs:

```rust
pub struct SpanDependencyIndex {
    built_from_plane_epoch: u64,
    epoch: u64,
}
```

On ordinary edits, update incrementally only when the operation is simple and
obvious. On structural edits or complex normalization, mark indexes stale and
rebuild from `SpanStore`, `TemplateStore`, dependency summaries, and
`FormulaOverlay`. Rebuilding over compressed spans should be cheap compared with
legacy per-cell graph materialization.

## 7. Dirty propagation

Dirty propagation is a two-stage process.

### 7.1 Stage 1: may-affect routing

The graph/range dependency machinery and FormulaPlane sidecar answer:

```text
Given changed region R, which legacy vertices or spans may be affected?
```

This stage may over-approximate. It must never under-approximate.

### 7.2 Stage 2: span dirty projection

For each candidate span, FormulaPlane projects the changed region through the
span dependency summary:

```rust
fn project_dirty(
    span: &FormulaSpan,
    summary: &SpanDependencySummary,
    changed: RegionSet,
) -> DirtyDomain;
```

Examples:

| Formula shape | Source edit | Dirty placement |
|---|---|---|
| `C_r = A_r * B_r` | `A10` | `C10` |
| `C_r = A_r * $F$1` | `F1` | whole `C` span |
| `C_r = SUM(A$1:A_r)` | `A10` | `C10:C_end` |
| `C_r = SUM($A$1:$A$10)` | `A5` | whole `C` span |

Active FP6.5R supersession: initial runtime must make partial dirty domains
first-class. Whole-span dirty is allowed only when exact, such as absolute fanout,
or as an explicitly counted conservative fallback. It is not the default V1
architecture.

### 7.3 DirtyProjection vocabulary

```rust
pub enum DirtyProjection {
    WholeTarget,
    SameRow,
    SameCol,
    Shifted { row_delta: i32, col_delta: i32 },
    PrefixFromSource,
    SuffixFromSource,
    FixedRangeToWhole,
    ConservativeWhole,
    UnsupportedUnbounded,
}
```

| Projection | Exact narrowing allowed when | Fallback |
|---|---|---|
| `WholeTarget` | any source overlap affects every active placement | whole span |
| `SameRow` | source and target placement share row identity exactly | whole span if row mapping is not exact |
| `SameCol` | source and target placement share column identity exactly | whole span if col mapping is not exact |
| `Shifted` | source-to-target affine delta is fixed and bounded | whole span if shifted region escapes domain |
| `PrefixFromSource` / `SuffixFromSource` | cumulative range direction is proven and monotonic | whole span or legacy if not monotonic |
| `FixedRangeToWhole` | fixed precedent range affects every placement | whole span |
| `ConservativeWhole` | precedent footprint is bounded but projection is not narrower | whole span |
| `UnsupportedUnbounded` | footprint is dynamic/open/opaque | reject promotion / remain legacy |

Unsupported/dynamic cases return `ConservativeWhole` only when the dependency
footprint itself is bounded. If the footprint cannot be bounded, the formula
remains legacy. The active FP6.5R design requires partial dirty to be first-class
from V1; whole-span dirty is permitted only when exact (for example absolute
fanout) or as an explicitly counted conservative fallback. See
`FORMULA_PRODUCER_PLANNING_V1.md`.

## 8. Scheduling and evaluation

The first normal-runtime scheduler seam is region-derived mixed formula-producer
planning, not graph-native span proxy vertices. The scheduler operates over
formula producers and dirty-domain work payloads:

```rust
pub enum FormulaProducerId {
    Legacy(VertexId),
    Span(FormulaSpanId),
}

pub struct FormulaProducerWork {
    producer: FormulaProducerId,
    dirty: ProducerDirtyDomain,
}
```

FormulaPlane/engine adapter code constructs a temporary recalc work graph over
only the current dirty producer set. The graph remains the source of legacy
formula metadata and may-affect routing; graph-owned FormulaPlane authority
supplies span result regions, retained read summaries, and dirty projection
metadata. Edges are derived by intersecting consumer read regions with producer
result regions.

Graph-native `DepTarget::SpanProxy` or `VertexKind::FormulaSpanProxy` remains out
of scope for V1.

A span task contains:

```rust
pub struct SpanEvalTask {
    span_id: FormulaSpanId,
    dirty: DirtyDomain,
    plane_epoch: u64,
    span_generation: u32,
}
```

The first evaluator is scalar-semantics-preserving:

```text
for placement in dirty effective domain:
  evaluate one stored template with existing interpreter/function semantics
  using a placement-relative current_sheet/current_cell context
  push result into ComputedWriteBuffer
flush computed writes into fragments at the scheduled flush boundary
```

This already captures the major win:

```text
one template
one producer work item with dirty-domain payload
compressed region-derived dependencies
fragment-backed result write
```

The evaluator must not create one graph formula vertex, AST root, dependency row,
or temporary formula object per accepted placement. Normal recalc integration
waits for scheduler-ordering tests; before that, evaluator work is direct or
test-only.

Optional span-aware function kernels are later accelerators, not a prerequisite.

## 9. Function opt-in model

Function span awareness follows the existing Arrow/RangeView pattern:

```text
scalar path remains default
span-aware path is optional and semantics-preserving
unsupported functions use scalar/fallback path
```

A future defaulted hook may look conceptually like:

```rust
fn eval_span(
    &self,
    args: SpanArgumentView,
    ctx: &mut SpanFunctionContext,
) -> Option<SpanResult> {
    None
}
```

But early runtime work should avoid a broad new function API. It should reuse
existing `ArgumentHandle`, `FunctionContext`, `RangeView`, and
`Function::dependency_contract(...)` concepts wherever possible.

## 10. Edits and punchouts

FormulaOverlay is the edit semantic. Splitting and merging spans are
normalization strategies.

| Operation against span placement | Formula authority effect | Value-plane effect | Dirty/index effect |
|---|---|---|---|
| value edit | create `ValueOverride` punchout | write user value overlay | dirty result cell/region dependents; invalidate overlay projection/index epoch |
| clear | create `Cleared` punchout | write explicit Empty or existing clear semantics | dirty result cell/region dependents; invalidate overlay projection/index epoch |
| same-template formula edit | remove compatible exception / reabsorb | clear stale computed result as needed | dirty placement; update overlay epoch |
| different supported formula edit | `FormulaOverride(template_id)` or local span candidate | clear stale computed result as needed | dirty placement; update overlay/index epochs |
| unsupported formula edit | `LegacyOwned(vertex_id)` or `Unsupported(reason)` | graph/value path owns result | remove span authority for placement; dirty downstream |
| paste/block edit | bulk overlay entries over affected region; optional local repattern | bulk user value/formula writes by current semantics | region dirty event; no per-cell eager span splitting |
| undo/rollback | restore prior overlay/value/graph state atomically | restore prior value-plane state | restore epochs or bump and rebuild derived indexes |
| demotion | create legacy-owned authority or remove span and materialize as scoped | legacy path owns values | stale/remove span/domain/dep/overlay projections; dirty downstream |

The effective span domain used by dirty projection and evaluation is:

```text
span.domain - intrinsic mask - active FormulaOverlay punchouts
```

Bulk paste/clear operations query `SpanDomainIndex` and `FormulaOverlayIndex` by
region. They should apply region punchouts/exceptions first and leave
normalization to a later exact pass. Do not eagerly split spans per cell.

## 11. Patterning and normalization

Pattern detection may be heuristic in representation choice; semantic grouping
must be exact.

```text
If detection misses a pattern: performance loss only.
If detection merges non-equivalent formulas: correctness bug.
```

Allowed normalization transforms:

```text
span + small holes -> keep span + overlay projection
span + interval holes -> split if cheaper/clearer
adjacent compatible spans -> merge
exceptions forming repeated pattern -> create new span
many exceptions -> demote affected region to legacy
```

All normalization is representation-only. It must preserve the authority
cascade.

Patterning sources:

1. loader/shared-formula metadata when available;
2. exact AST canonicalization/family grouping;
3. local repatterning after edits/pastes;
4. explicit/background global optimize pass.

## 12. Structural edits

Structural operations transform spans, formula overlays, dependency summaries,
and dirty indexes.

Common cases:

| Structural op | Span action |
|---|---|
| insert/delete before span | shift domain/result/dependencies |
| insert/delete inside span | expand/shrink, hole, split, or demote affected region |
| insert/delete precedent region | relocate template refs and recompute summary |
| unsupported transform | demote affected span/region to legacy |

Correct fallback is acceptable. Unsupported structural transforms must not keep a
stale optimized span alive.

Initial runtime policy may demote affected spans or regions on structural edits
rather than transforming them in place. Demotion must:

```text
remove/stale span authority
remove/stale sidecar index entries
clear span dirty state for stale generations
materialize or restore legacy formula authority only where needed
dirty downstream dependents of affected result regions
count the demotion reason
```

Exact shift/shrink/split/recanonicalize transforms belong to FP6.9 after oracle
coverage exists.

## 13. Virtual lookup and lazy materialization

The engine must be able to answer formula queries without materializing per-cell
vertices.

```rust
pub enum FormulaResolution {
    StagedFormula { text: Arc<str> },
    Overlay(FormulaOverlayEntryId),
    SpanPlacement {
        span_id: FormulaSpanId,
        span_generation: u32,
        template_id: FormulaTemplateId,
        placement: PlacementCoord,
    },
    LegacyVertex(VertexId),
    Empty,
    Stale,
}

pub struct FormulaHandle {
    resolution: FormulaResolution,
    plane_epoch: u64,
}
```

Formula lookup for a span-owned placement should return a virtual relocated
formula or AST view. It must not allocate a graph vertex simply because a caller
asked for formula text.

If a subsystem truly requires a concrete graph vertex, FormulaPlane may
materialize that placement or region as legacy through an explicit, counted
escape hatch:

```text
resolve span placement
relocate template AST/formula text for that placement
create FormulaOverlay LegacyOwned/FormulaOverride punchout
create graph formula vertex and dependency rows
remove/stale sidecar index entries for that placement
mark affected dependents dirty
count materialization reason
```

Lazy materialization is not the normal path and must never be hidden by optimized
diagnostics. Stale span/template/overlay handles must fail closed by rebuilding,
returning `Stale`, or demoting; they must not silently resolve to old authority.

## 14. Internal span dependencies

Internal span dependencies require explicit classification.

Examples:

```excel
C_r = C_{r-1} + A_r   // possible ordered recurrence
C_r = C_r + 1         // cycle
```

Initial runtime support should reject/fallback spans with internal dependencies.
Later phases may add known-safe recurrence classes.

```rust
pub enum SpanInternalDependencyKind {
    None,
    SamePlacement,
    PreviousRow,
    NextRow,
    Prefix,
    Arbitrary,
}
```

## 15. Observability

Every runtime phase must expose counters sufficient to prove compact authority is
actually active:

```text
formula cells scanned
accepted span cells
legacy fallback cells
span count
template count
span work item count
formula vertices avoided
AST roots avoided
edge rows avoided
per-placement formula vertices created
per-placement AST roots created
per-placement edge rows created
dirty span count
dirty domain shape
span dependency index entries
span domain index entries
formula overlay index entries
region-index query candidate count
region-index exact-filter drop count
formula overlay exception count
result fragment shape counts
computed write buffer push/flush counts
computed fragment cell count
rangeview fragment read count where available
fallback/demotion/materialization reasons
span eval ms
computed flush ms / bytes where available
```

No optimized diagnostic may silently hide legacy materialization.

## 16. Correctness invariants

1. Observable cell values must match the legacy engine for all supported spans.
2. Dirty propagation must never under-approximate.
3. Unsupported formulas remain legacy or demote before optimized authority is
   claimed.
4. FormulaOverlay has higher formula authority than spans.
5. User value overlay has higher value authority than computed fragments.
6. Explicit `Empty` masks lower layers.
7. Sparse gaps do not fill base cells.
8. Structural transforms either update spans correctly or demote.
9. Optional span-aware function kernels must match scalar semantics.
10. FormulaPlane must not require graph amputation to deliver the first runtime
    wins.
11. Sidecar indexes may over-return but must never under-return; over-returning
    queries must exact-filter against authoritative records.
12. Span result writes that claim runtime authority must use the eval-flush
    computed write buffer / fragment substrate.
13. Lazy materialization must be explicit, counted, and represented as a
    FormulaOverlay punchout before graph authority owns the placement.

## 17. Relationship to previous docs

Completed passive docs and reports remain valid historical inputs. This document
realigns forward runtime work:

```text
old forward FP5-FP7:
  graph-build hints -> first materialization reduction -> first span executor

new runtime path:
  hidden span authority -> sidecar region indexes
  -> region-derived formula producer planning with V1 partial dirty
  -> mixed FormulaProducerId scheduling, no graph proxy nodes
  -> dirty-domain span work through ComputedWriteBuffer/fragments
  -> punchouts/edit support -> normalization/function kernels
```

`REPHASE_PLAN.md` should be treated as historical plus passive-phase context; new
runtime implementation work should follow this architecture and the companion
`FORMULA_PLANE_IMPLEMENTATION_PLAN.md`.
