# FP6 Dirty Projection And Sidecar Region-Index Shore-Up

Date: 2026-05-03  
Branch: `formula-plane/bridge`  
Scope: technical contract for FormulaPlane sidecar region indexes and dirty projection. No source implementation in this report.

## Verdict

FP6.3 is startable only as a pure FormulaPlane sidecar-index and dirty-projection substrate under `crates/formualizer-eval/src/formula_plane/`. It is not ready to participate in normal recalculation until FP6.1/FP6.2 provide runtime span authority and FP6.5 defines scheduling. The design should be made stricter around coordinate convention, role-specific indexes, exact filtering, changed-region extraction, and epoch handling before any build agent touches graph-adjacent dirty routing.

The core contract is:

```text
changed region
  -> SpanDependencyIndex candidate entries
  -> exact filter against authoritative precedent regions
  -> DirtyProjection maps changed source region to target placements
  -> effective-domain filter subtracts intrinsic masks and FormulaOverlay punchouts
  -> SpanDirtyStore unions by span id + generation/version
```

Index lookup may over-return. It must never under-return. Projection may over-dirty. It must never under-dirty.

## 1. Source Reality Check

Relevant existing source is useful but passive or graph-specific:

- `crates/formualizer-eval/src/formula_plane/dependency_summary.rs` has passive `FormulaDependencySummary`, `PrecedentPattern::Cell(AffineCellPattern)`, instantiated run summaries, `FiniteRegion`, and a reverse query path. It proves the family of logic but is keyed by sheet names and 1-based coordinates and only handles the current static pointwise subset.
- `crates/formualizer-eval/src/formula_plane/span_store.rs` builds passive `FormulaRunStore` descriptors from scanner candidates. It is not runtime span authority and uses `FormulaRunId`, `source_template_id`, sheet names, and row/col coordinates from diagnostics.
- `crates/formualizer-eval/src/engine/plan.rs` uses `RangeKey` for compact graph dependency planning. `Rect` uses `SheetId + AbsCoord` where `AbsCoord::row()` and `AbsCoord::col()` are 0-based; `WholeRow` and `WholeCol` are converted to 0-based when added to graph range deps.
- `crates/formualizer-eval/src/engine/graph/range_deps.rs` already uses coarse row/column/block stripe indexes, then exact-filters candidate dependents against authoritative range records. FormulaPlane should preserve that pattern rather than replace graph range deps.
- `crates/formualizer-eval/src/engine/interval_tree.rs` provides a BTreeMap-backed interval helper over `u32` intervals with cloned `HashSet<T>` query payloads. It is acceptable as a first 1D substrate if payload sizes are small and tests prove no under-return.

The runtime sidecar should reuse ideas, not IDs or authority records, from the passive modules.

## 2. Coordinate Convention

### 2.1 Canonical runtime coordinates

Use a single FormulaPlane runtime coordinate convention for all sidecar indexes:

```rust
pub(crate) struct RegionKey {
    sheet_id: SheetId,
    row: u32, // 0-based engine coordinate
    col: u32, // 0-based engine coordinate
}

pub(crate) struct RectRegion {
    sheet_id: SheetId,
    row_start: u32, // inclusive, 0-based
    row_end: u32,   // inclusive, 0-based
    col_start: u32, // inclusive, 0-based
    col_end: u32,   // inclusive, 0-based
}
```

Rationale:

- `AbsCoord::row()` / `AbsCoord::col()` are 0-based.
- graph stripes, `IntervalTree`, Arrow chunks, and RangeView paths operate naturally in 0-based engine coordinates.
- conversion to user-facing A1/Excel 1-based coordinates is already explicit through `AbsCoord::from_excel` and display helpers.

This differs from passive FP4 `FiniteRegion`, which stores sheet names and 1-based coordinates. Runtime adapters must convert passive records at the boundary. Do not mix passive `FiniteRegion` directly into runtime indexes.

### 2.2 Boundaries and conversions

Allowed conversion boundaries:

| Source | Conversion into runtime region |
|---|---|
| `AbsCoord` | `RegionKey { sheet_id, row: coord.row(), col: coord.col() }` |
| `RangeKey::Rect` | use `start.row()/col()` and `end.row()/col()`, normalized inclusive |
| `RangeKey::WholeRow` | convert stored row to 0-based at boundary; document whether incoming `RangeKey` row is 1-based in the caller path |
| `RangeKey::WholeCol` | convert stored col to 0-based at boundary; document whether incoming `RangeKey` col is 1-based in the caller path |
| passive `FiniteCell` / `FiniteRegion` | subtract 1 after validating nonzero, resolve sheet name to stable `SheetId` |
| A1/user API | parse to `AbsCoord`, then use 0-based runtime region |
| structural row/col API | define API-specific conversion once, then emit runtime regions |

Required guard tests:

```text
region_key_from_abscoord_is_zero_based
finite_region_to_runtime_rect_rejects_zero_coordinates
range_key_rect_to_runtime_rect_preserves_abscoord_bounds
range_key_whole_row_col_conversion_is_documented_and_tested
```

### 2.3 Sheet identity

Runtime sidecar indexes must use stable `SheetId`, not sheet display names. If a passive summary or loader hint references a sheet name, promotion to runtime authority requires name resolution to `SheetId` and a sheet-generation/epoch dependency. Sheet rename/delete must invalidate or demote affected span/index entries.

## 3. RegionSet And RegionPattern Vocabulary

### 3.1 RegionSet

Use a small region set for query/event boundaries:

```rust
pub(crate) enum RegionSet {
    Empty,
    One(RegionPattern),
    Many(SmallVec<[RegionPattern; 4]>),
    WholeWorkbook, // structural/global fallback only, not normal dirty authority
}
```

`RegionSet` queries should dedupe payloads after querying each member. `WholeWorkbook` must be counted and should generally force legacy/global fallback or full FormulaPlane rebuild; it is not an acceptable representation for routine span dependency authority.

### 3.2 RegionPattern

Use shape-specific patterns so indexes do not encode every region as a rectangle:

```rust
pub(crate) enum RegionPattern {
    Cell(RegionKey),
    RowInterval { sheet_id: SheetId, row: u32, col_start: u32, col_end: u32 },
    ColInterval { sheet_id: SheetId, col: u32, row_start: u32, row_end: u32 },
    Rect(RectRegion),
    WholeRow { sheet_id: SheetId, row: u32 },
    WholeCol { sheet_id: SheetId, col: u32 },
    WholeSheet { sheet_id: SheetId },
    Structural { sheet_id: SheetId, axis: StructuralAxis, start: u32, end: u32 },
}
```

`Structural` is not a normal dependency region. It exists so structural edit handlers can query impacted domains/overlays/dependency entries and then rebuild/demote. It should not be used to optimize formula dirty projection until structural transform rules are exact.

### 3.3 Normalization rules

- Normalize all finite intervals to `start <= end`.
- Singleton rects should be stored as `Cell` where possible.
- One-column rects should use `ColInterval`.
- One-row rects should use `RowInterval`.
- Whole-axis references should use explicit `WholeRow` / `WholeCol`, not giant bounded intervals.
- Open/unbounded or dynamic regions that cannot be represented by these variants are `UnsupportedUnbounded` for FormulaPlane promotion.

## 4. SheetRegionIndex<T> Contract

### 4.1 Proposed storage

```rust
pub(crate) struct SheetRegionIndex<T> {
    points: FxHashMap<(u32, u32), SmallVec<[T; 2]>>,
    col_intervals: FxHashMap<u32, IntervalTree<T>>, // key col, row interval payloads
    row_intervals: FxHashMap<u32, IntervalTree<T>>, // key row, col interval payloads
    rect_buckets: RectBucketIndex<T>,
    whole_cols: FxHashMap<u32, SmallVec<[T; 2]>>,
    whole_rows: FxHashMap<u32, SmallVec<[T; 2]>>,
    whole_sheet: SmallVec<[T; 2]>,
}
```

The existing `IntervalTree<T>` type takes `insert(low, high, value)` and `query(q_low, q_high)`. If reused directly, FormulaPlane should wrap it so callers do not see cloned `HashSet<T>` internals and so query counters/deduping are centralized.

### 4.2 Insert behavior

| Insert shape | Storage |
|---|---|
| `Cell` | `points[(row,col)]` |
| `ColInterval` | `col_intervals[col].insert(row_start,row_end,payload)` |
| `RowInterval` | `row_intervals[row].insert(col_start,col_end,payload)` |
| true finite `Rect` | `rect_buckets` by overlapping coarse blocks |
| `WholeCol` | `whole_cols[col]` |
| `WholeRow` | `whole_rows[row]` |
| `WholeSheet` | `whole_sheet` |
| `Structural` | structural side list/index owned by the caller, or explicit whole-axis buckets with a structural flag |

Do not encode whole rows/cols as `0..=u32::MAX` intervals in normal 1D maps. That leaks used-region assumptions and makes exact filtering ambiguous.

### 4.3 Query behavior

A query returns candidate payloads plus observability, not authoritative final answers:

```rust
pub(crate) struct RegionQueryResult<T> {
    pub(crate) candidates: SmallVec<[T; 8]>,
    pub(crate) candidate_count: u64,
    pub(crate) deduped_count: u64,
    pub(crate) exact_filter_drop_count: u64, // filled by role-specific wrapper
    pub(crate) bucket_hit_count: u64,
}
```

Base `SheetRegionIndex<T>` may over-return. Role-specific indexes must exact-filter against authoritative records before returning semantic results.

For a finite `Cell(r,c)` query, candidates are union of:

```text
points[(r,c)]
col_intervals[c].query(r,r)
row_intervals[r].query(c,c)
rect_buckets containing (r,c)
whole_cols[c]
whole_rows[r]
whole_sheet
```

For a `ColInterval(c, r0..r1)` query, candidates are union of:

```text
points in column c within r0..r1, only if this remains cheap or via a point secondary index
col_intervals[c].query(r0,r1)
row_intervals for rows touched, only when bounded by a small threshold; otherwise rely on rect/axis fallback or wrapper policy
rect_buckets overlapping the query rect
whole_cols[c]
whole_rows for rows touched, thresholded or handled by explicit broad-query path
whole_sheet
```

For a `RowInterval`, mirror the column logic.

For a true `Rect`, query rect buckets and explicit whole-axis/sheet buckets. Iterating every point/row/col in large rects is not acceptable without a cap. The first implementation may choose a simple exact fallback list for rare true rectangles, but it must be explicit and counted.

### 4.4 Rect bucket behavior

`RectBucketIndex<T>` should be deliberately simple:

```text
bucket_row = row / RECT_BUCKET_H
bucket_col = col / RECT_BUCKET_W
key = (bucket_row, bucket_col)
```

Insertion stores payload in every bucket overlapped by the authoritative rect. Query visits every bucket overlapped by the changed rect. It may over-return payloads whose authoritative rect does not intersect the changed rect; role wrappers must exact-filter.

Use the existing graph block-stripe constants only if they are accessible and fit the workload. Otherwise define FormulaPlane-local constants and counters. Do not expose bucket size as a semantic contract.

### 4.5 No-under-return requirement

For every payload whose authoritative region intersects the query region, `SheetRegionIndex<T>` plus the role-specific exact filter must return it. Tests should generate boundary cases across:

```text
same point
left/right/top/bottom edge touching
corner touching
disjoint in same bucket
disjoint in adjacent bucket
whole-row/whole-column cross intersections
whole-sheet intersections
```

## 5. Role-Specific Index Contracts

The three sidecar indexes share `SheetRegionIndex<T>` mechanics but answer different semantic questions. They must not be collapsed.

### 5.1 SpanDomainIndex

Question:

```text
Which span placement/result domains geometrically cover or intersect this cell/region?
```

Payload:

```rust
pub(crate) struct SpanDomainEntryRef {
    span_id: FormulaSpanId,
    span_generation: u32,
    span_version: u32,
    domain_kind: SpanDomainKind, // Placement or Result
    indexed_region: RegionPattern,
    plane_epoch: u64,
}
```

Contract:

- Built from runtime `SpanStore`, not passive `FormulaRunStore`.
- Stores placement domain entries for formula lookup/edit/punchout operations.
- Stores result-region entries separately if result regions can differ from placement domains. For initial scalar formulas, placement and result may be identical but the type should not assume that forever.
- Exact-filters against the current authoritative span domain/result region in `SpanStore` using span id + generation + version.
- Does not consult FormulaOverlay. A covered cell can still be punched out.
- Does not decide dirty projection.

Primary APIs:

```rust
find_span_at(cell: RegionKey) -> Vec<SpanDomainHit>
find_spans_intersecting(region: &RegionPattern) -> Vec<SpanDomainHit>
```

A `find_span_at` query may return multiple hits only during transitional/overlap states; steady-state authority should reject overlapping active spans unless the overlay/resolution layer has an explicit disambiguation rule.

### 5.2 SpanDependencyIndex

Question:

```text
Which span dependency entries may be affected by a changed precedent region?
```

Payload:

```rust
pub(crate) struct SpanDependencyEntryRef {
    entry_id: SpanDependencyEntryId,
    span_id: FormulaSpanId,
    span_generation: u32,
    span_version: u32,
    template_id: FormulaTemplateId,
    template_version: u32,
    dependency_summary_version: u32,
    precedent_region: RegionPattern,
    projection: DirtyProjection,
    plane_epoch: u64,
}
```

Contract:

- Built only from accepted, exact, bounded runtime dependency summaries.
- Does not index `UnsupportedUnbounded` entries.
- Exact-filters against the authoritative `precedent_region`, not against bucket keys.
- Produces `SpanDependencyHit` entries; projection to dirty domain is a separate step.
- Stale span/template/summary versions are dropped and counted as stale; the query must rebuild or fallback rather than silently returning no hit.
- A whole-span initial dirty policy is allowed only after a hit is discovered through a no-under-return query.

Primary APIs:

```rust
query_changed_region(changed: &RegionPattern) -> SpanDependencyQueryResult
project_hits(changed: &RegionSet, hits: &[SpanDependencyHit]) -> DirtyDomainDelta
```

### 5.3 FormulaOverlayIndex

Question:

```text
Which FormulaOverlay entries/punchouts intersect this cell/region?
```

Payload:

```rust
pub(crate) struct FormulaOverlayEntryRef {
    entry_id: FormulaOverlayEntryId,
    overlay_generation: u32,
    overlay_epoch: u64,
    region: RegionPattern,
    kind: FormulaOverlayEntryKind,
}
```

Contract:

- Built from `FormulaOverlay`, not Arrow value overlays.
- Exact-filters against authoritative overlay entry regions and generations.
- Supports bulk region queries for paste/clear/structural edit handling.
- Does not decide span splitting, normalization, or dirty projection.
- Its changes invalidate `SpanProjectionCache` and may create dirty events for affected result regions.

Primary APIs:

```rust
find_entry_at(cell: RegionKey) -> Vec<FormulaOverlayHit>
find_entries_intersecting(region: &RegionPattern) -> Vec<FormulaOverlayHit>
```

## 6. DirtyProjection Contract

`DirtyProjection` maps changed precedent regions to dirty placement/result domains after `SpanDependencyIndex` discovers a candidate. Projection must use the authoritative dependency entry plus the current effective span domain.

Effective target domain:

```text
effective_domain = span.placement_domain - intrinsic_mask - FormulaOverlay projection
```

For scalar initial spans, dirty placement domain and dirty result domain are the same. The types should still distinguish them:

```rust
pub(crate) struct DirtyDomain {
    span_id: FormulaSpanId,
    span_generation: u32,
    placement_domain: EffectiveDomainShape,
    result_region: RegionSet,
    exactness: DirtyExactness,
}
```

### 6.1 Projection table

| Projection | Exact when | Dirty result | Fallback policy |
|---|---|---|---|
| `WholeTarget` | any intersecting source cell/range truly affects all active placements, e.g. absolute scalar ref or fixed aggregate source | whole effective span | safe initial default for bounded summaries; count as whole-span dirty |
| `SameRow` | dependency maps source row to target row with no row offset and target domain has a row dimension | intersect changed rows with effective domain rows | if target shape cannot represent row subset, use `ConservativeWhole` |
| `SameCol` | dependency maps source col to target col with no col offset and target domain has a col dimension | intersect changed cols with effective domain cols | if target shape cannot represent col subset, use `ConservativeWhole` |
| `Shifted { row_delta, col_delta }` | source coordinate = placement/result coordinate + fixed delta on both axes | inverse-shift changed region by `-delta`, intersect effective domain | if inverse shift overflows or crosses unsupported shape, use whole or reject based on summary policy |
| `PrefixFromSource` | dependency is monotonic prefix where changed source before/inside span affects current and later placements along one axis | suffix of effective domain from changed source boundary | if axis, anchor, or monotonicity is not exact, use whole or reject |
| `SuffixFromSource` | dependency is monotonic suffix where changed source affects current and earlier/later placements as explicitly defined | prefix/suffix interval according to stored direction | same as prefix; direction must be encoded, not inferred |
| `FixedRangeToWhole` | fixed bounded range dependency is shared by every placement | whole effective span if changed intersects fixed range | reject if fixed range is unbounded or dynamic |
| `ConservativeWhole` | precedent region is bounded but exact placement mapping is unavailable or intentionally deferred | whole effective span | allowed but counted; should not hide unsupported summaries |
| `UnsupportedUnbounded` | dependency footprint cannot be bounded or represented safely | no sidecar entry | reject promotion or demote to legacy; do not use routine whole-workbook dirty as optimization |

### 6.2 Projection details

`WholeTarget` and `ConservativeWhole` are distinct. `WholeTarget` is semantically exact; `ConservativeWhole` is a safe over-dirty fallback for bounded but not-yet-specific projections. Observability should count both.

`Shifted` should cover the common pointwise relative case. For a formula family like `C_r = A_r`, the source coordinate is placement coordinate plus a fixed col delta. Querying changed `A10` inverse-shifts to target `C10` and intersects the span domain. This is exact when both axes are finite and affine.

`SameRow` / `SameCol` are specialized forms that may be easier to express for row-run/col-run spans. They should be implemented only where the target shape and source shape make the subset exact.

`PrefixFromSource` / `SuffixFromSource` must include axis and direction metadata, for example:

```rust
PrefixFromSource { axis: Row, source_to_target_delta: i32, inclusive: bool }
SuffixFromSource { axis: Row, source_to_target_delta: i32, inclusive: bool }
```

Do not infer prefix/suffix from formula text during projection. The dependency summary builder must prove it.

### 6.3 Dirty union rules

- Dirty domains are keyed by `(span_id, span_generation, span_version)`.
- Multiple hits for the same span union their dirty domains.
- If any hit produces whole effective span, the union is whole effective span.
- Stale dirty entries from older span generations are ignored and counted.
- Dirty projection must subtract FormulaOverlay punchouts at projection time or before evaluation, but candidate discovery must not skip a span just because some placements are punched out.

## 7. Changed-Region Extraction Sources

FormulaPlane dirty routing needs changed regions from engine operations, not only dirty vertices.

### 7.1 Value edit

Source:

```text
user edits value cell/region
```

Regions emitted:

- changed value region for graph/range dependents and `SpanDependencyIndex`.
- if edit lands inside a span-owned formula placement, FormulaOverlay creates a punchout; the affected result region is also dirty for downstream dependents.

### 7.2 Formula edit

Source:

```text
user edits formula cell/region
```

Regions emitted:

- old formula result region as changed, so downstream dependents of previous output dirty correctly.
- new formula result region after placement/materialization as changed, if the edit creates or replaces output authority.
- changed precedent summaries are not themselves changed regions; they rebuild index entries.

Formula edit inside a span is primarily a FormulaOverlay/placement event. It should not be modeled as source precedent dirty only.

### 7.3 Clear

Source:

```text
user clears cell/region
```

Regions emitted:

- cleared value/result region as changed for downstream dependents.
- FormulaOverlay `Cleared` entries for span-owned placements.
- index/projection-cache invalidation for overlay epoch.

### 7.4 Paste/block edit

Source:

```text
bulk paste values/formulas/mixed cells
```

Regions emitted:

- one or more changed regions covering pasted output cells.
- FormulaOverlay bulk punchout entries for span-owned placements.
- optional local repatterning events after paste; these rebuild spans/indexes and must not use stale dirty entries.

Do not degrade to per-cell span splits as the semantic operation.

### 7.5 Computed span result

Source:

```text
span evaluator writes computed result fragments
```

Regions emitted:

- span result region or dirty subdomain result region after flush.
- downstream graph/range dependents and other span dependency entries must see this as a changed precedent region.

This is the key bridge for span -> legacy and span -> span dependencies. It must be ordered by scheduler/proxy rules before runtime recalc uses spans.

### 7.6 Structural edit

Source:

```text
insert/delete rows/cols, sheet rename/delete/copy
```

Regions emitted:

- impacted span domains through `SpanDomainIndex`.
- impacted FormulaOverlay entries through `FormulaOverlayIndex`.
- impacted precedent regions through `SpanDependencyIndex`.
- broad changed result regions after shift/shrink/demote.

MVP policy: structural edits may demote/rebuild rather than transform, but they must not leave stale indexes or optimized spans alive through unsupported transforms.

## 8. Epoch, Rebuild, And Stale-Index Policy

### 8.1 Epochs

Track separate epochs so the sidecar can rebuild narrowly:

```rust
FormulaPlaneEpoch          // any runtime authority mutation
SpanStoreEpoch             // span add/remove/normalize/demote/domain change
TemplateStoreEpoch         // template/dependency-summary change
FormulaOverlayEpoch        // punchout/override/clear/materialization change
SheetTopologyEpoch         // sheet rename/delete/copy or sheet-id generation change
StructuralGridEpoch        // row/col insert/delete affecting coordinates
```

Index records should store `built_from_*` epochs relevant to their role:

| Index | Required epoch dependencies |
|---|---|
| `SpanDomainIndex` | `SpanStoreEpoch`, `SheetTopologyEpoch`, `StructuralGridEpoch` |
| `SpanDependencyIndex` | `SpanStoreEpoch`, `TemplateStoreEpoch`, `SheetTopologyEpoch`, `StructuralGridEpoch` |
| `FormulaOverlayIndex` | `FormulaOverlayEpoch`, `SheetTopologyEpoch`, `StructuralGridEpoch` |
| `SpanProjectionCache` | `SpanStoreEpoch`, `FormulaOverlayEpoch`, intrinsic mask epoch |

### 8.2 Query-time stale behavior

A sidecar index query must never silently return stale empty results. Choose one of these behaviors per API:

```rust
enum IndexQueryStalePolicy {
    RebuildBeforeQuery,
    ReturnStaleErrorForCallerFallback,
}
```

Rules:

- mutation-capable engine paths should rebuild before query when the rebuild is bounded.
- read-only diagnostic paths may return a counted stale error.
- normal dirty routing must not ignore stale errors; it must rebuild or conservatively fallback/demote.

### 8.3 Payload version checks

Even if index epochs match, exact filtering should validate payload versions:

```text
span id generation matches current SpanStore slot
span version matches indexed entry or entry can be recomputed
summary/template version matches dependency entry
FormulaOverlay entry generation matches current overlay slot
sheet id generation remains live
```

Mismatches are counted as stale drops and should trigger rebuild in mutation paths.

### 8.4 Incremental update policy

Allowed incremental updates in early phases:

- adding/removing a span entry during controlled placement/demotion.
- adding/removing a FormulaOverlay entry for a single cell or bulk region.
- marking `SpanDirtyStore` for changed regions.

Use rebuild instead of incremental update for:

- structural edits;
- normalization/splitting/merging;
- sheet rename/delete/copy;
- template dependency-summary changes;
- any operation where old and new regions are not trivially known.

## 9. Test-First Acceptance List

Suggested primary locations:

```text
crates/formualizer-eval/src/formula_plane/region_index.rs
crates/formualizer-eval/src/formula_plane/dirty.rs
crates/formualizer-eval/src/engine/tests/formula_plane_dirty.rs
```

### 9.1 Coordinate and conversion tests

```text
region_key_from_abscoord_is_zero_based
finite_region_to_runtime_rect_subtracts_one_and_resolves_sheet_id
finite_region_to_runtime_rect_rejects_zero_coordinates
range_key_rect_to_runtime_rect_preserves_abscoord_bounds
range_key_whole_row_to_runtime_region_uses_documented_base
range_key_whole_col_to_runtime_region_uses_documented_base
sheet_name_based_passive_summary_requires_sheet_id_resolution
sheet_rename_invalidates_sheet_id_generation_for_indexes
```

### 9.2 SheetRegionIndex no-under-return tests

```text
sheet_region_index_point_query_returns_point_interval_rect_axis_and_sheet_hits
sheet_region_index_col_interval_query_returns_overlapping_col_intervals
sheet_region_index_row_interval_query_returns_overlapping_row_intervals
sheet_region_index_rect_bucket_query_returns_all_intersecting_rects
sheet_region_index_rect_bucket_overreturns_disjoint_same_bucket_candidate
sheet_region_index_whole_col_intersects_cell_and_col_interval_queries
sheet_region_index_whole_row_intersects_cell_and_row_interval_queries
sheet_region_index_whole_sheet_intersects_all_query_shapes
sheet_region_index_boundary_touching_edges_are_intersections
sheet_region_index_disjoint_adjacent_edges_do_not_survive_exact_filter
```

### 9.3 Role separation tests

```text
span_domain_index_finds_row_run_owner_without_overlay_semantics
span_domain_index_finds_result_region_separately_from_placement_domain
span_dependency_index_returns_dependency_entries_not_span_domain_hits
formula_overlay_index_finds_punchout_without_returning_domain_or_dependency_hits
role_indexes_do_not_share_payload_types_or_query_result_types
```

### 9.4 Exact-filter tests

```text
span_domain_index_exact_filter_drops_stale_span_generation
span_dependency_index_exact_filter_drops_rect_bucket_overreturn
span_dependency_index_exact_filter_drops_stale_template_summary_version
formula_overlay_index_exact_filter_drops_removed_overlay_entry_generation
region_query_exact_filter_drop_count_is_reported
```

### 9.5 DirtyProjection tests

```text
whole_target_dirty_marks_whole_effective_span
conservative_whole_dirty_is_counted_separately_from_exact_whole
same_row_projection_marks_matching_rows_only_when_exact
same_col_projection_marks_matching_cols_only_when_exact
shifted_projection_inverse_maps_changed_source_to_target_domain
shifted_projection_overflow_falls_back_or_rejects_by_policy
fixed_range_to_whole_marks_whole_span_on_intersection
fixed_range_to_whole_ignores_unrelated_edit
prefix_from_source_marks_suffix_interval_when_monotonicity_proven
suffix_from_source_marks_prefix_or_suffix_by_encoded_direction
unsupported_unbounded_summary_does_not_enter_span_dependency_index
multiple_changed_regions_union_dirty_domain_once
formula_overlay_punchout_is_subtracted_from_effective_dirty_domain
stale_dirty_entry_from_old_span_generation_is_ignored_and_counted
```

### 9.6 Changed-region extraction tests

```text
value_edit_emits_changed_region_for_graph_and_span_dependency_index
value_edit_inside_span_also_creates_overlay_punchout_dirty_region
formula_edit_emits_old_and_new_result_regions_for_downstream_dependents
clear_inside_span_emits_cleared_result_region_and_overlay_epoch_change
paste_block_emits_bulk_regions_and_bulk_overlay_queries
computed_span_result_emits_result_region_for_downstream_legacy_dependents
structural_edit_marks_domain_dependency_and_overlay_indexes_stale
```

### 9.7 Oracle/no-under-approx integration tests

```text
same_row_dependency_edit_marks_candidate_span_dirty_whole
absolute_dependency_edit_marks_candidate_span_dirty_whole
whole_column_dependency_query_marks_candidate_span
rect_dependency_query_exact_filters_bucket_candidates
unrelated_edit_does_not_mark_span_dirty
unsupported_dependency_prevents_span_index_entry
sidecar_dirty_has_zero_under_approx_against_dependency_summary_fixture
sidecar_dirty_has_zero_under_approx_against_graph_range_dependents_for_supported_shapes
```

The existing `dependency_summary.rs` reverse-query tests are good reference material. Runtime tests should not reuse passive IDs as authority; they should compare sidecar outputs to expected regions or to graph planner/range-dependency oracles.

## 10. Observability Required For FP6.3

Add counters before behavior integration:

```text
span_domain_index_entries
span_dependency_index_entries
formula_overlay_index_entries
region_query_count
region_query_candidate_count
region_query_deduped_count
region_query_exact_filter_drop_count
rect_bucket_hit_count
whole_axis_hit_count
index_rebuild_count
index_stale_epoch_count
stale_payload_drop_count
dirty_projection_whole_exact_count
dirty_projection_conservative_whole_count
dirty_projection_partial_exact_count
dirty_under_approx_oracle_misses
unsupported_unbounded_rejection_count
```

Any FP6.3 acceptance test that claims may-affect routing should assert `dirty_under_approx_oracle_misses == 0`.

## 11. Non-Goals

- Do not implement runtime span scheduling or evaluation in FP6.3.
- Do not add graph-native `DepTarget::SpanProxy` as a prerequisite for the sidecar index substrate.
- Do not replace existing graph range dependencies or stripe indexes.
- Do not invent a broad R-tree/geometry dependency for M1.
- Do not promote FormulaPlane region/index types to `formualizer-common`.
- Do not support volatile, dynamic, spill, reference-returning, name/table, local-environment, open-range, or internal-dependency formulas through sidecar dirty authority without separate exact contracts.
- Do not treat passive `FormulaRunStore` / `FormulaRunId` / sheet-name `FiniteRegion` as runtime authority.
- Do not implement partial dirty projections before whole-span conservative routing proves no-under-return.

## 12. Circuit Breakers

Stop and replan if an implementation:

- uses mixed 1-based and 0-based coordinates inside sidecar indexes without explicit conversion tests;
- keys runtime sidecar indexes by sheet display name instead of stable `SheetId` plus sheet-generation invalidation;
- collapses `SpanDomainIndex`, `SpanDependencyIndex`, and `FormulaOverlayIndex` into one map or one payload type;
- lets a query under-return a true intersection;
- skips exact filtering after rect bucket, stripe, whole-axis, or interval over-return;
- treats whole-span dirty as a substitute for no-under-return candidate discovery;
- indexes `UnsupportedUnbounded` dependencies and relies on routine whole-workbook dirty to preserve correctness;
- returns stale empty results when an index epoch is behind the authoritative store;
- subtracts FormulaOverlay punchouts during candidate discovery instead of during formula resolution/effective-domain projection;
- hooks sidecar dirty results into normal recalc before scheduler/proxy ordering is defined;
- reuses passive scanner/run IDs or lossy template IDs as runtime span/dependency authority;
- hides fallback, stale-index, exact-filter-drop, or conservative-whole counters.

## 13. Recommended FP6.3 Dispatch Boundary

First build agents should implement only:

```text
RegionKey / RegionPattern conversion tests
SheetRegionIndex<T> unit tests and substrate
role-specific wrapper types with exact-filter tests
DirtyProjection pure unit tests
SpanDirtyStore union semantics by span generation/version
observability counters
```

They should not touch:

```text
Engine normal dirty propagation
Scheduler
Evaluator
ComputedWriteBuffer
public/default configuration
```

Graph-adjacent dirty integration should wait until:

1. FP6.1 runtime stores and generational IDs exist;
2. FP6.2 can create accepted spans from exact bounded summaries;
3. FP6.3 no-under-return and stale-index tests pass;
4. FP6.5 scheduler/proxy seam is specified for any normal recalc execution.
