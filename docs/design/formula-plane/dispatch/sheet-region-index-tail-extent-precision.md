# SheetRegionIndex tail-extent precision: options

> Authored by `gpt-5.5` ideation pass, reviewed and adopted by PM. Anchors every claim in code with file:line refs.
>
> **Status:** workaround landed in v0.6.0-rc1 (`StructuralScope::Sheet` broadening for unbounded `Rect`). Phase 0 of the resolution lands in v0.6.x. Full Option E migration follows in v0.7-v0.8 per `option-e-execution-plan.md`.

## 1. The architectural gap, restated precisely

`RegionPattern::Rect(sheet_id, 0, u32::MAX, c, u32::MAX)` currently means an inclusive rectangle on one sheet: every row from `0` through `u32::MAX`, and every column from `c` through `u32::MAX`. The code treats `RectRegion` bounds as inclusive (`RectRegion::new` asserts `start <= end`, and `contains_key` uses `<=` on both ends; `region_index.rs:41-66`). `Engine::structural_col_region` constructs exactly that shape for column structural edits, and `structural_row_region` constructs the row-tail analog (`eval.rs:3904-3918`).

The existing whole-axis variants do not represent this shape. `WholeRow` is one row across all columns, `WholeCol` is all rows at one column, and `WholeSheet` is all rows and all columns (`RegionPattern` variants at `region_index.rs:74-96`; `axis_extents` maps them at `region_index.rs:227-229`). They are point-axis collapses, not range-axis tails. There is no current representation for "all columns from N onward" or "all rows from N onward" except a `Rect` with a sentinel high bound.

The predicate layer handles this because `RegionPattern::intersects` delegates to per-axis extent arithmetic (`region_index.rs:187-197`), and `AxisExtent::Span` intersection is pure `a_start <= b_end && b_start <= a_end` (`region_index.rs:249-256`). A `Span(c, u32::MAX)` is cheap there.

The index layer does not handle it. `SheetRegionIndex::index_entry` routes every `Rect` through `rect_buckets_for_rect` (`region_index.rs:440-447`), and bounded rect queries do the same in `collect_rect_candidates` (`region_index.rs:507-523`). `rect_buckets_for_rect` materializes a `Vec` containing every `(sheet_id, row_bucket, col_bucket)` pair from start bucket through end bucket (`region_index.rs:550-562`). With default bucket sizes `64 × 16` (`region_index.rs:324-325`), a `u32::MAX` tail creates an astronomically large bucket grid. **OOM observed at 87 GB anon-rss before completion.**

The current workaround is explicit: `structural_change_scope_for_region` broadens any `Rect` whose `row_end` or `col_end` is `u32::MAX` to `StructuralScope::Sheet` because the rect bucket grid cannot handle it (`eval.rs:3920-3933`). Recording a sheet scope inserts a `WholeSheet` changed region (`eval.rs:6026-6043`), and pending changed regions feed `compute_dirty_closure` during `DirtyClosure` seeding (`authority.rs:61-69`; `eval.rs:7373-7391`; `eval.rs:7638-7651`).

That broadening is correct because `WholeSheet` intersects every read region on the sheet. It loses precision because `project_changed_region` receives `WholeSheet`, `query_extents` maps it to `(All, All)` (`producer.rs:978-1014`), and relative affine projection maps `All` to the entire result axis (`producer.rs:607-615`; range analog at `producer.rs:648-656`). `WholeResult` and `WholeColumnRange` also return whole/result-region dirty domains after the initial intersection test (`producer.rs:491-505`). Result: every surviving span whose read region is on that sheet becomes dirty even when the precise tail rectangle is disjoint from that span's actual read/result footprint. The s035 50k-placement recompute delta is observed.

## 2. Solution sketches

### A. Half-open boundary variants

**Description.** Add explicit `RowsFrom { sheet_id, row_start }` and `ColsFrom { sheet_id, col_start }` region variants for structural tails.

**Representation.** Extend `RegionPattern` beyond the current seven variants (`region_index.rs:74-96`). Add `AxisExtent::From(u32)` or equivalent so `axis_extents` does not encode tails as `Span(_, u32::MAX)`. Add dedicated maps in `SheetRegionIndex`, e.g. `rows_from: FxHashMap<SheetId, BTreeMap<u32, Vec<usize>>>` and `cols_from: ...`, following the precedent of `whole_rows`, `whole_cols`, and `whole_sheets` (`region_index.rs:306-314`).

**Insertion.** Insert a `RowsFrom`/`ColsFrom` entry once, keyed by sheet and boundary. No bucket grid.

**Query.** Add `collect_tail_axis_candidates`. An indexed `ColsFrom(c)` intersects a query whose column extent reaches `c`; an indexed `RowsFrom(r)` intersects a query whose row extent reaches `r`. Tail queries against existing bounded rect buckets must not call `rect_buckets_for_rect`; they scan existing per-sheet rect buckets or use a capped range walk, then rely on `entry.region.intersects(&query)` as the exact filter (`region_index.rs:386-399`).

**Projection.** Extend `QueryAxisExtent` beyond `Span`/`All` (`producer.rs:923-942`) with `From`. Update `project_changed_axis` and `project_changed_range_axis` to treat `From` without overflowing offsets at `u32::MAX` (`producer.rs:607-615`; `producer.rs:648-656`). `bounded_extents` returns `None` for tail variants when they are result regions, matching whole-axis behavior (`producer.rs:944-976`).

**Touch surface.** Medium: `region_index.rs`, `producer.rs`, `eval.rs`, tests; roughly 300–600 LOC.

**Correctness.** Restores structural tail precision and makes the semantic shape first-class.

**Performance.** Insert O(1). Query O(number of tail entries plus bounded-index scan cost for tail queries); projection O(1) per matched entry.

**Sharp edges.** Variant proliferation starts immediately: `RowsTo`, `ColsTo`, bounded axis ranges with an unbounded opposite axis, and possibly open intervals follow.

### B. Bounded-rect substitution at insertion/query time

**Description.** Clamp `u32::MAX` to an application maximum before bucket enumeration.

**Representation.** No enum change. Introduce clamp constants, such as Excel's `1,048,576 × 16,384` grid, in `SheetRegionIndex`.

**Insertion.** Before `rect_buckets_for_rect`, replace `u32::MAX` ends with the clamp. This still routes through the current `Rect` branch (`region_index.rs:440-447`).

**Query.** Apply the same clamp before `collect_rect_candidates` calls `rect_buckets_for_rect` (`region_index.rs:507-523`).

**Projection.** No structural changes. `query_extents` still sees a finite span (`producer.rs:978-1008`).

**Touch surface.** Small: `region_index.rs` plus tests.

**Correctness.** Correct only inside the chosen coordinate universe. The engine's region coordinates are `u32` (`RegionKey` at `region_index.rs:13-17`), so a hard Excel clamp is a semantic narrowing unless the product commits to Excel bounds.

**Performance.** Excel-sized clamp still creates about 16.8M rect buckets at default sizing, before hash-map/vector overhead. It reduces infinity to "very large," not to safe.

**Sharp edges.** It creates silent false negatives beyond the clamp and large memory spikes inside the clamp.

**Verdict:** REJECTED. Replaces OOM with a memory cliff and changes semantics outside the clamp.

### C. Per-axis projection of unbounded rects onto existing whole-axis variants

**Description.** Represent a tail rectangle by synthesizing existing `WholeRow` or `WholeCol` entries.

**Representation.** No enum change. Add normalization logic: `Rect(all rows, c..MAX cols)` becomes a set of `WholeCol` records, and `Rect(r..MAX rows, all cols)` becomes `WholeRow` records.

**Insertion.** Insert synthesized whole-axis entries into `whole_cols`/`whole_rows`, whose dedicated maps already avoid bucket grids (`region_index.rs:449-457`).

**Query.** Use existing `collect_whole_axis_candidates` (`region_index.rs:529-547`) plus exact filtering.

**Projection.** `query_extents` for synthesized `WholeCol` becomes `(All, Span(col, col))`, and `WholeRow` becomes `(Span(row,row), All)` (`producer.rs:1009-1014`). That is precise per synthesized axis entry but loses the original tail boundary unless every affected row/col is enumerated.

**Touch surface.** Small-to-medium, depending on where synthesis occurs.

**Correctness.** Full precision requires enumerating every affected row/col, which is impossible for `u32::MAX` tails. Any finite synthesis needs a bound and inherits Option B's false-negative risk. Collapsing to `WholeSheet` reproduces the current precision loss.

**Performance.** O(number of synthesized rows/cols). For unbounded tails, this is not bounded.

**Sharp edges.** This abuses point-axis variants for range-axis semantics and embeds policy bounds into indexing.

**Verdict:** REJECTED. Cannot represent unbounded range-axis tails without enumerating every affected row/col.

### D. Lazy bucket enumeration

**Description.** Replace `rect_buckets_for_rect -> Vec<_>` with an iterator.

**Representation.** No semantic type change; only bucket enumeration changes.

**Insertion.** Stream buckets in `index_entry` instead of allocating the full `Vec` (`region_index.rs:440-447`, `region_index.rs:550-562`).

**Query.** Stream lookup buckets in `collect_rect_candidates` (`region_index.rs:507-523`).

**Projection.** No changes.

**Touch surface.** Small.

**Correctness.** Preserves existing semantics.

**Performance.** Removes the immediate `Vec` allocation peak but still performs O(bucket grid) work. For `u32::MAX` tails, insertion/query remain non-terminating in practice.

**Sharp edges.** It hides OOM as CPU runaway. It does not solve the architectural gap.

**Verdict:** REJECTED. Hides OOM as CPU runaway.

### E. Tail-extent first-class type

**Description.** Replace shape-specific region variants with a first-class per-axis range model.

**Representation.** Introduce `AxisRange = Point(u32) | Span(u32,u32) | From(u32) | To(u32) | All`; define a general region as `{ sheet_id, rows: AxisRange, cols: AxisRange }`. Keep current constructors as compatibility sugar for `Point`, `ColInterval`, `RowInterval`, `Rect`, `WholeRow`, `WholeCol`, and `WholeSheet`.

**Insertion.** Dispatch by the pair of axis-range kinds: point map, one-axis interval trees, bounded rect buckets, whole-axis maps, and tail-boundary maps. This consolidates the current duplicated distinction between `Rect` and whole-axis variants (`region_index.rs:74-96`, `region_index.rs:306-314`).

**Query.** Replace the five collector families (`region_index.rs:462-547`) with range-kind dispatch. Tail queries enumerate existing indexed structures by sheet/boundary, not theoretical bucket cells.

**Projection.** Replace `QueryAxisExtent`/`BoundedAxisExtent` split with query-capable `AxisRange` plus a finite-only adapter for result regions. `bounded_extents` remains the gate for affine result regions (`producer.rs:944-976`).

**Touch surface.** Large: region types, indexes, projection helpers, engine conversion points, tests; ~2,300 LOC total per the lift estimate.

**Correctness.** Best long-term correctness. It eliminates sentinel `u32::MAX` as a semantic carrier.

**Performance.** Best long-term performance envelope: O(1) tail insertion, bounded query by indexed-entry population, no bucket explosion.

**Sharp edges.** Migration risk is high because `RegionPattern` is used by producer/result indexes, authority pending changes, engine structural scopes, and tests (`producer.rs:119-153`; `authority.rs:61-73`; `eval.rs:6026-6043`). 211 constructor call sites, 134 + 96 + 32 + 25 + 21 + 13 + 5 = 326 `RegionPattern` references across the codebase.

### F. Separate unbounded-changes log

**Description.** Do not query unbounded rects through `SheetRegionIndex`; store them as pending unbounded changes and process them through a special dirty-closure path.

**Representation.** Add a pending tail-change collection near `pending_changed_regions` (`authority.rs:21-25`). Keep ordinary indexable pending regions unchanged.

**Insertion.** Recording detects unbounded `Rect` and stores it outside the normal pending-region vector, avoiding index/query bucket logic.

**Query.** Add a `FormulaConsumerReadIndex::query_unbounded_changed_region` that scans read entries directly and uses `read_region.intersects(&precise_tail)` before projection. The entries already exist in `FormulaConsumerReadIndex` (`producer.rs:200-218`).

**Projection.** Pass the precise tail to `project_changed_region`; add tail-aware `QueryAxisExtent` to avoid `u32::MAX` offset overflow.

**Touch surface.** Medium: authority, producer dirty closure, engine recording, tests.

**Correctness.** Restores precision for unbounded changed regions without changing indexed read-region representation.

**Performance.** O(read entries on affected sheet) per unbounded structural change. Acceptable when structural edits are rare relative to cell edits.

**Sharp edges.** Creates a parallel dirty path that must remain semantically identical to indexed query.

### G. Span-side filtering/projection with side-band precise structural region

**Description.** Continue indexing/querying `WholeSheet`, but carry the original tail rect as projection metadata.

**Representation.** Replace pending `Vec<RegionPattern>` with a changed-region record: `{ query_region: RegionPattern, projection_region: RegionPattern }`. For structural tails, `query_region = WholeSheet`, `projection_region = precise Rect`.

**Insertion.** No index insertion changes. Recording still uses `WholeSheet`, which is already safe (`eval.rs:3920-3933`; `eval.rs:6026-6043`).

**Query.** `FormulaConsumerReadIndex::query_changed_region` uses `query_region` for `self.index.query`, but calls `project_changed_region(projection_region, read_region, result_region)` instead of using the query region (`producer.rs:233-251`).

**Projection.** Same tail-aware `QueryAxisExtent` requirement as A/F for full precision.

**Touch surface.** Medium-small: pending changed-region type, compute closure signature, read-index query signature, engine recording.

**Correctness.** Restores precision for structural changes while preserving the current OOM workaround.

**Performance.** Query still over-returns every read entry on the sheet because `WholeSheet` is the query key; exact projection then filters. That is better than full recompute and worse than a true tail index.

**Sharp edges.** Metadata plumbing must survive fixed-point closure: dirty result regions emitted by producers are ordinary regions (`producer.rs:86-96`; `producer.rs:842-876`), not structural side-band records.

### H. Wide-rect query fallback

**Description.** Add a cap in `collect_rect_candidates`: when a query rect spans too many buckets or has `u32::MAX`, scan existing rect buckets by sheet instead of enumerating the theoretical grid.

**Representation.** No type change.

**Insertion.** Unchanged; this does not make inserting unbounded `Rect` safe.

**Query.** For wide/tail queries, use the existing "All axis" fallback shape: iterate `self.rect_buckets` for the sheet and exact-filter later (`region_index.rs:521-527`; `region_index.rs:386-399`). Other collectors already avoid bucket grids: points scan keys, interval collectors query interval trees after computing query bounds, whole-axis collectors scan whole maps (`region_index.rs:470-547`).

**Projection.** Needs tail-aware `QueryAxisExtent` for full generality; otherwise it fixes OOM and restores most structural precision but keeps sentinel-edge projection hazards.

**Touch surface.** Small-to-medium.

**Correctness.** Full for the dirty-closure query step; not full for insertion of unbounded indexed entries.

**Performance.** O(existing rect bucket keys on sheet) for wide queries, not O(theoretical tail buckets).

**Sharp edges.** It is a tactical guardrail, not a region model.

## 3. Comparison matrix

| Option | Correctness recoverability | Touch | Risk | Forward fit | v0.6 fit | Headline upside / downside |
|---|---:|---:|---:|---|---|---|
| A. Half-open variants | Yes for structural tails | M | M | Good for `RowsFrom`/`ColsFrom`; variant growth | v0.6.x, not rc1 | First-class enough; starts enum proliferation |
| B. Clamp | No unless product adopts clamp | S | M | Poor | rc1-sized but not recommended | Cheap; large memory and false-negative risk |
| C. Project to whole axes | No for unbounded tails | M | M | Poor | Not recommended | Reuses maps; cannot represent range-axis tails |
| D. Lazy buckets | No practical recovery | S | S | Poor | Not recommended | Reduces allocation peak; preserves runaway work |
| E. AxisRange redesign | Yes | L | L | Excellent | v0.7+ | Cleans model; invasive migration |
| F. Unbounded log | Yes for changed tails | M | M | Medium | v0.6.x | Precise special path; parallel semantics |
| G. Side-band precise region | Yes for structural tails | M/S | M | Medium | v0.6.x if urgent | Keeps workaround; over-queries sheet |
| H. Wide-query fallback | Yes for query-side tail OOM | S/M | S/M | Medium | rc1 candidate if precision is mandatory | Smallest precision patch; not an insertion solution |

## 4. Decision

**Adopted plan: full Option E migration via 6-phase strangler-fig rollout.** See `option-e-execution-plan.md` for the per-phase execution detail.

- **v0.6.0-rc1 (current):** WholeSheet broadening at the recording boundary. Workaround explicit, documented, gated by `structural_change_scope_for_region`. Parity preserved; structural-edit perf wins (~89s → ~30s) preserved at the cost of ~50-200ms recompute per structural cycle.
- **v0.6.x:** Phase 0 = Option A (half-open `RowsFrom`/`ColsFrom` variants). Restores precision for structural tails. ~500 LOC. Validates the AxisRange design space at small scope.
- **v0.7-v0.8:** Phases 1-5 of Option E. Internal `AxisRange` introduction, `SheetRegionIndex` axis-range dispatch, producer / dirty-closure propagation, `RegionPattern` variant collapse, test consolidation.
- **v0.9+:** `RegionPattern` typedef removal. Final cleanup of backward-compat sugar.

## 5. What we pushed back on

- **Pure clamping (B):** replaces an OOM with a memory cliff and changes semantics outside the clamp.
- **Lazy bucket enumeration (D):** preserves runaway work; hides OOM as CPU runaway.
- **Per-axis whole-axis projection (C):** cannot represent unbounded range-axis tails without enumerating every affected row/col.
- **Starting full Option E migration inside rc1:** correct destination, wrong release-gate change. Phased rollout via Option A first.
