# AxisRange Phase 2 dispatch table for `SheetRegionIndex`

## 1. Goal restatement

Phase 2 replaces `SheetRegionIndex` insertion and query dispatch that currently matches the nine `Region` variants with dispatch on the pair `(rows.kind(), cols.kind())`, where each axis has one of five `AxisKind` values: `Point`, `Span`, `From`, `To`, or `All`. The full kind space is 5×5, but Phase 2 implements the nine kind pairs that the current `Region::axis_ranges()` conversion can construct. The remaining sixteen pairs are unreachable until the region representation is collapsed in Phase 4; observing one in Phase 2 is a programmer error and must panic with a message naming the unsupported kind pair.

Insertion chooses exactly one index family from the current `SheetRegionIndex` inventory using the inserted region's `(row_kind, col_kind)`. The Phase 2 insertion rewrite is a routing refactor only: `(Point, Point)` still inserts into `points`, `(Span, Point)` into `col_intervals`, `(Point, Span)` into `row_intervals`, `(Span, Span)` into `rect_buckets`, `(From, All)` into `rows_from`, `(All, From)` into `cols_from`, `(Point, All)` into `whole_rows`, `(All, Point)` into `whole_cols`, and `(All, All)` into `whole_sheets`.

Query chooses which families to walk using the query region's `(row_kind, col_kind)`. For every insert-kind/query-kind pair that Phase 2 can construct, the candidate collector must return a superset of all true intersections. The existing exact-filter step, `entry.region.intersects(&query)`, remains mandatory and removes false positives introduced by coarse buckets, whole-axis scans, and per-sheet scans.

## 2. Index family inventory

Notation: `N_f` is the number of entries in a family, `N_sheet` is the number of indexed entries on the queried sheet, `K` is the number of returned ids before exact filtering, `B_entry` is the number of rect buckets covered by an indexed bounded rect, `B_query` is the number of rect buckets covered by a bounded query, and `B_sheet` is the number of populated rect bucket keys on a sheet.

| Family | Data structure | Insert cost | Point-query cost | Range-query cost |
|---|---|---:|---:|---:|
| `points` | `FxHashMap<RegionKey, Vec<usize>>` | O(1) | O(1+K) | O(N_f) |
| `col_intervals` | `FxHashMap<(SheetId, u32), IntervalTree>` | O(log) | O(tree+K) | O(C_sheet+Σ tree+K) |
| `row_intervals` | `FxHashMap<(SheetId, u32), IntervalTree>` | O(log) | O(tree+K) | O(R_sheet+Σ tree+K) |
| `rect_buckets` | `FxHashMap<(SheetId, u32, u32), Vec<usize>>` | O(B_entry) | O(occupancy+K) | O(B_query+occupancy+K) bounded; O(B_sheet+K) unbounded scan |
| `rows_from` | `FxHashMap<SheetId, BTreeMap<u32, Vec<usize>>>` | O(log) | O(log+K) via `range(..=row)` | O(log+K) |
| `cols_from` | `FxHashMap<SheetId, BTreeMap<u32, Vec<usize>>>` | O(log) | O(log+K) via `range(..=col)` | O(log+K) |
| `whole_rows` | `FxHashMap<(SheetId, u32), Vec<usize>>` | O(1) | O(1+K) | O(N_f) |
| `whole_cols` | `FxHashMap<(SheetId, u32), Vec<usize>>` | O(1) | O(1+K) | O(N_f) |
| `whole_sheets` | `FxHashMap<SheetId, Vec<usize>>` | O(1) | O(1+K) | O(1+K) |

## 3. Recommended NEW index families

**None.** The existing nine families match the nine currently-constructible kind pairs exactly. Phase 0's `rows_from`/`cols_from` covers tail precision without bucket grids.

The Option E memo's broader `tail_extents` family is for kind pairs like `(From, Span)`, `(Span, From)`, `(From, From)`, `(To, All)`, `(All, To)`. None are constructible by current `Region`; deferring to Phase 4.

**Non-negotiable rule:** any kind pair containing `From` or `To` must NEVER route through `rect_buckets_for_rect`. Tail axes turn its returned `Vec<(SheetId, row_bucket, col_bucket)>` into an unbounded grid.

## 4. The 5×5 INSERTION dispatch table

Phase 2 covers the nine cells produced by `Region::axis_ranges()`. Other sixteen cells panic.

| rows \ cols | Point | Span | From | To | All |
|---|---|---|---|---|---|
| **Point** | `points` | `row_intervals` | panic | panic | `whole_rows` |
| **Span** | `col_intervals` | `rect_buckets` | panic | panic | panic |
| **From** | panic | panic | panic | panic | `rows_from` |
| **To** | panic | panic | panic | panic | panic |
| **All** | `whole_cols` | panic | `cols_from` | panic | `whole_sheets` |

## 5. The 5×5 QUERY dispatch table

Each reachable query kind pair walks all 9 families because each family can contain a region that intersects. Dispatch matters because the walk strategy changes by query kind: finite axes use exact lookups or finite bucket enumeration; `From`/`To`/`All` axes scan populated structures by sheet predicate (never enumerate theoretical buckets).

| rows \ cols | Point | Span | From | To | All |
|---|---|---|---|---|---|
| **Point** | walk all 9; rect_buckets one bucket | walk all 9; rect_buckets finite span | panic | panic | walk all 9; rect_buckets scan sheet, matching row bucket |
| **Span** | walk all 9; rect_buckets finite span | walk all 9; rect_buckets bounded grid | panic | panic | panic |
| **From** | panic | panic | panic | panic | walk all 9; rect_buckets scan sheet, `row_bucket >= start_bucket` |
| **To** | panic | panic | panic | panic | panic |
| **All** | walk all 9; rect_buckets scan sheet, matching col bucket | panic | walk all 9; rect_buckets scan sheet, `col_bucket >= start_bucket` | panic | walk all 9; rect_buckets scan all sheet keys |

## 6. Per-family walk strategies

(Per-family algorithm tables omitted from this on-disk document for brevity; the build implementation contains the canonical algorithm. Both the insertion and query dispatch use the kind tag to drive the walk; the exact filter `entry.region.intersects(&query)` is mandatory after candidate collection.)

Key invariants enforced by the walk:

- **`points`**: direct hash lookup for `(Point, Point)`; iterate-and-filter by sheet for unbounded query axes.
- **`col_intervals` / `row_intervals`**: direct hash lookup on the fixed axis; iterate keys with axis-range predicate for the other axis; tree-query the spanning axis with the query bound.
- **`rect_buckets`**: bounded grid enumeration ONLY for `(Span, Span)`-bounded queries; iterate populated keys filtered by sheet+bucket predicate for unbounded queries.
- **`rows_from` / `cols_from`**: `BTreeMap::range(..=high)` walk; intersection holds whenever the indexed `From(N)` boundary is `<=` the query's high bound on the same axis.
- **`whole_rows` / `whole_cols`**: direct lookup on exact axis; iterate keys filtered for unbounded axis.
- **`whole_sheets`**: direct lookup on `sheet_id`.

## 7. Worst-case complexity per query kind pair

| Query kind | Worst-case # candidates | Comments |
|---|---:|---|
| `(Point, Point)` | O(N_sheet) | Common case tight; worst when every family overlaps |
| `(Span, Point)` bounded | O(N_sheet+B_query) | Finite buckets only |
| `(Point, Span)` bounded | O(N_sheet+B_query) | Mirror |
| `(Span, Span)` bounded | O(N_sheet+B_query) | Preserves efficient `rect_buckets_for_rect` path |
| `(From, All)` tail | O(N_sheet+B_sheet) | Scans populated rect_bucket keys; never enumerates theoretical |
| `(All, From)` tail | O(N_sheet+B_sheet) | Mirror |
| `(Point, All)` whole row | O(N_sheet+B_sheet) | Scan populated rect_buckets with matching row bucket |
| `(All, Point)` whole col | O(N_sheet+B_sheet) | Scan populated rect_buckets with matching col bucket |
| `(All, All)` whole sheet | O(N_sheet+B_sheet) | Scans all sheet entries before exact filter |

## 8. Migration plan

### 8a. `index_entry(id, region)`

Replace variant match with axis-kind dispatch. Public `insert(region, value)` signature unchanged. 9 reachable arms route to existing families; default arm panics with `"unsupported SheetRegionIndex insertion kind pair in Phase 2: ({:?}, {:?})"`. Use `let-else` to extract the `AxisRange::Point(_)` / `Span(_,_)` etc. with `unreachable!()` (the kind tag has already proven the variant).

Only the `(Span, Span)` arm calls `rect_buckets_for_rect`. No arm containing `From`/`To`/`All` does.

### 8b. `collect_candidates(query, out)`

Single dispatcher function. Extracts `(rows, cols) = query.axis_ranges()`, matches `(rows.kind(), cols.kind())`, executes per-family walk sequence per Section 6 for each reachable arm. Default arm panics.

### 8c. Helper strategy

Delete six obsolete helpers:
- `collect_point_candidates`
- `collect_col_interval_candidates`
- `collect_row_interval_candidates`
- `collect_rect_candidates`
- `collect_tail_axis_candidates`
- `collect_whole_axis_candidates`

Keep small private utilities (e.g. `extend_ids`, bucket arithmetic) for mechanical reuse only. Do NOT keep a "query every family regardless of query kind" helper — that's the anti-pattern this phase removes.

## 9. Test plan

### 9a. Insertion+query matrix tests in `region_index.rs`

81-case table-driven test (9 insert kinds × 9 query kinds). For each combination: insert one region of insert kind on sheet 1, query with a region of query kind, assert the index returns the entry IFF `Region::intersects` returns true. Cross-sheet query returns nothing.

### 9b. Property tests in `axis_range_proptest.rs`

The hard SUPERSET INVARIANT TEST:

```rust
proptest! {
    #[test]
    fn region_index_query_returns_all_intersecting(
        indexed in vec(any_currently_constructible_region(), 0..50),
        query in any_currently_constructible_region(),
    ) {
        let mut idx = SheetRegionIndex::new();
        let inserted_ids: Vec<usize> = indexed.iter().enumerate().map(|(i, r)| idx.insert(*r, i)).collect();
        let result = idx.query(query);
        let result_ids: HashSet<usize> = result.matches.iter().map(|m| m.value).collect();
        let expected: HashSet<usize> = indexed.iter().enumerate()
            .filter(|(_, r)| r.intersects(&query))
            .map(|(i, _)| inserted_ids[i])
            .collect();
        prop_assert_eq!(expected, result_ids);
    }
}
```

Sheet IDs limited to 1..3, coords 0..20 to encourage same-sheet intersections. ~256 random cases per run cover the 81-pair shape combinations plus boundary edges automatically.

### 9c. Bucket-explosion regression tests

The Phase 0 `rows_from_index_does_not_explode` and `cols_from_index_does_not_explode` tests must continue to pass. Non-negotiable proof that no `From`/`To`/`All` query path enumerates theoretical buckets.

### 9d. Existing suite

All 1671 `formualizer-eval` tests continue to pass.

## 10. Risk register

**Risk: rect_buckets walk regression.** Common bounded `(Span, Span)` query uses `rect_buckets_for_rect` to enumerate finite grid. Mitigation: keep the `(Span, Span)` query arm on the exact efficient path; targeted test for small bounded rect query.

**Risk: over-broad walk for unbounded query.** A `(From, All)` query must include bounded rect entries on the sheet. Mitigation: scan populated `rect_buckets` keys filtered by sheet+predicate; never enumerate theoretical buckets.

**Risk: dispatch table bug producing missed intersections.** Mitigation: 81-case matrix + property test SUPERSET INVARIANT against `Region::intersects` ground truth.

**Risk: NEW index family decision creates rebuild churn.** Mitigation: stick with Phase 0's 9 families; route only the 9 currently-constructible pairs; panic on others until Phase 4 expands the constructible set.

## 11. Build agent dispatch sketch

- Worktree: `.worktrees/formula-plane-region-index-axis-range`.
- Hard scope: touch only `region_index.rs` + test files (no `producer.rs`, `eval.rs`, `authority.rs`, `scheduler.rs`).
- Implement insertion dispatch (Section 4) and query dispatch (Sections 5-6).
- Acceptance gate: region-index unit tests, 81-case matrix, property test, Phase 0 bucket-explosion regressions, all 1671 eval tests, probe-corpus parity numbers, perf vs Phase 1 baseline.
- Forbidden: no `rect_buckets_for_rect` for kind pairs containing `From`/`To`/`All`; no theoretical bucket enumeration; no source changes outside scope; no feature flags; no broad `WholeSheet` substitution; no skipped exact filter.
- Stop conditions: missed-intersection property failure, bucket-explosion regression, compile/test failure outside scope, any need to add a tenth index family in Phase 2, or any required edit to other source files.
