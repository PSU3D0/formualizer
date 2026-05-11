# Whole-column reference performance investigation

> Authored from a read-only `plan` agent investigation (gpt-5.5), reviewed and
> materialized by PM. Anchors every claim in code with file:line refs.

## 1. Reproduction & timing data

### Scenario

`crates/formualizer-bench-core/src/scenarios/s026_whole_column_refs_in_50k_formulas.rs`.

- Medium scale: 10k rows (`:25-33`).
- Col A values 1..=rows (`:57-59`).
- Col B formulas `=SUM($A:$A) - A{r}` for each row (`:60-64`).
- Edit plan: 5 single-cell value edits in col A (`:81-85`, `:115-122`).

### Timing data (medium 10k)

```
Off:  load 117ms,  first_eval 4583ms,  recalc 4763ms
Auth: load 120ms,  first_eval 4641ms,  recalc 4827ms (spans=0)
```

Derived:
- ~458μs first eval per formula
- ~476μs recalc per formula
- 10k formulas × 10k cells per SUM = 100M lane elements per recalc cycle
- ~48ns per lane element if counted as one SUM input cell

The observed 4.8s is consistent with **repeated Arrow range scans, not parsing/dispatch overhead**.

## 2. Why is it slow in BOTH Off and Auth?

The key fact: `Auth spans=0`. Auth does NOT evaluate this through a FormulaPlane span kernel. Falls back to legacy interpreter/builtin. Two independent reasons.

### 2.1 FormulaPlane currently rejects whole-axis references

- Canonicalization recognizes `(None, None)` axis as `AxisRef::WholeAxis` (`template_canonical.rs:716-745`).
- Dependency summary rejects it with `WholeAxisUnsupported` (`dependency_summary.rs:786-795, 844-864`).
- Scenario tag comment confirms `span_count=0` for s026 (`s026_whole_column_refs_in_50k_formulas.rs:50-53`).

### 2.2 Auth and Off then both execute 10,000 legacy formulas

No expression-result memoization at any layer:
- `evaluate_ast()` directly delegates to `evaluate_ast_uncached()` (`interpreter.rs:301-304`).
- `eval_with_plan()` for binary `-` recurses (`interpreter.rs:704-720, 1047-1062`).
- `eval_function_to_calc()` builds fresh `ArgumentHandle`s and calls `fun.dispatch()` (`interpreter.rs:1175-1189`).
- For `=SUM($A:$A) - A{r}`: each formula recomputes the identical SUM, then subtracts `A{r}`.

### 2.3 Dirty propagation is correct but amplifies

- `Engine::set_cell_value` updates graph + records FormulaPlane changed cell + Arrow overlay + snapshot_id + topology (`eval.rs:5370-5385`).
- `mark_dirty` propagates direct + compressed range dependents (`graph/mod.rs:2071-2134`).
- Whole-column ranges stored as `StripeType::Column` (`graph/range_deps.rs:51-82`).
- Single edit in A:R correctly dirties all 10k B formulas. Each then recomputes the same whole-column aggregate.

**The problem is NOT over-dirtying. It's that each dirty formula recomputes the same whole-column aggregate.**

## 3. What the legacy eval path does for `=SUM($A:$A)` × N

### Binary operator dispatch

`interpreter.rs`:
- `eval_with_plan()` → `eval_binary()` (`:704-720, 1047-1062`).
- For arithmetic: `let l_val = self.evaluate_ast(left)?; let r_val = self.evaluate_ast(right)?` (`:1060-1062`).
- No memo table passed through recursion.

### Function dispatch

For `SUM($A:$A)`:
- `eval_function_to_calc` looks up function, creates `ArgumentHandle`s, calls `fun.dispatch` (`interpreter.rs:1175-1189`).

### ArgumentHandle range resolution

`ArgumentHandle` has only `cached_ast: OnceCell<ASTNode>` and `cached_ref: OnceCell<ReferenceType>` (`traits.rs:224-229`). It does NOT cache evaluated values, aggregate results, or range scans.

### Range resolver for `$A:$A`

`Engine::resolve_range_view` for `ReferenceType::Range` (`eval.rs:9426-9533`). For full-column, both sr/er are None:
- anchors start row at 1
- uses `used_rows_for_columns(sheet_name, scv, ecv)` to find max row (`eval.rs:9322-9343`)
- returns `asheet.range_view(sr0, sc0, er0, ec0)` (`:9519-9532`)

For s026 col A has values 1..10000 → resolves as row 1 through 10000.

### SUM builtin

`SumFn::eval()` in `builtins/math/aggregate.rs`:
- Tries `arg.range_view()` (`:82-83`).
- For range: scans `errors_slices()` first for first error (`:84-99`), then `numbers_slices()` and sums each Arrow Float64 array via `arrow::compute::kernels::aggregate::sum()` (`:101-106`).

**Important**: SUM is already Arrow-vectorized inside each call. The cost is **repeated Arrow lane traversal across 10k calls**, not per-cell allocation.

## 4. Sub-expression CSE survey

### Interpreter: NO expression-result cache

- Method literally named `evaluate_ast_uncached()` (`interpreter.rs:569-681`).
- No `EvalMemo`, no per-recalc result map, no shared range aggregate cache.

### Planner: detects but doesn't use repeated fingerprints

`planner.rs`:
- `NodeHints::repeated_fp_count` (`:34-40`).
- `Planner::fp_cache` for sibling fingerprint counting (`:81-85`).
- `annotate()` computes repeated fingerprints among function args + binary children (`:278-291`).

But:
- `repeated_fp_count` not used in `select()` (`:305-368`).
- `PlanNode` has only `strategy` and `children` (`:50-53`).
- No "compute once and reuse" plan node.
- Detection only applies among siblings inside one AST, not across 10k separate formula vertices.

### ArgumentHandle: caches AST/reference only

Already noted. No `CalcValue`, `RangeView`, SUM, mask, or lookup cache.

### RangeView: caches lane helpers, not range computations

- Cheap view over Arrow.
- `numbers_slices()`, `errors_slices()`, `iter_row_chunks()` (`range_view.rs:477-481, 628-681, 842-891`).
- Does NOT cache aggregate results across calls.

### Arrow store: caches null lanes + lowered text, not aggregates

- Lazy null lanes per chunk (`arrow_store/mod.rs:83-88, 105-139`).
- Lowered text lane per chunk (`:150-152`).
- No column sum, count, error-presence, criteria mask, or lookup index cache.

### SUMIFS "cached mask" is not actually persisted

`EvaluationContext::build_criteria_mask()` default returns `None` (`traits.rs:1304-1314`). `Engine::build_criteria_mask` calls `compute_criteria_mask` directly (`eval.rs:9856-9872, 978-1155`) — no persistent cache lookup. The "cached mask" counters in `criteria_aggregates.rs` count reuse WITHIN a single SUMIFS call, not across formulas.

## 5. Whole-column reference taxonomy

### 5.1 `SUM($A:$A)` — invariant scalar reduction

- Range invariant across formulas.
- Result scalar.
- **Highest ROI first target**: cache `(sheet, range, value_epoch) → aggregate` gives 100× reduction.
- s026 first dirty formula computes once; remaining 9,999 reuse.
- Output work: O(dirty formulas) for cheap subtractions.

### 5.2 `SUM($A:$A) - A{r}` — invariant + per-row local

- SUM invariant; `A{r}` differs per row.
- Decomposes into shared invariant aggregate + row-local relative scalar.
- Legacy CSE handles shared sub-expression. FormulaPlane span kernel can later model shared invariant input + per-row local input.

### 5.3 `SUMIFS($B:$B, $A:$A, "Type1")` — fully invariant

- Sum range, criteria range, criterion all invariant → entire SUMIFS result identical across formulas.
- Optimization: cache complete SUMIFS result by normalized `(sum_range, criteria_range, predicate)` + value_epoch.
- Also cache criteria masks by `(range, col, predicate, value_epoch)` for SUMIFS sharing the same criteria range with different sum ranges.

### 5.4 `SUMIFS($B:$B, $A:$A, A{r})` — relative criterion

- Sum range invariant; criteria range invariant; criterion differs per row.
- Naive `(range, predicate)` cache helps only when criterion values repeat.
- Correct scalable structure: **GroupedSumIndex** keyed by `(criteria_range, sum_range)`:
  - bucketize by criteria values into key→sum maps
  - per-formula: `result_r = grouped_sum_index.lookup(A{r})`
  - cost: O(rows) build once + O(1) per formula
- Incremental updates possible: cell-A change moves B value between buckets; cell-B change adjusts bucket total.

### 5.5 `COUNTIF`/`COUNTIFS` — same as SUMIFS but counts

### 5.6 `VLOOKUP(key, $A:$D, 2, FALSE)`

- `VLookupFn::eval()` (`lookup/core.rs:485-524`).
- Exact match scans typed slices linearly via `find_exact_index_in_view()` (`lookup_utils.rs:334-445`).
- Optimization: build first-column index once per `(table, match_mode, value_epoch)`. Per formula: key eval + hash lookup + return cell read.

### 5.7 Open-ended/used-region growth

Whole-column refs use `used_rows_for_columns()` (`eval.rs:9322-9343`). Caches must key on **resolved used-bounds epoch** and invalidate when bounds grow/shrink. `snapshot_id` increments on external value edits (`eval.rs:5381-5383`); formula overlay writes need a separate value epoch.

## 6. FormulaPlane angle

### 6.1 Currently rejects whole-axis

- canonical: `template_canonical.rs:716-745`.
- dependency summary: `dependency_summary.rs:844-866`.
- s026 has spans=0.

### 6.2 Region vocabulary already supports whole columns

`Region` has `WholeRow`, `WholeCol`, `WholeSheet` (`region_index.rs:70-88`). Constructors at `:132-142`. Intersection semantics correct (`:220-236, 244-257`).

So runtime sidecar has the shape vocabulary. **The blocker is dependency summarization/promotion policy and span evaluation kernel support.**

### 6.3 What FormulaPlane needs for s026

1. Dependency summary support for whole-axis ranges (whole-column produces `Region::WholeCol` summary; used-region growth dirties span when new value appears beyond prior bounds).
2. Span template support for invariant range aggregates (SUM($A:$A) shared invariant; A{r} per-placement).
3. Runtime kernel: compute shared aggregate once + evaluate row-local subtraction + write overlay.
4. Dirty routing: WholeCol(A) intersects edit cell → mark span dirty; whole-span recompute acceptable.

### 6.4 SUMIFS / VLOOKUP

- Literal SUMIFS: constant-result broadcast candidate (s013 pattern + whole-column ranges).
- Relative SUMIFS: needs grouped index + per-placement key lookup (FormulaPlane represents the placement run; needs shared range-derived index input).
- VLOOKUP: function-contract integration — declares table-range as indexable invariant, builds first-column lookup, per-placement key lookup.

## 7. Recommended approach

### Multi-phase plan, build dispatches one phase at a time

**Critical decision: the cache/index service belongs BELOW FormulaPlane so both Off and Auth benefit.** A FormulaPlane-only fix would leave Off slow and not help non-promoted whole-column formulas.

### Architecture: Range Computation Service behind EvaluationContext / Engine

Core types:
```
RangeIdentity { sheet_id, start_row0, end_row0, start_col0, end_col0, resolved_used_bounds_epoch }
ValueEpoch { external_snapshot_id, overlay_write_generation }
RangeAggregateKey { range_identity, value_epoch, visibility_mode, aggregate_kind }
CriteriaMaskKey { range_identity, col_in_view, predicate_fingerprint, value_epoch }
GroupedAggregateKey { criteria_range_identity, sum_range_identity, criteria_semantics, value_epoch }
LookupIndexKey { table_range_identity, match_mode, key_column, value_epoch }
```

Eviction/invalidation:
- Clear all entries at start of each evaluate cycle.
- Keep entries during the cycle so peer formulas share.
- Invalidate intersecting entries on every formula/spill overlay write.
- Clear all on external mutation, structural edit, sheet/name/table edit, row/col insert/delete, used-region epoch change.
- LRU caps:
  - aggregates: 4096 entries or 8MB
  - masks/indexes: 1024 entries or 64MB
  - grouped/lookup indexes: 256 entries or 128MB

### Phase 1: SUM range aggregate cache (highest ROI)

- Whole-column AND finite-range SUM cache (very similar implementation; whole-column is high-value target).
- Key: normalized resolved range + value epoch.
- Cached value: `{ first_error: Option<ExcelErrorKind>, numeric_sum: f64, non_null_numeric_count: usize }`.
- Cache must include first-error semantics because SUM scans errors before numeric sums today (`aggregate.rs:82-107`).
- SUM builtin asks the service before scanning.

**Expected impact on s026 medium**:
- recalc < 100ms (target).
- one scan of $A:$A per cycle + 10k scalar reads/subtractions.

### Phase 2: persistent criteria mask cache

- For SUMIFS/COUNTIFS.
- Replace `Engine::build_criteria_mask` recomputation with lookup/build/insert.
- Reuse existing `compute_criteria_mask` as builder (`eval.rs:978-1155`).
- SUMIFS literal-criterion benefits immediately.

### Phase 3: grouped SUMIFS/COUNTIFS index

- Build one grouped index for `(criteria_range, sum_range, semantics)`.
- Lookup per criterion value.
- Add incremental update support after correctness.

### Phase 4: exact lookup indexes for VLOOKUP/XLOOKUP/MATCH

- Build first-match maps over lookup column.
- Preserve first-match and wildcard semantics.

### FormulaPlane integration (parallel/follow-up to Phase 1)

- FormulaPlane consumes the same Range Computation Service.
- Add whole-axis dependency summary support after legacy cache proves correctness.
- Promote s026-like spans to a kernel with shared invariant + per-row local.

### Rejected alternatives

1. **FormulaPlane-only fix**: rejected. Auth has spans=0; would leave Off slow.
2. **Parser/AST CSE**: rejected. Repeated expression occurs across 10k vertices, not within one AST. Can't change parser arena.
3. **Planner-only CSE**: rejected. Planner produces only execution strategies, not shared-value nodes.
4. **Parallelization**: rejected. Doesn't remove 100M repeated scans.
5. **Precomputing all whole-column sums globally**: rejected. Too broad. Demand-driven build with bounded eviction is safer.

## 8. Test-driven validation strategy

### Phase 1 unit test: SUM aggregate computed once

Create workbook: A1:A10000 = numbers, B1:B10000 = `=SUM($A:$A)-A{r}`. Evaluate all.

Add test-only instrumentation: aggregate cache misses, hits, build rows scanned.

Assertions for first eval:
- exactly one miss for SUM($A:$A)
- approximately 9,999 hits
- rows scanned for SUM aggregate = 10,000 (NOT 100M)
- results match invariants

### Phase 1 dirty propagation test

Same workbook. Edit one A cell. Recalc.

Assertions:
- All B formulas dirtied/in evaluation set.
- Aggregate cache: one miss + 9,999 hits during recalc.
- B sample rows match `new_total - A{r}`.

### Phase 1 perf gate (s026 medium)

Targets:
- recalc < 100ms (currently 4763ms — 47× speedup target)
- first_eval < 150ms (currently 4583ms)
- Auth and Off both improve
- Auth span_count may remain 0 for Phase 1 (proves legacy-path acceleration)

### Phase 1 SUM semantics tests

- Numeric column.
- Empty cells ignored.
- Text in ranges ignored.
- Error in range propagates first error.
- Overlay edits update cached result after invalidation.
- Used-region growth: edit A20000 after prior bound A10000.

### Phase 2 SUMIFS literal whole-column tests

C1:C10000 = `=SUMIFS($B:$B, $A:$A, "Type1")`. Assert mask reuse, edit invalidation, different used-region scenarios.

### Phase 3 SUMIFS relative tests

C{r} = `=SUMIFS($B:$B, $A:$A, A{r})`. Grouped index built once. Edit A row moves bucket. Edit B row changes total.

### Phase 4 VLOOKUP tests

E{r} = `=VLOOKUP(D{r}, $A:$B, 2, FALSE)`. Index built once. Duplicate keys → first match. Numeric/text/boolean/empty per existing semantics.

### FormulaPlane follow-up validation

After whole-axis summary support:
- s026 Auth reports spans > 0.
- Edit in A marks span dirty through `Region::WholeCol`.
- Span eval uses same aggregate cache provider.
- Off and Auth results match exactly.

## 9. Risks and rollback

### Risk 1: Stale cache

- Cached aggregate/index reused after value change.
- Mitigation: cache keys include value_epoch. External edits clear/increment epoch. Formula overlay writes increment overlay generation or invalidate intersecting entries. Structural edits clear all.

### Risk 2: Used-region growth

- $A:$A resolved to 1..10000, A20000 is edited, cache uses old bounds.
- Mitigation: range identity includes resolved used-bounds epoch. Edits affecting bounds clear affected identities. `used_rows_for_columns` remains source of truth.

### Risk 3: Error propagation

- SUM cache stores only numeric sum, misses error-first behavior.
- Mitigation: cache value includes first_error. Builder preserves order: errors scan before numeric scan.

### Risk 4: Memory

- Whole-column masks and lookup indexes can be large.
- Mitigation: LRU caps mandatory. Entries dropped at recalc boundaries. Cache miss falls back to streaming scan.

### Risk 5: SUMIFS criteria semantics

- Excel criteria are subtle: blanks, numeric text, wildcard, case-insensitive text.
- Mitigation: Phase 2 mask cache reuses existing `compute_criteria_mask` builder. Phase 3 grouped indexes added only after tests encode criteria semantics. Unsupported predicates fall back to current path.

### Risk 6: FormulaPlane promotion

- Whole-axis dependencies could under-approximate dirty regions.
- Mitigation: deliver cache in legacy path first. Enable FormulaPlane whole-axis dependency summaries only after `Region::WholeCol/WholeRow` dirty tests pass.

### Rollback

Remove Range Computation Service calls from builtins; existing scan path remains. Tests assert scan fallback still passes.

## 10. Open questions for PM

1. Is the first success criterion s026 medium recalc < 100ms in BOTH modes, even if Auth still has spans=0?
2. Should Phase 1 include finite-range SUM cache too, or only whole-column? (Implementation nearly the same; whole-column is high-value.)
3. For SUMIFS relative-criterion: cache repeated literal criteria first, grouped index for repeated/relative second, incremental updates third — confirm priority?
4. For VLOOKUP: part of this project, or tracked as the lookup-function-contract follow-up dispatch?
5. Memory ceiling for recalc-scoped range-derived indexes on large workbooks: 64MB, 128MB, or workbook-size proportional?
6. Should FormulaPlane whole-axis promotion wait until legacy Range Computation Service is complete, or begin in parallel after Phase 1 SUM cache lands?
7. Used-region semantics: caches use current resolved used bounds, or Excel max-row bounds? (Recommendation: preserve current resolver behavior using used bounds.)

**PM decision pending. The recommendation is Phase 1 SUM aggregate cache as a focused first dispatch.** Phases 2-4 are separate dispatches each with their own design memo refinement. FormulaPlane whole-axis promotion should be a parallel-track design after Phase 1 lands and proves the cache contract.

## Addendum: A/B repro confirms three separable optimization opportunities

The `repro_whole_col_vs_finite` example (`crates/formualizer-bench-core/examples/repro_whole_col_vs_finite.rs`) at 10k rows / 10k formulas surfaces three distinct effects:

```
=SUM($A:$A) - A{r}        Off    recalc 4683ms   Auth recalc 4766ms
=SUM($A$1:$A$10000) - A{r} Off    recalc 2393ms   Auth recalc 1621ms
=SUM($A:$A)               Off    recalc 4850ms   Auth recalc 4773ms
=SUM($A$1:$A$10000)       Off    recalc 2405ms   Auth recalc    0.81ms
```

**Effect 1 — Whole-column legacy tax (~2x in Off)**: `$A:$A` legacy SUM is ~2x slower than equivalent `$A$1:$A$10000` even with identical data extent. Independent of FormulaPlane. Likely culprits: per-call `used_rows_for_columns()` work in `eval.rs:9322-9343` despite the snapshot-keyed `row_bounds_cache`, or differences in how the resolver lands at `range_view`. **Small dedicated investigation.**

**Effect 2 — Constant-result broadcast for finite-range pure SUM (5800x)**: `=SUM($A$1:$A$10000)` Auth = 0.81ms vs Off 2405ms because all-absolute precedent + no relative deps → constant-result family → broadcast eval-once. Already working. **No action needed.**

**Effect 3 — Whole-axis blocks all FormulaPlane benefit**: `=SUM($A:$A)` Auth = 4773ms (same as Off) because `dependency_summary.rs:786-795, 844-864` rejects whole-axis with `WholeAxisUnsupported`. Lifting that rejection alone would let `=SUM($A:$A)` promote as constant-result and broadcast-eval, matching the 0.81ms finite-range pattern. **Medium-effort, high-value: lift the rejection + verify dirty-region projection works for `Region::WholeCol`.**

**Effect 4 — Per-row subtraction defeats broadcast**: `=SUM($A:$A) - A{r}` and `=SUM($A$1:$A$N) - A{r}` both have relative `A{r}` precedents → not constant-result → each placement re-evaluates the full SUM. This is what Phase 1's SUM aggregate cache is for. Even with whole-axis promotion landed, this shape needs cross-formula sub-expression sharing.

### Revised dispatch ordering

1. **Effect 1 fix** (small, independent investigation — separate dispatch): trace per-call `used_rows_for_columns` and AST resolver overhead for whole-column refs. Targets ~2x speedup in BOTH modes.
2. **Effect 3 fix** (FormulaPlane whole-axis dependency support): lift the rejection in `dependency_summary.rs`, add `Region::WholeCol/WholeRow` projection rules, verify dirty propagation. Unlocks broadcast for whole-column constant-result formulas (`=SUM($A:$A)`, `=COUNTA($B:$B)`, `=SUMIFS($B:$B, $A:$A, "literal")`, etc.).
3. **Effect 4 fix** (Phase 1 SUM aggregate cache as in this memo): handles per-row subtraction shapes that defeat broadcast. Big architectural project.

Fix 2 likely has the biggest real-world impact-per-effort ratio: a huge fraction of business workbooks are full of `=SUMIFS($B:$B, $A:$A, "literal")` shapes that would go from 4.8s to ~1ms with this single fix.

