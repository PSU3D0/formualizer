# Off-mode whole-column legacy tax investigation (Effect 1)

> Authored from a read-only `plan` agent investigation (gpt-5.5), reviewed and
> materialized by PM. Anchors every claim in code with file:line refs.

## 1. Reproduction & timing data

Benchmark source: `crates/formualizer-bench-core/examples/repro_whole_col_vs_finite.rs`. 10k inputs in `Sheet1!A1:A10000`, 10k formulas in `Sheet1!B1:B10000`. Recalc edits one input value and calls `evaluate_all()` again (`:47-51`).

After commit `0d287ce9` (whole-column FormulaPlane promotion), Off-mode timings:

```
=SUM($A:$A) - A{r}           Off  recalc 4725ms
=SUM($A$1:$A$10000)-A{r}     Off  recalc 2482ms

=SUM($A:$A)                  Off  recalc 4882ms
=SUM($A$1:$A$10000)          Off  recalc 2448ms
```

Pure SUM differential: 488μs/formula vs 245μs/formula. **Gap: ~243μs/formula × 10k formulas = ~2.4s of extra work per recalc.**

Both formulas resolve to the same `A1:A10000` extent and produce identical answers. The extra work must be in the per-formula bound-resolution path that whole-column triggers but finite-range skips.

## 2. Hypothesis space

- **A — whole-column bound resolution**: strongest hypothesis. Whole-col has missing row bounds; resolver discovers used row extent.
- **B — Arrow used-row cache miss/rebuild**: rejected after code reading. Arrow cache should hit after first call for non-empty col A.
- **C — formula row-bounds scan**: best code-supported explanation. `used_rows_for_columns` calls `formula_row_bounds_for_columns` on every whole-col call; that scans all vertices in the queried column.
- **D — RangeView shape/iteration**: rejected. Same view bounds after resolution.
- **E — SUM builtin**: rejected. SUM doesn't branch on whole-col identity.
- **F — Parser/AST identity**: real but is the trigger (drives the expensive branch), not the cost itself.
- **G — Per-call sheet-id lookup**: secondary overhead, too small alone.
- **H — RwLock contention**: secondary, single-threaded.
- **I — Planner/strategy selection**: rejected. Recalc uses arena evaluation, not planner.

## 3. Code-path tracing

### 3.1 Legacy Off recalc enters arena evaluation per formula vertex

- `evaluate_all_legacy_impl` at `eval.rs:7133`. Loops `get_evaluation_vertices()` at `:7147`.
- `legacy_pass_run_layers` at `:7114-7126` → `evaluate_layer_sequential` at `:7121-7123`.
- `evaluate_layer_sequential_effects` at `:10600` iterates `for &vertex_id in &layer.vertices` calling `evaluate_vertex_immutable` at `:10607`.
- For 10k formula vertices, the formula body is evaluated 10k times. **No higher-level cache of `SUM($A:$A)` results in legacy Off mode.**

### 3.2 Arena function call reaches SUM

- `Interpreter::evaluate_arena_ast` `Function` branch at `interpreter.rs:528`. Builds `ArgumentHandle::new_arena` at `:535-540`. Dispatches builtin at `:548`.

### 3.3 SUM builtin consumes RangeView identically for both formulas

- `SumFn::eval` at `aggregate.rs:64+`. Calls `arg.range_view()` at `:81`. Checks errors at `:83-96`, sums numerics at `:100-105`.
- **SUM does NOT inspect whole-column identity.** Only relevant input is the resolved `RangeView`.

### 3.4 ArgumentHandle::range_view reconstructs reference per formula

- `ArgumentHandle::range_view` at `traits.rs:494`. Arena branch resolves reference at `:540-548`, calls `context.resolve_range_view` at `:548-551`.
- Arena reference reconstruction preserves whole-column identity: `CompactRefType::Range` stores missing bounds as sentinels `0`/`u32::MAX` at `data_store.rs:487-496`; reconstruction converts back to `None` at `:704-721`.

### 3.5 Parser/arena AST shape

- Parser stores `start_row = None, end_row = None` for `$A:$A`, all-Some for `$A$1:$A$10000` (`parser.rs:858-865, 247-257`).
- Not a parser bug. Necessary representation for open-ended refs.
- The issue is that **legacy evaluation pays the bound-resolution tax every formula.**

### 3.6 `Engine::resolve_range_view`: whole-column branch vs finite fast path

- `Engine::resolve_range_view` at `eval.rs:9392`. `ReferenceType::Range` arm at `:9429`.
- `bounded_range` populated only if all four bounds present at `:9443-9451`.
- **Critical whole-column branch** at `:9466-9475`:
  ```rust
  if sr.is_none() && er.is_none() {
      // anchor start row to 1
      // call self.used_rows_for_columns(sheet_name, scv, ecv)
      // set er from result
  }
  ```
- Finite range skips this branch entirely.
- After normalization, both end up calling identical `asheet.range_view(0, 0, 9999, 0)` at `:9558-9562`.

**The differential is BEFORE `range_view(...)`.**

### 3.7 `used_rows_for_columns` always computes formula bounds on every whole-column call

```rust
// eval.rs:9322-9343
fn used_rows_for_columns(&self, sheet, start_col, end_col) -> Option<(u32, u32)> {
    let sheet_id = self.graph.sheet_id(sheet)?;          // O(1)
    let arrow_bounds = self.arrow_used_row_bounds(...);  // cached
    let formula_bounds = self.formula_row_bounds_for_columns(...);  // NOT cached, expensive
    if let Some(bounds) = Self::union_used_bounds(arrow_bounds, formula_bounds) {
        return Some(bounds);
    }
    // graph fallback ...
}
```

**Even when Arrow bounds are cached and immediately available, `formula_row_bounds_for_columns` is still called.** No final used-bounds cache at the wrapper level. No short-circuit when Arrow bounds already cover the column.

### 3.8 `arrow_used_row_bounds` cache behavior

- Has snapshot-keyed `row_bounds_cache: RwLock<Option<RowBoundsCache>>` at `eval.rs:349, 8799-8830`.
- For non-empty column A: first whole-col formula misses, computes & stores; later formulas hit.
- Per-call cost on hit: atomic snapshot load, RwLock reads, FxHashMap lookups, sheet-id lookups. Real but secondary.
- **Caveat**: `(None, None)` cached results are NOT treated as hit (`:3878` requires `Some(min)`). Empty columns rescan. Not the benchmark case but worth fixing in the same patch.

### 3.9 `formula_row_bounds_for_columns` is the hot per-formula scan

- Defined at `eval.rs:4143-4189`.
- Calls `index.vertices_in_col_range(sc0, ec0)` at `:4155-4156`.
- **Filters every returned vertex by kind**: `get_vertex_kind(vid)` at `:4157-4161`. Continues unless `FormulaScalar | FormulaArray`.
- `SheetIndex::vertices_in_col_range` is `O(log n + k)` where `k` is vertices in range (`sheet_index.rs:173-176`).

**Benchmark implication:**
- 10,000 input value vertices in column A.
- Whole-col query is for column A.
- `vertices_in_col_range(0, 0)` returns those 10k column-A vertices.
- Function performs 10k `get_vertex_kind` checks, skips them all (they're not formulas).
- Returns `None` (formulas live in column B).
- This scan occurs **once per whole-column SUM formula**.
- **10,000 formulas × 10,000 vertices scanned = 100,000,000 vertex-kind checks.**
- Observed gap ~2.4s ÷ 100M = ~24ns per check. Plausible for indexed iteration + graph kind lookups.

This **directly explains** the ~243μs/formula extra cost.

### 3.10 Other paths cleared

- `graph.used_row_bounds_for_columns` fallback at `:9339-9342` — not active here (Arrow bounds exist).
- `RangeView` construction/iteration identical post-resolution.
- Sheet-name/sheet-id lookups: real but small overhead.
- Planner not on this path.

## 4. Verified vs. suspected causes

### Verified

1. Whole-column takes branch at `eval.rs:9466-9475`; finite skips.
2. `used_rows_for_columns` called once per SUM argument per formula evaluation.
3. `used_rows_for_columns` not cached at wrapper level.
4. Arrow column bounds ARE snapshot-cached and hit for non-empty col A.
5. Arrow cache hit still pays atomic load, RwLock reads, HashMap lookups, sheet-id resolutions.
6. `formula_row_bounds_for_columns` is NOT cached; called every time.
7. `formula_row_bounds_for_columns` scans ALL indexed vertices in queried column range.
8. Benchmark queries column A which has 10k input value vertices → 10k vertex-kind checks per call.
9. Finite-range path skips the whole `used_rows_for_columns` call.
10. SUM builtin doesn't branch on whole-col vs finite.
11. RangeView iteration characteristics same once bounds resolved.
12. Cache write path NOT hit on every formula (snapshot stable during recalc).

### Suspected (not required to explain the gap)

- Exact split of 243μs/formula between `formula_row_bounds_for_columns` (dominant), RwLock reads, sheet-id lookups, RangeView creation, SUM setup. Code structure makes the dominant term clear.
- Empty-column inefficiency (`(None, None)` not cached as hit) — real, separate but small.
- Duplicated packed-load scan in `graph/mod.rs:912-920` — observed, not active for this benchmark.

## 5. Recommended fix

### Preferred: cache final used-axis bounds at `used_rows_for_columns` / `used_cols_for_rows` wrapper level

Add a final used-bounds cache, NOT inside SUM and NOT inside FormulaPlane.

Cache shape:

```rust
struct UsedAxisBoundsCache {
    snapshot: u64,
    row_bounds_by_col_span: FxHashMap<(SheetId, u32, u32), Option<(u32, u32)>>,
    col_bounds_by_row_span: FxHashMap<(SheetId, u32, u32), Option<(u32, u32)>>,
}
```

- Wrap in `RwLock<Option<UsedAxisBoundsCache>>` following existing `RowBoundsCache` pattern at `eval.rs:8799-8830`.
- Key by `snapshot_id` (data + topology edits both increment, see `eval.rs:2403-2413`).
- Cache `None` results too (closes the empty-column rescan hole).

Updated flow for `used_rows_for_columns`:
1. Resolve `sheet_id` once at `eval.rs:9328`.
2. Load `snapshot_id` once.
3. Check final cache for `(sheet_id, start_col, end_col)`.
4. On hit: return cached `Option<(u32,u32)>` immediately.
5. On miss: run current logic (Arrow bounds, formula bounds, union, graph fallback).
6. Store final result.
7. Return.

Apply the symmetric pattern to `used_cols_for_rows` (`eval.rs:9345-9365`).

**Why this directly removes the measured tax:**
- First `SUM($A:$A)` formula computes bounds.
- Remaining 9,999 formulas hit final cache.
- Hot `formula_row_bounds_for_columns` scan drops from 10,000 executions to 1 per `(Sheet1, A:A)` span per recalc.
- Arrow `RwLock`/HashMap hit cost also avoided.
- Finite path unchanged.

**Expected impact:**
- Whole-column Off recalc within ~5-10% of finite-range Off recalc for identical extents.

**Implementation notes:**
- Don't hold cache lock during expensive computation.
- Read-lock check, compute outside lock, write-lock insert.
- Duplicate computation under parallel evaluation acceptable.
- Use `sheet_id` (not `&str`) in keys to avoid string key allocation.

### Rejected alternatives

A. SUM aggregate cache / CSE — separate dispatch (`whole-column-references.md`). Different layer.
B. Rewrite SUM — SUM treats both forms identically after `arg.range_view()`.
C. Rewrite RangeView — Both formulas produce same view post-resolution.
D. Ignore formula bounds when Arrow bounds exist — would lose union correctness if formulas exist below Arrow data extent.
E. Formula-only sheet index — broader surface than necessary; the wrapper cache eliminates repeated scans for identical spans with much smaller diff.
F. Per-call profiling gate — code reading already identifies the loop.
G. Feature flag — pure correctness-preserving internal cache.
H. Parser/arena AST mutation — parser must remain independent of workbook data extent.
I. FormulaPlane changes — Off-mode regression independent of FormulaPlane.

## 6. Test-driven validation strategy

### Unit tests for cache correctness

Location: `crates/formualizer-eval/src/engine/tests/used_bounds_cache.rs`.

1. Existing test: `used_row_bounds_cache_parity_and_edit_invalidation` (`:8`). Verifies repeated calls return same result and edit invalidates.

2. New test: final cache reuse.
   - 10k values in col A. 10k formulas in col B.
   - Call `used_rows_for_columns("Sheet1", 1, 1)` twice.
   - Assert same result.
   - Assert internal `#[cfg(test)]` cache statistic: 1 miss, 1 hit, no second formula-bounds scan.

3. New test: caches `None`.
   - Empty column C.
   - Call twice. Both return `None`.
   - Second call hits final cache.

4. New test: invalidation.
   - Data through row 5. Call returns `(1, 5)` or similar.
   - Edit row 8. Snapshot increments.
   - Call again. Returns updated. Cache miss, then hit on third call.

5. New test: formula-coordinate union preserved.
   - Data A1:A5. Formula in A10.
   - Call returns max row 10. Protects union semantics.

6. Symmetric tests for `used_cols_for_rows`. Cache hit, invalidation, formula-column union.

### Integration tests

7. Engine evaluation test: 10k values + 10k `=SUM($A:$A)` formulas. Representative B values correct.
8. Parity with `=SUM($A$1:$A$10000)`. Same results.
9. Edit invalidation: edit `A5000`, recalc, whole-column SUM updates.
10. Formula-in-referenced-column: `A12000` formula + base data A1:A10000. `=SUM($A:$A)` includes row 12000.

### Performance validation

Re-run `repro_whole_col_vs_finite` (10k/10k, FormulaPlane Off):
- Pure SUM whole-col recalc: ~4882ms → ~2500ms (within 5-10% of finite).
- `=SUM($A:$A)-A{r}` whole-col recalc: ~4725ms → ~2500ms.

Keep as benchmark/example, not timing-sensitive unit test. Unit tests use internal counters, not wall-clock.

Confirm no FormulaPlane path changes: Auth-mode results unchanged.

## 7. Risks and rollback

### Risks

1. Stale bounds after data edit — mitigated by snapshot_id key + `mark_data_edited` (`eval.rs:2403-2406`).
2. Stale bounds after topology edit — same (`mark_topology_edited` at `:2410-2413`).
3. Stale bounds within recalc — current formula-bounds logic includes formulas by coordinate; snapshot-keyed cache preserves union semantics.
4. Caching `None` hiding writes — normal writes increment snapshot. Existing `RowBoundsCache` makes same assumption.
5. Parallel duplicate misses — acceptable; correctness preserved; avoid holding write lock during expensive scans.
6. Memory growth — bounded by distinct queried open-ended spans per snapshot. Tiny for benchmark.
7. Full-row references — apply same pattern to `used_cols_for_rows`.

### Rollback

- One new internal cache struct. One Engine field. Modifications to two wrappers. Tests in `used_bounds_cache.rs`.
- Revert: remove cache field, restore wrapper bodies. Existing `row_bounds_cache` unchanged.
- No persisted format / parser / FormulaPlane / public config changes.

## 8. Open questions for PM

1. Acceptance threshold: target whole-column Off recalc within 5%, 10%, or other % of finite-range Off recalc?
2. Symmetric scope: confirm both `used_rows_for_columns` and `used_cols_for_rows` in same patch?
3. Test-only counters acceptable for non-flaky regression test? (Avoids wall-clock assertions in CI.)

PM decisions: **Accept 10% threshold (within margin of measurement noise). Yes, symmetric scope. Yes, internal `#[cfg(test)]` counters.**
