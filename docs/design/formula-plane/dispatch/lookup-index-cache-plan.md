# Lookup-Index Cache (Phase 2a + 2b combined)

Author: PM (claude-opus-4-7) with input from plan agent
Branch: formula-plane/fp6-runtime-20260503 @ e69c8e6f
Status: PM-locked, ready for build dispatch

## Goal

Add a per-evaluate-all, snapshot-keyed engine-side cache for VLOOKUP /
HLOOKUP / MATCH / XLOOKUP **exact-match** lookups. Phase 2a establishes
a parity test scaffolding for every known landmine BEFORE implementing
the cache. Phase 2b implements the cache and verifies that all 2a tests
still pass under cache-active Auth mode.

Approximate mode, wildcard mode, and reverse-search mode are explicitly
**out of scope** for this dispatch. They are Phase 2c, dispatched
separately after this lands.

## Why test-first

The cache surface is small in code (~500 LOC) but every landmine in it
produces silent wrong-answer bugs, not crashes. Excel's lookup
equivalence semantics are not Rust-native: case-insensitive text,
loose numeric tolerance (1e-12), empty-as-zero, distinct boolean type,
duplicate-match ordering. A naive `HashMap<LiteralValue, row>` is
silently wrong on every one of these.

The discipline is:
1. Write parity scenarios that capture each landmine.
2. Run them under current Auth (no cache). Establish baseline correctness.
3. Implement cache.
4. Verify all scenarios still pass with cache active.
5. Add perf assertions confirming cache is firing where expected and
   skipped where unsafe.

This pattern is what caught the s054/s055 dirty-prop bug and the
literal-binding bug. We're applying it preemptively here.

## Landmines (12 enumerated, each must have a test)

### L1: Loose equality semantics
- `Int(1)` matches `Number(1.0)` (numeric coercion).
- `Text("ABC")` matches `Text("abc")` (case-insensitive).
- `Number(1.0000000001)` matches `Number(1.0)` within 1e-12 tolerance.
- `Number(0)` matches `Empty` cells (empty-as-zero).
- `Boolean(true)` does NOT match `Number(1)` in exact mode.
- `Text("1")` does NOT match `Number(1)` in exact mode.
- Reference: `crates/formualizer-eval/src/builtins/lookup/lookup_utils.rs:31-58, 360-397`.

### L2: Duplicate-match semantics
- VLOOKUP / HLOOKUP / MATCH / XLOOKUP forward: return FIRST match (lowest index).
- XLOOKUP reverse (search_mode=-1): return LAST match (highest index).
- Cache must store `DuplicateIndices { first, last, all }` per key.

### L3: Empty cells in lookup array
- Sparse columns with gaps; `Number(0)` matches empty.
- Empty needle does NOT match empty cell in exact mode (subtle).
- Reference: `lookup_utils.rs:478-508`.

### L4: Text encoding edge cases
- `to_lowercase()` is locale-INDEPENDENT in Rust stdlib.
- Cache must use SAME normalization as `PreparedLookupMatcher`
  (`lookup_utils.rs:62-104`).
- We do NOT match Excel's locale-specific text behaviors. Pre-existing
  limitation; cache must not make it worse.

### L5: Volatile cells in lookup table
- If table contains `=NOW()`, `=RAND()`, `=OFFSET(...)`, etc., the
  values change across recalcs.
- Cache must NOT be built for views containing volatile precedents.
- Engine knows this from formula-plane's `read_summary` machinery and
  vertex volatility flags.
- Refuse-to-build: skip the cache entirely; per-call eval handles.

### L6: Cross-sheet references
- Cache key MUST include `sheet_id` to distinguish identical ranges on
  different sheets.

### L7: Memory growth
- 1M-row lookup table × 16 bytes/entry = 16 MB per index.
- 100 distinct tables = 1.6 GB.
- Cache must enforce a per-engine memory cap (default 64 MB).
- Refuse-to-build OR LRU-evict when cap exceeded.

### L8: Error cells in lookup column
- `#REF!`, `#DIV/0!` cells in lookup_array.
- Cache must SKIP error cells at index build time (not registered as
  matchable keys).
- If lookup_value itself is an error, propagate before consulting cache.

### L9: Tiny tables
- For very small R and N, hash-build cost exceeds linear-scan savings.
- Refuse-to-build threshold: `R < 64 AND N < 64` skip the cache.
- Even `R = 100` is fine; the bar is pathological tiny cases.

### L9.5: Build-cost threshold
- Cache build is deferred for the first three eligible calls to the same
  `(view, axis, data_snapshot_id)` key.
- Calls below the threshold return `None` and use the existing linear
  lookup path.
- The fourth call builds the index; later calls hit the cached index.
- Per-key call counts are bounded with periodic pruning to avoid stale
  snapshot growth.

### L10: Snapshot invalidation timing
- Cache keyed on `data_snapshot_id` (`traits.rs:1288-1291`).
- Edit to lookup table bumps snapshot; existing cache entry becomes
  stale; next call builds a new entry.
- Cache must be `Sync` (RwLock) for future parallel evaluation
  readiness.

### L11: Out-of-order evaluation
- Theoretical: cache built before lookup table is fully evaluated.
- Scheduler enforces topological order so this shouldn't happen in
  current design.
- Cache must build by reading the live `RangeView` at the moment of
  first request — not pre-compute.
- This means cache builds lazily after the per-key build threshold is
  exceeded by `get_lookup_index` calls from function eval, AFTER the
  table cells have been evaluated.

### L12: Whole-column reference handling
- `Sheet2!$A:$B` returns RangeView with `end_row = used_region_max_row`.
- Cache key uses actual bounds, not declared bounds.
- If used region grows, key changes, new entry built. Correct behavior.

## PM-resolved decisions

### Memory cap
- 64 MB per-engine total, configurable via `EvalConfig::lookup_index_cache_max_bytes`.
- Default 64 MB.
- Refuse-to-build when next entry would exceed cap. NO eviction in this
  phase (LRU eviction adds complexity; deferred to v0.7 if needed).

### Cache lifetime
- Snapshot-keyed via `data_snapshot_id`.
- No explicit eviction on `evaluate_all` boundary. Multiple evaluate_all
  calls share the cache as long as snapshot doesn't bump.
- Topology edits (`mark_topology_edited`) bump snapshot, invalidating
  the entire cache. Data edits (`mark_data_edited`) also bump snapshot.
- This is consistent with `UsedAxisBoundsCache` (`eval.rs:9322-9343`).

### Refuse-to-build conditions
1. Any volatile precedent in the view (per formula-plane read_summary).
   Positive volatile detections are remembered per view identity so
   repeated formulas do not rescan the same volatile table.
2. View contains error cells beyond a threshold (configurable; default:
   any error in the LOOKUP column → skip; errors in non-lookup columns
   are fine because they don't affect index build).
3. Cap exceeded.
4. Tiny table (`R < 64 AND N < 64`).
5. View dimensions degenerate (zero rows or zero cols).

### Test-only counters
- `pub(crate)` counters via `AtomicUsize`. Visible to corpus scenarios
  via a new `pub(crate) fn last_lookup_index_cache_report()` on Engine.
- Counters: `builds`, `hits`, `misses`, `skipped_volatile`,
  `skipped_error`, `skipped_tiny`, `skipped_cap`,
  `skipped_below_threshold`, `bytes_in_cache`.

### Diagnostic API
- `pub(crate) fn last_lookup_index_cache_report() -> Option<&LookupIndexCacheReport>`
  returns the counter snapshot from the most recent evaluate_all.
- Used by both unit tests and probe-corpus JSON output.
- Plain struct, no allocation per call.

## Cache architecture

### Key

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct LookupIndexKey {
    pub(crate) sheet_id: SheetId,
    pub(crate) start_row: u32,
    pub(crate) start_col: u32,
    pub(crate) end_row: u32,
    pub(crate) end_col: u32,
    pub(crate) axis: LookupAxis,
    pub(crate) snapshot_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum LookupAxis {
    /// Index column N within the view, scanning row-major.
    /// VLOOKUP / vertical MATCH / vertical XLOOKUP use this.
    ColumnInView(usize),
    /// Index row N within the view, scanning col-major.
    /// HLOOKUP / horizontal MATCH / horizontal XLOOKUP use this.
    RowInView(usize),
}
```

### Hash key (the normalization)

```rust
#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) enum LookupHashKey {
    /// Numeric values bucket. Stored as bit-pattern of normalized f64
    /// (rounded to nearest representable when within 1e-12 of integer).
    /// Bucket collisions resolved by final-pass cmp_for_lookup.
    Number(u64),
    /// Lowercased text bucket.
    Text(Box<str>),
    /// Boolean kept distinct from Number.
    Boolean(bool),
    /// Empty cell, distinct from Number(0). Empty-vs-Number(0)
    /// equivalence handled at lookup-time, not at index-build time.
    Empty,
}

impl LookupHashKey {
    pub(crate) fn from_literal(v: &LiteralValue) -> Option<Self> {
        // Returns None for Error, Array, etc. — values that should not
        // be indexed. Caller treats None as "skip this cell at build".
        match v {
            LiteralValue::Number(n) => Some(Self::Number(normalize_f64_bits(*n))),
            LiteralValue::Int(i) => Some(Self::Number(normalize_f64_bits(*i as f64))),
            LiteralValue::Text(s) => Some(Self::Text(s.to_lowercase().into_boxed_str())),
            LiteralValue::Boolean(b) => Some(Self::Boolean(*b)),
            LiteralValue::Empty => Some(Self::Empty),
            LiteralValue::Error(_) | LiteralValue::Array(_)
                | LiteralValue::Date(_) | LiteralValue::DateTime(_)
                | LiteralValue::Time(_) | LiteralValue::Duration(_)
                | LiteralValue::Pending => None,
        }
    }
}

fn normalize_f64_bits(n: f64) -> u64 {
    // NaN normalization for hash safety, plus near-integer snap.
    if n.is_nan() { return f64::NAN.to_bits(); }
    let rounded = n.round();
    if (n - rounded).abs() < 1e-12 {
        rounded.to_bits()
    } else {
        n.to_bits()
    }
}
```

The Number bucketing via near-integer snap handles the common
"1 == 1.000000001" tolerance case for typical workbook data. For
bucket collisions where two distinct f64s map to the same bucket (rare
with the snap), the lookup-time verification re-runs `cmp_for_lookup`
to confirm the match.

### Index value

```rust
pub(crate) struct LookupIndex {
    pub(crate) len: usize,
    pub(crate) bytes: usize,
    /// Map from normalized hash key to all matching positions.
    /// HashMap keys are LookupHashKey; the original LiteralValue at
    /// each row is needed for final cmp_for_lookup verification.
    pub(crate) entries: FxHashMap<LookupHashKey, DuplicateIndices>,
    /// Original cell values at each row, for verification.
    pub(crate) cell_values: Box<[LiteralValue]>,
    /// Index of the first empty cell in the lookup axis (for
    /// Number(0)-vs-Empty equivalence at lookup-time).
    pub(crate) first_empty: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DuplicateIndices {
    pub(crate) first: usize,
    pub(crate) last: usize,
    pub(crate) all: smallvec::SmallVec<[usize; 1]>,
}
```

### Lookup operation

```rust
impl LookupIndex {
    /// Find first match for exact mode (VLOOKUP / HLOOKUP / MATCH /
    /// XLOOKUP forward).
    pub(crate) fn find_first_exact(&self, needle: &LiteralValue) -> Option<usize> {
        let hash_key = LookupHashKey::from_literal(needle)?;
        if let Some(dups) = self.entries.get(&hash_key) {
            // Verify against actual values to catch bucket collisions.
            for &idx in &dups.all {
                if cmp_for_lookup(needle, &self.cell_values[idx]) == Some(0) {
                    return Some(idx);
                }
            }
        }
        // Empty-vs-Number(0) equivalence at lookup-time.
        if let LiteralValue::Number(n) | LiteralValue::Int(_) = needle {
            let n = match needle {
                LiteralValue::Number(n) => *n,
                LiteralValue::Int(i) => *i as f64,
                _ => unreachable!(),
            };
            if n.abs() < 1e-12 {
                return self.first_empty;
            }
        }
        None
    }

    /// Find last match (XLOOKUP search_mode=-1). For Phase 2b, only
    /// the first-match path is integrated; this method is exposed for
    /// Phase 2c.
    pub(crate) fn find_last_exact(&self, needle: &LiteralValue) -> Option<usize> {
        // Symmetric to find_first_exact but iterates dups.all in reverse.
        // ...
    }
}
```

### FunctionContext extension

```rust
// crates/formualizer-eval/src/traits.rs
pub trait FunctionContext<'ctx> {
    // ... existing methods ...

    fn get_lookup_index(
        &self,
        view: &RangeView<'_>,
        axis: LookupAxis,
    ) -> Option<Arc<LookupIndex>> {
        None
    }
}

// EvaluationContext gets the corresponding builder method.
pub trait EvaluationContext: ... {
    fn build_lookup_index(
        &self,
        view: &RangeView<'_>,
        axis: LookupAxis,
    ) -> Option<Arc<LookupIndex>> {
        None
    }
}
```

`DefaultFunctionContext::get_lookup_index` delegates to
`EvaluationContext::build_lookup_index`, mirroring the pattern at
`traits.rs:1502-1508` for `get_criteria_mask`.

### Engine implementation

```rust
// crates/formualizer-eval/src/engine/eval.rs
pub struct LookupIndexCache {
    inner: RwLock<FxHashMap<LookupIndexKey, Arc<LookupIndex>>>,
    call_counts: RwLock<FxHashMap<LookupIndexKey, u32>>,
    volatile_keys: RwLock<FxHashMap<LookupIndexKey, ()>>,
    build_threshold: u32,
    bytes_in_use: AtomicUsize,
    max_bytes: usize,
    // counters
    builds: AtomicUsize,
    hits: AtomicUsize,
    misses: AtomicUsize,
    skipped_volatile: AtomicUsize,
    skipped_error: AtomicUsize,
    skipped_tiny: AtomicUsize,
    skipped_cap: AtomicUsize,
    skipped_below_threshold: AtomicUsize,
}

impl Engine<R> {
    fn build_lookup_index_impl(
        &self,
        view: &RangeView<'_>,
        axis: LookupAxis,
    ) -> Option<Arc<LookupIndex>> {
        // 1. Compute key from view + snapshot_id.
        // 2. Check cache; if hit, increment hits, return.
        // 3. Refuse-to-build checks: tiny, volatile, error, cap.
        // 4. Build by iterating view cells along the axis.
        // 5. Insert into cache, increment builds, update bytes.
        // ...
    }
}
```

### Integration into eval paths

VLOOKUP exact path (`core.rs:495-502`) currently calls
`find_exact_index_in_view`. Add cache consultation BEFORE that call:

```rust
// Pseudo
if !approximate {
    if let Some(index) = ctx.get_lookup_index(&rv, LookupAxis::ColumnInView(0)) {
        if let Some(idx) = index.find_first_exact(&lookup_value) {
            // Found via cache; extract target column value at row idx.
            return Ok(...);
        } else {
            // Cache says no match.
            return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)));
        }
    }
    // Fallback: cache unavailable (volatile, tiny, etc.)
    let idx = find_exact_index_in_view(&rv, &lookup_value, /*wildcard=*/false)?;
    // ... existing path ...
}
```

Same pattern for HLOOKUP, MATCH, XLOOKUP exact-mode paths. Approximate
and wildcard paths in those functions remain unchanged in this phase.

## Phase 2a: Test scaffolding (zero production code)

All tests run under current Auth (no cache yet) and MUST pass before
Phase 2b implementation begins. The agent should run them in the
no-cache state first to confirm they're correct, then implement
Phase 2b and re-run.

### Unit tests in `crates/formualizer-eval/src/engine/tests/formula_plane_lookup_semantics.rs`

Each test runs the same workbook under both `FormulaPlaneMode::Off`
and `FormulaPlaneMode::AuthoritativeExperimental` and asserts values
match. This is Off↔Auth parity at the unit-test level.

**Loose equality (L1)**:
1. `vlookup_int_vs_number_match` — Int(5) needle, Number(5.0) in table.
2. `vlookup_text_case_insensitive` — "ABC" needle, mixed case in table.
3. `vlookup_text_with_unicode_special` — German ß, Turkish dotted-i,
   Greek sigma. Document Off behavior; cache must match Off.
4. `vlookup_numeric_tolerance_match` — 1.0 vs 1.0000000000001.
5. `vlookup_numeric_tolerance_no_match` — 1.0 vs 1.0001 (outside).
6. `vlookup_empty_matches_zero` — needle 0, empty cell in table.
7. `vlookup_zero_does_not_match_empty_string` — needle 0, "" in table.
8. `vlookup_boolean_does_not_match_number_in_exact` — TRUE vs 1.
9. `vlookup_text_does_not_match_numeric_in_exact` — "1" vs 1.

**Duplicate match (L2)**:
10. `vlookup_first_match_with_duplicates` — table key "X" at rows 5, 10, 15.
11. `xlookup_forward_first_match` — same shape, search_mode=1 → row 5.
12. `xlookup_reverse_last_match` — same, search_mode=-1 → row 15. (NOTE:
    this exercises the reverse-search path which Phase 2b does NOT
    integrate with the cache. Test verifies legacy fallback is correct.)
13. `match_first_match_with_duplicates` — MATCH returns 1-based pos 5.
14. `hlookup_first_match_horizontal_duplicates`.

**Empty cell semantics (L3)**:
15. `vlookup_in_table_with_gaps` — empty cells interleaved.
16. `match_zero_against_table_with_empty_first_cell` — MATCH(0, ...) → 1.
17. `vlookup_against_used_region_smaller_than_declared` — declared
    $A$1:$B$1000, only A1:B100 has data.

**Volatile / non-cacheable cells (L5)**:
18. `vlookup_against_table_containing_now_function` — table column has
    =NOW(). Verify cache skips (counter `skipped_volatile` increments).
19. `vlookup_against_table_with_index_function_cells` — table column
    has =INDEX(...) (PURE). Cache should engage.

**Cross-sheet (L6)**:
20. `vlookup_cross_sheet_table` — table on different sheet.
21. `vlookup_two_lookups_on_different_sheets_share_no_cache` — verify
    isolation via `entries_count` counter.

**Error propagation (L8)**:
22. `vlookup_with_error_lookup_value` — needle is #DIV/0!.
23. `vlookup_against_table_with_errors_in_lookup_column` — some #REF!
    cells in the lookup column. Verify non-error needles still match.

**Memory bounding (L7)**:
24. `vlookup_against_huge_lookup_table_respects_memory_cap` — synthetic
    table large enough to exceed default 64 MB cap. Verify
    `skipped_cap` counter increments and cache does not engage. Result
    correctness still holds via fallback.

**Workbook shapes (L12)**:
25. `vlookup_lookup_array_is_full_column_reference` — `Sheet2!$A:$B`
    style; verify cache uses used-region bounds.

**Cache invalidation (L10)**:
26. `lookup_cache_invalidates_on_table_edit` — build, eval, edit table,
    re-eval, verify new value reflected.
27. `lookup_cache_invalidates_on_table_extend` — extend table by adding
    row, verify subsequent lookup against new key succeeds.

**Tiny-table refuse-to-build (L9)**:
28. `vlookup_against_tiny_table_skips_cache` — small R AND small N
    skips cache (counter `skipped_tiny` increments). Result still
    correct via fallback.

### Negative tests (locking in non-applicability)

29. `approximate_match_does_not_use_exact_cache` — VLOOKUP range_lookup=TRUE.
    Verify cache counter `hits` does NOT increment for this lookup.
30. `wildcard_match_does_not_use_exact_cache` — XLOOKUP match_mode=2.
    Same verification.
31. `offset_indirect_remain_uncacheable` — formulas using OFFSET
    don't promote, don't go through span eval, never consult cache.

### Corpus scenarios in `crates/formualizer-bench-core/src/scenarios/`

These are perf + correctness at scale. Each registers in
`scenarios/mod.rs`. Tags: `LookupHeavy`, `SingleCellEdit`, plus a
new `ScenarioTag::LookupCacheHeavy` (add to enum).

- **s070-vlookup-cache-K-much-less-than-N**: 10k formulas, 50 distinct
  keys, against 10k-row table.
- **s071-vlookup-cache-K-equals-N**: 10k unique keys, 10k-row table.
  Headline scenario.
- **s072-hlookup-cache-horizontal**: HLOOKUP-equivalent of s070.
- **s073-match-then-index-cache**: classic INDEX/MATCH where MATCH
  benefits from cache.
- **s074-mixed-lookup-and-arithmetic**: VLOOKUP nested inside
  arithmetic.
- **s075-lookup-with-edit-cycles**: edit cycles on (a) lookup_value,
  (b) lookup_array, (c) result column. Verify cache invalidation
  correctness AND perf at scale.
- **s076-lookup-against-volatile-table**: lookup table contains NOW().
  Cache must not engage; perf matches per-call baseline.
- **s077-lookup-with-sparse-empty-cells**: realistic empty-cell
  pattern in the lookup column.
- **s078-multiple-tables-cache-isolation**: two distinct lookup tables.

Each scenario:
- 5 edit cycles.
- NoErrorCells invariant.
- Per-row CellEquals for at least 3 sample rows.

## Phase 2b: Cache implementation

After all 31 unit tests + 9 corpus scenarios pass under no-cache Auth,
implement the cache.

### Files to add/modify

NEW:
- `crates/formualizer-eval/src/engine/lookup_index_cache.rs` —
  `LookupIndex`, `LookupHashKey`, `LookupAxis`, `LookupIndexKey`,
  `LookupIndexCache`, `LookupIndexCacheReport`. Module declaration in
  `crates/formualizer-eval/src/engine/mod.rs`.

MODIFY:
- `crates/formualizer-eval/src/traits.rs` — add `get_lookup_index` to
  `FunctionContext`, `build_lookup_index` to `EvaluationContext`,
  delegate impl in `DefaultFunctionContext`.
- `crates/formualizer-eval/src/engine/eval.rs` — own the
  `LookupIndexCache`, implement `build_lookup_index`. Add
  `last_lookup_index_cache_report()` accessor (`pub(crate)`).
  Snapshot invalidation hooks: cache uses `data_snapshot_id()` as key,
  no explicit invalidation needed.
- `crates/formualizer-eval/src/engine/eval_config.rs` (or wherever
  `EvalConfig` lives) — add `lookup_index_cache_max_bytes: usize` with
  default 64 * 1024 * 1024.
- `crates/formualizer-eval/src/builtins/lookup/core.rs` — VLOOKUP /
  HLOOKUP / MATCH eval paths consult cache for exact mode.
- `crates/formualizer-eval/src/builtins/lookup/dynamic.rs` — XLOOKUP
  exact forward path consults cache.

### Integration constraints

1. **Cache lookup happens ONLY in exact mode**:
   - VLOOKUP: range_lookup is FALSE.
   - HLOOKUP: range_lookup is FALSE.
   - MATCH: match_type is 0.
   - XLOOKUP: match_mode is 0 AND search_mode is 1.
   
   All other modes go through the existing per-call path. Approximate,
   wildcard, and reverse-search are Phase 2c.

2. **Cache lookup is opt-in via FunctionContext**: function calls
   `ctx.get_lookup_index(view, axis)` and gets `Option<Arc<LookupIndex>>`.
   None means fallback to per-call.

3. **Cache must be transparent**: result MUST exactly match the
   per-call path. Any divergence is a correctness bug.

4. **Memory cap is per-Engine**: `LookupIndexCache` lives on Engine,
   `bytes_in_use: AtomicUsize` tracks total. New entries that would
   exceed cap are skipped (`skipped_cap` counter).

5. **Tiny-table threshold**: `R < 64 AND N < 64` skip the cache (PM
   note: N here is the count of unique placement count, not table
   rows; the function call already has access to its placement context
   via the interpreter's current_cell so it knows the family size...
   actually we don't have N at call time in a clean way. Simpler: use
   `R < 64` as the threshold. If R is tiny, hash building isn't worth
   it. Single-call lookups against a small table also skip — that's
   fine).

6. **Volatile precedent detection**: Engine has access to vertex
   volatility flags via DependencyGraph. The view's covered cells can
   be checked for volatile precedents. If ANY covered cell is a
   volatile-formula vertex, refuse to build.

7. **Error-cell handling at build time**: skip cells with
   `LiteralValue::Error` from being indexed. Track count; if all cells
   are errors, the cache is empty but built (lookups return None).

8. **Concurrency**: `LookupIndexCache.inner` uses `RwLock` for
   future-readiness. Single-threaded access today; multi-threaded
   safety guaranteed.

### Integration test counters

After Phase 2b is implemented, the unit tests in
`formula_plane_lookup_semantics.rs` are EXTENDED to assert specific
counter values. Examples:

```rust
fn vlookup_cache_engages_for_repeated_keys() {
    let mut wb = build_workbook_with_repeated_keys();
    wb.evaluate_all().unwrap();
    let report = wb.engine().last_lookup_index_cache_report().unwrap();
    assert_eq!(report.builds, 1);
    assert!(report.hits > 0);
    assert_eq!(report.skipped_volatile, 0);
}
```

The same pattern proves that volatile / tiny / capped cases skip
correctly:

```rust
fn vlookup_against_volatile_table_skips_cache() {
    // ... build with =NOW() in lookup table ...
    wb.evaluate_all().unwrap();
    let report = wb.engine().last_lookup_index_cache_report().unwrap();
    assert_eq!(report.builds, 0);
    assert!(report.skipped_volatile > 0);
}
```

## Validation gates

### Phase 2a only (test scaffolding)

```bash
cargo fmt --all -- --check
cargo clippy -p formualizer-eval --all-targets -- -D warnings
cargo clippy -p formualizer-bench-core --all-targets -- -D warnings
cargo clippy -p formualizer-bench-core --features formualizer_runner --all-targets -- -D warnings
cargo test -p formualizer-eval --quiet
cargo test --workspace --quiet
cargo build -p formualizer-bench-core --features formualizer_runner --release --bin probe-corpus --bin probe-corpus-parity

./target/release/probe-corpus-parity --scale small --include 's070-*,s071-*,s072-*,s073-*,s074-*,s075-*,s076-*,s077-*,s078-*' --label cache-2a-baseline
```

All 31 unit tests pass under no-cache Auth. All 9 new corpus scenarios
pass parity.

Counters that exist (test-only) report all zeros for build/hits/etc.
because the cache machinery doesn't exist yet — but the
`last_lookup_index_cache_report()` Engine method exists and returns a
zeroed report. This API surface is added in Phase 2b; for Phase 2a
the tests that check counter values are written but `#[ignore]` is
**FORBIDDEN**. Instead, the counter-checking tests are added in Phase
2b only, not in Phase 2a.

### Phase 2b (cache implementation)

Same validation gate plus:

```bash
./target/release/probe-corpus-parity --scale small --label cache-2b-full-parity
./target/release/probe-corpus --label cache-2b-perf-small --scale small --modes off,auth --include 's070-*,s071-*,s072-*,s073-*,s074-*,s075-*,s076-*,s077-*,s078-*'
./target/release/probe-corpus --label cache-2b-perf-medium --scale medium --modes off,auth --include 's070-*,s071-*,s072-*,s073-*,s074-*,s075-*,s076-*,s077-*,s078-*'
```

Expected:
- All 31 + 9 tests still pass.
- s071 medium recalc: <50 ms (was several seconds without cache).
- s076 (volatile table) recalc: matches Off baseline (cache skipped).
- All previously-passing scenarios unchanged.
- `skipped_cap` counter is 0 for normal-sized scenarios.

### Perf assertions

Add explicit perf assertions to the corpus scenarios via probe-corpus
notes. Example: s071's first-eval Auth recalc must be <100x of the
linear-scan baseline. Compute the threshold from the no-cache 2a run
and assert in 2b that it's met.

This is the hard gate — if cache is implemented but no perf win
materializes, that's a real bug too.

## Hard scope (FORBIDDEN PATTERNS)

- **Do NOT touch Phase 2c modes** (approximate, wildcard, reverse).
  Cache is exact-only in this dispatch.
- **Do NOT use `LiteralValue` directly as HashMap key**. Must use
  `LookupHashKey` newtype with normalization.
- **Do NOT skip duplicate-match handling**. `DuplicateIndices` with
  first/last/all is required even though Phase 2b only consumes
  `first` (Phase 2c will use `last`).
- **Do NOT add `#[ignore]` or `expected_to_fail`** on any test.
- **Do NOT add hedge phrases** in code, comments, or commit messages.
- **Do NOT add a config flag to disable the cache**. Always-on with
  refuse-to-build conditions. PM may add a kill-switch later if needed.
- **Do NOT integrate cache into reverse-search XLOOKUP path** in
  Phase 2b. The cache supports `find_last_exact` but it's only
  consumed in Phase 2c.
- **Do NOT optimize hash construction** beyond the spec (e.g., don't
  parallelize, don't skip the cmp_for_lookup verification).
- **Do NOT make cache eviction LRU**. Refuse-to-build only when cap
  exceeded. LRU is v0.7.
- **Do NOT add CHOOSE / OFFSET / INDIRECT support**. They're rejected
  upstream; cache never sees them.
- **Don't push, don't commit. Stage with `git add -A`. PM commits.**

## Stop conditions

Stop and ask if:
- Any Phase 2a test fails under current Auth (would indicate a
  pre-existing correctness bug we haven't found yet).
- After Phase 2b implementation, any Phase 2a test fails (cache
  correctness bug; STOP and escalate).
- Performance scenarios show NO speedup despite cache being engaged
  (would indicate cache integration is wrong).
- Memory cap is exceeded by a normal-sized scenario (would indicate
  cap calculation is wrong).
- Volatile-table scenario unexpectedly engages the cache (would
  indicate volatile detection is wrong; correctness bug).

## Process

1. Implement Phase 2a (all 31 unit tests + 9 corpus scenarios). All
   tests must pass under no-cache Auth before proceeding.
2. Implement Phase 2b (cache + integration + counter assertions).
3. Re-run all Phase 2a tests; all must pass.
4. Run perf gates; all assertions must hold.
5. Stage with `git add -A`. Stop and report.

## Out of scope (Phase 2c, future)

- VLOOKUP / HLOOKUP / MATCH approximate (range_lookup=TRUE / match_type=±1).
- XLOOKUP wildcard mode (match_mode=2).
- XLOOKUP reverse search (search_mode=-1).
- Approximate XLOOKUP modes (match_mode=±1).
- LRU eviction.
- Per-pattern wildcard memo.
- Sorted-vec representation for binary-search approximate.
- Cache statistics in workbook public API.
