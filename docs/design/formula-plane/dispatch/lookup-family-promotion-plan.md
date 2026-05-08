# Lookup Family Promotion Plan (V/H/X-LOOKUP + MATCH)

Author: plan agent (gpt-5.5)
Reviewed: PM (claude-opus-4-7)
Branch: formula-plane/fp6-runtime-20260503 @ b4e003d3

## 1. Executive Summary

Promote `VLOOKUP`, `HLOOKUP`, `MATCH`, and scalar-returning `XLOOKUP` by adding the four function names to the formula-plane static-function allowlists in both canonicalization paths. No per-argument overrides are required: every argument in all four functions belongs in `AnalyzerContext::Value` and `SlotContext::Value`, so the current fall-through behavior is correct and must remain consistent across `function_arg_context` and `function_arg_slot_context` (`dependency_summary.rs:956-973`, `template_canonical.rs:1054-1072`). This promotion is materially simpler than INDEX because none of these four functions is currently in the reference-returning rejection list (`template_canonical.rs:918-920`, `canonical.rs:426-428`).

Do not add lookup-index cache hooks in this dispatch. Phase 1b should stay limited to allowlisting, tests, and corpus coverage. Phase 2 should add the `FunctionContext` lookup-index API and the engine-side cache together, with correctness tests for exact, approximate, wildcard, first/last duplicate semantics, and XLOOKUP search modes.

## 2. Per-arg Classification

Default fall-through (`AnalyzerContext::Value` + `SlotContext::Value`) is correct for every argument of every function. No explicit overrides needed.

**VLOOKUP(lookup_value, table_array, col_index_num, [range_lookup])** — All args `Value`. Runtime reads values via `args[k].value()` and `resolve_range_view`/`find_exact_index_in_view` (`core.rs:458-524`). No reference-identity dependency.

**HLOOKUP(lookup_value, table_array, row_index_num, [range_lookup])** — All args `Value`. Symmetric to VLOOKUP, horizontal axis (`core.rs:715-781`).

**MATCH(lookup_value, lookup_array, [match_type])** — All args `Value`. Returns 1-based numeric position from values; no reference-identity dependency (`core.rs:178-286`).

**XLOOKUP(lookup_value, lookup_array, return_array, [if_not_found], [match_mode], [search_mode])** — All args `Value`. Schema-level `by_ref=true` on lookup/return arrays (`dynamic.rs:100-101`) is orthogonal to formula-plane classification; it concerns runtime range-handle dispatch, not dependency tracking. Result depends on cell contents, not addresses.

**Implementation:** Do not add explicit arms to `function_arg_context` (`dependency_summary.rs:956-973`) or `function_arg_slot_context` (`template_canonical.rs:1054-1072`). Only add the four names to `is_known_static_function` in both canonicalization sites:
- `crates/formualizer-eval/src/formula_plane/template_canonical.rs:929-1013`
- `crates/formualizer-eval/src/engine/arena/canonical.rs:437-521`

## 3. Common Utilities Audit

Existing shared utilities in `lookup_utils.rs` already cover the important common pieces:
- `value_to_f64_lenient`, `cmp_for_lookup` — loose comparison.
- `PreparedLookupMatcher`, `CompiledWildcardPattern` — wildcard matching.
- `find_exact_index`, `find_exact_index_in_view` — exact scans (slice + Arrow-aware).
- `is_sorted_ascending`/`_descending`, `approximate_select_ascending` — approximate-mode helpers.

Per-function audit shows remaining duplication is either minor (wildcard-mode detection at `core.rs:213,497,762`) or tied to function-specific semantics (VLOOKUP/HLOOKUP target row-or-col extraction; XLOOKUP search modes and `if_not_found`).

**Recommendation:** Add no new shared utilities in this dispatch. Phase 2's lookup-index API will resolve the approximate-mode duplication, not a pre-cache refactor.

## 4. FunctionContext Cache Design (DEFERRED to Phase 2)

**Recommendation:** Do not add `FunctionContext::get_lookup_index` in this dispatch.

A default-`None` hook in Phase 1b gives no performance win, adds API surface without engine behavior, and touches every lookup eval path without validating the cache. Phase 2 must add API, engine cache, and eval call sites together because cache key, duplicate-match semantics, wildcard behavior, approximate-mode ordering, and XLOOKUP search modes all affect correctness and need integrated testing.

### Phase 2 cache key (design only)

```rust
struct LookupIndexKey {
    view_identity: ViewIdentity,            // (sheet_id, start_row, start_col, end_row, end_col)
    axis: LookupAxis,                       // ColumnInView(usize) | RowInView(usize)
    mode: LookupIndexMode,                  // Exact | Wildcard | ApproxAscLargestLeq | ApproxAscSmallestGeq | ApproxDescSmallestGeq | ExactLast | WildcardLast
    snapshot_id: u64,                       // EvaluationContext::data_snapshot_id
}
```

Axis-aware key handles VLOOKUP/MATCH (column) and HLOOKUP (row). `ExactLast`/`WildcardLast` handle XLOOKUP reverse search (`dynamic.rs:265-307`).

### Phase 2 cache value (design only)

`LookupIndex` with three optional internal structures:
- Exact: `HashMap<LookupKey, DuplicateIndices>` with `first/last/all` indices.
- Approximate: sorted `Vec<(ComparableLookupKey, usize)>` for binary search.
- Wildcard: folded text + per-pattern memo.

Excel loose-equality semantics (case-insensitive text, numeric tolerance, empty-as-zero) require validation against `cmp_for_lookup`/`PreparedLookupMatcher` after hash bucket lookup.

### Phase 2 lifetime

Engine-owned, per-evaluate-all, snapshot-keyed. Mirrors the row-visibility cache shape (`eval.rs:362-364, 1275, 3379-3382, 3624-3658`) and the criteria-mask delegation pattern (`traits.rs:1304-1312, 1400-1410, 1502-1508`).

## 5. Corpus Scenarios (Phase 1b)

Six new scenarios after s063. Register in `crates/formualizer-bench-core/src/scenarios/mod.rs`.

### Critical

**s064-hlookup-family-horizontal-table** — `=HLOOKUP(A{r}, Lookup!$A$1:$ALL$2, 2, FALSE)` × N. Validates horizontal range argument value classification.

**s065-xlookup-exact-with-if-not-found-ref** — `=XLOOKUP(A{r}, Lookup!$A$1:$A$1000, Lookup!$B$1:$B$1000, C{r}, 0, 1)` × N. Validates XLOOKUP `if_not_found` ref becomes value-ref slot.

**s066-xlookup-search-mode-2-exact** — `=XLOOKUP(A{r}, Lookup!$A$1:$A$1000, Lookup!$B$1:$B$1000, "NF", 0, 2)` × N. Validates `search_mode` parameterization.

**s067-index-match-approximate-chain** — `=INDEX($D$1:$D$1000, MATCH(A{r}, $E$1:$E$1000, 1))` × N. Validates MATCH approximate mode + INDEX/MATCH chain promotion.

**s068-vlookup-approximate-sorted-table** — `=VLOOKUP(A{r}, Lookup!$A$1:$B$1000, 2, TRUE)` × N. Validates VLOOKUP approximate TRUE.

### Nice-to-have

**s069-xlookup-wildcard-deeply-nested-if** — XLOOKUP wildcard mode at depth 4 inside IF chain. Validates deep composition + wildcard slot identity.

## 6. Risks and Rejection-List Interactions

### Rejection-list audit (verified clean)

None of `VLOOKUP/HLOOKUP/MATCH/XLOOKUP` appear in:
- Local environment (`template_canonical.rs:910-912`, `canonical.rs:418-420`).
- Volatile (`template_canonical.rs:914-916`, `canonical.rs:422-424`).
- Reference-returning (`template_canonical.rs:918-920`, `canonical.rs:426-428`).
- Array/spill (`template_canonical.rs:922-926`, `canonical.rs:430-434`, `dependency_summary.rs:1017-1022`).

### Function-specific risks

**XLOOKUP multi-cell-return correctness gap.** XLOOKUP eval can return `CalcValue::Range` when `return_array` has multiple columns or rows (`dynamic.rs:379-397`). Formula-plane span eval converts arrays via `into_literal()` and writes only the top-left value to overlay (`traits.rs:117-132`, `span_eval.rs:783-787`). This may produce Off↔Auth divergence for multi-cell XLOOKUP. **Mitigation:**
1. Add a parity-guard test (`xlookup_multi_cell_return_parity_guard`) that compares Off↔Auth for `=XLOOKUP(2, $A$1:$A$2, $B$1:$C$2)`.
2. New corpus scenarios use scalar-return shapes only.
3. If parity test exposes divergence, the build agent must STOP and escalate to PM. Do not paper over.

**No current-cell side channels** in any of the four functions. All use `ctx.current_sheet()` and `ctx.resolve_range_view()` only for range resolution; lookup behavior depends purely on cell values.

**No reference-identity dependencies.** All four return values, not references. None inspect argument addresses.

### CHOOSE remains out of scope

`CHOOSE` is reference-returning by rejection-list design (`template_canonical.rs:918-920`, `canonical.rs:426-428`). Defer.

## 7. Validation Strategy

### Unit/integration tests (new file `formula_plane_lookup_family_promotion.rs`)

1. `vlookup_exact_relative_key_promotes` — span=1, correct values.
2. `vlookup_constant_key_broadcasts` — span=1, `transient_ast_relocation_count == 1`.
3. `hlookup_exact_promotes` — span=1, horizontal axis.
4. `match_exact_promotes` — span=1, 1-based positions.
5. `xlookup_exact_scalar_promotes` — span=1, correct scalar values.
6. `xlookup_if_not_found_ref_is_value_slot` — fallback ref creates per-placement value-ref slot.
7. `lookup_table_edit_marks_dirty` — table edit propagates to dependent placements.
8. `xlookup_multi_cell_return_parity_guard` — Off↔Auth parity for multi-cell return; STOP if divergent.
9. `mixed_lookup_aggregate_logical_promotes` — formula like `=VLOOKUP(...)+IFERROR(VLOOKUP(...),0)+SUMIFS(...)+IF(...)+LEN(...)` (mirrors s029) promotes as one span.

### Existing test update

`formula_plane_index_promotion.rs:157-173` — `index_match_classic_pattern_promotes` currently asserts spans=0. After this dispatch, assertion must flip to spans=1.

### Corpus + parity gates

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --quiet
cargo test fp8_ingest_pipeline_parity --quiet
cargo build -p formualizer-bench-core --features formualizer_runner --release --bin probe-corpus --bin probe-corpus-parity

./target/release/probe-corpus-parity --scale small --include 's011-*,s012-*,s015-*,s029-*,s049-*,s050-*,s064-*,s065-*,s066-*,s067-*,s068-*,s069-*' --label lookup-promotion-parity
./target/release/probe-corpus-parity --scale small --label lookup-promotion-full-parity
./target/release/probe-corpus --label lookup-promotion-perf-medium --scale medium --modes off,auth --include 's011-*,s012-*,s015-*,s029-*,s049-*,s050-*,s064-*,s065-*,s066-*,s067-*,s068-*,s069-*'
```

### Expected promotion + perf

| Scenario | Promotion | Phase 1b perf expectation |
|---|---|---|
| s011 VLOOKUP 1k | yes | K≈N small scale: minor recalc improvement; medium/large with key repetition: memoization helps |
| s012 VLOOKUP 10k | yes | K≈N small: minor; large K=10k N=50k: memoization helps |
| s015 INDEX/MATCH | yes | Promotion only; perf gain depends on duplicate keys |
| s029 mixed nested | yes | Whole family stays in span; perf amortized |
| s049 row-relative VLOOKUP | yes | K=50 N=1000 → memoization win |
| s050 constant-key VLOOKUP | yes | Broadcast: ~1.5ms → near-zero |
| s064 HLOOKUP | yes | Like VLOOKUP exact |
| s065 XLOOKUP fallback ref | yes | Correctness + memoization identity |
| s066 XLOOKUP search_mode=2 | yes | Promotion + parity; no binary-search win until Phase 2 |
| s067 INDEX/MATCH approx | yes | Promotion + parity |
| s068 VLOOKUP approx | yes | Promotion + parity |
| s069 wildcard nested | yes | Promotion at depth + wildcard slot identity |

K=N scenarios won't show major speedup until Phase 2 lookup-index cache. Constant-key broadcast (s050) is the clear measurable win in Phase 1b.

## 8. Open Questions (PM-resolved)

1. **Phase 1b excludes lookup cache hooks** — confirmed. Phase 2 adds API + engine impl together.
2. **Phase 2 cache lifetime** — engine-owned, per-evaluate-all, snapshot-keyed. Decided.
3. **XLOOKUP multi-cell-return scope** — scalar-return only in new corpus; parity guard test added; if divergent, STOP and escalate.
4. **CHOOSE** — out of scope. Defer.

## Appendix: Key Code Locations

### Allowlists to update
- `crates/formualizer-eval/src/formula_plane/template_canonical.rs:929-1013` — `is_known_static_function`
- `crates/formualizer-eval/src/engine/arena/canonical.rs:437-521` — arena `is_known_static_function`

### No-change classification (verified, DO NOT touch)
- `crates/formualizer-eval/src/formula_plane/dependency_summary.rs:956-973` — `function_arg_context`
- `crates/formualizer-eval/src/formula_plane/template_canonical.rs:1054-1072` — `function_arg_slot_context`

### Function eval paths (read for verification, no changes needed)
- VLOOKUP: `crates/formualizer-eval/src/builtins/lookup/core.rs:458-577`
- HLOOKUP: `crates/formualizer-eval/src/builtins/lookup/core.rs:715-826`
- MATCH: `crates/formualizer-eval/src/builtins/lookup/core.rs:178-324`
- XLOOKUP: `crates/formualizer-eval/src/builtins/lookup/dynamic.rs:199-402`

### Cache precedent (Phase 2 reference)
- Criteria mask consumer: `crates/formualizer-eval/src/builtins/math/criteria_aggregates.rs:264-270`
- Criteria mask trait: `crates/formualizer-eval/src/traits.rs:1304-1312, 1400-1410, 1502-1508`
- Row-visibility cache: `crates/formualizer-eval/src/engine/eval.rs:362-364, 1275, 3379-3382, 3624-3658`

### Existing test to update
- `crates/formualizer-eval/src/engine/tests/formula_plane_index_promotion.rs:157-173` — flip assertion from spans=0 to spans=1
