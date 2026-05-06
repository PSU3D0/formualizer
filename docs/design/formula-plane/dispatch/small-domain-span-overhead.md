# Small-domain span overhead investigation (s021/s025 Auth regression)

> Authored from a read-only `plan` agent investigation (gpt-5.5), reviewed and
> materialized by PM. Anchors every claim in code with file:line refs.

## 1. Reproduction & timing data

### s021 scenario shape

Source: `crates/formualizer-bench-core/src/scenarios/s021_volatile_functions_sprinkled.rs`.

- Medium scale is 10,000 rows: lines 32-35.
- Fixture: values in col A, formulas in col B: lines 56-67.
- Formula distribution by `r % 10`: lines 62-66:
  - `r % 10 == 0`: `=A{r}*RAND()` (line 63)
  - `r % 10 == 1`: `=A{r}+TODAY()` (line 64)
  - `r % 10 == 2`: `=A{r}*NOW()` (line 65)
  - otherwise: `=A{r}*2` (line 66)
- Edit plan: 5 single-cell edit cycles, lines 85-88.

Provided medium timing:

- Off mode recalc: **4.27 ms**
- Auth mode recalc: **68.28 ms**
- Auth is **16x slower**
- `plane_span_count = 1000`

### Verified s021 span breakdown

The current code does **not** create single-cell volatile spans. Actual breakdown:

- `=A{r}*2`: **7000 cells** → **1000 row-run spans**, each 7 cells (rows `3..=9`, `13..=19`, ..., `9993..=9999`).
- `=A{r}*RAND()`, `=A{r}+TODAY()`, `=A{r}*NOW()`: **3000 cells**, all legacy.
- Single-cell spans: **0**.

Why:

- Authoritative ingest groups candidates by `(sheet_id, canonical_hash)`: `eval.rs:2749-2755`.
- Each hash group split into adjacency components: `eval.rs:2978-3038`.
- `=A{r}*2` rows share canonical hash, but only rows `3..=9` are adjacent within each ten-row block.
- Volatile functions are rejected before placement:
  - `CandidateAnalysis::from_ingested` rejects nonzero canonical rejects: `placement.rs:163-166`.
  - Arena canonicalization rejects volatile: `engine/arena/canonical.rs:376-379`, names at `420-421`.
  - Template canonicalization rejects parser/function volatility: `template_canonical.rs:348-354`, `673-677`, `812-813`.
- Placement unit test confirms `=RAND()` stays legacy: `placement.rs:873-884`.

So `plane_span_count = 1000` matches the **1000 small non-volatile `=A*2` row-run spans**.

### s025 scenario shape

Source: `crates/formualizer-bench-core/src/scenarios/s025_errors_propagating_through_family.rs`.

- Medium 10k rows: lines 32-35.
- Per-row formula by `r % 100`: lines 66-69:
  - `r % 100 == 0`: `=A{r}/0`
  - otherwise: `=A{r}*2`
- Edit plan: 5 single-cell cycles.

Provided medium timing:

- Off mode recalc: **0.50 ms**
- Auth mode recalc: **1.65 ms**
- Auth is **3.3x slower**
- `plane_span_count = 100`

### Verified s025 span breakdown

- `=A{r}*2`: **9900 cells** → **100 row-run spans**, each 99 cells (rows `1..=99`, `101..=199`, ...).
- `=A{r}/0`: **100 cells**, legacy (singleton components).
- Single-cell error spans: **0**.

Why:

- Error formulas appear at rows `100, 200, ..., 10000` — not adjacent.
- `split_shadow_candidate_components` splits non-adjacent candidates into singleton components: `eval.rs:2978-3038`.
- `detect_domain` rejects singleton components with `SingletonUnique`: `placement.rs:467-472`.
- `Legacy` placements materialized via `ingest_formula_batches`: `eval.rs:3058-3079`.

## 2. Root cause analysis

### PM hypothesis 1 (decline single-cell) is already implemented

Evidence:

- `detect_domain` rejects `analyses.len() < 2`: `placement.rs:467-472`.
- `place_analyzed_family` converts that to legacy: `placement.rs:334-339`.
- Singleton test: `placement.rs:830-844`.

So adding a `domain.size == 1` check would not change s021 or s025.

### Correct framing: small **multi-cell** domains, not single-cell

The actual issue:

- s021: 7-cell domains, 1000 spans.
- s025: 99-cell domains, 100 spans.

The FormulaPlane runtime has a fixed per-span cost. For a 7-cell span, fixed cost dominates. For a 99-cell span, current measured data still shows 3.3x slower than Off.

A promotion policy should require enough cells per span to amortize fixed dispatch cost — unless the span has a special low-cost evaluation path (constant-result broadcast).

### Authoritative evaluation hot path

`evaluate_authoritative_formula_plane_all` at `eval.rs:6760`:

1. **No-active-spans fast path**: delegate to `evaluate_all_legacy_impl` if `active_span_count == 0`: `eval.rs:6765-6767`.
   - **Critical**: if we demote all of s021's 1000 spans, Auth eval goes through the legacy path, removing all per-span overhead.
2. Take pending changed regions: `eval.rs:6780-6784`.
3. Build mixed schedule: `eval.rs:6790-6792`.
4. Iterate schedule layers: `eval.rs:6798-6843`.

Per scheduled span work item:

- Build `SpanEvalTask`: `eval.rs:6807-6812`.
- Lookup span and allocate `current_sheet` String via `to_string()`: `eval.rs:6813-6820`.
- Build fresh `SpanEvaluator`: `eval.rs:6821-6828`.
- Build `SpanComputedWriteSink`: `eval.rs:6829`.
- Call `evaluate_task`: `eval.rs:6830-6836`.

### Mixed schedule build overhead

`build_formula_plane_mixed_schedule` at `eval.rs:6863`:

Per-call allocations:

- `FormulaProducerResultIndex::default()`: line 6869.
- `FormulaConsumerReadIndex::default()`: line 6870.
- `dirty_legacy` hash set from `graph.get_evaluation_vertices()`: lines 6878-6880.
- `span_refs_by_id = BTreeMap`: lines 6883-6887.

Per active span:

- span lookup, result region computation: lines 6888-6892.
- producer result insert: line 6893.
- read summary lookup: lines 6894-6904.
- one consumer read entry per dependency: lines 6907-6912.
- whole-span work pushed in `WholeAll` mode: lines 6915-6921.

Two full-legacy-formula scans per build:

- `eval.rs:6924-6945` and `eval.rs:6947-6985`.
- `graph.formula_vertices()` allocates and sorts: `engine/graph/mod.rs:3138-3141`.

### Span evaluator overhead

`SpanEvaluator::evaluate_task` at `span_eval.rs:99`:

Per task:

- Plane epoch check, span/template lookup: lines 103-116.
- `validate_relocatable_arena_ast` recurse every task: line 118, impl at 248-286.
- `placements_for_dirty` allocates Vec: line 119, impl at 222-240.
- For whole-span dirty: `PlacementDomain::iter` allocates ANOTHER Vec at `runtime.rs:130-167`. The caller then collects again. So whole-span eval does two placement-vector materializations per span.
- Non-constant placement loop: lines 180-210 — per-placement overlay punchout, delta calc, interpreter setup, AST eval, push.

### Why constant-result broadcast does not save s021/s025

- s021's promoted `=A{r}*2` spans have relative `A{r}` reads → not constant-result.
- s025's same shape, same answer.

s021's volatile rows don't reach this path because volatile functions are rejected upstream.

### Volatile semantics note

If volatile formulas without precedent reads (e.g. `=RAND()`) ever became authority-supported, the current "all read projections are absolute" logic would classify them vacuously constant. That would incorrectly broadcast one random value across a span. **This must be guarded** if volatile promotion is ever enabled.

### Dirty / volatile behavior

- `graph.get_evaluation_vertices()` combines dirty + volatile sets: `engine/graph/mod.rs:2142-2143`.
- `redirty_volatiles` re-marks volatile vertices dirty after eval: `engine/graph/mod.rs:2176-2179`.
- Auth eval calls it after mixed eval: `eval.rs:6849-6854`.

`mark_all_active_spans_dirty` is NOT triggered by volatile recalc — only by structural all-sheet / removed-sheet cases: `eval.rs:5488-5497`.

### Is per-span overhead linear or super-linear?

- Index construction is linear in span count + dependencies.
- Work merge / topological scheduling uses BTreeMap → `O(W log W)`.
- Region query cost depends on index shape; not full graph scan per span in s021/s025 shapes.
- No code path has each active span scanning the entire graph.

## 3. Promotion-decision design space

### Preferred policy

Promote a family only if either:

1. `domain.cell_count() >= 100`, or
2. `is_constant_result == true`.

Otherwise → legacy with new fallback reason `SmallDomain`.

Rationale:

- Demotes s021's 7-cell `=A*2` spans → active spans become 0 → Auth fast path → Auth ~= Off.
- Demotes s025's 99-cell spans → same outcome.
- Preserves promotion for 100+ contiguous relative-read families.
- Preserves promotion for small constant-result families (broadcast is real win).
- A future authority-supported 100-cell `=A{r}*RAND()` family would still promote (meets threshold, evaluated per placement, not constant-result).

### Why not singleton-only

Singleton-only is a no-op in current code. Would not fix either provided regression.

### Does legacy fallback complicate batching?

No. Existing authoritative ingest already handles mixed Span/Legacy results:

- `FormulaPlacementResult::{Span, Legacy}`: `placement.rs:62-72`.
- Authoritative ingest consumes Legacy: `eval.rs:2868-2896`.
- Mixed schedule includes both producers: `eval.rs:6882-6921`, `6924-6945`, `6947-6985`.
- Existing tests cover it: `formula_plane_ingest_shadow.rs:394-424, 428-454, 461-487, 495-518`.

## 4. Per-span overhead surface (orthogonal improvements)

These are real but should NOT be the primary regression fix:

1. Avoid `current_sheet.to_string()` per scheduled span: `eval.rs:6813-6820`.
2. Avoid fresh `SpanEvaluator` construction per span: `eval.rs:6821-6828`.
3. Avoid double placement vector materialization: `runtime.rs:130-167` + `span_eval.rs:225-231`.
4. Avoid validating relocatability every task: `span_eval.rs:118` (could cache at template insertion).
5. Reuse retained authority indexes where safe: `authority.rs:99-151` already stores them.

Even good loop hygiene is unlikely to make 7-cell spans beat the legacy graph path. **The promotion decision must stop creating those spans.**

## 5. Recommended fix

### Preferred design

```rust
const MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS: u64 = 100;
```

In `place_analyzed_family`, after `detect_domain(analyses)` succeeds and before template interning:

```rust
if !first.is_constant_result
    && domain.cell_count() < MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS
{
    mark_all_legacy(
        &mut report,
        candidates,
        PlacementFallbackReason::SmallDomain,
    );
    return report;
}
```

Add `PlacementDomain::cell_count()`:

- `RowRun`: `row_end - row_start + 1`.
- `ColRun`: `col_end - col_start + 1`.
- `Rect`: `(row_end - row_start + 1) * (col_end - col_start + 1)`.

Add fallback reason `SmallDomain` to `PlacementFallbackReason`.

### Counters

`mark_all_legacy` already handles all counter accounting correctly: `placement.rs:562-577`.

### Alternatives rejected

1. Only decline `domain.size == 1` — already implemented.
2. Demote all volatile — irrelevant; current code rejects volatile.
3. Demote all error-producing — irrelevant; error rows are already legacy.
4. Per-span loop micro-optimizations only — won't change amortization break-even.
5. Feature flag — rejected by constraint.
6. Native span-eval function trait / `try_eval_span` — rejected by constraint.
7. Speculative cache — rejected. No measured key/hit-rate need.

## 6. Test-driven validation strategy

### Tests in `formula_plane_ingest_shadow.rs` (or placement-specific tests)

#### Test 1: demotes many small non-constant domains (s021 shape)

100 rows, formulas as in s021 (`r%10`).

Expected:
- `shadow_accepted_span_cells == 0`
- `shadow_fallback_cells == 100`
- `formula_plane_active_span_count == 0`
- Eval correctness preserved.

#### Test 2: demotes 99-cell non-constant domains (s025 shape)

200 rows: row 100/200 = `=A{r}/0`, others = `=A{r}*2`.

Expected:
- `shadow_accepted_span_cells == 0` for 99-cell runs.
- `shadow_fallback_cells == 200`.
- `formula_plane_active_span_count == 0`.
- Eval correctness (errors at row 100/200, multiplied values elsewhere).

#### Test 3: preserves 100-cell contiguous non-constant promotion

100 contiguous `=A{r}*2` cells.

Expected:
- `shadow_accepted_span_cells == 100`.
- `formula_plane_active_span_count == 1`.

#### Test 4: preserves small constant-result promotion

Keep existing `formula_plane_authoritative_constant_sumifs_family_promotes_via_broadcast` test at `formula_plane_ingest_shadow.rs:287-352`.

### Corpus assertions

```bash
cargo run -p formualizer-bench-core --features formualizer_runner --release --bin probe-corpus -- \
  --label fp-small-domain-span-overhead \
  --scale medium \
  --modes off,auth \
  --include s021-volatile-functions-sprinkled,s025-errors-propagating-through-family
```

Expected post-fix:
- s021 Auth `plane_span_count == 0`.
- s025 Auth `plane_span_count == 0`.
- s021 Auth recalc < 1.5x Off recalc (currently 16x).
- s025 Auth recalc < 1.5x Off recalc (currently 3.3x).

### Targeted cargo invocations

```bash
cargo test -p formualizer-eval formula_plane_authoritative_demotes_small_non_constant_domains -- --nocapture
cargo test -p formualizer-eval formula_plane_authoritative_demotes_99_cell_non_constant_runs -- --nocapture
cargo test -p formualizer-eval formula_plane_authoritative_promotes_100_cell_non_constant_run -- --nocapture
cargo test -p formualizer-eval formula_plane_authoritative_constant_sumifs_family_promotes_via_broadcast -- --nocapture
cargo test -p formualizer-eval formula_plane -- --nocapture
cargo test -p formualizer-workbook formula_plane -- --nocapture
```

## 7. Risks and rollback

### Risk 1: 100 threshold is heuristic

- 7 cells bad (s021), 99 cells bad (s025) → `>= 100` is the natural conservative cut.
- Constant-result exemption preserves broadcast wins.
- Document the threshold and reason in code.

### Risk 2: existing tests expect small non-constant promotion

Tests currently promoting 2-cell families (e.g. `formula_plane_authoritative_ingest_skips_accepted_span_graph_materialization` at `formula_plane_ingest_shadow.rs:90-136`) will need updating to use 100+ rows.

### Risk 3: volatile future semantics

Out of scope. If volatile authority support is added later, ensure no-read volatile formulas don't get vacuously classified as constant-result.

### Rollback

- Remove gate, remove fallback reason, revert tests.
- No data/config migration involved.

## 8. Open questions for PM

1. Volatile authority support is out of scope. Confirm.
2. Threshold = 100. Confirm acceptable as starting policy. Future tuning by corpus measurement.

PM decision: **Confirm both. 100 is a reasonable starting threshold; we can revisit after the next corpus baseline.**
