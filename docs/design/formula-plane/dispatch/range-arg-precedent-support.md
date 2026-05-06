# FormulaPlane range-arg precedent support

PM-edited memo combining the read-only investigation with PM decisions. Captures
the gap, the design, and what's deferred.

## 1. Problem statement and scope

FormulaPlane currently promotes cell-only static formulas but rejects every
finite range reference before it can become a span precedent. The smallest
useful change is to represent finite range precedents and translate them into
dirty/read projections.

Relevant code:

- `dependency_summary.rs:51` — `PrecedentPattern` only has `Cell(...)`.
- `dependency_summary.rs:63` — `AffineRectPattern` exists but unused.
- `dependency_summary.rs:733-764` — `analyze_reference` for `CanonicalReference::Range` always rejects.
- `dependency_summary.rs:772-806` — `reject_range` classifies finite ranges as `FiniteRangeUnsupported`.
- `producer.rs:281` — `SpanReadSummary::from_formula_summary` only handles `PrecedentPattern::Cell`.
- `producer.rs:372` — `DirtyProjectionRule` has only `AffineCell` and `WholeResult`.
- `ingest_pipeline.rs:775,789-798` — `compute_read_projections` rejects all `ReferenceType::Range`.

Scope: finite range **arguments** to known-static scalar/reduction functions.
NOT in scope: VLOOKUP/MATCH contracts, INDEX reference-returning behavior,
named ranges, structured references, open/whole-axis ranges, or mixed
growing-window ranges.

## 2. Wins available from range-arg support alone

PM question: "for even just ingest side for VLOOKUP, etc this yields wins?"

Answer: **not for VLOOKUP alone**. Range precedents are necessary for VLOOKUP
later, but not sufficient — VLOOKUP isn't in `is_known_static_function` and
FP8 `compute_read_projections` rejects unknown functions before range
projection helps.

Per-scenario classification:

- **s013 SUMIFS constant criteria**: range support alone unlocks this once the
  `function_arg_context` SUMIFS mapping is corrected. SUMIFS is in the
  allow-list. Sum_range and criteria_ranges are finite absolute cross-sheet
  refs. Criterion is a literal string with no precedent.

- **s014 SUMIFS varying criteria**: range support alone is **NOT** enough.
  - The PM hypothesis "varying criterion canonicalizes identically" was
    wrong: literal values are encoded into the canonical hash
    (`template_canonical.rs:999-1014`, `arena/canonical.rs:79-82`).
  - Rows with `"Type1"`, `"Type2"`, `"Type3"` become separate canonical groups,
    breaking into singleton/non-contiguous components.
  - Unlock requires literal-parameterization OR non-contiguous same-template
    grouping. Separate dispatch.

- **s011 VLOOKUP**: range support alone insufficient. VLOOKUP not in
  allow-list. Needs lookup-function allow-list/contract.

- **s015 INDEX/MATCH**: range support alone insufficient. INDEX is
  reference-returning (canonical reject). MATCH not in allow-list.

- **s029 calc tab**: mixed bag. Sub-expressions like
  `SUMIFS(Data!$B$1:$B$1000, Data!$C$1:$C$1000, "Type0")` are eligible after
  range support, but the surrounding `VLOOKUP` calls and row-varying
  literals (`"row-{r}"`) gate the whole formula.

Win-witness scenario: **s013-sumifs-family-constant-criteria**.

Expected structural win: Medium scale ~10,000 formula vertices represented as
1 span/template. Wall-clock target: ≥1.2x ingest+first-eval improvement.
Recalc speedup will be limited until vectorized SUMIFS span kernels exist
(out of scope).

## 3. Phasing decision

**Phase 1 only.** Do not split.

SUMIFS is the highest-value target. Finite criteria ranges use the same
read-region/dirty-projection machinery as value ranges. Splitting "SUM-only
first then SUMIFS later" would land partial value before SUMIFS is even
testable.

Phase 1 includes:
- range precedent + range projection rule.
- producer/ingest plumbing.
- per-function arg-context cleanup needed for SUMIFS/COUNTIFS to work
  correctly (this is a bug fix, not function-contract integration).
- AVERAGEIF/AVERAGEIFS allow-list addition (one-line oversight).

Phase 1 does NOT include:
- function-contract integration.
- VLOOKUP/HLOOKUP/MATCH/INDEX support.
- Mixed-axis ranges (`$A$1:A{r}` growing window).
- Literal parameterization for s014/s029.
- Vectorized span kernels for SUMIFS.

## 4. Design

### 4.1 New / changed types

Add:

```rust
pub(crate) enum PrecedentPattern {
    Cell(AffineCellPattern),
    Range(AffineRectPattern),
}
```

Add:

```rust
pub(crate) enum DirtyProjectionRule {
    AffineCell { row: AxisProjection, col: AxisProjection },
    AffineRange {
        row_start: AxisProjection, row_end: AxisProjection,
        col_start: AxisProjection, col_end: AxisProjection,
    },
    WholeResult,
}
```

Add reject reason:

```rust
DependencyRejectReason::MixedAxisRangeUnsupported { context: AnalyzerContext }
```

Argument-context cleanup in `dependency_summary.rs`:

The current `AnalyzerContext::CriteriaArg` is bug-overloaded. Fix by either:

**Option A (minimal):** rename `CriteriaArg` → `CriteriaExpressionArg`
(scalar/literal expected). Add `CriteriaRangeArg` (range expected). Update
`function_arg_context` per-function:

```rust
fn function_arg_context(function: &str, arg_index: usize) -> AnalyzerContext {
    match function {
        "LET" | "LAMBDA" => AnalyzerContext::LocalBinding,
        // SUMIFS(sum_range, criteria_range1, criterion1, ...): arg 0 sum, odd >=1 criteria_range, even >=2 criterion
        "SUMIFS" | "AVERAGEIFS" if arg_index == 0 => AnalyzerContext::Value,
        "SUMIFS" | "AVERAGEIFS" if arg_index % 2 == 1 => AnalyzerContext::CriteriaRangeArg,
        "SUMIFS" | "AVERAGEIFS" => AnalyzerContext::CriteriaExpressionArg,
        // COUNTIFS(criteria_range1, criterion1, ...): even criteria_range, odd criterion
        "COUNTIFS" if arg_index % 2 == 0 => AnalyzerContext::CriteriaRangeArg,
        "COUNTIFS" => AnalyzerContext::CriteriaExpressionArg,
        // SUMIF/AVERAGEIF(criteria_range, criterion, [value_range])
        "SUMIF" | "AVERAGEIF" if arg_index == 0 => AnalyzerContext::CriteriaRangeArg,
        "SUMIF" | "AVERAGEIF" if arg_index == 1 => AnalyzerContext::CriteriaExpressionArg,
        "SUMIF" | "AVERAGEIF" => AnalyzerContext::Value,  // optional value_range at index 2
        // COUNTIF(criteria_range, criterion)
        "COUNTIF" if arg_index == 0 => AnalyzerContext::CriteriaRangeArg,
        "COUNTIF" => AnalyzerContext::CriteriaExpressionArg,
        "INDEX" | "OFFSET" => AnalyzerContext::ByRefArg,
        _ => AnalyzerContext::Value,
    }
}
```

Range acceptance: `Value` and `CriteriaRangeArg` accept finite ranges (after
mixed-axis filtering). `CriteriaExpressionArg`, `ByRefArg`,
`ImplicitIntersection`, `LocalBinding`, `Reference` continue to reject.

Allow-list addition in `template_canonical.rs:is_known_static_function`:

```rust
| "AVERAGEIF" | "AVERAGEIFS"
```

(both are missing despite having dependency_contract impls in
`builtins/math/criteria_aggregates.rs`)

`ReadProjection` (already exists from cross-sheet work) still fits — a single
range is on one sheet.

### 4.2 Projection math

Support finite homogeneous endpoint pairs:

**Abs/Abs** (e.g., `=SUM($A$1:$B$10)`):
- `read_region_for_result(sheet_id, result_region)`: returns the absolute
  rectangle, independent of result placement.
  - rows: `BoundedAxisExtent::new(row_start.absolute_index, row_end.absolute_index)`
  - cols: similarly.
- `project_changed_region(changed, read_region, result_region)`:
  - if changed intersects read_region, return
    `Exact(ProducerDirtyDomain::Whole)` — every placement is dirty.
  - else `NoIntersection`.

**Rel/Rel** (e.g., copied `=SUM(A1:A6)` which becomes
`=SUM(A{r}:A{r+5})` after relocation):
- `read_region_for_result`: union of sliding windows over the result region:
  - row source extent: `[result.row_start + row_start.offset, result.row_end + row_end.offset]`
  - col source extent: similarly.
- `project_changed_region`: inverse projection:
  - dirty result row = `[changed.row_start - row_end.offset, changed.row_end - row_start.offset] ∩ result.rows`
  - dirty result col similarly.

**Mixed (Abs/Rel or Rel/Abs)** (e.g., `=SUM($A$1:A{r})` growing window):
- Reject as `MixedAxisRangeUnsupported { context }`.
- Defer.

### 4.3 reject_range / analyze_reference changes

Current range path always rejects (`dependency_summary.rs:753-761`).

New logic in `analyze_reference` for `CanonicalReference::Range`:

1. Classify whole-axis / open / unsupported axes via existing helper. If
   any present, reject with the existing reason variant.
2. Otherwise (all four axes finite):
   - If row_start kind != row_end kind OR col_start kind != col_end kind,
     reject as `MixedAxisRangeUnsupported`.
   - Else if context is in {`Value`, `CriteriaRangeArg`}, push
     `PrecedentPattern::Range(AffineRectPattern { ... })`, return true.
   - Else reject as `FiniteRangeUnsupported` (existing reason).

Critical nuance: do NOT auto-accept top-level `=A1:A10` formulas. Top-level
range references in Value context exist outside function-arg positions;
accepting them would imply array/spill semantics that FormulaPlane doesn't
yet handle. The acceptance gate must be that the CONTEXT is narrowly
"function arg position that consumes ranges" — keep the dispatch through
`function_arg_context` rather than blanket-accepting.

Pragmatic implementation: mark a top-level vs function-arg distinction
on the context, OR check the parent expression kind during `analyze_expr`
recursion. Simplest: add a flag to `analyze_reference` indicating whether
the caller is `analyze_expr` for a function arg or for a top-level value.
The build agent should pick the cleanest implementation.

### 4.4 compute_read_projections (FP8 ingest pipeline)

`compute_read_projections` (`ingest_pipeline.rs:775`) is a structural AST
walk and doesn't go through dependency_summary. It must MIRROR the new
acceptance policy:

- For `ReferenceType::Range`, accept only when in a known range-consuming
  function-arg position.
- Reject `None` endpoints (whole/open).
- Reject mixed endpoint pairs.
- Resolve sheet exactly as cell refs do today.
- Create `ReadProjection { target_sheet_id, rule: DirtyProjectionRule::AffineRange { ... } }`.

Because this is parallel policy to `dependency_summary`, share helper logic
where possible. At minimum, keep tests paired so divergence is caught.

### 4.5 Span eval verification

Span eval already shifts range refs correctly:
- `interpreter.rs:139-148` `effective_reference` relocates refs when delta is non-zero.
- `interpreter.rs:332-345` `evaluate_arena_ast_with_offset` sets the delta.
- `interpreter.rs:1544-1564` `ReferenceType::Range` shifts all four optional axes.

No eval-side change needed for finite ranges.

### 4.6 Span family detection / template interning

No change needed:
- canonical range keys at `template_canonical.rs:1082-1095`.
- finite ranges classified at `template_canonical.rs:575`.
- axis deltas encoded by `axis_pair_from_range` (`template_canonical.rs:730`).

Caveat: literal values are encoded → varying criteria split families. Out of
scope (s014 deferred).

### 4.7 Cross-sheet structural-op demotion

Existing demotion (`engine/eval.rs:3605-3648`) checks
`dependency.read_region.sheet_id() == affected_sheet_id`. Range read regions
have a single sheet_id, so this works without changes. Verify with a
regression test.

## 5. Test strategy

### 5.1 Unit tests in dependency_summary

Acceptance:
- `=SUM(A1:A10)` (Rel/Rel) — within a function arg → `StaticPointwise` with `Range` precedent.
- `=SUM($A$1:$A$10)` (Abs/Abs) — same, with absolute axes.
- `=AVERAGE($A$1:$A$50) * B1` — range + cell precedents.
- `=SUMIFS($B$1:$B$100, $A$1:$A$100, "Type1")` — both ranges + literal criterion.
- `=SUMIFS(Data!$B$1:$B$100, Data!$A$1:$A$100, "Type1")` — cross-sheet ranges.
- `=COUNTIF($A$1:$A$10, "x")` — range + literal.
- `=COUNTIFS($A$1:$A$10, "x", $B$1:$B$10, ">5")` — multiple range/criterion pairs.
- `=SUM(A1:A10) + Data!$X$1` — multiple precedent kinds + cross-sheet cell.

Rejection (must still reject):
- `=A1:A10` — top-level range, not inside a function. Should reject (or stay legacy).
- `=SUM($A$1:$A1)` — mixed-axis range → `MixedAxisRangeUnsupported`.
- `=SUM($A:$A)` — whole-axis → `WholeAxisUnsupported`.
- `=VLOOKUP(A1, $A$1:$B$100, 2, FALSE)` — VLOOKUP not in allow-list → `UnknownFunction`.
- `=INDEX($A$1:$A$10, 1)` — INDEX reference-returning → existing reject.
- `=OFFSET(A1, 1, 1)` — dynamic → existing reject.
- `=MyName` — named range → `NamedRangeUnsupported`.
- `=Table1[Amount]` — structured → `StructuredReferenceUnsupported`.

Updates to existing tests:
- `formula_plane_dependency_summary_rejects_sum_range_not_pointwise_authority`
  — currently asserts `FiniteRangeUnsupported{Value}` for `=SUM(A1:A10)`.
  After fix this should be `StaticPointwise` with a Range precedent. Update or
  rename the test.
- Other range-rejection tests that referenced SUM/COUNT/etc. — re-evaluate.

### 5.2 Projection rule unit tests in producer.rs

For each axis kind combination:
- Abs/Abs:
  - `read_region_for_result` returns the absolute rect, independent of result.
  - `project_changed_region` returns `Exact(Whole)` when changed intersects
    the read region; `NoIntersection` otherwise.
- Rel/Rel:
  - `read_region_for_result` produces the union of sliding windows.
  - `project_changed_region` correctly inverts to dirty result cells.
- Mixed-axis:
  - Construction either rejects at summary-level (preferred) or at projection-level. Pick one and document.

### 5.3 End-to-end engine tests in engine/tests/

`formula_plane_authoritative_sum_static_range_family_promotes`:
- Build A1:A10 with values, B1:B20 with `=A{r} * SUM($A$1:$A$10)`.
- Evaluate. Assert `formula_plane_active_span_count > 0`.
- Verify per-row values.
- Edit A5 = 100. Re-evaluate. Verify all B rows updated.

`formula_plane_authoritative_sumifs_family_promotes`:
- Build Data!A1..A100 with categories ("Type0"/"Type1"/"Type2"), Data!B1..B100 with values.
- Build Sheet1!A1..A50 each with `=SUMIFS(Data!$B$1:$B$100, Data!$A$1:$A$100, "Type1")`.
- Evaluate. Assert `formula_plane_active_span_count > 0` and one template.
- Verify all Sheet1!A{r} == sum-of-Type1-Bs.
- Edit Data!B5 (assume row 5 is Type1). Re-evaluate. Verify all Sheet1 rows updated.

`formula_plane_authoritative_range_precedent_dirty_propagation_through_structural_op`:
- Span on Sheet1 reads `Data!$A$1:$A$100`.
- `insert_rows("Data", ...)`. Verify span demotion (active_span_count → 0).

### 5.4 Corpus updates

- s013: should now promote. Strengthen invariants if needed (existing per-row
  CellEquals should hold). Use as win witness.
- s014: NOT promoted. Document deferral (literal parameterization needed).
  Don't change tags or invariants.
- s029: stays at spans=0. No invariant changes.
- s011/s015: stay at spans=0. No changes.

### 5.5 Performance witness

After build, run probe-corpus on s013 small. Expected output:

```
| s013-sumifs-family-constant-criteria | Off  | Small | ... | spans=0 |
| s013-sumifs-family-constant-criteria | Auth | Small | ... | spans=1 |
```

Auth/Off ratio target: ≤0.85x for first_eval (>=1.18x faster), with
template_count = 1 and spans = 1 instead of N formula vertices.

## 6. Risks

- Mixed endpoint projection math complexity → defer explicitly.
- `function_arg_context` cleanup is required for SUMIFS — without it, range
  acceptance won't reach the SUMIFS args correctly. Hold the line on this
  being a same-dispatch fix, not deferred.
- Large absolute ranges (e.g., `Data!$B$1:$B$10000`) cause whole-span dirty
  fanout on any intersecting edit. Correctness-correct, but recalc
  performance is bounded by per-cell SUMIFS eval cost. Vectorized kernels
  are out of scope.
- Existing tests asserting `FiniteRangeUnsupported` for SUM(A1:A10) need
  updating.
- Parallel policy in `compute_read_projections` and `dependency_summary` must
  match exactly. Add cross-checking tests if helpful.
- Top-level range refs (e.g., `=A1:A10`) must NOT promote — gate on function
  arg context only.
- Reversed ranges (end < start): parser/evaluator normalize; verify
  projection code matches.

## 7. Recommendation

Phase 1 only. Single dispatch.

Deliverables:
- `PrecedentPattern::Range` + `DirtyProjectionRule::AffineRange`.
- `DependencyRejectReason::MixedAxisRangeUnsupported`.
- New `AnalyzerContext` variants (CriteriaRangeArg / rename of CriteriaArg
  to CriteriaExpressionArg, OR a different cleanup that achieves the same).
- Corrected `function_arg_context` for SUMIFS/COUNTIFS/SUMIF/COUNTIF/AVERAGEIF/AVERAGEIFS.
- AVERAGEIF/AVERAGEIFS added to `is_known_static_function`.
- Range acceptance in dependency_summary (gated to function-arg contexts).
- Range projection rule + read_region_for_result + project_changed_region.
- Producer plumbing.
- compute_read_projections range support.
- Tests per §5.
- s013 invariants verified.

LOC estimate: 600-950 (production + tests). High end if AnalyzerContext
introduces multiple new variants and existing tests need broad updates.

Deferred (separate dispatches):
- Function-contract integration.
- VLOOKUP/HLOOKUP/MATCH (allow-list + lookup contracts).
- INDEX reference-returning support.
- Mixed/growing ranges.
- Literal parameterization (unblocks s014/s029).
- Vectorized SUMIFS span kernels.

## 8. Hedge audit

This proposal does NOT:
- add a feature flag.
- wire function contracts.
- add VLOOKUP/HLOOKUP/MATCH support.
- reject SUMIFS for later.
- attempt named ranges, structured refs, 3D/4D refs, dynamic ranges.
- defer the SUMIFS arg-context cleanup (it's bundled because s013 wouldn't
  work without it).
