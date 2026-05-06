# FormulaPlane function promotion gap

Investigation memo. Written before the fix dispatch. Captures the asymmetry
between canonical and summary layers and the scope of impact across the
scenario corpus.

## 1. Problem statement

FormulaPlane has a cross-layer asymmetry for function-bearing formulas.

The canonical layer (`crates/formualizer-eval/src/formula_plane/template_canonical.rs`)
maintains an explicit allow-list of "known static" functions
(`is_known_static_function`, ~line 827). It rejects only:
- Dynamic-reference functions (OFFSET, INDIRECT)
- Volatile functions (RAND, NOW, TODAY, ...)
- Reference-returning functions (INDEX, CHOOSE, ...)
- Array/spill functions (FILTER, SORT, UNIQUE, ...)
- Unknown/custom functions (anything not in the allow-list)

But the dependency summarizer
(`crates/formualizer-eval/src/formula_plane/dependency_summary.rs`)
unconditionally rejects every function expression. In `SummaryAnalyzer::analyze_expr`
for `CanonicalExpr::Function` (~line 626), it always calls `reject_function`
and returns `false`, regardless of whether the function is in the canonical
allow-list.

The asymmetry was deliberate at FP4.A.1 ("Until FP4.B wires in function
contracts...") but FP4.B never landed, leaving the dependency summary half-
configured.

Probe results confirming the gap:

```text
=ISNUMBER(A{r})              spans=0  fallback=UnsupportedDependencySummary
=A{r}*2                      spans=1  (promotes — pure binary op)
=IF(ISNUMBER(A{r}), A{r}*2, 0)  spans=0  fallback=UnsupportedDependencySummary
=ABS(A{r})                   spans=0  fallback=UnsupportedDependencySummary
=SUM(A1:A{r})                spans=0  (also has varying range; expected reject)
```

## 2. Function caps / contracts: what's already there

Surprising finding: the engine already has a two-layer function metadata
system that's MORE advanced than the FormulaPlane consumes today.

`crates/formualizer-eval/src/function.rs` defines `FnCaps`:
- `PURE`, `VOLATILE`, `REDUCTION`, `ELEMENTWISE`, `LOOKUP`,
  `RETURNS_REFERENCE`, `SHORT_CIRCUIT`, `PARALLEL_ARGS`,
  `PARALLEL_CHUNKS`, `DYNAMIC_DEPENDENCY`.
- Default `caps()` returns `FnCaps::PURE`.

`crates/formualizer-eval/src/function_contract.rs` defines an opt-in
machine-readable dependency contract:
- `FunctionDependencyClass::{StaticScalarAllArgs, StaticReduction, CriteriaAggregation}`
- `FunctionArgumentDependencyRole::{ScalarValue, FiniteRangeValue, ReductionValue, CriteriaRange, CriteriaExpression, ValueRange, LazyBranch, LookupKey, LookupTable, LookupResultSelector, ByReference, ...}`
- `FunctionArityRule::{Exactly, AtLeast, OneOf, EvenAtLeast, OddAtLeast}`
- Constructors: `static_scalar_all_args(arity)`, `static_reduction(arity, min)`, `criteria_aggregation(...)`.
- `Function::dependency_contract(arity) -> Option<FunctionDependencyContract>` defaults to `None`.

Several builtins already opt in:
- ISNUMBER, ABS: `static_scalar_all_args`
- SUM, COUNT, AVERAGE: `static_reduction`
- SUMIF, SUMIFS, COUNTIF, COUNTIFS, AVERAGEIFS: `criteria_aggregation`

But IF, AND, OR, and several other "obviously pure" functions in the
canonical allow-list don't have contracts yet.

The contract system is opt-in and tested
(`function_contract.rs` `#[cfg(test)] mod tests`), but **FormulaPlane does
not consult it today**. The canonicalization layer has no function
provider plumbing (the TODO at template_canonical.rs:694-696 explicitly
notes this). Dependency summary only does name-based dispatch via
`function_arg_context`.

## 3. Scope of impact

Classification of corpus scenarios s011-s030 with `spans=0` under Auth:

| Scenario | Formula shape | Class | Pure-scalar fix promotes? |
|---|---|:---:|---|
| s011 | =VLOOKUP(A{r}, ...) | b/d | No — needs lookup contracts + cross-sheet |
| s012 | =VLOOKUP(A{r}, ...) | b/d | No — same as s011 |
| s013 | =SUMIFS(...) constant criteria | b/d | No — needs range + criteria + cross-sheet |
| s014 | =SUMIFS(...) varying criteria | b/d | No — same as s013 |
| s015 | =INDEX(MATCH(A{r}, ...)) | b/d | No — lookup + reference + cross-sheet |
| s016 | =Data1!A{r}*2, =LEN(Data2!A{r}) | d | No — cross-sheet refs blocked |
| s017 | =Data!A{r} * 2 | d | No — cross-sheet refs blocked |
| s018 | =SUM(NamedRange) | d | No — named ranges unsupported |
| s019 | =SUM(Table[Col]) | d | No — structured refs unsupported |
| s020 | =SUMIFS(SalesTable[...]) | d | No — structured refs unsupported |
| s021 | =A{r}*RAND() intermixed | c/d | No — volatile rejection (intentional) |
| s022 | =OFFSET(...), =INDIRECT(...) | c | No — dynamic deps (intentional) |
| s023 | =A{r}*2 with gaps | other | No — already supported; not function-gap |
| **s024** | **=ISNUMBER(A{r}), =IF(ISNUMBER(A{r}), A{r}*2, 0)** | **a** | **YES — clean Option A win** |
| s025 | =A{r}/0, =A{r}*2 | other | No — error-template; not function-gap |
| s026 | =SUM($A:$A) - A{r} | c/d | No — whole-axis unsupported |
| s027 | =SUMPRODUCT({1,...}, ...) | c/d | No — array literals unsupported |
| s028 | =LET(x, ...) | c | No — local env (intentional) |
| s029 | VLOOKUP+IFERROR+SUMIFS+IF+LEN | b/d | No — multi-blocker |
| s030 | cross-sheet family + calc cells | b/d | No — cross-sheet + multi-blocker |

Classes:
- (a) Pure-scalar function fix alone unlocks promotion.
- (b) Needs range-arg + function-contract support.
- (c) Intentionally unsupported (volatile, dynamic, spill, local env).
- (d) Other layered blocker (cross-sheet, named ranges, structured refs).

**Summary**: s024 is the only direct corpus win for Option A alone. But Option A
unlocks an architectural pathway that opens s013-s015, s029, s030 to subsequent
contract-driven dispatches.

## 4. Options

### Option A: Trust the canonical allow-list in dependency_summary

Smallest fix. In `analyze_expr` for `CanonicalExpr::Function`, if the function
name is in the known-static allow-list AND the inherited context allows value
results, recursively analyze args, and if no other rejection triggered AND
all args are supported, return `true` instead of calling `reject_function`.

Files: ~1-2 (`dependency_summary.rs`, possibly a small helper in
`template_canonical.rs` to share `is_known_static_function`).
LOC: ~20-40 plus tests.

Pros:
- Smallest dispatch.
- Unblocks ISNUMBER, IF, ABS, MAX, ROUND, LEN, etc. with cell-only args.
- No new public API.
- Conservative: same allow-list both layers already trust.

Cons:
- Doesn't help functions over RANGES (SUM, SUMIF, SUMIFS) — still hit
  `FiniteRangeUnsupported`.
- Allow-list maintenance burden remains; doesn't move toward dynamic
  function caps.
- Doesn't advance the existing `FunctionDependencyContract` plumbing.

### Option B: Wire function contracts to dependency_summary

Replace the hard-coded allow-list with consultation of
`Function::dependency_contract(arity)`. Requires plumbing a function
provider into the FormulaPlane summary layer.

Files: ~4-8 (`dependency_summary.rs`, `producer.rs`, `placement.rs`, plus
tests, plus some functions need contracts added).
LOC: ~150-400.

Pros:
- Architecturally cleaner.
- Self-maintaining: new functions opt in via contract.
- Could simultaneously enable scalar + reduction + criteria support.

Cons:
- Larger blast radius.
- Several allow-list functions (IF, AND, OR, CONCAT, LEN, ROUND, MAX, MIN,
  ...) need contracts added before they'd promote.
- IF needs a `LazyBranch`-aware contract to handle short-circuit safely.
- Requires plumbing function provider into FormulaPlane (not currently
  available).

### Option C: Option A + finite range precedent support

Extend Option A to handle precedents that are ranges with both axes finite
(e.g., `SUM(A1:A100)`). Adds `PrecedentPattern::Range` and corresponding
`SpanReadDependency` machinery.

Files: ~4-7. LOC: ~200-500.

Pros:
- Unlocks s013/s014 (SUMIFS) over static ranges.

Cons:
- More work + correctness risk.
- Range-projection math is subtle.
- Some functions need argument-role semantics (criteria, lookup) that this
  alone doesn't provide.

### Option D: Per-function metadata for arg shape

Generalize Option C using `FunctionDependencyContract`.

Files: ~6-12. LOC: ~300-800+.

Same as Option B but combined with range support. Best long-term answer
but biggest single change.

## 5. Risks

- Existing behavior for non-promoting scenarios must NOT regress.
- The test `formula_plane_dependency_summary_rejects_sum_range_not_pointwise_authority`
  asserts BOTH `FiniteRangeUnsupported` AND `FunctionUnsupported{SUM}`.
  Under Option A, the second assertion needs to be removed (SUM is now
  trusted; rejection still happens via `FiniteRangeUnsupported`).
- The canonical allow-list should be audited for entries that AREN'T
  actually pointwise-pure (e.g., context-sensitive text functions).
- IF/AND/OR short-circuit: dependency summary should collect ALL branch
  precedents to over-approximate safely. Confirm `analyze_expr` already
  does this (it iterates all args before rejecting/accepting).

## 6. Recommendation

**Option A as the next dispatch.**

Reasoning:
- Direct corpus win: s024.
- Lowest risk (1-line policy change + test updates).
- Unblocks the architectural path: future Option B / C / D dispatches
  inherit a working function-pass-through model.
- The function-contract machinery (Option B-style) is real and worth
  pursuing later, but landing it now would gate several scenarios on
  contract additions that aren't critical.

Sequence:
1. **Now**: Option A — share `is_known_static_function` across layers,
   teach dependency summary to accept known-static functions with
   supported args.
2. **Later (separate dispatch)**: Option C — add `PrecedentPattern::Range`
   and finite-range read summaries, unlocks SUM(A1:A100)-style families.
3. **Later (separate dispatch)**: Option B — wire function contracts to
   replace the hard-coded allow-list, add contracts for IF/AND/OR/etc.
   Combined with Option C, this unlocks SUMIFS et al.
4. **Eventually**: cross-sheet read projection (separate workstream).

## 7. Test surface

For Option A:

Smallest reproducer (in `dependency_summary.rs` `#[cfg(test)] mod tests`):
- `=ISNUMBER(A1)` → `StaticPointwise`, precedent A1, no rejects.
- `=IF(ISNUMBER(A1), A1*2, 0)` → `StaticPointwise`, precedent A1 (deduped).
- `=ABS(A1)` → `StaticPointwise`, precedent A1.
- `=ROUND(1.234, 2)` → `StaticPointwise`, no precedents.

Still rejected:
- `=CUSTOMFN(A1)` → unknown function (canonical reject).
- `=RAND()+A1` → volatile (canonical reject).
- `=INDIRECT(A1)` → dynamic (canonical reject).
- `=INDEX(A1:A3, 1)` → reference-returning (canonical reject).
- `=LET(x, A1, x+1)` → local env (canonical reject).
- `=SUM(A1:A10)` → `FiniteRangeUnsupported` (range-arg path; SUM no longer
  in `FunctionUnsupported`).
- `=SUM($A:$A)` → `WholeAxisUnsupported`.

Existing tests to update:
- `formula_plane_dependency_summary_rejects_sum_range_not_pointwise_authority`:
  remove the `FunctionUnsupported{SUM}` assertion.

Corpus assertions:
- s024 should produce spans for both ISNUMBER and IF/ISNUMBER families.
  Add invariant or assert via post-fix corpus run.
- s022 / s028 should remain at zero spans.
- s013 / s014 / s026 should remain rejected until range support.

## 8. Hedge audit

Forbidden patterns:
- Feature flag for the new function support.
- "TODO: enable IF later" — IF must work after Option A lands.
- Defer to function-contracts-v2 — that's a different (later) dispatch.
- Frame Option A as a "temporary workaround". It's the right conservative
  next step.
- Mark s024 expected_to_fail to defer the fix.
