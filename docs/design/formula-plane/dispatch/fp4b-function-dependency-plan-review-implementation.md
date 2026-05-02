# FP4.B Function Dependency Plan Implementation/Testability Review

Date: 2026-05-02
Branch: `formula-plane/bridge`
Reviewed plan: `docs/design/formula-plane/dispatch/fp4b-function-dependency-plan.md`
Related architecture review: `docs/design/formula-plane/dispatch/fp4b-function-dependency-plan-review-architecture.md`

## Verdict

PASS-WITH-NITS

The plan is implementable as a passive FormulaPlane function dependency taxonomy.
The phase ordering is generally sound, the supported function set is bounded, and
no source/runtime behavior change is required before FP4.B.1 can start.

The main implementation risk is churn from a few underspecified contracts:
`FunctionSupportStatus` semantics, finite rectangle comparison/reverse handling,
criteria aggregation target-range semantics, scanner counter denominators, and
registry drift diagnostics. These can be resolved during coding or by a small
follow-up plan edit; none require broadening FP4.B beyond passive diagnostics.

## Blocking Issues

None.

## Nonblocking Nits

- Pin `FunctionSupportStatus` outcomes exactly. FP4.B.1 tests should not accept
  "classified-only or rejected" for the same function; choose the status and
  reject reason for every classified-but-unsupported class.
- Define the catch-all for known registered builtins that are not in the FP4.B
  supported or explicitly deferred lists. A safe default is `Unsupported` /
  `Rejected` with `UnsupportedFunctionClass`, while registry misses remain
  `OpaqueScalar` / `UnknownFunction`.
- Make registry drift diagnostics a required diagnostics deliverable, but keep
  them report-only and outside the dependency-summary decision path. Scanner
  code currently parses formulas without loading builtins, and some functions
  such as `LET`/`LAMBDA` can panic from `arg_schema()` through the default
  `Function` implementation.
- Choose scanner output Option A now:
  `dependency_summaries.function_dependency_taxonomy`. The plan should also say
  whether taxonomy counts are per authority template, per function occurrence,
  or per run, and how mixed templates such as `SUM(...) + RAND()` are counted.
- Specify the finite rectangle comparison rule in code-shaped terms: matching
  sheet binding, finite region containment, planner cells covered by summary
  cells or rects, planner rects covered by summary rects, and over-approximation
  counted without expanding large ranges.
- Specify the finite `Rect` run/reverse policy. Either implement a conservative
  inverse mapping for rectangle patterns or demote run summaries with an explicit
  fallback; do not silently reuse the current affine-cell reverse logic.
- Add SUMIF/AVERAGEIF target-range rules. Omitted target ranges use the criteria
  range; provided target ranges either need same finite shape or need Excel-style
  expansion from top-left to the criteria range dimensions. Reject unsupported
  shapes explicitly to avoid semantic under-approximation.
- Add `AVERAGEIF` and `AVERAGEIFS` canonical/classifier tests. They are in the
  FP4.B supported list but are not in the current `is_known_static_function` set,
  so they are an easy source of accidental `UnknownFunction` churn.

## Implementation/Testability Assessment

The plan is specific enough for FP4.B.1 and FP4.B.2 if the central classifier is
treated as the only owner of normalized function names, class/status assignment,
arity/pairing validation, and drift labels. `template_canonical.rs` currently has
private hard-coded function lists, and `dependency_summary.rs` has separate
function rejection helpers; moving both through one FormulaPlane-local module is
the right first slice.

The phase slices are mostly small and correctly ordered:

- FP4.B.1 adds the model and deterministic classifier without changing summary
  behavior.
- FP4.B.2 unifies canonicalizer and summary rejection before new support is
  added, which prevents two independent taxonomies from diverging.
- FP4.B.3 is a safe scalar wrapper slice because it only unions already-supported
  child dependencies.
- FP4.B.4 is the largest slice. It touches `PrecedentPattern::Rect`, finite range
  extraction, planner comparison, run instantiation, and reverse summaries; split
  it internally if rectangle reverse mapping is not straightforward.
- FP4.B.5 is properly after finite rectangle support because criteria functions
  need finite criteria/value ranges and criteria-expression dependencies.
- FP4.B.6 belongs after summaries are stable because scanner JSON should reflect
  the implemented taxonomy rather than drive it.

The supported summary functions are pinned well enough. Static scalar functions,
finite reductions, and criteria aggregations are bounded and should remain the
only supported FP4.B summaries. The rejected/deferred functions are directionally
clear, but the plan should make the status/reason exact for mask conditionals,
lookups, reference-returning functions, dynamic dependencies, volatile functions,
local environments, array/spill functions, known unsupported builtins, and
unknown/custom functions.

The data model direction is sound but needs a few concrete choices before B.4:
`AffineRectPattern` exists today, while `PrecedentPattern::Rect` does not; public
diagnostics must not expose crate-private enum types; and if precedent roles
such as value range, criteria range, and criteria expression are expected in JSON
or debug output, their storage location should be explicit rather than inferred
from function arguments later.

Scanner JSON expectations are close but not yet test-tight. Existing sections
must remain stable, and nesting the taxonomy under `dependency_summaries` keeps
FP4.A output compatible. Add a scanner unit test that serializes one workbook
with supported, rejected, and unknown functions and asserts the nested taxonomy
version, counters, and fallback histogram keys.

The test gates are bounded. The existing `formualizer-eval formula_plane` and
`formualizer-bench-core --features formualizer_runner` gates are appropriate.
Add focused tests for exact status/reason classification, finite range
containment, over/under-approximation counters, copied mixed-anchor ranges,
criteria expression dependencies, SUMIF/AVERAGEIF target-range behavior, and
private/public diagnostics conversion.

## Compile/API Visibility Pitfalls

- `formula_plane::dependency_summary` and `formula_plane::template_canonical` are
  crate-private; `formula_plane::diagnostics` is public only behind
  `formula_plane_diagnostics`. Keep `function_dependency.rs` crate-private and
  expose scanner-safe strings or public diagnostic mirror structs through
  `diagnostics.rs`.
- Do not put crate-private `FunctionDependencyClass`,
  `FunctionDependencyRejectReason`, or `FunctionDependencyContract` directly in
  public diagnostics structs; Rust will reject private types in public APIs.
- The scanner crate already enables `formualizer-eval/formula_plane_diagnostics`,
  so bench-core should call only public diagnostics functions and should not
  reach into crate-private FormulaPlane modules.
- If drift diagnostics call `function_registry::snapshot_registered()` or
  `function_registry::get()`, make registry initialization deterministic for the
  diagnostics path or treat an empty registry as a drift input, not as a summary
  rejection.
- Guard or avoid generic `arg_schema()` calls for functions with custom dispatch
  and no schema override. The default `Function::arg_schema()` panics when
  `min_args() > 0`, and `LET`/`LAMBDA` are known examples.
- Keep `FnCaps`, `ArgSchema`, registry identity, aliases, and `eval_reference`
  capability report-only in FP4.B. They should detect contract drift, not change
  evaluation, graph, dirty, loader, or public API behavior.

## Missing Edge Cases Likely To Cause Churn

- `SUMIF(A1:A10,">0",B1)` and `AVERAGEIF(A1:A10,">0",B1)` need either expanded
  target dependency ranges or explicit rejection.
- `SUMIF`/`AVERAGEIF` with omitted target range should include the criteria range
  as the value range.
- `COUNTIFS(A1:A10,">"&C1)` and similar criteria expressions must include the
  criteria-expression cell dependency.
- `SUMIFS`/`COUNTIFS`/`AVERAGEIFS` malformed pair counts should have distinct
  `InvalidArity` versus `InvalidCriteriaPairing` outcomes.
- `AVERAGEIF(S)` should not remain classified as unknown after FP4.B.2.
- `_xlfn.`/`_xll.`/`_xlws.` prefix stripping should be owned by the central
  classifier so canonicalizer, summary, scanner, and drift diagnostics agree.
- Multi-function templates need severity precedence for scanner counters.
  Rejected/dynamic/volatile should dominate supported when any function in the
  template makes the summary unsupported.
- Direct finite ranges in unsupported contexts should continue rejecting even
  after reductions support finite ranges in approved argument roles.

## Specific Recommended Edits

- In FP4.B.1, add exact status/reason expectations for each classified-only
  function in the required test table, and define the known-but-unsupported
  catch-all separately from unknown/custom functions.
- In FP4.B.1, replace the illustrative `FunctionArgContract` placeholder with
  concrete variants for all-args, variadic reductions, fixed arguments, and
  criteria pairs.
- In FP4.B.1 or FP4.B.6, require a report-only drift diagnostic that handles
  missing registry data and schema panics without changing summary support.
- In FP4.B.4, document finite-region containment comparison and add tests for
  planner cells covered by summary rects, planner rects covered by summary rects,
  over-approximation, and deliberate missing coverage.
- In FP4.B.4, state the run/reverse rule for `PrecedentPattern::Rect`: support
  only if conservative reverse invalidation can be represented, otherwise demote
  with an explicit fallback reason.
- In FP4.B.5, add tests for `SUMIF`/`AVERAGEIF` omitted target ranges, provided
  target range expansion or rejection, `AVERAGEIF` classification, and criteria
  expression dependencies.
- In FP4.B.6, choose the nested scanner JSON placement and add a serialization
  test for the taxonomy version, counters, and fallback histogram.

## Implementation May Proceed

Yes. Implementation may proceed after this review. The nits should be folded
into coding or a small follow-up plan edit before the relevant phase slices,
especially before FP4.B.4 finite rectangles and FP4.B.5 criteria aggregations.
FP4.B should remain passive: no graph authority, dirty propagation,
materialization reduction, loader behavior, evaluation semantics, public API, or
runtime-win claim should change.
