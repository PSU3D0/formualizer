# FP4.B Function Dependency Plan Architecture Review

Date: 2026-05-02
Branch: `formula-plane/bridge`
Reviewed plan: `docs/design/formula-plane/dispatch/fp4b-function-dependency-plan.md`

## Verdict

PASS-WITH-NITS

The plan is scoped correctly as a passive FormulaPlane dependency-taxonomy phase.
It does not require source/runtime behavior changes before implementation, and no
blocking architecture issue was found.

## Blocking issues

None.

## Nonblocking nits

- Make registry drift diagnostics an explicit FP4.B deliverable, not only an optional helper. The runtime contract says classification must consume `FnCaps`, `ArgSchema`, `eval_reference` capability, and registry identity; report-only drift checks are the right passive mechanism.
- Clarify that `FunctionSupportStatus::Supported` means "potentially supported for this name/arity"; the summary pass must still reject unsupported argument shapes such as whole/open ranges, names, tables, spills, arrays, dynamic refs, or unsupported nested functions.
- Update wording around rectangle support to match current code shape: `AffineRectPattern` exists, but `PrecedentPattern::Rect(AffineRectPattern)` still needs to be added/activated.
- Add an explicit finite-rectangle reverse-summary/demotion rule. If run-level or reverse dirty mapping for rect patterns cannot be represented conservatively, FP4.B should demote the run summary or report fallback rather than emitting misleading reverse counters.
- Prefer nesting scanner taxonomy under `dependency_summaries.function_dependency_taxonomy`; it preserves the FP4.A scanner surface and keeps the taxonomy tied to passive dependency-summary reporting.

## Architecture assessment

- Passive-only scope is correct. The plan repeatedly excludes scheduler, evaluator, graph mutation, dirty propagation, loader behavior, materialization reduction, public/default API changes, and runtime-win claims.
- Avoiding changes to the core `Function` trait is sound for FP4.B. The trait is object-safe runtime/eval infrastructure, while this phase needs crate-internal passive dependency contracts and reject reasons.
- A FormulaPlane-local contract registry is idiomatic Rust if kept `pub(crate)`, centralized, deterministic, and table/match driven. It also avoids scattering function-name conditionals across `template_canonical.rs`, `dependency_summary.rs`, and scanner code.
- The approach is compatible with existing `FnCaps` and `ArgSchema` because those surfaces are coarse validation/eval metadata, not complete dependency contracts. They should feed report-only drift checks instead of becoming the sole source of dependency truth.
- The supported set is bounded enough: selected scalar all-arg functions, finite static reductions, and finite criteria aggregations. Classified-only families are also explicitly bounded and rejected/deferred.
- Finite reductions and criteria aggregations are specified well enough to avoid under-approximation if implementation treats every finite range/cell argument as a precedent, rejects all non-finite/unsupported shapes, and compares against the fixed planner policy before counting support.
- Stop conditions are sufficient for passive implementation, with the recommended addition that finite-rectangle run/reverse instantiation must either be conservative or explicitly demoted.
- Hidden graph/eval/dirty/materialization/public API risk is low because outputs remain diagnostics only. The main future risk is misreading FP4.B summaries as graph or dirty authority before FP5/FP6 policies prove reverse invalidation and materialization semantics.

## Specific recommended edits

- In FP4.B.1 or FP4.B.6, require a drift diagnostic pass that compares local contracts with registry metadata: `FnCaps::VOLATILE`, `FnCaps::DYNAMIC_DEPENDENCY`, `FnCaps::RETURNS_REFERENCE`, `FnCaps::REDUCTION`, `FnCaps::LOOKUP`, `FnCaps::SHORT_CIRCUIT`, `ArgSchema::by_ref`, `ShapeKind`, arity, and aliases where available.
- In the contract type section, document that `Supported` is a contract-level status and that summary emission remains argument-shape gated.
- In the finite-range section, state that planner-vs-summary comparison is rectangle containment over finite cells/ranges with matching sheet binding; summary ranges may cover planner cells/ranges, but any excess must increment over-approximation counters.
- In FP4.B.4, add tests for copied finite-window reductions and mixed-anchor finite ranges, plus a case proving missing range coverage increments `under_approximation_count`.
- In FP4.B.5, add criteria-expression dependency tests such as `=COUNTIFS(A1:A10,">"&C1)`, omitted target-range cases for `SUMIF`/`AVERAGEIF`, and an `AVERAGEIFS` case proving it is classified by the new registry rather than reported as unknown.
- In scanner output, choose Option A unless there is a concrete consumer need for a top-level `function_dependency_taxonomy` section.

## Implementation may proceed

Yes. Implementation may proceed after this review. The nits can be folded into
coding or a small follow-up plan edit; none require changing source behavior,
public APIs, graph authority, dirty propagation, or materialization policy.
