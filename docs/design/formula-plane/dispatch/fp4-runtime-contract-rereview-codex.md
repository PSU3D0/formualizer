# FP4.0 runtime contract re-review — Codex

Date: 2026-04-30  
Reviewer model: `openai-codex/gpt-5.5`  
Reviewed commit: `7a1e09b docs(formula-plane): fold runtime contract review feedback`  
Scope: read-only architecture re-review of `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_CONTRACT.md`.

## Verdict

**PASS-WITH-NITS**

The revised FP4.0 contract is technically sound enough to begin a passive FP4.A slice. It addresses the prior blocking feedback: exact family identity, class/family separation, bidirectional dependency invariants, function-boundary scope, materialization downgrade semantics, and small-workbook budgets are now present.

This is not approval for graph bypass, dirty-propagation authority, materialization reduction, or span execution. Those still need implementation proofs and oracle coverage.

## Top Strengths

- **Family vs class is now correct:** family is whole normalized AST identity; class is a semantic dependency/evaluation contract. This avoids merging formulas that only share a subexpression or broad shape.
- **Dependency correctness is bidirectional:** the added reverse-dependent invariant is the key fix for incremental edits and aligns with the current graph’s reverse-edge/range-stripe model.
- **Function boundary is appropriately narrowed:** FP4.A is dependency classification only, consuming `FnCaps`, `ArgSchema`, `eval_reference`, and registry identity without designing span kernels too early.
- **Materialization sequencing is cautious:** hint-only, shared-template IR, summary sidecars, then materialization reduction, then span execution is the right risk order.
- **Small-workbook posture is much improved:** local-structure tiering plus explicit Tier-0 budgets avoids a cheap global `formula_count > N` switch.
- **Prior review feedback was substantially folded in:** mixed anchors, open ranges, `CollectPolicy`, LET/LAMBDA, spills, conservative cycles, and downgrade semantics are all now visible.

## Blocking Issues

- None for a passive FP4.A implementation.
- Blockers still apply to later authority phases: no summary-edge sidecar or materialization reduction should ship until reverse dirty queries, policy demotion, and oracle comparison are proven.

## High-Priority Nits

- **Authority template linkage is underspecified:** current `FormulaRunStore` is keyed by scanner `source_template_id` and stores no representative exact AST/IR. FP4.A needs an explicit authority template key/sidecar, not just a new canonicalizer.
- **Comparison oracle needs a common dependency universe:** `DependencyGraph::plan_dependencies` emits cells, `RangeKey`s, names, tables, and currently ignores unsupported 3D refs. FP4.A should compare normalized regions and reverse dirty probes, not only scalar cell lists.
- **Reverse summaries need bounded-overage counters:** correctness allows conservative dirtying, but accepted compact authority should not degenerate into “dirty all runs” without an explicit fallback reason.
- **Function caps still require audit:** some current builtins have semantics not fully expressed in `ArgSchema`/`FnCaps` (`SUMIFS` criteria modes, `IFERROR`/`IFNA` lazy fallback). The doc says caps are not complete; FP4.B should make that operational.
- **Value/reference context needs an explicit traversal stack:** `:` combinator, by-ref arguments, implicit intersection `@`, `eval_reference`, and LET/LAMBDA names require context-sensitive analysis.
- **Cancellation wording should be tightened:** `RangeView` exposes cancellation, but not every traversal helper polls equally. Future span kernels should explicitly poll `FunctionContext::cancellation_token()` independent of helper choice.

## Specific Suggested Edits

- In `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_CONTRACT.md` §8.1 / §16.1, add that `FormulaRunStore` must carry or be joined with an authority-grade `FormulaTemplateKey` plus representative canonical template IR. Keep `source_template_id` diagnostic-only.
- In §8.2 / §16.5, define the comparison artifact as a common dependency model: direct cells, finite/open ranges, names, tables, structural dependencies, and unsupported refs. State that 3D/external/dynamic cases are expected reject/fallback, not false mismatches.
- In §8.5, add reverse-query metrics: changed region, dependent run partitions returned, exact vs conservative count, max/median overage, and `global_dirty_fallback` as a rejection/demotion reason.
- In §9.1, state FP4.A should use a local sidecar dependency-contract registry keyed by function registry identity; do not add eval/span methods to the scalar `Function` trait.
- In §9.3 / §10, add an analyzer context enum such as `Value`, `Reference`, `ByRefArg`, `CriteriaArg`, `ImplicitIntersection`, and `LocalBinding`; each AST child should be analyzed under an explicit context.
- In §12.1, add that demotion must atomically remove or invalidate summary/reverse sidecar state before legacy materialization takes over, and mixed scalar/summary edges must dedupe dirty propagation.
- In §13.1, clarify the noise-floor rule for Tier-0 measurements and state that a cheap Tier-0 fingerprint may be non-authoritative, while dependency summaries require exact canonicalization.
- Update `docs/design/formula-plane/REPHASE_PLAN.md` soon: the runtime contract explains the FP4 naming shift, but the two docs still conflict on FP4/FP5 scope.

## Concrete Risks And Missing Invariants

| Risk | Severity | Recommended Change |
|---|---:|---|
| Existing run store continues using lossy scanner IDs | High | Introduce authority template keys before dependency summaries attach to runs |
| Reverse summary exists but dirties too broadly | High | Add overage counters and fallback when breadth is unbounded |
| Planner comparison treats current graph output as perfect oracle | Medium | Compare through normalized dependency regions and unsupported-case labels |
| Function contracts over-trust `FnCaps` | Medium | Add FP4.B builtin audit and dependency-contract registry |
| LET/LAMBDA local names look like workbook names | Medium | Require lexical environment analysis or explicit fallback |
| Open/whole-axis ranges miss used-region growth invalidation | Medium | Include used-region/sheet-bound snapshot dependencies or fallback |
| Stale summary sidecars survive demotion | Medium | Require atomic downgrade cleanup and counters |
| Span cancellation is assumed rather than specified | Low | Require explicit polling in future kernels |

## Recommended Next Implementation Slice For FP4.A

1. Add `crates/formualizer-eval/src/formula_plane/template_canonical.rs` with exact canonical template keys and tests for literals, mixed anchors, cross-sheet refs, names, tables, dynamic refs, and LET/LAMBDA.
2. Extend the passive scanner/run-store path to carry `authority_template_key` and a representative canonical template sidecar, while preserving existing `source_template_id` as diagnostic.
3. Add `dependency_summary.rs` for finite same-sheet/cross-sheet cells/ranges, unary/binary ops, and conservative branch unions; explicitly reject open ranges, names, tables, 3D, external, dynamic refs, reference-returning functions, spills, volatile policy, and local environments unless implemented.
4. Instantiate run/partition summaries only over accepted run cells, excluding holes/exceptions/rejections, and build passive reverse mappings from changed regions to run partitions.
5. Add an oracle/comparison harness against current dependency planning and dirty-propagation probes, reporting exact matches, conservative overage, rejects, policy drift, and fallback reasons.
6. Record Tier-0 small-workbook timing baselines before any production ingest integration. Do not claim runtime wins.
