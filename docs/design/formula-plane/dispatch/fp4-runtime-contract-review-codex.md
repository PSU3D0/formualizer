# FP4.0 runtime contract review — Codex

Date: 2026-04-30  
Reviewer model: `openai-codex/gpt-5.5`  
Reviewed commit: `6fe71c9 docs(formula-plane): draft runtime contract`  
Scope: read-only architecture review of `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_CONTRACT.md`.

## Verdict

**REVISION-REQUIRED**

The architecture direction is sound, but the contract needs a few correctness invariants tightened before FP4.A. The main issue is not the high-level model; it is that several terms are currently precise enough for discussion but not precise enough for implementation without accidentally reusing today’s lossy scanner/template IDs or over-trusting coarse `FnCaps`.

## Top Strengths

- Clear separation between formula family and formula class; whole-template identity vs semantic execution contract is the right split.
- Conservative dependency-summary invariant is the right core rule for dirty correctness, as long as its scope is expanded to include metadata and structural dependencies.
- The phased materialization sequence is appropriately cautious: passive summaries before graph hints, graph hints before materialization reduction, span execution last.
- The design explicitly rejects global workbook-size switches and hidden graph bypass, which aligns with current default-behavior constraints.
- The contract correctly separates dependency summaries from evaluation semantics, especially for short-circuit functions like `IF`.

## Blocking Issues

- The dependency-summary invariant is underspecified for implementation. `true_scalar_dependencies(run) ⊆ summarized_dependencies(run)` is necessary but not sufficient unless “true dependencies” explicitly includes named ranges, table metadata, structured-reference rewrites, open/whole-row/whole-column used-region growth, external/3D refs, volatile recalc sources, and structural sheet/name/table changes.
- Formula-family identity must be exact enough to prevent false template merging. The current scanner canonicalization in `crates/formualizer-bench-core/src/bin/scan-formula-templates.rs` collapses literal values to literal kinds, so `=A1+1` and `=A1+2` can share a template ID. That is acceptable only for passive opportunity metrics, not for FP4 dependency summaries, shared AST, or later authority.
- The function-contract boundary is too broad and conflates passive dependency classification with future span evaluation kernels. FP4.A should not require `compile_span_call` or `eval_span`; it needs a minimal dependency-only registry that handles current `Function`, `ArgSchema`, `FnCaps`, `eval_reference`, by-ref arguments, and unknown/custom functions conservatively.
- Mixed absolute/relative references are not modeled precisely enough. `RelativeCell`/`AbsoluteCell` and `RelativeRange`/`AbsoluteRange` are insufficient for Excel refs like `$A1`, `A$1`, `$A1:B$2`, and independently anchored range endpoints. The dependency pattern should be affine per axis/end-point.
- `SharedTemplateAst` is unsafe as currently named unless it is explicitly a placement-aware FormulaPlane template AST/span IR, not a scalar `ASTNode` reused by the current interpreter. Current scalar AST references are absolute at evaluation time; sharing them across placements would require on-demand reification or placement-aware evaluation.

## High-Priority Nits

- Reconcile phase naming between `docs/design/formula-plane/REPHASE_PLAN.md` and `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_CONTRACT.md`: the rephase plan has FP4 as loader/shared-formula bridge and FP5 as dependency summaries, while the runtime contract calls FP4.A passive dependency summaries.
- Add `OpenRect` / partially bounded range support to the dependency vocabulary, matching current `RangeKey::OpenRect` in `crates/formualizer-eval/src/engine/plan.rs`.
- State that over-approximation can create false cycles and that those partitions must fall back to legacy SCC/cycle handling unless an explicit recurrence/cycle policy accepts them.
- Treat `FnCaps` as hints, not contracts. Several semantic distinctions needed by FormulaPlane are not represented or are incomplete today; for example `IFERROR` and `IFNA` have lazy branch behavior but are not marked `SHORT_CIRCUIT`.
- Define the supported FP4.A subset explicitly: finite same-sheet/cross-sheet cell and range refs, mixed anchors, arithmetic/comparison operators, conservative branch union, and fallback for dynamic/volatile/name/table/external/3D unless resolved by explicit rules.

## Specific Suggested Edits

- In section 4.1, add: “Family identity includes literal values, function canonical identity, operator kind, argument count/order, reference anchoring, sheet/name/table binding mode, and value-vs-reference context. Diagnostics may use coarser fingerprints only when explicitly labeled non-authoritative.”
- In section 6.1, replace `RelativeCell` / `AbsoluteCell` / `RelativeRange` / `AbsoluteRange` with an affine model, e.g. `AffineCell { sheet, row: AxisRef, col: AxisRef }` and `AffineRect { start_row, start_col, end_row, end_col }`, where each axis can be absolute, relative-to-placement, open, whole-axis, or unsupported.
- In section 6.1, add explicit variants/reasons for `OpenRect`, `ThreeDReference`, `StructuredReferenceUnresolved`, `NamedRangeUnresolved`, `ExternalReference`, `ReferenceReturningFunction`, `UnknownFunction`, `CustomFunction`, and `UsedRegionDependentRange`.
- In section 6.2, define run summary instantiation over the actual accepted run cells only; holes, exceptions, rejected cells, and singletons must never inherit a neighboring run’s summary.
- In section 6.4, add that dynamic references may still report child-expression dependencies, but they cannot claim compact dependency authority unless runtime target tracking exists.
- In section 7, split the conceptual trait into two layers: `FormulaPlaneDependencyContract` for FP4.A/FP4.B and a later `FormulaPlaneSpanKernel` for FP7+.
- In section 7.1, add per-argument evaluation modes: eager value, lazy/short-circuit, by-reference, criteria expression, reference-returning, and current-cell-sensitive.
- In section 8, add explicit handling or rejection for `@` implicit intersection, `:` reference combinator, array literals/spills, `Call`/LAMBDA, and reference-context vs value-context lowering.
- In section 10, define `SharedTemplateAst` as “normalized template IR plus placement reifier” or rename it to avoid implying scalar AST reuse.
- In section 11, add an implementation budget: Tier 0 must be emitted during the existing parse/ingest pass, Tier 1+ must be lazy/local, and runtime code must not call the current full `FormulaRunStore::build` for mostly unique small workbooks.

## Recommended Next Implementation Slice For FP4.A

Implement a passive dependency-template summary slice with no graph behavior change:

1. Build an exact normalized-template fingerprint for FP4.A, separate from the current lossy scanner ID.
2. Add a dependency-only analyzer under `crates/formualizer-eval/src/formula_plane/` for literals, references, unary/binary operators, arrays, and conservative branch unions.
3. Support affine same-sheet and static cross-sheet cells/ranges with mixed absolute/relative anchors; reject or summary-only classify open ranges, names, tables, external refs, 3D refs, dynamic refs, volatile formulas, unknown/custom functions, and reference-returning functions.
4. Instantiate summaries for existing `FormulaRunStore` runs and emit counters/reason codes only.
5. Add shadow comparison against current `DependencyGraph::plan_dependencies` / `RangeKey` output on bounded fixtures, reporting exact match, conservative overage, and rejection counts without claiming runtime wins.
