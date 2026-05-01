# FP4.0 runtime contract review — Opus

Date: 2026-04-30  
Reviewer model: `anthropic/claude-opus-4-7`  
Reviewed commit: `6fe71c9 docs(formula-plane): draft runtime contract`  
Scope: read-only architecture review of `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_CONTRACT.md`.

## Verdict

**REVISION-REQUIRED** — narrowly scoped. The high-level layering is sound and the non-goals are well chosen. But three contracts — dependency-summary invariant, function-contract surface, materialization downgrade rules — are either one-sided or inconsistent with the existing `Function` / `DependencyGraph` / `CollectPolicy` machinery in a way that will bite at FP4.A. Three smaller doc tightenings are also needed before code starts. None of this is fatal; the document mostly needs to specify what it currently waves at.

## Top strengths

1. **Family vs class is the right cleavage.** Family = whole normalized AST identity, class = semantic contract category. This matches what the bench scanner already produces and avoids the trap of one class per textual shape.
2. **Per-run materialization policy, no global formula-count gate.** Explicit rejection of `if formula_count > N` is correct — FP3 already shows that compact-ratio is dominated by local template/run shape, not workbook total.
3. **Compatibility materialization must be counted.** Calling out the silent-allocation failure mode is the right correctness gate.
4. **Phasing FP4.A as passive dependency-template summaries** keeps authority off the critical path until the function taxonomy and span IR are real, which matches the FP1–FP3 trajectory.
5. **Span IR positioned as a compile target, not a public syntax.** This avoids prematurely promoting experimental types out of `formualizer-eval`.

## Blocking issues

### B1. Dependency-summary invariant is one-sided (precedent only)

The contract states `true_scalar_dependencies(run) ⊆ summarized_dependencies(run)` but says nothing about the dependent/reverse direction. Dirty propagation in `DependencyGraph` is driven by the reverse map; for any run that wants compact authority and needs to be invalidated correctly when a precedent changes, the invariant must also include:

```text
true_scalar_dependents(precedent_cell) ⊆ summarized_dependents(precedent_cell)
```

Partition-edge dirtying must over-approximate the scalar dependent set, and the summary must be able to map a single changed precedent cell back to one or more dependent partitions without enumerating cells. Without this, `SummaryEdgesSidecar` has no defined semantics on incremental edits.

Specific holes:

- **Volatile** is listed as a class and the existing code treats `FnCaps::VOLATILE` as an orthogonal flag. The contract should choose: either Volatile is a class-level requirement, or it is a flag that composes with classes.
- **Whole-column / whole-row references** are unbounded; "all required precedent result partitions complete before dependent partition executes" is undefined when the precedent set is the entire column. The doc must either bound or disallow this case in the first FP4 cut.
- **Conservative branches under dynamic dependency:** `IF(cond, A1, OFFSET(...))` has a static-looking conservative summary but a dynamic untaken branch. The doc must specify that `DYNAMIC_DEPENDENCY` anywhere in the subtree forces fallback regardless of outer class.
- **Cycles:** A conservative precedent superset can synthesize a cycle that the scalar engine does not see. Recommended: any run whose summary participates in a cycle that the AST-level dependency walker would not produce must be demoted out of summary authority.

### B2. `FormulaPlaneFunctionContract` overlaps and contradicts the existing `Function` trait

The proposed trait includes `dependency_contract`, `span_caps`, `compile_span_call`, and `eval_span`. The existing trait already exposes capability flags, `arg_schema`, and `eval_reference`.

The contract should explicitly state:

1. FP4.B is a consumer of `FnCaps` + `arg_schema`, not a parallel taxonomy. The class table should be derived from these caps, not asserted independently.
2. `RETURNS_REFERENCE` is not the same as `DynamicDependency`. `INDEX(A:A,3)` returns a reference but its dependency can be static; `OFFSET`/`INDIRECT` are dynamic.
3. New FormulaPlane caps should extend or layer cleanly with `FnCaps`; splitting caps across unrelated enums creates registration ambiguity.
4. `LET` / `LAMBDA` / `REDUCE` / `MAP` need explicit handling. The interpreter has a real `LocalEnv`, and dependency-summary derivation must specify how locally-bound names and lambda bodies are analyzed.
5. `compile_span_call`/`eval_span` assume `SpanInputs`/`SpanOutput`/`SpanEvalContext` designs that are not defined. This is FP6+ work; FP4 should defer eval-side methods and keep only dependency contracts in scope.

### B3. Materialization policy: no downgrade contract, no edge-edit semantics

The policy list lacks:

- the downgrade path when a run becomes ineligible;
- what `SummaryEdgesSidecar` means in terms of graph mutation and edit invalidation.

Examples:

- A `SpanExecutable` run with a new mid-run exception has no defined transition. Does it demote the whole run, split into sub-runs, or remain span with exception list?
- `SummaryEdgesSidecar` cannot answer "given a single edited precedent cell, which formula partitions must be re-evaluated?" without either enumerating dependents or building an explicit reverse summary.

The contract must define at minimum:

- a state machine over `MaterializationPolicy` with explicit allowed transitions and counters;
- the per-cell-edit contract for any policy that skips per-cell edges.

## High-priority nits

### N1. Family identity needs the normalization basis spelled out

Add a pointer to `FORMULA_PLANE_V2.md` coordinate basis: relative axes normalized to placement-anchor delta, absolute axes remain literal VC coordinates, explicit sheet identity is stable. Include an absolute-axis example.

### N2. Canonicalization currently lives in `bench-core`, not `formualizer-eval`

The only canonical-AST/relative-fingerprint code is in `crates/formualizer-bench-core/src/bin/scan-formula-templates.rs`. FP4.A needs either a move/rewrite or an explicit migration path.

### N3. "Parse once" vs Tier 0 fingerprinting

Adding canonicalization is a second AST traversal unless fused with dependency planning. The contract should declare whether canonicalization is fused with `extract_dependencies`/`plan_dependencies`, or accept a second walk and set a small-workbook overhead budget.

### N4. Precedent pattern enum is incomplete vs `ReferenceType`

The parser has `Cell`, `Range`, `Cell3D`, `Range3D`, `External`, `Table`, and `NamedRange`. Add explicit variants for 3D references, structured/table references with current-row context, and named ranges that resolve to expressions.

### N5. Spill / dynamic-array results are not in result-region model

The result region should be first-class. Dynamic array spills require known or conservatively bounded result regions; unbounded/data-dependent spills force fallback.

### N6. `CollectPolicy` interaction is undefined

`CollectPolicy { expand_small_ranges, range_expansion_limit }` directly changes what scalar dependencies are. Summary correctness must be defined with respect to a fixed `CollectPolicy`; mismatches are fallback reasons.

### N7. Cancellation token

Span kernels must honor the existing cooperative cancellation path. Not FP4.A scope, but it belongs in the function/span contract.

### N8. Self-recurrence is a partition shape, not just a class

`IntraRunRecurrence` runs with offset `-1` require intra-partition serial order until recurrence-aware kernels exist.

## Specific suggested edits

1. **§4.3 Dependency summary** — add the dependent-direction invariant:

   ```text
   true_scalar_dependents(precedent_cell) ⊆ summarized_dependents(precedent_cell)
   ```

   Any policy that skips per-cell edges must provide an explicit `precedent_cell -> dependent_partition_set` map.

2. **§4.1 Formula family** — add that normalization preserves the FP V2 stored-reference convention: relative axes normalize to placement-anchor deltas; absolute axes remain literal VC coordinates; explicit sheet identity is stable.

3. **§4.2 Formula class** — note that volatility is an orthogonal flag layered on any class, and `DynamicDependency` is recursive.

4. **§6.1 Template-level analysis** — extend `PrecedentPatternKind` with `Cross3DRange`, `StructuredTableThisRow`, and `NameDereferenceExpression`. Distinguish static named ranges from named expressions.

5. **§7 Function contracts** — restrict FP4.B trait surface to dependency contracts only. Move `compile_span_call`/`eval_span` to FP6+. Add a `LET`/`LAMBDA`/local-environment paragraph. Distinguish `RETURNS_REFERENCE` from `DYNAMIC_DEPENDENCY`.

6. **§10 Materialization policy** — add a state-machine subsection with allowed transitions, triggers, and counters. Define `SummaryEdgesSidecar` semantics for both edge directions.

7. **§11 Small-workbook overhead** — add concrete budgets such as Tier 0 fingerprinting overhead thresholds at 10/100/1k/5k formulas.

8. **§12 Scheduling** — state that conservative summaries that synthesize a cycle absent from the AST-level walker must demote out of summary authority.

9. **§14 FP4.A** — add explicit extraction of bench canonicalizer into `formualizer-eval/src/formula_plane/template_canonical.rs` as a passive utility.

10. **§15 Open questions** — add questions for `LocalEnv`/lambda dependency analysis, `CollectPolicy` correctness, and cycle synthesis.

## Recommended FP4.A implementation slice

Keep FP4.A passive, narrow, and read-only:

1. **Extract canonicalization.** Move/port canonical-AST and label-set logic from `scan-formula-templates.rs` into `crates/formualizer-eval/src/formula_plane/template_canonical.rs`. No public API. Add tests for relative-only copy, absolute anchors, mixed `IF` with dynamic child, named range, structured table current-row, and lambda body.

2. **Add `FormulaDependencyTemplateSummary` passively.** New module `formula_plane/dependency_summary.rs` producing `(template_id, formula_class, precedent_patterns, fallback_reasons)`. Walk canonical AST, map each `ReferenceType` to a pattern, classify by composing with `FnCaps`/`arg_schema` from the registry. Do not invent a new eval-side function trait yet.

3. **Define both directions of the dependency invariant in code.** Add a property-test scaffold comparing summarized precedents/dependents against current dependency extraction for the same AST.

4. **Run-instantiated summaries on top of FP3 `FormulaRunStore`.** Emit a companion `RunDependencySummary` as a passive report. Reconcile counters with existing FP2/FP3 reporting style.

5. **Scanner JSON exposure.** Add a `dependency_summaries` section to `scan-formula-templates` output. No graph behavior change.

6. **Small-workbook gate baseline.** Before ingest integration, record canonicalization-pass time on dense scenarios and deliberately small formula scenarios. Establish the budget later enforced by §11.

Explicitly out of scope for FP4.A: any new function trait, span-IR lowering, ingest changes, `DependencyGraph` changes, or small-workbook gate enforcement beyond baseline measurement.

## Summary of risks and missing invariants

| # | Risk | Severity |
|---|---|---|
| R1 | One-sided dependency invariant; reverse direction undefined | High |
| R2 | New function trait duplicates `FnCaps` and conflates references vs dynamic deps | High |
| R3 | No materialization-policy state machine / downgrade semantics | High |
| R4 | Conservative-summary cycle synthesis not handled | Medium |
| R5 | LET/LAMBDA local environment unspecified | Medium |
| R6 | Spill / dynamic-array result region absent | Medium |
| R7 | Canonicalizer lives in bench-core only | Medium |
| R8 | `CollectPolicy` not part of correctness statement | Low |
| R9 | Whole-column / 3D / structured-table coverage gaps in `PrecedentPatternKind` | Low |
| R10 | Small-workbook overhead has no numeric budget | Low |

No claim is made here about runtime wins. All comments are about correctness contracts and fit with existing `formualizer-eval` surfaces.
