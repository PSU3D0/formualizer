# FP4.0 runtime contract re-review — Opus

Date: 2026-04-30  
Reviewer model: `anthropic/claude-opus-4-7`  
Reviewed commit: `7a1e09b docs(formula-plane): fold runtime contract review feedback`  
Scope: read-only architecture re-review of `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_CONTRACT.md`.

## Verdict

**PASS-WITH-NITS**

The dual-review feedback was folded in faithfully. The remaining issues are clarifications and tightening, not contract-level defects. Nothing in §§4–17 is structurally incorrect, but several invariants still hand-wave at the operational layer in ways that will bite at FP4.A if not nailed down before code lands.

## Top strengths

- **Family vs class cleavage is correct and now precise enough.** §6.1 enumerates the fields that must drive authority-grade identity: literals, function canonical id, anchoring per axis/endpoint, sheet binding, value/reference context, volatility/dynamic flags, and array/spill behavior. The explicit refusal to reuse `formualizer-bench-core/src/bin/scan-formula-templates.rs` fingerprints for authority is the right call — that scanner collapses literal kinds, which would silently merge `=A1+1` and `=A1+2`.
- **Both directions of the dependency invariant are now stated** (§6.3), with the dependent-direction explicitly framed as the requirement that any policy skipping scalar edges must answer `precedent → dependent partition set`. This kills the previous one-sided framing cleanly.
- **FP4 dependency contract is narrowed off the eval kernel surface** (§9). `FnCaps`/`ArgSchema`/`eval_reference` are inputs, not parallel taxonomy; `compile_span_call`/`eval_span` are deferred to FP7+. This matches the existing `Function` trait.
- **Materialization has a state machine with named transitions and counters** (§12.1). `SummaryEdgesSidecar` is gated on having defined reverse-dirty semantics (§12.2). Conservative-cycle synthesis demotion is explicit (§8.10). This rules out silent downgrade.
- **`CollectPolicy` is part of the correctness statement** (§8.2). That is the only honest formulation given that `engine/plan.rs::build_dependency_plan` materializes summary content under a specific policy (`expand_small_ranges`, `range_expansion_limit`, `include_names`); a summary not stamped with the same policy is not comparable.
- **Volatility is flagged as orthogonal, dynamic-dependency is recursive, reference-returning ≠ dynamic-dependency.** All three were live confusions in the first draft.
- **Sequence is appropriately conservative:** passive summaries → graph hints → shared-template IR → sidecar → reduction → span execution. The doc continues to refuse global formula-count gates and hidden bypass.

## Blocking issues

None. None of the remaining items would prevent FP4.A from starting under the narrowed scope of §16.

## High-priority nits

1. **Reverse-dependent invariant is stated but never sized.** §6.3 asserts the inclusion `true_scalar_dependents(P) ⊆ summarized_dependents(P)` and §12.2 requires sidecar reverse mapping, but nowhere does the contract state that the reverse summary representation must be sub-linear in scalar edges. Without that, `SummaryEdgesSidecar` collapses to "build the scalar reverse adjacency anyway," which defeats its purpose. Add an explicit storage/cost requirement or measurement gate for the reverse summary.

2. **`true_scalar_dependents` is operationally undefined.** The doc never says whether the oracle for shadow comparison is `DependencyGraph::plan_dependencies` output after `RangeKey` expansion under a fixed `CollectPolicy`, the post-build reverse adjacency in `DependencyGraph`, or some symbolic ground truth. FP4.A's comparison harness is the first thing that will need this fixed.

3. **Canonicalization vs `rewrite_structured_references_for_cell`.** Ingest rewrites this-row structured references per cell before dependency planning. If `template_canonical.rs` runs over post-rewrite ASTs at one placement and pre-rewrite ASTs at another, two members of one true family will get different canonical forms. The contract should pick a side: either canonicalize over un-rewritten AST and represent this-row as a placement-relative pattern, or run after rewrite but normalize the rewrite back to a placement-anchor delta.

4. **`TemplateSupportStatus` collision.** §6.2 says volatility is a flag, not a peer class, but `crates/formualizer-eval/src/formula_plane/span_store.rs` defines `TemplateSupportStatus { ... Volatile, Dynamic, Mixed, ... }` as peer enum variants, and the FP3 store already populates them. The runtime contract should explicitly call this existing enum out as diagnostic-only or schedule its split into `class` + `flags` so FP4.A doesn't import it as authority.

5. **Tier-0 budget at 100 formulas is noise-bound on commodity hardware.** §13.1 already concedes the noise floor at 10 formulas; the same caveat should apply at 100. As written, a passing measurement of 5% at 100 formulas is roughly indistinguishable from 0% with reasonable repetitions. Either widen the floor or specify minimum repetitions / CI bound.

6. **Pending-name resolution timing is not in the dependency-scope list.** Ingest records pending name references when a name isn't yet resolved. FP4.A summaries computed before late-binding resolution will under-classify; this is a real correctness corner that §6.3 / §8.3's named-range bullets do not address explicitly.

7. **Volatile sources without a scalar precedent.** The reverse-dependent invariant talks about precedent cells/regions changing. Volatile recalculation triggers (`NOW()`, `RAND()`) have no precedent cell. The contract should say that volatility composes with the invariant by adding a synthetic "volatility tick" precedent that all volatile-flagged runs depend on, or fall back.

8. **`SharedTemplateIr` is renamed but the placement reifier is unspecified.** §12 calls out the rename and explicitly disclaims scalar `ASTNode` reuse, but the actual contract is left to "any use of the current scalar interpreter still needs correct per-placement reference materialization." That's enough to refuse misuse but not enough to design FP6 against; flag it as an explicit FP6 open question rather than a settled term.

9. **Open-axis ranges and used-region growth.** §8.3 lists `OpenStart`/`OpenEnd`/`WholeAxis` and says open/whole ranges are likely fallback or bounded conservative summaries first. The reverse invariant for `WholeAxis` is unbounded by definition — every cell on that axis must dirty the run. Recommend the contract say that `WholeAxis` and `OpenEnd` must fall back in FP4.A until a used-region contract with growth tracking exists.

10. **Phase-naming reconciliation.** §5 acknowledges the rephase-plan vs runtime-contract mismatch and proposes a renaming for the next `REPHASE_PLAN.md` update, but `REPHASE_PLAN.md` still has FP4 = loader bridge / FP5 = dependency summaries. Until that doc is updated, anyone reading just one file gets the wrong sequence. The dispatch report `fp3-store-materialization-report.md` already references FP4/FP5 under the old naming.

## Specific suggested edits to `FORMULA_PLANE_RUNTIME_CONTRACT.md`

- §6.3, after the reverse invariant, add: *"The reverse summary representation must be bounded sub-linearly in scalar edges; if a policy can only answer the reverse query by enumerating scalar dependents, it is not eligible for `SummaryEdgesSidecar`."*
- §6.3, add bullet: *"Volatile recalculation sources participate in the reverse invariant via a synthetic 'volatility tick' precedent; runs that depend on volatility but cannot represent the tick precedent fall back."*
- §8.1, fix the canonicalization-input ordering: *"Canonicalization operates on the AST as it enters dependency planning. Where ingest performs structural rewrites (e.g., this-row structured references), canonicalization must occur consistently on one side of the rewrite for all family members."*
- §8.3, change `open/whole-row/whole-column range` initial policy from *"likely fallback or bounded conservative summary first"* to *"FP4.A: explicit fallback. Bounded conservative summary requires used-region growth tracking (post-FP4.A)."*
- §8.2, define `DependencyCollectPolicyFingerprint` to include at minimum `expand_small_ranges`, `range_expansion_limit`, `include_names`, and a sheet-registry epoch — not just the type name.
- §9.3, add an explicit row for late-bound name reference / pending ingest resolution with policy *"explicit fallback / re-classify on resolution."*
- §12, on `SharedTemplateIr`, append: *"The placement reifier contract is an FP6 open question; FP4.A must not assume any concrete reifier."*
- §13.1, replace the 100-formula row's `≤5%` with `≤5% above noise floor or noise-floor bounded, whichever is larger`, and require minimum repetition counts in the benchmark report.
- §13.2, add: *"FP4.A may run a separate canonicalization pass strictly on scanner / passive paths; production ingest fusion is FP4.C / FP5 scope and must not regress until proven."*
- §15, bullet 7, expand: *"Fallback reasons are explicit, counted, and orthogonal to volatility flags. Oracle disagreement during FP4.A is diagnostic only and does not trigger any runtime action."*
- §16, step 5, name the oracle: *"`DependencyGraph::plan_dependencies` output under a fixed `CollectPolicy`, expanded to a comparable cell/range set."* Otherwise the harness is unspecified.
- §18, add open questions: reverse-summary representation cost; volatility-tick representation; late-bound name resolution timing vs canonicalization; `TemplateSupportStatus` rationalization.

## Recommended FP4.A implementation slice

Tighter than §16, sequenced for short feedback loops:

1. **Canonicalization module (`formula_plane/template_canonical.rs`)** — exact normalized AST + label set, internal-only. Decide and document in the module-level doc which side of `rewrite_structured_references_for_cell` it operates on. Tests: relative-only copy, mixed anchor, absolute literal, sheet binding stability under rename.
2. **One precedent-pattern surface** — affine cell/rect with `AxisRef`, plus an explicit reject set covering open/whole-axis, 3D, external, table-this-row, name-expression, dynamic, reference-returning, unknown function. Reject-only is fine for the first slice.
3. **Single class first: `StaticPointwise`.** Pure arithmetic/comparison binary/unary operators over finite cell refs. Defer `CriteriaAggregation`, `MaskConditional`, `LookupStaticRange`, `IntraRunRecurrence` to follow-up FP4.A increments. This is small enough that the oracle harness can be exact, not "where feasible."
4. **Run-instantiated summaries on top of existing `FormulaRunStore`.** Holes / exceptions / rejected / singletons explicitly excluded from inheritance, per §8.4.
5. **Comparison harness with named oracle.** Compare against `DependencyGraph::plan_dependencies` under a single fixed `CollectPolicy`, expanded to a comparable cell set. Report `exact_match | over_approximation_count | under_approximation_count | rejection_count | policy_drift`. Any `under_approximation_count > 0` is a hard test failure. Over-approximation is informational.
6. **Reverse-summary feasibility probe (paper exercise, not code).** Pick three FP3 corpus shapes and write down the proposed reverse-summary representation cost. If it is not sub-linear in scalar edges, flag for FP5 redesign before any sidecar is built.
7. **Scanner JSON `dependency_summaries` section.** No graph mutation, no ingest path change.

Explicitly defer until after this slice: any `FormulaPlaneDependencyContract` trait surface (start with a free function over `&ASTNode` + `FnCaps` + `ArgSchema`), all class taxonomy beyond `StaticPointwise`, all sidecar work, all ingest fusion experiments.

## Notes on the runtime-win disclaimer

The contract maintains the no-runtime-wins posture throughout, including in §13.1 ("initial design targets, not measured claims"), §15 ("oracle comparison ... before claiming runtime wins"), and §16 step 5 ("Do not claim runtime wins"). The FP3 report's `compact_representation_ratio` and avoidable-count tables are correctly framed as opportunity estimates, not deltas. Nothing in this re-review is taken as a claim that the contract or this slice will produce a measurable improvement.
