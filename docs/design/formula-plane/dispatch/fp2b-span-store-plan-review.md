# FP2.B passive span-store plan — independent spec review

Date: 2026-04-30  
Branch: `formula-plane/bridge`  
Spec under review: `docs/design/formula-plane/dispatch/fp2b-span-store-plan.md`  
Spec commit: `0d0af17` (`docs(formula-plane): plan fp2b passive span store`)

## Verdict

**PASS-WITH-NITS** — the plan is ready to implement after a small set of clarifications. The nits are about pinning down a few ambiguous edges so tests are deterministic and the FP2.A reconciliation is not accidentally broken. None of them block the start of implementation, but they should be answered in the implementation PR or in a one-line spec amendment.

## Summary judgment per criterion

| # | Criterion | Result | Notes |
|---|---|---|---|
| 1 | Scope discipline | OK | Non-goals are explicit and enumerated; no scheduler/eval/dirty/public-API surface added. |
| 2 | Module placement | OK | New module is `crates/formualizer-eval/src/formula_plane/span_store.rs`, re-exported only inside `formualizer-eval`. No drift to `formualizer-common`. |
| 3 | Data structures | OK with one nit | Coverage is sufficient. `FormulaPlacementId` vs `FormulaRunId` is left optional; recommend committing to one model for FP2.B. |
| 4 | Deterministic ID policy | OK with one nit | Sort-then-assign is correct; multi-sheet determinism is asserted in the policy but not exercised in the test matrix. |
| 5 | Shape policy | OK with one nit | Row/column/singleton/hole/exception definitions are precise per-axis; need explicit cross-axis de-duplication rule for holes/exceptions. |
| 6 | Rectangle policy | OK | Deferred to a counter / `RectangleDeferred` classification with a clear rationale (vertical-fill-dominated corpus, double-count risk, FP3/FP5 dependency). |
| 7 | Unsupported/dynamic/volatile handling | OK | Reason precedence is parse-error → unsupported → dynamic → volatile. Rejected cells excluded from runs but retained as exceptions inside other-template spans. |
| 8 | FP2.A reconciliation | OK with one nit | Reconciliation struct is good; the FP2.A axis double-counting of holes/exceptions is not explicitly named as an expected delta. |
| 9 | Test matrix | OK with one nit | 14 cases cover the shapes, IDs, rejection precedence, overlap, rectangles, and reconciliation. Multi-sheet and empty-input coverage is missing. |
| 10 | Acceptance criteria | OK | Measurable: file location, shuffled-input determinism, exact coordinates, FP2.A reconciliation, no production path changes. |
| 11 | Future-phase mapping | OK | Section 11/12 are honest: FP2.B/FP3 are representational, real wins are FP4–FP7. |
| 12 | Risks / circuit breakers | OK | Overlap precedence, rectangle eagerness, rejection handling, FP2.A drift, gap-scan cost are all named with concrete breakers (cap + truncated counter, sort-before-assign, passive-only path). |

## Nits to clarify before or during implementation

The following are small, non-blocking. They can land in the implementation PR.

- [ ] **Hole/exception cross-axis de-duplication.** The plan defines holes/exceptions as gaps inside the min/max span of a same-template *axis group*. A single missing coordinate `(sheet, row, col)` can be detected from both the row axis and the column axis simultaneously. State explicitly that `gaps` is deduped by `(template_id, sheet, row, col, kind)` so each missing/foreign coordinate appears at most once. Without this, `hole_count` and `exception_count` will silently double in cross-shaped templates.
- [ ] **FP2.A reconciliation: hole/exception axis delta.** Today `span_counters.rs` adds `row_holes + column_holes` and `row_exceptions + column_exceptions` (`crates/formualizer-eval/src/formula_plane/span_counters.rs`, `count_axis_gaps` callers). After cross-axis de-duplication FP2.B will, by design, undercount FP2.A in cross-shaped families. Add this as a named, allow-listed delta in `Fp2aReconciliation` reasons (e.g. `"fp2a counts gaps per-axis; fp2b stores per-coordinate"`).
- [ ] **Overlap precedence tiebreaker direction.** Section 5 says “prefer the longer run; break ties by shape order; then by sorted run key.” Pin the direction: which of `Row` / `Column` wins on equal-length tie. The shape order in section 4 is `Row, Column, Singleton, RectangleDeferred`; recommend stating in section 5 that the smaller shape-order index wins so it matches the ID-assignment order.
- [ ] **`FormulaPlacementId` decision.** Section 3 says placement IDs may be omitted if 1:1 with runs. Either commit to “FP2.B has runs only; `FormulaPlacementId` is reserved for FP3” or commit to “placements and runs are both stored from day one.” Carrying both with 1:1 mapping crosses the “not overbuilt” line; carrying neither today is fine. Pick one before implementation so the test matrix is unambiguous.
- [ ] **`TemplateSupportStatus::Mixed`.** Section 3 lists `Mixed` but section 6 only specifies per-cell rejection precedence. Specify when a template earns `Mixed` (e.g. some cells supported and some rejected with any non-`Supported` reason). Otherwise this status is unreachable or arbitrary.
- [ ] **Rectangle test expectation.** The `rectangle_deferred` row in the test matrix accepts either a rectangle-deferred candidate count or a deterministic row/column decomposition. Pick one for FP2.B so the test is a single fixed assertion rather than a two-branch one. Recommendation: keep the deterministic row/column decomposition and increment `rectangle_deferred_count` only; do not emit `RectangleDeferred` placements in FP2.B.
- [ ] **Multi-sheet determinism test.** Criterion 4 demands robustness across multi-sheet input but the test matrix never crosses a sheet boundary. Add a `multi_sheet_determinism` test with at least two sheets sharing one template, asserting stable IDs and stable `(sheet, row, col)` ordering for placements, runs, gaps, and rejected cells under shuffled input.
- [ ] **Empty / minimal input.** Add a one-line `empty_input` test (no cells in, empty store out, `matched = true`, all counters zero) and a `single_unsupported_template_only` case where the arena still contains the rejected-only template (per section 4, “Include rejected-only templates in the arena”).
- [ ] **Gap-scan cap value.** Section 13 introduces `gap_scan_truncated_count` with a builder option/hard cap but does not propose a default. Pick a default (e.g. `gap_scan_max_per_axis_group: 1_000_000`) so behavior is reproducible across runs without callers having to opt in.
- [ ] **`source_template_id` ordering note.** Lexicographic sort of parser template strings is correct as a deterministic key, but if the parser ever changes its template-id formatting, run/placement IDs shift even though semantics are unchanged. Worth a one-line note in section 4 that template-id stability is owned upstream by the parser, and that the FP2.B arena merely records whatever string identity the parser exposes.

## Strengths worth preserving

- The non-goals list (section 1) is unusually concrete. It explicitly names Core+Overlay closeout, scheduler authority, dependency graph mutation, loader changes, public API/serialization, CLI, and `formualizer-common` movement. This is exactly the boundary FP2 needs.
- `FormulaRunStoreBuildReport` + `Fp2aReconciliation` with named per-field deltas is the right shape for a passive-store phase. It makes drift from FP2.A debuggable instead of invisible.
- The shape contract enforces minimum-length-2 row/column runs and routes everything else through `Singleton` or `rejected_cells`. This avoids the FP2.A trap of representing the same supported cell on both axes.
- Rectangle deferral is justified by both corpus shape (vertical fill-down dominance per `fp2-span-partition-counters-report.md`) and dependency-summary readiness, not by laziness.
- Circuit breakers tie back to specific failure modes: axis overlap → precedence + dedup count, rectangle eagerness → defer, rejection regression → separate `rejected_cells`, FP2.A drift → reconciliation allow-list, gap cost → cap + truncated counter, build failure → fall back to FP2.A counters.
- Future-phase mapping (sections 11–12) is unusually candid that no user-visible wins arrive in FP2.B. This protects the phase from accidental scope creep into eval/scheduler.

## Implementation can proceed

Yes. The nits above can be resolved in the implementation PR or as a single follow-up amendment to `fp2b-span-store-plan.md`. None of them require a redesign or a new phase boundary.
