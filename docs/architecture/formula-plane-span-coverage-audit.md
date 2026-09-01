# FormulaPlane Span-Coverage Audit — Open Work Item Statements

**Provenance:** extracted verbatim from §6 ("Proposed GitHub issues") of the read-only FormulaPlane span-coverage audit performed at `788675a8` on 2026-08-29. The full audit report was written outside the repository; this file exists so that the open-work-item references in [`formula-plane-coverage-ledger.md`](formula-plane-coverage-ledger.md) §F resolve in-tree.

**Status:** these are statements of record, not a live tracker. The ledger §F table is authoritative for status; amend it, not this file, when a work item closes.

---

### WI-1 — `FormulaPlane: whole-row ranges are demoted by a missing projection rule, not by policy`

Whole-column ranges (`A:A`, `D:F`) canonicalize clean and reach 100% span coverage, but whole-row ranges (`3:13`, `1:1`) canonicalize equally clean and then hard-fail at `ingest_pipeline.rs:1063` with `ProjectionFallbackReason::UnsupportedDependencySummary`, because `DirtyProjectionRule` (producer.rs:623) has a `WholeColumnRange` variant and no `WholeRowRange` counterpart. The rejection is accidental rather than policy: the canonicalization layer accepts the reference (no `CanonicalRejectKind`), the two axes are treated asymmetrically with no stated rationale, and the emitted fallback label misattributes the cause to the dependency summary. Add `DirtyProjectionRule::WholeRowRange` mirroring the whole-column arm, or — if row-band spans are intentionally out of scope — introduce an explicit reason so the ledger can record it as policy.

### WI-2 — `FormulaPlane: span evaluation silently collapses array results to top-left with no backstop`

`literal_to_overlay` (span_eval.rs:1187) maps `LiteralValue::Array` to `rows[0][0]`, whereas the legacy path routes array results into the spill planner (eval.rs:26602). Today no admitted template can produce an array — but the reference-returning admission (template_canonical.rs:501) revokes the `MAY_SPILL` reject for IF/IFS/CHOOSE by function name while only requiring the *arms* to be scalar, so `=IF($B$2:$B$4>50,C2,D2)` is span-admitted with its spill guard removed; parity holds solely because the interpreter returns `#VALUE!` for array conditions rather than broadcasting. Add a fail-closed guard — if a span evaluation yields `LiteralValue::Array`, demote the span (or debug-assert) instead of collapsing — so that adding dynamic-array broadcast to IF/CHOOSE cannot silently produce workbook-wide wrong values.

### WI-3 — `FormulaPlane: anchor-side capability gate not updated for the reference-returning admission`

`c4816e51` taught the canonicalization layer to admit IF/IFS/CHOOSE with cell/scalar arms by revoking their `RETURNS_REFERENCE` and `MAY_SPILL` rejects, but the independent anchor-syntax capability gate at placement.rs:886 still hard-rejects any function with `contract.result.may_return_reference()` or `may_spill()`, returning `AnchorFunctionSemanticsUnsupported`. The two gates now encode different admission policies for the same function set, which is exactly the drift the capability-flag design was meant to prevent. Reconcile them — either route the anchor gate through the same `classify_reference_returning_admission` shape check, or document why the anchor path is deliberately stricter.

### WI-4 — `FormulaPlane: dependency-summary parity oracle skips whole-axis and open-rect dependencies`

`NormalizedDependencyUniverse::fallback_reasons()` (dependency_summary.rs:311-320) returns `"planner_whole_axis_dependency"` / `"planner_open_range_dependency"` for whole-row, whole-column and open-rect dependencies, and the comparison driver at :253-259 treats a non-empty reason list as a skip (`rejection_count += 1; continue`). Whole-column spans are accepted at 100% coverage, so the formula class with the widest read region is never checked by the summary-vs-planner parity oracle. Extend `instantiate_summary_universe` / `symbolic_universe_covers` to compare whole-axis universes so the oracle covers the classes the plane actually accepts. Low priority — telemetry completeness, no user-visible effect.
