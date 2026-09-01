# FormulaPlane Coverage Ledger

**Status:** Standing architecture ledger
**Last audited commit:** `a238d067`
**Last audited:** 2026-09-01

---

## What this document is

The plane-authoritative cutover requires two properties to hold simultaneously:

1. **Agreement** — everything the FormulaPlane accepts evaluates byte-identically to the legacy per-cell graph, including error kinds.
2. **Intentional rejection** — everything the plane refuses is refused as *documented policy*, not by accident or omission.

This ledger is the standing record of both. It is **updated per merge**: any PR that touches a plane eligibility site, a `FunctionResultSemantics` / `FnCaps` registration, a read-projection rule, or `span_eval`'s write/egress path must add or amend a row here, and state which of the four statuses applies.

### Status vocabulary

| Status | Meaning |
|---|---|
| `accepted` | Plane-eligible; span-evaluated; parity verified against legacy. |
| `rejected-by-policy` | Deliberately kept out of spans for a stated correctness or scoping reason. |
| `rejected-accidental` | Kept out by an omission, asymmetry, or unimplemented path — not by a decision. Carries a work item. |
| `divergent` | Surfaces disagree, or can disagree. **Cutover blocker.** |
| `pending-merge` | Class is not yet in the engine; eligibility constraints pre-registered ahead of the merge that introduces it. |

### Verdict vocabulary (for merges, not classes)

- **PLANE-ALIGNED** — the change works inside spans, or lives in a layer both surfaces share.
- **PLANE-NEUTRAL** — the change cannot cause divergence by construction. State *which* mechanism: rejection, or the shared `Interpreter`.
- **PLANE-DIVERGENT** — the surfaces disagree or can. Cutover debt.

> **Standing caution.** "PLANE-NEUTRAL because the plane rejects function F" is a *perishable* argument — an admission merge can invalidate it silently (this happened: see `IF / IFS / CHOOSE`). "PLANE-NEUTRAL because `span_eval` constructs a stock `Interpreter` and has no private dispatch" is durable. Prefer the durable form, and re-verify the perishable form at every audit.

---

## A. Formula and reference classes

| Class | Status | Coverage measured | Rationale / work item |
|---|---|---|---|
| Row-shifted arithmetic (`=B{r}*C{r}+A{r}`) | `accepted` | 100% | Baseline span class. Control in every differential. |
| Anchored/constant-result aggregates (`=SUM($B$2:$B$N)`) | `accepted` | 100% | Constant-result spans: evaluated once, broadcast. Exempt from the 100-cell promotion floor. |
| Mixed-anchor running totals / tail reads (`=SUM($B$2:$B{r})`) | `accepted` | 100% | Affine per bound; read projection inverts as half-open placement intervals. |
| Cross-sheet relative reads (`=Data!B{r}*2`) | `accepted` | 100% | Explicit sheet binding supported when the sheet resolves. |
| Defined names in dragged formulas | `accepted` | 100% | Names are a *flag* (`CanonicalTemplateFlag::NamedReference`), not a reject; resolved to absolute read regions per placement at ingest. Lifecycle invalidation covered by 19 tests. See §B. |
| Whole-**column** ranges (`A:A`, `D:F`) | `accepted` | 100% | `DirtyProjectionRule::WholeColumnRange`. |
| Whole-**row** ranges (`3:13`, `1:1`) | **`rejected-accidental`** | 0% | Canonicalizes clean, then hard-fails at `ingest_pipeline.rs:1063`. No `WholeRowRange` projection rule exists. Asymmetric with whole-column for no stated reason; fallback label misattributes the cause. **→ WI-1** |
| Scalar lookups (`VLOOKUP`, `MATCH`) | `accepted` | 100% | Scalar result, no `MAY_SPILL`. |
| `IF` / `IFS` / `CHOOSE` with **cell/scalar arms** | `accepted` | 100% | Admitted by `feat/refreturn-span-admission` (`c4816e51`). `RETURNS_REFERENCE` + `MAY_SPILL` rejects revoked when all args are `safe` and every result arm is `scalar`. |
| `IF` / `CHOOSE` with **array condition/index** (`=IF($B$2:$B$4>0,C2,D2)`) | `accepted` | 100% | Admitted: arg 0 need only be `safe`, not `scalar`, and `MAY_SPILL` is revoked by function **name**. Today parity holds because the interpreter returns `#VALUE!` rather than broadcasting; since `#388` that is no longer load-bearing — if these functions gain dynamic-array broadcast, the span sink fails closed and demotes rather than publishing the top-left value. See §C "Array results inside spans". |
| `IF` with a **range arm** (`=SUM(IF(c,$C$2:$C$4,$D$2:$D$4))`) | `rejected-by-policy` | 0% | Arms must be `scalar`. Range-arm firewall holds; verified. |
| `IF` wrapping a **non-admitted** reference-returning callee | `rejected-by-policy` | 0% | Admission merges with `&=` across occurrences — fail-closed. |
| `INDEX` | `rejected-by-policy` | 0% | `RETURNS_REFERENCE`. Not in the admitted set. |
| `OFFSET`, `INDIRECT` | `rejected-by-policy` | 0% | `DYNAMIC_DEPENDENCY` + `VOLATILE` (+ context dependence for `INDIRECT`). |
| `XLOOKUP` / `XMATCH` | `rejected-by-policy` | 0% | `FnCaps::MAY_SPILL` → `array_or_spill`. Lookup family splits along the `MAY_SPILL` line. |
| Dynamic-array producers (`SORT`, `UNIQUE`, `FILTER`, `SEQUENCE`) | `rejected-by-policy` | 0% | `MAY_SPILL`. |
| Array literals (`{1,2;3,4}`, `=SUM({1,2,3})`) | `rejected-by-policy` | 0% | Template fingerprint does not model array-shaped literal slots. Perf scoping; expandable. |
| Volatile functions (`RAND`, `TODAY`, `NOW`) | `rejected-by-policy` | 0% | `FnCaps::VOLATILE` + parser volatile flag. |
| Local-environment functions (`LET`, `LAMBDA`) | `rejected-by-policy` | 0% | `LOCAL_ENVIRONMENT` / `contract.environment != None`. |
| Spill refs (`A1#`) and implicit intersection (`@A1:A9`) | `rejected-by-policy` | 0% | Dynamic extents. |
| Structured/table refs (`Table1[Amount]`, `Table1[@Amount]`) | `rejected-by-policy` | 0% | Table extents mutate independently of the template. |
| 3-D references (`Sheet1:Sheet3!A1`) | `rejected-by-policy` | 0% | Sheet-set enumeration not modeled by read projections. |
| **External references** (`[1]Sheet1!A1`) | `rejected-by-policy` | 0% | `template_canonical.rs:937`, unconditional, on the path every ingest route traverses. Tracked as issue **#378**; documented policy, no work item. |
| Unregistered / custom functions | `rejected-by-policy` | 0% | `UnknownOrCustomFunction` — fail-closed default. |
| `CELL` | `rejected-by-policy` | 0% | Registers `FunctionContextDependence::WorkbookMetadata`, so canonicalization rejects it as `ContextDependentFunction` before span admission. This is the constraint #329 had to satisfy: registering `None` would have span-admitted it and every member of a span would return the *anchor* cell's answer. Pinned by `cell_is_not_plane_eligible_via_context_dependence_contract` (template_canonical.rs). |
| `HYPERLINK` | `accepted` | 100% | Value-channel only: `eval` ignores `ctx` and returns a scalar derived solely from its arguments, so there is no side channel that could bypass the span result path. Span admission is therefore safe by construction, not by a checked invariant that could later drift. |

## B. Structural / placement classes

| Class | Status | Rationale |
|---|---|---|
| Families `< 100` non-constant members | `rejected-by-policy` | `MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS` (placement.rs:45). Perf scoping, explicit. |
| Singleton families | `rejected-by-policy` | `SingletonUnique`. |
| Self/internal dependency (read ∩ own result region) | `rejected-by-policy` | `InternalDependency`. Documented: avoids O(N²) edit recalc on chains. |
| Statically-cyclic SCC members | `rejected-by-policy` | `CycleMember`. Cycle stamping would race span writes (refs #112). |
| Non-rectangular / gapped domains | `rejected-by-policy` | `UnsupportedShapeOrGaps`. |
| Cross-sheet families | `rejected-by-policy` | `CrossSheetOrSheetMismatch`. |
| Structurally non-identical members | `rejected-by-policy` | `NonEquivalentTemplate` — definition of a span. |
| Literal bindings over the byte cap | `rejected-by-policy` | `BindingMemoryCapExceeded`. Resource scoping. |
| Unresolvable sheet binding | `rejected-by-policy` | `UnknownSheetBinding` — preserves legacy `#REF!`. |
| Undefined / Literal / Formula-backed names | `rejected-by-policy` | `UnsupportedNamedReference` — preserves `#NAME?` and named-formula semantics. |
| Structural shifts straddling a span | `rejected-by-policy` | `SpanShiftPlan::Demote` family (`structural_shift.rs`) — demote to the per-cell adjuster. |
| Mid-domain row/column **insert** straddling a span | `accepted` | `SpanShiftPlan::Split` + `plan_span_split` — an untouched upper half and a shifted lower half, each re-classified in its own frame; not-provably-clean splits demote. |
| Row/column **delete** straddling a span's result domain | `accepted` (guarded) | Compaction keeps one span only when the surviving domain is expressible in one relative-offset frame: `delete_compaction_frame_is_sound` requires `d_r == d_p - d_o` (read / placement / origin displacement) for every relative read bound of every surviving placement, and refuses a read bound landing inside the deleted band. Otherwise the span splits at the band like an insert; if the split is not provably clean — or only one side of the band survives and its frame does not hold — it demotes. Before this rule compaction copied the projection verbatim while moving the origin, so a span reading *above* a mid-domain delete served the pre-delete formula on every placement below the band (#171). |

## C. Cross-cutting mechanisms

| Mechanism | Status | Notes |
|---|---|---|
| Format write channel | `accepted` | Span formats ride `ComputedWriteBuffer` exclusively. Each first successful non-empty authoritative span-buffer commit for a plane epoch prepares synchronization without mutation, passes the resource and commit-window preflights, then atomically purges then-existing plane-owned stale `derived_formats` immediately before applying buffered values and formats. Cancellation or typed preflight failure preserves both stores. Chunk effects explicitly distinguish no work, exact stale clear, and actual set. This is a transactionally committed point-in-time cleanup, not a permanent prohibition: later same-epoch legacy work may write consistent side-band entries, and effective read precedence preserves correctness; overlay punchouts remain legacy-owned. |
| Effective-format read cascade and temporal egress | `accepted` | User-overlay format wins, then a genuine non-General source `FormatRuns` entry, then computed-overlay format. A General source entry denotes no source format and falls through to the computed lane. The single `get_cell_value → effective_format_id → materialize_temporal_egress` funnel is keyed on coordinates and evaluator-agnostic; an engine-direct Arrow fixture pins Off/Authoritative effective-format and temporal-egress parity when the output chunk carries a source format lane. |
| Format preservation across constant-result broadcast | `accepted` | Fixed by `0ef92be3`; verified (`=$J$1+0` × 200 placements → `Date:200` both modes). |
| Name lifecycle invalidation reaching span members | `accepted` | define / update / delete / redefine / sheet-scope shadowing / undo-redo. 19 tests. `81560db7` closed the redefinition hole; `a84069db` fixed mixed-pass symbol scheduling. |
| Plane-mode gating of authority | `accepted` | Before `bc0c5bb0`, four entry points dispatched to the plane on `active_span_count() > 0` with no mode check. `b0c60080`/`bf2b1f8b` completed the fix: a live authoritative→Off transition transactionally materializes retained spans into legacy before any valid evaluation request, with pre-commit resource checks and infallible post-commit dirty acknowledgement. Any parity probe must assert `accepted_span_cells == 0` under `Off`. |
| Array results inside spans | `rejected-by-policy` *(guarded sink)* | Guarded by `#388`. `literal_to_overlay` no longer collapses to `rows[0][0]`: any `LiteralValue::Array` (including 1x1, which legacy still routes through the spill planner at eval.rs:26602) returns the typed `SpanEvalError::ArrayResultRequiresSpill`. The coordinator aborts the layer with its `ComputedWriteBuffer` unflushed — nothing is published — then transactionally demotes the offending span via `prepare`/`commit_prepared_formula_span_demotion` and replans, so the placements re-evaluate on the legacy path with correct spill semantics. Counted by `formula_plane_array_result_span_demotions` and the `ArrayResult` placement-fallback reason. Pinned by `formula_plane_array_result_backstop.rs` (test-only function with declared-scalar semantics returning a 1x2 array): authoritative/Off agree on every cell and column C spills instead of staying empty. |
| Anchor-side capability gate | **`rejected-accidental`** | placement.rs:886 still hard-rejects `may_return_reference()` / `may_spill()`, inconsistent with the canonicalization layer's admitted set post-`c4816e51`. **→ WI-3** |
| Dependency-summary parity oracle | `rejected-accidental` (audit-only) | Whole-axis and open-rect dependencies are skipped by the comparison (dependency_summary.rs:253-259, :311-320). The widest-read-region class the plane *accepts* is never parity-checked. No user-visible effect. **→ WI-4** |
| `#371` `OpenRect` arm | `rejected-by-policy` (inert) | Constructed at dependency_summary.rs:435-447, consumed only at :317-319 to emit a telemetry label. **No effect on eligibility, placement, or evaluation.** |

## D. Dead reject variants

Enumerated in `CanonicalRejectKind` with diagnostics labels, but **never constructed**. Keep or remove deliberately; do not treat their existence as evidence of a policy.

| Variant | Note |
|---|---|
| `WholeAxisReference` | Whole-axis is deliberately *supported*; pinned by `formula_plane_whole_axis_range_is_authority_supported`. |
| `UnsupportedReference` | No construction site. |
| `OpenRangeReference` | Constructed (template_canonical.rs:1052) but appears **unreachable from A1-parsed formulas** — fires only when exactly one bound of an axis is `None`, which A1 notation cannot express. Defensive guard. |

---

## E. Merge verdict log

| Issue / PR | Merge | Verdict | Basis |
|---|---|---|---|
| #357 format channel T1 | `28dcfaaf` | **PLANE-ALIGNED** | Shared format write funnel + single temporal egress funnel. |
| #364 format boundary | `07304121` | **PLANE-ALIGNED** | Same funnels; verified 600 cells `Date`, 0 divergent. |
| #360 named-range tracking | `82a0b0d8` | **PLANE-ALIGNED** | Names eligible; invalidation reaches span members (19 tests). |
| #369 / #370 / #372 | `ce93ff9e` / `b5bae5a4` / `85074efd` | **PLANE-NEUTRAL** | ⚠ **Basis corrected.** Neutral because `span_eval` builds a stock `Interpreter` with no private dispatch — **not** because the plane rejects IF/IFS/CHOOSE, which it no longer does. |
| #371 OpenRect axis bounds | `06dad200` | **PLANE-NEUTRAL** | `OpenRect` arm is diagnostics-only; no eligibility effect. Confirmed. |
| #340 XLOOKUP class rules | `35cd2376` | **PLANE-NEUTRAL** | XLOOKUP/XMATCH plane-rejected via `MAY_SPILL`. |
| #377 review asks / `precise_dispatch` | `60c0afad` | **PLANE-NEUTRAL** | No `formula_plane/` file touched; the lone eval.rs change is a cosmetic formatter guard. `CHOOSE` changes are span-reachable but neutral by shared interpreter. |
| #363 external refs → #378 | — | **PLANE-NEUTRAL** | `rejected-by-policy`; site verified template_canonical.rs:937. |
| #329 CELL / HYPERLINK | PR #329 | **PLANE-ALIGNED** | `CELL` registers `FunctionContextDependence::WorkbookMetadata` and is rejected at canonicalization as `ContextDependentFunction`, pinned by `cell_is_not_plane_eligible_via_context_dependence_contract`. `HYPERLINK` is value-channel only — `eval` ignores `ctx` and returns a scalar — so no side channel exists and span admission is safe by construction. |
| `fix/fp-format-broadcast-parity` | `0ef92be3` | **PLANE-ALIGNED** | Preserves formats across span broadcasts. |
| `fix/fp-gate-mode-conjuncts` | `bc0c5bb0` | **PLANE-ALIGNED (bug fix)** | `FormulaPlaneMode::Off` now actually disables plane authority. |
| `fix/name-redefine-healing` | `81560db7` | **PLANE-ALIGNED** | Redefined names heal dependents. |
| `feat/format-lane-writeback` | `1c2e4822` | **PLANE-ALIGNED** | Plane computed-writes carry `FormatId`. |
| `feat/refreturn-span-admission` | `c4816e51` | **PLANE-ALIGNED (expansion)** | Admits IF/IFS/CHOOSE scalar-arm spans. Invalidates the #372 rejection premise; introduces the array-condition coupling (WI-2) and the anchor-gate inconsistency (WI-3). |
| `fix/fp-mixed-schedule-symbol-drop` | `a84069db` | **PLANE-ALIGNED** | Named symbols scheduled in the mixed pass, fail-closed preflight. |
| Off-mode retained-span transition completion | `b0c60080` / `bf2b1f8b` | **PLANE-ALIGNED (bug fix)** | Off never dispatches FormulaPlane; retained spans are transactionally demoted before valid evaluation requests. Pre-commit failures preserve exact authority/dirty state; no recoverable branch follows commit. |
| `fix/388-fp-array-result-backstop` | next merge | **PLANE-ALIGNED (bug fix)** | Array span results fail closed: typed `SpanEvalError::ArrayResultRequiresSpill` before any publication, then transactional span demotion and replan onto the legacy spill path. Closes WI-2. |
| FormulaPlane format-write fast path | next merge | **PLANE-ALIGNED (bug fix)** | Span values, computed formats, and the epoch-scoped stale-sideband purge share the post-preflight authoritative commit seam. General source runs fall through to computed formats while non-General source and user-overlay precedence remain intact. Source-lane parity, commit-window failure, deterministic mid-span cancellation, DATE→General, Point, sparse, mixed-chunk, and virgin-100k regressions are pinned. |

---

## F. Open work items

| ID | Title | Severity |
|---|---|---|
| WI-1 | Whole-row ranges demoted by a missing projection rule, not policy | Medium — real coverage loss |
| WI-2 | Span evaluation silently collapses array results to top-left with no backstop | **Closed** — `#388`: the sink returns `SpanEvalError::ArrayResultRequiresSpill` before any publication and the coordinator transactionally demotes the span onto the legacy spill path; pinned by `formula_plane_array_result_backstop.rs` |
| WI-3 | Anchor-side capability gate not updated for the reference-returning admission | Low — consistency |
| WI-4 | Dependency-summary parity oracle skips whole-axis / open-rect dependencies | Low — audit completeness |
| WI-6 | Delete-compaction ignored the placement/read/origin displacement identity; `legacy_island_structural_summaries_trusted` is pinned `false` after every axis op as the standing mitigation | **Closed for the span path** — `delete_compaction_frame_is_sound` gates compaction and the delete split covers the two-sided case (#171). The trust flag is deliberately left `false` in that fix: re-enabling it needs a separate audit of the *legacy island* summaries, which this change does not touch. **→ WI-7** |
| WI-7 | Re-enable `legacy_island_structural_summaries_trusted` after an axis op | Low — perf, not correctness. Blocked on a legacy-island (non-span) structural summary audit; the #171 span-side blocker is cleared |
| WI-5 | Formatted legacy result admitted into a General span had no stale-clear regression pin | **Closed** — source-format-lane read parity and post-preflight purge timing are pinned alongside DATE→General, Point/sparse/mixed clears, deterministic mid-span cancellation, typed commit failure, and 100k virgin-lane counters |

Full statements in [`formula-plane-span-coverage-audit.md`](formula-plane-span-coverage-audit.md) (§6 of the `788675a8` span-coverage audit, checked in verbatim).

---

## G. Standing measurement

Per-class differential probe (engine-direct; **do not** use loader-based fixtures for format parity — `UmyaAdapter` drops the number-format channel entirely). The probe is in-tree at [`crates/formualizer-bench-core/src/bin/probe-fp-audit2.rs`](../../crates/formualizer-bench-core/src/bin/probe-fp-audit2.rs):

```bash
cargo run -p formualizer-bench-core --features formualizer_runner --release \
  --bin probe-fp-audit2 -- --rows 200
```

It writes the full per-class report as JSON on stdout and a summary table on stderr. Useful flags: `--rows N` (formula rows per class; >= 100 is required for non-constant span promotion), `--seed N`, `--only class1,class2`.

Acceptance gate: **`total_divergent_cells == 0`**, and every class's status matches its row in §A. The probe **exits non-zero** when that gate fails, so it is machine-checkable.

### Continuous enforcement

`.github/workflows/ci.yml` runs `cargo test --workspace --exclude formualizer-bench-core`, so nothing in the bench-core crate — including the parity harness — is covered by the main Rust job. The gate therefore has its own workflow, [`.github/workflows/formula-plane-parity.yml`](../../.github/workflows/formula-plane-parity.yml) (`formula-plane-parity` job, on `pull_request` and `push` to `main`), which runs:

1. `cargo clippy -p formualizer-bench-core --features formualizer_runner --all-targets -- -D warnings`
2. `probe-fp-audit2 --rows 200` — the per-class Off↔Authoritative differential above; asserts exit 0.
3. `cargo test -p formualizer-bench-core --features formualizer_runner --test parity_harness_smoke` — the full-cell Off↔Auth parity harness (`src/parity_harness.rs`) over loader-built workbooks.
4. `probe-corpus-parity --scale small --exclude 's040-*,s041-*,s042-*'` — full-cell Off↔Auth parity across the synthetic scenario registry, 81 scenarios, ~20 s wall clock. The three exclusions fail with `HarnessError` (no public `Workbook` API for undoable row inserts, native table extension, or external-source declaration), not with divergences; re-include them when those APIs land.

Any change to a plane eligibility site, a `FunctionResultSemantics` / `FnCaps` registration, a read-projection rule, or `span_eval`'s write/egress path must keep this job green.

### Last measured

At `a238d067` (this branch), `probe-fp-audit2 --rows 200 --seed 42`: **19 classes, 3 800 cells compared, 3 800 identical, 0 near-miss, 0 divergent** — first eval and after-edit, exit 0. Span residency: **13 of 19 classes at 100 %**, 6 at 0 % (5 rejected by documented policy, 1 — `open_range_rows` — rejected accidentally, WI-1).

`probe-corpus-parity --scale small` at the same commit: **81 scenarios run, 81 passed, 2 skipped (expected divergence), 0 total divergences.**

Historical: the `788675a8` audit measured the identical 19 / 3 800 / 0 figures, alongside `cargo test -p formualizer-eval --lib formula_plane` → **666 passed, 0 failed**. The Off-transition completion through `bf2b1f8b` was separately adversarially verified and passed workspace 3,714/0 plus Python 138/0.
