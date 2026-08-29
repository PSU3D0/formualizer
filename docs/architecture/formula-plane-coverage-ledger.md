# FormulaPlane Coverage Ledger

**Status:** Standing architecture ledger
**Last audited commit:** `bf2b1f8b`
**Last audited:** 2026-08-29

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
| `IF` / `CHOOSE` with **array condition/index** (`=IF($B$2:$B$4>0,C2,D2)`) | `accepted` *(latent coupling)* | 100% | Admitted: arg 0 need only be `safe`, not `scalar`, and `MAY_SPILL` is revoked by function **name**. Parity holds **only** because the interpreter returns `#VALUE!` rather than broadcasting. No backstop in `span_eval`. **→ WI-2** |
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
| `CELL` | `pending-merge` (#329) | n/a | Not implemented; currently rejects as `unknown_or_custom_function`. **Must register with `FunctionContextDependence::WorkbookMetadata` or `PlacementDependent`.** Registering it as `None` makes it span-admitted, and every member of a span returns the *anchor* cell's answer. Highest-risk registration decision in #329. |
| `HYPERLINK` | `pending-merge` (#329) | n/a | Not implemented. Natural scalar-text registration makes it span-eligible, which is fine for the value channel. Its display/link side-channel **must** ride the `record_cell_derived_format`-style funnel that `span_eval` mirrors, or `HYPERLINK` must carry `FunctionContextDependence` to force rejection. |

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

## C. Cross-cutting mechanisms

| Mechanism | Status | Notes |
|---|---|---|
| Format write channel | `accepted` | `span_eval` calls `record_cell_derived_format` on all four result paths (:328, :456, :485, :800); computed-writes carry `FormatId` to the overlay lane since `1c2e4822`. |
| Temporal egress | `accepted` | Single funnel `get_cell_value → effective_format_id → materialize_temporal_egress` (eval.rs:17314-17343), keyed on coordinates, evaluator-agnostic **by construction**. |
| Format preservation across constant-result broadcast | `accepted` | Fixed by `0ef92be3`; verified (`=$J$1+0` × 200 placements → `Date:200` both modes). |
| Name lifecycle invalidation reaching span members | `accepted` | define / update / delete / redefine / sheet-scope shadowing / undo-redo. 19 tests. `81560db7` closed the redefinition hole; `a84069db` fixed mixed-pass symbol scheduling. |
| Plane-mode gating of authority | `accepted` | Before `bc0c5bb0`, four entry points dispatched to the plane on `active_span_count() > 0` with no mode check. `b0c60080`/`bf2b1f8b` completed the fix: a live authoritative→Off transition transactionally materializes retained spans into legacy before any valid evaluation request, with pre-commit resource checks and infallible post-commit dirty acknowledgement. Any parity probe must assert `accepted_span_cells == 0` under `Off`. |
| Array results inside spans | `rejected-by-policy` *(unguarded sink)* | No admitted template currently returns an array, but `literal_to_overlay` (span_eval.rs:1187) would take `rows[0][0]` while legacy routes arrays to the spill planner (eval.rs:26602). Add a fail-closed backstop before expanding dynamic-array admission. **→ WI-2** |
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
| #329 CELL / HYPERLINK | *unmerged* | **PENDING** | Pre-registration constraints in §A. |
| `fix/fp-format-broadcast-parity` | `0ef92be3` | **PLANE-ALIGNED** | Preserves formats across span broadcasts. |
| `fix/fp-gate-mode-conjuncts` | `bc0c5bb0` | **PLANE-ALIGNED (bug fix)** | `FormulaPlaneMode::Off` now actually disables plane authority. |
| `fix/name-redefine-healing` | `81560db7` | **PLANE-ALIGNED** | Redefined names heal dependents. |
| `feat/format-lane-writeback` | `1c2e4822` | **PLANE-ALIGNED** | Plane computed-writes carry `FormatId`. |
| `feat/refreturn-span-admission` | `c4816e51` | **PLANE-ALIGNED (expansion)** | Admits IF/IFS/CHOOSE scalar-arm spans. Invalidates the #372 rejection premise; introduces the array-condition coupling (WI-2) and the anchor-gate inconsistency (WI-3). |
| `fix/fp-mixed-schedule-symbol-drop` | `a84069db` | **PLANE-ALIGNED** | Named symbols scheduled in the mixed pass, fail-closed preflight. |
| Off-mode retained-span transition completion | `b0c60080` / `bf2b1f8b` | **PLANE-ALIGNED (bug fix)** | Off never dispatches FormulaPlane; retained spans are transactionally demoted before valid evaluation requests. Pre-commit failures preserve exact authority/dirty state; no recoverable branch follows commit. |

---

## F. Open work items

| ID | Title | Severity |
|---|---|---|
| WI-1 | Whole-row ranges demoted by a missing projection rule, not policy | Medium — real coverage loss |
| WI-2 | Span evaluation silently collapses array results to top-left with no backstop | Medium — latent, high blast radius |
| WI-3 | Anchor-side capability gate not updated for the reference-returning admission | Low — consistency |
| WI-4 | Dependency-summary parity oracle skips whole-axis / open-rect dependencies | Low — audit completeness |

Full statements in `/tmp/formualizer-span-coverage-audit.md` §6.

---

## G. Standing measurement

Per-class differential probe (engine-direct; **do not** use loader-based fixtures for format parity — `UmyaAdapter` drops the number-format channel entirely):

```bash
git worktree add /tmp/fp-audit-wt <commit> --detach
cp probe-fp-audit2.rs /tmp/fp-audit-wt/crates/formualizer-bench-core/src/bin/
cd /tmp/fp-audit-wt
cargo build -p formualizer-bench-core --features formualizer_runner --release --bin probe-fp-audit2
./target/release/probe-fp-audit2 --rows 200
```

Acceptance gate: **`total_divergent_cells == 0`**, and every class's status matches its row in §A.

Last value differential at `788675a8`: **19 classes, 3 800 cells, 0 divergent** (first eval and after-edit); `cargo test -p formualizer-eval --lib formula_plane` → **666 passed, 0 failed**. The Off-transition completion through `bf2b1f8b` was separately adversarially verified and passed workspace 3,714/0 plus Python 138/0.
