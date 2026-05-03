# FP6 Subagent / Phase-Driven Execution-Risk Review

Date: 2026-05-03  
Branch: `formula-plane/bridge`  
Reviewer: review-only; no production code changes proposed.  
Scope: agent-execution risk for the FP6 dispatch program defined by
`FORMULA_PLANE_RUNTIME_ARCHITECTURE.md` and `FORMULA_PLANE_IMPLEMENTATION_PLAN.md`.
This is not a re-review of the architecture; it is a check on what build/review
subagents are likely to get wrong when they pick up FP6.1 through FP6.12 briefs.

## 1. Verdict

Overall: **WARN — ready to dispatch FP6.1 and the inert-substrate slice of FP6.3
behind tight prompt guardrails. Do not dispatch FP6.4 or anything that touches
recalculation until the integration-base and seam preconditions below are
written into the brief.**

| Dispatch target | Verdict | Notes |
|---|---|---|
| FP6.1 core stores/handles/resolution/overlay vocabulary | PASS-WITH-NITS | Safe as a behavior-inert internal slice. Brief must explicitly forbid engine wiring, public API, value-overlay confusion, and reuse of passive scanner IDs. |
| FP6.2 authority-grade placement | WARN | Only after FP6.1 lands and parity/counter tests exist. Brief must lock template-key payload, fallback labels, and "no graph vertex per accepted placement" gate. |
| FP6.3 sidecar region indexes (inert) | PASS-WITH-NITS for index primitives only | Hooking sidecar dirty into recalc is a separate dispatch and must wait. |
| FP6.3 dirty bridge | FAIL until FP6.1+FP6.2 land and changed-region seam is specified | Risk of premature engine integration is high; keep behind a phase gate. |
| FP6.4 span evaluator + computed fragment writes | FAIL until eval-flush PR #95 / Phase 5 substrate is integrated into this branch | This worktree has no `ComputedWriteBuffer` yet. |
| FP6.5 sidecar mixed scheduler seam | WARN | Must follow FP6.4 substrate availability and span-evaluator stability. |
| FP6.6 FormulaOverlay edit semantics | WARN | Requires FP6.1 overlay, FP6.3 indexes, undo/rollback hook, and value-plane parity tests already in place. |
| FP6.7 partial dirty | WARN | Easy place to under-approximate; gate strictly on no-under-return oracle. |
| FP6.8 normalization/repatterning | WARN | Easy place to silently change formula authority; gate on representation-only invariants. |
| FP6.9 structural edits | WARN | MVP must demote, not transform; explicitly defer exact transforms. |
| FP6.10 span-aware function kernels | WARN | Must not block scalar evaluator and must not extend public function APIs. |
| FP6.11 loader/shared-formula bridge | PASS-WITH-NITS | Parallelizable, but absence of metadata must remain inert. |
| FP6.12 default-off beta gate | WARN | Must arrive last; default-off parity must be a continuously-asserted invariant, not a one-shot test. |

The architecture and shore-up set are coherent enough that FP6.1 can be
dispatched today, but build agents will drift unless the brief disables the
common failure modes listed below.

## 2. Phase-by-phase risk matrix (FP6.1 – FP6.12)

| Phase | Top agent risks | Hard gates the brief must include |
|---|---|---|
| **FP6.1 stores/handles** | Reusing passive `FormulaRunStore` / `FormulaRunId` / `source_template_id` as runtime authority. Storing exception maps, ASTs, or dependency indexes inline in `FormulaSpan`. Confusing `FormulaOverlay` (formula authority) with Arrow `overlay` / `computed_overlay` (value authority). Touching `Engine`, `Scheduler`, or `DependencyGraph` for "wiring". Promoting types out of `crates/formualizer-eval/src/formula_plane/`. | All new types `pub(crate)`. No changes to `Engine`, scheduler, eval, graph, or public API. Generational IDs + epoch invariants tested. Sheet identity = `SheetId` + 0-based coords; display names diagnostics-only. `FormulaOverlay` lives in formula plane, not Arrow. |
| **FP6.2 placement** | Promoting on lossy scanner fingerprints. Calling `set_cell_formula` per accepted placement. Accepting cross-sheet spans before sheet-id/generation contract exists. Including dynamic/volatile/opaque/local-env/3D/external in the accepted family. Treating canonical hash equality as authority equality. | Template authority key is full payload, not a hash. `accepted_span_cells > 0` paired with `per_placement_formula_vertices_created == 0`, `ast_roots_avoided > 0`, `edge_rows_avoided > 0`. Cross-sheet spans rejected. Fallback reasons enumerated and counted. |
| **FP6.3 sidecar indexes (inert)** | Collapsing `SpanDomainIndex`, `SpanDependencyIndex`, `FormulaOverlayIndex` into one map. Skipping exact-filter step when bucket lookup is "obviously fine". Encoding whole-row/whole-col as `0..=u32::MAX` intervals. Importing the passive `FiniteRegion` (1-based + sheet name) into runtime indexes. Adding a generic R-tree/geometry crate. | Three role-specific indexes with separate query APIs. Exact-filter mandatory after every bucket query. No-under-return property tests on points, intervals, rects, whole-row/col, whole-sheet. Whole-axis stored in dedicated buckets. Region key convention documented and tested. |
| **FP6.3 dirty bridge** | Hooking sidecar dirty into `evaluate_all` / scheduler before FP6.5 seam exists. Replacing graph `mark_dirty` instead of running alongside. Under-approximating dirty by applying partial projection too early. Forgetting structural edits and bulk paste as changed-region inputs. | Sidecar dirty runs alongside graph dirty, not instead of it. Initial projection is whole-span only after no-under-return candidate query. Changed-region seam covers value edit, formula edit, clear, paste, span result region, and structural edit. Dirty entries keyed by span id + generation. |
| **FP6.4 span evaluator + writes** | Starting on a worktree without eval-flush substrate. Writing through `mirror_value_to_computed_overlay` per cell while reporting "fragment-backed". Cloning template AST per placement. Skipping flush boundaries so downstream reads see stale overlay. Accidentally bypassing user-overlay precedence. Hooking into normal recalc before FP6.5 seam lands. | Branch base **must** include PR #95 / Phase 5 substrate (`ComputedWriteBuffer`, `OverlayFragment`, fragment-aware `RangeView`); otherwise dispatch is rejected. One stored template + `Interpreter::new_with_cell` per placement. Writes go through `ComputedWriteBuffer` only. User overlay precedence verified. Evaluator is direct/test-only until FP6.5 lands. |
| **FP6.5 scheduler seam** | Reaching for `VertexKind::FormulaSpanProxy` instead of the sidecar `FormulaPlaneWorkItem` enum. Reusing `Engine::cached_static_schedule` while span work exists. Forgetting span-to-legacy and legacy-to-span ordering. Treating internal span dependencies as anything other than reject/demote. | Sidecar mixed work-item scheduler first; graph proxy explicitly deferred. Static schedule cache disabled or epoch-keyed when span work present. Ordering tests for legacy↔span and span↔span. Internal span deps reject with counted reason. |
| **FP6.6 FormulaOverlay edits** | Storing punchouts in Arrow overlays. Writing user value into `FormulaOverlayRecord`. Treating `ValueOverride`/`Cleared` as absence and letting span recompute. Eagerly splitting spans on every paste/clear. Wiring edits into atomic actions before undo journal exists. Confusing "remove user overlay" with "write explicit Empty". | Overlay stays under `formula_plane/`. `ValueOverride`/`Cleared` are formula tombstones. Region-first paste/clear via `FormulaOverlayIndex`. Undo batch added before atomic-action integration. Value-plane effect required for every overlay state transition. |
| **FP6.7 partial dirty** | Inventing projection variants beyond the listed `DirtyProjection` enum. Narrowing dirty for a projection family without a proof of monotonicity/exactness. Letting `ConservativeWhole` mask an unbounded footprint. | Only the eight enumerated projections may narrow. `UnsupportedUnbounded` rejects span. No-under-approx oracle remains green. `dirty_under_approx_oracle_misses == 0` enforced. |
| **FP6.8 normalization/repatterning** | Splitting/merging spans in a way that changes effective formula authority. Treating eager split-on-edit as the edit semantic. Using passive scanner output as repattern authority. Demoting silently without a counted reason. | All transforms are representation-only and preserve the authority cascade. Demotion always counted. Splits/merges require canonical equality + dependency-summary equality. |
| **FP6.9 structural edits** | Reimplementing parallel structural semantics instead of integrating with `VertexEditor`/Arrow shifts. Trying exact transforms in MVP. Updating indexes only without updating spans, overlays, summaries, dirty. Forgetting cross-sheet precedent transforms. | MVP demotes affected spans/regions on any unsupported case. Exact transforms gated behind FP6.9-bis brief with oracle coverage. Sheet rename/delete forces tombstone/generation update or demote. Atomic-action delete-rows/cols policy preserved. |
| **FP6.10 function kernels** | Building a broad new public function trait surface. Implementing kernels before scalar parity holds. Coupling function kernel decisions to dependency contracts. | Defaulted hook returns `None`. Kernels are optional accelerators; scalar fallback always available. Public `Function`/`function_contract` surface unchanged. |
| **FP6.11 loader bridge** | Treating loader hint as runtime authority. Letting hint absence change behavior. Bypassing canonical verification because the loader "said so". | Hints are inputs to candidate iteration only; canonical verification still required. No behavior change when hint absent. Loader feature flag under `formula_plane/`. |
| **FP6.12 default-off beta** | Adding the config gate too late so default behavior drifts. Counting wall-time before counters prove compact authority. Letting the beta corpus run only against FormulaPlane-enabled engine. | Default-off parity tests added on day one of FP6.1 and remain in main suite. Acceptance compares legacy vs FormulaPlane engines with both value parity and counter invariants in the same assertion. Wall-time only as supporting evidence. |

## 3. Likely subagent failure modes (concrete bad-assumption patterns)

These are the specific drifts to expect when subagents read the FP6 docs without
custom guardrails.

### 3.1 Hidden assumptions

- "`FormulaRunStore` and `FormulaRunId` are basically the runtime span store, so
  I'll add a few generational fields and call it FP6.1." Wrong: passive scanner
  output is candidate evidence only.
- "`ComputedWriteBuffer` must already exist somewhere in `arrow_store`, I'll
  thread through `mirror_value_to_computed_overlay`." Wrong: that path is
  per-cell and predates eval-flush. The substrate has not landed in this
  worktree.
- "The architecture says `FormulaOverlay`, and there's already
  `EvalConfig::write_formula_overlay_enabled` and `formula_overlay_writeback.rs`,
  so I'll plug into those." Wrong: those are computed-value mirroring; new
  `FormulaOverlay` is formula-definition authority.
- "Sheet identity by display name is fine for a row-run on a single sheet."
  Wrong: even single-sheet authority must use stable `SheetId` so rename/delete
  semantics behave deterministically and cross-sheet promotion can be added
  later without a rewrite.
- "`canonical_template.stable_hash()` equality means the family is identical."
  Wrong: hashes are diagnostic; full canonical payload comparison is mandatory.
- "Whole-row dependencies fit in an `IntervalTree<u32>` keyed `0..=u32::MAX`."
  Wrong: that leaks used-region heuristics and breaks exact filtering.

### 3.2 Hedging by quietly reducing scope

- Implementing FP6.4 against per-cell `mirror_value_to_computed_overlay` "until
  the buffer lands", then forgetting to gate the path on the substrate. The
  brief has to forbid this outright.
- Skipping FP6.3's exact-filter step on rect-bucket queries because "all
  candidates currently match". Without a property-based no-under/over-return
  test the path silently drifts.
- Building only `RowRun` placement and quietly dropping `ColRun`/`Rect` while
  reporting "FP6.2 done". The brief must list all three minimum domains and
  require their tests, even if `ColRun`/`Rect` are tiny fixtures.
- Implementing `ValueOverride` and `Cleared` as a single `Tombstone` variant
  "for now". They have different value-plane effects (write user value vs
  explicit Empty) and merging them silently changes clear semantics.
- Writing FP6.6 edits without a FormulaOverlay undo journal because "the
  existing transactions look close enough". This breaks rollback.

### 3.3 Drift into superseded REPHASE_PLAN FP5-FP7 framing

- "FP5 says graph-build hint integration with no authority change, so I'll keep
  graph materialization and add a sidecar diagnostic." That is the superseded
  forward path; FP6 is opt-in compressed authority backed by
  `ComputedWriteBuffer`.
- "FP7 was the first span executor; the new doc says FP6.4, so I'll just rename
  it." The new FP6.4 has additional preconditions (eval-flush substrate,
  scheduler-seam staging, FormulaOverlay effective domains) that the old FP7 did
  not enumerate.
- "FP6 / first materialization reduction is satisfied by emitting more
  diagnostics." No: any optimized claim must show
  `per_placement_formula_vertices_created == 0` and counted fallbacks.
- Re-citing `REPHASE_PLAN.md` Decision/Why/Non-goals as the controlling source
  in PR descriptions. Briefs should explicitly point at the active runtime docs
  and treat REPHASE_PLAN as historical only.

### 3.4 Over-implementing broad subsystems

- Introducing a generic R-tree, k-d tree, or workbook-wide spatial index in
  FP6.3 because "we'll need it eventually." The shoreups specify spreadsheet-
  shaped sidecar indexes.
- Adding a public `eval_span` trait method on `Function` in FP6.4 because the
  architecture mentions it conceptually. The hook is a defaulted `None` future
  accelerator for FP6.10, not an FP6.4 deliverable.
- Building a span-aware reference-resolution layer that bypasses
  `EvaluationContext::resolve_range_view`. Reuse the engine context.
- Designing graph-native `VertexKind::FormulaSpanProxy` and `DepTarget::SpanProxy`
  in FP6.5 instead of the sidecar `FormulaPlaneWorkItem` enum.
- Replacing `engine::interval_tree` with a "better" datastructure as part of
  FP6.3 instead of a thin reuse wrapper.

### 3.5 Accidentally changing public/default behavior

- Wiring `FormulaResolution` into `Workbook::get_formula` / `Engine::get_cell`
  in FP6.1 to "validate the contract". That changes formula text rendering for
  callers and risks materializing.
- Adding `EvalConfig::formula_plane_enabled` early and defaulting it true, or
  wiring it through public APIs before parity tests exist.
- Promoting `FormulaTemplateId` / `FormulaSpanId` into `formualizer-common` so
  other crates can "consume them later". Unscoped public surface change.
- Letting `VertexEditor` / undo paths see span-owned formulas as `None` and
  silently drop edits. Public-visible regression.

### 3.6 Conflating adjacent concepts

- Storing `FormulaOverlay` punchouts inside Arrow `ColumnChunk.overlay`.
- Treating `SpanDependencyIndex` as a reverse map of `SpanDomainIndex`.
- Treating FormulaPlane scheduler integration as "evaluate at Engine eval time"
  instead of "produce work items the engine adapter orders alongside legacy".
- Counting `passive span_counters.rs` numbers as runtime acceptance. They are
  representation/avoidable estimates, not runtime proof.
- Treating `FormulaOverlay::Cleared` as "remove user overlay" rather than
  "write explicit Empty under current clear semantics".

### 3.7 Phase-ordering drift

- Connecting FP6.3 sidecar dirty into `evaluate_all` before FP6.5 schedules
  span work items. The plan explicitly forbids this; subagents tend to add the
  wire because "the test runs faster end-to-end".
- Implementing FP6.7 partial dirty before FP6.3 no-under-return tests pass.
- Starting FP6.6 paste/clear semantics before FormulaOverlay undo journal and
  effective-domain projection are tested.
- Dispatching FP6.10 kernels before FP6.4 scalar parity holds.

### 3.8 Hidden legacy fallback / hidden materialization

- FP6.4 evaluator that calls `set_cell_formula` for each placement to "use the
  existing interpreter for free" and reports compact authority anyway.
- FP6.6 unsupported-formula path that creates a graph vertex without the
  matching `FormulaOverlay::LegacyOwned` punchout, leaving the span technically
  "alive" in dirty routing.
- FP6.9 structural edit handler that "shifts spans" by deleting and re-creating
  legacy graph formulas under the hood.
- Counters that show `accepted_span_cells > 0` while `legacy_fallback_cells` is
  silently uncounted because the demotion path forgot to label its reason.

## 4. Recommended prompt guardrails

### 4.1 Build-agent guardrails (every FP6.x build brief)

The brief MUST contain, verbatim or paraphrased:

1. "Active controlling docs are
   `FORMULA_PLANE_RUNTIME_ARCHITECTURE.md` and
   `FORMULA_PLANE_IMPLEMENTATION_PLAN.md`. `REPHASE_PLAN.md` FP5-FP7 are
   superseded; do not cite them as plan-of-record."
2. "All new FormulaPlane runtime types live under
   `crates/formualizer-eval/src/formula_plane/` and are `pub(crate)` unless this
   brief explicitly authorizes promotion."
3. "Public/default engine behavior must remain unchanged. No changes to
   `Workbook` API, `Engine` public methods, or `EvalConfig` defaults unless the
   brief lists the exact field and default value."
4. "FormulaOverlay is formula-definition authority. It must not be stored in
   `ColumnChunk.overlay` or `ColumnChunk.computed_overlay`. Value-plane writes
   continue to use Arrow overlays; the precedence is user/delta > computed >
   base."
5. "Sheet identity is `SheetId` plus 0-based row/col. Display names are
   diagnostics. Cross-sheet span promotion is rejected unless this brief opts
   in and adds rename/delete invalidation tests."
6. "Passive `FormulaRunStore`, `FormulaRunId`, `source_template_id`, and
   passive scanner counters are candidate inputs only. They must not be the
   runtime store, runtime ID, runtime authority key, or runtime acceptance
   metric."
7. "Span result writes must go through `ComputedWriteBuffer` /
   `OverlayFragment` (eval-flush PR #95 / Phase 5). If the integration base does
   not contain that substrate, this brief is rejected — escalate; do not fall
   back to `mirror_value_to_computed_overlay`."
8. "Compact-authority claims require co-asserted counters:
   `accepted_span_cells > 0`, `per_placement_formula_vertices_created == 0`,
   `ast_roots_avoided > 0`, `edge_rows_avoided > 0`, plus enumerated
   `fallback_reasons` for non-accepted cells."
9. "Index queries may over-return; they must not under-return. Every
   over-returning bucket lookup must run an exact filter and emit
   `region_query_exact_filter_drop_count`."
10. "Do not introduce graph-native span proxies, broad function-kernel APIs,
    R-tree/geometry crates, or new public types without an explicit out-of-band
    decision recorded in `dispatch/`."
11. "Stop and escalate if you cannot satisfy a counter invariant rather than
    relax the invariant."

### 4.2 Review-agent guardrails (every FP6.x review brief)

Reviewer briefs MUST instruct the reviewer to fail the PR if:

1. Any new type appears outside `crates/formualizer-eval/src/formula_plane/`
   without an authorization line in the corresponding build brief.
2. Public API surface changes without a recorded scope decision.
3. `EvalConfig` defaults change.
4. Span result writes use `mirror_value_to_computed_overlay`,
   `set_computed_overlay_cell_raw`, `update_vertex_value`, direct base-lane
   mutation, or any path other than `ComputedWriteBuffer` / fragments while
   claiming compact authority.
5. `FormulaOverlay` storage is implemented via Arrow overlays.
6. `ValueOverride` and `Cleared` are merged into one variant or treated as
   "absence" that lets a span recompute.
7. Any sidecar index lacks a no-under-return property test or an exact-filter
   step.
8. Any of the three sidecar indexes (`SpanDomainIndex`,
   `SpanDependencyIndex`, `FormulaOverlayIndex`) is implemented as a thin
   wrapper around the others.
9. Whole-row/whole-col/whole-sheet dependencies live in numeric interval trees
   instead of dedicated buckets.
10. Dirty propagation introduces partial projection variants beyond the
    enumerated `DirtyProjection` set.
11. A structural edit handler updates spans/overlays/indexes without also
    invalidating projection caches and dirty entries by epoch.
12. Counters claim wins without paired fallback labels.
13. The PR cites `REPHASE_PLAN.md` FP5-FP7 as the controlling plan.
14. The PR introduces new graph vertex/AST/edge allocation per accepted span
    placement. (Diff-grep: `set_cell_formula`, `add_formula_vertex`,
    `vertex_formulas`, `dependency_graph::insert_edge`.)
15. FP6.4 lands without eval-flush substrate present in the integration base.
16. FP6.6 atomic-action edits land without a FormulaOverlay undo journal.

## 5. Recommended phase-gate / doc additions to prevent drift

1. Add a "Branch-base preconditions" section at the top of
   `FORMULA_PLANE_IMPLEMENTATION_PLAN.md` listing the required substrate per
   phase (FP6.1: none beyond current branch; FP6.2: FP6.1 merged; FP6.3 inert:
   FP6.1; FP6.3 dirty bridge: FP6.1+FP6.2+changed-region seam; FP6.4: PR #95
   eval-flush substrate; FP6.5: FP6.4 stable; FP6.6: FP6.1+FP6.3+undo journal;
   FP6.7: FP6.5 ordering tests green). Each phase brief copies this checklist
   verbatim.
2. Add a single short page `dispatch/fp6-agent-brief-template.md` that future
   build/review briefs must instantiate. Skeleton in §7 below.
3. Add an explicit "Counter Invariants" appendix (or merge into §15 of the
   architecture doc) that names the minimum counters every accepting fixture
   must assert, paired with the minimum fallback labels every non-accepting
   fixture must assert. The implementation plan §3 already enumerates DoD; this
   should be promoted to a checklist that briefs paste in.
4. Add a "Substrate gap" warning in the architecture doc near §1 that this
   worktree currently lacks the eval-flush types, with the explicit instruction
   that FP6.4+ must rebase onto / merge with PR #95 before claiming
   fragment-backed wins.
5. Mark the FP5-FP7 sections in `REPHASE_PLAN.md` with a brief inline header
   such as "Superseded — do not cite as plan-of-record" so a subagent that
   greps for "FP5" finds the supersession marker before the body. (The doc
   already has a top-level note; make it inline at each subsection too.)
6. Add a "Coordinate convention" line to FP6.3 doc tying `RegionKey` to 0-based
   `AbsCoord` semantics; the dirty-projection shoreup specifies this but it is
   not yet in the main architecture doc.
7. Add to FP6.1 brief a forbidden-paths list: `Engine::*`, `DependencyGraph::*`,
   `Scheduler`, `evaluate_*`, `set_cell_formula`, `mirror_value_to_computed_overlay`,
   `ColumnChunk.overlay`, `ColumnChunk.computed_overlay`, `formualizer-common`.
   Diffs must not touch these in FP6.1.
8. Add a "Forbidden vocabulary mapping" appendix:
   `FormulaRunStore` ≠ `SpanStore`, `FormulaRunId` ≠ `FormulaSpanId`,
   `source_template_id` ≠ runtime template authority key,
   `EvalConfig::write_formula_overlay_enabled` ≠ `FormulaOverlay`,
   `computed_overlay` ≠ FormulaPlane formula authority. Briefs cite this so
   agents stop conflating.

## 6. Sequencing and concurrency constraints

Strict ordering (must not run earlier in parallel):

```text
FP6.1 stores/handles/resolution/overlay vocabulary
  -> FP6.2 placement
  -> FP6.3 dirty bridge (sidecar dirty into recalc-adjacent code)
     -> FP6.5 sidecar mixed scheduler seam
        -> FP6.4 span evaluator participating in normal recalc
           -> FP6.6 FormulaOverlay edit semantics
              -> FP6.7 partial dirty
                 -> FP6.8 normalization/repatterning
                    -> FP6.9 structural edits MVP (demote)
                       -> FP6.10 optional kernels
                          -> FP6.12 default-off beta
```

Parallelizable (can dispatch concurrently if interfaces stabilize):

- FP6.3 *inert* index primitives (`SheetRegionIndex<T>`, role-specific wrappers,
  property tests) can run alongside FP6.2 once FP6.1 IDs/handles are merged.
- FP6.4 *direct/test-only* span evaluator skeleton can be drafted alongside
  FP6.5 design once FP6.1/FP6.2 are merged AND eval-flush substrate is in the
  integration base. Without that substrate the FP6.4 lane stays blocked.
- FP6.11 loader/shared-formula bridge can run in parallel with FP6.4-FP6.10
  because its outputs are passive hints; absence cannot change behavior.
- FP6.8 normalization design notes can be drafted in parallel with FP6.6 once
  FP6.1 overlay vocabulary lands, but implementation must wait for FP6.6.
- Lane I (oracle/benchmark/red-team) runs continuously alongside every lane.

Hard "do not parallelize" pairings:

- FP6.4 implementation and FP6.5 implementation must not start simultaneously;
  FP6.4 needs the work-item shape from FP6.5 design at minimum.
- FP6.6 (edits) and FP6.9 (structural) must not be in flight simultaneously;
  both touch FormulaOverlay/index epochs and merge conflicts will produce
  silent dirty/index inconsistencies.
- FP6.7 and FP6.8 must not be in flight simultaneously; both rewrite
  effective-domain semantics.
- FP6.10 kernels must not start before FP6.4 parity is green; otherwise scalar
  semantics lose their oracle.

Continuous gates that every dispatched lane must keep green:

- Default-off parity tests (FP6.1 onward).
- `cargo fmt --all -- --check`, `cargo test -p formualizer-eval --quiet`.
- `formula_plane_disabled_*` tests (added in FP6.1).
- Counter invariants on accepted fixtures.

## 7. Agent brief skeleton (build briefs paste this)

```markdown
# FP6.<x> Build Brief

## Phase target
- Phase: FP6.<x>
- Lane: <A/B/C/D/E/F/G/H/I>
- Controlling docs: FORMULA_PLANE_RUNTIME_ARCHITECTURE.md, FORMULA_PLANE_IMPLEMENTATION_PLAN.md.
- Superseded docs (do not cite): REPHASE_PLAN.md FP5-FP7.

## Branch-base preconditions
- Required merged work: <list FP6.x predecessors>.
- Required substrate: <e.g. eval-flush PR #95 for FP6.4+>; reject brief if absent.

## In-scope deliverables
- <bullet list copied from active doc>

## Out-of-scope (forbidden in this PR)
- Public API changes, default-config changes.
- Touching: Engine::*, DependencyGraph::*, Scheduler, evaluate_*, set_cell_formula,
  mirror_value_to_computed_overlay, ColumnChunk.{overlay,computed_overlay},
  formualizer-common (unless this brief explicitly opts in).
- Reusing FormulaRunStore / FormulaRunId / source_template_id as runtime authority.
- Graph-native span proxies, broad function-kernel APIs, R-tree/geometry crates.

## Authority/identity rules
- Sheet identity = SheetId + 0-based coords; display names diagnostics-only.
- FormulaOverlay is formula-definition authority; never stored in Arrow overlays.
- ValueOverride and Cleared are formula tombstones; not merged.

## Required tests (paste as fail-closed list)
- <copy from §6/§7 of the relevant shoreup>

## Required counters (must be asserted in accepting fixtures)
- accepted_span_cells, per_placement_formula_vertices_created == 0,
  ast_roots_avoided, edge_rows_avoided, fallback_reasons, +
  phase-specific counters (region_query_*, span_eval_*, computed_fragment_*).

## Stop conditions
- Cannot satisfy counter invariant -> escalate; do not relax invariant.
- Required substrate missing -> reject brief.
- Public/default behavior would change -> escalate.

## Validation ladder
- cargo fmt --all -- --check
- cargo test -p formualizer-eval formula_plane --quiet
- cargo test -p formualizer-eval --quiet
- (FP6.4+) cargo test -p formualizer-eval computed_flush --quiet
- (FP6.4+) cargo test -p formualizer-eval rangeview_ --quiet

## Exit claim
- <one-paragraph statement of what is and is not claimed; copy from active doc §X>
```

## 8. Reviewer checklist skeleton (review briefs paste this)

```markdown
# FP6.<x> Review Brief

## Scope check
- [ ] PR cites active controlling docs only (no FP5-FP7 from REPHASE_PLAN as
      plan-of-record).
- [ ] PR scope matches brief's "In-scope deliverables"; out-of-scope items
      flagged or rejected.

## Public/default behavior
- [ ] No `pub` symbols added outside `crates/formualizer-eval/src/formula_plane/`
      unless brief authorizes it.
- [ ] `EvalConfig` defaults unchanged.
- [ ] `Engine`, `Workbook`, `DependencyGraph` public APIs unchanged.
- [ ] Default-off parity tests still green (`formula_plane_disabled_*`).

## Authority and overlay separation
- [ ] FormulaOverlay storage is in `formula_plane/`, not Arrow overlays.
- [ ] `ValueOverride` and `Cleared` are distinct variants with distinct
      value-plane effects (user write vs explicit Empty).
- [ ] No re-use of `FormulaRunStore` / `FormulaRunId` / `source_template_id`
      as runtime authority.
- [ ] Sheet identity uses `SheetId` + 0-based coords; no display-name authority.

## Indexes and dirty
- [ ] Three role-specific indexes implemented separately, not collapsed.
- [ ] Every bucket query has an exact-filter step + drop counter.
- [ ] No-under-return property tests present for points, intervals, rects,
      whole-row/col, whole-sheet.
- [ ] Whole-axis stored in dedicated buckets, not as `0..=u32::MAX` intervals.
- [ ] Dirty entries keyed by span id + generation; stale entries rejected.

## Result-write seam (FP6.4+)
- [ ] All span result writes go through `ComputedWriteBuffer` /
      `OverlayFragment`. No `mirror_value_to_computed_overlay`,
      `update_vertex_value`, `set_computed_overlay_cell_raw`, or direct base
      mutation in span paths.
- [ ] `RangeView` fragment-aware reads validated.
- [ ] Flush boundaries respect downstream read ordering.

## Scheduler seam (FP6.5)
- [ ] Sidecar `FormulaPlaneWorkItem` enum, not graph-native proxy vertex.
- [ ] Static schedule cache disabled or epoch-keyed when span work present.
- [ ] Legacy↔span and span↔span ordering tested.

## FormulaOverlay edits (FP6.6)
- [ ] FormulaOverlay undo journal integrated with atomic actions.
- [ ] Region-first paste/clear; no per-cell eager span split.
- [ ] Compatible reabsorb / incompatible override / unsupported materialize
      paths each have a counted reason label.

## Structural (FP6.9)
- [ ] MVP demotes affected spans/regions; exact transforms gated.
- [ ] Indexes, projection cache, dirty entries all invalidated by epoch.
- [ ] Sheet rename/delete handled via tombstone/generation.

## Counters and fallbacks
- [ ] Accepting fixtures assert: accepted_span_cells > 0,
      per_placement_formula_vertices_created == 0, ast_roots_avoided > 0,
      edge_rows_avoided > 0.
- [ ] Non-accepting fixtures assert fallback_reasons enumerated.
- [ ] No "win" claim without paired fallback label.

## Drift checks (diff-grep)
- [ ] No new `set_cell_formula` / `add_formula_vertex` calls per accepted
      placement.
- [ ] No new public types in `formualizer-common`.
- [ ] No R-tree / geometry crate added.
- [ ] No new `Function` trait method added (FP6.10 only).

## Verdict
- [ ] PASS / WARN / FAIL with specific items above cited.
```

## 9. Closing note

The plan as written is dispatchable for FP6.1 and the inert slice of FP6.3 once
build briefs paste in the guardrails above. The most likely real-world failure
mode is not a flawed architecture; it is subagent drift around three load-bearing
distinctions: formula-authority overlay vs value overlay, runtime template
authority vs passive scanner identity, and contracted computed-write substrate
vs convenient per-cell mirror. Every FP6.x build/review brief should disable
those three drifts in its prompt, regardless of phase.
