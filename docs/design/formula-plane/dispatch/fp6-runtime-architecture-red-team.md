# FP6 Runtime Architecture Red-Team

Date: 2026-05-03  
Branch: `formula-plane/bridge`  
Scope: review of the active FormulaPlane runtime docs plus current source seams under `crates/formualizer-eval/src/`. No production code changes are proposed here.

## Verdict

| Target | Verdict | Readiness call |
|---|---|---|
| FP6.1 core runtime stores and handles | WARN | Startable only as internal, behavior-inert storage work. The docs are directionally ready, but agents need tighter gates around generational IDs, formula/value authority separation, sheet identity, and not reusing passive scanner/run IDs as runtime authority. |
| FP6.3 sidecar region indexes and span dependency routing | WARN for isolated index primitives; FAIL for engine-integrated dirty authority | The sidecar-index architecture is the right shape, but runtime dirty integration is not ready until FP6.1/FP6.2 create real span/overlay authority and until the graph/eval bridge invariants below are documented. A test-only `SheetRegionIndex` skeleton is startable; hooking it into recalculation is not. |

Overall: the architecture is sound enough to begin FP6.1 behind a strict no-behavior-change boundary. FP6.3 should be staged as pure derived-index infrastructure first, with no claim that FormulaPlane dirty routing participates in normal evaluation until explicit bridge gates pass.

## Source Reality Check

- `crates/formualizer-eval/src/formula_plane/` is currently passive: `span_store.rs` builds `FormulaRunStore` descriptors from scanner candidates, and `dependency_summary.rs` builds passive pointwise summaries plus a reverse-query feasibility model.
- There is no current `FormulaPlane`, `TemplateStore`, `SpanStore`, `FormulaOverlay`, `SpanDirtyStore`, `SpanDomainIndex`, `SpanDependencyIndex`, or `FormulaOverlayIndex` runtime store in source.
- `crates/formualizer-eval/src/engine/graph/mod.rs` is still `VertexId`-centric for dirty propagation and evaluation selection; `get_evaluation_vertices()` filters to formula/name vertex kinds.
- `crates/formualizer-eval/src/engine/scheduler.rs` schedules `VertexId` layers and supports only `VertexId -> VertexId` virtual dependencies.
- `crates/formualizer-eval/src/engine/eval.rs` evaluates scalar graph formula vertices and mirrors outputs through per-cell computed overlay calls; span work items do not exist.
- `crates/formualizer-eval/src/arrow_store/mod.rs` and `crates/formualizer-eval/src/engine/range_view.rs` enforce value precedence as user delta overlay over computed overlay over base, including explicit `Empty` masking.
- The active docs reference `ComputedWriteBuffer` plus `DenseRange` / `RunRange` / `SparseOffsets` computed fragments, but this worktree exposes per-chunk `computed_overlay` maps and compaction; a source grep found no type named `ComputedWriteBuffer`. If the buffer exists in another substrate, the docs need to point agents to the concrete path. If not, FP6.4 must add it before any span evaluator claims fragment-backed writes.
- `crates/formualizer-eval/src/function_contract.rs` is already a public crate module consumed by the public `Function` trait. That surface may be useful input, but FP6 agents must not treat it as license to promote new FormulaPlane runtime primitives outside `formula_plane/`.

## Semantic Holes To Close Before Runtime Authority

### Authority Boundaries

- Formula authority and value authority are named similarly in source and docs. Current `EvalConfig::write_formula_overlay_enabled` is about computed value mirroring, not the new `FormulaOverlay` formula-authority layer. The active docs should explicitly call this out to prevent agents from storing formula punchouts in Arrow overlays.
- Existing passive `FormulaRunStore` uses `source_template_id: String` and scanner-derived support flags. That is not authority-grade runtime identity. FP6.2 must use exact canonical template keys and stable sheet binding, not `source_template_id` or lossy fingerprints.
- Sheet identity is under-specified. Runtime docs show `SheetId`, while current passive canonicalization and run descriptors often carry sheet display names. Cross-sheet formulas, sheet rename/delete, and structural transforms require a stable sheet-id/generation rule before cross-sheet spans can be authoritative.
- FormulaOverlay entries need a scope and coordinate convention. The docs say cell/region punchouts, but they do not yet say whether entries are keyed by placement coordinate, result coordinate, or both for non-scalar result regions. Initial scalar-only spans can key them identically, but the invariant should be documented.
- FormulaPlane state should remain internal. FP6.1 should not add public/default config or public API exposure; default-off runtime gating belongs later.

### Dependency And Dirty Semantics

- Current passive summaries only support `StaticPointwise` cell dependencies. The FP6 docs discuss row/column/rect spans and future whole-axis/range summaries. Promotion must hard-reject any formula whose accepted dependency summary is not exact and bounded under the active policy.
- The reverse-dependent invariant is present in the contract, but FP6.3 needs a stronger sidecar guarantee: every indexed precedent region that intersects a changed region must return a candidate entry before exact filtering. Over-return is safe; under-return is a correctness bug.
- Whole-span dirty is safe only after candidate discovery is no-under-return. It does not excuse an under-returning index or a missing exact filter after rect/axis bucket over-return.
- Current graph dirty starts from edited vertices, not arbitrary changed regions. FP6.3 needs an explicit changed-region extraction seam for value edits, formula edits, clears, bulk pastes, spills, structural edits, and computed span result regions.
- Dirty sidecar entries must be invalidated or rebuilt on any span normalization, overlay epoch change, template dependency-summary change, sheet rename/delete, row/column structural transform, or demotion.

### Evaluation Semantics

- The interpreter requires placement context through `current_sheet` and often `current_cell`. A span scalar loop must synthesize a correct `CellRef` for each placement without materializing one graph formula vertex per placement.
- Functions can read `FunctionContext::current_cell`, RNG seeds, row visibility masks, `RangeView`, date system, locale, cancellation token, and recalc epoch. The span evaluator must preserve all of these scalar semantics even when it avoids per-cell graph vertices.
- `RangeView` reads Arrow base plus overlays. A span task that writes computed results must flush before downstream tasks read those cells, or schedule ordering must prove no read observes stale computed overlay/base lanes.
- Explicit `Empty` is semantic data. Span output writes must preserve absent vs explicit `Empty`, including the case where a computed `Empty` masks old base content but remains below user overlay.
- Dynamic references, spills, volatile formulas, reference-returning functions, names/tables without stable contracts, local environments, and internal span dependencies must remain legacy until exact contracts exist.

### Graph And Scheduler Assumptions

- The graph is not optional. It is the correctness/may-affect backbone and currently owns most public edit/eval behavior. The sidecar must augment graph routing, not bypass it.
- `Scheduler` can only order `VertexId` tasks today. A span task cannot be injected safely without either a sidecar scheduling pass or a proxy vertex seam that preserves legacy precedents/dependents.
- The static schedule cache in `Engine` is keyed on graph topology and candidate vertices. FormulaPlane span epochs and index epochs must invalidate any cached plan that could miss span work.
- Cycle detection is graph-vertex based. Internal span dependencies and span-to-span/legacy cycles will be invisible unless FP6.5 defines proxy or sidecar SCC semantics. Initial runtime must reject internal span dependencies before acceptance.
- Current virtual dependency machinery is `VertexId` based. FormulaPlane should reject dynamic dependencies rather than trying to reuse runtime virtual dependency convergence for hidden span authority.

## Dangerous Freeball Zones

- Reusing `FormulaRunStore` / `FormulaRunId` as the runtime `FormulaSpanId` arena. The passive store is diagnostic and scanner-shaped; FP6.1 needs generational runtime IDs and owned stores.
- Creating one graph formula vertex, AST root, or scalar edge set per accepted placement while still reporting a compact FormulaPlane win.
- Treating `FormulaOverlay` as another Arrow/value overlay. FormulaOverlay controls formula definition authority; Arrow delta/computed overlays control values.
- Collapsing `SpanDomainIndex`, `SpanDependencyIndex`, and `FormulaOverlayIndex` into one map because they all query regions. Their over-return and exact-filter semantics differ.
- Skipping exact filtering because the first dirty policy is whole-span dirty. Whole-span dirty is a projection policy after a no-under-return candidate query, not a candidate-query substitute.
- Writing span results through graph value caches, user delta overlays, direct base-lane mutation, or ad hoc per-cell computed overlay calls instead of the contracted computed write buffer/fragment path.
- Extending public `function_contract` or `EvalConfig` as a convenience during FP6.1/FP6.3. New FormulaPlane runtime primitives should stay under `formula_plane/` and remain internal.
- Adding broad span-aware function APIs before the scalar span evaluator proves semantic parity.
- Accepting cross-sheet spans keyed by sheet display name without rename/delete/tombstone invalidation.
- Treating structural edits as index-only updates. Structural edits transform formulas, dependency summaries, domains, overlays, results, and graph dirty routing; unsupported cases must demote.

## Sidecar Region Index Design Check

The three-index split in the active architecture is correct and should be preserved.

### Required Index Roles

| Index | Must answer | Must not decide |
|---|---|---|
| `SpanDomainIndex` | Which span placement/result domains geometrically cover or intersect a cell/region. | Whether a covered placement is punched out or formula-authoritative. |
| `SpanDependencyIndex` | Which span dependency entries may be affected by a changed precedent region. | Whether the span output cell is still active after FormulaOverlay projection. |
| `FormulaOverlayIndex` | Which overlay entries/punchouts intersect a cell/region. | Whether a span should be split, merged, or demoted. |

### No-Under-Return Contract

- Every index query may over-return candidates, but must never under-return true intersections.
- Every over-returning query must run an exact filter against the authoritative region/domain record, not against only the bucket key.
- Exact-filter drop counts must be observable separately from candidate counts.
- Rect buckets, whole-row buckets, whole-column buckets, and whole-sheet buckets need independent tests that prove no under-return at boundaries.
- The exact filter must use a single coordinate convention. Current graph internals often use 0-based `AbsCoord`, while FormulaPlane docs use 1-based placement examples; FP6.3 should document the region key convention and conversion points.

### SheetRegionIndex Cautions

- Reusing `engine::interval_tree::IntervalTree` is acceptable for a first implementation, but agents should not assume it is a fully profiled geometry engine. Its current query shape is BTreeMap-based and can over-scan for high query endpoints.
- A spreadsheet-shaped index is still the right first substrate. Do not introduce a broad R-tree or generic geometry dependency before profiling says the specified buckets fail.
- Candidate payloads should include enough versioning to reject stale entries: span id generation, span version, template/dependency-summary version, overlay epoch if relevant, and index build epoch.
- Whole-axis and structural dependencies belong in explicit side buckets; do not encode them as giant bounded intervals that accidentally depend on current used-region heuristics.
- FormulaOverlayIndex must be bulk-operation friendly. Pasting or clearing a region should query one region and update entries in bulk, not degrade into per-cell eager span splitting.

## FormulaOverlay, Dirty, Scheduler, Evaluator, And Fallback Invariants

### FormulaOverlay

- Formula resolution must be: FormulaOverlay/user override > FormulaPlane span/template > legacy graph formula.
- Value resolution must remain: user/edit overlay > computed overlay > base.
- A value edit inside a span must create a FormulaOverlay `ValueOverride` or equivalent tombstone and write the user value overlay; the computed overlay at that cell must not be allowed to resurface after compaction.
- A clear inside a span must create a formula punchout and a value-plane explicit empty according to current clear semantics.
- A compatible formula edit may reabsorb into a span only after exact canonical-template equality and dependency-summary compatibility are proven.
- `LegacyOwned(VertexId)` must mean the span no longer owns that placement. It must also have dirty and index removal semantics, not just a lookup result.

### Dirty Store

- Span dirty must be unioned by span id and version. Stale dirty entries from a previous span generation must be ignored or rejected.
- Dirty projection must operate on the effective domain: placement domain minus intrinsic mask minus FormulaOverlay projection.
- Overlay changes themselves are dirty events. Creating/removing a punchout changes which engine owns formula results and must dirty downstream dependents of the affected result region.
- Whole-span dirty is a safe initial policy only for accepted bounded summaries. Unbounded/dynamic summaries must not enter the sidecar.
- Dirty sidecar routing must run alongside graph routing. It must not replace graph `mark_dirty` or graph range dependents.

### Scheduler

- FP6.3 should not schedule span work yet. It may populate `SpanDirtyStore` and prove candidate discovery.
- FP6.5 must define whether span tasks are sidecar work items or proxy vertices before any span output is evaluated in normal recalc.
- Legacy-to-span, span-to-legacy, and span-to-span edges need ordering tests. A span result region that feeds a legacy formula range dependency must dirty and schedule that legacy formula.
- Schedule caches must include FormulaPlane epochs or be disabled when FormulaPlane dirty work exists.
- Cycles synthesized by conservative summaries must demote unless an explicit recurrence/cycle policy accepts them.

### Evaluator

- The scalar span loop must use one stored template and placement reification, not clone/store one AST per placement.
- The evaluator must write through the contracted computed write buffer/fragment API. If the only available source path is per-cell `mirror_value_to_computed_overlay`, FP6.4 needs a prerequisite adapter or buffer implementation before claiming the eval-flush substrate.
- Span evaluation should produce a staged write set and then flush atomically enough that downstream reads do not see mixed old/new values within one scheduled task.
- Cancellation, deterministic RNG, current cell, date system, row visibility, and `RangeView` behavior must match the scalar interpreter.
- Any unsupported evaluation construct must demote or run legacy and increment fallback counters.

### Fallback And Demotion

- Demotion must remove or stale all derived sidecar index entries, clear span dirty state, update formula resolution, and dirty downstream result dependents.
- Fallback reason codes must distinguish unsupported summary, unsupported evaluator, structural transform failure, conservative-cycle synthesis, stale index epoch, materialization request, and public API demand for concrete formula objects.
- Lazy materialization must create a FormulaOverlay punchout before creating a legacy formula vertex for a span-owned placement.
- No optimized diagnostic may hide legacy materialization; materialized cells, vertices, AST roots, and edge rows avoided/created must be counted.

## Concrete Doc Updates And Phase Gates

### FP6.1 Doc Updates

- Add a short "source inventory" note: existing `FormulaRunStore`, `FormulaTemplateId`, and passive summaries are inputs, not the runtime `FormulaPlane` store.
- Add a naming guard to avoid confusion between existing computed value overlay flags and the new `FormulaOverlay` formula-authority store.
- Add a sheet identity gate: FP6.1 must choose stable `SheetId`/generation keys for runtime spans, templates, overlays, and indexes; display names are diagnostics only.
- Add a visibility gate: FP6.1 types stay `pub(crate)` or internal under `crates/formualizer-eval/src/formula_plane/` unless a separate stable-contract decision promotes them.
- Add stale-id tests: old span/template/overlay ids must fail after removal/reuse generation changes.

### FP6.1 Phase Gates

- `formula_resolution_prefers_overlay_over_span_over_legacy` must also verify value precedence remains user overlay over computed overlay over base.
- Add `value_edit_inside_span_requires_formula_punchout_and_user_overlay` as a store-level test with no evaluator authority.
- Add `legacy_owned_overlay_prevents_span_resolution` to define the lazy materialization escape hatch.
- Add `sheet_rename_or_delete_invalidates_span_resolution_or_demotes` as a behavior-inert invariant test or design test.
- Add a gate that no FP6.1 code changes `Engine` public/default behavior, graph dirty propagation, scheduler behavior, or evaluator writes.

### FP6.3 Doc Updates

- Add an explicit `RegionKey` coordinate convention and conversion rule from graph `AbsCoord`/`RangeKey` to FormulaPlane regions.
- Specify that each sidecar entry stores the authoritative region/domain reference plus version data for exact filtering after bucket lookup.
- Add a required exact-filter step to all three index query APIs, not only dependency queries.
- Define stale-index behavior: if `built_from_plane_epoch` or overlay/dependency-summary epoch mismatches, queries must rebuild or return a counted stale-index fallback; they must not silently return old candidates.
- Clarify FP6.3 exit claim: sidecar lookup and conservative dirty projection may populate FormulaPlane dirty state, but no span scheduling/evaluation authority is claimed.

### FP6.3 Phase Gates

- Add property tests for no-under-return on points, row intervals, column intervals, rectangles, whole rows, whole columns, whole sheet, and edge-boundary queries.
- Add over-return tests that assert exact-filter drop counts and final candidate correctness.
- Add parity tests against the current graph dependency planner for supported pointwise fixtures, including cross-sheet static cells once sheet identity is stable.
- Add stale epoch tests: mutate spans/overlays/templates, query without rebuild, and require rebuild/fallback rather than stale results.
- Add dirty bridge tests that prove changed regions from value edit, formula edit, clear, paste, and structural edit reach both legacy graph dependents and FormulaPlane candidate spans.
- Add a hard gate that FP6.3 cannot modify scheduler or evaluator behavior except for test-only plumbing until FP6.5/FP6.4 gates are satisfied.

### Result-Write Contract Update

- Add a concrete source path and API name for the computed write buffer/fragment substrate. If the intended abstraction is not yet in this worktree, add it as an explicit FP6.4 prerequisite.
- State that span output writes must not use graph `update_vertex_value`, user delta overlays, direct Arrow base writes, or unbatched per-cell mirror calls for any path claiming compressed span evaluation.
- Add counters for buffer input cells, coalesced fragment shapes, flush count, flush bytes if available, and fallback to scalar/per-cell write paths.

## Circuit Breakers

Stop and replan immediately if any implementation does one of these:

- Claims accepted span authority while allocating one formula graph vertex, AST root, or scalar edge set per accepted placement.
- Removes or bypasses the graph as the correctness/may-affect backbone.
- Stores FormulaOverlay punchouts in value overlays or inline inside `FormulaSpan` records.
- Collapses `SpanDomainIndex`, `SpanDependencyIndex`, and `FormulaOverlayIndex` into one ambiguous region map.
- Allows any sidecar index query to under-return, or skips exact filtering after bucket/interval over-return.
- Hooks FP6.3 sidecar dirty results into normal evaluation before a scheduler/proxy ordering seam exists.
- Writes span results through graph value cache, user delta overlay, direct Arrow base mutation, or an undefined per-cell mirror shortcut instead of the contracted computed write buffer/fragment path.
- Accepts dynamic, volatile, opaque, spill, reference-returning, name/table/local-env, or internal-dependency formulas without exact dependency and evaluation contracts.
- Uses sheet display names as runtime authority across rename/delete/structural edits.
- Lets structural edits keep optimized spans alive after unsupported transform paths.
- Adds public/default behavior changes or promotes FormulaPlane runtime types outside `formula_plane/` without a stable-contract decision.
- Adds broad span-aware function APIs before the scalar span evaluator and computed write path pass oracle parity.
- Suppresses fallback/demotion/materialization counters in optimized diagnostics.
- Evaluates span tasks in parallel with legacy tasks without a proof that all precedent writes are visible and all downstream dependents are dirtied.

## Recommended Start Plan

1. Start FP6.1 with a narrow store/handle PR and no `Engine` integration beyond internal tests.
2. In parallel, allow FP6.3 agents to prototype only `SheetRegionIndex<T>` and index-specific tests under `formula_plane/`, not dirty authority integration.
3. Add the doc gates above before dispatching FP6.2 promotion or FP6.3 graph-adjacent work.
4. Resolve the computed-write-buffer source/API mismatch before FP6.4 starts.
5. Treat `REPHASE_PLAN.md` FP5-FP7 as historical; all forward runtime gates should live in the two active docs plus small targeted dispatch notes.
