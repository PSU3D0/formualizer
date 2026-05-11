# FormulaPlane Pipeline Architectural & Perf Audit

Date: 2026-05-04
Status: read-only audit by subagent. No source changes. Findings inform priority queue.

## Executive summary

- **Span dirty regions are only captured for two edit paths.** `record_formula_plane_changed_cell` is wired into `set_cell_value` (eval.rs:4901), `set_cell_formula` (eval.rs:5026), and `bulk_set_formulas` (eval.rs:5054), and *nowhere else*. Spill commit/clear, source/table invalidation, structural ops (insert/delete row/col, remove_sheet, set_row_hidden), undo/redo, atomic actions, and formula‑authority rebuild after re‑ingest never call it. Under `AuthoritativeExperimental` this means span correctness silently depends on `formula_plane_indexes_epoch_seen`'s `WholeAll` fall-through every time the indexes_epoch bumps; the bounded‑dirty closure path is genuinely live only for set‑cell flows. **Most user‑visible architectural hole right now.**
- **`build_formula_plane_mixed_schedule` runs O(F) per `evaluate_all` even when nothing changed and even when there are no spans.** It walks `graph.formula_vertices()` *twice*, materializes `producer_results`/`consumer_reads` from scratch, and rebuilds `dirty_legacy` via `get_evaluation_vertices()` (which itself rebuilds an `FxHashSet`). On a 250k‑formula sheet that is ~500k point inserts plus dependency reflection per call. There is no schedule cache for the FP coordinator and the indexes_epoch is *not* consulted to skip rebuilds.
- **`SpanEvaluator::evaluate_task` builds a fresh `Planner` (with `PlanConfig`, range‑probe closure, function lookup) on *every placement*** via `evaluate_ast_with_offset` → `evaluate_ast_uncached` → `Planner::new(...)` (interpreter.rs:647). *Note:* re-checking, `evaluate_ast_with_offset` sets `disable_ast_planner: true` and short-circuits to `eval_tree_uncached`, so planner is **not** built on the FP path. But the *legacy* path still builds a planner per call.
- **The 7 "semantic‑regression" gates are still true silent‑degradation paths.** `evaluate_all_with_delta`, `evaluate_cells_with_delta`, `evaluate_all_logged`, `evaluate_all_cancellable`, `evaluate_cells_cancellable`, `evaluate_until_cancellable`, and `evaluate_recalc_plan` all detect `active_span_count() > 0` and *bypass* their delta/log/cancellation/plan and call `evaluate_authoritative_formula_plane_all()`. Cancellation is checked exactly once before the call; a long span pass cannot be cancelled, deltas are returned empty, ChangeLogs miss every span write.
- **Mixed‑schedule cycle handling is fail‑closed `NImpl` not `#CIRC!`.** `build_formula_plane_mixed_schedule` uses `MixedSchedule::is_authoritative_safe()`; on cycle detection it raises `ExcelErrorKind::NImpl` (eval.rs:6122) for the *whole* `evaluate_all` rather than mirroring `#CIRC!` only on the cyclic vertices. Phase‑3 fix has not landed; legacy `evaluate_all` does not have this property. Today masked because no-span workbooks shortcircuit to `evaluate_all_legacy_impl`.

## Architectural findings

### 1. FormulaAuthority is updated only on cell‑value/cell‑formula edits
`crates/formualizer-eval/src/engine/eval.rs:4915` (`record_formula_plane_changed_cell`). The ingest path also rebuilds indexes (`eval.rs:2651`). Every other state change — `commit_spill_and_mirror` (eval.rs:9260), `clear_spill_region`, `invalidate_source` (eval.rs:1776), `remove_sheet` (eval.rs:1578), `insert_rows`/`insert_columns` (eval.rs:635/696), `set_row_hidden` (eval.rs:538), undo/redo, transaction rollback — bumps `topology_epoch` but never calls `record_changed_region` or `mark_all_active_spans_dirty`. **A spill that lands inside a span's read region today will not redirty that span.**

Direction: every site that bumps `topology_epoch` should either call `record_changed_region` for the affected region(s) or `mark_all_active_spans_dirty()`; for spill commit, record the spill rect; for `remove_sheet`/`insert_rows`/`set_row_hidden`, mark the whole sheet dirty.

### 2. evaluate_authoritative_formula_plane_all rebuilds the mixed schedule from scratch per pass
`eval.rs:6089` builds `producer_results` and `consumer_reads` over *all* `legacy_vertices` in two separate loops (`eval.rs:6249` and `eval.rs:6262`) on every call. Authority already has these indexes precomputed for spans (`authority.producer_results`, `authority.consumer_reads`) — they are only used by Phase‑2 dirty closure but not by the scheduler itself. The spans are re‑inserted into a *fresh* index for the schedule, even when nothing changed.

Direction: keep two separate indexes — span‑side built once per `indexes_epoch`, legacy‑side built and *cached* alongside the static schedule cache, keyed by `topology_epoch`. The mixed schedule itself can be cached when `indexes_epoch` and `topology_epoch` are stable and dirty seeding is the only varying input (Phase 5).

### 3. evaluate_all_legacy_impl is invoked when no spans exist, but the FP coordinator pays path entry cost
`eval.rs:6094` shortcircuits to `evaluate_all_legacy_impl` only when `active_span_count() == 0`. Good — but every other `evaluate_*` that gates on `active_span_count() > 0` still has the *opposite* direction.

Direction: introduce one private `evaluate_coordinator(EvalIntent)` that dispatches and replace gates with `mode == AuthoritativeExperimental` checks per the universal‑runtime plan.

### 4. build_formula_plane_mixed_schedule returns NImpl on cycles; legacy returns #CIRC! and cycle_errors
`eval.rs:6121–6125`. `MixedSchedule::is_authoritative_safe` requires `fallbacks.is_empty() && cycle_count == 0`. A single `MixedScheduleFallbackReason::CycleDetected` makes the whole pass fail rather than degrading to legacy `#CIRC!` semantics.

Direction: split the schedule's `fallbacks` into `cycles` (apply `#CIRC!` on participating result cells, count, and continue) and `genuine_unsafe` (still `NImpl`). For legacy‑only schedule subsets, defer to the existing `Scheduler::create_schedule` cycle output as Phase 3 calls for.

### 5. MaxCandidatesExceeded / MaxEdgesExceeded aborts legacy‑only work too
Same `is_authoritative_safe()` check. The mixed scheduler's caps were intended for span‑side ordering; under the universal coordinator they now apply to the *legacy* dependency graph, which routinely has >100k edges. `MixedScheduleConfig::default()` is `max_edges=100_000`. On a 500k‑formula book this fails closed where legacy schedule succeeds.

Direction: caps must scope to span‑side edges only.

### 6. shared_range_to_region_pattern returns Ok(None) for SharedSheetLocator::Name(_)
eval.rs:6418. This silently drops named‑sheet range deps from the consumer‑read index. Correct behavior but no telemetry.

Direction: track a counter and assert under tests that legacy‑legacy ordering is retained for these.

### 7. formula_plane_indexes_epoch_seen is on Engine, not on the authority
eval.rs:378. Two coordinators sharing this counter cannot decide independently.

Direction: move to `FormulaAuthority` (alongside `indexes_epoch`) and gate on `(indexes_epoch, last_full_eval_epoch)` per coordinator.

### 8. Active spans are represented twice in the runtime
`authority.producer_results` / `authority.consumer_reads` are rebuilt by `rebuild_indexes()` at ingest, but `build_formula_plane_mixed_schedule` ignores them and rebuilds from `authority.active_span_refs()` again (eval.rs:6210–6238). Two parallel data structures that must agree, with no debug assertion they do.

Direction: have the scheduler consume the authority's owned indexes directly; only legacy‑side indexes need to be derived per pass.

### 9. evaluate_recalc_plan ignores the supplied plan whenever any span exists
eval.rs:6016. `EvalResult` returned does not indicate this.

Direction: at minimum return a flag in `EvalResult`/telemetry; better, materialize spans into a sidecar plan as the universal‑runtime plan recommends.

### 10. evaluate_vertex runs full whole‑eval before reading a single cell
eval.rs:5158. Demand‑driven evaluation of one downstream cell pays the cost of recomputing every active span on first call after each `indexes_epoch` bump.

Direction: Phase‑4 demand‑driven coordinator.

### 11. canonicalize_template runs eagerly on every record, even for trivially singleton groups
eval.rs:2604. Probe shows 32–50% load tax on no‑span workbooks. Issue B addresses this.

### 12. split_shadow_candidate_components uses BTreeMap/BTreeSet/VecDeque per group
eval.rs:2693. For sorted row‑run candidates from a single column, this is N log N with significant constants.

Direction: when candidates within a group all share a column or row and are pre‑sorted, fast‑path the contiguous‑run detection.

### 13. get_evaluation_vertices returns a Vec<VertexId> after building an FxHashSet
graph/mod.rs:1899. Called twice per FP pass.

Direction: return both `dirty_set` and a sorted vec once.

### 14. graph.formula_vertices() clones every formula vertex id and sorts
graph/mod.rs:2904. Called twice in `build_formula_plane_mixed_schedule`. For 500k formulas, two 4MB Vecs per pass plus two sorts.

Direction: cache by `topology_epoch`.

### 15. Authoritative ingest re‑clones every record's AST
eval.rs:2615 (`Arc::new(record.ast.clone())`). Records were freshly parsed; not shared. Then for legacy fallback (eval.rs:2640) the AST is cloned again via `(*candidate.ast).clone()`. Accepted spans clone once; rejected fallback formulas clone twice.

Direction: parsers should produce `Arc<ASTNode>` upstream.

### 16. evaluate_authoritative_formula_plane_all calls flush_computed_write_buffer per layer with a new ComputedWriteBuffer::default() each layer
eval.rs:6132. Each new `Vec<ComputedWrite>` reallocates as it grows.

Direction: thread one `ComputedWriteBuffer` through the whole pass and `clear()` between flushes.

### 17. evaluate_vertex_impl is called inline for legacy producers in the FP coordinator
eval.rs:6164, instead of going through `evaluate_layer_sequential`/`_parallel`. Bypasses parallel layer policy, spill‑aware planning helper, and source‑cache session boundaries.

Direction: route `FormulaProducerId::Legacy` work through `evaluate_layer_sequential`/`_parallel` for layers that contain only legacy producers.

## Allocation hotpaths

### 1. Planner construction in legacy path (per legacy cell, per eval)
`evaluate_ast_uncached` builds a Planner with `PlanConfig`, two closures, and a PlanNode tree per call. For 50k‑legacy‑formula evaluate_all this is 50k Planner allocations. (FP path is exempt via `disable_ast_planner: true`.)

Direction: cache per Interpreter; skip planner for trivial (no‑function, no‑range) ASTs.

### 2. Interpreter::with_current_cell clone per placement
span_eval.rs:133. Allocation cost ≈ memcpy of ~80B struct + LocalEnv::clone (one Option<Arc<...>> clone). When LocalEnv is empty (always true for span placements), essentially free. **Per‑placement, no heap allocation.**

### 3. ComputedWriteBuffer::push_cell per placement
For 50k placements, ≈ log(50k) ≈ 12 reallocations of the Vec.

Direction: `Vec::with_capacity(span_total_placements)` at top of layer.

### 4. build_formula_plane_mixed_schedule allocations per evaluate_all
- `dirty_legacy`: FxHashSet<VertexId> sized to |dirty|
- `producer_results`: index + Vec sized to |active_spans| + |formula_vertices|
- `consumer_reads`: same structure sized to |read_summary_deps_total| + |legacy_dep_edges|
- `legacy_vertices`: Vec<VertexId> built **twice** (eval.rs:6249, 6262)
- `seen` FxHashSet<Region> per legacy vertex (eval.rs:6271). **Per‑legacy‑vertex, per‑pass.**
- `span_refs_by_id`: BTreeMap<FormulaSpanId, FormulaSpanRef>
- `scheduled_legacy_vertices`: Vec<VertexId>
- `merged_work` inside scheduler: BTreeMap<FormulaProducerId, DirtyAccumulator> sized O(work)

Direction: this is the single biggest no‑edit‑occurred CPU cost. The whole struct should be epoch‑cached.

### 5. get_evaluation_vertices builds FxHashSet and a Vec then sorts
graph/mod.rs:1900. Called twice per FP coordinator entry.

### 6. take_pending_changed_regions
authority.rs:67. Cheap. Seen-set capacity preserved via `clear()`. Good.

### 7. compute_dirty_closure
Reasonably tight. Result `FormulaDirtyClosure` (work + changed_result_regions + fallbacks) is freshly allocated per call. Not reusable across calls.

Direction: low priority.

### 8. canonicalize_template produces a String payload per record
template_canonical.rs:42. For 50k singletons, 50k template payload Strings + 50k FxHashMap key allocations. Issue B root cause.

### 9. evaluate_arena_ast allocates Vec::with_capacity(args.len()) per Function call
interpreter.rs:523. Per recursion. Hot for arithmetic‑heavy cells.

### 10. evaluate_until builds VirtualDepBuilder per visited vertex
eval.rs:7170. Pre‑existing legacy issue inherited by FP coordinator once it routes demand‑driven.

## Optimization opportunities (beyond Issues A/B)

### 1. Uniform‑value span detection at canonicalize_template time. (HIGH, 50–200×)
When `CanonicalTemplateLabels::flags` contains only `AbsoluteReferenceAxis` (no Relative), every placement evaluates to the same value. Compute the result *once* at the origin and emit a `RunRange` fragment of length |domain| directly. For a 250k `=$A$1+1` family, one interpret call and one `OverlayFragment::run_range` instead of 250k cell pushes.

### 2. Direct Rect/RowRun/ColRun pushes for span output. (HIGH, 2–4×)
`SpanComputedWriteSink::push_cell` emits one `ComputedWrite::Cell` per placement. For varying‑value spans, `push_rect` already exists; if row‑major output collapses into Vec<Vec<OverlayValue>>, push once per layer.

### 3. Cache mixed‑schedule producer/consumer indexes by (topology_epoch, indexes_epoch). (HIGH)
On 250k‑formula book this rebuilds is multiple ms per evaluate_all even when nothing changed.

### 4. Span‑aware kernels for =A_n + scalar family. (HIGH, 5–10×)
Recognize `BinaryOp{Reference{relative}, Literal/AbsoluteCell}` and emit one Arrow vector op per span.

### 5. Short‑circuit canonicalization on unique formula text. (MEDIUM, Issue B)
Pre‑group records by `formula_text` only. Groups of size 1 emit `FormulaIngestRecord` directly to fallback without canonicalize_template.

### 6. Span‑level constant folding. (MEDIUM)
Fold `CanonicalExpr::Literal` subtrees once per template into a LiteralValue cache.

### 7. Buffer pool for ComputedWriteBuffer. (LOW)
Cache one buffer on Engine and reset between layers/passes.

### 8. Skip the legacy half of build_formula_plane_mixed_schedule when the dirty closure visits no legacy producers. (MEDIUM)

### 9. Cache Planner per Interpreter. (MEDIUM)
For 50k‑legacy‑formula evaluate_all this saves 50k closure constructions.

### 10. formula_vertices() Vec caching. (LOW)
Cache by `topology_epoch`.

### 11. Dirty‑propagation hooks at structural ops. (HIGH for correctness, MEDIUM for perf.)

### 12. Reuse seen FxHashSet across legacy‑vertex iteration. (LOW)

## Correctness risks

1. **Spill output not redirtying spans.** A spill that lands in a span's read region does not record a changed region in authority.
2. **invalidate_source does not redirty spans.** A span whose read summary references an invalidated source cell will not be redirtied.
3. **remove_sheet does not propagate #REF! to span result cells.** Spans have no graph vertices; the loop never touches them.
4. **evaluate_all_with_delta returns an empty delta when active_span_count() > 0.** Same for `evaluate_cells_with_delta` and `evaluate_all_logged`. Documented Phase‑6 hedge but currently observable.
5. **Cancellation observed only before scheduling.** Long passes are uncancellable.
6. **Mixed‑schedule cycles return NImpl for the entire pass.** Avoided today only because evaluate_all_legacy_impl is short‑circuit‑called when active_span_count() == 0.
7. **Concurrent dirty propagation between graph and authority.** mark_all_active_spans_dirty is never called by graph helpers.
8. **take_pending_changed_regions accumulates forever when active_span_count() == 0.** Small memory leak; harmless because subsequent ingest triggers `WholeAll`.
9. **Volatile redirty path under FP runtime.** Spans containing volatile functions are rejected at canonicalization, so non‑issue today.
10. **Snapshot id is bumped on edit but FP indexes never consult it.** Will become a risk under Phase 5.
11. **Post‑ingest rebuild_indexes() does not bump topology_epoch.** Static schedule cache may serve a stale legacy schedule (currently masked).
12. **evaluate_recalc_plan ignores plan entirely when spans exist.** User constructed a partial plan; gets full eval.

## Recommended priority queue

1. **Wire structural / spill / source / table edit hooks into FormulaAuthority::record_changed_region (Phase 7 minimal).** Only finding that is a *correctness* risk under default config. Use `mark_all_active_spans_dirty()` as conservative escape hatch where region precision is hard.
2. **Split mixed‑schedule cycle reporting from "unsafe" reporting (Phase 3).** Apply legacy cycle mirroring for legacy‑producer SCCs through `legacy_pass_apply_cycles`.
3. **Cache producer/consumer indexes and legacy_vertices snapshot in the FP coordinator keyed by (topology_epoch, indexes_epoch).** Single largest CPU win for unchanged repeated evaluate_all calls.
4. **Promote evaluate_recalc_plan, the delta variants, the logged variant, and the cancellable variants out of the silent‑degradation gate (Phase 5+6).** Either route their captured DeltaCollector / ChangeLog / cancel_flag into the FP coordinator (preferred), or *fail loudly* with NImpl when spans are present.
5. **Implement uniform‑value span broadcast and direct Rect writes.** 50–200× on absolute‑only families and 2–4× on dense varying families.
6. **Land Issue B short‑circuit canonicalization.** Eliminates the 32–50% no‑span load tax. Cheap and additive.
7. **Demand‑driven coordinator (Phase 4).** Replace the 4 conservative‑correct gates with target‑bounded mixed work.
8. **Wire Interpreter planner caching.** Saves O(N) closure + Vec + String::to_string() allocations per pass on legacy path.
9. **Move formula_plane_indexes_epoch_seen onto FormulaAuthority.** Prepares for demand‑driven coordinator.
10. **Improve mixed‑scheduler cap scoping.** Don't count legacy↔legacy edges.
