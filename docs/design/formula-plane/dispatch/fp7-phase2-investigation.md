# FP7 Phase 2 Investigation: evaluate_all Universal Coordinator

Date: 2026-05-04
Status: investigation complete; option (ii) selected.

## Reproduction

Forcing FP runtime in `evaluate_all_coordinator` regardless of `active_span_count`:

```rust
if self.config.formula_plane_mode == FormulaPlaneMode::AuthoritativeExperimental {
    return self.evaluate_authoritative_formula_plane_all();
}
```

yields **45 failures / 1369 passing** in `cargo test -p formualizer-eval`.

## Failure Classification

| Class | Count | Sample |
|-------|------:|--------|
| Cycle / error semantics | 2 | `arrow_canonical_606`, `layer_evaluation::test_evaluation_with_cycles` |
| Virtual deps / OFFSET / INDIRECT | 3 | `indirect::*` |
| Named ranges (non-cell legacy deps) | 20 | `named_ranges::*`, `arrow_canonical_604/607` |
| Sources / tables | 9 | `sources::*` |
| Spill | 1 | `spill_edges::spill_values_update_dependents` |
| Schedule cache | 2 | `schedule_cache::*` |
| Computed overlay shape | 1 | `eval_flush_recalc_probe::repeated_edit_recalc_keeps_computed_overlays_bounded_and_correct` |
| Open / unbounded ranges | 2 | `infinite_ranges::*` |
| Range fan-out cap | 1 | `range_dependencies::test_massive_range_fan_out_performance` |
| Computed flush coalescing | 1 | `computed_flush::cap_zero_batches_computed_writes_before_compaction` |
| Transactions / undo / redo | 3 | `engine_transactions_617::*`, `engine_atomic_actions_618::*` |

## Root Causes

1. `evaluate_authoritative_formula_plane_all` returns `NImpl` on cycles (`schedule.is_authoritative_safe()`); legacy applies `#CIRC!` and returns `cycle_errors`.
2. FP runtime never calls `create_evaluation_schedule` / `changed_virtual_dep_vertices`, so virtual-dep replan, OFFSET/INDIRECT semantics, schedule cache hits, and telemetry are skipped.
3. `build_formula_plane_mixed_schedule` errors on legacy formulas whose direct dependencies aren't cell vertices (named ranges, sources, tables, dynamic refs).
4. `shared_range_to_region_pattern` rejects unbounded ranges that legacy graph dirty propagation handles fine.
5. FP runtime evaluates legacy formulas one-by-one through `evaluate_vertex_impl`, bypassing source cache, spill, array, and computed-flush coalescing semantics.
6. FP scheduler caps (`MaxCandidatesExceeded`) abort universal legacy eval on large workbooks where graph propagation succeeds.

## Decision: Option (ii)

The minimum-viable evaluate_all universal coordinator must compose with legacy primitives, not replace them.

```text
evaluate_all coordinator
  -> legacy graph scheduler executes legacy producers
  -> FormulaPlane contributes span producers and span ordering/dirty edges
  -> changed virtual deps redirty as in legacy
  -> cycles applied with #CIRC! and counted
  -> source cache / spill / arrays / overlay batching unchanged
  -> mixed scheduler caps may fail closed for span ordering only, never for legacy-only work
```

Concrete obligations to lift the `active_span_count() > 0` gate at `evaluate_all` only:

- Legacy work executes through `evaluate_layer_sequential` / `evaluate_layer_parallel`, not direct `evaluate_vertex_impl` calls.
- Cycles use legacy mirroring (`#CIRC!`, overlay, `cycle_errors`).
- Virtual-dep replan loop preserved.
- Schedule cache stays active when no span ordering edges need to be added.
- FP runtime constraints contribute span producer ordering only when active spans exist.
- Mixed scheduler caps must not abort legacy-only schedules.
- Non-cell legacy dependencies (names/sources/tables) do not propagate into FP `consumer_reads` index. They remain graph-owned dependency edges and the legacy scheduler honors them unchanged.

## Out of Scope for Phase 2

```text
- removing the other 11 active_span_count gates
- demand-driven coordinator (evaluate_until / evaluate_cells)
- delta / log / cancellation FP semantics
- RecalcPlan / schedule cache mixed plans
- structural / source / table / spill FP-side invalidation
- virtual-dep span integration
```

These remain Phases 3–7 in `fp7-universal-runtime-plan.md`.
