# Arrow Canonical Migration: Phased Plan

Status: Draft

This document proposes a phased plan to migrate the engine toward an architecture where:

- Arrow (base lanes + overlays) is the canonical store for *values*.
- The dependency graph is the canonical store for *formulas, dependencies, and scheduling*.
- Engine-mediated bulk edits (spill, table effects) are scheduled and applied as explicit effects.

The plan is designed to preserve correctness at every step, avoid large flag-days, and keep
performance on range-heavy workloads improving incrementally.

## Non-Negotiable Invariants (All Phases)

1) Correctness is independent of overlay budgets
- A memory cap may change performance (more compaction, different overlay representation) but must
  not silently change results.

2) Evaluation remains deterministic under deterministic mode
- Parallel evaluation only affects Phase A (compute), not Phase C (apply effects).

3) Feature flags fail loudly
- If a feature is disabled (e.g., spill), formulas requiring it must return explicit errors and must
  not silently approximate behavior.

## Current State (Reality Check)

Today, the engine effectively maintains two value representations:

- The dependency graph stores computed values keyed by vertex.
- The Arrow store holds base lanes and overlays that are sometimes used for fast range scans.

The split-brain shows up when:
- computed overlay mirroring is disabled/capped
- spill clearing/resize happens in bulk
- parallel evaluation applies results without projecting spill regions

The plan below removes the split by converging reads/writes on Arrow as the value truth.

## Phase 0 (1 day): Architecture Contract + Guardrails

Goal
- Make the desired end state explicit.
- Ensure no correctness depends on a best-effort mirror.

Work
- Land `docs/architecture/arrow-canonical-contract.md` and this plan.
- Add a dedicated config flag (if not already present):
  - `EvalConfig.features.arrow_canonical_values: bool` (default false)
- Add guardrails:
  - If `arrow_canonical_values=false`, range fastpaths must not consult Arrow unless it is known
    to be consistent.
  - If `enable_parallel=true`, ensure spill projection is not bypassed.

Exit criteria
- Tests cover guardrail behavior.
- No silent correctness regression when overlay mirroring is disabled.

## Phase 1 (2-5 days): Make Arrow Reads Correct Under All Overlay Policies

Goal
- Ensure Arrow-backed range reads are always correct when enabled.

Work
- Define and enforce per-cell read precedence:
  1) delta overlay
  2) computed overlay
  3) base lanes
- Remove any paths where overlay caps cause the engine to stop reflecting computed values in the
  representation that range functions read.

Concrete steps
- Replace "disable computed overlay mirroring" behavior with one of:
  - compact computed overlay into base lanes chunk-locally, OR
  - switch computed overlay for the impacted chunk/region to a dense representation.
- Add `overlay_memory_usage()` and budget enforcement that is correctness-preserving.

Code touchpoints
- `crates/formualizer-eval/src/arrow_store/mod.rs`
- `crates/formualizer-eval/src/engine/range_view.rs`
- `crates/formualizer-eval/src/engine/eval.rs` (overlay write sites)

Exit criteria
- Range aggregates (SUM/COUNT/...) return correct results across:
  - spill regions
  - budget exceeded scenarios
  - repeated recalcs
- Budget enforcement is deterministic.

## Phase 2 (3-10 days): Formalize “Effects” (Plan -> Apply) for Spill and Bulk Writes

Goal
- Make engine-mediated bulk edits a first-class scheduled operation.

Work
- Introduce an internal "effects" representation:
  - `Effect::WriteCell { cell, value }`
  - `Effect::SpillCommit { anchor, rect, values }`
  - `Effect::SpillClear { anchor, rect }`
- Ensure evaluation produces effects in a deterministic way.
- Apply effects sequentially and atomically where required.

Spill specifics
- Plan spill rectangle and blockers.
- Apply spill as a bulk write to computed overlay.
- Update ownership metadata.
- Emit ChangeLog event as one compound operation.

Code touchpoints
- `crates/formualizer-eval/src/engine/eval.rs` (evaluation pipeline)
- `crates/formualizer-eval/src/engine/graph/mod.rs` (ownership metadata)
- `crates/formualizer-eval/src/engine/graph/editor/change_log.rs` (compound events)

Exit criteria
- Parallel evaluation is safe: Phase A parallel, Phase C sequential effects.
- Spill + undo/redo works with a single ChangeLog event per spill commit/clear.

## Phase 3 (5-15 days): Flip “Canonical Values” to Arrow (Incrementally)

Goal
- Stop treating the graph value cache as authoritative.

Strategy
- Keep graph value cache temporarily for transitions, but enforce that reads for cell values go
  through the Arrow store.

Steps
1) Introduce a single internal read API, e.g. `Engine::read_cell_value(cell)`.
   - Route it to Arrow + overlays when `arrow_canonical_values=true`.
   - Route it to existing graph mechanisms when false.
2) Update builtins and interpreter paths to use the unified read API.
3) Remove direct reads of `graph.get_value(...)` in evaluation paths (or gate them behind the
   canonical switch).
4) Ensure all evaluation writes go to computed overlay (and user edits to delta overlay).

Exit criteria
- With `arrow_canonical_values=true`, no functional code path uses graph-cached values as the
  source of truth.
- The engine passes the full test suite in both modes.

## Phase 4 (ongoing): Performance Tuning + Overlay Representation Upgrades

Goal
- Make spills and large bulk edits fast under Arrow-canonical mode.

Work
- Replace spill writes from per-cell HashMap overlay inserts with:
  - dense segments per chunk, OR
  - a "chunk replacement" path.
- Compaction improvements:
  - compact when computed overlay density crosses threshold
  - compact when overlay memory budgets are hit
  - provide explicit `compact()` and/or `compact_sheet(sheet)` API

Instrumentation
- Add counters for:
  - overlay inserts
  - compactions
  - delta edge rebuilds
  - range_view materialization fallbacks

Exit criteria
- Spill commit/clear complexity approaches O(N + affected_dependents) rather than O(N * V).
- Range aggregates remain vectorized in common cases.

## Testing Strategy (Across Phases)

Must-have tests
- Spill semantics tests for commit/resize/conflict.
- Parallel spill tests.
- Overlay budget tests that validate correctness (not just memory usage).
- ChangeLog spill undo/redo + replay test.
- Corpus fixtures for spill + names + tables.

Perf regression tests
- Large spill clear under configured caps.
- Large SUM over mixed base+overlay.

## Rollout Recommendation

Short-term default
- Keep `arrow_canonical_values=false` until Phases 1-3 are complete.

Intermediate
- Enable it in CI in a dedicated job or feature matrix to prevent drift.

Long-term
- Make `arrow_canonical_values=true` the default once:
  - overlay budgets are correctness-preserving
  - spill + parallel evaluation are stable
  - ChangeLog supports engine-mediated edits robustly
