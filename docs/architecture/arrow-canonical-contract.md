# Arrow Canonical Value Store Contract

Status: Draft

This document defines the intended architecture contract for treating Arrow storage as the
canonical source of truth for worksheet cell values, while the dependency graph remains the
canonical source for formulas, dependencies, scheduling, and metadata.

The goal is to preserve (or improve) performance for range-heavy workloads, while simplifying
correctness for engine-mediated bulk edits (spill, table rewrites) and making ChangeLog/undo
reliable and bounded.

## Problem Statement

The engine currently has two partially-overlapping representations of cell values:

- A dependency graph value cache (computed values stored against vertices).
- An Arrow sheet store (base lanes) with two overlays:
  - a delta overlay for user edits
  - a computed overlay for formula/spill outputs

When Arrow is treated as an acceleration structure and is “mirrored into”, correctness depends
on perfect mirroring and consistent read paths. Any budgeting/capping behavior in overlay mirroring
risks correctness regressions in range functions (e.g., SUM) that use Arrow-backed range views.

By making Arrow + overlays canonical for values, we remove the split-brain: all value reads come
from the same place, and all writes (scalar, spill, bulk) go through a unified “apply effects”
pipeline.

## Definitions

Canonical (Value): The source of truth for what a cell currently contains as a value.

Canonical (Formula/Deps): The source of truth for what a cell’s formula is and what it depends on.

Base Arrow Lanes: Immutable-ish Arrow arrays representing cell values for a sheet in chunked columns.

Delta Overlay: Sparse layer that represents user edits or direct API writes since last compaction.

Computed Overlay: Sparse or dense layer representing outputs of evaluation (formula results, spill
children, table rewrite results) prior to compaction.

Compaction: Engine-managed operation that merges overlays into the base lanes and clears overlays.

Engine-Mediated Edit: A bulk value mutation initiated by evaluation semantics (e.g., spilling an
array result into many cells). These edits are not direct user commands but are effects of formulas.

## Contract: Source of Truth

1) Values
- Arrow storage (base lanes + overlays) is the canonical store for all cell values.
- Any API that reads values (bindings, workbook exports, range functions) must ultimately consult
  Arrow + overlays.
- The dependency graph may cache values for micro-optimizations, but that cache must be treated as
  a derivative view. If present, it must be invalidatable and must not be the only correctness path.

2) Formulas and Dependency Graph
- The dependency graph remains canonical for:
  - formula text/AST storage
  - dependency edges (cell, range stripes, names, tables)
  - dirty propagation and scheduling
  - vertex identity and cell<->vertex mapping
- Any structural edits (row/col insert/delete) update formula references and dependency edges; values
  in Arrow update through the same edit pipeline.

## Contract: Apply Pipeline (Evaluate -> Plan -> Apply)

Evaluation is conceptually split into three phases:

Phase A: Evaluate
- For a vertex, compute a result value:
  - Scalar value (single cell)
  - Array value (2D)

Phase B: Plan Effects
- Produce a set of engine-mediated edits (effects) that must occur for the evaluation to become
  observable in the grid.
- Examples:
  - SpillCommit(anchor, rect, values)
  - SpillClear(anchor, previous_rect)
  - TableThisRowRewrite(...) (if implemented as value effects)

Phase C: Apply Effects (single-threaded, deterministic)
- Apply scalar writes and effects to the Arrow store:
  - Scalar writes: update computed overlay for the anchor cell
  - Spill: bulk update computed overlay for target cells + update spill ownership metadata
- Emit ChangeLog events for these effects as a single compound edit.

Correctness rule:
- A completed evaluation step is not externally visible until Phase C is applied.
- Parallelism is allowed in Phase A only. Phase C must remain single-threaded unless proven safe.

## Spill Semantics as Effects

Spill is treated as an engine-mediated bulk edit.

Required invariants:
- Spill projection is atomic from the perspective of observers: either the entire region is updated
  (and ownership maps updated), or no change occurs and the anchor returns `#SPILL!`.
- Conflicts are detected against:
  - user-edited delta overlay cells
  - other spill ownership
  - non-empty base values (as defined by engine policy)

Undo/redo:
- Spill effects must be represented in ChangeLog as explicit operations with enough information
  to rollback and replay without re-evaluating formulas.

## Overlay Semantics

Read precedence (per cell):
1) Delta overlay (user edits)
2) Computed overlay (formula/spill outputs)
3) Base Arrow lanes

Write destinations:
- User edit APIs write to delta overlay.
- Evaluation results write to computed overlay.
- Compaction merges both overlays into base lanes.

Budgeting / caps:
- Caps must never change correctness. If a cap would be exceeded, the engine must choose one of:
  - compact
  - switch representation (e.g., dense segment overlay)
  - error explicitly
- “Stop writing overlay entries” is not acceptable unless there is another correct storage target.

## Compaction Policy

Compaction is an internal policy decision.

Constraints:
- Deterministic: compaction decisions must not introduce nondeterminism in observable values.
- Opaque: external consumers do not need to know when compaction occurs.
- Incremental: compaction should be chunk-local when possible.

Recommended policy knobs:
- max overlay bytes (delta and computed separately)
- overlay density thresholds per chunk
- explicit `compact()` API for long-running sessions

## Feature Flags / Config

Some semantics should be globally gateable:
- `spill_enabled`: if disabled, array results must not mutate other cells; return a stable error
  (`#N/IMPL!` or `#SPILL!` by policy) and do not apply spill effects.

Compatibility toggles:
- case sensitivity for name/table resolution

Storage toggles:
- Arrow storage enabled (if disabled, fallback to pure graph storage; correctness required)
- overlay enabled
- computed overlay enabled

## Observability and ChangeLog

All mutations that change observable grid values must be logged as ChangeLog events when the
ChangeLog is enabled.

Event requirements:
- Bounded size (spill bounded by max_spill_cells and/or by chunk compression)
- Replayable without evaluation
- Metadata propagation (actor_id, correlation_id, reason)

## Performance Expectations

This contract is compatible with maximal performance because:
- Range functions can operate on Arrow slices with overlay-aware merging.
- Spill and other bulk effects can use dense overlay segments for contiguous rectangles.
- Compaction amortizes overlay cost.

The key is to implement bulk-friendly overlay representations and avoid per-cell HashMap writes
in large spill regions.
