# Lazy Reads — long-term vision (v0.8+)

## Position

Today (v0.6+), the engine requires explicit `evaluate_all` calls to
refresh computed values after edits. Reads can return stale or absent
values for dirty cells. The user-facing contract is "edit, then call
evaluate_all, then read".

The long-term direction is **lazy reads**: `get_cell_value(addr)`
auto-recomputes if the cell is dirty. The user contract becomes
"reads are always fresh".

## Why

Modern data systems converge on reactive / pull-based semantics
(React signals, observables, derive-on-access). The mental model
"reads are always fresh" eliminates an entire class of user errors
(forgetting to call evaluate_all). Internal state (cleared vs stale,
dirty vs clean) becomes invisible to the user.

## What it requires

The current engine has the underlying machinery:

- Per-vertex dirty tracking.
- Topological ordering for evaluation.
- Caching of intermediate results via Arrow overlays.
- Recursive evaluation through the interpreter.

What's missing:

1. **Topological subset evaluation**: when a single cell is read,
   evaluate only the dependency subgraph leading up to it, not the
   whole workbook.
2. **Cycle detection on read**: cycles must produce `#CIRCULAR!` errors
   without infinite recursion.
3. **Read-driven dirty propagation**: marking a cell clean only after
   its read completes.
4. **Memoization across reads within a single recalc epoch**: avoid
   re-computing the same cell if multiple reads request it.

## Interaction with explicit evaluate_all

`evaluate_all` would remain available as an explicit "compute everything
now" entry point. Lazy reads layer on top: a read that finds the
cell dirty triggers subset evaluation, then returns. `evaluate_all`
remains useful for batch workflows where the user wants to amortize
the cost up-front.

## Estimated scope

This is a v0.8+ goal. Significant work:
- Subset evaluator: ~1000 lines of new code in `engine/eval.rs`.
- Read-path integration: change `get_cell_value` to optionally
  trigger subset eval.
- API: probably a new method `Engine::get_cell_value_lazy()` or
  config flag `EvalConfig::lazy_reads: bool`.
- Tests: parity scenarios validating lazy reads produce identical
  values to explicit evaluate_all.

## What this dispatch unlocks

By unifying the post-structural-op contract (Off matches Auth, both
clear computed values), we set up lazy reads cleanly: cleared-state
means "needs evaluation"; lazy reads naturally trigger that evaluation
on access. No mode-specific divergence to disentangle later.
