# FormulaPlane Bridge Rephase Plan

Status: initial plan for `formula-plane/bridge`.

## Decision

Core+Overlay production closeout is paused as the immediate priority. Partitioning and span scheduling continue, but they are re-centered around FormulaPlane template/run ownership rather than the Core+Overlay `Session` no-legacy pathway.

This branch intentionally copies only reusable primitives and architecture notes from `migration/core-overlay` first. It must not import the Core+Overlay public facade, no-legacy harness, shadow-oracle machinery, or Session dirty-pilot authority as the product path.

## Why

The no-graph audit showed that hard `DependencyGraph` amputation is not close. At the same time, dense/shared-formula performance is not best solved by a weak sidecar that still allocates one formula AST/vertex/edge set per formula cell. The useful next unit is a FormulaPlane bridge:

```text
XLSX shared formula or inferred copied formula
  -> FormulaTemplateArena
  -> FormulaRunStore / placements / holes
  -> dependency summaries by region/partition
  -> dirty/result masks
  -> span scheduler / batched evaluator
  -> compatibility adapter only where required
```

## Non-goals for the first rephase

- No public API behavior change.
- No public/default fail-fast regressions.
- No hidden no-graph rewrite.
- No import of experimental `spike_no_graph` code.
- No Core+Overlay production-closeout continuation (`9.Q.5`, `9.Q.6`, etc.).
- No broad claim that XLSX/XML loading is or is not the bottleneck before baseline measurements are recorded.

## Copy-forward set

Copied from `migration/core-overlay` into this branch, then relocated to `formualizer-eval` because these are experimental runtime/planning concepts rather than stable cross-crate common types:

- `crates/formualizer-eval/src/formula_plane/ids.rs`
- `crates/formualizer-eval/src/formula_plane/partition.rs`
- `crates/formualizer-eval/src/formula_plane/grid.rs`
- `crates/formualizer-eval/src/formula_plane/virtual_ref.rs`
- `docs/design/formula-plane/FORMULA_PLANE_V2.md`
- `docs/design/formula-plane/VIRTUAL_REFERENCES.md`
- `docs/design/formula-plane/PARTITION_COMPATIBILITY_NOTES.md`

These are seed primitives/docs. They are not yet runtime authority and are not public API commitments.

## Incremental phases

### FP0 — Seed branch and reusable primitives

Deliverables:

- Fresh worktree from `origin/main`.
- Dependency-light FormulaPlane bridge primitives compiled under `formualizer-eval`.
- FormulaPlane/partition/virtual-reference docs copied to the new design namespace.
- Rephase plan recorded.

Gate:

```bash
cargo test -p formualizer-eval --quiet
cargo test --workspace --quiet
```

Success claim: only that reusable primitives compile and current behavior is unchanged.

### FP1 — Baseline and measurement closure

Deliverables:

- Baseline report under `docs/design/formula-plane/dispatch/`.
- Run or add bounded harnesses that record:
  - `load_ms`, `full_eval_ms`, `incremental_us`, RSS where available;
  - formula cell count;
  - formula AST/root count;
  - graph formula vertex count;
  - graph edge/dependency row count;
  - repeated-template/fill-down candidate counts;
  - shared-formula metadata visibility where available;
  - adapter/backend mode (`umya`, `calamine`, current native mode).
- Explicitly separate workbook open/read, engine ingest/build, full eval, and post-edit recalc where feasible.

Gate:

- No performance win required.
- Report must make uncertainty explicit and avoid broad claims.

### FP2 — Passive formula-template normalization metrics

Deliverables:

- Relative-semantic formula fingerprinting prototype for safe formula classes.
- Passive metrics over synthetic and at least one real/anchor corpus.
- No evaluation semantics change.

Measured outputs:

- template count;
- formula cell count;
- largest template families;
- contiguous row/column run candidates;
- exceptions/holes estimate;
- unsupported/dynamic/volatile counts.

FP2.A starts this phase by adding scanner-only FormulaPlane candidate span and row-block partition counters. These counters are diagnostic only: formula runs are dependent formula placements, fixed row-block partitions are estimates, and no dirty propagation, scheduler routing, dependency graph construction, or formula evaluation authority changes. The vocabulary remains `precedent region -> dependent formula placement -> result region`; FP2.A observes only the dependent placement shape.

Success claim: repeated formula structure can be quantified without accuracy risk.

### FP3 — Passive FormulaRunStore / placement model

Deliverables:

- Internal run/placement structures for row runs, column runs, rectangles, and holes.
- Builder that consumes formula snapshots or loader hints and emits passive runs.
- Diagnostics showing dense runs avoid duplicate representation in the passive model.

No runtime authority yet.

Gate:

- Existing eval/workbook tests unchanged.
- Shape tests prove stable run detection and hole splitting.

### FP4 — Shared-formula / loader capability bridge

Deliverables:

- Audit and document what Umya and Calamine expose for XLSX shared formulas and shared strings.
- Preserve shared-formula group hints where possible.
- If backend metadata is insufficient, add a scoped XML-reader experiment only as a metadata bridge, not a full loader rewrite.

Gate:

- Same workbook results as current loader.
- New hints are optional/passive; absence must not change semantics.

### FP5 — Dependency summaries and partition bridge

Deliverables:

- Formula-run dependency summaries using explicit vocabulary:

```text
precedent region -> dependent formula placement -> result region
```

- Partition summaries independent of Core+Overlay `Session` authority.
- Diagnostics for edge density, partition fanout, and dirty breadth.

Gate:

- No hidden legacy fallback in optimized diagnostics: compatibility use must be counted.
- Conservative summaries are allowed only with bounded/reportable breadth.

### FP6 — Compatibility adapter with allocation gates

Deliverables:

- Lazy graph/session compatibility adapter for legacy APIs that require cell materialization.
- Counters:
  - adapter materialized-cell count;
  - graph formula vertex count;
  - graph edge row count;
  - AST root count;
  - formula snapshot entry count;
  - run count;
  - template count;
  - exception count.

Success claim: any optimized path must prove it is not silently allocating one graph/AST/edge set per dense formula cell.

### FP7 — Narrow span scheduler MVP

Deliverables:

- One safe span execution path, initially for a simple dense formula family such as row-local arithmetic or scalar broadcast.
- Accuracy comparison against existing engine on bounded fixtures.
- Quantified wall-time/allocation result.

Gate:

- Accuracy-preserving against current engine outputs.
- Fallback for unsupported functions/dynamic refs is explicit and counted.
- Win must be tied to reduced materialization or batch execution, not hidden semantics changes.

## Landing policy

Each phase should land independently with:

1. narrow scope;
2. tests or reports;
3. baseline/metric deltas where relevant;
4. no public-support regression;
5. no hidden broad fallback;
6. clear statement of what is *not* claimed.

## Relationship to Core+Overlay

Core+Overlay remains a semantic reference for:

- VC/PC coordinate separation;
- page-table-compatible structural edits;
- deleted sheet tombstones;
- fallback taxonomy;
- partition diagnostic vocabulary.

It is not the primary product path for the FormulaPlane bridge. Future integration can consume FormulaPlane runs/partitions later if it becomes useful.
