# Adaptive Formula Partition: Unifying Legacy and FormulaPlane Evaluation

Status: Draft (design proposal)

This document proposes the end-state architecture for formula evaluation: one dependency
graph operating over an **adaptive partition** of the formula population, where a node is
`(domain, template, executor strategy)`, span and singleton are the two ends of one dial
rather than two pathways, and **refinement** (split toward singletons) and **coarsening**
(merge toward spans) are first-class operations. It subsumes the planned columnar-span
execution design and reframes the open boundary-tax issues (#144 and the warm-cycle
overhead characterized below) as the first tranche of the unification rather than
standalone fixes.

The target workload envelope is deliberately wide: hyper-sparse workbooks (thousands of
scattered one-off formulas) must pay nothing for the span machinery, while dense sheets
(10M+ rows, hundreds of formula columns) must never materialize per-cell graph state.
Both extremes, mixed in one workbook, are the design constraint throughout.

The staged delivery contract is defined in
[Evaluation Resources and Target-Driven Cutover](evaluation-resource-target-driven-cutover.md).
It supersedes the T1.1 cache-overflow bridge below with exact non-materializing request topology;
demotion remains for cycle/lifecycle semantics. C0-pre PR #189 fixes deferred cross-sheet delta
preparation before that cutover.

## 1. Problem Statement

Authoritative FormulaPlane mode currently splits a workbook between two evaluation
regimes: span cells (template + placement run, no graph vertices) and legacy cells
(per-vertex graph formulas — everything that rejected at ingest). The two regimes already
share more than the names suggest (see §2), but **dirty tracking and lifecycle authority
remain dual**, and every cost and bug we have catalogued in this area lives exactly on
that boundary:

- **Staleness across the bail boundary (#144).** When the authoritative coordinator bails
  to legacy evaluation on a capacity fallback, legacy readers of span result regions
  evaluate against still-empty span cells and stay stale: the computed-overlay flush does
  not re-dirty dependents.
- **Warm-cycle boundary tax (measured 2026-06).** On the fp-coverage corpus, a warm
  `evaluate_all` with *zero edits* costs ~2 ms in Off mode and ~25 ms in authoritative
  mode. Section isolation attributes it precisely: every section alone is ~0; the
  overhead requires legacy cells with *wide range reads* (`self_cumulative`) coexisting
  with *any* spans — even spans on unrelated sheets with no data flow between them.
  It is roughly additive per span section (4–9 ms each) and scales superlinearly with
  the legacy range mass (1k rows → 2 ms, 2k → 4 ms, 4k → 13 ms, 8k → 43 ms; ~×3.3 per
  doubling, tracking the quadratic range-subscription mass of the cumulative pattern).
  Legacy cells with single-cell reads (`chain`) contribute zero. Working hypothesis
  (profile to confirm in tranche 1): the per-cycle mixed-schedule rebuild and producer
  index maintenance walk legacy range subscriptions; the cost is bookkeeping, not data
  flow.
- **Edit-path asymmetry.** Engine-level name APIs invalidate name-dependent spans
  (#147), but `VertexEditor` name mutations (journal replay/undo) bypass the hook.
  This is a *pattern*, not an instance: any edit path must currently remember to notify
  both authorities, in both directions, forever.

The family keeps spawning members because there are two authorities. The fix is not a
better boundary; it is one authority.

## 2. Current State (what is already unified)

An accurate inventory matters because the distance to the end state is shorter than
"old and new" suggests. As of #147 (`d5727f66`):

| Concern | State |
| --- | --- |
| Evaluation loop | **Already one loop.** `evaluate_authoritative_formula_plane_all` builds a `MixedSchedule` whose layers interleave span producers and legacy vertices, evaluating both per layer with a per-layer computed-write flush (eval.rs, scheduler.rs). There is no global legacy-pass/span-pass boundary. |
| Authority ownership | **Graph-owned.** `FormulaAuthority` owns spans and producer/consumer indexes; `FormulaDirtyState` separately owns all formula dirtiness inside `DependencyGraph`. |
| Cross-span cycles | **Detected and demoted.** Mixed-schedule toposort detects producer-level cycles, demotes the cyclic spans to legacy vertices, and resolves via the legacy cycle prepass. Intra-family self-reads reject at placement (`InternalDependency`). |
| Structural ops | **Precisely dirty.** Insert/delete rows/cols classify each span, mutate proven-safe geometry, and publish exact `(span ref, result interval)` dirtiness only for moved or cleared placements. Demotions transfer work to sparse legacy vertices; no-op and unrelated-sheet spans publish nothing. |
| Result storage | **Already columnar.** Span results stage in a `ComputedWriteBuffer` and flush as coalesced fragments into the sheet's computed overlay (Arrow-side), consulted at read time. |
| Ingest/fingerprinting | **Shared.** One pipeline canonicalizes, fingerprints, and computes per-cell read projections for every formula; placement accepts or falls back per family. Reject-path cost is linear (#146). |

What remains dual — the actual gap:

| Concern | State |
| --- | --- |
| Dirty tracking | **Unified through T1.3.** `FormulaDirtyState` owns sparse legacy vertex dirtiness plus generation-leased changed-region, exact span-region, and exact whole-span events. `FormulaAuthority` has no dirty queue, and topology epochs never imply value dirtiness. |
| Per-cell overrides | **No punchouts.** Editing one cell inside a span (value or different formula) demotes the whole span. `PlacementDomain` is contiguous (`RowRun`/`ColRun`/`Rect`); holes are unrepresentable. |
| Iterative calculation | Legacy-only. Iterative members are `VertexId`s; spans are excluded from SCC analysis. |
| Demand-driven evaluation | Legacy-only. `build_demand_subgraph` walks graph vertices; span producers are invisible to it. |
| Edit notification | Engine APIs, editor journals, undo/redo, formulas, values, names, tables, sheets, structural operations, span appends, and prepared commits publish through the graph dirty API. Structural deltas are planned before mutation and published once after committed geometry/index changes; failed validation publishes nothing. |
| Schedule/dirty cost model | T1.1 caches immutable mixed topology. T1.2 consumes graph-owned leased dirty events per request, and T1.3 seeds structural producer work with bounded span regions. Warm no-op and span-free work remain on the sparse legacy floor. |

## 3. Target Model

### 3.1 Node

The unit of the dependency graph is a **formula node**:

```
node = (domain, template, executor)
  domain   : placement set on one sheet — interval set per axis (holes allowed),
             degenerate case: a single cell
  template : canonical fingerprinted formula template (existing machinery);
             singleton nodes may carry an arbitrary AST as a trivial template
  executor : strategy chosen per node — tree-walk (any AST, domain of 1..n),
             columnar batch (vectorizable templates over runs),
             wavefront/scan (templates with internal recurrence; see §5.3)
```

"Legacy" disappears as a category. A one-off `=A1*PMT(...)+INDIRECT(B2)` formula is a
singleton node with the tree-walk executor — exactly today's representation and cost,
under one set of rules.

### 3.2 Edges and dirtiness

Edges are **region-shaped where they are wide, per-cell where they are narrow** — both
queried through one `mark_dirty`:

- Per-cell CSR edges remain the representation for singleton↔singleton dependencies
  (today's hot path, unchanged).
- Region edges (the existing `SheetRegionIndex` interval trees + affine dirty-projection
  inversion from placements) become a native edge representation owned by the graph, not
  a parallel system fed by `record_changed_region`.
- `mark_dirty` on a changed cell/region does: CSR dependents (existing BFS) + one
  region-index probe on that sheet. With zero region edges on a sheet the probe is a
  branch on an empty index — this is the **sparse floor invariant** (§6).

The plane epoch survives only for genuinely global invalidations; the current
"epoch bump → re-evaluate every span whole" escalation is replaced by precise
region-level dirtiness (a structural shift translates intervals; it does not invalidate
values that did not move). At 10M rows, `WholeAll` on every topology edit is not a
fallback, it is an outage.

### 3.3 Scheduling

One topological schedule over nodes (the existing `MixedSchedule` generalized: every
work item is a node; "legacy vertex" items become singleton nodes). Producer-level
cycle handling stays, with one addition — **refinement before demotion** (§5.2):
a node-level cycle first splits the participating domains at the cyclic intersection;
only genuinely cyclic cells land in the iterative/SCC machinery as singletons. Iterative
calculation and demand subgraphs then need no span-awareness at all: by the time they
run, everything they see is a node, and demand expansion is placement-interval algebra
(evaluate only placements intersecting the demanded region) — strictly better than
per-cell demand BFS at scale.

### 3.4 Executors

Executor choice is per-node and invisible outside the node:

- **Tree-walk** — the existing interpreter; floor executor for any AST.
- **Columnar batch** — the span_eval endgame: evaluate a template over a domain as
  vectorized column operations against Arrow lanes (SUM/SUMIFS already vectorize;
  this generalizes the dispatch). Scalar per-placement span_eval is the interim
  executor and remains the fallback for non-vectorizable templates.
- **Wavefront/scan** — templates whose read regions intersect their own result region
  (today's `InternalDependency` rejects: running totals, chains, multi-column row-wise
  recurrences typical of financial models). The node's internal order is row-major
  evaluation with column locality; associative recurrences upgrade to prefix scans.
  This converts today's by-design rejects — and the scheduler's worst-case topology
  (a 50k-cell chain is 50k single-vertex layers today) — into single nodes.

### 3.5 External semantics

Per-cell addressing, value reads, changelog events, and the WASM/telemetry surface do
not change: the computed overlay already presents span results cell-wise, and node
lifecycle events (split/merge) are internal. Changelog records cell-level old state
exactly as today (#140's direct-append path); node membership is not user-visible state.

## 4. Why Per-Cell Cannot Reach the Dense Target

10M rows × 200 formula columns = 2×10⁹ formula cells. At ~32 bytes of vertex + edge +
AST-handle state per cell (optimistic), per-cell representation is ~64 GB before storing
a single value — disqualified by arithmetic, not by benchmarks. The adaptive partition
represents the same sheet as O(templates × columns) nodes (hundreds to thousands), with
region edges of O(1) per node and dirtiness via interval inversion. Compression is
admission to the problem, not an optimization. The existing counters
(`graph_formula_vertices_avoided`, `edge_rows_avoided`, `ast_roots_avoided`) already
measure exactly this.

## 5. The Four Hard Problems

These are the load-bearing design risks. Each has a required mechanism; if any lacks a
convincing answer, the architecture fails under real workloads. They lead this document
deliberately — the happy path is not the risk.

### 5.1 Fragmentation lifecycle (punchouts, splits, coalescing)

Real users paste values over ranges, override single cells with exception formulas, and
leave ragged edges. Today this demotes the whole span; at 10M rows, one edited cell
de-compressing 10M is unacceptable, and without re-merging, spans decay monotonically
toward singletons under edit churn — leaving "legacy with extra steps."

Required mechanisms:

- **Domain holes.** `PlacementDomain` generalizes from contiguous runs to per-axis
  interval sets. A value paste over `[r1,r2]` inside a span punches a hole (O(log)
  interval edit), creating no new nodes. A *formula* override creates a singleton (or
  new-template) node in the hole.
- **Surgical split.** Structural straddles and refinement (§5.2) split a node into ≤3
  nodes at interval boundaries instead of demoting.
- **Background coalescing.** A low-priority pass (or ingest-adjacent heuristic) re-merges
  adjacent same-template domains and re-promotes singleton populations that re-form a
  template run — bounded work per cycle, amortized, never on the edit critical path.
  Policy needs hysteresis (don't re-promote a region the user is actively editing);
  the existing `MIN_PROMOTED_NON_CONSTANT_SPAN_CELLS` threshold generalizes into the
  coalescing policy.

This is the heap-fragmentation problem of the design and the single largest piece of
genuinely new machinery.

### 5.2 False cycles from node granularity

Per-cell SCC localizes a 2-cell cycle exactly. Node-granularity dependency is
conservative: span A reading a region of span B and vice versa is a 2-node cycle even
when zero individual cells cycle — and node-level iterative evaluation over millions of
cells would be catastrophic. Today's answer (detect, demote both spans entirely) is
correct but de-compresses too much.

Required mechanism: **refinement-on-demand.** On node-level cycle detection, split the
participating domains at the read/result intersection (using the same interval math as
§5.1) and re-run cycle detection on the refined partition. Iterate until the cycle is
localized to singletons (then the existing SCC/iterative machinery applies, unchanged)
or proven structural. Refinement is bounded: each round strictly shrinks the cyclic
region, and the common case (false cycle from rectangular over-approximation) resolves
in one split. The current detect-and-demote path remains the backstop.

### 5.3 Intra-node scheduling for recurrences

The wavefront executor (§3.4) is the largest performance payoff — it dissolves both the
`InternalDependency` reject class and the chain worst-case topology — and it is new
machinery with real semantic risk: evaluation order inside the node must match Excel's
cell-by-cell semantics exactly (including error propagation and mixed-type arithmetic),
multi-column recurrences need a row-major wavefront across the template group, and only
provably associative recurrences may upgrade to parallel prefix scans. Prototype early,
behind the executor interface, validated cell-for-cell against the tree-walk executor on
the corpus (`self_cumulative`, `chain`, and a new multi-column recurrence section).

### 5.4 Columnar end-to-end at the dense extreme

Span results already flush as coalesced overlay fragments (§2) — the remaining risks are
*granularity* and *aggregate reads*:

- Flush and dirty-projection batches must stay chunk-shaped at 10M rows (never per-cell
  loops over a span's result region; the existing fragment coalescing generalizes).
- Wide aggregate reads (`SUM(A:A)` over 10M rows) must not rescan the column when one
  cell changes. Required: **chunk summaries** (per-chunk sum/count/min/max metadata on
  Arrow lanes, invalidated per-chunk by the same region dirtiness) so incremental
  aggregate cost is O(dirty chunks), not O(column). This composes with, and does not
  modify, the Arrow canonical value store contract.

## 6. Scale Invariants (acceptance criteria for every tranche)

1. **Sparse floor.** A workbook with no multi-cell templates evaluates bit-for-bit on
   today's singleton path: CSR edges, BFS dirty, tree-walk, zero region-index probes
   beyond an empty-index branch, no per-cycle plane bookkeeping. Gate: probe-edit-storm
   and probe-finance-recalc A/B against pre-unification main stay within noise.
2. **Warm no-op.** `evaluate_all` with nothing dirty costs O(volatiles), independent of
   span count, legacy range mass, or their product. Gate: fp-coverage warm-cycle
   off/auth delta ≤ 1 ms at every tranche (vs. ~23 ms today); the corpus pair
   `self_cumulative × any-span-section` is the regression sentinel.
3. **Edit locality.** Editing one cell dirties work proportional to actual dependents:
   one placement for relative reads (counter-pinned since #145), O(dirty chunks) for
   aggregates over the region, never whole-plane (`WholeAll` escalation removed).
4. **Dense representation.** Graph memory for a templated sheet is O(templates ×
   columns) nodes, not O(cells); the avoided-vertices/edges/ASTs counters are the gate.
5. **Churn stability.** Under a sustained random-override workload, steady-state node
   count stays bounded (coalescing keeps up with fragmentation); values match Off mode
   cell-for-cell throughout. New probe required (`probe-fp-churn`).
6. **Value parity always.** Cell-by-cell equality vs. Off mode on the full corpus on
   first eval and after every mutation class (edits, structural ops, name/table
   lifecycle, undo) — the existing fp-coverage equality verdict, extended to the churn
   probe.

## 7. Inherent Caps (non-goals)

- **Dynamic references** (`INDIRECT`, computed `OFFSET` targets): dependencies are
  statically unknowable; these remain singleton nodes forever. A 10M-row INDIRECT
  column is 10M singletons in any architecture (Excel pays the same tax). Out of scope.
- **Volatiles**: always-dirty singleton nodes (or always-dirty spans where the template
  is otherwise uniform and the volatile call is placement-invariant — a later
  refinement, not load-bearing).
- **Spill/array sources**: spill *outputs* already live in the computed overlay; making
  spill producers span-shaped is future work, not part of this unification.
- **Distribution/sharding**: `partition.rs` (Phase 8 placeholders) concerns a different
  axis and is untouched by this design.

## 8. Migration Tranches

Each tranche is independently shippable behind the existing
`FormulaPlaneMode::AuthoritativeExperimental` gate, lands with the §6 gates, and is
sequenced so the open boundary bugs are *eliminated by construction* rather than patched.

Before the main T1 migration, a bounded correctness bridge removes the unsafe capacity-bail
side exit without waiting for topology unification:

- **T1.0a — Transactional span demotion.** Prepare, validate, and commit exact-ref,
  multi-sheet span demotion as one additions-only graph/authority transaction; cyclic span
  demotion uses the same batch so a later failure cannot publish an earlier subset.
- **T1.0b — Capacity-bail parity.** Unsafe non-cycle schedules transactionally demote
  exactly their scheduled spans before one legacy completion pass. Pending changed regions
  are generation-leased until successful completion, and a finite fallback-cell cap fails
  closed without partial graph, authority, overlay, telemetry, or dirty-state publication.
  This directly closes #144 while T1 removes the dual-authority boundary that produced it.
- **T1.1 — Cached mixed topology.** Compile immutable producer relationships and dirty
  projections once per exact graph/authority/semantic revision, then derive each request's
  schedule from its current dirty domains. Indexed candidate visitors and finite candidate,
  edge, and retained-memory budgets stop atomically and route overflow through T1.0b; no
  partial topology is cached. Value-only edits reuse the cache, dependency mutations rebuild
  it, and span-free workbooks perform zero mixed-topology builds.
- **T1.2 — Graph-owned dirty authority.** `FormulaDirtyState` is the only owner/API for
  sparse legacy dirty vertices, changed regions, and exact whole-span seeds. Generation leases
  retain the exact prefix across faults and retries, including same-value post-lease events.
  Successful evaluation acknowledges only its leased prefix. Global invalidations may seed all
  active spans explicitly and are telemetered; authority epochs no longer hide whole-span work.

1. **T1 — Single dirty authority (complete through T1.3).** Structural shifts publish precise
   span-result intervals; duplicate, rename, unrelated name, and table edits avoid unrelated span
   work; and topology epochs/revisions invalidate only the compiled cache. Explicit whole-span
   events remain limited to new spans, cycle retry, or documented opaque invalidations. Source
   invalidation and sheet removal remain explicit global cases because external-source semantics
   and cross-sheet/name/default-sheet resolution do not yet have a complete exact closure. Warm
   no-op and span-free requests bypass mixed schedule construction when no graph-owned formula
   event is pending.
2. **T2 — Node lifecycle.** Domain interval sets + punchouts + surgical split
   (§5.1 minus coalescing); single-cell overrides stop demoting spans. Editor-path
   unification: `VertexEditor` mutations flow through the same node-lifecycle API as
   engine APIs (closes the name-hook bypass class). Churn probe lands (without
   coalescing, it documents decay as the baseline for T4).
3. **T3 — Cycle refinement.** Refinement-on-demand replaces whole-span demotion on
   producer cycles (§5.2); spans thereby compose with iterative calc (cyclic cells
   become singletons inside the existing SCC machinery). Demand subgraphs learn
   placement-interval expansion.
4. **T4 — Coalescing.** Background re-merge/re-promotion with hysteresis (§5.1);
   churn-stability gate flips from "documents decay" to "bounded steady state."
5. **T5 — Columnar batch executor.** Vectorized template evaluation over Arrow lanes
   for the supported function/operator subset; chunk summaries for incremental
   aggregates (§5.4). This is the formerly-standalone columnar design, landing last
   because every prior tranche is executor-agnostic.
6. **T6 — Wavefront executor.** Internal-recurrence nodes (§5.3): `InternalDependency`
   reject class converts to spans; chain/cumulative corpus sections flip verdicts;
   prefix-scan upgrade where associativity is provable.

T1 is the next concrete engineering item and subsumes the previously queued "#144 +
warm-tax investigation": same code, done in the direction of the end state instead of
patching a boundary scheduled for deletion.

## 9. Open Questions

- Coalescing policy specifics (trigger cadence, hysteresis windows, interaction with
  changelog/undo grouping) — needs the T2 churn-probe data before committing.
- Wavefront semantics for non-associative mixed-type recurrences (error-value
  propagation order) — prototype against the tree-walk oracle before generalizing.
- Whether chunk-summary maintenance lives in the Arrow store (per the canonical-store
  contract) or graph-side as node metadata — decide in T5 design review.
- Snapshot/persistence format for interval-set domains (today's spans serialize as
  contiguous runs).
