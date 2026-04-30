# Phase 8 Compatibility Notes

Status: draft compatibility note. This document constrains Phase 8 partitioning design so it remains compatible with Formula Plane V2, virtual references, Arrow-backed storage, WASM, and plugin-backed data. It does not implement Phase 8.

## Context

The feature-flagged beta branch is ready with caveats. Core+Overlay now has overlay-authoritative reads, compaction, auto-compaction, panic-safety, a workbook facade, e2e tests, atomic save routing, Python save bindings, bounded performance characterization, and a unified beta gate.

The next major architectural phase is partitioning and partition-level dirty propagation. Phase 8 must improve edit -> recalc scalability and prepare no-legacy-write-mirror work. It must not accidentally freeze a model that assumes:

```text
formula = one AST stored at one cell
dependency edge = cell/range only
range source = workbook sheet only
```

Future formula templates/runs and virtual references require a broader substrate.

## Phase 8 goals

Phase 8 should introduce enough partitioning to replace or reduce legacy stripe dirty propagation for supported paths:

- partition identifiers and partition membership;
- partition dependency summaries;
- dirty propagation through partition summaries;
- diagnostics for edge density and invalidation breadth;
- bounded edit -> recalc tests and performance smoke.

Phase 8 should not implement custom streaming ingest, shared-formula import, or full formula plane replacement unless explicitly re-scoped.

## Compatibility requirements

### 1. Partition APIs must not assume formulas are cell-local ASTs

Current implementation may consume existing formula snapshots. Public/internal partition APIs should still leave room for:

```text
FormulaTemplateId
FormulaRunId
FormulaPlacement
FormulaException
```

A formula run should be able to generate one or more partition dependency edges without materializing every formula cell edge.

### 2. Dependency regions and edge direction must be explicit

Conceptual dependency vocabulary should distinguish precedent roots, dependent formula placements, and result regions:

```rust
enum DependencyPrecedentRegion {
    WorkbookCell(PhysCoord),
    WorkbookRange { sheet: SheetPC, rows: AxisRange, cols: AxisRange },
    WorkbookPartition(PartitionId),
    VirtualReference(VirtualRangeId),
    VolatileRoot,
}

enum DependencyDependentRegion {
    FormulaCell(PhysCoord),
    FormulaRun(FormulaRunId),
    FormulaPlacementGroup(FormulaRunId),
    WorkbookPartition(PartitionId),
}

enum DependencyResultRegion {
    WorkbookCell(PhysCoord),
    WorkbookRange { sheet: SheetPC, rows: AxisRange, cols: AxisRange },
    WorkbookPartition(PartitionId),
}
```

The edge direction is:

```text
precedent region -> dependent formula/placement -> result region
```

Phase 8 may implement only workbook partition variants at first, but data structures should not require a breaking redesign to add formula runs or virtual refs.

### 3. Partition summaries should support rectangular/range dependencies

Range formulas are not just many cell dependencies. Partition summaries should represent:

- dependency on a whole partition;
- dependency on row-block ranges;
- cross-sheet dependencies;
- future virtual range dependencies;
- conservative superset edges when exact mapping is too expensive.

Conservative edges are acceptable if they are bounded and diagnosed.

### 4. Structural edits must work with page tables

Phase 8 dirty propagation must respect the VC/PC separation and page-table model already established in Phase 5A/5B.

Rules:

- store partition membership in physical/canonical terms where appropriate;
- use page tables to map edited visible coordinates to affected physical partitions;
- after compaction, rebuilt cores should have identity page tables and refreshed partition metadata;
- do not eagerly rewrite every formula or partition edge on every row/column edit when page-table translation can defer the work safely;
- preserve the Phase 5A.4 formula-axis convention: relative axes are PC-bound, absolute axes are literal VC-bound, and explicit sheet references use stable internal identity or deleted-reference sentinels.

### 5. Arrow-backed value/result storage remains the durable default

Phase 8 should not introduce a second durable value store.

Acceptable:

- temporary dense vectors during projection/build/test;
- partition metadata arrays;
- diagnostic edge tables;
- bitmaps/CSR adjacency for dirty propagation;
- sparse overlay maps.

Suspicious and review-required:

- durable `HashMap<CellRef, LiteralValue>` for core values;
- persistent row-major value matrices outside Arrow-backed core planes;
- facade range reads routed through legacy range materialization when scalar overlay authority should be used.

### 6. Virtual references are future dependency roots

Even if Phase 8 does not implement virtual providers, its graph model should allow future roots such as:

```text
VirtualRangeId changed -> dirty dependent workbook partitions
```

Do not bake in an invariant that all dirty roots are workbook cells. Persisted or deterministic virtual-reference identity must be stable source key + reference path/name + provider version token, not a session-local numeric handle.

### 7. WASM and plugin constraints remain active

Partition metadata and dependency summaries should avoid native-only dependencies. No Phase 8 primitive should require Polars, Parquet, mmap, threads, or direct filesystem access.

Provider-specific accelerators can be feature-gated later.

## Suggested primitive placement

### formualizer-eval for the bridge phase

While the FormulaPlane bridge is experimental, keep even low-dependency IDs/descriptors in `formualizer-eval::formula_plane`:

```rust
PartitionId
FormulaTemplateId
FormulaRunId
FormulaFingerprint
DependencyShapeFingerprint
VirtualSourceId
VirtualRangeId
GridExtent
GridShape
RangeCardinality
```

Runtime structures should also live in `formualizer-eval`:

```text
PartitionIndex
PartitionEdges
DirtyPartitionSet
FormulaStorageStats
FormulaRunStore
FormulaTemplateArena
```

Promotion to `formualizer-common` should require a later explicit stable-contract decision. The key for now is to avoid freezing runtime/planning vocabulary as shared public/common API too early.

## Recommended Phase 8 decomposition

### Phase 8.0: design and primitive seed

- finalize partition primitive names;
- introduce eval-local IDs/descriptors first; promote to common only after a stable-contract decision;
- add storage stats/audits if not already present;
- add docs tying partition dependencies to Formula Plane V2 and virtual refs.

### Phase 8.1: core partition metadata

- compute partitions during core construction/compaction;
- start with per-sheet or row-block partitions;
- record diagnostics only;
- no dirty-propagation behavior change yet.

### Phase 8.2: dependency summary graph

- build partition-level edges from existing formula/range data;
- include cross-sheet summaries;
- allow conservative superset edges;
- add diagnostics for fan-out and edge density;
- introduce or reserve the new dynamic range coverage vocabulary (`DynamicRangeCoverageIndex`, `DependencyRangeSummary`, or equivalent) rather than naming new runtime state after legacy `formula_to_range_deps`.

### Phase 8.3: dirty propagation integration

- route supported edit dirtying through partition summaries;
- keep legacy path as oracle/fallback initially;
- add edit -> recalc comparison tests;
- measure invalidation breadth.

### Phase 8.4: performance characterization

- compare legacy stripe path vs partition summaries on bounded edit -> recalc workloads;
- characterize dense formula columns, sparse formulas, and structural edits;
- document regressions and fallback triggers.

## Test requirements

Phase 8 tests should include both correctness and shape assertions.

### Correctness

- edit value -> dependent formula dirty/recalc;
- edit formula -> dependents dirty/recalc;
- row/column insert/delete -> affected formulas/ranges dirty;
- cross-sheet dependency propagation;
- compaction preserves partitioned dirty semantics;
- save/reload does not corrupt formulas/results.

### Shape/performance

- dense formula column produces bounded partition edge count;
- sparse formula sheet does not allocate huge run/partition structures;
- range dependency creates bounded conservative edges;
- edit invalidation breadth is recorded and below a budget for fixtures;
- no full-workbook dirtying for simple local edits unless explicitly documented.

### Storage invariants

- compacted value state remains Arrow-backed;
- overlay remains sparse after sparse edits;
- compaction clears overlay/page-table deltas and rebuilds partition metadata;
- no new durable row-major value store appears.

## Diagnostics

Add report-oriented diagnostics early:

```text
partition_count
partition_kind_counts
partition_edge_count
cross_sheet_edge_count
max_partition_fanout
average_partition_fanout
dirty_partition_count_per_edit
full_workbook_dirty_fallback_count
dynamic_range_coverage_entry_count
virtual_dependency_root_count (future)
formula_run_edge_count (future)
```

Diagnostics are essential for beta and performance work. They also make conservative fallback visible rather than hidden.

## Relationship to Formula Plane V2

Phase 8 should be able to consume current formula snapshots now and formula placements later.

Current:

```text
formula_snapshot cell ASTs -> derive dependency ranges -> partition edges
```

Future:

```text
formula templates + placements -> dependency templates -> partition edges
```

Design Phase 8 APIs around dependency regions/summaries, not direct formula AST ownership.

## Relationship to virtual references

Current:

```text
only workbook ranges produce dependency roots
```

Future:

```text
virtual source version change -> dirty dependent workbook partitions
```

Phase 8 should reserve a clean extension point for non-workbook roots.

## Risks

| Risk | Mitigation |
|---|---|
| Partitioning assumes one AST per cell | Document and type dependency summaries around regions/runs |
| Conservative edges dirty too much | Add diagnostics and budgets |
| Dense formulas explode edge counts | Allow run/rect summary edges |
| Virtual refs require redesign | Reserve dependency-source variants now |
| Arrow-backed storage regresses | Add storage stats and gate audits |
| WASM breaks due native deps | Keep eval-local bridge primitives dependency-light and WASM-safe |

## Phase 8 entry checklist

Before implementing Phase 8 behavior, confirm:

- Formula Plane V2 and Virtual References docs are present and reviewed;
- storage stats or equivalent audits can catch durable non-Arrow regressions;
- partition primitive names do not conflict with existing VC/PC brands;
- tests can measure dirty breadth and edge density;
- strict `arrow_canonical_611` baseline is either triaged or explicitly scoped;
- Phase 7.B beta gate still passes.

## Bottom line

Phase 8 is not only a performance phase. It is the dependency substrate that will decide whether future formula runs, virtual references, no-legacy-write-mirror, and Arrow-backed evaluation fit naturally. Its APIs should be region-oriented and provider-extensible from the start, even if the first implementation only handles workbook partitions derived from current formula snapshots.
