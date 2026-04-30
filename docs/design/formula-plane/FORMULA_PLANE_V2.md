# Formula Plane V2

Status: draft architecture note for post-beta planning. This document is not an implementation contract for the current beta branch. It records the intended long-term formula storage and execution model so Phase 8 partitioning and later legacy-removal work do not bake in a one-cell/one-AST assumption.

## Motivation

The current Core+Overlay migration has made the feature-flagged beta path usable: supported reads are overlay-authoritative, compaction is explicit/automatic/panic-safe, and workbook/Python save paths are covered by bounded gates. Formula storage is still transitional. It is correct enough for beta, but the long-term model must handle two very different workbook shapes without compromising either:

1. sparse calculation sheets with a few hundred irregular formulas;
2. super-dense sheets with tens or hundreds of thousands of copied formulas plus a small number of one-off overrides.

A model that stores one independent AST per formula cell is acceptable for the first case and poor for the second. A model that assumes every sheet has long formula runs is good for the second and wasteful for the first. Formula Plane V2 is a hybrid model: template arena + placement index + exceptions + Arrow-backed value/result planes.

## Non-goals

This document does not propose implementing the full formula plane before Phase 8/9. It also does not include:

- custom streaming XLSX XML ingest;
- shared-formula master/follower import implementation;
- table/structured-reference support;
- no-legacy-write-mirror promotion;
- a new public API;
- a Polars/Parquet dependency.

Those remain separate future phases. The near-term goal is to seed the right primitives and keep Phase 8 compatible with this direction.

## Design principles

1. Arrow is the durable value/result substrate, not the universal formula representation.
2. Formula identity is not raw text; copied formulas need relative semantic fingerprints.
3. Dense formula areas should be represented as runs/rectangles with exceptions, not as repeated ASTs.
4. Sparse formula areas should remain sparse; do not impose run infrastructure where it hurts.
5. Structural edits should update placement/page-table metadata rather than eagerly rewriting every follower formula.
6. Dependency summaries should be able to describe formula placements and virtual ranges, not only individual cells.
7. Compaction is the natural time to optimize representation.
8. WASM compatibility requires dependency-light core primitives and provider-neutral abstractions.

## Storage planes

### Literal value plane

Literal values remain Arrow-backed through the existing SheetStore/ArrowSheet path.

```text
ImmutableCore.value_plane -> SheetStore / ArrowSheet
```

Formula cells do not become literal values merely because they have cached results. Preserve the value/formula exclusivity invariant for cell definitions.

Terminology for stats and diagnostics:

| Term | Meaning |
|---|---|
| Literal value cell | cell whose definition is a literal value |
| Formula definition cell | cell whose definition is a formula/template placement |
| Cached formula result | evaluated/display result associated with a formula definition |
| Overlay value override | sparse pending literal value override in overlay |
| Overlay formula override | sparse pending formula definition override in overlay |

A cached formula result can be Arrow-backed, but it is not a literal value definition. Phase 7.C stats must keep these categories separate.

### Formula definition plane

Formula definitions move toward a template/placement model:

```text
FormulaPlane
  FormulaTemplateArena
  FormulaPlacementIndex
  FormulaExceptionStore
```

This plane owns formula semantics. It is not required to be Arrow-backed internally.

### Formula result plane

Cached or evaluated formula results should be Arrow-backed when durable in the core:

```text
FormulaResultPlane -> SheetStore / ArrowSheet-like typed arrays
```

The result plane can support display, save, and vectorized evaluation. It should not erase the formula definition.

## Formula templates

A template is the normalized executable meaning of a formula pattern.

```rust
struct FormulaTemplate {
    id: FormulaTemplateId,
    fingerprints: FormulaFingerprintSet,
    bytecode: Arc<[FormulaOp]>,
    dependency_template: DependencyTemplate,
    flags: FormulaFlags,
}
```

The exact Rust shape is deferred. The important contract is that a copied formula column such as:

```text
C2 = A2 + B2
C3 = A3 + B3
C4 = A4 + B4
```

should map to one relative-semantic template:

```text
R[0]C[-2] + R[0]C[-1]
```

### Fingerprint tiers

Formula Plane V2 should support multiple fingerprint classes:

| Fingerprint | Purpose |
|---|---|
| Text | exact-ish display/source identity, after trivia normalization |
| RelativeSemantic | copied-formula template dedupe |
| DependencyShape | dependency graph/partition summary grouping |
| EvalPlan | compiled bytecode/execution plan reuse |

The tiers intentionally differ. A formula can have the same dependency shape but a different eval plan, or the same eval plan but different display text.

### Coordinate basis for fingerprints and storage

Formula Plane V2 must preserve the Phase 5A.4 stored-reference convention. Today, stored formula references are not simply "all PC" or "all VC":

- relative row/column axes are stored in PC space so they follow structural edits through page tables;
- absolute row/column axes are stored as literal VC coordinates so Excel-style absolute references keep their user-visible address semantics;
- explicit sheet references use stable internal sheet identity or deleted-sheet sentinels, not display-name rebinding.

Formula fingerprints must encode this axis binding explicitly. A relative-semantic fingerprint may normalize copied formulas by host offset, but it must still distinguish absolute-axis VC literals from relative-axis PC offsets. Placement anchors are physical/canonical anchors; public materialization relocates through the current page-table view.

In short: Formula Plane V2 fingerprints are based on formula semantics after applying the current stored-reference convention, not on raw display strings and not on a naive all-PC rewrite.

### Fingerprint inputs

Include:

- canonical function/operator IDs;
- relative references;
- absolute references;
- explicit sheet identity or deleted-sheet sentinel;
- range shape;
- named reference identity;
- volatility flags;
- array/spill behavior when supported;
- locale-insensitive normalized function names.

Exclude:

- whitespace/trivia;
- parser source-token decoration;
- display sheet name spelling when a stable sheet identity is known;
- host absolute coordinate for purely relative copied formulas.

## Formula placements

A placement says where a template applies.

```rust
enum FormulaShape {
    RowRun { row: RowPC, col_start: ColPC, len: u32 },
    ColRun { col: ColPC, row_start: RowPC, len: u32 },
    Rect { row_start: RowPC, col_start: ColPC, rows: u32, cols: u32 },
    StridedRect { row_start: RowPC, col_start: ColPC, rows: u32, cols: u32, row_stride: u32, col_stride: u32 },
}

struct FormulaRun {
    id: FormulaRunId,
    template_id: FormulaTemplateId,
    sheet: SheetPC,
    anchor: PhysCoord,
    shape: FormulaShape,
    holes: Option<Bitmap>,
}
```

The exact data structures may be partition-local SegmentVec, sorted run arrays, roaring bitmaps, or bitvecs. Arrow arrays may be used for metadata export/diagnostics, but interval lookup and structural edit handling should use data structures built for that job.

## Exceptions

Exceptions are first-class. They are how the dense and sparse cases coexist.

```rust
struct FormulaExceptionStore {
    formula_overrides: Map<PhysCoord, FormulaTemplateId>,
    value_holes: BitmapOrMap,
    tombstones: BitmapOrMap,
}
```

Example:

```text
C2:C100000 = A+B copied down
C24537 = literal override
C50000 = A*B
```

Representation:

```text
Template T1 = R[0]C[-2] + R[0]C[-1]
Run R1 = T1 over C2:C100000
holes = {C24537, C50000}
ValuePlane[C24537] = literal
Exception[C50000] = T2
```

Lookup order:

```text
1. exception formula/value/tombstone
2. placement run/rect
3. no formula
```

## Sparse vs dense adaptation

Use per-partition heuristics, not a global one-size-fits-all policy.

Suggested initial heuristics:

| Pattern | Representation |
|---|---|
| fewer than ~256 formulas in a partition | sparse exception map is acceptable |
| contiguous same-template length >= 16 or 64 | run |
| rectangular same-template block | rect placement |
| run holes <= 5 percent | run + bitmap holes |
| run holes >= 30 percent | split or demote |
| highly fragmented but few templates | consider dictionary-coded template ID array |

These thresholds are not normative. They should be profiled.

## Evaluation model

Formula evaluation should group work by template and placement when possible:

```text
for each dirty formula run:
  compile/load template once
  resolve input slices/broadcasts
  evaluate batch
  write Arrow result arrays
for each exception:
  evaluate scalar or small batch
```

Input resolution examples:

| Reference type | Batch interpretation |
|---|---|
| relative same-row cell | Arrow slice offset from current run row |
| absolute cell | scalar broadcast |
| bounded range | slice/window over Arrow arrays |
| cross-sheet range | other sheet partition/range slice |
| virtual range | provider scan or projected batch |

This does not require every formula operation to be Arrow-native. It requires the hot data movement and result storage to be columnar where possible, with scalar fallback for unsupported or irregular formulas.

## Dependency model

Formula templates produce dependency templates:

```rust
struct DependencyTemplate {
    cell_refs: SmallVec<[RelativeRef; N]>,
    range_refs: SmallVec<[RelativeRange; N]>,
    names: SmallVec<[NameRef; N]>,
    virtual_refs: SmallVec<[VirtualRangeId; N]>,
    volatility: Volatility,
}
```

Formula placements expand these into partition-level dependency summaries. Use explicit edge-direction vocabulary:

```text
precedent region -> dependent formula placement -> formula result region
```

Example:

```text
FormulaRun T1 at Sheet1!C2:C100000
  precedent regions: Sheet1!A2:A100000 and Sheet1!B2:B100000
  dependent placement: FormulaRun T1 / C2:C100000
  result region: Sheet1!C2:C100000 cached/result plane
  partition edges: A/B row blocks -> C row blocks
```

A formula run is usually a dependent target with respect to its precedents. Its result region may then become a precedent for downstream formulas. Phase 8 APIs should preserve that distinction rather than using a vague `source` term for both sides.

Do not require Phase 8 to materialize one edge per formula cell. Phase 8 should allow dependency edges derived from formula runs/rectangles and virtual ranges.

## Structural edits

Structural edits should update page tables and placement metadata. They should not eagerly rewrite every follower formula. Stored formulas may remain in physical/template space; display materialization relocates through current page tables.

Compaction can bake structural edits into a new canonical core and rebuild optimized placements.

## Save/load implications

### Current beta path

The current beta still uses existing workbook load/save plumbing. Formula Plane V2 is not required for beta.

### Future XLSX ingest

When custom XLSX ingest arrives, it should:

1. preserve native XLSX shared-formula master/follower groups when present;
2. infer template runs by fingerprint when files contain expanded formulas;
3. record exceptions for manually edited cells;
4. build Arrow value/result planes directly;
5. build dependency templates and partition summaries in batch.

### Future save

Save should emit shared formulas for long runs when the writer supports it, with individual formulas/values for exceptions. Umya may remain compatibility output; a future writer/patcher can use FormulaPlaneV2 directly.

## Arrow fit

Use Arrow for:

- literal value arrays;
- cached formula result arrays;
- provider batches;
- save/export buffers;
- diagnostic tables;
- maybe formula-template ID arrays for fragmented dense regions.

Do not force Arrow for:

- AST tree ownership;
- interval/run lookup;
- structural edit page tables;
- dependency graph traversal.

Formula Plane V2 is Arrow-backed where Arrow wins, not Arrow-everywhere.

## Dynamic range coverage

Avoid carrying the legacy mutable-graph name `formula_to_range_deps` into the new substrate. That structure may remain as a compatibility source during migration, but the future concept should be named for its role, for example:

```text
DynamicRangeCoverageIndex
DependencyRangeSummary
RangeDependencyCoverage
```

This index answers: which dependent formula placements/partitions are covered by open-ended, whole-row/whole-column, named, or otherwise dynamic ranges? It is not an engine-live mutation journal and should not imply the old graph ownership model.

## Near-term affordances

Before implementing this model, we should lay down lightweight primitives:

- `FormulaTemplateId`, `FormulaRunId`, `FormulaFingerprint`, `DependencyShapeFingerprint` in `formualizer-eval::formula_plane` while the bridge is experimental; promote only deliberately if a stable cross-crate contract emerges;
- `GridShape`, `GridExtent`, `RangeCardinality` descriptors;
- `PartitionId` compatible with Phase 8;
- `CoreOverlayStorageStats` test/internal hooks in `formualizer-eval`;
- Phase 8 docs that require dependency summaries to support formula runs and virtual ranges.

## Open questions

1. Which fingerprint hash should be standard for stable content identity: xxh3/u128, blake3-derived u128, or an internal deterministic hasher?
2. Should formula runs be stored globally per sheet or partition-local from the start?
3. What is the first production consumer: partition dirty propagation, eval batching, save shared-formula emission, or custom ingest?
4. How should volatile functions interact with template/run grouping?
5. Should dictionary-coded template ID arrays be a real storage class or only a compaction/transient optimization?

## Compatibility rule for Phase 8

Phase 8 must not assume one formula cell equals one independent formula object. It may initially consume current formula snapshots, but its partition/dependency APIs should be able to accept formula placements and virtual references later without a redesign.
