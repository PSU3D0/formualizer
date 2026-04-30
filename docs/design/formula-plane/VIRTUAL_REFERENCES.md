# Virtual References

Status: draft architecture note for future provider-backed references. This is not a beta implementation requirement. It records the long-term model needed for virtual named ranges, SheetPort, plugin-provided data, Parquet/Polars-style native tables, and WASM-safe host providers.

## Motivation

Formualizer needs a reference model that can eventually address data beyond ordinary workbook cells while preserving spreadsheet semantics:

- virtual named references backed by in-process data;
- SheetPort-managed external sheets;
- Parquet/Arrow/Polars-like columnar data;
- plugin-provided tables or scalars;
- WASM host-provided datasets;
- cached immutable snapshots for reproducible evaluation.

The current beta path should not implement these providers. But Phase 8 partitioning and formula fingerprinting must not assume every range source is an Excel sheet rectangle.

## Non-goals

This document does not add:

- a Polars dependency;
- a Parquet reader;
- a plugin ABI;
- a new public API;
- external I/O behavior in WASM;
- streaming XLSX ingest;
- table/structured-reference semantics.

It defines vocabulary and constraints for later work.

## Design principles

1. Virtual reference primitives live below provider implementations.
2. Core formula/dependency logic depends on provider-neutral descriptors, not Polars/Parquet/native file APIs.
3. WASM support is a first-class constraint: providers may be host-backed, capability-limited, or unavailable.
4. Virtual data can be columnar without pretending to be an ordinary worksheet.
5. Dependency/dirty propagation must understand versioned/volatile external sources.
6. Security and determinism require explicit capabilities and source identity.
7. Virtual references should compose with named references and formula templates.

## Conceptual model

```text
VirtualSource
  owns one or more VirtualReferences

VirtualReference
  describes a scalar/range/table/dataframe-like object

Provider
  can read batches/scalars for a VirtualReference

Formula/name resolution
  can bind a name or formula token to a VirtualReferenceDescriptor
```

A virtual reference is not necessarily an Excel sheet. It may have rows/columns, but its shape, volatility, and lookup capabilities are provider-defined.

## Suggested bridge primitives

These should be dependency-light and WASM-safe. For the FormulaPlane bridge phase, keep them in `formualizer-eval::formula_plane` rather than `formualizer-common`; promote only after the stable cross-crate contract is clear.

```rust
pub struct VirtualSourceId(pub u32);
pub struct VirtualRangeId(pub u32);

pub struct VirtualSourceKey {
    pub namespace: String,
    pub name: String,
}

pub struct VirtualProviderVersion {
    pub fingerprint_hi: u64,
    pub fingerprint_lo: u64,
}
```

`VirtualSourceId` and `VirtualRangeId` are session-local handles unless explicitly persisted with a stable key. Persistent formula fingerprints and deterministic snapshots must use stable identity vocabulary:

- `VirtualSourceKey`: provider-neutral persisted source identity;
- virtual reference name/path within that source;
- `VirtualProviderVersion`: provider-supplied content/version token for immutable or versioned data;
- volatility class for providers that cannot provide a stable version.

Do not treat a numeric session-local ID as persisted identity.

pub enum VirtualReferenceKind {
    Scalar,
    Range,
    Column,
    Table,
    DataFrame,
}

pub enum VirtualReferenceVolatility {
    Immutable,
    Versioned,
    Volatile,
}

pub enum VirtualReferenceErrorKind {
    MissingProvider,
    MissingReference,
    ShapeMismatch,
    UnsupportedCapability,
    ProviderError,
}

pub enum RangeCardinality {
    Scalar,
    Bounded { rows: u32, cols: u32 },
    UnboundedRows { cols: u32 },
    UnboundedCols { rows: u32 },
    Unknown,
}
```

These are descriptors and IDs, not data readers.

## Capabilities

Providers should advertise what they can do:

```rust
pub struct VirtualSourceCapabilities {
    pub supports_scalar_lookup: bool,
    pub supports_rect_scan: bool,
    pub supports_column_projection: bool,
    pub supports_predicate_pushdown: bool,
    pub supports_streaming_batches: bool,
    pub supports_snapshot_version: bool,
}
```

Capabilities are important for planning evaluation. A provider that supports column projection can serve formula ranges efficiently. A provider that supports only scalar lookup may be correct but slow.

## Descriptor

```rust
pub struct VirtualReferenceDescriptor {
    pub id: VirtualRangeId,
    pub source: VirtualSourceId,
    pub name: String,
    pub kind: VirtualReferenceKind,
    pub cardinality: RangeCardinality,
    pub volatility: VirtualReferenceVolatility,
    pub capabilities: VirtualSourceCapabilities,
}
```

The descriptor is safe to store in formula/name/dependency metadata. It should not carry native handles, dataframes, file descriptors, or WASM host objects.

## Provider boundary

A future eval-side trait can use provider-neutral descriptors and workbook values:

```rust
trait VirtualRangeProvider {
    fn descriptor(&self, id: VirtualRangeId) -> Option<VirtualReferenceDescriptor>;

    fn read_scalar(&self, id: VirtualRangeId, row: u32, col: u32) -> Result<LiteralValue, ExcelError>;

    fn read_rect(&self, id: VirtualRangeId, rows: Range<u32>, cols: Range<u32>) -> Result<VirtualBatch, ExcelError>;
}
```

The exact trait should remain internal until the plugin story is settled.

Provider failures should be non-panic and classifiable. At the descriptor/eval boundary, missing providers, missing references, shape mismatches, and unsupported capabilities should map to a controlled error category, not a panic and not silent fallback to empty data. Future bindings can translate these categories into user-facing errors.

## Batch representation

Do not force Polars into core. Possible future batch forms:

```rust
enum VirtualBatch {
    Rows(Vec<Vec<LiteralValue>>),
    Arrow(ArrowBatchHandle),
    ProviderNative(ProviderBatchHandle),
}
```

For bridge primitives, avoid Arrow or provider-native types. Within `formualizer-eval`, Arrow-backed batches can be used behind features or internal abstractions at the provider/evaluator boundary.

## Named-reference integration

Virtual references should initially enter formulas through named references:

```text
Name: SalesData
Target: VirtualReferenceDescriptor { kind: DataFrame, ... }
```

Formula examples in future syntax might include:

```text
=SUM(SalesData[amount])
=COUNTROWS(MyParquetTable)
=SheetPort.CustomerLedger[Balance]
```

Structured reference syntax is deferred. The key near-term rule is that named references should not be assumed to resolve only to workbook cell/range/literal/formula definitions.

## Formula fingerprinting interaction

A formula fingerprint that includes virtual refs should use stable virtual identity, not provider memory address.

Include:

- virtual source key or stable source ID;
- virtual reference ID/name;
- projected column/range identity;
- provider version identity when immutable/versioned;
- volatility flag.

For volatile virtual references, eval caches and dependency summaries must treat them as invalidation roots.

## Dependency and dirty propagation

Virtual references become precedent roots. Formula placements depend on them, and provider version changes dirty the dependent formula placements/partitions:

```text
VirtualReference(VirtualRangeId) -> dependent formula placement -> workbook result region
VirtualRangeId version changes -> dirty dependent formula placements and result partitions
```

Phase 8 partitioning should support dependency edges whose precedent is not a workbook partition. Use the same precedent/dependent/result vocabulary as `PHASE_8_COMPATIBILITY_NOTES.md`:

```rust
enum DependencyPrecedentRegion {
    WorkbookPartition(PartitionId),
    VirtualReference(VirtualRangeId),
    VolatileRoot,
}

enum DependencyDependentRegion {
    FormulaCell(PhysCoord),
    FormulaRun(FormulaRunId),
    WorkbookPartition(PartitionId),
}

enum DependencyResultRegion {
    WorkbookCell(PhysCoord),
    WorkbookPartition(PartitionId),
}
```

This is a conceptual shape, not an immediate implementation requirement. The important rule is that `FormulaRun` is a dependent placement with respect to virtual precedents, not the precedent source of the virtual reference.

## Snapshot and determinism model

Virtual references need explicit version semantics.

| Volatility | Semantics |
|---|---|
| Immutable | content never changes for a given source key/version |
| Versioned | provider exposes a version token; changes dirty dependents |
| Volatile | reads may change every evaluation; treat as volatile root |

For beta and deterministic tests, prefer immutable or versioned fixtures. Volatile providers should be opt-in and clearly marked.

A deterministic snapshot record should contain stable source identity plus version token, not a provider memory address:

```text
VirtualSourceKey + virtual reference path/name + VirtualProviderVersion
```

If a provider cannot supply a version token, it should be marked `Volatile` or explicitly treated as non-cacheable.

## WASM constraints

Core descriptors must be WASM-safe:

- no OS file descriptors;
- no mmap assumptions;
- no native thread assumptions;
- no direct Polars dependency;
- no blocking filesystem requirement;
- no host object in serialized formula/core metadata.

A WASM build can provide virtual data through a host callback/provider registry. If no provider is registered, formulas depending on virtual refs should return a controlled error, not panic.

## Plugin constraints

The plugin model should map to the provider boundary:

```text
plugin registers VirtualSource
plugin exposes descriptors/capabilities
engine plans reads using capabilities
plugin supplies scalar or batch data
```

Plugin providers should be sandboxable and deterministic where requested. They should not be allowed to mutate workbook core state directly.

## Security considerations

Virtual source descriptors should separate identity from access:

```text
Descriptor says what the reference is.
Provider registry controls whether it can be read.
```

Do not put secrets, credentials, or direct URLs with credentials in persistent descriptors. Use opaque source keys and host-side resolution.

## Save/load implications

Saving a workbook with virtual references should preserve descriptors/names where supported, not materialize virtual data into ordinary cells unless explicitly requested.

Possible future save modes:

| Mode | Behavior |
|---|---|
| preserve-virtual | save descriptors/names, not data |
| materialize-values | write current virtual values as cells |
| materialize-cache | write cached results plus descriptors |

This is post-beta work.

## Relationship to Arrow

Arrow is a strong fit for virtual data batches and cached/materialized results. It is not required for every provider boundary. A Parquet/Polars provider can expose Arrow batches natively; a WASM host provider may expose row batches first.

The engine should be able to consume either, while preferring Arrow for high-throughput columnar scans.

## Near-term affordances

Before implementation, add only lightweight primitives and docs:

- virtual source/range IDs;
- descriptor shapes;
- cardinality and capability enums;
- dependency-source vocabulary compatible with Phase 8;
- tests/audits ensuring no Polars/Parquet dependency enters eval default features.

## Open questions

1. Should virtual refs be addressable through existing named ranges first, or a new namespace syntax?
2. What is the stable persisted form of a virtual source key?
3. How do we represent provider version tokens without adding provider-specific bytes to bridge metadata or prematurely freezing common-core metadata?
4. Should providers return Arrow batches directly or a neutral batch enum that can wrap Arrow where enabled?
5. Which save mode should be default when a workbook contains virtual refs?
6. How do plugin permissions interact with virtual source registration?

## Compatibility rule for Phase 8

Phase 8 must not assume all dependency sources are workbook cells/ranges. It may initially implement only workbook partitions, but the dependency graph and diagnostics should leave room for `VirtualReference` roots without redesign.
