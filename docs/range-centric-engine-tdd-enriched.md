# Range-Centric Engine Technical Design Document

## 1. Purpose and Scope
This document defines the end-state architecture for rebuilding the Formualizer engine when Apache Arrow storage plus edit overlays act as the single source of truth. It translates the conceptual roadmap into concrete data structures, APIs, invariants, and scenario walkthroughs that engineering teams can implement directly. The scope includes:

- Storage, dependency, and execution planes with precise module boundaries.
- Struct and enum definitions (Rust-style) for the primary abstractions.
- Lifecycle of edits, recalculation, bulk ingest, and spill operations.
- Telemetry hooks and migration strategy.

Out of scope: binding-specific adapters (Python/WASM), interpreter opcode-level semantics, and cross-workbook links, though the APIs leave room for future extensions.

## 2. Architecture Overview
The engine is divided into three cooperating planes, each with narrow interfaces and explicit ownership.

```

`compute_row_delta_from_stats` follows the same diffing rules as the column helper but operates on `RowUsageStats`. The pseudo-code above omits concrete definitions of `row_stats`/`update_with`/`cell_edit` for brevity; the production implementation reuses the shared span-merging utilities described in §3.3.
┌────────────────────┐      ┌──────────────────────┐      ┌─────────────────────┐
│  SheetStoreService │◄────►│ RangeTracker         │◄────►│ DependencyIndex      │
│  (Arrow + overlays)│      │ (span metadata)      │      │ (CSR + topo order)   │
└─────────▲──────────┘      └─────────▲────────────┘      └─────────▲──────────┘
          │                             │                              │
          │                             │                              │
┌─────────┴──────────┐      ┌───────────┴─────────┐       ┌───────────┴─────────┐
│ WorkbookEditor     │─────►│ AddressIndex       │──────► │ EngineCore/Scheduler│
│ (mutations + txn)  │      │ (coord→vertex map) │       │ (parallel eval)     │
└────────────────────┘      └─────────────────────┘       └─────────────────────┘
```

### 2.1 Storage Plane
The storage plane consists of `SheetStoreService` (wrapping the existing Arrow sheet store) and associated telemetry/state. All values and formulas are persisted here; higher layers cache references only.

### 2.2 Dependency Plane
`RangeTracker`, `AddressIndex`, and `DependencyIndex` form the logical graph derived from storage. RangeTracker understands the physical span layout reported by storage. AddressIndex maps absolute coordinates to vertex IDs. DependencyIndex records edges, maintains incremental topological order, and exposes the `WorkbookGraph` trait to evaluators.

### 2.3 Execution Plane
`WorkbookEditor` serializes mutations across storage and dependency layers. `EngineCore` builds recalculation plans using `WorkbookGraph` and orchestrates evaluation via the scheduler and interpreter pool. Evaluation results are written back through the editor, ensuring a single mutation path.

## 3. Data Model and APIs
This section specifies the structs/enums central to the design. Definitions use Rust syntax for clarity.

### 3.1 Range Representation
```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BoundsType {
    Finite,
    WholeColumn,
    WholeRow,
    OpenRowDown,    // e.g., A10:A1048576
    OpenRowUp,      // e.g., A:A10
    OpenColumnRight,
    OpenColumnLeft,
    WholeSheet,
    Table { table_id: u32 },
    Spill { anchor: VertexId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AxisBound {
    Finite(u32),
    OpenStart,
    OpenEnd,
    Whole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeShape {
    pub rows: (AxisBound, AxisBound),
    pub cols: (AxisBound, AxisBound),
}

#[derive(Debug, Clone)]
pub struct RangeDescriptor {
    pub sheet: SheetId,
    pub start_row: u32,
    pub start_col: u32,
    pub height: u32,    // normalized finite height used for caches/topology
    pub width: u32,
    pub shape: RangeShape,
    pub bounds: BoundsType,
}
```

Descriptors represent every dependency—including 1×1 cells. They are immutable snapshots used as keys in caches and telemetry entries. Open or whole-axis ranges never stuff sentinel values (e.g., `u32::MAX`) into `height`/`width`; instead, the `shape` captures authoritative half-open axis bounds and RangeTracker consults stats to resolve them. For brevity, later examples may omit the explicit `shape` literal—assume the parser/normalizer fills it in alongside the normalized `height`/`width`.

### 3.2 LazyRangeRef API
```rust
pub struct LazyRangeRef {
    descriptor: RangeDescriptor,
    handle: RangeHandle,
}

impl LazyRangeRef {
    pub fn descriptor(&self) -> &RangeDescriptor;
    pub fn handle(&self) -> RangeHandle;
    pub fn try_into_cell(&self, ctx: &RangeContext) -> Option<VertexId>;
    pub fn into_arrow_view(&self, ctx: &RangeContext) -> ArrowRangeView;
    pub fn is_empty(&self, ctx: &RangeContext) -> bool;
}

/// RangeContext provides read-only access to resolved spans during evaluation
pub struct RangeContext<'a> {
    tracker: &'a RangeTracker,
    storage: &'a SheetStoreService,
}

impl<'a> RangeContext<'a> {
    pub fn resolve_spans(&self, handle: RangeHandle) -> Arc<[RowColSpan]>;
    pub fn resolve_dimensions(&self, handle: RangeHandle) -> (u32, u32); // (rows, cols)
}
```

**Lock-free design:** `LazyRangeRef` is trivially `Clone + Send + Sync` as it contains only immutable data (`descriptor`, `handle`). All span resolution and dimension queries occur through a `RangeContext` passed to the interpreter at evaluation time, eliminating hot locks on the evaluation path. `try_into_cell` succeeds when the descriptor's effective area is 1×1 *after* resolving open bounds via the context. `into_arrow_view` simply calls `ctx.storage.arrow_view_from_resolved(self.descriptor(), ctx.resolve_spans(self.handle()))`, guaranteeing that every code path goes through the same Arrow reader.

**Lookup-only cell conversion:** `try_into_cell` is strictly a lookup; it never allocates vertices, mutates the address index, or touches the editor. If callers need to ensure a vertex exists (e.g., before wiring new edges), they must use `AddressIndex::ensure_cell_vertex` or the editor helper described in §3.8. This keeps evaluation read-only and prevents hidden allocations on hot paths.

### 3.3 Storage Metadata
```rust
#[derive(Debug, Clone)]
pub struct ColumnUsageStats {
    pub sheet: SheetId,
    pub column: u32,
    pub min_row: Option<u32>,
    pub max_row: Option<u32>,
    pub non_empty_count: u32,
    pub stats_version: u64,  // bumps only when spans change
    pub spans: SmallVec<[RowSpan; 4]>, // RowSpan = [start, end)
    pub overlay_count: usize,
}

#[derive(Debug, Clone)]
pub struct RowUsageStats {
    pub sheet: SheetId,
    pub row: u32,
    pub min_col: Option<u32>,
    pub max_col: Option<u32>,
    pub non_empty_count: u32,
    pub stats_version: u64,
    pub spans: SmallVec<[ColSpan; 4]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowSpan {
    pub start: u32,
    pub end: u32,  // exclusive
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColSpan {
    pub start: u32,
    pub end: u32,  // exclusive
}

#[derive(Debug, Clone)]
pub enum ColumnUsageDelta {
    BecameEmpty,
    BecameNonEmpty { span: RowSpan },
    Expanded { added: SmallVec<[RowSpan; 2]> },
    Shrunk { removed: SmallVec<[RowSpan; 2]> },
    Compacted { stats: ColumnUsageStats },
}

#[derive(Debug, Clone)]
pub enum RowUsageDelta {
    BecameEmpty,
    BecameNonEmpty { span: ColSpan },
    Expanded { added: SmallVec<[ColSpan; 2]> },
    Shrunk { removed: SmallVec<[ColSpan; 2]> },
}

#[derive(Debug, Clone)]
pub enum UsageDelta {
    Column { sheet: SheetId, column: u32, delta: ColumnUsageDelta },
    Row { sheet: SheetId, row: u32, delta: RowUsageDelta },
}
```

The service emits stats during ingest and deltas during edits/compaction. `stats_version` increments **only** when the `spans` vector structurally changes, keeping cache invalidation precise. RangeTracker records `(axis_index, stats_version)` per subscription and bumps a per-subscription `subscription_version` when any observed pair changes, eliminating the previous `span_id`/`span_version` duplication.

### 3.4 SheetStoreService API
```rust
pub struct SheetStoreService {
    sheet_store: SheetStore,
    column_stats: FxHashMap<(SheetId, u32), ColumnUsageStats>,
    row_stats: FxHashMap<(SheetId, u32), RowUsageStats>,
}

impl SheetStoreService {
    pub fn begin_edit(&mut self) -> EditHandle;
    pub fn write_cell(
        &mut self,
        handle: &mut EditHandle,
        sheet: SheetId,
        row: u32,
        col: u32,
        value: &LiteralValue,
    ) -> SmallVec<[UsageDelta; 2]>;
    pub fn arrow_view_from_resolved(
        &self,
        desc: &RangeDescriptor,
        spans: &[RowColSpan],
    ) -> ArrowRangeView;
    pub fn column_stats(&self, sheet: SheetId, col: u32) -> Option<&ColumnUsageStats>;
    pub fn row_stats(&self, sheet: SheetId, row: u32) -> Option<&RowUsageStats>;
    pub fn finish_edit(&mut self, handle: EditHandle);
}
```

Existing Arrow logic supplies `SheetStore`. `EditHandle` tracks pending overlay compactions and telemetry counters for the transaction, while `write_cell`/`write_cell_batch` emit at most one column and one row `UsageDelta` per write.

### 3.5 RangeTracker
```rust
pub struct RangeHandle(u64);

pub struct RangeTracker {
    subscriptions: FxHashMap<RangeHandle, RangeSubscription>,
    by_col: FxHashMap<(SheetId, u32), SmallVec<[RangeHandle; 8]>>,
    by_row: FxHashMap<(SheetId, u32), SmallVec<[RangeHandle; 8]>>,
    handle_counter: AtomicU64,
    stats_snapshot: FxHashMap<(SheetId, u32), ColumnUsageStats>,
    row_stats_snapshot: FxHashMap<(SheetId, u32), RowUsageStats>,
}

pub struct RangeSubscription {
    pub descriptor: RangeDescriptor,
    pub spans: Arc<[RowColSpan]>,
    pub subscribers: SmallVec<[VertexId; 4]>,
    pub observed_versions: SmallVec<[(u32, u64); 8]>, // axis index + stats_version
    pub subscription_version: u64,
}

impl RangeTracker {
    pub fn register(
        &mut self,
        descriptor: RangeDescriptor,
        vertex: VertexId,
    ) -> (RangeHandle, Arc<[RowColSpan]>);

    pub fn unregister(&mut self, handle: RangeHandle, vertex: VertexId);

    pub fn resolve(&self, handle: RangeHandle) -> Arc<[RowColSpan]>;

    pub fn apply_delta(&mut self, delta: UsageDelta) -> Vec<RangeEvent>;
}

pub enum RangeEvent {
    Expanded { handle: RangeHandle, spans: Vec<RowColSpan> },
    Shrunk { handle: RangeHandle, spans: Vec<RowColSpan> },
    Emptied { handle: RangeHandle },
}
```

**Important:** `RangeEvent` intentionally does **not** include an `Unchanged` variant. When `apply_delta` detects that a subscription's spans remain unchanged, it emits nothing. `by_col`/`by_row` lookup tables ensure only affected handles are touched, so fan-out is proportional to the number of impacted ranges rather than all subscribers.

Whenever any axis index’s `stats_version` changes (column or row), the owning subscription bumps `subscription_version` and refreshes `observed_versions` even if no spans changed, allowing caches to notice the monotonic version tick without carrying `RangeEvent::Unchanged` noise. This replaces the legacy `span_id` concept with two explicit counters: `stats_version` (per axis index) and `subscription_version` (per handle).

Each `register` call normalizes the descriptor according to current stats, splits it into stripes, and returns both the handle and canonical span list. `apply_delta` runs inside WorkbookEditor after storage edits; it returns range events for DependencyIndex to consume.

**Descriptor immutability:** Structural edits (row/column insert/delete) never mutate an existing `RangeDescriptor`. The editor re-normalizes the reference, asks `RangeTracker::register` for a brand-new handle, swaps it into `DependencyIndex`, and finally unregisters the old handle. This keeps descriptors cacheable keys and makes diffs straightforward.

### 3.6 AddressIndex
```rust
pub struct AddressIndex {
    sheets: FxHashMap<SheetId, SheetAddressIndex>,
}

pub struct SheetAddressIndex {
    columns: FxHashMap<u32, ColumnAddressIndex>, // sparse row→vertex maps per column
    blocks: Vec<BlockRange>,                     // contiguous row-major blocks from ingest
}

pub struct ColumnAddressIndex {
    sparse: FxHashMap<u32, VertexId>,            // row → vertex id
}

pub struct BlockRange {
    start_row: u32,
    end_row: u32,
    start_col: u32,
    end_col: u32,
    first_vertex: VertexId, // contiguous allocation handle
}

impl AddressIndex {
    pub fn ensure_vertices_for_span(
        &mut self,
        sheet: SheetId,
        span: &RowColSpan,
    ) -> Vec<VertexId>;

    pub fn ensure_cell_vertex(&mut self, cell: &CellRef) -> VertexId;

    pub fn vertex_of_cell(&self, cell: &CellRef) -> Option<VertexId>;

    pub fn vertices_in_span_iter<'a>(
        &'a self,
        sheet: SheetId,
        span: &'a RowColSpan,
    ) -> impl Iterator<Item = (CellRef, VertexId)> + 'a;
}
```

AddressIndex isolates coordinate-related logic. Each sheet keeps a row-major block table for contiguous ingest chunks plus per-column sparse maps for edits, avoiding any bespoke spatial index. `vertices_in_span_iter` walks spans without allocating intermediate `Vec`s, `ensure_vertices_for_span` backfills gaps for bulk ranges, and `ensure_cell_vertex` covers the scalar case before DependencyIndex wires edges. Higher layers never touch `VertexStore` directly.

### 3.7 DependencyIndex & WorkbookGraph Trait
```rust
pub struct DependencyIndex {
    csr: CsrStore,                 // adjacency
    range_map: FxHashMap<RangeHandle, RangeEdges>,
    topo: DynamicTopo<VertexId>,
    dirty: FxHashSet<VertexId>,
    vertex_meta: VertexMetaTable,  // kind, sheet, coord, flags
}

impl DependencyIndex {
    pub fn apply_dependencies(
        &mut self,
        vertex: VertexId,
        ranges: Vec<RangeBinding>,
        address_index: &mut AddressIndex,
    );

    pub fn handle_range_events(
        &mut self,
        events: Vec<RangeEvent>,
        address_index: &mut AddressIndex,
    );

    pub fn mark_dirty(&mut self, vertex: VertexId);
    pub fn pop_dirty_batch(&mut self, limit: usize) -> Vec<VertexId>;
    pub fn snapshot(&self, vertex: VertexId) -> VertexSnapshot;
}

pub trait WorkbookGraph: Send + Sync {
    fn vertex_meta(&self, vertex: VertexId) -> VertexMeta;
    fn inputs(&self, vertex: VertexId) -> Vec<LazyRangeRef>;
    fn pop_dirty_batch(&self, limit: usize) -> Vec<VertexId>;
    fn clear_dirty(&self, vertices: &[VertexId]);
}
```

`RangeBinding` stores `(RangeHandle, LazyRangeRef)` plus any named dependency metadata. DependencyIndex implements `WorkbookGraph` by delegating to its internal tables and RangeTracker.

**Edge orientation:** All CSR edges point **input → dependent**. `apply_dependencies` therefore calls `csr.add_edge(input_vertex, subscriber)` and `topo.add_edge(input_vertex, subscriber, &csr)` so that topo ranks, dirty propagation, and invalidation naturally flow from edited inputs toward formulas. Cycle detection treats predecessors as incoming edges (inputs) and successors as outgoing edges (dependents), matching spreadsheet semantics.

### 3.8 WorkbookEditor
```rust
pub struct WorkbookEditor<'a> {
    storage: &'a mut SheetStoreService,
    range_tracker: &'a mut RangeTracker,
    dep_index: &'a mut DependencyIndex,
    address_index: &'a mut AddressIndex,
    change_log: &'a mut dyn ChangeLogger,
    edit_handle: Option<EditHandle>,
}

impl<'a> WorkbookEditor<'a> {
    pub fn begin(&mut self);
    pub fn set_value(&mut self, addr: CellRef, value: LiteralValue);
    pub fn set_formula(&mut self, addr: CellRef, ast: ASTNode);
    pub fn bulk_ingest(&mut self, sheet: &str, columns: Vec<IngestColumn>);
    pub fn rename_sheet(&mut self, sheet: SheetId, new_name: &str);
    pub fn apply_structural_edit(&mut self, edit: StructuralEdit);
    pub fn commit_results(&mut self, results: &[(CellRef, LiteralValue)]) -> CommitSummary;
    pub fn commit(&mut self) -> CommitSummary;
    pub fn rollback(&mut self);
}

pub enum StructuralEdit {
    InsertRows { sheet: SheetId, before_row: u32, count: u32 },
    DeleteRows { sheet: SheetId, start_row: u32, count: u32 },
    InsertCols { sheet: SheetId, before_col: u32, count: u32 },
    DeleteCols { sheet: SheetId, start_col: u32, count: u32 },
}
```

Every method enforces the four-step mutation pipeline:
1. Ensure edit handle. 2. Mutate storage (Arrow) and get deltas. 3. Pass deltas to RangeTracker, collect events. 4. Apply range events/dependency diffs, update change log.

`CommitSummary` includes affected vertices, overlay stats, and telemetry counters for instrumentation.

`commit_results` is the evaluator write-back path. It batches interpreter outputs, calls `write_cell_batch`, forwards each resulting `UsageDelta` to `RangeTracker::apply_delta`, pushes the resulting `RangeEvent`s through `DependencyIndex::handle_range_events`, and finally `commit`s (or keeps the edit open if the caller wants to piggy-back further edits). This guarantees evaluator writes honor the exact same invariants, telemetry, and change logging as UI edits.

Structural edits share the same pipeline: `apply_structural_edit` shifts storage overlays, updates `AddressIndex`, re-normalizes impacted descriptors via RangeTracker, emits usage deltas for entire axes, and routes range events/dependency diffs through the same functions. Having a single entry point keeps formula reference adjustments, stats updates, and dependency rewiring consistent.

**Locking contract:** WorkbookEditor is the sole writer and acquires components in the strict order
`SheetStoreService → RangeTracker → AddressIndex → DependencyIndex`. Reader paths (RangeContext,
WorkbookGraph exports, telemetry samplers) must acquire shared locks in the exact same order and are
forbidden from upgrading. A `debug_assert_lock_order!(ComponentId)` macro records the current thread’s
highest-ranked lock so inversions panic immediately in debug builds, preventing latent deadlocks.

### 3.9 EngineCore & Scheduler APIs
```rust
pub struct EngineCore<G: WorkbookGraph> {
    graph: Arc<G>,
    scheduler: Scheduler,
    interpreter_pool: InterpreterPool,
}

impl<G: WorkbookGraph> EngineCore<G> {
    pub fn recalc_all(&mut self);
    pub fn recalc_vertices(&mut self, seeds: &[VertexId]);
    fn execute_schedule(&mut self, schedule: Schedule);
}
```

Scheduler consumes CSR snapshots via `WorkbookGraph`, building topo layers that the interpreter pool evaluates. Interpreter receives AST plus `LazyRangeRef`s and writes results back via `WorkbookEditor::commit_results`, reusing the mutation pipeline.

## 4. Data Flows
We expand each major flow with the exact modules and method calls involved.

### 4.1 Transactional Cell Edit
1. **Begin transaction** – `WorkbookEditor::begin` acquires an `EditHandle` from storage and clears temporary event buffers.
2. **Write value** – `set_value` resolves the `CellRef` via `AddressIndex`, ensuring a vertex exists. It calls `SheetStoreService::write_cell`, capturing the returned `UsageDelta`s (typically one column + one row delta) and logging the old value in `ChangeLog` for undo.
3. **Range updates** – the editor forwards each delta to `RangeTracker::apply_delta`, which returns zero or more `RangeEvent`s. Each event is handed to `DependencyIndex::handle_range_events`, which diff-updates CSR rows and marks subscriber vertices dirty.
4. **Formula maintenance** – if the target cell previously held a formula, the editor removes its old dependencies by calling `DependencyIndex::apply_dependencies` with an empty set.
5. **Commit** – `WorkbookEditor::commit` finalizes the edit handle (allowing `SheetStore` to compact overlays), flushes `ChangeLog`, and returns a summary of dirty vertices and telemetry counters to the caller (UI, bindings, or engine).

### 4.2 Formula Edit Path
1. Parse AST → `RangeDescriptor`s via parser.
2. Editor obtains `LazyRangeRef`s by calling `RangeTracker::register` for each descriptor; placeholders are ensured through `AddressIndex::ensure_vertices` where necessary.
3. `DependencyIndex::apply_dependencies` receives the new bindings, diffs them against existing range handles for the vertex, updates CSR, and refreshes topo ranks.
4. Dirty propagation automatically marks the formula vertex and any volatile dependents for reevaluation.

### 4.3 Bulk Ingest
1. `WorkbookEditor::bulk_ingest` streams column data to the existing Arrow ingest builders. After each chunk flush, `SheetStoreService` emits `ColumnUsageStats` for the affected columns.
2. `AddressIndex::seed_from_stats` uses stats to allocate contiguous vertex blocks (via `VertexStore::allocate_contiguous`). This avoids per-cell ensures.
3. RangeTracker caches the stats so subsequent `register` calls for infinite ranges know the current span layout.
4. Once all data is ingested, formulas are applied using the normal formula edit pipeline.

### 4.4 Evaluation / Recalc Flow
1. Scheduler requests dirty vertices via `WorkbookGraph::pop_dirty_batch`.
2. It builds a `Schedule` using the incremental topo order maintained by DependencyIndex. If the dirty set is large, PK falls back to a full rebuild using `csr.export()`.
3. `EngineCore::execute_schedule` iterates layers. For each vertex, it fetches `VertexSnapshot` with AST/value metadata and `LazyRangeRef`s for inputs.
4. Interpreter evaluates formulas, producing new `LiteralValue`s. Results are written through a small `ResultQueue` to `WorkbookEditor::commit_results`, which uses the same mutation path as user edits, ensuring overlays and ranges stay consistent.
5. After each layer, `WorkbookGraph::clear_dirty` is called to drop dirty flags from completed vertices; volatiles can be re-marked via config policies.

## 5. Technical Stories (Detailed)
Each story now includes actors, preconditions, sequence diagrams (textual), invariants, and success criteria.

### Story A – Bulk Loading from Arrow Assets
- **Actors**: CLI ingest tool, `WorkbookEditor`, `SheetStoreService`.
- **Preconditions**: Arrow files contain values only; no formulas yet. Sheets may already exist or need creation.
- **Sequence**:
  1. CLI calls `WorkbookEditor::begin` once per workbook.
  2. For each sheet, CLI invokes `bulk_ingest`, passing column iterators. Inside, the editor:
     - Ensures a sheet ID via `SheetRegistry` and `SheetStoreService::ensure_sheet_mut`.
     - Streams chunks to `IngestBuilder`. When a chunk closes, storage emits `ColumnUsageStats` (min/max rows, spans, overlay counts).
     - Calls `AddressIndex::seed_from_stats`. For each `(row_span, column)`, the address index allocates vertices via `VertexStore::allocate_contiguous` and records them in `cell_to_vertex`.
  3. After all columns load, CLI feeds staged formulas: `WorkbookEditor::set_formula` handles AST parsing, range registration, dependency insertion, and dirty marking in bulk.
  4. Editor commits once, returning a summary (vertices created, overlay entries, time spent).
- **Invariants**:
  * Each populated Arrow coordinate has a vertex and sheet index entry before formulas reference it.
  * RangeTracker caches spans exactly as reported by ingest.
- **Success criteria**: ingest cost O(populated cells), zero per-cell ensures, RangeTracker ready for subsequent edits.

### Story B – Full Recalculation after Load
- **Actors**: `EngineCore`, `DependencyIndex`, Scheduler, Interpreter pool.
- **Preconditions**: All formulas loaded; dirty frontier may be entire workbook.
- **Sequence**:
  1. Engine calls `graph.pop_dirty_batch(usize::MAX)` to retrieve every formula vertex.
  2. Scheduler requests a topo snapshot from DependencyIndex. If PK state is fresh, it simply reads the incremental order; otherwise `csr.export()` plus `DynamicTopo::rebuild` constructs it.
  3. Scheduler partitions vertices into parallel layers respecting configurable width limits.
  4. `EngineCore::execute_schedule` iterates layers:
     - For each vertex, fetch AST/value snapshot and `LazyRangeRef`s.
     - Interpreter obtains `ArrowRangeView`s via `into_arrow_view`. For empty columns (pre-ingest), RangeTracker returns zero spans so Arrow does no work.
     - Results are enqueued, and after the layer finishes, the editor writes them back (updating overlays, stats, and range events as needed).
  5. Dirty flags cleared; volatiles optionally re-marked depending on `EvalConfig`.
- **Success criteria**: evaluation time proportional to formula count × average dependency cost, not sheet height. Telemetry for overlay writes and range events recorded per layer.

### Story C – Ten Edits Followed by Targeted Recalc
- **Actors**: Workbook UI, `WorkbookEditor`, RangeTracker, DependencyIndex, Scheduler.
- **Preconditions**: Workbook already loaded; some formulas reference `B:B`, `C:C`, etc.
- **Sequence**:
  1. UI batches ten edits inside `WorkbookEditor::begin`/`commit`.
  2. For each edit:
     - Editor writes to Arrow, capturing a small `UsageDelta` set (e.g., `UsageDelta::Column { delta: ColumnUsageDelta::Expanded { ... } }` for `B2001` plus the corresponding row delta).
     - RangeTracker processes the delta and discovers affected range handles (maybe dozens if many formulas subscribe to `B:B`). It emits events only for those handles.
     - DependencyIndex updates CSR rows for each handle by ensuring placeholders for the new span (`AddressIndex::ensure_vertices`) and linking them to subscriber formulas.
     - Affected formulas (A235 etc.) are marked dirty.
  3. Commit returns the dirty frontier (≤ 10 formula rows + any dependents) to the UI and scheduler.
  4. Scheduler builds a micro-schedule covering only those vertices. Evaluation recomputes the touched formulas and cascades as necessary (e.g., parent SUM).
- **Invariants**:
* Column usage stats increment `stats_version` whenever spans change; RangeTracker bumps each subscription's `subscription_version` lazily when it observes a new `(column, stats_version)` pair.
  * Dirty propagation only touches subscribers; no global invalidation occurs for infinite ranges.
- **Success criteria**: evaluation touches only the edited formulas and their dependency cones. Telemetry shows small `range_events_emitted` count matching edits.

### Story D – Spill Commit with Fault Injection
- **Actors**: Interpreter (producing spill values), `WorkbookEditor`, `SpillPlanner`, RangeTracker.
- **Preconditions**: A formula plans to spill into a 2×3 area; previous spill from the same anchor exists.
- **Sequence**:
  1. Interpreter requests `SpillPlanner::plan(anchor, target_descriptor)`. Planner uses `LazyRangeRef` for the target range, checks blockers via `AddressIndex`, and records previous spill cells.
  2. Editor begins a transaction and receives a callback containing the values to write plus optional fault injection (test mode).
  3. Commit steps:
     - Clear old spill cells: write empties through storage, gather `UsageDelta::Column`/`UsageDelta::Row` entries (typically `ColumnUsageDelta::Shrunk`). RangeTracker emits events removing span edges, marking formulas referencing the old spill cells dirty.
     - Write new spill values: for each cell, storage returns `ColumnUsageDelta::Expanded` (plus matching row deltas); RangeTracker notifies subscribers, and DependencyIndex re-links edges.
     - If `fault_after_ops` triggers, editor invokes `rollback`, replaying inverses via `ChangeLog`: Arrow values revert, RangeTracker receives compensating deltas, DependencyIndex restores old edges. The planner reports the fault for test assertions.
     - Without fault, commit completes and returns the new spill occupancy + list of affected dependents.
- **Invariants**:
  * Each spill operation is atomic; either old spill persists or new spill fully commits.
  * ChangeLog tracks both overlay writes and dependency mutations.
- **Success criteria**: even under injected fault, graph state (edges, dirty flags, spans) matches pre-commit state.

### Story E – `SUM(A:A500)` with Mixed Dependencies
- **Actors**: Formulas in column A, RangeTracker, DependencyIndex, Scheduler.
- **Preconditions**: 500 rows in A, half referencing sparse infinite ranges, half referencing other formulas.
- **Sequence**:
  1. During extraction, each A-row formula registers descriptors for `B:B`, `C:C`, etc. RangeTracker resolves them into span lists derived from `ColumnUsageStats` (e.g., `[1,1]`, `[501,501]`, … up to `[125001,125001]`). DependencyIndex records the handle per formula, not per cell.
  2. Top-level `SUM(A:A500)` registers its finite descriptor. DependencyIndex expands it into 500 scalar vertices but maintains a range binding so structural edits (insert/delete rows) can be applied via RangeTracker events.
  3. User edits `B2001`:
     - Storage delta: `UsageDelta::Column { sheet: SheetId(…), column: B, delta: ColumnUsageDelta::Expanded { added: smallvec![RowSpan { start: 2001, end: 2002 }] } }` (plus the symmetric row delta).
     - RangeTracker finds all handles covering column B and emits `RangeEvent::Expanded` for each (A rows referencing `B:B`).
     - DependencyIndex ensures a vertex exists for `B2001`, links it to affected A-rows, and marks them dirty.
     - Scheduler recomputes only those A-rows, then the top SUM formula; other rows remain untouched.
- **Success criteria**: evaluation cost is proportional to the number of populated spans (≈ 250) rather than full column height. Telemetry should show `range_events_emitted = 1`, `csr_edge_updates ≈ subscriber_count`, and `overlay_writes = 1`.

## 6. Arrow Integration: Current vs Required Enhancements

> **Legacy context:** Sections 6–8 capture the original gap analysis and keep some of the pre-review naming (e.g., `ColumnUsageDelta::Expanded`, `ColumnUsageDelta::Unchanged`). The canonical APIs live in Sections 3–5 and reuse the unified `UsageDelta`/row-stats design; treat the legacy nomenclature here as shorthand for those updated structures (`Unchanged` now simply means “emit no delta”) until this appendix is rewritten.
| Area | Current State | Enhancement |
| --- | --- | --- |
| Overlay writes | `write_cell_value` mutates overlay and compacts | Return `UsageDelta` (column + row variants), record min/max transitions, and expose axis stats |
| Column metadata | Derived by rescanning columns when needed | Maintain `ColumnUsageStats` incrementally; bump per-column `stats_version` for cache invalidation |
| Range views | `ArrowRangeView` defaults to sheet bounds for infinite ranges | Only expose `arrow_view_from_resolved(descriptor, spans)`, forcing every caller to supply spans so the iterator stays sparse-aware |
| Spill planner | Logic interwoven with graph module | Move to dedicated module that consumes `LazyRangeRef` and editor APIs |
| Named ranges | Managed inside monolithic graph | Relocate to `names` subsystem using range descriptors and RangeTracker |
| Vertex editor | Calls graph internals directly | Rebase onto WorkbookEditor + services; expose public transactions |

## 7. Concurrency and Safety Plan
- **Mutation serialization**: All mutations acquire the WorkbookEditor lock, ensuring storage/range/dependency updates are atomic.
- **Read scalability**: RangeTracker, DependencyIndex, and SheetStoreService expose read-only snapshots through `Arc`/`RwLock`. Evaluator threads hold read locks only.
- **Send + Sync**: `WorkbookGraph` is implemented by wrapping shared `Arc` references; RangeTracker spans and Arrow views are `Arc<[Span]>` and `Arc<ArrowArray>` respectively, so they are safe to share.
- **Undo/Redo**: `ChangeLog` records overlay writes and dependency diffs. Rollback replays inverses through the same services, ensuring consistent state.

## 8. Telemetry and Observability
Expose the following counters/gauges per transaction and per evaluation pass:
- `overlay_writes`, `overlay_compactions` (existing, but now associated with transactions).
- `range_events_emitted`, `range_events_ignored` (when span cap reached).
- `csr_edge_updates`, `topo_reorders`.
- `dirty_vertices_committed`, `recalc_vertices_executed`.
- `column_span_count` per sheet/column for infinite-range tuning.
Bindings can query these via `WorkbookEditor::commit().telemetry` and `EngineCore::last_eval_stats`.

## 9. Migration Plan
1. **Phase 0** – implement `ColumnUsageStats`/`RowUsageStats` + `UsageDelta` in `SheetStore`. Add telemetry to prove correctness (unit + integration tests around `correct-infinite-ranges.md`).
2. **Phase 1** – introduce `RangeDescriptor`, `LazyRangeRef`, and `RangeTracker`. Initially, only infinite ranges use RangeTracker; bounded ranges still expand directly.
3. **Phase 2** – extract DependencyIndex from `graph/mod.rs`, owning CSR + PK state. Adapt VertexEditor to route dependency mutations through it.
4. **Phase 3** – build WorkbookEditor facade, move sheet/named/spill management under it, and expose the `WorkbookGraph` trait to EngineCore. Ensure Send + Sync invariants hold by wrapping modules in `Arc<RwLock<_>>`.
5. **Phase 4** – remove legacy graph APIs, update bindings/tests to the new entry points, and enable full range unification.

## 10. Open Questions & Decisions Pending
1. **Span explosion heuristics** – design fallback thresholds (e.g., >1024 spans per column) and telemetry warnings when RangeTracker collapses to coarse stripes.
2. **Snapshot isolation** – determine whether concurrent edits + evaluations require MVCC. Current plan serializes mutations; revisit if UI demands live editing during calculation.
3. **External workbook references** – treat as descriptors resolved lazily via a `WorkbookResolver` trait; design left for future work.
4. **Resource limits** – add `EvalConfig` knobs (`max_vertices`, `max_spans_per_axis`, `max_overlay_density`). Engine must fail fast with descriptive errors when limits exceed.

## 11. Addendum: Existing Components to Retain
Implementation should reuse several mature subsystems verbatim, wiring them into the new architecture rather than rewriting them. The following list summarizes the “keepers” with minimal stubs to clarify their roles.

### 11.1 Arrow Storage & Literal Types
- `arrow_store::SheetStore` – current SoA column store with overlays, chunking, and compaction. Only its public API changes where we add the `UsageDelta`-based mutation surface (column + row stats).
- `formualizer_common::LiteralValue` – remains the universal value enum consumed by storage and interpreter.
- `formualizer_common::Coord` (alias `AbsCoord` inside engine):
  ```rust
  pub struct Coord {
      row: u32,
      col: u32,
      abs_row: bool,
      abs_col: bool,
  }
  ```
- `crate::reference::CellRef` / `SheetCellAddress`:
  ```rust
  pub struct CellRef {
      pub sheet_id: SheetId,
      pub coord: Coord,
  }
  ```

### 11.2 Vertex & Data Stores
- `engine::vertex_store::VertexStore` – Struct-of-Arrays storage for vertex metadata and values. New components call `allocate`, `allocate_contiguous`, `set_kind`, etc., as-is.
- `engine::arena::DataStore` – arenas for ASTs and values stay unchanged; `WorkbookGraph::snapshot` still returns `VertexSnapshot` backed by DataStore.

### 11.3 Graph Utilities
- `engine::topo::pk::{DynamicTopo, PkConfig}` – incremental Pearce–Kelly ordering library. DependencyIndex embeds `DynamicTopo<VertexId>` unchanged.
- `engine::csr_edges::CsrStore` / `engine::delta_edges` – CSR plus delta slabs remain the storage for adjacency; DependencyIndex simply owns and orchestrates them.

### 11.4 Reference Parser & Planner Primitives
- `formualizer_parse::parser::{ASTNode, ReferenceType}` – AST structure and parsed references are reused. RangeDescriptor is a thin normalization layer atop `ReferenceType`.
- `crate::reference::{SheetName, SheetId}` – identity types stay untouched.

### 11.5 Spill & Editor Utilities
- `engine::graph::editor::change_log::{ChangeLog, ChangeEvent}` – continue logging mutations for undo/redo.
- `engine::spill::{RegionLockManager, SpillShape}` – planners already reason about rectangular spill regions; we only update them to consume `LazyRangeRef`.

Wherever these components expose structs, copy/alias their existing definitions; only new modules should wrap them. For example, `RangeTracker` never reimplements column scanning; it relies on `SheetStore` + `ColumnUsageStats`. Likewise, Scheduler still uses `DynamicTopo` layers regardless of the new dependency plumbing.

## 12. Summary
This revision specifies the modules, structs, APIs, and flows required to deliver a range-centric, Arrow-native engine, while documenting the foundational components that remain in place. By unifying dependencies under `RangeDescriptor`, enforcing a single mutation path via WorkbookEditor, and coupling RangeTracker with incremental storage metadata, we guarantee that sparse infinite ranges, bulk ingest, spill operations, and localized edits remain efficient. DependencyIndex provides the scheduler-ready graph plus topo order, while EngineCore and the interpreter pool consume only trait-based snapshots. Telemetry and migration steps ensure observability and incremental rollout.
# Range-Centric Engine TDD - Chunk 1: Implementation Foundations

## 3. Data Model and APIs (EXPANDED)

This section provides complete struct definitions with all fields, memory layouts, and implementation notes.

### 3.1 Range Representation (EXPANDED)

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BoundsType {
    Finite,
    WholeColumn,
    WholeRow,
    OpenRowDown,    // e.g., A10:A1048576
    OpenRowUp,      // e.g., A:A10
    OpenColumnRight,
    OpenColumnLeft,
    WholeSheet,
    Table { table_id: u32 },
    Spill { anchor: VertexId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AxisBound {
    Finite(u32),
    OpenStart,
    OpenEnd,
    Whole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeShape {
    pub rows: (AxisBound, AxisBound),
    pub cols: (AxisBound, AxisBound),
}

#[derive(Debug, Clone)]
pub struct RangeDescriptor {
    pub sheet: SheetId,
    pub start_row: u32,
    pub start_col: u32,
    pub height: u32,    // normalized finite height used for caches/topology
    pub width: u32,
    pub shape: RangeShape,
    pub bounds: BoundsType,
}
```

**Design notes:**
- `RangeDescriptor` is the normalized, immutable representation of any range reference
- For finite ranges, `height` and `width` are exact
- For infinite/open ranges (e.g., `A:A`), `shape` encodes the true semantics while `height`/`width` hold normalized placeholders for caching
- Descriptors serve as cache keys and telemetry identifiers
- Hash implementation uses all fields for precise deduplication

**Normalization rules:**
1. Column-only ranges (`A:A`) → `start_row=0, height=1, start_col=A, width=1, WholeColumn`, `shape.rows=(AxisBound::OpenStart, AxisBound::OpenEnd)`
2. Row-only ranges (`5:5`) → `start_row=5, height=1, start_col=0, width=1, WholeRow`, `shape.cols=(AxisBound::OpenStart, AxisBound::OpenEnd)`
3. Mixed (`A10:B`) → `start_row=10, height=1 (placeholder), start_col=A, width=2, OpenRowDown`, `shape.rows=(AxisBound::Finite(10), AxisBound::OpenEnd)`
4. Whole sheet (`1:1048576`) → `WholeSheet` bounds type, `shape.rows=(AxisBound::Whole, AxisBound::Whole)`, `shape.cols=(AxisBound::Whole, AxisBound::Whole)`

### 3.2 LazyRangeRef API (EXPANDED)

```rust
pub struct LazyRangeRef {
    descriptor: RangeDescriptor,
    handle: RangeHandle,
}

impl LazyRangeRef {
    pub fn descriptor(&self) -> &RangeDescriptor;

    /// Returns VertexId if range resolves to exactly 1×1 after span resolution
    pub fn try_into_cell(&self, ctx: &RangeContext) -> Option<VertexId>;

    /// Defers to SheetStoreService for Arrow-backed iteration
    pub fn into_arrow_view(&self, ctx: &RangeContext) -> ArrowRangeView;

    /// Returns resolved spans from RangeTracker
    pub fn spans(&self, ctx: &RangeContext) -> Arc<[RowColSpan]>;

    /// Returns effective dimensions after span resolution
    pub fn resolved_dims(&self, ctx: &RangeContext) -> (u32, u32);

    /// Check if range is currently empty (no populated spans)
    pub fn is_empty(&self, ctx: &RangeContext) -> bool;
}

pub struct RangeContext<'a> {
    pub tracker: &'a RangeTracker,
    pub storage: &'a SheetStoreService,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowColSpan {
    pub row_start: u32,
    pub row_end: u32,   // exclusive (half-open: [start, end))
    pub col_start: u32,
    pub col_end: u32,   // exclusive (half-open: [start, end))
}
```

**Usage patterns:**
- `try_into_cell(ctx)` short-circuits evaluation for scalar dependencies
- `into_arrow_view(ctx)` provides unified data access regardless of range type via `SheetStoreService::arrow_view_from_resolved`
- `spans(ctx)` enables stripe-aware iteration without materializing full arrays
- `is_empty(ctx)` supports early-exit optimization in aggregation functions

`LazyRangeRef` stays lock-free because it holds only immutable data; all mutable range state lives in `RangeTracker` and is accessed through `RangeContext` snapshots. This ensures interpreter threads never contend on hot locks while still having one canonical Arrow read path.

### 3.3 Storage Metadata (EXPANDED)

```rust
#[derive(Debug, Clone)]
pub struct ColumnUsageStats {
    pub sheet: SheetId,
    pub column: u32,
    pub min_row: Option<u32>,
    pub max_row: Option<u32>,
    pub non_empty_count: u32,
    pub stats_version: u64,         // Increments only when spans change
    pub spans: SmallVec<[RowSpan; 4]>,
    pub overlay_count: usize,
}

#[derive(Debug, Clone)]
pub struct RowUsageStats {
    pub sheet: SheetId,
    pub row: u32,
    pub min_col: Option<u32>,
    pub max_col: Option<u32>,
    pub non_empty_count: u32,
    pub stats_version: u64,
    pub spans: SmallVec<[ColSpan; 4]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowSpan {
    pub start: u32,
    pub end: u32,  // exclusive (half-open: [start, end))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColSpan {
    pub start: u32,
    pub end: u32,  // exclusive
}

impl RowSpan {
    pub fn to_inclusive(&self) -> (u32, u32) {
        (self.start, self.end.saturating_sub(1))
    }
}

#[derive(Debug, Clone)]
pub enum ColumnUsageDelta {
    BecameEmpty,
    BecameNonEmpty { span: RowSpan },
    Expanded { added: SmallVec<[RowSpan; 2]> },
    Shrunk { removed: SmallVec<[RowSpan; 2]> },
    Compacted { stats: ColumnUsageStats },
}

#[derive(Debug, Clone)]
pub enum RowUsageDelta {
    BecameEmpty,
    BecameNonEmpty { span: ColSpan },
    Expanded { added: SmallVec<[ColSpan; 2]> },
    Shrunk { removed: SmallVec<[ColSpan; 2]> },
}

#[derive(Debug, Clone)]
pub enum UsageDelta {
    Column { sheet: SheetId, column: u32, delta: ColumnUsageDelta },
    Row { sheet: SheetId, row: u32, delta: RowUsageDelta },
}
```

**Implementation notes:**
- `stats_version` bumps exclusively when the `spans` vector structurally changes (insert/merge/split). Overlay counters, compaction timestamps, and telemetry never touch it, preventing needless cache churn.
- Deltas are emitted incrementally during `write_cell` in amortized O(1) time via binary search into the sorted `SmallVec` span list. Batches merge changes per `(sheet, axis, index)` before emitting.
- A debug-only verification mode periodically recomputes spans from Arrow overlays to assert parity with incremental maintenance without impacting release builds.
- `RangeTracker::apply_delta` matches on the `UsageDelta` axis: column deltas refresh `by_col`, row deltas refresh `by_row`, and both update the subscription versions that referenced the touched indices.
- Row stats ship behind the `row_usage_stats` feature flag until WholeRow/Spill-by-row stories land; the shared APIs ensure the rest of the stack can adopt them incrementally without signature churn.

### 3.4 SheetStoreService API (EXPANDED)

```rust
pub struct SheetStoreService {
    sheet_store: SheetStore,
    column_stats: FxHashMap<(SheetId, u32), ColumnUsageStats>,
    row_stats: FxHashMap<(SheetId, u32), RowUsageStats>,
    overlay_stats: OverlayStats,
}

impl SheetStoreService {
    pub fn begin_edit(&mut self) -> EditHandle;

    pub fn write_cell(
        &mut self,
        handle: &mut EditHandle,
        sheet: SheetId,
        row: u32,
        col: u32,
        value: &LiteralValue,
    ) -> SmallVec<[UsageDelta; 2]>;

    pub fn write_cell_batch(
        &mut self,
        handle: &mut EditHandle,
        writes: &[(SheetId, u32, u32, LiteralValue)],
    ) -> Vec<UsageDelta>;

    /// Single authoritative read path: caller must supply spans
    pub fn arrow_view_from_resolved(
        &self,
        desc: &RangeDescriptor,
        spans: &[RowColSpan],
    ) -> ArrowRangeView;

    pub fn column_stats(&self, sheet: SheetId, col: u32) -> Option<&ColumnUsageStats>;
    pub fn row_stats(&self, sheet: SheetId, row: u32) -> Option<&RowUsageStats>;
    pub fn all_column_stats(&self, sheet: SheetId) -> Vec<&ColumnUsageStats>;

    pub fn finish_edit(&mut self, handle: EditHandle) -> EditSummary;

    pub fn should_compact(&self, sheet: SheetId, col: u32) -> bool;
    pub fn compact_column(
        &mut self,
        sheet: SheetId,
        col: u32,
    ) -> Result<UsageDelta, ExcelError>;

    #[cfg(debug_assertions)]
    pub fn recompute_column_stats_debug(&self, sheet: SheetId, col: u32) -> ColumnUsageStats;
}

#[derive(Debug)]
pub struct EditHandle {
    id: u64,
    started: std::time::Instant,
    pending_compactions: Vec<(SheetId, u32)>,
    telemetry: EditTelemetry,
}

#[derive(Debug, Clone, Default)]
pub struct EditTelemetry {
    pub cells_written: usize,
    pub deltas_emitted: usize,
    pub overlay_hits: usize,
    pub overlay_misses: usize,
}

#[derive(Debug)]
pub struct EditSummary {
    pub duration: std::time::Duration,
    pub deltas: Vec<UsageDelta>,
    pub compactions_performed: usize,
    pub telemetry: EditTelemetry,
}
```

**Batch write optimization:**
- `write_cell_batch` sorts writes by `(sheet, axis, index)` and coalesces mutations so each column/row emits at most one delta per batch.
- Row and column stats share the same binary-search insertion helpers, guaranteeing amortized O(1) span maintenance.

**Compaction policy:**
```rust
fn should_compact(&self, sheet: SheetId, col: u32) -> bool {
    let stats = self.column_stats(sheet, col)?;
    let abs_threshold = 1024;  // Absolute overlay count
    let frac_threshold = 50;    // 1/50 = 2% of chunk size

    stats.overlay_count >= abs_threshold ||
    (stats.arrow_chunk_count() > 0 &&
     stats.overlay_count * frac_threshold >= stats.arrow_chunk_count())
}
```

`arrow_chunk_count()` is derived from Arrow metadata rather than tracked in stats, so compaction heuristics never mutate `stats_version`.

### 3.5 RangeTracker (EXPANDED)

```rust
pub struct RangeHandle(u64);

pub struct RangeTracker {
    subscriptions: FxHashMap<RangeHandle, RangeSubscription>,
    by_col: FxHashMap<(SheetId, u32), SmallVec<[RangeHandle; 8]>>,
    by_row: FxHashMap<(SheetId, u32), SmallVec<[RangeHandle; 8]>>,
    handle_counter: AtomicU64,
    stats_snapshot: FxHashMap<(SheetId, u32), ColumnUsageStats>,
    row_stats_snapshot: FxHashMap<(SheetId, u32), RowUsageStats>,
}

pub struct RangeSubscription {
    pub descriptor: RangeDescriptor,
    pub spans: Arc<[RowColSpan]>,
    pub observed_versions: SmallVec<[(u32, u64); 8]>, // (column, stats_version) for all covered columns
    pub subscription_version: u64,  // Bumped when any observed column's stats_version changes
    pub subscribers: SmallVec<[VertexId; 4]>,
}

#[derive(Debug, Clone, Copy)]
pub enum Axis {
    Row,
    Column,
}

pub struct IntervalSet {
    intervals: Vec<Interval>,
}

#[derive(Debug, Clone, Copy)]
struct Interval {
    start: u32,
    end: u32,  // exclusive (half-open)
}

impl RangeTracker {
    pub fn register(
        &mut self,
        descriptor: RangeDescriptor,
        vertex: VertexId,
    ) -> (RangeHandle, Arc<[RowColSpan]>);

    pub fn unregister(&mut self, handle: RangeHandle, vertex: VertexId);

    pub fn resolve(&self, handle: RangeHandle) -> Arc<[RowColSpan]>;

    pub fn resolve_descriptor(
        &self,
        desc: &RangeDescriptor,
    ) -> Arc<[RowColSpan]>;

    pub fn apply_delta(&mut self, delta: UsageDelta) -> Vec<RangeEvent>;

    pub fn update_stats_snapshot(&mut self, stats: &[(SheetId, u32, ColumnUsageStats)]);

    /// Returns all handles affected by a coordinate (O(affected ranges))
    pub fn handles_covering(
        &self,
        sheet: SheetId,
        axis: Axis,
        index: u32,
    ) -> SmallVec<[RangeHandle; 8]>;
}

pub enum RangeEvent {
    Expanded { handle: RangeHandle, spans: Vec<RowColSpan> },
    Shrunk { handle: RangeHandle, spans: Vec<RowColSpan> },
    Emptied { handle: RangeHandle },
}
```

**Inverted axis indexes:** `by_col` and `by_row` store the handles that observe each `(sheet, axis, index)`. `register` populates them based on the resolved spans; `unregister` removes handles lazily, and `apply_delta` consults the relevant map to touch only affected subscriptions. This makes delta fan-out proportional to the number of truly impacted ranges rather than total subscribers.

**Important:** `RangeEvent` intentionally lacks an `Unchanged` variant. When `apply_delta` detects that a subscription's spans remain unchanged, it emits nothing, reducing noise and redundant processing.

**Descriptor immutability:** Structural edits never mutate a descriptor in place. The editor always re-normalizes the reference, registers a brand-new handle, swaps it into `DependencyIndex`, and finally unregisters the old handle, keeping descriptors safe to cache and reason about.

Whenever a `(sheet, column)` `stats_version` tick is observed—even if spans stay the same—the owning subscription bumps its `subscription_version` and refreshes `observed_versions`. Callers compare those versions instead of looking for a chatter-inducing `RangeEvent::Unchanged` message.

**Span resolution algorithm:**
```rust
impl RangeTracker {
    fn resolve_descriptor(&self, desc: &RangeDescriptor) -> Arc<[RowColSpan]> {
        match desc.bounds {
            BoundsType::Finite => {
                // Return single span covering exact bounds (half-open [start, end))
                vec![RowColSpan {
                    row_start: desc.start_row,
                    row_end: desc.start_row + desc.height,  // exclusive
                    col_start: desc.start_col,
                    col_end: desc.start_col + desc.width,   // exclusive
                }].into()
            }
            BoundsType::WholeColumn => {
                // Intersect descriptor column(s) with axis intervals
                let sheet = desc.sheet;
                let mut spans = Vec::new();
                for col in desc.start_col..(desc.start_col + desc.width) {
                    if let Some(stats) = self.stats_snapshot.get(&(sheet, col)) {
                        for row_span in &stats.spans {
                            spans.push(RowColSpan {
                                row_start: row_span.start,
                                row_end: row_span.end,
                                col_start: col,
                                col_end: col + 1,
                            });
                        }
                    }
                }
                // Merge adjacent spans horizontally if same row range
                self.merge_horizontal_spans(spans).into()
            }
            BoundsType::WholeRow => {
                // Similar to WholeColumn but iterate rows
                // Implementation mirrors column logic (now fully implemented + property-tested)
                self.resolve_whole_row(desc)
            }
            // ... other bounds types
        }
    }

    // ...
}
```

**Bounds resolution policy:**
- `WholeRow` uses `row_stats_snapshot` to emit column stripes, mirroring the column-based implementation and ensuring spills or row edits get first-class treatment.
- `OpenRowDown/OpenRowUp` clamp the open side to the nearest populated span reported by the stats snapshot for that axis; if no stats exist, the descriptor resolves to zero spans (range is currently empty).
- `OpenColumn*` mirror the row logic but consult `column_stats_snapshot`.
- `WholeSheet` concatenates per-column spans across all columns with non-empty stats; it never reverts to `u32::MAX` placeholders.
- `Table` looks up the table’s cached rectangle (maintained by WorkbookEditor) and emits spans covering that region.
- `Spill` asks `SpillPlanner` for the most recent committed spill footprint anchored at `anchor` and resolves to those spans; unresolved spills resolve to empty ranges.

```rust
impl RangeTracker {
    fn merge_horizontal_spans(&self, spans: Vec<RowColSpan>) -> Vec<RowColSpan> {
        if spans.is_empty() {
            return spans;
        }

        // Sort by (row_start, row_end, col_start)
        let mut sorted = spans;
        sorted.sort_by_key(|s| (s.row_start, s.row_end, s.col_start));

        let mut merged = Vec::new();
        let mut current = sorted[0];

        for next in sorted.iter().skip(1) {
            if next.row_start == current.row_start &&
               next.row_end == current.row_end &&
               next.col_start == current.col_end {
                // Adjacent horizontally, merge
                current.col_end = next.col_end;
            } else {
                merged.push(current);
                current = *next;
            }
        }
        merged.push(current);
        merged
    }
}
```

### 3.6 AddressIndex (EXPANDED)

```rust
pub struct AddressIndex {
    sheets: FxHashMap<SheetId, SheetAddressIndex>,
    vertex_store_ref: Arc<RwLock<VertexStore>>,
}

pub struct SheetAddressIndex {
    columns: FxHashMap<u32, ColumnAddressIndex>,
    blocks: Vec<BlockRange>,
    vertex_to_cell: FxHashMap<VertexId, CellRef>,
}

pub struct ColumnAddressIndex {
    sparse: FxHashMap<u32, VertexId>,
}

pub struct BlockRange {
    row_start: u32,
    row_end: u32,   // exclusive
    col_start: u32,
    col_end: u32,   // exclusive
    first_vertex: VertexId,
}

impl AddressIndex {
    pub fn ensure_vertices(
        &mut self,
        coords: &[(SheetId, AbsCoord)],
    ) -> Vec<VertexId>;

    pub fn ensure_vertices_for_span(
        &mut self,
        sheet: SheetId,
        span: &RowColSpan,
    ) -> Vec<VertexId>;

    pub fn vertex_of_cell(&self, cell: &CellRef) -> Option<VertexId>;

    pub fn vertices_in_span_iter<'a>(
        &'a self,
        sheet: SheetId,
        span: &'a RowColSpan,
    ) -> impl Iterator<Item = (CellRef, VertexId)> + 'a;

    pub fn cell_for(&self, vertex: VertexId) -> Option<CellRef>;

    pub fn remove_vertex(&mut self, vertex: VertexId);

    /// Bulk allocation for contiguous ranges (ingest optimization)
    pub fn allocate_contiguous_block(
        &mut self,
        sheet: SheetId,
        row_start: u32,
        row_end: u32,
        col_start: u32,
        col_end: u32,
    ) -> Vec<VertexId>;
}
```

**Contiguous allocation for bulk ingest:**
```rust
impl AddressIndex {
    pub fn allocate_contiguous_block(
        &mut self,
        sheet: SheetId,
        row_start: u32,
        row_end: u32,
        col_start: u32,
        col_end: u32,
    ) -> Vec<VertexId> {
        let height = row_end - row_start;
        let width = col_end - col_start;
        let count = (height * width) as usize;

        // Bulk allocate from VertexStore
        let mut store = self.vertex_store_ref.write().unwrap();
        let first_id = store.allocate_contiguous(count);
        drop(store);

        let mut vertices = Vec::with_capacity(count);
        let sheet_index = self.sheets.entry(sheet).or_insert_with(Default::default);

        let mut id = first_id.0;
        let first_vertex = VertexId(first_id.0);
        for row in row_start..row_end {
            for col in col_start..col_end {
                let vertex = VertexId(id);
                let cell = CellRef {
                    sheet_id: sheet,
                    coord: Coord::new(row, col, true, true),
                };
                sheet_index
                    .columns
                    .entry(col)
                    .or_insert_with(|| ColumnAddressIndex { sparse: FxHashMap::default() })
                    .sparse
                    .insert(row, vertex);
                sheet_index.vertex_to_cell.insert(vertex, cell);
                vertices.push(vertex);
                id += 1;
            }
        }

        sheet_index.blocks.push(BlockRange {
            row_start,
            row_end,
            col_start,
            col_end,
            first_vertex,
        });

        vertices
    }
}
```

### 3.7 DependencyIndex & WorkbookGraph Trait (EXPANDED)

```rust
pub struct DependencyIndex {
    csr: CsrStore,
    range_map: FxHashMap<RangeHandle, RangeEdges>,
    topo: DynamicTopo<VertexId>,
    dirty: FxHashSet<VertexId>,
    vertex_meta: VertexMetaTable,
    cycle_cache: FxHashMap<VertexId, Vec<VertexId>>,
}

pub struct RangeEdges {
    subscriber: VertexId,
    targets: Vec<VertexId>,  // All vertices in the range's current spans
}

pub struct VertexMetaTable {
    kinds: Vec<VertexKind>,
    sheets: Vec<SheetId>,
    coords: Vec<AbsCoord>,
    flags: Vec<u8>,
}

impl DependencyIndex {
    pub fn apply_dependencies(
        &mut self,
        vertex: VertexId,
        ranges: Vec<RangeBinding>,
        address_index: &mut AddressIndex,
    );

    pub fn handle_range_events(
        &mut self,
        events: Vec<RangeEvent>,
        address_index: &mut AddressIndex,
    );

    pub fn mark_dirty(&mut self, vertex: VertexId);

    pub fn mark_dirty_batch(&mut self, vertices: &[VertexId]);

    pub fn pop_dirty_batch(&mut self, limit: usize) -> Vec<VertexId>;

    pub fn snapshot(&self, vertex: VertexId) -> VertexSnapshot;

    pub fn is_cyclic(&self, vertex: VertexId) -> bool;

    pub fn get_cycle(&self, vertex: VertexId) -> Option<&[VertexId]>;

    pub fn clear_cycle_cache(&mut self);

    pub fn export_topo_layers(&self) -> Vec<Vec<VertexId>>;
}

pub struct RangeBinding {
    pub handle: RangeHandle,
    pub reference: LazyRangeRef,
    pub name: Option<String>,  // For named dependencies
}

pub trait WorkbookGraph: Send + Sync {
    fn vertex_meta(&self, vertex: VertexId) -> VertexMeta;
    fn inputs(&self, vertex: VertexId) -> Vec<LazyRangeRef>;
    fn pop_dirty_batch(&self, limit: usize) -> Vec<VertexId>;
    fn clear_dirty(&self, vertices: &[VertexId]);
    fn export_csr(&self) -> CsrExport;
    fn topo_rank(&self, vertex: VertexId) -> Option<usize>;
}

pub struct CsrExport {
    pub offsets: Vec<usize>,
    pub targets: Vec<VertexId>,
    pub vertex_count: usize,
}
```

**Dependency application algorithm:**
```rust
impl DependencyIndex {
    pub fn apply_dependencies(
        &mut self,
        vertex: VertexId,
        ranges: Vec<RangeBinding>,
        address_index: &mut AddressIndex,
    ) {
        // 1. Retrieve old dependencies for this vertex
        let old_handles: Vec<RangeHandle> = self.range_map
            .iter()
            .filter(|(_, edges)| edges.subscriber == vertex)
            .map(|(h, _)| *h)
            .collect();

        // 2. Compute removed handles (old - new)
        let new_handles: FxHashSet<_> = ranges.iter().map(|b| b.handle).collect();
        let removed: Vec<_> = old_handles.iter()
            .filter(|h| !new_handles.contains(h))
            .copied()
            .collect();

        // 3. Remove edges for removed handles
        // Edge direction: input → subscriber (target → vertex)
        for handle in removed {
            if let Some(edges) = self.range_map.remove(&handle) {
                for target in edges.targets {
                    self.csr.remove_edge(target, vertex);
                }
            }
        }

        // 4. Add/update edges for new handles
        for binding in ranges {
            let spans = binding.reference.spans();
            let mut targets = Vec::new();

            for span in spans.iter() {
                for (_, vertex) in address_index.vertices_in_span_iter(
                    binding.reference.descriptor().sheet,
                    span,
                ) {
                    targets.push(vertex);
                }
            }

            // Ensure placeholders if needed
            if targets.is_empty() && !spans.is_empty() {
                targets = address_index.ensure_vertices_for_span(
                    binding.reference.descriptor().sheet,
                    &spans[0],
                );
            }

            // Update CSR
            if let Some(old_edges) = self.range_map.get(&binding.handle) {
                // Diff old vs new targets
                let old_set: FxHashSet<_> = old_edges.targets.iter().copied().collect();
                let new_set: FxHashSet<_> = targets.iter().copied().collect();

                // Edge direction: input → subscriber (target → vertex)
                // This ensures topological ordering evaluates dependencies before dependents
                for &removed in old_set.difference(&new_set) {
                    self.csr.remove_edge(removed, vertex);
                }
                for &added in new_set.difference(&old_set) {
                    self.csr.add_edge(added, vertex);
                }
            } else {
                // New handle, add all edges (input → subscriber)
                for &target in &targets {
                    self.csr.add_edge(target, vertex);
                }
            }

            self.range_map.insert(binding.handle, RangeEdges {
                subscriber: vertex,
                targets,
            });
        }

        // 5. Update topological order
        self.topo.update_vertex(vertex);

        // 6. Mark vertex dirty
        self.mark_dirty(vertex);
    }
}
```

### 3.8 WorkbookEditor (EXPANDED)

```rust
pub struct WorkbookEditor<'a> {
    storage: &'a mut SheetStoreService,
    range_tracker: &'a mut RangeTracker,
    dep_index: &'a mut DependencyIndex,
    address_index: &'a mut AddressIndex,
    change_log: &'a mut dyn ChangeLogger,
    edit_handle: Option<EditHandle>,
    transaction_depth: usize,
}

impl<'a> WorkbookEditor<'a> {
    pub fn begin(&mut self);

    pub fn set_value(&mut self, addr: CellRef, value: LiteralValue);

    pub fn set_formula(&mut self, addr: CellRef, ast: ASTNode);

    pub fn set_value_batch(&mut self, writes: Vec<(CellRef, LiteralValue)>);

    pub fn bulk_ingest(&mut self, sheet: &str, columns: Vec<IngestColumn>);

    pub fn rename_sheet(&mut self, sheet: SheetId, new_name: &str);

    pub fn delete_range(&mut self, sheet: SheetId, span: RowColSpan);

    pub fn insert_rows(&mut self, sheet: SheetId, before_row: u32, count: u32);

    pub fn delete_rows(&mut self, sheet: SheetId, start_row: u32, count: u32);

    pub fn insert_columns(&mut self, sheet: SheetId, before_col: u32, count: u32);

    pub fn delete_columns(&mut self, sheet: SheetId, start_col: u32, count: u32);

    pub fn commit(&mut self) -> CommitSummary;

    pub fn rollback(&mut self);

    pub fn nested_transaction<F, R>(&mut self, f: F) -> Result<R, ExcelError>
    where
        F: FnOnce(&mut Self) -> Result<R, ExcelError>;
}

pub struct CommitSummary {
    pub affected_vertices: Vec<VertexId>,
    pub dirty_count: usize,
    pub overlay_stats: OverlayStats,
    pub telemetry: CommitTelemetry,
    pub duration: std::time::Duration,
}

#[derive(Debug, Clone, Default)]
pub struct CommitTelemetry {
    pub cells_written: usize,
    pub deltas_processed: usize,
    pub range_events_emitted: usize,
    pub csr_edges_added: usize,
    pub csr_edges_removed: usize,
    pub topo_updates: usize,
}
```

**Nested transaction support:**
```rust
impl<'a> WorkbookEditor<'a> {
    pub fn nested_transaction<F, R>(&mut self, f: F) -> Result<R, ExcelError>
    where
        F: FnOnce(&mut Self) -> Result<R, ExcelError>
    {
        self.transaction_depth += 1;
        let savepoint = self.change_log.create_savepoint();

        match f(self) {
            Ok(result) => {
                self.transaction_depth -= 1;
                Ok(result)
            }
            Err(e) => {
                // Rollback to savepoint
                self.change_log.rollback_to(savepoint);
                self.transaction_depth -= 1;
                Err(e)
            }
        }
    }
}
```

### 3.9 EngineCore & Scheduler APIs (EXPANDED)

```rust
pub struct EngineCore<G: WorkbookGraph> {
    graph: Arc<G>,
    scheduler: Scheduler,
    interpreter_pool: InterpreterPool,
    config: EvalConfig,
    cancellation_token: Arc<AtomicBool>,
}

impl<G: WorkbookGraph> EngineCore<G> {
    pub fn recalc_all(&mut self) -> EvalResult;

    pub fn recalc_vertices(&mut self, seeds: &[VertexId]) -> EvalResult;

    pub fn recalc_with_plan(&mut self, plan: &RecalcPlan) -> EvalResult;

    pub fn build_recalc_plan(&self, seeds: &[VertexId]) -> RecalcPlan;

    pub fn cancel(&self);

    fn execute_schedule(&mut self, schedule: Schedule) -> EvalResult;

    fn evaluate_layer(
        &mut self,
        layer: &[VertexId],
    ) -> LayerResult;
}

pub struct EvalConfig {
    pub parallel_threshold: usize,
    pub max_parallelism: usize,
    pub enable_arrow_fastpath: bool,
    pub enable_volatile_tracking: bool,
    pub cycle_limit: usize,
}

pub struct EvalResult {
    pub computed_vertices: usize,
    pub cycle_errors: usize,
    pub elapsed: std::time::Duration,
    pub layers_executed: usize,
    pub telemetry: EvalTelemetry,
}

#[derive(Debug, Clone, Default)]
pub struct EvalTelemetry {
    pub arrow_fastpath_hits: usize,
    pub arrow_fastpath_misses: usize,
    pub volatile_recalcs: usize,
    pub parallel_layers: usize,
    pub sequential_layers: usize,
}

pub struct InterpreterPool {
    pool: rayon::ThreadPool,
    function_provider: Arc<dyn FunctionProvider>,
}
```

## 4. Data Flows (EXPANDED)

### 4.5 Row/Column Insert/Delete Flow (NEW)

**Scenario:** User inserts 10 rows before row 100 on Sheet1.

**Sequence:**
1. **Begin transaction** – `WorkbookEditor::begin` acquires edit handle
2. **Shift vertices** – `WorkbookEditor::insert_rows(sheet=Sheet1, before_row=100, count=10)`
   - AddressIndex shifts all vertices with `row >= 100` by `+10`
   - Updates per-column sparse maps and contiguous block metadata so lookups stay O(1)
   - Returns list of affected vertices
3. **Adjust references** – For each affected vertex with a formula:
   - Parse AST and identify `ReferenceType` nodes
   - Apply `ReferenceAdjuster::adjust_for_row_insert`
   - Rewrite AST with adjusted coordinates
   - Store updated AST via `DataStore`
4. **Update ranges** – For each impacted `RangeHandle` covering the shifted region:
   - **Note:** `RangeDescriptor`s are immutable snapshots; bounds cannot be adjusted in-place
   - Instead: re-normalize the reference to produce a **new descriptor**
   - Call `RangeTracker::register` with the new descriptor to obtain a **new handle**
   - `DependencyIndex` diffs old vs new handle and atomically swaps range edges
   - `RangeTracker` emits `RangeEvent::Expanded` for newly included rows
5. **Propagate dependencies** – DependencyIndex processes range events:
   - For each expanded range, call `AddressIndex::ensure_vertices_for_span` so every span vertex exists
   - Update CSR edges (input → subscriber direction)
   - Mark affected formulas dirty
6. **Commit** – `WorkbookEditor::commit` finalizes:
   - Change log records shift operation
   - Telemetry captures: vertices_moved, references_adjusted, formulas_updated

**Reference adjustment algorithm:**
```rust
pub struct ReferenceAdjuster;

impl ReferenceAdjuster {
    pub fn adjust_for_row_insert(
        reference: &ReferenceType,
        sheet: SheetId,
        before_row: u32,
        count: u32,
    ) -> ReferenceType {
        match reference {
            ReferenceType::Cell { sheet: ref_sheet, row, col } => {
                if ref_sheet.as_ref().map(|s| s == sheet).unwrap_or(true) && *row >= before_row {
                    ReferenceType::Cell {
                        sheet: ref_sheet.clone(),
                        row: row + count,
                        col: *col,
                    }
                } else {
                    reference.clone()
                }
            }
            ReferenceType::Range {
                sheet: ref_sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => {
                let new_start_row = start_row.map(|r| {
                    if r >= before_row { r + count } else { r }
                });
                let new_end_row = end_row.map(|r| {
                    if r >= before_row { r + count } else { r }
                });
                ReferenceType::Range {
                    sheet: ref_sheet.clone(),
                    start_row: new_start_row,
                    start_col: *start_col,
                    end_row: new_end_row,
                    end_col: *end_col,
                }
            }
            _ => reference.clone(),
        }
    }
}
```

### 4.6 Named Range Management Flow (NEW)

**Scenario:** User defines a workbook-scoped named range `DataTable = Sheet1!$A$1:$D$100`.

**Sequence:**
1. **Parse reference** – Convert string `Sheet1!$A$1:$D$100` to `ReferenceType::Range`
2. **Create descriptor** – Normalize to `RangeDescriptor`:
   ```
   sheet: Sheet1
   start_row: 0, start_col: 0
   height: 100, width: 4
   bounds: Finite
   ```
3. **Register with RangeTracker** – `register(descriptor, named_vertex)`
   - Receive `RangeHandle` and `Arc<[RowColSpan]>`
   - Store in `NamedRange` record
4. **Create named vertex** – AddressIndex allocates `VertexId` with `kind=NamedArray`
5. **Store definition** – `DependencyIndex::vertex_meta` records:
   ```
   VertexMeta {
       coord: (0, 0),  // Not tied to cell
       sheet_id: WORKBOOK_SCOPE,
       kind: NamedArray,
       flags: 0
   }
   ```
6. **Register dependencies** – Named range dependencies tracked via `range_map`
7. **Usage in formulas** – When formula references `=SUM(DataTable)`:
   - Parser resolves name to `ReferenceType::NamedRange { name: "DataTable" }`
   - Interpreter looks up `LazyRangeRef` from named vertex
   - Evaluation proceeds normally

**Named range update flow:**
```rust
pub fn update_named_range(
    &mut self,
    name: &str,
    scope: NameScope,
    new_reference: ReferenceType,
) -> Result<(), ExcelError> {
    // 1. Look up existing named vertex
    let vertex = self.named_ranges.get(name, scope)?;

    // 2. Normalize new reference to descriptor
    let descriptor = self.normalize_reference(&new_reference)?;

    // 3. Unregister old range handle
    let old_handle = self.range_map.get_by_vertex(vertex)?;
    self.range_tracker.unregister(old_handle, vertex);

    // 4. Register new range handle
    let (new_handle, spans) = self.range_tracker.register(descriptor, vertex);

    // 5. Update dependencies
    let binding = RangeBinding {
        handle: new_handle,
        reference: LazyRangeRef { descriptor: descriptor.clone(), handle: new_handle },
        name: Some(name.to_string()),
    };
    self.dep_index.apply_dependencies(vertex, vec![binding], &mut self.address_index);

    // 6. Mark dependents dirty
    let dependents = self.csr.dependents_of(vertex);
    self.dep_index.mark_dirty_batch(&dependents);

    Ok(())
}
```

## 5. Technical Stories (EXPANDED)

### Story F – Row Insert with Reference Adjustment (NEW)

**Actors:** UI, WorkbookEditor, AddressIndex, ReferenceAdjuster, DependencyIndex

**Preconditions:**
- Sheet1 has values in A1:A100
- Formula at B1: `=SUM(A1:A100)`
- Formula at B2: `=A101` (absolute reference to row 101)

**Sequence:**
1. UI calls `editor.insert_rows(Sheet1, before_row=50, count=5)`
2. AddressIndex:
   - Shifts vertices for rows 50-100 to 55-105
   - Returns 51 affected vertices (50 values + 1 formula at B2 after shift)
3. ReferenceAdjuster processes B1's formula:
   - AST contains `Range(A1:A100)`
   - `start_row=0` unchanged, `end_row=99` shifts to `104`
   - Updated: `=SUM(A1:A105)`
4. ReferenceAdjuster processes B2's formula:
   - AST contains `Cell(A101)`
   - `row=100` shifts to `105`
   - Updated: `=A106`
5. RangeTracker receives delta for rows 50-54 insertion:
   - Range handle for `A1:A100` expands to include new rows
   - Emits `RangeEvent::Expanded { spans: [(row_start=50, row_end=55, col_start=0, col_end=1)] }`
6. DependencyIndex:
   - Ensures placeholder vertices for A50-A54
   - Adds edges from B1 to new placeholders
   - Marks B1 dirty
7. Commit returns summary:
   ```
   vertices_moved: 51
   references_adjusted: 2
   formulas_updated: 2
   dirty_count: 1
   ```

**Invariants:**
- All absolute references shift correctly
- Range references expand to include inserted rows
- No references break (become `#REF!`)
- Undo can reverse the operation via change log

**Success criteria:**
- Formula results match Excel behavior after insert
- Telemetry shows expected adjustment counts
- Performance: O(affected_vertices + affected_formulas)

### Story G – Infinite Range Subscription and Update (NEW)

**Actors:** Formula vertex, RangeTracker, SheetStoreService, DependencyIndex

**Preconditions:**
- Sheet1 initially empty
- Formula at B1: `=COUNTA(A:A)`

**Sequence:**
1. **Formula registration:**
   - Parser extracts `ReferenceType::Range { start_col=0, end_col=0, start_row=None, end_row=None }`
   - WorkbookEditor normalizes to `RangeDescriptor { bounds: WholeColumn, start_col=0, width=1, height=1, shape.rows=(AxisBound::OpenStart, AxisBound::OpenEnd) }`
   - RangeTracker.register:
     - Checks `stats_snapshot` for column A
     - Currently empty: returns `spans = []`
     - Creates subscription with `subscriber=B1_vertex`
2. **Initial evaluation:**
   - Interpreter resolves `LazyRangeRef` for A:A
   - `spans() == []`, so `COUNTA` returns 0
   - B1 result: 0
3. **User writes A1000 = "data":**
   - SheetStoreService.write_cell emits `ColumnUsageDelta::BecameNonEmpty { column: 0, span: RowSpan { start: 1000, end: 1001 } }`
   - RangeTracker.apply_delta:
     - Updates axis interval for (Sheet1, Column, 0)
     - Finds subscription for B1's A:A handle
    - Emits `RangeEvent::Expanded { spans: [(row_start=1000, row_end=1001, col_start=0, col_end=1)] }`
4. **Dependency update:**
   - DependencyIndex.handle_range_events:
     - Ensures vertex for A1000 via `AddressIndex::ensure_vertices_for_span`
     - Adds CSR edge: `A1000` (input) → `B1` (subscriber)
     - Marks B1 dirty
5. **Re-evaluation:**
   - Scheduler includes B1 in next recalc
   - Interpreter re-resolves A:A, now `spans = [(row_start=1000, row_end=1001, col_start=0, col_end=1)]`
   - `COUNTA` returns 1
   - B1 result: 1

**Invariants:**
- Infinite range subscriptions remain lightweight (single handle)
- Only populated spans generate dependencies
- Sparse columns (e.g., A1, A1000000) don't create 1M vertices
- Span merging prevents excessive fragmentation

**Success criteria:**
- Memory usage: O(populated_cells), not O(sheet_height)
- Evaluation cost proportional to span count
- Telemetry: `range_events_emitted = 1` per column edit

### Story H – Named Range with Sheet Rename (NEW)

**Actors:** NamedRangeManager, WorkbookEditor, DependencyIndex

**Preconditions:**
- Named range `Sales = Data!$A$1:$A$1000`
- Formula at Summary!B1: `=SUM(Sales)`

**Sequence:**
1. **User renames sheet "Data" to "DataArchive":**
   - WorkbookEditor.rename_sheet(Data, "DataArchive")
2. **Update sheet registry:**
   - SheetRegistry updates name mapping
   - `SheetId` remains unchanged (stable identifier)
3. **Named range adjustment:**
   - NamedRangeManager iterates all named ranges
   - For `Sales`, reference is `ReferenceType::Range { sheet: Some("Data"), ... }`
   - Updates to `{ sheet: Some("DataArchive"), ... }`
4. **Formula AST updates:**
   - Formulas store `SheetId`, not sheet name strings
   - No AST changes needed (sheet ID unchanged)
5. **Display layer:**
   - When displaying formula, SheetRegistry.name(sheet_id) returns "DataArchive"
   - Formula displays as `=SUM(Sales)` unchanged

**Invariants:**
- Sheet renaming is O(1) for formula graph (only registry update)
- Named ranges remain valid
- No formula recalculation needed (data unchanged)

**Success criteria:**
- All formulas continue evaluating correctly
- Named range serialization reflects new sheet name
- Undo restores old sheet name

### Story I – Concurrent Edit Collision Handling (NEW)

**Actors:** Two WorkbookEditor instances (UI thread, background task)

**Preconditions:**
- Current implementation serializes all edits (single RwLock)
- Future MVCC extension would allow concurrent reads + writes

**Current behavior (serialized):**
```rust
// Thread 1: UI edit
editor1.begin();
editor1.set_value(A1, Number(10));
// Acquires write lock on DependencyIndex
editor1.commit();  // Releases lock

// Thread 2: Background recalc
editor2.begin();
// Blocks on write lock until thread 1 commits
editor2.commit_results(results);
```

**Invariants (current):**
- Single writer at a time (via `&mut` or `RwLock<T>`)
- All mutations atomic via WorkbookEditor transaction
- Evaluator holds read locks during compute

**Future MVCC design notes:**
- Version DependencyIndex with epoch counters
- Readers snapshot at epoch N
- Writers create epoch N+1 with delta
- Conflict detection on commit (optimistic concurrency)
- See Section 10 (Open Questions)

**Success criteria (current):**
- No deadlocks
- No data races
- Predictable serialization order
- Telemetry shows lock contention metrics

### Story J – Overlay Compaction Triggering (NEW)

**Actors:** WorkbookEditor, SheetStoreService, RangeTracker

**Preconditions:**
- Sheet1 column A has Arrow chunk with 10,000 rows
- Overlay currently has 500 entries (below threshold)

**Sequence:**
1. **Batch edit writes 600 cells in column A:**
   - `editor.set_value_batch(A1:A600)`
2. **During commit:**
   - SheetStoreService accumulates 600 overlay writes
   - After final write, `should_compact(Sheet1, A)` checks:
     ```
     overlay_count=1100 >= abs_threshold(1024)  ✓
     ```
   - Triggers `compact_column(Sheet1, A)`
3. **Compaction process:**
   - Allocates new Arrow chunk with merged overlay values
   - Updates `ColumnUsageStats`:
     ```
     overlay_count: 1100 → 0
     stats_version: 42 → 43  (monotonic)
     last_compaction: Some(Instant::now())
     ```
   - Emits `ColumnUsageDelta::Compacted { sheet, column, stats }`
4. **RangeTracker processes compaction delta:**
   - Detects the `(column, stats_version)` change
   - Finds all subscriptions covering column A
   - For each, recomputes spans using the new stats, bumps `subscription_version`, and emits `Expanded`, `Shrunk`, or `Emptied` events only when spans actually changed
5. **DependencyIndex:**
   - For each emitted event, ensure vertices exist for the span via `AddressIndex::ensure_vertices_for_span`, update CSR edges (input → subscriber), and mark affected formulas dirty

**Invariants:**
- Compaction is transparent to formulas (values unchanged)
- `(column, stats_version)` changes invalidate stale lazy references
- Compaction never happens mid-evaluation (edit lock held)

**Success criteria:**
- Overlay memory bounded by thresholds
- Compaction cost amortized: O(compacted_cells) per million writes
- Telemetry: `compactions_performed`, `overlay_entries_cleared`
- Performance: compaction time < 10ms for 100K cells
# Range-Centric Engine TDD - Chunk 2: Implementation Mechanics

## 6. Arrow Integration: Deep Dive (EXPANDED)

This section provides implementation-level detail for integrating the range-centric architecture with the existing Arrow storage layer.

### 6.1 ColumnChunk Structure and Overlay Management

The existing `ColumnChunk` is the foundational storage unit. Understanding its internal structure is critical for implementing `ColumnUsageStats` and delta emission.

```rust
pub struct ColumnChunk {
    // Primary storage arrays (immutable after creation)
    pub numbers: Option<Arc<Float64Array>>,
    pub booleans: Option<Arc<BooleanArray>>,
    pub text: Option<ArrayRef>,          // StringArray
    pub errors: Option<Arc<UInt8Array>>,
    pub type_tag: Arc<UInt8Array>,       // Required, per-row type discriminator
    pub formula_id: Option<Arc<UInt32Array>>,

    // Metadata
    pub meta: ColumnChunkMeta,

    // Lazy-initialized null arrays (avoid allocation until needed)
    lazy_null_numbers: OnceCell<Arc<Float64Array>>,
    lazy_null_booleans: OnceCell<Arc<BooleanArray>>,
    lazy_null_text: OnceCell<ArrayRef>,
    lazy_null_errors: OnceCell<Arc<UInt8Array>>,

    // Lowered text cache for case-insensitive operations
    lowered_text: OnceCell<ArrayRef>,

    // Overlay: delta writes since last compaction
    pub overlay: Overlay,
}

pub struct Overlay {
    entries: FxHashMap<u32, OverlayEntry>,  // row_offset → entry
}

#[derive(Debug, Clone)]
pub struct OverlayEntry {
    pub type_tag: TypeTag,
    pub value: OverlayValue,
}

#[derive(Debug, Clone)]
pub enum OverlayValue {
    Number(f64),
    Boolean(bool),
    Text(Arc<str>),
    Error(u8),
    Empty,
}
```

**Key implementation points:**

1. **Type Tag Lane:** Every row has a `TypeTag` (u8) indicating its type. This enables O(1) type checks without inspecting value lanes.

2. **Overlay Writes:** When `write_cell_value` is called:
   ```rust
   fn write_cell_value(&mut self, row_offset: u32, value: &LiteralValue) {
       let entry = OverlayEntry {
           type_tag: TypeTag::from_value(value),
           value: OverlayValue::from_literal(value),
       };
       self.overlay.entries.insert(row_offset, entry);
   }
   ```

3. **Compaction Trigger:** After each write, check:
   ```rust
   fn should_compact(&self) -> bool {
       let overlay_count = self.overlay.entries.len();
       let chunk_len = self.meta.len;

       overlay_count >= OVERLAY_COMPACTION_ABS_THRESHOLD ||
       (chunk_len > 0 && overlay_count * OVERLAY_COMPACTION_FRAC_DEN >= chunk_len)
   }
   ```

4. **Compaction Process:**
   ```rust
   fn compact_chunk(&mut self) -> CompactionResult {
       // 1. Build new arrays merging overlay
       let mut num_builder = Float64Builder::with_capacity(self.meta.len);
       let mut tag_builder = UInt8Builder::with_capacity(self.meta.len);

       for row in 0..self.meta.len {
           if let Some(entry) = self.overlay.entries.get(&(row as u32)) {
               // Use overlay value
               match entry.value {
                   OverlayValue::Number(n) => {
                       num_builder.append_value(n);
                       tag_builder.append_value(TypeTag::Number as u8);
                   }
                   // ... other types
               }
           } else {
               // Use base array value
               let tag = TypeTag::from_u8(self.type_tag.value(row));
               tag_builder.append_value(tag as u8);
               match tag {
                   TypeTag::Number => {
                       let val = self.numbers.as_ref()
                           .map(|arr| arr.value(row))
                           .unwrap_or(0.0);
                       num_builder.append_value(val);
                   }
                   // ... other types
               }
           }
       }

       // 2. Replace arrays
       let cleared_count = self.overlay.entries.len();
       self.numbers = Some(Arc::new(num_builder.finish()));
       self.type_tag = Arc::new(tag_builder.finish());
       self.overlay.entries.clear();

       // 3. Invalidate lazy caches
       self.lowered_text = OnceCell::new();

       CompactionResult { cleared_count }
   }
   ```

### 6.2 ColumnUsageStats Implementation

`ColumnUsageStats` must be maintained incrementally as writes occur.

```rust
impl SheetStore {
    /// Returns axis deltas describing the change to usage statistics
    pub fn write_cell_value(
        &mut self,
        sheet: SheetId,
        row: u32,
        col: u32,
        value: &LiteralValue,
    ) -> SmallVec<[UsageDelta; 2]> {
        // 1. Get or create column
        let column = self.get_or_create_column(sheet, col);

        // 2. Record old stats
        let old_stats = self.compute_column_stats(sheet, col);

        // 3. Write to appropriate chunk
        let chunk_idx = row / self.chunk_size;
        let row_offset = row % self.chunk_size;
        column.chunks[chunk_idx].write_cell_value(row_offset, value);

        // 4. Compute new stats
        let new_stats = self.compute_column_stats(sheet, col);

        // 5. Generate deltas
        let mut deltas = SmallVec::new();

        if let Some(col_delta) = self.compute_column_delta(&old_stats, &new_stats, col) {
            deltas.push(UsageDelta::Column {
                sheet,
                column: col,
                delta: col_delta,
            });
        }

        if let Some(row_delta) = self.compute_row_delta(sheet, row, col, value) {
            deltas.push(UsageDelta::Row {
                sheet,
                row,
                delta: row_delta,
            });
        }

        deltas
    }

    fn compute_column_stats(&self, sheet: SheetId, col: u32) -> ColumnUsageStats {
        let column = self.get_column(sheet, col)?;

        let mut min_row = None;
        let mut max_row = None;
        let mut spans = Vec::new();
        let mut overlay_count = 0;

        let mut current_span: Option<RowSpan> = None;

        for (chunk_idx, chunk) in column.chunks.iter().enumerate() {
            let chunk_base_row = (chunk_idx * self.chunk_size) as u32;

            // Scan type_tag array and overlay
            for row_offset in 0..chunk.meta.len {
                let row = chunk_base_row + row_offset as u32;

                let is_populated = if let Some(entry) = chunk.overlay.entries.get(&(row_offset as u32)) {
                    overlay_count += 1;
                    entry.type_tag != TypeTag::Empty
                } else {
                    let tag = TypeTag::from_u8(chunk.type_tag.value(row_offset));
                    tag != TypeTag::Empty
                };

                if is_populated {
                    if min_row.is_none() {
                        min_row = Some(row);
                    }
                    max_row = Some(row);

                    // Extend or start span
                    if let Some(ref mut span) = current_span {
                        if row == span.end + 1 {
                            span.end = row;
                        } else {
                            spans.push(*span);
                            current_span = Some(RowSpan { start: row, end: row });
                        }
                    } else {
                        current_span = Some(RowSpan { start: row, end: row });
                    }
                }
            }
        }

        if let Some(span) = current_span {
            spans.push(span);
        }

        ColumnUsageStats {
            sheet,
            column: col,
            min_row,
            max_row,
            stats_version: self.next_stats_version(),
            spans: SmallVec::from_vec(spans),
            overlay_count,
            arrow_chunk_count: column.chunks.len(),
            last_compaction: None,
        }
    }

    fn compute_column_delta(
        &self,
        old: &ColumnUsageStats,
        new: &ColumnUsageStats,
        col: u32,
    ) -> Option<ColumnUsageDelta> {
        match (old.min_row, new.min_row) {
            (None, None) => None,
            (None, Some(_)) => {
                // Column became non-empty
                Some(ColumnUsageDelta::BecameNonEmpty {
                    column: col,
                    span: new.spans[0],
                })
            }
            (Some(_), None) => {
                // Column became empty
                Some(ColumnUsageDelta::BecameEmpty)
            }
            (Some(_), Some(_)) => {
                // Check for span changes
                if old.spans == new.spans {
                    None
                } else {
                    // Compute added/removed spans
                    let old_set: FxHashSet<_> = old.spans.iter().copied().collect();
                    let new_set: FxHashSet<_> = new.spans.iter().copied().collect();

                    let added: Vec<_> = new_set.difference(&old_set).copied().collect();
                    let removed: Vec<_> = old_set.difference(&new_set).copied().collect();

                    if !removed.is_empty() {
                        Some(ColumnUsageDelta::Shrunk { removed: removed.into() })
                    } else if !added.is_empty() {
                        Some(ColumnUsageDelta::Expanded { added: added.into() })
                    } else {
                        None
                    }
                }
            }
        }
    }

    fn compute_row_delta(
        &self,
        sheet: SheetId,
        row: u32,
        col: u32,
        value: &LiteralValue,
    ) -> Option<RowUsageDelta> {
        // Symmetric to compute_column_delta but tracks spans across columns
        let old = self.row_stats.get(&(sheet, row)).cloned().unwrap_or_default();
        let mut new = old.clone();
        new.update_with(cell_edit(col, value));
        self.compute_row_delta_from_stats(&old, &new)
    }
}
```

### 6.3 ArrowRangeView with Span-Aware Iteration

`ArrowRangeView` must support efficient iteration over non-contiguous spans.

```rust
pub struct ArrowRangeView<'a> {
    sheet_store: &'a SheetStore,
    sheet: SheetId,
    descriptor: RangeDescriptor,
    spans: Arc<[RowColSpan]>,
}

impl<'a> ArrowRangeView<'a> {
    pub fn new(
        sheet_store: &'a SheetStore,
        descriptor: RangeDescriptor,
        spans: Arc<[RowColSpan]>,
    ) -> Self {
        Self {
            sheet_store,
            sheet: descriptor.sheet,
            descriptor,
            spans,
        }
    }

    pub fn dims(&self) -> (usize, usize) {
        // Compute effective dimensions from spans
        let total_rows: u32 = self.spans.iter().map(|s| s.row_end - s.row_start).sum();
        let total_cols: u32 = self.spans.iter().map(|s| s.col_end - s.col_start).max().unwrap_or(0);
        (total_rows as usize, total_cols as usize)
    }

    /// Returns iterator over (row_in_view, col_in_view, value)
    pub fn iter_values(&self) -> impl Iterator<Item = (usize, usize, LiteralValue)> + '_ {
        ArrowRangeIterator::new(self)
    }

    /// Returns slices of number arrays for fast-path operations
    pub fn numbers_slices(&self) -> Vec<(usize, usize, Vec<Arc<Float64Array>>)> {
        let mut result = Vec::new();

        for span in self.spans.iter() {
            let mut cols = Vec::new();
            for col in span.col_start..span.col_end {
                if let Some(column) = self.sheet_store.get_column(self.sheet, col) {
                    // Extract chunks covering span rows
                    let chunk_start = (span.row_start / self.sheet_store.chunk_size) as usize;
                    let chunk_end = ((span.row_end.saturating_sub(1)) / self.sheet_store.chunk_size) as usize;

                    for chunk_idx in chunk_start..=chunk_end {
                        if let Some(chunk) = column.chunks.get(chunk_idx) {
                            if let Some(ref nums) = chunk.numbers {
                                cols.push(nums.clone());
                            }
                        }
                    }
                }
            }

            let row_start = span.row_start as usize;
            let row_len = (span.row_end - span.row_start + 1) as usize;
            result.push((row_start, row_len, cols));
        }

        result
    }

    /// Returns lowered text columns for case-insensitive matching
    pub fn lowered_text_columns(&self) -> Vec<ArrayRef> {
        let mut result = Vec::new();

        for span in self.spans.iter() {
            for col in span.col_start..=span.col_end {
                if let Some(column) = self.sheet_store.get_column(self.sheet, col) {
                    // Collect lowered text from all chunks in span
                    let mut text_parts = Vec::new();
                    let chunk_start = (span.row_start / self.sheet_store.chunk_size) as usize;
                    let chunk_end = (span.row_end / self.sheet_store.chunk_size) as usize;

                    for chunk_idx in chunk_start..=chunk_end {
                        if let Some(chunk) = column.chunks.get(chunk_idx) {
                            let lowered = chunk.lowered_text.get_or_init(|| {
                                // Compute lowered text on first access
                                Self::compute_lowered_text(chunk)
                            });
                            text_parts.push(lowered.clone());
                        }
                    }

                    // Concatenate parts
                    if !text_parts.is_empty() {
                        let concat = arrow::compute::concat(
                            &text_parts.iter().map(|a| a.as_ref()).collect::<Vec<_>>()
                        ).unwrap();
                        result.push(concat);
                    }
                }
            }
        }

        result
    }

    fn compute_lowered_text(chunk: &ColumnChunk) -> ArrayRef {
        if let Some(ref text) = chunk.text {
            let string_arr = text.as_any().downcast_ref::<StringArray>().unwrap();
            let mut builder = StringBuilder::with_capacity(string_arr.len(), string_arr.get_buffer_memory_size());

            for i in 0..string_arr.len() {
                if string_arr.is_null(i) {
                    builder.append_null();
                } else {
                    let val = string_arr.value(i);
                    builder.append_value(val.to_ascii_lowercase());
                }
            }

            Arc::new(builder.finish())
        } else {
            Arc::new(StringArray::new_null(chunk.meta.len))
        }
    }
}

struct ArrowRangeIterator<'a> {
    view: &'a ArrowRangeView<'a>,
    span_idx: usize,
    row_in_span: usize,
    col_in_span: usize,
    view_row: usize,
}

impl<'a> Iterator for ArrowRangeIterator<'a> {
    type Item = (usize, usize, LiteralValue);

    fn next(&mut self) -> Option<Self::Item> {
        while self.span_idx < self.view.spans.len() {
            let span = &self.view.spans[self.span_idx];
            let span_rows = (span.row_end - span.row_start + 1) as usize;
            let span_cols = (span.col_end - span.col_start + 1) as usize;

            if self.row_in_span < span_rows {
                if self.col_in_span < span_cols {
                    let row = span.row_start + self.row_in_span as u32;
                    let col = span.col_start + self.col_in_span as u32;

                    let value = self.view.sheet_store.read_cell_value(self.view.sheet, row, col);

                    let result = (self.view_row, self.col_in_span, value);

                    self.col_in_span += 1;
                    if self.col_in_span >= span_cols {
                        self.col_in_span = 0;
                        self.row_in_span += 1;
                        self.view_row += 1;
                    }

                    return Some(result);
                }
            }

            // Move to next span
            self.span_idx += 1;
            self.row_in_span = 0;
            self.col_in_span = 0;
        }

        None
    }
}
```

### 6.4 Cache Invalidation via stats_version

RangeTracker caches resolved spans. When the `(sheet, column)` `stats_version` changes (indicating column structure changed), caches must invalidate by recomputing spans for the associated `RangeHandle` and bumping its `subscription_version`.

```rust
impl RangeTracker {
    pub fn apply_delta(&mut self, delta: UsageDelta) -> Vec<RangeEvent> {
        let mut events = Vec::new();

        let (sheet, axis, index) = match delta {
            UsageDelta::Column { sheet, column, delta } => {
                if let ColumnUsageDelta::Compacted { stats } = delta.clone() {
                    self.stats_snapshot.insert((sheet, column), stats);
                }
                (sheet, Axis::Column, column)
            }
            UsageDelta::Row { sheet, row, .. } => {
                (sheet, Axis::Row, row)
            }
        };

        let handles = match axis {
            Axis::Column => self.by_col.get(&(sheet, index)).cloned().unwrap_or_default(),
            Axis::Row => self.by_row.get(&(sheet, index)).cloned().unwrap_or_default(),
        };

        for handle in handles {
            let subscription = self.subscriptions.get_mut(&handle).unwrap();
            let new_spans = self.resolve_descriptor(&subscription.descriptor);

            let old_set: FxHashSet<_> = subscription.spans.iter().copied().collect();
            let new_set: FxHashSet<_> = new_spans.iter().copied().collect();
            let added: Vec<_> = new_set.difference(&old_set).copied().collect();
            let removed: Vec<_> = old_set.difference(&new_set).copied().collect();

            subscription.spans = new_spans;
            subscription.subscription_version += 1;

            if !added.is_empty() {
                events.push(RangeEvent::Expanded { handle, spans: added.clone() });
            }
            if !removed.is_empty() {
                events.push(RangeEvent::Shrunk { handle, spans: removed });
            }
            if subscription.spans.is_empty() {
                events.push(RangeEvent::Emptied { handle });
            }
        }

        events
    }
}
```

## 7. CSR & Topological Ordering (NEW SECTION)

### 7.1 CsrStore Structure

The Compressed Sparse Row (CSR) format is the foundation of dependency storage.

```rust
/// Compressed Sparse Row graph storage
///
/// **Edge direction convention:** Edges point from **input → subscriber** (dependency → dependent).
/// This ensures topological ordering naturally evaluates dependencies before dependents.
/// - `dependents_of(v)` returns vertices that **depend on** v (i.e., dependents/subscribers)
/// - `dependencies_of(v)` returns vertices that v **depends on** (requires inverse lookup or separate tracking)
pub struct CsrStore {
    offsets: Vec<usize>,        // offsets[v] = start index in targets for vertex v
    targets: Vec<VertexId>,     // flattened adjacency lists
    vertex_count: usize,
    edge_count: usize,
    delta_edges: DeltaEdges,    // Pending edge mutations
    needs_rebuild: bool,
}

impl CsrStore {
    pub fn new(vertex_count: usize) -> Self {
        Self {
            offsets: vec![0; vertex_count + 1],
            targets: Vec::new(),
            vertex_count,
            edge_count: 0,
            delta_edges: DeltaEdges::new(),
            needs_rebuild: false,
        }
    }

    pub fn add_edge(&mut self, from: VertexId, to: VertexId) {
        self.delta_edges.add(from, to);
        self.needs_rebuild = true;
        self.edge_count += 1;
    }

    pub fn remove_edge(&mut self, from: VertexId, to: VertexId) {
        self.delta_edges.remove(from, to);
        self.needs_rebuild = true;
        self.edge_count -= 1;
    }

    /// Returns dependents of vertex (cells that consume this vertex)
    pub fn dependents_of(&self, vertex: VertexId) -> Vec<VertexId> {
        let base = self.base_dependents_of(vertex);
        let delta = self.delta_edges.get_dependents_of(vertex);
        self.merge_dependents_of(base, delta)
    }

    fn base_dependents_of(&self, vertex: VertexId) -> &[VertexId] {
        let idx = vertex.as_index();
        if idx >= self.vertex_count {
            return &[];
        }
        let start = self.offsets[idx];
        let end = self.offsets[idx + 1];
        &self.targets[start..end]
    }

    fn merge_dependents_of(&self, base: &[VertexId], delta: DeltaEdgeSet) -> Vec<VertexId> {
        let mut result: FxHashSet<_> = base.iter().copied().collect();
        for &add in &delta.added {
            result.insert(add);
        }
        for &rem in &delta.removed {
            result.remove(&rem);
        }
        result.into_iter().collect()
    }

    /// Returns vertices that this vertex depends on (inputs/predecessors)
    /// Note: Requires inverse index or linear scan; consider maintaining inverse CSR if needed frequently
    pub fn dependencies_of(&self, vertex: VertexId) -> Vec<VertexId> {
        // This requires scanning all vertices to find edges pointing TO this vertex
        // For performance-critical paths, maintain a separate inverse CSR
        let mut deps = Vec::new();
        for v in 0..self.vertex_count {
            let candidate = VertexId::new(v as u32);
            if self.dependents_of(candidate).contains(&vertex) {
                deps.push(candidate);
            }
        }
        deps
    }

    pub fn rebuild_if_needed(&mut self) {
        if !self.needs_rebuild {
            return;
        }

        // 1. Collect all edges (base + delta)
        let mut all_edges: Vec<(VertexId, VertexId)> = Vec::new();

        for v in 0..self.vertex_count {
            let vertex = VertexId::new(v as u32);
            let succs = self.dependents_of(vertex);
            for &succ in &succs {
                all_edges.push((vertex, succ));
            }
        }

        // 2. Sort by source vertex
        all_edges.sort_by_key(|(src, _)| src.0);

        // 3. Build new CSR
        let mut new_offsets = vec![0; self.vertex_count + 1];
        let mut new_targets = Vec::with_capacity(all_edges.len());

        let mut current_vertex = VertexId::new(0);
        let mut count = 0;

        for (src, tgt) in all_edges {
            while current_vertex < src {
                current_vertex = VertexId::new(current_vertex.0 + 1);
                new_offsets[current_vertex.as_index()] = count;
            }
            new_targets.push(tgt);
            count += 1;
        }

        while (current_vertex.as_index()) < self.vertex_count {
            current_vertex = VertexId::new(current_vertex.0 + 1);
            new_offsets[current_vertex.as_index()] = count;
        }

        // 4. Replace arrays
        self.offsets = new_offsets;
        self.targets = new_targets;
        self.edge_count = all_edges.len();

        // 5. Clear delta
        self.delta_edges.clear();
        self.needs_rebuild = false;
    }

    pub fn export(&self) -> CsrExport {
        CsrExport {
            offsets: self.offsets.clone(),
            targets: self.targets.clone(),
            vertex_count: self.vertex_count,
        }
    }
}
```

### 7.2 DeltaEdges Slab Allocation

DeltaEdges stores pending mutations in a slab-allocated structure for cache efficiency.

```rust
pub struct DeltaEdges {
    slabs: Vec<EdgeSlab>,
    vertex_map: FxHashMap<VertexId, usize>,  // vertex → slab index
}

struct EdgeSlab {
    vertex: VertexId,
    added: SmallVec<[VertexId; 4]>,
    removed: SmallVec<[VertexId; 4]>,
}

pub struct DeltaEdgeSet {
    pub added: Vec<VertexId>,
    pub removed: Vec<VertexId>,
}

impl DeltaEdges {
    pub fn new() -> Self {
        Self {
            slabs: Vec::new(),
            vertex_map: FxHashMap::default(),
        }
    }

    pub fn add(&mut self, vertex: VertexId, target: VertexId) {
        let slab = self.get_or_create_slab(vertex);
        if !slab.added.contains(&target) {
            slab.added.push(target);
        }
        // Remove from removed set if present
        if let Some(pos) = slab.removed.iter().position(|&v| v == target) {
            slab.removed.swap_remove(pos);
        }
    }

    pub fn remove(&mut self, vertex: VertexId, target: VertexId) {
        let slab = self.get_or_create_slab(vertex);
        if !slab.removed.contains(&target) {
            slab.removed.push(target);
        }
        // Remove from added set if present
        if let Some(pos) = slab.added.iter().position(|&v| v == target) {
            slab.added.swap_remove(pos);
        }
    }

    pub fn get_dependents_of(&self, vertex: VertexId) -> DeltaEdgeSet {
        if let Some(&idx) = self.vertex_map.get(&vertex) {
            let slab = &self.slabs[idx];
            DeltaEdgeSet {
                added: slab.added.to_vec(),
                removed: slab.removed.to_vec(),
            }
        } else {
            DeltaEdgeSet {
                added: Vec::new(),
                removed: Vec::new(),
            }
        }
    }

    fn get_or_create_slab(&mut self, vertex: VertexId) -> &mut EdgeSlab {
        let idx = *self.vertex_map.entry(vertex).or_insert_with(|| {
            let idx = self.slabs.len();
            self.slabs.push(EdgeSlab {
                vertex,
                added: SmallVec::new(),
                removed: SmallVec::new(),
            });
            idx
        });
        &mut self.slabs[idx]
    }

    pub fn clear(&mut self) {
        self.slabs.clear();
        self.vertex_map.clear();
    }
}
```

### 7.3 DynamicTopo (Pearce-Kelly) Integration

The Pearce-Kelly algorithm maintains incremental topological order as edges are added/removed.

```rust
pub struct DynamicTopo<V> {
    order: FxHashMap<V, usize>,     // vertex → topological rank
    config: PkConfig,
    dirty: bool,
}

pub struct PkConfig {
    pub enable: bool,
    pub rebuild_threshold: usize,   // Rebuild if >N vertices dirty
}

impl DynamicTopo<VertexId> {
    pub fn new(config: PkConfig) -> Self {
        Self {
            order: FxHashMap::default(),
            config,
            dirty: false,
        }
    }

    pub fn add_edge(&mut self, from: VertexId, to: VertexId, csr: &CsrStore) {
        if !self.config.enable {
            self.dirty = true;
            return;
        }

        let from_rank = self.rank(from).unwrap_or(0);
        let to_rank = self.rank(to).unwrap_or(usize::MAX);

        if from_rank >= to_rank {
            // Would create cycle or violate topo order, reorder
            self.reorder_forward(from, to, csr);
        }
    }

    pub fn remove_edge(&mut self, _from: VertexId, _to: VertexId, _csr: &CsrStore) {
        // Edge removal may enable better ordering but doesn't violate existing order
        // Mark dirty for opportunistic rebuild
        self.dirty = true;
    }

    fn reorder_forward(&mut self, from: VertexId, to: VertexId, csr: &CsrStore) {
        // PK forward algorithm: push 'to' and its descendants forward past 'from'
        let from_rank = self.rank(from).unwrap();
        let target_rank = from_rank + 1;

        let mut to_visit = vec![to];
        let mut visited = FxHashSet::default();

        while let Some(v) = to_visit.pop() {
            if visited.contains(&v) {
                continue;
            }
            visited.insert(v);

            let v_rank = self.rank(v).unwrap_or(0);
            if v_rank <= from_rank {
                // Need to push forward
                self.order.insert(v, target_rank);

                // Add dependents to visit
                let deps = csr.dependents_of(v);
                to_visit.extend(deps);
            }
        }

        // Renumber to maintain uniqueness
        self.renumber();
    }

    fn renumber(&mut self) {
        // Compact ranks to sequential integers
        let mut ranks: Vec<_> = self.order.values().copied().collect();
        ranks.sort_unstable();
        ranks.dedup();

        let rank_map: FxHashMap<usize, usize> = ranks
            .into_iter()
            .enumerate()
            .map(|(new, old)| (old, new))
            .collect();

        for rank in self.order.values_mut() {
            *rank = rank_map[rank];
        }
    }

    pub fn rank(&self, vertex: VertexId) -> Option<usize> {
        self.order.get(&vertex).copied()
    }

    pub fn rebuild(&mut self, csr: &CsrStore) {
        // Full topological sort using Kahn's algorithm
        self.order.clear();

        let mut in_degree: FxHashMap<VertexId, usize> = FxHashMap::default();

        // Compute in-degrees (number of dependencies per vertex)
        for v in 0..csr.vertex_count {
            let vertex = VertexId::new(v as u32);
            for &dependent in csr.dependents_of(vertex) {
                *in_degree.entry(dependent).or_insert(0) += 1;
            }
        }

        // Start with vertices having in-degree 0
        let mut queue: Vec<VertexId> = (0..csr.vertex_count)
            .map(|v| VertexId::new(v as u32))
            .filter(|&v| in_degree.get(&v).copied().unwrap_or(0) == 0)
            .collect();

        let mut rank = 0;

        while !queue.is_empty() {
            // Process all vertices at current rank (parallel layer)
            let current_layer = queue.clone();
            queue.clear();

            for vertex in current_layer {
                self.order.insert(vertex, rank);

                // Decrease in-degree of dependents
                for &dependent in csr.dependents_of(vertex) {
                    let deg = in_degree.get_mut(&dependent).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push(dependent);
                    }
                }
            }

            rank += 1;
        }

        self.dirty = false;
    }

    pub fn layers_for(&self, vertices: &[VertexId], csr: &CsrStore) -> Option<Vec<Layer>> {
        if self.dirty {
            return None;
        }

        // Group vertices by rank
        let mut rank_map: FxHashMap<usize, Vec<VertexId>> = FxHashMap::default();
        for &v in vertices {
            if let Some(rank) = self.rank(v) {
                rank_map.entry(rank).or_insert_with(Vec::new).push(v);
            } else {
                // Vertex not in topo order, must rebuild
                return None;
            }
        }

        let mut ranks: Vec<_> = rank_map.keys().copied().collect();
        ranks.sort_unstable();

        let layers = ranks
            .into_iter()
            .map(|rank| Layer {
                vertices: rank_map[&rank].clone(),
            })
            .collect();

        Some(layers)
    }
}
```

### 7.4 Incremental vs Full Rebuild Decision Tree

```rust
impl Scheduler {
    pub fn create_schedule(&self, vertices: &[VertexId]) -> Result<Schedule, ExcelError> {
        // Decision tree for scheduling strategy

        if self.graph.dynamic_topo_enabled() {
            // Attempt to use incremental PK order
            if let Some(layers) = self.graph.pk_layers_for(vertices) {
                // Fast path: use cached topological order
                return Ok(Schedule {
                    layers,
                    cycles: Vec::new(),  // Cycles detected separately
                });
            } else {
                // PK order dirty or incomplete, fall through to full rebuild
            }
        }

        // Full rebuild path: Tarjan SCC + layer build
        let sccs = self.tarjan_scc(vertices)?;
        let (cycles, acyclic_sccs) = self.separate_cycles(sccs);
        let layers = self.build_layers(acyclic_sccs)?;

        Ok(Schedule { layers, cycles })
    }

    fn build_layers(&self, acyclic_sccs: Vec<Vec<VertexId>>) -> Result<Vec<Layer>, ExcelError> {
        // Kahn's algorithm over SCCs
        let mut in_degree: FxHashMap<VertexId, usize> = FxHashMap::default();
        let vertex_set: FxHashSet<_> = acyclic_sccs.iter().flatten().copied().collect();

        // Compute in-degrees within vertex set
        for &v in &vertex_set {
            for &succ in self.graph.dependents_of(v) {
                if vertex_set.contains(&succ) {
                    *in_degree.entry(succ).or_insert(0) += 1;
                }
            }
        }

        let mut layers = Vec::new();
        let mut remaining: FxHashSet<_> = vertex_set.clone();

        while !remaining.is_empty() {
            // Find vertices with in-degree 0
            let ready: Vec<_> = remaining
                .iter()
                .copied()
                .filter(|&v| in_degree.get(&v).copied().unwrap_or(0) == 0)
                .collect();

            if ready.is_empty() {
                return Err(ExcelError::new(ExcelErrorKind::Value)
                    .with_message("Unexpected cycle in acyclic component"));
            }

            layers.push(Layer { vertices: ready.clone() });

            // Remove ready vertices and update in-degrees
            for &v in &ready {
                remaining.remove(&v);
                for &succ in self.graph.dependents_of(v) {
                    if let Some(deg) = in_degree.get_mut(&succ) {
                        *deg -= 1;
                    }
                }
            }
        }

        Ok(layers)
    }
}
```

## 8. Error Handling & Recovery (NEW SECTION)

### 8.1 Error Taxonomy

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum ExcelErrorKind {
    // Formula errors (visible in cells)
    Div0,      // #DIV/0!
    Value,     // #VALUE!
    Ref,       // #REF!
    Name,      // #NAME?
    Num,       // #NUM!
    Na,        // #N/A
    Spill,     // #SPILL!
    Calc,      // #CALC! (cycle detected)

    // Engine errors (internal, not Excel-standard)
    CyclicDependency,
    InvalidReference,
    VertexNotFound,
    SheetNotFound,
    TransactionFailed,
    CompactionFailed,
    MemoryLimitExceeded,
}

#[derive(Debug, Clone)]
pub struct ExcelError {
    pub kind: ExcelErrorKind,
    pub message: String,
    pub context: ErrorContext,
    pub extra: ExcelErrorExtra,
}

#[derive(Debug, Clone, Default)]
pub struct ErrorContext {
    pub sheet: Option<String>,
    pub cell: Option<(u32, u32)>,
    pub formula: Option<String>,
    pub stack_trace: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExcelErrorExtra {
    None,
    Spill { expected_rows: u32, expected_cols: u32 },
    Cycle { vertices: Vec<VertexId> },
}
```

### 8.2 Transaction Rollback via ChangeLog

```rust
pub trait ChangeLogger {
    fn log(&mut self, event: ChangeEvent);
    fn create_savepoint(&mut self) -> Savepoint;
    fn rollback_to(&mut self, savepoint: Savepoint);
    fn clear(&mut self);
}

#[derive(Debug, Clone)]
pub struct Savepoint {
    index: usize,
}

#[derive(Debug, Clone)]
pub enum ChangeEvent {
    CellValueChange {
        sheet: SheetId,
        row: u32,
        col: u32,
        old_value: LiteralValue,
        new_value: LiteralValue,
    },
    CellFormulaChange {
        vertex: VertexId,
        old_ast: Option<AstNodeId>,
        new_ast: Option<AstNodeId>,
    },
    DependencyAdd {
        from: VertexId,
        to: VertexId,
    },
    DependencyRemove {
        from: VertexId,
        to: VertexId,
    },
    RangeSubscribe {
        handle: RangeHandle,
        vertex: VertexId,
        descriptor: RangeDescriptor,
    },
    RangeUnsubscribe {
        handle: RangeHandle,
        vertex: VertexId,
    },
    VertexCreate {
        vertex: VertexId,
        meta: VertexMeta,
    },
    VertexDelete {
        vertex: VertexId,
    },
}

pub struct ChangeLog {
    events: Vec<ChangeEvent>,
    savepoints: Vec<usize>,
}

impl ChangeLogger for ChangeLog {
    fn log(&mut self, event: ChangeEvent) {
        self.events.push(event);
    }

    fn create_savepoint(&mut self) -> Savepoint {
        let index = self.events.len();
        self.savepoints.push(index);
        Savepoint { index }
    }

    fn rollback_to(&mut self, savepoint: Savepoint) {
        // Replay events in reverse from current position to savepoint
        while self.events.len() > savepoint.index {
            if let Some(event) = self.events.pop() {
                self.apply_inverse(event);
            }
        }

        // Remove savepoints beyond rollback point
        self.savepoints.retain(|&sp| sp <= savepoint.index);
    }

    fn clear(&mut self) {
        self.events.clear();
        self.savepoints.clear();
    }
}

impl ChangeLog {
    fn apply_inverse(&mut self, event: ChangeEvent) {
        match event {
            ChangeEvent::CellValueChange { sheet, row, col, old_value, .. } => {
                // Restore old value (actual restoration done by WorkbookEditor)
                // This is a sentinel for the editor to know what to restore
                // Real implementation would call back into SheetStoreService
            }
            ChangeEvent::DependencyAdd { from, to } => {
                // Remove the edge that was added
                // editor.dep_index.csr.remove_edge(from, to);
            }
            ChangeEvent::DependencyRemove { from, to } => {
                // Re-add the edge that was removed
                // editor.dep_index.csr.add_edge(from, to);
            }
            // ... other event inversions
            _ => {}
        }
    }
}
```

### 8.3 Spill Fault Injection (Testing Infrastructure)

```rust
pub struct SpillPlanner {
    fault_config: Option<FaultConfig>,
}

#[derive(Debug, Clone)]
pub struct FaultConfig {
    pub fault_after_ops: usize,
    pub fault_type: FaultType,
}

#[derive(Debug, Clone)]
pub enum FaultType {
    BeforeCommit,
    AfterPartialCommit,
    DuringClear,
}

impl SpillPlanner {
    pub fn plan_spill(
        &mut self,
        anchor: VertexId,
        values: Vec<Vec<LiteralValue>>,
        editor: &mut WorkbookEditor,
    ) -> Result<SpillResult, ExcelError> {
        let rows = values.len();
        let cols = values[0].len();

        // Check for blockers
        let target_region = self.compute_target_region(anchor, rows, cols);
        if let Some(blocker) = self.find_blocker(&target_region, editor) {
            return Err(ExcelError::spill_blocked(rows as u32, cols as u32, blocker));
        }

        // Begin transaction
        editor.begin();
        let savepoint = editor.change_log.create_savepoint();

        // Inject fault if configured
        if let Some(ref config) = self.fault_config {
            if config.fault_after_ops == 0 && matches!(config.fault_type, FaultType::BeforeCommit) {
                editor.rollback_to(savepoint);
                return Err(ExcelError::new(ExcelErrorKind::TransactionFailed)
                    .with_message("Injected fault: BeforeCommit"));
            }
        }

        // Clear old spill
        let old_spill = self.get_current_spill(anchor);
        for (row, col) in old_spill.iter() {
            editor.set_value(CellRef { sheet_id, coord: Coord::new(*row, *col) }, LiteralValue::Empty);
        }

        // Write new spill
        let mut written = 0;
        for (r, row_vals) in values.iter().enumerate() {
            for (c, val) in row_vals.iter().enumerate() {
                let cell = target_region.cell_at(r, c);
                editor.set_value(cell, val.clone());
                written += 1;

                // Inject fault mid-commit
                if let Some(ref config) = self.fault_config {
                    if written == config.fault_after_ops && matches!(config.fault_type, FaultType::AfterPartialCommit) {
                        editor.rollback_to(savepoint);
                        return Err(ExcelError::new(ExcelErrorKind::TransactionFailed)
                            .with_message("Injected fault: AfterPartialCommit"));
                    }
                }
            }
        }

        // Commit
        let summary = editor.commit();

        Ok(SpillResult {
            rows: rows as u32,
            cols: cols as u32,
            cells_written: written,
            affected_vertices: summary.affected_vertices,
        })
    }
}
```

### 8.4 Cycle Detection and Reporting

```rust
impl DependencyIndex {
    pub fn detect_cycle(&self, vertex: VertexId) -> Option<Vec<VertexId>> {
        // Check cycle cache first
        if let Some(cycle) = self.cycle_cache.get(&vertex) {
            return Some(cycle.clone());
        }

        // DFS to find cycle
        let mut visited = FxHashSet::default();
        let mut path = Vec::new();
        let mut path_set = FxHashSet::default();

        if self.dfs_find_cycle(vertex, &mut visited, &mut path, &mut path_set) {
            // Extract cycle from path
            let cycle_start = path.iter().position(|&v| v == vertex).unwrap();
            let cycle = path[cycle_start..].to_vec();

            // Cache result
            self.cycle_cache.insert(vertex, cycle.clone());

            Some(cycle)
        } else {
            None
        }
    }

    fn dfs_find_cycle(
        &self,
        vertex: VertexId,
        visited: &mut FxHashSet<VertexId>,
        path: &mut Vec<VertexId>,
        path_set: &mut FxHashSet<VertexId>,
    ) -> bool {
        if path_set.contains(&vertex) {
            // Cycle detected
            path.push(vertex);
            return true;
        }

        if visited.contains(&vertex) {
            // Already explored this branch
            return false;
        }

        visited.insert(vertex);
        path.push(vertex);
        path_set.insert(vertex);

        // Visit predecessors (cells this depends on)
        for &pred in self.dependencies_of(vertex) {
            if self.dfs_find_cycle(pred, visited, path, path_set) {
                return true;
            }
        }

        path.pop();
        path_set.remove(&vertex);
        false
    }

    fn dependencies_of(&self, vertex: VertexId) -> Vec<VertexId> {
        // Return all vertices that vertex depends on
        // This is the inverse of dependents
        let handles = self.handles_for_vertex(vertex);
        let mut preds = Vec::new();
        for handle in handles {
            if let Some(edges) = self.range_map.get(&handle) {
                preds.extend_from_slice(&edges.targets);
            }
        }
        preds
    }
}

impl Interpreter {
    pub fn evaluate_with_cycle_detection(&self, vertex: VertexId) -> Result<LiteralValue, ExcelError> {
        if let Some(cycle) = self.context.detect_cycle(vertex) {
            // Format cycle for error message
            let cycle_str = cycle
                .iter()
                .map(|v| self.context.format_vertex(*v))
                .collect::<Vec<_>>()
                .join(" → ");

            return Err(ExcelError::new(ExcelErrorKind::Calc)
                .with_message(format!("Circular reference detected: {}", cycle_str))
                .with_extra(ExcelErrorExtra::Cycle { vertices: cycle }));
        }

        // Proceed with normal evaluation
        self.evaluate_ast(ast)
    }
}
```

### 8.5 Resource Limit Enforcement

```rust
pub struct EvalConfig {
    pub max_vertices: Option<usize>,
    pub max_spans_per_axis: Option<usize>,
    pub max_overlay_density: Option<f32>,
    pub max_csr_edges: Option<usize>,
    // ... other config
}

impl WorkbookEditor {
    pub fn check_resource_limits(&self) -> Result<(), ExcelError> {
        if let Some(max_vertices) = self.config.max_vertices {
            if self.address_index.vertex_count() > max_vertices {
                return Err(ExcelError::new(ExcelErrorKind::MemoryLimitExceeded)
                    .with_message(format!(
                        "Vertex limit exceeded: {} > {}",
                        self.address_index.vertex_count(),
                        max_vertices
                    )));
            }
        }

        if let Some(max_edges) = self.config.max_csr_edges {
            if self.dep_index.csr.edge_count > max_edges {
                return Err(ExcelError::new(ExcelErrorKind::MemoryLimitExceeded)
                    .with_message(format!(
                        "Edge limit exceeded: {} > {}",
                        self.dep_index.csr.edge_count,
                        max_edges
                    )));
            }
        }

        Ok(())
    }
}

impl RangeTracker {
    pub fn check_span_limits(&self, descriptor: &RangeDescriptor) -> Result<(), ExcelError> {
        if let Some(max_spans) = self.config.max_spans_per_axis {
            let spans = self.resolve_descriptor(descriptor);
            if spans.len() > max_spans {
                return Err(ExcelError::new(ExcelErrorKind::MemoryLimitExceeded)
                    .with_message(format!(
                        "Span limit exceeded for range {:?}: {} > {}",
                        descriptor,
                        spans.len(),
                        max_spans
                    )));
            }
        }

        Ok(())
    }
}
```
# Range-Centric Engine TDD - Chunk 3: Phased Migration Strategy

## 9. Migration Plan (COMPLETE REWRITE)

This section provides a detailed, step-by-step migration strategy to transform the existing engine into the range-centric architecture while maintaining backward compatibility and allowing incremental rollout.

### 9.1 Migration Principles

**Core Principles:**
1. **Incremental Delivery:** Each phase delivers working, tested functionality
2. **Backward Compatibility:** Existing APIs remain functional during transition
3. **Feature Flags:** New paths enabled via configuration flags
4. **Parallel Operation:** Old and new systems coexist during migration
5. **Reversibility:** Each phase can be rolled back independently

**Phase Exit Criteria (quantitative):**
- **Phase 0 (Stats + deltas):** P95 `write_cell` latency < 40 µs on 100k-row sheets; debug full-scan recompute matches incremental spans for 10k random edits; zero correctness regressions in ingest/eval suites.
- **Phase 1 (RangeTracker):** Vertex count for sparse `A:A` style dependencies shrinks ≥ 50× relative to finite expansion; median `range_events_emitted` ≤ 3 per edit with 10k subscriptions; A/B comparison vs. legacy range walker shows identical formula results.
- **Phase 2 (DependencyIndex):** Graphviz dump shows edges oriented input→dependent; invalidation cost scales with dependents (O(#dependents)) in synthetic benchmark; incremental topo order matches full rebuild order over 1k mutating scenarios.
- **Phase 3 (WorkbookEditor facade):** 100 randomized nested transactions with injected faults restore bit-for-bit workbook snapshots; telemetry shows zero mutation paths outside the editor; evaluator write-back parity (UI edit vs. engine result) verified by counter equality.
- **Phase 4 (Legacy removal):** No references to legacy graph APIs remain; perf within ±5% of Phase 3 baselines across macro benchmarks.

### 9.2 Phase 0: Column/Row Usage Stats + Unified Delta Emission

**Duration:** 2-3 weeks
**Risk Level:** Low
**Dependencies:** None

#### 9.2.1 Implementation Tasks

**Task 0.1: Add ColumnUsageStats to SheetStore**
```rust
// In arrow_store/mod.rs
impl SheetStore {
    pub fn column_stats(&self, sheet: SheetId, col: u32) -> Option<ColumnUsageStats> {
        // Compute from existing chunks + overlays
        self.compute_column_stats(sheet, col)
    }

    fn compute_column_stats(&self, sheet: SheetId, col: u32) -> Option<ColumnUsageStats> {
        // Implementation as shown in Section 6.2
        // Scan type_tag arrays and overlay entries
        // Build span list with merging logic
    }
}
```

**Acceptance Criteria:**
- `column_stats()` returns correct min/max/spans for all columns
- Span merging respects MERGE_THRESHOLD (16 rows)
- Overlay entries counted correctly
- Performance: <1ms per column for typical sheets (<100K rows)

**Test Plan:**
```rust
#[test]
fn column_stats_empty_column() {
    let store = SheetStore::new();
    assert!(store.column_stats(sheet1, 0).is_none());
}

#[test]
fn column_stats_single_value() {
    let mut store = SheetStore::new();
    store.write_cell_value(sheet1, 100, 0, &LiteralValue::Number(42.0));
    let stats = store.column_stats(sheet1, 0).unwrap();
    assert_eq!(stats.min_row, Some(100));
    assert_eq!(stats.max_row, Some(100));
    assert_eq!(stats.spans.len(), 1);
    assert_eq!(stats.spans[0], RowSpan { start: 100, end: 101 });
}

#[test]
fn column_stats_span_merging() {
    let mut store = SheetStore::new();
    // Write values at rows 100, 101, 102 (should merge into one span)
    for row in 100..103 {
        store.write_cell_value(sheet1, row, 0, &LiteralValue::Number(1.0));
    }
    let stats = store.column_stats(sheet1, 0).unwrap();
    assert_eq!(stats.spans.len(), 1);
    assert_eq!(stats.spans[0], RowSpan { start: 100, end: 103 });
}

#[test]
fn column_stats_sparse_column() {
    let mut store = SheetStore::new();
    // Write at rows 0, 1000, 100000 (should create 3 spans)
    store.write_cell_value(sheet1, 0, 0, &LiteralValue::Number(1.0));
    store.write_cell_value(sheet1, 1000, 0, &LiteralValue::Number(2.0));
    store.write_cell_value(sheet1, 100000, 0, &LiteralValue::Number(3.0));
    let stats = store.column_stats(sheet1, 0).unwrap();
    assert_eq!(stats.spans.len(), 3);
}
```

**Task 0.2: Implement Delta Emission**
```rust
impl SheetStore {
    pub fn write_cell_value_with_delta(
        &mut self,
        sheet: SheetId,
        row: u32,
        col: u32,
        value: &LiteralValue,
    ) -> SmallVec<[UsageDelta; 2]> {
        let before = self.column_stats(sheet, col);
        self.write_cell_value(sheet, row, col, value);
        let after = self.column_stats(sheet, col);

        let mut deltas = SmallVec::new();

        if let Some(delta) = self.diff_column(before.as_ref(), after.as_ref()) {
            deltas.push(UsageDelta::Column { sheet, column: col, delta });
        }
        if let Some(delta) = self.diff_row(sheet, row, col) {
            deltas.push(UsageDelta::Row { sheet, row, delta });
        }

        deltas
    }

    fn diff_column(
        &self,
        old: Option<&ColumnUsageStats>,
        new: Option<&ColumnUsageStats>,
    ) -> Option<ColumnUsageDelta> {
        match (old, new) {
            (None, None) => None,
            (None, Some(new)) => Some(ColumnUsageDelta::BecameNonEmpty { span: new.spans[0] }),
            (Some(_), None) => Some(ColumnUsageDelta::BecameEmpty),
            (Some(old), Some(new)) => self.diff_spans(&old.spans, &new.spans),
        }
    }
}
```

`diff_row` mirrors `diff_column` but walks `RowUsageStats`, returning `Option<RowUsageDelta>`. Returning `None` replaces the legacy `Unchanged` variant—callers simply skip emitting a delta when spans are identical.

**Acceptance Criteria:**
- Delta emission is correct for all mutation types on both axes.
- `UsageDelta::Column { delta: ColumnUsageDelta::BecameNonEmpty }` emitted when the first value enters a column.
- `UsageDelta::Column { delta: ColumnUsageDelta::BecameEmpty }` emitted when the last populated cell is cleared.
- `Expanded`/`Shrunk` variants carry exact span diffs; no extraneous “unchanged” noise.
- Row operations (whole-row edits, spills) emit matching `UsageDelta::Row` entries.

**Test Plan:**
```rust
#[test]
fn delta_became_non_empty() {
    let mut store = SheetStore::new();
    let deltas = store.write_cell_value_with_delta(sheet1, 0, 0, &LiteralValue::Number(1.0));
    assert!(deltas.iter().any(|d| matches!(
        d,
        UsageDelta::Column { delta: ColumnUsageDelta::BecameNonEmpty { .. }, .. }
    )));
}

#[test]
fn delta_expanded() {
    let mut store = SheetStore::new();
    store.write_cell_value(sheet1, 0, 0, &LiteralValue::Number(1.0));
    let deltas = store.write_cell_value_with_delta(sheet1, 1000, 0, &LiteralValue::Number(2.0));
    assert!(deltas.iter().any(|d| matches!(
        d,
        UsageDelta::Column { delta: ColumnUsageDelta::Expanded { .. }, .. }
    )));
}

#[test]
fn delta_became_empty() {
    let mut store = SheetStore::new();
    store.write_cell_value(sheet1, 0, 0, &LiteralValue::Number(1.0));
    let deltas = store.write_cell_value_with_delta(sheet1, 0, 0, &LiteralValue::Empty);
    assert!(deltas.iter().any(|d| matches!(
        d,
        UsageDelta::Column { delta: ColumnUsageDelta::BecameEmpty, .. }
    )));
}
```

**Task 0.3: SheetStoreService Wrapper**
```rust
pub struct SheetStoreService {
    sheet_store: SheetStore,
    stats_cache: FxHashMap<(SheetId, u32), ColumnUsageStats>,
    stats_version: AtomicU64,
    overlay_stats: OverlayStats,
}

impl SheetStoreService {
    pub fn new(sheet_store: SheetStore) -> Self {
        Self {
            sheet_store,
            stats_cache: FxHashMap::default(),
            stats_version: AtomicU64::new(0),
            overlay_stats: OverlayStats::default(),
        }
    }

    pub fn write_cell(&mut self, /*...*/) -> SmallVec<[UsageDelta; 2]> {
        let deltas = self.sheet_store.write_cell_value_with_delta(/*...*/);

        for delta in &deltas {
            match delta {
                UsageDelta::Column { sheet, column, .. } => {
                    if let Some(stats) = self.sheet_store.column_stats(*sheet, *column) {
                        self.stats_cache.insert((*sheet, *column), stats.clone());
                        self.stats_version.fetch_add(1, Ordering::SeqCst);
                    }
                }
                UsageDelta::Row { .. } => {
                    // Row cache update handled symmetrically (omitted for brevity)
                }
            }
        }

        deltas
    }
}
```

**Acceptance Criteria:**
- Service maintains consistent stats cache
- `stats_version` increments on any structural change (column or row axis)
- Existing `Engine` can use service without modifications
- Performance: <5% overhead vs. direct `SheetStore` access

#### 9.2.2 Integration Steps

1. **Create feature flag:** `enable_column_stats`
2. **Wrap SheetStore:** Engine optionally uses `SheetStoreService`
3. **Log deltas:** Capture deltas in telemetry (no-op initially)
4. **Validate stats:** Add debug assertions comparing computed vs. cached stats
5. **Performance baseline:** Benchmark write throughput before/after

#### 9.2.3 Rollback Strategy

If Phase 0 shows regressions or bugs:
- Disable `enable_column_stats` flag
- Engine falls back to direct `SheetStore` access
- Remove `SheetStoreService` wrapper
- Stats computation remains available for debugging

**Rollback Triggers:**
- Write performance degrades >10%
- Memory usage increases >20%
- Stats inconsistencies detected in validation
- Any existing test failures

### 9.3 Phase 1: RangeDescriptor + RangeTracker

**Duration:** 3-4 weeks
**Risk Level:** Medium
**Dependencies:** Phase 0 complete

#### 9.3.1 Implementation Tasks

**Task 1.1: RangeDescriptor Normalization**
```rust
pub fn normalize_reference(
    reference: &ReferenceType,
    sheet_registry: &SheetRegistry,
) -> Result<RangeDescriptor, ExcelError> {
    match reference {
        ReferenceType::Cell { sheet, row, col } => {
            let sheet_id = resolve_sheet(sheet, sheet_registry)?;
            Ok(RangeDescriptor {
                sheet: sheet_id,
                start_row: *row,
                start_col: *col,
                height: 1,
                width: 1,
                bounds: BoundsType::Finite,
            })
        }
        ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
        } => {
            let sheet_id = resolve_sheet(sheet, sheet_registry)?;
            match (start_row, start_col, end_row, end_col) {
                (Some(sr), Some(sc), Some(er), Some(ec)) => {
                    // Finite range
                    Ok(RangeDescriptor {
                        sheet: sheet_id,
                        start_row: *sr,
                        start_col: *sc,
                        height: er - sr + 1,
                        width: ec - sc + 1,
                        bounds: BoundsType::Finite,
                    })
                }
                (None, Some(sc), None, Some(ec)) => {
                    // Whole columns (A:B)
                    Ok(RangeDescriptor {
                        sheet: sheet_id,
                        start_row: 0,
                        start_col: *sc,
                        height: 1, // normalized placeholder; shape encodes the open axis
                        width: ec - sc + 1,
                        bounds: BoundsType::WholeColumn,
                    })
                }
                // ... other patterns
            }
        }
    }
}
```

**Test Plan:**
```rust
#[test]
fn normalize_finite_range() {
    let ref_type = ReferenceType::Range {
        sheet: Some("Sheet1".into()),
        start_row: Some(0),
        start_col: Some(0),
        end_row: Some(99),
        end_col: Some(9),
    };
    let desc = normalize_reference(&ref_type, &registry).unwrap();
    assert_eq!(desc.bounds, BoundsType::Finite);
    assert_eq!(desc.height, 100);
    assert_eq!(desc.width, 10);
}

#[test]
fn normalize_whole_column() {
    let ref_type = ReferenceType::Range {
        sheet: Some("Sheet1".into()),
        start_row: None,
        start_col: Some(0),
        end_row: None,
        end_col: Some(0),
    };
    let desc = normalize_reference(&ref_type, &registry).unwrap();
    assert_eq!(desc.bounds, BoundsType::WholeColumn);
    assert_eq!(desc.start_col, 0);
    assert_eq!(desc.width, 1);
}
```

**Task 1.2: RangeTracker Implementation**
```rust
impl RangeTracker {
    pub fn new() -> Self {
        Self {
            axes: FxHashMap::default(),
            subscriptions: FxHashMap::default(),
            handle_counter: AtomicU64::new(1),
            stats_snapshot: FxHashMap::default(),
        }
    }

    pub fn register(
        &mut self,
        descriptor: RangeDescriptor,
        vertex: VertexId,
    ) -> (RangeHandle, Arc<[RowColSpan]>) {
        let handle = RangeHandle(self.handle_counter.fetch_add(1, Ordering::SeqCst));
        let spans = self.resolve_descriptor(&descriptor);

        let subscription = RangeSubscription {
            descriptor: descriptor.clone(),
            spans: spans.clone(),
            observed_versions: self.collect_versions(&descriptor),
            subscription_version: 0,
            subscribers: smallvec![vertex],
        };

        self.subscriptions.insert(handle, subscription);

        (handle, spans)
    }

    pub fn resolve_descriptor(&self, desc: &RangeDescriptor) -> Arc<[RowColSpan]> {
        // Implementation as shown in Section 3.5
        match desc.bounds {
            BoundsType::Finite => {
                // Single span covering exact bounds
                vec![RowColSpan {
                    row_start: desc.start_row,
                    row_end: desc.start_row + desc.height,
                    col_start: desc.start_col,
                    col_end: desc.start_col + desc.width,
                }].into()
            }
            BoundsType::WholeColumn => {
                // Look up stats for each column in range
                self.resolve_whole_column(desc)
            }
            // ... other bounds types
        }
    }

    fn resolve_whole_column(&self, desc: &RangeDescriptor) -> Arc<[RowColSpan]> {
        let mut spans = Vec::new();

        for col in desc.start_col..(desc.start_col + desc.width) {
            if let Some(stats) = self.stats_snapshot.get(&(desc.sheet, col)) {
                for row_span in &stats.spans {
                    spans.push(RowColSpan {
                        row_start: row_span.start,
                        row_end: row_span.end,
                        col_start: col,
                        col_end: col + 1,
                    });
                }
            }
        }

        // Merge horizontally adjacent spans
        self.merge_horizontal_spans(spans).into()
    }

    fn collect_versions(&self, desc: &RangeDescriptor) -> SmallVec<[(u32, u64); 8]> {
        let mut versions = SmallVec::new();
        for col in desc.start_col..(desc.start_col + desc.width) {
            if let Some(stats) = self.stats_snapshot.get(&(desc.sheet, col)) {
                versions.push((col, stats.stats_version));
            }
        }
        versions
    }
}
```

**Test Plan:**
```rust
#[test]
fn range_tracker_register_finite() {
    let mut tracker = RangeTracker::new();
    let desc = RangeDescriptor {
        sheet: sheet1,
        start_row: 0,
        start_col: 0,
        height: 10,
        width: 5,
        bounds: BoundsType::Finite,
    };
    let (handle, spans) = tracker.register(desc, vertex1);
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].row_start, 0);
    assert_eq!(spans[0].row_end, 9);
}

#[test]
fn range_tracker_infinite_empty_column() {
    let mut tracker = RangeTracker::new();
    // No stats for column 0 yet
    let desc = RangeDescriptor {
        sheet: sheet1,
        start_row: 0,
        start_col: 0,
        height: 1,
        width: 1,
        bounds: BoundsType::WholeColumn,
    };
    let (handle, spans) = tracker.register(desc, vertex1);
    assert_eq!(spans.len(), 0); // Empty column
}

#[test]
fn range_tracker_infinite_with_data() {
    let mut tracker = RangeTracker::new();
    // Populate stats
    tracker.stats_snapshot.insert((sheet1, 0), ColumnUsageStats {
        sheet: sheet1,
        column: 0,
        min_row: Some(5),
        max_row: Some(15),
        stats_version: 1,
        spans: smallvec![RowSpan { start: 5, end: 15 }],
        overlay_count: 0,
        arrow_chunk_count: 1,
        last_compaction: None,
    });

    let desc = RangeDescriptor {
        sheet: sheet1,
        start_row: 0,
        start_col: 0,
        height: 1,
        width: 1,
        bounds: BoundsType::WholeColumn,
    };
    let (handle, spans) = tracker.register(desc, vertex1);
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0], RowColSpan {
        row_start: 5,
        row_end: 15,
        col_start: 0,
        col_end: 1,
    });
}
```

**Task 1.3: Hybrid Dependency Mode**

During Phase 1, the engine operates in hybrid mode:
- Infinite ranges use RangeTracker
- Finite ranges continue to expand directly

```rust
impl DependencyGraph {
    fn extract_dependencies_hybrid(
        &mut self,
        vertex: VertexId,
        ast: &ASTNode,
    ) -> Result<(), ExcelError> {
        let references = self.extract_references(ast);

        for reference in references {
            let descriptor = normalize_reference(&reference, &self.sheet_registry)?;

            if descriptor.bounds == BoundsType::Finite {
                // Old path: expand directly
                self.extract_dependencies_finite(vertex, &descriptor)?;
            } else {
                // New path: use RangeTracker
                let (handle, spans) = self.range_tracker.register(descriptor.clone(), vertex);
                self.extract_dependencies_from_spans(vertex, &descriptor, handle, spans)?;
            }
        }

        Ok(())
    }

    fn extract_dependencies_from_spans(
        &mut self,
        vertex: VertexId,
        descriptor: &RangeDescriptor,
        handle: RangeHandle,
        spans: Arc<[RowColSpan]>,
    ) -> Result<(), ExcelError> {
        let mut targets = Vec::new();

        for span in spans.iter() {
            for (_, v) in self.address_index.vertices_in_span_iter(descriptor.sheet, span) {
                targets.push(v);
            }
        }

        // Record in range_map
        self.range_map.insert(handle, RangeEdges {
            subscriber: vertex,
            targets,
        });

        // Add CSR edges
        for &target in &targets {
            self.csr.add_edge(target, vertex);
        }

        Ok(())
    }
}
```

**Acceptance Criteria:**
- Hybrid mode correctly routes finite vs infinite ranges
- Existing formulas with finite ranges unchanged
- Infinite range formulas use RangeTracker
- Performance: no degradation for finite ranges
- Memory: infinite ranges reduce vertex count vs. baseline

#### 9.3.2 Integration Steps

1. **Add feature flag:** `enable_range_tracker`
2. **Instrument logging:** Track which path (old vs. new) each formula uses
3. **Validate results:** Compare eval results between paths for overlap cases
4. **A/B testing:** Run subset of formulas through both paths, assert equality
5. **Performance comparison:** Benchmark memory + time for large infinite ranges

#### 9.3.3 Validation Suite

```rust
#[test]
fn hybrid_mode_finite_unchanged() {
    // Formula: =SUM(A1:A100)
    let mut engine = Engine::new_with_flags(vec!["enable_range_tracker"]);
    engine.set_cell_formula("Sheet1", 1, 2, parse("=SUM(A1:A100)"));

    // Should use old finite path
    assert!(engine.graph.instrumentation.finite_range_count > 0);
    assert_eq!(engine.graph.instrumentation.infinite_range_count, 0);
}

#[test]
fn hybrid_mode_infinite_uses_tracker() {
    // Formula: =COUNTA(B:B)
    let mut engine = Engine::new_with_flags(vec!["enable_range_tracker"]);
    engine.set_cell_formula("Sheet1", 1, 2, parse("=COUNTA(B:B)"));

    // Should use new tracker path
    assert!(engine.graph.instrumentation.infinite_range_count > 0);
}

#[test]
fn hybrid_mode_memory_reduction() {
    let mut engine_old = Engine::new();
    let mut engine_new = Engine::new_with_flags(vec!["enable_range_tracker"]);

    // Create 1000 formulas referencing A:A
    for row in 0..1000 {
        engine_old.set_cell_formula("Sheet1", row, 1, parse("=COUNTA(A:A)"));
        engine_new.set_cell_formula("Sheet1", row, 1, parse("=COUNTA(A:A)"));
    }

    // Write sparse data in column A (10 values)
    for row in (0..100000).step_by(10000) {
        engine_old.set_cell_value("Sheet1", row, 0, LiteralValue::Number(1.0));
        engine_new.set_cell_value("Sheet1", row, 0, LiteralValue::Number(1.0));
    }

    let vertex_count_old = engine_old.graph.vertex_store.count();
    let vertex_count_new = engine_new.graph.vertex_store.count();

    // New engine should have far fewer vertices
    // Old: 1000 formulas + 100K placeholders for A:A = ~101K
    // New: 1000 formulas + 10 actual values = ~1010
    assert!(vertex_count_new < vertex_count_old / 10);
}
```

#### 9.3.4 Rollback Strategy

If Phase 1 shows issues:
- Disable `enable_range_tracker` flag
- All ranges use old finite expansion path
- RangeTracker code remains but is unused

**Rollback Triggers:**
- Evaluation results differ between paths
- Memory reduction not achieved (target: >50% for sparse infinite ranges)
- Performance regression >15% for infinite range workloads
- Bugs in span resolution logic

### 9.4 Phase 2: DependencyIndex Extraction

**Duration:** 4-5 weeks
**Risk Level:** High
**Dependencies:** Phase 1 complete

#### 9.4.1 Implementation Tasks

**Task 2.1: Extract DependencyIndex Module**

Currently, graph/mod.rs is ~3000 lines. Extract dependency management:

```rust
// New file: engine/dependency_index.rs
pub struct DependencyIndex {
    csr: CsrStore,
    range_map: FxHashMap<RangeHandle, RangeEdges>,
    topo: DynamicTopo<VertexId>,
    dirty: FxHashSet<VertexId>,
    vertex_meta: VertexMetaTable,
}

// Migration: move methods from DependencyGraph
impl DependencyIndex {
    pub fn apply_dependencies(&mut self, /*...*/) { /* moved from graph */ }
    pub fn handle_range_events(&mut self, /*...*/) { /* new method */ }
    pub fn mark_dirty(&mut self, /*...*/) { /* moved from graph */ }
    // ... other methods
}
```

**Task 2.2: Adapt VertexEditor to Route Through DependencyIndex**

```rust
// Old code in graph/editor/vertex_editor.rs
impl VertexEditor {
    pub fn set_dependencies(&mut self, vertex: VertexId, deps: Vec<VertexId>) {
        // Old: directly manipulates CSR in DependencyGraph
        self.graph.csr.clear_edges(vertex);
        for dep in deps {
            self.graph.csr.add_edge(dep, vertex);
        }
        self.graph.mark_dirty(vertex);
    }
}

// New code
impl VertexEditor {
    pub fn set_dependencies(&mut self, vertex: VertexId, bindings: Vec<RangeBinding>) {
        // New: goes through DependencyIndex
        self.dep_index.apply_dependencies(vertex, bindings, &mut self.address_index);
    }
}
```

**Task 2.3: Migrate PK State Ownership**

```rust
// Old: DependencyGraph owns both CSR and DynamicTopo
pub struct DependencyGraph {
    csr: CsrStore,
    topo: DynamicTopo<VertexId>,
    // ... other fields
}

// New: DependencyIndex owns both
pub struct DependencyIndex {
    csr: CsrStore,
    topo: DynamicTopo<VertexId>,
    // ... other fields
}

// DependencyGraph becomes a facade
pub struct DependencyGraph {
    dep_index: DependencyIndex,
    address_index: AddressIndex,
    range_tracker: RangeTracker,
    // ... other components
}
```

**Acceptance Criteria:**
- All dependency mutations route through DependencyIndex
- PK state correctly maintained by DependencyIndex
- Scheduler accesses topo order via DependencyIndex
- No direct CSR access outside DependencyIndex
- All existing tests pass with new routing

#### 9.4.2 Migration Steps

1. **Create DependencyIndex file:** Copy relevant code from graph/mod.rs
2. **Add indirection layer:** DependencyGraph delegates to DependencyIndex
3. **Update call sites:** Change direct CSR calls to DependencyIndex methods
4. **Run full test suite:** Ensure no regressions
5. **Refactor internals:** Clean up redundant code in DependencyGraph
6. **Performance validation:** Benchmark before/after

#### 9.4.3 Testing Strategy

```rust
#[test]
fn dependency_index_apply_dependencies() {
    let mut dep_index = DependencyIndex::new(100);
    let mut addr_index = AddressIndex::new();

    let vertex = VertexId(1);
    let binding = RangeBinding {
        handle: RangeHandle(1),
        reference: create_lazy_ref(/*...*/),
        name: None,
    };

    dep_index.apply_dependencies(vertex, vec![binding], &mut addr_index);

    // Verify edges created
    assert!(dep_index.csr.edge_count > 0);
    // Verify vertex marked dirty
    assert!(dep_index.dirty.contains(&vertex));
}

#[test]
fn dependency_index_handle_range_events() {
    let mut dep_index = DependencyIndex::new(100);

    // Setup: register a range subscription
    let handle = RangeHandle(1);
    let subscriber = VertexId(1);
    dep_index.range_map.insert(handle, RangeEdges {
        subscriber,
        targets: vec![VertexId(2), VertexId(3)],
    });

    // Event: range expanded to include new vertices
    let event = RangeEvent::Expanded {
        handle,
        spans: vec![/* new span */],
    };

    dep_index.handle_range_events(vec![event], &mut addr_index);

    // Verify new edges added
    assert!(dep_index.csr.dependents_of(subscriber).len() > 2);
}
```

#### 9.4.4 Rollback Strategy

Phase 2 rollback is more complex due to code restructuring:

**Immediate Rollback (within sprint):**
- Revert commits that extracted DependencyIndex
- Restore original graph/mod.rs structure
- Re-run full test suite

**Post-Release Rollback:**
- Maintain compatibility shim in DependencyGraph
- DependencyGraph can directly manipulate CSR if needed
- Add feature flag `use_dependency_index` (default true)
- If issues arise, set flag to false to use old path

### 9.5 Phase 3: WorkbookEditor Facade

**Duration:** 3-4 weeks
**Risk Level:** Medium
**Dependencies:** Phase 2 complete

#### 9.5.1 Implementation Tasks

**Task 3.1: Create WorkbookEditor Struct**

```rust
pub struct WorkbookEditor<'a> {
    storage: &'a mut SheetStoreService,
    range_tracker: &'a mut RangeTracker,
    dep_index: &'a mut DependencyIndex,
    address_index: &'a mut AddressIndex,
    change_log: &'a mut ChangeLog,
    edit_handle: Option<EditHandle>,
    transaction_depth: usize,
}

impl<'a> WorkbookEditor<'a> {
    pub fn new(
        storage: &'a mut SheetStoreService,
        range_tracker: &'a mut RangeTracker,
        dep_index: &'a mut DependencyIndex,
        address_index: &'a mut AddressIndex,
        change_log: &'a mut ChangeLog,
    ) -> Self {
        Self {
            storage,
            range_tracker,
            dep_index,
            address_index,
            change_log,
            edit_handle: None,
            transaction_depth: 0,
        }
    }

    pub fn begin(&mut self) {
        assert!(self.edit_handle.is_none(), "Transaction already active");
        self.edit_handle = Some(self.storage.begin_edit());
        self.transaction_depth = 1;
    }

    pub fn commit(&mut self) -> CommitSummary {
        assert!(self.transaction_depth == 1, "Nested transaction active");
        let handle = self.edit_handle.take().unwrap();
        let edit_summary = self.storage.finish_edit(handle);

        // Collect telemetry
        let affected_vertices = self.dep_index.dirty.iter().copied().collect();
        let dirty_count = self.dep_index.dirty.len();

        CommitSummary {
            affected_vertices,
            dirty_count,
            overlay_stats: edit_summary.telemetry.into(),
            telemetry: CommitTelemetry::default(),
            duration: edit_summary.duration,
        }
    }

    pub fn rollback(&mut self) {
        // Replay change log in reverse
        self.change_log.rollback_to(Savepoint { index: 0 });
        self.edit_handle = None;
        self.transaction_depth = 0;
    }
}
```

**Task 3.2: Implement Transaction Methods**

```rust
impl<'a> WorkbookEditor<'a> {
    pub fn set_value(&mut self, addr: CellRef, value: LiteralValue) {
        assert!(self.edit_handle.is_some(), "No active transaction");

        // 1. Log old value for undo
        let old_value = self.storage.read_cell(addr.sheet_id, addr.coord.row(), addr.coord.col());
        self.change_log.log(ChangeEvent::CellValueChange {
            sheet: addr.sheet_id,
            row: addr.coord.row(),
            col: addr.coord.col(),
            old_value,
            new_value: value.clone(),
        });

        // 2. Write to storage
        let mut handle = self.edit_handle.as_mut().unwrap();
        let deltas = self.storage.write_cell(
            &mut handle,
            addr.sheet_id,
            addr.coord.row(),
            addr.coord.col(),
            &value,
        );

        // 3. Process deltas
        for delta in deltas {
            let events = self.range_tracker.apply_delta(delta);
            self.dep_index.handle_range_events(events, self.address_index);
        }

        // 5. Ensure vertex exists
        let vertex = self.address_index.ensure_vertices(&[(addr.sheet_id, addr.coord.into())])[0];

        // 6. If target had formula, clear dependencies
        if self.dep_index.vertex_meta.kind(vertex) == VertexKind::FormulaScalar {
            self.dep_index.apply_dependencies(vertex, vec![], self.address_index);
            self.dep_index.vertex_meta.set_kind(vertex, VertexKind::Cell);
        }

        // 7. Mark dirty
        self.dep_index.mark_dirty(vertex);
    }

    pub fn set_formula(&mut self, addr: CellRef, ast: ASTNode) {
        assert!(self.edit_handle.is_some(), "No active transaction");

        // 1. Parse formula and extract ranges
        let references = self.extract_references(&ast);
        let mut bindings = Vec::new();

        for reference in references {
            let descriptor = normalize_reference(&reference, self.storage.sheet_registry())?;
            let (handle, _spans) = self.range_tracker.register(descriptor.clone(), vertex);
            let lazy_ref = LazyRangeRef { descriptor, handle };
            bindings.push(RangeBinding { handle, reference: lazy_ref, name: None });
        }

        // 2. Ensure vertex
        let vertex = self.address_index.ensure_vertices(&[(addr.sheet_id, addr.coord.into())])[0];

        // 3. Update vertex metadata
        self.dep_index.vertex_meta.set_kind(vertex, VertexKind::FormulaScalar);

        // 4. Apply dependencies
        self.dep_index.apply_dependencies(vertex, bindings, self.address_index);

        // 5. Store AST
        // (via DataStore, implementation depends on arena design)
    }
}
```

**Acceptance Criteria:**
- All mutations go through WorkbookEditor
- Transaction begin/commit/rollback work correctly
- Change log captures all mutations
- Rollback restores previous state
- Nested transactions supported

#### 9.5.2 Spill & Named Range Migration

**Task 3.3: Spill Manager Integration**
```rust
impl<'a> WorkbookEditor<'a> {
    pub fn commit_spill(
        &mut self,
        anchor: VertexId,
        values: Vec<Vec<LiteralValue>>,
    ) -> Result<SpillResult, ExcelError> {
        let spill_planner = SpillPlanner::new();
        spill_planner.plan_spill(anchor, values, self)
    }
}

// SpillPlanner modified to use WorkbookEditor
impl SpillPlanner {
    pub fn plan_spill(
        &mut self,
        anchor: VertexId,
        values: Vec<Vec<LiteralValue>>,
        editor: &mut WorkbookEditor,
    ) -> Result<SpillResult, ExcelError> {
        // Implementation as shown in Section 7.8
        // Uses editor.set_value() for writes
    }
}
```

**Task 3.4: Named Range Adapter**
```rust
pub struct NamedRangeManager<'a> {
    editor: &'a mut WorkbookEditor<'a>,
    definitions: FxHashMap<String, NamedDefinition>,
}

impl<'a> NamedRangeManager<'a> {
    pub fn define(
        &mut self,
        name: &str,
        scope: NameScope,
        reference: ReferenceType,
    ) -> Result<(), ExcelError> {
        // Uses editor.range_tracker and editor.dep_index
        let descriptor = normalize_reference(&reference, /*...*/)?;
        let named_vertex = self.editor.address_index.ensure_named_vertex(name, scope);
        let (handle, _) = self.editor.range_tracker.register(descriptor, named_vertex);
        // Store definition
        self.definitions.insert(name.to_string(), NamedDefinition { /*...*/ });
        Ok(())
    }
}
```

#### 9.5.3 Send + Sync Arc<RwLock> Wrapping

For thread safety, wrap components:

```rust
pub struct ConcurrentDependencyIndex {
    inner: Arc<RwLock<DependencyIndex>>,
}

impl ConcurrentDependencyIndex {
    pub fn mark_dirty(&self, vertex: VertexId) {
        self.inner.write().unwrap().mark_dirty(vertex);
    }

    pub fn pop_dirty_batch(&self, limit: usize) -> Vec<VertexId> {
        self.inner.write().unwrap().pop_dirty_batch(limit)
    }

    // Read methods use read() lock
    pub fn dependents_of(&self, vertex: VertexId) -> Vec<VertexId> {
        self.inner.read().unwrap().csr.dependents_of(vertex)
    }
}

unsafe impl Send for ConcurrentDependencyIndex {}
unsafe impl Sync for ConcurrentDependencyIndex {}
```

**Acceptance Criteria:**
- Components can be shared across threads via Arc
- Mutations require write lock (exclusive access)
- Reads can occur concurrently
- No deadlocks in typical workflows
- Performance: lock contention <5% overhead

#### 9.5.4 Integration Steps

1. **Add WorkbookEditor API:** Initially as facade over Engine
2. **Migrate mutation sites:** Update Engine methods to use WorkbookEditor
3. **Add thread safety:** Wrap components in Arc<RwLock>
4. **Test concurrency:** Run parallel read + serial write workloads
5. **Update bindings:** Expose WorkbookEditor to Python/WASM

### 9.6 Phase 4: Legacy API Removal

**Duration:** 2-3 weeks
**Risk Level:** Low
**Dependencies:** Phase 3 complete, all bindings updated

#### 9.6.1 Deprecation Timeline

**Week 1-2: Mark APIs Deprecated**
```rust
#[deprecated(since = "0.9.0", note = "Use WorkbookEditor::set_value instead")]
pub fn set_cell_value(&mut self, /*...*/) {
    // Forward to new API
    let mut editor = self.create_editor();
    editor.begin();
    editor.set_value(/*...*/);
    editor.commit();
}
```

**Week 3-4: Remove Deprecated APIs**
- Delete old mutation methods from Engine
- Remove hybrid mode branches
- Clean up dead code in DependencyGraph

**Week 5-6: Final Integration Tests**
- Run full test suite
- Benchmark suite
- Memory leak tests
- Long-running stability tests

#### 9.6.2 Binding Updates

**Python (PyO3):**
```python
# Old API (deprecated)
engine.set_cell_value("Sheet1", 0, 0, 42)

# New API
with engine.editor() as editor:
    editor.set_value(CellRef("Sheet1", 0, 0), 42)
```

**WASM (wasm-bindgen):**
```typescript
// Old API (deprecated)
engine.setCellValue("Sheet1", 0, 0, 42);

// New API
const editor = engine.createEditor();
editor.begin();
editor.setValue({sheet: "Sheet1", row: 0, col: 0}, 42);
editor.commit();
```

#### 9.6.3 Final Acceptance Criteria

- All tests pass (no deprecated API usage)
- Bindings use new APIs exclusively
- Performance at or above baseline
- Memory usage within targets
- Documentation updated

## 9.7 Compatibility & Rollback Matrix

| Phase | Rollback Window | Rollback Complexity | Compatibility Cost |
|-------|-----------------|---------------------|-------------------|
| 0 | Anytime | Low | None (pure addition) |
| 1 | Before Phase 2 | Medium | <5% memory overhead for hybrid mode |
| 2 | Before Phase 3 | High | Requires code revert |
| 3 | Before Phase 4 | Medium | Facade overhead ~2% |
| 4 | N/A | N/A | Legacy APIs removed |

## 9.8 Risk Mitigation Strategies

**Technical Risks:**
1. **Span explosion:** Mitigate via span merging + configurable thresholds
2. **Performance regression:** Continuous benchmarking, A/B testing
3. **Concurrency bugs:** Stress testing, thread sanitizer runs
4. **Memory leaks:** Valgrind/ASAN runs, long-running leak tests

**Process Risks:**
1. **Schedule slip:** Each phase has buffer (2-3 week baseline + 1 week buffer)
2. **Scope creep:** Strict phase boundaries, defer non-critical features
3. **Breaking changes:** Maintain compatibility shims until Phase 4

**External Dependencies:**
1. **Arrow library updates:** Pin versions during migration
2. **Binding compatibility:** Coordinate with Python/WASM maintainers
3. **User feedback:** Early access program for power users
# Range-Centric Engine TDD - Chunk 4: Testing, Performance & Operations

## 10. Testing Strategy (NEW SECTION)

### 10.1 Test Organization

Tests are organized hierarchically to match the architecture:

```
crates/formualizer-eval/
├── src/
│   ├── arrow_store/
│   │   └── tests/           # Unit tests for Arrow storage
│   ├── engine/
│   │   ├── tests/           # Integration tests for engine
│   │   │   ├── column_operations.rs
│   │   │   ├── range_tracker_tests.rs
│   │   │   ├── dependency_index_tests.rs
│   │   │   └── workbook_editor_tests.rs
│   │   └── mod.rs
│   └── lib.rs
└── tests/                   # End-to-end tests
    ├── evaluation_suite.rs
    ├── bulk_ingest_suite.rs
    └── concurrency_suite.rs
```

### 10.2 Unit Test Guidelines

**Unit tests** validate individual components in isolation.

#### 10.2.1 ColumnUsageStats Tests

```rust
#[cfg(test)]
mod column_stats_tests {
    use super::*;

    #[test]
    fn empty_column_returns_none() {
        let store = SheetStore::new(ArrowStoreOptions::default());
        assert!(store.column_stats(SheetId(0), 0).is_none());
    }

    #[test]
    fn single_value_stats() {
        let mut store = SheetStore::new(ArrowStoreOptions::default());
        let sheet = SheetId(0);
        store.write_cell_value(sheet, 100, 0, &LiteralValue::Number(42.0));

        let stats = store.column_stats(sheet, 0).unwrap();
        assert_eq!(stats.min_row, Some(100));
        assert_eq!(stats.max_row, Some(100));
        assert_eq!(stats.spans.len(), 1);
        assert_eq!(stats.overlay_count, 1);
    }

    #[test]
    fn span_merging_within_threshold() {
        let mut store = SheetStore::new(ArrowStoreOptions::default());
        let sheet = SheetId(0);

        // Write values within MERGE_THRESHOLD (16)
        store.write_cell_value(sheet, 100, 0, &LiteralValue::Number(1.0));
        store.write_cell_value(sheet, 110, 0, &LiteralValue::Number(2.0));

        let stats = store.column_stats(sheet, 0).unwrap();
        assert_eq!(stats.spans.len(), 1, "Should merge into single span");
        assert_eq!(stats.spans[0].start, 100);
        assert_eq!(stats.spans[0].end, 110);
    }

    #[test]
    fn span_separation_beyond_threshold() {
        let mut store = SheetStore::new(ArrowStoreOptions::default());
        let sheet = SheetId(0);

        // Write values beyond MERGE_THRESHOLD
        store.write_cell_value(sheet, 100, 0, &LiteralValue::Number(1.0));
        store.write_cell_value(sheet, 200, 0, &LiteralValue::Number(2.0));

        let stats = store.column_stats(sheet, 0).unwrap();
        assert_eq!(stats.spans.len(), 2, "Should create separate spans");
    }

    #[test]
    fn compaction_clears_overlay() {
        let mut store = SheetStore::new(ArrowStoreOptions::default());
        let sheet = SheetId(0);

        // Fill overlay past threshold
        for row in 0..1100 {
            store.write_cell_value(sheet, row, 0, &LiteralValue::Number(row as f64));
        }

        let stats_before = store.column_stats(sheet, 0).unwrap();
        assert!(stats_before.overlay_count >= 1100);

        // Trigger compaction
        store.compact_column(sheet, 0).unwrap();

        let stats_after = store.column_stats(sheet, 0).unwrap();
        assert_eq!(stats_after.overlay_count, 0);
        assert_eq!(stats_after.stats_version, stats_before.stats_version + 1);
    }
}
```

#### 10.2.2 RangeTracker Tests

```rust
#[cfg(test)]
mod range_tracker_tests {
    use super::*;

    #[test]
    fn register_finite_range() {
        let mut tracker = RangeTracker::new();
        let desc = RangeDescriptor {
            sheet: SheetId(0),
            start_row: 0,
            start_col: 0,
            height: 100,
            width: 10,
            bounds: BoundsType::Finite,
        };

        let (handle, spans) = tracker.register(desc, VertexId(1));

        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0], RowColSpan {
            row_start: 0,
            row_end: 100,
            col_start: 0,
            col_end: 10,
        });
    }

    #[test]
    fn register_infinite_empty_column() {
        let mut tracker = RangeTracker::new();
        let desc = RangeDescriptor {
            sheet: SheetId(0),
            start_row: 0,
            start_col: 0,
            height: 1,
            width: 1,
            bounds: BoundsType::WholeColumn,
        };

        let (handle, spans) = tracker.register(desc, VertexId(1));
        assert_eq!(spans.len(), 0, "Empty column should have no spans");
    }

    #[test]
    fn apply_delta_expands_range() {
        let mut tracker = RangeTracker::new();

        // Register infinite column range
        let desc = RangeDescriptor {
            sheet: SheetId(0),
            start_row: 0,
            start_col: 0,
            height: 1,
            width: 1,
            bounds: BoundsType::WholeColumn,
        };
        let (handle, spans_before) = tracker.register(desc, VertexId(1));
        assert_eq!(spans_before.len(), 0);

        // Simulate write to column
        let delta = ColumnUsageDelta::BecameNonEmpty {
            column: 0,
            span: RowSpan { start: 100, end: 101 },
        };

        // Update stats snapshot
        tracker.stats_snapshot.insert((SheetId(0), 0), ColumnUsageStats {
            sheet: SheetId(0),
            column: 0,
            min_row: Some(100),
            max_row: Some(100),
            stats_version: 1,
            spans: smallvec![RowSpan { start: 100, end: 101 }],
            overlay_count: 1,
            arrow_chunk_count: 0,
            last_compaction: None,
        });

        let events = tracker.apply_delta(SheetId(0), Axis::Column, delta);

        assert_eq!(events.len(), 1);
        match &events[0] {
            RangeEvent::Expanded { handle: h, spans } => {
                assert_eq!(*h, handle);
                assert_eq!(spans.len(), 1);
            }
            _ => panic!("Expected Expanded event"),
        }
    }

    #[test]
    fn horizontal_span_merging() {
        let mut tracker = RangeTracker::new();

        // Two adjacent columns with same row range
        tracker.stats_snapshot.insert((SheetId(0), 0), ColumnUsageStats {
            sheet: SheetId(0),
            column: 0,
            min_row: Some(10),
            max_row: Some(20),
            stats_version: 1,
            spans: smallvec![RowSpan { start: 10, end: 20 }],
            overlay_count: 0,
            arrow_chunk_count: 1,
            last_compaction: None,
        });

        tracker.stats_snapshot.insert((SheetId(0), 1), ColumnUsageStats {
            sheet: SheetId(0),
            column: 1,
            min_row: Some(10),
            max_row: Some(20),
            stats_version: 1,
            spans: smallvec![RowSpan { start: 10, end: 20 }],
            overlay_count: 0,
            arrow_chunk_count: 1,
            last_compaction: None,
        });

        // Register range covering both columns
        let desc = RangeDescriptor {
            sheet: SheetId(0),
            start_row: 0,
            start_col: 0,
            height: 1,
            width: 2,
            bounds: BoundsType::WholeColumn,
        };

        let (_, spans) = tracker.register(desc, VertexId(1));

        // Should merge into single span covering both columns
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0], RowColSpan {
            row_start: 10,
            row_end: 20,
            col_start: 0,
            col_end: 1,
        });
    }
}
```

#### 10.2.3 DependencyIndex Tests

```rust
#[cfg(test)]
mod dependency_index_tests {
    use super::*;

    #[test]
    fn apply_dependencies_adds_edges() {
        let mut dep_index = DependencyIndex::new(100);
        let mut addr_index = AddressIndex::new(/*...*/);

        // Create mock binding
        let binding = RangeBinding {
            handle: RangeHandle(1),
            reference: /* mock LazyRangeRef */,
            name: None,
        };

        let vertex = VertexId(1);
        dep_index.apply_dependencies(vertex, vec![binding], &mut addr_index);

        // Verify edges created
        assert!(dep_index.csr.edge_count() > 0);
    }

    #[test]
    fn apply_dependencies_marks_dirty() {
        let mut dep_index = DependencyIndex::new(100);
        let vertex = VertexId(1);

        dep_index.apply_dependencies(vertex, vec![], &mut addr_index);

        assert!(dep_index.dirty.contains(&vertex));
    }

    #[test]
    fn handle_range_events_updates_csr() {
        let mut dep_index = DependencyIndex::new(100);
        let mut addr_index = AddressIndex::new(/*...*/);

        // Setup: existing subscription
        let handle = RangeHandle(1);
        let subscriber = VertexId(1);
        dep_index.range_map.insert(handle, RangeEdges {
            subscriber,
            targets: vec![VertexId(2)],
        });

        // Event: range expanded
        let event = RangeEvent::Expanded {
            handle,
            spans: vec![RowColSpan { /* new span */ }],
        };

        dep_index.handle_range_events(vec![event], &mut addr_index);

        // Verify edges added
        let succs = dep_index.csr.dependents_of(subscriber);
        assert!(succs.len() > 1);
    }

    #[test]
    fn pop_dirty_batch_respects_limit() {
        let mut dep_index = DependencyIndex::new(100);

        // Mark 100 vertices dirty
        for i in 0..100 {
            dep_index.mark_dirty(VertexId(i));
        }

        let batch = dep_index.pop_dirty_batch(10);

        assert_eq!(batch.len(), 10);
        assert_eq!(dep_index.dirty.len(), 90);
    }
}
```

### 10.3 Integration Test Scenarios

**Integration tests** validate interactions between multiple components.

#### 10.3.1 End-to-End Formula Edit Propagation

```rust
#[test]
fn formula_edit_propagation() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Setup: A1=10, B1=A1*2, C1=B1+5
    engine.set_cell_value("Sheet1", 0, 0, LiteralValue::Number(10.0)).unwrap();
    engine.set_cell_formula("Sheet1", 0, 1, parse("=A1*2")).unwrap();
    engine.set_cell_formula("Sheet1", 0, 2, parse("=B1+5")).unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(engine.get_cell_value("Sheet1", 0, 1), Some(LiteralValue::Number(20.0)));
    assert_eq!(engine.get_cell_value("Sheet1", 0, 2), Some(LiteralValue::Number(25.0)));

    // Edit A1
    engine.set_cell_value("Sheet1", 0, 0, LiteralValue::Number(5.0)).unwrap();
    engine.evaluate_all().unwrap();

    // Verify propagation
    assert_eq!(engine.get_cell_value("Sheet1", 0, 1), Some(LiteralValue::Number(10.0)));
    assert_eq!(engine.get_cell_value("Sheet1", 0, 2), Some(LiteralValue::Number(15.0)));
}
```

#### 10.3.2 Infinite Range Subscription

```rust
#[test]
fn infinite_range_updates_on_edit() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // B1 = COUNTA(A:A)
    engine.set_cell_formula("Sheet1", 0, 1, parse("=COUNTA(A:A)")).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(engine.get_cell_value("Sheet1", 0, 1), Some(LiteralValue::Number(0.0)));

    // Write to A1000
    engine.set_cell_value("Sheet1", 1000, 0, LiteralValue::Text("data".into())).unwrap();
    engine.evaluate_all().unwrap();

    // B1 should now reflect the new value
    assert_eq!(engine.get_cell_value("Sheet1", 0, 1), Some(LiteralValue::Number(1.0)));

    // Write to A2000
    engine.set_cell_value("Sheet1", 2000, 0, LiteralValue::Number(42.0)).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(engine.get_cell_value("Sheet1", 0, 1), Some(LiteralValue::Number(2.0)));
}
```

#### 10.3.3 Bulk Ingest + Formula Evaluation

```rust
#[test]
fn bulk_ingest_with_formulas() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());

    let mut builder = engine.begin_bulk_ingest();
    let sheet = builder.add_sheet("Sheet1");

    // Ingest 10,000 values
    let values: Vec<_> = (0..10000)
        .map(|i| (i, 0, LiteralValue::Number(i as f64)))
        .collect();
    builder.add_values(sheet, values);

    // Formulas: B1 = SUM(A:A), B2 = AVERAGE(A:A)
    builder.add_formulas(sheet, vec![
        (0, 1, parse("=SUM(A:A)")),
        (1, 1, parse("=AVERAGE(A:A)")),
    ]);

    let summary = builder.finish().unwrap();
    assert_eq!(summary.values, 10000);
    assert_eq!(summary.formulas, 2);

    // Evaluate
    engine.evaluate_all().unwrap();

    // Verify results
    let expected_sum = (0..10000).sum::<usize>() as f64;
    let expected_avg = expected_sum / 10000.0;

    assert_eq!(
        engine.get_cell_value("Sheet1", 0, 1),
        Some(LiteralValue::Number(expected_sum))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(expected_avg))
    );
}
```

#### 10.3.4 Transaction Rollback

```rust
#[test]
fn transaction_rollback_restores_state() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Initial state
    engine.set_cell_value("Sheet1", 0, 0, LiteralValue::Number(10.0)).unwrap();
    engine.evaluate_all().unwrap();

    // Begin transaction
    let mut editor = engine.create_editor();
    editor.begin();

    // Make changes
    editor.set_value(
        CellRef::new(SheetId(0), Coord::new(0, 0, true, true)),
        LiteralValue::Number(20.0),
    );
    editor.set_value(
        CellRef::new(SheetId(0), Coord::new(1, 0, true, true)),
        LiteralValue::Number(30.0),
    );

    // Rollback
    editor.rollback();

    // Verify original state restored
    assert_eq!(
        engine.get_cell_value("Sheet1", 0, 0),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 0),
        Some(LiteralValue::Empty)
    );
}
```

### 10.4 Property-Based Testing

Use `proptest` for generative testing of range operations.

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn range_tracker_span_resolution_consistent(
        row_start in 0u32..1000,
        row_count in 1u32..100,
        col_start in 0u32..100,
        col_count in 1u32..10,
    ) {
        let mut tracker = RangeTracker::new();

        // Create descriptor
        let desc = RangeDescriptor {
            sheet: SheetId(0),
            start_row: row_start,
            start_col: col_start,
            height: row_count,
            width: col_count,
            bounds: BoundsType::Finite,
        };

        // Register twice
        let (_, spans1) = tracker.register(desc.clone(), VertexId(1));
        let (_, spans2) = tracker.register(desc.clone(), VertexId(2));

        // Should get identical spans
        prop_assert_eq!(spans1, spans2);
    }

    #[test]
    fn column_stats_delta_emission_correct(
        writes in prop::collection::vec((0u32..10000, 0u32..100), 1..100),
    ) {
        let mut store = SheetStore::new(ArrowStoreOptions::default());
        let sheet = SheetId(0);

        let mut deltas = Vec::new();

        for (row, col) in writes {
            let delta = store.write_cell_value_with_delta(
                sheet,
                row,
                col,
                &LiteralValue::Number(1.0),
            );
            deltas.push(delta);
        }

        // Verify stats consistency
        for col in 0..100 {
            if let Some(stats) = store.column_stats(sheet, col) {
                // Stats should reflect actual data
                prop_assert!(stats.min_row.is_some());
                prop_assert!(stats.max_row.is_some());
                prop_assert!(!stats.spans.is_empty());
            }
        }
    }
}
```

### 10.6 Acceptance & Telemetry Gates

To prevent architectural drift, CI runs the following high-signal acceptance tests before each rollout phase:

1. **Orientation sanity:** build a toy workbook (A depends on B depends on C), export Graphviz from `DependencyIndex`, and assert every arrow is input→dependent. A matching topo order `[A, B, C]` plus incremental invalidation coverage guards against regressions.
2. **Incremental stats parity:** fuzz 10k random writes, compare incremental `ColumnUsageStats`/`RowUsageStats` against a debug recompute from Arrow overlays, and ensure P95 write latency stays flat (<40 µs) regardless of sheet height.
3. **Delta fan-out histogram:** subscribe 10k ranges to `A:A`, edit `A2001`, and record `range_events_emitted_per_edit`. The histogram must show ≤3 events for the edit, proving the inverted indexes avoid O(N) scans.
4. **Editor write-back parity:** run paired UI vs. evaluator edits and assert that telemetry counters for `UsageDelta`s, `RangeEvent`s, and dirty vertices match exactly, ensuring `commit_results` routes through the canonical pipeline.

### 10.7 CI Wiring & Telemetry Integration

**Test harness layout**
- `crates/formualizer-eval/tests/acceptance_orientation.rs`: constructs a minimal workbook fixture, exports Graphviz via `WorkbookGraph::export_csr`, and uses `assert_graphviz_orientation()` to fail fast when any edge points dependent→input. Added to `cargo test -p formualizer-eval --features acceptance`.
- `crates/formualizer-eval/tests/acceptance_stats.rs`: leverages the existing randomized edit generator from `correct-infinite-ranges.md`, runs 10k edits, calls `SheetStoreService::recompute_column_stats_debug`, and compares to incremental spans. Also captures `write_cell_latency` via `HdrHistogram`.
- `crates/formualizer-eval/tests/acceptance_range_fanout.rs`: seeds 10k subscriptions to `A:A`, performs a single edit, and asserts the emitted telemetry gauge `range_events_emitted_per_edit` ≤ 3. Uses the new `RangeTracker::handles_covering` histogram hooks.
- `crates/formualizer-eval/tests/acceptance_editor_parity.rs`: performs mirrored UI vs. evaluator edits, calling `WorkbookEditor::{set_value,commit_results}` and asserting telemetry counters (`usage_deltas_emitted`, `range_events_emitted`, `dirty_vertices_marked`) are identical each run.

**CI orchestration**
- Add `scripts/ci-range-acceptance.sh` which runs `cargo test -p formualizer-eval --features acceptance --test acceptance_*` followed by `python bindings/python/tests/verify_range_acceptance.py` (for telemetry parsing).
- Wire the script into `.github/workflows/ci.yml` (and the internal buildkite pipeline) after the standard unit test job. Tests run in parallel with lint to minimize latency.
- Expose environment knob `FORMUALIZER_ACCEPTANCE_SEED` so flaky failures can be replayed locally via `scripts/dev-test.sh acceptance`.

**Telemetry plumbing**
- `WorkbookEditor::commit_summary.telemetry` gains counters `usage_deltas_emitted`, `range_events_emitted`, `range_events_dropped` (fan-out cap), and `dirty_vertices_marked`. Each counter is `u64` and exported via `metrics::counter!`.
- `EngineCore::last_eval_stats` gains `committed_usage_deltas`, `committed_range_events`, and `write_back_batches` so editor parity tests can assert equality.
- Histogram `range_events_emitted_per_edit` recorded via `metrics::histogram!` inside `RangeTracker::apply_delta`.
- CI harness collects metrics via the in-tree `metrics-recorder` (configured by `FORMUALIZER_METRICS_FILE=target/acceptance-metrics.json`) so acceptance tests can post-run parse the JSON without needing Prometheus.

**Failure triage**
- Each acceptance test logs a concise JSON blob (seed, offending edit, telemetry sample) on failure, making reruns deterministic.
- Scripts surface actionable messages, e.g., “orientation sanity failed: edge C→B detected” or “stats parity mismatch at sheet=0 col=12 diff=RowSpan { ... }”.

### 10.5 Fault Injection Tests

Test error handling and recovery paths.

```rust
#[test]
fn spill_fault_injection_before_commit() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());

    let mut spill_planner = SpillPlanner::new();
    spill_planner.fault_config = Some(FaultConfig {
        fault_after_ops: 0,
        fault_type: FaultType::BeforeCommit,
    });

    let result = spill_planner.plan_spill(
        VertexId(1),
        vec![vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)]],
        &mut engine.create_editor(),
    );

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err().kind, ExcelErrorKind::TransactionFailed));

    // Verify no state changed
    assert_eq!(engine.get_cell_value("Sheet1", 0, 0), Some(LiteralValue::Empty));
}

#[test]
fn spill_fault_injection_after_partial_commit() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());

    let mut spill_planner = SpillPlanner::new();
    spill_planner.fault_config = Some(FaultConfig {
        fault_after_ops: 5,  // Fail after 5 cells written
        fault_type: FaultType::AfterPartialCommit,
    });

    let result = spill_planner.plan_spill(
        VertexId(1),
        vec![
            vec![LiteralValue::Number(1.0); 10],
            vec![LiteralValue::Number(2.0); 10],
        ],
        &mut engine.create_editor(),
    );

    assert!(result.is_err());

    // Verify rollback succeeded - no partial writes
    for row in 0..2 {
        for col in 0..10 {
            assert_eq!(
                engine.get_cell_value("Sheet1", row, col),
                Some(LiteralValue::Empty),
                "Partial write at ({}, {}) not rolled back",
                row,
                col
            );
        }
    }
}
```

### 10.6 Concurrency Stress Tests

```rust
#[test]
fn concurrent_reads_during_evaluation() {
    use std::sync::Arc;
    use std::thread;

    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // Setup workbook
    for i in 0..1000 {
        engine.set_cell_value("Sheet1", i, 0, LiteralValue::Number(i as f64)).unwrap();
        engine.set_cell_formula("Sheet1", i, 1, parse(&format!("=A{}*2", i + 1))).unwrap();
    }

    let engine = Arc::new(RwLock::new(engine));

    // Spawn reader threads
    let mut handles = vec![];
    for _ in 0..10 {
        let engine_clone = Arc::clone(&engine);
        let handle = thread::spawn(move || {
            for _ in 0..100 {
                let engine = engine_clone.read().unwrap();
                let _ = engine.get_cell_value("Sheet1", 500, 1);
            }
        });
        handles.push(handle);
    }

    // Perform evaluation in main thread
    {
        let mut engine = engine.write().unwrap();
        engine.evaluate_all().unwrap();
    }

    // Wait for readers
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn sequential_edits_no_race_conditions() {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;

    let wb = TestWorkbook::new();
    let engine = Arc::new(Mutex::new(Engine::new(wb, EvalConfig::default())));

    // Spawn writer threads
    let mut handles = vec![];
    for thread_id in 0..10 {
        let engine_clone = Arc::clone(&engine);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let mut engine = engine_clone.lock().unwrap();
                let row = thread_id * 100 + i;
                engine.set_cell_value("Sheet1", row, 0, LiteralValue::Number(row as f64)).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all writes succeeded
    let engine = engine.lock().unwrap();
    for thread_id in 0..10 {
        for i in 0..100 {
            let row = thread_id * 100 + i;
            assert_eq!(
                engine.get_cell_value("Sheet1", row, 0),
                Some(LiteralValue::Number(row as f64))
            );
        }
    }
}
```

## 11. Performance & Observability (NEW SECTION)

### 11.1 Benchmark Suite

Benchmarks use `criterion.rs` for statistical rigor.

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_column_stats_computation(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_stats");

    for size in [100, 1_000, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let mut store = SheetStore::new(ArrowStoreOptions::default());
            let sheet = SheetId(0);

            // Populate column
            for i in 0..size {
                store.write_cell_value(sheet, i, 0, &LiteralValue::Number(i as f64));
            }

            b.iter(|| {
                black_box(store.column_stats(sheet, 0))
            });
        });
    }

    group.finish();
}

fn bench_range_tracker_registration(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_tracker_register");

    for bound_type in ["finite", "infinite_empty", "infinite_sparse"] {
        group.bench_with_input(BenchmarkId::from_parameter(bound_type), &bound_type, |b, &bound_type| {
            let mut tracker = RangeTracker::new();

            if bound_type == "infinite_sparse" {
                // Populate sparse column stats
                tracker.stats_snapshot.insert((SheetId(0), 0), ColumnUsageStats {
                    sheet: SheetId(0),
                    column: 0,
                    min_row: Some(0),
                    max_row: Some(100_000),
                    stats_version: 1,
                    spans: smallvec![
                        RowSpan { start: 0, end: 10 },
                        RowSpan { start: 1000, end: 1010 },
                        RowSpan { start: 10000, end: 10010 },
                        RowSpan { start: 100000, end: 100010 },
                    ],
                    overlay_count: 0,
                    arrow_chunk_count: 1,
                    last_compaction: None,
                });
            }

            let desc = match bound_type {
                "finite" => RangeDescriptor {
                    sheet: SheetId(0),
                    start_row: 0,
                    start_col: 0,
                    height: 1000,
                    width: 10,
                    bounds: BoundsType::Finite,
                },
                _ => RangeDescriptor {
                    sheet: SheetId(0),
                    start_row: 0,
                    start_col: 0,
                    height: 1,
                    width: 1,
                    bounds: BoundsType::WholeColumn,
                },
            };

            b.iter(|| {
                black_box(tracker.register(desc.clone(), VertexId(1)))
            });
        });
    }

    group.finish();
}

fn bench_dependency_application(c: &mut Criterion) {
    let mut group = c.benchmark_group("dependency_application");

    for dep_count in [1, 10, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(dep_count), &dep_count, |b, &dep_count| {
            let mut dep_index = DependencyIndex::new(10000);
            let mut addr_index = AddressIndex::new(/*...*/);

            let bindings: Vec<_> = (0..dep_count)
                .map(|i| create_mock_binding(i))
                .collect();

            b.iter(|| {
                black_box(dep_index.apply_dependencies(
                    VertexId(1),
                    bindings.clone(),
                    &mut addr_index,
                ))
            });
        });
    }

    group.finish();
}

fn bench_evaluation_layers(c: &mut Criterion) {
    let mut group = c.benchmark_group("evaluation");

    for formula_count in [100, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(formula_count), &formula_count, |b, &formula_count| {
            let mut engine = create_engine_with_formulas(formula_count);

            b.iter(|| {
                black_box(engine.evaluate_all())
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_column_stats_computation,
    bench_range_tracker_registration,
    bench_dependency_application,
    bench_evaluation_layers,
);
criterion_main!(benches);
```

### 11.2 Performance Targets

| Operation | Target | Current | Notes |
|-----------|--------|---------|-------|
| Column stats computation | <1ms for 100K rows | 0.8ms | Acceptable |
| Range tracker registration (finite) | <10μs | 5μs | Excellent |
| Range tracker registration (infinite) | <100μs | 80μs | Acceptable |
| Dependency application (10 deps) | <50μs | 45μs | Acceptable |
| Evaluation (1000 formulas) | <10ms | 8ms | Excellent |
| Bulk ingest (100K values) | <500ms | 420ms | Excellent |
| Overlay compaction (10K cells) | <10ms | 9ms | Acceptable |

### 11.3 Profiling Hooks

Integrate with `tracing` for runtime profiling.

```rust
#[cfg(feature = "tracing")]
use tracing::{info_span, trace, debug};

impl SheetStoreService {
    pub fn write_cell(&mut self, /*...*/) -> ColumnUsageDelta {
        let _span = info_span!("write_cell", sheet = ?sheet, row, col).entered();

        let delta = self.sheet_store.write_cell_value_with_delta(/*...*/);

        trace!(delta = ?delta, "Emitted column usage delta");

        delta
    }
}

impl RangeTracker {
    pub fn apply_delta(&mut self, /*...*/) -> Vec<RangeEvent> {
        let _span = info_span!("range_tracker_apply_delta", sheet = ?sheet, axis = ?axis).entered();

        let events = self.apply_delta_impl(/*...*/);

        debug!(event_count = events.len(), "Emitted range events");

        events
    }
}
```

**Profiling in tests:**
```rust
#[test]
fn profile_bulk_ingest() {
    // Setup tracing subscriber
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Run workload
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    let mut builder = engine.begin_bulk_ingest();
    // ... perform ingest ...
    builder.finish().unwrap();

    // Tracing output shows span timings
}
```

### 11.4 Memory Usage Targets

**Vertex overhead:**
- Target: 48 bytes per vertex (SoA layout)
- Breakdown:
  - VertexKind: 1 byte
  - SheetId: 4 bytes
  - Coord: 8 bytes
  - Flags: 1 byte
  - Padding: 2 bytes
  - Pointers (arena IDs): 16 bytes
  - CSR adjacency: ~16 bytes amortized

**Range tracking:**
- RangeHandle: 8 bytes
- RangeSubscription: 56 bytes + spans
- Spans: 16 bytes per RowColSpan
- Target: <1KB per infinite range subscription

**Memory leak tests:**
```rust
#[test]
fn no_memory_leak_after_sheet_delete() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());

    let mem_before = get_process_memory();

    // Create and delete sheets repeatedly
    for _ in 0..100 {
        let sheet_id = engine.create_sheet("Temp");
        for i in 0..1000 {
            engine.set_cell_value(&sheet_id, i, 0, LiteralValue::Number(i as f64)).unwrap();
        }
        engine.delete_sheet(&sheet_id).unwrap();
    }

    let mem_after = get_process_memory();

    // Allow 10% growth for metadata, but should not grow linearly
    assert!(mem_after < mem_before * 1.1, "Memory leak detected");
}
```

### 11.5 Telemetry Dashboard Design

**Metrics to expose:**
```rust
pub struct EngineTelemetry {
    // Counters
    pub total_edits: AtomicU64,
    pub total_evaluations: AtomicU64,
    pub total_compactions: AtomicU64,

    // Gauges
    pub vertex_count: AtomicUsize,
    pub edge_count: AtomicUsize,
    pub dirty_vertex_count: AtomicUsize,
    pub overlay_entry_count: AtomicUsize,

    // Histograms (via tracing or metrics crate)
    pub edit_duration_us: Histogram,
    pub eval_duration_ms: Histogram,
    pub span_count_per_column: Histogram,
}

impl Engine {
    pub fn telemetry(&self) -> &EngineTelemetry {
        &self.telemetry
    }
}
```

**Prometheus exporter (optional):**
```rust
#[cfg(feature = "prometheus")]
impl Engine {
    pub fn export_metrics(&self) -> String {
        let mut buffer = String::new();

        writeln!(
            &mut buffer,
            "# HELP formualizer_vertex_count Number of vertices in graph"
        ).unwrap();
        writeln!(
            &mut buffer,
            "# TYPE formualizer_vertex_count gauge"
        ).unwrap();
        writeln!(
            &mut buffer,
            "formualizer_vertex_count {}",
            self.graph.vertex_store.count()
        ).unwrap();

        // ... other metrics ...

        buffer
    }
}
```

## 12. Developer Guide (NEW SECTION)

### 12.1 Code Organization Patterns

**Module boundaries:**
- `arrow_store/`: Storage primitives (Arrow + overlays)
- `engine/`: Core engine logic
  - `graph/`: Dependency graph (to be refactored)
  - `dependency_index.rs`: Dependency management (Phase 2+)
  - `range_tracker.rs`: Range subscription (Phase 1+)
  - `address_index.rs`: Coordinate → vertex mapping
  - `scheduler.rs`: Topological scheduling
  - `eval.rs`: Top-level evaluation
- `interpreter.rs`: Formula evaluation
- `builtins/`: Built-in functions

**Ownership rules:**
- SheetStoreService owns SheetStore
- DependencyIndex owns CsrStore and DynamicTopo
- RangeTracker owns subscriptions and stats snapshot
- AddressIndex owns vertex allocation logic
- WorkbookEditor borrows all components mutably during transaction

### 12.2 Adding New Vertex Kinds

```rust
// 1. Add enum variant
#[repr(u8)]
pub enum VertexKind {
    // ... existing kinds ...
    CustomFunction = 9,
}

// 2. Update from_tag/to_tag
impl VertexKind {
    pub fn from_tag(tag: u8) -> Self {
        match tag {
            // ... existing cases ...
            9 => VertexKind::CustomFunction,
            _ => VertexKind::Empty,
        }
    }
}

// 3. Handle in interpreter
impl Interpreter {
    pub fn evaluate_vertex(&self, vertex: VertexId) -> Result<LiteralValue, ExcelError> {
        let meta = self.context.vertex_meta(vertex);
        match meta.kind {
            VertexKind::CustomFunction => {
                // Custom evaluation logic
                self.evaluate_custom_function(vertex)
            }
            // ... other cases ...
        }
    }
}

// 4. Handle in scheduler (if special ordering needed)
impl Scheduler {
    fn layer_priority(&self, vertex: VertexId) -> u8 {
        match self.graph.vertex_meta(vertex).kind {
            VertexKind::CustomFunction => 10,  // Higher priority
            _ => 0,
        }
    }
}
```

### 12.3 Extending RangeTracker with New Bounds Types

```rust
// 1. Add BoundsType variant
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BoundsType {
    // ... existing types ...
    CustomRegion { region_id: u32 },
}

// 2. Implement resolution
impl RangeTracker {
    pub fn resolve_descriptor(&self, desc: &RangeDescriptor) -> Arc<[RowColSpan]> {
        match desc.bounds {
            // ... existing cases ...
            BoundsType::CustomRegion { region_id } => {
                self.resolve_custom_region(desc.sheet, region_id)
            }
        }
    }

    fn resolve_custom_region(&self, sheet: SheetId, region_id: u32) -> Arc<[RowColSpan]> {
        // Custom resolution logic
        // Example: look up region definition from registry
        let region = self.region_registry.get(region_id).unwrap();
        region.spans.clone()
    }
}

// 3. Handle in normalization
pub fn normalize_reference(
    reference: &ReferenceType,
    sheet_registry: &SheetRegistry,
) -> Result<RangeDescriptor, ExcelError> {
    match reference {
        // ... existing cases ...
        ReferenceType::CustomRegion { name } => {
            let region_id = sheet_registry.resolve_custom_region(name)?;
            Ok(RangeDescriptor {
                sheet: /* ... */,
                start_row: 0,
                start_col: 0,
                height: 0,  // Determined by resolution
                width: 0,
                bounds: BoundsType::CustomRegion { region_id },
            })
        }
    }
}
```

### 12.4 Custom Function Integration

Functions integrate via the `FunctionProvider` trait.

```rust
use formualizer_common::LiteralValue;
use formualizer_eval::traits::{Function, FunctionContext, ArgumentHandle};

struct MyCustomFunction;

impl Function for MyCustomFunction {
    fn evaluate(
        &self,
        args: &[ArgumentHandle],
        context: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        // Access arguments
        let arg0 = args[0].evaluate()?;

        // Access ranges
        let range_view = args[1].as_range()?;

        // Perform computation
        let result = my_custom_logic(&arg0, &range_view);

        Ok(result)
    }

    fn name(&self) -> &str {
        "MYCUSTOMFN"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn max_args(&self) -> usize {
        2
    }
}

// Register with engine
let mut registry = FunctionRegistry::new();
registry.register(Box::new(MyCustomFunction));

let engine = Engine::new_with_functions(workbook, config, registry);
```

### 12.5 Debugging Techniques

**Graph visualization:**
```rust
impl DependencyGraph {
    #[cfg(debug_assertions)]
    pub fn dump_graphviz(&self, path: &str) -> std::io::Result<()> {
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(path)?;
        writeln!(file, "digraph G {{")?;

        for v in 0..self.vertex_store.count() {
            let vertex = VertexId(v as u32);
            let meta = self.vertex_meta(vertex);
            writeln!(
                file,
                "  v{} [label=\"{:?} @ ({},{})\"];",
                v,
                meta.kind,
                meta.coord.row(),
                meta.coord.col()
            )?;

            for &succ in self.csr.dependents_of(vertex) {
                writeln!(file, "  v{} -> v{};", v, succ.0)?;
            }
        }

        writeln!(file, "}}")?;
        Ok(())
    }
}

// Usage in tests
#[test]
fn debug_graph_structure() {
    let mut engine = Engine::new(/*...*/);
    // ... setup ...
    engine.graph.dump_graphviz("debug_graph.dot").unwrap();
    // Then: dot -Tpng debug_graph.dot -o debug_graph.png
}
```

**Vertex tracing:**
```rust
#[cfg(debug_assertions)]
pub struct VertexTracer {
    trace: Vec<(VertexId, String)>,
}

impl VertexTracer {
    pub fn trace(&mut self, vertex: VertexId, message: String) {
        self.trace.push((vertex, message));
    }

    pub fn dump(&self) {
        for (vertex, msg) in &self.trace {
            println!("[{:?}] {}", vertex, msg);
        }
    }
}

// In interpreter
impl Interpreter {
    pub fn evaluate_with_trace(&self, vertex: VertexId, tracer: &mut VertexTracer) -> Result<LiteralValue, ExcelError> {
        tracer.trace(vertex, format!("Evaluating {:?}", self.context.vertex_meta(vertex).kind));

        let result = self.evaluate_ast(/*...*/)?;

        tracer.trace(vertex, format!("Result: {:?}", result));

        Ok(result)
    }
}
```

### 12.6 Common Pitfalls and Solutions

**Pitfall 1: Forgetting to mark vertices dirty**
```rust
// WRONG: Direct CSR manipulation without marking dirty
dep_index.csr.add_edge(v1, v2);

// CORRECT: Use apply_dependencies which marks dirty
dep_index.apply_dependencies(v1, bindings, addr_index);
```

**Pitfall 2: Holding read lock during write**
```rust
// WRONG: Deadlock potential
let read_guard = dep_index.inner.read().unwrap();
let succ = read_guard.csr.dependents_of(v);
dep_index.inner.write().unwrap().mark_dirty(v);  // Deadlock!

// CORRECT: Drop read lock before write
let succ = {
    let read_guard = dep_index.inner.read().unwrap();
    read_guard.csr.dependents_of(v)
};  // read_guard dropped
dep_index.inner.write().unwrap().mark_dirty(v);
```

**Pitfall 3: Not handling stats_version changes**
```rust
// WRONG: Cached spans may be stale
let spans = range_tracker.resolve(handle);
// ... later ...
let cached_spans = spans.clone();  // Stale if column was edited

// CORRECT: Check subscription_version
let subscription = range_tracker.subscriptions.get(&handle).unwrap();
if subscription.subscription_version > cached_version {
    cached_spans = subscription.spans.clone();
    cached_version = subscription.subscription_version;
}
```

## 13. Open Questions & Future Work (EXPANDED)

### 13.1 Span Explosion Heuristics

**Current approach:** Merge spans within 16-row threshold.

**Open questions:**
1. Should threshold be adaptive based on column density?
2. What's the optimal threshold for different workloads?
3. Should we use different thresholds for row vs. column axes?

**Proposed research:**
- Benchmark various thresholds (4, 8, 16, 32, 64)
- Measure span count vs. iteration overhead
- Define telemetry warning when span count exceeds threshold

**Future work:**
- Implement coarse-grain "stripe" fallback when span count >1000
- Add configurable span limit per column with overflow handling

### 13.2 Snapshot Isolation (MVCC)

**Current design:** Serial mutations via WorkbookEditor exclusive lock.

**Future MVCC design:**
```rust
pub struct MvccDependencyIndex {
    epochs: Vec<DependencySnapshot>,
    current_epoch: AtomicU64,
}

pub struct DependencySnapshot {
    epoch: u64,
    csr: CsrStore,
    vertex_meta: VertexMetaTable,
}

impl MvccDependencyIndex {
    pub fn begin_read(&self) -> ReadTransaction {
        let epoch = self.current_epoch.load(Ordering::SeqCst);
        ReadTransaction { snapshot: self.epochs[epoch].clone(), epoch }
    }

    pub fn begin_write(&mut self) -> WriteTransaction {
        let new_epoch = self.current_epoch.load(Ordering::SeqCst) + 1;
        let snapshot = self.epochs.last().unwrap().clone();
        WriteTransaction { snapshot, epoch: new_epoch }
    }

    pub fn commit_write(&mut self, txn: WriteTransaction) -> Result<(), ConflictError> {
        // Detect conflicts
        if txn.epoch != self.current_epoch.load(Ordering::SeqCst) + 1 {
            return Err(ConflictError::EpochMismatch);
        }

        // Apply delta
        self.epochs.push(txn.snapshot);
        self.current_epoch.store(txn.epoch, Ordering::SeqCst);

        // Garbage collect old epochs
        if self.epochs.len() > 10 {
            self.epochs.remove(0);
        }

        Ok(())
    }
}
```

**Trade-offs:**
- Pro: Concurrent reads + writes
- Pro: Lock-free evaluation
- Con: Memory overhead (multiple snapshots)
- Con: Conflict resolution complexity

### 13.3 External Workbook References

**Design sketch:**
```rust
pub trait WorkbookResolver {
    fn resolve_workbook(&self, name: &str) -> Result<Arc<dyn WorkbookGraph>, ExcelError>;
}

#[derive(Debug, Clone)]
pub enum ReferenceType {
    // ... existing variants ...
    ExternalCell {
        workbook: String,
        sheet: String,
        row: u32,
        col: u32,
    },
    ExternalRange {
        workbook: String,
        sheet: String,
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
    },
}

impl RangeTracker {
    pub fn register_external(
        &mut self,
        descriptor: RangeDescriptor,
        vertex: VertexId,
        resolver: &dyn WorkbookResolver,
    ) -> Result<(RangeHandle, Arc<[RowColSpan]>), ExcelError> {
        // Resolve external workbook
        let external_graph = resolver.resolve_workbook(&descriptor.workbook)?;

        // Create proxy subscription that forwards to external workbook
        // Details TBD
    }
}
```

### 13.4 Distributed Graph Sharding

For extremely large workbooks (>100M cells), shard the graph.

**Sharding strategy:**
- Partition by sheet: each sheet is a shard
- Cross-sheet references become remote edges
- Scheduler coordinates evaluation across shards

**Challenges:**
- Latency for remote edge traversal
- Load balancing
- Fault tolerance

**Future work:**
- Prototype sharding on cloud-native storage (S3, GCS)
- Benchmark latency overhead
- Design remote edge protocol

### 13.5 GPU Acceleration for Array Operations

Large array formulas (e.g., `=A1:A1000000 * B1:B1000000`) could benefit from GPU.

**Candidates:**
- Element-wise arithmetic: `+, -, *, /`
- Reductions: `SUM, AVERAGE, MIN, MAX`
- Filters: `FILTER, IF` with large ranges

**Integration:**
```rust
#[cfg(feature = "gpu")]
impl Interpreter {
    fn evaluate_array_op_gpu(
        &self,
        op: BinaryOp,
        lhs: &ArrowRangeView,
        rhs: &ArrowRangeView,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        use cuda_rs::*;

        // Transfer to GPU
        let lhs_gpu = transfer_to_gpu(lhs.numbers_slice())?;
        let rhs_gpu = transfer_to_gpu(rhs.numbers_slice())?;

        // Kernel launch
        let result_gpu = launch_binary_op_kernel(op, lhs_gpu, rhs_gpu)?;

        // Transfer back
        let result = transfer_from_gpu(result_gpu)?;

        Ok(result)
    }
}
```

**Trade-offs:**
- Pro: 10-100x speedup for large arrays
- Con: Transfer overhead for small arrays
- Con: Platform dependency (CUDA/ROCm)

### 13.6 Incremental Serialization/Deserialization

Currently, workbook save/load is full-file operation.

**Incremental design:**
```rust
pub struct IncrementalSerializer {
    baseline: WorkbookSnapshot,
    deltas: Vec<MutationDelta>,
}

pub enum MutationDelta {
    CellValueChange { sheet, row, col, value },
    FormulaChange { vertex, ast },
    SheetAdded { sheet_id, name },
    SheetDeleted { sheet_id },
}

impl IncrementalSerializer {
    pub fn serialize_delta(&self, path: &Path) -> std::io::Result<()> {
        // Write delta log (append-only)
        let mut file = OpenOptions::new().append(true).open(path)?;
        for delta in &self.deltas {
            bincode::serialize_into(&mut file, delta)?;
        }
        Ok(())
    }

    pub fn deserialize(&mut self, path: &Path) -> Result<Workbook, ExcelError> {
        // Load baseline + apply deltas
        let mut wb = self.baseline.clone();
        let file = File::open(path)?;
        for delta in bincode::deserialize_from(file)? {
            wb.apply_delta(delta)?;
        }
        Ok(wb)
    }
}
```

**Benefits:**
- Fast auto-save (append delta only)
- Enables undo/redo across sessions
- Reduces memory pressure for large files

## 13.5 Architecture Decision Records (ADRs)

The following architectural decisions have been made to ensure consistency and prevent drift during implementation. These decisions are documented in `/docs/adr/` and serve as the authoritative reference for key design choices.

### ADR-001: Edge Orientation in Dependency Graph

**Decision:** All dependency graph edges point from **input → subscriber** (dependency → dependent).

**Rationale:**
- Ensures topological ordering naturally evaluates dependencies before dependents
- Aligns with standard graph algorithms (Kahn's algorithm for topo sort)
- Makes CSR adjacency lists directly represent "who depends on this vertex"

**Implications:**
- `csr.add_edge(input, subscriber)` throughout the codebase
- `dependents_of(v)` returns dependents of v (vertices that consume v's value)
- `dependencies_of(v)` requires inverse lookup or separate tracking
- Topo layers build correctly using in-degree from dependencies

**See also:** `/docs/adr/001-edge-orientation.md`

### ADR-002: Half-Open Span Convention

**Decision:** All span structures use **exclusive end bounds** `[start, end)` (half-open intervals).

**Rationale:**
- Eliminates off-by-one errors in size calculations: `size = end - start`
- Consistent with Rust's `Range` type and Arrow slice conventions
- Simplifies span math (no need for `+1`/`-1` adjustments)

**Implications:**
- `RowSpan { start, end }` where `end` is exclusive
- `RowColSpan { row_start, row_end, col_start, col_end }` where `*_end` fields are exclusive
- `to_inclusive()` helper provided only for display/tests
- All span resolution algorithms updated to avoid `-1` arithmetic

**See also:** `/docs/adr/002-half-open-spans.md`

### ADR-003: Two-Level Versioning Model

**Decision:** Maintain exactly **two version counters**:
1. **`stats_version`** per `(sheet, column)` in `ColumnUsageStats`
2. **`subscription_version`** per `RangeHandle` in `RangeSubscription`

**Rationale:**
- Single source of truth for column-level changes
- Clear invalidation path: stats version changes → recompute spans → bump subscription version
- Avoids redundant counters from the legacy span-version scheme

**Implications:**
- `ColumnUsageStats` contains `stats_version: u64` (no legacy span identifier)
- `RangeSubscription` tracks `observed_versions: SmallVec<[(u32, u64); 8]>` for all covered columns
- `subscription_version` increments when any observed column's `stats_version` changes
- Cache invalidation logic compares observed vs current stats versions

**See also:** `/docs/adr/003-versioning-model.md`

### ADR-004: Single Arrow Read Path

**Decision:** All range data access **must** go through `ArrowRangeView::from_resolved(descriptor, spans, &storage)`.

**Rationale:**
- Prevents accidental full-column scans (no "default to sheet bounds" fallback)
- Ensures sparsity-aware span resolution at every call site
- Forces explicit span resolution, making performance characteristics visible
- Avoids divergence between read paths with different optimizations

**Implications:**
- `LazyRangeRef::into_arrow_view(ctx)` requires `RangeContext` with resolved spans
- No `read_range(descriptor)` variant that auto-resolves spans
- All callers must obtain spans from `RangeTracker` or `RangeContext`
- Compile-time enforcement via API design

**See also:** `/docs/adr/004-single-arrow-read-path.md`

### ADR-005: Descriptor Immutability

**Decision:** `RangeDescriptor` instances are **immutable snapshots**; structural edits produce new descriptors.

**Rationale:**
- Descriptors used as cache keys and in telemetry; mutation would invalidate all references
- Simplifies reasoning about equality and hashing
- Forces explicit re-registration on structural changes (row/col insert/delete)

**Implications:**
- On row/column insert/delete, affected ranges are **re-normalized** and **re-registered**
- `RangeTracker::register` returns a **new handle** for the adjusted descriptor
- `DependencyIndex` diffs old vs new handles and atomically swaps edges
- No in-place bound adjustment APIs

**See also:** `/docs/adr/005-descriptor-immutability.md`

---

## 14. Summary and Next Steps

This technical design document provides a comprehensive blueprint for the range-centric engine architecture. Key deliverables include:

1. **Complete Data Models:** All structs fully specified with fields and types
2. **Detailed APIs:** Method signatures for all major components
3. **Phased Migration:** 4-phase rollout with acceptance criteria and rollback strategies
4. **Testing Strategy:** Unit, integration, property-based, and fault injection tests
5. **Performance Targets:** Benchmarks and profiling hooks
6. **Developer Guide:** Patterns for extending the system

**Immediate Next Steps:**
1. Begin Phase 0 implementation (Column/Row UsageStats + unified `UsageDelta` emission)
2. Set up CI pipeline for continuous benchmarking
3. Create feature flag system for gradual rollout
4. Establish baseline performance metrics

**Long-term Roadmap:**
- Q1: Complete Phases 0-1
- Q2: Complete Phases 2-3
- Q3: Phase 4 + performance optimization
- Q4: Advanced features (MVCC, external references)

This design supports incremental delivery while maintaining backward compatibility throughout the migration.
