# Evaluation Resources and Target-Driven Cutover

Status: Approved implementation contract; C1a through C4 implemented; C5 next

This document defines the staged cutover from workbook-wide preparation and mixed evaluation to
resource-accounted, target-driven preparation and evaluation. It complements
[Adaptive Formula Partition](adaptive-formula-partition.md): that document describes the long-term
formula-node model, while this document governs the compatibility boundary, request algorithms,
transactions, public API migration, and rollout gates needed to reach it safely.

The program deliberately combines two changes:

1. Evaluation limits become typed budgets with explicit compatibility behavior.
2. Every targeted evaluation API prepares and evaluates one complete mixed-producer closure.

Changing limits without an exact overflow strategy would make FormulaPlane less available than Off
mode. Adding target evaluation without transactional staged-source discovery would under-approximate
cross-sheet work. The two tracks therefore ship as one cutover.

## 1. Compatibility Contract

Given the same workbook, explicit evaluation budgets, cancellation or deadline, and supported
semantics, authoritative mode must not return a FormulaPlane-only resource error for a request that
Off mode completes. FormulaPlane optimization overflow selects another exact strategy. Common hard
budgets may reject both modes with the same typed error and unchanged retry state.

With all evaluation budget fields unset, current public behavior is preserved. Authoritative
execution must keep degrading through paging, sorted runs, and repeated indexed passes until it reaches
a limit that equivalent Off-mode graph or schedule work would also reach. Zero FormulaPlane-only
rejection is the invariant; identical failure points are required only under shared explicit budgets.

`FormulaPlaneMode::Off` remains the default independently of evaluation-budget rollout. Explicit
budgets must work in Off and Shadow before any authoritative-default discussion.

## 2. Current State and Cutover Boundary

### 2.1 Deferred graph preparation

Staged formulas live in `Engine::staged_formulas`, keyed by sheet. Each staged sheet retains formula
insertion order, coordinate lookup, and optionally a non-clone deferred source package.

```text
Workbook::prepare_graph_all
  -> Engine::build_graph_all
     -> take every staged sheet
     -> build_graph_from_staged_batches
        -> prepare_staged_formula_batches
        -> ingest_formula_batches
        -> ingest_compressed_formula_source_batches
        -> finish_compressed_formula_sources

Workbook::prepare_graph_for_sheets
  -> Engine::build_graph_for_sheets
     -> remove selected staged sheets
     -> the same batch pipeline
```

Failures during parsing and preflight restore staged batches and diagnostics. The legacy prepare-all
pipeline still uses separate ingest publications. C2/C3 target preparation instead composes ordinary
formulas, exact compressed replay, and checked FormulaPlane append into one all-or-nothing
transaction.

Sheet selection is not dependency selection. A formula on a target sheet may depend transitively on
staged formulas on any other sheet. Existing cell, cell-list, and cancellable evaluators therefore
drain all staged sheets before evaluation.

C0-pre, shipped as PR #189, applies the same rule to `evaluate_cells_with_delta`: deferred delta
evaluation drained every staged sheet and had cross-sheet value/delta parity plus parse-failure state
coverage. C4 removes the former active-FormulaPlane empty-delta shortcut and feeds legacy, span, and
spill writes into mixed target delta collection.

### 2.2 Evaluation entry points before cutover

```text
evaluate_cell / evaluate_cells / evaluate_cells_cancellable
  -> build_graph_all when deferred
  -> active spans ? evaluate_authoritative_formula_plane_all : legacy demand evaluation

evaluate_cells_with_delta
  -> build_graph_all when deferred (C0-pre, PR #189)
  -> active spans ? full authoritative coordinator + empty delta : legacy demand + delta

evaluate_until / evaluate_until_cancellable
  -> active spans ? full authoritative coordinator : legacy demand evaluation

evaluate_recalc_plan
  -> build_graph_all when deferred
  -> active spans ? full authoritative coordinator
  -> dynamic refs ? evaluate_all
  -> otherwise replay a full-workbook legacy schedule filtered by dirty vertices
```

The active-span gate is correct but defeats target locality. Legacy demand traversal starts from
`VertexId` targets, follows legacy graph precedents, schedules SCCs and layers, clears selected dirty
flags, and re-dirties volatiles. FormulaPlane producers are not visible to that traversal.

Current `RecalcPlan` stores a legacy schedule plus `has_dynamic_refs`; it is neither target-scoped nor
revision-bound. Compatibility-shaped plans escalate dynamic references to `evaluate_all`, and that
behavior must remain until a caller explicitly selects the new target plan policy.

### 2.3 Authoritative evaluation and capacity fallback

The authoritative coordinator leases graph-owned `FormulaDirtyState`, compiles or reuses mixed
topology, derives a dirty schedule, transactionally demotes mixed cycles, evaluates mixed layers,
flushes computed writes after each layer, and acknowledges the exact lease on success.

Mixed topology is retained under candidate, edge, and memory caps. Today an incomplete topology can
route through exact span demotion and legacy completion. Demotion is atomic and correct, but expands
placements into per-cell graph formulas under a finite materialization cap. It is not an acceptable
cache-overflow strategy for large workbooks.

The demotion bridge remains valid only for true mixed SCC semantics, explicit lifecycle demotion, or
operator-requested compatibility materialization. Cache, candidate, edge, retained-memory, and dirty
closure work overflow must not consume the materialization cap.

### 2.4 SheetPort before cutover

Current one-shot SheetPort evaluation chooses among prepare-all, prepare-output-sheets, and
error-then-prepare-all/evaluate-all. Names and native tables force full evaluation. This conflates
opaque selector semantics, staged dependency discovery, active FormulaPlane work, and genuine
failures. It can duplicate expensive work and can swallow typed errors.

The fallback ladder remains only until C5 has target preparation, mixed evaluation, names, tables,
layout sentinels, cancellation, and stale-plan policy. C5 removes it rather than broadening it.

## 3. Definitions and Invariants

### 3.1 Closure vocabulary

These directions must never be conflated:

- **Demand closure** starts at requested targets and follows dependency precedents to every producer
  needed to compute those targets. It uses producer ownership and precedent adjacency or queries.
- **Dirty closure** starts at changed cells or producer result regions and follows consumer-read
  dependents to work whose stored value may now be stale. It uses consumer-read indexes and dirty
  projection.

Target preparation and target scheduling build a demand closure. Dirty propagation and full dirty
scheduling build a dirty closure. Every implementation traversal, metric, and test names which
closure it is computing.

### 3.2 Required invariants

1. **One authority.** Committed dependencies, producer topology, values, and dirty state remain
   graph-owned. The staged index records only uncommitted source presence and geometry.
2. **Complete demand closure.** Targets include all static precedents, cycle peers, live virtual
   dependencies, symbols, tables, sources, staged formulas, spills, and FormulaPlane producers.
3. **No partial topology.** Incomplete cache, visitor, work, memory, stale, deadline, or cancellation
   results are never consumed as a closure or schedule.
4. **Atomic target preparation.** Commit publishes all selected staged/source/graph/FormulaPlane work
   and removes exactly its leases, or publishes no semantic state.
5. **Explicit inert residue.** Failed or cancelled preparation may retain only enumerated,
   value-invisible residue: interned immutable ASTs, string/symbol interner growth, and warmed
   read-only caches. Digest tests exclude exactly these. Sheet IDs, graph vertices and edges,
   authority, staged ownership, diagnostics, reports, dirty state, overlays, epochs, and visible
   values are not residue and must remain unchanged. Any addition to the residue list requires a
   contract change.
6. **Exact dirty ownership.** Successful target evaluation acknowledges only closure events whose
   writes were flushed. Unrelated events and events arriving during evaluation remain pending.
7. **Layer visibility.** A mixed layer flushes before its dependents run. Delta observes those same
   committed writes. Cancellation never exposes half a layer.
8. **Opaque correctness.** Dynamic references, unknown context reads, unresolved cross-sheet
   bindings, and unsupported source semantics widen before evaluation; they never under-approximate.
9. **Cycle parity.** Legacy cycle policies remain unchanged. Mixed SCCs use one exact prepared
   demotion transaction until native refinement replaces it.
10. **Mode parity.** Off and Shadow keep replay order, values, diagnostics, and cached-value semantics.
    Optimization overflow cannot become an authoritative-only rejection.
11. **Bounded cancellation.** Planning and evaluation are cooperative. Prepared commit is
    non-cancellable only after a deadline-feasibility check and final validation.
12. **Monotone scope.** Widening is a set-union lattice for the entire request, including runtime
    retries: `Exact` is contained by a set of `Sheets`, which is contained by `Workbook`.
13. **No broad error fallback.** Only an explicit `Widen` decision changes scope. Parse, resource,
    cancellation, stale-plan, semantic, and selector errors propagate unchanged.
14. **Volatile epoch consistency.** All volatiles evaluated by one request share its evaluation epoch.
    Unrelated volatiles remain dirty/stale; target evaluation is not a workbook recalc tick.

## 4. Typed Resource Model

### 4.1 Cap classes

Every limit has one primary class and an explicit overflow rule:

| Class | Meaning | Overflow rule |
| --- | --- | --- |
| `S` semantic/format | Changes workbook meaning or supported shape | Explicit semantic or format error; never auto-tune |
| `A` admission/availability | Common hard load or request envelope | Typed, atomic error shared by modes |
| `R` retained memory | Cache or retained state | Skip, evict, or select another exact strategy |
| `X` scratch memory | Per-request temporary state | Page, sort/merge, repeat passes, or common typed error |
| `W` work/time | CPU visits or deadline | Change exact strategy, widen, or return common deadline/resource error |
| `O` optimization eligibility | Representation or fast-path threshold | Choose an equivalent representation; never change values |

Important classifications include:

| Limit family | Class and cutover behavior |
| --- | --- |
| Excel rows/columns | `S/A`; preserve format bounds in every mode |
| Logical/dense sheet cells | split `A/X`; distinguish populated source admission from dense materialization |
| FormulaPlane fallback cells | `A/X`; rename toward materialization transaction cells; cycle/lifecycle only |
| Formula replay spool bytes/files | `A/X`; hard correctness-state limits with separate memory/disk/file telemetry |
| Replay memory prefix and sparse ingest thresholds | `O/R/W`; strategy switches only |
| Source family members/fragments/evidence/exclusions | `O/R/W`; exact replay/legacy representation on overflow |
| Mixed topology candidates/edges/bytes | `R/X/W`; soft cache limits, followed by exact request topology |
| Lookup/warmup/source caches | `R/O`; skip or evict, never affect values |
| Range expansion, stripe shape, PK visit/compaction | `O/R/W`; preserve exact compressed/indexed dependencies |
| Spill cells/output | `S/A/X`; common semantics, separate from FormulaPlane materialization |
| Cycle iteration settings | `S/W`; calculation semantics, never budget-derived |
| Dirty closure iteration | `W`; existing typed incomplete reason must route to an exact full closure |
| SheetPort layout scan | `S/W/A`; explicit selector/manifest limit with typed exhaustion, never silent truncation |

The implementation inventory starts from these current defaults and internal guards:

| Current cap or threshold | Class | Required treatment |
| --- | --- | --- |
| 1,048,576 rows and 16,384 columns | `S/A` | Keep Excel format bounds in every mode |
| 128M sheet logical cells | `A/X` | Split dense materialization from populated/source admission |
| 2M FormulaPlane fallback cells | `A/X` | Keep finite; migrate to materialization transaction budget |
| Sparse threshold 250k and ratio 1024 | `O/W` | Storage strategy only |
| Replay spool 256 MiB/sheet, 1 GiB/workbook, 1024 files | `A/X` | Keep hard and separately telemeter memory, disk, encoded bytes, and files |
| Replay prefix 1 MiB; memory-only spool 16 MiB | `O/R` then `A/X` | Adaptive native switch; explicit no-disk host limit |
| Family members 4,096; fragments 128; evidence/bindings 8 MiB | `O/R/W` | Select exact replay/legacy representation |
| Family exclusions 64; dependency exclusions 4,096; axis gap scan 1M | `O/R/W` | Strategy/work limit, never partial evidence |
| Mixed cache 100k candidates, 100k edges, 64 MiB | `R/X/W` | Soft cache skip into exact request topology |
| Lookup cache 64 MiB | `R` | Account actual retained indexes and evict/skip |
| Range expansion 64; stripe dimensions 256 | `O/R/W` | Preserve compressed/indexed dependencies |
| PK visit 50k and compaction interval 100k | `O/W` | Exact rebuild on exhaustion |
| Dirty closure 100k iterations | `W` | Consume typed incomplete only by exact conservative reroute |
| Mixed cycle demotion 64 rounds; virtual replan 5 rounds | `W/A` and `W/O` | Exact terminal strategy with reason, never partial commit |
| Spill output 10k cells | `S/A/X` | Reconcile with larger generated-array guards before allocation |
| SheetPort layout scan 100k rows | `S/W/A` | Typed selector exhaustion and explicit manifest override |

`max_vertices` is deprecated as an ambient `EvalConfig` field. New
`graph_vertex_hard_limit` and `graph_edge_hard_limit` remain declarative and diagnostic throughout
C1a. A legacy value maps into an otherwise-unset explicit budget field, but no budget changes graph
acceptance in C1a. Activation moves to C2, after direct, bulk, logged, replacement, demotion, staged,
and generic graph mutation paths share one composed prepared transaction. Existing baseline
`max_vertices` behavior is
not broadened by the mapping.

The unused `max_memory_mb` and `max_eval_time` hooks are deprecated. Legacy memory bytes split 50/50
between retained and request scratch totals (an odd byte belongs to retained), and legacy time maps to
the request deadline. Precedence is field-level: an explicit destination budget wins and the
conflicting legacy mapping is ignored, while every non-conflicting legacy destination still maps. One
once-per-engine diagnostic reports each source/destination as mapped, ignored by an explicit budget,
or absent. `max_vertices` follows the same rule for the declarative graph vertex limit.

### 4.2 Budgets and envelope derivation

`EvalConfig::evaluation_budgets` is the sole evaluation resource configuration object. Its default has
all fields unset. `ResourceEnvelope` is an optional aggregate value-to-budgets derivation helper; it
does not select a mode or named set.

```rust
pub struct ResourceEnvelope {
    pub retained_bytes: u64,
    pub request_scratch_bytes: u64,
    pub materialized_graph_bytes: u64,
    pub max_work_units: u64,
    pub deadline: Option<Duration>,
    pub max_threads: usize,
    pub disk_scratch: DiskScratchPolicy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SemanticResourceBudget {
    pub max_rows: Option<u32>,
    pub max_columns: Option<u32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdmissionResourceBudget {
    pub graph_vertex_hard_limit: Option<usize>,
    pub graph_edge_hard_limit: Option<usize>,
    pub materialization_cells: Option<u64>,
    pub materialized_graph_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetainedResourceBudget {
    pub total_bytes: Option<u64>,
    pub mixed_cache_bytes: Option<u64>,
    pub lookup_cache_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScratchResourceBudget {
    pub total_bytes: Option<u64>,
    pub schedule_discovery_bytes: Option<u64>,
    pub graph_source_bytes: Option<u64>,
    pub spill_overlay_bytes: Option<u64>,
    pub disk_scratch_policy: Option<DiskScratchPolicy>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkResourceBudget {
    pub max_work_units: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeadlineResourceBudget {
    pub max_elapsed: Option<Duration>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OptimizationResourceBudget {
    pub mixed_cache_candidates: Option<usize>,
    pub mixed_cache_edges: Option<usize>,
    pub max_threads: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EvaluationBudgets {
    pub semantic: SemanticResourceBudget,
    pub admission: AdmissionResourceBudget,
    pub retained: RetainedResourceBudget,
    pub scratch: ScratchResourceBudget,
    pub work: WorkResourceBudget,
    pub deadline: DeadlineResourceBudget,
    pub optimization: OptimizationResourceBudget,
}
```

Budget derivation is a neutral value conversion using explicit envelope arithmetic, never ambient
free-memory sampling or a named default. The current shape is:

- Retained total is copied exactly; its 12.5% cache pool is split 60% to FormulaPlane topology and 40%
  to lookup indexes.
- Request scratch total is copied exactly and split 50% to schedule/discovery, 35% to graph/source
  preparation, and the remainder to spill/overlay flush.
- Materialized graph bytes, work units, deadline, maximum threads, and disk scratch policy are copied
  exactly into their corresponding optional budget fields.
- Mixed-cache candidate and edge limits each derive from 64-byte units in the mixed-cache share.
- WASM can derive explicit smaller, memory-only budgets from an envelope.

Recommended named constructors and default sets are deferred until cold calibration. If introduced,
they will produce ordinary `EvaluationBudgets` values; they will not be enums, profiles, modes, or
precedence layers. Interactive workbooks and SheetPort retain all-unset budgets until a separately
reviewed rollout.

### 4.3 Request ledger and typed errors

One `ResourceLedger` owns checked retained reservations, scratch reservations, work charges, and
deadline checks. Scratch is released on request exit. Reservations combine estimates with allocator
`try_reserve`; telemetry records estimated and observed capacity bytes.

Native topology scratch has a policy separate from the formula replay spool. Replay is correctness
state with request-spanning lifetime; topology runs and transaction journals are delete-on-drop
request scratch. They may share a temporary root, not byte or file-count budgets. WASM/no-disk uses
bounded in-memory runs and repeated indexed passes. Its worst-case pass count is explicit and charged
to work units.

A strategy-level ledger failure chooses another exact algorithm. Final shared exhaustion returns a
typed `Resource` detail and leaves leases and staged state retryable. C1 includes a public-surface
audit for Python, WASM, serialized diagnostics, and SheetPort, plus a mapping from current demotion
`LoadLimit` details to new resource details. Compatibility of error kind strings is documented and
tested.

## 5. Exact Non-Materializing Topology Ladder

Mixed cache overflow follows this ladder:

```text
complete retained mixed topology
  -> exact paged request topology
  -> bounded in-memory sorted runs and merge
  -> native delete-on-drop sorted/deduplicated edge records, if policy permits
  -> bounded repeated indexed passes (required no-disk path)
  -> common typed resource/deadline error
```

The implementation contract is:

1. Retained compilation returns `Cached(CompleteTopology)` or
   `CacheSkipped { reason, observed }`. Partial topology is never usable or cached.
2. `ExactRequestTopologyBuilder` visits producer-result and consumer-read indexes in resumable,
   exact pages. Page size bounds scratch, not total matches.
3. Producer dedup first uses a ledger-backed set, then sorted runs and merge. No strategy stores one
   item per span placement.
4. Full evaluation stores `O(producers + actual boundary edges)`, not `O(placements)`.
5. The dirty fixed-point limit already reports an incomplete fallback reason. Consumers discard its
   partial result and invoke the exact builder for a conservative complete dirty schedule.
6. Cache overflow never invokes span demotion or charges materialization cells.
7. True mixed SCCs, lifecycle operations, and explicit operator materialization retain the prepared
   demotion bridge and exact materialization preflight.
8. A per-engine `cache_skipped` streak records consecutive exact rebuilds. Telemetry names the cap,
   observed size, selected strategy, pass count, and operator remedy: raise the retained budget.

A workload that perpetually skips cache must remain value-identical while emitting the streak; hidden
rebuild cost is not acceptable.

## 6. Target APIs and Pure Discovery

### 6.1 Public model

```rust
#[derive(Clone, Debug)]
pub enum EvaluationTarget {
    Cell { sheet: String, row: u32, col: u32 },
    Range(RangeAddress),
    Name { name: String, scope_sheet: Option<String> },
    Table { name: String, selection: TableSelection },
}

pub struct PrepareTargetsOptions<'a> {
    pub request_id: Option<RequestId>,
    pub cancel: Option<&'a AtomicBool>,
    pub deadline: Option<Instant>,
    pub budgets: Option<&'a EvaluationBudgets>,
    pub opaque_policy: OpaquePreparePolicy,
}

pub struct PreparedTargetGraphReport {
    pub requested_targets: usize,
    pub normalized_regions: usize,
    pub selected_staged_cells: usize,
    pub selected_source_families: usize,
    pub retained_staged_cells: usize,
    pub widened_scope: PrepareScope,
    pub widening_reasons: Vec<OpaqueReason>,
    pub revisions: PreparationRevision,
    pub commit_window: Duration,
    pub estimated_scratch_bytes: u64,
    pub observed_scratch_bytes: u64,
}

pub fn prepare_graph_for_targets(
    &mut self,
    targets: &[EvaluationTarget],
    options: PrepareTargetsOptions<'_>,
) -> Result<PreparedTargetGraphReport, ExcelError>;
```

Names and tables remain engine targets because the engine owns scoping, shadowing, symbol vertices,
and their formula/value semantics. Workbook and bindings may provide cell/range convenience wrappers,
but must not duplicate symbol resolution outside the authority.

All evaluation methods become adapters over one internal coordinator:

```rust
fn evaluate_targets(
    &mut self,
    targets: &[EvaluationTarget],
    mode: TargetEvalMode<'_>,
) -> Result<TargetEvalResult, ExcelError>;
```

No adapter retains a private staged-build or active-span gate.

### 6.2 Staged source-presence index

`StagedFormulaIndex` indexes coordinates, source-family geometry, stable insertion order, package
identity, and a revision. It never supplies evaluated values or committed dependency truth.
Structural edits, formula edits, rename/remove, package invalidation, and sheet lifecycle update it in
the same transaction as staged storage. An audit test proves that every such mutation changes its
revision so prepare/commit stale checks are honest.

C3 selects an entire deferred package when any of its compressed-family, fragmented-family, or exact
fallback geometry intersects the demand closure. Selection and consumption are package-atomic:
family-level residual replay ownership is never split, and unrelated packages remain staged. Residual
splitting is deferred until after C5 as a separately reviewed optimization.

### 6.3 Discovery and widening

Discovery is logically pure: it snapshots revisions, creates transaction-local plans and leases, and
publishes no semantic state. Parser/interner residue is permitted only under Invariant 5.

The algorithm is:

1. Snapshot engine identity, topology and authority revisions, semantic/provider revisions,
   name/table/source revisions, staged-index revisions, limits, and budget values.
2. Normalize targets. Preserve name/table symbol roots while adding their concrete result regions.
3. Seed a deterministic demand-closure queue of cells, regions, and symbols.
4. For each item, resolve committed legacy producers, spill anchors, FormulaPlane result producers,
   and intersecting staged units. Analyze staged units under the immutable semantic snapshot and
   enqueue their direct cell/range/name/table/source precedents.
5. Query committed precedent topology to continue the demand closure. Never use the dirty-closure
   consumer direction for this traversal.
6. Track visited identities with staged generations, `VertexId`, `FormulaSpanRef` generation,
   symbol revision, and region key. Cycles terminate discovery by identity and are scheduled later.
7. Select each intersecting package as a whole, compose complete and fragmented family disposition,
   and retain exact exceptions and replay records in source order.
8. On opaque semantics, return a widening decision rather than a partial plan.

Widening uses set union and moves strictly upward for the request lifetime:

- Proven sheet-local dynamic reference -> include the required sheet set.
- Explicit external declared regions -> include those regions.
- Workbook name, unknown custom context access, unresolved cross-sheet/table/source binding, runtime
  text reference, or uncertain default-sheet binding -> workbook.
- Any escape without a proven containing scope -> workbook.

A runtime virtual-dependency escape jumps directly to the smallest proven containing scope, otherwise
workbook. Correct preparation committed by an earlier aborted evaluation attempt remains committed;
it is valid graph state, not rollback state. Discovery, runtime retries, and all scratch charges share
one request ledger. The final permitted attempt is workbook-wide exact evaluation, preventing an
incremental widening loop.

## 7. Composed Prepared Transaction

`PreparedGraphForTargets` composes the existing prepared legacy plan, checked FormulaPlane append,
fragmented source transaction, replay disposition, and dirty publication:

```rust
struct PreparedGraphForTargets {
    engine_token: Arc<()>,
    assumptions: PreparationRevision,
    selected_staged: Vec<StagedUnitLease>,
    selected_packages: Vec<PreparedWholePackage>,
    legacy_graph: PreparedLegacyGraphPlan,
    formula_plane: PreparedFormulaPlaneAppend,
    symbol_assumptions: Vec<SymbolAssumption>,
    diagnostics_delta: Vec<FormulaParseDiagnostic>,
    ingest_report_delta: FormulaIngestReport,
    dirty_delta: PreparedFormulaDirtyDelta,
    resource_reservation: PreparedResourceReservation,
    estimated_commit_work: CommitWorkEstimate,
}
```

Commit order is fixed:

1. Check cancellation and deadline.
2. Estimate the non-cancellable commit duration from preflighted counts and compare it with remaining
   deadline. If it cannot fit, exit with zero semantic mutation. Telemetry compares estimate with
   actual duration; estimate accuracy is observability, not a correctness assumption.
3. Revalidate every revision, lease generation, symbol/source assumption, span reference, target
   conflict, budgets and limits.
4. Reserve final graph, AST, index, authority, dirty, report, and staged-residual capacities.
5. Run injected fault seams before first mutation.
6. Apply prevalidated legacy and FormulaPlane plans without logical failure or allocation.
7. Remove exactly leased staged formulas and whole packages; retain every unselected package/index entry.
8. Publish diagnostics, reports, dirty events, and topology revisions once.

There is no cancellation point from steps 6 through 8. The section is bounded by final counts and
instrumented. Every failure before step 6 preserves semantic state under the explicit residue policy.
The current three sequential ingest publications are not used by target preparation.

## 8. Mixed Target Evaluation

### 8.1 Producer roots and demand closure

Targets resolve to mixed producer roots:

```rust
enum TargetProducer {
    Legacy(VertexId),
    Span { span_ref: FormulaSpanRef, demanded: Region },
    Symbol(VertexId),
    ValueOnly(CellRef),
}
```

A span cell carries a demanded result region, a spill child resolves to its anchor, and a name/table
keeps its symbol root plus concrete regions. `ValueOnly` returns immediately only after preparation
proves no staged or committed producer owns the cell.

Cached mixed topology gains precedent adjacency. Cache-skipped requests use the exact request builder.
Demand traversal pulls all precedents, intersects spans with demanded result regions, and finally
intersects work with dirty/volatile domains. An unrelated dirty producer is not scheduled merely
because it shares a sheet or authority.

### 8.2 Scheduling, cycles, volatiles, and dirty subleases

The coordinator retains mixed topological layers and per-layer `ComputedWriteBuffer` flush. When a
demanded member belongs to an SCC, the complete SCC unit is included. Pure legacy SCCs preserve
existing cycle configuration. Mixed SCCs use one exact prepared demotion transaction until adaptive
cycle refinement replaces it.

C4 must add FormulaDirtyState subleases or partial acknowledgement. Success acknowledges exactly the
target closure's flushed events; identical or unrelated events arriving during evaluation remain
pending. Fault tests cover exact closure acknowledgement, post-lease event retention, and failure
before acknowledgement. This is required for success independently of cancellation behavior.

All volatile producers inside a request share one evaluation epoch. A volatile outside the demand
closure remains dirty and retains its old value. Tests use two `NOW()` cells to pin this behavior and
document that targeted evaluation is not a workbook recalc tick.

### 8.3 Cancellation

Checks occur before and after every discovery page and family parse, before final preparation
validation, before topology pages, SCC tasks, mixed layers and large span chunks, and before computed
buffer flush.

A cancelled uncommitted plan is dropped. Cancellation after preparation leaves newly committed dirty
work pending. Initial C4 cancellation acknowledges no sublease, even if earlier complete layers
flushed; retry recomputes idempotently. Cancellation-time acknowledgement of completed subleases is a
later optimization, not part of the initial cutover.

### 8.4 Delta representation

C4 removes the empty-authoritative-delta shortcut. Legacy, span, and spill writes feed one delta
collector at layer flush.

The delta API gains a versioned additive run/region representation. Large span or spill changes do
not allocate one delta record per placement. Legacy callers may request per-cell records up
to a documented cap; caller policy then either upgrades to run/region records or returns a typed
common overflow. Python, WASM, serialized diagnostics, and SheetPort mappings are audited with the
resource error surface.

### 8.5 Revision-bound recalc plans

```rust
pub struct RecalcPlan {
    key: RecalcPlanKey,
    targets: Vec<EvaluationTarget>,
    scope: PrepareScope,
    topology: RecalcTopology,
    dynamic: DynamicPlanPolicy,
}
```

The key includes graph, authority, semantic/provider, staged, name/table, span-generation, and budget
value revisions. A stale plan returns `PlanStale` with the revision category that moved; the engine
never replays stale producer identities. SheetPort batch may opt into `RebuildOnStale` because it owns
the workbook and target list.

The existing full already-prepared legacy plan API remains initially. Compatibility-shaped plans
preserve today's `has_dynamic_refs -> evaluate_all` escalation. New target plans may explicitly choose
bounded replan-with-monotone-widening.

## 9. SheetPort Migration and Fallback Policy

C5 changes one-shot and batch evaluation as follows:

1. Resolve cell, range, record, layout, name, and native-table selectors into typed targets. Layout
   targets include terminator/sentinel cells used during output reading.
2. Call target preparation once and target evaluation once.
3. Remove prepare-all, prepare-output-sheets, and generic error-then-full-eval branches.
4. Resolve Cell/Range names through symbol-aware targets; formula/literal names retain the symbol
   vertex. Resolve table headers/data/totals through table metadata.
5. Return typed errors for unsupported structured selector syntax. Never reinterpret an error as a
   request for full evaluation.
6. Make layout scan exhaustion a typed selector error controlled by an explicit manifest/selector
   setting. Evaluation budgets cannot change selector meaning.
7. Prepare batch targets before building a revision-bound target plan. Apply `PlanStale` or
   `RebuildOnStale` according to batch options.
8. Restore evaluation options with RAII across preparation, cancellation, resource, stale, and
   evaluation failures.

Before C5, the existing conservative prepare-all behavior stays in place rather than introducing an
intermediate partial fallback. After C5, conservative widening is the only path to full preparation
or evaluation. It is a successful strategy decision with a reason, not an error catch.

Tests stop requiring all staged formulas to be consumed. They require every reachable cross-sheet
chain to be consumed, unrelated staged units to remain, and a later request to prepare/evaluate those
units correctly.

## 10. Ratified Design Decisions

| # | Decision |
| --- | --- |
| 1 | Parity is under the same explicit budgets; all-unset budgets additionally guarantee no new FormulaPlane-only rejection. |
| 2 | Mixed cache limits are soft. Exact request topology handles overflow; demotion is cycle/lifecycle/operator-only. |
| 3 | Native disk scratch is allowed by a separate request-scratch policy. WASM uses bounded runs and repeated passes with a charged pass bound. |
| 4 | Deprecate ambient `max_vertices`; explicit vertex/edge hard limits apply at every mutation seam, never newly with all budget fields unset. |
| 5 | Deprecate `max_memory_mb` and `max_eval_time`; map them to explicit retained/scratch/deadline budgets with diagnostics. |
| 6 | All-unset budgets remain the interactive and SheetPort default; FormulaPlane default is independent. |
| 7 | Whole deferred family/package is the C3 atomic unit. Residual family ownership is post-C5 work. |
| 8 | Opaque widening is monotone `Exact -> Sheets -> Workbook` across discovery and runtime retries under one request ledger. |
| 9 | Engine exposes Name and Table targets because it owns symbol resolution and dependency roots. |
| 10 | Engine returns typed `PlanStale` with reason; SheetPort batch may opt into rebuild. |
| 11 | Initial cancellation acknowledges no dirty sublease; success requires exact partial acknowledgement at C4. |
| 12 | C0 records cold baselines; performance numbers are ratified at C1 exit and revalidated at C6. |
| 13 | Authority admission needs no future-cycle capability certificate; an over-cap true mixed SCC returns a deterministic typed error after exact preflight. |
| 14 | SheetPort layout scan exhaustion is a typed selector error with an explicit manifest setting, never budget-based widening or truncation. |
| 15 | Delta adds versioned run/region records; per-cell compatibility output is bounded and policy-controlled. |

## 11. Delivery Order

### C0-pre - correctness hotfix

PR #189 makes deferred `evaluate_cells_with_delta` drain all staged sheets, matching sibling APIs. It
includes cross-sheet transitive value/delta parity and strict parse-failure restoration. Its former
active-span empty-delta limitation is removed by the C4 mixed collector.

### C0 - contract, telemetry, and cold harness

Status: Implemented observationally. Engine request IDs are monotonic and are not reset with
telemetry counters. Stable request/baseline stats expose staged preparation, mixed topology
cache/overflow observations, exact fallback materialization, dirty-lease outcome, phase timings, and
success/cancellation/error outcome. Requests aggregate every topology build, hit, skip, and cap event;
build-attempt producer/candidate/edge work is summed and retained bytes preserve the request peak.
The load-envelope probe reports these with process RSS/HWM, output-read time, replay spool
memory/disk/file counts, FormulaPlane mode, and fresh-child sample
identity. Eager loader graph preparation remains part of `load_ms`; request-time deferred graph
preparation is reported separately. Allocator-specific byte counters remain omitted because no stable
global allocator observer exists; no allocator or evaluation behavior was changed to obtain them.

- Land cap classes, typed reason vocabulary, request IDs, closure definitions, and observational
  counters without behavior changes.
- Split replay-spool telemetry into encoded, in-memory, disk, spill-file, and replay counts.
- Record load, target/full preparation, topology, evaluation, output read, RSS/HWM, allocator bytes,
  staged selection, producer/edge counts, cache strategy, widening, materialization, and dirty lease
  outcomes.
- Build a fresh-process fixture runner and store baseline raw JSON artifacts.

Gate: Off, Shadow, and authoritative values/errors are unchanged; telemetry is observational.

### C1a - contract, ledger, deadlines, and typed completeness

Status: Implemented. One explicit `EvaluationBudgets` value resolves without ambient host sampling;
all fields are unset by default. `ResourceEnvelope` only derives a budget value. One request-bound
checked ledger is shared by nested public coordinators across modes; typed resource details retain the
existing Excel error kind. Legacy fields merge at the destination-field level, with explicit fields
winning conflicts and one diagnostic reporting every mapped or ignored destination. C1a activates
common work and deadline limits while graph vertex/edge and materialization budgets remain declarative
until C2. C1b additionally activates only retained mixed-cache limits and topology/schedule-discovery
request scratch. Graph/source preparation, spill/overlay, lookup-cache, graph admission, materialization,
and thread budget fields remain inactive.

Topology allocation and semantic errors preserve their baseline errors and never become cache skips
or demotion. Only the pre-existing configured candidate, edge, and byte incomplete stats select the
baseline capacity fallback. Dirty fixed-point incompleteness discards partial closure output and
continues conservatively in the same schedule path. Existing demotion/materialization guards now carry
typed `Resource` details without changing their canonical Excel error kind. Allocator OOM and panic are
process-fatal by contract; C1a does not catch either and continue. Temporary ledger accounting is
restored on every normal `Result` exit, and no unwind is treated as recoverable engine state.

- Add explicit budget/envelope derivation, `ResourceLedger`, legacy deadline checkpoints, typed
  resource details, and binding/serialization audits.
- Deprecate ambient vertex/memory/time fields under field-level precedence rules.
- Preserve baseline topology errors and configured-cap fallback while routing dirty-closure
  incompleteness conservatively without consuming partial data or adding a demotion route.

Mixed-schedule discovery and construction work is charged per attempt to the shared request ledger,
including attempts rebuilt after cycle demotion; retry work is not refunded.

Gate: mapping and deadline tests pass; no consumer accepts an incomplete result.

### C1b - exact request topology

Status: Implemented. Retained compilation publishes only a complete topology or an explicit cache skip. Configured candidate, edge, and retained-byte skips retain FormulaPlane authority and select exact request topology through paged/indexed construction, bounded in-memory runs, explicit native delete-on-drop topology scratch, or bounded work-accounted repeated passes. Native topology scratch is request-owned and independent from formula replay spool ownership and limits. Temporary producer/read indexes are conservatively preflighted against schedule/discovery scratch before construction, then trued up and held until cache publication or exact scheduling completes. Skip streak, cap/observed size, strategy, pass count, native topology disk bytes, typed exhaustion, and operator guidance are exposed in request telemetry. Only retained mixed-cache and topology/schedule-discovery scratch budget seams are active; C2 fields remain declarative.

The C1b residual was the pre-existing configured mixed-cache candidate/edge/byte cap overflow demotion.
C1a did not add a retained-ledger or scratch-ledger overflow route. C2 activates graph hard-limit
enforcement at the common exact preflight used by graph mutation paths.

- Implement exact paged topology, sorted runs, native disk policy, and bounded no-disk repeated passes.
- Add skip-streak telemetry and operator guidance.

Gate: candidate/edge/byte caps at zero and cap+1 preserve Off/authoritative parity, retain spans, and
materialize zero cells absent true SCCs. WASM no-disk passes with work accounting. Ledger observed
scratch stays within estimate plus the accounting tolerance.

### C2 - transactional target preparation in Off and Shadow

Status: Complete. `EvaluationTarget`, target options/reports, Workbook wrappers, the
name-based generation/revision-coupled staged index, bounded sheet-index discovery, exact and Sheets
widening, and prepared legacy addition/replacement publication are active in Off and Shadow.
Graph vertex/edge/materialization-cell/materialized-byte admission uses the common exact preflight for
direct, bulk, logged/editor replay, staged compatibility, prepared legacy, demotion, fragmented, and
generic graph mutations. All-unset budgets bypass preview work. At the C2 boundary, authoritative
mode and deferred source packages retained whole-workbook/whole-package compatibility; C3 now
supplies package-atomic deferred source selection.
FormulaPlane-only direct spans charge no hypothetical legacy materialization.

- Staged source indexing, pure discovery, monotone widening, and composed ordinary publication are
  active.
- Exact common admission covers direct, bulk, logged, replay, replacement, demotion, staged,
  compressed fallback, fragmented, and generic graph publication.
- Fault and seam audits pin semantic state, staged-index revision coupling, and scratch release.

Gate: every pre-commit fault preserves the semantic digest; only reachable staged units commit;
prepare-target values match prepare-all.

### C3 - deferred FormulaPlane source units

Status: Complete. Exact package geometry participates in demand discovery, and intersection with any
complete family, fragmented family, or fallback point selects the whole deferred package. There is no
residual splitting: the selected package is consumed atomically while unrelated packages remain
staged. Complete and fragmented dispositions, exact replay fallbacks, ordinary staged replacements,
legacy graph publication, and checked FormulaPlane append share one prepared transaction.

Off mode replays the selected package into the legacy graph. Shadow analyzes eligible FormulaPlane
placements but retains legacy graph authority. Authoritative experimental mode commits supported
complete or fragmented placements directly and sends exact unsupported or invalidated records through
the legacy graph. All modes preserve source order and last-writer behavior. A package spool is replayed
once per successful preparation (a package attached after ordinary formulas reuses its one indexed
reconciliation replay), replay records are work-accounted in bounded chunks, and stale, parse,
resource, cancellation, deadline, admission, or injected pre-commit failure leaves package ownership,
staged formulas, reports, diagnostics, graph topology, and FormulaPlane authority unpublished and
retryable.

Gate complete: focused eager/deferred Off/Shadow/authoritative parity, source-order, replay-count,
package-retention, exact failure restoration, resource, cancellation, stale-revision, and Calamine
workbook tests pass. No C3 benchmark result is claimed.

### C4 - unified mixed target coordinator

Status: Complete. Retained mixed topology now stores and accounts both consumer and precedent
adjacency. Cache skips retain FormulaPlane authority and use precedent-oriented paged, in-memory-run,
native-policy, or repeated-pass demand closure with parity against the retained closure. Cell, range,
cancellable, until, and delta adapters prepare typed targets and enter one mixed coordinator under the
outer request ledger. Roots preserve legacy and symbol graph scheduling, demanded span regions,
spill-child anchors, and value-only proof after preparation.

Dirty ownership is event scoped. Successful requests acknowledge only events whose complete consumer
closure was demanded, scheduled, evaluated, and flushed; partial span-region events remain whole and
pending. Release and acknowledgement rebuild dedup state while preserving post-lease identical events.
Full evaluation uses the same sublease substrate. Mixed SCCs retain exact prepared demotion and the
existing 64-round/cycle policy, while retry seeds remain closure scoped. Cache overflow never demotes.

Mixed layers retain `ComputedWriteBuffer` visibility, one volatile epoch, cancellation-before-flush,
and five-round virtual dependency replanning. Dynamic FormulaPlane references contribute runtime
regions to mixed precedent topology; scope widening is monotone and telemetry limits the final
workbook-exact attempt to one. Each bounded buffer flush performs deadline commit-window feasibility
preflight before its non-cancellable write phase.

`TargetEvalDelta` version 1 and `EvalDeltaRecord::{Run, Region}` collect legacy, span, and spill
changes without per-placement report expansion. Existing `EvalDelta` remains field compatible and
unbounded by default; callers may opt into an explicit cell limit that returns typed common resource
overflow without truncation. Zero-span and warm no-dirty requests retain their topology-free sparse
paths. These additive Rust APIs are not yet projected into every language binding.

Gate complete: focused topology-oracle, target-root, SCC, sublease, cancellation, dynamic-replan,
volatile, span/spill delta, cache-overflow, sparse/warm, same-ledger, and commit-window tests pass.

### C5 - target recalc plans and SheetPort

- Add revision-bound target plans, stale reasons, and compatibility dynamic escalation.
- Migrate one-shot and batch SheetPort and remove its fallback ladder.

Gate: static/name/table/layout snapshots match, staged consumption is reachable-only, dynamic/opaque
requests widen explicitly, prior errors are not swallowed, and options restore on every exit.

### C6 - calibration and rollout

- Run the full native and WASM/no-disk cold matrices.
- Revalidate C1-ratified performance numbers.
- Keep all-unset budgets as the default. Defer recommended named constructors/default sets until
  calibration; any future helpers return budget values and are not enums or modes.

Budget rollout remains separate from FormulaPlane-mode rollout.

## 12. Validation and Fault Matrix

### 12.1 Resource and topology

- Cache candidate/edge/byte limits at zero, cap, and cap+1.
- Dirty closure limit at zero routes incomplete to exact full closure with no acknowledgement.
- Scratch failure during discovery, topology, schedule, delta, and flush.
- Shared deadline/cancellation checkpoints in Off and authoritative modes.
- Materialization cap enforced only for true cycle/lifecycle demotion.
- Explicit graph vertex/edge limits at every mutation seam.
- Retained accounting includes mirrored indexes, bindings, adjacency, and allocated capacities.
- Zero-span workbooks build no mixed topology and stay on the sparse legacy path.
- Perpetual cache skip preserves values and increments the skip streak.

### 12.2 Discovery and transaction

- Three-sheet staged dependency chain with unrelated staged units retained.
- Independent chains on one sheet and range intersection selection.
- Cycles across staged sheets, names with every definition/scope case, and native tables.
- Dynamic references and unknown providers with exact widening reasons.
- Spill anchors/children and formula/source inspection reads.
- Complete and fragmented families with holes, exceptions, and duplicate replay records.
- Every parse policy publishes diagnostics exactly once.
- Rename, edit, remove, structure, provider, or budget change between prepare and commit returns stale.
- Faults and cancellation before commit preserve graph, authority, staged/package ownership, reports,
  diagnostics, dirty state, overlay, epochs, and visible values except declared inert residue.

### 12.3 Mixed evaluation

- Legacy, span first/middle/last, span-to-legacy, legacy-to-span, span-to-span, and mixed range roots.
- Unrelated dirty legacy and span branches remain pending after success.
- Pure legacy and mixed SCCs under every cycle policy.
- Cancellation before preparation, during discovery, before topology, between layers, in a large span,
  and during SCC handling.
- Exact delta parity for legacy cells and run/region parity for spans and spills.
- Repeated warm target evaluation scales with target volatiles, not workbook producers.
- Two `NOW()` cells prove one request epoch and out-of-closure staleness.
- Stale plan reasons cover graph, authority, staged, name/table, semantic/provider, and span generation.
- Post-lease identical events survive exact successful acknowledgement; cancellation acknowledges none.

### 12.4 SheetPort and bindings

- A1/name scalar, range, record, native table, and layout with terminator.
- Reachable staged chain consumption and unrelated retention.
- Dynamic widening, typed unsupported selectors, and typed layout exhaustion.
- Evaluation failure is never retried as a full evaluation.
- Batch stale rebuild policy, cancellation, and deterministic option restoration.
- Python and WASM error, delta, and session smoke tests.

## 13. Cold-Process Gates

The harness builds binaries and immutable fixtures before measurement, then spawns a fresh process for
each fixture, mode, budget set, target scope, and sample. It collects seven successful samples, median
and p95, an external hard timeout, current RSS/HWM or WASM linear-memory high water, fixture checksum,
and raw JSON. Run order is randomized. Load, prepare, topology, evaluation, and output read are reported
separately.

Fixtures cover 50k and 250k finance chains/rollups plus the largest CI-safe tier; targets touching less
than 1%, about 10%, and 100%; cross-sheet chains with unrelated staging; names/tables/layout; opaque
workbook widening; cache limits at zero/default/derived; no-disk/WASM; and true mixed cycles around the
materialization cap.

### 13.1 Structural gates, hard from C1b

- Under a shared envelope, authoritative completes every Off-completing case or both return the same
  typed error.
- Cache overflow causes zero capacity demotion and zero materialized cells absent a true SCC.
- Prepared and scheduled units are at most reachable oracle plus 1%; unrelated units stay staged/dirty.
- Sparse all-unset-budget regression stays within 5% median and 10% p95 of the C0 baseline.
- Observed peak scratch stays within ledger estimate plus 10%.
- Authoritative peak RSS remains inside envelope plus measured unaccounted baseline and 10%, and does
  not exceed Off RSS on the same fixture.

### 13.2 Ratified at C1 exit, revalidated at C6

The following begin as calibration targets, not safety gates: target-vs-full median/p95 ratios, warm
target scaling bounds, dynamic-widening allowance, and the 70%-of-Off RSS aspiration. C0 and C1 cold
data set absolute fixture-tier gates at C1 exit. C6 revalidates those exact values on the complete
matrix. A failed gate changes algorithm, accounting, or budgets; it never raises a safety or
materialization cap merely to make a benchmark pass.

## 14. Migration and Public Compatibility

- Existing evaluation signatures remain and delegate to typed targets.
- `prepare_graph_all` and `prepare_graph_for_sheets` remain compatibility APIs. Documentation states
  that only target preparation proves transitive completeness. Sheet preparation can be deprecated
  after loaders and SheetPort migrate.
- Each explicit budget field wins over only its conflicting legacy mapping. Non-conflicting legacy
  destinations still map, and one diagnostic reports every mapped/ignored field. Legacy fields are
  removed only in a semver-breaking release.
- `WorkbookLoadLimits` stays load-facing; evaluation materialization moves behind evaluation budgets
  with compatibility accessors.
- Off and Shadow preserve parser, replay, cached-value, source-order, and diagnostics behavior.
- Error-kind strings and additive delta versions are audited across Rust, Python, WASM, serialized
  diagnostics, and SheetPort before release.
- SheetPort exposes the same output values without silent error fallback; widening is telemetry-visible.

## 15. Residual Risks and Follow-On Work

- True mixed SCCs may exceed materialization limits. Exact preflight returns a deterministic typed
  error naming affected spans/families and the cap. Admission does not attempt to certify all future
  edit-created cycles.
- Transaction-local parsing was rejected in favor of explicit inert residue. Arena/interner growth
  remains observable memory and must be accounted, bounded, and kept semantically invisible.
- Repeated-pass no-disk topology can be slow. Work-unit bounds preserve availability symmetry; skip
  streak telemetry tells operators when to raise retained memory.
- Whole-family C3 selection may prepare more formulas than an ideal residual package design. Residual
  splitting is post-C5 and requires a separate adversarial replay-order review.
- Cancellation initially recomputes already flushed layers. Exact cancellation-time sublease
  acknowledgement may follow after successful partial acknowledgement is proven.
- Delta run/region adoption requires callers to consume the additive version; bounded compatibility
  expansion remains available during migration.
- Recommended named constructors/default sets and target latency gates remain deferred until the
  C1-exit calibration artifact; future recommendations will be plain budget-producing helpers, never
  enums or modes.
- Adaptive node punchouts, cycle refinement, coalescing, columnar execution, and wavefront execution
  remain governed by the long-term adaptive partition design. This cutover supplies their target and
  resource contract but does not implement those executors.

## 16. Primary Implementation Areas

- `formualizer-eval`: budgets, ledger, typed errors, target preparation/evaluation, recalc plans,
  dirty subleases, exact topology visitors, scheduler, region delta collection, and fault seams.
- `formualizer-workbook`: typed preparation/evaluation wrappers and compatibility adapters.
- `formualizer-sheetport`: target collection, revision-bound batch plans, selector limits, and removal
  of the fallback ladder.
- `formualizer-bench-core`: fresh-process finance/load probes, memory telemetry, and raw artifact
  emission.
- Python/WASM bindings: typed resource/stale/selector errors and additive delta records.
