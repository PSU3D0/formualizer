# FormulaPlane Scenario Corpus + Allocation Instrumentation Plan

Status: shared dispatch plan (multi-agent coordinated)
Date: 2026-05-04
Owner: PM (sequenced agent dispatch)

## Goal

Build a 30-50 scenario corpus of realistic workbook shapes that lets us
measure ingest-time and evaluate-time wall clock, allocation counts/bytes,
arena/graph/plane state, and Off vs Auth deltas, on a per-scenario basis.

The corpus is **measurement infrastructure**, not optimization. Optimizations
are dispatched separately, informed by the regression matrix this corpus
produces.

This document is the contract every dispatch follows. Agents must not deviate
from the shape laid out here without escalating to PM.

## Anti-goals — read these first

1. **NO optimization in this dispatch series.** This is corpus + harness +
   instrumentation. Do not touch evaluator, ingest pipeline, scheduler,
   range view, function impls, or any optimization-relevant code path.
2. **NO new abstractions in formualizer-eval.** All new code lives in a new
   bench/instrumentation crate and tests/binaries that consume the existing
   public API.
3. **NO renames or refactors of existing types.** The corpus exists to measure
   the system as-is.
4. **NO speculative scenarios.** Each scenario must correspond to a real
   workbook shape we have evidence (or strong expectation) of seeing in user
   workbooks. If a scenario can't be justified, drop it.
5. **NO #[ignore] or feature-flag-gated tests.** The harness is default-on.
6. **NO removal or weakening of existing tests or probes.**

## Architecture overview

Three new pieces of code:

```
crates/formualizer-bench-core/
  src/scenarios/
    mod.rs              -- ScenarioRegistry + Scenario trait
    common.rs           -- shared helpers (data generation, etc)
    s001_*.rs           -- one scenario per file (numbered)
    s002_*.rs
    ...
  src/instrumentation/
    mod.rs              -- AllocationCounter, PhaseMetrics, Reporter
    dhat.rs             -- dhat-rs integration (feature-gated)
  src/bin/
    probe-corpus.rs     -- runs the corpus, emits JSON/CSV
```

The scaffold + first 5 scenarios are dispatched first. Subsequent agents add
scenarios in groups of 12 each. The PM reviews after the first 5 to validate
the contract is correct, then again at the end.

## Crate setup constraints

```text
1. Place all new code under crates/formualizer-bench-core. Do NOT add a new
   crate. Add module hierarchy under existing src/.
2. Existing probe binaries (probe-fp-scenarios, probe-finance-recalc, etc)
   must remain unchanged and continue to work.
3. The corpus harness must compile under `cargo build -p formualizer-bench-core
   --features formualizer_runner`. The new feature flag `dhat-heap` is OPTIONAL
   and gates allocation tracking only.
4. Use `umya-spreadsheet` directly through `formualizer-testkit::write_workbook`
   (and the lower-level helpers in xlsx.rs) for fixture generation. Do NOT
   add a different XLSX writer.
5. Use `Workbook::from_reader(UmyaAdapter::open_path(...), ...)` for loading,
   matching probe-finance-recalc.
6. Each scenario must be deterministic given the same input parameters.
```

## Scenario trait

Every scenario implements:

```rust
pub trait Scenario: Send + Sync {
    /// Stable, immutable identifier. Format: "sNNN-name", e.g. "s003-multi-column-family".
    fn id(&self) -> &'static str;

    /// One-line human description.
    fn description(&self) -> &'static str;

    /// Categorical tag set. Use predefined tags from ScenarioTag enum.
    fn tags(&self) -> &'static [ScenarioTag];

    /// Build a workbook fixture to disk. Idempotent for given (path, params).
    /// Returns the path written (caller may pass a desired path or accept default).
    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture, anyhow::Error>;

    /// Optional: provide an EditPlan for edit-cycle measurement. None = no edits measured.
    fn edit_plan(&self) -> Option<EditPlan> { None }

    /// Expected result invariants checked after each phase. Optional but encouraged.
    /// E.g. "rollup at Sheet1!Z1 must equal expected_rollup" or "no error cells".
    fn invariants(&self, _phase: ScenarioPhase) -> Vec<ScenarioInvariant> { Vec::new() }
}

// NOTE (post-dispatch-1): the trait was implemented as above. Because
// invariants() takes only `phase` and not the scale or fixture metadata,
// scenarios with scale-dependent invariants (most of them) need to remember
// the scale that was last used to build them. Dispatch 1 implemented this
// via per-scenario `ScaleState` plus a process-global mutex set by the
// runner before invariant evaluation (`scenarios::common::ScaleState` and
// `set_invariant_scale`). Subsequent dispatches MUST use the same
// `ScaleState` + `set_invariant_scale` pattern; do not introduce a parallel
// mechanism. If your scenario does not need scale in invariants, ignore.
// Dispatch 4's PM review may consider folding this back into the trait;
// dispatches 2-3 must NOT change the trait surface.

pub struct ScenarioBuildCtx {
    /// Target scale parameter: "small" / "medium" / "large".
    /// Each scenario maps these to its own row/col counts.
    pub scale: ScenarioScale,
    /// Where to put the .xlsx fixture.
    pub fixture_dir: std::path::PathBuf,
    /// Workbook label (for fixture filename).
    pub label: String,
}

pub enum ScenarioScale { Small, Medium, Large }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScenarioTag {
    /// Categories
    NoFormulas,
    SingleColumnFamily,
    MultiColumnFamily,
    AnchoredArithmetic,
    LongChain,
    InternalDependency,
    LookupHeavy,
    AggregationHeavy,
    Mixed,
    MultiSheet,
    StructuredRefs,
    NamedRanges,
    Volatile,
    Dynamic,
    LetLambda,
    EmptyGaps,
    MixedTypes,
    ErrorPropagation,
    WholeColumnRefs,
    LargeArrayLiteral,

    /// Edit shapes
    SingleCellEdit,
    BulkEdit,
    InsertRows,
    DeleteRows,
    InsertColumns,
    DeleteColumns,
    SheetRename,
    UndoRedo,

    /// Engine paths
    SpanPromotable,
    LegacyOnly,
    CrossSheet,
}

pub struct ScenarioFixture {
    pub path: std::path::PathBuf,
    /// Workbook-level facts known at build time, used for invariant checks
    /// and reporter output (NOT for runtime decisions).
    pub metadata: FixtureMetadata,
}

pub struct FixtureMetadata {
    pub rows: u32,
    pub cols: u32,
    pub sheets: usize,
    pub formula_cells: u32,
    pub value_cells: u32,
    pub has_named_ranges: bool,
    pub has_tables: bool,
}

pub struct EditPlan {
    /// Number of edit/recalc cycles to run.
    pub cycles: usize,
    /// Function called once per cycle. Mutates the workbook in place.
    /// Returns a label for the edit kind ("dense_units", "sparse_prices", etc).
    pub apply: fn(&mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error>,
}

pub enum ScenarioPhase {
    AfterLoad,
    AfterFirstEval,
    AfterEdit { cycle: usize, kind: &'static str },
    AfterRecalc { cycle: usize, kind: &'static str },
}

pub enum ScenarioInvariant {
    CellEquals { sheet: String, row: u32, col: u32, expected: LiteralValue },
    NoErrorCells { sheet: String },
}
```

## Instrumentation — what to capture

For each scenario × mode (Off, Auth) × scale, capture the following per phase:

### Phases

```text
phase_fixture_gen   -- workbook fixture (re)built (only if not reused)
phase_load          -- read XLSX from disk + ingest into Workbook/Engine
phase_first_eval    -- evaluate_all() initial pass
phase_edit_n        -- one apply() call (per cycle)
phase_recalc_n      -- evaluate_all() after edit
```

### Per-phase metrics

```text
wall_ms                    f64
cpu_ms                     f64                       (optional; fallback to wall)
rss_start_mb               f64                       (linux /proc/self/status)
rss_end_mb                 f64
rss_peak_phase_mb          f64                       (best-effort)
allocs_count               u64                       (dhat, when feature enabled)
allocs_bytes               u64                       (dhat)
allocs_max_bytes           u64                       (dhat: max heap during phase)
arena_node_count           u64                       (engine introspection)
arena_node_bytes           u64                       (engine introspection)
graph_vertex_count         u64
graph_edge_count           u64
graph_name_count           u64
plane_span_count           u64
plane_template_count       u64
plane_active_span_cells    u64
computed_overlay_cells     u64                       (after flush)
delta_overlay_cells        u64
fragments_emitted          u64                       (since phase start)
```

### Per-scenario summary

```text
scenario_id                String
mode                       Off | Auth
scale                      Small | Medium | Large
fixture_path               String
fixture_size_bytes         u64
phases                     Vec<PhaseReport>
final_invariants_passed    bool
notes                      Vec<String>               (optional warnings)
```

### Engine introspection

To populate arena/graph/plane counters, the harness uses existing engine
public/pub(crate) accessors. **If a needed accessor doesn't exist, escalate
to PM** — do not add new accessors in this dispatch series.

Likely-needed accessors (verify each exists; if not, downgrade to None
gracefully and note it):

```text
engine.graph().vertex_count()        -> usize
engine.graph().edge_count()          -> usize
engine.graph().name_count()          -> usize
engine.graph().data_store().arena_node_count() -> usize
engine.formula_authority().plane().span_count() -> usize
engine.formula_authority().plane().template_count() -> usize
engine.arrow_sheets()                -> &SheetStore (existing)
```

If these accessors don't exist publicly, the corpus reports `None` for the
missing fields. Do NOT add accessors or visibility relaxations as part of
this dispatch — that's a follow-up the PM coordinates separately.

### dhat-rs integration

```text
1. Add `dhat = "0.3"` to formualizer-bench-core dev-dependencies (or main
   deps if needed under a feature flag). NOT to formualizer-eval.
2. Gate behind a new feature flag `dhat-heap`.
3. When the feature is enabled, use dhat::Profiler::new_heap() to wrap
   each phase and capture per-phase counters via dhat's API, then drop
   to flush to a per-phase file.
4. When the feature is disabled, fall back to None for allocation fields.
   The harness must work either way.

Important: dhat replaces the global allocator. We CAN'T have dhat-heap
on by default because it slows everything down. The harness must support
"run without dhat for fast iteration" AND "run with dhat for allocation
profiling" via the same code paths, gated only by the feature.
```

### Output

The harness writes one JSON file per (scenario, mode, scale) tuple, plus
one aggregate CSV / Markdown table at the end. Output directory:
`target/scenario-corpus/<run-label>/`.

Aggregate table columns include scenario id, all phase wall_ms, allocs_bytes,
peak RSS, plane span count, etc, with Auth/Off ratio columns highlighted.

## Scenario list — pre-allocated to dispatch agents

The following slot assignments are fixed. Each agent works only within its
assigned slots. Detailed scenario specs are below; agents flesh out the
implementation of the assigned scenarios, do NOT reassign slots.

### Dispatch 1 (scaffold + first 5)

```text
s001  no-formulas-static-grid              tag: NoFormulas
s002  single-column-trivial-family         tag: SingleColumnFamily, SpanPromotable
s003  finance-anchored-arithmetic-family   tag: AnchoredArithmetic, SpanPromotable
s004  five-mixed-families                  tag: MultiColumnFamily, SpanPromotable
s005  long-chain-family                    tag: LongChain, InternalDependency
```

These mirror existing probe-fp-scenarios shapes precisely so we have
direct continuity with that probe's numbers. Agent should start each
scenario by reading the corresponding probe-fp-scenarios case as the
implementation reference.

### Dispatch 2 (next 12)

```text
s006  rect-family-10cols                   tag: MultiColumnFamily, SpanPromotable
s007  fixed-anchor-family                  tag: AnchoredArithmetic, SpanPromotable
s008  two-anchored-families                tag: MultiColumnFamily, SpanPromotable
s009  heavy-arith-family                   tag: SingleColumnFamily, SpanPromotable
s010  all-unique-singletons                tag: LegacyOnly
s011  vlookup-family-against-1k-table      tag: LookupHeavy
s012  vlookup-family-against-10k-table     tag: LookupHeavy
s013  sumifs-family-constant-criteria      tag: AggregationHeavy
s014  sumifs-family-varying-criteria       tag: AggregationHeavy
s015  index-match-chain                    tag: LookupHeavy
s016  multi-sheet-5-tabs                   tag: MultiSheet, CrossSheet
s017  cross-sheet-references-in-family     tag: SpanPromotable, CrossSheet
```

### Dispatch 3 (next 12)

```text
s018  named-ranges-100                     tag: NamedRanges
s019  table-with-structured-refs           tag: StructuredRefs
s020  multi-table-cross-references         tag: StructuredRefs, CrossSheet
s021  volatile-functions-sprinkled         tag: Volatile
s022  dynamic-functions-offset-indirect    tag: Dynamic
s023  empty-cell-gaps-in-family            tag: SingleColumnFamily, EmptyGaps
s024  mixed-text-and-number-columns        tag: MixedTypes
s025  errors-propagating-through-family    tag: SingleColumnFamily, ErrorPropagation
s026  whole-column-refs-in-50k-formulas    tag: WholeColumnRefs, SpanPromotable
s027  large-array-literals                 tag: LargeArrayLiteral
s028  let-lambda-formulas                  tag: LetLambda
s029  calc-tab-200-complex-cells           tag: Mixed
s030  calc-and-data-tabs-mixed             tag: Mixed, MultiSheet
```

### Dispatch 4 (next 12 — edit-pattern variants)

```text
s031  finance-anchored-with-edit-cycles    tag: AnchoredArithmetic, SingleCellEdit, BulkEdit
s032  family-with-row-insert-cycles        tag: SingleColumnFamily, InsertRows
s033  family-with-row-delete-cycles        tag: SingleColumnFamily, DeleteRows
s034  family-with-column-insert            tag: MultiColumnFamily, InsertColumns
s035  family-with-column-delete            tag: MultiColumnFamily, DeleteColumns
s036  multi-sheet-with-sheet-rename        tag: MultiSheet, SheetRename
s037  named-range-update-cycles            tag: NamedRanges, BulkEdit
s038  bulk-edit-50-cells-per-cycle         tag: SingleColumnFamily, BulkEdit
s039  undo-redo-of-bulk-edit               tag: SingleColumnFamily, UndoRedo
s040  undo-redo-of-row-insert              tag: SingleColumnFamily, UndoRedo, InsertRows
s041  table-grow-by-row-append             tag: StructuredRefs, BulkEdit
s042  external-source-version-bump         tag: Mixed
```

### Final review (PM)

```text
- Run the full corpus once, collect baseline regression matrix.
- Identify any scenario that fails to build, fails invariants, or
  produces unexpected Off/Auth divergences.
- Open issues for follow-up; do not optimize in this dispatch.
```

## Scenario specifications

Each scenario must include:
- An ID string matching the slot assignment.
- A description.
- Tags from ScenarioTag.
- A `build_fixture` that uses umya-spreadsheet via formualizer-testkit
  helpers to write a deterministic .xlsx file.
- Mappings from ScenarioScale to row/col counts. Default mapping is:

```text
Small:   1k rows / minimal cols
Medium:  10k rows / typical cols
Large:   50k rows / typical cols
```

Each scenario's `build_fixture` MUST produce a fixture in <5 seconds at
Medium scale. If a scenario can't, downscale Medium or document the cost.

For full-scale spec details of s001-s005, see "Detailed Specs Below."

## Detailed scenario specs (s001-s005, dispatch 1)

### s001 no-formulas-static-grid

Pure value grid, no formulas. Baseline for arena/graph/plane state at zero
formulas. Tests that Off and Auth modes have similar overhead when there's
nothing to optimize.

```text
Scale Small:   1000 rows × 5 cols of f64 numbers
Scale Medium:  10000 rows × 10 cols
Scale Large:   50000 rows × 10 cols

Fixture pattern: row r col c = (r * 0.001) + c. Sheet "Sheet1".
Edit plan: 5 cycles. Cycle k edits cell (k+1, 1) to k as f64.
Invariant: no errors at any phase.
```

### s002 single-column-trivial-family

Single column of `=A{r}*2` formulas. The simplest span-promotable shape.
Mirrors probe-fp-scenarios "single-trivial-family".

```text
Scale Small:   1k rows
Scale Medium:  10k rows
Scale Large:   50k rows

Sheet Sheet1:
  Column A: row r = r as f64 (1.0 .. N)
  Column B: row r = "=A{r}*2"

Edit plan: 5 cycles.
  Cycle k edits A((k * 37) % rows + 1) to (1000.0 + k as f64).
Invariant: B{r} == A{r} * 2 at all rows.
Expected: family promotes to one span at column B in Auth mode.
```

### s003 finance-anchored-arithmetic-family

Finance-shape: `=A{r}*B{r}*$F$1` plus rollup `=SUM(C1:Cn)`. Mirrors
probe-finance-recalc shape. Has two distinct scheduling layers (the
column C family must finish before the rollup runs).

```text
Scale Small:   1k rows
Scale Medium:  10k rows
Scale Large:   50k rows

Sheet Sheet1:
  Column A: row r = r as f64 (units)
  Column B: row r = 10.0 + (r - 1) % 17 as f64 (prices)
  F1:       1.0 (multiplier)
  Column C: row r = "=A{r}*B{r}*$F$1"
  G1:       "=SUM(C1:Cn)" where n = total rows

Edit plan: 5 cycles, mirroring probe-finance-recalc:
  cycle 0: multiplier (F1) := 1 + (cycle % 5)
  cycle 1: dense_units (16 cells, A col)
  cycle 2: sparse_prices (16 random cells, B col)
  cycle 3: multiplier
  cycle 4: dense_units

Invariants per cycle:
  - G1 equals the deterministic rollup computed by the harness (use the
    same logic as probe-finance-recalc::expected_rollup)
  - C{r} == A{r} * B{r} * F1 at all rows

Expected: column C family promotes; G1 is a Legacy producer in layer 2.
```

### s004 five-mixed-families

Five distinct family shapes side by side. Tests scheduler handling of
heterogeneous family sets and cross-family read coordination.

```text
Scale Small:   1k rows
Scale Medium:  10k rows
Scale Large:   50k rows

Sheet Sheet1:
  Column A: f64 numbers (r as f64)
  Column B: =A{r}+1
  Column C: =A{r}*2
  Column D: =A{r}-3
  Column E: =A{r}/2 (avoid divide-by-zero by using r >= 1)
  Column F: =A{r}+B{r}

Edit plan: 5 cycles, each editing a random subset of column A.
Invariants: each column matches its derived formula.
Expected: 5 separate span families promote in Auth mode.
```

### s005 long-chain-family

Chain where each row reads the previous row. This is the
internal-dependency case rejected at family placement today.

```text
Scale Small:   1k rows
Scale Medium:  10k rows
Scale Large:   50k rows

Sheet Sheet1:
  Column A: row 1 = 1.0; row r > 1 = "=A{r-1}+1"

Edit plan: 5 cycles, each edits A1 to a different value.
Invariants: A{r} == A1 + (r - 1) at all rows.
Expected: family is REJECTED at placement (InternalDependency).
  All formulas remain Legacy producers. Mode comparison shows graph
  cost is comparable Off vs Auth.
```

(Remaining scenarios specified in dispatch 2-4 contracts.)

## Build commands

```bash
# Compile the harness binary
cargo build -p formualizer-bench-core --features formualizer_runner --release \
    --bin probe-corpus

# Run the corpus (writes JSON + CSV under target/scenario-corpus/<label>/)
./target/release/probe-corpus \
    --label baseline-2026-05-04 \
    --scale medium \
    --modes off,auth \
    --include 's001-*,s002-*,...'

# Run with allocation profiling (slow)
cargo build -p formualizer-bench-core \
    --features formualizer_runner,dhat-heap --release --bin probe-corpus
./target/release/probe-corpus --label baseline-with-dhat ...
```

## Validation per dispatch

Each agent must verify before reporting:

```text
1. cargo fmt --all -- --check
2. cargo clippy -p formualizer-bench-core --all-targets -- -D warnings
3. cargo build -p formualizer-bench-core --features formualizer_runner --release \
       --bin probe-corpus
4. ./target/release/probe-corpus --label dispatch-N-validation --scale small \
       --modes off,auth --include 's001-*,...'
   must complete without panics on every assigned scenario
5. cargo test -p formualizer-eval --quiet         (no regression)
6. cargo test -p formualizer-workbook --quiet     (no regression)
7. cargo test --workspace --quiet                  (no regression)
8. fp8_ingest_pipeline_parity passes
```

## Reporting per dispatch

The agent's output report must include:

1. Diff stats per file.
2. List of new files added with one-line description of each.
3. List of scenarios implemented, with:
   a. Scale parameters (rows × cols at each scale).
   b. Whether edit_plan implemented.
   c. Whether invariants implemented.
4. Output of `probe-corpus --scale small --modes off,auth --include s00X,...`
   for the scenarios in the dispatch. This is the smoke test.
5. Any deviations from this plan, with justification.
6. Any engine introspection accessors that don't exist; mark them in the
   report so PM can coordinate adding them.
7. Worktree status (`git status --short`). DO NOT commit.
