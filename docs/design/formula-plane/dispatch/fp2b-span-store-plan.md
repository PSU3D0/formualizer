# FP2.B passive span-store plan

Date: 2026-04-30  
Branch: `formula-plane/bridge`  
Base commit: `3891d8e` (`docs(formula-plane): record fp2 span counter baseline`)

## 1. Decision summary

FP2.B will add a passive, in-memory FormulaPlane span-store builder under `crates/formualizer-eval/src/formula_plane/`. It will consume the same parser-backed candidate cells/runs used by FP2.A and produce deterministic `FormulaTemplateArena` / `FormulaRunStore` structures that describe template families, placements, holes, exceptions, unsupported formulas, and simple shape classes.

The store is descriptive only. It must not become execution authority and must not change loader, graph, scheduler, dependency, recalc, or public API behavior.

Non-goals:

- No scheduler behavior, task partitioning authority, dirty propagation, or formula evaluation.
- No dependency graph bypass or dependency graph mutation.
- No loader behavior change, shared-formula preservation change, or workbook compatibility policy change.
- No public API, serialization contract, CLI contract, or stable cross-crate `formualizer-common` type movement.
- No Core+Overlay Session/no-legacy integration and no continuation of paused Phase 9.Q.* work.
- No benchmark claims beyond representation/accounting checks.

## 2. Inputs and outputs

Inputs:

- `FormulaPlaneCandidateCell` records from FP2.A: `sheet`, `row`, `col`, parser-backed `template_id`, and `parse_ok` / `volatile` / `dynamic` / `unsupported` labels.
- Optional `SpanPartitionCounterOptions` or a small sibling options struct for row-block size and future scanner reporting knobs.
- Candidate runs computed with the FP2.A row/column run algorithm, either reused internally or rebuilt by the store builder from candidate cells.

Outputs:

- `FormulaTemplateArena`: deterministic template table keyed by internal `FormulaTemplateId`, carrying source parser template identity and aggregate counts.
- `FormulaRunStore`: deterministic workbook-level run/placement table, carrying shape, placement span, template reference, row-block partition summary, holes, exceptions, and singleton/unsupported records.
- Optional validation/report struct, for example `FormulaRunStoreBuildReport`, with FP2.A-compatible counters and explicit delta explanations.
- Unit-test-only/debug accessors that let tests inspect IDs, ordering, shape classes, holes, exceptions, and rejected formulas without exposing public API.

The builder returns all output in memory. It does not write files, mutate workbooks, or register runtime state.

## 3. Proposed Rust structures and placement

Module placement remains experimental and local to eval:

```text
crates/formualizer-eval/src/formula_plane/
  mod.rs
  span_counters.rs          # existing FP2.A counters
  span_store.rs             # new FP2.B passive arena/store/builder
```

`mod.rs` should add and re-export the new module only inside `formualizer-eval`. Do not move these primitives to `formualizer-common`.

Proposed structures:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FormulaTemplateId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FormulaRunId(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FormulaPlacementId(pub u32);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaTemplateDescriptor {
    pub id: FormulaTemplateId,
    pub source_template_id: String,
    pub formula_cell_count: u64,
    pub status: TemplateSupportStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemplateSupportStatus {
    Supported,
    ParseError,
    Unsupported,
    Dynamic,
    Volatile,
    Mixed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormulaRunShape {
    Row,
    Column,
    Singleton,
    RectangleDeferred,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaPlacement {
    pub id: FormulaPlacementId,
    pub template_id: FormulaTemplateId,
    pub sheet: String,
    pub row_start: u32,
    pub row_end: u32,
    pub col_start: u32,
    pub col_end: u32,
    pub shape: FormulaRunShape,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaRunDescriptor {
    pub id: FormulaRunId,
    pub placement_id: FormulaPlacementId,
    pub template_id: FormulaTemplateId,
    pub shape: FormulaRunShape,
    pub len: u64,
    pub row_block_start: u32,
    pub row_block_end: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanGapDescriptor {
    pub template_id: FormulaTemplateId,
    pub sheet: String,
    pub row: u32,
    pub col: u32,
    pub kind: SpanGapKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanGapKind {
    Hole,
    Exception { other_template_id: FormulaTemplateId },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaRejectedCell {
    pub sheet: String,
    pub row: u32,
    pub col: u32,
    pub source_template_id: String,
    pub reason: FormulaRejectReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormulaRejectReason {
    ParseError,
    Unsupported,
    Dynamic,
    Volatile,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaTemplateArena {
    pub templates: Vec<FormulaTemplateDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaRunStoreBuildReport {
    pub template_count: u64,
    pub formula_cell_count: u64,
    pub supported_formula_cell_count: u64,
    pub rejected_formula_cell_count: u64,
    pub row_run_count: u64,
    pub column_run_count: u64,
    pub singleton_run_count: u64,
    pub hole_count: u64,
    pub exception_count: u64,
    pub overlap_dropped_count: u64,
    pub rectangle_deferred_count: u64,
    pub gap_scan_truncated_count: u64,
    pub reconciliation: Fp2aReconciliation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fp2aReconciliation {
    pub matched: bool,
    pub deltas: Vec<Fp2aCounterDelta>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fp2aCounterDelta {
    pub field: &'static str,
    pub fp2a_value: i64,
    pub span_store_value: i64,
    pub reason: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormulaRunStore {
    pub row_block_size: u32,
    pub arena: FormulaTemplateArena,
    pub placements: Vec<FormulaPlacement>,
    pub runs: Vec<FormulaRunDescriptor>,
    pub gaps: Vec<SpanGapDescriptor>,
    pub rejected_cells: Vec<FormulaRejectedCell>,
    pub report: FormulaRunStoreBuildReport,
}
```

The final implementation may reduce field visibility or use getters if that matches local style, but tests need deterministic introspection. `FormulaPlacementId` can be omitted if the first implementation has a one-to-one run/placement model; if kept, IDs must still be deterministic.

## 4. Deterministic ID policy

Template IDs:

- Sort unique `source_template_id` values lexicographically using `BTreeMap`/`BTreeSet` semantics.
- Assign `FormulaTemplateId(0..n-1)` after sorting, independent of input order.
- Preserve the source parser template string in every template descriptor for scanner/report correlation.
- Include rejected-only templates in the arena so unsupported/dynamic/volatile accounting is stable and explainable.

Run IDs:

- Build candidate row/column/singleton runs from normalized sorted cells, never from input order.
- Sort run descriptors by `(template_id, sheet, shape_order, row_start, col_start, row_end, col_end)`.
- Use shape order `Row`, `Column`, `Singleton`, `RectangleDeferred` or another explicitly documented total order; keep it fixed in tests.
- Assign `FormulaRunId(0..n-1)` after final sorting.

Placement IDs:

- If placements are separate from runs, sort placements by `(template_id, sheet, shape_order, row_start, col_start, row_end, col_end)` and assign `FormulaPlacementId(0..n-1)` after sorting.
- A `FormulaRunDescriptor` references the placement produced from the same sorted key, not a position discovered during scanning.
- If omitted for FP2.B, document that placement identity is temporarily equal to run identity and reserve separate IDs for FP3 reporting.

Shuffled-input determinism:

- All builder tests must run the same candidate set in original order, reversed order, and a fixed shuffled order.
- The produced arena, runs, placements, gaps, rejected cells, and report must compare equal byte-for-byte through `PartialEq` or stable debug snapshots.
- Avoid hash maps unless they are only transient and followed by explicit total sorting; prefer `BTreeMap` and `BTreeSet`.

## 5. Shape policy

Column runs:

- A column run is a contiguous sequence of at least two supported cells with the same template, same sheet, same column, and consecutive rows.
- Store as `shape = Column`, `row_start..row_end`, fixed `col_start == col_end`.
- Row-block summary uses the FP2.A row-block policy: `row_block_index(row) = (row - 1) / row_block_size.max(1)`.

Row runs:

- A row run is a contiguous sequence of at least two supported cells with the same template, same sheet, same row, and consecutive columns.
- Store as `shape = Row`, fixed `row_start == row_end`, `col_start..col_end`.
- Row-block summary usually has one row block because the placement touches one row.

Singleton formulas:

- A supported formula cell not represented by any accepted row or column run is stored as a `Singleton` placement/run with `len = 1`.
- Singletons are not errors. They are required to make store formula-cell coverage reconcile with FP2.A counters.
- Rejected unsupported/dynamic/volatile/parse-error cells are not represented as singleton runs; they live in `rejected_cells`.

Holes:

- A hole is an empty cell inside the min/max span of a same-template axis group.
- Store holes as `SpanGapDescriptor { kind: Hole }` with the template and exact coordinate.
- Holes do not create placements and must not extend a run across the missing cell.

Exceptions:

- An exception is a formula cell with a different template inside the min/max span of a same-template axis group.
- Store exceptions as `SpanGapDescriptor { kind: Exception { other_template_id } }` with both template identities resolved to deterministic arena IDs.
- Exceptions do not create merged runs; each side of the exception remains a separate run or singleton.

Rectangles:

- Defer first-class rectangle runs in FP2.B.
- Rationale: FP2.A currently reports row and column runs, the synthetic corpus is dominated by vertical fill-down families, and rectangle orientation can double-count cells unless a precedence rule is designed carefully.
- FP2.B should detect dense same-template rectangles only enough to classify/report them as `RectangleDeferred` candidates or a `rectangle_deferred_count` in the build report; it should not store rectangle runs as executable placements.
- Rectangle acceptance belongs in a later phase after FP3 reporting shows real corpus prevalence and after FP5 dependency summaries define safe region semantics.

Overlap policy:

- A supported formula cell may be eligible for both a row run and a column run. FP2.B must avoid double representation in `FormulaRunStore` coverage.
- Use a deterministic precedence: prefer the longer run; break ties by shape order; then by sorted run key.
- Record any dropped alternative as a report-only overlap count if useful, but do not store two authoritative placements for the same cell.

## 6. Unsupported, dynamic, and volatile handling

- A candidate cell with `parse_ok == false` is rejected with `ParseError` regardless of other flags.
- Else if `unsupported == true`, reject with `Unsupported`.
- Else if `dynamic == true`, reject with `Dynamic`.
- Else if `volatile == true`, reject with `Volatile`.
- Rejected cells are retained in `rejected_cells` with deterministic ordering by `(sheet, row, col, source_template_id, reason)`.
- Rejected cells are included in arena/template aggregate counts but excluded from supported run construction.
- Rejected cells can still appear as exceptions when they sit inside a supported template span; the gap descriptor should identify the other template and the report should also count the rejection reason.
- Do not evaluate volatility or dynamic behavior; rely only on scanner-provided flags.

## 7. Relationship to FP2.A counters

FP2.B must include a reconciliation path against FP2.A counters for the same input cells.

Expected matches:

- `template_count` and `formula_cell_count` match exactly.
- Parse-error, unsupported, dynamic, and volatile counts match exactly.
- Hole and exception counts match unless FP2.B documents a deliberate supported-only filtering delta.
- Row-block partition counts and run-to-partition edge estimates match for accepted non-overlapping row/column runs.

Expected deltas to explain:

- `candidate_formula_run_count`, row-run count, column-run count, and represented-cell count can differ if FP2.B applies overlap de-duplication while FP2.A counted both axes diagnostically.
- Singleton count can differ if FP2.B excludes rejected cells from singleton runs while FP2.A treated all unrepresented formula cells as singleton formula count.
- Future `rectangle_deferred_count` has no FP2.A equivalent and must be reported separately.

The build report should include a compact `Fp2aReconciliation` section with `matched: bool` and per-field deltas. For FP2.B unit tests, every delta must be either zero or named in an allow-list with a short reason string.

## 8. Unit test matrix

Minimum tests in `span_store.rs`:

| Test | Input shape | Assertions |
|---|---|---|
| deterministic_template_ids | Same cells with templates `b`, `a`, `c` in shuffled orders | Arena IDs are `a=0`, `b=1`, `c=2` every time |
| deterministic_run_ids_for_shuffled_input | One vertical run, one horizontal run, one singleton in three input orders | Equal store output and stable run IDs |
| column_run_basic | Same template in one column over consecutive rows | One `Column` run, correct row/col bounds, row-block range, no gaps |
| row_run_basic | Same template in one row over consecutive columns | One `Row` run, correct bounds, single row-block range |
| singleton_supported_cell | One supported formula | One `Singleton` run and placement, no rejection |
| hole_splits_run | Rows 1, 2, 4, 5 in same column/template | Two column runs and one hole at row 3 |
| exception_splits_run | Template A rows 1, 2, 4 with template B at row 3 | A has two runs/singletons as appropriate and one exception pointing to B |
| rejected_parse_error | One parse-error formula | No run, one rejected cell, parse-error report count matches FP2.A |
| rejected_unsupported_dynamic_volatile_order | Cells with multiple flags | Reason precedence is parse-error, unsupported, dynamic, volatile |
| rejected_inside_supported_span | Supported A rows 1 and 3, rejected B row 2 | Rejection retained and A records an exception, not a hole |
| overlap_dedup_longer_run_wins | Cross shape where center belongs to row and column candidates | One representation per cell, deterministic dropped-overlap count |
| rectangle_deferred | Dense 2x3 same-template block | No rectangle execution run; report records rectangle-deferred candidate or deterministic row/column decomposition |
| fp2a_reconciliation_dense_vertical | FP2.A-style 10-row vertical run with row block size 4 | Store report matches FP2.A run/partition counts |
| row_block_size_normalization | Row block size 0 | Normalized to 1 and deterministic partitions |

Tests should avoid file IO and should run as ordinary `cargo test -p formualizer-eval` unit tests.

## 9. Validation commands

Use strict timeouts and no benchmarks:

```bash
timeout 30s git status --short
timeout 30s git log -1 --oneline
timeout 30s rg "FormulaRunStore|FormulaTemplateArena|span_store" crates/formualizer-eval/src/formula_plane docs/design/formula-plane/dispatch
timeout 2m cargo fmt --all -- --check
timeout 2m cargo test -p formualizer-eval formula_plane --quiet
```

If the targeted test filter misses tests because names change, run only the bounded eval crate test suite with a strict timeout:

```bash
timeout 2m cargo test -p formualizer-eval --quiet
```

Do not run benchmarks for FP2.B acceptance.

## 10. Acceptance criteria

- `span_store.rs` exists under `crates/formualizer-eval/src/formula_plane/` and is re-exported by the local module tree only.
- Builder accepts FP2.A candidate cells and returns an in-memory store without side effects.
- Deterministic template, placement, and run IDs are proven by shuffled-input tests.
- Supported column runs, row runs, singletons, holes, exceptions, and rejection reasons are represented with exact coordinates.
- Rectangles are explicitly deferred or report-only; they are not silently promoted into executable run authority.
- FP2.A reconciliation report exists and either matches counters or explains every intentional delta.
- No loader, scheduler, evaluator, dependency graph, public API, or Core+Overlay code path changes.
- Validation commands pass within the specified timeouts.

## 11. Future phase mapping

- FP3 passive store reporting / scanner integration: expose `FormulaRunStoreBuildReport` in scanner JSON next to `formula_plane_candidates`, with no runtime behavior change.
- FP4 loader/shared-formula hints: compare loader-preserved shared-formula metadata to parser-derived template/run IDs and report preservation gaps; still passive.
- FP5 dependency summaries: add precedent/result summary descriptors per run for compatibility checks, not scheduling authority.
- FP6 compatibility/materialization gates: introduce opt-in gates that decide when materialization avoidance is safe, with fallback/circuit breakers.
- FP7 first narrow span executor: only then route a small supported subset through span execution and scheduler logic behind explicit gates.

## 12. When real wins arrive

FP2.B and FP3 can produce representation wins: fewer descriptors for dense formula families, deterministic IDs, better scanner reports, and clearer accounting of holes/exceptions/rejections. These are not user-visible performance wins by themselves.

Load and memory wins arrive later, after FP4-FP6 preserve or infer shared-formula/span hints early enough to avoid per-cell materialization and after compatibility gates prove safe fallback behavior.

Eval and recalc wins arrive only with FP7 or later, when a narrow executor and scheduler can use run/dependency summaries to avoid per-cell graph/evaluation work. FP2.B must not claim those wins.

## 13. Risks and circuit breakers

Risks:

- Axis overlap can double-count cells or produce unstable IDs if precedence is underspecified.
- Rectangle eagerness can create accidental execution semantics before dependency summaries exist.
- Rejected cells can disappear from accounting if support status is mixed into run construction too early.
- FP2.A counter drift can hide semantic changes unless every delta is named.
- Large dense inputs can make gap enumeration expensive if implemented with broad min/max scans over sparse ranges.

Circuit breakers:

- Keep all FP2.B code passive and unreachable from production load/eval paths except unit tests and later scanner-only reporting.
- Add a builder option or hard cap for gap enumeration in pathological sparse spans; when exceeded, record a conservative `gap_scan_truncated_count` and avoid huge allocations.
- Require deterministic sort keys before assigning IDs; tests fail if shuffled inputs differ.
- Reject unsupported/dynamic/volatile/parse-error cells from run construction by default.
- Preserve a simple fallback: if store build fails or truncates, scanner/reporting can emit FP2.A counters only and production behavior remains unchanged.
