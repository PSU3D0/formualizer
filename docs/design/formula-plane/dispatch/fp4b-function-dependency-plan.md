# FP4.B Implementation Plan — Passive Function Dependency Taxonomy

Date: 2026-05-02  
Branch: `formula-plane/bridge`  
Base before plan: `14309a5 docs(formula-plane): clarify fp4a report artifacts`

## 1. Purpose

FP4.B extends the FP4.A passive dependency-summary pipeline from a narrow
`StaticPointwise` subset into a **passive function dependency taxonomy**. The
phase answers, for selected functions and argument shapes:

```text
Can FormulaPlane describe this function's dependencies safely and boundedly?
If yes, what finite cell/range dependency summary should be emitted?
If no, what explicit fallback/reject reason explains why?
```

FP4.B is a dependency-classification and reporting phase only. It must not route
execution, mutate graph behavior, change dirty propagation, reduce
materialization, change loader behavior, or change public/default APIs.

The target passive pipeline after FP4.B is:

```text
parsed formula AST
  -> authority-grade canonical template key       # FP4.A
  -> function dependency contract classification # FP4.B
  -> dependency template summary                 # cells + finite ranges where supported
  -> run-instantiated summary                    # FP4.A machinery extended as needed
  -> fixed-policy planner comparison             # no under-approximation
  -> scanner JSON/reporting
```

## 2. Core design decision

FP4.B uses a **FormulaPlane-local function dependency contract registry**.

It does **not** modify the core `Function` trait in this phase, and it does not
try to encode the taxonomy purely as `FnCaps` bitflags.

Rationale:

- `Function` is core runtime/evaluation machinery; FP4.B is an experimental
  passive FormulaPlane phase.
- The registry stores functions as `Arc<dyn Function>`, so associated constants
  are not a good fit for rich dependency contracts.
- Existing `FnCaps` are useful coarse facts (`VOLATILE`, `REDUCTION`, `LOOKUP`,
  `RETURNS_REFERENCE`, `DYNAMIC_DEPENDENCY`) but cannot encode per-argument
  roles like `SUMIFS(value_range, criteria_range, criteria_expr, ...)`.
- Existing `ArgSchema` is useful validation/evaluation metadata but is not a full
  dependency contract.

FP4.B should consume `FnCaps`, `ArgSchema`, and registry identity as **inputs and
optional drift diagnostics**, not as the only source of dependency truth.

The implementation should centralize normalized function-name matching in one
FormulaPlane module. Scattered ad-hoc `if name == ...` checks across canonicalizer,
dependency summary, scanner, and future graph-hint code are out of scope.

## 3. Strict non-goals

FP4.B must not implement or change:

- scheduler behavior;
- formula evaluation;
- span execution kernels;
- dirty propagation authority;
- dependency graph bypass;
- graph materialization reduction;
- loader behavior;
- public API behavior;
- save/output behavior;
- Core+Overlay Session/no-legacy integration;
- FP5 graph-build hint integration;
- FP6 materialization reduction;
- runtime performance-win claims.

All outputs remain passive diagnostics, tests, and scanner/report data.

## 4. Supported and classified scope

FP4.B has two levels of recognition:

1. **Classified**: the function is recognized and assigned a dependency class,
   but may still reject/fallback for this phase.
2. **Supported summary**: the function emits a dependency summary and participates
   in no-under-approximation comparison.

### 4.1 Supported summary classes for FP4.B

FP4.B should support these dependency summaries when all references are finite
and static:

#### Static scalar all-args

Initial supported functions:

```text
ABS
NOT
ISBLANK
ISERR
ISERROR
ISLOGICAL
ISNA
ISNONTEXT
ISNUMBER
ISTEXT
N
T
```

Dependency rule:

```text
summary dependencies = union of supported child dependencies
```

No span eval semantics are implied.

#### Static finite reductions

Initial supported functions:

```text
SUM
COUNT
COUNTA
MIN
MAX
AVERAGE
PRODUCT
```

Supported argument shape:

- finite cell references;
- finite rectangular range references;
- literals;
- nested supported scalar expressions if dependency union is exact/conservative.

Rejected argument shape:

- open-ended ranges;
- whole-row/whole-column ranges;
- names;
- tables/structured refs;
- 3D/external references;
- dynamic refs;
- spills/implicit intersection;
- arrays unless explicitly handled as literals with no dependencies.

Dependency rule:

```text
summary dependencies = union of finite cell/range dependencies in all args
```

#### Criteria aggregations over finite ranges

Initial classified and supported for finite static shapes:

```text
COUNTIF
COUNTIFS
SUMIF
SUMIFS
AVERAGEIF
AVERAGEIFS
```

Dependency rule:

- include all finite criteria ranges;
- include finite sum/average/value ranges where present;
- include dependencies of criteria expressions when criteria arguments reference
  cells/ranges;
- literals/criteria strings add no cell dependency.

Shape constraints:

- `COUNTIF(criteria_range, criteria)`;
- `COUNTIFS(criteria_range1, criteria1, ...)` with range/criteria pairs;
- `SUMIF(criteria_range, criteria, [sum_range])`;
- `SUMIFS(sum_range, criteria_range1, criteria1, ...)`;
- `AVERAGEIF(criteria_range, criteria, [average_range])`;
- `AVERAGEIFS(average_range, criteria_range1, criteria1, ...)`.

Invalid arity/pairing should reject with explicit fallback reasons.

### 4.2 Classified but not supported in FP4.B

These functions/classes should be recognized and explicitly rejected unless a
later plan expands support:

| Class | Examples | FP4.B status |
|---|---|---|
| `MaskConditional` | `IF`, `IFERROR`, `IFNA`, `IFS`, `SWITCH` | classified, rejected/deferred |
| `LookupStaticRange` | `VLOOKUP`, `HLOOKUP`, `XLOOKUP`, `MATCH` | classified, rejected/deferred |
| `ReferenceReturning` | `INDEX`, `CHOOSE` | rejected unless a later context-specific contract is approved |
| `DynamicDependency` | `INDIRECT`, `OFFSET` | rejected |
| `Volatile` | `NOW`, `TODAY`, `RAND`, `RANDBETWEEN` | rejected or volatility pseudo-precedent deferred |
| `LocalEnvironment` | `LET`, `LAMBDA` | rejected |
| `ArrayOrSpill` | `FILTER`, `SEQUENCE`, `SORT`, `SORTBY`, `UNIQUE`, `RANDARRAY`, `TEXTSPLIT` | rejected |
| `OpaqueScalar` | unknown/custom functions | rejected |

## 5. Proposed module and type shape

Add a FormulaPlane-local module:

```text
crates/formualizer-eval/src/formula_plane/function_dependency.rs
```

Wire it crate-internally from:

```text
crates/formualizer-eval/src/formula_plane/mod.rs
```

Names are illustrative but the implementation should preserve these concepts:

```rust
pub(crate) enum FunctionDependencyClass {
    StaticScalarAllArgs,
    StaticReduction,
    CriteriaAggregation,
    MaskConditional,
    LookupStaticRange,
    DynamicDependency,
    Volatile,
    ReferenceReturning,
    LocalEnvironment,
    ArrayOrSpill,
    OpaqueScalar,
    Unsupported,
}

pub(crate) enum FunctionSupportStatus {
    Supported,
    ClassifiedOnly,
    Rejected,
}

pub(crate) enum ArgumentDependencyRole {
    ScalarValue,
    FiniteRangeValue,
    CriteriaRange,
    CriteriaExpression,
    ReductionValue,
    LazyBranch,
    LookupKey,
    LookupTable,
    LookupResultSelector,
    ByReference,
    LocalBindingName,
    LocalBindingValue,
    LambdaBody,
    IgnoredLiteral,
    Unsupported,
}

pub(crate) enum FunctionDependencyRejectReason {
    UnknownFunction,
    DynamicDependency,
    VolatileFunction,
    ReferenceReturningFunction,
    LocalEnvironmentFunction,
    ArrayOrSpillFunction,
    UnsupportedFunctionClass,
    InvalidArity,
    InvalidCriteriaPairing,
    UnsupportedArgumentRole,
    FunctionContractDrift,
}

pub(crate) struct FunctionDependencyContract {
    pub(crate) canonical_name: String,
    pub(crate) class: FunctionDependencyClass,
    pub(crate) support_status: FunctionSupportStatus,
    pub(crate) arg_roles: FunctionArgContract,
    pub(crate) reject_reasons: Vec<FunctionDependencyRejectReason>,
}
```

The central classifier should be the only place that maps normalized function
names to FormulaPlane dependency contracts:

```rust
pub(crate) fn dependency_contract_for_function(
    canonical_name: &str,
    arity: usize,
) -> FunctionDependencyContract;
```

A later helper may optionally consult existing registry metadata:

```rust
pub(crate) fn dependency_contract_drift(
    contract: &FunctionDependencyContract,
    caps: FnCaps,
    schema: &[ArgSchema],
) -> Vec<FunctionContractDrift>;
```

Drift checks are report-only and must not change runtime behavior.

## 6. Dependency-summary model changes

FP4.A has cell precedent patterns. FP4.B needs finite rectangular range patterns.
Use the existing concept already present in `dependency_summary.rs`:

```rust
pub(crate) enum PrecedentPattern {
    Cell(AffineCellPattern),
    Rect(AffineRectPattern),
}
```

If `AffineRectPattern` already exists but is unused, FP4.B should activate it
instead of introducing a parallel rectangle type.

Supported finite range summaries must preserve:

- sheet binding;
- start/end row axes;
- start/end column axes;
- absolute/relative anchors per endpoint;
- value/reference/criteria context labels where relevant;
- fallback reasons for non-finite axes.

## 7. Planner comparison extension

FP4.A comparison supports finite cells. FP4.B must extend the comparison universe
for supported finite ranges.

The fixed policy remains:

```rust
CollectPolicy {
    expand_small_ranges: false,
    range_expansion_limit: 0,
    include_names: true,
}
```

Comparison rules:

```text
planner finite cell dependency must be covered by summary cells or ranges
planner finite range dependency must be covered by summary finite ranges
summary may over-approximate only when explicitly counted
summary must never under-approximate
```

Whole-axis/open/names/tables/external/3D/dynamic/volatile categories remain
fallback/reject unless explicitly supported by a later plan.

Required comparison counters remain:

```text
exact_match_count
over_approximation_count
under_approximation_count
rejection_count
policy_drift_count
fallback_reason_histogram
```

Hard gate:

```text
under_approximation_count == 0
```

## 8. Scanner/report output

FP4.B should preserve all existing scanner sections and extend diagnostics without
breaking FP4.A output.

Existing sections that must remain:

```text
totals
formula_plane_candidates
formula_run_store
authority_templates
dependency_summaries
materialization_accounting
templates
```

Add one of the following, with reviewers deciding preferred placement:

Option A, nested under `dependency_summaries`:

```json
"dependency_summaries": {
  "function_dependency_taxonomy": { ... }
}
```

Option B, top-level:

```json
"function_dependency_taxonomy": { ... }
```

Minimum taxonomy fields:

```json
{
  "contract_version": "fp4b_function_dependency_v1",
  "classified_template_count": 0,
  "supported_function_template_count": 0,
  "rejected_function_template_count": 0,
  "static_scalar_template_count": 0,
  "static_reduction_template_count": 0,
  "criteria_aggregation_template_count": 0,
  "classified_only_template_count": 0,
  "dynamic_dependency_template_count": 0,
  "volatile_template_count": 0,
  "reference_returning_template_count": 0,
  "unknown_function_template_count": 0,
  "fallback_reasons": {}
}
```

## 9. Phased implementation plan

### FP4.B.0 — Plan and review

Status: this document.

Tasks:

1. Commit this plan.
2. Dispatch two read-only or docs-writing reviewers.
3. Fold blocking review feedback before code.

Gate:

```bash
timeout 30s git status --short
```

Commit suggestion:

```text
docs(formula-plane): plan fp4b function dependency taxonomy
```

### FP4.B.1 — Contract model and central classifier

Goal: add FormulaPlane-local function dependency contracts without changing
summary behavior.

Deliverables:

- `crates/formualizer-eval/src/formula_plane/function_dependency.rs`
- crate-internal module wire-up
- tests for classification and arity/pairing decisions

Required tests:

| Function | Expected class/status |
|---|---|
| `ABS` | `StaticScalarAllArgs` / supported |
| `SUM` | `StaticReduction` / supported for finite args |
| `COUNTIFS` | `CriteriaAggregation` / supported for valid finite pairs |
| `IF` | `MaskConditional` / classified-only or rejected |
| `VLOOKUP` | `LookupStaticRange` / classified-only or rejected |
| `INDIRECT` | `DynamicDependency` / rejected |
| `INDEX` | `ReferenceReturning` / rejected |
| `RAND` | `Volatile` / rejected |
| `LET` | `LocalEnvironment` / rejected |
| `FILTER` | `ArrayOrSpill` / rejected |
| `CUSTOMFN` | `OpaqueScalar` / rejected |

Gate:

```bash
timeout 10m cargo fmt --all -- --check
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
```

### FP4.B.2 — Canonicalizer and summary classifier unification

Goal: eliminate duplicated FormulaPlane-local function classification logic.

Tasks:

- Update `template_canonical.rs` to ask the central classifier for dynamic,
  volatile, reference-returning, local-env, array/spill, and unknown/custom
  labels.
- Keep authority template keys stable where semantics are unchanged; if key
  payload changes are unavoidable, document that FP4.A artifacts were generated
  at `6b527c9` and FP4.B emits a new diagnostic version.
- Update `dependency_summary.rs` to use the same contracts rather than ad-hoc
  function rejection.

Gate:

```bash
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
```

### FP4.B.3 — Static scalar function dependencies

Goal: support dependency summaries through selected scalar functions.

Supported functions:

```text
ABS, NOT, ISBLANK, ISERR, ISERROR, ISLOGICAL, ISNA, ISNONTEXT,
ISNUMBER, ISTEXT, N, T
```

Rule:

```text
supported if all arguments are supported scalar/cell expressions;
dependencies are the union of child dependencies.
```

Tests:

- `=ABS(A1)` exact match;
- `=NOT(A1>0)` exact match;
- `=ISNUMBER(A1)` exact match;
- unsupported nested function rejects explicitly;
- no under-approximation against planner.

### FP4.B.4 — Static finite reductions

Goal: support finite range reductions.

Supported functions:

```text
SUM, COUNT, COUNTA, MIN, MAX, AVERAGE, PRODUCT
```

Tasks:

- Activate `PrecedentPattern::Rect(AffineRectPattern)`.
- Add finite-range dependency summaries.
- Extend run instantiation for finite rect patterns where feasible.
- Extend fixed-policy planner comparison to match planner finite ranges.
- Keep open/whole-axis ranges rejected.

Tests:

- `=SUM(A1:A10)` exact range match;
- copied `=SUM(A1:A10)` down/up with relative endpoints preserves affine ranges;
- `=$A$1:A10` mixed endpoints preserved;
- `=SUM(A:A)` whole-axis fallback;
- `=SUM(A1:A)` open-range fallback;
- under-approximation detection for missing range.

### FP4.B.5 — Criteria aggregation dependencies

Goal: support bounded dependency summaries for finite criteria aggregations.

Supported functions:

```text
COUNTIF, COUNTIFS, SUMIF, SUMIFS, AVERAGEIF, AVERAGEIFS
```

Tasks:

- Encode arity/pairing rules in `FunctionDependencyContract`.
- Include finite criteria ranges and value ranges as range dependencies.
- Include criteria expression cell dependencies when criteria args reference cells.
- Reject malformed pairings and unsupported shapes explicitly.

Tests:

- `=COUNTIF(A1:A10,">0")` exact range match;
- `=COUNTIFS(A1:A10,">0",B1:B10,C1)` includes both ranges and criteria cell;
- `=SUMIFS(C1:C10,A1:A10,B1)` includes sum range, criteria range, criteria cell;
- `=AVERAGEIFS(C1:C10,A1:A10,">0")` classified/supported;
- invalid arity rejects;
- whole/open-axis ranges reject;
- no under-approximation against planner.

### FP4.B.6 — Scanner taxonomy integration

Goal: expose passive function dependency taxonomy in scanner JSON.

Tasks:

- Extend the existing `formula_plane_diagnostics` bridge narrowly.
- Preserve FP4.A `dependency_summaries` fields.
- Emit taxonomy counters and fallback histogram.
- Ensure ambiguous diagnostic-source template mappings remain fallback, not silent
  authority mappings.

Gate:

```bash
timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet
```

### FP4.B.7 — Bounded baseline and closeout report

Goal: record evidence and status.

Create:

```text
docs/design/formula-plane/dispatch/fp4b-function-dependency-taxonomy-report.md
```

Suggested artifacts:

```text
target/fp4b-function-dependency-taxonomy/$(git rev-parse --short HEAD)
```

Run the same six FP4.A scenarios if feasible:

```text
headline_100k_single_edit
chain_100k
fanout_100k
inc_cross_sheet_mesh_3x25k
agg_countifs_multi_criteria_100k
agg_mixed_rollup_grid_2k_reports
```

Report must include:

- supported/rejected function template counts;
- static reduction and criteria aggregation coverage;
- exact/over/under/rejection comparison counts;
- top fallback reasons;
- aggregate `under_approximation_count_total`;
- explicit no-runtime-authority/no-runtime-win statement;
- next risks before FP5.

Validation gate:

```bash
timeout 10m cargo fmt --all -- --check
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet
```

## 10. Stop conditions

Stop and report instead of broadening if:

- finite range summaries cannot be compared to planner output without graph
  behavior changes;
- criteria aggregation shape semantics become ambiguous enough to risk
  under-approximation;
- function registry metadata conflicts with FormulaPlane contracts and no clear
  report-only drift policy exists;
- support for a function would require eval semantics, span kernels, loader
  changes, or public API changes;
- whole/open-axis ranges require used-region tracking not available in this phase;
- `under_approximation_count` becomes nonzero for any supported fixture;
- scanner JSON integration would break FP1-FP4.A sections.

## 11. Expected FP4.B status statement

If all gates pass, the correct closeout wording is:

```text
FP4.B PASS: passive function dependency taxonomy exists for selected builtins,
finite static reductions and finite criteria aggregations are classified and
summarized where bounded, scanner reporting exposes supported/rejected function
contracts, and comparison against current dependency planning shows no
under-approximation for supported fixtures. No graph/runtime/materialization
authority changed.
```

Do not claim load, memory, full-eval, or incremental-recalc wins from FP4.B.
