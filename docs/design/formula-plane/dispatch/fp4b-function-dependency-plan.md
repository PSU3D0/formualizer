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

FP4.B uses **function-owned, opt-in dependency contracts**.

The dependency semantics for a function belong on the `Function` abstraction,
not in a FormulaPlane sidecar name table and not in a separate registration side
map. The core trait gains an additive default method:

```rust
fn dependency_contract(&self, arity: usize) -> Option<FunctionDependencyContract> {
    None
}
```

Default `None` is the conservative production contract: no dependency-summary
optimization, no materialization reduction, and no behavior change. Functions opt
in only when their implementation can describe dependency roles for the supplied
arity without under-approximating dependencies.

Rationale:

- Function identity, aliases, namespaces, and Excel compatibility prefixes are
  already owned by `function_registry`; FormulaPlane must not duplicate that
  normalization path.
- Dependency roles are function semantics and should be colocated with each
  `impl Function` as functions are brought into FormulaPlane support.
- Existing `FnCaps` are useful coarse facts (`VOLATILE`, `REDUCTION`, `LOOKUP`,
  `RETURNS_REFERENCE`, `DYNAMIC_DEPENDENCY`) but cannot encode per-argument
  roles like `SUMIFS(value_range, criteria_range, criteria_expr, ...)`.
- Existing `ArgSchema` is useful validation/evaluation metadata but is not a full
  dependency contract.

FP4.B should still consume `FnCaps`, `ArgSchema`, `RETURNS_REFERENCE` /
`eval_reference` capability, aliases, and registry identity as report-only
consistency checks where safely available. Those checks must never decide runtime
behavior or silently change whether a summary is supported.

The implementation must not introduce FormulaPlane-owned function-name lists.
FormulaPlane should resolve function calls through the standard registry path and
then ask the resolved `Function` for its opt-in dependency contract.

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

`Some(FunctionDependencyContract)` is a **contract-level possibility** for a
function/arity, not unconditional summary authority. The summary pass must still
reject unsupported argument shapes such as whole/open ranges, names, tables,
spills, arrays, dynamic references, unsupported nested functions, and malformed
criteria pairings.

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

`SUMIF` / `AVERAGEIF` target-range policy:

- omitted target range means the criteria range is also the value/average range;
- provided target ranges are supported only when they are finite and either have
  the same finite shape as the criteria range or can be represented by an
  explicitly documented Excel-style top-left expansion to the criteria-range
  dimensions;
- if that expansion is not implemented in the first FP4.B build slice, mismatched
  target shapes must reject explicitly rather than risk under-approximation.

Invalid arity/pairing should reject with explicit fallback reasons.

### 4.2 Conservative fallbacks in FP4.B

Functions without an opt-in dependency contract are conservative fallbacks unless
a later plan expands support. Diagnostics may still label broad categories using
registry facts such as `FnCaps` and function-owned metadata, but absence of a
contract means no FormulaPlane dependency-summary optimization.

| Category | Examples | FP4.B behavior |
|---|---|---|
| mask/lazy conditionals | `IF`, `IFERROR`, `IFNA`, `IFS`, `SWITCH` | no summary support unless a function-owned contract is added later |
| lookups | `VLOOKUP`, `HLOOKUP`, `XLOOKUP`, `MATCH` | no summary support unless a function-owned contract is added later |
| reference-returning | `INDEX`, `CHOOSE` | conservative fallback unless context-specific reference contracts are approved |
| dynamic dependency | `INDIRECT`, `OFFSET` | conservative fallback from `FnCaps::DYNAMIC_DEPENDENCY` / `RETURNS_REFERENCE` |
| volatile | `NOW`, `TODAY`, `RAND`, `RANDBETWEEN` | conservative fallback from `FnCaps::VOLATILE`; volatility pseudo-precedent deferred |
| local environment | `LET`, `LAMBDA` | conservative fallback until local-scope dependency contracts are approved |
| array/spill | `FILTER`, `SEQUENCE`, `SORT`, `SORTBY`, `UNIQUE`, `RANDARRAY`, `TEXTSPLIT` | conservative fallback until result-region/spill contracts are approved |
| registered builtin without contract | any builtin not opted in | `NoDependencyContract` / conservative fallback |
| unknown/custom function without contract | registry miss or custom no-opt-in | unknown/no-contract conservative fallback |

## 5. Proposed module and type shape

Add root-level function dependency metadata, not a FormulaPlane sidecar:

```text
crates/formualizer-eval/src/function_contract.rs
crates/formualizer-eval/src/function.rs
```

Expose the contract types from the core eval crate because `Function` implementors
need them for opt-in declarations. Names are illustrative but the implementation
should preserve these concepts:

```rust
pub enum FunctionDependencyClass {
    StaticScalarAllArgs,
    StaticReduction,
    CriteriaAggregation,
}

pub enum FunctionArgumentDependencyRole {
    ScalarValue,
    FiniteRangeValue,
    ReductionValue,
    CriteriaRange,
    CriteriaExpression,
    ValueRange,
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

pub enum FunctionArityRule {
    Exactly(usize),
    AtLeast(usize),
    OneOf(&'static [usize]),
    EvenAtLeast(usize),
    OddAtLeast(usize),
}

pub enum CriteriaValueRange {
    None,
    Fixed(usize),
    Optional { provided_index: usize, fallback_criteria_range_index: usize },
}

pub struct FunctionDependencyContract {
    pub class: FunctionDependencyClass,
    pub arity: FunctionArityRule,
    pub arguments: FunctionArgumentDependencyContract,
}
```

`Function` gets a defaulted opt-in method:

```rust
fn dependency_contract(&self, arity: usize) -> Option<FunctionDependencyContract> {
    None
}
```

Contracts should be declared beside the relevant `impl Function`, for example
`SUM` declares `StaticReduction`, `ABS` declares `StaticScalarAllArgs`, and
`COUNTIFS` declares `CriteriaAggregation` with even criteria-pair arity. Functions
without an explicit contract remain conservative and unsupported for FormulaPlane
summary optimization.

Drift/consistency checks, where added, compare the function-owned contract to
registry metadata (`FnCaps`, `ArgSchema`, aliases, arity, and `eval_reference`)
without changing runtime behavior. They must be safe when builtins are not loaded
and must not call `Function::arg_schema()` blindly: the default implementation
can panic for functions with `min_args() > 0` and no schema override.

## 6. Dependency-summary model changes

FP4.A has cell precedent patterns. FP4.B needs finite rectangular range patterns.
Use the existing concept already present in `dependency_summary.rs`:

```rust
pub(crate) enum PrecedentPattern {
    Cell(AffineCellPattern),
    Rect(AffineRectPattern),
}
```

`AffineRectPattern` exists today, but `PrecedentPattern::Rect(AffineRectPattern)`
still needs to be added/activated. FP4.B should activate that existing concept
instead of introducing a parallel rectangle type.

Supported finite range summaries must preserve:

- sheet binding;
- start/end row axes;
- start/end column axes;
- absolute/relative anchors per endpoint;
- value/reference/criteria context labels where relevant;
- fallback reasons for non-finite axes.

Run/reverse rule for rectangle precedents:

- If finite rectangle precedents can be conservatively mapped from changed
  precedent regions back to dependent run partitions, emit reverse counters with
  explicit overage.
- If they cannot be represented without misleading precision, demote the affected
  run summary with an explicit fallback reason such as
  `rect_reverse_mapping_unsupported`.
- Do not silently reuse cell-only inverse mapping for rectangle precedents.

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
planner finite cell dependency must be covered by summary cells or finite rects
planner finite range dependency must be covered by summary finite rects
matching requires the same resolved/display sheet binding
summary finite rect containment is inclusive over one-based row/col coordinates
summary may over-approximate only when explicitly counted
summary must never under-approximate
```

Do not expand large finite ranges into scalar cells for comparison. Compare
finite rectangles by containment and only use cell expansion in tiny unit tests
where it is explicitly bounded.

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

Use the nested placement under `dependency_summaries`:

```json
"dependency_summaries": {
  "function_dependency_taxonomy": { ... }
}
```

This preserves the FP4.A top-level scanner surface and keeps function taxonomy
coupled to passive dependency-summary reporting.

Minimum taxonomy fields:

```json
{
  "contract_version": "fp4b_function_dependency_v1",
  "counter_unit": "authority_template",
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

Default counter denominator is authority templates. If a template contains
multiple function calls, the most severe outcome dominates template-level counts:
`Rejected` > `ClassifiedOnly` > `Supported`. Optional per-function-occurrence
counters may be added, but they must be named separately.

## 9. Phased implementation plan

### FP4.B.0 — Plan and review

Status: this document.

Tasks:

1. Commit this plan.
2. Dispatch two read-only or docs-writing reviewers.
3. Fold blocking review feedback before code.
4. Fold pass-with-nits review feedback that affects code-shape decisions before
   dispatching FP4.B.1 implementation.

Gate:

```bash
timeout 30s git status --short
```

Commit suggestion:

```text
docs(formula-plane): plan fp4b function dependency taxonomy
```

### FP4.B.1 — Function-owned contract model and opt-ins

Goal: add root-level function dependency contract types and a defaulted
`Function::dependency_contract(...)` opt-in without changing summary behavior.

Deliverables:

- `crates/formualizer-eval/src/function_contract.rs`
- additive default method on `Function`
- deletion of the provisional FormulaPlane sidecar classifier
- concrete contract variants for all-args scalar functions, variadic reductions,
  and criteria pairs
- tests proving default/no-opt-in functions return `None` and remain
  conservative
- initial colocated opt-ins only for the selected FP4.B supported functions

Required tests:

| Function / behavior | Expected contract behavior |
|---|---|
| no opt-in function | `dependency_contract(_) == None` |
| `ABS` | `StaticScalarAllArgs` for arity 1, `None` otherwise |
| `SUM` | `StaticReduction`, including runtime-supported zero-arity |
| `AVERAGE` | `None` for arity 0, `StaticReduction` for valid arities |
| `COUNTIFS` | `CriteriaAggregation` for even arity >= 2, `None` for malformed pairing |
| `SUMIF` / `AVERAGEIF` | optional value range contract for arity 2 or 3 |
| `SUMIFS` / `AVERAGEIFS` | fixed value range at arg 0 and odd arity >= 3 |

Gate:

```bash
timeout 10m cargo fmt --all -- --check
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
```

### FP4.B.2 — Registry resolution and summary contract unification

Goal: eliminate duplicated function classification logic while using
registry-resolved, function-owned contracts.

Tasks:

- Update `template_canonical.rs` to resolve calls through `function_registry`
  instead of duplicating function-name/prefix logic.
- Use resolved `Function` metadata (`FnCaps`, `eval_reference`, and
  `dependency_contract(args.len())`) for passive labels and explicit conservative
  fallbacks.
- Keep authority template keys stable where semantics are unchanged; if key
  payload changes are unavoidable, document that FP4.A artifacts were generated
  at `6b527c9` and FP4.B emits a new diagnostic version.
- Update `dependency_summary.rs` to consume the resolved function-owned contract
  rather than ad-hoc function-name rejection.

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
- Add the finite-rect reverse mapping/demotion rule from §6.
- Extend fixed-policy planner comparison to match planner finite ranges by
  region containment, not large scalar expansion.
- Keep open/whole-axis ranges rejected.

Tests:

- `=SUM(A1:A10)` exact range match;
- copied `=SUM(A1:A10)` down/up with relative endpoints preserves affine ranges;
- `=$A$1:A10` mixed endpoints preserved;
- planner cells covered by summary rects;
- planner rects covered by summary rects;
- over-approximation counted for larger summary rects;
- finite rect reverse mapping either conservative with overage counters or demoted
  with explicit fallback;
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
- Implement or explicitly reject `SUMIF` / `AVERAGEIF` provided target-range
  top-left expansion; omitted target range uses the criteria range.
- Reject malformed pairings and unsupported shapes explicitly.

Tests:

- `=COUNTIF(A1:A10,">0")` exact range match;
- `=COUNTIFS(A1:A10,">0",B1:B10,C1)` includes both ranges and criteria cell;
- `=COUNTIFS(A1:A10,">"&C1)` includes criteria-expression cell dependency;
- `=SUMIF(A1:A10,">0")` uses criteria range as value range;
- `=SUMIF(A1:A10,">0",B1:B10)` includes provided target range;
- `=AVERAGEIF(A1:A10,">0")` and `=AVERAGEIFS(C1:C10,A1:A10,">0")` are classified/supported;
- `=SUMIFS(C1:C10,A1:A10,B1)` includes sum range, criteria range, criteria cell;
- invalid arity rejects with `InvalidArity`;
- malformed criteria pairing rejects with `InvalidCriteriaPairing`;
- whole/open-axis ranges reject;
- no under-approximation against planner.

### FP4.B.6 — Scanner taxonomy integration

Goal: expose passive function dependency taxonomy in scanner JSON.

Tasks:

- Extend the existing `formula_plane_diagnostics` bridge narrowly.
- Preserve FP4.A `dependency_summaries` fields.
- Emit nested `dependency_summaries.function_dependency_taxonomy` counters and
  fallback histogram using authority-template counters by default.
- Ensure ambiguous diagnostic-source template mappings remain fallback, not silent
  authority mappings.
- Add serialization coverage for taxonomy version, counters, and fallback keys.

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
- function-owned dependency contracts conflict with registry metadata or runtime
  function behavior and cannot be resolved conservatively;
- support for a function would require eval semantics, span kernels, loader
  changes, or public API changes;
- whole/open-axis ranges require used-region tracking not available in this phase;
- `under_approximation_count` becomes nonzero for any supported fixture;
- scanner JSON integration would break FP1-FP4.A sections;
- registry initialization or `arg_schema()` access would make diagnostics panic
  or depend on runtime initialization order.

## 11. Expected FP4.B status statement

If all gates pass, the correct closeout wording is:

```text
FP4.B PASS: selected builtins expose function-owned opt-in dependency contracts,
finite static reductions and finite criteria aggregations are summarized where
bounded, scanner reporting exposes supported/conservative function-contract
outcomes, and comparison against current dependency planning shows no
under-approximation for supported fixtures. No graph/runtime/materialization
authority changed.
```

Do not claim load, memory, full-eval, or incremental-recalc wins from FP4.B.
