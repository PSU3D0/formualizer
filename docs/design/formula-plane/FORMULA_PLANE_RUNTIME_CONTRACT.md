# FormulaPlane Runtime Contract

Status: **initial design draft for FP4.0 review**  
Branch: `formula-plane/bridge`  
Date: 2026-04-30

## 1. Purpose

FP1–FP3 established passive observability and representation:

```text
formula cells
  -> parser-backed template candidates
  -> passive span/partition counters
  -> FormulaTemplateArena / FormulaRunStore
  -> materialization accounting vs current graph AST/vertex/edge counts
```

This document defines the runtime contract required before FormulaPlane can move
from passive reporting into graph-build hints, dependency-summary authority, or
span evaluation. It is intentionally broader than a single formula optimization:
FormulaPlane should classify arbitrary formulas by semantic contracts, promote
only safe local structures, and keep compatibility fallback explicit and counted.

This is a design contract, not a public API. All types remain experimental and
local to `formualizer-eval` unless a later stable-contract decision promotes
some subset elsewhere.

## 2. Current posture

The current public/default engine still uses `DependencyGraph` authority.
FormulaPlane FP2.B/FP3 is passive and does not change workbook loading,
dependency graph construction, dirty propagation, scheduling, evaluation,
public APIs, save/output behavior, or Core+Overlay Session/no-legacy behavior.

The performance opportunity exposed by FP3 is large in dense formula cases, but
it is still only an opportunity:

```text
current dense workbook path:
  one formula cell -> one AST root -> one graph formula vertex -> scalar edges

FormulaPlane target path:
  one formula family -> one template -> one/few runs -> dependency summaries
```

Actual load/eval wins require later phases to consume this representation.

## 3. Non-goals

FP4.0 and the immediate follow-up phases must not introduce:

- public/default behavior changes;
- hidden graph bypass;
- hidden fallback to legacy structures in paths that claim compact execution;
- hard dependency-graph amputation;
- scheduler or evaluator authority before passive summaries are validated;
- global workbook-size gates such as `if formula_count > N enable FormulaPlane`;
- promotion of FormulaPlane experimental types into `formualizer-common`;
- Core+Overlay Phase 9.Q continuation under the old production-closeout framing.

## 4. Core vocabulary

### 4.1 Formula family

A **formula family** is a set of formula cells whose **whole normalized formula
AST** is identical under the active stored-reference convention.

Example:

```excel
C2 = A2 + B2
C3 = A3 + B3
C4 = A4 + B4
```

Relative semantic template:

```text
R[0]C[-2] + R[0]C[-1]
```

This is one formula family over one column run.

Formula family identity is whole-formula identity. Two formulas that only share
a subexpression are not the same family.

### 4.2 Formula class

A **formula class** is a semantic contract category derived from the AST and
function contracts. It is not one textual formula shape.

A class describes:

- dependency footprint kind;
- output shape kind;
- evaluation strategy kind;
- function semantic requirements;
- self-dependency / recurrence shape;
- span-kernel availability.

Many formula families can share one class.

Examples:

| Class | Meaning |
|---|---|
| `StaticPointwise` | Scalar output, static affine precedent cells/ranges per output. |
| `StaticWindow` | Scalar output, bounded window over precedents. |
| `StaticReduction` | Scalar output, static range reduction. |
| `CriteriaAggregation` | `SUMIFS`/`COUNTIFS`-like static ranges plus criteria expressions. |
| `MaskConditional` | Short-circuit conditional semantics, usually `IF`-like. |
| `LookupStaticRange` | Lookup over static bounded ranges. |
| `IntraRunRecurrence` | Output depends on earlier/later cells in same run by static offset. |
| `DynamicDependency` | Dependency footprint depends on runtime values. |
| `Volatile` | Re-evaluates according to volatile semantics. |
| `OpaqueScalar` | Unknown/custom function or unsupported construct; compatibility path only. |

Classes are compiler/analyzer outputs. They are not hand-written cases for every
possible formula string.

### 4.3 Dependency summary

A dependency summary is a symbolic relation:

```text
precedent region(s) -> dependent formula run/partition -> result region
```

Correctness invariant:

```text
true_scalar_dependencies(run) ⊆ summarized_dependencies(run)
```

A summary may be exact or conservative. Over-approximation can cause extra dirty
work; under-approximation is a correctness bug. If a safe summary cannot be
proven, FormulaPlane must reject compact authority for that run and record a
fallback reason.

### 4.4 Span execution

Span execution computes a run or run partition as a batch/chunk rather than
invoking the scalar interpreter independently for every formula cell.

Full-workbook evaluation still has an O(output cells) lower bound when all
results must be produced. Span execution aims to reduce avoidable overhead:

- per-cell AST dispatch;
- per-cell graph traversal;
- per-cell function dispatch;
- poor cache locality;
- repeated scans/index work for copied formula families.

### 4.5 Compatibility materialization

Compatibility materialization creates legacy AST/graph/cell structures for APIs
or constructs that require them. It must be explicit and counted. A path may not
claim compact FormulaPlane authority while silently allocating one graph/AST/edge
set per dense formula cell.

## 5. Layered runtime architecture

The target architecture is layered:

```text
FormulaRunStore
  - templates
  - runs
  - holes/exceptions/rejections

DependencySummaryArena
  - per-template dependency patterns
  - per-run instantiated precedent/result summaries
  - row-block/partition summaries

SpanPlanArena
  - lowered span expression IR
  - function/operator execution plan
  - fallback reasons

RunScheduler
  - schedules value partitions and formula-run partitions
  - handles static recurrence classes and conservative cycles
  - delegates legacy SCC/cycle behavior when unsupported

SpanExecutor
  - evaluates run partitions
  - calls vectorized operators and function span kernels
  - writes formula result plane

CompatibilityMaterializer
  - materializes legacy graph/cell/AST structures when required
  - emits explicit counters and fallback reasons
```

FP4 should implement only passive dependency summary pieces first. Later phases
may integrate graph-build hints and eventually span execution.

## 6. Dependency-summary model

### 6.1 Template-level analysis

For each formula template, the analyzer traverses the normalized AST and emits:

```rust
struct FormulaDependencyTemplateSummary {
    template_id: FormulaTemplateId,
    status: DependencySummaryStatus,
    formula_class: FormulaClass,
    precedent_patterns: Vec<PrecedentPattern>,
    flags: DependencySummaryFlags,
    fallback_reasons: Vec<DependencyRejectReason>,
}
```

`PrecedentPattern` should express the relation between a formula placement and a
precedent region without enumerating each cell edge:

```rust
enum PrecedentPatternKind {
    RelativeCell,
    AbsoluteCell,
    RelativeRange,
    AbsoluteRange,
    StaticCrossSheetCell,
    StaticCrossSheetRange,
    WholeRowOrColumn,
    NamedRangeStatic,
    TableReferenceStatic,
    DynamicRuntimeReference,
    ExternalReference,
    Unsupported,
}
```

Initial FP4 implementation can support a conservative subset and classify the
rest as unsupported or dependency-summary-only. The model must still have reason
codes for every rejected construct.

### 6.2 Run-level instantiation

Template summaries become run summaries by applying a run placement:

```text
template dependency pattern + run placement -> precedent/result region summary
```

Example:

```excel
C1:C100000 = A_i + B_i
```

Template patterns:

```text
R[0]C[-2]
R[0]C[-1]
```

Run summary:

```text
result:     C1:C100000
precedents: A1:A100000, B1:B100000
```

Partition summary with 4096-row blocks:

```text
A rows 1..4096 -> C rows 1..4096
B rows 1..4096 -> C rows 1..4096
...
```

This is a graph, but it is a symbolic/partition graph rather than a scalar cell
edge graph.

### 6.3 Conservative branches

For conditionals:

```excel
=IF(A_i > 0, B_i, C_i)
```

Dependency summary should conservatively include all possible branch references:

```text
A_i, B_i, C_i
```

Evaluation must still preserve short-circuit semantics. Dependency summary and
evaluation strategy are related but separate contracts.

### 6.4 Dynamic references

For constructs such as runtime-resolved references, the analyzer must not invent
a static summary. Initial policy:

```text
DynamicDependency -> explicit fallback / legacy materialization
```

Future work may add bounded dynamic summaries, but only with explicit runtime
tracking and invalidation contracts.

### 6.5 Intra-run recurrence

A run may depend on itself by a static offset:

```excel
B2:B100000 = B_{i-1} + 1
```

This is one formula family, but not independent pointwise evaluation.

Dependency summary:

```text
same-run predecessor dependency: row_offset = -1
```

Scheduling class:

```text
IntraRunRecurrence
```

Initial policy may be dependency-summary-only or legacy fallback. A later span
executor can evaluate supported recurrences by sequential scan or specialized
prefix algorithms. Unsupported cycles remain legacy/fallback authority.

## 7. Function contracts

The existing scalar `Function` trait remains the compatibility path. It supports
scalar formula evaluation and already gives functions access to `RangeView` and
capability flags.

FormulaPlane needs additional optional contracts. The contract should live in
`formualizer-eval`, likely under `formula_plane/function_contract.rs`, and
builtins can implement/register span contracts near their existing scalar
implementations.

Conceptual contract:

```rust
trait FormulaPlaneFunctionContract {
    fn dependency_contract(&self, args: &[TemplateExpr]) -> DependencyContract;
    fn span_caps(&self) -> SpanFunctionCaps;
    fn compile_span_call(
        &self,
        args: &[SpanExpr],
        ctx: &SpanCompileContext,
    ) -> Result<SpanOp, SpanRejectReason>;
    fn eval_span(
        &self,
        op: &SpanOp,
        inputs: &SpanInputs,
        ctx: &mut SpanEvalContext,
        out: &mut SpanOutput,
    ) -> Result<(), ExcelError>;
}
```

This is illustrative; FP4 should first define passive dependency contracts and
classification before requiring eval kernels.

### 7.1 Contract categories

Each function should eventually classify as one or more of:

| Category | Dependency contract | Span eval contract |
|---|---|---|
| Pure scalar/operator | Union child dependencies | Generic vector/chunk operator possible. |
| Reduction | Static range dependencies | Reduction kernel over `RangeView` chunks. |
| Criteria aggregation | Static target/criteria ranges and criteria expressions | Specialized multi-output kernel. |
| Short-circuit conditional | Conservative union of all branches | Mask-aware lazy branch evaluation required. |
| Lookup | Static lookup/table ranges when bounded | Specialized lookup/index kernel. |
| Reference-returning | Depends on reference result semantics | Initially fallback unless explicitly supported. |
| Dynamic dependency | Runtime-derived dependencies | Fallback initially. |
| Volatile | Static deps plus volatile scheduling policy | Usually fallback or explicit volatile span policy. |
| Opaque/custom | Unknown | Scalar/legacy fallback unless contract provided. |

### 7.2 `IF` example

Dependency summary:

```text
deps(IF(cond, then, else)) = deps(cond) ∪ deps(then) ∪ deps(else)
```

Span evaluation cannot eagerly evaluate both branches if doing so would expose
errors or side effects that scalar Excel semantics would suppress. A span IF
kernel must:

```text
1. evaluate condition mask;
2. evaluate true branch only for true lanes;
3. evaluate false branch only for false lanes;
4. merge results preserving scalar semantics.
```

### 7.3 `SUMIFS` example

Scalar execution sees one formula call at a time. A copied-report run exposes a
larger opportunity:

```text
same sum range
same criteria ranges
vector of criteria values from output-row-relative refs
many output rows
```

A span `SUMIFS` kernel can reuse masks, group repeated criteria, stream fact
ranges once per chunk, or build indexes. This is beyond graph-build avoidance and
is required for full-eval wins in report/fact-table workloads.

## 8. Span IR

FormulaPlane should lower normalized AST templates into an internal span IR
before execution authority is attempted.

Illustrative shape:

```rust
enum SpanExpr {
    Literal(LiteralValue),
    RelativeCellRef { row_offset: i32, col_offset: i32 },
    AbsoluteCellRef { sheet: SheetBinding, row: u32, col: u32 },
    RelativeRangeRef { row_offset: i32, col_offset: i32, rows: u32, cols: u32 },
    AbsoluteRangeRef { sheet: SheetBinding, range: Rect },
    Unary { op: OpId, expr: ExprId },
    Binary { op: OpId, left: ExprId, right: ExprId },
    FunctionCall { function: FunctionId, args: Vec<ExprId> },
    Conditional { cond: ExprId, then_expr: ExprId, else_expr: ExprId },
    ScalarFallbackSubtree { reason: SpanRejectReason },
}
```

Span IR is not a new public formula syntax. It is a compile target for template
analysis, dependency summaries, span planning, and eventual batch evaluation.

Sub-AST reuse belongs at this layer, not in whole-formula family identity.

## 9. Sub-AST and expression-template policy

Formula family identity remains whole normalized formula identity.

Example:

```excel
Rows 1..50000:      D_i = A_i + B_i + C_i
Rows 50001..100000: D_i = A_i + B_i
```

Representation:

```text
Template T0 = A_i + B_i + C_i, Run R0 = D1:D50000
Template T1 = A_i + B_i,       Run R1 = D50001:D100000
```

These are not exceptions. They are two clean families/runs.

A future expression-template arena may discover:

```text
SubExpr S0 = A_i + B_i
T0 = S0 + C_i
T1 = S0
```

This can reduce dependency analysis, AST storage, or span execution cost, but it
must not blur output formula identity or incorrectly merge different formulas
into one run.

Sparse localized deviations are represented as exceptions/singletons/gaps;
systematic regional differences are represented as separate templates/runs.

## 10. Materialization policy

FormulaPlane must use per-template/per-run policy, not global workbook-size
enablement.

Conceptual policy:

```rust
enum MaterializationPolicy {
    EagerLegacy,
    TemplateMetadataOnly,
    SharedTemplateAst,
    DependencySummaryOnly,
    SummaryEdgesSidecar,
    SpanExecutable,
    LazyMaterializeOnDemand,
}
```

Every policy assignment carries reason codes and counters:

```text
run_id
policy
eligible_cells
materialized_cells
skipped_ast_roots_estimate
skipped_formula_vertices_estimate
skipped_scalar_edges_estimate
fallback_reason[]
```

Initial runtime-facing phases should prefer:

1. hint-only policy reporting;
2. shared-template AST experiments;
3. summary-edge sidecars;
4. lazy/eager graph materialization reduction;
5. span execution authority.

The design must not jump directly from passive representation to broad graph
bypass.

## 11. Small-workbook overhead contract

FormulaPlane targets dense and large workbooks, but small workbooks must not pay
large duplicated overhead.

Avoid:

```text
parse once for graph + scan again for FormulaPlane
always build full FormulaRunStore
always deeply classify every AST
always build dependency summaries
always run through span executor wrappers
always build scalar graph plus full compact graph
```

Required posture:

```text
parse/stage once
  -> emit legacy graph inputs
  -> opportunistically emit cheap template fingerprints/counters
```

Promotion is local and lazy:

```text
Tier 0: template fingerprint/count bookkeeping
Tier 1: run detection for repeated templates
Tier 2: dependency summaries for promising static runs
Tier 3: span plans for eligible runs when evaluation needs them
Tier 4: span execution authority only for supported contracts
```

The decision is not `workbook_formula_count > N`. It is based on local structure
and cost:

```text
unique template -> likely metadata only
short run -> metadata or shared AST only
long dense static run -> dependency summary candidate
supported function kernel + evaluation demand -> span plan candidate
unsupported/dynamic/volatile -> explicit fallback
```

Small-workbook gates should include 10, 100, 1k, and 5k formula cases with
mostly unique formulas, small dense copied blocks, and mixed unsupported shapes.

## 12. Scheduling contract

FormulaPlane scheduling should operate on run partitions, not scalar graph cells,
where dependency summaries prove that is safe.

Node examples:

```text
ValuePartition(sheet, row_block, col_range)
FormulaRunPartition(run_id, row_block)
CompatibilityMaterializationTask(run_id or cell range)
```

Edges are summary relations:

```text
precedent partition(s) -> formula run partition -> result partition
```

Correctness requirement:

```text
all required precedent result partitions are complete before a dependent formula
partition executes, unless an accepted recurrence/cycle policy applies.
```

Unsupported cycles and unsupported recurrences must remain legacy/fallback until
FormulaPlane has explicit semantics and oracle coverage.

## 13. Evaluation contract

For any FormulaPlane-authoritative run/partition:

1. Placement coverage is exact: holes/exceptions/rejections are excluded or
   explicitly represented.
2. Every cell in the run uses the recorded normalized template.
3. Dependency summary is exact or conservative.
4. Scheduler respects summary edges and recurrence policy.
5. Span evaluation result equals scalar interpreter result for every output cell.
6. Unsupported constructs fall back explicitly and are counted.

Initial span-authority phases must use oracle comparison against current engine
on bounded fixtures before claiming runtime wins.

## 14. Phase recommendations after FP4.0

### FP4.A — Passive dependency-template summaries

Implement scanner/report-only dependency summaries:

```text
template AST -> dependency template summary -> run-instantiated summaries
```

Emit unsupported reason counts and partition-edge estimates. No graph behavior
change.

### FP4.B — Function contract taxonomy

Add passive function classification tied to current builtin registry/caps plus
FormulaPlane-specific reject reasons. Do not require span kernels yet.

### FP4.C — Small-workbook overhead gates

Add bounded small-workbook cases and instrumentation to prove metadata overhead
is negligible and deeper planning is local/lazy.

### FP5 — Graph-build hint integration

Feed run/dependency summary metadata into ingest/graph build as hint-only data.
Graph still materializes normally. Report what would have been skipped and why
runs did or did not promote.

### FP6 — First materialization reduction

Prefer lower-risk reductions first, such as shared template AST or sidecar
summary edges, before broad compact graph authority.

### FP7 — First span executor

Add narrow, contract-driven span execution with oracle coverage. Candidate first
kernels: pointwise arithmetic/comparison, mask-aware `IF`, and criteria
aggregation. Fallback remains explicit.

## 15. Open questions for review

1. Is the formula-family vs formula-class distinction precise enough for future
   implementation and reporting?
2. Is the dependency-summary invariant sufficient for correctness, especially
   for conditionals, lookup functions, ranges, and intra-run recurrence?
3. Should FP4.A support only references/operators first, or include passive
   function classification in the same implementation slice?
4. Is the proposed `FormulaPlaneFunctionContract` boundary the right place for
   builtins to expose dependency/span behavior, or should dependency contracts
   remain separate from eval kernels?
5. What is the minimal useful span IR for passive dependency summaries without
   overcommitting to an evaluator design?
6. How should small-workbook overhead gates be measured and enforced before any
   ingest integration?
7. What is the safest first materialization reduction: shared template AST,
   summary-edge sidecar, or lazy graph materialization for a single static class?
8. Which unsupported constructs must be explicit in the first fallback taxonomy
   to avoid hidden correctness risk?

## 16. Status

This document is a starting contract for FP4.0 architecture review. It makes no
runtime-win claim. The next action is independent review before FP4.A code.
