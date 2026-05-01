# FormulaPlane Runtime Contract

Status: **revised FP4.0 design contract after dual architecture review**

Branch: `formula-plane/bridge`  
Initial draft: `6fe71c9 docs(formula-plane): draft runtime contract`

Reviews: `docs/design/formula-plane/dispatch/fp4-runtime-contract-review-codex.md`, `docs/design/formula-plane/dispatch/fp4-runtime-contract-review-opus.md`

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
from passive reporting into dependency-summary authority, graph-build hints,
materialization reduction, or span evaluation. It is intentionally broader than a
single formula optimization: FormulaPlane should classify arbitrary formulas by
semantic contracts, promote only safe local structures, and keep compatibility
fallback explicit and counted.

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

## 4. Review-driven corrections from the initial draft

The first FP4.0 draft was reviewed independently by `openai-codex/gpt-5.5` and
`anthropic/claude-opus-4-7`. Both reviews returned `REVISION-REQUIRED` while
agreeing that the high-level architecture is sound. This revision folds in the
shared feedback:

1. dependency summaries need both precedent and reverse-dependent invariants;
2. dependency scope must include structural/metadata dependencies, not only cell
   references;
3. authority-grade formula family identity must be exact and separate from the
   current lossy bench scanner fingerprint;
4. reference patterns require affine per-axis/per-endpoint modeling for mixed
   anchors and open/whole-axis ranges;
5. FP4 must separate passive dependency contracts from future span evaluation
   kernels;
6. `FnCaps`, `ArgSchema`, and existing reference-returning semantics are inputs
   to classification, not optional side channels;
7. materialization policy requires downgrade/state-machine semantics;
8. small-workbook overhead requires concrete Tier-0 budgets and no production
   full-store build for mostly unique workbooks;
9. `CollectPolicy`, conservative cycle synthesis, LET/LAMBDA/local environments,
   dynamic spills/result regions, and future cancellation behavior must be
   explicit.

## 5. Phase naming note

`REPHASE_PLAN.md` used an earlier coarse phase naming where FP4 was the
shared-formula/loader capability bridge and FP5 was dependency summaries. This
runtime contract refines the immediate architecture sequence after FP3:

```text
FP4.0  Runtime contract and review
FP4.A  Passive dependency-template summaries
FP4.B  Passive function dependency taxonomy
FP4.C  Small-workbook overhead gates
FP5    Graph-build hint integration, no authority change
FP6    First materialization reduction
FP7    First span executor
```

The loader/shared-formula bridge remains necessary, but it should not block the
runtime contract and dependency-summary work. When the rephase plan is next
updated, the historical loader bridge should be renamed or slotted as a parallel
metadata-input phase so FP4 naming is not ambiguous.

## 6. Core vocabulary

### 6.1 Formula family

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

Authority-grade family identity includes:

- literal values, not just literal kinds;
- function canonical identity, namespace/registry identity where applicable,
  argument count, and argument order;
- operator kind and associativity/structure;
- reference anchoring for every row/column axis and range endpoint;
- value-context vs reference-context behavior where it affects semantics;
- stable sheet identity where available;
- name/table binding mode where references are not ordinary cells/ranges;
- volatility/dynamic-reference flags that affect dependency authority;
- array/spill behavior when present.

Diagnostics may use coarser fingerprints only when explicitly labeled
non-authoritative. The current bench scanner's `source_template_id` and canonical
strings are useful for FP1–FP3 opportunity reporting, but FP4 authority work must
not reuse any lossy fingerprint that collapses formulas such as `=A1+1` and
`=A1+2` into one executable identity.

Normalization preserves the Formula Plane V2 stored-reference convention:

- relative axes normalize to placement-anchor deltas;
- absolute axes remain literal user-visible/VC coordinates;
- mixed anchors are per-axis, not whole-reference flags;
- explicit sheet identity is stable when known and must not be rebound by display
  name spelling alone.

Example:

```excel
C2 = $A$1 + B2
C3 = $A$1 + B3
```

These can share a family because `$A$1` remains an absolute literal coordinate
and `B2`/`B3` normalize to the same relative offset from `C2`/`C3`.

### 6.2 Formula class

A **formula class** is a semantic contract category derived from the AST and
function contracts. It is not one textual formula shape.

A class describes:

- dependency footprint kind;
- output/result shape kind;
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
| `OpaqueScalar` | Unknown/custom function or unsupported construct; compatibility path only. |

Volatility is an orthogonal flag layered on top of any class, not a peer class.
A static-looking formula containing a volatile child is a static class with a
volatile scheduling/evaluation requirement, unless policy demotes it.

Dynamic dependency is recursive: if any subtree can produce runtime-derived
reference targets, the whole template is ineligible for compact dependency
authority until a runtime target-tracking contract exists. For example,
`IF(A1, B1, OFFSET(...))` is not safe for compact static dependency authority
merely because the dynamic branch may be untaken.

### 6.3 Dependency summary

A dependency summary is a symbolic relation:

```text
precedent region(s) -> dependent formula run/partition -> result region
```

Correctness requires two complementary invariants.

Precedent invariant:

```text
true_scalar_dependencies(run) ⊆ summarized_dependencies(run)
```

Reverse-dependent invariant:

```text
true_scalar_dependents(precedent_cell_or_region)
  ⊆ summarized_dependents(precedent_cell_or_region)
```

The first invariant protects scheduling/readiness of a run. The second protects
incremental dirty propagation and edit invalidation. Any materialization policy
that skips scalar per-cell edges must provide an explicit operation equivalent to:

```text
precedent cell/region change -> dependent run partition set
```

A summary may be exact or conservative. Over-approximation can cause extra dirty
work; under-approximation is a correctness bug. If a safe summary cannot be
proven in both relevant directions, FormulaPlane must reject compact authority
for that run and record a fallback reason.

Dependency scope includes more than ordinary cell/range references. The summary
must account for, classify, or explicitly reject:

- named ranges and named expressions;
- table metadata and structured references, including current-row references;
- open/whole-row/whole-column ranges and used-region growth;
- 3D references;
- external references;
- volatile recalculation sources;
- dynamic runtime references;
- sheet/name/table structural changes;
- spill/result-region shape dependencies;
- the active `CollectPolicy` used by the graph planner.

### 6.4 Span execution

Span execution computes a run or run partition as a batch/chunk rather than
invoking the scalar interpreter independently for every formula cell.

Full-workbook evaluation still has an O(output cells) lower bound when all
results must be produced. Span execution aims to reduce avoidable overhead:

- per-cell AST dispatch;
- per-cell graph traversal;
- per-cell function dispatch;
- poor cache locality;
- repeated scans/index work for copied formula families.

Future span kernels must honor the existing cooperative cancellation path used by
`RangeView`/`FunctionContext`. This is not FP4.A scope, but it belongs to the
runtime contract before FP7 authority.

### 6.5 Compatibility materialization

Compatibility materialization creates legacy AST/graph/cell structures for APIs
or constructs that require them. It must be explicit and counted. A path may not
claim compact FormulaPlane authority while silently allocating one graph/AST/edge
set per dense formula cell.

## 7. Layered runtime architecture

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
  - reverse dependent summaries for edit invalidation

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

## 8. Dependency-summary model

### 8.1 Authority-grade canonicalization input

FP4.A requires an exact normalized-template representation in
`formualizer-eval`, not only the bench-only scanner canonicalizer. The bench
scanner remains valuable, but authority-grade dependency summaries need an input
that preserves literals, mixed anchors, reference context, function identity, and
all fallback-relevant labels.

Recommended FP4.A input slice:

```text
crates/formualizer-eval/src/formula_plane/template_canonical.rs
```

This module should be passive and internal. The bench scanner can call it, but
runtime/authority code must not depend on `formualizer-bench-core`.

### 8.2 Template-level analysis

For each formula template, the analyzer traverses the exact normalized AST and
emits:

```rust
struct FormulaDependencyTemplateSummary {
    template_id: FormulaTemplateId,
    status: DependencySummaryStatus,
    formula_class: FormulaClass,
    precedent_patterns: Vec<PrecedentPattern>,
    flags: DependencySummaryFlags,
    fallback_reasons: Vec<DependencyRejectReason>,
    collect_policy: DependencyCollectPolicyFingerprint,
}
```

`CollectPolicy` is part of the correctness statement. If FormulaPlane summaries
and `DependencyGraph::plan_dependencies` use different expansion policies, the
comparison is not meaningful and compact authority must demote or record a
policy-drift fallback reason.

### 8.3 Affine reference pattern model

A coarse enum such as `RelativeCell` vs `AbsoluteCell` is not sufficient. Excel
references can mix anchors independently per axis and per range endpoint:

```excel
$A1
A$1
$A1:B$2
A:A
1:1
A1:B
```

Use an affine axis/end-point model conceptually like:

```rust
enum AxisRef {
    RelativeToPlacement { offset: i32 },
    AbsoluteVc { index: u32 },
    OpenStart,
    OpenEnd,
    WholeAxis,
    Unsupported,
}

struct AffineCellPattern {
    sheet: SheetBinding,
    row: AxisRef,
    col: AxisRef,
}

struct AffineRectPattern {
    sheet: SheetBinding,
    start_row: AxisRef,
    start_col: AxisRef,
    end_row: AxisRef,
    end_col: AxisRef,
}
```

Additional pattern/reject variants must cover at least:

| Pattern/reason | Initial policy |
|---|---|
| finite same-sheet cell/range | support in FP4.A if exact/conservative summary is straightforward |
| finite static cross-sheet cell/range | support or classify with explicit sheet binding |
| mixed-anchor cell/range | support via affine axes |
| open/whole-row/whole-column range | classify; likely fallback or bounded conservative summary first |
| 3D cell/range | explicit fallback/reason initially |
| external reference | explicit fallback/reason initially |
| structured table static range | classify; fallback until table metadata contract exists |
| structured table current-row | explicit current-cell-sensitive fallback/reason initially |
| static named range | classify only if resolver provides stable static range |
| named expression | explicit fallback/reason until expression dependency expansion exists |
| reference-returning function | distinguish static reference result from dynamic dependency |
| dynamic runtime reference | explicit fallback/reason initially |
| unknown/custom function | scalar/legacy fallback unless dependency contract is supplied |
| array/spill result | explicit result-region contract or fallback |

### 8.4 Run-level instantiation

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

Run summary instantiation applies only to actual accepted run cells. Holes,
exceptions, rejected cells, and singleton runs must never inherit a neighboring
run's summary by implication. They either have their own summary or remain
legacy/materialized.

### 8.5 Reverse summaries for edit invalidation

For any policy that skips scalar edges, FormulaPlane needs a reverse operation:

```text
changed precedent cell/region -> dependent formula run partitions
```

This can be conservative. For example, a changed cell in `A5000` may dirty the
whole dependent partition for `C4097:C8192` even if only one result row uses it.
But the mapping must not miss dependent partitions.

### 8.6 Conservative branches

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

If any branch contains a dynamic runtime reference, compact dependency authority
must be rejected unless runtime target tracking exists, even if the branch may be
untaken.

### 8.7 Dynamic references

For constructs such as runtime-resolved references, the analyzer may still record
child expression dependencies for diagnostics, but it must not claim compact
static dependency authority for the produced reference target.

Initial policy:

```text
DynamicDependency -> explicit fallback / legacy materialization
```

Future work may add bounded dynamic summaries, but only with explicit runtime
tracking and invalidation contracts.

### 8.8 Result regions and spills

Dependency summaries must model both precedent regions and result regions.
For ordinary scalar formulas, the result region is the formula placement itself.
For dynamic-array/spill formulas, the result region may be larger or
data-dependent.

Initial policy:

```text
known bounded result region -> classify but likely fallback until supported
unknown/data-dependent spill region -> explicit fallback
```

A span executor must not write results outside the proven result region.

### 8.9 Intra-run recurrence

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

Initial policy may be dependency-summary-only or legacy fallback. If accepted by
a later scheduler, recurrence runs require intra-partition serial order unless a
recurrence-aware span kernel exists. Unsupported cycles remain legacy/fallback.

### 8.10 Conservative cycle synthesis

A conservative dependency summary can synthesize a cycle that the scalar edge
walker would not produce. For example, conservative branch unions or broad range
summaries can create partition-level backedges absent in exact scalar edges.

Policy:

```text
if conservative summary creates a cycle not accepted by an explicit recurrence
or cycle policy, demote the affected run/partition out of summary authority and
record `conservative_cycle_synthesis`.
```

Do not let conservative summaries silently alter observable cycle behavior.

## 9. Function contracts

The existing scalar `Function` trait remains the compatibility path. It supports
scalar formula evaluation and already gives functions access to `RangeView`,
capability flags, argument schema, and reference-returning behavior.

FP4 dependency classification must consume existing surfaces:

- `FnCaps` (`VOLATILE`, `DYNAMIC_DEPENDENCY`, `RETURNS_REFERENCE`,
  `SHORT_CIRCUIT`, `REDUCTION`, `ELEMENTWISE`, `WINDOWED`, `LOOKUP`, etc.);
- `ArgSchema`, including by-reference vs by-value semantics;
- `eval_reference` capability, but only as a classification signal unless an
  explicit static-reference dependency contract exists;
- function registry identity and unknown/custom-function fallback.

`FnCaps` are necessary inputs but not a complete FormulaPlane contract. Some
builtins may need more precise dependency contracts than current caps express.
FP4 should add a dependency-only contract layer first, not an eval kernel API.

### 9.1 FP4 dependency-only contract

Conceptual FP4-scope trait/registry:

```rust
trait FormulaPlaneDependencyContract {
    fn dependency_contract(
        &self,
        args: &[TemplateExpr],
        scalar_function_caps: FnCaps,
        scalar_arg_schema: &[ArgSchema],
    ) -> DependencyContract;
}
```

This is illustrative. The key boundary is:

```text
FP4.A/FP4.B: classify dependencies and reject reasons only
FP7+: compile/evaluate span kernels
```

Do not require `compile_span_call`, `eval_span`, or `SpanOutput` design in FP4.A.
Those belong to a later span-kernel contract once passive dependency summaries
and graph hints are validated.

### 9.2 Reference-returning vs dynamic dependency

`RETURNS_REFERENCE` is not equivalent to `DYNAMIC_DEPENDENCY`.

Examples:

- A reference-returning function can return a statically analyzable reference in
  a supported context.
- A dynamic-reference function can derive a target from runtime values and must
  fall back until runtime target tracking exists.

Initial policy can conservatively fallback all reference-returning functions, but
reports must distinguish:

```text
reference_returning_static_unimplemented
```

from:

```text
dynamic_dependency_runtime_target
```

### 9.3 Argument evaluation modes

Function dependency contracts need per-argument modes, not just function-level
flags:

| Mode | Meaning |
|---|---|
| eager value | ordinary value dependency over child expression |
| lazy/short-circuit | child may not be evaluated for all lanes; dependency summary may still union conservatively |
| by-reference | argument should be analyzed as a reference object |
| criteria expression | value expression interpreted by criteria parser/logic |
| reference-returning | argument or result can be a reference in reference context |
| current-cell-sensitive | semantics depend on formula placement/current row |
| local-binding | name resolves through LET/LAMBDA/local environment, not workbook global names |

### 9.4 `IF` example

Dependency summary:

```text
deps(IF(cond, then, else)) = deps(cond) ∪ deps(then) ∪ deps(else)
```

Span evaluation cannot eagerly evaluate both branches if doing so would expose
errors or side effects that scalar Excel semantics would suppress. A future span
IF kernel must:

```text
1. evaluate condition mask;
2. evaluate true branch only for true lanes;
3. evaluate false branch only for false lanes;
4. merge results preserving scalar semantics;
5. honor cancellation.
```

This is not FP4.A scope; FP4.A only classifies the dependency contract.

### 9.5 `SUMIFS` example

Scalar execution sees one formula call at a time. A copied-report run exposes a
larger opportunity:

```text
same sum range
same criteria ranges
vector of criteria values from output-row-relative refs
many output rows
```

A future span `SUMIFS` kernel can reuse masks, group repeated criteria, stream
fact ranges once per chunk, or build indexes. This is beyond graph-build
avoidance and is required for full-eval wins in report/fact-table workloads.

FP4 dependency contract should only identify the static target/criteria ranges,
criteria expressions, and unsupported reasons.

### 9.6 LET/LAMBDA/local environment

The interpreter has local bindings and callable values. Dependency analysis must
not treat locally-bound names as workbook named ranges. Initial policy:

- classify LET/LAMBDA/local-binding constructs explicitly;
- walk into bodies only when a conservative lexical dependency model exists;
- otherwise fallback with `lambda_or_local_env_unsupported`;
- never silently skip body dependencies.

## 10. Span IR

FormulaPlane should eventually lower normalized AST templates into an internal
span IR before execution authority is attempted.

Illustrative future shape:

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

FP4.A should not implement full span IR lowering. It may define a narrower
passive dependency expression representation if needed.

Explicit handling or rejection is required for:

- implicit intersection / `@` behavior;
- `:` reference combinator;
- array literals and dynamic arrays;
- `Call` / LAMBDA invocation;
- reference context vs value context;
- spill/result-region behavior.

Sub-AST reuse belongs at this layer, not in whole-formula family identity.

## 11. Sub-AST and expression-template policy

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

## 12. Materialization policy

FormulaPlane must use per-template/per-run policy, not global workbook-size
enablement.

Conceptual policy:

```rust
enum MaterializationPolicy {
    EagerLegacy,
    TemplateMetadataOnly,
    SharedTemplateIr,
    DependencySummaryOnly,
    SummaryEdgesSidecar,
    SpanExecutable,
    LazyMaterializeOnDemand,
}
```

`SharedTemplateIr` intentionally avoids the unsafe implication that the current
scalar `ASTNode` can be reused across placements. It means normalized
placement-aware template IR plus an explicit placement reifier. Any use of the
current scalar interpreter still needs correct per-placement reference
materialization.

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

### 12.1 Policy state machine

Policies must have explicit transitions. Examples:

| Event | Allowed transition | Counter/reason |
|---|---|---|
| new mid-run formula exception | split run or demote affected segment | `run_split_for_exception`, `demoted_for_exception` |
| literal override/hole | mark hole or split run | `hole_recorded`, `run_split_for_hole` |
| dynamic dependency discovered | any compact policy -> `EagerLegacy` or `TemplateMetadataOnly` | `dynamic_dependency_demote` |
| conservative cycle synthesized | compact dependency policy -> legacy/materialized | `conservative_cycle_synthesis` |
| policy/CollectPolicy drift | summary policy -> legacy/materialized | `collect_policy_drift` |
| unsupported function/kernel | span policy -> dependency-only or legacy | `span_kernel_missing` |
| structural name/table/sheet change | invalidate summaries or demote | `structural_dependency_invalidated` |

A future implementation may choose split-vs-demote policies, but the choice must
be explicit and counted. Silent downgrade is not allowed.

### 12.2 Summary-edge sidecar semantics

`SummaryEdgesSidecar` is only safe if it defines both directions:

```text
run/partition -> precedent summary regions
precedent change -> dependent run/partition set
```

If a sidecar cannot answer the reverse dirty query conservatively, it is not a
safe stepping stone to compact authority.

### 12.3 Materialization sequence

Initial runtime-facing phases should prefer:

1. hint-only policy reporting;
2. shared-template IR experiments;
3. summary-edge sidecars with reverse dirty semantics;
4. lazy/eager graph materialization reduction;
5. span execution authority.

The design must not jump directly from passive representation to broad graph
bypass.

## 13. Small-workbook overhead contract

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

Tiering:

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
short run -> metadata or shared-template IR only
long dense static run -> dependency summary candidate
supported function kernel + evaluation demand -> span plan candidate
unsupported/dynamic/volatile -> explicit fallback
```

### 13.1 Tier-0 budget targets

Before any ingest integration, establish small-workbook gates. Initial targets:

| Workbook shape | Tier-0 allowed median ingest overhead target |
|---|---:|
| 10 formulas | <= 1% or noise-floor bounded, whichever is larger |
| 100 formulas | <= 5% |
| 1k formulas | <= 5% |
| 5k formulas | <= 5% |
| mostly unique formulas | no full `FormulaRunStore::build` in production ingest path |

These are initial design targets, not measured claims. The benchmark report must
record hardware/context and use bounded repetitions. If the targets are too
strict or too lax after measurement, update the contract with evidence.

### 13.2 Parse-once clarification

"Parse once" does not mean FormulaPlane analysis is free. Canonicalization is an
AST walk unless fused with existing dependency planning. FP4.A may use a separate
pass for passive reporting. Production ingest integration must either:

1. fuse Tier-0 fingerprinting with existing parse/dependency planning; or
2. prove the second walk stays within the small-workbook budget.

## 14. Scheduling contract

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
FormulaPlane has explicit semantics and oracle coverage. Conservative summaries
that synthesize cycles absent from the scalar dependency walker must demote out
of compact summary authority unless an explicit recurrence/cycle policy accepts
them.

`IntraRunRecurrence` is not only a formula class; it is also a scheduling shape.
Until recurrence-aware kernels exist, such runs require intra-partition serial
order or legacy fallback.

## 15. Evaluation contract

For any FormulaPlane-authoritative run/partition:

1. Placement coverage is exact: holes/exceptions/rejections are excluded or
   explicitly represented.
2. Every cell in the run uses the recorded normalized template.
3. Dependency summary is exact or conservative in both precedent and
   reverse-dependent directions.
4. The active `CollectPolicy` is compatible with the summary contract.
5. Scheduler respects summary edges, reverse dirty semantics, and recurrence
   policy.
6. Span evaluation result equals scalar interpreter result for every output cell.
7. Unsupported constructs fall back explicitly and are counted.
8. Future span kernels honor cancellation.

Initial span-authority phases must use oracle comparison against current engine
on bounded fixtures before claiming runtime wins.

## 16. FP4.A implementation contract

FP4.A should remain passive and read-only. It should not change graph behavior,
loader behavior, scheduler behavior, evaluation behavior, public APIs, or save
behavior.

Recommended implementation slice:

1. **Authority-grade canonicalization module**
   - Add `crates/formualizer-eval/src/formula_plane/template_canonical.rs`.
   - Preserve literals, mixed anchors, sheet/name/table binding modes, and
     reference/value context labels.
   - Keep bench scanner fingerprints explicitly diagnostic if they remain lossy.

2. **Passive dependency summary module**
   - Add `crates/formualizer-eval/src/formula_plane/dependency_summary.rs`.
   - Support affine same-sheet and static cross-sheet finite cell/range refs.
   - Support unary/binary operators and conservative branch unions for ordinary
     AST nodes where safe.
   - Classify but initially reject/fallback open ranges, names, tables, 3D,
     external refs, dynamic refs, volatile policy, spills, unknown/custom
     functions, reference-returning functions, and LET/LAMBDA unless explicit
     rules are implemented.

3. **Function classification using existing surfaces**
   - Consume `FnCaps`, `ArgSchema`, and registry lookup.
   - Do not add span eval kernel APIs in FP4.A.

4. **Run-instantiated summaries**
   - Instantiate summaries over existing `FormulaRunStore` accepted runs.
   - Keep holes/exceptions/rejections separate.
   - Emit precedent and reverse-dependent summary counters.

5. **Comparison harness**
   - Compare passive summaries with current dependency planner output on bounded
     fixtures where feasible.
   - Report exact match, conservative overage, rejection count, policy drift, and
     unsupported reasons.
   - Do not claim runtime wins.

6. **Scanner JSON/reporting**
   - Add a `dependency_summaries` section to `scan-formula-templates` output.
   - Include fallback reason histograms and small-workbook timing counters if
     measured.

Explicitly out of scope for FP4.A:

- new public API;
- graph mutation or graph bypass;
- dirty propagation authority;
- scheduler authority;
- span IR evaluator;
- span function kernels;
- materialization reduction;
- production ingest integration unless explicitly re-scoped.

## 17. Later phase recommendations

### FP4.B — Passive function dependency taxonomy

Expand function classification and dependency contracts, still passive. Tie
classification to current `FnCaps`, `ArgSchema`, registry identity, and explicit
FormulaPlane reject reasons.

### FP4.C — Small-workbook overhead gates

Add bounded small-workbook cases and instrumentation to prove Tier-0 metadata
overhead is negligible and deeper planning is local/lazy.

### FP5 — Graph-build hint integration

Feed run/dependency summary metadata into ingest/graph build as hint-only data.
Graph still materializes normally. Report what would have been skipped and why
runs did or did not promote.

### FP6 — First materialization reduction

Prefer lower-risk reductions first, such as shared-template IR or sidecar summary
edges with reverse dirty semantics, before broad compact graph authority.

### FP7 — First span executor

Add narrow, contract-driven span execution with oracle coverage. Candidate first
kernels: pointwise arithmetic/comparison, mask-aware `IF`, and criteria
aggregation. Fallback remains explicit.

## 18. Open questions for follow-up

1. What exact representation should `template_canonical.rs` expose: normalized
   AST snapshots, fingerprints plus metadata, or a minimal dependency-oriented
   template IR?
2. How much of dependency summary comparison can reuse current
   `DependencyGraph::plan_dependencies` without depending on graph internals?
3. Should static named ranges be resolved during FP4.A, or classified only until
   name-expression semantics are specified?
4. Where should LET/LAMBDA/local-environment dependency analysis live?
5. Which subset of structured references can be static enough for FP4.A, if any?
6. How should `CollectPolicy` drift be detected and reported in scanner-only
   artifacts?
7. What is the smallest reverse-dependent summary representation that can answer
   edit invalidation without scalar edge expansion?
8. What policy should split vs demote runs when exceptions appear after a compact
   policy has been selected?
9. What benchmark shapes should define the small-workbook overhead gates before
   production ingest integration?
10. Which future function contract additions belong in `FnCaps` versus a local
    FormulaPlane dependency-contract registry?

## 19. Status

This revision folds the dual architecture-review feedback into the FP4.0 runtime
contract. It makes no runtime-win claim. The next action should be FP4.A planning
or implementation against the narrowed passive dependency-summary contract.
