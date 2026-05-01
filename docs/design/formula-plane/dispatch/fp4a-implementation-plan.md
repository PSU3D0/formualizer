# FP4.A Implementation Plan — Passive Dependency-Template Summaries

Date: 2026-04-30  
Branch: `formula-plane/bridge`  
Base before plan: `7eaa02b docs(formula-plane): rereview runtime contract`

## 1. Purpose

FP4.A is the first implementation slice after the FP4.0 runtime contract. Its
job is to add **passive dependency-template summaries** for a narrowly supported
formula class, without changing runtime behavior.

The target passive pipeline is:

```text
parsed formula AST
  -> authority-grade canonical template key
  -> FormulaRunStore run/template join
  -> dependency template summary
  -> run-instantiated precedent/result summaries
  -> reverse dirty-query summary counters
  -> scanner JSON/reporting
```

FP4.A is successful when it can explain a bounded `StaticPointwise` subset with
no under-approximation against the current dependency planner, while explicitly
rejecting everything outside scope.

## 2. Inputs

Primary design inputs:

- `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_CONTRACT.md`
- `docs/design/formula-plane/dispatch/fp4-runtime-contract-rereview-codex.md`
- `docs/design/formula-plane/dispatch/fp4-runtime-contract-rereview-opus.md`
- `docs/design/formula-plane/dispatch/fp3-store-materialization-report.md`

Current code inputs:

- `crates/formualizer-eval/src/formula_plane/span_store.rs`
- `crates/formualizer-eval/src/formula_plane/span_counters.rs`
- `crates/formualizer-bench-core/src/bin/scan-formula-templates.rs`
- `crates/formualizer-eval/src/engine/ingest_builder.rs`
- `crates/formualizer-eval/src/engine/graph/mod.rs`
- `crates/formualizer-eval/src/function.rs`
- `crates/formualizer-parse/src/parser.rs`

## 3. Strict non-goals

FP4.A must not implement or change:

- scheduler behavior;
- formula evaluation;
- dirty propagation authority;
- dependency graph bypass;
- graph materialization reduction;
- loader behavior;
- public API;
- save/output behavior;
- Core+Overlay Session/no-legacy integration;
- span execution kernels;
- production ingest integration, unless explicitly re-scoped later.

All outputs are passive diagnostics, tests, and scanner/report data.

## 4. Supported initial formula class

FP4.A starts with one authority-eligible class:

```text
StaticPointwise
```

Initial supported subset:

- scalar-output formulas;
- finite same-sheet cell references;
- finite static cross-sheet cell references if stable sheet binding is available;
- mixed absolute/relative anchors represented per axis;
- literals preserved exactly;
- unary operators over supported children;
- binary arithmetic/comparison operators over supported children;
- deterministic template canonicalization independent of input order.

Explicit initial fallback/reject set:

- open-ended ranges;
- whole-row / whole-column ranges;
- finite ranges in value position that imply array/spill/broadcast semantics;
- names and named expressions;
- tables and structured references, including current-row references;
- 3D references;
- external references;
- dynamic runtime references;
- volatile formulas unless represented only as diagnostic fallback;
- reference-returning functions;
- unknown/custom functions;
- LET/LAMBDA/local environment;
- dynamic arrays/spills;
- unsupported parser nodes or context-sensitive constructs.

Later FP4.A increments may relax some rejects, but the first slice should be
small enough that planner comparison can be exact for supported cases.

## 5. Core invariants

### 5.1 No under-approximation

For supported summaries:

```text
true_scalar_dependencies(run) ⊆ summarized_dependencies(run)
true_scalar_dependents(precedent) ⊆ summarized_dependents(precedent)
```

FP4.A comparison must treat any under-approximation as a correctness failure.
Over-approximation is allowed only when reported and bounded.

### 5.2 Diagnostic IDs are not authority IDs

Existing scanner `source_template_id` and FP2/FP3 template IDs remain diagnostic.
FP4.A introduces or joins an authority-grade canonical template key for dependency
summaries.

### 5.3 Exact family identity

Authority-grade template identity must preserve:

- literal values;
- operator/function identity;
- argument order;
- mixed anchor semantics;
- sheet binding mode;
- reference/value context labels where relevant;
- fallback-relevant flags.

### 5.4 Accepted run cells only

Run-instantiated summaries apply only to accepted run cells. Holes, exceptions,
rejected cells, and unrelated singleton cells must not inherit neighboring run
summaries.

### 5.5 Fixed comparison policy

Planner comparison is performed under an explicit fixed `CollectPolicy`.
Policy drift is a reported fallback/reject reason, not a silent mismatch.

## 6. Deliverables

Code deliverables:

```text
crates/formualizer-eval/src/formula_plane/template_canonical.rs
crates/formualizer-eval/src/formula_plane/dependency_summary.rs
crates/formualizer-eval/src/formula_plane/mod.rs          # re-export internal module
crates/formualizer-bench-core/src/bin/scan-formula-templates.rs
```

Test deliverables:

- unit tests for canonicalization;
- unit tests for dependency summary classification;
- unit tests for run-instantiated summaries and reverse mapping;
- comparison tests against current dependency planning for supported pointwise
  formulas;
- scanner JSON smoke test if practical.

Documentation/report deliverables:

```text
docs/design/formula-plane/dispatch/fp4a-dependency-summary-report.md
```

Related doc consistency:

```text
docs/design/formula-plane/REPHASE_PLAN.md
```

`REPHASE_PLAN.md` should use the same FP4.0/FP4.A/FP4.B/FP4.C/FP4.D/FP5/FP6/FP7
phase map as the runtime contract. Historical dispatch reports may retain their
original wording, but forward-looking docs should use the updated map.

## 7. Phased implementation plan

### FP4.A.0 — Plan alignment and remaining doc nits

Goal: make implementation scope unambiguous before code.

Tasks:

1. Commit this implementation plan.
2. Confirm `REPHASE_PLAN.md` points to the revised FP4.0/FP4.A sequence before
   FP4.A code begins.
3. If any later report uses old FP4/FP5 wording, update the forward-looking owner
   references or add an explicit historical-note qualifier.

Tests/gates:

```bash
timeout 30s git status --short
```

Commit suggestion:

```text
docs(formula-plane): plan fp4a dependency summaries
```

### FP4.A.1 — Authority-grade template canonicalization

Goal: add exact, internal canonicalization suitable for dependency-summary
attachment.

New module:

```text
crates/formualizer-eval/src/formula_plane/template_canonical.rs
```

Key types, names illustrative:

```rust
pub(crate) struct CanonicalTemplate {
    pub key: FormulaTemplateKey,
    pub expr: CanonicalExpr,
    pub labels: CanonicalTemplateLabels,
}

pub(crate) struct FormulaTemplateKey(/* stable debug/hash payload */);

pub(crate) enum CanonicalExpr {
    Literal(...),
    Reference(CanonicalReference),
    Unary { op, expr },
    Binary { op, left, right },
    Function { id, args },
    ArrayUnsupported,
    CallUnsupported,
}
```

The module must document its structured-reference rewrite side:

- FP4.A scanner/passive path may canonicalize raw parsed formulas.
- Production ingest fusion is not in FP4.A scope.
- If current-row structured references are observed, classify explicitly rather
  than silently canonicalizing post-rewrite ASTs inconsistently.

Test matrix:

| Test | Expected |
|---|---|
| `=A1+1` vs `=A1+2` | different authority keys |
| copied `=A2+B2` from `C2:C4` | same key after relative normalization |
| copied `=$A$1+B2` | same key; absolute axis preserved |
| `$A1`, `A$1`, `$A1:B$2` | mixed axes preserved in canonical reference |
| cross-sheet static ref | sheet binding represented |
| dynamic ref function | dynamic/reject label present |
| unknown/custom function | unknown/reject label present |
| LET/LAMBDA/local env | explicit unsupported/local-env label |
| input order shuffled | deterministic keys |

Gate:

```bash
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
```

Commit suggestion:

```text
feat(formula-plane): add authority template canonicalizer
```

### FP4.A.2 — Authority template sidecar for scanner/run-store path

Goal: join passive runs to authority-grade canonical template keys without
breaking FP3 diagnostic output.

Tasks:

1. Keep `source_template_id` diagnostic-only.
2. Add scanner-side construction of `CanonicalTemplate` per formula or per
   diagnostic template group.
3. Add a sidecar mapping:

```text
source_template_id -> authority_template_key(s)
authority_template_key -> representative canonical template
run_id -> authority_template_key
```

4. Detect and report diagnostic collisions, e.g. one `source_template_id` mapping
   to multiple authority keys.

Expected JSON addition, names illustrative:

```json
"authority_templates": {
  "template_count": 2,
  "diagnostic_collision_count": 0,
  "templates": [ ... ]
}
```

Tests:

- `source_template_id` collision for `=A1+1` / `=A1+2` is detected, not merged.
- Existing FP3 `formula_run_store` output still appears.
- Run-to-authority-template mapping is deterministic.

Gate:

```bash
timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet
```

Commit suggestion:

```text
feat(formula-plane): join runs to authority templates
```

### FP4.A.3 — Passive dependency summary core

Goal: classify supported canonical templates and emit precedent patterns.

New module:

```text
crates/formualizer-eval/src/formula_plane/dependency_summary.rs
```

Core model, names illustrative:

```rust
pub(crate) enum FormulaClass {
    StaticPointwise,
    Rejected,
}

pub(crate) enum AnalyzerContext {
    Value,
    Reference,
    ByRefArg,
    CriteriaArg,
    ImplicitIntersection,
    LocalBinding,
}

pub(crate) enum AxisRef {
    RelativeToPlacement { offset: i32 },
    AbsoluteVc { index: u32 },
    OpenStart,
    OpenEnd,
    WholeAxis,
    Unsupported,
}

pub(crate) struct AffineCellPattern { ... }
pub(crate) struct AffineRectPattern { ... }

pub(crate) enum DependencyRejectReason {
    OpenRangeUnsupported,
    WholeAxisUnsupported,
    NamedRangeUnsupported,
    StructuredReferenceUnsupported,
    ThreeDReferenceUnsupported,
    ExternalReferenceUnsupported,
    DynamicDependency,
    VolatileUnsupported,
    ReferenceReturningUnsupported,
    UnknownFunction,
    LocalEnvUnsupported,
    SpillUnsupported,
    UnsupportedAstNode,
}
```

Scope:

- Support finite cell references in `Value` context.
- Support unary/binary operators if both children are supported.
- Preserve fallback labels for unsupported references/functions.
- Treat whole/open-axis ranges as explicit FP4.A fallback.
- Do not add span eval function contracts.

Tests:

| Test | Expected |
|---|---|
| `=A1+B1` | `StaticPointwise`, two finite precedent patterns |
| `=$A$1+B2` copied down | absolute + relative patterns |
| `=Sheet2!A1+B1` | static cross-sheet pattern if sheet binding available |
| `=SUM(A1:A10)` | rejected or dependency-only fallback, not pointwise authority |
| `=A:A` or `=SUM(A:A)` | `WholeAxisUnsupported` |
| `=INDIRECT(A1)` | `DynamicDependency` |
| unknown/custom function | `UnknownFunction` |
| volatile function | volatile fallback/reject or diagnostic volatility label |
| LET/LAMBDA | `LocalEnvUnsupported` |

Gate:

```bash
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
```

Commit suggestion:

```text
feat(formula-plane): add passive dependency summaries
```

### FP4.A.4 — Run-instantiated summaries and reverse mapping

Goal: instantiate template summaries over accepted `FormulaRunStore` runs and
produce passive reverse dirty-query summaries.

Tasks:

1. For each accepted run with supported template summary, compute:

```text
result region
precedent region(s)
row-block partition summary
reverse changed-region -> dependent run partition summary
```

2. Exclude holes/exceptions/rejections from inherited summaries.
3. Add conservative overage counters:

```text
reverse_query_count
reverse_exact_partition_count
reverse_conservative_partition_count
reverse_max_overage
reverse_median_overage
global_dirty_fallback_count
```

4. If reverse mapping degenerates to dirty-all/global fallback, mark the run as
   rejected/demoted for compact authority in reporting.

Tests:

| Test | Expected |
|---|---|
| vertical run `C1:C100` depends on `A1:A100` | row-block summaries deterministic |
| changed `A50` | maps to dependent partition containing `C50` |
| hole in run | hole not included in result region summary |
| exception in run | exception not inherited by main run summary |
| rejected template | no run summary authority |
| row-block size normalization | deterministic block IDs |
| shuffled input | identical summaries |

Gate:

```bash
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
```

Commit suggestion:

```text
feat(formula-plane): instantiate run dependency summaries
```

### FP4.A.5 — Fixed-policy planner comparison harness

Goal: compare passive summaries to current dependency planning under a named
oracle.

Oracle:

```text
DependencyGraph::plan_dependencies output under fixed CollectPolicy
```

Comparison model should normalize planner output into a common dependency
universe:

```text
direct cells
finite ranges
open ranges
names
tables
structural dependencies
unsupported references
```

Initial FP4.A can compare only supported finite-cell `StaticPointwise` cases and
report others as rejected/unsupported.

Required report fields:

```text
exact_match_count
over_approximation_count
under_approximation_count
rejection_count
policy_drift_count
fallback_reason_histogram
```

Gate:

```text
under_approximation_count == 0
```

Tests:

- `=A1+B1` exact match.
- copied relative formulas exact match after instantiation.
- mixed anchors exact or conservative match.
- unsupported refs reject, not mismatch.
- intentional policy drift is detected if testable.

Gate:

```bash
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
```

Commit suggestion:

```text
test(formula-plane): compare dependency summaries to planner
```

### FP4.A.6 — Scanner JSON integration

Goal: expose passive dependency summaries in benchmark scanner output.

Extend:

```text
crates/formualizer-bench-core/src/bin/scan-formula-templates.rs
```

Add JSON section:

```json
"dependency_summaries": {
  "authority_template_count": 0,
  "supported_template_count": 0,
  "rejected_template_count": 0,
  "run_summary_count": 0,
  "precedent_region_count": 0,
  "result_region_count": 0,
  "reverse_summary_count": 0,
  "comparison": { ... },
  "fallback_reasons": { ... }
}
```

Output rules:

- preserve existing FP1–FP3 JSON sections;
- keep diagnostic `source_template_id` separate from authority key;
- label all estimates and unsupported reasons clearly;
- no runtime-win claims.

Tests/gates:

```bash
timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet
```

Smoke command:

```bash
timeout 2m target/release/scan-formula-templates \
  --scenarios benchmarks/scenarios.yaml \
  --scenario headline_100k_single_edit \
  --root . \
  > target/fp4a-smoke/headline_100k_single_edit.dependency-summary.json
```

Commit suggestion:

```text
feat(formula-plane): report dependency summaries in scanner
```

### FP4.A.7 — Bounded baseline and closeout report

Goal: record bounded evidence and remaining gaps.

Create:

```text
docs/design/formula-plane/dispatch/fp4a-dependency-summary-report.md
```

Suggested artifact directory:

```bash
RUN_DIR=target/fp4a-dependency-summaries/$(git rev-parse --short HEAD)
mkdir -p "$RUN_DIR"
```

Run scanner on the six FP3 scenarios if build time permits:

```bash
for s in headline_100k_single_edit chain_100k fanout_100k inc_cross_sheet_mesh_3x25k agg_countifs_multi_criteria_100k agg_mixed_rollup_grid_2k_reports; do
  timeout 2m target/release/scan-formula-templates \
    --scenarios benchmarks/scenarios.yaml \
    --scenario "$s" \
    --root . \
    > "$RUN_DIR/$s.dependency-summary.json"
done
```

Report must include:

- code/doc commits;
- supported/rejected template counts;
- fallback reason histogram;
- exact/over/under comparison counts;
- reverse summary counters;
- small-workbook timing if measured;
- explicit statement that runtime behavior is unchanged;
- next risks before FP5 graph-build hints.

Validation gate:

```bash
timeout 10m cargo fmt --all -- --check
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet
```

Commit suggestion:

```text
docs(formula-plane): record fp4a dependency summary report
```

## 8. End-to-end FP4.A validation gate

Minimum closeout commands:

```bash
timeout 30s git status --short
timeout 30s git log -1 --oneline
timeout 30s rg "template_canonical|dependency_summary|dependency_summaries" \
  crates/formualizer-eval/src/formula_plane \
  crates/formualizer-bench-core/src/bin/scan-formula-templates.rs \
  docs/design/formula-plane/dispatch

timeout 10m cargo fmt --all -- --check
timeout 15m cargo test -p formualizer-eval formula_plane --quiet
timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet
```

Optional if filters miss tests:

```bash
timeout 15m cargo test -p formualizer-eval --quiet
```

No full workspace, fuzz, soak, or nightly benchmark gate is required for FP4.A.

## 9. Stop conditions

Stop and report instead of broadening scope if any of the following occur:

- authority canonicalization cannot be made exact without parser changes beyond
  this phase;
- supported `StaticPointwise` summaries under-approximate planner dependencies;
- comparison requires mutating `DependencyGraph` or changing runtime behavior;
- run summary instantiation cannot exclude holes/exceptions/rejections cleanly;
- reverse mapping degenerates into unbounded/global dirty fallback for supported
  pointwise runs;
- scanner JSON integration risks breaking existing FP1–FP3 output;
- implementation requires public API or loader behavior changes;
- compile/test time exceeds bounded gates and requires larger suite decisions.

## 10. Commit boundaries

Preferred commit sequence:

1. `docs(formula-plane): plan fp4a dependency summaries`
2. `feat(formula-plane): add authority template canonicalizer`
3. `feat(formula-plane): join runs to authority templates`
4. `feat(formula-plane): add passive dependency summaries`
5. `feat(formula-plane): instantiate run dependency summaries`
6. `test(formula-plane): compare dependency summaries to planner`
7. `feat(formula-plane): report dependency summaries in scanner`
8. `docs(formula-plane): record fp4a dependency summary report`

If the implementation is smaller, adjacent code commits may be combined. Keep
docs/report commits separate from behavior/code commits where practical.

## 11. Expected FP4.A status statement

If all gates pass, the correct status statement is:

```text
FP4.A PASS: passive dependency-template summaries exist for a narrow
StaticPointwise subset, scanner reporting exposes supported/rejected summaries,
and comparison against current dependency planning shows no under-approximation
for supported fixtures. No graph/runtime/materialization authority changed.
```

Do not claim load, memory, full-eval, or incremental-recalc wins from FP4.A.
