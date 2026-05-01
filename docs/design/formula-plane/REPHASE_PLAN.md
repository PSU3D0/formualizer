# FormulaPlane Bridge Rephase Plan

Status: updated plan for `formula-plane/bridge` after FP4.0 runtime-contract review and FP4.A implementation planning.

## Decision

Core+Overlay production closeout is paused as the immediate priority. Partitioning and span scheduling continue, but they are re-centered around FormulaPlane template/run ownership rather than the Core+Overlay `Session` no-legacy pathway.

This branch intentionally copies only reusable primitives and architecture notes from `migration/core-overlay` first. It must not import the Core+Overlay public facade, no-legacy harness, shadow-oracle machinery, or Session dirty-pilot authority as the product path.

## Why

The no-graph audit showed that hard `DependencyGraph` amputation is not close. At the same time, dense/shared-formula performance is not best solved by a weak sidecar that still allocates one formula AST/vertex/edge set per formula cell. The useful next unit is a FormulaPlane bridge:

```text
XLSX shared formula or inferred copied formula
  -> FormulaTemplateArena
  -> FormulaRunStore / placements / holes
  -> dependency summaries by region/partition
  -> dirty/result masks
  -> span scheduler / batched evaluator
  -> compatibility adapter only where required
```

## Non-goals for the first rephase

- No public API behavior change.
- No public/default fail-fast regressions.
- No hidden no-graph rewrite.
- No import of experimental `spike_no_graph` code.
- No Core+Overlay production-closeout continuation (`9.Q.5`, `9.Q.6`, etc.).
- No broad claim that XLSX/XML loading is or is not the bottleneck before baseline measurements are recorded.

## Copy-forward set

Copied from `migration/core-overlay` into this branch, then relocated to `formualizer-eval` because these are experimental runtime/planning concepts rather than stable cross-crate common types:

- `crates/formualizer-eval/src/formula_plane/ids.rs`
- `crates/formualizer-eval/src/formula_plane/partition.rs`
- `crates/formualizer-eval/src/formula_plane/grid.rs`
- `crates/formualizer-eval/src/formula_plane/virtual_ref.rs`
- `docs/design/formula-plane/FORMULA_PLANE_V2.md`
- `docs/design/formula-plane/VIRTUAL_REFERENCES.md`
- `docs/design/formula-plane/PARTITION_COMPATIBILITY_NOTES.md`

These are seed primitives/docs. They are not yet runtime authority and are not public API commitments.

## Incremental phases

This phase map supersedes the initial coarse FP4/FP5 naming. Historical reports
may refer to the older labels; forward-looking work should use this map.

### FP0 — Seed branch and reusable primitives

Status: **complete**.

Deliverables:

- Fresh worktree from `origin/main`.
- Dependency-light FormulaPlane bridge primitives compiled under
  `formualizer-eval`.
- FormulaPlane/partition/virtual-reference docs copied to the new design
  namespace.
- Rephase plan recorded.

Gate:

```bash
cargo test -p formualizer-eval --quiet
cargo test --workspace --quiet
```

Success claim: only that reusable primitives compile and current behavior is
unchanged.

### FP1 — Baseline and measurement closure

Status: **complete through FP1.B**.

Deliverables:

- Baseline reports under `docs/design/formula-plane/dispatch/`.
- Bounded harnesses and reports for:
  - `load_ms`, `full_eval_ms`, `incremental_us`, RSS where available;
  - formula cell count;
  - formula AST/root count;
  - graph formula vertex count;
  - graph edge/dependency row count;
  - repeated-template/fill-down candidate counts;
  - raw shared-formula metadata visibility where available;
  - adapter/backend mode (`umya`, `calamine`, current native mode);
  - load split (`open_read_ms`, `workbook_ingest_ms`, retained `load_ms`);
  - adapter load counters for formula/value handoff.

Gate:

- No performance win required.
- Reports must make uncertainty explicit and avoid broad claims.

### FP2 — Passive formula-template/run representation

Status: **complete through FP2.B**.

Deliverables:

- FP2.A scanner-only FormulaPlane candidate span and row-block partition
  counters.
- FP2.B passive in-memory `FormulaTemplateArena` / `FormulaRunStore` builder.
- Deterministic template/run IDs, row/column/singleton runs, holes, exceptions,
  rejected cells, and FP2.A reconciliation.
- No evaluation semantics change.

Measured outputs:

- template count;
- formula cell count;
- largest template families;
- contiguous row/column run candidates;
- exceptions/holes estimate;
- unsupported/dynamic/volatile counts;
- row-block partition fanout estimates;
- passive store counters and reconciliation deltas.

Success claim: repeated formula structure can be quantified and represented
without accuracy risk.

### FP3 — Passive store reporting and materialization accounting

Status: **complete**.

Deliverables:

- `scan-formula-templates` emits `formula_run_store` from the passive
  `FormulaRunStore`.
- Scanner JSON emits `materialization_accounting` by joining optional runner
  graph materialization stats.
- Bounded report quantifies compact representation opportunity vs current graph
  formula vertices, AST roots/nodes, and graph edges.

No runtime authority yet.

Gate:

- Existing eval/workbook tests unchanged.
- Scanner/reporting integration remains read-only.
- Avoidable counts are labeled representation/materialization opportunity
  estimates, not runtime wins.

### FP4.0 — Runtime contract and architecture review

Status: **complete**.

Deliverables:

- `docs/design/formula-plane/FORMULA_PLANE_RUNTIME_CONTRACT.md`.
- Independent architecture reviews from `openai-codex/gpt-5.5` and
  `anthropic/claude-opus-4-7`.
- Review feedback folded into the runtime contract.
- Re-review verdicts `PASS-WITH-NITS`.

Gate:

- Contract distinguishes formula family vs formula class.
- Contract includes bidirectional dependency-summary invariants.
- Contract separates dependency-only function contracts from future span kernels.
- Contract preserves small-workbook overhead discipline and no global
  workbook-size enable/disable heuristic.

### FP4.A — Passive dependency-template summaries

Status: **complete**.

Plan/report:

- `docs/design/formula-plane/dispatch/fp4a-implementation-plan.md`.
- `docs/design/formula-plane/dispatch/fp4a-dependency-summary-report.md`.

Deliverables:

- Authority-grade `template_canonical.rs` under
  `crates/formualizer-eval/src/formula_plane/`.
- Passive `dependency_summary.rs` under
  `crates/formualizer-eval/src/formula_plane/`.
- Initial supported class: `StaticPointwise` only.
- Affine finite cell/range precedent pattern surface.
- Run-instantiated summaries over accepted `FormulaRunStore` runs.
- Passive reverse mapping counters for edit invalidation feasibility.
- Fixed-`CollectPolicy` comparison harness against current dependency planning.
- Scanner JSON section `dependency_summaries`.

Gate:

- No graph/runtime/materialization authority change.
- Any supported summary under-approximation is a correctness failure.
- Unsupported/dynamic/volatile/open/whole-axis/name/table/3D/external/spill/local
  constructs are explicit fallback/reject reasons.

### FP4.B — Passive function dependency taxonomy

Status: **future**.

Deliverables:

- Builtin/function dependency classification audit.
- Dependency-only function contract registry or equivalent local sidecar.
- Classification tied to existing `FnCaps`, `ArgSchema`, registry identity, and
  explicit FormulaPlane reject reasons.
- No span eval kernels yet.

Gate:

- Function classification is passive and report-only.
- Unknown/custom/reference-returning/dynamic functions have explicit fallback
  reasons.

### FP4.C — Small-workbook overhead gates

Status: **future**.

Deliverables:

- Bounded small-workbook corpus: 10, 100, 1k, and 5k formula shapes; mostly
  unique formulas; small dense copied blocks; mixed unsupported formulas.
- Tier-0 overhead measurements for template fingerprint/count bookkeeping.
- Local/lazy promotion policy validation.

Gate:

- No production full `FormulaRunStore` build for mostly unique small workbooks.
- No global workbook-size heuristic as the primary enable/disable mechanism.

### FP4.D — Loader/shared-formula metadata bridge

Status: **future parallel metadata-input phase**.

Deliverables:

- Audit and document what Umya and Calamine expose for XLSX shared formulas and
  shared strings.
- Preserve or surface shared-formula group hints where possible.
- If backend metadata is insufficient, add a scoped XML-reader experiment only as
  a metadata bridge, not a full loader rewrite.

Gate:

- Same workbook results as current loader.
- New hints are optional/passive; absence must not change semantics.
- Loader hints can inform FormulaPlane reporting/planning but do not create
  runtime authority by themselves.

### FP5 — Graph-build hint integration, no authority change

Status: **future**.

Deliverables:

- Feed run/dependency summary metadata into ingest/graph build as hint-only data.
- Graph still materializes normally.
- Report what could have been skipped and why runs did or did not promote.
- Diagnostics for edge density, partition fanout, dirty breadth, and fallback
  reasons.

Gate:

- No hidden legacy fallback in optimized diagnostics: compatibility use must be
  counted.
- Conservative summaries are allowed only with bounded/reportable breadth.
- No graph bypass or dirty authority yet.

### FP6 — First materialization reduction

Status: **future**.

Deliverables:

- First low-risk materialization reduction, preferably shared-template IR or
  summary-edge sidecar with reverse dirty semantics before broad compact graph
  authority.
- Counters:
  - adapter/materialized-cell count;
  - graph formula vertex count;
  - graph edge row count;
  - AST root count;
  - formula snapshot entry count;
  - run count;
  - template count;
  - exception count;
  - fallback/demotion reason counts.

Success claim: any optimized path must prove it is not silently allocating one
graph/AST/edge set per dense formula cell.

### FP7 — First span executor

Status: **future**.

Deliverables:

- One narrow, contract-driven span execution path with oracle coverage.
- Candidate first kernels: pointwise arithmetic/comparison, mask-aware `IF`, and
  criteria aggregation.
- Accuracy comparison against existing engine on bounded fixtures.
- Quantified wall-time/allocation result only after materialization/execution
  authority is actually changed.

Gate:

- Accuracy-preserving against current engine outputs.
- Fallback for unsupported functions/dynamic refs is explicit and counted.
- Win must be tied to reduced materialization or batch execution, not hidden
  semantics changes.

## Landing policy

Each phase should land independently with:

1. narrow scope;
2. tests or reports;
3. baseline/metric deltas where relevant;
4. no public-support regression;
5. no hidden broad fallback;
6. clear statement of what is *not* claimed.

## Relationship to Core+Overlay

Core+Overlay remains a semantic reference for:

- VC/PC coordinate separation;
- page-table-compatible structural edits;
- deleted sheet tombstones;
- fallback taxonomy;
- partition diagnostic vocabulary.

It is not the primary product path for the FormulaPlane bridge. Future integration can consume FormulaPlane runs/partitions later if it becomes useful.
