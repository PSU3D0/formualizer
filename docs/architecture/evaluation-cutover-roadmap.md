# Evaluation Cutover Roadmap and Session Handoff

Status: Durable strategic roadmap with a mutable current-status section

This document gives a fresh engineering session enough context to continue the Formualizer evaluation program without relying on chat history, temporary plans, or an earlier agent session. It explains the long-term architecture, the work already completed, the remaining delivery sequence, and the operational rules for safely advancing it.

The detailed contracts live in:

- [Evaluation Resources and Target-Driven Cutover](evaluation-resource-target-driven-cutover.md)
- [Adaptive Formula Partition](adaptive-formula-partition.md)
- [Formula Family Stream Ingest](formula-family-stream-ingest.md)
- [Formula Function Closure and Fragmented Families](formula-function-closure-fragmented-families.md)

## 1. North Star

Formualizer must evaluate small interactive workbooks and very large finance workbooks through one semantically consistent engine without forcing either workload into the wrong representation.

The target system has these properties:

- XLSX loading is single-pass and preserves exact source meaning, including malformed or unsupported formula families.
- Dense repeated formulas remain compressed as FormulaPlane spans rather than becoming millions of graph vertices.
- Sparse formulas remain ordinary graph vertices and pay effectively no FormulaPlane tax.
- Mixed workbooks evaluate through one dependency authority and one target-driven coordinator.
- Resource exhaustion is deterministic, typed, mode-neutral, and transactional.
- Optimization limits change strategy; they never change workbook values, destroy compressed authority, or reject work that the ordinary evaluator accepts.
- Cell, range, delta, cancellation, recalc-plan, and SheetPort requests all use the same preparation and evaluation contract.
- Authoritative FormulaPlane execution is enabled only after cold-process, corpus, and canary evidence proves value, error, latency, and memory parity.

This is not merely a performance project. It connects source fidelity, compressed representation, dependency ownership, dirty-state correctness, resource governance, demand-driven evaluation, public APIs, and rollout safety.

## 2. How the Architecture Fits Together

### 2.1 Calamine and source-family ingest

Calamine owns XLSX observation: source order, formula metadata, cached values, observed occupancy, shared-family geometry, and replay evidence. The evaluator receives backend-neutral source-family transport and must never infer formulas from OOXML dimensions or ranges.

Proven families may become FormulaPlane authority. Unsupported, malformed, incomplete, or uncertain families replay exactly through legacy formulas. Replay ownership remains explicit so no formula is skipped or applied twice.

### 2.2 FormulaPlane and the legacy graph

FormulaPlane stores repeated formula families as compressed spans. The legacy graph stores sparse or unsupported formulas as vertices. The graph remains the sole dependency authority; FormulaPlane contributes producer summaries, result regions, and span identities without creating a second dependency graph.

The engine must support both representations in the same workbook. A cache may accelerate the mixed topology, but cache availability cannot define correctness.

### 2.3 Dirty authority

`FormulaDirtyState` is graph-owned and is the only owner of formula dirtiness, changed regions, whole-span seeds, and generation-based leases. Topology revisions invalidate planning caches only; they do not imply value dirtiness.

Successful evaluation acknowledges exactly the leased generation or prefix. Cancellation and failure preserve retryable work, including later events that happen to be identical to leased events.

### 2.4 Resource budgets

`EvalConfig` owns one explicit, nested `EvaluationBudgets` value. Every field is unset by default, preserving current behavior. There are no resource profiles, named operating modes, or policy enums such as Compatibility or FinanceBalanced.

`ResourceEnvelope` is only a neutral helper that converts explicit host constraints into ordinary budget values. Recommended values may be published after calibration, but they must remain normal values or constructors rather than behavioral modes.

The request ledger accounts work, deadlines, retained memory, scratch memory, and typed exhaustion. Some budget classes are deliberately dormant until the transaction or exact-strategy layer required to enforce them safely exists.

### 2.5 Target preparation and evaluation

The end state is one target pipeline:

```text
selectors or API arguments
  -> normalized EvaluationTarget values
  -> pure staged-source discovery
  -> monotonic Exact -> Sheets -> Workbook widening
  -> one prepared multi-unit transaction
  -> revision-bound mixed producer closure
  -> evaluation with exact dirty subleases and deltas
```

A discovery pass is pure. If it widens, its local work is discarded. No abandoned pass may mutate staging, caches, diagnostics, reports, dirty state, authority, or visible values.

### 2.6 SheetPort

SheetPort becomes an adapter over typed engine targets. It must not maintain a separate prepare-all, prepare-sheets, catch-error, and full-evaluation fallback ladder. Static requests prepare and evaluate once. Dynamic or opaque dependencies widen explicitly under engine policy and telemetry.

## 3. Non-Negotiable Invariants

### 3.1 Source fidelity

- Never synthesize formula occupancy from OOXML dimensions or ranges.
- Preserve formulas, cached values, malformed-family behavior, source order, parse policies, and load limits.
- Calamine owns observed occupancy and exact replay evidence.
- Supported authority is registry- and contract-driven; do not maintain a second function-name allowlist.
- Authority geometry remains `RowRun`, `ColRun`, or `Rect`. Uncertainty fails closed.
- A selected compressed or fragmented source family is initially one atomic ownership unit.

### 3.2 Transactionality

- All recoverable validation, revision checks, capacity checks, and injected faults occur before first mutation.
- After mutation starts, there is no recoverable branch and no allocation required for logical completion.
- Do not clone, scan, or roll back existing workbook state to simulate atomicity.
- Allocator OOM and invariant panic are process-fatal, not recoverable transaction outcomes.
- Failed preparation preserves staging and all semantic state except explicitly documented inert arena or interner residue.

### 3.3 Evaluation correctness

- Off and Shadow remain replay-only.
- Authoritative FormulaPlane remains experimental until rollout gates pass.
- The graph remains the sole dependency authority.
- Topology epochs are cache invalidation only.
- Optimization overflow changes exact strategy; it does not demote spans.
- True hard request limits are applied at a common coordinator boundary.
- Cycles preserve configured static, runtime, or iterative semantics.
- Cancellation acknowledges no uncompleted dirty lease.

### 3.4 Resource API

- There is one explicit `EvaluationBudgets`; its nested optional fields are the public truth.
- There are no resource profiles or named behavioral modes.
- Legacy fields merge independently into unset destination fields during the compatibility period.
- Explicit budget fields win only their own conflicts; unrelated legacy values still map.
- Legacy memory splits 50/50 into individually unset retained and scratch totals, with an odd byte assigned to retained.
- Recommended values may be named later only as ordinary values or constructors backed by calibration evidence.

### 3.5 Engineering workflow

- Keep the main checkout clean; use a dedicated worktree per tranche.
- Use one writer for a worktree.
- Use fresh-context agents when delegation materially helps; do not revive stale sessions for convenience.
- Use fresh, read-only reviewers after implementation.
- Do not let reviewers write into the implementation worktree.
- Land tranches in dependency order and do not start a successor before the predecessor is green and merged.

## 4. Completed Program History

### 4.1 Single-pass Calamine and source transport

PR #179 moved XLSX family ingest to Calamine 0.36 single-pass APIs, consolidated backend-neutral source transport, preserved exact fallback, and established anchor-once analysis. Formula-only dimensions, sparse behavior, cached values, parse policies, and load limits remained intact.

PR #180 documented the function-closure and fragmented-family design.

PR #181 closed registry-owned function semantics, fragmented replay ownership, provider snapshotting, and eager transactional fragmented authority.

PR #182 activated deferred fragmented authority using the same prepared transaction model as eager loading.

PR #183 made Calamine the default native Python byte reader while retaining Umya for writing and Pyodide fallback.

### 4.2 FormulaPlane T1 correctness and topology

PR #184, T1.0a, added transactional exact-reference span demotion across sheets. It preserved overlays, validated generations and revisions, and made cycle demotion atomic.

PR #185, T1.0b, established capacity-bail parity, finite materialization limits, exact scheduled-span demotion, generation/prefix dirty leases, and success-only bailout telemetry.

PR #186, T1.1, added immutable Engine-owned mixed-topology caching with exact graph, authority, provider, and semantic revision keys. Bounded indexed visitors and checked candidate, edge, and memory accounting prevent full scans and partial cache publication.

PR #187, T1.2, made graph-owned `FormulaDirtyState` the sole dirty authority and fixed cycle-retry lease extension.

PR #188, T1.3, made structural dirtiness precise, indexed candidate selection, and name lifecycle demotion exact, prepared, atomic, and retry-safe. Global invalidation remains explicit only for genuinely opaque operations.

### 4.3 Evaluation resource and target cutover foundation

PR #189 fixed deferred cross-sheet `evaluate_cells_with_delta` preparation and added value, delta, and strict-failure restoration coverage.

PR #190 added the durable resource and target-driven cutover contract.

PR #191 added observational C0 telemetry: request IDs, topology events, cap observations, materialization counts, dirty-lease outcomes, staged preparation, phase timings, replay-spool metrics, and fresh-process RSS/HWM probes. It changed no evaluation behavior.

PR #192 implements C1a: explicit nested budgets, a request ledger, work and deadline checkpoints, typed resource errors, typed incomplete results, fixed-point atomicity, and structured Python/WASM/SheetPort error propagation. Resource profiles were removed before merge.

## 5. Mutable Current Status

This section is intentionally time-sensitive. A fresh session must verify it rather than assume it is still true.

As of 2026-07-16:

- `origin/main` is `1c03ec2f`, containing merged PRs through #191.
- PR #192 is open and mergeable. Its implementation head `9a880851` passed Rust, Python, WASM, native-wheel, and Pyodide-wheel checks; later documentation commits may have triggered a fresh run.
- The latest PR head and checks must be read from GitHub before merge.
- The branch is `feat/evaluation-resource-ledger` in worktree `codebase/oss/.worktrees/formualizer-evaluation-resource-ledger`.
- PR #192 must be merged before C1b starts.

If any of these facts differ, use GitHub and `origin/main` as the source of truth and update this section in the next documentation-bearing tranche.

## 6. Delivery Roadmap

### 6.1 C1b: exact non-materializing request topology

C1b replaces the remaining configured mixed-cache overflow demotion with an exact request strategy.

The strategy ladder is:

```text
retained complete mixed topology
  -> exact paged request topology
  -> bounded in-memory sorted runs and merge
  -> optional explicit native temporary scratch
  -> bounded repeated indexed passes for no-disk/WASM
  -> typed common exhaustion only when every exact strategy is unavailable
```

C1b activates the retained and scratch budget semantics that are observational in C1a.

Required gates:

- Candidate, edge, and byte cache limits at zero and cap-plus-one preserve Off/authoritative values and errors.
- Optimization overflow retains spans and materializes zero cells.
- No partial topology or dirty closure is consumed.
- Request scratch remains within its estimate and accounting tolerance.
- WASM succeeds through bounded no-disk passes with explicit work accounting.
- Native scratch uses a policy separate from formula replay spool ownership.
- Cache skip streak, exact strategy, pass count, and exhaustion reason are telemetered.
- True SCC or lifecycle demotion remains separate from optimization overflow.

C1b must not begin target preparation or SheetPort migration.

### 6.2 C2: transactional target preparation

C2 introduces the staged source-presence index and target preparation API for ordinary formulas in Off and Shadow first.

Principal types include:

- `EvaluationTarget`
- `StagedFormulaIndex`
- `StagedSourceUnit`
- `PrepareTargetsOptions`
- `PreparationRevision`
- `PreparedGraphForTargets`
- `PreparedTargetGraphReport`

Discovery must be side-effect-free and memoized by staged unit generation and semantic snapshot. Dynamic or opaque dependencies widen monotonically from exact targets to sheets to workbook.

One prepared transaction composes all selected graph additions, source dispositions, diagnostics, reports, and staged removals. C2 is where graph vertex/edge and materialization admission budgets become enforceable, because every relevant mutation must first share the same exact preflight boundary.

Required gates:

- Every pre-commit fault preserves the semantic digest.
- Only reachable staged units commit.
- Cross-sheet, name, and range dependencies match prepare-all values.
- Widening restarts publish nothing from abandoned passes.
- Stale revisions fail before mutation.
- Existing `prepare_graph_all` and `prepare_graph_for_sheets` remain compatibility APIs but do not claim transitive target completeness.

### 6.3 C3: deferred FormulaPlane source units

C3 brings complete and fragmented source families into the target transaction.

Selection remains whole-family or whole-package. The transaction composes checked FormulaPlane append, exact replay disposition, fragmented authority, ordinary exceptions, and legacy graph plans without splitting residual ownership.

Required gates:

- Eager/deferred parity in Off, Shadow, and authoritative modes.
- Source-order preservation.
- No skipped or duplicate replay.
- Complete package restoration on failure.
- Provider and semantic revision changes fail before mutation.

### 6.4 C4: unified mixed target coordinator

C4 introduces one `evaluate_targets` coordinator over mixed legacy vertices, FormulaPlane producers, and spill anchors.

It must support:

- Producer roots for cells, ranges, names, and tables.
- Mixed demand closure through legacy and span dependencies.
- Exact demanded span regions.
- Complete SCC-unit inclusion.
- One request-scoped retry ledger.
- Exact dirty subleases and successful partial acknowledgement.
- One volatile request epoch.
- Versioned run/region deltas with bounded per-cell compatibility output.

The existing cell, cells, delta, cancellation, and `evaluate_until` APIs become adapters.

Required gates:

- Unrelated dirty legacy and span branches remain pending.
- Target values and errors match full evaluation.
- Cancellation acknowledges nothing and retry converges.
- Dynamic widening reaches workbook at most once.
- Authoritative delta paths no longer return an empty delta merely because spans exist.

### 6.5 C5: recalc plans and SheetPort

C5 adds revision-bound target recalc plans with typed stale reasons. The engine returns `PlanStale`; higher-level callers may explicitly choose to rebuild.

SheetPort normalizes selectors to engine targets, prepares once, evaluates once, and removes its silent fallback ladder. Layout scan exhaustion becomes a typed selector error controlled by an explicit manifest setting.

Required gates:

- Static, name, table, and layout snapshots match current behavior.
- Dynamic or opaque requests widen explicitly.
- No prior error is swallowed by a full-evaluation retry.
- Engine options and cancellation state restore on every exit.

### 6.6 C6: calibration and rollout

C6 runs the complete native and WASM cold-process matrices and calibrates recommended budget values.

Evidence must include:

- Eligibility and exact replay rates.
- Widening frequency and cause.
- Cache hit, skip, paged, run-merge, disk, and repeated-pass strategies.
- Materialization count and reason.
- Load, prepare, topology, evaluation, and output latency.
- Peak RSS/HWM and accounted scratch.
- Cancellation and retry behavior.
- Value, error, and delta parity.

Recommended values may be published only after this evidence exists. They may be ordinary constructors or documented values returning `EvaluationBudgets`; they must not become profiles or behavioral modes.

Authoritative FormulaPlane remains a controlled canary until corpus and production evidence meet the gates. Changing a resource default does not automatically change FormulaPlane authority defaults.

## 7. Longer-Term FormulaPlane Roadmap

The C-series completes safe preparation, resource governance, and target-driven evaluation. It does not complete the entire adaptive FormulaPlane architecture.

### 7.1 T2: node lifecycle and fragmentation

Replace whole-span demotion for ordinary edits with domain interval sets, punchouts, and surgical splitting. Unify editor and engine mutation paths through the same lifecycle API.

A churn probe must quantify fragmentation and establish the baseline before automatic coalescing is introduced.

### 7.2 T3: cycle refinement

Replace whole-span cycle demotion with refinement on demand. Cyclic placements become singleton graph units inside the existing SCC machinery while non-cyclic placements remain compressed.

Demand closure must understand placement-interval expansion and preserve static, runtime, and iterative cycle semantics.

### 7.3 T4: coalescing and re-promotion

Add bounded background re-merge or re-promotion with hysteresis. Coalescing must not thrash under edit/undo/redo workloads and must preserve source ownership and diagnostics.

The T2 churn probe becomes the acceptance gate for bounded steady-state fragmentation.

### 7.4 T5: columnar batch execution

Add vectorized template evaluation over Arrow lanes for a proven operator and function subset. Maintain chunk summaries for incremental aggregates where associativity and error order are proven.

This executor is intentionally later because source ownership, dirty authority, target closure, and lifecycle must be stable first.

### 7.5 T6: wavefront and internal recurrences

Represent internal recurrence dependencies explicitly and evaluate them through wavefront or prefix algorithms where semantics permit. This admits cumulative and chain families that are currently rejected as internal dependencies.

Non-associative mixed-type recurrences must remain on the tree-walk oracle until error and coercion order are proven identical.

## 8. Rollout Policy

The rollout sequence is:

1. Off-mode and Shadow correctness across the corpus.
2. Cold-process resource and latency baselines.
3. Authoritative experimental corpus parity.
4. Controlled canary workloads with exact rollback to replay-only mode.
5. Production soak with telemetry and no silent fallback.
6. Only then consider changing defaults.

A benchmark is valid only if it asserts that the intended representation and execution path actually ran. Span count, vertex count, topology strategy, cache events, materialization, and dirty work must accompany latency numbers.

Warm benchmarks do not replace cold-process gates. In-process RSS deltas do not define budget decisions.

## 9. Known Residuals and Non-Goals

### 9.1 Current residuals

- Until C1b, configured mixed-cache candidate, edge, or byte overflow still uses the existing transactional materialization bridge.
- Until C2, graph and materialization budget fields are declarative rather than enforced.
- Until C4, active spans still force some cell-oriented APIs through full authoritative evaluation.
- Until C4, run/region deltas and exact partial dirty acknowledgement are incomplete.
- Until C5, SheetPort retains its current preparation and fallback structure.

### 9.2 Intentional non-goals

- No secondary list of supported function names.
- No formula occupancy inferred from OOXML ranges.
- No ambient free-memory sampling for budget decisions.
- No disk topology scratch unless explicitly configured.
- No residual per-family ownership splitting before the whole-family transaction is proven.
- No unconditional authoritative default before C6 evidence.
- No public resource profile enum or named behavioral mode.

## 10. Primary Code and Test Areas

A fresh implementation session should inspect these areas as relevant:

- `crates/formualizer-eval/src/engine/eval.rs`
- `crates/formualizer-eval/src/engine/resource_ledger.rs`
- `crates/formualizer-eval/src/engine/resource_observability.rs`
- `crates/formualizer-eval/src/engine/graph/prepared_legacy_graph.rs`
- `crates/formualizer-eval/src/engine/fragmented_transaction.rs`
- `crates/formualizer-eval/src/engine/formula_dirty.rs` or its current graph-owned location
- `crates/formualizer-eval/src/formula_plane/producer.rs`
- `crates/formualizer-eval/src/formula_plane/scheduler.rs`
- `crates/formualizer-eval/src/formula_plane/region_index.rs`
- `crates/formualizer-eval/src/formula_plane/authority.rs`
- `crates/formualizer-workbook/src/workbook.rs`
- `crates/formualizer-sheetport/src/runtime.rs`
- `bindings/python/`
- `bindings/wasm/`
- `crates/formualizer-bench-core/src/bin/probe-load-envelope.rs`
- `crates/formualizer-bench-core/src/bin/probe-load-envelope-matrix.rs`

Tests should cover Off, Shadow, and authoritative modes; eager and deferred ingestion; first, warm, and post-edit evaluation; cross-sheet dependencies; names and tables; cycles; cancellation; strict parse restoration; cap zero and cap-plus-one; native and WASM/no-disk execution.

## 11. Fresh-Session Boot Procedure

A fresh session should follow this order:

1. Read all applicable `AGENTS.md` files from the workspace root to the Formualizer worktree.
2. Check `git status`, `origin/main`, open PRs, and CI. Do not trust the mutable status in this document without verification.
3. Read this roadmap completely.
4. Read the target-driven cutover and adaptive partition documents completely.
5. Inspect the final diffs and discussion for the most recently merged tranche.
6. Create a clean worktree from the merged `origin/main`; do not reuse the dirty main checkout.
7. Confirm the tranche boundary and acceptance gates before editing.
8. Use one writer. If delegation is useful, use fresh-context agents and fresh read-only reviewers.
9. Run focused tests while iterating, then the required full default, all-feature, WASM, binding, formatting, and cold-process gates before merge.
10. Update the mutable status and roadmap only when the durable state changes.

## 12. Ready-to-Paste Fresh-Session Prompt

```text
Continue the Formualizer evaluation cutover program from durable repository state.

First:
1. Read every applicable AGENTS.md file.
2. Check origin/main, git status, open PRs, and CI. Do not assume the status recorded in docs is current.
3. Read completely:
   - docs/architecture/evaluation-cutover-roadmap.md
   - docs/architecture/evaluation-resource-target-driven-cutover.md
   - docs/architecture/adaptive-formula-partition.md
4. Inspect the final diff and review history for the latest merged tranche.
5. Create a clean dedicated worktree from the merged origin/main.

Strategic constraints:
- Preserve exact XLSX source meaning and Calamine-owned observed occupancy.
- Never synthesize formulas from OOXML dimensions or ranges.
- Off and Shadow remain replay-only; authoritative remains experimental.
- The graph is the sole dependency authority and FormulaDirtyState is graph-owned.
- All recoverable validation and faults precede first mutation.
- Do not clone, scan, or roll back existing workbook state to implement transactions.
- Optimization overflow changes exact strategy; it never demotes spans.
- Target discovery is pure and widening is monotonic Exact -> Sheets -> Workbook.
- There is one explicit nested EvaluationBudgets object, all unset by default.
- Do not introduce resource profiles or named behavioral modes.
- Recommended budget values may be added only after calibration as ordinary values or constructors.
- Use one writer and fresh-context reviewers.

Determine the next tranche from merged state:
- If PR #192 is not merged, finish and merge it only after all checks pass.
- If #192 is merged and C1b is not, implement C1b exact non-materializing request topology only.
- If C1b is merged, continue in order through C2 target preparation, C3 deferred FormulaPlane source units, C4 unified target evaluation, C5 recalc-plan/SheetPort migration, and C6 calibration.

Before editing, state the selected tranche, its dependencies, explicit non-goals, and acceptance gates. Do not combine successor tranches in one PR.
```
