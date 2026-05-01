# FP4.A Dependency Summary Closeout Report

Date: 2026-05-01  
Branch: `formula-plane/bridge`  
Scanner artifact code HEAD: `6b527c9 feat(formula-plane): report dependency summaries in scanner`  
Artifact directory: `target/fp4a-dependency-summaries/6b527c9`

## 1. Scope

FP4.A is closed out as a passive reporting slice only. The scanner artifacts in
this report exercise the new authority-template and dependency-summary reporting
surface, but they do not make those summaries runtime authority.

Explicit authority boundary:

- no graph authority changed;
- no runtime/evaluation authority changed;
- no materialization authority changed;
- no loader, scheduler, dirty-propagation, save/output, or public/default API
  behavior changed;
- no load, memory, full-eval, or incremental-recalc runtime-win claims are made.

## 2. FP4.A commit inventory

Code/doc commits included in the FP4.A implementation sequence before this
closeout report:

| Commit | Subject |
|---|---|
| `a649bff` | `docs(formula-plane): plan fp4a dependency summaries` |
| `cb81989` | `docs(formula-plane): align phase map` |
| `6354fe8` | `feat(formula-plane): add authority template canonicalizer` |
| `02bdff0` | `fix(formula-plane): reject spill and implicit intersection templates` |
| `b53849e` | `feat(formula-plane): join runs to authority templates` |
| `10af8b3` | `feat(formula-plane): add passive dependency summaries` |
| `049bed3` | `fix(formula-plane): tighten dependency summary rejects` |
| `e19a3eb` | `feat(formula-plane): instantiate run dependency summaries` |
| `4ff9ae7` | `test(formula-plane): compare dependency summaries to planner` |
| `6b527c9` | `feat(formula-plane): report dependency summaries in scanner` |

This document is the FP4.A.7 closeout artifact. Its commit is docs-only and does
not change the scanner artifacts above.

## 3. Raw artifact set

The raw scanner outputs were generated under
`target/fp4a-dependency-summaries/6b527c9` using a release build of
`scan-formula-templates`.

Notable files:

| File | Purpose |
|---|---|
| `metadata.txt` | Artifact HEAD, branch, timestamp, and worktree metadata. |
| `build.release.stdout` / `build.release.stderr` | Bounded release scanner build logs. |
| `scenario-status.tsv` | Per-scenario timeout status, duration, and artifact path. |
| `scenario-summary.tsv` | Extracted scenario counters used by this report. |
| `*.dependency-summary.json` | Raw scanner JSON for each FP3 scenario. |
| `*.stderr` | Per-scenario scanner stderr; all six were empty. |
| `validation-status.tsv` | Bounded validation command status and log paths. |
| `validation.*.stdout` / `validation.*.stderr` | Validation command logs. |

## 4. Scanner command summary

Build command:

```bash
timeout 15m cargo build -p formualizer-bench-core \
  --features formualizer_runner \
  --bin scan-formula-templates \
  --release
```

Result: exit `0`.

Per-scenario command shape:

```bash
timeout 2m target/release/scan-formula-templates \
  --scenarios benchmarks/scenarios.yaml \
  --scenario "$scenario" \
  --root . \
  > "$RUN_DIR/$scenario.dependency-summary.json"
```

All six requested scenarios completed within the 2 minute per-scenario bound.
No scenario timed out and no scenario wrote stderr.

## 5. Scenario dependency-summary table

Counters below are copied from `scenario-summary.tsv`. `Precedent/result/reverse`
means `precedent_region_count`, `result_region_count`, and
`reverse_summary_count` from the scanner's `dependency_summaries` section.
`Comparison exact/over/under/rejected` means the fixed-policy comparison counts
from `dependency_summaries.comparison`.

| Scenario | Formula cells | Authority templates | Supported / rejected templates | Run summaries | Precedent / result / reverse | Comparison exact / over / under / rejected | Top fallback reasons |
|---|---:|---:|---:|---:|---:|---:|---|
| `headline_100k_single_edit` | 100001 | 2 | 1 / 1 | 1 | 1 / 1 / 25 | 100000 / 0 / 0 / 1 | template: `finite_range_unsupported=2`, `function_unsupported:SUM=2`; comparison: `finite_range_unsupported=1`, `function_unsupported:SUM=1` |
| `chain_100k` | 99999 | 1 | 1 / 0 | 1 | 1 / 1 / 25 | 99999 / 0 / 0 / 0 | none |
| `fanout_100k` | 100000 | 100000 | 100000 / 0 | 0 | 0 / 0 / 0 | 0 / 0 / 0 / 0 | template: `diagnostic_source_template_collision=1`, `missing_template_summary=1`; comparison: none |
| `inc_cross_sheet_mesh_3x25k` | 50000 | 2 | 2 / 0 | 2 | 4 / 2 / 14 | 50000 / 0 / 0 / 0 | none |
| `agg_countifs_multi_criteria_100k` | 1000 | 1 | 0 / 1 | 0 | 0 / 0 / 0 | 0 / 0 / 0 / 1000 | template: `finite_range_unsupported=2`, `function_unsupported:COUNTIFS=2`; comparison: `finite_range_unsupported=1000`, `function_unsupported:COUNTIFS=1000` |
| `agg_mixed_rollup_grid_2k_reports` | 12000 | 5 | 1 / 4 | 1 | 2 / 1 / 3 | 10000 / 0 / 0 / 2000 | template: `finite_range_unsupported=12`, `function_unsupported:SUMIFS=4`, `function_unsupported:COUNTIFS=2`; comparison: `finite_range_unsupported=3000`, `function_unsupported:SUMIFS=1000`, `function_unsupported:COUNTIFS=500` |

## 6. Under-approximation status

`under_approximation_count` is `0` for every generated scenario artifact.

Aggregate across the six bounded FP3 scenario scans:

```text
under_approximation_count_total = 0
```

This is the required FP4.A correctness posture for supported summaries. Any
future nonzero `under_approximation_count` remains a correctness failure, not a
performance tradeoff.

## 7. Validation commands and results

Validation was run after generating the raw artifacts and after the docs-only
closeout edits. The `git log` result still points at scanner code HEAD `6b527c9`
because this report had not been committed yet.

| Command | Result |
|---|---|
| `timeout 30s git status --short` | exit `0`; expected docs-only changes: `REPHASE_PLAN.md` modified and this report untracked before commit. |
| `timeout 30s git log -1 --oneline` | exit `0`; `6b527c9 feat(formula-plane): report dependency summaries in scanner`. |
| `timeout 10m cargo fmt --all -- --check` | exit `0`. |
| `timeout 15m cargo test -p formualizer-eval formula_plane --quiet` | exit `0`; 66 passed, 0 failed, 1173 filtered out. |
| `timeout 15m cargo test -p formualizer-bench-core --features formualizer_runner --quiet` | exit `0`; package targets passed, including 2 scanner-adjacent tests and 4 additional bench-core tests, 0 failed. |

## 8. Limitations and deferred items before FP5 graph-build hints

- `StaticPointwise` remains the only supported authority class in FP4.A; function
  families such as `SUM`, `COUNTIFS`, `SUMIFS`, and `AVERAGEIFS` remain explicit
  fallback/reject reasons until FP4.B taxonomy work.
- Finite ranges in value position are still rejected in the current passive
  dependency-summary slice, which explains the aggregation scenario rejections.
- `fanout_100k` exposes an authority-vs-diagnostic mapping limitation: the
  authority canonicalizer distinguishes 100000 templates, while the diagnostic
  source template collapses them into one source template, so no unambiguous run
  summary is emitted for that scenario.
- Names, tables/structured references, 3D/external references, dynamic runtime
  references, volatility policy, spill/local-environment semantics, and
  unknown/custom functions remain fallback/deferred unless an explicit later
  contract supports them.
- Reverse summary counts are passive feasibility counters only; they are not dirty
  propagation authority and are not used to skip scalar graph edges.
- FP5 graph-build integration must consume these summaries as hints only. It must
  preserve current graph materialization authority until a later phase proves a
  safe materialization-reduction policy.

## 9. Closeout status

FP4.A PASS: passive dependency-template summaries exist for a narrow
StaticPointwise subset, scanner reporting exposes supported/rejected summaries,
and comparison against current dependency planning shows no under-approximation
for supported fixtures. No graph/runtime/materialization authority changed.
