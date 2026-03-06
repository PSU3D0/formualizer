# Benchmark Reporting Governance

This document defines how benchmark scenarios are classified, which scenario subsets are safe to compare publicly, and how reports should expose caveats.

The canonical metadata lives in:

- `benchmarks/scenarios.yaml` for family, tier, comparison-profile, runtime-mode, and regression-gate tagging.
- `benchmarks/function_matrix.yaml` for support policy, claim class, and caveat labels.

## Scenario Taxonomy

| Family | Intent | Default report posture |
|---|---|---|
| `incremental_locality` | Dirty-region containment, sparse locality, selective propagation | Claim-safe now for routine/core variants; caveated for nightly-scale variants |
| `chain_topology` | Chain depth, fan-out, and graph-shape stress | Claim-safe now |
| `lookup_dimension_join` | Exact-match lookup and dimension/fact join workloads | Claim-safe now |
| `aggregate_analytics` | Repeated aggregates and report-grid analytics | Mixed: finite-range core variants can be claim-safe now; whole-column or native-only variants are caveated |
| `structural_edit` | Row/sheet mutation, ref rewrite, dependency recovery | Claim-safe with caveats |
| `real_world_anchor` | Curated real XLSX anchors grounding synthetic results | Claim-safe with caveats |
| `nightly_stress` | Scale-biased or engine-advantaged stress workloads | Internal-only unless explicitly caveated |

## Tiering Rules

| Tier | Use | Reporting implication |
|---|---|---|
| `pr_smoke` | Fast correctness/regression subset | Safe for PR cadence; may also appear in comparative views if its comparison profile allows it |
| `comparative` | Routine recurring multi-engine/native runs | Primary source for recurring comparison tables |
| `nightly_heavy` | Large or slow scheduled workloads | Excluded from routine multi-engine runs unless explicitly promoted later |

## Approved Comparison Profiles

| Comparison profile | Runtime view | Intended use |
|---|---|---|
| `core_smoke` | `native_best`, `runtime_parity` | Fast regression and correctness checks on the `pr_smoke` subset |
| `core_comparative` | `native_best` | Broad recurring comparisons across the core scenario profile |
| `runtime_parity_core` | `runtime_parity` | Hosted/runtime-parity reporting on intentionally comparable scenarios only |
| `native_strength` | `native_best` | Native-best view for formualizer-strength and structural scenarios with explicit caveats |
| `nightly_scale` | `native_best` | Scheduled heavy runs and anchor/stress watchlists |

## Strong-Claim Rule

A scenario row is eligible for a strong public comparison only when all of the following are true:

1. correctness passes,
2. no fallback path is used unless the report discloses it,
3. scenario metadata is complete (`family`, `tier`, `profile`, comparison-profile, runtime-mode),
4. the row belongs to an approved comparison profile, and
5. the declared support policy is satisfied.

## Scenario Matrix

| Scenario | Family | Tier | Scenario profile | Comparison profiles | Runtime modes | Regression gate |
|---|---|---|---|---|---|---|
| `headline_100k_single_edit` | `incremental_locality` | `pr_smoke` | `core` | `core_smoke`, `core_comparative`, `runtime_parity_core` | `native_best`, `runtime_parity` | yes |
| `chain_100k` | `chain_topology` | `pr_smoke` | `core` | `core_smoke`, `core_comparative`, `runtime_parity_core` | `native_best`, `runtime_parity` | yes |
| `fanout_100k` | `chain_topology` | `comparative` | `core` | `core_comparative`, `runtime_parity_core` | `native_best`, `runtime_parity` | no |
| `cross_sheet_mesh` | `chain_topology` | `comparative` | `core` | `core_comparative`, `runtime_parity_core` | `native_best`, `runtime_parity` | no |
| `inc_sparse_dirty_region_1m` | `incremental_locality` | `nightly_heavy` | `core` | `nightly_scale` | `native_best` | no |
| `inc_cross_sheet_mesh_3x25k` | `incremental_locality` | `comparative` | `core` | `core_comparative`, `runtime_parity_core` | `native_best`, `runtime_parity` | no |
| `lookup_index_match_dense_50k` | `lookup_dimension_join` | `comparative` | `core` | `core_comparative`, `runtime_parity_core` | `native_best`, `runtime_parity` | no |
| `lookup_cross_sheet_dim_fact` | `lookup_dimension_join` | `comparative` | `core` | `core_comparative`, `runtime_parity_core` | `native_best`, `runtime_parity` | no |
| `sparse_whole_column_refs` | `nightly_stress` | `nightly_heavy` | `formualizer_strength` | `native_strength`, `nightly_scale` | `native_best` | no |
| `sumifs_fact_table_100k` | `aggregate_analytics` | `comparative` | `formualizer_strength` | `native_strength` | `native_best` | no |
| `agg_countifs_multi_criteria_100k` | `aggregate_analytics` | `comparative` | `core` | `core_comparative`, `runtime_parity_core` | `native_best`, `runtime_parity` | yes |
| `agg_mixed_rollup_grid_2k_reports` | `aggregate_analytics` | `comparative` | `core` | `core_comparative` | `native_best` | no |
| `struct_row_insert_middle_50k_refs` | `structural_edit` | `comparative` | `formualizer_strength` | `native_strength` | `native_best` | no |
| `struct_sheet_rename_rebind` | `structural_edit` | `comparative` | `formualizer_strength` | `native_strength` | `native_best` | no |
| `structural_sheet_recovery` | `structural_edit` | `pr_smoke` | `formualizer_strength` | `native_strength` | `native_best` | no |
| `real_finance_model_v1` | `real_world_anchor` | `nightly_heavy` | `core` | `nightly_scale` | `native_best` | yes |
| `real_ops_model_v1` | `real_world_anchor` | `nightly_heavy` | `core` | `nightly_scale` | `native_best` | no |

## Support And Claim Matrix

| Scenario | Support policy | Claim class | Caveat labels |
|---|---|---|---|
| `headline_100k_single_edit` | `all_engines` | `claim_safe_now` | none |
| `chain_100k` | `all_engines` | `claim_safe_now` | none |
| `fanout_100k` | `all_engines` | `claim_safe_now` | none |
| `cross_sheet_mesh` | `all_engines` | `claim_safe_now` | none |
| `inc_sparse_dirty_region_1m` | `explicit_caveat` | `claim_safe_with_caveats` | `runtime_mode_difference`, `parity_corpus_constraint` |
| `inc_cross_sheet_mesh_3x25k` | `all_engines` | `claim_safe_now` | none |
| `lookup_index_match_dense_50k` | `all_engines` | `claim_safe_now` | none |
| `lookup_cross_sheet_dim_fact` | `all_engines` | `claim_safe_now` | none |
| `sparse_whole_column_refs` | `explicit_caveat` | `internal_only` | `whole_column_bias`, `runtime_mode_difference`, `parity_corpus_constraint` |
| `sumifs_fact_table_100k` | `profile_subset` | `claim_safe_with_caveats` | `whole_column_bias`, `runtime_mode_difference` |
| `agg_countifs_multi_criteria_100k` | `all_engines` | `claim_safe_now` | none |
| `agg_mixed_rollup_grid_2k_reports` | `profile_subset` | `claim_safe_with_caveats` | `runtime_mode_difference` |
| `struct_row_insert_middle_50k_refs` | `profile_subset` | `claim_safe_with_caveats` | `structural_edit_support`, `runtime_mode_difference` |
| `struct_sheet_rename_rebind` | `profile_subset` | `claim_safe_with_caveats` | `structural_edit_support`, `runtime_mode_difference` |
| `structural_sheet_recovery` | `profile_subset` | `claim_safe_with_caveats` | `structural_edit_support`, `runtime_mode_difference` |
| `real_finance_model_v1` | `explicit_caveat` | `claim_safe_with_caveats` | `real_world_anchor_scope`, `parity_corpus_constraint`, `runtime_mode_difference` |
| `real_ops_model_v1` | `explicit_caveat` | `claim_safe_with_caveats` | `real_world_anchor_scope`, `parity_corpus_constraint`, `runtime_mode_difference` |

## Nightly Scale Strategy

Nightly-scale scenarios are intentionally separated from routine comparative runs when one or more of these are true:

- workbook size or evaluation time would distort routine multi-engine cadence,
- hosted/runtime-parity execution would become misleading or operationally expensive,
- the scenario is valuable primarily as a scale watchlist or realism anchor rather than a broad parity claim.

Current nightly-scale watchlist:

- `inc_sparse_dirty_region_1m`
- `sparse_whole_column_refs`
- `real_finance_model_v1`
- `real_ops_model_v1`

## Runtime-Parity Selection

Use `runtime_parity_core` only for scenarios tagged with `runtime_modes: [native_best, runtime_parity]`.

`native_best_cached_plan` is an optional formualizer-native analysis mode for stable-topology plan-reuse experiments. Treat it as an internal optimization view unless equivalent plan-reuse modes are implemented for comparison engines.

Current runtime-parity corpus:

- `headline_100k_single_edit`
- `chain_100k`
- `fanout_100k`
- `cross_sheet_mesh`
- `inc_cross_sheet_mesh_3x25k`
- `lookup_index_match_dense_50k`
- `lookup_cross_sheet_dim_fact`
- `agg_countifs_multi_criteria_100k`

Do not force these into runtime-parity reports unless they are explicitly reclassified later:

- `inc_sparse_dirty_region_1m`
- `sparse_whole_column_refs`
- `sumifs_fact_table_100k`
- `agg_mixed_rollup_grid_2k_reports`
- `struct_row_insert_middle_50k_refs`
- `struct_sheet_rename_rebind`
- `structural_sheet_recovery`
- `real_finance_model_v1`
- `real_ops_model_v1`

## Regression Gate Shortlist

The recurring regression shortlist is intentionally small and high signal:

| Scenario | Family | Why it stays on the shortlist |
|---|---|---|
| `headline_100k_single_edit` | `incremental_locality` | Baseline incremental edit latency and correctness |
| `chain_100k` | `chain_topology` | Deep dependency propagation regression detector |
| `agg_countifs_multi_criteria_100k` | `aggregate_analytics` | Repeated criteria evaluation and report-style aggregate coverage |
| `real_finance_model_v1` | `real_world_anchor` | Real workbook anchor for scheduled realism checks |

## Report Grouping And Caveats

Reports should support grouping and filtering by:

- `family`
- `tier`
- `profile`
- `mode`
- `comparison_profile`
- `claim_class`

Reports must surface at least these caveats when present:

- unsupported paths,
- fallback paths,
- correctness failures,
- parity-corpus constraints,
- runtime-mode differences,
- structural-edit-only comparison scope.

## Execution Plans

The default scheduled entry points live in `benchmarks/harness/plans.yaml`:

- `ci_formualizer_gate`
  - formualizer-only
  - fast smoke coverage for default CI
  - runs `core_smoke` scenarios plus `structural_sheet_recovery`
- `nightly_native_compares`
  - scheduled native-best compare plan
  - runs `core_comparative` across `formualizer_rust_native`, `ironcalc_rust_native`, and `hyperformula_node`
  - runs `native_strength` across `formualizer_rust_native` and `ironcalc_rust_native`
  - runs `nightly_scale` on `formualizer_rust_native` as a heavy realism/watchlist lane

Plan runs emit:

- raw JSON rows into `benchmarks/harness/results/raw/`
- a plan-scoped markdown summary into `benchmarks/harness/results/reports/`
- a small JSON manifest describing the executed plan run

## Validation And Reporting Commands

From repository root:

```bash
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py validate-suite
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py validate-plans
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py run-plan --plan ci_formualizer_gate
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py run-plan --plan nightly_native_compares --dry-run
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py report --group-by family,tier
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py report --comparison-profile runtime_parity_core --mode runtime_parity
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py report --regression-gate --group-by family,mode
```
