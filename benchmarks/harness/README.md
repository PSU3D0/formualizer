# Benchmark Harness

Comparative benchmark runner for Formualizer and external engines.

## Current status

- `formualizer_rust_native` adapter is wired to a Rust binary runner (`run-formualizer-native` in `crates/formualizer-bench-core`).
- Other adapters are scaffolded and currently return `not_implemented` or partial support results.
- Metadata governance for grouping, claim safety, and caveat policy lives in `benchmarks/scenarios.yaml`, `benchmarks/function_matrix.yaml`, and `benchmarks/reporting.md`.
- Execution plans for CI and nightly runs live in `benchmarks/harness/plans.yaml`.

## Local refs for external engines (ignored)

External sources are cloned under:

- `benchmarks/harness/ref-libs/IronCalc`
- `benchmarks/harness/ref-libs/hyperformula`

`ref-libs/` is gitignored to avoid vendoring external code/license payload into this repo.

## Commands

From repository root:

```bash
# Generate benchmark corpus files first
cargo run -p formualizer-bench-core --features xlsx --bin generate-corpus -- \
  --scenarios benchmarks/scenarios.yaml

# List configured engine IDs
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py list-engines

# Validate scenario/reporting governance metadata
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py validate-suite

# List and validate YAML-defined execution plans
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py list-plans
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py validate-plans

# Run one scenario via formualizer native adapter
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py run \
  --engine formualizer_rust_native \
  --scenario headline_100k_single_edit \
  --mode native_best

# Run the default formualizer-only CI gate
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py run-plan \
  --plan ci_formualizer_gate

# Run the scheduled nightly native compare plan
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py run-plan \
  --plan nightly_native_compares

# Preview a plan without executing it
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py run-plan \
  --plan nightly_native_compares \
  --dry-run

# Build markdown summary grouped by family/tier
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py report \
  --group-by family,tier

# Build runtime-parity-only markdown summary
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py report \
  --comparison-profile runtime_parity_core \
  --mode runtime_parity
```

## Default execution plans

- `ci_formualizer_gate` runs a fast formualizer-native-only gate over the `core_smoke` subset plus `structural_sheet_recovery`.
- `nightly_native_compares` runs native-best scheduled comparisons across:
  - `core_comparative` on `formualizer_rust_native`, `ironcalc_rust_native`, and `hyperformula_node`
  - `native_strength` on `formualizer_rust_native` and `ironcalc_rust_native`
  - `nightly_scale` on `formualizer_rust_native`
- Plan runs write per-run raw JSON under `results/raw/` and plan-scoped markdown/manifest files under `results/reports/`.
- Raw result filenames now include `mode` so native-best and runtime-parity runs can coexist safely.

## Report semantics

- Grouping/filtering keys: `family`, `tier`, `profile`, `mode`, `comparison_profile`, `claim_class`.
- Report rows inherit support policy and claim class from `benchmarks/function_matrix.yaml`.
- Result-derived caveats add labels for fallback paths, unsupported paths, execution failures, and correctness failures.
- Strong public claims should only use rows that satisfy the claim-safety rules in `benchmarks/reporting.md`.

## Canonical contracts

- `../scenarios.yaml`
- `../function_matrix.yaml`
- `../reporting.md`
- `crates/formualizer-bench-core` types
