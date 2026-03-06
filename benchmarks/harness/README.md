# Benchmark Harness

Comparative benchmark runner for Formualizer and external engines.

## Current status

- `formualizer_rust_native` adapter is wired to a Rust binary runner (`run-formualizer-native` in `crates/formualizer-bench-core`).
- Other adapters are scaffolded and currently return `not_implemented` or partial support results.
- Metadata governance for grouping, claim safety, and caveat policy lives in `benchmarks/scenarios.yaml`, `benchmarks/function_matrix.yaml`, and `benchmarks/reporting.md`.

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

# Run one scenario via formualizer native adapter
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py run \
  --engine formualizer_rust_native \
  --scenario headline_100k_single_edit \
  --mode native_best

# Build markdown summary grouped by family/tier
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py report \
  --group-by family,tier

# Build runtime-parity-only markdown summary
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py report \
  --comparison-profile runtime_parity_core \
  --mode runtime_parity
```

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
