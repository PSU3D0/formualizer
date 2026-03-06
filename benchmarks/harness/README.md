# Benchmark Harness

Comparative benchmark runner for Formualizer and external engines.

## Current status

- `formualizer_rust_native` adapter is wired to a Rust binary runner
  (`run-formualizer-native` in `crates/formualizer-bench-core`).
- Other adapters are scaffolded and return `not_implemented`.

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

# Run one scenario via formualizer native adapter
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py run \
  --engine formualizer_rust_native \
  --scenario headline_100k_single_edit \
  --mode native_best

# Build markdown summary from raw results
uv run --project benchmarks/harness python benchmarks/harness/runner/main.py report
```

## Canonical contracts

- `../scenarios.yaml`
- `../function_matrix.yaml`
- `crates/formualizer-bench-core` types
