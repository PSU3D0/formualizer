# Benchmarks

This directory contains benchmark suite inputs and harness docs.

## Layout

- `scenarios.yaml` — canonical benchmark scenarios
- `function_matrix.yaml` — feature/function support matrix per scenario
- `corpus/` — generated and curated `.xlsx` benchmark artifacts
- `expected/` — expected outputs for verification checks
- `harness/` — runner/adapters documentation and implementation notes

## Generate synthetic corpus

From repository root:

```bash
cargo run -p formualizer-bench-core --features xlsx --bin generate-corpus -- \
  --scenarios benchmarks/scenarios.yaml
```

Optional filters:

```bash
cargo run -p formualizer-bench-core --features xlsx --bin generate-corpus -- \
  --scenarios benchmarks/scenarios.yaml \
  --only headline_100k_single_edit --only chain_100k
```

## Notable scenarios

- `inc_sparse_dirty_region_1m` exercises tiny dirty-region edits across a 1M-row sparse sheet span.
- `inc_cross_sheet_mesh_3x25k` exercises selective cross-sheet propagation across three 25k-row sheets.

## Design split

- Rust-native contract and corpus tooling live in `crates/formualizer-bench-core`.
- Reusable fixture generation helpers live in `crates/formualizer-testkit`.
- Polyglot comparative runners (Python/Node/Rust adapters) should be rooted in `benchmarks/harness`.
