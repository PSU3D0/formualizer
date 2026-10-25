# formualizer-bench-core

Shared benchmark suite contract types for scenario definitions and benchmark result records.

This crate is intentionally lightweight and runtime-agnostic so it can be used by:
- Rust benchmark runners
- Python/Node adapters via JSON/YAML schema interchange
- CI report tooling

## Published headline workload

[`program1-perf`](src/bin/program1-perf.rs) is the preserved harness behind the million-formula headline. The [2026-10-25 native rerun](https://github.com/psu3d0/formualizer/blob/main/benchmarks/2026-10-25-native-financial-model.md) records **0.262 s median first calculation**, with load measured separately, raw samples and the seeded workbook generator. Use `--mode ephemeral --no-alloc-count` for that timing boundary; allocation-counted runs are not comparable wall-clock observations.

## Corpus generator

The crate includes a corpus generation binary:

```bash
cargo run -p formualizer-bench-core --features xlsx --bin generate-corpus -- \
  --scenarios benchmarks/scenarios.yaml
```
