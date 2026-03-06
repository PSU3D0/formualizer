# formualizer-bench-core

Shared benchmark suite contract types for scenario definitions and benchmark result records.

This crate is intentionally lightweight and runtime-agnostic so it can be used by:
- Rust benchmark runners
- Python/Node adapters via JSON/YAML schema interchange
- CI report tooling

## Corpus generator

The crate includes a corpus generation binary:

```bash
cargo run -p formualizer-bench-core --features xlsx --bin generate-corpus -- \
  --scenarios benchmarks/scenarios.yaml
```
