# Evaluation C1 Exit Gate

Status: Reproducible C1b structural gate; cold calibration output is generated, not committed

C1b activates only retained mixed-topology cache limits and request-scoped topology/schedule-discovery scratch. Graph admission, materialization admission, graph/source preparation scratch, spill/overlay scratch, lookup-cache budgets, and target-driven C2+ fields remain inactive.

## Structural Gate

Run from the repository root:

```bash
mkdir -p target/c1-exit
cargo test -p formualizer-eval engine::tests::evaluation_resource_ledger -- --nocapture | tee target/c1-exit/evaluation-resource-ledger.txt
cargo test -p formualizer-eval engine::tests::evaluation_resource_observability -- --nocapture | tee target/c1-exit/evaluation-resource-observability.txt
cargo test -p formualizer-eval engine::tests::formula_plane_mixed_legacy_tail_reads -- --nocapture | tee target/c1-exit/exact-request-topology.txt
cargo test -p formualizer-eval formula_plane::scheduler::tests -- --nocapture | tee target/c1-exit/scheduler.txt
cargo test -p formualizer-eval formula_plane::producer::tests -- --nocapture | tee target/c1-exit/producer.txt
```

The gate requires Off/Shadow/authoritative value parity, explicit complete-cache-or-skip results, zero capacity materialization with candidate/edge/byte cache skips, retained span authority, typed schedule-scratch exhaustion, native-policy and memory-only repeated-pass strategy coverage, skip-streak/pass telemetry, and unchanged true-cycle demotion coverage.

## Cold Matrix

Build fixtures and capture fresh-process output outside the source tree:

```bash
mkdir -p target/c1-exit/cold
cargo run -p formualizer-bench-core --release --features formualizer_runner --bin probe-load-envelope-matrix -- \
  --preset envelope \
  --backend calamine \
  --formula-plane-mode authoritative \
  --samples 7 \
  --output-dir target/c1-exit/cold/work \
  --json-out target/c1-exit/cold/results.json \
  --markdown-out target/c1-exit/cold/summary.md
```

Run the same command with `--formula-plane-mode off` and `shadow`, using distinct output paths. Preserve `results.json`, `summary.md`, the commit SHA, `rustc -Vv`, host/WASM identity, and fixture checksums together in the external release-calibration archive. C1 exit ratification consumes those raw files; this document pins their reproducible location and command without checking machine-specific measurements into Git.
