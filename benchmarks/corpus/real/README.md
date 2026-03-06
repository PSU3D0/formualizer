# Real-World Anchor Corpus

These benchmark anchors are deterministic, generated in-repo fixtures.
They are intended to feel like realistic workbook models without vendoring
third-party or proprietary XLSX files.

Licensing / redistributability:
- No external workbook content is committed here.
- The `.xlsx` artifacts are generated from repository code via
  `crates/formualizer-bench-core/src/bin/generate-corpus.rs`.
- Expected verification values live under `benchmarks/expected/`.

Anchors:
- `real_finance_model_v1` models a finance forecast with editable assumptions,
  segment-driven pricing, annual rollups, and a debt/cash schedule.
- `real_ops_model_v1` models a service operations workbook with queue and
  priority lookups, work-order facts, and dashboard staffing/margin rollups.

Generate only the anchors:

```bash
cargo run -p formualizer-bench-core --features xlsx --bin generate-corpus -- \
  --scenarios benchmarks/scenarios.yaml \
  --only real_finance_model_v1 \
  --only real_ops_model_v1
```
