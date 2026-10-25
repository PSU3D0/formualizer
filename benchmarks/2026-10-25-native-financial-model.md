# Million-formula native benchmark — 2026-10-25

Formualizer at merged commit [`d1bbae03`](https://github.com/PSU3D0/formualizer/commit/d1bbae0339406a746d534c85be0a9f548525cc78) calculated the existing **1,000,000-formula financial model in 0.262 seconds median**, after loading. That is **31% less time / 1.45× faster than the historical 0.38-second headline**. Loading took 2.58 seconds, separately.

These are three fresh native Rust processes on a shared 24-logical-CPU AMD Ryzen 9 3900XT Linux host. This is not a new LibreOffice comparison, a same-day old/new A/B, or a browser/Python-call performance guarantee. The same engine is available through Rust, Python and JavaScript, but host/runtime and workbook configuration affect timings.

## Measurements

| Metric | Sample 1 | Sample 2 | Sample 3 | Median |
|---|---:|---:|---:|---:|
| Load | 2.869 s | 2.254 s | 2.581 s | **2.581 s** |
| First calculation, excluding load | 275.650 ms | 210.318 ms | 262.440 ms | **262.440 ms** |
| Value edit + recalc, per-process p50 | 47.028 ms | 48.388 ms | 48.974 ms | **48.388 ms** |

The per-process p50 edit figures above are summarized by their median; they are not the first-calculation metric. The historical edit result was 52 ms, a small difference on this shared host, not a strong new improvement claim. See [the exact machine-readable samples](results/2026-10-25-native-financial-model.json) for all values and ranges.

All three runs successfully computed **1,000,000 formulas**, with zero parser rejections and zero value/formula-edit errors. First and final result digests were stable. The harness digest rounds numbers to 12 significant digits, so this is not a claim of bitwise equality with every historical result.

The original 0.10 [report](0.10-vs-0.9.3.md) remains historical evidence. Its 4.2 s load / 0.38 s first-calculation figures have not been rewritten. Current load time is about 39% lower than that historical figure, but neither comparison isolates individual changes or compiler effects.

## Timing boundary

The existing `program1-perf` harness uses the public Workbook API with `LoadStrategy::EagerAll` and **ephemeral** configuration. Load and first `evaluate_all` are timed separately. There is no `--prebuild` or `--presync` moving work out of calculation. Allocation counting is disabled for wall-clock measurements because the counting allocator contends between evaluator threads. Each observation is a fresh process; the harness's untimed target pre-read and value digests remain outside the timed boundaries, as in the historical run.

The harness still spells its adapter `CalamineAdapter`; on this commit that name is an alias for the native reader, not the retired Calamine parser. Default parallel evaluation was used, without single-core pinning. Rust was 1.99.0, release build, incremental disabled. No build or scheduled backup overlapped these observations. Other shared-host workloads were present; the 210–276 ms range is visible rather than hidden.

## Reproduce

```sh
# The seeded generator creates 100,000 rows with ten formulas per row.
uv run benchmarks/generate-financial-model.py --rows 100000 --output /tmp/fin_1m.xlsx

RUSTUP_TOOLCHAIN=1.99.0 CARGO_INCREMENTAL=0 cargo build --release --locked \
  -p formualizer-bench-core --features formualizer_runner --bin program1-perf

# Run this command three times, as separate processes, without a concurrent build.
target/release/program1-perf --xlsx /tmp/fin_1m.xlsx \
  --mode ephemeral --edits 20 --no-alloc-count
```

The measured original fixture was 9,184,309 bytes, SHA-256 `94823c46cd53837d45802424527cf70c7a07515e38f655449ea16dd1efae6f1c`. The preserved seed-7 generator recreates the cell workload; generated ZIP bytes can differ with producer versions and timestamps. Do not claim a regenerated file has the original hash without checking it. Build/run the measured commit for a version-matched reproduction; running newer code is a new observation.

## Memory and media

These timing runs did **not** count live allocations. Median RSS high-water was approximately 515 MiB through first calculation and 626 MiB through the complete edit sequence. Neither is comparable to the old 44.5/45 MB **live heap** figure. No new controlled historical memory saving is claimed.

The root README animation is illustrative, not a real-time benchmark recording. Its end-card figures come from the JSON above; the [animation source and setup](../assets/animation/README.md) are preserved. The separate LibreOffice chart remains labeled **Formualizer 0.10** and is rebuilt by [the chart renderer](../assets/render-benchmark-charts.py). No new LibreOffice ratio can be inferred from this single-workbook rerun.
