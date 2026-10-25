# README charts and animation

## Historical LibreOffice comparison chart

The chart source, displayed data and labels are tracked in [`render-benchmark-charts.py`](render-benchmark-charts.py). It rebuilds the dark/light PNGs used by the root README:

```sh
uv run assets/render-benchmark-charts.py
```

The script declares Matplotlib 3.10.8 in inline dependency metadata. It writes `benchmark-load-calculate-dark.png` and `benchmark-load-calculate-light.png` next to itself. The rows are saved **historical Formualizer 0.10 versus LibreOffice Calc 24.2** measurements, documented in [the versioned report](../benchmarks/0.10-vs-0.9.3.md#libreoffice-calc). This redraws saved data; it does not launch LibreOffice or acquire new measurements. Never multiply these rows by a speedup from a different workload and relabel the chart as current results.

## Illustrative GIF / video

The full frame renderer and GIF/MP4 encoding setup are now tracked under [`animation/`](animation/README.md). Its end-card statistics come from the [current single-model native benchmark JSON](../benchmarks/results/2026-10-25-native-financial-model.json), not the historical LibreOffice data. Font setup/licensing, dependency locking, stills, render flags and the explicit copy step for the tracked GIF are documented there.

The images/video illustrate formula families and incremental recalculation; they are not benchmark screen recordings or runtime guarantees. The only current measured headline is tied to its exact native workload, commit and report.
