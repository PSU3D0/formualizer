# README animation source (GIF and MP4)

`render.py` preserves the original deterministic frame renderer behind `assets/formualizer-calc.gif`. It produces both GIF and H.264 MP4 from the same 960×480, 25 fps, 11.6-second illustration. This is **not a real-time recording of the engine**.

The end card reads the measured native first-calculation and edit medians from [`benchmarks/results/2026-10-25-native-financial-model.json`](../../benchmarks/results/2026-10-25-native-financial-model.json): **0.26 s** and **48 ms**. Loading is excluded; the [benchmark report](../../benchmarks/2026-10-25-native-financial-model.md) supplies the full timing boundary and qualifications. Do not rescale the animation timeline to imply a measured playback speed.

## Dependencies and fonts

Install `uv` and `ffmpeg` (including libx264). Python image/font dependencies are in the script's inline metadata and companion uv lockfile.

Font binaries are **not bundled or automatically downloaded**. Obtain and use them under their respective licenses:

- JetBrains Mono: [official source and license](https://github.com/JetBrains/JetBrainsMono). Provide a WOFF2 file named `JetBrainsMono.woff2`.
- Satoshi Variable: [official Fontshare distribution](https://www.fontshare.com/fonts/satoshi). Provide a WOFF2 file named `Satoshi-Variable.woff2` and observe its license.

Put both in a local directory and pass `--font-dir`, or set `FORMUALIZER_ANIMATION_FONTS`. The renderer converts them into ignored TTF build files; supplied font changes are reconverted instead of silently using a stale cache. Different font/encoder versions can change pixels or encoded bytes. The original rendering inputs had these SHA-256 hashes, recorded for exact-input identification, not as permission to redistribute font files:

```text
1e06740a02a443fb7f3eeda8fcaa685a0f6c620e3f01e6666e847295469ce3ad  JetBrainsMono.woff2
e739aff9b4d02c264341d6d4872edcda28e79373aeda936f659566a1cd3eb47f  Satoshi-Variable.woff2
```

## Rebuild from the repository root

```sh
uv run --locked assets/animation/render.py --font-dir /path/to/fonts --jobs 12
```

Outputs (ignored build artifacts):

- `assets/animation/out/formualizer-calc.gif`
- `assets/animation/out/formualizer-calc.mp4`
- `assets/animation/stills/*.png`
- `assets/animation/build/` intermediate frames/fonts/palette

After visually reviewing the stills and animation, update the tracked README asset explicitly:

```sh
cp assets/animation/out/formualizer-calc.gif assets/formualizer-calc.gif
```

For a quick preview without encoding:

```sh
uv run --locked assets/animation/render.py --font-dir /path/to/fonts --stills-only
uv run --locked assets/animation/render.py --font-dir /path/to/fonts --at 10.2
```

The default ffmpeg encoder uses palettegen/paletteuse and variable frame duration during the fast scroll. `--gifski` is optional and can leave ghosts in the final fade; it requires a separately installed gifski. MP4 and temporary frames are not committed merely to preserve the setup. Keep historical benchmark JSON immutable; a future new benchmark should point the renderer at its new, reviewed dataset rather than changing old measurements.
