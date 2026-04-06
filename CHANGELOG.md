# Changelog

All notable changes to Formualizer will be documented in this file.

## [Unreleased]

## [0.5.4] - 2026-04-06

### Fixed

- Fixed UTF-8-safe parsing for structured table specifiers so non-ASCII structured references no longer panic on invalid byte boundaries. (#40)
- Fixed Unicode case-insensitive matching for structured table names and headers, named ranges, database field/header matching, and exact/wildcard lookup text matching across parser, evaluator, and workbook integration paths. (#40)
- Fixed `SUMIFS` and related structured-table evaluation regressions for Unicode headers and criteria values, with new regression coverage across parser, engine, Arrow-backed evaluation, and workbook loader tests. (#40)

### Performance

- Improved text-heavy `MATCH`/`XMATCH`/`XLOOKUP` exact and wildcard scans by reusing cached lowered Arrow text lanes for view-backed searches and prepared text matchers for vector/reverse scan paths. In evaluator smoke benchmarks, this reduced lookup scan times by about `1.85x` for exact Arrow-view matches, `1.20x` for Arrow-view wildcards, `1.73x` for exact vector scans, and `3.05x` for vector wildcard scans.

## [0.5.3] - 2026-04-01

### Added

- Added explicit dual-runtime WebAssembly profiles: `portable-wasm` for raw/wasmtime-safe guests and `wasm-js` for browser/Node hosts via `wasm-bindgen`.
- Added CI validation for both wasm profiles, including a standalone portable wasm probe that inspects the final emitted `.wasm` import section to catch `wasm-bindgen`/browser regressions.

### Fixed

- Removed `wasm-bindgen`/JS runtime leakage from the portable wasm path by minimizing core chrono features, splitting ambient system clock support from the portable evaluator, and routing dynamic lookup randomness through the deterministic workbook-seeded RNG pathway.
- Preserved the browser/Node wasm story by making the JS binding crate explicitly opt into the `wasm-js` runtime profile instead of relying on incidental transitive behavior.
- Made GitHub release creation fall back gracefully to generated release notes when a `CHANGELOG.md` section for the tagged version is missing.

### Tooling and quality

- Excluded `formualizer-bench-core` from the default expensive workspace-wide clippy/test CI path so the comparative IronCalc benchmark harness no longer inflates baseline CI minutes.

## [0.5.2] - 2026-04-01

### Fixed

- Resolved all 9 open Dependabot security alerts (npm): bumped `next`, `rollup`, `picomatch`, `fumadocs-*`, and `brace-expansion` across docs-site, bindings/wasm, and benchmarks/harness.
- Enabled `formualizer-sheetport` standalone compilation for `wasm32-unknown-unknown` by removing the unconditional `umya_integration` feature and adding target-conditional `getrandom` wasm shims.
- Enabled `formualizer-eval` (and downstream `formualizer-workbook`, `formualizer-cffi`, `xtask`) compilation for `wasm32-unknown-unknown` by adding the same `getrandom` 0.2 + 0.3 wasm shims.

### Changed

- `formualizer-sheetport` no longer unconditionally enables the `umya_integration` feature on `formualizer-workbook`. Consumers needing umya support should enable the new `umya` feature on `formualizer-sheetport`.

## [0.5.1] - 2026-03-22

> Supersedes the incomplete `0.5.0` product release.

### Added

- Added pending symbolic formula healing so formulas referencing not-yet-defined names now evaluate as `#NAME?` and automatically heal when a matching workbook-scoped, sheet-scoped, or source-backed name is later created. (#33)
- Added the `RRI` financial function for equivalent rate-of-return calculations. (#25)

### Fixed

- Improved `IRR` convergence by using a two-phase solver with a Brent-method fallback, reducing `#NUM!` failures on difficult cash-flow patterns. (#24)
- Corrected `WEEKDAY`, `WEEKNUM`, and `DATEDIF("YD")` behavior by switching to serial-based date arithmetic that handles Excel's 1900 date-system quirks correctly. (#23)
- Hardened function arity validation with `min_args` checks to prevent panics when functions are called with too few arguments. (#26)
- Preserved workbook-global and sheet-local defined names across both Umya and Calamine import pathways, including correct local shadowing and same-name isolation across sheets. (#34)

### Performance

- Added recalculation plan reuse and static schedule caching for stable workbook topologies, improving repeat recalculation performance on unchanged dependency graphs. (#28)

### Tooling and quality

- Added a comparative benchmark harness with scenario plans, real-world anchors, and fairness-oriented reporting to improve performance validation and regression tracking. (#28)
- Expanded JSON-driven conformance coverage across info, logical, lookup, text, and date function families. (#32)

## [0.5.0] - 2026-03-22

- Incomplete product release due to partial publication during the release workflow. Superseded by `0.5.1`.

[Unreleased]: https://github.com/PSU3D0/formualizer/compare/v0.5.4...HEAD
[0.5.4]: https://github.com/PSU3D0/formualizer/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/PSU3D0/formualizer/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/PSU3D0/formualizer/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/PSU3D0/formualizer/compare/v0.4.4...v0.5.1
[0.5.0]: https://github.com/PSU3D0/formualizer/releases/tag/v0.5.0
