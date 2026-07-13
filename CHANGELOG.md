# Changelog

All notable changes to Formualizer will be documented in this file.

## Unreleased

### Added

- Added backend-neutral source-family ingest with anchor-once FormulaPlane authority for proven complete domains. Calamine supplies bounded XLSX evidence and exact replay for eager and deferred loading, with structural-edit, cycle-demotion, and source-family telemetry support.
- Added registry-owned function semantic contracts so safe current and future ordinary functions can use FormulaPlane authority without a secondary supported-name list, while exceptional and untrusted functions continue to replay conservatively.
- Added bounded fragmented shared-formula evidence, exact coordinate-disposition replay, one-analysis Shadow preparation, and typed ordinary-exception ownership as the replay-only foundation for transactional fragmented authority.

### Improved

- Upgraded Calamine-backed XLSX loading to Calamine 0.36 and a single-pass value/formula metadata stream, preserving formula-only worksheet dimensions, cached-value semantics, load limits, shared-formula relocation, and malformed-family fallback.

### Performance

- Proven complete source-formula families now parse and analyze one anchor and avoid per-descendant strings, ASTs, staging entries, and graph vertices. In same-machine release probes, a clean 100k-family load improved from 997 ms and 313 MiB RSS under forced replay to 129 ms and 26 MiB RSS; a 1M-family load completed in 2.2 s at 167 MiB RSS instead of 13.1 s at 3.0 GiB.

## [0.7.1] - 2026-07-02

### Fixed

- INDEX and OFFSET now clamp unbounded whole-column/whole-row range arguments (`B:B`, `2:2`, `Data!$A:$C`) to the sheet's used region instead of returning `#REF!`, restoring the common `INDEX(range, MATCH(...), MATCH(...))` lookup pattern. (#162, #163)
- INDEX supports `row_num`/`column_num` of 0 to return the entire column or row, matching Excel, in both the reference and array-constant paths. (#156)
- FIND and SEARCH index by character rather than byte, fixing incorrect positions and a panic on multi-byte UTF-8 text (e.g. `SEARCH("?z","éz")`). (#153)
- TEXT returns non-numeric text unchanged instead of `#VALUE!`, matching Excel; locale-ambiguous numeric-looking strings such as `"1.234,56"` still error. (#155)
- SUMIFS/COUNTIFS/SUMIF `<>text` criteria now match blank cells, matching Excel, with whole-column and edge-case coverage. (#160, #161)
- INDIRECT resolves defined names and tables when `a1_style` is FALSE. (#154)

### Performance

- Release builds (crates, Python wheels, npm/WASM packages) now compile with fat LTO and `codegen-units = 1` for smaller, faster artifacts. (#19, #20)

## [0.7.0] - 2026-06-12

### Breaking changes

- Added the `Iterate` variant to the public `CyclePolicy` enum; downstream code matching `CyclePolicy` exhaustively must add an arm. (#130)

### Added

- Added Excel-style iterative calculation: `CyclePolicy::Iterate` evaluates intentional circular references with configurable max-iteration and convergence-threshold settings, built on runtime SCC cycle detection via live-edge iteration so only genuinely cyclic cells iterate and short-circuited branches never create false cycles. (#118, #119, #130)
- Added XLSX `calcPr` round-trip so workbooks authored with iterative calculation enabled in Excel load and save with the same cycle configuration, plus Python and WASM/JS cycle-configuration surfaces. (#131)
- Added WASM cycle configuration on the plain `new Workbook(options)` constructor and a `lastCycleTelemetry()` accessor exposing iteration, convergence, and cycle-outcome telemetry for the most recent evaluation. (#138)
- Added FormulaPlane named-range support: formulas referencing defined names with concrete cell or range definitions now canonicalize, fingerprint, and evaluate as spans; names resolve per cell at projection time with the same scope/shadowing semantics as legacy evaluation, and define/update/delete of a name invalidates affected spans. (#147)
- Added FormulaPlane mixed-anchor range support so tail reads (e.g. `$A2:$A$100`) and running totals (e.g. `$B$2:$B2`) evaluate as spans with placement-precise dirty projection. (#145)
- Added MIT and Apache-2.0 license files to the repository.

### Improved

- Improved cycle evaluation infrastructure with condensation-ordered schedule units, per-SCC cycle outcomes, and a live-edge collector with lazy `SHORT_CIRCUIT` dispatch. (#116, #117, #118)
- Excluded FormulaPlane span members from static cycle detection so span-covered families do not produce spurious cycle verdicts. (#121)

### Performance

- Batched cell edits now run one multi-source dirty propagation per bulk operation instead of one per cell, making large `write_range`/`set_values` calls up to ~270x faster (15.9 s to 59 ms for a 20k-cell batch with changelog off). (#139)
- Changelog old-state capture is recorded directly at edit time instead of patched by an O(N²) reverse scan, reducing a 20k-cell batch with changelog on from 506 ms to 112 ms (combined with #139, ~144x end to end). (#140)
- Hot-path improvements from profiling: iterative Tarjan SCC (no recursion-depth limits), fixed exponentially repeated dispatch on deeply nested expressions, multi-source dirty marking, and aggregate infinity sanitization. (#136)
- Amortized CSR edge rebuilds across per-cell formula edits instead of rebuilding per edit. (#127)
- Recorded per-cell staged-formula deltas in the changelog instead of whole-sheet snapshots. (#128)
- Stored schedule units as indices instead of cloned layers. (#117)
- `IFERROR`/`IFNA` now short-circuit (the fallback branch is not evaluated when the value is clean), `SEQUENCE`/`RANDARRAY` reject array shapes beyond Excel grid limits before allocating, and order-statistic functions (`LARGE`, `SMALL`, `MEDIAN`, `PERCENTILE.*`, `QUARTILE.*`) use quickselect instead of full sorts (~12x on large ranges). (#141)
- Fixed a FormulaPlane mixed-mode interaction (degenerate rectangle routing and a repeated demote spin) that made authoritative evaluation slower than Off on mixed workbooks; the mixed corpus improved from 996 ms to 26 ms, faster than Off. (#143)
- Linearized the FormulaPlane reject path with O(1) candidate mapping on family rejection, reducing the ingest penalty of a 50k-cell dependent chain from 863 ms to 112 ms. (#146)

### Fixed

- Fixed unqualified references in cross-sheet formula contexts leaking to the default sheet. (#110, #114)
- Fixed spill projections and region locks not being torn down when a cycle stamps `#CIRC!`. (#115)
- Fixed whole-axis and stripe self-inclusion not being detected as circular references. (#129)
- Fixed `CycleTelemetry` not being populated on the workbook/Arrow evaluate path. (#124)
- Fixed two iterative-calculation edge-case bugs found by corpus testing, and hardened the pre-ship surface with per-cycle clock snapshots, Python telemetry, and persistence pins. (#134, #135)
- Fixed a wasm32 panic from clock access during evaluation under the JS runtime. (#138)
- Fixed the Umya XLSX loader registering defined names after eager formula ingest, which prevented named formulas from resolving at ingest time. (#147)

### Tooling and quality

- Added a property-test oracle evaluating random guarded workbooks against a reference lazy interpreter. (#122)
- Added standing SCC cost-model probes (phantom pairs, iterate workloads) and an iterative-calculation edge-case corpus. (#123, #132, #135)
- Added the FormulaPlane span-coverage corpus, `probe-fp-coverage`, and generator-driven pinning tests as the standing coverage measurement for fingerprint expansions. (#142)
- Added the adaptive formula partition architecture document describing the unified evaluation end state. (#148)
- Added cycle detection and iterative calculation guides, reference pages, and interactive sandboxes to the docs site. (#137)

## [0.6.0] - 2026-06-03

### Breaking changes

- Consolidated parser implementations by removing the legacy token-vector parser and making the source-span parser the public `Parser`; consumers relying on legacy parser internals should update to the canonical parser APIs. (#104)

### Added

- Added experimental opt-in FormulaPlane span evaluation for large copied-formula families. The default workbook path remains the stable dependency graph; span evaluation must be enabled explicitly through Rust, Python, WASM/JS, or C FFI configuration.
- Added sparse initial ingest paths for JSON, Umya, and Calamine loaders to avoid materializing formatting-only worksheet extent as populated cells.
- Added publishable Calamine-backed XLSX loading improvements that preserve sparse-friendly engine ingest while remaining compatible with the crates.io Calamine API.
- Added benchmark corpus tooling and structural invariants for Off/Auth parity, backend comparison, and FormulaPlane promotion metrics.
- Added idiomatic parser APIs including `Parser::new`, builder-based construction, `TokenStream` parsing, `FromStr`, and `TryFrom` conversions for `Parser` and `ASTNode`. (#104)
- Added 28 worksheet functions across engineering, info/reference, lookup/array-shape, and text categories, including Bessel functions, `FORMULATEXT`, `SHEET`, `SHEETS`, `ISREF`, `TOCOL`, `TOROW`, and byte-oriented text functions. (#101)

### Improved

- Improved FormulaPlane promotion and evaluation for arithmetic, lookup, criteria aggregate, whole-axis, cross-sheet, and affine literal formula families.
- Improved structural edit handling for promoted spans, including row/column insert/delete shifting, bounded dirty projection, and conservative demotion when required.
- Reduced FormulaPlane memory usage for integer-like affine literal families by encoding literal bindings compactly instead of retaining one dictionary entry per placement.
- Kept FormulaPlane structural demotion linear by pre-creating direct-dependency placeholder vertices before batched edge insertion and by clearing computed overlays by range instead of one cell at a time.
- Optimized holiday handling in `NETWORKDAYS`, `WORKDAY`, and their `.INTL` variants by deduplicating holidays once and using binary search during date loops. (#102)
- Aligned direct XLSX helper dependencies with the newer Calamine/`zip` stack where possible.

### Fixed

- Preserved default stable semantics by keeping FormulaPlane/span evaluation disabled unless explicitly requested.
- Preserved Off/Auth parity across the validated benchmark corpus while falling back to the legacy graph for unsupported span shapes.
- Fixed parser handling for leading empty function arguments such as `=FOO(,A1:C3)`, preserving the intended empty-argument arity. (#103, #104)
- Hardened FormulaPlane sheet lifecycle operations so add/remove/duplicate/rename operations preserve unrelated spans, demote only affected spans, reject unbounded references to unknown sheets without creating phantom sheets, and avoid region-index panics or iterator overflow. (#105)
- Fixed deferred graph-building evaluation so `evaluate_cell` and `evaluate_cells` drain all staged sheets before demand evaluation, preventing cross-sheet references to staged formula cells from resolving as `None`. (#106)
- Fixed date functions to coerce `Date` and `DateTime` cells through the common lenient numeric path, and corrected `EDATE`/`EOMONTH` negative-month year-boundary handling. (#107)
- Fixed named-range incremental evaluation by walking through `Named`/`Range` pass-through vertices in demand subgraphs and preserving named-range edges through CSR rebuilds. (#108)

### Security and hardening

- Updated lockfiles to pick up patched `openssl`, `thin-vec`, and `tmp` versions, clearing high-severity Dependabot alerts in Rust and benchmark harness dependencies.

### Known limitations

- FormulaPlane span evaluation remains experimental and opt-in in this release.
- Internal dependency chains such as running balances and cumulative schedules remain on the legacy dependency graph.
- Array-literal formula families are not span-promoted.
- Calamine-backed structured table metadata is still incomplete for some table-reference workloads; Umya remains the fuller XLSX compatibility path for those cases.
- Calamine formula-record streaming is deferred until the upstream API is available in a crates.io release.

## [0.5.9] - 2026-05-18

### Fixed

- Treated unary `+` as a pass-through (identity) operator to match Excel/LibreOffice semantics. Previously, `=+A1` returned `#VALUE!` when `A1` held a non-numeric string such as `"2014F"`; the leading-`=+` idiom is common in finance models carried over from Lotus 1-2-3 and now preserves text, booleans, and other non-numeric operand types. Unary `-` and `%` retain their numeric-coercion semantics. (#100)
- Preserved computed-overlay accounting when edits remove previously computed values, preventing stale overlay memory estimates and keeping later recalc flushes consistent. (#95)

### Performance

- Improved computed formula overlay flushing by buffering formula-result writes and coalescing them into sparse, dense, or run-length overlay fragments instead of emitting every result as an individual point write; narrow layers now use the direct point-write path so deep chains do not pay coalescing overhead when there is nothing to coalesce. In local `v0.5.8` → 0.5.9-candidate A/B runs with a 20 GiB process memory cap, `headline_100k_single_edit` incremental recalc improved from 22.01 ms to 6.89 ms (3.19x), `agg_countifs_multi_criteria_100k` incremental recalc improved from 9.80 ms to 8.35 ms (1.17x), and a 50k-row finance repeated-edit probe improved total recalc from 223.83 ms to 170.75 ms (1.31x) with flat peak RSS. The adversarial `chain_100k` watchlist scenario is much closer to baseline after the narrow-layer fast path (57.58 ms to 63.23 ms, 0.91x). (#95)
- Added finance-shaped recalc probes and computed-overlay observability coverage for dense, sparse, and run-length formula-result flush patterns.

### Changed

- Bumped Arrow dependencies from the 56.x series to `58.2.0` and Wasmtime from `42.0.2` to `43.0.2`. (#95, #97)
- Bumped the docs site to Next.js `16.2.6` to pick up current security fixes.

### Tooling and quality

- Hardened the WASM CI path with explicit portable-wasm and wasm-js profile checks, artifact import validation, and Node.js 24 for npm release builds.
- Refreshed Python development dependencies, including `pytest` `9.0.3`.

## [0.5.8] - 2026-04-27

### Breaking changes

- Bumped the parser SDK track to `2.0.0` because parser AST enums now expose additional variants for new Excel syntax, including LAMBDA immediate-invocation calls and 3D sheet references. Consumers that exhaustively match parser AST enums may need to handle the new cases.

### Added

- Added parser support for Excel reference operators, including `:` range composition and space intersection, with precedence coverage and pretty-printer round-trips. (#69)
- Added parser support for 3D sheet-range references such as `Sheet1:Sheet3!A1` and `Sheet1:Sheet3!A1:B2`. (#70)
- Added parser support for dynamic-array spill postfix references such as `A1#`. (#71)
- Added parser support for real structured/table reference parsing, including special items, column ranges, escapes, Unicode column names, and display round-trips. (#73)
- Added parser support for LAMBDA immediate invocation syntax such as `LAMBDA(x, x + 1)(2)`. (#68)
- Added a differential harness that compares the classic token parser and canonical span parser and documents remaining parser-front-end divergence. (#77)

### Fixed

- Accepted lowercase and mixed-case boolean literals such as `true` and `fAlSe` without misclassifying longer named ranges. (#72)
- Tightened scientific-notation tokenization so incomplete exponent forms no longer consume following operators or references. (#78)
- Preserved pending `A1:` prefixes before double-quoted strings instead of silently discarding them. (#79)
- Preserved error kind for sheet-qualified error literals and accepted lowercase sheet-qualified error literals. (#74)
- Recognized modern Excel `#SPILL!` and `#CALC!` error literals. (#75)
- Prevented R1C1-shaped inputs from being misclassified as structured table references while preserving valid A1 references such as `R1`. (#76)

### Tooling and quality

- Excluded Pyodide/Emscripten wheels from PyPI uploads while continuing to build and smoke-test them in release workflows.

## [0.5.7] - 2026-04-26

### Fixed

- Fixed unary minus precedence to bind tighter than exponentiation, matching Excel semantics (`=-2^2` now evaluates to `4` instead of `-4`). (#65)

### Performance

- Fixed O(N²) bulk-ingest scaling for row-major formulas by introducing `CoordBuildHasher` for packed coordinate keys and applying it to the hot dependency-graph and spill-commit maps. (#67)

## [0.5.6] - 2026-04-14

### Fixed

- Raised the default workbook logical-cell ingest budget from `8_000_000` to `128_000_000`, allowing much larger dense workbooks to load through the existing `load_workbook(...)`, `Workbook.load_path(...)`, and `recalculate_file(...)` paths while keeping row, column, and sparse-sheet guardrails in place. (#57)

## [0.5.5] - 2026-04-13

### Security and hardening

- Hardened native Wasmtime-backed plugins by enforcing fuel and memory budgets, revoking cached modules on unregister, capping guest ABI payload sizing, and bumping `wasmtime` to `42.0.2` to clear the current security advisories. (#42, #44, #51)
- Added workbook ingest guardrails for oversized logical sheets and extreme sparse-sheet ratios across JSON, Calamine, and Umya loaders. (#47)
- Hardened workbook coordinate validation across Python and wasm bindings so zero-based or non-positive coordinates are rejected consistently. (#45)

### Fixed

- Fixed `SheetPort` evaluation overrides leaking after invalid deterministic-mode requests and made staged input writes atomic across multi-port and range updates. (#43, #46)
- Restored whole/open-ended range dependency scheduling for far-formula rows and dynamic `INDIRECT` consumers, improving recalculation correctness for compressed and open-ended ranges. (#48)
- Fixed `INDEX` over single-row references so two-argument calls like `INDEX(A1:C1, 2)` resolve horizontally and match Excel/Python SDK expectations. (#50)

### Tooling and quality

- Bumped `next` in the docs site to `16.2.3` to resolve the remaining product-track Dependabot alert. (#52)

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

[Unreleased]: https://github.com/PSU3D0/formualizer/compare/v0.7.1...HEAD
[0.7.1]: https://github.com/PSU3D0/formualizer/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/PSU3D0/formualizer/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/PSU3D0/formualizer/compare/v0.5.9...v0.6.0
[0.5.9]: https://github.com/PSU3D0/formualizer/compare/v0.5.8...v0.5.9
[0.5.8]: https://github.com/PSU3D0/formualizer/compare/v0.5.7...v0.5.8
[0.5.7]: https://github.com/PSU3D0/formualizer/compare/v0.5.6...v0.5.7
[0.5.6]: https://github.com/PSU3D0/formualizer/compare/v0.5.5...v0.5.6
[0.5.5]: https://github.com/PSU3D0/formualizer/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/PSU3D0/formualizer/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/PSU3D0/formualizer/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/PSU3D0/formualizer/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/PSU3D0/formualizer/compare/v0.4.4...v0.5.1
[0.5.0]: https://github.com/PSU3D0/formualizer/releases/tag/v0.5.0
