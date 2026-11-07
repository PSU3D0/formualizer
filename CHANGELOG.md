# Changelog

All notable changes to Formualizer will be documented in this file.

## [Unreleased]

- Added typed `RecalcJobError` lifecycle errors and `recalc_with` closure drivers on Engine and Workbook, with Finished, Cancelled and resumable Paused outcomes. Existing evaluation signatures are unchanged; active-job refusals now carry `ExcelErrorExtra::RecalcJobActive`.

### Fixed

- Kept the Python event loop responsive during asynchronous recalculation admission and cleanup. Concurrent drivers now refuse promptly without waiting for a running step, and cancellation during admission cleans up any job admitted by the worker.
- Contained runtime function metadata, source/table/version/provider resolver, returned range/table and clock-provider panics at their host boundaries. Planning faults decline optimization and become typed errors at executed roots; resolver errors preserve source attribution, while failed clock samples affect only clock-reading roots and remain frozen through a job. Deferred replay faults no longer poison spool locks.
- Converted native Rust function panics into typed `#VALUE!` cell errors and completeness diagnostics, including Workbook callbacks and optimized kernels. `IFERROR` may catch the value while source `requireComplete` still refuses incomplete calculation. Engine-internal unwinds now permanently refuse further evaluation and mutation; `is_poisoned()` lets hosts detect the state.
- Fixed ordinary recalculation dirtiness routing through observed dynamic reads: an `INDIRECT` into an empty cell later filled by a spill now recalculates instead of keeping its earlier value.

### Added

- Added Python recalculation jobs with immutable step reports, viewport priority and pending queries, a pausable `recalc` driver, and `evaluate_async` with cooperative task cancellation. Native steps release the GIL; Pyodide yields between in-thread steps. Active-job refusals and poisoned engines have typed exceptions, and function-panic payloads retain attribution on Python errors.
- Added engine-owned recalculation jobs with held schedules, formula progress, changed regions, viewport priority and pending queries. Explicit jobs refuse foreign evaluation requests and apply work/deadline limits across steps; cancellation preserves installed units without advancing the random epoch.

- Added `Engine::evaluate_all_until` and its `Workbook` passthrough for cooperative recalculation with soft deadlines and cancellation, preserving installed work and one clock, random epoch and diagnostics collection across calls. Results are independent of call boundaries except for iterative cycles created by edits during a pending recalculation: new cycles start from current cell values, including results installed by earlier calls; existing SCCs retain their original recalculation seeds.
- Fixed spill shrink and teardown erasing literal values written into an old spill extent; user edits now retain their own cells and block a live spill.
- Fixed removing literal spill blockers leaving anchors stuck at `#SPILL!`; only anchors waiting on the edited blocker are recalculated.

- Fixed stale dynamic readers, including `INDIRECT` into a cell populated by a later spill, by routing dirty propagation through observed reads.

### Practical workbook recalculation

- Added a workbook-aware source recalculation profile that applies saved calculation settings and preserves useful independent results when an executed feature is unsupported or unverified. The low-level strict profile remains available and is still the Rust default.
- Added explicit complete, partial and deferred outcomes, affected-cache invalidation, optional physical telemetry and portable finite limits across Rust, Python and WASM/TypeScript. Corruption, cancellation, resource exhaustion and output failures remain errors.
- Removed sparse-coordinate admission failures caused by treating declared worksheet rectangles as populated work. Actual byte, cell, formula and host-memory limits still apply.
- Fixed lazy error consumers, dynamic/name invalidation and cycle handling, plus supported array, lookup, rounding and text-format modes. Executed unsupported or unverified modes remain visible through caught errors and optimized reuse.
- Preserved source formulas and package ownership during selective cache publication. What-If table calculation, saved iterative seeds and external-data refresh are not newly implemented; partial output does not claim full Excel equivalence.
- Prepared product packages 0.14.0 with parser/common SDK 4.2.0. Published seeded-RAND streams remain target-specific. Small-workbook WASM overhead and a family linear-memory increase remain documented maintenance follow-ups.

## [0.13.0] - 2026-10-25

XLSX reading moves in-house: Rust, Python and JavaScript now use Formualizer's native reader by default, without a second production parser underneath it. Ordinary loading reads the workbook data it needs rather than decompressing every picture and attachment along the way. Source-preserving recalculation keeps its stricter whole-package integrity contract, and Umya remains available for rich document editing and export.

### Breaking changes and upgrading

- **Retired the Calamine parser and dependency.** `NativeXlsxAdapter` replaces its implementation. The Rust `CalamineAdapter` name, `backends::calamine` module path and `calamine` / `calamine_integration` feature names remain native compatibility aliases, not an alternate parser or fallback. Python `backend="calamine"` and JavaScript `{ backend: 'calamine' }` also select native. Most binding users can simply omit `backend`.
- **Changed Calamine-specific Rust error types.** The adapter's reader/stream associated error is now `IoError`. Removed `IoError::Calamine(calamine::Error)` and `From<calamine::Error>`; match `IoError::XlsxRead(XlsxReadError)` instead. Removed the adapter-only `external_link_target` accessor and test-only relocation-comparator injection hook. See [the native XLSX migration guide](docs/native-xlsx-migration.md) for replacements and retained interfaces.
- **Changed ordinary loading from whole-package payload validation to selective consumption.** A corrupt or very large unused image, embedded object or other unread payload no longer prevents value/formula loading. Required parts still undergo CRC, decoded-size and actual-output-limit checks. Ordinary loading is not a certificate of every ZIP member's integrity; strict source-preserving recalc retains its full-package checks and refusal policy.
- **Preserved supported rich-document capabilities.** Umya reading, mutable workbook operations and rich writing/export remain available where previously supported. Removing Calamine does not remove all spreadsheet dependencies. The former adapter used Calamine's XLSX reader, not its XLS implementation, so this does not withdraw an existing `.xls` integration.

### Highlights

- **Added explicit broader template recognition in parser SDK 4.1.0.** `template_lex::lex_template_for_certified_ast` recognises invariant sheet qualifiers, names, whitespace, `@` and general functions for callers that independently certify the parsed AST. It is not proof of purity or non-volatility. The existing `lex_template` retains its published 4.0 conservative subset; native source-family ingestion uses the new opt-in path without narrowing its existing recognition.
- **Selected the native XLSX reader by default** across Rust and the Python/JavaScript bindings, including the Python API used by Pyodide. It preserves loaded values/formulas, names and calculation settings, date/time behavior, cached external references and inert-sheet diagnostics. Cached links remain read-only: linked files are never refreshed or opened.
- **Added configurable ordinary XLSX limits before admission.** Rust exposes `XlsxLoadLimits` and additive native constructors; Python accepts keyword-only `load_limits`, including through the SheetPort path helper; JavaScript accepts nested `loadLimits`. Native and legacy backend aliases use the same options and checks.
- **Raised the ordinary compressed-input default to 127 MiB** (133,169,152 bytes). Expanded data has independent defaults: 128 MiB per consumed part and 256 MiB aggregate actual output. Callers can raise or lower these separately; a compressed-size allowance is not a guarantee that the expanded workbook, formulas or engine state fit in memory.

### Loading and safety

- **Skipped decompression of unread payloads**, including shadowed ZIP entries, while retaining the existing Compatible ZIP structure/index checks and consistent selected-member resolution. Images and attachments remain in the retained source; they are not expanded, decoded, executed or removed by a value/formula load.
- **Bounded actual consumed output throughout a load**, including lazy external-cache reads and real metadata rereads. Byte counts are based on emitted data, not trusted ZIP size claims. Existing worksheet buffers move to their consumers without a new retained-part cache. Source and part buffers grow geometrically within their allowance rather than requesting a new exact allocation for every chunk.
- **Kept required-part failures fatal.** CRC errors, declared/actual decoded-size mismatches, resource failures and cancellation cannot disappear into an unavailable external cache or a successful partial workbook. Existing best-effort unsupported-link/name semantics remain separate. Terminal safety failures also prevent subsequent sheet reads and bounds lookups through the same adapter.
- **Validated nested options strictly.** Unknown keys, booleans, negative/fractional/non-finite values and host-unrepresentable numbers are refused; JavaScript also rejects unsafe integers. Zero is a zero allowance, not unlimited. Explicit XLSX-only overrides are rejected on unsupported loading routes instead of being silently ignored.
- **Preserved independent limits and host constraints.** XML/cell/grid protections and engine geometry/formula-spool budgets still apply. Cancellation is cooperative; a single XML token is not a hard real-time cancellation boundary. Browser/WASM memory and address-space limits remain relevant even when a caller raises byte allowances.

### Performance

- **Reduced recalculation ingestion overhead** by removing redundant formula payload staging/publication work, streamlining worksheet scanning, and loading native values/formulas without another general-reader pass. These changes build on the recalculation work in 0.12.0. (#537–#539)
- **Removed decompression and CRC CPU work for unused ordinary-load parts.** The benefit depends on workbook contents; attachment-heavy examples are not representative of every formula/data-heavy workbook. Opaque payloads were already streamed through bounded scratch memory, so skipped expanded bytes must not be described as an equal reduction in peak RSS.
- **Kept benchmark comparisons distinct.** Reader-transition measurements against an already optimized Calamine control are neither an isolated default-switch speedup nor a total 0.12.0-to-0.13.0 improvement. Stage percentages are not additive. No universal speedup, wheel/WASM-size reduction or peak-memory saving is promised here.

### Compatibility and validation

- **Added boundary coverage** for selected versus unread corruption, Stored/Deflated decoded-size mismatches, exact/overflow output budgets, duplicate and Unicode member names, lazy-cache retries, retained source handles and current-position readers. Required-part checks do not depend on auditing unread content first.
- **Expanded binding coverage** for configuration validation, native aliases, Python forwarding/stubs and actual WASM/TypeScript loading. Native-versus-legacy-alias tests check API compatibility, not independent parser equivalence. Pre-retirement differential evidence used the real Calamine implementation; Pyodide wheel/SDK execution remains a separate CI gate.
- **Retained attribution** for the small shared-formula/error routines adapted from Calamine under its MIT license.

### Maintenance and versions

- **Patched documentation-site and comparative-harness dependencies**, including Next.js, PostCSS and transitive dependencies with known security advisories. The documentation site remains a static export. These updates do not change the spreadsheet evaluator's numerical behavior.
- **The Rust product crates, native CLI, Python and npm packages are 0.13.0.** `formualizer-common` and `formualizer-parse` move together to 4.1.0; publish that SDK track before the product track. `sheetport-spec` remains 0.3.3.

## [0.12.0] - 2026-10-19

This release makes `formualizer recalc` and the ordinary loader accept the same workbooks, including ones with external links, chart sheets and unevaluable names; fixes date and time cells inside formulas; aligns summation with Excel's order; removes a set of crashes reachable from formula text and workbook content; and makes recalculation about a third faster. The parser and shared-types crates move to 4.0.0 with API changes; the product crates, Python and npm packages are 0.12.0.

### Highlights

- **Workbooks with external links recalculate** from the values Excel cached in the file. Of 15,871 real spreadsheets, 2,786 have links; 1,638 of them now recalculate where all were refused before. (#518)
- **The loader accepts what recalc accepts.** `load_workbook` / `Workbook.load_path` now load every workbook recalc accepts on a 900-file sample, up from 536 of 800. (#519, #520)
- **Dates and times are correct inside formulas** for loaded and edited workbooks; `TEXT`, `COUNTIF`, `MATCH`, `VLOOKUP` and date arithmetic over date cells were wrong before. (#520)
- **`SUM` and its relatives add in Excel's order**, so results match Excel's to the last bit instead of depending on the CPU's vector width. (#530)
- **Crashes, aborts and hangs** reachable from formula text or workbook content are fixed, and `recalc` reports any remaining internal panic as an ordinary error. (#524, #525)
- **Recalculation is about a third faster** over a 275-workbook corpus, with lower peak memory. (#517, #526 to #528, #531 to #535)

### Upgrading

- **Most applications need no changes.** The Python, JavaScript and C APIs are unchanged except where noted below.
- **`formualizer-parse` and `formualizer-common` 4.0.0.** `ASTNodeType::UnaryOp` and `BinaryOp` hold `UnaryOperator` / `BinaryOperator` enums instead of strings. `ExcelError::message`, `context` and `extra` are methods instead of fields (`message()`, `set_message(..)` and so on; `kind` stays a field). `Token::make_subexp*` and `make_separator*` are removed. In the OpenFormula dialect, `;` outside a function argument list (`=A1;B1`) is rejected; its union operator is `~`.
- **`formualizer-eval` 0.12.0.** Arena nodes read back from `AstArena::get`, `DataStore::get_node` and the graph are `AstNodeData<RefId>`; `get_compact_ref(RefId)` returns the reference, or `None` for a stale id. Insertion APIs still take `CompactRefType`. `AstArena::insert_unary_op` / `insert_binary_op` take the operator enums.
- **`formualizer-workbook`.** `XlsxRecalculateResult` gains `external_links_used` and `XlsxRecalculateOptions` gains `external_links`; struct literals that list every field need updating.
- **Results that change.** Summation order (last-bit differences on order-sensitive sums), `SORT`/`SORTBY` on mixed-type columns, the overflow cases listed under Changed, and non-finite numbers now stored as `#NUM!`.

### Recalc and loading

- **External links** (#518). `formualizer recalc` reads the values Excel cached for each link; links are never refreshed, and link parts are kept byte for byte. Over the cells that read linked values, 99.3% match the result Excel stored.
  - A cell the cache omits reads as blank; on a linked sheet whose last refresh failed it reads as `#REF!`, and a range over such cells is refused.
  - Still refused, each with its own reason: DDE and OLE links, links or sheets without a cache, names defined in the linked workbook, whole-row or whole-column external ranges, `OFFSET`/`ROW`/`CELL`-style functions over external references, and `INDIRECT` text that could name another workbook.
  - The `formualizer.recalc/1` report gains an optional `external_links` object (`{"links_used": n, "refreshed": false, "policy": "cached"}`); the Python and wasm results gain the same key. `--external-links refuse` (Python `external_links="refuse"`, wasm `externalLinks: 'refuse'`, Rust `ExternalLinkPolicy::Refuse`) refuses any workbook whose formulas read link values.
  - `INDEX` over an external range, implicit intersection of a bounded external range, and defined names that are exactly one external reference read the cached source.
- **Inert sheets and unevaluable names** (#519). Workbooks with chart sheets, dialog sheets, empty module sheets, or defined names that cannot be evaluated or form a cycle now recalculate, with that content preserved unchanged.
- **The ordinary loader** (#520) applies the same rules. External references read cached values (`Workbook.cached_external_link_indices` lists the links used; a formula recalc would refuse evaluates to `#REF!` while `get_formula` keeps its text). Chart, dialog and empty module sheets are omitted and listed in `sheet_import_diagnostics`; references to them evaluate to `#REF!` and `SHEETS()` does not count them. Unevaluable names become a scoped `#NAME?` listed in `name_import_diagnostics`. New `SpreadsheetReader` provided methods: `cached_external_link_values`, `sheet_import_diagnostics`, `name_import_diagnostics`.

### Changed

- **`SUM`, `AVERAGE`, `SUMIF`, `SUMIFS`, `AVERAGEIF` and `AVERAGEIFS` add values in Excel's order** (#530): one running total, arguments left to right, each range row by row. Previously ranges used a multi-accumulator reduction whose last bits differed from Excel's whenever the order of additions matters. In a 600-workbook sample with 3,496 order-sensitive `SUM` cells, matches rise from 923 to 3,167; the remaining differences come from other cells upstream, not from the sum. Summing very large single ranges is slower (about 2x at 1,000 cells and 3.7x at 100,000 cells without blanks); real workbooks are unaffected.
- `EvalConfig::numeric_profile` is reserved for how reductions are ordered. Its only value, and the default, is `NumericProfile::ExcelCompatible`; the enum is non-exhaustive so a faster, documented profile can be added later.
- **`SORT` and `SORTBY` use Excel's order for mixed values**: numbers, then text (case-insensitive), then FALSE, TRUE, errors and blanks. `SORT({"10";"9"})` now gives "10", "9" as Excel does; mixed columns of 21 or more values no longer panic. (#525)
- Functions now return Excel's error instead of an overflowed or wrapped value: `EVEN` on very large inputs, `LCM` at 2^53 and above, `TIME` with a component of 32768 or more, `COMBIN`/`PERMUT`/`COMBINA`/`FACTDOUBLE` beyond the double range, and `FIXED`/`DOLLAR` with more than 127 decimals. (#525)
- Non-finite numbers (NaN, infinity) are stored as `#NUM!` on every load and write path: XLSX, JSON, `set_value`/`set_values`/`write_range`, workbook actions, SheetPort writes, and the Python, JavaScript and C bindings. (#525)
- `evaluation_budgets.scratch.materialized_array_cells` caps the cells any single range or array value may expand to (default 16,777,216); exceeding it returns `#NUM!` instead of allocating. Ranges are counted after trimming to the used area, so whole-column references over ordinary data are unaffected. (#525)
- `Tokenizer` in `formualizer-parse` is now an owned wrapper over the span tokenizer that `Parser` already used; the older separate tokenizer is gone. Its public surface and the Python and JavaScript `Tokenizer`/`tokenize` are unchanged. On 320,789 real-world formulas both produced identical tokens; `=true#` is now rejected like `=TRUE#`. (#524)
- `ExcelError` shrinks from 104 to 16 bytes and `LiteralValue` from 104 to 32 bytes; errors without a message allocate nothing. (#528)

### Added

- **SheetPort manifests can be built in code** in every language, producing the same structure as the YAML (#521). Python: `formualizer.sheetport` (`Manifest`, `Input`, `Output`, location and schema helpers) and `SheetPortSession(workbook, manifest)`, with `Manifest.to_yaml()` / `from_yaml()`. JavaScript: `defineManifest(...)`, `SheetPortSession.fromManifest(manifest, wb)` and `FioManifest` types. Rust: `Manifest::new(id).port(Port::input(id, location, schema))`, `Manifest::from_json_value`, `From<LiteralValue> for PortValue`, and `InputUpdate` from `(port, value)` pairs.
- The SheetPort examples in the READMEs and docs quickstart are complete programs run by the test suites; the previous quickstart called functions that did not exist. (#521)
- `Coord::try_new` in `formualizer-eval` and `LiteralValue::finite_or_num_error` in `formualizer-common`. (#525)
- `FZ_DEBUG_RECALC=1` prints the wall time of each recalculation phase to stderr. (#517)

### Performance

- **`formualizer recalc` is about a third faster.** Over a 275-workbook corpus of real spreadsheets, total recalculation time fell from 38.8 s to 25.4 s, and the largest workbooks (65,000 formulas) from about 1.06 s to 0.67 s with lower peak memory. Apart from the summation-order change above, outputs are identical to 0.11.1.
- Loading no longer parses formulas that are provably copies of one already parsed: Excel shared-formula copies, and formulas that equal the one above or to the left with references moved (the form openpyxl writes), within a conservative subset (local A1 references, pure functions, no sheet qualifiers or array constants). About 75% of corpus formulas skip parsing; `load_path` plus `evaluate_all` is about 20% faster on formula-heavy workbooks. `FZ_SOURCE_FAMILIES=off` restores parsing every formula. (#517)
- Recalculation's worksheet validation reads XML without per-element allocation, about 2.6 times faster, and checks built-in document names only in formulas that mention one. (#517)
- Formula evaluation resolves each function name once per pass instead of on every call, for single formulas and for filled-down runs (98.6% fewer function lookups on real workbooks). (#526, #531)
- Function arguments are prepared without heap allocation, and single-cell results commit without allocating; allocations per evaluated single-cell formula fall from 14 to under 1 in a scalar benchmark. (#532, #533)
- Arena nodes shrink from 88 to 16 bytes (67% less arena memory on a 10,000-formula model), and sheet-qualified ranges in function arguments resolve directly from the compact node. (#534)
- `COUNTIF(S)`, `SUMIF(S)` and `AVERAGEIF(S)` formulas testing the same range with the same criterion share one match mask (1,000 mask builds become 97 in a criteria-heavy benchmark). (#535)
- `Tokenizer::new` is about 15% faster. (#524)

### Fixed

- **Date and time cells gave wrong results inside formulas** in workbooks loaded with `load_workbook` / `Workbook.load_path` or edited with `set_value` (recalc was not affected): `TEXT(A1,"mm/dd/yyyy")` on a date returned `01/00/1900`, `COUNTIF`/`SUMIF`/`MATCH`/`VLOOKUP` did not find date keys, and a date plus a fractional number dropped the fraction, which also changed `XIRR`. Formulas now read the stored serial; typed values appear only where a value is returned to the caller. `get_value` and range reads return a date with a time of day as `datetime` (Rust `DateTime`) rather than a truncated `date`, and a time outside one day (`[h]:mm` such as 25:30, or negative) as `timedelta` (Rust `Duration`) rather than a wrapped time. (#520)
- **Crashes, aborts and hangs reachable from formula text** (#525): `BETA.DIST`/`BETA.INV` and their legacy forms recursing until the stack overflowed; `LINEST`, `TRANSPOSE`, `SORT` and array arithmetic over whole-sheet ranges trying to allocate hundreds of gigabytes; `COMBINA(0,1)` and `COMBIN`/`PERMUT`/`FACTDOUBLE`/`BINOM*` with large counts looping for a very long time; `BASE` accepting an unbounded minimum length; about twenty functions (including `MID`, `TAKE`, `DATE`, `BITLSHIFT`, `DB`) overflowing integer arithmetic on extreme arguments.
- **Crashes from workbook content** (#525): a built-in name (`_xlnm.Print_Area`, `_FilterDatabase`, `Print_Titles`) or a JSON name or table extending past the last row; a cell at `XFD1048576` making the reader allocate the full sheet rectangle (sheets are now read cell by cell); a NaN cell value in `MEDIAN`, `SMALL`, `LARGE`, `RANK`, `PERCENTILE` or `FREQUENCY`.
- **Parser crashes** (#524): a reference with many `[0]` workbook prefixes (`='[0][0]...[0]Sheet1'!A1`) was parsed recursively and overflowed the stack at about 6 KB of text under WebAssembly and 45 KB natively (Python exited with a segmentation fault). `TemplateLexeme::match_relocated` on a different template, `collect_references` on a hand-built range near `u32::MAX`, and an array literal with an unexpected separator kind in a hand-built `TokenStream` no longer panic, over-allocate or loop. `formualizer-parse` library code no longer uses `unwrap`, `expect`, `panic!` or unchecked indexing, enforced by Clippy.
- `formualizer recalc` reports an unexpected internal panic as its normal error result (`status: "error"`, exit 1, JSON still printed with `--json`) instead of exiting with code 101, and panics inside the readers during a load are returned as load errors. In Python, a panic no longer leaves the workbook's value cache unusable. (#525)
- Panics when inserting rows or columns near the edge of the grid. As in Excel, a reference pushed past row 1,048,576 or column XFD becomes `#REF!`, a range ending at the edge stays there, and an insert that would push content off the sheet is refused with nothing changed. (#517)
- A panic when inserting or deleting rows or columns next to a spilled array whose spill was blocked (#525); a panic ("members instantiate on the grid") when loading filled-down formulas whose relative range end passes their fixed end, such as `SUM(A25:A$24)` below `SUM(A24:A$24)` (#517).
- Defined names on sheets whose names contain a comma (`'Sche 3 (L-Craft, By Trade)'!$1:$3`) were imported as formulas instead of ranges; a whole-sheet print area on such a sheet was evaluated as a million-row array, taking 12.8 s instead of 0.14 s. (#517)
- SheetPort input writes to a table anchored near the bottom of the grid, and Python `duration` values beyond the supported date range (now `ValueError`). (#525)

### Versions

- The Rust product crates and the Python and npm packages are 0.12.0. `formualizer-common` and `formualizer-parse` move together to 4.0.0. `sheetport-spec` moves to 0.3.3 (manifest builders).

## [0.11.1] - 2026-10-07

A patch release with two performance fixes and a set of Excel-compatibility corrections. SUBTOTAL and AGGREGATE no longer make every recalculation re-evaluate them, and repeated lookups over large numeric ranges no longer slow down quadratically. Two changes alter formula results to match Excel: SUBTOTAL 1-11 over filtered rows, and DATEVALUE on day-first dates (see Changed).

### Changed

- **SUBTOTAL and AGGREGATE are no longer volatile.** They recalculate when a cell in their ranges changes, or when row visibility changes (manual or filter hides, row inserts and deletes, undo and redo of those). A recalculation with no edits, or after an edit outside every SUBTOTAL/AGGREGATE range, no longer re-evaluates them. On a workbook with about 4,000 SUBTOTAL formulas, that was the whole cost of an unrelated single-cell edit. (#513)
- **SUBTOTAL 1-11 skip rows hidden by a filter, as in Excel.** Only manually hidden rows still count; 101-111 skip both. `=SUBTOTAL(9,...)` over a filtered range changes value. (#515)
- **`DATEVALUE` no longer swaps day and month.** `DATEVALUE("13/1/2003")` and other day-first slash dates return `#VALUE!`, as Excel does in US locales and as `="13/1/2003"+0` already did, instead of silently reading 13 January. Year-first slash dates and dates with month names are unchanged. (#415)

### Added

- `AGGREGATE` options 4-7 (previously `#N/IMPL!`). They count nested SUBTOTAL and AGGREGATE cells; 5 and 7 skip hidden rows, 6 and 7 skip errors. (#514)

### Fixed

- Fixed repeated lookups over large numeric ranges slowing down sharply: building the shared lookup index was quadratic in the number of rows (7.4 s for four lookups over 200,000 rows, now under 0.1 s). Affects VLOOKUP, HLOOKUP, MATCH and XLOOKUP once a range is looked up more than three times. (#414)
- Fixed `%`, `_` and `\` in criteria text acting as SQL LIKE wildcards: `COUNTIF(A:A,"a%b")` counted `axxxb` and `a_b` as matches. They are literal now, while `*`, `?` and `~` keep their Excel meaning. Affects COUNTIF, SUMIF, AVERAGEIF, the `IFS` variants and MAXIFS/MINIFS. (#295)
- Fixed the sheet index keeping cells at their old positions after rows or columns were inserted or deleted, so lookups through it missed moved cells. An enclosing SUBTOTAL or AGGREGATE then counted nested subtotals in the shifted rows.
- The `formualizer-cli` crate now includes the `LICENSE-MIT` and `LICENSE-APACHE` texts. (#443)

### Versions

- The Rust product crates and the Python and npm packages are 0.11.1. `formualizer-common` and `formualizer-parse` stay at 3.2.0 and `sheetport-spec` at 0.3.2; none of them changed.

## [0.11.0] - 2026-10-06

### Recalc CLI

This release introduces `formualizer recalc`: after openpyxl or another editor saves an `.xlsx`, it recomputes the cached formula values and writes them back without rebuilding the workbook. Formula text, styles, drawings, names and untouched package content are preserved. Workbooks outside the supported subset are refused as a whole instead of published with guessed values.

- Added the native `formualizer recalc <INPUT> [-o PATH] [--check] [--json] [--max-errors N] [--now TIMESTAMP] [--tz ZONE] [--seed U64]` command. It writes in place atomically by default (a true no-op leaves the file untouched), `-o` always publishes a separate file, and `--check` computes without writing (exit 3 when stale). Exit codes are 0 success, 1 error, 2 refused, 3 stale, 64 usage error and 130 interrupted; Ctrl-C cancels cooperatively before publication. `--json` prints one `formualizer.recalc/1` object with counters, error-cell locations and structured refusals.
- Added distribution channels for the same Rust command: the `formualizer` console script and `python -m formualizer` in native Python wheels (not Pyodide); the npm package `@formualizer/cli`, a launcher with seven `@formualizer/cli-<platform>` native binary packages and no WebAssembly fallback (the unscoped `formualizer` npm package remains the WASM library); the `formualizer-cli` crate for `cargo install` and `cargo binstall`; and `formualizer-cli-v<version>-<target>` archives with `SHA256SUMS` on GitHub releases, for Linux x64/arm64 (glibc and static musl), macOS x64/arm64 and Windows x64.
- Added source-preserving XLSX writeback for new multi-cell spills and validated existing XLDAPR dynamic arrays, including growth, shrink, collapse and blocked/re-expanded anchors. Source-declared child caches are recalculated, while formula text, styled obsolete cell shells and unrelated package content are retained.
- Added current-spill references through `A1#` and `_xlfn.ANCHORARRAY(A1)` in Rust, Python and WASM evaluation.
- Added legacy fixed-extent CSE array recalculation, including single-cell top-left results, broadcasting, padding and truncation. Re-saved spills without dynamic metadata retain their declared extent; spill-reference readers return `#REF!`.
- Added strict source-preserving XLSX table admission and pre-ingestion table hydration, with byte-preserved table XML, managed-formula coverage checks and table spill obstructions.
- Enabled current volatile cache publication (`TODAY`, `NOW`, `RAND`, `OFFSET`, `INDIRECT`, `SUBTOTAL`, `AGGREGATE`) in source XLSX recalculation, sampled once per run, with hidden-row reducer refusals.
- Added reproducible runs: `--now` (RFC 3339 with an offset or `Z`) fixes `TODAY`/`NOW`, `--tz UTC|±HH:MM` sets the zone they are read in (default: the `--now` offset, otherwise host local time) and `--seed` sets the `RAND`/`RANDBETWEEN` seed, which is already stable run to run by default. With `--now`, reruns are byte-identical and `--check` is meaningful for volatile workbooks; builds without a system clock admit `TODAY`/`NOW` when `--now` is given. Invalid values exit 64. Computed `--json` reports add `clock` (`now`, `timezone`, `fixed`) and `seed`, null on refusals and errors, so any run can be replayed. `recalc --help` now describes every option, the exit codes and in-place writing.
- Added `rng_seed`, `deterministic_timestamp_utc` and `deterministic_timezone` to Python `recalculate_xlsx_file`/`recalculate_xlsx_bytes`, and an options object `{rngSeed, deterministicTimestampUtc, deterministicTimezone}` to WASM `recalculateXlsxBytes`; both results report `clock` and `seed`. Python and WASM deterministic timezones also accept `"±HH:MM"` strings.
- Added `XlsxRecalculateResult::clock_now_utc`, the instant `TODAY`/`NOW` observed. A system-clock source recalculation now takes one whole-second sample immediately before evaluation, and an invalid deterministic mode (a `Local` timezone with a fixed timestamp) is an error instead of a silent fallback to the system clock.
- Admitted the ZIP containers real spreadsheet applications write: Excel's `0xA220` growth-hint padding (desktop, Mac and Online), LibreOffice and Google Sheets data descriptors, and Info-ZIP/NTFS timestamp and owner extra fields. Previously every Excel-saved, LibreOffice and Google Sheets workbook was refused. Each extra block is parsed strictly; ZIP64, encryption, entry comments and unknown extra fields are still refused. Untouched members keep every byte; a rewritten member is written without extra fields or a data descriptor.
- Admitted the extension markup Excel 2010+ writes routinely, in the positions Excel writes it: `<x15:workbookPr chartTrackingRefBase="1"/>` in the workbook extension list, `xdr:row` in form-control and OLE-object anchors, `xm:f` in x14 data validations and conditional formats, and `mc:Ignorable`/`xr:uid`/`xr3:uid` on tables. These previously refused as `foreign workbook metadata lookalike`, `foreign worksheet lookalike` or `unsupported table attribute mc:Ignorable`. The date system still comes only from the main `workbookPr`; lookalikes inside `sheetData`, extension defined names or merge cells, other attributes and unknown namespaces are still refused.
- Added error reasons to recalculation receipts. Each `--json` `errors` entry has a `message` with the engine's reason (`Unknown function: SPDVOL`, `Undefined name: Rate`; null when the cell has none of its own, such as an inherited `#NAME?`), and the new top-level `unknown_functions` lists every function the engine does not implement with its cell count, complete even when `--max-errors` truncates `errors`. The human line shows the reason next to each listed error plus a one-line unknown-function summary. Python `recalculate_xlsx_*` and WASM `recalculateXlsxBytes` summaries add `messages` (parallel to `locations`) and `unknown_functions`; Rust adds `RecalculateErrorSummary::messages` and `RecalculateSummary::unknown_functions`. Add-in (`_xll.`), VBA/macro and other unimplemented functions keep producing `#NAME?` and are written, not refused.
- Numeric caches that agree with the computed value within one unit in the 15th significant digit (Excel's display precision) are now current: their original bytes are kept, they are not counted in `cache_cells_changed`, and `--check` reports `current` when nothing else changed. Previously last-bit differences, such as Excel's sequential `SUM` against ours, made such files stale. Zero caches against nonzero residues, larger differences and type changes are still written.
- Documented that workbooks set to "precision as displayed" (`fullPrecision="0"`) are computed in full precision, so their values can differ from Excel's displayed-precision results.
- Enabled source recalculation (`recalculate_xlsx_bytes`) and the Calamine backend in the Pyodide wheel, which builds with Pyodide 314's Rust 1.93. Pyodide has no system clock: workbooks using `TODAY`/`NOW` are refused unless `deterministic_timestamp_utc` is passed. The Pyodide byte-loading default stays `umya`.
- Excel-calculated formulas now keep their legacy (pre-dynamic-array) meaning in source recalculation. When a formula cell is listed in the workbook's calculation chain (`xl/calcChain.xml`, which Excel writes and openpyxl, XlsxWriter and umya-spreadsheet do not) and it is not an array formula, a multi-cell range or array in a value position is implicitly intersected with the formula's row or column, as Excel does (Excel 365 shows these formulas with `@`). For example, `=VLOOKUP($B$4:$B$2636,…)` in row 900 looks up `B900`, `=Sheet2!E1:E20` in row 5 reads `Sheet2!E5`, and an off-range intersection gives `#VALUE!`. Previously these cells spilled or gave `#N/A`/`#SPILL!`. Where the intersection happens follows Excel's parameter classes for functions up to Excel 2013. Formulas using newer functions, structured references, 3-D references or `LET`/`LAMBDA` keep dynamic-array evaluation. Workbooks without a calc chain, including every openpyxl-authored workbook, are unchanged. The stored formula text and the calc chain are never rewritten, so a second recalc is a no-op.
- Refused stored formulas the parser cannot read as `unparseable formula` (exit 2, with the sheet, cell and parser message) instead of failing with a Calamine I/O error (exit 1).
- Added an agent edit/recalc/inspect workflow guide, a portable `formualizer-recalc` agent skill and a Recalc CLI section on formualizer.dev with install, agent workflow, CLI reference, supported/refused and agent skill pages.

### Changed

- **`^` is now left-associative, as in Excel.** `=2^3^2` returns `64` (was `512`), and `=2^-1^2` returns `0.25` (was `2`). Formulas that chain `^` without brackets change value. (#508, @mcdonaldsam)
- **AND, OR and XOR evaluate every argument, as in Excel.** They no longer stop at the first FALSE (AND) or TRUE (OR): an error in any argument is returned, the first one in argument order, so `=AND(FALSE,1/0)` is `#DIV/0!` (was FALSE). Text and blank cells inside references are ignored, direct text arguments are `#VALUE!`, and arguments with no logical values at all give `#VALUE!`. A blank cell no longer counts as FALSE in AND.
- Retargeted the Pyodide wheel from Pyodide 0.29.x to Pyodide 314.0.7 (Python 3.14). The wheel is now tagged `pyemscripten_2026_0_wasm32` and no longer installs in Pyodide 0.29.x. CI and releases build with Rust 1.99.0; the Pyodide wheel builds with Pyodide's Rust 1.93.0.
- **Versions:** the Rust product crates and the Python and npm packages are 0.11.0. `formualizer-common` and `formualizer-parse` move together to 3.2.0 (parser resource limits, `^` associativity and printer fixes; additive API). `sheetport-spec` moves to 0.3.2 with no API change: its source dropped an emscripten-only `#![feature]` attribute for the stable-Rust Pyodide build. Rust code that matches every `CriteriaPredicate` variant without a wildcard arm must handle the new `NotBlank`, `NotTextLike` and `Text*` comparison variants.

### Added

- Added `Engine::evaluate_all_for_snapshot` for consistent one-request volatile results without suppressing iterative-SCC or other freshness checks.
- Parser resource limits. By default a formula may have at most 65,536 source bytes, 16,384 tokens, 8,192 AST nodes, 72 active Pratt-parser frames (about 64 levels of nested calls or brackets) and an AST height of 256. A formula over a limit is a parse error instead of a potential stack overflow. Rust callers can change any limit with `ParserLimits::default().with_ast_height(..)` (also `with_source_bytes`, `with_tokens`, `with_ast_nodes`, `with_pratt_frames`) and pass it to `Parser::builder().limits(..)` or `BatchParser::builder().limits(..)`. Raising the stack-sensitive limits needs more thread stack, about 2.5 KiB per extra AST level in release builds; see the `formualizer-parse` README.
- `BatchParser` bounds its token cache to 16,384 entries and 8 MiB of source and token payload (FIFO). `BatchParser::builder().cache_capacity(entries, bytes)` configures it; zero entries disables caching.
- Externally constructed token streams are validated (ordered, disjoint, in-bounds UTF-8 spans) before parsing.

### Fixed

- Fixed `@` (implicit intersection) on whole-row and whole-column references, such as `=@21:21` or `=@A:A`, which returned `#VALUE!` when the formula's column or row lay outside the row's or column's used cells. The intersection now uses the sheet bounds.
- Fixed `@` on a relative range in formulas filled down a column (one shape shared by many cells): every cell intersected the first cell's range, so rows past that range returned `#VALUE!` (for example `=@A4:A10` filled from `B5` gave `#VALUE!` from `B11` on).
- Fixed silently wrong values in recurrences filled down a column that also read a fixed cell in that same column, such as a decay schedule `B9=+B8-(B8*$B$4)` with its rate in `B4`. Once the run was long enough (about 32 rows or more), every row computed as if the fixed cell were blank, so the schedule repeated its starting value instead of 17700, 17405, and so on. The same applied to fixed cells below the run and to an absolute-row reference to the row above the run. Affected 0.10.0 and 0.10.1; 0.9.3 and earlier were not affected.
- Applied `IF` elementwise to array and range conditions with singleton-axis broadcasting, lazy branch evaluation and per-element errors. Scalar conditions retain short-circuit reference selection.
- Preserved calculation names during Calamine import, including supported constants, grounded formulas and sheet-local shadowing; refused unsupported or cyclic definitions instead of publishing metadata-loss `#NAME?` results.
- Lowered bare table names (`SUM(Table1)`, `VLOOKUP(x,Table1,2,FALSE)`) to the table data body in source XLSX recalculation instead of publishing `#NAME?`; a defined name pointing at a bare table name now refuses as unsupported (exit 2) rather than failing as a backend error.
- Refused multi-cell shared formulas whose sheet qualifiers Calamine's shared-formula expansion would rewrite (such as `'Q1'!`, `'FY2024'!` or a name containing `"`), including qualifiers injected by cross-sheet table lowering. Previously followers could silently read another sheet.
- Refused cell-sourced or computed `INDIRECT` text in table-bearing source workbooks, and `[#This Row]` in a table's totals row, instead of publishing values from unsupported table resolution.
- Admitted tables that extend past the last stored cell when the worksheet has no `<dimension>`.
- Refused source XLSX `SUBTOTAL`/`AGGREGATE` results whose hidden-row exclusion could not be proved: range, intersection and union operators are checked over their whole row span (function operands, names and LET/LAMBDA-bound arguments are refused), and zero-height rows, collapsed outline groups and `zeroHeight` sheets count as hidden. Previously `SUBTOTAL(109,A1:A2:A5)` and similar forms could publish sums that included hidden rows.
- Left snapshot mode when `Engine::evaluate_all_for_snapshot` unwinds from a panic, so a caller that catches the panic and keeps the engine gets ordinary volatile redirty on its next evaluation.
- Recalculated readers of cells written by `Engine::bulk_set_formulas`, including chains, range, cross-sheet and formula-family readers. Previously only the written cells were recomputed and their dependents kept stale values, unlike `Engine::set_cell_formula`.
- Reported a dynamic-array spill blocked by another formula or by another spill as `#SPILL!` on the anchor (with its attempted extent, encoded as for a value blocker) instead of aborting the evaluation with a `BlockedByFormula`/`BlockedBySpill` error; `formualizer recalc` now writes the blocked anchor and exits 0. The blocking formula evaluates normally, the anchor's dependents see `#SPILL!` and its `A1#` readers `#REF!`. Removing the blocker re-spills the anchor on the next recalc.
- Made a formula entered into a live spill block it, as on a fresh evaluation: the anchor becomes `#SPILL!` and the new formula keeps its value. Previously the anchor overwrote the formula's cell on its next recompute and could delete the formula when its spill was cleared; an anchor entered into a live spill was itself `#SPILL!`. When two spill extents collide without either anchor inside the other's rectangle, the anchor first in (sheet, column, row) order spills and the other is `#SPILL!`, whatever the evaluation or edit order.
- Cleared a spill when `Engine::set_cell_value` replaces its anchor formula with a value. Previously the old children and their ownership remained.
- A range from a cell to a reference function's result parses as the range operator followed by the call. `=SUM(B10:INDEX(B:B,5))` and `=STDEV(G3:OFFSET(G3,-4,0))` now compute the range, where they previously returned `#NAME?` for an unknown function `B10:INDEX`. (#501, @mcdonaldsam)
- A quoted sheet span such as `'Jan 24:Mar 24'!B5` is read as a 3D reference over those sheets, not as one sheet with a colon in its name. 3D references are not evaluated yet, so these formulas return `#N/IMPL!` instead of a silent `#REF!`, and XLSX cache recalculation refuses such workbooks rather than writing `#REF!`. The printer writes such spans the way Excel does. (#502, @mcdonaldsam)
- `Table1[]` refers to the table's data body, like `Table1`, instead of including the header and totals rows. (#503, @mcdonaldsam)
- A defined name on either side of `:` parses as the range operator (`=SUM(D22:Total)`, `Start:B10`), where it was a parse error. Name endpoints are not resolved yet, so these ranges return `#N/IMPL!` with an explanatory message; Excel computes the bounding range. XLSX cache recalculation refuses such workbooks instead of writing a value. (#504, @mcdonaldsam)
- Ranges with a deleted end, which Excel writes as `=SUM(A1:#REF!)`, `=#REF!:A1` or `=Sheet1!#REF!:A1`, parse and evaluate to `#REF!`, as in Excel. (#505, @mcdonaldsam)
- A defined name whose sheet was deleted, which Excel shows as `=#REF!#REF!`, parses as a single `#REF!` error. (#506, @mcdonaldsam)
- The pretty printer and `canonical_formula` keep brackets the formula needs. `A1*(B1*C1)` and `1=(2=3)` no longer print in a form that re-parses differently. A union passed as an argument keeps its brackets, so `RANK(A1,(B1,B5))` and `LARGE((F38,C38),1)` keep the same number of arguments when printed. The same applies inside nested calls and array constants, such as `SUM((A1,B1)+1)`. (#507, @mcdonaldsam)
- Criteria functions (COUNTIF, SUMIF, AVERAGEIF, their *IFS forms, MAXIFS/MINIFS) follow Excel: a criterion that references an empty cell is 0, so blank cells no longer match it; wildcards match text only, never numbers; `"="` matches only blank cells and `"<>"` every non-blank cell; text comparisons such as `">b"` compare text; trailing spaces in criteria text are significant; and date text such as `"=2/1/02"` or `">2/15/02"` matches dates. `CriteriaPredicate` has new variants for these cases.
- SUBTOTAL ignores cells in its ranges whose formulas contain SUBTOTAL, and AGGREGATE (options 0 to 3) ignores cells containing SUBTOTAL or AGGREGATE, so nested subtotals are no longer counted twice. Finding those cells checks a filled-down column of formulas once rather than per cell, so running totals such as `SUBTOTAL(9,$B$2:B2)` stay about as fast as in 0.10.1.
- `VLOOKUP` and `HLOOKUP` pass an empty matched cell to the surrounding formula as blank, as Excel does: `ISBLANK(VLOOKUP(..))` is TRUE, `VLOOKUP(..)&""` is empty text and `VLOOKUP(..)=""` is TRUE. A cell holding only the lookup still shows 0.
- Database functions (DSUM, DCOUNT, DGET, ...) match no records when a criteria header is not a database label, instead of ignoring that column, and return the error of an error database or criteria argument (such as a name defined as `#REF!`) instead of `#VALUE!`. DSUM with no matching records returns 0, not -0.
- TEXT renders date and time format codes as Excel does: `yy`, `ddd`/`dddd`, month names, `m`/`mm` as minutes after `h` or before `s`, AM/PM and A/P, elapsed `[h]`/`[m]`/`[s]`, fractional seconds, and quoted or escaped literals. `DATEVALUE(TEXT(date,"mm/dd/yy"))` now round-trips.
- YEAR, MONTH and DAY of 0 or a blank cell return 1900, 1 and 0 (Excel's "January 0, 1900"), and `DAY(60)` is 29.
- YEARFRAC returns a positive value when the start date is after the end date, and bases 0 and 1 follow Excel's month-end and leap-year rules. DAYS360 (US method) moves an end date only when it is the 31st, so `DAYS360("11/24/2001","11/30/2001")` is 6.
- HOUR rounds the time to the nearest second, like MINUTE and SECOND, so a serial a hair below the hour returns that hour (`HOUR(37054.54166666666)` is 13). TEXT time codes round the same way.
- Text such as "NaN", "inf" or "1e400" is no longer read as a number; arithmetic on it returns `#VALUE!`. VALUE converts date and time text with Excel's two-digit-year rule (`VALUE("12/31/00")` is 36891), and DATEVALUE accepts a date followed by a time.
- REPT with a negative count, including fractions such as -0.13, returns `#VALUE!`; the 32,767-character limit counts characters, not bytes.
- Added the Excel 2007 statistical names NORMSDIST, NORMSINV, NORMDIST, NORMINV, TDIST, CHIDIST, CHIINV, FDIST, FINV, BETADIST, BETAINV, GAMMADIST, LOGNORMDIST, LOGINV, POISSON, BINOMDIST, EXPONDIST, WEIBULL, HYPGEOMDIST and NEGBINOMDIST with their original argument lists and tails, and made TINV two-tailed with truncated degrees of freedom. GAMMA.INV, CHISQ.INV, F.INV and BETA.INV no longer return wrong values in the tails.
- CORREL, PEARSON, RSQ, SLOPE, INTERCEPT, STEYX, COVAR, COVARIANCE.P/S and FORECAST skip a position when either array has a blank, text or logical value there, instead of returning `#N/A`.
- LINEST, LOGEST, TREND and GROWTH support several x variables (multiple regression), with the full statistics block, collinear columns removed as Excel does, and legacy array formulas wider than the result padded with `#N/A` instead of `#REF!`.
- Formulas using Excel's current-workbook qualifier (`[0]!Name`, `[0]Sheet1!A1`) resolve to the local name or sheet instead of `#NAME?`.
- XLSX import keeps stored zero-length strings as empty text, so COUNTA counts them, ISBLANK is FALSE and references return `""`, and keeps the exact value of date- and time-formatted numbers instead of rounding them to whole seconds.

### Known limitations


[Showing lines 1-267 of 1143 (50.0KB limit). Use offset=268 to continue.]