# Changelog

All notable changes to Formualizer will be documented in this file.

## Unreleased

### Changed

- FormulaPlane correctness now covers mixed-pass named-symbol scheduling with dynamic fail-closed preflight, format preservation across broadcast and computed-overlay writeback, transactional demotion of retained spans before Off-mode legacy evaluation without FormulaPlane dispatch, and dependent healing after name redefinition.
- FormulaPlane now admits `IF`, `IFS`, and `CHOOSE` spans when every result arm is a single cell or a statically scalar expression. Range, open-range, named, volatile, and dynamic-reference arms remain on the legacy path.
- Named-range definitions now track structural inserts and deletes like all other references; previously they stayed pinned to their original coordinates. Deleting a band containing a name target now invalidates the definition to `#REF!`. (#170)
- Computed temporals are numeric during evaluation (`ISNUMBER`/`TYPE` now match Excel). Native scalar, range, table, Python, and SheetPort egress materializes date/time values from the cell's effective format; callers can opt into uniform raw serials. A known datetime class preserves midnight datetimes, while calamine's code-lossy date-ish signal can only classify pure fractions as time, day-plus-fraction serials as datetime, and integers as date.
- Arrow dependencies upgraded 58.2 → 59.2 across `formualizer-eval` (`arrow`, `arrow-array`, `arrow-buffer`, `arrow-schema`, `arrow-select`, `arrow-cast`). No API or behaviour change; full suite green on the pinned surface, native and wasm32.

### Fixed

- SheetPort ports now coerce temporal values to the manifest-declared type at the port boundary: `number`/`integer` ports receive serials regardless of the engine's `TemporalEgress` policy, and `date`/`datetime` ports receive native temporals; the manifest is the protocol contract, engine egress is only a default.
- Date arithmetic no longer conditionally propagates `LiteralValue::Date`; a position-keyed, non-sticky scalar format annotation carries temporal display class independently, fixing #312 without exposing annotations to bulk numeric kernels. Date-plus-time yields datetime, date-plus-percent and date-plus-plain-number yield date, and unspecified class pairs drop the annotation. Selection functions preserve the chosen scalar annotation, including `IFERROR`, `IFNA`, and scalar-argument `MAX`/`MIN`; `MAX`/`MIN` over multi-cell ranges do not yet recover the winning cell's format.
- Before merge, the unreleased format channel was hardened so row, column, and sheet structural edits purge affected derived-format positions, and value/formula writes invalidate format state in both ephemeral and interactive/changelog modes. This prevents shifted plain values and logged overwrites from inheriting stale temporal formats; neither pre-merge regression shipped.
- Temporal values saved through `Workbook::to_xlsx_bytes` round-trip as date-system-aware numeric serials instead of text, restoring arithmetic while leaving display and number-format fidelity to the format channel. (#355)

- Structural row inserts and deletes now invalidate compressed open-range readers when the edited axis intersects and an indexed occupied column crosses the range. Bounded formulas whose AST is adjusted remain conservatively dirtied, and column edits retain conservative cross-axis invalidation because Arrow has no cheap occupied-row index. (#313, #314)
- **A structural edit no longer reaches a defined name, table or external source.** Those three, plus sheet-scoped names, are identified by name and have no position on any sheet, but the graph gave them fabricated grid coordinates on a real user-visible sheet: the first workbook name landed on `Sheet1!$A$1`, the second on `$B$1`, and a table on its range's anchor cell. The only thing separating such a vertex from the actual cell at that address was its deliberate absence from the cell index — a convention, not a structural property. Two code paths broke it, and both are fixed by giving symbols their own address space. (#302, #304)

  A **default-sheet row or column insert** shifted a name's vertex onto an addressable cell *and* published it in the cell index at the new address. Every dependency resolved afterwards against that address then bound to the name instead of the cell. Three consequences followed, each measured against a matched control that varied only the insert row: a formula written later as `=A2+1` acquired `deps == ["NamedScalar@Sheet1!$A$2"]` rather than a direct cell edge, so editing the *name's* target dirtied an unrelated default-sheet formula; an ordinary data write at the hijacked address rewrote the name's own vertex from `NamedScalar` to `Cell`, silently destroying the name while `resolve_name_entry` kept returning it; and deleting the row the vertex then occupied stranded the name, after which `=Tracked+1` served `71` where `701` was correct. The same happened for sheet-scoped names, for tables, for expanded ranges that merely *covered* the address, and for the vertex's later positions after repeated inserts. (#304)

  A **structural edit on a completely unrelated sheet** dropped formula→name edges. Deleting row 1 or column 1 of the default sheet swept up any name vertex parked at `$A$1` as an ordinary resident of that row or column and deleted it, taking its edges with it — even when both the formula and the name's target lived on another sheet entirely. Subsequent changes to the name's target no longer dirtied the formula, which then served silently stale values. (#302)

  Symbol vertices now hold a `SymbolAddr` in an address space disjoint from the grid's, and the structures keyed by position — the cell index, the per-sheet range index, and the iteration that drives every structural edit — accept a `GridAddr`, which a symbol cannot produce. `NameScope` is now purely lookup metadata and no longer decides where a vertex lives. Evaluation results are unchanged; a name's scope, resolution and dirty propagation all behave exactly as before.

## [0.8.4] - 2026-08-14

### Changed

- **Loading a file no longer leaves a phantom default sheet, so sheet indices shift.** A freshly constructed `Workbook`/`Engine` is seeded with one default sheet (`Sheet1`), and every backend loader appended the file's sheets alongside it. Loading a workbook whose sheets were `Data` and `Extra` produced `sheet_names() == ["Sheet1", "Data", "Extra"]` — a sheet that does not exist in the file — and `SHEET()` on the file's first sheet returned `2` instead of `1`. The default sheet is now folded into the file's first sheet, so the same load produces `["Data", "Extra"]` with `SHEET()` returning `1` and `2`. This affects the calamine, umya, json and csv backends alike; previously only csv was accidentally immune, because its sheet name is `Sheet1` by default and so always collided with the seeded one. (#332)

  Concretely, for any file whose first sheet is not named `Sheet1`: `Workbook::sheet_names()` (and `wb.sheet_names` in the Python and WASM bindings) loses the leading `"Sheet1"`; `SHEET()` and `SHEETS()` return one less than before; engine `SheetId`s shift down by one (`Data#1` becomes `Data#0`); and a saved artifact no longer carries the injected empty sheet — measured, `to_xlsx_bytes()` after a calamine load wrote `["Sheet1", "Data", "Extra"]` into the output file and now writes `["Data", "Extra"]`. Sheets are addressed by name everywhere they are persisted (the JSON document format keys sheets by name, SheetPort manifests bind by sheet name), so no stored artifact embeds an index that this changes; the change is confined to in-memory ids, `SHEET()`/`SHEETS()` values, `sheet_names()` output, and the sheet list written into saved files.

  The fold happens only when the engine is otherwise untouched — one sheet, the default one, with no cells, no named ranges, no staged formulas and no row-visibility state. `EngineLoadStream::stream_into_engine` is public and takes a caller-supplied engine; when that engine already holds user content, the default sheet is left exactly as it is and the file's sheets are added alongside it, so existing data is never renamed away or overwritten. All four backends now route sheet registration through one shared entry point, `Engine::adopt_file_sheets`.

- **A file containing two sheets whose names differ only by case is now rejected at load** with `#VALUE!: Duplicate sheet name in workbook`. Sheet names are unique and case-insensitive in Excel, and `add_sheet` is idempotent, so such a file previously merged the two sheets silently and let the second sheet's cells overwrite the first's. (#332)

### Performance

- `Workbook::set_values` — the batch path behind `Sheet.setValues` in the JS/WASM binding — pre-allocates the Arrow sheet to the batch extent once instead of growing it a row at a time. Each per-cell growth rebuilt the whole column's type-tag array and every present lane, so a batch of N cells did O(N²) work. Measured natively on a single numeric column: 20,000 rows went from 835 ms to 126 ms, a 6.6× improvement, with per-cell cost flat rather than growing with N. Contributed externally. (#335)

### Fixed

- A name scoped to a single sheet no longer answers a query that asked for no scope. `has_name(name, None)` and `resolved_name_value(name, None)` mapped the absent scope to the *default* sheet, and sheet-scoped names are resolved before workbook-scoped ones, so a name scoped only to `Sheet1` was indistinguishable from a workbook-scoped name — while the same query against any other sheet correctly reported it absent. An unscoped query now means workbook scope, and nothing else. (#110)
- Target preparation no longer pulls in the default sheet for range dependencies that carry an unresolved sheet locator. A `Current` locator means "the sheet this reference lives on", and two sites substituted the workbook default instead — one of them via a `_` wildcard that also swallowed named-sheet locators, so a sheet name that failed to resolve silently became the default sheet. A named formula scoped to `Alpha` with a whole-column dependency selected cells on both `Alpha` and `Sheet1`; it now selects `Alpha` alone. Sheet resolution for these paths is now a single derivation that requires an explicit context sheet, and every locator variant is matched exhaustively so a new variant is a compile error rather than a silent default. (#110)

- Batch writes no longer report a sheet extent larger than the cells they wrote. `set_formulas` computed its pre-allocation from the row count including trailing empty rows, which write nothing, so a batch ending in an empty row reported one row too many — and a batch anchored at the last grid row reported row 1,048,577, which cannot exist. `set_values` inherited the same computation when it gained pre-allocation. Both now derive the extent from the last row that actually contains cells. (#335)

- Date-typed cells are treated as the numbers they are throughout the financial and statistical builtins. Excel has no separate date type on the sheet — a date cell holds a serial and is only formatted as a date — but three copy-pasted private coercion helpers plus two range collectors had no date arm, so `Date`/`DateTime`/`Time`/`Duration` cells were dropped or rejected. The consequences ranged from loud to invisible: `PRICE`, `YIELD`, `ACCRINT`, `ACCRINTM`, `TBILLPRICE`, `TBILLEQ`, `TBILLYIELD` returned `#VALUE!` when handed the date cells a workbook loaded from XLSX actually contains in their `settlement`/`maturity`/`issue` arguments; `NPV` returned `#VALUE!`; `XNPV`/`XIRR` returned `#NUM!` on their `values` argument; and `IRR`, `MIRR`, `MEDIAN`, `STDEV`, `VAR`, `LARGE`, `SMALL`, `PRODUCT`, `DEVSQ`, `PERCENTILE`, `QUARTILE`, `CORREL`, `SLOPE`, `RSQ`, `COVARIANCE`, `MAXA`, `MINA`, `AVERAGEA`, `STDEVA`, `VARA` and their siblings returned a *different number* with no error at all — while `SUM`, `AVERAGE` and `COUNT` over the very same range counted the date. All of these now agree with the numerically identical serial. (#328)
- `XNPV` and `XIRR` truncate date serials to whole days. A cell holding `2024-01-01 12:00` (serial 45292.5) now discounts as day 45292, matching Microsoft's documented remark that numbers in dates are truncated to integers, and reproduced against LibreOffice 24.2.7.2, which returns `308.187137202582` where formualizer previously returned `308.357947738306`. (#328)
- The date-serial conversion used by the financial builtins is now the crate-central `coercion::to_serial_strict` rather than three local duplicates, so it honours the workbook's date system and covers `Time`/`Duration` as well as `Date`/`DateTime`. (#328)

### Python bindings

- `SheetPortSession.evaluate_once`, `write_inputs`, `read_inputs` and `read_outputs` release the GIL while the engine runs. `SheetPortSession` owns a `Workbook` and `from_manifest_yaml` accepts a user-supplied one, so Python-backed custom functions are reachable from a SheetPort evaluation and hit exactly the deadlock fixed on `Workbook.evaluate_*`: the calling thread held the GIL waiting for the parallel layer while rayon workers blocked in `PyGILState_Ensure`. (#327)
- Using a `Workbook` from inside one of its own Python custom functions raises `RuntimeError` instead of hanging forever. The engine holds a non-reentrant lock for the duration of an evaluation, so `get_value` on an uncached cell, `set_value`, `sheet_names`, a nested `evaluate_*`, `Sheet.get_cell` and every other workbook operation could never be granted and blocked permanently — non-deterministically, since `get_value` served from the compatibility cache returned normally. `cancel()` and `reset_cancel()` remain safe from a callback, and a callback may still drive a *different* workbook. The contract is documented on `register_function`. (#327)
- The Python test suite is bounded by `faulthandler_timeout` with `faulthandler_exit_on_timeout`, and every CI job has a `timeout-minutes`. A deadlock regression now fails in 90 seconds with every thread's stack dumped, instead of running until GitHub's six-hour default. Measured: neither pytest-timeout's `thread` nor its `signal` method fires during a GIL deadlock, because both need to execute Python. (#327)

## [0.8.3] - 2026-08-13

### Fixed

- Approximate `MATCH`, `VLOOKUP`, and `HLOOKUP` skip error cells in the lookup range instead of propagating them. 0.8.2 introduced range-wide propagation, which diverged from desktop Excel on 8 of 10 measured oracle rows: a single `#DIV/0!` anywhere in a lookup column poisoned every approximate lookup over that column, where Excel answers normally. Error cells are projected out of the search exactly like blanks and other-class entries — they cannot be returned, do not break sortedness, and are not propagated even when a bisection probe lands on one. A range with nothing searchable left yields `#N/A`, and exact mode still never matches an error cell. Oracle contributed by an external reporter running desktop Excel 16.105.3 under AppleScript automation. (#326)
- Descending approximate `MATCH` returns the *first* entry of an exact-match run rather than the last, matching Excel; the last qualifying entry is still returned when the needle falls between two values. This affected ranges with fewer than eight searchable entries, whose linear path lacked the exact-match rule the bisection path already had. (#326)

## [0.8.2] - 2026-08-11

### Fixed

- Python wheels evaluate `TODAY()` and `NOW()` against the host clock. The binding did not enable the `system-clock` feature, so the engine fell back to a fixed clock at the UTC epoch and every volatile date/time builtin answered 1970-01-01 in every published wheel — silently, with no error and no type difference, so any ageing bucket or accrual-to-date returned a plausible wrong number. Reported externally. (#318)
- Temporal values compare as workbook-date-system serials across exact and approximate lookup paths, including warm lookup indexes. (#316)
- Approximate `MATCH`, `VLOOKUP`, and `HLOOKUP` skip blank and incomparable entries without losing original-range positions, while propagating lookup-range errors. (#317)
- Descending approximate `MATCH` returns the last qualifying entry for ranges with eight or more searchable values, including ranges with interior blanks. (#320)

## [0.8.1] - 2026-08-11

### Fixed

- Row and column deletion now recalculates formulas that read affected whole-column, whole-row, or other compressed open ranges, instead of returning stale pre-deletion values while reporting them as current. Insertion still does not invalidate position-sensitive open-range readers such as `MATCH` and `INDEX`; that pre-existing gap is tracked separately. (#306)
- Arithmetic on a date-typed cell no longer returns `#NUM!` when the result falls outside the representable date range. `Date`/`DateTime` operands keep their temporal tag when the resulting serial is representable, and degrade to a plain number when it is not, instead of manufacturing a numeric-domain error. This most visibly affected `end_date - start_date` accrual expressions whenever the period ran backwards: a perfectly ordinary negative day count became `#NUM!` and propagated through every downstream cell. NaN and infinity operands continue to error. Reported externally with a full reproduction and LibreOffice cross-check. (#310)

## [0.8.0] - 2026-08-10

### Engine introspection (new)

- Added `formualizer_eval::engine::inspect`, a public, engine-native introspection API: `inspect_cell`, `precedents`, `dependents`, `trace`, and `range_page`. Reports are owned, stamped snapshots addressed in A1 space, bounded by explicit `max_work` discovery budgets that degrade in band rather than failing, with truncation reported through `TruncationReport`/`OmittedCount`. Declared references come from the formula authority rather than the dependency graph, so precedent shape and source order survive; the graph supplies reverse reachability and state. Cycle classification is complete, not merely sound: a reachability post-pass guarantees that an unmarked link is safe to expand. Bounded `dependents` selection is canonical — the address-least N of the candidates discovered within budget — so truncated reports do not depend on internal representation.
- Reports are plane-independent: legacy and authoritative FormulaPlane engines return field-identical reports for identical logical workbook state, apart from state stamps, with two documented exceptions — per-cell staleness may be more conservative under FormulaPlane authority after structural edits and before re-evaluation, and reports produced under a binding `max_work` budget are representation-dependent in how much they discover.
- Python bindings expose all five entry points with keyword options matching core defaults, immutable typed reports, `Arc`-backed trace graphs whose node handles outlive their parents, A1-keyed mapping access, bounded `__repr__`, `to_dict()`, value equality and hashing where the core defines them, and a typed exception hierarchy carrying stable codes. Every output enum carries an explicit `Unknown` member so future core variants can never masquerade as a specific known one.
- WASM bindings expose the same surface with owned maps-as-objects reports, decimal-string `u64` stamps and omitted counts, stable tagged enum and value shapes, typed inspection errors including binding-side input validation, and an explicit hand-written TypeScript surface checked against real runtime output by a committed conformance gate.

### Added

- `formualizer_common::{CellAddress, RangeArea}` grid address types with validated construction and total `Display`, plus `address::format_a1_sheet_name` and `a1_sheet_name_needs_quoting`.
- A unified used-extent resolver behind explicit `ExtentPolicy` variants, replacing roughly two dozen ad hoc extent derivations while preserving each site's documented behaviour.
- A streaming, source-ordered semantic reference collector (`engine::refs`) now used by the graph, ingest, and plan-expansion paths, replacing three independent walks.

### Fixed

- Omitted function-argument slots are represented in the AST and evaluated as Excel does, instead of being silently treated as empty text or blank cells. (#277)
- `DCOUNTA` and `DGET` no longer mishandle empty-text criteria. (#281)
- `VLOOKUP` and `HLOOKUP` default `range_lookup` to approximate matching, matching Excel and LibreOffice. (#280)
- Formula results reached through a truly blank cell publish numeric `0`. (#279)
- Date and time text values coerce correctly in arithmetic. (#289)
- Wildcard `*` matching in criteria is correct for all patterns, verified by 64,687,036 differential comparisons against two independent reference implementations. (#284)
- `NPV` accepts variadic value arguments with Excel's reference and array semantics, verified against 804 corpus workbook cells. (#293)
- Saturating area arithmetic at three reference-collection sites, and heap-based iteration replacing recursion in reference collection, so deeply nested formulas no longer risk stack exhaustion.
- `Engine::get_cell` returned the anchor placement's literals for parameterized span families under FormulaPlane authority, producing wrong formula text.


### Python bindings

- Added immutable, typed reports for `Workbook.inspect_cell`, `precedents`, `dependents`, `trace`, and `range_page`, including A1-keyed trace-node access, bounded representations, serialization helpers, state stamps, and inspection-specific exceptions.

### Breaking changes

- VLOOKUP and HLOOKUP now default `range_lookup` to approximate matching (`TRUE`), matching Excel and LibreOffice. Existing formulas that relied on the old exact default should pass `FALSE` or `0` explicitly to keep exact matching; numeric zero and explicitly omitted slots are exact.

#### Parser/SDK 3.0 preparation

- `ASTNodeType` adds the `Omitted` variant for explicitly omitted function-argument slots. Exhaustive Rust matches and AST projections must handle the new node; it remains distinct from empty text and blank-cell values. (#277)
- `formualizer-common` and `formualizer-parse` now share version 3.0.0. The major records the removal of the four 2.0 token-vector/classifier `Parser` constructors in favor of source strings, `TokenStream`, and `ParserBuilder`; the deleted token-vector parser is not restored. See `docs/parser-sdk-3-migration.md`. (#257)
- Reported error/outcome vocabularies (`ExcelErrorKind`, resource and staleness reasons, `ExcelErrorExtra`, common coordinate/address/value errors, `RecoveryAction`, and `ParsingError`) are now non-exhaustive. Downstream output mappings use deliberate future fallbacks, while caller-supplied/core enums remain exhaustive. Serde unknown variants remain a separate wire-compatibility concern. (#257)
- Restored the inexpensive legacy `formualizer_common::value::{datetime_to_serial, serial_to_datetime}` paths as forwarding reexports; the root and `date_serial` paths remain available. (#257)

#### Rust product API freeze (#259)

- **FnCaps representation:** `bits`, `from_bits*`, and the `Flags::{Bits,Primitive}` associated representations widen from `u16` to `u32`, because `LOCAL_ENVIRONMENT` and `MAY_SPILL` use bits above 15. Callers persisting or transporting capability bits must use `u32`; inferred integer types generally need no source change.
- **Checked graph mutations:** `DependencyGraph::add_dependency_edge`, `DependencyGraph::bulk_insert_values`, and `DependencyGraph::set_cell_value_bulk_untracked` now return `Result<(), ExcelError>` instead of `()`. Propagate with `?` or deliberately handle each error; do not discard it.
- **Checked editor transaction:** `Engine::edit_with_logger` now returns `Result<T, EditorError>` instead of `T`. Propagate or handle validation and rollback failures.
- **Opaque cancellation:** `Engine::{evaluate_all_cancellable,evaluate_cells_cancellable,evaluate_until_cancellable,cancellation_token}`, `Workbook::{evaluate_all_cancellable,evaluate_cells_cancellable}`, `EvaluationContext::cancellation_token`, `FunctionContext::cancellation_token`, and `RangeView::with_cancel_token` use `CancelToken` rather than released raw `Arc<AtomicBool>` handles. Existing flags migrate with `CancelToken::from_flag(Arc::clone(&flag))`; context implementers should clone the handle, and hot loops should poll `is_cancelled()`. (#233)
- **Compatibility restoration (A):** The nine `formualizer_eval::builtins::datetime` paths (`create_date_normalized`, `date_to_serial`, `date_to_serial_for`, `datetime_to_serial`, `datetime_to_serial_for`, `serial_to_date`, `serial_to_datetime`, `serial_to_datetime_for`, and `time_to_fraction`), four-argument Excel-1900 `ArrowSheet::new_sparse`, explicit `new_sparse_with_date_system`, and the already-restored common date aliases are available again. The documented typed `#NUM!` hardening remains for 0.7.1 cases that panicked.
- **Accidental-surface narrowing (B):** Raw `TableEntry` lookup, the graph-only `first_load_assume_new` hint, dynamic-reference collector storage, policy/context variants, raw FormulaPlane tuple construction, concrete builtin implementations, default test support, and transaction prototypes are no longer supported public surface. Migrate direct callers to `TableMetadata`, `Engine::first_load_assume_new`, released reference-adjustment conveniences, opaque FormulaPlane descriptors/accessors, `builtins::load_builtins`, the opt-in `test-support` feature, or the retained production editor APIs as applicable; dynamic-reference collector storage has no supported public replacement.
- **Extensible outputs (C):** Approved report/output enums and structs are non-exhaustive. Downstream enum matches need `_`; report struct literals are unsupported, while fields on engine-returned values remain readable. Treat `TargetEvalDelta` sheet IDs and FormulaPlane route IDs/epochs as request/session-local telemetry that must not be persisted or replayed; Python and WASM map future SheetPort errors to generic fallbacks.
- **Package/freeze evidence (D):** CFFI, Python, and WASM Cargo packages are non-publishable on crates.io; use their C library/header, PyPI wheel, and npm channels instead.

### Added

- Exposed all five engine introspection entry points through the WASM workbook binding with owned maps-as-objects reports, decimal-string state stamps and omitted counts, stable tagged enum/value shapes, typed inspection errors, and an explicit TypeScript surface. Duration values retain the inherited `TimeDelta { secs: N, nanos: N }` debug string pending a coordinated pre-1.0 workbook value-convention revision.
- Added the read-only engine-native introspection API: stamped owned cell snapshots, declared precedents, bounded direct and range-covering dependents, bounded BFS trace DAGs with reachability-based cycle/convergence/spill semantics, and revision-checked semantic range pages. Reports preserve canonical sheet casing and source-ordered reference shape without semantic mutation, though inspection may warm snapshot-guarded performance caches. Inspection reports are plane-independent: legacy and authoritative FormulaPlane engines return field-identical semantics for identical logical workbook state, apart from state stamps. Two exceptions apply: after structural edits and before re-evaluation, per-cell staleness may be more conservative (`Dirty`) under FormulaPlane authority than legacy; and reports produced under a binding `max_work` budget are representation-dependent in how much they discover. Bounded dependents retain the first `max_results` by canonical address from all candidates discovered within `max_work`, making non-work-bound truncation deterministic across authorities. `formualizer-eval` exposes serialization behind its new non-default `serde` feature; stamps and request option structs deserialize as well as serialize. Ordinary dependents now leave `Dependent::via` empty, reserving it for spill-anchor queries, and `SnapshotOptions::include_values` uses the same plural spelling as the other options. The `formualizer` facade reexports the inspection DTOs as types-only until workbook and binding integration lands; call the engine methods through `formualizer-eval` for now.
- Added binding-neutral `CellAddress` and open-ended `RangeArea` types to `formualizer-common`, with checked in-grid 1-based construction, finite `RangeAddress` conversions, serde support, and total canonical quoted-sheet A1 display for cells, including `#REF!` sentinels for unchecked zero coordinates and unbounded bijective base-26 rendering beyond XFD. `TryFrom<RangeAddress> for CellAddress` now consistently reports `SheetAddressError`.
- Tables can be defined at runtime, without a serialise-and-reload round trip: `Workbook::define_table` (Rust), `Workbook.addTable` (WASM) and `Workbook.add_table` (Python), plus `tables()` / `getTables()` to list them. Tables are metadata over cells that already exist, so populate the region first; structured references resolve immediately afterwards and edits inside the region propagate. Definitions are validated up front -- unknown sheet, 1-based range violations, an inverted range, a header row with no data rows, and a header count that does not match the range width are all rejected by name rather than silently producing a table whose columns read outside its own range. The WASM binding rejects unknown keys instead of ignoring them and ships `TableDefinition`/`TableMetadata` TypeScript interfaces. (#212)
- Documented the JSON workbook format in `docs/json-workbook-format.md`, including the previously undocumented `tables` entry, the adjacently tagged cell-value shape, and the trap that a `tables` key outside a sheet is silently ignored. The worked example and the documented defaults are covered by tests. (#212)
- Added canonical checked Excel 1900/1904 date-serial conversion APIs to `formualizer-common`, including separate display semantics for serials 0 and 60 and source-compatible Excel-1900 wrappers for the existing common API.
- Added C5 revision-bound compatibility and typed target recalculation plans with deterministic `PlanStale` reasons, cross-engine rejection, value-edit reuse, and shared run-local C4 execution. SheetPort now builds compact symbolic target requests for cells, ranges, names, bounded layouts, and the supported full-v0 native-table output subset; one-shot and batch execution use the same target-plan path with explicit stale policy, option restoration, and baseline output restoration. Layout scan limits count data rows after the header, and `until_marker` retains its prior blank-row termination behavior.
- Completed C4 unified mixed target evaluation. Cell, range, cancellable, until, full, and delta paths now share typed target preparation and one request ledger across legacy vertices, FormulaPlane spans, symbols, spill anchors, and proven value-only cells. Accounted precedent adjacency and exact cache-skip builders preserve demanded closure locality without demotion; event-scoped dirty subleases acknowledge only completely flushed consumer closures; mixed SCCs retain exact demotion and existing cycle semantics; volatile and dynamic references use one epoch plus bounded runtime replanning and monotone workbook widening. Added versioned `TargetEvalDelta` run/region records for legacy, span, and spill writes while preserving unlimited-by-default `EvalDelta` compatibility and explicit caller-limited typed expansion overflow.
- Completed C3 deferred FormulaPlane source-unit preparation. Target discovery selects and consumes an intersecting deferred source package atomically, never splits residual replay ownership, preserves source order and exact replay/failure restoration, and publishes legacy graph work plus checked FormulaPlane authority in one transaction. Off replays the selected package to the legacy graph, Shadow prepares FormulaPlane placements but keeps legacy graph authority, and authoritative experimental mode commits supported placements directly while replaying exact fallbacks; unrelated packages remain staged.
- Completed C2 transactional target preparation for Off and Shadow modes. Typed targets use bounded sheet indexes, immutable function-planning snapshots, exact/Sheets/Workbook widening, prepared addition or replacement publication, scoped graph-source scratch, structured stale reasons, and common exact graph admission across direct, bulk, logged/replay, staged, demotion, fragmented, compressed-fallback, and generic graph mutation seams. C3 extends the same transaction to deferred FormulaPlane source packages.
- Added C1b exact non-materializing request topology for FormulaPlane mixed-cache skips. Candidate, edge, and retained-byte overflow now retains spans and selects paged/indexed, bounded in-memory, explicit native delete-on-drop scratch, or work-accounted no-disk repeated-pass construction; cache compilation publishes only complete topology or an explicit skip. Retained mixed-cache and topology/schedule-discovery scratch budgets are now active with typed exhaustion, strategy/pass/cap telemetry, skip streaks, and operator guidance; C2 activates graph admission and graph-source scratch.
- Added a reproducible C1-exit structural and cold-process gate at `docs/architecture/evaluation-c1-exit-gate.md`.
- Added the C1a evaluation resource contract with one all-unset-by-default `EvaluationBudgets` configuration, optional envelope-to-budget derivation, field-level legacy mapping diagnostics, checked request ledgers, common-mode work/deadline checkpoints, and typed resource exhaustion details. Explicit budget fields win only their conflicting legacy mappings; retained/scratch/cache settings are observational, and graph/materialization admission remains declarative until C2's single composed transaction. Recommended named constructors/default sets are deferred until calibration and will not be enums or modes.
- Added observational evaluation-resource telemetry with monotonic request IDs, typed cap reasons, FormulaPlane topology/cache/materialization/dirty-lease counters, staged preparation and phase timings, replay-spool storage counters, and cold-process load-envelope JSON reporting. Defaults, fallback strategies, errors, and evaluation behavior are unchanged.
- Added Calamine-backed in-memory XLSX loading to the native Python binding and made it the default for `Workbook.from_bytes` and `load_workbook_bytes`; Pyodide retains its Umya fallback.
- Added backend-neutral source-family ingest with anchor-once FormulaPlane authority for proven complete domains. Calamine supplies bounded XLSX evidence and exact replay for eager and deferred loading, with structural-edit, cycle-demotion, and source-family telemetry support.
- Added registry-owned function semantic contracts so safe current and future ordinary functions can use FormulaPlane authority without a secondary supported-name list, while exceptional and untrusted functions continue to replay conservatively.
- Added bounded fragmented shared-formula evidence, exact coordinate-disposition replay, one-analysis Shadow preparation, and typed ordinary-exception ownership as the replay-only foundation for transactional fragmented authority.
- Added graph-owned FormulaPlane dirty authority with generation-leased region and exact-span work, unified sparse legacy dirty tracking, explicit global-invalidation telemetry, and retry-safe prefix acknowledgement.
- Added exact span-region structural dirty events so row and column shifts schedule only moved or cleared placement intervals while preserving post-lease retry identity.

### Improved

- Consolidated the engine's graph, ingest, and planning AST dependency walks behind one crate-private semantic reference collector without changing their independent policies or behavior.
- Consolidated whole-axis and partially open used-extent resolution behind one internal policy-driven resolver while preserving the distinct runtime, virtual-dependency, graph, and semantic policies.
- Narrowed the published eval API to stable table metadata, reference-adjustment conveniences, builtin loading/date compatibility helpers, and opaque Formula Plane descriptors; test-only helpers now require the non-default `test-support` feature. (#259)
- Upgraded Calamine-backed XLSX loading to Calamine 0.36 and a single-pass value/formula metadata stream, preserving formula-only worksheet dimensions, cached-value semantics, load limits, shared-formula relocation, and malformed-family fallback.

### Testing / internal

- Added a fixed-seed AST-to-edge parity harness covering structural edits, compressed ranges, symbol vertices, dirty propagation, and mutation-tested edge-maintenance failures.
- Pinned a fourth AST-to-edge finding, `AST_EDGE_INSERT_SHIFTS_NAME_VERTEX_ONTO_GRID`: a default-sheet row or column insert shifts a workbook-name vertex off its `Sheet1!$A$1` home onto an addressable cell, so references resolved afterwards bind to the name vertex instead of the cell. The campaign carve-out for default-sheet inserts is narrowed to that shape, restoring formula and value overwrite coverage on the default sheet.

### Fixed

- `Engine::get_cell` now reconstructs placement-specific literals for parameterized authoritative FormulaPlane families instead of returning the anchor placement's formula text for every member.
- Whole-surface ranges whose cell count overflows `u32` are now always kept as compressed range dependencies instead of panicking or attempting expansion.
- Lookup wildcards now let `*` consume zero or more characters instead of at most one, including multi-star backtracking, `?` adjacency, `~` escapes, case-insensitive Unicode text, and exact-mode VLOOKUP/HLOOKUP/MATCH plus wildcard-mode XLOOKUP/XMATCH. (#284)
- `NPV` now accepts up to 254 scalar, reference, and array cash-flow arguments in argument order. Text, blank, and logical cells in references are ignored without consuming periods, direct text and computed-array text return `#VALUE!`, omitted slots count as zero, errors propagate, and `rate=-1` returns `#NUM!`. (#293)
- Arithmetic operators now coerce supported en-US date, time, and datetime text operands to serial numbers using the workbook's 1900 or 1904 date system. Two-digit years in slash and English month-name forms use the Excel 29/30 window, ISO dates require a four-digit year, slash dates use month/day/year ordering, and `T` is restricted to ISO datetimes; aggregate, comparison, criteria, `N`, and concatenation text semantics remain unchanged. (#289)
- `DATEVALUE` and `TIMEVALUE` now use the shared deterministic temporal parsers: two-digit date years use the Excel 29/30 window, surrounding whitespace is accepted, interior slash-date whitespace is rejected, and whitespace around time separators is accepted. `DATEVALUE` retains its pre-existing unambiguous day/month/year and year/month/day slash fallbacks for compatibility. (#289)
- Date, datetime, time, and duration literals used by `*`, `/`, `^`, unary minus, or `%` now convert under the workbook's selected date system instead of implicitly using the Excel-1900 system. (#289)
- Database functions now distinguish explicit empty text from genuinely blank field cells: `DCOUNTA` counts the former, while `DGET` ignores the latter, matching LibreOffice and Excel. (#281)
- Formula cells whose final result passes through a truly blank cell now publish numeric `0`, matching Excel and LibreOffice, while direct blank-cell inspection and range semantics remain unchanged. Blank elements in spilled results are likewise published as zero; JSON persistence and Python/WASM value surfaces now expose these computed results as `0` instead of empty/`None`/`null`. (#279)
- Explicitly omitted function arguments now retain their syntax through parsing and canonical rendering, coerce as Excel empty arguments (`0`, `FALSE`, or empty text by context), and count as numeric zero in aggregates without changing absent optional defaults or blank-reference behavior. (#277)
- `TEXT` with an empty format string (explicit `""` or an omitted second argument) now returns empty text, matching Excel and LibreOffice; it previously rendered the value as if unformatted. (#277)
- Restored the released `formualizer_eval::builtins::datetime` conversion helpers and sparse-sheet constructor compatibility paths, including explicit 1904 date-system handling. The eval decode wrappers retain released 0.7.1 component-clamping and negative-fraction behavior; as an intentional safety deviation, infinities and chrono date/duration overflow now return typed `#NUM!` instead of panicking. (#259)

- CFFI targeted cell evaluation now reports both the targeted graph-preparation error and the distinct full-graph fallback error when both preparation attempts fail.
- CFFI canonical formula rendering now honors the selected Excel or OpenFormula dialect while retaining canonical Excel output and existing input/output contracts.
- CFFI status errors now use canonical JSON string escaping, preserving valid round-trippable messages for control characters and other special text.
- CFFI cell and rectangular block entry points now reject zero, out-of-grid, and overflowing Excel coordinates with a typed status before touching workbook state, preventing packed-coordinate aborts.
- CFFI `RangeAddress` JSON and CBOR boundaries now reject zero, inverted, and out-of-grid ranges before formatting or workbook reads, preventing malformed-bound underflow and out-of-grid materialization.
- Python `Workbook.undo()` and `redo()` now invalidate the compatibility cell cache before replay, so `Workbook.get_value()` and existing `Sheet` handles immediately reflect authoritative values after successful, failed, or no-op replay without introducing a cache-lock panic.
- Authoritative FormulaPlane fallback no longer reports a false `#CIRC!` when a statically indexed compressed range contains the formula cell but `INDEX` selects a different cell. Whole-row, whole-column, bounded, omitted-column, zero, negative, and genuine self-selection cases now follow the same reference semantics in Off and authoritative modes.
- Deferred malformed-formula retries now publish one parse diagnostic per successful ingest event instead of duplicating a source record during authoritative replay; a later distinct ingest of the same malformed source still emits its own diagnostic.
- Compatibility-widened target preparation now performs the same final graph, authority, staged, symbol, provider, and relevant function-semantic revision validation as exact preparation before publishing. Provider changes and injected final-validation failures remain atomic in Off, Shadow, and authoritative modes.
- Prepared `sheetport-spec` 0.3.1 as a validator-only patch: layout `header_row` now rejects 0 and values above Excel's 1,048,576-row limit. The manifest protocol and canonical schema remain fio 0.3.0, while product dependency requirements adopt a 0.3.1 floor so packaged behavior matches the path source tested in the workspace. (#258)
- Authoritative FormulaPlane families are no longer replayed to the legacy graph when an unrelated function is registered between planning and commit. Semantic validation now scopes registry changes to the functions used by each prepared family while retaining conservative fallback for affected functions and truncated change history. (#241)
- Range-consuming builtins accept a scalar as a 1x1 array, matching Excel. `=TRANSPOSE(2)`, `=SORT(2)`, `=UNIQUE(2)`, `=TAKE(2,1)`, `=DROP(2,0)`, `=FILTER(2,1)`, XLOOKUP, XMATCH, SORTBY, GROUPBY, PIVOTBY, CHOOSEROWS and CHOOSECOLS previously returned `#REF!`. An error argument now also keeps its own error rather than being masked, so `=TRANSPOSE(NA())` is `#N/A` instead of `#REF!`. The promotion is opt-in through the new `ArgumentHandle::range_view_or_scalar`, not a change to `range_view` itself, because several builtins use a resolution failure as type dispatch: statistical functions coerce a direct scalar (`=MEDIAN(TRUE)` is `1`) while skipping a range cell of the same type, and a D-function's scalar criteria argument is an error rather than an empty criteria block that matches every row. (#224)
- Layout selectors are no longer capped by a declared row limit. `LayoutDescriptor.max_scan_rows` bounded both the scan and the preparation envelope, which meant a contiguous table longer than the limit failed with `LayoutExhausted` -- at the 100,000-row default, a 150,000-row table was unresolvable -- while a short table on a large sheet still declared a 100,000-row envelope and evaluated rows that were not part of its output. Both bounds are now derived from the sheet's used range via `sheet_dimensions()`, which is a property of the workbook and so stays deterministic for a given input. Measured cost is unchanged: preparation tracks populated cells, not envelope extent, and is flat (~9ms) across used ranges from 200 to 1,000,000 rows. `max_scan_rows` is removed from the manifest and the `fio-0.3` schema, which returns that schema to its released shape; manifests that set it are rejected as unknown fields, and manifests that omit it are unaffected. `SheetPortError::LayoutExhausted` remains as a defensive guard for stores that cannot report dimensions.
- Unified engine cancellation onto a single source. Cancellation previously arrived through two independent channels: `PrepareTargetsOptions::cancel` drove the target-preparation checkpoints while a separate `cancel_flag` argument drove evaluation, so a caller that supplied only the evaluation flag -- which is what `evaluate_targets_cancellable` did -- got no cancellation at any preparation checkpoint, the phase that dominates on large workbooks. There is now one `CancelToken` carried on `TargetEvalOptions` (renamed from `PrepareTargetsOptions`, which remains as a deprecated alias), hoisted onto the engine for the duration of a call and read by both phases. `CancelToken` replaces the raw `Arc<AtomicBool>` in engine, workbook and SheetPort entry points, hiding the representation so richer cancellation can be added without a break; use `CancelToken::from_flag` to adopt a flag you already own. (#229)
- Reduced the pre-0.8.0 public surface further: telemetry, resource-reason and ledger-error enums are now `#[non_exhaustive]` so new variants are not breaking changes, and the redundant `evaluate_targets_cancellable` / `evaluate_recalc_plan_cancellable` wrappers were removed in favour of the `..._with_options` and `..._with_controls` forms they were strict subsets of. Caller-supplied configuration enums such as `DiskScratchPolicy` remain exhaustive on purpose. (#229)
- Narrowed public API that was exposed only for tests and benchmarks before it could be frozen by a release: the engine's `ResourceLedger` accounting mechanism is now crate-internal (its budget, snapshot and error types remain public), `set_before_target_preparation_commit_hook` is crate-internal, and the cross-crate test seam `set_before_prepared_span_commit_hook` plus SheetPort's `workbook_for_benchmark` are marked `#[doc(hidden)]`.
- Date functions now follow the workbook's 1900 or 1904 date system instead of assuming 1900. `DATEVALUE`, `EDATE`, `EOMONTH`, `YEAR`/`MONTH`/`DAY`, `HOUR`/`MINUTE`/`SECOND`, `DAYS`, `DAYS360`, `DATEDIF`, `YEARFRAC`, `WEEKDAY`, `WEEKNUM`, `ISOWEEKNUM`, `WORKDAY`, `NETWORKDAYS`, and the bond day-count functions previously decoded every serial through a fixed Excel-1900 mapping, so a 1904 workbook stored dates correctly and then computed answers four years and a day off -- `YEAR(A1)` disagreed with `TEXT(A1,"yyyy")` on the same cell. Date-bearing literals passed to these functions were affected by the same fixed mapping and now resolve through the workbook's system as well. (#225)
- Planning snapshot capture is now validated against the functions a request actually named instead of the global registry epoch. Registering any function previously invalidated concurrent captures, which surfaced as spurious `RegistryChangedDuringCapture` results and, under authoritative FormulaPlane mode, as family fallbacks that misreported an unrelated registration as a provider revision change. Capture stays conservative when the bounded semantic change log has been truncated. (#223)
- Date and datetime values now retain the workbook's 1900 or 1904 date system through Arrow storage, sparse overlays, and FormulaPlane writes instead of decoding through an implicit 1900-only path.
- Dynamic-array builtins (`FILTER`, `SORT`, `SORTBY`, `UNIQUE`, `TRANSPOSE`, `TAKE`, `DROP`, `XLOOKUP`, `XMATCH`, `GROUPBY`, `PIVOTBY`) now accept computed array arguments such as `FILTER(A1:A3,B1:B3="x")`, `SORT(SEQUENCE(3))`, and `TRANSPOSE({1,2})` instead of rejecting them with `#REF!`. Their data arguments no longer claim reference semantics, and `range_view` resolves through the same cached value-or-reference path, so references keep their lazy views, volatile expressions are still evaluated once, and genuine reference failures are preserved. (#216, #218)
- Aggregate and range-consuming math functions now consume computed arrays through one cached value-or-reference path, preserving conditional reference fallback, exact errors, cancellation, and single evaluation of volatile or custom expressions. (#211)
- `TEXT` date formatting now follows the workbook's 1900 or 1904 date system, preserves Excel's display-only serial 60, rejects invalid serials safely, and carries rounded times across day boundaries without corrupting near-midnight datetimes. (#210)
- `CONCAT` and `TEXTJOIN` now expand range and computed-array arguments in row-major order with exact blank/error handling and Excel's 32,767-character limit, while legacy `CONCATENATE` retains scalar top-left behavior. (#209)
- Formula tokenization now accepts TAB, CR, LF, and CRLF as lexical whitespace in both tokenizer frontends while preserving raw source spans. Only an ASCII space can spell the range-intersection operator, whose semantic token/AST value is canonicalized to `" "` even when its source run also contains line breaks or tabs.
- Structural row and column deletes now rewrite invalidated reference leaves to ordinary `#REF!` error literals instead of magic `#REF` sheet sentinels. This preserves lazy `IF`/`IFERROR` semantics, dependency rewiring, formula display/reparse, undo/redo, whole-axis adjustment, cross-sheet locality, and legitimate worksheets named `#REF` across legacy and FormulaPlane structural paths.
- Fixed deferred `evaluate_cells_with_delta` requests missing transitive formula precedents staged on non-target sheets.
- Fixed experimental FormulaPlane capacity fallbacks evaluating legacy readers before required span results were available. Unsafe requests now transactionally demote exactly their scheduled spans, retain pending dirty regions across failed attempts, and fail closed behind a finite materialization limit.

### Security and hardening

- Added a credential-free release preflight that packages each Rust release track through a temporary local registry in publish order, verifies downstream archives against the exact prospective upstream bytes, and rejects source drift when a package version already exists on crates.io. Clean-checkout, symlink, archive-member, checksum, exclusive-lock, and exact isolated-tool guards keep the pre-tag check transactional; tag workflows repeat it before any publish job receives a registry token. (#257, #258)
- Upgraded PyO3 and NumPy bindings to 0.29.0, resolving the PyO3 out-of-bounds iterator and missing closure `Sync` advisories. Upgraded `pyo3-stub-gen` to 0.23.0, made existing clone-based Python extraction explicit, and aligned generated stubs with the private maturin extension-module path while preserving the public `formualizer` package API.

### Performance

- Experimental authoritative FormulaPlane evaluation now contracts proven span-disconnected legacy islands into the compressed legacy scheduler while retaining span-connected legacy producers in the mixed cycle detector. Dynamic references, names, spills, structural-summary uncertainty, and boundary-discovery overflow fail closed to the global mixed planner. (#251)
- Mixed FormulaPlane topology overflow now retains the bounded, accounted prefix and its indexes instead of discarding successful compile work. Paged schedule discovery reuses complete cached sources and derives only the uncovered tail exactly, so dense growing-range workbooks avoid rebuilding the same overflowed topology on every recalculation.
- Experimental authoritative FormulaPlane evaluation now uses the existing consumer-read interval index when exact schedule construction falls back after topology-cache overflow, avoiding full-table scans per dirty formula during the first evaluation of bulk-loaded workbooks. (#240)
- Experimental authoritative FormulaPlane evaluation now caches accounted consumer and precedent topology across warm and value-only evaluations. Exact graph, authority, semantic, provider, and dynamic-reference revisions invalidate or bypass stale cache generations; candidate, edge, and retained-byte cache overflow selects exact paged/run/native/repeated-pass request topology without span demotion, while span-free and warm no-dirty requests retain topology-free sparse paths.
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

[Unreleased]: https://github.com/PSU3D0/formualizer/compare/v0.8.4...HEAD
[0.8.4]: https://github.com/PSU3D0/formualizer/compare/v0.8.3...v0.8.4
[0.8.3]: https://github.com/PSU3D0/formualizer/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/PSU3D0/formualizer/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/PSU3D0/formualizer/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/PSU3D0/formualizer/compare/v0.7.1...v0.8.0
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
