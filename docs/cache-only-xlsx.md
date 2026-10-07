# Cache-only XLSX recalculation

The default-enabled `xlsx-recalc` feature exposes `recalculate_xlsx_bytes` and native `recalculate_xlsx_file`. The minimal feature graph uses Calamine, not Umya. Existing rich workbook APIs and legacy `recalculate_file` are unchanged. Both `formualizer-workbook` and the `formualizer` facade enable this feature by default; use `default-features = false` to omit XLSX dependencies from minimal builds. Default availability does not automatically route existing recalculation calls through this strict cache-only path.

```rust,ignore
use formualizer_workbook::{recalculate_xlsx_bytes, XlsxRecalculateOptions};
let result = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default())?;
// result.bytes, result.summary, result.formula_cells,
// result.cache_cells_changed, result.worksheet_parts_changed
```

Counters:

- `formula_cells` and `summary.evaluated` count source formula cells, dynamic-array anchors included. Generated spill members are not formulas and are not counted.
- `cache_cells_changed` counts physical cells whose cache was inserted, replaced or cleared: ordinary formulas, anchors, written or inserted spill members and cleared obsolete members. It can exceed `formula_cells`. Attribute-only edits (an anchor's `ref`, the worksheet dimension, row `spans`) are not cache changes.
- `worksheet_parts_changed` counts changed worksheets only. Metadata, relationship and content-type edits are not counted.

A native example requires explicit input and output paths:

```sh
cargo run -p formualizer-workbook --release --no-default-features \
  --features xlsx-recalc --example cache_recalculate -- input.xlsx output.xlsx
```

The `formualizer recalc` command, the Python `recalculate_xlsx_*` functions and the WASM source facade all use this path. The sections below are the authoritative description of what it admits and refuses.

## Supported at a glance

Recalculated, with formula text and untouched package content preserved:

- Workbooks as saved by Excel (desktop, Mac and Online), LibreOffice, Google Sheets, openpyxl and other ZIP writers, including Info-ZIP `zip`. Their containers' metadata-only ZIP extra fields (Excel's growth-hint padding, extended timestamps, Unix owners, NTFS times) and data descriptors are admitted; see [ZIP containers](#zip-containers). Excel's routine extension markup (`x15:workbookPr`, form-control and OLE-object anchors, x14 data validations, table revision IDs) is admitted; see [Excel extension markup](#excel-extension-markup).
- Ordinary and shared formulas over values, other formulas and other sheets, using the engine's built-in function library.
- Supported defined names: constants, absolute cell/range names and grounded formula names (see [Ownership and writeback](#ownership-and-writeback)).
- Dynamic arrays: new multi-cell spills from ordinary formulas, and existing dynamic-array anchors that grow, shrink, collapse or become blocked. `A1#` and `_xlfn.ANCHORARRAY(A1)` read the current spill. A spill blocked by existing content publishes `#SPILL!` as a formula result.
- Legacy fixed-extent (CSE) array formulas, including elementwise `IF` such as `SUM(IF(A1:A3>0,A1:A3))`.
- Legacy implicit intersection in formulas Excel calculated (listed in `xl/calcChain.xml`); see [Legacy implicit intersection](#legacy-implicit-intersection).
- Excel tables: structured references, bare table names and calculated columns whose every row carries a worksheet formula.
- Volatile functions (`TODAY`, `NOW`, `RAND`, `OFFSET`, `INDIRECT`), sampled once per run.
- Formula errors such as `#DIV/0!`, `#NAME?` or `#SPILL!`. These are calculated results, reported as error cells; they are not refusals.
- Calls to functions the engine does not implement, such as add-in (`_xll.`) or VBA/macro functions. They evaluate to `#NAME?` and are written, with the reason in the receipt (see [unimplemented functions](#unimplemented-functions)).
- Workbooks set to "precision as displayed" (`fullPrecision="0"`). They are computed in full precision (see [numeric precision](#numeric-precision)).
- Blocked spills. A dynamic-array result whose spill range is occupied by a value, another formula or another spill's cells leaves the anchor as `#SPILL!` and recalculation continues: the blocking formula keeps its own value, readers of the anchor see `#SPILL!` and `A1#` readers `#REF!`. The anchor is written with the blocked-anchor encoding (`#SPILL!` cache, `ref` collapsed to the anchor) and the run exits 0. When two spill rectangles collide and neither anchor lies inside the other's rectangle, the anchor first in (sheet, column, row) order spills and the other is `#SPILL!`, whatever the evaluation order. This is engine policy, not an Excel equivalence claim.

Refused as a whole, with nothing written (CLI exit 2):

- What-If data tables, external workbook links, rich values, metadata other than dynamic-array metadata, and digitally signed packages.
- `SUBTOTAL`/`AGGREGATE` ranges over hidden or filtered rows, or whose hidden-row intersection cannot be proved.
- Multi-cell shared formulas whose sheet qualifiers look like cell references (`'Q1'!`, `'FY2024'!`).
- Table features outside the validated subset: connection-backed tables, table-managed formulas missing from some row, unknown tables or columns, `[#This Row]` outside the data body, computed `INDIRECT` text in table-bearing workbooks, and defined names that refer to tables.
- Unsupported or cyclic defined names, and results that cannot be cached faithfully: circular references (`#CIRC!`), functions the engine recognizes but does not implement (`#N/IMPL!`), non-finite numbers and results that are not current after evaluation.
- Stored formulas the parser cannot read (`unparseable formula`).
- Malformed or ambiguous packages, ZIP features outside the admitted container subset (ZIP64, encryption, entry comments, unknown extra fields), extension markup that reuses a SpreadsheetML name outside the positions Excel writes it in (see [Excel extension markup](#excel-extension-markup)), and inputs over the resource bounds.

## Ownership and writeback

The source package is authoritative. A reconstructible evaluator consumes its values, ordinary/shared formulas and supported defined names. No rich document graph or independent evaluator is introduced.

Calamine calculation import supports numeric, Boolean, quoted text and error constants, absolute cell/range names and grounded formula names. Sheet-local definitions retain their scope and shadow workbook definitions. Grounded references must be absolute and identify an existing sheet (an unqualified absolute reference is allowed only for a sheet-local definition). Formula names support arithmetic, name dependencies and the explicit pure-function subset `SUM`, `AVERAGE`, `MIN`, `MAX`, `COUNT`, `COUNTA`, `ABS`, `ROUND`, `ROUNDUP`, `ROUNDDOWN`, `IF`, `AND`, `OR`, `NOT`. Engine name registration owns dependency binding, including formulas loaded before their names.

Strict recalculation conservatively refuses unsupported non-built-in definitions even if unused, and refuses cyclic names before evaluation. Relative, multi-area, external, structured/3D, array-valued and context-dependent/reference-producing definitions are not admitted. Unsupported `_xlnm.*` document metadata (including print/filter definitions) is preserved without calculation import only when no calculation/name expression references it; a reference causes explicit refusal. Supported built-in definitions may be calculated normally. Original error constants remain errors, not metadata-loss signals. All workbook name XML is preserved byte-for-byte. The public range/literal name DTO and JSON schema are unchanged; this does not repair the separate legacy Umya import path.

Preflight uses namespace-aware XML events and source offsets. Changed worksheet XML is assembled once from non-overlapping edits to formula cache types/values. Formula XML, styles, drawings and other untouched content retain their original bytes. Existing dates are evaluated in the source workbook's epoch; serial egress avoids lossy native-date conversion, including Excel-1900 serial 60.

The admitted ZIP32 package is edited surgically. ZIP7 supplies compression/CRC generation for changed and added payloads. Untouched members keep every byte (local header, extra fields, payload, data descriptor and central record); only the central local-offset fields of relocated members change. A changed member is rewritten in one simple form: its name, versions, general-purpose flags (less the data-descriptor bit), compression method, DOS time and attributes are kept, its new CRC-32 and sizes are written in both its local and its central record, and it has no extra fields and no data descriptor. When a dynamic-array metadata part must be added, it is written as one deflated ZIP32 local record before the original central directory with a matching central record after the existing ones. The end record's entry counts, directory size and offset are patched. Duplicate names, entry-count overflow, ZIP64 sizes/offsets and the output/expanded-byte limits are refused, and the resulting package is re-audited before it is returned. The archive comment is preserved. A true cache no-op returns the entire original byte sequence.

Calamine's cached-value decoder is not authority for formula results. If an old formula cache uses a representation it cannot decode faithfully, a bounded transient ingestion view clears that cache only. The original package is still used for comparison/writeback, including exact no-op output. Literal dependency values are never cleared this way.

## Numeric precision

Formulas are computed in IEEE 754 double precision, as Excel does, but the order of floating-point operations can differ (for example, `SUM` over a long range adds in a different order). Results can then differ from Excel's in the last binary digits, beyond anything Excel displays. A numeric cache is therefore treated as current when it agrees with the computed number within one unit in the 15th significant digit (Excel's display precision):

- both are finite numbers and equal, or
- they have the same sign and `|cached - computed| <= 10^(E - 14)`, where `E = floor(log10(max(|cached|, |computed|)))` is the decimal exponent of the larger magnitude. The bound is the double nearest `10^(E - 14)`, so for subnormal magnitudes it tightens towards exact equality.
- Exact zero agrees only with exact zero (either sign). A cached `0` against a computed cancellation residue such as `5.55e-17` is stale.

Such a cache keeps its original bytes, including Excel's 17-digit spelling, is not counted in `cache_cells_changed`, and leaves `--check` current when nothing else changed. For example, Excel's `53433.999999999949` against a computed `53433.99999999998`, or `8.8664999999999985` against `8.8665`, stay as they are. A larger difference, a type change (number to text, Boolean or error) and any non-numeric result are written as before. The rule applies to formula cells and dynamic-array anchors; generated spill members are compared byte for byte. Values of dependent formulas are always computed from the engine's own results, never from kept caches.

"Precision as displayed" (`<calcPr fullPrecision="0"/>`) is not refused and not emulated: formulas are computed in full precision. Excel in that mode rounds stored values to their number format, so its results can differ from ours in workbooks that rely on that rounding. The `calcPr` element is left unchanged.

## Unimplemented functions

A formula that calls a function the engine does not implement evaluates to `#NAME?`, as it does in Excel without the providing add-in. This covers add-in functions (`_xll.EURO`, `_xll.BDP`), VBA and XLM macro functions, functions of newer Excel versions and misspelled names. The workbook is not refused: the `#NAME?` result is written like any other formula error. The receipt says why. Each listed error location carries the engine's reason (`Unknown function: NAME`, or `Undefined name: NAME` for a name that is not defined), and the run reports every unknown function with the number of cells calling it, complete even when the location list is truncated:

- CLI: `errors[].message` and `unknown_functions` (see [the JSON schema](cli.md#json)).
- Python `recalculate_xlsx_file`/`recalculate_xlsx_bytes`: `summary["error_summary"][token]["messages"]` (parallel to `locations`) and `summary["unknown_functions"]`.
- WASM `recalculateXlsxBytes`: `summary.error_summary[token].messages` and `summary.unknown_functions`.
- Rust: `RecalculateErrorSummary::messages` and `RecalculateSummary::unknown_functions`.

The reason is recovered from the cell's own formula with the evaluator's lookup rules, because computed values are stored without their messages. A cell that only inherits `#NAME?` from a precedent has no reason of its own (null). Formulas that call functions the engine recognizes but does not implement (`#N/IMPL!`) remain refusals.

## ZIP containers

Package admission audits the ZIP structure itself, before ZIP7 indexes it. The central directory is authoritative: every local header must agree with its central record, and the members (with any data descriptors) must tile the bytes before the directory without gaps or overlaps.

| Producer | Container | Admitted |
| --- | --- | --- |
| Excel (desktop, Mac, Online) | `0xA220` growth-hint padding in local headers | Yes |
| LibreOffice, Google Sheets | Data descriptors (`PK\x07\x08`), zero CRC/sizes in local headers | Yes |
| openpyxl, XlsxWriter, Python `zipfile` | Plain ZIP32 | Yes |
| Info-ZIP `zip` | `0x5455` extended timestamp and `0x7875` Unix UID/GID, local and central | Yes |
| 7-Zip and other Windows archivers | `0x000A` NTFS times | Yes |

Each extra block is parsed in full and must be well formed: `0xA220` is the Microsoft growth hint (signature `0xA028`, a padding-size word, then zero padding), `0x5455` a flag byte with one 32-bit time per flag (central records may carry only the modification time), `0x7875` version 1 with sized UID/GID, and `0x000A` a zero reserved word with at most one 24-byte times attribute. A data descriptor is admitted for 32-bit members with or without its signature; its CRC-32 and sizes must equal the central record's, and the local header's must be zero or equal to them.

Still refused: ZIP64 (sizes, offsets, the `0x0001` extra field or 64-bit descriptors), encryption (traditional, strong or AES), compression methods other than stored and deflate, entry comments, any other extra field ID (for example the `0x4453` NT security descriptor and `0x7075` Unicode path), duplicate or malformed extra blocks, split, prefixed or self-extracting archives, and unaccounted bytes between members. The archive comment is admitted. None of the producers above writes entry comments.

## Excel extension markup

Excel 2010 and later write extension markup into most workbooks. Extension elements outside the SpreadsheetML main namespace are ignored unless they reuse the local name of an element the readers interpret (`workbookPr`, `sheet`, `definedName`, `row`, `c`, `f`, `v`, `mergeCell`, ...). Calamine, which ingests the workbook, matches such elements by local name, so a lookalike in the wrong place could change the date system, the defined names or the cell data. A lookalike is admitted only in the form and position Excel writes it, where neither Calamine nor the source index reads it, and refused everywhere else:

| Markup | Written by | Admitted where |
| --- | --- | --- |
| `<x15:workbookPr chartTrackingRefBase="1"/>` | Excel 2013+ | Once, in `workbook/extLst/ext` with URI `{140A7094-0E35-4892-8432-C4D2E57EDEB5}`, with `chartTrackingRefBase` as its only attribute. The date system always comes from the main `workbookPr`. |
| `xdr:row` in form-control and OLE-object anchors | Excel 2010+ | In the `from`/`to` of an `anchor` inside `mc:AlternateContent`, outside `sheetData`. |
| `xm:f` in x14 data validations, conditional formats and sparklines | Excel 2010+ | Under `worksheet/extLst/ext`. |
| `mc:Ignorable` and `xr:uid` on `table`, `xr3:uid` on `tableColumn` | Excel 2016+ | On those elements; table parts stay byte-for-byte unchanged. |

Still refused: an `x15:workbookPr` with a `date1904` or any other attribute, in another position or extension, or repeated; main-namespace duplicates such as a second `workbookPr` in the extension list; extension defined names (`x15:definedName`); any lookalike inside `sheetData`, where Calamine's cell reader matches `row` by local name and reads every element inside a cell as cell content; extension merge cells; other drawing names (`xdr:c`) and anchors outside markup-compatibility content; lookalikes in unknown namespaces; and other namespaced table attributes. Calamine resets its own 1904 flag from an `x15:workbookPr`, but source recalculation reads Calamine dates as raw serials and never uses that flag.

## Dynamic arrays

Recalculation supports this dynamic-array subset:

- **Existing anchors.** A formula cell with `cm` on a top-left `t="array"` formula whose `ref` is its prior extent, where `cm` resolves through `xl/metadata.xml` (one-based `cm` into `cellMetadata`, `rc/@t` naming the `XLDAPR` metadata type, zero-based `rc/@v` into the XLDAPR `futureMetadata` blocks, the `{bdbb8cdc-fa1e-496e-a857-3c3f30c029c3}` extension and the 2017 `dynamicarray` namespace with `fDynamic="1"`). A `fCollapsed="1"` record is accepted only for a one-cell extent.
- **New spills.** An ordinary (non-shared) formula whose result is a multi-cell spill becomes an anchor.
- **Readers.** `A1#` and `_xlfn.ANCHORARRAY(A1)` read the current spill.

Ownership is source-declared. Cells serialized inside an admitted anchor's prior `ref` (anchor excluded) are treated as generated children: their caches are not inputs, are masked during ingestion and are recalculated from the anchor. A separately authored formula inside a prior extent, overlapping extents and source shared-family members carrying `cm` are refused. A one-cell anchor keeps its identity for `A1#`. A fresh ordinary formula with a one-cell, error or blocked result stays a plain scalar formula; dynamic-array identity for it is not inferred from function names.

Publication writes the current shape:

- A successful spill writes typed member caches, inserting missing cells and rows in order with the worksheet's prefix. The anchor's `ref` becomes the current extent, and the dimension grows when needed (it never shrinks).
- Obsolete children have their cache and type removed. Their elements, styles and comments are kept.
- Shrink and collapse clear the members that are no longer covered. A collapsed anchor keeps `t="array"`, its `cm` and `ref="<anchor>"`.
- A blocked anchor gets a typed `#SPILL!` cache, `ref` set to the anchor cell and no children, and keeps its `cm`. This means a later reopen claims no stale cells, and the next recalculation can expand again. An erroring anchor is encoded the same way with its own error token. Readers of a blocked or erroring spill are `#REF!` (the engine's semantics).
- A new or rebound anchor reuses the lowest existing XLDAPR record with `fCollapsed="0"`. If there is none, exactly one future block and one cell block are appended, without renumbering existing records. A shared record's `fCollapsed` is never toggled. If the package has no metadata part, a minimal canonical `xl/metadata.xml` is added, together with a non-colliding workbook relationship ID and a content-type override.
- An unchanged anchor requests no metadata edit, so recalculating a published output again is a byte-identical no-op when its computed caches are unchanged (see volatile snapshots below).

This writer makes no claim of Excel equivalence. The encodings follow the format documentation and independently produced packages; no Excel execution oracle was used. In particular, Excel's own encodings of blocked or erroring anchors and of `SEQUENCE(0)` are not verified. The engine returns `#VALUE!` for `SEQUENCE(0)`, where Excel documents `#CALC!`.

## Legacy fixed-extent arrays

A top-left `t="array"` formula with a finite `ref` and no `cm` binding is a legacy CSE array. Its children are owned generated caches. The expression is evaluated once and fitted to the declared rectangle: scalars (including errors) fill it, single rows/columns broadcast, missing positions become `#N/A`, and excess positions are truncated. Empty members become numeric zero, as on the dynamic-spill path. A single-cell declaration takes the top-left result without spilling. The declared extent is capped at admission; large intermediate arrays still materialize fully.

The writer keeps the original `t="array"` and `ref`, without adding metadata or `cm`; the extent never grows or shrinks. Other dynamic spills cannot occupy its children. `A1#` and `ANCHORARRAY(A1)` on a CSE anchor return `#REF!`; ordinary range readers see fitted values. Child formulas/metadata, overlaps and over-cap declarations are refused.

When openpyxl re-saves a published dynamic spill, it can discard XLDAPR metadata while retaining the array formula. A subsequent recalc treats this as a fixed-size CSE array, not a dynamic spill. Recalc must still be the last writing step to retain caches. These are explicit policies, not claims of Excel equivalence.

Array-condition `IF`, including `SUM(IF(A1:A3>0,A1:A3))`, selects branches elementwise in both ordinary and CSE formulas. Singleton axes broadcast; incompatible shapes return `#VALUE!` rather than padding with `#N/A`. Each needed branch evaluates once; unused branches are not evaluated. Scalar conditions retain reference selection and short-circuit behavior.

For CSE-containing source recalculation, family execution is disabled for the whole engine run to avoid declaration-insensitive family memoization. Other workbooks retain the caller's configuration. The Rust declaration API requires `family_execution = false`.

## Legacy implicit intersection

Before dynamic arrays, a range in a position that expects one value was reduced by implicit intersection: `=A1:A10*2` in row 5 means `A5*2`, `=VLOOKUP($B$4:$B$2636,…)` in row 900 looks up `$B900`, and `=IF(E2=21:21,…)` compares with row 21 in the formula's column. The file format still carries that meaning. Excel marks formulas that need dynamic-array evaluation with `t="array"` and XLDAPR `cm` metadata, and evaluates every other formula with legacy semantics (Excel 365 shows them with `@`; the stored text never contains it).

Recalculation applies legacy semantics only where the file shows that Excel calculated the formula: the cell is listed in the workbook's calculation chain (`xl/calcChain.xml`, matched by `sheetId`). For a shared formula, every cell of the family must be listed. Array formulas, CSE or dynamic, are never intersected. openpyxl, XlsxWriter and umya-spreadsheet do not write a calc chain, so workbooks they create, and Excel workbooks they re-save, keep dynamic-array evaluation as before. New multi-cell spills are still marked with `cm`. A missing, malformed or foreign calc chain is not evidence and is not a refusal.

Where a listed formula intersects follows Excel's token classes. The formula's result is a value. Operators take values. Each function parameter takes a value, a reference or an array, as Excel's built-in functions up to Excel 2013 declare. Array parameters (`SUMPRODUCT`, `MMULT`, `LOOKUP`'s vectors, `INDEX`'s array, ...) evaluate their argument as an array. Reference parameters (`SUM`, `COUNTIF`/`SUMIF` ranges, `VLOOKUP`'s table, `MATCH`'s array) take the range as is, although an operator inside them still intersects (`SUM(A1:A3*2)` without CSE). In a value position:

- a single-column range picks the formula's row, a single-row range its column, and a 2-D range needs both; otherwise the result is `#VALUE!`. Whole rows and columns, other sheets and defined names behave the same way;
- a reference returned by `IF`, `CHOOSE`, `IFERROR`, `IFNA`, `INDEX`, `OFFSET` or `INDIRECT` is intersected; an array result (`TRANSPOSE`, `MMULT`, `ROW(A1:A3)`, an array constant) gives its top-left value.

The intersection is applied in the transient ingestion view as explicit `@` operators; the stored formula text and the calc chain are unchanged, so a second recalculation is a no-op. Dependencies cover the whole range. Formulas that this rule does not classify keep dynamic-array evaluation: functions newer than Excel 2013 or user-defined, `LET`/`LAMBDA`, structured references and table names, 3-D references, and a multi-cell range passed where an Analysis ToolPak function or `N`/`T`/`CELL` expects one cell by reference. A formula without any multi-cell value position is not touched. An explicit `@` that Excel stores as `_xlfn.SINGLE(...)` is not supported and evaluates to `#NAME?`.

An Excel workbook re-saved by a tool that drops the calc chain loses this evidence, and its legacy formulas are then evaluated with dynamic-array semantics, which Excel will not share when it reopens the file. A tool that keeps the calc chain and rewrites a listed formula in place gets legacy semantics for it, which matches what Excel does with that file. These are explicit policies; the classification has been checked against Excel-computed caches in real workbooks but not by running Excel.

## Excel tables

Excel ListObjects are admitted after relationship, content-type, bounded geometry, ordered column/header, collision and formula-coverage validation. Table XML, relationships and content types stay byte-for-byte unchanged, and table geometry never changes. Calculated columns and totals require worksheet `<f>` formulas in every managed cell: write the formula into each row when extending a table. Cell formulas, including calculated-column exceptions, remain the authority; table-level formulas are never evaluated or rewritten.

Stored structured references are lowered only in the transient ingestion view, before dependency analysis: columns and column spans, `#Data`, `#All`, `#Headers`, `#Totals`, `#This Row`, `[@Qty]`, combinations selecting a contiguous rectangle, apostrophe-escaped names and bare table names (`SUM(Table1)`, `VLOOKUP(x,Table1,2,FALSE)`), which mean the data body. Output `<f>` text is retained exactly. Whole-table selectors keep absolute bounds. Same-sheet qualifiers are omitted; this-row rows stay relative and their columns are relative only for single-cell or one-column shared placements, otherwise absolute. A shared master uses these exact this-row references; its declared extent must stay inside the table's data body. Unknown tables/columns, this-row references outside the data body (header or totals row), empty or disjoint selections, sheet-qualified table names, bare table names in LET/LAMBDA formulas, and defined-name formulas containing structured references or bare table names are refused. In a table-bearing workbook every `INDIRECT` argument must be literal text (or a concatenation of literals) free of `[` and of every table name; cell-sourced or computed `INDIRECT` text is refused there. Workbooks without tables keep ordinary `INDIRECT`. A cross-sheet table reference in a multi-cell shared formula is refused when the injected sheet qualifier would be rewritten by shared-formula expansion (see below). Without a `<dimension>`, a table may extend past the last stored cell within the sheet limits. An empty data-body table itself is valid.

Shared-formula followers are expanded from the master text by Calamine, which offsets every `[A-Za-z0-9._\$:]` run that parses as an A1 cell or range, outside `"` strings, without understanding `'` quoting. A multi-cell shared formula is therefore refused, with or without tables, when it contains a sheet qualifier (quoted or not) containing `"` or one that a one-row or one-column shift would rewrite, for example `'Q1'!`, `'FY2024'!` or `'Q1 2024'!`. Names such as `Sheet1`, `'Sheet 1'`, `'Data'` and `'Summary'` are unaffected. Ordinary cells with such qualifiers are not expanded and remain supported.

Multi-cell dynamic results intersecting any table rectangle give `#SPILL!` before publication, even if table cells are blank. A 1x1 result inside a table remains scalar. Source-declared dynamic and legacy CSE footprints intersecting a table are refused. The general mutable workbook loaders and their native table-reference behavior are unchanged; this support is specific to immutable source recalculation.

## Volatile snapshots

Source recalculation evaluates a throwaway engine using `Engine::evaluate_all_for_snapshot`. Volatile values and their dependents remain Current for result projection; iterative-SCC redirty and every other stale-result check remain in force. The engine samples its clock once per evaluation request and uses the configured RNG policy/seed. `TODAY`, `NOW`, `RAND`, `OFFSET` and `INDIRECT` can therefore publish a consistent single-request result instead of being refused for next-cycle volatile dirtiness.

Volatile formulas are recomputed on every run. `RAND`/`RANDBETWEEN` depend only on the seed (`EvalConfig::workbook_seed`) and cell position, so they repeat run to run. `TODAY`/`NOW` follow the clock: a fixed `DeterministicMode::Enabled` timestamp (CLI `--now`) makes reruns byte-identical, while the system clock is sampled once per run, to the whole second, and `XlsxRecalculateResult::clock_now_utc` reports that instant. Without a fixed timestamp, CLI `--check` reports stale whenever the new sample changes a cache. An invalid deterministic mode (a `Local` timezone) is an error rather than a silent fallback to the system clock. Source row visibility is not hydrated: `SUBTOTAL`/`AGGREGATE` ranges intersecting a stored hidden row or active-filter row are refused, as are dynamic reducer ranges whose intersection cannot be proved. Rows count as hidden when they carry `hidden="1"` or a zero height (`ht="0"`), and every outline-grouped row counts as hidden when the sheet has a collapsed row; on a sheet whose `sheetFormatPr` declares `zeroHeight="1"` every reducer is refused. Range, intersection and union operators in a reducer argument are proved over their whole bounding row span and must join static references on one sheet: function operands (`INDEX(...):A5`), names and LET/LAMBDA-bound parameters are refused. Static reducer ranges that avoid hidden rows remain supported.

## Strict eligibility

This is not a fallback for every XLSX package. It rejects unsupported inputs/results instead of silently producing incomplete caches:

- Data-table formulas, rich value metadata (`vm`, `xl/richData/`), metadata other than XLDAPR (value/MDX metadata, other types or extension URIs), dangling or malformed `cm` chains, external workbook links and package signatures.
- Non-default spill conflict or bounds policies, when array anchors or spills are involved (including fixed-extent CSE).
- A spill over a merged range, or from a member of a source shared-formula family. A spill that exceeds the cell or width limits is refused before any member is materialized.
- Unsupported table metadata, connection/query-backed tables, missing managed worksheet formulas, mismatched headers, table/name/merge/array collisions, unlowerable structured-reference contexts and non-literal `INDIRECT` text in table-bearing workbooks.
- Multi-cell shared formulas whose sheet qualifiers shared-formula expansion would rewrite (cell-like names such as `'Q1'` or names containing `"`).
- Ambiguous namespaces/relationships, noncanonical internal part targets, duplicate or non-increasing rows/cells, invalid shared families, unsupported XML encodings/names, DTDs and CDATA in parsed parts.
- Literal scalar representations that differ from Calamine's raw ASCII parsing assumptions; unsupported literal error tokens are rejected before ingestion. Ordinary XML escapes remain supported for formula/text content.
- Missing/noncurrent results, non-finite numbers, pending values and unrepresentable arrays.
- XML-invalid output text controls and literal `_xHHHH_`-looking strings. Their cross-reader escaping semantics are not silently guessed.

Computed representable Excel errors, including scalar `#SPILL!` and `#CALC!`, produce `ErrorsFound` summaries and typed error caches. Internal engine errors without an approved Excel cache representation (`#N/IMPL!`, `#CIRC!`, `#ERROR!`) are unsupported results, not invented Excel tokens. Unsupported or noncurrent outcomes can only be identified after evaluation; they still publish no package. No evaluation-result error is silently mapped to another error kind.

## Refusal messages

A refusal is `IoError::Unsupported { feature, context }`. The CLI reports it as exit 2 with `"status": "refused"` and `"refusal": {"feature": ..., "context": ...}`; Python raises `RuntimeError("Unsupported feature: <feature> in <context>")`. `feature` names what was declined and `context` says where (a sheet and cell, a table, a package part or `XLSX package`). Both are diagnostics for people, not a stable enumeration: branch on the exit code or status, not on the text. A refusal means the workbook is outside the supported subset, so retrying the same file gives the same answer; change the workbook or use another tool, and never report its caches as recalculated.

Common refusals:

| `feature` (`context`) | Meaning |
| --- | --- |
| `data-table formula` (`worksheet`) | A What-If data table (`t="dataTable"`). Data tables are never evaluated. |
| `external links or rich value data` (`XLSX package`) | The package has external workbook links or rich values. External references are not resolved. |
| `package digital signature` (`XLSX package`) | Rewriting the package would invalidate its signature. |
| `SUBTOTAL/AGGREGATE references a stored hidden row; source row visibility is not hydrated` (`Sheet1!C1 range Sheet1!1:5`) | The reducer's range includes a hidden, zero-height, collapsed-outline or filtered row, so the result would depend on visibility the recalc does not model. |
| `cannot prove ... against stored hidden rows` (cell) | A dynamic, range-operator or LET/LAMBDA-bound reducer range whose hidden-row intersection cannot be checked statically. |
| `shared formula sheet qualifier would be rewritten by shared-formula expansion` (`C2 qualifier 'Q1'!`) | A copied (shared) formula refers to a sheet named like a cell reference. Write ordinary per-cell formulas instead. |
| `cross-sheet table reference in a shared formula would be rewritten by shared-formula expansion` (table, sheet, reference) | The same problem, through a structured reference to a table on such a sheet. |
| `INDIRECT text in a table-bearing workbook must be literal and free of table names/structured references` (cell) | `INDIRECT` in a workbook with tables reads its text from a cell, computes it, or names a table. |
| `table-managed formula is missing a worksheet <f>; ...` (table, column) | A calculated column or totals cell lacks a formula in some row. Write the formula into every row, then recalc. |
| `unknown table in structured reference`, `unsupported structured-reference spelling/context` (reference) | A structured reference names an unknown table or column, or uses `[#This Row]` outside the data body. |
| `defined-name formula refers to a table name`, `defined-name formula contains structured references` (name) | Defined names that point at tables are not lowered. |
| `connection-backed table` (table part) | A table backed by a query or data connection. |
| `unsupported or cyclic calculation name` (name) | A defined name outside the supported subset, or one that depends on itself, even if unused. |
| `unparseable formula` (`Sheet1!D1: <parser message>`) | A stored formula the formula parser cannot read. Fix or rewrite the formula. |
| `foreign workbook metadata lookalike`, `foreign worksheet lookalike`, `unsupported table attribute ...` (element or attribute) | Extension markup that reuses a SpreadsheetML name outside the [admitted positions](#excel-extension-markup). |
| `unsupported ZIP extra field 0x....`, `ZIP64 member`, `ZIP entry comment` (part) | A ZIP container feature outside the [admitted subset](#zip-containers). Re-saving the workbook in Excel, LibreOffice or openpyxl writes an admitted container. |
| `engine-specific error has no approved XLSX cache encoding` (`#CIRC!`, `#N/IMPL!`, ...) | A circular reference, a function the engine does not implement, or another result that has no Excel cache representation. |
| `formula result is not current: ...` (sheet) | A formula was still not current after evaluation. Stale values are never published. |
| `input byte limit`, `formula cell count limit`, `worksheet width limit`, ... (`XLSX package`, part) | The input exceeds a resource bound (see below). |
| `symlink destination` (`atomic XLSX output`) | The destination path is a symbolic link. Write to a regular path. |

Other features name the malformed, ambiguous or unsupported package structure that was found (ZIP layout, XML encoding, relationships, content types, metadata records). They are refused rather than guessed at.

## Bounds and cancellation

Default limits are 64 MiB input/output, 10,000 entries, 256 MiB actual expanded bytes, 128 MiB per worksheet/metadata part, 100,000 formulas, XML depth 128, 256 columns, and 8,000,000 serialized cells/aggregate zero-origin logical cells. The conservative width limit bounds Calamine's per-column ingestion builders, including wide sheets with few rows. Limits are configurable in Rust; they are not a promise of an exact process RSS ceiling. Evaluation policies/budgets remain available through `EvalConfig`.

Cancellation is cooperative. Preflight, cancellable Calamine reads/row/replay boundaries, evaluation and output construction check the token. A parser/engine operation already in progress runs until its next checkpoint. `CalamineAdapter::open_bytes_cancellable` also exposes cancellable parsing/streaming independently of this feature.

The file wrapper takes a bounded input snapshot, computes privately, writes a same-directory temporary, syncs it and atomically replaces the destination. It preserves existing destination permissions and rejects symlink destinations. Errors and cancellation observed before the commit point leave the destination unchanged; there is no cancellation error reported after publication. This is not source compare-and-swap or a guarantee of directory-entry crash durability. Higher-level session/CAS authority remains the caller's responsibility.

Native CLI defaults explicitly enable `system-clock`; native Python also enables it. WASM/npm's `wasm-js` profile uses the JavaScript Date clock. Portable builds without `system-clock`, including the Pyodide wheel, refuse `TODAY`/`NOW` in cells or defined names rather than publish the epoch fallback. A fixed instant admits them: `EvalConfig::deterministic_mode = Enabled` in Rust, `deterministic_timestamp_utc` in Python (including Pyodide), `deterministicTimestampUtc` in WASM and `--now` in the CLI.
