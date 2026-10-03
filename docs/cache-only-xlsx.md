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

- Ordinary and shared formulas over values, other formulas and other sheets, using the engine's built-in function library.
- Supported defined names: constants, absolute cell/range names and grounded formula names (see [Ownership and writeback](#ownership-and-writeback)).
- Dynamic arrays: new multi-cell spills from ordinary formulas, and existing dynamic-array anchors that grow, shrink, collapse or become blocked. `A1#` and `_xlfn.ANCHORARRAY(A1)` read the current spill. A spill blocked by existing content publishes `#SPILL!` as a formula result.
- Legacy fixed-extent (CSE) array formulas, including elementwise `IF` such as `SUM(IF(A1:A3>0,A1:A3))`.
- Excel tables: structured references, bare table names and calculated columns whose every row carries a worksheet formula.
- Volatile functions (`TODAY`, `NOW`, `RAND`, `OFFSET`, `INDIRECT`, `SUBTOTAL`, `AGGREGATE`), sampled once per run.
- Formula errors such as `#DIV/0!`, `#NAME?` or `#SPILL!`. These are calculated results, reported as error cells; they are not refusals.

Refused as a whole, with nothing written (CLI exit 2):

- What-If data tables, external workbook links, rich values, metadata other than dynamic-array metadata, and digitally signed packages.
- `SUBTOTAL`/`AGGREGATE` ranges over hidden or filtered rows, or whose hidden-row intersection cannot be proved.
- Multi-cell shared formulas whose sheet qualifiers look like cell references (`'Q1'!`, `'FY2024'!`).
- Table features outside the validated subset: connection-backed tables, table-managed formulas missing from some row, unknown tables or columns, `[#This Row]` outside the data body, computed `INDIRECT` text in table-bearing workbooks, and defined names that refer to tables.
- Unsupported or cyclic defined names, and results that cannot be cached faithfully: circular references (`#CIRC!`), functions the engine recognizes but does not implement (`#N/IMPL!`), non-finite numbers and results that are not current after evaluation.
- Malformed or ambiguous packages, and inputs over the resource bounds.

## Ownership and writeback

The source package is authoritative. A reconstructible evaluator consumes its values, ordinary/shared formulas and supported defined names. No rich document graph or independent evaluator is introduced.

Calamine calculation import supports numeric, Boolean, quoted text and error constants, absolute cell/range names and grounded formula names. Sheet-local definitions retain their scope and shadow workbook definitions. Grounded references must be absolute and identify an existing sheet (an unqualified absolute reference is allowed only for a sheet-local definition). Formula names support arithmetic, name dependencies and the explicit pure-function subset `SUM`, `AVERAGE`, `MIN`, `MAX`, `COUNT`, `COUNTA`, `ABS`, `ROUND`, `ROUNDUP`, `ROUNDDOWN`, `IF`, `AND`, `OR`, `NOT`. Engine name registration owns dependency binding, including formulas loaded before their names.

Strict recalculation conservatively refuses unsupported non-built-in definitions even if unused, and refuses cyclic names before evaluation. Relative, multi-area, external, structured/3D, array-valued and context-dependent/reference-producing definitions are not admitted. Unsupported `_xlnm.*` document metadata (including print/filter definitions) is preserved without calculation import only when no calculation/name expression references it; a reference causes explicit refusal. Supported built-in definitions may be calculated normally. Original error constants remain errors, not metadata-loss signals. All workbook name XML is preserved byte-for-byte. The public range/literal name DTO and JSON schema are unchanged; this does not repair the separate legacy Umya import path.

Preflight uses namespace-aware XML events and source offsets. Changed worksheet XML is assembled once from non-overlapping edits to formula cache types/values. Formula XML, styles, drawings and other untouched content retain their original bytes. Existing dates are evaluated in the source workbook's epoch; serial egress avoids lossy native-date conversion, including Excel-1900 serial 60.

The admitted ZIP32 package is edited surgically. ZIP7 supplies compression/CRC generation for changed and added payloads. Original local and central metadata is preserved; only payloads, affected CRC/size fields, relocated local-header offsets and the directory offset change. When a dynamic-array metadata part must be added, it is written as one deflated ZIP32 local record before the original central directory with a matching central record after the existing ones, and the end record's entry counts, directory size and offset are patched. Duplicate names, entry-count overflow, ZIP64 sizes/offsets and the output/expanded-byte limits are refused, and the resulting package is re-audited before it is returned. Untouched compressed payloads and the archive comment are preserved. Entry extras/comments, descriptors, ZIP64 and split/prefixed containers are outside the admitted subset. A true cache no-op returns the entire original byte sequence.

Calamine's cached-value decoder is not authority for formula results. If an old formula cache uses a representation it cannot decode faithfully, a bounded transient ingestion view clears that cache only. The original package is still used for comparison/writeback, including exact no-op output. Literal dependency values are never cleared this way.

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

## Excel tables

Excel ListObjects are admitted after relationship, content-type, bounded geometry, ordered column/header, collision and formula-coverage validation. Table XML, relationships and content types stay byte-for-byte unchanged, and table geometry never changes. Calculated columns and totals require worksheet `<f>` formulas in every managed cell: write the formula into each row when extending a table. Cell formulas, including calculated-column exceptions, remain the authority; table-level formulas are never evaluated or rewritten.

Stored structured references are lowered only in the transient ingestion view, before dependency analysis: columns and column spans, `#Data`, `#All`, `#Headers`, `#Totals`, `#This Row`, `[@Qty]`, combinations selecting a contiguous rectangle, apostrophe-escaped names and bare table names (`SUM(Table1)`, `VLOOKUP(x,Table1,2,FALSE)`), which mean the data body. Output `<f>` text is retained exactly. Whole-table selectors keep absolute bounds. Same-sheet qualifiers are omitted; this-row rows stay relative and their columns are relative only for single-cell or one-column shared placements, otherwise absolute. A shared master uses these exact this-row references; its declared extent must stay inside the table's data body. Unknown tables/columns, this-row references outside the data body (header or totals row), empty or disjoint selections, sheet-qualified table names, bare table names in LET/LAMBDA formulas, and defined-name formulas containing structured references or bare table names are refused. In a table-bearing workbook every `INDIRECT` argument must be literal text (or a concatenation of literals) free of `[` and of every table name; cell-sourced or computed `INDIRECT` text is refused there. Workbooks without tables keep ordinary `INDIRECT`. A cross-sheet table reference in a multi-cell shared formula is refused when the injected sheet qualifier would be rewritten by shared-formula expansion (see below). Without a `<dimension>`, a table may extend past the last stored cell within the sheet limits. An empty data-body table itself is valid.

Shared-formula followers are expanded from the master text by Calamine, which offsets every `[A-Za-z0-9._\$:]` run that parses as an A1 cell or range, outside `"` strings, without understanding `'` quoting. A multi-cell shared formula is therefore refused, with or without tables, when it contains a sheet qualifier (quoted or not) containing `"` or one that a one-row or one-column shift would rewrite, for example `'Q1'!`, `'FY2024'!` or `'Q1 2024'!`. Names such as `Sheet1`, `'Sheet 1'`, `'Data'` and `'Summary'` are unaffected. Ordinary cells with such qualifiers are not expanded and remain supported.

Multi-cell dynamic results intersecting any table rectangle give `#SPILL!` before publication, even if table cells are blank. A 1x1 result inside a table remains scalar. Source-declared dynamic and legacy CSE footprints intersecting a table are refused. The general mutable workbook loaders and their native table-reference behavior are unchanged; this support is specific to immutable source recalculation.

## Volatile snapshots

Source recalculation evaluates a throwaway engine using `Engine::evaluate_all_for_snapshot`. Volatile values and their dependents remain Current for result projection; iterative-SCC redirty and every other stale-result check remain in force. The engine samples its clock once per evaluation request and uses the configured RNG policy/seed. `TODAY`, `NOW`, `RAND`, `OFFSET`, `INDIRECT`, `SUBTOTAL` and `AGGREGATE` can therefore publish a consistent single-request result instead of being refused for next-cycle volatile dirtiness.

Volatile formulas are recomputed on every run. Byte-identical reruns are guaranteed only when the newly computed values also match; `NOW` and RNG-policy changes may alter caches, and CLI `--check` reports stale whenever the new sample differs. Source row visibility is not hydrated: `SUBTOTAL`/`AGGREGATE` ranges intersecting a stored hidden row or active-filter row are refused, as are dynamic reducer ranges whose intersection cannot be proved. Rows count as hidden when they carry `hidden="1"` or a zero height (`ht="0"`), and every outline-grouped row counts as hidden when the sheet has a collapsed row; on a sheet whose `sheetFormatPr` declares `zeroHeight="1"` every reducer is refused. Range, intersection and union operators in a reducer argument are proved over their whole bounding row span and must join static references on one sheet: function operands (`INDEX(...):A5`), names and LET/LAMBDA-bound parameters are refused. Static reducer ranges that avoid hidden rows remain supported.

## Strict eligibility

This is not a fallback for every XLSX package. It rejects unsupported inputs/results instead of silently producing incomplete caches:

- Data-table formulas, rich value metadata (`vm`, `xl/richData/`), metadata other than XLDAPR (value/MDX metadata, other types or extension URIs), dangling or malformed `cm` chains, external workbook links and package signatures.
- Non-default spill conflict or bounds policies, when array anchors or spills are involved (including fixed-extent CSE).
- A spill over a merged range, over an unowned source value or formula, or from a member of a source shared-formula family. A spill that exceeds the cell or width limits is refused before any member is materialized.
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
| `engine-specific error has no approved XLSX cache encoding` (`#CIRC!`, `#N/IMPL!`, ...) | A circular reference, a function the engine does not implement, or another result that has no Excel cache representation. |
| `formula result is not current: ...` (sheet) | A formula was still not current after evaluation. Stale values are never published. |
| `input byte limit`, `formula cell count limit`, `worksheet width limit`, ... (`XLSX package`, part) | The input exceeds a resource bound (see below). |
| `symlink destination` (`atomic XLSX output`) | The destination path is a symbolic link. Write to a regular path. |

Other features name the malformed, ambiguous or unsupported package structure that was found (ZIP layout, XML encoding, relationships, content types, metadata records). They are refused rather than guessed at.

## Bounds and cancellation

Default limits are 64 MiB input/output, 10,000 entries, 256 MiB actual expanded bytes, 128 MiB per worksheet/metadata part, 100,000 formulas, XML depth 128, 256 columns, and 8,000,000 serialized cells/aggregate zero-origin logical cells. The conservative width limit bounds Calamine's per-column ingestion builders, including wide sheets with few rows. Limits are configurable in Rust; they are not a promise of an exact process RSS ceiling. Evaluation policies/budgets remain available through `EvalConfig`.

Cancellation is cooperative. Preflight, cancellable Calamine reads/row/replay boundaries, evaluation and output construction check the token. A parser/engine operation already in progress runs until its next checkpoint. `CalamineAdapter::open_bytes_cancellable` also exposes cancellable parsing/streaming independently of this feature.

The file wrapper takes a bounded input snapshot, computes privately, writes a same-directory temporary, syncs it and atomically replaces the destination. It preserves existing destination permissions and rejects symlink destinations. Errors and cancellation observed before the commit point leave the destination unchanged; there is no cancellation error reported after publication. This is not source compare-and-swap or a guarantee of directory-entry crash durability. Higher-level session/CAS authority remains the caller's responsibility.

Native CLI defaults explicitly enable `system-clock`; native Python also enables it. WASM/npm's `wasm-js` profile uses the JavaScript Date clock. Portable/Pyodide source builds without `system-clock` refuse `TODAY`/`NOW` in cells or defined names rather than publish the epoch fallback. Rust callers can supply `EvalConfig::deterministic_mode = Enabled` with a fixed instant. The current Python/WASM source facade does not expose a fixed-timestamp option.
