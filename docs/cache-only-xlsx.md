# Cache-only XLSX recalculation

The default-enabled `xlsx-recalc` feature exposes `recalculate_xlsx_bytes` and native `recalculate_xlsx_file`. The minimal feature graph uses Calamine, not Umya. Existing rich workbook APIs and legacy `recalculate_file` are unchanged. Both `formualizer-workbook` and the `formualizer` facade enable this feature by default; use `default-features = false` to omit XLSX dependencies from minimal builds. Default availability does not automatically route existing recalculation calls through this strict cache-only path.

```rust,ignore
use formualizer_workbook::{recalculate_xlsx_bytes, XlsxRecalculateOptions};
let result = recalculate_xlsx_bytes(&input, XlsxRecalculateOptions::default())?;
// result.bytes, result.summary, result.formula_cells,
// result.cache_cells_changed, result.worksheet_parts_changed
```

A native example requires explicit input and output paths:

```sh
cargo run -p formualizer-workbook --release --no-default-features \
  --features xlsx-recalc --example cache_recalculate -- input.xlsx output.xlsx
```

## Ownership and writeback

The source package is authoritative. A reconstructible evaluator consumes its values, ordinary/shared formulas and supported defined names. No rich document graph or independent evaluator is introduced.

Preflight uses namespace-aware XML events and source offsets. Changed worksheet XML is assembled once from non-overlapping edits to formula cache types/values. Formula XML, styles, drawings and other untouched content retain their original bytes. Existing dates are evaluated in the source workbook's epoch; serial egress avoids lossy native-date conversion, including Excel-1900 serial 60.

The admitted ZIP32 package is edited surgically. ZIP7 supplies compression/CRC generation for changed worksheet payloads. Original local and central metadata is preserved; only payloads, affected CRC/size fields, relocated local-header offsets and the directory offset change. Untouched compressed payloads and the archive comment are preserved. Entry extras/comments, descriptors, ZIP64 and split/prefixed containers are outside the admitted subset. A true cache no-op returns the entire original byte sequence.

Calamine's cached-value decoder is not authority for formula results. If an old formula cache uses a representation it cannot decode faithfully, a bounded transient ingestion view clears that cache only. The original package is still used for comparison/writeback, including exact no-op output. Literal dependency values are never cleared this way.

## Strict eligibility

This is not a fallback for every XLSX package. It rejects unsupported inputs/results instead of silently producing incomplete caches:

- Array/data-table formula metadata, rich/dynamic cell metadata, external workbook links and package signatures.
- Tables: the existing Calamine adapter does not populate the evaluator's table registry. Preserving table XML while evaluating structured references against an empty registry would be incorrect. Table-bearing sheets are therefore explicitly unsupported in this first path.
- Ambiguous namespaces/relationships, noncanonical internal part targets, duplicate or non-increasing rows/cells, invalid shared families, unsupported XML encodings/names, DTDs and CDATA in parsed parts.
- Literal scalar representations that differ from Calamine's raw ASCII parsing assumptions; unsupported literal error tokens are rejected before ingestion. Ordinary XML escapes remain supported for formula/text content.
- Successful multi-cell spills, missing/noncurrent results, non-finite numbers, pending values and unrepresentable arrays. A one-cell dynamic result needs no geometry rewrite and is supported.
- XML-invalid output text controls and literal `_xHHHH_`-looking strings. Their cross-reader escaping semantics are not silently guessed.

Computed representable Excel errors, including scalar `#SPILL!` and `#CALC!`, produce `ErrorsFound` summaries and typed error caches. Internal engine errors without an approved Excel cache representation (`#N/IMPL!`, `#CIRC!`, `#ERROR!`) are unsupported results, not invented Excel tokens. Unsupported or noncurrent outcomes can only be identified after evaluation; they still publish no package. No evaluation-result error is silently mapped to another error kind.

## Bounds and cancellation

Default limits are 64 MiB input/output, 10,000 entries, 256 MiB actual expanded bytes, 128 MiB per worksheet/metadata part, 100,000 formulas, XML depth 128, 256 columns, and 8,000,000 serialized cells/aggregate zero-origin logical cells. The conservative width limit bounds Calamine's per-column ingestion builders, including wide sheets with few rows. Limits are configurable in Rust; they are not a promise of an exact process RSS ceiling. Evaluation policies/budgets remain available through `EvalConfig`.

Cancellation is cooperative. Preflight, cancellable Calamine reads/row/replay boundaries, evaluation and output construction check the token. A parser/engine operation already in progress runs until its next checkpoint. `CalamineAdapter::open_bytes_cancellable` also exposes cancellable parsing/streaming independently of this feature.

The file wrapper takes a bounded input snapshot, computes privately, writes a same-directory temporary, syncs it and atomically replaces the destination. It preserves existing destination permissions and rejects symlink destinations. Errors and cancellation observed before the commit point leave the destination unchanged; there is no cancellation error reported after publication. This is not source compare-and-swap or a guarantee of directory-entry crash durability. Higher-level session/CAS authority remains the caller's responsibility.
