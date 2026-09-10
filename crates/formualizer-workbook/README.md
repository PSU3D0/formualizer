![Formualizer banner](https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-banner.png)

# formualizer-workbook

![Arrow Powered](https://img.shields.io/badge/Arrow-Powered-0A66C2?logo=apache&logoColor=white)

**Ergonomic workbook API with sheets, evaluation, undo/redo, and file I/O.**

`formualizer-workbook` is the recommended high-level interface for Formualizer. It wraps the calculation engine with workbook-friendly APIs for managing sheets, editing cells, evaluating formulas, tracking changes, and importing/exporting files.

## When to use this crate

Use `formualizer-workbook` for **most integrations**:
- Set cell values and formulas, evaluate cells and ranges
- Load and save XLSX, CSV, and JSON workbooks
- Undo/redo with automatic action grouping
- Batch operations with transactional semantics
- This is the API that the Python and WASM bindings expose

Use [`formualizer-eval`](https://crates.io/crates/formualizer-eval) instead if you need direct engine access with custom resolvers.

## Cache-only XLSX recalculation

The default-enabled `xlsx-recalc` feature provides `recalculate_xlsx_bytes`; minimal builds can omit it with `default-features = false`. It uses Calamine and the existing evaluator, then surgically patches formula caches and the corresponding ZIP32 metadata without importing an Umya document. Untouched XML, metadata and compressed payloads are retained; a cache no-op returns the original bytes exactly. Native `recalculate_xlsx_file` adds bounded snapshots and same-directory atomic replacement, not source compare-and-swap.

This is a strict opt-in subset: tables, array/data-table metadata, multi-cell spills, unsupported ZIP/XML representations and nonrepresentable results fail without publishing partial output. Source epochs, typed caches, cooperative cancellation and configurable bounds are covered by the shared core. See [cache-only XLSX](../../docs/cache-only-xlsx.md) for exact eligibility, defaults and error policy.

## Quick start

```rust
use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;

let mut wb = Workbook::new();
wb.add_sheet("Sheet1")?;

wb.set_value("Sheet1", 1, 1, LiteralValue::Number(100.0))?;
wb.set_value("Sheet1", 2, 1, LiteralValue::Number(200.0))?;
wb.set_formula("Sheet1", 1, 2, "=SUM(A1:A2)")?;

let result = wb.evaluate_cell("Sheet1", 1, 2)?;
assert_eq!(result, LiteralValue::Number(300.0));
```

## Workbook-local custom functions

You can register callbacks directly on a `Workbook`:

- `register_custom_function(name, options, handler)`
- `unregister_custom_function(name)`
- `list_custom_functions()`

Semantics:

- Names are case-insensitive and canonicalized to uppercase.
- Workbook-local custom functions resolve before global built-ins.
- Overriding a built-in is blocked unless `CustomFnOptions { allow_override_builtin: true, .. }` is set.
- Args are by value (`LiteralValue`); range args are materialized as `LiteralValue::Array`.
- Returning `LiteralValue::Array` spills like dynamic-array formulas.
- Handler errors propagate as `ExcelError` values.

Runnable example:

```bash
cargo run -p formualizer-workbook --example custom_function_registration
```

WASM plugin support (native Rust, workbook-local):

- Effect-free inspect APIs:
  - `inspect_wasm_module_bytes(...)`
  - `inspect_wasm_module_file(...)` *(native only)*
  - `inspect_wasm_modules_dir(...)` *(native only)*
- Explicit workbook-local attach APIs:
  - `attach_wasm_module_bytes(...)`
  - `attach_wasm_module_file(...)` *(native only)*
  - `attach_wasm_modules_dir(...)` *(native only)*
- Bind formula names explicitly:
  - `bind_wasm_function(name, options, spec)`

Runtime notes:

- With `wasm_plugins` only, default runtime remains pending and bind returns `ExcelErrorKind::NImpl`.
- With `wasm_runtime_wasmtime` on native targets, you can call `use_wasmtime_runtime()` and execute compatible exports.

Runnable plugin examples:

```bash
cargo run -p formualizer-workbook --features wasm_plugins --example wasm_plugin_inspect_catalog
cargo run -p formualizer-workbook --features wasm_runtime_wasmtime --example wasm_plugin_inspect_attach_bind
cargo run -p formualizer-workbook --features wasm_runtime_wasmtime --example wasm_plugin_attach_dir
```

## Features

- **Mutable workbook model** — add sheets, edit cells, and track staged formula changes without rebuilding the entire dependency graph.
- **400+ Excel functions** — all built-ins from `formualizer-eval` are available through the workbook surface.
- **Changelog + undo/redo** — opt into change logging with automatic action grouping. Single edits are individually undoable; batch operations group as one step.
- **I/O backends** — pluggable readers/writers behind feature flags:
  - `calamine` — XLSX/ODS reading. XLSX paths default to the safe `XlsxPathSource::SharedFile`; use `CalamineAdapter::open_path_with_source(..., XlsxPathSource::DirectMmap)` for an explicit native read-only mapping. Direct mmap never falls back and requires the underlying file/inode not be destructively modified or truncated for the adapter lifetime (violations can terminate a Unix process with `SIGBUS`). The legacy `mmap` feature no longer selects behavior.
  - `umya` — existing Umya 2 XLSX backend
  - `umya3` — opt-in Umya 3 backend sharing the same adapter algorithms. Includes paired cold import/export repair for border-colour loss and colour-selector hash collisions, retaining theme/indexed/RGB identity. See [Umya 3 import compatibility](../../docs/umya3-import.md).
  - `json` — structured JSON serialization
  - `csv` — CSV/TSV import/export
- **Batch transactions** — atomic multi-cell operations with rollback.
- **Evaluation planning** — inspect the dependency schedule before computing.

## Retaining a rich Umya document after ingestion

Both Umya adapters expose `into_document(self)`. After `EngineLoadStream::stream_into_engine` ingests an evaluator, consume the adapter to retain its existing Umya document for rich edits. This transfers ownership without cloning the cell graph or serializing/reimporting XLSX. It preserves the document's current lazy/deserialized state; ingestion materializes the sheets it reads. Source-only adapter metadata has already been consumed by ingestion and is not a separate persistent document authority.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
