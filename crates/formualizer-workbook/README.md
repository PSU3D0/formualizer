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
- **320+ Excel functions** — all built-ins from `formualizer-eval` are available through the workbook surface.
- **Changelog + undo/redo** — opt into change logging with automatic action grouping. Single edits are individually undoable; batch operations group as one step.
- **I/O backends** — pluggable readers/writers behind feature flags:
  - `calamine` — XLSX/ODS reading
  - `umya` — XLSX reading/writing with round-trip support
  - `json` — structured JSON serialization
  - `csv` — CSV/TSV import/export
- **Batch transactions** — atomic multi-cell operations with rollback.
- **Evaluation planning** — inspect the dependency schedule before computing.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
