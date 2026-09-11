![Formualizer banner](https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-banner.png)

# Formualizer

![Arrow Powered](https://img.shields.io/badge/Arrow-Powered-0A66C2?logo=apache&logoColor=white)

**Embeddable spreadsheet engine — parse, evaluate, and mutate Excel workbooks from Rust.**

`formualizer` is the batteries-included entry point for the Formualizer ecosystem. It re-exports the workbook, engine, parser, and SheetPort crates behind feature flags, so you can depend on a single crate and get everything you need.

## When to use this crate

This is the **recommended default** for most Rust integrations. It gives you:
- Workbook API with sheets, values, formulas, undo/redo, and I/O backends
- 400+ Excel-compatible built-in functions
- Formula parsing, tokenization, and pretty-printing
- SheetPort runtime for typed spreadsheet I/O

If you only need a subset, depend on the individual crates directly:
- [`formualizer-parse`](https://crates.io/crates/formualizer-parse) — parsing only
- [`formualizer-eval`](https://crates.io/crates/formualizer-eval) — calculation engine with custom resolvers
- [`formualizer-workbook`](https://crates.io/crates/formualizer-workbook) — workbook API without SheetPort

## Quick start

```rust
use formualizer_workbook::Workbook;
use formualizer_common::LiteralValue;

let mut wb = Workbook::new();
wb.add_sheet("Sheet1")?;

wb.set_value("Sheet1", 1, 1, LiteralValue::Number(1000.0))?;
wb.set_value("Sheet1", 2, 1, LiteralValue::Number(0.05))?;
wb.set_value("Sheet1", 3, 1, LiteralValue::Number(12.0))?;
wb.set_formula("Sheet1", 1, 2, "=PMT(A2/12, A3, -A1)")?;

let payment = wb.evaluate_cell("Sheet1", 1, 2)?;
```

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `portable-wasm` | Yes | Full stack preset: `eval`, `workbook`, `sheetport`, `parse`, `common`, with no ambient clock or JS runtime hooks |
| `system-clock` | Yes | Ambient wall-clock time for `NOW()`, `TODAY()` and friends; disable for wasmtime/non-JS wasm guests and inject a clock instead |
| `json` | Yes | JSON workbook serialization |
| `csv` | Yes | CSV workbook loading |
| `xlsx-recalc` | Yes | Explicitly invoked cache-only XLSX recalculation (`recalculate_xlsx_bytes` / file variants) |
| `eval` | via preset | Calculation engine and built-in functions |
| `workbook` | via preset | Workbook API with sheets, undo/redo |
| `sheetport` | via preset | SheetPort runtime (spreadsheets as typed APIs) |
| `parse` | via preset | Tokenizer, parser, pretty-printer |
| `common` | via preset | Shared types (values, errors, references) |
| `calamine` | No | XLSX/ODS reading via calamine, including runtime `XlsxPathSource` selection |
| `umya` | No | XLSX reading/writing via umya-spreadsheet 2 |
| `umya3` | No | Opt-in umya-spreadsheet 3 backend sharing the same adapter algorithms |
| `js-runtime` | No | Browser/wasm-bindgen runtime hooks (`web-time`, JS-backed entropy); only for browser targets |
| `wasm-js` | No | Preset: `portable-wasm` + `system-clock` + `js-runtime` |
| `tracing` / `tracing_chrome` | No | Performance tracing hooks and Chrome trace output |

## License

Dual-licensed under MIT or Apache-2.0, at your option.
