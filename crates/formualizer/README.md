![Formualizer banner](https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-banner.png)

# Formualizer

![Arrow Powered](https://img.shields.io/badge/Arrow-Powered-0A66C2?logo=apache&logoColor=white)

**Embeddable spreadsheet engine — parse, evaluate, and mutate Excel workbooks from Rust.**

`formualizer` is the batteries-included entry point for the Formualizer ecosystem. It re-exports the workbook, engine, parser, and SheetPort crates behind feature flags, so you can depend on a single crate and get everything you need.

## When to use this crate

This is the **recommended default** for most Rust integrations. It gives you:
- Workbook API with sheets, values, formulas, undo/redo, and I/O backends
- 320+ Excel-compatible built-in functions
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
| `eval` | Yes | Calculation engine and built-in functions |
| `workbook` | Yes | Workbook API with sheets, undo/redo |
| `sheetport` | Yes | SheetPort runtime (spreadsheets as typed APIs) |
| `parse` | Yes | Tokenizer, parser, pretty-printer |
| `common` | Yes | Shared types (values, errors, references) |
| `calamine` | No | XLSX/ODS reading via calamine |
| `umya` | No | XLSX reading/writing via umya-spreadsheet |
| `json` | No | JSON workbook serialization |
| `tracing` | No | Performance tracing hooks |

## License

Dual-licensed under MIT or Apache-2.0, at your option.
