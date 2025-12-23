# formualizer-workbook

`formualizer-workbook` layers workbook/session ergonomics on top of the
Formualizer engine. It offers mutable sheet APIs, batch transactions, undo/redo
support, and optional IO backends for XLSX/ODS/json import/export.

## Features

- **Mutable workbook model** – add sheets, edit cells, and track staged formula
  changes without rebuilding the entire dependency graph.
- **Engine integration** – wraps `formualizer-eval` with workbook-friendly
  helpers for evaluating individual cells, ranges, or the whole model.
- **Changelog + undo** – opt into change logging and undo/redo stacks for UI or
  collaborative flows.
- **IO adapters** – pluggable backends (`calamine`, `umya-spreadsheet`, JSON)
  gated behind feature flags.

## Usage

```rust
# use formualizer_common::LiteralValue;
# use formualizer_workbook::{IoError, Workbook};
# fn main() -> Result<(), IoError> {
let mut wb = Workbook::new();
wb.add_sheet("Sheet1")?;
wb.set_value("Sheet1", 1, 1, LiteralValue::Number(2.0))?;
wb.set_value("Sheet1", 1, 2, LiteralValue::Number(3.0))?;
wb.set_formula("Sheet1", 1, 3, "=A1+A2")?;

let result = wb.evaluate_cell("Sheet1", 1, 3)?;
assert_eq!(result, LiteralValue::Number(5.0));
# Ok(())
# }
```

## License

Dual-licensed under MIT or Apache-2.0, at your option.
