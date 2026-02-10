# formualizer-workbook

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
