# formualizer-eval

`formualizer-eval` hosts the Formualizer spreadsheet calculation engine. It
turns ASTs produced by `formualizer-parse` into dependency graphs backed by
Arrow storage, executes built-in functions, and produces Excel-compatible
results with incremental recomputation.

## Features

- **Arrow-backed storage** – columnar sheet backing with spill overlays for fast
  analytical workloads.
- **Dependency graph engine** – incremental graph with cycle detection,
  parallel evaluation, and warm-up planning.
- **Extensible functions** – built-in registry plus traits for plugging in
  custom functions and resolvers.
- **Dialect aware** – respects the parser's Excel/OpenFormula dialect choices
  for reference interpretation.

## Usage

```rust,no_run
use formualizer_common::LiteralValue;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;

let resolver = TestWorkbook::new()
    .with_cell_a1("Sheet1", "A1", LiteralValue::Number(2.0))
    .with_cell_a1("Sheet1", "A2", LiteralValue::Number(3.0));

let mut engine = Engine::new(resolver, EvalConfig::default());
// Insert sheets/formulas using the engine graph editors, then trigger evaluation:
// engine.graph.ingest_formula("Sheet1", 1, 3, "=A1+A2");
// let result = engine.evaluate().unwrap();
```

## License

Dual-licensed under MIT or Apache-2.0, at your option.
