# formualizer-eval

**Arrow-backed Excel formula engine with dependency graph and incremental recalculation.**

`formualizer-eval` is the calculation core of Formualizer. It takes ASTs from `formualizer-parse`, tracks dependencies between cells, and evaluates 320+ Excel-compatible functions with incremental recomputation and optional parallel execution.

## When to use this crate

Use `formualizer-eval` when you want **full control** over the evaluation engine:
- You have your own cell storage and want to plug in Formualizer's evaluator
- You need custom resolvers, function providers, or evaluation observers
- You're building a spreadsheet product with custom data models

For most integrations, [`formualizer-workbook`](https://crates.io/crates/formualizer-workbook) is easier — it wraps this engine with ergonomic workbook APIs and handles storage for you.

## Features

- **Apache Arrow storage** — columnar sheet backing with spill overlays for efficient large-workbook evaluation.
- **Dependency graph** — incremental graph with cycle detection, topological scheduling, and CSR (Compressed Sparse Row) edge format.
- **320+ built-in functions** — math, text, lookup (XLOOKUP, VLOOKUP, HLOOKUP), date/time, financial, statistics, database, engineering.
- **Dynamic arrays** — FILTER, UNIQUE, SORT, SORTBY with automatic spill semantics.
- **Parallel evaluation** — optional multi-threaded evaluation via Rayon with configurable thread pools.
- **Deterministic mode** — inject clock, timezone, and RNG seed for reproducible results.
- **Extensible** — `Resolver`, `RangeResolver`, and `FunctionProvider` traits for custom implementations.
- **Warm-up planning** — pre-compute evaluation strategy for large workbooks.

## Quick start

```rust
use formualizer_common::LiteralValue;
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;

let resolver = TestWorkbook::new()
    .with_cell_a1("Sheet1", "A1", LiteralValue::Number(2.0))
    .with_cell_a1("Sheet1", "A2", LiteralValue::Number(3.0));

let mut engine = Engine::new(resolver, EvalConfig::default());
// Insert formulas, build dependency graph, and evaluate
```

## License

Dual-licensed under MIT or Apache-2.0, at your option.
