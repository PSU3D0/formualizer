# Formualizer

An open‑source, embeddable spreadsheet engine. Formualizer parses, evaluates, and mutates Excel‑style workbooks at speed — with a modern Rust core, Arrow‑powered storage, deferred/demand evaluation, and first‑class Python and WASM bindings.

[![CI](https://github.com/psu3d0/formualizer/actions/workflows/ci.yml/badge.svg)](https://github.com/psu3d0/formualizer/actions/workflows/ci.yml)
![Coverage](https://raw.githubusercontent.com/psu3d0/formualizer/badges/coverage.svg)

## Overview

Formualizer is a full spreadsheet engine you can embed anywhere:

- Engine: evaluates Excel‑style formulas with hundreds of built‑ins, dependency tracking, cycle detection, and deterministic volatility policies
- Storage: Arrow columnar backing with spill overlays, parallel evaluation, and staged graph building for fast recalcs on large workbooks
- Dialects: tokenizer/parser handle Excel and OpenFormula syntax with the same AST surface used throughout the stack
- Tooling: streaming loaders, undoable change logs, and tracing hooks surface engine internals for profiling or telemetry
- Bindings: first‑class Python and WASM APIs mirror the Rust workbook so scripting and browser clients share behavior

Use what you need: parse only, run the engine against your own sheet model, or let the workbook crate manage ingestion, evaluation, and ergonomics.

## Core Crates

- `formualizer-parse`: tokenizer, parser, and pretty printer that turn raw formula text into a dialect-aware AST shared by every other crate. Best when you are linting formulas, performing static analysis, or building custom tooling that stops at the parse tree.
- `formualizer-eval`: Arrow-backed calculation engine with dependency graph, incremental recomputation, and extensible built-ins. Best for power users who already own workbook storage but want Formualizer's evaluator, planning, and telemetry primitives.
- `formualizer-workbook`: high-level workbook surface with sheets, batch editors, undo/redo, staged formulas, and optional IO backends. Best choice for most integrations—it provides a stable API that mirrors what the Python and WASM bindings expose.

### Choosing the right Rust layer

- `formualizer-parse` is for parsing only. Reach for it when you need tokens, ASTs, or pretty-printing without executing formulas.
- `formualizer-eval` fits when you want full evaluation control, custom resolvers, or to embed the engine in an existing data model. Expect to manage ingestion and cell storage yourself.
- `formualizer-workbook` is the recommended default. It wraps the engine with ergonomic sheet APIs, keeps staged formulas in sync, and stays API-compatible with downstream bindings.

## Bindings

### Python (bindings/python)

- Engine: evaluate formulas on demand (`Engine.from_path`, `Engine.from_workbook`, `evaluate_cell`, `evaluate_all`)
- Workbook: set values/formulas, batch operations, undo/redo; evaluation via `Engine.from_workbook(wb)`
- No begin/end required: single edits are individually undoable; batch methods auto‑group as one undo step

Install: `pip install formualizer` (see bindings/python/README.md)

### WASM (bindings/wasm)

- Workbook + Sheet facade for values/formulas and evaluation
- Changelog/undo/redo controls for power users; single edits are undoable without manual grouping
- Optional JSON loader when the `json` feature is enabled

Install: `npm i formualizer-wasm` (see bindings/wasm/README.md)

## Quick Start

### Rust (parse)

```rust
use formualizer_parse::tokenizer::Tokenizer;
use formualizer_parse::parser::Parser;

let t = Tokenizer::new("=A1+B2").unwrap();
let mut p = Parser::new(t.items, false);
let ast = p.parse().unwrap();
println!("{}", formualizer_parse::pretty::canonical_formula(&ast));
```

### Python (evaluate)

```python
import formualizer as fz

wb = fz.Workbook()
s = wb.sheet("Data")
s.set_value(1, 1, fz.LiteralValue.int(10))
s.set_value(1, 2, fz.LiteralValue.int(20))
s.set_formula(1, 3, "=A1+B1")

eng = fz.Engine.from_workbook(wb)
assert eng.evaluate_cell("Data", 1, 3).as_number() == 30.0
```

### WASM (browser)

```ts
import init, { Workbook } from 'formualizer'
await init()

const wb = new Workbook()
wb.addSheet('S')
wb.setValue('S', 1, 1, 5)
wb.setValue('S', 1, 2, 7)
wb.setFormula('S', 1, 3, '=A1+B1')
console.log(await wb.evaluateCell('S', 1, 3)) // 12
```

## CI & Release

- CI runs Rust tests + clippy + fmt, builds Python wheels via maturin, generates Python stubs, and runs Python tests.
- WASM workflow builds the package and runs wasm‑pack tests on Node.
- Publishing is set up for artifacts; configure PyPI/NPM tokens on tag release to publish.

See `.github/workflows/ci.yml` and `.github/workflows/wasm.yml`.

## Performance & Excel Parity

- Performance: The evaluator uses columnar Arrow storage, vectorized kernels, and a staged/deferred graph for large workbooks. Benchmarks (coming soon) will include common workloads (SUMIFS/XLOOKUP/array formulas) across realistic data sizes.
- Excel parity: Built‑ins aim for Excel compatibility. An OpenFormula and Excel conformance test suite is being prepared; we will publish parity results and gaps.

Planned: a public benchmark harness and conformance dashboards.

## License

MIT OR Apache‑2.0
