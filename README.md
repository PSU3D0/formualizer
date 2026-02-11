# Formualizer

**The spreadsheet engine that actually evaluates formulas.** Parse, evaluate, and mutate Excel workbooks from Rust, Python, or the browser.

A permissively-licensed, production-grade spreadsheet engine with 320+ Excel-compatible functions, Apache Arrow storage, incremental dependency tracking, undo/redo, and dynamic array support. One Rust core, three language targets, MIT/Apache-2.0.

[![CI](https://github.com/psu3d0/formualizer/actions/workflows/ci.yml/badge.svg)](https://github.com/psu3d0/formualizer/actions/workflows/ci.yml)
![Coverage](https://raw.githubusercontent.com/psu3d0/formualizer/badges/coverage.svg)
[![crates.io](https://img.shields.io/crates/v/formualizer.svg)](https://crates.io/crates/formualizer)
[![PyPI](https://img.shields.io/pypi/v/formualizer.svg)](https://pypi.org/project/formualizer/)
[![npm](https://img.shields.io/npm/v/formualizer.svg)](https://www.npmjs.com/package/formualizer)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

---

## Highlights

| | |
|---|---|
| **320+ Excel functions** | Math, text, lookup (XLOOKUP, VLOOKUP), date/time, statistics, financial, database, engineering |
| **Three language targets** | Rust, Python (PyO3), and WASM (browser + Node) with consistent APIs |
| **Arrow-powered storage** | Apache Arrow columnar backing with spill overlays for efficient large-workbook evaluation |
| **Dependency graph** | Incremental recalculation, cycle detection, topological scheduling, optional parallel evaluation |
| **Dynamic arrays** | FILTER, UNIQUE, SORT, SORTBY, XLOOKUP with automatic spill semantics |
| **Undo / redo** | Transactional changelog with action grouping, rollback, and replay |
| **File I/O** | Load and write XLSX (calamine, umya), CSV, JSON — all behind feature flags |
| **SheetPort** | Treat any spreadsheet as a typed API with YAML manifests, schema validation, and batch evaluation |
| **Deterministic mode** | Inject clock, timezone, and RNG seed for reproducible evaluation (built for AI agents) |

## Who is this for?

- **Fintech & insurance teams** replacing Excel VBA or server-side workbook evaluation with a fast, deterministic engine that doesn't require Excel installed.
- **AI / agent builders** who need programmatic spreadsheet manipulation with deterministic evaluation, auditable changelogs, and typed I/O via SheetPort.
- **SaaS products** embedding spreadsheet logic — pricing calculators, planning tools, configurators — without shipping a full spreadsheet UI.
- **Data engineers** extracting business logic trapped in spreadsheets into reproducible, testable pipelines.

## Quick start

### Rust

```rust
use formualizer_workbook::Workbook;
use formualizer_common::LiteralValue;

let mut wb = Workbook::new();
wb.add_sheet("Sheet1")?;

// Populate data
wb.set_value("Sheet1", 1, 1, LiteralValue::Number(1000.0))?;  // A1: principal
wb.set_value("Sheet1", 2, 1, LiteralValue::Number(0.05))?;     // A2: rate
wb.set_value("Sheet1", 3, 1, LiteralValue::Number(12.0))?;     // A3: periods

// Monthly payment formula
wb.set_formula("Sheet1", 1, 2, "=PMT(A2/12, A3, -A1)")?;
let payment = wb.evaluate_cell("Sheet1", 1, 2)?;
// => ~85.61
```

```toml
# Cargo.toml
[dependencies]
formualizer = "0.3"
```

### Python

```bash
pip install formualizer
```

```python
import formualizer as fz

wb = fz.Workbook()
s = wb.sheet("Forecast")

# Load actuals
s.set_values_batch(1, 1, [
    [fz.LiteralValue.text("Month"), fz.LiteralValue.text("Revenue"), fz.LiteralValue.text("Growth")],
    [fz.LiteralValue.text("Jan"),   fz.LiteralValue.number(50000.0), fz.LiteralValue.empty()],
    [fz.LiteralValue.text("Feb"),   fz.LiteralValue.number(53000.0), fz.LiteralValue.empty()],
    [fz.LiteralValue.text("Mar"),   fz.LiteralValue.number(58000.0), fz.LiteralValue.empty()],
])

# Add growth formulas
s.set_formula(3, 3, "=(B3-B2)/B2")  # C3: Feb growth
s.set_formula(4, 3, "=(B4-B3)/B3")  # C4: Mar growth

print(wb.evaluate_cell("Forecast", 3, 3))  # 0.06 (6%)
print(wb.evaluate_cell("Forecast", 4, 3))  # ~0.094 (9.4%)
```

### WASM (browser / Node)

```bash
npm install formualizer
```

```typescript
import init, { Workbook } from 'formualizer';
await init();

const wb = new Workbook();
wb.addSheet('Pricing');
wb.setValue('Pricing', 1, 1, 100);     // base price
wb.setValue('Pricing', 2, 1, 0.15);    // discount
wb.setFormula('Pricing', 1, 2, '=A1*(1-A2)');

console.log(await wb.evaluateCell('Pricing', 1, 2)); // 85
```

## How is this different?

| Library | Language | Parse | Evaluate | Write | Functions | Dep. graph | License |
|---------|----------|-------|----------|-------|-----------|------------|---------|
| **Formualizer** | Rust / Python / WASM | Yes | Yes | Yes | 320+ | Yes (incremental) | MIT / Apache-2.0 |
| HyperFormula | JavaScript | Yes | Yes | No | ~400 | Yes | **AGPL-3.0** (or commercial) |
| calamine | Rust | No | No | No | N/A | N/A | MIT / Apache-2.0 |
| openpyxl | Python | No | No | Yes | N/A | N/A | MIT |
| xlcalc | Python | Yes | Yes | No | ~50 | Partial | MIT |
| formulajs | JavaScript | No | Yes | No | ~100 | No | MIT |

- **HyperFormula** is the closest feature competitor, but its AGPL-3.0 license requires you to open-source your entire application or purchase a commercial license from Handsontable. Formualizer is permissively licensed with no strings attached.
- **calamine** is read-only — it extracts cached values from XLSX files but cannot evaluate formulas.
- **openpyxl** reads and writes XLSX but has no formula evaluation engine.
- **xlcalc** evaluates formulas but supports a fraction of Excel's function library and has limited dependency tracking.
- **Formualizer** is a complete, permissively-licensed engine: parse formulas, track dependencies, evaluate with 320+ functions, mutate workbooks, undo/redo — from Rust, Python, or the browser.

## Architecture

Formualizer is organized as a layered crate workspace. Pick the layer that fits your use case:

```
formualizer              <-- recommended: batteries-included re-export
  formualizer-workbook   <-- high-level workbook API, sheets, undo/redo, I/O
    formualizer-eval     <-- calculation engine, dependency graph, built-ins
      formualizer-parse  <-- tokenizer, parser, AST, pretty-printer
      formualizer-common <-- shared types (values, errors, references)
  formualizer-sheetport  <-- SheetPort runtime (spreadsheets as typed APIs)
```

| Crate | When to use it |
|-------|---------------|
| `formualizer` | Default choice — re-exports workbook, engine, and SheetPort with feature flags |
| `formualizer-workbook` | You want the full workbook experience: sheets, I/O, undo/redo, batch operations |
| `formualizer-eval` | You own your own data model and want just the calculation engine with custom resolvers |
| `formualizer-parse` | You only need formula parsing, tokenization, AST analysis, or pretty-printing |

## SheetPort: spreadsheets as typed APIs

SheetPort lets you treat any spreadsheet as a deterministic function with typed inputs and outputs, defined by a YAML manifest:

```python
from formualizer import SheetPortSession, Workbook

session = SheetPortSession.from_manifest_yaml(manifest_yaml, workbook)

# Write typed inputs — validated against schema
session.write_inputs({"loan_amount": 250000, "rate": 0.045, "term_months": 360})

# Evaluate and read typed outputs
result = session.evaluate_once(freeze_volatile=True)
print(result["monthly_payment"])  # deterministic, schema-validated
```

Use cases: financial model APIs, AI agent tool-use, configuration-driven business logic, batch scenario evaluation.

## Performance

The evaluation engine is built on Apache Arrow columnar storage with:
- Incremental dependency graph (only recalculates what changed)
- CSR (Compressed Sparse Row) edge format for memory-efficient graphs
- Optional parallel evaluation via Rayon
- Warm-up planning for large workbooks
- Spill overlays for dynamic array results

Formal benchmarks are in progress.

## Bindings

| Target | Install | Docs |
|--------|---------|------|
| Rust | `cargo add formualizer` | [docs.rs](https://docs.rs/formualizer) |
| Python | `pip install formualizer` | [bindings/python/README.md](bindings/python/README.md) |
| WASM | `npm install formualizer` | [bindings/wasm/README.md](bindings/wasm/README.md) |

Both Python and WASM bindings expose the same core API surface: tokenization, parsing, workbook operations, evaluation, undo/redo, and SheetPort.

## Roadmap

Roadmap and active development are tracked via GitHub Issues, milestones, and pull requests.

## Contributing

Contributions are welcome. If you're looking for something to work on, browse open issues or open a new issue to discuss a proposal.

```bash
# Build and test
cargo test --workspace
cd bindings/python && maturin develop && pytest
cd bindings/wasm && wasm-pack build --target bundler && wasm-pack test --node
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
