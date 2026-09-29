<h1 align="center">Formualizer</h1>

![Formualizer — The fastest open-source spreadsheet engine](https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-banner.png)

<p align="center"><b><big>The fastest open-source spreadsheet engine.</big></b></p>

<p align="center">A million formulas in 0.38 seconds, anywhere you run code: Rust, Python or JavaScript, on a server, in the browser or at the edge.<br/>Load Excel workbooks, change inputs, recalculate and read results, in-process. Built for AI agents.</p>

<p align="center">
  <a href="https://github.com/psu3d0/formualizer/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/psu3d0/formualizer/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://crates.io/crates/formualizer"><img alt="crates.io" src="https://img.shields.io/crates/v/formualizer.svg" /></a>
  <a href="https://pypi.org/project/formualizer/"><img alt="PyPI" src="https://img.shields.io/pypi/v/formualizer.svg" /></a>
  <a href="https://www.npmjs.com/package/formualizer"><img alt="npm" src="https://img.shields.io/npm/v/formualizer.svg" /></a>
  <img alt="Arrow Powered" src="https://img.shields.io/badge/Arrow-Powered-0A66C2?logo=apache&logoColor=white" />
  <img alt="Python Coverage" src="https://raw.githubusercontent.com/psu3d0/formualizer/badges/coverage.svg" />
  <img alt="Rust Core Coverage" src="https://raw.githubusercontent.com/psu3d0/formualizer/badges/rust-core-coverage.svg" />
  <a href="https://www.formualizer.dev/docs"><img alt="Documentation" src="https://img.shields.io/badge/docs-formualizer.dev-blue" /></a>
  <a href="#license"><img alt="License: MIT/Apache-2.0" src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg" /></a>
</p>

Calculating a spreadsheet from code usually means automating an office suite: LibreOffice over UNO, Excel over COM, or Excel Online through Microsoft Graph and a subscription. They are heavy to deploy, slow to start, hard to sandbox and awkward to hand to an agent. Formualizer is a library instead.

- **Runs anywhere.** One Rust core, shipped as a Rust crate, a Python package (including Pyodide) and a WASM module for browsers, Node and edge runtimes. No external process, no Windows, no license server.
- **Built for agents.** Deterministic evaluation (injectable clock, timezone and random seed), an auditable change log with undo, typed inputs and outputs through SheetPort, and [agent-spreadsheet](#building-an-ai-agent-use-agent-spreadsheet) for CLI and MCP tooling.
- **No compromise on speed.** Copied formulas compute as families in one pass over Arrow columns, lookups index their table once, and only what changed recalculates. The numbers are [below](#how-fast).
- **Excel-compatible.** 400+ functions, dynamic arrays, `LET` and `LAMBDA`, with edge cases checked against Excel.

## How fast?

Load an `.xlsx` and calculate every formula in it, from a cold start. The comparison is headless LibreOffice Calc 24.2 with threaded calculation, the usual open-source way to do this, on the same 24-core Linux machine, median of 3 runs:

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/benchmark-load-calculate-dark.png" />
  <img alt="Load and calculate time for nine workloads, comparing LibreOffice Calc 24.2 with Formualizer 0.10 on a logarithmic time axis. Exact values and workload descriptions follow." src="https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/benchmark-load-calculate-light.png" />
</picture>

**Across all 52 workbooks and workloads we measured, Formualizer takes about a quarter of the time and is faster in 48.** The [full report](benchmarks/0.10-vs-0.9.3.md#libreoffice-calc) has the method and the four exceptions. These shared-machine results are indicative, not guarantees for every workload or runtime.

<details>
<summary>Exact timings and workload descriptions</summary>

Speedups use unrounded measurements; displayed times are rounded.

| Workload | LibreOffice | Formualizer | |
|---|---:|---:|---:|
| Product lookups <sup>[1]</sup> | 53.3 s | **0.35 s** | **154× faster** |
| Sales report joins <sup>[2]</sup> | 19.7 s | **0.50 s** | **40× faster** |
| Enron cash-flow forecast <sup>[3]</sup> | 1.57 s | **0.07 s** | **23× faster** |
| Multi-criteria summary report <sup>[4]</sup> | 6.11 s | **0.28 s** | **22× faster** |
| Revenue rollup <sup>[5]</sup> | 4.46 s | **0.58 s** | **7.7× faster** |
| Service operations model <sup>[6]</sup> | 212 ms | **80 ms** | **2.6× faster** |
| Enron trading workbook <sup>[7]</sup> | 1.31 s | **0.50 s** | **2.6× faster** |
| 100,000-row running balance <sup>[8]</sup> | 525 ms | **291 ms** | **1.8× faster** |
| 100,000 copied formulas <sup>[9]</sup> | 550 ms | **347 ms** | **1.6× faster** |

1. 20,000 query rows, each with two `INDEX`/`MATCH` lookups of a product key in a 50,000-row table.
2. A 5,000-row report that finds each sale in a 50,000-row fact table on another sheet, then looks up its region and product in two dimension tables (`INDEX`/`MATCH` throughout). The fact table computes 50,000 revenue formulas.
3. A cash-flow forecast from the public Enron spreadsheet corpus: about 4,900 formulas across 18 sheets.
4. 1,000 `COUNTIFS` rows, each counting 100,000 transactions against five conditions.
5. 100,000 line items (price × quantity) rolled up by 1,000 `SUMIFS` over whole columns.
6. A generated operations workbook: work orders, priority lookup tables and dashboard rollups (15,000 formulas).
7. A trading-volume workbook from the Enron corpus: 65,000 formulas across 16 sheets.
8. A balance where each row adds to the one above (`=A1+1`, `=A2+1`, …), the hardest shape to parallelize.
9. One formula (`=A1*2`) filled down 100,000 rows, plus a `SUM` over the results.

</details>

### And against Formualizer 0.9.3

0.10 is a new engine under the same API:

| | 0.9.3 | 0.10 | |
|---|---:|---:|---:|
| 1M-formula financial model: load | 25.1 s | 4.2 s | 6× faster |
| … first calculation | 4.8 s | 0.38 s | 12× faster |
| … change one input and recalculate | 2.5 s | 52 ms | 47× faster |
| … memory after calculation | 897 MB | 45 MB | 20× less |
| Enron spreadsheets (25): load and calculate | | | 3.2× faster |
| … change an input and recalculate | | | 6.7× faster |
| … memory after calculation | | | 7.7× less |

Values are unchanged. Every step of the rewrite was checked cell by cell against the previous engine.

### Why it's fast

![Illustration of filling a formula down a column, evaluating a formula family, and recalculating dependents after an input edit](https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-calc.gif)

*Illustrative animation, not a real-time recording. Its timing figures refer to the financial-model benchmark above.*

- **Copied formulas are one unit.** Fill a formula down 100,000 rows and Formualizer stores one template and one dependency node, and evaluates the run in one pass over typed [Apache Arrow](https://arrow.apache.org/) columns. It does not track 100,000 separate formulas.
- **Lookups and conditional sums index their table once.** A column of `VLOOKUP`, `INDEX`/`MATCH`, `SUMIFS` or `COUNTIFS` over the same range builds one index for the whole run instead of scanning the table once per row.
- **Only what changed recalculates.** Dependencies are tracked by region, so editing one input touches only the cells that read it. Row and column inserts move whole runs at once.
- **Exact results.** The fast paths reproduce Excel's per-cell semantics bit for bit, including summation order, error precedence and number formats. The cell-by-cell engine stays in the codebase as a test oracle.

## Quick start

### Python

```bash
pip install formualizer
```

```python
import formualizer as fz

wb = fz.load_workbook("financial_model.xlsx")
wb.set_value("Assumptions", 3, 2, 0.07)   # change an input (B3)
wb.evaluate_all()                          # recalculates only what depends on it
print(wb.get_value("Summary", 5, 2))       # read an output (B5)
```

### Rust

```toml
[dependencies]
formualizer = { version = "0.10", features = ["calamine"] }
```

```rust
use formualizer::workbook::{CalamineAdapter, SpreadsheetReader};
use formualizer::{LiteralValue, LoadStrategy, Workbook, WorkbookConfig};

let reader = CalamineAdapter::open_path("financial_model.xlsx")?;
let mut wb = Workbook::from_reader(reader, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())?;
wb.set_value("Assumptions", 3, 2, LiteralValue::Number(0.07))?;
wb.evaluate_all()?;
println!("{:?}", wb.get_value("Summary", 5, 2));
```

### JavaScript / WASM (browser and Node)

```bash
npm install formualizer
```

```typescript
import init, { Workbook } from 'formualizer';
await init();

const wb = new Workbook();
wb.addSheet('Loans');
wb.setValue('Loans', 1, 1, 250000);  // principal
wb.setValue('Loans', 2, 1, 0.045);   // annual rate
wb.setValue('Loans', 3, 1, 360);     // months
wb.setFormula('Loans', 1, 2, '=PMT(A2/12, A3, -A1)');
console.log(wb.evaluateCell('Loans', 1, 2)); // ~1266.71
```

More in the [quickstarts](https://www.formualizer.dev/docs/quickstarts) for [Rust](https://www.formualizer.dev/docs/quickstarts/rust-quickstart), [Python](https://www.formualizer.dev/docs/quickstarts/python-quickstart), [Pyodide](https://www.formualizer.dev/docs/quickstarts/pyodide-quickstart) and [JS/WASM](https://www.formualizer.dev/docs/quickstarts/js-wasm-quickstart).

## What you get

| | |
|---|---|
| **400+ Excel functions** | Math, text, lookup (`XLOOKUP`, `VLOOKUP`, `INDEX`/`MATCH`), date and time, statistics, financial, database, engineering, with edge cases checked against Excel. |
| **Dynamic arrays** | `FILTER`, `UNIQUE`, `SORT`, `SORTBY`, `SEQUENCE`, `LET`, `LAMBDA`, with spill semantics |
| **Real workbooks** | Load and write XLSX (Calamine, umya), CSV and JSON. Defined names, tables and cross-sheet references. |
| **Incremental recalculation** | Change an input and only its dependents recalculate. Cycle detection, iterative calculation and optional parallel evaluation. |
| **Undo / redo** | A transactional change log with action grouping, rollback and replay |
| **SheetPort** | Treat a spreadsheet as a typed function: YAML-declared inputs and outputs, validated |
| **Custom functions** | Register workbook-local functions from Rust, Python or JavaScript, or load WASM plugins |
| **Permissive license** | MIT or Apache-2.0: no AGPL, no commercial license needed |

## Who uses it for what

- **Financial models as services.** Run pricing, lending, insurance and planning workbooks server-side, with no Excel install and no office suite to babysit.
- **AI agents that work with spreadsheets.** Deterministic evaluation, an auditable change log and typed I/O. See [agent-spreadsheet](#building-an-ai-agent-use-agent-spreadsheet).
- **Products with spreadsheet logic inside.** Calculators, configurators and planning tools that run formulas in the browser or on the server, without shipping a spreadsheet UI.
- **Data pipelines.** Business logic trapped in spreadsheets, turned into reproducible, testable code paths.

## How it compares

| Library | Language | Parse | Evaluate | Write XLSX | Functions | Incremental recalc | License |
|---------|----------|-------|----------|------------|-----------|--------------------|---------|
| **Formualizer** | Rust / Python / WASM | Yes | Yes | Yes | 400+ | Yes | MIT / Apache-2.0 |
| HyperFormula | JavaScript | Yes | Yes | No | ~400 | Yes | **AGPL-3.0** or commercial |
| calamine | Rust | No | No | No | n/a | n/a | MIT / Apache-2.0 |
| openpyxl | Python | No | No | Yes | n/a | n/a | MIT |
| xlcalculator | Python | Yes | Yes | No | ~50 | Partial | MIT |
| formulajs | JavaScript | No | Yes | No | ~100 | No | MIT |

- **HyperFormula** is the closest embeddable competitor, but AGPL-3.0 requires you to open-source your application or buy a commercial license.
- **calamine** and **openpyxl** read (and, for openpyxl, write) XLSX, but don't evaluate formulas.

## Building an AI agent? Use agent-spreadsheet

Formualizer is the **engine**. For an agent that *works with* workbooks (read, profile, edit, recalculate, diff and verify them safely), use **[agent-spreadsheet](https://github.com/PSU3D0/agent-spreadsheet)**, the agent tooling layer built on it:

```
your agent / app
      │
agent-spreadsheet     CLI (`agent-spreadsheet` / `asp`) · MCP server · JS SDK
      │
  formualizer         parsing · dependency tracking · 400+ functions · recalc
```

- **CLI:** one-shot reads, safe edits, recalculation and verifiable diffs for shell-native agents and CI (`npm i -g agent-spreadsheet` or `cargo install agent-spreadsheet`).
- **MCP server:** stateful multi-turn sessions with workbook forks, checkpoints, staged edits and native recalculation.
- **JS SDK:** a typed API for app integrations, backed by the MCP server or an embedded in-process WASM engine.

Every edit is recalculated with this engine, and is traceable and diffable. That is the difference from screenshot-driven UI automation, or MCP servers that can't compute a formula.

## SheetPort: spreadsheets as typed APIs

SheetPort treats a spreadsheet as a deterministic function with typed inputs and outputs, declared in a YAML manifest:

```python
from formualizer import SheetPortSession

session = SheetPortSession.from_manifest_yaml(manifest_yaml, workbook)
session.write_inputs({"loan_amount": 250000, "rate": 0.045, "term_months": 360})
result = session.evaluate_once(freeze_volatile=True)
print(result["monthly_payment"])  # deterministic, schema-validated
```

Use it for financial model APIs, agent tool use, configuration-driven business logic and batch scenario runs. See the [SheetPort guide](https://www.formualizer.dev/docs/sheetport).

## Custom functions

Register workbook-local functions in any host:
- **Rust:** `register_custom_function`
- **Python:** `register_function`
- **JavaScript:** `registerFunction`

The rules are the same everywhere:
- Names are case-insensitive.
- Custom functions resolve before built-ins; overriding a built-in requires opting in.
- Range arguments arrive as 2-D arrays, and returning an array spills.
- A failing callback becomes a spreadsheet error.

The Rust workbook can also load sandboxed WASM plugins (features `wasm_plugins` and `wasm_runtime_wasmtime`).

Runnable examples:

```bash
cargo run -p formualizer-workbook --example custom_function_registration
python bindings/python/examples/custom_function_registration.py
cd bindings/wasm && npm run build && node examples/custom-function-registration.mjs
cargo run -p formualizer-workbook --features wasm_runtime_wasmtime --example wasm_plugin_inspect_attach_bind
```

## Architecture

```
formualizer              <-- recommended: batteries-included re-export
  formualizer-workbook   <-- workbook API, sheets, undo/redo, XLSX/CSV/JSON I/O
    formualizer-eval     <-- calculation engine, dependency authority, built-ins
      formualizer-parse  <-- tokenizer, parser, AST, pretty-printer
      formualizer-common <-- shared types (values, errors, references)
  formualizer-sheetport  <-- SheetPort runtime (spreadsheets as typed APIs)
```

| Crate | Use it when |
|-------|-------------|
| `formualizer` | Default: re-exports the workbook, engine and SheetPort behind feature flags |
| `formualizer-workbook` | You want the full workbook: sheets, I/O, undo/redo, batch operations |
| `formualizer-eval` | You own the data model and want only the calculation engine, with custom resolvers |
| `formualizer-parse` | You need only formula parsing, tokenizing, AST analysis or pretty-printing |

## Bindings

| Target | Install | Docs |
|--------|---------|------|
| Rust | `cargo add formualizer` | [docs.rs](https://docs.rs/formualizer) · [guide](https://www.formualizer.dev/docs/quickstarts/rust-quickstart) |
| Python | `pip install formualizer` | [README](bindings/python/README.md) · [guide](https://www.formualizer.dev/docs/quickstarts/python-quickstart) |
| Python (Pyodide) | `await micropip.install(wheel_url)` | [README](bindings/python/README.md#using-in-pyodide-browser--webassembly) · [guide](https://www.formualizer.dev/docs/quickstarts/pyodide-quickstart) |
| WASM | `npm install formualizer` | [README](bindings/wasm/README.md) · [guide](https://www.formualizer.dev/docs/quickstarts/js-wasm-quickstart) |

### WebAssembly profiles

- **`wasm-js`:** the browser and Node runtime used by the `formualizer` npm package, with `performance.now()`, JS entropy and wall-clock time.
- **`portable-wasm`:** no JS imports, safe for raw `wasm32-unknown-unknown` hosts such as wasmtime. Time functions default to the UTC epoch unless you inject a clock (`FixedClock` or a `ClockProvider`).

```toml
formualizer = { version = "0.10", default-features = false, features = ["portable-wasm"] }
```

Native Rust needs no feature selection.

## Documentation

**[formualizer.dev](https://www.formualizer.dev/docs)** has the full documentation:
- [Function reference](https://www.formualizer.dev/docs/reference/functions)
- the interactive [formula parser](https://www.formualizer.dev/formula-parser)
- [Core concepts](https://www.formualizer.dev/docs/core-concepts)
- [Large workbook performance](https://www.formualizer.dev/docs/guides/large-workbook-performance)

## Contributing

Contributions are welcome. Browse the open issues, or open one to discuss a proposal. Excel-compatibility fixes that come with a before-and-after table measured in Excel are especially appreciated.

```bash
cargo test --workspace
cd bindings/python && maturin develop && pytest
cd bindings/wasm && wasm-pack build --target bundler && wasm-pack test --node
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
