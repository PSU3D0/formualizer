<h1 align="center">Formualizer</h1>

<p align="center">
  <img alt="Arrow Powered" src="https://img.shields.io/badge/Arrow-Powered-0A66C2?logo=apache&logoColor=white" />
  <a href="https://github.com/psu3d0/formualizer/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/psu3d0/formualizer/actions/workflows/ci.yml/badge.svg" /></a>
  <img alt="Python Coverage" src="https://raw.githubusercontent.com/psu3d0/formualizer/badges/coverage.svg" />
  <img alt="Rust Core Coverage" src="https://raw.githubusercontent.com/psu3d0/formualizer/badges/rust-core-coverage.svg" />
  <a href="https://crates.io/crates/formualizer"><img alt="crates.io" src="https://img.shields.io/crates/v/formualizer.svg" /></a>
  <a href="https://pypi.org/project/formualizer/"><img alt="PyPI" src="https://img.shields.io/pypi/v/formualizer.svg" /></a>
  <a href="https://www.npmjs.com/package/formualizer"><img alt="npm" src="https://img.shields.io/npm/v/formualizer.svg" /></a>
  <a href="https://www.formualizer.dev/docs"><img alt="Documentation" src="https://img.shields.io/badge/docs-formualizer.dev-blue" /></a>
  <a href="#license"><img alt="License: MIT/Apache-2.0" src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg" /></a>
</p>

<p align="center">
  <img alt="Formualizer banner" src="https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-banner.png" />
</p>

<br />

**A lightning-fast, embeddable spreadsheet runtime for Rust, Python, and JavaScript.** Parse formulas, load and edit Excel workbooks, recalculate them incrementally, and expose spreadsheet models as deterministic, typed APIs—without Excel.

No Excel COM automation. No LibreOffice UNO bridge. No stitching together slow Python libraries for workbook editing and partial formula evaluation.

Formualizer combines broad Excel-compatible formula support with Arrow-backed storage, dependency-aware recalculation, dynamic arrays, file I/O, undo/redo, and SheetPort. One permissively licensed Rust core ships to Rust, Python, browsers, and Node.js.

**New in 0.10:** copied formulas compute as families instead of cell by cell. Real-world workbooks load and calculate in about a third of the time 0.9.3 took, recalculate an edit 5–25× faster and use about an eighth of the memory. A 1M-formula model calculates in 0.38 s with 45 MB of heap. Against LibreOffice Calc, loading and calculating takes about a quarter of the time, and lookup-heavy models run up to 150× faster. [See the numbers](#performance).

---

## Highlights

| | |
|---|---|
| **400+ Excel functions** | Math, text, lookup (XLOOKUP, VLOOKUP), date/time, statistics, financial, database, engineering |
| **Three language targets** | Rust, Python (PyO3), and WASM (browser + Node) with consistent APIs |
| **Arrow-powered storage** | Apache Arrow columnar backing with spill overlays for efficient large-workbook evaluation |
| **Family execution** | Copied formulas are one dependency node and one evaluation over typed Arrow lanes, with range, criteria and lookup kernels |
| **Incremental recalculation** | Region-based dependency tracking, cycle detection, chained recurrences, optional parallel evaluation |
| **Dynamic arrays** | FILTER, UNIQUE, SORT, SORTBY, XLOOKUP with automatic spill semantics |
| **Undo / redo** | Transactional changelog with action grouping, rollback, and replay |
| **File I/O** | Load and write XLSX (calamine, umya), CSV, JSON — all behind feature flags |
| **SheetPort** | Treat any spreadsheet as a typed API with YAML manifests, schema validation, and batch evaluation |
| **Deterministic mode** | Inject clock, timezone, and RNG seed for reproducible evaluation (built for AI agents) |

## Documentation

📖 **[formualizer.dev](https://www.formualizer.dev/docs)** — full documentation, interactive tools, and API reference.

- [Quickstarts](https://www.formualizer.dev/docs/quickstarts) — get running in Rust, Python, or JS/WASM in minutes
- [Function Reference](https://www.formualizer.dev/docs/reference/functions) — 400+ built-in functions with examples
- [Formula Parser](https://www.formualizer.dev/formula-parser) — interactive browser-based formula parser and AST inspector
- [SheetPort Guide](https://www.formualizer.dev/docs/sheetport) — treat spreadsheets as typed, deterministic APIs
- [Core Concepts](https://www.formualizer.dev/docs/core-concepts) — dependency tracking, evaluation pipeline, coercion rules
- [Large Workbook Performance](https://www.formualizer.dev/docs/guides/large-workbook-performance) — loading, sparse ingest and large-model guidance

## Who is this for?

- **Fintech & insurance teams** replacing Excel VBA or server-side workbook evaluation with a fast, deterministic engine that doesn't require Excel installed.
- **AI / agent builders** who need programmatic spreadsheet manipulation with deterministic evaluation, auditable changelogs, and typed I/O via SheetPort.
- **SaaS products** embedding spreadsheet logic — pricing calculators, planning tools, configurators — without shipping a full spreadsheet UI.
- **Data engineers** extracting business logic trapped in spreadsheets into reproducible, testable pipelines.

## Building an AI agent? Use agent-spreadsheet

Formualizer is the **engine**. If you want an agent to *work with* workbooks — read, profile, edit, recalculate, diff, and verify them safely — use **[agent-spreadsheet](https://github.com/PSU3D0/agent-spreadsheet)**, the official agent tooling layer built on this engine:

```
your agent / app
      │
agent-spreadsheet     CLI (`agent-spreadsheet` / `asp`) · MCP server · JS SDK
      │
  formualizer         parsing · dependency graph · 400+ functions · recalc
```

- **CLI** — stateless one-shot reads, safe edits, recalc, and verifiable diffs for shell-native agents and CI (`npm i -g agent-spreadsheet` or `cargo install agent-spreadsheet`)
- **MCP server** — stateful multi-turn sessions with workbook forks, checkpoints, staged edits, and native recalculation (no LibreOffice fallback)
- **JS SDK** — typed API for app integrations, backed by the MCP server or an embedded in-process WASM engine

Unlike screenshot-driven UI automation or MCP servers that can't compute a formula, agent-spreadsheet evaluates the actual workbook with this engine — every edit is recalculated, traceable, and diffable.

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
formualizer = "0.10"
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

## Performance

Formualizer 0.10 evaluates copied formulas as families rather than one cell at a time.
- Fill a formula down a column and the whole run becomes one dependency node, with one shared template.
- The run evaluates in one pass over typed Arrow columns, and its results are written back as a block.
- Lookups and conditional aggregates over a fixed table index that table once per run, not once per cell.
- Cells that hold only values cost no graph vertex at all.

Values are identical to cell-by-cell evaluation. The per-cell path stays in the engine as a differential-testing oracle.

**Against Formualizer 0.9.3** (release builds, same machine; lower is better):

| | load + first calculation | value edit + recalc | heap after calculation |
|---|---|---|---|
| Enron workbook sample (25 real-world files) | 0.31× | 0.15× | 0.13× |
| Real financial and operating models | 0.29× | 0.22× | 0.06× |
| Synthetic benchmark workbooks (17) | 0.30× | 0.10× | 0.21× |

**A 1M-formula financial model:**

| | 0.9.3 | 0.10 |
|---|---|---|
| load | 25.1 s | 4.2 s |
| first calculation | 4.8 s | 0.38 s |
| value edit + recalc | 2.5 s | 52 ms |
| heap after calculation | 897 MB | 45 MB |
| peak heap | 2.9 GB | 195 MB |

**Against LibreOffice Calc 24.2** (headless, threaded calculation), loading and calculating the same 52 workbooks and generated workloads:

| | Formualizer / LibreOffice |
|---|---|
| load + first calculation | 0.24× (faster in 48 of 52) |
| calculation alone | 0.44× |
| lookup- and criteria-heavy models | 7–150× faster end to end (40k `INDEX`/`MATCH` over 50k rows: 0.35 s vs 53 s) |

Measured on a shared 24-core machine, so treat the figures as indicative. The method and full tables are in [benchmarks/0.10-vs-0.9.3.md](benchmarks/0.10-vs-0.9.3.md).

What makes it fast:
- **A region-based dependency authority.** A formula family is one node with relative edges, so dependency memory and dirty propagation scale with the number of distinct formulas, not cells.
- **Family execution on typed lanes.** Arithmetic, comparisons, `IF`/`IFERROR`/`AND`/`OR`, `ROUND`, `MIN`/`MAX`/`SUM`, type tests and date parts run column-wise over `f64` lanes, with the exact scalar semantics for errors and formats.
- **Kernels.** Windowed `SUM`/`AVERAGE`/`MIN`/`MAX`/`COUNT` replicate the scalar summation order bit for bit. `SUMIFS`-style criteria index their fixed ranges once. Run-invariant lookups are computed once per run, and lookup indexes are shared across threads.
- **Recurrences as chains.** Running totals and `=C2+D1`-style recurrences evaluate as one sequential unit instead of one scheduling layer per row.
- **Incremental everything.** Only dirty regions recalculate. Small recalculations skip the planner. Structural edits move runs of copied formulas as blocks.
- **Optional parallelism.** Independent work runs on Rayon, and cheap layers stay on one thread.

## Custom functions (workbook-local)

You can register custom functions per workbook in Rust, Python, and JS/WASM.

- Rust: `register_custom_function` / `unregister_custom_function` / `list_custom_functions`
- Python: `register_function` / `unregister_function` / `list_functions`
- JS/WASM: `registerFunction` / `unregisterFunction` / `listFunctions`

Semantics are consistent across hosts:

- Function names are case-insensitive (`my_fn`, `MY_FN`, and `My_Fn` refer to the same function).
- Custom functions are workbook-local and resolve before global built-ins.
- Overriding built-ins is blocked by default; opt in with `allow_override_builtin` (Rust/Python) or `allowOverrideBuiltin` (JS).
- Arguments are passed by value; range arguments are materialized as 2D arrays/lists.
- Returning an array spills into the grid using normal dynamic-array behavior.
- Callback failures become spreadsheet errors (`ExcelError` in Rust, `#VALUE!` mapping for Python/JS exceptions).

Runnable examples:

- Rust callback custom function: `cargo run -p formualizer-workbook --example custom_function_registration`
- Python callback custom function: `python bindings/python/examples/custom_function_registration.py`
- JS/WASM callback custom function: `cd bindings/wasm && npm run build && node examples/custom-function-registration.mjs`
- Rust WASM plugin inspect catalog: `cargo run -p formualizer-workbook --features wasm_plugins --example wasm_plugin_inspect_catalog`
- Rust WASM plugin inspect + attach + bind: `cargo run -p formualizer-workbook --features wasm_runtime_wasmtime --example wasm_plugin_inspect_attach_bind`
- Rust WASM plugin directory attach: `cargo run -p formualizer-workbook --features wasm_runtime_wasmtime --example wasm_plugin_attach_dir`

WASM plugin path (Rust workbook API):

- Effect-free inspect APIs are available:
  - `inspect_wasm_module_bytes`
  - `inspect_wasm_module_file` *(native only)*
  - `inspect_wasm_modules_dir` *(native only)*
- Explicit workbook-local attach/bind APIs are available:
  - `attach_wasm_module_bytes` / `attach_wasm_module_file` / `attach_wasm_modules_dir`
  - `bind_wasm_function`
- Runtime behavior:
  - `wasm_plugins` only: runtime remains pending (`#N/IMPL` on bind)
  - `wasm_runtime_wasmtime` (native): `use_wasmtime_runtime()` enables executable plugin bindings

## How is this different?

| Library | Language | Parse | Evaluate | Write | Functions | Dep. graph | License |
|---------|----------|-------|----------|-------|-----------|------------|---------|
| **Formualizer** | Rust / Python / WASM | Yes | Yes | Yes | 400+ | Yes (incremental) | MIT / Apache-2.0 |
| HyperFormula | JavaScript | Yes | Yes | No | ~400 | Yes | **AGPL-3.0** (or commercial) |
| calamine | Rust | No | No | No | N/A | N/A | MIT / Apache-2.0 |
| openpyxl | Python | No | No | Yes | N/A | N/A | MIT |
| xlcalc | Python | Yes | Yes | No | ~50 | Partial | MIT |
| formulajs | JavaScript | No | Yes | No | ~100 | No | MIT |

- **HyperFormula** is the closest feature competitor, but its AGPL-3.0 license requires you to open-source your entire application or purchase a commercial license from Handsontable. Formualizer is permissively licensed with no strings attached.
- **calamine** is read-only — it extracts cached values from XLSX files but cannot evaluate formulas.
- **openpyxl** reads and writes XLSX but has no formula evaluation engine.
- **xlcalc** evaluates formulas but supports a fraction of Excel's function library and has limited dependency tracking.
- **Formualizer** is a complete, permissively-licensed engine: parse formulas, track dependencies, evaluate with 400+ functions, mutate workbooks, undo/redo — from Rust, Python, or the browser.

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

## Bindings

| Target | Install | Docs |
|--------|---------|------|
| Rust | `cargo add formualizer` | [docs.rs](https://docs.rs/formualizer) · [guide](https://www.formualizer.dev/docs/quickstarts/rust-quickstart) |
| Python | `pip install formualizer` | [README](bindings/python/README.md) · [guide](https://www.formualizer.dev/docs/quickstarts/python-quickstart) |
| Python (Pyodide) | `await micropip.install(wheel_url)` (compatible wheel URL) | [README](bindings/python/README.md#using-in-pyodide-browser--webassembly) · [guide](https://www.formualizer.dev/docs/quickstarts/pyodide-quickstart) |
| WASM | `npm install formualizer` | [README](bindings/wasm/README.md) · [guide](https://www.formualizer.dev/docs/quickstarts/js-wasm-quickstart) |

Both Python and WASM bindings expose the same core API surface: tokenization, parsing, workbook operations, evaluation, undo/redo, and SheetPort.

## WebAssembly runtime profiles

Formualizer supports two explicit wasm profiles so the right runtime assumptions are always in scope.

### `portable-wasm` — raw / wasmtime-safe

No JS globals, no `wasm-bindgen` imports, no browser clock or entropy. Safe to compile as a raw `wasm32-unknown-unknown` module and instantiate inside wasmtime or any non-JS wasm host.

Time-dependent functions (`NOW`, `TODAY`, etc.) default to UTC epoch when no clock is injected; supply a `FixedClock` or implement `ClockProvider` to drive them deterministically.

```toml
# Cargo.toml
[dependencies]
formualizer = { version = "0.5", default-features = false, features = ["portable-wasm"] }
```

For individual crates:
```toml
formualizer-eval = { version = "0.5", default-features = false }
```

### `wasm-js` — browser / Node via wasm-bindgen

Full browser-compatible runtime: `web-time` for `performance.now()` timing, JS-backed entropy (`crypto.getRandomValues`), and ambient wall-clock time for date functions. This is the profile used by the `formualizer` npm package.

```toml
# Cargo.toml — for crates that also depend on wasm-bindgen
[dependencies]
formualizer = { version = "0.5", default-features = false, features = ["wasm-js"] }
```

The `formualizer` npm package (`bindings/wasm`) selects this profile automatically.

### Default (native)

No action needed for native Rust targets. `cargo add formualizer` gives the full stack with system clock, JSON/CSV support, and no wasm-specific features.

---

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
