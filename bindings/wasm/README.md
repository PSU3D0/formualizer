<h1 align="center">Formualizer for WASM</h1>

<p align="center">
  <img alt="Arrow Powered" src="https://img.shields.io/badge/Arrow-Powered-0A66C2?logo=apache&logoColor=white" />
  <a href="https://www.npmjs.com/package/formualizer"><img alt="npm" src="https://img.shields.io/npm/v/formualizer.svg" /></a>
  <a href="../../LICENSE-MIT"><img alt="License: MIT/Apache-2.0" src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg" /></a>
  <a href="https://www.formualizer.dev/docs/quickstarts/js-wasm-quickstart"><img alt="Documentation" src="https://img.shields.io/badge/docs-formualizer.dev-blue" /></a>
</p>

<p align="center">
  <img alt="Formualizer banner" src="https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-banner.png" />
</p>

<br />

**Parse, evaluate, and mutate Excel workbooks in the browser or Node.js.**

A Rust-powered spreadsheet engine compiled to WebAssembly with 320+ Excel-compatible functions, Arrow-powered storage, and a clean TypeScript API.

## Installation

```bash
npm install formualizer
```

## Documentation

Full documentation at **[formualizer.dev](https://www.formualizer.dev/docs)**:

- [JS/WASM Quickstart](https://www.formualizer.dev/docs/quickstarts/js-wasm-quickstart)
- [JS/WASM API Reference](https://www.formualizer.dev/docs/reference/js-wasm-api-map)
- [Formula Parser](https://www.formualizer.dev/formula-parser) — interactive in-browser tool
- [Function Reference](https://www.formualizer.dev/docs/reference/functions) — 320+ built-in functions
- [SheetPort Guide](https://www.formualizer.dev/docs/sheetport) — spreadsheets as typed APIs

## Quick start

### Evaluate a workbook

```typescript
import init, { Workbook } from 'formualizer';
await init();

const wb = new Workbook();
wb.addSheet('Loans');

wb.setValue('Loans', 1, 1, 250000);  // principal
wb.setValue('Loans', 2, 1, 0.045);   // annual rate
wb.setValue('Loans', 3, 1, 360);     // months

wb.setFormula('Loans', 1, 2, '=PMT(A2/12, A3, -A1)');
console.log(await wb.evaluateCell('Loans', 1, 2)); // ~1266.71
```

### Parse formulas

```typescript
import init, { tokenize, parse } from 'formualizer';
await init();

const tokens = await tokenize('=SUMIFS(Sales,Region,"West",Year,2024)');
console.log(tokens.tokens);      // structured token array
console.log(tokens.render());    // reconstructed formula string

const ast = await parse('=IF(A1>100, A1*0.9, A1)');
console.log(ast);  // AST with node types, references, operators
```

### Undo / redo

```typescript
const wb = new Workbook();
wb.addSheet('S');
await wb.setChangelogEnabled(true);

wb.setValue('S', 1, 1, 10);
wb.setValue('S', 1, 1, 20);
await wb.undo();  // back to 10
await wb.redo();  // back to 20

// Group multiple edits into one undo step
await wb.beginAction('bulk update');
wb.setValue('S', 1, 1, 100);
wb.setValue('S', 2, 1, 200);
await wb.endAction();
await wb.undo();  // reverts both
```

### Register custom functions

```typescript
import init, { Workbook } from 'formualizer';
await init();

const wb = new Workbook();
wb.addSheet('Sheet1');

wb.registerFunction(
  'js_add',
  (a, b) => Number(a) + Number(b),
  { minArgs: 2, maxArgs: 2 },
);

wb.setFormula('Sheet1', 1, 1, '=JS_ADD(20,22)');
console.log(wb.evaluateCell('Sheet1', 1, 1)); // 42
console.log(wb.listFunctions());
wb.unregisterFunction('js_add');
```

Key semantics:

- Names are case-insensitive and normalized internally.
- Custom functions are workbook-local and resolve before global built-ins.
- Built-in override is blocked by default; opt in with `allowOverrideBuiltin: true`.
- Args are by value; ranges are delivered as JS arrays (`[[...], [...]]`).
- Return scalars, `null`/`undefined`, 1D/2D arrays (array results spill into the grid).
- JS exceptions are sanitized and mapped to `#VALUE!` errors.

Runnable example: `node bindings/wasm/examples/custom-function-registration.mjs` (after `npm run build`)

Note: the phase-4 Rust plugin seam (`register_wasm_function`) is intentionally still stubbed/pending runtime integration and is not yet surfaced in the JS API.

---

## API

### Initialization

```typescript
import init from 'formualizer';
await init(); // must be called once before using any API
```

### Formula parsing

```typescript
tokenize(formula: string, dialect?: FormulaDialect): Promise<Tokenizer>
parse(formula: string, dialect?: FormulaDialect): Promise<ASTNodeData>
```

### Tokenizer

| Method / Property | Description |
|---|---|
| `tokens` | Array of all tokens |
| `render()` | Reconstruct original formula from tokens |
| `length` | Number of tokens |
| `getToken(index)` | Get a specific token by index |

### Workbook

| Method | Description |
|---|---|
| `new Workbook()` | Create an empty workbook |
| `addSheet(name)` | Add a new sheet |
| `sheetNames()` | List all sheet names |
| `sheet(name)` | Get or create a Sheet facade |
| `setValue(sheet, row, col, value)` | Set a cell value |
| `setFormula(sheet, row, col, formula)` | Set a cell formula |
| `evaluateCell(sheet, row, col)` | Evaluate and return a cell's value |
| `evaluateAll()` | Evaluate all dirty cells |
| `evaluateCells(targets)` | Evaluate specific cells |
| `setChangelogEnabled(enabled)` | Enable/disable undo tracking |
| `beginAction(description)` | Start a named action group |
| `endAction()` | End the current action group |
| `undo()` | Undo the last action |
| `redo()` | Redo the last undone action |
| `registerFunction(name, callback, options?)` | Register a workbook-local custom function |
| `unregisterFunction(name)` | Remove a previously registered custom function |
| `listFunctions()` | List registered custom function metadata |
| `static fromJson(json)` | Load workbook from JSON string |

### Sheet

| Method | Description |
|---|---|
| `setValue(row, col, value)` | Set a cell value |
| `getValue(row, col)` | Get a cell's current value |
| `setFormula(row, col, formula)` | Set a cell formula |
| `getFormula(row, col)` | Get a cell's formula (if any) |
| `setValues(startRow, startCol, data)` | Bulk-set a 2D array of values |
| `setFormulas(startRow, startCol, data)` | Bulk-set a 2D array of formulas |
| `evaluateCell(row, col)` | Evaluate a single cell |
| `readRange(startRow, startCol, endRow, endCol)` | Read a range of values |

### SheetPortSession

| Method | Description |
|---|---|
| `static fromManifestYaml(yaml, workbook)` | Create session from YAML manifest |
| `manifest()` | Get the parsed manifest |
| `describePorts()` | List all port definitions |
| `readInputs()` | Read current input values |
| `readOutputs()` | Read current output values |
| `writeInputs(updates)` | Write typed input values |
| `evaluateOnce(options)` | Evaluate with deterministic options |

### Reference

| Method / Property | Description |
|---|---|
| `sheet` | Optional sheet name |
| `rowStart` / `rowEnd` / `colStart` / `colEnd` | Coordinates |
| `isSingleCell()` | True if single cell reference |
| `isRange()` | True if range reference |
| `toString()` | Excel-style string (e.g., `Sheet1!A1:B2`) |

---

## Building from source

```bash
# Install wasm-pack
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh

# Build
cd bindings/wasm
wasm-pack build --target bundler --out-dir pkg --release

# Full build with TypeScript wrapper
npm run build
```

## Testing

```bash
cargo test -p formualizer-wasm
wasm-pack test --node
```

## Why Formualizer?

- **Complete engine**: Parse, evaluate, mutate, and persist — not just read cached values.
- **320+ functions**: Math, text, lookup (XLOOKUP), date/time, financial, statistics, and more.
- **Fast**: Arrow-powered storage with incremental dependency tracking and parallel evaluation.
- **Portable**: Same Rust engine runs natively, in Python, and in the browser via WASM.
- **Deterministic**: Inject clock, timezone, and RNG for reproducible results.

## License

Dual-licensed under [MIT](../../LICENSE-MIT) or [Apache-2.0](../../LICENSE-APACHE), at your option.
