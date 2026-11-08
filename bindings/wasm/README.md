<h1 align="center">Formualizer</h1>

<p align="center">
  <img alt="Arrow Powered" src="https://img.shields.io/badge/Arrow-Powered-0A66C2?logo=apache&logoColor=white" />
  <a href="https://www.npmjs.com/package/formualizer"><img alt="npm" src="https://img.shields.io/npm/v/formualizer.svg" /></a>
  <a href="https://github.com/PSU3D0/formualizer/blob/main/LICENSE-MIT"><img alt="License: MIT/Apache-2.0" src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg" /></a>
  <a href="https://www.formualizer.dev/docs/quickstarts/js-wasm-quickstart"><img alt="Documentation" src="https://img.shields.io/badge/docs-formualizer.dev-blue" /></a>
</p>

<p align="center">
  <img alt="Formualizer banner" src="https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-banner.png" />
</p>

**The fastest open-source spreadsheet engine. In your JavaScript app.**

The shared Rust engine calculates a million-formula financial model in **0.26 seconds**. [See the benchmarks](https://github.com/psu3d0/formualizer#how-fast).

Load Excel workbooks, change inputs, recalculate and read results—in the browser, Node.js or a compatible JavaScript edge runtime. Formualizer ships a Rust calculation engine as WebAssembly with a typed JavaScript API. No Excel installation, office-suite process or remote calculation service. Built for AI agents and applications that need spreadsheet logic without a spreadsheet UI.

- **No compromise on speed.** Copied formulas compute as families over Arrow columns; lookups reuse indexes; only affected cells recalculate. The [engine benchmarks](https://github.com/psu3d0/formualizer#how-fast) report native measurements, not browser or edge timings.
- **Excel-compatible.** 400+ functions, dynamic arrays, `LET`, `LAMBDA`, names and cross-sheet references. Load XLSX bytes and calculate formulas, not just read cached values.
- **Built for agents.** Inspect dependencies, group edits with undo/redo, and expose deterministic typed inputs and outputs through SheetPort. For ready-made CLI and MCP tools, see [agent-spreadsheet](https://github.com/PSU3D0/agent-spreadsheet).
- **One engine across languages.** Also available for [Rust and Python](https://github.com/psu3d0/formualizer#bindings). This npm package targets JavaScript hosts; non-JavaScript WASM hosts use the [portable Rust profile](#runtime-profile).

> This package is the WebAssembly library. The `formualizer recalc` command-line tool is the separate native package [`@formualizer/cli`](https://www.npmjs.com/package/@formualizer/cli) (`npx @formualizer/cli recalc book.xlsx`); it is not included here.

## Interruptible recalculation

Use `beginRecalc()` when your UI owns the loop, `recalc()` for synchronous progress/pause, or `evaluateWithYield()` to give the host event loop task turns between steps. They use the same engine-owned job: edits between steps replan dirty work, viewport priority changes which pending cells are visited first, and cancellation retains already installed values.

```js
const outcome = await wb.evaluateWithYield({
  budgetMs: 5,
  signal: controller.signal,
  priority: [{ sheet: 'Model', startRow: 1, startColumn: 1, endRow: 40, endColumn: 12 }],
  onProgress(report, job) {
    // Repaint report.changed; on report.overflowSheets repaint visible cells.
    // wb.pendingCellsInRect(viewport) distinguishes pending from current cells.
  },
});
if (outcome.status === 'Finished') console.log(outcome.result.computedVertices);
```

`budgetMs` defaults to 5; `null` has no soft deadline and zero still progresses. Steps stop at atomic engine boundaries: a graph/schedule build, family, cycle or synchronous UDF may overrun the budget. This runs on the calling thread. The default asynchronous yield uses `setTimeout(0)`; a custom `yieldControl` must provide a task turn if timers/UI events need to run.

`recalc({ onStep(report, job) { return false; } })` returns `{ status: 'Paused', job }` while the job is still active. Inspect or edit, then call `job.step(budgetMs)` until Finished, or `job.cancel()`. Cancellation wins over false; an explicit `wb.resetCancel()` can clear cooperative `wb.cancel()`, but cannot undo terminal job cancellation or an aborted signal. `evaluateWithYield` awaits `onProgress` and ignores its return value. Abort rejects with `AbortError`, including while awaiting progress/yield hooks. Hook failures preserve the original thrown value and cancel the owned job.

Only one job/driver can own a workbook. Competing evaluation is refused, including during awaited terminal callbacks. A callback receives the owned job outside engine locks, so it may inspect, edit, reprioritize or cancel. Jobs retain the workbook after the original wrapper is freed; async abort also uses this retained owner. Explicitly freeing the job handle while a driver owns it is unsupported. End jobs explicitly; freeing a handle alone does not cancel.

Reports are detached per-step objects. Only Finished carries `result`; `progress.totalExact === false` means the denominator is not proven fixed. `changed` is conservative, and `overflowSheets` means repaint the visible region of those sheets. Job rectangles use finite, 1-based inclusive coordinates. TypeScript exports `RecalcJob`, `RecalcJobOptions`, `StepReport`, `RecalcOutcome`, driver options, and error interfaces. See the [guide](https://www.formualizer.dev/docs/guides/interruptible-recalculation) for error codes and lifecycle details.

### Run the local examples

From this checkout's `bindings/wasm` directory, with cached Rust/WASM and pnpm tooling available:

```sh
pnpm install --offline --frozen-lockfile
wasm-pack build --target bundler --out-dir pkg --dev --mode no-install -- --offline
pnpm run build:ts
pnpm run test:recalc-conformance
pnpm run example:recalc
```

These commands use the local package's self-reference (`import ... from 'formualizer'`), never an installed release. Node 26 supports this bundler build's WASM module imports; older Node versions need a WASM-aware bundler/loader. The three assertion-bearing examples are `examples/recalc-viewport.mjs` (scroll, priority, edit, changed/pending), `recalc-async.mjs` (heartbeat, abort, supersede), and `recalc-pause.mjs` (inspect, repair, resume).

For a browser smoke using the same built TypeScript facade and examples:

```sh
wasm-pack build --target web --out-dir pkg-node/recalc-browser/raw --dev --mode no-install -- --offline
pnpm run example:recalc-browser:prepare
uv run --no-project python -m http.server --bind 127.0.0.1 --directory pkg-node/recalc-browser 8000
# Open http://127.0.0.1:8000/index.html
# Expect #result[data-status="passed"] and RECALC_BROWSER_OK.
```

Preparation copies the compiled facade into ignored staging and substitutes only its raw import with a web-target initializer. It does not modify `pkg/` or `dist/`. Serve `.wasm` as `application/wasm`. Browser results must be checked in a browser; Node tests alone do not establish browser behavior.

## Installation

```bash
npm install formualizer
```

## Documentation

Full documentation at **[formualizer.dev](https://www.formualizer.dev/docs)**:

- [JS/WASM Quickstart](https://www.formualizer.dev/docs/quickstarts/js-wasm-quickstart)
- [JS/WASM API Reference](https://www.formualizer.dev/docs/reference/js-wasm-api-map)
- [Formula Parser](https://www.formualizer.dev/formula-parser) — interactive in-browser tool
- [Function Reference](https://www.formualizer.dev/docs/reference/functions) — 400+ built-in functions
- [SheetPort Guide](https://www.formualizer.dev/docs/sheetport) — spreadsheets as typed APIs

## Quick start

Rows and columns are **1-based**: row 3, column 2 is B3.

### Create and calculate a model

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


### Load an XLSX, change an input and recalculate

```typescript
import init, { Workbook } from 'formualizer';
await init();

// Browser example; use a workbook with Assumptions and Summary sheets.
const response = await fetch('/financial_model.xlsx');
if (!response.ok) throw new Error(`Workbook fetch failed: ${response.status}`);
const wb = Workbook.fromXlsxBytes(new Uint8Array(await response.arrayBuffer()));
wb.setValue('Assumptions', 3, 2, 0.07);  // B3
wb.evaluateAll();
console.log(wb.sheet('Summary').getValue(5, 2)); // B5
```

For Node.js, read the bytes with `readFile` from `node:fs/promises` instead of `fetch`. Initialization and WASM asset loading depend on your runtime and bundler; see the [JS quickstart](https://www.formualizer.dev/docs/quickstarts/js-wasm-quickstart). Calculation methods are synchronous: use a Web Worker for large browser workloads to keep the UI responsive.

Native is the default XLSX reader. `backend: 'calamine'` remains a native compatibility alias. The raw WASM API accepts the same options through `Workbook.fromXlsxBytesWithOptions`. Native reading requires `xlsx-recalc` (included in default builds); it does not enable XLSX writing.

```ts
const wb = Workbook.fromXlsxBytes(bytes, {
  loadLimits: {
    maxInputBytes: 16 * 1024 * 1024,
    maxPartBytes: 32 * 1024 * 1024,
  },
});
```

`loadLimits` has exactly five optional numeric fields: `maxInputBytes`, `maxEntries`, `maxExpandedBytes`, `maxPartBytes`, and `maxXmlDepth`. Omitted fields inherit Rust defaults: **127 MiB** compressed input (133,169,152 bytes), 10,000 ZIP entries, 256 MiB aggregate actual consumed output, 128 MiB per consumed part, and XML depth 128. Zero means zero allowance. Unknown nested keys and values other than non-negative safe integers within the host allocation width are rejected (on wasm32, at most 2,147,483,647). Explicit `loadLimits`, including `{}`, is unsupported for JSON loading and empty Workbook construction.

Ordinary loading selectively reads required parts: an unread image/attachment is not decompressed or CRC-validated; corrupt required parts still fail. This is not whole-file integrity validation. Raising a limit is an explicit resource opt-in, not a guarantee that a workbook fits browser memory. These limits do not replace engine/model/host memory bounds and cannot avoid the inbound JS-to-WASM byte transfer allocation. Strict XLSX recalculation keeps its separate limits and integrity rules.

### Cache-only XLSX recalculation

```typescript
import { recalculateXlsxBytes } from 'formualizer';

const input = new Uint8Array(await (await fetch('/model.xlsx')).arrayBuffer());
const result = await recalculateXlsxBytes(input);
console.log(result.summary.status, result.cache_cells_changed);
// `result.bytes` is a Uint8Array ready for download/upload.

// Reproducible TODAY/NOW and RAND (the CLI's --now/--tz/--seed):
const fixed = await recalculateXlsxBytes(input, undefined, {
  deterministicTimestampUtc: '2026-01-31T09:00:00Z',
  deterministicTimezone: '+01:00', // default 'utc'
  rngSeed: 7,
});
console.log(fixed.clock, fixed.seed); // what the run used; seed is a bigint
```

`RAND` is reproducible by default. Without `deterministicTimestampUtc`,
`TODAY`/`NOW` use the host's local time; `result.clock.now` reports the instant
they saw.

This delegates to the shared Rust cache-only XLSX recalculator: formula text and
unrelated package members are retained, while formula cached values are updated.
Safe core resource limits apply; `errorLocationLimit` only limits stored error
locations.

Supported inputs include ordinary/shared scalar formulas, supported calculation
names, new multi-cell spills from ordinary non-shared formulas, and existing
XLDAPR anchors with validated `cm`/array-`ref` metadata. Growth, shrink, collapse
and blocked anchors are supported, with readers `A1#` and
`_xlfn.ANCHORARRAY(A1)`. Source-declared children are generated caches, not
independent inputs: externally edited child values are recalculated while their
ownership remains declared. Obsolete caches are cleared while styles remain.

`formula_cells` and `summary.evaluated` count source formulas/anchors, not
children. `cache_cells_changed` counts physical caches inserted, replaced or
cleared, including children, and may exceed `formula_cells`.
`worksheet_parts_changed` counts worksheets only, not metadata/relationships.
`external_links` (`{ links_used, refreshed: false, policy: 'cached' }`) is
present only when formulas read external workbook links; their values come from
the cache Excel stored in the workbook and are never refreshed. Pass
`{ externalLinks: 'refuse' }` (default `'cached'`, like the CLI's
`--external-links`) to reject such workbooks instead; the policy is decided
after calculation and nothing is returned. Workbooks whose links nothing reads
still recalculate.
Deterministic unchanged outputs recalculate to byte-identical no-ops.

Legacy fixed-extent (CSE) arrays, elementwise `IF`, Excel tables within a
validated subset, volatile functions and external links (read from the values
cached in the workbook) are recalculated too. Data tables, unservable external
references, rich/unknown or malformed metadata, hidden-row
`SUBTOTAL`/`AGGREGATE` ranges, circular references, shared-family multi-cell
spills and spill publication across merges are refused. Safe ZIP/XML/cell/output
limits remain in effect. Fresh unmarked 1x1 results have no spill identity
(`A1#` returns `#REF!`). No Excel-equivalence claim is made; no Excel execution
oracle was used. See [the precise eligibility contract](https://github.com/psu3d0/formualizer/blob/main/docs/cache-only-xlsx.md).

### Source-aware calculation policy

Native Formualizer remains strict by default. Pass `{ profile: 'workbook', calculation: {...}, requireComplete: true }` as the third argument to `recalculateXlsxBytes` to select source-aware settings and sparse admission:

```ts
const result = await recalculateXlsxBytes(input, undefined, {
  profile: 'workbook',
  calculation: {
    workbookSeed: '18446744073709551615',
    fixedNowUtc: '2024-01-02T03:04:05Z',
    timezone: 'utc',
    iterationInitialState: 'require_verified',
  },
  requireComplete: true,
});
```

`SourceCalculationPolicy` is the shared portable request: `iteration` (`mode: workbook|disabled|iterate`; optional `maxIterations`/`maxChange` only for `iterate`), `cycleDetection` (`runtime|static`), exact decimal-string or JS-safe integer `workbookSeed`, RFC3339 `fixedNowUtc`, `timezone` (`utc|local|{offsetSeconds}`), `maxElapsedMs`, `maxWorkUnits`, and `iterationInitialState` (`require_verified|cold_start`). Unknown options, invalid types and bounds fail before package loading. The source header owns the date system.

Options must be a plain object or null-prototype record with own enumerable string-keyed data properties. Nested `calculation` and `limits` values accept the same records and JSON scalar values; omit optional fields instead of supplying `undefined`. Maps, arrays, custom instances/prototypes, accessors, symbols, nonfinite numbers and cycles are rejected. Portable subrecords are bounded to 32 object ancestors, 4,096 portable nodes per request and 4,096 own properties per record and 65,536 UTF-16 units per string/key. Values are snapshotted once into the exact JSON request decoded upstream. Reflection on proxies may execute traps; a request either rejects or consumes the validated descriptor snapshot. Proxies are not categorically detected. Legacy bigint `rngSeed` and Date `deterministicTimestampUtc` remain supported at their existing top-level fields.

Legacy `rngSeed`, `deterministicTimestampUtc` and `deterministicTimezone` remain available. They fill omitted nested fields; equal explicit values are accepted and conflicts fail. Legacy timezone still requires legacy timestamp. The result's bigint `seed` and `clock` reflect the effective policy.

Results forward `calculation` settings/origins/warnings (or null on the legacy route), `coverage`, `coverage_scope`, `caches_invalidated`, `calculation_flags_changed`, and the full canonical `operation_diagnostics`, including function incidence, unavailable result cells and bounded samples. Complete coverage describes known gaps under declared assumptions, not numerical Excel equivalence. Partial output clears affected caches and retains independent results.

`limits` is an optional partial `SourceRecalculateLimits` object with fourteen fields: `maxInputBytes`, `maxEntries`, `maxExpandedBytes`, `maxWorksheetBytes`, `maxFormulaCells`, `maxOutputBytes`, `maxXmlDepth`, `maxCells`, `maxColumns`, `maxOperationRoots`, `maxOperationTextBytes`, `maxAffectedWork`, `maxAffectedRegions`, and `maxAffectedScratchBytes`. Missing/null limits preserve the selected preset, missing fields preserve that field, and zero is an explicit bound. Supplied fields require nonnegative integers: bytes/counts fit u32, columns do not exceed 16,384, and affected work fits JS safe integers. Null fields, strings, booleans, fractions, negative/unsafe/nonfinite numbers are rejected. Presets are selected before legacy overrides; `requireComplete` does not select a profile.

`physicalTelemetry` is a strict boolean, false by default (undefined is absent). Physical counters are present as null when unmeasured, measured zero as 0, and integers above the JS-safe range as exact decimal strings. Diagnostics retain typed executed function modes and unverified providers independently of bounded display samples. Dependency evidence concerns the `admitted_workbook_and_fixed_cached_sources` source domain; semantic reads remain unattested. Its `work` is `queryWork + publicationMembershipWorkBound`, including the conservative publication membership bound. These budgets do not promise universal CPU time or RSS bounds.

`requireComplete` accepts legitimate Excel errors but rejects known gaps. Rejected promises retain `kind: 'CalculationDeferred'`, `reason`, and `context` for typed source deferral. Live source iteration defers until initial state is verified unless `cold_start` is explicitly selected and recorded. Cancellation, resource exhaustion, corrupt packages and output failures remain errors.

### SheetPort: a workbook as a typed function

A manifest names the input and output cells with their types and constraints; the session validates inputs, recalculates and returns typed outputs. `defineManifest` fills in the boilerplate (spec version, shapes, `{ a1: ... }` locations) and returns the canonical manifest object, typed as `FioManifest`; `fromManifest` also accepts a hand-written canonical object.

<!-- example: bindings/wasm/examples/sheetport-quickstart.mjs#readme -->
```typescript
import init, { Workbook, SheetPortSession, defineManifest } from 'formualizer';
await init();

// The model: B4 computes the monthly payment from B1:B3.
const wb = new Workbook();
wb.addSheet('Calculator');
wb.setFormula('Calculator', 4, 2, '=PMT(B2/12, B3, -B1)');

const manifest = defineManifest({
  id: 'loan-calculator',
  name: 'Loan calculator',
  ports: [
    { id: 'principal', dir: 'in', location: 'Calculator!B1', type: 'number', constraints: { min: 0 } },
    { id: 'rate', dir: 'in', location: 'Calculator!B2', type: 'number' },
    { id: 'term_months', dir: 'in', location: 'Calculator!B3', type: 'integer' },
    { id: 'monthly_payment', dir: 'out', location: 'Calculator!B4', type: 'number' },
  ],
});

const session = SheetPortSession.fromManifest(manifest, wb);
session.writeInputs({ principal: 250000, rate: 0.045, term_months: 360 });
const outputs = session.evaluateOnce({ freezeVolatile: true });
console.log(outputs.monthly_payment); // 1266.71...
```

A rejected update throws an error whose `violations` name each failing port, and writes nothing. A port's `location` can also be a defined name (`{ name: 'Rate' }`) or the rows under a header (`{ layout: ... }`), and its `schema` a range, record or table instead of `type`. To ship the manifest as a file, load it with `SheetPortSession.fromManifestYaml(yaml, wb)`. See the [manifest format](https://www.formualizer.dev/docs/sheetport/manifest-format).

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
wb.setChangelogEnabled(true);

wb.setValue('S', 1, 1, 10);
wb.setValue('S', 1, 1, 20);
wb.undo();  // back to 10
wb.redo();  // back to 20

// Group multiple edits into one undo step
wb.beginAction('bulk update');
wb.setValue('S', 1, 1, 100);
wb.setValue('S', 2, 1, 200);
wb.endAction();
wb.undo();  // reverts both
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

Runnable [custom-function example](https://github.com/psu3d0/formualizer/blob/main/bindings/wasm/examples/custom-function-registration.mjs) (after building from a source checkout).

Note: the Rust-side WASM UDF plugin path (`formualizer-workbook` features `wasm_plugins` / `wasm_runtime_wasmtime`) is not surfaced in the JS API; JS callbacks registered with `registerFunction` are the custom-function mechanism here.

---

## API

### Initialization

```typescript
import init from 'formualizer';
await init(); // must be called once before using any API
```

`init`, `tokenize`, `parse` and `recalculateXlsxBytes` return promises. `Workbook`, `Sheet` and `SheetPortSession` methods are synchronous once the module is initialized.

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
| `new Workbook(options?)` | Create an empty workbook; options cover span evaluation, cycle detection/policy, iteration limits and per-request work/time budgets |
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
| `static fromXlsxBytes(bytes, options?)` | Load XLSX bytes; `backend: 'native'` (default) or `'calamine'` (native alias) |
| `static fromXlsxBytesWithOptions(bytes, options)` / `static fromJsonWithOptions(json, options)` | Same loaders with `WorkbookLoadOptions` |
| `inspectCell(cell, options?)` / `precedents(...)` / `dependents(...)` / `trace(...)` / `rangePage(...)` | Inspection and dependency tracing |
| `getEvalPlan(targets)` | Inspect the evaluation schedule before computing |
| `lastCycleTelemetry()` | Per-recalc iterative-calculation counters |
| `cancel()` / `resetCancel()` | Cooperative cancellation of a running evaluation |

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
| `static fromManifest(manifest, workbook)` | Create a session from a manifest object (see `defineManifest`) |
| `static fromManifestYaml(yaml, workbook)` | Create a session from a YAML manifest |
| `manifest()` | Get the parsed manifest |
| `describePorts()` | List all port definitions |
| `readInputs()` | Read current input values |
| `readOutputs()` | Read current output values |
| `writeInputs(updates)` | Write typed input values |
| `evaluateOnce(options)` | Evaluate with deterministic options |

`evaluateOnce(options)` accepts:
- `freezeVolatile?: boolean`
- `rngSeed?: number` - non-negative safe integer
- `deterministicTimestampUtc?: Date | string` - fixed instant as a JS `Date` or RFC3339 timestamp
- `deterministicTimezone?: "utc" | "local" | number` - timezone for `NOW()` / `TODAY()`; numeric offsets are seconds east of UTC and require `deterministicTimestampUtc`

### Reference

| Method / Property | Description |
|---|---|
| `sheet` | Optional sheet name |
| `rowStart` / `rowEnd` / `colStart` / `colEnd` | Coordinates |
| `isSingleCell()` | True if single cell reference |
| `isRange()` | True if range reference |
| `toString()` | Excel-style string (e.g., `Sheet1!A1:B2`) |

---

## Runtime profile

This package uses the **`wasm-js`** runtime profile of the Formualizer Rust core.

That means:
- `performance.now()` is used for timing (via `web-time`).
- `crypto.getRandomValues` is used for entropy.
- Ambient wall-clock time is available for `NOW()`, `TODAY()`, etc.

This is the correct profile for browser and Node.js hosts. If you are embedding Formualizer inside a raw **wasmtime** guest or any non-JS wasm host, use the `portable-wasm` Rust feature on the `formualizer` crate instead (see the [main README](https://github.com/psu3d0/formualizer#webassembly-profiles)).

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

## License

Dual-licensed under [MIT](https://github.com/PSU3D0/formualizer/blob/main/LICENSE-MIT) or [Apache-2.0](https://github.com/PSU3D0/formualizer/blob/main/LICENSE-APACHE), at your option. Both license texts ship inside the package.


## Workbook workers

Import the worker client without loading WASM on the calling thread:

```js
import { createWorkbookWorker } from 'formualizer/worker';

const client = await createWorkbookWorker(
  new Worker(new URL('formualizer/worker-entry', import.meta.url), { type: 'module' }),
  { autoRecalc: true, autoRecalcBudgetMs: 5 },
);
await client.addSheet('Model');
const viewport = await client.subscribeViewport({
  rect: { sheet: 'Model', startRow: 1, startColumn: 1, endRow: 40, endColumn: 12 },
  mode: 'values',
  onFrame(frame) {
    // Keep optimistic edits whose clientSeq is greater than frame.ackClientSeq.
    // frame.grid replaces a viewport; frame.patches updates individual cells.
    console.log(frame.revision, frame.evaluatedRevision, frame.hasPending);
  },
});
await client.setValue('Model', 1, 1, 20);
await client.setFormula('Model', 2, 1, '=A1+22');
await client.recalc({ budgetMs: 5 });
await viewport.setRect({ ...viewport.rect, startRow: 10, endRow: 49 });
await viewport.unsubscribe();
await client.dispose();
```

Worker endpoints accept DOM Worker/MessagePort/worker-global APIs and Node worker_threads Worker/MessagePort/parentPort APIs. The packaged `worker-entry` targets browser module workers; in Node, use a small host entry:

```js
// workbook-host.mjs
import { parentPort } from 'node:worker_threads';
import * as runtime from 'formualizer';
import { attachWorkbookWorker } from 'formualizer/worker';
await attachWorkbookWorker(parentPort, { runtime });
```

Pass `new Worker(new URL('./workbook-host.mjs', import.meta.url))` to `createWorkbookWorker`. A custom browser host similarly calls `attachWorkbookWorker(self, { runtime })`. `createInProcessWorkbookWorker({ runtime, autoRecalc: true })` uses a MessageChannel for testing or a single-thread fallback. If runtime is omitted on a host, it dynamically imports and initializes `formualizer`. Your worker bundler/runtime must support the package's bundler-target WASM import; the client-only `formualizer/worker` entry has no static WASM dependency.

Each mutation resolves with a `WorkerStateHeader`: `revision`, `evaluatedRevision`, `ackClientSeq`, `recalcStatus`, and `hasActiveJob`. Loads reset revision to zero while acknowledging the monotonic client sequence. `client.state`, `client.lastSentClientSeq`, `client.hasInFlightMutations`, and `onStatusChange` support optimistic views. Ordinary reads return their usual values; `getValueWithState` and `readRangeWithState` return `{ data, ...header }`. Cached reads never evaluate or create a missing sheet.

Viewport modes are `values` (full grids or sparse patches), `snapshots` (inspection cells), `deltas` (locations/pending only), and `columnar` (transferable tags, numbers, string table, pending bitset). All coordinates are 1-based inclusive. Subscriptions prioritize their rectangles unless `prioritize: false`; `setRect` retargets them on active and future jobs. Intermediate updates skip unchanged viewports, coalesce dirty rectangles behind a one-frame credit, and honor `throttleMs` (default 16). Initial, manual, rect-change, and terminal refreshes flush immediately. Call `refresh()` for a full reread and `unsubscribe()` or `[Symbol.dispose]()` when finished. Columnar tags: 0 empty, 1 number, 2 boolean, 3 string, 4 error, 5 temporal/other JSON; `numbers` holds either the numeric value or the string-table index.

The async RPC surface includes sheet/table/name metadata, scalar/block mutations, `editBatch`, action boundaries, undo/redo, temporal egress, inspection, dependency tracing, range pages, eval plans, telemetry, one-shot evaluation, parser/tokenizer helpers, and SheetPort sessions. `editBatch` accepts `{ op: 'setValue', sheet, row, col, value }`, formula/block equivalents, or `addSheet`; `{ actionDescription }` groups edits into a native undo action when changelog is enabled. An edit batch is ordered, not rollback-atomic on engine failure. `createSheetPortSession(manifestOrYaml)` returns an async session with manifest/port description, input/output reads, input writes, evaluation and disposal; `evaluateSheetPort` is the one-shot equivalent. Reload disposes old sessions and cached sheet handles.

`autoRecalc` folds edits into one background job and yields a real task turn between steps. One-shot `evaluateAll`, `evaluateCell(s)`, and SheetPort evaluation cancel an implicit auto job first, preserve installed values, then reschedule background work. An explicit paused/driven job retains the engine's `RECALC_JOB_ACTIVE` refusal. `beginRecalc` starts/adopts a paused explicit job; `stepRecalc`, `pauseRecalc`, `prioritize`, and `cancelRecalc` control it. `recalc` resumes paused work and returns Finished/Paused/Cancelled; returning false from `onStep` pauses. `recalcStream` yields detached step reports; breaking defaults to pausing, or cancels with `pauseOnReturn: false`. Both drivers support AbortSignal, including cancellation while awaiting `onProgress`; abort rejects with AbortError. Budgets are soft: cancellation cannot preempt a running indivisible WASM unit.

Register custom functions inside the host with `attachWorkbookWorker(endpoint, { functions })`, load a worker-side ESM module using `registerFunctionModule(url, { exportName: 'functions' })`, or explicitly opt into `allowFunctionFromSource: true` for `registerFunctionFromSource(name, source, options)`. Modules export a name-to-callback map (or callback/options entries); the registry persists across reloads unless the host sets `clearFunctionsOnLoad`. Source registration executes code and is disabled by default. `listFunctions` and `unregisterFunction` manage the same registry.

`loadFromJson(json, options)` forwards workbook options just like the XLSX loader. `loadFromXlsxBytes(bytes, options, { transfer: true })` and `recalculateXlsxBytes(bytes, limit, options, { transfer: true })` transfer whole ArrayBuffers; typed subviews and Node Buffers are copied first so unrelated slab bytes are never detached. Cache-only XLSX output buffers transfer by default (disable with `transfer: false`). Error replies preserve domain fields, exact resource strings and recursive causes.

Run `npm run test:worker-conformance` and `npm run example:worker`. The examples are `examples/worker-reactive-grid.mjs` and `examples/worker-rpc-service.mjs`; the browser fixture also verifies a real Worker. Dispose clients to free jobs, sessions, sheets and the workbook; workers terminate by default, configurable with `terminateOnDispose: false`.
