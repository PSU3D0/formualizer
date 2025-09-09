# Formualizer WASM Bindings

WebAssembly bindings for Formualizer — an engine-backed Excel-like workbook with a fast parser and evaluator.

## Installation

```bash
npm install formualizer-wasm
```

## Usage

### JavaScript/TypeScript

```typescript
import init, { tokenize, parse, Workbook } from 'formualizer-wasm';

// Initialize the WASM module once
await init();

// Tokenize a formula
const tokenizer = await tokenize("=SUM(A1:B2)");
console.log(tokenizer.tokens);
console.log(tokenizer.render());

// Parse a formula into an AST
const ast = await parse("=A1+B2*2");
console.log(ast);

// Engine-backed workbook usage
const wb = new Workbook();
wb.addSheet("Data");
wb.setValue("Data", 1, 1, 10);
wb.setValue("Data", 1, 2, 20);
wb.setFormula("Data", 1, 3, "=A1+B1");
console.log(await wb.evaluateCell("Data", 1, 3)); // 30

// Sheet facade
const sheet = wb.sheet("Sheet2");
await sheet.setValue(1, 1, 5);
await sheet.setFormula(1, 2, "=A1*3");
console.log(await sheet.evaluateCell(1, 2)); // 15
```

### Direct WASM Usage

```javascript
import init, { Tokenizer, Parser, Workbook } from 'formualizer-wasm/pkg';

await init();

// Use the WASM classes directly
const tokenizer = new Tokenizer("=SUM(A1:B2)");
const tokens = JSON.parse(tokenizer.tokens());

const parser = new Parser("=A1+B2*2");
const ast = parser.parse();
const astJson = JSON.parse(ast.toJSON());

// Workbook + changelog/undo/redo
const wb = new Workbook();
wb.addSheet("S");
await wb.setChangelogEnabled(true);

await wb.beginAction("seed");
await wb.setValue("S", 1, 1, 10);
await wb.endAction();

await wb.beginAction("edit");
await wb.setValue("S", 1, 1, 20);
await wb.endAction();

await wb.undo(); // value back to 10
await wb.redo(); // value back to 20
```

## API

### `tokenize(formula: string): Promise<Tokenizer>`

Tokenizes an Excel formula string into tokens.

### `parse(formula: string): Promise<ASTNodeData>`

Parses an Excel formula string into an Abstract Syntax Tree.

### `Tokenizer`

- `tokens`: Get all tokens as an array
- `render()`: Reconstruct the original formula from tokens
- `length`: Number of tokens
- `getToken(index)`: Get a specific token by index

### `Parser`

- `parse()`: Parse the formula and return an AST

### `ASTNode`

- `toJSON()`: Convert the AST node to JSON
- `toString()`: Get a string representation
- `getType()`: Get the node type

### `Reference`

Represents a cell or range reference in Excel notation.

### `Workbook`

- `constructor()`
- `addSheet(name: string): void`
- `sheetNames(): string[]`
- `sheet(name: string): Sheet` — idempotently creates and returns a sheet facade
- `setValue(sheet: string, row: number, col: number, value: any): void`
- `setFormula(sheet: string, row: number, col: number, formula: string): void`
- `evaluateCell(sheet: string, row: number, col: number): any`
- `setChangelogEnabled(enabled: boolean): void`
- `beginAction(description: string): void`
- `endAction(): void`
- `undo(): void`
- `redo(): void`

### `Sheet`

- `setValue(row: number, col: number, value: any): void`
- `getValue(row: number, col: number): any`
- `setFormula(row: number, col: number, formula: string): void`
- `getFormula(row: number, col: number): string | undefined`
- `setValues(startRow: number, startCol: number, data: any[][]): void`
- `setFormulas(startRow: number, startCol: number, data: string[][]): void`
- `evaluateCell(row: number, col: number): any`

## Building from Source

```bash
# Install wasm-pack
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh

# Build the WASM module (web target)
wasm-pack build --target web --out-dir pkg --release

# or for bundlers like Node test runner
# wasm-pack build --target bundler --out-dir pkg --release

# Build the JavaScript wrapper
npm run build
```

## Testing

```bash
# Run Rust tests
cargo test -p formualizer-wasm

# Run WASM tests
wasm-pack test --node
```

## License

MIT OR Apache-2.0
