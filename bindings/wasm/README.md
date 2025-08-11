# Formualizer WASM Bindings

WebAssembly bindings for the Formualizer Excel formula parser.

## Installation

```bash
npm install formualizer-wasm
```

## Usage

### JavaScript/TypeScript

```typescript
import init, { tokenize, parse } from 'formualizer-wasm';

// Initialize the WASM module once
await init();

// Tokenize a formula
const tokenizer = await tokenize("=SUM(A1:B2)");
console.log(tokenizer.tokens);
console.log(tokenizer.render());

// Parse a formula into an AST
const ast = await parse("=A1+B2*2");
console.log(ast);
```

### Direct WASM Usage

```javascript
import init, { Tokenizer, Parser } from 'formualizer-wasm/pkg';

await init();

// Use the WASM classes directly
const tokenizer = new Tokenizer("=SUM(A1:B2)");
const tokens = JSON.parse(tokenizer.tokens());

const parser = new Parser("=A1+B2*2");
const ast = parser.parse();
const astJson = JSON.parse(ast.toJSON());
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

## Building from Source

```bash
# Install wasm-pack
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh

# Build the WASM module
wasm-pack build --target web --out-dir pkg --release

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