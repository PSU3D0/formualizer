# Formualizer — Python Bindings

A blazing‑fast Excel formula **tokenizer, parser, and evaluator** powered by Rust, exposed through a clean, Pythonic API.
These bindings wrap the core `formualizer‑core` and `formualizer‑eval` crates and let you work with spreadsheet logic at native speed while writing idiomatic Python.

---

## Key Features

| Capability              | Description                                                                                                                        |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| **Tokenization**        | Breaks a formula string into structured `Token` objects, preserving exact byte spans and operator metadata.                        |
| **Parsing → AST**       | Produces a rich **Abstract Syntax Tree** (`ASTNode`) that normalises references, tracks source tokens, and fingerprints structure. |
| **Reference Model**     | First‑class `CellRef`, `RangeRef`, `TableRef`, `NamedRangeRef` objects with helpers like `.normalise()` / `.to_excel()`.           |
| **Pretty‑printing**     | Canonical formatter — returns Excel‑style string with consistent casing, spacing, and minimal parentheses.                         |
| **Visitor utilities**   | `walk_ast`, `collect_references`, `collect_function_names`, and more for ergonomic tree traversal.                                 |
| **Evaluation (opt‑in)** | Bring in `formualizer‑eval` to execute the AST with a pluggable workbook/resolver interface.                                       |
| **Rich Errors**         | Typed `TokenizerError` / `ParserError` that annotate byte positions for precise diagnostics.                                       |

---

## Installation

### Pre‑built wheels (recommended)

```bash
pip install formualizer
```

### Build from source

You need a recent Rust toolchain (≥ 1.70) and **maturin**:

```bash
# one‑off – install maturin
pip install maturin

# from repo root
cd bindings/python
maturin develop  # builds the native extension and installs an editable package
```

This compiles the Rust crates (`formualizer‑*`) into a CPython extension named `formualizer`.

---

## Quick‑start

```python
from formualizer import tokenize, parse
from formualizer.visitor import collect_references

formula = "=SUM(A1:B2) + 3%"

# 1️⃣ Tokenize
for tok in tokenize(formula):
    print(tok)

# 2️⃣ Parse → AST
ast = parse(formula)
print(ast.pretty())           # indented tree
print(ast.to_formula())       # canonical Excel string
print(ast.fingerprint())      # 64‑bit structural hash

# 3️⃣ Analyse
refs = collect_references(ast)
print([r.to_excel() for r in refs])  # ['A1:B2']
```

> **Tip:** You can build your own visitor by returning `VisitControl.SKIP` or `STOP` to short‑circuit traversal.

---

## Public API Surface

### Convenience helpers

```python
tokenize(formula: str) -> Tokenizer
parse(formula: str, include_whitespace: bool = False) -> ASTNode
```

### Core classes (excerpt)

* **`Tokenizer`** — iterable collection of `Token`; `.render()` reconstructs the original string.
* **`Token`** — `.value`, `.token_type`, `.subtype`, `.start`, `.end`, `.is_operator()`.
* **`Parser`** — OO interface when you need to parse the same `Tokenizer` twice.
* **`ASTNode`** — `.pretty()`, `.to_formula()`, `.children()`, `.walk_refs()`…
* **Reference types** — `CellRef`, `RangeRef`, `TableRef`, `NamedRangeRef`, `UnknownRef`.
* **Errors** — `TokenizerError`, `ParserError` (carry `.message` and `.position`).

### Visitor helpers (`formualizer.visitor`)

* `walk_ast(node, fn)` — DFS with early‑exit control.
* `collect_nodes_by_type(node, "Function")` → list\[ASTNode]
* `collect_references(node)` → list\[ReferenceLike]
* `collect_function_names(node)` → list\[str]

---

## Workspace Layout

```
formualizer/
│
├─ crates/               # Pure‑Rust core, common types, evaluator, macros
│   ├─ formualizer-core      (tokenizer + parser + pretty)
│   ├─ formualizer-eval      (optional interpreter + built‑ins)
│   ├─ formualizer-common    (shared literal / error / arg specs)
│   └─ formualizer-macros    (proc‑macro helpers)
│
└─ bindings/python/      # This package (native module + Python helpers)
```

The Python wheel links directly against the crates — there is **no runtime FFI overhead** beyond the initial C→Rust boundary.

---

## Development & Testing

```bash
# run Rust tests
cargo test --workspace

# TODO: add pytest once Python‑side tests exist
```

When hacking on the Rust side, you can rebuild the extension in place:

```bash
maturin develop --release  # faster extension; omit --release for debug builds
```

---

## Roadmap

* Full coverage of Excel 365 functions via `formualizer‑eval`
* SIMD‑accelerated bulk range operations
* ChatGPT‑powered formula explanations 🎯

Have an idea or found a bug? Open an issue or PR — contributions are welcome!

---

## License

Dual‑licensed under **MIT** or **Apache‑2.0** — choose whichever you prefer.
