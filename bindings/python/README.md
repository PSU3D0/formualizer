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

# For local development (tests + lint/typecheck)
pip install formualizer[dev]
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

### Changelog, Undo, and Redo

Formualizer’s engine tracks edits and can undo/redo changes. You do not need to manually group edits for everyday use:

- Single‑cell edits (e.g., `Workbook.set_value`, `Workbook.set_formula`) are individually undoable when changelog is enabled.
- Batch operations (`Workbook.set_values_batch`, `Workbook.set_formulas_batch`) are automatically wrapped into a single undoable action for you.

Power users can group multiple calls into one undo step using `begin_action(...)` / `end_action()` — this is optional and not required for typical workflows.

```python
wb.set_changelog_enabled(True)

# Each set_value is its own undo step
wb.set_value("S", 1, 1, fz.LiteralValue.int(10))
wb.set_value("S", 1, 1, fz.LiteralValue.int(20))
wb.undo()  # back to 10

# Batch is auto‑grouped as one action
wb.set_values_batch("S", 1, 1, [[fz.LiteralValue.int(1), fz.LiteralValue.int(2)]])
wb.undo()  # reverts the entire batch
```

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

### Dependency Tracing (`formualizer.dependency_tracer`)

This module is not part of the current Python package.

If you need dependency information today, use the workbook engine itself (incremental recalculation + demand-driven evaluation) and treat the sheet as the source of truth. A higher-level dependency analysis API may be added in the future.

### Workbook Evaluation

```python
import formualizer as fz

wb = fz.Workbook()
s = wb.sheet("Sheet1")

s.set_value(1, 1, 10)
s.set_value(2, 1, 20)
s.set_formula(1, 2, "=A1+A2")

assert wb.evaluate_cell("Sheet1", 1, 2) == 30.0
```

---

## Workspace Layout

```
formualizer/
│
├─ crates/               # Pure‑Rust core, common types, evaluator, macros
│   ├─ formualizer-common    (shared types: values/errors/addresses)
│   ├─ formualizer-parse      (tokenizer + parser + pretty)
│   ├─ formualizer-eval      (calc engine + built‑ins)
│   ├─ formualizer-workbook  (workbook facade + I/O backends)
│   ├─ formualizer-sheetport (SheetPort runtime)
│   └─ formualizer-macros    (proc‑macro helpers)
│
└─ bindings/python/      # This package (native module + Python helpers)
    ├─ formualizer/          # Python package (helpers + re-exports)
    │   ├─ __init__.py
    │   ├─ visitor.py
    │   └─ _types.py
    └─ src/                  # Rust‑Python bridge (pyo3)
```

The Python wheel links directly against the crates — there is **no runtime FFI overhead** beyond the initial C→Rust boundary.

---

## Examples & Practical Usage

### Load an XLSX and evaluate

```python
import formualizer as fz

wb = fz.load_workbook("model.xlsx", strategy="eager_all")
print(wb.evaluate_cell("Sheet1", 1, 2))
```

### SheetPort: spreadsheets as typed functions

```python
import textwrap

from formualizer import SheetPortSession, Workbook

manifest_yaml = textwrap.dedent(
    """
    spec: fio
    spec_version: "0.3.0"
    manifest:
      id: demo
      name: Demo
      workbook:
        uri: memory://demo.xlsx
        locale: en-US
        date_system: 1900
    ports:
      - id: demand
        dir: in
        shape: scalar
        location: { a1: Inputs!A1 }
        schema: { type: number }
      - id: out
        dir: out
        shape: scalar
        location: { a1: Outputs!A1 }
        schema: { type: number }
    """
)

wb = Workbook()
wb.add_sheet("Inputs")
wb.add_sheet("Outputs")
wb.set_value("Inputs", 1, 1, 120)

session = SheetPortSession.from_manifest_yaml(manifest_yaml, wb)
session.write_inputs({"demand": 250.5})
print(session.evaluate_once())
```

---

## Development & Testing

```bash
# run Rust tests
cargo test --workspace

# run Python tests
pytest -q bindings/python/tests

# lint / typecheck
ruff check bindings/python
mypy bindings/python/formualizer
```

When hacking on the Rust side, you can rebuild the extension in place:

```bash
maturin develop --release  # faster extension; omit --release for debug builds
```

---

## Roadmap

* Full coverage of Excel 365 functions via `formualizer‑eval`
* SIMD‑accelerated bulk range operations  
* Enhanced dependency visualization and interactive formula exploration
* ChatGPT‑powered formula explanations with dependency context 🎯
* Integration with pandas DataFrames and other Python data analysis tools

Have an idea or found a bug? Open an issue or PR — contributions are welcome!

---

## License

Dual‑licensed under **MIT** or **Apache‑2.0** — choose whichever you prefer.
