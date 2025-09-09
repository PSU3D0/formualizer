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
| **Dependency Tracing**  | Comprehensive dependency analysis with precedent/dependent tracing, cycle detection, and intelligent caching.                      |
| **Rich Errors**         | Typed `TokenizerError` / `ParserError` that annotate byte positions for precise diagnostics.                                       |

---

## Installation

### Pre‑built wheels (recommended)

```bash
pip install formualizer

# For Excel file support (OpenpyxlResolver)
pip install formualizer[excel]  # includes openpyxl

# For all optional dependencies
pip install formualizer[all]    # includes openpyxl, fastexcel
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

The dependency tracer provides a robust, resolver-agnostic system for analyzing formula dependencies with intelligent caching and cycle detection.

#### Key Components

* **`DependencyTracer`** — Main engine for tracing precedents/dependents with caching and cycle detection.
* **`FormulaResolver` (ABC)** — Abstract interface for data source integration (JSON, Excel, custom).
* **`DependencyNode`** — Unified node representing dependency relationships with directionality.
* **`TraceResult`** — Container for results with filtering and traversal utilities.
* **`RangeContainer`** — Smart consolidation and classification of range references.
* **`LabelProjector`** — Context label discovery for enhanced formula interpretation.

#### Quick Example

```python
from formualizer.dependency_tracer import DependencyTracer
from formualizer.dependency_tracer.resolvers import JsonResolver

# Set up your data source (JSON, openpyxl, or custom)
resolver = JsonResolver(workbook_data)
tracer = DependencyTracer(resolver)

# Trace what a formula depends on (precedents)
precedents = tracer.trace_precedents("Summary!B4", recursive=True)
print(f"Found {len(precedents)} precedents")

# Trace what depends on a cell (dependents)  
dependents = tracer.trace_dependents("Inputs!B2", recursive=True)
print(f"Found {len(dependents)} dependents")

# Find circular dependencies
cycles = tracer.find_circular_dependencies()
if cycles:
    print(f"Warning: {len(cycles)} circular reference(s) detected")

# Get evaluation order
try:
    eval_order = tracer.topological_sort()
    print("Evaluation order:", [str(cell) for cell in eval_order])
except ValueError:
    print("Cannot sort: circular dependencies exist")
```

#### Built-in Resolvers

* **`JsonResolver`** — Load from JSON files or dictionaries with Excel-style data structure.
* **`DictResolver`** — Simple nested dictionary resolver for testing and prototyping.
* **`OpenpyxlResolver`** — Direct integration with openpyxl workbooks (requires `pip install openpyxl`).
* **`CombinedResolver`** — Chain multiple resolvers with priority fallback for data overlays.

#### Advanced Features

* **Intelligent Caching** — Automatic formula parsing and reference resolution caching with selective invalidation.
* **Range Classification** — Automatic categorization of ranges as data ranges, lookup columns, or selection ranges.
* **Label Discovery** — Find contextual text labels near cells for enhanced formula interpretation.
* **Performance Monitoring** — Built-in cache statistics and performance tracking.
* **Cycle Detection** — Robust circular dependency detection with detailed cycle reporting.

#### Example Workflows

```python
# Performance analysis with caching
tracer = DependencyTracer(resolver, enable_caching=True)
stats = tracer.get_stats()
print(f"Cache hit ratio: {stats}")

# Range analysis and consolidation
precedents = tracer.trace_precedents("Summary!Total")
range_container = precedents.filter_ranges_only().create_range_container()
data_ranges = range_container.get_data_ranges()
lookup_columns = range_container.get_column_ranges()

# Context-aware formula analysis
from formualizer.dependency_tracer import LabelProjector
projector = LabelProjector(resolver)
labels = projector.find_labels_for_cell(CellRef("Sheet1", 5, "B"))
print(f"Context for B5: {[label.text for label in labels]}")
```

---

## Workspace Layout

```
formualizer/
│
├─ crates/               # Pure‑Rust core, common types, evaluator, macros
│   ├─ formualizer-parse      (tokenizer + parser + pretty)
│   ├─ formualizer-eval      (optional interpreter + built‑ins)
│   ├─ formualizer-common    (shared literal / error / arg specs)
│   └─ formualizer-macros    (proc‑macro helpers)
│
└─ bindings/python/      # This package (native module + Python helpers)
    ├─ formualizer/
    │   ├─ dependency_tracer/    # Dependency analysis system
    │   │   ├─ dependency_tracer.py  (main engine + data classes)
    │   │   ├─ resolvers.py          (data source integrations)
    │   │   ├─ examples.py           (practical demonstrations)
    │   │   └─ test_dependency_tracer.py  (test suite)
    │   └─ visitor.py            # AST traversal utilities
    └─ src/                  # Rust‑Python bridge
```

The Python wheel links directly against the crates — there is **no runtime FFI overhead** beyond the initial C→Rust boundary.

---

## Examples & Practical Usage

The `formualizer.dependency_tracer.examples` module provides comprehensive demonstrations:

```python
# Run all examples to see the system in action
from formualizer.dependency_tracer.examples import run_all_examples
run_all_examples()

# Or run individual examples
from formualizer.dependency_tracer.examples import (
    example_1_simple_json_tracing,      # Basic JSON dependency analysis
    example_2_openpyxl_integration,     # Real Excel file processing
    example_3_combined_resolvers,       # Multi-source data overlays
    example_4_cycle_detection,          # Circular dependency handling
    example_5_performance_and_caching,  # Performance optimization
)
```

### Real-World Use Cases

* **Financial Modeling** — Trace how changes to assumptions ripple through complex financial models
* **Data Pipeline Analysis** — Understand dependencies between calculated fields in data workflows  
* **Spreadsheet Auditing** — Identify circular references and optimize calculation order
* **Formula Documentation** — Auto-generate dependency maps and impact analysis reports
* **Migration Planning** — Analyze formula complexity before system migrations

---

## Development & Testing

```bash
# run Rust tests
cargo test --workspace

# run Python dependency tracer tests
python -m formualizer.dependency_tracer.test_dependency_tracer

# run the examples (also serves as integration tests)
python -m formualizer.dependency_tracer.examples
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
