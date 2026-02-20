# Formualizer for Python

**Parse, evaluate, and mutate Excel workbooks at native speed from Python.**

A Rust-powered spreadsheet engine with 320+ Excel-compatible functions, exposed through a clean Pythonic API. Tokenize formulas, walk ASTs, evaluate workbooks, and use SheetPort to treat spreadsheets as typed APIs.

[![PyPI](https://img.shields.io/pypi/v/formualizer.svg)](https://pypi.org/project/formualizer/)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](../../LICENSE-MIT)

## Installation

```bash
pip install formualizer
```

Prebuilt wheels are available for Python 3.10-3.13 on Linux, macOS, and Windows. No Rust toolchain required.

## Quick start

### Evaluate a workbook

```python
import formualizer as fz

wb = fz.Workbook()
s = wb.sheet("Sheet1")

s.set_value(1, 1, fz.LiteralValue.number(1000.0))   # A1: principal
s.set_value(2, 1, fz.LiteralValue.number(0.05))      # A2: annual rate
s.set_value(3, 1, fz.LiteralValue.number(12.0))       # A3: periods

s.set_formula(1, 2, "=PMT(A2/12, A3, -A1)")
print(wb.evaluate_cell("Sheet1", 1, 2))  # ~85.61
```

### Load an XLSX and evaluate

```python
import formualizer as fz

wb = fz.load_workbook("financial_model.xlsx", strategy="eager_all")
print(wb.evaluate_cell("Summary", 1, 2))
```

### Recalculate XLSX cached values (writeback)

```python
import formualizer as fz

# in-place
summary = fz.recalculate_file("financial_model.xlsx")
print(summary["status"], summary["evaluated"], summary["errors"])

# write to a new file
summary = fz.recalculate_file("financial_model.xlsx", output="financial_model.recalc.xlsx")
```

> Current limitation: cached values for formula cells are currently written as
> string-typed payloads by the underlying umya writer. Formula text is preserved.

### Parse and analyze formulas

```python
from formualizer import parse
from formualizer.visitor import collect_references, collect_function_names

ast = parse("=SUMIFS(Revenue,Region,A1,Year,B1)")
print(ast.pretty())                          # indented AST tree
print(ast.to_formula())                      # canonical Excel string
print(collect_references(ast))               # [Revenue, Region, A1, Year, B1]
print(collect_function_names(ast))           # ['SUMIFS']
```

---

## Key features

| Capability | Description |
|---|---|
| **Tokenization** | Break formulas into structured `Token` objects with byte spans and operator metadata |
| **Parsing** | Produce a rich AST with reference normalization, source tracking, and 64-bit structural fingerprints |
| **320+ built-in functions** | Math, text, lookup (XLOOKUP, VLOOKUP), date/time, financial, statistics, database, engineering |
| **Workbook evaluation** | Set values and formulas, evaluate cells/ranges, load XLSX/CSV/JSON |
| **XLSX cache writeback** | `recalculate_file(path, output=None)` recalculates formulas and writes cached values back |
| **Batch operations** | `set_values_batch` / `set_formulas_batch` for efficient bulk updates |
| **Undo / redo** | Optional changelog with automatic action grouping — single edits are individually undoable |
| **Evaluation planning** | Inspect the dependency graph and evaluation schedule before computing |
| **SheetPort** | Treat spreadsheets as typed functions with YAML manifests, schema validation, and batch scenarios |
| **Deterministic mode** | Inject clock, timezone, and RNG seed for reproducible evaluation |
| **Visitor utilities** | `walk_ast`, `collect_references`, `collect_function_names` for ergonomic tree traversal |
| **Rich errors** | Typed `TokenizerError` / `ParserError` / `ExcelEvaluationError` with position info |

---

## Workbook evaluation

```python
import formualizer as fz

wb = fz.Workbook()
s = wb.sheet("Data")

# Set values and formulas
s.set_value(1, 1, fz.LiteralValue.number(100.0))
s.set_value(2, 1, fz.LiteralValue.number(200.0))
s.set_value(3, 1, fz.LiteralValue.number(300.0))
s.set_formula(4, 1, "=SUM(A1:A3)")
s.set_formula(4, 2, "=AVERAGE(A1:A3)")

print(wb.evaluate_cell("Data", 4, 1))  # 600.0
print(wb.evaluate_cell("Data", 4, 2))  # 200.0
```

## Custom functions

Register workbook-local callbacks without forking Formualizer:

```python
import formualizer as fz

wb = fz.Workbook(mode=fz.WorkbookMode.Ephemeral)
wb.add_sheet("Sheet1")

wb.register_function(
    "py_add",
    lambda a, b: a + b,
    min_args=2,
    max_args=2,
)

wb.set_formula("Sheet1", 1, 1, "=PY_ADD(20,22)")
print(wb.evaluate_cell("Sheet1", 1, 1))  # 42
print(wb.list_functions())
wb.unregister_function("py_add")
```

Key semantics:

- Names are case-insensitive and stored canonically (`py_add` -> `PY_ADD`).
- Custom functions are workbook-local and take precedence over global built-ins.
- Built-in override is disabled by default; set `allow_override_builtin=True` to opt in.
- Args are passed by value; range inputs arrive as nested Python lists.
- Return Python primitives, datetime/date/time/timedelta, dict error objects, or nested lists for array spill output.
- Python callback exceptions are sanitized and mapped to `#VALUE!`.

Runnable example: `python bindings/python/examples/custom_function_registration.py`

## Batch operations

```python
# Bulk-set values (auto-grouped as one undo step when changelog is enabled)
s.set_values_batch(1, 1, 3, 2, [
    [fz.LiteralValue.number(10.0), fz.LiteralValue.number(20.0)],
    [fz.LiteralValue.number(30.0), fz.LiteralValue.number(40.0)],
    [fz.LiteralValue.number(50.0), fz.LiteralValue.number(60.0)],
])
```

## Undo / redo

The changelog is opt-in. Once enabled, every edit is tracked:

```python
wb.set_changelog_enabled(True)

s.set_value(1, 1, fz.LiteralValue.number(10.0))
s.set_value(1, 1, fz.LiteralValue.number(20.0))
wb.undo()  # back to 10
wb.redo()  # back to 20

# Batch methods are auto-grouped as one undo step.
# For manual grouping of multiple calls:
wb.begin_action("update prices")
s.set_value(1, 1, fz.LiteralValue.number(100.0))
s.set_value(2, 1, fz.LiteralValue.number(200.0))
wb.end_action()
wb.undo()  # reverts both values at once
```

## Evaluation planning

Inspect what the engine will compute before running:

```python
plan = wb.get_eval_plan([("Sheet1", 1, 2)])
print(f"Vertices to evaluate: {plan.total_vertices_to_evaluate}")
print(f"Parallel layers: {plan.estimated_parallel_layers}")
for layer in plan.layers:
    print(f"  Layer: {layer.vertex_count} vertices, parallel={layer.parallel_eligible}")
```

## SheetPort: spreadsheets as typed APIs

Define a YAML manifest to treat a spreadsheet as a typed function with validated inputs/outputs:

```python
from formualizer import SheetPortSession, Workbook

manifest_yaml = """
spec: fio
spec_version: "0.3.0"
manifest:
  id: pricing-model
  name: Pricing Model
  workbook:
    uri: memory://pricing.xlsx
    locale: en-US
    date_system: 1900
ports:
  - id: base_price
    dir: in
    shape: scalar
    location: { a1: Inputs!A1 }
    schema: { type: number }
  - id: final_price
    dir: out
    shape: scalar
    location: { a1: Outputs!A1 }
    schema: { type: number }
"""

wb = Workbook()
wb.add_sheet("Inputs")
wb.add_sheet("Outputs")
wb.set_formula("Outputs", 1, 1, "=Inputs!A1*1.2")

session = SheetPortSession.from_manifest_yaml(manifest_yaml, wb)
session.write_inputs({"base_price": 100.0})
result = session.evaluate_once(freeze_volatile=True)
print(result["final_price"])  # 120.0
```

---

## API reference

### Top-level functions

```python
tokenize(formula: str, dialect: FormulaDialect = None) -> Tokenizer
parse(formula: str, dialect: FormulaDialect = None) -> ASTNode
load_workbook(path: str, strategy: str = None) -> Workbook
recalculate_file(path: str, output: str | None = None) -> dict
```

### Core classes

- **`Workbook`** — create, load, evaluate, undo/redo. Supports `from_path()` and `load_path()` class methods.
- **`Sheet`** — per-sheet facade for `set_value`, `set_formula`, `get_cell`, batch operations.
- **`LiteralValue`** — typed values: `.int()`, `.number()`, `.text()`, `.boolean()`, `.date()`, `.empty()`, `.error()`, `.array()`.
- **`Tokenizer`** — iterable token sequence with `.render()` and `.tokens`.
- **`ASTNode`** — `.pretty()`, `.to_formula()`, `.fingerprint()`, `.children()`, `.walk_refs()`.
- **`CellRef` / `RangeRef` / `TableRef` / `NamedRangeRef`** — typed references.
- **`SheetPortSession`** — bind manifests to workbooks, read/write typed ports, evaluate.
- **`EvaluationConfig`** — tune parallel evaluation, warmup, range limits, date systems.

### Visitor helpers (`formualizer.visitor`)

```python
walk_ast(node, visitor_fn)              # DFS with VisitControl (CONTINUE/SKIP/STOP)
collect_references(node)                # -> list[ReferenceLike]
collect_function_names(node)            # -> list[str]
collect_nodes_by_type(node, "Function") # -> list[ASTNode]
```

Full type stubs are included in the package (`.pyi` files) for IDE autocompletion and mypy.

---

## Building from source

Requires Rust >= 1.70 and [maturin](https://github.com/PyO3/maturin):

```bash
pip install maturin
cd bindings/python
maturin develop          # debug build
maturin develop --release  # optimized build
```

## Testing

```bash
pip install formualizer[dev]
pytest bindings/python/tests
ruff check bindings/python
mypy bindings/python/formualizer
```

## Workspace layout

```
formualizer/
  crates/                    # Rust core (parse, eval, workbook, sheetport)
  bindings/python/
    formualizer/             # Python package (helpers, visitor, type stubs)
    src/                     # PyO3 bridge (Rust -> Python)
```

The Python wheel links directly against the Rust crates — there is no runtime FFI overhead beyond the initial C-to-Rust boundary.

## License

Dual-licensed under [MIT](../../LICENSE-MIT) or [Apache-2.0](../../LICENSE-APACHE), at your option.
