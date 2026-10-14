![Formualizer banner](https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-banner.png)

# formualizer-sheetport

![Arrow Powered](https://img.shields.io/badge/Arrow-Powered-0A66C2?logo=apache&logoColor=white)

**SheetPort runtime — treat any spreadsheet as a typed, deterministic function.**

`formualizer-sheetport` binds SheetPort manifests (built in code or loaded from YAML) to workbooks, resolves selectors, enforces schemas, and provides deterministic read/write/evaluate primitives. A workbook plus manifest behaves like a pure function: write typed inputs, recalculate, read typed outputs.

## When to use this crate

Use `formualizer-sheetport` when you want to:
- Expose a spreadsheet as a typed API with schema validation
- Run batch scenarios through a financial model
- Give AI agents safe, typed access to spreadsheet logic
- Enforce constraints and defaults on spreadsheet inputs/outputs

## Example

Through the [`formualizer`](https://crates.io/crates/formualizer) crate, which re-exports this crate as `formualizer::sheetport` and the manifest types as `formualizer::sheetport_spec`:

<!-- example: crates/formualizer/examples/sheetport_quickstart.rs#readme -->
```rust
use formualizer::sheetport::{EvalOptions, InputUpdate, PortValue, SheetPortSession};
use formualizer::sheetport_spec::{Constraints, Manifest, Port, ValueType};
use formualizer::{LiteralValue, Workbook};

// The model: B4 computes the monthly payment from B1:B3.
let mut wb = Workbook::new(); // or load one from an .xlsx
wb.add_sheet("Calculator")?;
wb.set_formula("Calculator", 4, 2, "=PMT(B2/12, B3, -B1)")?;

let manifest = Manifest::new("loan-calculator")
    .with_name("Loan calculator")
    .port(
        Port::input("principal", "Calculator!B1", ValueType::Number).with_constraints(
            Constraints {
                min: Some(0.0),
                ..Default::default()
            },
        ),
    )
    .port(Port::input("rate", "Calculator!B2", ValueType::Number))
    .port(Port::input(
        "term_months",
        "Calculator!B3",
        ValueType::Integer,
    ))
    .port(Port::output(
        "monthly_payment",
        "Calculator!B4",
        ValueType::Number,
    ));

let mut session = SheetPortSession::new(wb, manifest)?;
session.write_inputs(InputUpdate::from_iter([
    ("principal", LiteralValue::Number(250_000.0)),
    ("rate", LiteralValue::Number(0.045)),
    ("term_months", LiteralValue::Int(360)),
]))?;
let outputs = session.evaluate_once(EvalOptions {
    freeze_volatile: true,
    ..Default::default()
})?;
let payment = outputs
    .get("monthly_payment")
    .and_then(PortValue::as_scalar);
println!("{payment:?}"); // Some(Number(1266.71...))
```

`Port::input` and `Port::output` take an id, a location (a string is an A1 reference; `Selector::name` and `Selector::layout` select a defined name or the rows under a header) and a schema (a `ValueType` for a single cell, or `Schema::range`, `Schema::record` and `Schema::table`). The same manifest can be loaded from YAML with `Manifest::from_yaml_str` and written with `Manifest::to_yaml`.

## How it works

1. **Define a manifest**, in code or as YAML, that declares typed input and output ports with locations and schemas.
2. **Bind** the manifest to a workbook — selectors (`a1`, named ranges, header-based layouts) are resolved to workbook coordinates.
3. **Write inputs** — values are validated against the manifest schema, with defaults and constraints enforced.
4. **Evaluate** — the workbook is recalculated deterministically.
5. **Read outputs** — results are extracted and coerced to the declared output types.

## Features

- **Manifest conformance** — enforces `core-v0` profile with selector validation.
- **Schema enforcement** — type coercion, defaults, and constraint checking with detailed violation paths.
- **Deterministic evaluation** — freeze volatile functions, inject timestamps, seed RNG.
- **Batch execution** — `BatchExecutor` fans scenarios across a shared workbook with baseline reset between runs.
- **Selector resolution** — `a1` references, named ranges, and header-based table layouts.

## Documentation

- [What Is SheetPort?](https://www.formualizer.dev/docs/sheetport/what-is-sheetport)
- [Manifest Format (FIO Spec)](https://www.formualizer.dev/docs/sheetport/manifest-format)
- [SheetPort Quickstart](https://www.formualizer.dev/docs/sheetport/quickstart)
- [Sessions and Evaluation](https://www.formualizer.dev/docs/sheetport/sessions-and-evaluation)

## License

Dual-licensed under MIT or Apache-2.0, at your option.
