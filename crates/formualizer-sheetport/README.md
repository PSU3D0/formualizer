![Formualizer banner](https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-banner.png)

# formualizer-sheetport

![Arrow Powered](https://img.shields.io/badge/Arrow-Powered-0A66C2?logo=apache&logoColor=white)

**SheetPort runtime — treat any spreadsheet as a typed, deterministic function.**

`formualizer-sheetport` binds SheetPort YAML manifests to workbooks, resolves selectors, enforces schemas, and provides deterministic read/write/evaluate primitives. A workbook plus manifest behaves like a pure function: write typed inputs, recalculate, read typed outputs.

## When to use this crate

Use `formualizer-sheetport` when you want to:
- Expose a spreadsheet as a typed API with schema validation
- Run batch scenarios through a financial model
- Give AI agents safe, typed access to spreadsheet logic
- Enforce constraints and defaults on spreadsheet inputs/outputs

## How it works

1. **Define a manifest** (YAML) that declares typed input and output ports with locations and schemas.
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

## License

Dual-licensed under MIT or Apache-2.0, at your option.
