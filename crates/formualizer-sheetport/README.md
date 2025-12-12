# Formualizer SheetPort Runtime

`formualizer-sheetport` binds SheetPort manifests to concrete workbooks. It
resolves selectors, enforces schemas, and provides deterministic read/write
primitives so a workbook plus manifest behaves like a pure function.

## Scope

- Enforce manifest conformance profile (`core-v0` only; `full-v0` rejected).
- Resolve supported selectors under `core-v0` (`a1`, named ranges, header-based
  layouts) into workbook coordinates.
- Coerce values into typed inputs/outputs in accordance with manifest schemas,
  including default application and constraint checking. Violations surface as
  `SheetPortError::ConstraintViolation` with detailed paths so callers can
  highlight the offending cells or fields.
- Provide ergonomic APIs for loading manifests, reading inputs/outputs, writing
  inputs, and triggering evaluation through the existing Formualizer engine.
- Batch execution helpers (`BatchExecutor`) fan scenarios across a shared
  workbook while reapplying the baseline manifest between runs.

## Non-goals (for now)

- Delta/patch authoring or merge orchestration.
- Heuristic sheet analysis, agent runtimes, or policy enforcement layers.

These capabilities will layer on top of the runtime crate once the core I/O
surface is stable.
