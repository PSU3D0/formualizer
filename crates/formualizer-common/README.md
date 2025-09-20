# formualizer-common

`formualizer-common` hosts the shared data structures that keep the Formualizer
spreadsheet engine cohesive across crates and language bindings. Types in this
crate represent literal values, ranges, function signatures, and diagnostic
errors that travel between the parser, evaluator, workbook, and external
surfaces.

> **Note:** This crate exists primarily to satisfy compile-time sharing across
> the Formualizer workspace. It is published so downstream crates resolve, but
> its APIs are considered internal and may change between releases without
> notice. Prefer the higher-level `formualizer-*` crates unless you are working
> within the Formualizer project itself.

## Highlights

- **Canonical value model** – `LiteralValue` and related helpers cover Excel
  scalars, arrays, dates/times, and error variants with round-trippable
  conversions.
- **Range plumbing** – strongly-typed range and reference utilities used by the
  parser and evaluator to agree on addresses.
- **Function metadata** – descriptors and enums that describe built-in
  functions, their argument shapes, and capability flags.
- **Error surface** – consistent `ExcelError` and evaluation diagnostics shared
  across Rust, Python, and WASM bindings.

## When to use it

Most users interact with higher-level crates such as `formualizer-parse`,
`formualizer-eval`, or `formualizer-workbook`. Depend on `formualizer-common`
directly if you need the shared types in a standalone integration or when
building a new Formualizer dialect/binding.

```rust
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};

fn normalize(value: LiteralValue) -> LiteralValue {
    match value {
        LiteralValue::Text(text) if text.is_empty() => LiteralValue::Empty,
        LiteralValue::Boolean(_) | LiteralValue::Number(_) => value,
        LiteralValue::Error(_) => value,
        other => other.coerce_to_single_value().unwrap_or_else(|_| {
            LiteralValue::Error(ExcelError::from(ExcelErrorKind::Value))
        })
    }
}
```

## License

Dual-licensed under MIT or Apache-2.0, at your option.

See the main project README for contributor guidelines and release steps.
