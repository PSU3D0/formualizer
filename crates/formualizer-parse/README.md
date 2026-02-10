# formualizer-parse

**High-performance Excel and OpenFormula tokenizer, parser, and pretty-printer.**

`formualizer-parse` turns raw formula strings into a structured AST that downstream crates use for evaluation, analysis, and transformation. It handles both Excel and OpenFormula dialects with source location tracking.

## When to use this crate

Use `formualizer-parse` when you need formula analysis **without** evaluation:
- Formula linting and validation
- Static analysis of cell dependencies
- AST transformation and rewriting
- Pretty-printing formulas to canonical form
- Building custom formula tooling

If you also need evaluation, use [`formualizer-workbook`](https://crates.io/crates/formualizer-workbook) or [`formualizer-eval`](https://crates.io/crates/formualizer-eval) instead.

## Quick start

```rust
use formualizer_parse::{FormulaDialect, Tokenizer, canonical_formula};
use formualizer_parse::parser::Parser;

// Tokenize and parse
let tokenizer = Tokenizer::new_with_dialect("=SUM(A1:B3)", FormulaDialect::Excel)?;
let mut parser = Parser::new(tokenizer.items, false);
let ast = parser.parse()?;

// Canonical form
assert_eq!(canonical_formula(&ast), "=SUM(A1:B3)");
```

## Features

- **Tokenization** — streaming tokenizer with dialect-aware classification, source location tracking, and operator metadata.
- **Pratt parser** — precedence-climbing parser producing a stable AST with reference normalization.
- **Dialects** — Excel (default) and OpenFormula syntax support through a single API.
- **Pretty-printing** — canonicalize formulas or render diagnostic trees for debugging.
- **Source spans** — every token and AST node carries byte positions for precise error reporting.
- **Fingerprinting** — 64-bit structural hashes for formula identity comparison.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
