![Formualizer banner](https://raw.githubusercontent.com/psu3d0/formualizer/main/assets/formualizer-banner.png)

# formualizer-parse

![Arrow Powered](https://img.shields.io/badge/Arrow-Powered-0A66C2?logo=apache&logoColor=white)

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
use formualizer_parse::{FormulaDialect, Parser, canonical_formula, parse_with_dialect};

// One-shot parse
let ast = parse_with_dialect("=SUM(A1:B3)", FormulaDialect::Excel)?;

// Or use the stateful source-span parser directly
let mut parser = Parser::new("=SUM(A1:B3)")?;
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

## Resource limits

Default parsing admits at most 64 KiB of UTF-8 source bytes, 16,384 tokens, 8,192 AST nodes (including omitted arguments), 72 active Pratt frames and AST height 256. Height counts the root as one; parentheses do not add AST nodes. Limits apply independently, so a formula can hit one before another. Errors retain the existing parser/tokenizer error types.

Start from `ParserLimits::default()` and adjust budgets with the `with_source_bytes`, `with_tokens`, `with_ast_nodes`, `with_pratt_frames` and `with_ast_height` setters, then pass the value to `Parser::builder().limits(limits)` (or `BatchParser::builder().limits(limits)`). Values are used exactly as given; nothing is clamped. Raising `pratt_frames` or `ast_height` above the defaults requires a correspondingly larger stack on every thread that parses, clones, drops or evaluates the tree. These guarantees concern parser-produced trees, not manually constructed or externally deserialized ASTs.

```rust
use formualizer_parse::{Parser, ParserLimits};

let limits = ParserLimits::default().with_ast_nodes(1_024);
let ast = Parser::builder().limits(limits).parse("=SUM(A1:B3)").unwrap();
```

`parser::BatchParser` retains lexical results in a FIFO cache bounded by 16,384 entries and 8 MiB of source/token payload (excluding container overhead). Hits do not reorder entries. `cache_capacity(entries, bytes)` configures retention; zero entries disables it. Entries exceeding the byte capacity still parse without retention. Eviction does not change formula semantics, but a working set larger than either bound may lose token-cache reuse. Tune the capacity for such workloads.

Externally supplied `TokenStream.spans` must be ordered, disjoint and valid UTF-8 byte ranges. Stream-to-parser and stream-to-owned-tokenizer admission validates these before copying. Infallible best-effort stream tokenization reports resource failures through its diagnostics; `Tokenizer::new_best_effort` and `Tokenizer::from_token_stream` expose them through `admission_error()`, with empty output on admission failure.

These are resource policies, not exact Excel compatibility limits: a valid Excel formula with more than 256 operands in a left-associated chain can exceed the AST-height ceiling even if its text fits Excel's character limit.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
