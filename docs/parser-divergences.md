# Parser consolidation

Tracking issue: [PSU3D0/formualizer#77](https://github.com/PSU3D0/formualizer/issues/77).

`formualizer-parse` now has one canonical parser implementation: the
source-span-backed public `parser::Parser`.

The old token-vector parser (`Parser::new(tokens, include_whitespace)`) was
removed while the crate is still early. This eliminates the previous classic vs
span divergence class and ensures fixes only need to land in one parser.

## Supported public entrypoints

- `parse(formula)` for ergonomic one-shot parsing.
- `parse_with_dialect(formula, dialect)` for explicit dialect selection.
- `parse_with_volatility_classifier(...)` and
  `parse_with_dialect_and_volatility_classifier(...)` for volatility marking.
- `Parser::new(formula)` / `Parser::new_with_dialect(formula, dialect)` for a
  stateful parser value.
- `Parser::builder()` for configured parser construction.
- `Parser::from_token_stream(&TokenStream)` for callers that already tokenized
  into the source-backed span stream.
- `BatchParser` for repeated parsing with cached token spans.
- Rust idioms: `Parser::try_from(...)`, `ASTNode::try_from(...)`, and
  `"=A1+B1".parse::<ASTNode>()`.

The runnable compatibility contract lives in
[`crates/formualizer-parse/tests/parser_differential.rs`](../crates/formualizer-parse/tests/parser_differential.rs),
which asserts that all supported public entrypoints agree structurally across a
representative formula corpus.

## Breaking API note

Callers that constructed parser input from owned `Vec<Token>` should migrate to
one of:

```rust
let ast = formualizer_parse::parse("=SUM(A1:A10)")?;

let mut parser = formualizer_parse::Parser::new("=SUM(A1:A10)")?;
let ast = parser.parse()?;

let stream = formualizer_parse::TokenStream::new("=SUM(A1:A10)")?;
let mut parser = formualizer_parse::Parser::from_token_stream(&stream);
let ast = parser.parse()?;
```

The tokenizer still exposes owned `Token`s for FFI/debugging, but parsing is now
source-backed so AST source locations remain tied to the original formula text.
