# Parser/SDK 3.0 migration

`formualizer-common` and `formualizer-parse` move together from 2.0.0 to 3.0.0. Update both requirements in the same change:

```toml
[dependencies]
formualizer-common = "3.0.0"
formualizer-parse = "3.0.0"
```

The major version records the parser constructor changes already present on `main` and makes reported error/outcome vocabularies extensible before the 3.0 API is released.

## Construct a parser from source

The 2.0 parser accepted owned classic-token vectors and an `include_whitespace` flag. The 3.0 parser owns its formula source and source spans instead:

```rust
// 2.0
let tokens = Tokenizer::new("=SUM(A1:A3)")?.items;
let mut parser = Parser::new(tokens, false);

// 3.0
let mut parser = Parser::new("=SUM(A1:A3)")?;
let ast = parser.parse()?;
```

`Parser::new_with_dialect(tokens, include_whitespace, dialect)` becomes `Parser::new_with_dialect(formula, dialect)`. Whitespace remains represented by source spans; there is no parser flag that discards or includes classic whitespace tokens.

When tokenization and parsing are separate steps, retain the source-backed stream:

```rust
use formualizer_parse::{Parser, TokenStream};

let stream = TokenStream::new("=SUM(A1:A3)")?;
let mut parser = Parser::from_token_stream(&stream);
let ast = parser.parse()?;
```

There is intentionally no constructor that rebuilds a parser from `Vec<Token>` alone: owned classic tokens do not retain the canonical source needed by the source-span parser.

## Migrate classifier constructors

Replace the removed token-vector classifier constructors with parser configuration or a builder:

```rust
// Replaces Parser::new_with_classifier(tokens, include_whitespace, classifier)
let mut parser = Parser::new(formula)?.with_volatility_classifier(classifier);

// Replaces Parser::new_with_classifier_and_dialect(
//     tokens, include_whitespace, dialect, classifier,
// )
let mut parser = ParserBuilder::default()
    .dialect(dialect)
    .with_volatility_classifier(classifier)
    .build(formula)?;
```

For one-shot parsing, `parse_with_volatility_classifier` and `parse_with_dialect_and_volatility_classifier` provide the same annotation without retaining a `Parser`.

## Select a date system explicitly

New date-aware code should choose `DateSystem::Excel1900` or `DateSystem::Excel1904` and use the `*_for` APIs, for example `datetime_to_serial_for`, `try_serial_to_datetime_for`, and `try_serial_to_display_date_parts_for`. The display-parts API is the only API that distinguishes Excel's fictitious 1900-02-29 at serial 60; representable `chrono` conversion aliases serial 60 to 1900-02-28.

The root `datetime_to_serial` and `serial_to_datetime` functions keep the implicit Excel-1900 API. Their legacy module-qualified paths are also available again in 3.0:

```rust
formualizer_common::value::datetime_to_serial(&datetime);
formualizer_common::value::serial_to_datetime(serial);
```

The restored module paths provide source-path compatibility, not every 2.0 edge-case bug. In 3.0, `serial_to_datetime` uses the canonical carry-aware rounding: a fractional day that rounds to 86,400 seconds advances to midnight on the next date. The 2.0 implementation instead wrapped that boundary to midnight on the same date; that end-of-day wrap bug is intentionally not restored.

## Add wildcard arms for reported vocabularies

The following reported error/outcome enums are now `#[non_exhaustive]`. Matches outside the crate that defines an enum must include a wildcard arm:

- `ExcelErrorKind`
- `ResourceExhaustionReason`
- `PreparationStaleReason`
- `PlanStaleReason`
- `ExcelErrorExtra`
- `CoordError`
- `A1ParseError`
- `SheetAddressError`
- `ValueError`
- `formualizer_parse::RecoveryAction`
- `formualizer_parse::ParsingError`

Choose a fallback that matches the output boundary. For example, an error-code storage layer can map a future `ExcelErrorKind` to its generic error code, while a diagnostic binding can omit an unknown structured-extra projection and still preserve the error itself. Future variants must not reach `unreachable!`, `panic!`, or an equivalent assertion.

Caller-supplied/core vocabularies remain exhaustive deliberately: `DateSystem`, `LiteralValue`, `ArgKind`, `CoercionPolicy`, `SheetLocator`, and `SheetRef`, along with parser dialect, token, and core AST/value enums. Continue matching those explicitly when the compiler verifies the complete contract; do not add speculative wildcard arms.

## Serde wire compatibility

`#[non_exhaustive]` protects Rust source compatibility when a later crate version adds a variant. It does **not** make Serde formats forward-compatible: a consumer whose enum definition does not contain a serialized variant will still fail to deserialize it. Version or envelope persisted/network payloads separately, and define an explicit unknown-variant representation when forward-compatible wire decoding is required.
