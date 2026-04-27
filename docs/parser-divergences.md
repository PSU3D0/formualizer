# Classic `Parser` vs `SpanParser` — known divergences

Tracking issue: [PSU3D0/formualizer#77](https://github.com/PSU3D0/formualizer/issues/77).

`formualizer-parse` exposes two public parser front-ends:

| Front-end | Entry points | Implementation |
|-----------|--------------|----------------|
| **Classic** token-based parser | `Parser::new(tokens, include_whitespace)`, `Parser::try_from(&str)`, `Parser::try_from_formula(&str)` | `parser::Parser` (`src/parser.rs`, ~L1930) |
| **Span** parser (preferred) | `parse`, `parse_with_dialect`, `parse_with_volatility_classifier`, `parse_with_dialect_and_volatility_classifier`, `BatchParser` | `parser::SpanParser` (`src/parser.rs`, ~L2451) |

The long-term plan in #77 is to make `SpanParser` the canonical
implementation and rewrite `Parser::parse` as a forwarder that
re-tokenizes the original source string. Until that lands, the two
parsers produce subtly different ASTs for several edge inputs. This
document is the canonical catalog of those differences; the
authoritative *runnable* version lives in
[`crates/formualizer-parse/tests/parser_differential.rs`](../crates/formualizer-parse/tests/parser_differential.rs).

## Status legend

- **Pinned** — divergence is currently observable; a test in
  `parser_differential.rs::divergence` asserts the current behaviour and
  will fail loudly when the divergence is fixed (forcing the entry to
  move into the "agreeing corpus").
- **Resolved** — currently produces equivalent ASTs across both
  parsers; a representative formula lives in `AGREEING_CORPUS`.

## Pinned divergences

### 1. `=source!#ref!` — sheet-qualified lowercase error literal

| Parser | Result |
|--------|--------|
| Classic | `Reference { original: "source!#ref!", reference: NamedRange("source!#ref!") }` |
| Span | `Literal(Error(<placeholder>))` with `kind: Error` and message `"Unknown error code: source!#ref!"` |

Both are wrong; they are wrong differently. Resolution is owned by the
"sheet-qualified-error" parity work, not by the unification PR. Pinned
by `divergence::divergence_sheet_qualified_lowercase_error_literal`.

### 2. Whitespace before `)` in classic `Parser` with `include_whitespace=true`

When the classic parser is constructed with `include_whitespace=true`,
its argument-list productions do **not** call `skip_whitespace` before
checking for `)` or before reading the next argument. This causes the
following inputs to error in classic-WS mode while parsing fine via the
span parser and via classic with `include_whitespace=false`:

- `=SUM(  )`        — empty arg list with interior whitespace
- `=SUM(A1, )`      — trailing whitespace after a comma before `)`
- `=SUM(A1, B1, )`  — trailing comma + whitespace

Source pointers:

- Classic `parse_function_arguments`: `parser.rs:1667-1672`
- Span `parse_function_arguments`: `parser.rs:2697-2704` (calls
  `skip_whitespace` before close-check)

Pinned by
`divergence::divergence_classic_with_ws_tokens_rejects_whitespace_before_close_paren`.

## Resolved (currently equal — guarded by `AGREEING_CORPUS`)

The differential harness asserts equality across ~80 formulas spanning
literals, references, arithmetic and comparison precedence, function
calls (including `IF`, `IFS`, `LET`, `LAMBDA`), arrays, sheet-qualified
references, the spilled-range `#` operator, unary precedence (post-PR
\#81), and whitespace handling such as `=SUM( A1 , B1 )` and
`= ( A1 + B1 ) ` (when classic is used with `include_whitespace=false`,
which is what `Parser::try_from(&str)` chooses by default).

## Notes on `source_token`

The differential harness ignores `source_token` and
`contains_volatile`:

- `source_token` legitimately differs because the two parsers build
  `Token` instances from different sources (filtered token vector vs.
  `TokenSpan` projected back onto the source string). After unification
  the underlying `start`/`end` offsets should match for every node, but
  for now only structural AST equality is asserted.
- `contains_volatile` is only set when a volatility classifier is
  supplied; the corpus does not configure one.

## Plan for full unification (later PR)

Per #77's "Step 3" sketch:

```rust
impl Parser {
    pub fn parse(&mut self) -> Result<ASTNode, ParserError> {
        let source: String = self.tokens.iter().map(|t| t.value.as_str()).collect();
        let source = if source.starts_with('=') { source } else { format!("={source}") };
        let spans = tokenize_spans_with_dialect(&source, self.dialect)?;
        let mut parser = SpanParser::new(&source, &spans, self.dialect);
        if let Some(c) = self.volatility_classifier.take() {
            parser = parser.with_volatility_classifier(move |n| c(n));
        }
        parser.parse()
    }
}
```

Open questions to resolve before that PR can land:

1. **Hand-constructed token streams.** `Parser::new(tokens, ...)` is
   public and accepts arbitrary `Vec<Token>`. Concatenating
   `Token::value` is lossy when tokens were built without going through
   `Tokenizer::new` (e.g. tokens lacking the implicit leading `=`,
   tokens that round-trip differently, or tokens hand-built in tests).
   Investigate real-world consumers; document a fallback (verbatim
   concatenation of `value` with a synthesised leading `=`).
2. **Source location preservation.** After re-tokenization,
   `Token::start`/`end` offsets refer to the reconstructed source
   string, not whatever the caller may have used originally. This is
   probably fine — there is no other source to refer to — but should
   be called out in the changelog.
3. **`include_whitespace` semantics.** With `SpanParser` as the
   canonical implementation, the `include_whitespace` flag becomes a
   pre-filter on the span vector; the parser already tolerates
   whitespace tokens, so passing `true` should become a no-op for AST
   shape. Document this.
4. **Repeated `parse` calls.** `SpanParser` is single-shot; `Parser`'s
   contract on repeated calls (currently it advances `position` and
   subsequent calls fail) should be preserved.
5. **`Parser::new_with_*` constructors.** Keep as public surface, route
   through the forwarder. Mark internal helpers
   (`parse_expression`, `parse_prefix`, `parse_bp`, `parse_primary`,
   `parse_operand`, `parse_function`, `parse_function_arguments`,
   `parse_array`) as `#[deprecated]` or make them private.
6. **`legacy-token-parser` feature flag.** If any downstream depends on
   hand-constructed tokens with non-trivial `Token::value` shape,
   gate the original implementation behind a feature flag rather than
   deleting it outright.

The harness in `crates/formualizer-parse/tests/parser_differential.rs`
is the contract that the unification PR must satisfy: every formula in
`AGREEING_CORPUS` keeps agreeing, and every divergence currently pinned
in `parser_differential.rs::divergence` is migrated into
`AGREEING_CORPUS` (with an assertion that classic and span produce
identical ASTs).
