# formualizer-parse

`formualizer-parse` provides the tokenization, parsing, and pretty-printing
infrastructure for the Formualizer spreadsheet engine. It understands both
Excel-style and OpenFormula dialects, producing an AST that downstream crates
(`formualizer-eval`, `formualizer-workbook`, and the language bindings) rely on.

## Features

- **Tokenization** – streaming tokenizer with dialect-aware classification and
  source location tracking.
- **Parser** – Pratt-style parser that yields a stable AST and reference model
  shared with the evaluator.
- **Dialects** – switch between Excel and OpenFormula syntaxes while defaulting
  to Excel for backwards compatibility.
- **Pretty printing** – canonicalize formulas or render diagnostic trees for
  debugging and tests.

## Usage

```rust
use formualizer_parse::{FormulaDialect, Tokenizer, canonical_formula};
use formualizer_parse::parser::Parser;
# fn example() -> Result<(), Box<dyn std::error::Error>> {
let tokenizer = Tokenizer::new_with_dialect("=SUM(A1:B3)", FormulaDialect::Excel)?;
let tokens = tokenizer.items;
let mut parser = Parser::new(tokens, false);
let ast = parser.parse()?;
assert_eq!(canonical_formula(&ast), "=SUM(A1:B3)");
#     Ok(())
# }
# example().unwrap();
```

`Tokenizer::new(..)` keeps Excel semantics, while
`Tokenizer::new_with_dialect(.., FormulaDialect::OpenFormula)` enables
OpenFormula rules.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
