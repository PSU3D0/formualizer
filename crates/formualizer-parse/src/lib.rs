#![cfg_attr(target_os = "emscripten", feature(let_chains))]
// See formualizer-common/lib.rs for rationale: the Pyodide nightly predates
// let-chain stabilization, so nested `if let ... { if cond { ... } }` is
// deliberate here; silence clippy's collapse suggestion crate-wide.
#![allow(clippy::collapsible_if)]

mod hasher;
pub mod parser;
pub mod pretty;
mod tests;
pub mod tokenizer;
pub mod types;

pub use parser::{
    ASTNode, ASTNodeType, parse, parse_with_dialect, parse_with_dialect_and_volatility_classifier,
    parse_with_volatility_classifier,
};
pub use pretty::{canonical_formula, pretty_parse_render, pretty_print};
pub use tokenizer::{
    RecoveryAction, Token, TokenDiagnostic, TokenSpan, TokenStream, TokenSubType, TokenType,
    TokenView, Tokenizer, TokenizerError,
};
pub use types::{FormulaDialect, ParsingError};

// Re-export common types
pub use formualizer_common::{ArgKind, ExcelError, ExcelErrorKind, LiteralValue};
