//! Meta crate that re-exports the primary Formualizer building blocks with
//! sensible defaults. Downstream users can depend on this crate and opt into
//! specific layers via feature flags while keeping access to the underlying
//! crates when deeper integration is required.

#[cfg(feature = "common")]
pub use formualizer_common as common;

#[cfg(feature = "parse")]
pub use formualizer_parse as parse;

#[cfg(feature = "eval")]
pub use formualizer_eval as eval;

#[cfg(feature = "workbook")]
pub use formualizer_workbook as workbook;

#[cfg(feature = "sheetport")]
pub use formualizer_sheetport as sheetport;

#[cfg(feature = "sheetport")]
pub use sheetport_spec;

#[cfg(feature = "common")]
pub use formualizer_common::{
    ErrorContext, ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue, RangeAddress,
};

#[cfg(feature = "parse")]
pub use formualizer_parse::{
    ASTNode, ASTNodeType, FormulaDialect, Token, TokenSubType, TokenType, Tokenizer,
    pretty::canonical_formula,
};

#[cfg(feature = "parse")]
pub use formualizer_parse::parser::{Parser, ReferenceType, parse_with_dialect};

#[cfg(feature = "sheetport")]
pub use formualizer_sheetport::{
    AreaLocation, BoundPort, ConstraintViolation, EvalOptions, InputUpdate, ManifestBindings,
    PortBinding, PortValue, RecordBinding, RecordFieldBinding, ScalarBinding, ScalarLocation,
    SheetPort, SheetPortError, TableBinding, TableLocation, TableRow, TableValue,
};

#[cfg(feature = "workbook")]
pub use formualizer_workbook::{
    LoadStrategy, Workbook, WorkbookConfig, WorkbookMode, WorksheetHandle,
};

#[cfg(feature = "eval")]
pub use formualizer_eval::engine::{DateSystem, EvalConfig};

#[cfg(feature = "eval")]
pub use formualizer_eval::engine::eval::EvalPlan;
