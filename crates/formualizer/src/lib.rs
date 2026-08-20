//! Meta crate that re-exports the primary Formualizer building blocks with
//! sensible defaults. Downstream users can depend on this crate and opt into
//! specific layers via feature flags while keeping access to the underlying
//! crates when deeper integration is required.
//!
//! Inspection DTOs are currently types-only through this facade. Until the
//! workbook and language-binding integrations land, use `formualizer-eval`
//! directly to call the engine inspection methods; the facade intentionally
//! does not re-export `Engine`.

#[cfg(feature = "common")]
pub use formualizer_common as common;

#[cfg(feature = "parse")]
pub use formualizer_parse as parse;

#[cfg(feature = "eval")]
pub use formualizer_eval as eval;

#[cfg(feature = "workbook")]
pub use formualizer_workbook as workbook;

#[cfg(feature = "workbook")]
pub mod doc_examples;

#[cfg(feature = "sheetport")]
pub use formualizer_sheetport as sheetport;

#[cfg(feature = "sheetport")]
pub use sheetport_spec;

#[cfg(feature = "common")]
pub use formualizer_common::{
    CellAddress, ErrorContext, ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue,
    RangeAddress, RangeArea,
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

#[cfg(all(feature = "workbook", feature = "umya"))]
pub use formualizer_workbook::{
    DEFAULT_ERROR_LOCATION_LIMIT, RecalculateErrorSummary, RecalculateSheetSummary,
    RecalculateStatus, RecalculateSummary, recalculate_file, recalculate_file_with_limit,
};

#[cfg(feature = "eval")]
pub use formualizer_eval::engine::{DateSystem, EvalConfig, TemporalEgress};

#[cfg(feature = "eval")]
pub use formualizer_eval::engine::inspect::{
    CellSnapshot, CellSnapshotReport, Dependent, DependentsOptions, DependentsReport, InspectError,
    InspectionUnavailableReason, LinkDisposition, NameResolution, OmittedCount, Precedent,
    PrecedentOptions, PrecedentReport, Provenance, RangePage, RangePageOptions, SemanticReference,
    SnapshotOptions, SpillRole, Staleness, StateStamp, TraceDirection, TraceGraph, TraceLink,
    TraceLinkKind, TraceLinkTarget, TraceNode, TraceNodeId, TraceOptions, TruncationReport,
};

#[cfg(feature = "eval")]
pub use formualizer_eval::engine::eval::EvalPlan;
