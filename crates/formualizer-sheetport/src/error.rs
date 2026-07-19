use crate::validation::ConstraintViolation;
use formualizer_common::ExcelError;
use formualizer_workbook::error::IoError;
use sheetport_spec::{ManifestIssue, ValidationError};
use thiserror::Error;

/// Errors produced when constructing or operating a SheetPort runtime.
#[derive(Debug, Error)]
pub enum SheetPortError {
    /// Manifest failed canonical validation.
    #[error("manifest validation failed")]
    InvalidManifest { issues: Vec<ManifestIssue> },
    /// Selector combination is not yet supported for the given port.
    #[error("unsupported selector for port `{port}`: {reason}")]
    UnsupportedSelector { port: String, reason: String },
    /// Reference string could not be parsed.
    #[error("invalid reference `{reference}` in port `{port}`: {details}")]
    InvalidReference {
        port: String,
        reference: String,
        details: String,
    },
    /// Referenced sheet was not present in the workbook.
    #[error("sheet `{sheet}` referenced by port `{port}` was not found in the workbook")]
    MissingSheet { port: String, sheet: String },
    /// Structural invariant could not be satisfied.
    #[error("invariant violation for port `{port}`: {message}")]
    InvariantViolation { port: String, message: String },
    /// A bounded layout selector did not observe its configured terminator.
    #[error(
        "layout selector for port `{port}` on sheet `{sheet}` exhausted {limit} rows from {scan_start} using `{termination}`"
    )]
    LayoutExhausted {
        port: String,
        sheet: String,
        termination: String,
        scan_start: u32,
        limit: u32,
        observed: u32,
    },
    /// A selector could not be proven safe inside its evaluation envelope.
    #[error("selector safety error for port `{port}`: {reason}")]
    SelectorSafety { port: String, reason: String },
    /// Input or resolved data violated manifest constraints.
    #[error("value did not satisfy manifest constraints")]
    ConstraintViolation {
        violations: Vec<ConstraintViolation>,
    },
    /// Baseline restoration failed after a scenario failure.
    #[error("batch failed ({primary}); baseline restoration also failed ({restoration})")]
    BatchRestoration {
        primary: Box<SheetPortError>,
        restoration: Box<SheetPortError>,
    },
    /// Underlying engine reported an evaluation error.
    #[error("engine error: {source}")]
    Engine {
        #[from]
        source: ExcelError,
    },
    /// Failure when interacting with the underlying workbook backend.
    #[error("workbook error: {source}")]
    Workbook {
        #[from]
        source: IoError,
    },
}

impl From<ValidationError> for SheetPortError {
    fn from(err: ValidationError) -> Self {
        SheetPortError::InvalidManifest {
            issues: err.issues().to_vec(),
        }
    }
}
