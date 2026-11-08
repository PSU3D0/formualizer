use formualizer_common::error::ExcelError;
use thiserror::Error;

/// A known calculation gap after package admission, before publishing caches.
/// Consumers may preserve their valid edited input with an explicit deferred receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalculationDeferredReason {
    IterativeInitialStateUnverified,
    KnownCoverageIncomplete,
    WorkbookPrecisionUnverified,
    DependencyCoverageUnavailable,
    UnlocatedOperation,
    AffectedArrayCoverageUnverified,
}
impl std::fmt::Display for CalculationDeferredReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IterativeInitialStateUnverified => {
                f.write_str("iterative source initial state is unverified")
            }
            Self::WorkbookPrecisionUnverified => {
                f.write_str("workbook precision-as-displayed semantics are unverified")
            }
            Self::KnownCoverageIncomplete => {
                f.write_str("known calculation coverage is incomplete")
            }
            Self::DependencyCoverageUnavailable => {
                f.write_str("safe affected dependency coverage is unavailable")
            }
            Self::UnlocatedOperation => {
                f.write_str("unavailable operation has no attributable source origin")
            }
            Self::AffectedArrayCoverageUnverified => {
                f.write_str("affected array publication is not yet isolated")
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum IoError {
    #[error("Calculation deferred: {reason} ({context})")]
    CalculationDeferred {
        reason: CalculationDeferredReason,
        context: String,
    },

    #[error("Invalid calculation option {field}: {message}")]
    InvalidOptions { field: String, message: String },

    #[error("Backend error in {backend}: {message}")]
    Backend { backend: String, message: String },

    #[error("Engine error: {0}")]
    Engine(#[from] ExcelError),

    #[error("Formula parse error at {sheet}!{col}{row}: {message}")]
    FormulaParser {
        sheet: String,
        row: u32,
        col: String,
        message: String,
    },

    #[error("Schema error: {message}")]
    Schema {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Unsupported feature: {feature} in {context}")]
    Unsupported { feature: String, context: String },

    #[error("Cell error at {sheet}!{col}{row}: {message}")]
    CellError {
        sheet: String,
        row: u32,
        col: String,
        message: String,
    },

    #[error("Workbook load budget exceeded in {backend} for sheet {sheet}: {message}")]
    LoadBudgetExceeded {
        backend: String,
        sheet: String,
        message: String,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(feature = "json")]
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[cfg(feature = "xlsx-recalc")]
    #[error("Calamine error: {0}")]
    XlsxRead(#[from] crate::xlsx::types::XlsxReadError),
}

impl IoError {
    pub fn from_backend<E: std::error::Error>(backend: &str, err: E) -> Self {
        IoError::Backend {
            backend: backend.to_string(),
            message: err.to_string(),
        }
    }

    pub fn load_budget_exceeded(backend: &str, sheet: &str, message: String) -> Self {
        IoError::LoadBudgetExceeded {
            backend: backend.to_string(),
            sheet: sheet.to_string(),
            message,
        }
    }
}

pub fn with_cell_context(err: impl std::error::Error, sheet: &str, row: u32, col: u32) -> IoError {
    IoError::CellError {
        sheet: sheet.to_string(),
        row,
        col: col_to_a1(col),
        message: err.to_string(),
    }
}

pub fn col_to_a1(col: u32) -> String {
    let mut result = String::new();
    let mut n = col - 1; // Convert to 0-based

    loop {
        result.insert(0, (b'A' + (n % 26) as u8) as char);
        n /= 26;
        if n == 0 {
            break;
        }
        n -= 1;
    }

    result
}
