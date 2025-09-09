use formualizer_common::error::ExcelError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IoError {
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

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(feature = "json")]
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[cfg(feature = "calamine")]
    #[error("Calamine error: {0}")]
    Calamine(#[from] calamine::Error),
}

impl IoError {
    pub fn from_backend<E: std::error::Error>(backend: &str, err: E) -> Self {
        IoError::Backend {
            backend: backend.to_string(),
            message: err.to_string(),
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
