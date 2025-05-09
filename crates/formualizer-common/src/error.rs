//! Excel-style error representation that is both ergonomic **now**
//! *and* flexible enough to grow new, data-rich variants later.
//!
//! - **`ExcelErrorKind`** : the canonical set of Excel error codes  
//! - **`ErrorContext`**   : lightweight, sheet-agnostic location info  
//! - **`ExcelErrorExtra`**: per-kind “extension slot” (e.g. `Spill`)  
//! - **`ExcelError`**     : one struct that glues the three together
//!
//! When a future error needs its own payload, just add another variant
//! to `ExcelErrorExtra`; existing code does not break.

use std::{borrow::Cow, error::Error, fmt};

use crate::LiteralValue;

/// All recognised Excel error codes.
///
/// **Note:** names are CamelCase (idiomatic Rust) while `Display`
/// renders them exactly as Excel shows them (`#DIV/0!`, …).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ExcelErrorKind {
    Null,
    Ref,
    Name,
    Value,
    Div,
    Na,
    Num,
    Error,
    NImpl,
    Spill,
    Calc,
    Circ,
}

impl fmt::Display for ExcelErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Null => "#NULL!",
            Self::Ref => "#REF!",
            Self::Name => "#NAME?",
            Self::Value => "#VALUE!",
            Self::Div => "#DIV/0!",
            Self::Na => "#N/A",
            Self::Num => "#NUM!",
            Self::Error => "#ERROR!",
            Self::NImpl => "#N/IMPL!",
            Self::Spill => "#SPILL!",
            Self::Calc => "#CALC!",
            Self::Circ => "#CIRC!",
        })
    }
}

impl ExcelErrorKind {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "#null!" => Self::Null,
            "#ref!" => Self::Ref,
            "#name?" => Self::Name,
            "#value!" => Self::Value,
            "#div/0!" => Self::Div,
            "#n/a" => Self::Na,
            "#num!" => Self::Num,
            "#error!" => Self::Error,
            "#n/impl!" => Self::NImpl,
            "#spill!" => Self::Spill,
            "#calc!" => Self::Calc,
            "#circ!" => Self::Circ,
            _ => panic!("Unknown error kind '{}'", s),
        }
    }
}

/// Generic, lightweight metadata that *any* error may carry.
///
/// Keep this minimal—anything only one error kind needs belongs in
/// `ExcelErrorExtra`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ErrorContext {
    pub row: Option<u32>,
    pub col: Option<u32>,
    // Add more sheet-wide coordinates here if ever required (sheet name, etc.)
}

impl Default for ErrorContext {
    fn default() -> Self {
        Self {
            row: None,
            col: None,
        }
    }
}

/// Kind-specific payloads (“extension slot”).
///
/// Only variants that need extra data get it—rest stay at `None`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExcelErrorExtra {
    /// No additional payload (the vast majority of errors).
    None,

    /// `#SPILL!` – information about the intended spill size.
    Spill {
        expected_rows: u32,
        expected_cols: u32,
    },
    // --- Add future custom payloads below -------------------------------
    // AnotherKind { … },
}

impl Default for ExcelErrorExtra {
    fn default() -> Self {
        ExcelErrorExtra::None
    }
}

/// The single struct your API passes around.
///
/// It combines:
/// * **kind**   – the mandatory Excel error code
/// * **message**– optional human explanation
/// * **context**– generic location†
/// * **extra**  – optional, kind-specific data
///
/// † If you *never* need row/col you can build the value with
///   `ExcelError::from(kind)`, which sets `context = None`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExcelError {
    pub kind: ExcelErrorKind,
    pub message: Option<String>,
    pub context: Option<ErrorContext>,
    pub extra: ExcelErrorExtra,
}

/* ───────────────────── Constructors & helpers ─────────────────────── */

impl From<ExcelErrorKind> for ExcelError {
    fn from(kind: ExcelErrorKind) -> Self {
        Self {
            kind,
            message: None,
            context: None,
            extra: ExcelErrorExtra::None,
        }
    }
}

impl ExcelError {
    /// Basic constructor (no message, no location, no extra).
    pub fn new(kind: ExcelErrorKind) -> Self {
        kind.into()
    }

    /// Attach a human-readable explanation.
    pub fn with_message<S: Into<String>>(mut self, msg: S) -> Self {
        self.message = Some(msg.into());
        self
    }

    /// Attach generic row/column coordinates.
    pub fn with_location(mut self, row: u32, col: u32) -> Self {
        self.context = Some(ErrorContext {
            row: Some(row),
            col: Some(col),
        });
        self
    }

    /// Attach kind-specific extra data.
    pub fn with_extra(mut self, extra: ExcelErrorExtra) -> Self {
        self.extra = extra;
        self
    }

    pub fn from_error_string(s: &str) -> Self {
        let kind = ExcelErrorKind::parse(s);
        Self::new(kind)
    }
}

/* ───────────────────────── Display / Error ────────────────────────── */

impl fmt::Display for ExcelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Start with the canonical code:
        write!(f, "{}", self.kind)?;

        // Optional human message.
        if let Some(ref msg) = self.message {
            write!(f, ": {}", msg)?;
        }

        // Optional row/col context.
        if let Some(ErrorContext {
            row: Some(r),
            col: Some(c),
        }) = self.context
        {
            write!(f, " (row {}, col {})", r, c)?;
        }

        // Optional kind-specific payload - keep it terse for logs.
        match &self.extra {
            ExcelErrorExtra::None => {}
            ExcelErrorExtra::Spill {
                expected_rows,
                expected_cols,
            } => {
                write!(f, " [spill {}×{}]", expected_rows, expected_cols)?;
            }
        }

        Ok(())
    }
}

impl Error for ExcelError {}
impl From<ExcelError> for String {
    fn from(error: ExcelError) -> Self {
        format!("{}", error)
    }
}
impl From<ExcelError> for LiteralValue {
    fn from(error: ExcelError) -> Self {
        LiteralValue::Error(error)
    }
}

impl PartialEq<str> for ExcelErrorKind {
    fn eq(&self, other: &str) -> bool {
        self.to_string() == other
    }
}

impl PartialEq<&str> for ExcelError {
    fn eq(&self, other: &&str) -> bool {
        self.kind.to_string() == *other
    }
}

impl PartialEq<str> for ExcelError {
    fn eq(&self, other: &str) -> bool {
        self.kind.to_string() == other
    }
}
