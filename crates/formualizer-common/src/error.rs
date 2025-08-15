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

use std::{error::Error, fmt};

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
    Cancelled,
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
            Self::Cancelled => "#CANCELLED!",
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
            "#cancelled!" => Self::Cancelled,
            _ => panic!("Unknown error kind '{s}'"),
        }
    }
}

/// Generic, lightweight metadata that *any* error may carry.
///
/// Keep this minimal—anything only one error kind needs belongs in
/// `ExcelErrorExtra`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ErrorContext {
    pub row: Option<u32>,
    pub col: Option<u32>,
    // Origin location where the error first occurred (if different from row/col)
    pub origin_row: Option<u32>,
    pub origin_col: Option<u32>,
    pub origin_sheet: Option<String>,
}

/// Kind-specific payloads (“extension slot”).
///
/// Only variants that need extra data get it—rest stay at `None`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum ExcelErrorExtra {
    /// No additional payload (the vast majority of errors).
    #[default]
    None,

    /// `#SPILL!` – information about the intended spill size.
    Spill {
        expected_rows: u32,
        expected_cols: u32,
    },
    // --- Add future custom payloads below -------------------------------
    // AnotherKind { … },
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
            origin_row: None,
            origin_col: None,
            origin_sheet: None,
        });
        self
    }

    /// Attach origin location where the error first occurred.
    pub fn with_origin(mut self, sheet: Option<String>, row: u32, col: u32) -> Self {
        if let Some(ref mut ctx) = self.context {
            ctx.origin_sheet = sheet;
            ctx.origin_row = Some(row);
            ctx.origin_col = Some(col);
        } else {
            self.context = Some(ErrorContext {
                row: None,
                col: None,
                origin_row: Some(row),
                origin_col: Some(col),
                origin_sheet: sheet,
            });
        }
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
            write!(f, ": {msg}")?;
        }

        // Optional row/col context.
        if let Some(ref ctx) = self.context {
            if let (Some(r), Some(c)) = (ctx.row, ctx.col) {
                write!(f, " (row {r}, col {c})")?;
            }
            
            // Show origin if different from the evaluation location
            if let (Some(or), Some(oc)) = (ctx.origin_row, ctx.origin_col) {
                if ctx.origin_row != ctx.row || ctx.origin_col != ctx.col {
                    if let Some(ref sheet) = ctx.origin_sheet {
                        write!(f, " [origin: {sheet}!R{or}C{oc}]")?;
                    } else {
                        write!(f, " [origin: R{or}C{oc}]")?;
                    }
                }
            }
        }

        // Optional kind-specific payload - keep it terse for logs.
        match &self.extra {
            ExcelErrorExtra::None => {}
            ExcelErrorExtra::Spill {
                expected_rows,
                expected_cols,
            } => {
                write!(f, " [spill {expected_rows}×{expected_cols}]")?;
            }
        }

        Ok(())
    }
}

impl Error for ExcelError {}
impl From<ExcelError> for String {
    fn from(error: ExcelError) -> Self {
        format!("{error}")
    }
}
impl From<ExcelError> for LiteralValue {
    fn from(error: ExcelError) -> Self {
        LiteralValue::Error(error)
    }
}

impl PartialEq<str> for ExcelErrorKind {
    fn eq(&self, other: &str) -> bool {
        format!("{self}") == other
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
