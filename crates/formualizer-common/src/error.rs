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

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::LiteralValue;

/// All recognised Excel error codes.
///
/// **Note:** names are CamelCase (idiomatic Rust) while `Display`
/// renders them exactly as Excel shows them (`#DIV/0!`, …).
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
    pub fn try_parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "#null!" => Some(Self::Null),
            "#ref!" => Some(Self::Ref),
            "#name?" => Some(Self::Name),
            "#value!" => Some(Self::Value),
            "#div/0!" => Some(Self::Div),
            "#n/a" => Some(Self::Na),
            "#num!" => Some(Self::Num),
            "#error!" => Some(Self::Error),
            "#n/impl!" => Some(Self::NImpl),
            "#spill!" => Some(Self::Spill),
            "#calc!" => Some(Self::Calc),
            "#circ!" => Some(Self::Circ),
            "#cancelled!" => Some(Self::Cancelled),
            _ => None,
        }
    }

    pub fn parse(s: &str) -> Self {
        Self::try_parse(s).unwrap_or(Self::Error)
    }
}

/// Generic, lightweight metadata that *any* error may carry.
///
/// Keep this minimal—anything only one error kind needs belongs in
/// `ExcelErrorExtra`.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct ErrorContext {
    pub row: Option<u32>,
    pub col: Option<u32>,
    // Origin location where the error first occurred (if different from row/col)
    pub origin_row: Option<u32>,
    pub origin_col: Option<u32>,
    pub origin_sheet: Option<String>,
}

/// Stable reason for a resource exhaustion diagnostic.
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ResourceExhaustionReason {
    Admission,
    RetainedMemory,
    ScratchMemory,
    WorkUnits,
    Deadline,
    GraphVertices,
    GraphEdges,
    MaterializationCells,
    ArithmeticOverflow,
}

impl ResourceExhaustionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::RetainedMemory => "retained_memory",
            Self::ScratchMemory => "scratch_memory",
            Self::WorkUnits => "work_units",
            Self::Deadline => "deadline",
            Self::GraphVertices => "graph_vertices",
            Self::GraphEdges => "graph_edges",
            Self::MaterializationCells => "materialization_cells",
            Self::ArithmeticOverflow => "arithmetic_overflow",
        }
    }
}

/// Binding-safe typed resource exhaustion payload.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceExhaustionDetail {
    pub reason: ResourceExhaustionReason,
    pub limit: u64,
    pub observed: u64,
    pub request_id: Option<u64>,
}

/// Stable category for a target-preparation plan rejected at its final boundary.
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PreparationStaleReason {
    Graph,
    Authority,
    Staged,
    Symbols,
    Semantic,
    Provider,
}

impl PreparationStaleReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Graph => "graph",
            Self::Authority => "authority",
            Self::Staged => "staged",
            Self::Symbols => "symbols",
            Self::Semantic => "semantic",
            Self::Provider => "provider",
        }
    }
}

/// Stable category for a reusable recalculation plan rejected before execution.
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PlanStaleReason {
    Engine,
    Provider,
    Semantic,
    Budget,
    Staged,
    Symbols,
    Authority,
    SpanGeneration,
    Graph,
}

impl PlanStaleReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Engine => "engine",
            Self::Provider => "provider",
            Self::Semantic => "semantic",
            Self::Budget => "budget",
            Self::Staged => "staged",
            Self::Symbols => "symbols",
            Self::Authority => "authority",
            Self::SpanGeneration => "span_generation",
            Self::Graph => "graph",
        }
    }
}

/// Kind-specific payloads (“extension slot”).
///
/// Only variants that need extra data get it—rest stay at `None`.
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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

    /// Typed resource exhaustion. The canonical Excel error kind remains
    /// unchanged so existing bindings and formula error handling stay compatible.
    Resource {
        detail: Box<ResourceExhaustionDetail>,
    },

    PreparationStale {
        reason: PreparationStaleReason,
    },

    PlanStale {
        reason: PlanStaleReason,
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
#[derive(Clone, PartialEq, Eq)]
pub struct ExcelError {
    pub kind: ExcelErrorKind,
    // Empty metadata is always represented by None, keeping derived equality logical.
    payload: Option<Box<ExcelErrorPayload>>,
}

#[derive(Clone, PartialEq, Eq, Default)]
struct ExcelErrorPayload {
    message: Option<String>,
    context: Option<ErrorContext>,
    extra: ExcelErrorExtra,
}

// Keep the logical fields (rather than the storage representation) observable.
impl fmt::Debug for ExcelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExcelError")
            .field("kind", &self.kind)
            .field("message", &self.message())
            .field("context", &self.context())
            .field("extra", self.extra())
            .finish()
    }
}

impl std::hash::Hash for ExcelError {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.message().hash(state);
        self.context().hash(state);
        self.extra().hash(state);
    }
}

#[cfg(feature = "serde")]
impl Serialize for ExcelError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut fields = serializer.serialize_struct("ExcelError", 4)?;
        fields.serialize_field("kind", &self.kind)?;
        fields.serialize_field("message", &self.message())?;
        fields.serialize_field("context", &self.context())?;
        fields.serialize_field("extra", self.extra())?;
        fields.end()
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for ExcelError {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "ExcelError")]
        struct Fields {
            kind: ExcelErrorKind,
            message: Option<String>,
            context: Option<ErrorContext>,
            extra: ExcelErrorExtra,
        }
        let fields = Fields::deserialize(deserializer)?;
        let mut error = ExcelError::new(fields.kind);
        error.set_message(fields.message);
        error.set_context(fields.context);
        error.set_extra(fields.extra);
        Ok(error)
    }
}

/* ───────────────────── Constructors & helpers ─────────────────────── */

impl From<ExcelErrorKind> for ExcelError {
    fn from(kind: ExcelErrorKind) -> Self {
        Self {
            kind,
            payload: None,
        }
    }
}

impl ExcelError {
    /// Basic constructor (no message, no location, no extra).
    pub fn new(kind: ExcelErrorKind) -> Self {
        kind.into()
    }

    /// Human-readable explanation, if supplied.
    pub fn message(&self) -> Option<&str> {
        self.payload
            .as_ref()
            .and_then(|payload| payload.message.as_deref())
    }

    /// Generic location metadata, if supplied.
    pub fn context(&self) -> Option<&ErrorContext> {
        self.payload
            .as_ref()
            .and_then(|payload| payload.context.as_ref())
    }

    /// Kind-specific data; payload-free errors return `ExcelErrorExtra::None`.
    pub fn extra(&self) -> &ExcelErrorExtra {
        self.payload
            .as_ref()
            .map_or(&ExcelErrorExtra::None, |payload| &payload.extra)
    }

    fn payload_mut(&mut self) -> &mut ExcelErrorPayload {
        self.payload.get_or_insert_default()
    }

    fn discard_empty_payload(&mut self) {
        if self.payload.as_ref().is_some_and(|payload| {
            payload.message.is_none()
                && payload.context.is_none()
                && payload.extra == ExcelErrorExtra::None
        }) {
            self.payload = None;
        }
    }

    /// Replace or clear the explanation, retaining any other metadata.
    pub fn set_message(&mut self, message: Option<String>) {
        if message.is_some() || self.payload.is_some() {
            self.payload_mut().message = message;
            self.discard_empty_payload();
        }
    }

    /// Replace or clear the location, retaining any other metadata.
    pub fn set_context(&mut self, context: Option<ErrorContext>) {
        if context.is_some() || self.payload.is_some() {
            self.payload_mut().context = context;
            self.discard_empty_payload();
        }
    }

    /// Replace or clear the kind-specific data, retaining any other metadata.
    pub fn set_extra(&mut self, extra: ExcelErrorExtra) {
        if extra != ExcelErrorExtra::None || self.payload.is_some() {
            self.payload_mut().extra = extra;
            self.discard_empty_payload();
        }
    }

    /// Attach a human-readable explanation.
    pub fn with_message<S: Into<String>>(mut self, msg: S) -> Self {
        self.set_message(Some(msg.into()));
        self
    }

    /// Attach generic row/column coordinates.
    pub fn with_location(mut self, row: u32, col: u32) -> Self {
        self.set_context(Some(ErrorContext {
            row: Some(row),
            col: Some(col),
            origin_row: None,
            origin_col: None,
            origin_sheet: None,
        }));
        self
    }

    /// Attach origin location where the error first occurred.
    pub fn with_origin(mut self, sheet: Option<String>, row: u32, col: u32) -> Self {
        let ctx = self.payload_mut().context.get_or_insert_default();
        ctx.origin_sheet = sheet;
        ctx.origin_row = Some(row);
        ctx.origin_col = Some(col);
        self
    }

    /// Attach kind-specific extra data.
    pub fn with_extra(mut self, extra: ExcelErrorExtra) -> Self {
        self.set_extra(extra);
        self
    }

    pub fn from_error_string(s: &str) -> Self {
        match ExcelErrorKind::try_parse(s) {
            Some(kind) => Self::new(kind),
            None => {
                Self::new(ExcelErrorKind::Error).with_message(format!("Unknown error code: {s}"))
            }
        }
    }

    pub fn new_value() -> Self {
        Self::new(ExcelErrorKind::Value)
    }

    pub fn new_name() -> Self {
        Self::new(ExcelErrorKind::Name)
    }

    pub fn new_div() -> Self {
        Self::new(ExcelErrorKind::Div)
    }

    pub fn new_ref() -> Self {
        Self::new(ExcelErrorKind::Ref)
    }

    pub fn new_circ() -> Self {
        Self::new(ExcelErrorKind::Circ)
    }

    pub fn new_num() -> Self {
        Self::new(ExcelErrorKind::Num)
    }

    pub fn new_na() -> Self {
        Self::new(ExcelErrorKind::Na)
    }
}

/* ───────────────────────── Display / Error ────────────────────────── */

impl fmt::Display for ExcelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Start with the canonical code:
        write!(f, "{}", self.kind)?;

        // Optional human message.
        if let Some(msg) = self.message() {
            write!(f, ": {msg}")?;
        }

        // Optional row/col context.
        if let Some(ctx) = self.context() {
            if let (Some(r), Some(c)) = (ctx.row, ctx.col) {
                write!(f, " (row {r}, col {c})")?;
            }

            // Show origin if different from the evaluation location
            if let (Some(or), Some(oc)) = (ctx.origin_row, ctx.origin_col) {
                if ctx.row != Some(or) || ctx.col != Some(oc) {
                    if let Some(ref sheet) = ctx.origin_sheet {
                        write!(f, " [origin: {sheet}!R{or}C{oc}]")?;
                    } else {
                        write!(f, " [origin: R{or}C{oc}]")?;
                    }
                }
            }
        }

        // Optional kind-specific payload - keep it terse for logs.
        match self.extra() {
            ExcelErrorExtra::None => {}
            ExcelErrorExtra::Spill {
                expected_rows,
                expected_cols,
            } => {
                write!(f, " [spill {expected_rows}×{expected_cols}]")?;
            }
            ExcelErrorExtra::Resource { detail } => {
                write!(
                    f,
                    " [resource {} {}/{}]",
                    detail.reason.as_str(),
                    detail.observed,
                    detail.limit
                )?;
            }
            ExcelErrorExtra::PreparationStale { reason } => {
                write!(f, " [preparation stale {}]", reason.as_str())?;
            }
            ExcelErrorExtra::PlanStale { reason } => {
                write!(f, " [plan stale {}]", reason.as_str())?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_known_error_kind() {
        assert_eq!(ExcelErrorKind::parse("#DIV/0!"), ExcelErrorKind::Div);
        assert_eq!(ExcelErrorKind::parse("#n/a"), ExcelErrorKind::Na);
    }

    #[test]
    fn parse_unknown_error_kind_falls_back() {
        assert_eq!(ExcelErrorKind::parse("#BOGUS!"), ExcelErrorKind::Error);
        let err = ExcelError::from_error_string("#BOGUS!");
        assert_eq!(err.kind, ExcelErrorKind::Error);
        assert!(err.message().unwrap_or_default().contains("#BOGUS!"));
    }

    #[test]
    fn preparation_stale_reason_has_stable_snake_case_names() {
        assert_eq!(PreparationStaleReason::Graph.as_str(), "graph");
        assert_eq!(PreparationStaleReason::Authority.as_str(), "authority");
        assert_eq!(PreparationStaleReason::Staged.as_str(), "staged");
        assert_eq!(PreparationStaleReason::Symbols.as_str(), "symbols");
        assert_eq!(PreparationStaleReason::Semantic.as_str(), "semantic");
        assert_eq!(PreparationStaleReason::Provider.as_str(), "provider");
    }

    #[test]
    fn plan_stale_reason_has_stable_snake_case_names() {
        assert_eq!(PlanStaleReason::Engine.as_str(), "engine");
        assert_eq!(PlanStaleReason::Provider.as_str(), "provider");
        assert_eq!(PlanStaleReason::Semantic.as_str(), "semantic");
        assert_eq!(PlanStaleReason::Budget.as_str(), "budget");
        assert_eq!(PlanStaleReason::Staged.as_str(), "staged");
        assert_eq!(PlanStaleReason::Symbols.as_str(), "symbols");
        assert_eq!(PlanStaleReason::Authority.as_str(), "authority");
        assert_eq!(PlanStaleReason::SpanGeneration.as_str(), "span_generation");
        assert_eq!(PlanStaleReason::Graph.as_str(), "graph");
    }
}

#[cfg(test)]
mod compact_tests {
    use super::*;
    use std::hash::{Hash, Hasher};

    #[test]
    fn compact_layouts() {
        assert!(size_of::<ExcelError>() <= 16);
        assert!(size_of::<LiteralValue>() <= 32);
        assert!(size_of::<Result<LiteralValue, ExcelError>>() <= 40);
    }

    #[test]
    fn empty_errors_do_not_have_a_payload() {
        let mut error = ExcelError::new_value().with_extra(ExcelErrorExtra::None);
        error.set_message(None);
        error.set_context(None);
        assert!(error.payload.is_none());
        error.set_message(Some(String::new()));
        assert!(error.payload.is_some());
        error.set_context(Some(ErrorContext::default()));
        error.set_extra(ExcelErrorExtra::Spill {
            expected_rows: 2,
            expected_cols: 3,
        });
        error.set_message(None);
        error.set_context(None);
        assert!(error.payload.is_some());
        error.set_extra(ExcelErrorExtra::None);
        assert!(error.payload.is_none());
        assert_eq!(error, ExcelError::new_value());
    }

    #[test]
    fn logical_debug_hash_equality_and_display_are_unchanged() {
        #[derive(Debug, Hash)]
        struct ExcelError {
            kind: ExcelErrorKind,
            message: Option<String>,
            context: Option<ErrorContext>,
            extra: ExcelErrorExtra,
        }
        fn hash(value: &impl Hash) -> u64 {
            let mut state = std::collections::hash_map::DefaultHasher::new();
            value.hash(&mut state);
            state.finish()
        }
        for error in [
            super::ExcelError::new_div(),
            super::ExcelError::new_value().with_message("explanation"),
            super::ExcelError::new(ExcelErrorKind::Spill)
                .with_message("blocked")
                .with_location(2, 3)
                .with_origin(Some("Sheet1".into()), 4, 5)
                .with_extra(ExcelErrorExtra::Spill {
                    expected_rows: 6,
                    expected_cols: 7,
                }),
        ] {
            let old = ExcelError {
                kind: error.kind,
                message: error.message().map(str::to_owned),
                context: error.context().cloned(),
                extra: error.extra().clone(),
            };
            assert_eq!(format!("{error:?}"), format!("{old:?}"));
            assert_eq!(hash(&error), hash(&old));
            assert_eq!(error, error.clone());
        }
        let error = super::ExcelError::new(ExcelErrorKind::Spill)
            .with_message("blocked")
            .with_location(2, 3)
            .with_origin(Some("Sheet1".into()), 4, 5)
            .with_extra(ExcelErrorExtra::Spill {
                expected_rows: 6,
                expected_cols: 7,
            });
        assert_eq!(
            error.to_string(),
            "#SPILL!: blocked (row 2, col 3) [origin: Sheet1!R4C5] [spill 6×7]"
        );
        assert_ne!(error, error.clone().with_message("different"));
        assert_ne!(error, error.clone().with_location(1, 1));
        assert_ne!(error, error.clone().with_extra(ExcelErrorExtra::None));
    }
}
