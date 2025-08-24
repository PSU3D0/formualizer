use crate::engine::range_view::RangeView;
pub use crate::function::Function;
use crate::interpreter::Interpreter;
use crate::reference::CellRef;
use formualizer_common::{
    LiteralValue,
    error::{ExcelError, ExcelErrorKind},
};
use std::any::Any;
use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType, TableSpecifier};

/* ───────────────────────────── Range ───────────────────────────── */

pub trait Range: Debug + Send + Sync {
    fn get(&self, row: usize, col: usize) -> Result<LiteralValue, ExcelError>;
    fn dimensions(&self) -> (usize, usize);

    fn is_sparse(&self) -> bool {
        false
    }

    // Handle infinite ranges (A:A, 1:1)
    fn is_infinite(&self) -> bool {
        false
    }

    fn materialise(&self) -> Cow<'_, [Vec<LiteralValue>]> {
        Cow::Owned(
            (0..self.dimensions().0)
                .map(|r| {
                    (0..self.dimensions().1)
                        .map(|c| self.get(r, c).unwrap_or(LiteralValue::Empty))
                        .collect()
                })
                .collect(),
        )
    }

    fn iter_cells<'a>(&'a self) -> Box<dyn Iterator<Item = LiteralValue> + 'a> {
        let (rows, cols) = self.dimensions();
        Box::new((0..rows).flat_map(move |r| (0..cols).map(move |c| self.get(r, c).unwrap())))
    }
    fn iter_rows<'a>(&'a self) -> Box<dyn Iterator<Item = Vec<LiteralValue>> + 'a> {
        let (rows, cols) = self.dimensions();
        Box::new((0..rows).map(move |r| (0..cols).map(|c| self.get(r, c).unwrap()).collect()))
    }

    /* down-cast hook for SIMD back-ends */
    fn as_any(&self) -> &dyn Any;
}

/* blanket dyn passthrough */
impl Range for Box<dyn Range> {
    fn get(&self, r: usize, c: usize) -> Result<LiteralValue, ExcelError> {
        (**self).get(r, c)
    }
    fn dimensions(&self) -> (usize, usize) {
        (**self).dimensions()
    }
    fn is_sparse(&self) -> bool {
        (**self).is_sparse()
    }
    fn materialise(&self) -> Cow<'_, [Vec<LiteralValue>]> {
        (**self).materialise()
    }
    fn iter_cells<'a>(&'a self) -> Box<dyn Iterator<Item = LiteralValue> + 'a> {
        (**self).iter_cells()
    }
    fn iter_rows<'a>(&'a self) -> Box<dyn Iterator<Item = Vec<LiteralValue>> + 'a> {
        (**self).iter_rows()
    }
    fn as_any(&self) -> &dyn Any {
        (**self).as_any()
    }
}

/* ────────────────────── ArgumentHandle helpers ───────────────────── */

pub type CowValue<'a> = Cow<'a, LiteralValue>;

pub enum EvaluatedArg<'a> {
    LiteralValue(CowValue<'a>),
    Range(Box<dyn Range>),
}

pub struct ArgumentHandle<'a, 'b> {
    node: &'a ASTNode,
    interp: &'a Interpreter<'b>,
}

impl<'a, 'b> ArgumentHandle<'a, 'b> {
    pub(crate) fn new(node: &'a ASTNode, interp: &'a Interpreter<'b>) -> Self {
        Self { node, interp }
    }

    pub fn value(&self) -> Result<CowValue<'_>, ExcelError> {
        if let ASTNodeType::Literal(ref v) = self.node.node_type {
            return Ok(Cow::Borrowed(v));
        }
        self.interp.evaluate_ast(self.node).map(Cow::Owned)
    }

    pub fn range(&self) -> Result<Box<dyn Range>, ExcelError> {
        match &self.node.node_type {
            ASTNodeType::Reference { reference, .. } => {
                self.interp.context.resolve_range_like(reference)
            }
            ASTNodeType::Array(rows) => {
                let mut materialized = Vec::new();
                for row in rows {
                    let mut materialized_row = Vec::new();
                    for cell in row {
                        materialized_row.push(self.interp.evaluate_ast(cell)?);
                    }
                    materialized.push(materialized_row);
                }
                Ok(Box::new(InMemoryRange::new(materialized)))
            }
            _ => Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message(format!("Expected a range, got {:?}", self.node.node_type))),
        }
    }

    /// Resolve as a RangeView (Phase 2 API). Only supports reference arguments.
    pub fn range_view(&self) -> Result<RangeView<'_>, ExcelError> {
        match &self.node.node_type {
            ASTNodeType::Reference { reference, .. } => self
                .interp
                .context
                .resolve_range_view(reference, self.interp.current_sheet()),
            // Treat array literals (LiteralValue::Array) as ranges for RangeView APIs
            ASTNodeType::Literal(formualizer_common::LiteralValue::Array(arr)) => {
                // Borrow the rows directly from the AST literal
                Ok(RangeView::from_borrowed(&arr[..]))
            }
            ASTNodeType::Array(rows) => {
                // Materialize AST array to values, then return a borrowed view
                let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(rows.len());
                for r in rows {
                    let mut row_vals = Vec::with_capacity(r.len());
                    for cell in r {
                        row_vals.push(self.interp.evaluate_ast(cell)?);
                    }
                    out.push(row_vals);
                }
                Ok(RangeView::from_borrowed(Box::leak(Box::new(out))))
            }
            _ => Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Argument cannot be interpreted as a range.")),
        }
    }

    pub fn value_or_range(&self) -> Result<EvaluatedArg<'_>, ExcelError> {
        self.range()
            .map(EvaluatedArg::Range)
            .or_else(|_| self.value().map(EvaluatedArg::LiteralValue))
    }

    /// Lazily iterate values for this argument in row-major expansion order.
    /// - Reference: stream via RangeView (row-major)
    /// - Array literal: evaluate each element lazily per cell
    /// - Scalar/other expressions: a single value
    pub fn lazy_values_owned(
        &'a self,
    ) -> Result<Box<dyn Iterator<Item = LiteralValue> + 'a>, ExcelError> {
        match &self.node.node_type {
            ASTNodeType::Reference { .. } => {
                let view = self.range_view()?;
                let mut values: Vec<LiteralValue> = Vec::new();
                view.for_each_cell(&mut |v| {
                    values.push(v.clone());
                    Ok(())
                })?;
                Ok(Box::new(values.into_iter()))
            }
            ASTNodeType::Array(rows) => {
                struct ArrayEvalIter<'a, 'b> {
                    rows: &'a [Vec<ASTNode>],
                    r: usize,
                    c: usize,
                    interp: &'a Interpreter<'b>,
                }
                impl<'a, 'b> Iterator for ArrayEvalIter<'a, 'b> {
                    type Item = LiteralValue;
                    fn next(&mut self) -> Option<Self::Item> {
                        if self.rows.is_empty() {
                            return None;
                        }
                        let rows = self.rows;
                        let mut r = self.r;
                        let mut c = self.c;
                        if r >= rows.len() {
                            return None;
                        }
                        let node = &rows[r][c];
                        // advance indices
                        c += 1;
                        if c >= rows[r].len() {
                            r += 1;
                            c = 0;
                        }
                        self.r = r;
                        self.c = c;
                        match self.interp.evaluate_ast(node) {
                            Ok(v) => Some(v),
                            Err(e) => Some(LiteralValue::Error(e)),
                        }
                    }
                }
                let it = ArrayEvalIter {
                    rows,
                    r: 0,
                    c: 0,
                    interp: self.interp,
                };
                Ok(Box::new(it))
            }
            _ => {
                // Single value expression
                let v = self.value()?.into_owned();
                Ok(Box::new(std::iter::once(v)))
            }
        }
    }

    pub fn ast(&self) -> &'a ASTNode {
        self.node
    }

    /// Returns the raw reference from the AST when this argument is a reference.
    /// This does not evaluate the reference or materialize values.
    pub fn as_reference(&self) -> Result<&'a ReferenceType, ExcelError> {
        match &self.node.node_type {
            ASTNodeType::Reference { reference, .. } => Ok(reference),
            _ => Err(ExcelError::new(ExcelErrorKind::Ref)
                .with_message("Expected a reference (by-ref argument)")),
        }
    }

    /// Returns a `ReferenceType` if this argument is a reference or a function that
    /// can yield a reference via `eval_reference`. Materializes no values.
    pub fn as_reference_or_eval(&self) -> Result<ReferenceType, ExcelError> {
        match &self.node.node_type {
            ASTNodeType::Reference { reference, .. } => Ok(reference.clone()),
            ASTNodeType::Function { name, args } => {
                if let Some(fun) = self.interp.context.get_function("", name) {
                    let handles: Vec<ArgumentHandle> = args
                        .iter()
                        .map(|n| ArgumentHandle::new(n, self.interp))
                        .collect();
                    let fctx =
                        crate::traits::DefaultFunctionContext::new(self.interp.context, None);
                    if let Some(res) = fun.eval_reference(&handles, &fctx) {
                        res
                    } else {
                        Err(ExcelError::new(ExcelErrorKind::Ref)
                            .with_message("Function does not return a reference"))
                    }
                } else {
                    Err(ExcelError::new(ExcelErrorKind::Name))
                }
            }
            _ => {
                Err(ExcelError::new(ExcelErrorKind::Ref)
                    .with_message("Argument is not a reference"))
            }
        }
    }

    /* tiny validator helper for macro */
    pub fn matches_kind(&self, k: formualizer_common::ArgKind) -> Result<bool, ExcelError> {
        Ok(match k {
            formualizer_common::ArgKind::Any => true,
            formualizer_common::ArgKind::Range => self.range().is_ok(),
            formualizer_common::ArgKind::Number => matches!(
                self.value()?.as_ref(),
                LiteralValue::Number(_) | LiteralValue::Int(_)
            ),
            formualizer_common::ArgKind::Text => {
                matches!(self.value()?.as_ref(), LiteralValue::Text(_))
            }
            formualizer_common::ArgKind::Logical => {
                matches!(self.value()?.as_ref(), LiteralValue::Boolean(_))
            }
        })
    }
}

/* simple Vec-backed range */
#[derive(Debug, Clone)]
pub struct InMemoryRange {
    data: Vec<Vec<LiteralValue>>,
}
impl InMemoryRange {
    pub fn new(d: Vec<Vec<LiteralValue>>) -> Self {
        Self { data: d }
    }
}
impl Range for InMemoryRange {
    fn get(&self, r: usize, c: usize) -> Result<LiteralValue, ExcelError> {
        Ok(self
            .data
            .get(r)
            .and_then(|row| row.get(c))
            .cloned()
            .unwrap_or(LiteralValue::Empty))
    }
    fn dimensions(&self) -> (usize, usize) {
        (self.data.len(), self.data.first().map_or(0, |r| r.len()))
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/* ───────────────────────── Table abstraction ───────────────────────── */

pub trait Table: Debug + Send + Sync {
    fn get_cell(&self, row: usize, column: &str) -> Result<LiteralValue, ExcelError>;
    fn get_column(&self, column: &str) -> Result<Box<dyn Range>, ExcelError>;
    /// Ordered list of column names
    fn columns(&self) -> Vec<String> {
        vec![]
    }
    /// Number of data rows (excluding headers/totals)
    fn data_height(&self) -> usize {
        0
    }
    /// Whether the table has a header row
    fn has_headers(&self) -> bool {
        false
    }
    /// Whether the table has a totals row
    fn has_totals(&self) -> bool {
        false
    }
    /// Headers row as a 1xW range
    fn headers_row(&self) -> Option<Box<dyn Range>> {
        None
    }
    /// Totals row as a 1xW range, if present
    fn totals_row(&self) -> Option<Box<dyn Range>> {
        None
    }
    /// Entire data body as HxW range
    fn data_body(&self) -> Option<Box<dyn Range>> {
        None
    }
    fn clone_box(&self) -> Box<dyn Table>;
}
impl Table for Box<dyn Table> {
    fn get_cell(&self, r: usize, c: &str) -> Result<LiteralValue, ExcelError> {
        (**self).get_cell(r, c)
    }
    fn get_column(&self, c: &str) -> Result<Box<dyn Range>, ExcelError> {
        (**self).get_column(c)
    }
    fn columns(&self) -> Vec<String> {
        (**self).columns()
    }
    fn data_height(&self) -> usize {
        (**self).data_height()
    }
    fn has_headers(&self) -> bool {
        (**self).has_headers()
    }
    fn has_totals(&self) -> bool {
        (**self).has_totals()
    }
    fn headers_row(&self) -> Option<Box<dyn Range>> {
        (**self).headers_row()
    }
    fn totals_row(&self) -> Option<Box<dyn Range>> {
        (**self).totals_row()
    }
    fn data_body(&self) -> Option<Box<dyn Range>> {
        (**self).data_body()
    }
    fn clone_box(&self) -> Box<dyn Table> {
        (**self).clone_box()
    }
}

/* ─────────────────────── Resolver super-trait ─────────────────────── */

pub trait ReferenceResolver: Send + Sync {
    fn resolve_cell_reference(
        &self,
        sheet: Option<&str>,
        row: u32,
        col: u32,
    ) -> Result<LiteralValue, ExcelError>;
}
pub trait RangeResolver: Send + Sync {
    fn resolve_range_reference(
        &self,
        sheet: Option<&str>,
        sr: Option<u32>,
        sc: Option<u32>,
        er: Option<u32>,
        ec: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError>;
}
pub trait NamedRangeResolver: Send + Sync {
    fn resolve_named_range_reference(
        &self,
        name: &str,
    ) -> Result<Vec<Vec<LiteralValue>>, ExcelError>;
}
pub trait TableResolver: Send + Sync {
    fn resolve_table_reference(
        &self,
        tref: &formualizer_core::parser::TableReference,
    ) -> Result<Box<dyn Table>, ExcelError>;
}
pub trait Resolver: ReferenceResolver + RangeResolver + NamedRangeResolver + TableResolver {
    fn resolve_range_like(&self, r: &ReferenceType) -> Result<Box<dyn Range>, ExcelError> {
        match r {
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
            } => self.resolve_range_reference(
                sheet.as_deref(),
                *start_row,
                *start_col,
                *end_row,
                *end_col,
            ),
            ReferenceType::Table(tref) => {
                let t = self.resolve_table_reference(tref)?;
                match &tref.specifier {
                    Some(TableSpecifier::Column(c)) => t.get_column(c),
                    Some(TableSpecifier::ColumnRange(start, end)) => {
                        // Build a rectangular range from start..=end columns in table order
                        let cols = t.columns();
                        let start_idx = cols.iter().position(|n| n.eq_ignore_ascii_case(start));
                        let end_idx = cols.iter().position(|n| n.eq_ignore_ascii_case(end));
                        if let (Some(mut si), Some(mut ei)) = (start_idx, end_idx) {
                            if si > ei {
                                std::mem::swap(&mut si, &mut ei);
                            }
                            // Materialize by stacking columns into a 2D array
                            let h = t.data_height();
                            let w = ei - si + 1;
                            let mut rows = vec![vec![LiteralValue::Empty; w]; h];
                            for (offset, ci) in (si..=ei).enumerate() {
                                let cname = &cols[ci];
                                let col_range = t.get_column(cname)?;
                                let (rh, _) = col_range.dimensions();
                                for (r, row) in rows.iter_mut().enumerate().take(h.min(rh)) {
                                    row[offset] = col_range.get(r, 0)?;
                                }
                            }
                            Ok(Box::new(InMemoryRange::new(rows)))
                        } else {
                            Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                                "Column range refers to unknown column(s)".to_string(),
                            ))
                        }
                    }
                    Some(TableSpecifier::SpecialItem(
                        formualizer_core::parser::SpecialItem::Headers,
                    )) => {
                        if let Some(h) = t.headers_row() {
                            Ok(h)
                        } else {
                            Ok(Box::new(InMemoryRange::new(vec![])))
                        }
                    }
                    Some(TableSpecifier::SpecialItem(
                        formualizer_core::parser::SpecialItem::Totals,
                    )) => {
                        if let Some(tr) = t.totals_row() {
                            Ok(tr)
                        } else {
                            Ok(Box::new(InMemoryRange::new(vec![])))
                        }
                    }
                    Some(TableSpecifier::SpecialItem(
                        formualizer_core::parser::SpecialItem::Data,
                    )) => {
                        if let Some(body) = t.data_body() {
                            Ok(body)
                        } else {
                            Ok(Box::new(InMemoryRange::new(vec![])))
                        }
                    }
                    Some(TableSpecifier::SpecialItem(
                        formualizer_core::parser::SpecialItem::All,
                    )) => {
                        // Equivalent to TableSpecifier::All handling
                        let mut out: Vec<Vec<LiteralValue>> = Vec::new();
                        if let Some(h) = t.headers_row() {
                            out.extend(h.iter_rows());
                        }
                        if let Some(body) = t.data_body() {
                            out.extend(body.iter_rows());
                        }
                        if let Some(tr) = t.totals_row() {
                            out.extend(tr.iter_rows());
                        }
                        Ok(Box::new(InMemoryRange::new(out)))
                    }
                    Some(TableSpecifier::SpecialItem(
                        formualizer_core::parser::SpecialItem::ThisRow,
                    )) => Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "@ (This Row) requires table-aware context; not yet supported".to_string(),
                    )),
                    Some(TableSpecifier::All) => {
                        // Concatenate headers (if any), data, totals (if any)
                        let mut out: Vec<Vec<LiteralValue>> = Vec::new();
                        if let Some(h) = t.headers_row() {
                            out.extend(h.iter_rows());
                        }
                        if let Some(body) = t.data_body() {
                            out.extend(body.iter_rows());
                        }
                        if let Some(tr) = t.totals_row() {
                            out.extend(tr.iter_rows());
                        }
                        Ok(Box::new(InMemoryRange::new(out)))
                    }
                    Some(TableSpecifier::Data) => {
                        if let Some(body) = t.data_body() {
                            Ok(body)
                        } else {
                            Ok(Box::new(InMemoryRange::new(vec![])))
                        }
                    }
                    // Defer complex combinations and row selectors for tranche 1
                    Some(TableSpecifier::Combination(_)) => Err(ExcelError::new(
                        ExcelErrorKind::NImpl,
                    )
                    .with_message("Complex structured references not yet supported".to_string())),
                    Some(TableSpecifier::Row(_)) => Err(ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("Row selectors (@/index) not yet supported".to_string())),
                    Some(TableSpecifier::Headers) | Some(TableSpecifier::Totals) => {
                        Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                            "Legacy Headers/Totals variants not used; use SpecialItem".to_string(),
                        ))
                    }
                    None => Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                        "Table reference without specifier is unsupported".to_string(),
                    )),
                }
            }
            ReferenceType::NamedRange(n) => {
                let v = self.resolve_named_range_reference(n)?;
                Ok(Box::new(InMemoryRange::new(v)))
            }
            ReferenceType::Cell { sheet, row, col } => {
                let v = self.resolve_cell_reference(sheet.as_deref(), *row, *col)?;
                Ok(Box::new(InMemoryRange::new(vec![vec![v]])))
            }
        }
    }
}

/* ───────────────────── EvaluationContext = Resolver+Fns ───────────── */

pub trait FunctionProvider: Send + Sync {
    fn get_function(&self, ns: &str, name: &str) -> Option<Arc<dyn Function>>;
}

pub trait EvaluationContext: Resolver + FunctionProvider {
    /// Get access to the shared thread pool for parallel evaluation
    /// Returns None if parallel evaluation is disabled or unavailable
    fn thread_pool(&self) -> Option<&Arc<rayon::ThreadPool>> {
        None
    }

    /// Optional cancellation token. When Some, long-running operations should periodically abort.
    fn cancellation_token(&self) -> Option<&std::sync::atomic::AtomicBool> {
        None
    }

    /// Optional chunk size hint for streaming visitors.
    fn chunk_hint(&self) -> Option<usize> {
        None
    }

    /// Resolve a reference into a `RangeView` with clear bounds.
    /// Implementations should resolve un/partially bounded references using used-region.
    fn resolve_range_view<'c>(
        &'c self,
        _reference: &ReferenceType,
        _current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::NImpl))
    }

    /// Locale provider: invariant by default
    fn locale(&self) -> crate::locale::Locale {
        crate::locale::Locale::invariant()
    }

    /// Timezone provider for date/time functions
    /// Default: Local (Excel-compatible behavior)
    /// Functions should use local timezone when this returns Local
    fn timezone(&self) -> &crate::timezone::TimeZoneSpec {
        // Static default to avoid allocation
        static DEFAULT_TZ: std::sync::OnceLock<crate::timezone::TimeZoneSpec> =
            std::sync::OnceLock::new();
        DEFAULT_TZ.get_or_init(crate::timezone::TimeZoneSpec::default)
    }

    /// Volatile granularity. Default Always for backwards compatibility.
    fn volatile_level(&self) -> VolatileLevel {
        VolatileLevel::Always
    }

    /// A stable workbook seed for RNG composition.
    fn workbook_seed(&self) -> u64 {
        0xF0F0_D0D0_AAAA_5555
    }

    /// Recalc epoch that increments on each full recalc when appropriate.
    fn recalc_epoch(&self) -> u64 {
        0
    }

    /* ─────────────── Future-proof IO/backends hooks (default no-op) ─────────────── */

    /// Optional: Return the min/max used rows for a set of columns on a sheet.
    /// When None, the backend does not provide used-region hints.
    fn used_rows_for_columns(
        &self,
        _sheet: &str,
        _start_col: u32,
        _end_col: u32,
    ) -> Option<(u32, u32)> {
        None
    }

    /// Optional: Return the min/max used columns for a set of rows on a sheet.
    /// When None, the backend does not provide used-region hints.
    fn used_cols_for_rows(
        &self,
        _sheet: &str,
        _start_row: u32,
        _end_row: u32,
    ) -> Option<(u32, u32)> {
        None
    }

    /// Optional: Physical sheet bounds (max rows, max cols) if known.
    fn sheet_bounds(&self, _sheet: &str) -> Option<(u32, u32)> {
        None
    }

    /// Monotonic identifier for the current data snapshot; increments on mutation.
    fn data_snapshot_id(&self) -> u64 {
        0
    }

    /// Backend capability advertisement for IO/adapters.
    fn backend_caps(&self) -> BackendCaps {
        BackendCaps::default()
    }

    /// Optional: Retrieve a pre-flattened view for a reference if available (Phase 2).
    /// Default: None (no warmup/cache available through this context).
    fn get_or_flatten(
        &self,
        _reference: &ReferenceType,
        _prefer_numeric: bool,
    ) -> Option<crate::engine::cache::FlatView> {
        None
    }
}

/// Minimal backend capability descriptor for planning and adapters.
#[derive(Copy, Clone, Debug, Default)]
pub struct BackendCaps {
    /// Provides lazy access (// TODO REMOVE?)
    pub streaming: bool,
    /// Can compute used-region for rows/columns
    pub used_region: bool,
    /// Supports write-back mutations via external sink
    pub write: bool,
    /// Provides table metadata/streaming beyond basic column access
    pub tables: bool,
    /// May provide asynchronous/lazy remote streams (reserved)
    pub async_stream: bool,
}

/* ───────────────────── FunctionContext (narrow) ───────────────────── */

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum VolatileLevel {
    /// Value can change at any edit; seed excludes recalc_epoch by default.
    Always,
    /// Value changes per recalculation; seed should include recalc_epoch.
    OnRecalc,
    /// Value changes per open; seed uses only workbook_seed.
    OnOpen,
}

/// Minimal context exposed to functions (no engine/graph APIs)
pub trait FunctionContext {
    fn locale(&self) -> crate::locale::Locale;
    fn timezone(&self) -> &crate::timezone::TimeZoneSpec;
    fn thread_pool(&self) -> Option<&std::sync::Arc<rayon::ThreadPool>>;
    fn cancellation_token(&self) -> Option<&std::sync::atomic::AtomicBool>;
    fn chunk_hint(&self) -> Option<usize>;

    fn volatile_level(&self) -> VolatileLevel;
    fn workbook_seed(&self) -> u64;
    fn recalc_epoch(&self) -> u64;
    fn current_cell(&self) -> Option<CellRef>;

    /// Resolve a reference into a RangeView using the underlying engine context.
    fn resolve_range_view<'c>(
        &'c self,
        _reference: &ReferenceType,
        _current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::NImpl))
    }

    /// Get a pre-flattened view of a range if available (Phase 2)
    /// Returns None if not warmed up or not available
    fn get_or_flatten(
        &self,
        _reference: &ReferenceType,
        _prefer_numeric: bool,
    ) -> Option<crate::engine::cache::FlatView> {
        None
    }

    /// Get a pre-built criteria mask if available (Phase 3)
    /// Returns None if not warmed up or not available
    fn get_or_build_mask(&self, _key: &str) -> Option<crate::engine::masks::DenseMask> {
        None
    }

    /// Deterministic RNG seeded for the current evaluation site and function salt.
    fn rng_for_current(&self, fn_salt: u64) -> rand::rngs::SmallRng {
        use crate::rng::{compose_seed, small_rng_from_lanes};
        let (sheet_id, row, col) = self
            .current_cell()
            .map(|c| (c.sheet_id as u32, c.coord.row, c.coord.col))
            .unwrap_or((0, 0, 0));
        // Include epoch only for OnRecalc
        let epoch = match self.volatile_level() {
            VolatileLevel::OnRecalc => self.recalc_epoch(),
            _ => 0,
        };
        let (l0, l1) = compose_seed(self.workbook_seed(), sheet_id, row, col, fn_salt, epoch);
        small_rng_from_lanes(l0, l1)
    }
}

/// Default adapter that wraps an EvaluationContext and provides the narrow FunctionContext.
pub struct DefaultFunctionContext<'a> {
    pub base: &'a dyn EvaluationContext,
    pub current: Option<CellRef>,
}

impl<'a> DefaultFunctionContext<'a> {
    pub fn new(base: &'a dyn EvaluationContext, current: Option<CellRef>) -> Self {
        Self { base, current }
    }
}

impl<'a> FunctionContext for DefaultFunctionContext<'a> {
    fn locale(&self) -> crate::locale::Locale {
        self.base.locale()
    }
    fn timezone(&self) -> &crate::timezone::TimeZoneSpec {
        self.base.timezone()
    }
    fn thread_pool(&self) -> Option<&std::sync::Arc<rayon::ThreadPool>> {
        self.base.thread_pool()
    }
    fn cancellation_token(&self) -> Option<&std::sync::atomic::AtomicBool> {
        self.base.cancellation_token()
    }
    fn chunk_hint(&self) -> Option<usize> {
        self.base.chunk_hint()
    }

    fn volatile_level(&self) -> VolatileLevel {
        self.base.volatile_level()
    }
    fn workbook_seed(&self) -> u64 {
        self.base.workbook_seed()
    }
    fn recalc_epoch(&self) -> u64 {
        self.base.recalc_epoch()
    }
    fn current_cell(&self) -> Option<CellRef> {
        self.current
    }

    fn resolve_range_view<'c>(
        &'c self,
        reference: &ReferenceType,
        current_sheet: &str,
    ) -> Result<RangeView<'c>, ExcelError> {
        self.base.resolve_range_view(reference, current_sheet)
    }

    fn get_or_flatten(
        &self,
        reference: &ReferenceType,
        prefer_numeric: bool,
    ) -> Option<crate::engine::cache::FlatView> {
        self.base.get_or_flatten(reference, prefer_numeric)
    }
}
