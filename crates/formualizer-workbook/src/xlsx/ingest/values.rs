//! Native Arrow value plane. Ephemeral worksheet borrows are copied directly
//! into Arrow, never round-tripped through LiteralValue::Text.
use crate::load_limits::enforce_sheet_dimension_limits;
use crate::xlsx::recalculate::{
    IoError,
    metadata::{SharedStringTable, Styles},
    sheet::events::{Literal, Value},
    unsupported,
};
use formualizer_common::{DateSystem, ExcelErrorKind};
use formualizer_eval::{
    arrow_store::{ArrowSheet, CellIngest, IngestBuilder, OverlayValue, map_error_code},
    engine::WorkbookLoadLimits,
    format::FormatId,
};
use std::borrow::Cow;

pub(in crate::xlsx) struct Context<'a> {
    pub name: &'a str,
    pub source: &'a [u8],
    pub strings: &'a SharedStringTable,
    pub styles: &'a Styles,
    pub dimensions: (usize, usize),
    pub chunk_rows: usize,
    pub date_system: DateSystem,
    pub limits: &'a WorkbookLoadLimits,
}
#[derive(Clone)]
enum Scalar<'a> {
    Empty,
    Number(f64),
    Boolean(bool),
    Error(u8),
    Text(Cow<'a, str>),
}
#[derive(Clone)]
struct RowValue<'a> {
    scalar: Scalar<'a>,
    format: Option<FormatId>,
}
impl Default for RowValue<'_> {
    fn default() -> Self {
        Self {
            scalar: Scalar::Empty,
            format: None,
        }
    }
}
impl RowValue<'_> {
    fn token(&self) -> CellIngest<'_> {
        match &self.scalar {
            Scalar::Empty => CellIngest::Empty,
            Scalar::Number(n) => self.format.map_or(CellIngest::Number(*n), |f| {
                CellIngest::FormattedNumber(*n, f)
            }),
            Scalar::Boolean(b) => CellIngest::Boolean(*b),
            Scalar::Error(e) => CellIngest::ErrorCode(*e),
            Scalar::Text(s) => CellIngest::Text(s),
        }
    }
    fn overlay(&self) -> Option<OverlayValue> {
        Some(match &self.scalar {
            Scalar::Empty => return None,
            Scalar::Number(n) => OverlayValue::Number(*n),
            Scalar::Boolean(b) => OverlayValue::Boolean(*b),
            Scalar::Error(e) => OverlayValue::Error(*e),
            Scalar::Text(s) => OverlayValue::Text(std::sync::Arc::from(s.as_ref())),
        })
    }
}
struct Dense<'a> {
    builder: IngestBuilder,
    row: Vec<RowValue<'a>>,
    touched: Vec<usize>,
    current: usize,
    appended: usize,
    started: bool,
}
impl Dense<'_> {
    fn append(&mut self) -> Result<(), IoError> {
        self.builder
            .append_row_cells_iter(self.row.iter().map(RowValue::token))
            .map_err(load_error)
    }
}
#[derive(Default)]
struct SparseValueBatch {
    cells: Vec<(usize, usize, OverlayValue, Option<FormatId>)>,
    max_row: usize,
}
impl SparseValueBatch {
    fn push(&mut self, sheet: &mut ArrowSheet, row: usize, col: usize, value: &RowValue<'_>) {
        if let Some(overlay) = value.overlay() {
            self.max_row = self.max_row.max(row);
            self.cells.push((row, col, overlay, value.format));
            if self.cells.len() >= 1 << 16 {
                self.flush(sheet);
            }
        }
    }
    fn flush(&mut self, sheet: &mut ArrowSheet) {
        if self.cells.is_empty() {
            return;
        }
        sheet.ensure_row_capacity(self.max_row + 1);
        for (row, col, value, format) in self.cells.drain(..) {
            sheet.set_sparse_overlay_value(row, col, value);
            sheet.set_sparse_overlay_format(row, col, format);
        }
        self.max_row = 0;
    }
}
pub(in crate::xlsx) struct IngestedValues {
    pub arrow: ArrowSheet,
    pub dimensions: (usize, usize),
    pub values_observed: usize,
    pub values_handed_to_engine: usize,
    pub used_sparse_fallback: bool,
}
pub(in crate::xlsx) struct ValueSink<'a> {
    context: Context<'a>,
    dimensions: (usize, usize),
    dense: Option<Dense<'a>>,
    sparse: Option<ArrowSheet>,
    pending: SparseValueBatch,
    values: usize,
    formulas: usize,
    used_sparse: bool,
}
impl<'a> ValueSink<'a> {
    pub(in crate::xlsx) fn new(context: Context<'a>) -> Result<Self, IoError> {
        let dimensions = (context.dimensions.0.max(1), context.dimensions.1.max(1));
        enforce_sheet_dimension_limits(
            "calamine",
            context.name,
            dimensions.0 as u32,
            dimensions.1 as u32,
            context.limits,
        )
        .map_err(load_error)?;
        let sparse = (dimensions.0 as u64).saturating_mul(dimensions.1 as u64)
            > context.limits.max_sheet_logical_cells;
        let dense = (!sparse).then(|| Dense {
            builder: IngestBuilder::new(
                context.name,
                dimensions.1,
                context.chunk_rows,
                context.date_system,
            ),
            row: vec![RowValue::default(); dimensions.1],
            touched: Vec::new(),
            current: 0,
            appended: 0,
            started: false,
        });
        let arrow = sparse.then(|| {
            ArrowSheet::new_sparse_with_date_system(
                context.name,
                dimensions.1,
                dimensions.0,
                context.chunk_rows,
                context.date_system,
            )
        });
        Ok(Self {
            context,
            dimensions,
            dense,
            sparse: arrow,
            pending: SparseValueBatch::default(),
            values: 0,
            formulas: 0,
            used_sparse: sparse,
        })
    }
    fn coordinate(&mut self, row: u32, col: u32) -> Result<(usize, usize), IoError> {
        let row = row
            .checked_sub(1)
            .ok_or_else(|| unsupported("zero native row", self.context.name))?
            as usize;
        let col = col
            .checked_sub(1)
            .ok_or_else(|| unsupported("zero native column", self.context.name))?
            as usize;
        if row < self.dimensions.0 && col < self.dimensions.1 {
            return Ok((row, col));
        }
        self.dimensions.0 = self.dimensions.0.max(row + 1);
        self.dimensions.1 = self.dimensions.1.max(col + 1);
        enforce_sheet_dimension_limits(
            "calamine",
            self.context.name,
            self.dimensions.0 as u32,
            self.dimensions.1 as u32,
            self.context.limits,
        )
        .map_err(load_error)?;
        Ok((row, col))
    }
    fn budget(&self) -> Result<(), IoError> {
        if (self.values as u64).saturating_add(self.formulas as u64)
            > self.context.limits.max_sheet_logical_cells
        {
            return Err(load_error(format!(
                "Workbook load budget exceeded in calamine for sheet {}: observed populated cell count exceeds configured logical-cell budget of {}",
                self.context.name, self.context.limits.max_sheet_logical_cells
            )));
        }
        Ok(())
    }
    pub(in crate::xlsx) fn observe_formula(&mut self, row: u32, col: u32) -> Result<(), IoError> {
        let (row, col) = self.coordinate(row, col)?;
        self.formulas += 1;
        self.budget()?;
        if let Some(dense) = self.dense.as_mut()
            && dense.started
            && dense.current == row
            && let Some(value) = dense.row.get_mut(col)
        {
            *value = RowValue::default();
        }
        Ok(())
    }
    pub(in crate::xlsx) fn push(&mut self, literal: Literal<'_>) -> Result<(), IoError> {
        let (row, col) = self.coordinate(literal.row, literal.col)?;
        let kind = literal.raw_type(self.context.source);
        let style = literal.raw_style(self.context.source);
        let payload = literal.raw_value(self.context.source);
        // Calamine examines t only for v, never for is or an empty c shell.
        if payload.is_some()
            && let Some(kind) = kind
            && !matches!(kind, "s" | "b" | "d" | "e" | "str" | "n")
        {
            return Err(IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(
                crate::xlsx::types::XlsxError::CellTAttribute(kind.to_owned()),
            )));
        }
        let mut format = None;
        let scalar = if kind == Some("s")
            && let Some(payload) = payload
        {
            if payload.is_empty() {
                Scalar::Empty
            } else {
                let index = unsigned_index(payload);
                Scalar::Text(Cow::Borrowed(self.context.strings.get(index)?))
            }
        } else {
            match literal.value {
                Value::Empty => Scalar::Empty,
                Value::Number(n) => {
                    format = self.context.styles.format(style, n);
                    if n.is_finite() {
                        Scalar::Number(n)
                    } else {
                        Scalar::Error(map_error_code(ExcelErrorKind::Num))
                    }
                }
                Value::Boolean(b) => Scalar::Boolean(b),
                Value::Error(e) => Scalar::Error(error_code(e)),
                Value::Text(Cow::Owned(s)) => Scalar::Text(Cow::Owned(s)),
                Value::Text(Cow::Borrowed(s)) => Scalar::Text(self.source_text(s)),
                Value::Shared(index) => {
                    Scalar::Text(Cow::Borrowed(self.context.strings.get(index)?))
                }
                Value::Unreadable => {
                    return Err(unsupported(
                        "unmasked unreadable native value",
                        self.context.name,
                    ));
                }
            }
        };
        self.store(row, col, RowValue { scalar, format })
    }
    /// Already-decoded compatible cells need neither raw attribute spans nor
    /// a second type/index parse. Keep the general scanner entry independent.
    pub(in crate::xlsx) fn push_decoded(
        &mut self,
        row: u32,
        col: u32,
        value: Value<'_>,
        format: Option<FormatId>,
    ) -> Result<(), IoError> {
        let (row, col) = self.coordinate(row, col)?;
        let scalar = match value {
            Value::Empty => Scalar::Empty,
            Value::Number(n) if n.is_finite() => Scalar::Number(n),
            Value::Number(_) => Scalar::Error(map_error_code(ExcelErrorKind::Num)),
            Value::Boolean(b) => Scalar::Boolean(b),
            Value::Error(e) => Scalar::Error(error_code(e)),
            Value::Text(Cow::Owned(s)) => Scalar::Text(Cow::Owned(s)),
            Value::Text(Cow::Borrowed(s)) => Scalar::Text(self.source_text(s)),
            Value::Shared(index) => Scalar::Text(Cow::Borrowed(self.context.strings.get(index)?)),
            Value::Unreadable => {
                return Err(unsupported(
                    "unmasked unreadable native value",
                    self.context.name,
                ));
            }
        };
        self.store(row, col, RowValue { scalar, format })
    }
    fn store(&mut self, row: usize, col: usize, value: RowValue<'a>) -> Result<(), IoError> {
        if matches!(value.scalar, Scalar::Empty) {
            return Ok(());
        }
        self.values += 1;
        self.budget()?;
        if let Some(sheet) = self.sparse.as_mut() {
            self.pending.push(sheet, row, col, &value);
            return Ok(());
        }
        let dense = self
            .dense
            .as_ref()
            .ok_or_else(|| unsupported("missing native dense state", self.context.name))?;
        let gap = if dense.started {
            row.saturating_sub(dense.current)
        } else {
            row
        };
        let fallback = (dense.started && row < dense.current)
            || col >= dense.row.len()
            || gap > 128
            || (dense.appended.saturating_mul(dense.row.len()) as u64)
                > self.context.limits.max_sheet_logical_cells;
        if fallback {
            let mut dense = self
                .dense
                .take()
                .ok_or_else(|| unsupported("missing native dense state", self.context.name))?;
            if dense.started && dense.current == dense.appended {
                dense.append()?;
            }
            let mut sheet = dense.builder.finish();
            sheet.ensure_row_capacity(self.dimensions.0.max(row + 1));
            if col >= sheet.columns.len() {
                sheet.insert_columns(sheet.columns.len(), col + 1 - sheet.columns.len());
            }
            self.pending.push(&mut sheet, row, col, &value);
            self.sparse = Some(sheet);
            self.used_sparse = true;
            return Ok(());
        }
        let dense = self
            .dense
            .as_mut()
            .ok_or_else(|| unsupported("missing native dense state", self.context.name))?;
        if !dense.started {
            while dense.appended < row {
                dense.append()?;
                dense.appended += 1;
            }
            dense.current = row;
            dense.started = true;
        } else if row > dense.current {
            dense.append()?;
            dense.appended += 1;
            for col in dense.touched.drain(..) {
                dense.row[col] = RowValue::default();
            }
            while dense.appended < row {
                dense.append()?;
                dense.appended += 1;
            }
            dense.current = row;
        }
        let target = dense
            .row
            .get_mut(col)
            .ok_or_else(|| unsupported("native column overflow", self.context.name))?;
        if matches!(target.scalar, Scalar::Empty) {
            dense.touched.push(col);
        }
        *target = value;
        Ok(())
    }
    /// Recover the source borrow under its owning context lifetime. The event
    /// callback's short lifetime need not escape; every range is checked and
    /// no raw pointer is dereferenced. Owned inline/entity text moves directly.
    fn source_text(&self, text: &str) -> Cow<'a, str> {
        if text.is_empty() {
            return Cow::Borrowed("");
        }
        let source = self.context.source;
        if let Some(start) = (text.as_ptr() as usize).checked_sub(source.as_ptr() as usize)
            && let Some(end) = start.checked_add(text.len())
            && let Some(bytes) = source.get(start..end)
            && bytes == text.as_bytes()
            && let Ok(value) = std::str::from_utf8(bytes)
        {
            return Cow::Borrowed(value);
        }
        Cow::Owned(text.to_owned())
    }
    pub(in crate::xlsx) fn finish(
        mut self,
        authoritative_bounds: (u32, u32),
    ) -> Result<IngestedValues, IoError> {
        self.dimensions.0 = self.dimensions.0.max(authoritative_bounds.0 as usize);
        self.dimensions.1 = self.dimensions.1.max(authoritative_bounds.1 as usize);
        self.budget()?;
        enforce_sheet_dimension_limits(
            "calamine",
            self.context.name,
            self.dimensions.0 as u32,
            self.dimensions.1 as u32,
            self.context.limits,
        )
        .map_err(load_error)?;
        let mut arrow = if let Some(mut sheet) = self.sparse {
            self.pending.flush(&mut sheet);
            sheet
        } else {
            let mut dense = self
                .dense
                .take()
                .ok_or_else(|| unsupported("missing native dense state", self.context.name))?;
            if dense.started {
                dense.append()?;
            }
            dense.builder.finish()
        };
        arrow.ensure_row_capacity(self.dimensions.0);
        if self.dimensions.1 > arrow.columns.len() {
            arrow.insert_columns(arrow.columns.len(), self.dimensions.1 - arrow.columns.len());
        }
        Ok(IngestedValues {
            arrow,
            dimensions: self.dimensions,
            values_observed: self.values,
            values_handed_to_engine: self.values,
            used_sparse_fallback: self.used_sparse,
        })
    }
}
/// Advisory allocation hint only. No dedicated prefix admission limit exists
/// in today's options: cap the probe at 64 KiB and the existing part limit.
/// Truncation, incomplete UTF-8/XML and unrecognized metadata mean 1x1; the
/// authoritative complete scan still owns every acceptance check and refusal.
pub(in crate::xlsx) fn dimension_hint(
    bytes: &[u8],
    options: &crate::xlsx::recalculate::XlsxRecalculateOptions,
) -> (usize, usize) {
    let length = bytes
        .len()
        .min(options.limits.max_worksheet_bytes)
        .min(64 * 1024);
    let prefix = bytes.get(..length).unwrap_or_default();
    let mut hint = (1, 1);
    // The fused machine no longer exports a worksheet Tokens wrapper. Reuse
    // the validated generic walker only for this bounded pre-sheetData prefix;
    // this is not an additional body/admission walk. Its result is advisory.
    let _ = crate::xlsx::xml::walk(prefix, &options.into(), |path, node| {
        if !matches!(node.kind, crate::xlsx::recalculate::xml::Kind::Open { .. }) {
            return Ok(());
        }
        if crate::xlsx::recalculate::xml::path_is(
            path,
            crate::xlsx::recalculate::xml::MAIN,
            &["worksheet", "sheetData"],
        ) {
            // No-allocation internal stop signal, never surfaced to the caller.
            return Err(IoError::Io(std::io::ErrorKind::Interrupted.into()));
        }
        if crate::xlsx::recalculate::xml::path_is(
            path,
            crate::xlsx::recalculate::xml::MAIN,
            &["worksheet", "dimension"],
        ) && let Some(value) = node.value("ref")
            && let Ok(rect) = crate::xlsx::recalculate::sheet::SourceRect::parse(value)
        {
            hint = (rect.last_row as usize, rect.last_col as usize);
        }
        Ok(())
    });
    hint
}
fn load_error(error: impl std::fmt::Display) -> IoError {
    IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Io(
        std::io::Error::other(error.to_string()),
    ))
}
fn unsigned_index(raw: &[u8]) -> usize {
    if raw.iter().all(u8::is_ascii_digit) {
        std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    } else {
        0
    }
}
#[cfg(test)]
#[path = "values_bulk_tests.rs"]
mod bulk_tests;

fn error_code(error: crate::xlsx::types::CellErrorType) -> u8 {
    let kind = match error {
        crate::xlsx::types::CellErrorType::Div0 => ExcelErrorKind::Div,
        crate::xlsx::types::CellErrorType::NA => ExcelErrorKind::Na,
        crate::xlsx::types::CellErrorType::Name => ExcelErrorKind::Name,
        crate::xlsx::types::CellErrorType::Null => ExcelErrorKind::Null,
        crate::xlsx::types::CellErrorType::Num => ExcelErrorKind::Num,
        crate::xlsx::types::CellErrorType::Ref => ExcelErrorKind::Ref,
        crate::xlsx::types::CellErrorType::Value => ExcelErrorKind::Value,
    };
    map_error_code(kind)
}
