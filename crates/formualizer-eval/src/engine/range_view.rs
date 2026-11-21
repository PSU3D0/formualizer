use crate::arrow_store;
use crate::arrow_store::IngestBuilder;
use crate::engine::CancelToken;
use crate::engine::resource_ledger::{DEFAULT_MATERIALIZED_ARRAY_CELLS, materialized_cells_exceed};
use crate::stripes::NumericChunk;
use arrow_array::Array;
use arrow_schema::DataType;
use formualizer_common::{CoercionPolicy, DateSystem, ExcelError, LiteralValue};
use std::sync::Arc;

#[cfg(test)]
pub(crate) mod range_work {
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, Default, serde::Serialize)]
    pub(crate) struct Work {
        pub iterators: usize,
        pub search_probes: usize,
        pub candidates: usize,
        pub segments: usize,
        pub segment_rows: usize,
        pub generic_columns: usize,
        pub null_arrays: usize,
        pub null_slots: usize,
        pub provider_requests: [usize; 4],
        pub provider_builds: [usize; 4],
        pub provider_slots: [usize; 4],
        pub error_pieces: usize,
        pub error_piece_max_rows: usize,
    }

    thread_local! {
        static WORK: Cell<Option<Work>> = const { Cell::new(None) };
    }

    pub(crate) fn begin() {
        WORK.with(|work| work.set(Some(Work::default())));
    }

    pub(crate) fn take() -> Work {
        WORK.with(|work| work.replace(None).unwrap_or_default())
    }

    #[inline]
    pub(crate) fn record(f: impl FnOnce(&mut Work)) {
        WORK.with(|work| {
            if let Some(mut value) = work.get() {
                f(&mut value);
                work.set(Some(value));
            }
        });
    }
}

#[derive(Clone)]
pub enum RangeBacking<'a> {
    Borrowed(&'a arrow_store::ArrowSheet),
    Owned(Arc<arrow_store::ArrowSheet>),
}

/// Uniform tail values for a [`RangeView`] whose logical shape `(rows, cols)`
/// extends beyond its active core `(active_rows, active_cols)`.
///
/// Layout (relative to the view's origin):
/// - `r < active_rows, c < active_cols`: stored in `RangeView::backing`
/// - `r < active_rows, c >= active_cols`: `right_tail[r]`
/// - `r >= active_rows, c < active_cols`: `bottom_tail[c]`
/// - `r >= active_rows, c >= active_cols`: `corner_tail`
#[derive(Clone, Debug)]
pub(crate) struct RangeTail {
    pub(crate) active_rows: usize,
    pub(crate) active_cols: usize,
    pub(crate) right_tail: Vec<LiteralValue>,
    pub(crate) bottom_tail: Vec<LiteralValue>,
    pub(crate) corner_tail: LiteralValue,
}

impl RangeTail {
    #[inline]
    fn right_tail_slice(&self, row_start: usize, row_len: usize) -> &[LiteralValue] {
        let start = row_start.min(self.right_tail.len());
        let end = row_start.saturating_add(row_len).min(self.right_tail.len());
        &self.right_tail[start..end]
    }
}

/// Numeric value as stored in an Arrow numeric lane (ignoring booleans, text, and blanks).
#[inline]
pub(crate) fn range_numeric_value(v: &LiteralValue, date_system: DateSystem) -> Option<f64> {
    match v {
        LiteralValue::Number(n) => Some(*n),
        LiteralValue::Int(i) => Some(*i as f64),
        LiteralValue::Date(d) => {
            let dt = d.and_hms_opt(0, 0, 0)?;
            Some(formualizer_common::datetime_to_serial_for(date_system, &dt))
        }
        LiteralValue::DateTime(dt) => {
            Some(formualizer_common::datetime_to_serial_for(date_system, dt))
        }
        LiteralValue::Time(t) => Some(formualizer_common::time_to_fraction(t)),
        LiteralValue::Duration(dur) => Some(dur.num_seconds() as f64 / 86_400.0),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum TailValues<'a> {
    Constant(&'a LiteralValue),
    Slice(&'a [LiteralValue]),
}

impl<'a> TailValues<'a> {
    #[inline]
    fn get(&self, idx: usize) -> &'a LiteralValue {
        match self {
            TailValues::Constant(v) => v,
            TailValues::Slice(s) => s.get(idx).unwrap_or(&LiteralValue::Empty),
        }
    }
}

fn tail_numbers_array(
    vals: TailValues<'_>,
    len: usize,
    date_system: DateSystem,
) -> Arc<arrow_array::Float64Array> {
    match vals {
        TailValues::Constant(v) => match range_numeric_value(v, date_system) {
            Some(n) => Arc::new(arrow_array::Float64Array::from(vec![n; len])),
            None => Arc::new(arrow_array::Float64Array::new_null(len)),
        },
        TailValues::Slice(s) => {
            let mut b = arrow_array::builder::Float64Builder::with_capacity(len);
            for i in 0..len {
                match range_numeric_value(s.get(i).unwrap_or(&LiteralValue::Empty), date_system) {
                    Some(n) => b.append_value(n),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

fn tail_booleans_array(vals: TailValues<'_>, len: usize) -> Arc<arrow_array::BooleanArray> {
    match vals {
        TailValues::Constant(LiteralValue::Boolean(b)) => {
            Arc::new(arrow_array::BooleanArray::from(vec![*b; len]))
        }
        TailValues::Constant(_) => Arc::new(arrow_array::BooleanArray::new_null(len)),
        TailValues::Slice(s) => {
            let mut b = arrow_array::builder::BooleanBuilder::with_capacity(len);
            for i in 0..len {
                match s.get(i).unwrap_or(&LiteralValue::Empty) {
                    LiteralValue::Boolean(v) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

fn tail_text_array(vals: TailValues<'_>, len: usize) -> arrow_array::ArrayRef {
    match vals {
        TailValues::Constant(LiteralValue::Text(t)) => {
            Arc::new(arrow_array::StringArray::from(vec![t.as_str(); len]))
        }
        TailValues::Constant(_) => arrow_array::new_null_array(&DataType::Utf8, len),
        TailValues::Slice(s) => {
            let mut b = arrow_array::builder::StringBuilder::with_capacity(len, len * 4);
            for i in 0..len {
                match s.get(i).unwrap_or(&LiteralValue::Empty) {
                    LiteralValue::Text(t) => b.append_value(t),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

fn tail_lowered_text_array(vals: TailValues<'_>, len: usize) -> Arc<arrow_array::StringArray> {
    match vals {
        TailValues::Constant(LiteralValue::Text(t)) => {
            let lowered = t.to_lowercase();
            Arc::new(arrow_array::StringArray::from(vec![lowered.as_str(); len]))
        }
        TailValues::Constant(_) => Arc::new(arrow_array::StringArray::new_null(len)),
        TailValues::Slice(s) => {
            let mut b = arrow_array::builder::StringBuilder::with_capacity(len, len * 4);
            for i in 0..len {
                match s.get(i).unwrap_or(&LiteralValue::Empty) {
                    LiteralValue::Text(t) => b.append_value(t.to_lowercase()),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

fn tail_errors_array(vals: TailValues<'_>, len: usize) -> Arc<arrow_array::UInt8Array> {
    match vals {
        TailValues::Constant(LiteralValue::Error(e)) => {
            let code = arrow_store::map_error_code(e.kind);
            Arc::new(arrow_array::UInt8Array::from(vec![code; len]))
        }
        TailValues::Constant(_) => Arc::new(arrow_array::UInt8Array::new_null(len)),
        TailValues::Slice(s) => {
            let mut b = arrow_array::builder::UInt8Builder::with_capacity(len);
            for i in 0..len {
                match s.get(i).unwrap_or(&LiteralValue::Empty) {
                    LiteralValue::Error(e) => b.append_value(arrow_store::map_error_code(e.kind)),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

fn tail_type_tag(v: &LiteralValue) -> u8 {
    match v {
        LiteralValue::Empty => arrow_store::TypeTag::Empty as u8,
        LiteralValue::Number(_)
        | LiteralValue::Int(_)
        | LiteralValue::Date(_)
        | LiteralValue::DateTime(_)
        | LiteralValue::Time(_)
        | LiteralValue::Duration(_) => arrow_store::TypeTag::Number as u8,
        LiteralValue::Boolean(_) => arrow_store::TypeTag::Boolean as u8,
        LiteralValue::Text(_) => arrow_store::TypeTag::Text as u8,
        LiteralValue::Error(_) | LiteralValue::Array(_) => arrow_store::TypeTag::Error as u8,
        LiteralValue::Pending => arrow_store::TypeTag::Pending as u8,
    }
}

fn tail_type_tags_array(vals: TailValues<'_>, len: usize) -> Arc<arrow_array::UInt8Array> {
    match vals {
        TailValues::Constant(v) => {
            Arc::new(arrow_array::UInt8Array::from(vec![tail_type_tag(v); len]))
        }
        TailValues::Slice(_) => {
            let mut tags = Vec::with_capacity(len);
            for i in 0..len {
                tags.push(tail_type_tag(vals.get(i)));
            }
            Arc::new(arrow_array::UInt8Array::from(tags))
        }
    }
}

/// Unified view over a 2D range with efficient traversal utilities.
/// Phase 4: Arrow-only backing.
#[derive(Clone)]
pub struct RangeView<'a> {
    backing: RangeBacking<'a>,
    sr: usize,
    sc: usize,
    er: usize,
    ec: usize,
    rows: usize,
    cols: usize,
    cancel_token: Option<CancelToken>,
    /// Most cells this view may be materialised into as a dense array
    /// (`ScratchResourceBudget::materialized_array_cells`). Views resolved
    /// through an evaluation context carry the configured value.
    cell_budget: u64,
    /// Optional non-empty uniform tail for computed array views extending beyond
    /// the stored core in `backing`. When `None`, cells outside `backing` are
    /// implicitly `LiteralValue::Empty`.
    tail: Option<Arc<RangeTail>>,
}

impl<'a> core::fmt::Debug for RangeView<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RangeView")
            .field("rows", &self.rows)
            .field("cols", &self.cols)
            .field("kind", &self.kind_probe())
            .finish()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RangeKind {
    Empty,
    NumericOnly,
    TextOnly,
    Mixed,
}

pub struct ChunkCol {
    pub numbers: Option<arrow_array::ArrayRef>,
    pub booleans: Option<arrow_array::ArrayRef>,
    pub text: Option<arrow_array::ArrayRef>,
    pub errors: Option<arrow_array::ArrayRef>,
    pub type_tag: arrow_array::ArrayRef,
}

pub struct ChunkSlice {
    pub row_start: usize, // relative to view top
    pub row_len: usize,
    pub cols: Vec<ChunkCol>,
}

const SYNTHETIC_TAIL_CHUNK_ROWS: usize = 32 * 1024;

enum SegmentKind {
    Backing {
        chunk_idx: usize,
        chunk_offset: usize,
    },
    BottomTail,
}

struct RowSegment {
    kind: SegmentKind,
    row_start: usize,
    row_len: usize,
}

struct RowSegmentIterator<'a> {
    view: &'a RangeView<'a>,
    chunks: Option<core::ops::Range<usize>>,
    next_bottom_row: usize,
}

impl Iterator for RowSegmentIterator<'_> {
    type Item = Result<RowSegment, ExcelError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self
            .view
            .cancel_token
            .as_ref()
            .is_some_and(CancelToken::is_cancelled)
        {
            return Some(Err(ExcelError::new(
                formualizer_common::ExcelErrorKind::Cancelled,
            )));
        }
        let sheet = self.view.sheet();
        let starts = &sheet.chunk_starts;
        let sheet_rows = sheet.nrows as usize;
        let backing_end = match &self.view.tail {
            Some(tail) if tail.active_rows == 0 => None,
            Some(tail) => {
                let active_er = self.view.sr + tail.active_rows - 1;
                Some(
                    active_er
                        .min(self.view.er)
                        .min(sheet_rows.saturating_sub(1)),
                )
            }
            None => (sheet_rows > 0).then(|| self.view.er.min(sheet_rows.saturating_sub(1))),
        };
        let chunks = self.chunks.get_or_insert_with(|| {
            // Match row coverage independently of logical column/zero-sized subview dimensions.
            let Some(row_end) = backing_end else {
                return 0..0;
            };
            if sheet_rows == 0 || starts.is_empty() || self.view.sr > row_end {
                return 0..0;
            }
            let first = starts
                .partition_point(|&start| {
                    #[cfg(test)]
                    range_work::record(|w| w.search_probes += 1);
                    start <= self.view.sr
                })
                .saturating_sub(1);
            let end = starts.partition_point(|&start| {
                #[cfg(test)]
                range_work::record(|w| w.search_probes += 1);
                start <= row_end
            });
            first..end
        });
        if let Some(row_end) = backing_end {
            for ci in chunks.by_ref() {
                #[cfg(test)]
                range_work::record(|w| w.candidates += 1);
                let start = starts[ci];
                let end = starts.get(ci + 1).copied().unwrap_or(sheet_rows);
                let len = end.saturating_sub(start);
                if len == 0 {
                    continue;
                }
                let lo = start.max(self.view.sr);
                let hi = (start + len - 1).min(row_end);
                if lo > hi {
                    continue;
                }
                let row_len = hi - lo + 1;
                #[cfg(test)]
                range_work::record(|w| {
                    w.segments += 1;
                    w.segment_rows += row_len;
                });
                return Some(Ok(RowSegment {
                    kind: SegmentKind::Backing {
                        chunk_idx: ci,
                        chunk_offset: lo - start,
                    },
                    row_start: lo - self.view.sr,
                    row_len,
                }));
            }
        }
        if let Some(tail) = &self.view.tail {
            if self.next_bottom_row < tail.active_rows {
                self.next_bottom_row = tail.active_rows;
            }
            if self.next_bottom_row < self.view.rows {
                let row_start = self.next_bottom_row;
                let row_len = (self.view.rows - row_start).min(SYNTHETIC_TAIL_CHUNK_ROWS);
                self.next_bottom_row += row_len;
                return Some(Ok(RowSegment {
                    kind: SegmentKind::BottomTail,
                    row_start,
                    row_len,
                }));
            }
        }
        None
    }
}

pub struct RowChunkIterator<'a> {
    segments: RowSegmentIterator<'a>,
}

impl Iterator for RowChunkIterator<'_> {
    type Item = Result<ChunkSlice, ExcelError>;

    fn next(&mut self) -> Option<Self::Item> {
        let segment = match self.segments.next()? {
            Ok(segment) => segment,
            Err(error) => return Some(Err(error)),
        };
        let view = self.segments.view;
        let sheet = view.sheet();
        let seg_len = segment.row_len;
        let mut cols = Vec::with_capacity(view.cols);
        match segment.kind {
            SegmentKind::BottomTail => {
                let tail = view.tail.as_deref().expect("BottomTail requires tail");
                for local_c in 0..view.cols {
                    let val = if local_c < tail.active_cols {
                        tail.bottom_tail
                            .get(local_c)
                            .unwrap_or(&LiteralValue::Empty)
                    } else {
                        &tail.corner_tail
                    };
                    let tv = TailValues::Constant(val);
                    cols.push(ChunkCol {
                        numbers: Some(tail_numbers_array(tv, seg_len, sheet.date_system)),
                        booleans: Some(tail_booleans_array(tv, seg_len)),
                        text: Some(tail_text_array(tv, seg_len)),
                        errors: Some(tail_errors_array(tv, seg_len)),
                        type_tag: tail_type_tags_array(tv, seg_len),
                    });
                }
            }
            SegmentKind::Backing {
                chunk_idx: ci,
                chunk_offset: rel_off,
            } => {
                for (local_c, col_idx) in (view.sc..=view.ec).enumerate() {
                    if let Some(tail) = &view.tail
                        && local_c >= tail.active_cols
                    {
                        let slice = tail.right_tail_slice(segment.row_start, seg_len);
                        let tv = TailValues::Slice(slice);
                        cols.push(ChunkCol {
                            numbers: Some(tail_numbers_array(tv, seg_len, sheet.date_system)),
                            booleans: Some(tail_booleans_array(tv, seg_len)),
                            text: Some(tail_text_array(tv, seg_len)),
                            errors: Some(tail_errors_array(tv, seg_len)),
                            type_tag: tail_type_tags_array(tv, seg_len),
                        });
                        continue;
                    }
                    #[cfg(test)]
                    range_work::record(|w| w.generic_columns += 1);
                    if col_idx >= sheet.columns.len() {
                        #[cfg(test)]
                        range_work::record(|w| {
                            w.null_arrays += 4;
                            w.null_slots += 4 * seg_len;
                        });
                        let numbers =
                            Some(arrow_array::new_null_array(&DataType::Float64, seg_len));
                        let booleans =
                            Some(arrow_array::new_null_array(&DataType::Boolean, seg_len));
                        let text = Some(arrow_array::new_null_array(&DataType::Utf8, seg_len));
                        let errors = Some(arrow_array::new_null_array(&DataType::UInt8, seg_len));
                        let type_tag: arrow_array::ArrayRef =
                            Arc::new(arrow_array::UInt8Array::from(vec![
                                arrow_store::TypeTag::Empty
                                    as u8;
                                seg_len
                            ]));
                        cols.push(ChunkCol {
                            numbers,
                            booleans,
                            text,
                            errors,
                            type_tag,
                        });
                    } else {
                        let col = &sheet.columns[col_idx];
                        let Some(ch) = col.chunk(ci) else {
                            #[cfg(test)]
                            range_work::record(|w| {
                                w.null_arrays += 4;
                                w.null_slots += 4 * seg_len;
                            });
                            let numbers =
                                Some(arrow_array::new_null_array(&DataType::Float64, seg_len));
                            let booleans =
                                Some(arrow_array::new_null_array(&DataType::Boolean, seg_len));
                            let text = Some(arrow_array::new_null_array(&DataType::Utf8, seg_len));
                            let errors =
                                Some(arrow_array::new_null_array(&DataType::UInt8, seg_len));
                            let type_tag: arrow_array::ArrayRef =
                                Arc::new(arrow_array::UInt8Array::from(vec![
                                    arrow_store::TypeTag::Empty
                                        as u8;
                                    seg_len
                                ]));
                            cols.push(ChunkCol {
                                numbers,
                                booleans,
                                text,
                                errors,
                                type_tag,
                            });
                            continue;
                        };

                        let numbers_base: arrow_array::ArrayRef = ch.numbers_or_null();
                        let booleans_base: arrow_array::ArrayRef = ch.booleans_or_null();
                        let text_base: arrow_array::ArrayRef = ch.text_or_null();
                        let errors_base: arrow_array::ArrayRef = ch.errors_or_null();

                        let numbers = Some(numbers_base.slice(rel_off, seg_len));
                        let booleans = Some(booleans_base.slice(rel_off, seg_len));
                        let text = Some(text_base.slice(rel_off, seg_len));
                        let errors = Some(errors_base.slice(rel_off, seg_len));
                        let type_tag: arrow_array::ArrayRef =
                            Arc::new(ch.type_tag.slice(rel_off, seg_len));
                        cols.push(ChunkCol {
                            numbers,
                            booleans,
                            text,
                            errors,
                            type_tag,
                        });
                    }
                }
            }
        }
        Some(Ok(ChunkSlice {
            row_start: segment.row_start,
            row_len: seg_len,
            cols,
        }))
    }
}

impl<'a> RangeView<'a> {
    pub(crate) fn new(
        backing: RangeBacking<'a>,
        sr: usize,
        sc: usize,
        er: usize,
        ec: usize,
        rows: usize,
        cols: usize,
    ) -> Self {
        Self {
            backing,
            sr,
            sc,
            er,
            ec,
            rows,
            cols,
            cancel_token: None,
            cell_budget: DEFAULT_MATERIALIZED_ARRAY_CELLS,
            tail: None,
        }
    }

    /// Build an owned `RangeView` of logical dimensions `(rows, cols)` from an
    /// active core `core_sheet` (`active_rows x active_cols`) plus uniform tails.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_core_and_tail(
        rows: usize,
        cols: usize,
        active_rows: usize,
        active_cols: usize,
        core_sheet: Arc<arrow_store::ArrowSheet>,
        right_tail: Vec<LiteralValue>,
        bottom_tail: Vec<LiteralValue>,
        corner_tail: LiteralValue,
        cancel_token: Option<CancelToken>,
        cell_budget: u64,
    ) -> RangeView<'static> {
        if rows == 0 || cols == 0 {
            return RangeView {
                backing: RangeBacking::Owned(core_sheet),
                sr: 1,
                sc: 1,
                er: 0,
                ec: 0,
                rows: 0,
                cols: 0,
                cancel_token,
                cell_budget,
                tail: None,
            };
        }
        let has_tail = active_rows < rows || active_cols < cols;
        let tail_all_empty = !has_tail
            || (right_tail.iter().all(|v| matches!(v, LiteralValue::Empty))
                && bottom_tail.iter().all(|v| matches!(v, LiteralValue::Empty))
                && (active_rows == rows
                    || active_cols == cols
                    || matches!(corner_tail, LiteralValue::Empty)));
        let tail = if tail_all_empty {
            None
        } else {
            Some(Arc::new(RangeTail {
                active_rows,
                active_cols,
                right_tail,
                bottom_tail,
                corner_tail,
            }))
        };
        RangeView {
            backing: RangeBacking::Owned(core_sheet),
            sr: 0,
            sc: 0,
            er: rows - 1,
            ec: cols - 1,
            rows,
            cols,
            cancel_token,
            cell_budget,
            tail,
        }
    }

    #[inline]
    pub(crate) fn tail(&self) -> Option<&RangeTail> {
        self.tail.as_deref()
    }

    /// Dimensions `(active_rows, active_cols)` of the non-uniform prefix stored in `backing`.
    /// Every cell outside `(0..active_rows, 0..active_cols)` is uniform along each trailing axis
    /// (either implicitly `Empty` when `tail` is `None`, or given by `RangeTail`).
    pub(crate) fn active_dims(&self) -> (usize, usize) {
        if let Some(tail) = &self.tail {
            return (tail.active_rows, tail.active_cols);
        }
        if self.rows == 0 || self.cols == 0 {
            return (0, 0);
        }
        let sheet = self.sheet();
        let sheet_rows = sheet.nrows as usize;
        let max_cols = self.cols.min(sheet.columns.len().saturating_sub(self.sc));
        let max_rows = self.rows.min(sheet_rows.saturating_sub(self.sr));
        if max_rows == 0 || max_cols == 0 {
            return (0, 0);
        }
        if max_cols <= 64 {
            let mut col_end_row = 0usize;
            for col_idx in self.sc..(self.sc + max_cols) {
                let col = &sheet.columns[col_idx];
                let dense_last = col.chunks.len().checked_sub(1);
                let sparse_last = col.sparse_chunks.keys().copied().max();
                let last_ci = match (dense_last, sparse_last) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (Some(a), None) | (None, Some(a)) => Some(a),
                    (None, None) => None,
                };
                if let Some(ci) = last_ci {
                    let chunk_end = sheet
                        .chunk_starts
                        .get(ci + 1)
                        .copied()
                        .unwrap_or(sheet_rows);
                    col_end_row = col_end_row.max(chunk_end);
                }
            }
            let active_rows = max_rows.min(col_end_row.saturating_sub(self.sr));
            if active_rows == 0 {
                return (0, 0);
            }
            return (active_rows, max_cols);
        }
        (max_rows, max_cols)
    }

    #[inline]
    pub(crate) fn has_uniform_tail(&self) -> bool {
        self.active_dims() != (self.rows, self.cols)
    }

    #[inline]
    pub(crate) fn right_tail_cell(&self, r: usize) -> LiteralValue {
        match &self.tail {
            Some(tail) => tail
                .right_tail
                .get(r)
                .cloned()
                .unwrap_or(LiteralValue::Empty),
            None => LiteralValue::Empty,
        }
    }

    #[inline]
    pub(crate) fn bottom_tail_cell(&self, c: usize) -> LiteralValue {
        match &self.tail {
            Some(tail) => tail
                .bottom_tail
                .get(c)
                .cloned()
                .unwrap_or(LiteralValue::Empty),
            None => LiteralValue::Empty,
        }
    }

    #[inline]
    pub(crate) fn corner_tail_cell(&self) -> LiteralValue {
        match &self.tail {
            Some(tail) => tail.corner_tail.clone(),
            None => LiteralValue::Empty,
        }
    }

    /// View restricted to the active core `(0..active_rows, 0..active_cols)` with `tail: None`.
    pub(crate) fn core_sub_view(&self) -> RangeView<'a> {
        let (ar, ac) = self.active_dims();
        if ar == 0 || ac == 0 {
            return RangeView {
                backing: match &self.backing {
                    RangeBacking::Borrowed(s) => RangeBacking::Borrowed(s),
                    RangeBacking::Owned(s) => RangeBacking::Owned(s.clone()),
                },
                sr: 1,
                sc: 1,
                er: 0,
                ec: 0,
                rows: 0,
                cols: 0,
                cancel_token: self.cancel_token.clone(),
                cell_budget: self.cell_budget,
                tail: None,
            };
        }
        RangeView {
            backing: match &self.backing {
                RangeBacking::Borrowed(s) => RangeBacking::Borrowed(s),
                RangeBacking::Owned(s) => RangeBacking::Owned(s.clone()),
            },
            sr: self.sr,
            sc: self.sc,
            er: self.sr + ar - 1,
            ec: self.sc + ac - 1,
            rows: ar,
            cols: ac,
            cancel_token: self.cancel_token.clone(),
            cell_budget: self.cell_budget,
            tail: None,
        }
    }

    /// Visits each uniform tail entry `(value, cell_count)` outside the active core.
    /// Does nothing when `active_dims() == dims()`.
    pub(crate) fn for_each_tail_entry(
        &self,
        mut f: impl FnMut(&LiteralValue, u64) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        let (ar, ac) = self.active_dims();
        let tail_rows = (self.rows - ar) as u64;
        let tail_cols = (self.cols - ac) as u64;
        if tail_rows == 0 && tail_cols == 0 {
            return Ok(());
        }
        if let Some(tail) = &self.tail {
            if tail_cols > 0 {
                for r in 0..ar {
                    let v = tail.right_tail.get(r).unwrap_or(&LiteralValue::Empty);
                    f(v, tail_cols)?;
                }
            }
            if tail_rows > 0 {
                for c in 0..ac {
                    let v = tail.bottom_tail.get(c).unwrap_or(&LiteralValue::Empty);
                    f(v, tail_rows)?;
                }
            }
            if tail_rows > 0 && tail_cols > 0 {
                f(&tail.corner_tail, tail_rows.saturating_mul(tail_cols))?;
            }
        } else {
            let empty_count = (self.rows as u64)
                .saturating_mul(self.cols as u64)
                .saturating_sub((ar as u64).saturating_mul(ac as u64));
            if empty_count > 0 {
                f(&LiteralValue::Empty, empty_count)?;
            }
        }
        Ok(())
    }

    /// Attaches a shared cancellation handle to cancellation-aware range walks.
    ///
    /// Cloning a [`CancelToken`] shares its signal without allocating. Retrieve
    /// a context token once before a hot loop and poll
    /// [`CancelToken::is_cancelled`] periodically.
    #[must_use]
    pub fn with_cancel_token(mut self, token: Option<CancelToken>) -> Self {
        self.cancel_token = token;
        self
    }

    /// Attaches the materialisation cell budget of the evaluation context.
    #[must_use]
    pub(crate) fn with_cell_budget(mut self, cells: u64) -> Self {
        self.cell_budget = cells;
        self
    }

    /// `#NUM!` when this view has more cells than its materialisation budget
    /// allows. Call once before copying the view into a dense array.
    #[inline]
    pub(crate) fn materialization_error(&self) -> Option<ExcelError> {
        materialized_cells_exceed(self.rows, self.cols, self.cell_budget)
    }

    /// `#NUM!` when a `rows x cols` array built from this view (its shape or a
    /// derived one) exceeds the view's materialisation budget.
    #[inline]
    pub(crate) fn shape_materialization_error(
        &self,
        rows: usize,
        cols: usize,
    ) -> Option<ExcelError> {
        materialized_cells_exceed(rows, cols, self.cell_budget)
    }

    #[inline]
    pub(crate) fn criteria_cache_cancelled(&self) -> bool {
        self.cancel_token
            .as_ref()
            .is_some_and(CancelToken::is_cancelled)
    }

    #[inline]
    fn poll_axis_cancelled(&self, step: usize) -> Result<(), ExcelError> {
        if step & 4095 == 0 && self.criteria_cache_cancelled() {
            return Err(ExcelError::new(
                formualizer_common::ExcelErrorKind::Cancelled,
            ));
        }
        Ok(())
    }

    pub fn sheet(&self) -> &arrow_store::ArrowSheet {
        match &self.backing {
            RangeBacking::Borrowed(s) => s,
            RangeBacking::Owned(s) => s,
        }
    }

    pub fn from_owned_rows(
        rows: Vec<Vec<LiteralValue>>,
        date_system: DateSystem,
    ) -> RangeView<'static> {
        Self::try_from_owned_rows(rows, date_system, None)
            .expect("uncancelled RangeView conversion")
    }

    pub(crate) fn try_from_owned_rows(
        rows: Vec<Vec<LiteralValue>>,
        date_system: DateSystem,
        cancel_token: Option<CancelToken>,
    ) -> Result<RangeView<'static>, ExcelError> {
        let nrows = rows.len();
        let ncols = rows.iter().map(|r| r.len()).max().unwrap_or(0);

        let chunk_rows = 32 * 1024;
        let mut ib = IngestBuilder::new("__tmp", ncols, chunk_rows, date_system);

        for mut r in rows {
            if cancel_token.as_ref().is_some_and(CancelToken::is_cancelled) {
                return Err(ExcelError::new(
                    formualizer_common::ExcelErrorKind::Cancelled,
                ));
            }
            r.resize(ncols, LiteralValue::Empty);
            ib.append_row(&r).expect("append_row for RangeView");
        }

        let sheet = Arc::new(ib.finish());

        if nrows == 0 || ncols == 0 {
            return Ok(RangeView {
                backing: RangeBacking::Owned(sheet),
                sr: 1,
                sc: 1,
                er: 0,
                ec: 0,
                rows: 0,
                cols: 0,
                cancel_token,
                cell_budget: DEFAULT_MATERIALIZED_ARRAY_CELLS,
                tail: None,
            });
        }

        Ok(RangeView {
            backing: RangeBacking::Owned(sheet),
            sr: 0,
            sc: 0,
            er: nrows - 1,
            ec: ncols - 1,
            rows: nrows,
            cols: ncols,
            cancel_token,
            cell_budget: DEFAULT_MATERIALIZED_ARRAY_CELLS,
            tail: None,
        })
    }

    pub fn dims(&self) -> (usize, usize) {
        (self.rows, self.cols)
    }

    pub fn expand_to(&self, rows: usize, cols: usize) -> RangeView<'a> {
        if self.tail.is_some() {
            if rows == 0 || cols == 0 {
                return self.sub_view(0, 0, 0, 0);
            }
            if rows <= self.rows && cols <= self.cols {
                return self.sub_view(0, 0, rows, cols);
            }
            let r_ov = rows.min(self.rows);
            let c_ov = cols.min(self.cols);
            let ov = self.sub_view(0, 0, r_ov, c_ov);
            let (ar, ac) = ov.active_dims();
            let row_tail_non_empty = rows > r_ov
                && ar < r_ov
                && ((0..ac).any(|c| !matches!(ov.bottom_tail_cell(c), LiteralValue::Empty))
                    || (ac < c_ov && !matches!(ov.corner_tail_cell(), LiteralValue::Empty)));
            let col_tail_non_empty = cols > c_ov
                && ac < c_ov
                && ((0..ar).any(|r| !matches!(ov.right_tail_cell(r), LiteralValue::Empty))
                    || (ar < r_ov && !matches!(ov.corner_tail_cell(), LiteralValue::Empty)));

            if !row_tail_non_empty && !col_tail_non_empty {
                let right_tail = if ac < cols && ar > 0 {
                    if cols > c_ov {
                        vec![LiteralValue::Empty; ar]
                    } else {
                        (0..ar).map(|r| ov.right_tail_cell(r)).collect()
                    }
                } else {
                    Vec::new()
                };
                let bottom_tail = if ar < rows && ac > 0 {
                    if rows > r_ov {
                        vec![LiteralValue::Empty; ac]
                    } else {
                        (0..ac).map(|c| ov.bottom_tail_cell(c)).collect()
                    }
                } else {
                    Vec::new()
                };
                let corner_tail = LiteralValue::Empty;
                let tail_all_empty = (ar == rows && ac == cols)
                    || (right_tail.iter().all(|v| matches!(v, LiteralValue::Empty))
                        && bottom_tail.iter().all(|v| matches!(v, LiteralValue::Empty)));
                let backing_clean_outside_active = (ar == rows
                    || ov.sr + ar >= ov.sheet().nrows as usize)
                    && (ac == cols || ov.sc + ac >= ov.sheet().columns.len());
                let tail = if tail_all_empty && backing_clean_outside_active {
                    None
                } else {
                    Some(Arc::new(RangeTail {
                        active_rows: ar,
                        active_cols: ac,
                        right_tail,
                        bottom_tail,
                        corner_tail,
                    }))
                };
                return RangeView {
                    backing: match &ov.backing {
                        RangeBacking::Borrowed(s) => RangeBacking::Borrowed(s),
                        RangeBacking::Owned(s) => RangeBacking::Owned(s.clone()),
                    },
                    sr: ov.sr,
                    sc: ov.sc,
                    er: ov.sr + rows - 1,
                    ec: ov.sc + cols - 1,
                    rows,
                    cols,
                    cancel_token: self.cancel_token.clone(),
                    cell_budget: self.cell_budget,
                    tail,
                };
            }

            // Growing past a non-empty uniform tail along rows and/or cols requires
            // materializing the overlap up to the non-empty tail boundary so cells
            // beyond `(r_ov, c_ov)` remain `Empty` instead of extrapolating the tail.
            let new_ar = if row_tail_non_empty { r_ov } else { ar };
            let new_ac = if col_tail_non_empty { c_ov } else { ac };
            let date_system = self.sheet().date_system;
            let mut ib = IngestBuilder::new("__tmp", new_ac, 32 * 1024, date_system);
            if new_ar > 0 && new_ac > 0 {
                let mut row_buf = Vec::with_capacity(new_ac);
                for r in 0..new_ar {
                    row_buf.clear();
                    for c in 0..new_ac {
                        row_buf.push(ov.get_cell(r, c));
                    }
                    ib.append_row(&row_buf)
                        .expect("append_row for expand_to tailed RangeView");
                }
            }
            let mut core_sheet = ib.finish();
            if new_ar > 0 && new_ac == 0 {
                core_sheet.ensure_row_capacity(new_ar);
            }
            let right_tail = if new_ac < cols && new_ar > 0 {
                if cols > c_ov {
                    vec![LiteralValue::Empty; new_ar]
                } else {
                    (0..new_ar).map(|r| ov.get_cell(r, new_ac)).collect()
                }
            } else {
                Vec::new()
            };
            let bottom_tail = if new_ar < rows && new_ac > 0 {
                if rows > r_ov {
                    vec![LiteralValue::Empty; new_ac]
                } else {
                    (0..new_ac).map(|c| ov.get_cell(new_ar, c)).collect()
                }
            } else {
                Vec::new()
            };
            return RangeView::from_core_and_tail(
                rows,
                cols,
                new_ar,
                new_ac,
                Arc::new(core_sheet),
                right_tail,
                bottom_tail,
                LiteralValue::Empty,
                self.cancel_token.clone(),
                self.cell_budget,
            );
        }
        let er = self.sr + rows.saturating_sub(1);
        let ec = self.sc + cols.saturating_sub(1);
        RangeView {
            backing: match &self.backing {
                RangeBacking::Borrowed(s) => RangeBacking::Borrowed(s),
                RangeBacking::Owned(s) => RangeBacking::Owned(s.clone()),
            },
            sr: self.sr,
            sc: self.sc,
            er,
            ec,
            rows,
            cols,
            cancel_token: self.cancel_token.clone(),
            cell_budget: self.cell_budget,
            tail: None,
        }
    }

    pub fn sub_view(&self, rs: usize, cs: usize, rows: usize, cols: usize) -> RangeView<'a> {
        if rows == 0 || cols == 0 {
            return RangeView {
                backing: match &self.backing {
                    RangeBacking::Borrowed(s) => RangeBacking::Borrowed(s),
                    RangeBacking::Owned(s) => RangeBacking::Owned(s.clone()),
                },
                sr: 1,
                sc: 1,
                er: 0,
                ec: 0,
                rows: 0,
                cols: 0,
                cancel_token: self.cancel_token.clone(),
                cell_budget: self.cell_budget,
                tail: None,
            };
        }
        let abs_sr = self.sr + rs;
        let abs_sc = self.sc + cs;
        let er = abs_sr + rows.saturating_sub(1);
        let ec = abs_sc + cols.saturating_sub(1);
        let tail = self.tail.as_ref().and_then(|t| {
            let new_ar = t.active_rows.saturating_sub(rs).min(rows);
            let new_ac = t.active_cols.saturating_sub(cs).min(cols);
            if new_ar == rows && new_ac == cols {
                return None;
            }
            let right_tail = if new_ac < cols && new_ar > 0 {
                t.right_tail
                    .get(rs..rs + new_ar)
                    .map(|s| s.to_vec())
                    .unwrap_or_else(|| vec![LiteralValue::Empty; new_ar])
            } else {
                Vec::new()
            };
            let bottom_tail = if new_ar < rows && new_ac > 0 {
                t.bottom_tail
                    .get(cs..cs + new_ac)
                    .map(|s| s.to_vec())
                    .unwrap_or_else(|| vec![LiteralValue::Empty; new_ac])
            } else {
                Vec::new()
            };
            let corner_tail = if new_ar < rows && new_ac < cols {
                t.corner_tail.clone()
            } else {
                LiteralValue::Empty
            };
            Some(Arc::new(RangeTail {
                active_rows: new_ar,
                active_cols: new_ac,
                right_tail,
                bottom_tail,
                corner_tail,
            }))
        });
        RangeView {
            backing: match &self.backing {
                RangeBacking::Borrowed(s) => RangeBacking::Borrowed(s),
                RangeBacking::Owned(s) => RangeBacking::Owned(s.clone()),
            },
            sr: abs_sr,
            sc: abs_sc,
            er,
            ec,
            rows,
            cols,
            cancel_token: self.cancel_token.clone(),
            cell_budget: self.cell_budget,
            tail,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows == 0 || self.cols == 0
    }

    /// Absolute 0-based start row of this view.
    pub fn start_row(&self) -> usize {
        self.sr
    }
    /// Absolute 0-based end row of this view (inclusive).
    pub fn end_row(&self) -> usize {
        self.er
    }
    /// Absolute 0-based start column of this view.
    pub fn start_col(&self) -> usize {
        self.sc
    }
    /// Absolute 0-based end column of this view (inclusive).
    pub fn end_col(&self) -> usize {
        self.ec
    }
    /// Whether this view reads a workbook sheet, as opposed to rows owned by
    /// the view itself (array results, and names bound to a constant or a
    /// formula), whose backing is a temporary sheet with no cell addresses.
    pub(crate) fn is_sheet_backed(&self) -> bool {
        matches!(self.backing, RangeBacking::Borrowed(_))
    }

    /// Owning sheet name.
    pub fn sheet_name(&self) -> &str {
        &self.sheet().name
    }

    pub fn kind_probe(&self) -> RangeKind {
        if self.is_empty() {
            return RangeKind::Empty;
        }

        let mut has_num = false;
        let mut has_text = false;
        let mut is_mixed = false;
        let (ar, ac) = self.active_dims();

        for r in 0..ar {
            for c in 0..ac {
                match self.get_cell(r, c) {
                    LiteralValue::Empty => {}
                    LiteralValue::Number(_) | LiteralValue::Int(_) => has_num = true,
                    LiteralValue::Text(_) => has_text = true,
                    _ => return RangeKind::Mixed,
                }
                if has_num && has_text {
                    return RangeKind::Mixed;
                }
            }
        }
        let _ = self.for_each_tail_entry(|v, _| {
            match v {
                LiteralValue::Empty => {}
                LiteralValue::Number(_) | LiteralValue::Int(_) => has_num = true,
                LiteralValue::Text(_) => has_text = true,
                _ => is_mixed = true,
            }
            Ok(())
        });
        if is_mixed {
            return RangeKind::Mixed;
        }

        match (has_num, has_text) {
            (false, false) => RangeKind::Empty,
            (true, false) => RangeKind::NumericOnly,
            (false, true) => RangeKind::TextOnly,
            (true, true) => RangeKind::Mixed,
        }
    }

    pub fn as_1x1(&self) -> Option<LiteralValue> {
        if self.rows == 1 && self.cols == 1 {
            Some(self.get_cell(0, 0))
        } else {
            None
        }
    }

    /// Get a specific cell by row and column index (0-based).
    /// Returns Empty for out-of-bounds access.
    pub fn get_cell(&self, row: usize, col: usize) -> LiteralValue {
        if row >= self.rows || col >= self.cols {
            return LiteralValue::Empty;
        }
        if let Some(tail) = &self.tail {
            match (row < tail.active_rows, col < tail.active_cols) {
                (true, true) => {}
                (true, false) => {
                    return tail
                        .right_tail
                        .get(row)
                        .cloned()
                        .unwrap_or(LiteralValue::Empty);
                }
                (false, true) => {
                    return tail
                        .bottom_tail
                        .get(col)
                        .cloned()
                        .unwrap_or(LiteralValue::Empty);
                }
                (false, false) => return tail.corner_tail.clone(),
            }
        }
        let abs_row = self.sr + row;
        let abs_col = self.sc + col;
        let sheet = self.sheet();
        let sheet_rows = sheet.nrows as usize;
        if abs_row >= sheet_rows {
            return LiteralValue::Empty;
        }
        if abs_col >= sheet.columns.len() {
            return LiteralValue::Empty;
        }
        let col_ref = &sheet.columns[abs_col];
        // Locate chunk by binary searching start offsets
        let chunk_starts = &sheet.chunk_starts;
        let ch_idx = match chunk_starts.binary_search(&abs_row) {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        };
        let Some(ch) = col_ref.chunk(ch_idx) else {
            return LiteralValue::Empty;
        };
        let row_start = chunk_starts[ch_idx];
        let in_off = abs_row - row_start;
        // Overlay takes precedence: user edits over computed over base.
        let cascade = arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
        if let Some(ov) = cascade.get_scalar(in_off) {
            return ov.to_literal_for(sheet.date_system);
        }
        // Read tag and route to lane
        let tag_u8 = ch.type_tag.value(in_off);
        match arrow_store::TypeTag::from_u8(tag_u8) {
            arrow_store::TypeTag::Empty => LiteralValue::Empty,
            arrow_store::TypeTag::Number => {
                if let Some(arr) = &ch.numbers {
                    if arr.is_null(in_off) {
                        return LiteralValue::Empty;
                    }
                    LiteralValue::Number(arr.value(in_off))
                } else {
                    LiteralValue::Empty
                }
            }
            arrow_store::TypeTag::DateTime | arrow_store::TypeTag::Duration => {
                if let Some(arr) = &ch.numbers {
                    if arr.is_null(in_off) {
                        LiteralValue::Empty
                    } else {
                        LiteralValue::Number(arr.value(in_off))
                    }
                } else {
                    LiteralValue::Empty
                }
            }
            arrow_store::TypeTag::Boolean => {
                if let Some(arr) = &ch.booleans {
                    if arr.is_null(in_off) {
                        return LiteralValue::Empty;
                    }
                    LiteralValue::Boolean(arr.value(in_off))
                } else {
                    LiteralValue::Empty
                }
            }
            arrow_store::TypeTag::Text => {
                if let Some(arr) = &ch.text {
                    if arr.is_null(in_off) {
                        return LiteralValue::Empty;
                    }
                    let sa = arr
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap();
                    LiteralValue::Text(sa.value(in_off).to_string())
                } else {
                    LiteralValue::Empty
                }
            }
            arrow_store::TypeTag::Error => {
                if let Some(arr) = &ch.errors {
                    if arr.is_null(in_off) {
                        return LiteralValue::Empty;
                    }
                    let kind = arrow_store::unmap_error_code(arr.value(in_off));
                    LiteralValue::Error(ExcelError::new(kind))
                } else {
                    LiteralValue::Empty
                }
            }
            arrow_store::TypeTag::Pending => LiteralValue::Pending,
        }
    }

    /// Iterate overlapping chunks by row segment.
    pub fn iter_row_chunks(&self) -> RowChunkIterator<'_> {
        RowChunkIterator {
            segments: self.iter_row_segments(),
        }
    }

    fn iter_row_segments(&self) -> RowSegmentIterator<'_> {
        #[cfg(test)]
        range_work::record(|w| w.iterators += 1);
        RowSegmentIterator {
            view: self,
            chunks: None,
            next_bottom_row: 0,
        }
    }

    #[inline]
    fn read_chunk_cell(
        ch: &arrow_store::ColumnChunk,
        tags: &[u8],
        numbers: Option<&arrow_array::Float64Array>,
        booleans: Option<&arrow_array::BooleanArray>,
        text: Option<&arrow_array::StringArray>,
        errors: Option<&arrow_array::UInt8Array>,
        in_off: usize,
        has_overlay: bool,
        date_system: DateSystem,
    ) -> LiteralValue {
        if has_overlay {
            let cascade = arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
            if let Some(ov) = cascade.get_scalar(in_off) {
                return ov.to_literal_for(date_system);
            }
        }
        match arrow_store::TypeTag::from_u8(tags[in_off]) {
            arrow_store::TypeTag::Empty => LiteralValue::Empty,
            arrow_store::TypeTag::Number
            | arrow_store::TypeTag::DateTime
            | arrow_store::TypeTag::Duration => match numbers {
                Some(arr) if !arr.is_null(in_off) => LiteralValue::Number(arr.value(in_off)),
                _ => LiteralValue::Empty,
            },
            arrow_store::TypeTag::Boolean => match booleans {
                Some(arr) if !arr.is_null(in_off) => LiteralValue::Boolean(arr.value(in_off)),
                _ => LiteralValue::Empty,
            },
            arrow_store::TypeTag::Text => match text {
                Some(sa) if !sa.is_null(in_off) => LiteralValue::Text(sa.value(in_off).to_string()),
                _ => LiteralValue::Empty,
            },
            arrow_store::TypeTag::Error => match errors {
                Some(arr) if !arr.is_null(in_off) => LiteralValue::Error(ExcelError::new(
                    arrow_store::unmap_error_code(arr.value(in_off)),
                )),
                _ => LiteralValue::Empty,
            },
            arrow_store::TypeTag::Pending => LiteralValue::Pending,
        }
    }

    /// Visit every cell along a 1D lookup axis chunk-by-chunk, avoiding per-cell
    /// chunk binary searches and redundant Arrow array downcasts.
    pub(crate) fn visit_axis_values(
        &self,
        axis: crate::engine::lookup_index_cache::LookupAxis,
        f: &mut impl FnMut(usize, LiteralValue) -> std::ops::ControlFlow<()>,
    ) -> Result<(), ExcelError> {
        use crate::engine::lookup_index_cache::LookupAxis;
        use std::ops::ControlFlow;

        if let Some(tail) = &self.tail {
            let ar = tail.active_rows.min(self.rows);
            let ac = tail.active_cols.min(self.cols);
            match axis {
                LookupAxis::ColumnInView(col) => {
                    if col >= self.cols || self.rows == 0 {
                        return Ok(());
                    }
                    if col < ac {
                        if ar > 0
                            && self
                                .core_sub_view()
                                .visit_axis_values_backing(LookupAxis::ColumnInView(col), f)?
                        {
                            return Ok(());
                        }
                        let tail_val = tail.bottom_tail.get(col).unwrap_or(&LiteralValue::Empty);
                        for r in ar..self.rows {
                            if (r - ar) & 4095 == 0 && self.criteria_cache_cancelled() {
                                return Err(ExcelError::new(
                                    formualizer_common::ExcelErrorKind::Cancelled,
                                ));
                            }
                            if let ControlFlow::Break(()) = f(r, tail_val.clone()) {
                                return Ok(());
                            }
                        }
                    } else {
                        for r in 0..ar {
                            if r & 4095 == 0 && self.criteria_cache_cancelled() {
                                return Err(ExcelError::new(
                                    formualizer_common::ExcelErrorKind::Cancelled,
                                ));
                            }
                            let val = tail
                                .right_tail
                                .get(r)
                                .cloned()
                                .unwrap_or(LiteralValue::Empty);
                            if let ControlFlow::Break(()) = f(r, val) {
                                return Ok(());
                            }
                        }
                        for r in ar..self.rows {
                            if (r - ar) & 4095 == 0 && self.criteria_cache_cancelled() {
                                return Err(ExcelError::new(
                                    formualizer_common::ExcelErrorKind::Cancelled,
                                ));
                            }
                            if let ControlFlow::Break(()) = f(r, tail.corner_tail.clone()) {
                                return Ok(());
                            }
                        }
                    }
                    return Ok(());
                }
                LookupAxis::RowInView(row) => {
                    if row >= self.rows || self.cols == 0 {
                        return Ok(());
                    }
                    if row < ar {
                        if ac > 0
                            && self
                                .core_sub_view()
                                .visit_axis_values_backing(LookupAxis::RowInView(row), f)?
                        {
                            return Ok(());
                        }
                        let tail_val = tail.right_tail.get(row).unwrap_or(&LiteralValue::Empty);
                        for c in ac..self.cols {
                            if (c - ac) & 4095 == 0 && self.criteria_cache_cancelled() {
                                return Err(ExcelError::new(
                                    formualizer_common::ExcelErrorKind::Cancelled,
                                ));
                            }
                            if let ControlFlow::Break(()) = f(c, tail_val.clone()) {
                                return Ok(());
                            }
                        }
                    } else {
                        for c in 0..ac {
                            if c & 4095 == 0 && self.criteria_cache_cancelled() {
                                return Err(ExcelError::new(
                                    formualizer_common::ExcelErrorKind::Cancelled,
                                ));
                            }
                            let val = tail
                                .bottom_tail
                                .get(c)
                                .cloned()
                                .unwrap_or(LiteralValue::Empty);
                            if let ControlFlow::Break(()) = f(c, val) {
                                return Ok(());
                            }
                        }
                        for c in ac..self.cols {
                            if (c - ac) & 4095 == 0 && self.criteria_cache_cancelled() {
                                return Err(ExcelError::new(
                                    formualizer_common::ExcelErrorKind::Cancelled,
                                ));
                            }
                            if let ControlFlow::Break(()) = f(c, tail.corner_tail.clone()) {
                                return Ok(());
                            }
                        }
                    }
                    return Ok(());
                }
            }
        }

        let _ = self.visit_axis_values_backing(axis, f)?;
        Ok(())
    }

    /// Backing-only implementation of [`Self::visit_axis_values`] (no tail).
    /// Returns `Ok(true)` if `f` requested an early break.
    fn visit_axis_values_backing(
        &self,
        axis: crate::engine::lookup_index_cache::LookupAxis,
        f: &mut impl FnMut(usize, LiteralValue) -> std::ops::ControlFlow<()>,
    ) -> Result<bool, ExcelError> {
        use crate::engine::lookup_index_cache::LookupAxis;
        use std::ops::ControlFlow;

        let sheet = self.sheet();
        let date_system = sheet.date_system;
        match axis {
            LookupAxis::ColumnInView(col) => {
                if col >= self.cols || self.rows == 0 {
                    return Ok(false);
                }
                let abs_col = self.sc + col;
                let col_ref = sheet.columns.get(abs_col);
                let mut next_idx = 0usize;
                for seg in self.iter_row_segments() {
                    let seg = seg?;
                    let SegmentKind::Backing {
                        chunk_idx,
                        chunk_offset,
                    } = seg.kind
                    else {
                        continue;
                    };
                    while next_idx < seg.row_start {
                        self.poll_axis_cancelled(next_idx)?;
                        if let ControlFlow::Break(()) = f(next_idx, LiteralValue::Empty) {
                            return Ok(true);
                        }
                        next_idx += 1;
                    }
                    let Some(ch) = col_ref.and_then(|c| c.chunk(chunk_idx)) else {
                        for k in 0..seg.row_len {
                            self.poll_axis_cancelled(k)?;
                            if let ControlFlow::Break(()) =
                                f(seg.row_start + k, LiteralValue::Empty)
                            {
                                return Ok(true);
                            }
                        }
                        next_idx = seg.row_start + seg.row_len;
                        continue;
                    };
                    let tags = ch.type_tag.values();
                    let numbers = ch.numbers.as_deref();
                    let booleans = ch.booleans.as_deref();
                    let text = ch
                        .text
                        .as_deref()
                        .and_then(|a| a.as_any().downcast_ref::<arrow_array::StringArray>());
                    let errors = ch.errors.as_deref();
                    let cascade =
                        arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                    let has_overlay =
                        cascade.has_any_in_range(chunk_offset..chunk_offset + seg.row_len);

                    for k in 0..seg.row_len {
                        self.poll_axis_cancelled(k)?;
                        let in_off = chunk_offset + k;
                        let val = Self::read_chunk_cell(
                            ch,
                            tags,
                            numbers,
                            booleans,
                            text,
                            errors,
                            in_off,
                            has_overlay,
                            date_system,
                        );
                        if let ControlFlow::Break(()) = f(seg.row_start + k, val) {
                            return Ok(true);
                        }
                    }
                    next_idx = seg.row_start + seg.row_len;
                }
                while next_idx < self.rows {
                    self.poll_axis_cancelled(next_idx)?;
                    if let ControlFlow::Break(()) = f(next_idx, LiteralValue::Empty) {
                        return Ok(true);
                    }
                    next_idx += 1;
                }
                Ok(false)
            }
            LookupAxis::RowInView(row) => {
                if row >= self.rows || self.cols == 0 {
                    return Ok(false);
                }
                let abs_row = self.sr + row;
                let sheet_rows = sheet.nrows as usize;
                if abs_row >= sheet_rows || sheet.chunk_starts.is_empty() {
                    for c in 0..self.cols {
                        if c & 4095 == 0 && self.criteria_cache_cancelled() {
                            return Err(ExcelError::new(
                                formualizer_common::ExcelErrorKind::Cancelled,
                            ));
                        }
                        if let ControlFlow::Break(()) = f(c, LiteralValue::Empty) {
                            return Ok(true);
                        }
                    }
                    return Ok(false);
                }
                let ch_idx = match sheet.chunk_starts.binary_search(&abs_row) {
                    Ok(i) => i,
                    Err(0) => 0,
                    Err(i) => i - 1,
                };
                let in_off = abs_row - sheet.chunk_starts[ch_idx];
                for c in 0..self.cols {
                    if c & 4095 == 0 && self.criteria_cache_cancelled() {
                        return Err(ExcelError::new(
                            formualizer_common::ExcelErrorKind::Cancelled,
                        ));
                    }
                    let abs_col = self.sc + c;
                    let val = match sheet.columns.get(abs_col).and_then(|col| col.chunk(ch_idx)) {
                        Some(ch) => {
                            let tags = ch.type_tag.values();
                            let numbers = ch.numbers.as_deref();
                            let booleans = ch.booleans.as_deref();
                            let text = ch.text.as_deref().and_then(|a| {
                                a.as_any().downcast_ref::<arrow_array::StringArray>()
                            });
                            let errors = ch.errors.as_deref();
                            Self::read_chunk_cell(
                                ch,
                                tags,
                                numbers,
                                booleans,
                                text,
                                errors,
                                in_off,
                                true,
                                date_system,
                            )
                        }
                        None => LiteralValue::Empty,
                    };
                    if let ControlFlow::Break(()) = f(c, val) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    /// Stream approximate-searchable numeric values (as `f64`, excluding blanks and
    /// errors) along a 1D lookup axis with zero per-cell heap allocations.
    pub(crate) fn visit_axis_approx_numeric(
        &self,
        axis: crate::engine::lookup_index_cache::LookupAxis,
        f: &mut impl FnMut(usize, f64) -> std::ops::ControlFlow<()>,
    ) -> Result<(), ExcelError> {
        use crate::builtins::lookup::lookup_utils::value_to_f64_lenient;
        use crate::engine::lookup_index_cache::LookupAxis;
        use std::ops::ControlFlow;

        if let Some(tail) = &self.tail {
            let date_system = self.sheet().date_system;
            let ar = tail.active_rows.min(self.rows);
            let ac = tail.active_cols.min(self.cols);
            let extract_lit = |v: &LiteralValue| -> Option<f64> {
                if matches!(v, LiteralValue::Empty) {
                    None
                } else {
                    value_to_f64_lenient(v, date_system)
                }
            };
            // A uniform constant tail spanning `start..end` only needs its first
            // and last positions emitted: sortedness across identical values is
            // always satisfied, ascending tie-breaking selects `end - 1`, and
            // descending exact tie-breaking selects `start` while `>` selects `end - 1`.
            let emit_uniform_span =
                |start: usize,
                 end: usize,
                 v: &LiteralValue,
                 f: &mut dyn FnMut(usize, f64) -> ControlFlow<()>| {
                    if start >= end {
                        return;
                    }
                    if let Some(n) = extract_lit(v) {
                        if let ControlFlow::Break(()) = f(start, n) {
                            return;
                        }
                        if end - 1 > start {
                            let _ = f(end - 1, n);
                        }
                    }
                };

            match axis {
                LookupAxis::ColumnInView(col) => {
                    if col >= self.cols || self.rows == 0 {
                        return Ok(());
                    }
                    if col < ac {
                        if ar > 0
                            && self.core_sub_view().visit_axis_approx_numeric_backing(
                                LookupAxis::ColumnInView(col),
                                f,
                            )?
                        {
                            return Ok(());
                        }
                        let tail_val = tail.bottom_tail.get(col).unwrap_or(&LiteralValue::Empty);
                        emit_uniform_span(ar, self.rows, tail_val, f);
                    } else {
                        for r in 0..ar {
                            self.poll_axis_cancelled(r)?;
                            if let Some(n) = tail.right_tail.get(r).and_then(extract_lit)
                                && let ControlFlow::Break(()) = f(r, n)
                            {
                                return Ok(());
                            }
                        }
                        emit_uniform_span(ar, self.rows, &tail.corner_tail, f);
                    }
                    return Ok(());
                }
                LookupAxis::RowInView(row) => {
                    if row >= self.rows || self.cols == 0 {
                        return Ok(());
                    }
                    if row < ar {
                        if ac > 0
                            && self
                                .core_sub_view()
                                .visit_axis_approx_numeric_backing(LookupAxis::RowInView(row), f)?
                        {
                            return Ok(());
                        }
                        let tail_val = tail.right_tail.get(row).unwrap_or(&LiteralValue::Empty);
                        emit_uniform_span(ac, self.cols, tail_val, f);
                    } else {
                        for c in 0..ac {
                            self.poll_axis_cancelled(c)?;
                            if let Some(n) = tail.bottom_tail.get(c).and_then(extract_lit)
                                && let ControlFlow::Break(()) = f(c, n)
                            {
                                return Ok(());
                            }
                        }
                        emit_uniform_span(ac, self.cols, &tail.corner_tail, f);
                    }
                    return Ok(());
                }
            }
        }

        let _ = self.visit_axis_approx_numeric_backing(axis, f)?;
        Ok(())
    }

    /// Backing-only implementation of [`Self::visit_axis_approx_numeric`] (no tail).
    /// Returns `Ok(true)` if `f` requested an early break.
    fn visit_axis_approx_numeric_backing(
        &self,
        axis: crate::engine::lookup_index_cache::LookupAxis,
        f: &mut impl FnMut(usize, f64) -> std::ops::ControlFlow<()>,
    ) -> Result<bool, ExcelError> {
        use crate::builtins::lookup::lookup_utils::try_parse_f64_fast;
        use crate::engine::lookup_index_cache::LookupAxis;
        use std::ops::ControlFlow;

        let extract_overlay = |ov: &arrow_store::OverlayScalar<'_>| -> Option<f64> {
            match ov.type_tag() {
                arrow_store::TypeTag::Number
                | arrow_store::TypeTag::DateTime
                | arrow_store::TypeTag::Duration => ov.numeric_lane_value(),
                arrow_store::TypeTag::Boolean => {
                    ov.boolean_lane_value().map(|b| if b { 1.0 } else { 0.0 })
                }
                arrow_store::TypeTag::Text => ov.text_lane_value().and_then(try_parse_f64_fast),
                arrow_store::TypeTag::Empty
                | arrow_store::TypeTag::Error
                | arrow_store::TypeTag::Pending => None,
            }
        };

        let sheet = self.sheet();
        match axis {
            LookupAxis::ColumnInView(col) => {
                if col >= self.cols || self.rows == 0 {
                    return Ok(false);
                }
                let abs_col = self.sc + col;
                let Some(col_ref) = sheet.columns.get(abs_col) else {
                    return Ok(false);
                };
                for seg in self.iter_row_segments() {
                    let seg = seg?;
                    let SegmentKind::Backing {
                        chunk_idx,
                        chunk_offset,
                    } = seg.kind
                    else {
                        continue;
                    };
                    let Some(ch) = col_ref.chunk(chunk_idx) else {
                        continue;
                    };
                    let cascade =
                        arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                    let has_overlay =
                        cascade.has_any_in_range(chunk_offset..chunk_offset + seg.row_len);
                    let tags = ch.type_tag.values();
                    let numbers = ch.numbers.as_deref();
                    let booleans = ch.booleans.as_deref();
                    let text = ch
                        .text
                        .as_deref()
                        .and_then(|a| a.as_any().downcast_ref::<arrow_array::StringArray>());

                    if !has_overlay {
                        // Fast path: chunk has non-null numeric lane and no boolean/text lanes.
                        if let Some(num_arr) = numbers
                            && num_arr.null_count() == 0
                            && booleans.is_none()
                            && text.is_none()
                        {
                            let num_vals = num_arr.values();
                            let num_tag = arrow_store::TypeTag::Number as u8;
                            let dt_tag = arrow_store::TypeTag::DateTime as u8;
                            let dur_tag = arrow_store::TypeTag::Duration as u8;
                            for k in 0..seg.row_len {
                                self.poll_axis_cancelled(k)?;
                                let in_off = chunk_offset + k;
                                let t = tags[in_off];
                                if (t == num_tag || t == dt_tag || t == dur_tag)
                                    && let ControlFlow::Break(()) =
                                        f(seg.row_start + k, num_vals[in_off])
                                {
                                    return Ok(true);
                                }
                            }
                            continue;
                        }
                        for k in 0..seg.row_len {
                            self.poll_axis_cancelled(k)?;
                            let in_off = chunk_offset + k;
                            let val = match arrow_store::TypeTag::from_u8(tags[in_off]) {
                                arrow_store::TypeTag::Number
                                | arrow_store::TypeTag::DateTime
                                | arrow_store::TypeTag::Duration => numbers
                                    .filter(|a| !a.is_null(in_off))
                                    .map(|a| a.value(in_off)),
                                arrow_store::TypeTag::Boolean => booleans
                                    .filter(|a| !a.is_null(in_off))
                                    .map(|a| if a.value(in_off) { 1.0 } else { 0.0 }),
                                arrow_store::TypeTag::Text => text
                                    .filter(|sa| !sa.is_null(in_off))
                                    .and_then(|sa| try_parse_f64_fast(sa.value(in_off))),
                                arrow_store::TypeTag::Empty
                                | arrow_store::TypeTag::Error
                                | arrow_store::TypeTag::Pending => None,
                            };
                            if let Some(v) = val
                                && let ControlFlow::Break(()) = f(seg.row_start + k, v)
                            {
                                return Ok(true);
                            }
                        }
                    } else {
                        for k in 0..seg.row_len {
                            self.poll_axis_cancelled(k)?;
                            let in_off = chunk_offset + k;
                            let val = if let Some(ov) = cascade.get_scalar(in_off) {
                                extract_overlay(&ov)
                            } else {
                                match arrow_store::TypeTag::from_u8(tags[in_off]) {
                                    arrow_store::TypeTag::Number
                                    | arrow_store::TypeTag::DateTime
                                    | arrow_store::TypeTag::Duration => numbers
                                        .filter(|a| !a.is_null(in_off))
                                        .map(|a| a.value(in_off)),
                                    arrow_store::TypeTag::Boolean => booleans
                                        .filter(|a| !a.is_null(in_off))
                                        .map(|a| if a.value(in_off) { 1.0 } else { 0.0 }),
                                    arrow_store::TypeTag::Text => text
                                        .filter(|sa| !sa.is_null(in_off))
                                        .and_then(|sa| try_parse_f64_fast(sa.value(in_off))),
                                    arrow_store::TypeTag::Empty
                                    | arrow_store::TypeTag::Error
                                    | arrow_store::TypeTag::Pending => None,
                                }
                            };
                            if let Some(v) = val
                                && let ControlFlow::Break(()) = f(seg.row_start + k, v)
                            {
                                return Ok(true);
                            }
                        }
                    }
                }
                Ok(false)
            }
            LookupAxis::RowInView(row) => {
                if row >= self.rows || self.cols == 0 {
                    return Ok(false);
                }
                let abs_row = self.sr + row;
                let sheet_rows = sheet.nrows as usize;
                if abs_row >= sheet_rows || sheet.chunk_starts.is_empty() {
                    return Ok(false);
                }
                let ch_idx = match sheet.chunk_starts.binary_search(&abs_row) {
                    Ok(i) => i,
                    Err(0) => 0,
                    Err(i) => i - 1,
                };
                let in_off = abs_row - sheet.chunk_starts[ch_idx];
                let max_cols = self.cols.min(sheet.columns.len().saturating_sub(self.sc));
                for c in 0..max_cols {
                    if c & 4095 == 0 && self.criteria_cache_cancelled() {
                        return Err(ExcelError::new(
                            formualizer_common::ExcelErrorKind::Cancelled,
                        ));
                    }
                    let Some(ch) = sheet.columns[self.sc + c].chunk(ch_idx) else {
                        continue;
                    };
                    let cascade =
                        arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                    let val = if let Some(ov) = cascade.get_scalar(in_off) {
                        extract_overlay(&ov)
                    } else {
                        match arrow_store::TypeTag::from_u8(ch.type_tag.value(in_off)) {
                            arrow_store::TypeTag::Number
                            | arrow_store::TypeTag::DateTime
                            | arrow_store::TypeTag::Duration => ch
                                .numbers
                                .as_deref()
                                .filter(|a| !a.is_null(in_off))
                                .map(|a| a.value(in_off)),
                            arrow_store::TypeTag::Boolean => ch
                                .booleans
                                .as_deref()
                                .filter(|a| !a.is_null(in_off))
                                .map(|a| if a.value(in_off) { 1.0 } else { 0.0 }),
                            arrow_store::TypeTag::Text => ch
                                .text
                                .as_deref()
                                .and_then(|a| a.as_any().downcast_ref::<arrow_array::StringArray>())
                                .filter(|sa| !sa.is_null(in_off))
                                .and_then(|sa| try_parse_f64_fast(sa.value(in_off))),
                            arrow_store::TypeTag::Empty
                            | arrow_store::TypeTag::Error
                            | arrow_store::TypeTag::Pending => None,
                        }
                    };
                    if let Some(v) = val
                        && let ControlFlow::Break(()) = f(c, v)
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    /// Stream approximate-searchable text values (borrowed `&str` from Arrow/overlay)
    /// along a 1D lookup axis with zero per-cell heap allocations.
    pub(crate) fn visit_axis_approx_text(
        &self,
        axis: crate::engine::lookup_index_cache::LookupAxis,
        f: &mut impl FnMut(usize, &str) -> std::ops::ControlFlow<()>,
    ) -> Result<(), ExcelError> {
        use crate::engine::lookup_index_cache::LookupAxis;
        use std::ops::ControlFlow;

        if let Some(tail) = &self.tail {
            let ar = tail.active_rows.min(self.rows);
            let ac = tail.active_cols.min(self.cols);
            let emit_uniform_span =
                |start: usize,
                 end: usize,
                 v: &LiteralValue,
                 f: &mut dyn FnMut(usize, &str) -> ControlFlow<()>| {
                    if start >= end {
                        return;
                    }
                    if let LiteralValue::Text(s) = v {
                        if let ControlFlow::Break(()) = f(start, s.as_str()) {
                            return;
                        }
                        if end - 1 > start {
                            let _ = f(end - 1, s.as_str());
                        }
                    }
                };

            match axis {
                LookupAxis::ColumnInView(col) => {
                    if col >= self.cols || self.rows == 0 {
                        return Ok(());
                    }
                    if col < ac {
                        if ar > 0
                            && self
                                .core_sub_view()
                                .visit_axis_approx_text_backing(LookupAxis::ColumnInView(col), f)?
                        {
                            return Ok(());
                        }
                        let tail_val = tail.bottom_tail.get(col).unwrap_or(&LiteralValue::Empty);
                        emit_uniform_span(ar, self.rows, tail_val, f);
                    } else {
                        for r in 0..ar {
                            self.poll_axis_cancelled(r)?;
                            if let Some(LiteralValue::Text(s)) = tail.right_tail.get(r)
                                && let ControlFlow::Break(()) = f(r, s.as_str())
                            {
                                return Ok(());
                            }
                        }
                        emit_uniform_span(ar, self.rows, &tail.corner_tail, f);
                    }
                    return Ok(());
                }
                LookupAxis::RowInView(row) => {
                    if row >= self.rows || self.cols == 0 {
                        return Ok(());
                    }
                    if row < ar {
                        if ac > 0
                            && self
                                .core_sub_view()
                                .visit_axis_approx_text_backing(LookupAxis::RowInView(row), f)?
                        {
                            return Ok(());
                        }
                        let tail_val = tail.right_tail.get(row).unwrap_or(&LiteralValue::Empty);
                        emit_uniform_span(ac, self.cols, tail_val, f);
                    } else {
                        for c in 0..ac {
                            self.poll_axis_cancelled(c)?;
                            if let Some(LiteralValue::Text(s)) = tail.bottom_tail.get(c)
                                && let ControlFlow::Break(()) = f(c, s.as_str())
                            {
                                return Ok(());
                            }
                        }
                        emit_uniform_span(ac, self.cols, &tail.corner_tail, f);
                    }
                    return Ok(());
                }
            }
        }

        let _ = self.visit_axis_approx_text_backing(axis, f)?;
        Ok(())
    }

    /// Backing-only implementation of [`Self::visit_axis_approx_text`] (no tail).
    /// Returns `Ok(true)` if `f` requested an early break.
    fn visit_axis_approx_text_backing(
        &self,
        axis: crate::engine::lookup_index_cache::LookupAxis,
        f: &mut impl FnMut(usize, &str) -> std::ops::ControlFlow<()>,
    ) -> Result<bool, ExcelError> {
        use crate::engine::lookup_index_cache::LookupAxis;
        use std::ops::ControlFlow;

        let sheet = self.sheet();
        match axis {
            LookupAxis::ColumnInView(col) => {
                if col >= self.cols || self.rows == 0 {
                    return Ok(false);
                }
                let abs_col = self.sc + col;
                let Some(col_ref) = sheet.columns.get(abs_col) else {
                    return Ok(false);
                };
                let text_tag = arrow_store::TypeTag::Text as u8;
                for seg in self.iter_row_segments() {
                    let seg = seg?;
                    let SegmentKind::Backing {
                        chunk_idx,
                        chunk_offset,
                    } = seg.kind
                    else {
                        continue;
                    };
                    let Some(ch) = col_ref.chunk(chunk_idx) else {
                        continue;
                    };
                    let cascade =
                        arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                    let has_overlay =
                        cascade.has_any_in_range(chunk_offset..chunk_offset + seg.row_len);
                    let text = ch
                        .text
                        .as_deref()
                        .and_then(|a| a.as_any().downcast_ref::<arrow_array::StringArray>());
                    if !has_overlay {
                        let Some(sa) = text else {
                            continue;
                        };
                        let tags = ch.type_tag.values();
                        if sa.null_count() == 0 {
                            for k in 0..seg.row_len {
                                self.poll_axis_cancelled(k)?;
                                let in_off = chunk_offset + k;
                                if tags[in_off] == text_tag
                                    && let ControlFlow::Break(()) =
                                        f(seg.row_start + k, sa.value(in_off))
                                {
                                    return Ok(true);
                                }
                            }
                        } else {
                            for k in 0..seg.row_len {
                                self.poll_axis_cancelled(k)?;
                                let in_off = chunk_offset + k;
                                if tags[in_off] == text_tag
                                    && !sa.is_null(in_off)
                                    && let ControlFlow::Break(()) =
                                        f(seg.row_start + k, sa.value(in_off))
                                {
                                    return Ok(true);
                                }
                            }
                        }
                    } else {
                        let tags = ch.type_tag.values();
                        for k in 0..seg.row_len {
                            self.poll_axis_cancelled(k)?;
                            let in_off = chunk_offset + k;
                            let ov_opt = cascade.get_scalar(in_off);
                            let val = if let Some(ov) = ov_opt.as_ref() {
                                ov.text_lane_value()
                            } else if tags[in_off] == text_tag {
                                text.filter(|sa| !sa.is_null(in_off))
                                    .map(|sa| sa.value(in_off))
                            } else {
                                None
                            };
                            if let Some(s) = val
                                && let ControlFlow::Break(()) = f(seg.row_start + k, s)
                            {
                                return Ok(true);
                            }
                        }
                    }
                }
                Ok(false)
            }
            LookupAxis::RowInView(row) => {
                if row >= self.rows || self.cols == 0 {
                    return Ok(false);
                }
                let abs_row = self.sr + row;
                let sheet_rows = sheet.nrows as usize;
                if abs_row >= sheet_rows || sheet.chunk_starts.is_empty() {
                    return Ok(false);
                }
                let ch_idx = match sheet.chunk_starts.binary_search(&abs_row) {
                    Ok(i) => i,
                    Err(0) => 0,
                    Err(i) => i - 1,
                };
                let in_off = abs_row - sheet.chunk_starts[ch_idx];
                let max_cols = self.cols.min(sheet.columns.len().saturating_sub(self.sc));
                let text_tag = arrow_store::TypeTag::Text as u8;
                for c in 0..max_cols {
                    if c & 4095 == 0 && self.criteria_cache_cancelled() {
                        return Err(ExcelError::new(
                            formualizer_common::ExcelErrorKind::Cancelled,
                        ));
                    }
                    let Some(ch) = sheet.columns[self.sc + c].chunk(ch_idx) else {
                        continue;
                    };
                    let cascade =
                        arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                    let ov_opt = cascade.get_scalar(in_off);
                    let val = if let Some(ov) = ov_opt.as_ref() {
                        ov.text_lane_value()
                    } else if ch.type_tag.value(in_off) == text_tag {
                        ch.text
                            .as_deref()
                            .and_then(|a| a.as_any().downcast_ref::<arrow_array::StringArray>())
                            .filter(|sa| !sa.is_null(in_off))
                            .map(|sa| sa.value(in_off))
                    } else {
                        None
                    };
                    if let Some(s) = val
                        && let ControlFlow::Break(()) = f(c, s)
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    /// Row-major cell traversal.
    pub fn for_each_cell(
        &self,
        f: &mut dyn FnMut(&LiteralValue) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        if self.tail.is_some() {
            let mut since_poll = 4096usize;
            for r in 0..self.rows {
                for c in 0..self.cols {
                    if since_poll >= 4096 {
                        since_poll = 0;
                        if self
                            .cancel_token
                            .as_ref()
                            .is_some_and(CancelToken::is_cancelled)
                        {
                            return Err(ExcelError::new(
                                formualizer_common::ExcelErrorKind::Cancelled,
                            ));
                        }
                    }
                    since_poll += 1;
                    let tmp = self.get_cell(r, c);
                    f(&tmp)?;
                }
            }
            return Ok(());
        }
        for res in self.iter_row_chunks() {
            let cs = res?;
            for r in 0..cs.row_len {
                for c in 0..self.cols {
                    let tmp = self.get_cell(cs.row_start + r, c);
                    f(&tmp)?;
                }
            }
        }
        Ok(())
    }

    /// Visit each row as a borrowed slice (buffered).
    pub fn for_each_row(
        &self,
        f: &mut dyn FnMut(&[LiteralValue]) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        let mut buf: Vec<LiteralValue> = Vec::with_capacity(self.cols);
        for r in 0..self.rows {
            buf.clear();
            for c in 0..self.cols {
                buf.push(self.get_cell(r, c));
            }
            f(&buf[..])?;
        }
        Ok(())
    }

    /// Visit each column as a contiguous slice (buffered).
    pub fn for_each_col(
        &self,
        f: &mut dyn FnMut(&[LiteralValue]) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        let mut col_buf: Vec<LiteralValue> = Vec::with_capacity(self.rows);
        for c in 0..self.cols {
            col_buf.clear();
            for r in 0..self.rows {
                col_buf.push(self.get_cell(r, c));
            }
            f(&col_buf[..])?;
        }
        Ok(())
    }

    /// Get a numeric value at a specific cell, with coercion.
    /// Returns None for empty cells or non-coercible values.
    pub fn get_cell_numeric(&self, row: usize, col: usize, policy: CoercionPolicy) -> Option<f64> {
        if row >= self.rows || col >= self.cols {
            return None;
        }

        let val = self.get_cell(row, col);
        pack_numeric(&val, policy).ok().flatten()
    }

    /// Numeric chunk iteration with coercion policy.
    pub fn numbers_chunked(
        &self,
        policy: CoercionPolicy,
        min_chunk: usize,
        f: &mut dyn FnMut(NumericChunk) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
        // Fast path for Arrow numbers lane when policy allows ignoring non-numeric cells in ranges (standard Excel behavior for SUM/AVERAGE/etc over ranges)
        if matches!(policy, CoercionPolicy::NumberStrict) {
            for res in self.numbers_slices() {
                let (_, _, cols) = res?;
                for col in cols {
                    if col.null_count() < col.len() {
                        let data = col.values();
                        // If there are nulls, we need to handle them.
                        // Currently NumericChunk doesn't have a perfect way to represent sparse Arrow slices
                        // without copying if we want a contiguous f64 slice.
                        // For now, we can just provide the raw data and the validity mask if it exists.

                        let validity = if col.null_count() > 0 {
                            // Extract validity mask.
                            // Note: This is still slightly awkward with the current NumericChunk design.
                            None // TODO: Implement validity mask propagation
                        } else {
                            None
                        };

                        if col.null_count() == 0 {
                            f(NumericChunk { data, validity })?;
                        } else {
                            // Fallback for nulls: iterate and push to a small buffer
                            let mut buf = Vec::with_capacity(col.len());
                            for i in 0..col.len() {
                                if !col.is_null(i) {
                                    buf.push(col.value(i));
                                }
                            }
                            if !buf.is_empty() {
                                f(NumericChunk {
                                    data: &buf,
                                    validity: None,
                                })?;
                            }
                        }
                    }
                }
            }
            return Ok(());
        }

        let min_chunk = min_chunk.max(1);
        let mut buf: Vec<f64> = Vec::with_capacity(min_chunk);
        let mut flush = |buf: &mut Vec<f64>| -> Result<(), ExcelError> {
            if buf.is_empty() {
                return Ok(());
            }
            // SAFETY: read-only borrow for callback duration
            let ptr = buf.as_ptr();
            let len = buf.len();
            let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
            let chunk = NumericChunk {
                data: slice,
                validity: None,
            };
            f(chunk)?;
            buf.clear();
            Ok(())
        };

        self.for_each_cell(&mut |v| {
            if let Some(n) = pack_numeric(v, policy)? {
                buf.push(n);
                if buf.len() >= min_chunk {
                    flush(&mut buf)?;
                }
            }
            Ok(())
        })?;
        flush(&mut buf)?;

        Ok(())
    }

    /// Typed numeric slices per row-segment: (row_start, row_len, per-column Float64 arrays)
    pub fn numbers_slices(
        &self,
    ) -> impl Iterator<Item = Result<(usize, usize, Vec<Arc<arrow_array::Float64Array>>), ExcelError>> + '_
    {
        self.iter_row_segments().map(move |res| {
            let segment = res?;
            let mut out_cols = Vec::with_capacity(self.cols);
            let sheet = self.sheet();
            match segment.kind {
                SegmentKind::BottomTail => {
                    let tail = self.tail.as_deref().expect("BottomTail requires tail");
                    for local_c in 0..self.cols {
                        let val = if local_c < tail.active_cols {
                            tail.bottom_tail
                                .get(local_c)
                                .unwrap_or(&LiteralValue::Empty)
                        } else {
                            &tail.corner_tail
                        };
                        out_cols.push(tail_numbers_array(
                            TailValues::Constant(val),
                            segment.row_len,
                            sheet.date_system,
                        ));
                    }
                }
                SegmentKind::Backing {
                    chunk_idx,
                    chunk_offset,
                } => {
                    for (local_c, col_idx) in (self.sc..=self.ec).enumerate() {
                        if let Some(tail) = &self.tail
                            && local_c >= tail.active_cols
                        {
                            let slice = tail.right_tail_slice(segment.row_start, segment.row_len);
                            out_cols.push(tail_numbers_array(
                                TailValues::Slice(slice),
                                segment.row_len,
                                sheet.date_system,
                            ));
                            continue;
                        }
                        let Some(ch) = sheet
                            .columns
                            .get(col_idx)
                            .and_then(|col| col.chunk(chunk_idx))
                        else {
                            #[cfg(test)]
                            range_work::record(|w| {
                                w.null_arrays += 1;
                                w.null_slots += segment.row_len;
                            });
                            out_cols.push(Arc::new(arrow_array::Float64Array::new_null(
                                segment.row_len,
                            )));
                            continue;
                        };
                        let base = ch.numbers_or_null().slice(chunk_offset, segment.row_len);
                        let range = chunk_offset..chunk_offset + segment.row_len;
                        let cascade =
                            arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                        out_cols.push(if cascade.has_any_in_range(range.clone()) {
                            ch.merged_numbers(range.clone())
                                .unwrap_or_else(|| cascade.select_numbers(range, &base))
                        } else {
                            Arc::new(base)
                        });
                    }
                }
            }
            Ok((segment.row_start, segment.row_len, out_cols))
        })
    }

    /// Typed boolean slices per row-segment, overlay-aware via zip.
    pub fn booleans_slices(
        &self,
    ) -> impl Iterator<Item = Result<(usize, usize, Vec<Arc<arrow_array::BooleanArray>>), ExcelError>> + '_
    {
        self.iter_row_chunks().map(move |res| {
            let cs = res?;
            let mut out_cols: Vec<Arc<arrow_array::BooleanArray>> =
                Vec::with_capacity(cs.cols.len());
            let sheet = self.sheet();
            let chunk_starts = &sheet.chunk_starts;

            for (local_c, col_idx) in (self.sc..=self.ec).enumerate() {
                let base = cs.cols[local_c]
                    .booleans
                    .as_ref()
                    .expect("booleans lane exists")
                    .clone();
                let base_ba = base
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .unwrap()
                    .clone();
                let base_arc = Arc::new(base_ba);

                if let Some(tail) = &self.tail
                    && (cs.row_start >= tail.active_rows || local_c >= tail.active_cols)
                {
                    out_cols.push(base_arc);
                    continue;
                }

                // Identify chunk and overlay segment
                let abs_seg_start = self.sr + cs.row_start;
                let ch_idx = match chunk_starts.binary_search(&abs_seg_start) {
                    Ok(i) => i,
                    Err(0) => 0,
                    Err(i) => i - 1,
                };
                if col_idx >= sheet.columns.len() {
                    out_cols.push(base_arc);
                    continue;
                }
                let col = &sheet.columns[col_idx];
                let Some(ch) = col.chunk(ch_idx) else {
                    out_cols.push(base_arc);
                    continue;
                };
                let rel_off = (self.sr + cs.row_start) - chunk_starts[ch_idx];
                let seg_range = rel_off..(rel_off + cs.row_len);
                let cascade = arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                if cascade.has_any_in_range(seg_range.clone()) {
                    let base_ba = base
                        .as_any()
                        .downcast_ref::<arrow_array::BooleanArray>()
                        .unwrap();
                    out_cols.push(cascade.select_booleans(seg_range, base_ba));
                } else {
                    out_cols.push(base_arc);
                }
            }
            Ok((cs.row_start, cs.row_len, out_cols))
        })
    }

    /// Text slices per row-segment (erased as ArrayRef for Utf8 today; future Dict/View support).
    pub fn text_slices(
        &self,
    ) -> impl Iterator<Item = Result<(usize, usize, Vec<arrow_array::ArrayRef>), ExcelError>> + '_
    {
        self.iter_row_chunks().map(move |res| {
            let cs = res?;
            let mut out_cols: Vec<arrow_array::ArrayRef> = Vec::with_capacity(cs.cols.len());
            let sheet = self.sheet();
            let chunk_starts = &sheet.chunk_starts;

            for (local_c, col_idx) in (self.sc..=self.ec).enumerate() {
                let base = cs.cols[local_c]
                    .text
                    .as_ref()
                    .expect("text lane exists")
                    .clone();
                if let Some(tail) = &self.tail
                    && (cs.row_start >= tail.active_rows || local_c >= tail.active_cols)
                {
                    out_cols.push(base);
                    continue;
                }
                let abs_seg_start = self.sr + cs.row_start;
                let ch_idx = match chunk_starts.binary_search(&abs_seg_start) {
                    Ok(i) => i,
                    Err(0) => 0,
                    Err(i) => i - 1,
                };
                if col_idx >= sheet.columns.len() {
                    out_cols.push(base.clone());
                    continue;
                }
                let col = &sheet.columns[col_idx];
                let Some(ch) = col.chunk(ch_idx) else {
                    out_cols.push(base.clone());
                    continue;
                };
                let rel_off = (self.sr + cs.row_start) - chunk_starts[ch_idx];
                let seg_range = rel_off..(rel_off + cs.row_len);
                let cascade = arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                if cascade.has_any_in_range(seg_range.clone()) {
                    let base_sa = base
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap();
                    out_cols.push(cascade.select_text(seg_range, base_sa));
                } else {
                    out_cols.push(base.clone());
                }
            }
            Ok((cs.row_start, cs.row_len, out_cols))
        })
    }

    /// Typed lowered text slices per row-segment, overlay-aware via zip.
    pub fn lowered_text_slices(
        &self,
    ) -> impl Iterator<Item = Result<(usize, usize, Vec<Arc<arrow_array::StringArray>>), ExcelError>> + '_
    {
        self.iter_row_chunks().map(move |res| {
            let cs = res?;
            let mut out_cols: Vec<Arc<arrow_array::StringArray>> =
                Vec::with_capacity(cs.cols.len());
            let sheet = self.sheet();
            let chunk_starts = &sheet.chunk_starts;

            for (local_c, col_idx) in (self.sc..=self.ec).enumerate() {
                if let Some(tail) = &self.tail {
                    if cs.row_start >= tail.active_rows {
                        let val = if local_c < tail.active_cols {
                            tail.bottom_tail
                                .get(local_c)
                                .unwrap_or(&LiteralValue::Empty)
                        } else {
                            &tail.corner_tail
                        };
                        out_cols.push(tail_lowered_text_array(
                            TailValues::Constant(val),
                            cs.row_len,
                        ));
                        continue;
                    }
                    if local_c >= tail.active_cols {
                        let slice = tail.right_tail_slice(cs.row_start, cs.row_len);
                        out_cols.push(tail_lowered_text_array(
                            TailValues::Slice(slice),
                            cs.row_len,
                        ));
                        continue;
                    }
                }
                // Identify chunk
                let abs_seg_start = self.sr + cs.row_start;
                let ch_idx = match chunk_starts.binary_search(&abs_seg_start) {
                    Ok(i) => i,
                    Err(0) => 0,
                    Err(i) => i - 1,
                };
                if col_idx >= sheet.columns.len() {
                    out_cols.push(Arc::new(arrow_array::StringArray::new_null(cs.row_len)));
                    continue;
                }
                let col = &sheet.columns[col_idx];
                let Some(ch) = col.chunk(ch_idx) else {
                    out_cols.push(Arc::new(arrow_array::StringArray::new_null(cs.row_len)));
                    continue;
                };
                let rel_off = (self.sr + cs.row_start) - chunk_starts[ch_idx];
                let seg_range = rel_off..(rel_off + cs.row_len);

                let base_lowered = ch.text_lower_or_null();
                let base_seg = base_lowered.slice(rel_off, cs.row_len);
                let base_sa = base_seg
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("lowered slice downcast");

                let cascade = arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                if cascade.has_any_in_range(seg_range.clone()) {
                    out_cols.push(cascade.select_lowered_text(seg_range, base_sa));
                } else {
                    out_cols.push(Arc::new(base_sa.clone()));
                }
            }
            Ok((cs.row_start, cs.row_len, out_cols))
        })
    }

    /// Typed error-code slices per row-segment.
    pub fn errors_slices(
        &self,
    ) -> impl Iterator<Item = Result<(usize, usize, Vec<Arc<arrow_array::UInt8Array>>), ExcelError>> + '_
    {
        self.iter_row_segments().map(move |res| {
            let segment = res?;
            let mut out_cols = Vec::with_capacity(self.cols);
            let sheet = self.sheet();
            match segment.kind {
                SegmentKind::BottomTail => {
                    let tail = self.tail.as_deref().expect("BottomTail requires tail");
                    for local_c in 0..self.cols {
                        let val = if local_c < tail.active_cols {
                            tail.bottom_tail
                                .get(local_c)
                                .unwrap_or(&LiteralValue::Empty)
                        } else {
                            &tail.corner_tail
                        };
                        out_cols.push(tail_errors_array(
                            TailValues::Constant(val),
                            segment.row_len,
                        ));
                    }
                }
                SegmentKind::Backing {
                    chunk_idx,
                    chunk_offset,
                } => {
                    for (local_c, col_idx) in (self.sc..=self.ec).enumerate() {
                        if let Some(tail) = &self.tail
                            && local_c >= tail.active_cols
                        {
                            let slice = tail.right_tail_slice(segment.row_start, segment.row_len);
                            out_cols
                                .push(tail_errors_array(TailValues::Slice(slice), segment.row_len));
                            continue;
                        }
                        let Some(ch) = sheet
                            .columns
                            .get(col_idx)
                            .and_then(|col| col.chunk(chunk_idx))
                        else {
                            #[cfg(test)]
                            range_work::record(|w| {
                                w.null_arrays += 1;
                                w.null_slots += segment.row_len;
                            });
                            out_cols
                                .push(Arc::new(arrow_array::UInt8Array::new_null(segment.row_len)));
                            continue;
                        };
                        let base = ch.errors_or_null().slice(chunk_offset, segment.row_len);
                        let range = chunk_offset..chunk_offset + segment.row_len;
                        let cascade =
                            arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                        out_cols.push(if cascade.has_any_in_range(range.clone()) {
                            ch.merged_errors(range.clone())
                                .unwrap_or_else(|| cascade.select_errors(range, &base))
                        } else {
                            Arc::new(base)
                        });
                    }
                }
            }
            Ok((segment.row_start, segment.row_len, out_cols))
        })
    }

    /// Visits overlay-merged error lanes in bounded pieces: one column and at
    /// most `max_rows` rows each, in column-major order within each row
    /// segment. `visit` returns `Ok(true)` to stop early; the method then
    /// returns `Ok(true)`.
    ///
    /// Unlike [`Self::errors_slices`], preparing a piece never builds more
    /// than `max_rows` slots: it does not consult the whole-chunk merged-lane
    /// cache, does not materialize whole-chunk null lanes, and skips columns
    /// and chunks without an error lane or overlay entries in the piece
    /// (they hold no errors). The view's cancellation token is checked before
    /// every piece is prepared, so a cancelled walk returns
    /// `Err(Cancelled)` rather than a partial clean result.
    pub(crate) fn try_for_each_error_piece(
        &self,
        max_rows: usize,
        visit: &mut dyn FnMut(&arrow_array::UInt8Array) -> Result<bool, ExcelError>,
    ) -> Result<bool, ExcelError> {
        let max_rows = max_rows.max(1);
        let cancelled = || {
            self.cancel_token
                .as_ref()
                .is_some_and(CancelToken::is_cancelled)
        };
        if self.tail.is_some() {
            if self
                .core_sub_view()
                .try_for_each_error_piece(max_rows, visit)?
            {
                return Ok(true);
            }
            let mut matched = false;
            self.for_each_tail_entry(|val, _| {
                if matched {
                    return Ok(());
                }
                if cancelled() {
                    return Err(ExcelError::new(
                        formualizer_common::ExcelErrorKind::Cancelled,
                    ));
                }
                if let LiteralValue::Error(error) = val {
                    let lane = arrow_array::UInt8Array::from(vec![Some(
                        arrow_store::map_error_code(error.kind),
                    )]);
                    if visit(&lane)? {
                        matched = true;
                    }
                }
                Ok(())
            })?;
            return Ok(matched);
        }
        let sheet = self.sheet();
        for segment in self.iter_row_segments() {
            let segment = segment?;
            let SegmentKind::Backing {
                chunk_idx,
                chunk_offset,
            } = segment.kind
            else {
                unreachable!("tail.is_none() yields only Backing segments");
            };
            for col_idx in self.sc..=self.ec {
                let Some(ch) = sheet
                    .columns
                    .get(col_idx)
                    .and_then(|col| col.chunk(chunk_idx))
                else {
                    continue;
                };
                let cascade = arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                let mut done = 0usize;
                while done < segment.row_len {
                    if cancelled() {
                        return Err(ExcelError::new(
                            formualizer_common::ExcelErrorKind::Cancelled,
                        ));
                    }
                    let len = (segment.row_len - done).min(max_rows);
                    let start = chunk_offset + done;
                    let range = start..start + len;
                    done += len;
                    #[cfg(test)]
                    range_work::record(|w| {
                        w.error_pieces += 1;
                        w.error_piece_max_rows = w.error_piece_max_rows.max(len);
                    });
                    let lane = if cascade.has_any_in_range(range.clone()) {
                        let base = match &ch.errors {
                            Some(errors) => errors.slice(start, len),
                            None => arrow_array::UInt8Array::new_null(len),
                        };
                        cascade.select_errors(range, &base)
                    } else if let Some(errors) = &ch.errors {
                        Arc::new(errors.slice(start, len))
                    } else {
                        continue;
                    };
                    if visit(&lane)? {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    /// Typed type-tag slices per row-segment.
    pub fn type_tags_slices(
        &self,
    ) -> impl Iterator<Item = Result<(usize, usize, Vec<Arc<arrow_array::UInt8Array>>), ExcelError>> + '_
    {
        self.iter_row_chunks().map(move |res| {
            let cs = res?;
            let mut out_cols: Vec<Arc<arrow_array::UInt8Array>> = Vec::with_capacity(cs.cols.len());
            let sheet = self.sheet();
            let chunk_starts = &sheet.chunk_starts;

            for (local_c, col_idx) in (self.sc..=self.ec).enumerate() {
                let base = cs.cols[local_c].type_tag.clone();
                let base_ta = base
                    .as_any()
                    .downcast_ref::<arrow_array::UInt8Array>()
                    .unwrap()
                    .clone();
                let base_arc = Arc::new(base_ta);

                if let Some(tail) = &self.tail
                    && (cs.row_start >= tail.active_rows || local_c >= tail.active_cols)
                {
                    out_cols.push(base_arc);
                    continue;
                }

                let abs_seg_start = self.sr + cs.row_start;
                let ch_idx = match chunk_starts.binary_search(&abs_seg_start) {
                    Ok(i) => i,
                    Err(0) => 0,
                    Err(i) => i - 1,
                };
                if col_idx >= sheet.columns.len() {
                    out_cols.push(base_arc);
                    continue;
                }
                let col = &sheet.columns[col_idx];
                let Some(ch) = col.chunk(ch_idx) else {
                    out_cols.push(base_arc);
                    continue;
                };
                let rel_off = (self.sr + cs.row_start) - chunk_starts[ch_idx];
                let seg_range = rel_off..(rel_off + cs.row_len);
                let cascade = arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                if cascade.has_any_in_range(seg_range.clone()) {
                    let base_ta = base
                        .as_any()
                        .downcast_ref::<arrow_array::UInt8Array>()
                        .unwrap();
                    out_cols.push(cascade.select_type_tags(seg_range, base_ta));
                } else {
                    out_cols.push(base_arc);
                }
            }
            Ok((cs.row_start, cs.row_len, out_cols))
        })
    }

    /// Build per-column concatenated lowered text arrays for this view.
    /// Uses per-chunk lowered cache for base text and merges overlays via zip_select.
    pub fn lowered_text_columns(&self) -> Vec<arrow_array::ArrayRef> {
        use crate::compute_prelude::concat_engine_arrays as concat_arrays;

        let mut out: Vec<arrow_array::ArrayRef> = Vec::with_capacity(self.cols);
        if self.rows == 0 || self.cols == 0 {
            return out;
        }
        if self.tail.is_some() {
            for c in 0..self.cols {
                let mut b = arrow_array::builder::StringBuilder::with_capacity(
                    self.rows,
                    self.rows.saturating_mul(4),
                );
                for r in 0..self.rows {
                    match self.get_cell(r, c) {
                        LiteralValue::Text(t) => b.append_value(t.to_lowercase()),
                        _ => b.append_null(),
                    }
                }
                out.push(Arc::new(b.finish()));
            }
            return out;
        }
        let sheet = self.sheet();
        let chunk_starts = &sheet.chunk_starts;
        // Clamp to physically materialized sheet rows; this view may be logically larger (e.g. A:A).
        let sheet_rows = sheet.nrows as usize;
        if sheet_rows == 0 || self.sr >= sheet_rows {
            for _ in 0..self.cols {
                out.push(arrow_array::new_null_array(&DataType::Utf8, 0));
            }
            return out;
        }
        let row_end = self.er.min(sheet_rows.saturating_sub(1));
        let physical_len = row_end.saturating_sub(self.sr) + 1;
        for col_idx in self.sc..=self.ec {
            let mut segs: Vec<arrow_array::ArrayRef> = Vec::new();
            if col_idx >= sheet.columns.len() {
                // OOB: nulls across rows
                segs.push(arrow_array::new_null_array(&DataType::Utf8, physical_len));
            } else {
                let col_ref = &sheet.columns[col_idx];
                for (ci, &start) in chunk_starts.iter().enumerate() {
                    let chunk_end = chunk_starts
                        .get(ci + 1)
                        .copied()
                        .unwrap_or(sheet.nrows as usize);
                    let len = chunk_end.saturating_sub(start);
                    if len == 0 {
                        continue;
                    }
                    let end = start + len - 1;
                    let is = start.max(self.sr);
                    let ie = end.min(row_end);
                    if is > ie {
                        continue;
                    }
                    let seg_len = ie - is + 1;
                    let rel_off = is - start;
                    if let Some(ch) = col_ref.chunk(ci) {
                        // Overlay-aware lowered segment
                        let seg_range = rel_off..(rel_off + seg_len);
                        let cascade =
                            arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                        if cascade.has_any_in_range(seg_range.clone()) {
                            let base_lowered = ch.text_lower_or_null();
                            let base_seg = base_lowered.slice(rel_off, seg_len);
                            let base_sa = base_seg
                                .as_any()
                                .downcast_ref::<arrow_array::StringArray>()
                                .expect("lowered slice downcast");
                            segs.push(cascade.select_lowered_text(seg_range, base_sa));
                        } else {
                            // No overlay: slice from lowered base
                            let lowered = ch.text_lower_or_null();
                            segs.push(lowered.slice(rel_off, seg_len));
                        }
                    } else {
                        segs.push(arrow_array::new_null_array(&DataType::Utf8, seg_len));
                    }
                }
            }
            // Ensure concat has at least one segment (can happen on sparse/empty sheets).
            if segs.is_empty() {
                segs.push(arrow_array::new_null_array(&DataType::Utf8, physical_len));
            }
            // Concat segments for this column
            let anys: Vec<&dyn arrow_array::Array> = segs
                .iter()
                .map(|a| a.as_ref() as &dyn arrow_array::Array)
                .collect();
            let conc = concat_arrays(&anys).expect("concat lowered segments");
            out.push(conc);
        }
        out
    }

    /// Slice typed float arrays for a specific row interval (relative to view).
    pub fn slice_numbers(
        &self,
        rel_start: usize,
        len: usize,
    ) -> Vec<Option<Arc<arrow_array::Float64Array>>> {
        if let Some(tail) = &self.tail {
            let date_system = self.sheet().date_system;
            if len > 0
                && rel_start >= tail.active_rows
                && rel_start.saturating_add(len) <= self.rows
            {
                let mut out_cols = Vec::with_capacity(self.cols);
                for c in 0..self.cols {
                    let val = if c < tail.active_cols {
                        tail.bottom_tail.get(c).unwrap_or(&LiteralValue::Empty)
                    } else {
                        &tail.corner_tail
                    };
                    if let Some(n) = range_numeric_value(val, date_system) {
                        out_cols.push(Some(Arc::new(arrow_array::Float64Array::from(vec![
                            n;
                            len
                        ]))));
                    } else {
                        out_cols.push(None);
                    }
                }
                return out_cols;
            }
            let mut out_cols = Vec::with_capacity(self.cols);
            for c in 0..self.cols {
                let mut b = arrow_array::builder::Float64Builder::with_capacity(len);
                let mut has_any = false;
                for offset in 0..len {
                    match range_numeric_value(&self.get_cell(rel_start + offset, c), date_system) {
                        Some(n) => {
                            b.append_value(n);
                            has_any = true;
                        }
                        None => b.append_null(),
                    }
                }
                if has_any {
                    out_cols.push(Some(Arc::new(b.finish())));
                } else {
                    out_cols.push(None);
                }
            }
            return out_cols;
        }
        let abs_start = self.sr + rel_start;
        let abs_end = abs_start + len;
        let sheet = self.sheet();
        let chunk_starts = &sheet.chunk_starts;

        let mut out_cols = Vec::with_capacity(self.cols);
        for col_idx in self.sc..=self.ec {
            if col_idx >= sheet.columns.len() {
                out_cols.push(None);
                continue;
            }
            let col = &sheet.columns[col_idx];

            let start_ch_idx = match chunk_starts.binary_search(&abs_start) {
                Ok(i) => i,
                Err(0) => 0,
                Err(i) => i - 1,
            };

            let mut segments: Vec<Arc<arrow_array::Float64Array>> = Vec::new();
            let mut null_only = true;

            let mut curr = abs_start;
            let mut remaining = len;
            let mut ch_idx = start_ch_idx;

            while remaining > 0 && ch_idx < chunk_starts.len() {
                let ch_start = chunk_starts[ch_idx];
                let ch_end = chunk_starts
                    .get(ch_idx + 1)
                    .copied()
                    .unwrap_or(sheet.nrows as usize);
                let ch_len = ch_end.saturating_sub(ch_start);
                if ch_len == 0 {
                    ch_idx += 1;
                    continue;
                }

                let overlap_start = curr.max(ch_start);
                let overlap_end = ch_end.min(abs_end);

                if overlap_start < overlap_end {
                    let seg_len = overlap_end - overlap_start;
                    let rel_off_in_chunk = overlap_start - ch_start;

                    if let Some(ch) = col.chunk(ch_idx) {
                        let base_nums_arc = ch.numbers_or_null();
                        let base_nums = base_nums_arc.as_ref();

                        let seg_range = rel_off_in_chunk..(rel_off_in_chunk + seg_len);
                        let cascade =
                            arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);

                        let final_arr = if cascade.has_any_in_range(seg_range.clone()) {
                            let base_slice = base_nums.slice(rel_off_in_chunk, seg_len);
                            let base_fa = base_slice
                                .as_any()
                                .downcast_ref::<arrow_array::Float64Array>()
                                .unwrap();
                            cascade.select_numbers(seg_range, base_fa).as_ref().clone()
                        } else {
                            let sl = base_nums.slice(rel_off_in_chunk, seg_len);
                            sl.as_any()
                                .downcast_ref::<arrow_array::Float64Array>()
                                .unwrap()
                                .clone()
                        };

                        if final_arr.null_count() < final_arr.len() {
                            null_only = false;
                        }
                        segments.push(Arc::new(final_arr));
                    } else {
                        segments.push(Arc::new(arrow_array::Float64Array::new_null(seg_len)));
                    }
                    curr += seg_len;
                    remaining -= seg_len;
                }
                ch_idx += 1;
            }

            if remaining > 0 {
                segments.push(Arc::new(arrow_array::Float64Array::new_null(remaining)));
            }

            if segments.len() == 1 {
                if null_only && segments[0].null_count() == segments[0].len() {
                    out_cols.push(None);
                } else {
                    out_cols.push(Some(segments.pop().unwrap()));
                }
            } else {
                let refs: Vec<&dyn Array> =
                    segments.iter().map(|a| a.as_ref() as &dyn Array).collect();
                let c = crate::compute_prelude::concat_engine_arrays(&refs).expect("concat slice");
                let fa = c
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .unwrap()
                    .clone();
                out_cols.push(Some(Arc::new(fa)));
            }
        }
        out_cols
    }

    /// Slice typed lowered text arrays for a specific row interval (relative to view).
    pub fn slice_lowered_text(
        &self,
        rel_start: usize,
        len: usize,
    ) -> Vec<Option<Arc<arrow_array::StringArray>>> {
        if self.tail.is_some() {
            let mut out_cols = Vec::with_capacity(self.cols);
            for c in 0..self.cols {
                let mut b = arrow_array::builder::StringBuilder::with_capacity(len, len * 4);
                let mut has_any = false;
                for offset in 0..len {
                    match self.get_cell(rel_start + offset, c) {
                        LiteralValue::Text(t) => {
                            b.append_value(t.to_lowercase());
                            has_any = true;
                        }
                        _ => b.append_null(),
                    }
                }
                if has_any {
                    out_cols.push(Some(Arc::new(b.finish())));
                } else {
                    out_cols.push(None);
                }
            }
            return out_cols;
        }
        let abs_start = self.sr + rel_start;
        let abs_end = abs_start + len;
        let sheet = self.sheet();
        let chunk_starts = &sheet.chunk_starts;

        let mut out_cols = Vec::with_capacity(self.cols);
        for col_idx in self.sc..=self.ec {
            if col_idx >= sheet.columns.len() {
                out_cols.push(None);
                continue;
            }
            let col = &sheet.columns[col_idx];
            let start_ch_idx = match chunk_starts.binary_search(&abs_start) {
                Ok(i) => i,
                Err(0) => 0,
                Err(i) => i - 1,
            };

            let mut segments: Vec<Arc<arrow_array::StringArray>> = Vec::new();
            let mut null_only = true;

            let mut curr = abs_start;
            let mut remaining = len;
            let mut ch_idx = start_ch_idx;

            while remaining > 0 && ch_idx < chunk_starts.len() {
                let ch_start = chunk_starts[ch_idx];
                let ch_end = chunk_starts
                    .get(ch_idx + 1)
                    .copied()
                    .unwrap_or(sheet.nrows as usize);
                let ch_len = ch_end.saturating_sub(ch_start);
                if ch_len == 0 {
                    ch_idx += 1;
                    continue;
                }

                let overlap_start = curr.max(ch_start);
                let overlap_end = ch_end.min(abs_end);

                if overlap_start < overlap_end {
                    let seg_len = overlap_end - overlap_start;
                    let rel_off_in_chunk = overlap_start - ch_start;

                    if let Some(ch) = col.chunk(ch_idx) {
                        let base_lowered = ch.text_lower_or_null();
                        let seg_range = rel_off_in_chunk..(rel_off_in_chunk + seg_len);
                        let cascade =
                            arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);

                        let final_arr = if cascade.has_any_in_range(seg_range.clone()) {
                            let base_slice = base_lowered.slice(rel_off_in_chunk, seg_len);
                            let base_sa = base_slice
                                .as_any()
                                .downcast_ref::<arrow_array::StringArray>()
                                .unwrap();
                            cascade
                                .select_lowered_text(seg_range, base_sa)
                                .as_ref()
                                .clone()
                        } else {
                            let sl = base_lowered.slice(rel_off_in_chunk, seg_len);
                            sl.as_any()
                                .downcast_ref::<arrow_array::StringArray>()
                                .unwrap()
                                .clone()
                        };

                        if final_arr.null_count() < final_arr.len() {
                            null_only = false;
                        }
                        segments.push(Arc::new(final_arr));
                    } else {
                        segments.push(Arc::new(arrow_array::StringArray::new_null(seg_len)));
                    }
                    curr += seg_len;
                    remaining -= seg_len;
                }
                ch_idx += 1;
            }

            if remaining > 0 {
                segments.push(Arc::new(arrow_array::StringArray::new_null(remaining)));
            }

            if segments.len() == 1 {
                if null_only && segments[0].null_count() == segments[0].len() {
                    out_cols.push(None);
                } else {
                    out_cols.push(Some(segments.pop().unwrap()));
                }
            } else {
                let refs: Vec<&dyn Array> =
                    segments.iter().map(|a| a.as_ref() as &dyn Array).collect();
                let c = crate::compute_prelude::concat_engine_arrays(&refs).expect("concat text");
                let sa = c
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap()
                    .clone();
                out_cols.push(Some(Arc::new(sa)));
            }
        }
        out_cols
    }
}

/// Uniform-tail-aware operand for elementwise operators and array functions.
pub(crate) enum TailedOperand<'a> {
    Range(RangeView<'a>),
    Array(Vec<Vec<LiteralValue>>),
    Scalar(LiteralValue),
}

impl<'a> TailedOperand<'a> {
    pub(crate) fn from_calc_value(value: crate::traits::CalcValue<'a>) -> Self {
        match value {
            crate::traits::CalcValue::Range(rv) => {
                if rv.dims() == (1, 1) {
                    Self::Scalar(rv.get_cell(0, 0))
                } else {
                    Self::Range(rv)
                }
            }
            crate::traits::CalcValue::Scalar(LiteralValue::Array(rows))
            | crate::traits::CalcValue::AnnotatedScalar(LiteralValue::Array(rows), _) => {
                Self::Array(rows)
            }
            other => Self::Scalar(other.into_literal()),
        }
    }

    #[inline]
    pub(crate) fn dims(&self) -> (usize, usize) {
        match self {
            Self::Range(rv) => rv.dims(),
            Self::Array(arr) => (arr.len(), arr.first().map_or(0, Vec::len)),
            Self::Scalar(_) => (1, 1),
        }
    }

    #[inline]
    pub(crate) fn active_dims(&self) -> (usize, usize) {
        match self {
            Self::Range(rv) => rv.active_dims(),
            Self::Array(arr) => (arr.len(), arr.first().map_or(0, Vec::len)),
            Self::Scalar(_) => (1, 1),
        }
    }

    #[inline]
    pub(crate) fn has_uniform_tail(&self) -> bool {
        match self {
            Self::Range(rv) => rv.has_uniform_tail(),
            Self::Array(_) | Self::Scalar(_) => false,
        }
    }

    #[inline]
    pub(crate) fn get_core(&self, r: usize, c: usize) -> LiteralValue {
        let shape = self.dims();
        let (pr, pc) = crate::broadcast::project_index((r, c), shape);
        match self {
            Self::Range(rv) => rv.get_cell(pr, pc),
            Self::Array(arr) => arr
                .get(pr)
                .and_then(|row| row.get(pc))
                .cloned()
                .unwrap_or(LiteralValue::Empty),
            Self::Scalar(v) => v.clone(),
        }
    }

    #[inline]
    pub(crate) fn get_right_tail(&self, r: usize) -> LiteralValue {
        let (rows, cols) = self.dims();
        if cols == 1 {
            return self.get_core(r, 0);
        }
        let pr = if rows == 1 { 0 } else { r };
        match self {
            Self::Range(rv) => {
                let (ar, _) = rv.active_dims();
                if pr < ar {
                    rv.right_tail_cell(pr)
                } else {
                    rv.corner_tail_cell()
                }
            }
            Self::Array(_) | Self::Scalar(_) => {
                unreachable!("Array/Scalar with cols>1 has no right tail")
            }
        }
    }

    #[inline]
    pub(crate) fn get_bottom_tail(&self, c: usize) -> LiteralValue {
        let (rows, cols) = self.dims();
        if rows == 1 {
            return self.get_core(0, c);
        }
        let pc = if cols == 1 { 0 } else { c };
        match self {
            Self::Range(rv) => {
                let (_, ac) = rv.active_dims();
                if pc < ac {
                    rv.bottom_tail_cell(pc)
                } else {
                    rv.corner_tail_cell()
                }
            }
            Self::Array(_) | Self::Scalar(_) => {
                unreachable!("Array/Scalar with rows>1 has no bottom tail")
            }
        }
    }

    #[inline]
    pub(crate) fn get_corner_tail(&self) -> LiteralValue {
        let (rows, cols) = self.dims();
        match (rows == 1, cols == 1) {
            (true, true) => self.get_core(0, 0),
            (true, false) => self.get_right_tail(0),
            (false, true) => self.get_bottom_tail(0),
            (false, false) => match self {
                Self::Range(rv) => rv.corner_tail_cell(),
                Self::Array(_) | Self::Scalar(_) => {
                    unreachable!("Array/Scalar with rows>1 && cols>1 has no corner tail")
                }
            },
        }
    }
}

/// Computes the combined active core dimensions `(active_rows, active_cols)` for a
/// broadcast output of logical shape `target` produced elementwise from `operands`.
pub(crate) fn combined_active_dims(
    target: (usize, usize),
    operands: &[&TailedOperand<'_>],
) -> (usize, usize) {
    let active_rows = if target.0 == 0 {
        0
    } else if target.0 == 1 {
        1
    } else {
        operands
            .iter()
            .filter(|op| op.dims().0 > 1)
            .map(|op| op.active_dims().0)
            .max()
            .unwrap_or(1)
            .min(target.0)
    };
    let active_cols = if target.1 == 0 {
        0
    } else if target.1 == 1 {
        1
    } else {
        operands
            .iter()
            .filter(|op| op.dims().1 > 1)
            .map(|op| op.active_dims().1)
            .max()
            .unwrap_or(1)
            .min(target.1)
    };
    (active_rows, active_cols)
}

/// Position inside a tailed 2D grid passed to [`build_tailed_range_view`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum TailPos {
    Core(usize, usize),
    Right(usize),
    Bottom(usize),
    Corner,
}

/// Streams an elementwise core `(0..active.0, 0..active.1)` plus uniform tails into a
/// compact `RangeView<'static>` of logical shape `target`.
pub(crate) fn build_tailed_range_view(
    target: (usize, usize),
    active: (usize, usize),
    date_system: DateSystem,
    cancel_token: Option<CancelToken>,
    cell_budget: u64,
    mut eval: impl FnMut(TailPos) -> Result<LiteralValue, ExcelError>,
) -> Result<RangeView<'static>, ExcelError> {
    let (rows, cols) = target;
    let (active_rows, active_cols) = active;
    let mut ib = IngestBuilder::new("__tmp", active_cols, 32 * 1024, date_system);
    let mut since_poll = 4096usize;
    let check_cancel = |since_poll: &mut usize| -> Result<(), ExcelError> {
        if *since_poll >= 4096 {
            *since_poll = 0;
            if cancel_token.as_ref().is_some_and(CancelToken::is_cancelled) {
                return Err(ExcelError::new(
                    formualizer_common::ExcelErrorKind::Cancelled,
                ));
            }
        }
        *since_poll += 1;
        Ok(())
    };

    if active_rows > 0 && active_cols > 0 {
        let mut row_buf = Vec::with_capacity(active_cols);
        for r in 0..active_rows {
            row_buf.clear();
            for c in 0..active_cols {
                check_cancel(&mut since_poll)?;
                row_buf.push(eval(TailPos::Core(r, c))?);
            }
            ib.append_row(&row_buf)
                .expect("append_row for tailed RangeView");
        }
    }
    let mut core_sheet = ib.finish();
    if active_rows > 0 && active_cols == 0 {
        core_sheet.ensure_row_capacity(active_rows);
    }

    let mut right_tail = Vec::new();
    if active_cols < cols && active_rows > 0 {
        right_tail.reserve(active_rows);
        for r in 0..active_rows {
            check_cancel(&mut since_poll)?;
            right_tail.push(eval(TailPos::Right(r))?);
        }
    }

    let mut bottom_tail = Vec::new();
    if active_rows < rows && active_cols > 0 {
        bottom_tail.reserve(active_cols);
        for c in 0..active_cols {
            check_cancel(&mut since_poll)?;
            bottom_tail.push(eval(TailPos::Bottom(c))?);
        }
    }

    let corner_tail = if active_rows < rows && active_cols < cols {
        check_cancel(&mut since_poll)?;
        eval(TailPos::Corner)?
    } else {
        LiteralValue::Empty
    };

    Ok(RangeView::from_core_and_tail(
        rows,
        cols,
        active_rows,
        active_cols,
        Arc::new(core_sheet),
        right_tail,
        bottom_tail,
        corner_tail,
        cancel_token,
        cell_budget,
    ))
}

#[inline]
fn pack_numeric(v: &LiteralValue, policy: CoercionPolicy) -> Result<Option<f64>, ExcelError> {
    match policy {
        CoercionPolicy::NumberLenientText => match v {
            LiteralValue::Error(e) => Err(e.clone()),
            LiteralValue::Empty => Ok(None),
            other => Ok(crate::coercion::to_number_lenient(other).ok()),
        },
        CoercionPolicy::NumberStrict => match v {
            LiteralValue::Error(e) => Err(e.clone()),
            LiteralValue::Empty => Ok(None),
            other => Ok(crate::coercion::to_number_strict(other).ok()),
        },
        _ => match v {
            LiteralValue::Error(e) => Err(e.clone()),
            _ => Ok(None),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_rows_numeric_chunking() {
        let data: Vec<Vec<LiteralValue>> = vec![
            vec![
                LiteralValue::Number(1.0),
                LiteralValue::Text("x".into()),
                LiteralValue::Number(3.0),
            ],
            vec![
                LiteralValue::Boolean(true),
                LiteralValue::Empty,
                LiteralValue::Number(2.5),
            ],
        ];
        let view = RangeView::from_owned_rows(data, DateSystem::Excel1900);
        let mut sum = 0.0f64;
        view.numbers_chunked(CoercionPolicy::NumberLenientText, 2, &mut |chunk| {
            for &n in chunk.data {
                sum += n;
            }
            Ok(())
        })
        .unwrap();
        assert!((sum - 7.5).abs() < 1e-9);
    }

    #[test]
    fn as_1x1_works() {
        let view = RangeView::from_owned_rows(
            vec![vec![LiteralValue::Number(7.0)]],
            DateSystem::Excel1900,
        );
        assert_eq!(view.as_1x1(), Some(LiteralValue::Number(7.0)));
    }

    #[test]
    fn pre_cancelled_token_stops_owned_row_construction() {
        let token = CancelToken::new();
        token.cancel();

        let error = RangeView::try_from_owned_rows(
            vec![vec![LiteralValue::Number(1.0)]],
            DateSystem::Excel1900,
            Some(token),
        )
        .unwrap_err();

        assert_eq!(error.kind, formualizer_common::ExcelErrorKind::Cancelled);
    }

    #[test]
    fn pre_cancelled_token_stops_row_chunk_iteration() {
        let token = CancelToken::new();
        token.cancel();
        let view = RangeView::from_owned_rows(
            vec![vec![LiteralValue::Number(1.0)]],
            DateSystem::Excel1900,
        )
        .with_cancel_token(Some(token));

        let Some(Err(error)) = view.iter_row_chunks().next() else {
            panic!("pre-cancelled chunk iteration should return cancellation");
        };

        assert_eq!(error.kind, formualizer_common::ExcelErrorKind::Cancelled);
    }
}
#[cfg(test)]
mod bounded_projection_tests {
    use super::*;
    use crate::arrow_store::{ArrowSheet, OverlayFragment, OverlayValue};
    use arrow_array::{Float64Array, UInt8Array};
    use formualizer_common::ExcelErrorKind;

    // Deliberately retain the exhaustive baseline intersection algorithm as an oracle.
    fn segments(view: &RangeView<'_>) -> Vec<(usize, usize, usize, usize)> {
        let sheet = view.sheet();
        let mut out = Vec::new();
        let row_end = view.er.min((sheet.nrows as usize).saturating_sub(1));
        for (ci, &start) in sheet.chunk_starts.iter().enumerate() {
            let end = sheet
                .chunk_starts
                .get(ci + 1)
                .copied()
                .unwrap_or(sheet.nrows as usize);
            let len = end.saturating_sub(start);
            if len == 0 {
                continue;
            }
            let lo = start.max(view.sr);
            let hi = (start + len - 1).min(row_end);
            if lo <= hi {
                out.push((ci, lo - start, lo - view.sr, hi - lo + 1));
            }
        }
        out
    }

    fn assert_projection(view: &RangeView<'_>) {
        let expected = segments(view);
        let generic: Vec<_> = view.iter_row_chunks().map(Result::unwrap).collect();
        assert_eq!(
            generic
                .iter()
                .map(|s| (s.row_start, s.row_len))
                .collect::<Vec<_>>(),
            expected.iter().map(|s| (s.2, s.3)).collect::<Vec<_>>()
        );
        let numbers: Vec<_> = view.numbers_slices().map(Result::unwrap).collect();
        let errors: Vec<_> = view.errors_slices().map(Result::unwrap).collect();
        assert_eq!(numbers.len(), expected.len());
        assert_eq!(errors.len(), expected.len());
        for (seg, &(ci, offset, row_start, len)) in expected.iter().enumerate() {
            assert_eq!((numbers[seg].0, numbers[seg].1), (row_start, len));
            assert_eq!((errors[seg].0, errors[seg].1), (row_start, len));
            let columns = (view.sc..=view.ec).count();
            assert_eq!(generic[seg].cols.len(), columns);
            assert_eq!(numbers[seg].2.len(), columns);
            assert_eq!(errors[seg].2.len(), columns);
            for (c, absolute) in (view.sc..=view.ec).enumerate() {
                let (base_n, base_e) =
                    match view.sheet().columns.get(absolute).and_then(|c| c.chunk(ci)) {
                        Some(ch) => {
                            // Access physical lanes directly, without the optimized cursor/providers.
                            let n = ch
                                .numbers
                                .as_ref()
                                .map(|a| a.slice(offset, len))
                                .unwrap_or_else(|| Float64Array::new_null(len));
                            let e = ch
                                .errors
                                .as_ref()
                                .map(|a| a.slice(offset, len))
                                .unwrap_or_else(|| UInt8Array::new_null(len));
                            let cascade =
                                arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                            (
                                cascade.select_numbers(offset..offset + len, &n),
                                cascade.select_errors(offset..offset + len, &e),
                            )
                        }
                        None => (
                            Arc::new(Float64Array::new_null(len)),
                            Arc::new(UInt8Array::new_null(len)),
                        ),
                    };
                let actual_n = &numbers[seg].2[c];
                let actual_e = &errors[seg].2[c];
                assert_eq!(actual_n.len(), len);
                assert_eq!(actual_e.len(), len);
                for i in 0..len {
                    assert_eq!(actual_n.is_null(i), base_n.is_null(i));
                    assert_eq!(actual_e.is_null(i), base_e.is_null(i));
                    if !base_n.is_null(i) {
                        assert_eq!(actual_n.value(i).to_bits(), base_n.value(i).to_bits());
                    }
                    if !base_e.is_null(i) {
                        assert_eq!(actual_e.value(i), base_e.value(i));
                    }
                }
            }
        }
    }

    fn fixture(chunk_rows: usize) -> ArrowSheet {
        let mut ingest = IngestBuilder::new("S", 3, chunk_rows, DateSystem::Excel1900);
        for row in 0..11 {
            ingest
                .append_row(&[
                    LiteralValue::Number(row as f64),
                    if row % 3 == 0 {
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div))
                    } else {
                        LiteralValue::Empty
                    },
                    LiteralValue::Text(format!("r{row}")),
                ])
                .unwrap();
        }
        let mut sheet = ingest.finish();
        sheet.ensure_row_capacity(29);
        for (row, value) in [
            (1, OverlayValue::Empty),
            (3, OverlayValue::Text(Arc::from("mask"))),
            (
                7,
                OverlayValue::Error(arrow_store::map_error_code(ExcelErrorKind::Na)),
            ),
            (22, OverlayValue::Number(17.0)),
            (28, OverlayValue::Pending),
        ] {
            let (ci, off) = sheet.chunk_of_row(row).unwrap();
            let chunk = sheet.ensure_column_chunk_mut(0, ci).unwrap();
            chunk.computed_overlay.set(off, OverlayValue::Number(99.0));
            chunk.overlay.set(off, value);
        }
        for (row, user) in [(0, OverlayValue::Empty), (8, OverlayValue::Number(17.0))] {
            let (ci, off) = sheet.chunk_of_row(row).unwrap();
            let ch = sheet.ensure_column_chunk_mut(1, ci).unwrap();
            ch.computed_overlay.set(
                off,
                OverlayValue::Error(arrow_store::map_error_code(ExcelErrorKind::Na)),
            );
            ch.overlay.set(off, user);
        }
        sheet
    }

    #[test]
    fn range_projection_matches_independent_oracle_for_all_small_bounds() {
        for chunk_rows in [1, 3, 8, 32] {
            let sheet = fixture(chunk_rows);
            for sr in 0..=31 {
                for er in 0..=31 {
                    for (sc, ec) in [(0, 0), (1, 2), (0, 4), (4, 5), (2, 1)] {
                        assert_projection(&sheet.range_view(sr, sc, er, ec));
                    }
                }
            }
            let base = sheet.range_view(2, 1, 10, 2);
            for rows in [0, 1, 9, 40] {
                for cols in [0, 1, 4] {
                    assert_projection(&base.sub_view(1, 1, rows, cols));
                    assert_projection(&base.expand_to(rows, cols));
                }
            }
        }
        let empty = IngestBuilder::new("S", 1, 4, DateSystem::Excel1900).finish();
        assert_projection(&empty.range_view(0, 0, 7, 3));
    }

    #[test]
    fn range_projection_preserves_dense_run_and_partial_overlay_paths() {
        for run in [false, true] {
            let mut sheet = fixture(32);
            let chunk = &mut sheet.columns[0].chunks[0];
            chunk.overlay.clear();
            chunk.computed_overlay.clear();
            let values = vec![OverlayValue::Number(7.0); 29];
            let fragment = if run {
                OverlayFragment::run_range(0, values)
            } else {
                OverlayFragment::dense_range(0, values)
            }
            .unwrap();
            chunk.computed_overlay.apply_fragment(fragment);
            let view = sheet.range_view(2, 0, 9, 0);
            arrow_store::reset_overlay_select_stats();
            let numeric = view.numbers_slices().next().unwrap().unwrap();
            assert_eq!(numeric.2[0].value(0), 7.0);
            let stats = arrow_store::snapshot_overlay_select_stats();
            assert_eq!(stats.zip_select_calls, 0);
            assert_eq!(stats.direct_dense_slices, usize::from(!run));
            assert_eq!(stats.direct_run_materializations, usize::from(run));
            assert_projection(&view);
            let chunk = &mut sheet.columns[0].chunks[0];
            for (off, value) in [
                (3, OverlayValue::Empty),
                (4, OverlayValue::Pending),
                (5, OverlayValue::DateTime(45000.25)),
                (6, OverlayValue::Duration(0.5)),
                (7, OverlayValue::Boolean(true)),
                (
                    8,
                    OverlayValue::Error(arrow_store::map_error_code(ExcelErrorKind::Ref)),
                ),
                (9, OverlayValue::Text(Arc::from("not numeric"))),
            ] {
                chunk.overlay.set(off, value);
            }
            assert_projection(&sheet.range_view(2, 0, 10, 3));
        }
    }

    #[test]
    fn range_discovery_work_is_bounded_and_projection_is_lane_local() {
        let mut ingest = IngestBuilder::new("S", 1, 256, DateSystem::Excel1900);
        for _ in 0..256 {
            ingest.append_row(&[LiteralValue::Number(1.0)]).unwrap();
        }
        let mut sheet = ingest.finish();
        sheet.ensure_row_capacity(256 * 4096);
        for start in [0, 256 * 2048 + 1, 256 * 4096 - 8, 256 * 4096 + 8] {
            let view = sheet.range_view(start, 0, start + 7, 2);
            range_work::begin();
            let generic: Vec<_> = view.iter_row_chunks().collect();
            let work = range_work::take();
            assert_eq!(work.candidates, generic.len());
            assert_eq!(work.segments, generic.len());
            assert!(work.search_probes <= 32);
            range_work::begin();
            let _: Vec<_> = view.numbers_slices().collect();
            let numeric = range_work::take();
            assert_eq!(numeric.generic_columns, 0);
            assert_eq!(numeric.provider_requests[1..], [0, 0, 0]);
            assert!(numeric.null_arrays <= 3);
            range_work::begin();
            let _: Vec<_> = view.errors_slices().collect();
            let errors = range_work::take();
            assert_eq!(errors.generic_columns, 0);
            assert_eq!(errors.provider_requests[0], 0);
            assert_eq!(errors.provider_requests[1], 0);
            assert_eq!(errors.provider_requests[3], 0);
        }
        let view = sheet.range_view(0, 0, sheet.nrows as usize - 1, 0);
        range_work::begin();
        assert!(view.iter_row_chunks().next().unwrap().is_ok());
        let work = range_work::take();
        assert_eq!(work.candidates, 1);
        assert_eq!(work.segments, 1);
    }

    #[test]
    fn range_cold_numeric_does_not_initialize_unused_null_providers() {
        let mut ingest = IngestBuilder::new("S", 1, 32768, DateSystem::Excel1900);
        for _ in 0..32768 {
            ingest.append_row(&[LiteralValue::Number(1.0)]).unwrap();
        }
        let sheet = ingest.finish();
        let view = sheet.range_view(1, 0, 8, 0);
        range_work::begin();
        view.numbers_slices().next().unwrap().unwrap();
        let numeric = range_work::take();
        assert_eq!(numeric.provider_requests, [1, 0, 0, 0]);
        assert_eq!(numeric.provider_builds, [0; 4]);
        range_work::begin();
        view.errors_slices().next().unwrap().unwrap();
        let errors = range_work::take();
        assert_eq!(errors.provider_requests, [0, 0, 1, 0]);
        assert_eq!(errors.provider_builds, [0, 0, 1, 0]);
        assert_eq!(errors.provider_slots, [0, 0, 32768, 0]);
    }

    /// One 32,768-row chunk: column 0 numbers only, column 1 a dense computed
    /// overlay of `#DIV/0!` with `#N/A` at row 30,000, column 2 a single user
    /// `#REF!` point at row 20,000.
    fn dense_error_sheet() -> ArrowSheet {
        let rows = 32_768;
        let mut ingest = IngestBuilder::new("S", 3, rows, DateSystem::Excel1900);
        for row in 0..rows {
            ingest
                .append_row(&[
                    LiteralValue::Number(row as f64),
                    LiteralValue::Number(1.0),
                    LiteralValue::Empty,
                ])
                .unwrap();
        }
        let mut sheet = ingest.finish();
        let mut dense =
            vec![OverlayValue::Error(arrow_store::map_error_code(ExcelErrorKind::Div)); rows];
        dense[30_000] = OverlayValue::Error(arrow_store::map_error_code(ExcelErrorKind::Na));
        sheet
            .ensure_column_chunk_mut(1, 0)
            .unwrap()
            .computed_overlay
            .apply_fragment(OverlayFragment::dense_range(0, dense).unwrap());
        sheet.ensure_column_chunk_mut(2, 0).unwrap().overlay.set(
            20_000,
            OverlayValue::Error(arrow_store::map_error_code(ExcelErrorKind::Ref)),
        );
        sheet
    }

    fn error_codes_by_slices(view: &RangeView<'_>) -> Vec<u8> {
        let mut out = Vec::new();
        for segment in view.errors_slices() {
            for lane in segment.unwrap().2 {
                out.extend(lane.iter().flatten());
            }
        }
        out.sort_unstable();
        out
    }

    #[test]
    fn error_pieces_are_bounded_and_match_errors_slices() {
        let sheet = dense_error_sheet();
        for (sr, er) in [(0, 32_767), (5, 30_001), (29_999, 30_000)] {
            let view = sheet.range_view(sr, 0, er, 2);
            let mut codes = Vec::new();
            range_work::begin();
            let stopped = view
                .try_for_each_error_piece(4096, &mut |lane| {
                    assert!(lane.len() <= 4096);
                    codes.extend(lane.iter().flatten());
                    Ok(false)
                })
                .unwrap();
            let work = range_work::take();
            assert!(!stopped);
            codes.sort_unstable();
            assert_eq!(codes, error_codes_by_slices(&view), "rows {sr}..={er}");
            assert!(work.error_piece_max_rows <= 4096);
            // No whole-chunk null error lane is materialized for columns
            // without an error lane.
            assert_eq!(work.provider_builds[2], 0);
        }
    }

    #[test]
    fn error_pieces_stop_early_and_observe_cancellation_between_pieces() {
        let sheet = dense_error_sheet();
        // Dense overlay: cancellation raised while visiting a piece stops
        // preparation of the next one.
        let token = CancelToken::new();
        let view = sheet
            .range_view(0, 1, 32_767, 1)
            .with_cancel_token(Some(token.clone()));
        let mut visits = 0;
        range_work::begin();
        let error = view
            .try_for_each_error_piece(4096, &mut |_| {
                visits += 1;
                if visits == 2 {
                    token.cancel();
                }
                Ok(false)
            })
            .unwrap_err();
        let work = range_work::take();
        assert_eq!(error.kind, ExcelErrorKind::Cancelled);
        assert_eq!(visits, 2);
        assert_eq!(work.error_pieces, 2);

        // Early stop reports a match without preparing further pieces.
        let view = sheet.range_view(0, 1, 32_767, 1);
        range_work::begin();
        assert!(
            view.try_for_each_error_piece(4096, &mut |_| Ok(true))
                .unwrap()
        );
        assert_eq!(range_work::take().error_pieces, 1);

        // Wide single-row view: every column is a piece, and cancellation is
        // observed between columns.
        let mut ingest = IngestBuilder::new("W", 64, 8, DateSystem::Excel1900);
        ingest
            .append_row(&vec![
                LiteralValue::Error(ExcelError::new(
                    ExcelErrorKind::Div
                ));
                64
            ])
            .unwrap();
        let wide = ingest.finish();
        let token = CancelToken::new();
        let view = wide
            .range_view(0, 0, 0, 63)
            .with_cancel_token(Some(token.clone()));
        let mut visits = 0;
        let error = view
            .try_for_each_error_piece(4096, &mut |_| {
                visits += 1;
                token.cancel();
                Ok(false)
            })
            .unwrap_err();
        assert_eq!(error.kind, ExcelErrorKind::Cancelled);
        assert_eq!(visits, 1);
    }

    #[test]
    fn range_cancellation_keeps_empty_exhausted_and_between_segment_errors() {
        let sheet = fixture(3);
        for bounds in [(0, 0, 28, 0), (30, 0, 35, 0), (3, 0, 1, 0), (0, 2, 5, 1)] {
            let token = CancelToken::new();
            let view = sheet
                .range_view(bounds.0, bounds.1, bounds.2, bounds.3)
                .with_cancel_token(Some(token.clone()));
            let mut generic = view.iter_row_chunks();
            let mut numeric = view.numbers_slices();
            let mut errors = view.errors_slices();
            token.cancel();
            for _ in 0..2 {
                assert_eq!(
                    generic.next().unwrap().err().unwrap().kind,
                    ExcelErrorKind::Cancelled
                );
                assert_eq!(
                    numeric.next().unwrap().err().unwrap().kind,
                    ExcelErrorKind::Cancelled
                );
                assert_eq!(
                    errors.next().unwrap().err().unwrap().kind,
                    ExcelErrorKind::Cancelled
                );
            }
        }
        let token = CancelToken::new();
        let view = sheet
            .range_view(0, 0, 28, 0)
            .with_cancel_token(Some(token.clone()));
        let mut numeric = view.numbers_slices();
        assert!(numeric.next().unwrap().is_ok());
        token.cancel();
        assert_eq!(
            numeric.next().unwrap().err().unwrap().kind,
            ExcelErrorKind::Cancelled
        );
        let token = CancelToken::new();
        let view = sheet
            .range_view(30, 0, 35, 0)
            .with_cancel_token(Some(token.clone()));
        let mut numeric = view.numbers_slices();
        assert!(numeric.next().is_none());
        token.cancel();
        assert_eq!(
            numeric.next().unwrap().err().unwrap().kind,
            ExcelErrorKind::Cancelled
        );
    }

    #[test]
    fn range_projection_keeps_numeric_bits_and_engine_error_order() {
        let values = [
            0.0,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::from_bits(0x7ff8000000000001),
        ];
        let view = RangeView::from_owned_rows(
            values
                .iter()
                .map(|n| vec![LiteralValue::Number(*n)])
                .collect(),
            DateSystem::Excel1900,
        );
        assert_projection(&view);
        let slice = view.numbers_slices().next().unwrap().unwrap().2.remove(0);
        for (i, n) in values.iter().enumerate() {
            assert_eq!(slice.value(i).to_bits(), n.to_bits());
        }
        for chunk_rows in [1, 2] {
            let mut engine = crate::engine::Engine::new(
                crate::test_workbook::TestWorkbook::new(),
                crate::engine::EvalConfig {
                    arrow_storage_enabled: true,
                    delta_overlay_enabled: true,
                    write_formula_overlay_enabled: true,
                    enable_parallel: false,
                    ..Default::default()
                },
            );
            let mut ingest = engine.begin_bulk_ingest_arrow();
            ingest.add_sheet("Source", 2, chunk_rows);
            ingest
                .append_row(
                    "Source",
                    &[
                        LiteralValue::Number(1.0),
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na)),
                    ],
                )
                .unwrap();
            ingest
                .append_row(
                    "Source",
                    &[
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)),
                        LiteralValue::Number(2.0),
                    ],
                )
                .unwrap();
            ingest.finish().unwrap();
            engine
                .set_cell_formula(
                    "Result",
                    1,
                    1,
                    formualizer_parse::parser::parse("=SUM(Source!A1:B2)").unwrap(),
                )
                .unwrap();
            engine.evaluate_all().unwrap();
            let expected = if chunk_rows == 1 {
                ExcelErrorKind::Na
            } else {
                ExcelErrorKind::Div
            };
            assert!(
                matches!(engine.get_cell_value("Result", 1, 1), Some(LiteralValue::Error(e)) if e.kind == expected)
            );
        }
    }

    #[test]
    fn range_arrays_outlive_owned_view_and_new_views_follow_structural_edits() {
        let numbers = {
            let view = RangeView::from_owned_rows(
                vec![vec![LiteralValue::Number(-0.0)]],
                DateSystem::Excel1900,
            );
            view.numbers_slices().next().unwrap().unwrap().2.remove(0)
        };
        assert_eq!(numbers.value(0).to_bits(), (-0.0f64).to_bits());
        let errors = {
            let view = RangeView::from_owned_rows(
                vec![vec![LiteralValue::Error(ExcelError::new(
                    ExcelErrorKind::Div,
                ))]],
                DateSystem::Excel1900,
            );
            view.errors_slices().next().unwrap().unwrap().2.remove(0)
        };
        assert_eq!(
            errors.value(0),
            arrow_store::map_error_code(ExcelErrorKind::Div)
        );
        let mut sheet = fixture(3);
        let chunks_before: usize = sheet.columns.iter().map(|c| c.total_chunk_count()).sum();
        assert_projection(&sheet.range_view(0, 0, 40, 4));
        assert_eq!(
            sheet
                .columns
                .iter()
                .map(|c| c.total_chunk_count())
                .sum::<usize>(),
            chunks_before
        );
        sheet.insert_rows(4, 2);
        assert_projection(&sheet.range_view(1, 0, 35, 4));
        sheet.delete_rows(2, 1);
        assert_projection(&sheet.range_view(0, 0, 35, 4));
        sheet.ensure_row_capacity(50);
        assert_projection(&sheet.range_view(0, 0, 55, 4));
        sheet.insert_columns(1, 1);
        assert_projection(&sheet.range_view(0, 0, 55, 4));
        sheet.delete_columns(0, 1);
        assert_projection(&sheet.range_view(0, 0, 55, 4));
    }

    #[test]
    fn tailed_expand_to_preserves_invariants_and_pads_with_empty() {
        let mut ib = IngestBuilder::new("__tmp", 2, 32, DateSystem::Excel1900);
        ib.append_row(&[LiteralValue::Number(1.0), LiteralValue::Number(2.0)])
            .unwrap();
        ib.append_row(&[LiteralValue::Number(3.0), LiteralValue::Number(4.0)])
            .unwrap();
        let core = Arc::new(ib.finish());

        // 2D tailed view: 6x5, active 2x2, right_tail=[10,20], bottom_tail=[30,40], corner=50
        let view_2d = RangeView::from_core_and_tail(
            6,
            5,
            2,
            2,
            core,
            vec![LiteralValue::Number(10.0), LiteralValue::Number(20.0)],
            vec![LiteralValue::Number(30.0), LiteralValue::Number(40.0)],
            LiteralValue::Number(50.0),
            None,
            DEFAULT_MATERIALIZED_ARRAY_CELLS,
        );

        // 1D vertical tailed view: 100x1, active 50x1, right_tail=[], bottom_tail=[1.0]
        let mut ib1 = IngestBuilder::new("__tmp", 1, 32, DateSystem::Excel1900);
        for r in 0..50 {
            ib1.append_row(&[LiteralValue::Number((r + 1) as f64)])
                .unwrap();
        }
        let view_1d = RangeView::from_core_and_tail(
            100,
            1,
            50,
            1,
            Arc::new(ib1.finish()),
            Vec::new(),
            vec![LiteralValue::Number(1.0)],
            LiteralValue::Empty,
            None,
            DEFAULT_MATERIALIZED_ARRAY_CELLS,
        );

        // Empty-core tailed view: 6x1, active 0x0, corner=7.0
        let empty_core =
            Arc::new(IngestBuilder::new("__tmp", 0, 32, DateSystem::Excel1900).finish());
        let view_empty_core = RangeView::from_core_and_tail(
            6,
            1,
            0,
            0,
            empty_core,
            Vec::new(),
            Vec::new(),
            LiteralValue::Number(7.0),
            None,
            DEFAULT_MATERIALIZED_ARRAY_CELLS,
        );

        for (base, targets) in [
            (
                &view_2d,
                vec![
                    (0, 0),
                    (2, 2),
                    (4, 3),
                    (1, 7),
                    (9, 1),
                    (9, 5),
                    (6, 8),
                    (9, 8),
                ],
            ),
            (
                &view_1d,
                vec![(10, 2), (100, 2), (120, 1), (120, 3), (10, 1)],
            ),
            (&view_empty_core, vec![(3, 1), (10, 1), (3, 3), (10, 3)]),
        ] {
            let (orig_r, orig_c) = base.dims();
            for (tr, tc) in targets {
                let expanded = base.expand_to(tr, tc);
                assert_eq!(expanded.dims(), (tr, tc));
                if let Some(t) = expanded.tail() {
                    assert!(t.active_rows <= tr);
                    assert!(t.active_cols <= tc);
                }

                // Every cell must match `base` inside `(orig_r, orig_c)` and be `Empty` outside.
                for r in 0..tr {
                    for c in 0..tc {
                        let expected = if r < orig_r && c < orig_c {
                            base.get_cell(r, c)
                        } else {
                            LiteralValue::Empty
                        };
                        assert_eq!(
                            expanded.get_cell(r, c),
                            expected,
                            "cell ({r},{c}) for expand_to({tr},{tc}) from ({orig_r},{orig_c})"
                        );
                    }
                }

                // Chunk/lane iterators must never panic and must match `get_cell`.
                let mut seen_rows = 0usize;
                for seg in expanded.numbers_slices() {
                    let (rs, rl, cols) = seg.unwrap();
                    assert_eq!(cols.len(), tc);
                    for off in 0..rl {
                        for (c, col_arr) in cols.iter().enumerate() {
                            let expected = match expanded.get_cell(rs + off, c) {
                                LiteralValue::Number(n) => Some(n),
                                _ => None,
                            };
                            let actual = (!col_arr.is_null(off)).then(|| col_arr.value(off));
                            assert_eq!(actual, expected);
                        }
                    }
                    seen_rows += rl;
                }
                if expanded.tail().is_some() {
                    assert_eq!(seen_rows, tr);
                }
                for seg in expanded.iter_row_chunks() {
                    seg.unwrap();
                }
                for seg in expanded.errors_slices() {
                    seg.unwrap();
                }
                for seg in expanded.lowered_text_slices() {
                    seg.unwrap();
                }
            }
        }
    }

    #[test]
    fn axis_visitors_poll_cancellation_in_column_and_tail_loops() {
        use crate::engine::lookup_index_cache::LookupAxis;
        use std::ops::ControlFlow;

        let token = CancelToken::new();
        token.cancel();

        let num_view = RangeView::from_owned_rows(
            vec![vec![LiteralValue::Number(1.0)]; 8],
            DateSystem::Excel1900,
        )
        .with_cancel_token(Some(token.clone()));
        assert_eq!(
            num_view
                .visit_axis_values(LookupAxis::ColumnInView(0), &mut |_, _| {
                    ControlFlow::Continue(())
                })
                .unwrap_err()
                .kind,
            ExcelErrorKind::Cancelled
        );
        assert_eq!(
            num_view
                .visit_axis_approx_numeric(LookupAxis::ColumnInView(0), &mut |_, _| {
                    ControlFlow::Continue(())
                })
                .unwrap_err()
                .kind,
            ExcelErrorKind::Cancelled
        );

        let text_view = RangeView::from_owned_rows(
            vec![vec![LiteralValue::Text("a".into())]; 8],
            DateSystem::Excel1900,
        )
        .with_cancel_token(Some(token.clone()));
        assert_eq!(
            text_view
                .visit_axis_approx_text(LookupAxis::ColumnInView(0), &mut |_, _| {
                    ControlFlow::Continue(())
                })
                .unwrap_err()
                .kind,
            ExcelErrorKind::Cancelled
        );

        let core = RangeView::from_owned_rows(
            vec![
                vec![LiteralValue::Number(1.0)],
                vec![LiteralValue::Number(2.0)],
            ],
            DateSystem::Excel1900,
        );
        let tailed = RangeView::from_core_and_tail(
            8,
            2,
            2,
            1,
            Arc::new(core.sheet().clone()),
            vec![LiteralValue::Number(3.0), LiteralValue::Text("x".into())],
            vec![LiteralValue::Empty],
            LiteralValue::Empty,
            None,
            1_000_000,
        )
        .with_cancel_token(Some(token));
        assert_eq!(
            tailed
                .visit_axis_approx_numeric(LookupAxis::ColumnInView(1), &mut |_, _| {
                    ControlFlow::Continue(())
                })
                .unwrap_err()
                .kind,
            ExcelErrorKind::Cancelled
        );
        assert_eq!(
            tailed
                .visit_axis_approx_text(LookupAxis::ColumnInView(1), &mut |_, _| {
                    ControlFlow::Continue(())
                })
                .unwrap_err()
                .kind,
            ExcelErrorKind::Cancelled
        );
    }
}
