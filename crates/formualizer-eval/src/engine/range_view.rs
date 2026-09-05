use crate::arrow_store;
use crate::arrow_store::IngestBuilder;
use crate::engine::CancelToken;
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
        pub selector_searches: usize,
        pub null_arrays: usize,
        pub null_slots: usize,
        pub provider_requests: [usize; 4],
        pub provider_builds: [usize; 4],
        pub provider_slots: [usize; 4],
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

struct RowSegment {
    chunk_idx: usize,
    chunk_offset: usize,
    row_start: usize,
    row_len: usize,
}

struct RowSegmentIterator<'a> {
    view: &'a RangeView<'a>,
    chunks: Option<core::ops::Range<usize>>,
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
        let row_end = self.view.er.min(sheet_rows.saturating_sub(1));
        let chunks = self.chunks.get_or_insert_with(|| {
            // Match row coverage independently of logical column/zero-sized subview dimensions.
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
                chunk_idx: ci,
                chunk_offset: lo - start,
                row_start: lo - self.view.sr,
                row_len,
            }));
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
        let ci = segment.chunk_idx;
        let rel_off = segment.chunk_offset;
        let seg_len = segment.row_len;
        let mut cols = Vec::with_capacity(view.cols);
        for col_idx in view.sc..=view.ec {
            #[cfg(test)]
            range_work::record(|w| w.generic_columns += 1);
            if col_idx >= sheet.columns.len() {
                #[cfg(test)]
                range_work::record(|w| {
                    w.null_arrays += 4;
                    w.null_slots += 4 * seg_len;
                });
                let numbers = Some(arrow_array::new_null_array(&DataType::Float64, seg_len));
                let booleans = Some(arrow_array::new_null_array(&DataType::Boolean, seg_len));
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
                    let numbers = Some(arrow_array::new_null_array(&DataType::Float64, seg_len));
                    let booleans = Some(arrow_array::new_null_array(&DataType::Boolean, seg_len));
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
                let type_tag: arrow_array::ArrayRef = Arc::new(ch.type_tag.slice(rel_off, seg_len));
                cols.push(ChunkCol {
                    numbers,
                    booleans,
                    text,
                    errors,
                    type_tag,
                });
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
        }
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

    #[inline]
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
        })
    }

    pub fn dims(&self) -> (usize, usize) {
        (self.rows, self.cols)
    }

    pub fn expand_to(&self, rows: usize, cols: usize) -> RangeView<'a> {
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
        }
    }

    pub fn sub_view(&self, rs: usize, cs: usize, rows: usize, cols: usize) -> RangeView<'a> {
        let abs_sr = self.sr + rs;
        let abs_sc = self.sc + cs;
        let er = abs_sr + rows.saturating_sub(1);
        let ec = abs_sc + cols.saturating_sub(1);
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

        for r in 0..self.rows {
            for c in 0..self.cols {
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
        }
    }

    /// Row-major cell traversal.
    pub fn for_each_cell(
        &self,
        f: &mut dyn FnMut(&LiteralValue) -> Result<(), ExcelError>,
    ) -> Result<(), ExcelError> {
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
            for col_idx in self.sc..=self.ec {
                let Some(ch) = sheet
                    .columns
                    .get(col_idx)
                    .and_then(|col| col.chunk(segment.chunk_idx))
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
                let base = ch
                    .numbers_or_null()
                    .slice(segment.chunk_offset, segment.row_len);
                let range = segment.chunk_offset..segment.chunk_offset + segment.row_len;
                let cascade = arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                out_cols.push(if cascade.has_any_in_range(range.clone()) {
                    cascade.select_numbers(range, &base)
                } else {
                    Arc::new(base)
                });
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
            for col_idx in self.sc..=self.ec {
                let Some(ch) = sheet
                    .columns
                    .get(col_idx)
                    .and_then(|col| col.chunk(segment.chunk_idx))
                else {
                    #[cfg(test)]
                    range_work::record(|w| {
                        w.null_arrays += 1;
                        w.null_slots += segment.row_len;
                    });
                    out_cols.push(Arc::new(arrow_array::UInt8Array::new_null(segment.row_len)));
                    continue;
                };
                let base = ch
                    .errors_or_null()
                    .slice(segment.chunk_offset, segment.row_len);
                let range = segment.chunk_offset..segment.chunk_offset + segment.row_len;
                let cascade = arrow_store::OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
                out_cols.push(if cascade.has_any_in_range(range.clone()) {
                    cascade.select_errors(range, &base)
                } else {
                    Arc::new(base)
                });
            }
            Ok((segment.row_start, segment.row_len, out_cols))
        })
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
        use crate::compute_prelude::concat_arrays;

        let mut out: Vec<arrow_array::ArrayRef> = Vec::with_capacity(self.cols);
        if self.rows == 0 || self.cols == 0 {
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
                let c = crate::compute_prelude::concat_arrays(&refs).expect("concat slice");
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
                let c = crate::compute_prelude::concat_arrays(&refs).expect("concat text");
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
            assert_eq!(numeric.selector_searches, 0);
            assert_eq!(numeric.provider_requests[1..], [0, 0, 0]);
            assert!(numeric.null_arrays <= 3);
            range_work::begin();
            let _: Vec<_> = view.errors_slices().collect();
            let errors = range_work::take();
            assert_eq!(errors.generic_columns, 0);
            assert_eq!(errors.selector_searches, 0);
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
}
