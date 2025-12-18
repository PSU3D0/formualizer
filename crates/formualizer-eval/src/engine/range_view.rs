use crate::arrow_store;
use crate::arrow_store::IngestBuilder;
use crate::stripes::NumericChunk;
use arrow_array::Array;
use arrow_schema::DataType;
use formualizer_common::{CoercionPolicy, DateSystem, ExcelError, LiteralValue};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
    cancel_token: Option<Arc<AtomicBool>>,
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

pub struct RowChunkIterator<'a> {
    view: &'a RangeView<'a>,
    current_chunk_idx: usize,
}

impl<'a> Iterator for RowChunkIterator<'a> {
    type Item = Result<ChunkSlice, ExcelError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(token) = &self.view.cancel_token
            && token.load(Ordering::Relaxed)
        {
            return Some(Err(ExcelError::new(
                formualizer_common::ExcelErrorKind::Cancelled,
            )));
        }

        let sheet = self.view.sheet();
        let chunk_starts = &sheet.chunk_starts;
        let sheet_rows = sheet.nrows as usize;
        let row_end = self.view.er.min(sheet_rows.saturating_sub(1));

        while self.current_chunk_idx < chunk_starts.len() {
            let ci = self.current_chunk_idx;
            let start = chunk_starts[ci];
            self.current_chunk_idx += 1;

            let end = if ci + 1 < chunk_starts.len() {
                chunk_starts[ci + 1]
            } else {
                sheet_rows
            };
            let len = end.saturating_sub(start);
            if len == 0 {
                continue;
            }
            let chunk_end_abs = start + len - 1;
            let is = start.max(self.view.sr);
            let ie = chunk_end_abs.min(row_end);
            if is > ie {
                continue;
            }
            let seg_len = ie - is + 1;
            let rel_off = is - start;

            let mut cols = Vec::with_capacity(self.view.cols);
            for col_idx in self.view.sc..=self.view.ec {
                if col_idx >= sheet.columns.len() {
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
            return Some(Ok(ChunkSlice {
                row_start: is - self.view.sr,
                row_len: seg_len,
                cols,
            }));
        }
        None
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

    #[must_use]
    pub fn with_cancel_token(mut self, token: Option<Arc<AtomicBool>>) -> Self {
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
        let nrows = rows.len();
        let ncols = rows.iter().map(|r| r.len()).max().unwrap_or(0);

        let chunk_rows = 32 * 1024;
        let mut ib = IngestBuilder::new("__tmp", ncols, chunk_rows, date_system);

        for mut r in rows {
            r.resize(ncols, LiteralValue::Empty);
            ib.append_row(&r).expect("append_row for RangeView");
        }

        let sheet = Arc::new(ib.finish());

        if nrows == 0 || ncols == 0 {
            return RangeView {
                backing: RangeBacking::Owned(sheet),
                sr: 1,
                sc: 1,
                er: 0,
                ec: 0,
                rows: 0,
                cols: 0,
                cancel_token: None,
            };
        }

        RangeView {
            backing: RangeBacking::Owned(sheet),
            sr: 0,
            sc: 0,
            er: nrows - 1,
            ec: ncols - 1,
            rows: nrows,
            cols: ncols,
            cancel_token: None,
        }
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
        if let Some(ov) = ch
            .overlay
            .get(in_off)
            .or_else(|| ch.computed_overlay.get(in_off))
        {
            return match ov {
                arrow_store::OverlayValue::Empty => LiteralValue::Empty,
                arrow_store::OverlayValue::Number(n) => LiteralValue::Number(*n),
                arrow_store::OverlayValue::Boolean(b) => LiteralValue::Boolean(*b),
                arrow_store::OverlayValue::Text(s) => LiteralValue::Text((**s).to_string()),
                arrow_store::OverlayValue::Error(code) => {
                    let kind = arrow_store::unmap_error_code(*code);
                    LiteralValue::Error(ExcelError::new(kind))
                }
                arrow_store::OverlayValue::Pending => LiteralValue::Pending,
            };
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
                        return LiteralValue::Empty;
                    }
                    LiteralValue::from_serial_number(arr.value(in_off))
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
            view: self,
            current_chunk_idx: 0,
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
        use crate::compute_prelude::zip_select;
        use arrow_array::builder::{BooleanBuilder, Float64Builder};

        self.iter_row_chunks().map(move |res| {
            let cs = res?;
            let mut out_cols: Vec<Arc<arrow_array::Float64Array>> =
                Vec::with_capacity(cs.cols.len());
            let sheet = self.sheet();
            let chunk_starts = &sheet.chunk_starts;

            for (local_c, col_idx) in (self.sc..=self.ec).enumerate() {
                let base = cs.cols[local_c]
                    .numbers
                    .as_ref()
                    .expect("numbers lane exists")
                    .clone();
                let base_fa = base
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .unwrap()
                    .clone();
                let base_arc = Arc::new(base_fa);

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
                let has_overlay = ch.overlay.any_in_range(seg_range.clone())
                    || (!ch.computed_overlay.is_empty()
                        && ch.computed_overlay.any_in_range(seg_range.clone()));
                if has_overlay {
                    let mut mask_b = BooleanBuilder::with_capacity(cs.row_len);
                    let mut ob = Float64Builder::with_capacity(cs.row_len);
                    for i in 0..cs.row_len {
                        if let Some(ov) = ch
                            .overlay
                            .get(rel_off + i)
                            .or_else(|| ch.computed_overlay.get(rel_off + i))
                        {
                            mask_b.append_value(true);
                            match ov {
                                arrow_store::OverlayValue::Number(n) => ob.append_value(*n),
                                _ => ob.append_null(),
                            }
                        } else {
                            mask_b.append_value(false);
                            ob.append_null();
                        }
                    }
                    let mask = mask_b.finish();
                    let overlay_vals = ob.finish();
                    let base_fa = base
                        .as_any()
                        .downcast_ref::<arrow_array::Float64Array>()
                        .unwrap();
                    let zipped = zip_select(&mask, &overlay_vals, base_fa).expect("zip overlay");
                    let fa = zipped
                        .as_any()
                        .downcast_ref::<arrow_array::Float64Array>()
                        .unwrap()
                        .clone();
                    out_cols.push(Arc::new(fa));
                } else {
                    out_cols.push(base_arc);
                }
            }
            Ok((cs.row_start, cs.row_len, out_cols))
        })
    }

    /// Typed boolean slices per row-segment, overlay-aware via zip.
    pub fn booleans_slices(
        &self,
    ) -> impl Iterator<Item = Result<(usize, usize, Vec<Arc<arrow_array::BooleanArray>>), ExcelError>> + '_
    {
        use crate::compute_prelude::zip_select;
        use arrow_array::builder::BooleanBuilder;

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
                let has_overlay = ch.overlay.any_in_range(seg_range.clone())
                    || (!ch.computed_overlay.is_empty()
                        && ch.computed_overlay.any_in_range(seg_range.clone()));
                if has_overlay {
                    let mut mask_b = BooleanBuilder::with_capacity(cs.row_len);
                    let mut bb = BooleanBuilder::with_capacity(cs.row_len);
                    for i in 0..cs.row_len {
                        if let Some(ov) = ch
                            .overlay
                            .get(rel_off + i)
                            .or_else(|| ch.computed_overlay.get(rel_off + i))
                        {
                            mask_b.append_value(true);
                            match ov {
                                arrow_store::OverlayValue::Boolean(b) => bb.append_value(*b),
                                _ => bb.append_null(),
                            }
                        } else {
                            mask_b.append_value(false);
                            bb.append_null();
                        }
                    }
                    let mask = mask_b.finish();
                    let overlay_vals = bb.finish();
                    let base_ba = base
                        .as_any()
                        .downcast_ref::<arrow_array::BooleanArray>()
                        .unwrap();
                    let zipped =
                        zip_select(&mask, &overlay_vals, base_ba).expect("zip boolean overlay");
                    let ba = zipped
                        .as_any()
                        .downcast_ref::<arrow_array::BooleanArray>()
                        .unwrap()
                        .clone();
                    out_cols.push(Arc::new(ba));
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
        use crate::compute_prelude::zip_select;
        use arrow_array::builder::{BooleanBuilder, StringBuilder};

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
                let has_overlay = ch.overlay.any_in_range(seg_range.clone())
                    || (!ch.computed_overlay.is_empty()
                        && ch.computed_overlay.any_in_range(seg_range.clone()));
                if has_overlay {
                    let mut mask_b = BooleanBuilder::with_capacity(cs.row_len);
                    let mut sb = StringBuilder::with_capacity(cs.row_len, cs.row_len * 8);
                    for i in 0..cs.row_len {
                        if let Some(ov) = ch
                            .overlay
                            .get(rel_off + i)
                            .or_else(|| ch.computed_overlay.get(rel_off + i))
                        {
                            mask_b.append_value(true);
                            match ov {
                                arrow_store::OverlayValue::Text(s) => sb.append_value(s),
                                _ => sb.append_null(),
                            }
                        } else {
                            mask_b.append_value(false);
                            sb.append_null();
                        }
                    }
                    let mask = mask_b.finish();
                    let overlay_vals = sb.finish();
                    let base_sa = base
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap();
                    let zipped =
                        zip_select(&mask, &overlay_vals, base_sa).expect("zip text overlay");
                    out_cols.push(zipped);
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
        use crate::compute_prelude::zip_select;
        use arrow_array::builder::{BooleanBuilder, StringBuilder};

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

                // Check overlay
                let has_overlay = ch.overlay.any_in_range(seg_range.clone())
                    || (!ch.computed_overlay.is_empty()
                        && ch.computed_overlay.any_in_range(seg_range.clone()));

                let base_lowered = ch.text_lower_or_null();
                let base_seg = base_lowered.slice(rel_off, cs.row_len);
                let base_sa = base_seg
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("lowered slice downcast");

                if has_overlay {
                    // Build lowered overlay values builder
                    let mut sb = StringBuilder::with_capacity(cs.row_len, cs.row_len * 8);
                    let mut mb = BooleanBuilder::with_capacity(cs.row_len);
                    for i in 0..cs.row_len {
                        if let Some(ov) = ch
                            .overlay
                            .get(rel_off + i)
                            .or_else(|| ch.computed_overlay.get(rel_off + i))
                        {
                            mb.append_value(true);
                            match ov {
                                arrow_store::OverlayValue::Text(s) => {
                                    sb.append_value(s.to_ascii_lowercase());
                                }
                                arrow_store::OverlayValue::Empty => {
                                    sb.append_null();
                                }
                                arrow_store::OverlayValue::Number(n) => {
                                    sb.append_value(n.to_string());
                                }
                                arrow_store::OverlayValue::Boolean(b) => {
                                    sb.append_value(if *b { "true" } else { "false" });
                                }
                                arrow_store::OverlayValue::Error(_)
                                | arrow_store::OverlayValue::Pending => {
                                    sb.append_null();
                                }
                            }
                        } else {
                            sb.append_null();
                            mb.append_value(false);
                        }
                    }
                    let overlay_vals = sb.finish();
                    let mask = mb.finish();
                    let zipped = zip_select(&mask, &overlay_vals, base_sa)
                        .expect("zip lowered text overlay");
                    let za = zipped
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .unwrap()
                        .clone();
                    out_cols.push(Arc::new(za));
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
        use crate::compute_prelude::zip_select;
        use arrow_array::builder::{BooleanBuilder, UInt8Builder};

        self.iter_row_chunks().map(move |res| {
            let cs = res?;
            let mut out_cols: Vec<Arc<arrow_array::UInt8Array>> = Vec::with_capacity(cs.cols.len());
            let sheet = self.sheet();
            let chunk_starts = &sheet.chunk_starts;

            for (local_c, col_idx) in (self.sc..=self.ec).enumerate() {
                let base = cs.cols[local_c]
                    .errors
                    .as_ref()
                    .expect("errors lane exists")
                    .clone();
                let base_e = base
                    .as_any()
                    .downcast_ref::<arrow_array::UInt8Array>()
                    .unwrap()
                    .clone();
                let base_arc: Arc<arrow_array::UInt8Array> = Arc::new(base_e);
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
                let has_overlay = ch.overlay.any_in_range(seg_range.clone())
                    || (!ch.computed_overlay.is_empty()
                        && ch.computed_overlay.any_in_range(seg_range.clone()));
                if has_overlay {
                    let mut mask_b = BooleanBuilder::with_capacity(cs.row_len);
                    let mut eb = UInt8Builder::with_capacity(cs.row_len);
                    for i in 0..cs.row_len {
                        if let Some(ov) = ch
                            .overlay
                            .get(rel_off + i)
                            .or_else(|| ch.computed_overlay.get(rel_off + i))
                        {
                            mask_b.append_value(true);
                            match ov {
                                arrow_store::OverlayValue::Error(code) => eb.append_value(*code),
                                _ => eb.append_null(),
                            }
                        } else {
                            mask_b.append_value(false);
                            eb.append_null();
                        }
                    }
                    let mask = mask_b.finish();
                    let overlay_vals = eb.finish();
                    let base_ea = base
                        .as_any()
                        .downcast_ref::<arrow_array::UInt8Array>()
                        .unwrap();
                    let zipped =
                        zip_select(&mask, &overlay_vals, base_ea).expect("zip err overlay");
                    let ea = zipped
                        .as_any()
                        .downcast_ref::<arrow_array::UInt8Array>()
                        .unwrap()
                        .clone();
                    out_cols.push(Arc::new(ea));
                } else {
                    out_cols.push(base_arc);
                }
            }
            Ok((cs.row_start, cs.row_len, out_cols))
        })
    }

    /// Typed type-tag slices per row-segment.
    pub fn type_tags_slices(
        &self,
    ) -> impl Iterator<Item = Result<(usize, usize, Vec<Arc<arrow_array::UInt8Array>>), ExcelError>> + '_
    {
        use crate::compute_prelude::zip_select;
        use arrow_array::builder::{BooleanBuilder, UInt8Builder};

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
                let has_overlay = ch.overlay.any_in_range(seg_range.clone())
                    || (!ch.computed_overlay.is_empty()
                        && ch.computed_overlay.any_in_range(seg_range.clone()));
                if has_overlay {
                    let mut mask_b = BooleanBuilder::with_capacity(cs.row_len);
                    let mut tb = UInt8Builder::with_capacity(cs.row_len);
                    for i in 0..cs.row_len {
                        if let Some(ov) = ch
                            .overlay
                            .get(rel_off + i)
                            .or_else(|| ch.computed_overlay.get(rel_off + i))
                        {
                            mask_b.append_value(true);
                            let tag = match ov {
                                arrow_store::OverlayValue::Empty => arrow_store::TypeTag::Empty,
                                arrow_store::OverlayValue::Number(_) => {
                                    arrow_store::TypeTag::Number
                                }
                                arrow_store::OverlayValue::Boolean(_) => {
                                    arrow_store::TypeTag::Boolean
                                }
                                arrow_store::OverlayValue::Text(_) => arrow_store::TypeTag::Text,
                                arrow_store::OverlayValue::Error(_) => arrow_store::TypeTag::Error,
                                arrow_store::OverlayValue::Pending => arrow_store::TypeTag::Pending,
                            };
                            tb.append_value(tag as u8);
                        } else {
                            mask_b.append_value(false);
                            tb.append_null();
                        }
                    }
                    let mask = mask_b.finish();
                    let overlay_vals = tb.finish();
                    let base_ta = base
                        .as_any()
                        .downcast_ref::<arrow_array::UInt8Array>()
                        .unwrap();
                    let zipped =
                        zip_select(&mask, &overlay_vals, base_ta).expect("zip tag overlay");
                    let ta = zipped
                        .as_any()
                        .downcast_ref::<arrow_array::UInt8Array>()
                        .unwrap()
                        .clone();
                    out_cols.push(Arc::new(ta));
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
        use crate::compute_prelude::{concat_arrays, zip_select};
        use arrow_array::builder::{BooleanBuilder, StringBuilder};

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
                        let has_overlay = ch.overlay.any_in_range(rel_off..(rel_off + seg_len))
                            || (!ch.computed_overlay.is_empty()
                                && ch
                                    .computed_overlay
                                    .any_in_range(rel_off..(rel_off + seg_len)));
                        if has_overlay {
                            // Build lowered overlay values builder
                            let mut sb = StringBuilder::with_capacity(seg_len, seg_len * 8);
                            // mask overlaid rows
                            let mut mb = BooleanBuilder::with_capacity(seg_len);
                            for i in 0..seg_len {
                                if let Some(ov) = ch
                                    .overlay
                                    .get(rel_off + i)
                                    .or_else(|| ch.computed_overlay.get(rel_off + i))
                                {
                                    mb.append_value(true);
                                    match ov {
                                        arrow_store::OverlayValue::Text(s) => {
                                            sb.append_value(s.to_ascii_lowercase());
                                        }
                                        arrow_store::OverlayValue::Empty => {
                                            sb.append_null();
                                        }
                                        arrow_store::OverlayValue::Number(n) => {
                                            sb.append_value(n.to_string());
                                        }
                                        arrow_store::OverlayValue::Boolean(b) => {
                                            sb.append_value(if *b { "true" } else { "false" });
                                        }
                                        arrow_store::OverlayValue::Error(_)
                                        | arrow_store::OverlayValue::Pending => {
                                            sb.append_null();
                                        }
                                    }
                                } else {
                                    // not overlaid
                                    sb.append_null();
                                    mb.append_value(false);
                                }
                            }
                            let overlay_vals = sb.finish();
                            let mask = mb.finish();
                            // base lowered segment
                            let base_lowered = ch.text_lower_or_null();
                            let base_seg = base_lowered.slice(rel_off, seg_len);
                            let base_sa = base_seg
                                .as_any()
                                .downcast_ref::<arrow_array::StringArray>()
                                .expect("lowered slice downcast");
                            let zipped = zip_select(&mask, &overlay_vals, base_sa)
                                .expect("zip lowered text overlay");
                            segs.push(zipped);
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
                        let has_overlay = ch.overlay.any_in_range(seg_range.clone())
                            || (!ch.computed_overlay.is_empty()
                                && ch.computed_overlay.any_in_range(seg_range.clone()));

                        let final_arr = if has_overlay {
                            let mut nb =
                                arrow_array::builder::Float64Builder::with_capacity(seg_len);
                            let mut mask_b =
                                arrow_array::builder::BooleanBuilder::with_capacity(seg_len);
                            for i in 0..seg_len {
                                if let Some(ov) = ch
                                    .overlay
                                    .get(rel_off_in_chunk + i)
                                    .or_else(|| ch.computed_overlay.get(rel_off_in_chunk + i))
                                {
                                    mask_b.append_value(true);
                                    match ov {
                                        arrow_store::OverlayValue::Number(n) => nb.append_value(*n),
                                        _ => nb.append_null(),
                                    }
                                } else {
                                    mask_b.append_value(false);
                                    nb.append_null();
                                }
                            }
                            let mask = mask_b.finish();
                            let overlay_vals = nb.finish();
                            let base_slice = base_nums.slice(rel_off_in_chunk, seg_len);
                            let base_fa = base_slice
                                .as_any()
                                .downcast_ref::<arrow_array::Float64Array>()
                                .unwrap();
                            let zipped =
                                crate::compute_prelude::zip_select(&mask, &overlay_vals, base_fa)
                                    .expect("zip slice");
                            zipped
                                .as_any()
                                .downcast_ref::<arrow_array::Float64Array>()
                                .unwrap()
                                .clone()
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
                        let has_overlay = ch.overlay.any_in_range(seg_range.clone())
                            || (!ch.computed_overlay.is_empty()
                                && ch.computed_overlay.any_in_range(seg_range.clone()));

                        let final_arr = if has_overlay {
                            let mut sb = arrow_array::builder::StringBuilder::with_capacity(
                                seg_len,
                                seg_len * 8,
                            );
                            let mut mask_b =
                                arrow_array::builder::BooleanBuilder::with_capacity(seg_len);
                            for i in 0..seg_len {
                                if let Some(ov) = ch
                                    .overlay
                                    .get(rel_off_in_chunk + i)
                                    .or_else(|| ch.computed_overlay.get(rel_off_in_chunk + i))
                                {
                                    mask_b.append_value(true);
                                    match ov {
                                        arrow_store::OverlayValue::Text(s) => {
                                            sb.append_value(s.to_ascii_lowercase())
                                        }
                                        arrow_store::OverlayValue::Number(n) => {
                                            sb.append_value(n.to_string())
                                        }
                                        arrow_store::OverlayValue::Boolean(b) => {
                                            sb.append_value(if *b { "true" } else { "false" })
                                        }
                                        _ => sb.append_null(),
                                    }
                                } else {
                                    mask_b.append_value(false);
                                    sb.append_null();
                                }
                            }
                            let mask = mask_b.finish();
                            let overlay_vals = sb.finish();
                            let base_slice = base_lowered.slice(rel_off_in_chunk, seg_len);
                            let base_sa = base_slice
                                .as_any()
                                .downcast_ref::<arrow_array::StringArray>()
                                .unwrap();
                            let zipped =
                                crate::compute_prelude::zip_select(&mask, &overlay_vals, base_sa)
                                    .expect("zip text");
                            zipped
                                .as_any()
                                .downcast_ref::<arrow_array::StringArray>()
                                .unwrap()
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
}
