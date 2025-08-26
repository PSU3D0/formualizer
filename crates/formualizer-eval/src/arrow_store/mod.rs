use arrow_array::Array;
use arrow_array::new_null_array;
use arrow_schema::DataType;
use chrono::Timelike;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, StringBuilder, UInt8Builder, UInt32Builder,
};
use arrow_array::{ArrayRef, BooleanArray, Float64Array, StringArray, UInt8Array, UInt32Array};
use once_cell::sync::OnceCell;

use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};

/// Compact type tag per row (UInt8 backing)
#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TypeTag {
    Empty = 0,
    Number = 1,
    Boolean = 2,
    Text = 3,
    Error = 4,
    DateTime = 5, // reserved for future temporal lanes
    Duration = 6, // reserved
    Pending = 7,
}

impl TypeTag {
    fn from_value(v: &LiteralValue) -> Self {
        match v {
            LiteralValue::Empty => TypeTag::Empty,
            LiteralValue::Int(_) | LiteralValue::Number(_) => TypeTag::Number,
            LiteralValue::Boolean(_) => TypeTag::Boolean,
            LiteralValue::Text(_) => TypeTag::Text,
            LiteralValue::Error(_) => TypeTag::Error,
            LiteralValue::Date(_) | LiteralValue::DateTime(_) | LiteralValue::Time(_) => {
                TypeTag::DateTime
            }
            LiteralValue::Duration(_) => TypeTag::Duration,
            LiteralValue::Pending => TypeTag::Pending,
            LiteralValue::Array(_) => TypeTag::Error, // arrays not storable in a single cell lane
        }
    }
}

impl TypeTag {
    #[inline]
    fn from_u8(b: u8) -> Self {
        match b {
            x if x == TypeTag::Empty as u8 => TypeTag::Empty,
            x if x == TypeTag::Number as u8 => TypeTag::Number,
            x if x == TypeTag::Boolean as u8 => TypeTag::Boolean,
            x if x == TypeTag::Text as u8 => TypeTag::Text,
            x if x == TypeTag::Error as u8 => TypeTag::Error,
            x if x == TypeTag::DateTime as u8 => TypeTag::DateTime,
            x if x == TypeTag::Duration as u8 => TypeTag::Duration,
            x if x == TypeTag::Pending as u8 => TypeTag::Pending,
            _ => TypeTag::Empty,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ColumnChunkMeta {
    pub len: usize,
    pub non_null_num: usize,
    pub non_null_bool: usize,
    pub non_null_text: usize,
    pub non_null_err: usize,
}

#[derive(Debug, Clone)]
pub struct ColumnChunk {
    pub numbers: Option<Arc<Float64Array>>,
    pub booleans: Option<Arc<BooleanArray>>,
    pub text: Option<ArrayRef>,          // Utf8 for Phase A
    pub errors: Option<Arc<UInt8Array>>, // compact error code (UInt8)
    pub type_tag: Arc<UInt8Array>,
    pub formula_id: Option<Arc<UInt32Array>>, // reserved for Phase A+
    pub meta: ColumnChunkMeta,
    // Lazy null providers (per-chunk)
    lazy_null_numbers: OnceCell<Arc<Float64Array>>,
    lazy_null_booleans: OnceCell<Arc<BooleanArray>>,
    lazy_null_text: OnceCell<ArrayRef>,
    lazy_null_errors: OnceCell<Arc<UInt8Array>>,
}

impl ColumnChunk {
    #[inline]
    pub fn len(&self) -> usize {
        self.type_tag.len()
    }
    #[inline]
    pub fn numbers_or_null(&self) -> Arc<Float64Array> {
        if let Some(a) = &self.numbers {
            return a.clone();
        }
        self.lazy_null_numbers
            .get_or_init(|| {
                let arr = new_null_array(&DataType::Float64, self.len());
                Arc::new(arr.as_any().downcast_ref::<Float64Array>().unwrap().clone())
            })
            .clone()
    }
    #[inline]
    pub fn booleans_or_null(&self) -> Arc<BooleanArray> {
        if let Some(a) = &self.booleans {
            return a.clone();
        }
        self.lazy_null_booleans
            .get_or_init(|| {
                let arr = new_null_array(&DataType::Boolean, self.len());
                Arc::new(arr.as_any().downcast_ref::<BooleanArray>().unwrap().clone())
            })
            .clone()
    }
    #[inline]
    pub fn errors_or_null(&self) -> Arc<UInt8Array> {
        if let Some(a) = &self.errors {
            return a.clone();
        }
        self.lazy_null_errors
            .get_or_init(|| {
                let arr = new_null_array(&DataType::UInt8, self.len());
                Arc::new(arr.as_any().downcast_ref::<UInt8Array>().unwrap().clone())
            })
            .clone()
    }
    #[inline]
    pub fn text_or_null(&self) -> ArrayRef {
        if let Some(a) = &self.text {
            return a.clone();
        }
        self.lazy_null_text
            .get_or_init(|| new_null_array(&DataType::Utf8, self.len()))
            .clone()
    }
}

#[derive(Debug, Clone)]
pub struct ArrowColumn {
    pub chunks: Vec<ColumnChunk>,
    pub index: u32,
}

#[derive(Debug, Clone)]
pub struct ArrowSheet {
    pub name: Arc<str>,
    pub columns: Vec<ArrowColumn>,
    pub nrows: u32,
    pub chunk_starts: Vec<usize>,
}

#[derive(Debug, Default, Clone)]
pub struct SheetStore {
    pub sheets: Vec<ArrowSheet>,
}

impl SheetStore {
    pub fn sheet(&self, name: &str) -> Option<&ArrowSheet> {
        self.sheets.iter().find(|s| s.name.as_ref() == name)
    }
}

/// Ingestion builder that writes per-column Arrow arrays with a lane/tag design.
pub struct IngestBuilder {
    name: Arc<str>,
    ncols: usize,
    chunk_rows: usize,

    // Per-column active builders for current chunk
    num_builders: Vec<Float64Builder>,
    bool_builders: Vec<BooleanBuilder>,
    text_builders: Vec<StringBuilder>,
    err_builders: Vec<UInt8Builder>,
    tag_builders: Vec<UInt8Builder>,

    // Per-column per-lane non-null counters for current chunk
    lane_counts: Vec<LaneCounts>,

    // Accumulated chunks
    chunks: Vec<Vec<ColumnChunk>>, // indexed by col
    row_in_chunk: usize,
    total_rows: u32,
}

#[derive(Debug, Clone, Copy, Default)]
struct LaneCounts {
    n_num: usize,
    n_bool: usize,
    n_text: usize,
    n_err: usize,
}

impl IngestBuilder {
    pub fn new(sheet_name: &str, ncols: usize, chunk_rows: usize) -> Self {
        let mut chunks = Vec::with_capacity(ncols);
        chunks.resize_with(ncols, Vec::new);
        Self {
            name: Arc::from(sheet_name.to_string()),
            ncols,
            chunk_rows: chunk_rows.max(1),
            num_builders: (0..ncols)
                .map(|_| Float64Builder::with_capacity(chunk_rows))
                .collect(),
            bool_builders: (0..ncols)
                .map(|_| BooleanBuilder::with_capacity(chunk_rows))
                .collect(),
            text_builders: (0..ncols)
                .map(|_| StringBuilder::with_capacity(chunk_rows, chunk_rows * 12))
                .collect(),
            err_builders: (0..ncols)
                .map(|_| UInt8Builder::with_capacity(chunk_rows))
                .collect(),
            tag_builders: (0..ncols)
                .map(|_| UInt8Builder::with_capacity(chunk_rows))
                .collect(),
            lane_counts: vec![LaneCounts::default(); ncols],
            chunks,
            row_in_chunk: 0,
            total_rows: 0,
        }
    }

    /// Append a single row of values. Length must match `ncols`.
    pub fn append_row(&mut self, row: &[LiteralValue]) -> Result<(), ExcelError> {
        assert_eq!(row.len(), self.ncols, "row width mismatch");

        for (c, v) in row.iter().enumerate() {
            let tag = TypeTag::from_value(v) as u8;
            self.tag_builders[c].append_value(tag);

            match v {
                LiteralValue::Empty => {
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                LiteralValue::Int(i) => {
                    self.num_builders[c].append_value(*i as f64);
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                LiteralValue::Number(n) => {
                    self.num_builders[c].append_value(*n);
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                LiteralValue::Boolean(b) => {
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_value(*b);
                    self.lane_counts[c].n_bool += 1;
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                LiteralValue::Text(s) => {
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_value(s);
                    self.lane_counts[c].n_text += 1;
                    self.err_builders[c].append_null();
                }
                LiteralValue::Error(e) => {
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_value(map_error_code(e.kind));
                    self.lane_counts[c].n_err += 1;
                }
                // Phase A: coerce temporal to serials in numeric lane with DateTime tag
                LiteralValue::Date(d) => {
                    let dt = d.and_hms_opt(0, 0, 0).unwrap();
                    self.num_builders[c].append_value(formualizer_common::datetime_to_serial(&dt));
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                LiteralValue::DateTime(dt) => {
                    self.num_builders[c].append_value(formualizer_common::datetime_to_serial(dt));
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                LiteralValue::Time(t) => {
                    let serial = t.num_seconds_from_midnight() as f64 / 86_400.0;
                    self.num_builders[c].append_value(serial);
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                LiteralValue::Duration(dur) => {
                    let serial = dur.num_seconds() as f64 / 86_400.0;
                    self.num_builders[c].append_value(serial);
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                LiteralValue::Array(_) => {
                    // Not allowed as a stored scalar; mark as error kind VALUE
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_value(map_error_code(ExcelErrorKind::Value));
                    self.lane_counts[c].n_err += 1;
                }
                LiteralValue::Pending => {
                    // Pending: tag only; all lanes remain null (no error)
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
            }
        }

        self.row_in_chunk += 1;
        self.total_rows += 1;

        if self.row_in_chunk >= self.chunk_rows {
            self.finish_chunk();
        }

        Ok(())
    }

    fn finish_chunk(&mut self) {
        if self.row_in_chunk == 0 {
            return;
        }
        for c in 0..self.ncols {
            let len = self.row_in_chunk;
            let numbers_arc: Option<Arc<Float64Array>> = if self.lane_counts[c].n_num == 0 {
                None
            } else {
                Some(Arc::new(self.num_builders[c].finish()))
            };
            let booleans_arc: Option<Arc<BooleanArray>> = if self.lane_counts[c].n_bool == 0 {
                None
            } else {
                Some(Arc::new(self.bool_builders[c].finish()))
            };
            let text_ref: Option<ArrayRef> = if self.lane_counts[c].n_text == 0 {
                None
            } else {
                Some(Arc::new(self.text_builders[c].finish()))
            };
            let errors_arc: Option<Arc<UInt8Array>> = if self.lane_counts[c].n_err == 0 {
                None
            } else {
                Some(Arc::new(self.err_builders[c].finish()))
            };
            let tags: UInt8Array = self.tag_builders[c].finish();

            let chunk = ColumnChunk {
                numbers: numbers_arc,
                booleans: booleans_arc,
                text: text_ref,
                errors: errors_arc,
                type_tag: Arc::new(tags),
                formula_id: None,
                meta: ColumnChunkMeta {
                    len,
                    non_null_num: self.lane_counts[c].n_num,
                    non_null_bool: self.lane_counts[c].n_bool,
                    non_null_text: self.lane_counts[c].n_text,
                    non_null_err: self.lane_counts[c].n_err,
                },
                lazy_null_numbers: OnceCell::new(),
                lazy_null_booleans: OnceCell::new(),
                lazy_null_text: OnceCell::new(),
                lazy_null_errors: OnceCell::new(),
            };
            self.chunks[c].push(chunk);

            // re-init builders for next chunk
            self.num_builders[c] = Float64Builder::with_capacity(self.chunk_rows);
            self.bool_builders[c] = BooleanBuilder::with_capacity(self.chunk_rows);
            self.text_builders[c] =
                StringBuilder::with_capacity(self.chunk_rows, self.chunk_rows * 12);
            self.err_builders[c] = UInt8Builder::with_capacity(self.chunk_rows);
            self.tag_builders[c] = UInt8Builder::with_capacity(self.chunk_rows);
            self.lane_counts[c] = LaneCounts::default();
        }
        self.row_in_chunk = 0;
    }

    pub fn finish(mut self) -> ArrowSheet {
        // flush partial chunk
        if self.row_in_chunk > 0 {
            self.finish_chunk();
        }

        let mut columns = Vec::with_capacity(self.ncols);
        for (idx, chunks) in self.chunks.into_iter().enumerate() {
            columns.push(ArrowColumn {
                chunks,
                index: idx as u32,
            });
        }
        // Precompute chunk starts from first column and enforce alignment across columns
        let mut chunk_starts: Vec<usize> = Vec::new();
        if let Some(col0) = columns.get(0) {
            let chunks_len0 = col0.chunks.len();
            for (ci, col) in columns.iter().enumerate() {
                if col.chunks.len() != chunks_len0 {
                    panic!(
                        "ArrowSheet chunk misalignment: column {} chunks={} != {}",
                        ci,
                        col.chunks.len(),
                        chunks_len0
                    );
                }
            }
            let mut cur = 0usize;
            for i in 0..chunks_len0 {
                let len_i = col0.chunks[i].type_tag.len();
                for (ci, col) in columns.iter().enumerate() {
                    let got = col.chunks[i].type_tag.len();
                    if got != len_i {
                        panic!(
                            "ArrowSheet chunk row-length misalignment at chunk {}: col {} len={} != {}",
                            i, ci, got, len_i
                        );
                    }
                }
                chunk_starts.push(cur);
                cur += len_i;
            }
        }
        ArrowSheet {
            name: self.name,
            columns,
            nrows: self.total_rows,
            chunk_starts,
        }
    }
}

fn map_error_code(kind: ExcelErrorKind) -> u8 {
    match kind {
        ExcelErrorKind::Null => 1,
        ExcelErrorKind::Ref => 2,
        ExcelErrorKind::Name => 3,
        ExcelErrorKind::Value => 4,
        ExcelErrorKind::Div => 5,
        ExcelErrorKind::Na => 6,
        ExcelErrorKind::Num => 7,
        ExcelErrorKind::Error => 8,
        ExcelErrorKind::NImpl => 9,
        ExcelErrorKind::Spill => 10,
        ExcelErrorKind::Calc => 11,
        ExcelErrorKind::Circ => 12,
        ExcelErrorKind::Cancelled => 13,
    }
}

fn unmap_error_code(code: u8) -> ExcelErrorKind {
    match code {
        1 => ExcelErrorKind::Null,
        2 => ExcelErrorKind::Ref,
        3 => ExcelErrorKind::Name,
        4 => ExcelErrorKind::Value,
        5 => ExcelErrorKind::Div,
        6 => ExcelErrorKind::Na,
        7 => ExcelErrorKind::Num,
        8 => ExcelErrorKind::Error,
        9 => ExcelErrorKind::NImpl,
        10 => ExcelErrorKind::Spill,
        11 => ExcelErrorKind::Calc,
        12 => ExcelErrorKind::Circ,
        13 => ExcelErrorKind::Cancelled,
        _ => ExcelErrorKind::Error,
    }
}

/// A lightweight view over a rectangular range in an `ArrowSheet`.
/// Coordinates are 0-based and inclusive.
pub struct ArrowRangeView<'a> {
    sheet: &'a ArrowSheet,
    sr: usize,
    sc: usize,
    er: usize,
    ec: usize,
    rows: usize,
    cols: usize,
    chunk_starts: &'a [usize],
}

impl ArrowSheet {
    /// Return a summary of each column's chunk counts, total rows, and lane presence.
    pub fn shape(&self) -> Vec<ColumnShape> {
        self.columns
            .iter()
            .map(|c| {
                let chunks = c.chunks.len();
                let rows = self.nrows as usize;
                let has_num = c.chunks.iter().any(|ch| ch.meta.non_null_num > 0);
                let has_bool = c.chunks.iter().any(|ch| ch.meta.non_null_bool > 0);
                let has_text = c.chunks.iter().any(|ch| ch.meta.non_null_text > 0);
                let has_err = c.chunks.iter().any(|ch| ch.meta.non_null_err > 0);
                ColumnShape {
                    index: c.index,
                    chunks,
                    rows,
                    has_num,
                    has_bool,
                    has_text,
                    has_err,
                }
            })
            .collect()
    }
    pub fn range_view(&self, sr: usize, sc: usize, er: usize, ec: usize) -> ArrowRangeView<'_> {
        let r0 = er.checked_sub(sr).map(|d| d + 1).unwrap_or(0);
        let c0 = ec.checked_sub(sc).map(|d| d + 1).unwrap_or(0);
        let (rows, cols) = if r0 == 0 || c0 == 0 { (0, 0) } else { (r0, c0) };
        ArrowRangeView {
            sheet: self,
            sr,
            sc,
            er,
            ec,
            rows,
            cols,
            chunk_starts: &self.chunk_starts,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnShape {
    pub index: u32,
    pub chunks: usize,
    pub rows: usize,
    pub has_num: bool,
    pub has_bool: bool,
    pub has_text: bool,
    pub has_err: bool,
}

impl<'a> ArrowRangeView<'a> {
    #[inline]
    pub fn dims(&self) -> (usize, usize) {
        (self.rows, self.cols)
    }

    /// Returns a single cell value relative to this view (row/col 0-based).
    /// OOB returns Empty. Phase A: Date/Time/Duration come back as Number
    /// with the corresponding TypeTag preserved for higher layers.
    pub fn get_cell(&self, row: usize, col: usize) -> LiteralValue {
        if row >= self.rows || col >= self.cols {
            return LiteralValue::Empty;
        }
        let abs_row = self.sr + row;
        let abs_col = self.sc + col;
        let sheet_rows = self.sheet.nrows as usize;
        if abs_row >= sheet_rows {
            return LiteralValue::Empty;
        }
        if abs_col >= self.sheet.columns.len() {
            return LiteralValue::Empty;
        }
        let col_ref = &self.sheet.columns[abs_col];
        // Locate chunk by binary searching start offsets
        let ch_idx = match self.chunk_starts.binary_search(&abs_row) {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        };
        if ch_idx >= col_ref.chunks.len() {
            return LiteralValue::Empty;
        }
        let ch = &col_ref.chunks[ch_idx];
        let row_start = self.chunk_starts[ch_idx];
        let in_off = abs_row - row_start;
        // Read tag and route to lane
        let tag_u8 = ch.type_tag.value(in_off);
        match TypeTag::from_u8(tag_u8) {
            TypeTag::Empty => LiteralValue::Empty,
            TypeTag::Number | TypeTag::DateTime | TypeTag::Duration => {
                let arr = ch.numbers.as_ref().unwrap();
                if arr.is_null(in_off) {
                    return LiteralValue::Empty;
                }
                let nums = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                LiteralValue::Number(nums.value(in_off))
            }
            TypeTag::Boolean => {
                let arr = ch.booleans.as_ref().unwrap();
                if arr.is_null(in_off) {
                    return LiteralValue::Empty;
                }
                let ba = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                LiteralValue::Boolean(ba.value(in_off))
            }
            TypeTag::Text => {
                let arr = ch.text.as_ref().unwrap();
                if arr.is_null(in_off) {
                    return LiteralValue::Empty;
                }
                let sa = arr.as_any().downcast_ref::<StringArray>().unwrap();
                LiteralValue::Text(sa.value(in_off).to_string())
            }
            TypeTag::Error => {
                let arr = ch.errors.as_ref().unwrap();
                if arr.is_null(in_off) {
                    return LiteralValue::Empty;
                }
                let ea = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
                let kind = unmap_error_code(ea.value(in_off));
                LiteralValue::Error(ExcelError::new(kind))
            }
            TypeTag::Pending => LiteralValue::Pending,
        }
    }

    /// Row-aligned chunk slices within this view. Each item represents
    /// a contiguous row segment that lies fully within a single row chunk.
    pub fn row_chunk_slices(&self) -> Vec<ChunkSlice> {
        let mut out = Vec::new();
        if self.rows == 0 || self.cols == 0 {
            return out;
        }
        // Iterate overlapping chunks by row using first column's chunk map
        let sheet_rows = self.sheet.nrows as usize;
        let row_end = self.er.min(sheet_rows.saturating_sub(1));
        if self.chunk_starts.is_empty() {
            return out;
        }
        // For each chunk, compute intersection with [sr..=row_end]
        for (ci, &start) in self.chunk_starts.iter().enumerate() {
            let len = if ci + 1 < self.chunk_starts.len() {
                self.chunk_starts[ci + 1] - start
            } else {
                // last chunk length from first column
                if let Some(col0) = self.sheet.columns.get(0) {
                    col0.chunks[ci].type_tag.len()
                } else {
                    0
                }
            };
            let end = start + len - 1;
            let is = start.max(self.sr);
            let ie = end.min(row_end);
            if is > ie {
                continue;
            }
            let seg_len = ie - is + 1;
            let rel_off = is - start; // offset into chunk arrays
            // Collect per-column lane slices for columns in [sc..=ec]
            let mut cols = Vec::with_capacity(self.cols);
            for col_idx in self.sc..=self.ec {
                if col_idx >= self.sheet.columns.len() {
                    // Pad out-of-bounds columns with empty (null) lanes and Empty type_tag
                    use arrow_array::Array;
                    let numbers = Some(new_null_array(&DataType::Float64, seg_len));
                    let booleans = Some(new_null_array(&DataType::Boolean, seg_len));
                    let text = Some(new_null_array(&DataType::Utf8, seg_len));
                    let errors = Some(new_null_array(&DataType::UInt8, seg_len));
                    let type_tag: ArrayRef =
                        Arc::new(UInt8Array::from(vec![TypeTag::Empty as u8; seg_len]));
                    cols.push(ChunkCol {
                        numbers,
                        booleans,
                        text,
                        errors,
                        type_tag,
                    });
                } else {
                    let col = &self.sheet.columns[col_idx];
                    let ch = if ci < col.chunks.len() {
                        &col.chunks[ci]
                    } else {
                        // Should not happen with enforced alignment; pad as OOB if it does
                        let numbers = Some(new_null_array(&DataType::Float64, seg_len));
                        let booleans = Some(new_null_array(&DataType::Boolean, seg_len));
                        let text = Some(new_null_array(&DataType::Utf8, seg_len));
                        let errors = Some(new_null_array(&DataType::UInt8, seg_len));
                        let type_tag: ArrayRef =
                            Arc::new(UInt8Array::from(vec![TypeTag::Empty as u8; seg_len]));
                        cols.push(ChunkCol {
                            numbers,
                            booleans,
                            text,
                            errors,
                            type_tag,
                        });
                        continue;
                    };
                    use arrow_array::Array;
                    // Always provide a slice, lazily using per-chunk null arrays when the lane is absent
                    let numbers_base: ArrayRef = ch.numbers_or_null();
                    let booleans_base: ArrayRef = ch.booleans_or_null();
                    let text_base: ArrayRef = ch.text_or_null();
                    let errors_base: ArrayRef = ch.errors_or_null();
                    let numbers = Some(Array::slice(numbers_base.as_ref(), rel_off, seg_len));
                    let booleans = Some(Array::slice(booleans_base.as_ref(), rel_off, seg_len));
                    let text = Some(Array::slice(text_base.as_ref(), rel_off, seg_len));
                    let errors = Some(Array::slice(errors_base.as_ref(), rel_off, seg_len));
                    let type_tag: ArrayRef = Array::slice(ch.type_tag.as_ref(), rel_off, seg_len);
                    cols.push(ChunkCol {
                        numbers,
                        booleans,
                        text,
                        errors,
                        type_tag,
                    });
                }
            }
            out.push(ChunkSlice {
                row_start: is - self.sr,
                row_len: seg_len,
                cols,
            });
        }
        out
    }

    /// Convenience iterator over row-aligned chunk slices.
    pub fn iter_row_chunks(&'a self) -> impl Iterator<Item = ChunkSlice> + 'a {
        self.row_chunk_slices().into_iter()
    }

    /// Typed numeric slices per row-segment: (row_start, row_len, per-column Float64 arrays)
    pub fn numbers_slices(
        &'a self,
    ) -> impl Iterator<Item = (usize, usize, Vec<Arc<Float64Array>>)> + 'a {
        self.iter_row_chunks().map(|cs| {
            let cols = cs
                .cols
                .iter()
                .map(|cc| {
                    let a = cc.numbers.as_ref().expect("numbers lane exists");
                    let fa = a.as_any().downcast_ref::<Float64Array>().unwrap().clone();
                    Arc::new(fa)
                })
                .collect();
            (cs.row_start, cs.row_len, cols)
        })
    }

    /// Typed boolean slices per row-segment.
    pub fn booleans_slices(
        &'a self,
    ) -> impl Iterator<Item = (usize, usize, Vec<Arc<BooleanArray>>)> + 'a {
        self.iter_row_chunks().map(|cs| {
            let cols = cs
                .cols
                .iter()
                .map(|cc| {
                    let a = cc.booleans.as_ref().expect("booleans lane exists");
                    let ba = a.as_any().downcast_ref::<BooleanArray>().unwrap().clone();
                    Arc::new(ba)
                })
                .collect();
            (cs.row_start, cs.row_len, cols)
        })
    }

    /// Text slices per row-segment (erased as ArrayRef for Utf8 today; future Dict/View support).
    pub fn text_slices(&'a self) -> impl Iterator<Item = (usize, usize, Vec<ArrayRef>)> + 'a {
        self.iter_row_chunks().map(|cs| {
            let cols = cs
                .cols
                .iter()
                .map(|cc| cc.text.as_ref().expect("text lane exists").clone())
                .collect();
            (cs.row_start, cs.row_len, cols)
        })
    }

    /// Typed error-code slices per row-segment.
    pub fn errors_slices(
        &'a self,
    ) -> impl Iterator<Item = (usize, usize, Vec<Arc<UInt8Array>>)> + 'a {
        self.iter_row_chunks().map(|cs| {
            let cols = cs
                .cols
                .iter()
                .map(|cc| {
                    let a = cc.errors.as_ref().expect("errors lane exists");
                    let ea = a.as_any().downcast_ref::<UInt8Array>().unwrap().clone();
                    Arc::new(ea)
                })
                .collect();
            (cs.row_start, cs.row_len, cols)
        })
    }
}

pub struct ChunkSlice {
    pub row_start: usize, // relative to view top
    pub row_len: usize,
    pub cols: Vec<ChunkCol>,
}

pub struct ChunkCol {
    pub numbers: Option<ArrayRef>,
    pub booleans: Option<ArrayRef>,
    pub text: Option<ArrayRef>,
    pub errors: Option<ArrayRef>,
    pub type_tag: ArrayRef,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_schema::DataType;

    #[test]
    fn ingest_mixed_rows_into_lanes_and_tags() {
        let mut b = IngestBuilder::new("Sheet1", 1, 1024);
        let data = vec![
            LiteralValue::Number(42.5),                   // Number
            LiteralValue::Empty,                          // Empty
            LiteralValue::Text(String::new()),            // Empty text (Text lane)
            LiteralValue::Boolean(true),                  // Boolean
            LiteralValue::Error(ExcelError::new_value()), // Error
        ];
        for v in &data {
            b.append_row(std::slice::from_ref(v)).unwrap();
        }
        let sheet = b.finish();
        assert_eq!(sheet.nrows, 5);
        assert_eq!(sheet.columns.len(), 1);
        assert_eq!(sheet.columns[0].chunks.len(), 1);
        let ch = &sheet.columns[0].chunks[0];

        // Type tags
        let tags = ch.type_tag.values();
        assert_eq!(tags.len(), 5);
        assert_eq!(tags[0], TypeTag::Number as u8);
        assert_eq!(tags[1], TypeTag::Empty as u8);
        assert_eq!(tags[2], TypeTag::Text as u8);
        assert_eq!(tags[3], TypeTag::Boolean as u8);
        assert_eq!(tags[4], TypeTag::Error as u8);

        // Numbers lane validity
        let nums = ch.numbers.as_ref().unwrap();
        assert_eq!(nums.len(), 5);
        assert_eq!(nums.null_count(), 4);
        assert!(nums.is_valid(0));

        // Booleans lane validity
        let bools = ch.booleans.as_ref().unwrap();
        assert_eq!(bools.len(), 5);
        assert_eq!(bools.null_count(), 4);
        assert!(bools.is_valid(3));

        // Text lane validity
        let txt = ch.text.as_ref().unwrap();
        assert_eq!(txt.len(), 5);
        assert_eq!(txt.null_count(), 4);
        assert!(txt.is_valid(2)); // ""

        // Errors lane
        let errs = ch.errors.as_ref().unwrap();
        assert_eq!(errs.len(), 5);
        assert_eq!(errs.null_count(), 4);
        assert!(errs.is_valid(4));
    }

    #[test]
    fn range_view_get_cell_and_padding() {
        let mut b = IngestBuilder::new("S", 2, 2);
        b.append_row(&[LiteralValue::Number(1.0), LiteralValue::Text("".into())])
            .unwrap();
        b.append_row(&[LiteralValue::Empty, LiteralValue::Text("x".into())])
            .unwrap();
        b.append_row(&[LiteralValue::Boolean(true), LiteralValue::Empty])
            .unwrap();
        let sheet = b.finish();
        let rv = sheet.range_view(0, 0, 2, 1);
        assert_eq!(rv.dims(), (3, 2));
        // Inside
        assert_eq!(rv.get_cell(0, 0), LiteralValue::Number(1.0));
        assert_eq!(rv.get_cell(0, 1), LiteralValue::Text(String::new())); // empty string
        assert_eq!(rv.get_cell(1, 0), LiteralValue::Empty); // truly Empty
        assert_eq!(rv.get_cell(2, 0), LiteralValue::Boolean(true));
        // OOB padding
        assert_eq!(rv.get_cell(3, 0), LiteralValue::Empty);
        assert_eq!(rv.get_cell(0, 2), LiteralValue::Empty);

        // Numbers slices should produce one 2-row and one 1-row segment
        let nums: Vec<_> = rv.numbers_slices().collect();
        assert_eq!(nums.len(), 2);
        assert_eq!(nums[0].0, 0);
        assert_eq!(nums[0].1, 2);
        assert_eq!(nums[1].0, 2);
        assert_eq!(nums[1].1, 1);
    }

    #[test]
    fn row_chunk_slices_shape() {
        // chunk_rows=2 leads to two slices for 3 rows
        let mut b = IngestBuilder::new("S", 2, 2);
        b.append_row(&[LiteralValue::Text("a".into()), LiteralValue::Number(1.0)])
            .unwrap();
        b.append_row(&[LiteralValue::Text("b".into()), LiteralValue::Number(2.0)])
            .unwrap();
        b.append_row(&[LiteralValue::Text("c".into()), LiteralValue::Number(3.0)])
            .unwrap();
        let sheet = b.finish();
        let rv = sheet.range_view(0, 0, 2, 1);
        let slices = rv.row_chunk_slices();
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].row_start, 0);
        assert_eq!(slices[0].row_len, 2);
        assert_eq!(slices[0].cols.len(), 2);
        assert_eq!(slices[1].row_start, 2);
        assert_eq!(slices[1].row_len, 1);
        assert_eq!(slices[1].cols.len(), 2);
    }

    #[test]
    fn oob_columns_are_padded() {
        // Build with 2 columns; request 3 columns (ec beyond last col)
        let mut b = IngestBuilder::new("S", 2, 2);
        b.append_row(&[LiteralValue::Number(1.0), LiteralValue::Text("a".into())])
            .unwrap();
        b.append_row(&[LiteralValue::Number(2.0), LiteralValue::Text("b".into())])
            .unwrap();
        let sheet = b.finish();
        // Request cols [0..=2] → 3 columns with padding
        let rv = sheet.range_view(0, 0, 1, 2);
        assert_eq!(rv.dims(), (2, 3));
        let slices = rv.row_chunk_slices();
        assert!(!slices.is_empty());
        for cs in &slices {
            assert_eq!(cs.cols.len(), 3);
        }
        // Also validate typed slices return 3 entries per segment
        for (_rs, _rl, cols) in rv.numbers_slices() {
            assert_eq!(cols.len(), 3);
        }
        for (_rs, _rl, cols) in rv.booleans_slices() {
            assert_eq!(cols.len(), 3);
        }
        for (_rs, _rl, cols) in rv.text_slices() {
            assert_eq!(cols.len(), 3);
        }
        for (_rs, _rl, cols) in rv.errors_slices() {
            assert_eq!(cols.len(), 3);
        }
    }

    #[test]
    fn reversed_range_is_empty() {
        let mut b = IngestBuilder::new("S", 1, 4);
        b.append_row(&[LiteralValue::Number(1.0)]).unwrap();
        b.append_row(&[LiteralValue::Number(2.0)]).unwrap();
        let sheet = b.finish();
        let rv = sheet.range_view(3, 0, 1, 0); // er < sr
        assert_eq!(rv.dims(), (0, 0));
        assert!(rv.row_chunk_slices().is_empty());
        assert_eq!(rv.get_cell(0, 0), LiteralValue::Empty);
    }

    #[test]
    fn chunk_alignment_invariant() {
        let mut b = IngestBuilder::new("S", 3, 2);
        // 5 rows, 2-row chunks => 3 chunks (2,2,1)
        for r in 0..5 {
            b.append_row(&[
                LiteralValue::Number(r as f64),
                LiteralValue::Text(format!("{r}")),
                if r % 2 == 0 {
                    LiteralValue::Empty
                } else {
                    LiteralValue::Boolean(true)
                },
            ])
            .unwrap();
        }
        let sheet = b.finish();
        // chunk_starts should be [0,2,4]
        assert_eq!(sheet.chunk_starts, vec![0, 2, 4]);
        // All columns must share per-chunk lengths equal to [2,2,1]
        let lens0: Vec<usize> = sheet.columns[0]
            .chunks
            .iter()
            .map(|ch| ch.type_tag.len())
            .collect();
        for col in &sheet.columns[1..] {
            let lens: Vec<usize> = col.chunks.iter().map(|ch| ch.type_tag.len()).collect();
            assert_eq!(lens, lens0);
        }
    }

    #[test]
    fn chunking_splits_rows() {
        // Two columns, chunk size 2 → expect two chunks
        let mut b = IngestBuilder::new("S", 2, 2);
        let rows = vec![
            vec![LiteralValue::Number(1.0), LiteralValue::Text("a".into())],
            vec![LiteralValue::Empty, LiteralValue::Text("b".into())],
            vec![LiteralValue::Boolean(true), LiteralValue::Empty],
        ];
        for r in rows {
            b.append_row(&r).unwrap();
        }
        let sheet = b.finish();
        assert_eq!(sheet.columns[0].chunks.len(), 2);
        assert_eq!(sheet.columns[1].chunks.len(), 2);
        assert_eq!(sheet.columns[0].chunks[0].numbers_or_null().len(), 2);
        assert_eq!(sheet.columns[0].chunks[1].numbers_or_null().len(), 1);
    }

    #[test]
    fn pending_is_not_error() {
        let mut b = IngestBuilder::new("S", 1, 8);
        b.append_row(&[LiteralValue::Pending]).unwrap();
        let sheet = b.finish();
        let ch = &sheet.columns[0].chunks[0];
        // tag is Pending
        assert_eq!(ch.type_tag.values()[0], super::TypeTag::Pending as u8);
        // errors lane is effectively null
        let errs = ch.errors_or_null();
        assert_eq!(errs.null_count(), 1);
    }

    #[test]
    fn all_null_numeric_lane_uses_null_array() {
        // Only text values in first column → numbers lane should be all null with correct dtype
        let mut b = IngestBuilder::new("S", 1, 16);
        b.append_row(&[LiteralValue::Text("a".into())]).unwrap();
        b.append_row(&[LiteralValue::Text("".into())]).unwrap();
        b.append_row(&[LiteralValue::Text("b".into())]).unwrap();
        let sheet = b.finish();
        let ch = &sheet.columns[0].chunks[0];
        let nums = ch.numbers_or_null();
        assert_eq!(nums.len(), 3);
        assert_eq!(nums.null_count(), 3);
        assert_eq!(nums.data_type(), &DataType::Float64);
    }
}
