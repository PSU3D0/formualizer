use arrow_array::Array;
use arrow_array::new_null_array;
use arrow_schema::DataType;
use chrono::Timelike;
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, StringBuilder, UInt8Builder};
use arrow_array::{ArrayRef, BooleanArray, Float64Array, StringArray, UInt8Array, UInt32Array};
use once_cell::sync::OnceCell;

use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use rustc_hash::FxHashMap;
use std::collections::HashMap;

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
    pub fn from_u8(b: u8) -> Self {
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
    // Cache: lowered text lane (ASCII lower), nulls preserved
    lowered_text: OnceCell<ArrayRef>,
    // Phase C: per-chunk overlay (delta edits since last compaction)
    pub overlay: Overlay,
    // Phase 0/1: separate computed overlay (formula/spill outputs)
    pub computed_overlay: Overlay,
}

impl ColumnChunk {
    #[inline]
    pub fn len(&self) -> usize {
        self.type_tag.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    /// Lowercased text lane (ASCII lower), with nulls preserved. Cached per chunk.
    pub fn text_lower_or_null(&self) -> ArrayRef {
        if let Some(a) = self.lowered_text.get() {
            return a.clone();
        }
        // Lowercase when text present; else return null Utf8
        let out: ArrayRef = if let Some(txt) = &self.text {
            let sa = txt.as_any().downcast_ref::<StringArray>().unwrap();
            let mut b = arrow_array::builder::StringBuilder::with_capacity(sa.len(), sa.len() * 8);
            for i in 0..sa.len() {
                if sa.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(sa.value(i).to_ascii_lowercase());
                }
            }
            let lowered = b.finish();
            Arc::new(lowered)
        } else {
            new_null_array(&DataType::Utf8, self.len())
        };
        self.lowered_text.get_or_init(|| out.clone());
        out
    }
}

#[derive(Debug, Clone)]
pub struct ArrowColumn {
    pub chunks: Vec<ColumnChunk>,
    pub sparse_chunks: FxHashMap<usize, ColumnChunk>,
    pub index: u32,
}

impl ArrowColumn {
    #[inline]
    pub fn chunk(&self, idx: usize) -> Option<&ColumnChunk> {
        if idx < self.chunks.len() {
            Some(&self.chunks[idx])
        } else {
            self.sparse_chunks.get(&idx)
        }
    }

    #[inline]
    pub fn chunk_mut(&mut self, idx: usize) -> Option<&mut ColumnChunk> {
        if idx < self.chunks.len() {
            Some(&mut self.chunks[idx])
        } else {
            self.sparse_chunks.get_mut(&idx)
        }
    }

    #[inline]
    pub fn has_sparse_chunks(&self) -> bool {
        !self.sparse_chunks.is_empty()
    }

    #[inline]
    pub fn total_chunk_count(&self) -> usize {
        self.chunks.len() + self.sparse_chunks.len()
    }
}

#[derive(Debug, Clone)]
pub struct ArrowSheet {
    pub name: Arc<str>,
    pub columns: Vec<ArrowColumn>,
    pub nrows: u32,
    pub chunk_starts: Vec<usize>,
    /// Preferred chunk size (rows) for capacity growth operations.
    ///
    /// For Arrow-ingested sheets this matches the ingest `chunk_rows`. For sparse/overlay-created
    /// sheets this defaults to 32k to avoid creating thousands of tiny chunks during growth.
    pub chunk_rows: usize,
}

#[derive(Debug, Default, Clone)]
pub struct SheetStore {
    pub sheets: Vec<ArrowSheet>,
}

impl SheetStore {
    pub fn sheet(&self, name: &str) -> Option<&ArrowSheet> {
        self.sheets.iter().find(|s| s.name.as_ref() == name)
    }
    pub fn sheet_mut(&mut self, name: &str) -> Option<&mut ArrowSheet> {
        self.sheets.iter_mut().find(|s| s.name.as_ref() == name)
    }
}

/// Ingestion builder that writes per-column Arrow arrays with a lane/tag design.
pub struct IngestBuilder {
    name: Arc<str>,
    ncols: usize,
    chunk_rows: usize,
    date_system: crate::engine::DateSystem,

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
    pub fn new(
        sheet_name: &str,
        ncols: usize,
        chunk_rows: usize,
        date_system: crate::engine::DateSystem,
    ) -> Self {
        let mut chunks = Vec::with_capacity(ncols);
        chunks.resize_with(ncols, Vec::new);
        Self {
            name: Arc::from(sheet_name.to_string()),
            ncols,
            chunk_rows: chunk_rows.max(1),
            date_system,
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

    /// Zero-allocation row append from typed cell tokens (no LiteralValue).
    /// Text borrows are copied into the internal StringBuilder.
    pub fn append_row_cells<'a>(&mut self, row: &[CellIngest<'a>]) -> Result<(), ExcelError> {
        assert_eq!(row.len(), self.ncols, "row width mismatch");
        for (c, cell) in row.iter().enumerate() {
            match cell {
                CellIngest::Empty => {
                    self.tag_builders[c].append_value(TypeTag::Empty as u8);
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                CellIngest::Number(n) => {
                    self.tag_builders[c].append_value(TypeTag::Number as u8);
                    self.num_builders[c].append_value(*n);
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                CellIngest::Boolean(b) => {
                    self.tag_builders[c].append_value(TypeTag::Boolean as u8);
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_value(*b);
                    self.lane_counts[c].n_bool += 1;
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                CellIngest::Text(s) => {
                    self.tag_builders[c].append_value(TypeTag::Text as u8);
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_value(s);
                    self.lane_counts[c].n_text += 1;
                    self.err_builders[c].append_null();
                }
                CellIngest::ErrorCode(code) => {
                    self.tag_builders[c].append_value(TypeTag::Error as u8);
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_value(*code);
                    self.lane_counts[c].n_err += 1;
                }
                CellIngest::DateSerial(serial) => {
                    self.tag_builders[c].append_value(TypeTag::DateTime as u8);
                    self.num_builders[c].append_value(*serial);
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                CellIngest::Pending => {
                    self.tag_builders[c].append_value(TypeTag::Pending as u8);
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

    /// Streaming row append from an iterator of typed cell tokens.
    /// Requires an `ExactSizeIterator` to validate row width without materializing a Vec.
    pub fn append_row_cells_iter<'a, I>(&mut self, iter: I) -> Result<(), ExcelError>
    where
        I: ExactSizeIterator<Item = CellIngest<'a>>,
    {
        assert_eq!(iter.len(), self.ncols, "row width mismatch");
        for (c, cell) in iter.enumerate() {
            match cell {
                CellIngest::Empty => {
                    self.tag_builders[c].append_value(TypeTag::Empty as u8);
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                CellIngest::Number(n) => {
                    self.tag_builders[c].append_value(TypeTag::Number as u8);
                    self.num_builders[c].append_value(n);
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                CellIngest::Boolean(b) => {
                    self.tag_builders[c].append_value(TypeTag::Boolean as u8);
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_value(b);
                    self.lane_counts[c].n_bool += 1;
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                CellIngest::Text(s) => {
                    self.tag_builders[c].append_value(TypeTag::Text as u8);
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_value(s);
                    self.lane_counts[c].n_text += 1;
                    self.err_builders[c].append_null();
                }
                CellIngest::ErrorCode(code) => {
                    self.tag_builders[c].append_value(TypeTag::Error as u8);
                    self.num_builders[c].append_null();
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_value(code);
                    self.lane_counts[c].n_err += 1;
                }
                CellIngest::DateSerial(serial) => {
                    self.tag_builders[c].append_value(TypeTag::DateTime as u8);
                    self.num_builders[c].append_value(serial);
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                CellIngest::Pending => {
                    self.tag_builders[c].append_value(TypeTag::Pending as u8);
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
                    let serial =
                        crate::builtins::datetime::datetime_to_serial_for(self.date_system, &dt);
                    self.num_builders[c].append_value(serial);
                    self.lane_counts[c].n_num += 1;
                    self.bool_builders[c].append_null();
                    self.text_builders[c].append_null();
                    self.err_builders[c].append_null();
                }
                LiteralValue::DateTime(dt) => {
                    let serial =
                        crate::builtins::datetime::datetime_to_serial_for(self.date_system, dt);
                    self.num_builders[c].append_value(serial);
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
                lowered_text: OnceCell::new(),
                overlay: Overlay::new(),
                computed_overlay: Overlay::new(),
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
                sparse_chunks: FxHashMap::default(),
                index: idx as u32,
            });
        }
        // Precompute chunk starts from first column and enforce alignment across columns
        let mut chunk_starts: Vec<usize> = Vec::new();
        if let Some(col0) = columns.first() {
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
                            "ArrowSheet chunk row-length misalignment at chunk {i}: col {ci} len={got} != {len_i}"
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
            chunk_rows: self.chunk_rows,
        }
    }
}

pub fn map_error_code(kind: ExcelErrorKind) -> u8 {
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

pub fn unmap_error_code(code: u8) -> ExcelErrorKind {
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

// ─────────────────────────── Overlay (Phase C) ────────────────────────────

/// Zero-allocation cell token for ingestion.
pub enum CellIngest<'a> {
    Empty,
    Number(f64),
    Boolean(bool),
    Text(&'a str),
    ErrorCode(u8),
    DateSerial(f64),
    Pending,
}

#[derive(Debug, Clone)]
pub enum OverlayValue {
    Empty,
    Number(f64),
    Boolean(bool),
    Text(Arc<str>),
    Error(u8),
    Pending,
}

#[derive(Debug, Default, Clone)]
pub struct Overlay {
    map: HashMap<usize, OverlayValue>,
}

impl Overlay {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    #[inline]
    pub fn get(&self, off: usize) -> Option<&OverlayValue> {
        self.map.get(&off)
    }
    #[inline]
    pub fn set(&mut self, off: usize, v: OverlayValue) {
        self.map.insert(off, v);
    }
    #[inline]
    pub fn clear(&mut self) {
        self.map.clear();
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    #[inline]
    pub fn any_in_range(&self, range: core::ops::Range<usize>) -> bool {
        self.map.keys().any(|k| range.contains(k))
    }
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

    pub fn range_view(
        &self,
        sr: usize,
        sc: usize,
        er: usize,
        ec: usize,
    ) -> crate::engine::range_view::RangeView<'_> {
        let r0 = er.checked_sub(sr).map(|d| d + 1).unwrap_or(0);
        let c0 = ec.checked_sub(sc).map(|d| d + 1).unwrap_or(0);
        let (rows, cols) = if r0 == 0 || c0 == 0 { (0, 0) } else { (r0, c0) };
        crate::engine::range_view::RangeView::new(
            crate::engine::range_view::RangeBacking::Borrowed(self),
            sr,
            sc,
            er,
            ec,
            rows,
            cols,
        )
    }

    /// Ensure capacity to address at least `target_rows` rows by extending the row chunk map.
    ///
    /// This updates `chunk_starts`/`nrows` but does **not** eagerly densify all columns with
    /// new empty chunks. Missing chunks are treated as all-empty and can be materialized lazily.
    pub fn ensure_row_capacity(&mut self, target_rows: usize) {
        if target_rows as u32 <= self.nrows {
            return;
        }

        if self.chunk_starts.is_empty() {
            self.chunk_starts.push(0);
        }

        let chunk_size = self.chunk_rows.max(1);

        let mut cur_rows = self.nrows as usize;
        while cur_rows < target_rows {
            let len = (target_rows - cur_rows).min(chunk_size.max(1));
            // Start of the next chunk is the current row count.
            if self.chunk_starts.last().copied() != Some(cur_rows) {
                self.chunk_starts.push(cur_rows);
            }
            cur_rows += len;
            self.nrows = cur_rows as u32;
        }
    }

    /// Ensure a mutable chunk for a given column/chunk index.
    ///
    /// If the chunk is beyond the column's dense chunk vector, it is stored in `sparse_chunks`.
    pub fn ensure_column_chunk_mut(
        &mut self,
        col_idx: usize,
        ch_idx: usize,
    ) -> Option<&mut ColumnChunk> {
        let start = *self.chunk_starts.get(ch_idx)?;
        let end = self
            .chunk_starts
            .get(ch_idx + 1)
            .copied()
            .unwrap_or(self.nrows as usize);
        let len = end.saturating_sub(start);

        let col = self.columns.get_mut(col_idx)?;
        if ch_idx < col.chunks.len() {
            return Some(&mut col.chunks[ch_idx]);
        }
        Some(
            col.sparse_chunks
                .entry(ch_idx)
                .or_insert_with(|| Self::make_empty_chunk(len)),
        )
    }

    /// Return (chunk_idx, in_chunk_offset) for absolute 0-based row.
    pub fn chunk_of_row(&self, abs_row: usize) -> Option<(usize, usize)> {
        if abs_row >= self.nrows as usize {
            return None;
        }
        let ch_idx = match self.chunk_starts.binary_search(&abs_row) {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        };
        let start = self.chunk_starts[ch_idx];
        Some((ch_idx, abs_row - start))
    }

    fn recompute_chunk_starts(&mut self) {
        self.chunk_starts.clear();
        if let Some(col0) = self.columns.first() {
            let mut cur = 0usize;
            for ch in &col0.chunks {
                self.chunk_starts.push(cur);
                cur += ch.type_tag.len();
            }
        }
    }

    fn make_empty_chunk(len: usize) -> ColumnChunk {
        ColumnChunk {
            numbers: None,
            booleans: None,
            text: None,
            errors: None,
            type_tag: Arc::new(UInt8Array::from(vec![TypeTag::Empty as u8; len])),
            formula_id: None,
            meta: ColumnChunkMeta {
                len,
                non_null_num: 0,
                non_null_bool: 0,
                non_null_text: 0,
                non_null_err: 0,
            },
            lazy_null_numbers: OnceCell::new(),
            lazy_null_booleans: OnceCell::new(),
            lazy_null_text: OnceCell::new(),
            lazy_null_errors: OnceCell::new(),
            lowered_text: OnceCell::new(),
            overlay: Overlay::new(),
            computed_overlay: Overlay::new(),
        }
    }

    fn slice_chunk(ch: &ColumnChunk, off: usize, len: usize) -> ColumnChunk {
        // Slice type tags
        use arrow_array::Array;
        let type_tag: Arc<UInt8Array> = Arc::new(
            Array::slice(ch.type_tag.as_ref(), off, len)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .clone(),
        );
        // Slice numbers if present and keep only if any non-null
        let numbers: Option<Arc<Float64Array>> = ch.numbers.as_ref().and_then(|a| {
            let sl = Array::slice(a.as_ref(), off, len);
            let fa = sl.as_any().downcast_ref::<Float64Array>().unwrap().clone();
            let nn = len.saturating_sub(fa.null_count());
            if nn == 0 { None } else { Some(Arc::new(fa)) }
        });
        let booleans: Option<Arc<BooleanArray>> = ch.booleans.as_ref().and_then(|a| {
            let sl = Array::slice(a.as_ref(), off, len);
            let ba = sl.as_any().downcast_ref::<BooleanArray>().unwrap().clone();
            let nn = len.saturating_sub(ba.null_count());
            if nn == 0 { None } else { Some(Arc::new(ba)) }
        });
        let text: Option<ArrayRef> = ch.text.as_ref().and_then(|a| {
            let sl = Array::slice(a.as_ref(), off, len);
            let sa = sl.as_any().downcast_ref::<StringArray>().unwrap().clone();
            let nn = len.saturating_sub(sa.null_count());
            if nn == 0 {
                None
            } else {
                Some(Arc::new(sa) as ArrayRef)
            }
        });
        let errors: Option<Arc<UInt8Array>> = ch.errors.as_ref().and_then(|a| {
            let sl = Array::slice(a.as_ref(), off, len);
            let ea = sl.as_any().downcast_ref::<UInt8Array>().unwrap().clone();
            let nn = len.saturating_sub(ea.null_count());
            if nn == 0 { None } else { Some(Arc::new(ea)) }
        });
        // Split overlays for this slice
        let mut overlay = Overlay::new();
        for (k, v) in ch.overlay.map.iter() {
            if *k >= off && *k < off + len {
                overlay.set(*k - off, v.clone());
            }
        }
        let mut computed_overlay = Overlay::new();
        for (k, v) in ch.computed_overlay.map.iter() {
            if *k >= off && *k < off + len {
                computed_overlay.set(*k - off, v.clone());
            }
        }
        let non_null_num = numbers.as_ref().map(|a| len - a.null_count()).unwrap_or(0);
        let non_null_bool = booleans.as_ref().map(|a| len - a.null_count()).unwrap_or(0);
        let non_null_text = text.as_ref().map(|a| len - a.null_count()).unwrap_or(0);
        let non_null_err = errors.as_ref().map(|a| len - a.null_count()).unwrap_or(0);
        ColumnChunk {
            numbers: numbers.clone(),
            booleans: booleans.clone(),
            text: text.clone(),
            errors: errors.clone(),
            type_tag,
            formula_id: None,
            meta: ColumnChunkMeta {
                len,
                non_null_num,
                non_null_bool,
                non_null_text,
                non_null_err,
            },
            lazy_null_numbers: OnceCell::new(),
            lazy_null_booleans: OnceCell::new(),
            lazy_null_text: OnceCell::new(),
            lazy_null_errors: OnceCell::new(),
            lowered_text: OnceCell::new(),
            overlay,
            computed_overlay,
        }
    }

    /// Heuristic compaction: rebuilds a chunk's base arrays by applying its overlay when
    /// overlay density crosses thresholds. Returns true if a rebuild occurred.
    pub fn maybe_compact_chunk(
        &mut self,
        col_idx: usize,
        ch_idx: usize,
        abs_threshold: usize,
        frac_den: usize,
    ) -> bool {
        if col_idx >= self.columns.len() {
            return false;
        }

        let (len, tags, numbers, booleans, text, errors, non_num, non_bool, non_text, non_err) = {
            let Some(ch_ref) = self.columns[col_idx].chunk(ch_idx) else {
                return false;
            };
            let len = ch_ref.type_tag.len();
            if len == 0 {
                return false;
            }

            let ov_len = ch_ref.overlay.len();
            let den = frac_den.max(1);
            let trig = ov_len > (len / den) || ov_len > abs_threshold;
            if !trig {
                return false;
            }

            // Rebuild: merge base lanes with overlays row-by-row.
            let mut tag_b = UInt8Builder::with_capacity(len);
            let mut nb = Float64Builder::with_capacity(len);
            let mut bb = BooleanBuilder::with_capacity(len);
            let mut sb = StringBuilder::with_capacity(len, len * 8);
            let mut eb = UInt8Builder::with_capacity(len);
            let mut non_num = 0usize;
            let mut non_bool = 0usize;
            let mut non_text = 0usize;
            let mut non_err = 0usize;

            for i in 0..len {
                // If overlay present, use it. Otherwise, use base tag+lane.
                if let Some(ov) = ch_ref.overlay.get(i) {
                    match ov {
                        OverlayValue::Empty => {
                            tag_b.append_value(TypeTag::Empty as u8);
                            nb.append_null();
                            bb.append_null();
                            sb.append_null();
                            eb.append_null();
                        }
                        OverlayValue::Number(n) => {
                            tag_b.append_value(TypeTag::Number as u8);
                            nb.append_value(*n);
                            non_num += 1;
                            bb.append_null();
                            sb.append_null();
                            eb.append_null();
                        }
                        OverlayValue::Boolean(b) => {
                            tag_b.append_value(TypeTag::Boolean as u8);
                            nb.append_null();
                            bb.append_value(*b);
                            non_bool += 1;
                            sb.append_null();
                            eb.append_null();
                        }
                        OverlayValue::Text(s) => {
                            tag_b.append_value(TypeTag::Text as u8);
                            nb.append_null();
                            bb.append_null();
                            sb.append_value(s);
                            non_text += 1;
                            eb.append_null();
                        }
                        OverlayValue::Error(code) => {
                            tag_b.append_value(TypeTag::Error as u8);
                            nb.append_null();
                            bb.append_null();
                            sb.append_null();
                            eb.append_value(*code);
                            non_err += 1;
                        }
                        OverlayValue::Pending => {
                            tag_b.append_value(TypeTag::Pending as u8);
                            nb.append_null();
                            bb.append_null();
                            sb.append_null();
                            eb.append_null();
                        }
                    }
                } else {
                    let tag = TypeTag::from_u8(ch_ref.type_tag.value(i));
                    match tag {
                        TypeTag::Empty => {
                            tag_b.append_value(TypeTag::Empty as u8);
                            nb.append_null();
                            bb.append_null();
                            sb.append_null();
                            eb.append_null();
                        }
                        TypeTag::Number | TypeTag::DateTime | TypeTag::Duration => {
                            tag_b.append_value(TypeTag::Number as u8);
                            if let Some(a) = &ch_ref.numbers {
                                let fa = a.as_any().downcast_ref::<Float64Array>().unwrap();
                                if fa.is_null(i) {
                                    nb.append_null();
                                } else {
                                    nb.append_value(fa.value(i));
                                    non_num += 1;
                                }
                            } else {
                                nb.append_null();
                            }
                            bb.append_null();
                            sb.append_null();
                            eb.append_null();
                        }
                        TypeTag::Boolean => {
                            tag_b.append_value(TypeTag::Boolean as u8);
                            nb.append_null();
                            if let Some(a) = &ch_ref.booleans {
                                let ba = a.as_any().downcast_ref::<BooleanArray>().unwrap();
                                if ba.is_null(i) {
                                    bb.append_null();
                                } else {
                                    bb.append_value(ba.value(i));
                                    non_bool += 1;
                                }
                            } else {
                                bb.append_null();
                            }
                            sb.append_null();
                            eb.append_null();
                        }
                        TypeTag::Text => {
                            tag_b.append_value(TypeTag::Text as u8);
                            nb.append_null();
                            bb.append_null();
                            if let Some(a) = &ch_ref.text {
                                let sa = a.as_any().downcast_ref::<StringArray>().unwrap();
                                if sa.is_null(i) {
                                    sb.append_null();
                                } else {
                                    sb.append_value(sa.value(i));
                                    non_text += 1;
                                }
                            } else {
                                sb.append_null();
                            }
                            eb.append_null();
                        }
                        TypeTag::Error => {
                            tag_b.append_value(TypeTag::Error as u8);
                            nb.append_null();
                            bb.append_null();
                            sb.append_null();
                            if let Some(a) = &ch_ref.errors {
                                let ea = a.as_any().downcast_ref::<UInt8Array>().unwrap();
                                if ea.is_null(i) {
                                    eb.append_null();
                                } else {
                                    eb.append_value(ea.value(i));
                                    non_err += 1;
                                }
                            } else {
                                eb.append_null();
                            }
                        }
                        TypeTag::Pending => {
                            tag_b.append_value(TypeTag::Pending as u8);
                            nb.append_null();
                            bb.append_null();
                            sb.append_null();
                            eb.append_null();
                        }
                    }
                }
            }

            let tags = Arc::new(tag_b.finish());
            let numbers = {
                let a = nb.finish();
                if non_num == 0 {
                    None
                } else {
                    Some(Arc::new(a))
                }
            };
            let booleans = {
                let a = bb.finish();
                if non_bool == 0 {
                    None
                } else {
                    Some(Arc::new(a))
                }
            };
            let text = {
                let a = sb.finish();
                if non_text == 0 {
                    None
                } else {
                    Some(Arc::new(a) as ArrayRef)
                }
            };
            let errors = {
                let a = eb.finish();
                if non_err == 0 {
                    None
                } else {
                    Some(Arc::new(a))
                }
            };

            (
                len, tags, numbers, booleans, text, errors, non_num, non_bool, non_text, non_err,
            )
        };

        let Some(ch_mut) = self.columns[col_idx].chunk_mut(ch_idx) else {
            return false;
        };

        ch_mut.type_tag = tags;
        ch_mut.numbers = numbers;
        ch_mut.booleans = booleans;
        ch_mut.text = text;
        ch_mut.errors = errors;
        ch_mut.overlay.clear();
        ch_mut.lowered_text = OnceCell::new();
        ch_mut.meta.len = len;
        ch_mut.meta.non_null_num = non_num;
        ch_mut.meta.non_null_bool = non_bool;
        ch_mut.meta.non_null_text = non_text;
        ch_mut.meta.non_null_err = non_err;
        true
    }

    /// Insert `count` rows before absolute 0-based row `before`.
    pub fn insert_rows(&mut self, before: usize, count: usize) {
        if count == 0 {
            return;
        }

        let total_rows = self.nrows as usize;
        if total_rows == 0 {
            self.nrows = count as u32;
            if self.nrows > 0 && self.chunk_starts.is_empty() {
                self.chunk_starts.push(0);
            }
            return;
        }

        // Ensure a valid chunk map for non-empty sheets.
        if self.chunk_starts.is_empty() {
            self.chunk_starts.push(0);
        }

        // "Dense" mode: every column has every chunk (legacy invariant).
        let dense_aligned = self
            .columns
            .iter()
            .all(|c| c.sparse_chunks.is_empty() && c.chunks.len() == self.chunk_starts.len());

        let insert_at = before.min(total_rows);
        let (split_idx, split_off) = if insert_at == total_rows {
            // Append at end: split after last chunk.
            let last_idx = self.chunk_starts.len() - 1;
            let last_start = self.chunk_starts[last_idx];
            let last_len = total_rows.saturating_sub(last_start);
            (last_idx, last_len)
        } else {
            self.chunk_of_row(insert_at).unwrap_or((0, 0))
        };

        if dense_aligned {
            // Rebuild chunks for each column (including inserted empty chunk) and recompute starts.
            for col in &mut self.columns {
                let mut new_chunks: Vec<ColumnChunk> = Vec::with_capacity(col.chunks.len() + 2);
                for i in 0..col.chunks.len() {
                    if i != split_idx {
                        new_chunks.push(col.chunks[i].clone());
                    } else {
                        let orig = &col.chunks[i];
                        let len = orig.type_tag.len();
                        if split_off > 0 {
                            new_chunks.push(Self::slice_chunk(orig, 0, split_off));
                        }
                        new_chunks.push(Self::make_empty_chunk(count));
                        if split_off < len {
                            new_chunks.push(Self::slice_chunk(orig, split_off, len - split_off));
                        }
                    }
                }
                col.chunks = new_chunks;
                col.sparse_chunks.clear();
            }
            self.nrows = (total_rows + count) as u32;
            self.recompute_chunk_starts();
            return;
        }

        // Sparse-aware mode: `chunk_starts` is authoritative and missing chunks are treated as empty.
        #[derive(Clone, Copy)]
        enum PlanItem {
            Slice {
                old_idx: usize,
                off: usize,
                len: usize,
            },
            Empty {
                len: usize,
            },
        }

        let mut plan: Vec<PlanItem> = Vec::with_capacity(self.chunk_starts.len() + 2);
        for old_idx in 0..self.chunk_starts.len() {
            let ch_start = self.chunk_starts[old_idx];
            let ch_end = self
                .chunk_starts
                .get(old_idx + 1)
                .copied()
                .unwrap_or(total_rows);
            let ch_len = ch_end.saturating_sub(ch_start);
            if ch_len == 0 {
                continue;
            }

            if old_idx != split_idx {
                plan.push(PlanItem::Slice {
                    old_idx,
                    off: 0,
                    len: ch_len,
                });
                continue;
            }

            let left_len = split_off.min(ch_len);
            let right_len = ch_len.saturating_sub(left_len);
            if left_len > 0 {
                plan.push(PlanItem::Slice {
                    old_idx,
                    off: 0,
                    len: left_len,
                });
            }
            plan.push(PlanItem::Empty { len: count });
            if right_len > 0 {
                plan.push(PlanItem::Slice {
                    old_idx,
                    off: left_len,
                    len: right_len,
                });
            }
        }

        let mut new_starts: Vec<usize> = Vec::with_capacity(plan.len());
        let mut cur = 0usize;
        for item in &plan {
            let len = match *item {
                PlanItem::Slice { len, .. } => len,
                PlanItem::Empty { len } => len,
            };
            if len == 0 {
                continue;
            }
            new_starts.push(cur);
            cur = cur.saturating_add(len);
        }

        debug_assert_eq!(cur, total_rows.saturating_add(count));

        // Update sheet row layout first.
        self.nrows = (total_rows + count) as u32;
        self.chunk_starts = new_starts;

        // Rebuild stored chunks per column using the plan.
        for col in &mut self.columns {
            let old_dense = std::mem::take(&mut col.chunks);
            let old_sparse = std::mem::take(&mut col.sparse_chunks);
            let get_old = |idx: usize| -> Option<&ColumnChunk> {
                if idx < old_dense.len() {
                    Some(&old_dense[idx])
                } else {
                    old_sparse.get(&idx)
                }
            };

            let mut dense: Vec<ColumnChunk> = Vec::new();
            let mut sparse: FxHashMap<usize, ColumnChunk> = FxHashMap::default();
            let mut dense_prefix = true;

            for (new_idx, item) in plan.iter().enumerate() {
                let produced: Option<ColumnChunk> = match *item {
                    PlanItem::Empty { .. } => None,
                    PlanItem::Slice { old_idx, off, len } => match get_old(old_idx) {
                        Some(orig) => {
                            if off == 0 && len == orig.type_tag.len() {
                                Some(orig.clone())
                            } else {
                                Some(Self::slice_chunk(orig, off, len))
                            }
                        }
                        None => None,
                    },
                };

                if let Some(ch) = produced {
                    if dense_prefix && new_idx == dense.len() {
                        dense.push(ch);
                    } else {
                        sparse.insert(new_idx, ch);
                        dense_prefix = false;
                    }
                } else if dense_prefix && new_idx == dense.len() {
                    dense_prefix = false;
                }
            }

            col.chunks = dense;
            col.sparse_chunks = sparse;
        }
    }

    /// Delete `count` rows starting from absolute 0-based row `start`.
    pub fn delete_rows(&mut self, start: usize, count: usize) {
        if count == 0 || self.nrows == 0 {
            return;
        }

        let total_rows = self.nrows as usize;
        if start >= total_rows {
            return;
        }
        let end = (start + count).min(total_rows);
        let del_len = end.saturating_sub(start);
        if del_len == 0 {
            return;
        }

        // Ensure a valid chunk map for non-empty sheets.
        if total_rows > 0 && self.chunk_starts.is_empty() {
            self.chunk_starts.push(0);
        }

        // "Dense" mode: every column has every chunk (legacy invariant).
        let dense_aligned = self
            .columns
            .iter()
            .all(|c| c.sparse_chunks.is_empty() && c.chunks.len() == self.chunk_starts.len());

        if dense_aligned {
            // Dense rebuild by slicing out the deleted window.
            for col in &mut self.columns {
                let mut new_chunks: Vec<ColumnChunk> = Vec::new();
                let mut cur_start = 0usize;
                for ch in &col.chunks {
                    let len = ch.type_tag.len();
                    let ch_end = cur_start + len;
                    // No overlap
                    if ch_end <= start || cur_start >= end {
                        new_chunks.push(ch.clone());
                    } else {
                        // Overlap exists
                        let del_start = start.max(cur_start);
                        let del_end = end.min(ch_end);
                        let left_len = del_start.saturating_sub(cur_start);
                        let right_len = ch_end.saturating_sub(del_end);
                        if left_len > 0 {
                            new_chunks.push(Self::slice_chunk(ch, 0, left_len));
                        }
                        if right_len > 0 {
                            let off = len - right_len;
                            new_chunks.push(Self::slice_chunk(ch, off, right_len));
                        }
                    }
                    cur_start = ch_end;
                }
                col.chunks = new_chunks;
                col.sparse_chunks.clear();
            }
            self.nrows = (total_rows - del_len) as u32;
            self.recompute_chunk_starts();
            return;
        }

        // Sparse-aware mode: `chunk_starts` is authoritative and missing chunks are treated as empty.
        #[derive(Clone, Copy)]
        enum PlanItem {
            Slice {
                old_idx: usize,
                off: usize,
                len: usize,
            },
        }

        let mut plan: Vec<PlanItem> = Vec::with_capacity(self.chunk_starts.len());
        for old_idx in 0..self.chunk_starts.len() {
            let ch_start = self.chunk_starts[old_idx];
            let ch_end = self
                .chunk_starts
                .get(old_idx + 1)
                .copied()
                .unwrap_or(total_rows);
            let ch_len = ch_end.saturating_sub(ch_start);
            if ch_len == 0 {
                continue;
            }

            // No overlap
            if ch_end <= start || ch_start >= end {
                plan.push(PlanItem::Slice {
                    old_idx,
                    off: 0,
                    len: ch_len,
                });
                continue;
            }

            // Left remainder
            if start > ch_start {
                let left_end = start.min(ch_end);
                let left_len = left_end.saturating_sub(ch_start);
                if left_len > 0 {
                    plan.push(PlanItem::Slice {
                        old_idx,
                        off: 0,
                        len: left_len,
                    });
                }
            }

            // Right remainder
            if end < ch_end {
                let right_off = end.saturating_sub(ch_start);
                let right_len = ch_end.saturating_sub(end);
                if right_len > 0 {
                    plan.push(PlanItem::Slice {
                        old_idx,
                        off: right_off,
                        len: right_len,
                    });
                }
            }
        }

        let mut new_starts: Vec<usize> = Vec::with_capacity(plan.len());
        let mut cur = 0usize;
        for item in &plan {
            let len = match *item {
                PlanItem::Slice { len, .. } => len,
            };
            if len == 0 {
                continue;
            }
            new_starts.push(cur);
            cur = cur.saturating_add(len);
        }

        debug_assert_eq!(cur, total_rows.saturating_sub(del_len));

        // Update sheet row layout first.
        self.nrows = (total_rows - del_len) as u32;
        self.chunk_starts = new_starts;

        // Rebuild stored chunks per column using the plan.
        for col in &mut self.columns {
            let old_dense = std::mem::take(&mut col.chunks);
            let old_sparse = std::mem::take(&mut col.sparse_chunks);
            let get_old = |idx: usize| -> Option<&ColumnChunk> {
                if idx < old_dense.len() {
                    Some(&old_dense[idx])
                } else {
                    old_sparse.get(&idx)
                }
            };

            let mut dense: Vec<ColumnChunk> = Vec::new();
            let mut sparse: FxHashMap<usize, ColumnChunk> = FxHashMap::default();
            let mut dense_prefix = true;

            for (new_idx, item) in plan.iter().enumerate() {
                let produced: Option<ColumnChunk> = match *item {
                    PlanItem::Slice { old_idx, off, len } => match get_old(old_idx) {
                        Some(orig) => {
                            if off == 0 && len == orig.type_tag.len() {
                                Some(orig.clone())
                            } else {
                                Some(Self::slice_chunk(orig, off, len))
                            }
                        }
                        None => None,
                    },
                };

                if let Some(ch) = produced {
                    if dense_prefix && new_idx == dense.len() {
                        dense.push(ch);
                    } else {
                        sparse.insert(new_idx, ch);
                        dense_prefix = false;
                    }
                } else if dense_prefix && new_idx == dense.len() {
                    dense_prefix = false;
                }
            }

            col.chunks = dense;
            col.sparse_chunks = sparse;
        }
    }

    /// Insert `count` columns before absolute 0-based column `before` with empty chunks.
    pub fn insert_columns(&mut self, before: usize, count: usize) {
        if count == 0 {
            return;
        }
        // Determine chunk schema from first column if present
        let empty_col = |lens: &[usize]| -> ArrowColumn {
            let mut chunks = Vec::with_capacity(lens.len());
            for &l in lens {
                chunks.push(Self::make_empty_chunk(l));
            }
            ArrowColumn {
                chunks,
                sparse_chunks: FxHashMap::default(),
                index: 0,
            }
        };
        let dense_aligned = !self.columns.is_empty()
            && self
                .columns
                .iter()
                .all(|c| c.sparse_chunks.is_empty() && c.chunks.len() == self.chunk_starts.len());

        let lens: Vec<usize> = if dense_aligned {
            self.columns[0]
                .chunks
                .iter()
                .map(|c| c.type_tag.len())
                .collect()
        } else if self.columns.is_empty() {
            // No columns: single chunk matching nrows if any
            if self.nrows > 0 {
                vec![self.nrows as usize]
            } else {
                Vec::new()
            }
        } else {
            // Sparse sheet: keep inserted columns cheap by materializing no chunks.
            Vec::new()
        };
        let mut cols_new: Vec<ArrowColumn> = Vec::with_capacity(self.columns.len() + count);
        let before_idx = before.min(self.columns.len());
        for (i, col) in self.columns.iter_mut().enumerate() {
            if i == before_idx {
                for _ in 0..count {
                    cols_new.push(empty_col(&lens));
                }
            }
            cols_new.push(col.clone());
        }
        if before_idx == self.columns.len() {
            for _ in 0..count {
                cols_new.push(empty_col(&lens));
            }
        }
        // Fix column indices
        for (idx, col) in cols_new.iter_mut().enumerate() {
            col.index = idx as u32;
        }
        self.columns = cols_new;
        // chunk_starts unchanged; lens were matched
    }

    /// Delete `count` columns starting at absolute 0-based column `start`.
    pub fn delete_columns(&mut self, start: usize, count: usize) {
        if count == 0 || self.columns.is_empty() {
            return;
        }
        let end = (start + count).min(self.columns.len());
        if start >= end {
            return;
        }
        self.columns.drain(start..end);
        for (idx, col) in self.columns.iter_mut().enumerate() {
            col.index = idx as u32;
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_schema::DataType;

    #[test]
    fn ingest_mixed_rows_into_lanes_and_tags() {
        let mut b = IngestBuilder::new("Sheet1", 1, 1024, crate::engine::DateSystem::Excel1900);
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
        let mut b = IngestBuilder::new("S", 2, 2, crate::engine::DateSystem::Excel1900);
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
        let nums: Vec<_> = rv.numbers_slices().map(|r| r.unwrap()).collect();
        assert_eq!(nums.len(), 2);
        assert_eq!(nums[0].0, 0);
        assert_eq!(nums[0].1, 2);
        assert_eq!(nums[1].0, 2);
        assert_eq!(nums[1].1, 1);
    }

    #[test]
    fn overlay_precedence_user_over_computed() {
        let mut b = IngestBuilder::new("S", 1, 8, crate::engine::DateSystem::Excel1900);
        b.append_row(&[LiteralValue::Number(1.0)]).unwrap();
        b.append_row(&[LiteralValue::Empty]).unwrap();
        b.append_row(&[LiteralValue::Empty]).unwrap();
        let mut sheet = b.finish();

        let (ch_i, off) = sheet.chunk_of_row(0).unwrap();
        sheet.columns[0].chunks[ch_i]
            .computed_overlay
            .set(off, OverlayValue::Number(2.0));

        let rv0 = sheet.range_view(0, 0, 0, 0);
        assert_eq!(rv0.get_cell(0, 0), LiteralValue::Number(2.0));
        let nums0: Vec<_> = rv0.numbers_slices().map(|r| r.unwrap()).collect();
        assert_eq!(nums0.len(), 1);
        assert_eq!(nums0[0].2[0].value(0), 2.0);

        sheet.columns[0].chunks[ch_i]
            .overlay
            .set(off, OverlayValue::Number(3.0));

        let rv1 = sheet.range_view(0, 0, 0, 0);
        assert_eq!(rv1.get_cell(0, 0), LiteralValue::Number(3.0));
        let nums1: Vec<_> = rv1.numbers_slices().map(|r| r.unwrap()).collect();
        assert_eq!(nums1.len(), 1);
        assert_eq!(nums1[0].2[0].value(0), 3.0);
    }

    #[test]
    fn row_chunk_slices_shape() {
        // chunk_rows=2 leads to two slices for 3 rows
        let mut b = IngestBuilder::new("S", 2, 2, crate::engine::DateSystem::Excel1900);
        b.append_row(&[LiteralValue::Text("a".into()), LiteralValue::Number(1.0)])
            .unwrap();
        b.append_row(&[LiteralValue::Text("b".into()), LiteralValue::Number(2.0)])
            .unwrap();
        b.append_row(&[LiteralValue::Text("c".into()), LiteralValue::Number(3.0)])
            .unwrap();
        let sheet = b.finish();
        let rv = sheet.range_view(0, 0, 2, 1);
        let slices: Vec<_> = rv.iter_row_chunks().map(|r| r.unwrap()).collect();
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
        let mut b = IngestBuilder::new("S", 2, 2, crate::engine::DateSystem::Excel1900);
        b.append_row(&[LiteralValue::Number(1.0), LiteralValue::Text("a".into())])
            .unwrap();
        b.append_row(&[LiteralValue::Number(2.0), LiteralValue::Text("b".into())])
            .unwrap();
        let sheet = b.finish();
        // Request cols [0..=2] → 3 columns with padding
        let rv = sheet.range_view(0, 0, 1, 2);
        assert_eq!(rv.dims(), (2, 3));
        let slices: Vec<_> = rv.iter_row_chunks().map(|r| r.unwrap()).collect();
        assert!(!slices.is_empty());
        for cs in &slices {
            assert_eq!(cs.cols.len(), 3);
        }
        // Also validate typed slices return 3 entries per segment
        for res in rv.numbers_slices() {
            let (_rs, _rl, cols) = res.unwrap();
            assert_eq!(cols.len(), 3);
        }
        for res in rv.booleans_slices() {
            let (_rs, _rl, cols) = res.unwrap();
            assert_eq!(cols.len(), 3);
        }
        for res in rv.text_slices() {
            let (_rs, _rl, cols) = res.unwrap();
            assert_eq!(cols.len(), 3);
        }
        for res in rv.errors_slices() {
            let (_rs, _rl, cols) = res.unwrap();
            assert_eq!(cols.len(), 3);
        }
        for res in rv.lowered_text_slices() {
            let (_rs, _rl, cols) = res.unwrap();
            assert_eq!(cols.len(), 3);
        }
    }

    #[test]
    fn reversed_range_is_empty() {
        let mut b = IngestBuilder::new("S", 1, 4, crate::engine::DateSystem::Excel1900);
        b.append_row(&[LiteralValue::Number(1.0)]).unwrap();
        b.append_row(&[LiteralValue::Number(2.0)]).unwrap();
        let sheet = b.finish();
        let rv = sheet.range_view(3, 0, 1, 0); // er < sr
        assert_eq!(rv.dims(), (0, 0));
        assert!(rv.iter_row_chunks().next().is_none());
        assert_eq!(rv.get_cell(0, 0), LiteralValue::Empty);
    }

    #[test]
    fn chunk_alignment_invariant() {
        let mut b = IngestBuilder::new("S", 3, 2, crate::engine::DateSystem::Excel1900);
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
        let mut b = IngestBuilder::new("S", 2, 2, crate::engine::DateSystem::Excel1900);
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
        let mut b = IngestBuilder::new("S", 1, 8, crate::engine::DateSystem::Excel1900);
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
        let mut b = IngestBuilder::new("S", 1, 16, crate::engine::DateSystem::Excel1900);
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

    #[test]
    fn row_insert_delete_across_chunk_boundaries_with_overlays() {
        // Build 1 column, chunk size 4, 10 rows -> chunks at [0..4],[4..8],[8..10]
        let mut b = IngestBuilder::new("S", 1, 4, crate::engine::DateSystem::Excel1900);
        for _ in 0..10 {
            b.append_row(&[LiteralValue::Empty]).unwrap();
        }
        let mut sheet = b.finish();
        // Add overlays at row 3 and row 4
        {
            let (c0, o0) = sheet.chunk_of_row(3).unwrap();
            sheet.columns[0].chunks[c0]
                .overlay
                .set(o0, OverlayValue::Number(30.0));
            let (c1, o1) = sheet.chunk_of_row(4).unwrap();
            sheet.columns[0].chunks[c1]
                .overlay
                .set(o1, OverlayValue::Number(40.0));
        }
        // Insert 2 rows before row 4 (at chunk boundary)
        sheet.insert_rows(4, 2);
        assert_eq!(sheet.nrows, 12);
        // Validate overlays moved correctly: 3 stays, 4 becomes Empty, 6 has 40
        let av = sheet.range_view(0, 0, (sheet.nrows - 1) as usize, 0);
        assert_eq!(av.get_cell(3, 0), LiteralValue::Number(30.0));
        assert_eq!(av.get_cell(4, 0), LiteralValue::Empty);
        assert_eq!(av.get_cell(6, 0), LiteralValue::Number(40.0));

        // Now delete 3 rows starting at 3: removes rows 3,4,5 → moves 40.0 from 6 → 3
        sheet.delete_rows(3, 3);
        assert_eq!(sheet.nrows, 9);
        let av2 = sheet.range_view(0, 0, (sheet.nrows - 1) as usize, 0);
        assert_eq!(av2.get_cell(3, 0), LiteralValue::Number(40.0));
        // All columns share chunk lengths; chunk_starts monotonic and cover nrows
        let lens0: Vec<usize> = sheet.columns[0]
            .chunks
            .iter()
            .map(|ch| ch.type_tag.len())
            .collect();
        for col in &sheet.columns {
            let lens: Vec<usize> = col.chunks.iter().map(|ch| ch.type_tag.len()).collect();
            assert_eq!(lens, lens0);
        }
        // chunk_starts should be monotonic and final chunk end == nrows
        assert!(sheet.chunk_starts.windows(2).all(|w| w[0] < w[1]));
        let last_start = *sheet.chunk_starts.last().unwrap_or(&0);
        let last_len = sheet.columns[0]
            .chunks
            .last()
            .map(|c| c.type_tag.len())
            .unwrap_or(0);
        assert_eq!(last_start + last_len, sheet.nrows as usize);
    }

    #[test]
    fn column_insert_delete_retains_chunk_alignment() {
        let mut b = IngestBuilder::new("S", 3, 3, crate::engine::DateSystem::Excel1900);
        for _ in 0..5 {
            b.append_row(&[
                LiteralValue::Empty,
                LiteralValue::Empty,
                LiteralValue::Empty,
            ])
            .unwrap();
        }
        let mut sheet = b.finish();
        // Record reference chunk lengths of first column
        let ref_lens: Vec<usize> = sheet.columns[0]
            .chunks
            .iter()
            .map(|ch| ch.type_tag.len())
            .collect();
        // Insert 2 columns before index 1
        sheet.insert_columns(1, 2);
        assert_eq!(sheet.columns.len(), 5);
        for col in &sheet.columns {
            let lens: Vec<usize> = col.chunks.iter().map(|ch| ch.type_tag.len()).collect();
            assert_eq!(lens, ref_lens);
        }
        let starts_before = sheet.chunk_starts.clone();
        // Delete 2 columns starting at index 2 → back to 3 columns
        sheet.delete_columns(2, 2);
        assert_eq!(sheet.columns.len(), 3);
        for col in &sheet.columns {
            let lens: Vec<usize> = col.chunks.iter().map(|ch| ch.type_tag.len()).collect();
            assert_eq!(lens, ref_lens);
        }
        // chunk_starts unchanged by column operations
        assert_eq!(sheet.chunk_starts, starts_before);
    }

    #[test]
    fn multiple_adjacent_row_ops_overlay_mixed_types() {
        use formualizer_common::ExcelErrorKind;
        // Two columns to ensure alignment preserved across columns
        let mut b = IngestBuilder::new("S", 2, 3, crate::engine::DateSystem::Excel1900);
        for _ in 0..9 {
            b.append_row(&[LiteralValue::Empty, LiteralValue::Empty])
                .unwrap();
        }
        let mut sheet = b.finish();
        // Overlays at rows (0-based): 2->Number, 3->Text, 5->Boolean, 6->Error, 8->Empty
        // Column 0 only
        let set_ov = |sh: &mut ArrowSheet, row: usize, ov: OverlayValue| {
            let (ch_i, off) = sh.chunk_of_row(row).unwrap();
            sh.columns[0].chunks[ch_i].overlay.set(off, ov);
        };
        set_ov(&mut sheet, 2, OverlayValue::Number(12.5));
        set_ov(&mut sheet, 3, OverlayValue::Text(Arc::from("hello")));
        set_ov(&mut sheet, 5, OverlayValue::Boolean(true));
        set_ov(
            &mut sheet,
            6,
            OverlayValue::Error(map_error_code(ExcelErrorKind::Div)),
        );
        set_ov(&mut sheet, 8, OverlayValue::Empty);

        // Insert 1 row before index 3
        sheet.insert_rows(3, 1);
        // Expected new positions: 2->2 (unchanged), 3->4, 5->6, 6->7, 8->9
        let av1 = sheet.range_view(0, 0, (sheet.nrows - 1) as usize, 0);
        assert_eq!(av1.get_cell(2, 0), LiteralValue::Number(12.5));
        assert_eq!(av1.get_cell(4, 0), LiteralValue::Text("hello".into()));
        assert_eq!(av1.get_cell(6, 0), LiteralValue::Boolean(true));
        match av1.get_cell(7, 0) {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Div),
            other => panic!("expected error at row 7, got {other:?}"),
        }
        assert_eq!(av1.get_cell(9, 0), LiteralValue::Empty);

        // Insert 2 rows before index 4 (adjacent to previous region)
        sheet.insert_rows(4, 2);
        // Now positions: 2->2, 4->6, 6->8, 7->9, 9->11
        let av2 = sheet.range_view(0, 0, (sheet.nrows - 1) as usize, 0);
        assert_eq!(av2.get_cell(2, 0), LiteralValue::Number(12.5));
        assert_eq!(av2.get_cell(6, 0), LiteralValue::Text("hello".into()));
        assert_eq!(av2.get_cell(8, 0), LiteralValue::Boolean(true));
        match av2.get_cell(9, 0) {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Div),
            other => panic!("expected error at row 9, got {other:?}"),
        }
        assert_eq!(av2.get_cell(11, 0), LiteralValue::Empty);

        // Delete 2 rows starting at index 6 → removes the text at 6 and one empty row
        sheet.delete_rows(6, 2);
        let av3 = sheet.range_view(0, 0, (sheet.nrows - 1) as usize, 0);
        // Remaining expected: 2->Number 12.5, 6 (was 8)->true, 7 (was 9)->#DIV/0!, 9 (was 11)->Empty
        assert_eq!(av3.get_cell(2, 0), LiteralValue::Number(12.5));
        assert_eq!(av3.get_cell(6, 0), LiteralValue::Boolean(true));
        match av3.get_cell(7, 0) {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Div),
            other => panic!("expected error at row 8, got {other:?}"),
        }
        assert_eq!(av3.get_cell(9, 0), LiteralValue::Empty);

        // Alignment checks
        let lens0: Vec<usize> = sheet.columns[0]
            .chunks
            .iter()
            .map(|ch| ch.type_tag.len())
            .collect();
        for col in &sheet.columns {
            let lens: Vec<usize> = col.chunks.iter().map(|ch| ch.type_tag.len()).collect();
            assert_eq!(lens, lens0);
        }
        // chunk_starts monotonically increasing and cover nrows
        assert!(sheet.chunk_starts.windows(2).all(|w| w[0] < w[1]));
        let last_start = *sheet.chunk_starts.last().unwrap_or(&0);
        let last_len = sheet.columns[0]
            .chunks
            .last()
            .map(|c| c.type_tag.len())
            .unwrap_or(0);
        assert_eq!(last_start + last_len, sheet.nrows as usize);
    }

    #[test]
    fn multiple_adjacent_column_ops_alignment() {
        // Start with 2 columns, chunk_rows=2, rows=5
        let mut b = IngestBuilder::new("S", 2, 2, crate::engine::DateSystem::Excel1900);
        for _ in 0..5 {
            b.append_row(&[LiteralValue::Empty, LiteralValue::Empty])
                .unwrap();
        }
        let mut sheet = b.finish();
        let ref_lens: Vec<usize> = sheet.columns[0]
            .chunks
            .iter()
            .map(|ch| ch.type_tag.len())
            .collect();
        // Insert 1 at start, then 2 at index 2 → columns = 5
        sheet.insert_columns(0, 1);
        sheet.insert_columns(2, 2);
        assert_eq!(sheet.columns.len(), 5);
        for col in &sheet.columns {
            let lens: Vec<usize> = col.chunks.iter().map(|ch| ch.type_tag.len()).collect();
            assert_eq!(lens, ref_lens);
        }
        let starts_before = sheet.chunk_starts.clone();
        // Delete 1 at index 1, then 2 at the end if available
        sheet.delete_columns(1, 1);
        let remain = sheet.columns.len();
        if remain >= 3 {
            sheet.delete_columns(remain - 2, 2);
        }
        for col in &sheet.columns {
            let lens: Vec<usize> = col.chunks.iter().map(|ch| ch.type_tag.len()).collect();
            assert_eq!(lens, ref_lens);
        }
        assert_eq!(sheet.chunk_starts, starts_before);
    }

    #[test]
    fn overlays_on_multiple_columns_row_col_ops() {
        // 3 columns, chunk_rows=3, rows=6 → chunks [0..3), [3..6)
        let mut b = IngestBuilder::new("S", 3, 3, crate::engine::DateSystem::Excel1900);
        for _ in 0..6 {
            b.append_row(&[
                LiteralValue::Empty,
                LiteralValue::Empty,
                LiteralValue::Empty,
            ])
            .unwrap();
        }
        let mut sheet = b.finish();
        // Overlays at row2 and row3 across columns with different types
        let set_ov = |sh: &mut ArrowSheet, col: usize, row: usize, ov: OverlayValue| {
            let (ch_i, off) = sh.chunk_of_row(row).unwrap();
            sh.columns[col].chunks[ch_i].overlay.set(off, ov);
        };
        set_ov(&mut sheet, 0, 2, OverlayValue::Number(12.0));
        set_ov(&mut sheet, 1, 2, OverlayValue::Text(Arc::from("xx")));
        set_ov(&mut sheet, 2, 2, OverlayValue::Boolean(true));
        set_ov(&mut sheet, 0, 3, OverlayValue::Number(33.0));
        set_ov(&mut sheet, 1, 3, OverlayValue::Text(Arc::from("yy")));
        set_ov(&mut sheet, 2, 3, OverlayValue::Boolean(false));

        // Insert a row at boundary (before row index 3)
        sheet.insert_rows(3, 1);
        // Now original row>=3 shift down by 1
        let av = sheet.range_view(0, 0, (sheet.nrows - 1) as usize, 2);
        // Row 2 values unchanged
        assert_eq!(av.get_cell(2, 0), LiteralValue::Number(12.0));
        assert_eq!(av.get_cell(2, 1), LiteralValue::Text("xx".into()));
        assert_eq!(av.get_cell(2, 2), LiteralValue::Boolean(true));
        // Row 3 became Empty (inserted)
        assert_eq!(av.get_cell(3, 0), LiteralValue::Empty);
        // Row 4 holds old row 3 overlays
        assert_eq!(av.get_cell(4, 0), LiteralValue::Number(33.0));
        assert_eq!(av.get_cell(4, 1), LiteralValue::Text("yy".into()));
        assert_eq!(av.get_cell(4, 2), LiteralValue::Boolean(false));

        // Delete column 1 (middle), values shift left
        sheet.delete_columns(1, 1);
        let av2 = sheet.range_view(0, 0, (sheet.nrows - 1) as usize, 1);
        assert_eq!(av2.get_cell(2, 0), LiteralValue::Number(12.0));
        // Column 1 now was old column 2
        assert_eq!(av2.get_cell(2, 1), LiteralValue::Boolean(true));
        assert_eq!(av2.get_cell(4, 0), LiteralValue::Number(33.0));
        assert_eq!(av2.get_cell(4, 1), LiteralValue::Boolean(false));

        // Alignment preserved
        let lens0: Vec<usize> = sheet.columns[0]
            .chunks
            .iter()
            .map(|ch| ch.type_tag.len())
            .collect();
        for col in &sheet.columns {
            let lens: Vec<usize> = col.chunks.iter().map(|ch| ch.type_tag.len()).collect();
            assert_eq!(lens, lens0);
        }
    }

    #[test]
    fn effective_slices_overlay_precedence_numbers_text() {
        // 1 column, chunk_rows=3, rows=6. Base numbers in lane; overlays include text on row1 and number on row4.
        let mut b = IngestBuilder::new("S", 1, 3, crate::engine::DateSystem::Excel1900);
        for i in 0..6 {
            b.append_row(&[LiteralValue::Number((i + 1) as f64)])
                .unwrap();
        }
        let mut sheet = b.finish();
        // Overlays: row1 -> Text("X"), row4 -> Number(99)
        let (c1, o1) = sheet.chunk_of_row(1).unwrap();
        sheet.columns[0].chunks[c1]
            .overlay
            .set(o1, OverlayValue::Text(Arc::from("X")));
        let (c4, o4) = sheet.chunk_of_row(4).unwrap();
        sheet.columns[0].chunks[c4]
            .overlay
            .set(o4, OverlayValue::Number(99.0));

        let av = sheet.range_view(0, 0, 5, 0);
        // Validate numbers_slices: row1 should be null (text overlay), row4 should be 99.0, others base
        let mut numeric: Vec<Option<f64>> = vec![None; 6];
        for res in av.numbers_slices() {
            let (row_start, row_len, cols) = res.unwrap();
            let a = &cols[0];
            for i in 0..row_len {
                let idx = row_start + i;
                numeric[idx] = if a.is_null(i) { None } else { Some(a.value(i)) };
            }
        }
        assert_eq!(numeric[0], Some(1.0));
        assert_eq!(numeric[1], None); // overshadowed by text overlay
        assert_eq!(numeric[2], Some(3.0));
        assert_eq!(numeric[3], Some(4.0));
        assert_eq!(numeric[4], Some(99.0));
        assert_eq!(numeric[5], Some(6.0));

        // Validate text_slices: row1 has "X", others null
        let mut texts: Vec<Option<String>> = vec![None; 6];
        for res in av.text_slices() {
            let (row_start, row_len, cols) = res.unwrap();
            let a = cols[0].as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..row_len {
                let idx = row_start + i;
                texts[idx] = if a.is_null(i) {
                    None
                } else {
                    Some(a.value(i).to_string())
                };
            }
        }
        assert_eq!(texts[1].as_deref(), Some("X"));
        assert!(texts[0].is_none());
        assert!(texts[2].is_none());
        assert!(texts[3].is_none());
        assert!(texts[4].is_none());
        assert!(texts[5].is_none());
    }

    #[test]
    fn effective_slices_overlay_precedence_booleans() {
        // Base booleans over 1 column; overlays include boolean and non-boolean types.
        let mut b = IngestBuilder::new("S", 1, 4, crate::engine::DateSystem::Excel1900);
        for i in 0..6 {
            let v = if i % 2 == 0 {
                LiteralValue::Boolean(true)
            } else {
                LiteralValue::Boolean(false)
            };
            b.append_row(&[v]).unwrap();
        }
        let mut sheet = b.finish();
        // Overlays: row1 -> Boolean(true), row2 -> Text("T")
        let (c1, o1) = sheet.chunk_of_row(1).unwrap();
        sheet.columns[0].chunks[c1]
            .overlay
            .set(o1, OverlayValue::Boolean(true));
        let (c2, o2) = sheet.chunk_of_row(2).unwrap();
        sheet.columns[0].chunks[c2]
            .overlay
            .set(o2, OverlayValue::Text(Arc::from("T")));

        let av = sheet.range_view(0, 0, 5, 0);
        // Validate booleans_slices: row1 should be true (overlay), row2 should be null (text overlay), others base
        let mut bools: Vec<Option<bool>> = vec![None; 6];
        for res in av.booleans_slices() {
            let (row_start, row_len, cols) = res.unwrap();
            let a = &cols[0];
            for i in 0..row_len {
                let idx = row_start + i;
                bools[idx] = if a.is_null(i) { None } else { Some(a.value(i)) };
            }
        }
        assert_eq!(bools[0], Some(true));
        assert_eq!(bools[1], Some(true)); // overlay to true
        assert_eq!(bools[2], None); // overshadowed by text overlay
        // spot-check others remain base
        assert_eq!(bools[3], Some(false));
    }

    #[test]
    fn effective_slices_overlay_precedence_errors() {
        // Base numbers; overlay an error at one row and ensure errors_slices reflect it.
        let mut b = IngestBuilder::new("S", 1, 3, crate::engine::DateSystem::Excel1900);
        for i in 0..6 {
            b.append_row(&[LiteralValue::Number((i + 1) as f64)])
                .unwrap();
        }
        let mut sheet = b.finish();
        // Overlay error at row 4
        let (c4, o4) = sheet.chunk_of_row(4).unwrap();
        sheet.columns[0].chunks[c4]
            .overlay
            .set(o4, OverlayValue::Error(map_error_code(ExcelErrorKind::Div)));

        let av = sheet.range_view(0, 0, 5, 0);
        let mut errs: Vec<Option<u8>> = vec![None; 6];
        for res in av.errors_slices() {
            let (row_start, row_len, cols) = res.unwrap();
            let a = &cols[0];
            for i in 0..row_len {
                let idx = row_start + i;
                errs[idx] = if a.is_null(i) { None } else { Some(a.value(i)) };
            }
        }
        assert_eq!(errs[4], Some(map_error_code(ExcelErrorKind::Div)));
        assert!(errs[3].is_none());
    }
}
