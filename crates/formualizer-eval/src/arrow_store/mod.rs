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
use std::collections::{BTreeMap, HashMap};

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
    // Cache: lowered text lane, nulls preserved
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

    /// Lowercased text lane, with nulls preserved. Cached per chunk.
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
                    b.append_value(sa.value(i).to_lowercase());
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

    /// Grow this chunk's logical length to `new_len` (padding with empty/null values).
    ///
    /// This is used to keep already-materialized chunks consistent when `ArrowSheet::nrows`
    /// grows incrementally inside the current last chunk.
    pub fn grow_len_to(&mut self, new_len: usize) {
        let old_len = self.len();
        if new_len <= old_len {
            return;
        }

        // Grow type tags (pad with Empty).
        let mut tags: Vec<u8> = self.type_tag.values().to_vec();
        tags.resize(new_len, TypeTag::Empty as u8);
        self.type_tag = Arc::new(UInt8Array::from(tags));

        // Grow lanes when present; append nulls for new rows.
        if let Some(a) = &self.numbers {
            use arrow_array::builder::Float64Builder;
            let mut b = Float64Builder::with_capacity(new_len);
            for i in 0..old_len {
                if a.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(a.value(i));
                }
            }
            for _ in old_len..new_len {
                b.append_null();
            }
            self.numbers = Some(Arc::new(b.finish()));
        }
        if let Some(a) = &self.booleans {
            use arrow_array::builder::BooleanBuilder;
            let mut b = BooleanBuilder::with_capacity(new_len);
            for i in 0..old_len {
                if a.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(a.value(i));
                }
            }
            for _ in old_len..new_len {
                b.append_null();
            }
            self.booleans = Some(Arc::new(b.finish()));
        }
        if let Some(a) = &self.errors {
            use arrow_array::builder::UInt8Builder;
            let mut b = UInt8Builder::with_capacity(new_len);
            for i in 0..old_len {
                if a.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(a.value(i));
                }
            }
            for _ in old_len..new_len {
                b.append_null();
            }
            self.errors = Some(Arc::new(b.finish()));
        }
        if let Some(a) = &self.text {
            use arrow_array::builder::StringBuilder;
            let sa = a.as_any().downcast_ref::<StringArray>().unwrap();
            let mut b = StringBuilder::with_capacity(new_len, 0);
            for i in 0..old_len {
                if sa.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(sa.value(i));
                }
            }
            for _ in old_len..new_len {
                b.append_null();
            }
            self.text = Some(Arc::new(b.finish()) as ArrayRef);
        }

        // Length-dependent caches must be dropped.
        self.lazy_null_numbers = OnceCell::new();
        self.lazy_null_booleans = OnceCell::new();
        self.lazy_null_text = OnceCell::new();
        self.lazy_null_errors = OnceCell::new();
        self.lowered_text = OnceCell::new();

        self.meta.len = new_len;
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

#[derive(Debug, Clone, PartialEq)]
pub enum OverlayValue {
    Empty,
    Number(f64),
    /// Date/Time/DateTime stored as an Excel serial in the numeric lane.
    DateTime(f64),
    /// Duration stored as an Excel-style day-fraction in the numeric lane.
    Duration(f64),
    Boolean(bool),
    Text(Arc<str>),
    Error(u8),
    Pending,
}

impl OverlayValue {
    #[inline]
    pub(crate) fn estimated_payload_bytes(&self) -> usize {
        match self {
            OverlayValue::Empty | OverlayValue::Pending => 0,
            OverlayValue::Number(_) | OverlayValue::DateTime(_) | OverlayValue::Duration(_) => {
                core::mem::size_of::<f64>()
            }
            OverlayValue::Boolean(_) => core::mem::size_of::<bool>(),
            OverlayValue::Error(_) => core::mem::size_of::<u8>(),
            // Deterministic estimate: count string bytes only.
            OverlayValue::Text(s) => s.len(),
        }
    }

    #[inline]
    pub(crate) fn type_tag(&self) -> TypeTag {
        match self {
            OverlayValue::Empty => TypeTag::Empty,
            OverlayValue::Number(_) => TypeTag::Number,
            OverlayValue::DateTime(_) => TypeTag::DateTime,
            OverlayValue::Duration(_) => TypeTag::Duration,
            OverlayValue::Boolean(_) => TypeTag::Boolean,
            OverlayValue::Text(_) => TypeTag::Text,
            OverlayValue::Error(_) => TypeTag::Error,
            OverlayValue::Pending => TypeTag::Pending,
        }
    }

    #[inline]
    pub(crate) fn numeric_lane_value(&self) -> Option<f64> {
        match self {
            OverlayValue::Number(n) | OverlayValue::DateTime(n) | OverlayValue::Duration(n) => {
                Some(*n)
            }
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn boolean_lane_value(&self) -> Option<bool> {
        match self {
            OverlayValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn text_lane_value(&self) -> Option<&str> {
        match self {
            OverlayValue::Text(s) => Some(s.as_ref()),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn error_lane_value(&self) -> Option<u8> {
        match self {
            OverlayValue::Error(code) => Some(*code),
            _ => None,
        }
    }

    pub(crate) fn lowered_text_value(&self) -> Option<String> {
        match self {
            OverlayValue::Text(s) => Some(s.to_lowercase()),
            OverlayValue::Number(n) | OverlayValue::DateTime(n) | OverlayValue::Duration(n) => {
                Some(n.to_string())
            }
            OverlayValue::Boolean(b) => Some(if *b { "true" } else { "false" }.to_string()),
            OverlayValue::Empty | OverlayValue::Error(_) | OverlayValue::Pending => None,
        }
    }

    pub(crate) fn to_literal(&self) -> LiteralValue {
        match self {
            OverlayValue::Empty => LiteralValue::Empty,
            OverlayValue::Number(n) => LiteralValue::Number(*n),
            OverlayValue::DateTime(serial) => LiteralValue::from_serial_number(*serial),
            OverlayValue::Duration(serial) => {
                let nanos_f = *serial * 86_400.0 * 1_000_000_000.0;
                let nanos = nanos_f.round().clamp(i64::MIN as f64, i64::MAX as f64) as i64;
                LiteralValue::Duration(chrono::Duration::nanoseconds(nanos))
            }
            OverlayValue::Boolean(b) => LiteralValue::Boolean(*b),
            OverlayValue::Text(s) => LiteralValue::Text((**s).to_string()),
            OverlayValue::Error(code) => {
                LiteralValue::Error(ExcelError::new(unmap_error_code(*code)))
            }
            OverlayValue::Pending => LiteralValue::Pending,
        }
    }
}

const OVERLAY_ENTRY_BASE_BYTES: usize = 32;
const OVERLAY_FRAGMENT_BASE_BYTES: usize = 48;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct OverlayFragmentPayload {
    values: Arc<[OverlayValue]>,
    type_tags: Arc<UInt8Array>,
    numbers: Option<Arc<Float64Array>>,
    booleans: Option<Arc<BooleanArray>>,
    text: Option<ArrayRef>,
    errors: Option<Arc<UInt8Array>>,
    estimated_bytes: usize,
}

impl OverlayFragmentPayload {
    fn from_values(values: Vec<OverlayValue>) -> Self {
        let len = values.len();
        let mut tag_b = UInt8Builder::with_capacity(len);
        let mut nb = Float64Builder::with_capacity(len);
        let mut bb = BooleanBuilder::with_capacity(len);
        let mut sb = StringBuilder::with_capacity(len, len.saturating_mul(8));
        let mut eb = UInt8Builder::with_capacity(len);
        let mut non_num = 0usize;
        let mut non_bool = 0usize;
        let mut non_text = 0usize;
        let mut non_err = 0usize;

        for value in &values {
            append_overlay_value_to_lane_builders(
                value,
                &mut tag_b,
                &mut nb,
                &mut bb,
                &mut sb,
                &mut eb,
                &mut non_num,
                &mut non_bool,
                &mut non_text,
                &mut non_err,
            );
        }

        let payload_bytes = values
            .iter()
            .map(|value| value.estimated_payload_bytes())
            .fold(0usize, usize::saturating_add);
        let estimated_bytes = len
            .saturating_mul(OVERLAY_ENTRY_BASE_BYTES)
            .saturating_add(payload_bytes);

        Self {
            values: Arc::from(values.into_boxed_slice()),
            type_tags: Arc::new(tag_b.finish()),
            numbers: {
                let a = nb.finish();
                (non_num > 0).then(|| Arc::new(a))
            },
            booleans: {
                let a = bb.finish();
                (non_bool > 0).then(|| Arc::new(a))
            },
            text: {
                let a = sb.finish();
                (non_text > 0).then(|| Arc::new(a) as ArrayRef)
            },
            errors: {
                let a = eb.finish();
                (non_err > 0).then(|| Arc::new(a))
            },
            estimated_bytes,
        }
    }

    #[inline]
    fn get(&self, idx: usize) -> Option<&OverlayValue> {
        self.values.get(idx)
    }

    #[inline]
    fn values_slice(&self, start: usize, len: usize) -> Vec<OverlayValue> {
        self.values[start..start.saturating_add(len)].to_vec()
    }

    #[inline]
    fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }
}

#[derive(Debug, Clone)]
pub(crate) enum OverlayFragment {
    SparseOffsets {
        offsets: Vec<u32>,
        payload: OverlayFragmentPayload,
    },
    DenseRange {
        start: u32,
        len: u32,
        payload: OverlayFragmentPayload,
    },
    RunRange {
        start: u32,
        len: u32,
        run_ends: Vec<u32>,
        payload: OverlayFragmentPayload,
    },
}

impl OverlayFragment {
    const MAX_SPLIT_SEGMENTS_BEFORE_SPARSE_FALLBACK: usize = 128;

    pub(crate) fn sparse_offsets(items: Vec<(usize, OverlayValue)>) -> Option<Self> {
        let mut by_offset: BTreeMap<usize, OverlayValue> = BTreeMap::new();
        for (offset, value) in items {
            by_offset.insert(offset, value);
        }
        if by_offset.is_empty() {
            return None;
        }

        let mut offsets = Vec::with_capacity(by_offset.len());
        let mut values = Vec::with_capacity(by_offset.len());
        for (offset, value) in by_offset {
            offsets.push(u32::try_from(offset).expect("overlay offset fits in u32"));
            values.push(value);
        }

        Some(Self::SparseOffsets {
            offsets,
            payload: OverlayFragmentPayload::from_values(values),
        })
    }

    pub(crate) fn dense_range(start: usize, values: Vec<OverlayValue>) -> Option<Self> {
        let len = values.len();
        if len == 0 {
            return None;
        }
        Some(Self::DenseRange {
            start: u32::try_from(start).expect("overlay start fits in u32"),
            len: u32::try_from(len).expect("overlay length fits in u32"),
            payload: OverlayFragmentPayload::from_values(values),
        })
    }

    pub(crate) fn run_range(start: usize, values: Vec<OverlayValue>) -> Option<Self> {
        if values.is_empty() {
            return None;
        }

        let mut run_ends = Vec::new();
        let mut run_values = Vec::new();
        let mut current = values[0].clone();
        for (idx, value) in values.iter().enumerate().skip(1) {
            if *value != current {
                run_ends.push(idx);
                run_values.push(current);
                current = value.clone();
            }
        }
        run_ends.push(values.len());
        run_values.push(current);

        Self::run_range_from_parts(start, values.len(), run_ends, run_values)
    }

    fn run_range_from_parts(
        start: usize,
        len: usize,
        run_ends: Vec<usize>,
        values: Vec<OverlayValue>,
    ) -> Option<Self> {
        if len == 0 || run_ends.is_empty() || run_ends.len() != values.len() {
            return None;
        }

        let mut merged_ends: Vec<u32> = Vec::with_capacity(run_ends.len());
        let mut merged_values: Vec<OverlayValue> = Vec::with_capacity(values.len());
        let mut prev_end = 0usize;
        for (end, value) in run_ends.into_iter().zip(values.into_iter()) {
            if end <= prev_end || end > len {
                return None;
            }
            if merged_values.last().is_some_and(|last| *last == value) {
                if let Some(last_end) = merged_ends.last_mut() {
                    *last_end = u32::try_from(end).expect("run end fits in u32");
                }
            } else {
                merged_ends.push(u32::try_from(end).expect("run end fits in u32"));
                merged_values.push(value);
            }
            prev_end = end;
        }

        if prev_end != len || merged_ends.last().copied() != Some(len as u32) {
            return None;
        }

        Some(Self::RunRange {
            start: u32::try_from(start).expect("overlay start fits in u32"),
            len: u32::try_from(len).expect("overlay length fits in u32"),
            run_ends: merged_ends,
            payload: OverlayFragmentPayload::from_values(merged_values),
        })
    }

    #[inline]
    fn estimated_bytes(&self) -> usize {
        match self {
            OverlayFragment::SparseOffsets { offsets, payload } => OVERLAY_FRAGMENT_BASE_BYTES
                .saturating_add(offsets.len().saturating_mul(core::mem::size_of::<u32>()))
                .saturating_add(payload.estimated_bytes()),
            OverlayFragment::DenseRange { payload, .. } => {
                OVERLAY_FRAGMENT_BASE_BYTES.saturating_add(payload.estimated_bytes())
            }
            OverlayFragment::RunRange {
                run_ends, payload, ..
            } => OVERLAY_FRAGMENT_BASE_BYTES
                .saturating_add(run_ends.len().saturating_mul(core::mem::size_of::<u32>()))
                .saturating_add(payload.estimated_bytes()),
        }
    }

    #[inline]
    fn coverage_len(&self) -> usize {
        match self {
            OverlayFragment::SparseOffsets { offsets, .. } => offsets.len(),
            OverlayFragment::DenseRange { len, .. } | OverlayFragment::RunRange { len, .. } => {
                *len as usize
            }
        }
    }

    fn interval_coverage(&self) -> Option<core::ops::Range<usize>> {
        match self {
            OverlayFragment::DenseRange { start, len, .. }
            | OverlayFragment::RunRange { start, len, .. } => {
                let start = *start as usize;
                Some(start..start.saturating_add(*len as usize))
            }
            OverlayFragment::SparseOffsets { .. } => None,
        }
    }

    fn sparse_offsets_slice(&self) -> Option<&[u32]> {
        match self {
            OverlayFragment::SparseOffsets { offsets, .. } => Some(offsets.as_slice()),
            _ => None,
        }
    }

    fn has_any_in_range(&self, range: core::ops::Range<usize>) -> bool {
        if range.is_empty() {
            return false;
        }
        match self {
            OverlayFragment::SparseOffsets { offsets, .. } => {
                let start = u32::try_from(range.start).unwrap_or(u32::MAX);
                let idx = offsets.partition_point(|off| *off < start);
                offsets
                    .get(idx)
                    .is_some_and(|off| (*off as usize) < range.end)
            }
            OverlayFragment::DenseRange { .. } | OverlayFragment::RunRange { .. } => self
                .interval_coverage()
                .is_some_and(|r| r.start < range.end && range.start < r.end),
        }
    }

    fn intersects_fragment_exact(&self, replacement: &OverlayFragment) -> bool {
        if let Some(offsets) = replacement.sparse_offsets_slice() {
            self.intersects_sparse_offsets(offsets)
        } else if let Some(range) = replacement.interval_coverage() {
            self.intersects_interval(range)
        } else {
            false
        }
    }

    fn intersects_interval(&self, range: core::ops::Range<usize>) -> bool {
        if range.is_empty() {
            return false;
        }
        match self {
            OverlayFragment::SparseOffsets { offsets, .. } => {
                let start = u32::try_from(range.start).unwrap_or(u32::MAX);
                let idx = offsets.partition_point(|off| *off < start);
                offsets
                    .get(idx)
                    .is_some_and(|off| (*off as usize) < range.end)
            }
            OverlayFragment::DenseRange { .. } | OverlayFragment::RunRange { .. } => self
                .interval_coverage()
                .is_some_and(|own| own.start < range.end && range.start < own.end),
        }
    }

    fn intersects_sparse_offsets(&self, replacement_offsets: &[u32]) -> bool {
        if replacement_offsets.is_empty() {
            return false;
        }
        match self {
            OverlayFragment::SparseOffsets { offsets, .. } => {
                Self::sorted_offsets_intersect(offsets, replacement_offsets)
            }
            OverlayFragment::DenseRange { .. } | OverlayFragment::RunRange { .. } => {
                self.interval_coverage().is_some_and(|range| {
                    let start = u32::try_from(range.start).unwrap_or(u32::MAX);
                    let idx = replacement_offsets.partition_point(|off| *off < start);
                    replacement_offsets
                        .get(idx)
                        .is_some_and(|off| (*off as usize) < range.end)
                })
            }
        }
    }

    fn sorted_offsets_intersect(a: &[u32], b: &[u32]) -> bool {
        let mut ai = 0usize;
        let mut bi = 0usize;
        while ai < a.len() && bi < b.len() {
            match a[ai].cmp(&b[bi]) {
                core::cmp::Ordering::Equal => return true,
                core::cmp::Ordering::Less => ai += 1,
                core::cmp::Ordering::Greater => bi += 1,
            }
        }
        false
    }

    fn covers_offset(&self, off: usize) -> bool {
        self.get_scalar(off).is_some()
    }

    fn get_scalar(&self, off: usize) -> Option<&OverlayValue> {
        match self {
            OverlayFragment::SparseOffsets { offsets, payload } => {
                let off = u32::try_from(off).ok()?;
                let idx = offsets.binary_search(&off).ok()?;
                payload.get(idx)
            }
            OverlayFragment::DenseRange {
                start,
                len,
                payload,
            } => {
                let start = *start as usize;
                let rel = off.checked_sub(start)?;
                if rel >= *len as usize {
                    return None;
                }
                payload.get(rel)
            }
            OverlayFragment::RunRange {
                start,
                len,
                run_ends,
                payload,
            } => {
                let start = *start as usize;
                let rel = off.checked_sub(start)?;
                if rel >= *len as usize {
                    return None;
                }
                let rel_u32 = u32::try_from(rel).ok()?;
                let run_idx = run_ends.partition_point(|end| *end <= rel_u32);
                payload.get(run_idx)
            }
        }
    }

    fn subtract_fragment(&self, replacement: &OverlayFragment) -> Vec<OverlayFragment> {
        if let Some(offsets) = replacement.sparse_offsets_slice() {
            self.subtract_sparse_offsets(offsets)
        } else if let Some(range) = replacement.interval_coverage() {
            self.subtract_interval(range)
        } else {
            vec![self.clone()]
        }
    }

    fn subtract_offset(&self, off: usize) -> Vec<OverlayFragment> {
        match self {
            OverlayFragment::SparseOffsets { .. } => {
                let Ok(off) = u32::try_from(off) else {
                    return vec![self.clone()];
                };
                self.subtract_sparse_offsets(core::slice::from_ref(&off))
            }
            OverlayFragment::DenseRange { .. } | OverlayFragment::RunRange { .. } => {
                self.subtract_interval(off..off.saturating_add(1))
            }
        }
    }

    fn subtract_interval(&self, replacement: core::ops::Range<usize>) -> Vec<OverlayFragment> {
        if replacement.is_empty() {
            return vec![self.clone()];
        }

        match self {
            OverlayFragment::SparseOffsets { offsets, payload } => {
                let cells: Vec<_> = offsets
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, off)| {
                        let off_usize = *off as usize;
                        (!replacement.contains(&off_usize))
                            .then(|| payload.get(idx).cloned().map(|value| (off_usize, value)))?
                    })
                    .collect();
                OverlayFragment::sparse_offsets(cells).into_iter().collect()
            }
            OverlayFragment::DenseRange { .. } => {
                let Some(own) = self.interval_coverage() else {
                    return vec![self.clone()];
                };
                if own.end <= replacement.start || replacement.end <= own.start {
                    return vec![self.clone()];
                }
                let cut_start = replacement.start.max(own.start);
                let cut_end = replacement.end.min(own.end);
                let mut out = Vec::with_capacity(2);
                if own.start < cut_start
                    && let Some(left) =
                        self.dense_segment_with_start(own.start, own.start, cut_start)
                {
                    out.push(left);
                }
                if cut_end < own.end
                    && let Some(right) = self.dense_segment_with_start(cut_end, cut_end, own.end)
                {
                    out.push(right);
                }
                out
            }
            OverlayFragment::RunRange { .. } => {
                let Some(own) = self.interval_coverage() else {
                    return vec![self.clone()];
                };
                if own.end <= replacement.start || replacement.end <= own.start {
                    return vec![self.clone()];
                }
                let cut_start = replacement.start.max(own.start);
                let cut_end = replacement.end.min(own.end);
                let mut out = Vec::with_capacity(2);
                if own.start < cut_start
                    && let Some(left) = self.run_segment_with_start(own.start, own.start, cut_start)
                {
                    out.push(left);
                }
                if cut_end < own.end
                    && let Some(right) = self.run_segment_with_start(cut_end, cut_end, own.end)
                {
                    out.push(right);
                }
                out
            }
        }
    }

    fn subtract_sparse_offsets(&self, replacement_offsets: &[u32]) -> Vec<OverlayFragment> {
        if replacement_offsets.is_empty() {
            return vec![self.clone()];
        }

        match self {
            OverlayFragment::SparseOffsets { offsets, payload } => {
                let cells: Vec<_> = offsets
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, off)| {
                        replacement_offsets.binary_search(off).is_err().then(|| {
                            payload
                                .get(idx)
                                .cloned()
                                .map(|value| (*off as usize, value))
                        })?
                    })
                    .collect();
                OverlayFragment::sparse_offsets(cells).into_iter().collect()
            }
            OverlayFragment::DenseRange { .. } => {
                self.subtract_sparse_offsets_from_dense(replacement_offsets)
            }
            OverlayFragment::RunRange { .. } => {
                self.subtract_sparse_offsets_from_run(replacement_offsets)
            }
        }
    }

    fn sparse_holes_in_interval(offsets: &[u32], range: core::ops::Range<usize>) -> Vec<usize> {
        if range.is_empty() {
            return Vec::new();
        }
        let start = u32::try_from(range.start).unwrap_or(u32::MAX);
        let mut idx = offsets.partition_point(|off| *off < start);
        let mut holes = Vec::new();
        let mut last = None;
        while let Some(off) = offsets.get(idx).copied() {
            let off_usize = off as usize;
            if off_usize >= range.end {
                break;
            }
            if last != Some(off_usize) {
                holes.push(off_usize);
                last = Some(off_usize);
            }
            idx += 1;
        }
        holes
    }

    fn subtract_sparse_offsets_from_dense(
        &self,
        replacement_offsets: &[u32],
    ) -> Vec<OverlayFragment> {
        let Some(own) = self.interval_coverage() else {
            return vec![self.clone()];
        };
        let holes = Self::sparse_holes_in_interval(replacement_offsets, own.clone());
        if holes.is_empty() {
            return vec![self.clone()];
        }
        if holes.len().saturating_add(1) > Self::MAX_SPLIT_SEGMENTS_BEFORE_SPARSE_FALLBACK {
            return self.sparse_remainder_excluding_offsets(&holes);
        }

        let mut out = Vec::with_capacity(holes.len().saturating_add(1));
        let mut seg_start = own.start;
        for hole in holes {
            if seg_start < hole
                && let Some(segment) = self.dense_segment_with_start(seg_start, seg_start, hole)
            {
                out.push(segment);
            }
            seg_start = hole.saturating_add(1);
        }
        if seg_start < own.end
            && let Some(segment) = self.dense_segment_with_start(seg_start, seg_start, own.end)
        {
            out.push(segment);
        }
        out
    }

    fn subtract_sparse_offsets_from_run(
        &self,
        replacement_offsets: &[u32],
    ) -> Vec<OverlayFragment> {
        let Some(own) = self.interval_coverage() else {
            return vec![self.clone()];
        };
        let holes = Self::sparse_holes_in_interval(replacement_offsets, own.clone());
        if holes.is_empty() {
            return vec![self.clone()];
        }
        if holes.len().saturating_add(1) > Self::MAX_SPLIT_SEGMENTS_BEFORE_SPARSE_FALLBACK {
            return self.sparse_remainder_excluding_offsets(&holes);
        }

        let mut out = Vec::with_capacity(holes.len().saturating_add(1));
        let mut seg_start = own.start;
        for hole in holes {
            if seg_start < hole
                && let Some(segment) = self.run_segment_with_start(seg_start, seg_start, hole)
            {
                out.push(segment);
            }
            seg_start = hole.saturating_add(1);
        }
        if seg_start < own.end
            && let Some(segment) = self.run_segment_with_start(seg_start, seg_start, own.end)
        {
            out.push(segment);
        }
        out
    }

    fn sparse_remainder_excluding_offsets(&self, sorted_holes: &[usize]) -> Vec<OverlayFragment> {
        let cells: Vec<_> = self
            .cells()
            .into_iter()
            .filter(|(off, _)| sorted_holes.binary_search(off).is_err())
            .collect();
        OverlayFragment::sparse_offsets(cells).into_iter().collect()
    }

    fn dense_segment_with_start(
        &self,
        new_start: usize,
        abs_start: usize,
        abs_end: usize,
    ) -> Option<OverlayFragment> {
        match self {
            OverlayFragment::DenseRange { start, payload, .. } => {
                if abs_start >= abs_end {
                    return None;
                }
                let base = *start as usize;
                let rel_start = abs_start.checked_sub(base)?;
                let len = abs_end.saturating_sub(abs_start);
                OverlayFragment::dense_range(new_start, payload.values_slice(rel_start, len))
            }
            _ => None,
        }
    }

    fn run_segment_with_start(
        &self,
        new_start: usize,
        abs_start: usize,
        abs_end: usize,
    ) -> Option<OverlayFragment> {
        let OverlayFragment::RunRange {
            start,
            len,
            run_ends,
            payload,
        } = self
        else {
            return None;
        };
        if abs_start >= abs_end {
            return None;
        }
        let base = *start as usize;
        let frag_end = base.saturating_add(*len as usize);
        if abs_start < base || abs_end > frag_end {
            return None;
        }

        let rel_start = abs_start - base;
        let rel_end = abs_end - base;
        let mut new_run_ends = Vec::new();
        let mut new_values = Vec::new();
        let mut prev_end = 0usize;

        for (run_idx, end) in run_ends.iter().enumerate() {
            let run_start = prev_end;
            let run_end = *end as usize;
            let inter_start = run_start.max(rel_start);
            let inter_end = run_end.min(rel_end);
            if inter_start < inter_end {
                new_run_ends.push(inter_end - rel_start);
                if let Some(value) = payload.get(run_idx).cloned() {
                    new_values.push(value);
                }
            }
            prev_end = run_end;
            if prev_end >= rel_end {
                break;
            }
        }

        OverlayFragment::run_range_from_parts(
            new_start,
            abs_end.saturating_sub(abs_start),
            new_run_ends,
            new_values,
        )
    }

    fn cells(&self) -> Vec<(usize, OverlayValue)> {
        match self {
            OverlayFragment::SparseOffsets { offsets, payload } => offsets
                .iter()
                .enumerate()
                .filter_map(|(idx, off)| {
                    payload
                        .get(idx)
                        .cloned()
                        .map(|value| (*off as usize, value))
                })
                .collect(),
            OverlayFragment::DenseRange {
                start,
                len,
                payload,
            } => {
                let start = *start as usize;
                (0..*len as usize)
                    .filter_map(|idx| {
                        payload
                            .get(idx)
                            .cloned()
                            .map(|value| (start.saturating_add(idx), value))
                    })
                    .collect()
            }
            OverlayFragment::RunRange { start, len, .. } => {
                let start = *start as usize;
                (0..*len as usize)
                    .filter_map(|idx| {
                        self.get_scalar(start.saturating_add(idx))
                            .cloned()
                            .map(|value| (start.saturating_add(idx), value))
                    })
                    .collect()
            }
        }
    }

    fn slice(&self, off: usize, len: usize) -> Option<OverlayFragment> {
        let end = off.saturating_add(len);
        if len == 0 {
            return None;
        }

        match self {
            OverlayFragment::SparseOffsets { offsets, payload } => {
                let start = u32::try_from(off).unwrap_or(u32::MAX);
                let lo = offsets.partition_point(|candidate| *candidate < start);
                let hi = offsets.partition_point(|candidate| (*candidate as usize) < end);
                let cells: Vec<_> = (lo..hi)
                    .filter_map(|idx| {
                        let rebased = (offsets[idx] as usize).saturating_sub(off);
                        payload.get(idx).cloned().map(|value| (rebased, value))
                    })
                    .collect();
                OverlayFragment::sparse_offsets(cells)
            }
            OverlayFragment::DenseRange { .. } => {
                let own = self.interval_coverage()?;
                let seg_start = own.start.max(off);
                let seg_end = own.end.min(end);
                if seg_start >= seg_end {
                    return None;
                }
                self.dense_segment_with_start(seg_start - off, seg_start, seg_end)
            }
            OverlayFragment::RunRange { .. } => {
                let own = self.interval_coverage()?;
                let seg_start = own.start.max(off);
                let seg_end = own.end.min(end);
                if seg_start >= seg_end {
                    return None;
                }
                self.run_segment_with_start(seg_start - off, seg_start, seg_end)
            }
        }
    }
}
#[derive(Debug, Default, Clone)]
pub struct Overlay {
    points: HashMap<usize, OverlayValue>,
    fragments: Vec<OverlayFragment>,
    // Deterministic (and intentionally approximate) accounting of overlay memory.
    // This is used for budget enforcement/observability; it does not attempt to reflect
    // the allocator's exact overhead.
    estimated_bytes: usize,
}

impl Overlay {
    // Deterministic estimate per entry to keep budget enforcement stable across platforms.
    // Includes key + map/node overhead (approx) and value payload bytes.
    const ENTRY_BASE_BYTES: usize = OVERLAY_ENTRY_BASE_BYTES;

    pub fn new() -> Self {
        Self {
            points: HashMap::new(),
            fragments: Vec::new(),
            estimated_bytes: 0,
        }
    }

    #[inline]
    fn point_estimate(v: &OverlayValue) -> usize {
        Self::ENTRY_BASE_BYTES + v.estimated_payload_bytes()
    }

    #[inline]
    fn adjust_estimated_bytes(&mut self, delta: isize) {
        if delta >= 0 {
            self.estimated_bytes = self.estimated_bytes.saturating_add(delta as usize);
        } else {
            self.estimated_bytes = self.estimated_bytes.saturating_sub((-delta) as usize);
        }
    }

    #[inline]
    pub(crate) fn get_scalar(&self, off: usize) -> Option<&OverlayValue> {
        self.points
            .get(&off)
            .or_else(|| self.fragments.iter().rev().find_map(|f| f.get_scalar(off)))
    }

    #[inline]
    pub fn get(&self, off: usize) -> Option<&OverlayValue> {
        self.get_scalar(off)
    }

    #[inline]
    pub(crate) fn set_scalar(&mut self, off: usize, v: OverlayValue) -> isize {
        let removed = self.remove_scalar(off);
        let new_est = Self::point_estimate(&v);
        self.points.insert(off, v);
        self.adjust_estimated_bytes(new_est as isize);
        removed.saturating_add(new_est as isize)
    }

    #[inline]
    pub fn set(&mut self, off: usize, v: OverlayValue) -> isize {
        self.set_scalar(off, v)
    }

    pub(crate) fn apply_fragment(&mut self, fragment: OverlayFragment) -> isize {
        let mut delta = self.remove_points_covered_by_fragment(&fragment);
        delta = delta.saturating_add(self.remove_fragments_covered_by_fragment(&fragment));

        let fragment_est = fragment.estimated_bytes();
        self.fragments.push(fragment);
        self.adjust_estimated_bytes(fragment_est as isize);
        delta.saturating_add(fragment_est as isize)
    }

    fn remove_points_covered_by_fragment(&mut self, fragment: &OverlayFragment) -> isize {
        let mut removed = 0usize;
        match fragment {
            OverlayFragment::SparseOffsets { offsets, .. } => {
                for off in offsets.iter().copied() {
                    if let Some(old) = self.points.remove(&(off as usize)) {
                        removed = removed.saturating_add(Self::point_estimate(&old));
                    }
                }
            }
            OverlayFragment::DenseRange { .. } | OverlayFragment::RunRange { .. } => {
                if let Some(range) = fragment.interval_coverage() {
                    let keys: Vec<_> = self
                        .points
                        .keys()
                        .copied()
                        .filter(|off| range.contains(off))
                        .collect();
                    for off in keys {
                        if let Some(old) = self.points.remove(&off) {
                            removed = removed.saturating_add(Self::point_estimate(&old));
                        }
                    }
                }
            }
        }
        self.estimated_bytes = self.estimated_bytes.saturating_sub(removed);
        -(removed as isize)
    }

    fn remove_fragments_covered_by_fragment(&mut self, replacement: &OverlayFragment) -> isize {
        if self.fragments.is_empty() {
            return 0;
        }

        let mut delta: isize = 0;
        let mut fragments = Vec::with_capacity(self.fragments.len());
        for fragment in self.fragments.drain(..) {
            if !fragment.intersects_fragment_exact(replacement) {
                fragments.push(fragment);
                continue;
            }

            let old_est = fragment.estimated_bytes();
            let replacements = fragment.subtract_fragment(replacement);
            let new_est = replacements
                .iter()
                .map(OverlayFragment::estimated_bytes)
                .fold(0usize, usize::saturating_add);
            fragments.extend(replacements);
            delta = delta.saturating_add(new_est as isize - old_est as isize);
        }
        self.fragments = fragments;
        self.adjust_estimated_bytes(delta);
        delta
    }

    #[inline]
    pub(crate) fn remove_scalar(&mut self, off: usize) -> isize {
        let mut delta = 0isize;
        if let Some(old) = self.points.remove(&off) {
            let old_est = Self::point_estimate(&old);
            self.estimated_bytes = self.estimated_bytes.saturating_sub(old_est);
            delta = delta.saturating_sub(old_est as isize);
        }

        if !self.fragments.is_empty() {
            let mut fragments = Vec::with_capacity(self.fragments.len());
            for fragment in self.fragments.drain(..) {
                if fragment.get_scalar(off).is_none() {
                    fragments.push(fragment);
                    continue;
                }

                let old_est = fragment.estimated_bytes();
                let replacements = fragment.subtract_offset(off);
                let new_est = replacements
                    .iter()
                    .map(OverlayFragment::estimated_bytes)
                    .fold(0usize, usize::saturating_add);
                fragments.extend(replacements);
                delta = delta.saturating_add(new_est as isize - old_est as isize);
            }
            self.fragments = fragments;
            self.adjust_estimated_bytes(delta);
        }

        delta
    }

    #[inline]
    pub fn remove(&mut self, off: usize) -> isize {
        self.remove_scalar(off)
    }

    #[inline]
    pub(crate) fn clear_all(&mut self) -> usize {
        let freed = self.estimated_bytes;
        self.points.clear();
        self.fragments.clear();
        self.estimated_bytes = 0;
        freed
    }

    #[inline]
    pub fn clear(&mut self) -> usize {
        self.clear_all()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.points.len().saturating_add(
            self.fragments
                .iter()
                .map(OverlayFragment::coverage_len)
                .sum(),
        )
    }

    #[inline]
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.points.is_empty() && self.fragments.is_empty()
    }

    #[inline]
    pub(crate) fn has_any_in_range(&self, range: core::ops::Range<usize>) -> bool {
        self.points.keys().any(|k| range.contains(k))
            || self
                .fragments
                .iter()
                .any(|fragment| fragment.has_any_in_range(range.clone()))
    }

    #[inline]
    pub fn any_in_range(&self, range: core::ops::Range<usize>) -> bool {
        self.has_any_in_range(range)
    }

    pub(crate) fn slice(&self, off: usize, len: usize) -> Overlay {
        let mut out = Overlay::new();
        let end = off.saturating_add(len);
        for fragment in &self.fragments {
            if let Some(sliced) = fragment.slice(off, len) {
                let _ = out.apply_fragment(sliced);
            }
        }
        for (k, v) in self.points.iter() {
            if *k >= off && *k < end {
                let _ = out.set_scalar(*k - off, v.clone());
            }
        }
        out
    }

    /// Iterate over point `(offset, value)` pairs in the overlay.
    pub fn iter(&self) -> impl Iterator<Item = (&usize, &OverlayValue)> {
        self.points.iter()
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct OverlayDebugStats {
    pub(crate) points: usize,
    pub(crate) sparse_fragments: usize,
    pub(crate) dense_fragments: usize,
    pub(crate) run_fragments: usize,
    pub(crate) covered_len: usize,
}

#[cfg(test)]
impl Overlay {
    pub(crate) fn debug_stats(&self) -> OverlayDebugStats {
        let mut stats = OverlayDebugStats {
            points: self.points.len(),
            covered_len: self.len(),
            ..OverlayDebugStats::default()
        };
        for fragment in &self.fragments {
            match fragment {
                OverlayFragment::SparseOffsets { .. } => stats.sparse_fragments += 1,
                OverlayFragment::DenseRange { .. } => stats.dense_fragments += 1,
                OverlayFragment::RunRange { .. } => stats.run_fragments += 1,
            }
        }
        stats
    }

    pub(crate) fn debug_is_normalized(&self) -> bool {
        let mut covered = std::collections::HashSet::new();
        for off in self.points.keys().copied() {
            if !covered.insert(off) {
                return false;
            }
        }
        for fragment in &self.fragments {
            for (off, _) in fragment.cells() {
                if !covered.insert(off) {
                    return false;
                }
            }
        }
        covered.len() == self.len()
    }
}

pub(crate) struct OverlayCascade<'a> {
    user: &'a Overlay,
    computed: &'a Overlay,
}

impl<'a> OverlayCascade<'a> {
    #[inline]
    pub(crate) fn new(user: &'a Overlay, computed: &'a Overlay) -> Self {
        Self { user, computed }
    }

    #[inline]
    pub(crate) fn get_scalar(&self, off: usize) -> Option<&'a OverlayValue> {
        self.user
            .get_scalar(off)
            .or_else(|| self.computed.get_scalar(off))
    }

    #[inline]
    pub(crate) fn has_any_in_range(&self, range: core::ops::Range<usize>) -> bool {
        self.user.has_any_in_range(range.clone()) || self.computed.has_any_in_range(range)
    }

    pub(crate) fn select_numbers(
        &self,
        range: core::ops::Range<usize>,
        base: &Float64Array,
    ) -> Arc<Float64Array> {
        let len = range.end.saturating_sub(range.start);
        let mut mask_b = BooleanBuilder::with_capacity(len);
        let mut values_b = Float64Builder::with_capacity(len);
        for off in range {
            if let Some(value) = self.get_scalar(off) {
                mask_b.append_value(true);
                if let Some(n) = value.numeric_lane_value() {
                    values_b.append_value(n);
                } else {
                    values_b.append_null();
                }
            } else {
                mask_b.append_value(false);
                values_b.append_null();
            }
        }
        let mask = mask_b.finish();
        let values = values_b.finish();
        let zipped =
            crate::compute_prelude::zip_select(&mask, &values, base).expect("zip numeric overlay");
        Arc::new(
            zipped
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("numeric overlay zip type")
                .clone(),
        )
    }

    pub(crate) fn select_booleans(
        &self,
        range: core::ops::Range<usize>,
        base: &BooleanArray,
    ) -> Arc<BooleanArray> {
        let len = range.end.saturating_sub(range.start);
        let mut mask_b = BooleanBuilder::with_capacity(len);
        let mut values_b = BooleanBuilder::with_capacity(len);
        for off in range {
            if let Some(value) = self.get_scalar(off) {
                mask_b.append_value(true);
                if let Some(b) = value.boolean_lane_value() {
                    values_b.append_value(b);
                } else {
                    values_b.append_null();
                }
            } else {
                mask_b.append_value(false);
                values_b.append_null();
            }
        }
        let mask = mask_b.finish();
        let values = values_b.finish();
        let zipped =
            crate::compute_prelude::zip_select(&mask, &values, base).expect("zip boolean overlay");
        Arc::new(
            zipped
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean overlay zip type")
                .clone(),
        )
    }

    pub(crate) fn select_text(
        &self,
        range: core::ops::Range<usize>,
        base: &StringArray,
    ) -> ArrayRef {
        let len = range.end.saturating_sub(range.start);
        let mut mask_b = BooleanBuilder::with_capacity(len);
        let mut values_b = StringBuilder::with_capacity(len, len * 8);
        for off in range {
            if let Some(value) = self.get_scalar(off) {
                mask_b.append_value(true);
                if let Some(s) = value.text_lane_value() {
                    values_b.append_value(s);
                } else {
                    values_b.append_null();
                }
            } else {
                mask_b.append_value(false);
                values_b.append_null();
            }
        }
        let mask = mask_b.finish();
        let values = values_b.finish();
        crate::compute_prelude::zip_select(&mask, &values, base).expect("zip text overlay")
    }

    pub(crate) fn select_errors(
        &self,
        range: core::ops::Range<usize>,
        base: &UInt8Array,
    ) -> Arc<UInt8Array> {
        let len = range.end.saturating_sub(range.start);
        let mut mask_b = BooleanBuilder::with_capacity(len);
        let mut values_b = UInt8Builder::with_capacity(len);
        for off in range {
            if let Some(value) = self.get_scalar(off) {
                mask_b.append_value(true);
                if let Some(code) = value.error_lane_value() {
                    values_b.append_value(code);
                } else {
                    values_b.append_null();
                }
            } else {
                mask_b.append_value(false);
                values_b.append_null();
            }
        }
        let mask = mask_b.finish();
        let values = values_b.finish();
        let zipped =
            crate::compute_prelude::zip_select(&mask, &values, base).expect("zip error overlay");
        Arc::new(
            zipped
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("error overlay zip type")
                .clone(),
        )
    }

    pub(crate) fn select_type_tags(
        &self,
        range: core::ops::Range<usize>,
        base: &UInt8Array,
    ) -> Arc<UInt8Array> {
        let len = range.end.saturating_sub(range.start);
        let mut mask_b = BooleanBuilder::with_capacity(len);
        let mut values_b = UInt8Builder::with_capacity(len);
        for off in range {
            if let Some(value) = self.get_scalar(off) {
                mask_b.append_value(true);
                values_b.append_value(value.type_tag() as u8);
            } else {
                mask_b.append_value(false);
                values_b.append_null();
            }
        }
        let mask = mask_b.finish();
        let values = values_b.finish();
        let zipped =
            crate::compute_prelude::zip_select(&mask, &values, base).expect("zip type-tag overlay");
        Arc::new(
            zipped
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("type-tag overlay zip type")
                .clone(),
        )
    }

    pub(crate) fn select_lowered_text(
        &self,
        range: core::ops::Range<usize>,
        base: &StringArray,
    ) -> Arc<StringArray> {
        let len = range.end.saturating_sub(range.start);
        let mut mask_b = BooleanBuilder::with_capacity(len);
        let mut values_b = StringBuilder::with_capacity(len, len * 8);
        for off in range {
            if let Some(value) = self.get_scalar(off) {
                mask_b.append_value(true);
                if let Some(s) = value.lowered_text_value() {
                    values_b.append_value(&s);
                } else {
                    values_b.append_null();
                }
            } else {
                mask_b.append_value(false);
                values_b.append_null();
            }
        }
        let mask = mask_b.finish();
        let values = values_b.finish();
        let zipped = crate::compute_prelude::zip_select(&mask, &values, base)
            .expect("zip lowered text overlay");
        Arc::new(
            zipped
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("lowered text overlay zip type")
                .clone(),
        )
    }
}

fn append_overlay_value_to_lane_builders(
    ov: &OverlayValue,
    tag_b: &mut UInt8Builder,
    nb: &mut Float64Builder,
    bb: &mut BooleanBuilder,
    sb: &mut StringBuilder,
    eb: &mut UInt8Builder,
    non_num: &mut usize,
    non_bool: &mut usize,
    non_text: &mut usize,
    non_err: &mut usize,
) {
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
            *non_num += 1;
            bb.append_null();
            sb.append_null();
            eb.append_null();
        }
        OverlayValue::DateTime(serial) => {
            tag_b.append_value(TypeTag::DateTime as u8);
            nb.append_value(*serial);
            *non_num += 1;
            bb.append_null();
            sb.append_null();
            eb.append_null();
        }
        OverlayValue::Duration(serial) => {
            tag_b.append_value(TypeTag::Duration as u8);
            nb.append_value(*serial);
            *non_num += 1;
            bb.append_null();
            sb.append_null();
            eb.append_null();
        }
        OverlayValue::Boolean(b) => {
            tag_b.append_value(TypeTag::Boolean as u8);
            nb.append_null();
            bb.append_value(*b);
            *non_bool += 1;
            sb.append_null();
            eb.append_null();
        }
        OverlayValue::Text(s) => {
            tag_b.append_value(TypeTag::Text as u8);
            nb.append_null();
            bb.append_null();
            sb.append_value(s);
            *non_text += 1;
            eb.append_null();
        }
        OverlayValue::Error(code) => {
            tag_b.append_value(TypeTag::Error as u8);
            nb.append_null();
            bb.append_null();
            sb.append_null();
            eb.append_value(*code);
            *non_err += 1;
        }
        OverlayValue::Pending => {
            tag_b.append_value(TypeTag::Pending as u8);
            nb.append_null();
            bb.append_null();
            sb.append_null();
            eb.append_null();
        }
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

    /// Fast single-cell read (0-based row/col) with overlay precedence.
    ///
    /// This avoids constructing a 1x1 RangeView and is intended for tight read loops.
    #[inline]
    pub fn get_cell_value(&self, abs_row: usize, abs_col: usize) -> LiteralValue {
        let sheet_rows = self.nrows as usize;
        if abs_row >= sheet_rows {
            return LiteralValue::Empty;
        }
        if abs_col >= self.columns.len() {
            return LiteralValue::Empty;
        }
        let Some((ch_idx, in_off)) = self.chunk_of_row(abs_row) else {
            return LiteralValue::Empty;
        };
        let col_ref = &self.columns[abs_col];
        let Some(ch) = col_ref.chunk(ch_idx) else {
            return LiteralValue::Empty;
        };

        // Overlay takes precedence: user edits over computed over base.
        let cascade = OverlayCascade::new(&ch.overlay, &ch.computed_overlay);
        if let Some(ov) = cascade.get_scalar(in_off) {
            return ov.to_literal();
        }

        // Read tag and route to lane.
        let tag_u8 = ch.type_tag.value(in_off);
        match TypeTag::from_u8(tag_u8) {
            TypeTag::Empty => LiteralValue::Empty,
            TypeTag::Number => {
                if let Some(arr) = &ch.numbers {
                    if arr.is_null(in_off) {
                        return LiteralValue::Empty;
                    }
                    LiteralValue::Number(arr.value(in_off))
                } else {
                    LiteralValue::Empty
                }
            }
            TypeTag::DateTime => {
                if let Some(arr) = &ch.numbers {
                    if arr.is_null(in_off) {
                        return LiteralValue::Empty;
                    }
                    LiteralValue::from_serial_number(arr.value(in_off))
                } else {
                    LiteralValue::Empty
                }
            }
            TypeTag::Duration => {
                if let Some(arr) = &ch.numbers {
                    if arr.is_null(in_off) {
                        return LiteralValue::Empty;
                    }
                    let serial = arr.value(in_off);
                    let nanos_f = serial * 86_400.0 * 1_000_000_000.0;
                    let nanos = nanos_f.round().clamp(i64::MIN as f64, i64::MAX as f64) as i64;
                    LiteralValue::Duration(chrono::Duration::nanoseconds(nanos))
                } else {
                    LiteralValue::Empty
                }
            }
            TypeTag::Boolean => {
                if let Some(arr) = &ch.booleans {
                    if arr.is_null(in_off) {
                        return LiteralValue::Empty;
                    }
                    LiteralValue::Boolean(arr.value(in_off))
                } else {
                    LiteralValue::Empty
                }
            }
            TypeTag::Text => {
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
            TypeTag::Error => {
                if let Some(arr) = &ch.errors {
                    if arr.is_null(in_off) {
                        return LiteralValue::Empty;
                    }
                    let kind = unmap_error_code(arr.value(in_off));
                    LiteralValue::Error(ExcelError::new(kind))
                } else {
                    LiteralValue::Empty
                }
            }
            TypeTag::Pending => LiteralValue::Pending,
        }
    }

    /// Ensure capacity to address at least `target_rows` rows by extending the row chunk map.
    ///
    /// This updates `chunk_starts`/`nrows` but does **not** eagerly densify all columns with
    /// new empty chunks. Missing chunks are treated as all-empty and can be materialized lazily.
    pub fn ensure_row_capacity(&mut self, target_rows: usize) {
        if target_rows as u32 <= self.nrows {
            return;
        }

        let chunk_size = self.chunk_rows.max(1);

        // `chunk_starts` must represent fixed-size chunk boundaries based on `chunk_rows`, not
        // incremental growth steps. In particular, repeated calls like ensure_row_capacity(1),
        // ensure_row_capacity(2), ... must NOT create a new chunk per row.
        if self.chunk_starts.is_empty() {
            self.chunk_starts.push(0);
        }

        // Extend chunk starts only when `target_rows` crosses a chunk boundary.
        // Example: chunk_size=3, target_rows=6 => chunk_starts=[0,3]
        let mut next_start = self
            .chunk_starts
            .last()
            .copied()
            .unwrap_or(0)
            .saturating_add(chunk_size);
        while next_start < target_rows {
            self.chunk_starts.push(next_start);
            next_start = next_start.saturating_add(chunk_size);
        }

        self.nrows = target_rows as u32;

        // Any previously-materialized chunk may have been created when the sheet had fewer rows.
        // When `chunk_starts` extends, chunks that used to be "last" can become interior chunks
        // with a larger fixed boundary. Ensure materialized chunks are grown to their current
        // boundary-derived length so RangeView slicing stays in-bounds.
        let starts = self.chunk_starts.clone();
        let nrows = self.nrows as usize;
        let required_len_for = |ch_idx: usize| -> Option<usize> {
            let start = *starts.get(ch_idx)?;
            let end = starts.get(ch_idx + 1).copied().unwrap_or(nrows);
            Some(end.saturating_sub(start))
        };

        for col in &mut self.columns {
            for (idx, ch) in col.chunks.iter_mut().enumerate() {
                if let Some(req) = required_len_for(idx) {
                    ch.grow_len_to(req);
                }
            }
            if !col.sparse_chunks.is_empty() {
                let keys: Vec<usize> = col.sparse_chunks.keys().copied().collect();
                for idx in keys {
                    if let (Some(req), Some(ch)) =
                        (required_len_for(idx), col.sparse_chunks.get_mut(&idx))
                    {
                        ch.grow_len_to(req);
                    }
                }
            }
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
        // Split overlays for this slice.
        let overlay = ch.overlay.slice(off, len);
        let computed_overlay = ch.computed_overlay.slice(off, len);
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
    ) -> usize {
        if col_idx >= self.columns.len() {
            return 0;
        }

        let (len, tags, numbers, booleans, text, errors, non_num, non_bool, non_text, non_err) = {
            let Some(ch_ref) = self.columns[col_idx].chunk(ch_idx) else {
                return 0;
            };
            let len = ch_ref.type_tag.len();
            if len == 0 {
                return 0;
            }

            let ov_len = ch_ref.overlay.len();
            let den = frac_den.max(1);
            let trig = ov_len > (len / den) || ov_len > abs_threshold;
            if !trig {
                return 0;
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
                if let Some(ov) = ch_ref.overlay.get_scalar(i) {
                    append_overlay_value_to_lane_builders(
                        ov,
                        &mut tag_b,
                        &mut nb,
                        &mut bb,
                        &mut sb,
                        &mut eb,
                        &mut non_num,
                        &mut non_bool,
                        &mut non_text,
                        &mut non_err,
                    );
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
                            tag_b.append_value(tag as u8);
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
            return 0;
        };

        ch_mut.type_tag = tags;
        ch_mut.numbers = numbers;
        ch_mut.booleans = booleans;
        ch_mut.text = text;
        ch_mut.errors = errors;
        let freed = ch_mut.overlay.clear();
        ch_mut.lowered_text = OnceCell::new();
        ch_mut.meta.len = len;
        ch_mut.meta.non_null_num = non_num;
        ch_mut.meta.non_null_bool = non_bool;
        ch_mut.meta.non_null_text = non_text;
        ch_mut.meta.non_null_err = non_err;
        freed
    }

    /// Compact a dense chunk's computed overlay into its base arrays, freeing overlay memory
    /// while preserving the data. Returns the number of bytes freed.
    ///
    /// This is the computed-overlay counterpart of `maybe_compact_chunk` (which compacts
    /// user-edit overlays). The read cascade is `overlay → computed_overlay → base`, so
    /// folding computed overlay entries into base arrays is transparent: the `overlay` layer
    /// (user edits) is left untouched and still takes precedence on reads.
    pub fn compact_computed_overlay_chunk(&mut self, col_idx: usize, ch_idx: usize) -> usize {
        if col_idx >= self.columns.len() {
            return 0;
        }

        let (len, tags, numbers, booleans, text, errors, non_num, non_bool, non_text, non_err) = {
            let Some(ch_ref) = self.columns[col_idx].chunk(ch_idx) else {
                return 0;
            };
            let len = ch_ref.type_tag.len();
            if len == 0 || ch_ref.computed_overlay.is_empty() {
                return 0;
            }

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
                if let Some(ov) = ch_ref.computed_overlay.get_scalar(i) {
                    append_overlay_value_to_lane_builders(
                        ov,
                        &mut tag_b,
                        &mut nb,
                        &mut bb,
                        &mut sb,
                        &mut eb,
                        &mut non_num,
                        &mut non_bool,
                        &mut non_text,
                        &mut non_err,
                    );
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
                            tag_b.append_value(tag as u8);
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
            return 0;
        };

        ch_mut.type_tag = tags;
        ch_mut.numbers = numbers;
        ch_mut.booleans = booleans;
        ch_mut.text = text;
        ch_mut.errors = errors;
        let freed = ch_mut.computed_overlay.clear();
        ch_mut.lowered_text = OnceCell::new();
        ch_mut.meta.len = len;
        ch_mut.meta.non_null_num = non_num;
        ch_mut.meta.non_null_bool = non_bool;
        ch_mut.meta.non_null_text = non_text;
        ch_mut.meta.non_null_err = non_err;
        freed
    }

    /// Compact a sparse chunk's computed overlay into its base arrays.
    /// Equivalent to `compact_computed_overlay_chunk` but for sparse chunks.
    pub fn compact_computed_overlay_sparse_chunk(
        &mut self,
        col_idx: usize,
        ch_idx: usize,
    ) -> usize {
        // Sparse chunks are accessed via the same chunk/chunk_mut API,
        // so we delegate to the dense method which already handles both.
        self.compact_computed_overlay_chunk(col_idx, ch_idx)
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
    fn overlay_slice_preserves_explicit_empty_and_offsets() {
        let mut overlay = Overlay::new();
        overlay.set(2, OverlayValue::Number(2.0));
        overlay.set(4, OverlayValue::Empty);
        overlay.set(6, OverlayValue::Text(Arc::from("outside")));

        let sliced = overlay.slice(1, 4);
        assert!(sliced.get_scalar(0).is_none());
        assert_eq!(
            sliced.get_scalar(1).unwrap().to_literal(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            sliced.get_scalar(3).unwrap().to_literal(),
            LiteralValue::Empty
        );
        assert!(sliced.get_scalar(5).is_none());
    }

    #[test]
    fn overlay_cascade_user_empty_masks_computed_and_base() {
        let mut user = Overlay::new();
        let mut computed = Overlay::new();
        computed.set(1, OverlayValue::Number(42.0));
        user.set(1, OverlayValue::Empty);

        let cascade = OverlayCascade::new(&user, &computed);
        assert_eq!(
            cascade.get_scalar(1).unwrap().to_literal(),
            LiteralValue::Empty
        );
        assert!(cascade.has_any_in_range(1..2));
    }

    #[test]
    fn overlay_storage_pointmap_backward_compat_get_set_remove() {
        let mut overlay = Overlay::new();
        assert!(overlay.is_empty());

        let delta = overlay.set_scalar(1, OverlayValue::Number(10.0));
        assert!(delta > 0);
        assert_eq!(overlay.len(), 1);
        assert_eq!(
            overlay.get_scalar(1).unwrap().to_literal(),
            LiteralValue::Number(10.0)
        );

        let replace_delta = overlay.set_scalar(1, OverlayValue::Text(Arc::from("x")));
        assert_ne!(replace_delta, 0);
        assert_eq!(overlay.len(), 1);
        assert_eq!(
            overlay.get_scalar(1).unwrap().to_literal(),
            LiteralValue::Text("x".into())
        );

        let remove_delta = overlay.remove_scalar(1);
        assert!(remove_delta < 0);
        assert!(overlay.is_empty());
        assert!(overlay.get_scalar(1).is_none());
    }

    #[test]
    fn overlay_storage_no_fragments_behavior_matches_old_map() {
        let mut overlay = Overlay::new();
        overlay.set_scalar(0, OverlayValue::Number(1.0));
        overlay.set_scalar(3, OverlayValue::Empty);

        assert!(overlay.has_any_in_range(0..1));
        assert!(!overlay.has_any_in_range(1..3));
        assert!(overlay.has_any_in_range(3..4));

        let sliced = overlay.slice(2, 3);
        assert!(sliced.get_scalar(0).is_none());
        assert_eq!(
            sliced.get_scalar(1).unwrap().to_literal(),
            LiteralValue::Empty
        );
    }

    #[test]
    fn overlay_cascade_user_layer_masks_computed_fragment_regardless_of_sequence() {
        let mut user = Overlay::new();
        let mut computed = Overlay::new();

        user.set_scalar(0, OverlayValue::Number(3.0));
        computed.apply_fragment(
            OverlayFragment::dense_range(0, vec![OverlayValue::Number(2.0)]).unwrap(),
        );

        let cascade = OverlayCascade::new(&user, &computed);
        assert_eq!(
            cascade.get_scalar(0).unwrap().to_literal(),
            LiteralValue::Number(3.0)
        );
    }

    #[test]
    fn overlay_same_layer_later_point_replaces_fragment_cell() {
        let mut overlay = Overlay::new();
        overlay.apply_fragment(
            OverlayFragment::dense_range(
                0,
                vec![
                    OverlayValue::Number(1.0),
                    OverlayValue::Number(2.0),
                    OverlayValue::Number(3.0),
                ],
            )
            .unwrap(),
        );

        overlay.set_scalar(1, OverlayValue::Number(99.0));

        assert_eq!(
            overlay.get_scalar(0).unwrap().to_literal(),
            LiteralValue::Number(1.0)
        );
        assert_eq!(
            overlay.get_scalar(1).unwrap().to_literal(),
            LiteralValue::Number(99.0)
        );
        assert_eq!(
            overlay.get_scalar(2).unwrap().to_literal(),
            LiteralValue::Number(3.0)
        );
    }

    #[test]
    fn overlay_same_layer_later_fragment_replaces_point_range() {
        let mut overlay = Overlay::new();
        overlay.set_scalar(0, OverlayValue::Number(1.0));
        overlay.set_scalar(1, OverlayValue::Number(2.0));
        overlay.set_scalar(2, OverlayValue::Number(3.0));

        overlay.apply_fragment(
            OverlayFragment::dense_range(
                0,
                vec![
                    OverlayValue::Number(10.0),
                    OverlayValue::Number(20.0),
                    OverlayValue::Number(30.0),
                ],
            )
            .unwrap(),
        );

        let stats = overlay.debug_stats();
        assert_eq!(stats.points, 0);
        assert_eq!(stats.dense_fragments, 1);
        assert!(overlay.debug_is_normalized());
        assert_eq!(
            overlay.get_scalar(0).unwrap().to_literal(),
            LiteralValue::Number(10.0)
        );
        assert_eq!(
            overlay.get_scalar(1).unwrap().to_literal(),
            LiteralValue::Number(20.0)
        );
        assert_eq!(
            overlay.get_scalar(2).unwrap().to_literal(),
            LiteralValue::Number(30.0)
        );
    }

    #[test]
    fn overlay_sparse_far_apart_replacement_does_not_rewrite_unrelated_dense_fragment() {
        let mut overlay = Overlay::new();
        overlay.apply_fragment(
            OverlayFragment::dense_range(100, vec![OverlayValue::Number(1.0); 10]).unwrap(),
        );

        overlay.apply_fragment(
            OverlayFragment::sparse_offsets(vec![
                (0, OverlayValue::Empty),
                (1000, OverlayValue::Number(1000.0)),
            ])
            .unwrap(),
        );

        let stats = overlay.debug_stats();
        assert_eq!(stats.dense_fragments, 1);
        assert_eq!(stats.sparse_fragments, 1);
        assert_eq!(stats.run_fragments, 0);
        assert!(overlay.debug_is_normalized());
        assert_eq!(
            overlay.get_scalar(105).unwrap().to_literal(),
            LiteralValue::Number(1.0)
        );
        assert_eq!(
            overlay.get_scalar(0).unwrap().to_literal(),
            LiteralValue::Empty
        );
        assert_eq!(
            overlay.get_scalar(1000).unwrap().to_literal(),
            LiteralValue::Number(1000.0)
        );
    }

    #[test]
    fn overlay_sparse_offsets_are_sorted_unique_last_write_wins() {
        let mut overlay = Overlay::new();
        overlay.apply_fragment(
            OverlayFragment::sparse_offsets(vec![
                (3, OverlayValue::Number(3.0)),
                (1, OverlayValue::Number(1.0)),
                (3, OverlayValue::Number(33.0)),
            ])
            .unwrap(),
        );

        let stats = overlay.debug_stats();
        assert_eq!(stats.sparse_fragments, 1);
        assert_eq!(overlay.len(), 2);
        assert_eq!(
            overlay.get_scalar(1).unwrap().to_literal(),
            LiteralValue::Number(1.0)
        );
        assert_eq!(
            overlay.get_scalar(3).unwrap().to_literal(),
            LiteralValue::Number(33.0)
        );
        assert!(overlay.debug_is_normalized());
    }

    #[test]
    fn overlay_dense_point_replacement_splits_dense_not_sparse() {
        let mut overlay = Overlay::new();
        overlay.apply_fragment(
            OverlayFragment::dense_range(
                0,
                (0..6)
                    .map(|i| OverlayValue::Number(i as f64))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        );

        overlay.set_scalar(3, OverlayValue::Number(99.0));

        let stats = overlay.debug_stats();
        assert_eq!(stats.points, 1);
        assert_eq!(stats.dense_fragments, 2);
        assert_eq!(stats.sparse_fragments, 0);
        assert!(overlay.debug_is_normalized());
        assert_eq!(
            overlay.get_scalar(2).unwrap().to_literal(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            overlay.get_scalar(3).unwrap().to_literal(),
            LiteralValue::Number(99.0)
        );
        assert_eq!(
            overlay.get_scalar(4).unwrap().to_literal(),
            LiteralValue::Number(4.0)
        );
    }

    #[test]
    fn overlay_dense_fragment_replacement_splits_left_and_right_dense() {
        let mut overlay = Overlay::new();
        overlay.apply_fragment(
            OverlayFragment::dense_range(
                0,
                (0..8)
                    .map(|i| OverlayValue::Number(i as f64))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        );

        overlay.apply_fragment(
            OverlayFragment::dense_range(
                3,
                vec![OverlayValue::Number(30.0), OverlayValue::Number(40.0)],
            )
            .unwrap(),
        );

        let stats = overlay.debug_stats();
        assert_eq!(stats.points, 0);
        assert_eq!(stats.dense_fragments, 3);
        assert_eq!(stats.sparse_fragments, 0);
        assert!(overlay.debug_is_normalized());
        assert_eq!(
            overlay.get_scalar(2).unwrap().to_literal(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            overlay.get_scalar(3).unwrap().to_literal(),
            LiteralValue::Number(30.0)
        );
        assert_eq!(
            overlay.get_scalar(4).unwrap().to_literal(),
            LiteralValue::Number(40.0)
        );
        assert_eq!(
            overlay.get_scalar(5).unwrap().to_literal(),
            LiteralValue::Number(5.0)
        );
    }

    #[test]
    fn overlay_run_point_replacement_splits_run_not_sparse() {
        let mut overlay = Overlay::new();
        overlay.apply_fragment(
            OverlayFragment::run_range(0, vec![OverlayValue::Number(1.0); 10]).unwrap(),
        );

        overlay.set_scalar(5, OverlayValue::Number(99.0));

        let stats = overlay.debug_stats();
        assert_eq!(stats.points, 1);
        assert_eq!(stats.run_fragments, 2);
        assert_eq!(stats.sparse_fragments, 0);
        assert!(overlay.debug_is_normalized());
        assert_eq!(
            overlay.get_scalar(4).unwrap().to_literal(),
            LiteralValue::Number(1.0)
        );
        assert_eq!(
            overlay.get_scalar(5).unwrap().to_literal(),
            LiteralValue::Number(99.0)
        );
        assert_eq!(
            overlay.get_scalar(6).unwrap().to_literal(),
            LiteralValue::Number(1.0)
        );
    }

    #[test]
    fn overlay_run_fragment_replacement_splits_left_and_right_run() {
        let mut overlay = Overlay::new();
        let values = [
            vec![OverlayValue::Number(1.0); 4],
            vec![OverlayValue::Number(2.0); 4],
            vec![OverlayValue::Number(3.0); 4],
        ]
        .concat();
        overlay.apply_fragment(OverlayFragment::run_range(0, values).unwrap());

        overlay.apply_fragment(
            OverlayFragment::dense_range(
                5,
                vec![OverlayValue::Number(50.0), OverlayValue::Number(60.0)],
            )
            .unwrap(),
        );

        let stats = overlay.debug_stats();
        assert_eq!(stats.run_fragments, 2);
        assert_eq!(stats.dense_fragments, 1);
        assert_eq!(stats.sparse_fragments, 0);
        assert!(overlay.debug_is_normalized());
        assert_eq!(
            overlay.get_scalar(4).unwrap().to_literal(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            overlay.get_scalar(5).unwrap().to_literal(),
            LiteralValue::Number(50.0)
        );
        assert_eq!(
            overlay.get_scalar(6).unwrap().to_literal(),
            LiteralValue::Number(60.0)
        );
        assert_eq!(
            overlay.get_scalar(7).unwrap().to_literal(),
            LiteralValue::Number(2.0)
        );
    }

    #[test]
    fn overlay_slice_preserves_dense_and_run_encodings() {
        let mut overlay = Overlay::new();
        overlay.apply_fragment(
            OverlayFragment::dense_range(
                10,
                (0..5)
                    .map(|i| OverlayValue::Number(i as f64))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        );
        overlay.apply_fragment(
            OverlayFragment::run_range(
                20,
                [
                    vec![OverlayValue::Number(1.0); 3],
                    vec![OverlayValue::Number(2.0); 3],
                ]
                .concat(),
            )
            .unwrap(),
        );

        let dense_slice = overlay.slice(12, 2);
        let dense_stats = dense_slice.debug_stats();
        assert_eq!(dense_stats.dense_fragments, 1);
        assert_eq!(dense_stats.sparse_fragments, 0);
        assert_eq!(
            dense_slice.get_scalar(0).unwrap().to_literal(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            dense_slice.get_scalar(1).unwrap().to_literal(),
            LiteralValue::Number(3.0)
        );
        assert!(dense_slice.debug_is_normalized());

        let run_slice = overlay.slice(22, 3);
        let run_stats = run_slice.debug_stats();
        assert_eq!(run_stats.run_fragments, 1);
        assert_eq!(run_stats.sparse_fragments, 0);
        assert_eq!(
            run_slice.get_scalar(0).unwrap().to_literal(),
            LiteralValue::Number(1.0)
        );
        assert_eq!(
            run_slice.get_scalar(1).unwrap().to_literal(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            run_slice.get_scalar(2).unwrap().to_literal(),
            LiteralValue::Number(2.0)
        );
        assert!(run_slice.debug_is_normalized());
    }

    #[test]
    fn overlay_computed_empty_run_masks_non_empty_base() {
        let mut b = IngestBuilder::new("S", 1, 8, crate::engine::DateSystem::Excel1900);
        b.append_row(&[LiteralValue::Number(1.0)]).unwrap();
        b.append_row(&[LiteralValue::Number(2.0)]).unwrap();
        b.append_row(&[LiteralValue::Number(3.0)]).unwrap();
        let mut sheet = b.finish();

        let (ch_i, _) = sheet.chunk_of_row(0).unwrap();
        sheet.columns[0].chunks[ch_i]
            .computed_overlay
            .apply_fragment(
                OverlayFragment::run_range(
                    0,
                    vec![
                        OverlayValue::Empty,
                        OverlayValue::Empty,
                        OverlayValue::Empty,
                    ],
                )
                .unwrap(),
            );

        assert_eq!(sheet.get_cell_value(0, 0), LiteralValue::Empty);
        assert_eq!(sheet.get_cell_value(1, 0), LiteralValue::Empty);
        assert_eq!(sheet.get_cell_value(2, 0), LiteralValue::Empty);
    }

    #[test]
    fn overlay_segment_numbers_masks_base_for_non_numeric_overlays() {
        let mut user = Overlay::new();
        user.set(1, OverlayValue::Text(Arc::from("x")));
        user.set(2, OverlayValue::Empty);
        user.set(3, OverlayValue::Error(map_error_code(ExcelErrorKind::Div)));
        user.set(4, OverlayValue::Pending);
        let computed = Overlay::new();
        let cascade = OverlayCascade::new(&user, &computed);

        let base = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        let selected = cascade.select_numbers(0..5, &base);
        assert_eq!(selected.value(0), 10.0);
        assert!(selected.is_null(1));
        assert!(selected.is_null(2));
        assert!(selected.is_null(3));
        assert!(selected.is_null(4));
    }

    #[test]
    fn overlay_segment_type_tags_preserve_temporal_tags() {
        let mut computed = Overlay::new();
        computed.set(0, OverlayValue::DateTime(45000.5));
        computed.set(1, OverlayValue::Duration(0.25));
        let user = Overlay::new();
        let cascade = OverlayCascade::new(&user, &computed);

        let base = UInt8Array::from(vec![TypeTag::Empty as u8; 2]);
        let selected = cascade.select_type_tags(0..2, &base);
        assert_eq!(selected.value(0), TypeTag::DateTime as u8);
        assert_eq!(selected.value(1), TypeTag::Duration as u8);
    }

    #[test]
    fn overlay_lowered_text_matches_existing_overlay_semantics() {
        let mut user = Overlay::new();
        user.set(0, OverlayValue::Text(Arc::from("HeLLo")));
        user.set(1, OverlayValue::Number(1.5));
        user.set(2, OverlayValue::Boolean(true));
        user.set(3, OverlayValue::Empty);
        let computed = Overlay::new();
        let cascade = OverlayCascade::new(&user, &computed);

        let base = StringArray::from(vec![Some("A"), Some("B"), Some("C"), Some("D")]);
        let selected = cascade.select_lowered_text(0..4, &base);
        assert_eq!(selected.value(0), "hello");
        assert_eq!(selected.value(1), "1.5");
        assert_eq!(selected.value(2), "true");
        assert!(selected.is_null(3));
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
            let _ = sh.columns[0].chunks[ch_i].overlay.set(off, ov);
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
            let _ = sh.columns[col].chunks[ch_i].overlay.set(off, ov);
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
