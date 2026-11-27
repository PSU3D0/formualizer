//! Unified region-write engine primitive (`v0.16.0` Decision 2).
//!
//! Handles fresh sheets, populated sheets, dense columnar regions, dense/ragged
//! row-major blocks, and sparse coordinate patches through a single reconciliation
//! pipeline that preserves exact equivalence with per-cell `Engine::set_cell_value`.

use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, StringBuilder, UInt8Builder};
use arrow_array::{Array, ArrayRef, Float64Array, StringArray, UInt8Array};
use formualizer_common::numfmt::FormatClass;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};

use crate::arrow_store::{
    ArrowSheet, ColumnChunk, ColumnChunkMeta, FormatRuns, Overlay, OverlayValue, TypeTag,
    map_error_code,
};
use crate::engine::eval::{Engine, StructuralScope};
use crate::engine::template::region::Region;
use crate::engine::{DateSystem, VertexKind};
use crate::format::FormatId;
use crate::reference::{CellRef, Coord};
use crate::traits::EvaluationContext;

/// Maximum magnitude of an exact IEEE-754 `f64` integer (`2^53`).
pub(crate) const MAX_SAFE_F64_INT: i64 = 1_i64 << 53;

/// Private wire sentinel for ragged-row skipped cells (never exposed publicly
/// and always rejected by public wire APIs).
#[allow(dead_code)]
pub(crate) const WIRE_TAG_SKIP: u8 = 255;

/// Declared wire tag enum for packed bulk-write blocks.
///
/// Intentionally decoupled from internal `arrow_store::TypeTag` so storage
/// renumbering cannot break the wire ABI. Unknown bytes, `TypeTag::Pending` (7),
/// and internal `Skip` (255) are rejected atomically during preflight.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkCellTag {
    Empty = 0,
    Number = 1,
    Boolean = 2,
    Text = 3,
    Error = 4,
    DateTime = 5,
    Duration = 6,
}

impl TryFrom<u8> for BulkCellTag {
    type Error = ExcelError;

    #[inline]
    fn try_from(value: u8) -> Result<Self, ExcelError> {
        match value {
            0 => Ok(Self::Empty),
            1 => Ok(Self::Number),
            2 => Ok(Self::Boolean),
            3 => Ok(Self::Text),
            4 => Ok(Self::Error),
            5 => Ok(Self::DateTime),
            6 => Ok(Self::Duration),
            7 => Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("Bulk write wire tag 7 (Pending) is not allowed")),
            other => Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("Unknown bulk write wire tag: {other}"))),
        }
    }
}

/// Summary returned by bulk cell-write operations.
///
/// Coordinates are 1-based inclusive bounds of the written cells. When an
/// operation writes zero cells (empty input), all four coordinate fields are `0`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteRangeReport {
    pub start_row: u32,
    pub start_col: u32,
    pub end_row: u32,
    pub end_col: u32,
    pub history_cleared: bool,
}

/// Packed mixed-type column block using the unified wire format.
#[derive(Clone, Copy, Debug)]
pub struct PackedColumnBlock<'a> {
    pub tags: &'a [u8],
    pub numbers: &'a [f64],
    pub text_bytes: &'a [u8],
    pub text_offsets: &'a [u32],
}

/// Typed columnar input for dense column-major bulk writes at the engine layer.
///
/// Every `validity` slice is strictly an Arrow LSB validity bitmap where bit `i`
/// is `(validity[i / 8] >> (i % 8)) & 1 != 0`.
#[derive(Clone, Copy, Debug)]
pub enum BulkColumn<'a> {
    Float64 {
        values: &'a [f64],
        validity: Option<&'a [u8]>,
    },
    Int64 {
        values: &'a [i64],
        validity: Option<&'a [u8]>,
    },
    Boolean {
        values: &'a [bool],
        validity: Option<&'a [u8]>,
    },
    Utf8 {
        bytes: &'a [u8],
        offsets: &'a [u32],
        validity: Option<&'a [u8]>,
    },
    Serial {
        values: &'a [f64],
        format: FormatId,
        validity: Option<&'a [u8]>,
    },
    Null {
        len: usize,
    },
    Mixed(PackedColumnBlock<'a>),
}

impl<'a> BulkColumn<'a> {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Float64 { values, .. } => values.len(),
            Self::Int64 { values, .. } => values.len(),
            Self::Boolean { values, .. } => values.len(),
            Self::Utf8 { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Serial { values, .. } => values.len(),
            Self::Null { len } => *len,
            Self::Mixed(block) => block.tags.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Sparse point-patch input backed by the unified wire block format and 1-based coordinates.
#[derive(Clone, Copy, Debug)]
pub struct CellBatch<'a> {
    pub rows: &'a [u32],
    pub cols: &'a [u32],
    pub tags: &'a [u8],
    pub numbers: &'a [f64],
    pub text_bytes: &'a [u8],
    pub text_offsets: &'a [u32],
}

impl CellBatch<'_> {
    #[inline]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[inline]
fn validity_is_valid(validity: Option<&[u8]>, idx: usize) -> bool {
    match validity {
        None => true,
        Some(bits) => ((bits[idx >> 3] >> (idx & 7)) & 1) != 0,
    }
}

fn validate_validity_bitmap(
    validity: Option<&[u8]>,
    len: usize,
    col_idx: usize,
) -> Result<(), ExcelError> {
    if let Some(bits) = validity {
        let needed = len.div_ceil(8);
        if bits.len() < needed {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                "Column {col_idx}: validity bitmap has {} bytes, expected at least {needed} for {len} rows",
                bits.len()
            )));
        }
    }
    Ok(())
}

fn validate_utf8_offsets(
    bytes: &[u8],
    offsets: &[u32],
    validity: Option<&[u8]>,
    context: &str,
) -> Result<(), ExcelError> {
    if offsets.is_empty() {
        return Ok(());
    }
    let mut prev = offsets[0] as usize;
    if prev > bytes.len() {
        return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
            "{context}: initial UTF-8 offset {prev} exceeds byte buffer length {}",
            bytes.len()
        )));
    }
    for (i, &raw_end) in offsets[1..].iter().enumerate() {
        let end = raw_end as usize;
        if end < prev {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                "{context}: non-monotonic UTF-8 offsets at index {i} ({prev} -> {end})"
            )));
        }
        if end > bytes.len() {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                "{context}: UTF-8 offset {end} at index {i} exceeds byte buffer length {}",
                bytes.len()
            )));
        }
        if validity_is_valid(validity, i) && std::str::from_utf8(&bytes[prev..end]).is_err() {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                "{context}: invalid UTF-8 string bytes at index {i}"
            )));
        }
        prev = end;
    }
    Ok(())
}

fn validate_wire_cells(
    tags: &[u8],
    numbers: &[f64],
    text_bytes: &[u8],
    text_offsets: &[u32],
    context: &str,
) -> Result<(), ExcelError> {
    if tags.len() != numbers.len() {
        return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
            "{context}: tags length ({}) does not match numbers length ({})",
            tags.len(),
            numbers.len()
        )));
    }
    validate_utf8_offsets(text_bytes, text_offsets, None, context)?;
    let string_count = text_offsets.len().saturating_sub(1);
    for (i, (&raw_tag, &num)) in tags.iter().zip(numbers.iter()).enumerate() {
        let tag = BulkCellTag::try_from(raw_tag).map_err(|e| {
            ExcelError::new(e.kind).with_message(format!(
                "{context} at index {i}: {}",
                e.message().unwrap_or_default()
            ))
        })?;
        match tag {
            BulkCellTag::Empty
            | BulkCellTag::Number
            | BulkCellTag::DateTime
            | BulkCellTag::Duration => {}
            BulkCellTag::Boolean => {
                if !(num == 0.0 || num == 1.0) {
                    return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                        "{context}: invalid boolean payload {num} at index {i} (expected 0.0 or 1.0)"
                    )));
                }
            }
            BulkCellTag::Text => {
                if !num.is_finite()
                    || num < 0.0
                    || num.fract() != 0.0
                    || num > (MAX_SAFE_F64_INT as f64)
                    || (num as usize) >= string_count
                {
                    return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                        "{context}: string table index {num} at index {i} out of bounds (table len {string_count})"
                    )));
                }
            }
            BulkCellTag::Error => {
                if !num.is_finite() || num.fract() != 0.0 || !(1.0..=13.0).contains(&num) {
                    return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                        "{context}: invalid Excel error code {num} at index {i} (expected 1..=13)"
                    )));
                }
            }
        }
    }
    Ok(())
}

#[inline]
fn wire_datetime_format(serial: f64) -> FormatId {
    if serial.fract() == 0.0 {
        FormatId::DATE
    } else {
        FormatId::DATETIME
    }
}

#[inline]
fn decode_wire_cell<'a>(
    tag: u8,
    num: f64,
    text_bytes: &'a [u8],
    text_offsets: &'a [u32],
) -> DecodedCell<'a> {
    match tag {
        0 => DecodedCell::Empty,
        1 => {
            if num.is_finite() {
                DecodedCell::Number(num, FormatId::GENERAL)
            } else {
                DecodedCell::Error(map_error_code(ExcelErrorKind::Num))
            }
        }
        2 => DecodedCell::Boolean(num != 0.0),
        3 => {
            let idx = num as usize;
            let s0 = text_offsets[idx] as usize;
            let s1 = text_offsets[idx + 1] as usize;
            let s = std::str::from_utf8(&text_bytes[s0..s1])
                .expect("validated UTF-8 string table in preflight");
            DecodedCell::Text(s)
        }
        4 => DecodedCell::Error(num as u8),
        5 => {
            if num.is_finite() {
                DecodedCell::DateTime(num, wire_datetime_format(num))
            } else {
                DecodedCell::Error(map_error_code(ExcelErrorKind::Num))
            }
        }
        6 => {
            if num.is_finite() {
                DecodedCell::Duration(num, FormatId::DURATION)
            } else {
                DecodedCell::Error(map_error_code(ExcelErrorKind::Num))
            }
        }
        _ => unreachable!("wire tag validated in preflight"),
    }
}

#[derive(Clone, Copy)]
enum DecodedCell<'a> {
    Empty,
    Number(f64, FormatId),
    DateTime(f64, FormatId),
    Duration(f64, FormatId),
    Boolean(bool),
    Text(&'a str),
    Error(u8),
    Pending,
}

impl<'a> DecodedCell<'a> {
    #[inline]
    fn from_literal(value: &'a LiteralValue, date_system: DateSystem) -> Self {
        match value {
            LiteralValue::Empty => Self::Empty,
            LiteralValue::Int(i) => Self::Number(*i as f64, FormatId::GENERAL),
            LiteralValue::Number(n) => {
                if n.is_finite() {
                    Self::Number(*n, FormatId::GENERAL)
                } else {
                    Self::Error(map_error_code(ExcelErrorKind::Num))
                }
            }
            LiteralValue::Boolean(b) => Self::Boolean(*b),
            LiteralValue::Text(s) => Self::Text(s.as_str()),
            LiteralValue::Error(e) => Self::Error(map_error_code(e.kind)),
            LiteralValue::Date(d) => {
                let dt = d.and_hms_opt(0, 0, 0).unwrap();
                let serial = formualizer_common::datetime_to_serial_for(date_system, &dt);
                Self::DateTime(serial, FormatId::DATE)
            }
            LiteralValue::DateTime(dt) => {
                let serial = formualizer_common::datetime_to_serial_for(date_system, dt);
                Self::DateTime(serial, FormatId::DATETIME)
            }
            LiteralValue::Time(t) => {
                let serial = formualizer_common::time_to_fraction(t);
                Self::DateTime(serial, FormatId::TIME)
            }
            LiteralValue::Duration(d) => {
                let serial = d.num_seconds() as f64 / 86_400.0;
                Self::Duration(serial, FormatId::DURATION)
            }
            LiteralValue::Pending => Self::Pending,
            LiteralValue::Array(_) => Self::Error(map_error_code(ExcelErrorKind::Value)),
        }
    }

    #[inline]
    fn format_id(self) -> FormatId {
        match self {
            Self::Number(_, fmt) | Self::DateTime(_, fmt) | Self::Duration(_, fmt) => fmt,
            _ => FormatId::GENERAL,
        }
    }

    #[inline]
    fn format_opt(self) -> Option<FormatId> {
        let f = self.format_id();
        (f != FormatId::GENERAL).then_some(f)
    }

    #[inline]
    fn to_overlay_value(self) -> OverlayValue {
        match self {
            Self::Empty => OverlayValue::Empty,
            Self::Number(n, _) => OverlayValue::Number(n),
            Self::DateTime(s, _) => OverlayValue::DateTime(s),
            Self::Duration(s, _) => OverlayValue::Duration(s),
            Self::Boolean(b) => OverlayValue::Boolean(b),
            Self::Text(s) => OverlayValue::Text(Arc::from(s)),
            Self::Error(code) => OverlayValue::Error(code),
            Self::Pending => OverlayValue::Pending,
        }
    }

    #[inline]
    fn is_empty(self) -> bool {
        matches!(self, Self::Empty)
    }
}

#[inline]
fn decode_bulk_column_cell<'a>(
    col: &BulkColumn<'a>,
    idx: usize,
    serial_tag: TypeTag,
) -> DecodedCell<'a> {
    match col {
        BulkColumn::Float64 { values, validity } => {
            if !validity_is_valid(*validity, idx) {
                DecodedCell::Empty
            } else {
                let v = values[idx];
                if v.is_finite() {
                    DecodedCell::Number(v, FormatId::GENERAL)
                } else {
                    DecodedCell::Error(map_error_code(ExcelErrorKind::Num))
                }
            }
        }
        BulkColumn::Int64 { values, validity } => {
            if !validity_is_valid(*validity, idx) {
                DecodedCell::Empty
            } else {
                DecodedCell::Number(values[idx] as f64, FormatId::GENERAL)
            }
        }
        BulkColumn::Boolean { values, validity } => {
            if !validity_is_valid(*validity, idx) {
                DecodedCell::Empty
            } else {
                DecodedCell::Boolean(values[idx])
            }
        }
        BulkColumn::Utf8 {
            bytes,
            offsets,
            validity,
        } => {
            if !validity_is_valid(*validity, idx) {
                DecodedCell::Empty
            } else {
                let s0 = offsets[idx] as usize;
                let s1 = offsets[idx + 1] as usize;
                let s = std::str::from_utf8(&bytes[s0..s1])
                    .expect("validated UTF-8 string in preflight");
                DecodedCell::Text(s)
            }
        }
        BulkColumn::Serial {
            values,
            format,
            validity,
        } => {
            if !validity_is_valid(*validity, idx) {
                DecodedCell::Empty
            } else {
                let v = values[idx];
                if !v.is_finite() {
                    DecodedCell::Error(map_error_code(ExcelErrorKind::Num))
                } else {
                    match serial_tag {
                        TypeTag::DateTime => DecodedCell::DateTime(v, *format),
                        TypeTag::Duration => DecodedCell::Duration(v, *format),
                        _ => DecodedCell::Number(v, *format),
                    }
                }
            }
        }
        BulkColumn::Null { .. } => DecodedCell::Empty,
        BulkColumn::Mixed(block) => decode_wire_cell(
            block.tags[idx],
            block.numbers[idx],
            block.text_bytes,
            block.text_offsets,
        ),
    }
}

/// Lazy multi-lane chunk builder that only allocates Arrow builders for lanes
/// that actually receive non-null values.
struct LazyChunkBuilders {
    len: usize,
    row: usize,
    tags: Vec<u8>,
    nums: Option<Float64Builder>,
    bools: Option<BooleanBuilder>,
    texts: Option<StringBuilder>,
    errs: Option<UInt8Builder>,
    non_num: usize,
    non_bool: usize,
    non_text: usize,
    non_err: usize,
}

impl LazyChunkBuilders {
    fn with_capacity(len: usize) -> Self {
        Self {
            len,
            row: 0,
            tags: Vec::with_capacity(len),
            nums: None,
            bools: None,
            texts: None,
            errs: None,
            non_num: 0,
            non_bool: 0,
            non_text: 0,
            non_err: 0,
        }
    }

    #[inline]
    fn ensure_nums(&mut self) -> &mut Float64Builder {
        let len = self.len;
        let row = self.row;
        self.nums.get_or_insert_with(|| {
            let mut b = Float64Builder::with_capacity(len);
            if row > 0 {
                b.append_nulls(row);
            }
            b
        })
    }

    #[inline]
    fn ensure_bools(&mut self) -> &mut BooleanBuilder {
        let len = self.len;
        let row = self.row;
        self.bools.get_or_insert_with(|| {
            let mut b = BooleanBuilder::with_capacity(len);
            if row > 0 {
                b.append_nulls(row);
            }
            b
        })
    }

    #[inline]
    fn ensure_texts(&mut self) -> &mut StringBuilder {
        let len = self.len;
        let row = self.row;
        self.texts.get_or_insert_with(|| {
            let mut b = StringBuilder::with_capacity(len, len.saturating_mul(8));
            for _ in 0..row {
                b.append_null();
            }
            b
        })
    }

    #[inline]
    fn ensure_errs(&mut self) -> &mut UInt8Builder {
        let len = self.len;
        let row = self.row;
        self.errs.get_or_insert_with(|| {
            let mut b = UInt8Builder::with_capacity(len);
            if row > 0 {
                b.append_nulls(row);
            }
            b
        })
    }

    #[inline]
    fn push_cell(&mut self, cell: DecodedCell<'_>) {
        match cell {
            DecodedCell::Empty => {
                self.tags.push(TypeTag::Empty as u8);
                if let Some(b) = &mut self.nums {
                    b.append_null();
                }
                if let Some(b) = &mut self.bools {
                    b.append_null();
                }
                if let Some(b) = &mut self.texts {
                    b.append_null();
                }
                if let Some(b) = &mut self.errs {
                    b.append_null();
                }
            }
            DecodedCell::Number(n, _) => {
                self.tags.push(TypeTag::Number as u8);
                self.ensure_nums().append_value(n);
                self.non_num += 1;
                if let Some(b) = &mut self.bools {
                    b.append_null();
                }
                if let Some(b) = &mut self.texts {
                    b.append_null();
                }
                if let Some(b) = &mut self.errs {
                    b.append_null();
                }
            }
            DecodedCell::DateTime(s, _) => {
                self.tags.push(TypeTag::DateTime as u8);
                self.ensure_nums().append_value(s);
                self.non_num += 1;
                if let Some(b) = &mut self.bools {
                    b.append_null();
                }
                if let Some(b) = &mut self.texts {
                    b.append_null();
                }
                if let Some(b) = &mut self.errs {
                    b.append_null();
                }
            }
            DecodedCell::Duration(s, _) => {
                self.tags.push(TypeTag::Duration as u8);
                self.ensure_nums().append_value(s);
                self.non_num += 1;
                if let Some(b) = &mut self.bools {
                    b.append_null();
                }
                if let Some(b) = &mut self.texts {
                    b.append_null();
                }
                if let Some(b) = &mut self.errs {
                    b.append_null();
                }
            }
            DecodedCell::Boolean(v) => {
                self.tags.push(TypeTag::Boolean as u8);
                if let Some(b) = &mut self.nums {
                    b.append_null();
                }
                self.ensure_bools().append_value(v);
                self.non_bool += 1;
                if let Some(b) = &mut self.texts {
                    b.append_null();
                }
                if let Some(b) = &mut self.errs {
                    b.append_null();
                }
            }
            DecodedCell::Text(s) => {
                self.tags.push(TypeTag::Text as u8);
                if let Some(b) = &mut self.nums {
                    b.append_null();
                }
                if let Some(b) = &mut self.bools {
                    b.append_null();
                }
                self.ensure_texts().append_value(s);
                self.non_text += 1;
                if let Some(b) = &mut self.errs {
                    b.append_null();
                }
            }
            DecodedCell::Error(code) => {
                self.tags.push(TypeTag::Error as u8);
                if let Some(b) = &mut self.nums {
                    b.append_null();
                }
                if let Some(b) = &mut self.bools {
                    b.append_null();
                }
                if let Some(b) = &mut self.texts {
                    b.append_null();
                }
                self.ensure_errs().append_value(code);
                self.non_err += 1;
            }
            DecodedCell::Pending => {
                self.tags.push(TypeTag::Pending as u8);
                if let Some(b) = &mut self.nums {
                    b.append_null();
                }
                if let Some(b) = &mut self.bools {
                    b.append_null();
                }
                if let Some(b) = &mut self.texts {
                    b.append_null();
                }
                if let Some(b) = &mut self.errs {
                    b.append_null();
                }
            }
        }
        self.row += 1;
    }

    #[inline]
    fn push_base_cell_from_chunk(&mut self, ch: &ColumnChunk, i: usize) {
        let tag = TypeTag::from_u8(ch.type_tag.value(i));
        match tag {
            TypeTag::Empty => self.push_cell(DecodedCell::Empty),
            TypeTag::Number => {
                if let Some(a) = &ch.numbers
                    && !a.is_null(i)
                {
                    self.push_cell(DecodedCell::Number(a.value(i), FormatId::GENERAL));
                } else {
                    self.push_cell(DecodedCell::Empty);
                }
            }
            TypeTag::DateTime => {
                if let Some(a) = &ch.numbers
                    && !a.is_null(i)
                {
                    self.push_cell(DecodedCell::DateTime(a.value(i), FormatId::GENERAL));
                } else {
                    self.push_cell(DecodedCell::Empty);
                }
            }
            TypeTag::Duration => {
                if let Some(a) = &ch.numbers
                    && !a.is_null(i)
                {
                    self.push_cell(DecodedCell::Duration(a.value(i), FormatId::GENERAL));
                } else {
                    self.push_cell(DecodedCell::Empty);
                }
            }
            TypeTag::Boolean => {
                if let Some(a) = &ch.booleans
                    && !a.is_null(i)
                {
                    self.push_cell(DecodedCell::Boolean(a.value(i)));
                } else {
                    self.push_cell(DecodedCell::Empty);
                }
            }
            TypeTag::Text => {
                if let Some(a) = &ch.text
                    && !a.is_null(i)
                {
                    let sa = a.as_any().downcast_ref::<StringArray>().unwrap();
                    self.push_cell(DecodedCell::Text(sa.value(i)));
                } else {
                    self.push_cell(DecodedCell::Empty);
                }
            }
            TypeTag::Error => {
                if let Some(a) = &ch.errors
                    && !a.is_null(i)
                {
                    self.push_cell(DecodedCell::Error(a.value(i)));
                } else {
                    self.push_cell(DecodedCell::Empty);
                }
            }
            TypeTag::Pending => self.push_cell(DecodedCell::Pending),
        }
    }

    fn finish(
        self,
        format: Option<FormatRuns>,
        overlay: Overlay,
        computed_overlay: Overlay,
    ) -> ColumnChunk {
        debug_assert_eq!(self.row, self.len);
        let type_tag = Arc::new(UInt8Array::from(self.tags));
        let numbers = self
            .nums
            .and_then(|mut b| (self.non_num > 0).then(|| Arc::new(b.finish())));
        let booleans = self
            .bools
            .and_then(|mut b| (self.non_bool > 0).then(|| Arc::new(b.finish())));
        let text = self
            .texts
            .and_then(|mut b| (self.non_text > 0).then(|| Arc::new(b.finish()) as ArrayRef));
        let errors = self
            .errs
            .and_then(|mut b| (self.non_err > 0).then(|| Arc::new(b.finish())));
        let meta = ColumnChunkMeta {
            len: self.len,
            non_null_num: self.non_num,
            non_null_bool: self.non_bool,
            non_null_text: self.non_text,
            non_null_err: self.non_err,
        };
        ArrowSheet::build_chunk(
            type_tag,
            numbers,
            booleans,
            text,
            errors,
            format,
            meta,
            overlay,
            computed_overlay,
        )
    }
}

/// Fast full-chunk constructor for a `BulkColumn` slice `[col_off .. col_off + chunk_len)`.
/// Returns `None` if the slice is completely empty (all-Empty + all-GENERAL) so callers
/// can avoid allocating a chunk on previously-unmaterialized columns.
fn build_full_chunk_from_column(
    col: &BulkColumn<'_>,
    col_off: usize,
    chunk_len: usize,
    serial_tag: TypeTag,
) -> Option<ColumnChunk> {
    match col {
        BulkColumn::Null { .. } => None,
        BulkColumn::Float64 { values, validity } => {
            let slice = &values[col_off..col_off + chunk_len];
            if validity.is_none() && slice.iter().all(|v| v.is_finite()) {
                let type_tag = Arc::new(UInt8Array::from(vec![TypeTag::Number as u8; chunk_len]));
                let numbers = Some(Arc::new(Float64Array::from(slice.to_vec())));
                let meta = ColumnChunkMeta {
                    len: chunk_len,
                    non_null_num: chunk_len,
                    non_null_bool: 0,
                    non_null_text: 0,
                    non_null_err: 0,
                };
                return Some(ArrowSheet::build_chunk(
                    type_tag,
                    numbers,
                    None,
                    None,
                    None,
                    None,
                    meta,
                    Overlay::new(),
                    Overlay::new(),
                ));
            }
            let mut b = LazyChunkBuilders::with_capacity(chunk_len);
            for i in 0..chunk_len {
                let idx = col_off + i;
                b.push_cell(decode_bulk_column_cell(col, idx, serial_tag));
            }
            if b.non_num == 0 && b.non_err == 0 {
                return None;
            }
            Some(b.finish(None, Overlay::new(), Overlay::new()))
        }
        BulkColumn::Int64 { values, validity } => {
            let slice = &values[col_off..col_off + chunk_len];
            if validity.is_none() {
                let type_tag = Arc::new(UInt8Array::from(vec![TypeTag::Number as u8; chunk_len]));
                let nums: Vec<f64> = slice.iter().map(|&v| v as f64).collect();
                let numbers = Some(Arc::new(Float64Array::from(nums)));
                let meta = ColumnChunkMeta {
                    len: chunk_len,
                    non_null_num: chunk_len,
                    non_null_bool: 0,
                    non_null_text: 0,
                    non_null_err: 0,
                };
                return Some(ArrowSheet::build_chunk(
                    type_tag,
                    numbers,
                    None,
                    None,
                    None,
                    None,
                    meta,
                    Overlay::new(),
                    Overlay::new(),
                ));
            }
            let mut b = LazyChunkBuilders::with_capacity(chunk_len);
            for i in 0..chunk_len {
                b.push_cell(decode_bulk_column_cell(col, col_off + i, serial_tag));
            }
            if b.non_num == 0 {
                return None;
            }
            Some(b.finish(None, Overlay::new(), Overlay::new()))
        }
        BulkColumn::Boolean { values, validity } => {
            let mut tags = Vec::with_capacity(chunk_len);
            let mut bb = BooleanBuilder::with_capacity(chunk_len);
            let mut non_bool = 0usize;
            for i in 0..chunk_len {
                let idx = col_off + i;
                if validity_is_valid(*validity, idx) {
                    tags.push(TypeTag::Boolean as u8);
                    bb.append_value(values[idx]);
                    non_bool += 1;
                } else {
                    tags.push(TypeTag::Empty as u8);
                    bb.append_null();
                }
            }
            if non_bool == 0 {
                return None;
            }
            let meta = ColumnChunkMeta {
                len: chunk_len,
                non_null_num: 0,
                non_null_bool: non_bool,
                non_null_text: 0,
                non_null_err: 0,
            };
            Some(ArrowSheet::build_chunk(
                Arc::new(UInt8Array::from(tags)),
                None,
                Some(Arc::new(bb.finish())),
                None,
                None,
                None,
                meta,
                Overlay::new(),
                Overlay::new(),
            ))
        }
        BulkColumn::Utf8 {
            bytes,
            offsets,
            validity,
        } => {
            let total_bytes =
                (offsets[col_off + chunk_len] as usize).saturating_sub(offsets[col_off] as usize);
            let mut tags = Vec::with_capacity(chunk_len);
            let mut sb = StringBuilder::with_capacity(chunk_len, total_bytes);
            let mut non_text = 0usize;
            for i in 0..chunk_len {
                let idx = col_off + i;
                if validity_is_valid(*validity, idx) {
                    let s0 = offsets[idx] as usize;
                    let s1 = offsets[idx + 1] as usize;
                    let s = std::str::from_utf8(&bytes[s0..s1])
                        .expect("validated UTF-8 string in preflight");
                    tags.push(TypeTag::Text as u8);
                    sb.append_value(s);
                    non_text += 1;
                } else {
                    tags.push(TypeTag::Empty as u8);
                    sb.append_null();
                }
            }
            if non_text == 0 {
                return None;
            }
            let meta = ColumnChunkMeta {
                len: chunk_len,
                non_null_num: 0,
                non_null_bool: 0,
                non_null_text: non_text,
                non_null_err: 0,
            };
            Some(ArrowSheet::build_chunk(
                Arc::new(UInt8Array::from(tags)),
                None,
                None,
                Some(Arc::new(sb.finish()) as ArrayRef),
                None,
                None,
                meta,
                Overlay::new(),
                Overlay::new(),
            ))
        }
        BulkColumn::Serial {
            values,
            format,
            validity,
        } => {
            let slice = &values[col_off..col_off + chunk_len];
            if validity.is_none() && slice.iter().all(|v| v.is_finite()) {
                let type_tag = Arc::new(UInt8Array::from(vec![serial_tag as u8; chunk_len]));
                let numbers = Some(Arc::new(Float64Array::from(slice.to_vec())));
                let fmt =
                    FormatRuns::replace_uniform_range(None, chunk_len, 0, chunk_len, format.0);
                let meta = ColumnChunkMeta {
                    len: chunk_len,
                    non_null_num: chunk_len,
                    non_null_bool: 0,
                    non_null_text: 0,
                    non_null_err: 0,
                };
                return Some(ArrowSheet::build_chunk(
                    type_tag,
                    numbers,
                    None,
                    None,
                    None,
                    fmt,
                    meta,
                    Overlay::new(),
                    Overlay::new(),
                ));
            }
            let mut b = LazyChunkBuilders::with_capacity(chunk_len);
            let mut fmt_ids = (*format != FormatId::GENERAL).then(|| Vec::with_capacity(chunk_len));
            for i in 0..chunk_len {
                let cell = decode_bulk_column_cell(col, col_off + i, serial_tag);
                if let Some(ids) = fmt_ids.as_mut() {
                    ids.push(cell.format_id().0);
                }
                b.push_cell(cell);
            }
            if b.non_num == 0 && b.non_err == 0 {
                return None;
            }
            let fmt = fmt_ids.and_then(|ids| FormatRuns::from_ids(&ids));
            Some(b.finish(fmt, Overlay::new(), Overlay::new()))
        }
        BulkColumn::Mixed(_) => {
            let mut b = LazyChunkBuilders::with_capacity(chunk_len);
            let mut fmt_ids: Option<Vec<u16>> = None;
            for i in 0..chunk_len {
                let cell = decode_bulk_column_cell(col, col_off + i, serial_tag);
                let fmt = cell.format_id().0;
                if fmt != FormatId::GENERAL.0 && fmt_ids.is_none() {
                    let mut v = Vec::with_capacity(chunk_len);
                    v.resize(i, FormatId::GENERAL.0);
                    fmt_ids = Some(v);
                }
                if let Some(ids) = fmt_ids.as_mut() {
                    ids.push(fmt);
                }
                b.push_cell(cell);
            }
            if b.non_num == 0 && b.non_bool == 0 && b.non_text == 0 && b.non_err == 0 {
                return None;
            }
            let fmt = fmt_ids.and_then(|ids| FormatRuns::from_ids(&ids));
            Some(b.finish(fmt, Overlay::new(), Overlay::new()))
        }
    }
}

/// Fast full-chunk constructor for a rectangular slice of `rows[row_off..row_off + chunk_len][c]`.
/// Returns `None` if every cell in the slice is `Empty`.
/// Returns `Err(())` if any cell contains `ExcelErrorExtra::FunctionPanicked` (rare diagnostic
/// error that must stay in overlay to preserve its typed payload).
fn build_full_chunk_from_literal_col(
    rows: &[Vec<LiteralValue>],
    row_off: usize,
    chunk_len: usize,
    c: usize,
    date_system: DateSystem,
) -> Result<Option<ColumnChunk>, ()> {
    // Fast numeric scan: common case (e.g. 10k x 10 numeric) builds a single Float64Array
    // directly with zero builders.
    let mut all_finite_number = true;
    for r in &rows[row_off..row_off + chunk_len] {
        match &r[c] {
            LiteralValue::Number(n) if n.is_finite() => {}
            LiteralValue::Int(_) => {}
            LiteralValue::Error(e)
                if matches!(
                    e.extra(),
                    formualizer_common::ExcelErrorExtra::FunctionPanicked { .. }
                ) =>
            {
                return Err(());
            }
            _ => {
                all_finite_number = false;
                break;
            }
        }
    }
    if all_finite_number {
        let mut nums = Vec::with_capacity(chunk_len);
        for r in &rows[row_off..row_off + chunk_len] {
            match &r[c] {
                LiteralValue::Number(n) => nums.push(*n),
                LiteralValue::Int(i) => nums.push(*i as f64),
                _ => unreachable!(),
            }
        }
        let type_tag = Arc::new(UInt8Array::from(vec![TypeTag::Number as u8; chunk_len]));
        let numbers = Some(Arc::new(Float64Array::from(nums)));
        let meta = ColumnChunkMeta {
            len: chunk_len,
            non_null_num: chunk_len,
            non_null_bool: 0,
            non_null_text: 0,
            non_null_err: 0,
        };
        return Ok(Some(ArrowSheet::build_chunk(
            type_tag,
            numbers,
            None,
            None,
            None,
            None,
            meta,
            Overlay::new(),
            Overlay::new(),
        )));
    }

    let mut b = LazyChunkBuilders::with_capacity(chunk_len);
    let mut fmt_ids: Option<Vec<u16>> = None;
    let mut any_non_empty = false;
    for (i, r) in rows[row_off..row_off + chunk_len].iter().enumerate() {
        let val = &r[c];
        if let LiteralValue::Error(e) = val
            && matches!(
                e.extra(),
                formualizer_common::ExcelErrorExtra::FunctionPanicked { .. }
            )
        {
            return Err(());
        }
        let cell = DecodedCell::from_literal(val, date_system);
        if !cell.is_empty() {
            any_non_empty = true;
        }
        let fmt = cell.format_id().0;
        if fmt != FormatId::GENERAL.0 && fmt_ids.is_none() {
            let mut v = Vec::with_capacity(chunk_len);
            v.resize(i, FormatId::GENERAL.0);
            fmt_ids = Some(v);
        }
        if let Some(ids) = fmt_ids.as_mut() {
            ids.push(fmt);
        }
        b.push_cell(cell);
    }
    if !any_non_empty {
        return Ok(None);
    }
    let fmt = fmt_ids.and_then(|ids| FormatRuns::from_ids(&ids));
    Ok(Some(b.finish(fmt, Overlay::new(), Overlay::new())))
}

/// Rebuild a chunk by preserving untouched rows' base lanes, overlays, and formats verbatim
/// while writing new dense cells across `[lo..hi)` and clearing overlays/formats on `[lo..hi)`.
/// Returns `(new_chunk, computed_overlay_bytes_delta)`.
fn rebuild_partial_dense_chunk<'a>(
    existing: Option<ColumnChunk>,
    chunk_len: usize,
    lo: usize,
    hi: usize,
    mut cell_at: impl FnMut(usize) -> DecodedCell<'a>,
) -> (Option<ColumnChunk>, isize) {
    let mut computed_delta = 0isize;
    let Some(mut old_ch) = existing else {
        let mut b = LazyChunkBuilders::with_capacity(chunk_len);
        let mut fmt_ids: Option<Vec<u16>> = None;
        let mut any_non_empty = false;
        for _ in 0..lo {
            b.push_cell(DecodedCell::Empty);
        }
        for off in lo..hi {
            let cell = cell_at(off);
            if !cell.is_empty() {
                any_non_empty = true;
            }
            let fmt = cell.format_id().0;
            if fmt != FormatId::GENERAL.0 && fmt_ids.is_none() {
                let mut v = Vec::with_capacity(hi - lo);
                v.resize(off - lo, FormatId::GENERAL.0);
                fmt_ids = Some(v);
            }
            if let Some(ids) = fmt_ids.as_mut() {
                ids.push(fmt);
            }
            b.push_cell(cell);
        }
        if !any_non_empty {
            return (None, 0);
        }
        for _ in hi..chunk_len {
            b.push_cell(DecodedCell::Empty);
        }
        let fmt = fmt_ids.and_then(|ids| FormatRuns::replace_range(None, chunk_len, lo, &ids));
        return (Some(b.finish(fmt, Overlay::new(), Overlay::new())), 0);
    };

    // Remove touched range from both overlays (values AND formats) while
    // preserving untouched offsets verbatim.
    let _ = old_ch.overlay.remove_range(lo..hi);
    old_ch.overlay.clear_format_range(lo, hi);
    computed_delta = computed_delta.saturating_add(old_ch.computed_overlay.remove_range(lo..hi));
    old_ch.computed_overlay.clear_format_range(lo, hi);

    let mut b = LazyChunkBuilders::with_capacity(chunk_len);
    let mut touched_fmts: Option<Vec<u16>> = None;
    for i in 0..lo {
        b.push_base_cell_from_chunk(&old_ch, i);
    }
    for off in lo..hi {
        let cell = cell_at(off);
        let fmt = cell.format_id().0;
        if fmt != FormatId::GENERAL.0 && touched_fmts.is_none() {
            let mut v = Vec::with_capacity(hi - lo);
            v.resize(off - lo, FormatId::GENERAL.0);
            touched_fmts = Some(v);
        }
        if let Some(ids) = touched_fmts.as_mut() {
            ids.push(fmt);
        }
        b.push_cell(cell);
    }
    for i in hi..chunk_len {
        b.push_base_cell_from_chunk(&old_ch, i);
    }

    let new_format = match touched_fmts {
        Some(ids) => FormatRuns::replace_range(old_ch.format.as_ref(), chunk_len, lo, &ids),
        None => old_ch
            .format
            .as_ref()
            .and_then(|runs| runs.clear_range(lo, hi)),
    };

    let new_chunk = b.finish(new_format, old_ch.overlay, old_ch.computed_overlay);
    (Some(new_chunk), computed_delta)
}

/// Rebuild a chunk for a heavy sparse update (sorted unique `in_off` entries).
/// Preserves untouched rows' base lanes, overlays, and formats verbatim.
fn rebuild_sparse_chunk<'a>(
    existing: Option<ColumnChunk>,
    chunk_len: usize,
    sorted_writes: &[(usize, DecodedCell<'a>)],
) -> (Option<ColumnChunk>, isize) {
    let mut computed_delta = 0isize;
    let Some(mut old_ch) = existing else {
        if sorted_writes.iter().all(|(_, c)| c.is_empty()) {
            return (None, 0);
        }
        let mut b = LazyChunkBuilders::with_capacity(chunk_len);
        let mut fmt_ids: Option<Vec<u16>> = None;
        let mut wi = 0usize;
        for i in 0..chunk_len {
            if wi < sorted_writes.len() && sorted_writes[wi].0 == i {
                let cell = sorted_writes[wi].1;
                wi += 1;
                let fmt = cell.format_id().0;
                if fmt != FormatId::GENERAL.0 && fmt_ids.is_none() {
                    let mut v = vec![FormatId::GENERAL.0; chunk_len];
                    v[i] = fmt;
                    fmt_ids = Some(v);
                } else if let Some(ids) = fmt_ids.as_mut() {
                    ids[i] = fmt;
                }
                b.push_cell(cell);
            } else {
                b.push_cell(DecodedCell::Empty);
            }
        }
        let fmt = fmt_ids.and_then(|ids| FormatRuns::from_ids(&ids));
        return (Some(b.finish(fmt, Overlay::new(), Overlay::new())), 0);
    };

    let mut any_new_fmt = false;
    for &(off, cell) in sorted_writes {
        let _ = old_ch.overlay.remove(off);
        old_ch.overlay.set_format(off, None);
        computed_delta = computed_delta.saturating_add(old_ch.computed_overlay.remove(off));
        old_ch.computed_overlay.set_format(off, None);
        if cell.format_id() != FormatId::GENERAL {
            any_new_fmt = true;
        }
    }

    let new_format = if any_new_fmt {
        let mut ids = old_ch
            .format
            .as_ref()
            .map(|r| r.to_ids(chunk_len))
            .unwrap_or_else(|| vec![FormatId::GENERAL.0; chunk_len]);
        for &(off, cell) in sorted_writes {
            ids[off] = cell.format_id().0;
        }
        FormatRuns::from_ids(&ids)
    } else if let Some(runs) = old_ch.format.as_ref() {
        let offsets: Vec<usize> = sorted_writes.iter().map(|&(o, _)| o).collect();
        runs.clear_offsets(&offsets)
    } else {
        None
    };

    let mut b = LazyChunkBuilders::with_capacity(chunk_len);
    let mut wi = 0usize;
    for i in 0..chunk_len {
        if wi < sorted_writes.len() && sorted_writes[wi].0 == i {
            b.push_cell(sorted_writes[wi].1);
            wi += 1;
        } else {
            b.push_base_cell_from_chunk(&old_ch, i);
        }
    }
    let new_chunk = b.finish(new_format, old_ch.overlay, old_ch.computed_overlay);
    (Some(new_chunk), computed_delta)
}

#[inline]
fn should_rebuild_chunk(chunk_len: usize, touched: usize) -> bool {
    let threshold = (chunk_len / 50).min(1024);
    touched > threshold
}

/// Group rows in `rows[..=last_non_empty]` into maximal consecutive 0-based rectangles
/// `(r0, r1, c0, c1)` of equal non-zero width. For rectangular grids this emits 1 rectangle.
fn maximal_row_rects(
    start_row0: u32,
    start_col0: u32,
    rows: &[Vec<LiteralValue>],
) -> Vec<(u32, u32, u32, u32)> {
    let mut rects: Vec<(u32, u32, u32, u32)> = Vec::new();
    for (ri, row) in rows.iter().enumerate() {
        if row.is_empty() {
            continue;
        }
        let r = start_row0 + ri as u32;
        let c0 = start_col0;
        let c1 = start_col0 + (row.len() as u32 - 1);
        if let Some(last) = rects.last_mut()
            && last.1 + 1 == r
            && last.2 == c0
            && last.3 == c1
        {
            last.1 = r;
        } else {
            rects.push((r, r, c0, c1));
        }
    }
    rects
}

/// Convert sorted deduplicated `(col0, row0)` points into sorted 0-based column runs `(col0, r0, r1)`.
fn coalesce_sorted_col_runs(points_col_row: &[(u32, u32)]) -> Vec<(u32, u32, u32)> {
    let mut runs: Vec<(u32, u32, u32)> = Vec::new();
    for &(c, r) in points_col_row {
        if let Some(last) = runs.last_mut()
            && last.0 == c
            && last.2 + 1 == r
        {
            last.2 = r;
        } else {
            runs.push((c, r, r));
        }
    }
    runs
}

impl<R> Engine<R>
where
    R: EvaluationContext,
{
    fn check_bulk_write_limits(
        &self,
        sheet: &str,
        max_row: u32,
        max_col: u32,
        written_cells: usize,
    ) -> Result<(), ExcelError> {
        if max_row == 0 || max_col == 0 {
            return Ok(());
        }
        let (cur_rows, cur_cols) = self
            .sheet_store()
            .sheet(sheet)
            .map(|s| (s.nrows, s.columns.len() as u32))
            .unwrap_or((0, 0));
        let target_rows = cur_rows.max(max_row);
        let target_cols = cur_cols.max(max_col);
        let limits = self.workbook_load_limits();
        if target_rows > limits.max_sheet_rows {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                "write extends sheet {sheet} to {target_rows} rows, which exceeds the configured write row budget of {}",
                limits.max_sheet_rows
            )));
        }
        if target_cols > limits.max_sheet_cols {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                "write extends sheet {sheet} to {target_cols} columns, which exceeds the configured write column budget of {}",
                limits.max_sheet_cols
            )));
        }
        if written_cells as u64 > limits.max_sheet_logical_cells {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                "write batch of {written_cells} cells on sheet {sheet} exceeds the configured write cell budget of {}",
                limits.max_sheet_logical_cells
            )));
        }
        Ok(())
    }

    fn resolve_serial_type_tag(&self, format: FormatId) -> TypeTag {
        match self.format_registry.class(format) {
            Some(FormatClass::Duration) => TypeTag::Duration,
            Some(FormatClass::Date | FormatClass::DateTime | FormatClass::Time) => {
                TypeTag::DateTime
            }
            _ => TypeTag::Number,
        }
    }

    fn ensure_arrow_sheet_exists(&mut self, sheet: &str, min_rows: usize, min_cols: usize) {
        let date_system = self.config.date_system;
        let store = self.sheet_store_mut();
        if store.sheet(sheet).is_none() {
            store.sheets.push(ArrowSheet {
                name: Arc::<str>::from(sheet),
                date_system,
                columns: Vec::new(),
                nrows: 0,
                chunk_starts: Vec::new(),
                chunk_rows: 32 * 1024,
            });
        }
        let asheet = store.sheet_mut(sheet).expect("ArrowSheet must exist");
        asheet.ensure_column_capacity_lazy(min_cols);
        if min_rows > asheet.nrows as usize {
            asheet.ensure_row_capacity(min_rows);
        }
    }

    /// Reconcile vertices, spill anchors, spill children, staged formulas,
    /// empty tables, and derived formats across 0-based inclusive `rects`.
    /// Returns `(replaced_formula, resized_table)`.
    fn reconcile_rects(
        &mut self,
        sheet: &str,
        sheet_id: crate::SheetId,
        rects: &[(u32, u32, u32, u32)],
        empty_tables: &[crate::engine::graph::TableEntry],
    ) -> Result<(bool, bool), ExcelError> {
        let mut replaced_formula = false;

        // 1. E3: Range-query ALL vertices in each rectangle, clear spill anchors first,
        //    then call vacate_cell on every vertex.
        for &(r0, r1, c0, c1) in rects {
            let vertices = self.graph.vertices_in_region(sheet_id, r0, r1, c0, c1);
            for v in vertices {
                if !self.graph.vertex_exists(v) {
                    continue;
                }
                let Some(addr) = self.graph.get_cell_ref(v) else {
                    continue;
                };
                let is_formula = matches!(
                    self.graph.get_vertex_kind(v),
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
                );
                if is_formula {
                    replaced_formula = true;
                    if self.graph.spill_registry_has_anchor(v) {
                        self.clear_spill_projection_and_mirror(v, None);
                    }
                }
                self.graph.vacate_cell(&addr);
            }
        }

        // 2. B1: Per-child spill release for spill children owned by anchors outside
        //    the region, followed by StructuralScope::Region waiter/pending wakeups.
        for &(r0, r1, c0, c1) in rects {
            let dirtied_owners = self
                .graph
                .release_spill_children_in_rect(sheet_id, r0, r1, c0, c1);
            if !dirtied_owners.is_empty() {
                self.graph.mark_dirty_many(&dirtied_owners);
            }
            self.record_structural_change(StructuralScope::Region(Region::rect(
                sheet_id, r0, r1, c0, c1,
            )));
        }

        // 3. E6: Clear ordinary staged formulas and range-suppress deferred packages.
        if self.staged_formulas.contains_key(sheet) {
            for &(r0, r1, c0, c1) in rects {
                self.clear_staged_formulas_in_rect(sheet, r0 + 1, r1 + 1, c0 + 1, c1 + 1);
            }
        }

        // 4. E5: Apply preflighted empty-table resizes.
        let mut resized_table = false;
        for table in empty_tables {
            self.graph.resize_table(&table.name, table.range)?;
            resized_table = true;
        }

        // 5. B3: Clear derived formats intersecting rects.
        if !self.derived_formats.is_empty() {
            self.derived_formats.retain(|cell| {
                if cell.sheet_id != sheet_id {
                    return true;
                }
                let r = cell.coord.row();
                let c = cell.coord.col();
                !rects
                    .iter()
                    .any(|&(r0, r1, c0, c1)| r >= r0 && r <= r1 && c >= c0 && c <= c1)
            });
        }

        Ok((replaced_formula, resized_table))
    }

    /// Unified engine bulk write for dense, column-major typed columns.
    pub fn set_region_columns(
        &mut self,
        sheet: &str,
        start_row: u32,
        start_col: u32,
        columns: &[BulkColumn<'_>],
    ) -> Result<WriteRangeReport, ExcelError> {
        // ── Phase 1: Atomic Preflight (zero mutation before success) ──
        self.ensure_not_poisoned()?;
        let start_coord = Self::try_excel_coord(sheet, start_row, start_col)?;
        self.observe_function_semantic_epoch()?;

        if columns.is_empty() {
            return Ok(WriteRangeReport::default());
        }

        let nrows = columns[0].len();
        for (ci, col) in columns.iter().enumerate() {
            if col.len() != nrows {
                return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                    "Column {ci} length ({}) does not match column 0 length ({nrows})",
                    col.len()
                )));
            }
            match col {
                BulkColumn::Float64 { values, validity } => {
                    validate_validity_bitmap(*validity, values.len(), ci)?;
                }
                BulkColumn::Int64 { values, validity } => {
                    validate_validity_bitmap(*validity, values.len(), ci)?;
                    for (ri, &v) in values.iter().enumerate() {
                        if validity_is_valid(*validity, ri)
                            && !(-MAX_SAFE_F64_INT..=MAX_SAFE_F64_INT).contains(&v)
                        {
                            return Err(ExcelError::new(ExcelErrorKind::Num).with_message(format!(
                                "Column {ci} row {ri}: 64-bit integer {v} exceeds exact f64 range (±2^53); cast explicitly to text or float"
                            )));
                        }
                    }
                }
                BulkColumn::Boolean { values, validity } => {
                    validate_validity_bitmap(*validity, values.len(), ci)?;
                }
                BulkColumn::Utf8 {
                    bytes,
                    offsets,
                    validity,
                } => {
                    validate_validity_bitmap(*validity, offsets.len().saturating_sub(1), ci)?;
                    validate_utf8_offsets(bytes, offsets, *validity, &format!("Column {ci}"))?;
                }
                BulkColumn::Serial {
                    values, validity, ..
                } => {
                    validate_validity_bitmap(*validity, values.len(), ci)?;
                }
                BulkColumn::Null { .. } => {}
                BulkColumn::Mixed(block) => {
                    validate_wire_cells(
                        block.tags,
                        block.numbers,
                        block.text_bytes,
                        block.text_offsets,
                        &format!("Column {ci}"),
                    )?;
                }
            }
        }

        if nrows == 0 {
            return Ok(WriteRangeReport::default());
        }

        let ncols = columns.len();
        let row_span = u32::try_from(nrows - 1).map_err(|_| {
            ExcelError::new(ExcelErrorKind::Ref).with_message("Column row count overflows grid")
        })?;
        let col_span = u32::try_from(ncols - 1).map_err(|_| {
            ExcelError::new(ExcelErrorKind::Ref).with_message("Column count overflows grid")
        })?;
        let end_row = start_row.checked_add(row_span).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Ref).with_message("Bulk write row extent overflows")
        })?;
        let end_col = start_col.checked_add(col_span).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Ref).with_message("Bulk write column extent overflows")
        })?;
        let end_coord = Self::try_excel_coord(sheet, end_row, end_col)?;

        let written_cells = nrows.checked_mul(ncols).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Value).with_message("Bulk write cell count overflows")
        })?;
        self.check_bulk_write_limits(sheet, end_row, end_col, written_cells)?;

        let r0 = start_coord.row();
        let r1 = end_coord.row();
        let c0 = start_coord.col();
        let c1 = end_coord.col();
        let rects = [(r0, r1, c0, c1)];

        let existing_sheet_id = self.graph.sheet_id(sheet);
        self.graph
            .preflight_bulk_value_admission_rects(existing_sheet_id, &rects)?;

        let empty_tables = existing_sheet_id
            .map(|sid| {
                self.graph
                    .empty_tables_with_non_empty_in_body(sid, |br0, br1, bc0, bc1| {
                        let ir0 = r0.max(br0);
                        let ir1 = r1.min(br1);
                        let ic0 = c0.max(bc0);
                        let ic1 = c1.min(bc1);
                        if ir0 > ir1 || ic0 > ic1 {
                            return false;
                        }
                        for c in ic0..=ic1 {
                            let col = &columns[(c - c0) as usize];
                            if matches!(col, BulkColumn::Null { .. }) {
                                continue;
                            }
                            for r in ir0..=ir1 {
                                let idx = (r - r0) as usize;
                                if !decode_bulk_column_cell(col, idx, TypeTag::Number).is_empty() {
                                    return true;
                                }
                            }
                        }
                        false
                    })
            })
            .unwrap_or_default();

        // Pre-resolve serial tags before taking mutable borrow of arrow_sheets.
        let serial_tags: Vec<TypeTag> = columns
            .iter()
            .map(|col| match col {
                BulkColumn::Serial { format, .. } => self.resolve_serial_type_tag(*format),
                _ => TypeTag::Number,
            })
            .collect();

        // ── Phase 2: Engine accessors, lazy storage, deferred dirty ──
        let sheet_existed = existing_sheet_id.is_some();
        let sheet_id = self.graph.sheet_id_mut(sheet);
        self.begin_deferred_dirty();

        // ── Phase 3: Region-level reconciliation ──
        let res = self.reconcile_rects(sheet, sheet_id, &rects, &empty_tables);
        let (replaced_formula, resized_table) = match res {
            Ok(flags) => flags,
            Err(e) => {
                self.end_deferred_dirty();
                return Err(e);
            }
        };

        // ── Phase 4: Chunk-aligned engine-owned storage write + overlay byte accounting ──
        if self.config.arrow_storage_enabled && self.config.delta_overlay_enabled {
            self.ensure_arrow_sheet_exists(sheet, (r1 as usize) + 1, (c1 as usize) + 1);
            let mut computed_overlay_delta = 0isize;
            let mut compactions = 0u64;

            let asheet = self
                .sheet_store_mut()
                .sheet_mut(sheet)
                .expect("ArrowSheet must exist");

            let r0_usize = r0 as usize;
            let r1_excl = (r1 as usize) + 1;
            let first_ch = asheet
                .chunk_of_row(r0_usize)
                .map(|(idx, _)| idx)
                .unwrap_or(0);
            let last_ch = asheet
                .chunk_of_row(r1 as usize)
                .map(|(idx, _)| idx)
                .unwrap_or(first_ch);

            for (ci, col) in columns.iter().enumerate() {
                let abs_col = (c0 as usize) + ci;
                let serial_tag = serial_tags[ci];

                for ch_idx in first_ch..=last_ch {
                    let Some((ch_start, ch_len)) = asheet.chunk_extent(ch_idx) else {
                        continue;
                    };
                    if ch_len == 0 {
                        continue;
                    }
                    let ch_end = ch_start + ch_len;
                    let write_start = r0_usize.max(ch_start);
                    let write_end = r1_excl.min(ch_end);
                    if write_start >= write_end {
                        continue;
                    }
                    let lo = write_start - ch_start;
                    let hi = write_end - ch_start;
                    let touched = hi - lo;
                    let is_full_chunk = lo == 0 && hi == ch_len;
                    let chunk_exists = asheet.columns[abs_col].chunk(ch_idx).is_some();

                    if is_full_chunk {
                        if let Some(old_ch) = asheet.columns[abs_col].chunk(ch_idx) {
                            computed_overlay_delta = computed_overlay_delta
                                .saturating_sub(old_ch.computed_overlay.estimated_bytes() as isize);
                        }
                        let col_off = write_start - r0_usize;
                        match build_full_chunk_from_column(col, col_off, ch_len, serial_tag) {
                            Some(new_chunk) => {
                                asheet.install_column_chunk(abs_col, ch_idx, new_chunk);
                            }
                            None => {
                                if chunk_exists {
                                    asheet.install_column_chunk(
                                        abs_col,
                                        ch_idx,
                                        ArrowSheet::make_empty_chunk(ch_len),
                                    );
                                }
                            }
                        }
                    } else if !chunk_exists || should_rebuild_chunk(ch_len, touched) {
                        let existing = if chunk_exists {
                            let col_mut = &mut asheet.columns[abs_col];
                            if ch_idx < col_mut.chunks.len() {
                                Some(std::mem::replace(
                                    &mut col_mut.chunks[ch_idx],
                                    ArrowSheet::make_empty_chunk(0),
                                ))
                            } else {
                                col_mut.sparse_chunks.remove(&ch_idx)
                            }
                        } else {
                            None
                        };
                        let (rebuilt, delta) =
                            rebuild_partial_dense_chunk(existing, ch_len, lo, hi, |in_off| {
                                let abs_row = ch_start + in_off;
                                decode_bulk_column_cell(col, abs_row - r0_usize, serial_tag)
                            });
                        computed_overlay_delta = computed_overlay_delta.saturating_add(delta);
                        if let Some(new_ch) = rebuilt {
                            asheet.install_column_chunk(abs_col, ch_idx, new_ch);
                        }
                    } else {
                        // Light overlay update on an existing chunk.
                        if let Some(ch) = asheet.columns[abs_col].chunk_mut(ch_idx) {
                            for in_off in lo..hi {
                                let abs_row = ch_start + in_off;
                                let cell =
                                    decode_bulk_column_cell(col, abs_row - r0_usize, serial_tag);
                                let _ = ch.overlay.set(in_off, cell.to_overlay_value());
                                ch.overlay.set_format(in_off, cell.format_opt());
                                computed_overlay_delta = computed_overlay_delta
                                    .saturating_add(ch.computed_overlay.remove(in_off));
                                ch.computed_overlay.set_format(in_off, None);
                            }
                            if let Some(runs) = ch.format.as_ref() {
                                ch.format = runs.clear_range(lo, hi);
                            }
                        }
                        if asheet.maybe_compact_chunk(abs_col, ch_idx, 1024, 50) > 0 {
                            compactions = compactions.saturating_add(1);
                        }
                    }
                }
            }

            if compactions > 0 {
                self.overlay_compactions = self.overlay_compactions.saturating_add(compactions);
            }
            self.adjust_computed_overlay_bytes(computed_overlay_delta);
        }

        // ── Phase 5: Extent, dirty propagation, flush & epochs (ONCE per batch) ──
        self.graph.note_extent_rect(sheet_id, r0, r1, c0, c1);
        let _ = self.graph.mark_dirty_rects(&[(sheet_id, r0, r1, c0, c1)]);
        self.end_deferred_dirty();
        if !sheet_existed || replaced_formula || resized_table {
            self.mark_topology_edited();
        }
        self.mark_data_edited();

        Ok(WriteRangeReport {
            start_row,
            start_col,
            end_row,
            end_col,
            history_cleared: false,
        })
    }

    /// Unified engine bulk write for dense or ragged row-major `LiteralValue` rows.
    ///
    /// Trailing empty rows are ignored; ragged shorter rows leave trailing cells
    /// untouched (preserving exact parity with cell-by-cell `set_cell_value`).
    pub fn set_region_values(
        &mut self,
        sheet: &str,
        start_row: u32,
        start_col: u32,
        rows: &[Vec<LiteralValue>],
    ) -> Result<WriteRangeReport, ExcelError> {
        // ── Phase 1: Atomic Preflight ──
        self.ensure_not_poisoned()?;
        let start_coord = Self::try_excel_coord(sheet, start_row, start_col)?;
        self.observe_function_semantic_epoch()?;

        let width = rows.iter().map(Vec::len).max().unwrap_or(0);
        let Some(last_non_empty) = rows.iter().rposition(|r| !r.is_empty()) else {
            return Ok(WriteRangeReport::default());
        };
        if width == 0 {
            return Ok(WriteRangeReport::default());
        }
        let first_non_empty = rows.iter().position(|r| !r.is_empty()).unwrap_or(0);

        let row_span = u32::try_from(last_non_empty).map_err(|_| {
            ExcelError::new(ExcelErrorKind::Ref).with_message("Batch row extent overflows grid")
        })?;
        let col_span = u32::try_from(width - 1).map_err(|_| {
            ExcelError::new(ExcelErrorKind::Ref).with_message("Batch column extent overflows grid")
        })?;
        let end_row = start_row.checked_add(row_span).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Ref).with_message("Batch row extent overflows grid")
        })?;
        let end_col = start_col.checked_add(col_span).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Ref).with_message("Batch column extent overflows grid")
        })?;
        let _end_coord = Self::try_excel_coord(sheet, end_row, end_col)?;

        let active_rows = &rows[..=last_non_empty];
        let written_cells: usize = active_rows.iter().map(Vec::len).sum();
        self.check_bulk_write_limits(sheet, end_row, end_col, written_cells)?;

        let start_row0 = start_coord.row();
        let start_col0 = start_coord.col();
        let is_rectangular = active_rows.iter().all(|r| r.len() == width);
        let rects = maximal_row_rects(start_row0, start_col0, active_rows);

        let existing_sheet_id = self.graph.sheet_id(sheet);
        self.graph
            .preflight_bulk_value_admission_rects(existing_sheet_id, &rects)?;

        let empty_tables = existing_sheet_id
            .map(|sid| {
                self.graph
                    .empty_tables_with_non_empty_in_body(sid, |br0, br1, bc0, bc1| {
                        let ir0 = start_row0.max(br0);
                        let ir1 = (start_row0 + last_non_empty as u32).min(br1);
                        if ir0 > ir1 {
                            return false;
                        }
                        for r in ir0..=ir1 {
                            let row_vals = &active_rows[(r - start_row0) as usize];
                            if row_vals.is_empty() {
                                continue;
                            }
                            let row_c1 = start_col0 + (row_vals.len() as u32 - 1);
                            let ic0 = start_col0.max(bc0);
                            let ic1 = row_c1.min(bc1);
                            if ic0 > ic1 {
                                continue;
                            }
                            for c in ic0..=ic1 {
                                if row_vals[(c - start_col0) as usize] != LiteralValue::Empty {
                                    return true;
                                }
                            }
                        }
                        false
                    })
            })
            .unwrap_or_default();

        // ── Phase 2: Engine accessors, lazy storage, deferred dirty ──
        let sheet_existed = existing_sheet_id.is_some();
        let sheet_id = self.graph.sheet_id_mut(sheet);
        self.begin_deferred_dirty();

        // ── Phase 3: Region-level reconciliation ──
        let res = self.reconcile_rects(sheet, sheet_id, &rects, &empty_tables);
        let (replaced_formula, resized_table) = match res {
            Ok(flags) => flags,
            Err(e) => {
                self.end_deferred_dirty();
                return Err(e);
            }
        };

        // ── Phase 4: Chunk-aligned engine-owned storage write + overlay byte accounting ──
        if self.config.arrow_storage_enabled && self.config.delta_overlay_enabled {
            let max_row0 = (start_row0 as usize) + last_non_empty;
            let max_col0 = (start_col0 as usize) + (width - 1);
            self.ensure_arrow_sheet_exists(sheet, max_row0 + 1, max_col0 + 1);
            let mut computed_overlay_delta = 0isize;
            let mut compactions = 0u64;

            let asheet = self
                .sheet_store_mut()
                .sheet_mut(sheet)
                .expect("ArrowSheet must exist");
            let date_system = asheet.date_system;

            let r0_usize = start_row0 as usize;
            let r1_excl = max_row0 + 1;
            let first_ch = asheet
                .chunk_of_row(r0_usize)
                .map(|(idx, _)| idx)
                .unwrap_or(0);
            let last_ch = asheet
                .chunk_of_row(max_row0)
                .map(|(idx, _)| idx)
                .unwrap_or(first_ch);

            if is_rectangular {
                for ci in 0..width {
                    let abs_col = (start_col0 as usize) + ci;
                    for ch_idx in first_ch..=last_ch {
                        let Some((ch_start, ch_len)) = asheet.chunk_extent(ch_idx) else {
                            continue;
                        };
                        if ch_len == 0 {
                            continue;
                        }
                        let ch_end = ch_start + ch_len;
                        let write_start = r0_usize.max(ch_start);
                        let write_end = r1_excl.min(ch_end);
                        if write_start >= write_end {
                            continue;
                        }
                        let lo = write_start - ch_start;
                        let hi = write_end - ch_start;
                        let touched = hi - lo;
                        let is_full_chunk = lo == 0 && hi == ch_len;
                        let chunk_exists = asheet.columns[abs_col].chunk(ch_idx).is_some();
                        let row_off = write_start - r0_usize;

                        let has_panicked = active_rows[row_off..row_off + touched].iter().any(|r| {
                            matches!(
                                &r[ci],
                                LiteralValue::Error(e)
                                    if matches!(
                                        e.extra(),
                                        formualizer_common::ExcelErrorExtra::FunctionPanicked { .. }
                                    )
                            )
                        });

                        if !has_panicked && is_full_chunk {
                            if let Some(old_ch) = asheet.columns[abs_col].chunk(ch_idx) {
                                computed_overlay_delta = computed_overlay_delta.saturating_sub(
                                    old_ch.computed_overlay.estimated_bytes() as isize,
                                );
                            }
                            match build_full_chunk_from_literal_col(
                                active_rows,
                                row_off,
                                ch_len,
                                ci,
                                date_system,
                            ) {
                                Ok(Some(new_chunk)) => {
                                    asheet.install_column_chunk(abs_col, ch_idx, new_chunk);
                                }
                                Ok(None) => {
                                    if chunk_exists {
                                        asheet.install_column_chunk(
                                            abs_col,
                                            ch_idx,
                                            ArrowSheet::make_empty_chunk(ch_len),
                                        );
                                    }
                                }
                                Err(()) => unreachable!("has_panicked already checked"),
                            }
                        } else if !has_panicked
                            && (!chunk_exists || should_rebuild_chunk(ch_len, touched))
                        {
                            let existing = if chunk_exists {
                                let col_mut = &mut asheet.columns[abs_col];
                                if ch_idx < col_mut.chunks.len() {
                                    Some(std::mem::replace(
                                        &mut col_mut.chunks[ch_idx],
                                        ArrowSheet::make_empty_chunk(0),
                                    ))
                                } else {
                                    col_mut.sparse_chunks.remove(&ch_idx)
                                }
                            } else {
                                None
                            };
                            let (rebuilt, delta) =
                                rebuild_partial_dense_chunk(existing, ch_len, lo, hi, |in_off| {
                                    let abs_row = ch_start + in_off;
                                    DecodedCell::from_literal(
                                        &active_rows[abs_row - r0_usize][ci],
                                        date_system,
                                    )
                                });
                            computed_overlay_delta = computed_overlay_delta.saturating_add(delta);
                            if let Some(new_ch) = rebuilt {
                                asheet.install_column_chunk(abs_col, ch_idx, new_ch);
                            }
                        } else {
                            // Light overlay update (or FunctionPanicked fallback).
                            let ch = if has_panicked && !chunk_exists {
                                asheet.ensure_column_chunk_mut(abs_col, ch_idx)
                            } else {
                                asheet.columns[abs_col].chunk_mut(ch_idx)
                            };
                            if let Some(ch) = ch {
                                for in_off in lo..hi {
                                    let abs_row = ch_start + in_off;
                                    let val = &active_rows[abs_row - r0_usize][ci];
                                    let ov = OverlayValue::from_literal_value(
                                        &val.clone().finite_or_num_error(),
                                        date_system,
                                    );
                                    let fmt =
                                        DecodedCell::from_literal(val, date_system).format_opt();
                                    let _ = ch.overlay.set(in_off, ov);
                                    ch.overlay.set_format(in_off, fmt);
                                    computed_overlay_delta = computed_overlay_delta
                                        .saturating_add(ch.computed_overlay.remove(in_off));
                                    ch.computed_overlay.set_format(in_off, None);
                                }
                                if let Some(runs) = ch.format.as_ref() {
                                    ch.format = runs.clear_range(lo, hi);
                                }
                            }
                            if !has_panicked
                                && asheet.maybe_compact_chunk(abs_col, ch_idx, 1024, 50) > 0
                            {
                                compactions = compactions.saturating_add(1);
                            }
                        }
                    }
                }
            } else {
                // Ragged rows: write only touched cells (leaving short-row trailing cells untouched).
                for ci in 0..width {
                    let abs_col = (start_col0 as usize) + ci;
                    for ch_idx in first_ch..=last_ch {
                        let Some((ch_start, ch_len)) = asheet.chunk_extent(ch_idx) else {
                            continue;
                        };
                        if ch_len == 0 {
                            continue;
                        }
                        let ch_end = ch_start + ch_len;
                        let write_start = r0_usize.max(ch_start);
                        let write_end = r1_excl.min(ch_end);
                        if write_start >= write_end {
                            continue;
                        }
                        let mut writes: Vec<(usize, DecodedCell<'_>)> = Vec::new();
                        let mut has_panicked = false;
                        for abs_row in write_start..write_end {
                            let r_slice = &active_rows[abs_row - r0_usize];
                            if ci < r_slice.len() {
                                let val = &r_slice[ci];
                                if let LiteralValue::Error(e) = val
                                    && matches!(
                                        e.extra(),
                                        formualizer_common::ExcelErrorExtra::FunctionPanicked { .. }
                                    )
                                {
                                    has_panicked = true;
                                }
                                writes.push((
                                    abs_row - ch_start,
                                    DecodedCell::from_literal(val, date_system),
                                ));
                            }
                        }
                        if writes.is_empty() {
                            continue;
                        }
                        let chunk_exists = asheet.columns[abs_col].chunk(ch_idx).is_some();
                        if !has_panicked
                            && (!chunk_exists || should_rebuild_chunk(ch_len, writes.len()))
                        {
                            let existing = if chunk_exists {
                                let col_mut = &mut asheet.columns[abs_col];
                                if ch_idx < col_mut.chunks.len() {
                                    Some(std::mem::replace(
                                        &mut col_mut.chunks[ch_idx],
                                        ArrowSheet::make_empty_chunk(0),
                                    ))
                                } else {
                                    col_mut.sparse_chunks.remove(&ch_idx)
                                }
                            } else {
                                None
                            };
                            let (rebuilt, delta) = rebuild_sparse_chunk(existing, ch_len, &writes);
                            computed_overlay_delta = computed_overlay_delta.saturating_add(delta);
                            if let Some(new_ch) = rebuilt {
                                asheet.install_column_chunk(abs_col, ch_idx, new_ch);
                            }
                        } else {
                            let all_empty = writes.iter().all(|(_, c)| c.is_empty());
                            if !chunk_exists && all_empty {
                                continue;
                            }
                            if let Some(ch) = asheet.ensure_column_chunk_mut(abs_col, ch_idx) {
                                let mut offsets = Vec::with_capacity(writes.len());
                                for &(in_off, cell) in &writes {
                                    let abs_row = ch_start + in_off;
                                    let val = &active_rows[abs_row - r0_usize][ci];
                                    let ov = if has_panicked {
                                        OverlayValue::from_literal_value(
                                            &val.clone().finite_or_num_error(),
                                            date_system,
                                        )
                                    } else {
                                        cell.to_overlay_value()
                                    };
                                    let _ = ch.overlay.set(in_off, ov);
                                    ch.overlay.set_format(in_off, cell.format_opt());
                                    computed_overlay_delta = computed_overlay_delta
                                        .saturating_add(ch.computed_overlay.remove(in_off));
                                    ch.computed_overlay.set_format(in_off, None);
                                    offsets.push(in_off);
                                }
                                if let Some(runs) = ch.format.as_ref() {
                                    ch.format = runs.clear_offsets(&offsets);
                                }
                            }
                            if !has_panicked
                                && asheet.maybe_compact_chunk(abs_col, ch_idx, 1024, 50) > 0
                            {
                                compactions = compactions.saturating_add(1);
                            }
                        }
                    }
                }
            }

            if compactions > 0 {
                self.overlay_compactions = self.overlay_compactions.saturating_add(compactions);
            }
            self.adjust_computed_overlay_bytes(computed_overlay_delta);
        }

        // ── Phase 5: Extent, dirty propagation, flush & epochs (ONCE per batch) ──
        let mut dirty_rects = Vec::with_capacity(rects.len());
        for &(r0, r1, c0, c1) in &rects {
            self.graph.note_extent_rect(sheet_id, r0, r1, c0, c1);
            dirty_rects.push((sheet_id, r0, r1, c0, c1));
        }
        let _ = self.graph.mark_dirty_rects(&dirty_rects);
        self.end_deferred_dirty();
        if !sheet_existed || replaced_formula || resized_table {
            self.mark_topology_edited();
        }
        self.mark_data_edited();

        Ok(WriteRangeReport {
            start_row: start_row + first_non_empty as u32,
            start_col,
            end_row,
            end_col,
            history_cleared: false,
        })
    }

    /// Unified engine bulk write for sparse coordinate patches (`CellBatch`).
    ///
    /// Duplicate coordinates resolve deterministically in last-write-wins order.
    pub fn set_region_cells(
        &mut self,
        sheet: &str,
        batch: &CellBatch<'_>,
    ) -> Result<WriteRangeReport, ExcelError> {
        // ── Phase 1: Atomic Preflight ──
        self.ensure_not_poisoned()?;
        self.observe_function_semantic_epoch()?;

        let n = batch.rows.len();
        if batch.cols.len() != n {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                "CellBatch: rows length ({n}) does not match cols length ({})",
                batch.cols.len()
            )));
        }
        validate_wire_cells(
            batch.tags,
            batch.numbers,
            batch.text_bytes,
            batch.text_offsets,
            "CellBatch",
        )?;
        if n == 0 {
            return Ok(WriteRangeReport::default());
        }

        let mut min_row = u32::MAX;
        let mut max_row = 0u32;
        let mut min_col = u32::MAX;
        let mut max_col = 0u32;
        // Pack (col0, row0, orig_idx) for stable last-write-wins deduplication and
        // cache-friendly column/chunk grouping.
        let mut entries: Vec<(u32, u32, u32)> = Vec::with_capacity(n);
        for i in 0..n {
            let r = batch.rows[i];
            let c = batch.cols[i];
            let coord = Self::try_excel_coord(sheet, r, c)?;
            min_row = min_row.min(r);
            max_row = max_row.max(r);
            min_col = min_col.min(c);
            max_col = max_col.max(c);
            entries.push((coord.col(), coord.row(), i as u32));
        }

        self.check_bulk_write_limits(sheet, max_row, max_col, n)?;

        // Sort by (col0, row0, orig_idx) and keep the LAST entry for each (col0, row0).
        entries.sort_unstable();
        let mut deduped: Vec<(u32, u32, u32)> = Vec::with_capacity(entries.len());
        for item in entries {
            if let Some(last) = deduped.last_mut()
                && last.0 == item.0
                && last.1 == item.1
            {
                *last = item; // last-write-wins (larger orig_idx)
            } else {
                deduped.push(item);
            }
        }

        // Build row-major sorted (row0, col0) points for graph admission, spill release,
        // and empty-table checks.
        let mut points_row_col: Vec<(u32, u32)> =
            deduped.iter().map(|&(c0, r0, _)| (r0, c0)).collect();
        points_row_col.sort_unstable();

        let existing_sheet_id = self.graph.sheet_id(sheet);
        self.graph
            .preflight_bulk_value_admission_points0(existing_sheet_id, &points_row_col)?;

        let empty_tables = existing_sheet_id
            .map(|sid| {
                self.graph
                    .empty_tables_with_non_empty_in_body(sid, |br0, br1, bc0, bc1| {
                        // Binary-search column range [bc0..=bc1] inside col-sorted `deduped`.
                        let start = deduped.partition_point(|&(c0, _, _)| c0 < bc0);
                        for &(c0, r0, orig_idx) in &deduped[start..] {
                            if c0 > bc1 {
                                break;
                            }
                            if r0 >= br0
                                && r0 <= br1
                                && batch.tags[orig_idx as usize] != BulkCellTag::Empty as u8
                            {
                                return true;
                            }
                        }
                        false
                    })
            })
            .unwrap_or_default();

        // Coalesce contiguous vertical runs `(col0, r0, r1)` for extent, dirty rects,
        // spill waiters, and staged formula suppression.
        let points_col_row: Vec<(u32, u32)> = deduped.iter().map(|&(c0, r0, _)| (c0, r0)).collect();
        let col_runs = coalesce_sorted_col_runs(&points_col_row);

        // ── Phase 2: Engine accessors, lazy storage, deferred dirty ──
        let sheet_existed = existing_sheet_id.is_some();
        let sheet_id = self.graph.sheet_id_mut(sheet);
        self.begin_deferred_dirty();

        // ── Phase 3: Point-level reconciliation ──
        let mut replaced_formula = false;
        // 1. E3: Vacate all vertices at touched points (clearing spill anchors first).
        for &(r0, c0) in &points_row_col {
            let addr = CellRef::new(sheet_id, Coord::new(r0, c0, true, true));
            if let Some(v) = self.graph.get_vertex_id_for_address(&addr)
                && self.graph.vertex_exists(v)
            {
                let is_formula = matches!(
                    self.graph.get_vertex_kind(v),
                    VertexKind::FormulaScalar | VertexKind::FormulaArray
                );
                if is_formula {
                    replaced_formula = true;
                    if self.graph.spill_registry_has_anchor(v) {
                        self.clear_spill_projection_and_mirror(v, None);
                    }
                }
                self.graph.vacate_cell(&addr);
            }
        }

        // 2. B1: Per-child spill release O(min(spills_on_sheet, num_points)) + waiter wakeups.
        let dirtied_owners = self
            .graph
            .release_spill_children_at_points0(sheet_id, &points_row_col);
        if !dirtied_owners.is_empty() {
            self.graph.mark_dirty_many(&dirtied_owners);
        }
        if !self.spill_blocker_waiters.is_empty() || !self.blocked_pending_spills.is_empty() {
            for &(c0, r0, r1) in &col_runs {
                self.record_structural_change(StructuralScope::Region(Region::rect(
                    sheet_id, r0, r1, c0, c0,
                )));
            }
        }

        // 3. E6: Staged formulas & deferred shared-formula packages.
        if self.staged_formulas.contains_key(sheet) {
            let points_1based: Vec<(u32, u32)> = points_row_col
                .iter()
                .map(|&(r0, c0)| (r0 + 1, c0 + 1))
                .collect();
            let col_runs_1based: Vec<(u32, u32, u32)> = col_runs
                .iter()
                .map(|&(c0, r0, r1)| (c0 + 1, r0 + 1, r1 + 1))
                .collect();
            self.clear_staged_formulas_at_points(sheet, &points_1based, &col_runs_1based);
        }

        // 4. E5: Empty-table resizes.
        let mut resized_table = false;
        for table in &empty_tables {
            if let Err(e) = self.graph.resize_table(&table.name, table.range) {
                self.end_deferred_dirty();
                return Err(e);
            }
            resized_table = true;
        }

        // 5. B3: Clear derived formats at touched points.
        if !self.derived_formats.is_empty() {
            for &(r0, c0) in &points_row_col {
                let cell = CellRef::new(sheet_id, Coord::new(r0, c0, true, true));
                self.derived_formats.set(cell, None);
            }
        }

        // ── Phase 4: Chunk-aligned storage write + overlay byte accounting ──
        if self.config.arrow_storage_enabled && self.config.delta_overlay_enabled {
            self.ensure_arrow_sheet_exists(sheet, max_row as usize, max_col as usize);
            let mut computed_overlay_delta = 0isize;
            let mut compactions = 0u64;

            let asheet = self
                .sheet_store_mut()
                .sheet_mut(sheet)
                .expect("ArrowSheet must exist");

            // Group `deduped` by `(col0, ch_idx)`.
            let mut idx = 0usize;
            while idx < deduped.len() {
                let abs_col = deduped[idx].0 as usize;
                let col_start = idx;
                while idx < deduped.len() && deduped[idx].0 as usize == abs_col {
                    idx += 1;
                }
                let col_entries = &deduped[col_start..idx];

                let mut sub = 0usize;
                while sub < col_entries.len() {
                    let first_row = col_entries[sub].1 as usize;
                    let Some((ch_idx, _)) = asheet.chunk_of_row(first_row) else {
                        sub += 1;
                        continue;
                    };
                    let Some((ch_start, ch_len)) = asheet.chunk_extent(ch_idx) else {
                        sub += 1;
                        continue;
                    };
                    let ch_end = ch_start + ch_len;
                    let mut writes: Vec<(usize, DecodedCell<'_>)> = Vec::new();
                    while sub < col_entries.len() {
                        let r0 = col_entries[sub].1 as usize;
                        if r0 >= ch_end {
                            break;
                        }
                        let orig = col_entries[sub].2 as usize;
                        let cell = decode_wire_cell(
                            batch.tags[orig],
                            batch.numbers[orig],
                            batch.text_bytes,
                            batch.text_offsets,
                        );
                        writes.push((r0 - ch_start, cell));
                        sub += 1;
                    }
                    if writes.is_empty() {
                        continue;
                    }

                    let chunk_exists = asheet.columns[abs_col].chunk(ch_idx).is_some();
                    if should_rebuild_chunk(ch_len, writes.len()) {
                        let existing = if chunk_exists {
                            let col_mut = &mut asheet.columns[abs_col];
                            if ch_idx < col_mut.chunks.len() {
                                Some(std::mem::replace(
                                    &mut col_mut.chunks[ch_idx],
                                    ArrowSheet::make_empty_chunk(0),
                                ))
                            } else {
                                col_mut.sparse_chunks.remove(&ch_idx)
                            }
                        } else {
                            None
                        };
                        let (rebuilt, delta) = rebuild_sparse_chunk(existing, ch_len, &writes);
                        computed_overlay_delta = computed_overlay_delta.saturating_add(delta);
                        if let Some(new_ch) = rebuilt {
                            asheet.install_column_chunk(abs_col, ch_idx, new_ch);
                        }
                    } else {
                        let all_empty = writes.iter().all(|(_, c)| c.is_empty());
                        if !chunk_exists && all_empty {
                            continue;
                        }
                        if let Some(ch) = asheet.ensure_column_chunk_mut(abs_col, ch_idx) {
                            let mut offsets = Vec::with_capacity(writes.len());
                            for &(in_off, cell) in &writes {
                                let _ = ch.overlay.set(in_off, cell.to_overlay_value());
                                ch.overlay.set_format(in_off, cell.format_opt());
                                computed_overlay_delta = computed_overlay_delta
                                    .saturating_add(ch.computed_overlay.remove(in_off));
                                ch.computed_overlay.set_format(in_off, None);
                                offsets.push(in_off);
                            }
                            if let Some(runs) = ch.format.as_ref() {
                                ch.format = runs.clear_offsets(&offsets);
                            }
                        }
                        if asheet.maybe_compact_chunk(abs_col, ch_idx, 1024, 50) > 0 {
                            compactions = compactions.saturating_add(1);
                        }
                    }
                }
            }

            if compactions > 0 {
                self.overlay_compactions = self.overlay_compactions.saturating_add(compactions);
            }
            self.adjust_computed_overlay_bytes(computed_overlay_delta);
        }

        // ── Phase 5: Extent, dirty propagation, flush & epochs (ONCE per batch) ──
        self.graph.note_extent_runs(sheet_id, &col_runs);
        let dirty_rects: Vec<(crate::SheetId, u32, u32, u32, u32)> = col_runs
            .iter()
            .map(|&(c0, r0, r1)| (sheet_id, r0, r1, c0, c0))
            .collect();
        let _ = self.graph.mark_dirty_rects(&dirty_rects);
        self.end_deferred_dirty();
        if !sheet_existed || replaced_formula || resized_table {
            self.mark_topology_edited();
        }
        self.mark_data_edited();

        Ok(WriteRangeReport {
            start_row: min_row,
            start_col: min_col,
            end_row: max_row,
            end_col: max_col,
            history_cleared: false,
        })
    }
}
