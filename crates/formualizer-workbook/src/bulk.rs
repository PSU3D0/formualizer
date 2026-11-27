//! Bulk columnar and sparse write types for `Workbook` (`v0.16.0` Decision 2).

use formualizer_eval::engine::{BulkColumn, PackedColumnBlock};
use formualizer_eval::format::FormatId;

/// Columnar input for dense column-major bulk writes.
///
/// Every `validity` slice is strictly an Arrow LSB validity bitmap where bit `i`
/// is `(validity[i / 8] >> (i % 8)) & 1 != 0`.
#[derive(Clone, Copy, Debug)]
pub enum ColumnData<'a> {
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

impl<'a> ColumnData<'a> {
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

    #[inline]
    pub(crate) fn as_bulk_column(&self) -> BulkColumn<'a> {
        match *self {
            Self::Float64 { values, validity } => BulkColumn::Float64 { values, validity },
            Self::Int64 { values, validity } => BulkColumn::Int64 { values, validity },
            Self::Boolean { values, validity } => BulkColumn::Boolean { values, validity },
            Self::Utf8 {
                bytes,
                offsets,
                validity,
            } => BulkColumn::Utf8 {
                bytes,
                offsets,
                validity,
            },
            Self::Serial {
                values,
                format,
                validity,
            } => BulkColumn::Serial {
                values,
                format,
                validity,
            },
            Self::Null { len } => BulkColumn::Null { len },
            Self::Mixed(block) => BulkColumn::Mixed(block),
        }
    }
}

/// Owned backing buffers for columnar bulk writes (produced by converters such as
/// `ColumnData::try_from_arrow` when the opt-in `arrow` feature is enabled).
#[derive(Clone, Debug)]
pub enum OwnedColumnData {
    Float64 {
        values: Vec<f64>,
        validity: Option<Vec<u8>>,
    },
    Int64 {
        values: Vec<i64>,
        validity: Option<Vec<u8>>,
    },
    Boolean {
        values: Vec<bool>,
        validity: Option<Vec<u8>>,
    },
    Utf8 {
        bytes: Vec<u8>,
        offsets: Vec<u32>,
        validity: Option<Vec<u8>>,
    },
    Serial {
        values: Vec<f64>,
        format: FormatId,
        validity: Option<Vec<u8>>,
    },
    Null {
        len: usize,
    },
    Mixed {
        tags: Vec<u8>,
        numbers: Vec<f64>,
        text_bytes: Vec<u8>,
        text_offsets: Vec<u32>,
    },
}

impl OwnedColumnData {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Float64 { values, .. } => values.len(),
            Self::Int64 { values, .. } => values.len(),
            Self::Boolean { values, .. } => values.len(),
            Self::Utf8 { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Serial { values, .. } => values.len(),
            Self::Null { len } => *len,
            Self::Mixed { tags, .. } => tags.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn as_column_data(&self) -> ColumnData<'_> {
        match self {
            Self::Float64 { values, validity } => ColumnData::Float64 {
                values,
                validity: validity.as_deref(),
            },
            Self::Int64 { values, validity } => ColumnData::Int64 {
                values,
                validity: validity.as_deref(),
            },
            Self::Boolean { values, validity } => ColumnData::Boolean {
                values,
                validity: validity.as_deref(),
            },
            Self::Utf8 {
                bytes,
                offsets,
                validity,
            } => ColumnData::Utf8 {
                bytes,
                offsets,
                validity: validity.as_deref(),
            },
            Self::Serial {
                values,
                format,
                validity,
            } => ColumnData::Serial {
                values,
                format: *format,
                validity: validity.as_deref(),
            },
            Self::Null { len } => ColumnData::Null { len: *len },
            Self::Mixed {
                tags,
                numbers,
                text_bytes,
                text_offsets,
            } => ColumnData::Mixed(PackedColumnBlock {
                tags,
                numbers,
                text_bytes,
                text_offsets,
            }),
        }
    }
}

#[cfg(feature = "arrow")]
mod arrow_interop {
    use super::{ColumnData, OwnedColumnData};
    use crate::IoError;
    use arrow_array::types::{
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    };
    use arrow_array::{
        Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, DictionaryArray,
        DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
        DurationSecondArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, TimeUnit};
    use chrono::DateTime;
    use formualizer_eval::engine::{BulkCellTag, DateSystem};
    use formualizer_eval::format::FormatId;

    const MAX_SAFE_I64: i64 = 1_i64 << 53;
    const MAX_SAFE_U64: u64 = 1_u64 << 53;

    fn arrow_err(col_label: &str, msg: impl Into<String>) -> IoError {
        IoError::Backend {
            backend: "arrow".to_string(),
            message: format!("Column '{col_label}': {}", msg.into()),
        }
    }

    fn copy_validity(array: &dyn Array) -> Option<Vec<u8>> {
        if array.null_count() == 0 {
            return None;
        }
        let len = array.len();
        let mut bits = vec![0u8; len.div_ceil(8)];
        for i in 0..len {
            if array.is_valid(i) {
                bits[i >> 3] |= 1u8 << (i & 7);
            }
        }
        Some(bits)
    }

    /// Parse Arrow timestamp timezone string per ADR Q4:
    /// - `None`, `"UTC"`, `"Z"`, `"Etc/UTC"` -> `Ok(0)`
    /// - Fixed offset `±HH:MM`, `±HHMM`, `±HH` -> `Ok(offset_seconds)`
    /// - Any IANA timezone name -> `Err` instructing the caller to localize/strip it first.
    fn parse_arrow_tz_offset_secs(tz: Option<&str>, col_label: &str) -> Result<i64, IoError> {
        let Some(tz) = tz else {
            return Ok(0);
        };
        if tz.eq_ignore_ascii_case("UTC")
            || tz.eq_ignore_ascii_case("Z")
            || tz.eq_ignore_ascii_case("Etc/UTC")
        {
            return Ok(0);
        }
        let bytes = tz.as_bytes();
        if let Some(&sign_byte) = bytes.first()
            && (sign_byte == b'+' || sign_byte == b'-')
        {
            let sign: i64 = if sign_byte == b'-' { -1 } else { 1 };
            let rest = &tz[1..];
            let parsed = if let Some((h, m)) = rest.split_once(':') {
                if h.len() == 2 && m.len() == 2 {
                    h.parse::<i64>().ok().zip(m.parse::<i64>().ok())
                } else {
                    None
                }
            } else if rest.len() == 4 {
                rest[..2]
                    .parse::<i64>()
                    .ok()
                    .zip(rest[2..].parse::<i64>().ok())
            } else if rest.len() == 2 {
                rest.parse::<i64>().ok().map(|h| (h, 0))
            } else {
                None
            };
            if let Some((hours, mins)) = parsed
                && (0..=23).contains(&hours)
                && (0..=59).contains(&mins)
            {
                return Ok(sign * (hours * 3600 + mins * 60));
            }
        }
        Err(arrow_err(
            col_label,
            format!(
                "IANA timezone '{tz}' is not supported directly; localize or strip the timezone to naive/UTC/fixed-offset before calling Formualizer"
            ),
        ))
    }

    fn timestamp_secs_nanos_to_serial(
        secs: i64,
        nanos: u32,
        offset_secs: i64,
        date_system: DateSystem,
        col_label: &str,
    ) -> Result<f64, IoError> {
        let wall_secs = secs
            .checked_add(offset_secs)
            .ok_or_else(|| arrow_err(col_label, "timestamp overflows i64 seconds"))?;
        let dt = DateTime::from_timestamp(wall_secs, nanos)
            .ok_or_else(|| arrow_err(col_label, "timestamp out of chrono DateTime range"))?
            .naive_utc();
        Ok(formualizer_common::datetime_to_serial_for(date_system, &dt))
    }

    fn convert_string_dictionary(
        keys: &[Option<usize>],
        values: &dyn Array,
        col_label: &str,
    ) -> Result<OwnedColumnData, IoError> {
        let mut text_bytes = Vec::new();
        let mut text_offsets = Vec::with_capacity(values.len() + 1);
        text_offsets.push(0u32);
        let mut value_valid = Vec::with_capacity(values.len());

        match values.data_type() {
            DataType::Utf8 => {
                let arr = values.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        let s = arr.value(i);
                        text_bytes.extend_from_slice(s.as_bytes());
                        value_valid.push(true);
                    } else {
                        value_valid.push(false);
                    }
                    let off = u32::try_from(text_bytes.len()).map_err(|_| {
                        arrow_err(
                            col_label,
                            "dictionary string buffer exceeds 4 GiB u32 offset limit",
                        )
                    })?;
                    text_offsets.push(off);
                }
            }
            DataType::LargeUtf8 => {
                let arr = values.as_any().downcast_ref::<LargeStringArray>().unwrap();
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        let s = arr.value(i);
                        text_bytes.extend_from_slice(s.as_bytes());
                        value_valid.push(true);
                    } else {
                        value_valid.push(false);
                    }
                    let off = u32::try_from(text_bytes.len()).map_err(|_| {
                        arrow_err(
                            col_label,
                            "dictionary string buffer exceeds 4 GiB u32 offset limit",
                        )
                    })?;
                    text_offsets.push(off);
                }
            }
            DataType::Utf8View => {
                let arr = values.as_any().downcast_ref::<StringViewArray>().unwrap();
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        let s = arr.value(i);
                        text_bytes.extend_from_slice(s.as_bytes());
                        value_valid.push(true);
                    } else {
                        value_valid.push(false);
                    }
                    let off = u32::try_from(text_bytes.len()).map_err(|_| {
                        arrow_err(
                            col_label,
                            "dictionary string buffer exceeds 4 GiB u32 offset limit",
                        )
                    })?;
                    text_offsets.push(off);
                }
            }
            _ => unreachable!("caller checks string value type"),
        }

        let mut tags = Vec::with_capacity(keys.len());
        let mut numbers = Vec::with_capacity(keys.len());
        for &k in keys {
            match k {
                Some(idx) if idx < value_valid.len() && value_valid[idx] => {
                    tags.push(BulkCellTag::Text as u8);
                    numbers.push(idx as f64);
                }
                Some(idx) if idx >= value_valid.len() => {
                    return Err(arrow_err(
                        col_label,
                        format!(
                            "dictionary key {idx} out of bounds (len {})",
                            value_valid.len()
                        ),
                    ));
                }
                _ => {
                    tags.push(BulkCellTag::Empty as u8);
                    numbers.push(0.0);
                }
            }
        }
        Ok(OwnedColumnData::Mixed {
            tags,
            numbers,
            text_bytes,
            text_offsets,
        })
    }

    type DictKeysAndValues = (Vec<Option<usize>>, std::sync::Arc<dyn Array>);

    fn extract_dict_keys(
        array: &dyn Array,
        key_type: &DataType,
        col_label: &str,
    ) -> Result<DictKeysAndValues, IoError> {
        macro_rules! read_dict {
            ($kty:ty) => {{
                let d = array
                    .as_any()
                    .downcast_ref::<DictionaryArray<$kty>>()
                    .ok_or_else(|| arrow_err(col_label, "failed to downcast dictionary array"))?;
                let keys_arr = d.keys();
                let mut out = Vec::with_capacity(keys_arr.len());
                for i in 0..keys_arr.len() {
                    if keys_arr.is_null(i) {
                        out.push(None);
                    } else {
                        let raw = keys_arr.value(i);
                        let idx = usize::try_from(raw).map_err(|_| {
                            arrow_err(col_label, format!("negative dictionary key {raw}"))
                        })?;
                        out.push(Some(idx));
                    }
                }
                Ok((out, d.values().clone()))
            }};
        }
        match key_type {
            DataType::Int8 => read_dict!(Int8Type),
            DataType::Int16 => read_dict!(Int16Type),
            DataType::Int32 => read_dict!(Int32Type),
            DataType::Int64 => read_dict!(Int64Type),
            DataType::UInt8 => read_dict!(UInt8Type),
            DataType::UInt16 => read_dict!(UInt16Type),
            DataType::UInt32 => read_dict!(UInt32Type),
            DataType::UInt64 => read_dict!(UInt64Type),
            other => Err(arrow_err(
                col_label,
                format!("unsupported dictionary key type: {other:?}"),
            )),
        }
    }

    fn convert_dictionary(
        array: &dyn Array,
        key_type: &DataType,
        value_type: &DataType,
        date_system: DateSystem,
        col_label: &str,
    ) -> Result<OwnedColumnData, IoError> {
        let (keys, values) = extract_dict_keys(array, key_type, col_label)?;
        if matches!(
            value_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return convert_string_dictionary(&keys, values.as_ref(), col_label);
        }

        // Convert dictionary values via the same converter, then materialize by key.
        let decoded_vals =
            ColumnData::try_from_arrow_with_options(values.as_ref(), date_system, Some(col_label))?;
        let n = keys.len();
        let make_validity = |val_validity: Option<&[u8]>| -> Option<Vec<u8>> {
            let mut bits = vec![0u8; n.div_ceil(8)];
            let mut any_null = false;
            for (i, k) in keys.iter().enumerate() {
                let valid = match k {
                    Some(idx) => val_validity
                        .map(|b| ((b[idx >> 3] >> (idx & 7)) & 1) != 0)
                        .unwrap_or(true),
                    None => false,
                };
                if valid {
                    bits[i >> 3] |= 1u8 << (i & 7);
                } else {
                    any_null = true;
                }
            }
            any_null.then_some(bits)
        };

        match decoded_vals {
            OwnedColumnData::Null { .. } => Ok(OwnedColumnData::Null { len: n }),
            OwnedColumnData::Float64 { values, validity } => {
                let mut out = Vec::with_capacity(n);
                for &k in &keys {
                    match k {
                        Some(idx) if idx < values.len() => out.push(values[idx]),
                        Some(idx) => {
                            return Err(arrow_err(
                                col_label,
                                format!("dictionary key {idx} out of bounds"),
                            ));
                        }
                        None => out.push(0.0),
                    }
                }
                Ok(OwnedColumnData::Float64 {
                    values: out,
                    validity: make_validity(validity.as_deref()),
                })
            }
            OwnedColumnData::Int64 { values, validity } => {
                let mut out = Vec::with_capacity(n);
                for &k in &keys {
                    match k {
                        Some(idx) if idx < values.len() => out.push(values[idx]),
                        Some(idx) => {
                            return Err(arrow_err(
                                col_label,
                                format!("dictionary key {idx} out of bounds"),
                            ));
                        }
                        None => out.push(0),
                    }
                }
                Ok(OwnedColumnData::Int64 {
                    values: out,
                    validity: make_validity(validity.as_deref()),
                })
            }
            OwnedColumnData::Boolean { values, validity } => {
                let mut out = Vec::with_capacity(n);
                for &k in &keys {
                    match k {
                        Some(idx) if idx < values.len() => out.push(values[idx]),
                        Some(idx) => {
                            return Err(arrow_err(
                                col_label,
                                format!("dictionary key {idx} out of bounds"),
                            ));
                        }
                        None => out.push(false),
                    }
                }
                Ok(OwnedColumnData::Boolean {
                    values: out,
                    validity: make_validity(validity.as_deref()),
                })
            }
            OwnedColumnData::Serial {
                values,
                format,
                validity,
            } => {
                let mut out = Vec::with_capacity(n);
                for &k in &keys {
                    match k {
                        Some(idx) if idx < values.len() => out.push(values[idx]),
                        Some(idx) => {
                            return Err(arrow_err(
                                col_label,
                                format!("dictionary key {idx} out of bounds"),
                            ));
                        }
                        None => out.push(0.0),
                    }
                }
                Ok(OwnedColumnData::Serial {
                    values: out,
                    format,
                    validity: make_validity(validity.as_deref()),
                })
            }
            _ => Err(arrow_err(
                col_label,
                format!("unsupported dictionary value type: {value_type:?}"),
            )),
        }
    }

    impl ColumnData<'_> {
        /// Convert an Apache Arrow [`arrow_array::Array`] into engine-owned column
        /// buffers using [`DateSystem::Excel1900`].
        ///
        /// Always copies into owned buffers (never pins or slices foreign Arrow buffers).
        /// Available only when the opt-in `arrow` Cargo feature is enabled.
        pub fn try_from_arrow(array: &dyn Array) -> Result<OwnedColumnData, IoError> {
            Self::try_from_arrow_with_options(array, DateSystem::Excel1900, None)
        }

        /// Convert an Apache Arrow [`arrow_array::Array`] into engine-owned column
        /// buffers using the specified [`DateSystem`] and optional column name for
        /// preflight diagnostics.
        ///
        /// Available only when the opt-in `arrow` Cargo feature is enabled.
        pub fn try_from_arrow_with_options(
            array: &dyn Array,
            date_system: DateSystem,
            column_name: Option<&str>,
        ) -> Result<OwnedColumnData, IoError> {
            let col_label = column_name.unwrap_or("column");
            let len = array.len();
            match array.data_type() {
                DataType::Null => Ok(OwnedColumnData::Null { len }),
                DataType::Float64 => {
                    let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    let values = (0..len)
                        .map(|i| if a.is_valid(i) { a.value(i) } else { 0.0 })
                        .collect();
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::Float32 => {
                    let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                f64::from(a.value(i))
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::Float16 => {
                    let a = array.as_any().downcast_ref::<Float16Array>().unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                a.value(i).to_f64()
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::Int8 => {
                    let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                f64::from(a.value(i))
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::Int16 => {
                    let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                f64::from(a.value(i))
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::Int32 => {
                    let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                f64::from(a.value(i))
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::UInt8 => {
                    let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                f64::from(a.value(i))
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::UInt16 => {
                    let a = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                f64::from(a.value(i))
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::UInt32 => {
                    let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                f64::from(a.value(i))
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::Int64 => {
                    let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    let mut values = Vec::with_capacity(len);
                    for i in 0..len {
                        if a.is_valid(i) {
                            let v = a.value(i);
                            if !(-MAX_SAFE_I64..=MAX_SAFE_I64).contains(&v) {
                                return Err(arrow_err(
                                    col_label,
                                    format!(
                                        "row {i}: Int64 value {v} exceeds exact f64 range (±2^53); cast explicitly to text or float"
                                    ),
                                ));
                            }
                            values.push(v);
                        } else {
                            values.push(0);
                        }
                    }
                    Ok(OwnedColumnData::Int64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::UInt64 => {
                    let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                    let mut values = Vec::with_capacity(len);
                    for i in 0..len {
                        if a.is_valid(i) {
                            let v = a.value(i);
                            if v > MAX_SAFE_U64 {
                                return Err(arrow_err(
                                    col_label,
                                    format!(
                                        "row {i}: UInt64 value {v} exceeds exact f64 range (2^53); cast explicitly to text or float"
                                    ),
                                ));
                            }
                            values.push(v as f64);
                        } else {
                            values.push(0.0);
                        }
                    }
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::Decimal128(_, scale) => {
                    let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                    let factor = 10_f64.powi(i32::from(*scale));
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                (a.value(i) as f64) / factor
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Float64 {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::Boolean => {
                    let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    let values = (0..len).map(|i| a.is_valid(i) && a.value(i)).collect();
                    Ok(OwnedColumnData::Boolean {
                        values,
                        validity: copy_validity(array),
                    })
                }
                DataType::Utf8 => {
                    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                    let mut bytes = Vec::new();
                    let mut offsets = Vec::with_capacity(len + 1);
                    offsets.push(0u32);
                    for i in 0..len {
                        if a.is_valid(i) {
                            bytes.extend_from_slice(a.value(i).as_bytes());
                        }
                        let off = u32::try_from(bytes.len()).map_err(|_| {
                            arrow_err(col_label, "UTF-8 buffer exceeds 4 GiB u32 offset limit")
                        })?;
                        offsets.push(off);
                    }
                    Ok(OwnedColumnData::Utf8 {
                        bytes,
                        offsets,
                        validity: copy_validity(array),
                    })
                }
                DataType::LargeUtf8 => {
                    let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    let mut bytes = Vec::new();
                    let mut offsets = Vec::with_capacity(len + 1);
                    offsets.push(0u32);
                    for i in 0..len {
                        if a.is_valid(i) {
                            bytes.extend_from_slice(a.value(i).as_bytes());
                        }
                        let off = u32::try_from(bytes.len()).map_err(|_| {
                            arrow_err(col_label, "LargeUtf8 buffer exceeds 4 GiB u32 offset limit")
                        })?;
                        offsets.push(off);
                    }
                    Ok(OwnedColumnData::Utf8 {
                        bytes,
                        offsets,
                        validity: copy_validity(array),
                    })
                }
                DataType::Utf8View => {
                    let a = array.as_any().downcast_ref::<StringViewArray>().unwrap();
                    let mut bytes = Vec::new();
                    let mut offsets = Vec::with_capacity(len + 1);
                    offsets.push(0u32);
                    for i in 0..len {
                        if a.is_valid(i) {
                            bytes.extend_from_slice(a.value(i).as_bytes());
                        }
                        let off = u32::try_from(bytes.len()).map_err(|_| {
                            arrow_err(col_label, "Utf8View buffer exceeds 4 GiB u32 offset limit")
                        })?;
                        offsets.push(off);
                    }
                    Ok(OwnedColumnData::Utf8 {
                        bytes,
                        offsets,
                        validity: copy_validity(array),
                    })
                }
                DataType::Date32 => {
                    let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
                    let mut values = Vec::with_capacity(len);
                    for i in 0..len {
                        if a.is_valid(i) {
                            let secs = i64::from(a.value(i)) * 86_400;
                            values.push(timestamp_secs_nanos_to_serial(
                                secs,
                                0,
                                0,
                                date_system,
                                col_label,
                            )?);
                        } else {
                            values.push(0.0);
                        }
                    }
                    Ok(OwnedColumnData::Serial {
                        values,
                        format: FormatId::DATE,
                        validity: copy_validity(array),
                    })
                }
                DataType::Date64 => {
                    let a = array.as_any().downcast_ref::<Date64Array>().unwrap();
                    let mut values = Vec::with_capacity(len);
                    for i in 0..len {
                        if a.is_valid(i) {
                            let ms = a.value(i);
                            let secs = ms.div_euclid(1_000);
                            let nanos = (ms.rem_euclid(1_000) as u32) * 1_000_000;
                            values.push(timestamp_secs_nanos_to_serial(
                                secs,
                                nanos,
                                0,
                                date_system,
                                col_label,
                            )?);
                        } else {
                            values.push(0.0);
                        }
                    }
                    Ok(OwnedColumnData::Serial {
                        values,
                        format: FormatId::DATETIME,
                        validity: copy_validity(array),
                    })
                }
                DataType::Time32(TimeUnit::Second) => {
                    let a = array.as_any().downcast_ref::<Time32SecondArray>().unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                f64::from(a.value(i)) / 86_400.0
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Serial {
                        values,
                        format: FormatId::TIME,
                        validity: copy_validity(array),
                    })
                }
                DataType::Time32(TimeUnit::Millisecond) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<Time32MillisecondArray>()
                        .unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                f64::from(a.value(i)) / 86_400_000.0
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Serial {
                        values,
                        format: FormatId::TIME,
                        validity: copy_validity(array),
                    })
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<Time64MicrosecondArray>()
                        .unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                (a.value(i) as f64) / 86_400_000_000.0
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Serial {
                        values,
                        format: FormatId::TIME,
                        validity: copy_validity(array),
                    })
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .unwrap();
                    let values = (0..len)
                        .map(|i| {
                            if a.is_valid(i) {
                                (a.value(i) as f64) / 86_400_000_000_000.0
                            } else {
                                0.0
                            }
                        })
                        .collect();
                    Ok(OwnedColumnData::Serial {
                        values,
                        format: FormatId::TIME,
                        validity: copy_validity(array),
                    })
                }
                DataType::Duration(unit) => {
                    let values: Vec<f64> = match unit {
                        TimeUnit::Second => {
                            let a = array
                                .as_any()
                                .downcast_ref::<DurationSecondArray>()
                                .unwrap();
                            (0..len)
                                .map(|i| {
                                    if a.is_valid(i) {
                                        (a.value(i) as f64) / 86_400.0
                                    } else {
                                        0.0
                                    }
                                })
                                .collect()
                        }
                        TimeUnit::Millisecond => {
                            let a = array
                                .as_any()
                                .downcast_ref::<DurationMillisecondArray>()
                                .unwrap();
                            (0..len)
                                .map(|i| {
                                    if a.is_valid(i) {
                                        (a.value(i) as f64) / 86_400_000.0
                                    } else {
                                        0.0
                                    }
                                })
                                .collect()
                        }
                        TimeUnit::Microsecond => {
                            let a = array
                                .as_any()
                                .downcast_ref::<DurationMicrosecondArray>()
                                .unwrap();
                            (0..len)
                                .map(|i| {
                                    if a.is_valid(i) {
                                        (a.value(i) as f64) / 86_400_000_000.0
                                    } else {
                                        0.0
                                    }
                                })
                                .collect()
                        }
                        TimeUnit::Nanosecond => {
                            let a = array
                                .as_any()
                                .downcast_ref::<DurationNanosecondArray>()
                                .unwrap();
                            (0..len)
                                .map(|i| {
                                    if a.is_valid(i) {
                                        (a.value(i) as f64) / 86_400_000_000_000.0
                                    } else {
                                        0.0
                                    }
                                })
                                .collect()
                        }
                    };
                    Ok(OwnedColumnData::Serial {
                        values,
                        format: FormatId::DURATION,
                        validity: copy_validity(array),
                    })
                }
                DataType::Timestamp(unit, tz) => {
                    let offset_secs = parse_arrow_tz_offset_secs(tz.as_deref(), col_label)?;
                    let mut values = Vec::with_capacity(len);
                    match unit {
                        TimeUnit::Second => {
                            let a = array
                                .as_any()
                                .downcast_ref::<TimestampSecondArray>()
                                .unwrap();
                            for i in 0..len {
                                if a.is_valid(i) {
                                    values.push(timestamp_secs_nanos_to_serial(
                                        a.value(i),
                                        0,
                                        offset_secs,
                                        date_system,
                                        col_label,
                                    )?);
                                } else {
                                    values.push(0.0);
                                }
                            }
                        }
                        TimeUnit::Millisecond => {
                            let a = array
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .unwrap();
                            for i in 0..len {
                                if a.is_valid(i) {
                                    let v = a.value(i);
                                    values.push(timestamp_secs_nanos_to_serial(
                                        v.div_euclid(1_000),
                                        (v.rem_euclid(1_000) as u32) * 1_000_000,
                                        offset_secs,
                                        date_system,
                                        col_label,
                                    )?);
                                } else {
                                    values.push(0.0);
                                }
                            }
                        }
                        TimeUnit::Microsecond => {
                            let a = array
                                .as_any()
                                .downcast_ref::<TimestampMicrosecondArray>()
                                .unwrap();
                            for i in 0..len {
                                if a.is_valid(i) {
                                    let v = a.value(i);
                                    values.push(timestamp_secs_nanos_to_serial(
                                        v.div_euclid(1_000_000),
                                        (v.rem_euclid(1_000_000) as u32) * 1_000,
                                        offset_secs,
                                        date_system,
                                        col_label,
                                    )?);
                                } else {
                                    values.push(0.0);
                                }
                            }
                        }
                        TimeUnit::Nanosecond => {
                            let a = array
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .unwrap();
                            for i in 0..len {
                                if a.is_valid(i) {
                                    let v = a.value(i);
                                    values.push(timestamp_secs_nanos_to_serial(
                                        v.div_euclid(1_000_000_000),
                                        v.rem_euclid(1_000_000_000) as u32,
                                        offset_secs,
                                        date_system,
                                        col_label,
                                    )?);
                                } else {
                                    values.push(0.0);
                                }
                            }
                        }
                    }
                    Ok(OwnedColumnData::Serial {
                        values,
                        format: FormatId::DATETIME,
                        validity: copy_validity(array),
                    })
                }
                DataType::Dictionary(key_type, value_type) => {
                    convert_dictionary(array, key_type, value_type, date_system, col_label)
                }
                other => Err(arrow_err(
                    col_label,
                    format!("unsupported Arrow DataType {other:?}"),
                )),
            }
        }
    }
}
