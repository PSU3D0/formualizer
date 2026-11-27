//! Bulk ingest conversions for Python (`set_values`, `set_columns`, `set_cells`)
//! and hand-rolled Arrow PyCapsule FFI import (`__arrow_c_stream__` / `__arrow_c_array__`).
//!
//! Safety & lock ordering invariant (ADR Decision 3 / REDTEAM §6):
//! All Arrow streams and Python iterables MUST be drained and converted into
//! engine-owned buffers BEFORE acquiring the `Workbook` write lock. A stream
//! backed by a Python iterator executes arbitrary Python code in `get_next`;
//! holding the workbook write lock while draining would deadlock if that code
//! touches the workbook.

use std::collections::HashMap;
use std::ffi::CStr;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchReader, StructArray, make_array};
use formualizer::common::LiteralValue;
use formualizer::common::error::ExcelErrorKind;
use formualizer::eval::arrow_store::map_error_code;
use formualizer::eval::engine::{BulkCellTag, DateSystem, WorkbookLoadLimits};
use formualizer::eval::format::FormatId;
use formualizer::workbook::{CellBatch, ColumnData, OwnedColumnData};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyCapsule, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};

use crate::value::{MAX_SAFE_F64_INT, check_safe_py_int, py_to_cell_literal};

const MAX_EXCEL_ROWS: u32 = 1_048_576;
const MAX_EXCEL_COLS: u32 = 16_384;
const MAX_SAFE_U64: u64 = 1_u64 << 53;

const CAPSULE_STREAM: &CStr = c"arrow_array_stream";
const CAPSULE_SCHEMA: &CStr = c"arrow_schema";
const CAPSULE_ARRAY: &CStr = c"arrow_array";

/// Prepared columnar payload ready to commit to a [`formualizer::workbook::Workbook`]
/// without holding the GIL or referencing any foreign Arrow/Python buffers.
pub(crate) struct PreparedColumns {
    pub headers: Option<Vec<String>>,
    pub batches: Vec<(usize, Vec<OwnedColumnData>)>,
    pub total_rows: usize,
    pub ncols: usize,
}

impl PreparedColumns {
    pub(crate) fn total_cells(&self) -> usize {
        let header_cells = if self.headers.is_some() {
            self.ncols
        } else {
            0
        };
        self.total_rows
            .saturating_mul(self.ncols)
            .saturating_add(header_cells)
    }
}

/// Owned backing buffers for a sparse [`CellBatch`].
pub(crate) struct OwnedCellBatch {
    pub rows: Vec<u32>,
    pub cols: Vec<u32>,
    pub tags: Vec<u8>,
    pub numbers: Vec<f64>,
    pub text_bytes: Vec<u8>,
    pub text_offsets: Vec<u32>,
}

impl OwnedCellBatch {
    pub(crate) fn as_cell_batch(&self) -> CellBatch<'_> {
        CellBatch {
            rows: &self.rows,
            cols: &self.cols,
            tags: &self.tags,
            numbers: &self.numbers,
            text_bytes: &self.text_bytes,
            text_offsets: &self.text_offsets,
        }
    }
}

/// Error produced during detached Arrow stream draining (must be `Send`, no `PyErr`).
pub(crate) enum DetachedPrepError {
    Value(String),
    Type(String),
}

impl DetachedPrepError {
    pub(crate) fn into_pyerr(self) -> PyErr {
        match self {
            Self::Value(msg) => pyo3::exceptions::PyValueError::new_err(msg),
            Self::Type(msg) => pyo3::exceptions::PyTypeError::new_err(msg),
        }
    }
}

fn check_dense_bounds(
    start_row: u32,
    start_col: u32,
    total_rows: usize,
    ncols: usize,
    has_headers: bool,
    limits: &WorkbookLoadLimits,
) -> Result<(), DetachedPrepError> {
    if ncols == 0 {
        return Ok(());
    }
    let effective_rows = total_rows.saturating_add(usize::from(has_headers));
    if effective_rows == 0 {
        return Ok(());
    }
    let col_span = u32::try_from(ncols - 1)
        .map_err(|_| DetachedPrepError::Value("Column count exceeds Excel grid limit".into()))?;
    let end_col = start_col
        .checked_add(col_span)
        .ok_or_else(|| DetachedPrepError::Value("Column extent overflows Excel grid".into()))?;
    if end_col > MAX_EXCEL_COLS {
        return Err(DetachedPrepError::Value(format!(
            "Write extends to column {end_col}, exceeding Excel limit ({MAX_EXCEL_COLS})"
        )));
    }
    if end_col > limits.max_sheet_cols {
        return Err(DetachedPrepError::Value(format!(
            "Write extends to {end_col} columns, exceeding configured write column budget ({})",
            limits.max_sheet_cols
        )));
    }
    let row_span = u32::try_from(effective_rows - 1)
        .map_err(|_| DetachedPrepError::Value("Row count exceeds Excel grid limit".into()))?;
    let end_row = start_row
        .checked_add(row_span)
        .ok_or_else(|| DetachedPrepError::Value("Row extent overflows Excel grid".into()))?;
    if end_row > MAX_EXCEL_ROWS {
        return Err(DetachedPrepError::Value(format!(
            "Write extends to row {end_row}, exceeding Excel limit ({MAX_EXCEL_ROWS})"
        )));
    }
    if end_row > limits.max_sheet_rows {
        return Err(DetachedPrepError::Value(format!(
            "Write extends to {end_row} rows, exceeding configured write row budget ({})",
            limits.max_sheet_rows
        )));
    }
    let total_cells = (effective_rows as u64).saturating_mul(ncols as u64);
    if total_cells > limits.max_sheet_logical_cells {
        return Err(DetachedPrepError::Value(format!(
            "Write batch of {total_cells} cells exceeds configured write cell budget ({})",
            limits.max_sheet_logical_cells
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Hand-rolled Arrow PyCapsule import (zero external pyo3-arrow crate)
// ---------------------------------------------------------------------------

fn has_callable_attr(obj: &Bound<'_, PyAny>, name: &str) -> bool {
    obj.getattr(name)
        .map(|attr| attr.is_callable())
        .unwrap_or(false)
}

fn take_stream_from_capsule(capsule_obj: &Bound<'_, PyAny>) -> PyResult<ArrowArrayStreamReader> {
    let capsule = capsule_obj.cast::<PyCapsule>().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "__arrow_c_stream__ did not return a PyCapsule('arrow_array_stream')",
        )
    })?;
    let ptr = capsule.pointer_checked(Some(CAPSULE_STREAM)).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid Arrow stream capsule (expected name 'arrow_array_stream'): {e}"
        ))
    })?;
    // SAFETY: `pointer_checked` verified the capsule name is `"arrow_array_stream"`
    // and the pointer is non-null. Per the Arrow C Stream PyCapsule protocol, we
    // move the `FFI_ArrowArrayStream` out and replace it in-place with an empty
    // (released, `release = NULL`) stream so the capsule destructor is a no-op
    // and cannot double-free on Python GC.
    let stream = unsafe {
        let raw = ptr.as_ptr().cast::<FFI_ArrowArrayStream>();
        std::ptr::replace(raw, FFI_ArrowArrayStream::empty())
    };
    ArrowArrayStreamReader::try_new(stream).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Failed to import Arrow stream: {e}"))
    })
}

fn take_array_from_capsules(
    schema_obj: &Bound<'_, PyAny>,
    array_obj: &Bound<'_, PyAny>,
) -> PyResult<ArrayRef> {
    let schema_capsule = schema_obj.cast::<PyCapsule>().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "__arrow_c_array__ first element must be a PyCapsule('arrow_schema')",
        )
    })?;
    let array_capsule = array_obj.cast::<PyCapsule>().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "__arrow_c_array__ second element must be a PyCapsule('arrow_array')",
        )
    })?;
    let schema_ptr = schema_capsule
        .pointer_checked(Some(CAPSULE_SCHEMA))
        .map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Invalid Arrow schema capsule (expected name 'arrow_schema'): {e}"
            ))
        })?;
    let array_ptr = array_capsule
        .pointer_checked(Some(CAPSULE_ARRAY))
        .map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "Invalid Arrow array capsule (expected name 'arrow_array'): {e}"
            ))
        })?;

    // SAFETY: Both capsule names were checked against the Arrow C Data Interface
    // PyCapsule specification (`"arrow_schema"` and `"arrow_array"`). Per the C Data
    // Interface specification, a NULL `release` callback marks an already-released
    // struct that must not be consumed. Verify both callbacks (and schema `format`)
    // before moving either struct out so a reused or half-released pair never panics
    // or leaks the sibling capsule.
    let schema_raw = schema_ptr.as_ptr().cast::<FFI_ArrowSchema>();
    let array_raw = array_ptr.as_ptr().cast::<FFI_ArrowArray>();
    let (schema_released, array_released, schema_format_null) = unsafe {
        (
            (*schema_raw).release.is_none(),
            (*array_raw).release.is_none(),
            (*schema_raw).format.is_null(),
        )
    };
    if schema_released || array_released {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Arrow PyCapsule is already released (cannot consume the same capsule twice)",
        ));
    }
    if schema_format_null {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Invalid Arrow schema capsule: format pointer is null",
        ));
    }

    // SAFETY: Both structs have non-null `release` callbacks and valid capsule names.
    // Moving both structs out and replacing them with empty (`release = NULL`) sentinels
    // transfers ownership cleanly so capsule GC does not double-free.
    let (ffi_schema, ffi_array) = unsafe {
        let s = std::ptr::replace(schema_raw, FFI_ArrowSchema::empty());
        let a = std::ptr::replace(array_raw, FFI_ArrowArray::empty());
        (s, a)
    };

    // SAFETY: `ffi_array` and `ffi_schema` were produced together by `__arrow_c_array__`.
    let array_data = unsafe { from_ffi(ffi_array, &ffi_schema) }.map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Failed to import Arrow array: {e}"))
    })?;
    array_data.validate_data().map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid Arrow array data/offsets: {e}"))
    })?;
    Ok(make_array(array_data))
}

fn extract_arrow_stream(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
) -> PyResult<ArrowArrayStreamReader> {
    let capsule = obj.call_method1("__arrow_c_stream__", (py.None(),))?;
    take_stream_from_capsule(&capsule)
}

fn extract_arrow_array(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    let pair = obj.call_method1("__arrow_c_array__", (py.None(),))?;
    let tuple = pair.cast::<PyTuple>().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "__arrow_c_array__ must return a (schema_capsule, array_capsule) 2-tuple",
        )
    })?;
    if tuple.len() != 2 {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "__arrow_c_array__ must return a 2-tuple of PyCapsules",
        ));
    }
    take_array_from_capsules(&tuple.get_item(0)?, &tuple.get_item(1)?)
}

fn convert_record_batch_columns(
    batch: &RecordBatch,
    date_system: DateSystem,
) -> Result<Vec<OwnedColumnData>, DetachedPrepError> {
    let schema = batch.schema();
    let ncols = schema.fields().len();
    let mut owned_cols = Vec::with_capacity(ncols);
    for (ci, field) in schema.fields().iter().enumerate() {
        let col_arr = batch.column(ci);
        col_arr.to_data().validate_data().map_err(|e| {
            DetachedPrepError::Value(format!(
                "Column '{}': invalid Arrow offsets/buffers: {e}",
                field.name()
            ))
        })?;
        let owned = ColumnData::try_from_arrow_with_options(
            col_arr.as_ref(),
            date_system,
            Some(field.name()),
        )
        .map_err(|e| DetachedPrepError::Value(e.to_string()))?;
        owned_cols.push(owned);
    }
    Ok(owned_cols)
}

fn drain_arrow_stream_detached(
    reader: ArrowArrayStreamReader,
    start_row: u32,
    start_col: u32,
    headers: bool,
    date_system: DateSystem,
    limits: WorkbookLoadLimits,
) -> Result<PreparedColumns, DetachedPrepError> {
    let schema = reader.schema();
    let ncols = schema.fields().len();
    let header_names = if headers && ncols > 0 {
        Some(
            schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };

    check_dense_bounds(
        start_row,
        start_col,
        0,
        ncols,
        header_names.is_some(),
        &limits,
    )?;

    let mut total_rows: usize = 0;
    let mut batches: Vec<(usize, Vec<OwnedColumnData>)> = Vec::new();

    for batch_res in reader {
        let batch = batch_res.map_err(|e| {
            DetachedPrepError::Value(format!("Arrow stream error while reading batch: {e}"))
        })?;
        if batch.num_columns() != ncols {
            return Err(DetachedPrepError::Value(format!(
                "Arrow stream column count mismatch: expected {ncols}, got {}",
                batch.num_columns()
            )));
        }
        let nrows = batch.num_rows();
        if nrows == 0 || ncols == 0 {
            continue;
        }
        total_rows = total_rows.checked_add(nrows).ok_or_else(|| {
            DetachedPrepError::Value("Arrow stream row count overflows usize".into())
        })?;
        check_dense_bounds(
            start_row,
            start_col,
            total_rows,
            ncols,
            header_names.is_some(),
            &limits,
        )?;
        let owned_cols = convert_record_batch_columns(&batch, date_system)?;
        // `batch` drops here, releasing foreign Arrow buffers immediately.
        batches.push((nrows, owned_cols));
    }

    Ok(PreparedColumns {
        headers: header_names,
        batches,
        total_rows,
        ncols,
    })
}

fn convert_arrow_array_as_table_detached(
    array: ArrayRef,
    start_row: u32,
    start_col: u32,
    headers: bool,
    date_system: DateSystem,
    limits: WorkbookLoadLimits,
) -> Result<PreparedColumns, DetachedPrepError> {
    let struct_arr = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DetachedPrepError::Type(
                "__arrow_c_array__ passed directly to set_columns must be a StructArray / RecordBatch; \
                 wrap a single column Arrow array in a list or dict"
                    .into(),
            )
        })?;
    if struct_arr.null_count() > 0 {
        return Err(DetachedPrepError::Value(format!(
            "Arrow StructArray passed to set_columns cannot contain top-level null rows ({} null row(s))",
            struct_arr.null_count()
        )));
    }
    let batch = RecordBatch::from(struct_arr);
    let schema = batch.schema();
    let ncols = schema.fields().len();
    let nrows = batch.num_rows();
    let header_names = if headers && ncols > 0 {
        Some(
            schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };

    check_dense_bounds(
        start_row,
        start_col,
        nrows,
        ncols,
        header_names.is_some(),
        &limits,
    )?;

    let mut batches = Vec::new();
    if nrows > 0 && ncols > 0 {
        let owned_cols = convert_record_batch_columns(&batch, date_system)?;
        batches.push((nrows, owned_cols));
    }
    Ok(PreparedColumns {
        headers: header_names,
        batches,
        total_rows: nrows,
        ncols,
    })
}

// ---------------------------------------------------------------------------
// Buffer-protocol & Python sequence column converters
// ---------------------------------------------------------------------------

fn check_safe_i64_slice(values: &[i64], col_label: &str) -> PyResult<()> {
    for (i, &v) in values.iter().enumerate() {
        if !(-MAX_SAFE_F64_INT..=MAX_SAFE_F64_INT).contains(&v) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Column '{col_label}': row {i}: integer {v} exceeds exact f64 range (±2^53); cast explicitly to text or float"
            )));
        }
    }
    Ok(())
}

/// Attempt to import a 1D buffer-protocol object (`array.array`, `memoryview`,
/// `numpy.ndarray`, etc.) using `memoryview` (100% `abi3-py310` & Pyodide safe,
/// zero unsafe, single C copy via `memoryview.tobytes()`).
///
/// Returns `Ok(Some(col))` if handled via buffer protocol, `Ok(None)` if the
/// object does not expose a supported numeric/bool buffer (caller falls back
/// to Python sequence iteration), or `Err(...)` on validation failure.
fn try_convert_buffer_column(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    col_label: &str,
) -> PyResult<Option<OwnedColumnData>> {
    // Reject strings/bytes early (bytes/bytearray support buffer protocol but are not numeric columns).
    if obj.is_instance_of::<PyString>()
        || obj.is_instance_of::<PyBytes>()
        || obj.is_instance_of::<PyByteArray>()
    {
        return Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "Column '{col_label}': expected a column sequence or array, got {}",
            obj.get_type().name()?
        )));
    }
    // Fast skip for plain lists and tuples which never implement the buffer protocol.
    if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
        return Ok(None);
    }

    let builtins = py.import("builtins")?;
    let mv = match builtins.getattr("memoryview")?.call1((obj,)) {
        Ok(mv) => mv,
        Err(err) if err.is_instance_of::<pyo3::exceptions::PyTypeError>(py) => return Ok(None),
        Err(err) => return Err(err),
    };

    let result = (|| -> PyResult<Option<OwnedColumnData>> {
        let ndim: usize = mv.getattr("ndim")?.extract()?;
        if ndim != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Column '{col_label}': buffer must be 1-dimensional (got {ndim} dimensions)"
            )));
        }
        let format_str: String = mv.getattr("format")?.extract()?;
        // Strip native byte-order prefix ('@', '=') if present.
        let fmt = format_str
            .strip_prefix('@')
            .or_else(|| format_str.strip_prefix('='))
            .unwrap_or(&format_str);
        let itemsize: usize = mv.getattr("itemsize")?.extract()?;

        // Only handle standard native-endian scalar numeric/bool formats via fast byte copy.
        let handled = matches!(
            fmt,
            "d" | "f" | "b" | "B" | "h" | "H" | "i" | "I" | "l" | "L" | "q" | "Q" | "?"
        );
        if !handled {
            return Ok(None);
        }

        let raw_obj = mv.call_method0("tobytes")?;
        let raw_bytes = raw_obj.cast::<PyBytes>()?.as_bytes();

        let col = match (fmt, itemsize) {
            ("d", 8) => {
                let values = raw_bytes
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|c| f64::from_ne_bytes(*c))
                    .collect();
                OwnedColumnData::Float64 {
                    values,
                    validity: None,
                }
            }
            ("f", 4) => {
                let values = raw_bytes
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| f64::from(f32::from_ne_bytes(*c)))
                    .collect();
                OwnedColumnData::Float64 {
                    values,
                    validity: None,
                }
            }
            ("b", 1) => {
                let values = raw_bytes.iter().map(|&b| f64::from(b as i8)).collect();
                OwnedColumnData::Float64 {
                    values,
                    validity: None,
                }
            }
            ("B", 1) => {
                let values = raw_bytes.iter().map(|&b| f64::from(b)).collect();
                OwnedColumnData::Float64 {
                    values,
                    validity: None,
                }
            }
            ("h", 2) => {
                let values = raw_bytes
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|c| f64::from(i16::from_ne_bytes(*c)))
                    .collect();
                OwnedColumnData::Float64 {
                    values,
                    validity: None,
                }
            }
            ("H", 2) => {
                let values = raw_bytes
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|c| f64::from(u16::from_ne_bytes(*c)))
                    .collect();
                OwnedColumnData::Float64 {
                    values,
                    validity: None,
                }
            }
            ("i", 4) | ("l", 4) => {
                let values = raw_bytes
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| f64::from(i32::from_ne_bytes(*c)))
                    .collect();
                OwnedColumnData::Float64 {
                    values,
                    validity: None,
                }
            }
            ("I", 4) | ("L", 4) => {
                let values = raw_bytes
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|c| f64::from(u32::from_ne_bytes(*c)))
                    .collect();
                OwnedColumnData::Float64 {
                    values,
                    validity: None,
                }
            }
            ("q", 8) | ("l", 8) => {
                let values: Vec<i64> = raw_bytes
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|c| i64::from_ne_bytes(*c))
                    .collect();
                check_safe_i64_slice(&values, col_label)?;
                OwnedColumnData::Int64 {
                    values,
                    validity: None,
                }
            }
            ("Q", 8) | ("L", 8) => {
                let mut values = Vec::with_capacity(raw_bytes.len() / 8);
                for (i, c) in raw_bytes.as_chunks::<8>().0.iter().enumerate() {
                    let v = u64::from_ne_bytes(*c);
                    if v > MAX_SAFE_U64 {
                        return Err(pyo3::exceptions::PyValueError::new_err(format!(
                            "Column '{col_label}': row {i}: unsigned integer {v} exceeds exact f64 range (2^53); cast explicitly to text or float"
                        )));
                    }
                    values.push(v as f64);
                }
                OwnedColumnData::Float64 {
                    values,
                    validity: None,
                }
            }
            ("?", 1) => {
                let values = raw_bytes.iter().map(|&b| b != 0).collect();
                OwnedColumnData::Boolean {
                    values,
                    validity: None,
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(col))
    })();

    let _ = mv.call_method0("release");
    result
}

/// Deduplicated UTF-8 string table builder for packed wire blocks (`Mixed` / `CellBatch`).
struct StringTableBuilder {
    index: HashMap<String, usize>,
    bytes: Vec<u8>,
    offsets: Vec<u32>,
}

impl StringTableBuilder {
    fn new() -> Self {
        Self {
            index: HashMap::new(),
            bytes: Vec::new(),
            offsets: vec![0u32],
        }
    }

    fn intern(&mut self, s: &str, context: &str) -> PyResult<usize> {
        if let Some(&idx) = self.index.get(s) {
            return Ok(idx);
        }
        let idx = self.offsets.len() - 1;
        self.bytes.extend_from_slice(s.as_bytes());
        let end = u32::try_from(self.bytes.len()).map_err(|_| {
            pyo3::exceptions::PyValueError::new_err(format!(
                "{context}: packed string table exceeds 4 GiB u32 offset limit"
            ))
        })?;
        self.offsets.push(end);
        self.index.insert(s.to_owned(), idx);
        Ok(idx)
    }

    fn finish(self) -> (Vec<u8>, Vec<u32>) {
        if self.offsets.len() <= 1 {
            (Vec::new(), Vec::new())
        } else {
            (self.bytes, self.offsets)
        }
    }
}

fn push_literal_to_wire(
    lit: &LiteralValue,
    date_system: DateSystem,
    tags: &mut Vec<u8>,
    numbers: &mut Vec<f64>,
    strings: &mut StringTableBuilder,
    context: &str,
) -> PyResult<()> {
    match lit {
        LiteralValue::Empty => {
            tags.push(BulkCellTag::Empty as u8);
            numbers.push(0.0);
        }
        LiteralValue::Int(i) => {
            tags.push(BulkCellTag::Number as u8);
            numbers.push(*i as f64);
        }
        LiteralValue::Number(n) => {
            if n.is_finite() {
                tags.push(BulkCellTag::Number as u8);
                numbers.push(*n);
            } else {
                tags.push(BulkCellTag::Error as u8);
                numbers.push(f64::from(map_error_code(ExcelErrorKind::Num)));
            }
        }
        LiteralValue::Boolean(b) => {
            tags.push(BulkCellTag::Boolean as u8);
            numbers.push(if *b { 1.0 } else { 0.0 });
        }
        LiteralValue::Text(s) => {
            let idx = strings.intern(s, context)?;
            tags.push(BulkCellTag::Text as u8);
            numbers.push(idx as f64);
        }
        LiteralValue::Error(e) => {
            tags.push(BulkCellTag::Error as u8);
            numbers.push(f64::from(map_error_code(e.kind)));
        }
        LiteralValue::Date(d) => {
            let dt = d.and_hms_opt(0, 0, 0).unwrap();
            let serial = formualizer::common::datetime_to_serial_for(date_system, &dt);
            tags.push(BulkCellTag::Date as u8);
            numbers.push(serial);
        }
        LiteralValue::DateTime(dt) => {
            let serial = formualizer::common::datetime_to_serial_for(date_system, dt);
            tags.push(BulkCellTag::DateTime as u8);
            numbers.push(serial);
        }
        LiteralValue::Time(t) => {
            let serial = formualizer::common::time_to_fraction(t);
            tags.push(BulkCellTag::Time as u8);
            numbers.push(serial);
        }
        LiteralValue::Duration(d) => {
            let serial = formualizer::common::duration_to_days_fraction(d);
            tags.push(BulkCellTag::Duration as u8);
            numbers.push(serial);
        }
        LiteralValue::Pending => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "{context}: Pending literal values cannot be written via bulk ingest"
            )));
        }
        LiteralValue::Array(_) => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "{context}: Cannot store an array in a single cell; use set_values"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HomogKind {
    EmptyOnly,
    Number,
    Boolean,
    Text,
    Date,
    DateTime,
    Time,
    Duration,
    Mixed,
}

fn convert_sequence_column(
    obj: &Bound<'_, PyAny>,
    col_label: &str,
    date_system: DateSystem,
) -> PyResult<OwnedColumnData> {
    if obj.is_instance_of::<PyString>()
        || obj.is_instance_of::<PyBytes>()
        || obj.is_instance_of::<PyByteArray>()
    {
        return Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "Column '{col_label}': expected a column sequence or array, got {}",
            obj.get_type().name()?
        )));
    }

    let iter = obj.try_iter().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(format!(
            "Column '{col_label}': expected a sequence or iterable"
        ))
    })?;

    let mut literals = Vec::with_capacity(obj.len().unwrap_or(0));
    let mut kind = HomogKind::EmptyOnly;

    for (row_idx, item_res) in iter.enumerate() {
        let item = item_res?;
        let lit = py_to_cell_literal(&item, Some(col_label)).map_err(|err| {
            // Attach row index if not already present.
            if err.is_instance_of::<pyo3::exceptions::PyValueError>(item.py()) {
                let msg = err.value(item.py()).to_string();
                if msg.contains("Column '") {
                    err
                } else {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "Column '{col_label}': row {row_idx}: {msg}"
                    ))
                }
            } else {
                err
            }
        })?;
        let item_kind = match &lit {
            LiteralValue::Empty => HomogKind::EmptyOnly,
            LiteralValue::Int(_) | LiteralValue::Number(_) => HomogKind::Number,
            LiteralValue::Boolean(_) => HomogKind::Boolean,
            LiteralValue::Text(_) => HomogKind::Text,
            LiteralValue::Date(_) => HomogKind::Date,
            LiteralValue::DateTime(_) => HomogKind::DateTime,
            LiteralValue::Time(_) => HomogKind::Time,
            LiteralValue::Duration(_) => HomogKind::Duration,
            LiteralValue::Error(_) | LiteralValue::Pending | LiteralValue::Array(_) => {
                HomogKind::Mixed
            }
        };
        if item_kind != HomogKind::EmptyOnly {
            if kind == HomogKind::EmptyOnly {
                kind = item_kind;
            } else if kind != item_kind {
                kind = HomogKind::Mixed;
            }
        }
        literals.push(lit);
    }

    let len = literals.len();
    if len == 0 || kind == HomogKind::EmptyOnly {
        return Ok(OwnedColumnData::Null { len });
    }

    let build_validity = |lits: &[LiteralValue]| -> Option<Vec<u8>> {
        let has_null = lits.iter().any(|l| matches!(l, LiteralValue::Empty));
        if !has_null {
            return None;
        }
        let mut bits = vec![0u8; len.div_ceil(8)];
        for (i, l) in lits.iter().enumerate() {
            if !matches!(l, LiteralValue::Empty) {
                bits[i >> 3] |= 1u8 << (i & 7);
            }
        }
        Some(bits)
    };

    match kind {
        HomogKind::EmptyOnly => Ok(OwnedColumnData::Null { len }),
        HomogKind::Number => {
            let validity = build_validity(&literals);
            let values = literals
                .into_iter()
                .map(|l| match l {
                    LiteralValue::Int(i) => i as f64,
                    LiteralValue::Number(n) => n,
                    _ => 0.0,
                })
                .collect();
            Ok(OwnedColumnData::Float64 { values, validity })
        }
        HomogKind::Boolean => {
            let validity = build_validity(&literals);
            let values = literals
                .into_iter()
                .map(|l| matches!(l, LiteralValue::Boolean(true)))
                .collect();
            Ok(OwnedColumnData::Boolean { values, validity })
        }
        HomogKind::Text => {
            let validity = build_validity(&literals);
            let mut bytes = Vec::new();
            let mut offsets = Vec::with_capacity(len + 1);
            offsets.push(0u32);
            for l in literals {
                if let LiteralValue::Text(s) = l {
                    bytes.extend_from_slice(s.as_bytes());
                }
                let off = u32::try_from(bytes.len()).map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "Column '{col_label}': UTF-8 buffer exceeds 4 GiB u32 offset limit"
                    ))
                })?;
                offsets.push(off);
            }
            Ok(OwnedColumnData::Utf8 {
                bytes,
                offsets,
                validity,
            })
        }
        HomogKind::Date => {
            let validity = build_validity(&literals);
            let values = literals
                .into_iter()
                .map(|l| match l {
                    LiteralValue::Date(d) => {
                        let dt = d.and_hms_opt(0, 0, 0).unwrap();
                        formualizer::common::datetime_to_serial_for(date_system, &dt)
                    }
                    _ => 0.0,
                })
                .collect();
            Ok(OwnedColumnData::Serial {
                values,
                format: FormatId::DATE,
                validity,
            })
        }
        HomogKind::DateTime => {
            let validity = build_validity(&literals);
            let values = literals
                .into_iter()
                .map(|l| match l {
                    LiteralValue::DateTime(dt) => {
                        formualizer::common::datetime_to_serial_for(date_system, &dt)
                    }
                    _ => 0.0,
                })
                .collect();
            Ok(OwnedColumnData::Serial {
                values,
                format: FormatId::DATETIME,
                validity,
            })
        }
        HomogKind::Time => {
            let validity = build_validity(&literals);
            let values = literals
                .into_iter()
                .map(|l| match l {
                    LiteralValue::Time(t) => formualizer::common::time_to_fraction(&t),
                    _ => 0.0,
                })
                .collect();
            Ok(OwnedColumnData::Serial {
                values,
                format: FormatId::TIME,
                validity,
            })
        }
        HomogKind::Duration => {
            let validity = build_validity(&literals);
            let values = literals
                .into_iter()
                .map(|l| match l {
                    LiteralValue::Duration(d) => formualizer::common::duration_to_days_fraction(&d),
                    _ => 0.0,
                })
                .collect();
            Ok(OwnedColumnData::Serial {
                values,
                format: FormatId::DURATION,
                validity,
            })
        }
        HomogKind::Mixed => {
            let mut tags = Vec::with_capacity(len);
            let mut numbers = Vec::with_capacity(len);
            let mut strings = StringTableBuilder::new();
            let ctx = format!("Column '{col_label}'");
            for lit in &literals {
                push_literal_to_wire(
                    lit,
                    date_system,
                    &mut tags,
                    &mut numbers,
                    &mut strings,
                    &ctx,
                )?;
            }
            let (text_bytes, text_offsets) = strings.finish();
            Ok(OwnedColumnData::Mixed {
                tags,
                numbers,
                text_bytes,
                text_offsets,
            })
        }
    }
}

fn convert_single_column(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    col_label: &str,
    date_system: DateSystem,
) -> PyResult<OwnedColumnData> {
    if has_callable_attr(obj, "__arrow_c_array__") {
        let array = extract_arrow_array(py, obj)?;
        return ColumnData::try_from_arrow_with_options(
            array.as_ref(),
            date_system,
            Some(col_label),
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()));
    }
    if let Some(col) = try_convert_buffer_column(py, obj, col_label)? {
        return Ok(col);
    }
    convert_sequence_column(obj, col_label, date_system)
}

fn is_mapping(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    if obj.is_instance_of::<PyDict>() {
        return Ok(true);
    }
    let mapping_abc = py.import("collections.abc")?.getattr("Mapping")?;
    obj.is_instance(&mapping_abc)
}

/// Convert any supported `set_columns` input into engine-owned column buffers
/// completely BEFORE acquiring the workbook write lock.
pub(crate) fn prepare_columns_input(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    start_row: u32,
    start_col: u32,
    headers: bool,
    date_system: DateSystem,
    limits: WorkbookLoadLimits,
) -> PyResult<PreparedColumns> {
    // 1. Arrow PyCapsule Stream (Polars DataFrame, PyArrow Table/RecordBatchReader,
    //    DuckDB relation, pandas DataFrame with pyarrow installed, etc.)
    if has_callable_attr(data, "__arrow_c_stream__") {
        let reader = extract_arrow_stream(py, data)?;
        // CRITICAL: Drain the stream with GIL released and NO workbook lock held!
        let prepared = py
            .detach(move || {
                drain_arrow_stream_detached(
                    reader,
                    start_row,
                    start_col,
                    headers,
                    date_system,
                    limits,
                )
            })
            .map_err(DetachedPrepError::into_pyerr)?;
        return Ok(prepared);
    }

    // 2. Arrow PyCapsule Array (PyArrow RecordBatch / StructArray)
    if has_callable_attr(data, "__arrow_c_array__") {
        let array = extract_arrow_array(py, data)?;
        let prepared = py
            .detach(move || {
                convert_arrow_array_as_table_detached(
                    array,
                    start_row,
                    start_col,
                    headers,
                    date_system,
                    limits,
                )
            })
            .map_err(DetachedPrepError::into_pyerr)?;
        return Ok(prepared);
    }

    // Reject bare string / bytes / bytearray before sequence checks.
    if data.is_instance_of::<PyString>()
        || data.is_instance_of::<PyBytes>()
        || data.is_instance_of::<PyByteArray>()
    {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "set_columns expects an Arrow object, a mapping of column names to columns, or a sequence of columns",
        ));
    }

    // 3. Mapping of column name -> column
    if is_mapping(py, data)? {
        let mut names = Vec::new();
        let mut cols = Vec::new();
        if let Ok(dict) = data.cast::<PyDict>() {
            names.reserve(dict.len());
            cols.reserve(dict.len());
            for (k, v) in dict.iter() {
                let name: String = k.extract().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(
                        "set_columns mapping keys must be strings",
                    )
                })?;
                let col = convert_single_column(py, &v, &name, date_system)?;
                names.push(name);
                cols.push(col);
            }
        } else {
            let items = data.call_method0("items")?;
            for entry in items.try_iter()? {
                let entry = entry?;
                let pair = entry.cast::<PyTuple>().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(
                        "Mapping.items() must yield (name, column) pairs",
                    )
                })?;
                let name: String = pair.get_item(0)?.extract().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(
                        "set_columns mapping keys must be strings",
                    )
                })?;
                let v = pair.get_item(1)?;
                let col = convert_single_column(py, &v, &name, date_system)?;
                names.push(name);
                cols.push(col);
            }
        }

        let ncols = cols.len();
        let nrows = cols.first().map_or(0, OwnedColumnData::len);
        for (ci, col) in cols.iter().enumerate() {
            if col.len() != nrows {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Column '{}' length ({}) does not match column '{}' length ({nrows})",
                    names[ci],
                    col.len(),
                    names[0]
                )));
            }
        }
        let header_names = if headers && ncols > 0 {
            Some(names)
        } else {
            None
        };
        check_dense_bounds(
            start_row,
            start_col,
            nrows,
            ncols,
            header_names.is_some(),
            &limits,
        )
        .map_err(DetachedPrepError::into_pyerr)?;

        let batches = if nrows > 0 && ncols > 0 {
            vec![(nrows, cols)]
        } else {
            Vec::new()
        };
        return Ok(PreparedColumns {
            headers: header_names,
            batches,
            total_rows: nrows,
            ncols,
        });
    }

    // Reject multi-dimensional NumPy arrays / memoryviews passed directly so we
    // never silently iterate their rows and transpose them into columns.
    if let Ok(ndim_obj) = data.getattr("ndim")
        && !ndim_obj.is_instance_of::<PyBool>()
        && let Ok(ndim) = ndim_obj.extract::<usize>()
        && ndim >= 2
        && (data.hasattr("__array_interface__").unwrap_or(false)
            || data.get_type().name().is_ok_and(|n| n == "memoryview"))
    {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "set_columns does not accept a {ndim}-D array directly (iterating rows would silently transpose data); \
             use set_values for row-major 2-D arrays or pass explicit 1-D columns in a dict or list"
        )));
    }

    // 4. Sequence of columns (each a buffer-protocol array, Arrow array, or Python sequence)
    if headers {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "headers=True requires named columns (an Arrow table/RecordBatch or a dict mapping column names to columns)",
        ));
    }

    let iter = data.try_iter().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "set_columns expects an Arrow object, a mapping of column names to columns, or a sequence of columns",
        )
    })?;

    let mut cols = Vec::with_capacity(data.len().unwrap_or(0));
    for (ci, item_res) in iter.enumerate() {
        let item = item_res?;
        let label = format!("#{ci}");
        let col = convert_single_column(py, &item, &label, date_system)?;
        cols.push(col);
    }

    let ncols = cols.len();
    let nrows = cols.first().map_or(0, OwnedColumnData::len);
    for (ci, col) in cols.iter().enumerate() {
        if col.len() != nrows {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Column #{ci} length ({}) does not match column #0 length ({nrows})",
                col.len()
            )));
        }
    }
    check_dense_bounds(start_row, start_col, nrows, ncols, false, &limits)
        .map_err(DetachedPrepError::into_pyerr)?;

    let batches = if nrows > 0 && ncols > 0 {
        vec![(nrows, cols)]
    } else {
        Vec::new()
    };
    Ok(PreparedColumns {
        headers: None,
        batches,
        total_rows: nrows,
        ncols,
    })
}

// ---------------------------------------------------------------------------
// Dense row-major `set_values` preparation
// ---------------------------------------------------------------------------

pub(crate) fn prepare_values_rows(rows: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<LiteralValue>>> {
    if rows.is_instance_of::<PyString>()
        || rows.is_instance_of::<PyBytes>()
        || rows.is_instance_of::<PyByteArray>()
    {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "set_values expects a sequence of row sequences",
        ));
    }

    let iter = rows.try_iter().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err("set_values expects a sequence of row sequences")
    })?;

    let mut out = Vec::with_capacity(rows.len().unwrap_or(0));
    for row_res in iter {
        let row_obj = row_res?;
        if row_obj.is_instance_of::<PyString>()
            || row_obj.is_instance_of::<PyBytes>()
            || row_obj.is_instance_of::<PyByteArray>()
        {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "Each row in set_values must be a sequence of cell values, not str/bytes",
            ));
        }
        let mut row_vals = if let Ok(list) = row_obj.cast::<PyList>() {
            let mut v = Vec::with_capacity(list.len());
            for cell in list.iter() {
                v.push(py_to_cell_literal(&cell, None)?);
            }
            v
        } else if let Ok(tuple) = row_obj.cast::<PyTuple>() {
            let mut v = Vec::with_capacity(tuple.len());
            for cell in tuple.iter() {
                v.push(py_to_cell_literal(&cell, None)?);
            }
            v
        } else {
            let row_iter = row_obj.try_iter().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err(
                    "Each row in set_values must be a sequence of cell values",
                )
            })?;
            let mut v = Vec::with_capacity(row_obj.len().unwrap_or(0));
            for cell_res in row_iter {
                let cell = cell_res?;
                v.push(py_to_cell_literal(&cell, None)?);
            }
            v
        };
        row_vals.shrink_to_fit();
        out.push(row_vals);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Sparse `set_cells` preparation
// ---------------------------------------------------------------------------

fn extract_1based_coord(obj: &Bound<'_, PyAny>) -> PyResult<u32> {
    if obj.is_instance_of::<PyBool>() || !obj.is_instance_of::<PyInt>() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "Cell coordinates must be positive 1-based integers",
        ));
    }
    let val: i64 = obj.extract().map_err(|_| {
        pyo3::exceptions::PyValueError::new_err("Row/col coordinate out of 1-based u32 range")
    })?;
    if val <= 0 || val > i64::from(u32::MAX) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Row/col are 1-based",
        ));
    }
    Ok(val as u32)
}

struct SparseBatchBuilder {
    rows: Vec<u32>,
    cols: Vec<u32>,
    tags: Vec<u8>,
    numbers: Vec<f64>,
    strings: StringTableBuilder,
    date_system: DateSystem,
}

impl SparseBatchBuilder {
    fn with_capacity(cap: usize, date_system: DateSystem) -> Self {
        Self {
            rows: Vec::with_capacity(cap),
            cols: Vec::with_capacity(cap),
            tags: Vec::with_capacity(cap),
            numbers: Vec::with_capacity(cap),
            strings: StringTableBuilder::new(),
            date_system,
        }
    }

    fn push(&mut self, row: u32, col: u32, value: &Bound<'_, PyAny>) -> PyResult<()> {
        // Fast-path common Python primitives directly into wire buffers without intermediate LiteralValue.
        if value.is_none() {
            self.rows.push(row);
            self.cols.push(col);
            self.tags.push(BulkCellTag::Empty as u8);
            self.numbers.push(0.0);
            return Ok(());
        }
        if value.is_instance_of::<PyBool>() {
            let b = value.extract::<bool>()?;
            self.rows.push(row);
            self.cols.push(col);
            self.tags.push(BulkCellTag::Boolean as u8);
            self.numbers.push(if b { 1.0 } else { 0.0 });
            return Ok(());
        }
        if let Ok(py_int) = value.cast::<PyInt>() {
            let v = check_safe_py_int(py_int, None)?;
            self.rows.push(row);
            self.cols.push(col);
            self.tags.push(BulkCellTag::Number as u8);
            self.numbers.push(v as f64);
            return Ok(());
        }
        if value.is_instance_of::<PyFloat>() {
            let n = value.extract::<f64>()?;
            self.rows.push(row);
            self.cols.push(col);
            if n.is_finite() {
                self.tags.push(BulkCellTag::Number as u8);
                self.numbers.push(n);
            } else {
                self.tags.push(BulkCellTag::Error as u8);
                self.numbers
                    .push(f64::from(map_error_code(ExcelErrorKind::Num)));
            }
            return Ok(());
        }
        if let Ok(py_str) = value.cast::<PyString>() {
            let s = py_str.to_str()?;
            let idx = self.strings.intern(s, "set_cells")?;
            self.rows.push(row);
            self.cols.push(col);
            self.tags.push(BulkCellTag::Text as u8);
            self.numbers.push(idx as f64);
            return Ok(());
        }

        // General path (temporal types, LiteralValue, error dicts, array rejection, etc.)
        let lit = py_to_cell_literal(value, None)?;
        push_literal_to_wire(
            &lit,
            self.date_system,
            &mut self.tags,
            &mut self.numbers,
            &mut self.strings,
            "set_cells",
        )?;
        self.rows.push(row);
        self.cols.push(col);
        Ok(())
    }

    fn finish(self) -> OwnedCellBatch {
        let (text_bytes, text_offsets) = self.strings.finish();
        OwnedCellBatch {
            rows: self.rows,
            cols: self.cols,
            tags: self.tags,
            numbers: self.numbers,
            text_bytes,
            text_offsets,
        }
    }
}

/// Convert a mapping `{(row, col): value}` or iterable of `(row, col, value)` triples
/// into an [`OwnedCellBatch`] completely BEFORE acquiring the workbook lock.
pub(crate) fn prepare_sparse_cells(
    py: Python<'_>,
    cells: &Bound<'_, PyAny>,
    date_system: DateSystem,
) -> PyResult<OwnedCellBatch> {
    if cells.is_instance_of::<PyString>()
        || cells.is_instance_of::<PyBytes>()
        || cells.is_instance_of::<PyByteArray>()
    {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "set_cells expects an iterable of (row, col, value) triples or a mapping {(row, col): value}",
        ));
    }

    let hint = cells.len().unwrap_or(0);
    let mut builder = SparseBatchBuilder::with_capacity(hint, date_system);

    if is_mapping(py, cells)? {
        if let Ok(dict) = cells.cast::<PyDict>() {
            for (k, v) in dict.iter() {
                let pair = k.cast::<PyTuple>().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(
                        "set_cells mapping keys must be (row, col) 2-tuples",
                    )
                })?;
                if pair.len() != 2 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "set_cells mapping keys must be (row, col) 2-tuples",
                    ));
                }
                let r = extract_1based_coord(&pair.get_item(0)?)?;
                let c = extract_1based_coord(&pair.get_item(1)?)?;
                builder.push(r, c, &v)?;
            }
        } else {
            let items = cells.call_method0("items")?;
            for entry in items.try_iter()? {
                let entry = entry?;
                let kv = entry.cast::<PyTuple>().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(
                        "Mapping.items() must yield ((row, col), value) pairs",
                    )
                })?;
                let key = kv.get_item(0)?;
                let pair = key.cast::<PyTuple>().map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err(
                        "set_cells mapping keys must be (row, col) 2-tuples",
                    )
                })?;
                if pair.len() != 2 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "set_cells mapping keys must be (row, col) 2-tuples",
                    ));
                }
                let r = extract_1based_coord(&pair.get_item(0)?)?;
                let c = extract_1based_coord(&pair.get_item(1)?)?;
                let v = kv.get_item(1)?;
                builder.push(r, c, &v)?;
            }
        }
    } else {
        let iter = cells.try_iter().map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err(
                "set_cells expects an iterable of (row, col, value) triples or a mapping {(row, col): value}",
            )
        })?;
        for entry_res in iter {
            let entry = entry_res?;
            let (r_obj, c_obj, v_obj) = if let Ok(tup) = entry.cast::<PyTuple>() {
                if tup.len() != 3 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Each item in set_cells must be a (row, col, value) 3-tuple",
                    ));
                }
                (tup.get_item(0)?, tup.get_item(1)?, tup.get_item(2)?)
            } else if let Ok(lst) = entry.cast::<PyList>() {
                if lst.len() != 3 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Each item in set_cells must be a (row, col, value) triple",
                    ));
                }
                (lst.get_item(0)?, lst.get_item(1)?, lst.get_item(2)?)
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "Each item in set_cells must be a (row, col, value) tuple or list",
                ));
            };
            let r = extract_1based_coord(&r_obj)?;
            let c = extract_1based_coord(&c_obj)?;
            builder.push(r, c, &v_obj)?;
        }
    }

    Ok(builder.finish())
}
