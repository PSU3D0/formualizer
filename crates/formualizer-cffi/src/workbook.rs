#![allow(clippy::missing_safety_doc)]

use crate::guard::{catch_ffi, validate_block, validate_cell};
use crate::{fz_buffer, fz_encoding_format, fz_status};

use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_workbook::{
    LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::ffi::{CStr, c_char, c_int, c_uint};
use std::ptr;
use std::sync::{Arc, RwLock};

/// Message reported when the workbook `RwLock` has been poisoned by a panic in
/// another thread.
const POISONED: &str = "workbook lock poisoned";

pub struct OpaqueWorkbook(pub Arc<RwLock<Workbook>>);

#[repr(C)]
#[derive(Copy, Clone)]
pub struct fz_workbook_h(pub *mut std::ffi::c_void);

#[derive(Serialize, Deserialize)]
pub struct CffiEvalResult {
    pub computed_vertices: usize,
    pub cycle_errors: usize,
    pub elapsed_ms: u64,
}

#[derive(Serialize)]
struct CffiSheetDimensions {
    rows: u32,
    cols: u32,
}

#[derive(Deserialize)]
struct CffiCellTarget {
    sheet: String,
    row: u32,
    col: u32,
}

fn decode_payload<T: DeserializeOwned>(
    payload: *const u8,
    len: usize,
    format: fz_encoding_format,
) -> Result<T, String> {
    if payload.is_null() || len == 0 {
        return Err("empty payload".to_string());
    }
    let bytes = unsafe { std::slice::from_raw_parts(payload, len) };
    match format {
        fz_encoding_format::FZ_ENCODING_JSON => {
            serde_json::from_slice(bytes).map_err(|e| e.to_string())
        }
        fz_encoding_format::FZ_ENCODING_CBOR => {
            ciborium::from_reader(bytes).map_err(|e| e.to_string())
        }
    }
}

fn encode_payload<T: Serialize>(value: &T, format: fz_encoding_format) -> Result<Vec<u8>, String> {
    match format {
        fz_encoding_format::FZ_ENCODING_JSON => {
            serde_json::to_vec(value).map_err(|e| e.to_string())
        }
        fz_encoding_format::FZ_ENCODING_CBOR => {
            let mut buf = Vec::new();
            ciborium::into_writer(value, &mut buf)
                .map_err(|e| e.to_string())
                .map(|_| buf)
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_create(status: *mut fz_status) -> fz_workbook_h {
    catch_ffi(status, fz_workbook_h(ptr::null_mut()), || {
        let wb = Workbook::new();
        let opaque = Box::new(OpaqueWorkbook(Arc::new(RwLock::new(wb))));
        Ok(fz_workbook_h(Box::into_raw(opaque) as *mut std::ffi::c_void))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_open_xlsx(
    path: *const c_char,
    status: *mut fz_status,
) -> fz_workbook_h {
    unsafe { fz_workbook_open_xlsx_with_span_evaluation(path, false, status) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_open_xlsx_with_span_evaluation(
    path: *const c_char,
    span_evaluation: bool,
    status: *mut fz_status,
) -> fz_workbook_h {
    catch_ffi(status, fz_workbook_h(ptr::null_mut()), || {
        if path.is_null() {
            return Err("invalid arguments".to_string());
        }

        let path_str = unsafe { CStr::from_ptr(path).to_string_lossy() };
        let backend = UmyaAdapter::open_path(path_str.as_ref()).map_err(|e| e.to_string())?;

        let cfg = WorkbookConfig::interactive().with_span_evaluation(span_evaluation);
        let wb = Workbook::from_reader(backend, LoadStrategy::EagerAll, cfg)
            .map_err(|e| e.to_string())?;

        let opaque = Box::new(OpaqueWorkbook(Arc::new(RwLock::new(wb))));
        Ok(fz_workbook_h(Box::into_raw(opaque) as *mut std::ffi::c_void))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_free(wb: fz_workbook_h) {
    if !wb.0.is_null() {
        unsafe {
            let _ = Box::from_raw(wb.0 as *mut OpaqueWorkbook);
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_add_sheet(
    wb: fz_workbook_h,
    name: *const c_char,
    status: *mut fz_status,
) {
    catch_ffi(status, (), || {
        if wb.0.is_null() || name.is_null() {
            return Err("invalid arguments".to_string());
        }

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };

        let mut wb_lock = opaque.0.write().map_err(|_| POISONED.to_string())?;
        wb_lock.add_sheet(&name_str).map_err(|e| e.to_string())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_set_cell_value(
    wb: fz_workbook_h,
    sheet: *const c_char,
    row: c_uint,
    col: c_uint,
    value_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) {
    catch_ffi(status, (), || {
        if wb.0.is_null() || sheet.is_null() || value_payload.is_null() {
            return Err("invalid arguments".to_string());
        }
        validate_cell(row, col)?;

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };
        let value: LiteralValue = decode_payload(value_payload, len, format)?;

        let mut wb_lock = opaque.0.write().map_err(|_| POISONED.to_string())?;
        wb_lock
            .set_value(&sheet_str, row, col, value)
            .map_err(|e| e.to_string())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_set_cell_formula(
    wb: fz_workbook_h,
    sheet: *const c_char,
    row: c_uint,
    col: c_uint,
    formula: *const c_char,
    status: *mut fz_status,
) {
    catch_ffi(status, (), || {
        if wb.0.is_null() || sheet.is_null() || formula.is_null() {
            return Err("invalid arguments".to_string());
        }
        validate_cell(row, col)?;

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };
        let formula_str = unsafe { CStr::from_ptr(formula).to_string_lossy() };

        let mut wb_lock = opaque.0.write().map_err(|_| POISONED.to_string())?;
        wb_lock
            .set_formula(&sheet_str, row, col, &formula_str)
            .map_err(|e| e.to_string())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_get_cell_formula(
    wb: fz_workbook_h,
    sheet: *const c_char,
    row: c_uint,
    col: c_uint,
    status: *mut fz_status,
) -> fz_buffer {
    catch_ffi(status, fz_buffer::empty(), || {
        if wb.0.is_null() || sheet.is_null() {
            return Err("invalid arguments".to_string());
        }
        validate_cell(row, col)?;

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };

        let wb_lock = opaque.0.read().map_err(|_| POISONED.to_string())?;
        // An absent formula is not an error: report OK with an empty buffer.
        Ok(match wb_lock.get_formula(&sheet_str, row, col) {
            Some(f) => fz_buffer::from_vec(f.into_bytes()),
            None => fz_buffer::empty(),
        })
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_get_cell_value(
    wb: fz_workbook_h,
    sheet: *const c_char,
    row: c_uint,
    col: c_uint,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    catch_ffi(status, fz_buffer::empty(), || {
        if wb.0.is_null() || sheet.is_null() {
            return Err("invalid arguments".to_string());
        }
        validate_cell(row, col)?;

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };

        let value = {
            let wb_lock = opaque.0.read().map_err(|_| POISONED.to_string())?;
            wb_lock
                .get_value(&sheet_str, row, col)
                .unwrap_or(LiteralValue::Empty)
        };

        encode_payload(&value, format).map(fz_buffer::from_vec)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_evaluate_all(
    wb: fz_workbook_h,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    catch_ffi(status, fz_buffer::empty(), || {
        if wb.0.is_null() {
            return Err("invalid arguments".to_string());
        }

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let mut wb_lock = opaque.0.write().map_err(|_| POISONED.to_string())?;

        // Workbook needs to build graph if deferred
        wb_lock.prepare_graph_all().map_err(|e| e.to_string())?;

        let res = wb_lock.evaluate_all().map_err(|e| e.to_string())?;
        let cffi_res = CffiEvalResult {
            computed_vertices: res.computed_vertices,
            cycle_errors: res.cycle_errors,
            elapsed_ms: res.elapsed.as_millis() as u64,
        };

        encode_payload(&cffi_res, format).map(fz_buffer::from_vec)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_evaluate_cells(
    wb: fz_workbook_h,
    targets_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    catch_ffi(status, fz_buffer::empty(), || {
        if wb.0.is_null() || targets_payload.is_null() || len == 0 {
            return Err("invalid arguments".to_string());
        }

        let targets: Vec<CffiCellTarget> = decode_payload(targets_payload, len, format)?;
        for target in &targets {
            validate_cell(target.row, target.col)?;
        }

        let sheets: BTreeSet<&str> = targets.iter().map(|t| t.sheet.as_str()).collect();

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let mut wb_lock = opaque.0.write().map_err(|_| POISONED.to_string())?;

        // Prefer the targeted graph build; fall back to a full build when the
        // targeted path fails (e.g. an unknown sheet name). Report the
        // *fallback* error when both fail -- the previous code reported the
        // first error while silently discarding the second.
        if let Err(targeted_err) = wb_lock.prepare_graph_for_sheets(sheets.iter().copied())
            && let Err(full_err) = wb_lock.prepare_graph_all()
        {
            return Err(format!(
                "failed to prepare dependency graph: {targeted_err}; \
                 full rebuild also failed: {full_err}"
            ));
        }

        let target_refs: Vec<(&str, u32, u32)> = targets
            .iter()
            .map(|t| (t.sheet.as_str(), t.row, t.col))
            .collect();

        let values = wb_lock
            .evaluate_cells(&target_refs)
            .map_err(|e| e.to_string())?;

        encode_payload(&values, format).map(fz_buffer::from_vec)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_sheet_names(
    wb: fz_workbook_h,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    catch_ffi(status, fz_buffer::empty(), || {
        if wb.0.is_null() {
            return Err("invalid arguments".to_string());
        }

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let names = {
            let wb_lock = opaque.0.read().map_err(|_| POISONED.to_string())?;
            wb_lock.sheet_names()
        };

        encode_payload(&names, format).map(fz_buffer::from_vec)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_has_sheet(
    wb: fz_workbook_h,
    name: *const c_char,
    status: *mut fz_status,
) -> c_int {
    catch_ffi(status, 0, || {
        if wb.0.is_null() || name.is_null() {
            return Err("invalid arguments".to_string());
        }

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };

        let wb_lock = opaque.0.read().map_err(|_| POISONED.to_string())?;
        Ok(if wb_lock.has_sheet(&name_str) { 1 } else { 0 })
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_sheet_dimensions(
    wb: fz_workbook_h,
    name: *const c_char,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    catch_ffi(status, fz_buffer::empty(), || {
        if wb.0.is_null() || name.is_null() {
            return Err("invalid arguments".to_string());
        }

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };

        let (rows, cols) = {
            let wb_lock = opaque.0.read().map_err(|_| POISONED.to_string())?;
            wb_lock
                .sheet_dimensions(&name_str)
                .ok_or_else(|| "sheet not found".to_string())?
        };

        encode_payload(&CffiSheetDimensions { rows, cols }, format).map(fz_buffer::from_vec)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_delete_sheet(
    wb: fz_workbook_h,
    name: *const c_char,
    status: *mut fz_status,
) {
    catch_ffi(status, (), || {
        if wb.0.is_null() || name.is_null() {
            return Err("invalid arguments".to_string());
        }

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };

        let mut wb_lock = opaque.0.write().map_err(|_| POISONED.to_string())?;
        wb_lock.delete_sheet(&name_str).map_err(|e| e.to_string())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_rename_sheet(
    wb: fz_workbook_h,
    old_name: *const c_char,
    new_name: *const c_char,
    status: *mut fz_status,
) {
    catch_ffi(status, (), || {
        if wb.0.is_null() || old_name.is_null() || new_name.is_null() {
            return Err("invalid arguments".to_string());
        }

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let old_str = unsafe { CStr::from_ptr(old_name).to_string_lossy() };
        let new_str = unsafe { CStr::from_ptr(new_name).to_string_lossy() };

        let mut wb_lock = opaque.0.write().map_err(|_| POISONED.to_string())?;
        wb_lock
            .rename_sheet(&old_str, &new_str)
            .map_err(|e| e.to_string())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_read_range(
    wb: fz_workbook_h,
    range_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    catch_ffi(status, fz_buffer::empty(), || {
        if wb.0.is_null() {
            return Err("invalid arguments".to_string());
        }

        let addr: RangeAddress = decode_payload(range_payload, len, format)?;
        // `RangeAddress` derives `Deserialize` over public fields, so a decoded
        // value has bypassed the checked constructor. Reject 0-based/inverted
        // ranges here instead of letting them underflow downstream.
        addr.validate().map_err(|e| e.to_string())?;

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let values = {
            let wb_lock = opaque.0.read().map_err(|_| POISONED.to_string())?;
            wb_lock.read_range(&addr)
        };

        encode_payload(&values, format).map(fz_buffer::from_vec)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_set_values(
    wb: fz_workbook_h,
    sheet: *const c_char,
    start_row: c_uint,
    start_col: c_uint,
    values_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) {
    catch_ffi(status, (), || {
        if wb.0.is_null() || sheet.is_null() || values_payload.is_null() {
            return Err("invalid arguments".to_string());
        }

        let values: Vec<Vec<LiteralValue>> = decode_payload(values_payload, len, format)?;
        let widest = values.iter().map(Vec::len).max().unwrap_or(0);
        validate_block(start_row, start_col, values.len(), widest)?;

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };

        let mut wb_lock = opaque.0.write().map_err(|_| POISONED.to_string())?;
        wb_lock
            .set_values(&sheet_str, start_row, start_col, &values)
            .map_err(|e| e.to_string())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_set_formulas(
    wb: fz_workbook_h,
    sheet: *const c_char,
    start_row: c_uint,
    start_col: c_uint,
    formulas_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) {
    catch_ffi(status, (), || {
        if wb.0.is_null() || sheet.is_null() || formulas_payload.is_null() {
            return Err("invalid arguments".to_string());
        }

        let formulas: Vec<Vec<String>> = decode_payload(formulas_payload, len, format)?;
        let widest = formulas.iter().map(Vec::len).max().unwrap_or(0);
        validate_block(start_row, start_col, formulas.len(), widest)?;

        let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };

        let mut wb_lock = opaque.0.write().map_err(|_| POISONED.to_string())?;
        wb_lock
            .set_formulas(&sheet_str, start_row, start_col, &formulas)
            .map_err(|e| e.to_string())
    })
}
