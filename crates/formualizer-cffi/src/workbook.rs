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
pub extern "C" fn fz_workbook_create(status: *mut fz_status) -> fz_workbook_h {
    let wb = Workbook::new();
    let opaque = Box::new(OpaqueWorkbook(Arc::new(RwLock::new(wb))));
    if !status.is_null() {
        unsafe {
            *status = fz_status::ok();
        }
    }
    fz_workbook_h(Box::into_raw(opaque) as *mut std::ffi::c_void)
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_open_xlsx(
    path: *const c_char,
    status: *mut fz_status,
) -> fz_workbook_h {
    if path.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return fz_workbook_h(ptr::null_mut());
    }

    let path_str = unsafe { CStr::from_ptr(path).to_string_lossy() };
    let backend = match UmyaAdapter::open_path(path_str.as_ref()) {
        Ok(adapter) => adapter,
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e.to_string());
                }
            }
            return fz_workbook_h(ptr::null_mut());
        }
    };

    let cfg = WorkbookConfig::interactive();
    let wb = match Workbook::from_reader(backend, LoadStrategy::EagerAll, cfg) {
        Ok(wb) => wb,
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e.to_string());
                }
            }
            return fz_workbook_h(ptr::null_mut());
        }
    };

    let opaque = Box::new(OpaqueWorkbook(Arc::new(RwLock::new(wb))));
    if !status.is_null() {
        unsafe {
            *status = fz_status::ok();
        }
    }
    fz_workbook_h(Box::into_raw(opaque) as *mut std::ffi::c_void)
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_free(wb: fz_workbook_h) {
    if !wb.0.is_null() {
        unsafe {
            let _ = Box::from_raw(wb.0 as *mut OpaqueWorkbook);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_add_sheet(
    wb: fz_workbook_h,
    name: *const c_char,
    status: *mut fz_status,
) {
    if wb.0.is_null() || name.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return;
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };

    let mut wb_lock = opaque.0.write().unwrap();
    if let Err(e) = wb_lock.add_sheet(&name_str) {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error(e.to_string());
            }
        }
    } else if !status.is_null() {
        unsafe {
            *status = fz_status::ok();
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_set_cell_value(
    wb: fz_workbook_h,
    sheet: *const c_char,
    row: c_uint,
    col: c_uint,
    value_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) {
    if wb.0.is_null() || sheet.is_null() || value_payload.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return;
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };
    let payload = unsafe { std::slice::from_raw_parts(value_payload, len) };

    let value: Result<LiteralValue, String> = match format {
        fz_encoding_format::FZ_ENCODING_JSON => {
            serde_json::from_slice(payload).map_err(|e| e.to_string())
        }
        fz_encoding_format::FZ_ENCODING_CBOR => {
            ciborium::from_reader(payload).map_err(|e| e.to_string())
        }
    };

    let value = match value {
        Ok(v) => v,
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e);
                }
            }
            return;
        }
    };

    let mut wb_lock = opaque.0.write().unwrap();
    if let Err(e) = wb_lock.set_value(&sheet_str, row, col, value) {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error(e.to_string());
            }
        }
    } else if !status.is_null() {
        unsafe {
            *status = fz_status::ok();
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_set_cell_formula(
    wb: fz_workbook_h,
    sheet: *const c_char,
    row: c_uint,
    col: c_uint,
    formula: *const c_char,
    status: *mut fz_status,
) {
    if wb.0.is_null() || sheet.is_null() || formula.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return;
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };
    let formula_str = unsafe { CStr::from_ptr(formula).to_string_lossy() };

    let mut wb_lock = opaque.0.write().unwrap();
    if let Err(e) = wb_lock.set_formula(&sheet_str, row, col, &formula_str) {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error(e.to_string());
            }
        }
    } else if !status.is_null() {
        unsafe {
            *status = fz_status::ok();
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_get_cell_formula(
    wb: fz_workbook_h,
    sheet: *const c_char,
    row: c_uint,
    col: c_uint,
    status: *mut fz_status,
) -> fz_buffer {
    if wb.0.is_null() || sheet.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };

    let wb_lock = opaque.0.read().unwrap();
    let formula = wb_lock.get_formula(&sheet_str, row, col);

    match formula {
        Some(f) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::ok();
                }
            }
            fz_buffer::from_vec(f.into_bytes())
        }
        None => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::ok();
                }
            }
            fz_buffer::empty()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_get_cell_value(
    wb: fz_workbook_h,
    sheet: *const c_char,
    row: c_uint,
    col: c_uint,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    if wb.0.is_null() || sheet.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };

    let wb_lock = opaque.0.read().unwrap();
    let value = wb_lock
        .get_value(&sheet_str, row, col)
        .unwrap_or(LiteralValue::Empty);

    let result: Result<Vec<u8>, String> = match format {
        fz_encoding_format::FZ_ENCODING_JSON => {
            serde_json::to_vec(&value).map_err(|e| e.to_string())
        }
        fz_encoding_format::FZ_ENCODING_CBOR => {
            let mut buf = Vec::new();
            ciborium::into_writer(&value, &mut buf)
                .map_err(|e| e.to_string())
                .map(|_| buf)
        }
    };

    match result {
        Ok(v) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::ok();
                }
            }
            fz_buffer::from_vec(v)
        }
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e);
                }
            }
            fz_buffer::empty()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_evaluate_all(
    wb: fz_workbook_h,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    if wb.0.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let mut wb_lock = opaque.0.write().unwrap();

    // Workbook needs to build graph if deferred
    if let Err(e) = wb_lock.prepare_graph_all() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error(e.to_string());
            }
        }
        return fz_buffer::empty();
    }

    match wb_lock.evaluate_all() {
        Ok(res) => {
            let cffi_res = CffiEvalResult {
                computed_vertices: res.computed_vertices,
                cycle_errors: res.cycle_errors,
                elapsed_ms: res.elapsed.as_millis() as u64,
            };

            let result: Result<Vec<u8>, String> = match format {
                fz_encoding_format::FZ_ENCODING_JSON => {
                    serde_json::to_vec(&cffi_res).map_err(|e| e.to_string())
                }
                fz_encoding_format::FZ_ENCODING_CBOR => {
                    let mut buf = Vec::new();
                    ciborium::into_writer(&cffi_res, &mut buf)
                        .map_err(|e| e.to_string())
                        .map(|_| buf)
                }
            };

            match result {
                Ok(v) => {
                    if !status.is_null() {
                        unsafe {
                            *status = fz_status::ok();
                        }
                    }
                    fz_buffer::from_vec(v)
                }
                Err(e) => {
                    if !status.is_null() {
                        unsafe {
                            *status = fz_status::error(e);
                        }
                    }
                    fz_buffer::empty()
                }
            }
        }
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e.to_string());
                }
            }
            fz_buffer::empty()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_evaluate_cells(
    wb: fz_workbook_h,
    targets_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    if wb.0.is_null() || targets_payload.is_null() || len == 0 {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let targets: Vec<CffiCellTarget> = match decode_payload(targets_payload, len, format) {
        Ok(targets) => targets,
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e);
                }
            }
            return fz_buffer::empty();
        }
    };

    let mut sheets: BTreeSet<&str> = BTreeSet::new();
    for target in &targets {
        sheets.insert(target.sheet.as_str());
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let mut wb_lock = opaque.0.write().unwrap();

    if let Err(e) = wb_lock.prepare_graph_for_sheets(sheets.iter().copied()) {
        if wb_lock.prepare_graph_all().is_err() {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e.to_string());
                }
            }
            return fz_buffer::empty();
        }
    }

    let target_refs: Vec<(&str, u32, u32)> = targets
        .iter()
        .map(|t| (t.sheet.as_str(), t.row, t.col))
        .collect();

    let values = match wb_lock.evaluate_cells(&target_refs) {
        Ok(values) => values,
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e.to_string());
                }
            }
            return fz_buffer::empty();
        }
    };

    match encode_payload(&values, format) {
        Ok(buf) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::ok();
                }
            }
            fz_buffer::from_vec(buf)
        }
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e);
                }
            }
            fz_buffer::empty()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_sheet_names(
    wb: fz_workbook_h,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    if wb.0.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let wb_lock = opaque.0.read().unwrap();
    let names = wb_lock.sheet_names();

    match encode_payload(&names, format) {
        Ok(v) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::ok();
                }
            }
            fz_buffer::from_vec(v)
        }
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e);
                }
            }
            fz_buffer::empty()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_has_sheet(
    wb: fz_workbook_h,
    name: *const c_char,
    status: *mut fz_status,
) -> c_int {
    if wb.0.is_null() || name.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return 0;
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
    let wb_lock = opaque.0.read().unwrap();
    let has = wb_lock.has_sheet(&name_str);

    if !status.is_null() {
        unsafe {
            *status = fz_status::ok();
        }
    }
    if has { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_sheet_dimensions(
    wb: fz_workbook_h,
    name: *const c_char,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    if wb.0.is_null() || name.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
    let wb_lock = opaque.0.read().unwrap();

    let Some((rows, cols)) = wb_lock.sheet_dimensions(&name_str) else {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("sheet not found".to_string());
            }
        }
        return fz_buffer::empty();
    };

    let dims = CffiSheetDimensions { rows, cols };
    match encode_payload(&dims, format) {
        Ok(v) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::ok();
                }
            }
            fz_buffer::from_vec(v)
        }
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e);
                }
            }
            fz_buffer::empty()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_delete_sheet(
    wb: fz_workbook_h,
    name: *const c_char,
    status: *mut fz_status,
) {
    if wb.0.is_null() || name.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return;
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };

    let mut wb_lock = opaque.0.write().unwrap();
    if let Err(e) = wb_lock.delete_sheet(&name_str) {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error(e.to_string());
            }
        }
    } else if !status.is_null() {
        unsafe {
            *status = fz_status::ok();
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_rename_sheet(
    wb: fz_workbook_h,
    old_name: *const c_char,
    new_name: *const c_char,
    status: *mut fz_status,
) {
    if wb.0.is_null() || old_name.is_null() || new_name.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return;
    }

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let old_str = unsafe { CStr::from_ptr(old_name).to_string_lossy() };
    let new_str = unsafe { CStr::from_ptr(new_name).to_string_lossy() };

    let mut wb_lock = opaque.0.write().unwrap();
    if let Err(e) = wb_lock.rename_sheet(&old_str, &new_str) {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error(e.to_string());
            }
        }
    } else if !status.is_null() {
        unsafe {
            *status = fz_status::ok();
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_read_range(
    wb: fz_workbook_h,
    range_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    if wb.0.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let addr: RangeAddress = match decode_payload(range_payload, len, format) {
        Ok(v) => v,
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e);
                }
            }
            return fz_buffer::empty();
        }
    };

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let wb_lock = opaque.0.read().unwrap();
    let values = wb_lock.read_range(&addr);

    match encode_payload(&values, format) {
        Ok(v) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::ok();
                }
            }
            fz_buffer::from_vec(v)
        }
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e);
                }
            }
            fz_buffer::empty()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_set_values(
    wb: fz_workbook_h,
    sheet: *const c_char,
    start_row: c_uint,
    start_col: c_uint,
    values_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) {
    if wb.0.is_null() || sheet.is_null() || values_payload.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return;
    }

    let values: Vec<Vec<LiteralValue>> = match decode_payload(values_payload, len, format) {
        Ok(v) => v,
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e);
                }
            }
            return;
        }
    };

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };

    let mut wb_lock = opaque.0.write().unwrap();
    if let Err(e) = wb_lock.set_values(&sheet_str, start_row, start_col, &values) {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error(e.to_string());
            }
        }
    } else if !status.is_null() {
        unsafe {
            *status = fz_status::ok();
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_set_formulas(
    wb: fz_workbook_h,
    sheet: *const c_char,
    start_row: c_uint,
    start_col: c_uint,
    formulas_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) {
    if wb.0.is_null() || sheet.is_null() || formulas_payload.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("invalid arguments".to_string());
            }
        }
        return;
    }

    let formulas: Vec<Vec<String>> = match decode_payload(formulas_payload, len, format) {
        Ok(v) => v,
        Err(e) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::error(e);
                }
            }
            return;
        }
    };

    let opaque = unsafe { &*(wb.0 as *mut OpaqueWorkbook) };
    let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };

    let mut wb_lock = opaque.0.write().unwrap();
    if let Err(e) = wb_lock.set_formulas(&sheet_str, start_row, start_col, &formulas) {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error(e.to_string());
            }
        }
    } else if !status.is_null() {
        unsafe {
            *status = fz_status::ok();
        }
    }
}
