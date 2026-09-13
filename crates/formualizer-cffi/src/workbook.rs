#![allow(clippy::missing_safety_doc)]

use crate::guard::{catch_ffi, catch_unwind_silent};
use crate::{
    EXCEL_MAX_COLS, EXCEL_MAX_ROWS, fz_buffer, fz_encoding_format, fz_status, validate_cffi_range,
};

use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_workbook::{
    LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::ffi::{CStr, c_char, c_int, c_uint};
use std::ptr;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct OpaqueWorkbook(pub Arc<RwLock<Workbook>>);

impl OpaqueWorkbook {
    fn write(&self) -> Result<RwLockWriteGuard<'_, Workbook>, String> {
        self.0
            .write()
            .map_err(|_| "workbook lock poisoned".to_string())
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, Workbook>, String> {
        self.0
            .read()
            .map_err(|_| "workbook lock poisoned".to_string())
    }
}

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

/// Validate a 1-based cell or block before it reaches the workbook engine.
fn checked_excel_coordinates(
    start_row: u32,
    start_col: u32,
    block_dimensions: Option<(usize, usize)>,
) -> Result<(), String> {
    if !(1..=EXCEL_MAX_ROWS).contains(&start_row) {
        return Err(format!(
            "row {start_row} is outside Excel's valid range 1..={EXCEL_MAX_ROWS}"
        ));
    }
    if !(1..=EXCEL_MAX_COLS).contains(&start_col) {
        return Err(format!(
            "column {start_col} is outside Excel's valid range 1..={EXCEL_MAX_COLS}"
        ));
    }

    let Some((height, width)) = block_dimensions else {
        return Ok(());
    };
    if height == 0 || width == 0 {
        return Ok(());
    }

    let row_offset =
        u32::try_from(height - 1).map_err(|_| "cell block row extent exceeds u32".to_string())?;
    let col_offset =
        u32::try_from(width - 1).map_err(|_| "cell block column extent exceeds u32".to_string())?;
    let end_row = start_row
        .checked_add(row_offset)
        .ok_or_else(|| "cell block row extent overflows".to_string())?;
    let end_col = start_col
        .checked_add(col_offset)
        .ok_or_else(|| "cell block column extent overflows".to_string())?;

    if end_row > EXCEL_MAX_ROWS {
        return Err(format!(
            "cell block ends at row {end_row}, outside Excel's valid range 1..={EXCEL_MAX_ROWS}"
        ));
    }
    if end_col > EXCEL_MAX_COLS {
        return Err(format!(
            "cell block ends at column {end_col}, outside Excel's valid range 1..={EXCEL_MAX_COLS}"
        ));
    }
    Ok(())
}

fn actual_block_dimensions<T>(rows: &[Vec<T>]) -> (usize, usize) {
    let height = rows
        .iter()
        .rposition(|row| !row.is_empty())
        .map_or(0, |index| index + 1);
    let width = rows.iter().map(Vec::len).max().unwrap_or(0);
    (height, width)
}

fn formula_block_dimensions(rows: &[Vec<String>]) -> (usize, usize) {
    let width = rows.iter().map(Vec::len).max().unwrap_or(0);
    if width == 0 {
        (0, 0)
    } else {
        (rows.len(), width)
    }
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

fn opaque_ref(wb: fz_workbook_h) -> Result<&'static OpaqueWorkbook, String> {
    if wb.0.is_null() {
        return Err("invalid arguments".to_string());
    }
    // SAFETY: handle was produced by create/open and not yet freed.
    Ok(unsafe { &*(wb.0 as *mut OpaqueWorkbook) })
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
    catch_unwind_silent(|| {
        if !wb.0.is_null() {
            unsafe {
                let _ = Box::from_raw(wb.0 as *mut OpaqueWorkbook);
            }
        }
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_add_sheet(
    wb: fz_workbook_h,
    name: *const c_char,
    status: *mut fz_status,
) {
    catch_ffi(status, (), || {
        if name.is_null() {
            return Err("invalid arguments".to_string());
        }
        let opaque = opaque_ref(wb)?;
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
        opaque
            .write()?
            .add_sheet(&name_str)
            .map_err(|e| e.to_string())
    });
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
        if sheet.is_null() || value_payload.is_null() {
            return Err("invalid arguments".to_string());
        }
        checked_excel_coordinates(row, col, None)?;
        let opaque = opaque_ref(wb)?;
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };
        let payload = unsafe { std::slice::from_raw_parts(value_payload, len) };
        let value: LiteralValue = match format {
            fz_encoding_format::FZ_ENCODING_JSON => {
                serde_json::from_slice(payload).map_err(|e| e.to_string())?
            }
            fz_encoding_format::FZ_ENCODING_CBOR => {
                ciborium::from_reader(payload).map_err(|e| e.to_string())?
            }
        };
        opaque
            .write()?
            .set_value(&sheet_str, row, col, value)
            .map_err(|e| e.to_string())
    });
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
        if sheet.is_null() || formula.is_null() {
            return Err("invalid arguments".to_string());
        }
        checked_excel_coordinates(row, col, None)?;
        let opaque = opaque_ref(wb)?;
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };
        let formula_str = unsafe { CStr::from_ptr(formula).to_string_lossy() };
        opaque
            .write()?
            .set_formula(&sheet_str, row, col, &formula_str)
            .map_err(|e| e.to_string())
    });
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
        if sheet.is_null() {
            return Err("invalid arguments".to_string());
        }
        checked_excel_coordinates(row, col, None)?;
        let opaque = opaque_ref(wb)?;
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };
        let formula = opaque.read()?.get_formula(&sheet_str, row, col);
        Ok(match formula {
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
        if sheet.is_null() {
            return Err("invalid arguments".to_string());
        }
        checked_excel_coordinates(row, col, None)?;
        let opaque = opaque_ref(wb)?;
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };
        let value = opaque
            .read()?
            .get_value(&sheet_str, row, col)
            .unwrap_or(LiteralValue::Empty);
        let bytes = encode_payload(&value, format)?;
        Ok(fz_buffer::from_vec(bytes))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_evaluate_all(
    wb: fz_workbook_h,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    catch_ffi(status, fz_buffer::empty(), || {
        let opaque = opaque_ref(wb)?;
        let mut wb_lock = opaque.write()?;
        wb_lock.prepare_graph_all().map_err(|e| e.to_string())?;
        let res = wb_lock.evaluate_all().map_err(|e| e.to_string())?;
        let cffi_res = CffiEvalResult {
            computed_vertices: res.computed_vertices,
            cycle_errors: res.cycle_errors,
            elapsed_ms: res.elapsed.as_millis() as u64,
        };
        Ok(fz_buffer::from_vec(encode_payload(&cffi_res, format)?))
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
        if targets_payload.is_null() || len == 0 {
            return Err("invalid arguments".to_string());
        }
        let targets: Vec<CffiCellTarget> = decode_payload(targets_payload, len, format)?;
        for target in &targets {
            checked_excel_coordinates(target.row, target.col, None)?;
        }
        let mut sheets: BTreeSet<&str> = BTreeSet::new();
        for target in &targets {
            sheets.insert(target.sheet.as_str());
        }
        let opaque = opaque_ref(wb)?;
        let mut wb_lock = opaque.write()?;
        if let Err(targeted_error) = wb_lock.prepare_graph_for_sheets(sheets.iter().copied())
            && let Err(full_error) = wb_lock.prepare_graph_all()
        {
            return Err(format!(
                "targeted graph preparation failed: {targeted_error}; \
full graph preparation fallback failed: {full_error}"
            ));
        }
        let target_refs: Vec<(&str, u32, u32)> = targets
            .iter()
            .map(|t| (t.sheet.as_str(), t.row, t.col))
            .collect();
        let values = wb_lock
            .evaluate_cells(&target_refs)
            .map_err(|e| e.to_string())?;
        Ok(fz_buffer::from_vec(encode_payload(&values, format)?))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_sheet_names(
    wb: fz_workbook_h,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    catch_ffi(status, fz_buffer::empty(), || {
        let opaque = opaque_ref(wb)?;
        let names = opaque.read()?.sheet_names();
        Ok(fz_buffer::from_vec(encode_payload(&names, format)?))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_has_sheet(
    wb: fz_workbook_h,
    name: *const c_char,
    status: *mut fz_status,
) -> c_int {
    catch_ffi(status, 0, || {
        if name.is_null() {
            return Err("invalid arguments".to_string());
        }
        let opaque = opaque_ref(wb)?;
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
        Ok(if opaque.read()?.has_sheet(&name_str) {
            1
        } else {
            0
        })
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
        if name.is_null() {
            return Err("invalid arguments".to_string());
        }
        let opaque = opaque_ref(wb)?;
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let (rows, cols) = opaque
            .read()?
            .sheet_dimensions(&name_str)
            .ok_or_else(|| "sheet not found".to_string())?;
        let dims = CffiSheetDimensions { rows, cols };
        Ok(fz_buffer::from_vec(encode_payload(&dims, format)?))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_delete_sheet(
    wb: fz_workbook_h,
    name: *const c_char,
    status: *mut fz_status,
) {
    catch_ffi(status, (), || {
        if name.is_null() {
            return Err("invalid arguments".to_string());
        }
        let opaque = opaque_ref(wb)?;
        let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
        opaque
            .write()?
            .delete_sheet(&name_str)
            .map_err(|e| e.to_string())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_workbook_rename_sheet(
    wb: fz_workbook_h,
    old_name: *const c_char,
    new_name: *const c_char,
    status: *mut fz_status,
) {
    catch_ffi(status, (), || {
        if old_name.is_null() || new_name.is_null() {
            return Err("invalid arguments".to_string());
        }
        let opaque = opaque_ref(wb)?;
        let old_str = unsafe { CStr::from_ptr(old_name).to_string_lossy() };
        let new_str = unsafe { CStr::from_ptr(new_name).to_string_lossy() };
        opaque
            .write()?
            .rename_sheet(&old_str, &new_str)
            .map_err(|e| e.to_string())
    });
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
        let opaque = opaque_ref(wb)?;
        let addr: RangeAddress = decode_payload(range_payload, len, format)?;
        validate_cffi_range(&addr)?;
        let values = opaque.read()?.read_range(&addr);
        Ok(fz_buffer::from_vec(encode_payload(&values, format)?))
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
        if sheet.is_null() || values_payload.is_null() {
            return Err("invalid arguments".to_string());
        }
        let values: Vec<Vec<LiteralValue>> = decode_payload(values_payload, len, format)?;
        checked_excel_coordinates(start_row, start_col, Some(actual_block_dimensions(&values)))?;
        let opaque = opaque_ref(wb)?;
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };
        opaque
            .write()?
            .set_values(&sheet_str, start_row, start_col, &values)
            .map_err(|e| e.to_string())
    });
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
        if sheet.is_null() || formulas_payload.is_null() {
            return Err("invalid arguments".to_string());
        }
        let formulas: Vec<Vec<String>> = decode_payload(formulas_payload, len, format)?;
        checked_excel_coordinates(
            start_row,
            start_col,
            Some(formula_block_dimensions(&formulas)),
        )?;
        let opaque = opaque_ref(wb)?;
        let sheet_str = unsafe { CStr::from_ptr(sheet).to_string_lossy() };
        opaque
            .write()?
            .set_formulas(&sheet_str, start_row, start_col, &formulas)
            .map_err(|e| e.to_string())
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_excel_coordinates_cover_limits_and_extents() {
        assert!(checked_excel_coordinates(0, 1, None).is_err());
        assert!(checked_excel_coordinates(EXCEL_MAX_ROWS, EXCEL_MAX_COLS, None).is_ok());
        assert!(checked_excel_coordinates(EXCEL_MAX_ROWS + 1, 1, None).is_err());
        assert!(checked_excel_coordinates(1, EXCEL_MAX_COLS + 1, None).is_err());
        assert!(checked_excel_coordinates(EXCEL_MAX_ROWS, 1, Some((2, 1))).is_err());
        assert!(checked_excel_coordinates(1, EXCEL_MAX_COLS, Some((1, 2))).is_err());
        assert!(checked_excel_coordinates(1, 1, Some((usize::MAX, 1))).is_err());
        assert!(checked_excel_coordinates(1, 1, Some((1, usize::MAX))).is_err());
    }
}
