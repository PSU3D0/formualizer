#![allow(clippy::missing_safety_doc)]

use std::ffi::{c_char, c_int};

use serde::Serialize;
use std::ptr;
use std::slice;

pub mod parse;
pub mod workbook;

pub use workbook::*;

/// A buffer owned by Rust, to be freed by `fz_buffer_free`.
#[repr(C)]
pub struct fz_buffer {
    pub data: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl fz_buffer {
    pub fn from_vec(mut v: Vec<u8>) -> Self {
        let b = fz_buffer {
            data: v.as_mut_ptr(),
            len: v.len(),
            cap: v.capacity(),
        };
        std::mem::forget(v);
        b
    }

    pub fn empty() -> Self {
        fz_buffer {
            data: ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }
}

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(PartialEq, Debug)]
pub enum fz_status_code {
    FZ_STATUS_OK = 0,
    FZ_STATUS_ERROR = 1,
}

/// Status reporting for FFI calls.
#[repr(C)]
pub struct fz_status {
    pub code: fz_status_code,
    pub error: fz_buffer, // JSON encoded error if code != OK
}

#[derive(Serialize)]
struct StatusError<'a> {
    message: &'a str,
}

impl fz_status {
    pub fn ok() -> Self {
        fz_status {
            code: fz_status_code::FZ_STATUS_OK,
            error: fz_buffer::empty(),
        }
    }

    pub fn error(msg: String) -> Self {
        let error_json = match serde_json::to_vec(&StatusError { message: &msg }) {
            Ok(json) => json,
            // String serialization cannot fail, but keep the C boundary total if the
            // serializer gains a fallible path in the future.
            Err(_) => br#"{"message":"failed to serialize error"}"#.to_vec(),
        };
        fz_status {
            code: fz_status_code::FZ_STATUS_ERROR,
            error: fz_buffer::from_vec(error_json),
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_buffer_free(buffer: fz_buffer) {
    if !buffer.data.is_null() {
        unsafe {
            let _ = Vec::from_raw_parts(buffer.data, buffer.len, buffer.cap);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_common_abi_version() -> c_int {
    1
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_parse_abi_version() -> c_int {
    1
}

#[unsafe(no_mangle)]
pub extern "C" fn fz_workbook_abi_version() -> c_int {
    1
}

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub enum fz_encoding_format {
    FZ_ENCODING_JSON = 0,
    FZ_ENCODING_CBOR = 1,
}

pub(crate) const EXCEL_MAX_ROWS: u32 = 1_048_576;
pub(crate) const EXCEL_MAX_COLS: u32 = 16_384;

/// Validate a range after it crosses the CFFI serialization boundary.
pub(crate) fn validate_cffi_range(addr: &formualizer_common::RangeAddress) -> Result<(), String> {
    if addr.start_row == 0 || addr.start_col == 0 || addr.end_row == 0 || addr.end_col == 0 {
        return Err("range bounds must be 1-based".to_string());
    }
    if addr.start_row > addr.end_row || addr.start_col > addr.end_col {
        return Err("range bounds must be ordered: start <= end".to_string());
    }
    if addr.end_row > EXCEL_MAX_ROWS {
        return Err(format!(
            "range ends at row {}, outside Excel's valid range 1..={EXCEL_MAX_ROWS}",
            addr.end_row
        ));
    }
    if addr.end_col > EXCEL_MAX_COLS {
        return Err(format!(
            "range ends at column {}, outside Excel's valid range 1..={EXCEL_MAX_COLS}",
            addr.end_col
        ));
    }
    Ok(())
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct fz_parse_options {
    pub include_spans: bool,
    pub dialect: fz_formula_dialect,
}

#[allow(non_camel_case_types)]
#[repr(C)]
#[derive(Copy, Clone)]
pub enum fz_formula_dialect {
    FZ_DIALECT_EXCEL = 0,
    FZ_DIALECT_OPENFORMULA = 1,
}

impl From<fz_formula_dialect> for formualizer_parse::FormulaDialect {
    fn from(d: fz_formula_dialect) -> Self {
        match d {
            fz_formula_dialect::FZ_DIALECT_EXCEL => formualizer_parse::FormulaDialect::Excel,
            fz_formula_dialect::FZ_DIALECT_OPENFORMULA => {
                formualizer_parse::FormulaDialect::OpenFormula
            }
        }
    }
}

/// Render a formula with the selected dialect while preserving the CFFI
/// canonical-rendering contract.
fn cffi_pretty_parse_render(
    formula: &str,
    dialect: formualizer_parse::FormulaDialect,
) -> Result<String, String> {
    if formula.is_empty() {
        return Ok(String::new());
    }

    let needs_equals = !formula.starts_with('=');
    let formula_to_parse = if needs_equals {
        format!("={formula}")
    } else {
        formula.to_string()
    };

    let ast = formualizer_parse::parse_with_dialect(&formula_to_parse, dialect)
        .map_err(|e| e.to_string())?;
    let pretty_printed = formualizer_parse::pretty_print(&ast);

    if needs_equals {
        Ok(pretty_printed)
    } else {
        Ok(format!("={pretty_printed}"))
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_parse_tokenize(
    formula: *const c_char,
    options: fz_parse_options,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    use crate::parse::CffiToken;
    use formualizer_parse::FormulaDialect;
    use formualizer_parse::tokenizer::Tokenizer;
    use std::ffi::CStr;

    if formula.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("formula is null".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let input = unsafe { CStr::from_ptr(formula).to_string_lossy() };

    let result: Result<Vec<u8>, String> = (|| {
        let dialect = FormulaDialect::from(options.dialect);
        let tokens = Tokenizer::new_with_dialect(&input, dialect)
            .map_err(|e| e.to_string())?
            .items;

        let cffi_tokens: Vec<CffiToken> = tokens
            .iter()
            .map(|t| CffiToken::from_core(t, options.include_spans))
            .collect();

        match format {
            fz_encoding_format::FZ_ENCODING_JSON => {
                serde_json::to_vec(&cffi_tokens).map_err(|e| e.to_string())
            }
            fz_encoding_format::FZ_ENCODING_CBOR => {
                let mut buf = Vec::new();
                ciborium::into_writer(&cffi_tokens, &mut buf).map_err(|e| e.to_string())?;
                Ok(buf)
            }
        }
    })();

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
pub unsafe extern "C" fn fz_parse_ast(
    formula: *const c_char,
    options: fz_parse_options,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    use crate::parse::CffiASTNode;
    use formualizer_parse::FormulaDialect;
    use formualizer_parse::parser::parse_with_dialect;
    use std::ffi::CStr;

    if formula.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("formula is null".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let input = unsafe { CStr::from_ptr(formula).to_string_lossy() };

    let result: Result<Vec<u8>, String> = (|| {
        let dialect = FormulaDialect::from(options.dialect);
        let ast = parse_with_dialect(&input, dialect).map_err(|e| e.to_string())?;

        let cffi_ast = CffiASTNode::from_core(&ast, options.include_spans);

        match format {
            fz_encoding_format::FZ_ENCODING_JSON => {
                serde_json::to_vec(&cffi_ast).map_err(|e| e.to_string())
            }
            fz_encoding_format::FZ_ENCODING_CBOR => {
                let mut buf = Vec::new();
                ciborium::into_writer(&cffi_ast, &mut buf).map_err(|e| e.to_string())?;
                Ok(buf)
            }
        }
    })();

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
pub unsafe extern "C" fn fz_parse_canonical_formula(
    formula: *const c_char,
    dialect: fz_formula_dialect,
    status: *mut fz_status,
) -> fz_buffer {
    use formualizer_parse::FormulaDialect;
    use std::ffi::CStr;

    if formula.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("formula is null".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let input = unsafe { CStr::from_ptr(formula).to_string_lossy() };

    let result = cffi_pretty_parse_render(&input, FormulaDialect::from(dialect));

    match result {
        Ok(v) => {
            if !status.is_null() {
                unsafe {
                    *status = fz_status::ok();
                }
            }
            fz_buffer::from_vec(v.into_bytes())
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
pub unsafe extern "C" fn fz_common_parse_range_a1(
    range_a1: *const c_char,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    use formualizer_common::{RangeAddress, coord};
    use std::ffi::CStr;

    if range_a1.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("range_a1 is null".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let input = unsafe { CStr::from_ptr(range_a1).to_string_lossy() };

    let result: Result<Vec<u8>, String> = (|| {
        // Simple A1 range parser: [Sheet!]A1[:B2]
        let (sheet, rest) = if let Some(pos) = input.find('!') {
            (&input[..pos], &input[pos + 1..])
        } else {
            ("", input.as_ref())
        };

        let (start_str, end_str) = if let Some(pos) = rest.find(':') {
            (&rest[..pos], &rest[pos + 1..])
        } else {
            (rest, rest)
        };

        let (sr, sc, _, _) = coord::parse_a1_1based(start_str).map_err(|e| e.to_string())?;
        let (er, ec, _, _) = coord::parse_a1_1based(end_str).map_err(|e| e.to_string())?;

        let addr = RangeAddress::new(sheet, sr, sc, er, ec).map_err(|e| e.to_string())?;
        crate::validate_cffi_range(&addr)?;

        match format {
            fz_encoding_format::FZ_ENCODING_JSON => {
                serde_json::to_vec(&addr).map_err(|e| e.to_string())
            }
            fz_encoding_format::FZ_ENCODING_CBOR => {
                let mut buf = Vec::new();
                ciborium::into_writer(&addr, &mut buf).map_err(|e| e.to_string())?;
                Ok(buf)
            }
        }
    })();

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
pub unsafe extern "C" fn fz_common_format_range_a1(
    range_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    use formualizer_common::{RangeAddress, coord};

    if range_payload.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("range_payload is null".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let result: Result<Vec<u8>, String> = (|| {
        let payload = unsafe { slice::from_raw_parts(range_payload, len) };
        let addr: RangeAddress = match format {
            fz_encoding_format::FZ_ENCODING_JSON => {
                serde_json::from_slice(payload).map_err(|e| e.to_string())?
            }
            fz_encoding_format::FZ_ENCODING_CBOR => {
                ciborium::from_reader(payload).map_err(|e| e.to_string())?
            }
        };
        crate::validate_cffi_range(&addr)?;

        let start_col =
            coord::col_letters_from_1based(addr.start_col).map_err(|e| e.to_string())?;
        let end_col = coord::col_letters_from_1based(addr.end_col).map_err(|e| e.to_string())?;

        let mut out = String::new();
        if !addr.sheet.is_empty() {
            out.push_str(&addr.sheet);
            out.push('!');
        }
        out.push_str(&start_col);
        out.push_str(&addr.start_row.to_string());
        if addr.start_row != addr.end_row || addr.start_col != addr.end_col {
            out.push(':');
            out.push_str(&end_col);
            out.push_str(&addr.end_row.to_string());
        }

        Ok(out.into_bytes())
    })();

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
pub unsafe extern "C" fn fz_common_normalize_literal_value(
    value_payload: *const u8,
    len: usize,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    use formualizer_common::LiteralValue;

    if value_payload.is_null() {
        if !status.is_null() {
            unsafe {
                *status = fz_status::error("value_payload is null".to_string());
            }
        }
        return fz_buffer::empty();
    }

    let result: Result<Vec<u8>, String> = (|| {
        let payload = unsafe { slice::from_raw_parts(value_payload, len) };
        let value: LiteralValue = match format {
            fz_encoding_format::FZ_ENCODING_JSON => {
                serde_json::from_slice(payload).map_err(|e| e.to_string())?
            }
            fz_encoding_format::FZ_ENCODING_CBOR => {
                ciborium::from_reader(payload).map_err(|e| e.to_string())?
            }
        };

        // Normalization roundtrip validates schema.

        match format {
            fz_encoding_format::FZ_ENCODING_JSON => {
                serde_json::to_vec(&value).map_err(|e| e.to_string())
            }
            fz_encoding_format::FZ_ENCODING_CBOR => {
                let mut buf = Vec::new();
                ciborium::into_writer(&value, &mut buf).map_err(|e| e.to_string())?;
                Ok(buf)
            }
        }
    })();

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
