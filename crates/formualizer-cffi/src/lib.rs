#![allow(clippy::missing_safety_doc)]

use std::ffi::{c_char, c_int};

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

impl fz_status {
    pub fn ok() -> Self {
        fz_status {
            code: fz_status_code::FZ_STATUS_OK,
            error: fz_buffer::empty(),
        }
    }

    pub fn error(msg: String) -> Self {
        let error_json = format!("{{\"message\": {:?}}}", msg);
        fz_status {
            code: fz_status_code::FZ_STATUS_ERROR,
            error: fz_buffer::from_vec(error_json.into_bytes()),
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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn fz_parse_tokenize(
    formula: *const c_char,
    options: fz_parse_options,
    format: fz_encoding_format,
    status: *mut fz_status,
) -> fz_buffer {
    use crate::parse::CffiToken;
    use formualizer_parse::tokenizer::Tokenizer;
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
    use formualizer_parse::parser::Parser;
    use formualizer_parse::tokenizer::Tokenizer;
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

    let result: Result<Vec<u8>, String> = (|| {
        let dialect = FormulaDialect::from(options.dialect);
        let tokens = Tokenizer::new_with_dialect(&input, dialect)
            .map_err(|e| e.to_string())?
            .items;
        let mut parser = Parser::new_with_dialect(tokens, true, dialect);
        let ast = parser.parse().map_err(|e| e.to_string())?;

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
    use formualizer_parse::{pretty_parse_render, FormulaDialect};
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

    let _ = FormulaDialect::from(dialect);
    let result: Result<String, String> = pretty_parse_render(&input).map_err(|e| e.to_string());

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
    use formualizer_common::{coord, RangeAddress};
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
    use formualizer_common::{coord, RangeAddress};

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
