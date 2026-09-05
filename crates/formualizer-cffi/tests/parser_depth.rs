use formualizer_cffi::*;
use std::ffi::CString;

const DEPTH_ERROR: &str = "Formula nesting too deep (max 72)";

fn accepted_formulas() -> [(&'static str, String); 3] {
    [
        (
            "parentheses",
            format!("={}1{}", "(".repeat(64), ")".repeat(64)),
        ),
        ("sum", format!("={}1{}", "SUM(".repeat(64), ")".repeat(64))),
        (
            "if",
            format!("={}1{}", "IF(A1>0,".repeat(64), ",0)".repeat(64)),
        ),
    ]
}

fn hostile_formulas() -> [(&'static str, String); 7] {
    [
        (
            "parentheses",
            format!("={}1{}", "(".repeat(5000), ")".repeat(5000)),
        ),
        ("unary", format!("={}1", "-".repeat(5000))),
        (
            "sum",
            format!("={}1{}", "SUM(".repeat(5000), ")".repeat(5000)),
        ),
        (
            "right-infix",
            format!("={}1{}", "1+(".repeat(5000), ")".repeat(5000)),
        ),
        (
            "if",
            format!("={}1{}", "IF(A1>0,".repeat(5000), ",0)".repeat(5000)),
        ),
        (
            "arrays",
            format!("={}1{}", "{".repeat(5000), "}".repeat(5000)),
        ),
        ("power", format!("={}1", "1^".repeat(5000))),
    ]
}

fn buffer_as_bytes(buffer: &fz_buffer) -> Vec<u8> {
    if buffer.data.is_null() || buffer.len == 0 {
        return Vec::new();
    }
    unsafe { std::slice::from_raw_parts(buffer.data, buffer.len).to_vec() }
}

fn parse_options() -> fz_parse_options {
    fz_parse_options {
        include_spans: false,
        dialect: fz_formula_dialect::FZ_DIALECT_EXCEL,
    }
}

fn assert_ast_accepts(shape: &str, formula: &str) {
    let c_formula = CString::new(formula).unwrap();
    let mut status = fz_status::ok();
    let buffer = unsafe {
        fz_parse_ast(
            c_formula.as_ptr(),
            parse_options(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        )
    };
    let output = buffer_as_bytes(&buffer);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK, "{shape}");
    assert!(!output.is_empty(), "{shape} AST output should be nonempty");

    unsafe {
        fz_buffer_free(buffer);
        fz_buffer_free(status.error);
    }
}

fn assert_ast_rejects(shape: &str, formula: &str) {
    let c_formula = CString::new(formula).unwrap();
    let mut status = fz_status::ok();
    let buffer = unsafe {
        fz_parse_ast(
            c_formula.as_ptr(),
            parse_options(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        )
    };
    let output = buffer_as_bytes(&buffer);
    let error = String::from_utf8_lossy(&buffer_as_bytes(&status.error)).into_owned();
    let code = std::mem::replace(&mut status.code, fz_status_code::FZ_STATUS_OK);

    unsafe {
        fz_buffer_free(buffer);
        fz_buffer_free(status.error);
    }

    assert_eq!(code, fz_status_code::FZ_STATUS_ERROR, "{shape}");
    assert!(output.is_empty(), "{shape} AST output should be empty");
    assert!(error.contains(DEPTH_ERROR), "{shape} error: {error}");
}

fn assert_canonical_accepts(shape: &str, formula: &str) {
    let c_formula = CString::new(formula).unwrap();
    let mut status = fz_status::ok();
    let buffer = unsafe {
        fz_parse_canonical_formula(
            c_formula.as_ptr(),
            fz_formula_dialect::FZ_DIALECT_EXCEL,
            &mut status,
        )
    };
    let output = buffer_as_bytes(&buffer);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK, "{shape}");
    assert!(
        !output.is_empty(),
        "{shape} canonical output should be nonempty"
    );

    unsafe {
        fz_buffer_free(buffer);
        fz_buffer_free(status.error);
    }
}

fn assert_canonical_rejects(shape: &str, formula: &str) {
    let c_formula = CString::new(formula).unwrap();
    let mut status = fz_status::ok();
    let buffer = unsafe {
        fz_parse_canonical_formula(
            c_formula.as_ptr(),
            fz_formula_dialect::FZ_DIALECT_EXCEL,
            &mut status,
        )
    };
    let output = buffer_as_bytes(&buffer);
    let error = String::from_utf8_lossy(&buffer_as_bytes(&status.error)).into_owned();
    let code = std::mem::replace(&mut status.code, fz_status_code::FZ_STATUS_OK);

    unsafe {
        fz_buffer_free(buffer);
        fz_buffer_free(status.error);
    }

    assert_eq!(code, fz_status_code::FZ_STATUS_ERROR, "{shape}");
    assert!(
        output.is_empty(),
        "{shape} canonical output should be empty"
    );
    assert!(error.contains(DEPTH_ERROR), "{shape} error: {error}");
}

#[test]
fn test_parser_depth_through_c_abi_and_recovers_for_next_formula() {
    for (shape, formula) in accepted_formulas() {
        assert_ast_accepts(shape, &formula);
        assert_canonical_accepts(shape, &formula);
    }

    for (shape, formula) in hostile_formulas() {
        assert_ast_rejects(shape, &formula);
        assert_canonical_rejects(shape, &formula);
    }

    assert_ast_accepts("ordinary after rejection", "=A1+1");
    assert_canonical_accepts("ordinary after rejection", "=SUM(A1:A2)");
}
