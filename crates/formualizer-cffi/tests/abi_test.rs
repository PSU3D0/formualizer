use formualizer_cffi::*;
use formualizer_common::{LiteralValue, RangeAddress};
use serde::Deserialize;
use std::ffi::CString;
use std::io::Cursor;

fn buffer_as_bytes(buffer: &fz_buffer) -> Vec<u8> {
    if buffer.data.is_null() || buffer.len == 0 {
        return Vec::new();
    }
    unsafe { std::slice::from_raw_parts(buffer.data, buffer.len).to_vec() }
}

#[derive(Deserialize)]
struct SheetDimensions {
    rows: u32,
    cols: u32,
}

#[test]
fn test_abi_version() {
    assert_eq!(fz_common_abi_version(), 1);
    assert_eq!(fz_parse_abi_version(), 1);
    assert_eq!(fz_workbook_abi_version(), 1);
}

#[test]
fn test_range_roundtrip() {
    let range_str = "Sheet1!A1:B2";
    let c_range_str = CString::new(range_str).unwrap();
    let mut status = fz_status::ok();

    // Parse
    let buffer = fz_common_parse_range_a1(
        c_range_str.as_ptr(),
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    assert!(buffer.len > 0);

    // Format
    let mut status2 = fz_status::ok();
    let formatted_buffer = fz_common_format_range_a1(
        buffer.data,
        buffer.len,
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status2,
    );
    assert_eq!(status2.code, fz_status_code::FZ_STATUS_OK);

    let result_str = unsafe {
        let slice = std::slice::from_raw_parts(formatted_buffer.data, formatted_buffer.len);
        String::from_utf8_lossy(slice).to_string()
    };
    assert_eq!(result_str, range_str);

    fz_buffer_free(buffer);
    fz_buffer_free(formatted_buffer);
}

#[test]
fn test_invalid_range() {
    let range_str = "Invalid Range!";
    let c_range_str = CString::new(range_str).unwrap();
    let mut status = fz_status::ok();

    let buffer = fz_common_parse_range_a1(
        c_range_str.as_ptr(),
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
    assert_eq!(buffer.len, 0);

    fz_buffer_free(status.error);
}

#[test]
fn test_range_roundtrip_cbor() {
    let range_str = "Sheet1!A1:B2";
    let c_range_str = CString::new(range_str).unwrap();
    let mut status = fz_status::ok();

    // Parse
    let buffer = fz_common_parse_range_a1(
        c_range_str.as_ptr(),
        fz_encoding_format::FZ_ENCODING_CBOR,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    assert!(buffer.len > 0);

    // Format
    let mut status2 = fz_status::ok();
    let formatted_buffer = fz_common_format_range_a1(
        buffer.data,
        buffer.len,
        fz_encoding_format::FZ_ENCODING_CBOR,
        &mut status2,
    );
    assert_eq!(status2.code, fz_status_code::FZ_STATUS_OK);

    let result_str = unsafe {
        let slice = std::slice::from_raw_parts(formatted_buffer.data, formatted_buffer.len);
        String::from_utf8_lossy(slice).to_string()
    };
    assert_eq!(result_str, range_str);

    fz_buffer_free(buffer);
    fz_buffer_free(formatted_buffer);
}

#[test]
fn test_parse_tokenize() {
    let formula = "=SUM(A1, 10)";
    let c_formula = CString::new(formula).unwrap();
    let mut status = fz_status::ok();
    let options = fz_parse_options {
        include_spans: true,
        dialect: fz_formula_dialect::FZ_DIALECT_EXCEL,
    };

    unsafe {
        let buffer = fz_parse_tokenize(
            c_formula.as_ptr(),
            options,
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        assert!(buffer.len > 0);

        let json = String::from_utf8_lossy(std::slice::from_raw_parts(buffer.data, buffer.len));
        assert!(json.contains("SUM"));
        assert!(json.contains("A1"));
        assert!(json.contains("10"));
        assert!(json.contains("span"));

        fz_buffer_free(buffer);
    }
}

#[test]
fn test_parse_ast() {
    let formula = "=A1+10";
    let c_formula = CString::new(formula).unwrap();
    let mut status = fz_status::ok();
    let options = fz_parse_options {
        include_spans: false,
        dialect: fz_formula_dialect::FZ_DIALECT_EXCEL,
    };

    unsafe {
        let buffer = fz_parse_ast(
            c_formula.as_ptr(),
            options,
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        assert!(buffer.len > 0);

        let json = String::from_utf8_lossy(std::slice::from_raw_parts(buffer.data, buffer.len));
        assert!(json.contains(r#""type":"binary_op""#));
        assert!(json.contains(r#""op":"+""#));
        assert!(json.contains("A1"));
        assert!(json.contains("10"));
        // Should not contain spans
        assert!(!json.contains("span"));

        fz_buffer_free(buffer);
    }
}

#[test]
fn test_literal_value_normalization() {
    let value_json = r#"{"Text":"Hello"}"#;
    let mut status = fz_status::ok();

    unsafe {
        let buffer = fz_common_normalize_literal_value(
            value_json.as_ptr(),
            value_json.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        assert!(buffer.len > 0);

        let result_json =
            String::from_utf8_lossy(std::slice::from_raw_parts(buffer.data, buffer.len));
        assert!(result_json.contains("Hello"));

        fz_buffer_free(buffer);
    }
}

#[test]
fn test_workbook_invalid_formula() {
    let mut status = fz_status::ok();
    let wb = fz_workbook_create(&mut status);
    let sheet_name = CString::new("Sheet1").unwrap();
    fz_workbook_add_sheet(wb, sheet_name.as_ptr(), &mut status);

    let invalid_formula = CString::new("=SUM(").unwrap();
    fz_workbook_set_cell_formula(
        wb,
        sheet_name.as_ptr(),
        1,
        1,
        invalid_formula.as_ptr(),
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    // Error happens during evaluation/graph building
    let eval_buffer =
        fz_workbook_evaluate_all(wb, fz_encoding_format::FZ_ENCODING_JSON, &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
    let err_msg = unsafe {
        String::from_utf8_lossy(std::slice::from_raw_parts(
            status.error.data,
            status.error.len,
        ))
    };
    assert!(err_msg.contains("Unmatched opening parenthesis"));

    fz_buffer_free(eval_buffer);
    fz_buffer_free(status.error);
    fz_workbook_free(wb);
}

#[test]
fn test_workbook_full_loop() {
    let mut status = fz_status::ok();
    let wb = fz_workbook_create(&mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let sheet_name = CString::new("Sheet1").unwrap();
    fz_workbook_add_sheet(wb, sheet_name.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    // Set A1 = 10
    let a1_val = r#"{"Number": 10.0}"#;
    fz_workbook_set_cell_value(
        wb,
        sheet_name.as_ptr(),
        1,
        1,
        a1_val.as_ptr(),
        a1_val.len(),
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    // Set B1 = A1 * 2
    let b1_formula = CString::new("=A1*2").unwrap();
    fz_workbook_set_cell_formula(
        wb,
        sheet_name.as_ptr(),
        1,
        2,
        b1_formula.as_ptr(),
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    // Evaluate
    let eval_buffer =
        fz_workbook_evaluate_all(wb, fz_encoding_format::FZ_ENCODING_JSON, &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    assert!(eval_buffer.len > 0);
    fz_buffer_free(eval_buffer);

    // Get B1 value (should be 20)
    let val_buffer = fz_workbook_get_cell_value(
        wb,
        sheet_name.as_ptr(),
        1,
        2,
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    let val_json = unsafe {
        String::from_utf8_lossy(std::slice::from_raw_parts(val_buffer.data, val_buffer.len))
    };
    assert!(val_json.contains("20"));

    fz_buffer_free(val_buffer);
    fz_workbook_free(wb);
}

#[test]
fn test_workbook_sheet_management() {
    let mut status = fz_status::ok();
    let wb = fz_workbook_create(&mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let sheet1 = CString::new("Sheet1").unwrap();
    let sheet2 = CString::new("Sheet2").unwrap();
    fz_workbook_add_sheet(wb, sheet1.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    fz_workbook_add_sheet(wb, sheet2.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let a1_val = r#"{"Number": 3.0}"#;
    fz_workbook_set_cell_value(
        wb,
        sheet1.as_ptr(),
        1,
        1,
        a1_val.as_ptr(),
        a1_val.len(),
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let has_sheet1 = fz_workbook_has_sheet(wb, sheet1.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    assert_eq!(has_sheet1, 1);

    let names_buffer =
        fz_workbook_sheet_names(wb, fz_encoding_format::FZ_ENCODING_JSON, &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    let names: Vec<String> = serde_json::from_slice(&buffer_as_bytes(&names_buffer)).unwrap();
    assert!(names.contains(&"Sheet1".to_string()));
    assert!(names.contains(&"Sheet2".to_string()));
    fz_buffer_free(names_buffer);

    let renamed = CString::new("Inputs").unwrap();
    fz_workbook_rename_sheet(wb, sheet1.as_ptr(), renamed.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let has_old = fz_workbook_has_sheet(wb, sheet1.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    assert_eq!(has_old, 0);

    let dims_buffer = fz_workbook_sheet_dimensions(
        wb,
        renamed.as_ptr(),
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    let dims: SheetDimensions = serde_json::from_slice(&buffer_as_bytes(&dims_buffer)).unwrap();
    assert!(dims.rows >= 1);
    assert!(dims.cols >= 1);
    fz_buffer_free(dims_buffer);

    fz_workbook_delete_sheet(wb, sheet2.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    let has_sheet2 = fz_workbook_has_sheet(wb, sheet2.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    assert_eq!(has_sheet2, 0);

    fz_workbook_free(wb);
}

#[test]
fn test_workbook_range_read_write_json() {
    let mut status = fz_status::ok();
    let wb = fz_workbook_create(&mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let sheet = CString::new("Sheet1").unwrap();
    fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let values = vec![
        vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)],
        vec![
            LiteralValue::Text("Hi".to_string()),
            LiteralValue::Boolean(true),
        ],
    ];
    let values_payload = serde_json::to_vec(&values).unwrap();

    fz_workbook_set_values(
        wb,
        sheet.as_ptr(),
        1,
        1,
        values_payload.as_ptr(),
        values_payload.len(),
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let range = RangeAddress::new("Sheet1", 1, 1, 2, 2).unwrap();
    let range_payload = serde_json::to_vec(&range).unwrap();
    let range_buffer = fz_workbook_read_range(
        wb,
        range_payload.as_ptr(),
        range_payload.len(),
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let read_values: Vec<Vec<LiteralValue>> =
        serde_json::from_slice(&buffer_as_bytes(&range_buffer)).unwrap();
    assert_eq!(read_values, values);

    fz_buffer_free(range_buffer);
    fz_workbook_free(wb);
}

#[test]
fn test_workbook_set_formulas_batch() {
    let mut status = fz_status::ok();
    let wb = fz_workbook_create(&mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let sheet = CString::new("Sheet1").unwrap();
    fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let values = vec![
        vec![LiteralValue::Number(2.0)],
        vec![LiteralValue::Number(3.0)],
    ];
    let values_payload = serde_json::to_vec(&values).unwrap();
    fz_workbook_set_values(
        wb,
        sheet.as_ptr(),
        1,
        1,
        values_payload.as_ptr(),
        values_payload.len(),
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let formulas = vec![vec!["=A1*2".to_string()], vec!["=A2*3".to_string()]];
    let formulas_payload = serde_json::to_vec(&formulas).unwrap();
    fz_workbook_set_formulas(
        wb,
        sheet.as_ptr(),
        1,
        2,
        formulas_payload.as_ptr(),
        formulas_payload.len(),
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let eval_buffer =
        fz_workbook_evaluate_all(wb, fz_encoding_format::FZ_ENCODING_JSON, &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    fz_buffer_free(eval_buffer);

    let val_buffer = fz_workbook_get_cell_value(
        wb,
        sheet.as_ptr(),
        2,
        2,
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    let value: LiteralValue = serde_json::from_slice(&buffer_as_bytes(&val_buffer)).unwrap();
    match value {
        LiteralValue::Number(n) => assert!((n - 9.0).abs() < 1e-9),
        _ => panic!("Expected number value"),
    }

    fz_buffer_free(val_buffer);
    fz_workbook_free(wb);
}

#[test]
fn test_workbook_cross_sheet_reference() {
    let mut status = fz_status::ok();
    let wb = fz_workbook_create(&mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let sheet1 = CString::new("Sheet1").unwrap();
    let sheet2 = CString::new("Sheet2").unwrap();
    fz_workbook_add_sheet(wb, sheet1.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    fz_workbook_add_sheet(wb, sheet2.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let a1_val = r#"{"Number": 5.0}"#;
    fz_workbook_set_cell_value(
        wb,
        sheet1.as_ptr(),
        1,
        1,
        a1_val.as_ptr(),
        a1_val.len(),
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let b2_formula = CString::new("=Sheet1!A1+7").unwrap();
    fz_workbook_set_cell_formula(wb, sheet2.as_ptr(), 2, 2, b2_formula.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let eval_buffer =
        fz_workbook_evaluate_all(wb, fz_encoding_format::FZ_ENCODING_JSON, &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    let eval_result: CffiEvalResult =
        serde_json::from_slice(&buffer_as_bytes(&eval_buffer)).unwrap();
    assert_eq!(eval_result.cycle_errors, 0);
    assert!(eval_result.computed_vertices > 0);
    fz_buffer_free(eval_buffer);

    let val_buffer = fz_workbook_get_cell_value(
        wb,
        sheet2.as_ptr(),
        2,
        2,
        fz_encoding_format::FZ_ENCODING_JSON,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    let value: LiteralValue = serde_json::from_slice(&buffer_as_bytes(&val_buffer)).unwrap();
    match value {
        LiteralValue::Number(n) => assert!((n - 12.0).abs() < 1e-9),
        _ => panic!("Expected number value"),
    }
    fz_buffer_free(val_buffer);
    fz_workbook_free(wb);
}

#[test]
fn test_workbook_value_roundtrip_cbor() {
    let mut status = fz_status::ok();
    let wb = fz_workbook_create(&mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let sheet = CString::new("Sheet1").unwrap();
    fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let value = LiteralValue::Text("Hello".to_string());
    let mut payload = Vec::new();
    ciborium::into_writer(&value, &mut payload).unwrap();

    fz_workbook_set_cell_value(
        wb,
        sheet.as_ptr(),
        1,
        1,
        payload.as_ptr(),
        payload.len(),
        fz_encoding_format::FZ_ENCODING_CBOR,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let val_buffer = fz_workbook_get_cell_value(
        wb,
        sheet.as_ptr(),
        1,
        1,
        fz_encoding_format::FZ_ENCODING_CBOR,
        &mut status,
    );
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    let value_roundtrip: LiteralValue =
        ciborium::from_reader(Cursor::new(buffer_as_bytes(&val_buffer))).unwrap();
    assert_eq!(value_roundtrip, value);

    fz_buffer_free(val_buffer);
    fz_workbook_free(wb);
}

#[test]
fn test_workbook_cycle_errors_in_eval_result() {
    let mut status = fz_status::ok();
    let wb = fz_workbook_create(&mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let sheet = CString::new("Sheet1").unwrap();
    fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let a1_formula = CString::new("=B1").unwrap();
    fz_workbook_set_cell_formula(wb, sheet.as_ptr(), 1, 1, a1_formula.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let b1_formula = CString::new("=A1").unwrap();
    fz_workbook_set_cell_formula(wb, sheet.as_ptr(), 1, 2, b1_formula.as_ptr(), &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let eval_buffer =
        fz_workbook_evaluate_all(wb, fz_encoding_format::FZ_ENCODING_JSON, &mut status);
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    let eval_result: CffiEvalResult =
        serde_json::from_slice(&buffer_as_bytes(&eval_buffer)).unwrap();
    assert!(eval_result.cycle_errors > 0);
    fz_buffer_free(eval_buffer);

    fz_workbook_free(wb);
}

#[test]
fn test_canonical_formula() {
    let formula = "=sum( a1,10 )";
    let c_formula = CString::new(formula).unwrap();
    let mut status = fz_status::ok();

    unsafe {
        let buffer = fz_parse_canonical_formula(
            c_formula.as_ptr(),
            fz_formula_dialect::FZ_DIALECT_EXCEL,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let result = String::from_utf8_lossy(std::slice::from_raw_parts(buffer.data, buffer.len));
        assert_eq!(result, "=SUM(A1, 10)");

        fz_buffer_free(buffer);
    }
}
