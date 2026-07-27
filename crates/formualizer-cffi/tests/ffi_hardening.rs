//! Regression tests for the FFI hardening pass.
//!
//! These cover three classes of defect that previously escaped the C ABI:
//!
//! * out-of-grid / 0-based coordinates reaching engine internals that assert,
//! * `RangeAddress` payloads deserialised straight past the checked
//!   constructor, and
//! * error messages emitted as Rust `Debug` output rather than valid JSON.

use formualizer_cffi::*;
use std::ffi::CString;

/// Read a status' error buffer as a UTF-8 string and free it.
unsafe fn take_error(status: &mut fz_status) -> String {
    assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
    let buf = std::mem::replace(&mut status.error, fz_buffer::empty());
    assert!(!buf.data.is_null(), "error status must carry a message");
    let bytes = unsafe { std::slice::from_raw_parts(buf.data, buf.len) }.to_vec();
    unsafe { fz_buffer_free(buf) };
    String::from_utf8(bytes).expect("error payload must be UTF-8")
}

fn new_workbook_with_sheet(name: &str) -> (fz_workbook_h, CString) {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        let sheet = CString::new(name).unwrap();
        fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        (wb, sheet)
    }
}

#[test]
fn error_payload_is_valid_json() {
    unsafe {
        let mut status = fz_status::ok();
        // A formula containing characters that Rust's `{:?}` escapes in a
        // non-JSON way; the resulting message must still parse as JSON.
        let formula = CString::new("=SUM(\u{7f}\u{1}\"x\")").unwrap();
        let buffer = fz_parse_ast(
            formula.as_ptr(),
            fz_parse_options {
                include_spans: false,
                dialect: fz_formula_dialect::FZ_DIALECT_EXCEL,
            },
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );

        if status.code == fz_status_code::FZ_STATUS_ERROR {
            let raw = take_error(&mut status);
            let parsed: serde_json::Value =
                serde_json::from_str(&raw).unwrap_or_else(|e| panic!("not valid JSON ({e}): {raw}"));
            assert!(parsed.get("message").and_then(|m| m.as_str()).is_some());
        } else {
            fz_buffer_free(buffer);
        }
    }
}

#[test]
fn set_cell_value_rejects_zero_based_coordinates() {
    unsafe {
        let (wb, sheet) = new_workbook_with_sheet("Sheet1");
        let mut status = fz_status::ok();
        let payload = "{\"Number\":1.0}";

        for (row, col) in [(0u32, 1u32), (1, 0)] {
            fz_workbook_set_cell_value(
                wb,
                sheet.as_ptr(),
                row,
                col,
                payload.as_ptr(),
                payload.len(),
                fz_encoding_format::FZ_ENCODING_JSON,
                &mut status,
            );
            let msg = take_error(&mut status);
            assert!(msg.contains("1-based"), "unexpected message: {msg}");
        }

        fz_workbook_free(wb);
    }
}

#[test]
fn set_cell_value_rejects_out_of_grid_coordinates() {
    unsafe {
        let (wb, sheet) = new_workbook_with_sheet("Sheet1");
        let mut status = fz_status::ok();
        let payload = "{\"Number\":1.0}";

        // Beyond Excel's 1,048,576 x 16,384 grid. Previously this reached
        // `Coord::new`, whose `assert!` aborted the process across the C ABI.
        for (row, col) in [(u32::MAX, 1u32), (1, u32::MAX)] {
            fz_workbook_set_cell_value(
                wb,
                sheet.as_ptr(),
                row,
                col,
                payload.as_ptr(),
                payload.len(),
                fz_encoding_format::FZ_ENCODING_JSON,
                &mut status,
            );
            let msg = take_error(&mut status);
            assert!(msg.contains("exceeds maximum"), "unexpected message: {msg}");
        }

        fz_workbook_free(wb);
    }
}

#[test]
fn get_cell_value_rejects_out_of_grid_coordinates() {
    unsafe {
        let (wb, sheet) = new_workbook_with_sheet("Sheet1");
        let mut status = fz_status::ok();

        let buffer = fz_workbook_get_cell_value(
            wb,
            sheet.as_ptr(),
            u32::MAX,
            1,
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert!(buffer.data.is_null());
        let _ = take_error(&mut status);

        fz_workbook_free(wb);
    }
}

#[test]
fn set_values_rejects_block_running_off_the_grid() {
    unsafe {
        let (wb, sheet) = new_workbook_with_sheet("Sheet1");
        let mut status = fz_status::ok();

        // Anchored on the last row, but two rows tall.
        let payload = "[[{\"Number\":1.0}],[{\"Number\":2.0}]]";
        fz_workbook_set_values(
            wb,
            sheet.as_ptr(),
            1_048_576,
            1,
            payload.as_ptr(),
            payload.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        let msg = take_error(&mut status);
        assert!(msg.contains("exceeds maximum"), "unexpected message: {msg}");

        fz_workbook_free(wb);
    }
}

#[test]
fn read_range_rejects_unvalidated_range_payloads() {
    unsafe {
        let (wb, _sheet) = new_workbook_with_sheet("Sheet1");
        let mut status = fz_status::ok();

        // 0-based rows: `to_sheet_range` used to underflow on `start_row - 1`.
        let zero_based =
            r#"{"sheet":"Sheet1","start_row":0,"start_col":1,"end_row":1,"end_col":1}"#;
        let buffer = fz_workbook_read_range(
            wb,
            zero_based.as_ptr(),
            zero_based.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert!(buffer.data.is_null());
        let msg = take_error(&mut status);
        assert!(msg.contains("1-based"), "unexpected message: {msg}");

        // Inverted range: `height()`/`width()` used to underflow to ~4 billion
        // and drive an enormous allocation.
        let inverted =
            r#"{"sheet":"Sheet1","start_row":10,"start_col":1,"end_row":2,"end_col":1}"#;
        let buffer = fz_workbook_read_range(
            wb,
            inverted.as_ptr(),
            inverted.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert!(buffer.data.is_null());
        let msg = take_error(&mut status);
        assert!(msg.contains("ordered"), "unexpected message: {msg}");

        fz_workbook_free(wb);
    }
}

#[test]
fn evaluate_cells_rejects_out_of_grid_targets() {
    unsafe {
        let (wb, _sheet) = new_workbook_with_sheet("Sheet1");
        let mut status = fz_status::ok();

        let targets = r#"[{"sheet":"Sheet1","row":4294967295,"col":1}]"#;
        let buffer = fz_workbook_evaluate_cells(
            wb,
            targets.as_ptr(),
            targets.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert!(buffer.data.is_null());
        let _ = take_error(&mut status);

        fz_workbook_free(wb);
    }
}

#[test]
fn format_range_a1_rejects_unvalidated_payloads() {
    unsafe {
        let mut status = fz_status::ok();
        let payload = r#"{"sheet":"S","start_row":0,"start_col":0,"end_row":0,"end_col":0}"#;
        let buffer = fz_common_format_range_a1(
            payload.as_ptr(),
            payload.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert!(buffer.data.is_null());
        let msg = take_error(&mut status);
        assert!(msg.contains("1-based"), "unexpected message: {msg}");
    }
}

#[test]
fn valid_round_trip_still_succeeds() {
    unsafe {
        let (wb, sheet) = new_workbook_with_sheet("Sheet1");
        let mut status = fz_status::ok();

        let payload = "{\"Number\":21.0}";
        fz_workbook_set_cell_value(
            wb,
            sheet.as_ptr(),
            1,
            1,
            payload.as_ptr(),
            payload.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let formula = CString::new("=A1*2").unwrap();
        fz_workbook_set_cell_formula(wb, sheet.as_ptr(), 1, 2, formula.as_ptr(), &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let targets = r#"[{"sheet":"Sheet1","row":1,"col":2}]"#;
        let buffer = fz_workbook_evaluate_cells(
            wb,
            targets.as_ptr(),
            targets.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        assert!(buffer.len > 0);
        fz_buffer_free(buffer);

        // Boundary coordinates must remain accepted.
        fz_workbook_get_cell_value(
            wb,
            sheet.as_ptr(),
            1_048_576,
            16_384,
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        fz_workbook_free(wb);
    }
}
