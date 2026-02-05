use formualizer_cffi::*;
use std::ffi::CString;
use std::ptr;

#[test]
fn open_xlsx_null_path_reports_error() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_open_xlsx(ptr::null(), &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        assert!(wb.0.is_null());
        fz_buffer_free(status.error);
    }
}

#[test]
fn set_cell_value_rejects_invalid_payload() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let sheet = CString::new("Sheet1").unwrap();
        fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let invalid = "{not-json}";
        fz_workbook_set_cell_value(
            wb,
            sheet.as_ptr(),
            1,
            1,
            invalid.as_ptr(),
            invalid.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        fz_buffer_free(status.error);

        fz_workbook_free(wb);
    }
}

#[test]
fn evaluate_cells_rejects_empty_payload() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let buffer = fz_workbook_evaluate_cells(
            wb,
            ptr::null(),
            0,
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        assert!(buffer.data.is_null());
        assert_eq!(buffer.len, 0);
        fz_buffer_free(status.error);

        fz_workbook_free(wb);
    }
}

#[test]
fn sheet_names_allows_null_status() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let buffer =
            fz_workbook_sheet_names(wb, fz_encoding_format::FZ_ENCODING_JSON, ptr::null_mut());
        assert!(!buffer.data.is_null());
        assert!(buffer.len > 0);
        fz_buffer_free(buffer);

        fz_workbook_free(wb);
    }
}

#[test]
fn tokenize_buffer_roundtrip_multiple_times() {
    unsafe {
        let mut status = fz_status::ok();
        let formula = CString::new("=SUM(A1,2)").unwrap();
        let options = fz_parse_options {
            include_spans: false,
            dialect: fz_formula_dialect::FZ_DIALECT_EXCEL,
        };

        for _ in 0..64 {
            let buffer = fz_parse_tokenize(
                formula.as_ptr(),
                options,
                fz_encoding_format::FZ_ENCODING_JSON,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
            fz_buffer_free(buffer);
        }
    }
}

#[test]
fn parse_ast_null_formula_reports_error() {
    unsafe {
        let mut status = fz_status::ok();
        let options = fz_parse_options {
            include_spans: false,
            dialect: fz_formula_dialect::FZ_DIALECT_EXCEL,
        };
        let buffer = fz_parse_ast(
            ptr::null(),
            options,
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        assert!(buffer.data.is_null());
        assert_eq!(buffer.len, 0);
        fz_buffer_free(status.error);
    }
}

#[test]
fn add_sheet_null_name_reports_error() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        fz_workbook_add_sheet(wb, ptr::null(), &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        fz_buffer_free(status.error);

        fz_workbook_free(wb);
    }
}

#[test]
fn set_formula_null_pointer_reports_error() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let sheet = CString::new("Sheet1").unwrap();
        fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        fz_workbook_set_cell_formula(wb, sheet.as_ptr(), 1, 1, ptr::null(), &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        fz_buffer_free(status.error);

        fz_workbook_free(wb);
    }
}

#[test]
fn sheet_dimensions_missing_sheet_reports_error() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let sheet = CString::new("Missing").unwrap();
        let buffer = fz_workbook_sheet_dimensions(
            wb,
            sheet.as_ptr(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        assert!(buffer.data.is_null());
        assert_eq!(buffer.len, 0);
        fz_buffer_free(status.error);

        fz_workbook_free(wb);
    }
}

#[test]
fn evaluate_cells_rejects_invalid_json_payload() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let invalid = "[not-json]";
        let buffer = fz_workbook_evaluate_cells(
            wb,
            invalid.as_ptr(),
            invalid.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        assert!(buffer.data.is_null());
        assert_eq!(buffer.len, 0);
        fz_buffer_free(status.error);

        fz_workbook_free(wb);
    }
}

#[test]
fn evaluate_cells_allows_null_status_pointer() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let sheet = CString::new("Sheet1").unwrap();
        fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let a1_json = "{\"Number\":8.0}";
        fz_workbook_set_cell_value(
            wb,
            sheet.as_ptr(),
            1,
            1,
            a1_json.as_ptr(),
            a1_json.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let formula = CString::new("=A1*2").unwrap();
        fz_workbook_set_cell_formula(wb, sheet.as_ptr(), 1, 2, formula.as_ptr(), &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let targets = "[{\"sheet\":\"Sheet1\",\"row\":1,\"col\":2}]";
        let buffer = fz_workbook_evaluate_cells(
            wb,
            targets.as_ptr(),
            targets.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            ptr::null_mut(),
        );
        assert!(buffer.len > 0);
        fz_buffer_free(buffer);

        fz_workbook_free(wb);
    }
}
