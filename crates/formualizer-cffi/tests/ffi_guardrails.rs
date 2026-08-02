use formualizer_cffi::*;
use std::ffi::CString;
use std::process::Command;
use std::ptr;

unsafe fn clear_status(status: &mut fz_status) {
    let error = std::mem::replace(&mut status.error, fz_buffer::empty());
    unsafe {
        fz_buffer_free(error);
    }
}

#[test]
fn invalid_coordinate_subprocess() {
    if std::env::var_os("FORMUALIZER_CFFI_COORDINATE_CHILD").is_some() {
        unsafe {
            let mut status = fz_status::ok();
            let wb = fz_workbook_create(&mut status);
            let sheet = CString::new("Sheet1").unwrap();
            fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);
            let value = "{\"Number\":1.0}";
            fz_workbook_set_cell_value(
                wb,
                sheet.as_ptr(),
                1,
                1,
                value.as_ptr(),
                value.len(),
                fz_encoding_format::FZ_ENCODING_JSON,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
            let values = "[[{\"Number\":1.0}],[{\"Number\":2.0}]]";
            fz_workbook_set_values(
                wb,
                sheet.as_ptr(),
                1_048_576,
                1,
                values.as_ptr(),
                values.len(),
                fz_encoding_format::FZ_ENCODING_JSON,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
            clear_status(&mut status);
            fz_workbook_free(wb);
        }
        return;
    }

    let status = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("invalid_coordinate_subprocess")
        .env("FORMUALIZER_CFFI_COORDINATE_CHILD", "1")
        .status()
        .unwrap();
    assert!(status.success(), "coordinate child exited with {status}");
}

#[test]
fn coordinate_boundaries_are_checked_atomically() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        let sheet = CString::new("Sheet1").unwrap();
        fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);

        let value = "{\"Number\":7.0}";
        fz_workbook_set_cell_value(
            wb,
            sheet.as_ptr(),
            1,
            1,
            value.as_ptr(),
            value.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        for &(row, col) in &[(0, 1), (1_048_576 + 1, 1), (1, 0), (1, 16_384 + 1)] {
            let cell = fz_workbook_get_cell_value(
                wb,
                sheet.as_ptr(),
                row,
                col,
                fz_encoding_format::FZ_ENCODING_JSON,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
            fz_buffer_free(cell);
            clear_status(&mut status);

            let formula = fz_workbook_get_cell_formula(wb, sheet.as_ptr(), row, col, &mut status);
            assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
            fz_buffer_free(formula);
            clear_status(&mut status);

            fz_workbook_set_cell_value(
                wb,
                sheet.as_ptr(),
                row,
                col,
                value.as_ptr(),
                value.len(),
                fz_encoding_format::FZ_ENCODING_JSON,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
            clear_status(&mut status);

            let formula = CString::new("=1").unwrap();
            fz_workbook_set_cell_formula(
                wb,
                sheet.as_ptr(),
                row,
                col,
                formula.as_ptr(),
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
            clear_status(&mut status);

            let targets = format!("[{{\"sheet\":\"Sheet1\",\"row\":{row},\"col\":{col}}}]");
            let evaluated = fz_workbook_evaluate_cells(
                wb,
                targets.as_ptr(),
                targets.len(),
                fz_encoding_format::FZ_ENCODING_JSON,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
            fz_buffer_free(evaluated);
            clear_status(&mut status);
        }

        for &(row, col) in &[(1_048_576, 16_384), (1_048_576, 1), (1, 16_384)] {
            let cell = fz_workbook_get_cell_value(
                wb,
                sheet.as_ptr(),
                row,
                col,
                fz_encoding_format::FZ_ENCODING_JSON,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
            fz_buffer_free(cell);
        }

        let values = "[[{\"Number\":1.0}],[{\"Number\":2.0}]]";
        fz_workbook_set_values(
            wb,
            sheet.as_ptr(),
            1_048_576,
            1,
            values.as_ptr(),
            values.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        clear_status(&mut status);

        let formulas = "[[\"=1\",\"=2\"],[\"=3\",\"=4\"]]";
        fz_workbook_set_formulas(
            wb,
            sheet.as_ptr(),
            1,
            16_384,
            formulas.as_ptr(),
            formulas.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        clear_status(&mut status);

        let cell = fz_workbook_get_cell_value(
            wb,
            sheet.as_ptr(),
            1,
            1,
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        let bytes = std::slice::from_raw_parts(cell.data, cell.len);
        assert!(std::str::from_utf8(bytes).unwrap().contains("7"));
        fz_buffer_free(cell);
        fz_workbook_free(wb);
    }
}

#[test]
fn ragged_block_extents_match_mutated_cells() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        let sheet = CString::new("Sheet1").unwrap();
        fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);

        let anchor = "{\"Number\":7.0}";
        fz_workbook_set_cell_value(
            wb,
            sheet.as_ptr(),
            1,
            1,
            anchor.as_ptr(),
            anchor.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let final_row = 1_048_576;
        let trailing_values = "[[{\"Number\":11.0}],[]]";
        fz_workbook_set_values(
            wb,
            sheet.as_ptr(),
            final_row,
            1,
            trailing_values.as_ptr(),
            trailing_values.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let final_value = fz_workbook_get_cell_value(
            wb,
            sheet.as_ptr(),
            final_row,
            1,
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        let final_value_bytes = std::slice::from_raw_parts(final_value.data, final_value.len);
        assert!(
            std::str::from_utf8(final_value_bytes)
                .unwrap()
                .contains("11")
        );
        fz_buffer_free(final_value);

        let value_dimensions = fz_workbook_sheet_dimensions(
            wb,
            sheet.as_ptr(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        let value_dimensions_bytes =
            std::slice::from_raw_parts(value_dimensions.data, value_dimensions.len).to_vec();
        fz_buffer_free(value_dimensions);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&value_dimensions_bytes).unwrap(),
            serde_json::json!({"rows": final_row, "cols": 1})
        );

        let seed_formula = "[[\"=13\"]]";
        fz_workbook_set_formulas(
            wb,
            sheet.as_ptr(),
            final_row,
            2,
            seed_formula.as_ptr(),
            seed_formula.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let dimensions_before = fz_workbook_sheet_dimensions(
            wb,
            sheet.as_ptr(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        let dimensions_before_bytes =
            std::slice::from_raw_parts(dimensions_before.data, dimensions_before.len).to_vec();
        fz_buffer_free(dimensions_before);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&dimensions_before_bytes).unwrap(),
            serde_json::json!({"rows": final_row, "cols": 2})
        );

        let trailing_formulas = "[[\"=19\"],[]]";
        fz_workbook_set_formulas(
            wb,
            sheet.as_ptr(),
            final_row,
            2,
            trailing_formulas.as_ptr(),
            trailing_formulas.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        clear_status(&mut status);

        let dimensions_after = fz_workbook_sheet_dimensions(
            wb,
            sheet.as_ptr(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        let dimensions_after_bytes =
            std::slice::from_raw_parts(dimensions_after.data, dimensions_after.len).to_vec();
        fz_buffer_free(dimensions_after);
        assert_eq!(dimensions_after_bytes, dimensions_before_bytes);

        let final_formula =
            fz_workbook_get_cell_formula(wb, sheet.as_ptr(), final_row, 2, &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        let final_formula_bytes = std::slice::from_raw_parts(final_formula.data, final_formula.len);
        assert_eq!(std::str::from_utf8(final_formula_bytes).unwrap(), "=13");
        fz_buffer_free(final_formula);

        let leading_values = "[[],[{\"Number\":17.0}]]";
        fz_workbook_set_values(
            wb,
            sheet.as_ptr(),
            final_row,
            1,
            leading_values.as_ptr(),
            leading_values.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        clear_status(&mut status);

        let leading_formulas = "[[],[\"=19\"]]";
        fz_workbook_set_formulas(
            wb,
            sheet.as_ptr(),
            final_row,
            2,
            leading_formulas.as_ptr(),
            leading_formulas.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        clear_status(&mut status);

        let preserved_value = fz_workbook_get_cell_value(
            wb,
            sheet.as_ptr(),
            final_row,
            1,
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        let preserved_value_bytes =
            std::slice::from_raw_parts(preserved_value.data, preserved_value.len);
        assert!(
            std::str::from_utf8(preserved_value_bytes)
                .unwrap()
                .contains("11")
        );
        fz_buffer_free(preserved_value);

        let preserved_formula =
            fz_workbook_get_cell_formula(wb, sheet.as_ptr(), final_row, 2, &mut status);
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        let preserved_formula_bytes =
            std::slice::from_raw_parts(preserved_formula.data, preserved_formula.len);
        assert_eq!(std::str::from_utf8(preserved_formula_bytes).unwrap(), "=13");
        fz_buffer_free(preserved_formula);

        let interior_values = "[[{\"Number\":21.0}],[],[{\"Number\":23.0}]]";
        fz_workbook_set_values(
            wb,
            sheet.as_ptr(),
            10,
            1,
            interior_values.as_ptr(),
            interior_values.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let interior_formulas = "[[\"=25\"],[],[\"=27\"]]";
        fz_workbook_set_formulas(
            wb,
            sheet.as_ptr(),
            10,
            2,
            interior_formulas.as_ptr(),
            interior_formulas.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let empty_values = "[[],[]]";
        fz_workbook_set_values(
            wb,
            sheet.as_ptr(),
            final_row,
            16_384,
            empty_values.as_ptr(),
            empty_values.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let empty_formulas = "[[],[]]";
        fz_workbook_set_formulas(
            wb,
            sheet.as_ptr(),
            final_row,
            16_384,
            empty_formulas.as_ptr(),
            empty_formulas.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

        let invalid_empty_values = "[[],[]]";
        fz_workbook_set_values(
            wb,
            sheet.as_ptr(),
            0,
            1,
            invalid_empty_values.as_ptr(),
            invalid_empty_values.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        clear_status(&mut status);

        let invalid_empty_formulas = "[[],[]]";
        fz_workbook_set_formulas(
            wb,
            sheet.as_ptr(),
            1,
            0,
            invalid_empty_formulas.as_ptr(),
            invalid_empty_formulas.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        clear_status(&mut status);

        let anchor_value = fz_workbook_get_cell_value(
            wb,
            sheet.as_ptr(),
            1,
            1,
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        let anchor_bytes = std::slice::from_raw_parts(anchor_value.data, anchor_value.len);
        assert!(std::str::from_utf8(anchor_bytes).unwrap().contains("7"));
        fz_buffer_free(anchor_value);
        fz_workbook_free(wb);
    }
}

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

#[test]
fn evaluate_cells_reports_targeted_and_full_preparation_failures() {
    unsafe {
        let mut status = fz_status::ok();
        let mut config = formualizer_workbook::WorkbookConfig::interactive();
        config.eval.evaluation_budgets.work.max_work_units = Some(0);
        let workbook = formualizer_workbook::Workbook::new_with_config(config);
        let opaque = Box::new(OpaqueWorkbook(std::sync::Arc::new(std::sync::RwLock::new(
            workbook,
        ))));
        let wb = fz_workbook_h(Box::into_raw(opaque) as *mut std::ffi::c_void);

        for name in ["Existing", "Target"] {
            let name = CString::new(name).unwrap();
            fz_workbook_add_sheet(wb, name.as_ptr(), &mut status);
            assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        }
        for (sheet, formula) in [("Existing", "=1"), ("Target", "=2")] {
            let sheet = CString::new(sheet).unwrap();
            let formula = CString::new(formula).unwrap();
            fz_workbook_set_cell_formula(wb, sheet.as_ptr(), 1, 1, formula.as_ptr(), &mut status);
            assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
        }

        let targets = r#"[{"sheet":"Target","row":1,"col":1}]"#;
        let result = fz_workbook_evaluate_cells(
            wb,
            targets.as_ptr(),
            targets.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        assert!(result.data.is_null());
        assert_eq!(result.len, 0);
        let error = std::slice::from_raw_parts(status.error.data, status.error.len);
        let error_json: serde_json::Value = serde_json::from_slice(error).unwrap();
        let message = error_json["message"].as_str().unwrap();
        let targeted = message.find("targeted graph preparation failed: ").unwrap();
        let full = message
            .find("full graph preparation fallback failed: ")
            .unwrap();
        assert!(targeted < full);
        let targeted_message = &message[targeted..full];
        let full_message = &message[full..];
        assert!(targeted_message.contains("evaluation resource exhausted: work_units"));
        assert!(targeted_message.contains("work_units (observed 1, limit 0)"));
        assert!(!targeted_message.contains("work_units (observed 2, limit 0)"));
        assert!(full_message.contains("evaluation resource exhausted: work_units"));
        assert!(full_message.contains("work_units (observed 2, limit 0)"));
        assert!(!full_message.contains("work_units (observed 1, limit 0)"));
        let first_message = message.to_string();
        fz_buffer_free(result);
        clear_status(&mut status);

        let null_status = fz_workbook_evaluate_cells(
            wb,
            targets.as_ptr(),
            targets.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            ptr::null_mut(),
        );
        assert!(null_status.data.is_null());
        assert_eq!(null_status.len, 0);
        fz_buffer_free(null_status);

        let repeated = fz_workbook_evaluate_cells(
            wb,
            targets.as_ptr(),
            targets.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
        assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
        assert!(repeated.data.is_null());
        assert_eq!(repeated.len, 0);
        let repeated_error = std::slice::from_raw_parts(status.error.data, status.error.len);
        let repeated_json: serde_json::Value = serde_json::from_slice(repeated_error).unwrap();
        assert_eq!(
            repeated_json["message"].as_str(),
            Some(first_message.as_str())
        );
        fz_buffer_free(repeated);
        clear_status(&mut status);

        for (sheet, expected_formula) in [("Existing", "=1"), ("Target", "=2")] {
            let sheet = CString::new(sheet).unwrap();
            let formula = fz_workbook_get_cell_formula(wb, sheet.as_ptr(), 1, 1, &mut status);
            assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
            let formula_bytes = std::slice::from_raw_parts(formula.data, formula.len);
            assert_eq!(formula_bytes, expected_formula.as_bytes());
            fz_buffer_free(formula);
        }
        clear_status(&mut status);
        fz_workbook_free(wb);
    }
}
