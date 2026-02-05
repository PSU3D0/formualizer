use formualizer_cffi::*;
use formualizer_common::LiteralValue;
use std::ffi::CString;

#[test]
fn cffi_open_xlsx_update_evaluate() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("cffi_open.xlsx");

    let mut book = umya_spreadsheet::new_file();
    let ws = book.get_sheet_by_name_mut("Sheet1").expect("default sheet");
    ws.get_cell_mut((1, 1)).set_value_number(10);
    ws.get_cell_mut((2, 1)).set_formula("A1*2");
    umya_spreadsheet::writer::xlsx::write(&book, &path).expect("write xlsx");

    let path_c = CString::new(path.to_string_lossy().as_ref()).expect("cstr path");
    let mut status = fz_status::ok();
    let wb = unsafe { fz_workbook_open_xlsx(path_c.as_ptr(), &mut status) };
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
    assert!(!wb.0.is_null());

    let sheet = CString::new("Sheet1").unwrap();
    let value_json = "{\"Number\":15.0}";
    unsafe {
        fz_workbook_set_cell_value(
            wb,
            sheet.as_ptr(),
            1,
            1,
            value_json.as_ptr(),
            value_json.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        );
    }
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    #[derive(serde::Serialize)]
    struct CellTarget {
        sheet: String,
        row: u32,
        col: u32,
    }

    let targets = vec![CellTarget {
        sheet: "Sheet1".to_string(),
        row: 1,
        col: 2,
    }];
    let targets_payload = serde_json::to_vec(&targets).expect("targets json");

    let eval_buffer = unsafe {
        fz_workbook_evaluate_cells(
            wb,
            targets_payload.as_ptr(),
            targets_payload.len(),
            fz_encoding_format::FZ_ENCODING_JSON,
            &mut status,
        )
    };
    assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);

    let bytes = if eval_buffer.data.is_null() || eval_buffer.len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(eval_buffer.data, eval_buffer.len).to_vec() }
    };
    let values: Vec<LiteralValue> = serde_json::from_slice(&bytes).expect("values json");
    unsafe { fz_buffer_free(eval_buffer) };

    assert_eq!(values.len(), 1);
    match values[0] {
        LiteralValue::Number(n) => assert!((n - 30.0).abs() < 1e-9),
        _ => panic!("expected number result"),
    }

    unsafe { fz_workbook_free(wb) };
}
