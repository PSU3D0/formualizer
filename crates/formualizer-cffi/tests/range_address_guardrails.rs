use formualizer_cffi::*;
use serde::Serialize;
use std::process::Command;

#[derive(Serialize)]
struct RawRangeAddress {
    sheet: String,
    start_row: u32,
    start_col: u32,
    end_row: u32,
    end_col: u32,
}

fn payload(range: &RawRangeAddress, format: fz_encoding_format) -> Vec<u8> {
    match format {
        fz_encoding_format::FZ_ENCODING_JSON => serde_json::to_vec(range).unwrap(),
        fz_encoding_format::FZ_ENCODING_CBOR => {
            let mut bytes = Vec::new();
            ciborium::into_writer(range, &mut bytes).unwrap();
            bytes
        }
    }
}

unsafe fn clear_status(status: &mut fz_status) {
    let error = std::mem::replace(&mut status.error, fz_buffer::empty());
    unsafe {
        fz_buffer_free(error);
    }
}

#[test]
fn malformed_range_payloads_are_subprocess_safe() {
    const CHILD_ENV: &str = "FORMUALIZER_CFFI_RANGE_CHILD";
    const CASES: usize = 12;

    if let Some(case) = std::env::var_os(CHILD_ENV) {
        let case: usize = case.to_str().unwrap().parse().unwrap();
        let cases = [
            (0, 1, 1, 1),
            (1, 0, 1, 1),
            (2, 1, 1, 1),
            (1, 2, 1, 1),
            (1_048_577, 1, 1_048_577, 1),
            (1, 16_385, 1, 16_385),
        ];
        let (start_row, start_col, end_row, end_col) = cases[case / 2];
        let range = RawRangeAddress {
            sheet: "Sheet1".to_string(),
            start_row,
            start_col,
            end_row,
            end_col,
        };
        let format = if case.is_multiple_of(2) {
            fz_encoding_format::FZ_ENCODING_JSON
        } else {
            fz_encoding_format::FZ_ENCODING_CBOR
        };
        let bytes = payload(&range, format);

        unsafe {
            let mut status = fz_status::ok();
            let wb = fz_workbook_create(&mut status);
            let common =
                fz_common_format_range_a1(bytes.as_ptr(), bytes.len(), format, &mut status);
            let common_failed = status.code == fz_status_code::FZ_STATUS_ERROR;
            assert_eq!(common.len, 0);
            assert!(common.data.is_null());
            fz_buffer_free(common);
            clear_status(&mut status);

            let read = fz_workbook_read_range(wb, bytes.as_ptr(), bytes.len(), format, &mut status);
            assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
            assert_eq!(read.len, 0);
            assert!(common_failed);
            assert!(read.data.is_null());
            fz_buffer_free(read);
            clear_status(&mut status);
            fz_workbook_free(wb);
        }
        return;
    }

    for case in 0..CASES {
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("malformed_range_payloads_are_subprocess_safe")
            .env(CHILD_ENV, case.to_string())
            .status()
            .unwrap();
        assert!(child.success(), "range case {case} exited with {child}");
    }
}

#[test]
fn valid_last_cell_and_invalid_ranges_preserve_workbook_state() {
    unsafe {
        let mut status = fz_status::ok();
        let wb = fz_workbook_create(&mut status);
        let sheet = std::ffi::CString::new("Sheet1").unwrap();
        fz_workbook_add_sheet(wb, sheet.as_ptr(), &mut status);
        let value = b"{\"Number\":7.0}";
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

        let last = RawRangeAddress {
            sheet: "Sheet1".to_string(),
            start_row: 1_048_576,
            start_col: 16_384,
            end_row: 1_048_576,
            end_col: 16_384,
        };
        let invalid = RawRangeAddress {
            sheet: "Sheet1".to_string(),
            start_row: 0,
            start_col: 1,
            end_row: 1,
            end_col: 1,
        };
        let anchor = RawRangeAddress {
            sheet: "Sheet1".to_string(),
            start_row: 1,
            start_col: 1,
            end_row: 1,
            end_col: 1,
        };

        for format in [
            fz_encoding_format::FZ_ENCODING_JSON,
            fz_encoding_format::FZ_ENCODING_CBOR,
        ] {
            let last_payload = payload(&last, format);
            let formatted = fz_common_format_range_a1(
                last_payload.as_ptr(),
                last_payload.len(),
                format,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
            let formatted_bytes = std::slice::from_raw_parts(formatted.data, formatted.len);
            assert_eq!(formatted_bytes, b"Sheet1!XFD1048576");
            fz_buffer_free(formatted);

            let last_read = fz_workbook_read_range(
                wb,
                last_payload.as_ptr(),
                last_payload.len(),
                format,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
            assert!(!last_read.data.is_null());
            fz_buffer_free(last_read);

            let invalid_payload = payload(&invalid, format);
            let invalid_common = fz_common_format_range_a1(
                invalid_payload.as_ptr(),
                invalid_payload.len(),
                format,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
            assert_eq!(invalid_common.len, 0);
            assert!(invalid_common.data.is_null());
            fz_buffer_free(invalid_common);
            clear_status(&mut status);

            let invalid_read = fz_workbook_read_range(
                wb,
                invalid_payload.as_ptr(),
                invalid_payload.len(),
                format,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
            assert_eq!(invalid_read.len, 0);
            assert!(invalid_read.data.is_null());
            fz_buffer_free(invalid_read);
            clear_status(&mut status);

            let no_status_common = fz_common_format_range_a1(
                invalid_payload.as_ptr(),
                invalid_payload.len(),
                format,
                std::ptr::null_mut(),
            );
            assert_eq!(no_status_common.len, 0);
            assert!(no_status_common.data.is_null());
            fz_buffer_free(no_status_common);
            let no_status_read = fz_workbook_read_range(
                wb,
                invalid_payload.as_ptr(),
                invalid_payload.len(),
                format,
                std::ptr::null_mut(),
            );
            assert_eq!(no_status_read.len, 0);
            assert!(no_status_read.data.is_null());
            fz_buffer_free(no_status_read);

            let anchor_payload = payload(&anchor, format);
            let anchor_read = fz_workbook_read_range(
                wb,
                anchor_payload.as_ptr(),
                anchor_payload.len(),
                format,
                &mut status,
            );
            assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
            let anchor_bytes = std::slice::from_raw_parts(anchor_read.data, anchor_read.len);
            let values: Vec<Vec<formualizer_common::LiteralValue>> = match format {
                fz_encoding_format::FZ_ENCODING_JSON => {
                    serde_json::from_slice(anchor_bytes).unwrap()
                }
                fz_encoding_format::FZ_ENCODING_CBOR => {
                    ciborium::from_reader(anchor_bytes).unwrap()
                }
            };
            assert_eq!(
                values,
                vec![vec![formualizer_common::LiteralValue::Number(7.0)]]
            );
            fz_buffer_free(anchor_read);
        }
        fz_workbook_free(wb);
    }
}

#[test]
fn parse_range_rejects_out_of_grid_and_accepts_last_cell() {
    unsafe {
        for format in [
            fz_encoding_format::FZ_ENCODING_JSON,
            fz_encoding_format::FZ_ENCODING_CBOR,
        ] {
            for input in ["Sheet1!A0", "Sheet1!A1048577", "Sheet1!XFE1"] {
                let input = std::ffi::CString::new(input).unwrap();
                let mut status = fz_status::ok();
                let buffer = fz_common_parse_range_a1(input.as_ptr(), format, &mut status);
                assert_eq!(status.code, fz_status_code::FZ_STATUS_ERROR);
                assert_eq!(buffer.len, 0);
                assert!(buffer.data.is_null());
                fz_buffer_free(buffer);
                clear_status(&mut status);
            }

            let input = std::ffi::CString::new("Sheet1!XFD1048576").unwrap();
            let mut status = fz_status::ok();
            let buffer = fz_common_parse_range_a1(input.as_ptr(), format, &mut status);
            assert_eq!(status.code, fz_status_code::FZ_STATUS_OK);
            fz_buffer_free(buffer);
        }
    }
}
