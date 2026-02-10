#![cfg(feature = "csv")]

use formualizer_common::LiteralValue;
use formualizer_workbook::traits::{
    SaveDestination, SheetData, SpreadsheetReader, SpreadsheetWriter,
};

use formualizer_workbook::backends::csv::{
    CsvAdapter, CsvArrayPolicy, CsvNewline, CsvReadOptions, CsvTypeInference, CsvWriteOptions,
};

fn sheet_value_map(sheet: &SheetData) -> std::collections::BTreeMap<(u32, u32), LiteralValue> {
    let mut out = std::collections::BTreeMap::new();
    for (k, cd) in sheet.cells.iter() {
        if let Some(v) = cd.value.clone() {
            out.insert(*k, v);
        }
    }
    out
}

#[test]
fn csv_roundtrip_simple() {
    let input = b"1,2,hello\n3,4.5,FALSE\n".to_vec();
    let read_opts = CsvReadOptions {
        has_headers: false,
        type_inference: CsvTypeInference::Basic,
        ..CsvReadOptions::default()
    };
    let mut adapter = CsvAdapter::open_bytes_with_options(input, read_opts.clone()).unwrap();

    let s = adapter.sheet_names().unwrap();
    assert_eq!(s, vec!["Sheet1".to_string()]);

    let sheet = adapter.read_sheet("Sheet1").unwrap();
    assert_eq!(sheet.dimensions, Some((2, 3)));
    assert_eq!(
        sheet.cells.get(&(1, 1)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Int(1))
    );
    assert_eq!(
        sheet.cells.get(&(2, 2)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Number(4.5))
    );
    assert_eq!(
        sheet.cells.get(&(2, 3)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Boolean(false))
    );

    let out = adapter
        .write_sheet_to("Sheet1", SaveDestination::Bytes, CsvWriteOptions::default())
        .unwrap()
        .unwrap();

    let mut adapter2 = CsvAdapter::open_bytes_with_options(out, read_opts).unwrap();
    let sheet2 = adapter2.read_sheet("Sheet1").unwrap();

    assert_eq!(sheet2.dimensions, sheet.dimensions);
    assert_eq!(sheet_value_map(&sheet2), sheet_value_map(&sheet));
}

#[test]
fn csv_quotes_newlines() {
    // Note: embedded newline in a quoted field is allowed.
    let input = b"A,B\n\"hello, world\",\"line1\nline2\"\n\"he said \"\"hi\"\"\",x\n".to_vec();
    let read_opts = CsvReadOptions {
        has_headers: true,
        type_inference: CsvTypeInference::Off,
        ..CsvReadOptions::default()
    };
    let mut adapter = CsvAdapter::open_bytes_with_options(input, read_opts.clone()).unwrap();

    let sheet = adapter.read_sheet("Sheet1").unwrap();
    assert_eq!(sheet.dimensions, Some((3, 2)));
    assert_eq!(
        sheet.cells.get(&(2, 1)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Text("hello, world".to_string()))
    );
    assert_eq!(
        sheet.cells.get(&(2, 2)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Text("line1\nline2".to_string()))
    );
    assert_eq!(
        sheet.cells.get(&(3, 1)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Text("he said \"hi\"".to_string()))
    );

    let out = adapter
        .write_sheet_to(
            "Sheet1",
            SaveDestination::Bytes,
            CsvWriteOptions {
                newline: CsvNewline::Lf,
                ..CsvWriteOptions::default()
            },
        )
        .unwrap()
        .unwrap();

    // Ensure it re-reads exactly.
    let mut adapter2 = CsvAdapter::open_bytes_with_options(out, read_opts).unwrap();
    let sheet2 = adapter2.read_sheet("Sheet1").unwrap();
    assert_eq!(sheet_value_map(&sheet2), sheet_value_map(&sheet));
}

#[test]
fn csv_ragged_rows() {
    let input = b"a,b,c\n1,2\n3,4,5,6\n".to_vec();
    let read_opts = CsvReadOptions {
        has_headers: false,
        type_inference: CsvTypeInference::Off,
        ..CsvReadOptions::default()
    };
    let mut adapter = CsvAdapter::open_bytes_with_options(input, read_opts).unwrap();

    assert_eq!(adapter.sheet_bounds("Sheet1"), Some((3, 4)));

    // Missing cells are treated as empty and are not present in the sparse map.
    let empty = adapter.read_range("Sheet1", (2, 3), (2, 4)).unwrap();
    assert!(empty.is_empty());

    let out = adapter
        .write_sheet_to("Sheet1", SaveDestination::Bytes, CsvWriteOptions::default())
        .unwrap()
        .unwrap();
    let out_s = String::from_utf8(out).unwrap();

    // Row 2 has 4 columns, last two are empty -> trailing commas.
    assert!(out_s.contains("\n1,2,,\n"));
}

#[test]
fn csv_infer_types_off() {
    let input = b"1,true,3.14\n".to_vec();
    let read_opts = CsvReadOptions {
        type_inference: CsvTypeInference::Off,
        ..CsvReadOptions::default()
    };
    let mut adapter = CsvAdapter::open_bytes_with_options(input, read_opts).unwrap();
    let sheet = adapter.read_sheet("Sheet1").unwrap();
    assert_eq!(
        sheet.cells.get(&(1, 1)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Text("1".to_string()))
    );
    assert_eq!(
        sheet.cells.get(&(1, 2)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Text("true".to_string()))
    );
    assert_eq!(
        sheet.cells.get(&(1, 3)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Text("3.14".to_string()))
    );
}

#[test]
fn csv_rejects_non_utf8() {
    let bytes = vec![0xff, b',', b'1', b'\n'];
    let err = CsvAdapter::open_bytes_with_options(bytes, CsvReadOptions::default())
        .err()
        .unwrap();
    match err {
        formualizer_workbook::IoError::Backend { backend, .. } => {
            assert_eq!(backend, "csv");
        }
        other => panic!("expected backend error, got {other:?}"),
    }
}

#[test]
fn csv_array_export_policy_error_is_default_safe_behavior() {
    let mut adapter = CsvAdapter::new();
    adapter
        .write_cell(
            "Sheet1",
            1,
            1,
            formualizer_workbook::CellData {
                value: Some(LiteralValue::Array(vec![vec![LiteralValue::Int(1)]])),
                formula: None,
                style: None,
            },
        )
        .unwrap();

    let err = adapter
        .write_sheet_to("Sheet1", SaveDestination::Bytes, CsvWriteOptions::default())
        .expect_err("expected array export error");

    match err {
        formualizer_workbook::IoError::Backend { backend, .. } => assert_eq!(backend, "csv"),
        other => panic!("expected backend error, got {other:?}"),
    }
}

#[test]
fn csv_array_export_policy_top_left_exports_first_element() {
    let mut adapter = CsvAdapter::new();
    adapter
        .write_cell(
            "Sheet1",
            1,
            1,
            formualizer_workbook::CellData {
                value: Some(LiteralValue::Array(vec![
                    vec![LiteralValue::Int(1), LiteralValue::Int(2)],
                    vec![LiteralValue::Int(3), LiteralValue::Int(4)],
                ])),
                formula: None,
                style: None,
            },
        )
        .unwrap();

    let bytes = adapter
        .write_sheet_to(
            "Sheet1",
            SaveDestination::Bytes,
            CsvWriteOptions {
                array_policy: CsvArrayPolicy::TopLeft,
                ..CsvWriteOptions::default()
            },
        )
        .unwrap()
        .unwrap();
    let out = String::from_utf8(bytes).unwrap();
    assert!(out.starts_with("1"));
}

#[test]
fn csv_array_export_policy_blank_exports_empty_field() {
    let mut adapter = CsvAdapter::new();
    adapter
        .write_cell(
            "Sheet1",
            1,
            1,
            formualizer_workbook::CellData {
                value: Some(LiteralValue::Array(vec![vec![LiteralValue::Int(1)]])),
                formula: None,
                style: None,
            },
        )
        .unwrap();

    let bytes = adapter
        .write_sheet_to(
            "Sheet1",
            SaveDestination::Bytes,
            CsvWriteOptions {
                array_policy: CsvArrayPolicy::Blank,
                ..CsvWriteOptions::default()
            },
        )
        .unwrap()
        .unwrap();
    let out = String::from_utf8(bytes).unwrap();
    // CSV writers may or may not emit a trailing record terminator for an all-empty row.
    assert!(
        out.is_empty() || out == "\n" || out == "\r\n" || out == "\"\"\n" || out == "\"\"\r\n",
        "unexpected csv output: {out:?}"
    );
}
