// Integration test for Umya backend; run with `--features umya`.
use crate::common::build_workbook;
use formualizer_workbook::LiteralValue;
use formualizer_workbook::{CellData, SpreadsheetReader, SpreadsheetWriter, UmyaAdapter};
use std::io::Cursor;

#[test]
fn umya_save_in_place_and_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("save_test.xlsx");
    let book = umya_spreadsheet::new_file();
    umya_spreadsheet::writer::xlsx::write(&book, &path).unwrap();

    let mut adapter = UmyaAdapter::open_path(&path).unwrap();
    adapter
        .write_cell("Sheet1", 1, 1, CellData::from_value(123.0))
        .unwrap();
    // In place save
    adapter.save().unwrap();
    // Re-open and verify persists
    let mut adapter2 = UmyaAdapter::open_path(&path).unwrap();
    let sheet = adapter2.read_sheet("Sheet1").unwrap();
    assert!(sheet.cells.contains_key(&(1, 1)));

    // Bytes save
    adapter2
        .write_cell("Sheet1", 2, 1, CellData::from_value(456.0))
        .unwrap();
    let bytes = adapter2.save_to_bytes().unwrap();
    assert!(bytes.len() > 100, "Expected non-trivial XLSX byte output");
}

#[test]
fn umya_open_bytes_and_reader_load_cells_and_formulas() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(10); // A1
        sh.get_cell_mut((2, 1)).set_formula("A1*2"); // B1
    });

    let bytes = std::fs::read(&path).unwrap();

    let mut from_bytes = UmyaAdapter::open_bytes(bytes.clone()).unwrap();
    let sheet = from_bytes.read_sheet("Sheet1").unwrap();
    assert_eq!(
        sheet.cells.get(&(1, 1)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        sheet.cells.get(&(1, 2)).and_then(|c| c.formula.as_deref()),
        Some("=A1*2")
    );

    let mut from_reader = UmyaAdapter::open_reader(Box::new(Cursor::new(bytes))).unwrap();
    let sheet2 = from_reader.read_sheet("Sheet1").unwrap();
    assert_eq!(
        sheet2.cells.get(&(1, 1)).and_then(|c| c.value.clone()),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        sheet2.cells.get(&(1, 2)).and_then(|c| c.formula.as_deref()),
        Some("=A1*2")
    );
}

#[test]
fn umya_open_bytes_supports_save_to_bytes_but_not_save_in_place() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.get_cell_mut((1, 1)).set_value_number(1);
    });
    let bytes = std::fs::read(&path).unwrap();

    let mut adapter = UmyaAdapter::open_bytes(bytes).unwrap();
    adapter
        .write_cell("Sheet1", 2, 1, CellData::from_value(999.0))
        .unwrap();

    let err = adapter
        .save()
        .expect_err("open_bytes adapters must not support in-place save");
    let msg = err.to_string();
    assert!(msg.contains("no original path"), "unexpected error: {msg}");

    let out = adapter.save_to_bytes().unwrap();
    assert!(out.len() > 100, "Expected non-trivial XLSX byte output");
}

#[test]
fn umya_open_bytes_returns_parse_error_for_invalid_payload() {
    let err = match UmyaAdapter::open_bytes(vec![0x01, 0x02, 0x03, 0x04]) {
        Ok(_) => panic!("expected parse failure"),
        Err(err) => err,
    };
    assert!(
        !err.to_string().to_ascii_lowercase().contains("unsupported"),
        "open_bytes should attempt parsing, got unsupported error: {err}"
    );
}
