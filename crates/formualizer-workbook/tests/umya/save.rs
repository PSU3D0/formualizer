// Integration test for Umya backend; run with `--features umya`.
use formualizer_workbook::{CellData, SpreadsheetReader, SpreadsheetWriter, UmyaAdapter};

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
