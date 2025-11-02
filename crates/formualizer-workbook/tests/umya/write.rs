// Integration test for Umya backend; run with `--features umya`.

use formualizer_workbook::{
    CellData, LiteralValue, SpreadsheetReader, SpreadsheetWriter, UmyaAdapter,
};

#[test]
fn umya_write_and_read_cells() {
    // Start with new empty workbook by creating a temp file and loading then modifying.
    // For now, reuse build pattern: create an empty workbook via umya directly.
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("write_test.xlsx");
    let book = umya_spreadsheet::new_file();
    umya_spreadsheet::writer::xlsx::write(&book, &path).unwrap();

    let mut adapter = UmyaAdapter::open_path(&path).expect("open");
    // Write values
    adapter
        .write_cell("Data", 1, 1, CellData::from_value(10.0))
        .unwrap(); // auto creates sheet
    adapter
        .write_cell("Data", 1, 2, CellData::from_formula("=A1*2"))
        .unwrap();
    // Overwrite value
    adapter
        .write_cell("Data", 1, 1, CellData::from_value(11.0))
        .unwrap();

    // Read back entire sheet
    let sheet = adapter.read_sheet("Data").unwrap();
    assert_eq!(
        sheet.cells.get(&(1, 1)).unwrap().value,
        Some(LiteralValue::Number(11.0))
    );
    assert!(
        sheet
            .cells
            .get(&(1, 2))
            .unwrap()
            .formula
            .as_ref()
            .unwrap()
            .starts_with("=A1")
    );

    // Clear range
    adapter.clear_range("Data", (1, 1), (1, 1)).unwrap();
    let sheet2 = adapter.read_sheet("Data").unwrap();
    assert!(
        !sheet2.cells.contains_key(&(1, 1)),
        "Cell should be cleared"
    );

    // Rename sheet
    adapter.rename_sheet("Data", "Renamed").unwrap();
    let names = adapter.sheet_names().unwrap();
    assert!(names.iter().any(|n| n == "Renamed"));
}
