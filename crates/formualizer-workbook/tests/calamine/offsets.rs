// Integration test for Calamine backend; run with `--features calamine,umya`.
use crate::common::build_workbook;
use formualizer_workbook::{CalamineAdapter, SpreadsheetReader}; // umya uses (col,row)

#[test]
fn calamine_handles_offset_ranges() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        // Leave first 4 rows & 3 cols empty; start at D5 (umya coordinate (col=4,row=5))
        sh.get_cell_mut((4, 5)).set_value_number(99); // D5
    });
    let mut backend = CalamineAdapter::open_path(&path).unwrap();
    let sheet = backend.read_sheet("Sheet1").unwrap();
    assert!(
        sheet.cells.contains_key(&(5, 4)),
        "Expected D5 => (row5,col4) present"
    );
    assert!(!sheet.cells.contains_key(&(1, 1)), "A1 should be absent");
}
