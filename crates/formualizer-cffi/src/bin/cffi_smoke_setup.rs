use std::path::PathBuf;

fn main() {
    let path = PathBuf::from("/tmp/formualizer_cffi_smoke.xlsx");
    let mut book = umya_spreadsheet::new_file();
    let ws = book.get_sheet_by_name_mut("Sheet1").expect("default sheet");

    ws.get_cell_mut((1, 1)).set_value_number(10);
    ws.get_cell_mut((2, 1)).set_formula("A1*2");

    umya_spreadsheet::writer::xlsx::write(&book, &path).expect("write smoke workbook");
}
