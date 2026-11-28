use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;
use std::path::PathBuf;

fn main() {
    let path = PathBuf::from("/tmp/formualizer_cffi_smoke.xlsx");
    let mut wb = Workbook::new();
    wb.add_sheet("Sheet1").expect("add Sheet1");
    wb.set_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .expect("set A1");
    wb.set_formula("Sheet1", 1, 2, "=A1*2").expect("set B1");

    let bytes = wb.to_xlsx_bytes().expect("serialize smoke workbook");
    std::fs::write(&path, bytes).expect("write smoke workbook");
}
