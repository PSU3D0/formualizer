// Integration test for native Excel tables; run with `--features umya`.

use crate::common::build_workbook;
use formualizer_common::LiteralValue;
use formualizer_workbook::{
    LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};

#[test]
fn umya_loads_native_table_metadata_and_eval_structured_ref() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();

        // Table region A1:B3: headers + 2 data rows.
        sh.get_cell_mut((1, 1)).set_value("Region");
        sh.get_cell_mut((2, 1)).set_value("Amount");
        sh.get_cell_mut((1, 2)).set_value("N");
        sh.get_cell_mut((2, 2)).set_value_number(10);
        sh.get_cell_mut((1, 3)).set_value("S");
        sh.get_cell_mut((2, 3)).set_value_number(20);

        // Formula in D1: SUM over the Amount column.
        sh.get_cell_mut((4, 1)).set_formula("SUM(Sales[Amount])");

        let mut table = umya_spreadsheet::structs::Table::new("Sales", ("A1", "B3"));
        table.add_column(umya_spreadsheet::structs::TableColumn::new("Region"));
        table.add_column(umya_spreadsheet::structs::TableColumn::new("Amount"));
        sh.add_table(table);
    });

    let backend = UmyaAdapter::open_path(&path).expect("open workbook");
    let mut wb = Workbook::from_reader(
        backend,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .expect("load into engine workbook");

    let v = wb.evaluate_cell("Sheet1", 1, 4).unwrap();
    assert_eq!(v, LiteralValue::Number(30.0));
}
