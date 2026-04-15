use crate::common::build_workbook;
use formualizer_workbook::{
    LiteralValue, LoadStrategy, SpreadsheetReader, UmyaAdapter, Workbook, WorkbookConfig,
};

fn build_row_chain_workbook(rows: u32) -> std::path::PathBuf {
    build_workbook(|book| {
        let sheet = book.get_sheet_by_name_mut("Sheet1").unwrap();
        for row in 1..=rows {
            sheet.get_cell_mut((1, row)).set_value_number(row as f64);
            sheet
                .get_cell_mut((2, row))
                .set_formula(format!("=A{row}*2"));
            sheet
                .get_cell_mut((3, row))
                .set_formula(format!("=B{row}+1"));
            sheet
                .get_cell_mut((4, row))
                .set_formula(format!("=C{row}+A{row}"));
        }
    })
}

#[test]
fn umya_load_fast_path_survives_multi_batch_load_then_edit() {
    let rows = 6_000u32; // 18k formulas → crosses 10k ingest batches.
    let path = build_row_chain_workbook(rows);

    let backend = UmyaAdapter::open_path(&path).unwrap();
    let mut wb =
        Workbook::from_reader(backend, LoadStrategy::EagerAll, WorkbookConfig::ephemeral())
            .expect("load workbook");

    wb.evaluate_all().unwrap();
    assert_eq!(
        wb.get_value("Sheet1", rows, 4),
        Some(LiteralValue::Number(
            (rows as f64 * 2.0 + 1.0) + rows as f64
        ))
    );

    wb.set_value("Sheet1", rows, 1, LiteralValue::Number(10.0))
        .unwrap();
    assert_eq!(
        wb.evaluate_cell("Sheet1", rows, 4).unwrap(),
        LiteralValue::Number(31.0)
    );
}
