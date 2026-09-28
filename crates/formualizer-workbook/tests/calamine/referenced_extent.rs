//! Cells family members reference at load count toward the used extent of
//! an otherwise empty column, as the legacy placeholders did (decision 27
//! keeps them in the graph's extent record, not as vertices).
use crate::common::build_workbook;
use formualizer_workbook::{
    CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

#[test]
fn load_time_family_references_bound_an_empty_columns_extent() {
    let path = build_workbook(|book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
        sh.set_name("Data");
        for r in 1..=40u32 {
            sh.get_cell_mut((3, r)).set_value_number(f64::from(r));
            // A family reading column Z far below anything in it.
            sh.get_cell_mut((2, r))
                .set_formula(format!("Z{}+1", r + 3000));
        }
        sh.get_cell_mut((5, 1)).set_formula("ROWS(Z:Z*1)");
        sh.get_cell_mut((5, 2))
            .set_formula("SUMPRODUCT(--(Z1:Z=\"\"))");
    });
    for interactive in [false, true] {
        let config = if interactive {
            WorkbookConfig::interactive()
        } else {
            WorkbookConfig::ephemeral()
        };
        let adapter = CalamineAdapter::open_path(&path).unwrap();
        let mut wb = Workbook::from_reader(adapter, LoadStrategy::EagerAll, config).unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(
            wb.get_value("Data", 1, 5),
            Some(LiteralValue::Number(3040.0))
        );
        assert_eq!(
            wb.get_value("Data", 2, 5),
            Some(LiteralValue::Number(3040.0))
        );
        // Rows inserted above the referenced cells move them.
        wb.engine_mut().insert_rows("Data", 100, 2).unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(
            wb.get_value("Data", 1, 5),
            Some(LiteralValue::Number(3042.0)),
            "interactive {interactive}"
        );
    }
}
