use formualizer_workbook::{
    CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

// LibreOffice 24.2.7.2, isolated synthetic XLSX oracle: a blank criterion
// excludes numbers/booleans; interior and trailing absent cells are blanks.
#[test]
fn criteria_blanks_agree_for_imported_constructed_and_edited_workbooks() {
    let mut source = umya_spreadsheet::new_file();
    let sheet = source.get_sheet_by_name_mut("Sheet1").unwrap();
    sheet.get_cell_mut("A1").set_value_number(1);
    sheet.get_cell_mut("A2").set_value_bool(true);
    sheet.get_cell_mut("A4").set_value_string("x");
    for r in 1..=6 {
        sheet.get_cell_mut((2, r)).set_value_number(10);
    }
    let formulas = [
        (r#"COUNTIF(A1:A6,"")"#, 3.0),
        (r#"COUNTIF(A1:A6,"<>")"#, 3.0),
        (r#"COUNTIFS(A1:A6,"",B1:B6,">0")"#, 3.0),
        (r#"SUMIF(A1:A6,"",B1:B6)"#, 30.0),
        (r#"SUMIFS(B1:B6,A1:A6,"")"#, 30.0),
        (r#"COUNTIF(D1:D6,"")"#, 6.0),
        (r#"COUNTIF(A1:A20,"")"#, 17.0),
        (r#"COUNTBLANK(A1:A20)"#, 17.0),
        (r#"COUNTIF(A15:A20,"")"#, 6.0),
        (r#"COUNTBLANK(A15:A20)"#, 6.0),
        (r#"COUNTIF(A1:A1048576,"")"#, 1_048_573.0),
        (r#"COUNTBLANK(A1:A1048576)"#, 1_048_573.0),
        (r#"COUNTIF(A15:A20,"<>")"#, 0.0),
        (r#"COUNTIF(A:A,"")"#, 1_048_573.0),
        (r#"COUNTBLANK(A:A)"#, 1_048_573.0),
    ];
    for (i, (f, _)) in formulas.iter().enumerate() {
        sheet.get_cell_mut((3, i as u32 + 1)).set_formula(*f);
    }
    let mut bytes = Vec::new();
    umya_spreadsheet::writer::xlsx::write_writer(&source, &mut bytes).unwrap();
    let reader = <CalamineAdapter as SpreadsheetReader>::open_bytes(bytes).unwrap();
    let loaded = Workbook::from_reader(
        reader,
        LoadStrategy::EagerAll,
        WorkbookConfig::interactive(),
    )
    .unwrap();
    let mut built = Workbook::new_with_config(WorkbookConfig::ephemeral());
    built.add_sheet("Sheet1").unwrap();
    for (r, v) in [
        (1, LiteralValue::Number(1.0)),
        (2, LiteralValue::Boolean(true)),
        (4, LiteralValue::Text("x".into())),
    ] {
        built.set_value("Sheet1", r, 1, v).unwrap();
    }
    for r in 1..=6 {
        built
            .set_value("Sheet1", r, 2, LiteralValue::Number(10.0))
            .unwrap();
    }
    for (i, (f, _)) in formulas.iter().enumerate() {
        built.set_formula("Sheet1", i as u32 + 1, 3, f).unwrap();
    }
    for mut wb in [loaded, built] {
        for _ in 0..2 {
            wb.evaluate_all().unwrap();
            for (i, (_, expected)) in formulas.iter().enumerate() {
                assert_eq!(
                    wb.get_value("Sheet1", i as u32 + 1, 3),
                    Some(LiteralValue::Number(*expected)),
                    "formula {}",
                    formulas[i].0
                );
            }
        }
        wb.set_value("Sheet1", 1, 1, LiteralValue::Empty).unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(
            wb.get_value("Sheet1", 1, 3),
            Some(LiteralValue::Number(4.0))
        );
        assert_eq!(
            wb.get_value("Sheet1", 2, 3),
            Some(LiteralValue::Number(2.0))
        );
    }
}
