use formualizer_workbook::{
    CalamineAdapter, LiteralValue, LoadStrategy, SpreadsheetReader, Workbook, WorkbookConfig,
};

// Compatibility oracle: the engine's established scalar wildcard contract,
// including numeric/boolean rendering and Empty matching '*'. This intentionally
// does not assert Excel parity (and LibreOffice excludes truly absent cells).
#[test]
fn wildcard_import_and_overlay_follow_existing_scalar_contract() {
    let mut source = umya_spreadsheet::new_file();
    let sheet = source.get_sheet_by_name_mut("Sheet1").unwrap();
    for r in 1..=3 {
        sheet.get_cell_mut((1, r)).set_value_number(r);
    }
    sheet.get_cell_mut("A4").set_value_string("1");
    sheet.get_cell_mut("A5").set_value_bool(true);
    for r in 1..=6 {
        sheet.get_cell_mut((2, r)).set_value_number(10);
    }
    let formulas = [
        (r#"COUNTIF(A1:A6,"*")"#, 6.0),
        (r#"COUNTIF(A1:A6,"1*")"#, 2.0),
        (r#"COUNTIF(A1:A6,"?")"#, 4.0),
        (r#"COUNTIFS(A1:A6,"*",B1:B6,">0")"#, 6.0),
        (r#"SUMIF(A1:A6,"*",B1:B6)"#, 60.0),
        (r#"SUMIFS(B1:B6,A1:A6,"*")"#, 60.0),
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
    for (i, v) in [
        LiteralValue::Number(1.0),
        LiteralValue::Number(2.0),
        LiteralValue::Number(3.0),
        LiteralValue::Text("1".into()),
        LiteralValue::Boolean(true),
    ]
    .into_iter()
    .enumerate()
    {
        built.set_value("Sheet1", i as u32 + 1, 1, v).unwrap();
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
            for (i, (f, expected)) in formulas.iter().enumerate() {
                assert_eq!(
                    wb.get_value("Sheet1", i as u32 + 1, 3),
                    Some(LiteralValue::Number(*expected)),
                    "{f}"
                );
            }
        }
        wb.set_value("Sheet1", 1, 1, LiteralValue::Empty).unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(
            wb.get_value("Sheet1", 1, 3),
            Some(LiteralValue::Number(6.0))
        );
        assert_eq!(
            wb.get_value("Sheet1", 2, 3),
            Some(LiteralValue::Number(1.0))
        );
    }
}
