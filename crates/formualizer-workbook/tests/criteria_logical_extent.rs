use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};

#[test]
fn blank_counts_keep_wide_logical_extents_arithmetic() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
    wb.add_sheet("Data").unwrap();
    wb.add_sheet("Results").unwrap();
    wb.set_value("Data", 5, 3, LiteralValue::Number(1.0))
        .unwrap();
    let cases = [
        (r#"COUNTBLANK(Data!A:XFD)"#, 17_179_869_183.0),
        (r#"COUNTBLANK(Data!1:1048576)"#, 17_179_869_183.0),
        (r#"COUNTIF(Data!A:XFD,"")"#, 17_179_869_183.0),
        (r#"COUNTIF(Data!1:1048576,"")"#, 17_179_869_183.0),
        (r#"COUNTBLANK(Data!C:C)"#, 1_048_575.0),
        (r#"COUNTIF(Data!C:C,"")"#, 1_048_575.0),
        (r#"COUNTBLANK(Data!5:5)"#, 16_383.0),
        (r#"COUNTIF(Data!5:5,"")"#, 16_383.0),
    ];
    for (row, (formula, _)) in cases.iter().enumerate() {
        wb.set_formula("Results", row as u32 + 1, 1, formula)
            .unwrap();
    }
    wb.evaluate_all().unwrap();
    for (row, (formula, expected)) in cases.iter().enumerate() {
        assert_eq!(
            wb.get_value("Results", row as u32 + 1, 1),
            Some(LiteralValue::Number(*expected)),
            "{formula}"
        );
    }
}
