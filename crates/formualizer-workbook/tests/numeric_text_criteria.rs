use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};

#[test]
fn numeric_text_criteria_include_numbers_and_preserve_text_matches() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
    wb.add_sheet("Data").unwrap();
    wb.add_sheet("Results").unwrap();
    for (index, value) in [
        LiteralValue::Number(3200.0),
        LiteralValue::Number(3200.0),
        LiteralValue::Text("3200".into()),
        LiteralValue::Text("other".into()),
        LiteralValue::Number(10.25),
    ]
    .into_iter()
    .enumerate()
    {
        wb.set_value("Data", index as u32 + 1, 1, value).unwrap();
        wb.set_value(
            "Data",
            index as u32 + 1,
            2,
            LiteralValue::Number((index + 1) as f64 * 10.0),
        )
        .unwrap();
    }
    let cases = [
        (r#"COUNTIF(Data!A1:A5,"3200")"#, 3.0),
        (r#"COUNTIF(Data!A:A,"3200")"#, 3.0),
        (r#"COUNTIF(Data!A1:A5,"10.25")"#, 1.0),
        (r#"COUNTIFS(Data!A1:A5,"3200")"#, 3.0),
        (r#"SUMIF(Data!A1:A5,"3200",Data!B1:B5)"#, 60.0),
        (r#"SUMIFS(Data!B1:B5,Data!A1:A5,"3200")"#, 60.0),
        (r#"COUNTIF(Data!A1:A5,"other")"#, 1.0),
    ];
    for (index, (formula, _)) in cases.iter().enumerate() {
        wb.set_formula("Results", index as u32 + 1, 1, formula)
            .unwrap();
    }
    wb.evaluate_all().unwrap();
    for (index, (formula, expected)) in cases.iter().enumerate() {
        assert_eq!(
            wb.get_value("Results", index as u32 + 1, 1),
            Some(LiteralValue::Number(*expected)),
            "{formula}"
        );
    }
    wb.set_value("Data", 1, 1, LiteralValue::Number(999.0))
        .unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(
        wb.get_value("Results", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        wb.get_value("Results", 5, 1),
        Some(LiteralValue::Number(50.0))
    );
}
