use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;

fn assert_num(value: LiteralValue, expected: f64) {
    match value {
        LiteralValue::Number(n) => assert!((n - expected).abs() < 1e-9),
        LiteralValue::Int(i) => assert!(((i as f64) - expected).abs() < 1e-9),
        other => panic!("expected numeric {expected}, got {other:?}"),
    }
}

#[test]
fn workbook_subtotal_visibility_hide_unhide_updates_aggregate_results() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    wb.set_value("S", 2, 1, LiteralValue::Int(10)).unwrap();
    wb.set_value("S", 3, 1, LiteralValue::Int(20)).unwrap();
    wb.set_value("S", 4, 1, LiteralValue::Int(30)).unwrap();
    wb.set_value("S", 5, 1, LiteralValue::Int(100)).unwrap();

    wb.set_formula("S", 1, 2, "SUBTOTAL(109,A2:A5)").unwrap();
    wb.set_formula("S", 1, 3, "AGGREGATE(9,1,A2:A5)").unwrap();
    wb.set_formula("S", 1, 4, "AGGREGATE(9,0,A2:A5)").unwrap();

    assert_num(wb.evaluate_cell("S", 1, 2).unwrap(), 160.0);
    assert_num(wb.evaluate_cell("S", 1, 3).unwrap(), 160.0);
    assert_num(wb.evaluate_cell("S", 1, 4).unwrap(), 160.0);

    wb.set_row_hidden("S", 3, true).unwrap();
    assert_num(wb.evaluate_cell("S", 1, 2).unwrap(), 140.0);
    assert_num(wb.evaluate_cell("S", 1, 3).unwrap(), 140.0);
    assert_num(wb.evaluate_cell("S", 1, 4).unwrap(), 160.0);

    wb.set_row_hidden("S", 4, true).unwrap();
    assert_num(wb.evaluate_cell("S", 1, 2).unwrap(), 110.0);
    assert_num(wb.evaluate_cell("S", 1, 3).unwrap(), 110.0);
    assert_num(wb.evaluate_cell("S", 1, 4).unwrap(), 160.0);

    wb.set_row_hidden("S", 3, false).unwrap();
    assert_num(wb.evaluate_cell("S", 1, 2).unwrap(), 130.0);
    assert_num(wb.evaluate_cell("S", 1, 3).unwrap(), 130.0);

    wb.set_row_hidden("S", 4, false).unwrap();
    assert_num(wb.evaluate_cell("S", 1, 2).unwrap(), 160.0);
    assert_num(wb.evaluate_cell("S", 1, 3).unwrap(), 160.0);
}
