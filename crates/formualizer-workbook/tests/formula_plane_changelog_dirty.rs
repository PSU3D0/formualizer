use formualizer_common::LiteralValue;
use formualizer_workbook::{Workbook, WorkbookConfig};

fn formula_plane_workbook() -> Workbook {
    Workbook::new_with_config(WorkbookConfig::interactive().with_span_evaluation(true))
}

fn assert_number(actual: Option<LiteralValue>, expected: f64) {
    match actual {
        Some(LiteralValue::Number(n)) => {
            assert!((n - expected).abs() < 1e-9, "expected {expected}, got {n}")
        }
        Some(LiteralValue::Int(i)) => assert_eq!(i as f64, expected),
        other => panic!("expected numeric {expected}, got {other:?}"),
    }
}

#[test]
fn formula_plane_changelog_set_value_redirties_promoted_span() {
    let mut wb = formula_plane_workbook();
    wb.add_sheet("S").unwrap();

    const ROWS: u32 = 128;
    for row in 1..=ROWS {
        wb.set_value("S", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        wb.set_value("S", row, 2, LiteralValue::Number(2.0))
            .unwrap();
        wb.set_formula("S", row, 3, &format!("=A{row}*B{row}"))
            .unwrap();
    }
    wb.set_formula("S", 1, 4, &format!("=SUM(C1:C{ROWS})"))
        .unwrap();

    wb.evaluate_all().unwrap();
    assert_number(wb.get_value("S", 20, 3), 40.0);

    wb.set_value("S", 20, 1, LiteralValue::Number(1000.0))
        .unwrap();
    wb.evaluate_all().unwrap();

    assert_number(wb.get_value("S", 20, 3), 2000.0);
    let expected_sum = (ROWS as f64 * (ROWS as f64 + 1.0)) + (2000.0 - 40.0);
    assert_number(wb.get_value("S", 1, 4), expected_sum);
}

#[test]
fn formula_plane_changelog_set_formula_redirties_promoted_span_reads() {
    let mut wb = formula_plane_workbook();
    wb.add_sheet("S").unwrap();

    const ROWS: u32 = 128;
    for row in 1..=ROWS {
        wb.set_value("S", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        wb.set_value("S", row, 2, LiteralValue::Number(2.0))
            .unwrap();
        wb.set_formula("S", row, 3, &format!("=A{row}*B{row}"))
            .unwrap();
    }
    wb.set_value("S", 7, 4, LiteralValue::Number(10.0)).unwrap();
    wb.set_value("S", 7, 5, LiteralValue::Number(30.0)).unwrap();
    wb.set_formula("S", 7, 1, "=D7").unwrap();

    wb.evaluate_all().unwrap();
    assert_number(wb.get_value("S", 7, 1), 10.0);
    assert_number(wb.get_value("S", 7, 3), 20.0);

    wb.set_formula("S", 7, 1, "=E7").unwrap();
    wb.evaluate_all().unwrap();

    assert_number(wb.get_value("S", 7, 1), 30.0);
    assert_number(wb.get_value("S", 7, 3), 60.0);
}
