use formualizer_common::{LiteralValue, RangeAddress, error::ExcelErrorKind};
use formualizer_workbook::{Workbook, traits::NamedRangeScope};

#[test]
fn workbook_named_range_crud() {
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet1").unwrap();

    workbook
        .set_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .expect("set seed value");

    let addr = RangeAddress::new("Sheet1", 1, 1, 1, 1).expect("address");
    workbook
        .define_named_range("Input", &addr, NamedRangeScope::Workbook)
        .expect("define named range");

    workbook
        .set_formula("Sheet1", 1, 2, "=Input*2")
        .expect("set formula");

    let initial = workbook.evaluate_cell("Sheet1", 1, 2).expect("evaluate");
    assert!(
        matches!(initial, LiteralValue::Number(n) if (n - 20.0).abs() < 1e-9),
        "expected 20, got {initial:?}"
    );

    workbook
        .set_value("Sheet1", 2, 1, LiteralValue::Number(25.0))
        .expect("set new input");
    let addr2 = RangeAddress::new("Sheet1", 2, 1, 2, 1).expect("address");
    workbook
        .update_named_range("Input", &addr2, NamedRangeScope::Workbook)
        .expect("update named range");

    let updated = workbook
        .evaluate_cell("Sheet1", 1, 2)
        .expect("evaluate updated");
    assert!(
        matches!(updated, LiteralValue::Number(n) if (n - 50.0).abs() < 1e-9),
        "expected 50, got {updated:?}"
    );

    workbook
        .delete_named_range("Input", NamedRangeScope::Workbook, None)
        .expect("delete named range");

    let missing = workbook
        .evaluate_cell("Sheet1", 1, 2)
        .expect("evaluate after delete");
    match missing {
        LiteralValue::Error(err) => assert_eq!(err.kind, ExcelErrorKind::Name),
        other => panic!("expected NAME error, got {other:?}"),
    }
}

/// Defined names used as `:` endpoints parse, but the evaluator does not
/// resolve them to bounds yet. They must surface `#N/IMPL!` (not `#REF!`) so
/// callers can refuse the result instead of publishing a wrong value.
#[test]
fn defined_name_range_endpoints_are_not_implemented() {
    let mut workbook = Workbook::new();
    workbook.add_sheet("Sheet1").unwrap();
    for row in 1..=10 {
        workbook
            .set_value("Sheet1", row, 2, LiteralValue::Number(row as f64))
            .unwrap();
    }
    for (name, row) in [("TopCell", 1), ("EndofRange", 5)] {
        let addr = RangeAddress::new("Sheet1", row, 2, row, 2).unwrap();
        workbook
            .define_named_range(name, &addr, NamedRangeScope::Workbook)
            .unwrap();
    }
    let cases = [
        (1, "=SUM(Sheet1!$B$2:EndofRange)"),
        (2, "=SUM(TopCell:B5)"),
        (3, "=SUM(TopCell:EndofRange)"),
        (4, "=SUM(TopCell:OFFSET(TopCell,4,0))"),
        (5, "=SUM(TopCell:INDEX(B:B,5))"),
    ];
    for (row, formula) in cases {
        workbook.set_formula("Sheet1", row, 4, formula).unwrap();
    }
    // Controls: a computed endpoint is supported; a deleted endpoint is #REF!.
    workbook
        .set_formula("Sheet1", 6, 4, "=SUM(B1:INDEX(B:B,5))")
        .unwrap();
    workbook
        .set_formula("Sheet1", 7, 4, "=SUM(B1:#REF!)")
        .unwrap();
    workbook.evaluate_all().unwrap();
    for (row, formula) in cases {
        match workbook.get_value("Sheet1", row, 4) {
            Some(LiteralValue::Error(err)) => {
                assert_eq!(err.kind, ExcelErrorKind::NImpl, "{formula}: {err:?}")
            }
            other => panic!("{formula}: expected #N/IMPL!, got {other:?}"),
        }
        match workbook.evaluate_cell("Sheet1", row, 4).unwrap() {
            LiteralValue::Error(err) => assert_eq!(err.kind, ExcelErrorKind::NImpl, "{formula}"),
            other => panic!("{formula}: expected #N/IMPL!, got {other:?}"),
        }
    }
    assert_eq!(
        workbook.get_value("Sheet1", 6, 4),
        Some(LiteralValue::Number(15.0))
    );
    match workbook.get_value("Sheet1", 7, 4) {
        Some(LiteralValue::Error(err)) => assert_eq!(err.kind, ExcelErrorKind::Ref),
        other => panic!("expected #REF!, got {other:?}"),
    }
}
