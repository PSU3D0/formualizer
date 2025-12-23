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
