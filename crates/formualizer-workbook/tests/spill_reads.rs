use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_workbook::Workbook;

#[test]
fn workbook_read_range_sees_spilled_values() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    wb.set_formula("S", 1, 1, "{1,2;3,4}").unwrap();
    let _ = wb.evaluate_cell("S", 1, 1).unwrap();

    // Direct cell reads
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(1.0)));
    assert_eq!(wb.get_value("S", 1, 2), Some(LiteralValue::Number(2.0)));
    assert_eq!(wb.get_value("S", 2, 1), Some(LiteralValue::Number(3.0)));
    assert_eq!(wb.get_value("S", 2, 2), Some(LiteralValue::Number(4.0)));

    // Arrow-backed range reads (Workbook::read_range)
    let ra = RangeAddress::new("S", 1, 1, 2, 2).unwrap();
    let vals = wb.read_range(&ra);
    assert_eq!(
        vals,
        vec![
            vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.0), LiteralValue::Number(4.0)],
        ]
    );
}
