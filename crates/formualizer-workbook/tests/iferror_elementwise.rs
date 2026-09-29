use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_workbook::{Workbook, WorkbookConfig};

fn workbook() -> Workbook {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();
    wb
}
fn number(wb: &Workbook, row: u32, col: u32, expected: f64) {
    let value = wb.get_value("S", row, col).unwrap();
    assert!(
        matches!(value, LiteralValue::Number(n) if n == expected)
            || matches!(value, LiteralValue::Int(n) if n as f64 == expected),
        "{value:?}"
    );
}

#[test]
fn iferror_elementwise_horizontal_and_matrix() {
    let mut wb = workbook();
    wb.set_formula("S", 1, 1, "=IFERROR({#DIV/0!,4},8)")
        .unwrap();
    wb.set_formula("S", 3, 1, "=IFERROR({#N/A,2;3,#VALUE!},{10,20;30,40})")
        .unwrap();
    wb.set_formula("S", 6, 1, "=IFERROR({1,4}/{0,1},8)")
        .unwrap();
    wb.evaluate_all().unwrap();
    number(&wb, 6, 1, 8.0);
    number(&wb, 6, 2, 4.0);
    number(&wb, 1, 1, 8.0);
    number(&wb, 1, 2, 4.0);
    for (r, c, n) in [(3, 1, 10.0), (3, 2, 2.0), (4, 1, 3.0), (4, 2, 40.0)] {
        number(&wb, r, c, n);
    }
    assert_eq!(wb.get_value("S", 1, 3), None);
}

#[test]
fn iferror_elementwise_range_and_fallback_range() {
    let mut wb = workbook();
    wb.set_value(
        "S",
        1,
        1,
        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref)),
    )
    .unwrap();
    wb.set_value("S", 2, 1, LiteralValue::Int(4)).unwrap();
    wb.set_value("S", 1, 2, LiteralValue::Int(8)).unwrap();
    wb.set_value("S", 2, 2, LiteralValue::Int(9)).unwrap();
    wb.set_formula("S", 1, 4, "=IFERROR(A1:A2,B1:B2)").unwrap();
    wb.evaluate_all().unwrap();
    number(&wb, 1, 4, 8.0);
    number(&wb, 2, 4, 4.0);
}

#[test]
fn iferror_elementwise_fallback_shapes_and_errors() {
    let mut wb = workbook();
    wb.set_formula("S", 1, 1, "=IFERROR({#N/A,2;#REF!,#NUM!},{8;9})")
        .unwrap();
    wb.set_formula("S", 4, 1, "=IFERROR(#DIV/0!,{5,6})")
        .unwrap();
    wb.set_formula("S", 6, 1, "=IFERROR({#DIV/0!,4},1/0)")
        .unwrap();
    wb.set_formula("S", 8, 1, "=IFERROR({#N/A,#N/A,#N/A},{8,9})")
        .unwrap();
    wb.evaluate_all().unwrap();
    for (r, c, n) in [
        (1, 1, 8.0),
        (1, 2, 2.0),
        (2, 1, 9.0),
        (2, 2, 9.0),
        (4, 1, 5.0),
        (4, 2, 6.0),
        (6, 2, 4.0),
        (8, 1, 8.0),
        (8, 2, 9.0),
    ] {
        number(&wb, r, c, n);
    }
    assert!(
        matches!(wb.get_value("S",6,1),Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Div)
    );
    assert!(
        matches!(wb.get_value("S",8,3),Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Na)
    );
}

#[test]
fn iferror_elementwise_clean_input_keeps_lazy_fallback() {
    let mut wb = workbook();
    wb.set_formula("S", 1, 1, "=IFERROR({1,4},SEQUENCE(1000000000))")
        .unwrap();
    wb.set_formula("S", 3, 1, "=IFERROR(7,1/0)").unwrap();
    wb.evaluate_all().unwrap();
    number(&wb, 1, 1, 1.0);
    number(&wb, 1, 2, 4.0);
    number(&wb, 3, 1, 7.0);
}

#[test]
fn iferror_elementwise_large_range_is_guarded_before_materialization() {
    let mut wb = workbook();
    wb.set_formula("S", 1, 1, "=IFERROR(B1:XFD1048576,8)")
        .unwrap();
    wb.evaluate_all().unwrap();
    assert!(
        matches!(wb.get_value("S",1,1),Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Num)
    );
}

#[test]
fn iferror_elementwise_respects_spill_blockers_and_cap() {
    let mut cfg = WorkbookConfig::interactive();
    cfg.eval.spill.max_spill_cells = 1;
    let mut wb = Workbook::new_with_config(cfg);
    wb.add_sheet("S").unwrap();
    wb.set_formula("S", 1, 1, "=IFERROR({#N/A,4},8)").unwrap();
    wb.evaluate_all().unwrap();
    assert!(
        matches!(wb.get_value("S",1,1),Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Spill)
    );
    let mut wb = workbook();
    wb.set_value("S", 1, 2, LiteralValue::Int(99)).unwrap();
    wb.set_formula("S", 1, 1, "=IFERROR({#N/A,4},8)").unwrap();
    wb.evaluate_all().unwrap();
    assert!(
        matches!(wb.get_value("S",1,1),Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Spill)
    );
    number(&wb, 1, 2, 99.0);
}
