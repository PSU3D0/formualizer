//! FORM214: `A1#` and `_xlfn.ANCHORARRAY(A1)` through the Workbook API.

use formualizer_common::{LiteralValue, RangeAddress, error::ExcelErrorKind};
use formualizer_workbook::{Workbook, WorkbookConfig, traits::NamedRangeScope};

fn num(v: Option<LiteralValue>) -> Option<f64> {
    match v {
        Some(LiteralValue::Number(n)) => Some(n),
        Some(LiteralValue::Int(i)) => Some(i as f64),
        _ => None,
    }
}

fn is_ref_error(v: Option<LiteralValue>) -> bool {
    matches!(v, Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Ref)
}

fn workbook(parallel: bool) -> Workbook {
    let mut cfg = WorkbookConfig::interactive();
    cfg.eval.enable_parallel = parallel;
    let mut wb = Workbook::new_with_config(cfg);
    wb.add_sheet("S").unwrap();
    wb.add_sheet("Data").unwrap();
    wb
}

#[test]
fn both_spellings_evaluate_to_the_spill_range() {
    for parallel in [false, true] {
        let mut wb = workbook(parallel);
        wb.set_formula("S", 1, 1, "=SEQUENCE(2)").unwrap();
        wb.set_formula("S", 1, 2, "=SUM(A1#)").unwrap();
        wb.set_formula("S", 1, 3, "=SUM(_xlfn.ANCHORARRAY(A1))")
            .unwrap();
        wb.set_formula("S", 1, 4, "=ROWS(A1#)").unwrap();
        wb.set_formula("S", 1, 5, "=A1#*10").unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(num(wb.get_value("S", 1, 2)), Some(3.0), "p={parallel}");
        assert_eq!(num(wb.get_value("S", 1, 3)), Some(3.0), "p={parallel}");
        assert_eq!(num(wb.get_value("S", 1, 4)), Some(2.0), "p={parallel}");
        assert_eq!(num(wb.get_value("S", 1, 5)), Some(10.0));
        assert_eq!(num(wb.get_value("S", 2, 5)), Some(20.0));

        // Neither spelling is rewritten into the other (function names are
        // upper-cased by the existing formula printer).
        assert_eq!(wb.get_formula("S", 1, 2).as_deref(), Some("=SUM(A1#)"));
        let stored = wb.get_formula("S", 1, 3).unwrap();
        assert!(
            stored.eq_ignore_ascii_case("=SUM(_xlfn.ANCHORARRAY(A1))"),
            "{stored}"
        );

        // Growing the anchor with an unchanged top-left value updates readers.
        wb.set_formula("S", 1, 1, "=SEQUENCE(4)").unwrap();
        wb.evaluate_all().unwrap();
        assert_eq!(num(wb.get_value("S", 1, 2)), Some(10.0));
        assert_eq!(num(wb.get_value("S", 1, 3)), Some(10.0));
        assert_eq!(num(wb.get_value("S", 1, 4)), Some(4.0));
    }
}

#[test]
fn cross_sheet_and_single_cell_name() {
    let mut wb = workbook(false);
    wb.set_formula("Data", 2, 2, "=SEQUENCE(3)").unwrap();
    let addr = RangeAddress::new("Data", 2, 2, 2, 2).unwrap();
    wb.define_named_range("Anchor", &addr, NamedRangeScope::Workbook)
        .unwrap();
    wb.set_formula("S", 1, 1, "=SUM(Data!B2#)").unwrap();
    wb.set_formula("S", 1, 2, "=SUM(_xlfn.ANCHORARRAY(Data!B2))")
        .unwrap();
    wb.set_formula("S", 1, 3, "=SUM(Anchor#)").unwrap();
    wb.set_formula("S", 1, 4, "=SUM(_xlfn.ANCHORARRAY(Anchor))")
        .unwrap();
    wb.evaluate_all().unwrap();
    for col in 1..=4 {
        assert_eq!(num(wb.get_value("S", 1, col)), Some(6.0), "col {col}");
    }
}

#[test]
fn no_current_spill_is_ref_error() {
    let mut wb = workbook(false);
    wb.set_value("S", 1, 1, LiteralValue::Number(5.0)).unwrap();
    wb.set_formula("S", 1, 3, "=SEQUENCE(1)").unwrap();
    wb.set_formula("S", 2, 1, "=SUM(A1#)").unwrap();
    wb.set_formula("S", 2, 2, "=SUM(_xlfn.ANCHORARRAY(A1))")
        .unwrap();
    wb.set_formula("S", 2, 3, "=SUM(C1#)").unwrap();
    wb.set_formula("S", 2, 4, "=SUM(_xlfn.ANCHORARRAY(A1:A2))")
        .unwrap();
    wb.evaluate_all().unwrap();
    assert!(is_ref_error(wb.get_value("S", 2, 1)));
    assert!(is_ref_error(wb.get_value("S", 2, 2)));
    // A fresh 1x1 result is a scalar and has no spill range (deferred).
    assert!(is_ref_error(wb.get_value("S", 2, 3)));
    assert!(is_ref_error(wb.get_value("S", 2, 4)));
}
