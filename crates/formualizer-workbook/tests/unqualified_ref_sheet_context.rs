//! Regression tests for issue #110: an unqualified reference inside a formula
//! must resolve against the *formula's* sheet, not the workbook default sheet.
//!
//! Setup mirrors the issue repro:
//!   Sheet1!A1 = 10, Sheet2!A1 = 99
//!   A formula on Sheet2 that references unqualified `A1` must read 99 (Sheet2),
//!   not 10 (Sheet1, the default sheet).

use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;

fn num(v: LiteralValue) -> f64 {
    match v {
        LiteralValue::Number(n) => n,
        LiteralValue::Int(i) => i as f64,
        other => panic!("expected numeric, got {other:?}"),
    }
}

/// Build a workbook where the default sheet (Sheet1) and a second sheet (Sheet2)
/// both have data at A1/A2/B-area, so an unqualified ref that leaks to the default
/// sheet produces an observably different value than the correct one.
fn fixture() -> Workbook {
    let mut wb = Workbook::new();
    // Sheet1 is the implicit default ("Sheet1").
    wb.add_sheet("Sheet1").ok(); // may already exist; ignore
    wb.add_sheet("Sheet2").unwrap();

    wb.set_value("Sheet1", 1, 1, LiteralValue::Int(10)).unwrap(); // Sheet1!A1
    wb.set_value("Sheet1", 2, 1, LiteralValue::Int(20)).unwrap(); // Sheet1!A2

    wb.set_value("Sheet2", 1, 1, LiteralValue::Int(99)).unwrap(); // Sheet2!A1
    wb.set_value("Sheet2", 2, 1, LiteralValue::Int(88)).unwrap(); // Sheet2!A2
    wb
}

/// Scalar unqualified reference `=A1` on Sheet2 must equal Sheet2!A1 (99).
#[test]
fn unqualified_scalar_ref_uses_formula_sheet() {
    let mut wb = fixture();
    wb.set_formula("Sheet2", 1, 2, "=A1").unwrap(); // Sheet2!B1
    let v = wb.evaluate_cell("Sheet2", 1, 2).unwrap();
    assert_eq!(
        num(v),
        99.0,
        "unqualified =A1 on Sheet2 should read Sheet2!A1"
    );
}

/// Unqualified range reference `=SUM(A1:A2)` on Sheet2 must sum Sheet2 (99+88=187),
/// not Sheet1 (10+20=30).
#[test]
fn unqualified_range_ref_uses_formula_sheet() {
    let mut wb = fixture();
    wb.set_formula("Sheet2", 1, 2, "=SUM(A1:A2)").unwrap(); // Sheet2!B1
    let v = wb.evaluate_cell("Sheet2", 1, 2).unwrap();
    assert_eq!(
        num(v),
        187.0,
        "unqualified =SUM(A1:A2) on Sheet2 should sum Sheet2 cells"
    );
}

/// Implicit intersection via the `@` operator on an unqualified ref must use the
/// formula's sheet. `=@A1:A2` evaluated on Sheet2 row 2 should pick Sheet2!A2 (88).
#[test]
fn unqualified_implicit_intersection_uses_formula_sheet() {
    let mut wb = fixture();
    // On row 2: implicit intersection of column A range picks the row-2 element.
    wb.set_formula("Sheet2", 2, 2, "=@A1:A2").unwrap(); // Sheet2!B2
    let v = wb.evaluate_cell("Sheet2", 2, 2).unwrap();
    assert_eq!(
        num(v),
        88.0,
        "implicit intersection on Sheet2 should pick Sheet2!A2"
    );
}

/// A SUMIF-style criteria function that materializes a range/cell through the
/// `resolve_range_like` path must also honor the formula's sheet. Here the
/// criteria range and sum range are both unqualified column-A references.
#[test]
fn unqualified_sumif_uses_formula_sheet() {
    let mut wb = fixture();
    // SUMIF(A1:A2, ">90") on Sheet2 -> only Sheet2!A1 (99) qualifies => 99.
    // If it leaked to Sheet1 (10,20) nothing qualifies => 0.
    wb.set_formula("Sheet2", 1, 2, "=SUMIF(A1:A2,\">90\")")
        .unwrap();
    let v = wb.evaluate_cell("Sheet2", 1, 2).unwrap();
    assert_eq!(
        num(v),
        99.0,
        "unqualified SUMIF on Sheet2 should evaluate against Sheet2 cells"
    );
}

/// Control: a *qualified* cross-sheet reference must be unchanged by the fix.
/// `=Sheet1!A1` on Sheet2 must still read Sheet1!A1 (10).
#[test]
fn qualified_cross_sheet_ref_unchanged() {
    let mut wb = fixture();
    wb.set_formula("Sheet2", 1, 3, "=Sheet1!A1").unwrap(); // Sheet2!C1
    let v = wb.evaluate_cell("Sheet2", 1, 3).unwrap();
    assert_eq!(num(v), 10.0, "qualified =Sheet1!A1 must read Sheet1");
}

/// Control: a qualified range reference across sheets is unchanged.
#[test]
fn qualified_cross_sheet_range_unchanged() {
    let mut wb = fixture();
    wb.set_formula("Sheet2", 1, 3, "=SUM(Sheet1!A1:A2)")
        .unwrap();
    let v = wb.evaluate_cell("Sheet2", 1, 3).unwrap();
    assert_eq!(num(v), 30.0, "qualified =SUM(Sheet1!A1:A2) must sum Sheet1");
}

/// Direct trait-level regression for the *latent* bug in
/// `ReferenceResolver::resolve_cell_reference` / `Resolver::resolve_range_like`.
///
/// These trait methods carry no current-sheet context. The buggy implementation
/// silently fell back to the workbook *default* sheet for an unqualified
/// (`sheet == None`) reference, which can read a totally unrelated cell. After
/// the fix, an unqualified reference reaching this context-free path must NOT be
/// silently mapped to the default sheet (it returns a #REF! error instead),
/// while a *qualified* reference keeps working.
#[test]
fn resolver_trait_does_not_leak_unqualified_ref_to_default_sheet() {
    use formualizer_common::error::ExcelErrorKind;
    use formualizer_eval::traits::{ReferenceResolver, Resolver};
    use formualizer_parse::parser::ReferenceType;

    let wb = fixture(); // default sheet is Sheet1; Sheet1!A1 = 10, Sheet2!A1 = 99
    let engine = wb.engine();

    // Qualified ref via the context-free trait still works.
    let qualified = engine
        .resolve_cell_reference(Some("Sheet2"), 1, 1)
        .expect("qualified resolve should succeed");
    assert_eq!(num(qualified), 99.0, "qualified Sheet2!A1 should be 99");

    // Unqualified ref must NOT silently read Sheet1!A1 (10). With no sheet
    // context available at this layer, the only correct answers are an error
    // or Empty -- crucially never the default-sheet value.
    let unqualified = engine.resolve_cell_reference(None, 1, 1);
    match unqualified {
        Err(e) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        Ok(LiteralValue::Number(n)) => {
            assert_ne!(n, 10.0, "unqualified ref leaked to default Sheet1!A1")
        }
        Ok(LiteralValue::Int(i)) => {
            assert_ne!(i, 10, "unqualified ref leaked to default Sheet1!A1")
        }
        Ok(other) => panic!("unexpected unqualified result: {other:?}"),
    }

    // Same expectation through the generic `resolve_range_like` cell branch.
    let cell_ref = ReferenceType::Cell {
        sheet: None,
        row: 1,
        col: 1,
        row_abs: true,
        col_abs: true,
    };
    let via_range_like = engine.resolve_range_like(&cell_ref);
    match via_range_like {
        Err(e) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        Ok(range) => {
            let v = range.get(0, 0).unwrap_or(LiteralValue::Empty);
            match v {
                LiteralValue::Number(n) => {
                    assert_ne!(n, 10.0, "resolve_range_like leaked to default Sheet1!A1")
                }
                LiteralValue::Int(i) => {
                    assert_ne!(i, 10, "resolve_range_like leaked to default Sheet1!A1")
                }
                LiteralValue::Error(_) | LiteralValue::Empty => {}
                other => panic!("unexpected resolve_range_like result: {other:?}"),
            }
        }
    }
}
