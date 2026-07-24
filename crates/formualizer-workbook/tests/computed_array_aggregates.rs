use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::FormulaPlaneMode;
use formualizer_workbook::{Workbook, WorkbookConfig};

fn build(mode: FormulaPlaneMode) -> Workbook {
    let config = WorkbookConfig::interactive().with_formula_plane_mode(mode);
    let mut workbook = Workbook::new_with_config(config);
    workbook.add_sheet("S").unwrap();
    for (row, value) in [(1, 10.0), (2, 20.0), (3, 10.0)] {
        workbook
            .set_value("S", row, 1, LiteralValue::Number(value))
            .unwrap();
    }
    let formulas = [
        "=SUM(SEQUENCE(3))",
        "=COUNT(UNIQUE(A1:A3))",
        "=COUNTA(SORT(A1:A3))",
        "=AVERAGE(TRANSPOSE(A1:A3))",
        "=MAX((A1:A3=10)*A1:A3)",
        "=SUM(OFFSET(A1,0,0,3,1))",
        "=SUM(INDIRECT(\"A1:A3\"))",
        "=SUM(SEQUENCE(-1))",
        "=SUM(OFFSET(A1,-1,0))",
        "=SUM(INDIRECT(\"not a reference\"))",
    ];
    for (row, formula) in formulas.into_iter().enumerate() {
        workbook
            .set_formula("S", row as u32 + 1, 2, formula)
            .unwrap();
    }
    workbook.evaluate_all().unwrap();
    workbook
}

#[test]
fn workbook_computed_array_aggregates_preserve_reference_errors_and_mode_parity() {
    let off = build(FormulaPlaneMode::Off);
    let authoritative = build(FormulaPlaneMode::AuthoritativeExperimental);

    for row in 1..=10 {
        assert_eq!(
            authoritative.get_value("S", row, 2),
            off.get_value("S", row, 2),
            "mode mismatch at B{row}"
        );
    }

    let expected = [6.0, 2.0, 3.0, 40.0 / 3.0, 10.0, 40.0, 40.0];
    for (row, expected) in expected.into_iter().enumerate() {
        assert_eq!(
            off.get_value("S", row as u32 + 1, 2),
            Some(LiteralValue::Number(expected))
        );
    }
    assert!(matches!(
        off.get_value("S", 8, 2),
        Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Value
    ));
    for (row, kind) in [(9, ExcelErrorKind::Ref), (10, ExcelErrorKind::Name)] {
        let Some(LiteralValue::Error(error)) = off.get_value("S", row, 2) else {
            panic!("expected an error value at B{row}");
        };
        assert_eq!(error.kind, kind, "error kind at B{row}");
        assert_eq!(error.message, None, "error message at B{row}");
    }
}
