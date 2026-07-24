use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;

fn build(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    for (row, value) in [(1, 10.0), (2, 20.0), (3, 10.0)] {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(value))
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
        "=SUM(CHOOSE(1,5,6))",
        "=SUM(CHOOSE(1,SEQUENCE(3),SEQUENCE(2)))",
        "=SUM(INDEX(SEQUENCE(3),0))",
        "=SUM(SEQUENCE(-1))",
        "=SUM(OFFSET(A1,-1,0))",
        "=SUM(INDIRECT(\"not a reference\"))",
    ];
    for (row, formula) in formulas.into_iter().enumerate() {
        engine
            .set_cell_formula("Sheet1", row as u32 + 1, 2, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn arena_computed_array_aggregates_match_off_and_authoritative_modes() {
    let off = build(FormulaPlaneMode::Off);
    let authoritative = build(FormulaPlaneMode::AuthoritativeExperimental);

    for row in 1..=13 {
        assert_eq!(
            authoritative.get_cell_value("Sheet1", row, 2),
            off.get_cell_value("Sheet1", row, 2),
            "mode mismatch at B{row}"
        );
    }

    let expected = [
        LiteralValue::Number(6.0),
        LiteralValue::Number(2.0),
        LiteralValue::Number(3.0),
        LiteralValue::Number(40.0 / 3.0),
        LiteralValue::Number(10.0),
        LiteralValue::Number(40.0),
        LiteralValue::Number(40.0),
        LiteralValue::Number(5.0),
        LiteralValue::Number(6.0),
        LiteralValue::Number(6.0),
    ];
    for (row, expected) in expected.into_iter().enumerate() {
        assert_eq!(
            off.get_cell_value("Sheet1", row as u32 + 1, 2),
            Some(expected)
        );
    }
    assert!(matches!(
        off.get_cell_value("Sheet1", 11, 2),
        Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Value
    ));
    for (row, kind) in [(12, ExcelErrorKind::Ref), (13, ExcelErrorKind::Name)] {
        let Some(LiteralValue::Error(error)) = off.get_cell_value("Sheet1", row, 2) else {
            panic!("expected an error value at B{row}");
        };
        assert_eq!(error.kind, kind, "error kind at B{row}");
        assert_eq!(error.message, None, "error message at B{row}");
    }
}
