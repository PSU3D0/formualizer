//! Independent shape oracles exercised through parsed-formula dispatch.
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn n(v: i32) -> LiteralValue {
    LiteralValue::Number(v as f64)
}
fn err(kind: ExcelErrorKind) -> LiteralValue {
    LiteralValue::Error(ExcelError::new(kind))
}
fn matrix(rows: Vec<Vec<LiteralValue>>) -> LiteralValue {
    LiteralValue::Array(rows)
}
fn check(formula: &str, expected: LiteralValue) {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, col, value) in [(1, 1, 1), (2, 1, 2), (1, 2, 2)] {
        engine.set_cell_value("Sheet1", row, col, n(value)).unwrap();
    }
    let got = crate::interpreter::Interpreter::new(&engine, "Sheet1")
        .evaluate_ast(&parse(formula).unwrap())
        .unwrap()
        .into_literal();
    if let (LiteralValue::Error(actual), LiteralValue::Error(expected)) = (&got, &expected) {
        assert_eq!(actual.kind, expected.kind, "{formula}");
    } else {
        assert_eq!(got, expected, "{formula}");
    }
}

#[test]
fn stack_scalar_and_range_orders_pad_missing_cells() {
    let na = || err(ExcelErrorKind::Na);
    for formula in ["=HSTACK({1;2},3)", "=HSTACK(A1:A2,3)"] {
        check(formula, matrix(vec![vec![n(1), n(3)], vec![n(2), na()]]));
    }
    check(
        "=HSTACK(3,A1:A2)",
        matrix(vec![vec![n(3), n(1)], vec![na(), n(2)]]),
    );
    for formula in ["=VSTACK({1,2},3)", "=VSTACK(A1:B1,3)"] {
        check(formula, matrix(vec![vec![n(1), n(2)], vec![n(3), na()]]));
    }
    check(
        "=VSTACK(3,A1:B1)",
        matrix(vec![vec![n(3), na()], vec![n(1), n(2)]]),
    );
}

#[test]
fn stack_rectangular_shapes_and_embedded_errors() {
    let na = || err(ExcelErrorKind::Na);
    check(
        "=HSTACK({1,2;3,4},{5;6;7})",
        matrix(vec![
            vec![n(1), n(2), n(5)],
            vec![n(3), n(4), n(6)],
            vec![na(), na(), n(7)],
        ]),
    );
    check(
        "=VSTACK({1,2;3,4},{5,6,7})",
        matrix(vec![
            vec![n(1), n(2), na()],
            vec![n(3), n(4), na()],
            vec![n(5), n(6), n(7)],
        ]),
    );
    check(
        "=HSTACK({1;#DIV/0!},#VALUE!)",
        matrix(vec![
            vec![n(1), err(ExcelErrorKind::Value)],
            vec![err(ExcelErrorKind::Div), na()],
        ]),
    );
    check(
        "=VSTACK({1,#DIV/0!},#VALUE!)",
        matrix(vec![
            vec![n(1), err(ExcelErrorKind::Div)],
            vec![err(ExcelErrorKind::Value), na()],
        ]),
    );
}

#[test]
fn stack_iferror_replaces_padding_elementwise() {
    check(
        "=IFERROR(HSTACK({1;2},3),9)",
        matrix(vec![vec![n(1), n(3)], vec![n(2), n(9)]]),
    );
    check(
        "=IFERROR(VSTACK({1,2},3),9)",
        matrix(vec![vec![n(1), n(2)], vec![n(3), n(9)]]),
    );
    check("=HSTACK()", err(ExcelErrorKind::Value));
    check("=VSTACK()", err(ExcelErrorKind::Value));
}

#[test]
fn stack_padded_output_obeys_materialization_budget() {
    for formula in ["=HSTACK(A1:A3,B1:C1)", "=VSTACK(A1:C1,B1:B2)"] {
        for limit in [8, 9] {
            let mut config = EvalConfig::default();
            config.evaluation_budgets.scratch.materialized_array_cells = Some(limit);
            let mut engine = Engine::new(TestWorkbook::new(), config);
            engine
                .set_cell_formula("Sheet1", 10, 5, parse(formula).unwrap())
                .unwrap();
            engine.evaluate_all().unwrap();
            let got = engine.get_cell_value("Sheet1", 10, 5).unwrap();
            if limit == 8 {
                assert_eq!(got, err(ExcelErrorKind::Num), "{formula} at {limit}");
            } else {
                assert!(
                    !matches!(got, LiteralValue::Error(_)),
                    "{formula} at {limit}: {got:?}"
                );
                assert_eq!(
                    engine.get_cell_value("Sheet1", 12, 7),
                    Some(err(ExcelErrorKind::Na))
                );
            }
        }
    }
}
