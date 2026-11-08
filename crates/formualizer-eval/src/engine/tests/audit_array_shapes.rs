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

#[test]
fn unique_columns_preserve_the_original_row_axis() {
    check(
        "=UNIQUE({1,1;2,2},TRUE)",
        matrix(vec![vec![n(1)], vec![n(2)]]),
    );
    check(
        "=UNIQUE({1,1;2,2},TRUE,FALSE)",
        matrix(vec![vec![n(1)], vec![n(2)]]),
    );
    check(
        "=UNIQUE({1,1;2,2;3,3},TRUE)",
        matrix(vec![vec![n(1)], vec![n(2)], vec![n(3)]]),
    );
    check(
        "=UNIQUE({1,2,1,3;10,20,10,30},TRUE)",
        matrix(vec![vec![n(1), n(2), n(3)], vec![n(10), n(20), n(30)]]),
    );
    // All columns survive, still in their original order.
    check(
        "=UNIQUE({3,1,2;30,10,20},TRUE,TRUE)",
        matrix(vec![vec![n(3), n(1), n(2)], vec![n(30), n(10), n(20)]]),
    );
}

#[test]
fn unique_columns_exactly_once_and_no_survivors() {
    check(
        "=UNIQUE({1,2,1,3;10,20,10,30},TRUE,TRUE)",
        matrix(vec![vec![n(2), n(3)], vec![n(20), n(30)]]),
    );
    // Preserve the existing empty-result representation.
    check("=UNIQUE({1,1;2,2},TRUE,TRUE)", matrix(vec![]));
    check("=UNIQUE({1;1},FALSE,TRUE)", matrix(vec![]));
}

#[test]
fn unique_columns_preserve_errors_and_by_row_controls() {
    check(
        "=UNIQUE({#N/A,#N/A,3;2,2,#DIV/0!},TRUE)",
        matrix(vec![
            vec![err(ExcelErrorKind::Na), n(3)],
            vec![n(2), err(ExcelErrorKind::Div)],
        ]),
    );
    check(
        "=UNIQUE({#N/A,#N/A,3;2,2,#DIV/0!},TRUE,TRUE)",
        matrix(vec![vec![n(3)], vec![err(ExcelErrorKind::Div)]]),
    );
    check(
        "=UNIQUE({1,1;2,2},FALSE)",
        matrix(vec![vec![n(1), n(1)], vec![n(2), n(2)]]),
    );
    check(
        "=UNIQUE({1,2;1,2;3,4},FALSE)",
        matrix(vec![vec![n(1), n(2)], vec![n(3), n(4)]]),
    );
    check(
        "=UNIQUE({1,2;1,2;3,4},FALSE,TRUE)",
        matrix(vec![vec![n(3), n(4)]]),
    );
    check("=UNIQUE()", err(ExcelErrorKind::Value));
}

#[test]
fn unique_columns_keep_existing_signed_zero_key_semantics() {
    // Distinct full-column keys retain their signed zero values. Columns that
    // differ only by zero's sign encounter a pre-existing Eq/Hash inconsistency
    // in LiteralValue; this orientation repair leaves that policy untouched.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(0.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(-0.0))
        .unwrap();
    engine.set_cell_value("Sheet1", 2, 1, n(1)).unwrap();
    engine.set_cell_value("Sheet1", 2, 2, n(2)).unwrap();
    engine.set_cell_value("Sheet1", 3, 1, n(3)).unwrap();
    engine.set_cell_value("Sheet1", 3, 2, n(4)).unwrap();
    let got = crate::interpreter::Interpreter::new(&engine, "Sheet1")
        .evaluate_ast(&parse("=UNIQUE(A1:B3,TRUE)").unwrap())
        .unwrap()
        .into_literal();
    assert_eq!(
        got,
        matrix(vec![vec![n(0), n(0)], vec![n(1), n(2)], vec![n(3), n(4)]])
    );
    let LiteralValue::Array(rows) = got else {
        panic!("expected array")
    };
    let (LiteralValue::Number(positive), LiteralValue::Number(negative)) =
        (&rows[0][0], &rows[0][1])
    else {
        panic!("expected zeros");
    };
    assert_eq!(positive.to_bits(), 0.0f64.to_bits());
    assert_eq!(negative.to_bits(), (-0.0f64).to_bits());
}

#[test]
fn unique_materialization_budget_is_preserved() {
    for by_col in ["TRUE", "FALSE"] {
        let formula = format!("=UNIQUE(A1:C2,{by_col})");
        let mut config = EvalConfig::default();
        config.evaluation_budgets.scratch.materialized_array_cells = Some(5);
        let mut engine = Engine::new(TestWorkbook::new(), config);
        engine
            .set_cell_formula("Sheet1", 10, 5, parse(&formula).unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        assert_eq!(
            engine.get_cell_value("Sheet1", 10, 5),
            Some(err(ExcelErrorKind::Num)),
            "{formula}"
        );
    }
}
