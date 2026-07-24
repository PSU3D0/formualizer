use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType, parse};

fn set_formula(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, formula: &str) {
    engine
        .set_cell_formula("Sheet1", row, col, parse(formula).expect("parse formula"))
        .expect("set formula");
}

fn set_value(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, value: LiteralValue) {
    engine
        .set_cell_value("Sheet1", row, col, value)
        .expect("set value");
}

fn assert_text(engine: &Engine<TestWorkbook>, row: u32, col: u32, expected: &str) {
    assert_eq!(
        engine.get_cell_value("Sheet1", row, col),
        Some(LiteralValue::Text(expected.into()))
    );
}

#[test]
fn concat_and_textjoin_expand_ranges_and_computed_arrays_in_formulas() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, col, text) in [(1, 1, "a"), (1, 2, "b"), (2, 1, "c"), (2, 2, "d")] {
        set_value(&mut engine, row, col, LiteralValue::Text(text.into()));
    }

    set_formula(&mut engine, 1, 4, "=CONCAT(A1:B2)");
    set_formula(&mut engine, 2, 4, "=TEXTJOIN(\"|\",TRUE,A1:B2)");
    set_formula(&mut engine, 3, 4, "=CONCAT(SEQUENCE(2,3))");
    set_formula(&mut engine, 4, 4, "=TEXTJOIN(\"-\",TRUE,SEQUENCE(2,2))");
    set_formula(&mut engine, 5, 4, "=CONCAT(OFFSET(A1,0,0,2,2))");
    set_formula(
        &mut engine,
        6,
        4,
        "=TEXTJOIN(\"|\",TRUE,INDIRECT(\"A1:B2\"))",
    );
    set_formula(&mut engine, 7, 4, "=CONCAT(CHOOSE(1,A1:B2,C1:C2))");
    set_formula(&mut engine, 8, 4, "=CONCAT(OFFSET(A1,-1,0))");
    set_formula(
        &mut engine,
        9,
        4,
        "=TEXTJOIN(\",\",TRUE,INDIRECT(\"not a reference\"))",
    );

    engine.evaluate_all().expect("evaluate formulas");

    assert_text(&engine, 1, 4, "abcd");
    assert_text(&engine, 2, 4, "a|b|c|d");
    assert_text(&engine, 3, 4, "123456");
    assert_text(&engine, 4, 4, "1-2-3-4");
    assert_text(&engine, 5, 4, "abcd");
    assert_text(&engine, 6, 4, "a|b|c|d");
    assert_text(&engine, 7, 4, "abcd");
    for (row, kind) in [(8, ExcelErrorKind::Ref), (9, ExcelErrorKind::Name)] {
        assert!(matches!(
            engine.get_cell_value("Sheet1", row, 4),
            Some(LiteralValue::Error(error)) if error.kind == kind && error.message.is_none()
        ));
    }
}

#[test]
fn concatenate_uses_top_left_of_arena_literal_and_computed_arrays() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (col, rows) in [
        (
            1,
            vec![
                vec![LiteralValue::Text("top".into()), LiteralValue::Int(2)],
                vec![LiteralValue::Int(3), LiteralValue::Int(4)],
            ],
        ),
        (2, Vec::new()),
    ] {
        let formula = ASTNode::new(
            ASTNodeType::Function {
                name: "CONCATENATE".into(),
                args: vec![
                    ASTNode::new(ASTNodeType::Literal(LiteralValue::Array(rows)), None),
                    ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("!".into())), None),
                ],
            },
            None,
        );
        engine
            .set_cell_formula("Sheet1", 1, col, formula)
            .expect("set arena literal formula");
    }
    set_formula(&mut engine, 1, 3, "=CONCATENATE(SEQUENCE(2,2),\"!\")");

    engine.evaluate_all().expect("evaluate formulas");

    assert_text(&engine, 1, 1, "top!");
    assert_text(&engine, 1, 2, "!");
    assert_text(&engine, 1, 3, "1!");
}

#[test]
fn textjoin_formula_range_blanks_obey_ignore_empty() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    set_value(&mut engine, 1, 1, LiteralValue::Text("a".into()));
    set_value(&mut engine, 1, 3, LiteralValue::Text(String::new()));
    set_value(&mut engine, 1, 4, LiteralValue::Text("d".into()));
    set_formula(&mut engine, 1, 6, "=TEXTJOIN(\"-\",TRUE,A1:D1)");
    set_formula(&mut engine, 2, 6, "=TEXTJOIN(\"-\",FALSE,A1:D1)");

    engine.evaluate_all().expect("evaluate formulas");

    assert_text(&engine, 1, 6, "a-d");
    assert_text(&engine, 2, 6, "a---d");
}

#[test]
fn expanded_formula_range_propagates_later_error_and_concatenate_stays_scalar() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    set_value(&mut engine, 1, 1, LiteralValue::Text("first".into()));
    set_formula(&mut engine, 1, 2, "=1/0");
    set_value(&mut engine, 1, 3, LiteralValue::Text("last".into()));
    set_formula(&mut engine, 1, 5, "=CONCAT(A1:C1)");
    set_formula(&mut engine, 2, 5, "=TEXTJOIN(\",\",TRUE,A1:C1)");
    set_formula(&mut engine, 3, 5, "=CONCATENATE(A1:C1,\"!\")");

    engine.evaluate_all().expect("evaluate formulas");

    for row in [1, 2] {
        match engine.get_cell_value("Sheet1", row, 5) {
            Some(LiteralValue::Error(error)) => assert_eq!(error.kind, ExcelErrorKind::Div),
            other => panic!("expected #DIV/0! at E{row}, got {other:?}"),
        }
    }
    assert_text(&engine, 3, 5, "first!");
}
