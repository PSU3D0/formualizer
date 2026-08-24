use crate::engine::{CycleConfig, CycleDetection, CyclePolicy, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use chrono::NaiveDate;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn runtime_engine() -> Engine<TestWorkbook> {
    let cycle = CycleConfig {
        detection: CycleDetection::Runtime,
        policy: CyclePolicy::Error,
    };
    Engine::new(
        TestWorkbook::new(),
        EvalConfig::default()
            .with_cycle(cycle)
            .with_virtual_dep_telemetry(true),
    )
}

fn set_formula(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, formula: &str) {
    engine
        .set_cell_formula("Sheet1", row, col, parse(formula).expect("parse formula"))
        .expect("set formula");
}

fn is_circ(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> bool {
    matches!(
        engine.get_cell_value("Sheet1", row, col),
        Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Circ
    )
}

fn build_guarded_chain(consumer_formula: &str) -> Engine<TestWorkbook> {
    let mut engine = runtime_engine();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Int(i64::from(row)))
            .expect("set row number");
        set_formula(&mut engine, row, 5, &format!("=IF(B{row}>=29,$C$9,0)"));
        if row == 1 {
            set_formula(&mut engine, row, 17, "=E1");
        } else {
            set_formula(&mut engine, row, 17, &format!("=Q{}+E{row}", row - 1));
        }
    }
    set_formula(&mut engine, 9, 3, consumer_formula);
    engine
}

#[test]
fn index_rect_edge_precision_acyclic_chain() {
    let mut engine = build_guarded_chain("=INDEX(Q1:Q100,24)");
    engine.evaluate_all().expect("evaluate");

    let c9 = engine.get_cell_value("Sheet1", 9, 3);
    let c9_is_zero = matches!(c9, Some(LiteralValue::Number(0.0) | LiteralValue::Int(0)));
    let circ_count = (1..=100)
        .flat_map(|row| (1..=17).map(move |col| (row, col)))
        .filter(|&(row, col)| is_circ(&engine, row, col))
        .count();
    assert!(
        c9_is_zero && circ_count == 0,
        "C9 expected numeric zero, got {c9:?}; expected zero Circ cells, got {circ_count}"
    );
}

#[test]
fn index_rect_edge_precision_if_selected_base() {
    let mut engine = build_guarded_chain("=INDEX(IF(B1=1,Q1:Q100,A1:A100),24,1)");
    engine.evaluate_all().expect("evaluate");

    let c9 = engine.get_cell_value("Sheet1", 9, 3);
    let c9_is_zero = matches!(c9, Some(LiteralValue::Number(0.0) | LiteralValue::Int(0)));
    let circ_count = (1..=100)
        .flat_map(|row| (1..=17).map(move |col| (row, col)))
        .filter(|&(row, col)| is_circ(&engine, row, col))
        .count();
    assert!(
        c9_is_zero && circ_count == 0,
        "C9 through IF-selected reference expected numeric zero, got {c9:?}; expected zero Circ cells, got {circ_count}"
    );
}

#[test]
fn index_rect_edge_precision_omitted_column_single_vector() {
    let mut engine = build_guarded_chain("=INDEX(Q1:Q100,24,)");
    engine.evaluate_all().expect("evaluate");
    assert!(
        matches!(
            engine.get_cell_value("Sheet1", 9, 3),
            Some(LiteralValue::Number(0.0) | LiteralValue::Int(0))
        ),
        "vertical vector with omitted column must resolve one cell"
    );
    assert_eq!(
        (1..=100)
            .flat_map(|row| (1..=17).map(move |col| (row, col)))
            .filter(|&(row, col)| is_circ(&engine, row, col))
            .count(),
        0
    );
}

#[test]
fn index_rect_edge_precision_omitted_row_single_vector() {
    let mut engine = runtime_engine();
    engine
        .set_cell_value("Sheet1", 1, 17, LiteralValue::Int(0))
        .expect("set Q1");
    engine
        .set_cell_value("Sheet1", 1, 18, LiteralValue::Int(0))
        .expect("set R1");
    set_formula(&mut engine, 1, 19, "=$C$9");
    set_formula(&mut engine, 9, 3, "=INDEX(Q1:S1,,2)");
    engine.evaluate_all().expect("evaluate");
    assert!(
        matches!(
            engine.get_cell_value("Sheet1", 9, 3),
            Some(LiteralValue::Number(0.0) | LiteralValue::Int(0))
        ),
        "horizontal vector with omitted row must resolve one cell"
    );
    assert!(!is_circ(&engine, 1, 19));
}

#[test]
fn index_rect_edge_genuine_cycle_still_detected() {
    let mut engine = build_guarded_chain("=INDEX(Q1:Q100,40)");
    engine.evaluate_all().expect("evaluate");
    assert!(is_circ(&engine, 9, 3), "C9 must remain Circ");
}

#[test]
fn index_selected_error_propagates() {
    let mut engine = runtime_engine();
    set_formula(&mut engine, 1, 17, "=1/0");
    engine
        .set_cell_value("Sheet1", 3, 17, LiteralValue::Int(9))
        .expect("set Q3");
    set_formula(&mut engine, 9, 3, "=INDEX(Q1:Q3,1)");
    engine.evaluate_all().expect("evaluate");
    assert!(
        matches!(
            engine.get_cell_value("Sheet1", 9, 3),
            Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Div
        ),
        "INDEX must propagate the selected cell's DIV/0 error"
    );
}

#[test]
fn index_unselected_error_is_ignored() {
    let mut engine = runtime_engine();
    engine
        .set_cell_value("Sheet1", 1, 17, LiteralValue::Int(7))
        .expect("set Q1");
    set_formula(&mut engine, 3, 17, "=NA()");
    set_formula(&mut engine, 9, 3, "=INDEX(Q1:Q3,1)+1");
    engine.evaluate_all().expect("evaluate");
    assert!(
        matches!(
            engine.get_cell_value("Sheet1", 9, 3),
            Some(LiteralValue::Number(8.0) | LiteralValue::Int(8))
        ),
        "an unselected error must not affect the INDEX result"
    );
}

#[test]
fn index_selected_error_phantom_cycle_stays_acyclic() {
    let mut engine = runtime_engine();
    set_formula(&mut engine, 1, 17, "=1/0");
    set_formula(&mut engine, 3, 17, "=C9");
    set_formula(&mut engine, 9, 3, "=INDEX(Q1:Q3,1)");
    engine.evaluate_all().expect("evaluate");
    for (row, col, label) in [(9, 3, "C9"), (3, 17, "Q3")] {
        assert!(
            matches!(
                engine.get_cell_value("Sheet1", row, col),
                Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Div
            ),
            "{label} must propagate DIV/0 without a circular error"
        );
    }
}

#[test]
fn index_precise_path_applies_format_policy() {
    let mut engine = runtime_engine();
    engine
        .set_cell_value(
            "Sheet1",
            1,
            17,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 1).expect("valid date")),
        )
        .expect("set Q1");
    let ast = parse("=INDEX(Q1:Q3,1)").expect("valid INDEX formula");
    let value = crate::interpreter::Interpreter::new(&engine, "Sheet1")
        .evaluate_ast(&ast)
        .expect("evaluate INDEX");
    assert_eq!(value.format_id(), None, "INDEX drops source annotations");
}

#[test]
fn sum_over_rect_keeps_whole_rect_edges() {
    let mut engine = build_guarded_chain("=SUM(Q1:Q100)");
    engine.evaluate_all().expect("evaluate");
    assert!(is_circ(&engine, 9, 3), "C9 must remain Circ");
}

#[test]
fn index_unbounded_column_selection_measured() {
    let mut engine = build_guarded_chain("=INDEX(Q:Q,24)");
    engine.evaluate_all().expect("evaluate");
    assert!(
        is_circ(&engine, 9, 3),
        "unbounded INDEX retains whole-column live edges while resolving bounds"
    );
}
