use std::time::Duration;

use crate::engine::eval::EvaluationSlice;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn chain_engine(rows: u32) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.add_sheet("Sheet1").unwrap();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    for row in 2..=rows {
        let formula = parse(format!("=A{}+1", row - 1)).unwrap();
        engine.set_cell_formula("Sheet1", row, 1, formula).unwrap();
    }
    for row in 1..=rows {
        let formula = parse(format!("=A{row}*2")).unwrap();
        engine.set_cell_formula("Sheet1", row, 2, formula).unwrap();
    }
    engine
}

fn run_slices(engine: &mut Engine<TestWorkbook>, slice: &mut EvaluationSlice) -> usize {
    let mut calls = 0;
    loop {
        calls += 1;
        let result = engine.evaluate_slice(slice, Duration::ZERO).unwrap();
        if result.finished {
            return calls;
        }
        assert!(calls < 1_000_000, "evaluate_slice never finished");
    }
}

#[test]
fn a_zero_budget_still_moves_forward_and_matches_a_full_evaluation() {
    let mut sliced = chain_engine(300);
    let mut slice = EvaluationSlice::default();
    let calls = run_slices(&mut sliced, &mut slice);
    assert!(calls > 1, "a zero budget should take several calls");

    let mut whole = chain_engine(300);
    whole.evaluate_all().unwrap();
    for row in 1..=300 {
        for col in 1..=2 {
            assert_eq!(
                sliced.get_cell_value("Sheet1", row, col),
                whole.get_cell_value("Sheet1", row, col),
                "row {row} col {col}"
            );
        }
    }
}

#[test]
fn finished_work_stays_committed_between_calls() {
    let mut engine = chain_engine(300);
    let mut slice = EvaluationSlice::default();
    let first = engine.evaluate_slice(&mut slice, Duration::ZERO).unwrap();
    assert!(!first.finished);
    assert!(first.computed_vertices > 0);
    // Nothing of what the first call finished is computed again.
    let mut rest = 0;
    loop {
        let result = engine.evaluate_slice(&mut slice, Duration::ZERO).unwrap();
        rest += result.computed_vertices;
        if result.finished {
            break;
        }
    }
    assert_eq!(first.computed_vertices + rest, 599);
}

#[test]
fn an_edit_between_calls_is_picked_up_after_reset() {
    let mut engine = chain_engine(300);
    let mut slice = EvaluationSlice::default();
    assert!(
        !engine
            .evaluate_slice(&mut slice, Duration::ZERO)
            .unwrap()
            .finished
    );

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(100.0))
        .unwrap();
    slice.reset();
    run_slices(&mut engine, &mut slice);
    assert_eq!(
        engine.get_cell_value("Sheet1", 300, 1),
        Some(LiteralValue::Number(399.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 300, 2),
        Some(LiteralValue::Number(798.0))
    );
}

#[test]
fn adopted_values_stay_until_a_precedent_changes() {
    let config = EvalConfig {
        defer_graph_building: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine.add_sheet("Sheet1").unwrap();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(3.0))
        .unwrap();
    // Both stored values are wrong on purpose, so a recalculation would show.
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(999.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(888.0))
        .unwrap();
    engine.stage_formula_text("Sheet1", 1, 2, "=A1*10".to_string());
    engine.stage_formula_text("Sheet1", 2, 2, "=A2*10".to_string());

    assert_eq!(engine.adopt_stored_values().unwrap(), 2);
    let mut slice = EvaluationSlice::default();
    assert!(
        engine
            .evaluate_slice(&mut slice, Duration::from_millis(50))
            .unwrap()
            .finished
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(999.0))
    );

    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(4.0))
        .unwrap();
    slice.reset();
    run_slices(&mut engine, &mut slice);
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(40.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(999.0))
    );
}

#[test]
fn marking_everything_dirty_recalculates_everything() {
    let mut engine = chain_engine(20);
    engine.evaluate_all().unwrap();
    engine.mark_all_formulas_dirty();
    let mut slice = EvaluationSlice::default();
    let mut computed = 0;
    loop {
        let result = engine
            .evaluate_slice(&mut slice, Duration::from_millis(50))
            .unwrap();
        computed += result.computed_vertices;
        if result.finished {
            break;
        }
    }
    assert_eq!(computed, 39);
}
