use chrono::Duration;

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn run(formula: &str, a1: LiteralValue) -> Option<LiteralValue> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine.set_cell_value("Sheet1", 1, 1, a1).unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.get_cell_value("Sheet1", 1, 2)
}

fn dur_2d_14h_30m_45s() -> LiteralValue {
    LiteralValue::Duration(Duration::seconds(2 * 86_400 + 14 * 3600 + 30 * 60 + 45))
}

fn assert_int_like(v: Option<LiteralValue>, expected: i64) {
    match v {
        Some(LiteralValue::Int(n)) => assert_eq!(n, expected),
        Some(LiteralValue::Number(n)) => assert!(
            (n - expected as f64).abs() < 1e-9,
            "expected ~{expected}, got {n}",
        ),
        other => panic!("expected integer-like {expected}, got {other:?}"),
    }
}

#[test]
fn hour_on_duration_cell_returns_fraction_hours() {
    assert_int_like(run("=HOUR(A1)", dur_2d_14h_30m_45s()), 14);
}

#[test]
fn minute_on_duration_cell_returns_fraction_minutes() {
    assert_int_like(run("=MINUTE(A1)", dur_2d_14h_30m_45s()), 30);
}

#[test]
fn second_on_duration_cell_returns_fraction_seconds() {
    assert_int_like(run("=SECOND(A1)", dur_2d_14h_30m_45s()), 45);
}

#[test]
fn weekday_on_duration_cell_does_not_value_error() {
    let r = run(
        "=WEEKDAY(A1)",
        LiteralValue::Duration(Duration::days(45657)),
    );
    match r {
        Some(LiteralValue::Int(n)) => assert!((1..=7).contains(&n), "weekday out of range: {n}"),
        Some(LiteralValue::Number(n)) => {
            assert!((1.0..=7.0).contains(&n), "weekday out of range: {n}")
        }
        other => panic!("expected weekday integer, got {other:?}"),
    }
}
