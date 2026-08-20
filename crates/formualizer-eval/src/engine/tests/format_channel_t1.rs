use crate::engine::{EvalConfig, TemporalEgress, eval::Engine};
use crate::format::FormatId;
use crate::test_workbook::TestWorkbook;
use chrono::NaiveDate;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn issue_312_engine(policy: TemporalEgress) -> Engine<TestWorkbook> {
    let config = EvalConfig {
        temporal_egress: policy,
        ..Default::default()
    };
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine
        .set_cell_value(
            "Sheet1",
            1,
            1,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(45_658.0))
        .unwrap();
    for (col, formula) in [(3, "=A1+B1"), (4, "=A1-45627"), (5, "=A1+0"), (6, "=A1-B1")] {
        engine
            .set_cell_formula("Sheet1", 1, col, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn issue_312_serial_divergence_table_matches_excel() {
    let engine = issue_312_engine(TemporalEgress::Serial);
    let values: Vec<_> = (3..=6)
        .map(|col| engine.get_cell_value("Sheet1", 1, col))
        .collect();
    assert_eq!(
        values,
        vec![
            Some(LiteralValue::Number(91_285.0)),
            Some(LiteralValue::Number(0.0)),
            Some(LiteralValue::Number(45_627.0)),
            Some(LiteralValue::Number(-31.0)),
        ]
    );
}

#[test]
fn date_plus_number_preserves_generic_date_annotation() {
    let engine = issue_312_engine(TemporalEgress::Serial);
    assert_eq!(
        engine.effective_format_id("Sheet1", 1, 1),
        Some(FormatId::DATE)
    );
    let ast = parse("=A1+0").unwrap();
    let cv = crate::interpreter::Interpreter::new(&engine, "Sheet1")
        .evaluate_ast(&ast)
        .unwrap();
    assert_eq!(
        cv.format_id(),
        Some(FormatId::DATE),
        "direct interpreter annotation"
    );
    assert_eq!(
        engine.effective_format_id("Sheet1", 1, 5),
        Some(FormatId::DATE)
    );
}

#[test]
fn date_minus_date_drops_annotation() {
    let mut engine = issue_312_engine(TemporalEgress::Serial);
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=A1-A1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(0.0))
    );
    assert_eq!(engine.effective_format_id("Sheet1", 2, 1), None);
}

#[test]
fn native_egress_consults_computed_format_and_serial_opt_out_is_uniform() {
    let native = issue_312_engine(TemporalEgress::Native);
    assert_eq!(
        native.get_cell_value("Sheet1", 1, 5),
        Some(LiteralValue::Date(
            NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()
        ))
    );
    let serial = issue_312_engine(TemporalEgress::Serial);
    assert_eq!(
        serial.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(45_627.0))
    );
    assert_eq!(
        serial.get_cell_value("Sheet1", 1, 5),
        Some(LiteralValue::Number(45_627.0))
    );
}

#[test]
fn temporal_constructor_annotation_reaches_native_egress() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=DATE(2024,12,1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.effective_format_id("Sheet1", 1, 1),
        Some(FormatId::DATE)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Date(
            NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()
        ))
    );
}

#[test]
fn computed_temporals_are_numbers_to_type_functions() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=ISNUMBER(DATE(2024,12,1))").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=TYPE(DATE(2024,12,1))").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Boolean(true))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(1.0))
    );
}

#[test]
fn midnight_datetime_uses_format_instead_of_value_heuristic() {
    let midnight = NaiveDate::from_ymd_opt(2024, 12, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::DateTime(midnight))
        .unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::DateTime(midnight))
    );
}

#[test]
fn two_vec_format_runs_compress_and_slice_without_dense_storage() {
    use crate::arrow_store::FormatRuns;
    assert!(FormatRuns::from_ids(&[0, 0, 0]).is_none());
    let runs = FormatRuns::from_ids(&[0, 14, 14, 0, 22, 22]).unwrap();
    assert_eq!(runs.get(0), FormatId::GENERAL);
    assert_eq!(runs.get(2), FormatId::DATE);
    assert_eq!(runs.get(4), FormatId::DATETIME);
    let slice = runs.slice(1, 3).unwrap();
    assert_eq!(slice.to_ids(3), vec![14, 14, 0]);
}

#[test]
fn issue_312_interpreter_values_are_always_numeric() {
    let engine = issue_312_engine(TemporalEgress::Serial);
    for (formula, expected) in [
        ("=A1+B1", 91_285.0),
        ("=A1-45627", 0.0),
        ("=A1+0", 45_627.0),
        ("=A1-B1", -31.0),
    ] {
        let ast = parse(formula).unwrap();
        let result = crate::interpreter::Interpreter::new(&engine, "Sheet1")
            .evaluate_ast(&ast)
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Number(expected), "{formula}");
    }
}
