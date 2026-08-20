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
            10,
            6,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value("Sheet1", 10, 7, LiteralValue::Number(45_658.0))
        .unwrap();
    for (col, formula) in [
        (8, "=F10+G10"),
        (9, "=F10-45627"),
        (10, "=F10+0"),
        (11, "=F10-G10"),
    ] {
        engine
            .set_cell_formula("Sheet1", 10, col, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    engine
}

#[test]
fn issue_312_serial_divergence_table_matches_excel() {
    let engine = issue_312_engine(TemporalEgress::Serial);
    let values: Vec<_> = (8..=11)
        .map(|col| engine.get_cell_value("Sheet1", 10, col))
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
        engine.effective_format_id("Sheet1", 10, 6),
        Some(FormatId::DATE)
    );
    let ast = parse("=F10+0").unwrap();
    let cv = crate::interpreter::Interpreter::new(&engine, "Sheet1")
        .evaluate_ast(&ast)
        .unwrap();
    assert_eq!(
        cv.format_id(),
        Some(FormatId::DATE),
        "direct interpreter annotation"
    );
    assert_eq!(
        engine.effective_format_id("Sheet1", 10, 10),
        Some(FormatId::DATE)
    );
}

#[test]
fn date_minus_date_drops_annotation() {
    let mut engine = issue_312_engine(TemporalEgress::Serial);
    engine
        .set_cell_formula("Sheet1", 11, 6, parse("=F10-F10").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 11, 6),
        Some(LiteralValue::Number(0.0))
    );
    assert_eq!(engine.effective_format_id("Sheet1", 11, 6), None);
}

#[test]
fn native_egress_consults_computed_format_and_serial_opt_out_is_uniform() {
    let native = issue_312_engine(TemporalEgress::Native);
    assert_eq!(
        native.get_cell_value("Sheet1", 10, 10),
        Some(LiteralValue::Date(
            NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()
        ))
    );
    let serial = issue_312_engine(TemporalEgress::Serial);
    assert_eq!(
        serial.get_cell_value("Sheet1", 10, 6),
        Some(LiteralValue::Number(45_627.0))
    );
    assert_eq!(
        serial.get_cell_value("Sheet1", 10, 10),
        Some(LiteralValue::Number(45_627.0))
    );
}

#[test]
fn temporal_constructor_annotation_reaches_native_egress() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_formula("Sheet1", 5, 7, parse("=DATE(2024,12,1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.effective_format_id("Sheet1", 5, 7),
        Some(FormatId::DATE)
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 7),
        Some(LiteralValue::Date(
            NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()
        ))
    );
}

#[test]
fn selection_functions_preserve_the_selected_scalar_format() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value(
            "Sheet1",
            10,
            6,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()),
        )
        .unwrap();
    for (row, formula) in [
        (12, "=IFERROR(F10,0)"),
        (13, "=IFNA(F10,0)"),
        (14, "=MAX(F10,0)"),
        (15, "=MIN(F10,99999)"),
    ] {
        engine
            .set_cell_formula("Sheet1", row, 8, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();

    for row in 12..=15 {
        assert_eq!(
            engine.effective_format_id("Sheet1", row, 8),
            Some(FormatId::DATE)
        );
        assert!(matches!(
            engine.get_cell_value("Sheet1", row, 8),
            Some(LiteralValue::Date(_))
        ));
    }
}

#[test]
fn max_over_a_multi_cell_range_documents_the_format_limitation() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for (row, day) in [(10, 1), (11, 2)] {
        engine
            .set_cell_value(
                "Sheet1",
                row,
                6,
                LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, day).unwrap()),
            )
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 12, 8, parse("=MAX(F10:F11)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(engine.effective_format_id("Sheet1", 12, 8), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 12, 8),
        Some(LiteralValue::Number(45_628.0))
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
fn general_annotation_is_filtered_from_calc_values() {
    let value = crate::traits::CalcValue::Scalar(LiteralValue::Number(1.0))
        .with_format(Some(FormatId::GENERAL));
    assert!(matches!(value, crate::traits::CalcValue::Scalar(_)));
    assert_eq!(value.format_id(), None);
}

#[test]
fn issue_312_interpreter_values_are_always_numeric() {
    let engine = issue_312_engine(TemporalEgress::Serial);
    for (formula, expected) in [
        ("=F10+G10", 91_285.0),
        ("=F10-45627", 0.0),
        ("=F10+0", 45_627.0),
        ("=F10-G10", -31.0),
    ] {
        let ast = parse(formula).unwrap();
        let result = crate::interpreter::Interpreter::new(&engine, "Sheet1")
            .evaluate_ast(&ast)
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Number(expected), "{formula}");
    }
}
