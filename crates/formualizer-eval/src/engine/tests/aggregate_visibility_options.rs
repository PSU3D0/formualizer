use super::common::arrow_eval_config;
use crate::engine::{Engine, RowVisibilitySource};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn assert_num(value: Option<LiteralValue>, expected: f64) {
    match value {
        Some(LiteralValue::Number(n)) => assert!((n - expected).abs() < 1e-9),
        Some(LiteralValue::Int(i)) => assert!(((i as f64) - expected).abs() < 1e-9),
        other => panic!("expected numeric {expected}, got {other:?}"),
    }
}

fn assert_error_kind(value: Option<LiteralValue>, expected: ExcelErrorKind) {
    match value {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, expected),
        other => panic!("expected error {:?}, got {other:?}", expected),
    }
}

fn op_expected(function_num_1_to_11: i32, values: &[f64]) -> f64 {
    assert!(!values.is_empty());

    let n = values.len() as f64;
    let sum = values.iter().copied().sum::<f64>();
    let mean = sum / n;

    match function_num_1_to_11 {
        1 => mean,
        2 | 3 => n,
        4 => values.iter().copied().reduce(f64::max).unwrap(),
        5 => values.iter().copied().reduce(f64::min).unwrap(),
        6 => values.iter().copied().product::<f64>(),
        7 => {
            let ss = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>();
            (ss / (n - 1.0)).sqrt()
        }
        8 => {
            let ss = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>();
            (ss / n).sqrt()
        }
        9 => sum,
        10 => {
            let ss = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>();
            ss / (n - 1.0)
        }
        11 => {
            let ss = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>();
            ss / n
        }
        _ => panic!("unsupported op code: {function_num_1_to_11}"),
    }
}

#[test]
fn aggregate_options_apply_hidden_and_error_policies() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());

    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(20))
        .unwrap();
    engine
        .set_cell_value(
            "Sheet1",
            4,
            1,
            LiteralValue::Error(formualizer_common::ExcelError::new_div()),
        )
        .unwrap();
    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(100))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=AGGREGATE(9,0,A2:A5)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=AGGREGATE(9,1,A2:A5)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=AGGREGATE(9,2,A2:A5)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 5, parse("=AGGREGATE(9,3,A2:A5)").unwrap())
        .unwrap();

    engine
        .set_row_hidden("Sheet1", 3, true, RowVisibilitySource::Manual)
        .unwrap();
    engine
        .set_row_hidden("Sheet1", 5, true, RowVisibilitySource::Filter)
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_error_kind(engine.get_cell_value("Sheet1", 1, 2), ExcelErrorKind::Div);
    assert_error_kind(engine.get_cell_value("Sheet1", 1, 3), ExcelErrorKind::Div);
    assert_num(engine.get_cell_value("Sheet1", 1, 4), 130.0);
    assert_num(engine.get_cell_value("Sheet1", 1, 5), 10.0);

    engine
        .set_row_hidden("Sheet1", 5, false, RowVisibilitySource::Filter)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 5), 110.0);
}

#[test]
fn aggregate_phase1_unsupported_paths_surface_expected_errors() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());

    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(10))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=AGGREGATE(9,4,A2:A2)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=AGGREGATE(12,0,A2:A2)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=AGGREGATE(9,8,A2:A2)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_error_kind(engine.get_cell_value("Sheet1", 1, 1), ExcelErrorKind::NImpl);
    assert_error_kind(engine.get_cell_value("Sheet1", 1, 2), ExcelErrorKind::NImpl);
    assert_error_kind(engine.get_cell_value("Sheet1", 1, 3), ExcelErrorKind::Value);
}

#[test]
fn aggregate_all_phase1_function_and_option_codes_match_matrix() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());

    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(20))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 1, LiteralValue::Int(30))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(100))
        .unwrap();

    engine
        .set_row_hidden("Sheet1", 3, true, RowVisibilitySource::Manual)
        .unwrap();
    engine
        .set_row_hidden("Sheet1", 4, true, RowVisibilitySource::Filter)
        .unwrap();

    let mut col = 2u32;
    for function_num in 1..=11 {
        for options in 0..=3 {
            let formula = format!("=AGGREGATE({function_num},{options},A2:A5)");
            engine
                .set_cell_formula("Sheet1", 1, col, parse(&formula).unwrap())
                .unwrap();
            col += 1;
        }
    }

    engine.evaluate_all().unwrap();

    let include_all = [10.0, 20.0, 30.0, 100.0];
    let visible_only = [10.0, 100.0];

    let mut verify_col = 2u32;
    for function_num in 1..=11 {
        for options in 0..=3 {
            let values = if options == 1 || options == 3 {
                &visible_only[..]
            } else {
                &include_all[..]
            };
            let expected = op_expected(function_num, values);
            assert_num(engine.get_cell_value("Sheet1", 1, verify_col), expected);
            verify_col += 1;
        }
    }
}
