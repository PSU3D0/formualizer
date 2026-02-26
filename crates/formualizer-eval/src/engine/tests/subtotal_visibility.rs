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
fn subtotal_109_respects_manual_and_filter_hidden_rows() {
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
        .set_cell_formula("Sheet1", 1, 2, parse("=SUBTOTAL(9,A2:A5)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=SUBTOTAL(109,A2:A5)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 2), 160.0);
    assert_num(engine.get_cell_value("Sheet1", 1, 3), 160.0);

    engine
        .set_row_hidden("Sheet1", 3, true, RowVisibilitySource::Manual)
        .unwrap();
    engine
        .set_row_hidden("Sheet1", 4, true, RowVisibilitySource::Filter)
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 2), 160.0);
    assert_num(engine.get_cell_value("Sheet1", 1, 3), 110.0);

    engine
        .set_row_hidden("Sheet1", 3, false, RowVisibilitySource::Manual)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 3), 130.0);

    engine
        .set_row_hidden("Sheet1", 4, false, RowVisibilitySource::Filter)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 3), 160.0);
}

#[test]
fn subtotal_109_skips_error_when_row_is_hidden() {
    let mut engine = Engine::new(TestWorkbook::new(), arrow_eval_config());

    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_value(
            "Sheet1",
            3,
            1,
            LiteralValue::Error(formualizer_common::ExcelError::new_div()),
        )
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 1, LiteralValue::Int(30))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUBTOTAL(109,A2:A4)").unwrap())
        .unwrap();

    engine
        .set_row_hidden("Sheet1", 3, true, RowVisibilitySource::Manual)
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("Sheet1", 1, 2), 40.0);

    engine
        .set_row_hidden("Sheet1", 3, false, RowVisibilitySource::Manual)
        .unwrap();
    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Div),
        other => panic!("expected #DIV/0!, got {other:?}"),
    }
}

#[test]
fn subtotal_all_function_codes_match_expected_matrix() {
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
    for code in 1..=11 {
        let formula = format!("=SUBTOTAL({code},A2:A5)");
        engine
            .set_cell_formula("Sheet1", 1, col, parse(&formula).unwrap())
            .unwrap();
        col += 1;
    }
    for code in 101..=111 {
        let formula = format!("=SUBTOTAL({code},A2:A5)");
        engine
            .set_cell_formula("Sheet1", 1, col, parse(&formula).unwrap())
            .unwrap();
        col += 1;
    }

    engine.evaluate_all().unwrap();

    let include_all = [10.0, 20.0, 30.0, 100.0];
    let visible_only = [10.0, 100.0];

    let mut verify_col = 2u32;
    for code in 1..=11 {
        let expected = op_expected(code, &include_all);
        assert_num(engine.get_cell_value("Sheet1", 1, verify_col), expected);
        verify_col += 1;
    }
    for code in 101..=111 {
        let expected = op_expected(code - 100, &visible_only);
        assert_num(engine.get_cell_value("Sheet1", 1, verify_col), expected);
        verify_col += 1;
    }
}
