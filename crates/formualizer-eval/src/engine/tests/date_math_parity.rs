use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn date_functions_days_yearfrac_isoweeknum_in_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            parse("=DAYS(DATE(2021,3,15),DATE(2021,2,1))").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            2,
            parse("=YEARFRAC(DATE(2021,1,1),DATE(2021,7,1),2)").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            3,
            parse("=ISOWEEKNUM(DATE(2016,1,1))").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(42.0))
    );

    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Number(v)) => assert!((v - (181.0 / 360.0)).abs() < 1e-12),
        other => panic!("expected numeric YEARFRAC value, got {other:?}"),
    }

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(53.0))
    );
}

#[test]
fn math_functions_mround_roman_arabic_sumsq_in_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=MROUND(1.3,0.2)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=ROMAN(499,4)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=ARABIC(B1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=SUMSQ(3,4)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Number(v)) => assert!((v - 1.4).abs() < 1e-12),
        other => panic!("expected numeric MROUND result, got {other:?}"),
    }
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Text("ID".to_string()))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(499.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(25.0))
    );
}

#[test]
fn math_functions_legacy_pack_in_engine() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=QUOTIENT(10,3)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=EVEN(1.5)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 3, parse("=ODD(2)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 4, parse("=SQRTPI(1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 5, parse("=MULTINOMIAL(2,3,4)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 6, parse("=SERIESSUM(2,0,1,{1,2,3})").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(3.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 3),
        Some(LiteralValue::Number(3.0))
    );

    match engine.get_cell_value("Sheet1", 2, 4) {
        Some(LiteralValue::Number(v)) => {
            assert!((v - std::f64::consts::PI.sqrt()).abs() < 1e-12)
        }
        other => panic!("expected numeric SQRTPI result, got {other:?}"),
    }

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 5),
        Some(LiteralValue::Number(1260.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 6),
        Some(LiteralValue::Number(17.0))
    );
}
