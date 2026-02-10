use chrono::NaiveDate;

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn countifs_date_criteria_with_ampersand_concatenation() {
    // Regression for criteria strings like ">="&C1 where C1 is a date.
    // Libre/Excel treat dates as serials for criteria parsing.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let sheet = "Calculations";

    // Criteria bounds: 2024-11-01 .. 2024-11-30
    engine
        .set_cell_value(
            sheet,
            1,
            3,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 11, 1).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value(
            sheet,
            1,
            4,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 11, 30).unwrap()),
        )
        .unwrap();

    // Values: 11/15, 11/29, 12/13
    engine
        .set_cell_value(
            sheet,
            110,
            3,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 11, 15).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value(
            sheet,
            111,
            3,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 11, 29).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value(
            sheet,
            112,
            3,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 13).unwrap()),
        )
        .unwrap();

    // COUNTIFS(C110:C112,">="&C1,C110:C112,"<="&D1)
    engine
        .set_cell_formula(
            sheet,
            109,
            8,
            parse("=COUNTIFS(C110:C112,\">=\"&C1,C110:C112,\"<=\"&D1)").unwrap(),
        )
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value(sheet, 109, 8),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn countifs_date_equality_accepts_date_literal_criteria() {
    // Criteria passed as a date value (not a string) should work.
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let sheet = "Calculations";

    let target = NaiveDate::from_ymd_opt(2024, 11, 29).unwrap();

    engine
        .set_cell_value(sheet, 1, 3, LiteralValue::Date(target))
        .unwrap();

    engine
        .set_cell_value(
            sheet,
            110,
            3,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 11, 15).unwrap()),
        )
        .unwrap();
    engine
        .set_cell_value(sheet, 111, 3, LiteralValue::Date(target))
        .unwrap();
    engine
        .set_cell_value(
            sheet,
            112,
            3,
            LiteralValue::Date(NaiveDate::from_ymd_opt(2024, 12, 13).unwrap()),
        )
        .unwrap();

    engine
        .set_cell_formula(sheet, 10, 8, parse("=COUNTIFS(C110:C112,C1)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value(sheet, 10, 8),
        Some(LiteralValue::Number(1.0))
    );
}
