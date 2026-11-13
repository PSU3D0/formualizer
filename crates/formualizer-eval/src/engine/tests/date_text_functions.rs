//! Date functions over cells that hold a date as text (an apostrophe-escaped
//! `'10/26/1999`, or ISO text imported from another tool). Excel keeps such a
//! cell as text (`ISTEXT` is TRUE) but reads the text as a date wherever a
//! function expects a date serial (#576).
//!
//! Serials (Excel 1900 system): 10/26/1999 = 36459, 10/27/1999 = 36460.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

#[derive(Clone, Copy, Debug)]
enum Expected {
    Number(f64),
    Boolean(bool),
    Error(ExcelErrorKind),
}

/// A1:A3 hold date text, A4 plain text and A5 date text before the 1900
/// epoch; A6:A8 hold the inputs of the saved Excel workbook in #576 as typed
/// there, A9 ISO text with a `T` separator and A10 with a lowercase `t`.
/// `formula` goes in D1.
fn eval_over_date_text(formula: &str) -> LiteralValue {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    let texts = [
        "10/26/1999",
        "1999-10-27",
        "2023-05-04 14:05",
        "abc",
        "1/1/1899",
        "10/26/1999",
        "10/27/1999",
        "5/4/2023  2:05:00 PM",
        "1999-10-26T00:00",
        "1999-10-26t00:00",
    ];
    for (row, text) in texts.iter().enumerate() {
        let row = row as u32 + 1;
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Text((*text).into()))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 4, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .get_cell_value("Sheet1", 1, 4)
        .unwrap_or(LiteralValue::Empty)
}

fn check(cases: &[(&str, Expected)]) {
    let mut failures = Vec::new();
    for (formula, expected) in cases {
        let actual = eval_over_date_text(formula);
        let ok = match expected {
            Expected::Number(n) => actual == LiteralValue::Number(*n),
            Expected::Boolean(b) => actual == LiteralValue::Boolean(*b),
            Expected::Error(kind) => matches!(&actual, LiteralValue::Error(e) if e.kind == *kind),
        };
        if !ok {
            failures.push(format!("{formula}: expected {expected:?}, got {actual:?}"));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Behaviour that already matches Excel and must not change.
#[test]
fn date_text_stays_text_and_comparisons_skip_it() {
    check(&[
        ("=ISTEXT(A1)", Expected::Boolean(true)),
        ("=A1+1", Expected::Number(36_460.0)),
        ("=A2-A1", Expected::Number(1.0)),
        // Comparison criteria never match text, date text included.
        ("=COUNTIF(A1:A3,\">1/1/1999\")", Expected::Number(0.0)),
        // A number lookup value does not match date text.
        (
            "=MATCH(DATE(1999,10,26),A1:A3,0)",
            Expected::Error(ExcelErrorKind::Na),
        ),
    ]);
}

#[test]
fn date_functions_read_date_text() {
    check(&[
        ("=YEAR(A1)", Expected::Number(1999.0)),
        ("=MONTH(A1)", Expected::Number(10.0)),
        ("=DAY(A1)", Expected::Number(26.0)),
        // Tuesday, with Sunday = 1.
        ("=WEEKDAY(A1)", Expected::Number(3.0)),
        ("=EDATE(A1,1)", Expected::Number(36_490.0)),
        ("=EOMONTH(A1,0)", Expected::Number(36_464.0)),
        ("=DATEDIF(A1,A2,\"d\")", Expected::Number(1.0)),
        ("=DAYS(A2,A1)", Expected::Number(1.0)),
        ("=NETWORKDAYS(A1,A2)", Expected::Number(2.0)),
        ("=YEAR(A3)", Expected::Number(2023.0)),
        ("=HOUR(A3)", Expected::Number(14.0)),
        // The Excel workbook's own inputs (#576).
        ("=DATEDIF(A6,A7,\"d\")", Expected::Number(1.0)),
        ("=YEAR(A8)", Expected::Number(2023.0)),
        ("=HOUR(A8)", Expected::Number(14.0)),
        // ISO text with a `T` reads as a date, as `A9+0` does and as in Google
        // Sheets. Excel 2609 returns #VALUE! for both (#576).
        ("=A9+0", Expected::Number(36_459.0)),
        ("=YEAR(A9)", Expected::Number(1999.0)),
        // A lowercase `t` too, in date functions and arithmetic alike, as in
        // Google Sheets (Excel 2609: #VALUE! for both; #576).
        ("=YEAR(A10)", Expected::Number(1999.0)),
        ("=A10+0", Expected::Number(36_459.0)),
    ]);
}

/// Every date argument switched to `DateTimeSerial` reads date text as the
/// serial it holds: each pair is the function over the text cells and over
/// the serials (10/26/1999 = 36459, 10/27/1999 = 36460, A3 = 5/4/2023 14:05).
#[test]
fn every_date_argument_reads_date_text_as_its_serial() {
    let a3 = "(DATE(2023,5,4)+TIME(14,5,0))";
    let pairs = [
        ("YEAR(A1)".to_string(), "YEAR(36459)".to_string()),
        ("MONTH(A1)".to_string(), "MONTH(36459)".to_string()),
        ("DAY(A1)".to_string(), "DAY(36459)".to_string()),
        ("HOUR(A3)".to_string(), format!("HOUR({a3})")),
        ("WEEKDAY(A1)".to_string(), "WEEKDAY(36459)".to_string()),
        ("EOMONTH(A1,0)".to_string(), "EOMONTH(36459,0)".to_string()),
        (
            "DATEDIF(A1,A3,\"d\")".to_string(),
            format!("DATEDIF(36459,{a3},\"d\")"),
        ),
        (
            "DATEDIF(36459,A3,\"d\")".to_string(),
            format!("DATEDIF(36459,{a3},\"d\")"),
        ),
        (
            "NETWORKDAYS(A1,A3)".to_string(),
            format!("NETWORKDAYS(36459,{a3})"),
        ),
        (
            "NETWORKDAYS(36459,A3)".to_string(),
            format!("NETWORKDAYS(36459,{a3})"),
        ),
        ("MINUTE(A3)".to_string(), format!("MINUTE({a3})")),
        ("SECOND(A3)".to_string(), format!("SECOND({a3})")),
        ("WEEKNUM(A1)".to_string(), "WEEKNUM(36459)".to_string()),
        (
            "ISOWEEKNUM(A1)".to_string(),
            "ISOWEEKNUM(36459)".to_string(),
        ),
        ("DAYS360(A1,A3)".to_string(), format!("DAYS360(36459,{a3})")),
        (
            "YEARFRAC(A1,A3)".to_string(),
            format!("YEARFRAC(36459,{a3})"),
        ),
        ("WORKDAY(A1,1)".to_string(), "WORKDAY(36459,1)".to_string()),
        (
            "WORKDAY.INTL(A1,1)".to_string(),
            "WORKDAY.INTL(36459,1)".to_string(),
        ),
        (
            "NETWORKDAYS.INTL(A1,A3)".to_string(),
            format!("NETWORKDAYS.INTL(36459,{a3})"),
        ),
        ("DAYS(A3,A1)".to_string(), format!("DAYS({a3},36459)")),
        ("EDATE(A2,-1)".to_string(), "EDATE(36460,-1)".to_string()),
    ];
    let mut failures = Vec::new();
    for (text, serial) in pairs {
        let (got, want) = (
            eval_over_date_text(&format!("={text}")),
            eval_over_date_text(&format!("={serial}")),
        );
        if !matches!(want, LiteralValue::Number(_) | LiteralValue::Int(_)) || got != want {
            failures.push(format!("{text}: got {got:?}, {serial} is {want:?}"));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Text that is not a date stays #VALUE!. Date text before the epoch is a
/// negative serial, so YEAR rejects it with #NUM!, as Google Sheets does
/// (Excel 2609: #VALUE!; #576).
#[test]
fn plain_text_is_a_value_error_and_pre_1900_text_a_num_error() {
    let value = Expected::Error(ExcelErrorKind::Value);
    check(&[
        ("=YEAR(A4)", value),
        ("=EDATE(A4,1)", value),
        ("=DAYS(A4,A1)", value),
        ("=NETWORKDAYS(A1,A4)", value),
        ("=YEAR(A5)", Expected::Error(ExcelErrorKind::Num)),
    ]);
}

/// In a 1904 workbook date functions read date text in that system: the text
/// is the same date, so its serial is 1462 days smaller.
#[test]
fn date_functions_read_date_text_in_the_1904_system() {
    use crate::engine::DateSystem;
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_date_system(DateSystem::Excel1904),
    );
    for (row, text) in [(1, "10/26/1999"), (2, "10/27/1999")] {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Text(text.into()))
            .unwrap();
    }
    let cases = [
        ("=YEAR(A1)", 1999.0),
        ("=EDATE(A1,1)", 36_490.0 - 1_462.0),
        ("=A1+0", 36_459.0 - 1_462.0),
    ];
    for (row, (formula, _)) in cases.iter().enumerate() {
        engine
            .set_cell_formula("Sheet1", row as u32 + 1, 4, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    for (row, (formula, expected)) in cases.iter().enumerate() {
        let actual = engine.get_cell_value("Sheet1", row as u32 + 1, 4);
        let actual = match actual {
            Some(LiteralValue::Int(i)) => i as f64,
            Some(LiteralValue::Number(n)) => n,
            other => panic!("{formula}: {other:?}"),
        };
        assert_eq!(actual, *expected, "{formula}");
    }
}
