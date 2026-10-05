//! Date and time functions, `TEXT` date codes and text-to-number coercion
//! against Excel's documented behaviour. Each block cites its source:
//! Microsoft support pages for the function or format code, or Excel's cached
//! results in real workbooks where the documentation is silent.
use crate::engine::{DateSystem, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

#[derive(Debug)]
enum Expected {
    Number(f64),
    Text(&'static str),
    Error(ExcelErrorKind),
}
use Expected::{Error, Number, Text};

/// Evaluates `formulas` in column J of a sheet where A1 is blank, A2 holds 0,
/// A3 holds 60 and A4 holds the text "NaN".
fn evaluate(system: DateSystem, formulas: &[&str]) -> Vec<LiteralValue> {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_date_system(system),
    );
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(0.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Number(60.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 1, LiteralValue::Text("NaN".into()))
        .unwrap();
    for (row, formula) in formulas.iter().enumerate() {
        engine
            .set_cell_formula("Sheet1", row as u32 + 1, 10, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    (0..formulas.len())
        .map(|row| {
            engine
                .get_cell_value("Sheet1", row as u32 + 1, 10)
                .unwrap_or(LiteralValue::Empty)
        })
        .collect()
}

fn check(system: DateSystem, cases: &[(&str, Expected)]) {
    let formulas: Vec<&str> = cases.iter().map(|(formula, _)| *formula).collect();
    let values = evaluate(system, &formulas);
    let mut failures = Vec::new();
    for ((formula, expected), actual) in cases.iter().zip(values) {
        let ok = match (expected, &actual) {
            (Number(want), LiteralValue::Number(got)) => (want - got).abs() <= 1e-12 * want.abs(),
            (Number(want), LiteralValue::Int(got)) => *want == *got as f64,
            (Number(want), LiteralValue::Boolean(got)) => (*want == 1.0) == *got,
            (Number(want), other @ (LiteralValue::Date(_) | LiteralValue::DateTime(_))) => {
                other.as_serial_number_for(system) == Some(*want)
            }
            (Text(want), LiteralValue::Text(got)) => want == got,
            (Error(kind), LiteralValue::Error(got)) => *kind == got.kind,
            _ => false,
        };
        if !ok {
            failures.push(format!("{formula}: expected {expected:?}, got {actual:?}"));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// TEXT date and time codes. Source: Microsoft support, "TEXT function"
/// (https://support.microsoft.com/en-us/office/text-function-20d5ac4d-7b94-49fd-bb38-93d29371225c)
/// and "Format numbers as dates or times"
/// (https://support.microsoft.com/en-us/office/format-numbers-as-dates-or-times-418bd3fe-0577-47c8-8caa-b4d30c528309):
/// `yy` is a two-digit year, `ddd`/`dddd` are weekday names, `m`/`mm`
/// straight after `h`/`hh` or straight before `ss` are minutes, `[h]` is
/// elapsed hours.
#[test]
fn text_date_and_time_codes_match_excel() {
    // 37000.41799768519 is Thursday 2001-04-19 10:01:55.
    check(
        DateSystem::Excel1900,
        &[
            ("=TEXT(37000.41799768519,\"mm/dd/yy\")", Text("04/19/01")),
            (
                "=TEXT(37000.41799768519,\"mm/dd/yyyy\")",
                Text("04/19/2001"),
            ),
            ("=TEXT(37000.41799768519,\"m/d/yy\")", Text("4/19/01")),
            (
                "=DATEVALUE(TEXT(37000.41799768519,\"mm/dd/yy\"))",
                Number(37000.0),
            ),
            (
                "=TEXT(37000.41799768519,\"dddd, mmmm d, yyyy\")",
                Text("Thursday, April 19, 2001"),
            ),
            (
                "=TEXT(37000.41799768519,\"ddd d-mmm-yy\")",
                Text("Thu 19-Apr-01"),
            ),
            ("=TEXT(37000.41799768519,\"mmmmm\")", Text("A")),
            ("=TEXT(37000.41799768519,\"h:mm AM/PM\")", Text("10:01 AM")),
            ("=TEXT(37000.41799768519,\"hh:mm:ss\")", Text("10:01:55")),
            ("=TEXT(37000.41799768519,\"mm:ss\")", Text("01:55")),
            (
                "=TEXT(37000.41799768519,\"yyyy-mm-dd hh:mm\")",
                Text("2001-04-19 10:01"),
            ),
            ("=TEXT(2,\"dddd\")", Text("Monday")),
            ("=TEXT(6,\"dddd\")", Text("Friday")),
            ("=TEXT(36745,\"ddd\")", Text("Mon")),
            ("=TEXT(WEEKDAY(36710),\"dddd\")", Text("Monday")),
            ("=TEXT(0,\"dddd mm/dd/yyyy\")", Text("Saturday 01/00/1900")),
            ("=TEXT(60,\"mm/dd/yyyy\")", Text("02/29/1900")),
            ("=TEXT(1.5,\"[h]:mm\")", Text("36:00")),
            ("=TEXT(0.75,\"h AM/PM\")", Text("6 PM")),
            ("=TEXT(0.5+1.25/86400,\"hh:mm:ss.00\")", Text("12:00:01.25")),
            ("=TEXT(\"4/19/2001\",\"yyyy\")", Text("2001")),
            ("=TEXT(37000,\"\"\"Day \"\"d\")", Text("Day 19")),
            ("=TEXT(37000,\"\\Q yyyy\")", Text("Q 2001")),
            // Rounded to whole seconds, then split: 10:29:45 is 10:29 and
            // 23:59:59.6 is the next day's midnight.
            ("=TEXT(TIME(10,29,45),\"h:mm\")", Text("10:29")),
            (
                "=TEXT(45306+86399.6/86400,\"yyyy-mm-dd hh:mm\")",
                Text("2024-01-16 00:00"),
            ),
            ("=TEXT(37054.541666666664,\"h\")", Text("13")),
            ("=TEXT(-1,\"yyyy\")", Error(ExcelErrorKind::Value)),
        ],
    );
    check(
        DateSystem::Excel1904,
        &[
            ("=TEXT(0,\"dddd yyyy-mm-dd\")", Text("Friday 1904-01-01")),
            ("=TEXT(35538,\"mm/dd/yy\")", Text("04/19/01")),
        ],
    );
}

/// Serial 0 is "January 0, 1900" and serial 60 the phantom February 29,
/// 1900. Source: Microsoft support, "Date systems in Excel"
/// (https://support.microsoft.com/en-us/office/date-systems-in-excel-e7fe7167-48a9-4b96-bb53-5612a800b487)
/// and Excel's cached `MONTH`/`YEAR` of blank cells in real workbooks
/// (MONTH = 1, YEAR = 1900). A blank cell is 0.
#[test]
fn date_parts_of_serial_zero_and_the_phantom_leap_day() {
    check(
        DateSystem::Excel1900,
        &[
            ("=YEAR(A1)", Number(1900.0)),
            ("=MONTH(A1)", Number(1.0)),
            ("=DAY(A1)", Number(0.0)),
            ("=YEAR(A2)", Number(1900.0)),
            ("=MONTH(A2)", Number(1.0)),
            ("=DAY(A2)", Number(0.0)),
            ("=YEAR(0)", Number(1900.0)),
            ("=MONTH(0.75)", Number(1.0)),
            ("=YEAR(A3)", Number(1900.0)),
            ("=MONTH(A3)", Number(2.0)),
            ("=DAY(A3)", Number(29.0)),
            ("=DAY(59)", Number(28.0)),
            ("=DAY(61)", Number(1.0)),
            ("=WEEKDAY(0)", Number(7.0)),
            ("=TEXT(A1,\"dd/mm/yyyy\")", Text("00/01/1900")),
        ],
    );
    check(
        DateSystem::Excel1904,
        &[
            ("=YEAR(A1)", Number(1904.0)),
            ("=MONTH(A1)", Number(1.0)),
            ("=DAY(A1)", Number(1.0)),
        ],
    );
}

/// YEARFRAC returns the same positive value whatever the order of the dates,
/// and basis 0 uses Excel's US (NASD) 30/360 rule, which only rolls an end
/// day of 31 and treats February month-ends specially. Sources: Microsoft
/// support, "YEARFRAC function"
/// (https://support.microsoft.com/en-us/office/yearfrac-function-3844141e-c76d-4143-82b6-208454ddc6a8);
/// Excel's cached values in real workbooks (the reversed and month-end cases);
/// D. A. Wheeler's reverse-engineered Excel YEARFRAC algorithms
/// (https://dwheeler.com/yearfrac/) for basis 0 and basis 1.
#[test]
fn yearfrac_matches_excel() {
    check(
        DateSystem::Excel1900,
        &[
            ("=YEARFRAC(44958,36708)", Number(22.583333333333332)),
            ("=YEARFRAC(36708,44958)", Number(22.583333333333332)),
            ("=YEARFRAC(39448,37256)", Number(6.002777777777778)),
            ("=YEARFRAC(37256,39448)", Number(6.002777777777778)),
            ("=YEARFRAC(36586,39416)", Number(7.747222222222222)),
            ("=YEARFRAC(36586,39872)", Number(8.991666666666667)),
            ("=YEARFRAC(DATE(2001,2,28),DATE(2002,2,28))", Number(1.0)),
            ("=YEARFRAC(DATE(2000,2,29),DATE(2001,2,28))", Number(1.0)),
            (
                "=YEARFRAC(DATE(2001,2,28),DATE(2001,3,31))",
                Number(31.0 / 360.0),
            ),
            (
                "=YEARFRAC(DATE(2001,1,30),DATE(2001,3,31))",
                Number(60.0 / 360.0),
            ),
            (
                "=YEARFRAC(DATE(2001,1,31),DATE(2001,3,31))",
                Number(60.0 / 360.0),
            ),
            (
                "=YEARFRAC(DATE(2001,1,15),DATE(2001,3,31))",
                Number(76.0 / 360.0),
            ),
            (
                "=YEARFRAC(DATE(2021,7,1),DATE(2021,1,1),1)",
                Number(181.0 / 365.0),
            ),
            (
                "=YEARFRAC(DATE(2021,7,1),DATE(2021,1,1),2)",
                Number(181.0 / 360.0),
            ),
            (
                "=YEARFRAC(DATE(2021,7,1),DATE(2021,1,1),3)",
                Number(181.0 / 365.0),
            ),
            (
                "=YEARFRAC(DATE(2021,7,31),DATE(2021,1,1),4)",
                Number(209.0 / 360.0),
            ),
            // Basis 1: within a year, 366 days only when a February 29 is
            // covered; across years, the average length of the years spanned.
            (
                "=YEARFRAC(DATE(1999,6,1),DATE(2000,5,1),1)",
                Number(335.0 / 366.0),
            ),
            (
                "=YEARFRAC(DATE(2000,3,1),DATE(2001,2,1),1)",
                Number(337.0 / 365.0),
            ),
            (
                "=YEARFRAC(DATE(2000,1,1),DATE(2001,7,1),1)",
                Number(547.0 / 365.5),
            ),
            (
                "=YEARFRAC(DATE(2001,7,1),DATE(2000,1,1),1)",
                Number(547.0 / 365.5),
            ),
        ],
    );
}

/// DAYS360 with the US method: a start date on the last day of its month
/// becomes the 30th; an end date on the 31st becomes the 1st of the next
/// month when the start day is before the 30th, otherwise the 30th. An end
/// date on the 30th or on February 28 stays as it is. Sources: Microsoft
/// support, "DAYS360 function"
/// (https://support.microsoft.com/en-us/office/days360-function-b9a509fd-49ef-407e-94df-0cbda5718c2a)
/// and Excel's cached `DAYS360(11/24/2001, 11/30/2001)` = 6 in a real workbook.
#[test]
fn days360_us_month_end_rules_match_excel() {
    check(
        DateSystem::Excel1900,
        &[
            ("=DAYS360(DATE(2001,11,24),DATE(2001,11,30))", Number(6.0)),
            ("=DAYS360(DATE(2001,1,15),DATE(2001,2,28))", Number(43.0)),
            ("=DAYS360(DATE(2001,1,15),DATE(2001,3,31))", Number(76.0)),
            ("=DAYS360(DATE(2011,1,31),DATE(2011,2,28))", Number(28.0)),
            ("=DAYS360(DATE(2001,2,28),DATE(2001,3,31))", Number(30.0)),
            ("=DAYS360(DATE(2001,11,30),DATE(2001,11,24))", Number(-6.0)),
            (
                "=DAYS360(DATE(2011,1,31),DATE(2011,2,28),TRUE)",
                Number(28.0),
            ),
        ],
    );
}

/// HOUR, MINUTE and SECOND read the time of day rounded to the nearest
/// second, so a serial a hair below a whole hour is that hour; a time that
/// rounds to 24:00:00 is hour 0. Source: Excel's cached `HOUR` results in a
/// real workbook (`HOUR(37054.54166666666)` = 13), consistent with how Excel
/// displays times ("Format numbers as dates or times", above).
#[test]
fn time_parts_round_to_the_nearest_second() {
    check(
        DateSystem::Excel1900,
        &[
            ("=HOUR(37054.541666666664)", Number(13.0)),
            ("=HOUR(37049.791666666664)", Number(19.0)),
            ("=HOUR(13/24)", Number(13.0)),
            ("=HOUR(0.99999999)", Number(0.0)),
            ("=MINUTE(0.520833333)", Number(30.0)),
            ("=SECOND(0.5+0.6/86400)", Number(1.0)),
            ("=SECOND(0.5+0.4/86400)", Number(0.0)),
            ("=HOUR(-0.5)", Error(ExcelErrorKind::Num)),
        ],
    );
}

/// Text that only Rust's float parser reads as a number (`NaN`, `inf`,
/// `infinity`) is text to Excel, so arithmetic on it is `#VALUE!`. Source:
/// Microsoft support, "How to correct a #VALUE! error"
/// (https://support.microsoft.com/en-us/office/how-to-correct-a-value-error-15e1b616-fbf2-4147-9c0b-0a11a20e409e)
/// and Excel's cached `#VALUE!` for `0.0131*C199` with C199 = "NaN".
#[test]
fn non_finite_number_spellings_are_text() {
    check(
        DateSystem::Excel1900,
        &[
            ("=0.0131*A4", Error(ExcelErrorKind::Value)),
            ("=0.0131*\"NaN\"", Error(ExcelErrorKind::Value)),
            ("=1*\"inf\"", Error(ExcelErrorKind::Value)),
            ("=1*\"-Infinity\"", Error(ExcelErrorKind::Value)),
            ("=1*\"+inf\"", Error(ExcelErrorKind::Value)),
            ("=1*\"nan%\"", Error(ExcelErrorKind::Value)),
            ("=1*\"1e400\"", Error(ExcelErrorKind::Value)),
            ("=VALUE(\"NaN\")", Error(ExcelErrorKind::Value)),
            ("=SUM(\"inf\")", Error(ExcelErrorKind::Value)),
            ("=1*\"1e5\"", Number(100000.0)),
        ],
    );
}

/// VALUE and DATEVALUE read date text with Excel's
/// two-digit-year rule (00-29 is 2000-2029, 30-99 is 1930-1999). Sources:
/// Microsoft support, "VALUE function"
/// (https://support.microsoft.com/en-us/office/value-function-257d0108-07dc-437d-ae1c-bc2d3953d8c2:
/// "text can be in any of the constant number, date, or time formats"),
/// "DATEVALUE function"
/// (https://support.microsoft.com/en-us/office/datevalue-function-df8b07d4-7761-4a93-bc33-b7471bbff252)
/// and "How Excel works with two-digit year numbers"
/// (https://learn.microsoft.com/en-us/troubleshoot/microsoft-365-apps/excel/two-digit-year-numbers).
#[test]
fn date_text_coerces_with_the_two_digit_year_rule() {
    check(
        DateSystem::Excel1900,
        &[
            ("=VALUE(\"12/31/00\")", Number(36891.0)),
            ("=36981-VALUE(\"12/31/00\")", Number(90.0)),
            ("=VALUE(\"1/1/29\")=DATE(2029,1,1)", Number(1.0)),
            ("=VALUE(\"1/1/30\")=DATE(1930,1,1)", Number(1.0)),
            ("=VALUE(\"22-May-11\")=DATE(2011,5,22)", Number(1.0)),
            ("=VALUE(\"May 22, 2011\")=DATE(2011,5,22)", Number(1.0)),
            ("=VALUE(\"12:00\")", Number(0.5)),
            ("=VALUE(\"1/1/2001 12:00\")", Number(36892.5)),
            ("=DATEVALUE(\"1/1/30\")=DATE(1930,1,1)", Number(1.0)),
            ("=DATEVALUE(\"04/19/01\")", Number(37000.0)),
            (
                "=DATEVALUE(\"8/22/2011 10:00 AM\")=DATE(2011,8,22)",
                Number(1.0),
            ),
            ("=DATEVALUE(\"2011/02/23\")=DATE(2011,2,23)", Number(1.0)),
            ("=VALUE(\"abc\")", Error(ExcelErrorKind::Value)),
        ],
    );
}
