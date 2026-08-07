use crate::engine::{DateSystem, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

#[derive(Clone, Copy, Debug)]
enum Expected {
    Number(f64),
    Boolean(bool),
    Text(&'static str),
    Error(ExcelErrorKind),
}

fn eval_formula(system: DateSystem, formula: &str) -> LiteralValue {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_date_system(system),
    );
    engine
        .set_cell_formula("Sheet1", 1, 1, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine
        .get_cell_value("Sheet1", 1, 1)
        .unwrap_or(LiteralValue::Empty)
}

fn assert_expected(system: DateSystem, formula: &str, oracle: &str, expected: Expected) {
    let actual = eval_formula(system, formula);
    match expected {
        Expected::Number(value) => {
            assert_eq!(actual, LiteralValue::Number(value), "{formula} ({oracle})")
        }
        Expected::Boolean(value) => {
            assert_eq!(actual, LiteralValue::Boolean(value), "{formula} ({oracle})")
        }
        Expected::Text(value) => assert_eq!(
            actual,
            LiteralValue::Text(value.to_string()),
            "{formula} ({oracle})"
        ),
        Expected::Error(kind) => match actual {
            LiteralValue::Error(error) => assert_eq!(error.kind, kind, "{formula} ({oracle})"),
            other => panic!("{formula} ({oracle}): expected {kind:?}, got {other:?}"),
        },
    }
}

#[test]
fn date_time_text_arithmetic_oracle_table() {
    let cases = [
        ("=\"1/1/03\"-\"6/01/2002\"", Expected::Number(214.0)),
        ("=\"1/1/2003\"-\"6/1/2002\"", Expected::Number(214.0)),
        ("=\"1/1/03\"+0", Expected::Number(37_622.0)),
        ("=-\"1/1/03\"", Expected::Number(-37_622.0)),
        ("=\"1/1/03\"*1", Expected::Number(37_622.0)),
        ("=\"1/1/03\"/1", Expected::Number(37_622.0)),
        ("=\"1/1/03\"^1", Expected::Number(37_622.0)),
        ("=\"1/1/03\"%", Expected::Number(376.22)),
        ("=\"12:00\"-\"6:00\"", Expected::Number(0.25)),
        ("=\"1/1/03 12:00\"+0", Expected::Number(37_622.5)),
        ("=ISNUMBER(\"1/1/03\"+0)", Expected::Boolean(true)),
    ];

    for (formula, expected) in cases {
        assert_expected(
            DateSystem::Excel1900,
            formula,
            "oracle: lo-verified",
            expected,
        );
    }
}

#[test]
fn date_text_arithmetic_honors_workbook_date_system() {
    let cases = [
        (
            DateSystem::Excel1900,
            "=\"1/1/03\"+0",
            Expected::Number(37_622.0),
        ),
        (
            DateSystem::Excel1904,
            "=\"1/1/03\"+0",
            Expected::Number(36_160.0),
        ),
        (
            DateSystem::Excel1900,
            "=\"1/1/03 12:00\"+0",
            Expected::Number(37_622.5),
        ),
        (
            DateSystem::Excel1904,
            "=\"1/1/03 12:00\"+0",
            Expected::Number(36_160.5),
        ),
        (
            DateSystem::Excel1900,
            "=\"1/1/29\"+0",
            Expected::Number(47_119.0),
        ),
        (
            DateSystem::Excel1904,
            "=\"1/1/29\"+0",
            Expected::Number(45_657.0),
        ),
        (
            DateSystem::Excel1900,
            "=\"1/1/30\"+0",
            Expected::Number(10_959.0),
        ),
        (
            DateSystem::Excel1904,
            "=\"1/1/30\"+0",
            Expected::Number(9_497.0),
        ),
    ];

    for (system, formula, expected) in cases {
        assert_expected(system, formula, "oracle: lo-verified", expected);
    }
}

#[test]
fn invalid_date_time_text_remains_value_error() {
    let cases = [
        "=\"2/30/03\"+0",
        "=\"abc\"+0",
        "=\"\"+0",
        "=\"13/13/13\"+0",
        "=\"123-456\"+0",
    ];

    for system in [DateSystem::Excel1900, DateSystem::Excel1904] {
        for formula in cases {
            assert_expected(
                system,
                formula,
                "oracle: lo-verified",
                Expected::Error(ExcelErrorKind::Value),
            );
        }
    }
}

#[test]
fn non_arithmetic_text_semantics_are_unchanged() {
    let cases = [
        ("=\"5\"+\"3\"", Expected::Number(8.0)),
        ("=\"5\"-\"3\"", Expected::Number(2.0)),
        (
            "=SUM(\"1/1/03\",\"1\")",
            Expected::Error(ExcelErrorKind::Value),
        ),
        ("=N(\"1/1/03\")", Expected::Number(0.0)),
        ("=T(\"1/1/03\")", Expected::Text("1/1/03")),
        ("=\"1/1/03\"&\"\"", Expected::Text("1/1/03")),
        ("=+\"1/1/03\"", Expected::Text("1/1/03")),
        ("=\"1/1/03\"=37622", Expected::Boolean(false)),
    ];

    for (formula, expected) in cases {
        assert_expected(
            DateSystem::Excel1900,
            formula,
            "oracle: lo-verified",
            expected,
        );
    }
}
