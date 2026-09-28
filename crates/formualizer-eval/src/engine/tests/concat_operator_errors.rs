//! The `&` operator returns an error operand instead of spelling it into the
//! text, and works element by element over arrays and ranges. Expected values
//! were measured in Excel for the web (Microsoft 365) on the same grid.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

enum Expected {
    Number(f64),
    Boolean(bool),
    Text(&'static str),
    Error(ExcelErrorKind),
}

use Expected::{Boolean, Error, Number, Text};

// A1 = #DIV/0!, A2 = 5, A3 = #N/A, A4 = "x", A5 = "y"; B1 blank.
const CASES: &[(&str, Expected)] = &[
    ("=\"a\"&1/0", Error(ExcelErrorKind::Div)),
    ("=A1&\"x\"", Error(ExcelErrorKind::Div)),
    ("=\"x\"&A1", Error(ExcelErrorKind::Div)),
    ("=A2&A1", Error(ExcelErrorKind::Div)),
    // Both operands are errors: the left one wins.
    ("=A1&A3", Error(ExcelErrorKind::Div)),
    ("=A3&A1", Error(ExcelErrorKind::Na)),
    ("=1/0&NA()", Error(ExcelErrorKind::Div)),
    ("=NA()&1/0", Error(ExcelErrorKind::Na)),
    ("=\"a\"&\"b\"&A1", Error(ExcelErrorKind::Div)),
    ("=LEN(\"a\"&A1)", Error(ExcelErrorKind::Div)),
    ("=ISERROR(\"a\"&A1)", Boolean(true)),
    ("=IFERROR(\"a\"&A1,\"caught\")", Text("caught")),
    // Unchanged for non-error operands.
    ("=\"a\"&B1", Text("a")),
    ("=TRUE&1", Text("TRUE1")),
    ("=A2&\"\"", Text("5")),
    // Element by element over arrays and ranges.
    ("=TEXTJOIN(\",\",,{\"a\",\"b\"}&\"x\")", Text("ax,bx")),
    ("=TEXTJOIN(\",\",,A4:A5&\"!\")", Text("x!,y!")),
    (
        "=IFERROR(TEXTJOIN(\",\",,A1:A2&\"!\"),\"err\")",
        Text("err"),
    ),
    ("=INDEX(A1:A2&\"!\",2)", Text("5!")),
    (
        "=TEXTJOIN(\",\",,{\"a\",\"b\";\"c\",\"d\"}&{\"1\",\"2\";\"3\",\"4\"})",
        Text("a1,b2,c3,d4"),
    ),
    ("=LEN(\"a\"&A2)", Number(2.0)),
];

fn grid_engine(config: EvalConfig) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=1/0").unwrap())
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=NA()").unwrap())
        .unwrap();
    engine
        .set_cell_value("Sheet1", 4, 1, LiteralValue::Text("x".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Text("y".into()))
        .unwrap();
    engine
}

fn assert_expected(actual: LiteralValue, expected: &Expected, formula: &str) {
    match expected {
        Number(value) => {
            let number = match actual {
                LiteralValue::Number(number) => number,
                LiteralValue::Int(integer) => integer as f64,
                other => panic!("{formula}: expected {value}, got {other:?}"),
            };
            assert_eq!(number, *value, "{formula}");
        }
        Boolean(value) => assert_eq!(actual, LiteralValue::Boolean(*value), "{formula}"),
        Text(text) => assert_eq!(actual, LiteralValue::Text((*text).into()), "{formula}"),
        Error(kind) => match actual {
            LiteralValue::Error(error) => assert_eq!(error.kind, *kind, "{formula}"),
            other => panic!("{formula}: expected {kind:?}, got {other:?}"),
        },
    }
}

#[test]
fn concat_operator_errors_and_arrays_match_excel() {
    let mut engine = grid_engine(EvalConfig::default());
    for (index, (formula, _)) in CASES.iter().enumerate() {
        engine
            .set_cell_formula("Sheet1", index as u32 + 1, 10, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    for (index, (formula, expected)) in CASES.iter().enumerate() {
        let actual = engine
            .get_cell_value("Sheet1", index as u32 + 1, 10)
            .unwrap_or(LiteralValue::Empty);
        assert_expected(actual, expected, formula);
    }
}

/// Arrays of different shapes take the engine's existing broadcast rule and
/// give `#VALUE!` for the whole result. This pins current behaviour, not
/// Excel's: Excel pads the result and fills the unmatched cells with `#N/A`,
/// so `=INDEX({"a","b"}&{"1","2","3"},1,1)` is `a1` and the `TEXTJOIN` below
/// is `#N/A` there.
#[test]
fn concat_operator_shape_mismatch_is_value() {
    let mut engine = grid_engine(EvalConfig::default());
    for (row, formula) in [
        (1, "=TEXTJOIN(\",\",,{\"a\",\"b\"}&{\"1\",\"2\",\"3\"})"),
        (2, "=INDEX({\"a\",\"b\"}&{\"1\",\"2\",\"3\"},1,1)"),
    ] {
        engine
            .set_cell_formula("Sheet1", row, 10, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    for row in 1..=2 {
        let actual = engine
            .get_cell_value("Sheet1", row, 10)
            .unwrap_or(LiteralValue::Empty);
        assert_expected(actual, &Error(ExcelErrorKind::Value), &format!("J{row}"));
    }
}

/// A filled-down `=A{r}&B{r}` family over 120 rows evaluates through the
/// elementwise lift when family execution is on (the lift runs on the
/// recalculation after an edit) and through the per-cell walk when it is
/// off; both return the error operand for the error rows.
#[test]
fn concat_operator_errors_in_a_filled_down_family() {
    const ROWS: u32 = 120;
    let a = |row: u32| match row % 4 {
        0 => LiteralValue::Error(formualizer_common::ExcelError::new(ExcelErrorKind::Div)),
        1 => LiteralValue::Number(row as f64),
        2 => LiteralValue::Error(formualizer_common::ExcelError::new(ExcelErrorKind::Na)),
        _ => LiteralValue::Text(format!("t{row}")),
    };
    let check =
        |engine: &Engine<TestWorkbook>, suffix: &dyn Fn(u32) -> &'static str, label: &str| {
            for row in 1..=ROWS {
                let actual = engine
                    .get_cell_value("Sheet1", row, 3)
                    .unwrap_or(LiteralValue::Empty);
                match (a(row), actual) {
                    (LiteralValue::Error(expected), LiteralValue::Error(actual)) => {
                        assert_eq!(actual.kind, expected.kind, "C{row} {label}")
                    }
                    (LiteralValue::Number(number), actual) => assert_eq!(
                        actual,
                        LiteralValue::Text(format!("{number}{}", suffix(row))),
                        "C{row} {label}"
                    ),
                    (LiteralValue::Text(text), actual) => assert_eq!(
                        actual,
                        LiteralValue::Text(format!("{text}{}", suffix(row))),
                        "C{row} {label}"
                    ),
                    (expected, actual) => {
                        panic!("C{row} {label}: expected {expected:?}, got {actual:?}")
                    }
                }
            }
        };
    for family_execution in [true, false] {
        let label = format!("family_execution={family_execution}");
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_execution,
                enable_parallel: false,
                ..super::common::arrow_eval_config()
            },
        );
        for row in 1..=ROWS {
            engine.set_cell_value("Sheet1", row, 1, a(row)).unwrap();
            engine
                .set_cell_value("Sheet1", row, 2, LiteralValue::Text("!".into()))
                .unwrap();
        }
        for row in 1..=ROWS {
            engine
                .set_cell_formula("Sheet1", row, 3, parse(format!("=A{row}&B{row}")).unwrap())
                .unwrap();
        }
        engine.evaluate_all().unwrap();
        check(&engine, &|_| "!", &label);

        engine
            .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("?".into()))
            .unwrap();
        engine
            .set_cell_value("Sheet1", ROWS, 2, LiteralValue::Text("?".into()))
            .unwrap();
        engine.evaluate_all().unwrap();
        check(
            &engine,
            &|row| if row == 1 || row == ROWS { "?" } else { "!" },
            &label,
        );
        assert_eq!(
            engine.lifted_members_for_test() > 0,
            family_execution,
            "lifted members, {label}"
        );
    }
}

/// The AST walk (`Interpreter::evaluate_ast`) takes the same rule.
#[test]
fn concat_operator_errors_on_the_ast_path() {
    let wb = TestWorkbook::new()
        .with_cell_a1("Sheet1", "A4", LiteralValue::Text("x".into()))
        .with_cell_a1("Sheet1", "A5", LiteralValue::Text("y".into()));
    let interpreter = wb.interpreter();
    let eval = |formula: &str| {
        interpreter
            .evaluate_ast(&parse(formula).unwrap())
            .unwrap()
            .into_literal()
    };
    for (formula, kind) in [
        ("=\"a\"&1/0", ExcelErrorKind::Div),
        ("=1/0&#N/A", ExcelErrorKind::Div),
        ("=#N/A&1/0", ExcelErrorKind::Na),
    ] {
        match eval(formula) {
            LiteralValue::Error(error) => assert_eq!(error.kind, kind, "{formula}"),
            other => panic!("{formula}: expected {kind:?}, got {other:?}"),
        }
    }
    assert_eq!(
        eval("={\"a\",\"b\"}&\"x\""),
        LiteralValue::Array(vec![vec![
            LiteralValue::Text("ax".into()),
            LiteralValue::Text("bx".into()),
        ]])
    );
    assert_eq!(
        eval("=A4:A5&\"!\""),
        LiteralValue::Array(vec![
            vec![LiteralValue::Text("x!".into())],
            vec![LiteralValue::Text("y!".into())],
        ])
    );
}
