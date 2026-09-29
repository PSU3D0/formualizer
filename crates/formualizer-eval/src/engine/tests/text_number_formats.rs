//! `TEXT` renders number formats like Excel. Expected values were measured in
//! Excel for the web (Microsoft 365) on the same grid: A1 = 0.5, A2 = "0.5",
//! Z1 blank.
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

enum Expected {
    Number(f64),
    Text(&'static str),
    ValueError,
}
use Expected::{Number, Text, ValueError};

const CASES: &[(&str, Expected)] = &[
    ("=TEXT(0.09,\"0.00%\")", Text("9.00%")),
    ("=TEXT(0.095,\"0.00%\")", Text("9.50%")),
    ("=TEXT(0.0975,\"0.00%\")", Text("9.75%")),
    ("=TEXT(0.09754,\"0.00%\")", Text("9.75%")),
    ("=TEXT(0.09756,\"0.00%\")", Text("9.76%")),
    ("=TEXT(-0.0975,\"0.00%\")", Text("-9.75%")),
    ("=TEXT(0,\"0.00%\")", Text("0.00%")),
    ("=TEXT(1,\"0.00%\")", Text("100.00%")),
    ("=TEXT(0.256,\"0%\")", Text("26%")),
    ("=TEXT(12.3,\"0.00\")", Text("12.30")),
    ("=TEXT(0.09,\"0.000\")", Text("0.090")),
    ("=TEXT(1.23456,\"0.000\")", Text("1.235")),
    ("=TEXT(7,\"0000\")", Text("0007")),
    ("=TEXT(12345.6,\"#,##0.00\")", Text("12,345.60")),
    ("=TEXT(-12345.6,\"#,##0.00\")", Text("-12,345.60")),
    ("=TEXT(1234567,\"#,##0\")", Text("1,234,567")),
    ("=TEXT(999.999,\"#,##0.00\")", Text("1,000.00")),
    ("=TEXT(0.5,\"#,##0.00\")", Text("0.50")),
    ("=TEXT(12,\"#,###\")", Text("12")),
    ("=TEXT(0,\"#,###\")", Text("")),
    ("=TEXT(0.5,\"#.##\")", Text(".5")),
    ("=TEXT(0,\"#\")", Text("")),
    ("=TEXT(0,\"0\")", Text("0")),
    ("=TEXT(3,\"#.00\")", Text("3.00")),
    ("=TEXT(0.5,\"0.0#\")", Text("0.5")),
    ("=TEXT(0.567,\"0.0#\")", Text("0.57")),
    ("=TEXT(0.5,\".00\")", Text(".50")),
    ("=TEXT(-0.5,\"#.00\")", Text("-.50")),
    ("=TEXT(0.5,\"0.00\")", Text("0.50")),
    ("=TEXT(12200000,\"0.0,,\")", Text("12.2")),
    ("=TEXT(1234567,\"#,##0,\")", Text("1,235")),
    ("=TEXT(999,\"0,\")", Text("1")),
    ("=TEXT(0.09,\"0.00% Cap\")", Text("9.00% Cap")),
    ("=TEXT(0.09,\"0.00\\%\")", Text("0.09%")),
    (
        "=TEXT(0.09,\"0.00\"&CHAR(34)&\"%\"&CHAR(34))",
        Text("0.09%"),
    ),
    (
        "=TEXT(0.09,\"0.00% \"&CHAR(34)&\"Cap\"&CHAR(34))",
        Text("9.00% Cap"),
    ),
    (
        "=TEXT(0.09,CHAR(34)&\"Cap \"&CHAR(34)&\"0.00%\")",
        Text("Cap 9.00%"),
    ),
    (
        "=TEXT(7,\"0\"&CHAR(34)&\";units\"&CHAR(34))",
        Text("7;units"),
    ),
    ("=TEXT(0.001,\"0.00%.\")", Text("0.10%.")),
    ("=TEXT(5,\"$0.00\")", Text("$5.00")),
    ("=TEXT(-5,\"$#,##0.00\")", Text("-$5.00")),
    ("=TEXT(5,\"0 units\")", ValueError),
    ("=TEXT(5,\"0 kg\")", ValueError),
    ("=TEXT(5,\"(0)\")", Text("(5)")),
    (
        "=TEXT(-1234.5,\"#,##0.00;[Red](#,##0.00)\")",
        Text("(1,234.50)"),
    ),
    (
        "=TEXT(0,\"0.00;[Red]-0.00;\"&CHAR(34)&\"zero\"&CHAR(34))",
        Text("zero"),
    ),
    (
        "=TEXT(0.004,\"0.00;[Red]-0.00;\"&CHAR(34)&\"zero\"&CHAR(34))",
        Text("0.00"),
    ),
    ("=TEXT(-0.004,\"0.00\")", Text("0.00")),
    ("=TEXT(-0.004,\"0.00;(0.00)\")", Text("(0.00)")),
    ("=TEXT(-1,\"0;;\")", Text("")),
    ("=TEXT(0,\"0;-0;\")", Text("")),
    ("=TEXT(-5,\"0;\")", Text("")),
    ("=TEXT(1,\"[Blue]0.0\")", Text("1.0")),
    ("=TEXT(-1,\"0.0;[Red]0.0\")", Text("1.0")),
    ("=TEXT(-0.5,\"0%;(0%)\")", Text("(50%)")),
    ("=TEXT(0,\"0;(0)\")", Text("0")),
    ("=TEXT(1.005,\"0.00\")", Text("1.01")),
    ("=TEXT(0.145,\"0.00\")", Text("0.15")),
    ("=TEXT(2.675,\"0.00\")", Text("2.68")),
    ("=TEXT(44821.875,\"0.00\")", Text("44821.88")),
    ("=TEXT(8.835,\"0.00\")", Text("8.84")),
    ("=TEXT(1.5,\"0\")", Text("2")),
    ("=TEXT(2.5,\"0\")", Text("3")),
    ("=TEXT(-2.5,\"0\")", Text("-3")),
    ("=TEXT(1.225,\"0.00\")", Text("1.23")),
    ("=LEN(TEXT(1E+100,\"0\"))", Number(101.0)),
    ("=LEFT(TEXT(1E+100,\"0\"),20)", Text("10000000000000000000")),
    ("=TEXT(1E+20,\"0.00\")", Text("100000000000000000000.00")),
    ("=LEN(TEXT(1E+254,\"0\"))", Number(255.0)),
    ("=TEXT(1E+255,\"0\")", ValueError),
    ("=LEN(TEXT(1E+252,\"0\"))", Number(253.0)),
    ("=TEXT(1E+252,\"0.00\")", ValueError),
    ("=TEXT(1E+307,\"0.00\")", ValueError),
    ("=TEXT(-1E+307,\"0.00\")", ValueError),
    ("=TEXT(1E+307,\"0.00%\")", ValueError),
    (
        "=TEXT(1,REPT(\"0\",40))",
        Text("0000000000000000000000000000000000000001"),
    ),
    ("=TEXT(1,REPT(\"0\",300))", ValueError),
    ("=TEXT(1,\"0m\")", ValueError),
    ("=TEXT(45000,\"0.00m\")", ValueError),
    ("=TEXT(5,\"0 mm\")", ValueError),
    ("=TEXT(1,\"0\"&CHAR(34)&\"m\"&CHAR(34))", Text("1m")),
    ("=TEXT(1,\"0\\m\")", Text("1m")),
    ("=TEXT(A2,\"0.00%\")", Text("50.00%")),
    ("=TEXT(Z1,\"0.00\")", Text("0.00")),
    ("=TEXT(A1,\"0.0\")", Text("0.5")),
    ("=TEXT(5,\"0 a\")", Text("5 a")),
    ("=TEXT(5,\"0 d\")", ValueError),
    ("=TEXT(5,\"s0\")", ValueError),
    ("=TEXT(5,\"K0\")", Text("K5")),
    ("=TEXT(5,\"0 Y\")", ValueError),
    ("=TEXT(-0.00004,\"0.00%\")", Text("0.00%")),
    ("=TEXT(-0.4,\"0\")", Text("0")),
    ("=TEXT(-0.4,\"#\")", Text("")),
    ("=TEXT(-0.4,\"$0\")", Text("$0")),
    ("=TEXT(-0.004,\"0.00;(0.00);\"\"zero\"\"\")", Text("(0.00)")),
    ("=TEXT(-0.004,\"#,##0.00\")", Text("0.00")),
    ("=TEXT(-0.5,\"0\")", Text("-1")),
    ("=TEXT(-0.049,\"0.0\")", Text("0.0")),
    ("=TEXT(-1E-20,\"0.00\")", Text("0.00")),
    ("=TEXT(-400,\"0,\")", Text("0")),
    ("=TEXT(5,\"0 Cap\")", Text("5 Cap")),
    ("=TEXT(5,\"0 ok\")", Text("5 ok")),
    ("=TEXT(5,\"0 !\")", Text("5 !")),
    ("=TEXT(5,\"0 ~\")", Text("5 ~")),
    ("=TEXT(5,\"0-\")", Text("5-")),
    ("=TEXT(5,\"+0\")", Text("+5")),
    ("=TEXT(5,\"0:\")", Text("5:")),
    ("=TEXT(5,\"0^\")", Text("5^")),
    ("=TEXT(5,\"0{}\")", Text("5{}")),
    ("=TEXT(5,\"0<>=\")", Text("5<>=")),
    ("=TEXT(5,\"0'\")", Text("5'")),
    ("=TEXT(5,\"0&\")", Text("5&")),
    ("=TEXT(5,\"cat\")", Text("cat")),
    ("=TEXT(5,\"x\")", Text("x")),
    ("=TEXT(0,\"0;-0;zip\")", Text("zip")),
    ("=TEXT(5,\"\"\"abc\"\"\")", Text("abc")),
    ("=TEXT(5,\"-\")", Text("-")),
    ("=TEXT(-5,\"0;-\")", Text("-")),
    ("=TEXT(5,\"General\")", Text("5")),
    ("=TEXT(5.5,\"General\")", Text("5.5")),
    ("=TEXT(0,\"0;0;\")", Text("")),
    ("=TEXT(5,\"0;0;0;@\")", Text("5")),
    ("=TEXT(5,\"0;0;0;0;0\")", ValueError),
    ("=TEXT(5,\"$\")", Text("$")),
    ("=TEXT(5,\"[Red]\")", Text("5")),
    ("=TEXT(1234.5,\"#,##0.00,\")", Text("1.23")),
    ("=TEXT(1234.5,\"0,0.0\")", Text("1,234.5")),
    ("=TEXT(5,\"0%%\")", Text("50000%%")),
    ("=TEXT(5,\"%0\")", Text("%500")),
    ("=TEXT(5,\"0 %\")", Text("500 %")),
    ("=TEXT(5,\"[BLUE]0\")", Text("5")),
    ("=TEXT(5,\"[Color10]0\")", Text("5")),
    ("=TEXT(-5,\"[Red]-0\")", Text("--5")),
    ("=TEXT(5,\"0E0\")", ValueError),
    ("=TEXT(5,\"0.0 \")", Text("5.0 ")),
    ("=TEXT(5,\" 0\")", Text(" 5")),
    ("=TEXT(5,\"0\"\"\")", ValueError),
    ("=TEXT(5,\"0\\\")", ValueError),
    ("=TEXT(0.5,\"0.#\")", Text("0.5")),
    ("=TEXT(0.05,\"0.#\")", Text("0.1")),
    ("=TEXT(10,\"#\")", Text("10")),
    ("=TEXT(10,\"#.#\")", Text("10.")),
    ("=TEXT(-1E-20,\"0.00;(0.00)\")", Text("(0.00)")),
    ("=TEXT(-0,\"0.00\")", Text("0.00")),
    ("=TEXT(1E-5,\"0.00000\")", Text("0.00001")),
    ("=TEXT(1.23E-10,\"0.000000000000\")", Text("0.000000000123")),
    (
        "=TEXT(0.1+0.2,\"0.00000000000000000\")",
        Text("0.30000000000000000"),
    ),
    (
        "=TEXT(1/3,\"0.00000000000000000000\")",
        Text("0.33333333333333300000"),
    ),
    (
        "=TEXT(2/3,\"0.00000000000000000000\")",
        Text("0.66666666666666700000"),
    ),
    ("=TEXT(2/3*1E+20,\"0\")", Text("66666666666666700000")),
    ("=TEXT(5,\"0.#\")", Text("5.")),
    ("=TEXT(3,\"#.##\")", Text("3.")),
    ("=TEXT(0,\"#.##\")", Text(".")),
    ("=TEXT(0,\"0.#\")", Text("0.")),
    ("=TEXT(-5,\"0.#\")", Text("-5.")),
    ("=TEXT(5,\"0.\")", Text("5.")),
    ("=TEXT(98765432109876.5,\"0\")", Text("98765432109877")),
    // A single literal section keeps the minus sign; `;` after `\` does not split.
    ("=TEXT(-5,\"cat\")", Text("-cat")),
    ("=TEXT(-5,\"$\")", Text("-$")),
    ("=TEXT(-5,\"-\")", Text("--")),
    ("=TEXT(-0.4,\"cat\")", Text("-cat")),
    ("=TEXT(-5,\"\"\"abc\"\"\")", Text("-abc")),
    ("=TEXT(-5,\"x\")", Text("-x")),
    ("=TEXT(7,\"0\\;x\")", Text("7;x")),
    ("=TEXT(-7,\"0\\;x\")", Text("-7;x")),
    ("=TEXT(5,\"%\")", Text("%")),
    ("=TEXT(5,\"0 %;x\")", Text("500 %")),
    ("=TEXT(0,\"0;-0;cat\")", Text("cat")),
    ("=TEXT(-5,\"0;cat\")", Text("cat")),
    ("=TEXT(5,\"[Color0]0\")", ValueError),
    ("=TEXT(5,\"[Color57]0\")", ValueError),
    ("=TEXT(-5,\"\")", Text("-")),
    ("=TEXT(-5,\"\"\"\"\"\")", Text("-")),
    ("=TEXT(5,\"\"\"\"\"\")", Text("")),
];

fn grid_engine() -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(0.5))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Text("0.5".into()))
        .unwrap();
    engine
}

fn evaluate(engine: &mut Engine<TestWorkbook>, formulas: &[&str]) -> Vec<LiteralValue> {
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

#[test]
fn text_number_formats_match_excel() {
    let mut engine = grid_engine();
    let formulas: Vec<&str> = CASES.iter().map(|(formula, _)| *formula).collect();
    let values = evaluate(&mut engine, &formulas);
    for ((formula, expected), actual) in CASES.iter().zip(values) {
        match expected {
            Number(value) => {
                let number = match actual {
                    LiteralValue::Number(number) => number,
                    LiteralValue::Int(integer) => integer as f64,
                    other => panic!("{formula}: expected {value}, got {other:?}"),
                };
                assert_eq!(number, *value, "{formula}");
            }
            Text(text) => assert_eq!(actual, LiteralValue::Text((*text).into()), "{formula}"),
            ValueError => match actual {
                LiteralValue::Error(error) => {
                    assert_eq!(error.kind, ExcelErrorKind::Value, "{formula}")
                }
                other => panic!("{formula}: expected #VALUE!, got {other:?}"),
            },
        }
    }
}

/// Codes the renderer does not handle keep their previous results. These are
/// not Excel's results (Excel gives `1.23E+04`, `1 1/4`, `123-456` and
/// `5.0`); they pin the fallback so a later change shows up here.
#[test]
fn other_format_codes_keep_their_previous_rendering() {
    let cases = [
        ("=TEXT(12345,\"0.00E+00\")", "12345.00"),
        ("=TEXT(1.25,\"# ?/?\")", "1.25"),
        ("=TEXT(123456,\"000-000\")", "123456"),
        ("=TEXT(5,\"[>=1]0.0\")", "5"),
        ("=TEXT(45306,\"yyyy-mm-dd\")", "2024-01-15"),
        ("=TEXT(45306.5,\"yyyy-mm-dd hh:mm\")", "2024-01-15 12:00"),
    ];
    let mut engine = grid_engine();
    let formulas: Vec<&str> = cases.iter().map(|(formula, _)| *formula).collect();
    let values = evaluate(&mut engine, &formulas);
    for ((formula, expected), actual) in cases.iter().zip(values) {
        assert_eq!(actual, LiteralValue::Text((*expected).into()), "{formula}");
    }
}
