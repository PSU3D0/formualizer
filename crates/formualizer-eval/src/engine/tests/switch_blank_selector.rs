//! SWITCH compares a blank cell as numeric zero, on the selector side and on
//! the case side, through the engine's stored-formula path. Expected values
//! were measured in Excel for the web (Microsoft 365) on the same grid.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

enum Expected {
    Text(&'static str),
    Error(ExcelErrorKind),
}

use Expected::{Error, Text};

// Z1 and Z2 are blank, A1 holds 0 and Y1 holds the empty text `=""`.
const CASES: &[(&str, Expected)] = &[
    ("=SWITCH(Z1,0,\"zero\",\"none\")", Text("zero")),
    ("=SWITCH(Z1,1-1,\"calczero\",\"none\")", Text("calczero")),
    ("=SWITCH(Z1,-0,\"negzero\",\"none\")", Text("negzero")),
    ("=SWITCH(Z1,A1,\"zerocell\",\"none\")", Text("zerocell")),
    ("=SWITCH(0,Z1,\"blankcase\",\"none\")", Text("blankcase")),
    ("=SWITCH(A1,Z1,\"blankcase\",\"none\")", Text("blankcase")),
    (
        "=SWITCH(Z1,\"\",\"empty\",0,\"zero\",\"none\")",
        Text("zero"),
    ),
    ("=SWITCH(Z1,0,\"a\",0,\"b\",\"none\")", Text("a")),
    // A blank case in the second position still matches a blank selector.
    (
        "=SWITCH(Z1,\"x\",\"x\",Z2,\"blank\",\"none\")",
        Text("blank"),
    ),
    ("=SWITCH(Z1,0,\"zero\")", Text("zero")),
    // Unchanged: a blank is not empty text, FALSE, text "0" or a nonzero number.
    ("=SWITCH(Z1,\"\",\"empty\",\"none\")", Text("none")),
    ("=SWITCH(Z1,FALSE,\"false\",\"none\")", Text("none")),
    ("=SWITCH(Z1,\"0\",\"textzero\",\"none\")", Text("none")),
    ("=SWITCH(Z1,1,\"one\",\"none\")", Text("none")),
    (
        "=SWITCH(Z1,0.0000000000001,\"tiny\",\"none\")",
        Text("none"),
    ),
    ("=SWITCH(\"\",Z1,\"blankcase\",\"none\")", Text("none")),
    ("=SWITCH(FALSE,Z1,\"blankcase\",\"none\")", Text("none")),
    ("=SWITCH(Z1,Z2,\"bothblank\",\"none\")", Text("bothblank")),
    ("=SWITCH(Y1,0,\"zero\",\"none\")", Text("none")),
    ("=SWITCH(Y1,\"\",\"empty\",\"none\")", Text("empty")),
    ("=SWITCH(Z1,1,\"one\")", Error(ExcelErrorKind::Na)),
    ("=SWITCH(Z1,\"\",\"e\")", Error(ExcelErrorKind::Na)),
];

#[test]
fn switch_blank_selector_and_case_match_excel() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(0.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 25, parse("=\"\"").unwrap())
        .unwrap();
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
        match expected {
            Text(text) => assert_eq!(actual, LiteralValue::Text((*text).into()), "{formula}"),
            Error(kind) => match actual {
                LiteralValue::Error(error) => assert_eq!(error.kind, *kind, "{formula}"),
                other => panic!("{formula}: expected {kind:?}, got {other:?}"),
            },
        }
    }
}
