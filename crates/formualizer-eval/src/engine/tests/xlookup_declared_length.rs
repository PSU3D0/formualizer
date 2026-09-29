//! XLOOKUP answers `#VALUE!` when its lookup and return arrays declare
//! different lengths along the lookup axis, through the engine's
//! stored-formula path. Expected values were measured in Excel for the web
//! (Microsoft 365) on the same grid.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

enum Expected {
    Number(f64),
    Text(&'static str),
    Value,
}

use Expected::{Number, Text, Value};

// A1:A5 = 1..5, B1:B5 = 10..50 (A6, B6 and column E blank);
// A8:C8 = 1, 2, 3 and A9:D9 = 10, 20, 30, 40.
const CASES: &[(&str, Expected)] = &[
    // Declared lengths differ: #VALUE!, whatever the data and if_not_found.
    ("=XLOOKUP(2,A1:A3,B1:B4)", Value),
    ("=XLOOKUP(2,A1:A3,B1:B4,\"nf\")", Value),
    ("=XLOOKUP(9,A1:A3,B1:B4,\"nf\")", Value),
    ("=XLOOKUP(2,A1:A3,B1:B4,,0,-1)", Value),
    ("=XLOOKUP(2,A1:A3,B1:B4,,2)", Value),
    ("=XLOOKUP(2,A1:A3,B1:B2)", Value),
    ("=XLOOKUP(2,A1:A6,B1:B5)", Value),
    ("=XLOOKUP(2,A:A,B1:B10)", Value),
    ("=XLOOKUP(2,A8:C8,A9:D9)", Value),
    ("=XLOOKUP(2,8:8,A9:D9)", Value),
    ("=XLOOKUP(2,A1:A3,B1:C4)", Value),
    ("=XLOOKUP(2,A1:A3,{10;20})", Value),
    ("=XLOOKUP(2,{1;2;3},B1:B4)", Value),
    ("=XLOOKUP(2,A1:A3,B1:B4*1)", Value),
    ("=XLOOKUP(2,E1:E3,B1:B4,\"nf\")", Value),
    ("=XLOOKUP(2,OFFSET(A1,0,0,3,1),B1:B4)", Value),
    ("=XLOOKUP(2,IF(TRUE,A1:A3),B1:B4)", Value),
    ("=LET(r,A1:A3,XLOOKUP(2,r,B1:B4))", Value),
    (
        "=IFERROR(XLOOKUP(2,A1:A3,B1:B4),\"caught\")",
        Text("caught"),
    ),
    // Equal declared lengths search as before.
    ("=XLOOKUP(2,A1:A5,B1:B5)", Number(20.0)),
    ("=XLOOKUP(2,A1:A6,B1:B6)", Number(20.0)),
    ("=XLOOKUP(2,A:A,B:B)", Number(20.0)),
    ("=XLOOKUP(2,A8:C8,A9:C9)", Number(20.0)),
    ("=XLOOKUP(2,8:8,9:9)", Number(20.0)),
    ("=XLOOKUP(2,A1:A3,B2:B4)", Number(30.0)),
    // A single-cell lookup array searches down one row; the return row spills.
    ("=XLOOKUP(2,A2,B2:C2)", Number(20.0)),
    ("=INDEX(XLOOKUP(2,A1:A3,B1:C3),1,1)", Number(20.0)),
    ("=XLOOKUP(2,A1:A3,{10;20;30})", Number(20.0)),
    ("=LET(r,A1:A3,XLOOKUP(2,r,B1:B3))", Number(20.0)),
    ("=XLOOKUP(2,E:E,B:B,\"nf\")", Text("nf")),
];

#[test]
fn xlookup_declared_length_mismatch_matches_excel() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for row in 1..=5u32 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", row, 2, LiteralValue::Number(10.0 * row as f64))
            .unwrap();
    }
    for (col, value) in [(1u32, 1.0), (2, 2.0), (3, 3.0)] {
        engine
            .set_cell_value("Sheet1", 8, col, LiteralValue::Number(value))
            .unwrap();
    }
    for (col, value) in [(1u32, 10.0), (2, 20.0), (3, 30.0), (4, 40.0)] {
        engine
            .set_cell_value("Sheet1", 9, col, LiteralValue::Number(value))
            .unwrap();
    }
    // Formulas go in column J from row 11, clear of the rows 8 and 9 above.
    for (index, (formula, _)) in CASES.iter().enumerate() {
        engine
            .set_cell_formula("Sheet1", index as u32 + 11, 10, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();

    for (index, (formula, expected)) in CASES.iter().enumerate() {
        let actual = engine
            .get_cell_value("Sheet1", index as u32 + 11, 10)
            .unwrap_or(LiteralValue::Empty);
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
            Value => match actual {
                LiteralValue::Error(error) => {
                    assert_eq!(error.kind, ExcelErrorKind::Value, "{formula}")
                }
                other => panic!("{formula}: expected #VALUE!, got {other:?}"),
            },
        }
    }
}
