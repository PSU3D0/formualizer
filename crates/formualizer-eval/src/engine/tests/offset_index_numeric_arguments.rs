//! OFFSET and INDEX position and size arguments coerce like Excel, through
//! the engine's stored-formula path. Expected values were measured in Excel
//! for the web (Microsoft 365) on the same grid.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

#[derive(Clone, Copy, Debug)]
enum Expected {
    Number(f64),
    Error(ExcelErrorKind),
}

use Expected::{Error, Number};

const CASES: &[(&str, Expected)] = &[
    // Blank, boolean and numeric-text offsets.
    ("=OFFSET(A1,0,B1)", Number(7.0)),
    ("=OFFSET(A1,B1,0)", Number(7.0)),
    ("=OFFSET(A1,\"1\",0)", Number(8.0)),
    ("=OFFSET(A1,C1,0)", Number(8.0)),
    ("=OFFSET(A1,TRUE,0)", Number(8.0)),
    ("=OFFSET(A1,E1,0)", Number(8.0)),
    ("=OFFSET(A1,\" 1 \",0)", Number(8.0)),
    ("=OFFSET(A1,\"1e0\",0)", Number(8.0)),
    ("=OFFSET(A1,\"50%\",0)", Number(7.0)),
    ("=OFFSET(A1,1.9,0)", Number(8.0)),
    ("=OFFSET(A3,-1.5,0)", Number(8.0)),
    // Blank, boolean and numeric-text sizes.
    ("=SUM(OFFSET(A1,0,0,\"2\",1))", Number(15.0)),
    ("=SUM(OFFSET(A1,0,0,TRUE,1))", Number(7.0)),
    ("=SUM(OFFSET(A1,0,0,C1,1))", Number(7.0)),
    ("=OFFSET(A1,0,0,B1,1)", Error(ExcelErrorKind::Ref)),
    ("=OFFSET(A1,0,0,1,B1)", Error(ExcelErrorKind::Ref)),
    // Text that is not a number, and error arguments.
    ("=OFFSET(A1,0,\"x\")", Error(ExcelErrorKind::Value)),
    ("=OFFSET(A1,\"TRUE\",0)", Error(ExcelErrorKind::Value)),
    ("=OFFSET(A1,\"inf\",0)", Error(ExcelErrorKind::Value)),
    ("=OFFSET(A1,0,0,\"x\",1)", Error(ExcelErrorKind::Value)),
    ("=OFFSET(A1,0,D1)", Error(ExcelErrorKind::Div)),
    // Off the sheet.
    ("=OFFSET(A1,-1,0)", Error(ExcelErrorKind::Ref)),
    ("=OFFSET(A1,2000000,0)", Error(ExcelErrorKind::Ref)),
    ("=OFFSET(A1,1E+300,0)", Error(ExcelErrorKind::Ref)),
    ("=OFFSET(A1,1048575,0,2,1)", Error(ExcelErrorKind::Ref)),
    ("=OFFSET(A1,0,20000)", Error(ExcelErrorKind::Ref)),
    ("=ROW(OFFSET(A1,1048575,16383))", Number(1048576.0)),
    ("=COLUMN(OFFSET(A1,1048575,16383))", Number(16384.0)),
    // INDEX positions.
    ("=SUM(INDEX(A1:A3,B1))", Number(24.0)),
    ("=INDEX(A1:A3,\"2\")", Number(8.0)),
    ("=INDEX(A1:A3,\" 2 \")", Number(8.0)),
    ("=INDEX(A1:A3,TRUE)", Number(7.0)),
    ("=INDEX(A1:A3,C1)", Number(7.0)),
    ("=INDEX(A1:B3,2,\"2\")", Number(80.0)),
    ("=SUM(INDEX(A1:B3,2,B1))", Number(88.0)),
    ("=INDEX({7;8;9},\"2\")", Number(8.0)),
    ("=SUM(INDEX({7;8;9},B1))", Number(24.0)),
    ("=INDEX(A1:A3,\"x\")", Error(ExcelErrorKind::Value)),
    // Positions of 2^32 or more must not wrap back into range.
    ("=INDEX(A1:A3,4294967297)", Error(ExcelErrorKind::Ref)),
    ("=INDEX(A1:A3,1E+300)", Error(ExcelErrorKind::Ref)),
    ("=INDEX({7;8;9},4294967297)", Error(ExcelErrorKind::Ref)),
];

#[test]
fn offset_and_index_numeric_arguments_match_excel() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    // A1:A3 = 7, 8, 9; B2:B3 = 80, 90; B1 blank; C1 = text "1"; D1 = #DIV/0!;
    // E1 = TRUE.
    let values = [
        (1, 1, LiteralValue::Number(7.0)),
        (2, 1, LiteralValue::Number(8.0)),
        (3, 1, LiteralValue::Number(9.0)),
        (2, 2, LiteralValue::Number(80.0)),
        (3, 2, LiteralValue::Number(90.0)),
        (1, 3, LiteralValue::Text("1".into())),
        (1, 5, LiteralValue::Boolean(true)),
    ];
    for (row, col, value) in values {
        engine.set_cell_value("Sheet1", row, col, value).unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=1/0").unwrap())
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
            Number(value) => {
                let number = match actual {
                    LiteralValue::Number(number) => number,
                    LiteralValue::Int(integer) => integer as f64,
                    other => panic!("{formula}: expected {value}, got {other:?}"),
                };
                assert_eq!(number, *value, "{formula}");
            }
            Error(kind) => match actual {
                LiteralValue::Error(error) => assert_eq!(error.kind, *kind, "{formula}"),
                other => panic!("{formula}: expected {kind:?}, got {other:?}"),
            },
        }
    }
}
