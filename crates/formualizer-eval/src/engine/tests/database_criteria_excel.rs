//! Database functions (DSUM, DCOUNT, DGET, ...): Excel's handling of
//! criteria headers that are not database labels, and of error arguments.
//!
//! - A criteria column whose header is blank or not a database label is a
//!   computed criterion: Excel evaluates its formula per record and keeps the
//!   records for which it is TRUE. A constant that is not TRUE (text such as
//!   `"No"`, FALSE, a number) therefore matches no record; an empty cell under
//!   such a header sets no condition.
//! - A database or criteria argument that is an error (for example a defined
//!   name whose formula is `#REF!`) returns that error.

use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn text(s: &str) -> LiteralValue {
    LiteralValue::Text(s.into())
}

/// Database A1:C4 (Name, Flag, Amt): a/No/10, b/Yes/20, c/No/30.
fn build(engine: &mut Engine<TestWorkbook>) {
    let rows = [
        [text("Name"), text("Flag"), text("Amt")],
        [text("a"), text("No"), LiteralValue::Number(10.0)],
        [text("b"), text("Yes"), LiteralValue::Number(20.0)],
        [text("c"), text("No"), LiteralValue::Number(30.0)],
    ];
    for (r, row) in rows.iter().enumerate() {
        for (c, v) in row.iter().enumerate() {
            engine
                .set_cell_value("Sheet1", r as u32 + 1, c as u32 + 1, v.clone())
                .unwrap();
        }
    }
}

fn set(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, v: LiteralValue) {
    engine.set_cell_value("Sheet1", row, col, v).unwrap();
}

fn eval(engine: &mut Engine<TestWorkbook>, formula: &str) -> LiteralValue {
    engine
        .set_cell_formula("Sheet1", 30, 10, parse(formula).unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    engine.get_cell_value("Sheet1", 30, 10).unwrap()
}

fn error_kind(v: LiteralValue) -> Option<ExcelErrorKind> {
    match v {
        LiteralValue::Error(e) => Some(e.kind),
        _ => None,
    }
}

#[test]
fn criteria_header_that_is_not_a_label_matches_no_record() {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    build(&mut e);
    // E1:F2: headers EMISSIONS / No (not labels), values Name / Flag.
    set(&mut e, 1, 5, text("EMISSIONS"));
    set(&mut e, 1, 6, text("No"));
    set(&mut e, 2, 5, text("Name"));
    set(&mut e, 2, 6, text("Flag"));
    // G4:G5: a known label (control).
    set(&mut e, 4, 7, text("Flag"));
    set(&mut e, 5, 7, text("No"));
    // H1:I2: a known label AND an unknown one.
    set(&mut e, 1, 8, text("Flag"));
    set(&mut e, 1, 9, text("Other"));
    set(&mut e, 2, 8, text("No"));
    set(&mut e, 2, 9, text("x"));

    let n = LiteralValue::Number;
    assert_eq!(eval(&mut e, "=DSUM(A1:C4,3,E1:F2)"), n(0.0));
    assert_eq!(eval(&mut e, "=DSUM(A1:C4,3,E1:E2)"), n(0.0));
    assert_eq!(eval(&mut e, "=DSUM(A1:C4,3,G4:G5)"), n(40.0));
    assert_eq!(eval(&mut e, "=DSUM(A1:C4,3,H1:I2)"), n(0.0));
    assert_eq!(eval(&mut e, "=DCOUNT(A1:C4,3,E1:F2)"), n(0.0));
    assert_eq!(eval(&mut e, "=DCOUNTA(A1:C4,1,E1:F2)"), n(0.0));
    assert_eq!(eval(&mut e, "=DMAX(A1:C4,3,E1:F2)"), n(0.0));
    assert_eq!(
        error_kind(eval(&mut e, "=DAVERAGE(A1:C4,3,E1:F2)")),
        Some(ExcelErrorKind::Div)
    );
    assert_eq!(
        error_kind(eval(&mut e, "=DGET(A1:C4,3,E1:F2)")),
        Some(ExcelErrorKind::Value)
    );
}

#[test]
fn computed_criteria_constants() {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    build(&mut e);
    // K1:K2: blank header, TRUE criterion: every record.
    set(&mut e, 2, 11, LiteralValue::Boolean(true));
    // L1:L2: blank header, FALSE criterion: no record.
    set(&mut e, 2, 12, LiteralValue::Boolean(false));
    // M1:N2: known label row plus a blank header with no criterion below:
    // the empty cell sets no condition.
    set(&mut e, 1, 13, text("Flag"));
    set(&mut e, 2, 13, text("No"));

    let n = LiteralValue::Number;
    assert_eq!(eval(&mut e, "=DSUM(A1:C4,3,K1:K2)"), n(60.0));
    assert_eq!(eval(&mut e, "=DSUM(A1:C4,3,L1:L2)"), n(0.0));
    assert_eq!(eval(&mut e, "=DSUM(A1:C4,3,M1:N2)"), n(40.0));
}

#[test]
fn error_database_or_criteria_argument_propagates() {
    let mut e = Engine::new(TestWorkbook::new(), EvalConfig::default());
    build(&mut e);
    set(&mut e, 1, 5, text("Flag"));
    set(&mut e, 2, 5, text("No"));
    let broken = || NamedDefinition::Formula {
        ast: parse("=#REF!").unwrap(),
        dependencies: Vec::new(),
        range_deps: Vec::new(),
    };
    e.define_name("SUPPLEMENTALDATA", broken(), NameScope::Workbook)
        .unwrap();

    for f in [
        "=DSUM(SUPPLEMENTALDATA,\"Amt\",E1:E2)",
        "=DSUM(SUPPLEMENTALDATA,\"Amt\",E1:E2)/1000",
        "=DAVERAGE(SUPPLEMENTALDATA,3,E1:E2)",
        "=DSTDEV(SUPPLEMENTALDATA,3,E1:E2)",
        "=DGET(SUPPLEMENTALDATA,3,E1:E2)",
        "=DCOUNTA(SUPPLEMENTALDATA,3,E1:E2)",
        "=DSUM(A1:C4,3,SUPPLEMENTALDATA)",
        "=DSTDEV(A1:C4,3,SUPPLEMENTALDATA)",
        "=DGET(A1:C4,3,SUPPLEMENTALDATA)",
        "=DCOUNTA(A1:C4,3,SUPPLEMENTALDATA)",
        "=DSUM(#N/A,3,E1:E2)",
    ] {
        let want = if f.contains("#N/A") {
            ExcelErrorKind::Na
        } else {
            ExcelErrorKind::Ref
        };
        assert_eq!(error_kind(eval(&mut e, f)), Some(want), "{f}");
    }
}
