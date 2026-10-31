//! An error criterion of `COUNTIF(S)`/`SUMIF(S)` (an error value such as
//! `NA()`, or its text `"#N/A"`) matches the cells holding that error, as in
//! Excel, instead of making the whole call that error. Expected values were
//! measured in Excel 16; the `#DIV/0!` and `"<>#N/A"` rows follow from the
//! same rule.

use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;

/// K1 = #N/A, K2 = 1, K3 = #N/A, K4 = #DIV/0!; L1:L4 = 1, 10, 100, 1000.
fn build(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut e = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    let err = |k| LiteralValue::Error(ExcelError::new(k));
    let k = [
        err(ExcelErrorKind::Na),
        LiteralValue::Number(1.0),
        err(ExcelErrorKind::Na),
        err(ExcelErrorKind::Div),
    ];
    for (i, v) in k.into_iter().enumerate() {
        let row = i as u32 + 1;
        e.set_cell_value("Sheet1", row, 11, v).unwrap();
        e.set_cell_value(
            "Sheet1",
            row,
            12,
            LiteralValue::Number(10f64.powi(i as i32)),
        )
        .unwrap();
    }
    e
}

const CASES: &[(&str, f64)] = &[
    ("=COUNTIF(K1:K4,NA())", 2.0),
    ("=COUNTIF(K1:K4,\"#N/A\")", 2.0),
    ("=COUNTIF(K1:K4,\"#DIV/0!\")", 1.0),
    ("=COUNTIF(K1:K4,1/0)", 1.0),
    ("=SUMIF(K1:K4,NA(),L1:L4)", 101.0),
    ("=SUMIF(K1:K4,\"#DIV/0!\",L1:L4)", 1000.0),
    ("=COUNTIFS(K1:K4,\"<>#N/A\")", 2.0),
];

#[test]
fn error_criteria_match_cells_holding_that_error() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut e = build(mode);
        for (i, (formula, _)) in CASES.iter().enumerate() {
            e.set_cell_formula("Sheet1", 20 + i as u32, 14, parse(formula).unwrap())
                .unwrap();
        }
        e.evaluate_all().unwrap();
        for (i, (formula, want)) in CASES.iter().enumerate() {
            let got = e.get_cell_value("Sheet1", 20 + i as u32, 14);
            let ok = match &got {
                Some(LiteralValue::Number(g)) => (g - want).abs() < 1e-9,
                Some(LiteralValue::Int(g)) => *g as f64 == *want,
                _ => false,
            };
            assert!(ok, "{formula} = {got:?}, Excel gives {want} ({mode:?})");
        }
    }
}
