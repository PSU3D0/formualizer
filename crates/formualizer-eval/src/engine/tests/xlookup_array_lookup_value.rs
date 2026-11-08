//! `XLOOKUP` and `XMATCH` with an array (or multi-cell range) lookup value
//! look up each element, as in Excel, and return an array of the same
//! shape. With a multi-column return range each lookup contributes only the
//! first column. Expected values were measured in Excel 16 on the same
//! grid.

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;

fn n(v: f64) -> LiteralValue {
    LiteralValue::Number(v)
}

fn t(s: &str) -> LiteralValue {
    LiteralValue::Text(s.into())
}

/// D1:D6 = x,y,z,x,y,q; H1:H3 = x,y,z; I1:I3 = 10,20,30; J1:J3 = 100,200,300.
fn build(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut e = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    for (i, d) in ["x", "y", "z", "x", "y", "q"].iter().enumerate() {
        e.set_cell_value("Sheet1", i as u32 + 1, 4, t(d)).unwrap();
    }
    for (i, h) in ["x", "y", "z"].iter().enumerate() {
        let row = i as u32 + 1;
        e.set_cell_value("Sheet1", row, 8, t(h)).unwrap();
        e.set_cell_value("Sheet1", row, 9, n(10.0 * row as f64))
            .unwrap();
        e.set_cell_value("Sheet1", row, 10, n(100.0 * row as f64))
            .unwrap();
    }
    e
}

fn cases() -> Vec<(&'static str, Vec<Vec<LiteralValue>>)> {
    vec![
        (
            "=XLOOKUP(D1:D6,H1:H3,I1:I3,\"nf\")",
            vec![
                vec![n(10.0)],
                vec![n(20.0)],
                vec![n(30.0)],
                vec![n(10.0)],
                vec![n(20.0)],
                vec![t("nf")],
            ],
        ),
        (
            "=SUMPRODUCT(XLOOKUP(D1:D6,H1:H3,I1:I3,0))",
            vec![vec![n(90.0)]],
        ),
        (
            "=XLOOKUP({\"y\",\"x\"},H1:H3,I1:I3)",
            vec![vec![n(20.0), n(10.0)]],
        ),
        (
            "=XLOOKUP(D1:D3,H1:H3,I1:J3)",
            vec![vec![n(10.0)], vec![n(20.0)], vec![n(30.0)]],
        ),
        ("=XMATCH({\"y\",\"x\"},H1:H3)", vec![vec![n(2.0), n(1.0)]]),
        // A single lookup value still returns its whole return row.
        ("=XLOOKUP(\"y\",H1:H3,I1:J3)", vec![vec![n(20.0), n(200.0)]]),
    ]
}

fn close(got: &LiteralValue, want: &LiteralValue) -> bool {
    match (got, want) {
        (LiteralValue::Number(a), LiteralValue::Number(b)) => (a - b).abs() < 1e-9,
        (LiteralValue::Int(a), LiteralValue::Number(b)) => (*a as f64 - b).abs() < 1e-9,
        _ => got == want,
    }
}

#[test]
fn array_lookup_values_lift_xlookup_and_xmatch() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut e = build(mode);
        let cases = cases();
        for (i, (formula, _)) in cases.iter().enumerate() {
            e.set_cell_formula("Sheet1", 20 + 10 * i as u32, 12, parse(formula).unwrap())
                .unwrap();
        }
        e.evaluate_all().unwrap();
        for (i, (formula, want)) in cases.iter().enumerate() {
            for (r, row) in want.iter().enumerate() {
                for (c, w) in row.iter().enumerate() {
                    let got = e
                        .get_cell_value("Sheet1", 20 + 10 * i as u32 + r as u32, 12 + c as u32)
                        .unwrap_or(LiteralValue::Empty);
                    assert!(
                        close(&got, w),
                        "{formula} [{r},{c}] = {got:?}, Excel gives {w:?} ({mode:?})"
                    );
                }
            }
            let base = 20 + 10 * i as u32;
            let below = e.get_cell_value("Sheet1", base + want.len() as u32, 12);
            let right = e.get_cell_value("Sheet1", base, 12 + want[0].len() as u32);
            assert!(
                matches!(below, None | Some(LiteralValue::Empty))
                    && matches!(right, None | Some(LiteralValue::Empty)),
                "{formula} spilled past its shape: below {below:?}, right {right:?} ({mode:?})"
            );
        }
    }
}
