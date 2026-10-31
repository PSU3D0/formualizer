//! A criteria argument of `SUMIF(S)`, `COUNTIF(S)` or `AVERAGEIF(S)` that is
//! an array or a multi-cell range lifts the call, as in Excel: the result is
//! an array with one aggregate per criterion element. Several array criteria
//! broadcast against each other (a row by a column gives a grid), and an
//! element past a shorter array's end is `#N/A`, which as a criterion matches
//! nothing (0), not an `#N/A` result. Expected values were measured in Excel
//! 16 on the same grid.

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

/// A1:A6 = a,b,a,c,b,a; B1:B6 = 1..6; F1:F2 = a,b.
fn build(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut e = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    for (i, a) in ["a", "b", "a", "c", "b", "a"].iter().enumerate() {
        let row = i as u32 + 1;
        e.set_cell_value("Sheet1", row, 1, t(a)).unwrap();
        e.set_cell_value("Sheet1", row, 2, n(row as f64)).unwrap();
    }
    e.set_cell_value("Sheet1", 1, 6, t("a")).unwrap();
    e.set_cell_value("Sheet1", 2, 6, t("b")).unwrap();
    e
}

/// `(formula, expected grid)`; each formula gets its own block of cells
/// below the data so a spill has room.
fn cases() -> Vec<(&'static str, Vec<Vec<LiteralValue>>)> {
    vec![
        (
            "=SUM(SUMIFS(B1:B6,A1:A6,{\"a\",\"b\"}))",
            vec![vec![n(17.0)]],
        ),
        (
            "=SUMPRODUCT(COUNTIFS(A1:A6,{\"a\",\"b\"}))",
            vec![vec![n(5.0)]],
        ),
        (
            "=SUMIF(A1:A6,F1:F2,B1:B6)",
            vec![vec![n(10.0)], vec![n(7.0)]],
        ),
        ("=COUNTIF(A1:A6,F1:F2)", vec![vec![n(3.0)], vec![n(2.0)]]),
        (
            "=COUNTIFS(A1:A6,{\"a\",\"b\"},B1:B6,{\">1\";\">3\"})",
            vec![vec![n(2.0), n(2.0)], vec![n(1.0), n(1.0)]],
        ),
        (
            "=COUNTIFS(A1:A6,{\"a\",\"b\",\"c\"},B1:B6,{\">0\",\">3\"})",
            vec![vec![n(3.0), n(1.0), n(0.0)]],
        ),
        (
            "=AVERAGEIFS(B1:B6,A1:A6,{\"a\";\"b\"})",
            vec![vec![n(10.0 / 3.0)], vec![n(3.5)]],
        ),
        ("=SUM(1/COUNTIF(A1:A6,A1:A6))", vec![vec![n(3.0)]]),
        ("=FILTER(F1:F2,COUNTIF(A1:A6,F1:F2)>2)", vec![vec![t("a")]]),
        // An error element matches only cells holding that error (none here).
        ("=COUNTIFS(A1:A6,{\"a\",#N/A})", vec![vec![n(3.0), n(0.0)]]),
        (
            "=SUMIFS(B1:B6,A1:A6,{\"a\",#N/A})",
            vec![vec![n(10.0), n(0.0)]],
        ),
        // A 1x1 array is one criterion: the result stays scalar.
        ("=COUNTIF(A1:A6,{\"a\"})", vec![vec![n(3.0)]]),
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
fn array_criteria_lift_criteria_aggregates() {
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
            // Nothing spills past the expected shape.
            let below = e.get_cell_value("Sheet1", 20 + 10 * i as u32 + want.len() as u32, 12);
            assert!(
                matches!(below, None | Some(LiteralValue::Empty)),
                "{formula} spilled too far down: {below:?} ({mode:?})"
            );
        }
    }
}
