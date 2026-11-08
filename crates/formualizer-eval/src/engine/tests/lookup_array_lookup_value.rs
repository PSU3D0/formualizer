//! `MATCH`, `VLOOKUP`, `HLOOKUP` and `LOOKUP` with an array (or multi-cell
//! range) lookup value look up each element, as in Excel, and return an
//! array of the same shape. Expected values were measured in Excel 16 on the
//! same grid (`COUNT(MATCH(..))` follows from the measured MATCH array;
//! the measured `SUMPRODUCT(--ISNUMBER(MATCH(..)))` also needs ISNUMBER to
//! test array elements, a separate fix). A single lookup value keeps the scalar path.

use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;

fn n(v: f64) -> LiteralValue {
    LiteralValue::Number(v)
}

fn t(s: &str) -> LiteralValue {
    LiteralValue::Text(s.into())
}

fn na() -> LiteralValue {
    LiteralValue::Error(ExcelErrorKind::Na.into())
}

/// D1:D6 = x,y,z,x,y,q; H1:H3 = x,y,z; I1:I3 = 10,20,30;
/// H5:J5 = x,y,z; H6:J6 = 10,20,30.
fn build(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut e = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    for (i, d) in ["x", "y", "z", "x", "y", "q"].iter().enumerate() {
        e.set_cell_value("Sheet1", i as u32 + 1, 4, t(d)).unwrap();
    }
    for (i, h) in ["x", "y", "z"].iter().enumerate() {
        let k = i as u32 + 1;
        e.set_cell_value("Sheet1", k, 8, t(h)).unwrap();
        e.set_cell_value("Sheet1", k, 9, n(10.0 * k as f64))
            .unwrap();
        e.set_cell_value("Sheet1", 5, 7 + k, t(h)).unwrap();
        e.set_cell_value("Sheet1", 6, 7 + k, n(10.0 * k as f64))
            .unwrap();
    }
    e
}

fn column(values: &[LiteralValue]) -> Vec<Vec<LiteralValue>> {
    values.iter().map(|v| vec![v.clone()]).collect()
}

fn cases() -> Vec<(&'static str, Vec<Vec<LiteralValue>>)> {
    vec![
        (
            "=MATCH(D1:D6,H1:H3,0)",
            column(&[n(1.0), n(2.0), n(3.0), n(1.0), n(2.0), na()]),
        ),
        ("=COUNT(MATCH(D1:D6,H1:H3,0))", vec![vec![n(5.0)]]),
        ("=MATCH({15,25},I1:I3,1)", vec![vec![n(1.0), n(2.0)]]),
        (
            "=VLOOKUP(D1:D3,H1:I3,2,FALSE)",
            column(&[n(10.0), n(20.0), n(30.0)]),
        ),
        (
            "=VLOOKUP(D1:D6,H1:I3,2,FALSE)",
            column(&[n(10.0), n(20.0), n(30.0), n(10.0), n(20.0), na()]),
        ),
        (
            "=SUM(IFERROR(VLOOKUP(D1:D6,H1:I3,2,FALSE),0))",
            vec![vec![n(90.0)]],
        ),
        (
            "=HLOOKUP({\"z\",\"x\"},H5:J6,2,FALSE)",
            vec![vec![n(30.0), n(10.0)]],
        ),
        (
            "=LOOKUP(D1:D3,H1:H3,I1:I3)",
            column(&[n(10.0), n(20.0), n(30.0)]),
        ),
        // A single lookup value is unchanged.
        ("=VLOOKUP(\"y\",H1:I3,2,FALSE)", vec![vec![n(20.0)]]),
    ]
}

fn same(got: &LiteralValue, want: &LiteralValue) -> bool {
    match (got, want) {
        (LiteralValue::Number(a), LiteralValue::Number(b)) => (a - b).abs() < 1e-9,
        (LiteralValue::Int(a), LiteralValue::Number(b)) => (*a as f64 - b).abs() < 1e-9,
        (LiteralValue::Error(a), LiteralValue::Error(b)) => a.kind == b.kind,
        _ => got == want,
    }
}

#[test]
fn array_lookup_values_lift_match_vlookup_hlookup_lookup() {
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
            let base = 20 + 10 * i as u32;
            for (r, row) in want.iter().enumerate() {
                for (c, w) in row.iter().enumerate() {
                    let got = e
                        .get_cell_value("Sheet1", base + r as u32, 12 + c as u32)
                        .unwrap_or(LiteralValue::Empty);
                    assert!(
                        same(&got, w),
                        "{formula} [{r},{c}] = {got:?}, Excel gives {w:?} ({mode:?})"
                    );
                }
            }
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

/// A filled column of single-value lookups (the memoized family path) is
/// unchanged next to an array lookup over the same table.
#[test]
fn memoized_single_value_lookups_are_unchanged() {
    let mut e = build(FormulaPlaneMode::Off);
    for row in 1..=6u32 {
        e.set_cell_formula(
            "Sheet1",
            row,
            14,
            parse(format!("=IFERROR(VLOOKUP(D{row},$H$1:$I$3,2,FALSE),0)")).unwrap(),
        )
        .unwrap();
    }
    e.set_cell_formula(
        "Sheet1",
        1,
        16,
        parse("=SUM(IFERROR(VLOOKUP(D1:D6,$H$1:$I$3,2,FALSE),0))").unwrap(),
    )
    .unwrap();
    e.evaluate_all().unwrap();
    let want = [10.0, 20.0, 30.0, 10.0, 20.0, 0.0];
    for (i, w) in want.iter().enumerate() {
        let got = e.get_cell_value("Sheet1", i as u32 + 1, 14);
        assert!(
            same(&got.clone().unwrap_or(LiteralValue::Empty), &n(*w)),
            "N{} = {got:?}",
            i + 1
        );
    }
    let total = e
        .get_cell_value("Sheet1", 1, 16)
        .unwrap_or(LiteralValue::Empty);
    assert!(same(&total, &n(90.0)), "array total = {total:?}");
}

#[test]
fn array_lookup_reads_long_vectors_once_for_all_lookup_values() {
    const ROWS: u32 = 10_000;
    const NEEDLES: u32 = 1_000;
    let mut e = Engine::new(TestWorkbook::default(), EvalConfig::default());
    for row in 1..=ROWS {
        e.set_cell_value("Sheet1", row, 1, n(f64::from(row)))
            .unwrap();
        e.set_cell_value("Sheet1", row, 2, n(f64::from(row) * 2.0))
            .unwrap();
    }
    // Rereading both vectors per lookup value takes tens of seconds here.
    let formula = format!("=SUM(LOOKUP(A1:A{NEEDLES},A1:A{ROWS},B1:B{ROWS}))");
    e.set_cell_formula("Sheet1", 1, 4, parse(&formula).unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let want = f64::from(NEEDLES * (NEEDLES + 1));
    let got = e.get_cell_value("Sheet1", 1, 4).unwrap();
    assert!(same(&got, &n(want)), "{got:?}");
}

#[test]
fn array_lookup_keeps_each_form_and_a_miss_per_element() {
    let mut e = build(FormulaPlaneMode::Off);
    let cases = [
        // Tall array form: first column, result from the last.
        ("=SUM(LOOKUP({\"y\",\"z\"},H1:I3))", 50.0),
        // Wide array form: first row, result from the last.
        ("=SUM(LOOKUP({\"y\",\"z\"},H5:J6))", 50.0),
        // Before the first entry is #N/A for that element alone.
        ("=SUM(IFERROR(LOOKUP({5,15},I1:I3,H6:J6),100))", 110.0),
    ];
    for (i, (formula, _)) in cases.iter().enumerate() {
        e.set_cell_formula("Sheet1", 40 + i as u32, 12, parse(formula).unwrap())
            .unwrap();
    }
    e.evaluate_all().unwrap();
    for (i, (formula, want)) in cases.iter().enumerate() {
        let got = e.get_cell_value("Sheet1", 40 + i as u32, 12).unwrap();
        assert!(same(&got, &n(*want)), "{formula} = {got:?}");
    }
}
