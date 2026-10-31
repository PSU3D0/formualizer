//! The type tests (`ISNUMBER`, `ISTEXT`, `ISBLANK`, `ISERROR`, ...) applied
//! to an array or a multi-cell range test each element, as in Excel: the
//! result is an array of booleans, not one verdict on the array as a whole
//! (which read every range as "not a number" and returned a single FALSE).
//! Expected values were measured in Excel 16 on the same grid. `ISEVEN` and
//! `ISODD` are Analysis ToolPak functions: Excel gives one `#VALUE!` for a
//! range rather than an array, and so does this engine.

use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;

/// C1:C6 = 5, "txt", blank, TRUE, 2.5, "7" (text); B1:B6 = 1..6.
fn build(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut e = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    let c = [
        LiteralValue::Number(5.0),
        LiteralValue::Text("txt".into()),
        LiteralValue::Empty,
        LiteralValue::Boolean(true),
        LiteralValue::Number(2.5),
        LiteralValue::Text("7".into()),
    ];
    for (i, v) in c.into_iter().enumerate() {
        e.set_cell_value(
            "Sheet1",
            i as u32 + 1,
            2,
            LiteralValue::Number(i as f64 + 1.0),
        )
        .unwrap();
        if !matches!(v, LiteralValue::Empty) {
            e.set_cell_value("Sheet1", i as u32 + 1, 3, v).unwrap();
        }
    }
    e
}

/// A column of booleans from a `T`/`F` pattern.
fn column(pattern: &str) -> Vec<Vec<LiteralValue>> {
    pattern
        .chars()
        .map(|ch| vec![LiteralValue::Boolean(ch == 'T')])
        .collect()
}

/// A row of booleans from a `T`/`F` pattern.
fn row(pattern: &str) -> Vec<Vec<LiteralValue>> {
    vec![
        pattern
            .chars()
            .map(|ch| LiteralValue::Boolean(ch == 'T'))
            .collect(),
    ]
}

fn value_error() -> LiteralValue {
    LiteralValue::Error(formualizer_common::ExcelError::new_value())
}

fn cases() -> Vec<(&'static str, Vec<Vec<LiteralValue>>)> {
    let n = |v: f64| vec![vec![LiteralValue::Number(v)]];
    vec![
        ("=ISNUMBER(C1:C6)", column("TFFFTF")),
        ("=ROWS(FILTER(C1:C6,ISNUMBER(C1:C6)))", n(2.0)),
        ("=SUMPRODUCT(--ISNUMBER(C1:C6))", n(2.0)),
        ("=ISTEXT(C1:C6)", column("FTFFFT")),
        ("=ISBLANK(C1:C6)", column("FFTFFF")),
        ("=ISERROR({1,#N/A,\"x\"})", row("FTF")),
        ("=ISLOGICAL(C1:C6)", column("FFFTFF")),
        ("=ISNONTEXT(C1:C6)", column("TFTTTF")),
        ("=ISNA({1,#N/A,#DIV/0!})", row("FTF")),
        ("=ISERR({1,#N/A,#DIV/0!})", row("FFT")),
        ("=ISEVEN(B1:B6)", vec![vec![value_error()]]),
        ("=ISODD(B1:B6)", vec![vec![value_error()]]),
        // A single cell is still one verdict.
        ("=ISNUMBER(C1)", row("T")),
    ]
}

#[test]
fn type_tests_lift_over_arrays_and_ranges() {
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
            for (r, want_row) in want.iter().enumerate() {
                for (c, w) in want_row.iter().enumerate() {
                    let got = e
                        .get_cell_value("Sheet1", 20 + 10 * i as u32 + r as u32, 12 + c as u32)
                        .unwrap_or(LiteralValue::Empty);
                    let same = match (&got, w) {
                        (LiteralValue::Int(a), LiteralValue::Number(b)) => *a as f64 == *b,
                        (LiteralValue::Error(a), LiteralValue::Error(b)) => a.kind == b.kind,
                        _ => &got == w,
                    };
                    assert!(
                        same,
                        "{formula} [{r},{c}] = {got:?}, Excel gives {w:?} ({mode:?})"
                    );
                }
            }
            let below = e.get_cell_value("Sheet1", 20 + 10 * i as u32 + want.len() as u32, 12);
            assert!(
                matches!(below, None | Some(LiteralValue::Empty)),
                "{formula} spilled too far down: {below:?} ({mode:?})"
            );
        }
    }
}
