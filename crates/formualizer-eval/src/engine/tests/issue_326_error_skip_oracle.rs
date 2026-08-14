//! Excel oracle for error cells and duplicate tie-breaks in approximate lookups.
//!
//! Issue #326. Oracle: desktop Microsoft Excel 16.105.3, driven by AppleScript
//! against fresh workbooks so there is no manual-entry ambiguity; error cells
//! produced by real formulas (`=1/0`, `=SQRT(-1)`); results read back twice.
//! Contributed by @tommy230.
//!
//! The 100-element rows are load bearing: the range is large enough for true
//! bisection and the error sits exactly where the midpoint probe lands, yet
//! nothing propagates. The descending 100-element row is the projection proof --
//! were the error cell live-valued the answer would be 50, and Excel returns 49.
//!
//! 0.8.2 shipped range-wide error propagation here, which diverged from Excel on
//! 8 of these rows.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parse;

fn mk() -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_parallel(false),
    )
}
fn num(e: &mut Engine<TestWorkbook>, c: u32, r: u32, v: f64) {
    e.set_cell_value("S", r, c, LiteralValue::Number(v))
        .unwrap();
}
fn err(e: &mut Engine<TestWorkbook>, c: u32, r: u32, f: &str) {
    e.set_cell_formula("S", r, c, parse(f).unwrap()).unwrap();
}
fn ev(e: &mut Engine<TestWorkbook>, f: &str) -> String {
    e.set_cell_formula("S", 200, 20, parse(f).unwrap()).unwrap();
    e.evaluate_all().unwrap();
    match e.get_cell_value("S", 200, 20) {
        Some(LiteralValue::Number(n)) => format!("{n}"),
        Some(LiteralValue::Error(x)) => format!("{:?}", x.kind),
        o => format!("{o:?}"),
    }
}
fn row(label: &str, excel: &str, got: String, fails: &mut u32) {
    if got != excel {
        *fails += 1;
    }
    println!(
        "  {:<6} excel={:<6} fz={:<8} {}",
        label,
        excel,
        got,
        if got == excel { "ok" } else { "** DIVERGES **" }
    );
}

#[test]
fn excel_oracle_table_for_error_cells_and_duplicate_tie_breaks() {
    let mut f = 0u32;
    println!("=== #326 oracle vs fix ===");
    let mut e = mk();
    num(&mut e, 8, 1, 1.0);
    num(&mut e, 8, 2, 2.0);
    err(&mut e, 8, 3, "=1/0");
    num(&mut e, 8, 4, 9.0);
    for r in 1..=4 {
        num(&mut e, 9, r, f64::from(r * 10));
    }
    row("r1", "2", ev(&mut e, "=MATCH(5,H1:H4,1)"), &mut f);
    row("r2", "20", ev(&mut e, "=VLOOKUP(5,H1:I4,2,TRUE)"), &mut f);
    let mut a = mk();
    for r in 1..=3 {
        err(&mut a, 12, r, "=1/0");
    }
    row("r3", "Na", ev(&mut a, "=MATCH(5,L1:L3,1)"), &mut f);
    let mut m = mk();
    num(&mut m, 13, 1, 9.0);
    num(&mut m, 13, 2, 8.0);
    err(&mut m, 13, 3, "=1/0");
    num(&mut m, 13, 4, 4.0);
    num(&mut m, 13, 5, 1.0);
    row("r4", "2", ev(&mut m, "=MATCH(5.5,M1:M5,-1)"), &mut f);
    let mut s = mk();
    for r in 1..=100u32 {
        num(&mut s, 19, r, f64::from(r));
    }
    err(&mut s, 19, 50, "=SQRT(-1)");
    err(&mut s, 19, 25, "=1/0");
    row("r5", "75", ev(&mut s, "=MATCH(75.5,S1:S100,1)"), &mut f);
    row("r6", "49", ev(&mut s, "=MATCH(50,S1:S100,1)"), &mut f);
    row("r8", "Na", ev(&mut s, "=MATCH(50,S1:S100,0)"), &mut f);
    let mut t = mk();
    for r in 1..=100u32 {
        num(&mut t, 20, r, f64::from(101 - r));
    }
    err(&mut t, 20, 50, "=SQRT(-1)");
    row("r7", "49", ev(&mut t, "=MATCH(50.5,T1:T100,-1)"), &mut f);
    // duplicates: descending small (<8) and large (>=8)
    let mut d = mk();
    for (i, v) in [9.0, 7.0, 7.0, 7.0, 5.0].iter().enumerate() {
        num(&mut d, 21, (i + 1) as u32, *v);
    }
    row("dupA", "2", ev(&mut d, "=MATCH(7,U1:U5,-1)"), &mut f);
    row("dupB", "4", ev(&mut d, "=MATCH(6,U1:U5,-1)"), &mut f);
    let mut d2 = mk();
    for (i, v) in [20.0, 9.0, 7.0, 7.0, 7.0, 5.0, 3.0, 2.0, 1.0, 0.0]
        .iter()
        .enumerate()
    {
        num(&mut d2, 22, (i + 1) as u32, *v);
    }
    row("dupA10", "3", ev(&mut d2, "=MATCH(7,V1:V10,-1)"), &mut f);
    row("dupB10", "5", ev(&mut d2, "=MATCH(6,V1:V10,-1)"), &mut f);
    // ascending duplicates: Excel returns LAST on equality
    let mut asc = mk();
    for (i, v) in [1.0, 5.0, 7.0, 7.0, 7.0, 9.0].iter().enumerate() {
        num(&mut asc, 23, (i + 1) as u32, *v);
    }
    row("ascDup", "5", ev(&mut asc, "=MATCH(7,W1:W6,1)"), &mut f);
    println!("=== divergences: {f} ===");
    assert_eq!(f, 0, "{f} rows diverge from the Excel oracle");
}
