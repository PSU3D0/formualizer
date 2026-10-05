//! SUBTOTAL and AGGREGATE ignore nested subtotals (Excel): a cell in a
//! range argument whose formula calls SUBTOTAL anywhere (`SUBTOTAL(..)+8`,
//! `IF(..,SUBTOTAL(..),..)`) is skipped by SUBTOTAL; AGGREGATE options 0-3
//! skip cells whose formulas call SUBTOTAL or AGGREGATE. Both evaluation
//! paths agree (per-cell oracle and family execution), and the 101-111
//! hidden-row rule still applies.

use super::common::arrow_eval_config;
use crate::engine::{Engine, EvalConfig, RowVisibilitySource};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn engine(family: bool) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: family,
            ..arrow_eval_config()
        },
    )
}

fn formula(e: &mut Engine<TestWorkbook>, row: u32, col: u32, f: &str) {
    e.set_cell_formula("Sheet1", row, col, parse(f).unwrap())
        .unwrap();
}

fn value(e: &mut Engine<TestWorkbook>, row: u32, col: u32, v: LiteralValue) {
    e.set_cell_value("Sheet1", row, col, v).unwrap();
}

fn get(e: &Engine<TestWorkbook>, row: u32, col: u32) -> LiteralValue {
    e.get_cell_value("Sheet1", row, col)
        .unwrap_or(LiteralValue::Empty)
}

fn close(v: &LiteralValue, expected: f64) -> bool {
    matches!(v, LiteralValue::Number(n) if (n - expected).abs() < 1e-9)
}

#[test]
fn nested_subtotal_sum_is_ignored() {
    // Repro: A3 = SUBTOTAL(9,A1:A2); A5 = SUBTOTAL(9,A1:A3) is 30, not 60.
    let mut e = engine(true);
    value(&mut e, 1, 1, LiteralValue::Number(10.0));
    value(&mut e, 2, 1, LiteralValue::Number(20.0));
    formula(&mut e, 3, 1, "=SUBTOTAL(9,A1:A2)");
    formula(&mut e, 5, 1, "=SUBTOTAL(9,A1:A3)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, 3, 1), LiteralValue::Number(30.0));
    assert_eq!(get(&e, 5, 1), LiteralValue::Number(30.0));
}

#[test]
fn subtotal_inside_a_larger_formula_is_ignored() {
    // Repro: A3 = SUBTOTAL(3,A1:A2)+8; A5 = SUBTOTAL(3,A1:A3)+8 is 10, not 11.
    let mut e = engine(true);
    value(&mut e, 1, 1, LiteralValue::Text("x".into()));
    value(&mut e, 2, 1, LiteralValue::Text("y".into()));
    formula(&mut e, 3, 1, "=SUBTOTAL(3,A1:A2)+8");
    formula(&mut e, 5, 1, "=SUBTOTAL(3,A1:A3)+8");
    // A formula containing SUBTOTAL in an untaken branch is still nested.
    formula(&mut e, 4, 1, "=IF(FALSE,SUBTOTAL(9,A1:A2),5)");
    formula(&mut e, 6, 1, "=SUBTOTAL(9,A1:A4)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, 3, 1), LiteralValue::Number(10.0));
    assert_eq!(get(&e, 5, 1), LiteralValue::Number(10.0));
    assert_eq!(get(&e, 6, 1), LiteralValue::Number(0.0));
}

#[test]
fn every_function_number_ignores_nested_subtotals() {
    // B1:B4 = 1..4; B5 = SUBTOTAL(9,B1:B4) (10); B6 = SUM(B1:B4) (10, an
    // ordinary formula, counted). Every SUBTOTAL(k, B1:B6) equals the
    // function over {1,2,3,4,10}.
    let mut e = engine(false);
    for r in 1..=4u32 {
        value(&mut e, r, 2, LiteralValue::Number(r as f64));
    }
    formula(&mut e, 5, 2, "=SUBTOTAL(9,B1:B4)");
    formula(&mut e, 6, 2, "=SUM(B1:B4)");
    let ks: Vec<u32> = (1..=11).chain(101..=111).collect();
    for (i, k) in ks.iter().enumerate() {
        formula(&mut e, 1, 4 + i as u32, &format!("=SUBTOTAL({k},B1:B6)"));
        formula(&mut e, 2, 4 + i as u32, &format!("=SUBTOTAL({k},B1:B4,B6)"));
    }
    e.evaluate_all().unwrap();
    for (i, k) in ks.iter().enumerate() {
        let got = get(&e, 1, 4 + i as u32);
        let want = get(&e, 2, 4 + i as u32);
        assert!(matches!(want, LiteralValue::Number(_)), "k={k} {want:?}");
        assert_eq!(got, want, "SUBTOTAL({k}) must ignore the nested subtotal");
    }
}

#[test]
fn hidden_rows_still_apply_with_nested_subtotals() {
    let mut e = engine(false);
    for (r, v) in [(1, 10.0), (2, 20.0), (3, 30.0)] {
        value(&mut e, r, 1, LiteralValue::Number(v));
    }
    formula(&mut e, 4, 1, "=SUBTOTAL(9,A1:A3)");
    formula(&mut e, 6, 1, "=SUBTOTAL(9,A1:A4)");
    formula(&mut e, 7, 1, "=SUBTOTAL(109,A1:A4)");
    e.set_row_hidden("Sheet1", 2, true, RowVisibilitySource::Manual)
        .unwrap();
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, 6, 1), LiteralValue::Number(60.0));
    assert_eq!(get(&e, 7, 1), LiteralValue::Number(40.0));
}

#[test]
fn aggregate_options_ignore_nested_subtotal_and_aggregate() {
    let mut e = engine(false);
    for (r, v) in [(1, 1.0), (2, 2.0), (3, 3.0)] {
        value(&mut e, r, 1, LiteralValue::Number(v));
    }
    formula(&mut e, 4, 1, "=SUBTOTAL(9,A1:A3)");
    formula(&mut e, 5, 1, "=AGGREGATE(9,0,A1:A3)");
    formula(&mut e, 6, 1, "=SUM(A1:A3)");
    for (i, opt) in (0..=3).enumerate() {
        formula(
            &mut e,
            10,
            1 + i as u32,
            &format!("=AGGREGATE(9,{opt},A1:A6)"),
        );
    }
    // SUBTOTAL's documented rule names nested subtotals only: the
    // AGGREGATE cell counts.
    formula(&mut e, 11, 1, "=SUBTOTAL(9,A1:A6)");
    e.evaluate_all().unwrap();
    for i in 0..4u32 {
        assert_eq!(get(&e, 10, 1 + i), LiteralValue::Number(12.0), "option {i}");
    }
    assert_eq!(get(&e, 11, 1), LiteralValue::Number(18.0));
}

#[test]
fn computed_arrays_and_other_functions_are_not_filtered() {
    let mut e = engine(false);
    value(&mut e, 1, 1, LiteralValue::Number(4.0));
    formula(&mut e, 2, 1, "=SUBTOTAL(9,A1)");
    // SUM is not SUBTOTAL: it counts the nested subtotal.
    formula(&mut e, 3, 1, "=SUM(A1:A2)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, 3, 1), LiteralValue::Number(8.0));
}

/// A family run of SUBTOTAL formulas (one per row) summed by an outer
/// SUBTOTAL: the nested cells are skipped whether the run is evaluated per
/// cell or as a family, and after an edit.
#[test]
fn family_run_of_nested_subtotals_is_ignored() {
    const N: u32 = 64;
    let run = |family: bool| -> (Vec<LiteralValue>, u64) {
        let mut e = engine(family);
        for r in 1..=N {
            value(&mut e, r, 1, LiteralValue::Number(r as f64));
            formula(&mut e, r, 2, &format!("=SUBTOTAL(9,A{r}:A{r})*2"));
            formula(&mut e, r, 3, &format!("=SUBTOTAL(9,A{r}:B{r})"));
        }
        formula(&mut e, N + 2, 2, &format!("=SUBTOTAL(9,B1:B{N})"));
        formula(&mut e, N + 3, 2, &format!("=SUM(B1:B{N})"));
        formula(&mut e, N + 4, 2, &format!("=SUBTOTAL(9,A1:C{N})"));
        e.evaluate_all().unwrap();
        let mut out = vec![get(&e, N + 2, 2), get(&e, N + 3, 2), get(&e, N + 4, 2)];
        value(&mut e, 5, 1, LiteralValue::Number(1000.0));
        e.evaluate_all().unwrap();
        out.extend([get(&e, N + 2, 2), get(&e, N + 3, 2), get(&e, N + 4, 2)]);
        (out, e.family_members_for_test())
    };
    let (oracle, _) = run(false);
    let (family, members) = run(true);
    assert!(members > 0, "no family run executed");
    assert_eq!(family, oracle);
    let sum = (N * (N + 1) / 2) as f64;
    assert!(close(&oracle[0], 0.0), "{:?}", oracle[0]);
    assert!(close(&oracle[1], 2.0 * sum), "{:?}", oracle[1]);
    assert!(close(&oracle[2], sum), "{:?}", oracle[2]);
    let sum2 = sum - 5.0 + 1000.0;
    assert!(close(&oracle[3], 0.0));
    assert!(close(&oracle[4], 2.0 * sum2));
    assert!(close(&oracle[5], sum2));
}
