//! Iterate edge corpus — SCC-feeds-SCC condensation shapes (RFC #112/#113,
//! spec §3.6 downstream rule).
//!
//! Two (or four) iterating SCCs in a dependency chain/diamond: condensation
//! order must evaluate upstream SCCs to their converged values BEFORE
//! downstream SCCs start, and telemetry must count every iterated SCC.
//!
//! Contraction template used throughout: X = 0.5·input + 0.5·Y, Y = 0.5·X
//! has the closed-form fixed point x = (2/3)·input, y = x/2 (Gauss–Seidel
//! contraction factor 0.25 → fast convergence; |x − x*| ≲ max_change/3 at
//! stop, so chained tolerances stay well under 10·max_change).

use crate::engine::{CycleConfig, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn iterate_engine(max_iterations: u32, max_change: f64) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_cycle(CycleConfig::iterate(max_iterations, max_change)),
    )
}

fn set_formula(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, f: &str) {
    engine
        .set_cell_formula(sheet, row, col, parse(f).expect("parse"))
        .expect("set formula");
}

fn num(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> f64 {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet} r{row}c{col}, got {other:?}"),
    }
}

const TOL: f64 = 0.01; // 10 × max_change — generous for chained residuals

#[test]
fn scc_feeds_scc_converges_both_and_telemetry_counts_two() {
    // SCC₁ {A1, A2}: A1 = 0.5·10 + 0.5·A2, A2 = 0.5·A1 → a1 = 20/3.
    // SCC₂ {B1, B2}: B1 = 0.5·A1 + 0.5·B2, B2 = 0.5·B1 → b1 = (2/3)·a1 = 40/9.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=0.5*10+0.5*A2");
    set_formula(&mut engine, "Sheet1", 2, 1, "=0.5*A1");
    set_formula(&mut engine, "Sheet1", 1, 2, "=0.5*A1+0.5*B2");
    set_formula(&mut engine, "Sheet1", 2, 2, "=0.5*B1");
    engine.evaluate_all().unwrap();

    assert!((num(&engine, "Sheet1", 1, 1) - 20.0 / 3.0).abs() < TOL);
    assert!((num(&engine, "Sheet1", 2, 1) - 10.0 / 3.0).abs() < TOL);
    assert!(
        (num(&engine, "Sheet1", 1, 2) - 40.0 / 9.0).abs() < TOL,
        "B1 must be built from SCC₁'s CONVERGED output, got {}",
        num(&engine, "Sheet1", 1, 2)
    );
    assert!((num(&engine, "Sheet1", 2, 2) - 20.0 / 9.0).abs() < TOL);

    let t = engine.last_cycle_telemetry();
    assert_eq!(t.static_sccs, 2);
    assert_eq!(t.iterated_sccs, 2, "both SCCs iterate");
    assert_eq!(t.converged_sccs, 2);
    assert_eq!(t.capped_sccs, 0);
    assert_eq!(t.live_cycles_witnessed, 2);
    assert!(t.max_passes_single_scc < 100);
}

#[test]
fn diamond_of_four_sccs_converges_in_condensation_order() {
    // SCC_A {A1,A2} (source) feeds SCC_B {B1,B2} and SCC_C {C1,C2} in
    // parallel; both feed SCC_D {D1,D2}.
    //   a1 = 20/3; b1 = c1 = (2/3)a1 = 40/9; d1 = (2/3)(b1+c1) = 160/27.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=0.5*10+0.5*A2");
    set_formula(&mut engine, "Sheet1", 2, 1, "=0.5*A1");
    set_formula(&mut engine, "Sheet1", 1, 2, "=0.5*A1+0.5*B2");
    set_formula(&mut engine, "Sheet1", 2, 2, "=0.5*B1");
    set_formula(&mut engine, "Sheet1", 1, 3, "=0.5*A1+0.5*C2");
    set_formula(&mut engine, "Sheet1", 2, 3, "=0.5*C1");
    set_formula(&mut engine, "Sheet1", 1, 4, "=0.5*(B1+C1)+0.5*D2");
    set_formula(&mut engine, "Sheet1", 2, 4, "=0.5*D1");
    engine.evaluate_all().unwrap();

    assert!((num(&engine, "Sheet1", 1, 1) - 20.0 / 3.0).abs() < TOL);
    assert!((num(&engine, "Sheet1", 1, 2) - 40.0 / 9.0).abs() < TOL);
    assert!((num(&engine, "Sheet1", 1, 3) - 40.0 / 9.0).abs() < TOL);
    assert!(
        (num(&engine, "Sheet1", 1, 4) - 160.0 / 27.0).abs() < TOL,
        "D1 needs BOTH parallel SCC outputs converged first, got {}",
        num(&engine, "Sheet1", 1, 4)
    );

    let t = engine.last_cycle_telemetry();
    assert_eq!(t.static_sccs, 4);
    assert_eq!(t.iterated_sccs, 4);
    assert_eq!(t.converged_sccs, 4);
    assert_eq!(t.capped_sccs, 0);
}

#[test]
fn capped_scc_feeding_converging_scc_uses_the_capped_value() {
    // SCC₁ diverges (accumulates +1 per pass, cap 5 → A1 = 5·k after k
    // recalcs); SCC₂ contracts FAST (Gauss–Seidel factor 0.01, so it
    // converges within the same 5-pass budget) on SCC₁'s capped output:
    //   B1 = 0.5·A1 + 0.02·B2, B2 = 0.5·B1 ⟹ b1 = 0.5·a1/(1 − 0.01).
    let mut engine = iterate_engine(5, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=A1+1"); // caps at 5 per recalc
    set_formula(&mut engine, "Sheet1", 1, 2, "=0.5*A1+0.02*B2");
    set_formula(&mut engine, "Sheet1", 2, 2, "=0.5*B1");
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 5.0);
    assert!(
        (num(&engine, "Sheet1", 1, 2) - 0.5 * 5.0 / 0.99).abs() < TOL,
        "B1 from A1's capped value, got {}",
        num(&engine, "Sheet1", 1, 2)
    );
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 2);
    assert_eq!(t.converged_sccs, 1);
    assert_eq!(t.capped_sccs, 1);

    // Second recalc: SCC₁ runs to 10; SCC₂ re-contracts onto the new input.
    engine.evaluate_all().unwrap();
    assert_eq!(num(&engine, "Sheet1", 1, 1), 10.0);
    assert!((num(&engine, "Sheet1", 1, 2) - 0.5 * 10.0 / 0.99).abs() < TOL);
}

#[test]
fn scc_chain_through_plain_acyclic_middleman_stays_ordered() {
    // SCC₁ → M1 (plain cell, = A1·3) → SCC₂. The acyclic middleman must be
    // scheduled between the two SCC units.
    let mut engine = iterate_engine(100, 0.001);
    set_formula(&mut engine, "Sheet1", 1, 1, "=0.5*10+0.5*A2");
    set_formula(&mut engine, "Sheet1", 2, 1, "=0.5*A1");
    set_formula(&mut engine, "Sheet1", 1, 13, "=A1*3"); // M1
    set_formula(&mut engine, "Sheet1", 1, 2, "=0.5*M1+0.5*B2");
    set_formula(&mut engine, "Sheet1", 2, 2, "=0.5*B1");
    engine.evaluate_all().unwrap();

    let a1 = 20.0 / 3.0;
    assert!((num(&engine, "Sheet1", 1, 13) - a1 * 3.0).abs() < 3.0 * TOL);
    assert!(
        (num(&engine, "Sheet1", 1, 2) - (2.0 / 3.0) * a1 * 3.0).abs() < 3.0 * TOL,
        "got {}",
        num(&engine, "Sheet1", 1, 2)
    );
    let t = engine.last_cycle_telemetry();
    assert_eq!(t.iterated_sccs, 2);
    assert_eq!(t.converged_sccs, 2);
}
