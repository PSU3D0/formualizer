//! Freshness of dynamic reads (Program 1 M1c, design §8.2/§8.5).
//!
//! Value-level versions of the §8.5 named cases. They run on the legacy
//! graph (the oracle) and under `unified_authority`: whatever the plan and
//! its hints order first, a dynamic reader of a dirty cell must never leave
//! a stale value behind, and cycles first discovered through a dynamic read
//! must be reported as cycles.
//!
//! Not here: a self read first discovered through INDIRECT (`A1 =
//! INDIRECT("A1")+1`) evaluates to 1 in legacy and under the authority,
//! where design §8.2 case 2 expects #CIRC; that is a planned Δ of the
//! freshness recorder, not current behavior.

use crate::engine::{Engine, EvalConfig, EvaluationTarget};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

fn engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

fn num(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> Option<f64> {
    match engine.get_cell_value("Sheet1", row, col) {
        Some(LiteralValue::Number(n)) => Some(n),
        Some(LiteralValue::Int(n)) => Some(n as f64),
        _ => None,
    }
}

fn set(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, v: f64) {
    engine
        .set_cell_value("Sheet1", row, col, LiteralValue::Number(v))
        .unwrap();
}

fn formula(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, f: &str) {
    engine
        .set_cell_formula("Sheet1", row, col, parse(f).unwrap())
        .unwrap();
}

fn is_circ(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> bool {
    matches!(
        engine.get_cell_value("Sheet1", row, col),
        Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Circ
    )
}

/// §8.5(1): first evaluation, INDIRECT to a dirty formula ordered later.
#[test]
fn indirect_to_dirty_formula_first_evaluation() {
    let mut e = engine();
    set(&mut e, 1, 1, 3.0);
    formula(&mut e, 1, 2, "=INDIRECT(\"C1\")*2");
    formula(&mut e, 1, 3, "=A1+1");
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 2), Some(8.0));
    set(&mut e, 1, 1, 10.0);
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 3), Some(11.0));
    assert_eq!(num(&e, 1, 2), Some(22.0));
}

/// §8.5(2): the INDIRECT target changes between recalcs.
#[test]
fn indirect_target_changes_between_recalcs() {
    let mut e = engine();
    set(&mut e, 1, 1, 1.0);
    formula(&mut e, 2, 1, "=A1*10");
    formula(&mut e, 3, 1, "=A1*100");
    e.engine_set_text("D1", "A2");
    formula(&mut e, 1, 2, "=INDIRECT(D1)+1");
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 2), Some(11.0));
    e.engine_set_text("D1", "A3");
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 2), Some(101.0));
    set(&mut e, 1, 1, 2.0);
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 2), Some(201.0));
}

/// §8.5(11a): X dynamically reads dirty Y outside the static closure; static
/// Z reads X. Z must end fresh.
#[test]
fn dynamic_to_static_chain_no_stale_clean() {
    let mut e = engine();
    set(&mut e, 1, 1, 1.0);
    formula(&mut e, 5, 1, "=A1+100"); // Y
    formula(&mut e, 1, 2, "=OFFSET(A1,4,0)*2"); // X reads Y dynamically
    formula(&mut e, 1, 3, "=B1+1"); // Z
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 1, 3), Some(203.0));
    set(&mut e, 1, 1, 5.0);
    e.evaluate_all().unwrap();
    assert_eq!(num(&e, 5, 1), Some(105.0));
    assert_eq!(num(&e, 1, 2), Some(210.0));
    assert_eq!(num(&e, 1, 3), Some(211.0));
}

/// §8.5(11c): a two-cycle first discovered through a dynamic read, with no
/// external dirty read.
#[test]
fn first_discovered_dynamic_two_cycle_is_a_cycle() {
    let mut e = engine();
    formula(&mut e, 1, 1, "=INDIRECT(\"B1\")+1");
    formula(&mut e, 1, 2, "=A1+1");
    e.evaluate_all().unwrap();
    assert!(is_circ(&e, 1, 1) && is_circ(&e, 1, 2));
}

/// §8.5(6): a targeted evaluation whose dynamic precedent lies outside the
/// initial static closure.
#[test]
fn demand_late_precedent_outside_closure() {
    let mut e = engine();
    set(&mut e, 1, 1, 2.0);
    formula(&mut e, 7, 1, "=A1*3");
    formula(&mut e, 1, 2, "=INDIRECT(\"A7\")+1");
    e.evaluate_all().unwrap();
    set(&mut e, 1, 1, 4.0);
    e.evaluate_targets(&[EvaluationTarget::Cell {
        sheet: "Sheet1".into(),
        row: 1,
        col: 2,
    }])
    .unwrap();
    assert_eq!(num(&e, 7, 1), Some(12.0));
    assert_eq!(num(&e, 1, 2), Some(13.0));
}

trait SetText {
    fn engine_set_text(&mut self, a1: &str, text: &str);
}

impl SetText for Engine<TestWorkbook> {
    fn engine_set_text(&mut self, a1: &str, text: &str) {
        let col = u32::from(a1.as_bytes()[0] - b'A') + 1;
        let row: u32 = a1[1..].parse().unwrap();
        self.set_cell_value("Sheet1", row, col, LiteralValue::Text(text.into()))
            .unwrap();
    }
}
