//! Iterate edge corpus — workbook-surface interactions (RFC #112/#113,
//! spec §7.15 undo/redo × iterative calculation).
//!
//! PINNED SEMANTICS (and a spec gap, see the findings report): workbook
//! `undo()` restores USER EDITS only — it does not roll back values that an
//! iterated recalc committed. Spec §7.15 reads "values restore to pre-recalc
//! state on undo (one changelog entry per cell per recalc)", but the SCC
//! commit path emits no changelog entries, so iterated cycle state survives
//! undo and the cycle simply continues from its current value with the
//! restored inputs (which is also what Excel does — Excel never undoes
//! calculation). Until the spec text or the implementation moves, this file
//! pins the implemented behavior.

use formualizer_eval::engine::CycleConfig;
use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};

fn iterate_workbook(max_iterations: u32, max_change: f64) -> Workbook {
    let mut config = WorkbookConfig::ephemeral();
    config.enable_changelog = true; // undo/redo journaling
    config.eval = config
        .eval
        .with_cycle(CycleConfig::iterate(max_iterations, max_change));
    Workbook::new_with_config(config)
}

fn num(wb: &Workbook, sheet: &str, row: u32, col: u32) -> f64 {
    match wb.get_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet} r{row}c{col}, got {other:?}"),
    }
}

#[test]
fn undo_restores_edits_but_not_iterated_cycle_state() {
    // Accumulator B1 = B1 + A1, cap 1: adds A1 exactly once per recalc.
    let mut wb = iterate_workbook(1, 0.001);
    wb.add_sheet("S").unwrap();
    wb.begin_action("seed");
    wb.set_value("S", 1, 1, LiteralValue::Number(5.0)).unwrap();
    wb.set_formula("S", 1, 2, "=B1+A1").unwrap();
    wb.end_action();
    wb.evaluate_all().unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(num(&wb, "S", 1, 2), 10.0, "two recalcs × 5");

    // Edit the input inside an undoable action, recalc once.
    wb.begin_action("bump A1");
    wb.set_value("S", 1, 1, LiteralValue::Number(7.0)).unwrap();
    wb.end_action();
    wb.evaluate_all().unwrap();
    assert_eq!(num(&wb, "S", 1, 2), 17.0, "10 + 7");

    // Undo restores A1 = 5 but NOT the iterated value (B1 stays 17): the
    // recalc's cycle-state writes are not journaled (spec-§7.15 gap, pinned).
    wb.undo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 5.0, "edit undone");
    assert_eq!(num(&wb, "S", 1, 2), 17.0, "iterated state NOT rolled back");

    // The cycle continues from its current state with the restored input.
    wb.evaluate_all().unwrap();
    assert_eq!(num(&wb, "S", 1, 2), 22.0, "17 + restored 5");

    // Redo re-applies the edit; iteration keeps walking forward.
    wb.redo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 7.0);
    assert_eq!(num(&wb, "S", 1, 2), 22.0);
    wb.evaluate_all().unwrap();
    assert_eq!(num(&wb, "S", 1, 2), 29.0, "22 + 7");
}

#[test]
fn undo_of_the_formula_itself_dissolves_the_cycle_and_stops_redirty() {
    // Undoing the action that CREATED the circular formula removes the SCC;
    // later recalcs schedule nothing and the cell reverts to its pre-action
    // state (empty).
    let mut wb = iterate_workbook(1, 0.001);
    wb.add_sheet("S").unwrap();
    wb.begin_action("input");
    wb.set_value("S", 1, 1, LiteralValue::Number(5.0)).unwrap();
    wb.end_action();
    wb.begin_action("circular formula");
    wb.set_formula("S", 1, 2, "=B1+A1").unwrap();
    wb.end_action();
    wb.evaluate_all().unwrap();
    assert_eq!(num(&wb, "S", 1, 2), 5.0);

    wb.undo().unwrap();
    assert_eq!(wb.get_formula("S", 1, 2), None, "formula gone");
    wb.evaluate_all().unwrap();
    // No SCC anywhere; the input cell is untouched.
    assert_eq!(num(&wb, "S", 1, 1), 5.0);
    assert_eq!(wb.get_formula("S", 1, 2), None);

    // Redo restores the formula; iteration resumes from the (empty) seed.
    wb.redo().unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(num(&wb, "S", 1, 2), 5.0, "fresh accumulator after redo");
}
