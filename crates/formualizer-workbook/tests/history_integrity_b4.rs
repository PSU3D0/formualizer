//! Regression tests for Red-Team B4 history integrity invariants:
//! 1. Disabling the changelog clears logged undo/redo history so a subsequent
//!    unlogged edit cannot be silently reverted after re-enabling.
//! 2. Any unjournaled mutation clears action-journal undo/redo stacks too.
//! 3. Toggling the changelog while a compound action is open closes the
//!    compound first so later edits are never merged into a stranded compound.

use formualizer_common::LiteralValue;
use formualizer_workbook::{Workbook, WorkbookConfig};

fn num(wb: &Workbook, sheet: &str, row: u32, col: u32) -> f64 {
    match wb.get_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet}!R{row}C{col}, got {other:?}"),
    }
}

#[test]
fn b4_scenario1_disable_and_unlogged_write_cannot_revert_on_reenable() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    // Logged edit A1: 1 -> 2
    wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Number(2.0)).unwrap();
    assert!(!wb.changelog().is_empty());

    // Disable changelog -> clears logged history; unlogged set_values writes 100
    wb.set_changelog_enabled(false);
    assert!(wb.changelog().is_empty());
    wb.set_values("S", 1, 1, &[vec![LiteralValue::Number(100.0)]])
        .unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 100.0);

    // Re-enable and undo -> must be a no-op, never reverting 100 back to 1
    wb.set_changelog_enabled(true);
    wb.undo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 100.0);

    // Redo must also be a no-op (stale logged redo cleared on disable)
    wb.redo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 100.0);
}

#[test]
fn b4_scenario1b_disable_clears_logged_redo_stack() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Number(2.0)).unwrap();
    wb.undo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 1.0);

    // Disabling must clear the logged redo stack too.
    wb.set_changelog_enabled(false);
    wb.set_value("S", 1, 1, LiteralValue::Number(50.0)).unwrap();
    wb.set_changelog_enabled(true);
    wb.redo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 50.0);
}

#[test]
fn b4_scenario2_unjournaled_write_clears_action_journal_when_changelog_off() {
    let mut cfg = WorkbookConfig::interactive();
    cfg.enable_changelog = false;
    let mut wb = Workbook::new_with_config(cfg);
    wb.add_sheet("S").unwrap();

    wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
    wb.action("set-two", |tx| {
        tx.set_value("S", 1, 1, LiteralValue::Number(2.0))
    })
    .unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 2.0);

    // Unjournaled set_value puts 100 in A1; this breaks contiguity and must
    // clear the action-journal undo/redo stacks.
    wb.set_value("S", 1, 1, LiteralValue::Number(100.0))
        .unwrap();
    wb.undo().unwrap();
    assert_eq!(
        num(&wb, "S", 1, 1),
        100.0,
        "undo must not replay stale action journal over an unjournaled write"
    );

    // Same for action-journal redo stack: undo an action, then make an
    // unjournaled write, then redo must not overwrite the unjournaled write.
    wb.action("set-three", |tx| {
        tx.set_value("S", 1, 1, LiteralValue::Number(3.0))
    })
    .unwrap();
    wb.undo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 100.0);
    wb.set_values("S", 1, 1, &[vec![LiteralValue::Number(200.0)]])
        .unwrap();
    wb.redo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 200.0);
}

#[test]
fn b4_scenario3_toggle_while_compound_open_closes_compound_first() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    wb.begin_action("open-compound");
    wb.set_value("S", 1, 1, LiteralValue::Number(10.0)).unwrap();
    assert_eq!(wb.changelog().compound_depth(), 1);

    // Disabling closes the open compound first and clears logged history.
    wb.set_changelog_enabled(false);
    assert_eq!(wb.changelog().compound_depth(), 0);

    // end_action while disabled is a no-op.
    wb.end_action();
    assert_eq!(wb.changelog().compound_depth(), 0);

    // Re-enabling starts cleanly with compound_depth == 0.
    wb.set_changelog_enabled(true);
    assert_eq!(wb.changelog().compound_depth(), 0);

    wb.set_value("S", 2, 1, LiteralValue::Number(20.0)).unwrap();
    wb.set_value("S", 3, 1, LiteralValue::Number(30.0)).unwrap();

    // One undo must revert ONLY the last edit (row 3), leaving row 2 intact.
    wb.undo().unwrap();
    assert_eq!(num(&wb, "S", 2, 1), 20.0);
    assert!(matches!(
        wb.get_value("S", 3, 1),
        None | Some(LiteralValue::Empty)
    ));
}

#[test]
fn b4_scenario3b_redundant_enable_while_compound_open_closes_compound() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    wb.begin_action("open-compound");
    wb.set_value("S", 1, 1, LiteralValue::Number(10.0)).unwrap();
    assert_eq!(wb.changelog().compound_depth(), 1);

    // Calling set_changelog_enabled(true) while compound_depth > 0 closes it first.
    wb.set_changelog_enabled(true);
    assert_eq!(wb.changelog().compound_depth(), 0);

    wb.set_value("S", 2, 1, LiteralValue::Number(20.0)).unwrap();
    wb.undo().unwrap();
    assert!(matches!(
        wb.get_value("S", 2, 1),
        None | Some(LiteralValue::Empty)
    ));
    assert_eq!(num(&wb, "S", 1, 1), 10.0);
}

#[test]
fn changelog_inspection_and_with_changelog_builder_work_across_modes() {
    // Default interactive workbook still has changelog enabled in v0.15.1.
    let mut wb = Workbook::new();
    assert!(wb.changelog_enabled());
    assert!(!wb.can_undo());
    assert!(!wb.can_redo());

    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
    assert!(wb.can_undo());
    assert!(!wb.can_redo());

    wb.undo().unwrap();
    assert!(!wb.can_undo());
    assert!(wb.can_redo());

    // A new forward edit clears the redo stack immediately.
    wb.set_value("S", 1, 1, LiteralValue::Number(2.0)).unwrap();
    assert!(wb.can_undo());
    assert!(!wb.can_redo());

    // Disabling clears logged history so can_undo/can_redo drop to false.
    wb.set_changelog_enabled(false);
    assert!(!wb.changelog_enabled());
    assert!(!wb.can_undo());
    assert!(!wb.can_redo());

    // Opting out via WorkbookConfig::interactive().with_changelog(false).
    let mut wb_off = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
    assert!(!wb_off.changelog_enabled());
    assert!(!wb_off.can_undo());
    assert!(!wb_off.can_redo());
    wb_off.add_sheet("S").unwrap();
    wb_off
        .action("a1", |tx| {
            tx.set_value("S", 1, 1, LiteralValue::Number(7.0))
        })
        .unwrap();
    assert!(wb_off.can_undo());
    assert!(!wb_off.can_redo());
    wb_off.undo().unwrap();
    assert!(!wb_off.can_undo());
    assert!(wb_off.can_redo());
    // Unjournaled mutation clears action-journal undo/redo stacks.
    wb_off
        .set_value("S", 1, 1, LiteralValue::Number(9.0))
        .unwrap();
    assert!(!wb_off.can_undo());
    assert!(!wb_off.can_redo());
}
