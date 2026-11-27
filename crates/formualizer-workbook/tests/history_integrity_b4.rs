//! Regression tests for Red-Team B4 history integrity invariants:
//! 1. Disabling the changelog clears logged undo/redo history so a subsequent
//!    unlogged edit cannot be silently reverted after re-enabling.
//! 2. Any unjournaled mutation clears action-journal undo/redo stacks too.
//! 3. Toggling the changelog while a compound action is open closes the
//!    compound first so later edits are never merged into a stranded compound.

use formualizer_common::LiteralValue;
use formualizer_workbook::{CellData, IoError, Workbook, WorkbookConfig};
use std::collections::BTreeMap;

fn logged_workbook() -> Workbook {
    Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(true))
}

fn num(wb: &Workbook, sheet: &str, row: u32, col: u32) -> f64 {
    match wb.get_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet}!R{row}C{col}, got {other:?}"),
    }
}

#[test]
fn b4_scenario1_disable_and_unlogged_write_cannot_revert_on_reenable() {
    let mut wb = logged_workbook();
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
    let mut wb = logged_workbook();
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
    let mut wb = Workbook::new();
    assert!(!wb.changelog_enabled());
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
    assert!(matches!(wb.undo(), Err(IoError::HistoryDisabled)));
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
    assert!(matches!(wb.redo(), Err(IoError::HistoryDisabled)));
    assert_eq!(num(&wb, "S", 1, 1), 200.0);
}

#[test]
fn b4_scenario3_toggle_while_compound_open_closes_compound_first() {
    let mut wb = logged_workbook();
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
    let mut wb = logged_workbook();
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
    // Default interactive workbook has changelog disabled in v0.16.0, and undo/redo error explicitly.
    let mut wb_default = Workbook::new();
    assert!(!wb_default.changelog_enabled());
    assert!(!wb_default.can_undo());
    assert!(!wb_default.can_redo());
    wb_default.begin_action("noop");
    wb_default.end_action();
    assert!(matches!(wb_default.undo(), Err(IoError::HistoryDisabled)));
    assert!(matches!(wb_default.redo(), Err(IoError::HistoryDisabled)));

    // Opting in via WorkbookConfig::interactive().with_changelog(true).
    let mut wb = logged_workbook();
    assert!(wb.changelog_enabled());
    assert!(!wb.can_undo());
    assert!(!wb.can_redo());

    // When changelog is enabled, undo/redo with nothing recorded remain Ok(()) no-ops.
    wb.undo().unwrap();
    wb.redo().unwrap();

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

    // Disabling clears logged history so can_undo/can_redo drop to false and undo/redo error.
    wb.set_changelog_enabled(false);
    assert!(!wb.changelog_enabled());
    assert!(!wb.can_undo());
    assert!(!wb.can_redo());
    assert!(matches!(wb.undo(), Err(IoError::HistoryDisabled)));
    assert!(matches!(wb.redo(), Err(IoError::HistoryDisabled)));

    // Explicit action journals still replay when changelog is off.
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
    assert!(matches!(wb_off.undo(), Err(IoError::HistoryDisabled)));
    assert!(matches!(wb_off.redo(), Err(IoError::HistoryDisabled)));
}

#[test]
fn b4_f2_failing_unlogged_batch_writes_invalidate_action_history_before_partial_prefix() {
    // 1. set_formulas failing mid-batch (Fable's exact repro).
    {
        let mut cfg = WorkbookConfig::interactive().with_changelog(false);
        cfg.eval.defer_graph_building = false;
        let mut wb = Workbook::new_with_config(cfg);
        wb.add_sheet("S").unwrap();

        wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
        wb.action("set-two", |tx| {
            tx.set_value("S", 1, 1, LiteralValue::Number(2.0))
        })
        .unwrap();
        assert!(wb.can_undo());

        let err = wb.set_formulas("S", 1, 1, &[vec!["=100".into()], vec!["=((".into()]]);
        assert!(err.is_err(), "malformed second formula must fail");
        assert_eq!(wb.get_formula("S", 1, 1).as_deref(), Some("=100"));
        assert!(!wb.can_undo(), "partial unlogged batch must clear history");

        assert!(matches!(wb.undo(), Err(IoError::HistoryDisabled)));
        assert_eq!(
            wb.get_formula("S", 1, 1).as_deref(),
            Some("=100"),
            "undo must not revert partially committed set_formulas prefix back to 1"
        );
        wb.evaluate_all().unwrap();
        assert_eq!(num(&wb, "S", 1, 1), 100.0);
    }

    // 2. write_range failing mid-batch.
    {
        let mut cfg = WorkbookConfig::interactive().with_changelog(false);
        cfg.eval.defer_graph_building = false;
        let mut wb = Workbook::new_with_config(cfg);
        wb.add_sheet("S").unwrap();

        wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
        wb.action("set-two", |tx| {
            tx.set_value("S", 1, 1, LiteralValue::Number(2.0))
        })
        .unwrap();
        assert!(wb.can_undo());

        let mut cells = BTreeMap::new();
        cells.insert((1, 1), CellData::from_formula("=100"));
        cells.insert((2, 1), CellData::from_formula("=(("));
        let err = wb.write_range("S", (1, 1), cells);
        assert!(err.is_err(), "malformed second formula must fail");
        assert_eq!(wb.get_formula("S", 1, 1).as_deref(), Some("=100"));
        assert!(
            !wb.can_undo(),
            "partial unlogged write_range must clear history"
        );

        assert!(matches!(wb.undo(), Err(IoError::HistoryDisabled)));
        assert_eq!(
            wb.get_formula("S", 1, 1).as_deref(),
            Some("=100"),
            "undo must not revert partially committed write_range prefix back to 1"
        );
        wb.evaluate_all().unwrap();
        assert_eq!(num(&wb, "S", 1, 1), 100.0);
    }

    // 3. set_values unlogged batch invalidates action-journal undo/redo stacks before writing.
    {
        let mut cfg = WorkbookConfig::interactive().with_changelog(false);
        cfg.eval.defer_graph_building = false;
        let mut wb = Workbook::new_with_config(cfg);
        wb.add_sheet("S").unwrap();

        wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
        wb.action("set-two", |tx| {
            tx.set_value("S", 1, 1, LiteralValue::Number(2.0))
        })
        .unwrap();
        assert!(wb.can_undo());

        wb.set_values(
            "S",
            1,
            1,
            &[
                vec![LiteralValue::Number(100.0)],
                vec![LiteralValue::Number(200.0)],
            ],
        )
        .unwrap();
        assert!(!wb.can_undo());
        assert!(matches!(wb.undo(), Err(IoError::HistoryDisabled)));
        assert_eq!(num(&wb, "S", 1, 1), 100.0);
        assert_eq!(num(&wb, "S", 2, 1), 200.0);
    }
}

#[test]
fn b4_f2_failing_logged_batch_clears_prior_action_and_redo_stacks() {
    // Logged twin hole for both set_formulas and write_range:
    // action-journal from an earlier changelog-off period must not survive a
    // partially committed logged batch and replay after disabling changelog again.
    for use_write_range in [false, true] {
        let mut cfg = WorkbookConfig::interactive().with_changelog(false);
        cfg.eval.defer_graph_building = false;
        let mut wb = Workbook::new_with_config(cfg);
        wb.add_sheet("S").unwrap();

        wb.set_value("S", 1, 1, LiteralValue::Number(1.0)).unwrap();
        wb.action("set-two", |tx| {
            tx.set_value("S", 1, 1, LiteralValue::Number(2.0))
        })
        .unwrap();
        assert!(wb.can_undo());

        wb.set_changelog_enabled(true);
        // Also seed a logged redo entry so we confirm can_redo drops to false too.
        wb.set_value("S", 3, 1, LiteralValue::Number(5.0)).unwrap();
        wb.undo().unwrap();
        assert!(wb.can_redo());

        // Re-seed action journal while changelog off, then re-enable and run failing logged batch.
        wb.set_changelog_enabled(false);
        wb.action("set-three", |tx| {
            tx.set_value("S", 1, 1, LiteralValue::Number(3.0))
        })
        .unwrap();
        assert!(wb.can_undo());

        wb.set_changelog_enabled(true);
        let err = if use_write_range {
            let mut cells = BTreeMap::new();
            cells.insert((1, 1), CellData::from_formula("=100"));
            cells.insert((2, 1), CellData::from_formula("=(("));
            wb.write_range("S", (1, 1), cells)
        } else {
            wb.set_formulas("S", 1, 1, &[vec!["=100".into()], vec!["=((".into()]])
        };
        assert!(err.is_err());
        assert_eq!(wb.get_formula("S", 1, 1).as_deref(), Some("=100"));
        assert!(!wb.can_redo());

        // Disable changelog again: the old action journal must have been cleared.
        wb.set_changelog_enabled(false);
        assert!(!wb.can_undo());
        assert!(matches!(wb.undo(), Err(IoError::HistoryDisabled)));
        assert_eq!(
            wb.get_formula("S", 1, 1).as_deref(),
            Some("=100"),
            "stale action journal must not replay after a partially committed logged batch"
        );
    }
}

#[test]
fn d1_row_visibility_is_journaled_when_changelog_on_and_invalidates_when_off() {
    // Changelog ON: set_row_hidden / set_rows_hidden preserve prior undo history and are undoable.
    let mut wb = logged_workbook();
    wb.add_sheet("S").unwrap();

    wb.set_value("S", 1, 1, LiteralValue::Number(10.0)).unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Number(20.0)).unwrap();
    let len_before = wb.changelog().len();
    assert!(len_before >= 2);

    wb.set_row_hidden("S", 2, true).unwrap();
    assert!(wb.is_row_hidden("S", 2).unwrap());
    assert!(
        wb.changelog().len() > len_before,
        "set_row_hidden must append to changelog instead of clearing it"
    );
    assert!(wb.can_undo());

    wb.set_rows_hidden("S", 4, 5, true).unwrap();
    assert!(wb.is_row_hidden("S", 4).unwrap());
    assert!(wb.is_row_hidden("S", 5).unwrap());

    // Undo set_rows_hidden -> rows 4..=5 unhidden, row 2 still hidden, A1 still 20.
    wb.undo().unwrap();
    assert!(!wb.is_row_hidden("S", 4).unwrap());
    assert!(!wb.is_row_hidden("S", 5).unwrap());
    assert!(wb.is_row_hidden("S", 2).unwrap());
    assert_eq!(num(&wb, "S", 1, 1), 20.0);

    // Undo set_row_hidden -> row 2 unhidden, A1 still 20.
    wb.undo().unwrap();
    assert!(!wb.is_row_hidden("S", 2).unwrap());
    assert_eq!(num(&wb, "S", 1, 1), 20.0);

    // Undo prior cell edit -> A1 reverts to 10 (prior history preserved!).
    wb.undo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 10.0);

    // Redo replays cell edit and row visibility.
    wb.redo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 20.0);
    wb.redo().unwrap();
    assert!(wb.is_row_hidden("S", 2).unwrap());

    // Changelog OFF: direct set_row_hidden / set_rows_hidden still invalidates action history.
    let mut wb_off = Workbook::new_with_config(WorkbookConfig::interactive().with_changelog(false));
    wb_off.add_sheet("S").unwrap();
    wb_off
        .action("set-a1", |tx| {
            tx.set_value("S", 1, 1, LiteralValue::Number(7.0))
        })
        .unwrap();
    assert!(wb_off.can_undo());
    wb_off.set_row_hidden("S", 2, true).unwrap();
    assert!(!wb_off.can_undo());
    wb_off
        .action("set-a1-again", |tx| {
            tx.set_value("S", 1, 1, LiteralValue::Number(8.0))
        })
        .unwrap();
    assert!(wb_off.can_undo());
    wb_off.set_rows_hidden("S", 4, 5, true).unwrap();
    assert!(!wb_off.can_undo());
}
