//! Bounded interactive history regression tests for issue #366.
use formualizer_common::LiteralValue;
use formualizer_workbook::{IoError, Workbook, WorkbookConfig};

fn abort_err() -> IoError {
    IoError::Backend {
        backend: "test".into(),
        message: "deliberate abort".into(),
    }
}

fn num(wb: &Workbook, sheet: &str, row: u32, col: u32) -> f64 {
    match wb.get_value(sheet, row, col) {
        Some(LiteralValue::Number(n)) => n,
        Some(LiteralValue::Int(i)) => i as f64,
        other => panic!("expected number at {sheet}!R{row}C{col}, got {other:?}"),
    }
}

#[test]
fn default_interactive_workbook_stays_bounded_over_many_single_cell_edits() {
    let mut wb = Workbook::new();
    let expected_cap = WorkbookConfig::DEFAULT_INTERACTIVE_HISTORY_LIMIT;
    assert_eq!(wb.history_limit(), Some(expected_cap));

    wb.add_sheet("S").unwrap();
    wb.set_formula("S", 1, 2, "=A1*2").unwrap();

    let total_edits = 55_000u32;
    for i in 0..total_edits {
        wb.set_value("S", 1, 1, LiteralValue::Number(i as f64))
            .unwrap();
    }

    assert_eq!(wb.changelog().len(), expected_cap);
    assert_eq!(
        wb.evaluate_cell("S", 1, 2).unwrap(),
        LiteralValue::Number(((total_edits - 1) * 2) as f64)
    );

    // Recent edits remain undoable and redoable.
    wb.undo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), (total_edits - 2) as f64);
    wb.redo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), (total_edits - 1) as f64);
}

#[test]
fn undo_redo_consistency_across_eviction_boundary_single_cell_edits() {
    let mut wb =
        Workbook::new_with_config(WorkbookConfig::interactive().with_history_limit(Some(5)));
    wb.add_sheet("S").unwrap();

    // 12 edits to distinct cells under cap=5 -> only edits 8..=12 retained.
    for i in 1..=12u32 {
        wb.set_value("S", i, 1, LiteralValue::Number(i as f64))
            .unwrap();
    }
    assert_eq!(wb.changelog().len(), 5);

    for _ in 0..5 {
        wb.undo().unwrap();
    }
    assert_eq!(wb.changelog().len(), 0);

    for i in 1..=7u32 {
        assert_eq!(num(&wb, "S", i, 1), i as f64);
    }
    for i in 8..=12u32 {
        assert!(matches!(
            wb.get_value("S", i, 1),
            None | Some(LiteralValue::Empty)
        ));
    }

    // Further undo past the boundary is a clean no-op.
    wb.undo().unwrap();
    wb.undo().unwrap();
    for i in 1..=7u32 {
        assert_eq!(num(&wb, "S", i, 1), i as f64);
    }

    // Redo all 5 restored edits cleanly.
    for _ in 0..5 {
        wb.redo().unwrap();
    }
    for i in 1..=12u32 {
        assert_eq!(num(&wb, "S", i, 1), i as f64);
    }

    // Further redo is a clean no-op, and another undo cycle still works.
    wb.redo().unwrap();
    for _ in 0..5 {
        wb.undo().unwrap();
    }
    for i in 8..=12u32 {
        assert!(matches!(
            wb.get_value("S", i, 1),
            None | Some(LiteralValue::Empty)
        ));
    }
}

#[test]
fn undo_redo_consistency_across_eviction_boundary_action_journal_mode() {
    let mut cfg = WorkbookConfig::ephemeral().with_history_limit(Some(5));
    cfg.enable_changelog = false;
    let mut wb = Workbook::new_with_config(cfg);
    wb.add_sheet("S").unwrap();

    for i in 1..=12u32 {
        wb.action("set", |tx| {
            tx.set_value("S", i, 1, LiteralValue::Number(i as f64))
        })
        .unwrap();
    }
    assert_eq!(wb.changelog().len(), 0);

    for _ in 0..5 {
        wb.undo().unwrap();
    }
    for i in 1..=7u32 {
        assert_eq!(num(&wb, "S", i, 1), i as f64);
    }
    for i in 8..=12u32 {
        assert!(matches!(
            wb.get_value("S", i, 1),
            None | Some(LiteralValue::Empty)
        ));
    }

    // Further undo is a clean no-op.
    wb.undo().unwrap();
    for i in 1..=7u32 {
        assert_eq!(num(&wb, "S", i, 1), i as f64);
    }

    // Redo all 5 cleanly.
    for _ in 0..5 {
        wb.redo().unwrap();
    }
    for i in 1..=12u32 {
        assert_eq!(num(&wb, "S", i, 1), i as f64);
    }
}

#[test]
fn compound_actions_and_rollback_under_small_cap() {
    // Each 2-cell compound action emits 4 events: Start, Set, Set, End.
    // With cap=5, action 1 is partially evicted (1 event left) and action 2 is intact (4 events).
    let mut wb =
        Workbook::new_with_config(WorkbookConfig::interactive().with_history_limit(Some(5)));
    wb.add_sheet("S").unwrap();

    wb.begin_action("action-1");
    wb.set_value("S", 1, 1, LiteralValue::Number(10.0)).unwrap();
    wb.set_value("S", 1, 2, LiteralValue::Number(20.0)).unwrap();
    wb.end_action();

    wb.begin_action("action-2");
    wb.set_value("S", 2, 1, LiteralValue::Number(30.0)).unwrap();
    wb.set_value("S", 2, 2, LiteralValue::Number(40.0)).unwrap();
    wb.end_action();

    assert_eq!(wb.changelog().len(), 5);

    // Failed action under saturated cap must roll back cleanly without evicting history.
    let len_before_abort = wb.changelog().len();
    let err = wb
        .action("abort-action", |tx| {
            tx.set_value("S", 2, 1, LiteralValue::Number(999.0))?;
            tx.set_value("S", 3, 1, LiteralValue::Number(888.0))?;
            Err::<(), _>(abort_err())
        })
        .unwrap_err();
    assert!(err.to_string().contains("deliberate abort"));
    assert_eq!(wb.changelog().len(), len_before_abort);
    assert_eq!(num(&wb, "S", 2, 1), 30.0);
    assert!(matches!(
        wb.get_value("S", 3, 1),
        None | Some(LiteralValue::Empty)
    ));

    // Undo action-2 cleanly restores both of its cells.
    wb.undo().unwrap();
    assert!(matches!(
        wb.get_value("S", 2, 1),
        None | Some(LiteralValue::Empty)
    ));
    assert!(matches!(
        wb.get_value("S", 2, 2),
        None | Some(LiteralValue::Empty)
    ));

    // Action-1 was partially evicted, so further undo must be a clean no-op (never half-undone).
    wb.undo().unwrap();
    assert_eq!(num(&wb, "S", 1, 1), 10.0);
    assert_eq!(num(&wb, "S", 1, 2), 20.0);

    // Redo restores action-2 cleanly.
    wb.redo().unwrap();
    assert_eq!(num(&wb, "S", 2, 1), 30.0);
    assert_eq!(num(&wb, "S", 2, 2), 40.0);
}

#[test]
fn spill_action_history_handles_front_eviction_inside_open_compound() {
    let mut wb =
        Workbook::new_with_config(WorkbookConfig::interactive().with_history_limit(Some(4)));
    wb.add_sheet("S").unwrap();
    wb.set_formula("S", 1, 2, "=SEQUENCE(5)").unwrap();
    wb.evaluate_all().unwrap();

    wb.begin_action("oversized-compound");
    // Overwrite one spill child (triggers SpillCleared), then emit >cap edits before set_formula.
    wb.set_value("S", 3, 2, LiteralValue::Number(99.0)).unwrap();
    for r in 10..20u32 {
        wb.set_value("S", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    wb.set_formula("S", 4, 2, "=88").unwrap();
    wb.end_action();

    assert_eq!(wb.changelog().len(), 4);
    assert_eq!(num(&wb, "S", 3, 2), 99.0);
    assert_eq!(wb.get_formula("S", 4, 2), Some("=88".to_string()));
}

#[test]
fn history_limit_opt_out_and_runtime_update() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::interactive().with_history_limit(None));
    assert_eq!(wb.history_limit(), None);
    wb.add_sheet("S").unwrap();

    for i in 0..12_000u32 {
        wb.set_value("S", 1, 1, LiteralValue::Number(i as f64))
            .unwrap();
    }
    assert_eq!(wb.changelog().len(), 12_000);

    wb.set_history_limit(Some(5));
    assert_eq!(wb.history_limit(), Some(5));
    assert_eq!(wb.changelog().len(), 5);

    for _ in 0..5 {
        wb.undo().unwrap();
    }
    assert_eq!(num(&wb, "S", 1, 1), 11_994.0);
}
