//! Tests for the standalone ChangeLog implementation

use crate::engine::addr::GridAddr;

use crate::engine::graph::editor::change_log::{
    ChangeEvent, ChangeLog, ChangeLogger, NullChangeLogger,
};
use crate::reference::{CellRef, Coord};
use formualizer_common::LiteralValue;

fn cell_ref(sheet_id: u16, row: u32, col: u32) -> CellRef {
    CellRef::new(sheet_id, Coord::new(row, col, false, false))
}

fn create_test_event() -> ChangeEvent {
    ChangeEvent::SetValue {
        addr: cell_ref(0, 1, 1),
        old_value: None,
        old_formula: None,
        new: LiteralValue::Number(42.0),
    }
}

fn create_test_event_with_value(value: f64) -> ChangeEvent {
    ChangeEvent::SetValue {
        addr: cell_ref(0, 1, 1),
        old_value: None,
        old_formula: None,
        new: LiteralValue::Number(value),
    }
}

#[test]
fn test_change_log_basic_operations() {
    let mut log = ChangeLog::new();
    assert!(log.is_empty());

    let event = create_test_event();

    log.record(event.clone());
    assert_eq!(log.len(), 1);
    assert_eq!(log.events()[0], event);

    log.clear();
    assert!(log.is_empty());
}

#[test]
fn test_change_log_enabled_flag() {
    let mut log = ChangeLog::new();

    log.set_enabled(false);
    log.record(create_test_event());
    assert!(log.is_empty()); // Not recorded when disabled

    log.set_enabled(true);
    log.record(create_test_event());
    assert_eq!(log.len(), 1); // Recorded when enabled
}

#[test]
fn test_change_log_take_from() {
    let mut log = ChangeLog::new();

    for i in 0..5 {
        log.record(create_test_event_with_value(i as f64));
    }

    let taken = log.take_from(3);
    assert_eq!(taken.len(), 2); // Took events at index 3 and 4
    assert_eq!(log.len(), 3); // Events 0, 1, 2 remain
}

#[test]
fn test_change_log_fifo_eviction_drops_oldest() {
    let mut log = ChangeLog::with_max_changelog_events(3);

    for i in 0..5 {
        log.record(create_test_event_with_value(i as f64));
    }

    assert_eq!(log.len(), 3);
    let values: Vec<f64> = log
        .events()
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::SetValue {
                new: LiteralValue::Number(n),
                ..
            } => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec![2.0, 3.0, 4.0]);
}

#[test]
fn test_compound_operations() {
    let mut log = ChangeLog::new();

    log.begin_compound("InsertRows".to_string());
    assert_eq!(log.compound_depth(), 1);

    log.record(create_test_event());

    // Nested compound
    log.begin_compound("NestedOp".to_string());
    assert_eq!(log.compound_depth(), 2);
    log.record(create_test_event_with_value(2.0));
    log.end_compound();
    assert_eq!(log.compound_depth(), 1);

    log.end_compound();
    assert_eq!(log.compound_depth(), 0);

    // Check events were recorded
    let events = log.events();
    assert_eq!(events.len(), 6); // Start, event, nested start, event, nested end, end

    // Verify compound markers
    assert!(matches!(
        events[0],
        ChangeEvent::CompoundStart { depth: 1, .. }
    ));
    assert!(matches!(
        events[2],
        ChangeEvent::CompoundStart { depth: 2, .. }
    ));
    assert!(matches!(events[4], ChangeEvent::CompoundEnd { depth: 2 }));
    assert!(matches!(events[5], ChangeEvent::CompoundEnd { depth: 1 }));
}

#[test]
fn test_compound_operations_disabled() {
    let mut log = ChangeLog::new();
    log.set_enabled(false);

    log.begin_compound("InsertRows".to_string());
    log.record(create_test_event());
    log.end_compound();

    // Nothing should be recorded when disabled
    assert!(log.is_empty());
}

#[test]
fn test_null_logger() {
    let mut logger = NullChangeLogger;
    logger.record(create_test_event()); // Should not panic
    logger.set_enabled(false); // Should not panic
    logger.begin_compound("test".to_string()); // Should not panic
    logger.end_compound(); // Should not panic
}

#[test]
fn test_change_logger_trait_object() {
    let mut log = ChangeLog::new();
    let logger: &mut dyn ChangeLogger = &mut log;

    logger.record(create_test_event());
    // Verify through ChangeLog's methods
    assert_eq!(log.len(), 1);
}

#[test]
fn test_change_log_with_formula_events() {
    use formualizer_parse::parser::parse;

    let mut log = ChangeLog::new();

    let old_formula = parse("=A1*2").unwrap();
    let new_formula = parse("=A1*3").unwrap();

    let event = ChangeEvent::SetFormula {
        addr: cell_ref(0, 1, 1),
        old_value: None,
        old_formula: Some(old_formula.clone()),
        new: new_formula.clone(),
    };

    log.record(event.clone());
    assert_eq!(log.len(), 1);

    match &log.events()[0] {
        ChangeEvent::SetFormula {
            old_formula: old,
            new,
            ..
        } => {
            assert_eq!(old.as_ref(), Some(&old_formula));
            assert_eq!(new, &new_formula);
        }
        _ => panic!("Wrong event type"),
    }
}

#[test]
fn test_granular_change_events() {
    use crate::engine::vertex::VertexId;
    use formualizer_parse::parser::parse;

    let mut log = ChangeLog::new();

    // Test VertexMoved event
    let move_event = ChangeEvent::VertexMoved {
        id: VertexId(1),
        sheet_id: 0,
        old_coord: GridAddr::new(5, 1),
        new_coord: GridAddr::new(7, 1),
    };
    log.record(move_event);

    // Test FormulaAdjusted event
    let adjust_event = ChangeEvent::FormulaAdjusted {
        id: VertexId(2),
        addr: None,
        old_ast: parse("=A5").unwrap(),
        new_ast: parse("=A7").unwrap(),
    };
    log.record(adjust_event);

    // Test EdgeAdded event
    let edge_add = ChangeEvent::EdgeAdded {
        from: VertexId(1),
        to: VertexId(2),
    };
    log.record(edge_add);

    // Test EdgeRemoved event
    let edge_remove = ChangeEvent::EdgeRemoved {
        from: VertexId(3),
        to: VertexId(4),
    };
    log.record(edge_remove);

    assert_eq!(log.len(), 4);

    // Verify all events were recorded correctly
    assert!(matches!(log.events()[0], ChangeEvent::VertexMoved { .. }));
    assert!(matches!(
        log.events()[1],
        ChangeEvent::FormulaAdjusted { .. }
    ));
    assert!(matches!(log.events()[2], ChangeEvent::EdgeAdded { .. }));
    assert!(matches!(log.events()[3], ChangeEvent::EdgeRemoved { .. }));
}

#[test]
fn test_remove_vertex_event() {
    use crate::engine::vertex::VertexId;
    use formualizer_parse::parser::parse;

    let mut log = ChangeLog::new();

    let event = ChangeEvent::RemoveVertex {
        id: VertexId(1),
        old_value: Some(LiteralValue::Number(42.0)),
        old_formula: Some(parse("=A1*2").unwrap()),
        old_dependencies: vec![VertexId(2), VertexId(3)],
        old_dependents: vec![VertexId(4)],
        coord: None,
        sheet_id: None,
        kind: None,
        flags: None,
    };

    log.record(event);
    assert_eq!(log.len(), 1);

    match &log.events()[0] {
        ChangeEvent::RemoveVertex {
            id,
            old_value,
            old_formula,
            old_dependencies,
            ..
        } => {
            assert_eq!(id, &VertexId(1));
            assert_eq!(old_value, &Some(LiteralValue::Number(42.0)));
            assert!(old_formula.is_some());
            assert_eq!(old_dependencies.len(), 2);
        }
        _ => panic!("Wrong event type"),
    }
}

#[test]
fn test_change_log_amortized_eviction_and_dynamic_resize() {
    let mut log = ChangeLog::with_max_changelog_events(100);
    for i in 0..50_000 {
        log.set_actor_id(Some(format!("actor-{}", i % 7)));
        log.record(create_test_event_with_value(i as f64));
    }

    assert_eq!(log.len(), 100);
    let values: Vec<f64> = log
        .events()
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::SetValue {
                new: LiteralValue::Number(n),
                ..
            } => Some(*n),
            _ => None,
        })
        .collect();
    let expected: Vec<f64> = (49_900..50_000).map(|i| i as f64).collect();
    assert_eq!(values, expected);
    for idx in 0..100 {
        let (seq, group) = log.meta(idx).unwrap();
        assert_eq!(seq, (49_900 + idx) as u64);
        assert_eq!(group, None);
        assert_eq!(
            log.event_meta(idx).unwrap().actor_id.as_deref(),
            Some(format!("actor-{}", (49_900 + idx) % 7).as_str())
        );
    }

    log.set_max_changelog_events(Some(10));
    assert_eq!(log.len(), 10);
    let shrunk: Vec<f64> = log
        .events()
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::SetValue {
                new: LiteralValue::Number(n),
                ..
            } => Some(*n),
            _ => None,
        })
        .collect();
    let expected_shrunk: Vec<f64> = (49_990..50_000).map(|i| i as f64).collect();
    assert_eq!(shrunk, expected_shrunk);
}

#[test]
fn test_change_log_drain_front_evicts_and_slices_lazy_runs_without_expanding() {
    use crate::engine::graph::editor::change_log::{
        ChangeEventMeta, FormulaRunAdjusted, MutationCapture,
    };
    use crate::engine::vertex::VertexId;
    use formualizer_parse::parser::parse;

    let mut log = ChangeLog::with_max_changelog_events(6);

    let mut capture = MutationCapture::new(ChangeEventMeta {
        actor_id: Some("structural".to_string()),
        ..Default::default()
    });
    // 1 plain event (seq 0)
    capture.record(create_test_event_with_value(100.0));
    // Run 1: 4 events (seqs 1..=4) — will be wholly evicted
    capture.record_lazy(FormulaRunAdjusted {
        first: VertexId(10),
        len: 4,
        sheet_id: 0,
        col: 1,
        row0: 0,
        old_first: parse("=A1").unwrap(),
        new_first: parse("=A2").unwrap(),
    });
    // 1 plain event (seq 5) — will be evicted
    capture.record(create_test_event_with_value(200.0));
    // Run 2: 5 events (seqs 6..=10) — first 1 evicted (seq 6), remaining 4 retained (seqs 7..=10)
    capture.record_lazy(FormulaRunAdjusted {
        first: VertexId(100),
        len: 5,
        sheet_id: 0,
        col: 2,
        row0: 10,
        old_first: parse("=B11").unwrap(),
        new_first: parse("=B12").unwrap(),
    });
    // 2 trailing plain events (seqs 11, 12) — retained
    capture.record(create_test_event_with_value(300.0));
    capture.record(create_test_event_with_value(400.0));

    // Total emitted = 1 + 4 + 1 + 5 + 2 = 13; cap = 6 => drop 7 (seqs 0..=6), retain seqs 7..=12.
    log.begin_compound("structural-batch".to_string()); // seq 0 => total becomes 14, drop 8 (seqs 0..=7), retain seqs 8..=13
    log.publish_capture(capture);
    log.end_compound(); // seq 14 => total 15, drop 9 (seqs 0..=8), retain seqs 9..=14

    // Before reading `events()`, the remaining straddling lazy run must STILL be unexpanded.
    assert_eq!(log.len(), 6);
    assert_eq!(log.next_seq(), 15);
    assert_eq!(log.unexpanded_run_records(), 1);
    assert_eq!(log.expansion_work(), 0);

    // Group was partially evicted from the front, so `last_group_indices` must be empty.
    // (Calling `events()` now expands only the 3 remaining members of Run 2 + 3 trailing plain events.)
    let events = log.events();
    assert_eq!(events.len(), 6);
    assert_eq!(log.unexpanded_run_records(), 0);
    // Remaining in Run 2: indices 2, 3, 4 (VertexId 102, 103, 104; seqs 9, 10, 11)
    for (idx, expected_vid) in [102u32, 103, 104].into_iter().enumerate() {
        match &events[idx] {
            ChangeEvent::FormulaAdjusted { id, addr, .. } => {
                assert_eq!(*id, VertexId(expected_vid));
                assert_eq!(addr.unwrap().coord.row(), 10 + (idx as u32 + 2));
            }
            other => panic!("expected FormulaAdjusted at {idx}, got {other:?}"),
        }
        assert_eq!(log.meta(idx).unwrap().0, 9 + idx as u64);
        assert_eq!(
            log.event_meta(idx).unwrap().actor_id.as_deref(),
            Some("structural")
        );
    }
    assert!(matches!(
        events[3],
        ChangeEvent::SetValue {
            new: LiteralValue::Number(n),
            ..
        } if n == 300.0
    ));
    assert!(matches!(
        events[4],
        ChangeEvent::SetValue {
            new: LiteralValue::Number(n),
            ..
        } if n == 400.0
    ));
    assert!(matches!(events[5], ChangeEvent::CompoundEnd { depth: 1 }));
    assert_eq!(log.meta(5).unwrap().0, 14);
    assert!(log.last_group_indices().is_empty());
}

#[test]
fn test_change_log_zero_cap_fast_path_preserves_seq_and_spill_tracking() {
    use crate::engine::graph::editor::change_log::{ChangeEventMeta, SpillSnapshot};
    use crate::engine::vertex::VertexId;

    let mut log = ChangeLog::with_max_changelog_events(0);
    log.begin_compound("zero-cap".to_string());
    log.record(create_test_event_with_value(1.0));
    let spill_cells = vec![cell_ref(0, 0, 0), cell_ref(0, 1, 0)];
    log.record_with_meta(
        ChangeEvent::SpillCleared {
            anchor: VertexId(1),
            old: SpillSnapshot {
                target_cells: spill_cells.clone(),
                values: vec![
                    vec![LiteralValue::Number(1.0)],
                    vec![LiteralValue::Number(2.0)],
                ],
            },
        },
        ChangeEventMeta::default(),
    );

    assert_eq!(log.len(), 0);
    assert_eq!(log.next_seq(), 3);
    assert_eq!(log.spill_clear_revision(), 1);
    let mut drained = log.drain_open_compound_spill_cleared();
    drained.sort_by_key(|c| (c.sheet_id, c.coord.row(), c.coord.col()));
    assert_eq!(drained, spill_cells);

    log.end_compound();
    assert_eq!(log.len(), 0);
    assert_eq!(log.next_seq(), 4);
    assert!(log.drain_open_compound_spill_cleared().is_empty());
}
