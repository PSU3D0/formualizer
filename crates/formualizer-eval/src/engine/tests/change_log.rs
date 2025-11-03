//! Tests for the standalone ChangeLog implementation

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
        old: None,
        new: LiteralValue::Number(42.0),
    }
}

fn create_test_event_with_value(value: f64) -> ChangeEvent {
    ChangeEvent::SetValue {
        addr: cell_ref(0, 1, 1),
        old: None,
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
        old: Some(old_formula.clone()),
        new: new_formula.clone(),
    };

    log.record(event.clone());
    assert_eq!(log.len(), 1);

    match &log.events()[0] {
        ChangeEvent::SetFormula { old, new, .. } => {
            assert_eq!(old.as_ref(), Some(&old_formula));
            assert_eq!(new, &new_formula);
        }
        _ => panic!("Wrong event type"),
    }
}

#[test]
fn test_granular_change_events() {
    use crate::engine::vertex::VertexId;
    use formualizer_common::Coord as AbsCoord;
    use formualizer_parse::parser::parse;

    let mut log = ChangeLog::new();

    // Test VertexMoved event
    let move_event = ChangeEvent::VertexMoved {
        id: VertexId(1),
        old_coord: AbsCoord::new(5, 1),
        new_coord: AbsCoord::new(7, 1),
    };
    log.record(move_event);

    // Test FormulaAdjusted event
    let adjust_event = ChangeEvent::FormulaAdjusted {
        id: VertexId(2),
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
