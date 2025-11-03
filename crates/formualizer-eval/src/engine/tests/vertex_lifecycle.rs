use crate::engine::graph::DependencyGraph;
use crate::engine::graph::editor::{
    EditorError, VertexDataPatch, VertexEditor, VertexMeta, VertexMetaPatch,
};
use crate::engine::vertex::{VertexId, VertexKind};
use crate::reference::{CellRef, Coord};
use formualizer_common::Coord as AbsCoord;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parse;

fn create_test_graph() -> DependencyGraph {
    DependencyGraph::new()
}

fn cell_ref(sheet_id: u16, row: u32, col: u32) -> CellRef {
    CellRef {
        sheet_id,
        coord: Coord::new(row, col, true, true),
    }
}

fn lit_num(value: f64) -> LiteralValue {
    LiteralValue::Number(value)
}

#[test]
fn test_vertex_removal_cleanup() {
    let mut graph = create_test_graph();

    // Use the graph API directly to ensure proper dependency setup
    // Create A1 = 10 (Excel uses 1-based indexing: A1 = row 1, col 1)
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();

    // Create B1 = A1 * 2 (B1 = row 1, col 2)
    let b1_formula = parse("=A1*2").unwrap();
    let b1_result = graph.set_cell_formula("Sheet1", 1, 2, b1_formula).unwrap();
    let b1 = b1_result.affected_vertices[0];

    // Create C1 = B1 + A1 (C1 = row 1, col 3)
    let c1_formula = parse("=B1+A1").unwrap();
    let c1_result = graph.set_cell_formula("Sheet1", 1, 3, c1_formula).unwrap();
    let c1 = c1_result.affected_vertices[0];

    // Now use editor to remove B1
    let mut editor = VertexEditor::new(&mut graph);
    assert!(editor.remove_vertex(b1).is_ok());

    // Drop editor to release borrow
    drop(editor);

    // Verify C1 now has #REF! error
    let c1_value = graph.get_value(c1);
    assert!(matches!(c1_value, Some(LiteralValue::Error(ref e)) if e.kind == ExcelErrorKind::Ref));
}

#[test]
fn test_vertex_patch_meta() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    let meta = VertexMeta::new(1, 1, 0, VertexKind::Cell);
    let id = editor.add_vertex(meta);

    let patch = VertexMetaPatch {
        kind: None,
        coord: None,
        dirty: Some(true),
        volatile: Some(true),
    };

    let summary = editor.patch_vertex_meta(id, patch).unwrap();
    assert!(summary.flags_changed);

    // Drop editor to release borrow
    drop(editor);

    assert!(graph.is_dirty(id));

    // Check volatile flag is set
    let flags = graph.get_flags(id);
    assert_ne!(flags & 0x02, 0, "Volatile flag should be set");
}

#[test]
fn test_vertex_move_updates_mappings() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Create a cell at A1
    let id = editor.set_cell_value(cell_ref(0, 0, 0), lit_num(42.0));

    // Move to new location (5, 10)
    assert!(editor.move_vertex(id, AbsCoord::new(5, 10)).is_ok());

    // Drop editor to release borrow
    drop(editor);

    // Verify coordinate was updated
    assert_eq!(graph.get_coord(id), AbsCoord::new(5, 10));

    // Value should be preserved
    assert_eq!(graph.get_value(id), Some(lit_num(42.0)));
}

#[test]
fn test_patch_vertex_data() {
    let mut graph = create_test_graph();

    // Use graph API to ensure proper dependencies (Excel uses 1-based indexing)
    let a1_result = graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    let a1 = a1_result.affected_vertices[0];

    // Create B1 that depends on A1 (B1 = row 1, col 2)
    let formula = parse("=A1*2").unwrap();
    let b1_result = graph.set_cell_formula("Sheet1", 1, 2, formula).unwrap();
    let b1 = b1_result.affected_vertices[0];

    // Now use editor to patch A1's value
    let mut editor = VertexEditor::new(&mut graph);

    let patch = VertexDataPatch {
        value: Some(lit_num(20.0)),
        formula: None,
    };

    let summary = editor.patch_vertex_data(a1, patch).unwrap();
    assert!(summary.value_changed);
    assert!(!summary.formula_changed);
    // B1 should be marked dirty
    assert!(
        summary.dependents_marked_dirty.contains(&b1),
        "B1 should be marked dirty"
    );

    // Drop editor to release borrow
    drop(editor);

    // Verify new value
    assert_eq!(graph.get_value(a1), Some(lit_num(20.0)));

    // Verify dependent is dirty
    assert!(graph.is_dirty(b1));
}

#[test]
fn test_move_vertex_with_dependencies() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Create A1 with value
    let a1 = editor.set_cell_value(cell_ref(0, 0, 0), lit_num(100.0));

    // Create B1 that depends on A1
    let formula = parse("=A1+10").unwrap();
    let b1 = editor.set_cell_formula(cell_ref(0, 0, 1), formula);

    // Move A1 to new location
    assert!(editor.move_vertex(a1, AbsCoord::new(5, 5)).is_ok());

    // Drop editor to release borrow
    drop(editor);

    // B1 should be marked dirty
    assert!(graph.is_dirty(b1));
}

#[test]
fn test_patch_vertex_coord() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Create vertex
    let id = editor.set_cell_value(cell_ref(0, 1, 1), lit_num(50.0));

    // Patch coordinate
    let patch = VertexMetaPatch {
        coord: Some(AbsCoord::new(10, 20)),
        kind: None,
        dirty: None,
        volatile: None,
    };

    let summary = editor.patch_vertex_meta(id, patch).unwrap();
    assert!(summary.coord_changed);
    assert!(!summary.kind_changed);
    assert!(!summary.flags_changed);

    // Drop editor to release borrow
    drop(editor);

    // Verify coordinate changed
    assert_eq!(graph.get_coord(id), AbsCoord::new(10, 20));
}

#[test]
fn test_patch_vertex_kind() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Create vertex as Cell
    let meta = VertexMeta::new(0, 0, 0, VertexKind::Cell);
    let id = editor.add_vertex(meta);

    // Change kind to FormulaScalar
    let patch = VertexMetaPatch {
        kind: Some(VertexKind::FormulaScalar),
        coord: None,
        dirty: None,
        volatile: None,
    };

    let summary = editor.patch_vertex_meta(id, patch).unwrap();
    assert!(summary.kind_changed);

    // Drop editor to release borrow
    drop(editor);

    // Verify kind changed
    assert_eq!(graph.get_kind(id), VertexKind::FormulaScalar);
}

#[test]
fn test_remove_nonexistent_vertex() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    // Try to remove vertex that doesn't exist
    let fake_id = VertexId::new(99999);
    let result = editor.remove_vertex(fake_id);

    assert!(result.is_err());
    match result {
        Err(EditorError::Excel(e)) => {
            assert_eq!(e.kind, ExcelErrorKind::Ref);
        }
        _ => panic!("Expected Excel Ref error"),
    }
}

#[test]
fn test_patch_nonexistent_vertex() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    let fake_id = VertexId::new(99999);

    // Try to patch metadata
    let meta_patch = VertexMetaPatch {
        dirty: Some(true),
        kind: None,
        coord: None,
        volatile: None,
    };

    let result = editor.patch_vertex_meta(fake_id, meta_patch);
    assert!(result.is_err());

    // Try to patch data
    let data_patch = VertexDataPatch {
        value: Some(lit_num(123.0)),
        formula: None,
    };

    let result = editor.patch_vertex_data(fake_id, data_patch);
    assert!(result.is_err());
}

#[test]
fn test_move_nonexistent_vertex() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    let fake_id = VertexId::new(99999);
    let result = editor.move_vertex(fake_id, AbsCoord::new(1, 1));

    assert!(result.is_err());
    match result {
        Err(EditorError::Excel(e)) => {
            assert_eq!(e.kind, ExcelErrorKind::Ref);
        }
        _ => panic!("Expected Excel Ref error"),
    }
}

#[test]
fn test_complex_removal_scenario() {
    let mut graph = create_test_graph();

    // Use graph API for proper dependency setup (Excel uses 1-based indexing)
    // Create A1 (row 1, col 1)
    let a1_result = graph.set_cell_value("Sheet1", 1, 1, lit_num(5.0)).unwrap();
    let a1 = a1_result.affected_vertices[0];

    // Create B1 = A1*2 (row 1, col 2)
    let b1_result = graph
        .set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())
        .unwrap();
    let b1 = b1_result.affected_vertices[0];

    // Create C1 = B1+1 (row 1, col 3)
    let c1_result = graph
        .set_cell_formula("Sheet1", 1, 3, parse("=B1+1").unwrap())
        .unwrap();
    let c1 = c1_result.affected_vertices[0];

    // Create D2 = B1-1 (row 2, col 4)
    let _d2_result = graph
        .set_cell_formula("Sheet1", 2, 4, parse("=B1-1").unwrap())
        .unwrap();

    // Create E1 = C1+D2 (row 1, col 5)
    let _e1_result = graph
        .set_cell_formula("Sheet1", 1, 5, parse("=C1+D2").unwrap())
        .unwrap();

    // Now use editor to remove B1
    let mut editor = VertexEditor::new(&mut graph);
    assert!(editor.remove_vertex(b1).is_ok());

    // Drop editor to release borrow
    drop(editor);

    // C1 should have #REF!
    assert!(matches!(
        graph.get_value(c1),
        Some(LiteralValue::Error(ref e)) if e.kind == ExcelErrorKind::Ref
    ));

    // A1 should still have its value
    assert_eq!(graph.get_value(a1), Some(lit_num(5.0)));
}

#[test]
fn test_batch_operations_with_lifecycle() {
    let mut graph = create_test_graph();
    let mut editor = VertexEditor::new(&mut graph);

    editor.begin_batch();

    // Create multiple vertices
    let v1 = editor.set_cell_value(cell_ref(0, 0, 0), lit_num(1.0));
    let v2 = editor.set_cell_value(cell_ref(0, 1, 0), lit_num(2.0));
    let v3 = editor.set_cell_value(cell_ref(0, 2, 0), lit_num(3.0));

    // Move one
    editor.move_vertex(v1, AbsCoord::new(10, 10)).unwrap();

    // Remove one
    editor.remove_vertex(v2).unwrap();

    // Patch one
    let patch = VertexDataPatch {
        value: Some(lit_num(30.0)),
        formula: None,
    };
    editor.patch_vertex_data(v3, patch).unwrap();

    editor.commit_batch();

    // Drop editor to release borrow
    drop(editor);

    // Verify results
    assert_eq!(graph.get_coord(v1), AbsCoord::new(10, 10));
    assert!(graph.is_deleted(v2));
    assert_eq!(graph.get_value(v3), Some(lit_num(30.0)));
}

#[test]
fn test_error_display() {
    let cell = cell_ref(0, 5, 10);
    let err = EditorError::TargetOccupied { cell };
    assert!(err.to_string().contains("row 5"));
    assert!(err.to_string().contains("col 10"));

    let err = EditorError::OutOfBounds { row: 100, col: 200 };
    assert!(err.to_string().contains("100"));
    assert!(err.to_string().contains("200"));

    let err = EditorError::InvalidName {
        name: "BadName".to_string(),
        reason: "Contains invalid characters".to_string(),
    };
    assert!(err.to_string().contains("BadName"));
    assert!(err.to_string().contains("invalid characters"));

    let err = EditorError::TransactionFailed {
        reason: "Lock timeout".to_string(),
    };
    assert!(err.to_string().contains("Lock timeout"));
}
