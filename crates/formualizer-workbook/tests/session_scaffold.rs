use formualizer_common::LiteralValue;
use formualizer_eval::engine::graph::editor::vertex_editor::VertexEditor;
use formualizer_workbook::{EditorSession, IoConfig};

#[test]
fn session_scaffold_basic() {
    let mut sess = EditorSession::new(IoConfig {
        enable_changelog: true,
    });
    // Simple set
    let res = sess.with_action("Set A1", |ed: &mut VertexEditor| {
        use formualizer_eval::{CellRef, Coord};
        let a1 = CellRef::new(0, Coord::new(1, 1, true, true));
        ed.set_cell_value(a1, LiteralValue::Number(10.0));
        Ok(())
    });
    assert!(res.is_ok());
    assert!(sess.changelog_enabled());
    // Undo/redo should not panic
    sess.undo().unwrap();
    sess.redo().unwrap();
}
