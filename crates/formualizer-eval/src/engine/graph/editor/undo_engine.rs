//! Basic Undo/Redo engine scaffold using ChangeLog groups.
use super::change_log::{ChangeEvent, ChangeLog};
use super::vertex_editor::VertexEditor;
use crate::engine::graph::DependencyGraph;
use crate::engine::graph::editor::vertex_editor::EditorError;

#[derive(Debug, Default)]
pub struct UndoEngine {
    /// Stack of applied groups (their last event index snapshot) for redo separation
    undone: Vec<Vec<ChangeEvent>>, // redo stack stores full event batches
}

impl UndoEngine {
    pub fn new() -> Self {
        Self { undone: Vec::new() }
    }

    /// Undo last group in the provided change log, applying inverses through a VertexEditor.
    pub fn undo(
        &mut self,
        graph: &mut DependencyGraph,
        log: &mut ChangeLog,
    ) -> Result<(), EditorError> {
        let idxs = log.last_group_indices();
        if idxs.is_empty() {
            return Ok(());
        }
        let batch: Vec<ChangeEvent> = idxs.iter().map(|i| log.events()[*i].clone()).collect();
        let max_idx = *idxs.iter().max().unwrap();
        if max_idx + 1 == log.events().len() {
            let truncate_to = idxs.iter().min().copied().unwrap();
            log.truncate(truncate_to);
        } else {
            return Err(EditorError::TransactionFailed {
                reason: "Non-tail undo not supported".into(),
            });
        }
        let mut editor = VertexEditor::new(graph);
        for ev in batch.iter().rev() {
            editor.apply_inverse(ev.clone())?;
        }
        self.undone.push(batch);
        Ok(())
    }

    pub fn redo(
        &mut self,
        graph: &mut DependencyGraph,
        log: &mut ChangeLog,
    ) -> Result<(), EditorError> {
        if let Some(batch) = self.undone.pop() {
            let mut editor = VertexEditor::new(graph);
            log.begin_compound("redo".to_string());
            for ev in batch {
                // Re-log original event for audit consistency
                log.record(ev.clone());
                match ev {
                    ChangeEvent::SetValue { addr, new, .. } => {
                        editor.set_cell_value(addr, new);
                    }
                    ChangeEvent::SetFormula { addr, new, .. } => {
                        editor.set_cell_formula(addr, new);
                    }
                    ChangeEvent::AddVertex {
                        coord,
                        sheet_id,
                        kind,
                        ..
                    } => {
                        let meta = crate::engine::graph::editor::vertex_editor::VertexMeta::new(
                            coord.row(),
                            coord.col(),
                            sheet_id,
                            kind.unwrap_or(crate::engine::vertex::VertexKind::Cell),
                        );
                        editor.add_vertex(meta);
                    }
                    ChangeEvent::RemoveVertex {
                        coord, sheet_id, ..
                    } => {
                        if let (Some(c), Some(sid)) = (coord, sheet_id) {
                            let cell_ref = crate::reference::CellRef::new(
                                sid,
                                crate::reference::Coord::new(c.row(), c.col(), true, true),
                            );
                            let _ = editor.remove_vertex_at(cell_ref);
                        }
                    }
                    ChangeEvent::VertexMoved {
                        id,
                        old_coord: _,
                        new_coord,
                    } => {
                        let _ = editor.move_vertex(id, new_coord);
                    }
                    ChangeEvent::DefineName {
                        name,
                        scope,
                        definition,
                    } => {
                        let _ = editor.define_name(&name, definition, scope);
                    }
                    ChangeEvent::UpdateName {
                        name,
                        scope,
                        new_definition,
                        ..
                    } => {
                        let _ = editor.update_name(&name, new_definition, scope);
                    }
                    ChangeEvent::DeleteName { name, scope, .. } => {
                        let _ = editor.delete_name(&name, scope);
                    }
                    ChangeEvent::NamedRangeAdjusted {
                        name,
                        scope,
                        new_definition,
                        ..
                    } => {
                        let _ = editor.update_name(&name, new_definition, scope);
                    }
                    _ => {}
                }
            }
            log.end_compound();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::editor::change_log::ChangeLog;
    use crate::reference::{CellRef, Coord};
    use formualizer_common::LiteralValue;

    #[test]
    fn test_undo_redo_single_value() {
        let mut graph = DependencyGraph::new();
        let mut log = ChangeLog::new();
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            let cell = CellRef {
                sheet_id: 0,
                coord: Coord::new(1, 1, true, true),
            };
            editor.set_cell_value(cell, LiteralValue::Number(10.0));
        }
        assert_eq!(log.len(), 1);
        let mut undo = UndoEngine::new();
        undo.undo(&mut graph, &mut log).unwrap();
        assert_eq!(log.len(), 0); // event removed (simplified policy)
        // Redo
        undo.redo(&mut graph, &mut log).unwrap();
        assert!(!log.is_empty());
    }

    #[test]
    fn test_undo_redo_row_shift() {
        let mut graph = DependencyGraph::new();
        let mut log = ChangeLog::new();
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            // Seed some cells
            for r in [5u32, 6u32, 10u32] {
                let cell = CellRef {
                    sheet_id: 0,
                    coord: Coord::new(r, 1, true, true),
                };
                editor.set_cell_value(cell, LiteralValue::Number(r as f64));
            }
        }
        log.clear(); // focus on shift only
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.insert_rows(0, 6, 2).unwrap(); // shift rows >=6 down by 2
        }
        assert!(
            log.events()
                .iter()
                .any(|e| matches!(e, ChangeEvent::VertexMoved { .. }))
        );
        let moved_count_before = log
            .events()
            .iter()
            .filter(|e| matches!(e, ChangeEvent::VertexMoved { .. }))
            .count();
        let mut undo = UndoEngine::new();
        undo.undo(&mut graph, &mut log).unwrap();
        assert_eq!(log.events().len(), 0); // group removed
        undo.redo(&mut graph, &mut log).unwrap();
        let moved_count_after = log
            .events()
            .iter()
            .filter(|e| matches!(e, ChangeEvent::VertexMoved { .. }))
            .count();
        assert_eq!(moved_count_before, moved_count_after);
    }

    #[test]
    fn test_undo_redo_column_shift() {
        let mut graph = DependencyGraph::new();
        let mut log = ChangeLog::new();
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            for c in [3u32, 4u32, 8u32] {
                let cell = CellRef {
                    sheet_id: 0,
                    coord: Coord::new(1, c, true, true),
                };
                editor.set_cell_value(cell, LiteralValue::Number(c as f64));
            }
        }
        log.clear();
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.insert_columns(0, 5, 2).unwrap();
        }
        assert!(
            log.events()
                .iter()
                .any(|e| matches!(e, ChangeEvent::VertexMoved { .. }))
        );
        let mut undo = UndoEngine::new();
        undo.undo(&mut graph, &mut log).unwrap();
        assert_eq!(log.events().len(), 0);
    }

    #[test]
    fn test_remove_vertex_dependency_roundtrip() {
        use formualizer_parse::parser::parse;
        let mut graph = DependencyGraph::new();
        let mut log = ChangeLog::new();
        let (a1_cell, a2_cell) = (
            CellRef {
                sheet_id: 0,
                coord: Coord::new(0, 0, true, true), // A1 internal
            },
            CellRef {
                sheet_id: 0,
                coord: Coord::new(1, 0, true, true), // A2 internal
            },
        );
        let a2_id;
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.set_cell_value(a1_cell, LiteralValue::Number(10.0));
            a2_id = editor.set_cell_formula(a2_cell, parse("=A1").unwrap());
        }
        // Ensure dependency exists
        let deps_before = graph.get_dependencies(a2_id);
        assert!(!deps_before.is_empty());
        // Clear log then remove A1
        log.clear();
        {
            // Obtain id prior to editor mutable borrow
            let a1_vid = graph.get_vertex_id_for_address(&a1_cell).copied().unwrap();
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.remove_vertex(a1_vid).unwrap();
        }
        assert!(
            log.events()
                .iter()
                .any(|e| matches!(e, ChangeEvent::RemoveVertex { .. }))
        );
        // After removal dependency list should be empty
        let deps_after_remove = graph.get_dependencies(a2_id);
        assert!(deps_after_remove.is_empty());
        let mut undo = UndoEngine::new();
        undo.undo(&mut graph, &mut log).unwrap();
        // Dependency restored (may be different vertex id)
        let deps_after_undo = graph.get_dependencies(a2_id);
        assert!(!deps_after_undo.is_empty());
        // Redo removal
        undo.redo(&mut graph, &mut log).unwrap();
        let deps_after_redo = graph.get_dependencies(a2_id);
        assert!(deps_after_redo.is_empty());
    }
}
