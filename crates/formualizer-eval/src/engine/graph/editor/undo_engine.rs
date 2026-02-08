//! Basic Undo/Redo engine scaffold using ChangeLog groups.
use super::change_log::{ChangeEvent, ChangeEventMeta, ChangeLog};
use super::vertex_editor::VertexEditor;
use crate::engine::graph::editor::vertex_editor::EditorError;
use crate::engine::graph::DependencyGraph;

#[derive(Debug, Clone)]
pub struct UndoBatchItem {
    pub event: ChangeEvent,
    pub meta: ChangeEventMeta,
}

#[derive(Debug, Default)]
pub struct UndoEngine {
    /// Stack of applied groups (their last event index snapshot) for redo separation
    undone: Vec<Vec<UndoBatchItem>>, // redo stack stores full event batches
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
    ) -> Result<Vec<UndoBatchItem>, EditorError> {
        let idxs = log.last_group_indices();
        if idxs.is_empty() {
            return Ok(Vec::new());
        }
        let batch: Vec<UndoBatchItem> = idxs
            .iter()
            .map(|i| UndoBatchItem {
                event: log.events()[*i].clone(),
                meta: log.event_meta(*i).cloned().unwrap_or_default(),
            })
            .collect();
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
        for item in batch.iter().rev() {
            editor.apply_inverse(item.event.clone())?;
        }

        // Keep a copy for redo, but also return the batch so callers can mirror side effects.
        self.undone.push(batch.clone());
        Ok(batch)
    }

    pub fn redo(
        &mut self,
        graph: &mut DependencyGraph,
        log: &mut ChangeLog,
    ) -> Result<Vec<UndoBatchItem>, EditorError> {
        if let Some(batch) = self.undone.pop() {
            log.begin_compound("redo".to_string());
            // Return value for callers (e.g. Arrow mirroring) must remain available even though
            // we apply events by value below.
            let ret = batch.clone();

            for item in batch {
                // Re-log original event for audit consistency
                log.record_with_meta(item.event.clone(), item.meta.clone());
                match item.event {
                    ChangeEvent::SetValue { addr, new, .. } => {
                        let mut editor = VertexEditor::new(graph);
                        editor.set_cell_value(addr, new);
                    }
                    ChangeEvent::SetFormula { addr, new, .. } => {
                        let mut editor = VertexEditor::new(graph);
                        editor.set_cell_formula(addr, new);
                    }
                    ChangeEvent::AddVertex {
                        coord,
                        sheet_id,
                        kind,
                        ..
                    } => {
                        let mut editor = VertexEditor::new(graph);
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
                            let mut editor = VertexEditor::new(graph);
                            let cell_ref = crate::reference::CellRef::new(
                                sid,
                                crate::reference::Coord::new(c.row(), c.col(), true, true),
                            );
                            let _ = editor.remove_vertex_at(cell_ref);
                        }
                    }
                    ChangeEvent::VertexMoved { id, new_coord, .. } => {
                        let mut editor = VertexEditor::new(graph);
                        let _ = editor.move_vertex(id, new_coord);
                    }
                    ChangeEvent::FormulaAdjusted { id, new_ast, .. } => {
                        // Keep it simple: apply directly by vertex id.
                        // (This is used for structural ops formula rewrites.)
                        let _ = graph.update_vertex_formula(id, new_ast);
                        graph.mark_vertex_dirty(id);
                    }
                    ChangeEvent::DefineName {
                        name,
                        scope,
                        definition,
                    } => {
                        let mut editor = VertexEditor::new(graph);
                        let _ = editor.define_name(&name, definition, scope);
                    }
                    ChangeEvent::UpdateName {
                        name,
                        scope,
                        new_definition,
                        ..
                    } => {
                        let mut editor = VertexEditor::new(graph);
                        let _ = editor.update_name(&name, new_definition, scope);
                    }
                    ChangeEvent::DeleteName { name, scope, .. } => {
                        let mut editor = VertexEditor::new(graph);
                        let _ = editor.delete_name(&name, scope);
                    }
                    ChangeEvent::NamedRangeAdjusted {
                        name,
                        scope,
                        new_definition,
                        ..
                    } => {
                        let mut editor = VertexEditor::new(graph);
                        let _ = editor.update_name(&name, new_definition, scope);
                    }
                    ChangeEvent::SpillCommitted { anchor, new, .. } => {
                        let _ = graph.commit_spill_region_atomic_with_fault(
                            anchor,
                            new.target_cells,
                            new.values,
                            None,
                        );
                    }
                    ChangeEvent::SpillCleared { anchor, .. } => {
                        graph.clear_spill_region(anchor);
                    }
                    _ => {}
                }
            }
            log.end_compound();
            Ok(ret)
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::editor::change_log::ChangeLog;
    use crate::engine::EvalConfig;
    use crate::reference::{CellRef, Coord};
    use formualizer_common::LiteralValue;

    fn create_test_graph() -> DependencyGraph {
        let mut cfg = EvalConfig::default();
        cfg.arrow_canonical_values = false;
        DependencyGraph::new_with_config(cfg)
    }

    #[test]
    fn test_undo_redo_single_value() {
        let mut graph = create_test_graph();
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
        let mut graph = create_test_graph();
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
        assert!(log
            .events()
            .iter()
            .any(|e| matches!(e, ChangeEvent::VertexMoved { .. })));
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
    fn test_undo_redo_spill_clear_on_scalar_edit_restores_registry_and_cells() {
        let mut graph = create_test_graph();
        let sheet_id = graph.sheet_id_mut("Sheet1");

        let anchor_cell = CellRef::new(sheet_id, Coord::new(0, 0, true, true));
        let anchor_vid = {
            let mut editor = VertexEditor::new(&mut graph);
            editor.set_cell_value(anchor_cell, LiteralValue::Number(0.0))
        };

        let target_cells = vec![
            CellRef::new(sheet_id, Coord::new(0, 0, true, true)),
            CellRef::new(sheet_id, Coord::new(0, 1, true, true)),
            CellRef::new(sheet_id, Coord::new(1, 0, true, true)),
            CellRef::new(sheet_id, Coord::new(1, 1, true, true)),
        ];
        let values = vec![
            vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.0), LiteralValue::Number(4.0)],
        ];
        graph
            .commit_spill_region_atomic_with_fault(anchor_vid, target_cells.clone(), values, None)
            .unwrap();

        assert!(graph.spill_registry_has_anchor(anchor_vid));

        let mut log = ChangeLog::new();
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            // Scalar edit of the anchor should clear spill children + ownership.
            editor.set_cell_value(anchor_cell, LiteralValue::Number(9.0));
        }

        assert!(!graph.spill_registry_has_anchor(anchor_vid));
        assert_eq!(graph.spill_registry_counts(), (0, 0));
        let b1 = graph
            .get_cell_value("Sheet1", 1, 2)
            .unwrap_or(LiteralValue::Empty);
        assert_eq!(b1, LiteralValue::Empty);

        let mut undo = UndoEngine::new();
        undo.undo(&mut graph, &mut log).unwrap();

        assert!(graph.spill_registry_has_anchor(anchor_vid));
        for cell in &target_cells {
            assert_eq!(
                graph.spill_registry_anchor_for_cell(*cell),
                Some(anchor_vid)
            );
        }
        let b1_restored = graph
            .get_cell_value("Sheet1", 1, 2)
            .unwrap_or(LiteralValue::Empty);
        assert_eq!(b1_restored, LiteralValue::Number(2.0));

        // Redo should clear the spill again.
        undo.redo(&mut graph, &mut log).unwrap();
        assert!(!graph.spill_registry_has_anchor(anchor_vid));
        assert_eq!(graph.spill_registry_counts(), (0, 0));
    }

    #[test]
    fn test_undo_depth_truncates_gracefully_under_changelog_cap() {
        let mut graph = create_test_graph();
        let sheet_id = graph.sheet_id_mut("Sheet1");
        let mut log = ChangeLog::with_max_changelog_events(3);

        // Record 5 independent edits; cap keeps only the last 3.
        for i in 0..5u32 {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            let cell = CellRef::new(sheet_id, Coord::new(i, 0, true, true));
            editor.set_cell_value(cell, LiteralValue::Number(i as f64));
        }
        assert_eq!(log.len(), 3);

        let mut undo = UndoEngine::new();
        undo.undo(&mut graph, &mut log).unwrap();
        undo.undo(&mut graph, &mut log).unwrap();
        undo.undo(&mut graph, &mut log).unwrap();
        // Beyond retained history: no-op, should not error.
        undo.undo(&mut graph, &mut log).unwrap();
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn test_undo_redo_column_shift() {
        let mut graph = create_test_graph();
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
        assert!(log
            .events()
            .iter()
            .any(|e| matches!(e, ChangeEvent::VertexMoved { .. })));
        let mut undo = UndoEngine::new();
        undo.undo(&mut graph, &mut log).unwrap();
        assert_eq!(log.events().len(), 0);
    }

    #[test]
    fn test_remove_vertex_dependency_roundtrip() {
        use formualizer_parse::parser::parse;
        let mut graph = create_test_graph();
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
        assert!(log
            .events()
            .iter()
            .any(|e| matches!(e, ChangeEvent::RemoveVertex { .. })));
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
