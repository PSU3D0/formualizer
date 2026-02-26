//! Internal action journal types used for atomicity and undo/redo.
//!
//! This module intentionally does not depend on the external `ChangeLog` as a correctness
//! mechanism. It uses graph `ChangeEvent` as a structural delta representation and records
//! explicit Arrow overlay mutations for value truth rollback.

use crate::SheetId;
use crate::engine::DependencyGraph;
use crate::engine::graph::editor::change_log::ChangeEvent;
use crate::engine::graph::editor::vertex_editor::{EditorError, VertexEditor};
use formualizer_common::LiteralValue;

#[derive(Debug, Clone, PartialEq)]
pub enum ArrowOp {
    SetDeltaCell {
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        old: Option<LiteralValue>,
        new: Option<LiteralValue>,
    },
    SetComputedCell {
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        old: Option<LiteralValue>,
        new: Option<LiteralValue>,
    },
    RestoreComputedRect {
        sheet_id: SheetId,
        sr0: u32,
        sc0: u32,
        er0: u32,
        ec0: u32,
        old: Vec<Vec<LiteralValue>>,
        new: Vec<Vec<LiteralValue>>,
    },
    InsertRows {
        sheet_id: SheetId,
        before0: u32,
        count: u32,
    },
    InsertCols {
        sheet_id: SheetId,
        before0: u32,
        count: u32,
    },
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ArrowUndoBatch {
    pub ops: Vec<ArrowOp>,
}

impl ArrowUndoBatch {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    #[inline]
    pub fn record_delta_cell(
        &mut self,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        old: Option<LiteralValue>,
        new: Option<LiteralValue>,
    ) {
        if old == new {
            return;
        }
        self.ops.push(ArrowOp::SetDeltaCell {
            sheet_id,
            row0,
            col0,
            old,
            new,
        });
    }

    #[inline]
    pub fn record_computed_cell(
        &mut self,
        sheet_id: SheetId,
        row0: u32,
        col0: u32,
        old: Option<LiteralValue>,
        new: Option<LiteralValue>,
    ) {
        if old == new {
            return;
        }
        self.ops.push(ArrowOp::SetComputedCell {
            sheet_id,
            row0,
            col0,
            old,
            new,
        });
    }

    #[inline]
    pub fn record_restore_computed_rect(
        &mut self,
        sheet_id: SheetId,
        sr0: u32,
        sc0: u32,
        er0: u32,
        ec0: u32,
        old: Vec<Vec<LiteralValue>>,
        new: Vec<Vec<LiteralValue>>,
    ) {
        if old == new {
            return;
        }
        self.ops.push(ArrowOp::RestoreComputedRect {
            sheet_id,
            sr0,
            sc0,
            er0,
            ec0,
            old,
            new,
        });
    }

    #[inline]
    pub fn record_insert_rows(&mut self, sheet_id: SheetId, before0: u32, count: u32) {
        if count == 0 {
            return;
        }
        self.ops.push(ArrowOp::InsertRows {
            sheet_id,
            before0,
            count,
        });
    }

    #[inline]
    pub fn record_insert_cols(&mut self, sheet_id: SheetId, before0: u32, count: u32) {
        if count == 0 {
            return;
        }
        self.ops.push(ArrowOp::InsertCols {
            sheet_id,
            before0,
            count,
        });
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphUndoBatch {
    pub events: Vec<ChangeEvent>,
}

impl GraphUndoBatch {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn undo(&self, graph: &mut DependencyGraph) -> Result<(), EditorError> {
        let mut editor = VertexEditor::new(graph);
        let mut compound_stack: Vec<usize> = Vec::new();
        for ev in self.events.iter().rev() {
            match ev {
                ChangeEvent::CompoundEnd { depth } => compound_stack.push(*depth),
                ChangeEvent::CompoundStart { depth, .. } => {
                    if compound_stack.last() == Some(depth) {
                        compound_stack.pop();
                    }
                }
                _ => {
                    editor.apply_inverse(ev.clone())?;
                }
            }
        }
        Ok(())
    }

    pub fn redo(&self, graph: &mut DependencyGraph) -> Result<(), EditorError> {
        for ev in &self.events {
            apply_forward_change_event(graph, ev)?;
        }
        Ok(())
    }
}

fn apply_forward_change_event(
    graph: &mut DependencyGraph,
    ev: &ChangeEvent,
) -> Result<(), EditorError> {
    use crate::engine::graph::editor::vertex_editor::VertexMeta;
    match ev {
        ChangeEvent::SetValue { addr, new, .. } => {
            let mut editor = VertexEditor::new(graph);
            editor.set_cell_value(*addr, new.clone());
        }
        ChangeEvent::SetFormula { addr, new, .. } => {
            let mut editor = VertexEditor::new(graph);
            editor.set_cell_formula(*addr, new.clone());
        }
        ChangeEvent::SetRowVisibility { .. } => {
            // Engine-level sidecar metadata; applied by Engine undo/redo orchestration.
        }
        ChangeEvent::AddVertex {
            coord,
            sheet_id,
            kind,
            ..
        } => {
            let mut editor = VertexEditor::new(graph);
            let meta = VertexMeta::new(
                coord.row(),
                coord.col(),
                *sheet_id,
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
                    *sid,
                    crate::reference::Coord::new(c.row(), c.col(), true, true),
                );
                let _ = editor.remove_vertex_at(cell_ref);
            }
        }
        ChangeEvent::VertexMoved { id, new_coord, .. } => {
            let mut editor = VertexEditor::new(graph);
            let _ = editor.move_vertex(*id, *new_coord);
        }
        ChangeEvent::FormulaAdjusted { id, new_ast, .. } => {
            let _ = graph.update_vertex_formula(*id, new_ast.clone());
            graph.mark_vertex_dirty(*id);
        }
        ChangeEvent::DefineName {
            name,
            scope,
            definition,
        } => {
            let mut editor = VertexEditor::new(graph);
            let _ = editor.define_name(name, definition.clone(), *scope);
        }
        ChangeEvent::UpdateName {
            name,
            scope,
            new_definition,
            ..
        } => {
            let mut editor = VertexEditor::new(graph);
            let _ = editor.update_name(name, new_definition.clone(), *scope);
        }
        ChangeEvent::DeleteName { name, scope, .. } => {
            let mut editor = VertexEditor::new(graph);
            let _ = editor.delete_name(name, *scope);
        }
        ChangeEvent::NamedRangeAdjusted {
            name,
            scope,
            new_definition,
            ..
        } => {
            let mut editor = VertexEditor::new(graph);
            let _ = editor.update_name(name, new_definition.clone(), *scope);
        }
        ChangeEvent::SpillCommitted { anchor, new, .. } => {
            let _ = graph.commit_spill_region_atomic_with_fault(
                *anchor,
                new.target_cells.clone(),
                new.values.clone(),
                None,
            );
        }
        ChangeEvent::SpillCleared { anchor, .. } => {
            graph.clear_spill_region(*anchor);
        }
        ChangeEvent::EdgeAdded { from, to } => {
            let mut editor = VertexEditor::new(graph);
            let _ = editor.add_edge(*from, *to);
        }
        ChangeEvent::EdgeRemoved { from, to } => {
            let mut editor = VertexEditor::new(graph);
            let _ = editor.remove_edge(*from, *to);
        }
        ChangeEvent::CompoundStart { .. } | ChangeEvent::CompoundEnd { .. } => {}
    }
    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ActionJournal {
    pub name: String,
    pub graph: GraphUndoBatch,
    pub arrow: ArrowUndoBatch,
    pub affected_cells: usize,
}
