use crate::engine::graph::editor::change_log::{ChangeEvent, ChangeLog};
use crate::engine::graph::editor::VertexEditor;
use crate::engine::graph::DependencyGraph;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::VertexKind;
use crate::reference::{CellRef, Coord};
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
struct CellSnapshot {
    kind: VertexKind,
    value: Option<LiteralValue>,
    formula: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct GraphSnapshot {
    cells: BTreeMap<String, CellSnapshot>,
    names: BTreeMap<String, NamedDefinition>,
}

fn snapshot_graph(graph: &DependencyGraph) -> GraphSnapshot {
    let mut cells: BTreeMap<String, CellSnapshot> = BTreeMap::new();
    for (addr, vid) in graph.cell_to_vertex() {
        let a1 = graph.to_a1(*addr);
        let kind = graph.get_vertex_kind(*vid);
        let value = graph.get_value(*vid);
        let formula = graph
            .get_formula(*vid)
            .map(|ast| formualizer_parse::pretty::canonical_formula(&ast));
        cells.insert(
            a1,
            CellSnapshot {
                kind,
                value,
                formula,
            },
        );
    }

    let mut names: BTreeMap<String, NamedDefinition> = BTreeMap::new();
    for (name, nr) in graph.named_ranges_iter() {
        names.insert(
            format!("{:?}:{name}", NameScope::Workbook),
            nr.definition.clone(),
        );
    }
    for ((sheet_id, name), nr) in graph.sheet_named_ranges_iter() {
        names.insert(
            format!("{:?}:{name}", NameScope::Sheet(*sheet_id)),
            nr.definition.clone(),
        );
    }

    GraphSnapshot { cells, names }
}

fn replay_events(graph: &mut DependencyGraph, events: &[ChangeEvent]) {
    for ev in events.iter().cloned() {
        match ev {
            ChangeEvent::CompoundStart { .. } | ChangeEvent::CompoundEnd { .. } => {}

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
                    kind.unwrap_or(VertexKind::Cell),
                );
                let _ = editor.add_vertex(meta);
            }
            ChangeEvent::RemoveVertex {
                coord, sheet_id, ..
            } => {
                if let (Some(c), Some(sid)) = (coord, sheet_id) {
                    let mut editor = VertexEditor::new(graph);
                    let cell_ref = CellRef::new(sid, Coord::new(c.row(), c.col(), true, true));
                    let _ = editor.remove_vertex_at(cell_ref);
                }
            }

            ChangeEvent::VertexMoved {
                sheet_id,
                old_coord,
                new_coord,
                ..
            } => {
                let old_addr = CellRef::new(
                    sheet_id,
                    Coord::new(old_coord.row(), old_coord.col(), true, true),
                );
                if let Some(id) = graph.get_vertex_for_cell(&old_addr) {
                    let mut editor = VertexEditor::new(graph);
                    let _ = editor.move_vertex(id, new_coord);
                }
            }
            ChangeEvent::FormulaAdjusted { addr, new_ast, .. } => {
                if let Some(addr) = addr {
                    if let Some(id) = graph.get_vertex_for_cell(&addr) {
                        let _ = graph.update_vertex_formula(id, new_ast);
                        graph.mark_vertex_dirty(id);
                    }
                }
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

            // Not currently emitted by public editor mutations.
            ChangeEvent::EdgeAdded { .. } | ChangeEvent::EdgeRemoved { .. } => {}
        }
    }
}

#[test]
fn changelog_replay_roundtrip_matches_end_state() {
    let mut g1 = DependencyGraph::new();
    let sheet_id = g1.sheet_id_mut("Sheet1");

    let mut log = ChangeLog::new();
    {
        let mut editor = VertexEditor::with_logger(&mut g1, &mut log);

        let a1 = CellRef::new(sheet_id, Coord::new(0, 0, true, true));
        let b2 = CellRef::new(sheet_id, Coord::new(1, 1, true, true));
        let c4 = CellRef::new(sheet_id, Coord::new(3, 2, true, true));

        let anchor_vid = editor.set_cell_value(a1, LiteralValue::Number(10.0));
        editor.set_cell_formula(b2, parse("=A1*2").unwrap());
        editor.set_cell_value(c4, LiteralValue::Number(99.0));

        // Ensure named-range adjustments get exercised.
        editor
            .define_name("X", NamedDefinition::Cell(a1), NameScope::Workbook)
            .unwrap();
        editor
            .define_name("Y", NamedDefinition::Cell(b2), NameScope::Sheet(sheet_id))
            .unwrap();

        // Structural edits.
        editor.insert_rows(sheet_id, 1, 2).unwrap();
        editor.delete_columns(sheet_id, 1, 1).unwrap();

        // Exercise spill events in the replay harness. We commit a small 2x2 spill
        // anchored at A1 in the current sheet.
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
        editor
            .commit_spill_region(anchor_vid, target_cells, values)
            .unwrap();
    }

    let events: Vec<ChangeEvent> = log.events().to_vec();
    let snap1 = snapshot_graph(&g1);

    let mut g2 = DependencyGraph::new();
    let _ = g2.sheet_id_mut("Sheet1");
    replay_events(&mut g2, &events);
    let snap2 = snapshot_graph(&g2);

    assert_eq!(snap1, snap2);
}
