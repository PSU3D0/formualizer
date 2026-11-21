//! Basic Undo/Redo engine scaffold using ChangeLog groups.
use super::change_log::{ChangeEvent, ChangeEventMeta, ChangeLog};
use super::vertex_editor::VertexEditor;
use crate::engine::graph::DependencyGraph;
use crate::engine::graph::editor::vertex_editor::EditorError;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct UndoBatchItem {
    pub event: ChangeEvent,
    pub meta: ChangeEventMeta,
}

#[derive(Debug, Default)]
pub struct UndoEngine {
    /// Stack of applied groups for redo separation.
    undone: VecDeque<Vec<UndoBatchItem>>,

    /// Journal-based undo/redo stack for atomic actions.
    actions_done: VecDeque<crate::engine::ActionJournal>,
    actions_undone: VecDeque<crate::engine::ActionJournal>,

    /// Optional cap on retained undo/redo entries per stack (FIFO eviction).
    max_actions: Option<usize>,

    /// Last observed `ChangeLog::mutation_epoch` at the end of `undo`/`redo`
    /// so new forward edits invalidate stale logged redo history.
    expected_log_epoch: Option<u64>,
}

fn enforce_stack_cap<T>(stack: &mut VecDeque<T>, max_actions: Option<usize>) {
    let Some(max) = max_actions else {
        return;
    };
    if max == 0 {
        stack.clear();
        return;
    }
    while stack.len() > max {
        stack.pop_front();
    }
}

fn is_single_top_level_compound(batch: &[UndoBatchItem]) -> bool {
    if batch.len() < 2 {
        return false;
    }
    let mut depth = 0usize;
    for (i, item) in batch.iter().enumerate() {
        match item.event {
            ChangeEvent::CompoundStart { .. } => depth += 1,
            ChangeEvent::CompoundEnd { .. } => {
                if depth == 0 {
                    return false;
                }
                depth -= 1;
                if depth == 0 && i + 1 != batch.len() {
                    return false;
                }
            }
            _ if depth == 0 => return false,
            _ => {}
        }
    }
    depth == 0
}

impl UndoEngine {
    pub fn new() -> Self {
        Self {
            undone: VecDeque::new(),
            actions_done: VecDeque::new(),
            actions_undone: VecDeque::new(),
            max_actions: None,
            expected_log_epoch: None,
        }
    }

    pub fn with_max_actions(max: usize) -> Self {
        let mut out = Self::new();
        out.max_actions = Some(max);
        out
    }

    pub fn max_actions(&self) -> Option<usize> {
        self.max_actions
    }

    pub fn set_max_actions(&mut self, max: Option<usize>) {
        self.max_actions = max;
        enforce_stack_cap(&mut self.undone, max);
        enforce_stack_cap(&mut self.actions_done, max);
        enforce_stack_cap(&mut self.actions_undone, max);
    }

    pub fn actions_done_len(&self) -> usize {
        self.actions_done.len()
    }

    pub fn actions_undone_len(&self) -> usize {
        self.actions_undone.len()
    }

    pub fn undone_len(&self) -> usize {
        self.undone.len()
    }

    pub fn clear_logged_redo(&mut self) {
        self.undone.clear();
        self.expected_log_epoch = None;
    }

    pub(crate) fn sync_with_log(&mut self, log: &ChangeLog) {
        let epoch = log.mutation_epoch();
        if self.expected_log_epoch != Some(epoch) {
            self.undone.clear();
            self.expected_log_epoch = Some(epoch);
        }
    }

    /// Record a committed atomic action journal for future undo/redo.
    pub fn push_action(&mut self, journal: crate::engine::ActionJournal) {
        self.actions_undone.clear();
        if self.max_actions == Some(0) {
            return;
        }
        self.actions_done.push_back(journal);
        enforce_stack_cap(&mut self.actions_done, self.max_actions);
    }

    pub fn pop_undo_action(&mut self) -> Option<crate::engine::ActionJournal> {
        self.actions_done.pop_back()
    }

    pub fn push_redo_action(&mut self, journal: crate::engine::ActionJournal) {
        if self.max_actions == Some(0) {
            return;
        }
        self.actions_undone.push_back(journal);
        enforce_stack_cap(&mut self.actions_undone, self.max_actions);
    }

    pub fn pop_redo_action(&mut self) -> Option<crate::engine::ActionJournal> {
        self.actions_undone.pop_back()
    }

    pub fn push_done_action(&mut self, journal: crate::engine::ActionJournal) {
        if self.max_actions == Some(0) {
            return;
        }
        self.actions_done.push_back(journal);
        enforce_stack_cap(&mut self.actions_done, self.max_actions);
    }

    /// Undo last group in the provided change log, applying inverses through a VertexEditor.
    pub fn undo(
        &mut self,
        graph: &mut DependencyGraph,
        log: &mut ChangeLog,
    ) -> Result<Vec<UndoBatchItem>, EditorError> {
        self.sync_with_log(log);
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
        graph.begin_deferred_dirty();
        graph.authority_set_replay(crate::engine::authority::history::Replay::Undo);
        let replayed = (|| {
            let mut editor = VertexEditor::new(graph);
            for (i, item) in batch.iter().enumerate().rev() {
                if let Some(description) =
                    super::change_log::compound_start_description(i, |j| &batch[j].event)
                {
                    editor.inverse_compound_end(description);
                }
                editor.apply_inverse(item.event.clone())?;
            }
            Ok::<_, EditorError>(())
        })();
        graph.authority_set_replay(crate::engine::authority::history::Replay::Forward);
        let _ = graph.end_deferred_dirty();
        replayed?;

        // Keep a copy for redo, but also return the batch so callers can mirror side effects.
        if self.max_actions != Some(0) {
            self.undone.push_back(batch.clone());
            enforce_stack_cap(&mut self.undone, self.max_actions);
        }
        self.expected_log_epoch = Some(log.mutation_epoch());
        Ok(batch)
    }

    pub(crate) fn pending_redo_events(&self) -> Vec<ChangeEvent> {
        self.undone
            .back()
            .into_iter()
            .flat_map(|batch| batch.iter().map(|item| item.event.clone()))
            .collect()
    }

    pub(crate) fn pending_redo_events_for_log(&mut self, log: &ChangeLog) -> Vec<ChangeEvent> {
        self.sync_with_log(log);
        self.pending_redo_events()
    }

    pub fn redo(
        &mut self,
        graph: &mut DependencyGraph,
        log: &mut ChangeLog,
    ) -> Result<Vec<UndoBatchItem>, EditorError> {
        self.sync_with_log(log);
        if let Some(batch) = self.undone.pop_back() {
            enum RedoGroupMode {
                Bare,
                ReplayGroup,
                CompoundWrapper,
            }
            let mode = if batch.len() == 1
                && !matches!(
                    batch[0].event,
                    ChangeEvent::CompoundStart { .. } | ChangeEvent::CompoundEnd { .. }
                ) {
                RedoGroupMode::Bare
            } else if is_single_top_level_compound(&batch) {
                RedoGroupMode::ReplayGroup
            } else {
                RedoGroupMode::CompoundWrapper
            };
            match mode {
                RedoGroupMode::Bare => {}
                RedoGroupMode::ReplayGroup => {
                    log.begin_replay_group();
                }
                RedoGroupMode::CompoundWrapper => {
                    log.begin_compound("redo".to_string());
                }
            }
            // Return value for callers (e.g. Arrow mirroring) must remain available even though
            // we apply events by value below.
            let ret = batch.clone();

            graph.begin_deferred_dirty();
            graph.authority_set_replay(crate::engine::authority::history::Replay::Redo);
            let replayed = (|| {
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
                            editor.try_add_vertex(meta)?;
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
                            let _ = graph.restore_spill_snapshot(anchor, &new);
                        }
                        ChangeEvent::SpillCleared { anchor, .. } => {
                            graph.clear_spill_region(anchor);
                        }
                        ChangeEvent::TableResized {
                            name,
                            new_range,
                            new_empty_body,
                            ..
                        } => {
                            graph.resize_table_body(&name, new_range, new_empty_body)?;
                        }
                        ChangeEvent::SetRowVisibility { .. } => {
                            // Engine-level sidecar metadata; applied by Engine undo/redo wrappers.
                        }
                        ChangeEvent::CompoundStart { description, .. } => {
                            // A structural edit's marker shifts the retired-id
                            // side table (decision 27).
                            graph.replay_structural_marker(&description, true);
                        }
                        _ => {}
                    }
                }
                Ok::<_, EditorError>(())
            })();
            graph.authority_set_replay(crate::engine::authority::history::Replay::Forward);
            let _ = graph.end_deferred_dirty();
            match mode {
                RedoGroupMode::Bare => {}
                RedoGroupMode::ReplayGroup => log.end_replay_group(),
                RedoGroupMode::CompoundWrapper => log.end_compound(),
            }
            replayed?;
            self.expected_log_epoch = Some(log.mutation_epoch());
            Ok(ret)
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EvalConfig;
    use crate::engine::graph::editor::change_log::ChangeLog;
    use crate::reference::{CellRef, Coord};
    use formualizer_common::LiteralValue;

    fn create_test_graph() -> DependencyGraph {
        DependencyGraph::new_with_config(EvalConfig::default())
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
                // Formula cells: value cells have no vertex to move
                // (decision 27).
                editor.set_cell_formula(
                    cell,
                    formualizer_parse::parser::parse(format!("={r}")).unwrap(),
                );
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
    fn test_undo_redo_spill_clear_on_scalar_edit_restores_registry_and_cells() {
        let mut graph = create_test_graph();
        let sheet_id = graph.sheet_id_mut("Sheet1");

        let anchor_cell = CellRef::new(sheet_id, Coord::new(0, 0, true, true));
        let anchor_vid = {
            let mut editor = VertexEditor::new(&mut graph);
            // A formula anchors a spill (value cells have no vertex,
            // decision 27; this used a value cell's vertex).
            editor.set_cell_formula(anchor_cell, formualizer_parse::parser::parse("=0").unwrap())
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

        let mut undo = UndoEngine::new();
        undo.undo(&mut graph, &mut log).unwrap();

        assert!(graph.spill_registry_has_anchor(anchor_vid));
        for cell in &target_cells {
            assert_eq!(
                graph.spill_registry_anchor_for_cell(*cell),
                Some(anchor_vid)
            );
        }
        // Graph does not cache spill child values in Arrow-truth mode; the contract here is
        // that the spill registry ownership is restored by undo.

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
                // Formula cells: value cells have no vertex to move
                // (decision 27).
                editor.set_cell_formula(
                    cell,
                    formualizer_parse::parser::parse(format!("={c}")).unwrap(),
                );
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

    // Reclassified (M5, internal representation): asserts legacy's edge lists
    // through a RemoveVertex undo (`ChangeEvent::RemoveVertex` edge fields are a
    // decision-8 removal); values after undo are covered by the undo tests.
    #[ignore = "M5 legacy-internal: legacy edge lists across RemoveVertex undo"]
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
            let a1_vid = graph.get_vertex_id_for_address(&a1_cell).unwrap();
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

    #[test]
    fn test_action_journal_stack_caps_fifo() {
        use crate::engine::ActionJournal;

        let mut undo = UndoEngine::with_max_actions(3);
        for i in 0..5 {
            undo.push_action(ActionJournal {
                name: format!("act-{i}"),
                ..Default::default()
            });
        }
        assert_eq!(undo.actions_done_len(), 3);
        assert_eq!(undo.actions_undone_len(), 0);

        let popped: Vec<String> = std::iter::from_fn(|| undo.pop_undo_action())
            .map(|j| j.name)
            .collect();
        assert_eq!(popped, vec!["act-4", "act-3", "act-2"]);
        assert_eq!(undo.actions_done_len(), 0);

        for name in popped.into_iter().rev() {
            undo.push_redo_action(ActionJournal {
                name,
                ..Default::default()
            });
        }
        assert_eq!(undo.actions_undone_len(), 3);
        undo.set_max_actions(Some(2));
        assert_eq!(undo.actions_undone_len(), 2);
        let redone: Vec<String> = std::iter::from_fn(|| undo.pop_redo_action())
            .map(|j| j.name)
            .collect();
        assert_eq!(redone, vec!["act-4", "act-3"]);
    }

    #[test]
    fn test_partial_compound_group_is_not_partially_undone() {
        let mut graph = create_test_graph();
        let sheet_id = graph.sheet_id_mut("Sheet1");
        let a1 = CellRef::new(sheet_id, Coord::new(0, 0, true, true));
        let b1 = CellRef::new(sheet_id, Coord::new(0, 1, true, true));
        let c1 = CellRef::new(sheet_id, Coord::new(0, 2, true, true));
        let d1 = CellRef::new(sheet_id, Coord::new(0, 3, true, true));

        let mut log = ChangeLog::with_max_changelog_events(5);

        // Compound 1: 4 events (CompoundStart, SetValue A1, SetValue B1, CompoundEnd).
        log.begin_compound("c1".into());
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.set_cell_value(a1, LiteralValue::Number(1.0));
            editor.set_cell_value(b1, LiteralValue::Number(2.0));
        }
        log.end_compound();

        // Compound 2: 4 events (CompoundStart, SetValue C1, SetValue D1, CompoundEnd).
        // Cap 5 evicts the first 3 events of Compound 1, leaving only CompoundEnd of c1.
        log.begin_compound("c2".into());
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.set_cell_value(c1, LiteralValue::Number(3.0));
            editor.set_cell_value(d1, LiteralValue::Number(4.0));
        }
        log.end_compound();

        assert_eq!(log.len(), 5);
        let mut undo = UndoEngine::with_max_actions(5);

        // Undo 1 cleanly reverts Compound 2.
        let undone_c2 = undo.undo(&mut graph, &mut log).unwrap();
        assert_eq!(undone_c2.len(), 4);

        // Undo 2 must be a clean no-op: Compound 1 was partially evicted.
        let undone_c1 = undo.undo(&mut graph, &mut log).unwrap();
        assert!(undone_c1.is_empty());

        // Redo brings Compound 2 back cleanly without inflating event count.
        let redone_c2 = undo.redo(&mut graph, &mut log).unwrap();
        assert_eq!(redone_c2.len(), 4);
    }

    #[test]
    fn test_ungrouped_edit_after_compound_group_and_redo_invalidation() {
        let mut graph = create_test_graph();
        let sheet_id = graph.sheet_id_mut("Sheet1");
        let a1 = CellRef::new(sheet_id, Coord::new(0, 0, true, true));
        let b1 = CellRef::new(sheet_id, Coord::new(0, 1, true, true));

        let mut log = ChangeLog::with_max_changelog_events(10);
        log.begin_compound("group".into());
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.set_cell_value(a1, LiteralValue::Number(1.0));
        }
        log.end_compound();

        // Ungrouped edit after a compound group.
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.set_cell_value(b1, LiteralValue::Number(2.0));
        }

        let mut undo = UndoEngine::new();
        let u1 = undo.undo(&mut graph, &mut log).unwrap();
        assert_eq!(u1.len(), 1);
        assert_eq!(undo.undone_len(), 1);

        // A new forward edit invalidates the logged redo stack.
        {
            let mut editor = VertexEditor::with_logger(&mut graph, &mut log);
            editor.set_cell_value(b1, LiteralValue::Number(99.0));
        }
        let r = undo.redo(&mut graph, &mut log).unwrap();
        assert!(r.is_empty());
        assert_eq!(undo.undone_len(), 0);
    }
}
