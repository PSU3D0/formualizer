use crate::error::IoError;
use formualizer_eval::engine::graph::DependencyGraph;
use formualizer_eval::engine::graph::editor::change_log::ChangeLog;
use formualizer_eval::engine::graph::editor::undo_engine::UndoEngine;
use formualizer_eval::engine::graph::editor::vertex_editor::{EditorError, VertexEditor};

/// IO-level configuration toggles.
#[derive(Clone, Debug)]
pub struct IoConfig {
    /// When false, ChangeLog is disabled and undo/redo become no-ops.
    pub enable_changelog: bool,
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            enable_changelog: true,
        }
    }
}

/// High-level editing session that owns a graph and optional change tracking.
pub struct EditorSession {
    pub graph: DependencyGraph,
    enable_changelog: bool,
    log: ChangeLog,
    undo: UndoEngine,
}

impl EditorSession {
    pub fn new_with_graph(graph: DependencyGraph, cfg: IoConfig) -> Self {
        let mut log = ChangeLog::new();
        log.set_enabled(cfg.enable_changelog);
        Self {
            graph,
            enable_changelog: cfg.enable_changelog,
            log,
            undo: UndoEngine::new(),
        }
    }

    pub fn new(cfg: IoConfig) -> Self {
        Self::new_with_graph(DependencyGraph::new(), cfg)
    }

    /// Begin a user-visible action; opens a compound group when enabled.
    pub fn begin_action(&mut self, description: impl Into<String>) {
        if self.enable_changelog {
            self.log.begin_compound(description.into());
        }
    }

    /// End the current user-visible action; closes group when enabled.
    pub fn end_action(&mut self) {
        if self.enable_changelog {
            self.log.end_compound();
        }
    }

    /// Helper to run an action with automatic begin/end.
    pub fn with_action<F, R>(
        &mut self,
        description: impl Into<String>,
        f: F,
    ) -> Result<R, EditorError>
    where
        F: FnOnce(&mut VertexEditor) -> Result<R, EditorError>,
    {
        self.begin_action(description);
        let res = {
            let mut editor = self.make_editor();
            f(&mut editor)
        };
        self.end_action();
        res
    }

    /// Get a VertexEditor wired appropriately (with or without logger).
    pub fn make_editor(&mut self) -> VertexEditor<'_> {
        if self.enable_changelog {
            VertexEditor::with_logger(&mut self.graph, &mut self.log)
        } else {
            VertexEditor::new(&mut self.graph)
        }
    }

    /// Undo last compound group if enabled. No-op otherwise.
    pub fn undo(&mut self) -> Result<(), EditorError> {
        if self.enable_changelog {
            self.undo.undo(&mut self.graph, &mut self.log)
        } else {
            Ok(())
        }
    }

    /// Redo last undone group if enabled. No-op otherwise.
    pub fn redo(&mut self) -> Result<(), EditorError> {
        if self.enable_changelog {
            self.undo.redo(&mut self.graph, &mut self.log)
        } else {
            Ok(())
        }
    }

    /// Commit using a persistence callback. On error, roll back last group when enabled.
    pub fn commit_with_rollback<F>(&mut self, persist: F) -> Result<(), IoError>
    where
        F: FnOnce(&DependencyGraph) -> Result<(), IoError>,
    {
        match persist(&self.graph) {
            Ok(()) => Ok(()),
            Err(e) => {
                if self.enable_changelog {
                    // Best-effort rollback of the last group
                    let _ = self.undo();
                }
                Err(e)
            }
        }
    }

    /// Access to ChangeLog for inspection; None when disabled.
    pub fn changelog_enabled(&self) -> bool {
        self.enable_changelog
    }
}
