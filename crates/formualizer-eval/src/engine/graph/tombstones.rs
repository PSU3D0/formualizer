use crate::engine::VertexId;
use rustc_hash::FxHashMap;

/// Categorizes different types of names that can go missing in a workbook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityKind {
    Sheet,
    NamedRange,
    Table,
    CustomFunction,
}

/// The TombstoneRegistry acts as a "Subscription Manager" for formulas
/// that are waiting for a missing entity to (re)appear.
#[derive(Default, Debug, Clone)]
pub struct TombstoneRegistry {
    // Maps (Kind, Name) -> List of Vertices that need a rebuild
    pending: FxHashMap<(EntityKind, String), Vec<VertexId>>,
}

impl TombstoneRegistry {
    /// Registers a vertex as "waiting" for a specific entity.
    pub fn register(&mut self, kind: EntityKind, name: &str, dependent: VertexId) {
        self.pending
            .entry((kind, name.to_string()))
            .or_default()
            .push(dependent);
    }

    /// Retrieves all vertices waiting for a specific entity and clears them.
    /// This is called when the entity is created or renamed.
    pub fn take_dependents(&mut self, kind: EntityKind, name: &str) -> Vec<VertexId> {
        self.pending
            .remove(&(kind, name.to_string()))
            .unwrap_or_default()
    }

    /// Checks if any formulas are currently orphaned.
    pub fn has_orphans(&self) -> bool {
        !self.pending.is_empty()
    }
}
