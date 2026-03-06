use crate::engine::vertex::VertexId;
use rustc_hash::FxHashMap;

/// Categorizes different types of names that can go missing in a workbook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityKind {
    Sheet,
    NamedRange,
    Table,
    TableColumn,
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
        let key = Self::make_key(kind, name);
        self.pending.entry(key).or_default().push(dependent);
    }

    pub fn take_dependents(&mut self, kind: EntityKind, name: &str) -> Vec<VertexId> {
        let key = Self::make_key(kind, name);
        self.pending.remove(&key).unwrap_or_default()
    }

    /// Checks if any formulas are currently orphaned.
    pub fn has_orphans(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Internal helper to create a consistent lookup key based on entity rules
    fn make_key(kind: EntityKind, name: &str) -> (EntityKind, String) {
        match kind {
            EntityKind::NamedRange
            | EntityKind::Table
            | EntityKind::TableColumn
            | EntityKind::Sheet => {
                // These are all case-insensitive in Excel
                (kind, name.to_uppercase())
            }
            _ => (kind, name.to_string()),
        }
    }

    pub fn list_all_keys(&self) -> Vec<(EntityKind, String)> {
        self.pending
            .keys()
            .cloned() // Assumes EntityKind and String are Clone
            .collect()
    }
}
