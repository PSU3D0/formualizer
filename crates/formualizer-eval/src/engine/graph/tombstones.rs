use crate::engine::vertex::VertexId;
use rustc_hash::{FxHashMap, FxHashSet}; // Added HashSet

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityKind {
    Sheet,
    NamedRange,
    Table,
    TableColumn,
    CustomFunction,
}

#[derive(Default, Debug, Clone)]
pub struct TombstoneRegistry {
    // Maps (Kind, NormalizedName) -> Set of Unique Vertices
    pending: FxHashMap<(EntityKind, String), FxHashSet<VertexId>>,
}

impl TombstoneRegistry {
    pub fn register(&mut self, kind: EntityKind, name: &str, dependent: VertexId) {
        let key = Self::make_key(kind, name);
        self.pending.entry(key).or_default().insert(dependent);
    }

    /// Returns all vertices waiting for this entity and clears them from the registry.
    pub fn take_dependents(&mut self, kind: EntityKind, name: &str) -> Vec<VertexId> {
        let key = Self::make_key(kind, name);
        self.pending
            .remove(&key)
            .map(|set| set.into_iter().collect())
            .unwrap_or_default()
    }

    pub fn has_orphans(&self) -> bool {
        !self.pending.is_empty()
    }

    fn make_key(kind: EntityKind, name: &str) -> (EntityKind, String) {
        match kind {
            EntityKind::NamedRange
            | EntityKind::Table
            | EntityKind::TableColumn
            | EntityKind::Sheet => {
                // Excel is case-insensitive for these; use uppercase for stable lookup keys.
                (kind, name.to_uppercase())
            }
            _ => (kind, name.to_string()),
        }
    }

    /// Returns a list of all names the graph is currently "listening" for.
    pub fn list_pending_entities(&self) -> Vec<(EntityKind, String)> {
        self.pending.keys().cloned().collect()
    }
}
