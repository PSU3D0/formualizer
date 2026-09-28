use crate::engine::VertexId;
use crate::engine::idset::DenseIdSet;

/// Graph-owned dirty set: the formula vertices awaiting evaluation (a
/// bitmap by vertex id, Program 3; iteration is in ascending id order).
#[derive(Debug, Default)]
pub(super) struct FormulaDirtyState {
    legacy_vertices: DenseIdSet,
}

impl FormulaDirtyState {
    pub(super) fn legacy_len(&self) -> usize {
        self.legacy_vertices.len()
    }

    pub(super) fn legacy_contains(&self, vertex: &VertexId) -> bool {
        self.legacy_vertices.contains(vertex)
    }

    pub(super) fn legacy_insert(&mut self, vertex: VertexId) {
        self.legacy_vertices.insert(vertex);
    }

    pub(super) fn legacy_extend(&mut self, vertices: impl IntoIterator<Item = VertexId>) {
        self.legacy_vertices.extend(vertices);
    }

    pub(super) fn legacy_remove(&mut self, vertex: &VertexId) {
        self.legacy_vertices.remove(vertex);
    }

    /// Iteration covers only the words between the lowest and highest id
    /// set since the set was last empty; an empty set gives back large
    /// words.
    pub(super) fn legacy_shrink_if_sparse(&mut self) {
        self.legacy_vertices.shrink_if_empty();
    }

    pub(super) fn legacy_reserve(&mut self, _additional: usize) {}

    pub(super) fn legacy_iter(&self) -> impl Iterator<Item = VertexId> + '_ {
        self.legacy_vertices.iter()
    }
}
