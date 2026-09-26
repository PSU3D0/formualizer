use rustc_hash::FxHashSet;

use crate::engine::VertexId;

/// Graph-owned dirty set: the formula vertices awaiting evaluation.
#[derive(Debug, Default)]
pub(super) struct FormulaDirtyState {
    legacy_vertices: FxHashSet<VertexId>,
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

    pub(super) fn legacy_reserve(&mut self, additional: usize) {
        self.legacy_vertices.reserve(additional);
    }

    pub(super) fn legacy_iter(&self) -> impl Iterator<Item = &VertexId> {
        self.legacy_vertices.iter()
    }
}
