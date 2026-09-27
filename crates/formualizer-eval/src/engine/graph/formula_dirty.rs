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

    /// Give back capacity after removals: iterating a hash set costs its
    /// capacity, so a set that held every formula at first eval would make
    /// every later small recalc O(formulas). Shrinks once the set is under
    /// 1/8 of its capacity; each shrink at least quarters the capacity, so
    /// the cost is amortized over the removals.
    pub(super) fn legacy_shrink_if_sparse(&mut self) {
        const MIN_CAPACITY: usize = 1024;
        let cap = self.legacy_vertices.capacity();
        if cap > MIN_CAPACITY && self.legacy_vertices.len().saturating_mul(8) < cap {
            self.legacy_vertices
                .shrink_to(self.legacy_vertices.len().saturating_mul(2));
        }
    }

    pub(super) fn legacy_reserve(&mut self, additional: usize) {
        self.legacy_vertices.reserve(additional);
    }

    pub(super) fn legacy_iter(&self) -> impl Iterator<Item = &VertexId> {
        self.legacy_vertices.iter()
    }
}
