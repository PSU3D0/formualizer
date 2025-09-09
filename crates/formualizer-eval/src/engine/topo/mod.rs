//! Dynamic topological ordering utilities
//!
//! Exposes a Pearce–Kelly based fully-dynamic topological orderer. This module owns
//! only ordering metadata; the dependency graph remains the source of truth for edges.

pub mod pk;

use crate::engine::graph::DependencyGraph;
use crate::engine::vertex::VertexId;

/// Adapter to expose the engine's dependency graph as a conceptual DAG view
/// where edges go from precedent (dependency) -> dependent (formula).
#[derive(Clone, Copy, Debug)]
pub struct GraphAdapter<'a> {
    pub g: &'a DependencyGraph,
}

impl<'a> GraphAdapter<'a> {
    pub fn new(g: &'a DependencyGraph) -> Self {
        Self { g }
    }
}

impl pk::GraphView<VertexId> for GraphAdapter<'_> {
    fn successors(&self, n: VertexId, out: &mut Vec<VertexId>) {
        // Conceptual successors of a precedent are dependents in our storage
        out.clear();
        out.extend(self.g.get_dependents(n));
    }

    fn predecessors(&self, n: VertexId, out: &mut Vec<VertexId>) {
        // Conceptual predecessors of a dependent are its dependencies in storage
        out.clear();
        out.extend(self.g.get_dependencies(n));
    }

    fn exists(&self, n: VertexId) -> bool {
        self.g.vertex_exists(n)
    }
}
