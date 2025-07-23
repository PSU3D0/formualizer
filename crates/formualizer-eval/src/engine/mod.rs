//! Formualizer Dependency Graph Engine
//!
//! Provides incremental formula evaluation with dependency tracking.

pub mod eval;
pub mod graph;
pub mod range_stream;
pub mod scheduler;
pub mod vertex;

#[cfg(test)]
mod tests;

pub use eval::{Engine, EvalResult};
pub use graph::{CellAddr, DependencyGraph};
pub use scheduler::{Layer, Schedule, Scheduler};
pub use vertex::{Vertex, VertexId, VertexKind};

use crate::traits::EvaluationContext;

/// Configuration for the evaluation engine
#[derive(Debug, Clone)]
pub struct EvalConfig {
    pub enable_parallel: bool,
    pub max_threads: Option<usize>,
    // 🔮 Scalability Hook: Resource limits (future-proofing)
    pub max_vertices: Option<usize>,
    pub max_eval_time: Option<std::time::Duration>,
    pub max_memory_mb: Option<usize>,
}

impl Default for EvalConfig {
    fn default() -> Self {
        Self {
            enable_parallel: true,
            max_threads: None,
            max_vertices: None,
            max_eval_time: None,
            max_memory_mb: None,
        }
    }
}

/// Construct a new engine with the given resolver and configuration
pub fn new_engine<R>(resolver: R, config: EvalConfig) -> Engine<R>
where
    R: EvaluationContext + 'static,
{
    Engine::new(resolver, config)
}
