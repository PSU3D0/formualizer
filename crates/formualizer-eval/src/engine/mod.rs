//! Formualizer Dependency Graph Engine
//!
//! Provides incremental formula evaluation with dependency tracking.

pub mod eval;
pub mod graph;
pub mod range_stream;
pub mod scheduler;
pub mod vertex;

// New SoA modules
pub mod csr_edges;
pub mod debug_views;
pub mod delta_edges;
pub mod epoch_tracker;
pub mod interval_tree;
pub mod named_range;
pub mod packed_coord;
pub mod sheet_index;
pub mod sheet_registry;
pub mod vertex_store;

// Phase 1: Arena modules
pub mod arena;

#[cfg(test)]
mod tests;

pub use eval::{Engine, EvalResult};
// Use SoA implementation
pub use graph::snapshot::VertexSnapshot;
pub use graph::{
    ChangeEvent, DependencyGraph, DependencyRef, OperationSummary, StripeKey, StripeType,
    block_index,
};
pub use scheduler::{Layer, Schedule, Scheduler};
pub use vertex::{VertexId, VertexKind};

pub use graph::editor::{
    DataUpdateSummary, EditorError, MetaUpdateSummary, RangeSummary, ShiftSummary, TransactionId,
    VertexDataPatch, VertexEditor, VertexMeta, VertexMetaPatch,
};

pub use graph::editor::change_log::{ChangeLog, ChangeLogger, NullChangeLogger};

// CalcObserver is defined below

use crate::traits::EvaluationContext;
use crate::traits::VolatileLevel;

/// 🔮 Scalability Hook: Performance monitoring trait for calculation observability
pub trait CalcObserver: Send + Sync {
    fn on_eval_start(&self, vertex_id: VertexId);
    fn on_eval_complete(&self, vertex_id: VertexId, duration: std::time::Duration);
    fn on_cycle_detected(&self, cycle: &[VertexId]);
    fn on_dirty_propagation(&self, vertex_id: VertexId, affected_count: usize);
}

/// Default no-op observer
impl CalcObserver for () {
    fn on_eval_start(&self, _vertex_id: VertexId) {}
    fn on_eval_complete(&self, _vertex_id: VertexId, _duration: std::time::Duration) {}
    fn on_cycle_detected(&self, _cycle: &[VertexId]) {}
    fn on_dirty_propagation(&self, _vertex_id: VertexId, _affected_count: usize) {}
}

/// Configuration for the evaluation engine
#[derive(Debug, Clone)]
pub struct EvalConfig {
    pub enable_parallel: bool,
    pub max_threads: Option<usize>,
    // 🔮 Scalability Hook: Resource limits (future-proofing)
    pub max_vertices: Option<usize>,
    pub max_eval_time: Option<std::time::Duration>,
    pub max_memory_mb: Option<usize>,

    /// Stable workbook seed used for deterministic RNG composition
    pub workbook_seed: u64,

    /// Volatile granularity for RNG seeding and re-evaluation policy
    pub volatile_level: VolatileLevel,

    // Range handling configuration (Phase 5)
    /// Ranges with size <= this limit are expanded into individual Cell dependencies
    pub range_expansion_limit: usize,
    /// Height of stripe blocks for dense range indexing
    pub stripe_height: u32,
    /// Width of stripe blocks for dense range indexing  
    pub stripe_width: u32,
    /// Enable block stripes for dense ranges (vs row/column stripes only)
    pub enable_block_stripes: bool,
}

impl Default for EvalConfig {
    fn default() -> Self {
        Self {
            enable_parallel: true,
            max_threads: None,
            max_vertices: None,
            max_eval_time: None,
            max_memory_mb: None,

            // Deterministic RNG seed (matches traits default)
            workbook_seed: 0xF0F0_D0D0_AAAA_5555,

            // Volatile model default
            volatile_level: VolatileLevel::Always,

            // Range handling defaults (Phase 5)
            range_expansion_limit: 64,
            stripe_height: 256,
            stripe_width: 256,
            enable_block_stripes: false,
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
