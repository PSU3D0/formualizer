//! Formualizer Dependency Graph Engine
//!
//! Provides incremental formula evaluation with dependency tracking.

pub mod arrow_ingest;
pub mod eval;
pub mod eval_delta;
pub mod graph;
pub mod ingest;
pub mod ingest_builder;
pub mod plan;
pub mod range_view;
pub mod scheduler;
pub mod spill;
pub mod vertex;

// New SoA modules
pub mod csr_edges;
pub mod debug_views;
pub mod delta_edges;
pub mod interval_tree;
pub mod named_range;
pub mod sheet_index;
pub mod sheet_registry;
pub mod topo;
pub mod vertex_store;

// Phase 1: Arena modules
pub mod arena;

// Phase 1: Warmup configuration (kept for compatibility)
pub mod tuning;

#[cfg(test)]
mod tests;

pub use eval::{Engine, EvalResult, RecalcPlan};
pub use eval_delta::{DeltaMode, EvalDelta};
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

impl<R: EvaluationContext> Engine<R> {
    pub fn begin_bulk_ingest(&mut self) -> ingest_builder::BulkIngestBuilder<'_> {
        ingest_builder::BulkIngestBuilder::new(&mut self.graph)
    }
}

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

    /// Default sheet name used when no sheet is provided.
    pub default_sheet_name: String,

    /// Stable workbook seed used for deterministic RNG composition
    pub workbook_seed: u64,

    /// Volatile granularity for RNG seeding and re-evaluation policy
    pub volatile_level: VolatileLevel,

    // Range handling configuration (Phase 5)
    /// Ranges with size <= this limit are expanded into individual Cell dependencies
    pub range_expansion_limit: usize,

    /// Fallback maximum row bound for open-ended references (e.g. `A:A`, `A1:A`).
    ///
    /// This is only used when used-bounds cannot be determined.
    pub max_open_ended_rows: u32,

    /// Fallback maximum column bound for open-ended references (e.g. `1:1`, `A1:1`).
    ///
    /// This is only used when used-bounds cannot be determined.
    pub max_open_ended_cols: u32,

    /// Height of stripe blocks for dense range indexing
    pub stripe_height: u32,
    /// Width of stripe blocks for dense range indexing  
    pub stripe_width: u32,
    /// Enable block stripes for dense ranges (vs row/column stripes only)
    pub enable_block_stripes: bool,

    /// Spill behavior configuration (conflicts, bounds, buffering)
    pub spill: SpillConfig,

    /// Use dynamic topological ordering (Pearce-Kelly algorithm)
    pub use_dynamic_topo: bool,
    /// Maximum nodes to visit before falling back to full rebuild
    pub pk_visit_budget: usize,
    /// Operations between periodic rank compaction
    pub pk_compaction_interval_ops: u64,
    /// Maximum width for parallel evaluation layers
    pub max_layer_width: Option<usize>,
    /// If true, reject edge insertions that would create a cycle (skip adding that dependency).
    /// If false, allow insertion and let scheduler handle cycles at evaluation time.
    pub pk_reject_cycle_edges: bool,
    /// Sheet index build strategy for bulk loads
    pub sheet_index_mode: SheetIndexMode,

    /// Warmup configuration for global pass planning (Phase 1)
    pub warmup: tuning::WarmupConfig,

    /// Enable Arrow-backed storage reads (Phase A)
    pub arrow_storage_enabled: bool,
    /// Enable delta overlay for Arrow sheets (Phase C)
    pub delta_overlay_enabled: bool,

    /// Mirror formula scalar results into Arrow overlay for Arrow-backed reads
    /// This enables Arrow-only RangeView correctness without Hybrid fallback.
    pub write_formula_overlay_enabled: bool,

    /// Workbook date system: Excel 1900 (default) or 1904.
    pub date_system: DateSystem,

    /// Defer dependency graph building: ingest values immediately but stage formulas
    /// for on-demand graph construction during evaluation.
    pub defer_graph_building: bool,
}

impl Default for EvalConfig {
    fn default() -> Self {
        Self {
            enable_parallel: true,
            max_threads: None,
            max_vertices: None,
            max_eval_time: None,
            max_memory_mb: None,

            default_sheet_name: format!("Sheet{}", 1),

            // Deterministic RNG seed (matches traits default)
            workbook_seed: 0xF0F0_D0D0_AAAA_5555,

            // Volatile model default
            volatile_level: VolatileLevel::Always,

            // Range handling defaults (Phase 5)
            range_expansion_limit: 64,
            // Open-ended reference defaults (Excel max dimensions).
            // Lower these to cap `A:A` / `1:1` when used-bounds are unknown.
            max_open_ended_rows: 1_048_576,
            max_open_ended_cols: 16_384,
            stripe_height: 256,
            stripe_width: 256,
            enable_block_stripes: false,
            spill: SpillConfig::default(),

            // Dynamic topology configuration
            use_dynamic_topo: false, // Disabled by default for compatibility
            pk_visit_budget: 50_000,
            pk_compaction_interval_ops: 100_000,
            max_layer_width: None,
            pk_reject_cycle_edges: false,
            sheet_index_mode: SheetIndexMode::Eager,
            warmup: tuning::WarmupConfig::default(),
            arrow_storage_enabled: true,
            delta_overlay_enabled: true,
            write_formula_overlay_enabled: true,
            date_system: DateSystem::Excel1900,
            defer_graph_building: false,
        }
    }
}

impl EvalConfig {
    #[inline]
    pub fn with_range_expansion_limit(mut self, limit: usize) -> Self {
        self.range_expansion_limit = limit;
        self
    }

    #[inline]
    pub fn with_parallel(mut self, enable: bool) -> Self {
        self.enable_parallel = enable;
        self
    }

    #[inline]
    pub fn with_block_stripes(mut self, enable: bool) -> Self {
        self.enable_block_stripes = enable;
        self
    }

    #[inline]
    pub fn with_arrow_storage(mut self, enable: bool) -> Self {
        self.arrow_storage_enabled = enable;
        self
    }

    #[inline]
    pub fn with_delta_overlay(mut self, enable: bool) -> Self {
        self.delta_overlay_enabled = enable;
        self
    }

    #[inline]
    pub fn with_formula_overlay(mut self, enable: bool) -> Self {
        self.write_formula_overlay_enabled = enable;
        self
    }

    #[inline]
    pub fn with_date_system(mut self, system: DateSystem) -> Self {
        self.date_system = system;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SheetIndexMode {
    /// Build full interval-tree based index during inserts (current behavior)
    Eager,
    /// Defer building any sheet index until first range query or explicit finalize
    Lazy,
    /// Use fast batch building (sorted arrays -> tree) when bulk loading, otherwise incremental
    FastBatch,
}

pub use formualizer_common::DateSystem;

/// Construct a new engine with the given resolver and configuration
pub fn new_engine<R>(resolver: R, config: EvalConfig) -> Engine<R>
where
    R: EvaluationContext + 'static,
{
    Engine::new(resolver, config)
}

/// Configuration for spill behavior. Nested under EvalConfig to avoid bloating the top-level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillConfig {
    /// What to do when target region overlaps non-empty cells or other spills.
    pub conflict_policy: SpillConflictPolicy,
    /// Tiebreaker used when policy allows preemption or multiple anchors race.
    pub tiebreaker: SpillTiebreaker,
    /// Bounds handling when result exceeds sheet capacity.
    pub bounds_policy: SpillBoundsPolicy,
    /// Buffering approach for spill writes.
    pub buffer_mode: SpillBufferMode,
    /// Optional memory budget for shadow buffering in bytes.
    pub memory_budget_bytes: Option<u64>,
    /// Cancellation behavior while streaming rows.
    pub cancellation: SpillCancellationPolicy,
    /// Visibility policy for staged writes.
    pub visibility: SpillVisibility,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            conflict_policy: SpillConflictPolicy::Error,
            tiebreaker: SpillTiebreaker::FirstWins,
            bounds_policy: SpillBoundsPolicy::Strict,
            buffer_mode: SpillBufferMode::ShadowBuffer,
            memory_budget_bytes: None,
            cancellation: SpillCancellationPolicy::Cooperative,
            visibility: SpillVisibility::OnCommit,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillConflictPolicy {
    Error,
    Preempt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillTiebreaker {
    FirstWins,
    EvaluationEpochAsc,
    AnchorAddressAsc,
    FunctionPriorityThenAddress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillBoundsPolicy {
    Strict,
    Truncate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillBufferMode {
    ShadowBuffer,
    PersistenceJournal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillCancellationPolicy {
    Cooperative,
    Strict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillVisibility {
    OnCommit,
    StagedLayer,
}
