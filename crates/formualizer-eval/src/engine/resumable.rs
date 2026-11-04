//! Cooperative, installed-work-preserving full recalculation.
use super::{CancelToken, EvalResult};
use std::time::Duration;

/// Controls one call. Deadlines are soft: at least one unit may finish.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct EvaluationStop {
    pub soft_deadline: Option<Duration>,
    pub cancel: Option<CancelToken>,
}
impl EvaluationStop {
    pub fn after(duration: Duration) -> Self {
        Self {
            soft_deadline: Some(duration),
            cancel: None,
        }
    }
    pub fn cancel(token: CancelToken) -> Self {
        Self::none().with_cancel(token)
    }
    pub fn none() -> Self {
        Self::default()
    }
    pub fn with_cancel(mut self, token: CancelToken) -> Self {
        self.cancel = Some(token);
        self
    }
}
#[derive(Debug)]
#[non_exhaustive]
pub enum EvaluationProgress {
    Complete(EvalResult),
    Incomplete(IncompleteEvaluation),
}
#[derive(Debug)]
#[non_exhaustive]
pub struct IncompleteEvaluation {
    pub reason: StopReason,
    /// Vertices computed during this call.
    pub computed_vertices: usize,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StopReason {
    Deadline,
    Cancelled,
}

/// Options for an engine-owned recalculation job.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RecalcJobOptions {
    pub collect_deltas: bool,
    pub max_delta_records: usize,
    pub initial_priority: Vec<super::EvaluationTarget>,
}
impl Default for RecalcJobOptions {
    fn default() -> Self {
        Self { collect_deltas: true, max_delta_records: 1024, initial_priority: Vec::new() }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StepPhase { BuildingGraph, Scheduling, Evaluating, Finished, Cancelled }
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StepProgress {
    pub pass: u32,
    pub completed_in_pass: usize,
    pub scheduled_in_pass: usize,
    pub cumulative_computed_vertices: usize,
    pub total_exact: bool,
    pub staged_formulas_remaining: usize,
    pub priority_pending: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StepDelta {
    pub delta: super::TargetEvalDelta,
    pub overflow_sheets: Vec<formualizer_common::SheetId>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SlowUnitKind { GraphBuild, ScheduleBuild, SingleCell, FamilyRun, Cycle }
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SlowUnitReport {
    pub kind: SlowUnitKind,
    pub representative_cell: Option<formualizer_common::PackedSheetCell>,
    pub vertex_count: usize,
    pub elapsed: Duration,
}
#[derive(Debug)]
#[non_exhaustive]
pub struct StepReport {
    pub phase: StepPhase,
    pub progress: StepProgress,
    pub step_computed_vertices: usize,
    pub step_elapsed: Duration,
    pub changed: StepDelta,
    pub longest_unit: Option<SlowUnitReport>,
    pub result: Option<EvalResult>,
}

/// Logical state only; no schedule/cursor survives a call. The engine's clock,
/// freshness hints, collector and pending iterative re-dirties share this lifetime.
#[derive(Default)]
pub(super) struct LogicalRecalculation {
    pub computed_vertices: usize,
    pub cycle_errors: usize,
    pub elapsed: Duration,
    pub replans: usize,
    // Keep independent admission revisions: ordinary edit APIs can consume
    // the engine's planning semantic-epoch guard between resumable calls.
    pub function_epoch: u64,
    pub provider_revision: Option<u64>,
    pub mutation_revision: u64,
    pub source_mutation_generation: u64,
    // FR5 must also propagate from readers installed in an earlier slice.
    pub installed: super::idset::DenseIdSet,
    // Retrying an SCC after an edit/cancellation must not count another
    // recalculation's iteration against its already advanced visible values.
    pub cycle_seeds: rustc_hash::FxHashMap<
        Vec<super::VertexId>,
        rustc_hash::FxHashMap<super::VertexId, formualizer_common::LiteralValue>,
    >,
    pub original_cycles: rustc_hash::FxHashSet<Vec<super::VertexId>>,
    pub original_cycles_recorded: bool,
    // Fast rejection for ordinary layer units; no scan of SCC seed maps on
    // the usual acyclic path. Conservatively retains bits after dissolution.
    pub new_cycle_members: super::idset::DenseIdSet,
}

impl LogicalRecalculation {
    pub fn forget_dissolved_new_cycles(&mut self, vertices: &[super::VertexId]) {
        if self.new_cycle_members.is_empty()
            || !vertices.iter().any(|v| self.new_cycle_members.contains(v))
        {
            return;
        }
        // A new cycle that dissolved and ran acyclically must start from its
        // then-current values if it forms again. Original SCC seeds survive.
        self.cycle_seeds.retain(|key, _| {
            self.original_cycles.contains(key)
                || !vertices.iter().any(|v| key.binary_search(v).is_ok())
        });
    }
}
