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

/// A job lifecycle or evaluation failure.
#[derive(Debug)]
#[non_exhaustive]
pub enum RecalcJobError {
    /// No explicit job exists.
    NoActiveJob,
    /// A logical recalculation already exists.
    AlreadyPending {
        /// True for an explicit job; false for an implicit continuation.
        explicit: bool,
    },
    /// A step failed. The job has ended; unfinished work remains dirty.
    Evaluation(formualizer_common::ExcelError),
}
impl std::fmt::Display for RecalcJobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoActiveJob => f.write_str("no active recalculation job"),
            Self::AlreadyPending { explicit } => write!(f, "recalculation already pending (explicit: {explicit})"),
            Self::Evaluation(error) => write!(f, "recalculation failed: {error}"),
        }
    }
}
impl std::error::Error for RecalcJobError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Evaluation(error) => Some(error),
            _ => None,
        }
    }
}
impl From<formualizer_common::ExcelError> for RecalcJobError {
    fn from(error: formualizer_common::ExcelError) -> Self { Self::Evaluation(error) }
}

/// Terminal or suspended outcome of the closure driver.
#[derive(Debug)]
#[non_exhaustive]
pub enum RecalcOutcome {
    /// All work finished; the job is no longer active.
    Finished(EvalResult),
    /// Cancellation ended the job.
    Cancelled,
    /// The callback broke; the job remains active for manual stepping.
    Paused,
}

/// Options for an engine-owned recalculation job.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RecalcJobOptions {
    /// Collect changed regions on each step (default true).
    pub collect_deltas: bool,
    /// Maximum coalesced records returned per step (default 1,024).
    /// Overflow returns sheet identities instead; collection itself is per-step.
    pub max_delta_records: usize,
    /// Initial viewport targets, materialized lazily after full planning.
    pub initial_priority: Vec<super::EvaluationTarget>,
}
impl Default for RecalcJobOptions {
    fn default() -> Self {
        Self {
            collect_deltas: true,
            max_delta_records: 1024,
            initial_priority: Vec::new(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StepPhase {
    BuildingGraph,
    Scheduling,
    Evaluating,
    Finished,
    Cancelled,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StepProgress {
    /// One-based schedule pass; changes invalidate the previous denominator.
    pub pass: u32,
    /// Full-schedule formula vertices visited, including clean priority work.
    pub completed_in_pass: usize,
    pub scheduled_in_pass: usize,
    pub cumulative_computed_vertices: usize,
    /// Conservative proof of a fixed total. False also means unknown:
    /// proofs are limited to 1,024 scalar arithmetic templates in v1.
    pub total_exact: bool,
    pub staged_formulas_remaining: usize,
    pub priority_pending: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StepDelta {
    /// Changed regions; scalar bulk commits conservatively include the whole
    /// installed run, so a region can also contain cells with unchanged values.
    pub delta: super::TargetEvalDelta,
    /// Repaint visible cells on these sheets. When populated, delta is empty.
    pub overflow_sheets: Vec<formualizer_common::SheetId>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SlowUnitKind {
    GraphBuild,
    ScheduleBuild,
    SingleCell,
    FamilyRun,
    Cycle,
}
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
/// Per-step locations and telemetry. Reads between steps observe installed
/// new values alongside old values for still-pending cells. Only Finished
/// carries a result; Cancelled is terminal for explicit jobs.
pub struct StepReport {
    pub phase: StepPhase,
    pub progress: StepProgress,
    pub step_computed_vertices: usize,
    pub step_elapsed: Duration,
    pub changed: StepDelta,
    pub longest_unit: Option<SlowUnitReport>,
    pub result: Option<EvalResult>,
}

/// Held pass, clock, hints, collector and pending iterative re-dirties.
#[derive(Default)]
pub(super) struct LogicalRecalculation {
    pub held: Option<super::eval::HeldRecalcSchedule>,
    pub pass: u32,
    pub pass_completed: usize,
    pub pass_scheduled: usize,
    pub pass_exact: bool,
    pub priority_targets: Vec<super::EvaluationTarget>,
    pub priority_deferred: bool,
    pub work_units: u64,
    pub active_elapsed: Duration,
    pub seed_retained_bytes: u64,
    pub topology_epoch: u64,
    pub authority_revision: (u64, u64),
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
    pub fn refresh_seed_retained_bytes(&mut self) {
        use formualizer_common::LiteralValue;
        use std::mem::size_of;
        fn payload(value: &LiteralValue) -> usize {
            match value {
                LiteralValue::Text(s) => s.capacity(),
                LiteralValue::Array(rows) => {
                    rows.capacity() * size_of::<Vec<LiteralValue>>()
                        + rows
                            .iter()
                            .map(|r| {
                                r.capacity() * size_of::<LiteralValue>()
                                    + r.iter().map(payload).sum::<usize>()
                            })
                            .sum::<usize>()
                }
                _ => 0,
            }
        }
        self.seed_retained_bytes = (self.cycle_seeds.capacity()
            * size_of::<(
                Vec<super::VertexId>,
                rustc_hash::FxHashMap<super::VertexId, LiteralValue>,
            )>()
            + self
                .cycle_seeds
                .iter()
                .map(|(key, values)| {
                    key.capacity() * size_of::<super::VertexId>()
                        + values.capacity() * size_of::<(super::VertexId, LiteralValue)>()
                        + values.values().map(payload).sum::<usize>()
                })
                .sum::<usize>()
            + self.original_cycles.capacity() * size_of::<Vec<super::VertexId>>()
            + self
                .original_cycles
                .iter()
                .map(|k| k.capacity() * size_of::<super::VertexId>())
                .sum::<usize>()) as u64;
    }
    pub fn forget_dissolved_new_cycles(&mut self, vertices: &[super::VertexId]) {
        if self.new_cycle_members.is_empty()
            || !vertices.iter().any(|v| self.new_cycle_members.contains(v))
        {
            return;
        }
        // A new cycle that dissolved and ran acyclically must start from its
        // then-current values if it forms again. Original SCC seeds survive.
        let before = self.cycle_seeds.len();
        self.cycle_seeds.retain(|key, _| {
            self.original_cycles.contains(key)
                || !vertices.iter().any(|v| key.binary_search(v).is_ok())
        });
        if before != self.cycle_seeds.len() {
            self.refresh_seed_retained_bytes();
        }
    }
}
