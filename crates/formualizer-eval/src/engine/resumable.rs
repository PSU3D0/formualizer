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
    pub fn record_original_cycles(&mut self, schedule: &super::scheduler::Schedule) {
        if !self.original_cycles_recorded {
            for cycle in &schedule.cycles {
                let mut key = cycle.clone();
                key.sort_unstable();
                self.original_cycles.insert(key);
            }
            self.original_cycles_recorded = true;
        }
    }

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
