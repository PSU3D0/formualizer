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
    // FR5 must also propagate from readers installed in an earlier slice.
    pub installed: super::idset::DenseIdSet,
    // Retrying an SCC after an edit/cancellation must not count another
    // recalculation's iteration against its already advanced visible values.
    pub cycle_seeds: rustc_hash::FxHashMap<super::VertexId, formualizer_common::LiteralValue>,
}
