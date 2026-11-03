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
        Self { soft_deadline: Some(duration), cancel: None }
    }
    pub fn cancel(token: CancelToken) -> Self {
        Self::none().with_cancel(token)
    }
    pub fn none() -> Self { Self::default() }
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
pub enum StopReason { Deadline, Cancelled }
