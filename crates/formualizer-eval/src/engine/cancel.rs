//! Cooperative cancellation for engine calls.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use crate::instant::FzInstant;
use formualizer_common::{ExcelError, ExcelErrorKind, ResourceExhaustionReason};

/// A cooperative cancellation signal for a single engine call.
///
/// Cancellation is checked at engine checkpoints during both target preparation
/// and evaluation. Setting it does not abort work in progress; it causes the next
/// checkpoint to return [`ExcelErrorKind::Cancelled`](formualizer_common::ExcelErrorKind).
///
/// The token is a handle: cloning shares one signal, so a caller can hold a clone
/// and cancel from another thread.
///
/// ```
/// use formualizer_eval::engine::CancelToken;
///
/// let token = CancelToken::new();
/// let remote = token.clone();
/// assert!(!token.is_cancelled());
/// remote.cancel();
/// assert!(token.is_cancelled());
/// ```
///
/// The underlying representation is deliberately private so that richer
/// cancellation (cancellation reasons, linked child tokens) can be added without
/// a breaking change. Use [`CancelToken::from_flag`] to adopt a flag you already
/// own.
///
/// # Tokens handed to functions
///
/// The token a function receives from
/// [`FunctionContext::cancellation_token`](crate::traits::FunctionContext::cancellation_token)
/// (and the one a [`RangeView`](crate::engine::range_view::RangeView) polls
/// during a walk) is a request-scoped view of the caller's token. It also
/// reports cancelled once the request's evaluation deadline has passed or its
/// `max_work_units` budget is spent, so one poll covers every reason a
/// long-running function must stop. A function that observes it returns
/// [`ExcelErrorKind::Cancelled`]; the engine does not commit that result and
/// fails the request with the real reason (`Cancelled`, or a
/// resource-exhausted deadline or work-unit error).
#[derive(Clone, Debug, Default)]
pub struct CancelToken {
    flag: Arc<AtomicBool>,
    limits: Option<Arc<RequestLimits>>,
}

impl CancelToken {
    /// Creates a token that has not been cancelled.
    pub fn new() -> Self {
        Self::from_flag(Arc::new(AtomicBool::new(false)))
    }

    /// Signals cancellation. Idempotent, and safe to call from another thread.
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }

    /// Returns whether cancellation has been signalled.
    ///
    /// On a token handed to a function this is also true once the request's
    /// deadline or work budget is exhausted (see the type documentation).
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
            || self.limits.as_ref().is_some_and(|limits| limits.exceeded())
    }

    /// Adopts an existing flag, for callers that already own one or share it with
    /// non-engine code.
    pub fn from_flag(flag: Arc<AtomicBool>) -> Self {
        Self { flag, limits: None }
    }

    /// Borrows the underlying flag as a legacy/interoperability bridge.
    ///
    /// New code should poll [`CancelToken::is_cancelled`] instead. The raw flag
    /// carries only the caller's explicit cancellation: it does not report a
    /// passed deadline or a spent work budget.
    pub fn as_flag(&self) -> &Arc<AtomicBool> {
        &self.flag
    }

    /// The token functions see during a request: `caller`'s signal (if any)
    /// plus the request's deadline and work budget.
    pub(crate) fn request_scoped(caller: Option<&CancelToken>, limits: Arc<RequestLimits>) -> Self {
        Self {
            flag: caller.map_or_else(|| Arc::new(AtomicBool::new(false)), |t| t.flag.clone()),
            limits: Some(limits),
        }
    }

    /// Charges `units` of in-function work (about one per cell visited) and
    /// reports whether the function must stop, for any reason.
    #[inline]
    pub(crate) fn charge_work(&self, units: u64) -> bool {
        self.flag.load(Ordering::Relaxed)
            || self
                .limits
                .as_ref()
                .is_some_and(|limits| limits.charge(units))
    }
}

impl From<Arc<AtomicBool>> for CancelToken {
    fn from(flag: Arc<AtomicBool>) -> Self {
        Self::from_flag(flag)
    }
}

/// The in-function stop check: `Err(Cancelled)` when `token` reports that the
/// request was cancelled, passed its deadline, or spent its work budget after
/// charging `units` (about one per cell the caller is about to visit).
///
/// Call it at a bounded cadence inside any loop whose length is set by the
/// formula's inputs. The engine translates the error into the request's real
/// stop reason, so callers only propagate it.
#[inline]
pub(crate) fn checkpoint(token: Option<&CancelToken>, units: u64) -> Result<(), ExcelError> {
    match token {
        Some(token) if token.charge_work(units) => Err(ExcelError::new(ExcelErrorKind::Cancelled)),
        _ => Ok(()),
    }
}

const NO_LIMIT: u64 = u64::MAX;
const TRIP_NONE: u8 = 0;
const TRIP_DEADLINE: u8 = 1;
const TRIP_WORK: u8 = 2;

/// The deadline and work budget of the request an engine is running, in a
/// form functions can poll through `&self` from any evaluation thread.
///
/// The engine owns one for its lifetime and arms it for each request from the
/// request's resource ledger (`sync`), so the ledger stays the single source of
/// truth; this is its lock-free mirror. A trip is sticky until the next request
/// begins: the engine reads it at its commit boundaries to refuse the
/// interrupted result and to report the reason.
#[derive(Debug)]
pub(crate) struct RequestLimits {
    epoch: FzInstant,
    /// Nanoseconds after `epoch` at which the request must stop.
    deadline_at_ns: AtomicU64,
    /// The configured limit behind `deadline_at_ns`, for the error detail.
    deadline_limit_ns: AtomicU64,
    /// Work units the ledger had charged at the last sync.
    work_base: AtomicU64,
    work_limit: AtomicU64,
    /// Units functions have charged since the last sync.
    work_pending: AtomicU64,
    tripped: AtomicU8,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            epoch: FzInstant::now(),
            deadline_at_ns: AtomicU64::new(NO_LIMIT),
            deadline_limit_ns: AtomicU64::new(NO_LIMIT),
            work_base: AtomicU64::new(0),
            work_limit: AtomicU64::new(NO_LIMIT),
            work_pending: AtomicU64::new(0),
            tripped: AtomicU8::new(TRIP_NONE),
        }
    }
}

impl RequestLimits {
    fn now_ns(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(NO_LIMIT - 1)
    }

    /// Whether a deadline or a work limit is set, i.e. whether functions need
    /// the request-scoped token at all.
    pub(crate) fn armed(&self) -> bool {
        self.deadline_at_ns.load(Ordering::Relaxed) != NO_LIMIT
            || self.work_limit.load(Ordering::Relaxed) != NO_LIMIT
    }

    /// Starts a request: clears the previous request's trip and limits.
    pub(crate) fn begin_request(&self) {
        self.disarm();
        self.tripped.store(TRIP_NONE, Ordering::Relaxed);
    }

    /// Removes the limits (the trip, if any, stays readable).
    pub(crate) fn disarm(&self) {
        self.deadline_at_ns.store(NO_LIMIT, Ordering::Relaxed);
        self.work_limit.store(NO_LIMIT, Ordering::Relaxed);
        self.work_base.store(0, Ordering::Relaxed);
        self.work_pending.store(0, Ordering::Relaxed);
    }

    /// Mirrors the ledger's state: `remaining` time until the deadline (with
    /// the configured `limit`, for reporting), and the work charged so far
    /// against `work_limit`. Returns the units functions charged since the
    /// previous sync, which the caller charges to the ledger.
    pub(crate) fn sync(
        &self,
        deadline: Option<(Duration, Duration)>,
        work_charged: u64,
        work_limit: Option<u64>,
    ) -> u64 {
        match deadline {
            Some((remaining, limit)) => {
                let remaining = u64::try_from(remaining.as_nanos()).unwrap_or(NO_LIMIT - 1);
                self.deadline_limit_ns.store(
                    u64::try_from(limit.as_nanos()).unwrap_or(NO_LIMIT - 1),
                    Ordering::Relaxed,
                );
                self.deadline_at_ns.store(
                    self.now_ns().saturating_add(remaining).min(NO_LIMIT - 1),
                    Ordering::Relaxed,
                );
            }
            None => self.deadline_at_ns.store(NO_LIMIT, Ordering::Relaxed),
        }
        let pending = self.work_pending.swap(0, Ordering::Relaxed);
        self.work_base
            .store(work_charged.saturating_add(pending), Ordering::Relaxed);
        self.work_limit
            .store(work_limit.unwrap_or(NO_LIMIT), Ordering::Relaxed);
        pending
    }

    #[cold]
    fn trip(&self, reason: u8) {
        // The first reason wins.
        let _ =
            self.tripped
                .compare_exchange(TRIP_NONE, reason, Ordering::Relaxed, Ordering::Relaxed);
    }

    fn exceeded(&self) -> bool {
        if self.tripped.load(Ordering::Relaxed) != TRIP_NONE {
            return true;
        }
        let deadline = self.deadline_at_ns.load(Ordering::Relaxed);
        if deadline != NO_LIMIT && self.now_ns() >= deadline {
            self.trip(TRIP_DEADLINE);
            return true;
        }
        false
    }

    fn charge(&self, units: u64) -> bool {
        if units > 0 {
            let pending = self
                .work_pending
                .fetch_add(units, Ordering::Relaxed)
                .saturating_add(units);
            let limit = self.work_limit.load(Ordering::Relaxed);
            if limit != NO_LIMIT
                && self
                    .work_base
                    .load(Ordering::Relaxed)
                    .saturating_add(pending)
                    > limit
            {
                self.trip(TRIP_WORK);
                return true;
            }
        }
        self.exceeded()
    }

    /// The limit a function ran into during this request, as
    /// `(reason, limit, observed)`.
    pub(crate) fn tripped(&self) -> Option<(ResourceExhaustionReason, u64, u64)> {
        match self.tripped.load(Ordering::Relaxed) {
            TRIP_DEADLINE => {
                let limit = self.deadline_limit_ns.load(Ordering::Relaxed);
                let at = self.deadline_at_ns.load(Ordering::Relaxed);
                let over = if at == NO_LIMIT {
                    0
                } else {
                    self.now_ns().saturating_sub(at)
                };
                Some((
                    ResourceExhaustionReason::Deadline,
                    limit,
                    limit.saturating_add(over),
                ))
            }
            TRIP_WORK => Some((
                ResourceExhaustionReason::WorkUnits,
                self.work_limit.load(Ordering::Relaxed),
                self.work_base
                    .load(Ordering::Relaxed)
                    .saturating_add(self.work_pending.load(Ordering::Relaxed)),
            )),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_one_signal() {
        let token = CancelToken::new();
        let clone = token.clone();
        assert!(!token.is_cancelled());
        clone.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn adopting_a_flag_observes_external_cancellation() {
        let flag = Arc::new(AtomicBool::new(false));
        let token = CancelToken::from_flag(Arc::clone(&flag));
        assert!(!token.is_cancelled());
        flag.store(true, Ordering::Relaxed);
        assert!(token.is_cancelled());
    }

    #[test]
    fn default_token_is_not_cancelled() {
        assert!(!CancelToken::default().is_cancelled());
    }

    #[test]
    fn request_scoped_token_reports_deadline_and_work_without_touching_the_caller_flag() {
        let limits = Arc::new(RequestLimits::default());
        limits.begin_request();
        let caller = CancelToken::new();
        let scoped = CancelToken::request_scoped(Some(&caller), Arc::clone(&limits));
        assert!(!scoped.is_cancelled());
        assert!(limits.tripped().is_none());
        assert!(!limits.armed());

        // Work: 10 charged of a limit of 16, then 5 + 2 more.
        assert_eq!(limits.sync(None, 10, Some(16)), 0);
        assert!(limits.armed());
        assert!(!scoped.charge_work(5));
        assert!(scoped.charge_work(2));
        assert!(scoped.is_cancelled(), "a trip is sticky");
        assert!(
            !caller.is_cancelled(),
            "the caller's token is not cancelled"
        );
        let (reason, limit, observed) = limits.tripped().unwrap();
        assert_eq!(reason, ResourceExhaustionReason::WorkUnits);
        assert_eq!((limit, observed), (16, 17));
        // The ledger takes the pending units at the next sync.
        assert_eq!(limits.sync(None, 10, Some(16)), 7);

        // A new request clears the trip; an elapsed deadline trips it again.
        limits.begin_request();
        assert!(!scoped.is_cancelled());
        limits.sync(Some((Duration::ZERO, Duration::from_secs(2))), 0, None);
        assert!(scoped.is_cancelled());
        let (reason, limit, observed) = limits.tripped().unwrap();
        assert_eq!(reason, ResourceExhaustionReason::Deadline);
        assert_eq!(limit, 2_000_000_000);
        assert!(observed >= limit);

        // The caller's own cancellation is seen through the scoped token and
        // is not a limit trip.
        limits.begin_request();
        assert!(!scoped.is_cancelled());
        caller.cancel();
        assert!(scoped.is_cancelled());
        assert!(checkpoint(Some(&scoped), 1).is_err());
        assert!(limits.tripped().is_none());
    }
}
