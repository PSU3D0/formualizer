//! Permanent fail-closed marker. No recovery or mutation rollback is attempted.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
pub(crate) const POISON_MESSAGE: &str =
    "formualizer engine is poisoned by an earlier internal panic; discard it";

pub(super) struct UnwindPoisonGuard(pub(super) Arc<AtomicBool>);
impl Drop for UnwindPoisonGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}
