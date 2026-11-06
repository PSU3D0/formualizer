//! Deferred host replay calls are caught inside their spool mutex.
//! Engine checkpoints resume their original unwind; they are not host failures.
use crate::engine::{
    DeferredFormulaReplay, DeferredReplayFormula, FormulaReplayDisposition,
    PartitionedSourceFormulaFamily,
};
use formualizer_common::ExcelError;
use std::{
    cell::Cell,
    sync::{Arc, Mutex, Weak, atomic::AtomicU64},
};
struct InternalCheckpoint<'a>(&'a Cell<bool>);
impl Drop for InternalCheckpoint<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.set(true);
        }
    }
}
fn run<T>(name: &str, internal: &Cell<bool>, call: impl FnOnce() -> T) -> Result<T, ExcelError> {
    crate::function::catch_external_panic(call).map_err(|payload| {
        if internal.get() {
            std::panic::resume_unwind(payload);
        }
        let error = ExcelError::from_function_panic(name, &*payload);
        crate::external_callbacks::clock_failure(error.clone());
        error
    })
}
pub(crate) struct GuardedReplay(Box<dyn DeferredFormulaReplay>);
impl GuardedReplay {
    pub(crate) fn boxed(inner: Box<dyn DeferredFormulaReplay>) -> Box<dyn DeferredFormulaReplay> {
        Box::new(Self(inner))
    }
    pub(crate) fn shared(replay: &Arc<Mutex<Box<dyn DeferredFormulaReplay>>>) {
        if let Ok(mut guard) = replay.lock() {
            let inner = std::mem::replace(&mut *guard, Box::new(EmptyReplay));
            *guard = Self::boxed(inner);
        }
    }
}
impl DeferredFormulaReplay for GuardedReplay {
    fn selection_cache_footprint(&self) -> Option<Weak<AtomicU64>> {
        run(
            "resolver:replay.selection_cache_footprint",
            &Cell::new(false),
            || self.0.selection_cache_footprint(),
        )
        .unwrap_or(None)
    }
    fn replay(
        &mut self,
        d: &FormulaReplayDisposition,
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        run("resolver:replay.replay", &Cell::new(false), || {
            self.0.replay(d)
        })
        .map_err(|e| e.to_string())?
    }
    fn replay_partitioned(
        &mut self,
        d: &FormulaReplayDisposition,
        p: &[PartitionedSourceFormulaFamily],
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        run(
            "resolver:replay.replay_partitioned",
            &Cell::new(false),
            || self.0.replay_partitioned(d, p),
        )
        .map_err(|e| e.to_string())?
    }
    fn replay_selected_ordinary(
        &mut self,
        c: &[(u32, u32)],
        checkpoint: &mut dyn FnMut(u64, u64) -> Result<(), ExcelError>,
    ) -> Result<Option<Vec<DeferredReplayFormula>>, ExcelError> {
        let internal = Cell::new(false);
        run(
            "resolver:replay.replay_selected_ordinary",
            &internal,
            || {
                self.0.replay_selected_ordinary(c, &mut |w, b| {
                    let _internal = InternalCheckpoint(&internal);
                    checkpoint(w, b)
                })
            },
        )?
    }
    fn replay_selected_exact(
        &mut self,
        c: &[(u32, u32)],
        checkpoint: &mut dyn FnMut(u64, u64) -> Result<(), ExcelError>,
    ) -> Result<Option<Vec<DeferredReplayFormula>>, ExcelError> {
        let internal = Cell::new(false);
        run("resolver:replay.replay_selected_exact", &internal, || {
            self.0.replay_selected_exact(c, &mut |w, b| {
                let _internal = InternalCheckpoint(&internal);
                checkpoint(w, b)
            })
        })?
    }
    fn formula_at(&mut self, row: u32, col: u32) -> Result<Option<DeferredReplayFormula>, String> {
        run("resolver:replay.formula_at", &Cell::new(false), || {
            self.0.formula_at(row, col)
        })
        .map_err(|e| e.to_string())?
    }
}
struct EmptyReplay;
impl DeferredFormulaReplay for EmptyReplay {
    fn replay(
        &mut self,
        _: &FormulaReplayDisposition,
    ) -> Result<Vec<DeferredReplayFormula>, String> {
        Ok(Vec::new())
    }
    fn formula_at(&mut self, _: u32, _: u32) -> Result<Option<DeferredReplayFormula>, String> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Fault;
    impl DeferredFormulaReplay for Fault {
        fn replay(
            &mut self,
            _: &FormulaReplayDisposition,
        ) -> Result<Vec<DeferredReplayFormula>, String> {
            panic!("host replay panic")
        }
        fn formula_at(&mut self, _: u32, _: u32) -> Result<Option<DeferredReplayFormula>, String> {
            panic!("host locator panic")
        }
        fn replay_selected_exact(
            &mut self,
            _: &[(u32, u32)],
            check: &mut dyn FnMut(u64, u64) -> Result<(), ExcelError>,
        ) -> Result<Option<Vec<DeferredReplayFormula>>, ExcelError> {
            check(0, 0)?;
            Ok(None)
        }
    }
    #[test]
    fn host_replay_panic_keeps_mutex_usable_and_internal_checkpoint_unwinds() {
        let spool = Mutex::new(GuardedReplay::boxed(Box::new(Fault)));
        assert!(spool.lock().unwrap().replay(&Default::default()).is_err());
        assert!(!spool.is_poisoned());
        assert!(spool.lock().unwrap().formula_at(1, 1).is_err());
        let mut replay = GuardedReplay::boxed(Box::new(Fault));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            replay
                .replay_selected_exact(&[(1, 1)], &mut |_, _| panic!("engine checkpoint panic"))
                .unwrap();
        }))
        .unwrap_err();
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"engine checkpoint panic")
        );
    }
}
