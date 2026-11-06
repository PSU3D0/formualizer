//! Narrow host callback catches and request-local metadata failures.
use crate::{
    CellRef,
    function::Function,
    operation_diagnostics::{OperationDiagnosticCollector, UnavailableOperation},
};
use formualizer_common::{ExcelError, ExcelErrorExtra};
use rustc_hash::FxHashMap;
use std::{
    cell::RefCell,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
pub(crate) struct CallbackState {
    metadata: Mutex<FxHashMap<usize, ExcelError>>,
    failed: AtomicBool,
    named: Mutex<FxHashMap<String, ExcelError>>,
    pending: Mutex<FxHashMap<Option<CellRef>, ExcelError>>,
    collector: Mutex<Option<Weak<OperationDiagnosticCollector>>>,
}
thread_local! {
    static ACTIVE: RefCell<Option<Arc<CallbackState>>> = const { RefCell::new(None) };
    static ROOT: RefCell<Option<CellRef>> = const { RefCell::new(None) };
}
pub(crate) struct RequestScope {
    previous: Option<Arc<CallbackState>>,
    previous_root: Option<CellRef>,
    changed: bool,
}
impl Drop for RequestScope {
    fn drop(&mut self) {
        if self.changed {
            ACTIVE.with(|a| *a.borrow_mut() = self.previous.take());
            ROOT.with(|r| *r.borrow_mut() = self.previous_root);
        }
    }
}
pub(crate) fn enter(state: Arc<CallbackState>) -> RequestScope {
    RequestScope {
        previous: ACTIVE.with(|a| a.replace(Some(state))),
        previous_root: ROOT.with(|r| r.replace(None)),
        changed: true,
    }
}
pub(crate) fn enter_if_missing(state: &Arc<CallbackState>) -> RequestScope {
    let same = ACTIVE.with(|a| {
        a.borrow()
            .as_ref()
            .is_some_and(|old| Arc::ptr_eq(old, state))
    });
    if same {
        RequestScope {
            previous: None,
            previous_root: None,
            changed: false,
        }
    } else {
        enter(state.clone())
    }
}
pub(crate) struct RootScope {
    previous: Option<CellRef>,
    changed: bool,
}
impl Drop for RootScope {
    fn drop(&mut self) {
        if self.changed {
            ROOT.with(|r| *r.borrow_mut() = self.previous);
        }
    }
}
pub(crate) fn root(cell: Option<CellRef>) -> RootScope {
    ROOT.with(|r| {
        if *r.borrow() == cell {
            RootScope {
                previous: None,
                changed: false,
            }
        } else {
            RootScope {
                previous: r.replace(cell),
                changed: true,
            }
        }
    })
}
fn active() -> Option<Arc<CallbackState>> {
    ACTIVE.with(|a| a.borrow().clone())
}
fn key<F: Function + ?Sized>(f: &F) -> usize {
    f as *const F as *const () as usize
}
impl CallbackState {
    pub(crate) fn begin(&self, collector: Option<&Arc<OperationDiagnosticCollector>>, reset: bool) {
        if reset {
            self.metadata.lock().unwrap().clear();
            self.named.lock().unwrap().clear();
            self.pending.lock().unwrap().clear();
            self.failed.store(false, Ordering::Release);
        }
        *self.collector.lock().unwrap() = collector.map(Arc::downgrade);
    }
    pub(crate) fn provider_failed(&self, name: &str, error: &ExcelError) {
        self.named
            .lock()
            .unwrap()
            .insert(name.into(), error.clone());
        self.failed.store(true, Ordering::Release);
    }
    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
    fn report(&self, error: &ExcelError) {
        if let ExcelErrorExtra::FunctionPanicked { function, .. } = error.extra()
            && let Some(collector) = self
                .collector
                .lock()
                .unwrap()
                .as_ref()
                .and_then(Weak::upgrade)
        {
            collector.record(
                ROOT.with(|r| *r.borrow()),
                UnavailableOperation::FunctionPanicked {
                    name: function.clone(),
                },
            );
        }
    }
    pub(crate) fn pending(&self) -> Option<ExcelError> {
        if !self.failed() {
            return None;
        }
        let root = ROOT.with(|r| *r.borrow());
        let mut pending = self.pending.lock().unwrap();
        // A global planning/provider failure affects every executed root.
        pending
            .get(&None)
            .cloned()
            .or_else(|| pending.remove(&root))
    }
}
#[derive(Clone, Copy)]
pub(crate) struct Label<'a>(pub(crate) &'static str, pub(crate) Option<&'a str>);
impl Label<'_> {
    fn text(self) -> String {
        match self.1 {
            Some(source) => format!("resolver:{}:{}", self.0, source),
            None => format!("resolver:{}", self.0),
        }
    }
}
/// Catch only the invocation, before returning through any engine-owned lock.
pub(crate) fn callback<T>(
    state: &CallbackState,
    label: Label<'_>,
    run: impl FnOnce() -> T,
) -> Result<T, ExcelError> {
    if let Some(error) = state.pending() {
        state.report(&error);
        return Err(error);
    }
    if state.failed()
        && let Some(source) = label.1
        && let Some(error) = state.named.lock().unwrap().get(source).cloned()
    {
        return Err(error);
    }
    crate::function::catch_external_panic(run).map_err(|payload| {
        let error = ExcelError::from_function_panic(&label.text(), &*payload);
        if ROOT.with(|r| r.borrow().is_some()) {
            state.report(&error);
        }
        error
    })
}
pub(crate) fn optional_callback<T>(
    state: &CallbackState,
    label: Label<'_>,
    fallback: impl FnOnce() -> T,
    run: impl FnOnce() -> T,
) -> T {
    match callback(state, label, run) {
        Ok(value) => value,
        Err(error) => {
            state.failed.store(true, Ordering::Release);
            if ROOT.with(|r| r.borrow().is_none())
                && let Some(source) = label.1
            {
                state.named.lock().unwrap().insert(source.into(), error);
            } else {
                state
                    .pending
                    .lock()
                    .unwrap()
                    .insert(ROOT.with(|r| *r.borrow()), error);
            }
            fallback()
        }
    }
}
pub(crate) fn named_error(name: &str) -> Option<ExcelError> {
    ACTIVE.with(|a| {
        let active = a.borrow();
        let state = active.as_ref()?;
        if !state.failed() {
            return None;
        }
        state.named.lock().unwrap().get(name).cloned()
    })
}
/// A metadata fault is sticky only within the current logical request.
pub(crate) fn metadata<T, F: Function + ?Sized>(
    function: &F,
    fallback: impl FnOnce() -> T,
    run: impl FnOnce() -> T,
) -> T {
    match inspect_metadata(function, run) {
        Ok(v) => v,
        Err(_) => fallback(),
    }
}
pub(crate) fn inspect_metadata<T, F: Function + ?Sized>(
    function: &F,
    run: impl FnOnce() -> T,
) -> Result<T, ExcelError> {
    let existing = ACTIVE.with(|a| {
        let active = a.borrow();
        let state = active.as_ref()?;
        if !state.failed() {
            return None;
        }
        state.metadata.lock().unwrap().get(&key(function)).cloned()
    });
    if let Some(error) = existing {
        return Err(error);
    }
    crate::function::catch_function_panic("function metadata", run).inspect_err(|error| {
        if let Some(state) = active() {
            state
                .metadata
                .lock()
                .unwrap()
                .insert(key(function), error.clone());
            state.failed.store(true, Ordering::Release);
        }
    })
}
pub(crate) fn metadata_error(function: &dyn Function, name: &str) -> Option<ExcelError> {
    ACTIVE.with(|a| {
        let active = a.borrow();
        let state = active.as_ref()?;
        if !state.failed() {
            return None;
        }
        let mut error = state.metadata.lock().unwrap().get(&key(function))?.clone();
        if let ExcelErrorExtra::FunctionPanicked { message, .. } = error.extra() {
            error = error.clone().with_extra(ExcelErrorExtra::FunctionPanicked {
                function: name.into(),
                message: message.clone(),
            });
        }
        Some(error)
    })
}
pub(crate) fn planning_failed() -> bool {
    ACTIVE.with(|a| a.borrow().as_ref().is_some_and(|state| state.failed()))
}
pub(crate) fn pending() -> Option<ExcelError> {
    ACTIVE.with(|a| {
        let active = a.borrow();
        let state = active.as_ref()?;
        let error = state.pending()?;
        state.report(&error);
        Some(error)
    })
}

/// A failed frozen sample is reported only by roots that actually read it.
pub(crate) fn clock_failure(error: ExcelError) {
    if let Some(state) = active() {
        state.failed.store(true, Ordering::Release);
        state.report(&error);
        state
            .pending
            .lock()
            .unwrap()
            .insert(ROOT.with(|r| *r.borrow()), error);
    }
}
