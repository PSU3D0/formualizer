//! Opt-in evidence of unavailable operations encountered in the current request.
//!
//! These events do not certify coverage of retained results. Roots describe logical
//! cell/reason incidence, not affected-output closure or final Excel error cells.
//! Only exceptional roots are retained; successful cells have no diagnostic payload.
use crate::{CellRef, traits::CalcValue};
use formualizer_common::{ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue};
use std::{cell::RefCell, collections::HashSet, marker::PhantomData, rc::Rc, sync::Mutex};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum UnavailableOperation {
    UnknownFunction { name: String },
    UnimplementedFunction { name: String },
    PostfixInvocation,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OperationRoot {
    pub cell: Option<CellRef>,
    /// Named-formula roots identify the symbol vertex, never a fabricated grid cell.
    pub named_vertex: Option<crate::engine::VertexId>,
    pub operation: UnavailableOperation,
}

/// The report intentionally has no retained-result-coverage variant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OperationDiagnosticScope {
    #[default]
    RequestEvents,
}

/// Request-event evidence only. Empty roots say nothing about retained caches.
#[derive(Clone, Debug, Default)]
pub struct OperationDiagnostics {
    pub scope: OperationDiagnosticScope,
    pub logical_cell_reason_count: usize,
    pub logical_root_cell_count: usize,
    pub logical_named_reason_count: usize,
    /// Actual root observations, including optimizer speculation; excludes debug oracles.
    pub physical_unavailable_observations: u64,
    /// Interpreter function dispatches, including speculation; excludes debug oracles.
    /// Unknown-name lookups and unsupported postfix nodes do not dispatch providers.
    pub physical_function_dispatches: u64,
    /// Complete sparse roots for conservative source closure. Not sample-limited.
    pub roots: Vec<OperationRoot>,
    pub samples: Vec<OperationRoot>,
}

#[derive(Default)]
struct State {
    roots: HashSet<OperationRoot>,
    physical: u64,
}

/// Collector shared by interpreters and worker threads in one engine request.
pub struct OperationDiagnosticCollector {
    sample_limit: usize,
    state: Mutex<State>,
    dispatches: std::sync::atomic::AtomicU64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Inherit,
    Speculative,
    Suppressed,
}

struct Frame {
    sink: usize,
    mode: Mode,
    events: u64,
    tainted: bool,
    named_vertex: Option<crate::engine::VertexId>,
}
thread_local! { static FRAMES: RefCell<Vec<Frame>> = const { RefCell::new(Vec::new()) }; }

pub(crate) struct Scope<'a> {
    collector: &'a OperationDiagnosticCollector,
    index: usize,
    _not_send: PhantomData<Rc<()>>,
}

impl Scope<'_> {
    pub(crate) fn encountered(&self) -> bool {
        FRAMES.with(|frames| frames.borrow()[self.index].events != 0)
    }
    pub(crate) fn tainted(&self) -> bool {
        FRAMES.with(|frames| {
            let frames = frames.borrow();
            let frame = &frames[self.index];
            frame.tainted || frame.events != 0
        })
    }
}
impl Drop for Scope<'_> {
    fn drop(&mut self) {
        FRAMES.with(|frames| {
            let mut frames = frames.borrow_mut();
            debug_assert_eq!(frames.len(), self.index + 1);
            debug_assert_eq!(frames[self.index].sink, self.collector.key());
            frames.pop();
        });
    }
}

impl OperationDiagnosticCollector {
    pub fn new(sample_limit: usize) -> Self {
        Self {
            sample_limit,
            state: Mutex::new(State::default()),
            dispatches: std::sync::atomic::AtomicU64::new(0),
        }
    }
    fn key(&self) -> usize {
        self as *const Self as usize
    }
    pub(crate) fn scope(&self, mode: Mode) -> Scope<'_> {
        let index = FRAMES.with(|frames| {
            let mut frames = frames.borrow_mut();
            let index = frames.len();
            frames.push(Frame {
                sink: self.key(),
                mode,
                events: 0,
                tainted: false,
                named_vertex: None,
            });
            index
        });
        Scope {
            collector: self,
            index,
            _not_send: PhantomData,
        }
    }
    pub(crate) fn named_scope(&self, vertex: crate::engine::VertexId) -> Scope<'_> {
        let scope = self.scope(Mode::Inherit);
        FRAMES.with(|frames| frames.borrow_mut()[scope.index].named_vertex = Some(vertex));
        scope
    }
    pub(crate) fn reset(&self) {
        *self.state.lock().unwrap() = State::default();
        self.dispatches
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn dispatch_started(&self) {
        let suppressed = FRAMES.with(|frames| {
            frames
                .borrow()
                .iter()
                .rev()
                .find(|f| f.sink == self.key() && f.mode != Mode::Inherit)
                .is_some_and(|f| f.mode == Mode::Suppressed)
        });
        if !suppressed {
            self.dispatches
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Explicit provider seam: call only on the executed unsupported path.
    /// Propagating an argument's error is not a new unavailable operation.
    pub fn record(&self, cell: Option<CellRef>, operation: UnavailableOperation) {
        let mode = FRAMES.with(|frames| {
            frames
                .borrow()
                .iter()
                .rev()
                .find(|f| f.sink == self.key() && f.mode != Mode::Inherit)
                .map_or(Mode::Inherit, |f| f.mode)
        });
        if mode == Mode::Suppressed {
            return;
        }
        FRAMES.with(|frames| {
            for frame in frames
                .borrow_mut()
                .iter_mut()
                .filter(|f| f.sink == self.key())
            {
                frame.events += 1;
            }
        });
        let mut state = self.state.lock().unwrap();
        state.physical += 1;
        if mode != Mode::Speculative {
            let named_vertex = FRAMES.with(|frames| {
                frames
                    .borrow()
                    .iter()
                    .rev()
                    .filter(|f| f.sink == self.key())
                    .find_map(|f| f.named_vertex)
            });
            state.roots.insert(OperationRoot {
                cell: if named_vertex.is_some() { None } else { cell },
                named_vertex,
                operation,
            });
        }
    }

    // An unavailable input (or a range with unknown provenance) must not be
    // relabelled as a new function root when a wrapper propagates #N/IMPL!.
    pub(crate) fn input(&self, value: &Result<CalcValue<'_>, ExcelError>) {
        match value {
            Ok(value) => self.input_value(value),
            Err(error) => self.input_error(error),
        }
    }
    pub(crate) fn input_value(&self, value: &CalcValue<'_>) {
        let tainted = match value {
            CalcValue::Scalar(v) | CalcValue::AnnotatedScalar(v, _) => literal_has_nimpl(v),
            CalcValue::Range(_) => true,
            _ => false,
        };
        if tainted {
            self.ambiguous_input();
        }
    }
    pub(crate) fn input_error(&self, error: &ExcelError) {
        if error.kind == ExcelErrorKind::NImpl {
            self.ambiguous_input();
        }
    }
    pub(crate) fn ambiguous_input(&self) {
        FRAMES.with(|frames| {
            for frame in frames
                .borrow_mut()
                .iter_mut()
                .filter(|f| f.sink == self.key())
            {
                frame.tainted = true;
            }
        });
    }
    pub fn snapshot(&self) -> OperationDiagnostics {
        let state = self.state.lock().unwrap();
        let mut roots: Vec<_> = state.roots.iter().cloned().collect();
        roots.sort_unstable();
        let cells: HashSet<_> = roots.iter().filter_map(|r| r.cell).collect();
        OperationDiagnostics {
            scope: OperationDiagnosticScope::RequestEvents,
            logical_cell_reason_count: roots.iter().filter(|r| r.cell.is_some()).count(),
            logical_named_reason_count: roots.iter().filter(|r| r.named_vertex.is_some()).count(),
            logical_root_cell_count: cells.len(),
            physical_unavailable_observations: state.physical,
            physical_function_dispatches: self
                .dispatches
                .load(std::sync::atomic::Ordering::Relaxed),
            samples: roots.iter().take(self.sample_limit).cloned().collect(),
            roots,
        }
    }
}

fn literal_has_nimpl(value: &LiteralValue) -> bool {
    match value {
        LiteralValue::Error(e) => e.kind == ExcelErrorKind::NImpl,
        // Arrays, like ranges, have opaque provenance. Do not scan a dense
        // successful payload merely to decide whether an outer error is a root.
        LiteralValue::Array(_) => true,
        _ => false,
    }
}

pub(crate) fn is_unimplemented(value: &Result<CalcValue<'_>, ExcelError>) -> bool {
    let error = match value {
        Err(e) => Some(e),
        Ok(
            CalcValue::Scalar(LiteralValue::Error(e))
            | CalcValue::AnnotatedScalar(LiteralValue::Error(e), _),
        ) => Some(e),
        _ => None,
    };
    error.is_some_and(|e| {
        e.kind == ExcelErrorKind::NImpl && matches!(e.extra(), ExcelErrorExtra::None)
    })
}
