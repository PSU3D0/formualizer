//! Opt-in request evidence of unavailable or unverified executed operations.
//!
//! Roots describe logical cell/reason incidence, not retained-result coverage,
//! affected output closure, or final Excel errors. Display sampling never
//! truncates the exact sparse evidence. Physical telemetry is independently opt-in.
mod scopes;
use crate::{CellRef, traits::CalcValue};
use formualizer_common::{
    ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue, ResourceExhaustionDetail,
    ResourceExhaustionReason,
};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum UnavailableOperation {
    /// Executed function code unwound instead of producing a result.
    FunctionPanicked {
        name: String,
    },
    UnknownFunction {
        name: String,
    },
    UnimplementedFunction {
        name: String,
    },
    UnimplementedFunctionMode {
        name: String,
        mode: &'static str,
    },
    /// The executed mode has no established support result. It is not a
    /// claim that the input is invalid or a known valid unsupported Excel mode.
    UnverifiedFunctionMode {
        name: String,
        mode: &'static str,
    },
    UnverifiedProvider {
        name: String,
    },
    PostfixInvocation,
}
impl UnavailableOperation {
    fn owned_text_bytes(&self) -> usize {
        match self {
            Self::FunctionPanicked { name }
            | Self::UnknownFunction { name }
            | Self::UnimplementedFunction { name }
            | Self::UnimplementedFunctionMode { name, .. }
            | Self::UnverifiedFunctionMode { name, .. }
            | Self::UnverifiedProvider { name } => name.capacity(),
            Self::PostfixInvocation => 0,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OperationRoot {
    pub cell: Option<CellRef>,
    /// Named-formula roots identify their symbol, never a fabricated grid cell.
    pub named_vertex: Option<crate::engine::VertexId>,
    /// Shared once per distinct operation, including through snapshots/samples.
    pub operation: Arc<UnavailableOperation>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OperationDiagnosticScope {
    #[default]
    RequestEvents,
}
#[derive(Clone, Debug, Default)]
pub struct OperationDiagnostics {
    pub scope: OperationDiagnosticScope,
    pub logical_cell_reason_count: usize,
    pub logical_root_cell_count: usize,
    pub logical_named_reason_count: usize,
    /// None means unmeasured; Some(0) means measured zero.
    pub physical_unavailable_observations: Option<u64>,
    pub physical_function_dispatches: Option<u64>,
    pub roots: Vec<OperationRoot>,
    pub samples: Vec<OperationRoot>,
}
/// Configured root/text admission limits and fallible collection reservations
/// return sticky typed resource errors. This is not universal OOM recovery:
/// String/Arc, TLS and error construction retain Rust's process-fatal allocator
/// failure behavior. These limits do not promise a total RSS budget.
#[derive(Clone, Copy, Debug)]
pub struct OperationDiagnosticOptions {
    pub sample_limit: usize,
    pub physical_telemetry: bool,
    /// Include uncertainty when the executing provider has no reporting contract.
    pub track_unverified_providers: bool,
    pub max_roots: Option<usize>,
    /// Owned String capacity, counted once per distinct operation.
    /// This is not a total RSS budget.
    pub max_operation_text_bytes: Option<usize>,
}
impl Default for OperationDiagnosticOptions {
    fn default() -> Self {
        Self {
            sample_limit: 16,
            physical_telemetry: false,
            track_unverified_providers: false,
            max_roots: None,
            max_operation_text_bytes: None,
        }
    }
}
#[derive(Default)]
struct State {
    roots: HashSet<OperationRoot>,
    operations: HashSet<Arc<UnavailableOperation>>,
    operation_text_bytes: usize,
    physical: u64,
    exhaustion: Option<ResourceExhaustionDetail>,
    finalized: bool,
}
pub struct OperationDiagnosticCollector {
    options: OperationDiagnosticOptions,
    state: Mutex<State>,
    dispatches: std::sync::atomic::AtomicU64,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Inherit,
    Speculative,
    Suppressed,
}
pub(crate) struct Scope<'a> {
    _collector: &'a OperationDiagnosticCollector,
    guard: scopes::Guard,
}
impl Scope<'_> {
    pub(crate) fn encountered(&self) -> bool {
        self.guard.encountered()
    }
    pub(crate) fn tainted(&self) -> bool {
        self.guard.tainted()
    }
}
fn resource_error(detail: ResourceExhaustionDetail) -> ExcelError {
    crate::engine::ResourceLedgerError::Exhausted(detail).into_excel_error()
}
fn state_error() -> ExcelError {
    ExcelError::new(ExcelErrorKind::Calc)
        .with_message("operation evidence has already been finalized")
}
fn allocation_detail(observed: usize) -> ResourceExhaustionDetail {
    ResourceExhaustionDetail {
        reason: ResourceExhaustionReason::ScratchMemory,
        limit: u64::MAX,
        observed: observed as u64,
        request_id: None,
    }
}
impl OperationDiagnosticCollector {
    /// Explicit historical diagnostic callers request physical telemetry.
    pub fn new(sample_limit: usize) -> Self {
        Self::with_options(OperationDiagnosticOptions {
            sample_limit,
            physical_telemetry: true,
            ..Default::default()
        })
    }
    pub fn with_options(options: OperationDiagnosticOptions) -> Self {
        Self {
            options,
            state: Mutex::new(State::default()),
            dispatches: std::sync::atomic::AtomicU64::new(0),
        }
    }
    fn key(&self) -> usize {
        self as *const Self as usize
    }
    pub(crate) fn scope(&self, mode: Mode) -> Scope<'_> {
        Scope {
            _collector: self,
            guard: scopes::enter(self.key(), mode, scopes::Origin::default()),
        }
    }
    pub(crate) fn named_scope(&self, vertex: crate::engine::VertexId) -> Scope<'_> {
        Scope {
            _collector: self,
            guard: scopes::enter(
                self.key(),
                Mode::Inherit,
                scopes::Origin {
                    named_vertex: Some(vertex),
                    reference_cell: None,
                },
            ),
        }
    }
    pub(crate) fn reference_scope(&self, cell: Option<CellRef>) -> Scope<'_> {
        Scope {
            _collector: self,
            guard: scopes::enter(
                self.key(),
                Mode::Inherit,
                scopes::Origin {
                    named_vertex: None,
                    reference_cell: cell,
                },
            ),
        }
    }
    pub(crate) fn reset(&self) {
        *self.state.lock().unwrap() = State::default();
        self.dispatches
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }
    pub(crate) fn tracks_unverified_providers(&self) -> bool {
        self.options.track_unverified_providers
    }
    pub(crate) fn dispatch_started(&self) {
        if !self.options.physical_telemetry {
            return;
        }
        if scopes::current(self.key()).0 != Mode::Suppressed {
            self.dispatches
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    /// Call only at the actually executed limitation. Propagating a spreadsheet
    /// error is not a new operation. Resource exhaustion remains sticky.
    pub fn record(&self, cell: Option<CellRef>, operation: UnavailableOperation) {
        let Some((mode, origin)) = scopes::event(self.key()) else {
            return;
        };
        let mut state = self.state.lock().unwrap();
        if self.options.physical_telemetry {
            state.physical = state.physical.saturating_add(1);
        }
        if mode == Mode::Speculative || state.exhaustion.is_some() || state.finalized {
            return;
        }
        let named_vertex = origin.named_vertex;
        let cell = if named_vertex.is_some() {
            None
        } else {
            cell.or(origin.reference_cell).map(|cell| {
                CellRef::new_absolute(cell.sheet_id, cell.coord.row(), cell.coord.col())
            })
        };
        let existing = state.operations.get(&operation).cloned();
        if let Some(operation) = &existing {
            let root = OperationRoot {
                cell,
                named_vertex,
                operation: Arc::clone(operation),
            };
            if state.roots.contains(&root) {
                return;
            }
        }
        if let Some(limit) = self.options.max_roots
            && state.roots.len() >= limit
        {
            state.exhaustion = Some(ResourceExhaustionDetail {
                reason: ResourceExhaustionReason::OperationEvidenceRoots,
                limit: limit as u64,
                observed: state.roots.len().saturating_add(1) as u64,
                request_id: None,
            });
            return;
        }
        let added_text = if existing.is_some() {
            0
        } else {
            operation.owned_text_bytes()
        };
        let Some(next_text) = state.operation_text_bytes.checked_add(added_text) else {
            state.exhaustion = Some(ResourceExhaustionDetail {
                reason: ResourceExhaustionReason::ArithmeticOverflow,
                limit: usize::MAX as u64,
                observed: u64::MAX,
                request_id: None,
            });
            return;
        };
        if let Some(limit) = self.options.max_operation_text_bytes
            && next_text > limit
        {
            state.exhaustion = Some(ResourceExhaustionDetail {
                reason: ResourceExhaustionReason::OperationEvidenceTextBytes,
                limit: limit as u64,
                observed: next_text as u64,
                request_id: None,
            });
            return;
        }
        if state.roots.try_reserve(1).is_err()
            || (existing.is_none() && state.operations.try_reserve(1).is_err())
        {
            state.exhaustion = Some(allocation_detail(state.roots.len().saturating_add(1)));
            return;
        }
        let operation = existing.unwrap_or_else(|| {
            let operation = Arc::new(operation);
            state.operations.insert(Arc::clone(&operation));
            state.operation_text_bytes = next_text;
            operation
        });
        state.roots.insert(OperationRoot {
            cell,
            named_vertex,
            operation,
        });
    }
    pub(crate) fn input(&self, value: &Result<CalcValue<'_>, ExcelError>) {
        match value {
            Ok(value) => self.input_value(value),
            Err(error) => self.input_error(error),
        }
    }
    pub(crate) fn input_value(&self, value: &CalcValue<'_>) {
        let tainted = match value {
            CalcValue::Scalar(value) | CalcValue::AnnotatedScalar(value, _) => {
                literal_has_nimpl(value)
            }
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
        scopes::taint(self.key());
    }
    fn validate_state(state: &State) -> Result<(), ExcelError> {
        if let Some(detail) = &state.exhaustion {
            return Err(resource_error(detail.clone()));
        }
        if state.finalized {
            return Err(state_error());
        }
        Ok(())
    }
    fn report(
        &self,
        roots: impl Iterator<Item = OperationRoot>,
        count: usize,
        physical: u64,
    ) -> Result<OperationDiagnostics, ExcelError> {
        let mut roots_vec = Vec::new();
        roots_vec
            .try_reserve_exact(count)
            .map_err(|_| resource_error(allocation_detail(count)))?;
        roots_vec.extend(roots);
        roots_vec.sort_unstable();
        let mut previous = None;
        let mut cells = 0;
        let mut cell_reasons = 0;
        let mut named_reasons = 0;
        for root in &roots_vec {
            if let Some(cell) = root.cell {
                cell_reasons += 1;
                if previous != Some(cell) {
                    cells += 1;
                    previous = Some(cell);
                }
            }
            if root.named_vertex.is_some() {
                named_reasons += 1;
            }
        }
        let sample_count = self.options.sample_limit.min(roots_vec.len());
        let mut samples = Vec::new();
        samples
            .try_reserve_exact(sample_count)
            .map_err(|_| resource_error(allocation_detail(sample_count)))?;
        samples.extend(roots_vec.iter().take(sample_count).cloned());
        Ok(OperationDiagnostics {
            scope: OperationDiagnosticScope::RequestEvents,
            logical_cell_reason_count: cell_reasons,
            logical_root_cell_count: cells,
            logical_named_reason_count: named_reasons,
            physical_unavailable_observations: self.options.physical_telemetry.then_some(physical),
            physical_function_dispatches: self
                .options
                .physical_telemetry
                .then(|| self.dispatches.load(std::sync::atomic::Ordering::Relaxed)),
            roots: roots_vec,
            samples,
        })
    }
    pub fn snapshot(&self) -> Result<OperationDiagnostics, ExcelError> {
        let mut state = self.state.lock().unwrap();
        Self::validate_state(&state)?;
        let report = self.report(
            state.roots.iter().cloned(),
            state.roots.len(),
            state.physical,
        );
        if let Err(error) = &report
            && let ExcelErrorExtra::Resource { detail } = error.extra()
        {
            state.exhaustion = Some(detail.as_ref().clone());
        }
        report
    }
    /// Consume exact roots after the request's scopes/workers have finished.
    /// A second drain cannot look like clean empty evidence.
    pub fn take_snapshot(&mut self) -> Result<OperationDiagnostics, ExcelError> {
        self.take_shared_snapshot()
    }
    pub(crate) fn take_shared_snapshot(&self) -> Result<OperationDiagnostics, ExcelError> {
        let mut state = self.state.lock().unwrap();
        Self::validate_state(&state)?;
        let roots = std::mem::take(&mut state.roots);
        let report = self.report(roots.iter().cloned(), roots.len(), state.physical);
        if report.is_ok() {
            state.operations = HashSet::new();
            state.operation_text_bytes = 0;
            state.finalized = true;
        } else {
            // Restore exact roots so an allocation failure cannot erase evidence.
            state.roots = roots;
            if let Err(error) = &report
                && let ExcelErrorExtra::Resource { detail } = error.extra()
            {
                state.exhaustion = Some(detail.as_ref().clone());
            }
        }
        report
    }
}
fn literal_has_nimpl(value: &LiteralValue) -> bool {
    match value {
        LiteralValue::Error(error) => error.kind == ExcelErrorKind::NImpl,
        LiteralValue::Array(_) => true,
        _ => false,
    }
}
pub(crate) fn is_unimplemented(value: &Result<CalcValue<'_>, ExcelError>) -> bool {
    let error = match value {
        Err(error) => Some(error),
        Ok(
            CalcValue::Scalar(LiteralValue::Error(error))
            | CalcValue::AnnotatedScalar(LiteralValue::Error(error), _),
        ) => Some(error),
        _ => None,
    };
    error.is_some_and(|error| {
        error.kind == ExcelErrorKind::NImpl && matches!(error.extra(), ExcelErrorExtra::None)
    })
}

pub(crate) fn is_unimplemented_reference(
    result: &Option<Result<formualizer_parse::parser::ReferenceType, ExcelError>>,
) -> bool {
    result.as_ref().is_some_and(|result| {
        result.as_ref().is_err_and(|error| {
            error.kind == ExcelErrorKind::NImpl && matches!(error.extra(), ExcelErrorExtra::None)
        })
    })
}

pub(crate) fn is_unimplemented_resolution(
    result: &Result<crate::function::FunctionResolution<'_>, ExcelError>,
) -> bool {
    use crate::function::FunctionResolution;
    let error = match result {
        Err(error) | Ok(FunctionResolution::ReferenceError(error)) => Some(error),
        Ok(FunctionResolution::Value(
            CalcValue::Scalar(LiteralValue::Error(error))
            | CalcValue::AnnotatedScalar(LiteralValue::Error(error), _),
        )) => Some(error),
        _ => None,
    };
    error.is_some_and(|error| {
        error.kind == ExcelErrorKind::NImpl && matches!(error.extra(), ExcelErrorExtra::None)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::Coord;
    fn cell(row: u32) -> CellRef {
        CellRef::new(0, Coord::new(row, 0, true, true))
    }
    fn gap(name: &str) -> UnavailableOperation {
        UnavailableOperation::UnknownFunction { name: name.into() }
    }
    fn failure_reason(error: &ExcelError) -> ResourceExhaustionReason {
        match error.extra() {
            ExcelErrorExtra::Resource { detail } => detail.reason,
            _ => panic!("expected typed resource failure: {error:?}"),
        }
    }

    #[test]
    fn collector_cell_anchoring_flags_do_not_duplicate_logical_identity() {
        let collector = OperationDiagnosticCollector::with_options(OperationDiagnosticOptions {
            max_roots: Some(1),
            ..Default::default()
        });
        collector.record(Some(cell(0)), gap("GAP"));
        collector.record(
            Some(CellRef::new(0, Coord::new(0, 0, false, false))),
            gap("GAP"),
        );
        let report = collector.snapshot().unwrap();
        assert_eq!(report.logical_root_cell_count, 1);
        assert_eq!(report.logical_cell_reason_count, 1);
        assert_eq!(report.roots.len(), 1);
        assert_eq!(
            report.roots[0].cell.unwrap().coord,
            Coord::new(0, 0, true, true)
        );
    }

    #[test]
    fn collector_interns_operation_identity_across_exact_roots_and_samples() {
        let collector = OperationDiagnosticCollector::with_options(OperationDiagnosticOptions {
            sample_limit: 1,
            max_roots: Some(2),
            ..Default::default()
        });
        collector.record(Some(cell(0)), gap("GAP"));
        collector.record(Some(cell(1)), gap("GAP"));
        let report = collector.snapshot().unwrap();
        assert_eq!(report.roots.len(), 2);
        assert_eq!(report.logical_root_cell_count, 2);
        assert_eq!(report.samples.len(), 1);
        assert!(Arc::ptr_eq(
            &report.roots[0].operation,
            &report.roots[1].operation
        ));
        assert!(Arc::ptr_eq(
            &report.roots[0].operation,
            &report.samples[0].operation
        ));
        assert_eq!(report.physical_function_dispatches, None);
        assert_eq!(report.physical_unavailable_observations, None);
    }

    #[test]
    fn duplicate_root_at_limit_does_not_exhaust_exact_evidence() {
        let collector = OperationDiagnosticCollector::with_options(OperationDiagnosticOptions {
            sample_limit: 0,
            max_roots: Some(1),
            ..Default::default()
        });
        collector.record(Some(cell(0)), gap("GAP"));
        collector.record(Some(cell(0)), gap("GAP"));
        let report = collector.snapshot().unwrap();
        assert_eq!(report.roots.len(), 1);
        assert!(report.samples.is_empty());
        assert_eq!(report.logical_cell_reason_count, 1);
    }

    #[test]
    fn root_limit_failure_is_typed_and_sticky_until_next_request() {
        let mut collector =
            OperationDiagnosticCollector::with_options(OperationDiagnosticOptions {
                sample_limit: 0,
                max_roots: Some(1),
                ..Default::default()
            });
        collector.record(Some(cell(0)), gap("GAP"));
        collector.record(Some(cell(1)), gap("GAP"));
        for error in [
            collector.snapshot().unwrap_err(),
            collector.take_snapshot().unwrap_err(),
        ] {
            assert_eq!(
                failure_reason(&error),
                ResourceExhaustionReason::OperationEvidenceRoots
            );
        }
        collector.reset();
        assert!(collector.snapshot().unwrap().roots.is_empty());
    }

    #[test]
    fn operation_text_budget_counts_owned_capacity_only_once() {
        let mut collector =
            OperationDiagnosticCollector::with_options(OperationDiagnosticOptions {
                max_operation_text_bytes: Some(3),
                ..Default::default()
            });
        collector.record(Some(cell(0)), gap("GAP"));
        collector.record(Some(cell(1)), gap("GAP"));
        assert_eq!(collector.snapshot().unwrap().roots.len(), 2);
        collector.record(Some(cell(2)), gap("NEXT"));
        assert_eq!(
            failure_reason(&collector.take_snapshot().unwrap_err()),
            ResourceExhaustionReason::OperationEvidenceTextBytes,
        );
    }

    #[test]
    fn owned_capacity_not_just_string_length_is_bounded() {
        let collector = OperationDiagnosticCollector::with_options(OperationDiagnosticOptions {
            max_operation_text_bytes: Some(3),
            ..Default::default()
        });
        let mut name = String::with_capacity(100);
        name.push_str("GAP");
        collector.record(None, UnavailableOperation::UnknownFunction { name });
        assert_eq!(
            failure_reason(&collector.snapshot().unwrap_err()),
            ResourceExhaustionReason::OperationEvidenceTextBytes,
        );
    }

    #[test]
    fn speculative_and_suppressed_work_do_not_consume_sparse_root_budget() {
        let collector = OperationDiagnosticCollector::with_options(OperationDiagnosticOptions {
            physical_telemetry: true,
            max_roots: Some(0),
            max_operation_text_bytes: Some(0),
            ..Default::default()
        });
        {
            let _speculation = collector.scope(Mode::Speculative);
            collector.dispatch_started();
            collector.record(Some(cell(0)), gap("GAP"));
        }
        {
            let _oracle = collector.scope(Mode::Suppressed);
            let _nested = collector.scope(Mode::Speculative);
            collector.dispatch_started();
            collector.record(Some(cell(1)), gap("IGNORED"));
        }
        let report = collector.snapshot().unwrap();
        assert!(report.roots.is_empty());
        assert_eq!(report.physical_function_dispatches, Some(1));
        assert_eq!(report.physical_unavailable_observations, Some(1));
    }

    #[test]
    fn finalization_consumes_once_and_reset_preserves_configuration() {
        let mut collector =
            OperationDiagnosticCollector::with_options(OperationDiagnosticOptions {
                sample_limit: 0,
                physical_telemetry: true,
                ..Default::default()
            });
        collector.record(Some(cell(0)), gap("GAP"));
        let report = collector.take_snapshot().unwrap();
        assert_eq!(report.roots.len(), 1);
        assert!(collector.snapshot().is_err());
        assert!(collector.take_snapshot().is_err());
        collector.reset();
        let next = collector.take_snapshot().unwrap();
        assert!(next.roots.is_empty());
        assert_eq!(next.physical_function_dispatches, Some(0));
        assert_eq!(next.physical_unavailable_observations, Some(0));
    }
}
