use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use formualizer_common::{
    ExcelError, ExcelErrorExtra, ExcelErrorKind, ResourceExhaustionDetail, ResourceExhaustionReason,
};

use crate::instant::FzInstant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvaluationIncompleteReason {
    FormulaPlaneTopologyCandidates,
    FormulaPlaneTopologyEdges,
    FormulaPlaneTopologyRetainedBytes,
    FormulaPlaneTopologyScratchBytes,
    FormulaPlaneTopologyAllocation,
    FormulaPlaneTopologySemanticStructural,
    DirtyClosureWork,
}

impl EvaluationIncompleteReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FormulaPlaneTopologyCandidates => "formula_plane_topology_candidates",
            Self::FormulaPlaneTopologyEdges => "formula_plane_topology_edges",
            Self::FormulaPlaneTopologyRetainedBytes => "formula_plane_topology_retained_bytes",
            Self::FormulaPlaneTopologyScratchBytes => "formula_plane_topology_scratch_bytes",
            Self::FormulaPlaneTopologyAllocation => "formula_plane_topology_allocation",
            Self::FormulaPlaneTopologySemanticStructural => {
                "formula_plane_topology_semantic_structural"
            }
            Self::DirtyClosureWork => "dirty_closure_work",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SemanticResourceBudget {
    pub max_rows: Option<u32>,
    pub max_columns: Option<u32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdmissionResourceBudget {
    /// Declarative in C1a. C2 activates this in one composed graph transaction.
    pub graph_vertex_hard_limit: Option<usize>,
    /// Declarative in C1a. C2 activates this in one composed graph transaction.
    pub graph_edge_hard_limit: Option<usize>,
    /// Declarative in C1a; existing workbook materialization guards remain authoritative.
    pub materialization_cells: Option<u64>,
    pub materialized_graph_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetainedResourceBudget {
    pub total_bytes: Option<u64>,
    pub mixed_cache_bytes: Option<u64>,
    pub lookup_cache_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScratchResourceBudget {
    pub total_bytes: Option<u64>,
    pub schedule_discovery_bytes: Option<u64>,
    pub graph_source_bytes: Option<u64>,
    pub spill_overlay_bytes: Option<u64>,
    /// Request scratch backing policy. Declarative until disk-backed scratch is activated.
    pub disk_scratch_policy: Option<DiskScratchPolicy>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkResourceBudget {
    pub max_work_units: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeadlineResourceBudget {
    pub max_elapsed: Option<Duration>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OptimizationResourceBudget {
    pub mixed_cache_candidates: Option<usize>,
    pub mixed_cache_edges: Option<usize>,
    pub max_threads: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EvaluationBudgets {
    pub semantic: SemanticResourceBudget,
    pub admission: AdmissionResourceBudget,
    pub retained: RetainedResourceBudget,
    pub scratch: ScratchResourceBudget,
    pub work: WorkResourceBudget,
    pub deadline: DeadlineResourceBudget,
    pub optimization: OptimizationResourceBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskScratchPolicy {
    NativeTemporary,
    MemoryOnly,
}

impl DiskScratchPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NativeTemporary => "native_temporary",
            Self::MemoryOnly => "memory_only",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceEnvelope {
    pub retained_bytes: u64,
    pub request_scratch_bytes: u64,
    pub materialized_graph_bytes: u64,
    pub max_work_units: u64,
    pub deadline: Option<Duration>,
    pub max_threads: usize,
    pub disk_scratch: DiskScratchPolicy,
}

impl ResourceEnvelope {
    /// Deterministically derive detailed budgets from this aggregate envelope.
    pub fn to_budgets(&self) -> EvaluationBudgets {
        let cache_pool = self.retained_bytes / 8;
        let mixed_cache = cache_pool.saturating_mul(60) / 100;
        let lookup_cache = cache_pool.saturating_sub(mixed_cache);
        let schedule = self.request_scratch_bytes / 2;
        let graph_source = self.request_scratch_bytes.saturating_mul(35) / 100;
        let spill_overlay = self
            .request_scratch_bytes
            .saturating_sub(schedule)
            .saturating_sub(graph_source);
        EvaluationBudgets {
            admission: AdmissionResourceBudget {
                materialized_graph_bytes: Some(self.materialized_graph_bytes),
                ..AdmissionResourceBudget::default()
            },
            retained: RetainedResourceBudget {
                total_bytes: Some(self.retained_bytes),
                mixed_cache_bytes: Some(mixed_cache),
                lookup_cache_bytes: Some(lookup_cache),
            },
            scratch: ScratchResourceBudget {
                total_bytes: Some(self.request_scratch_bytes),
                schedule_discovery_bytes: Some(schedule),
                graph_source_bytes: Some(graph_source),
                spill_overlay_bytes: Some(spill_overlay),
                disk_scratch_policy: Some(self.disk_scratch),
            },
            work: WorkResourceBudget {
                max_work_units: Some(self.max_work_units),
            },
            deadline: DeadlineResourceBudget {
                max_elapsed: self.deadline,
            },
            optimization: OptimizationResourceBudget {
                mixed_cache_candidates: Some(
                    usize::try_from(mixed_cache / 64).unwrap_or(usize::MAX),
                ),
                mixed_cache_edges: Some(usize::try_from(mixed_cache / 64).unwrap_or(usize::MAX)),
                max_threads: Some(self.max_threads),
            },
            ..EvaluationBudgets::default()
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LegacyResourceConfigDisposition {
    #[default]
    NotPresent,
    Mapped,
    IgnoredByExplicitBudget,
}

impl LegacyResourceConfigDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotPresent => "not_present",
            Self::Mapped => "mapped",
            Self::IgnoredByExplicitBudget => "ignored_by_explicit_budget",
        }
    }
}

/// Field-level disposition of deprecated resource configuration.
///
/// Every legacy value fills only its otherwise-unset destination budget field. An explicit
/// destination wins independently, so one `max_memory_mb` value can map to retained memory while
/// being ignored for scratch memory. Engines expose at most one diagnostic containing all fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationResourceConfigDiagnostic {
    pub max_vertices: LegacyResourceConfigDisposition,
    pub max_memory_mb_retained: LegacyResourceConfigDisposition,
    pub max_memory_mb_scratch: LegacyResourceConfigDisposition,
    pub max_eval_time: LegacyResourceConfigDisposition,
    /// C1a does not enforce graph caps in any mutation path; composed activation is C2.
    pub graph_admission_activation_deferred_to_c2: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedEvaluationBudgets {
    pub budgets: EvaluationBudgets,
    pub diagnostic: Option<EvaluationResourceConfigDiagnostic>,
}

pub(crate) fn split_legacy_memory_bytes(bytes: u64) -> (u64, u64) {
    let scratch = bytes / 2;
    (bytes.saturating_sub(scratch), scratch)
}

pub(crate) fn resolve_evaluation_budgets(
    explicit: &EvaluationBudgets,
    max_vertices: Option<usize>,
    max_memory_mb: Option<usize>,
    max_eval_time: Option<Duration>,
) -> ResolvedEvaluationBudgets {
    let legacy_present =
        max_vertices.is_some() || max_memory_mb.is_some() || max_eval_time.is_some();
    if !legacy_present {
        return ResolvedEvaluationBudgets {
            budgets: explicit.clone(),
            diagnostic: None,
        };
    }

    let mut budgets = explicit.clone();
    let mut diagnostic = EvaluationResourceConfigDiagnostic {
        max_vertices: LegacyResourceConfigDisposition::NotPresent,
        max_memory_mb_retained: LegacyResourceConfigDisposition::NotPresent,
        max_memory_mb_scratch: LegacyResourceConfigDisposition::NotPresent,
        max_eval_time: LegacyResourceConfigDisposition::NotPresent,
        graph_admission_activation_deferred_to_c2: true,
    };
    if let Some(max_vertices) = max_vertices {
        if budgets.admission.graph_vertex_hard_limit.is_none() {
            budgets.admission.graph_vertex_hard_limit = Some(max_vertices);
            diagnostic.max_vertices = LegacyResourceConfigDisposition::Mapped;
        } else {
            diagnostic.max_vertices = LegacyResourceConfigDisposition::IgnoredByExplicitBudget;
        }
    }
    if let Some(max_eval_time) = max_eval_time {
        if budgets.deadline.max_elapsed.is_none() {
            budgets.deadline.max_elapsed = Some(max_eval_time);
            diagnostic.max_eval_time = LegacyResourceConfigDisposition::Mapped;
        } else {
            diagnostic.max_eval_time = LegacyResourceConfigDisposition::IgnoredByExplicitBudget;
        }
    }
    if let Some(memory_mb) = max_memory_mb {
        let bytes = u64::try_from(memory_mb)
            .unwrap_or(u64::MAX)
            .saturating_mul(1024 * 1024);
        let (retained, scratch) = split_legacy_memory_bytes(bytes);
        if budgets.retained.total_bytes.is_none() {
            budgets.retained.total_bytes = Some(retained);
            diagnostic.max_memory_mb_retained = LegacyResourceConfigDisposition::Mapped;
        } else {
            diagnostic.max_memory_mb_retained =
                LegacyResourceConfigDisposition::IgnoredByExplicitBudget;
        }
        if budgets.scratch.total_bytes.is_none() {
            budgets.scratch.total_bytes = Some(scratch);
            diagnostic.max_memory_mb_scratch = LegacyResourceConfigDisposition::Mapped;
        } else {
            diagnostic.max_memory_mb_scratch =
                LegacyResourceConfigDisposition::IgnoredByExplicitBudget;
        }
    }
    ResolvedEvaluationBudgets {
        budgets,
        diagnostic: Some(diagnostic),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceLedgerError {
    Exhausted(ResourceExhaustionDetail),
    ReleaseUnderflow {
        reason: ResourceExhaustionReason,
        reserved: u64,
        released: u64,
    },
}

impl ResourceLedgerError {
    pub fn into_excel_error(self) -> ExcelError {
        let detail = match self {
            Self::Exhausted(detail) => detail,
            Self::ReleaseUnderflow {
                reserved, released, ..
            } => ResourceExhaustionDetail {
                reason: ResourceExhaustionReason::ArithmeticOverflow,
                limit: reserved,
                observed: released,
                request_id: None,
            },
        };
        ExcelError::new(ExcelErrorKind::NImpl)
            .with_message(format!(
                "evaluation resource exhausted: {} (observed {}, limit {})",
                detail.reason.as_str(),
                detail.observed,
                detail.limit
            ))
            .with_extra(ExcelErrorExtra::Resource {
                detail: Box::new(detail),
            })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceLedgerSnapshot {
    pub retained_limit: Option<u64>,
    pub retained_current: u64,
    pub retained_peak: u64,
    pub scratch_limit: Option<u64>,
    pub scratch_current: u64,
    pub scratch_peak: u64,
    pub disk_scratch_policy: Option<DiskScratchPolicy>,
    pub work_limit: Option<u64>,
    pub work_charged: u64,
    pub deadline_ns: Option<u64>,
    pub deadline_checkpoints: u64,
    pub exhaustion: Option<ResourceExhaustionReason>,
}

type ElapsedClock = Arc<dyn Fn() -> Duration + Send + Sync>;

pub struct ResourceLedger {
    request_id: Option<u64>,
    budgets: EvaluationBudgets,
    retained_current: u64,
    retained_peak: u64,
    scratch_current: u64,
    scratch_peak: u64,
    work_charged: u64,
    deadline_checkpoints: u64,
    exhaustion: Option<ResourceExhaustionReason>,
    elapsed: ElapsedClock,
}

impl fmt::Debug for ResourceLedger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceLedger")
            .field("request_id", &self.request_id)
            .field("budgets", &self.budgets)
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl ResourceLedger {
    pub fn new(request_id: Option<u64>, budgets: EvaluationBudgets) -> Self {
        let started = FzInstant::now();
        Self::with_elapsed_clock(request_id, budgets, Arc::new(move || started.elapsed()))
    }

    fn with_elapsed_clock(
        request_id: Option<u64>,
        budgets: EvaluationBudgets,
        elapsed: ElapsedClock,
    ) -> Self {
        Self {
            request_id,
            budgets,
            retained_current: 0,
            retained_peak: 0,
            scratch_current: 0,
            scratch_peak: 0,
            work_charged: 0,
            deadline_checkpoints: 0,
            exhaustion: None,
            elapsed,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_elapsed_clock(
        request_id: Option<u64>,
        budgets: EvaluationBudgets,
        elapsed: ElapsedClock,
    ) -> Self {
        Self::with_elapsed_clock(request_id, budgets, elapsed)
    }

    fn exhausted(
        &mut self,
        reason: ResourceExhaustionReason,
        limit: u64,
        observed: u64,
    ) -> ResourceLedgerError {
        self.exhaustion = Some(reason);
        ResourceLedgerError::Exhausted(ResourceExhaustionDetail {
            reason,
            limit,
            observed,
            request_id: self.request_id,
        })
    }

    /// Observe retained ownership without activating the C1b retained cap.
    pub(crate) fn observe_retained(&mut self, bytes: u64) {
        self.retained_current = self.retained_current.saturating_add(bytes);
        self.retained_peak = self.retained_peak.max(self.retained_current);
    }

    /// Observe scoped scratch ownership without activating the C1b scratch cap.
    pub(crate) fn observe_scratch(&mut self, bytes: u64) {
        self.scratch_current = self.scratch_current.saturating_add(bytes);
        self.scratch_peak = self.scratch_peak.max(self.scratch_current);
    }

    pub fn reserve_retained(&mut self, bytes: u64) -> Result<(), ResourceLedgerError> {
        let Some(next) = self.retained_current.checked_add(bytes) else {
            return Err(self.exhausted(
                ResourceExhaustionReason::ArithmeticOverflow,
                u64::MAX,
                u64::MAX,
            ));
        };
        if let Some(limit) = self.budgets.retained.total_bytes
            && next > limit
        {
            return Err(self.exhausted(ResourceExhaustionReason::RetainedMemory, limit, next));
        }
        self.retained_current = next;
        self.retained_peak = self.retained_peak.max(next);
        Ok(())
    }

    pub fn release_retained(&mut self, bytes: u64) -> Result<(), ResourceLedgerError> {
        let Some(next) = self.retained_current.checked_sub(bytes) else {
            return Err(ResourceLedgerError::ReleaseUnderflow {
                reason: ResourceExhaustionReason::RetainedMemory,
                reserved: self.retained_current,
                released: bytes,
            });
        };
        self.retained_current = next;
        Ok(())
    }

    pub fn reserve_scratch(&mut self, bytes: u64) -> Result<(), ResourceLedgerError> {
        let Some(next) = self.scratch_current.checked_add(bytes) else {
            return Err(self.exhausted(
                ResourceExhaustionReason::ArithmeticOverflow,
                u64::MAX,
                u64::MAX,
            ));
        };
        if let Some(limit) = self.budgets.scratch.total_bytes
            && next > limit
        {
            return Err(self.exhausted(ResourceExhaustionReason::ScratchMemory, limit, next));
        }
        self.scratch_current = next;
        self.scratch_peak = self.scratch_peak.max(next);
        Ok(())
    }

    pub fn release_scratch(&mut self, bytes: u64) -> Result<(), ResourceLedgerError> {
        let Some(next) = self.scratch_current.checked_sub(bytes) else {
            return Err(ResourceLedgerError::ReleaseUnderflow {
                reason: ResourceExhaustionReason::ScratchMemory,
                reserved: self.scratch_current,
                released: bytes,
            });
        };
        self.scratch_current = next;
        Ok(())
    }

    pub fn release_all_scratch(&mut self) {
        self.scratch_current = 0;
    }

    pub fn charge_work(&mut self, units: u64) -> Result<(), ResourceLedgerError> {
        let Some(next) = self.work_charged.checked_add(units) else {
            return Err(self.exhausted(
                ResourceExhaustionReason::ArithmeticOverflow,
                u64::MAX,
                u64::MAX,
            ));
        };
        if let Some(limit) = self.budgets.work.max_work_units
            && next > limit
        {
            return Err(self.exhausted(ResourceExhaustionReason::WorkUnits, limit, next));
        }
        self.work_charged = next;
        Ok(())
    }

    pub fn checkpoint_deadline(&mut self) -> Result<(), ResourceLedgerError> {
        self.deadline_checkpoints = self.deadline_checkpoints.saturating_add(1);
        let Some(limit) = self.budgets.deadline.max_elapsed else {
            return Ok(());
        };
        let elapsed = (self.elapsed)();
        if elapsed >= limit {
            let limit_ns = u64::try_from(limit.as_nanos()).unwrap_or(u64::MAX);
            let observed_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
            return Err(self.exhausted(ResourceExhaustionReason::Deadline, limit_ns, observed_ns));
        }
        Ok(())
    }

    pub fn snapshot(&self) -> ResourceLedgerSnapshot {
        ResourceLedgerSnapshot {
            retained_limit: self.budgets.retained.total_bytes,
            retained_current: self.retained_current,
            retained_peak: self.retained_peak,
            scratch_limit: self.budgets.scratch.total_bytes,
            scratch_current: self.scratch_current,
            scratch_peak: self.scratch_peak,
            disk_scratch_policy: self.budgets.scratch.disk_scratch_policy,
            work_limit: self.budgets.work.max_work_units,
            work_charged: self.work_charged,
            deadline_ns: self
                .budgets
                .deadline
                .max_elapsed
                .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)),
            deadline_checkpoints: self.deadline_checkpoints,
            exhaustion: self.exhaustion,
        }
    }
}
