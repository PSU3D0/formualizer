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
    pub fn finance_balanced() -> Self {
        Self {
            retained_bytes: 256 * 1024 * 1024,
            request_scratch_bytes: 256 * 1024 * 1024,
            materialized_graph_bytes: 1024 * 1024 * 1024,
            max_work_units: u64::MAX,
            deadline: None,
            max_threads: usize::MAX,
            disk_scratch: if cfg!(target_arch = "wasm32") {
                DiskScratchPolicy::MemoryOnly
            } else {
                DiskScratchPolicy::NativeTemporary
            },
        }
    }

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
            },
            work: WorkResourceBudget {
                max_work_units: Some(self.max_work_units),
            },
            deadline: DeadlineResourceBudget {
                max_elapsed: self.deadline,
            },
            optimization: OptimizationResourceBudget {
                mixed_cache_candidates: usize::try_from(mixed_cache / 64).ok(),
                mixed_cache_edges: usize::try_from(mixed_cache / 64).ok(),
                max_threads: Some(self.max_threads),
            },
            ..EvaluationBudgets::default()
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum EvaluationResourceProfile {
    #[default]
    Compatibility,
    FinanceBalanced,
    Constrained(ResourceEnvelope),
    Custom(EvaluationBudgets),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EvaluationResourceProfileKind {
    #[default]
    Compatibility,
    FinanceBalanced,
    Constrained,
    Custom,
}

impl EvaluationResourceProfileKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Compatibility => "compatibility",
            Self::FinanceBalanced => "finance_balanced",
            Self::Constrained => "constrained",
            Self::Custom => "custom",
        }
    }
}

impl EvaluationResourceProfile {
    pub fn budgets(&self) -> EvaluationBudgets {
        match self {
            Self::Compatibility => EvaluationBudgets::default(),
            Self::FinanceBalanced => ResourceEnvelope::finance_balanced().to_budgets(),
            Self::Constrained(envelope) => envelope.to_budgets(),
            Self::Custom(budgets) => budgets.clone(),
        }
    }

    pub const fn kind(&self) -> EvaluationResourceProfileKind {
        match self {
            Self::Compatibility => EvaluationResourceProfileKind::Compatibility,
            Self::FinanceBalanced => EvaluationResourceProfileKind::FinanceBalanced,
            Self::Constrained(_) => EvaluationResourceProfileKind::Constrained,
            Self::Custom(_) => EvaluationResourceProfileKind::Custom,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyResourceConfigDisposition {
    MappedToCustom,
    IgnoredByExplicitProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationResourceConfigDiagnostic {
    pub disposition: LegacyResourceConfigDisposition,
    pub max_vertices_present: bool,
    pub max_memory_mb_present: bool,
    pub max_eval_time_present: bool,
    /// C1a does not enforce graph caps in any mutation path; composed activation is C2.
    pub graph_admission_activation_deferred_to_c2: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedResourceProfile {
    pub kind: EvaluationResourceProfileKind,
    pub budgets: EvaluationBudgets,
    pub diagnostic: Option<EvaluationResourceConfigDiagnostic>,
}

pub(crate) fn split_legacy_memory_bytes(bytes: u64) -> (u64, u64) {
    let scratch = bytes / 2;
    (bytes.saturating_sub(scratch), scratch)
}

pub(crate) fn resolve_resource_profile(
    profile: &EvaluationResourceProfile,
    max_vertices: Option<usize>,
    max_memory_mb: Option<usize>,
    max_eval_time: Option<Duration>,
) -> ResolvedResourceProfile {
    let legacy_present =
        max_vertices.is_some() || max_memory_mb.is_some() || max_eval_time.is_some();
    if !matches!(profile, EvaluationResourceProfile::Compatibility) {
        return ResolvedResourceProfile {
            kind: profile.kind(),
            budgets: profile.budgets(),
            diagnostic: legacy_present.then_some(EvaluationResourceConfigDiagnostic {
                disposition: LegacyResourceConfigDisposition::IgnoredByExplicitProfile,
                max_vertices_present: max_vertices.is_some(),
                max_memory_mb_present: max_memory_mb.is_some(),
                max_eval_time_present: max_eval_time.is_some(),
                graph_admission_activation_deferred_to_c2: true,
            }),
        };
    }

    if !legacy_present {
        return ResolvedResourceProfile {
            kind: EvaluationResourceProfileKind::Compatibility,
            budgets: EvaluationBudgets::default(),
            diagnostic: None,
        };
    }

    let mut budgets = EvaluationBudgets::default();
    budgets.admission.graph_vertex_hard_limit = max_vertices;
    budgets.deadline.max_elapsed = max_eval_time;
    if let Some(memory_mb) = max_memory_mb {
        let bytes = u64::try_from(memory_mb)
            .unwrap_or(u64::MAX)
            .saturating_mul(1024 * 1024);
        let (retained, scratch) = split_legacy_memory_bytes(bytes);
        budgets.retained.total_bytes = Some(retained);
        budgets.scratch.total_bytes = Some(scratch);
    }
    ResolvedResourceProfile {
        kind: EvaluationResourceProfileKind::Custom,
        budgets,
        diagnostic: Some(EvaluationResourceConfigDiagnostic {
            disposition: LegacyResourceConfigDisposition::MappedToCustom,
            max_vertices_present: max_vertices.is_some(),
            max_memory_mb_present: max_memory_mb.is_some(),
            max_eval_time_present: max_eval_time.is_some(),
            graph_admission_activation_deferred_to_c2: true,
        }),
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
