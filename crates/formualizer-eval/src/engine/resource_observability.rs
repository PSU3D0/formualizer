use super::{
    EvaluationIncompleteReason, EvaluationResourceProfileKind, FormulaPlaneMode,
    ResourceLedgerSnapshot,
};
use formualizer_common::ResourceExhaustionReason;

/// Stable classification for evaluation limits. C0 is observational only: these classes do not
/// alter limit enforcement or fallback selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvaluationResourceClass {
    SemanticFormat,
    Admission,
    RetainedMemory,
    ScratchMemory,
    WorkTime,
    Optimization,
}

/// Stable reason vocabulary for observed evaluation-resource boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EvaluationResourceReason {
    FormulaPlaneTopologyCandidates,
    FormulaPlaneTopologyEdges,
    FormulaPlaneTopologyRetainedBytes,
    FormulaPlaneMaterializationCells,
    FormulaReplayEncodedBytes,
    FormulaReplayMemoryBytes,
    FormulaReplayDiskBytes,
    FormulaReplayFiles,
    EvaluationCancelled,
    EvaluationError,
}

impl EvaluationResourceClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SemanticFormat => "semantic_format",
            Self::Admission => "admission",
            Self::RetainedMemory => "retained_memory",
            Self::ScratchMemory => "scratch_memory",
            Self::WorkTime => "work_time",
            Self::Optimization => "optimization",
        }
    }
}

impl EvaluationResourceReason {
    pub const fn class(self) -> EvaluationResourceClass {
        match self {
            Self::FormulaPlaneTopologyCandidates | Self::FormulaPlaneTopologyEdges => {
                EvaluationResourceClass::Optimization
            }
            Self::FormulaPlaneTopologyRetainedBytes => EvaluationResourceClass::RetainedMemory,
            Self::FormulaPlaneMaterializationCells
            | Self::FormulaReplayEncodedBytes
            | Self::FormulaReplayDiskBytes
            | Self::FormulaReplayFiles => EvaluationResourceClass::Admission,
            Self::FormulaReplayMemoryBytes => EvaluationResourceClass::ScratchMemory,
            Self::EvaluationCancelled | Self::EvaluationError => EvaluationResourceClass::WorkTime,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FormulaPlaneTopologyCandidates => "formula_plane_topology_candidates",
            Self::FormulaPlaneTopologyEdges => "formula_plane_topology_edges",
            Self::FormulaPlaneTopologyRetainedBytes => "formula_plane_topology_retained_bytes",
            Self::FormulaPlaneMaterializationCells => "formula_plane_materialization_cells",
            Self::FormulaReplayEncodedBytes => "formula_replay_encoded_bytes",
            Self::FormulaReplayMemoryBytes => "formula_replay_memory_bytes",
            Self::FormulaReplayDiskBytes => "formula_replay_disk_bytes",
            Self::FormulaReplayFiles => "formula_replay_files",
            Self::EvaluationCancelled => "evaluation_cancelled",
            Self::EvaluationError => "evaluation_error",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum EvaluationRequestKind {
    Vertex,
    Targeted,
    RecalcPlan,
    #[default]
    Full,
    FullWithDelta,
    Cell,
    Cells,
    CellsCancellable,
    CellsWithDelta,
    FullCancellable,
    TargetedCancellable,
    FullLogged,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum EvaluationRequestOutcome {
    #[default]
    InProgress,
    Success,
    Cancelled,
    Error,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum FormulaPlaneTopologyStrategy {
    #[default]
    NotUsed,
    Legacy,
    SkippedNoActiveSpans,
    SkippedNoDirtyWork,
    Cached,
    CompiledAndCached,
    CapacityFallbackMaterialization,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum FormulaPlaneTopologyCacheOutcome {
    #[default]
    NotUsed,
    Hit,
    Built,
    SkippedOverflow,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum FormulaDirtyLeaseOutcome {
    #[default]
    NotAcquired,
    Acquired,
    Empty,
    Acknowledged,
    AcknowledgedEmpty,
    RetainedOnCancellation,
    RetainedOnError,
}

impl EvaluationRequestKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Vertex => "vertex",
            Self::Targeted => "targeted",
            Self::RecalcPlan => "recalc_plan",
            Self::Full => "full",
            Self::FullWithDelta => "full_with_delta",
            Self::Cell => "cell",
            Self::Cells => "cells",
            Self::CellsCancellable => "cells_cancellable",
            Self::CellsWithDelta => "cells_with_delta",
            Self::FullCancellable => "full_cancellable",
            Self::TargetedCancellable => "targeted_cancellable",
            Self::FullLogged => "full_logged",
        }
    }
}

impl EvaluationRequestOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InProgress => "in_progress",
            Self::Success => "success",
            Self::Cancelled => "cancelled",
            Self::Error => "error",
        }
    }
}

impl FormulaPlaneTopologyStrategy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotUsed => "not_used",
            Self::Legacy => "legacy",
            Self::SkippedNoActiveSpans => "skipped_no_active_spans",
            Self::SkippedNoDirtyWork => "skipped_no_dirty_work",
            Self::Cached => "cached",
            Self::CompiledAndCached => "compiled_and_cached",
            Self::CapacityFallbackMaterialization => "capacity_fallback_materialization",
        }
    }

    pub(crate) const fn severity(self) -> u8 {
        match self {
            Self::NotUsed => 0,
            Self::Legacy => 1,
            Self::SkippedNoActiveSpans => 2,
            Self::SkippedNoDirtyWork => 3,
            Self::Cached => 4,
            Self::CompiledAndCached => 5,
            Self::CapacityFallbackMaterialization => 6,
        }
    }
}

impl FormulaPlaneTopologyCacheOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotUsed => "not_used",
            Self::Hit => "hit",
            Self::Built => "built",
            Self::SkippedOverflow => "skipped_overflow",
        }
    }

    pub(crate) const fn severity(self) -> u8 {
        match self {
            Self::NotUsed => 0,
            Self::Hit => 1,
            Self::Built => 2,
            Self::SkippedOverflow => 3,
        }
    }
}

impl FormulaDirtyLeaseOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotAcquired => "not_acquired",
            Self::Acquired => "acquired",
            Self::Empty => "empty",
            Self::Acknowledged => "acknowledged",
            Self::AcknowledgedEmpty => "acknowledged_empty",
            Self::RetainedOnCancellation => "retained_on_cancellation",
            Self::RetainedOnError => "retained_on_error",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FormulaPlaneTopologyRequestStats {
    /// Most severe strategy observed during the request.
    pub strategy: FormulaPlaneTopologyStrategy,
    /// Most severe cache outcome observed during the request.
    pub cache_outcome: FormulaPlaneTopologyCacheOutcome,
    pub cache_hit_events: u64,
    pub cache_build_events: u64,
    pub cache_skip_events: u64,
    /// Sum across topology build attempts; cache hits do not replay prior build work.
    pub producers_observed: u64,
    pub candidates_observed: u64,
    pub edges_observed: u64,
    pub retained_bytes_observed: u64,
    pub candidate_cap_hits: u64,
    pub edge_cap_hits: u64,
    pub byte_cap_hits: u64,
    pub overflow_reason: Option<EvaluationResourceReason>,
    pub incomplete_reason: Option<EvaluationIncompleteReason>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EvaluationResourceLedgerRequestStats {
    pub profile: EvaluationResourceProfileKind,
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

impl EvaluationResourceLedgerRequestStats {
    pub(crate) fn update(
        &mut self,
        profile: EvaluationResourceProfileKind,
        snapshot: ResourceLedgerSnapshot,
    ) {
        self.profile = profile;
        self.retained_limit = snapshot.retained_limit;
        self.retained_current = snapshot.retained_current;
        self.retained_peak = snapshot.retained_peak;
        self.scratch_limit = snapshot.scratch_limit;
        self.scratch_current = snapshot.scratch_current;
        self.scratch_peak = snapshot.scratch_peak;
        self.work_limit = snapshot.work_limit;
        self.work_charged = snapshot.work_charged;
        self.deadline_ns = snapshot.deadline_ns;
        self.deadline_checkpoints = snapshot.deadline_checkpoints;
        self.exhaustion = snapshot.exhaustion;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EvaluationRequestPhaseTimings {
    pub total_ns: u64,
    pub staged_prepare_ns: u64,
    pub topology_ns: u64,
    pub materialization_ns: u64,
    pub evaluation_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluationResourceRequestStats {
    pub request_id: u64,
    pub kind: EvaluationRequestKind,
    pub formula_plane_mode: FormulaPlaneMode,
    pub outcome: EvaluationRequestOutcome,
    pub staged_selected: u64,
    pub staged_retained: u64,
    pub topology: FormulaPlaneTopologyRequestStats,
    pub fallback_materialized_cells: u64,
    pub cycle_materialized_cells: u64,
    pub dirty_lease: FormulaDirtyLeaseOutcome,
    pub ledger: EvaluationResourceLedgerRequestStats,
    pub phases: EvaluationRequestPhaseTimings,
}

impl EvaluationResourceRequestStats {
    pub(crate) fn new(
        request_id: u64,
        kind: EvaluationRequestKind,
        formula_plane_mode: FormulaPlaneMode,
        staged_retained: usize,
    ) -> Self {
        Self {
            request_id,
            kind,
            formula_plane_mode,
            outcome: EvaluationRequestOutcome::InProgress,
            staged_selected: 0,
            staged_retained: staged_retained as u64,
            topology: FormulaPlaneTopologyRequestStats {
                strategy: if formula_plane_mode == FormulaPlaneMode::AuthoritativeExperimental {
                    FormulaPlaneTopologyStrategy::NotUsed
                } else {
                    FormulaPlaneTopologyStrategy::Legacy
                },
                ..FormulaPlaneTopologyRequestStats::default()
            },
            fallback_materialized_cells: 0,
            cycle_materialized_cells: 0,
            dirty_lease: FormulaDirtyLeaseOutcome::NotAcquired,
            ledger: EvaluationResourceLedgerRequestStats::default(),
            phases: EvaluationRequestPhaseTimings::default(),
        }
    }
}

/// Cumulative observational counters since engine creation or the last explicit telemetry reset.
/// Resetting these counters never resets the monotonic request ID sequence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EvaluationResourceBaselineStats {
    pub last_request_id: u64,
    pub requests_started: u64,
    pub requests_succeeded: u64,
    pub requests_cancelled: u64,
    pub requests_errored: u64,
    pub staged_selected_total: u64,
    pub topology_cache_hits: u64,
    pub topology_cache_builds: u64,
    pub topology_cache_skips: u64,
    pub topology_candidate_cap_hits: u64,
    pub topology_edge_cap_hits: u64,
    pub topology_byte_cap_hits: u64,
    pub topology_candidates_observed_total: u64,
    pub topology_edges_observed_total: u64,
    pub topology_retained_bytes_observed_max: u64,
    pub fallback_materialized_cells_total: u64,
    pub cycle_materialized_cells_total: u64,
    pub dirty_leases_acknowledged: u64,
    pub dirty_leases_retained_on_cancel: u64,
    pub dirty_leases_retained_on_error: u64,
    pub ledger_retained_peak: u64,
    pub ledger_scratch_peak: u64,
    pub ledger_work_charged_total: u64,
    pub ledger_deadline_checkpoints: u64,
    pub ledger_exhaustions: u64,
    pub last_ledger_exhaustion: Option<ResourceExhaustionReason>,
    pub total_request_ns: u64,
    pub staged_prepare_ns: u64,
    pub topology_ns: u64,
    pub materialization_ns: u64,
    pub evaluation_ns: u64,
}

impl EvaluationResourceBaselineStats {
    pub(crate) fn record_started(&mut self, request_id: u64) {
        self.last_request_id = request_id;
        self.requests_started = self.requests_started.saturating_add(1);
    }

    pub(crate) fn record_finished(&mut self, stats: &EvaluationResourceRequestStats) {
        match stats.outcome {
            EvaluationRequestOutcome::Success => {
                self.requests_succeeded = self.requests_succeeded.saturating_add(1)
            }
            EvaluationRequestOutcome::Cancelled => {
                self.requests_cancelled = self.requests_cancelled.saturating_add(1)
            }
            EvaluationRequestOutcome::Error => {
                self.requests_errored = self.requests_errored.saturating_add(1)
            }
            EvaluationRequestOutcome::InProgress => {}
        }
        self.staged_selected_total = self
            .staged_selected_total
            .saturating_add(stats.staged_selected);
        self.topology_cache_hits = self
            .topology_cache_hits
            .saturating_add(stats.topology.cache_hit_events);
        self.topology_cache_builds = self
            .topology_cache_builds
            .saturating_add(stats.topology.cache_build_events);
        self.topology_cache_skips = self
            .topology_cache_skips
            .saturating_add(stats.topology.cache_skip_events);
        self.topology_candidate_cap_hits = self
            .topology_candidate_cap_hits
            .saturating_add(stats.topology.candidate_cap_hits);
        self.topology_edge_cap_hits = self
            .topology_edge_cap_hits
            .saturating_add(stats.topology.edge_cap_hits);
        self.topology_byte_cap_hits = self
            .topology_byte_cap_hits
            .saturating_add(stats.topology.byte_cap_hits);
        self.topology_candidates_observed_total = self
            .topology_candidates_observed_total
            .saturating_add(stats.topology.candidates_observed);
        self.topology_edges_observed_total = self
            .topology_edges_observed_total
            .saturating_add(stats.topology.edges_observed);
        self.topology_retained_bytes_observed_max = self
            .topology_retained_bytes_observed_max
            .max(stats.topology.retained_bytes_observed);
        self.fallback_materialized_cells_total = self
            .fallback_materialized_cells_total
            .saturating_add(stats.fallback_materialized_cells);
        self.cycle_materialized_cells_total = self
            .cycle_materialized_cells_total
            .saturating_add(stats.cycle_materialized_cells);
        match stats.dirty_lease {
            FormulaDirtyLeaseOutcome::Acknowledged
            | FormulaDirtyLeaseOutcome::AcknowledgedEmpty => {
                self.dirty_leases_acknowledged = self.dirty_leases_acknowledged.saturating_add(1)
            }
            FormulaDirtyLeaseOutcome::RetainedOnCancellation => {
                self.dirty_leases_retained_on_cancel =
                    self.dirty_leases_retained_on_cancel.saturating_add(1)
            }
            FormulaDirtyLeaseOutcome::RetainedOnError => {
                self.dirty_leases_retained_on_error =
                    self.dirty_leases_retained_on_error.saturating_add(1)
            }
            _ => {}
        }
        self.ledger_retained_peak = self.ledger_retained_peak.max(stats.ledger.retained_peak);
        self.ledger_scratch_peak = self.ledger_scratch_peak.max(stats.ledger.scratch_peak);
        self.ledger_work_charged_total = self
            .ledger_work_charged_total
            .saturating_add(stats.ledger.work_charged);
        self.ledger_deadline_checkpoints = self
            .ledger_deadline_checkpoints
            .saturating_add(stats.ledger.deadline_checkpoints);
        if let Some(reason) = stats.ledger.exhaustion {
            self.ledger_exhaustions = self.ledger_exhaustions.saturating_add(1);
            self.last_ledger_exhaustion = Some(reason);
        }
        self.total_request_ns = self.total_request_ns.saturating_add(stats.phases.total_ns);
        self.staged_prepare_ns = self
            .staged_prepare_ns
            .saturating_add(stats.phases.staged_prepare_ns);
        self.topology_ns = self.topology_ns.saturating_add(stats.phases.topology_ns);
        self.materialization_ns = self
            .materialization_ns
            .saturating_add(stats.phases.materialization_ns);
        self.evaluation_ns = self
            .evaluation_ns
            .saturating_add(stats.phases.evaluation_ns);
    }
}
