//! Formualizer Dependency Graph Engine
//!
//! Provides incremental formula evaluation with dependency tracking.

pub mod arrow_ingest;
pub(crate) mod convergence;
pub mod effects;
pub mod eval;
pub mod eval_delta;
pub mod formula_ingest;
mod formula_source;
pub(crate) mod fragmented_transaction;
pub mod graph;
pub mod ingest;
pub mod ingest_builder;
pub(crate) mod ingest_pipeline;
pub mod journal;
pub mod live_edges;
pub mod live_graph;
pub mod lookup_index_cache;
pub mod plan;
pub mod range_view;
pub mod resource_ledger;
pub mod resource_observability;
pub mod row_visibility;
pub mod scheduler;
pub mod spill;
mod target_preparation;
pub mod vertex;
pub mod virtual_deps;

// New SoA modules
pub mod csr_edges;
pub mod debug_views;
pub mod delta_edges;
pub mod interval_tree;
pub mod named_range;
pub mod sheet_index;
pub mod sheet_registry;
pub mod topo;
pub mod vertex_store;

// Phase 1: Arena modules
pub mod arena;

// Phase 1: Warmup configuration (kept for compatibility)
pub mod tuning;

#[cfg(test)]
mod tests;

pub use arena::AstNodeId;
pub use eval::{
    CycleTelemetry, Engine, EngineAction, EngineBaselineStats, EvalResult, RecalcPlan,
    SourceFormulaIngress, TableMetadata, VirtualDepTelemetry,
};
pub use eval_delta::{
    DeltaMode, EvalDelta, EvalDeltaCompatibilityPolicy, EvalDeltaRecord, TARGET_EVAL_DELTA_VERSION,
    TargetEvalDelta,
};
pub use formula_ingest::{FormulaIngestBatch, FormulaIngestRecord, FormulaIngestReport};
#[doc(hidden)]
pub use formula_source::{
    DeferredFormulaPackage, DeferredFormulaReplay, DeferredReplayFormula,
    ExplicitPartitionLegacyMembers, ExplicitSourceFamilyMembers, FormulaCompressedPreparation,
    FormulaCompressedSourceBatch, FormulaCompressedSourceReport,
    FormulaReplayCoordinateDisposition, FormulaReplayDisposition, FormulaReplayPartitionRouter,
    MAX_EXPLICIT_SOURCE_FAMILY_MEMBERS, MAX_PARTITIONED_SOURCE_FAMILY_FRAGMENTS,
    PartitionLegacyMember, PartitionLegacyMemberKind, PartitionReconciliation,
    PartitionedSourceFormulaFamily, PlacementDomainTransport, SourceCoord, SourceFamilyId,
    SourceFamilyMembers, SourceFormulaFamily, SourceFormulaOrder, SourceRect,
};
pub use journal::{ActionJournal, ArrowOp, ArrowUndoBatch, GraphUndoBatch};
// Use SoA implementation
pub use formualizer_common::{ResourceExhaustionDetail, ResourceExhaustionReason};
pub use graph::snapshot::VertexSnapshot;
pub use graph::{
    ChangeEvent, DependencyGraph, DependencyRef, GraphBaselineStats, OperationSummary, StripeKey,
    StripeType, block_index,
};
pub use resource_ledger::{
    AdmissionResourceBudget, DeadlineResourceBudget, DiskScratchPolicy, EvaluationBudgets,
    EvaluationIncompleteReason, EvaluationResourceConfigDiagnostic,
    LegacyResourceConfigDisposition, OptimizationResourceBudget, ResourceEnvelope, ResourceLedger,
    ResourceLedgerError, ResourceLedgerSnapshot, RetainedResourceBudget, ScratchResourceBudget,
    SemanticResourceBudget, WorkResourceBudget,
};
pub use resource_observability::{
    EvaluationRequestKind, EvaluationRequestOutcome, EvaluationRequestPhaseTimings,
    EvaluationResourceBaselineStats, EvaluationResourceClass, EvaluationResourceLedgerRequestStats,
    EvaluationResourceReason, EvaluationResourceRequestStats, FormulaDirtyLeaseOutcome,
    FormulaPlaneTopologyCacheOutcome, FormulaPlaneTopologyRequestStats,
    FormulaPlaneTopologyStrategy,
};
pub use row_visibility::{RowVisibilitySource, VisibilityMaskMode};
pub use scheduler::{Layer, Schedule, ScheduleUnit, Scheduler};
pub use target_preparation::{
    EvaluationTarget, OpaquePreparePolicy, OpaqueReason, PreparationOutcome, PreparationRevision,
    PrepareScope, PrepareTargetsOptions, PreparedTargetGraphReport, RequestId, TableSelection,
};
pub use vertex::{VertexId, VertexKind};

pub use graph::editor::{
    DataUpdateSummary, EditorError, MetaUpdateSummary, RangeSummary, ShiftSummary, TransactionId,
    VertexDataPatch, VertexEditor, VertexMeta, VertexMetaPatch,
};

pub use graph::editor::change_log::{ChangeLog, ChangeLogger, NullChangeLogger};

#[doc(hidden)]
pub mod fp8_parity_test_support {
    use super::{Engine, EvalConfig};
    use crate::engine::arena::CanonicalLabels;
    use crate::formula_plane::dependency_summary::summarize_canonical_template;
    use crate::formula_plane::producer::SpanReadSummary;
    use crate::formula_plane::runtime::{PlacementDomain, ResultRegion};
    use crate::formula_plane::template_canonical::{
        CanonicalRejectReason, CanonicalTemplateFlag, canonicalize_template,
    };
    use crate::reference::{CellRef, Coord};
    use crate::traits::EvaluationContext;
    use formualizer_common::{ExcelError, LiteralValue};
    use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType, parse};
    use std::collections::BTreeSet;
    use std::sync::Arc;

    #[derive(Clone, Debug)]
    pub struct Fp8ParityObservation {
        pub formula: String,
        pub placement: CellRef,
        pub old_payload: String,
        pub new_hash: u64,
    }

    pub fn default_config() -> EvalConfig {
        EvalConfig::default()
    }

    pub fn parse_formula(formula: &str) -> ASTNode {
        parse(formula).unwrap_or_else(|err| panic!("parse {formula}: {err}"))
    }

    pub fn cell(sheet_id: u16, row: u32, col: u32) -> CellRef {
        CellRef::new(sheet_id, Coord::from_excel(row, col, true, true))
    }

    fn local_binding_declarations(ast: &ASTNode, out: &mut BTreeSet<String>) {
        match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                let canonical = name.rsplit('.').next().unwrap_or(name).to_ascii_uppercase();
                let declaration_indices: Box<dyn Iterator<Item = usize>> = match canonical.as_str()
                {
                    "LET" => Box::new((0..args.len().saturating_sub(1)).step_by(2)),
                    "LAMBDA" => Box::new(0..args.len().saturating_sub(1)),
                    _ => Box::new(std::iter::empty()),
                };
                for index in declaration_indices {
                    if let Some(ASTNode {
                        node_type:
                            ASTNodeType::Reference {
                                reference: ReferenceType::NamedRange(name),
                                ..
                            },
                        ..
                    }) = args.get(index)
                    {
                        out.insert(name.to_ascii_uppercase());
                    }
                }
                for arg in args {
                    local_binding_declarations(arg, out);
                }
            }
            ASTNodeType::UnaryOp { expr, .. } => local_binding_declarations(expr, out),
            ASTNodeType::BinaryOp { left, right, .. } => {
                local_binding_declarations(left, out);
                local_binding_declarations(right, out);
            }
            ASTNodeType::Call { callee, args } => {
                local_binding_declarations(callee, out);
                for arg in args {
                    local_binding_declarations(arg, out);
                }
            }
            ASTNodeType::Array(rows) => {
                for item in rows.iter().flatten() {
                    local_binding_declarations(item, out);
                }
            }
            _ => {}
        }
    }

    pub fn assert_case<R: EvaluationContext>(
        engine: &mut Engine<R>,
        formula: &str,
        placement: CellRef,
    ) -> Fp8ParityObservation {
        let parsed = parse_formula(formula);
        assert_case_ast(engine, formula, parsed, placement)
    }

    pub fn assert_case_ast<R: EvaluationContext>(
        engine: &mut Engine<R>,
        formula: &str,
        parsed: ASTNode,
        placement: CellRef,
    ) -> Fp8ParityObservation {
        let mut local_declarations = BTreeSet::new();
        local_binding_declarations(&parsed, &mut local_declarations);
        let mut old_ast = parsed.clone();
        let old_rewrite = engine
            .graph
            .rewrite_structured_references_for_cell(&mut old_ast, placement);
        let old = old_rewrite.and_then(|_| old_path(engine, &old_ast, placement));

        let new = {
            let mut pipeline = engine.ingest_pipeline();
            pipeline.ingest_formula(
                crate::engine::ingest_pipeline::FormulaAstInput::Tree(parsed),
                placement,
                Some(Arc::<str>::from(formula)),
            )
        };

        match (old, new) {
            (Ok(old), Ok(new)) => {
                let new_direct = sorted_cells(new.dep_plan.direct_cell_deps.clone());
                assert_eq!(
                    old.direct_cells, new_direct,
                    "direct deps differ for {formula} at {placement:?}\nold={:?}\nnew={:?}",
                    old.direct_cells, new_direct
                );
                assert_eq!(
                    old.range_deps, new.dep_plan.range_deps,
                    "range deps differ for {formula} at {placement:?}"
                );
                let old_unresolved_names: Vec<_> = old
                    .unresolved_names
                    .iter()
                    .filter(|name| !local_declarations.contains(&name.to_ascii_uppercase()))
                    .cloned()
                    .collect();
                assert_eq!(
                    old_unresolved_names, new.dep_plan.named_refs,
                    "unresolved names differ for {formula} at {placement:?}"
                );
                assert_eq!(
                    old.volatile, new.dep_plan.volatile,
                    "volatile flag differs for {formula} at {placement:?}"
                );
                assert_eq!(
                    old.dynamic, new.dep_plan.dynamic,
                    "dynamic flag differs for {formula} at {placement:?}"
                );
                let mut expected_labels = canonical_labels_from_old(&old.labels);
                if old.dynamic {
                    expected_labels.flags |= CanonicalLabels::FLAG_DYNAMIC;
                }
                assert_eq!(
                    expected_labels.flags, new.labels.flags,
                    "canonical label flags differ for {formula} at {placement:?}\nold={:?}\nnew={:#x}",
                    old.labels.flags, new.labels.flags
                );
                assert_eq!(
                    expected_labels.rejects, new.labels.rejects,
                    "canonical label rejects differ for {formula} at {placement:?}\nold={:?}\nnew={:#x}",
                    old.labels.reject_reasons, new.labels.rejects
                );
                // The passive summary oracle cannot resolve defined names, so
                // a named formula that the ingest pipeline resolved to a
                // concrete region is an intentional superset: old None / new
                // Some is allowed exactly when the only blocking reason was a
                // named reference.
                let named_resolution_superset = old.summary_rejected_only_for_named_reference
                    && old.read_summary_debug.is_none();
                if !named_resolution_superset {
                    assert_eq!(
                        old.read_summary_debug,
                        new.read_summary.as_ref().map(|s| format!("{s:?}")),
                        "read summary differs for {formula} at {placement:?}"
                    );
                }
                assert_eq!(new.formula_text.as_deref(), Some(formula));
                assert_eq!(new.placement, placement);
                Fp8ParityObservation {
                    formula: formula.to_string(),
                    placement,
                    old_payload: old.payload,
                    new_hash: new.canonical_hash,
                }
            }
            (Err(old), Err(new)) => {
                assert_eq!(
                    old.kind.to_string(),
                    new.kind.to_string(),
                    "old and new errored differently for {formula} at {placement:?}: old={old:?} new={new:?}"
                );
                Fp8ParityObservation {
                    formula: formula.to_string(),
                    placement,
                    old_payload: format!("ERR:{:?}", old.kind),
                    new_hash: 0,
                }
            }
            (Ok(_), Err(new)) => panic!(
                "new pipeline errored but old path succeeded for {formula} at {placement:?}: {new:?}"
            ),
            (Err(old), Ok(_)) => panic!(
                "old path errored but new pipeline succeeded for {formula} at {placement:?}: {old:?}"
            ),
        }
    }

    #[derive(Debug)]
    struct OldOutput {
        payload: String,
        labels: crate::formula_plane::template_canonical::CanonicalTemplateLabels,
        direct_cells: Vec<CellRef>,
        range_deps: Vec<crate::reference::SharedRangeRef<'static>>,
        unresolved_names: Vec<String>,
        volatile: bool,
        dynamic: bool,
        read_summary_debug: Option<String>,
        summary_rejected_only_for_named_reference: bool,
    }

    fn old_path<R: EvaluationContext>(
        engine: &mut Engine<R>,
        ast: &ASTNode,
        placement: CellRef,
    ) -> Result<OldOutput, ExcelError> {
        let (_deps, ranges, placeholders, _named, unresolved_names) = engine
            .graph
            .fp8_parity_extract_dependencies_with_pending_names(ast, placement.sheet_id)?;
        let volatile = engine.graph.fp8_parity_is_ast_volatile(ast);
        let dynamic = engine.graph.is_ast_dynamic(ast);
        let template =
            canonicalize_template(ast, placement.coord.row() + 1, placement.coord.col() + 1);
        let summary = summarize_canonical_template(&template);
        let scalar_domain = PlacementDomain::row_run(
            placement.sheet_id,
            placement.coord.row(),
            placement.coord.row(),
            placement.coord.col(),
        );
        let result_region = ResultRegion::scalar_cells(scalar_domain);
        let read_summary = SpanReadSummary::from_formula_summary(
            placement.sheet_id,
            &result_region,
            &summary,
            engine.graph.sheet_reg(),
        )
        .ok();
        let summary_rejected_only_for_named_reference = !summary.reject_reasons.is_empty()
            && summary.reject_reasons.iter().all(|reason| {
                matches!(
                    reason,
                    crate::formula_plane::dependency_summary::DependencyRejectReason
                        ::NamedRangeUnsupported { .. }
                )
            });
        Ok(OldOutput {
            payload: template.key.payload().to_string(),
            labels: template.labels,
            direct_cells: sorted_cells(placeholders),
            range_deps: ranges,
            unresolved_names,
            volatile,
            dynamic,
            read_summary_debug: read_summary.as_ref().map(|s| format!("{s:?}")),
            summary_rejected_only_for_named_reference,
        })
    }

    fn sorted_cells(mut cells: Vec<CellRef>) -> Vec<CellRef> {
        cells.sort();
        cells.dedup();
        cells
    }

    fn canonical_labels_from_old(
        old: &crate::formula_plane::template_canonical::CanonicalTemplateLabels,
    ) -> CanonicalLabels {
        let mut labels = CanonicalLabels::default();
        for flag in &old.flags {
            labels.flags |= match flag {
                CanonicalTemplateFlag::ParserVolatileFlag => CanonicalLabels::FLAG_VOLATILE,
                CanonicalTemplateFlag::FunctionCall => CanonicalLabels::FLAG_CONTAINS_FUNCTION,
                CanonicalTemplateFlag::CurrentSheetBinding => CanonicalLabels::FLAG_CURRENT_SHEET,
                CanonicalTemplateFlag::ExplicitSheetBinding => CanonicalLabels::FLAG_EXPLICIT_SHEET,
                CanonicalTemplateFlag::RelativeReferenceAxis => CanonicalLabels::FLAG_RELATIVE_ONLY,
                CanonicalTemplateFlag::AbsoluteReferenceAxis => CanonicalLabels::FLAG_ABSOLUTE_ONLY,
                CanonicalTemplateFlag::MixedAnchors => CanonicalLabels::FLAG_MIXED_ANCHORS,
                CanonicalTemplateFlag::FiniteRangeReference => CanonicalLabels::FLAG_CONTAINS_RANGE,
                CanonicalTemplateFlag::NamedReference => CanonicalLabels::FLAG_CONTAINS_NAME,
            };
        }
        for reason in &old.reject_reasons {
            labels.flags |= match reason {
                CanonicalRejectReason::DynamicReferenceFunction { .. } => {
                    CanonicalLabels::FLAG_DYNAMIC
                }
                CanonicalRejectReason::ParserVolatileFlag
                | CanonicalRejectReason::VolatileFunction { .. } => CanonicalLabels::FLAG_VOLATILE,
                CanonicalRejectReason::LocalEnvironmentFunction { .. } => {
                    CanonicalLabels::FLAG_CONTAINS_LET_LAMBDA
                }
                CanonicalRejectReason::ArrayOrSpillFunction { .. }
                | CanonicalRejectReason::ArrayLiteral => CanonicalLabels::FLAG_CONTAINS_ARRAY,
                CanonicalRejectReason::StructuredReference { .. }
                | CanonicalRejectReason::StructuredReferenceCurrentRow { .. } => {
                    CanonicalLabels::FLAG_CONTAINS_TABLE
                        | CanonicalLabels::FLAG_CONTAINS_STRUCTURED_REF
                }
                CanonicalRejectReason::OpenRangeReference { .. }
                | CanonicalRejectReason::WholeAxisReference { .. } => {
                    CanonicalLabels::FLAG_CONTAINS_RANGE
                }
                _ => 0,
            };
            labels.rejects |= match reason {
                CanonicalRejectReason::InvalidPlacementAnchor { .. } => {
                    CanonicalLabels::REJECT_INVALID_PLACEMENT_ANCHOR
                }
                CanonicalRejectReason::DynamicReferenceFunction { .. } => {
                    CanonicalLabels::REJECT_DYNAMIC_REFERENCE
                }
                CanonicalRejectReason::UnknownOrCustomFunction { .. } => {
                    CanonicalLabels::REJECT_UNKNOWN_OR_CUSTOM_FUNCTION
                }
                CanonicalRejectReason::LocalEnvironmentFunction { .. } => {
                    CanonicalLabels::REJECT_LOCAL_ENVIRONMENT
                }
                CanonicalRejectReason::ParserVolatileFlag => {
                    CanonicalLabels::REJECT_PARSER_VOLATILE_FLAG
                }
                CanonicalRejectReason::VolatileFunction { .. } => {
                    CanonicalLabels::REJECT_VOLATILE_FUNCTION
                }
                CanonicalRejectReason::ReferenceReturningFunction { .. } => {
                    CanonicalLabels::REJECT_REFERENCE_RETURNING_FUNCTION
                }
                CanonicalRejectReason::ArrayOrSpillFunction { .. } => {
                    CanonicalLabels::REJECT_ARRAY_OR_SPILL_FUNCTION
                }
                CanonicalRejectReason::ArrayLiteral => CanonicalLabels::REJECT_ARRAY_LITERAL,
                CanonicalRejectReason::SpillReference { .. } => {
                    CanonicalLabels::REJECT_SPILL_REFERENCE
                }
                CanonicalRejectReason::SpillResultRegionOperator => {
                    CanonicalLabels::REJECT_SPILL_RESULT_REGION_OPERATOR
                }
                CanonicalRejectReason::ImplicitIntersectionOperator => {
                    CanonicalLabels::REJECT_IMPLICIT_INTERSECTION_OPERATOR
                }
                CanonicalRejectReason::CallExpression => CanonicalLabels::REJECT_CALL_EXPRESSION,
                CanonicalRejectReason::StructuredReference { .. } => {
                    CanonicalLabels::REJECT_STRUCTURED_REFERENCE
                }
                CanonicalRejectReason::StructuredReferenceCurrentRow { .. } => {
                    CanonicalLabels::REJECT_STRUCTURED_REFERENCE_CURRENT_ROW
                }
                CanonicalRejectReason::ThreeDReference { .. } => {
                    CanonicalLabels::REJECT_THREE_D_REFERENCE
                }
                CanonicalRejectReason::ExternalReference { .. } => {
                    CanonicalLabels::REJECT_EXTERNAL_REFERENCE
                }
                CanonicalRejectReason::OpenRangeReference { .. } => {
                    CanonicalLabels::REJECT_OPEN_RANGE_REFERENCE
                }
                CanonicalRejectReason::WholeAxisReference { .. } => {
                    CanonicalLabels::REJECT_WHOLE_AXIS_REFERENCE
                }
                CanonicalRejectReason::UnsupportedReference { .. } => {
                    CanonicalLabels::REJECT_UNSUPPORTED_REFERENCE
                }
                CanonicalRejectReason::FunctionContractUnsupported { .. }
                | CanonicalRejectReason::ContextDependentFunction { .. } => {
                    CanonicalLabels::REJECT_UNKNOWN_OR_CUSTOM_FUNCTION
                }
            };
        }
        labels
    }

    pub fn literal_number(value: f64) -> LiteralValue {
        LiteralValue::Number(value)
    }
}

// CalcObserver is defined below

use crate::timezone::TimeZoneSpec;
use crate::traits::EvaluationContext;
use crate::traits::VolatileLevel;
use chrono::{DateTime, Utc};
use formualizer_common::error::{ExcelError, ExcelErrorKind};
use std::collections::HashMap;

impl<R: EvaluationContext> Engine<R> {
    pub fn begin_bulk_ingest(&mut self) -> ingest_builder::BulkIngestBuilder<'_> {
        ingest_builder::BulkIngestBuilder::new(&mut self.graph)
    }

    pub fn intern_formula_ast(&mut self, ast: &formualizer_parse::parser::ASTNode) -> AstNodeId {
        self.graph.store_ast(ast)
    }
}

/// 🔮 Scalability Hook: Performance monitoring trait for calculation observability
pub trait CalcObserver: Send + Sync {
    fn on_eval_start(&self, vertex_id: VertexId);
    fn on_eval_complete(&self, vertex_id: VertexId, duration: std::time::Duration);
    fn on_cycle_detected(&self, cycle: &[VertexId]);
    fn on_dirty_propagation(&self, vertex_id: VertexId, affected_count: usize);
}

/// Default no-op observer
impl CalcObserver for () {
    fn on_eval_start(&self, _vertex_id: VertexId) {}
    fn on_eval_complete(&self, _vertex_id: VertexId, _duration: std::time::Duration) {}
    fn on_cycle_detected(&self, _cycle: &[VertexId]) {}
    fn on_dirty_propagation(&self, _vertex_id: VertexId, _affected_count: usize) {}
}

/// Deterministic evaluation configuration.
///
/// When enabled, volatile sources (clock/timezone) are derived solely from this config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeterministicMode {
    /// Non-deterministic: uses the system clock.
    Disabled {
        /// Timezone used by volatile date/time builtins.
        timezone: TimeZoneSpec,
    },
    /// Deterministic: uses a fixed timestamp in the provided timezone.
    Enabled {
        /// Fixed timestamp expressed in UTC.
        timestamp_utc: DateTime<Utc>,
        /// Timezone used to interpret `timestamp_utc` for NOW()/TODAY().
        timezone: TimeZoneSpec,
    },
}

impl Default for DeterministicMode {
    fn default() -> Self {
        Self::Disabled {
            timezone: TimeZoneSpec::default(),
        }
    }
}

impl DeterministicMode {
    pub fn is_enabled(&self) -> bool {
        matches!(self, DeterministicMode::Enabled { .. })
    }

    pub fn timezone(&self) -> &TimeZoneSpec {
        match self {
            DeterministicMode::Disabled { timezone } => timezone,
            DeterministicMode::Enabled { timezone, .. } => timezone,
        }
    }

    pub fn validate(&self) -> Result<(), ExcelError> {
        if let DeterministicMode::Enabled { timezone, .. } = self {
            timezone
                .validate_for_determinism()
                .map_err(|msg| ExcelError::new(ExcelErrorKind::Value).with_message(msg))?;
        }
        Ok(())
    }

    pub fn build_clock(
        &self,
    ) -> Result<std::sync::Arc<dyn crate::timezone::ClockProvider>, ExcelError> {
        self.validate()?;
        Ok(match self {
            #[cfg(feature = "system-clock")]
            DeterministicMode::Disabled { timezone } => {
                std::sync::Arc::new(crate::timezone::SystemClock::new(timezone.clone()))
            }
            #[cfg(not(feature = "system-clock"))]
            DeterministicMode::Disabled { timezone: _ } => {
                // Without the system-clock feature, Disabled mode falls back to a
                // UTC epoch clock so the engine still initialises cleanly in portable
                // wasm guests.  Callers that need real wall-clock time must inject a
                // `ClockProvider` via `EvalConfig::clock`.
                std::sync::Arc::new(crate::timezone::FixedClock::new(
                    chrono::DateTime::UNIX_EPOCH,
                    crate::timezone::TimeZoneSpec::Utc,
                ))
            }
            DeterministicMode::Enabled {
                timestamp_utc,
                timezone,
            } => std::sync::Arc::new(crate::timezone::FixedClock::new(
                *timestamp_utc,
                timezone.clone(),
            )),
        })
    }
}

/// Policy for handling malformed formulas encountered during workbook ingest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormulaParsePolicy {
    /// Reject malformed formulas and fail the load/evaluation path.
    Strict,
    /// Convert malformed formulas into literal error formulas (`#ERROR!`).
    CoerceToError,
    /// Keep the backend-provided cached value and drop the formula.
    KeepCachedValue,
    /// Treat the original formula text as a plain text literal.
    AsText,
}

/// Captured diagnostic for a malformed formula encountered during ingest/graph-build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormulaParseDiagnostic {
    pub sheet: String,
    pub row: u32,
    pub col: u32,
    pub formula: String,
    pub message: String,
    pub policy: FormulaParsePolicy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FormulaPlaneMode {
    /// Disable FormulaPlane promotion/evaluation. This is the stable default;
    /// span evaluation is explicitly opt-in through configuration.
    #[default]
    Off,
    Shadow,
    /// Experimental mode: accepted FormulaPlane spans are installed into
    /// graph-owned authority and are not materialized as per-cell graph formulas.
    AuthoritativeExperimental,
}

/// Storage policy for the private formula replay spool used while loading workbooks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormulaSpoolDiskPolicy {
    /// Spill to secure temporary storage after the configured memory prefix.
    NativeSpill,
    /// Never write formula replay data to a filesystem.
    MemoryOnly,
}

/// Workbook ingest limits applied by loader backends before they materialize large sheets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkbookLoadLimits {
    /// Hard cap for declared/logical sheet rows.
    pub max_sheet_rows: u32,
    /// Hard cap for declared/logical sheet columns.
    pub max_sheet_cols: u32,
    /// Hard cap for the rectangular logical area a backend may materialize.
    pub max_sheet_logical_cells: u64,
    /// Hard cap for formulas materialized by one FormulaPlane fallback.
    pub max_formula_plane_fallback_cells: u64,
    /// Sparse-sheet checks only trigger once a sheet reaches this many logical cells.
    pub sparse_sheet_cell_threshold: u64,
    /// Maximum allowed logical-to-populated-cell ratio once the sparse threshold is crossed.
    pub max_sparse_cell_ratio: u64,
    /// Maximum encoded formula replay bytes retained for one sheet.
    pub max_formula_spool_bytes_per_sheet: u64,
    /// Maximum encoded formula replay bytes produced across one workbook load.
    pub max_formula_spool_bytes_per_workbook: u64,
    /// Maximum number of native spill files created across one workbook load.
    pub max_formula_spool_files_per_workbook: u32,
    /// Encoded bytes retained in memory before native spill.
    pub formula_spool_memory_prefix_bytes: u64,
    /// Independent cap for a memory-only formula replay spool.
    pub max_formula_spool_memory_bytes: u64,
    /// Whether formula replay data may use secure native temporary storage.
    pub formula_spool_disk_policy: FormulaSpoolDiskPolicy,
}

impl Default for WorkbookLoadLimits {
    fn default() -> Self {
        Self {
            max_sheet_rows: 1_048_576,
            max_sheet_cols: 16_384,
            max_sheet_logical_cells: 128_000_000,
            max_formula_plane_fallback_cells: 2_000_000,
            sparse_sheet_cell_threshold: 250_000,
            max_sparse_cell_ratio: 1_024,
            max_formula_spool_bytes_per_sheet: 256 * 1024 * 1024,
            max_formula_spool_bytes_per_workbook: 1024 * 1024 * 1024,
            max_formula_spool_files_per_workbook: 1_024,
            formula_spool_memory_prefix_bytes: 1024 * 1024,
            max_formula_spool_memory_bytes: 16 * 1024 * 1024,
            formula_spool_disk_policy: if cfg!(target_arch = "wasm32") {
                FormulaSpoolDiskPolicy::MemoryOnly
            } else {
                FormulaSpoolDiskPolicy::NativeSpill
            },
        }
    }
}

/// Configuration for the evaluation engine
#[derive(Debug, Clone)]
pub struct EvalConfig {
    pub enable_parallel: bool,
    pub max_threads: Option<usize>,
    /// Deprecated. Maps to `evaluation_budgets.admission.graph_vertex_hard_limit` only when that
    /// explicit field is unset.
    pub max_vertices: Option<usize>,
    /// Deprecated. Maps to `evaluation_budgets.deadline.max_elapsed` only when that explicit field
    /// is unset.
    pub max_eval_time: Option<std::time::Duration>,
    /// Deprecated. Converts MiB to bytes and splits the result 50/50 between otherwise-unset
    /// retained and scratch totals; an odd byte goes to retained. Each explicit total wins its own
    /// conflict independently.
    pub max_memory_mb: Option<usize>,
    /// Explicit evaluation budgets. All fields are unset by default, preserving current behavior.
    /// Deprecated resource fields fill only otherwise-unset destination fields and produce one
    /// field-level diagnostic describing every mapping or conflict.
    pub evaluation_budgets: EvaluationBudgets,

    /// Default sheet name used when no sheet is provided.
    pub default_sheet_name: String,

    /// When false, resolve defined names case-insensitively (ASCII only).
    ///
    /// This matches Excel behavior for defined names.
    pub case_sensitive_names: bool,

    /// When false, resolve table names case-insensitively (ASCII only).
    ///
    /// This matches Excel behavior for native table (ListObject) names.
    pub case_sensitive_tables: bool,

    /// Stable workbook seed used for deterministic RNG composition
    pub workbook_seed: u64,

    /// Volatile granularity for RNG seeding and re-evaluation policy
    pub volatile_level: VolatileLevel,

    /// Deterministic evaluation configuration (clock/timezone injection).
    pub deterministic_mode: DeterministicMode,

    // Range handling configuration (Phase 5)
    /// Ranges with size <= this limit are expanded into individual Cell dependencies
    pub range_expansion_limit: usize,

    /// Fallback maximum row bound for open-ended references (e.g. `A:A`, `A1:A`).
    ///
    /// This is only used when used-bounds cannot be determined.
    pub max_open_ended_rows: u32,

    /// Fallback maximum column bound for open-ended references (e.g. `1:1`, `A1:1`).
    ///
    /// This is only used when used-bounds cannot be determined.
    pub max_open_ended_cols: u32,

    /// Height of stripe blocks for dense range indexing
    pub stripe_height: u32,
    /// Width of stripe blocks for dense range indexing  
    pub stripe_width: u32,
    /// Enable block stripes for dense ranges (vs row/column stripes only)
    pub enable_block_stripes: bool,

    /// Spill behavior configuration (conflicts, bounds, buffering)
    pub spill: SpillConfig,

    /// Cycle handling configuration (detection mode + policy). Defaults to
    /// `CycleDetection::Static` (today's stamp-every-static-SCC behavior);
    /// `CycleDetection::Runtime` is opt-in (RFC #112).
    pub cycle: CycleConfig,

    /// Use dynamic topological ordering (Pearce-Kelly algorithm)
    pub use_dynamic_topo: bool,
    /// Maximum nodes to visit before falling back to full rebuild
    pub pk_visit_budget: usize,
    /// Operations between periodic rank compaction
    pub pk_compaction_interval_ops: u64,
    /// Maximum width for parallel evaluation layers
    pub max_layer_width: Option<usize>,
    /// If true, reject edge insertions that would create a cycle (skip adding that dependency).
    /// If false, allow insertion and let scheduler handle cycles at evaluation time.
    pub pk_reject_cycle_edges: bool,
    /// Sheet index build strategy for bulk loads
    pub sheet_index_mode: SheetIndexMode,

    /// Warmup configuration for global pass planning (Phase 1)
    pub warmup: tuning::WarmupConfig,

    /// Enable Arrow-backed storage reads (Phase A)
    pub arrow_storage_enabled: bool,
    /// Enable delta overlay for Arrow sheets (Phase C)
    pub delta_overlay_enabled: bool,

    /// Mirror formula scalar results into Arrow overlay for Arrow-backed reads
    /// This enables Arrow-only RangeView correctness without Hybrid fallback.
    pub write_formula_overlay_enabled: bool,

    /// Optional memory budget (in bytes) for formula/spill computed Arrow overlays.
    ///
    /// When set, the engine will compact computed overlays into base lanes when the
    /// estimated usage exceeds this cap.
    pub max_overlay_memory_bytes: Option<usize>,

    /// Workbook date system: Excel 1900 (default) or 1904.
    pub date_system: DateSystem,

    /// Policy for malformed formulas encountered during ingest/graph-build.
    pub formula_parse_policy: FormulaParsePolicy,

    /// Defer dependency graph building: ingest values immediately but stage formulas
    /// for on-demand graph construction during evaluation.
    pub defer_graph_building: bool,

    /// Enable virtual dependency convergence telemetry collection.
    ///
    /// When disabled, the engine avoids per-pass timing/edge-count bookkeeping.
    pub enable_virtual_dep_telemetry: bool,

    /// FormulaPlane ingest/planning mode. Defaults to `Off`; span evaluation is
    /// explicitly opt-in while `AuthoritativeExperimental` remains experimental.
    /// `Shadow` may report candidate span opportunities but must still materialize
    /// every formula via the legacy graph path.
    pub formula_plane_mode: FormulaPlaneMode,
    /// Hard candidate bound for compiling mixed FormulaPlane topology.
    pub max_formula_plane_cache_candidates: usize,
    /// Hard relationship bound for compiled mixed FormulaPlane topology.
    pub max_formula_plane_cache_edges: usize,
    /// Hard byte estimate bound for compiled mixed FormulaPlane topology.
    pub max_formula_plane_cache_bytes: usize,

    /// Maximum bytes for the engine-side lookup-index cache.
    pub lookup_index_cache_max_bytes: usize,
}

impl Default for EvalConfig {
    fn default() -> Self {
        Self {
            enable_parallel: true,
            max_threads: None,
            max_vertices: None,
            max_eval_time: None,
            max_memory_mb: None,
            evaluation_budgets: EvaluationBudgets::default(),

            default_sheet_name: format!("Sheet{}", 1),

            // Excel compatibility: identifiers are case-insensitive by default.
            case_sensitive_names: false,
            case_sensitive_tables: false,

            // Deterministic RNG seed (matches traits default)
            workbook_seed: 0xF0F0_D0D0_AAAA_5555,

            // Volatile model default
            volatile_level: VolatileLevel::Always,

            deterministic_mode: DeterministicMode::default(),

            // Range handling defaults (Phase 5)
            range_expansion_limit: 64,
            // Open-ended reference defaults (Excel max dimensions).
            // Lower these to cap `A:A` / `1:1` when used-bounds are unknown.
            max_open_ended_rows: 1_048_576,
            max_open_ended_cols: 16_384,
            stripe_height: 256,
            stripe_width: 256,
            enable_block_stripes: false,
            spill: SpillConfig::default(),
            cycle: CycleConfig::default(),

            // Dynamic topology configuration
            use_dynamic_topo: false, // Disabled by default for compatibility
            pk_visit_budget: 50_000,
            pk_compaction_interval_ops: 100_000,
            max_layer_width: None,
            pk_reject_cycle_edges: false,
            sheet_index_mode: SheetIndexMode::Eager,
            warmup: tuning::WarmupConfig::default(),
            arrow_storage_enabled: true,
            delta_overlay_enabled: true,
            write_formula_overlay_enabled: true,
            max_overlay_memory_bytes: None,
            date_system: DateSystem::Excel1900,
            formula_parse_policy: FormulaParsePolicy::Strict,
            defer_graph_building: false,
            enable_virtual_dep_telemetry: false,
            formula_plane_mode: FormulaPlaneMode::Off,
            max_formula_plane_cache_candidates: 100_000,
            max_formula_plane_cache_edges: 100_000,
            max_formula_plane_cache_bytes: 64 * 1024 * 1024,
            lookup_index_cache_max_bytes: 64 * 1024 * 1024,
        }
    }
}

impl EvalConfig {
    #[inline]
    pub fn with_range_expansion_limit(mut self, limit: usize) -> Self {
        self.range_expansion_limit = limit;
        self
    }

    #[inline]
    pub fn with_parallel(mut self, enable: bool) -> Self {
        self.enable_parallel = enable;
        self
    }

    #[inline]
    pub fn with_block_stripes(mut self, enable: bool) -> Self {
        self.enable_block_stripes = enable;
        self
    }

    #[inline]
    pub fn with_case_sensitive_names(mut self, enable: bool) -> Self {
        self.case_sensitive_names = enable;
        self
    }

    #[inline]
    pub fn with_case_sensitive_tables(mut self, enable: bool) -> Self {
        self.case_sensitive_tables = enable;
        self
    }

    #[inline]
    pub fn with_arrow_storage(mut self, enable: bool) -> Self {
        self.arrow_storage_enabled = enable;
        self
    }

    #[inline]
    pub fn with_delta_overlay(mut self, enable: bool) -> Self {
        self.delta_overlay_enabled = enable;
        self
    }

    #[inline]
    pub fn with_formula_overlay(mut self, enable: bool) -> Self {
        self.write_formula_overlay_enabled = enable;
        self
    }

    #[inline]
    pub fn with_date_system(mut self, system: DateSystem) -> Self {
        self.date_system = system;
        self
    }

    #[inline]
    pub fn with_formula_parse_policy(mut self, policy: FormulaParsePolicy) -> Self {
        self.formula_parse_policy = policy;
        self
    }

    #[inline]
    pub fn with_virtual_dep_telemetry(mut self, enable: bool) -> Self {
        self.enable_virtual_dep_telemetry = enable;
        self
    }

    #[inline]
    pub fn with_formula_plane_mode(mut self, mode: FormulaPlaneMode) -> Self {
        self.formula_plane_mode = mode;
        self
    }

    #[inline]
    pub fn with_evaluation_budgets(mut self, budgets: EvaluationBudgets) -> Self {
        self.evaluation_budgets = budgets;
        self
    }

    /// Resolve explicit and deprecated resource settings without consulting ambient host state.
    pub fn resolved_evaluation_budgets(&self) -> EvaluationBudgets {
        resource_ledger::resolve_evaluation_budgets(
            &self.evaluation_budgets,
            self.max_vertices,
            self.max_memory_mb,
            self.max_eval_time,
        )
        .budgets
    }

    /// Set the cycle configuration.
    ///
    /// # Panics
    /// Panics when `cycle` is invalid (see [`CycleConfig::validate`]):
    /// `Iterate` with `detection: Static`, `max_iterations == 0`, or a
    /// negative/non-finite `max_change` are config errors rejected at build
    /// (spec §2). [`Engine::new`] re-validates for configs assembled via
    /// struct literals.
    #[inline]
    pub fn with_cycle(mut self, cycle: CycleConfig) -> Self {
        if let Err(msg) = cycle.validate() {
            panic!("invalid CycleConfig: {msg}");
        }
        self.cycle = cycle;
        self
    }
}

/// Cycle handling configuration (spec: `formualizer-cycle-semantics-spec.md` §2).
///
/// Nested under [`EvalConfig`] like [`SpillConfig`]; flows through
/// `WorkbookConfig.eval` automatically.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CycleConfig {
    pub detection: CycleDetection,
    pub policy: CyclePolicy,
}

impl CycleConfig {
    /// Runtime detection + Excel-default iterative calculation
    /// (`max_iterations: 100`, `max_change: 0.001`).
    pub fn iterate_excel_defaults() -> Self {
        Self {
            detection: CycleDetection::Runtime,
            policy: CyclePolicy::iterate_excel_defaults(),
        }
    }

    /// Runtime detection + iterative calculation with explicit knobs.
    pub fn iterate(max_iterations: u32, max_change: f64) -> Self {
        Self {
            detection: CycleDetection::Runtime,
            policy: CyclePolicy::Iterate {
                max_iterations,
                max_change,
            },
        }
    }

    /// Validate the configuration (spec §2). Invalid combinations are
    /// rejected at build: [`EvalConfig::with_cycle`] and engine construction
    /// both panic on `Err`.
    pub fn validate(&self) -> Result<(), String> {
        if let CyclePolicy::Iterate {
            max_iterations,
            max_change,
        } = self.policy
        {
            if self.detection == CycleDetection::Static {
                return Err(
                    "CyclePolicy::Iterate requires CycleDetection::Runtime (spec §2)".to_string(),
                );
            }
            if max_iterations == 0 {
                return Err("CyclePolicy::Iterate max_iterations must be >= 1".to_string());
            }
            if !max_change.is_finite() || max_change < 0.0 {
                return Err(format!(
                    "CyclePolicy::Iterate max_change must be finite and >= 0 (got {max_change})"
                ));
            }
        }
        Ok(())
    }

    /// Whether ingest may accept formulas whose dependencies include the
    /// formula's own cell (`=B1+A1` in B1). Excel accepts these only with
    /// iterative calculation enabled; everywhere else the edit-time
    /// "Self-reference detected" rejection stands.
    #[inline]
    pub(crate) fn allows_self_dependency(&self) -> bool {
        self.detection == CycleDetection::Runtime
            && matches!(self.policy, CyclePolicy::Iterate { .. })
    }
}

/// How statically-cyclic SCCs are treated at evaluation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CycleDetection {
    /// Today's behavior: every static SCC is stamped `#CIRC!`. Compat escape
    /// hatch; no live-edge machinery runs.
    #[default]
    Static,
    /// Static SCCs are candidates; members are evaluated with live-edge
    /// recording and only *live* cycles get the policy verdict. Phantom
    /// (live-acyclic) SCCs produce ordinary values (discussion #99).
    Runtime,
}

/// What happens to witnessed (live) cycles under `CycleDetection::Runtime`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum CyclePolicy {
    /// Live cycles produce `#CIRC!`.
    #[default]
    Error,
    /// Excel-style iterative calculation (RFC #113, spec §3.5/§6):
    /// live cycles keep running full passes over all SCC members in member
    /// order (Gauss–Seidel: each result is committed before the next member
    /// runs) until every member converges per the spec-§6 rules or
    /// `max_iterations` total passes (pass 1 included) have run. Hitting the
    /// cap keeps the last values and is NOT an error (Excel parity);
    /// telemetry records `capped_sccs`.
    Iterate {
        /// Total passes per SCC per recalc, pass 1 included. `1` means each
        /// member evaluates exactly once per recalc (the Excel accumulator
        /// contract, spec §7.6); `0` is a config error.
        max_iterations: u32,
        /// Absolute per-member convergence threshold on f64 serial values
        /// (`|Δ| < max_change`, strict — Excel semantics). Negative or
        /// non-finite values are config errors.
        max_change: f64,
    },
}

impl CyclePolicy {
    /// Excel's default iterative-calculation knobs.
    pub const EXCEL_DEFAULT_MAX_ITERATIONS: u32 = 100;
    /// Excel's default maximum-change threshold.
    pub const EXCEL_DEFAULT_MAX_CHANGE: f64 = 0.001;

    /// `Iterate` with Excel's defaults (100 iterations, 0.001 max change).
    pub fn iterate_excel_defaults() -> Self {
        CyclePolicy::Iterate {
            max_iterations: Self::EXCEL_DEFAULT_MAX_ITERATIONS,
            max_change: Self::EXCEL_DEFAULT_MAX_CHANGE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SheetIndexMode {
    /// Build full interval-tree based index during inserts (current behavior)
    Eager,
    /// Defer building any sheet index until first range query or explicit finalize
    Lazy,
    /// Use fast batch building (sorted arrays -> tree) when bulk loading, otherwise incremental
    FastBatch,
}

pub use formualizer_common::DateSystem;

/// Construct a new engine with the given resolver and configuration
pub fn new_engine<R>(resolver: R, config: EvalConfig) -> Engine<R>
where
    R: EvaluationContext + 'static,
{
    Engine::new(resolver, config)
}

/// Configuration for spill behavior. Nested under EvalConfig to avoid bloating the top-level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillConfig {
    /// What to do when target region overlaps non-empty cells or other spills.
    pub conflict_policy: SpillConflictPolicy,
    /// Tiebreaker used when policy allows preemption or multiple anchors race.
    pub tiebreaker: SpillTiebreaker,
    /// Bounds handling when result exceeds sheet capacity.
    pub bounds_policy: SpillBoundsPolicy,
    /// Buffering approach for spill writes.
    pub buffer_mode: SpillBufferMode,
    /// Optional memory budget for shadow buffering in bytes.
    pub memory_budget_bytes: Option<u64>,
    /// Cancellation behavior while streaming rows.
    pub cancellation: SpillCancellationPolicy,
    /// Visibility policy for staged writes.
    pub visibility: SpillVisibility,

    /// Hard cap on the number of cells a single spill may project.
    ///
    /// This prevents pathological vertex explosions from very large dynamic arrays.
    pub max_spill_cells: u32,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            conflict_policy: SpillConflictPolicy::Error,
            tiebreaker: SpillTiebreaker::FirstWins,
            bounds_policy: SpillBoundsPolicy::Strict,
            buffer_mode: SpillBufferMode::ShadowBuffer,
            memory_budget_bytes: None,
            cancellation: SpillCancellationPolicy::Cooperative,
            visibility: SpillVisibility::OnCommit,
            // Conservative: enough for common UI patterns, small enough to avoid graph blowups.
            max_spill_cells: 10_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillConflictPolicy {
    Error,
    Preempt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillTiebreaker {
    FirstWins,
    EvaluationEpochAsc,
    AnchorAddressAsc,
    FunctionPriorityThenAddress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillBoundsPolicy {
    Strict,
    Truncate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillBufferMode {
    ShadowBuffer,
    PersistenceJournal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillCancellationPolicy {
    Cooperative,
    Strict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillVisibility {
    OnCommit,
    StagedLayer,
}

/*
 * Scenario: Tombstone Registry for Missing Sheets
 * When a sheet is deleted, formulas pointing to it become "orphans."
 * Instead of losing the connection, we store the formula's VertexId
 * under the name of the missing sheet.
 *
 * Why it matters:
 * This allows Sheet Addition to remain O(1) for the general case,
 * while providing O(N_orphans) recovery for broken formulas.
 */
#[derive(Debug, Default)]
pub struct TombstoneRegistry {
    // Maps "SheetName" -> Vec<VertexId of formulas waiting for it>
    pub pending_references: HashMap<String, Vec<VertexId>>,
}

impl TombstoneRegistry {
    /// Record that a vertex is waiting for a specific sheet name to appear.
    pub fn add_orphan(&mut self, sheet_name: String, vertex_id: VertexId) {
        self.pending_references
            .entry(sheet_name)
            .or_default()
            .push(vertex_id);
    }

    /// Retrieve and remove all vertices waiting for a specific sheet name.
    pub fn take_orphans(&mut self, sheet_name: &str) -> Vec<VertexId> {
        self.pending_references
            .remove(sheet_name)
            .unwrap_or_default()
    }
}
