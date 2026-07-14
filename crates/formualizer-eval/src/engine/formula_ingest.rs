use std::collections::BTreeMap;
use std::sync::Arc;

use super::FormulaPlaneMode;
use super::arena::AstNodeId;
use super::formula_source::{SourceFamilyId, SourceFormulaOrder};

#[derive(Clone, Debug)]
pub struct FormulaIngestRecord {
    pub row: u32,
    pub col: u32,
    pub ast_id: AstNodeId,
    pub formula_text: Option<Arc<str>>,
    pub(crate) source_order: Option<SourceFormulaOrder>,
    pub(crate) source_family: Option<SourceFamilyId>,
    pub(crate) partition_owner: Option<SourceFamilyId>,
}

impl FormulaIngestRecord {
    pub fn new(row: u32, col: u32, ast_id: AstNodeId, formula_text: Option<Arc<str>>) -> Self {
        Self {
            row,
            col,
            ast_id,
            formula_text,
            source_order: None,
            source_family: None,
            partition_owner: None,
        }
    }

    pub(crate) fn with_source_proof(
        mut self,
        source_order: SourceFormulaOrder,
        source_family: Option<SourceFamilyId>,
        partition_owner: Option<SourceFamilyId>,
    ) -> Self {
        self.source_order = Some(source_order);
        self.source_family = source_family;
        self.partition_owner = partition_owner;
        self
    }
}

#[derive(Clone, Debug)]
pub struct FormulaIngestBatch {
    pub sheet_name: String,
    pub formulas: Vec<FormulaIngestRecord>,
}

impl FormulaIngestBatch {
    pub fn new(sheet_name: impl Into<String>, formulas: Vec<FormulaIngestRecord>) -> Self {
        Self {
            sheet_name: sheet_name.into(),
            formulas,
        }
    }

    pub fn len(&self) -> usize {
        self.formulas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.formulas.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormulaIngestReport {
    pub mode: FormulaPlaneMode,
    pub formula_cells_seen: u64,
    pub graph_formula_cells_materialized: u64,

    pub shadow_candidate_cells: u64,
    pub shadow_accepted_span_cells: u64,
    pub shadow_fallback_cells: u64,
    pub shadow_templates_interned: u64,
    pub shadow_spans_created: u64,

    pub graph_formula_vertices_avoided_shadow: u64,
    pub ast_roots_avoided_shadow: u64,
    pub edge_rows_avoided_shadow: u64,

    pub graph_vertices_created: u64,
    pub graph_edges_created: u64,

    pub source_formula_events: u64,
    pub source_formula_records_spooled: u64,
    pub source_spool_encoded_bytes: u64,
    pub source_spool_peak_memory_bytes: u64,
    pub source_spool_spilled_bytes: u64,
    pub source_spool_replays: u64,
    pub source_ordinary_events: u64,
    pub source_shared_anchor_events: u64,
    pub source_shared_descendant_events: u64,
    pub source_unknown_events: u64,
    pub source_families_seen: u64,
    pub source_family_cells_seen: u64,
    pub source_family_shadow_eligible: u64,
    pub source_family_shadow_eligible_cells: u64,
    pub source_family_promoted: u64,
    pub source_family_promoted_cells: u64,
    pub source_family_fallback: u64,
    pub source_family_fallback_cells: u64,
    pub source_forward_descendants: u64,
    pub source_evidence_limit_fallbacks: u64,
    pub source_evidence_peak_bytes: u64,
    pub source_anchor_parses: u64,
    pub source_anchor_asts: u64,
    pub source_anchor_analyses: u64,
    pub source_descendant_strings_avoided: u64,
    pub source_descendant_events_avoided: u64,
    pub source_descendant_analyses_avoided: u64,
    pub source_compressed_families_prepared: u64,
    pub source_compressed_cells_prepared: u64,
    pub source_partitioned_families_seen: u64,
    pub source_partitioned_families_prepared: u64,
    pub source_partitioned_families_rejected: u64,
    pub source_partition_fragments_prepared: u64,
    pub source_partition_span_cells_prepared: u64,
    pub source_partition_fallback_cells: u64,
    pub source_partition_analyses_reused: u64,
    pub source_partition_function_semantics: u64,
    pub source_partition_holes: u64,
    pub source_partition_ordinary_exceptions: u64,
    pub source_partition_failures: u64,
    pub source_partition_surviving_cells: u64,

    pub fallback_reasons: BTreeMap<String, u64>,
}

impl Default for FormulaIngestReport {
    fn default() -> Self {
        Self {
            mode: FormulaPlaneMode::Off,
            formula_cells_seen: 0,
            graph_formula_cells_materialized: 0,
            shadow_candidate_cells: 0,
            shadow_accepted_span_cells: 0,
            shadow_fallback_cells: 0,
            shadow_templates_interned: 0,
            shadow_spans_created: 0,
            graph_formula_vertices_avoided_shadow: 0,
            ast_roots_avoided_shadow: 0,
            edge_rows_avoided_shadow: 0,
            graph_vertices_created: 0,
            graph_edges_created: 0,
            source_formula_events: 0,
            source_formula_records_spooled: 0,
            source_spool_encoded_bytes: 0,
            source_spool_peak_memory_bytes: 0,
            source_spool_spilled_bytes: 0,
            source_spool_replays: 0,
            source_ordinary_events: 0,
            source_shared_anchor_events: 0,
            source_shared_descendant_events: 0,
            source_unknown_events: 0,
            source_families_seen: 0,
            source_family_cells_seen: 0,
            source_family_shadow_eligible: 0,
            source_family_shadow_eligible_cells: 0,
            source_family_promoted: 0,
            source_family_promoted_cells: 0,
            source_family_fallback: 0,
            source_family_fallback_cells: 0,
            source_forward_descendants: 0,
            source_evidence_limit_fallbacks: 0,
            source_evidence_peak_bytes: 0,
            source_anchor_parses: 0,
            source_anchor_asts: 0,
            source_anchor_analyses: 0,
            source_descendant_strings_avoided: 0,
            source_descendant_events_avoided: 0,
            source_descendant_analyses_avoided: 0,
            source_compressed_families_prepared: 0,
            source_compressed_cells_prepared: 0,
            source_partitioned_families_seen: 0,
            source_partitioned_families_prepared: 0,
            source_partitioned_families_rejected: 0,
            source_partition_fragments_prepared: 0,
            source_partition_span_cells_prepared: 0,
            source_partition_fallback_cells: 0,
            source_partition_analyses_reused: 0,
            source_partition_function_semantics: 0,
            source_partition_holes: 0,
            source_partition_ordinary_exceptions: 0,
            source_partition_failures: 0,
            source_partition_surviving_cells: 0,
            fallback_reasons: BTreeMap::new(),
        }
    }
}

impl FormulaIngestReport {
    pub(crate) fn with_mode(mode: FormulaPlaneMode) -> Self {
        Self {
            mode,
            ..Self::default()
        }
    }

    pub(crate) fn accumulate(&mut self, other: &Self) {
        self.formula_cells_seen = self
            .formula_cells_seen
            .saturating_add(other.formula_cells_seen);
        self.graph_formula_cells_materialized = self
            .graph_formula_cells_materialized
            .saturating_add(other.graph_formula_cells_materialized);
        self.shadow_candidate_cells = self
            .shadow_candidate_cells
            .saturating_add(other.shadow_candidate_cells);
        self.shadow_accepted_span_cells = self
            .shadow_accepted_span_cells
            .saturating_add(other.shadow_accepted_span_cells);
        self.shadow_fallback_cells = self
            .shadow_fallback_cells
            .saturating_add(other.shadow_fallback_cells);
        self.shadow_templates_interned = self
            .shadow_templates_interned
            .saturating_add(other.shadow_templates_interned);
        self.shadow_spans_created = self
            .shadow_spans_created
            .saturating_add(other.shadow_spans_created);
        self.graph_formula_vertices_avoided_shadow = self
            .graph_formula_vertices_avoided_shadow
            .saturating_add(other.graph_formula_vertices_avoided_shadow);
        self.ast_roots_avoided_shadow = self
            .ast_roots_avoided_shadow
            .saturating_add(other.ast_roots_avoided_shadow);
        self.edge_rows_avoided_shadow = self
            .edge_rows_avoided_shadow
            .saturating_add(other.edge_rows_avoided_shadow);
        self.graph_vertices_created = self
            .graph_vertices_created
            .saturating_add(other.graph_vertices_created);
        self.graph_edges_created = self
            .graph_edges_created
            .saturating_add(other.graph_edges_created);
        self.source_formula_events = self
            .source_formula_events
            .saturating_add(other.source_formula_events);
        self.source_formula_records_spooled = self
            .source_formula_records_spooled
            .saturating_add(other.source_formula_records_spooled);
        self.source_spool_encoded_bytes = self
            .source_spool_encoded_bytes
            .saturating_add(other.source_spool_encoded_bytes);
        self.source_spool_peak_memory_bytes = self
            .source_spool_peak_memory_bytes
            .max(other.source_spool_peak_memory_bytes);
        self.source_spool_spilled_bytes = self
            .source_spool_spilled_bytes
            .saturating_add(other.source_spool_spilled_bytes);
        self.source_spool_replays = self
            .source_spool_replays
            .saturating_add(other.source_spool_replays);
        self.source_ordinary_events = self
            .source_ordinary_events
            .saturating_add(other.source_ordinary_events);
        self.source_shared_anchor_events = self
            .source_shared_anchor_events
            .saturating_add(other.source_shared_anchor_events);
        self.source_shared_descendant_events = self
            .source_shared_descendant_events
            .saturating_add(other.source_shared_descendant_events);
        self.source_unknown_events = self
            .source_unknown_events
            .saturating_add(other.source_unknown_events);
        self.source_families_seen = self
            .source_families_seen
            .saturating_add(other.source_families_seen);
        self.source_family_cells_seen = self
            .source_family_cells_seen
            .saturating_add(other.source_family_cells_seen);
        self.source_family_shadow_eligible = self
            .source_family_shadow_eligible
            .saturating_add(other.source_family_shadow_eligible);
        self.source_family_shadow_eligible_cells = self
            .source_family_shadow_eligible_cells
            .saturating_add(other.source_family_shadow_eligible_cells);
        self.source_family_promoted = self
            .source_family_promoted
            .saturating_add(other.source_family_promoted);
        self.source_family_promoted_cells = self
            .source_family_promoted_cells
            .saturating_add(other.source_family_promoted_cells);
        self.source_family_fallback = self
            .source_family_fallback
            .saturating_add(other.source_family_fallback);
        self.source_family_fallback_cells = self
            .source_family_fallback_cells
            .saturating_add(other.source_family_fallback_cells);
        self.source_forward_descendants = self
            .source_forward_descendants
            .saturating_add(other.source_forward_descendants);
        self.source_evidence_limit_fallbacks = self
            .source_evidence_limit_fallbacks
            .saturating_add(other.source_evidence_limit_fallbacks);
        self.source_evidence_peak_bytes = self
            .source_evidence_peak_bytes
            .max(other.source_evidence_peak_bytes);
        self.source_anchor_parses = self
            .source_anchor_parses
            .saturating_add(other.source_anchor_parses);
        self.source_anchor_asts = self
            .source_anchor_asts
            .saturating_add(other.source_anchor_asts);
        self.source_anchor_analyses = self
            .source_anchor_analyses
            .saturating_add(other.source_anchor_analyses);
        self.source_descendant_strings_avoided = self
            .source_descendant_strings_avoided
            .saturating_add(other.source_descendant_strings_avoided);
        self.source_descendant_events_avoided = self
            .source_descendant_events_avoided
            .saturating_add(other.source_descendant_events_avoided);
        self.source_descendant_analyses_avoided = self
            .source_descendant_analyses_avoided
            .saturating_add(other.source_descendant_analyses_avoided);
        self.source_compressed_families_prepared = self
            .source_compressed_families_prepared
            .saturating_add(other.source_compressed_families_prepared);
        self.source_compressed_cells_prepared = self
            .source_compressed_cells_prepared
            .saturating_add(other.source_compressed_cells_prepared);
        self.source_partitioned_families_seen = self
            .source_partitioned_families_seen
            .saturating_add(other.source_partitioned_families_seen);
        self.source_partitioned_families_prepared = self
            .source_partitioned_families_prepared
            .saturating_add(other.source_partitioned_families_prepared);
        self.source_partitioned_families_rejected = self
            .source_partitioned_families_rejected
            .saturating_add(other.source_partitioned_families_rejected);
        self.source_partition_fragments_prepared = self
            .source_partition_fragments_prepared
            .saturating_add(other.source_partition_fragments_prepared);
        self.source_partition_span_cells_prepared = self
            .source_partition_span_cells_prepared
            .saturating_add(other.source_partition_span_cells_prepared);
        self.source_partition_fallback_cells = self
            .source_partition_fallback_cells
            .saturating_add(other.source_partition_fallback_cells);
        self.source_partition_analyses_reused = self
            .source_partition_analyses_reused
            .saturating_add(other.source_partition_analyses_reused);
        self.source_partition_function_semantics = self
            .source_partition_function_semantics
            .saturating_add(other.source_partition_function_semantics);
        self.source_partition_holes = self
            .source_partition_holes
            .saturating_add(other.source_partition_holes);
        self.source_partition_ordinary_exceptions = self
            .source_partition_ordinary_exceptions
            .saturating_add(other.source_partition_ordinary_exceptions);
        self.source_partition_failures = self
            .source_partition_failures
            .saturating_add(other.source_partition_failures);
        self.source_partition_surviving_cells = self
            .source_partition_surviving_cells
            .saturating_add(other.source_partition_surviving_cells);
        for (reason, count) in &other.fallback_reasons {
            let total = self.fallback_reasons.entry(reason.clone()).or_default();
            *total = total.saturating_add(*count);
        }
    }
}
