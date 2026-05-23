use std::collections::BTreeMap;
use std::sync::Arc;

use super::FormulaPlaneMode;
use super::arena::AstNodeId;

#[derive(Clone, Debug)]
pub struct FormulaIngestRecord {
    pub row: u32,
    pub col: u32,
    pub ast_id: AstNodeId,
    pub formula_text: Option<Arc<str>>,
}

impl FormulaIngestRecord {
    pub fn new(row: u32, col: u32, ast_id: AstNodeId, formula_text: Option<Arc<str>>) -> Self {
        Self {
            row,
            col,
            ast_id,
            formula_text,
        }
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
        for (reason, count) in &other.fallback_reasons {
            *self.fallback_reasons.entry(reason.clone()).or_default() += count;
        }
    }
}
