//! FP8 IngestPipeline substrate (Phase 1 stub).
//!
//! This module contains the internal type shape planned in
//! `docs/design/formula-plane/dispatch/fp8-ingest-pipeline-plan.md`.
//! Phase 1 is deliberately additive: methods are unimplemented and no existing
//! call site constructs or invokes the pipeline. Real parsing/rewriting,
//! canonical interning, dependency planning, and read-summary production arrive
//! in Phase 2+.

#![allow(dead_code)]

use crate::engine::arena::{AstNodeId, CanonicalLabels, DataStore};
use crate::engine::plan::DependencyPlan;
use crate::engine::sheet_registry::SheetRegistry;
use crate::engine::vertex::VertexId;
use crate::formula_plane::producer::SpanReadSummary;
use crate::reference::{CellRef, SharedRangeRef};
use crate::traits::FunctionProvider;
use formualizer_common::ExcelError;
use formualizer_parse::parser::{ASTNode, CollectPolicy};
use std::marker::PhantomData;
use std::sync::Arc;

/// Placeholder name-registry borrow for the Phase 1 pipeline shape.
///
/// The current engine stores name state inside `DependencyGraph`; Phase 2 will
/// replace this stub with the concrete read-only registry/view used by the real
/// pipeline implementation.
pub(crate) struct NameRegistry;

/// Placeholder table-registry borrow for the Phase 1 pipeline shape.
pub(crate) struct TableRegistry;

/// Placeholder source-registry borrow for the Phase 1 pipeline shape.
pub(crate) struct SourceRegistry;

pub(crate) struct IngestPipeline<'a> {
    data_store: &'a mut DataStore,
    sheet_registry: &'a mut SheetRegistry,
    names: &'a NameRegistry,
    tables: &'a TableRegistry,
    sources: &'a SourceRegistry,
    function_provider: &'a dyn FunctionProvider,
    policy: CollectPolicy,
}

impl<'a> IngestPipeline<'a> {
    pub(crate) fn new(
        data_store: &'a mut DataStore,
        sheet_registry: &'a mut SheetRegistry,
        names: &'a NameRegistry,
        tables: &'a TableRegistry,
        sources: &'a SourceRegistry,
        function_provider: &'a dyn FunctionProvider,
        policy: CollectPolicy,
    ) -> Self {
        Self {
            data_store,
            sheet_registry,
            names,
            tables,
            sources,
            function_provider,
            policy,
        }
    }

    pub(crate) fn ingest_formula(
        &mut self,
        _ast: FormulaAstInput<'_>,
        _placement: CellRef,
        _formula_text: Option<Arc<str>>,
    ) -> Result<IngestedFormula, ExcelError> {
        unimplemented!("FP8 Phase 1 stub; see fp8-ingest-pipeline-plan.md")
    }

    pub(crate) fn ingest_batch<'b, I>(
        &mut self,
        _formulas: I,
    ) -> Result<Vec<IngestedFormula>, ExcelError>
    where
        I: IntoIterator<Item = (FormulaAstInput<'b>, CellRef, Option<Arc<str>>)>,
    {
        unimplemented!("FP8 Phase 1 stub; see fp8-ingest-pipeline-plan.md")
    }
}

pub(crate) enum FormulaAstInput<'a> {
    Tree(ASTNode),
    RawArena(AstNodeId),
    #[doc(hidden)]
    _Lifetime(PhantomData<&'a ()>),
}

pub(crate) struct IngestedFormula {
    pub(crate) ast_id: AstNodeId,
    pub(crate) placement: CellRef,
    pub(crate) canonical_hash: u64,
    pub(crate) labels: CanonicalLabels,
    pub(crate) dep_plan: DependencyPlanRow,
    pub(crate) read_summary: Option<SpanReadSummary>,
    pub(crate) formula_text: Option<Arc<str>>,
}

pub(crate) struct DependencyPlanRow {
    pub(crate) direct_cell_deps: Vec<CellRef>,
    pub(crate) range_deps: Vec<SharedRangeRef<'static>>,
    pub(crate) created_placeholders: Vec<CellRef>,
    pub(crate) named_deps: Vec<VertexId>,
    pub(crate) unresolved_names: Vec<String>,
    pub(crate) volatile: bool,
    pub(crate) dynamic: bool,
}

impl From<DependencyPlanRow> for DependencyPlan {
    fn from(row: DependencyPlanRow) -> Self {
        let mut plan = DependencyPlan::default();
        plan.per_formula_flags.push(
            (if row.volatile {
                crate::engine::plan::F_VOLATILE
            } else {
                0
            }) | (if !row.range_deps.is_empty() {
                crate::engine::plan::F_HAS_RANGES
            } else {
                0
            }) | (if !row.unresolved_names.is_empty() || !row.named_deps.is_empty() {
                crate::engine::plan::F_HAS_NAMES
            } else {
                0
            }),
        );
        plan
    }
}
