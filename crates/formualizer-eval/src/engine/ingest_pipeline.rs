//! FP8 IngestPipeline: arena-native ingest parity path.
//!
//! The pipeline is intentionally side-effect-light: it interns/reuses formula AST
//! nodes in the engine arena, computes arena canonical metadata bottom-up, and
//! returns dependency-planning facts without adding graph edges or creating
//! vertices. Graph materialization remains in the existing graph code.

use crate::SheetId;
use crate::engine::arena::{
    AstNodeData, AstNodeId, AstNodeMetadata, CanonicalLabels, CompactRefType, DataStore, SheetKey,
    StringId, ValueRef,
};
use crate::engine::graph::DependencyGraph;
use crate::engine::plan::{DependencyPlan, F_HAS_NAMES, F_HAS_RANGES, F_HAS_TABLES, F_VOLATILE};
use crate::engine::sheet_registry::SheetRegistry;
use crate::engine::vertex::VertexId;
use crate::formula_plane::producer::{
    AxisProjection, DirtyProjectionRule, SpanReadDependency, SpanReadSummary,
};
use crate::formula_plane::region_index::RegionPattern;
use crate::formula_plane::template_canonical::{is_known_static_function, normalize_function_name};
use crate::function::FnCaps;
use crate::reference::{CellRef, Coord, RangeRef, SharedRangeRef, SharedRef, SharedSheetLocator};
use crate::traits::FunctionProvider;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{
    ASTNode, ASTNodeType, CollectPolicy, ExternalRefKind, ReferenceType, SpecialItem,
    TableSpecifier,
};
use rustc_hash::FxHashSet;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NamedEntryRef {
    pub(crate) vertex: VertexId,
}

#[derive(Clone, Debug)]
pub(crate) struct TableEntrySnapshot {
    pub(crate) name: String,
    pub(crate) range: RangeRef,
    pub(crate) header_row: bool,
    pub(crate) headers: Vec<String>,
    pub(crate) vertex: VertexId,
}

impl TableEntrySnapshot {
    fn sheet_id(&self) -> SheetId {
        self.range.start.sheet_id
    }

    fn col_index(&self, header: &str) -> Option<usize> {
        let header_key = header.to_lowercase();
        self.headers
            .iter()
            .position(|h| h.to_lowercase() == header_key)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SourceEntryRef {
    pub(crate) vertex: VertexId,
}

type NameResolveFn<'a> = dyn Fn(&str, SheetId) -> Option<NamedEntryRef> + 'a;
type TableResolveFn<'a> = dyn Fn(&str) -> Option<TableEntrySnapshot> + 'a;
type TableContainingCellFn<'a> = dyn Fn(CellRef) -> Option<TableEntrySnapshot> + 'a;
type SourceResolveFn<'a> = dyn Fn(&str) -> Option<SourceEntryRef> + 'a;

pub(crate) struct NameRegistryView<'a> {
    resolve: Box<NameResolveFn<'a>>,
}

impl<'a> NameRegistryView<'a> {
    pub(crate) fn new(resolve: impl Fn(&str, SheetId) -> Option<NamedEntryRef> + 'a) -> Self {
        Self {
            resolve: Box::new(resolve),
        }
    }

    pub(crate) fn resolve(&self, name: &str, current_sheet: SheetId) -> Option<NamedEntryRef> {
        (self.resolve)(name, current_sheet)
    }
}

pub(crate) struct TableRegistryView<'a> {
    resolve: Box<TableResolveFn<'a>>,
    containing_cell: Box<TableContainingCellFn<'a>>,
}

impl<'a> TableRegistryView<'a> {
    pub(crate) fn new(
        resolve: impl Fn(&str) -> Option<TableEntrySnapshot> + 'a,
        containing_cell: impl Fn(CellRef) -> Option<TableEntrySnapshot> + 'a,
    ) -> Self {
        Self {
            resolve: Box::new(resolve),
            containing_cell: Box::new(containing_cell),
        }
    }

    pub(crate) fn resolve(&self, name: &str) -> Option<TableEntrySnapshot> {
        (self.resolve)(name)
    }

    pub(crate) fn find_containing_cell(&self, cell: CellRef) -> Option<TableEntrySnapshot> {
        (self.containing_cell)(cell)
    }
}

pub(crate) struct SourceRegistryView<'a> {
    resolve_scalar: Box<SourceResolveFn<'a>>,
    resolve_table: Box<SourceResolveFn<'a>>,
}

impl<'a> SourceRegistryView<'a> {
    pub(crate) fn new(
        resolve_scalar: impl Fn(&str) -> Option<SourceEntryRef> + 'a,
        resolve_table: impl Fn(&str) -> Option<SourceEntryRef> + 'a,
    ) -> Self {
        Self {
            resolve_scalar: Box::new(resolve_scalar),
            resolve_table: Box::new(resolve_table),
        }
    }

    pub(crate) fn resolve_scalar(&self, name: &str) -> Option<SourceEntryRef> {
        (self.resolve_scalar)(name)
    }

    pub(crate) fn resolve_table(&self, name: &str) -> Option<SourceEntryRef> {
        (self.resolve_table)(name)
    }
}

pub(crate) struct IngestPipeline<'a> {
    data_store: &'a mut DataStore,
    sheet_registry: &'a mut SheetRegistry,
    names: NameRegistryView<'a>,
    tables: TableRegistryView<'a>,
    sources: SourceRegistryView<'a>,
    function_provider: &'a dyn FunctionProvider,
    policy: CollectPolicy,
}

impl<'a> IngestPipeline<'a> {
    pub(crate) fn new(
        data_store: &'a mut DataStore,
        sheet_registry: &'a mut SheetRegistry,
        names: NameRegistryView<'a>,
        tables: TableRegistryView<'a>,
        sources: SourceRegistryView<'a>,
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
        ast: FormulaAstInput<'_>,
        placement: CellRef,
        formula_text: Option<Arc<str>>,
    ) -> Result<IngestedFormula, ExcelError> {
        let (ast_id, ast_for_oracles) = match ast {
            FormulaAstInput::Tree(mut tree) => {
                self.rewrite_structured_references_for_cell(&mut tree, placement)?;
                let ast_id = self.data_store.store_ast(&tree, self.sheet_registry);
                (ast_id, tree)
            }
            FormulaAstInput::RawArena(id) => {
                let needs_rewrite = self.data_store.ast_needs_structural_rewrite(id);
                if needs_rewrite {
                    let mut tree = self
                        .data_store
                        .retrieve_ast(id, self.sheet_registry)
                        .ok_or_else(missing_ast_error)?;
                    self.rewrite_structured_references_for_cell(&mut tree, placement)?;
                    let rewritten_id = self.data_store.store_ast(&tree, self.sheet_registry);
                    (rewritten_id, tree)
                } else {
                    let tree = self
                        .data_store
                        .retrieve_ast(id, self.sheet_registry)
                        .ok_or_else(missing_ast_error)?;
                    (id, tree)
                }
            }
            FormulaAstInput::_Lifetime(_) => unreachable!("marker variant is not constructible"),
        };

        let metadata = compute_tree_metadata(
            &ast_for_oracles,
            self.data_store,
            self.function_provider,
            placement,
        );
        let mut dep_plan = DependencyPlanRow::default();
        let mut local_scopes = Vec::new();
        self.collect_dependencies_tree(
            &ast_for_oracles,
            placement.sheet_id,
            &mut dep_plan,
            &mut local_scopes,
        )?;
        dep_plan.volatile = self.ast_is_volatile(&ast_for_oracles);
        dep_plan.dynamic = metadata.labels.has_flag(CanonicalLabels::FLAG_DYNAMIC);
        dep_plan.dedup_and_sort();

        let read_projections = compute_read_projections(&ast_for_oracles, placement);
        let read_summary = read_projections.as_ref().and_then(|projections| {
            span_read_summary_from_projections(placement, projections).ok()
        });

        Ok(IngestedFormula {
            ast_id,
            placement,
            canonical_hash: metadata.canonical_hash,
            labels: metadata.labels,
            dep_plan,
            read_summary,
            read_projections,
            formula_text,
        })
    }

    pub(crate) fn ingest_batch<'b, I>(
        &mut self,
        formulas: I,
    ) -> Result<Vec<IngestedFormula>, ExcelError>
    where
        I: IntoIterator<Item = (FormulaAstInput<'b>, CellRef, Option<Arc<str>>)>,
    {
        let iter = formulas.into_iter();
        let (lower, _) = iter.size_hint();
        let mut out = Vec::with_capacity(lower);
        for (ast, placement, formula_text) in iter {
            out.push(self.ingest_formula(ast, placement, formula_text)?);
        }
        Ok(out)
    }

    fn ast_is_volatile(&self, ast: &ASTNode) -> bool {
        if ast.contains_volatile() {
            return true;
        }
        match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                self.function_provider
                    .get_function("", name)
                    .is_some_and(|function| function.caps().contains(FnCaps::VOLATILE))
                    || args.iter().any(|arg| self.ast_is_volatile(arg))
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.ast_is_volatile(left) || self.ast_is_volatile(right)
            }
            ASTNodeType::UnaryOp { expr, .. } => self.ast_is_volatile(expr),
            ASTNodeType::Array(rows) => rows
                .iter()
                .any(|row| row.iter().any(|cell| self.ast_is_volatile(cell))),
            ASTNodeType::Call { callee, args } => {
                self.ast_is_volatile(callee) || args.iter().any(|arg| self.ast_is_volatile(arg))
            }
            ASTNodeType::Literal(_) | ASTNodeType::Reference { .. } => false,
        }
    }

    fn collect_dependencies_tree(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
        plan: &mut DependencyPlanRow,
        local_scopes: &mut Vec<FxHashSet<String>>,
    ) -> Result<(), ExcelError> {
        match &ast.node_type {
            ASTNodeType::Reference { reference, .. } => {
                self.collect_reference(reference, current_sheet_id, plan, local_scopes)
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.collect_dependencies_tree(left, current_sheet_id, plan, local_scopes)?;
                self.collect_dependencies_tree(right, current_sheet_id, plan, local_scopes)
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                self.collect_dependencies_tree(expr, current_sheet_id, plan, local_scopes)
            }
            ASTNodeType::Function { name, args } => {
                if name.eq_ignore_ascii_case("LET") {
                    if args.len() >= 3 && args.len() % 2 == 1 {
                        local_scopes.push(FxHashSet::default());
                        for pair_idx in (0..args.len() - 1).step_by(2) {
                            self.collect_dependencies_tree(
                                &args[pair_idx + 1],
                                current_sheet_id,
                                plan,
                                local_scopes,
                            )?;
                            if let ASTNodeType::Reference {
                                reference: ReferenceType::NamedRange(local_name),
                                ..
                            } = &args[pair_idx].node_type
                                && let Some(scope) = local_scopes.last_mut()
                            {
                                scope.insert(local_name.to_ascii_uppercase());
                            }
                        }
                        self.collect_dependencies_tree(
                            &args[args.len() - 1],
                            current_sheet_id,
                            plan,
                            local_scopes,
                        )?;
                        local_scopes.pop();
                        Ok(())
                    } else {
                        for arg in args {
                            self.collect_dependencies_tree(
                                arg,
                                current_sheet_id,
                                plan,
                                local_scopes,
                            )?;
                        }
                        Ok(())
                    }
                } else if name.eq_ignore_ascii_case("LAMBDA") {
                    if let Some(body) = args.last() {
                        let mut lambda_scope = FxHashSet::default();
                        for param in &args[..args.len().saturating_sub(1)] {
                            if let ASTNodeType::Reference {
                                reference: ReferenceType::NamedRange(param_name),
                                ..
                            } = &param.node_type
                            {
                                lambda_scope.insert(param_name.to_ascii_uppercase());
                            }
                        }
                        local_scopes.push(lambda_scope);
                        self.collect_dependencies_tree(body, current_sheet_id, plan, local_scopes)?;
                        local_scopes.pop();
                    }
                    Ok(())
                } else {
                    for arg in args {
                        self.collect_dependencies_tree(arg, current_sheet_id, plan, local_scopes)?;
                    }
                    Ok(())
                }
            }
            ASTNodeType::Call { callee, args } => {
                self.collect_dependencies_tree(callee, current_sheet_id, plan, local_scopes)?;
                for arg in args {
                    self.collect_dependencies_tree(arg, current_sheet_id, plan, local_scopes)?;
                }
                Ok(())
            }
            ASTNodeType::Array(rows) => {
                for row in rows {
                    for item in row {
                        self.collect_dependencies_tree(item, current_sheet_id, plan, local_scopes)?;
                    }
                }
                Ok(())
            }
            ASTNodeType::Literal(_) => Ok(()),
        }
    }

    fn collect_reference(
        &mut self,
        reference: &ReferenceType,
        current_sheet_id: SheetId,
        plan: &mut DependencyPlanRow,
        local_scopes: &[FxHashSet<String>],
    ) -> Result<(), ExcelError> {
        match reference {
            ReferenceType::External(ext) => match ext.kind {
                ExternalRefKind::Cell { .. } => {
                    let name = ext.raw.as_str();
                    if self.sources.resolve_scalar(name).is_some() {
                        plan.source_refs.push(name.to_string());
                        Ok(())
                    } else {
                        Err(ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Undefined name: {name}")))
                    }
                }
                ExternalRefKind::Range { .. } => {
                    let name = ext.raw.as_str();
                    if self.sources.resolve_table(name).is_some() {
                        plan.source_refs.push(name.to_string());
                        Ok(())
                    } else {
                        Err(ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Undefined table: {name}")))
                    }
                }
            },
            ReferenceType::Cell {
                sheet, row, col, ..
            } => {
                let sheet_id = self.resolve_reference_sheet(sheet.as_deref(), current_sheet_id)?;
                plan.direct_cell_deps.push(CellRef::new(
                    sheet_id,
                    Coord::from_excel(*row, *col, true, true),
                ));
                Ok(())
            }
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                let has_unbounded = start_row.is_none()
                    || end_row.is_none()
                    || start_col.is_none()
                    || end_col.is_none();
                if has_unbounded {
                    if let Some(SharedRef::Range(range)) = reference.to_sheet_ref_lossy() {
                        let owned = range.into_owned();
                        let sheet_id = self.resolve_shared_sheet(owned.sheet, current_sheet_id)?;
                        plan.range_deps.push(SharedRangeRef {
                            sheet: SharedSheetLocator::Id(sheet_id),
                            start_row: owned.start_row,
                            start_col: owned.start_col,
                            end_row: owned.end_row,
                            end_col: owned.end_col,
                        });
                    }
                    return Ok(());
                }

                let sr = start_row.unwrap();
                let sc = start_col.unwrap();
                let er = end_row.unwrap();
                let ec = end_col.unwrap();
                if sr > er || sc > ec {
                    return Err(ExcelError::new(ExcelErrorKind::Ref));
                }

                let height = er.saturating_sub(sr) + 1;
                let width = ec.saturating_sub(sc) + 1;
                let size = (width * height) as usize;
                if self.policy.expand_small_ranges && size <= self.policy.range_expansion_limit {
                    let sheet_id =
                        self.resolve_reference_sheet(sheet.as_deref(), current_sheet_id)?;
                    for row in sr..=er {
                        for col in sc..=ec {
                            plan.direct_cell_deps.push(CellRef::new(
                                sheet_id,
                                Coord::from_excel(row, col, true, true),
                            ));
                        }
                    }
                } else if let Some(SharedRef::Range(range)) = reference.to_sheet_ref_lossy() {
                    let owned = range.into_owned();
                    let sheet_id = self.resolve_shared_sheet(owned.sheet, current_sheet_id)?;
                    plan.range_deps.push(SharedRangeRef {
                        sheet: SharedSheetLocator::Id(sheet_id),
                        start_row: owned.start_row,
                        start_col: owned.start_col,
                        end_row: owned.end_row,
                        end_col: owned.end_col,
                    });
                }
                Ok(())
            }
            ReferenceType::NamedRange(name) => {
                let key = name.to_ascii_uppercase();
                if local_scopes.iter().rev().any(|scope| scope.contains(&key)) {
                    return Ok(());
                }
                if self.names.resolve(name, current_sheet_id).is_some() {
                    plan.resolved_named_refs.push(name.to_string());
                } else if self.sources.resolve_scalar(name).is_some() {
                    plan.source_refs.push(name.to_string());
                } else {
                    plan.named_refs.push(name.to_string());
                }
                Ok(())
            }
            ReferenceType::Table(tref) => {
                if self.tables.resolve(&tref.name).is_some() {
                    plan.table_refs.push(tref.name.clone());
                    Ok(())
                } else if self.sources.resolve_table(&tref.name).is_some() {
                    plan.source_refs.push(tref.name.clone());
                    Ok(())
                } else {
                    Err(ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("Undefined table: {}", tref.name)))
                }
            }
            ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => Ok(()),
        }
    }

    fn resolve_reference_sheet(
        &mut self,
        sheet: Option<&str>,
        current_sheet_id: SheetId,
    ) -> Result<SheetId, ExcelError> {
        match sheet {
            Some(name) => self.sheet_registry.get_id(name).ok_or_else(|| {
                ExcelError::new(ExcelErrorKind::Ref)
                    .with_message(format!("Sheet not found: {name}"))
            }),
            None => Ok(current_sheet_id),
        }
    }

    fn resolve_shared_sheet(
        &mut self,
        sheet: SharedSheetLocator<'static>,
        current_sheet_id: SheetId,
    ) -> Result<SheetId, ExcelError> {
        match sheet {
            SharedSheetLocator::Id(id) => Ok(id),
            SharedSheetLocator::Current => Ok(current_sheet_id),
            SharedSheetLocator::Name(name) => Ok(self.sheet_registry.id_for(name.as_ref())),
        }
    }

    fn rewrite_structured_references_for_cell(
        &self,
        ast: &mut ASTNode,
        cell: CellRef,
    ) -> Result<bool, ExcelError> {
        self.rewrite_structured_references_node(ast, cell)
    }

    fn rewrite_structured_references_node(
        &self,
        node: &mut ASTNode,
        cell: CellRef,
    ) -> Result<bool, ExcelError> {
        match &mut node.node_type {
            ASTNodeType::Reference { reference, .. } => {
                self.rewrite_structured_reference(reference, cell)
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                self.rewrite_structured_references_node(expr, cell)
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                let left_rewritten = self.rewrite_structured_references_node(left, cell)?;
                let right_rewritten = self.rewrite_structured_references_node(right, cell)?;
                Ok(left_rewritten || right_rewritten)
            }
            ASTNodeType::Function { args, .. } => {
                let mut rewritten = false;
                for arg in args {
                    rewritten |= self.rewrite_structured_references_node(arg, cell)?;
                }
                Ok(rewritten)
            }
            ASTNodeType::Call { callee, args } => {
                let mut rewritten = self.rewrite_structured_references_node(callee, cell)?;
                for arg in args {
                    rewritten |= self.rewrite_structured_references_node(arg, cell)?;
                }
                Ok(rewritten)
            }
            ASTNodeType::Array(rows) => {
                let mut rewritten = false;
                for row in rows {
                    for item in row {
                        rewritten |= self.rewrite_structured_references_node(item, cell)?;
                    }
                }
                Ok(rewritten)
            }
            ASTNodeType::Literal(_) => Ok(false),
        }
    }

    fn rewrite_structured_reference(
        &self,
        reference: &mut ReferenceType,
        cell: CellRef,
    ) -> Result<bool, ExcelError> {
        let ReferenceType::Table(tref) = reference else {
            return Ok(false);
        };
        if !tref.name.is_empty() {
            return Ok(false);
        }

        let col_name = match &tref.specifier {
            Some(TableSpecifier::Combination(parts)) => {
                let mut saw_this_row = false;
                let mut col: Option<&str> = None;
                for part in parts {
                    match part.as_ref() {
                        TableSpecifier::SpecialItem(SpecialItem::ThisRow) => saw_this_row = true,
                        TableSpecifier::Column(c) => {
                            if col.is_some() {
                                return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                    "This-row structured reference with multiple columns is not supported".to_string(),
                                ));
                            }
                            col = Some(c.as_str());
                        }
                        other => {
                            return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                                format!(
                                    "Unsupported this-row structured reference component: {other}"
                                ),
                            ));
                        }
                    }
                }
                if !saw_this_row {
                    return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "Unnamed structured reference requires a this-row selector".to_string(),
                    ));
                }
                col.ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::NImpl).with_message(
                        "This-row structured reference missing column selector".to_string(),
                    )
                })?
            }
            _ => {
                return Err(ExcelError::new(ExcelErrorKind::NImpl).with_message(
                    "Unnamed structured reference form is not supported".to_string(),
                ));
            }
        };

        let Some(table) = self.tables.find_containing_cell(cell) else {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message("This-row structured reference used outside a table".to_string()));
        };

        let row0 = cell.coord.row();
        let col0 = cell.coord.col();
        let sr0 = table.range.start.coord.row();
        let sc0 = table.range.start.coord.col();
        let er0 = table.range.end.coord.row();
        let ec0 = table.range.end.coord.col();

        if table.sheet_id() != cell.sheet_id || row0 < sr0 || row0 > er0 || col0 < sc0 || col0 > ec0
        {
            return Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message("This-row structured reference used outside a table".to_string()));
        }
        if table.header_row && row0 == sr0 {
            return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                "This-row structured references are not valid in the table header row".to_string(),
            ));
        }
        let data_start = if table.header_row { sr0 + 1 } else { sr0 };
        if row0 < data_start {
            return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(
                "This-row structured references require a data/totals row context".to_string(),
            ));
        }

        let Some(idx) = table.col_index(col_name) else {
            return Err(ExcelError::new(ExcelErrorKind::Ref).with_message(format!(
                "Unknown table column in this-row reference: {col_name}"
            )));
        };
        *reference = ReferenceType::Cell {
            sheet: None,
            row: row0 + 1,
            col: sc0 + idx as u32 + 1,
            row_abs: true,
            col_abs: true,
        };
        Ok(true)
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
    pub(crate) read_projections: Option<Vec<DirtyProjectionRule>>,
    pub(crate) formula_text: Option<Arc<str>>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct DependencyPlanRow {
    pub(crate) direct_cell_deps: Vec<CellRef>,
    pub(crate) range_deps: Vec<SharedRangeRef<'static>>,
    pub(crate) named_refs: Vec<String>,
    pub(crate) table_refs: Vec<String>,
    pub(crate) source_refs: Vec<String>,
    pub(crate) external_refs: Vec<String>,
    pub(crate) volatile: bool,
    pub(crate) dynamic: bool,
    pub(crate) resolved_named_refs: Vec<String>,
}

impl DependencyPlanRow {
    fn dedup_and_sort(&mut self) {
        self.direct_cell_deps.sort();
        self.direct_cell_deps.dedup();
        dedup_strings(&mut self.named_refs);
        dedup_strings(&mut self.table_refs);
        dedup_strings(&mut self.source_refs);
        dedup_strings(&mut self.external_refs);
        dedup_strings(&mut self.resolved_named_refs);
        let mut ranges = Vec::new();
        for range in self.range_deps.drain(..) {
            if !ranges.contains(&range) {
                ranges.push(range);
            }
        }
        self.range_deps = ranges;
    }
}

impl From<DependencyPlanRow> for DependencyPlan {
    fn from(row: DependencyPlanRow) -> Self {
        let mut plan = DependencyPlan::default();
        plan.per_formula_flags.push(
            (if row.volatile { F_VOLATILE } else { 0 })
                | (if !row.range_deps.is_empty() {
                    F_HAS_RANGES
                } else {
                    0
                })
                | (if !row.named_refs.is_empty() || !row.resolved_named_refs.is_empty() {
                    F_HAS_NAMES
                } else {
                    0
                })
                | (if !row.table_refs.is_empty() {
                    F_HAS_TABLES
                } else {
                    0
                }),
        );
        plan.per_formula_names.push(row.named_refs);
        plan.per_formula_tables.push(row.table_refs);
        plan
    }
}

fn dedup_strings(values: &mut Vec<String>) {
    values.sort();
    values.dedup();
}

fn compute_read_projections(ast: &ASTNode, placement: CellRef) -> Option<Vec<DirtyProjectionRule>> {
    fn visit(
        ast: &ASTNode,
        placement: CellRef,
        projections: &mut Vec<DirtyProjectionRule>,
    ) -> Option<()> {
        match &ast.node_type {
            ASTNodeType::Literal(_) => Some(()),
            ASTNodeType::Reference { reference, .. } => {
                let ReferenceType::Cell {
                    sheet,
                    row,
                    col,
                    row_abs,
                    col_abs,
                } = reference
                else {
                    return None;
                };
                if sheet.is_some() {
                    return None;
                }
                let anchor_row = placement.coord.row() as i64 + 1;
                let anchor_col = placement.coord.col() as i64 + 1;
                let row_projection = if *row_abs {
                    AxisProjection::Absolute {
                        index: row.saturating_sub(1),
                    }
                } else {
                    AxisProjection::Relative {
                        offset: i64::from(*row) - anchor_row,
                    }
                };
                let col_projection = if *col_abs {
                    AxisProjection::Absolute {
                        index: col.saturating_sub(1),
                    }
                } else {
                    AxisProjection::Relative {
                        offset: i64::from(*col) - anchor_col,
                    }
                };
                let projection = DirtyProjectionRule::AffineCell {
                    row: row_projection,
                    col: col_projection,
                };
                if !projections.contains(&projection) {
                    projections.push(projection);
                }
                Some(())
            }
            ASTNodeType::UnaryOp { op, expr } => match op.as_str() {
                "+" | "-" | "%" => visit(expr, placement, projections),
                _ => None,
            },
            ASTNodeType::BinaryOp { op, left, right } => {
                if !matches!(
                    op.as_str(),
                    "+" | "-" | "*" | "/" | "^" | "&" | "=" | "<>" | "<" | "<=" | ">" | ">="
                ) {
                    return None;
                }
                visit(left, placement, projections)?;
                visit(right, placement, projections)
            }
            ASTNodeType::Function { name, args } => {
                let canonical_name = normalize_function_name(name);
                if !is_known_static_function(&canonical_name) {
                    return None;
                }
                for arg in args {
                    visit(arg, placement, projections)?;
                }
                Some(())
            }
            ASTNodeType::Call { .. } | ASTNodeType::Array(_) => None,
        }
    }

    let mut projections = Vec::new();
    visit(ast, placement, &mut projections)?;
    Some(projections)
}

pub(crate) fn span_read_summary_from_projections(
    placement: CellRef,
    projections: &[DirtyProjectionRule],
) -> Result<SpanReadSummary, crate::formula_plane::producer::ProjectionFallbackReason> {
    let result_region = RegionPattern::col_interval(
        placement.sheet_id,
        placement.coord.col(),
        placement.coord.row(),
        placement.coord.row(),
    );
    let mut dependencies = Vec::new();
    for &projection in projections {
        let read_region = projection.read_region_for_result(placement.sheet_id, result_region)?;
        let dependency = SpanReadDependency {
            read_region,
            projection,
        };
        if !dependencies.contains(&dependency) {
            dependencies.push(dependency);
        }
    }
    Ok(SpanReadSummary {
        result_region,
        dependencies,
    })
}

fn missing_ast_error() -> ExcelError {
    ExcelError::new(ExcelErrorKind::Value).with_message("Missing interned formula AST")
}

fn compute_tree_metadata(
    ast: &ASTNode,
    data_store: &DataStore,
    function_provider: &dyn FunctionProvider,
    placement: CellRef,
) -> AstNodeMetadata {
    fn visit(
        ast: &ASTNode,
        data_store: &DataStore,
        function_provider: &dyn FunctionProvider,
        placement: CellRef,
    ) -> AstNodeMetadata {
        let (mut data, child_metadata) = match &ast.node_type {
            ASTNodeType::Literal(value) => (
                AstNodeData::Literal(canonical_literal_value_ref(value.clone())),
                Vec::new(),
            ),
            ASTNodeType::Reference {
                original,
                reference,
            } => (
                AstNodeData::Reference {
                    original_id: ast_string_id(data_store, original),
                    ref_type: compact_ref_type_from_ast(reference, data_store),
                },
                Vec::new(),
            ),
            ASTNodeType::UnaryOp { op, expr } => (
                AstNodeData::UnaryOp {
                    op_id: ast_string_id(data_store, op),
                    expr_id: AstNodeId::from_u32(0),
                },
                vec![visit(expr, data_store, function_provider, placement)],
            ),
            ASTNodeType::BinaryOp { op, left, right } => (
                AstNodeData::BinaryOp {
                    op_id: ast_string_id(data_store, op),
                    left_id: AstNodeId::from_u32(0),
                    right_id: AstNodeId::from_u32(0),
                },
                vec![
                    visit(left, data_store, function_provider, placement),
                    visit(right, data_store, function_provider, placement),
                ],
            ),
            ASTNodeType::Function { name, args } => (
                AstNodeData::Function {
                    name_id: ast_string_id(data_store, name),
                    args_offset: 0,
                    args_count: args.len() as u16,
                },
                args.iter()
                    .map(|arg| visit(arg, data_store, function_provider, placement))
                    .collect(),
            ),
            ASTNodeType::Array(rows) => {
                let child_metadata = rows
                    .iter()
                    .flat_map(|row| row.iter())
                    .map(|cell| visit(cell, data_store, function_provider, placement))
                    .collect();
                (
                    AstNodeData::Array {
                        rows: rows.len() as u16,
                        cols: rows.first().map(|row| row.len()).unwrap_or(0) as u16,
                        elements_offset: 0,
                    },
                    child_metadata,
                )
            }
            ASTNodeType::Call { callee, args } => {
                let mut child_metadata = Vec::with_capacity(args.len() + 1);
                child_metadata.push(visit(callee, data_store, function_provider, placement));
                child_metadata.extend(
                    args.iter()
                        .map(|arg| visit(arg, data_store, function_provider, placement)),
                );
                (
                    AstNodeData::Function {
                        name_id: StringId::INVALID,
                        args_offset: 0,
                        args_count: child_metadata.len() as u16,
                    },
                    child_metadata,
                )
            }
        };

        normalize_node_for_canonical_metadata(&mut data, data_store, placement);
        let child_refs: Vec<&AstNodeMetadata> = child_metadata.iter().collect();
        let mut metadata = crate::engine::arena::canonical::compute_node_metadata(
            &data,
            &child_refs,
            data_store.ast_strings(),
            function_provider,
        );
        if matches!(ast.node_type, ASTNodeType::Call { .. }) {
            metadata.labels = CanonicalLabels::default();
            for child in &child_metadata {
                metadata.labels.flags |= child.labels.flags;
                metadata.labels.rejects |= child.labels.rejects;
            }
            metadata.labels.rejects |= CanonicalLabels::REJECT_CALL_EXPRESSION;
        }
        metadata
    }

    visit(ast, data_store, function_provider, placement)
}

fn ast_string_id(data_store: &DataStore, value: &str) -> StringId {
    data_store
        .ast_strings()
        .get_id(value)
        .unwrap_or(StringId::INVALID)
}

fn compact_ref_type_from_ast(reference: &ReferenceType, data_store: &DataStore) -> CompactRefType {
    match reference {
        ReferenceType::Cell {
            sheet,
            row,
            col,
            row_abs,
            col_abs,
        } => CompactRefType::Cell {
            sheet: sheet
                .as_ref()
                .map(|sheet| SheetKey::Name(ast_string_id(data_store, sheet))),
            row: *row,
            col: *col,
            row_abs: *row_abs,
            col_abs: *col_abs,
        },
        ReferenceType::Range {
            sheet,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => CompactRefType::Range {
            sheet: sheet
                .as_ref()
                .map(|sheet| SheetKey::Name(ast_string_id(data_store, sheet))),
            start_row: start_row.unwrap_or(0),
            start_col: start_col.unwrap_or(0),
            end_row: end_row.unwrap_or(u32::MAX),
            end_col: end_col.unwrap_or(u32::MAX),
            start_row_abs: *start_row_abs,
            start_col_abs: *start_col_abs,
            end_row_abs: *end_row_abs,
            end_col_abs: *end_col_abs,
        },
        ReferenceType::External(ext) => CompactRefType::External {
            raw_id: ast_string_id(data_store, &ext.raw),
            book_id: ast_string_id(data_store, ext.book.token()),
            sheet_id: ast_string_id(data_store, &ext.sheet),
            kind: ext.kind,
        },
        ReferenceType::NamedRange(name) => {
            CompactRefType::NamedRange(ast_string_id(data_store, name))
        }
        ReferenceType::Table(table) => CompactRefType::Table {
            name_id: ast_string_id(data_store, &table.name),
            specifier_id: None,
        },
        ReferenceType::Cell3D {
            sheet_first,
            sheet_last,
            row,
            col,
            row_abs,
            col_abs,
        } => CompactRefType::Cell3D {
            sheet_first: ast_string_id(data_store, sheet_first),
            sheet_last: ast_string_id(data_store, sheet_last),
            row: *row,
            col: *col,
            row_abs: *row_abs,
            col_abs: *col_abs,
        },
        ReferenceType::Range3D {
            sheet_first,
            sheet_last,
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
        } => CompactRefType::Range3D {
            sheet_first: ast_string_id(data_store, sheet_first),
            sheet_last: ast_string_id(data_store, sheet_last),
            start_row: start_row.unwrap_or(0),
            start_col: start_col.unwrap_or(0),
            end_row: end_row.unwrap_or(u32::MAX),
            end_col: end_col.unwrap_or(u32::MAX),
            start_row_abs: *start_row_abs,
            start_col_abs: *start_col_abs,
            end_row_abs: *end_row_abs,
            end_col_abs: *end_col_abs,
        },
    }
}

fn normalize_node_for_canonical_metadata(
    node: &mut AstNodeData,
    _data_store: &DataStore,
    placement: CellRef,
) {
    match node {
        AstNodeData::Literal(_) => {}
        AstNodeData::Reference { ref_type, .. } => normalize_reference_axes(ref_type, placement),
        AstNodeData::UnaryOp { .. }
        | AstNodeData::BinaryOp { .. }
        | AstNodeData::Function { .. }
        | AstNodeData::Array { .. } => {}
    }
}

fn normalize_reference_axes(ref_type: &mut CompactRefType, placement: CellRef) {
    let anchor_row = placement.coord.row() + 1;
    let anchor_col = placement.coord.col() + 1;
    match ref_type {
        CompactRefType::Cell {
            row,
            col,
            row_abs,
            col_abs,
            ..
        } => {
            normalize_finite_axis(row, anchor_row, *row_abs);
            normalize_finite_axis(col, anchor_col, *col_abs);
        }
        CompactRefType::Range {
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
            ..
        } => {
            normalize_start_axis(start_row, anchor_row, *start_row_abs);
            normalize_start_axis(start_col, anchor_col, *start_col_abs);
            normalize_end_axis(end_row, anchor_row, *end_row_abs);
            normalize_end_axis(end_col, anchor_col, *end_col_abs);
        }
        CompactRefType::Cell3D {
            row,
            col,
            row_abs,
            col_abs,
            ..
        } => {
            normalize_finite_axis(row, anchor_row, *row_abs);
            normalize_finite_axis(col, anchor_col, *col_abs);
        }
        CompactRefType::Range3D {
            start_row,
            start_col,
            end_row,
            end_col,
            start_row_abs,
            start_col_abs,
            end_row_abs,
            end_col_abs,
            ..
        } => {
            normalize_start_axis(start_row, anchor_row, *start_row_abs);
            normalize_start_axis(start_col, anchor_col, *start_col_abs);
            normalize_end_axis(end_row, anchor_row, *end_row_abs);
            normalize_end_axis(end_col, anchor_col, *end_col_abs);
        }
        CompactRefType::External { kind, .. } => match kind {
            ExternalRefKind::Cell {
                row,
                col,
                row_abs,
                col_abs,
            } => {
                normalize_finite_axis(row, anchor_row, *row_abs);
                normalize_finite_axis(col, anchor_col, *col_abs);
            }
            ExternalRefKind::Range {
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
            } => {
                normalize_optional_axis(start_row, anchor_row, *start_row_abs);
                normalize_optional_axis(start_col, anchor_col, *start_col_abs);
                normalize_optional_axis(end_row, anchor_row, *end_row_abs);
                normalize_optional_axis(end_col, anchor_col, *end_col_abs);
            }
        },
        CompactRefType::NamedRange(_) | CompactRefType::Table { .. } => {}
    }
}

fn normalize_start_axis(value: &mut u32, anchor: u32, absolute: bool) {
    if *value != 0 {
        normalize_finite_axis(value, anchor, absolute);
    }
}

fn normalize_end_axis(value: &mut u32, anchor: u32, absolute: bool) {
    if *value != u32::MAX {
        normalize_finite_axis(value, anchor, absolute);
    }
}

fn normalize_optional_axis(value: &mut Option<u32>, anchor: u32, absolute: bool) {
    if let Some(value) = value {
        normalize_finite_axis(value, anchor, absolute);
    }
}

fn normalize_finite_axis(value: &mut u32, anchor: u32, absolute: bool) {
    if !absolute {
        *value = ((i64::from(*value) - i64::from(anchor)) as i32 as u32) ^ 0x8000_0000;
    }
}

fn canonical_literal_value_ref(value: LiteralValue) -> ValueRef {
    match value {
        LiteralValue::Empty => ValueRef::empty(),
        LiteralValue::Boolean(value) => ValueRef::boolean(value),
        LiteralValue::Int(value) => i32::try_from(value)
            .ok()
            .and_then(ValueRef::small_int)
            .unwrap_or_else(|| {
                ValueRef::large_int(fnv1a_literal_payload(b"int", &value.to_le_bytes()))
            }),
        LiteralValue::Number(value) => ValueRef::number(fnv1a_literal_payload(
            b"number",
            &value.to_bits().to_le_bytes(),
        )),
        LiteralValue::Text(value) => {
            ValueRef::string(fnv1a_literal_payload(b"text", value.as_bytes()))
        }
        LiteralValue::Error(error) => ValueRef::error(fnv1a_literal_payload(
            b"error",
            error.to_string().as_bytes(),
        )),
        LiteralValue::Array(array) => ValueRef::array(fnv1a_literal_payload(
            b"array",
            format!("{array:?}").as_bytes(),
        )),
        LiteralValue::Date(value) => {
            ValueRef::date_time(fnv1a_literal_payload(b"date", value.to_string().as_bytes()))
        }
        LiteralValue::DateTime(value) => ValueRef::date_time(fnv1a_literal_payload(
            b"datetime",
            value.to_string().as_bytes(),
        )),
        LiteralValue::Time(value) => {
            ValueRef::date_time(fnv1a_literal_payload(b"time", value.to_string().as_bytes()))
        }
        LiteralValue::Duration(value) => ValueRef::duration(fnv1a_literal_payload(
            b"duration",
            &value
                .num_nanoseconds()
                .unwrap_or(value.num_seconds())
                .to_le_bytes(),
        )),
        LiteralValue::Pending => ValueRef::pending(),
    }
}

fn fnv1a_literal_payload(tag: &[u8], bytes: &[u8]) -> u32 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in tag.iter().chain(bytes) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash as u32 ^ (hash >> 32) as u32) & 0x0fff_ffff
}

impl DependencyGraph {
    pub(crate) fn ingest_pipeline<'a>(
        &'a mut self,
        function_provider: &'a dyn FunctionProvider,
    ) -> IngestPipeline<'a> {
        let range_expansion_limit = self.range_expansion_limit();
        let policy = CollectPolicy {
            expand_small_ranges: true,
            range_expansion_limit,
            include_names: true,
        };
        self.ingest_pipeline_with_policy(function_provider, policy)
    }

    pub(crate) fn ingest_pipeline_with_policy<'a>(
        &'a mut self,
        function_provider: &'a dyn FunctionProvider,
        policy: CollectPolicy,
    ) -> IngestPipeline<'a> {
        self.make_ingest_pipeline(function_provider, policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{Engine, EvalConfig};
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::parse;

    #[test]
    fn engine_constructs_and_runs_ingest_pipeline() {
        let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        let sheet = engine.graph.sheet_id_mut("Sheet1");
        let placement = CellRef::new(sheet, Coord::from_excel(1, 1, true, true));
        let ast = parse("=A1+1").unwrap();
        let mut pipeline = engine.ingest_pipeline();
        let ingested = pipeline
            .ingest_formula(FormulaAstInput::Tree(ast), placement, None)
            .unwrap();
        assert_eq!(ingested.placement, placement);
        assert_ne!(ingested.canonical_hash, 0);
    }
}
