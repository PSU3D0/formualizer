use super::*;
use formualizer_parse::parser::ASTNode;

// Type alias for complex return types (local to analysis).
type ExtractDependenciesResult = Result<
    (
        Vec<VertexId>,
        Vec<SharedRangeRef<'static>>,
        Vec<CellRef>,
        Vec<VertexId>,
    ),
    ExcelError,
>;

type ExtractDependenciesWithPendingNamesResult = Result<
    (
        Vec<VertexId>,
        Vec<SharedRangeRef<'static>>,
        Vec<CellRef>,
        Vec<VertexId>,
        Vec<String>,
    ),
    ExcelError,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnresolvedNamePolicy {
    Error,
    Collect,
}

struct GraphReferenceContext<'a> {
    graph: &'a mut DependencyGraph,
    current_sheet_id: SheetId,
    dependencies: &'a mut FxHashSet<VertexId>,
    range_dependencies: &'a mut Vec<SharedRangeRef<'static>>,
    created_placeholders: &'a mut Vec<CellRef>,
    named_dependencies: &'a mut Vec<VertexId>,
    unresolved_names: &'a mut FxHashSet<String>,
    unresolved_name_policy: UnresolvedNamePolicy,
}

fn graph_no_local_bindings(
    _: &GraphReferenceContext<'_>,
    _: &str,
    _: usize,
) -> crate::engine::refs::LocalBindingStyle {
    crate::engine::refs::LocalBindingStyle::None
}

fn graph_data_store<'context>(
    context: &'context GraphReferenceContext<'_>,
) -> &'context super::super::arena::DataStore {
    &context.graph.data_store
}

fn graph_sheet_registry<'context>(
    context: &'context GraphReferenceContext<'_>,
) -> &'context super::super::sheet_registry::SheetRegistry {
    &context.graph.sheet_reg
}

fn collect_graph_reference(
    context: &mut GraphReferenceContext<'_>,
    reference: crate::engine::refs::SemanticReference<'_>,
) -> Result<(), ExcelError> {
    use crate::engine::refs::SemanticReference;

    match reference {
        SemanticReference::ExternalSource(external) => match external.kind {
            formualizer_parse::parser::ExternalRefKind::Cell { .. } => {
                let name = external.raw.as_str();
                if let Some(source) = context.graph.resolve_source_scalar_entry(name) {
                    context.dependencies.insert(source.vertex);
                    Ok(())
                } else {
                    Err(ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("Undefined name: {name}")))
                }
            }
            formualizer_parse::parser::ExternalRefKind::Range { .. } => {
                let name = external.raw.as_str();
                if let Some(source) = context.graph.resolve_source_table_entry(name) {
                    context.dependencies.insert(source.vertex);
                    Ok(())
                } else {
                    Err(ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("Undefined table: {name}")))
                }
            }
        },
        SemanticReference::Cell(cell) => {
            let sheet_id = match cell.sheet.name() {
                Some(name) => context.graph.resolve_existing_sheet_id(name)?,
                None => context.current_sheet_id,
            };
            let address = CellRef::new(sheet_id, Coord::from_excel(cell.row, cell.col, true, true));
            let vertex = context
                .graph
                .get_or_create_vertex(&address, context.created_placeholders);
            context.dependencies.insert(vertex);
            Ok(())
        }
        SemanticReference::OpenRange(range) => {
            if let Some(SharedRef::Range(range)) = range.original.to_sheet_ref_lossy() {
                let owned = range.into_owned();
                // `Current` is the sheet the formula lives on.
                let sheet_id = context
                    .graph
                    .sheet_reg()
                    .resolve_locator(&owned.sheet, context.current_sheet_id)?;
                context.range_dependencies.push(SharedRangeRef {
                    sheet: SharedSheetLocator::Id(sheet_id),
                    start_row: owned.start_row,
                    start_col: owned.start_col,
                    end_row: owned.end_row,
                    end_col: owned.end_col,
                });
            }
            Ok(())
        }
        SemanticReference::FiniteRange(range) => {
            let (sr, sc, er, ec) = range
                .finite_bounds()
                .expect("finite reference must have all bounds");
            if range.is_reversed() {
                return Err(ExcelError::new(ExcelErrorKind::Ref));
            }
            let area = range.saturating_area().expect("finite area");

            // Graph ingest's configured default is 64. This policy intentionally
            // remains independent from dependency planning's historical 16.
            if area <= context.graph.config.range_expansion_limit as u64 {
                let sheet_id = match range.sheet.name() {
                    Some(name) => context.graph.resolve_existing_sheet_id(name)?,
                    None => context.current_sheet_id,
                };
                for row in sr..=er {
                    for col in sc..=ec {
                        let address =
                            CellRef::new(sheet_id, Coord::from_excel(row, col, true, true));
                        let vertex = context
                            .graph
                            .get_or_create_vertex(&address, context.created_placeholders);
                        context.dependencies.insert(vertex);
                    }
                }
            } else if let Some(SharedRef::Range(range)) = range.original.to_sheet_ref_lossy() {
                let owned = range.into_owned();
                // `Current` is the sheet the formula lives on.
                let sheet_id = context
                    .graph
                    .sheet_reg()
                    .resolve_locator(&owned.sheet, context.current_sheet_id)?;
                context.range_dependencies.push(SharedRangeRef {
                    sheet: SharedSheetLocator::Id(sheet_id),
                    start_row: owned.start_row,
                    start_col: owned.start_col,
                    end_row: owned.end_row,
                    end_col: owned.end_col,
                });
            }
            Ok(())
        }
        SemanticReference::Name(name) => {
            if let Some(named_range) = context
                .graph
                .resolve_name_entry(name, context.current_sheet_id)
            {
                context.dependencies.insert(named_range.vertex);
                context.named_dependencies.push(named_range.vertex);
            } else if let Some(source) = context.graph.resolve_source_scalar_entry(name) {
                context.dependencies.insert(source.vertex);
            } else {
                match context.unresolved_name_policy {
                    UnresolvedNamePolicy::Error => {
                        return Err(ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Undefined name: {name}")));
                    }
                    UnresolvedNamePolicy::Collect => {
                        context.unresolved_names.insert(name.to_string());
                    }
                }
            }
            Ok(())
        }
        SemanticReference::Table(table_reference) => {
            if let Some(table) = context.graph.resolve_table_entry(&table_reference.name) {
                context.dependencies.insert(table.vertex);
            } else if let Some(source) = context
                .graph
                .resolve_source_table_entry(&table_reference.name)
            {
                context.dependencies.insert(source.vertex);
            } else {
                return Err(ExcelError::new(ExcelErrorKind::Name)
                    .with_message(format!("Undefined table: {}", table_reference.name)));
            }
            Ok(())
        }
        SemanticReference::ThreeDimensional(_) | SemanticReference::Unsupported(_) => Ok(()),
    }
}
impl DependencyGraph {
    // Helper methods for formula analysis / dependency extraction.

    pub(super) fn extract_dependencies(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
    ) -> ExtractDependenciesResult {
        let (dependencies, ranges, placeholders, named_dependencies, _pending_names) =
            self.extract_dependencies_inner(ast, current_sheet_id, UnresolvedNamePolicy::Error)?;
        Ok((dependencies, ranges, placeholders, named_dependencies))
    }

    pub(super) fn extract_dependencies_with_pending_names(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
    ) -> ExtractDependenciesWithPendingNamesResult {
        self.extract_dependencies_inner(ast, current_sheet_id, UnresolvedNamePolicy::Collect)
    }

    pub(super) fn extract_dependencies_arena(
        &mut self,
        ast_id: AstNodeId,
        current_sheet_id: SheetId,
    ) -> ExtractDependenciesResult {
        let (dependencies, ranges, placeholders, named_dependencies, _pending_names) = self
            .extract_dependencies_inner_arena(
                ast_id,
                current_sheet_id,
                UnresolvedNamePolicy::Error,
            )?;
        Ok((dependencies, ranges, placeholders, named_dependencies))
    }

    pub(super) fn extract_dependencies_with_pending_names_arena(
        &mut self,
        ast_id: AstNodeId,
        current_sheet_id: SheetId,
    ) -> ExtractDependenciesWithPendingNamesResult {
        self.extract_dependencies_inner_arena(
            ast_id,
            current_sheet_id,
            UnresolvedNamePolicy::Collect,
        )
    }

    fn extract_dependencies_inner_arena(
        &mut self,
        ast_id: AstNodeId,
        current_sheet_id: SheetId,
        unresolved_name_policy: UnresolvedNamePolicy,
    ) -> ExtractDependenciesWithPendingNamesResult {
        let mut dependencies = FxHashSet::default();
        let mut range_dependencies: Vec<SharedRangeRef<'static>> = Vec::new();
        let mut created_placeholders = Vec::new();
        let mut named_dependencies = Vec::new();
        let mut unresolved_names = FxHashSet::default();
        let mut context = GraphReferenceContext {
            graph: self,
            current_sheet_id,
            dependencies: &mut dependencies,
            range_dependencies: &mut range_dependencies,
            created_placeholders: &mut created_placeholders,
            named_dependencies: &mut named_dependencies,
            unresolved_names: &mut unresolved_names,
            unresolved_name_policy,
        };
        crate::engine::refs::visit_arena_references(
            ast_id,
            &mut context,
            graph_data_store,
            graph_sheet_registry,
            collect_graph_reference,
        )?;

        // Deduplicate range references.
        let mut deduped_ranges = Vec::new();
        for range_ref in range_dependencies {
            if !deduped_ranges.contains(&range_ref) {
                deduped_ranges.push(range_ref);
            }
        }

        named_dependencies.sort_unstable_by_key(|v| v.0);
        named_dependencies.dedup_by_key(|v| v.0);

        let mut unresolved_names: Vec<String> = unresolved_names.into_iter().collect();
        unresolved_names.sort();

        Ok((
            dependencies.into_iter().collect(),
            deduped_ranges,
            created_placeholders,
            named_dependencies,
            unresolved_names,
        ))
    }

    fn extract_dependencies_inner(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
        unresolved_name_policy: UnresolvedNamePolicy,
    ) -> ExtractDependenciesWithPendingNamesResult {
        let mut dependencies = FxHashSet::default();
        let mut range_dependencies: Vec<SharedRangeRef<'static>> = Vec::new();
        let mut created_placeholders = Vec::new();
        let mut named_dependencies = Vec::new();
        let mut unresolved_names = FxHashSet::default();
        let mut context = GraphReferenceContext {
            graph: self,
            current_sheet_id,
            dependencies: &mut dependencies,
            range_dependencies: &mut range_dependencies,
            created_placeholders: &mut created_placeholders,
            named_dependencies: &mut named_dependencies,
            unresolved_names: &mut unresolved_names,
            unresolved_name_policy,
        };
        crate::engine::refs::visit_tree_references(
            ast,
            &mut context,
            graph_no_local_bindings,
            collect_graph_reference,
        )?;

        // Deduplicate range references.
        let mut deduped_ranges = Vec::new();
        for range_ref in range_dependencies {
            if !deduped_ranges.contains(&range_ref) {
                deduped_ranges.push(range_ref);
            }
        }

        named_dependencies.sort_unstable_by_key(|v| v.0);
        named_dependencies.dedup_by_key(|v| v.0);

        let mut unresolved_names: Vec<String> = unresolved_names.into_iter().collect();
        unresolved_names.sort();

        Ok((
            dependencies.into_iter().collect(),
            deduped_ranges,
            created_placeholders,
            named_dependencies,
            unresolved_names,
        ))
    }

    pub(super) fn is_ast_volatile(&self, ast: &ASTNode) -> bool {
        if ast.contains_volatile() {
            return true;
        }

        use formualizer_parse::parser::ASTNodeType;

        match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                if let Some(func) = crate::function_registry::get("", name)
                    && func.caps().contains(crate::function::FnCaps::VOLATILE)
                {
                    return true;
                }
                args.iter().any(|arg| self.is_ast_volatile(arg))
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.is_ast_volatile(left) || self.is_ast_volatile(right)
            }
            ASTNodeType::UnaryOp { expr, .. } => self.is_ast_volatile(expr),
            ASTNodeType::Array(rows) => rows
                .iter()
                .any(|row| row.iter().any(|cell| self.is_ast_volatile(cell))),
            ASTNodeType::Call { callee, args } => {
                self.is_ast_volatile(callee) || args.iter().any(|a| self.is_ast_volatile(a))
            }
            _ => false,
        }
    }

    pub fn is_ast_dynamic(&self, ast: &ASTNode) -> bool {
        use formualizer_parse::parser::ASTNodeType;

        match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                if let Some(func) = crate::function_registry::get("", name)
                    && func
                        .caps()
                        .contains(crate::function::FnCaps::DYNAMIC_DEPENDENCY)
                {
                    return true;
                }
                args.iter().any(|arg| self.is_ast_dynamic(arg))
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.is_ast_dynamic(left) || self.is_ast_dynamic(right)
            }
            ASTNodeType::UnaryOp { expr, .. } => self.is_ast_dynamic(expr),
            ASTNodeType::Array(rows) => rows
                .iter()
                .any(|row| row.iter().any(|cell| self.is_ast_dynamic(cell))),
            ASTNodeType::Call { callee, args } => {
                self.is_ast_dynamic(callee) || args.iter().any(|a| self.is_ast_dynamic(a))
            }
            _ => false,
        }
    }
}
