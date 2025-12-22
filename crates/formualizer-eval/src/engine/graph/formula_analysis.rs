use super::*;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

// Type alias for complex return type (local to analysis).
type ExtractDependenciesResult = Result<
    (
        Vec<VertexId>,
        Vec<SharedRangeRef<'static>>,
        Vec<CellRef>,
        Vec<VertexId>,
    ),
    ExcelError,
>;

impl DependencyGraph {
    // Helper methods for formula analysis / dependency extraction.

    pub(super) fn extract_dependencies(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
    ) -> ExtractDependenciesResult {
        let mut dependencies = FxHashSet::default();
        let mut range_dependencies: Vec<SharedRangeRef<'static>> = Vec::new();
        let mut created_placeholders = Vec::new();
        let mut named_dependencies = Vec::new();
        self.extract_dependencies_recursive(
            ast,
            current_sheet_id,
            &mut dependencies,
            &mut range_dependencies,
            &mut created_placeholders,
            &mut named_dependencies,
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

        Ok((
            dependencies.into_iter().collect(),
            deduped_ranges,
            created_placeholders,
            named_dependencies,
        ))
    }

    fn extract_dependencies_recursive(
        &mut self,
        ast: &ASTNode,
        current_sheet_id: SheetId,
        dependencies: &mut FxHashSet<VertexId>,
        range_dependencies: &mut Vec<SharedRangeRef<'static>>,
        created_placeholders: &mut Vec<CellRef>,
        named_dependencies: &mut Vec<VertexId>,
    ) -> Result<(), ExcelError> {
        match &ast.node_type {
            ASTNodeType::Reference { reference, .. } => match reference {
                ReferenceType::External(ext) => match ext.kind {
                    formualizer_parse::parser::ExternalRefKind::Cell { .. } => {
                        let name = ext.raw.as_str();
                        if let Some(source) = self.resolve_source_scalar_entry(name) {
                            dependencies.insert(source.vertex);
                        } else {
                            return Err(ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Undefined name: {name}")));
                        }
                    }
                    formualizer_parse::parser::ExternalRefKind::Range { .. } => {
                        let name = ext.raw.as_str();
                        if let Some(source) = self.resolve_source_table_entry(name) {
                            dependencies.insert(source.vertex);
                        } else {
                            return Err(ExcelError::new(ExcelErrorKind::Name)
                                .with_message(format!("Undefined table: {name}")));
                        }
                    }
                },
                ReferenceType::Cell { .. } => {
                    let vertex_id = self.get_or_create_vertex_for_reference(
                        reference,
                        current_sheet_id,
                        created_placeholders,
                    )?;
                    dependencies.insert(vertex_id);
                }
                ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    ..
                } => {
                    // If any bound is missing (infinite/partial range), always keep compressed.
                    let has_unbounded = start_row.is_none()
                        || end_row.is_none()
                        || start_col.is_none()
                        || end_col.is_none();
                    if has_unbounded {
                        if let Some(SharedRef::Range(range)) = reference.to_sheet_ref_lossy() {
                            let owned = range.into_owned();
                            let sheet_id = match owned.sheet {
                                SharedSheetLocator::Id(id) => id,
                                SharedSheetLocator::Current => current_sheet_id,
                                SharedSheetLocator::Name(name) => self.sheet_id_mut(name.as_ref()),
                            };
                            range_dependencies.push(SharedRangeRef {
                                sheet: SharedSheetLocator::Id(sheet_id),
                                start_row: owned.start_row,
                                start_col: owned.start_col,
                                end_row: owned.end_row,
                                end_col: owned.end_col,
                            });
                        }
                    } else {
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

                        if size <= self.config.range_expansion_limit {
                            // Expand to individual cells.
                            let sheet_id = match sheet {
                                Some(name) => self.resolve_existing_sheet_id(name)?,
                                None => current_sheet_id,
                            };
                            for row in sr..=er {
                                for col in sc..=ec {
                                    let coord = Coord::from_excel(row, col, true, true);
                                    let addr = CellRef::new(sheet_id, coord);
                                    let vertex_id =
                                        self.get_or_create_vertex(&addr, created_placeholders);
                                    dependencies.insert(vertex_id);
                                }
                            }
                        } else {
                            // Keep as a compressed range dependency.
                            if let Some(SharedRef::Range(range)) = reference.to_sheet_ref_lossy() {
                                let owned = range.into_owned();
                                let sheet_id = match owned.sheet {
                                    SharedSheetLocator::Id(id) => id,
                                    SharedSheetLocator::Current => current_sheet_id,
                                    SharedSheetLocator::Name(name) => {
                                        self.sheet_id_mut(name.as_ref())
                                    }
                                };
                                range_dependencies.push(SharedRangeRef {
                                    sheet: SharedSheetLocator::Id(sheet_id),
                                    start_row: owned.start_row,
                                    start_col: owned.start_col,
                                    end_row: owned.end_row,
                                    end_col: owned.end_col,
                                });
                            }
                        }
                    }
                }
                ReferenceType::NamedRange(name) => {
                    if let Some(named_range) = self.resolve_name_entry(name, current_sheet_id) {
                        dependencies.insert(named_range.vertex);
                        named_dependencies.push(named_range.vertex);
                    } else if let Some(source) = self.resolve_source_scalar_entry(name) {
                        dependencies.insert(source.vertex);
                    } else {
                        return Err(ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Undefined name: {name}")));
                    }
                }
                ReferenceType::Table(tref) => {
                    if let Some(table) = self.resolve_table_entry(&tref.name) {
                        dependencies.insert(table.vertex);
                    } else if let Some(source) = self.resolve_source_table_entry(&tref.name) {
                        dependencies.insert(source.vertex);
                    } else {
                        return Err(ExcelError::new(ExcelErrorKind::Name)
                            .with_message(format!("Undefined table: {}", tref.name)));
                    }
                }
            },
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.extract_dependencies_recursive(
                    left,
                    current_sheet_id,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                    named_dependencies,
                )?;
                self.extract_dependencies_recursive(
                    right,
                    current_sheet_id,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                    named_dependencies,
                )?;
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                self.extract_dependencies_recursive(
                    expr,
                    current_sheet_id,
                    dependencies,
                    range_dependencies,
                    created_placeholders,
                    named_dependencies,
                )?;
            }
            ASTNodeType::Function { args, .. } => {
                for arg in args {
                    self.extract_dependencies_recursive(
                        arg,
                        current_sheet_id,
                        dependencies,
                        range_dependencies,
                        created_placeholders,
                        named_dependencies,
                    )?;
                }
            }
            ASTNodeType::Array(rows) => {
                for row in rows {
                    for cell in row {
                        self.extract_dependencies_recursive(
                            cell,
                            current_sheet_id,
                            dependencies,
                            range_dependencies,
                            created_placeholders,
                            named_dependencies,
                        )?;
                    }
                }
            }
            ASTNodeType::Literal(_) => {}
        }
        Ok(())
    }

    /// Gets the VertexId for a reference, creating a placeholder vertex if it doesn't exist.
    fn get_or_create_vertex_for_reference(
        &mut self,
        reference: &ReferenceType,
        current_sheet_id: SheetId,
        created_placeholders: &mut Vec<CellRef>,
    ) -> Result<VertexId, ExcelError> {
        match reference {
            ReferenceType::Cell {
                sheet, row, col, ..
            } => {
                let sheet_id = match sheet {
                    Some(name) => self.resolve_existing_sheet_id(name)?,
                    None => current_sheet_id,
                };
                let coord = Coord::from_excel(*row, *col, true, true);
                let addr = CellRef::new(sheet_id, coord);
                Ok(self.get_or_create_vertex(&addr, created_placeholders))
            }
            _ => Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message("Expected a cell reference, but got a range or other type.")),
        }
    }

    #[inline]
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
            _ => false,
        }
    }
}
