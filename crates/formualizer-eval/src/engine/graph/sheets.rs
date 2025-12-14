use super::ast_utils::{update_internal_sheet_references, update_sheet_references_in_ast};
use super::*;

impl DependencyGraph {
    /// Add a new sheet to the workbook.
    ///
    /// Creates a new sheet with the given name. If a sheet with this name
    /// already exists, returns its ID without error (idempotent operation).
    pub fn add_sheet(&mut self, name: &str) -> Result<SheetId, ExcelError> {
        if let Some(id) = self.sheet_reg.get_id(name) {
            return Ok(id);
        }

        let sheet_id = self.sheet_reg.id_for(name);
        self.sheet_indexes.entry(sheet_id).or_default();
        Ok(sheet_id)
    }

    /// Remove a sheet from the workbook.
    pub fn remove_sheet(&mut self, sheet_id: SheetId) -> Result<(), ExcelError> {
        if self.sheet_reg.name(sheet_id).is_empty() {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message("Sheet does not exist"));
        }

        let sheet_count = self.sheet_reg.all_sheets().len();
        if sheet_count <= 1 {
            return Err(
                ExcelError::new(ExcelErrorKind::Value).with_message("Cannot remove the last sheet")
            );
        }

        self.begin_batch();

        let vertices_to_delete: Vec<VertexId> = self.vertices_in_sheet(sheet_id).collect();

        let mut formulas_to_update = Vec::new();
        for &formula_id in self.vertex_formulas.keys() {
            let deps = self.edges.out_edges(formula_id);
            for dep_id in deps {
                if self.store.sheet_id(dep_id) == sheet_id {
                    formulas_to_update.push(formula_id);
                    break;
                }
            }
        }

        for formula_id in formulas_to_update {
            self.mark_as_ref_error(formula_id);
        }

        for vertex_id in vertices_to_delete {
            if let Some(cell_ref) = self.get_cell_ref_for_vertex(vertex_id) {
                self.cell_to_vertex.remove(&cell_ref);
            }

            self.remove_all_edges(vertex_id);

            let coord = self.store.coord(vertex_id);
            if let Some(index) = self.sheet_indexes.get_mut(&sheet_id) {
                index.remove_vertex(coord, vertex_id);
            }

            self.vertex_formulas.remove(&vertex_id);
            self.vertex_values.remove(&vertex_id);

            self.mark_deleted(vertex_id, true);
        }

        let sheet_names_to_remove: Vec<(SheetId, String)> = self
            .sheet_named_ranges
            .keys()
            .filter(|(sid, _)| *sid == sheet_id)
            .cloned()
            .collect();

        for key in sheet_names_to_remove {
            if let Some(named_range) = self.sheet_named_ranges.remove(&key) {
                self.mark_named_vertex_deleted(&named_range);
            }
        }

        self.sheet_indexes.remove(&sheet_id);

        if self.default_sheet_id == sheet_id
            && let Some(&new_default) = self.sheet_indexes.keys().next()
        {
            self.default_sheet_id = new_default;
        }

        self.sheet_reg.remove(sheet_id)?;
        self.end_batch();

        Ok(())
    }

    /// Rename an existing sheet.
    pub fn rename_sheet(&mut self, sheet_id: SheetId, new_name: &str) -> Result<(), ExcelError> {
        if new_name.is_empty() || new_name.len() > 255 {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message("Invalid sheet name"));
        }

        let old_name = self.sheet_reg.name(sheet_id);
        if old_name.is_empty() {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message("Sheet does not exist"));
        }

        if let Some(existing_id) = self.sheet_reg.get_id(new_name) {
            if existing_id != sheet_id {
                return Err(ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("Sheet '{new_name}' already exists")));
            }
            return Ok(());
        }

        let old_name = old_name.to_string();
        self.sheet_reg.rename(sheet_id, new_name)?;

        let formulas_to_update: Vec<VertexId> = self.vertex_formulas.keys().copied().collect();
        for formula_id in formulas_to_update {
            let ast_id = match self.get_formula_id(formula_id) {
                Some(ast_id) => ast_id,
                None => continue,
            };
            let ast = match self.data_store.retrieve_ast(ast_id, &self.sheet_reg) {
                Some(ast) => ast,
                None => continue,
            };

            let updated_ast = update_sheet_references_in_ast(&ast, &old_name, new_name);
            if ast != updated_ast {
                let updated_ast_id = self.data_store.store_ast(&updated_ast, &self.sheet_reg);
                self.vertex_formulas.insert(formula_id, updated_ast_id);
                self.mark_vertex_dirty(formula_id);
            }
        }

        Ok(())
    }

    /// Duplicate an existing sheet.
    pub fn duplicate_sheet(
        &mut self,
        source_sheet_id: SheetId,
        new_name: &str,
    ) -> Result<SheetId, ExcelError> {
        if new_name.is_empty() || new_name.len() > 255 {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message("Invalid sheet name"));
        }

        let source_name = self.sheet_reg.name(source_sheet_id).to_string();
        if source_name.is_empty() {
            return Err(
                ExcelError::new(ExcelErrorKind::Value).with_message("Source sheet does not exist")
            );
        }

        if self.sheet_reg.get_id(new_name).is_some() {
            return Err(ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("Sheet '{new_name}' already exists")));
        }

        let new_sheet_id = self.add_sheet(new_name)?;

        self.begin_batch();

        let source_vertices: Vec<(VertexId, AbsCoord)> = self
            .vertices_in_sheet(source_sheet_id)
            .map(|id| (id, self.store.coord(id)))
            .collect();

        let mut vertex_mapping = FxHashMap::default();

        for (old_id, coord) in &source_vertices {
            let row = coord.row();
            let col = coord.col();
            let kind = self.store.kind(*old_id);

            let new_id = self.store.allocate(*coord, new_sheet_id, 0x01);
            self.edges.add_vertex(*coord, new_id.0);
            self.sheet_index_mut(new_sheet_id)
                .add_vertex(*coord, new_id);

            self.store.set_kind(new_id, kind);

            if let Some(&value_ref) = self.vertex_values.get(old_id) {
                self.vertex_values.insert(new_id, value_ref);
            }

            vertex_mapping.insert(*old_id, new_id);

            let cell_ref = CellRef::new(new_sheet_id, Coord::new(row, col, true, true));
            self.cell_to_vertex.insert(cell_ref, new_id);
        }

        for (old_id, _) in &source_vertices {
            if let Some(&new_id) = vertex_mapping.get(old_id)
                && let Some(&ast_id) = self.vertex_formulas.get(old_id)
                && let Some(ast) = self.data_store.retrieve_ast(ast_id, &self.sheet_reg)
            {
                let updated_ast = update_internal_sheet_references(
                    &ast,
                    &source_name,
                    new_name,
                    source_sheet_id,
                    new_sheet_id,
                );

                let new_ast_id = self.data_store.store_ast(&updated_ast, &self.sheet_reg);
                self.vertex_formulas.insert(new_id, new_ast_id);

                if let Ok((deps, range_deps, _, name_vertices)) =
                    self.extract_dependencies(&updated_ast, new_sheet_id)
                {
                    let mapped_deps: Vec<VertexId> = deps
                        .iter()
                        .map(|&dep_id| vertex_mapping.get(&dep_id).copied().unwrap_or(dep_id))
                        .collect();

                    self.add_dependent_edges(new_id, &mapped_deps);
                    self.add_range_dependent_edges(new_id, &range_deps, new_sheet_id);

                    if !name_vertices.is_empty() {
                        self.attach_vertex_to_names(new_id, &name_vertices);
                    }
                }
            }
        }

        let sheet_names: Vec<(String, NamedRange)> = self
            .sheet_named_ranges
            .iter()
            .filter(|((sid, _), _)| *sid == source_sheet_id)
            .map(|((_, name), range)| (name.clone(), range.clone()))
            .collect();

        for (name, mut named_range) in sheet_names {
            named_range.scope = NameScope::Sheet(new_sheet_id);

            match &mut named_range.definition {
                NamedDefinition::Cell(cell_ref) if cell_ref.sheet_id == source_sheet_id => {
                    cell_ref.sheet_id = new_sheet_id;
                }
                NamedDefinition::Range(range_ref) => {
                    if range_ref.start.sheet_id == source_sheet_id {
                        range_ref.start.sheet_id = new_sheet_id;
                        range_ref.end.sheet_id = new_sheet_id;
                    }
                }
                _ => {}
            }

            named_range.dependents.clear();
            let name_vertex = self.allocate_name_vertex(named_range.scope);
            if matches!(named_range.definition, NamedDefinition::Range(_)) {
                self.store.set_kind(name_vertex, VertexKind::NamedArray);
            } else {
                self.store.set_kind(name_vertex, VertexKind::NamedScalar);
            }
            named_range.vertex = name_vertex;

            let referenced_names = self.rebuild_name_dependencies(
                name_vertex,
                &named_range.definition,
                named_range.scope,
            );
            if !referenced_names.is_empty() {
                self.attach_vertex_to_names(name_vertex, &referenced_names);
            }

            self.sheet_named_ranges
                .insert((new_sheet_id, name.clone()), named_range);
            self.name_vertex_lookup
                .insert(name_vertex, (NameScope::Sheet(new_sheet_id), name));
        }

        self.end_batch();

        Ok(new_sheet_id)
    }
}
