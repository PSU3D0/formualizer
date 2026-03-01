use super::ast_utils::{update_internal_sheet_references, update_sheet_references_in_ast};
use super::*;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};

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

        let orphans = self.tombstone_registry.take_orphans(name);
        for vertex_id in orphans {
            self.rebind_vertex_to_sheet(vertex_id, name);
        }
        Ok(sheet_id)
    }

    /// Remove a sheet from the workbook.
    pub fn remove_sheet(&mut self, sheet_id: SheetId) -> Result<(), ExcelError> {
        let old_name = self.sheet_reg.name(sheet_id).to_string();
        if old_name.is_empty() {
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
        for &formula_id in &formulas_to_update {
            self.tombstone_registry
                .add_orphan(old_name.clone(), formula_id);
        }

        for formula_id in formulas_to_update {
            self.mark_as_ref_error(formula_id);
        }

        // Invalidate defined names that reference the removed sheet.
        //
        // In canonical (Arrow-truth) mode, cell/formula vertices do not cache values in the graph,
        // so we cannot rely on graph-stored ref errors. We must explicitly dirty name vertices and
        // their dependents so that subsequent evaluation updates Arrow overlays.
        let ref_err = LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref));
        let mut name_vertices_to_update: Vec<VertexId> = Vec::new();
        let mut dirty_vertices: Vec<VertexId> = Vec::new();

        for nr in self.named_ranges.values_mut() {
            match &nr.definition {
                NamedDefinition::Cell(c) if c.sheet_id == sheet_id => {
                    nr.definition = NamedDefinition::Literal(ref_err.clone());
                    name_vertices_to_update.push(nr.vertex);
                    dirty_vertices.push(nr.vertex);
                    dirty_vertices.extend(nr.dependents.iter().copied());
                }
                NamedDefinition::Range(r)
                    if r.start.sheet_id == sheet_id || r.end.sheet_id == sheet_id =>
                {
                    nr.definition = NamedDefinition::Literal(ref_err.clone());
                    name_vertices_to_update.push(nr.vertex);
                    dirty_vertices.push(nr.vertex);
                    dirty_vertices.extend(nr.dependents.iter().copied());
                }
                _ => {}
            }
        }
        for nr in self.sheet_named_ranges.values_mut() {
            match &nr.definition {
                NamedDefinition::Cell(c) if c.sheet_id == sheet_id => {
                    nr.definition = NamedDefinition::Literal(ref_err.clone());
                    name_vertices_to_update.push(nr.vertex);
                    dirty_vertices.push(nr.vertex);
                    dirty_vertices.extend(nr.dependents.iter().copied());
                }
                NamedDefinition::Range(r)
                    if r.start.sheet_id == sheet_id || r.end.sheet_id == sheet_id =>
                {
                    nr.definition = NamedDefinition::Literal(ref_err.clone());
                    name_vertices_to_update.push(nr.vertex);
                    dirty_vertices.push(nr.vertex);
                    dirty_vertices.extend(nr.dependents.iter().copied());
                }
                _ => {}
            }
        }

        // Update cached values for name vertices after the map borrows end.
        for vid in name_vertices_to_update {
            self.update_vertex_value(vid, ref_err.clone());
        }
        for vid in dirty_vertices {
            self.mark_vertex_dirty(vid);
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
                if !self.config.case_sensitive_names {
                    let normalized = key.1.to_ascii_lowercase();
                    self.sheet_named_ranges_lookup
                        .remove(&(sheet_id, normalized));
                } else {
                    self.sheet_named_ranges_lookup.remove(&key);
                }
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

    /// Rebuilds a ReferenceType using a specific sheet name.
    /// This is used to "heal" #REF! errors when a sheet returns.
    fn rebuild_reference_type(&self, reference: &ReferenceType, sheet_name: &str) -> ReferenceType {
        match reference {
            ReferenceType::Cell {
                row,
                col,
                row_abs,
                col_abs,
                ..
            } => ReferenceType::Cell {
                sheet: Some(sheet_name.to_string()),
                row: *row,
                col: *col,
                row_abs: *row_abs,
                col_abs: *col_abs,
            },
            ReferenceType::Range {
                start_row,
                start_col,
                end_row,
                end_col,
                start_row_abs,
                start_col_abs,
                end_row_abs,
                end_col_abs,
                ..
            } => ReferenceType::Range {
                sheet: Some(sheet_name.to_string()),
                start_row: *start_row,
                start_col: *start_col,
                end_row: *end_row,
                end_col: *end_col,
                start_row_abs: *start_row_abs,
                start_col_abs: *start_col_abs,
                end_row_abs: *end_row_abs,
                end_col_abs: *end_col_abs,
            },
            _ => reference.clone(),
        }
    }

    /// Recursively heals an AST by replacing broken references with the new sheet context.
    fn heal_ast_references(&self, mut node: ASTNode, new_sheet_name: &str) -> ASTNode {
        match &mut node.node_type {
            ASTNodeType::Reference {
                reference,
                original,
            } => {
                // Check if the original text was broken or contains the target name
                if original.contains("#REF!") || original.contains(new_sheet_name) {
                    // Re-map the ReferenceType to point to the new sheet name
                    *reference = self.rebuild_reference_type(reference, new_sheet_name);
                    // Fix the display string
                    *original = original.replace("#REF!", new_sheet_name);
                }
            }
            ASTNodeType::Function { args, .. } => {
                for arg in args {
                    *arg = self.heal_ast_references(arg.clone(), new_sheet_name);
                }
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                **left = self.heal_ast_references(*left.clone(), new_sheet_name);
                **right = self.heal_ast_references(*right.clone(), new_sheet_name);
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                **expr = self.heal_ast_references(*expr.clone(), new_sheet_name);
            }
            _ => {}
        }
        node
    }

    fn update_ast_ids_and_names(&self, node: &mut ASTNode, new_name: &str, new_id: SheetId) {
        // 1. Update the current node if it is a reference
        if let ASTNodeType::Reference {
            original,
            reference,
        } = &mut node.node_type
        {
            // Heal the display string: "#REF!A1" -> "Sheet2!A1"
            if original.contains("#REF!") {
                *original = original.replace("#REF!", new_name);
            }

            // Heal the internal sheet name.
            // Note: ReferenceType uses String names, so we use new_name.
            match reference {
                ReferenceType::Cell { sheet, .. } => {
                    *sheet = Some(new_name.to_string());
                }
                ReferenceType::Range { sheet, .. } => {
                    *sheet = Some(new_name.to_string());
                }
                _ => {}
            }
        }

        // 2. Recurse through children using Struct Variant syntax
        match &mut node.node_type {
            ASTNodeType::Function { args, .. } => {
                for arg in args {
                    self.update_ast_ids_and_names(arg, new_name, new_id);
                }
            }
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.update_ast_ids_and_names(left, new_name, new_id);
                self.update_ast_ids_and_names(right, new_name, new_id);
            }
            ASTNodeType::UnaryOp { expr, .. } => {
                self.update_ast_ids_and_names(expr, new_name, new_id);
            }
            _ => {}
        }
    }

    fn rebind_vertex_to_sheet(&mut self, vertex_id: VertexId, sheet_name: &str) {
        // Get the ID for the target sheet (needed for dependency rebuilding)
        let new_sheet_id = self
            .sheet_reg
            .get_id(sheet_name)
            .expect("Healed sheet must exist in registry");

        // Copy the ID out to avoid borrow-checker conflicts
        if let Some(&ast_id) = self.vertex_formulas.get(&vertex_id)
            && let Some(mut ast) = self.data_store.retrieve_ast(ast_id, &self.sheet_reg) {
                // Apply the healer
                self.update_ast_ids_and_names(&mut ast, sheet_name, new_sheet_id);

                // Store the updated formula
                let new_ast_id = self.data_store.store_ast(&ast, &self.sheet_reg);
                self.vertex_formulas.insert(vertex_id, new_ast_id);

                // Re-wire the graph so the formula depends on the NEW cell vertex
                self.rebuild_formula_dependencies(vertex_id, &ast);
                self.mark_vertex_dirty(vertex_id);
            }
    }

    /// Helper to identify if an AST node depends on a specific sheet.
    /// This is used during sheet removal to find which formulas need to be "parked."
    fn ast_contains_sheet(&self, ast_id: AstNodeId, sheet_id: SheetId) -> bool {
        if let Some(ast) = self.data_store.retrieve_ast(ast_id, &self.sheet_reg) {
            // Since we don't have the method, we check the node_type directly
            // or use a utility from your parser if available.
            // For now, let's assume we need to implement a basic search:
            return self.check_ast_for_sheet(&ast, sheet_id);
        }
        false
    }

    /// Helper to find all formulas that mention a specific sheet.
    fn find_formulas_referencing_sheet(&self, sheet_id: SheetId) -> Vec<VertexId> {
        self.vertex_formulas
            .iter()
            .filter_map(|(v_id, ast_id)| {
                if self.ast_contains_sheet(*ast_id, sheet_id) {
                    Some(*v_id)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Recursively checks an AST to see if it references a specific SheetId.
    fn check_ast_for_sheet(&self, ast: &ASTNode, target_sheet_id: SheetId) -> bool {
        match &ast.node_type {
            ASTNodeType::Reference { reference, .. } => {
                match reference {
                    ReferenceType::Cell { sheet, .. } => {
                        // You'll need to resolve the Option<String> sheet name to a SheetId
                        // using your sheet_reg to compare.
                        if let Some(name) = sheet {
                            return self.sheet_reg.get_id(name) == Some(target_sheet_id);
                        }
                        false
                    }
                    ReferenceType::Range { sheet, .. } => {
                        if let Some(name) = sheet {
                            return self.sheet_reg.get_id(name) == Some(target_sheet_id);
                        }
                        false
                    }
                    _ => false,
                }
            }
            ASTNodeType::Function { args, .. } => args
                .iter()
                .any(|arg| self.check_ast_for_sheet(arg, target_sheet_id)),
            ASTNodeType::BinaryOp { left, right, .. } => {
                self.check_ast_for_sheet(left, target_sheet_id)
                    || self.check_ast_for_sheet(right, target_sheet_id)
            }
            ASTNodeType::UnaryOp { expr, .. } => self.check_ast_for_sheet(expr, target_sheet_id),
            _ => false,
        }
    }

    /// Rename an existing sheet.
    pub fn rename_sheet(&mut self, sheet_id: SheetId, new_name: &str) -> Result<(), ExcelError> {
        if new_name.is_empty() || new_name.len() > 255 {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message("Invalid sheet name"));
        }

        let old_name = self.sheet_reg.name(sheet_id).to_string();
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

        self.begin_batch();

        // 1. Perform the actual rename in the registry
        self.sheet_reg.rename(sheet_id, new_name)?;

        // 2. RESCUE TRIGGER: Does this new name heal existing #REF! errors?
        let orphans = self.tombstone_registry.take_orphans(new_name);
        for vertex_id in orphans {
            self.rebind_vertex_to_sheet(vertex_id, new_name);
        }

        // 3. UPDATE VALID REFERENCES: Update formulas pointing to the old name
        let formulas_to_update: Vec<VertexId> = self.vertex_formulas.keys().copied().collect();
        for formula_id in formulas_to_update {
            if let Some(ast_id) = self.vertex_formulas.get(&formula_id)
                && let Some(ast) = self.data_store.retrieve_ast(*ast_id, &self.sheet_reg) {
                    let updated_ast = update_sheet_references_in_ast(&ast, &old_name, new_name);
                    if ast != updated_ast {
                        let updated_ast_id =
                            self.data_store.store_ast(&updated_ast, &self.sheet_reg);
                        self.vertex_formulas.insert(formula_id, updated_ast_id);
                        self.rebuild_formula_dependencies(formula_id, &updated_ast);
                        self.mark_vertex_dirty(formula_id);
                    }
                }
        }
        // 4. CLEANUP: Clear any structural #REF! markers for this sheet's vertices
        // We collect the IDs first to avoid borrowing 'self' inside 'retain'
        let vertices_to_clear: Vec<VertexId> = self
            .ref_error_vertices
            .iter()
            .filter(|&&v_id| {
                if let Some(cell_ref) = self.get_cell_ref(v_id) {
                    cell_ref.sheet_id == sheet_id
                } else {
                    false
                }
            })
            .copied()
            .collect();

        for v_id in vertices_to_clear {
            self.ref_error_vertices.remove(&v_id);
        }

        self.end_batch();
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
