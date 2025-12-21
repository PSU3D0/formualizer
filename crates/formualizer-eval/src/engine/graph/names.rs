use super::*;
use formualizer_common::parse_a1_1based;

/// Validate that a name conforms to Excel naming rules.
fn is_valid_excel_name(name: &str) -> bool {
    // Excel name rules:
    // 1. Must start with a letter, underscore, or backslash
    // 2. Can contain letters, numbers, periods, and underscores
    // 3. Cannot be a cell reference (like A1, B2, etc.)
    // 4. Cannot exceed 255 characters
    // 5. Cannot contain spaces

    if name.is_empty() || name.len() > 255 {
        return false;
    }

    if parse_a1_1based(name).is_ok() {
        return false;
    }

    let mut chars = name.chars();

    // First character must be letter, underscore, or backslash
    if let Some(first) = chars.next()
        && !first.is_alphabetic()
        && first != '_'
        && first != '\\'
    {
        return false;
    }

    // Remaining characters must be letters, digits, periods, or underscores
    for c in chars {
        if !c.is_alphanumeric() && c != '.' && c != '_' {
            return false;
        }
    }

    true
}

/// Helper function to adjust a named definition during structural operations.
fn adjust_named_definition(
    definition: &mut NamedDefinition,
    adjuster: &crate::engine::graph::editor::reference_adjuster::ReferenceAdjuster,
    operation: &crate::engine::graph::editor::reference_adjuster::ShiftOperation,
) -> Result<(), ExcelError> {
    match definition {
        NamedDefinition::Cell(cell_ref) => {
            if let Some(adjusted) = adjuster.adjust_cell_ref(cell_ref, operation) {
                *cell_ref = adjusted;
            } else {
                return Err(ExcelError::new(ExcelErrorKind::Ref));
            }
        }
        NamedDefinition::Range(range_ref) => {
            let adjusted_start = adjuster.adjust_cell_ref(&range_ref.start, operation);
            let adjusted_end = adjuster.adjust_cell_ref(&range_ref.end, operation);

            if let (Some(start), Some(end)) = (adjusted_start, adjusted_end) {
                range_ref.start = start;
                range_ref.end = end;
            } else {
                return Err(ExcelError::new(ExcelErrorKind::Ref));
            }
        }
        NamedDefinition::Formula {
            ast,
            dependencies,
            range_deps,
        } => {
            let adjusted_ast = adjuster.adjust_ast(ast, operation);
            *ast = adjusted_ast;

            dependencies.clear();
            range_deps.clear();
        }
    }
    Ok(())
}

impl DependencyGraph {
    fn next_name_coord(&mut self) -> AbsCoord {
        let seq = self.name_vertex_seq;
        self.name_vertex_seq = self.name_vertex_seq.wrapping_add(1);
        let row = (seq / 16_384).min(0x000F_FFFF);
        let col = seq % 16_384;
        AbsCoord::new(row, col)
    }

    pub(super) fn allocate_name_vertex(&mut self, scope: NameScope) -> VertexId {
        let coord = self.next_name_coord();
        let sheet_id = match scope {
            NameScope::Sheet(id) => id,
            NameScope::Workbook => self.default_sheet_id,
        };
        let vertex_id = self.store.allocate(coord, sheet_id, 0x01);
        self.store.set_kind(vertex_id, VertexKind::NamedScalar);
        self.store.set_dirty(vertex_id, true);
        self.edges.add_vertex(coord, vertex_id.0);
        self.dirty_vertices.insert(vertex_id);
        vertex_id
    }

    // Named Range Methods

    /// Define a new named range
    pub fn define_name(
        &mut self,
        name: &str,
        definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), ExcelError> {
        // Validate name
        if !is_valid_excel_name(name) {
            return Err(
                ExcelError::new(ExcelErrorKind::Name).with_message(format!("Invalid name: {name}"))
            );
        }

        // Check for duplicates
        match scope {
            NameScope::Workbook => {
                if self.named_ranges.contains_key(name) {
                    return Err(ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("Name already exists: {name}")));
                }
            }
            NameScope::Sheet(sheet_id) => {
                if self
                    .sheet_named_ranges
                    .contains_key(&(sheet_id, name.to_string()))
                {
                    return Err(ExcelError::new(ExcelErrorKind::Name)
                        .with_message(format!("Name already exists in sheet: {name}")));
                }
            }
        }

        let mut final_definition = definition;
        // Extract dependencies if formula
        if let NamedDefinition::Formula { ref ast, .. } = final_definition {
            let (deps, range_deps, _, _) = self.extract_dependencies(
                ast,
                match scope {
                    NameScope::Sheet(id) => id,
                    NameScope::Workbook => self.default_sheet_id,
                },
            )?;
            final_definition = NamedDefinition::Formula {
                ast: ast.clone(),
                dependencies: deps,
                range_deps,
            };
        }

        // Allocate vertex only after dependency extraction succeeds
        let vertex_id = self.allocate_name_vertex(scope);

        let named_range = NamedRange {
            definition: final_definition,
            scope,
            dependents: FxHashSet::default(),
            vertex: vertex_id,
        };

        if matches!(named_range.definition, NamedDefinition::Range(_)) {
            self.store.set_kind(vertex_id, VertexKind::NamedArray);
        } else {
            self.store.set_kind(vertex_id, VertexKind::NamedScalar);
        }

        let referenced_names =
            self.rebuild_name_dependencies(vertex_id, &named_range.definition, scope);
        if !referenced_names.is_empty() {
            self.attach_vertex_to_names(vertex_id, &referenced_names);
        }

        let key = name.to_string();

        match scope {
            NameScope::Workbook => {
                self.named_ranges.insert(key.clone(), named_range);
            }
            NameScope::Sheet(id) => {
                self.sheet_named_ranges
                    .insert((id, key.clone()), named_range);
            }
        }

        self.name_vertex_lookup.insert(vertex_id, (scope, key));
        self.resolve_pending_name_references(scope, name, vertex_id);

        Ok(())
    }

    /// Iterate workbook-scoped named ranges (for bindings/testing)
    pub fn named_ranges_iter(&self) -> impl Iterator<Item = (&String, &NamedRange)> {
        self.named_ranges.iter()
    }

    /// Iterate sheet-scoped named ranges (for bindings/testing)
    pub fn sheet_named_ranges_iter(
        &self,
    ) -> impl Iterator<Item = (&(SheetId, String), &NamedRange)> {
        self.sheet_named_ranges.iter()
    }

    pub fn resolve_name_entry(&self, name: &str, current_sheet: SheetId) -> Option<&NamedRange> {
        self.sheet_named_ranges
            .get(&(current_sheet, name.to_string()))
            .or_else(|| self.named_ranges.get(name))
    }

    /// Resolve a named range to its definition
    pub fn resolve_name(&self, name: &str, current_sheet: SheetId) -> Option<&NamedDefinition> {
        self.resolve_name_entry(name, current_sheet)
            .map(|nr| &nr.definition)
    }

    pub fn named_range_by_vertex(&self, vertex: VertexId) -> Option<&NamedRange> {
        self.name_vertex_lookup
            .get(&vertex)
            .and_then(|(scope, name)| match scope {
                NameScope::Workbook => self.named_ranges.get(name),
                NameScope::Sheet(sheet_id) => {
                    self.sheet_named_ranges.get(&(*sheet_id, name.clone()))
                }
            })
    }

    /// Update an existing named range definition
    pub fn update_name(
        &mut self,
        name: &str,
        new_definition: NamedDefinition,
        scope: NameScope,
    ) -> Result<(), ExcelError> {
        // First collect dependents to avoid borrow checker issues
        let dependents_to_dirty = match scope {
            NameScope::Workbook => self
                .named_ranges
                .get(name)
                .map(|nr| nr.dependents.iter().copied().collect::<Vec<_>>()),
            NameScope::Sheet(id) => self
                .sheet_named_ranges
                .get(&(id, name.to_string()))
                .map(|nr| nr.dependents.iter().copied().collect::<Vec<_>>()),
        };

        if let Some(dependents) = dependents_to_dirty {
            // Mark all dependents as dirty
            for vertex_id in dependents {
                self.mark_vertex_dirty(vertex_id);
            }

            // Now update the definition
            let named_range = match scope {
                NameScope::Workbook => self.named_ranges.get_mut(name),
                NameScope::Sheet(id) => self.sheet_named_ranges.get_mut(&(id, name.to_string())),
            };

            let mut update_data: Option<(VertexId, NameScope, NamedDefinition, bool)> = None;
            if let Some(named_range) = named_range {
                named_range.definition = new_definition;
                named_range.dependents.clear();
                let is_range = matches!(named_range.definition, NamedDefinition::Range(_));
                update_data = Some((
                    named_range.vertex,
                    named_range.scope,
                    named_range.definition.clone(),
                    is_range,
                ));
            }

            if let Some((vertex, scope_value, definition_snapshot, is_range)) = update_data {
                self.detach_vertex_from_names(vertex);

                if is_range {
                    self.store.set_kind(vertex, VertexKind::NamedArray);
                } else {
                    self.store.set_kind(vertex, VertexKind::NamedScalar);
                }
                self.store.set_dirty(vertex, true);
                self.dirty_vertices.insert(vertex);

                let referenced_names =
                    self.rebuild_name_dependencies(vertex, &definition_snapshot, scope_value);
                if !referenced_names.is_empty() {
                    self.attach_vertex_to_names(vertex, &referenced_names);
                }
            }

            Ok(())
        } else {
            Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Name not found: {name}")))
        }
    }

    /// Delete a named range
    pub fn delete_name(&mut self, name: &str, scope: NameScope) -> Result<(), ExcelError> {
        let named_range = match scope {
            NameScope::Workbook => self.named_ranges.remove(name),
            NameScope::Sheet(id) => self.sheet_named_ranges.remove(&(id, name.to_string())),
        };

        if let Some(named_range) = named_range {
            let mut affected: FxHashSet<VertexId> = FxHashSet::default();
            for &vertex_id in &named_range.dependents {
                affected.insert(vertex_id);
            }
            for (vertex_id, names) in self.vertex_to_names.iter() {
                if names.iter().any(|vid| *vid == named_range.vertex) {
                    affected.insert(*vertex_id);
                }
            }
            for vertex_id in affected {
                self.mark_vertex_dirty(vertex_id);
                if let Some(names) = self.vertex_to_names.get_mut(&vertex_id) {
                    names.retain(|vid| *vid != named_range.vertex);
                    if names.is_empty() {
                        self.vertex_to_names.remove(&vertex_id);
                    }
                }
            }
            self.mark_named_vertex_deleted(&named_range);
            Ok(())
        } else {
            Err(ExcelError::new(ExcelErrorKind::Name)
                .with_message(format!("Name not found: {name}")))
        }
    }

    pub(super) fn detach_vertex_from_names(&mut self, vertex: VertexId) {
        if let Some(prior) = self.vertex_to_names.remove(&vertex) {
            for name_vertex in prior {
                if let Some((scope, name)) = self.name_vertex_lookup.get(&name_vertex).cloned() {
                    match scope {
                        NameScope::Workbook => {
                            if let Some(entry) = self.named_ranges.get_mut(&name) {
                                entry.dependents.remove(&vertex);
                            }
                        }
                        NameScope::Sheet(sheet_id) => {
                            if let Some(entry) =
                                self.sheet_named_ranges.get_mut(&(sheet_id, name.clone()))
                            {
                                entry.dependents.remove(&vertex);
                            }
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn attach_vertex_to_names(&mut self, vertex: VertexId, names: &[VertexId]) {
        if names.is_empty() {
            return;
        }
        let mut unique = FxHashSet::default();
        let mut recorded = Vec::new();
        for &name_vertex in names {
            if !unique.insert(name_vertex) {
                continue;
            }
            if let Some((scope, name)) = self.name_vertex_lookup.get(&name_vertex).cloned() {
                match scope {
                    NameScope::Workbook => {
                        if let Some(entry) = self.named_ranges.get_mut(&name) {
                            entry.dependents.insert(vertex);
                        }
                    }
                    NameScope::Sheet(sheet_id) => {
                        if let Some(entry) =
                            self.sheet_named_ranges.get_mut(&(sheet_id, name.clone()))
                        {
                            entry.dependents.insert(vertex);
                        }
                    }
                }
                recorded.push(name_vertex);
            }
        }
        if !recorded.is_empty() {
            self.vertex_to_names.insert(vertex, recorded);
        }
    }

    pub(super) fn unregister_name_cell_dependencies(&mut self, name_vertex: VertexId) {
        if let Some(prev) = self.name_to_cell_dependencies.remove(&name_vertex) {
            for dep in prev {
                if let Some(set) = self.cell_to_name_dependents.get_mut(&dep) {
                    set.remove(&name_vertex);
                    if set.is_empty() {
                        self.cell_to_name_dependents.remove(&dep);
                    }
                }
            }
        }
    }

    pub(super) fn register_name_cell_dependencies(
        &mut self,
        name_vertex: VertexId,
        dependencies: &[VertexId],
    ) {
        self.unregister_name_cell_dependencies(name_vertex);
        if dependencies.is_empty() {
            return;
        }
        for dep in dependencies {
            self.cell_to_name_dependents
                .entry(*dep)
                .or_default()
                .insert(name_vertex);
        }
        self.name_to_cell_dependencies
            .insert(name_vertex, dependencies.to_vec());
    }

    pub(crate) fn record_pending_name_reference(
        &mut self,
        sheet_id: SheetId,
        name: &str,
        formula_vertex: VertexId,
    ) {
        self.pending_name_links
            .entry(name.to_string())
            .or_default()
            .push((sheet_id, formula_vertex));
    }

    fn resolve_pending_name_references(
        &mut self,
        scope: NameScope,
        name: &str,
        named_vertex: VertexId,
    ) {
        if let Some(mut entries) = self.pending_name_links.remove(name) {
            let mut remaining: Vec<(SheetId, VertexId)> = Vec::new();
            for (sheet_id, formula_vertex) in entries.drain(..) {
                let attach = match scope {
                    NameScope::Workbook => true,
                    NameScope::Sheet(expected) => expected == sheet_id,
                };
                if attach {
                    self.add_dependent_edges(formula_vertex, &[named_vertex]);
                    self.attach_vertex_to_names(formula_vertex, &[named_vertex]);
                } else {
                    remaining.push((sheet_id, formula_vertex));
                }
            }
            if !remaining.is_empty() {
                self.pending_name_links.insert(name.to_string(), remaining);
            }
        }
    }

    pub(super) fn name_depends_on_vertex(
        &self,
        name_vertex: VertexId,
        target: VertexId,
        visited: &mut FxHashSet<VertexId>,
    ) -> bool {
        if !visited.insert(name_vertex) {
            return false;
        }

        for dependency in self.edges.out_edges(name_vertex).iter().copied() {
            if dependency == target {
                return true;
            }

            if matches!(
                self.store.kind(dependency),
                VertexKind::NamedScalar | VertexKind::NamedArray
            ) && self.name_depends_on_vertex(dependency, target, visited)
            {
                return true;
            }
        }

        false
    }

    pub(super) fn rebuild_name_dependencies(
        &mut self,
        vertex: VertexId,
        definition: &NamedDefinition,
        scope: NameScope,
    ) -> Vec<VertexId> {
        self.remove_dependent_edges(vertex);
        self.unregister_name_cell_dependencies(vertex);

        let mut dependencies: Vec<VertexId> = Vec::new();
        let mut range_dependencies: Vec<SharedRangeRef<'static>> = Vec::new();
        let mut placeholders = Vec::new();

        match definition {
            NamedDefinition::Cell(cell_ref) => {
                let vertex_id = self.get_or_create_vertex(cell_ref, &mut placeholders);
                dependencies.push(vertex_id);
            }
            NamedDefinition::Range(range_ref) => {
                let height = range_ref
                    .end
                    .coord
                    .row()
                    .saturating_sub(range_ref.start.coord.row())
                    + 1;
                let width = range_ref
                    .end
                    .coord
                    .col()
                    .saturating_sub(range_ref.start.coord.col())
                    + 1;
                let size = (width * height) as usize;

                if size <= self.config.range_expansion_limit {
                    for row in range_ref.start.coord.row()..=range_ref.end.coord.row() {
                        for col in range_ref.start.coord.col()..=range_ref.end.coord.col() {
                            let coord = Coord::new(row, col, true, true);
                            let addr = CellRef::new(range_ref.start.sheet_id, coord);
                            let vertex_id = self.get_or_create_vertex(&addr, &mut placeholders);
                            dependencies.push(vertex_id);
                        }
                    }
                } else {
                    let sheet_loc = SharedSheetLocator::Id(range_ref.start.sheet_id);
                    let sr = formualizer_common::AxisBound::new(
                        range_ref.start.coord.row(),
                        range_ref.start.coord.row_abs(),
                    );
                    let sc = formualizer_common::AxisBound::new(
                        range_ref.start.coord.col(),
                        range_ref.start.coord.col_abs(),
                    );
                    let er = formualizer_common::AxisBound::new(
                        range_ref.end.coord.row(),
                        range_ref.end.coord.row_abs(),
                    );
                    let ec = formualizer_common::AxisBound::new(
                        range_ref.end.coord.col(),
                        range_ref.end.coord.col_abs(),
                    );
                    if let Ok(r) = SharedRangeRef::from_parts(
                        sheet_loc,
                        Some(sr),
                        Some(sc),
                        Some(er),
                        Some(ec),
                    ) {
                        range_dependencies.push(r.into_owned());
                    }
                }
            }
            NamedDefinition::Formula {
                dependencies: formula_deps,
                range_deps,
                ..
            } => {
                dependencies.extend(formula_deps.iter().copied());
                range_dependencies.extend(range_deps.iter().cloned());
            }
        }

        if !dependencies.is_empty() {
            self.add_dependent_edges(vertex, &dependencies);
        }
        self.register_name_cell_dependencies(vertex, &dependencies);

        if !range_dependencies.is_empty() {
            let sheet_id = match scope {
                NameScope::Sheet(id) => id,
                NameScope::Workbook => self.default_sheet_id,
            };
            self.add_range_dependent_edges(vertex, &range_dependencies, sheet_id);
        }

        dependencies
            .iter()
            .filter(|vid| {
                matches!(
                    self.store.kind(**vid),
                    VertexKind::NamedScalar | VertexKind::NamedArray
                )
            })
            .copied()
            .collect()
    }

    pub fn adjust_named_ranges(
        &mut self,
        operation: &crate::engine::graph::editor::reference_adjuster::ShiftOperation,
    ) -> Result<(), ExcelError> {
        let adjuster = crate::engine::graph::editor::reference_adjuster::ReferenceAdjuster::new();

        // Adjust workbook-scoped names
        for named_range in self.named_ranges.values_mut() {
            adjust_named_definition(&mut named_range.definition, &adjuster, operation)?;
        }

        // Adjust sheet-scoped names
        for named_range in self.sheet_named_ranges.values_mut() {
            adjust_named_definition(&mut named_range.definition, &adjuster, operation)?;
        }

        Ok(())
    }

    /// Mark a vertex as having a #NAME! error
    pub fn mark_as_name_error(&mut self, vertex_id: VertexId) {
        // Mark the vertex as dirty
        self.mark_vertex_dirty(vertex_id);
    }

    pub(super) fn mark_named_vertex_deleted(&mut self, named_range: &NamedRange) {
        self.detach_vertex_from_names(named_range.vertex);
        self.remove_dependent_edges(named_range.vertex);
        self.unregister_name_cell_dependencies(named_range.vertex);
        self.store.mark_deleted(named_range.vertex, true);
        self.vertex_values.remove(&named_range.vertex);
        self.vertex_formulas.remove(&named_range.vertex);
        self.dirty_vertices.remove(&named_range.vertex);
        self.volatile_vertices.remove(&named_range.vertex);
        self.vertex_to_names.remove(&named_range.vertex);
        self.name_vertex_lookup.remove(&named_range.vertex);
    }
}
