use super::*;
use crate::reference::RangeRef;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedLegacyGraphError {
    InvalidSheet(SheetId),
    InvalidCoordinate {
        sheet: SheetId,
        row: u32,
        col: u32,
    },
    InvalidAst(AstNodeId),
    DuplicateTarget {
        sheet: SheetId,
        row: u32,
        col: u32,
    },
    TargetConflict {
        sheet: SheetId,
        row: u32,
        col: u32,
    },
    VertexIdExhausted,
    PlanSizeOverflow,
    Stale,
    DynamicTopologyUnsupported,
    #[cfg(test)]
    InjectedFailure,
}

impl std::fmt::Display for PreparedLegacyGraphError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSheet(id) => write!(f, "invalid legacy graph sheet id {id}"),
            Self::InvalidCoordinate { sheet, row, col } => {
                write!(f, "invalid legacy graph coordinate {sheet}:{row}:{col}")
            }
            Self::InvalidAst(id) => write!(f, "invalid legacy graph AST id {id:?}"),
            Self::DuplicateTarget { sheet, row, col } => {
                write!(f, "duplicate legacy graph target {sheet}:{row}:{col}")
            }
            Self::TargetConflict { sheet, row, col } => {
                write!(
                    f,
                    "legacy graph target is not a pristine placeholder {sheet}:{row}:{col}"
                )
            }
            Self::VertexIdExhausted => write!(f, "legacy graph vertex ids exhausted"),
            Self::PlanSizeOverflow => write!(f, "legacy graph plan size overflow"),
            Self::Stale => write!(f, "prepared legacy graph plan is stale"),
            Self::DynamicTopologyUnsupported => {
                write!(
                    f,
                    "prepared legacy graph plans do not support dynamic topology"
                )
            }
            #[cfg(test)]
            Self::InjectedFailure => write!(f, "injected prepared legacy graph failure"),
        }
    }
}

impl std::error::Error for PreparedLegacyGraphError {}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SymbolKind {
    Name,
    SourceScalar,
    SourceTable,
    Table,
}

#[derive(Debug, Clone, PartialEq)]
enum SymbolMetadata {
    Missing,
    Name {
        scope: NameScope,
        definition: NamedDefinition,
        vertex: VertexId,
    },
    SourceScalar {
        name: String,
        vertex: VertexId,
        version: Option<u64>,
    },
    SourceTable {
        name: String,
        vertex: VertexId,
        version: Option<u64>,
    },
    Table {
        name: String,
        range: RangeRef,
        header_row: bool,
        headers: Vec<String>,
        totals_row: bool,
        vertex: VertexId,
    },
}

impl SymbolMetadata {
    fn vertex(&self) -> Option<VertexId> {
        match self {
            Self::Missing => None,
            Self::Name { vertex, .. }
            | Self::SourceScalar { vertex, .. }
            | Self::SourceTable { vertex, .. }
            | Self::Table { vertex, .. } => Some(*vertex),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SymbolBinding {
    sheet_id: SheetId,
    kind: SymbolKind,
    name: String,
    metadata: SymbolMetadata,
}

#[derive(Debug)]
struct PreparedFormula {
    current_sheet_id: SheetId,
    target: VertexId,
    target_packed: PackedSheetCell,
    ast_id: AstNodeId,
    plan: DependencyPlanRow,
    direct_dependencies: Vec<VertexId>,
    named_dependencies: Vec<VertexId>,
    other_dependencies: Vec<VertexId>,
    unresolved_names: Vec<String>,
}

/// A transaction-local, single-use legacy graph addition plan.
///
/// Preparation performs every fallible identifier and binding check without
/// changing the graph. Application first revalidates those assumptions and then
/// only appends the reserved vertices and installs this plan's formulas.
#[derive(Debug)]
pub(crate) struct PreparedLegacyGraphPlan {
    sheet_names: Vec<(SheetId, String)>,
    expected_vertex_len: usize,
    coordinates: BTreeMap<PackedSheetCell, VertexId>,
    new_vertices: Vec<(PackedSheetCell, VertexId)>,
    new_vertex_set: std::collections::BTreeSet<PackedSheetCell>,
    existing_targets: Vec<VertexId>,
    symbols: Vec<SymbolBinding>,
    formulas: Vec<PreparedFormula>,
}

impl PreparedLegacyGraphPlan {
    pub(crate) fn new_vertex_count(&self) -> usize {
        self.new_vertices.len()
    }

    pub(crate) fn planned_edge_count(&self) -> Option<usize> {
        self.formulas.iter().try_fold(0usize, |total, formula| {
            let mut dependencies: std::collections::BTreeSet<_> =
                formula.direct_dependencies.iter().copied().collect();
            dependencies.extend(formula.other_dependencies.iter().copied());
            let target = formula.target_packed;
            let target_row = target.row0();
            let target_col = target.col0();
            let range_contains_target = formula.plan.range_deps.iter().any(|range| {
                let sheet_id = match range.sheet {
                    SharedSheetLocator::Id(id) => id,
                    _ => formula.current_sheet_id,
                };
                sheet_id == target.sheet_id()
                    && range
                        .start_row
                        .is_none_or(|bound| target_row >= bound.index)
                    && range.end_row.is_none_or(|bound| target_row <= bound.index)
                    && range
                        .start_col
                        .is_none_or(|bound| target_col >= bound.index)
                    && range.end_col.is_none_or(|bound| target_col <= bound.index)
            });
            if range_contains_target {
                dependencies.insert(formula.target);
            }
            total.checked_add(dependencies.len())
        })
    }
}

impl DependencyGraph {
    fn checked_sheet_name(&self, id: SheetId) -> Result<String, PreparedLegacyGraphError> {
        let name = self.sheet_reg.name(id);
        if name.is_empty() || self.sheet_reg.get_id(name) != Some(id) {
            Err(PreparedLegacyGraphError::InvalidSheet(id))
        } else {
            Ok(name.to_string())
        }
    }

    fn prepared_symbol_binding(
        &self,
        kind: SymbolKind,
        name: &str,
        sheet: SheetId,
    ) -> SymbolBinding {
        let metadata = match kind {
            SymbolKind::Name => self
                .resolve_name_entry(name, sheet)
                .map(|entry| SymbolMetadata::Name {
                    scope: entry.scope,
                    definition: entry.definition.clone(),
                    vertex: entry.vertex,
                })
                .unwrap_or(SymbolMetadata::Missing),
            SymbolKind::SourceScalar => self
                .resolve_source_scalar_entry(name)
                .map(|entry| SymbolMetadata::SourceScalar {
                    name: entry.name.clone(),
                    vertex: entry.vertex,
                    version: entry.version,
                })
                .unwrap_or(SymbolMetadata::Missing),
            SymbolKind::SourceTable => self
                .resolve_source_table_entry(name)
                .map(|entry| SymbolMetadata::SourceTable {
                    name: entry.name.clone(),
                    vertex: entry.vertex,
                    version: entry.version,
                })
                .unwrap_or(SymbolMetadata::Missing),
            SymbolKind::Table => self
                .resolve_table_entry(name)
                .map(|entry| SymbolMetadata::Table {
                    name: entry.name.clone(),
                    range: entry.range,
                    header_row: entry.header_row,
                    headers: entry.headers.clone(),
                    totals_row: entry.totals_row,
                    vertex: entry.vertex,
                })
                .unwrap_or(SymbolMetadata::Missing),
        };
        SymbolBinding {
            sheet_id: sheet,
            kind,
            name: name.to_string(),
            metadata,
        }
    }

    pub(crate) fn prepare_legacy_graph_plan(
        &self,
        sheet_id: SheetId,
        planned: Vec<(u32, u32, AstNodeId, DependencyPlanRow)>,
    ) -> Result<PreparedLegacyGraphPlan, PreparedLegacyGraphError> {
        self.prepare_legacy_graph_plan_multi_sheet(
            planned
                .into_iter()
                .map(|(row, col, ast_id, plan)| (sheet_id, row, col, ast_id, plan))
                .collect(),
        )
    }

    /// Prepare one additions-only graph plan spanning any number of sheets.
    /// All targets and cross-sheet placeholders share one vertex-id reservation.
    pub(crate) fn prepare_legacy_graph_plan_multi_sheet(
        &self,
        planned: Vec<(SheetId, u32, u32, AstNodeId, DependencyPlanRow)>,
    ) -> Result<PreparedLegacyGraphPlan, PreparedLegacyGraphError> {
        if self.pk_order.is_some() {
            return Err(PreparedLegacyGraphError::DynamicTopologyUnsupported);
        }
        let mut sheet_names = BTreeMap::new();
        let mut packed_inputs = Vec::new();
        let mut target_inputs = Vec::with_capacity(planned.len());
        let mut seen_targets = std::collections::BTreeSet::new();
        for (sheet_id, row, col, ast_id, plan) in &planned {
            sheet_names
                .entry(*sheet_id)
                .or_insert(self.checked_sheet_name(*sheet_id)?);
            if self.data_store.get_node(*ast_id).is_none() {
                return Err(PreparedLegacyGraphError::InvalidAst(*ast_id));
            }
            let target = PackedSheetCell::try_from_excel_1based(*sheet_id, *row, *col).ok_or(
                PreparedLegacyGraphError::InvalidCoordinate {
                    sheet: *sheet_id,
                    row: *row,
                    col: *col,
                },
            )?;
            if !seen_targets.insert(target) {
                return Err(PreparedLegacyGraphError::DuplicateTarget {
                    sheet: *sheet_id,
                    row: *row,
                    col: *col,
                });
            }
            packed_inputs.push(target);
            target_inputs.push(target);
            for dep in &plan.direct_cell_deps {
                let packed =
                    PackedSheetCell::try_new(dep.sheet_id, dep.coord.row(), dep.coord.col())
                        .ok_or(PreparedLegacyGraphError::InvalidCoordinate {
                            sheet: dep.sheet_id,
                            row: dep.coord.row(),
                            col: dep.coord.col(),
                        })?;
                sheet_names
                    .entry(dep.sheet_id)
                    .or_insert(self.checked_sheet_name(dep.sheet_id)?);
                packed_inputs.push(packed);
            }
            for range in &plan.range_deps {
                if let SharedSheetLocator::Id(id) = range.sheet {
                    sheet_names
                        .entry(id)
                        .or_insert(self.checked_sheet_name(id)?);
                }
                let range_sheet = match range.sheet {
                    SharedSheetLocator::Id(id) => id,
                    _ => *sheet_id,
                };
                for bound in [range.start_row, range.end_row].into_iter().flatten() {
                    if bound.index > PackedSheetCell::MAX_ROW0 {
                        return Err(PreparedLegacyGraphError::InvalidCoordinate {
                            sheet: range_sheet,
                            row: bound.index,
                            col: 0,
                        });
                    }
                }
                for bound in [range.start_col, range.end_col].into_iter().flatten() {
                    if bound.index > PackedSheetCell::MAX_COL0 {
                        return Err(PreparedLegacyGraphError::InvalidCoordinate {
                            sheet: range_sheet,
                            row: 0,
                            col: bound.index,
                        });
                    }
                }
            }
        }

        let mut coordinates = BTreeMap::new();
        let mut new_packed = Vec::new();
        let mut seen_packed = std::collections::BTreeSet::new();
        for packed in packed_inputs {
            if !seen_packed.insert(packed) {
                continue;
            }
            let addr = CellRef::new(
                packed.sheet_id(),
                Coord::new(packed.row0(), packed.col0(), true, true),
            );
            if let Some(id) = self
                .cell_to_vertex
                .get(&addr)
                .copied()
                .or_else(|| self.load_packed_to_vertex.get(&packed).copied())
            {
                if !self.store.vertex_exists_active(id) {
                    return Err(PreparedLegacyGraphError::Stale);
                }
                coordinates.insert(packed, id);
            } else {
                new_packed.push(packed);
            }
        }
        let base = u32::try_from(self.store.len())
            .ok()
            .and_then(|len| len.checked_add(FIRST_NORMAL_VERTEX))
            .ok_or(PreparedLegacyGraphError::VertexIdExhausted)?;
        let last_offset = u32::try_from(new_packed.len())
            .map_err(|_| PreparedLegacyGraphError::VertexIdExhausted)?;
        if last_offset != 0 {
            base.checked_add(last_offset - 1)
                .ok_or(PreparedLegacyGraphError::VertexIdExhausted)?;
        }
        let mut new_vertices = Vec::with_capacity(new_packed.len());
        for (offset, packed) in new_packed.into_iter().enumerate() {
            let offset =
                u32::try_from(offset).map_err(|_| PreparedLegacyGraphError::VertexIdExhausted)?;
            let id = VertexId::new(
                base.checked_add(offset)
                    .ok_or(PreparedLegacyGraphError::VertexIdExhausted)?,
            );
            coordinates.insert(packed, id);
            new_vertices.push((packed, id));
        }

        let new_vertex_set: std::collections::BTreeSet<_> =
            new_vertices.iter().map(|(packed, _)| *packed).collect();
        let mut existing_targets = Vec::new();
        for target in &target_inputs {
            if new_vertex_set.contains(target) {
                continue;
            }
            let id = coordinates
                .get(target)
                .copied()
                .ok_or(PreparedLegacyGraphError::Stale)?;
            if !self.is_pristine_legacy_graph_target(id) {
                let (_, row, col) = target.to_excel_1based();
                return Err(PreparedLegacyGraphError::TargetConflict {
                    sheet: target.sheet_id(),
                    row,
                    col,
                });
            }
            existing_targets.push(id);
        }

        let mut symbols = Vec::new();
        let mut formulas = Vec::with_capacity(planned.len());
        for ((current_sheet_id, _, _, ast_id, plan), target_packed) in
            planned.into_iter().zip(target_inputs)
        {
            let target = coordinates
                .get(&target_packed)
                .copied()
                .ok_or(PreparedLegacyGraphError::Stale)?;
            let mut direct_dependencies = Vec::new();
            for dep in &plan.direct_cell_deps {
                let packed =
                    PackedSheetCell::try_new(dep.sheet_id, dep.coord.row(), dep.coord.col())
                        .ok_or(PreparedLegacyGraphError::Stale)?;
                let dependency = coordinates
                    .get(&packed)
                    .copied()
                    .ok_or(PreparedLegacyGraphError::Stale)?;
                push_unique(&mut direct_dependencies, dependency);
            }
            let mut named_dependencies = Vec::new();
            let mut other_dependencies = Vec::new();
            let mut unresolved_names = Vec::new();
            for name in plan
                .resolved_named_refs
                .iter()
                .chain(plan.named_refs.iter())
            {
                let binding =
                    self.prepared_symbol_binding(SymbolKind::Name, name, current_sheet_id);
                if let Some(id) = binding.metadata.vertex() {
                    push_unique(&mut named_dependencies, id);
                    push_unique(&mut other_dependencies, id);
                } else {
                    let scalar = self.prepared_symbol_binding(
                        SymbolKind::SourceScalar,
                        name,
                        current_sheet_id,
                    );
                    if let Some(id) = scalar.metadata.vertex() {
                        push_unique(&mut other_dependencies, id);
                    } else {
                        unresolved_names.push(name.clone());
                    }
                    symbols.push(scalar);
                }
                symbols.push(binding);
            }
            for name in &plan.source_refs {
                let scalar =
                    self.prepared_symbol_binding(SymbolKind::SourceScalar, name, current_sheet_id);
                let table =
                    self.prepared_symbol_binding(SymbolKind::SourceTable, name, current_sheet_id);
                if let Some(id) = scalar.metadata.vertex().or_else(|| table.metadata.vertex()) {
                    push_unique(&mut other_dependencies, id);
                }
                symbols.extend([scalar, table]);
            }
            for name in &plan.table_refs {
                let table = self.prepared_symbol_binding(SymbolKind::Table, name, current_sheet_id);
                let source =
                    self.prepared_symbol_binding(SymbolKind::SourceTable, name, current_sheet_id);
                if let Some(id) = table.metadata.vertex().or_else(|| source.metadata.vertex()) {
                    push_unique(&mut other_dependencies, id);
                }
                symbols.extend([table, source]);
            }
            formulas.push(PreparedFormula {
                current_sheet_id,
                target,
                target_packed,
                ast_id,
                plan,
                direct_dependencies,
                named_dependencies,
                other_dependencies,
                unresolved_names,
            });
        }
        Ok(PreparedLegacyGraphPlan {
            sheet_names: sheet_names.into_iter().collect(),
            expected_vertex_len: self.store.len(),
            coordinates,
            new_vertex_set,
            new_vertices,
            existing_targets,
            symbols,
            formulas,
        })
    }

    fn is_pristine_legacy_graph_target(&self, id: VertexId) -> bool {
        self.store.vertex_exists_active(id)
            && self.store.kind(id) == VertexKind::Empty
            && self.store.value_ref(id) == 0
            && !self.vertex_formulas.contains_key(&id)
            && !self.vertex_values.contains_key(&id)
            && !self.ref_error_vertices.contains(&id)
            && !self.is_dirty(id)
            && !self.store.is_volatile(id)
            && !self.volatile_vertices.contains(&id)
            && !self.store.is_dynamic(id)
            && self.edges.out_edges(id).is_empty()
            && !self.formula_to_range_deps.contains_key(&id)
            && !self.vertex_to_names.contains_key(&id)
            && !self.vertex_to_pending_names.contains_key(&id)
    }

    pub(crate) fn validate_prepared_legacy_graph_plan(
        &self,
        plan: &PreparedLegacyGraphPlan,
    ) -> Result<(), PreparedLegacyGraphError> {
        #[cfg(test)]
        if self.prepared_legacy_graph_failure_for_test {
            return Err(PreparedLegacyGraphError::InjectedFailure);
        }
        if self.pk_order.is_some() || self.store.len() != plan.expected_vertex_len {
            return Err(PreparedLegacyGraphError::Stale);
        }
        for (id, name) in &plan.sheet_names {
            if self.sheet_reg.name(*id) != name || self.sheet_reg.get_id(name) != Some(*id) {
                return Err(PreparedLegacyGraphError::Stale);
            }
        }
        for (packed, expected) in &plan.coordinates {
            let addr = CellRef::new(
                packed.sheet_id(),
                Coord::new(packed.row0(), packed.col0(), true, true),
            );
            let actual = self
                .cell_to_vertex
                .get(&addr)
                .copied()
                .or_else(|| self.load_packed_to_vertex.get(packed).copied());
            if plan.new_vertex_set.contains(packed) {
                if actual.is_some() {
                    return Err(PreparedLegacyGraphError::Stale);
                }
            } else if actual != Some(*expected) || !self.store.vertex_exists_active(*expected) {
                return Err(PreparedLegacyGraphError::Stale);
            }
        }
        if plan
            .existing_targets
            .iter()
            .any(|&id| !self.is_pristine_legacy_graph_target(id))
        {
            return Err(PreparedLegacyGraphError::Stale);
        }
        for binding in &plan.symbols {
            if self.prepared_symbol_binding(binding.kind.clone(), &binding.name, binding.sheet_id)
                != *binding
            {
                return Err(PreparedLegacyGraphError::Stale);
            }
            if binding
                .metadata
                .vertex()
                .is_some_and(|id| !self.store.vertex_exists_active(id))
            {
                return Err(PreparedLegacyGraphError::Stale);
            }
        }
        for formula in &plan.formulas {
            if self.data_store.get_node(formula.ast_id).is_none() {
                return Err(PreparedLegacyGraphError::Stale);
            }
        }
        Ok(())
    }

    pub(crate) fn apply_prepared_legacy_graph_plan(
        &mut self,
        plan: PreparedLegacyGraphPlan,
    ) -> Result<usize, PreparedLegacyGraphError> {
        self.validate_prepared_legacy_graph_plan(&plan)?;
        Ok(self.apply_prevalidated_legacy_graph_plan(plan))
    }

    /// Infallible application half for a plan that passed the final validator
    /// while the caller retained exclusive access to the graph.
    pub(crate) fn apply_prevalidated_legacy_graph_plan(
        &mut self,
        plan: PreparedLegacyGraphPlan,
    ) -> usize {
        let allocations: Vec<_> = plan
            .new_vertices
            .iter()
            .map(|(packed, _)| {
                (
                    AbsCoord::new(packed.row0(), packed.col0()),
                    packed.sheet_id(),
                    0,
                )
            })
            .collect();
        self.store.allocate_prevalidated_batch(&allocations);
        for ((packed, id), (coord, _, _)) in plan.new_vertices.iter().zip(allocations) {
            let id = *id;
            self.edges.add_vertex(coord, id.0);
            self.sheet_index_mut(packed.sheet_id())
                .add_vertex(coord, id);
            self.store.set_kind(id, VertexKind::Empty);
            let addr = CellRef::new(
                packed.sheet_id(),
                Coord::new(packed.row0(), packed.col0(), true, true),
            );
            self.cell_to_vertex.insert(addr, id);
        }
        let targets: Vec<_> = plan.formulas.iter().map(|formula| formula.target).collect();
        for formula in &plan.formulas {
            self.store
                .set_kind(formula.target, VertexKind::FormulaScalar);
            self.vertex_formulas.insert(formula.target, formula.ast_id);
            self.mark_volatile(formula.target, formula.plan.volatile);
            self.store.set_dynamic(formula.target, formula.plan.dynamic);
        }
        self.edges.begin_batch();
        for formula in &plan.formulas {
            if !formula.named_dependencies.is_empty() {
                self.attach_vertex_to_names(formula.target, &formula.named_dependencies);
            }
            for name in &formula.unresolved_names {
                self.record_pending_name_reference(formula.current_sheet_id, name, formula.target);
            }
            let mut deps = formula.direct_dependencies.clone();
            for id in &formula.other_dependencies {
                push_unique(&mut deps, *id);
            }
            if !deps.is_empty() {
                self.add_dependent_edges_nobatch(formula.target, &deps);
            }
            self.add_range_dependent_edges(
                formula.target,
                &formula.plan.range_deps,
                formula.current_sheet_id,
            );
        }
        self.edges.end_batch_deferred();
        let _ = self.mark_dirty_many(&targets);
        plan.formulas.len()
    }
}

fn push_unique(ids: &mut Vec<VertexId>, id: VertexId) {
    if !ids.contains(&id) {
        ids.push(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use formualizer_parse::parser::parse;

    fn graph() -> (DependencyGraph, SheetId) {
        let mut graph = DependencyGraph::new();
        let sheet = graph.sheet_id_mut("Sheet1");
        (graph, sheet)
    }

    fn planned(
        graph: &mut DependencyGraph,
        sheet: SheetId,
        row: u32,
        col: u32,
        formula: &str,
        deps: &[(u32, u32)],
    ) -> (u32, u32, AstNodeId, DependencyPlanRow) {
        let ast = parse(formula).expect("formula parses");
        let ast_id = graph.store_ast(&ast);
        let plan = DependencyPlanRow {
            direct_cell_deps: deps
                .iter()
                .map(|&(row, col)| CellRef::new(sheet, Coord::from_excel(row, col, true, true)))
                .collect(),
            ..DependencyPlanRow::default()
        };
        (row, col, ast_id, plan)
    }

    #[test]
    fn multi_sheet_plan_reserves_targets_and_cross_sheet_placeholders_once() {
        let (mut graph, sheet1) = graph();
        let sheet2 = graph.sheet_id_mut("Sheet2");
        let ast1 = graph.store_ast(&parse("=Sheet2!B1").unwrap());
        let ast2 = graph.store_ast(&parse("=Sheet1!B1").unwrap());
        let plan1 = DependencyPlanRow {
            direct_cell_deps: vec![CellRef::new(sheet2, Coord::from_excel(1, 2, true, true))],
            ..DependencyPlanRow::default()
        };
        let plan2 = DependencyPlanRow {
            direct_cell_deps: vec![CellRef::new(sheet1, Coord::from_excel(1, 2, true, true))],
            ..DependencyPlanRow::default()
        };
        let plan = graph
            .prepare_legacy_graph_plan_multi_sheet(vec![
                (sheet1, 1, 1, ast1, plan1),
                (sheet2, 1, 1, ast2, plan2),
            ])
            .unwrap();
        assert_eq!(plan.new_vertex_count(), 4);
        assert_eq!(plan.planned_edge_count(), Some(2));
        assert_eq!(graph.apply_prepared_legacy_graph_plan(plan).unwrap(), 2);
        let stats = graph.baseline_stats();
        assert_eq!(stats.graph_vertex_count, 4);
        assert_eq!(stats.graph_formula_vertex_count, 2);
        assert_eq!(stats.graph_edge_count, 2);
    }

    #[test]
    fn invalid_inputs_leave_graph_logically_unchanged() {
        let (mut graph, sheet) = graph();
        let item = planned(&mut graph, sheet, 1, 1, "=B1", &[(1, 2)]);
        let before = graph.baseline_stats();
        let err = graph.prepare_legacy_graph_plan(99, vec![item]).unwrap_err();
        assert_eq!(err, PreparedLegacyGraphError::InvalidSheet(99));
        assert_eq!(graph.baseline_stats(), before);

        let item = planned(&mut graph, sheet, 0, 1, "=1", &[]);
        let before = graph.baseline_stats();
        assert!(matches!(
            graph.prepare_legacy_graph_plan(sheet, vec![item]),
            Err(PreparedLegacyGraphError::InvalidCoordinate { .. })
        ));
        assert_eq!(graph.baseline_stats(), before);
    }

    #[test]
    fn duplicate_targets_are_rejected_before_mutation() {
        let (mut graph, sheet) = graph();
        let items = vec![
            planned(&mut graph, sheet, 1, 1, "=1", &[]),
            planned(&mut graph, sheet, 1, 1, "=2", &[]),
        ];
        let before = graph.baseline_stats();
        assert_eq!(
            graph.prepare_legacy_graph_plan(sheet, items).unwrap_err(),
            PreparedLegacyGraphError::DuplicateTarget {
                sheet,
                row: 1,
                col: 1,
            }
        );
        assert_eq!(graph.baseline_stats(), before);
    }

    #[test]
    fn existing_formula_target_is_rejected_before_mutation() {
        let (mut graph, sheet) = graph();
        let existing = planned(&mut graph, sheet, 1, 1, "=1", &[]);
        let existing = graph
            .prepare_legacy_graph_plan(sheet, vec![existing])
            .unwrap();
        graph.apply_prepared_legacy_graph_plan(existing).unwrap();

        let replacement = planned(&mut graph, sheet, 1, 1, "=2", &[]);
        let before = graph.baseline_stats();
        assert_eq!(
            graph
                .prepare_legacy_graph_plan(sheet, vec![replacement])
                .unwrap_err(),
            PreparedLegacyGraphError::TargetConflict {
                sheet,
                row: 1,
                col: 1,
            }
        );
        assert_eq!(graph.baseline_stats(), before);
    }

    #[test]
    fn changed_existing_placeholder_is_stale_without_vertex_allocation() {
        let (mut graph, sheet) = graph();
        let owner = planned(&mut graph, sheet, 1, 1, "=B1", &[(1, 2)]);
        let owner = graph.prepare_legacy_graph_plan(sheet, vec![owner]).unwrap();
        graph.apply_prepared_legacy_graph_plan(owner).unwrap();

        let addition = planned(&mut graph, sheet, 1, 2, "=C1", &[(1, 3)]);
        let addition = graph
            .prepare_legacy_graph_plan(sheet, vec![addition])
            .unwrap();
        let vertex_count = graph.baseline_stats().graph_vertex_count;
        graph
            .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(7.0))
            .unwrap();
        assert_eq!(graph.baseline_stats().graph_vertex_count, vertex_count);
        let before = graph.baseline_stats();
        assert_eq!(
            graph.apply_prepared_legacy_graph_plan(addition),
            Err(PreparedLegacyGraphError::Stale)
        );
        assert_eq!(graph.baseline_stats(), before);
    }

    #[test]
    fn pristine_placeholder_addition_preserves_incoming_dependents() {
        let (mut graph, sheet) = graph();
        let owner = planned(&mut graph, sheet, 1, 1, "=B1", &[(1, 2)]);
        let owner = graph.prepare_legacy_graph_plan(sheet, vec![owner]).unwrap();
        graph.apply_prepared_legacy_graph_plan(owner).unwrap();
        let a1 = CellRef::new(sheet, Coord::from_excel(1, 1, true, true));
        let a1_id = graph.get_vertex_for_cell(&a1).unwrap();
        graph.clear_dirty_flags(&[a1_id]);
        assert!(!graph.store.is_dirty(a1_id));
        assert!(!graph.get_evaluation_vertices().contains(&a1_id));
        let rebuilds = graph.edges_rebuild_count();

        let addition = planned(&mut graph, sheet, 1, 2, "=C1", &[(1, 3)]);
        let addition = graph
            .prepare_legacy_graph_plan(sheet, vec![addition])
            .unwrap();
        graph.apply_prepared_legacy_graph_plan(addition).unwrap();
        assert_eq!(graph.baseline_stats().graph_formula_vertex_count, 2);
        assert_eq!(graph.baseline_stats().graph_edge_count, 2);
        assert!(graph.store.is_dirty(a1_id));
        assert!(graph.get_evaluation_vertices().contains(&a1_id));
        assert_eq!(graph.edges_rebuild_count(), rebuilds);
    }

    #[test]
    fn dirty_placeholder_is_conflict_and_becomes_stale_after_prepare() {
        let (mut graph, sheet) = graph();
        let owner = planned(&mut graph, sheet, 1, 1, "=B1", &[(1, 2)]);
        let owner = graph.prepare_legacy_graph_plan(sheet, vec![owner]).unwrap();
        graph.apply_prepared_legacy_graph_plan(owner).unwrap();
        let b1 = CellRef::new(sheet, Coord::from_excel(1, 2, true, true));
        let b1_id = graph.get_vertex_for_cell(&b1).unwrap();

        graph.mark_vertex_dirty(b1_id);
        let conflict = planned(&mut graph, sheet, 1, 2, "=C1", &[(1, 3)]);
        assert_eq!(
            graph
                .prepare_legacy_graph_plan(sheet, vec![conflict])
                .unwrap_err(),
            PreparedLegacyGraphError::TargetConflict {
                sheet,
                row: 1,
                col: 2,
            }
        );

        graph.clear_dirty_flags(&[b1_id]);
        let addition = planned(&mut graph, sheet, 1, 2, "=C1", &[(1, 3)]);
        let addition = graph
            .prepare_legacy_graph_plan(sheet, vec![addition])
            .unwrap();
        graph.mark_vertex_dirty(b1_id);
        let before = graph.baseline_stats();
        assert_eq!(
            graph.apply_prepared_legacy_graph_plan(addition),
            Err(PreparedLegacyGraphError::Stale)
        );
        assert_eq!(graph.baseline_stats(), before);
    }

    #[test]
    fn successful_batch_installs_formulas_dependencies_and_dirty_state() {
        let (mut graph, sheet) = graph();
        let items = vec![
            planned(&mut graph, sheet, 1, 1, "=B1", &[(1, 2)]),
            planned(&mut graph, sheet, 2, 1, "=B2", &[(2, 2)]),
        ];
        let plan = graph.prepare_legacy_graph_plan(sheet, items).unwrap();
        assert_eq!(
            plan.new_vertices.len(),
            4,
            "reserved coordinates: {:?}",
            plan.new_vertices
        );
        assert_eq!(graph.baseline_stats().graph_vertex_count, 0);
        assert_eq!(graph.apply_prepared_legacy_graph_plan(plan).unwrap(), 2);
        let stats = graph.baseline_stats();
        assert_eq!(stats.graph_vertex_count, 4);
        assert_eq!(stats.graph_formula_vertex_count, 2);
        assert_eq!(stats.graph_edge_count, 2);
        assert_eq!(stats.dirty_vertex_count, 2);
        let a1 = CellRef::new(sheet, Coord::from_excel(1, 1, true, true));
        let a2 = CellRef::new(sheet, Coord::from_excel(2, 1, true, true));
        assert!(graph.vertex_has_formula(graph.get_vertex_for_cell(&a1).unwrap()));
        assert!(graph.vertex_has_formula(graph.get_vertex_for_cell(&a2).unwrap()));
    }

    #[test]
    fn preparation_is_transaction_local_and_does_not_rebuild_existing_graph() {
        let (mut graph, sheet) = graph();
        let first = planned(&mut graph, sheet, 10, 1, "=B10", &[(10, 2)]);
        let first = graph.prepare_legacy_graph_plan(sheet, vec![first]).unwrap();
        graph.apply_prepared_legacy_graph_plan(first).unwrap();
        let rebuilds = graph.edges_rebuild_count();
        let second = planned(&mut graph, sheet, 1, 1, "=B1", &[(1, 2)]);
        let before = graph.baseline_stats();
        let second = graph
            .prepare_legacy_graph_plan(sheet, vec![second])
            .unwrap();
        assert_eq!(graph.baseline_stats(), before);
        graph.apply_prepared_legacy_graph_plan(second).unwrap();
        assert_eq!(graph.edges_rebuild_count(), rebuilds);
        assert_eq!(
            graph.baseline_stats().graph_vertex_count,
            before.graph_vertex_count + 2
        );
    }

    #[test]
    fn same_vertex_name_definition_change_is_stale_before_mutation() {
        let (mut graph, sheet) = graph();
        graph
            .define_name(
                "N",
                NamedDefinition::Literal(LiteralValue::Number(1.0)),
                NameScope::Workbook,
            )
            .unwrap();
        let mut item = planned(&mut graph, sheet, 1, 1, "=1", &[]);
        item.3.resolved_named_refs.push("N".to_string());
        let plan = graph.prepare_legacy_graph_plan(sheet, vec![item]).unwrap();
        let vertex = graph.resolve_name_entry("N", sheet).unwrap().vertex;
        graph
            .update_name(
                "N",
                NamedDefinition::Literal(LiteralValue::Number(2.0)),
                NameScope::Workbook,
            )
            .unwrap();
        assert_eq!(graph.resolve_name_entry("N", sheet).unwrap().vertex, vertex);
        let before = format!("{graph:?}");
        assert_eq!(
            graph.apply_prepared_legacy_graph_plan(plan),
            Err(PreparedLegacyGraphError::Stale)
        );
        assert_eq!(format!("{graph:?}"), before);
    }

    #[test]
    fn same_vertex_table_metadata_change_is_stale_before_mutation() {
        let (mut graph, sheet) = graph();
        let range = RangeRef::new(
            CellRef::new(sheet, Coord::from_excel(10, 1, true, true)),
            CellRef::new(sheet, Coord::from_excel(20, 2, true, true)),
        );
        graph
            .define_table(
                "T",
                range,
                true,
                vec!["A".to_string(), "B".to_string()],
                false,
            )
            .unwrap();
        let mut item = planned(&mut graph, sheet, 1, 1, "=1", &[]);
        item.3.table_refs.push("T".to_string());
        let plan = graph.prepare_legacy_graph_plan(sheet, vec![item]).unwrap();
        let vertex = graph.resolve_table_entry("T").unwrap().vertex;
        let changed_range = RangeRef::new(
            CellRef::new(sheet, Coord::from_excel(10, 1, true, true)),
            CellRef::new(sheet, Coord::from_excel(20, 3, true, true)),
        );
        graph
            .update_table(
                "T",
                changed_range,
                true,
                vec!["A".to_string(), "B".to_string(), "C".to_string()],
                true,
            )
            .unwrap();
        assert_eq!(graph.resolve_table_entry("T").unwrap().vertex, vertex);
        let before = format!("{graph:?}");
        assert_eq!(
            graph.apply_prepared_legacy_graph_plan(plan),
            Err(PreparedLegacyGraphError::Stale)
        );
        assert_eq!(format!("{graph:?}"), before);
    }

    #[test]
    fn same_vertex_source_versions_are_stale_before_mutation() {
        let (mut graph, sheet) = graph();
        graph.define_source_scalar("S", Some(1)).unwrap();
        let mut scalar_item = planned(&mut graph, sheet, 1, 1, "=1", &[]);
        scalar_item.3.source_refs.push("S".to_string());
        let scalar_plan = graph
            .prepare_legacy_graph_plan(sheet, vec![scalar_item])
            .unwrap();
        let scalar_vertex = graph.resolve_source_scalar_entry("S").unwrap().vertex;
        graph.set_source_scalar_version("S", Some(2)).unwrap();
        assert_eq!(
            graph.resolve_source_scalar_entry("S").unwrap().vertex,
            scalar_vertex
        );
        let before = format!("{graph:?}");
        assert_eq!(
            graph.apply_prepared_legacy_graph_plan(scalar_plan),
            Err(PreparedLegacyGraphError::Stale)
        );
        assert_eq!(format!("{graph:?}"), before);

        graph.define_source_table("ST", Some(3)).unwrap();
        let mut table_item = planned(&mut graph, sheet, 2, 1, "=1", &[]);
        table_item.3.table_refs.push("ST".to_string());
        let table_plan = graph
            .prepare_legacy_graph_plan(sheet, vec![table_item])
            .unwrap();
        let table_vertex = graph.resolve_source_table_entry("ST").unwrap().vertex;
        graph.set_source_table_version("ST", Some(4)).unwrap();
        assert_eq!(
            graph.resolve_source_table_entry("ST").unwrap().vertex,
            table_vertex
        );
        let before = format!("{graph:?}");
        assert_eq!(
            graph.apply_prepared_legacy_graph_plan(table_plan),
            Err(PreparedLegacyGraphError::Stale)
        );
        assert_eq!(format!("{graph:?}"), before);
    }

    #[test]
    fn stale_and_fault_checks_happen_before_mutation() {
        let (mut graph, sheet) = graph();
        let item = planned(&mut graph, sheet, 1, 1, "=B1", &[(1, 2)]);
        let plan = graph.prepare_legacy_graph_plan(sheet, vec![item]).unwrap();
        graph
            .set_cell_value("Sheet1", 9, 9, LiteralValue::Number(1.0))
            .unwrap();
        let before = graph.baseline_stats();
        assert_eq!(
            graph.apply_prepared_legacy_graph_plan(plan),
            Err(PreparedLegacyGraphError::Stale)
        );
        assert_eq!(graph.baseline_stats(), before);

        let item = planned(&mut graph, sheet, 2, 1, "=B2", &[(2, 2)]);
        let plan = graph.prepare_legacy_graph_plan(sheet, vec![item]).unwrap();
        graph.prepared_legacy_graph_failure_for_test = true;
        let before = graph.baseline_stats();
        assert_eq!(
            graph.apply_prepared_legacy_graph_plan(plan),
            Err(PreparedLegacyGraphError::InjectedFailure)
        );
        assert_eq!(graph.baseline_stats(), before);
    }
}
