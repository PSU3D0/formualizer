use crate::SheetId;
use crate::engine::arena::{AstNodeId, DataStore};
use crate::engine::sheet_registry::SheetRegistry;
use formualizer_common::Coord as AbsCoord;
use formualizer_common::CoordBuildHasher;
use formualizer_common::ExcelError;
use formualizer_common::PackedSheetCell;
use formualizer_parse::parser::CollectPolicy;
use std::collections::HashMap;

/// Compact range descriptor used during planning (engine-only)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RangeKey {
    Rect {
        sheet: SheetId,
        start: AbsCoord,
        end: AbsCoord, // inclusive
    },
    WholeRow {
        sheet: SheetId,
        row: u32,
    },
    WholeCol {
        sheet: SheetId,
        col: u32,
    },
    /// Partially bounded rectangle; None means unbounded in that direction
    OpenRect {
        sheet: SheetId,
        start_row: Option<u32>,
        start_col: Option<u32>,
        end_row: Option<u32>,
        end_col: Option<u32>,
    },
}

/// Bitflags conveying per-formula traits
pub type FormulaFlags = u8;
pub const F_VOLATILE: FormulaFlags = 0b0000_0001;
pub const F_HAS_RANGES: FormulaFlags = 0b0000_0010;
pub const F_HAS_NAMES: FormulaFlags = 0b0000_0100;
pub const F_HAS_TABLES: FormulaFlags = 0b0001_0000;
pub const F_LIKELY_ARRAY: FormulaFlags = 0b0000_1000;

#[derive(Debug, Default, Clone)]
pub struct DependencyPlan {
    pub formula_targets: Vec<(SheetId, AbsCoord)>,
    pub global_cells: Vec<(SheetId, AbsCoord)>,
    pub vertex_pool: Vec<(SheetId, AbsCoord)>,
    pub vertex_pool_packed: Vec<PackedSheetCell>,
    pub formula_target_pool_indices: Vec<u32>,
    pub global_cell_pool_indices: Vec<u32>,
    pub per_formula_cells: Vec<Vec<u32>>, // indices into global_cells
    pub per_formula_ranges: Vec<Vec<RangeKey>>,
    pub per_formula_names: Vec<Vec<String>>,
    pub per_formula_tables: Vec<Vec<String>>,
    pub per_formula_flags: Vec<FormulaFlags>,
    pub edges_flat: Option<Vec<u32>>, // optional flat adjacency (indices into global_cells)
    pub offsets: Option<Vec<u32>>,    // len = num_formulas + 1 when edges_flat is Some
}

#[derive(Debug, Clone, Copy)]
pub enum DependencyPlanAst<'a> {
    Tree(&'a formualizer_parse::parser::ASTNode),
    Arena(AstNodeId),
}

type EnsureVertexPoolIndex<'a> = dyn FnMut(&mut DependencyPlan, (SheetId, AbsCoord)) -> u32 + 'a;

struct PlanReferenceContext<'a> {
    sheet_reg: &'a mut SheetRegistry,
    data_store: Option<&'a DataStore>,
    current_sheet: SheetId,
    policy: &'a CollectPolicy,
    plan: &'a mut DependencyPlan,
    cell_index: &'a mut HashMap<PackedSheetCell, u32, CoordBuildHasher>,
    ensure_vertex_pool_index: &'a mut EnsureVertexPoolIndex<'a>,
    per_cells: &'a mut Vec<u32>,
    per_ranges: &'a mut Vec<RangeKey>,
    per_names: &'a mut Vec<String>,
    per_tables: &'a mut Vec<String>,
    flags: &'a mut FormulaFlags,
}

fn no_local_bindings(
    _: &PlanReferenceContext<'_>,
    _: &str,
    _: usize,
) -> crate::engine::refs::LocalBindingStyle {
    crate::engine::refs::LocalBindingStyle::None
}

fn plan_data_store<'context>(context: &'context PlanReferenceContext<'_>) -> &'context DataStore {
    context
        .data_store
        .expect("arena traversal requires a data store")
}

fn plan_sheet_registry<'context>(
    context: &'context PlanReferenceContext<'_>,
) -> &'context SheetRegistry {
    context.sheet_reg
}

fn collect_plan_reference(
    context: &mut PlanReferenceContext<'_>,
    reference: crate::engine::refs::SemanticReference<'_>,
) -> Result<(), ExcelError> {
    use crate::engine::refs::SemanticReference;

    match reference {
        SemanticReference::Cell(cell) => {
            let dep_sheet = cell
                .sheet
                .name()
                .map(|name| context.sheet_reg.id_for(name))
                .unwrap_or(context.current_sheet);
            let key = (dep_sheet, AbsCoord::from_excel(cell.row, cell.col));
            let packed = PackedSheetCell::try_new(dep_sheet, key.1.row(), key.1.col())
                .expect("plan dependency coordinate must fit PackedSheetCell");
            let idx = match context.cell_index.get(&packed) {
                Some(&idx) => idx,
                None => {
                    let new_idx = context.plan.global_cells.len() as u32;
                    context.plan.global_cells.push(key);
                    context.cell_index.insert(packed, new_idx);
                    let pool_idx = (context.ensure_vertex_pool_index)(context.plan, key);
                    context.plan.global_cell_pool_indices.push(pool_idx);
                    new_idx
                }
            };
            context.per_cells.push(idx);
        }
        SemanticReference::FiniteRange(range) => {
            let (sr, sc, er, ec) = range
                .finite_bounds()
                .expect("finite reference must have all bounds");
            let area = range.saturating_area().expect("finite area");

            // Planning's caller-owned policy (historically 16 in the standard
            // planning setup) intentionally differs from graph ingest's default 64.
            if context.policy.expand_small_ranges
                && area <= context.policy.range_expansion_limit as u64
            {
                let row_abs = range.start_row_abs && range.end_row_abs;
                let col_abs = range.start_col_abs && range.end_col_abs;
                for row in sr..=er {
                    for col in sc..=ec {
                        collect_plan_reference(
                            context,
                            SemanticReference::Cell(crate::engine::refs::CellReference {
                                original: range.original,
                                sheet: range.sheet,
                                row,
                                col,
                                row_abs,
                                col_abs,
                            }),
                        )?;
                    }
                }
            } else {
                let dep_sheet = range
                    .sheet
                    .name()
                    .map(|name| context.sheet_reg.id_for(name))
                    .unwrap_or(context.current_sheet);
                context.per_ranges.push(RangeKey::Rect {
                    sheet: dep_sheet,
                    start: AbsCoord::from_excel(sr, sc),
                    end: AbsCoord::from_excel(er, ec),
                });
            }
        }
        SemanticReference::OpenRange(range) => {
            let dep_sheet = range
                .sheet
                .name()
                .map(|name| context.sheet_reg.id_for(name))
                .unwrap_or(context.current_sheet);
            match (
                range.start_row,
                range.start_col,
                range.end_row,
                range.end_col,
            ) {
                (None, Some(col), None, Some(end_col)) if col == end_col => {
                    context.per_ranges.push(RangeKey::WholeCol {
                        sheet: dep_sheet,
                        col,
                    })
                }
                (Some(row), None, Some(end_row), None) if row == end_row => {
                    context.per_ranges.push(RangeKey::WholeRow {
                        sheet: dep_sheet,
                        row,
                    })
                }
                _ => context.per_ranges.push(RangeKey::OpenRect {
                    sheet: dep_sheet,
                    start_row: range.start_row.map(|row| row.saturating_sub(1)),
                    start_col: range.start_col.map(|col| col.saturating_sub(1)),
                    end_row: range.end_row.map(|row| row.saturating_sub(1)),
                    end_col: range.end_col.map(|col| col.saturating_sub(1)),
                }),
            }
        }
        SemanticReference::ExternalSource(external) => match external.kind {
            formualizer_parse::parser::ExternalRefKind::Cell { .. } => {
                *context.flags |= F_HAS_NAMES;
                context.per_names.push(external.raw.clone());
            }
            formualizer_parse::parser::ExternalRefKind::Range { .. } => {
                *context.flags |= F_HAS_TABLES;
                context.per_tables.push(external.raw.clone());
            }
        },
        SemanticReference::Name(name) => {
            if context.policy.include_names {
                *context.flags |= F_HAS_NAMES;
                context.per_names.push(name.to_string());
            }
        }
        SemanticReference::Table(table) => {
            *context.flags |= F_HAS_TABLES;
            context.per_tables.push(table.name.clone());
        }
        SemanticReference::ThreeDimensional(_) | SemanticReference::Unsupported(_) => {}
    }
    Ok(())
}

/// Build a compact dependency plan from ASTs without mutating the graph.
/// Sheets referenced by name are resolved/created through SheetRegistry at plan time.
pub fn build_dependency_plan<'a, I>(
    sheet_reg: &mut SheetRegistry,
    formulas: I,
    policy: &CollectPolicy,
    volatile_flags: Option<&[bool]>,
) -> Result<DependencyPlan, ExcelError>
where
    I: Iterator<Item = (&'a str, u32, u32, &'a formualizer_parse::parser::ASTNode)>,
{
    let mut plan = DependencyPlan::default();

    // Global cell pool: packed absolute cell -> index.
    //
    // Uses CoordBuildHasher because FxHasher's weak avalanche collides badly
    // on structured packed keys (PackedSheetCell reserves bits 50..64 and has
    // narrow dynamic range on row-major workloads), turning this O(N) loop
    // into O(N^2). See formualizer_common::coord_hash.
    let mut cell_index: HashMap<PackedSheetCell, u32, CoordBuildHasher> =
        HashMap::with_hasher(CoordBuildHasher);
    // Unified vertex pool for loader-specialized ensure.
    let mut vertex_pool_index: HashMap<PackedSheetCell, u32, CoordBuildHasher> =
        HashMap::with_hasher(CoordBuildHasher);

    let mut ensure_vertex_pool_index =
        |plan: &mut DependencyPlan, key: (SheetId, AbsCoord)| -> u32 {
            let packed = PackedSheetCell::try_new(key.0, key.1.row(), key.1.col())
                .expect("plan vertex pool coordinate must fit PackedSheetCell");
            match vertex_pool_index.get(&packed) {
                Some(&idx) => idx,
                None => {
                    let new_idx = plan.vertex_pool.len() as u32;
                    plan.vertex_pool.push(key);
                    plan.vertex_pool_packed.push(packed);
                    vertex_pool_index.insert(packed, new_idx);
                    new_idx
                }
            }
        };

    for (i, (sheet_name, row, col, ast)) in formulas.enumerate() {
        let sheet_id = sheet_reg.id_for(sheet_name);
        let target = (sheet_id, AbsCoord::from_excel(row, col));
        plan.formula_targets.push(target);
        let target_pool_idx = ensure_vertex_pool_index(&mut plan, target);
        plan.formula_target_pool_indices.push(target_pool_idx);

        let mut flags: FormulaFlags = 0;
        if let Some(v) = volatile_flags.and_then(|v| v.get(i)).copied()
            && v
        {
            flags |= F_VOLATILE;
        }

        let mut per_cells: Vec<u32> = Vec::new();
        let mut per_ranges: Vec<RangeKey> = Vec::new();
        let mut per_names: Vec<String> = Vec::new();
        let mut per_tables: Vec<String> = Vec::new();

        {
            let mut context = PlanReferenceContext {
                sheet_reg,
                data_store: None,
                current_sheet: sheet_id,
                policy,
                plan: &mut plan,
                cell_index: &mut cell_index,
                ensure_vertex_pool_index: &mut ensure_vertex_pool_index,
                per_cells: &mut per_cells,
                per_ranges: &mut per_ranges,
                per_names: &mut per_names,
                per_tables: &mut per_tables,
                flags: &mut flags,
            };
            crate::engine::refs::visit_tree_references(
                ast,
                &mut context,
                no_local_bindings,
                collect_plan_reference,
            )?;
        }

        plan.per_formula_cells.push(per_cells);
        plan.per_formula_ranges.push(per_ranges);
        plan.per_formula_names.push(per_names);
        plan.per_formula_tables.push(per_tables);
        plan.per_formula_flags.push(flags);
    }

    Ok(plan)
}

/// Build a compact dependency plan from a mix of tree and arena ASTs.
pub fn build_dependency_plan_mixed<'a, I>(
    sheet_reg: &mut SheetRegistry,
    data_store: &DataStore,
    formulas: I,
    policy: &CollectPolicy,
    volatile_flags: Option<&[bool]>,
) -> Result<DependencyPlan, ExcelError>
where
    I: Iterator<Item = (&'a str, u32, u32, DependencyPlanAst<'a>)>,
{
    let mut plan = DependencyPlan::default();

    let mut cell_index: HashMap<PackedSheetCell, u32, CoordBuildHasher> =
        HashMap::with_hasher(CoordBuildHasher);
    let mut vertex_pool_index: HashMap<PackedSheetCell, u32, CoordBuildHasher> =
        HashMap::with_hasher(CoordBuildHasher);

    let mut ensure_vertex_pool_index =
        |plan: &mut DependencyPlan, key: (SheetId, AbsCoord)| -> u32 {
            let packed = PackedSheetCell::try_new(key.0, key.1.row(), key.1.col())
                .expect("plan vertex pool coordinate must fit PackedSheetCell");
            match vertex_pool_index.get(&packed) {
                Some(&idx) => idx,
                None => {
                    let new_idx = plan.vertex_pool.len() as u32;
                    plan.vertex_pool.push(key);
                    plan.vertex_pool_packed.push(packed);
                    vertex_pool_index.insert(packed, new_idx);
                    new_idx
                }
            }
        };

    for (i, (sheet_name, row, col, ast)) in formulas.enumerate() {
        let sheet_id = sheet_reg.id_for(sheet_name);
        let target = (sheet_id, AbsCoord::from_excel(row, col));
        plan.formula_targets.push(target);
        let target_pool_idx = ensure_vertex_pool_index(&mut plan, target);
        plan.formula_target_pool_indices.push(target_pool_idx);

        let mut flags: FormulaFlags = 0;
        if let Some(v) = volatile_flags.and_then(|v| v.get(i)).copied()
            && v
        {
            flags |= F_VOLATILE;
        }

        let mut per_cells: Vec<u32> = Vec::new();
        let mut per_ranges: Vec<RangeKey> = Vec::new();
        let mut per_names: Vec<String> = Vec::new();
        let mut per_tables: Vec<String> = Vec::new();

        {
            let mut context = PlanReferenceContext {
                sheet_reg,
                data_store: Some(data_store),
                current_sheet: sheet_id,
                policy,
                plan: &mut plan,
                cell_index: &mut cell_index,
                ensure_vertex_pool_index: &mut ensure_vertex_pool_index,
                per_cells: &mut per_cells,
                per_ranges: &mut per_ranges,
                per_names: &mut per_names,
                per_tables: &mut per_tables,
                flags: &mut flags,
            };
            match ast {
                DependencyPlanAst::Tree(ast) => crate::engine::refs::visit_tree_references(
                    ast,
                    &mut context,
                    no_local_bindings,
                    collect_plan_reference,
                )?,
                DependencyPlanAst::Arena(ast_id) => crate::engine::refs::visit_arena_references(
                    ast_id,
                    &mut context,
                    plan_data_store,
                    plan_sheet_registry,
                    collect_plan_reference,
                )?,
            }
        }

        plan.per_formula_cells.push(per_cells);
        plan.per_formula_ranges.push(per_ranges);
        plan.per_formula_names.push(per_names);
        plan.per_formula_tables.push(per_tables);
        plan.per_formula_flags.push(flags);
    }

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::arena::DataStore;
    use crate::engine::sheet_registry::SheetRegistry;
    use formualizer_parse::parse;

    #[test]
    fn overflow_sized_ranges_stay_compressed_in_plan() {
        for formula in [
            "=SUM(A1:FLA983055)",
            "=SUM(A1:XFD262144)",
            "=SUM(A1:XFD1048576)",
        ] {
            let ast = parse(formula).unwrap();
            let policy = CollectPolicy {
                expand_small_ranges: true,
                range_expansion_limit: 64,
                include_names: true,
            };
            let mut registry = SheetRegistry::new();
            let plan = build_dependency_plan(
                &mut registry,
                std::iter::once(("Sheet1", 1, 1, &ast)),
                &policy,
                None,
            )
            .unwrap();
            assert!(plan.per_formula_cells[0].is_empty(), "{formula}");
            assert_eq!(plan.per_formula_ranges[0].len(), 1, "{formula}");
        }
    }

    #[test]
    fn tree_plan_handles_deep_left_associative_formula_without_call_stack_growth() {
        let terms = if cfg!(debug_assertions) {
            20_000
        } else {
            100_000
        };
        let formula = format!(
            "={}",
            std::iter::repeat_n("A1", terms)
                .collect::<Vec<_>>()
                .join("+")
        );
        let ast = parse(&formula).unwrap();
        let policy = CollectPolicy {
            expand_small_ranges: true,
            range_expansion_limit: 16,
            include_names: true,
        };
        let mut registry = SheetRegistry::new();
        let plan = build_dependency_plan(
            &mut registry,
            std::iter::once(("Sheet1", 1, 1, &ast)),
            &policy,
            None,
        )
        .unwrap();
        assert_eq!(plan.global_cells.len(), 1);
        assert_eq!(plan.per_formula_cells[0].len(), terms);

        // ASTNode owns a recursively boxed tree, so avoid making this traversal
        // test depend on the standard library's recursive drop implementation.
        std::mem::forget(ast);
    }

    #[test]
    fn mixed_arena_plan_matches_tree_plan_for_basic_refs() {
        let asts = [
            parse("=A1+SUM(B2:C3)+NamedThing").unwrap(),
            parse("=Sheet2!D4+Table1[#Data]").unwrap(),
        ];
        let policy = CollectPolicy {
            expand_small_ranges: true,
            range_expansion_limit: 16,
            include_names: true,
        };

        let mut tree_reg = SheetRegistry::new();
        let tree_plan = build_dependency_plan(
            &mut tree_reg,
            asts.iter()
                .enumerate()
                .map(|(i, ast)| ("Sheet1", (i + 1) as u32, 5, ast)),
            &policy,
            Some(&[false, true]),
        )
        .unwrap();

        let mut arena_reg = SheetRegistry::new();
        arena_reg.id_for("Sheet1");
        let mut store = DataStore::new();
        let ids: Vec<_> = asts
            .iter()
            .map(|ast| store.store_ast(ast, &arena_reg))
            .collect();
        let arena_plan = build_dependency_plan_mixed(
            &mut arena_reg,
            &store,
            ids.iter()
                .enumerate()
                .map(|(i, id)| ("Sheet1", (i + 1) as u32, 5, DependencyPlanAst::Arena(*id))),
            &policy,
            Some(&[false, true]),
        )
        .unwrap();

        assert_eq!(arena_plan.formula_targets, tree_plan.formula_targets);
        assert_eq!(arena_plan.global_cells, tree_plan.global_cells);
        assert_eq!(arena_plan.vertex_pool, tree_plan.vertex_pool);
        assert_eq!(
            arena_plan.formula_target_pool_indices,
            tree_plan.formula_target_pool_indices
        );
        assert_eq!(
            arena_plan.global_cell_pool_indices,
            tree_plan.global_cell_pool_indices
        );
        assert_eq!(arena_plan.per_formula_cells, tree_plan.per_formula_cells);
        assert_eq!(arena_plan.per_formula_ranges, tree_plan.per_formula_ranges);
        assert_eq!(arena_plan.per_formula_names, tree_plan.per_formula_names);
        assert_eq!(arena_plan.per_formula_tables, tree_plan.per_formula_tables);
        assert_eq!(arena_plan.per_formula_flags, tree_plan.per_formula_flags);
    }
}
