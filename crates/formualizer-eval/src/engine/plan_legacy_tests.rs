//! Differential plan oracle frozen from `ccfeaf83`.
//!
//! The functions below are a test-only verbatim extraction of `engine/plan.rs`
//! at that commit. Only function names were prefixed with `legacy_`.

use super::arena::{AstNodeData, AstNodeId, DataStore};
use super::plan::*;
use super::sheet_registry::SheetRegistry;
use crate::SheetId;
use formualizer_common::{Coord as AbsCoord, CoordBuildHasher, ExcelError, PackedSheetCell};
use formualizer_parse::parser::{CollectPolicy, ReferenceType};
use std::collections::HashMap;

fn legacy_collect_references_arena(
    data_store: &DataStore,
    ast_id: AstNodeId,
    sheet_reg: &SheetRegistry,
    policy: &CollectPolicy,
) -> Result<Vec<ReferenceType>, ExcelError> {
    let mut out = Vec::new();
    let mut stack = Vec::with_capacity(8);
    stack.push(ast_id);

    while let Some(node_id) = stack.pop() {
        let Some(node) = data_store.get_node(node_id) else {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Value)
                .with_message("Missing interned formula AST"));
        };

        match node {
            AstNodeData::Reference { ref_type, .. } => {
                let reference = data_store.reconstruct_reference_type_for_eval(ref_type, sheet_reg);
                match reference {
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
                    } => {
                        if policy.expand_small_ranges
                            && let (Some(sr), Some(sc), Some(er), Some(ec)) =
                                (start_row, start_col, end_row, end_col)
                        {
                            let rows = er.saturating_sub(sr) + 1;
                            let cols = ec.saturating_sub(sc) + 1;
                            let area = rows.saturating_mul(cols);
                            if area as usize <= policy.range_expansion_limit {
                                let row_abs = start_row_abs && end_row_abs;
                                let col_abs = start_col_abs && end_col_abs;
                                for r in sr..=er {
                                    for c in sc..=ec {
                                        out.push(ReferenceType::Cell {
                                            sheet: sheet.clone(),
                                            row: r,
                                            col: c,
                                            row_abs,
                                            col_abs,
                                        });
                                    }
                                }
                                continue;
                            }
                        }
                        out.push(ReferenceType::Range {
                            sheet,
                            start_row,
                            start_col,
                            end_row,
                            end_col,
                            start_row_abs,
                            start_col_abs,
                            end_row_abs,
                            end_col_abs,
                        });
                    }
                    ReferenceType::NamedRange(_) if !policy.include_names => {}
                    other => out.push(other),
                }
            }
            AstNodeData::UnaryOp { expr_id, .. } => stack.push(*expr_id),
            AstNodeData::BinaryOp {
                left_id, right_id, ..
            } => {
                stack.push(*right_id);
                stack.push(*left_id);
            }
            AstNodeData::Function { .. } => {
                if let Some(args) = data_store.get_args(node_id) {
                    for arg in args.iter().rev() {
                        stack.push(*arg);
                    }
                }
            }
            AstNodeData::Array { .. } => {
                if let Some((_, _, elems)) = data_store.get_array_elems(node_id) {
                    for elem in elems.iter().rev() {
                        stack.push(*elem);
                    }
                }
            }
            AstNodeData::Literal(_) | AstNodeData::Omitted => {}
        }
    }

    Ok(out)
}

/// Build a compact dependency plan from ASTs without mutating the graph.
/// Sheets referenced by name are resolved/created through SheetRegistry at plan time.
fn legacy_build_dependency_plan<'a, I>(
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

        // Collect references using core collector (may expand small ranges per policy)
        let refs = ast.collect_references(policy);
        for r in refs {
            match r {
                ReferenceType::Cell {
                    sheet, row, col, ..
                } => {
                    let dep_sheet = sheet
                        .as_deref()
                        .map(|name| sheet_reg.id_for(name))
                        .unwrap_or(sheet_id);
                    let key = (dep_sheet, AbsCoord::from_excel(row, col));
                    let packed = PackedSheetCell::try_new(dep_sheet, key.1.row(), key.1.col())
                        .expect("plan dependency coordinate must fit PackedSheetCell");
                    let idx = match cell_index.get(&packed) {
                        Some(&idx) => idx,
                        None => {
                            let new_idx = plan.global_cells.len() as u32;
                            plan.global_cells.push(key);
                            cell_index.insert(packed, new_idx);
                            let pool_idx = ensure_vertex_pool_index(&mut plan, key);
                            plan.global_cell_pool_indices.push(pool_idx);
                            new_idx
                        }
                    };
                    per_cells.push(idx);
                }
                ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    ..
                } => {
                    let dep_sheet = sheet
                        .as_deref()
                        .map(|name| sheet_reg.id_for(name))
                        .unwrap_or(sheet_id);
                    match (start_row, start_col, end_row, end_col) {
                        (Some(sr), Some(sc), Some(er), Some(ec)) => {
                            per_ranges.push(RangeKey::Rect {
                                sheet: dep_sheet,
                                start: AbsCoord::from_excel(sr, sc),
                                end: AbsCoord::from_excel(er, ec),
                            })
                        }
                        (None, Some(c), None, Some(ec)) if c == ec => {
                            per_ranges.push(RangeKey::WholeCol {
                                sheet: dep_sheet,
                                col: c,
                            })
                        }
                        (Some(r), None, Some(er), None) if r == er => {
                            per_ranges.push(RangeKey::WholeRow {
                                sheet: dep_sheet,
                                row: r,
                            })
                        }
                        _ => per_ranges.push(RangeKey::OpenRect {
                            sheet: dep_sheet,
                            start_row: start_row.map(|row| row.saturating_sub(1)),
                            start_col: start_col.map(|col| col.saturating_sub(1)),
                            end_row: end_row.map(|row| row.saturating_sub(1)),
                            end_col: end_col.map(|col| col.saturating_sub(1)),
                        }),
                    }
                }
                ReferenceType::External(ext) => match ext.kind {
                    formualizer_parse::parser::ExternalRefKind::Cell { .. } => {
                        flags |= F_HAS_NAMES;
                        per_names.push(ext.raw.clone());
                    }
                    formualizer_parse::parser::ExternalRefKind::Range { .. } => {
                        flags |= F_HAS_TABLES;
                        per_tables.push(ext.raw.clone());
                    }
                },
                ReferenceType::NamedRange(name) => {
                    // Resolution handled later; mark via flags if caller cares
                    flags |= F_HAS_NAMES;
                    per_names.push(name);
                }
                ReferenceType::Table(tref) => {
                    flags |= F_HAS_TABLES;
                    per_tables.push(tref.name);
                }
                // 3D refs are parsed but not yet planned. They neither create
                // dependencies nor participate in the cell/range plan; the
                // evaluator will surface #N/IMPL! when one is encountered.
                ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => {}
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

/// Build a compact dependency plan from a mix of tree and arena ASTs.
fn legacy_build_dependency_plan_mixed<'a, I>(
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

        let refs = match ast {
            DependencyPlanAst::Tree(ast) => ast.collect_references(policy).into_iter().collect(),
            DependencyPlanAst::Arena(ast_id) => {
                legacy_collect_references_arena(data_store, ast_id, sheet_reg, policy)?
            }
        };
        for r in refs {
            match r {
                ReferenceType::Cell {
                    sheet, row, col, ..
                } => {
                    let dep_sheet = sheet
                        .as_deref()
                        .map(|name| sheet_reg.id_for(name))
                        .unwrap_or(sheet_id);
                    let key = (dep_sheet, AbsCoord::from_excel(row, col));
                    let packed = PackedSheetCell::try_new(dep_sheet, key.1.row(), key.1.col())
                        .expect("plan dependency coordinate must fit PackedSheetCell");
                    let idx = match cell_index.get(&packed) {
                        Some(&idx) => idx,
                        None => {
                            let new_idx = plan.global_cells.len() as u32;
                            plan.global_cells.push(key);
                            cell_index.insert(packed, new_idx);
                            let pool_idx = ensure_vertex_pool_index(&mut plan, key);
                            plan.global_cell_pool_indices.push(pool_idx);
                            new_idx
                        }
                    };
                    per_cells.push(idx);
                }
                ReferenceType::Range {
                    sheet,
                    start_row,
                    start_col,
                    end_row,
                    end_col,
                    ..
                } => {
                    let dep_sheet = sheet
                        .as_deref()
                        .map(|name| sheet_reg.id_for(name))
                        .unwrap_or(sheet_id);
                    match (start_row, start_col, end_row, end_col) {
                        (Some(sr), Some(sc), Some(er), Some(ec)) => {
                            per_ranges.push(RangeKey::Rect {
                                sheet: dep_sheet,
                                start: AbsCoord::from_excel(sr, sc),
                                end: AbsCoord::from_excel(er, ec),
                            })
                        }
                        (None, Some(c), None, Some(ec)) if c == ec => {
                            per_ranges.push(RangeKey::WholeCol {
                                sheet: dep_sheet,
                                col: c,
                            })
                        }
                        (Some(r), None, Some(er), None) if r == er => {
                            per_ranges.push(RangeKey::WholeRow {
                                sheet: dep_sheet,
                                row: r,
                            })
                        }
                        _ => per_ranges.push(RangeKey::OpenRect {
                            sheet: dep_sheet,
                            start_row: start_row.map(|row| row.saturating_sub(1)),
                            start_col: start_col.map(|col| col.saturating_sub(1)),
                            end_row: end_row.map(|row| row.saturating_sub(1)),
                            end_col: end_col.map(|col| col.saturating_sub(1)),
                        }),
                    }
                }
                ReferenceType::External(ext) => match ext.kind {
                    formualizer_parse::parser::ExternalRefKind::Cell { .. } => {
                        flags |= F_HAS_NAMES;
                        per_names.push(ext.raw.clone());
                    }
                    formualizer_parse::parser::ExternalRefKind::Range { .. } => {
                        flags |= F_HAS_TABLES;
                        per_tables.push(ext.raw.clone());
                    }
                },
                ReferenceType::NamedRange(name) => {
                    flags |= F_HAS_NAMES;
                    per_names.push(name);
                }
                ReferenceType::Table(tref) => {
                    flags |= F_HAS_TABLES;
                    per_tables.push(tref.name);
                }
                ReferenceType::Cell3D { .. } | ReferenceType::Range3D { .. } => {}
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
mod differential {
    use super::*;
    use formualizer_parse::parse;
    use proptest::prelude::*;

    fn assert_plan_eq(old: &DependencyPlan, new: &DependencyPlan) {
        assert_eq!(old.formula_targets, new.formula_targets);
        assert_eq!(old.global_cells, new.global_cells);
        assert_eq!(old.vertex_pool, new.vertex_pool);
        assert_eq!(old.vertex_pool_packed, new.vertex_pool_packed);
        assert_eq!(
            old.formula_target_pool_indices,
            new.formula_target_pool_indices
        );
        assert_eq!(old.global_cell_pool_indices, new.global_cell_pool_indices);
        assert_eq!(old.per_formula_cells, new.per_formula_cells);
        assert_eq!(old.per_formula_ranges, new.per_formula_ranges);
        assert_eq!(old.per_formula_names, new.per_formula_names);
        assert_eq!(old.per_formula_tables, new.per_formula_tables);
        assert_eq!(old.per_formula_flags, new.per_formula_flags);
        assert_eq!(old.edges_flat, new.edges_flat);
        assert_eq!(old.offsets, new.offsets);
    }

    fn fixture_registry() -> SheetRegistry {
        let mut registry = SheetRegistry::new();
        registry.id_for("Sheet1");
        registry.id_for("Sheet2");
        registry
    }

    fn sorted_sheet_names(registry: &SheetRegistry) -> Vec<String> {
        let mut names: Vec<_> = registry
            .all_sheets()
            .into_iter()
            .map(|(_, name)| name)
            .collect();
        names.sort();
        names
    }

    fn registry_delta(before: &[String], registry: &SheetRegistry) -> Vec<String> {
        sorted_sheet_names(registry)
            .into_iter()
            .filter(|name| !before.contains(name))
            .collect()
    }

    fn assert_outcome_eq(
        formula: &str,
        path: &str,
        old: Result<DependencyPlan, ExcelError>,
        new: Result<DependencyPlan, ExcelError>,
    ) {
        match (old, new) {
            (Ok(old), Ok(new)) => assert_plan_eq(&old, &new),
            (Err(old), Err(new)) => assert_eq!(old, new, "{path} formula={formula}"),
            (old, new) => {
                panic!("plan outcome mismatch for {path} formula={formula}: {old:?} != {new:?}")
            }
        }
    }

    fn assert_formula_parity(formula: &str, policy: CollectPolicy) {
        let ast = parse(formula).unwrap_or_else(|error| panic!("{formula}: {error}"));

        let mut old_registry = fixture_registry();
        let mut new_registry = fixture_registry();
        let tree_before = sorted_sheet_names(&old_registry);
        assert_eq!(tree_before, sorted_sheet_names(&new_registry));
        let old = legacy_build_dependency_plan(
            &mut old_registry,
            std::iter::once(("Sheet1", 10, 10, &ast)),
            &policy,
            None,
        );
        let new = build_dependency_plan(
            &mut new_registry,
            std::iter::once(("Sheet1", 10, 10, &ast)),
            &policy,
            None,
        );
        assert_outcome_eq(formula, "tree", old, new);
        assert_eq!(
            registry_delta(&tree_before, &old_registry),
            registry_delta(&tree_before, &new_registry),
            "tree registry delta formula={formula}"
        );

        let mut old_arena_registry = fixture_registry();
        let mut new_arena_registry = fixture_registry();
        let arena_before = sorted_sheet_names(&old_arena_registry);
        assert_eq!(arena_before, sorted_sheet_names(&new_arena_registry));
        let mut old_store = DataStore::new();
        let old_ast_id = old_store.store_ast(&ast, &old_arena_registry);
        let mut new_store = DataStore::new();
        let new_ast_id = new_store.store_ast(&ast, &new_arena_registry);
        let old_arena = legacy_build_dependency_plan_mixed(
            &mut old_arena_registry,
            &old_store,
            std::iter::once(("Sheet1", 10, 10, DependencyPlanAst::Arena(old_ast_id))),
            &policy,
            None,
        );
        let new_arena = build_dependency_plan_mixed(
            &mut new_arena_registry,
            &new_store,
            std::iter::once(("Sheet1", 10, 10, DependencyPlanAst::Arena(new_ast_id))),
            &policy,
            None,
        );
        assert_outcome_eq(formula, "arena", old_arena, new_arena);
        assert_eq!(
            registry_delta(&arena_before, &old_arena_registry),
            registry_delta(&arena_before, &new_arena_registry),
            "arena registry delta formula={formula}"
        );
    }

    #[test]
    fn frozen_plan_matches_reference_classes_and_reversed_range_quirk() {
        let formulas = [
            "=SUM(A1,$B2,C$3,$D$4)",
            "=SUM(A1:A1,A1:B2,Sheet2!C3:D4,A1:A,A1:1,A:A,1:1)",
            "=SUM(Sheet2!A1,NamedThing,Table1[#Data])",
            "=SUM([book]Sheet!A1,[book]Sheet!A1:B2)",
            "=SUM(Sheet1:Sheet2!A1,Sheet1:Sheet2!B2:C3)",
            "=SUM({A1,B2;C3,D4},IF(D4,,E5))",
            // Existing planner behavior: with expansion enabled this reversed
            // range contributes no cells rather than returning #REF!.
            "=SUM(D4:B2)",
            "=Ghost!D4:B2",
            "=Sheet2!D4:B2",
        ];
        for formula in formulas {
            for limit in [0, 1, 4, 16, 64] {
                assert_formula_parity(
                    formula,
                    CollectPolicy {
                        expand_small_ranges: true,
                        range_expansion_limit: limit,
                        include_names: true,
                    },
                );
            }
        }
    }

    fn atom() -> impl Strategy<Value = &'static str> {
        prop_oneof![
            Just("A1"),
            Just("$B2"),
            Just("C$3"),
            Just("A1:B2"),
            Just("B2:E6"),
            Just("A1:A"),
            Just("A1:1"),
            Just("A:A"),
            Just("1:1"),
            Just("Sheet2!E5"),
            Just("Sheet2!A1:C3"),
            Just("NamedThing"),
            Just("Table1[#Data]"),
            Just("Sheet1:Sheet2!A1"),
            Just("IF(A1,,B2)"),
            Just("SUM(A1,SUM(B2,SUM(C3,D4)))"),
            Just("SUM({A1,B2;C3,D4})"),
            Just("D4:B2"),
        ]
    }

    // Fixed seeds make CI failures reproducible without committing persistence artifacts.
    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            rng_seed: proptest::test_runner::RngSeed::Fixed(0x504c_414e),
            ..ProptestConfig::default()
        })]

        #[test]
        fn generated_formulas_match_frozen_plan(
            atoms in prop::collection::vec(atom(), 1..8),
            expand in any::<bool>(),
            include_names in any::<bool>(),
            limit in prop_oneof![Just(0usize), Just(1), Just(4), Just(16), Just(64)],
        ) {
            let formula = format!("={}", atoms.join("+"));
            assert_formula_parity(
                &formula,
                CollectPolicy { expand_small_ranges: expand, range_expansion_limit: limit, include_names },
            );
        }
    }
}
