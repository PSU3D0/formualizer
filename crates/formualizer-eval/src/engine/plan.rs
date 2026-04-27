use crate::SheetId;
use crate::engine::sheet_registry::SheetRegistry;
use formualizer_common::Coord as AbsCoord;
use formualizer_common::CoordBuildHasher;
use formualizer_common::ExcelError;
use formualizer_common::PackedSheetCell;
use formualizer_parse::parser::{CollectPolicy, ReferenceType};
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
        start: Option<AbsCoord>,
        end: Option<AbsCoord>,
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
                            start: start_row
                                .zip(start_col)
                                .map(|(r, c)| AbsCoord::from_excel(r, c)),
                            end: end_row
                                .zip(end_col)
                                .map(|(r, c)| AbsCoord::from_excel(r, c)),
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
