use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{
    ScaleState, completed_cycles, detect_nonempty_rows, fixture_path, has_evaluated_formulas,
    numeric,
};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const DENSE_EDIT_LEN: usize = 16;
const SPARSE_EDITS: usize = 16;
const BULK_EDIT_LEN: usize = 50;

pub struct S031FinanceAnchoredWithEditCycles {
    scale: ScaleState,
}

impl Default for S031FinanceAnchoredWithEditCycles {
    fn default() -> Self {
        Self::new()
    }
}

impl S031FinanceAnchoredWithEditCycles {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    pub fn rows(scale: ScenarioScale) -> u32 {
        match scale {
            ScenarioScale::Small => 1_000,
            ScenarioScale::Medium => 10_000,
            ScenarioScale::Large => 50_000,
        }
    }
}

impl Scenario for S031FinanceAnchoredWithEditCycles {
    fn id(&self) -> &'static str {
        "s031-finance-anchored-with-edit-cycles"
    }

    fn description(&self) -> &'static str {
        "Finance A*B*$F$1 family and SUM rollup with eight deterministic single and bulk edit cycles."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::AnchoredArithmetic,
            ScenarioTag::SingleCellEdit,
            ScenarioTag::BulkEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row0 in 0..rows {
                let row = row0 + 1;
                sheet.get_cell_mut((1, row)).set_value_number(row as f64);
                sheet
                    .get_cell_mut((2, row))
                    .set_value_number(initial_price(row));
                sheet
                    .get_cell_mut((3, row))
                    .set_formula(format!("=A{row}*B{row}*$F$1"));
            }
            sheet.get_cell_mut((6, 1)).set_value_number(1.0);
            sheet
                .get_cell_mut((7, 1))
                .set_formula(format!("=SUM(C1:C{rows})"));
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 7,
                sheets: 1,
                formula_cells: rows + 1,
                value_cells: rows.saturating_mul(2) + 1,
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 8,
            apply: apply_edit,
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let rows = Self::rows(self.scale.get_or_small());
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            invariants.reserve(rows as usize + 1);
            for row in 1..=rows {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 3,
                    expected: numeric(line_value(row, rows, cycles)),
                });
            }
            invariants.push(ScenarioInvariant::CellEquals {
                sheet: "Sheet1".to_string(),
                row: 1,
                col: 7,
                expected: numeric(expected_rollup(rows, cycles)),
            });
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let rows = detect_nonempty_rows(wb, "Sheet1", 1) as usize;
    match cycle {
        0 => {
            wb.set_value("Sheet1", 1, 6, LiteralValue::Number(2.0))?;
            Ok("multiplier_2")
        }
        1 => {
            for idx in 0..DENSE_EDIT_LEN.min(rows).max(1) {
                let row0 = (11 + idx) % rows;
                wb.set_value(
                    "Sheet1",
                    row0 as u32 + 1,
                    1,
                    LiteralValue::Number(dense_unit_value(cycle, idx)),
                )?;
            }
            Ok("dense_units_16")
        }
        2 => {
            for idx in 0..SPARSE_EDITS.min(rows).max(1) {
                let row0 = sparse_row0(cycle, idx, rows);
                wb.set_value(
                    "Sheet1",
                    row0 as u32 + 1,
                    2,
                    LiteralValue::Number(sparse_price_value(cycle, idx)),
                )?;
            }
            Ok("sparse_prices_16")
        }
        3 => {
            for idx in 0..BULK_EDIT_LEN.min(rows).max(1) {
                let row0 = bulk_a_start(cycle, rows) + idx;
                wb.set_value(
                    "Sheet1",
                    row0 as u32 + 1,
                    1,
                    LiteralValue::Number(bulk_a_value(cycle, idx)),
                )?;
            }
            Ok("bulk_units_contiguous_50")
        }
        4 => {
            for idx in 0..BULK_EDIT_LEN.min(rows).max(1) {
                let row0 = bulk_b_row0(cycle, idx, rows);
                wb.set_value(
                    "Sheet1",
                    row0 as u32 + 1,
                    2,
                    LiteralValue::Number(bulk_b_value(cycle, idx)),
                )?;
            }
            Ok("bulk_prices_sparse_50")
        }
        5 => {
            wb.set_value("Sheet1", 1, 6, LiteralValue::Number(3.5))?;
            Ok("multiplier_3_5")
        }
        6 => {
            for idx in 0..BULK_EDIT_LEN.min(rows).max(1) {
                let row0 = bulk_a_start(cycle, rows) + idx;
                wb.set_value(
                    "Sheet1",
                    row0 as u32 + 1,
                    1,
                    LiteralValue::Number(bulk_a_value(cycle, idx)),
                )?;
            }
            Ok("bulk_units_contiguous_50_b")
        }
        7 => {
            for idx in 0..BULK_EDIT_LEN.min(rows).max(1) {
                let row0 = bulk_b_row0(cycle, idx, rows);
                wb.set_value(
                    "Sheet1",
                    row0 as u32 + 1,
                    2,
                    LiteralValue::Number(bulk_b_value(cycle, idx)),
                )?;
            }
            Ok("bulk_prices_sparse_50_b")
        }
        _ => Ok("noop"),
    }
}

fn initial_price(row: u32) -> f64 {
    10.0 + ((row - 1) % 17) as f64
}

fn dense_unit_value(cycle: usize, idx: usize) -> f64 {
    1_000.0 + cycle as f64 + idx as f64
}

fn sparse_price_value(cycle: usize, idx: usize) -> f64 {
    20.0 + ((cycle + idx) % 23) as f64
}

fn bulk_a_start(cycle: usize, rows: usize) -> usize {
    ((cycle * 101) % rows).min(rows.saturating_sub(BULK_EDIT_LEN))
}

fn bulk_a_value(cycle: usize, idx: usize) -> f64 {
    2_000.0 + cycle as f64 * 10.0 + idx as f64
}

fn bulk_b_row0(cycle: usize, idx: usize, rows: usize) -> usize {
    (cycle * 53 + idx * 97) % rows
}

fn bulk_b_value(cycle: usize, idx: usize) -> f64 {
    50.0 + cycle as f64 + idx as f64 * 0.5
}

fn sparse_row0(cycle: usize, idx: usize, rows: usize) -> usize {
    (cycle * 53 + idx * 97) % rows
}

fn line_value(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    unit_at(row, rows, completed_cycles)
        * price_at(row, rows, completed_cycles)
        * multiplier_at(completed_cycles)
}

fn expected_rollup(rows: u32, completed_cycles: usize) -> f64 {
    (1..=rows)
        .map(|row| line_value(row, rows, completed_cycles))
        .sum()
}

fn unit_at(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    let mut unit = row as f64;
    let rows_usize = rows as usize;
    for cycle in 0..completed_cycles {
        match cycle {
            1 => {
                for idx in 0..DENSE_EDIT_LEN.min(rows_usize).max(1) {
                    if row == ((11 + idx) % rows_usize) as u32 + 1 {
                        unit = dense_unit_value(cycle, idx);
                    }
                }
            }
            3 | 6 => {
                let len = BULK_EDIT_LEN.min(rows_usize).max(1);
                let start = bulk_a_start(cycle, rows_usize);
                for idx in 0..len {
                    if row == (start + idx) as u32 + 1 {
                        unit = bulk_a_value(cycle, idx);
                    }
                }
            }
            _ => {}
        }
    }
    unit
}

fn price_at(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    let row0 = row - 1;
    let mut price = initial_price(row);
    let rows_usize = rows as usize;
    for cycle in 0..completed_cycles {
        match cycle {
            2 => {
                for idx in 0..SPARSE_EDITS.min(rows_usize).max(1) {
                    if row0 as usize == sparse_row0(cycle, idx, rows_usize) {
                        price = sparse_price_value(cycle, idx);
                    }
                }
            }
            4 | 7 => {
                for idx in 0..BULK_EDIT_LEN.min(rows_usize).max(1) {
                    if row0 as usize == bulk_b_row0(cycle, idx, rows_usize) {
                        price = bulk_b_value(cycle, idx);
                    }
                }
            }
            _ => {}
        }
    }
    price
}

fn multiplier_at(completed_cycles: usize) -> f64 {
    let mut multiplier = 1.0;
    for cycle in 0..completed_cycles {
        match cycle {
            0 => multiplier = 2.0,
            5 => multiplier = 3.5,
            _ => {}
        }
    }
    multiplier
}
