use anyhow::{Context, Result};
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const DELETE_ROWS: u32 = 10;
const BUFFER_ROWS: u32 = 100;

pub struct S033FamilyWithRowDeleteCycles {
    scale: ScaleState,
}

impl Default for S033FamilyWithRowDeleteCycles {
    fn default() -> Self {
        Self::new()
    }
}

impl S033FamilyWithRowDeleteCycles {
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

impl Scenario for S033FamilyWithRowDeleteCycles {
    fn id(&self) -> &'static str {
        "s033-family-with-row-delete-cycles"
    }

    fn description(&self) -> &'static str {
        "Single-column =A*2 family with a 100-row buffer and five deterministic 10-row deletes."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::SingleColumnFamily, ScenarioTag::DeleteRows]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale) + BUFFER_ROWS;
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet.get_cell_mut((1, r)).set_value_number(r as f64);
                sheet.get_cell_mut((2, r)).set_formula(format!("=A{r}*2"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 2,
                sheets: 1,
                formula_cells: rows,
                value_cells: rows,
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 5,
            apply: apply_edit,
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let base_rows = Self::rows(self.scale.get_or_small()) + BUFFER_ROWS;
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            let rows = base_rows.saturating_sub(DELETE_ROWS * cycles as u32);
            invariants.reserve(rows as usize);
            for row in 1..=rows {
                let origin = current_row_origin_after_deletes(row, base_rows, cycles);
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(origin as f64 * 2.0),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let rows = wb
        .sheet_dimensions("Sheet1")
        .map(|(rows, _)| rows)
        .unwrap_or(DELETE_ROWS + 1)
        .max(DELETE_ROWS + 1);
    let start = delete_start_row(rows, cycle);
    wb.engine_mut()
        .delete_rows("Sheet1", start, DELETE_ROWS)
        .with_context(|| format!("engine delete_rows Sheet1 start={start} count={DELETE_ROWS}"))?;
    Ok("delete_rows_10")
}

fn delete_start_row(current_rows: u32, cycle: usize) -> u32 {
    let divisors = [5, 4, 3, 2, 6];
    (current_rows / divisors[cycle % divisors.len()])
        .max(1)
        .min(current_rows.saturating_sub(DELETE_ROWS).max(1))
}

fn current_row_origin_after_deletes(row: u32, base_rows: u32, cycles: usize) -> u32 {
    let mut row = row;
    for cycle in (0..cycles).rev() {
        let rows_before_cycle = base_rows.saturating_sub(DELETE_ROWS * cycle as u32);
        let start = delete_start_row(rows_before_cycle, cycle);
        if row >= start {
            row += DELETE_ROWS;
        }
    }
    row
}
