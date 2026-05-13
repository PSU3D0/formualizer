use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const BULK_EDIT_LEN: usize = 50;

pub struct S038BulkEdit50CellsPerCycle {
    scale: ScaleState,
}

impl Default for S038BulkEdit50CellsPerCycle {
    fn default() -> Self {
        Self::new()
    }
}

impl S038BulkEdit50CellsPerCycle {
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

impl Scenario for S038BulkEdit50CellsPerCycle {
    fn id(&self) -> &'static str {
        "s038-bulk-edit-50-cells-per-cycle"
    }

    fn description(&self) -> &'static str {
        "Single-column =A*2 family with five cycles of 50 deterministic input edits."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::SingleColumnFamily, ScenarioTag::BulkEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
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
        let rows = Self::rows(self.scale.get_or_small());
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            for row in 1..=rows {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(input_value(row, rows, cycles) * 2.0),
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
        .unwrap_or(1)
        .max(1);
    for idx in 0..BULK_EDIT_LEN.min(rows as usize).max(1) {
        let row = edit_row(cycle, idx, rows);
        wb.set_value(
            "Sheet1",
            row,
            1,
            LiteralValue::Number(edit_value(cycle, idx)),
        )?;
    }
    Ok("bulk_a_50")
}

fn edit_row(cycle: usize, idx: usize, rows: u32) -> u32 {
    ((cycle * 101 + idx * 17) % rows as usize) as u32 + 1
}

fn edit_value(cycle: usize, idx: usize) -> f64 {
    1_000.0 + cycle as f64 * 100.0 + idx as f64
}

fn input_value(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    let mut value = row as f64;
    for cycle in 0..completed_cycles {
        for idx in 0..BULK_EDIT_LEN.min(rows as usize).max(1) {
            if row == edit_row(cycle, idx, rows) {
                value = edit_value(cycle, idx);
            }
        }
    }
    value
}
