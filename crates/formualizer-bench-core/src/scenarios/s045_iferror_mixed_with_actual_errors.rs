use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const ROWS: u32 = 10_000;

pub struct S045IferrorMixedWithActualErrors {
    scale: ScaleState,
}

impl Default for S045IferrorMixedWithActualErrors {
    fn default() -> Self {
        Self::new()
    }
}

impl S045IferrorMixedWithActualErrors {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    pub fn rows(_scale: ScenarioScale) -> u32 {
        ROWS
    }
}

impl Scenario for S045IferrorMixedWithActualErrors {
    fn id(&self) -> &'static str {
        "s045-iferror-mixed-with-actual-errors"
    }

    fn description(&self) -> &'static str {
        "IFERROR family masks divide-by-zero rows while preserving reciprocal rows."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::ShortCircuit,
            ScenarioTag::SpanPromotable,
            ScenarioTag::SingleCellEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=rows {
                sheet.get_cell_mut((1, row)).set_value_number(a_at(row, 0));
                sheet
                    .get_cell_mut((2, row))
                    .set_formula(format!("=IFERROR(1/A{row},0)"));
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
            for row in [1, rows / 2, rows] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(iferror_value(a_at(row, cycles))),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let row = edited_row(cycle);
    wb.set_value("Sheet1", row, 1, LiteralValue::Number(edited_value(cycle)))?;
    Ok("reciprocal_input")
}

fn a_at(row: u32, cycles: usize) -> f64 {
    for cycle in (0..cycles).rev() {
        if row == edited_row(cycle) {
            return edited_value(cycle);
        }
    }
    match row % 5 {
        0 => 0.0,
        1 => 1.0,
        2 => -2.0,
        3 => 4.0,
        _ => -5.0,
    }
}

fn iferror_value(a: f64) -> f64 {
    if a == 0.0 { 0.0 } else { 1.0 / a }
}

fn edited_row(cycle: usize) -> u32 {
    ((cycle * 1_529) % ROWS as usize) as u32 + 1
}

fn edited_value(cycle: usize) -> f64 {
    match cycle % 3 {
        0 => 0.0,
        1 => 8.0 + cycle as f64,
        _ => -8.0 - cycle as f64,
    }
}
