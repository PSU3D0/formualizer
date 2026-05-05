use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const DATA_ROWS: u32 = 10_000;

pub struct S013SumifsFamilyConstantCriteria {
    scale: ScaleState,
}

impl Default for S013SumifsFamilyConstantCriteria {
    fn default() -> Self {
        Self::new()
    }
}

impl S013SumifsFamilyConstantCriteria {
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

impl Scenario for S013SumifsFamilyConstantCriteria {
    fn id(&self) -> &'static str {
        "s013-sumifs-family-constant-criteria"
    }

    fn description(&self) -> &'static str {
        "SUMIFS family with identical literal text criterion across rows."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::AggregationHeavy, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            book.new_sheet("Data").expect("Data sheet");
            let data = book.get_sheet_by_name_mut("Data").expect("Data exists");
            for dr in 1..=DATA_ROWS {
                data.get_cell_mut((1, dr)).set_value(data_type(dr));
                data.get_cell_mut((2, dr)).set_value_number(dr as f64);
            }

            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet
                    .get_cell_mut((1, r))
                    .set_formula("=SUMIFS(Data!$B$1:$B$10000, Data!$A$1:$A$10000, \"Type1\")");
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 2,
                sheets: 2,
                formula_cells: rows,
                value_cells: DATA_ROWS.saturating_mul(2),
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
            let expected = sum_for_type("Type1", completed_cycles(phase));
            invariants.reserve(rows as usize);
            for row in 1..=rows {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 1,
                    expected: numeric(expected),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let row = ((cycle * 67) % DATA_ROWS as usize) as u32 + 1;
    wb.set_value(
        "Data",
        row,
        2,
        LiteralValue::Number(edited_data_value(cycle)),
    )?;
    Ok("data_value")
}

fn data_type(row: u32) -> &'static str {
    match row % 3 {
        0 => "Type1",
        1 => "Type2",
        _ => "Type3",
    }
}

fn edited_data_value(cycle: usize) -> f64 {
    20_000.0 + cycle as f64
}

fn sum_for_type(criteria: &str, completed_cycles: usize) -> f64 {
    let mut total = (1..=DATA_ROWS)
        .filter(|row| data_type(*row) == criteria)
        .map(|row| row as f64)
        .sum::<f64>();
    for cycle in 0..completed_cycles {
        let row = ((cycle * 67) % DATA_ROWS as usize) as u32 + 1;
        if data_type(row) == criteria {
            total += edited_data_value(cycle) - previous_data_value(row, cycle);
        }
    }
    total
}

fn previous_data_value(row: u32, cycle: usize) -> f64 {
    let mut value = row as f64;
    for prev_cycle in 0..cycle {
        let edit_row = ((prev_cycle * 67) % DATA_ROWS as usize) as u32 + 1;
        if row == edit_row {
            value = edited_data_value(prev_cycle);
        }
    }
    value
}
