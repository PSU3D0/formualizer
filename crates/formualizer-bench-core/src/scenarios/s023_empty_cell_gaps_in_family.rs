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

pub struct S023EmptyCellGapsInFamily {
    scale: ScaleState,
}

impl Default for S023EmptyCellGapsInFamily {
    fn default() -> Self {
        Self::new()
    }
}

impl S023EmptyCellGapsInFamily {
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

impl Scenario for S023EmptyCellGapsInFamily {
    fn id(&self) -> &'static str {
        "s023-empty-cell-gaps-in-family"
    }

    fn description(&self) -> &'static str {
        "Single-column formula family whose inputs have regular empty gaps."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::SingleColumnFamily,
            ScenarioTag::EmptyGaps,
            ScenarioTag::SingleCellEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                if r % 10 != 0 {
                    sheet.get_cell_mut((1, r)).set_value_number(r as f64);
                }
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
                value_cells: rows - rows / 10,
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
            invariants.reserve(rows as usize);
            for row in 1..=rows {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(data_value(row, rows, cycles).unwrap_or(0.0) * 2.0),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let rows = detect_nonempty_rows(wb, "Sheet1", 2) as usize;
    let row = ((cycle * 37) % rows) as u32 + 1;
    wb.set_value(
        "Sheet1",
        row,
        1,
        LiteralValue::Number(1000.0 + cycle as f64),
    )?;
    Ok("gap_input")
}

fn data_value(row: u32, rows: u32, completed_cycles: usize) -> Option<f64> {
    let mut value = (!row.is_multiple_of(10)).then_some(row as f64);
    for cycle in 0..completed_cycles {
        let edit_row = ((cycle * 37) % rows as usize) as u32 + 1;
        if row == edit_row {
            value = Some(1000.0 + cycle as f64);
        }
    }
    value
}
