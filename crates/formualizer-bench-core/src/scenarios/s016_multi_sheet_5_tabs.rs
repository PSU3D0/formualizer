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

pub struct S016MultiSheet5Tabs {
    scale: ScaleState,
}

impl Default for S016MultiSheet5Tabs {
    fn default() -> Self {
        Self::new()
    }
}

impl S016MultiSheet5Tabs {
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

impl Scenario for S016MultiSheet5Tabs {
    fn id(&self) -> &'static str {
        "s016-multi-sheet-5-tabs"
    }

    fn description(&self) -> &'static str {
        "Five-sheet cross-sheet topology with data, calc, and final tabs."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::MultiSheet,
            ScenarioTag::CrossSheet,
            ScenarioTag::SingleCellEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            book.new_sheet("Data1").expect("Data1 sheet");
            book.new_sheet("Data2").expect("Data2 sheet");
            book.new_sheet("Calc1").expect("Calc1 sheet");
            book.new_sheet("Calc2").expect("Calc2 sheet");

            let data1 = book.get_sheet_by_name_mut("Data1").expect("Data1 exists");
            for r in 1..=DATA_ROWS {
                data1.get_cell_mut((1, r)).set_value_number(r as f64);
            }

            let data2 = book.get_sheet_by_name_mut("Data2").expect("Data2 exists");
            for r in 1..=DATA_ROWS {
                data2.get_cell_mut((1, r)).set_value(format!("text-{r}"));
            }

            let calc1 = book.get_sheet_by_name_mut("Calc1").expect("Calc1 exists");
            for r in 1..=rows {
                calc1
                    .get_cell_mut((1, r))
                    .set_formula(format!("=Data1!A{r} * 2"));
            }

            let calc2 = book.get_sheet_by_name_mut("Calc2").expect("Calc2 exists");
            for r in 1..=rows {
                calc2
                    .get_cell_mut((1, r))
                    .set_formula(format!("=LEN(Data2!A{r})"));
            }

            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet
                    .get_cell_mut((1, r))
                    .set_formula(format!("=Calc1!A{r} + Calc2!A{r}"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 1,
                sheets: 5,
                formula_cells: rows.saturating_mul(3),
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
        let mut invariants = Vec::with_capacity(5);
        for sheet in ["Data1", "Data2", "Calc1", "Calc2", "Sheet1"] {
            invariants.push(ScenarioInvariant::NoErrorCells {
                sheet: sheet.to_string(),
            });
        }
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            invariants.reserve(rows as usize);
            for row in 1..=rows {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 1,
                    expected: numeric(sheet1_value(row, cycles)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let row = ((cycle * 37) % DATA_ROWS as usize) as u32 + 1;
    wb.set_value("Data1", row, 1, LiteralValue::Number(1000.0 + cycle as f64))?;
    Ok("data1_value")
}

fn sheet1_value(row: u32, completed_cycles: usize) -> f64 {
    data1_value(row, completed_cycles) * 2.0 + data2_len(row)
}

fn data1_value(row: u32, completed_cycles: usize) -> f64 {
    if row > DATA_ROWS {
        return 0.0;
    }
    let mut value = row as f64;
    for cycle in 0..completed_cycles {
        let edit_row = ((cycle * 37) % DATA_ROWS as usize) as u32 + 1;
        if row == edit_row {
            value = 1000.0 + cycle as f64;
        }
    }
    value
}

fn data2_len(row: u32) -> f64 {
    if row > DATA_ROWS {
        0.0
    } else {
        format!("text-{row}").len() as f64
    }
}
