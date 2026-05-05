use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const DATA_ROWS: u32 = 1_000;

pub struct S036MultiSheetWithSheetRename {
    scale: ScaleState,
}

impl Default for S036MultiSheetWithSheetRename {
    fn default() -> Self {
        Self::new()
    }
}

impl S036MultiSheetWithSheetRename {
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

impl Scenario for S036MultiSheetWithSheetRename {
    fn id(&self) -> &'static str {
        "s036-multi-sheet-with-sheet-rename"
    }

    fn description(&self) -> &'static str {
        "Three-sheet cross-sheet formulas while referenced sheets are renamed back and forth."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::MultiSheet,
            ScenarioTag::CrossSheet,
            ScenarioTag::SheetRename,
            ScenarioTag::SingleCellEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            {
                let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
                sheet.set_name("DataA");
            }
            book.new_sheet("DataB").expect("DataB sheet");
            book.new_sheet("Sheet1").expect("Sheet1 sheet");

            let data_a = book.get_sheet_by_name_mut("DataA").expect("DataA exists");
            for r in 1..=DATA_ROWS {
                data_a.get_cell_mut((1, r)).set_value_number(r as f64);
            }

            let data_b = book.get_sheet_by_name_mut("DataB").expect("DataB exists");
            for r in 1..=DATA_ROWS {
                data_b.get_cell_mut((1, r)).set_value_number(r as f64 * 2.0);
            }

            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet
                    .get_cell_mut((1, r))
                    .set_formula(format!("=DataA!A{r} + DataB!A{r}"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: rows.max(DATA_ROWS),
                cols: 1,
                sheets: 3,
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
            let cycles = completed_cycles(phase);
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
    match cycle {
        0 => {
            wb.rename_sheet("DataA", "DataAA")?;
            Ok("rename_data_a_forward")
        }
        1 => {
            wb.rename_sheet("DataAA", "DataA")?;
            Ok("rename_data_a_back")
        }
        2 => {
            wb.rename_sheet("DataB", "DataBB")?;
            Ok("rename_data_b_forward")
        }
        3 => {
            wb.rename_sheet("DataBB", "DataB")?;
            Ok("rename_data_b_back")
        }
        4 => {
            wb.set_value("DataA", edited_row(), 1, LiteralValue::Number(10_000.0))?;
            Ok("data_a_value")
        }
        _ => Ok("noop"),
    }
}

fn edited_row() -> u32 {
    ((4 * 37) % DATA_ROWS as usize) as u32 + 1
}

fn data_a(row: u32, completed_cycles: usize) -> f64 {
    if row > DATA_ROWS {
        return 0.0;
    }
    if completed_cycles > 4 && row == edited_row() {
        10_000.0
    } else {
        row as f64
    }
}

fn data_b(row: u32) -> f64 {
    if row > DATA_ROWS {
        0.0
    } else {
        row as f64 * 2.0
    }
}

fn sheet1_value(row: u32, completed_cycles: usize) -> f64 {
    data_a(row, completed_cycles) + data_b(row)
}
