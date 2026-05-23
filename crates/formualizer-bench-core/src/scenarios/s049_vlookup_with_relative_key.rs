use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const ROWS: u32 = 1_000;
const EDIT_KEY: u32 = 10;

pub struct S049VlookupWithRelativeKey {
    scale: ScaleState,
}

impl Default for S049VlookupWithRelativeKey {
    fn default() -> Self {
        Self::new()
    }
}

impl S049VlookupWithRelativeKey {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S049VlookupWithRelativeKey {
    fn id(&self) -> &'static str {
        "s049-vlookup-with-relative-key"
    }

    fn description(&self) -> &'static str {
        "VLOOKUP family with row-relative lookup keys and a shared result-side edit."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::LookupHeavy, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                sheet.get_cell_mut((1, row)).set_value(format!("K{row:04}"));
                sheet
                    .get_cell_mut((2, row))
                    .set_value_number(table_value(row, 0));
                let key = ((row - 1) % 50) + 1;
                sheet.get_cell_mut((3, row)).set_value(format!("K{key:04}"));
                sheet
                    .get_cell_mut((4, row))
                    .set_formula(format!("=VLOOKUP(C{row}, $A$1:$B$1000, 2, FALSE)"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 4,
                sheets: 1,
                formula_cells: ROWS,
                value_cells: ROWS * 3,
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 3,
            apply: apply_edit,
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            for row in [1, 10, 60] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 4,
                    expected: numeric(table_value(key_for_formula_row(row), cycles)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value(
        "Sheet1",
        EDIT_KEY,
        2,
        LiteralValue::Number(50_000.0 + cycle as f64),
    )?;
    Ok("vlookup_result_side")
}

fn key_for_formula_row(row: u32) -> u32 {
    ((row - 1) % 50) + 1
}

fn table_value(row: u32, cycles: usize) -> f64 {
    if row == EDIT_KEY && cycles > 0 {
        50_000.0 + (cycles - 1) as f64
    } else {
        row as f64 * 3.0
    }
}
