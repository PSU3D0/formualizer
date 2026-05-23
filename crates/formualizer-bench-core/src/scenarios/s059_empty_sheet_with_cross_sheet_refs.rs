use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const ROWS: u32 = 100;

pub struct S059EmptySheetWithCrossSheetRefs {
    scale: ScaleState,
}

impl Default for S059EmptySheetWithCrossSheetRefs {
    fn default() -> Self {
        Self::new()
    }
}

impl S059EmptySheetWithCrossSheetRefs {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S059EmptySheetWithCrossSheetRefs {
    fn id(&self) -> &'static str {
        "s059-empty-sheet-with-cross-sheet-refs"
    }

    fn description(&self) -> &'static str {
        "Formulas reference an initially empty second sheet that is populated later."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::MultiSheet, ScenarioTag::EmptyGaps]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            book.new_sheet("Sheet2").expect("Sheet2 sheet");
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                sheet
                    .get_cell_mut((1, row))
                    .set_formula(format!("=IFERROR(Sheet2!A{row}*2, -1)"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 1,
                sheets: 2,
                formula_cells: ROWS,
                value_cells: 0,
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 1,
            apply: apply_edit,
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            for row in [1, 50, 100] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 1,
                    expected: numeric(expected(row, cycles)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, _cycle: usize) -> Result<&'static str, anyhow::Error> {
    for row in 1..=ROWS {
        wb.set_value("Sheet2", row, 1, LiteralValue::Number(row as f64))?;
    }
    Ok("populate_empty_sheet")
}

fn expected(row: u32, cycles: usize) -> f64 {
    if cycles == 0 { 0.0 } else { row as f64 * 2.0 }
}
