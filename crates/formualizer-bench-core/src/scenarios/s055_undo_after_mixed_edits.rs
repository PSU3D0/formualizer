use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const ROWS: u32 = 200;

pub struct S055UndoAfterMixedEdits {
    scale: ScaleState,
}

impl Default for S055UndoAfterMixedEdits {
    fn default() -> Self {
        Self::new()
    }
}

impl S055UndoAfterMixedEdits {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S055UndoAfterMixedEdits {
    fn id(&self) -> &'static str {
        "s055-undo-after-mixed-edits"
    }

    fn description(&self) -> &'static str {
        "Grouped value and formula edits followed by undo to the initial state."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::UndoRedo]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                sheet.get_cell_mut((1, row)).set_value_number(row as f64);
                sheet
                    .get_cell_mut((2, row))
                    .set_formula(format!("=A{row}*2"));
                sheet
                    .get_cell_mut((3, row))
                    .set_formula(format!("=B{row}+1"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 3,
                sheets: 1,
                formula_cells: ROWS * 2,
                value_cells: ROWS,
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 2,
            apply: apply_edit,
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            if cycles != 1 {
                for row in [1, 100, 200] {
                    invariants.push(ScenarioInvariant::CellEquals {
                        sheet: "Sheet1".to_string(),
                        row,
                        col: 3,
                        expected: numeric(expected_c(row, cycles)),
                    });
                }
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    match cycle {
        0 => {
            wb.action("mixed value and formula edits", |action| {
                action.set_value("Sheet1", 1, 1, LiteralValue::Number(1_000.0))?;
                action.set_formula("Sheet1", 100, 2, "=A100*5")?;
                action.set_value("Sheet1", 200, 1, LiteralValue::Number(2_000.0))?;
                Ok(())
            })?;
            Ok("mixed_edits")
        }
        1 => {
            wb.undo()?;
            Ok("undo_mixed_edits")
        }
        _ => Ok("noop"),
    }
}

fn expected_c(row: u32, cycles: usize) -> f64 {
    match cycles {
        1 => match row {
            1 => 2_001.0,
            100 => 501.0,
            200 => 4_001.0,
            _ => row as f64 * 2.0 + 1.0,
        },
        _ => row as f64 * 2.0 + 1.0,
    }
}
