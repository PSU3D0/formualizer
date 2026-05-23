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

pub struct S054RecalcAfterAddThenDeleteSheet {
    scale: ScaleState,
}

impl Default for S054RecalcAfterAddThenDeleteSheet {
    fn default() -> Self {
        Self::new()
    }
}

impl S054RecalcAfterAddThenDeleteSheet {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S054RecalcAfterAddThenDeleteSheet {
    fn id(&self) -> &'static str {
        "s054-recalc-after-add-then-delete-sheet"
    }

    fn description(&self) -> &'static str {
        "Cross-sheet formulas across Aux sheet deletion and recreation."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::MultiSheet]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            book.new_sheet("Aux").expect("Aux sheet");
            let aux = book.get_sheet_by_name_mut("Aux").expect("Aux exists");
            for row in 1..=ROWS {
                aux.get_cell_mut((1, row)).set_value_number(row as f64);
            }
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                sheet
                    .get_cell_mut((1, row))
                    .set_formula(format!("=IFERROR(Aux!A{row}*2, -1)"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 1,
                sheets: 2,
                formula_cells: ROWS,
                value_cells: ROWS,
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
        let mut invariants = Vec::new();
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            if cycles <= 1 {
                for row in [1, 50, 100] {
                    invariants.push(ScenarioInvariant::CellEquals {
                        sheet: "Sheet1".to_string(),
                        row,
                        col: 1,
                        expected: numeric(expected(row, cycles)),
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
            wb.delete_sheet("Aux")?;
            Ok("delete_aux_sheet")
        }
        1 => {
            wb.add_sheet("Aux")?;
            for row in 1..=ROWS {
                wb.set_value("Aux", row, 1, LiteralValue::Number(row as f64 + 10.0))?;
            }
            Ok("add_aux_sheet")
        }
        _ => Ok("sheet_recalc_noop"),
    }
}

fn expected(row: u32, cycles: usize) -> f64 {
    match cycles {
        0 => row as f64 * 2.0,
        1 => -1.0,
        _ => (row as f64 + 10.0) * 2.0,
    }
}
