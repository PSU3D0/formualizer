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
const EDIT_ROW: u32 = 1;

pub struct S051MixedErrorCascade {
    scale: ScaleState,
}

impl Default for S051MixedErrorCascade {
    fn default() -> Self {
        Self::new()
    }
}

impl S051MixedErrorCascade {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S051MixedErrorCascade {
    fn id(&self) -> &'static str {
        "s051-mixed-error-cascade"
    }

    fn description(&self) -> &'static str {
        "Alternating DIV/0 and valid rows propagated through IFERROR."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::ErrorPropagation, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                if row % 2 == 1 {
                    sheet.get_cell_mut((1, row)).set_formula("=1/0");
                } else {
                    sheet.get_cell_mut((1, row)).set_value_number(row as f64);
                }
                sheet
                    .get_cell_mut((2, row))
                    .set_formula(format!("=A{row}*2"));
                sheet
                    .get_cell_mut((3, row))
                    .set_formula(format!("=IFERROR(B{row}, -1)"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 3,
                sheets: 1,
                formula_cells: ROWS * 2 + ROWS / 2,
                value_cells: ROWS / 2,
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
        let mut invariants = Vec::new();
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            for row in [1, 2, 3] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 3,
                    expected: c_expected(row, cycles),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, _cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value("Sheet1", EDIT_ROW, 1, LiteralValue::Number(7.0))?;
    Ok("clear_error_source")
}

fn c_expected(row: u32, cycles: usize) -> LiteralValue {
    if row == EDIT_ROW && cycles > 0 {
        numeric(14.0)
    } else if row % 2 == 1 {
        numeric(-1.0)
    } else {
        numeric(row as f64 * 2.0)
    }
}
