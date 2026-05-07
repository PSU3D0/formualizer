use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, ExpectedDivergence, ExpectedDivergenceAction, ExpectedDivergencePhase,
    FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant, ScenarioPhase,
    ScenarioTag,
};

const ROWS: u32 = 1_000;

pub struct S058VolatileNonVolatileMix {
    scale: ScaleState,
}

impl Default for S058VolatileNonVolatileMix {
    fn default() -> Self {
        Self::new()
    }
}

impl S058VolatileNonVolatileMix {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S058VolatileNonVolatileMix {
    fn id(&self) -> &'static str {
        "s058-volatile-non-volatile-mix"
    }

    fn description(&self) -> &'static str {
        "Alternating volatile RAND/NOW formulas and non-volatile dependents."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::Volatile, ScenarioTag::ShortCircuit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                sheet.get_cell_mut((1, row)).set_value_number(row as f64);
                if row % 2 == 1 {
                    sheet
                        .get_cell_mut((2, row))
                        .set_formula(format!("=A{row}+RAND()+NOW()*0"));
                } else {
                    sheet
                        .get_cell_mut((2, row))
                        .set_formula(format!("=A{row}*2"));
                }
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 2,
                sheets: 1,
                formula_cells: ROWS,
                value_cells: ROWS,
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

    fn expected_divergences(&self) -> Vec<ExpectedDivergence> {
        vec![ExpectedDivergence {
            phase: ExpectedDivergencePhase::Any,
            reason: "volatile RAND/NOW cells legitimately differ between separate runs",
            action: ExpectedDivergenceAction::Skip,
        }]
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            for row in [2, 4, 1000] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(a_value(row, cycles) * 2.0),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, _cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value("Sheet1", 2, 1, LiteralValue::Number(500.0))?;
    Ok("non_volatile_precedent")
}

fn a_value(row: u32, cycles: usize) -> f64 {
    if row == 2 && cycles > 0 {
        500.0
    } else {
        row as f64
    }
}
