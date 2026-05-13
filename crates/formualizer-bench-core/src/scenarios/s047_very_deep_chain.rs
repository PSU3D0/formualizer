use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const ROWS: u32 = 2_000;

pub struct S047VeryDeepChain {
    scale: ScaleState,
}

impl Default for S047VeryDeepChain {
    fn default() -> Self {
        Self::new()
    }
}

impl S047VeryDeepChain {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S047VeryDeepChain {
    fn id(&self) -> &'static str {
        "s047-very-deep-chain"
    }

    fn description(&self) -> &'static str {
        "A 2000-cell linear dependency chain with edits at the seed."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::LongChain, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            sheet.get_cell_mut((1, 1)).set_value_number(1.0);
            for row in 2..=ROWS {
                sheet
                    .get_cell_mut((1, row))
                    .set_formula(format!("=A{}+1", row - 1));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 1,
                sheets: 1,
                formula_cells: ROWS - 1,
                value_cells: 1,
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
            for row in [1, 1000, 2000] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 1,
                    expected: numeric(seed(cycles) + row as f64 - 1.0),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value("Sheet1", 1, 1, LiteralValue::Number(10.0 + cycle as f64))?;
    Ok("deep_chain_seed")
}

fn seed(cycles: usize) -> f64 {
    if cycles == 0 {
        1.0
    } else {
        10.0 + (cycles - 1) as f64
    }
}
