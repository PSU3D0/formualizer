use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, write_standard_grid_fixture};
use super::{
    EditPlan, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant, ScenarioPhase,
    ScenarioScale, ScenarioTag,
};

pub struct S001NoFormulasStaticGrid {
    scale: ScaleState,
}

impl Default for S001NoFormulasStaticGrid {
    fn default() -> Self {
        Self::new()
    }
}

impl S001NoFormulasStaticGrid {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    pub fn dimensions(scale: ScenarioScale) -> (u32, u32) {
        match scale {
            ScenarioScale::Small => (1_000, 5),
            ScenarioScale::Medium => (10_000, 10),
            ScenarioScale::Large => (50_000, 10),
        }
    }
}

impl Scenario for S001NoFormulasStaticGrid {
    fn id(&self) -> &'static str {
        "s001-no-formulas-static-grid"
    }

    fn description(&self) -> &'static str {
        "Pure deterministic value grid with no formulas."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::NoFormulas, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let (rows, cols) = Self::dimensions(ctx.scale);
        Ok(write_standard_grid_fixture(ctx, self.id(), rows, cols))
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 5,
            apply: apply_edit,
        })
    }

    fn invariants(&self, _phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }]
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value(
        "Sheet1",
        cycle as u32 + 1,
        1,
        LiteralValue::Number(cycle as f64),
    )?;
    Ok("single_value")
}
