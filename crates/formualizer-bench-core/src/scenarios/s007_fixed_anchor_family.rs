use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S007FixedAnchorFamily {
    scale: ScaleState,
}

impl Default for S007FixedAnchorFamily {
    fn default() -> Self {
        Self::new()
    }
}

impl S007FixedAnchorFamily {
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

impl Scenario for S007FixedAnchorFamily {
    fn id(&self) -> &'static str {
        "s007-fixed-anchor-family"
    }

    fn description(&self) -> &'static str {
        "Single anchored formula family B=$A$1+1 over N rows."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::AnchoredArithmetic,
            ScenarioTag::SpanPromotable,
            ScenarioTag::SingleCellEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            sh.get_cell_mut((1, 1)).set_value_number(42.0);
            for r in 1..=rows {
                sh.get_cell_mut((2, r)).set_formula("=$A$1+1");
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 2,
                sheets: 1,
                formula_cells: rows,
                value_cells: 1,
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
            let a1 = a1_at(completed_cycles(phase));
            invariants.reserve(rows as usize);
            for row in 1..=rows {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(a1 + 1.0),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value("Sheet1", 1, 1, LiteralValue::Number(50.0 + cycle as f64))?;
    Ok("anchor_a1")
}

fn a1_at(completed_cycles: usize) -> f64 {
    if completed_cycles == 0 {
        42.0
    } else {
        50.0 + (completed_cycles - 1) as f64
    }
}
