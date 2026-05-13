use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S006RectFamily10Cols {
    scale: ScaleState,
}

impl Default for S006RectFamily10Cols {
    fn default() -> Self {
        Self::new()
    }
}

impl S006RectFamily10Cols {
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

impl Scenario for S006RectFamily10Cols {
    fn id(&self) -> &'static str {
        "s006-rect-family-10cols"
    }

    fn description(&self) -> &'static str {
        "Rectangular anchored family across 10 columns by N rows."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::MultiColumnFamily,
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
            sh.get_cell_mut((1, 1)).set_value_number(1.0);
            for r in 1..=rows {
                for c in 2..=11 {
                    sh.get_cell_mut((c, r)).set_formula(format!("=$A$1+{c}"));
                }
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 11,
                sheets: 1,
                formula_cells: rows.saturating_mul(10),
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
            invariants.reserve(rows as usize * 10);
            for row in 1..=rows {
                for col in 2..=11 {
                    invariants.push(ScenarioInvariant::CellEquals {
                        sheet: "Sheet1".to_string(),
                        row,
                        col,
                        expected: numeric(a1 + col as f64),
                    });
                }
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value("Sheet1", 1, 1, LiteralValue::Number(10.0 + cycle as f64))?;
    Ok("anchor_a1")
}

fn a1_at(completed_cycles: usize) -> f64 {
    if completed_cycles == 0 {
        1.0
    } else {
        10.0 + (completed_cycles - 1) as f64
    }
}
