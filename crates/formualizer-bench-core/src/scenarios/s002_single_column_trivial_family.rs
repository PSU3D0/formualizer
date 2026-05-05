use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{
    ScaleState, fixture_path, has_evaluated_formulas, maybe_edited_s002_a, numeric,
};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S002SingleColumnTrivialFamily {
    scale: ScaleState,
}

impl Default for S002SingleColumnTrivialFamily {
    fn default() -> Self {
        Self::new()
    }
}

impl S002SingleColumnTrivialFamily {
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

impl Scenario for S002SingleColumnTrivialFamily {
    fn id(&self) -> &'static str {
        "s002-single-column-trivial-family"
    }

    fn description(&self) -> &'static str {
        "Single span-promotable formula column B=A*2."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::SingleColumnFamily,
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
            for r in 1..=rows {
                sh.get_cell_mut((1, r)).set_value_number(r as f64);
                sh.get_cell_mut((2, r)).set_formula(format!("=A{r}*2"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 2,
                sheets: 1,
                formula_cells: rows,
                value_cells: rows,
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
            invariants.reserve(rows as usize);
            for row in 1..=rows {
                let a = maybe_edited_s002_a(row, phase);
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(a * 2.0),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let row = (cycle * 37) as u32 + 1;
    let value = 1000.0 + cycle as f64;
    wb.set_value("Sheet1", row, 1, LiteralValue::Number(value))?;
    Ok("single_a")
}
