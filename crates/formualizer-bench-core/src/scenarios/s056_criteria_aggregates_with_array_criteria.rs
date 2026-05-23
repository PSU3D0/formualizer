use anyhow::Result;
use formualizer_testkit::write_workbook;

use super::common::{ScaleState, fixture_path, has_evaluated_formulas, numeric};
use super::{
    FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant, ScenarioPhase,
    ScenarioTag,
};

const ROWS: u32 = 1_000;

pub struct S056CriteriaAggregatesWithArrayCriteria {
    scale: ScaleState,
}

impl Default for S056CriteriaAggregatesWithArrayCriteria {
    fn default() -> Self {
        Self::new()
    }
}

impl S056CriteriaAggregatesWithArrayCriteria {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S056CriteriaAggregatesWithArrayCriteria {
    fn id(&self) -> &'static str {
        "s056-criteria-aggregates-with-array-criteria"
    }

    fn description(&self) -> &'static str {
        "SUMIFS formulas with criteria built by concatenating an operator and row value."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::AggregationHeavy]
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
                    .set_value_number((row * 2) as f64);
                let threshold = ((row - 1) % 10 + 1) * 100;
                sheet
                    .get_cell_mut((4, row))
                    .set_value_number(threshold as f64);
                sheet
                    .get_cell_mut((5, row))
                    .set_formula(format!("=SUMIFS(B:B, A:A, \"<=\"&D{row})"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 5,
                sheets: 1,
                formula_cells: ROWS,
                value_cells: ROWS * 3,
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            for row in [1, 500, 1000] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 5,
                    expected: numeric(expected(row)),
                });
            }
        }
        invariants
    }
}

fn expected(row: u32) -> f64 {
    let threshold = ((row - 1) % 10 + 1) * 100;
    (1..=threshold).map(|r| (r * 2) as f64).sum()
}
