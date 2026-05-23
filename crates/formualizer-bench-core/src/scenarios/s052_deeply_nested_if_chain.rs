use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;

use super::common::{ScaleState, fixture_path, has_evaluated_formulas};
use super::{
    FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant, ScenarioPhase,
    ScenarioTag,
};

const ROWS: u32 = 5_000;

pub struct S052DeeplyNestedIfChain {
    scale: ScaleState,
}

impl Default for S052DeeplyNestedIfChain {
    fn default() -> Self {
        Self::new()
    }
}

impl S052DeeplyNestedIfChain {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S052DeeplyNestedIfChain {
    fn id(&self) -> &'static str {
        "s052-deeply-nested-if-chain"
    }

    fn description(&self) -> &'static str {
        "Five-level nested IF formula family over 5000 rows."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::ShortCircuit, ScenarioTag::SingleColumnFamily]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                sheet
                    .get_cell_mut((1, row))
                    .set_value_number(cycle_value(row));
                sheet.get_cell_mut((2, row)).set_formula(format!(
                    "=IF(A{row}=1, \"one\", IF(A{row}=2, \"two\", IF(A{row}=3, \"three\", IF(A{row}=4, \"four\", IF(A{row}=5, \"five\", \"other\")))))"
                ));
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

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            for row in [1, 2500, 5000] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: LiteralValue::Text(label_for(cycle_value(row)).to_string()),
                });
            }
        }
        invariants
    }
}

fn cycle_value(row: u32) -> f64 {
    ((row - 1) % 6 + 1) as f64
}

fn label_for(value: f64) -> &'static str {
    match value as u32 {
        1 => "one",
        2 => "two",
        3 => "three",
        4 => "four",
        5 => "five",
        _ => "other",
    }
}
