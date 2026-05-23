use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;

use super::common::{ScaleState, fixture_path, has_evaluated_formulas};
use super::{
    FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant, ScenarioPhase,
    ScenarioTag,
};

const ROWS: u32 = 1_000;

pub struct S053TextHeavyConcatenation {
    scale: ScaleState,
}

impl Default for S053TextHeavyConcatenation {
    fn default() -> Self {
        Self::new()
    }
}

impl S053TextHeavyConcatenation {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S053TextHeavyConcatenation {
    fn id(&self) -> &'static str {
        "s053-text-heavy-concatenation"
    }

    fn description(&self) -> &'static str {
        "Eight-argument text concatenation family over 1000 rows."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::AnchoredArithmetic, ScenarioTag::TextHeavy]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                for col in 1..=8 {
                    sheet
                        .get_cell_mut((col, row))
                        .set_value(format!("R{row}C{col}"));
                }
                sheet.get_cell_mut((9, row)).set_formula(format!(
                    "=CONCATENATE(A{row}, \"-\", B{row}, \"-\", C{row}, \"-\", D{row}, \"-\", E{row}, \"-\", F{row}, \"-\", G{row}, \"-\", H{row})"
                ));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 9,
                sheets: 1,
                formula_cells: ROWS,
                value_cells: ROWS * 8,
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
                    col: 9,
                    expected: LiteralValue::Text(expected(row)),
                });
            }
        }
        invariants
    }
}

fn expected(row: u32) -> String {
    (1..=8)
        .map(|col| format!("R{row}C{col}"))
        .collect::<Vec<_>>()
        .join("-")
}
