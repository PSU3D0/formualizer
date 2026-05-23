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
const ABC_ROW: u32 = 123;

pub struct S050VlookupWithAbsoluteKey {
    scale: ScaleState,
}

impl Default for S050VlookupWithAbsoluteKey {
    fn default() -> Self {
        Self::new()
    }
}

impl S050VlookupWithAbsoluteKey {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S050VlookupWithAbsoluteKey {
    fn id(&self) -> &'static str {
        "s050-vlookup-with-absolute-key"
    }

    fn description(&self) -> &'static str {
        "VLOOKUP family with a fixed literal lookup key."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::LookupHeavy, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                let key = if row == ABC_ROW {
                    "ABC".to_string()
                } else {
                    format!("K{row:04}")
                };
                sheet.get_cell_mut((1, row)).set_value(key);
                sheet
                    .get_cell_mut((2, row))
                    .set_value_number(table_value(row, 0));
                sheet
                    .get_cell_mut((4, row))
                    .set_formula("=VLOOKUP(\"ABC\", $A$1:$B$1000, 2, FALSE)");
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 4,
                sheets: 1,
                formula_cells: ROWS,
                value_cells: ROWS * 2,
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
            for row in [1, 500, 1000] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 4,
                    expected: numeric(table_value(ABC_ROW, cycles)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value(
        "Sheet1",
        ABC_ROW,
        2,
        LiteralValue::Number(60_000.0 + cycle as f64),
    )?;
    Ok("absolute_vlookup_result")
}

fn table_value(row: u32, cycles: usize) -> f64 {
    if row == ABC_ROW && cycles > 0 {
        60_000.0 + (cycles - 1) as f64
    } else {
        row as f64 * 4.0
    }
}
