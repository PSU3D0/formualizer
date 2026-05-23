use anyhow::Result;
use formualizer_common::RangeAddress;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;
use formualizer_workbook::traits::NamedRangeScope;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const DATA_ROWS: u32 = 200;
const FORMULA_ROWS: u32 = 100;

pub struct S057NamedRangeRedefined {
    scale: ScaleState,
}

impl Default for S057NamedRangeRedefined {
    fn default() -> Self {
        Self::new()
    }
}

impl S057NamedRangeRedefined {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S057NamedRangeRedefined {
    fn id(&self) -> &'static str {
        "s057-named-range-redefined"
    }

    fn description(&self) -> &'static str {
        "Workbook named range Total is redefined between A1:A100 and A1:A200."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::NamedRanges, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=DATA_ROWS {
                sheet.get_cell_mut((1, row)).set_value_number(row as f64);
            }
            sheet
                .add_defined_name("Total", "Sheet1!$A$1:$A$100")
                .expect("add Total name");
            for row in 1..=FORMULA_ROWS {
                sheet.get_cell_mut((2, row)).set_formula("=SUM(Total)");
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: DATA_ROWS,
                cols: 2,
                sheets: 1,
                formula_cells: FORMULA_ROWS,
                value_cells: DATA_ROWS,
                has_named_ranges: true,
                has_tables: false,
            },
        })
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 2,
            apply: apply_edit,
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            for row in [1, 50, 100] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(expected(cycles)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let end = if cycle == 0 { 200 } else { 100 };
    let address = RangeAddress::new("Sheet1", 1, 1, end, 1).expect("valid Total address");
    wb.update_named_range("Total", &address, NamedRangeScope::Workbook)?;
    Ok(if cycle == 0 {
        "total_to_200"
    } else {
        "total_to_100"
    })
}

fn expected(cycles: usize) -> f64 {
    let end = if cycles == 1 { 200 } else { 100 };
    (1..=end).map(|row| row as f64).sum()
}
