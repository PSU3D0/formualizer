use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{
    ScaleState, col_name, completed_cycles, fixture_path, has_evaluated_formulas, numeric,
};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const FAMILIES: u32 = 50;
const ROWS: u32 = 200;

pub struct S048ManyDisjointFamilies {
    scale: ScaleState,
}

impl Default for S048ManyDisjointFamilies {
    fn default() -> Self {
        Self::new()
    }
}

impl S048ManyDisjointFamilies {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S048ManyDisjointFamilies {
    fn id(&self) -> &'static str {
        "s048-many-disjoint-families"
    }

    fn description(&self) -> &'static str {
        "Fifty independent two-column formula families on one sheet."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::SingleColumnFamily, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for family in 0..FAMILIES {
                let in_col = family * 2 + 1;
                let out_col = in_col + 1;
                let in_name = col_name(in_col);
                let factor = factor(family);
                for row in 1..=ROWS {
                    sheet
                        .get_cell_mut((in_col, row))
                        .set_value_number(input_value(family, row, 0));
                    sheet
                        .get_cell_mut((out_col, row))
                        .set_formula(format!("={in_name}{row}*{factor}"));
                }
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: FAMILIES * 2,
                sheets: 1,
                formula_cells: ROWS * FAMILIES,
                value_cells: ROWS * FAMILIES,
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
            for family in [0, 24, 49] {
                for row in [1, 100, 200] {
                    invariants.push(ScenarioInvariant::CellEquals {
                        sheet: "Sheet1".to_string(),
                        row,
                        col: family * 2 + 2,
                        expected: numeric(input_value(family, row, cycles) * factor(family)),
                    });
                }
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let row = 50 + cycle as u32;
    wb.set_value(
        "Sheet1",
        row,
        1,
        LiteralValue::Number(1_000.0 + cycle as f64),
    )?;
    Ok("one_family_input")
}

fn factor(family: u32) -> f64 {
    family as f64 + 2.0
}

fn input_value(family: u32, row: u32, cycles: usize) -> f64 {
    let mut value = row as f64 + family as f64;
    if family == 0 {
        for cycle in 0..cycles {
            if row == 50 + cycle as u32 {
                value = 1_000.0 + cycle as f64;
            }
        }
    }
    value
}
