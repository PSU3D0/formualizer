use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const ROWS: u32 = 1_000;
const TABLE_ROWS: u32 = 1_000;

pub struct S063IndexWithTableEdit {
    scale: ScaleState,
}

impl Default for S063IndexWithTableEdit {
    fn default() -> Self {
        Self::new()
    }
}

impl S063IndexWithTableEdit {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    pub fn rows(_scale: ScenarioScale) -> u32 {
        ROWS
    }
}

impl Scenario for S063IndexWithTableEdit {
    fn id(&self) -> &'static str {
        "s063-index-with-table-edit"
    }

    fn description(&self) -> &'static str {
        "INDEX lookup family whose edit cycles mutate the shared lookup table."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::LookupHeavy, ScenarioTag::ReferenceForwarding]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=TABLE_ROWS {
                sheet
                    .get_cell_mut((4, row))
                    .set_value_number(initial_table_value(row));
            }
            for row in 1..=rows {
                sheet
                    .get_cell_mut((1, row))
                    .set_value_number(initial_position(row));
                sheet
                    .get_cell_mut((2, row))
                    .set_formula(format!("=INDEX($D$1:$D${TABLE_ROWS}, A{row})"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: rows.max(TABLE_ROWS),
                cols: 4,
                sheets: 1,
                formula_cells: rows,
                value_cells: rows + TABLE_ROWS,
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
            let cycles = completed_cycles(phase);
            invariants.reserve(rows as usize);
            for row in 1..=rows {
                let position = initial_position(row) as u32;
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(table_value_at(position, cycles)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let table_row = edited_table_row(cycle);
    wb.set_value(
        "Sheet1",
        table_row,
        4,
        LiteralValue::Number(edited_table_value(cycle)),
    )?;
    Ok("index_table_cell")
}

fn initial_position(row: u32) -> f64 {
    ((row - 1) % TABLE_ROWS + 1) as f64
}

fn edited_table_row(cycle: usize) -> u32 {
    ((cycle * 97) % TABLE_ROWS as usize) as u32 + 1
}

fn edited_table_value(cycle: usize) -> f64 {
    5_000.0 + cycle as f64 * 11.0
}

fn table_value_at(row: u32, completed_cycles: usize) -> f64 {
    let mut value = initial_table_value(row);
    for cycle in 0..completed_cycles {
        if row == edited_table_row(cycle) {
            value = edited_table_value(cycle);
        }
    }
    value
}

fn initial_table_value(row: u32) -> f64 {
    row as f64 * 10.0 + 5.0
}
