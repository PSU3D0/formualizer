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

pub struct S062IndexDeeplyNestedInIf {
    scale: ScaleState,
}

impl Default for S062IndexDeeplyNestedInIf {
    fn default() -> Self {
        Self::new()
    }
}

impl S062IndexDeeplyNestedInIf {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    pub fn rows(_scale: ScenarioScale) -> u32 {
        ROWS
    }
}

impl Scenario for S062IndexDeeplyNestedInIf {
    fn id(&self) -> &'static str {
        "s062-index-deeply-nested-in-if"
    }

    fn description(&self) -> &'static str {
        "INDEX lookup nested inside a five-level IF/MOD short-circuit family."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::LookupHeavy,
            ScenarioTag::ShortCircuit,
            ScenarioTag::ReferenceForwarding,
        ]
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
                    .set_value_number(table_value(row));
            }
            for row in 1..=rows {
                sheet
                    .get_cell_mut((1, row))
                    .set_value_number(initial_position(row));
                sheet.get_cell_mut((2, row)).set_formula(format!(
                    "=IF(A{row}>0, IF(A{row}<150, IF(MOD(A{row},2)=0, IF(A{row}>50, INDEX($D$1:$D${TABLE_ROWS}, A{row}), -1), -2), -3), -4)"
                ));
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
                let position = position_at(row, rows, cycles);
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(nested_if_expected(position)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let row = ((cycle * 37) % ROWS as usize) as u32 + 1;
    wb.set_value(
        "Sheet1",
        row,
        1,
        LiteralValue::Number(edited_position(cycle) as f64),
    )?;
    Ok("nested_index_position")
}

fn initial_position(row: u32) -> f64 {
    ((row - 1) % TABLE_ROWS + 1) as f64
}

fn edited_position(cycle: usize) -> u32 {
    ((cycle as u32 * 41 + 54) % 149) + 1
}

fn position_at(row: u32, rows: u32, completed_cycles: usize) -> u32 {
    let mut position = initial_position(row) as u32;
    for cycle in 0..completed_cycles {
        let edit_row = ((cycle * 37) % rows as usize) as u32 + 1;
        if row == edit_row {
            position = edited_position(cycle);
        }
    }
    position
}

fn nested_if_expected(position: u32) -> f64 {
    if position > 0 {
        if position < 150 {
            if position.is_multiple_of(2) {
                if position > 50 {
                    table_value(position)
                } else {
                    -1.0
                }
            } else {
                -2.0
            }
        } else {
            -3.0
        }
    } else {
        -4.0
    }
}

fn table_value(row: u32) -> f64 {
    row as f64 * 10.0 + 5.0
}
