use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{
    ScaleState, completed_cycles, detect_nonempty_rows, fixture_path, has_evaluated_formulas,
    numeric,
};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const TABLE_ROWS: u32 = 1_000;

pub struct S067IndexMatchApproximateChain {
    scale: ScaleState,
}

impl Default for S067IndexMatchApproximateChain {
    fn default() -> Self {
        Self::new()
    }
}

impl S067IndexMatchApproximateChain {
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

impl Scenario for S067IndexMatchApproximateChain {
    fn id(&self) -> &'static str {
        "s067-index-match-approximate-chain"
    }

    fn description(&self) -> &'static str {
        "INDEX/MATCH family with approximate MATCH over a sorted table."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::LookupHeavy,
            ScenarioTag::SingleCellEdit,
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
                    .set_value_number(lookup_value(row));
                sheet.get_cell_mut((5, row)).set_value_number(row as f64);
            }
            for row in 1..=rows {
                sheet
                    .get_cell_mut((1, row))
                    .set_value_number(initial_key(row));
                sheet.get_cell_mut((2, row)).set_formula(format!(
                    "=INDEX($D$1:$D$1000, MATCH(A{row}, $E$1:$E$1000, 1))"
                ));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: rows.max(TABLE_ROWS),
                cols: 5,
                sheets: 1,
                formula_cells: rows,
                value_cells: rows + TABLE_ROWS.saturating_mul(2),
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
            for row in [1, rows / 2, rows] {
                let key = key_at(row, rows, cycles);
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(lookup_value(approx_position(key))),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let rows = detect_nonempty_rows(wb, "Sheet1", 1) as usize;
    let row = ((cycle * 37) % rows) as u32 + 1;
    wb.set_value("Sheet1", row, 1, LiteralValue::Number(edited_key(cycle)))?;
    Ok("index_match_approx_key")
}

fn initial_key(row: u32) -> f64 {
    ((row - 1) % TABLE_ROWS + 1) as f64 + 0.4
}

fn edited_key(cycle: usize) -> f64 {
    ((cycle as u32 * 113 + 29) % TABLE_ROWS + 1) as f64 + 0.4
}

fn key_at(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    let mut key = initial_key(row);
    for cycle in 0..completed_cycles {
        let edit_row = ((cycle * 37) % rows as usize) as u32 + 1;
        if row == edit_row {
            key = edited_key(cycle);
        }
    }
    key
}

fn approx_position(key: f64) -> u32 {
    key.floor().clamp(1.0, TABLE_ROWS as f64) as u32
}

fn lookup_value(row: u32) -> f64 {
    row as f64 * 10.0 + 5.0
}
