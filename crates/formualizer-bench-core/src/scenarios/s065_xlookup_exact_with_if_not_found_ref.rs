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

const LOOKUP_ROWS: u32 = 1_000;

pub struct S065XlookupExactWithIfNotFoundRef {
    scale: ScaleState,
}

impl Default for S065XlookupExactWithIfNotFoundRef {
    fn default() -> Self {
        Self::new()
    }
}

impl S065XlookupExactWithIfNotFoundRef {
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

impl Scenario for S065XlookupExactWithIfNotFoundRef {
    fn id(&self) -> &'static str {
        "s065-xlookup-exact-with-if-not-found-ref"
    }

    fn description(&self) -> &'static str {
        "XLOOKUP exact family with row-relative if_not_found fallback references."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::LookupHeavy, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            book.new_sheet("Lookup").expect("Lookup sheet");
            let lookup = book.get_sheet_by_name_mut("Lookup").expect("Lookup exists");
            for row in 1..=LOOKUP_ROWS {
                lookup.get_cell_mut((1, row)).set_value_number(row as f64);
                lookup
                    .get_cell_mut((2, row))
                    .set_value_number(lookup_value(row));
            }

            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=rows {
                sheet
                    .get_cell_mut((1, row))
                    .set_value_number(initial_key(row));
                sheet
                    .get_cell_mut((3, row))
                    .set_value_number(initial_fallback(row));
                sheet.get_cell_mut((2, row)).set_formula(format!(
                    "=XLOOKUP(A{row}, Lookup!$A$1:$A$1000, Lookup!$B$1:$B$1000, C{row}, 0, 1)"
                ));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: rows.max(LOOKUP_ROWS),
                cols: 3,
                sheets: 2,
                formula_cells: rows,
                value_cells: rows.saturating_mul(2) + LOOKUP_ROWS.saturating_mul(2),
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
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(expected_value(row, rows, cycles)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let rows = detect_nonempty_rows(wb, "Sheet1", 1) as usize;
    let row = ((cycle * 37) % rows) as u32 + 1;
    wb.set_value(
        "Sheet1",
        row,
        1,
        LiteralValue::Number(LOOKUP_ROWS as f64 + cycle as f64 + 1.0),
    )?;
    wb.set_value(
        "Sheet1",
        row,
        3,
        LiteralValue::Number(edited_fallback(cycle)),
    )?;
    Ok("xlookup_key_and_fallback")
}

fn initial_key(row: u32) -> f64 {
    if row.is_multiple_of(5) {
        LOOKUP_ROWS as f64 + row as f64
    } else {
        ((row - 1) % LOOKUP_ROWS + 1) as f64
    }
}

fn initial_fallback(row: u32) -> f64 {
    -(row as f64)
}

fn edited_fallback(cycle: usize) -> f64 {
    -10_000.0 - cycle as f64
}

fn expected_value(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    let mut key = initial_key(row);
    let mut fallback = initial_fallback(row);
    for cycle in 0..completed_cycles {
        let edit_row = ((cycle * 37) % rows as usize) as u32 + 1;
        if row == edit_row {
            key = LOOKUP_ROWS as f64 + cycle as f64 + 1.0;
            fallback = edited_fallback(cycle);
        }
    }
    if (1.0..=LOOKUP_ROWS as f64).contains(&key) {
        lookup_value(key as u32)
    } else {
        fallback
    }
}

fn lookup_value(row: u32) -> f64 {
    row as f64 * 8.0 + 2.0
}
