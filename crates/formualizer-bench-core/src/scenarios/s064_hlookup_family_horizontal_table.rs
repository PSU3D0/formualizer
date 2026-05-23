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

const LOOKUP_COLS: u32 = 1_000;

pub struct S064HlookupFamilyHorizontalTable {
    scale: ScaleState,
}

impl Default for S064HlookupFamilyHorizontalTable {
    fn default() -> Self {
        Self::new()
    }
}

impl S064HlookupFamilyHorizontalTable {
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

impl Scenario for S064HlookupFamilyHorizontalTable {
    fn id(&self) -> &'static str {
        "s064-hlookup-family-horizontal-table"
    }

    fn description(&self) -> &'static str {
        "HLOOKUP family against a shared horizontal 1k-column lookup table."
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
            for col in 1..=LOOKUP_COLS {
                lookup.get_cell_mut((col, 1)).set_value_number(col as f64);
                lookup
                    .get_cell_mut((col, 2))
                    .set_value_number(lookup_value(col));
            }

            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=rows {
                sheet
                    .get_cell_mut((1, row))
                    .set_value_number(initial_key(row));
                sheet
                    .get_cell_mut((2, row))
                    .set_formula(format!("=HLOOKUP(A{row}, Lookup!$A$1:$ALL$2, 2, FALSE)"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: rows.max(2),
                cols: LOOKUP_COLS,
                sheets: 2,
                formula_cells: rows,
                value_cells: rows + LOOKUP_COLS.saturating_mul(2),
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
                let key = key_at(row, rows, cycles) as u32;
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(lookup_value(key)),
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
    Ok("hlookup_key")
}

fn initial_key(row: u32) -> f64 {
    ((row - 1) % LOOKUP_COLS + 1) as f64
}

fn edited_key(cycle: usize) -> f64 {
    ((cycle as u32 * 113 + 17) % LOOKUP_COLS + 1) as f64
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

fn lookup_value(key: u32) -> f64 {
    key as f64 * 12.0 + 3.0
}
