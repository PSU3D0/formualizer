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

pub struct S069XlookupWildcardDeeplyNestedIf {
    scale: ScaleState,
}

impl Default for S069XlookupWildcardDeeplyNestedIf {
    fn default() -> Self {
        Self::new()
    }
}

impl S069XlookupWildcardDeeplyNestedIf {
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

impl Scenario for S069XlookupWildcardDeeplyNestedIf {
    fn id(&self) -> &'static str {
        "s069-xlookup-wildcard-deeply-nested-if"
    }

    fn description(&self) -> &'static str {
        "XLOOKUP exact-match family nested four levels inside IF short-circuit logic."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::LookupHeavy,
            ScenarioTag::SingleCellEdit,
            ScenarioTag::ShortCircuit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            book.new_sheet("Lookup").expect("Lookup sheet");
            let lookup = book.get_sheet_by_name_mut("Lookup").expect("Lookup exists");
            for row in 1..=LOOKUP_ROWS {
                lookup
                    .get_cell_mut((1, row))
                    .set_value(format!("K{row:04}-item"));
                lookup
                    .get_cell_mut((2, row))
                    .set_value_number(lookup_value(row));
            }

            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=rows {
                sheet.get_cell_mut((1, row)).set_value(initial_pattern(row));
                sheet.get_cell_mut((3, row)).set_value_number(1.0);
                sheet.get_cell_mut((2, row)).set_formula(format!(
                    "=IF(C{row}<0, -1, IF(C{row}<0, -2, IF(C{row}<0, -3, IF(C{row}<0, -4, XLOOKUP(A{row}, Lookup!$A$1:$A$1000, Lookup!$B$1:$B$1000, \"NF\", 0, 1)))))"
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
                let key = key_at(row, rows, cycles);
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
    wb.set_value(
        "Sheet1",
        row,
        1,
        LiteralValue::Text(pattern_for_key(edited_key(cycle))),
    )?;
    Ok("xlookup_wildcard_key")
}

fn initial_pattern(row: u32) -> String {
    pattern_for_key(((row - 1) % LOOKUP_ROWS) + 1)
}

fn pattern_for_key(key: u32) -> String {
    format!("K{key:04}-item")
}

fn edited_key(cycle: usize) -> u32 {
    ((cycle as u32 * 113 + 43) % LOOKUP_ROWS) + 1
}

fn key_at(row: u32, rows: u32, completed_cycles: usize) -> u32 {
    let mut key = ((row - 1) % LOOKUP_ROWS) + 1;
    for cycle in 0..completed_cycles {
        let edit_row = ((cycle * 37) % rows as usize) as u32 + 1;
        if row == edit_row {
            key = edited_key(cycle);
        }
    }
    key
}

fn lookup_value(row: u32) -> f64 {
    row as f64 * 13.0 + 9.0
}
