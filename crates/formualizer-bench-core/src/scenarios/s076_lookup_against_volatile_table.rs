use super::common::{
    ScaleState, completed_cycles, detect_nonempty_rows, fixture_path, has_evaluated_formulas,
    numeric,
};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};
use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;
const LOOKUP_ROWS: u32 = 10_000;
const DISTINCT_KEYS: u32 = 50;
pub struct S076LookupAgainstVolatileTable {
    scale: ScaleState,
}
impl Default for S076LookupAgainstVolatileTable {
    fn default() -> Self {
        Self::new()
    }
}
impl S076LookupAgainstVolatileTable {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
    fn rows(scale: ScenarioScale) -> u32 {
        match scale {
            ScenarioScale::Small => 1_000,
            ScenarioScale::Medium => 10_000,
            ScenarioScale::Large => 50_000,
        }
    }
}
impl Scenario for S076LookupAgainstVolatileTable {
    fn id(&self) -> &'static str {
        "s076-lookup-against-volatile-table"
    }
    fn description(&self) -> &'static str {
        "VLOOKUP exact-match against a table containing NOW, forcing lookup-cache refusal."
    }
    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::LookupHeavy,
            ScenarioTag::LookupCacheHeavy,
            ScenarioTag::Volatile,
            ScenarioTag::SingleCellEdit,
        ]
    }
    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            book.new_sheet("Lookup").expect("Lookup sheet");
            let lookup = book.get_sheet_by_name_mut("Lookup").expect("Lookup exists");
            lookup.get_cell_mut((1, 1)).set_formula("=IF(NOW()>0,0,0)");
            lookup.get_cell_mut((2, 1)).set_value_number(0.0);
            for r in 2..=LOOKUP_ROWS {
                lookup.get_cell_mut((1, r)).set_value_number(r as f64);
                lookup.get_cell_mut((2, r)).set_value_number(value_for(r));
            }
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet.get_cell_mut((1, r)).set_value_number(key_for(r));
                sheet.get_cell_mut((2, r)).set_formula(format!(
                    "=VLOOKUP(A{r}, Lookup!$A$1:$B${LOOKUP_ROWS}, 2, FALSE)"
                ));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 2,
                sheets: 2,
                formula_cells: rows + 1,
                value_cells: rows + LOOKUP_ROWS * 2 - 1,
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
        let mut out = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            for row in [2, rows.max(2) / 2, rows] {
                let key = key_at(row, rows, cycles).max(2.0);
                out.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(value_for(key as u32)),
                });
            }
        }
        out
    }
}
fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let rows = detect_nonempty_rows(wb, "Sheet1", 1) as usize;
    let row = ((cycle * 37) % rows) as u32 + 1;
    wb.set_value("Sheet1", row, 1, LiteralValue::Number(edited_key(cycle)))?;
    Ok("lookup_key")
}
fn key_for(row: u32) -> f64 {
    ((row - 1) % DISTINCT_KEYS + 2) as f64
}
fn edited_key(cycle: usize) -> f64 {
    ((cycle as u32 * 17) % DISTINCT_KEYS + 2) as f64
}
fn key_at(row: u32, rows: u32, cycles: usize) -> f64 {
    let mut key = key_for(row);
    for cycle in 0..cycles {
        let edit_row = ((cycle * 37) % rows as usize) as u32 + 1;
        if row == edit_row {
            key = edited_key(cycle);
        }
    }
    key
}
fn value_for(key: u32) -> f64 {
    key as f64 * 10.0 + 5.0
}
