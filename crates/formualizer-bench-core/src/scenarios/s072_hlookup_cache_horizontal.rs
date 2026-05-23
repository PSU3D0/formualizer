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
const LOOKUP_COLS: u32 = 10_000;
const DISTINCT_KEYS: u32 = 50;
pub struct S072HlookupCacheHorizontal {
    scale: ScaleState,
}
impl Default for S072HlookupCacheHorizontal {
    fn default() -> Self {
        Self::new()
    }
}
impl S072HlookupCacheHorizontal {
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
impl Scenario for S072HlookupCacheHorizontal {
    fn id(&self) -> &'static str {
        "s072-hlookup-cache-horizontal"
    }
    fn description(&self) -> &'static str {
        "HLOOKUP exact-match formulas reuse a cache over a horizontal lookup table."
    }
    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::LookupHeavy,
            ScenarioTag::LookupCacheHeavy,
            ScenarioTag::SingleCellEdit,
        ]
    }
    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        let last_col = col_letters(LOOKUP_COLS);
        write_workbook(&path, |book| {
            book.new_sheet("Lookup").expect("Lookup sheet");
            let lookup = book.get_sheet_by_name_mut("Lookup").expect("Lookup exists");
            for c in 1..=LOOKUP_COLS {
                lookup.get_cell_mut((c, 1)).set_value_number(c as f64);
                lookup
                    .get_cell_mut((c, 2))
                    .set_value_number(value_for(c as f64));
            }
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet.get_cell_mut((1, r)).set_value_number(key_for(r));
                sheet.get_cell_mut((2, r)).set_formula(format!(
                    "=HLOOKUP(A{r}, Lookup!$A$1:${last_col}$2, 2, FALSE)"
                ));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 2,
                sheets: 2,
                formula_cells: rows,
                value_cells: rows + LOOKUP_COLS * 2,
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
            for row in [1, rows.max(2) / 2, rows] {
                let key = key_at(row, rows, cycles);
                out.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(value_for(key)),
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
    ((row - 1) % DISTINCT_KEYS + 1) as f64
}
fn edited_key(cycle: usize) -> f64 {
    ((cycle as u32 * 17) % DISTINCT_KEYS + 1) as f64
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
fn value_for(key: f64) -> f64 {
    key * 10.0 + 5.0
}
fn col_letters(mut col: u32) -> String {
    let mut out = Vec::new();
    while col > 0 {
        col -= 1;
        out.push((b'A' + (col % 26) as u8) as char);
        col /= 26;
    }
    out.iter().rev().collect()
}
