use super::common::{ScaleState, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};
use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;
const LOOKUP_ROWS: u32 = 10_000;
pub struct S075LookupWithEditCycles {
    scale: ScaleState,
}
impl Default for S075LookupWithEditCycles {
    fn default() -> Self {
        Self::new()
    }
}
impl S075LookupWithEditCycles {
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
impl Scenario for S075LookupWithEditCycles {
    fn id(&self) -> &'static str {
        "s075-lookup-with-edit-cycles"
    }
    fn description(&self) -> &'static str {
        "VLOOKUP exact-match with edit cycles over lookup values, lookup array, and result column."
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
        write_workbook(&path, |book| {
            book.new_sheet("Lookup").expect("Lookup sheet");
            let lookup = book.get_sheet_by_name_mut("Lookup").expect("Lookup exists");
            for r in 1..=LOOKUP_ROWS {
                lookup.get_cell_mut((1, r)).set_value_number(r as f64);
                lookup.get_cell_mut((2, r)).set_value_number(value_for(r));
            }
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet
                    .get_cell_mut((1, r))
                    .set_value_number(((r - 1) % LOOKUP_ROWS + 1) as f64);
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
                formula_cells: rows,
                value_cells: rows + LOOKUP_ROWS * 2,
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
            for row in [1, rows.max(2) / 2, rows] {
                let key = ((row - 1) % LOOKUP_ROWS + 1) as f64;
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
    match cycle % 3 {
        0 => {
            wb.set_value("Sheet1", 999, 1, LiteralValue::Number(999.0))?;
            Ok("lookup_value")
        }
        1 => {
            wb.set_value("Lookup", 9_999, 1, LiteralValue::Number(9_999.0))?;
            Ok("lookup_array")
        }
        _ => {
            wb.set_value("Lookup", 9_998, 2, LiteralValue::Number(value_for(9_998)))?;
            Ok("result_column")
        }
    }
}
fn value_for(key: u32) -> f64 {
    key as f64 * 10.0 + 5.0
}
