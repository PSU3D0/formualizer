use anyhow::{Context, Result};
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, fixture_path};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S034FamilyWithColumnInsert {
    scale: ScaleState,
}

impl Default for S034FamilyWithColumnInsert {
    fn default() -> Self {
        Self::new()
    }
}

impl S034FamilyWithColumnInsert {
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

impl Scenario for S034FamilyWithColumnInsert {
    fn id(&self) -> &'static str {
        "s034-family-with-column-insert"
    }

    fn description(&self) -> &'static str {
        "Three-column formula family over column A with five one-column structural inserts."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::MultiColumnFamily, ScenarioTag::InsertColumns]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet.get_cell_mut((1, r)).set_value_number(r as f64);
                sheet.get_cell_mut((2, r)).set_formula(format!("=A{r}+1"));
                sheet.get_cell_mut((3, r)).set_formula(format!("=A{r}*2"));
                sheet.get_cell_mut((4, r)).set_formula(format!("=A{r}-3"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 9,
                sheets: 1,
                formula_cells: rows.saturating_mul(3),
                value_cells: rows,
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

    fn invariants(&self, _phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }]
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let before = [3, 2, 5, 1, 4][cycle % 5];
    wb.engine_mut()
        .insert_columns("Sheet1", before, 1)
        .with_context(|| format!("engine insert_columns Sheet1 before={before} count=1"))?;
    Ok("insert_column_1")
}
