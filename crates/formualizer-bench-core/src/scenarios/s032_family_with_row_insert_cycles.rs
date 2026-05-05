use anyhow::{Context, Result};
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, fixture_path};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const INSERT_ROWS: u32 = 10;

pub struct S032FamilyWithRowInsertCycles {
    scale: ScaleState,
}

impl Default for S032FamilyWithRowInsertCycles {
    fn default() -> Self {
        Self::new()
    }
}

impl S032FamilyWithRowInsertCycles {
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

impl Scenario for S032FamilyWithRowInsertCycles {
    fn id(&self) -> &'static str {
        "s032-family-with-row-insert-cycles"
    }

    fn description(&self) -> &'static str {
        "Single-column =A*2 family with five deterministic 10-row structural inserts."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::SingleColumnFamily, ScenarioTag::InsertRows]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet.get_cell_mut((1, r)).set_value_number(r as f64);
                sheet.get_cell_mut((2, r)).set_formula(format!("=A{r}*2"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: rows + INSERT_ROWS * 5,
                cols: 2,
                sheets: 1,
                formula_cells: rows,
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
    let rows = wb
        .sheet_dimensions("Sheet1")
        .map(|(rows, _)| rows)
        .unwrap_or(1)
        .max(1);
    let before = insert_before_row(rows, cycle);
    wb.engine_mut()
        .insert_rows("Sheet1", before, INSERT_ROWS)
        .with_context(|| {
            format!("engine insert_rows Sheet1 before={before} count={INSERT_ROWS}")
        })?;
    Ok("insert_rows_10")
}

fn insert_before_row(current_rows: u32, cycle: usize) -> u32 {
    let divisors = [5, 4, 3, 2, 6];
    (current_rows / divisors[cycle % divisors.len()]).max(1)
}
