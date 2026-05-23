use anyhow::{Context, Result};
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S035FamilyWithColumnDelete {
    scale: ScaleState,
}

impl Default for S035FamilyWithColumnDelete {
    fn default() -> Self {
        Self::new()
    }
}

impl S035FamilyWithColumnDelete {
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

impl Scenario for S035FamilyWithColumnDelete {
    fn id(&self) -> &'static str {
        "s035-family-with-column-delete"
    }

    fn description(&self) -> &'static str {
        "Six-column mixed formula family with five deterministic one-column structural deletes."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::MultiColumnFamily, ScenarioTag::DeleteColumns]
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
                sheet
                    .get_cell_mut((5, r))
                    .set_formula(format!("=A{r}+B{r}"));
                sheet
                    .get_cell_mut((6, r))
                    .set_formula(format!("=C{r}+D{r}"));
                for c in 7..=11 {
                    sheet.get_cell_mut((c, r)).set_value_number(0.0);
                }
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 11,
                sheets: 1,
                formula_cells: rows.saturating_mul(5),
                value_cells: rows.saturating_mul(6),
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
            let cols = 11u32.saturating_sub(cycles as u32);
            invariants.reserve(rows as usize * cols as usize);
            for row in 1..=rows {
                for col in 1..=cols {
                    invariants.push(ScenarioInvariant::CellEquals {
                        sheet: "Sheet1".to_string(),
                        row,
                        col,
                        expected: expected_value_after_buffer_deletes(row, col),
                    });
                }
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let _ = cycle;
    let col = 7;
    wb.engine_mut()
        .delete_columns("Sheet1", col, 1)
        .with_context(|| format!("engine delete_columns Sheet1 start={col} count=1"))?;
    Ok("delete_buffer_column_1")
}

fn expected_value_after_buffer_deletes(row: u32, col: u32) -> formualizer_common::LiteralValue {
    match col {
        1 => numeric(row as f64),
        2 => numeric(row as f64 + 1.0),
        3 => numeric(row as f64 * 2.0),
        4 => numeric(row as f64 - 3.0),
        5 => numeric(row as f64 * 2.0 + 1.0),
        6 => numeric(row as f64 * 3.0 - 3.0),
        _ => numeric(0.0),
    }
}
