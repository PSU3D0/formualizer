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

const SUBSET_EDITS: usize = 16;

pub struct S004FiveMixedFamilies {
    scale: ScaleState,
}

impl Default for S004FiveMixedFamilies {
    fn default() -> Self {
        Self::new()
    }
}

impl S004FiveMixedFamilies {
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

impl Scenario for S004FiveMixedFamilies {
    fn id(&self) -> &'static str {
        "s004-five-mixed-families"
    }

    fn description(&self) -> &'static str {
        "Five heterogeneous formula families derived from column A."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::MultiColumnFamily,
            ScenarioTag::SpanPromotable,
            ScenarioTag::Mixed,
            ScenarioTag::BulkEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sh.get_cell_mut((1, r)).set_value_number(r as f64);
                sh.get_cell_mut((2, r)).set_formula(format!("=A{r}+1"));
                sh.get_cell_mut((3, r)).set_formula(format!("=A{r}*2"));
                sh.get_cell_mut((4, r)).set_formula(format!("=A{r}-3"));
                sh.get_cell_mut((5, r)).set_formula(format!("=A{r}/2"));
                sh.get_cell_mut((6, r)).set_formula(format!("=A{r}+B{r}"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 6,
                sheets: 1,
                formula_cells: rows.saturating_mul(5),
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

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let rows = Self::rows(self.scale.get_or_small());
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            invariants.reserve(rows as usize * 5);
            for row in 1..=rows {
                let a = a_at(row, rows, cycles);
                let expected = [a + 1.0, a * 2.0, a - 3.0, a / 2.0, a + (a + 1.0)];
                for (offset, expected) in expected.into_iter().enumerate() {
                    invariants.push(ScenarioInvariant::CellEquals {
                        sheet: "Sheet1".to_string(),
                        row,
                        col: offset as u32 + 2,
                        expected: numeric(expected),
                    });
                }
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let rows = detect_nonempty_rows(wb, "Sheet1", 1) as usize;
    for idx in 0..SUBSET_EDITS {
        let row = ((cycle * 53 + idx * 97) % rows) as u32 + 1;
        let value = edited_value(cycle, idx);
        wb.set_value("Sheet1", row, 1, LiteralValue::Number(value))?;
    }
    Ok("random_a_subset")
}

fn a_at(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    let mut value = row as f64;
    for cycle in 0..completed_cycles {
        for idx in 0..SUBSET_EDITS {
            let edit_row = ((cycle * 53 + idx * 97) % rows as usize) as u32 + 1;
            if row == edit_row {
                value = edited_value(cycle, idx);
            }
        }
    }
    value
}

fn edited_value(cycle: usize, idx: usize) -> f64 {
    2000.0 + (cycle * SUBSET_EDITS + idx) as f64
}
