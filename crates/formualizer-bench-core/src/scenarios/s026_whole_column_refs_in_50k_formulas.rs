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

pub struct S026WholeColumnRefsIn50kFormulas {
    scale: ScaleState,
}

impl Default for S026WholeColumnRefsIn50kFormulas {
    fn default() -> Self {
        Self::new()
    }
}

impl S026WholeColumnRefsIn50kFormulas {
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

impl Scenario for S026WholeColumnRefsIn50kFormulas {
    fn id(&self) -> &'static str {
        "s026-whole-column-refs-in-50k-formulas"
    }

    fn description(&self) -> &'static str {
        "Formula family using whole-column SUM($A:$A) references."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        // Smoke probe observed span_count=0 for whole-column SUM($A:$A), so this
        // remains whole-column coverage without claiming SpanPromotable.
        &[ScenarioTag::WholeColumnRefs, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet.get_cell_mut((1, r)).set_value_number(r as f64);
            }
            for r in 1..=rows {
                sheet
                    .get_cell_mut((2, r))
                    .set_formula(format!("=SUM($A:$A) - A{r}"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
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

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let rows = Self::rows(self.scale.get_or_small());
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            let total = total_sum(rows, cycles);
            for row in sample_rows(rows) {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(total - data_value(row, rows, cycles)),
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
        LiteralValue::Number(1000.0 + cycle as f64),
    )?;
    Ok("whole_column_input")
}

fn data_value(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    let mut value = row as f64;
    for cycle in 0..completed_cycles {
        let edit_row = ((cycle * 37) % rows as usize) as u32 + 1;
        if row == edit_row {
            value = 1000.0 + cycle as f64;
        }
    }
    value
}

fn total_sum(rows: u32, completed_cycles: usize) -> f64 {
    let mut total = rows as f64 * (rows as f64 + 1.0) / 2.0;
    for cycle in 0..completed_cycles {
        let edit_row = ((cycle * 37) % rows as usize) as u32 + 1;
        total += 1000.0 + cycle as f64 - data_value(edit_row, rows, cycle);
    }
    total
}

fn sample_rows(rows: u32) -> Vec<u32> {
    vec![1, (rows / 2).max(1), rows]
}
