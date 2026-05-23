use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const DATA_ROWS: u32 = 1_100;
const NAMED_RANGES: u32 = 100;
const RANGE_LEN: u32 = 10;

pub struct S018NamedRanges100 {
    scale: ScaleState,
}

impl Default for S018NamedRanges100 {
    fn default() -> Self {
        Self::new()
    }
}

impl S018NamedRanges100 {
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

impl Scenario for S018NamedRanges100 {
    fn id(&self) -> &'static str {
        "s018-named-ranges-100"
    }

    fn description(&self) -> &'static str {
        "One hundred workbook named ranges referenced by a SUM formula family."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::NamedRanges, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=DATA_ROWS {
                sheet.get_cell_mut((1, r)).set_value_number(r as f64);
            }
            for k in 1..=NAMED_RANGES {
                let start = k * 10;
                let end = start + RANGE_LEN - 1;
                sheet
                    .add_defined_name(
                        format!("NamedRange_{k}"),
                        format!("Sheet1!$A${start}:$A${end}"),
                    )
                    .expect("add named range");
            }
            for r in 1..=rows {
                let named_idx = r % NAMED_RANGES + 1;
                sheet
                    .get_cell_mut((2, r))
                    .set_formula(format!("=SUM(NamedRange_{named_idx})"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: rows.max(DATA_ROWS),
                cols: 2,
                sheets: 1,
                formula_cells: rows,
                value_cells: DATA_ROWS,
                has_named_ranges: true,
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
            for row in sample_rows(rows) {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(named_range_sum(row, cycles)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let row = ((cycle * 37) % DATA_ROWS as usize) as u32 + 1;
    wb.set_value(
        "Sheet1",
        row,
        1,
        LiteralValue::Number(1000.0 + cycle as f64),
    )?;
    Ok("named_range_input")
}

fn named_range_sum(formula_row: u32, completed_cycles: usize) -> f64 {
    let named_idx = formula_row % NAMED_RANGES + 1;
    let start = named_idx * 10;
    (start..start + RANGE_LEN)
        .map(|row| data_value(row, completed_cycles))
        .sum()
}

fn data_value(row: u32, completed_cycles: usize) -> f64 {
    let mut value = row as f64;
    for cycle in 0..completed_cycles {
        let edit_row = ((cycle * 37) % DATA_ROWS as usize) as u32 + 1;
        if row == edit_row {
            value = 1000.0 + cycle as f64;
        }
    }
    value
}

fn sample_rows(rows: u32) -> Vec<u32> {
    vec![1, (rows / 2).max(1), rows]
}
