use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const DATA_ROWS: u32 = 2_000;
const CALC_ROWS: u32 = 200;

pub struct S030CalcAndDataTabsMixed {
    scale: ScaleState,
}

impl Default for S030CalcAndDataTabsMixed {
    fn default() -> Self {
        Self::new()
    }
}

impl S030CalcAndDataTabsMixed {
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

impl Scenario for S030CalcAndDataTabsMixed {
    fn id(&self) -> &'static str {
        "s030-calc-and-data-tabs-mixed"
    }

    fn description(&self) -> &'static str {
        "Three-tab mixed workbook: fixed Data, copied cross-sheet Family, and complex Calc cells."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::Mixed,
            ScenarioTag::MultiSheet,
            ScenarioTag::CrossSheet,
            ScenarioTag::SpanPromotable,
            ScenarioTag::SingleCellEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            {
                let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
                sheet.set_name("Data");
            }
            book.new_sheet("Family").expect("Family sheet");
            book.new_sheet("Calc").expect("Calc sheet");

            let data = book.get_sheet_by_name_mut("Data").expect("Data exists");
            for r in 1..=DATA_ROWS {
                data.get_cell_mut((1, r)).set_value_number(r as f64);
                data.get_cell_mut((2, r))
                    .set_value_number(initial_data_b(r));
                data.get_cell_mut((3, r)).set_value(data_type(r));
            }

            let family = book.get_sheet_by_name_mut("Family").expect("Family exists");
            for r in 1..=rows {
                family
                    .get_cell_mut((1, r))
                    .set_formula(format!("=Data!A{r} * 2 + Data!B{r}"));
            }

            let calc = book.get_sheet_by_name_mut("Calc").expect("Calc exists");
            for r in 1..=CALC_ROWS {
                calc.get_cell_mut((1, r)).set_formula(format!(
                    "=VLOOKUP({r}, Data!$A$1:$B$2000, 2, FALSE) \
                     + IFERROR(SUMIFS(Data!$B$1:$B$2000, Data!$C$1:$C$2000, \"Type0\"), 0) \
                     + IF({r} > 100, 1000, 0)"
                ));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: rows.max(DATA_ROWS),
                cols: 3,
                sheets: 3,
                formula_cells: rows + CALC_ROWS,
                value_cells: DATA_ROWS.saturating_mul(3),
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
        let mut invariants = Vec::with_capacity(3);
        for sheet in ["Data", "Family", "Calc"] {
            invariants.push(ScenarioInvariant::NoErrorCells {
                sheet: sheet.to_string(),
            });
        }
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            invariants.reserve(rows as usize + 1);
            for row in 1..=rows {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Family".to_string(),
                    row,
                    col: 1,
                    expected: numeric(family_value(row, cycles)),
                });
            }
            invariants.push(ScenarioInvariant::CellEquals {
                sheet: "Calc".to_string(),
                row: 1,
                col: 1,
                expected: numeric(calc_value_for_row1(cycles)),
            });
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value(
        "Data",
        edited_row(cycle),
        2,
        LiteralValue::Number(edited_value(cycle)),
    )?;
    Ok("data_b_value")
}

fn initial_data_b(row: u32) -> f64 {
    row as f64 * 2.0
}

fn edited_value(cycle: usize) -> f64 {
    10_000.0 + cycle as f64
}

fn edited_row(cycle: usize) -> u32 {
    ((cycle * 37) % DATA_ROWS as usize) as u32 + 1
}

fn data_a(row: u32) -> f64 {
    if row <= DATA_ROWS { row as f64 } else { 0.0 }
}

fn data_b(row: u32, completed_cycles: usize) -> f64 {
    if row > DATA_ROWS {
        return 0.0;
    }
    let mut value = initial_data_b(row);
    for cycle in 0..completed_cycles {
        if row == edited_row(cycle) {
            value = edited_value(cycle);
        }
    }
    value
}

fn data_type(row: u32) -> String {
    format!("Type{}", row % 3)
}

fn family_value(row: u32, completed_cycles: usize) -> f64 {
    data_a(row) * 2.0 + data_b(row, completed_cycles)
}

fn sum_type0(completed_cycles: usize) -> f64 {
    (1..=DATA_ROWS)
        .filter(|row| data_type(*row) == "Type0")
        .map(|row| data_b(row, completed_cycles))
        .sum()
}

fn calc_value_for_row1(completed_cycles: usize) -> f64 {
    data_b(1, completed_cycles) + sum_type0(completed_cycles)
}
