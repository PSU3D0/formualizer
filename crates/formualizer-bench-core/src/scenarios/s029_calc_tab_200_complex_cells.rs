use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const DATA_ROWS: u32 = 1_000;
const CALC_ROWS: u32 = 200;

pub struct S029CalcTab200ComplexCells {
    scale: ScaleState,
}

impl Default for S029CalcTab200ComplexCells {
    fn default() -> Self {
        Self::new()
    }
}

impl S029CalcTab200ComplexCells {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S029CalcTab200ComplexCells {
    fn id(&self) -> &'static str {
        "s029-calc-tab-200-complex-cells"
    }

    fn description(&self) -> &'static str {
        "Fixed-size Calc tab with 200 complex formulas over a 1k-row Data tab."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::Mixed,
            ScenarioTag::MultiSheet,
            ScenarioTag::CrossSheet,
            ScenarioTag::SingleCellEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            {
                let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
                sheet.set_name("Data");
            }
            book.new_sheet("Calc").expect("Calc sheet");

            let data = book.get_sheet_by_name_mut("Data").expect("Data exists");
            for r in 1..=DATA_ROWS {
                data.get_cell_mut((1, r)).set_value_number(r as f64);
                data.get_cell_mut((2, r)).set_value_number(data_value(r, 0));
                data.get_cell_mut((3, r)).set_value(data_type(r));
            }

            // s029 is intentionally fixed-size at all scales: 200 complex calc cells.
            let calc = book.get_sheet_by_name_mut("Calc").expect("Calc exists");
            for r in 1..=CALC_ROWS {
                let second_key = r * 7;
                calc.get_cell_mut((1, r)).set_formula(format!(
                    "=VLOOKUP({r}, Data!$A$1:$B$1000, 2, FALSE) \
                     + IFERROR(VLOOKUP({second_key}, Data!$A$1:$B$1000, 2, FALSE), 0) \
                     + SUMIFS(Data!$B$1:$B$1000, Data!$C$1:$C$1000, \"Type0\") \
                     + COUNTIFS(Data!$C$1:$C$1000, \"Type1\") \
                     + IF(MOD({r}, 2) = 0, 100, 200) \
                     + LEN(\"row-\" & {r})"
                ));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: DATA_ROWS,
                cols: 3,
                sheets: 2,
                formula_cells: CALC_ROWS,
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
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Calc".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            invariants.push(ScenarioInvariant::CellEquals {
                sheet: "Calc".to_string(),
                row: 1,
                col: 1,
                expected: numeric(calc_value_for_row1(completed_cycles(phase))),
            });
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let row = ((cycle * 37) % DATA_ROWS as usize) as u32 + 1;
    wb.set_value("Data", row, 2, LiteralValue::Number(edited_value(cycle)))?;
    Ok("data_value")
}

fn data_type(row: u32) -> String {
    format!("Type{}", row % 3)
}

fn initial_value(row: u32) -> f64 {
    row as f64 * 10.0 + 5.0
}

fn edited_value(cycle: usize) -> f64 {
    10_000.0 + cycle as f64
}

fn edited_row(cycle: usize) -> u32 {
    ((cycle * 37) % DATA_ROWS as usize) as u32 + 1
}

fn data_value(row: u32, completed_cycles: usize) -> f64 {
    let mut value = initial_value(row);
    for cycle in 0..completed_cycles {
        if row == edited_row(cycle) {
            value = edited_value(cycle);
        }
    }
    value
}

fn sum_for_type(criteria: &str, completed_cycles: usize) -> f64 {
    (1..=DATA_ROWS)
        .filter(|row| data_type(*row) == criteria)
        .map(|row| data_value(row, completed_cycles))
        .sum()
}

fn count_for_type(criteria: &str) -> f64 {
    (1..=DATA_ROWS)
        .filter(|row| data_type(*row) == criteria)
        .count() as f64
}

fn calc_value_for_row1(completed_cycles: usize) -> f64 {
    data_value(1, completed_cycles)
        + data_value(7, completed_cycles)
        + sum_for_type("Type0", completed_cycles)
        + count_for_type("Type1")
        + 200.0
        + "row-1".len() as f64
}
