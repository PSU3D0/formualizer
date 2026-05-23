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

const DENSE_EDIT_LEN: usize = 16;
const SPARSE_EDITS: usize = 16;

pub struct S003FinanceAnchoredArithmeticFamily {
    scale: ScaleState,
}

impl Default for S003FinanceAnchoredArithmeticFamily {
    fn default() -> Self {
        Self::new()
    }
}

impl S003FinanceAnchoredArithmeticFamily {
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

impl Scenario for S003FinanceAnchoredArithmeticFamily {
    fn id(&self) -> &'static str {
        "s003-finance-anchored-arithmetic-family"
    }

    fn description(&self) -> &'static str {
        "Finance-style A*B*$F$1 column plus SUM rollup."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::AnchoredArithmetic,
            ScenarioTag::SpanPromotable,
            ScenarioTag::SingleCellEdit,
            ScenarioTag::BulkEdit,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row0 in 0..rows {
                let row = row0 + 1;
                let unit = row as f64;
                let price = 10.0 + (row0 % 17) as f64;
                sheet.get_cell_mut((1, row)).set_value_number(unit);
                sheet.get_cell_mut((2, row)).set_value_number(price);
                sheet
                    .get_cell_mut((3, row))
                    .set_formula(format!("=A{row}*B{row}*$F$1"));
            }
            sheet.get_cell_mut((6, 1)).set_value_number(1.0);
            sheet
                .get_cell_mut((7, 1))
                .set_formula(format!("=SUM(C1:C{rows})"));
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 7,
                sheets: 1,
                formula_cells: rows + 1,
                value_cells: rows.saturating_mul(2) + 1,
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
            invariants.reserve(rows as usize + 1);
            for row in 1..=rows {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 3,
                    expected: numeric(line_value(row, rows, cycles)),
                });
            }
            invariants.push(ScenarioInvariant::CellEquals {
                sheet: "Sheet1".to_string(),
                row: 1,
                col: 7,
                expected: numeric(expected_rollup(rows, cycles)),
            });
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    match cycle % 3 {
        0 => {
            let multiplier = 1.0 + ((cycle % 5) as f64);
            wb.set_value("Sheet1", 1, 6, LiteralValue::Number(multiplier))?;
            Ok("multiplier")
        }
        1 => {
            let rows = detect_nonempty_rows(wb, "Sheet1", 1) as usize;
            let len = DENSE_EDIT_LEN.min(rows).max(1);
            let start = (cycle * 37) % rows;
            for idx in 0..len {
                let row0 = (start + idx) % rows;
                let value = 1000.0 + cycle as f64 + idx as f64;
                wb.set_value("Sheet1", row0 as u32 + 1, 1, LiteralValue::Number(value))?;
            }
            Ok("dense_units")
        }
        _ => {
            let rows = detect_nonempty_rows(wb, "Sheet1", 2) as usize;
            let edits = SPARSE_EDITS.min(rows).max(1);
            for idx in 0..edits {
                let row0 = (cycle * 53 + idx * 97) % rows;
                let value = 20.0 + ((cycle + idx) % 23) as f64;
                wb.set_value("Sheet1", row0 as u32 + 1, 2, LiteralValue::Number(value))?;
            }
            Ok("sparse_prices")
        }
    }
}

fn line_value(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    unit_at(row, rows, completed_cycles)
        * price_at(row, rows, completed_cycles)
        * multiplier_at(completed_cycles)
}

fn expected_rollup(rows: u32, completed_cycles: usize) -> f64 {
    (1..=rows)
        .map(|row| line_value(row, rows, completed_cycles))
        .sum()
}

fn unit_at(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    let mut unit = row as f64;
    for cycle in 0..completed_cycles {
        if cycle % 3 == 1 {
            let len = DENSE_EDIT_LEN.min(rows as usize).max(1);
            let start = (cycle * 37) % rows as usize;
            for idx in 0..len {
                let row0 = (start + idx) % rows as usize;
                if row == row0 as u32 + 1 {
                    unit = 1000.0 + cycle as f64 + idx as f64;
                }
            }
        }
    }
    unit
}

fn price_at(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    let row0 = row - 1;
    let mut price = 10.0 + (row0 % 17) as f64;
    for cycle in 0..completed_cycles {
        if cycle % 3 == 2 {
            let edits = SPARSE_EDITS.min(rows as usize).max(1);
            for idx in 0..edits {
                let edit_row0 = (cycle * 53 + idx * 97) % rows as usize;
                if row0 as usize == edit_row0 {
                    price = 20.0 + ((cycle + idx) % 23) as f64;
                }
            }
        }
    }
    price
}

fn multiplier_at(completed_cycles: usize) -> f64 {
    let mut multiplier = 1.0;
    for cycle in 0..completed_cycles {
        if cycle % 3 == 0 {
            multiplier = 1.0 + ((cycle % 5) as f64);
        }
    }
    multiplier
}
