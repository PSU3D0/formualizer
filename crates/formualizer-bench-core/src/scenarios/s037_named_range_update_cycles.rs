use anyhow::Result;
use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_testkit::write_workbook;
use formualizer_workbook::{Workbook, traits::NamedRangeScope};

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, ExpectedFailure, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture,
    ScenarioInvariant, ScenarioPhase, ScenarioScale, ScenarioTag,
};

const DATA_ROWS: u32 = 1_000;

pub struct S037NamedRangeUpdateCycles {
    scale: ScaleState,
}

impl Default for S037NamedRangeUpdateCycles {
    fn default() -> Self {
        Self::new()
    }
}

impl S037NamedRangeUpdateCycles {
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

impl Scenario for S037NamedRangeUpdateCycles {
    fn id(&self) -> &'static str {
        "s037-named-range-update-cycles"
    }

    fn description(&self) -> &'static str {
        "Workbook named range redefined across five cycles and consumed by a SUM-scaled formula family."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::NamedRanges, ScenarioTag::BulkEdit]
    }

    fn expected_to_fail_under(&self) -> &'static [ExpectedFailure] {
        &[]
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
            book.new_sheet("Sheet1").expect("Sheet1 sheet");

            let data = book.get_sheet_by_name_mut("Data").expect("Data exists");
            for r in 1..=DATA_ROWS {
                data.get_cell_mut((1, r)).set_value_number(r as f64);
            }
            data.add_defined_name("DataRange", "Data!$A$1:$A$100")
                .expect("add DataRange");

            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet
                    .get_cell_mut((1, r))
                    .set_formula(format!("=SUM(DataRange) * {}", r as f64));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: rows.max(DATA_ROWS),
                cols: 1,
                sheets: 2,
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
            let sum = current_range_sum(completed_cycles(phase));
            for row in sample_rows(rows) {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 1,
                    expected: numeric(sum * row as f64),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let (start, end) = range_for_cycle(cycle + 1);
    let address = RangeAddress::new("Data".to_string(), start, 1, end, 1)
        .map_err(|err| anyhow::anyhow!(err))?;
    wb.update_named_range("DataRange", &address, NamedRangeScope::Workbook)?;
    // Include a deterministic data edit in the measured cycle so the BulkEdit tag
    // reflects a real value write pattern in addition to name metadata changes.
    if cycle == 4 {
        for row in 1..=10 {
            wb.set_value("Data", row, 1, LiteralValue::Number(row as f64))?;
        }
    }
    Ok("update_named_range")
}

fn range_for_cycle(completed_cycles: usize) -> (u32, u32) {
    match completed_cycles {
        0 => (1, 100),
        1 => (1, 200),
        2 => (50, 150),
        3 => (100, 300),
        4 => (10, 400),
        _ => (250, 500),
    }
}

fn current_range_sum(completed_cycles: usize) -> f64 {
    let (start, end) = range_for_cycle(completed_cycles);
    (start..=end).map(|row| row as f64).sum()
}

fn sample_rows(rows: u32) -> Vec<u32> {
    vec![1, (rows / 2).max(1), rows]
}
