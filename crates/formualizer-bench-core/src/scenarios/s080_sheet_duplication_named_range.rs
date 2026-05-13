use anyhow::Result;
use formualizer_common::RangeAddress;
use formualizer_testkit::write_workbook;
use formualizer_workbook::{Workbook, traits::NamedRangeScope};

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const ROWS: u32 = 1_000;
const INITIAL_RANGE_END: u32 = 10;
const UPDATED_RANGE_END: u32 = 20;

pub struct S080SheetDuplicationNamedRange {
    scale: ScaleState,
}

impl Default for S080SheetDuplicationNamedRange {
    fn default() -> Self {
        Self::new()
    }
}

impl S080SheetDuplicationNamedRange {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    pub fn rows(_scale: ScenarioScale) -> u32 {
        ROWS
    }
}

impl Scenario for S080SheetDuplicationNamedRange {
    fn id(&self) -> &'static str {
        "s080-sheet-duplication-named-range"
    }

    fn description(&self) -> &'static str {
        "Sheet duplication preserves sheet-scoped named range dependents across named range updates."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::NamedRanges, ScenarioTag::MultiSheet]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            {
                let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
                sheet.set_name("Source");
            }
            let sheet = book.get_sheet_by_name_mut("Source").expect("Source exists");
            for row in 1..=rows {
                sheet.get_cell_mut((1, row)).set_value_number(row as f64);
                sheet
                    .get_cell_mut((2, row))
                    .set_formula(format!("=SUM(MyRange)+{}", row as f64));
            }
            sheet
                .add_defined_name("MyRange", "Source!$A$1:$A$10")
                .expect("add MyRange");
            sheet
                .get_defined_names_mut()
                .last_mut()
                .expect("MyRange local name")
                .set_local_sheet_id(0);
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 2,
                sheets: 1,
                formula_cells: rows,
                value_cells: rows,
                has_named_ranges: true,
                has_tables: false,
            },
        })
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 3,
            apply: apply_edit,
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let rows = Self::rows(self.scale.get_or_small());
        let mut invariants = Vec::new();
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            let range_sum = if cycles >= 2 {
                range_sum(UPDATED_RANGE_END)
            } else {
                range_sum(INITIAL_RANGE_END)
            };
            for sheet in sheets_for_phase(cycles) {
                invariants.push(ScenarioInvariant::NoErrorCells {
                    sheet: sheet.to_string(),
                });
                for row in sample_rows(rows) {
                    invariants.push(ScenarioInvariant::CellEquals {
                        sheet: sheet.to_string(),
                        row,
                        col: 2,
                        expected: numeric(range_sum + row as f64),
                    });
                }
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    match cycle {
        0 => {
            wb.duplicate_sheet("Source", "Copy")?;
            Ok("duplicate_sheet")
        }
        1 => {
            wb.update_named_range(
                "MyRange",
                &RangeAddress::new("Source".to_string(), 1, 1, UPDATED_RANGE_END, 1)
                    .map_err(|err| anyhow::anyhow!(err))?,
                NamedRangeScope::Sheet,
            )?;
            wb.update_named_range(
                "MyRange",
                &RangeAddress::new("Copy".to_string(), 1, 1, UPDATED_RANGE_END, 1)
                    .map_err(|err| anyhow::anyhow!(err))?,
                NamedRangeScope::Sheet,
            )?;
            Ok("update_sheet_scoped_named_ranges")
        }
        _ => {
            let _ = wb.get_value("Source", 1, 2);
            let _ = wb.get_value("Copy", 1, 2);
            Ok("read_values")
        }
    }
}

fn range_sum(end: u32) -> f64 {
    (1..=end).map(|row| row as f64).sum()
}

fn sheets_for_phase(completed_cycles: usize) -> &'static [&'static str] {
    if completed_cycles >= 1 {
        &["Source", "Copy"]
    } else {
        &["Source"]
    }
}

fn sample_rows(rows: u32) -> [u32; 3] {
    [1, (rows / 2).max(1), rows]
}
