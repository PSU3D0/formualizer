use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const ROWS: u32 = 100;
const EDIT_ROW: u32 = 51;

pub struct S060SelfReferencingTableRow {
    scale: ScaleState,
}

impl Default for S060SelfReferencingTableRow {
    fn default() -> Self {
        Self::new()
    }
}

impl S060SelfReferencingTableRow {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S060SelfReferencingTableRow {
    fn id(&self) -> &'static str {
        "s060-self-referencing-table-row"
    }

    fn description(&self) -> &'static str {
        "Excel table calculated column uses the prior column in the same row."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::StructuredRefs]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            sheet.get_cell_mut((1, 1)).set_value("PriorCol");
            sheet.get_cell_mut((2, 1)).set_value("Result");
            for row in 2..=ROWS + 1 {
                let data_row = row - 1;
                sheet
                    .get_cell_mut((1, row))
                    .set_value_number(data_value(data_row, 0));
                sheet.get_cell_mut((2, row)).set_formula("=[@PriorCol]*2");
            }
            let mut table = umya_spreadsheet::structs::Table::new("TableSelf", ("A1", "B101"));
            table.add_column(umya_spreadsheet::structs::TableColumn::new("PriorCol"));
            table.add_column(umya_spreadsheet::structs::TableColumn::new("Result"));
            sheet.add_table(table);
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS + 1,
                cols: 2,
                sheets: 1,
                formula_cells: ROWS,
                value_cells: ROWS + 2,
                has_named_ranges: false,
                has_tables: true,
            },
        })
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 1,
            apply: apply_edit,
        })
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            for data_row in [1, 50, 100] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row: data_row + 1,
                    col: 2,
                    expected: numeric(data_value(data_row, cycles) * 2.0),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, _cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value("Sheet1", EDIT_ROW + 1, 1, LiteralValue::Number(1_000.0))?;
    Ok("table_prior_col")
}

fn data_value(data_row: u32, cycles: usize) -> f64 {
    if data_row == EDIT_ROW && cycles > 0 {
        1_000.0
    } else {
        data_row as f64
    }
}
