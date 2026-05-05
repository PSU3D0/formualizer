use anyhow::{Result, bail};
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const INITIAL_TABLE_ROWS: u32 = 100;

pub struct S041TableGrowByRowAppend {
    scale: ScaleState,
}

impl Default for S041TableGrowByRowAppend {
    fn default() -> Self {
        Self::new()
    }
}

impl S041TableGrowByRowAppend {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S041TableGrowByRowAppend {
    fn id(&self) -> &'static str {
        "s041-table-grow-by-row-append"
    }

    fn description(&self) -> &'static str {
        "Native Table1 with SUM(Table1[Amount]); edit plan escalates because Workbook has no public extend_table/update_table API."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::StructuredRefs, ScenarioTag::BulkEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            sheet.get_cell_mut((1, 1)).set_value("ID");
            sheet.get_cell_mut((2, 1)).set_value("Amount");
            sheet.get_cell_mut((3, 1)).set_value("Type");
            for r in 2..=INITIAL_TABLE_ROWS + 1 {
                let id = r - 1;
                sheet.get_cell_mut((1, r)).set_value_number(id as f64);
                sheet
                    .get_cell_mut((2, r))
                    .set_value_number(initial_amount_for_id(id));
                sheet.get_cell_mut((3, r)).set_value(data_type_for_id(id));
            }
            sheet
                .get_cell_mut((5, 1))
                .set_formula("=SUM(Table1[Amount])");

            let mut table = umya_spreadsheet::structs::Table::new("Table1", ("A1", "C101"));
            table.add_column(umya_spreadsheet::structs::TableColumn::new("ID"));
            table.add_column(umya_spreadsheet::structs::TableColumn::new("Amount"));
            table.add_column(umya_spreadsheet::structs::TableColumn::new("Type"));
            sheet.add_table(table);
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: INITIAL_TABLE_ROWS + 51,
                cols: 5,
                sheets: 1,
                formula_cells: 1,
                value_cells: 3 + INITIAL_TABLE_ROWS.saturating_mul(3),
                has_named_ranges: false,
                has_tables: true,
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
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            invariants.push(ScenarioInvariant::CellEquals {
                sheet: "Sheet1".to_string(),
                row: 1,
                col: 5,
                expected: numeric(initial_total_amount()),
            });
        }
        invariants
    }
}

fn apply_edit(_wb: &mut Workbook, _cycle: usize) -> Result<&'static str, anyhow::Error> {
    bail!(
        "s041 requires extending native table metadata, but Workbook exposes no extend_table/update_table API; Engine has define_table only and graph update_table is not public through Workbook"
    )
}

fn data_type_for_id(id: u32) -> String {
    format!("Type{}", id % 3)
}

fn initial_amount_for_id(id: u32) -> f64 {
    id as f64 * 10.0
}

fn initial_total_amount() -> f64 {
    (1..=INITIAL_TABLE_ROWS).map(initial_amount_for_id).sum()
}
