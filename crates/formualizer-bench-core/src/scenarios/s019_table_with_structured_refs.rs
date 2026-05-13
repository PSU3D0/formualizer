use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const TABLE_ROWS: u32 = 1_000;

pub struct S019TableWithStructuredRefs {
    scale: ScaleState,
}

impl Default for S019TableWithStructuredRefs {
    fn default() -> Self {
        Self::new()
    }
}

impl S019TableWithStructuredRefs {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S019TableWithStructuredRefs {
    fn id(&self) -> &'static str {
        "s019-table-with-structured-refs"
    }

    fn description(&self) -> &'static str {
        "Native Excel table with SUM/COUNTIF/SUMIFS structured references."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[ScenarioTag::StructuredRefs, ScenarioTag::SingleCellEdit]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            sheet.get_cell_mut((1, 1)).set_value("ID");
            sheet.get_cell_mut((2, 1)).set_value("Amount");
            sheet.get_cell_mut((3, 1)).set_value("Type");
            for r in 2..=TABLE_ROWS + 1 {
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
            sheet
                .get_cell_mut((5, 2))
                .set_formula("=COUNTIF(Table1[Type], \"Type0\")");
            sheet
                .get_cell_mut((5, 3))
                .set_formula("=SUMIFS(Table1[Amount], Table1[Type], \"Type1\")");

            let mut table = umya_spreadsheet::structs::Table::new("Table1", ("A1", "C1001"));
            table.add_column(umya_spreadsheet::structs::TableColumn::new("ID"));
            table.add_column(umya_spreadsheet::structs::TableColumn::new("Amount"));
            table.add_column(umya_spreadsheet::structs::TableColumn::new("Type"));
            sheet.add_table(table);
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: TABLE_ROWS + 1,
                cols: 5,
                sheets: 1,
                formula_cells: 3,
                value_cells: 3 + TABLE_ROWS.saturating_mul(3),
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
            let cycles = completed_cycles(phase);
            invariants.push(ScenarioInvariant::CellEquals {
                sheet: "Sheet1".to_string(),
                row: 1,
                col: 5,
                expected: numeric(total_amount(cycles)),
            });
            invariants.push(ScenarioInvariant::CellEquals {
                sheet: "Sheet1".to_string(),
                row: 2,
                col: 5,
                expected: numeric(type0_count()),
            });
            invariants.push(ScenarioInvariant::CellEquals {
                sheet: "Sheet1".to_string(),
                row: 3,
                col: 5,
                expected: numeric(sum_for_type("Type1", cycles)),
            });
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let row = ((cycle * 37) % TABLE_ROWS as usize) as u32 + 2;
    wb.set_value("Sheet1", row, 2, LiteralValue::Number(edited_amount(cycle)))?;
    Ok("table_amount")
}

fn data_type_for_id(id: u32) -> String {
    format!("Type{}", id % 3)
}

fn initial_amount_for_id(id: u32) -> f64 {
    id as f64 * 10.0
}

fn edited_amount(cycle: usize) -> f64 {
    10_000.0 + cycle as f64
}

fn edited_id(cycle: usize) -> u32 {
    ((cycle * 37) % TABLE_ROWS as usize) as u32 + 1
}

fn amount_for_id(id: u32, completed_cycles: usize) -> f64 {
    let mut amount = initial_amount_for_id(id);
    for cycle in 0..completed_cycles {
        if id == edited_id(cycle) {
            amount = edited_amount(cycle);
        }
    }
    amount
}

fn total_amount(completed_cycles: usize) -> f64 {
    (1..=TABLE_ROWS)
        .map(|id| amount_for_id(id, completed_cycles))
        .sum()
}

fn type0_count() -> f64 {
    (1..=TABLE_ROWS)
        .filter(|id| data_type_for_id(*id) == "Type0")
        .count() as f64
}

fn sum_for_type(criteria: &str, completed_cycles: usize) -> f64 {
    (1..=TABLE_ROWS)
        .filter(|id| data_type_for_id(*id) == criteria)
        .map(|id| amount_for_id(id, completed_cycles))
        .sum()
}
