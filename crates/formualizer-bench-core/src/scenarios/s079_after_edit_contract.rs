use anyhow::{Context, Result};
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{fixture_path, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const ROWS: u32 = 20;
const SAMPLE_ROW: u32 = 8;
const INSERT_ROWS: u32 = 2;

pub struct S079AfterEditContract;

impl Default for S079AfterEditContract {
    fn default() -> Self {
        Self::new()
    }
}

impl S079AfterEditContract {
    pub fn new() -> Self {
        Self
    }
}

impl Scenario for S079AfterEditContract {
    fn id(&self) -> &'static str {
        "s079-after-edit-contract"
    }

    fn description(&self) -> &'static str {
        "Contract validation: structural edits clear affected computed values until recalc."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::InsertRows,
            ScenarioTag::SingleColumnFamily,
            ScenarioTag::ContractValidation,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                sheet.get_cell_mut((1, row)).set_value_number(row as f64);
                sheet
                    .get_cell_mut((2, row))
                    .set_formula(format!("=A{row}*2"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS + INSERT_ROWS,
                cols: 2,
                sheets: 1,
                formula_cells: ROWS,
                value_cells: ROWS,
                has_named_ranges: false,
                has_tables: false,
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
        match phase {
            ScenarioPhase::AfterFirstEval => {
                invariants.push(cell_equals(SAMPLE_ROW, 2, numeric((SAMPLE_ROW * 2) as f64)));
            }
            ScenarioPhase::AfterEdit { .. } => {
                invariants.push(cell_equals(SAMPLE_ROW, 2, LiteralValue::Empty));
                invariants.push(cell_equals(
                    SAMPLE_ROW + INSERT_ROWS,
                    2,
                    LiteralValue::Empty,
                ));
            }
            ScenarioPhase::AfterRecalc { .. } => {
                invariants.push(cell_equals(
                    SAMPLE_ROW + INSERT_ROWS,
                    2,
                    numeric((SAMPLE_ROW * 2) as f64),
                ));
            }
            ScenarioPhase::AfterLoad => {}
        }
        invariants
    }
}

fn cell_equals(row: u32, col: u32, expected: LiteralValue) -> ScenarioInvariant {
    ScenarioInvariant::CellEquals {
        sheet: "Sheet1".to_string(),
        row,
        col,
        expected,
    }
}

fn apply_edit(wb: &mut Workbook, _cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.engine_mut()
        .insert_rows("Sheet1", SAMPLE_ROW, INSERT_ROWS)
        .with_context(|| {
            format!("engine insert_rows Sheet1 before={SAMPLE_ROW} count={INSERT_ROWS}")
        })?;
    Ok("insert_rows_contract")
}
