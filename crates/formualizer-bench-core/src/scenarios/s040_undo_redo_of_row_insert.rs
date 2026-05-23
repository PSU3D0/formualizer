use anyhow::{Result, bail};
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, fixture_path};
use super::{
    EditPlan, ExpectedFailure, ExpectedFailureMode, FixtureMetadata, Scenario, ScenarioBuildCtx,
    ScenarioFixture, ScenarioInvariant, ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct S040UndoRedoOfRowInsert {
    scale: ScaleState,
}

impl Default for S040UndoRedoOfRowInsert {
    fn default() -> Self {
        Self::new()
    }
}

impl S040UndoRedoOfRowInsert {
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

impl Scenario for S040UndoRedoOfRowInsert {
    fn id(&self) -> &'static str {
        "s040-undo-redo-of-row-insert"
    }

    fn description(&self) -> &'static str {
        "Planned row insert undo/redo scenario; currently escalates because Workbook exposes undo/redo but no undoable row-insert action API."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::SingleColumnFamily,
            ScenarioTag::UndoRedo,
            ScenarioTag::InsertRows,
        ]
    }

    fn expected_to_fail_under(&self) -> &'static [ExpectedFailure] {
        &[
            ExpectedFailure {
                mode: ExpectedFailureMode::OffOnly,
                reason: "Workbook public API has no undoable insert_rows; engine_mut().insert_rows would bypass the Workbook undo/redo machinery this scenario tries to test. PM follow-up: add Workbook surface for structural ops.",
            },
            ExpectedFailure {
                mode: ExpectedFailureMode::AuthOnly,
                reason: "Workbook public API has no undoable insert_rows; engine_mut().insert_rows would bypass the Workbook undo/redo machinery this scenario tries to test. PM follow-up: add Workbook surface for structural ops.",
            },
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet.get_cell_mut((1, r)).set_value_number(r as f64);
                sheet.get_cell_mut((2, r)).set_formula(format!("=A{r}*2"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: rows + 20,
                cols: 2,
                sheets: 1,
                formula_cells: rows,
                value_cells: rows,
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

    fn invariants(&self, _phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }]
    }
}

fn apply_edit(_wb: &mut Workbook, _cycle: usize) -> Result<&'static str, anyhow::Error> {
    bail!(
        "s040 requires undoable row inserts, but Workbook exposes only undo/redo and Engine::insert_rows via engine_mut(); WorkbookAction has no insert_rows method and using engine_mut() would not exercise Workbook undo/redo"
    )
}
