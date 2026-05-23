use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

const BULK_EDIT_LEN: usize = 50;

pub struct S039UndoRedoOfBulkEdit {
    scale: ScaleState,
}

impl Default for S039UndoRedoOfBulkEdit {
    fn default() -> Self {
        Self::new()
    }
}

impl S039UndoRedoOfBulkEdit {
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

impl Scenario for S039UndoRedoOfBulkEdit {
    fn id(&self) -> &'static str {
        "s039-undo-redo-of-bulk-edit"
    }

    fn description(&self) -> &'static str {
        "Single-column =A*2 family where two grouped 50-cell bulk edits are undone and redone."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::SingleColumnFamily,
            ScenarioTag::BulkEdit,
            ScenarioTag::UndoRedo,
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
                rows,
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

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let rows = Self::rows(self.scale.get_or_small());
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let state = state_after_phase(phase);
            for row in 1..=rows {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(input_value(row, rows, state) * 2.0),
                });
            }
        }
        invariants
    }
}

#[derive(Clone, Copy)]
enum BulkState {
    Initial,
    First,
    FirstAndSecond,
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let rows = wb
        .sheet_dimensions("Sheet1")
        .map(|(rows, _)| rows)
        .unwrap_or(1)
        .max(1);
    match cycle {
        0 => {
            apply_bulk_action(wb, 0, rows)?;
            Ok("bulk_edit_first")
        }
        1 => {
            wb.undo()?;
            Ok("undo_first_bulk")
        }
        2 => {
            wb.redo()?;
            Ok("redo_first_bulk")
        }
        3 => {
            apply_bulk_action(wb, 1, rows)?;
            Ok("bulk_edit_second")
        }
        4 => {
            wb.undo()?;
            Ok("undo_second_bulk")
        }
        _ => Ok("noop"),
    }
}

fn apply_bulk_action(wb: &mut Workbook, edit_set: usize, rows: u32) -> Result<()> {
    wb.action(&format!("bulk edit {edit_set}"), |action| {
        for idx in 0..BULK_EDIT_LEN.min(rows as usize).max(1) {
            action.set_value(
                "Sheet1",
                edit_row(edit_set, idx, rows),
                1,
                LiteralValue::Number(edit_value(edit_set, idx)),
            )?;
        }
        Ok(())
    })?;
    Ok(())
}

fn state_after_phase(phase: ScenarioPhase) -> BulkState {
    match phase {
        ScenarioPhase::AfterRecalc { cycle: 0, .. } => BulkState::First,
        ScenarioPhase::AfterRecalc { cycle: 1, .. } => BulkState::Initial,
        ScenarioPhase::AfterRecalc { cycle: 2, .. } => BulkState::First,
        ScenarioPhase::AfterRecalc { cycle: 3, .. } => BulkState::FirstAndSecond,
        ScenarioPhase::AfterRecalc { cycle: 4, .. } => BulkState::First,
        _ => BulkState::Initial,
    }
}

fn edit_row(edit_set: usize, idx: usize, rows: u32) -> u32 {
    ((edit_set * 503 + idx * 17) % rows as usize) as u32 + 1
}

fn edit_value(edit_set: usize, idx: usize) -> f64 {
    1_000.0 + edit_set as f64 * 1_000.0 + idx as f64
}

fn input_value(row: u32, rows: u32, state: BulkState) -> f64 {
    let mut value = row as f64;
    if matches!(state, BulkState::First | BulkState::FirstAndSecond) {
        for idx in 0..BULK_EDIT_LEN.min(rows as usize).max(1) {
            if row == edit_row(0, idx, rows) {
                value = edit_value(0, idx);
            }
        }
    }
    if matches!(state, BulkState::FirstAndSecond) {
        for idx in 0..BULK_EDIT_LEN.min(rows as usize).max(1) {
            if row == edit_row(1, idx, rows) {
                value = edit_value(1, idx);
            }
        }
    }
    value
}
