use std::{path::PathBuf, sync::Mutex};

use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::{FixtureMetadata, ScenarioBuildCtx, ScenarioFixture, ScenarioPhase, ScenarioScale};

static GLOBAL_INVARIANT_SCALE: Mutex<Option<ScenarioScale>> = Mutex::new(None);

pub fn set_invariant_scale(scale: ScenarioScale) {
    *GLOBAL_INVARIANT_SCALE
        .lock()
        .expect("global invariant scale poisoned") = Some(scale);
}

pub fn fixture_path(ctx: &ScenarioBuildCtx, scenario_id: &str) -> PathBuf {
    ctx.fixture_dir.join(format!(
        "{}-{}-{}.xlsx",
        sanitize(&ctx.label),
        scenario_id,
        ctx.scale.as_str()
    ))
}

fn sanitize(label: &str) -> String {
    label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub fn write_standard_grid_fixture(
    ctx: &ScenarioBuildCtx,
    scenario_id: &str,
    rows: u32,
    cols: u32,
) -> ScenarioFixture {
    let path = fixture_path(ctx, scenario_id);
    write_workbook(&path, |book| {
        let sh = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
        for r in 1..=rows {
            for c in 1..=cols {
                sh.get_cell_mut((c, r))
                    .set_value_number(standard_grid_value(r, c));
            }
        }
    });
    ScenarioFixture {
        path,
        metadata: FixtureMetadata {
            rows,
            cols,
            sheets: 1,
            formula_cells: 0,
            value_cells: rows.saturating_mul(cols),
            has_named_ranges: false,
            has_tables: false,
        },
    }
}

pub fn standard_grid_value(row: u32, col: u32) -> f64 {
    (row as f64 * 0.001) + col as f64
}

pub fn numeric(value: f64) -> LiteralValue {
    LiteralValue::Number(value)
}

pub fn has_evaluated_formulas(phase: ScenarioPhase) -> bool {
    matches!(
        phase,
        ScenarioPhase::AfterFirstEval | ScenarioPhase::AfterRecalc { .. }
    )
}

#[derive(Debug)]
pub struct ScaleState {
    scale: Mutex<Option<ScenarioScale>>,
}

impl Default for ScaleState {
    fn default() -> Self {
        Self::new()
    }
}

impl ScaleState {
    pub fn new() -> Self {
        Self {
            scale: Mutex::new(None),
        }
    }

    pub fn set(&self, scale: ScenarioScale) {
        *self.scale.lock().expect("scale state poisoned") = Some(scale);
    }

    pub fn get_or_small(&self) -> ScenarioScale {
        self.scale
            .lock()
            .expect("scale state poisoned")
            .or_else(|| {
                *GLOBAL_INVARIANT_SCALE
                    .lock()
                    .expect("global invariant scale poisoned")
            })
            .unwrap_or(ScenarioScale::Small)
    }
}

pub fn maybe_edited_s002_a(row: u32, phase: ScenarioPhase) -> f64 {
    let mut value = row as f64;
    let completed_cycles = completed_cycles(phase);
    for cycle in 0..completed_cycles {
        let edit_row = (cycle * 37) as u32 + 1;
        if row == edit_row {
            value = 1000.0 + cycle as f64;
        }
    }
    value
}

pub fn completed_cycles(phase: ScenarioPhase) -> usize {
    match phase {
        ScenarioPhase::AfterLoad | ScenarioPhase::AfterFirstEval => 0,
        ScenarioPhase::AfterEdit { cycle, .. } | ScenarioPhase::AfterRecalc { cycle, .. } => {
            cycle + 1
        }
    }
}

pub fn sample_rows(rows: u32) -> [u32; 3] {
    [1, (rows / 2).max(1), rows.max(1)]
}

pub fn col_name(col: u32) -> String {
    let mut n = col;
    let mut chars = Vec::new();
    while n > 0 {
        n -= 1;
        chars.push((b'A' + (n % 26) as u8) as char);
        n /= 26;
    }
    chars.iter().rev().collect()
}

pub fn detect_nonempty_rows(wb: &Workbook, sheet: &str, col: u32) -> u32 {
    const MAX_EXCEL_ROW: u32 = 1_048_576;
    let mut high = 1u32;
    while high < MAX_EXCEL_ROW && wb.get_value(sheet, high, col).is_some() {
        high = high.saturating_mul(2).min(MAX_EXCEL_ROW);
    }
    if high == MAX_EXCEL_ROW && wb.get_value(sheet, high, col).is_some() {
        return MAX_EXCEL_ROW;
    }
    let mut low = high / 2;
    let mut upper = high;
    while low + 1 < upper {
        let mid = low + (upper - low) / 2;
        if wb.get_value(sheet, mid, col).is_some() {
            low = mid;
        } else {
            upper = mid;
        }
    }
    low.max(1)
}
