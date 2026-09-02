//! Issue #368 corpus scenario: many stable, converging iterative-calculation
//! SCCs (ring dependencies), plus an unrelated non-cyclic block, exercised
//! with an alternating edit plan of "unrelated edit" / "one-SCC dependency
//! edit" cycles.
//!
//! Formula shapes mirror `bin/probe-scc-reuse.rs`:
//! - Column A: ring + downstream inputs.
//! - Column B: `--sccs` independent ring SCCs of `--members` cells each,
//!   `B{r} = ROUND(0.5*B{prev}+A{r},6)`, ring head reads the ring tail
//!   (wraparound). `ROUND` makes the fixed point exact so a converged SCC
//!   is bit-stable under `CyclePolicy::Iterate`.
//! - Downstream families: `C{r}=B{r}*2+A{r}`, `D{r}=C{r}+1`, `E{r}=D{r}*A{r}`.
//! - Unrelated block: `G` input values, `H{r}=G{r}*3+1`, `I{r}=H{r}+G{r}`.
//!
//! The scenario runs under `CycleConfig::iterate_excel_defaults()` via
//! `Scenario::eval_config`; with the default `reuse_converged_sccs` every
//! ring is retained after the initial evaluation, so an unrelated edit
//! re-evaluates two cells and a dependency edit re-runs exactly one ring.

use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_eval::engine::{CycleConfig, EvalConfig};
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

/// Column layout (1-indexed), matching `bin/probe-scc-reuse.rs`.
const COL_A: u32 = 1;
const COL_B: u32 = 2;
const COL_C: u32 = 3;
const COL_D: u32 = 4;
const COL_E: u32 = 5;
const COL_G: u32 = 7;
const COL_H: u32 = 8;
const COL_I: u32 = 9;

const EDIT_CYCLES: usize = 6;

pub struct S087StableSccUnrelatedEdit {
    scale: ScaleState,
}

impl Default for S087StableSccUnrelatedEdit {
    fn default() -> Self {
        Self::new()
    }
}

impl S087StableSccUnrelatedEdit {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }

    /// (ring_count, members_per_ring) per scale. Row count is their product.
    pub fn rings_and_members(scale: ScenarioScale) -> (u32, u32) {
        match scale {
            ScenarioScale::Small => (10, 24),
            ScenarioScale::Medium => (78, 120),
            ScenarioScale::Large => (400, 125),
        }
    }

    pub fn rows(scale: ScenarioScale) -> u32 {
        let (sccs, members) = Self::rings_and_members(scale);
        sccs * members
    }
}

impl Scenario for S087StableSccUnrelatedEdit {
    fn id(&self) -> &'static str {
        "s087-stable-scc-unrelated-edit"
    }

    fn description(&self) -> &'static str {
        "Many stable convergent iterative-calculation ring SCCs with downstream families, plus an unrelated block, edited via alternating unrelated/one-SCC-dependency cycles (issue #368)."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::InternalDependency,
            ScenarioTag::SingleCellEdit,
            ScenarioTag::Mixed,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let (sccs, members) = Self::rings_and_members(ctx.scale);
        let rows = sccs * members;
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for r in 1..=rows {
                sheet.get_cell_mut((COL_A, r)).set_value_number(input_a(r));
                sheet.get_cell_mut((COL_G, r)).set_value_number(input_g(r));
            }
            for k in 0..sccs {
                let base = k * members + 1;
                let tail = base + members - 1;
                for r in base..=tail {
                    let prev = if r == base { tail } else { r - 1 };
                    sheet
                        .get_cell_mut((COL_B, r))
                        .set_formula(format!("=ROUND(0.5*B{prev}+A{r},6)"));
                }
            }
            for r in 1..=rows {
                sheet
                    .get_cell_mut((COL_C, r))
                    .set_formula(format!("=B{r}*2+A{r}"));
                sheet
                    .get_cell_mut((COL_D, r))
                    .set_formula(format!("=C{r}+1"));
                sheet
                    .get_cell_mut((COL_E, r))
                    .set_formula(format!("=D{r}*A{r}"));
                sheet
                    .get_cell_mut((COL_H, r))
                    .set_formula(format!("=G{r}*3+1"));
                sheet
                    .get_cell_mut((COL_I, r))
                    .set_formula(format!("=H{r}+G{r}"));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: COL_I,
                sheets: 1,
                formula_cells: rows * 6,
                value_cells: rows * 2,
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }

    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: EDIT_CYCLES,
            apply: apply_edit,
        })
    }

    fn eval_config(&self, base: EvalConfig) -> EvalConfig {
        base.with_cycle(CycleConfig::iterate_excel_defaults())
    }

    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        let scale = self.scale.get_or_small();
        let rows = Self::rows(scale);
        let cycles = completed_cycles(phase);
        let mut invariants = Vec::with_capacity(rows as usize * 3 + 1);
        if has_evaluated_formulas(phase) {
            // Rings converge under iterative calculation: no `#CIRC!`.
            invariants.push(ScenarioInvariant::NoErrorCells {
                sheet: "Sheet1".to_string(),
            });
        }
        for row in 1..=rows {
            let g = g_value(row, rows, cycles);
            // `G` is a plain input value, present from the moment the
            // fixture is loaded (unlike `H`/`I`, which are formulas and
            // only hold a value once `evaluate_all` has run).
            invariants.push(ScenarioInvariant::CellEquals {
                sheet: "Sheet1".to_string(),
                row,
                col: COL_G,
                expected: numeric(g),
            });
            if has_evaluated_formulas(phase) {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: COL_H,
                    expected: numeric(g * 3.0 + 1.0),
                });
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: COL_I,
                    expected: numeric((g * 3.0 + 1.0) + g),
                });
            }
        }
        invariants
    }
}

/// Even cycles: unrelated edit (write a `G` input, feeding only `H`/`I`).
/// Odd cycles: one-SCC dependency edit (write `A1`, ring 0's head, feeding
/// exactly one ring SCC + its downstream row).
fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    let rows = detect_nonempty_rows(wb, "Sheet1", COL_A).max(1);
    if cycle.is_multiple_of(2) {
        let row = unrelated_edit_row(cycle, rows);
        wb.set_value(
            "Sheet1",
            row,
            COL_G,
            LiteralValue::Number(unrelated_edit_value(cycle)),
        )?;
        Ok("unrelated_edit")
    } else {
        wb.set_value(
            "Sheet1",
            1,
            COL_A,
            LiteralValue::Number(dependency_edit_value(cycle)),
        )?;
        Ok("one_scc_dependency_edit")
    }
}

fn input_a(row: u32) -> f64 {
    1.0 + (row % 23) as f64
}

fn input_g(row: u32) -> f64 {
    2.0 + (row % 31) as f64
}

fn unrelated_edit_row(cycle: usize, rows: u32) -> u32 {
    ((cycle as u32).wrapping_mul(53) % rows) + 1
}

fn unrelated_edit_value(cycle: usize) -> f64 {
    500.0 + cycle as f64 * 7.0
}

fn dependency_edit_value(cycle: usize) -> f64 {
    -100.0 - cycle as f64
}

/// Expected value of `G{row}` after `completed_cycles` edit cycles, given
/// the fixture's total row count. Only even (unrelated-edit) cycles touch
/// `G`; odd (dependency-edit) cycles only touch `A1` and are irrelevant here.
fn g_value(row: u32, rows: u32, completed_cycles: usize) -> f64 {
    let mut value = input_g(row);
    for cycle in (0..completed_cycles).filter(|c| c.is_multiple_of(2)) {
        if unrelated_edit_row(cycle, rows) == row {
            value = unrelated_edit_value(cycle);
        }
    }
    value
}
