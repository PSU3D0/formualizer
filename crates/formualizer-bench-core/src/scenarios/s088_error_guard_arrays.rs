//! Original error-guard workloads. The oracle is host arithmetic, not engine
//! output or cached XLSX values. Every spill member (including edited members)
//! is checked after first calculation and each recalculation.
use anyhow::Result;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::EvalConfig;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioScale, ScenarioTag,
};

pub struct ErrorGuardArrays {
    /// 0 = clean, 100 = sparse (1%), 2 = dense (50%).
    period: u32,
    scale: ScaleState,
}

impl ErrorGuardArrays {
    pub fn new(period: u32) -> Self {
        assert!(matches!(period, 0 | 100 | 2));
        Self {
            period,
            scale: ScaleState::new(),
        }
    }

    pub fn rows(scale: ScenarioScale) -> u32 {
        match scale {
            ScenarioScale::Small => 256,
            ScenarioScale::Medium => 100_000,
            ScenarioScale::Large => 1_000_000,
        }
    }

    fn oracle(&self, rows: u32, cycles: usize) -> Vec<ScenarioInvariant> {
        let mut out = Vec::new();
        let mut clean_sum = 0.0;
        let mut reciprocal_sum = 0.0;
        let mut fallback_sum = 0.0;
        let mut na_sum = 0.0;
        let mut has_div = false;
        for row in 1..=rows {
            let (a, b, d) = inputs(row, self.period, cycles);
            let reciprocal = if b == 0.0 { 0.0 } else { 1.0 / b };
            let paired = if b == 0.0 { d } else { 1.0 / b };
            let guarded = if b == 0.0 { 0.0 } else { a };
            // IFNA must preserve DIV/0, not silently turn it into zero.
            let na = if b != 0.0 {
                numeric(a)
            } else if row % 4 == 0 {
                numeric(0.0)
            } else {
                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div))
            };
            for (col, expected) in [
                (5, numeric(a)),
                (6, numeric(a)),
                (7, numeric(reciprocal)),
                (8, numeric(paired)),
                (9, na),
                (10, numeric(guarded)),
            ] {
                out.push(cell(row, col, expected));
            }
            clean_sum += a;
            reciprocal_sum += reciprocal;
            fallback_sum += paired;
            na_sum += guarded;
            has_div |= b == 0.0 && row % 4 != 0;
        }
        for (row, value) in [(1, clean_sum), (2, reciprocal_sum), (3, fallback_sum)] {
            out.push(cell(row, 11, numeric(value)));
        }
        out.push(cell(
            4,
            11,
            if has_div {
                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div))
            } else {
                numeric(na_sum)
            },
        ));
        out
    }
}

fn cell(row: u32, col: u32, expected: LiteralValue) -> ScenarioInvariant {
    ScenarioInvariant::CellEquals {
        sheet: "Sheet1".into(),
        row,
        col,
        expected,
    }
}

impl Scenario for ErrorGuardArrays {
    fn id(&self) -> &'static str {
        match self.period {
            0 => "s088-error-guards-clean",
            100 => "s089-error-guards-sparse",
            _ => "s090-error-guards-dense",
        }
    }
    fn description(&self) -> &'static str {
        "Lazy clean scalar/range guards, masked reciprocal reductions, paired fallback and IFNA distinctions."
    }
    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::ShortCircuit,
            ScenarioTag::AggregationHeavy,
            ScenarioTag::ErrorPropagation,
            ScenarioTag::SingleCellEdit,
        ]
    }
    fn eval_config(&self, mut base: EvalConfig) -> EvalConfig {
        base.spill.max_spill_cells = 1_000_000;
        base
    }
    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let rows = Self::rows(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sh = book.get_sheet_by_name_mut("Sheet1").unwrap();
            for row in 1..=rows {
                let (a, b, d) = inputs(row, self.period, 0);
                for (col, value) in [(1, a), (2, b), (4, d)] {
                    sh.get_cell_mut((col, row)).set_value_number(value);
                }
                let error = if row % 4 == 0 { "NA()" } else { "1/0" };
                sh.get_cell_mut((3, row))
                    .set_formula(format!("=IF(B{row}=0,{error},A{row})"));
                let scalar_guard = if row % 2 == 0 { "IFERROR" } else { "IFNA" };
                sh.get_cell_mut((5, row))
                    .set_formula(format!("={scalar_guard}(A{row},1/0)"));
            }
            for (col, formula) in [
                (6, format!("=IFERROR(A1:A{rows},1/0)")),
                (7, format!("=IFERROR(1/B1:B{rows},0)")),
                (8, format!("=IFERROR(1/B1:B{rows},D1:D{rows})")),
                (9, format!("=IFNA(C1:C{rows},0)")),
                (10, format!("=IFERROR(C1:C{rows},0)")),
            ] {
                sh.get_cell_mut((col, 1)).set_formula(formula);
            }
            for (row, formula) in [
                (1, format!("=SUM(IFERROR(A1:A{rows},1/0))")),
                (2, format!("=SUM(IFERROR(1/B1:B{rows},0))")),
                (3, format!("=SUM(IFERROR(1/B1:B{rows},D1:D{rows}))")),
                (4, format!("=SUM(IFNA(C1:C{rows},0))")),
            ] {
                sh.get_cell_mut((11, row)).set_formula(formula);
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows,
                cols: 11,
                sheets: 1,
                formula_cells: 2 * rows + 9,
                value_cells: 3 * rows,
                has_named_ranges: false,
                has_tables: false,
            },
        })
    }
    fn edit_plan(&self) -> Option<EditPlan> {
        Some(EditPlan {
            cycles: 5,
            apply: if self.period == 0 {
                edit_clean
            } else {
                edit_mixed
            },
        })
    }
    fn invariants(&self, phase: ScenarioPhase) -> Vec<ScenarioInvariant> {
        if !has_evaluated_formulas(phase) {
            return vec![];
        }
        self.oracle(
            Self::rows(self.scale.get_or_small()),
            completed_cycles(phase),
        )
    }
}

// Fixed rows are deliberately inside the small fixture and include both kinds
// of errors. Every edit changes an observable guard member and a reduction.
fn edit(cycle: usize, clean: bool) -> (u32, u32, f64) {
    match cycle {
        0 => (100, 2, 8.0),
        1 => (100, 2, 4.0),
        2 => (2, 2, if clean { 8.0 } else { 0.0 }),
        3 => (2, if clean { 1 } else { 4 }, 32.0),
        _ => (100, 1, 64.0),
    }
}
fn inputs(row: u32, period: u32, cycles: usize) -> (f64, f64, f64) {
    let mut a = (row % 16 + 1) as f64;
    let mut b = if period != 0 && row.is_multiple_of(period) {
        0.0
    } else {
        4.0
    };
    let mut d = (row % 8 + 10) as f64;
    for cycle in 0..cycles {
        let (r, col, value) = edit(cycle, period == 0);
        if r == row {
            match col {
                1 => a = value,
                2 => b = value,
                _ => d = value,
            }
        }
    }
    (a, b, d)
}
fn apply(wb: &mut Workbook, cycle: usize, clean: bool) -> Result<&'static str> {
    let (row, col, value) = edit(cycle, clean);
    wb.set_value("Sheet1", row, col, numeric(value))?;
    Ok("guard_input")
}
fn edit_clean(wb: &mut Workbook, cycle: usize) -> Result<&'static str> {
    apply(wb, cycle, true)
}
fn edit_mixed(wb: &mut Workbook, cycle: usize) -> Result<&'static str> {
    apply(wb, cycle, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn error_guard_generator_oracle() {
        let tmp = tempfile::tempdir().unwrap();
        for period in [0, 100, 2] {
            let scenario = ErrorGuardArrays::new(period);
            let fixture = scenario
                .build_fixture(&ScenarioBuildCtx {
                    scale: ScenarioScale::Small,
                    fixture_dir: tmp.path().into(),
                    label: "oracle".into(),
                })
                .unwrap();
            assert!(fixture.path.exists());
            for cycles in 0..=5 {
                let oracle = scenario.oracle(256, cycles);
                assert_eq!(oracle.len(), 6 * 256 + 4);
                let value = |r, c| {
                    oracle
                        .iter()
                        .find_map(|inv| match inv {
                            ScenarioInvariant::CellEquals {
                                row, col, expected, ..
                            } if *row == r && *col == c => Some(expected.clone()),
                            _ => None,
                        })
                        .unwrap()
                };
                if cycles == 0 && period == 0 {
                    assert_eq!(value(4, 11), numeric(2176.0));
                    assert_eq!(value(2, 11), numeric(64.0));
                }
                if cycles == 0 && period == 100 {
                    assert_eq!(value(4, 11), numeric(2162.0));
                    assert_eq!(value(2, 11), numeric(63.5));
                }
                if period == 2 || period == 100 && cycles >= 3 {
                    assert_eq!(
                        value(4, 11),
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div))
                    );
                }
                // Corrected sparse/dense row, not just unaffected sentinels.
                if period != 0 && cycles == 0 {
                    assert_eq!(value(100, 7), numeric(0.0));
                    assert_eq!(value(100, 8), numeric(14.0));
                    assert_eq!(value(100, 9), numeric(0.0));
                }
                if period == 2 && cycles == 0 || period != 0 && cycles >= 3 {
                    assert_eq!(
                        value(2, 9),
                        LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div))
                    );
                }
                if period != 0 && cycles == 4 {
                    assert_eq!(value(2, 8), numeric(32.0));
                }
                if cycles >= 2 {
                    assert_eq!(value(100, 7), numeric(0.25));
                }
                if cycles == 5 {
                    assert_eq!(value(100, 6), numeric(64.0));
                }
            }
        }
    }
}
