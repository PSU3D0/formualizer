use anyhow::Result;
use formualizer_common::LiteralValue;
use formualizer_testkit::write_workbook;
use formualizer_workbook::Workbook;

use super::common::{ScaleState, completed_cycles, fixture_path, has_evaluated_formulas, numeric};
use super::{
    EditPlan, FixtureMetadata, Scenario, ScenarioBuildCtx, ScenarioFixture, ScenarioInvariant,
    ScenarioPhase, ScenarioTag,
};

const ROWS: u32 = 2_000;
const FORMULA_ROWS: u32 = 100;

pub struct S046GiantAstFormula200Deps {
    scale: ScaleState,
}

impl Default for S046GiantAstFormula200Deps {
    fn default() -> Self {
        Self::new()
    }
}

impl S046GiantAstFormula200Deps {
    pub fn new() -> Self {
        Self {
            scale: ScaleState::new(),
        }
    }
}

impl Scenario for S046GiantAstFormula200Deps {
    fn id(&self) -> &'static str {
        "s046-giant-ast-formula-200-deps"
    }

    fn description(&self) -> &'static str {
        "One hundred single-cell formulas with giant ASTs and a shared edited precedent."
    }

    fn tags(&self) -> &'static [ScenarioTag] {
        &[
            ScenarioTag::AnchoredArithmetic,
            ScenarioTag::SingleCellEdit,
            ScenarioTag::GiantAst,
        ]
    }

    fn build_fixture(&self, ctx: &ScenarioBuildCtx) -> Result<ScenarioFixture> {
        self.scale.set(ctx.scale);
        let path = fixture_path(ctx, self.id());
        write_workbook(&path, |book| {
            let sheet = book.get_sheet_by_name_mut("Sheet1").expect("Sheet1 exists");
            for row in 1..=ROWS {
                sheet.get_cell_mut((1, row)).set_value_number(row as f64);
            }
            for row in 1..=FORMULA_ROWS {
                sheet.get_cell_mut((2, row)).set_formula(giant_formula(row));
            }
        });
        Ok(ScenarioFixture {
            path,
            metadata: FixtureMetadata {
                rows: ROWS,
                cols: 2,
                sheets: 1,
                formula_cells: FORMULA_ROWS,
                value_cells: ROWS,
                has_named_ranges: false,
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
        let mut invariants = vec![ScenarioInvariant::NoErrorCells {
            sheet: "Sheet1".to_string(),
        }];
        if has_evaluated_formulas(phase) {
            let cycles = completed_cycles(phase);
            for row in [1, 50, 100] {
                invariants.push(ScenarioInvariant::CellEquals {
                    sheet: "Sheet1".to_string(),
                    row,
                    col: 2,
                    expected: numeric(expected(row, cycles)),
                });
            }
        }
        invariants
    }
}

fn apply_edit(wb: &mut Workbook, cycle: usize) -> Result<&'static str, anyhow::Error> {
    wb.set_value(
        "Sheet1",
        1,
        1,
        LiteralValue::Number(10_000.0 + cycle as f64),
    )?;
    Ok("giant_ast_precedent")
}

fn giant_formula(row: u32) -> String {
    let mut terms = vec!["A1".to_string()];
    for r in 2..=61 {
        terms.push(format!("A{r}"));
    }
    format!(
        "={}+SUM(A62:A91)*AVERAGE(A92:A121)+IF(A122>0,A123*A124,A125/A126)+SUM(A127:A176)/A177+A178*A179-A180/A181+{}",
        terms.join("+"),
        row
    )
}

fn a_value(row: u32, cycles: usize) -> f64 {
    if row == 1 && cycles > 0 {
        10_000.0 + (cycles - 1) as f64
    } else {
        row as f64
    }
}

fn expected(row: u32, cycles: usize) -> f64 {
    let explicit: f64 = (1..=61).map(|r| a_value(r, cycles)).sum();
    let sum_62_91: f64 = (62..=91).map(|r| a_value(r, cycles)).sum();
    let avg_92_121: f64 = (92..=121).map(|r| a_value(r, cycles)).sum::<f64>() / 30.0;
    explicit
        + sum_62_91 * avg_92_121
        + a_value(123, cycles) * a_value(124, cycles)
        + (127..=176).map(|r| a_value(r, cycles)).sum::<f64>() / a_value(177, cycles)
        + a_value(178, cycles) * a_value(179, cycles)
        - a_value(180, cycles) / a_value(181, cycles)
        + row as f64
}
