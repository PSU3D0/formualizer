//! Array-condition IF uses the general evaluator; scalar family kernels remain unchanged.
use crate::engine::{CancelToken, Engine, EvalConfig};
use crate::function::{FnCaps, Function};
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::Arc;

fn norm(value: LiteralValue) -> String {
    match value {
        LiteralValue::Array(rows) => format!(
            "[{}]",
            rows.into_iter()
                .map(|row| row.into_iter().map(norm).collect::<Vec<_>>().join(","))
                .collect::<Vec<_>>()
                .join(";")
        ),
        LiteralValue::Error(e) => e.kind.to_string(),
        LiteralValue::Boolean(b) => b.to_string().to_uppercase(),
        other => other.to_string(),
    }
}

#[test]
fn array_if_errors_text_broadcast_and_consumers_tree() {
    crate::builtins::load_builtins();
    let wb = TestWorkbook::new();
    let interp = wb.interpreter();
    for (formula, expected) in [
        ("=IF({TRUE,FALSE,#N/A,\"x\",0},7,8)", "[7,8,#N/A,#VALUE!,8]"),
        ("=IF({TRUE,FALSE},1/0,NA())", "[#DIV/0!,#N/A]"),
        ("=IF({TRUE,FALSE,TRUE},{1,2},0)", "#VALUE!"),
        ("=IF({TRUE;TRUE},{1,2},SEQUENCE(1000000000))", "[1,2;1,2]"),
        ("=MAX(IF({TRUE;FALSE;TRUE},{1;20;3},0))", "3"),
        ("=INDEX(IF({TRUE;FALSE;TRUE},{1;20;3},0),3,1)", "3"),
        ("=MATCH(3,IF({TRUE;FALSE;TRUE},{1;20;3},0),0)", "3"),
        ("=IF(SEQUENCE(4097)>0,SEQUENCE(1,4097),0)", "#NUM!"),
    ] {
        assert_eq!(
            norm(
                interp
                    .evaluate_ast(&parse(formula).unwrap())
                    .unwrap()
                    .into_literal()
            ),
            expected,
            "{formula}"
        );
    }
}

#[test]
fn ifs_and_switch_array_behavior_is_unchanged() {
    crate::builtins::load_builtins();
    let wb = TestWorkbook::new();
    let interp = wb.interpreter();
    for (formula, expected) in [
        ("=IFS({TRUE;FALSE},7,TRUE,8)", "#VALUE!"),
        ("=SWITCH({1;2},1,7,2,8,9)", "9"),
        ("=SWITCH({1;2},1,7,2,8)", "#N/A"),
    ] {
        assert_eq!(
            norm(
                interp
                    .evaluate_ast(&parse(formula).unwrap())
                    .unwrap()
                    .into_literal()
            ),
            expected
        );
    }
}

#[test]
fn array_if_range_spill_reduction_and_fixed_extents_arena() {
    for fixed in [None, Some(1), Some(3)] {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_execution: fixed.is_none(),
                ..EvalConfig::default()
            },
        );
        for r in 1..=3 {
            engine
                .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
                .unwrap();
        }
        engine
            .set_cell_formula("Sheet1", 1, 2, parse("=SUM(IF(A1:A3>0,A1:A3))").unwrap())
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 1, 3, parse("=IF(A1:A3>1,A1:A3,0)").unwrap())
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 1, 4, parse("=IF(A1:A3,A1:A3,0)").unwrap())
            .unwrap();
        if let Some(rows) = fixed {
            engine
                .declare_fixed_array_formula("Sheet1", 1, 2, 1, 1)
                .unwrap();
            engine
                .declare_fixed_array_formula("Sheet1", 1, 3, rows, 1)
                .unwrap();
        }
        engine.evaluate_all().unwrap();
        assert_eq!(norm(engine.get_cell_value("Sheet1", 1, 2).unwrap()), "6");
        for r in 1..=fixed.unwrap_or(3) {
            assert_eq!(
                norm(engine.get_cell_value("Sheet1", r, 3).unwrap()),
                if r == 1 { "0".into() } else { r.to_string() }
            );
        }
        for r in 1..=3 {
            assert_eq!(
                norm(engine.get_cell_value("Sheet1", r, 4).unwrap()),
                r.to_string()
            );
        }
    }
}

#[test]
fn array_if_result_uses_existing_spill_admission_cap() {
    let mut config = EvalConfig::default();
    config.spill.max_spill_cells = 2;
    let mut engine = Engine::new(TestWorkbook::new(), config);
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=IF({TRUE;FALSE;TRUE},7,0)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    let Some(LiteralValue::Error(error)) = engine.get_cell_value("Sheet1", 1, 1) else {
        panic!("expected spill error")
    };
    assert_eq!(error.kind, ExcelErrorKind::Spill);
}

#[test]
fn array_if_copied_family_sequential_and_parallel() {
    for parallel in [false, true] {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                enable_parallel: parallel,
                ..EvalConfig::default()
            },
        );
        for r in 1..=202 {
            engine
                .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
                .unwrap();
        }
        for r in 1..=200 {
            engine
                .set_cell_formula(
                    "Sheet1",
                    r,
                    2,
                    parse(&format!("=SUM(IF(A{r}:A{}>0,A{r}:A{}))", r + 2, r + 2)).unwrap(),
                )
                .unwrap();
        }
        engine.evaluate_all().unwrap();
        for r in 1..=200 {
            assert_eq!(
                norm(engine.get_cell_value("Sheet1", r, 2).unwrap()),
                (3 * r + 3).to_string(),
                "parallel={parallel}, row={r}"
            );
        }
    }
}

#[test]
fn array_if_bare_copied_family_spills_with_room() {
    use crate::engine::{FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode};
    for parallel in [false, true] {
        for mode in [
            FormulaPlaneMode::Off,
            FormulaPlaneMode::AuthoritativeExperimental,
        ] {
            let config = EvalConfig {
                enable_parallel: parallel,
                ..EvalConfig::default().with_formula_plane_mode(mode)
            };
            let mut engine = Engine::new(TestWorkbook::new(), config);
            let mut records = Vec::new();
            for r in 1..=120 {
                engine
                    .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
                    .unwrap();
                engine
                    .set_cell_value("Sheet1", r, 2, LiteralValue::Number(-(r as f64)))
                    .unwrap();
                let formula = format!("=IF(A{r}:B{r}>0,A{r}:B{r},0)");
                let ast_id = engine.intern_formula_ast(&parse(&formula).unwrap());
                records.push(FormulaIngestRecord::new(
                    r,
                    4,
                    ast_id,
                    Some(Arc::<str>::from(formula)),
                ));
            }
            engine
                .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
                .unwrap();
            engine.evaluate_all().unwrap();
            for r in 1..=120 {
                assert_eq!(
                    norm(engine.get_cell_value("Sheet1", r, 4).unwrap()),
                    r.to_string(),
                    "parallel={parallel}, mode={mode:?}"
                );
                assert_eq!(norm(engine.get_cell_value("Sheet1", r, 5).unwrap()), "0");
            }
        }
    }
}

#[derive(Debug)]
struct FailIf(ExcelError);
impl Function for FailIf {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        "FAIL_IF"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        Err(self.0.clone())
    }
}

#[test]
fn array_if_branch_failures_preserve_scalar_if_behavior() {
    use formualizer_common::{ExcelErrorExtra, ResourceExhaustionDetail, ResourceExhaustionReason};
    crate::builtins::load_builtins();
    for error in [
        ExcelError::new_value(),
        ExcelError::new(ExcelErrorKind::Cancelled),
        ExcelError::new(ExcelErrorKind::NImpl).with_extra(ExcelErrorExtra::Resource {
            detail: Box::new(ResourceExhaustionDetail {
                reason: ResourceExhaustionReason::ScratchMemory,
                limit: 1,
                observed: 2,
                request_id: None,
            }),
        }),
    ] {
        let wb = TestWorkbook::new().with_function(Arc::new(FailIf(error)));
        let interp = wb.interpreter();
        let scalar = interp.evaluate_ast(&parse("=IF(TRUE,FAIL_IF(),0)").unwrap());
        let array = interp.evaluate_ast(&parse("=IF({TRUE;FALSE},FAIL_IF(),0)").unwrap());
        assert!(scalar.is_err());
        assert_eq!(scalar.unwrap_err(), array.unwrap_err());
    }
}

#[derive(Debug)]
struct CancelArray(bool);
impl Function for CancelArray {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        "CANCEL_ARRAY"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        ctx.cancellation_token().unwrap().cancel();
        let rows = vec![vec![LiteralValue::Boolean(true)]; 5000];
        Ok(if self.0 {
            CalcValue::Range(crate::engine::range_view::RangeView::from_owned_rows(
                rows,
                crate::engine::DateSystem::Excel1900,
            ))
        } else {
            CalcValue::Scalar(LiteralValue::Array(rows))
        })
    }
}

#[test]
fn array_if_cancel_during_scan_and_materialization() {
    crate::builtins::load_builtins();
    for range in [false, true] {
        for formula in [
            "=IF(CANCEL_ARRAY(),1,2)",
            "=IF({TRUE;FALSE},CANCEL_ARRAY(),2)",
            "=IF(SEQUENCE(5000)>0,CANCEL_ARRAY(),2)",
        ] {
            let token = CancelToken::new();
            let wb = TestWorkbook::new()
                .with_cancellation_token(token)
                .with_function(Arc::new(CancelArray(range)));
            let interp = wb.interpreter();
            let error = interp.evaluate_ast(&parse(formula).unwrap()).unwrap_err();
            assert_eq!(error.kind, ExcelErrorKind::Cancelled, "{formula}");
        }
    }
}
