use crate::{
    engine::{Engine, EvalConfig, EvaluationStop, RecalcJobError, StepPhase},
    function::{FnCaps, Function},
    operation_diagnostics::{OperationDiagnosticOptions, UnavailableOperation},
    test_workbook::TestWorkbook,
    traits::{ArgumentHandle, CalcValue, FunctionContext},
};
use formualizer_common::{ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use std::sync::{Arc, atomic::Ordering};

struct Failing {
    payload: u8,
    returned: bool,
}
impl Function for Failing {
    fn name(&self) -> &'static str {
        "PANIC_TEST"
    }
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn reports_known_limitations(&self) -> bool {
        true
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        if self.returned {
            return Err(ExcelError::new(ExcelErrorKind::Div).with_message("ordinary"));
        }
        match self.payload {
            0 => panic!("string slice payload"),
            1 => std::panic::panic_any(String::from("owned payload")),
            _ => std::panic::panic_any(42_u32),
        }
    }
}
fn engine(payload: u8, returned: bool, family: bool, parallel: bool) -> Engine<TestWorkbook> {
    let provider = TestWorkbook::new().with_function(Arc::new(Failing { payload, returned }));
    let cfg = EvalConfig {
        family_execution: family,
        enable_parallel: parallel,
        ..Default::default()
    };
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap(),
    );
    let mut e = Engine::with_thread_pool(provider, cfg, pool);
    e.configure_operation_diagnostics(OperationDiagnosticOptions {
        physical_telemetry: true,
        ..Default::default()
    });
    e
}
fn formula(e: &mut Engine<TestWorkbook>, row: u32, text: &str) {
    e.set_cell_formula("Sheet1", row, 1, parse(text).unwrap())
        .unwrap();
}

#[test]
fn panic_payloads_are_typed_and_dependents_propagate() {
    for payload in 0..3 {
        for family in [false, true] {
            let mut e = engine(payload, false, family, false);
            formula(&mut e, 1, "=PANIC_TEST()");
            formula(&mut e, 2, "=A1+1");
            e.evaluate_all().unwrap();
            let Some(LiteralValue::Error(error)) = e.get_cell_value("Sheet1", 1, 1) else {
                panic!("missing error")
            };
            assert_eq!(error.kind, ExcelErrorKind::Value);
            assert_eq!(
                error.extra(),
                &ExcelErrorExtra::FunctionPanicked {
                    function: "PANIC_TEST".into(),
                    message: match payload {
                        0 => Some("string slice payload".into()),
                        1 => Some("owned payload".into()),
                        _ => None,
                    },
                }
            );
            assert_eq!(
                e.get_cell_value("Sheet1", 2, 1),
                Some(LiteralValue::Error(error))
            );
            assert_eq!(e.request_scope_for_test(), (0, false, false));
            assert!(!e.is_poisoned());
        }
    }
}

#[test]
fn iferror_lambda_arrays_and_parallel_calls_keep_executed_root_evidence() {
    for family in [false, true] {
        for parallel in [false, true] {
            let mut e = engine(0, false, family, parallel);
            for row in 1..=160 {
                // Copied families exercise per-member paths and parallel chunks.
                formula(&mut e, row, &format!("=IFERROR(PANIC_TEST(),0)+B{row}"));
            }
            formula(
                &mut e,
                161,
                "=LET(f,LAMBDA(x,PANIC_TEST()),IFERROR(f(1),0))",
            );
            formula(&mut e, 162, "=IFERROR(PANIC_TEST()+{1,2},0)");
            e.evaluate_all().unwrap();
            for row in 1..=161 {
                assert_eq!(
                    e.get_cell_value("Sheet1", row, 1),
                    Some(LiteralValue::Number(0.))
                );
            }
            let report = e.operation_diagnostics().unwrap().unwrap();
            assert_eq!(report.logical_root_cell_count, 162);
            assert!(report.roots.iter().all(|r| matches!(&*r.operation,
                UnavailableOperation::FunctionPanicked { name } if name == "PANIC_TEST")));
            assert!(!e.is_poisoned());
        }
    }
}

#[test]
fn ordinary_returned_errors_remain_complete() {
    let mut e = engine(0, true, true, true);
    formula(&mut e, 1, "=PANIC_TEST()");
    formula(&mut e, 2, "=IFERROR(PANIC_TEST(),0)");
    e.evaluate_all().unwrap();
    let Some(LiteralValue::Error(error)) = e.get_cell_value("Sheet1", 1, 1) else {
        panic!("missing error")
    };
    assert_eq!(error.kind, ExcelErrorKind::Div);
    assert_eq!(error.extra(), &ExcelErrorExtra::None);
    assert_eq!(
        e.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(0.))
    );
    assert_eq!(
        e.operation_diagnostics()
            .unwrap()
            .unwrap()
            .logical_cell_reason_count,
        0
    );
    assert!(!e.is_poisoned());
}

#[test]
fn aggregate_panic_discards_partial_run_and_recalculates_after_fix() {
    for job in [false, true] {
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                enable_parallel: false,
                family_execution: true,
                family_kernels: true,
                ..Default::default()
            },
        );
        e.configure_operation_diagnostics(Default::default());
        for row in 1..=32 {
            e.set_cell_value("Sheet1", row, 2, LiteralValue::Number(row as f64))
                .unwrap();
            formula(&mut e, row, &format!("=SUM(B{row}:B{})", row + 1));
        }
        e.panic_kernel_after_member.store(3, Ordering::SeqCst);
        if job {
            e.begin_recalc(Default::default()).unwrap();
            assert_eq!(
                e.step_recalc(EvaluationStop::none()).unwrap().phase,
                StepPhase::Finished
            );
        } else {
            e.evaluate_all().unwrap();
        }
        assert_eq!(
            e.panic_kernel_after_member.load(Ordering::SeqCst),
            usize::MAX,
            "must enter actual kernel"
        );
        let report = e.operation_diagnostics().unwrap().unwrap();
        assert_eq!(report.logical_root_cell_count, 31);
        // The final row has a distinct range-extent plan; rows 1..=31 form the run.
        assert_eq!(
            e.get_cell_value("Sheet1", 32, 1),
            Some(LiteralValue::Number(32.))
        );
        for row in 1..=31 {
            let Some(LiteralValue::Error(error)) = e.get_cell_value("Sheet1", row, 1) else {
                panic!("partial kernel value at {row}")
            };
            assert!(
                matches!(error.extra(), ExcelErrorExtra::FunctionPanicked { function, .. } if function=="SUM")
            );
        }
        assert!(!e.is_poisoned());
        for row in 1..=32 {
            formula(&mut e, row, &format!("=SUM(B{row}:B{})", row + 1));
        }
        e.evaluate_all().unwrap();
        for row in 1..=32 {
            let expected = row as f64 + if row < 32 { (row + 1) as f64 } else { 0. };
            assert_eq!(
                e.get_cell_value("Sheet1", row, 1),
                Some(LiteralValue::Number(expected))
            );
        }
    }
}

#[test]
fn internal_unwind_permanently_refuses_work_but_keeps_reads_and_cleanup() {
    for job in [false, true] {
        let mut e = engine(0, true, true, false);
        e.set_cell_value("Sheet1", 1, 2, LiteralValue::Number(9.))
            .unwrap();
        formula(&mut e, 1, "=B1+1");
        e.panic_internal_once = true;
        if job {
            e.begin_recalc(Default::default()).unwrap();
        }
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if job {
                    let _ = e.step_recalc(EvaluationStop::none());
                } else {
                    let _ = e.evaluate_all();
                }
            }))
            .is_err()
        );
        assert!(e.is_poisoned());
        let check = |error: ExcelError| {
            assert_eq!(error.kind, ExcelErrorKind::Value);
            assert_eq!(error.extra(), &ExcelErrorExtra::EnginePoisoned);
            assert_eq!(
                error.message(),
                Some("formualizer engine is poisoned by an earlier internal panic; discard it")
            );
        };
        check(e.evaluate_all().unwrap_err());
        check(e.evaluate_cell("Sheet1", 1, 1).unwrap_err());
        check(
            e.set_cell_value("Sheet1", 1, 2, LiteralValue::Number(99.))
                .unwrap_err(),
        );
        check(
            e.set_cell_formula("Sheet1", 1, 1, parse("=99").unwrap())
                .unwrap_err(),
        );
        check(e.build_recalc_plan().unwrap_err());
        assert!(matches!(
            e.begin_recalc(Default::default()),
            Err(RecalcJobError::Poisoned)
        ));
        assert!(matches!(
            e.step_recalc(EvaluationStop::none()),
            Err(RecalcJobError::Poisoned)
        ));
        assert!(matches!(
            e.prioritize_recalc(&[]),
            Err(RecalcJobError::Poisoned)
        ));
        assert!(e.cancel_recalc().is_none());
        let seed_before = e.config.workbook_seed;
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| e.set_workbook_seed(123)))
                .is_err()
        );
        assert_eq!(e.config.workbook_seed, seed_before);
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 2),
            Some(LiteralValue::Number(9.))
        );
        assert!(e.operation_diagnostics().is_ok());
    }
}

struct DispatchPanic;
impl Function for DispatchPanic {
    fn name(&self) -> &'static str {
        "DISPATCH_PANIC"
    }
    fn dispatch<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        panic!("dispatch override panic")
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        Ok(CalcValue::Scalar(LiteralValue::Number(7.)))
    }
}
struct ReferencePanic;
impl Function for ReferencePanic {
    fn name(&self) -> &'static str {
        "REFERENCE_PANIC"
    }
    fn caps(&self) -> FnCaps {
        FnCaps::PURE | FnCaps::RETURNS_REFERENCE
    }
    fn eval_reference<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Option<Result<formualizer_parse::parser::ReferenceType, ExcelError>> {
        panic!("reference callback panic")
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        Ok(CalcValue::Scalar(LiteralValue::Number(7.)))
    }
}
struct CallablePanic;
impl crate::traits::CustomCallable for CallablePanic {
    fn arity(&self) -> usize {
        1
    }
    fn invoke<'ctx>(
        &self,
        _: &crate::interpreter::Interpreter<'ctx>,
        _: &[LiteralValue],
    ) -> Result<CalcValue<'ctx>, ExcelError> {
        panic!("callable callback panic")
    }
}
struct MakeCallable;
impl Function for MakeCallable {
    fn name(&self) -> &'static str {
        "MAKE_PANIC_CALLABLE"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        Ok(CalcValue::Callable(Arc::new(CallablePanic)))
    }
}
#[test]
fn reference_dispatch_override_and_callable_boundaries_are_caught() {
    for family in [false, true] {
        let provider = TestWorkbook::new()
            .with_function(Arc::new(DispatchPanic))
            .with_function(Arc::new(ReferencePanic))
            .with_function(Arc::new(MakeCallable));
        let mut e = Engine::new(
            provider,
            EvalConfig {
                family_execution: family,
                enable_parallel: false,
                ..Default::default()
            },
        );
        e.configure_operation_diagnostics(Default::default());
        formula(&mut e, 1, "=DISPATCH_PANIC()");
        formula(&mut e, 2, "=SUM(REFERENCE_PANIC():B2)");
        formula(&mut e, 3, "=LET(f,MAKE_PANIC_CALLABLE(),f(1))");
        e.evaluate_all().unwrap();
        for (row, name) in [(1, "DISPATCH_PANIC"), (2, "REFERENCE_PANIC"), (3, "f")] {
            let Some(LiteralValue::Error(error)) = e.get_cell_value("Sheet1", row, 1) else {
                panic!("missing error at {row}")
            };
            assert!(
                matches!(error.extra(), ExcelErrorExtra::FunctionPanicked { function, .. } if function == name),
                "{error:?}"
            );
        }
        assert_eq!(
            e.operation_diagnostics()
                .unwrap()
                .unwrap()
                .logical_root_cell_count,
            3
        );
        assert!(!e.is_poisoned());
    }
}
