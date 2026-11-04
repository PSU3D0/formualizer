use super::*;
use std::ops::ControlFlow;

#[test]
fn no_active_job_is_typed() {
    let mut e = fixture();
    assert!(matches!(e.step_recalc(EvaluationStop::none()), Err(RecalcJobError::NoActiveJob)));
    assert!(matches!(e.prioritize_recalc(&[]), Err(RecalcJobError::NoActiveJob)));
}
#[test]
fn pending_job_is_typed() {
    let mut e = fixture();
    e.begin_recalc(Default::default()).unwrap();
    assert!(matches!(e.begin_recalc(Default::default()), Err(RecalcJobError::AlreadyPending { explicit: true })));
    assert!(matches!(e.recalc_with(Default::default(), EvaluationStop::none(), |_, _| ControlFlow::Continue(())), Err(RecalcJobError::AlreadyPending { explicit: true })));
    e.cancel_recalc();
    e.evaluate_all_until(EvaluationStop::after(Duration::ZERO)).unwrap();
    assert!(matches!(e.begin_recalc(Default::default()), Err(RecalcJobError::AlreadyPending { explicit: false })));
}
#[test]
fn closure_finishes_like_uninterrupted() {
    let mut e = fixture();
    let mut control = fixture();
    let expected = control.evaluate_all().unwrap();
    let mut calls = 0;
    let result = e.recalc_with(Default::default(), EvaluationStop::after(Duration::ZERO), |_, _| {
        calls += 1;
        ControlFlow::Continue(())
    }).unwrap();
    let RecalcOutcome::Finished(result) = result else { panic!("did not finish") };
    assert!(calls > 1);
    assert_eq!(result.computed_vertices, expected.computed_vertices);
    assert!(!e.has_active_recalc_job());
    for r in 1..=200 {
        assert_eq!(e.get_cell_value("Sheet1", r, 1), control.get_cell_value("Sheet1", r, 1));
    }
}
#[test]
fn closure_pause_resume_and_terminal_precedence() {
    let mut e = fixture();
    let mut calls = 0;
    assert!(matches!(e.recalc_with(Default::default(), EvaluationStop::after(Duration::ZERO), |_, _| {
        calls += 1;
        ControlFlow::Break(())
    }).unwrap(), RecalcOutcome::Paused));
    assert_eq!(calls, 1);
    assert!(e.has_active_recalc_job());
    assert_eq!(e.step_recalc(EvaluationStop::none()).unwrap().phase, StepPhase::Finished);
    // A final report cannot leave a resumable job, even when the callback breaks.
    assert!(matches!(e.recalc_with(Default::default(), EvaluationStop::none(), |_, _| ControlFlow::Break(())).unwrap(), RecalcOutcome::Finished(_)));
}
#[test]
fn closure_can_edit_prioritize_and_cancel() {
    let mut e = fixture();
    let mut calls = 0;
    let result = e.recalc_with(Default::default(), EvaluationStop::after(Duration::ZERO), |e, _| {
        calls += 1;
        if calls == 1 {
            e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.)).unwrap();
            e.prioritize_recalc(&[EvaluationTarget::Cell { sheet: "Sheet1".into(), row: 200, col: 1 }]).unwrap();
        } else {
            e.cancel_recalc().unwrap();
        }
        ControlFlow::Continue(())
    }).unwrap();
    assert!(matches!(result, RecalcOutcome::Cancelled));
    assert_eq!(calls, 2);
    assert!(!e.has_active_recalc_job());
    e.begin_recalc(Default::default()).unwrap();
    e.step_recalc(EvaluationStop::none()).unwrap();
    assert_eq!(e.get_cell_value("Sheet1", 200, 1), Some(LiteralValue::Number(209.)));
}
#[test]
fn closure_random_edits_and_priorities() {
    if isolated_test("polish::closure_random_edits_and_priorities") { return; }
    for family in [false, true] {
        for seed in 0..32u64 {
            let mut e = fixture();
            e.config.family_execution = family;
            let mut control = fixture();
            control.config.family_execution = family;
            let mut random = seed + 1;
            let mut steps = 0;
            let result = e.recalc_with(Default::default(), EvaluationStop::after(Duration::ZERO), |e, report| {
                steps += 1;
                if steps <= 8 && report.phase != StepPhase::Finished {
                    random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let row = 1 + (random % 100) as u32;
                    let value = LiteralValue::Number((random % 500) as f64);
                    e.set_cell_value("Sheet1", row, 1, value.clone()).unwrap();
                    control.set_cell_value("Sheet1", row, 1, value).unwrap();
                    e.prioritize_recalc(&[EvaluationTarget::Cell { sheet: "Sheet1".into(), row: 101 + (random % 100) as u32, col: 1 }]).unwrap();
                }
                ControlFlow::Continue(())
            }).unwrap();
            assert!(matches!(result, RecalcOutcome::Finished(_)));
            control.evaluate_all().unwrap();
            for row in 1..=200 {
                assert_eq!(e.get_cell_value("Sheet1", row, 1), control.get_cell_value("Sheet1", row, 1), "seed={seed} family={family} row={row}");
            }
        }
    }
}
#[test]
fn step_error_ends_job_and_allows_restart() {
    let mut e = fixture();
    e.set_evaluation_resource_budgets(EvaluationBudgets { work: WorkResourceBudget { max_work_units: Some(5) }, ..Default::default() });
    e.begin_recalc(Default::default()).unwrap();
    let err = e.step_recalc(EvaluationStop::none()).unwrap_err();
    assert!(matches!(&err, RecalcJobError::Evaluation(_)));
    assert!(std::error::Error::source(&err).is_some());
    assert!(!e.has_active_recalc_job());
    assert!(!e.has_pending_resumable_evaluation());
    e.set_evaluation_resource_budgets(Default::default());
    e.begin_recalc(Default::default()).unwrap();
    assert_eq!(e.step_recalc(EvaluationStop::none()).unwrap().phase, StepPhase::Finished);
    assert_eq!(e.get_cell_value("Sheet1", 200, 1), Some(LiteralValue::Number(200.)));
}
#[test]
fn job_types_are_send() {
    fn send<T: Send>() {}
    send::<Engine<TestWorkbook>>();
    send::<StepReport>();
    send::<RecalcOutcome>();
    send::<RecalcJobError>();
    send::<EvaluationStop>();
}

use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use std::sync::{Arc, Barrier, atomic::{AtomicBool, Ordering}};

#[derive(Debug)]
struct Gate {
    entered: Arc<Barrier>,
    released: Arc<Barrier>,
    first: AtomicBool,
}
impl Function for Gate {
    fn name(&self) -> &'static str { "JOB_THREAD_GATE" }
    fn eval<'a, 'b, 'c>(&self, _: &'c [ArgumentHandle<'a, 'b>], _: &dyn FunctionContext<'b>) -> Result<CalcValue<'b>, formualizer_common::ExcelError> {
        if self.first.swap(false, Ordering::SeqCst) {
            self.entered.wait();
            self.released.wait();
        }
        Ok(CalcValue::Scalar(LiteralValue::Number(7.)))
    }
}
#[test]
fn cross_thread_cancel_does_not_install_observing_unit() {
    if isolated_test("polish::cross_thread_cancel_does_not_install_observing_unit") { return; }
    for family in [false, true] {
        let entered = Arc::new(Barrier::new(2));
        let released = Arc::new(Barrier::new(2));
        crate::function_registry::register_function(Arc::new(Gate { entered: entered.clone(), released: released.clone(), first: AtomicBool::new(true) }));
        let mut e = Engine::new(TestWorkbook::new(), EvalConfig { family_execution: family, enable_parallel: false, ..Default::default() });
        e.set_cell_formula("Sheet1", 1, 1, parse("=JOB_THREAD_GATE()").unwrap()).unwrap();
        e.set_cell_formula("Sheet1", 2, 1, parse("=A1+1").unwrap()).unwrap();
        let token = CancelToken::new();
        let worker_token = token.clone();
        let worker = std::thread::spawn(move || {
            e.begin_recalc(Default::default()).unwrap();
            let report = e.step_recalc(EvaluationStop::cancel(worker_token)).unwrap();
            (e, report)
        });
        entered.wait();
        token.cancel();
        released.wait();
        let (mut e, report) = worker.join().unwrap();
        assert_eq!(report.phase, StepPhase::Cancelled);
        assert!(!e.has_active_recalc_job());
        assert_eq!(report.step_computed_vertices, 0);
        assert_eq!(e.get_cell_value("Sheet1", 1, 1), None);
        let sheet = e.graph.sheet_id("Sheet1").unwrap();
        assert!(e.has_pending_in_rect(sheet, 0, 0, 1, 0));
        e.begin_recalc(Default::default()).unwrap();
        assert_eq!(e.step_recalc(EvaluationStop::none()).unwrap().phase, StepPhase::Finished);
        assert_eq!(e.get_cell_value("Sheet1", 2, 1), Some(LiteralValue::Number(8.)));
    }
}

#[derive(Debug)]
struct PanicOnce(AtomicBool);
impl Function for PanicOnce {
    fn name(&self) -> &'static str { "JOB_PANIC_ONCE" }
    fn eval<'a, 'b, 'c>(&self, _: &'c [ArgumentHandle<'a, 'b>], _: &dyn FunctionContext<'b>) -> Result<CalcValue<'b>, formualizer_common::ExcelError> {
        assert!(!self.0.swap(false, Ordering::SeqCst), "custom function panic probe");
        Ok(CalcValue::Scalar(LiteralValue::Number(7.)))
    }
}
#[test]
fn custom_function_panic_characterization() {
    if isolated_test("polish::custom_function_panic_characterization") { return; }
    for family in [false, true] {
        crate::function_registry::register_function(Arc::new(PanicOnce(AtomicBool::new(true))));
        let mut e = Engine::new(TestWorkbook::new(), EvalConfig { family_execution: family, enable_parallel: false, ..Default::default() });
        e.set_cell_formula("Sheet1", 1, 1, parse("=JOB_PANIC_ONCE()").unwrap()).unwrap();
        e.set_cell_formula("Sheet1", 2, 1, parse("=A1+1").unwrap()).unwrap();
        e.begin_recalc(Default::default()).unwrap();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| e.step_recalc(EvaluationStop::none())));
        println!("PANIC family={family} propagated={} active={} pending={}", panic.is_err(), e.has_active_recalc_job(), e.has_pending_resumable_evaluation());
        assert!(panic.is_err());
        let resumed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| e.step_recalc(EvaluationStop::none())));
        println!("RESUME family={family} result={resumed:?} A2={:?}", e.get_cell_value("Sheet1",2,1));
        e.cancel_recalc();
        let restarted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| e.evaluate_all()));
        println!("RESTART family={family} result={restarted:?} A2={:?}", e.get_cell_value("Sheet1",2,1));
    }
}
