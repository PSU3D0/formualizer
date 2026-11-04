use super::*;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn finish(e: &mut Engine<TestWorkbook>) -> StepReport {
    for _ in 0..1000 {
        let r = e
            .step_recalc(EvaluationStop::after(Duration::ZERO))
            .unwrap();
        if r.phase == StepPhase::Finished {
            return r;
        }
    }
    panic!("no completion");
}
#[test]
fn job_sub_layer_cursor_executes_200_runs_once() {
    if isolated_test("invariants::job_sub_layer_cursor_executes_200_runs_once") {
        return;
    }
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..Default::default()
        },
    );
    for row in 1..=4 {
        e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(7.))
            .unwrap();
    }
    for col in 2..=201 {
        for row in 1..=4 {
            e.set_cell_formula("Sheet1", row, col, parse(format!("=$A$1*{col}")).unwrap())
                .unwrap();
        }
    }
    e.resumable_stop_after_units_for_test = Some(1);
    e.begin_recalc(Default::default()).unwrap();
    let first = e
        .step_recalc(EvaluationStop::after(Duration::ZERO))
        .unwrap();
    assert_eq!(first.phase, StepPhase::Scheduling);
    assert_eq!(first.progress.scheduled_in_pass, 800);
    let mut n = 0;
    let mut calls = 0;
    loop {
        let r = e.step_recalc(EvaluationStop::none()).unwrap();
        n += r.step_computed_vertices;
        calls += 1;
        assert_eq!(r.step_computed_vertices, 4);
        assert_eq!(r.progress.pass, 1);
        assert_eq!(r.progress.completed_in_pass, n);
        if r.phase == StepPhase::Finished {
            break;
        }
    }
    assert_eq!((n, calls), (800, 200));
}
#[test]
fn job_pending_and_delta_overflow_cover_spills() {
    if isolated_test("invariants::job_pending_and_delta_overflow_cover_spills") {
        return;
    }
    for cap in [0, 1024] {
        let mut e = fixture();
        e.set_cell_formula("Sheet1", 1, 3, parse("=SEQUENCE(5,2)").unwrap())
            .unwrap();
        let sheet = e.graph.sheet_id("Sheet1").unwrap();
        let options = RecalcJobOptions {
            max_delta_records: cap,
            ..Default::default()
        };
        e.begin_recalc(options).unwrap();
        assert!(e.has_pending_in_rect(sheet, 0, 0, 199, 3));
        let mut covered = std::collections::BTreeSet::new();
        let mut overflow = false;
        loop {
            let r = e
                .step_recalc(EvaluationStop::after(Duration::ZERO))
                .unwrap();
            overflow |= r.changed.overflow_sheets.contains(&sheet);
            for record in &r.changed.delta.records {
                let (r0, c0, r1, c1) = record.bounds();
                for row in r0..=r1 {
                    for col in c0..=c1 {
                        covered.insert((row, col));
                    }
                }
            }
            if r.phase == StepPhase::Finished {
                break;
            }
        }
        assert!(!e.has_pending_in_rect(sheet, 0, 0, 199, 3));
        assert!(e.pending_cells_in_rect(sheet, 0, 0, 199, 3).is_empty());
        if cap == 0 {
            assert!(overflow);
        } else {
            for row in 0..200 {
                assert!(covered.contains(&(row, 0)));
            }
            for row in 0..5 {
                for col in 2..4 {
                    assert!(covered.contains(&(row, col)), "spill {row},{col}");
                }
            }
        }
    }
}
#[test]
fn job_work_budget_accumulates_and_errors_end_job() {
    if isolated_test("invariants::job_work_budget_accumulates_and_errors_end_job") {
        return;
    }
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: false,
            enable_parallel: false,
            ..Default::default()
        },
    );
    // Independent units: each slice fits the cap, the entire job does not.
    for c in 2..=101 {
        e.set_cell_formula("Sheet1", 1, c, parse(format!("={c}")).unwrap())
            .unwrap();
    }
    e.set_evaluation_resource_budgets(EvaluationBudgets {
        work: WorkResourceBudget {
            max_work_units: Some(50),
        },
        ..Default::default()
    });
    e.resumable_stop_after_units_for_test = Some(1);
    e.begin_recalc(Default::default()).unwrap();
    let mut calls = 0;
    loop {
        calls += 1;
        match e.step_recalc(EvaluationStop::after(Duration::ZERO)) {
            Err(_) => break,
            Ok(r) => assert_ne!(r.phase, StepPhase::Finished),
        }
        assert!(calls < 1000);
    }
    assert!(calls > 2);
    assert!(!e.has_active_recalc_job());
    assert!(!e.has_pending_resumable_evaluation());
    e.set_evaluation_resource_budgets(Default::default());
    e.evaluate_all().unwrap();
}
#[derive(Debug)]
struct CancelFifth(AtomicUsize);
impl Function for CancelFifth {
    fn name(&self) -> &'static str {
        "JOB_CANCEL_FIFTH"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, formualizer_common::ExcelError> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 4
            && let Some(t) = ctx.cancellation_token()
        {
            t.cancel();
        }
        Ok(CalcValue::Scalar(LiteralValue::Number(1.)))
    }
}
#[test]
fn job_mid_scc_cancellation_restores_seed() {
    if isolated_test("invariants::job_mid_scc_cancellation_restores_seed") {
        return;
    }
    crate::function_registry::register_function(Arc::new(CancelFifth(AtomicUsize::new(0))));
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_cycle(CycleConfig::iterate(100, 0.)),
    );
    e.set_cell_formula("Sheet1", 1, 1, parse("=A1+JOB_CANCEL_FIFTH()").unwrap())
        .unwrap();
    e.begin_recalc(Default::default()).unwrap();
    let report = e
        .step_recalc(EvaluationStop::cancel(CancelToken::new()))
        .unwrap();
    assert_eq!(report.phase, StepPhase::Cancelled);
    assert!(report.result.is_none());
    assert!(!e.has_active_recalc_job());
    let sheet = e.graph.sheet_id("Sheet1").unwrap();
    assert!(e.has_pending_in_rect(sheet, 0, 0, 0, 0));
    e.begin_recalc(Default::default()).unwrap();
    finish(&mut e);
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(100.))
    );
}
#[test]
fn job_priority_finishes_viewport_before_background() {
    if isolated_test("invariants::job_priority_finishes_viewport_before_background") {
        return;
    }
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            ..Default::default()
        },
    );
    e.add_sheet("Visible").unwrap();
    for c in 1..=200 {
        for r in 1..=250 {
            e.set_cell_formula("Sheet1", r, c, parse(format!("=ROW()*{c}")).unwrap())
                .unwrap();
        }
    }
    for r in 1..=200 {
        e.set_cell_value("Visible", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
        e.set_cell_formula(
            "Visible",
            r,
            2,
            parse(format!("=SUMIFS($A$1:$A$200,$A$1:$A$200,\">0\")+A{r}")).unwrap(),
        )
        .unwrap();
    }
    e.set_cell_formula("Visible", 1, 3, parse("=B10+1").unwrap())
        .unwrap();
    let sheet = e.graph.sheet_id("Visible").unwrap();
    e.begin_recalc(Default::default()).unwrap();
    e.step_recalc(EvaluationStop::after(Duration::ZERO))
        .unwrap();
    e.prioritize_recalc(&[EvaluationTarget::Cell {
        sheet: "Visible".into(),
        row: 1,
        col: 3,
    }])
    .unwrap();
    e.resumable_stop_after_units_for_test = Some(2);
    let report = e.step_recalc(EvaluationStop::none()).unwrap();
    assert!(!e.has_pending_in_rect(sheet, 0, 1, 9, 2));
    assert!(report.step_computed_vertices >= 200);
    assert!(report.step_computed_vertices < 1000);
    let result = finish(&mut e).result.unwrap();
    assert_eq!(
        result.computed_vertices, 50201,
        "priority must not re-execute a family"
    );
}
#[test]
fn job_refuses_all_foreign_routes_without_changing_progress() {
    if isolated_test("invariants::job_refuses_all_foreign_routes_without_changing_progress") {
        return;
    }
    let mut e = fixture();
    e.begin_recalc(Default::default()).unwrap();
    e.step_recalc(EvaluationStop::after(Duration::ZERO))
        .unwrap();
    let before = e.recalc_reuse_probe();
    let targets = [EvaluationTarget::Cell {
        sheet: "Sheet1".into(),
        row: 200,
        col: 1,
    }];
    assert!(e.evaluate_all().is_err());
    assert!(e.evaluate_all_until(EvaluationStop::none()).is_err());
    assert!(e.evaluate_all_cancellable(CancelToken::new()).is_err());
    assert!(e.evaluate_cell("Sheet1", 1, 1).is_err());
    assert!(e.evaluate_cells(&[("Sheet1", 1, 1)]).is_err());
    assert!(e.evaluate_targets(&targets).is_err());
    assert!(e.evaluate_all_with_delta().is_err());
    assert!(e.evaluate_all_with_target_delta().is_err());
    assert_eq!(
        e.recalc_reuse_probe().schedule_requests,
        before.schedule_requests
    );
    let r = finish(&mut e);
    assert_eq!(r.result.unwrap().computed_vertices, 200);
}
