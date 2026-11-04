use super::*;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use std::sync::Arc;
#[derive(Debug)]
struct Pause;
impl Function for Pause {
    fn name(&self) -> &'static str {
        "JOB_RESOURCE_PAUSE"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, formualizer_common::ExcelError> {
        std::thread::sleep(Duration::from_millis(10));
        Ok(CalcValue::Scalar(LiteralValue::Number(1.)))
    }
}
#[test]
fn job_deadline_accumulates_active_time_and_excludes_idle() {
    if isolated_test("resources::job_deadline_accumulates_active_time_and_excludes_idle") {
        return;
    }
    crate::function_registry::register_function(Arc::new(Pause));
    let make = || {
        let mut e = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                family_execution: false,
                enable_parallel: false,
                ..Default::default()
            },
        );
        for c in 1..=100 {
            e.set_cell_formula("Sheet1", 1, c, parse("=JOB_RESOURCE_PAUSE()").unwrap())
                .unwrap();
        }
        e.resumable_stop_after_units_for_test = Some(1);
        e
    };
    let mut idle = make();
    idle.set_evaluation_resource_budgets(EvaluationBudgets {
        deadline: DeadlineResourceBudget {
            max_elapsed: Some(Duration::from_secs(1)),
        },
        ..Default::default()
    });
    idle.begin_recalc(Default::default()).unwrap();
    idle.step_recalc(EvaluationStop::after(Duration::ZERO))
        .unwrap();
    idle.step_recalc(EvaluationStop::after(Duration::ZERO))
        .unwrap();
    std::thread::sleep(Duration::from_millis(1100));
    idle.step_recalc(EvaluationStop::after(Duration::ZERO))
        .unwrap();
    idle.cancel_recalc();
    let mut e = make();
    e.set_evaluation_resource_budgets(EvaluationBudgets {
        deadline: DeadlineResourceBudget {
            max_elapsed: Some(Duration::from_millis(100)),
        },
        ..Default::default()
    });
    e.begin_recalc(Default::default()).unwrap();
    let mut calls = 0;
    loop {
        calls += 1;
        match e.step_recalc(EvaluationStop::after(Duration::ZERO)) {
            Err(error) => {
                assert!(matches!(
                    error.extra(),
                    formualizer_common::ExcelErrorExtra::Resource { .. }
                ));
                break;
            }
            Ok(report) => assert_ne!(report.phase, StepPhase::Finished),
        }
        assert!(calls < 100);
    }
    assert!(calls > 2);
    assert!(!e.has_active_recalc_job());
}
#[test]
fn job_graph_build_and_scheduling_have_separate_boundaries() {
    if isolated_test("resources::job_graph_build_and_scheduling_have_separate_boundaries") {
        return;
    }
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            defer_graph_building: true,
            ..Default::default()
        },
    );
    for r in 1..=10 {
        e.stage_formula_text("Sheet1", r, 1, format!("={r}"));
    }
    let sheet = e.graph.sheet_id("Sheet1").unwrap();
    e.begin_recalc(Default::default()).unwrap();
    assert!(e.has_pending_in_rect(sheet, 0, 0, 9, 0));
    let build = e
        .step_recalc(EvaluationStop::after(Duration::ZERO))
        .unwrap();
    assert_eq!(build.phase, StepPhase::BuildingGraph);
    assert_eq!(build.step_computed_vertices, 0);
    assert_eq!(build.progress.staged_formulas_remaining, 0);
    let plan = e
        .step_recalc(EvaluationStop::after(Duration::ZERO))
        .unwrap();
    assert_eq!(plan.phase, StepPhase::Scheduling);
    assert_eq!(plan.step_computed_vertices, 0);
    assert_eq!(plan.progress.scheduled_in_pass, 10);
    assert!(plan.progress.total_exact);
}
