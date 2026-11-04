#[path = "recalc_job_array_delta.rs"]
mod array_delta;
#[path = "recalc_job_invariants.rs"]
mod invariants;
#[path = "recalc_job_resources.rs"]
mod resources;
use crate::engine::*;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use std::time::Duration;

fn isolated_test(name: &str) -> bool {
    let full = format!("engine::tests::recalc_job::{name}");
    if std::env::var("FZ_RECALC_JOB_CHILD").ok().as_deref() == Some(full.as_str()) {
        return false;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &full, "--nocapture"])
        .env("FZ_RECALC_JOB_CHILD", &full)
        .status()
        .unwrap();
    assert!(status.success(), "isolated job fixture failed: {full}");
    true
}

fn fixture() -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            family_execution: false,
            enable_parallel: false,
            ..Default::default()
        },
    );
    for r in 1..=200 {
        let formula = if r == 1 {
            "=1".into()
        } else {
            format!("=A{}+1", r - 1)
        };
        e.set_cell_formula("Sheet1", r, 1, parse(&formula).unwrap())
            .unwrap();
    }
    e
}
#[test]
fn job_refuses_foreign_requests() {
    if isolated_test("job_refuses_foreign_requests") {
        return;
    }
    let mut e = fixture();
    e.begin_recalc(Default::default()).unwrap();
    assert!(
        e.evaluate_all().is_err(),
        "foreign evaluation must be refused"
    );
    assert!(e.has_active_recalc_job());
    assert!(e.cancel_recalc().is_some());
    e.evaluate_all().unwrap();
}
#[test]
fn job_reports_formula_progress_and_deltas() {
    if isolated_test("job_reports_formula_progress_and_deltas") {
        return;
    }
    let mut e = fixture();
    e.begin_recalc(Default::default()).unwrap();
    let mut computed = 0;
    let mut changed = 0;
    for _ in 0..210 {
        let report = e
            .step_recalc(EvaluationStop::after(Duration::ZERO))
            .unwrap();
        computed += report.step_computed_vertices;
        changed += report.changed.delta.records.len();
        assert!(report.progress.pass > 0);
        assert!(report.progress.completed_in_pass <= report.progress.scheduled_in_pass);
        if report.phase == StepPhase::Finished {
            assert_eq!(
                report.progress.completed_in_pass,
                report.progress.scheduled_in_pass
            );
            assert_eq!(computed, 200);
            assert!(changed > 0);
            assert_eq!(
                e.get_cell_value("Sheet1", 200, 1),
                Some(LiteralValue::Number(200.))
            );
            return;
        }
    }
    panic!("job did not finish");
}
#[test]
fn job_holds_schedule_across_steps() {
    if isolated_test("job_holds_schedule_across_steps") {
        return;
    }
    let mut e = fixture();
    let before = e.recalc_reuse_probe();
    e.begin_recalc(Default::default()).unwrap();
    while e.has_active_recalc_job() {
        e.step_recalc(EvaluationStop::after(Duration::ZERO))
            .unwrap();
    }
    let after = e.recalc_reuse_probe();
    assert_eq!(after.schedule_requests - before.schedule_requests, 1);
    assert_eq!(
        after.schedule_base_restrictions - before.schedule_base_restrictions,
        0
    );
}
