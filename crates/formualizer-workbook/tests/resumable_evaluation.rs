use formualizer_eval::engine::{CancelToken, EvaluationProgress, EvaluationStop};
use formualizer_eval::engine::{RecalcJobError, RecalcOutcome, StepPhase};
use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};
use std::ops::ControlFlow;
use std::time::Duration;

#[test]
fn workbook_job_types_and_errors() {
    fn send<T: Send>() {}
    send::<Workbook>();
    let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
    assert!(matches!(
        wb.step_recalc(EvaluationStop::none()),
        Err(RecalcJobError::NoActiveJob)
    ));
    assert!(matches!(
        wb.prioritize_recalc(&[]),
        Err(RecalcJobError::NoActiveJob)
    ));
    wb.begin_recalc(Default::default()).unwrap();
    assert!(matches!(
        wb.begin_recalc(Default::default()),
        Err(RecalcJobError::AlreadyPending { explicit: true })
    ));
    wb.cancel_recalc();
}
#[test]
fn workbook_closure_receives_workbook_and_can_pause_edit_prioritize() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
    wb.add_sheet("S").unwrap();
    wb.set_formula("S", 1, 1, "=1").unwrap();
    wb.set_formula("S", 2, 1, "=A1+1").unwrap();
    let outcome = wb
        .recalc_with(
            Default::default(),
            EvaluationStop::after(Duration::ZERO),
            |wb, report| {
                assert_eq!(report.phase, StepPhase::Scheduling);
                wb.set_value("S", 1, 1, LiteralValue::Number(10.)).unwrap();
                wb.prioritize_recalc(&[formualizer_eval::engine::EvaluationTarget::Cell {
                    sheet: "S".into(),
                    row: 2,
                    col: 1,
                }])
                .unwrap();
                ControlFlow::Break(())
            },
        )
        .unwrap();
    assert!(matches!(outcome, RecalcOutcome::Paused));
    assert!(wb.has_active_recalc_job());
    assert_eq!(
        wb.step_recalc(EvaluationStop::none()).unwrap().phase,
        StepPhase::Finished
    );
    assert_eq!(wb.get_value("S", 2, 1), Some(LiteralValue::Number(11.)));
    assert!(matches!(
        wb.recalc_with(Default::default(), EvaluationStop::none(), |_, _| {
            ControlFlow::Break(())
        })
        .unwrap(),
        RecalcOutcome::Finished(_)
    ));
}
#[test]
fn workbook_closure_cancel_is_terminal() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
    wb.add_sheet("S").unwrap();
    wb.set_formula("S", 1, 1, "=1").unwrap();
    assert!(matches!(
        wb.recalc_with(
            Default::default(),
            EvaluationStop::after(Duration::ZERO),
            |wb, _| {
                wb.cancel_recalc().unwrap();
                ControlFlow::Break(())
            }
        )
        .unwrap(),
        RecalcOutcome::Cancelled
    ));
    assert!(!wb.has_active_recalc_job());
}

#[test]
fn workbook_resumable_passthrough_retains_installed_values() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
    wb.add_sheet("S").unwrap();
    wb.set_formula("S", 1, 1, "=1").unwrap();
    for r in 2..=20 {
        wb.set_formula("S", r, 1, &format!("=A{}+1", r - 1))
            .unwrap();
    }
    assert!(matches!(
        wb.evaluate_all_until(EvaluationStop::after(Duration::ZERO))
            .unwrap(),
        EvaluationProgress::Incomplete(_)
    ));
    assert!(wb.engine().has_pending_resumable_evaluation());
    for _ in 0..20 {
        if matches!(
            wb.evaluate_all_until(EvaluationStop::after(Duration::ZERO))
                .unwrap(),
            EvaluationProgress::Complete(_)
        ) {
            assert_eq!(wb.get_value("S", 20, 1), Some(LiteralValue::Number(20.)));
            return;
        }
    }
    panic!("did not finish");
}

#[test]
fn workbook_job_lifecycle_passthrough() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
    wb.add_sheet("S").unwrap();
    wb.set_formula("S", 1, 1, "=42").unwrap();
    wb.begin_recalc(Default::default()).unwrap();
    assert!(wb.has_active_recalc_job());
    assert!(wb.evaluate_all().is_err());
    wb.prioritize_recalc(&[formualizer_eval::engine::EvaluationTarget::Cell {
        sheet: "S".into(),
        row: 1,
        col: 1,
    }])
    .unwrap();
    loop {
        let r = wb
            .step_recalc(EvaluationStop::after(Duration::ZERO))
            .unwrap();
        if r.phase == formualizer_eval::engine::StepPhase::Finished {
            break;
        }
    }
    assert!(!wb.has_active_recalc_job());
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(42.)));
    wb.begin_recalc(Default::default()).unwrap();
    assert_eq!(
        wb.cancel_recalc().unwrap().phase,
        formualizer_eval::engine::StepPhase::Cancelled
    );
    wb.evaluate_all().unwrap();
}

#[test]
fn workbook_resumable_precancel_is_incomplete() {
    let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
    wb.add_sheet("S").unwrap();
    wb.set_formula("S", 1, 1, "=42").unwrap();
    let token = CancelToken::new();
    token.cancel();
    assert!(matches!(
        wb.evaluate_all_until(EvaluationStop::cancel(token))
            .unwrap(),
        EvaluationProgress::Incomplete(_)
    ));
    assert!(matches!(
        wb.evaluate_all_until(EvaluationStop::none()).unwrap(),
        EvaluationProgress::Complete(_)
    ));
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(42.)));
}
