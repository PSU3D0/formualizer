use formualizer_eval::engine::{CancelToken, EvaluationProgress, EvaluationStop};
use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};
use std::time::Duration;

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
