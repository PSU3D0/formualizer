//! #454 decision: spreadsheet guards do not change preparation/request policy.
use formualizer_common::{ExcelErrorExtra, ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::{CancelToken, PreparationPolicy};
use formualizer_workbook::{IoError, Workbook, WorkbookConfig};

fn workbook() -> Workbook {
    workbook_with(PreparationPolicy::default())
}

fn workbook_with(policy: PreparationPolicy) -> Workbook {
    let mut cfg = WorkbookConfig::ephemeral();
    cfg.eval.defer_graph_building = true;
    cfg.eval.preparation_policy = policy;
    let mut workbook = Workbook::new_with_config(cfg);
    workbook.add_sheet("S").unwrap();
    workbook
}

#[test]
fn guards_handle_cell_errors_without_reclassifying_reference_preparation() {
    for (formula, expected) in [
        ("=IFERROR(#REF!,456)", 456.0),
        ("=IFNA(#N/A,456)", 456.0),
        ("=IFERROR(SUM(IFERROR(#REF!,0)),1)", 0.0),
        ("=IFERROR(7,1/0)", 7.0),
    ] {
        let mut wb = workbook();
        wb.set_formula("S", 1, 1, formula).unwrap();
        assert_eq!(
            wb.evaluate_cell("S", 1, 1).unwrap(),
            LiteralValue::Number(expected),
            "{formula}"
        );
    }
    let mut wb = workbook();
    wb.set_formula("S", 1, 1, "=IFNA(#REF!,456)").unwrap();
    assert!(matches!(
        wb.evaluate_cell("S", 1, 1).unwrap(),
        LiteralValue::Error(error) if error.kind == ExcelErrorKind::Ref
    ));
}

#[test]
fn unresolved_reference_preparation_still_aborts_before_guards_run() {
    // Strict (explicit since 0.10's BestEffort default) keeps #454's boundary.
    for formula in [
        "=IFERROR(NOSHEET!A1,456)",
        "=IFNA(NOSHEET!A1,456)",
        "=IFERROR(SUM(IFERROR(NOSHEET!A1,0)),1)",
        // Runtime laziness does not promise branch-lazy dependency binding.
        "=IFERROR(7,NOSHEET!A1)",
    ] {
        for targeted in [false, true] {
            let mut wb = workbook_with(PreparationPolicy::Strict);
            wb.engine_mut()
                .stage_formula_text("S", 1, 1, formula.into());
            let result = if targeted {
                wb.evaluate_cell("S", 1, 1).map(|_| ())
            } else {
                wb.evaluate_all().map(|_| ())
            };
            assert!(matches!(result, Err(IoError::Engine(_))), "{formula}");
        }
    }
}

/// BestEffort (the default since 0.10): the unbound reference is accepted at
/// preparation and evaluates to an error value, which guards see like any
/// other cell error.
#[test]
fn best_effort_unbound_reference_is_a_cell_error_that_guards_catch() {
    for (formula, expected) in [
        ("=IFERROR(NOSHEET!A1,456)", 456.0),
        ("=IFERROR(SUM(IFERROR(NOSHEET!A1,0)),1)", 0.0),
        ("=IFERROR(7,NOSHEET!A1)", 7.0),
        ("=IFERROR(SUM(MissingTable[Amount]),456)", 456.0),
    ] {
        for targeted in [false, true] {
            let mut wb = workbook();
            wb.engine_mut()
                .stage_formula_text("S", 1, 1, formula.into());
            let value = if targeted {
                wb.evaluate_cell("S", 1, 1).unwrap()
            } else {
                wb.evaluate_all().unwrap();
                wb.get_value("S", 1, 1).unwrap()
            };
            assert_eq!(value, LiteralValue::Number(expected), "{formula}");
        }
    }
    let mut wb = workbook();
    wb.engine_mut()
        .stage_formula_text("S", 1, 1, "=NOSHEET!A1".into());
    wb.evaluate_all().unwrap();
    assert!(matches!(
        wb.get_value("S", 1, 1),
        Some(LiteralValue::Error(_))
    ));
    // The sheet appearing later re-binds the formula.
    wb.add_sheet("NOSHEET").unwrap();
    wb.set_value("NOSHEET", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(5.0)));
}

#[test]
fn guards_do_not_turn_preparation_admission_failure_into_a_value() {
    let mut cfg = WorkbookConfig::ephemeral();
    cfg.eval.defer_graph_building = true;
    cfg.eval.evaluation_budgets.admission.graph_edge_hard_limit = Some(0);
    let mut wb = Workbook::new_with_config(cfg);
    wb.add_sheet("S").unwrap();
    wb.engine_mut()
        .stage_formula_text("S", 1, 1, "=IFERROR(Z99,456)".into());
    let error = wb.evaluate_all().unwrap_err();
    assert!(matches!(
        error,
        IoError::Engine(error) if matches!(error.extra, ExcelErrorExtra::Resource { .. })
    ));
}

#[test]
fn guards_do_not_hide_request_cancellation() {
    let mut wb = workbook();
    wb.set_formula("S", 1, 1, "=IFERROR(1/0,456)").unwrap();
    let cancel = CancelToken::new();
    cancel.cancel();
    let error = wb
        .engine_mut()
        .evaluate_all_cancellable(cancel)
        .unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
}
