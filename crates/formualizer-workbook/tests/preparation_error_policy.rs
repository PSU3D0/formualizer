//! #454 decision: spreadsheet guards do not change preparation/request policy.
use formualizer_common::{ExcelErrorExtra, ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::CancelToken;
use formualizer_workbook::{IoError, Workbook, WorkbookConfig};

fn workbook() -> Workbook {
    let mut cfg = WorkbookConfig::ephemeral();
    cfg.eval.defer_graph_building = true;
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
    for formula in [
        "=IFERROR(NOSHEET!A1,456)",
        "=IFNA(NOSHEET!A1,456)",
        "=IFERROR(SUM(IFERROR(NOSHEET!A1,0)),1)",
        // Runtime laziness does not promise branch-lazy dependency binding.
        "=IFERROR(7,NOSHEET!A1)",
    ] {
        for targeted in [false, true] {
            let mut wb = workbook();
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
