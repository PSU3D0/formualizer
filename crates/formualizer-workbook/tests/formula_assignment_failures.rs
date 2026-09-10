use formualizer_common::LiteralValue;
use formualizer_workbook::{Workbook, WorkbookConfig};

fn workbook(logged: bool, deferred: bool) -> Workbook {
    let mut cfg = WorkbookConfig::interactive();
    cfg.eval.defer_graph_building = deferred;
    let mut wb = Workbook::new_with_config(cfg);
    wb.set_changelog_enabled(logged);
    wb.add_sheet("S").unwrap();
    wb
}

#[test]
fn rejected_existing_formula_assignment_reports_error_without_history_or_value_change() {
    for logged in [false, true] {
        for deferred in [false, true] {
            for formula in ["=NOSHEET!A1", "=SUM(MissingTable[Amount])"] {
                let mut wb = workbook(logged, deferred);
                wb.set_value("S", 1, 1, LiteralValue::Text("sentinel".into()))
                    .unwrap();
                let events = wb.changelog().events().len();
                assert!(
                    wb.set_formula("S", 1, 1, formula).is_err(),
                    "logged={logged} deferred={deferred} formula={formula}"
                );
                assert_eq!(
                    wb.get_value("S", 1, 1),
                    Some(LiteralValue::Text("sentinel".into()))
                );
                assert_eq!(wb.get_formula("S", 1, 1), None);
                assert_eq!(wb.changelog().events().len(), events);
                wb.set_formula("S", 1, 1, "=1+2").unwrap();
                assert_eq!(
                    wb.evaluate_cell("S", 1, 1).unwrap(),
                    LiteralValue::Number(3.0)
                );
            }
        }
    }
}

#[test]
fn rejected_new_graph_assignment_leaves_no_formula_or_history() {
    for logged in [false, true] {
        let mut wb = workbook(logged, false);
        let events = wb.changelog().events().len();
        assert!(wb.set_formula("S", 1, 1, "=NOSHEET!A1").is_err());
        assert_eq!(wb.get_formula("S", 1, 1), None);
        assert_eq!(wb.changelog().events().len(), events);
    }
}

#[test]
fn logged_assignment_propagates_resource_admission_rejection() {
    let mut cfg = WorkbookConfig::interactive();
    cfg.eval.defer_graph_building = false;
    cfg.eval.evaluation_budgets.admission.graph_edge_hard_limit = Some(0);
    let mut wb = Workbook::new_with_config(cfg);
    wb.add_sheet("S").unwrap();
    wb.set_value("S", 1, 1, LiteralValue::Number(7.0)).unwrap();
    let events = wb.changelog().events().len();
    let error = wb.set_formula("S", 1, 1, "=Z99").unwrap_err();
    assert!(
        matches!(error, formualizer_workbook::IoError::Engine(ref error)
        if matches!(error.extra, formualizer_common::ExcelErrorExtra::Resource { .. }))
    );
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(7.0)));
    assert_eq!(wb.get_formula("S", 1, 1), None);
    assert_eq!(wb.changelog().events().len(), events);
}

#[test]
fn rejected_spill_anchor_assignment_retains_formula_spill_and_history() {
    let mut wb = workbook(true, false);
    wb.set_formula("S", 1, 1, "=SEQUENCE(2,1)").unwrap();
    wb.evaluate_all().unwrap();
    let formula = wb.get_formula("S", 1, 1);
    let events = wb.changelog().events().len();
    assert!(wb.set_formula("S", 1, 1, "=NOSHEET!A1").is_err());
    assert_eq!(wb.get_formula("S", 1, 1), formula);
    assert_eq!(wb.get_value("S", 2, 1), Some(LiteralValue::Number(2.0)));
    assert_eq!(wb.changelog().events().len(), events);
    wb.set_formula("S", 1, 1, "=7").unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 1, 1).unwrap(),
        LiteralValue::Number(7.0)
    );
    wb.undo().unwrap();
    assert_eq!(wb.get_formula("S", 1, 1), formula);
    assert_eq!(wb.get_value("S", 2, 1), Some(LiteralValue::Number(2.0)));
}

#[test]
fn graph_batch_reports_binding_error_and_retains_existing_prefix_commit_policy() {
    let mut wb = workbook(true, false);
    wb.set_value("S", 1, 2, LiteralValue::Number(9.0)).unwrap();
    assert!(
        wb.set_formulas(
            "S",
            1,
            1,
            &[vec!["=1+2".into(), "=NOSHEET!A1".into(), "=4+5".into()]]
        )
        .is_err()
    );
    assert_eq!(
        wb.evaluate_cell("S", 1, 1).unwrap(),
        LiteralValue::Number(3.0)
    );
    assert_eq!(wb.get_value("S", 1, 2), Some(LiteralValue::Number(9.0)));
    assert_eq!(wb.get_formula("S", 1, 2), None);
    assert_eq!(wb.get_formula("S", 1, 3), None);
    // The deferred-dirty scope must close even when the loop returns an error.
    wb.set_formula("S", 2, 1, "=A1*2").unwrap();
    assert_eq!(
        wb.evaluate_cell("S", 2, 1).unwrap(),
        LiteralValue::Number(6.0)
    );
}
