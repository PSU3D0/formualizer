use super::*;

#[test]
fn no_spill_action_history_cost_is_linear() {
    let mut workbook = Workbook::new();
    workbook.add_sheet("S").unwrap();
    let n = 32_000;
    workbook
        .set_formulas("S", 1, 1, &vec![vec!["=0".into()]; n as usize])
        .unwrap();
    workbook.begin_action("ordinary formulas");
    for row in 1..=n {
        workbook.set_formula("S", row, 1, "=1+1").unwrap();
        // Exercise the shared batch decision repeatedly with no new events.
        assert!(!workbook.formula_needs_spill_history("S", row, 1));
    }
    let history = &workbook.spill_action_history;
    assert_eq!(history.events_inspected, 0);
    assert!(history.cells.is_empty());
    workbook.end_action();
    workbook.undo().unwrap();
    assert_eq!(workbook.get_formula("S", n, 1), Some("=0".into()));
    workbook.redo().unwrap();
    assert_eq!(workbook.get_formula("S", n, 1), Some("=1+1".into()));

    let inspected = workbook.spill_action_history.events_inspected;
    workbook.begin_action("ordinary batch");
    workbook
        .set_formulas("S", 1, 1, &vec![vec!["=2+2".into()]; n as usize])
        .unwrap();
    // All N decisions skip history entirely when no spill was cleared.
    assert_eq!(
        workbook.spill_action_history.events_inspected - inspected,
        0
    );
    workbook.end_action();
}

#[test]
fn spill_history_survives_nested_actions_and_resets_between_actions() {
    let mut workbook = Workbook::new();
    workbook.add_sheet("S").unwrap();
    workbook.set_formula("S", 1, 1, "=SEQUENCE(5)").unwrap();
    workbook.evaluate_all().unwrap();
    workbook.begin_action("outer");
    workbook.begin_action("inner");
    workbook
        .set_value("S", 3, 1, LiteralValue::Number(99.0))
        .unwrap();
    assert!(workbook.formula_needs_spill_history("S", 4, 1));
    workbook.end_action();
    assert!(workbook.formula_needs_spill_history("S", 5, 1));
    workbook.end_action();
    assert!(workbook.spill_action_history.cells.is_empty());
    workbook.begin_action("fresh");
    assert!(!workbook.formula_needs_spill_history("S", 4, 1));
    workbook.end_action();
}
