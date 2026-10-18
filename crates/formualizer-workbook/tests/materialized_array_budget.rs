//! One cell-count budget bounds every range or array that is copied into a
//! dense array (`evaluation_budgets.scratch.materialized_array_cells`).
//! Exceeding it is `#NUM!`; whole-column references are counted after they
//! are trimmed to the used region.
//!
//! The over-budget formulas here used to request tens or hundreds of
//! gigabytes and abort the process, so each runs on a worker thread with a
//! deadline; a regression shows up as an abort or a timeout.

use std::sync::mpsc;
use std::time::Duration;

use formualizer_common::ExcelErrorKind;
use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};

fn workbook(budget: Option<u64>) -> Workbook {
    let mut config = WorkbookConfig::interactive();
    config
        .eval
        .evaluation_budgets
        .scratch
        .materialized_array_cells = budget;
    let mut wb = Workbook::new_with_config(config);
    wb.add_sheet("S").unwrap();
    wb.add_sheet("T").unwrap();
    for r in 1..=3 {
        for c in 1..=3 {
            wb.set_value("S", r, c, LiteralValue::Number(f64::from(r * 10 + c)))
                .unwrap();
        }
    }
    wb
}

/// Value of the formula cell (T!A1; the data is in S!A1:C3) with a 20 s
/// deadline.
fn eval_with(budget: Option<u64>, formula: &str) -> LiteralValue {
    let formula = formula.to_string();
    let shown = formula.clone();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut wb = workbook(budget);
        wb.set_formula("T", 1, 1, &formula).unwrap();
        wb.evaluate_all().unwrap();
        let _ = tx.send(wb.get_value("T", 1, 1).unwrap_or(LiteralValue::Empty));
    });
    match rx.recv_timeout(Duration::from_secs(20)) {
        Ok(v) => v,
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("{shown}: still running after 20 s"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("{shown}: evaluation panicked"),
    }
}

fn assert_num(budget: Option<u64>, formula: &str) {
    match eval_with(budget, formula) {
        LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Num, "{formula}: {e:?}"),
        other => panic!("{formula}: expected #NUM!, got {other:?}"),
    }
}

const WHOLE_SHEET: &str = "S!A11:XFD1048576";

#[test]
fn whole_sheet_ranges_are_num_instead_of_aborting() {
    for f in [
        "LINEST({R})",
        "LOGEST({R})",
        "TREND({R})",
        "GROWTH({R})",
        "TRANSPOSE({R})",
        "SORT({R})",
        "SORTBY({R},{R})",
        "UNIQUE({R})",
        "FILTER({R},{R})",
        "SUMPRODUCT({R})",
        "SUM({R}*2)",
        "{R}*2",
        "COS({R})",
        "ATAN2({R},1)",
        "DROP({R},1)",
        "HSTACK({R})",
        "VSTACK({R})",
        "TOCOL({R})",
        "TOROW({R})",
        "LOOKUP(1,{R})",
        "IRR({R})",
        "MIRR({R},0.1,0.1)",
        "XIRR({R},{R})",
        "ARRAYTOTEXT({R})",
    ] {
        assert_num(None, &format!("={}", f.replace("{R}", WHOLE_SHEET)));
    }
}

#[test]
fn broadcast_of_a_column_and_a_row_is_bounded() {
    assert_num(None, "=S!A1:A1048576*S!B1:XFD1");
    assert_num(None, "=SUMPRODUCT(S!A1:A1048576,S!B1:XFD1)");
}

#[test]
fn a_dynamic_reference_to_a_whole_sheet_operand_is_bounded() {
    // The INDIRECT keeps the formula off the static path.
    match eval_with(None, "=INDIRECT(\"S!B1\")+SUM(S!A1:XFD1048576*2)") {
        LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Num, "{e:?}"),
        other => panic!("expected #NUM!, got {other:?}"),
    }
}

#[test]
fn whole_columns_over_a_small_used_range_are_not_counted_in_full() {
    // A:Z is 27 million cells untrimmed, past the default budget; the used
    // region is 3 x 3.
    assert_eq!(
        eval_with(None, "=INDEX(TRANSPOSE(S!A:Z),1,3)"),
        LiteralValue::Number(31.0)
    );
    assert_eq!(
        eval_with(None, "=SUM(S!A:Z*2)"),
        LiteralValue::Number(396.0)
    );
    assert_eq!(
        eval_with(None, "=INDEX(SORT(S!A:Z,1,-1),1,1)"),
        LiteralValue::Number(31.0)
    );
    assert_eq!(
        eval_with(None, "=ROWS(UNIQUE(S!A:C))"),
        LiteralValue::Number(3.0)
    );
}

#[test]
fn the_budget_is_configurable() {
    // 9 cells fit in a budget of 9 and not in a budget of 8.
    assert_eq!(
        eval_with(Some(9), "=SUM(S!A1:C3*2)"),
        LiteralValue::Number(396.0)
    );
    assert_num(Some(8), "=SUM(S!A1:C3*2)");
    assert_num(Some(8), "=TRANSPOSE(S!A1:C3)");
    assert_eq!(
        eval_with(Some(9), "=TRANSPOSE(S!A1:C3)"),
        LiteralValue::Number(11.0)
    );
    assert_num(Some(8), "=SEQUENCE(3,3)");
    assert_eq!(
        eval_with(Some(9), "=SEQUENCE(3,3)"),
        LiteralValue::Number(1.0)
    );
    assert_num(None, "=SEQUENCE(4097,4096)");
}
