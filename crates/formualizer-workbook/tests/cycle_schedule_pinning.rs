//! Pinning tests for cycle-stamping result-equivalence (Stage 0 of the
//! cycle-evaluation work; pre-work for RFC #112).
//!
//! Stage 0 moves cycle stamping from "all cycles up-front" to "per-SCC at the
//! cycle's condensation position in the schedule". These tests pin the
//! observable contract: dependents of a cycle read the #CIRC-derived result
//! identically, and unrelated work alongside a cycle evaluates normally.

use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_workbook::Workbook;

fn is_circ(v: &Option<LiteralValue>) -> bool {
    matches!(v, Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Circ)
}

/// A1 <-> B1 cycle with downstream dependent C1 = A1 + 1, full recalc path.
/// C1 must observe the stamped #CIRC and propagate it.
#[test]
fn dependent_of_cycle_reads_circ_via_evaluate_all() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    wb.set_formula("S", 1, 1, "=B1").unwrap();
    wb.set_formula("S", 1, 2, "=A1").unwrap();
    wb.set_formula("S", 1, 3, "=A1+1").unwrap();
    wb.evaluate_all().unwrap();

    assert!(
        is_circ(&wb.get_value("S", 1, 1)),
        "A1 should be #CIRC, got {:?}",
        wb.get_value("S", 1, 1)
    );
    assert!(
        is_circ(&wb.get_value("S", 1, 2)),
        "B1 should be #CIRC, got {:?}",
        wb.get_value("S", 1, 2)
    );
    assert!(
        is_circ(&wb.get_value("S", 1, 3)),
        "C1 = A1+1 should propagate #CIRC, got {:?}",
        wb.get_value("S", 1, 3)
    );
}

/// Same shape, but evaluated through the demand-driven path (`evaluate_cell`),
/// which schedules the minimal subgraph with virtual deps.
#[test]
fn dependent_of_cycle_reads_circ_via_evaluate_cell() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    wb.set_formula("S", 1, 1, "=B1").unwrap();
    wb.set_formula("S", 1, 2, "=A1").unwrap();
    wb.set_formula("S", 1, 3, "=A1+1").unwrap();

    let c1 = wb.evaluate_cell("S", 1, 3).unwrap();
    assert!(
        is_circ(&Some(c1.clone())),
        "demand-driven C1 = A1+1 should propagate #CIRC, got {c1:?}"
    );
    assert!(
        is_circ(&wb.get_value("S", 1, 1)),
        "A1 should be #CIRC after demand eval, got {:?}",
        wb.get_value("S", 1, 1)
    );
    assert!(
        is_circ(&wb.get_value("S", 1, 2)),
        "B1 should be #CIRC after demand eval, got {:?}",
        wb.get_value("S", 1, 2)
    );
}

/// A cycle alongside an unrelated acyclic chain: the chain must evaluate to
/// normal values while the cycle members are stamped #CIRC.
#[test]
fn cycle_plus_unrelated_parallel_work_evaluates_correctly() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    // Cycle.
    wb.set_formula("S", 1, 1, "=B1").unwrap();
    wb.set_formula("S", 1, 2, "=A1").unwrap();
    // Unrelated chain.
    wb.set_formula("S", 1, 4, "=1+1").unwrap();
    wb.set_formula("S", 1, 5, "=D1*2").unwrap();
    wb.evaluate_all().unwrap();

    assert!(is_circ(&wb.get_value("S", 1, 1)));
    assert!(is_circ(&wb.get_value("S", 1, 2)));
    assert_eq!(wb.get_value("S", 1, 4), Some(LiteralValue::Number(2.0)));
    assert_eq!(wb.get_value("S", 1, 5), Some(LiteralValue::Number(4.0)));
}
