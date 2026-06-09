//! Regression tests for issue #111.
//!
//! When cycle detection stamps a spill anchor (a `FormulaArray` vertex) with
//! `#CIRC!`, the engine must tear down the previous spill projection and release
//! the region reservation. Before the fix the `#CIRC!` value was written directly
//! to the anchor while the spilled cells (and the graph's spill registry) were left
//! untouched, leaving stale values behind and blocking new spills into the region.

use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_workbook::Workbook;

fn is_circ(v: &Option<LiteralValue>) -> bool {
    matches!(v, Some(LiteralValue::Error(e)) if e.kind == ExcelErrorKind::Circ)
}

fn is_empty(v: &Option<LiteralValue>) -> bool {
    matches!(v, None | Some(LiteralValue::Empty))
}

/// Drive a spilling anchor into a dependency cycle, then re-evaluate via the full
/// `evaluate_all` legacy pass.
///
/// A1 first spills `=SEQUENCE(3)` (A1:A3 = 1,2,3), then is switched to
/// `=SEQUENCE(C1)` (still A1:A3) while C1 holds the constant 3. Finally C1 becomes
/// `=A1+1`, forming the cycle A1 -> C1 -> A1. Cycle detection stamps A1 (and C1)
/// with `#CIRC!`; A1's former spill (A2, A3) must be cleared and the region freed.
#[test]
fn circ_on_spill_anchor_tears_down_projection_and_region() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    // Prime: A1 spills a fixed SEQUENCE(3) -> A1:A3 = 1,2,3.
    wb.set_formula("S", 1, 1, "=SEQUENCE(3)").unwrap();
    wb.evaluate_all().unwrap();

    // Re-point the anchor at C1 (a constant) so it still spills 3 rows.
    wb.set_value("S", 1, 3, LiteralValue::Number(3.0)).unwrap();
    wb.set_formula("S", 1, 1, "=SEQUENCE(C1)").unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(wb.get_value("S", 1, 1), Some(LiteralValue::Number(1.0)));
    assert_eq!(wb.get_value("S", 2, 1), Some(LiteralValue::Number(2.0)));
    assert_eq!(wb.get_value("S", 3, 1), Some(LiteralValue::Number(3.0)));

    // Introduce the cycle: C1 = A1 + 1  =>  A1 -> C1 -> A1.
    wb.set_formula("S", 1, 3, "=A1+1").unwrap();
    wb.evaluate_all().unwrap();

    // Anchor is #CIRC.
    assert!(
        is_circ(&wb.get_value("S", 1, 1)),
        "anchor A1 should be #CIRC, got {:?}",
        wb.get_value("S", 1, 1)
    );

    // Formerly-spilled cells must be cleared (not stale 2.0 / 3.0).
    assert!(
        is_empty(&wb.get_value("S", 2, 1)),
        "spilled A2 should be cleared, got {:?}",
        wb.get_value("S", 2, 1)
    );
    assert!(
        is_empty(&wb.get_value("S", 3, 1)),
        "spilled A3 should be cleared, got {:?}",
        wb.get_value("S", 3, 1)
    );

    // Region-lock release proof: a NEW unrelated spill into the formerly-reserved
    // region must succeed. A2 anchors SEQUENCE(2) -> A2:A3 = 1,2.
    wb.set_formula("S", 2, 1, "=SEQUENCE(2)").unwrap();
    wb.evaluate_all().unwrap();

    assert_eq!(
        wb.get_value("S", 2, 1),
        Some(LiteralValue::Number(1.0)),
        "new spill anchor A2 should spill (region must be free); got {:?}",
        wb.get_value("S", 2, 1)
    );
    assert_eq!(
        wb.get_value("S", 3, 1),
        Some(LiteralValue::Number(2.0)),
        "new spill should fill A3; got {:?}",
        wb.get_value("S", 3, 1)
    );
}

/// Same scenario but the cycle-introducing edit is recalculated through the
/// targeted, delta-collecting path (`evaluate_cell`) instead of `evaluate_all`,
/// exercising a different cycle-stamping site.
#[test]
fn circ_on_spill_anchor_tears_down_via_targeted_recalc() {
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    wb.set_formula("S", 1, 1, "=SEQUENCE(3)").unwrap();
    wb.evaluate_all().unwrap();
    wb.set_value("S", 1, 3, LiteralValue::Number(3.0)).unwrap();
    wb.set_formula("S", 1, 1, "=SEQUENCE(C1)").unwrap();
    wb.evaluate_all().unwrap();
    assert_eq!(wb.get_value("S", 2, 1), Some(LiteralValue::Number(2.0)));
    assert_eq!(wb.get_value("S", 3, 1), Some(LiteralValue::Number(3.0)));

    // Edit C1 into the cycle and recalc through the demand-driven path.
    wb.set_formula("S", 1, 3, "=A1+1").unwrap();
    let _ = wb.evaluate_cell("S", 1, 1).unwrap();

    assert!(
        is_circ(&wb.get_value("S", 1, 1)),
        "anchor A1 should be #CIRC, got {:?}",
        wb.get_value("S", 1, 1)
    );
    assert!(
        is_empty(&wb.get_value("S", 2, 1)),
        "spilled A2 should be cleared, got {:?}",
        wb.get_value("S", 2, 1)
    );
    assert!(
        is_empty(&wb.get_value("S", 3, 1)),
        "spilled A3 should be cleared, got {:?}",
        wb.get_value("S", 3, 1)
    );
}
