//! A dynamic-array spill whose target holds another formula (a scalar
//! formula or another spill anchor) is blocked: the anchor becomes
//! `#SPILL!` (with its expected extent), the blocking formula evaluates
//! normally, and evaluation completes. Policy follows Excel's documented
//! semantics (no Excel oracle in this suite).
//!
//! Every scenario runs across the evaluation entry points (plain, delta and
//! cancellable, which take different layer paths), sequential and parallel
//! configs, and `family_execution` on and off.

use crate::engine::{CancelToken, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorExtra, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

#[derive(Clone, Copy, Debug)]
enum Entry {
    Plain,
    Delta,
    Cancellable,
}

#[derive(Clone, Copy, Debug)]
struct Mode {
    entry: Entry,
    parallel: bool,
    family: bool,
}

fn modes() -> Vec<Mode> {
    let mut out = Vec::new();
    for entry in [Entry::Plain, Entry::Delta, Entry::Cancellable] {
        for parallel in [false, true] {
            for family in [true, false] {
                out.push(Mode {
                    entry,
                    parallel,
                    family,
                });
            }
        }
    }
    out
}

fn engine(mode: Mode) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: mode.parallel,
            max_threads: if mode.parallel { Some(4) } else { None },
            family_execution: mode.family,
            ..Default::default()
        },
    )
}

fn eval(e: &mut Engine<TestWorkbook>, mode: Mode) {
    let result = match mode.entry {
        Entry::Plain => e.evaluate_all().map(|_| ()),
        Entry::Delta => e.evaluate_all_with_delta().map(|_| ()),
        Entry::Cancellable => e.evaluate_all_cancellable(CancelToken::new()).map(|_| ()),
    };
    if let Err(err) = result {
        panic!("{mode:?}: evaluation aborted: {err:?}");
    }
}

fn f(e: &mut Engine<TestWorkbook>, row: u32, col: u32, src: &str) {
    e.set_cell_formula("Sheet1", row, col, parse(src).unwrap())
        .unwrap();
}

fn get(e: &Engine<TestWorkbook>, row: u32, col: u32) -> Option<LiteralValue> {
    match e.get_cell_value("Sheet1", row, col) {
        Some(LiteralValue::Int(i)) => Some(LiteralValue::Number(i as f64)),
        Some(LiteralValue::Empty) | None => None,
        other => other,
    }
}

fn n(x: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(x))
}

#[track_caller]
fn assert_kind(value: Option<LiteralValue>, kind: ExcelErrorKind, ctx: &str) {
    match value {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, kind, "{ctx}: {e:?}"),
        other => panic!("{ctx}: expected {kind:?}, got {other:?}"),
    }
}

/// The value a value-blocked `=SEQUENCE(rows)` anchor holds in this mode:
/// the existing blocked-anchor encoding a formula-blocked anchor must match.
fn value_blocked_anchor(mode: Mode, rows: u32) -> Option<LiteralValue> {
    let mut e = engine(mode);
    e.set_cell_value("Sheet1", rows, 1, LiteralValue::Number(99.0))
        .unwrap();
    f(&mut e, 1, 1, &format!("=SEQUENCE({rows})"));
    eval(&mut e, mode);
    get(&e, 1, 1)
}

/// The anchor holds `#SPILL!`, encoded exactly as a value-blocked anchor of
/// the same attempted extent; where the error keeps its extent, it is the
/// attempted one.
#[track_caller]
fn assert_blocked(mode: Mode, value: Option<LiteralValue>, rows: u32, ctx: &str) {
    match &value {
        Some(LiteralValue::Error(e)) => {
            assert_eq!(e.kind, ExcelErrorKind::Spill, "{ctx}: {e:?}");
            if let ExcelErrorExtra::Spill {
                expected_rows,
                expected_cols,
            } = e.extra
            {
                assert_eq!((expected_rows, expected_cols), (rows, 1), "{ctx}");
            }
        }
        other => panic!("{ctx}: expected #SPILL!, got {other:?}"),
    }
    assert_eq!(value, value_blocked_anchor(mode, rows), "{ctx}: encoding");
}

fn has_spill(e: &Engine<TestWorkbook>, row: u32, col: u32) -> bool {
    let cell = e.graph.make_cell_ref("Sheet1", row, col);
    let vid = e.graph.get_vertex_for_cell(&cell).expect("anchor vertex");
    e.graph
        .spill_cells_for_anchor(vid)
        .is_some_and(|cells| !cells.is_empty())
}

/// A1 `=SEQUENCE(3)` with a scalar formula in A3, defined in both orders,
/// plus a dependent and spill-reference readers of the anchor.
#[test]
fn scalar_formula_blocker_yields_spill_error() {
    for mode in modes() {
        for anchor_first in [true, false] {
            let ctx = format!("{mode:?} anchor_first={anchor_first}");
            let mut e = engine(mode);
            if anchor_first {
                f(&mut e, 1, 1, "=SEQUENCE(3)");
                f(&mut e, 3, 1, "=1+1");
            } else {
                f(&mut e, 3, 1, "=1+1");
                f(&mut e, 1, 1, "=SEQUENCE(3)");
            }
            // Independent same-layer work, so parallel configs have a
            // multi-vertex layer to split.
            for r in 1..=8 {
                f(&mut e, r, 5, &format!("={r}*2"));
            }
            f(&mut e, 1, 2, "=A1+0");
            f(&mut e, 1, 3, "=SUM(A1#)");
            eval(&mut e, mode);

            assert_blocked(mode, get(&e, 1, 1), 3, &ctx);
            assert_eq!(get(&e, 2, 1), None, "{ctx}: no spill child");
            assert_eq!(get(&e, 3, 1), n(2.0), "{ctx}: blocker evaluates");
            assert_kind(get(&e, 1, 2), ExcelErrorKind::Spill, &ctx);
            assert_kind(get(&e, 1, 3), ExcelErrorKind::Ref, &ctx);
            assert_eq!(get(&e, 8, 5), n(16.0), "{ctx}");
            assert!(!has_spill(&e, 1, 1), "{ctx}: no spill registered");
        }
    }
}

/// A1 `=SEQUENCE(3)` and A2 `=SEQUENCE(2)`: A2's anchor is a formula in
/// A1's target, so A1 is blocked and A2 spills, whatever the definition
/// order.
#[test]
fn anchor_inside_another_spill_target_blocks_the_outer_anchor() {
    for mode in modes() {
        for outer_first in [true, false] {
            let ctx = format!("{mode:?} outer_first={outer_first}");
            let mut e = engine(mode);
            if outer_first {
                f(&mut e, 1, 1, "=SEQUENCE(3)");
                f(&mut e, 2, 1, "=SEQUENCE(2)");
            } else {
                f(&mut e, 2, 1, "=SEQUENCE(2)");
                f(&mut e, 1, 1, "=SEQUENCE(3)");
            }
            for r in 1..=8 {
                f(&mut e, r, 5, &format!("={r}*2"));
            }
            eval(&mut e, mode);

            assert_blocked(mode, get(&e, 1, 1), 3, &ctx);
            assert_eq!(get(&e, 2, 1), n(1.0), "{ctx}");
            assert_eq!(get(&e, 3, 1), n(2.0), "{ctx}");
            assert!(!has_spill(&e, 1, 1), "{ctx}");
            assert!(has_spill(&e, 2, 1), "{ctx}");
        }
    }
}

/// A column of same-template anchors, each blocked by the next one. Family
/// execution groups them into one run.
#[test]
fn family_of_anchors_each_blocked_by_the_next() {
    for mode in modes() {
        let ctx = format!("{mode:?}");
        let mut e = engine(mode);
        for r in 1..=12 {
            f(&mut e, r, 2, "=SEQUENCE(2)");
        }
        eval(&mut e, mode);
        for r in 1..=11 {
            assert_blocked(mode, get(&e, r, 2), 2, &format!("{ctx} B{r}"));
        }
        assert_eq!(get(&e, 12, 2), n(1.0), "{ctx}");
        assert_eq!(get(&e, 13, 2), n(2.0), "{ctx}");
    }
}

/// Edits: a formula placed into a live spill blocks it; removing the
/// blocker lets the anchor spill again on the next recalc; shrinking the
/// anchor so the spill fits also re-spills.
#[test]
fn formula_blocker_edits_block_and_release_the_spill() {
    for mode in modes() {
        let ctx = format!("{mode:?}");
        let mut e = engine(mode);
        e.set_cell_value("Sheet1", 1, 4, LiteralValue::Number(3.0))
            .unwrap();
        f(&mut e, 1, 1, "=SEQUENCE(D1)");
        f(&mut e, 1, 2, "=A1+0");
        f(&mut e, 1, 3, "=SUM(A1#)");
        eval(&mut e, mode);
        assert_eq!(get(&e, 3, 1), n(3.0), "{ctx}: initial spill");
        assert_eq!(get(&e, 1, 3), n(6.0), "{ctx}");

        // A formula lands inside the live spill.
        f(&mut e, 3, 1, "=1+1");
        eval(&mut e, mode);
        assert_blocked(mode, get(&e, 1, 1), 3, &format!("{ctx} blocked by edit"));
        assert_eq!(get(&e, 2, 1), None, "{ctx}: spill cleared");
        assert_eq!(get(&e, 3, 1), n(2.0), "{ctx}: blocker evaluates");
        assert_kind(get(&e, 1, 2), ExcelErrorKind::Spill, &ctx);
        assert_kind(get(&e, 1, 3), ExcelErrorKind::Ref, &ctx);

        // Removing the blocker: the anchor re-spills on the next recalc.
        e.set_cell_value("Sheet1", 3, 1, LiteralValue::Empty)
            .unwrap();
        eval(&mut e, mode);
        assert_eq!(get(&e, 1, 1), n(1.0), "{ctx}: re-spilled");
        assert_eq!(get(&e, 3, 1), n(3.0), "{ctx}: re-spilled");
        assert_eq!(get(&e, 1, 2), n(1.0), "{ctx}: dependent follows");
        assert_eq!(get(&e, 1, 3), n(6.0), "{ctx}: reader follows");

        // Block again, then shrink the anchor so the spill fits.
        f(&mut e, 3, 1, "=1+1");
        eval(&mut e, mode);
        assert_blocked(mode, get(&e, 1, 1), 3, &format!("{ctx} re-blocked"));
        e.set_cell_value("Sheet1", 1, 4, LiteralValue::Number(2.0))
            .unwrap();
        eval(&mut e, mode);
        assert_eq!(get(&e, 1, 1), n(1.0), "{ctx}: fits");
        assert_eq!(get(&e, 2, 1), n(2.0), "{ctx}: fits");
        assert_eq!(get(&e, 3, 1), n(2.0), "{ctx}: blocker unchanged");
        assert_eq!(get(&e, 1, 3), n(3.0), "{ctx}: reader follows");
    }
}

/// Moving the blocker's formula to a value outside the target releases the
/// spill too (the blocker is changed rather than cleared).
#[test]
fn formula_blocker_replaced_by_formula_elsewhere_releases_the_spill() {
    for mode in modes() {
        let ctx = format!("{mode:?}");
        let mut e = engine(mode);
        f(&mut e, 3, 1, "=1+1");
        f(&mut e, 1, 1, "=SEQUENCE(3)");
        eval(&mut e, mode);
        assert_blocked(mode, get(&e, 1, 1), 3, &ctx);

        e.set_cell_value("Sheet1", 3, 1, LiteralValue::Empty)
            .unwrap();
        f(&mut e, 4, 1, "=1+1");
        eval(&mut e, mode);
        assert_eq!(get(&e, 3, 1), n(3.0), "{ctx}: re-spilled");
        assert_eq!(get(&e, 4, 1), n(2.0), "{ctx}");
    }
}

/// Values and spill ownership (as the owning anchor's position) of A1:F6:
/// what must not depend on evaluation or edit history.
type Snapshot = Vec<(Option<LiteralValue>, Option<(u32, u32)>)>;

fn snapshot(e: &Engine<TestWorkbook>) -> Snapshot {
    let mut out = Vec::new();
    for row in 1..=6 {
        for col in 1..=6 {
            let cell = e.graph.make_cell_ref("Sheet1", row, col);
            let owner = e.graph.spill_registry_anchor_for_cell(cell).map(|vid| {
                let anchor = e.graph.get_cell_ref(vid).expect("owner cell");
                // Every claim belongs to a well-formed extent.
                let (first, last) = e
                    .graph
                    .spill_extent_for_anchor(vid)
                    .expect("owner extent is a rectangle");
                assert_eq!(first, anchor, "extent starts at its anchor");
                assert!(
                    (first.coord.row()..=last.coord.row()).contains(&cell.coord.row())
                        && (first.coord.col()..=last.coord.col()).contains(&cell.coord.col()),
                    "claim inside the owner's extent"
                );
                (anchor.coord.row() + 1, anchor.coord.col() + 1)
            });
            out.push((get(e, row, col), owner));
        }
    }
    out
}

/// Build `cells` (row, col, formula) in `order`, evaluating after each edit
/// when `stepwise`; return the final snapshot.
fn build(mode: Mode, cells: &[(u32, u32, &str)], order: &[usize], stepwise: bool) -> Snapshot {
    let mut e = engine(mode);
    for r in 1..=8 {
        f(&mut e, r, 8, &format!("={r}*2"));
    }
    for &i in order {
        let (row, col, src) = cells[i];
        f(&mut e, row, col, src);
        if stepwise {
            eval(&mut e, mode);
        }
    }
    eval(&mut e, mode);
    snapshot(&e)
}

/// The final state of colliding anchors is the same after a fresh load and
/// after either edit order (incumbent first, then the intruder, and the
/// reverse), in every mode.
#[track_caller]
fn assert_history_independent(cells: &[(u32, u32, &str)], expect: &[(u32, u32, Option<f64>)]) {
    for mode in modes() {
        let fresh = build(mode, cells, &[0, 1], false);
        let fresh_rev = build(mode, cells, &[1, 0], false);
        let edit = build(mode, cells, &[0, 1], true);
        let edit_rev = build(mode, cells, &[1, 0], true);
        assert_eq!(fresh, fresh_rev, "{mode:?}: fresh, definition order");
        assert_eq!(fresh, edit, "{mode:?}: first then second");
        assert_eq!(fresh, edit_rev, "{mode:?}: second then first");
        for &(row, col, want) in expect {
            let got = &fresh[((row - 1) * 6 + (col - 1)) as usize].0;
            match want {
                Some(x) => assert_eq!(got, &n(x), "{mode:?} ({row},{col})"),
                None => assert_kind(got.clone(), ExcelErrorKind::Spill, &format!("{mode:?}")),
            }
        }
    }
}

/// Nested: the later anchor (or a scalar formula) lies in the first
/// anchor's rectangle, so it is a formula blocker for that anchor whichever
/// was there first.
#[test]
fn nested_blockers_resolve_independently_of_history() {
    assert_history_independent(
        &[(1, 1, "={1,2;3,4}"), (2, 1, "={5,6;7,8}")],
        &[(1, 1, None), (2, 1, Some(5.0)), (3, 2, Some(8.0))],
    );
    assert_history_independent(
        &[(1, 1, "=SEQUENCE(3)"), (3, 1, "=1+1")],
        &[(1, 1, None), (3, 1, Some(2.0))],
    );
}

/// Crossing: A2:C2 and B1:B3 collide at B2 with neither anchor inside the
/// other's rectangle. The anchor first in (sheet, column, row) order, A2,
/// spills; B1 is `#SPILL!`, whatever the history.
#[test]
fn crossing_spills_resolve_independently_of_history() {
    assert_history_independent(
        &[(2, 1, "=SEQUENCE(1,3)"), (1, 2, "=SEQUENCE(3)")],
        &[
            (2, 1, Some(1.0)),
            (2, 2, Some(2.0)),
            (2, 3, Some(3.0)),
            (1, 2, None),
        ],
    );
    // Mirrored so the yielding anchor is not always the one defined last.
    assert_history_independent(
        &[(4, 2, "=SEQUENCE(3)"), (5, 1, "=SEQUENCE(1,4)")],
        &[(5, 1, Some(1.0)), (5, 4, Some(4.0)), (4, 2, None)],
    );
}

/// Removing the intruder (a scalar formula, or an anchor) lets the
/// incumbent spill again on the next recalc, with no stale claims left.
#[test]
fn removing_the_intruder_restores_the_incumbent() {
    for mode in modes() {
        for (row, col, intruder) in [
            (3, 1, "=1+1"),
            (2, 1, "=SEQUENCE(2)"),
            (2, 1, "=SEQUENCE(1,3)"),
        ] {
            let ctx = format!("{mode:?} {intruder} at ({row},{col})");
            let mut e = engine(mode);
            f(&mut e, 1, 1, "=SEQUENCE(3)");
            f(&mut e, 1, 3, "=SUM(A1#)");
            eval(&mut e, mode);
            let spilled = snapshot(&e);

            f(&mut e, row, col, intruder);
            eval(&mut e, mode);
            assert_kind(get(&e, 1, 1), ExcelErrorKind::Spill, &ctx);
            assert_kind(get(&e, 1, 3), ExcelErrorKind::Ref, &ctx);
            assert!(!has_spill(&e, 1, 1), "{ctx}: incumbent claims nothing");
            let a = e.graph.make_cell_ref("Sheet1", 1, 1);
            for r in 1..=3 {
                let cell = e.graph.make_cell_ref("Sheet1", r, 1);
                assert_ne!(
                    e.graph.spill_registry_anchor_for_cell(cell),
                    e.graph.get_vertex_for_cell(&a),
                    "{ctx}: no stale claim on row {r}"
                );
            }
            snapshot(&e);

            e.set_cell_value("Sheet1", row, col, LiteralValue::Empty)
                .unwrap();
            eval(&mut e, mode);
            assert_eq!(snapshot(&e), spilled, "{ctx}: restored");
            assert_eq!(get(&e, 1, 3), n(6.0), "{ctx}");
        }
    }
}
