//! FORM214: spill-range references, `A1#` and `_xlfn.ANCHORARRAY(A1)`.
//!
//! A spill reference resolves to the anchor's current committed spill
//! rectangle and then takes the ordinary reference path, so range
//! consumers (SUM, ROWS, INDEX) see a real range. Policy (no Excel oracle):
//! an unsupported operand, or an anchor with no current spill (a value, an
//! empty cell, a scalar result, a blocked or oversized spill, and a fresh
//! 1x1 result, which is committed as a scalar) yields `#REF!`.

use crate::engine::live_edges::{LiveEdgeCollector, RecordingContext};
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::virtual_deps::DynamicRefCollector;
use crate::engine::{CancelToken, Engine, EvalConfig};
use crate::function::{FnCaps, Function};
use crate::interpreter::Interpreter;
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use rustc_hash::FxHashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn config(parallel: bool) -> EvalConfig {
    EvalConfig {
        enable_parallel: parallel,
        max_threads: if parallel { Some(4) } else { None },
        ..Default::default()
    }
}

fn engine(parallel: bool) -> Engine<TestWorkbook> {
    crate::builtins::load_builtins();
    Engine::new(TestWorkbook::new(), config(parallel))
}

fn f(e: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, src: &str) {
    e.set_cell_formula(sheet, row, col, parse(src).unwrap())
        .unwrap();
}

fn set(e: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, v: LiteralValue) {
    e.set_cell_value(sheet, row, col, v).unwrap();
}

fn get(e: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
    match e.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Int(i)) => Some(LiteralValue::Number(i as f64)),
        Some(LiteralValue::Empty) | None => None,
        other => other,
    }
}

fn number(v: LiteralValue) -> LiteralValue {
    match v {
        LiteralValue::Int(i) => LiteralValue::Number(i as f64),
        other => other,
    }
}

fn n(x: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(x))
}

#[track_caller]
fn assert_err(value: Option<LiteralValue>, kind: ExcelErrorKind, ctx: &str) {
    match value {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, kind, "{ctx}: {e:?}"),
        other => panic!("{ctx}: expected {kind:?}, got {other:?}"),
    }
}

#[track_caller]
fn assert_ref(value: Option<LiteralValue>, ctx: &str) {
    assert_err(value, ExcelErrorKind::Ref, ctx);
}

fn cell(e: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> CellRef {
    CellRef::new(
        e.sheet_id(sheet).expect("sheet"),
        Coord::from_excel(row, col, true, true),
    )
}

/* ─────────────────────────── both spellings ─────────────────────────── */

#[test]
fn sum_over_both_spellings() {
    for parallel in [false, true] {
        let mut e = engine(parallel);
        f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(2)");
        f(&mut e, "Sheet1", 1, 2, "=SUM(A1#)");
        f(&mut e, "Sheet1", 1, 3, "=SUM(_xlfn.ANCHORARRAY(A1))");
        f(&mut e, "Sheet1", 1, 4, "=SUM(ANCHORARRAY(A1))");
        f(&mut e, "Sheet1", 1, 5, "=SUM($A$1#)");
        e.evaluate_all().unwrap();
        for col in 2..=5 {
            assert_eq!(get(&e, "Sheet1", 1, col), n(3.0), "col {col} p={parallel}");
        }
    }
}

#[test]
fn two_dimensional_spill_and_aggregates() {
    let mut e = engine(false);
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(2,3)");
    f(&mut e, "Sheet1", 5, 1, "=SUM(A1#)");
    f(&mut e, "Sheet1", 5, 2, "=COUNT(A1#)");
    f(&mut e, "Sheet1", 5, 3, "=AVERAGE(ANCHORARRAY(A1))");
    f(&mut e, "Sheet1", 5, 4, "=MAX(A1#)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 5, 1), n(21.0));
    assert_eq!(get(&e, "Sheet1", 5, 2), n(6.0));
    assert_eq!(get(&e, "Sheet1", 5, 3), n(3.5));
    assert_eq!(get(&e, "Sheet1", 5, 4), n(6.0));
}

#[test]
fn spill_reference_as_direct_spilling_result() {
    for parallel in [false, true] {
        let mut e = engine(parallel);
        f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(3)");
        f(&mut e, "Sheet1", 1, 2, "=A1#");
        f(&mut e, "Sheet1", 1, 3, "=_xlfn.ANCHORARRAY(A1)");
        f(&mut e, "Sheet1", 1, 4, "=A1#*10");
        e.evaluate_all().unwrap();
        for row in 1..=3 {
            let r = row as f64;
            assert_eq!(get(&e, "Sheet1", row, 2), n(r), "B{row} p={parallel}");
            assert_eq!(get(&e, "Sheet1", row, 3), n(r), "C{row} p={parallel}");
            assert_eq!(get(&e, "Sheet1", row, 4), n(r * 10.0), "D{row}");
        }
        assert_eq!(get(&e, "Sheet1", 4, 2), None);
    }
}

#[test]
fn cross_sheet_anchor() {
    let mut e = engine(false);
    e.add_sheet("Data").unwrap();
    f(&mut e, "Data", 2, 2, "=SEQUENCE(3)");
    f(&mut e, "Sheet1", 1, 1, "=SUM(Data!B2#)");
    f(&mut e, "Sheet1", 1, 2, "=SUM(_xlfn.ANCHORARRAY(Data!B2))");
    f(&mut e, "Sheet1", 1, 3, "=ROWS(Data!$B$2#)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 1), n(6.0));
    assert_eq!(get(&e, "Sheet1", 1, 2), n(6.0));
    assert_eq!(get(&e, "Sheet1", 1, 3), n(3.0));

    // The unqualified spelling on Sheet1 names a different (empty) anchor.
    f(&mut e, "Sheet1", 1, 4, "=SUM(B2#)");
    e.evaluate_all().unwrap();
    assert_ref(get(&e, "Sheet1", 1, 4), "unqualified B2 on Sheet1");

    // The anchor grows: the cross-sheet reader follows.
    f(&mut e, "Data", 2, 2, "=SEQUENCE(4)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 1), n(10.0));
    assert_eq!(get(&e, "Sheet1", 1, 2), n(10.0));
}

#[test]
fn single_cell_defined_name() {
    let mut e = engine(false);
    let a1 = cell(&e, "Sheet1", 1, 1);
    e.define_name("Anchor", NamedDefinition::Cell(a1), NameScope::Workbook)
        .unwrap();
    let range = RangeRef::new(cell(&e, "Sheet1", 1, 1), cell(&e, "Sheet1", 2, 1));
    e.define_name(
        "Twocells",
        NamedDefinition::Range(range),
        NameScope::Workbook,
    )
    .unwrap();
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(2)");
    f(&mut e, "Sheet1", 1, 2, "=SUM(Anchor#)");
    f(&mut e, "Sheet1", 1, 3, "=SUM(_xlfn.ANCHORARRAY(Anchor))");
    f(&mut e, "Sheet1", 1, 4, "=SUM(Twocells#)");
    f(&mut e, "Sheet1", 1, 5, "=SUM(ANCHORARRAY(Twocells))");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 2), n(3.0));
    assert_eq!(get(&e, "Sheet1", 1, 3), n(3.0));
    assert_ref(get(&e, "Sheet1", 1, 4), "name bound to a range");
    assert_ref(get(&e, "Sheet1", 1, 5), "ANCHORARRAY of a range name");

    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(5)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 2), n(15.0));
    assert_eq!(get(&e, "Sheet1", 1, 3), n(15.0));
}

#[test]
fn by_reference_consumers_see_a_reference() {
    let mut e = engine(false);
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(4,2)");
    f(&mut e, "Sheet1", 1, 4, "=ROWS(A1#)");
    f(&mut e, "Sheet1", 1, 5, "=COLUMNS(A1#)");
    f(&mut e, "Sheet1", 1, 6, "=ROWS(_xlfn.ANCHORARRAY(A1))");
    f(&mut e, "Sheet1", 1, 7, "=INDEX(A1#,3,2)");
    f(&mut e, "Sheet1", 1, 8, "=ISREF(A1#)");
    f(&mut e, "Sheet1", 1, 9, "=ISREF(ANCHORARRAY(A1))");
    f(&mut e, "Sheet1", 1, 10, "=SUM(OFFSET(A1#,1,0,1,2))");
    f(&mut e, "Sheet1", 1, 11, "=SUM(INDEX(A1#,0,1))");
    f(&mut e, "Sheet1", 1, 12, "=ROW(A1#)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 4), n(4.0));
    assert_eq!(get(&e, "Sheet1", 1, 5), n(2.0));
    assert_eq!(get(&e, "Sheet1", 1, 6), n(4.0));
    assert_eq!(get(&e, "Sheet1", 1, 7), n(6.0));
    assert_eq!(get(&e, "Sheet1", 1, 8), Some(LiteralValue::Boolean(true)));
    assert_eq!(get(&e, "Sheet1", 1, 9), Some(LiteralValue::Boolean(true)));
    assert_eq!(get(&e, "Sheet1", 1, 10), n(7.0));
    assert_eq!(get(&e, "Sheet1", 1, 11), n(16.0));
    assert_eq!(get(&e, "Sheet1", 1, 12), n(1.0));
}

#[test]
fn let_local_bound_to_anchor_cell() {
    let mut e = engine(false);
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(3)");
    f(&mut e, "Sheet1", 1, 2, "=LET(a,A1,SUM(a#))");
    f(&mut e, "Sheet1", 1, 3, "=LET(a,A1,SUM(ANCHORARRAY(a)))");
    f(&mut e, "Sheet1", 1, 4, "=LET(a,5,SUM(a#))");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 2), n(6.0));
    assert_eq!(get(&e, "Sheet1", 1, 3), n(6.0));
    assert_ref(get(&e, "Sheet1", 1, 4), "local bound to a value");
}

/* ─────────────────────── shape and state changes ────────────────────── */

#[test]
fn grow_and_shrink_with_unchanged_top_left() {
    for parallel in [false, true] {
        let mut e = engine(parallel);
        set(&mut e, "Sheet1", 1, 3, LiteralValue::Number(2.0));
        f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(C1)");
        f(&mut e, "Sheet1", 1, 2, "=SUM(A1#)");
        f(&mut e, "Sheet1", 1, 4, "=ROWS(ANCHORARRAY(A1))");
        e.evaluate_all().unwrap();
        assert_eq!(get(&e, "Sheet1", 1, 2), n(3.0));
        assert_eq!(get(&e, "Sheet1", 1, 4), n(2.0));

        for (rows, sum) in [(5.0, 15.0), (3.0, 6.0), (4.0, 10.0), (2.0, 3.0)] {
            set(&mut e, "Sheet1", 1, 3, LiteralValue::Number(rows));
            e.evaluate_all().unwrap();
            assert_eq!(get(&e, "Sheet1", 1, 1), n(1.0), "top-left unchanged");
            assert_eq!(get(&e, "Sheet1", 1, 2), n(sum), "rows={rows} p={parallel}");
            assert_eq!(get(&e, "Sheet1", 1, 4), n(rows), "rows={rows} p={parallel}");
        }
    }
}

/// A blocker in the way when the anchor recomputes: the anchor is
/// `#SPILL!` and its spill references are `#REF!`; removing the blocker
/// re-spills and the readers follow.
#[test]
fn blocker_added_then_removed() {
    for parallel in [false, true] {
        let mut e = engine(parallel);
        set(&mut e, "Sheet1", 1, 3, LiteralValue::Number(2.0));
        f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(C1)");
        f(&mut e, "Sheet1", 1, 2, "=SUM(A1#)");
        f(&mut e, "Sheet1", 1, 4, "=SUM(_xlfn.ANCHORARRAY(A1))");
        e.evaluate_all().unwrap();
        assert_eq!(get(&e, "Sheet1", 1, 2), n(3.0));

        // Blocker outside the current footprint, then the spill grows into it.
        set(&mut e, "Sheet1", 3, 1, LiteralValue::Text("x".into()));
        set(&mut e, "Sheet1", 1, 3, LiteralValue::Number(3.0));
        e.evaluate_all().unwrap();
        assert_err(
            get(&e, "Sheet1", 1, 1),
            ExcelErrorKind::Spill,
            "blocked anchor",
        );
        assert_ref(get(&e, "Sheet1", 1, 2), "blocked spill, # spelling");
        assert_ref(get(&e, "Sheet1", 1, 4), "blocked spill, ANCHORARRAY");

        // Clearing a value blocker does not by itself re-dirty the anchor
        // (pre-existing engine behavior, unrelated to spill references); the
        // next anchor recompute spills and the readers follow.
        set(&mut e, "Sheet1", 3, 1, LiteralValue::Empty);
        set(&mut e, "Sheet1", 1, 3, LiteralValue::Number(4.0));
        e.evaluate_all().unwrap();
        assert_eq!(get(&e, "Sheet1", 1, 1), n(1.0));
        assert_eq!(get(&e, "Sheet1", 1, 2), n(10.0), "p={parallel}");
        assert_eq!(get(&e, "Sheet1", 1, 4), n(10.0), "p={parallel}");
    }
}

/// Known limitation (pre-existing child-overwrite policy, not the
/// resolver): a user write into a cell the spill owns leaves the anchor
/// clean and the registry still owning the cell. A `SUM(A1:A3)` reader has
/// an edge to the written cell and recomputes; a `SUM(A1#)` reader has only
/// the anchor edge and keeps its value until the anchor next recomputes.
/// Excel would publish `#SPILL!` at the anchor instead.
#[test]
fn spill_ref_child_overwrite_is_stale_until_anchor_recomputes_known_limitation() {
    let mut e = engine(false);
    set(&mut e, "Sheet1", 1, 4, LiteralValue::Number(3.0));
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE($D$1)");
    f(&mut e, "Sheet1", 1, 2, "=SUM(A1#)");
    f(&mut e, "Sheet1", 1, 3, "=SUM(A1:A3)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 2), n(6.0));
    assert_eq!(get(&e, "Sheet1", 1, 3), n(6.0));

    set(&mut e, "Sheet1", 3, 1, LiteralValue::Number(50.0));
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 1), n(1.0), "anchor not re-dirtied");
    assert_eq!(get(&e, "Sheet1", 1, 3), n(53.0), "range reader is fresh");
    assert_eq!(
        get(&e, "Sheet1", 1, 2),
        n(6.0),
        "spill reader is stale (limitation)"
    );

    // The staleness is bounded by the next anchor recompute.
    set(&mut e, "Sheet1", 1, 4, LiteralValue::Number(4.0));
    e.evaluate_all().unwrap();
    let fresh: f64 = (1..=4)
        .map(|row| match get(&e, "Sheet1", row, 1) {
            Some(LiteralValue::Number(x)) => x,
            other => panic!("A{row}: {other:?}"),
        })
        .sum();
    assert_eq!(
        get(&e, "Sheet1", 1, 2),
        n(fresh),
        "spill reader agrees again"
    );
    assert_eq!(fresh, 10.0);
}

#[test]
fn anchor_changed_to_scalar_or_value() {
    let mut e = engine(false);
    set(&mut e, "Sheet1", 1, 3, LiteralValue::Number(3.0));
    f(&mut e, "Sheet1", 1, 1, "=IF(C1>1,SEQUENCE(C1),C1*100)");
    f(&mut e, "Sheet1", 1, 2, "=SUM(A1#)");
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 2), n(6.0));

    set(&mut e, "Sheet1", 1, 3, LiteralValue::Number(0.0));
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 1), n(0.0));
    assert_ref(get(&e, "Sheet1", 1, 2), "scalar result");

    set(&mut e, "Sheet1", 1, 3, LiteralValue::Number(4.0));
    e.evaluate_all().unwrap();
    assert_eq!(get(&e, "Sheet1", 1, 2), n(10.0), "spills again");

    // Formula replaced by a plain value.
    set(&mut e, "Sheet1", 1, 1, LiteralValue::Number(7.0));
    e.evaluate_all().unwrap();
    assert_ref(get(&e, "Sheet1", 1, 2), "anchor is a value");
}

#[test]
fn no_current_spill_policy() {
    let mut cfg = config(false);
    cfg.spill.max_spill_cells = 4;
    crate::builtins::load_builtins();
    let mut e = Engine::new(TestWorkbook::new(), cfg);
    set(&mut e, "Sheet1", 1, 1, LiteralValue::Number(5.0));
    f(&mut e, "Sheet1", 1, 5, "=SEQUENCE(9)"); // oversized
    f(&mut e, "Sheet1", 1, 6, "=5+1"); // scalar formula
    f(&mut e, "Sheet1", 1, 7, "=SEQUENCE(1)"); // fresh 1x1: deferred, scalar
    f(&mut e, "Sheet1", 1, 8, "={7}"); // 1x1 array literal
    f(&mut e, "Sheet1", 2, 1, "=SUM(A1#)"); // value anchor
    f(&mut e, "Sheet1", 2, 2, "=SUM(D1#)"); // empty anchor
    f(&mut e, "Sheet1", 2, 5, "=SUM(E1#)");
    f(&mut e, "Sheet1", 2, 6, "=SUM(F1#)");
    f(&mut e, "Sheet1", 2, 7, "=SUM(G1#)");
    f(&mut e, "Sheet1", 2, 8, "=SUM(H1#)");
    f(&mut e, "Sheet1", 3, 1, "=SUM(_xlfn.ANCHORARRAY(A1))");
    f(&mut e, "Sheet1", 3, 2, "=SUM(_xlfn.ANCHORARRAY(D1))");
    f(&mut e, "Sheet1", 3, 5, "=SUM(_xlfn.ANCHORARRAY(E1))");
    f(&mut e, "Sheet1", 3, 6, "=SUM(_xlfn.ANCHORARRAY(F1))");
    f(&mut e, "Sheet1", 3, 7, "=SUM(_xlfn.ANCHORARRAY(G1))");
    f(&mut e, "Sheet1", 3, 8, "=ROWS(G1#)");
    e.evaluate_all().unwrap();
    assert_err(get(&e, "Sheet1", 1, 5), ExcelErrorKind::Spill, "oversized");
    assert_eq!(
        get(&e, "Sheet1", 1, 7),
        n(1.0),
        "fresh SEQUENCE(1) is a scalar"
    );
    for (row, col, what) in [
        (2, 1, "value"),
        (2, 2, "empty"),
        (2, 5, "oversized"),
        (2, 6, "scalar formula"),
        (2, 7, "fresh SEQUENCE(1) (deferred)"),
        (2, 8, "1x1 array literal"),
        (3, 1, "value"),
        (3, 2, "empty"),
        (3, 5, "oversized"),
        (3, 6, "scalar formula"),
        (3, 7, "fresh SEQUENCE(1) (deferred)"),
        (3, 8, "ROWS of fresh SEQUENCE(1) (deferred)"),
    ] {
        assert_ref(get(&e, "Sheet1", row, col), what);
    }
}

#[test]
fn unsupported_operands_are_ref_errors() {
    let mut e = engine(false);
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(3)");
    let formulas = [
        "=SUM(ANCHORARRAY(A1:A2))",
        "=SUM(ANCHORARRAY(A1:A1))",
        "=SUM(ANCHORARRAY(INDEX(A1:A3,1)))",
        "=SUM(ANCHORARRAY(OFFSET(A1,0,0)))",
        "=SUM(ANCHORARRAY(5))",
        "=SUM(ANCHORARRAY(\"A1\"))",
        "=SUM(ANCHORARRAY(A1+0))",
        "=SUM(ANCHORARRAY(A1#))",
        "=SUM(ANCHORARRAY(Sheet1:Sheet1!A1))",
        "=SUM(INDEX(A1:A3,1)#)",
        "=SUM((A1:A2)#)",
        "=SUM(A1##)",
        "=SUM(ANCHORARRAY(A1,A1))",
        "=ANCHORARRAY()",
    ];
    let mut row = 5;
    for src in formulas {
        let Ok(ast) = parse(src) else {
            // A spelling the parser rejects is out of scope here.
            continue;
        };
        e.set_cell_formula("Sheet1", row, 3, ast).unwrap();
        row += 1;
    }
    e.evaluate_all().unwrap();
    let mut row = 5;
    for src in formulas {
        if parse(src).is_err() {
            continue;
        }
        match get(&e, "Sheet1", row, 3) {
            Some(LiteralValue::Error(err)) => assert!(
                matches!(err.kind, ExcelErrorKind::Ref | ExcelErrorKind::Value),
                "{src}: {err:?}"
            ),
            other => panic!("{src}: expected an error, got {other:?}"),
        }
        row += 1;
    }
    // The operand policy proper is `#REF!`.
    for (r, src) in [
        (20, "=SUM(ANCHORARRAY(A1:A2))"),
        (21, "=SUM(ANCHORARRAY(INDEX(A1:A3,1)))"),
        (22, "=SUM(ANCHORARRAY(5))"),
        (23, "=SUM(ANCHORARRAY(A1+0))"),
        (24, "=SUM(ANCHORARRAY(A1#))"),
    ] {
        f(&mut e, "Sheet1", r, 3, src);
        e.evaluate_all().unwrap();
        assert_ref(get(&e, "Sheet1", r, 3), src);
    }
}

/// Filled-down readers (a family shape) stay on the per-formula path, since
/// templates reject `#` and reference-returning calls, and each member
/// reads the current spill.
#[test]
fn filled_down_readers() {
    for parallel in [false, true] {
        let mut e = engine(parallel);
        set(&mut e, "Sheet1", 1, 1, LiteralValue::Number(3.0));
        f(&mut e, "Sheet1", 1, 2, "=SEQUENCE($A$1)");
        for row in 1..=64u32 {
            set(&mut e, "Sheet1", row, 4, LiteralValue::Number(row as f64));
            f(&mut e, "Sheet1", row, 5, &format!("=SUM($B$1#)+D{row}"));
            f(
                &mut e,
                "Sheet1",
                row,
                6,
                &format!("=ROWS(_xlfn.ANCHORARRAY($B$1))*D{row}"),
            );
        }
        e.evaluate_all().unwrap();
        for row in 1..=64u32 {
            let r = row as f64;
            assert_eq!(get(&e, "Sheet1", row, 5), n(6.0 + r), "E{row}");
            assert_eq!(get(&e, "Sheet1", row, 6), n(3.0 * r), "F{row}");
        }
        set(&mut e, "Sheet1", 1, 1, LiteralValue::Number(5.0));
        e.evaluate_all().unwrap();
        for row in 1..=64u32 {
            let r = row as f64;
            assert_eq!(
                get(&e, "Sheet1", row, 5),
                n(15.0 + r),
                "E{row} p={parallel}"
            );
            assert_eq!(get(&e, "Sheet1", row, 6), n(5.0 * r), "F{row} p={parallel}");
        }
    }
}

/* ───────────────────────── evaluation modes ─────────────────────────── */

#[test]
fn target_only_evaluation() {
    let mut e = engine(false);
    set(&mut e, "Sheet1", 1, 3, LiteralValue::Number(2.0));
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(C1)");
    f(&mut e, "Sheet1", 1, 2, "=SUM(A1#)");
    f(&mut e, "Sheet1", 1, 4, "=SUM(_xlfn.ANCHORARRAY(A1))");
    let out = e
        .evaluate_cells(&[("Sheet1", 1, 2), ("Sheet1", 1, 4)])
        .unwrap();
    assert_eq!(out[0], Some(LiteralValue::Number(3.0)));
    assert_eq!(out[1], Some(LiteralValue::Number(3.0)));

    set(&mut e, "Sheet1", 1, 3, LiteralValue::Number(6.0));
    assert_eq!(
        e.evaluate_cell("Sheet1", 1, 2).unwrap(),
        Some(LiteralValue::Number(21.0))
    );
    assert_eq!(
        e.evaluate_cell("Sheet1", 1, 4).unwrap(),
        Some(LiteralValue::Number(21.0))
    );
}

#[test]
fn parallel_layers_many_anchors_and_readers() {
    let mut seq = engine(false);
    let mut par = engine(true);
    for e in [&mut seq, &mut par] {
        for i in 1..=40u32 {
            set(e, "Sheet1", i, 1, LiteralValue::Number((i % 7 + 2) as f64));
            // Anchors in column C, spilling right across C:K.
            f(e, "Sheet1", i, 3, &format!("=SEQUENCE(1,A{i})"));
            // Readers in column M, chained through a second layer in N.
            f(e, "Sheet1", i, 13, &format!("=SUM(C{i}#)"));
            f(
                e,
                "Sheet1",
                i,
                14,
                &format!("=M{i}+COLUMNS(_xlfn.ANCHORARRAY(C{i}))"),
            );
        }
    }
    seq.evaluate_all().unwrap();
    par.evaluate_all().unwrap();
    for i in 1..=40u32 {
        let k = (i % 7 + 2) as f64;
        let expected = k * (k + 1.0) / 2.0;
        assert_eq!(get(&seq, "Sheet1", i, 13), n(expected), "seq M{i}");
        assert_eq!(get(&par, "Sheet1", i, 13), n(expected), "par M{i}");
        assert_eq!(get(&par, "Sheet1", i, 14), n(expected + k), "par N{i}");
    }
    for e in [&mut seq, &mut par] {
        for i in 1..=40u32 {
            set(e, "Sheet1", i, 1, LiteralValue::Number((i % 5 + 2) as f64));
        }
        e.evaluate_all().unwrap();
    }
    for i in 1..=40u32 {
        let k = (i % 5 + 2) as f64;
        let expected = k * (k + 1.0) / 2.0;
        assert_eq!(
            get(&seq, "Sheet1", i, 13),
            n(expected),
            "seq M{i} after edit"
        );
        assert_eq!(
            get(&par, "Sheet1", i, 13),
            n(expected),
            "par M{i} after edit"
        );
        assert_eq!(get(&par, "Sheet1", i, 14), n(expected + k), "par N{i}");
    }
}

/* ─────────────────────── cancellation and abort ─────────────────────── */

/// `PROBE()`: on call `cancel_at` cancels the request's token and returns
/// `Err(Cancelled)`; otherwise 100 (or `{1;2;3}` with `array`).
#[derive(Debug)]
struct Probe {
    calls: Arc<AtomicUsize>,
    cancel_at: usize,
    array: bool,
}

impl Function for Probe {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        "PROBE"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == self.cancel_at {
            ctx.cancellation_token()
                .expect("cancellable request exposes its token")
                .cancel();
            return Err(ExcelError::new(ExcelErrorKind::Cancelled));
        }
        Ok(CalcValue::Scalar(if self.array {
            LiteralValue::Array(vec![
                vec![LiteralValue::Number(1.0)],
                vec![LiteralValue::Number(2.0)],
                vec![LiteralValue::Number(3.0)],
            ])
        } else {
            LiteralValue::Number(100.0)
        }))
    }
}

fn probe_engine(
    parallel: bool,
    cancel_at: usize,
    array: bool,
) -> (Engine<TestWorkbook>, Arc<AtomicUsize>) {
    crate::builtins::load_builtins();
    let calls = Arc::new(AtomicUsize::new(0));
    let wb = TestWorkbook::new().with_function(Arc::new(Probe {
        calls: calls.clone(),
        cancel_at,
        array,
    }));
    (Engine::new(wb, config(parallel)), calls)
}

/// The reader is the final vertex and observes the cancellation: nothing
/// is committed for it, and a retry recomputes it against the spill.
#[test]
fn cancelled_reader_retries_against_current_spill() {
    for parallel in [false, true] {
        let (mut e, calls) = probe_engine(parallel, 0, false);
        f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(3)");
        f(&mut e, "Sheet1", 1, 2, "=SUM(A1#)+PROBE()");
        let err = e.evaluate_all_cancellable(CancelToken::new()).unwrap_err();
        assert_eq!(err.kind, ExcelErrorKind::Cancelled, "{err:?}");
        assert_eq!(
            get(&e, "Sheet1", 1, 2),
            None,
            "cancelled reader uncommitted"
        );

        e.evaluate_all().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(get(&e, "Sheet1", 1, 2), n(106.0), "p={parallel}");
    }
}

/// The anchor itself is cancelled before it spills: the reader must not be
/// published against a missing spill, and the retry converges.
#[test]
fn cancelled_anchor_retries_and_reader_converges() {
    for parallel in [false, true] {
        let (mut e, calls) = probe_engine(parallel, 0, true);
        f(&mut e, "Sheet1", 1, 1, "=PROBE()");
        f(&mut e, "Sheet1", 1, 2, "=SUM(A1#)");
        f(&mut e, "Sheet1", 1, 3, "=ROWS(_xlfn.ANCHORARRAY(A1))");
        let err = e.evaluate_all_cancellable(CancelToken::new()).unwrap_err();
        assert_eq!(err.kind, ExcelErrorKind::Cancelled, "{err:?}");
        assert_eq!(get(&e, "Sheet1", 1, 1), None);
        assert_eq!(get(&e, "Sheet1", 1, 2), None, "reader not published");

        e.evaluate_all().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(get(&e, "Sheet1", 2, 1), n(2.0));
        assert_eq!(get(&e, "Sheet1", 1, 2), n(6.0), "p={parallel}");
        assert_eq!(get(&e, "Sheet1", 1, 3), n(3.0), "p={parallel}");
    }
}

/// FORM192 pattern: the parallel group commit fails its one-shot preflight
/// after the first spill committed; the request unwinds and a retry
/// converges, including the spill-reference readers.
#[test]
fn spill_batch_abort_then_retry_converges() {
    for retry_targeted in [false, true] {
        let mut e = engine(true);
        set(&mut e, "Sheet1", 1, 1, LiteralValue::Int(2));
        f(&mut e, "Sheet1", 1, 2, "=SEQUENCE(A1)");
        f(&mut e, "Sheet1", 1, 3, "=SUM(B1#)+A1*0");
        f(&mut e, "Sheet1", 1, 4, "=A1+100");
        f(&mut e, "Sheet1", 1, 5, "=C1+ROWS(_xlfn.ANCHORARRAY(F1))");
        f(&mut e, "Sheet1", 1, 6, "=SEQUENCE(A1+1)");
        e.fail_evaluation_commit_preflight_once_for_test();
        let err = e.evaluate_all_cancellable(CancelToken::new()).unwrap_err();
        assert!(
            matches!(err.extra, ExcelErrorExtra::Resource { .. }),
            "expected injected preflight failure, got {err:?}"
        );
        if retry_targeted {
            let out = e
                .evaluate_cells(&[("Sheet1", 1, 5), ("Sheet1", 1, 3)])
                .unwrap();
            assert_eq!(out[0], Some(LiteralValue::Number(6.0)));
            assert_eq!(out[1], Some(LiteralValue::Number(3.0)));
        } else {
            e.evaluate_all_cancellable(CancelToken::new()).unwrap();
        }
        assert_eq!(
            get(&e, "Sheet1", 1, 3),
            n(3.0),
            "C1 targeted={retry_targeted}"
        );
        assert_eq!(
            get(&e, "Sheet1", 1, 5),
            n(6.0),
            "E1 targeted={retry_targeted}"
        );

        // A later edit still flows through the spill references.
        set(&mut e, "Sheet1", 1, 1, LiteralValue::Int(4));
        e.evaluate_all().unwrap();
        assert_eq!(get(&e, "Sheet1", 1, 3), n(10.0));
        assert_eq!(get(&e, "Sheet1", 1, 5), n(15.0));
    }
}

/* ─────────────────────── cycles and footprints ──────────────────────── */

#[test]
fn self_reference_stays_a_cycle() {
    let mut e = engine(false);
    // A direct self-reference is refused at edit time, as for `=A1+1`.
    for src in [
        "=SEQUENCE(ROWS(A1#)+1)",
        "=SEQUENCE(2)+SUM(_xlfn.ANCHORARRAY(A1))",
    ] {
        let err = e
            .set_cell_formula("Sheet1", 1, 1, parse(src).unwrap())
            .unwrap_err();
        assert_eq!(err.kind, ExcelErrorKind::Circ, "{src}: {err:?}");
    }
    // A two-anchor cycle through spill references is an ordinary cycle.
    f(&mut e, "Sheet1", 1, 3, "=SEQUENCE(ROWS(D1#)+1)");
    f(
        &mut e,
        "Sheet1",
        1,
        4,
        "=SEQUENCE(1,ROWS(_xlfn.ANCHORARRAY(C1)))",
    );
    e.evaluate_all().unwrap();
    assert_err(get(&e, "Sheet1", 1, 3), ExcelErrorKind::Circ, "C1");
    assert_err(get(&e, "Sheet1", 1, 4), ExcelErrorKind::Circ, "D1");
}

#[test]
fn reader_inside_the_footprint_blocks_the_spill() {
    let mut e = engine(false);
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(3)");
    f(&mut e, "Sheet1", 3, 1, "=SUM(A1#)");
    e.evaluate_all().unwrap();
    assert_err(
        get(&e, "Sheet1", 1, 1),
        ExcelErrorKind::Spill,
        "anchor blocked",
    );
    assert_ref(get(&e, "Sheet1", 3, 1), "reader over a blocked spill");
}

/* ─────────────────────────── wrapper contexts ───────────────────────── */

#[test]
fn indirect_in_the_same_formula() {
    for parallel in [false, true] {
        let mut e = engine(parallel);
        set(&mut e, "Sheet1", 1, 4, LiteralValue::Number(100.0));
        set(&mut e, "Sheet1", 1, 5, LiteralValue::Number(2.0));
        f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(E1)");
        f(&mut e, "Sheet1", 1, 2, "=SUM(A1#)+INDIRECT(\"D1\")");
        f(
            &mut e,
            "Sheet1",
            1,
            3,
            "=SUM(_xlfn.ANCHORARRAY(A1))+INDIRECT(\"D1\")",
        );
        e.evaluate_all().unwrap();
        assert_eq!(get(&e, "Sheet1", 1, 2), n(103.0), "p={parallel}");
        assert_eq!(get(&e, "Sheet1", 1, 3), n(103.0), "p={parallel}");

        set(&mut e, "Sheet1", 1, 5, LiteralValue::Number(4.0));
        e.evaluate_all().unwrap();
        assert_eq!(get(&e, "Sheet1", 1, 2), n(110.0), "p={parallel}");
        assert_eq!(get(&e, "Sheet1", 1, 3), n(110.0), "p={parallel}");

        set(&mut e, "Sheet1", 1, 4, LiteralValue::Number(1.0));
        e.evaluate_all().unwrap();
        assert_eq!(get(&e, "Sheet1", 1, 2), n(11.0), "p={parallel}");
    }
}

#[test]
fn dynamic_ref_collector_forwards_spill_references() {
    let mut e = engine(false);
    set(&mut e, "Sheet1", 1, 4, LiteralValue::Number(100.0));
    set(&mut e, "Sheet1", 1, 5, LiteralValue::Number(2.0));
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(E1)");
    e.evaluate_all().unwrap();
    // Dirty the anchor: the collector reports dirty formula vertices it
    // reads, and the committed spill is still the 2-row one.
    set(&mut e, "Sheet1", 1, 5, LiteralValue::Number(3.0));
    let anchor = e
        .graph
        .get_vertex_id_for_address(&cell(&e, "Sheet1", 1, 1))
        .unwrap();
    assert!(e.graph.is_dirty(anchor));
    let b1 = cell(&e, "Sheet1", 1, 2);
    for src in [
        "=SUM(A1#)+INDIRECT(\"D1\")",
        "=SUM(_xlfn.ANCHORARRAY(A1))+INDIRECT(\"D1\")",
    ] {
        let collector = DynamicRefCollector::new(&e, "Sheet1");
        let interp = Interpreter::new_with_cell(&collector, "Sheet1", b1);
        let v = interp
            .evaluate_ast(&parse(src).unwrap())
            .map(|cv| cv.into_literal())
            .unwrap_or_else(LiteralValue::Error);
        assert_eq!(v, LiteralValue::Number(103.0), "{src}");
        assert!(
            collector.collected.lock().unwrap().contains(&anchor),
            "{src}: the spill read reaches the anchor vertex"
        );
    }
}

#[test]
fn recording_context_forwards_and_records_the_anchor() {
    let mut e = engine(false);
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(2)");
    e.evaluate_all().unwrap();
    let c1 = cell(&e, "Sheet1", 1, 3);
    let a1 = cell(&e, "Sheet1", 1, 1);
    let collector = LiveEdgeCollector::new(&[c1, a1]);
    for src in ["=SUM(A1#)", "=SUM(_xlfn.ANCHORARRAY(A1))", "=ROWS(A1#)+1"] {
        collector.set_current(0);
        let ctx = RecordingContext::new(&e, &collector);
        let interp = Interpreter::new_with_cell(&ctx, "Sheet1", c1);
        let v = interp
            .evaluate_ast(&parse(src).unwrap())
            .map(|cv| number(cv.into_literal()))
            .unwrap_or_else(LiteralValue::Error);
        assert_eq!(v, LiteralValue::Number(3.0), "{src}");
        assert_eq!(
            collector.take_edges(),
            FxHashSet::from_iter([(0, 1)]),
            "{src}: the anchor read is recorded"
        );
    }
}

/* ───────────────────────── interpreter paths ────────────────────────── */

/// The tree interpreter (planner and plain tree walks) and the arena path
/// agree with the engine.
#[test]
fn tree_interpreter_paths() {
    let mut e = engine(false);
    f(&mut e, "Sheet1", 1, 1, "=SEQUENCE(3)");
    e.evaluate_all().unwrap();
    let b1 = cell(&e, "Sheet1", 1, 2);
    let interp = Interpreter::new_with_cell(&e, "Sheet1", b1);
    for (src, expected) in [
        ("=SUM(A1#)", LiteralValue::Number(6.0)),
        ("=SUM(_xlfn.ANCHORARRAY(A1))", LiteralValue::Number(6.0)),
        ("=ROWS(A1#)", LiteralValue::Number(3.0)),
        ("=INDEX(A1#,2)", LiteralValue::Number(2.0)),
        ("=SUM(A1#:B1)", LiteralValue::Number(6.0)),
    ] {
        let ast = parse(src).unwrap();
        let planned = interp
            .evaluate_ast(&ast)
            .map(|cv| number(cv.into_literal()))
            .unwrap_or_else(LiteralValue::Error);
        assert_eq!(planned, expected, "planned {src}");
        let tree = interp
            .evaluate_ast_with_offset(&ast, 0, 0)
            .map(|cv| number(cv.into_literal()))
            .unwrap_or_else(LiteralValue::Error);
        assert_eq!(tree, expected, "tree {src}");
    }
    let direct = interp
        .evaluate_ast(&parse("=A1#").unwrap())
        .map(|cv| cv.into_literal())
        .unwrap();
    assert_eq!(
        direct,
        LiteralValue::Array(vec![
            vec![LiteralValue::Number(1.0)],
            vec![LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.0)],
        ])
    );
    let reference = interp
        .evaluate_ast_as_reference(&parse("=A1#").unwrap())
        .unwrap();
    assert_eq!(
        reference,
        formualizer_parse::parser::ReferenceType::Range {
            sheet: None,
            start_row: Some(1),
            start_col: Some(1),
            end_row: Some(3),
            end_col: Some(1),
            start_row_abs: true,
            start_col_abs: true,
            end_row_abs: true,
            end_col_abs: true,
        }
    );
}

/// A context that does not implement the hook keeps the default `#REF!`.
#[test]
fn default_hook_is_ref_error() {
    let wb = TestWorkbook::new();
    let interp = Interpreter::new(&wb, "Sheet1");
    let v = interp
        .evaluate_ast(&parse("=A1#").unwrap())
        .map(|cv| cv.into_literal())
        .unwrap_or_else(LiteralValue::Error);
    assert!(
        matches!(v, LiteralValue::Error(ref e) if e.kind == ExcelErrorKind::Ref),
        "{v:?}"
    );
}
