//! FORM-192 abort evidence: a guarded batch pre-clears its members, an
//! early member's spill commits, the request fails before a later member
//! commits, and `freshness_abort_pass` leaves every uncommitted member,
//! the re-dirtied reader and its downstream dirty, so a retry converges.

use crate::engine::scheduler::Schedule;
use crate::engine::vertex::VertexId;
use crate::engine::{CancelToken, EvalConfig, eval::Engine};
use crate::reference::{CellRef, Coord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorExtra, LiteralValue, ResourceExhaustionReason};
use formualizer_parse::parser::parse;

fn engine(parallel: bool) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: parallel,
            ..Default::default()
        },
    )
}

fn f(e: &mut Engine<TestWorkbook>, row: u32, col: u32, src: &str) {
    e.set_cell_formula("Sheet1", row, col, parse(src).unwrap())
        .unwrap();
}

fn vid(e: &Engine<TestWorkbook>, row: u32, col: u32) -> VertexId {
    let sheet = e.sheet_id("Sheet1").unwrap();
    e.graph
        .get_vertex_id_for_address(&CellRef::new(
            sheet,
            Coord::from_excel(row, col, true, true),
        ))
        .expect("formula vertex")
}

fn v(e: &Engine<TestWorkbook>, row: u32, col: u32) -> Option<LiteralValue> {
    match e.get_cell_value("Sheet1", row, col) {
        Some(LiteralValue::Int(i)) => Some(LiteralValue::Number(i as f64)),
        Some(LiteralValue::Empty) | None => None,
        other => other,
    }
}

fn n(x: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(x))
}

fn col(xs: &[f64]) -> LiteralValue {
    LiteralValue::Array(xs.iter().map(|&x| vec![LiteralValue::Number(x)]).collect())
}

/// A1=2, X=B1 SEQUENCE(A1), R=C1 B2*10 (+A1*0 so an A1 edit schedules it),
/// Z=D1 A1+100, E1=C1+1 (downstream, next layer), Y=F1 SEQUENCE(A1+1).
fn workbook(parallel: bool) -> Engine<TestWorkbook> {
    let mut e = engine(parallel);
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
        .unwrap();
    f(&mut e, 1, 2, "=SEQUENCE(A1)");
    f(&mut e, 1, 3, "=B2*10+A1*0");
    f(&mut e, 1, 4, "=A1+100");
    f(&mut e, 1, 5, "=C1+1");
    f(&mut e, 1, 6, "=SEQUENCE(A1+1)");
    e
}

fn empty_schedule() -> Schedule {
    Schedule {
        units: Vec::new(),
        cycles: Vec::new(),
        layers: Vec::new(),
    }
}

fn commit(e: &mut Engine<TestWorkbook>, vertex: VertexId, value: LiteralValue) {
    let redirtied = e.freshness_batch_redirtied(true, vertex);
    for effect in e.plan_vertex_effects(vertex, value, None).unwrap() {
        e.apply_effect(&effect, None, None).unwrap();
    }
    e.freshness_keep_redirtied(redirtied, vertex);
}

fn assert_converged(e: &Engine<TestWorkbook>, ctx: &str) {
    assert_eq!(v(e, 2, 2), n(2.0), "B2 {ctx}");
    assert_eq!(v(e, 1, 3), n(20.0), "C1 {ctx}");
    assert_eq!(v(e, 1, 4), n(102.0), "D1 {ctx}");
    assert_eq!(v(e, 1, 5), n(21.0), "E1 {ctx}");
    assert_eq!(v(e, 3, 6), n(3.0), "F3 {ctx}");
}

/// Direct state transition: pre-clear, first spill commits and re-dirties
/// the reader, the pass aborts before Y/Z/R commit.
#[test]
fn guarded_batch_abort_restores_uncommitted_members_direct() {
    for retry_targeted in [false, true] {
        let mut e = workbook(false);
        let (x, r, z, d, y) = (
            vid(&e, 1, 2),
            vid(&e, 1, 3),
            vid(&e, 1, 4),
            vid(&e, 1, 5),
            vid(&e, 1, 6),
        );
        for m in [x, r, z, d, y] {
            assert!(e.graph.is_dirty(m), "new formulas start dirty");
        }
        e.freshness_begin_pass(&empty_schedule());
        let batch = vec![
            (x, col(&[1.0, 2.0])),
            (y, col(&[1.0, 2.0, 3.0])),
            (r, LiteralValue::Number(0.0)),
            (z, LiteralValue::Number(102.0)),
        ];
        assert!(
            e.freshness_begin_batch_commit(&batch),
            "array member guards the batch"
        );
        for m in [x, y, r, z] {
            assert!(!e.graph.is_dirty(m), "pre-cleared member {m:?}");
        }
        assert!(e.graph.is_dirty(d), "non-member untouched");

        commit(&mut e, x, col(&[1.0, 2.0]));
        assert_eq!(v(&e, 2, 2), n(2.0), "X spill committed");
        assert!(
            e.graph.is_dirty(r),
            "FR5: spill commit re-dirtied the reader"
        );
        assert!(e.freshness_batch_redirtied(true, r));
        assert!(
            !e.graph.is_dirty(y) && !e.graph.is_dirty(z),
            "commit window: Y/Z pre-cleared, not committed"
        );

        // A later member's preflight fails: the request unwinds.
        e.freshness_abort_pass();
        for m in [x, y, r, z, d] {
            assert!(e.graph.is_dirty(m), "abort must leave {m:?} dirty");
        }
        assert_eq!(v(&e, 2, 6), None, "Y never committed");

        if retry_targeted {
            let out = e
                .evaluate_cells(&[("Sheet1", 1, 5), ("Sheet1", 1, 4), ("Sheet1", 1, 6)])
                .unwrap();
            assert_eq!(out[0], Some(LiteralValue::Number(21.0)));
            assert_eq!(out[1], Some(LiteralValue::Number(102.0)));
        } else {
            e.evaluate_all().unwrap();
        }
        assert_converged(&e, &format!("retry_targeted={retry_targeted}"));
    }
}

/// Old footprint: X shrinks (SpillClear then SpillCommit) or turns scalar
/// (SpillClear then WriteCell) inside the window; the reader of the
/// dropped cell is re-dirtied and survives the abort.
#[test]
fn guarded_batch_abort_after_old_spill_clear_direct() {
    for scalar in [false, true] {
        let mut e = engine(false);
        e.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(3))
            .unwrap();
        f(&mut e, 1, 2, "=IF(A1>1,SEQUENCE(A1),A1)");
        f(&mut e, 1, 3, "=B3*10+A1*0");
        f(&mut e, 1, 4, "=A1+100");
        f(&mut e, 1, 5, "=C1+1");
        f(&mut e, 1, 6, "=SEQUENCE(A1+1)");
        e.evaluate_all().unwrap();
        assert_eq!(v(&e, 1, 3), n(30.0));
        let new_a1 = if scalar { 0 } else { 2 };
        e.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(new_a1))
            .unwrap();
        let (x, r, z, d, y) = (
            vid(&e, 1, 2),
            vid(&e, 1, 3),
            vid(&e, 1, 4),
            vid(&e, 1, 5),
            vid(&e, 1, 6),
        );
        e.freshness_begin_pass(&empty_schedule());
        let xv = if scalar {
            LiteralValue::Number(0.0)
        } else {
            col(&[1.0, 2.0])
        };
        let batch = vec![
            (x, xv.clone()),
            (y, col(&[1.0])),
            (r, LiteralValue::Number(30.0)),
            (z, LiteralValue::Number(new_a1 as f64 + 100.0)),
        ];
        // A scalar X guards through its existing spill anchor.
        assert!(e.freshness_begin_batch_commit(&batch));
        commit(&mut e, x, xv);
        assert_eq!(v(&e, 3, 2), None, "old footprint B3 cleared");
        assert!(e.graph.is_dirty(r), "clear re-dirtied the B3 reader");
        e.freshness_abort_pass();
        for m in [x, y, r, z, d] {
            assert!(
                e.graph.is_dirty(m),
                "abort must leave {m:?} dirty (scalar={scalar})"
            );
        }
        e.evaluate_all().unwrap();
        assert_eq!(v(&e, 1, 3), n(0.0), "C1 scalar={scalar}");
        assert_eq!(v(&e, 1, 5), n(1.0), "E1 scalar={scalar}");
        assert_eq!(v(&e, 1, 4), n(new_a1 as f64 + 100.0));
    }
}

/// End to end through the parallel group commit, with the existing
/// one-shot commit-preflight fault: X's buffered top-left makes Y's
/// pre-plan flush the request's first non-empty flush.
#[test]
fn guarded_group_preflight_failure_is_retryable_end_to_end() {
    for retry_targeted in [false, true] {
        let mut e = workbook(true);
        let (x, r, z, d, y) = (
            vid(&e, 1, 2),
            vid(&e, 1, 3),
            vid(&e, 1, 4),
            vid(&e, 1, 5),
            vid(&e, 1, 6),
        );
        e.fail_evaluation_commit_preflight_once_for_test();
        let err = e.evaluate_all_cancellable(CancelToken::new()).unwrap_err();
        let ExcelErrorExtra::Resource { detail } = &err.extra else {
            panic!("expected injected preflight failure, got {err:?}");
        };
        assert_eq!(detail.reason, ResourceExhaustionReason::Deadline);
        // The failure fell inside the guarded window: X committed, Y did not.
        assert_eq!(v(&e, 2, 2), n(2.0), "X spill committed before the failure");
        assert_eq!(v(&e, 2, 6), None, "Y not committed");
        for m in [x, y, r, z, d] {
            assert!(e.graph.is_dirty(m), "abort must leave {m:?} dirty");
        }
        if retry_targeted {
            let out = e
                .evaluate_cells(&[("Sheet1", 1, 5), ("Sheet1", 1, 4), ("Sheet1", 1, 6)])
                .unwrap();
            assert_eq!(out[0], Some(LiteralValue::Number(21.0)));
        } else {
            e.evaluate_all_cancellable(CancelToken::new()).unwrap();
        }
        assert_converged(&e, &format!("e2e retry_targeted={retry_targeted}"));
    }
}
