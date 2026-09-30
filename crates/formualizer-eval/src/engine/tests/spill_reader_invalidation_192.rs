//! FORM-192: a spill committed in a pass must invalidate the readers of its
//! (new, grown, shrunk or cleared) extent in the same request, whichever
//! evaluation entry point runs the pass and whether the anchor and its
//! reader were evaluated together (one parallel phase group).

use crate::engine::{CancelToken, EvalConfig, eval::Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;

#[derive(Clone, Copy, Debug)]
enum Entry {
    All,
    AllCancellable,
    AllWithDelta,
    AllLogged,
}

const ENTRIES: [Entry; 4] = [
    Entry::All,
    Entry::AllCancellable,
    Entry::AllWithDelta,
    Entry::AllLogged,
];

fn cfg(parallel: bool) -> EvalConfig {
    EvalConfig {
        enable_parallel: parallel,
        ..Default::default()
    }
}

fn run(engine: &mut Engine<TestWorkbook>, entry: Entry) {
    match entry {
        Entry::All => {
            engine.evaluate_all().unwrap();
        }
        Entry::AllCancellable => {
            engine.evaluate_all_cancellable(CancelToken::new()).unwrap();
        }
        Entry::AllWithDelta => {
            engine.evaluate_all_with_delta().unwrap();
        }
        Entry::AllLogged => {
            let mut log = crate::engine::ChangeLog::new();
            engine.evaluate_all_logged(&mut log).unwrap();
        }
    }
}

fn val(engine: &Engine<TestWorkbook>, sheet: &str, row: u32, col: u32) -> Option<LiteralValue> {
    match engine.get_cell_value(sheet, row, col) {
        Some(LiteralValue::Int(i)) => Some(LiteralValue::Number(i as f64)),
        Some(LiteralValue::Empty) => None,
        other => other,
    }
}

fn n(x: f64) -> Option<LiteralValue> {
    Some(LiteralValue::Number(x))
}

fn set_formula(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, f: &str) {
    engine
        .set_cell_formula(sheet, row, col, parse(f).unwrap())
        .unwrap();
}

/// Sheet1: A1=2, B1=SEQUENCE(A1), C1=B2*10 (inserted in either order).
fn contract_engine(parallel: bool, anchor_first: bool) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(TestWorkbook::new(), cfg(parallel));
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
        .unwrap();
    if anchor_first {
        set_formula(&mut engine, "Sheet1", 1, 2, "=SEQUENCE(A1)");
        set_formula(&mut engine, "Sheet1", 1, 3, "=B2*10");
    } else {
        set_formula(&mut engine, "Sheet1", 1, 3, "=B2*10");
        set_formula(&mut engine, "Sheet1", 1, 2, "=SEQUENCE(A1)");
    }
    engine
}

fn for_each_mode(mut f: impl FnMut(bool, bool, Entry)) {
    for parallel in [false, true] {
        for anchor_first in [true, false] {
            for entry in ENTRIES {
                f(parallel, anchor_first, entry);
            }
        }
    }
}

#[test]
fn initial_spill_updates_reader_in_one_full_evaluation() {
    for_each_mode(|parallel, anchor_first, entry| {
        let mut engine = contract_engine(parallel, anchor_first);
        run(&mut engine, entry);
        let ctx = format!("parallel={parallel} anchor_first={anchor_first} {entry:?}");
        assert_eq!(val(&engine, "Sheet1", 2, 2), n(2.0), "B2 {ctx}");
        assert_eq!(val(&engine, "Sheet1", 1, 3), n(20.0), "C1 {ctx}");
        // Converged: a second request has nothing left to fix.
        run(&mut engine, entry);
        assert_eq!(val(&engine, "Sheet1", 1, 3), n(20.0), "C1 second {ctx}");
    });
}

#[test]
fn grow_and_shrink_update_readers_without_stale_values() {
    for_each_mode(|parallel, anchor_first, entry| {
        let ctx = format!("parallel={parallel} anchor_first={anchor_first} {entry:?}");
        let mut engine = contract_engine(parallel, anchor_first);
        // D1 reads the row the spill reaches only after growing.
        set_formula(&mut engine, "Sheet1", 1, 4, "=B3*100");
        run(&mut engine, entry);
        assert_eq!(val(&engine, "Sheet1", 1, 3), n(20.0), "C1 {ctx}");
        assert_eq!(val(&engine, "Sheet1", 1, 4), n(0.0), "D1 {ctx}");

        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(3))
            .unwrap();
        run(&mut engine, entry);
        assert_eq!(val(&engine, "Sheet1", 3, 2), n(3.0), "B3 grown {ctx}");
        assert_eq!(val(&engine, "Sheet1", 1, 3), n(20.0), "C1 grown {ctx}");
        assert_eq!(val(&engine, "Sheet1", 1, 4), n(300.0), "D1 grown {ctx}");

        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
            .unwrap();
        run(&mut engine, entry);
        assert_eq!(val(&engine, "Sheet1", 1, 2), n(1.0), "B1 shrunk {ctx}");
        assert_eq!(val(&engine, "Sheet1", 2, 2), None, "B2 shrunk {ctx}");
        assert_eq!(val(&engine, "Sheet1", 3, 2), None, "B3 shrunk {ctx}");
        assert_eq!(val(&engine, "Sheet1", 1, 3), n(0.0), "C1 shrunk {ctx}");
        assert_eq!(val(&engine, "Sheet1", 1, 4), n(0.0), "D1 shrunk {ctx}");
    });
}

#[test]
fn spill_readers_on_another_sheet_update() {
    for parallel in [false, true] {
        for entry in ENTRIES {
            let ctx = format!("parallel={parallel} {entry:?}");
            let mut engine = Engine::new(TestWorkbook::new(), cfg(parallel));
            engine.add_sheet("Other").unwrap();
            engine
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
                .unwrap();
            set_formula(&mut engine, "Other", 1, 1, "=Sheet1!B2*10");
            set_formula(&mut engine, "Other", 1, 2, "=SUM(Sheet1!B1:B5)");
            set_formula(&mut engine, "Sheet1", 1, 2, "=SEQUENCE(A1)");
            run(&mut engine, entry);
            assert_eq!(val(&engine, "Other", 1, 1), n(20.0), "Other!A1 {ctx}");
            assert_eq!(val(&engine, "Other", 1, 2), n(3.0), "Other!B1 {ctx}");

            engine
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(4))
                .unwrap();
            run(&mut engine, entry);
            assert_eq!(val(&engine, "Other", 1, 2), n(10.0), "grown {ctx}");

            engine
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
                .unwrap();
            run(&mut engine, entry);
            assert_eq!(val(&engine, "Other", 1, 1), n(0.0), "shrunk {ctx}");
            assert_eq!(val(&engine, "Other", 1, 2), n(1.0), "shrunk sum {ctx}");
        }
    }
}

#[test]
fn spill_reader_chain_updates_downstream() {
    // E1 reads C1, which reads the spill child B2: the stale-reader
    // re-dirty must carry through to C1's own dependents.
    for_each_mode(|parallel, anchor_first, entry| {
        let ctx = format!("parallel={parallel} anchor_first={anchor_first} {entry:?}");
        let mut engine = contract_engine(parallel, anchor_first);
        set_formula(&mut engine, "Sheet1", 1, 5, "=C1+1");
        run(&mut engine, entry);
        assert_eq!(val(&engine, "Sheet1", 1, 3), n(20.0), "C1 {ctx}");
        assert_eq!(val(&engine, "Sheet1", 1, 5), n(21.0), "E1 {ctx}");
    });
}

#[test]
fn reader_committed_in_an_earlier_layer_updates_its_dependents() {
    // A1 is a formula, so the anchor B1 runs a layer after the reader C1
    // (no static edge orders C1 after B1). The spill re-dirties the
    // already committed C1; E1 (C1's dependent) must not keep the value it
    // computed from C1's stale result.
    for parallel in [false, true] {
        for entry in ENTRIES {
            let ctx = format!("parallel={parallel} {entry:?}");
            let mut engine = Engine::new(TestWorkbook::new(), cfg(parallel));
            set_formula(&mut engine, "Sheet1", 1, 5, "=C1+1");
            set_formula(&mut engine, "Sheet1", 1, 3, "=B2*10");
            set_formula(&mut engine, "Sheet1", 1, 2, "=SEQUENCE(A1)");
            set_formula(&mut engine, "Sheet1", 1, 1, "=1+1");
            run(&mut engine, entry);
            assert_eq!(val(&engine, "Sheet1", 1, 3), n(20.0), "C1 {ctx}");
            assert_eq!(val(&engine, "Sheet1", 1, 5), n(21.0), "E1 {ctx}");
        }
    }
}

#[test]
fn scalar_result_clearing_a_spill_updates_readers_in_the_same_group() {
    for parallel in [false, true] {
        for entry in ENTRIES {
            let ctx = format!("parallel={parallel} {entry:?}");
            let mut engine = Engine::new(TestWorkbook::new(), cfg(parallel));
            engine
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
                .unwrap();
            set_formula(&mut engine, "Sheet1", 1, 3, "=B2*10");
            set_formula(&mut engine, "Sheet1", 1, 2, "=IF(A1>1,SEQUENCE(A1),A1)");
            run(&mut engine, entry);
            assert_eq!(val(&engine, "Sheet1", 1, 3), n(20.0), "C1 {ctx}");
            engine
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(0))
                .unwrap();
            run(&mut engine, entry);
            assert_eq!(val(&engine, "Sheet1", 1, 2), n(0.0), "B1 scalar {ctx}");
            assert_eq!(val(&engine, "Sheet1", 2, 2), None, "B2 cleared {ctx}");
            assert_eq!(val(&engine, "Sheet1", 1, 3), n(0.0), "C1 cleared {ctx}");
        }
    }
}

#[test]
fn many_independent_spills_and_readers_converge_in_one_request() {
    // A wide layer: every anchor and reader sits in one layer so the
    // parallel path evaluates them in the same phase group.
    for parallel in [false, true] {
        for entry in ENTRIES {
            let ctx = format!("parallel={parallel} {entry:?}");
            let mut engine = Engine::new(TestWorkbook::new(), cfg(parallel));
            for i in 0..40u32 {
                let col = 1 + i * 3;
                engine
                    .set_cell_value("Sheet1", 1, col, LiteralValue::Int(i as i64 + 2))
                    .unwrap();
                let a = formualizer_common::col_letters_from_1based(col).unwrap();
                let b = formualizer_common::col_letters_from_1based(col + 1).unwrap();
                set_formula(&mut engine, "Sheet1", 1, col + 2, &format!("={b}2*10"));
                set_formula(
                    &mut engine,
                    "Sheet1",
                    1,
                    col + 1,
                    &format!("=SEQUENCE({a}1)"),
                );
            }
            run(&mut engine, entry);
            for i in 0..40u32 {
                let col = 1 + i * 3;
                assert_eq!(
                    val(&engine, "Sheet1", 1, col + 2),
                    n(20.0),
                    "reader {i} {ctx}"
                );
            }
        }
    }
}

#[test]
fn targeted_evaluation_sees_spill_committed_in_the_same_request() {
    for parallel in [false, true] {
        for anchor_first in [true, false] {
            let ctx = format!("parallel={parallel} anchor_first={anchor_first}");
            // Target the anchor and the reader together.
            let mut engine = contract_engine(parallel, anchor_first);
            let out = engine
                .evaluate_cells(&[("Sheet1", 1, 2), ("Sheet1", 1, 3)])
                .unwrap();
            assert_eq!(out[1], Some(LiteralValue::Number(20.0)), "cells {ctx}");

            let mut engine = contract_engine(parallel, anchor_first);
            let out = engine
                .evaluate_cells_cancellable(
                    &[("Sheet1", 1, 2), ("Sheet1", 1, 3)],
                    CancelToken::new(),
                )
                .unwrap();
            assert_eq!(
                out[1],
                Some(LiteralValue::Number(20.0)),
                "cancellable {ctx}"
            );

            // Known extent (after a full pass): growing and shrinking via a
            // targeted request on the reader alone.
            let mut engine = contract_engine(parallel, anchor_first);
            set_formula(&mut engine, "Sheet1", 1, 4, "=SUM(B1:B5)");
            engine.evaluate_all().unwrap();
            assert_eq!(val(&engine, "Sheet1", 1, 4), n(3.0), "sum {ctx}");
            engine
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(4))
                .unwrap();
            let out = engine.evaluate_cells(&[("Sheet1", 1, 4)]).unwrap();
            assert_eq!(out[0], Some(LiteralValue::Number(10.0)), "grown sum {ctx}");
            engine
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
                .unwrap();
            let out = engine
                .evaluate_cells(&[("Sheet1", 1, 3), ("Sheet1", 1, 4)])
                .unwrap();
            assert_eq!(out[0], Some(LiteralValue::Number(0.0)), "shrunk C1 {ctx}");
            assert_eq!(out[1], Some(LiteralValue::Number(1.0)), "shrunk sum {ctx}");
        }
    }
}

#[test]
fn cancelled_request_leaves_work_dirty_and_next_request_converges() {
    for parallel in [false, true] {
        for anchor_first in [true, false] {
            let ctx = format!("parallel={parallel} anchor_first={anchor_first}");
            let mut engine = contract_engine(parallel, anchor_first);
            let cancel = CancelToken::new();
            cancel.cancel();
            let err = engine.evaluate_all_cancellable(cancel).unwrap_err();
            assert_eq!(err.kind, ExcelErrorKind::Cancelled, "{ctx}");
            engine.evaluate_all_cancellable(CancelToken::new()).unwrap();
            assert_eq!(val(&engine, "Sheet1", 1, 3), n(20.0), "C1 {ctx}");
        }
    }
}

#[test]
fn spill_feeding_back_into_its_anchor_terminates() {
    // B1's spill child B2 feeds C1, which feeds B1: a cycle through the
    // spill that no static edge describes. Each request must terminate
    // through the bounded replan loop, never hang.
    for parallel in [false, true] {
        for entry in ENTRIES {
            let mut engine = Engine::new(TestWorkbook::new(), cfg(parallel));
            set_formula(&mut engine, "Sheet1", 1, 3, "=B2+1");
            set_formula(&mut engine, "Sheet1", 1, 2, "=SEQUENCE(2)+C1");
            let res = match entry {
                Entry::All => engine.evaluate_all().map(|_| ()),
                Entry::AllCancellable => engine
                    .evaluate_all_cancellable(CancelToken::new())
                    .map(|_| ()),
                Entry::AllWithDelta => engine.evaluate_all_with_delta().map(|_| ()),
                Entry::AllLogged => {
                    let mut log = crate::engine::ChangeLog::new();
                    engine.evaluate_all_logged(&mut log).map(|_| ())
                }
            };
            // The replan loop's existing bound ends the request with a
            // resource error rather than iterating until stable.
            let err = res.expect_err("spill cycle cannot converge");
            assert!(
                err.message
                    .as_deref()
                    .is_some_and(|m| m.contains("did not converge")),
                "parallel={parallel} {entry:?}: {err:?}"
            );
        }
    }
}

#[test]
fn targeted_evaluation_of_a_wide_layer_converges() {
    for parallel in [false, true] {
        let mut engine = Engine::new(TestWorkbook::new(), cfg(parallel));
        let mut targets = Vec::new();
        for i in 0..40u32 {
            let col = 1 + i * 3;
            engine
                .set_cell_value("Sheet1", 1, col, LiteralValue::Int(2))
                .unwrap();
            let a = formualizer_common::col_letters_from_1based(col).unwrap();
            let b = formualizer_common::col_letters_from_1based(col + 1).unwrap();
            set_formula(&mut engine, "Sheet1", 1, col + 2, &format!("={b}2*10"));
            set_formula(
                &mut engine,
                "Sheet1",
                1,
                col + 1,
                &format!("=SEQUENCE({a}1)"),
            );
            targets.push(("Sheet1", 1, col + 1));
            targets.push(("Sheet1", 1, col + 2));
        }
        let out = engine.evaluate_cells(&targets).unwrap();
        for (i, v) in out.iter().enumerate().filter(|(i, _)| i % 2 == 1) {
            assert_eq!(
                v,
                &Some(LiteralValue::Number(20.0)),
                "reader {i} parallel={parallel}"
            );
        }
    }
}

#[test]
fn resource_failure_mid_request_keeps_spill_readers_retryable() {
    use crate::engine::{EvaluationBudgets, WorkResourceBudget};
    // Budgets small enough to stop the request in its first pass, between
    // the passes, or not at all: whatever committed, a retry with default
    // budgets must converge (no reader left clean with a stale value).
    let (mut failed, mut succeeded) = (0, 0);
    for parallel in [false, true] {
        for anchor_first in [true, false] {
            for limit in 1..=8u64 {
                let ctx = format!("parallel={parallel} anchor_first={anchor_first} limit={limit}");
                let mut engine = contract_engine(parallel, anchor_first);
                set_formula(&mut engine, "Sheet1", 1, 5, "=C1+1");
                engine.set_evaluation_budgets_for_test(EvaluationBudgets {
                    work: WorkResourceBudget {
                        max_work_units: Some(limit),
                    },
                    ..EvaluationBudgets::default()
                });
                let first = engine.evaluate_all_cancellable(CancelToken::new());
                if first.is_ok() {
                    succeeded += 1;
                    assert_eq!(val(&engine, "Sheet1", 1, 3), n(20.0), "C1 {ctx}");
                    assert_eq!(val(&engine, "Sheet1", 1, 5), n(21.0), "E1 {ctx}");
                } else {
                    failed += 1;
                }
                engine.set_evaluation_budgets_for_test(EvaluationBudgets::default());
                engine.evaluate_all_cancellable(CancelToken::new()).unwrap();
                assert_eq!(val(&engine, "Sheet1", 1, 3), n(20.0), "C1 retry {ctx}");
                assert_eq!(val(&engine, "Sheet1", 1, 5), n(21.0), "E1 retry {ctx}");
            }
        }
    }
    assert!(
        failed > 0 && succeeded > 0,
        "failed={failed} succeeded={succeeded}"
    );
}
