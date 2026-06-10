//! Regression tests for issue #126: changelog staged-state recording must be
//! O(N), not O(N^2), and undo/redo semantics over staged formulas must be
//! preserved exactly.
//!
//! These tests are written against the contract (undo/redo restores state) and
//! against a payload-size budget (linear in the number of edits). They are
//! intended to pass both before and after the fix for the undo/redo contract,
//! and to *fail* on `main` for the scaling budget (documenting the regression).

use formualizer_common::LiteralValue;
use formualizer_eval::engine::ChangeEvent;
use formualizer_workbook::Workbook;

fn assert_numeric_eq(v: Option<LiteralValue>, expected: f64) {
    match v {
        Some(LiteralValue::Number(n)) => assert!((n - expected).abs() < 1e-9),
        Some(LiteralValue::Int(i)) => assert!(((i as f64) - expected).abs() < 1e-9),
        other => panic!("expected numeric {expected}, got {other:?}"),
    }
}

/// Total number of staged-formula text strings retained across every recorded
/// staged-formula changelog event. With the old snapshot-pair design this is
/// O(N^2) in the number of `set_formula` calls; with per-cell deltas it is O(N).
fn staged_formula_changelog_payload(wb: &Workbook) -> usize {
    let mut total = 0usize;
    for ev in wb.changelog().events() {
        total += staged_formula_event_payload(ev);
    }
    total
}

fn staged_formula_event_payload(ev: &ChangeEvent) -> usize {
    match ev {
        // Per-cell delta (counts the staged texts present in this event). The old
        // snapshot-pair design (`StagedFormulaStateChanged { before, after }`) recorded
        // O(N) texts per edit and is intentionally removed by the #126 fix.
        ChangeEvent::StagedFormulaCellChanged { old, new, .. } => {
            old.is_some() as usize + new.is_some() as usize
        }
        _ => 0,
    }
}

#[test]
fn undo_redo_mixed_sequence_restores_state() {
    // Mixed sequence: several set_formula (some overwriting each other), with
    // interleaved set_value, each wrapped in its own action. Undo all -> original
    // (empty) state. Redo all -> final state.
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    // Action 1: stage formula at A1
    wb.begin_action("a1=1+1");
    wb.set_formula("S", 1, 1, "1+1").unwrap();
    wb.end_action();

    // Action 2: stage formula at B1
    wb.begin_action("b1=2+2");
    wb.set_formula("S", 1, 2, "2+2").unwrap();
    wb.end_action();

    // Action 3: overwrite A1 staged formula with a new staged formula
    wb.begin_action("a1=3+3");
    wb.set_formula("S", 1, 1, "3+3").unwrap();
    wb.end_action();

    // Action 4: overwrite B1 staged formula with a literal value (clears staged)
    wb.begin_action("b1=9");
    wb.set_value("S", 1, 2, LiteralValue::Int(9)).unwrap();
    wb.end_action();

    // Action 5: stage formula at C1
    wb.begin_action("c1=5+5");
    wb.set_formula("S", 1, 3, "5+5").unwrap();
    wb.end_action();

    // Final state.
    assert_eq!(wb.get_formula("S", 1, 1), Some("3+3".to_string()));
    assert_eq!(wb.get_formula("S", 1, 2), None);
    assert_numeric_eq(wb.get_value("S", 1, 2), 9.0);
    assert_eq!(wb.get_formula("S", 1, 3), Some("5+5".to_string()));

    // Undo all five actions -> original empty state.
    for _ in 0..5 {
        wb.undo().unwrap();
    }
    assert_eq!(wb.get_formula("S", 1, 1), None);
    assert_eq!(wb.get_formula("S", 1, 2), None);
    assert_emptyish(wb.get_value("S", 1, 2));
    assert_eq!(wb.get_formula("S", 1, 3), None);

    // Redo all five actions -> final state.
    for _ in 0..5 {
        wb.redo().unwrap();
    }
    assert_eq!(wb.get_formula("S", 1, 1), Some("3+3".to_string()));
    assert_eq!(wb.get_formula("S", 1, 2), None);
    assert_numeric_eq(wb.get_value("S", 1, 2), 9.0);
    assert_eq!(wb.get_formula("S", 1, 3), Some("5+5".to_string()));
    assert_eq!(
        wb.evaluate_cell("S", 1, 1).unwrap(),
        LiteralValue::Number(6.0)
    );
    assert_eq!(
        wb.evaluate_cell("S", 1, 3).unwrap(),
        LiteralValue::Number(10.0)
    );
}

#[test]
fn undo_redo_overwrite_same_cell_chain() {
    // Repeatedly overwrite the same staged cell, then unwind step by step.
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    let chain = ["1+1", "2+2", "3+3", "4+4"];
    for f in chain {
        wb.begin_action("set");
        wb.set_formula("S", 1, 1, f).unwrap();
        wb.end_action();
    }
    assert_eq!(wb.get_formula("S", 1, 1), Some("4+4".to_string()));

    // Undo step by step; each step reveals the prior staged formula.
    wb.undo().unwrap();
    assert_eq!(wb.get_formula("S", 1, 1), Some("3+3".to_string()));
    wb.undo().unwrap();
    assert_eq!(wb.get_formula("S", 1, 1), Some("2+2".to_string()));
    wb.undo().unwrap();
    assert_eq!(wb.get_formula("S", 1, 1), Some("1+1".to_string()));
    wb.undo().unwrap();
    assert_eq!(wb.get_formula("S", 1, 1), None);

    // Redo all the way back up.
    for f in chain {
        wb.redo().unwrap();
        assert_eq!(wb.get_formula("S", 1, 1), Some(f.to_string()));
    }
}

#[test]
fn staged_formula_changelog_payload_is_linear() {
    // After N distinct set_formula calls (each staging a new cell), the total
    // staged-formula changelog payload must be O(N), not O(N^2).
    //
    // Old snapshot-pair design records `before` + `after` full vectors per edit:
    //   edit i records ~ (i-1) + i texts  => sum ~ N^2.
    // Per-cell delta design records at most a constant (here 1) per edit => N.
    let n: u32 = 400;
    let mut wb = Workbook::new();
    wb.add_sheet("S").unwrap();

    for i in 0..n {
        wb.set_formula("S", i + 1, 1, "1+1").unwrap();
    }

    let payload = staged_formula_changelog_payload(&wb);
    let n = n as usize;

    // Linear budget: at most a small constant factor of N. The per-cell delta
    // design records exactly 1 staged text per new staged cell (old=None,
    // new=Some), i.e. exactly N. We allow generous slack (4*N) while still
    // catching the quadratic regression, which for N=400 would be ~160_000.
    assert!(
        payload <= 4 * n,
        "staged-formula changelog payload {payload} exceeds linear budget {} for N={n} \
         (quadratic regression would be ~{})",
        4 * n,
        n * n
    );
}

fn assert_emptyish(v: Option<LiteralValue>) {
    assert!(matches!(v, None | Some(LiteralValue::Empty)));
}

/// Manual measurement loop for #126: interactive default (changelog on,
/// defer_graph_building on), per-cell `set_formula` on fresh cells at 2k/4k/8k.
/// Run with: `cargo test -p formualizer-workbook --test staged_formula_changelog \
///   --release -- --ignored --nocapture measure_set_formula_scaling`
#[test]
#[ignore]
fn measure_set_formula_scaling() {
    for n in [2000u32, 4000, 8000] {
        let mut wb = Workbook::new(); // interactive default: changelog on, defer on
        wb.add_sheet("S").unwrap();
        let start = std::time::Instant::now();
        for i in 0..n {
            wb.set_formula("S", i + 1, 1, "1+1").unwrap();
        }
        let elapsed = start.elapsed();
        let payload = staged_formula_changelog_payload(&wb);
        println!(
            "N={n:>5}  set_formula loop = {:>8.2} ms  changelog_staged_payload = {payload}",
            elapsed.as_secs_f64() * 1000.0
        );
    }
}
