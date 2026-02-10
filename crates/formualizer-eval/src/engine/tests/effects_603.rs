//! Tests for the formal effects pipeline (ticket 603).
//!
//! Validates: determinism, parallel spill correctness, ChangeLog integration,
//! and sequential-vs-parallel equivalence.

use crate::engine::graph::editor::change_log::{ChangeEvent, ChangeLog};
use crate::engine::{EvalConfig, eval::Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::LiteralValue;
use formualizer_parse::parser::parse;

/// Same inputs and config produce identical cell values across two independent
/// evaluation runs.
#[test]
fn effects_determinism() {
    let setup = |engine: &mut Engine<TestWorkbook>| {
        engine
            .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,2)").unwrap())
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 5, 1, parse("=SUM(A1:B3)").unwrap())
            .unwrap();
        engine
            .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(99.0))
            .unwrap();
    };

    let wb1 = TestWorkbook::new();
    let mut e1 = Engine::new(wb1, EvalConfig::default());
    setup(&mut e1);
    e1.evaluate_all().unwrap();
    let _ = e1.evaluate_all().unwrap();

    let wb2 = TestWorkbook::new();
    let mut e2 = Engine::new(wb2, EvalConfig::default());
    setup(&mut e2);
    e2.evaluate_all().unwrap();
    let _ = e2.evaluate_all().unwrap();

    for r in 1..=5 {
        for c in 1..=3 {
            assert_eq!(
                e1.get_cell_value("Sheet1", r, c),
                e2.get_cell_value("Sheet1", r, c),
                "determinism mismatch at R{r}C{c}"
            );
        }
    }
}

/// `enable_parallel=true` still produces correct spill results.
#[test]
fn parallel_spill_correctness() {
    let wb = TestWorkbook::new();
    let cfg = EvalConfig {
        enable_parallel: true,
        max_threads: Some(4),
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(wb, cfg);

    // Two independent spills in the same layer (no dependencies on each other)
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 5, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();
    // A dependent in a later layer
    engine
        .set_cell_formula("Sheet1", 5, 1, parse("=SUM(A1:A3)+SUM(E1:E3)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    let _ = engine.evaluate_all().unwrap();

    // Spill values correct
    for r in 1..=3 {
        assert_eq!(
            engine.get_cell_value("Sheet1", r, 1),
            Some(LiteralValue::Number(r as f64)),
            "spill A mismatch at row {r}"
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", r, 5),
            Some(LiteralValue::Number(r as f64)),
            "spill E mismatch at row {r}"
        );
    }
    // SUM(1+2+3) + SUM(1+2+3) = 12
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 1),
        Some(LiteralValue::Number(12.0))
    );
}

/// `evaluate_all_logged` records SpillCommitted events inside a compound.
#[test]
fn changelog_captures_spill_effects() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());
    let mut log = ChangeLog::new();

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();
    engine.evaluate_all_logged(&mut log).unwrap();

    let events = log.events();

    // Should have CompoundStart, at least one SpillCommitted, CompoundEnd.
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ChangeEvent::CompoundStart { .. })),
        "expected CompoundStart"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ChangeEvent::SpillCommitted { .. })),
        "expected SpillCommitted"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, ChangeEvent::CompoundEnd { .. })),
        "expected CompoundEnd"
    );
}

/// Parallel compute + sequential apply produces the same results as fully
/// sequential evaluation.
#[test]
fn sequential_apply_under_parallel_compute() {
    let setup = |engine: &mut Engine<TestWorkbook>| {
        for r in 1..=100 {
            engine
                .set_cell_formula("Sheet1", r, 1, parse("=ROW()").unwrap())
                .unwrap();
        }
        engine
            .set_cell_formula("Sheet1", 1, 2, parse("=SUM(A1:A100)").unwrap())
            .unwrap();
    };

    // Sequential
    let wb = TestWorkbook::new();
    let mut seq = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: false,
            ..Default::default()
        },
    );
    setup(&mut seq);
    seq.evaluate_all().unwrap();
    let _ = seq.evaluate_all().unwrap();

    // Parallel
    let wb = TestWorkbook::new();
    let mut par = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: true,
            max_threads: Some(4),
            ..Default::default()
        },
    );
    setup(&mut par);
    par.evaluate_all().unwrap();
    let _ = par.evaluate_all().unwrap();

    // Compare
    for r in 1..=100 {
        assert_eq!(
            seq.get_cell_value("Sheet1", r, 1),
            par.get_cell_value("Sheet1", r, 1),
            "seq/par mismatch at R{r}C1"
        );
    }
    assert_eq!(
        seq.get_cell_value("Sheet1", 1, 2),
        par.get_cell_value("Sheet1", 1, 2),
        "seq/par mismatch at SUM cell"
    );
}
