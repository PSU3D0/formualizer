//! Untimed supported-API oracle for schedule invalidation after logged edits and replay.
//! Run with `cargo run -p formualizer-eval --features test-support --example recalc_invalidation`.

use formualizer_common::LiteralValue;
use formualizer_eval::engine::graph::editor::undo_engine::UndoEngine;
use formualizer_eval::engine::{ChangeLog, Engine, EvalConfig, FormulaPlaneMode};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;

type TestEngine = Engine<TestWorkbook>;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Logged,
    AtomicCommit,
    Undo,
    Redo,
}

fn setup(reversed: bool) -> TestEngine {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            formula_plane_mode: FormulaPlaneMode::Off,
            enable_parallel: false,
            enable_virtual_dep_telemetry: true,
            ..EvalConfig::default()
        },
    );
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    let formulas = if reversed {
        ["=C1+1", "=A1+10"]
    } else {
        ["=A1+1", "=B1+1"]
    };
    for (col, formula) in (2..=3).zip(formulas) {
        engine
            .set_cell_formula("Sheet1", 1, col, parse(formula).unwrap())
            .unwrap();
    }
    engine.evaluate_all().unwrap();
    engine
}

fn prepare(operation: Operation) -> TestEngine {
    let mut engine = setup(false);
    if matches!(operation, Operation::Logged) {
        engine
            .action_with_logger(&mut ChangeLog::new(), "reverse dependencies", |action| {
                action.set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())?;
                action.set_cell_formula("Sheet1", 1, 3, parse("=A1+10").unwrap())
            })
            .unwrap();
        return engine;
    }

    let (_, journal) = engine
        .action_atomic_journal("reverse dependencies", |action| {
            action.set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())?;
            action.set_cell_formula("Sheet1", 1, 3, parse("=A1+10").unwrap())
        })
        .unwrap();
    let mut undo = UndoEngine::new();
    undo.push_action(journal);
    if matches!(operation, Operation::AtomicCommit) {
        return engine;
    }

    // Establish the correct reversed schedule so undo is tested independently of commit.
    engine.mark_topology_edited();
    engine.evaluate_all().unwrap();
    engine.undo_action(&mut undo).unwrap();
    if matches!(operation, Operation::Undo) {
        return engine;
    }

    // Likewise establish the original schedule before testing redo invalidation.
    engine.mark_topology_edited();
    engine.evaluate_all().unwrap();
    engine.redo_action(&mut undo).unwrap();
    engine
}

fn values(engine: &TestEngine) -> Vec<Option<LiteralValue>> {
    (2..=3)
        .map(|col| engine.get_cell_value("Sheet1", 1, col))
        .collect()
}

fn main() {
    let mut failures = 0;
    for operation in [
        Operation::Logged,
        Operation::AtomicCommit,
        Operation::Undo,
        Operation::Redo,
    ] {
        let mut cached = prepare(operation);
        let mut uncached = prepare(operation);
        let fresh = setup(!matches!(operation, Operation::Undo));
        let dirty_before = cached.evaluation_vertices();
        assert_eq!(dirty_before, uncached.evaluation_vertices());
        uncached.mark_topology_edited();
        let cached_result = cached.evaluate_all().unwrap();
        let uncached_result = uncached.evaluate_all().unwrap();
        let expected = values(&fresh);
        assert_eq!(
            values(&uncached),
            expected,
            "uncached oracle: {operation:?}"
        );
        let actual = values(&cached);
        let telemetry = cached.last_virtual_dep_telemetry();
        println!(
            "operation={operation:?} dirty={dirty_before:?} cached={actual:?} uncached={:?} fresh={expected:?} hits={} misses={} computed={}/{}",
            values(&uncached),
            telemetry.schedule_cache_hits,
            telemetry.schedule_cache_misses,
            cached_result.computed_vertices,
            uncached_result.computed_vertices,
        );
        failures += usize::from(actual != expected);
    }
    assert_eq!(
        failures, 0,
        "cached evaluation must match both exact oracles"
    );
}
