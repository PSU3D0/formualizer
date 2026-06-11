//! Deferred-dirty scope (`DependencyGraph::begin_deferred_dirty` /
//! `end_deferred_dirty`): batched multi-edit APIs queue dirty-propagation
//! sources and flush them as ONE multi-source `mark_dirty_many` instead of
//! running a full BFS per edit (O(edits × component) → O(component)).
//!
//! Union semantics of the single multi-source flush vs sequential per-edit
//! marks is pinned by `mark_dirty_many_equals_sequential_single_source_marks`
//! (mark_dirty_multi_source.rs); these tests pin (a) that the *scope* yields
//! the same dirty/evaluation state as sequential edits, (b) the O(component)
//! visit count, and (c) flush-on-error / no-leak behavior.
//!
//! Work is asserted via `dirty_propagation_visits`, never wall time.

use crate::engine::{DependencyGraph, Engine, EvalConfig, VertexId};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn set_formula(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, f: &str) {
    engine
        .set_cell_formula(sheet, row, col, parse(f).expect("parse"))
        .expect("set formula");
}

/* ─────────── batched edits are one component walk, not N walks ────────── */

#[test]
fn deferred_scope_batch_is_one_component_walk_not_one_per_edit() {
    // 200 inputs all feed one shared component: B1 = SUM(A1:A200) plus a
    // 200-formula chain off B1. A per-edit loop pays ≥ 200 × 201 visits;
    // the deferred scope's single flush visits the component once.
    let inputs = 200u32;
    let chain = 200u32;
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for r in 1..=inputs {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Int(1))
            .unwrap();
    }
    set_formula(&mut engine, "Sheet1", 1, 2, &format!("=SUM(A1:A{inputs})"));
    for r in 2..=(chain + 1) {
        set_formula(&mut engine, "Sheet1", r, 2, &format!("=B{}+1", r - 1));
    }
    engine.evaluate_all().unwrap();

    let before = engine.dirty_propagation_visits();
    engine.begin_deferred_dirty();
    for r in 1..=inputs {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Int(2))
            .unwrap();
    }
    engine.end_deferred_dirty();
    let delta = engine.dirty_propagation_visits() - before;
    eprintln!(
        "[deferred-dirty] batched {inputs} edits: {delta} BFS visits \
         ({}-component; per-edit loop was ≥ {})",
        chain + 1,
        inputs as u64 * (chain + 1) as u64
    );

    let component = (chain + 1) as u64; // B1 + chain (inputs are value cells)
    assert!(
        delta <= 2 * component + inputs as u64,
        "deferred batch must be ~one component walk (≈{component} visits), \
         got {delta} (quadratic was ≥ {})",
        inputs as u64 * component
    );

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(2.0 * inputs as f64)),
        "rollup must see the new inputs"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", chain + 1, 2),
        Some(LiteralValue::Number(2.0 * inputs as f64 + chain as f64)),
        "chain tail must see the new inputs"
    );
}

/* ───── scope == sequential edits: identical dirty/evaluation state ────── */

/// Mixed shared + disjoint downstream structure, formulas overwriting values
/// and vice versa. The deferred scope must leave the graph in EXACTLY the
/// state sequential per-edit calls produce: same evaluation set, same
/// affected union.
#[test]
fn deferred_scope_equals_sequential_edits() {
    fn build() -> DependencyGraph {
        let mut graph = DependencyGraph::new();
        // Values A1..A6.
        for r in 1..=6u32 {
            graph
                .set_cell_value("Sheet1", r, 1, LiteralValue::Int(r as i64))
                .unwrap();
        }
        // Shared diamond over A1..A3: B1=A1+A2, B2=A2+A3, C1=B1+B2, D1=C1.
        graph
            .set_cell_formula("Sheet1", 1, 2, parse("=A1+A2").unwrap())
            .unwrap();
        graph
            .set_cell_formula("Sheet1", 2, 2, parse("=A2+A3").unwrap())
            .unwrap();
        graph
            .set_cell_formula("Sheet1", 1, 3, parse("=B1+B2").unwrap())
            .unwrap();
        graph
            .set_cell_formula("Sheet1", 1, 4, parse("=C1").unwrap())
            .unwrap();
        // Disjoint island: E1=A5, E2=E1+1. A4/A6 untouched by any formula.
        graph
            .set_cell_formula("Sheet1", 1, 5, parse("=A5").unwrap())
            .unwrap();
        graph
            .set_cell_formula("Sheet1", 2, 5, parse("=E1+1").unwrap())
            .unwrap();
        // B3 is a formula the edit batch will OVERWRITE with a value.
        graph
            .set_cell_formula("Sheet1", 3, 2, parse("=A1*2").unwrap())
            .unwrap();
        graph.clear_dirty_flags(&graph.get_evaluation_vertices());
        graph
    }

    // The edit batch: values into shared inputs (A1, A2), value over a
    // formula (B3), formula over a value (A6 -> formula reading A4), and a
    // disjoint-island input (A5).
    fn apply_edits(graph: &mut DependencyGraph) -> Vec<VertexId> {
        let mut affected = Vec::new();
        affected.extend(
            graph
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
                .unwrap()
                .affected_vertices,
        );
        affected.extend(
            graph
                .set_cell_value("Sheet1", 2, 1, LiteralValue::Int(20))
                .unwrap()
                .affected_vertices,
        );
        affected.extend(
            graph
                .set_cell_value("Sheet1", 3, 2, LiteralValue::Int(99))
                .unwrap()
                .affected_vertices,
        );
        affected.extend(
            graph
                .set_cell_formula("Sheet1", 6, 1, parse("=A4+1").unwrap())
                .unwrap()
                .affected_vertices,
        );
        affected.extend(
            graph
                .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(50))
                .unwrap()
                .affected_vertices,
        );
        affected
    }

    let mut g_seq = build();
    let mut seq_affected = apply_edits(&mut g_seq);
    seq_affected.sort_unstable();
    seq_affected.dedup();
    let mut seq_eval = g_seq.get_evaluation_vertices();
    seq_eval.sort_unstable();

    let mut g_def = build();
    g_def.begin_deferred_dirty();
    let _per_edit_sources = apply_edits(&mut g_def);
    let mut def_affected = g_def.end_deferred_dirty();
    def_affected.sort_unstable();
    def_affected.dedup();
    let mut def_eval = g_def.get_evaluation_vertices();
    def_eval.sort_unstable();

    assert_eq!(
        def_eval, seq_eval,
        "deferred scope must produce the identical dirty evaluation set"
    );
    assert_eq!(
        def_affected, seq_affected,
        "the flush's affected set must equal the union of sequential per-edit sets"
    );
}

/* ─────────────── nesting + no-leak / post-scope behavior ──────────────── */

#[test]
fn nested_scopes_flush_once_at_outermost_end() {
    let mut graph = DependencyGraph::new();
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    graph
        .set_cell_formula("Sheet1", 1, 2, parse("=A1+1").unwrap())
        .unwrap();
    graph.clear_dirty_flags(&graph.get_evaluation_vertices());

    graph.begin_deferred_dirty();
    graph.begin_deferred_dirty();
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
        .unwrap();
    let inner = graph.end_deferred_dirty();
    assert!(inner.is_empty(), "inner end must not flush");
    assert!(graph.deferred_dirty_active(), "outer scope still active");
    assert!(
        graph.get_evaluation_vertices().is_empty(),
        "no propagation may run while any scope is active"
    );
    let outer = graph.end_deferred_dirty();
    assert!(!graph.deferred_dirty_active());
    assert!(!outer.is_empty(), "outermost end flushes the propagation");
    assert_eq!(
        graph.get_evaluation_vertices().len(),
        1,
        "B1 dirty after flush"
    );
}

#[test]
fn propagation_is_normal_after_scope_ends() {
    // A single edit AFTER a closed scope must propagate immediately (a
    // dangling deferral would silently swallow it).
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    set_formula(&mut engine, "Sheet1", 1, 2, "=A1*10");
    engine.evaluate_all().unwrap();

    engine.begin_deferred_dirty();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
        .unwrap();
    engine.end_deferred_dirty();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0))
    );

    // Plain single edit, no scope: must propagate on its own.
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(3))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(30.0))
    );
}
