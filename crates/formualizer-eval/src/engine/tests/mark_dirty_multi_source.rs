//! Multi-source dirty propagation (`DependencyGraph::mark_dirty_many`).
//!
//! Loop-of-`mark_dirty` callers paid O(sources × component): every source
//! re-walked the whole dependent component. The volatile redirty was the
//! worst realistic shape (every recalc, for every volatile). The fix is one
//! BFS with a per-call shared seen-set — NOT an early-stop at already-dirty
//! vertices, because several call sites set the dirty flag without
//! propagating (`DependencyGraph::set_dirty`, `mark_dependents_dirty`,
//! names.rs invalidation), so "dirty" does not imply "dependents dirty".
//!
//! Work is asserted via `dirty_propagation_visits` (BFS visit counts), never
//! wall time.

use crate::engine::{CycleConfig, DependencyGraph, Engine, EvalConfig, VertexId};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;

fn set_formula(engine: &mut Engine<TestWorkbook>, sheet: &str, row: u32, col: u32, f: &str) {
    engine
        .set_cell_formula(sheet, row, col, parse(f).expect("parse"))
        .expect("set formula");
}

/* ───────────── volatile redirty visits the component once ─────────────── */

#[test]
fn volatile_redirty_is_one_component_walk_not_one_per_volatile() {
    // 50 volatiles all feeding one shared 200-formula chain. The per-recalc
    // volatile redirty used to run a full BFS per volatile: ≥ 50 × 200 =
    // 10_000 visits per recalc. One multi-source walk visits each vertex at
    // most once: ~(50 volatiles + 201 chain) ≈ 251 visits.
    crate::builtins::random::register_builtins();
    let volatiles = 50u32;
    let chain = 200u32;
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    for r in 1..=volatiles {
        set_formula(&mut engine, "Sheet1", r, 1, "=RAND()");
    }
    // B1 collects all volatiles; B2..B{chain+1} chain off it.
    set_formula(
        &mut engine,
        "Sheet1",
        1,
        2,
        &format!("=SUM(A1:A{volatiles})"),
    );
    for r in 2..=(chain + 1) {
        set_formula(&mut engine, "Sheet1", r, 2, &format!("=B{}+1", r - 1));
    }

    engine.evaluate_all().unwrap();
    let after_first = engine.graph.dirty_propagation_visits();
    let t = std::time::Instant::now();
    engine.evaluate_all().unwrap();
    let recalc = t.elapsed();
    let delta = engine.graph.dirty_propagation_visits() - after_first;
    eprintln!(
        "[mark-dirty-multi] volatile-heavy recalc: {recalc:?}, {delta} BFS visits \
         ({volatiles} volatiles × {chain}-chain component)"
    );

    let component = (volatiles + chain + 1) as u64;
    assert!(
        delta <= 2 * component,
        "second recalc's redirty must be ~one component walk \
         (≈{component} visits), got {delta} (quadratic was ≥ {})",
        (volatiles as u64) * (chain as u64)
    );
}

/* ───────────── iterative-SCC redirty stays one walk (no dirty-flag lean) ─ */

#[test]
fn iterative_scc_redirty_is_one_component_walk() {
    // 400-member ring: the per-recalc iterative redirty marks all members.
    // With the multi-source walk this is ~400 visits, independent of any
    // dirty-flag state left behind by other marking paths.
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_cycle(CycleConfig::iterate(100, 0.001)),
    );
    let size = 400u32;
    set_formula(&mut engine, "Sheet1", 1, 1, &format!("=0.5*A{size}+1"));
    for r in 2..=size {
        set_formula(&mut engine, "Sheet1", r, 1, &format!("=A{}", r - 1));
    }

    engine.evaluate_all().unwrap();
    let after_first = engine.graph.dirty_propagation_visits();
    engine.evaluate_all().unwrap();
    let delta = engine.graph.dirty_propagation_visits() - after_first;

    assert!(
        delta <= 3 * size as u64,
        "stable-ring recalc redirty must be ~one component walk \
         (≈{size} visits), got {delta} (quadratic was ≥ {})",
        (size as u64) * (size as u64) / 2
    );
}

/* ───────────── union semantics: multi-source == N single-source calls ─── */

/// Build the same small mesh twice and compare one `mark_dirty_many` over all
/// sources against sequential single-source calls: the affected union, the
/// dirty flags, and the evaluation set must be identical.
#[test]
fn mark_dirty_many_equals_sequential_single_source_marks() {
    fn build() -> (DependencyGraph, Vec<VertexId>) {
        use super::common::abs_cell_ref;
        let mut graph = DependencyGraph::new();
        // Values A1..A4; diamond B1=A1+A2, B2=A2+A3, C1=B1+B2, D1=C1, and an
        // untouched island E1=A4.
        for r in 1..=4u32 {
            graph
                .set_cell_value(
                    "Sheet1",
                    r,
                    1,
                    formualizer_common::LiteralValue::Int(r as i64),
                )
                .unwrap();
        }
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
        graph
            .set_cell_formula("Sheet1", 1, 5, parse("=A4").unwrap())
            .unwrap();
        // Sources: value cells A1, A2 (mixed kinds) and formula B2.
        let ids = [(1u32, 1u32), (2, 1), (2, 2)]
            .iter()
            .map(|&(r, c)| {
                *graph
                    .cell_to_vertex()
                    .get(&abs_cell_ref(0, r, c))
                    .expect("vertex")
            })
            .collect();
        (graph, ids)
    }

    let (mut g_multi, sources) = build();
    let mut multi_affected = g_multi.mark_dirty_many(&sources);
    multi_affected.sort_unstable();
    let mut multi_eval = g_multi.get_evaluation_vertices();
    multi_eval.sort_unstable();

    let (mut g_seq, sources_seq) = build();
    assert_eq!(sources, sources_seq, "deterministic vertex ids");
    let mut seq_affected: Vec<VertexId> = Vec::new();
    for &s in &sources_seq {
        seq_affected.extend(g_seq.mark_dirty_many(&[s]));
    }
    seq_affected.sort_unstable();
    seq_affected.dedup();
    let mut seq_eval = g_seq.get_evaluation_vertices();
    seq_eval.sort_unstable();

    assert_eq!(
        multi_affected, seq_affected,
        "multi-source affected set must equal the union of single-source sets"
    );
    assert_eq!(
        multi_eval, seq_eval,
        "dirty evaluation set must be identical"
    );
}
