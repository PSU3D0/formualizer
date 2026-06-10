//! Regression tests for issue #125: per-edit full CSR rebuilds made
//! cell-by-cell formula edits O(N^2).
//!
//! The fix amortizes CSR rebuilds (threshold-based instead of per
//! `end_batch`/`add_vertex`) and makes dependent (reverse-edge) reads
//! delta-aware so correctness never depends on an eager rebuild.
//!
//! These tests use a rebuild counter rather than wall-clock time so they are
//! stable in CI.

use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn make_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), EvalConfig::default())
}

/// Run `n` per-cell formula edits (each referencing the cell to its left) and
/// return the number of CSR rebuilds the burst triggered.
fn rebuilds_for_edit_burst(n: u32) -> u64 {
    let mut engine = make_engine();
    // Seed input column with values.
    for row in 1..=n {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Int(row as i64))
            .unwrap();
    }
    let before = engine.graph.edges_rebuild_count();
    for row in 1..=n {
        let ast = parse(format!("=A{row}+1")).unwrap();
        engine.set_cell_formula("Sheet1", row, 2, ast).unwrap();
    }
    engine.graph.edges_rebuild_count() - before
}

/// Per-cell formula edits must not trigger a CSR rebuild per edit. With the
/// 1000-op delta threshold, a burst of N single-dependency edits should
/// rebuild roughly N/1000 times (each edit contributes a small constant
/// number of delta ops), not N times.
#[test]
fn per_cell_formula_edits_amortize_csr_rebuilds() {
    let n = 2000u32;
    let rebuilds = rebuilds_for_edit_burst(n);
    // Each edit contributes ~2 delta ops (placeholder churn + 1 dependency
    // edge), so allow a generous multiple of total_ops/threshold. The point
    // is to reject anything close to one rebuild per edit.
    assert!(
        rebuilds <= 20,
        "expected amortized rebuilds (~total_ops/1000) for {n} per-cell edits, got {rebuilds}"
    );
}

/// Rebuild count must scale linearly with edit count (counter-based stand-in
/// for the wall-clock scaling assertion, which is too noisy for CI).
#[test]
fn csr_rebuild_count_scales_linearly_with_edits() {
    let small = rebuilds_for_edit_burst(2000);
    let large = rebuilds_for_edit_burst(8000);
    // 4x the edits should mean ~4x the rebuilds (plus slack for off-by-one
    // threshold crossings) — not 16x as the old per-edit rebuild produced.
    assert!(
        large <= 4 * small + 8,
        "rebuild count grew superlinearly: {small} rebuilds at 2k edits vs {large} at 8k"
    );
}

/// Dependent (reverse-edge) reads must observe edits that have not yet been
/// folded into the CSR base, without falling back to an O(V) scan or
/// requiring an explicit rebuild.
#[test]
fn unrebuilt_edits_are_visible_to_dependent_reads() {
    let mut engine = make_engine();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(3))
        .unwrap();

    // B1 = A1 (single edit: stays in the delta slab, below rebuild threshold)
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1").unwrap())
        .unwrap();

    let sheet_id = engine.graph.sheet_id("Sheet1").unwrap();
    let a1 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sheet_id, Coord::from_excel(1, 1, true, true)))
        .unwrap();
    let b1 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sheet_id, Coord::from_excel(1, 2, true, true)))
        .unwrap();
    let c1 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sheet_id, Coord::from_excel(1, 3, true, true)))
        .unwrap();

    assert!(
        engine.graph.get_dependents(a1).contains(&b1),
        "freshly added edge A1<-B1 must be visible to dependent reads"
    );

    // Re-point B1 at C1: A1 loses the dependent, C1 gains it — again without
    // any rebuild in between.
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=C1").unwrap())
        .unwrap();
    assert!(
        !engine.graph.get_dependents(a1).contains(&b1),
        "removed edge A1<-B1 must disappear from dependent reads"
    );
    assert!(
        engine.graph.get_dependents(c1).contains(&b1),
        "new edge C1<-B1 must be visible to dependent reads"
    );
}

/// Editing a value with un-rebuilt formula edges pending must still dirty the
/// dependent formulas (mark_dirty consumes reverse edges).
#[test]
fn dirty_propagation_sees_unrebuilt_edges() {
    let mut engine = make_engine();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1*10").unwrap())
        .unwrap();

    let sheet_id = engine.graph.sheet_id("Sheet1").unwrap();
    let b1 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sheet_id, Coord::from_excel(1, 2, true, true)))
        .unwrap();

    // Value edit while the B1->A1 edge is still in the delta slab.
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(5))
        .unwrap();
    assert!(
        engine.graph.get_evaluation_vertices().contains(&b1),
        "dirty propagation must reach B1 through the un-rebuilt edge"
    );

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(50.0))
    );
}

/// End-to-end: edit -> evaluate -> re-edit dependencies -> evaluate. The
/// scheduler must respect fresh dependency edges after every edit burst.
#[test]
fn edit_then_evaluate_respects_fresh_dependencies() {
    let mut engine = make_engine();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(7))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1+1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(3.0))
    );

    // Re-point at C1; the old A1 edge must be gone and the new C1 edge live.
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(8.0))
    );

    // Editing A1 must no longer dirty B1; editing C1 must.
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(40))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(41.0))
    );
}

/// Long per-cell edit bursts followed by evaluation produce correct results
/// (the scheduling seam flushes pending deltas exactly once).
#[test]
fn edit_burst_then_evaluate_is_correct() {
    let n = 256u32;
    let mut engine = make_engine();
    for row in 1..=n {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Int(row as i64))
            .unwrap();
    }
    for row in 1..=n {
        let ast = parse(format!("=A{row}*2")).unwrap();
        engine.set_cell_formula("Sheet1", row, 2, ast).unwrap();
    }
    engine.evaluate_all().unwrap();
    for row in 1..=n {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, 2),
            Some(LiteralValue::Number(row as f64 * 2.0)),
            "row {row}"
        );
    }
}
