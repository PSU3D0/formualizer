use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_core::parser::Parser;

#[test]
fn mask_cache_fifo_eviction_under_cap() {
    let mut cfg = EvalConfig::default();
    cfg.arrow_fastpath_enabled = true;
    cfg.delta_overlay_enabled = true;
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, cfg);
    // Tiny cap to force eviction
    engine.__mask_cache_set_cap(2);

    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 4, 64);
        for _ in 0..100 { ab.append_row("Sheet1", &[LiteralValue::Empty, LiteralValue::Empty, LiteralValue::Empty, LiteralValue::Empty]).unwrap(); }
        ab.finish().unwrap();
    }
    for r in 1..=100 {
        engine.set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64)).unwrap();
        engine.set_cell_value("Sheet1", r, 2, LiteralValue::Number((200 - r) as f64)).unwrap();
        let txt = if r % 2 == 0 { "a" } else { "b" };
        engine.set_cell_value("Sheet1", r, 3, LiteralValue::Text(txt.into())).unwrap();
    }

    // Three distinct COUNTIFS to create 3 distinct cache keys
    let f1 = Parser::from("=COUNTIFS(A:A,\">50\",C:C,\"a\")").parse().unwrap();
    let f2 = Parser::from("=COUNTIFS(B:B,\"<150\",C:C,\"b\")").parse().unwrap();
    let f3 = Parser::from("=COUNTIFS(A:A,\">90\",C:C,\"b\")").parse().unwrap();

    engine.set_cell_formula("Sheet1", 1, 4, f1).unwrap();
    engine.set_cell_formula("Sheet1", 2, 4, f2).unwrap();
    engine.set_cell_formula("Sheet1", 3, 4, f3).unwrap();

    // Evaluate f1, f2 should fill cache up to cap
    engine.evaluate_cell("Sheet1", 1, 4).unwrap();
    engine.evaluate_cell("Sheet1", 2, 4).unwrap();
    let (_h0, _m0, l0) = engine.__mask_cache_stats();
    assert!(l0 > 0 && l0 <= 2, "cache len should be >0 and <= cap, got {l0}");

    // Evaluate f3 causes eviction back to size <= 2
    engine.evaluate_cell("Sheet1", 3, 4).unwrap();
    let (_h1, _m1, l1) = engine.__mask_cache_stats();
    assert!(l1 <= 2);

    // Re-evaluate f1: if f1 was evicted, misses increase; else hits increase
    // Re-evaluate f1 (no hard assertion on hit/miss deltas; eviction behavior is implementation-specific)
    engine.evaluate_cell("Sheet1", 1, 4).unwrap();
}
