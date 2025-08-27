use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_core::parser::Parser;

#[test]
fn mask_cache_invalidates_on_edit_snapshot() {
    let mut cfg = EvalConfig::default();
    cfg.arrow_fastpath_enabled = true;
    cfg.delta_overlay_enabled = true;
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, cfg);

    // Prepare Arrow sheet with 2 columns and 200 rows
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 3, 128);
        for _ in 0..200 { ab.append_row("Sheet1", &[LiteralValue::Empty, LiteralValue::Empty, LiteralValue::Empty]).unwrap(); }
        ab.finish().unwrap();
    }
    // Data: A numeric, B text
    for r in 1..=200 {
        engine.set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64)).unwrap();
        engine.set_cell_value("Sheet1", r, 2, LiteralValue::Text("foo".into())).unwrap();
    }
    // SUMIFS over bounded window to ensure Arrow fastpath dims alignment
    let f = Parser::from("=SUMIFS(A1:A200,A1:A200,\">150\",B1:B200,\"foo\")").parse().unwrap();
    engine.set_cell_formula("Sheet1", 1, 4, f).unwrap();

    engine.evaluate_cell("Sheet1", 1, 4).unwrap();
    let v_before = engine.get_cell_value("Sheet1", 1, 4).unwrap();
    let (h1, m1, _l1) = engine.__mask_cache_stats();
    assert!(m1 > 0 && h1 == 0, "expected misses on first eval; got hits={h1} misses={m1}");

    // Re-eval: hits should increase
    engine.evaluate_cell("Sheet1", 1, 4).unwrap();
    let (h2, m2, _l2) = engine.__mask_cache_stats();
    assert!(h2 >= h1);
    assert_eq!(m2, m1);

    // Now edit a relevant cell to change the result; this bumps snapshot
    // Change B160 from "foo" to "bar" (should reduce count by 1)
    engine.set_cell_value("Sheet1", 160, 2, LiteralValue::Text("bar".into())).unwrap();

    // After edit, snapshot changed; cache will be lazily cleared on next build

    // Re-evaluate; expect misses and new result
    engine.evaluate_cell("Sheet1", 1, 4).unwrap();
    let (h4, m4, _l4) = engine.__mask_cache_stats();
    assert!(m4 > 0);
    assert_eq!(h4, 0);

    let v_after = engine.get_cell_value("Sheet1", 1, 4).unwrap();
    // Value should decrease after edit (removing a matching row)
    if let (LiteralValue::Number(n_before), LiteralValue::Number(n_after)) = (v_before, v_after) {
        assert!(n_after < n_before, "sum after edit should decrease");
    }
}

#[test]
fn mask_cache_survives_overlay_compaction() {
    let mut cfg = EvalConfig::default();
    cfg.arrow_fastpath_enabled = true;
    cfg.delta_overlay_enabled = true;
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, cfg);

    // Prepare Arrow sheet with 2 columns and 512 rows
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 3, 128);
        for _ in 0..512 { ab.append_row("Sheet1", &[LiteralValue::Empty, LiteralValue::Empty, LiteralValue::Empty]).unwrap(); }
        ab.finish().unwrap();
    }
    // Fill a column and induce compaction by many edits in a chunk
    for r in 1..=512 { engine.set_cell_value("Sheet1", r, 1, LiteralValue::Number(0.0)).unwrap(); }
    for r in 1..=64 { engine.set_cell_value("Sheet1", r, 2, LiteralValue::Text("x".into())).unwrap(); }

    // SUMIFS sum A1:A512 where A1:A512==0 and B1:B512=="x"
    let f = Parser::from("=SUMIFS(A1:A512,A1:A512,\"=0\",B1:B512,\"x\")").parse().unwrap();
    engine.set_cell_formula("Sheet1", 1, 5, f).unwrap();

    engine.evaluate_cell("Sheet1", 1, 5).unwrap();
    let (h1, m1, l1) = engine.__mask_cache_stats();
    assert!(l1 > 0, "mask cache should have entries after first eval");

    // Force more edits in B to cross compaction thresholds
    for r in 65..=128 { engine.set_cell_value("Sheet1", r, 2, LiteralValue::Text("x".into())).unwrap(); }
    // Note: compaction happens internally; snapshot bumped by edits; cache will be cleared lazily on next build

    // Re-evaluate; result should reflect increased matches (sum remains 0 but mask count increases), and cache should populate again
    engine.evaluate_cell("Sheet1", 1, 5).unwrap();
    let (_h, _m, l) = engine.__mask_cache_stats();
    assert!(l > 0);
}
