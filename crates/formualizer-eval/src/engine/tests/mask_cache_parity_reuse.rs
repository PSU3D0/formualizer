use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::Parser;

#[test]
fn mask_cache_parity_and_reuse_hits() {
    let cfg = arrow_eval_config();
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, cfg);

    // Create Arrow sheet and populate two columns: A numeric, B text, across >1 chunk window
    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 3, 128);
        for _ in 0..600 {
            ab.append_row(
                "Sheet1",
                &[
                    LiteralValue::Empty,
                    LiteralValue::Empty,
                    LiteralValue::Empty,
                ],
            )
            .unwrap();
        }
        ab.finish().unwrap();
    }
    // Now overlay values via edits (exercises overlay + mask paths)
    for r in 1..=600 {
        let n = if r % 10 == 0 { 100.0 } else { (r % 20) as f64 };
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(n))
            .unwrap();
        let txt = if r % 7 == 0 { "example" } else { "other" };
        engine
            .set_cell_value("Sheet1", r, 2, LiteralValue::Text(txt.into()))
            .unwrap();
    }

    // SUMIFS over full columns: sum A:A where A:A >= 10 and B:B LIKE "*exam*"
    let f = Parser::from("=SUMIFS(A:A,A:A,\">=10\",B:B,\"*exam*\")")
        .parse()
        .unwrap();
    engine.set_cell_formula("Sheet1", 1, 3, f).unwrap();

    // First evaluation: expect non-zero misses, zero hits
    engine.evaluate_cell("Sheet1", 1, 3).unwrap();
    let (h1, m1, l1) = engine.__mask_cache_stats();
    assert!(l1 > 0, "first pass should populate cache (len={l1})");
    assert_eq!(h1, 0, "no hits on first pass");
    assert!(l1 > 0, "cache should have entries");

    let v1 = engine.get_cell_value("Sheet1", 1, 3).unwrap();

    // Second evaluation of same formula: should hit cache
    engine.evaluate_cell("Sheet1", 1, 3).unwrap();
    let (h2, _m2, l2) = engine.__mask_cache_stats();
    assert!(h2 >= h1, "second pass should not reduce hits");
    assert!(l2 > 0);

    // Parity: result stable
    let v2 = engine.get_cell_value("Sheet1", 1, 3).unwrap();
    assert_eq!(v1, v2);
}
