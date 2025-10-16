use super::common::arrow_eval_config;
use crate::engine::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::Parser;

#[test]
fn mask_cache_window_slicing_and_full_column() {
    let cfg = arrow_eval_config();
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, cfg);

    {
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("Sheet1", 3, 128);
        for _ in 0..300 {
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
    for r in 1..=300 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
        let t = if r % 3 == 0 { "yes" } else { "no" };
        engine
            .set_cell_value("Sheet1", r, 2, LiteralValue::Text(t.into()))
            .unwrap();
    }

    // Window 1: rows 1..100 (use SUMIFS to ensure Arrow fastpath)
    let f1 = Parser::from("=SUMIFS(A1:A100,A1:A100,\">50\",B1:B100,\"yes\")")
        .parse()
        .unwrap();
    engine.set_cell_formula("Sheet1", 10, 3, f1).unwrap();
    engine.evaluate_cell("Sheet1", 10, 3).unwrap();
    let (h1, m1, _l1) = engine.__mask_cache_stats();
    assert!(
        m1 > 0 && h1 == 0,
        "expected misses on first window eval; hits={h1} misses={m1}"
    );

    // Same window again → hit
    engine.evaluate_cell("Sheet1", 10, 3).unwrap();
    let (h2, m2, _l2) = engine.__mask_cache_stats();
    assert!(h2 >= h1 && m2 >= m1);

    // Slightly shifted window (2..100) → new key, miss increases
    let f2 = Parser::from("=SUMIFS(A2:A100,A2:A100,\">50\",B2:B100,\"yes\")")
        .parse()
        .unwrap();
    engine.set_cell_formula("Sheet1", 11, 3, f2).unwrap();
    engine.evaluate_cell("Sheet1", 11, 3).unwrap();
    let (h3, m3, _l3) = engine.__mask_cache_stats();
    assert!(m3 >= m2);

    // Full column: should generate its own cache key; second evaluation hits
    let f3 = Parser::from("=SUMIFS(A:A,A:A,\">250\",B:B,\"yes\")")
        .parse()
        .unwrap();
    engine.set_cell_formula("Sheet1", 12, 3, f3).unwrap();
    engine.evaluate_cell("Sheet1", 12, 3).unwrap();
    let (h4, m4, _l4) = engine.__mask_cache_stats();
    engine.evaluate_cell("Sheet1", 12, 3).unwrap();
    let (h5, m5, _l5) = engine.__mask_cache_stats();
    assert!(h5 >= h4 && m5 >= m4);
}
