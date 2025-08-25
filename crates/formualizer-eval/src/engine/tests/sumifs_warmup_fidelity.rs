use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_core::parser::Parser;

// Validate SUMIFS produces identical results with warmup disabled vs enabled (using flats)
#[test]
fn sumifs_fidelity_with_warmup_flats() {
    // Shared setup: small numeric-only sum range with two criteria ranges
    let mut base_wb = TestWorkbook::new();
    // sum range U:U (col 21) values
    base_wb = base_wb
        .with_cell("Sheet1", 1, 21, LiteralValue::Int(10))
        .with_cell("Sheet1", 2, 21, LiteralValue::Int(20))
        .with_cell("Sheet1", 3, 21, LiteralValue::Int(30));
    // criteria Z:Z (col 26) for equality ID
    base_wb = base_wb
        .with_cell("Sheet1", 1, 26, LiteralValue::Text("A".into()))
        .with_cell("Sheet1", 2, 26, LiteralValue::Text("B".into()))
        .with_cell("Sheet1", 3, 26, LiteralValue::Text("A".into()));
    // criteria V:V (col 22) for date const (use simple numbers for serials)
    base_wb = base_wb
        .with_cell("Sheet1", 1, 22, LiteralValue::Number(1.0))
        .with_cell("Sheet1", 2, 22, LiteralValue::Number(1.0))
        .with_cell("Sheet1", 3, 22, LiteralValue::Number(2.0));

    // Formula: =SUMIFS(U:U, Z:Z, "A", V:V, 1)
    let ast = Parser::from("=SUMIFS(U:U, Z:Z, \"A\", V:V, 1)")
        .parse()
        .expect("parse SUMIFS");

    // Engine A: warmup disabled (baseline)
    let mut cfg_a = EvalConfig::default();
    cfg_a.warmup.warmup_enabled = false;
    cfg_a.range_expansion_limit = 100_000;
    let mut eng_a = Engine::new(base_wb, cfg_a);
    eng_a
        .set_cell_formula("Sheet1", 10, 10, ast.clone())
        .unwrap();
    let _ = eng_a.evaluate_cell("Sheet1", 10, 10).expect("eval A");
    let val_a = eng_a.get_cell_value("Sheet1", 10, 10).unwrap();

    // Engine B: warmup enabled with small thresholds to force flats
    let mut cfg_b = EvalConfig::default();
    cfg_b.warmup.warmup_enabled = true;
    cfg_b.warmup.min_flat_cells = 1; // ensure tiny ranges are selected
    cfg_b.warmup.flat_reuse_threshold = 1;
    cfg_b.warmup.warmup_topk_refs = 10;
    cfg_b.range_expansion_limit = 100_000;
    // Rebuild the same workbook for engine B
    let mut base_wb2 = TestWorkbook::new();
    base_wb2 = base_wb2
        .with_cell("Sheet1", 1, 21, LiteralValue::Int(10))
        .with_cell("Sheet1", 2, 21, LiteralValue::Int(20))
        .with_cell("Sheet1", 3, 21, LiteralValue::Int(30))
        .with_cell("Sheet1", 1, 26, LiteralValue::Text("A".into()))
        .with_cell("Sheet1", 2, 26, LiteralValue::Text("B".into()))
        .with_cell("Sheet1", 3, 26, LiteralValue::Text("A".into()))
        .with_cell("Sheet1", 1, 22, LiteralValue::Number(1.0))
        .with_cell("Sheet1", 2, 22, LiteralValue::Number(1.0))
        .with_cell("Sheet1", 3, 22, LiteralValue::Number(2.0));
    let mut eng_b = Engine::new(base_wb2, cfg_b);
    eng_b.set_cell_formula("Sheet1", 10, 10, ast).unwrap();
    let _ = eng_b.evaluate_cell("Sheet1", 10, 10).expect("eval B");
    let val_b = eng_b.get_cell_value("Sheet1", 10, 10).unwrap();

    // Both evaluations should succeed and have identical values
    assert_eq!(
        val_a, val_b,
        "SUMIFS result should be identical with/without warmup flats"
    );
}
