use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_core::parser::Parser;

// Validate COUNTIFS produces identical results with warmup disabled vs enabled (criteria via flats)
#[test]
fn countifs_fidelity_with_warmup_flats() {
    let mut wb = TestWorkbook::new();
    // Criteria range 1: Z:Z (text)
    wb = wb
        .with_cell("Sheet1", 1, 26, LiteralValue::Text("A".into()))
        .with_cell("Sheet1", 2, 26, LiteralValue::Text("B".into()))
        .with_cell("Sheet1", 3, 26, LiteralValue::Text("A".into()));
    // Criteria range 2: V:V (numbers)
    wb = wb
        .with_cell("Sheet1", 1, 22, LiteralValue::Number(1.0))
        .with_cell("Sheet1", 2, 22, LiteralValue::Number(1.0))
        .with_cell("Sheet1", 3, 22, LiteralValue::Number(2.0));

    let ast = Parser::from("=COUNTIFS(Z:Z, \"A\", V:V, 1)")
        .parse()
        .unwrap();

    // Engine A: warmup disabled
    let mut cfg_a = EvalConfig::default();
    cfg_a.warmup.warmup_enabled = false;
    cfg_a.range_expansion_limit = 100_000;
    let mut eng_a = Engine::new(wb, cfg_a);
    eng_a.set_cell_formula("Sheet1", 5, 5, ast.clone()).unwrap();
    let _ = eng_a.evaluate_cell("Sheet1", 5, 5).unwrap();
    let val_a = eng_a.get_cell_value("Sheet1", 5, 5).unwrap();

    // Engine B: warmup enabled (force flats)
    let mut wb2 = TestWorkbook::new();
    wb2 = wb2
        .with_cell("Sheet1", 1, 26, LiteralValue::Text("A".into()))
        .with_cell("Sheet1", 2, 26, LiteralValue::Text("B".into()))
        .with_cell("Sheet1", 3, 26, LiteralValue::Text("A".into()))
        .with_cell("Sheet1", 1, 22, LiteralValue::Number(1.0))
        .with_cell("Sheet1", 2, 22, LiteralValue::Number(1.0))
        .with_cell("Sheet1", 3, 22, LiteralValue::Number(2.0));
    let mut cfg_b = EvalConfig::default();
    cfg_b.warmup.warmup_enabled = true;
    cfg_b.warmup.min_flat_cells = 1;
    cfg_b.warmup.flat_reuse_threshold = 1;
    cfg_b.warmup.warmup_topk_refs = 10;
    cfg_b.range_expansion_limit = 100_000;
    let mut eng_b = Engine::new(wb2, cfg_b);
    eng_b.set_cell_formula("Sheet1", 5, 5, ast).unwrap();
    let _ = eng_b.evaluate_cell("Sheet1", 5, 5).unwrap();
    let val_b = eng_b.get_cell_value("Sheet1", 5, 5).unwrap();

    assert_eq!(
        val_a, val_b,
        "COUNTIFS result should match with/without criteria flats"
    );
}
