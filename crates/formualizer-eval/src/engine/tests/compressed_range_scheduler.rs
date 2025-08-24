//! Reproducer for demand-driven scheduling with compressed (infinite) ranges.
//!
//! Scenario:
//! - Column `S:S` holds text values produced by formulas (e.g., referencing another cell)
//! - Column `P:P` holds numeric values produced by formulas (e.g., =B2)
//! - Cell `D7` has `=SUMIF(S:S, D3, P:P)`
//! - When evaluating only `D7` demand-driven, the engine must schedule and
//!   compute the formula cells in P/S within the used region before aggregating.
//!
//! Previously, compressed range dependencies were not surfaced to the demand-driven
//! traversal, so `D7` could see Empty/0 for P/S until those inner cells were
//! explicitly evaluated first. This test locks the desired behavior: one-shot
//! demand-driven evaluation of the SUMIF should yield the correct result.

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_core::parser::Parser;

#[test]
fn demand_driven_enters_compressed_ranges() {
    let mut cfg = EvalConfig::default();
    // Ensure infinite/whole-column ranges remain compressed
    cfg.range_expansion_limit = 0;
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, cfg);

    // Inputs: D3 is the criteria value
    engine
        .set_cell_value("Sheet1", 3, 4, LiteralValue::Text("X".into()))
        .unwrap(); // D3="X"
    // Helper value feeding P2 via a formula
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(5.0))
        .unwrap(); // B2=5

    // Column P (16): P2 = B2 (formula)
    let p2 = Parser::from("=B2").parse().unwrap();
    engine.set_cell_formula("Sheet1", 2, 16, p2).unwrap();

    // Column S (19): S2 = D3 (formula)
    let s2 = Parser::from("=D3").parse().unwrap();
    engine.set_cell_formula("Sheet1", 2, 19, s2).unwrap();

    // D7 = SUMIF(S:S, D3, P:P)
    let d7 = Parser::from("=SUMIF(S:S, D3, P:P)").parse().unwrap();
    engine.set_cell_formula("Sheet1", 7, 4, d7).unwrap();

    // Demand-driven: only ask for D7. The engine should pull in P2/S2 automatically
    // through the compressed range dependency and produce 5.0.
    let _ = engine.evaluate_cell("Sheet1", 7, 4);
    match engine.get_cell_value("Sheet1", 7, 4) {
        Some(LiteralValue::Number(n)) => assert!((n - 5.0).abs() < 1e-9),
        other => panic!("Expected 5.0 for SUMIF, got {:?}", other),
    }
}

#[test]
fn scheduler_layers_respect_range_formulas_first() {
    let mut cfg = EvalConfig::default();
    cfg.range_expansion_limit = 0;
    cfg.enable_parallel = false; // deterministic for assertions
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, cfg);

    // Setup: P2 = B2, S2 = D3, D7 = SUMIF(S:S, D3, P:P)
    engine
        .set_cell_value("Sheet1", 3, 4, LiteralValue::Text("X".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 16, Parser::from("=B2").parse().unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 19, Parser::from("=D3").parse().unwrap())
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            7,
            4,
            Parser::from("=SUMIF(S:S, D3, P:P)").parse().unwrap(),
        )
        .unwrap();

    let plan = engine.get_eval_plan(&[("Sheet1", 7, 4)]).expect("plan");
    // Locate indices by cell name
    let find_idx = |s: &str| -> Option<usize> {
        plan.layers
            .iter()
            .position(|ly| ly.sample_cells.iter().any(|c| c == s))
    };
    let d7_idx = find_idx("Sheet1!D7").expect("D7 not found");
    let p2_idx = find_idx("Sheet1!P2").expect("P2 not found");
    let s2_idx = find_idx("Sheet1!S2").expect("S2 not found");
    assert!(p2_idx < d7_idx, "P2 should be before D7");
    assert!(s2_idx < d7_idx, "S2 should be before D7");
}

#[test]
fn scheduler_layers_recursive_range_producers() {
    let mut cfg = EvalConfig::default();
    cfg.range_expansion_limit = 0;
    cfg.enable_parallel = false;
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, cfg);

    // Q2 = B2 (formula)
    engine
        .set_cell_value("Sheet1", 2, 2, LiteralValue::Number(7.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 17, Parser::from("=B2").parse().unwrap())
        .unwrap(); // Q column is col 17

    // P2 = SUM(Q:Q) (uses its own compressed range)
    engine
        .set_cell_formula("Sheet1", 2, 16, Parser::from("=SUM(Q:Q)").parse().unwrap())
        .unwrap();

    // D7 = SUMIF(S:S, "", P:P) but make S2 empty to avoid filtering P2 out
    engine
        .set_cell_value("Sheet1", 2, 19, LiteralValue::Text("".into()))
        .unwrap(); // S2 empty
    engine
        .set_cell_formula(
            "Sheet1",
            7,
            4,
            Parser::from("=SUMIF(S:S, \"\", P:P)").parse().unwrap(),
        )
        .unwrap();

    let plan = engine.get_eval_plan(&[("Sheet1", 7, 4)]).expect("plan");
    let idx = |cell: &str| -> usize {
        plan.layers
            .iter()
            .position(|ly| ly.sample_cells.iter().any(|c| c == cell))
            .unwrap_or(usize::MAX)
    };
    let q2 = idx("Sheet1!Q2");
    let p2 = idx("Sheet1!P2");
    let d7 = idx("Sheet1!D7");
    assert!(
        q2 < p2 && p2 < d7,
        "Expected Q2 < P2 < D7, got {:?}",
        (q2, p2, d7)
    );
}
