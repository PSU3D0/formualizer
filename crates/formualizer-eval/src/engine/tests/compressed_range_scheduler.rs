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
use formualizer_parse::parser::parse;

#[test]
fn demand_driven_enters_compressed_ranges() {
    // Ensure infinite/whole-column ranges remain compressed
    let cfg = EvalConfig {
        range_expansion_limit: 0,
        ..Default::default()
    };
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
    let p2 = parse("=B2").unwrap();
    engine.set_cell_formula("Sheet1", 2, 16, p2).unwrap();

    // Column S (19): S2 = D3 (formula)
    let s2 = parse("=D3").unwrap();
    engine.set_cell_formula("Sheet1", 2, 19, s2).unwrap();

    // D7 = SUMIF(S:S, D3, P:P)
    let d7 = parse("=SUMIF(S:S, D3, P:P)").unwrap();
    engine.set_cell_formula("Sheet1", 7, 4, d7).unwrap();

    // Demand-driven: only ask for D7. The engine should pull in P2/S2 automatically
    // through the compressed range dependency and produce 5.0.
    let _ = engine.evaluate_cell("Sheet1", 7, 4);
    match engine.get_cell_value("Sheet1", 7, 4) {
        Some(LiteralValue::Number(n)) => assert!((n - 5.0).abs() < 1e-9),
        other => panic!("Expected 5.0 for SUMIF, got {other:?}"),
    }
}
