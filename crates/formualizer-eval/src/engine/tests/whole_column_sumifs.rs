//! Test for SUMIFS with whole column references that have different used regions

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

#[test]
fn sumifs_whole_columns_different_used_regions() {
    // This test verifies that SUMIFS works correctly when whole column references
    // have different amounts of data (different used regions), which was causing
    // "range dims mismatch" errors before the padding fix.

    let wb = TestWorkbook::new();
    let mut engine = Engine::new(
        wb,
        EvalConfig {
            range_expansion_limit: 100_000, // Allow large ranges
            ..Default::default()
        },
    );

    // Set up data similar to the user's scenario:
    // Column P has data up to row 60256
    // Column K has data up to row 50035
    // This simulates the dimension mismatch issue

    // Add some sample data in column P (col 16) - more rows
    engine
        .set_cell_value("Sheet1", 100, 16, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 60256, 16, LiteralValue::Number(20.0))
        .unwrap();

    // Add data in column K (col 11) - fewer rows
    engine
        .set_cell_value(
            "Sheet1",
            100,
            11,
            LiteralValue::Text("Malpractice SC0279".into()),
        )
        .unwrap();
    engine
        .set_cell_value("Sheet1", 50035, 11, LiteralValue::Text("Other".into()))
        .unwrap();

    // Add data in column AV (col 48) - some other amount
    engine
        .set_cell_value("Sheet1", 100, 48, LiteralValue::Text("MatchValue".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 55000, 48, LiteralValue::Text("SomeValue".into()))
        .unwrap();

    // Add data in column R (col 18) - dates
    engine
        .set_cell_value("Sheet1", 100, 18, LiteralValue::Number(44562.0))
        .unwrap(); // Some date serial

    // Create a SUMIFS formula similar to the user's case
    // =SUMIFS(P:P, K:K, "Malpractice SC0279", AV:AV, "MatchValue")
    let formula =
        parse("=SUMIFS(P:P, K:K, \"Malpractice SC0279\", AV:AV, \"MatchValue\")").unwrap();

    engine.set_cell_formula("Sheet1", 1, 1, formula).unwrap();

    // This should not error with "range dims mismatch" anymore
    let result = engine.evaluate_cell("Sheet1", 1, 1);

    // Should succeed without dimension mismatch error
    assert!(
        result.is_ok(),
        "SUMIFS with different column lengths should not error"
    );

    // The result should be 10.0 (only row 100 matches both criteria)
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1).unwrap(),
        LiteralValue::Number(10.0)
    );
}

#[test]
fn sumifs_whole_columns_empty_vs_populated() {
    // Test edge case where one column is completely empty
    let config = EvalConfig::default();
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, config);

    // Column A has data
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(100.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1000, 1, LiteralValue::Number(200.0))
        .unwrap();

    // Column B has criteria values
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("Yes".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1000, 2, LiteralValue::Text("Yes".into()))
        .unwrap();

    // Column C is empty (but referenced in formula)
    // Column D has criteria for column C

    // SUMIFS with empty column reference should still work
    let formula = parse("=SUMIFS(A:A, B:B, \"Yes\", C:C, \"\")").unwrap();

    engine.set_cell_formula("Sheet1", 2, 1, formula).unwrap();

    let result = engine.evaluate_cell("Sheet1", 2, 1);
    assert!(result.is_ok(), "SUMIFS with empty column should not error");

    // Result should be 300 (both rows match "Yes" and empty matches empty)
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1).unwrap(),
        LiteralValue::Number(300.0)
    );
}
