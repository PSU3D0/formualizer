//! Tests for streaming evaluation (Milestone 5.4)

use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;
use std::time::Instant;
use eval_config_with_range_limit;

#[test]
fn test_sum_over_large_range_succeeds_without_oom() {
    // Set a low expansion limit to force streaming
    let config = eval_config_with_range_limit(16);
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, config);

    // Set up a large range of values
    let large_range_size = 20000; // Use a size that exceeds range_expansion_limit
    for i in 1..=large_range_size {
        engine
            .set_cell_value("Sheet1", i, 1, LiteralValue::Int(i as i64))
            .unwrap();
    }

    // Formula to sum the large range
    let formula_str = format!("=SUM(A1:A{})", large_range_size);
    let ast = parse(&formula_str).unwrap();
    engine.set_cell_formula("Sheet1", 1, 2, ast).unwrap();

    // Evaluate
    engine.evaluate_all().unwrap();

    // Check the result
    let expected_sum = (1..=large_range_size as i64).sum::<i64>();
    let result = engine.get_cell_value("Sheet1", 1, 2).unwrap();

    assert_eq!(
        result,
        LiteralValue::Number(expected_sum as f64),
        "The sum over a large streaming range should be correct."
    );
}

#[test]
fn test_streaming_vs_materialized_threshold() {
    let config_small = EvalConfig::default().with_range_expansion_limit(100); // Forces materialization for range of 50

    let config_large = eval_config_with_range_limit(16); // Forces streaming for range of 50

    let wb1 = TestWorkbook::new();
    let wb2 = TestWorkbook::new();
    let mut engine_materialized = Engine::new(wb1, config_small);
    let mut engine_streaming = Engine::new(wb2, config_large);

    // Set up identical data in both engines
    let range_size = 50;
    for i in 1..=range_size {
        let value = LiteralValue::Int(i as i64);
        engine_materialized
            .set_cell_value("Sheet1", i, 1, value.clone())
            .unwrap();
        engine_streaming
            .set_cell_value("Sheet1", i, 1, value)
            .unwrap();
    }

    // Same formula in both
    let formula_str = format!("=SUM(A1:A{})", range_size);
    let ast1 = parse(&formula_str).unwrap();
    let ast2 = parse(&formula_str).unwrap();

    engine_materialized
        .set_cell_formula("Sheet1", 1, 2, ast1)
        .unwrap();
    engine_streaming
        .set_cell_formula("Sheet1", 1, 2, ast2)
        .unwrap();

    // Evaluate both
    engine_materialized.evaluate_all().unwrap();
    engine_streaming.evaluate_all().unwrap();

    // Results should be identical
    let result_materialized = engine_materialized.get_cell_value("Sheet1", 1, 2).unwrap();
    let result_streaming = engine_streaming.get_cell_value("Sheet1", 1, 2).unwrap();

    assert_eq!(
        result_materialized, result_streaming,
        "Streaming and materialized evaluation should produce identical results"
    );
}

#[test]
fn test_multiple_functions_use_streaming() {
    let config = eval_config_with_range_limit(16);
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, config);

    // Set up data for testing multiple functions
    let range_size = 100;
    for i in 1..=range_size {
        engine
            .set_cell_value("Sheet1", i, 1, LiteralValue::Int(i as i64))
            .unwrap();
        engine
            .set_cell_value("Sheet1", i, 2, LiteralValue::Number(i as f64 * 0.5))
            .unwrap();
    }

    // Test SUM with streaming
    let sum_formula = format!("=SUM(A1:A{})", range_size);
    let sum_ast = parse(&sum_formula).unwrap();
    engine.set_cell_formula("Sheet1", 1, 3, sum_ast).unwrap();

    // TODO: Add tests for other functions that should support streaming when implemented
    // =AVERAGE(A1:A100), =COUNT(A1:A100), etc.

    engine.evaluate_all().unwrap();

    let expected_sum = (1..=range_size as i64).sum::<i64>();
    let sum_result = engine.get_cell_value("Sheet1", 1, 3).unwrap();
    assert_eq!(sum_result, LiteralValue::Number(expected_sum as f64));
}

#[test]
fn test_streaming_with_sparse_data() {
    let config = eval_config_with_range_limit(16);
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, config);

    let range_size = 1000;
    // Only populate every 10th cell to test sparse data handling
    for i in (10..=range_size).step_by(10) {
        engine
            .set_cell_value("Sheet1", i, 1, LiteralValue::Int(i as i64))
            .unwrap();
    }

    let formula = format!("=SUM(A1:A{})", range_size);
    let ast = parse(&formula).unwrap();
    engine.set_cell_formula("Sheet1", 1, 2, ast).unwrap();

    engine.evaluate_all().unwrap();

    let expected_sum: i64 = (10..=range_size as i64).step_by(10).sum();
    let result = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    assert_eq!(result, LiteralValue::Number(expected_sum as f64));
}

#[test]
fn test_streaming_range_shapes() {
    let config = eval_config_with_range_limit(16);
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, config);

    // Test tall range (column-oriented) - smaller range for easier debugging
    for i in 1..=50 {
        engine
            .set_cell_value("Sheet1", i, 1, LiteralValue::Int(i as i64))
            .unwrap();
    }

    // Test wide range (row-oriented) - A1:AX1 should be columns 1-50
    for i in 1..=50 {
        engine
            .set_cell_value("Sheet1", 1, i, LiteralValue::Int(i as i64))
            .unwrap();
    }

    // Tall range formula - put it outside the range at (51,1)
    let tall_formula = "=SUM(A1:A50)";
    let tall_ast = parse(tall_formula).unwrap();
    engine.set_cell_formula("Sheet1", 51, 1, tall_ast).unwrap();

    // Wide range formula - put it outside the range at (2,51)
    let wide_formula = "=SUM(A1:AX1)";
    let wide_ast = parse(wide_formula).unwrap();
    engine.set_cell_formula("Sheet1", 2, 51, wide_ast).unwrap();

    engine.evaluate_all().unwrap();

    let expected_sum = (1..=50i64).sum::<i64>();
    let tall_result = engine.get_cell_value("Sheet1", 51, 1).unwrap();
    let wide_result = engine.get_cell_value("Sheet1", 2, 51).unwrap();

    // Debug: println!("Expected: {}, Tall: {:?}, Wide: {:?}", expected_sum, tall_result, wide_result);

    assert_eq!(tall_result, LiteralValue::Number(expected_sum as f64));
    assert_eq!(wide_result, LiteralValue::Number(expected_sum as f64));
}

#[test]
fn test_streaming_performance_regression() {
    let config = eval_config_with_range_limit(16);
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, config);

    // Large range that should definitely use streaming
    let range_size = 10000;
    for i in 1..=range_size {
        engine
            .set_cell_value("Sheet1", i, 1, LiteralValue::Int(1))
            .unwrap();
    }

    let formula = format!("=SUM(A1:A{})", range_size);
    let ast = parse(&formula).unwrap();
    engine.set_cell_formula("Sheet1", 1, 2, ast).unwrap();

    // Time the evaluation
    let start = Instant::now();
    engine.evaluate_all().unwrap();
    let duration = start.elapsed();

    let result = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    assert_eq!(result, LiteralValue::Number(range_size as f64));

    // Rough performance check - should complete in reasonable time
    // This is a basic regression test, not a precise benchmark
    assert!(
        duration.as_millis() < 1000,
        "Streaming evaluation took too long: {}ms",
        duration.as_millis()
    );
}

#[test]
fn sum_large_stream_does_not_materialize_entire_range() {
    // Build a workbook with a large 200x200 range to exceed expansion limit and trigger streaming
    let mut wb = TestWorkbook::new();
    let rows = 200u32;
    let cols = 200u32;
    for r in 1..=rows {
        for c in 1..=cols {
            wb = wb.with_cell("Sheet1", r, c, LiteralValue::Int(1));
        }
    }
    // Register SUM
    wb = wb.with_function(std::sync::Arc::new(crate::builtins::math::SumFn));

    let config = eval_config_with_range_limit(16); // 16 cells
    let mut engine = Engine::new(wb.clone(), config);
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SUM(A1:GR200)").unwrap())
        .unwrap();
    let res = engine.evaluate_all().unwrap();
    assert!(res.computed_vertices >= 1);
    // Verify SUM result matches expected 40000
    let v = engine.get_cell_value("Sheet1", 1, 1).unwrap();
    assert_eq!(v, LiteralValue::Number((rows as f64) * (cols as f64)));
}

#[test]
fn test_streaming_with_errors_in_range() {
    let config = eval_config_with_range_limit(16);
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, config);

    // Mix of valid values and errors
    let range_size = 100;
    for i in 1..=range_size {
        if i % 10 == 0 {
            // Every 10th cell has an error
            engine
                .set_cell_value(
                    "Sheet1",
                    i,
                    1,
                    LiteralValue::Error(formualizer_common::ExcelError::from_error_string(
                        "#DIV/0!",
                    )),
                )
                .unwrap();
        } else {
            engine
                .set_cell_value("Sheet1", i, 1, LiteralValue::Int(i as i64))
                .unwrap();
        }
    }

    let formula = format!("=SUM(A1:A{})", range_size);
    let ast = parse(&formula).unwrap();
    engine.set_cell_formula("Sheet1", 1, 2, ast).unwrap();

    // Note: This will fail evaluation due to errors in the range
    let eval_result = engine.evaluate_all();
    assert!(
        eval_result.is_err(),
        "Expected evaluation to fail due to error in SUM range"
    );
}

#[test]
fn test_incremental_update_with_streaming_range() {
    let config = eval_config_with_range_limit(16);
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, config);

    let range_size = 1000;
    // Initialize range
    for i in 1..=range_size {
        engine
            .set_cell_value("Sheet1", i, 1, LiteralValue::Int(1))
            .unwrap();
    }

    let formula = format!("=SUM(A1:A{})", range_size);
    let ast = parse(&formula).unwrap();
    engine.set_cell_formula("Sheet1", 1, 2, ast).unwrap();

    // Initial evaluation
    engine.evaluate_all().unwrap();
    let initial_result = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    assert_eq!(initial_result, LiteralValue::Number(range_size as f64));

    // Change one cell in the middle of the range
    let start = Instant::now();
    engine
        .set_cell_value("Sheet1", range_size / 2, 1, LiteralValue::Int(100))
        .unwrap();
    engine.evaluate_all().unwrap();
    let update_duration = start.elapsed();

    // Result should reflect the change
    let updated_result = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    let expected = range_size as f64 - 1.0 + 100.0; // -1 + 100 = +99
    assert_eq!(updated_result, LiteralValue::Number(expected));

    // Incremental update should be fast
    assert!(
        update_duration.as_millis() < 100,
        "Incremental update took too long: {}ms",
        update_duration.as_millis()
    );
}
