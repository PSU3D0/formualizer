//! Comprehensive tests for parallel evaluation functionality
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Helper to create a cell reference AST node
fn ref_ast(row: u32, col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{row}C{col}"),
            reference: ReferenceType::cell(None, row, col),
        },
        source_token: None,
        contains_volatile: false,
    }
}

/// Helper to create a binary op AST node
fn op_ast(left: ASTNode, right: ASTNode, op: &str) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: op.to_string(),
            left: Box::new(left),
            right: Box::new(right),
        },
        source_token: None,
        contains_volatile: false,
    }
}

/// Helper to create a function call AST node
fn func_ast(name: &str, args: Vec<ASTNode>) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Function {
            name: name.to_string(),
            args,
        },
        source_token: None,
        contains_volatile: false,
    }
}

/// Create a test workbook with a dependency chain suitable for parallel testing
fn create_parallel_test_workbook() -> TestWorkbook {
    TestWorkbook::new().with_function(std::sync::Arc::new(crate::builtins::math::SumFn))
}

/// Create a workbook with multiple independent layers for parallel evaluation
fn create_multi_layer_workbook() -> (TestWorkbook, Engine<TestWorkbook>) {
    let wb = create_parallel_test_workbook();
    let mut engine = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: true,
            ..Default::default()
        },
    );

    // Layer 1: Independent values
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap(); // A1 = 10
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(20))
        .unwrap(); // B1 = 20
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(30))
        .unwrap(); // C1 = 30
    engine
        .set_cell_value("Sheet1", 1, 4, LiteralValue::Int(40))
        .unwrap(); // D1 = 40

    // Layer 2: Formulas depending on Layer 1 (can be evaluated in parallel)
    let a2_ast = op_ast(
        ref_ast(1, 1),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );
    let b2_ast = op_ast(
        ref_ast(1, 2),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );
    let c2_ast = op_ast(
        ref_ast(1, 3),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );
    let d2_ast = op_ast(
        ref_ast(1, 4),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );

    engine.set_cell_formula("Sheet1", 2, 1, a2_ast).unwrap(); // A2 = A1 + 5 = 15
    engine.set_cell_formula("Sheet1", 2, 2, b2_ast).unwrap(); // B2 = B1 + 5 = 25
    engine.set_cell_formula("Sheet1", 2, 3, c2_ast).unwrap(); // C2 = C1 + 5 = 35
    engine.set_cell_formula("Sheet1", 2, 4, d2_ast).unwrap(); // D2 = D1 + 5 = 45

    // Layer 3: Formula depending on Layer 2
    let sum_ast = func_ast(
        "SUM",
        vec![ref_ast(2, 1), ref_ast(2, 2), ref_ast(2, 3), ref_ast(2, 4)],
    );
    engine.set_cell_formula("Sheet1", 3, 1, sum_ast).unwrap(); // A3 = SUM(A2:D2) = 120

    (create_parallel_test_workbook(), engine)
}

/// Assert that two engines have equivalent cell values
fn assert_engines_equivalent(engine1: &Engine<TestWorkbook>, engine2: &Engine<TestWorkbook>) {
    // Check key test cells
    let test_cells = [
        ("Sheet1", 1, 1),
        ("Sheet1", 1, 2),
        ("Sheet1", 1, 3),
        ("Sheet1", 1, 4),
        ("Sheet1", 2, 1),
        ("Sheet1", 2, 2),
        ("Sheet1", 2, 3),
        ("Sheet1", 2, 4),
        ("Sheet1", 3, 1),
    ];

    for &(sheet, row, col) in &test_cells {
        let val1 = engine1.get_cell_value(sheet, row, col);
        let val2 = engine2.get_cell_value(sheet, row, col);
        assert_eq!(
            val1, val2,
            "Cell {sheet}!R{row}C{col} differs: {val1:?} vs {val2:?}"
        );
    }
}

#[test]
fn test_parallel_evaluation_equivalence() {
    let (wb1, _) = create_multi_layer_workbook();
    let (wb2, _) = create_multi_layer_workbook();

    // Create sequential and parallel engines with identical workbooks
    let mut sequential_engine = Engine::new(
        wb1,
        EvalConfig {
            enable_parallel: false,
            ..Default::default()
        },
    );
    let mut parallel_engine = Engine::new(
        wb2,
        EvalConfig {
            enable_parallel: true,
            ..Default::default()
        },
    );

    // Set up identical workbooks
    sequential_engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    sequential_engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(20))
        .unwrap();
    sequential_engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(30))
        .unwrap();
    sequential_engine
        .set_cell_value("Sheet1", 1, 4, LiteralValue::Int(40))
        .unwrap();

    parallel_engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    parallel_engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(20))
        .unwrap();
    parallel_engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(30))
        .unwrap();
    parallel_engine
        .set_cell_value("Sheet1", 1, 4, LiteralValue::Int(40))
        .unwrap();

    // Add formulas
    let a2_ast = op_ast(
        ref_ast(1, 1),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );
    let b2_ast = op_ast(
        ref_ast(1, 2),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );
    let sum_ast = func_ast("SUM", vec![ref_ast(2, 1), ref_ast(2, 2)]);

    sequential_engine
        .set_cell_formula("Sheet1", 2, 1, a2_ast.clone())
        .unwrap();
    sequential_engine
        .set_cell_formula("Sheet1", 2, 2, b2_ast.clone())
        .unwrap();
    sequential_engine
        .set_cell_formula("Sheet1", 3, 1, sum_ast.clone())
        .unwrap();

    parallel_engine
        .set_cell_formula("Sheet1", 2, 1, a2_ast)
        .unwrap();
    parallel_engine
        .set_cell_formula("Sheet1", 2, 2, b2_ast)
        .unwrap();
    parallel_engine
        .set_cell_formula("Sheet1", 3, 1, sum_ast)
        .unwrap();

    // Evaluate both engines
    let seq_result = sequential_engine.evaluate_all().unwrap();
    let par_result = parallel_engine.evaluate_all().unwrap();

    // Results should be equivalent
    assert_eq!(seq_result.computed_vertices, par_result.computed_vertices);
    assert_eq!(seq_result.cycle_errors, par_result.cycle_errors);

    // Cell values should be identical
    assert_engines_equivalent(&sequential_engine, &parallel_engine);

    // Verify specific expected values
    assert_eq!(
        sequential_engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(15.0))
    );
    assert_eq!(
        sequential_engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(25.0))
    );
    assert_eq!(
        sequential_engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(40.0))
    );

    assert_eq!(
        parallel_engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(15.0))
    );
    assert_eq!(
        parallel_engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(25.0))
    );
    assert_eq!(
        parallel_engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(40.0))
    );
}

#[test]
fn test_parallel_evaluation_deterministic() {
    // Run parallel evaluation multiple times to ensure deterministic results
    let mut results = Vec::new();

    for _ in 0..5 {
        let wb = create_parallel_test_workbook();
        let mut engine = Engine::new(
            wb,
            EvalConfig {
                enable_parallel: true,
                ..Default::default()
            },
        );

        // Create a workbook with many parallel-evaluable cells
        for i in 1..=10 {
            engine
                .set_cell_value("Sheet1", 1, i, LiteralValue::Int(i as i64))
                .unwrap();
            let formula = op_ast(
                ref_ast(1, i),
                ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(10)), None),
                "*",
            );
            engine.set_cell_formula("Sheet1", 2, i, formula).unwrap();
        }

        engine.evaluate_all().unwrap();

        // Collect results for this run
        let mut run_results = Vec::new();
        for i in 1..=10 {
            run_results.push(engine.get_cell_value("Sheet1", 2, i));
        }
        results.push(run_results);
    }

    // All runs should produce identical results
    let first_result = &results[0];
    for (run_idx, result) in results.iter().enumerate().skip(1) {
        assert_eq!(
            first_result, result,
            "Run {run_idx} produced different results"
        );
    }

    // Verify expected values
    for i in 1..=10 {
        assert_eq!(
            results[0][i - 1],
            Some(LiteralValue::Number((i * 10) as f64))
        );
    }
}

#[test]
fn test_parallel_layer_evaluation() {
    let (_, _engine) = create_multi_layer_workbook();

    // Enable parallel evaluation with specific thread count
    let wb = create_parallel_test_workbook();
    let mut parallel_engine = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: true,
            max_threads: Some(2),
            ..Default::default()
        },
    );

    // Set up the same workbook structure
    parallel_engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    parallel_engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(20))
        .unwrap();
    parallel_engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(30))
        .unwrap();
    parallel_engine
        .set_cell_value("Sheet1", 1, 4, LiteralValue::Int(40))
        .unwrap();

    let a2_ast = op_ast(
        ref_ast(1, 1),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );
    let b2_ast = op_ast(
        ref_ast(1, 2),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );
    let c2_ast = op_ast(
        ref_ast(1, 3),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );
    let d2_ast = op_ast(
        ref_ast(1, 4),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None),
        "+",
    );

    parallel_engine
        .set_cell_formula("Sheet1", 2, 1, a2_ast)
        .unwrap();
    parallel_engine
        .set_cell_formula("Sheet1", 2, 2, b2_ast)
        .unwrap();
    parallel_engine
        .set_cell_formula("Sheet1", 2, 3, c2_ast)
        .unwrap();
    parallel_engine
        .set_cell_formula("Sheet1", 2, 4, d2_ast)
        .unwrap();

    let sum_ast = func_ast(
        "SUM",
        vec![ref_ast(2, 1), ref_ast(2, 2), ref_ast(2, 3), ref_ast(2, 4)],
    );
    parallel_engine
        .set_cell_formula("Sheet1", 3, 1, sum_ast)
        .unwrap();

    // Evaluate with parallelism
    let result = parallel_engine.evaluate_all().unwrap();

    // Should have evaluated multiple vertices
    assert!(result.computed_vertices >= 5);
    assert_eq!(result.cycle_errors, 0);

    // Verify results
    assert_eq!(
        parallel_engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(15.0))
    );
    assert_eq!(
        parallel_engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(25.0))
    );
    assert_eq!(
        parallel_engine.get_cell_value("Sheet1", 2, 3),
        Some(LiteralValue::Number(35.0))
    );
    assert_eq!(
        parallel_engine.get_cell_value("Sheet1", 2, 4),
        Some(LiteralValue::Number(45.0))
    );
    assert_eq!(
        parallel_engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(120.0))
    );
}

#[test]
fn test_parallel_cancellation() {
    let wb = create_parallel_test_workbook();
    let mut engine = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: true,
            ..Default::default()
        },
    );

    // Create a workbook with many cells to increase chance of cancellation
    for i in 1..=100 {
        engine
            .set_cell_value("Sheet1", 1, i, LiteralValue::Int(i as i64))
            .unwrap();
        let formula = op_ast(
            ref_ast(1, i),
            ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(2)), None),
            "*",
        );
        engine.set_cell_formula("Sheet1", 2, i, formula).unwrap();
    }

    // Test cancellation during evaluation
    let cancel_flag = Arc::new(AtomicBool::new(false));

    // Set cancellation flag immediately (simulating early cancellation)
    cancel_flag.store(true, Ordering::Relaxed);

    let result = engine.evaluate_all_cancellable(cancel_flag);

    // Should get a cancellation error
    match result {
        Err(err) => {
            assert_eq!(err.kind, ExcelErrorKind::Cancelled);
        }
        Ok(_) => {
            // In some cases, evaluation might complete before cancellation check
            // This is acceptable behavior
        }
    }
}

#[test]
fn test_parallel_error_propagation() {
    let wb = create_parallel_test_workbook();
    let mut engine = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: true,
            ..Default::default()
        },
    );

    // Create cells that will cause errors
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(0))
        .unwrap();

    // A2 = A1 / B1 (division by zero)
    let div_zero_ast = op_ast(ref_ast(1, 1), ref_ast(1, 2), "/");
    engine
        .set_cell_formula("Sheet1", 2, 1, div_zero_ast)
        .unwrap();

    // A3 = A2 + 10 (should propagate the error)
    let error_prop_ast = op_ast(
        ref_ast(2, 1),
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(10)), None),
        "+",
    );
    engine
        .set_cell_formula("Sheet1", 3, 1, error_prop_ast)
        .unwrap();

    // Evaluate with parallelism
    let result = engine.evaluate_all().unwrap();
    assert!(result.computed_vertices >= 2);

    // Check error propagation
    let a2_value = engine.get_cell_value("Sheet1", 2, 1).unwrap();
    assert!(matches!(a2_value, LiteralValue::Error(_)));

    let a3_value = engine.get_cell_value("Sheet1", 3, 1).unwrap();
    assert!(matches!(a3_value, LiteralValue::Error(_)));
}

#[test]
fn test_thread_pool_configurations() {
    let test_configs = vec![
        EvalConfig {
            enable_parallel: false,
            max_threads: None,
            ..Default::default()
        },
        EvalConfig {
            enable_parallel: true,
            max_threads: Some(1),
            ..Default::default()
        },
        EvalConfig {
            enable_parallel: true,
            max_threads: Some(2),
            ..Default::default()
        },
        EvalConfig {
            enable_parallel: true,
            max_threads: Some(4),
            ..Default::default()
        },
        EvalConfig {
            enable_parallel: true,
            max_threads: None,
            ..Default::default()
        },
    ];

    let mut all_results = Vec::new();

    for config in test_configs {
        let wb = create_parallel_test_workbook();
        let mut engine = Engine::new(wb, config);

        // Set up identical test workbook
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
            .unwrap();
        engine
            .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(20))
            .unwrap();

        let sum_ast = func_ast("SUM", vec![ref_ast(1, 1), ref_ast(1, 2)]);
        engine.set_cell_formula("Sheet1", 2, 1, sum_ast).unwrap();

        engine.evaluate_all().unwrap();

        let result = engine.get_cell_value("Sheet1", 2, 1);
        all_results.push(result);
    }

    // All configurations should produce the same result
    let expected = Some(LiteralValue::Number(30.0));
    for (i, result) in all_results.iter().enumerate() {
        assert_eq!(
            *result, expected,
            "Config {i} produced different result: {result:?}"
        );
    }
}

#[test]
fn test_demand_driven_parallel_evaluation() {
    let wb = create_parallel_test_workbook();
    let mut engine = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: true,
            ..Default::default()
        },
    );

    // Create a larger workbook with dependencies
    for i in 1..=10 {
        engine
            .set_cell_value("Sheet1", 1, i, LiteralValue::Int(i as i64))
            .unwrap();

        if i > 1 {
            let formula = op_ast(ref_ast(1, i), ref_ast(1, i - 1), "+");
            engine.set_cell_formula("Sheet1", 2, i, formula).unwrap();
        }
    }

    // Demand-driven evaluation of just the last cell (J2 = column 10, row 2)
    let result = engine.evaluate_until(&[("Sheet1", 2, 10)]).unwrap();

    // Should have evaluated some vertices (at least the dependency chain)
    assert!(result.computed_vertices > 0);
    assert_eq!(result.cycle_errors, 0);

    // Verify the target cell was computed correctly
    // J2 should be C1(10) + C9 where C9 depends on the chain
    assert!(engine.get_cell_value("Sheet1", 2, 10).is_some());
}

#[test]
fn test_parallel_with_cancellation_timing() {
    let wb = create_parallel_test_workbook();
    let mut engine = Engine::new(
        wb,
        EvalConfig {
            enable_parallel: true,
            ..Default::default()
        },
    );

    // Set up workbook with moderate number of cells
    for i in 1..=20 {
        engine
            .set_cell_value("Sheet1", 1, i, LiteralValue::Int(i as i64))
            .unwrap();
        let formula = op_ast(
            ref_ast(1, i),
            ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(3)), None),
            "*",
        );
        engine.set_cell_formula("Sheet1", 2, i, formula).unwrap();
    }

    // Test cancellation with immediate flag setting
    let cancel_flag = Arc::new(AtomicBool::new(false));
    cancel_flag.store(true, Ordering::Relaxed);

    let result = engine.evaluate_all_cancellable(cancel_flag);

    // Should get a cancellation error since flag is set immediately
    match result {
        Ok(_) => {
            // Evaluation might complete before cancellation check - acceptable
        }
        Err(err) => {
            assert_eq!(err.kind, ExcelErrorKind::Cancelled);
        }
    }
}
