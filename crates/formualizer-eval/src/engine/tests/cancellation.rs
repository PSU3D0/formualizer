//! Tests for cancellation support in evaluation
//!
//! This module tests the coarse-grained cancellation functionality, following
//! the Phase 2.1 implementation requirements from FUTUREPROOF_MILESTONES.md

use super::common::{create_binary_op_ast, create_cell_ref_ast};
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

/// Test that cancellation works between layers in evaluate_all_cancellable
#[test]
fn test_cancellation_between_layers() {
    let workbook = TestWorkbook::new();
    let config = EvalConfig::default();
    let mut engine = Engine::new(workbook, config);

    // Create a multi-layer dependency chain:
    // A1 = 1, B1 = A1 + 1, C1 = B1 + 1, D1 = C1 + 1
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();

    let a1_ref = create_cell_ref_ast(None, 1, 1);
    let one = ASTNode {
        node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
        source_token: None,
        contains_volatile: false,
    };
    let b1_formula = create_binary_op_ast(a1_ref, one.clone(), "+");
    engine.set_cell_formula("Sheet1", 1, 2, b1_formula).unwrap();

    let b1_ref = create_cell_ref_ast(None, 1, 2);
    let c1_formula = create_binary_op_ast(b1_ref, one.clone(), "+");
    engine.set_cell_formula("Sheet1", 1, 3, c1_formula).unwrap();

    let c1_ref = create_cell_ref_ast(None, 1, 3);
    let d1_formula = create_binary_op_ast(c1_ref, one, "+");
    engine.set_cell_formula("Sheet1", 1, 4, d1_formula).unwrap();

    // Set up cancellation flag that will be triggered
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let cancel_flag_clone = Arc::clone(&cancel_flag);

    // Start evaluation in a separate thread
    let handle = thread::spawn(move || {
        // Small delay to ensure cancellation happens during evaluation
        thread::sleep(Duration::from_millis(1));
        cancel_flag_clone.store(true, Ordering::Relaxed);
    });

    // Attempt evaluation with cancellation
    let result = engine.evaluate_all_cancellable(cancel_flag);

    handle.join().unwrap();

    // Should return cancellation error
    match result {
        Err(ExcelError {
            kind: ExcelErrorKind::Cancelled,
            ..
        }) => {
            // Expected result - test passes
        }
        Ok(_) => {
            // This might happen if evaluation completes before cancellation
            // In that case, verify all values were computed correctly
            assert_eq!(
                engine.get_cell_value("Sheet1", 1, 1),
                Some(LiteralValue::Int(1))
            );
            assert_eq!(
                engine.get_cell_value("Sheet1", 1, 2),
                Some(LiteralValue::Number(2.0))
            );
            assert_eq!(
                engine.get_cell_value("Sheet1", 1, 3),
                Some(LiteralValue::Number(3.0))
            );
            assert_eq!(
                engine.get_cell_value("Sheet1", 1, 4),
                Some(LiteralValue::Number(4.0))
            );
        }
        Err(other_error) => {
            panic!("Expected cancellation error, got: {other_error:?}");
        }
    }
}

/// Test that cancellation works within large layers in evaluate_all_cancellable
#[test]
fn test_cancellation_within_large_layer() {
    let workbook = TestWorkbook::new();
    let config = EvalConfig::default();
    let mut engine = Engine::new(workbook, config);

    // Create a large layer with many independent formulas
    // A1 = 1, then A2 = 1, A3 = 1, ..., A500 = 1 (all in same layer)
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();

    let one = ASTNode {
        node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
        source_token: None,
        contains_volatile: false,
    };

    // Create 300 independent formulas (all will be in the same layer)
    for row in 2..=301 {
        engine
            .set_cell_formula("Sheet1", row, 1, one.clone())
            .unwrap();
    }

    // Set up cancellation flag
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let cancel_flag_clone = Arc::clone(&cancel_flag);

    // Start evaluation and cancel after a short delay
    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(5)); // Slightly longer delay for large layer
        cancel_flag_clone.store(true, Ordering::Relaxed);
    });

    let result = engine.evaluate_all_cancellable(cancel_flag);

    handle.join().unwrap();

    // Should return cancellation error or complete successfully
    match result {
        Err(ExcelError {
            kind: ExcelErrorKind::Cancelled,
            ..
        }) => {
            // Expected - cancellation worked within the layer
        }
        Ok(eval_result) => {
            // Evaluation completed before cancellation - verify results
            assert!(eval_result.computed_vertices > 0);
            assert_eq!(eval_result.cycle_errors, 0);
        }
        Err(other_error) => {
            panic!("Expected cancellation error or success, got: {other_error:?}");
        }
    }
}

/// Test that cancellation works in evaluate_until_cancellable
#[test]
fn test_cancellation_in_demand_driven_evaluation() {
    let workbook = TestWorkbook::new();
    let config = EvalConfig::default();
    let mut engine = Engine::new(workbook, config);

    // Create a dependency chain: A1 = 1, B1 = A1, C1 = B1, D1 = C1
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();

    let a1_ref = create_cell_ref_ast(None, 1, 1);
    engine.set_cell_formula("Sheet1", 1, 2, a1_ref).unwrap();

    let b1_ref = create_cell_ref_ast(None, 1, 2);
    engine.set_cell_formula("Sheet1", 1, 3, b1_ref).unwrap();

    let c1_ref = create_cell_ref_ast(None, 1, 3);
    engine.set_cell_formula("Sheet1", 1, 4, c1_ref).unwrap();

    // Set up cancellation
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let cancel_flag_clone = Arc::clone(&cancel_flag);

    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(1));
        cancel_flag_clone.store(true, Ordering::Relaxed);
    });

    // Try to evaluate until D1 with cancellation
    let result = engine.evaluate_until_cancellable(&["D1"], cancel_flag);

    handle.join().unwrap();

    match result {
        Err(ExcelError {
            kind: ExcelErrorKind::Cancelled,
            ..
        }) => {
            // Expected result - test passes
        }
        Ok(_) => {
            // Evaluation completed before cancellation
            assert_eq!(
                engine.get_cell_value("Sheet1", 1, 4),
                Some(LiteralValue::Number(1.0))
            );
        }
        Err(other_error) => {
            panic!("Expected cancellation error, got: {other_error:?}");
        }
    }
}

/// Test that cancellation during cycle handling works correctly
#[test]
fn test_cancellation_during_cycle_handling() {
    let workbook = TestWorkbook::new();
    let config = EvalConfig::default();
    let mut engine = Engine::new(workbook, config);

    // Create a cycle: A1 = B1, B1 = A1
    let b1_ref = create_cell_ref_ast(None, 1, 2);
    engine.set_cell_formula("Sheet1", 1, 1, b1_ref).unwrap();

    let a1_ref = create_cell_ref_ast(None, 1, 1);
    engine.set_cell_formula("Sheet1", 1, 2, a1_ref).unwrap();

    // Set up immediate cancellation
    let cancel_flag = Arc::new(AtomicBool::new(true));

    // Evaluation should be cancelled immediately
    let result = engine.evaluate_all_cancellable(cancel_flag);

    match result {
        Err(ExcelError {
            kind: ExcelErrorKind::Cancelled,
            ..
        }) => {
            // Expected result - test passes
        }
        Ok(_) => {
            panic!("Expected cancellation, but evaluation completed");
        }
        Err(other_error) => {
            panic!("Expected cancellation error, got: {other_error:?}");
        }
    }
}

/// Test that non-cancelled evaluation still works correctly
#[test]
fn test_non_cancelled_evaluation_works_normally() {
    let workbook = TestWorkbook::new();
    let config = EvalConfig::default();
    let mut engine = Engine::new(workbook, config);

    // Create simple formulas
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();

    let a1_ref = create_cell_ref_ast(None, 1, 1);
    let five = ASTNode {
        node_type: ASTNodeType::Literal(LiteralValue::Int(5)),
        source_token: None,
        contains_volatile: false,
    };
    let b1_formula = create_binary_op_ast(a1_ref, five, "+");
    engine.set_cell_formula("Sheet1", 1, 2, b1_formula).unwrap();

    // Use cancellation flag but never set it
    let cancel_flag = Arc::new(AtomicBool::new(false));

    // Evaluation should complete normally
    let result = engine.evaluate_all_cancellable(cancel_flag).unwrap();

    assert_eq!(result.computed_vertices, 1); // Only B1 needs evaluation
    assert_eq!(result.cycle_errors, 0);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(15.0))
    );
}

/// Test that evaluate_until_cancellable works normally when not cancelled
#[test]
fn test_demand_driven_non_cancelled_works_normally() {
    let workbook = TestWorkbook::new();
    let config = EvalConfig::default();
    let mut engine = Engine::new(workbook, config);

    // Create dependency chain
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(100))
        .unwrap();

    let a1_ref = create_cell_ref_ast(None, 1, 1);
    engine.set_cell_formula("Sheet1", 1, 2, a1_ref).unwrap();

    let b1_ref = create_cell_ref_ast(None, 1, 2);
    let ten = ASTNode {
        node_type: ASTNodeType::Literal(LiteralValue::Int(10)),
        source_token: None,
        contains_volatile: false,
    };
    let c1_formula = create_binary_op_ast(b1_ref, ten, "*");
    engine.set_cell_formula("Sheet1", 1, 3, c1_formula).unwrap();

    // Use cancellation flag but never set it
    let cancel_flag = Arc::new(AtomicBool::new(false));

    // Evaluation should complete normally
    let result = engine
        .evaluate_until_cancellable(&["C1"], cancel_flag)
        .unwrap();

    assert_eq!(result.computed_vertices, 2); // B1 and C1 need evaluation
    assert_eq!(result.cycle_errors, 0);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(1000.0))
    );
}

/// Test cancellation message differentiation
/// Test cancellation message differentiation
#[test]
fn test_cancellation_message_differentiation() {
    let workbook = TestWorkbook::new();
    let config = EvalConfig::default();
    let mut engine = Engine::new(workbook, config);

    // Simple formula
    let one = ASTNode {
        node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
        source_token: None,
        contains_volatile: false,
    };
    engine.set_cell_formula("Sheet1", 1, 1, one).unwrap();

    // Test immediate cancellation to get between-layers message
    let cancel_flag = Arc::new(AtomicBool::new(true));
    let result = engine.evaluate_all_cancellable(cancel_flag);

    match result {
        Err(ExcelError {
            kind: ExcelErrorKind::Cancelled,
            message: Some(msg),
            ..
        }) => {
            // Should contain context about where cancellation occurred
            assert!(
                msg.contains("between layers")
                    || msg.contains("cycle handling")
                    || msg.contains("within layer")
            );
        }
        _ => panic!("Expected cancellation error with message"),
    }
}
