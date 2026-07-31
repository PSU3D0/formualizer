//! Tests for cancellation support in evaluation
//!
//! This module tests the coarse-grained cancellation functionality, following
//! the Phase 2.1 implementation requirements from FUTUREPROOF_MILESTONES.md

use super::common::{create_binary_op_ast, create_cell_ref_ast};
use crate::engine::{CancelToken, Engine, EvalConfig};
use crate::function::Function;
use crate::test_workbook::TestWorkbook;
use crate::traits::{
    ArgumentHandle, CalcValue, DefaultFunctionContext, EvaluationContext, FunctionContext,
};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

#[derive(Debug)]
struct SharedCancellationProbe {
    expected: CancelToken,
    observed: Arc<AtomicBool>,
}

impl Function for SharedCancellationProbe {
    fn name(&self) -> &'static str {
        "SHARED_CANCELLATION_PROBE"
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let context_token = ctx
            .cancellation_token()
            .expect("engine cancellation token should reach custom functions");
        context_token.cancel();
        self.observed
            .store(self.expected.is_cancelled(), Ordering::SeqCst);
        Ok(CalcValue::Scalar(LiteralValue::Int(1)))
    }
}

#[derive(Debug)]
struct DynamicCollectorCancellationProbe {
    expected: CancelToken,
    calls: Arc<AtomicUsize>,
    missing_tokens: Arc<AtomicUsize>,
    shared_signal_observed: Arc<AtomicBool>,
}

impl Function for DynamicCollectorCancellationProbe {
    fn name(&self) -> &'static str {
        "DYNAMIC_COLLECTOR_CANCELLATION_PROBE"
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        match ctx.cancellation_token() {
            Some(token) if call == 0 => {
                token.cancel();
                self.shared_signal_observed
                    .store(self.expected.is_cancelled(), Ordering::SeqCst);
            }
            Some(_) => {}
            None => {
                self.missing_tokens.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(CalcValue::Scalar(LiteralValue::Int(0)))
    }
}

#[test]
fn engine_context_and_function_context_share_cancellation_signal() {
    let token = CancelToken::new();
    let observed = Arc::new(AtomicBool::new(false));
    let workbook = TestWorkbook::new().with_function(Arc::new(SharedCancellationProbe {
        expected: token.clone(),
        observed: Arc::clone(&observed),
    }));
    let mut engine = Engine::new(workbook, EvalConfig::default());
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            formualizer_parse::parser::parse("=SHARED_CANCELLATION_PROBE()").unwrap(),
        )
        .unwrap();

    let result = engine.evaluate_all_cancellable(token);

    assert!(
        observed.load(Ordering::SeqCst),
        "the FunctionContext token must share the Engine's active signal"
    );
    assert!(
        result.is_ok() || result.unwrap_err().kind == ExcelErrorKind::Cancelled,
        "cancelling from the function may be observed at the next engine checkpoint"
    );
}

#[test]
fn dynamic_ref_collector_forwards_shared_cancellation_token() {
    let token = CancelToken::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let missing_tokens = Arc::new(AtomicUsize::new(0));
    let shared_signal_observed = Arc::new(AtomicBool::new(false));
    let workbook = TestWorkbook::new().with_function(Arc::new(DynamicCollectorCancellationProbe {
        expected: token.clone(),
        calls: Arc::clone(&calls),
        missing_tokens: Arc::clone(&missing_tokens),
        shared_signal_observed: Arc::clone(&shared_signal_observed),
    }));
    let mut engine = Engine::new(workbook, EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            2,
            formualizer_parse::parser::parse(
                "=DYNAMIC_COLLECTOR_CANCELLATION_PROBE()+INDIRECT(\"A1\")",
            )
            .unwrap(),
        )
        .unwrap();

    let error = engine.evaluate_all_cancellable(token).unwrap_err();

    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    assert!(calls.load(Ordering::SeqCst) >= 1);
    assert_eq!(missing_tokens.load(Ordering::SeqCst), 0);
    assert!(
        shared_signal_observed.load(Ordering::SeqCst),
        "the DynamicRefCollector token must share the Engine's active signal"
    );
}

#[test]
fn cancellation_context_traits_remain_dyn_compatible() {
    fn accept_evaluation_context(_: &dyn EvaluationContext) {}
    fn accept_function_context<'ctx>(_: &dyn FunctionContext<'ctx>) {}

    let workbook = TestWorkbook::new();
    accept_evaluation_context(&workbook);
    let function_context = DefaultFunctionContext::new(&workbook, None, "Sheet1");
    accept_function_context(&function_context);
}

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
    let cancel = crate::engine::CancelToken::new();
    let cancel_clone = cancel.clone();

    // Start evaluation in a separate thread
    let handle = thread::spawn(move || {
        // Small delay to ensure cancellation happens during evaluation
        thread::sleep(Duration::from_millis(1));
        cancel_clone.cancel();
    });

    // Attempt evaluation with cancellation
    let result = engine.evaluate_all_cancellable(cancel);

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
                Some(LiteralValue::Number(1.0))
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
    let cancel = crate::engine::CancelToken::new();
    let cancel_clone = cancel.clone();

    // Start evaluation and cancel after a short delay
    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(5)); // Slightly longer delay for large layer
        cancel_clone.cancel();
    });

    let result = engine.evaluate_all_cancellable(cancel);

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
    let cancel = crate::engine::CancelToken::new();
    let cancel_clone = cancel.clone();

    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(1));
        cancel_clone.cancel();
    });

    // Try to evaluate until D1 with cancellation
    let result = engine.evaluate_until_cancellable(&["D1"], cancel);

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
    let cancel = {
        let token = crate::engine::CancelToken::new();
        token.cancel();
        token
    };

    // Evaluation should be cancelled immediately
    let result = engine.evaluate_all_cancellable(cancel);

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
    let cancel = crate::engine::CancelToken::new();

    // Evaluation should complete normally
    let result = engine.evaluate_all_cancellable(cancel).unwrap();

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
    let cancel = crate::engine::CancelToken::new();

    // Evaluation should complete normally
    let result = engine.evaluate_until_cancellable(&["C1"], cancel).unwrap();

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
    let cancel = {
        let token = crate::engine::CancelToken::new();
        token.cancel();
        token
    };
    let result = engine.evaluate_all_cancellable(cancel);

    match result {
        Err(ExcelError {
            kind: ExcelErrorKind::Cancelled,
            message: Some(msg),
            ..
        }) => {
            // Should contain context about where cancellation occurred.
            // Immediate cancellation can now happen before scheduling.
            assert!(
                msg.contains("before scheduling")
                    || msg.contains("between layers")
                    || msg.contains("cycle handling")
                    || msg.contains("within layer")
                    || msg.contains("before starting")
                    || msg.contains("during execution")
            );
        }
        _ => panic!("Expected cancellation error with message"),
    }
}
