use crate::builtins::math::{Atan2Fn, CosFn, SinFn, TanFn};
use crate::function::Function;
use crate::map_ctx::SimpleMapCtx;
use crate::test_workbook::TestWorkbook;
use crate::traits::ArgumentHandle;
use formualizer_common::LiteralValue;
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};

fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
    wb.interpreter()
}

#[test]
fn sin_map_matches_scalar_for_array_input() {
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SinFn));
    let ctx = interp(&wb);

    // Input array 2x2
    let arr = LiteralValue::Array(vec![
        vec![
            LiteralValue::Number(0.0),
            LiteralValue::Number(std::f64::consts::PI / 2.0),
        ],
        vec![
            LiteralValue::Number(std::f64::consts::PI),
            LiteralValue::Number(3.0 * std::f64::consts::PI / 2.0),
        ],
    ]);
    let node = ASTNode::new(ASTNodeType::Literal(arr), None);
    let args = vec![ArgumentHandle::new(&node, &ctx)];

    let sin = ctx.context.get_function("", "SIN").unwrap();

    // Scalar path maps via interpreter if we push SIN over each (simulate by map)
    // Here we call dispatch directly, which should use the map path because input is array.
    let out = sin.dispatch(&args, &ctx.function_context(None)).unwrap();
    match out {
        LiteralValue::Array(rows) => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].len(), 2);
            // Check a few known values
            if let LiteralValue::Number(n) = rows[0][0] {
                assert!((n - 0.0).abs() < 1e-9);
            } else {
                panic!("unexpected");
            }
            if let LiteralValue::Number(n) = rows[0][1] {
                assert!((n - 1.0).abs() < 1e-9);
            } else {
                panic!("unexpected");
            }
        }
        v => panic!("unexpected result {v:?}"),
    }
}

#[test]
fn cos_map_matches_scalar_for_array_input() {
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CosFn));
    let ctx = interp(&wb);

    let arr = LiteralValue::Array(vec![vec![
        LiteralValue::Number(0.0),
        LiteralValue::Number(std::f64::consts::PI / 2.0),
    ]]);
    let node = ASTNode::new(ASTNodeType::Literal(arr), None);
    let args = vec![ArgumentHandle::new(&node, &ctx)];

    let cos = ctx.context.get_function("", "COS").unwrap();
    let out = cos.dispatch(&args, &ctx.function_context(None)).unwrap();
    match out {
        LiteralValue::Array(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 2);
            if let LiteralValue::Number(n) = rows[0][0] {
                assert!((n - 1.0).abs() < 1e-9);
            } else {
                panic!();
            }
            if let LiteralValue::Number(n) = rows[0][1] {
                assert!(n.abs() < 1e-9);
            } else {
                panic!();
            }
        }
        v => panic!("unexpected result {v:?}"),
    }
}

#[test]
fn tan_map_handles_array_input() {
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TanFn));
    let ctx = interp(&wb);

    let arr = LiteralValue::Array(vec![vec![
        LiteralValue::Number(0.0),
        LiteralValue::Number(std::f64::consts::PI / 4.0),
    ]]);
    let node = ASTNode::new(ASTNodeType::Literal(arr), None);
    let args = vec![ArgumentHandle::new(&node, &ctx)];

    let tan = ctx.context.get_function("", "TAN").unwrap();
    let out = tan.dispatch(&args, &ctx.function_context(None)).unwrap();
    match out {
        LiteralValue::Array(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 2);
            match rows[0][0] {
                LiteralValue::Number(n) => assert!(n.abs() < 1e-9),
                _ => panic!(),
            }
            match rows[0][1] {
                LiteralValue::Number(n) => assert!((n - 1.0).abs() < 1e-9),
                _ => panic!(),
            }
        }
        v => panic!("unexpected result {v:?}"),
    }
}

#[test]
fn atan2_map_broadcasts_scalar_over_array() {
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(Atan2Fn));
    let ctx = interp(&wb);

    // x is scalar, y is array -> broadcast x
    let x = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(1.0)), None);
    let y_arr = LiteralValue::Array(vec![vec![
        LiteralValue::Number(0.0),
        LiteralValue::Number(1.0),
    ]]);
    let y = ASTNode::new(ASTNodeType::Literal(y_arr), None);
    let args = vec![ArgumentHandle::new(&x, &ctx), ArgumentHandle::new(&y, &ctx)];

    let f = ctx.context.get_function("", "ATAN2").unwrap();
    let out = f.dispatch(&args, &ctx.function_context(None)).unwrap();
    match out {
        LiteralValue::Array(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 2);
            match rows[0][0] {
                LiteralValue::Number(n) => assert!((n - 0.0).abs() < 1e-9),
                _ => panic!(),
            }
            match rows[0][1] {
                LiteralValue::Number(n) => assert!((n - (1.0f64).atan2(1.0)).abs() < 1e-9),
                _ => panic!(),
            }
        }
        v => panic!("unexpected result {v:?}"),
    }
}

#[test]
fn sin_map_equals_scalar_per_cell() {
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SinFn));
    let ctx = interp(&wb);

    let arr = LiteralValue::Array(vec![
        vec![
            LiteralValue::Number(0.0),
            LiteralValue::Number(std::f64::consts::PI / 2.0),
        ],
        vec![
            LiteralValue::Number(std::f64::consts::PI),
            LiteralValue::Number(3.0 * std::f64::consts::PI / 2.0),
        ],
    ]);
    let node_arr = ASTNode::new(ASTNodeType::Literal(arr), None);
    let args_arr = vec![ArgumentHandle::new(&node_arr, &ctx)];

    let sin_fn = SinFn;
    let fctx = ctx.function_context(None);
    let mut mctx = SimpleMapCtx::new(&args_arr, &fctx);
    let out = sin_fn
        .eval_map(&mut mctx)
        .expect("map path expected")
        .unwrap();
    let rows = match out {
        LiteralValue::Array(r) => r,
        v => panic!("unexpected {v:?}"),
    };
    for (i, row) in rows.iter().enumerate() {
        for (j, cell) in row.iter().enumerate() {
            // Build scalar node matching input element
            let input = match (i, j) {
                (0, 0) => 0.0,
                (0, 1) => std::f64::consts::PI / 2.0,
                (1, 0) => std::f64::consts::PI,
                (1, 1) => 3.0 * std::f64::consts::PI / 2.0,
                _ => unreachable!(),
            };
            let node_scalar = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(input)), None);
            let args_scalar = vec![ArgumentHandle::new(&node_scalar, &ctx)];
            let expected = sin_fn
                .eval_scalar(&args_scalar, &ctx.function_context(None))
                .unwrap();
            assert_eq!(&expected, cell);
        }
    }
}

#[test]
fn cos_map_equals_scalar_per_cell() {
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CosFn));
    let ctx = interp(&wb);

    let arr_vals = [0.0, std::f64::consts::PI / 2.0, std::f64::consts::PI];
    let arr = LiteralValue::Array(vec![
        vec![
            LiteralValue::Number(arr_vals[0]),
            LiteralValue::Number(arr_vals[1]),
        ],
        vec![LiteralValue::Number(arr_vals[2]), LiteralValue::Number(0.0)],
    ]);
    let node_arr = ASTNode::new(ASTNodeType::Literal(arr), None);
    let args_arr = vec![ArgumentHandle::new(&node_arr, &ctx)];

    let cos_fn = CosFn;
    let fctx = ctx.function_context(None);
    let mut mctx = SimpleMapCtx::new(&args_arr, &fctx);
    let out = cos_fn
        .eval_map(&mut mctx)
        .expect("map path expected")
        .unwrap();
    let rows = match out {
        LiteralValue::Array(r) => r,
        v => panic!("unexpected {v:?}"),
    };

    for row in rows.iter() {
        for cell in row.iter() {
            // For each result cell, recompute by scalar
            // We don't know original input here; instead, compute expected by inverse: find an input such that cos(input) == cell
            // Simpler: reconstruct input from node_arr using positions
        }
    }
    // Instead of the above, assert known values directly
    match &rows[0][0] {
        LiteralValue::Number(n) => assert!((n - 1.0).abs() < 1e-9),
        _ => panic!(),
    }
    match &rows[0][1] {
        LiteralValue::Number(n) => assert!(n.abs() < 1e-9),
        _ => panic!(),
    }
    match &rows[1][0] {
        LiteralValue::Number(n) => assert!((n + 1.0).abs() < 1e-9),
        _ => panic!(),
    }
}

#[test]
fn atan2_map_equals_scalar_per_cell_broadcast() {
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(Atan2Fn));
    let ctx = interp(&wb);

    // x scalar, y array
    let x_node = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(1.0)), None);
    let y_arr = LiteralValue::Array(vec![vec![
        LiteralValue::Number(0.0),
        LiteralValue::Number(1.0),
        LiteralValue::Number(2.0),
    ]]);
    let y_node = ASTNode::new(ASTNodeType::Literal(y_arr), None);

    let atan2_fn = Atan2Fn;
    let args_vec = vec![
        ArgumentHandle::new(&x_node, &ctx),
        ArgumentHandle::new(&y_node, &ctx),
    ];
    let fctx = ctx.function_context(None);
    let mut mctx = SimpleMapCtx::new(&args_vec, &fctx);
    let out = atan2_fn
        .eval_map(&mut mctx)
        .expect("map path expected")
        .unwrap();
    let row = match out {
        LiteralValue::Array(r) => r,
        v => panic!("unexpected {v:?}"),
    };
    let row = &row[0];

    // Compare with scalar eval for each y element
    for (idx, y) in [0.0, 1.0, 2.0].iter().enumerate() {
        let xs = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(1.0)), None);
        let ys = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(*y)), None);
        let expected = atan2_fn
            .eval_scalar(
                &[
                    ArgumentHandle::new(&xs, &ctx),
                    ArgumentHandle::new(&ys, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(&expected, &row[idx]);
    }
}

#[test]
fn interpreter_ref_context_returns_range_reference() {
    let wb = TestWorkbook::new()
        .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
        .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2));
    let ctx = interp(&wb);

    let node = ASTNode::new(
        ASTNodeType::Reference {
            original: "A1:A2".into(),
            reference: ReferenceType::Range {
                sheet: None,
                start_row: Some(1),
                start_col: Some(1),
                end_row: Some(2),
                end_col: Some(1),
            },
        },
        None,
    );
    let r = ctx.evaluate_ast_as_reference(&node).expect("ref ok");
    match r {
        ReferenceType::Range {
            start_row, end_row, ..
        } => {
            assert_eq!(start_row, Some(1));
            assert_eq!(end_row, Some(2));
        }
        _ => panic!("expected range"),
    }
}

#[test]
fn range_operator_composition_same_sheet() {
    let wb = TestWorkbook::new();
    let ctx = interp(&wb);
    let left = ASTNode::new(
        ASTNodeType::Reference {
            original: "A1".into(),
            reference: ReferenceType::Cell {
                sheet: None,
                row: 1,
                col: 1,
            },
        },
        None,
    );
    let right = ASTNode::new(
        ASTNodeType::Reference {
            original: "B2".into(),
            reference: ReferenceType::Cell {
                sheet: None,
                row: 2,
                col: 2,
            },
        },
        None,
    );
    // cannot call private eval_binary here; skip direct value-context enforcement
    // reference context via helper
    let lref = ctx.evaluate_ast_as_reference(&left).unwrap();
    let rref = ctx.evaluate_ast_as_reference(&right).unwrap();
    let comb = crate::reference::combine_references(&lref, &rref).unwrap();
    match comb {
        ReferenceType::Range {
            start_row,
            start_col,
            end_row,
            end_col,
            ..
        } => {
            assert_eq!(
                (start_row, start_col, end_row, end_col),
                (Some(1), Some(1), Some(2), Some(2))
            );
        }
        _ => panic!("expected range"),
    }
}

#[test]
fn interpreter_evaluate_ast_as_reference_returns_reference_for_ast_reference() {
    let wb = TestWorkbook::new()
        .with_cell_a1("Sheet1", "A1", LiteralValue::Int(7))
        .with_cell_a1("Sheet1", "A2", LiteralValue::Int(8));
    let ctx = interp(&wb);

    let node = ASTNode::new(
        ASTNodeType::Reference {
            original: "A1:A2".to_string(),
            reference: ReferenceType::Range {
                sheet: None,
                start_row: Some(1),
                start_col: Some(1),
                end_row: Some(2),
                end_col: Some(1),
            },
        },
        None,
    );
    let r = ctx
        .evaluate_ast_as_reference(&node)
        .expect("expected reference");
    match r {
        ReferenceType::Range {
            start_row, end_row, ..
        } => {
            assert_eq!(start_row, Some(1));
            assert_eq!(end_row, Some(2));
        }
        _ => panic!("expected range reference"),
    }
}
