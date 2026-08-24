use crate::builtins::math::{Atan2Fn, CosFn, SinFn, TanFn};
use crate::test_workbook::TestWorkbook;
use crate::traits::ArgumentHandle;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

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
    let out = sin
        .dispatch(&args, &ctx.function_context(None))
        .unwrap()
        .into_literal();
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
    let out = cos
        .dispatch(&args, &ctx.function_context(None))
        .unwrap()
        .into_literal();
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
    let out = tan
        .dispatch(&args, &ctx.function_context(None))
        .unwrap()
        .into_literal();
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
    let out = f
        .dispatch(&args, &ctx.function_context(None))
        .unwrap()
        .into_literal();
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

    let sin = ctx.context.get_function("", "SIN").unwrap();
    let fctx = ctx.function_context(None);
    let out = sin.dispatch(&args_arr, &fctx).unwrap().into_literal();
    let rows = match out {
        LiteralValue::Array(r) => r,
        v => panic!("unexpected {v:?}"),
    };

    for (i, row) in rows.iter().enumerate() {
        for (j, cell) in row.iter().enumerate() {
            let input = match (i, j) {
                (0, 0) => 0.0,
                (0, 1) => std::f64::consts::PI / 2.0,
                (1, 0) => std::f64::consts::PI,
                (1, 1) => 3.0 * std::f64::consts::PI / 2.0,
                _ => unreachable!(),
            };
            let node_scalar = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(input)), None);
            let args_scalar = vec![ArgumentHandle::new(&node_scalar, &ctx)];
            let expected = sin.dispatch(&args_scalar, &fctx).unwrap().into_literal();
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

    let cos = ctx.context.get_function("", "COS").unwrap();
    let out = cos
        .dispatch(&args_arr, &ctx.function_context(None))
        .unwrap()
        .into_literal();
    let rows = match out {
        LiteralValue::Array(r) => r,
        v => panic!("unexpected {v:?}"),
    };

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

    let atan2 = ctx.context.get_function("", "ATAN2").unwrap();
    let args_vec = vec![
        ArgumentHandle::new(&x_node, &ctx),
        ArgumentHandle::new(&y_node, &ctx),
    ];
    let fctx = ctx.function_context(None);
    let out = atan2.dispatch(&args_vec, &fctx).unwrap().into_literal();
    let rows = match out {
        LiteralValue::Array(r) => r,
        v => panic!("unexpected {v:?}"),
    };
    let row = &rows[0];

    for (idx, y) in [0.0, 1.0, 2.0].iter().enumerate() {
        let xs = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(1.0)), None);
        let ys = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(*y)), None);
        let expected = atan2
            .dispatch(
                &[
                    ArgumentHandle::new(&xs, &ctx),
                    ArgumentHandle::new(&ys, &ctx),
                ],
                &fctx,
            )
            .unwrap()
            .into_literal();
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
                start_row_abs: false,
                start_col_abs: false,
                end_row_abs: false,
                end_col_abs: false,
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
                row_abs: false,
                col_abs: false,
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
                row_abs: false,
                col_abs: false,
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
                start_row_abs: false,
                start_col_abs: false,
                end_row_abs: false,
                end_col_abs: false,
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

#[test]
fn structured_ref_basic_specifiers() {
    use crate::traits::Resolver;
    type V = LiteralValue;
    // Build a test workbook with a simple table
    let wb = TestWorkbook::new().with_simple_table(
        "Sales",
        vec!["Region".into(), "Amount".into(), "Units".into()],
        vec![
            vec![V::Text("N".into()), V::Number(10.0), V::Int(2)],
            vec![V::Text("S".into()), V::Number(20.0), V::Int(3)],
        ],
        Some(vec![V::Text("".into()), V::Number(30.0), V::Int(5)]),
    );

    // Column reference
    let r = ReferenceType::from_string("Sales[Amount]").unwrap();
    let range = wb.resolve_range_like(&r).unwrap();
    assert_eq!(range.dimensions(), (2, 1));
    assert_eq!(range.get(0, 0).unwrap(), V::Number(10.0));
    assert_eq!(range.get(1, 0).unwrap(), V::Number(20.0));

    // Column range
    let r = ReferenceType::from_string("Sales[Amount:Units]").unwrap();
    let range = wb.resolve_range_like(&r).unwrap();
    assert_eq!(range.dimensions(), (2, 2));
    assert_eq!(range.get(0, 0).unwrap(), V::Number(10.0));
    assert_eq!(range.get(1, 1).unwrap(), V::Int(3));

    // Headers
    let r = ReferenceType::from_string("Sales[#Headers]").unwrap();
    let range = wb.resolve_range_like(&r).unwrap();
    assert_eq!(range.dimensions(), (1, 3));

    // Totals
    let r = ReferenceType::from_string("Sales[#Totals]").unwrap();
    let range = wb.resolve_range_like(&r).unwrap();
    assert_eq!(range.dimensions(), (1, 3));
    assert_eq!(range.get(0, 1).unwrap(), V::Number(30.0));

    // All = headers + data + totals
    let r = ReferenceType::from_string("Sales[#All]").unwrap();
    let range = wb.resolve_range_like(&r).unwrap();
    assert_eq!(range.dimensions(), (1 + 2 + 1, 3));
}

#[test]
fn interpreter_broadcasts_numeric_binary() {
    let wb = TestWorkbook::new();
    let ctx = interp(&wb);

    // {1,2;3,4} + {10;20} => {11,12;23,24}
    let left = LiteralValue::Array(vec![
        vec![LiteralValue::Int(1), LiteralValue::Int(2)],
        vec![LiteralValue::Int(3), LiteralValue::Int(4)],
    ]);
    let right = LiteralValue::Array(vec![
        vec![LiteralValue::Int(10)],
        vec![LiteralValue::Int(20)],
    ]);
    let lnode = ASTNode::new(ASTNodeType::Literal(left), None);
    let rnode = ASTNode::new(ASTNodeType::Literal(right), None);
    let plus = ASTNode::new(
        ASTNodeType::BinaryOp {
            op: "+".into(),
            left: Box::new(lnode),
            right: Box::new(rnode),
        },
        None,
    );
    let out = ctx.evaluate_ast(&plus).unwrap().into_literal();
    match out {
        LiteralValue::Array(rows) => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].len(), 2);
            assert_eq!(rows[0][0], LiteralValue::Number(11.0));
            assert_eq!(rows[0][1], LiteralValue::Number(12.0));
            assert_eq!(rows[1][0], LiteralValue::Number(23.0));
            assert_eq!(rows[1][1], LiteralValue::Number(24.0));
        }
        v => panic!("unexpected {v:?}"),
    }
}

#[test]
fn interpreter_broadcast_scalar_over_array() {
    let wb = TestWorkbook::new();
    let ctx = interp(&wb);
    // 2 * {1,2,3} => {2,4,6}
    let lnode = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(2)), None);
    let right = LiteralValue::Array(vec![vec![
        LiteralValue::Int(1),
        LiteralValue::Int(2),
        LiteralValue::Int(3),
    ]]);
    let rnode = ASTNode::new(ASTNodeType::Literal(right), None);
    let node = ASTNode::new(
        ASTNodeType::BinaryOp {
            op: "*".into(),
            left: Box::new(lnode),
            right: Box::new(rnode),
        },
        None,
    );
    let out = ctx.evaluate_ast(&node).unwrap().into_literal();
    match out {
        LiteralValue::Array(rows) => {
            assert_eq!(
                rows[0],
                vec![
                    LiteralValue::Number(2.0),
                    LiteralValue::Number(4.0),
                    LiteralValue::Number(6.0),
                ]
            );
        }
        v => panic!("unexpected {v:?}"),
    }
}

#[test]
fn interpreter_incompatible_broadcast_is_value_error() {
    let wb = TestWorkbook::new();
    let ctx = interp(&wb);

    // {1,2} + {1,2,3} -> #VALUE!
    let l = LiteralValue::Array(vec![vec![LiteralValue::Int(1), LiteralValue::Int(2)]]);
    let r = LiteralValue::Array(vec![vec![
        LiteralValue::Int(1),
        LiteralValue::Int(2),
        LiteralValue::Int(3),
    ]]);
    let lnode = ASTNode::new(ASTNodeType::Literal(l), None);
    let rnode = ASTNode::new(ASTNodeType::Literal(r), None);
    let n = ASTNode::new(
        ASTNodeType::BinaryOp {
            op: "+".into(),
            left: Box::new(lnode),
            right: Box::new(rnode),
        },
        None,
    );
    match ctx.evaluate_ast(&n).unwrap().into_literal() {
        LiteralValue::Error(e) => assert_eq!(e, "#VALUE!"),
        v => panic!("expected value error, got {v:?}"),
    }
}

fn reference_returning_engine(g1: i64) -> crate::engine::Engine<TestWorkbook> {
    use crate::engine::{CycleConfig, CycleDetection, CyclePolicy, EvalConfig};

    let cfg = EvalConfig::default().with_cycle(CycleConfig {
        detection: CycleDetection::Runtime,
        policy: CyclePolicy::Error,
    });
    let mut engine = crate::engine::Engine::new(TestWorkbook::new(), cfg);
    for row in 1..=20 {
        for (col, value) in [
            (1, row as i64),
            (2, 100 + row as i64),
            (3, 200 + row as i64),
            (4, row as i64),
            (5, 400 + row as i64),
            (6, 500 + row as i64),
        ] {
            engine
                .set_cell_value("Sheet1", row, col, LiteralValue::Int(value))
                .expect("set reference fixture value");
        }
    }
    engine
        .set_cell_value("Sheet1", 1, 7, LiteralValue::Int(g1))
        .expect("set selector");
    engine
}

fn evaluate_reference_returning_formula(g1: i64, formula: &str) -> LiteralValue {
    let mut engine = reference_returning_engine(g1);
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            10,
            formualizer_parse::parser::parse(formula).expect("valid reference-returning formula"),
        )
        .expect("set reference-returning formula");
    engine
        .evaluate_all()
        .expect("evaluate reference-returning formula");
    engine
        .get_cell_value("Sheet1", 1, 10)
        .expect("formula result")
}

#[test]
fn reference_returning_if_offset_index() {
    assert_eq!(
        evaluate_reference_returning_formula(1, "=OFFSET(INDEX(IF(G1=1,A1:C20,D1:F20),2,1),0,1)",),
        LiteralValue::Number(102.0)
    );
    assert_eq!(
        evaluate_reference_returning_formula(0, "=OFFSET(INDEX(IF(G1=1,A1:C20,D1:F20),2,1),0,1)",),
        LiteralValue::Number(402.0)
    );
}

#[test]
fn reference_returning_if_offset_direct() {
    assert_eq!(
        evaluate_reference_returning_formula(1, "=OFFSET(IF(G1=1,A1:C20,D1:F20),1,1)"),
        LiteralValue::Number(102.0)
    );
    assert_eq!(
        evaluate_reference_returning_formula(0, "=OFFSET(IF(G1=1,A1:C20,D1:F20),1,1)"),
        LiteralValue::Number(402.0)
    );
}

#[test]
fn reference_returning_ifs_offset_index() {
    assert_eq!(
        evaluate_reference_returning_formula(
            1,
            "=OFFSET(INDEX(IFS(G1=1,A1:C20,TRUE,D1:F20),2,1),0,1)",
        ),
        LiteralValue::Number(102.0)
    );
    assert_eq!(
        evaluate_reference_returning_formula(
            0,
            "=OFFSET(INDEX(IFS(G1=1,A1:C20,TRUE,D1:F20),2,1),0,1)",
        ),
        LiteralValue::Number(402.0)
    );
}

#[test]
fn reference_returning_choose_offset_index() {
    assert_eq!(
        evaluate_reference_returning_formula(1, "=OFFSET(INDEX(CHOOSE(G1,A1:C20,D1:F20),2,1),0,1)",),
        LiteralValue::Number(102.0)
    );
    assert_eq!(
        evaluate_reference_returning_formula(2, "=OFFSET(INDEX(CHOOSE(G1,A1:C20,D1:F20),2,1),0,1)",),
        LiteralValue::Number(402.0)
    );
}

#[test]
fn reference_returning_if_family_value_paths() {
    assert_eq!(
        evaluate_reference_returning_formula(1, "=SUM(IF(G1=1,A1:A20,D1:D20))"),
        LiteralValue::Number(210.0)
    );
    assert_eq!(
        evaluate_reference_returning_formula(1, "=VLOOKUP(5,CHOOSE(1,A1:B20,D1:E20),2,FALSE)",),
        LiteralValue::Number(105.0)
    );
    assert_eq!(
        evaluate_reference_returning_formula(1, "=INDEX(IFERROR(1/0,A1:C20),3,3)"),
        LiteralValue::Number(203.0)
    );
    assert_eq!(
        evaluate_reference_returning_formula(1, "=IF(G1=1,5,A1:A3)"),
        LiteralValue::Number(5.0)
    );
    assert_eq!(
        evaluate_reference_returning_formula(0, "=SUM(IF(G1=1,5,A1:A3))"),
        LiteralValue::Number(6.0)
    );
}

#[test]
fn if_family_selector_evaluation_count() {
    use crate::function::{FnCaps, Function};
    use crate::traits::{FunctionContext, ResolvedArgument};
    use formualizer_common::ExcelError;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct CountSelectorFn {
        array: Arc<AtomicBool>,
        calls: Arc<AtomicUsize>,
        selected: Arc<AtomicBool>,
    }

    impl Function for CountSelectorFn {
        fn caps(&self) -> FnCaps {
            FnCaps::empty()
        }

        fn name(&self) -> &'static str {
            "COUNTSELECTOR"
        }

        fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
            &[]
        }

        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.array.load(Ordering::SeqCst) {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(vec![
                    vec![LiteralValue::Boolean(true), LiteralValue::Boolean(false)],
                ])));
            }
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                self.selected.load(Ordering::SeqCst),
            )))
        }
    }

    fn workbook(
        array: Arc<AtomicBool>,
        calls: Arc<AtomicUsize>,
        selected: Arc<AtomicBool>,
    ) -> TestWorkbook {
        TestWorkbook::new()
            .with_range(
                "Sheet1",
                1,
                1,
                vec![
                    vec![LiteralValue::Int(1)],
                    vec![LiteralValue::Int(2)],
                    vec![LiteralValue::Int(3)],
                ],
            )
            .with_function(Arc::new(CountSelectorFn {
                array,
                calls,
                selected,
            }))
    }

    crate::builtins::load_builtins();
    let array = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let selected = Arc::new(AtomicBool::new(true));

    let wb = workbook(
        Arc::clone(&array),
        Arc::clone(&calls),
        Arc::clone(&selected),
    );
    let interpreter = wb.interpreter();
    let ast = formualizer_parse::parser::parse("=IF(COUNTSELECTOR(),A1:A3,5)")
        .expect("valid AST selector formula");
    let handle = ArgumentHandle::new(&ast, &interpreter);
    assert!(matches!(
        handle.resolve_once(),
        Ok(ResolvedArgument::Range(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "AST reference arm");

    calls.store(0, Ordering::SeqCst);
    selected.store(false, Ordering::SeqCst);
    let ast = formualizer_parse::parser::parse("=IF(COUNTSELECTOR(),A1:A3,5)")
        .expect("valid AST selector formula");
    let handle = ArgumentHandle::new(&ast, &interpreter);
    assert!(matches!(
        handle.resolve_once(),
        Ok(ResolvedArgument::Value(crate::traits::CalcValue::Scalar(
            LiteralValue::Number(5.0)
        )))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "AST value arm");

    calls.store(0, Ordering::SeqCst);
    selected.store(true, Ordering::SeqCst);
    let ast = formualizer_parse::parser::parse("=INDEX(IF(COUNTSELECTOR(),A1:A3,D1:D3),0,1)")
        .expect("valid zero-index fallback formula");
    assert!(matches!(
        interpreter.evaluate_ast(&ast),
        Ok(crate::traits::CalcValue::Range(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "AST zero-index fallback");

    for (formula, label) in [
        (
            "=INDEX(IF(COUNTSELECTOR(),5,A1:A3),1)",
            "selected scalar fallback",
        ),
        (
            "=INDEX(IF(COUNTSELECTOR(),A1:A3,D1:D3),99,1)",
            "out-of-bounds fallback",
        ),
        (
            "=INDEX(IF(COUNTSELECTOR(),A1:B3,D1:E3),2)",
            "2-D omitted-column fallback",
        ),
    ] {
        calls.store(0, Ordering::SeqCst);
        let ast = formualizer_parse::parser::parse(formula).expect("valid INDEX fallback formula");
        let _ = interpreter.evaluate_ast(&ast);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "AST {label}");
    }

    for (select_reference, expected) in [(true, 6.0), (false, 5.0)] {
        calls.store(0, Ordering::SeqCst);
        selected.store(select_reference, Ordering::SeqCst);
        let wb = workbook(
            Arc::clone(&array),
            Arc::clone(&calls),
            Arc::clone(&selected),
        );
        let mut engine = crate::engine::Engine::new(
            wb,
            crate::engine::EvalConfig::default().with_cycle(crate::engine::CycleConfig {
                detection: crate::engine::CycleDetection::Runtime,
                policy: crate::engine::CyclePolicy::Error,
            }),
        );
        for (row, value) in [(1, 1), (2, 2), (3, 3)] {
            engine
                .set_cell_value("Sheet1", row, 1, LiteralValue::Int(value))
                .expect("set arena reference value");
        }
        engine
            .set_cell_formula(
                "Sheet1",
                1,
                10,
                formualizer_parse::parser::parse("=SUM(IF(COUNTSELECTOR(),A1:A3,5))")
                    .expect("valid arena selector formula"),
            )
            .expect("set arena selector formula");
        engine
            .evaluate_all()
            .expect("evaluate arena selector formula");
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 10),
            Some(LiteralValue::Number(expected))
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "Arena {} arm",
            if select_reference {
                "reference"
            } else {
                "value"
            }
        );
    }

    for path in ["AST", "Arena"] {
        calls.store(0, Ordering::SeqCst);
        array.store(true, Ordering::SeqCst);
        let wb = workbook(
            Arc::clone(&array),
            Arc::clone(&calls),
            Arc::clone(&selected),
        );
        if path == "AST" {
            let interpreter = wb.interpreter();
            let ast = formualizer_parse::parser::parse("=IF(COUNTSELECTOR(),{1,1},{2,2})")
                .expect("valid AST array-selector formula");
            let handle = ArgumentHandle::new(&ast, &interpreter);
            let _ = handle.resolve_once();
        } else {
            let mut engine = crate::engine::Engine::new(
                wb,
                crate::engine::EvalConfig::default().with_cycle(crate::engine::CycleConfig {
                    detection: crate::engine::CycleDetection::Runtime,
                    policy: crate::engine::CyclePolicy::Error,
                }),
            );
            engine
                .set_cell_formula(
                    "Sheet1",
                    1,
                    10,
                    formualizer_parse::parser::parse("=SUM(IF(COUNTSELECTOR(),{1,1},{2,2}))")
                        .expect("valid Arena array-selector formula"),
                )
                .expect("set Arena array-selector formula");
            engine.evaluate_all().expect("evaluate array selector");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2, "{path} array selector");
        array.store(false, Ordering::SeqCst);
    }
}

fn assert_reference_formula_number(formula: &str, expected: f64) {
    match evaluate_reference_returning_formula(1, formula) {
        LiteralValue::Int(value) => assert_eq!(value as f64, expected, "{formula}"),
        LiteralValue::Number(value) => assert_eq!(value, expected, "{formula}"),
        other => panic!("expected {expected} from {formula}, got {other:?}"),
    }
}

fn assert_reference_formula_value_error(formula: &str) {
    match evaluate_reference_returning_formula(1, formula) {
        LiteralValue::Error(error) => assert_eq!(
            error.kind,
            formualizer_common::ExcelErrorKind::Value,
            "{formula}"
        ),
        other => panic!("expected #VALUE! from {formula}, got {other:?}"),
    }
}

/// CHOOSE selector bounds on the value path. `CHOOSE(len, ...)` is the last
/// valid index and `CHOOSE(len + 1, ...)` must be #VALUE! rather than an
/// out-of-bounds argument access: a selector bound widened by one panics on
/// the len + 1 case while every wider overflow still errors.
#[test]
fn choose_selector_bounds_value_path() {
    assert_reference_formula_number("=CHOOSE(2,A1,B1)", 101.0);
    assert_reference_formula_value_error("=CHOOSE(0,A1,B1)");
    assert_reference_formula_value_error("=CHOOSE(3,A1,B1)");
}

/// The same bounds through `resolve_choose_reference_or_value`: OFFSET forces
/// CHOOSE down the reference path, which carries its own selector check.
#[test]
fn choose_selector_bounds_reference_path() {
    assert_reference_formula_number("=OFFSET(CHOOSE(2,A1,B1),0,0)", 101.0);
    assert_reference_formula_number("=OFFSET(CHOOSE(2,A1,B1),1,0)", 102.0);
    assert_reference_formula_value_error("=OFFSET(CHOOSE(0,A1,B1),0,0)");
    assert_reference_formula_value_error("=OFFSET(CHOOSE(3,A1,B1),0,0)");
}

/// Pin every syntax arm of `ArgumentHandle::may_return_reference` so the
/// guard is falsifiable: it exists to keep arguments that cannot produce a
/// reference off the reference-resolution path, and the downstream let-chain
/// re-rejects them, so only direct assertions can catch a broken arm.
#[test]
fn may_return_reference_syntax_arms() {
    crate::builtins::load_builtins();
    let wb = TestWorkbook::new();
    let interpreter = wb.interpreter();
    let arm = |formula: &str| {
        let ast = formualizer_parse::parser::parse(formula).expect("parse arm formula");
        ArgumentHandle::new(&ast, &interpreter).may_return_reference()
    };

    assert!(arm("=A1"), "cell reference");
    assert!(arm("=A1:B3"), "range reference");
    assert!(arm("=INDEX(A1:B3,1,1)"), "RETURNS_REFERENCE function");
    assert!(
        arm("=UNBOUNDNAME"),
        "an unbound name may be a workbook named range"
    );
    assert!(!arm("=SUM(A1:B3)"), "value-returning function");
    assert!(!arm("=1+2"), "arithmetic expression");
    assert!(!arm("=\"text\""), "literal");
}

/// The LET/LAMBDA exclusion: a local binding shadows any workbook name of the
/// same spelling and locals resolve only on the value path, so a bound name
/// must report itself as not reference-capable.
#[test]
fn may_return_reference_excludes_let_lambda_locals() {
    use crate::interpreter::{LocalBinding, LocalEnv};

    crate::builtins::load_builtins();
    let wb = TestWorkbook::new();
    let interpreter = wb.interpreter();
    let ast = formualizer_parse::parser::parse("=X").expect("parse local name");

    let unbound = ArgumentHandle::new(&ast, &interpreter);
    assert!(
        unbound.may_return_reference(),
        "without a local binding the name may be a workbook named range"
    );

    let env = LocalEnv::default().with_binding("X", LocalBinding::Value(LiteralValue::Int(7)));
    let scoped = interpreter.with_local_env(env);
    let bound = ArgumentHandle::new(&ast, &scoped);
    assert!(
        !bound.may_return_reference(),
        "a LET/LAMBDA local must not be sent down the named-range route"
    );
}
