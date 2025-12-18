use super::common::create_cell_ref_ast;
use crate::builtins::random::register_builtins;
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType};

#[test]
fn rand_reproducible_given_seed_and_cell_address() {
    register_builtins();
    let wb = TestWorkbook::new();
    let cfg = EvalConfig {
        workbook_seed: 123456789,
        ..Default::default()
    };
    let mut engine = Engine::new(wb, cfg);

    // A1 = RAND(), B1 = RAND()
    for col in [1u32, 2u32] {
        engine
            .set_cell_formula(
                "Sheet1",
                1,
                col,
                ASTNode {
                    node_type: ASTNodeType::Function {
                        name: "RAND".into(),
                        args: vec![],
                    },
                    source_token: None,
                    contains_volatile: true,
                },
            )
            .unwrap();
    }

    engine.evaluate_all().unwrap();
    let a1 = match engine.get_cell_value("Sheet1", 1, 1).unwrap() {
        LiteralValue::Number(n) => n,
        v => panic!("Expected number, got {v:?}"),
    };
    let b1 = match engine.get_cell_value("Sheet1", 1, 2).unwrap() {
        LiteralValue::Number(n) => n,
        v => panic!("Expected number, got {v:?}"),
    };
    assert_ne!(a1, b1, "Different cells should get different streams");

    // Re-evaluating without changing seed should keep values stable under OnOpen
    engine.set_volatile_level(crate::traits::VolatileLevel::OnOpen);
    engine.evaluate_all().unwrap();
    let a1_again = match engine.get_cell_value("Sheet1", 1, 1).unwrap() {
        LiteralValue::Number(n) => n,
        _ => unreachable!(),
    };
    assert_eq!(
        a1, a1_again,
        "OnOpen should keep RAND stable without seed change"
    );
}

#[test]
fn volatile_flags_propagate_through_graph_and_recalc_policy() {
    register_builtins();
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // A1 = RAND(); B1 = A1
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            1,
            ASTNode {
                node_type: ASTNodeType::Function {
                    name: "RAND".into(),
                    args: vec![],
                },
                source_token: None,
                contains_volatile: true,
            },
        )
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, create_cell_ref_ast(None, 1, 1))
        .unwrap();

    // Default Always: value should change between evaluations (we vary seed to be explicit)
    engine.set_workbook_seed(1);
    engine.evaluate_all().unwrap();
    let first = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    engine.set_workbook_seed(2);
    engine.evaluate_all().unwrap();
    let second = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    assert_ne!(
        first, second,
        "Volatile cell should refresh between runs with changed seed"
    );

    // OnRecalc: epoch increment should change RNG even with same seed
    engine.set_workbook_seed(42);
    engine.set_volatile_level(crate::traits::VolatileLevel::OnRecalc);
    engine.evaluate_all().unwrap();
    let v1 = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    engine.evaluate_all().unwrap();
    let v2 = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    assert_ne!(
        v1, v2,
        "OnRecalc should change value across full recalc cycles"
    );

    // OnOpen: stable across recalc unless seed changes
    engine.set_volatile_level(crate::traits::VolatileLevel::OnOpen);
    engine.set_workbook_seed(777);
    engine.evaluate_all().unwrap();
    let s1 = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    engine.evaluate_all().unwrap();
    let s2 = engine.get_cell_value("Sheet1", 1, 2).unwrap();
    assert_eq!(
        s1, s2,
        "OnOpen should not change across recalc if seed unchanged"
    );
}

#[test]
fn randbetween_uses_context_rng_and_bounds() {
    register_builtins();
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // A1 = RANDBETWEEN(1, 1)
    let lo = ASTNode {
        node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
        source_token: None,
        contains_volatile: false,
    };
    let hi = ASTNode {
        node_type: ASTNodeType::Literal(LiteralValue::Int(1)),
        source_token: None,
        contains_volatile: false,
    };
    let call = ASTNode {
        node_type: ASTNodeType::Function {
            name: "RANDBETWEEN".into(),
            args: vec![lo, hi],
        },
        source_token: None,
        contains_volatile: true,
    };
    engine.set_cell_formula("Sheet1", 1, 1, call).unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Int(1))
    );
}

#[test]
fn context_scoped_volatility_detection() {
    // Define a synthetic function VOL() that is marked VOLATILE and returns 0.
    use crate::args::ArgSchema;
    use crate::function::{FnCaps, Function};
    use crate::traits::{ArgumentHandle, FunctionContext};
    use std::sync::Arc;

    #[derive(Debug)]
    struct VolFn;
    impl Function for VolFn {
        fn caps(&self) -> FnCaps {
            FnCaps::VOLATILE
        }
        fn name(&self) -> &'static str {
            "VOL"
        }
        fn arg_schema(&self) -> &'static [ArgSchema] {
            &[]
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
        }
    }

    // Register only in the workbook (context provider), not globally
    let wb = TestWorkbook::new().with_function(Arc::new(VolFn));
    let mut engine = Engine::new(wb, EvalConfig::default());

    // A1 = VOL()
    let call = ASTNode {
        node_type: ASTNodeType::Function {
            name: "VOL".into(),
            args: vec![],
        },
        source_token: None,
        contains_volatile: true,
    };
    engine.set_cell_formula("Sheet1", 1, 1, call).unwrap();

    // Evaluate and verify engine marks the vertex volatile by cycling values between runs
    engine.set_workbook_seed(1);
    engine.evaluate_all().unwrap();
    let v1 = engine.get_cell_value("Sheet1", 1, 1).unwrap();
    engine.set_workbook_seed(2);
    engine.evaluate_all().unwrap();
    let v2 = engine.get_cell_value("Sheet1", 1, 1).unwrap();
    // Value is constant 0, but volatile status causes re-eval; just assert evaluation path doesn't error
    assert_eq!(v1, LiteralValue::Int(0));
    assert_eq!(v2, LiteralValue::Int(0));
}
