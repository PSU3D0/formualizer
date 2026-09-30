use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_eval::engine::{Engine, EvalConfig};
use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;

fn engine(families: bool, lift: bool) -> Engine<TestWorkbook> {
    Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            family_execution: families,
            family_kernels: families,
            family_lift: lift,
            ..EvalConfig::default()
        },
    )
}

fn formula(engine: &mut Engine<TestWorkbook>, row: u32, col: u32, text: &str) {
    engine
        .set_cell_formula("S", row, col, parse(text).unwrap())
        .unwrap();
}

fn assert_num(value: LiteralValue) {
    match value {
        LiteralValue::Error(error) => assert_eq!(error.kind, ExcelErrorKind::Num),
        other => panic!("expected #NUM!, got {other:?}"),
    }
}

#[test]
fn power_overflow_is_a_numeric_error() {
    let mut engine = engine(false, false);
    formula(&mut engine, 1, 1, "=POWER(1E200,2)");
    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("S", 1, 1).unwrap());
}

#[test]
fn exp_overflow_is_catchable_before_storage() {
    let mut engine = engine(false, false);
    formula(&mut engine, 1, 1, "=EXP(1000)");
    formula(&mut engine, 1, 2, "=IFERROR(EXP(1000),77)");
    formula(&mut engine, 1, 3, "=IFERROR(A1,88)");
    engine.evaluate_all().unwrap();
    assert_num(engine.get_cell_value("S", 1, 1).unwrap());
    assert_eq!(
        engine.get_cell_value("S", 1, 2),
        Some(LiteralValue::Number(77.0))
    );
    assert_eq!(
        engine.get_cell_value("S", 1, 3),
        Some(LiteralValue::Number(88.0))
    );
}

#[test]
fn numeric_overflow_does_not_redefine_host_nonfinite_or_udf_policy() {
    use formualizer_common::ExcelError;
    use formualizer_eval::function::Function;
    use formualizer_eval::traits::{ArgumentHandle, CalcValue, FunctionContext};

    struct HostInfinity;
    impl Function for HostInfinity {
        fn name(&self) -> &'static str {
            "HOST_INFINITY"
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _: &'c [ArgumentHandle<'a, 'b>],
            _: &dyn FunctionContext<'b>,
        ) -> Result<CalcValue<'b>, ExcelError> {
            Ok(CalcValue::Scalar(LiteralValue::Number(f64::INFINITY)))
        }
    }
    let mut engine = Engine::new(
        TestWorkbook::new().with_function(std::sync::Arc::new(HostInfinity)),
        EvalConfig {
            enable_parallel: false,
            ..EvalConfig::default()
        },
    );
    engine
        .set_cell_value("S", 1, 1, LiteralValue::Number(f64::INFINITY))
        .unwrap();
    engine
        .set_cell_value("S", 2, 1, LiteralValue::Number(f64::NAN))
        .unwrap();
    formula(&mut engine, 1, 2, "=EXP(A1)");
    formula(&mut engine, 1, 3, "=POWER(A1,2)");
    formula(&mut engine, 1, 4, "=IFERROR(HOST_INFINITY(),77)");
    formula(&mut engine, 2, 2, "=EXP(A2)");
    engine.evaluate_all().unwrap();
    for col in 1..=4 {
        assert_eq!(
            engine.get_cell_value("S", 1, col),
            Some(LiteralValue::Number(f64::INFINITY))
        );
    }
    for col in 1..=2 {
        match engine.get_cell_value("S", 2, col).unwrap() {
            LiteralValue::Number(n) => assert!(n.is_nan()),
            other => panic!("expected host NaN, got {other:?}"),
        }
    }
}

#[test]
fn numeric_overflow_family_dispatch_parity() {
    for (families, lift) in [(false, false), (true, false), (true, true)] {
        let mut engine = engine(families, lift);
        for row in 1..=32 {
            engine
                .set_cell_value("S", row, 1, LiteralValue::Number(1000.0))
                .unwrap();
            formula(&mut engine, row, 2, &format!("=EXP(A{row})"));
            formula(&mut engine, row, 3, &format!("=IFERROR(EXP(A{row}),77)"));
            formula(&mut engine, row, 4, &format!("=POWER(A{row},200)"));
            // Invariant subexpressions may be evaluated once by the lift.
            formula(&mut engine, row, 5, &format!("=IFERROR(EXP(1000),A{row})"));
            // This IFERROR can use typed lanes over stored error operands.
            formula(&mut engine, row, 6, &format!("=IFERROR(B{row},77)"));
        }
        engine.evaluate_all().unwrap();
        for row in 1..=32 {
            assert_num(engine.get_cell_value("S", row, 2).unwrap());
            assert_eq!(
                engine.get_cell_value("S", row, 3),
                Some(LiteralValue::Number(77.0))
            );
            assert_num(engine.get_cell_value("S", row, 4).unwrap());
            assert_eq!(
                engine.get_cell_value("S", row, 5),
                Some(LiteralValue::Number(1000.0))
            );
            assert_eq!(
                engine.get_cell_value("S", row, 6),
                Some(LiteralValue::Number(77.0))
            );
        }
        let stored = engine.get_range_values("S", 1, 2, 32, 2);
        for row in stored {
            assert_num(row[0].clone());
        }
    }
}

#[test]
fn numeric_overflow_preserves_finite_underflow_and_element_errors() {
    let mut engine = engine(true, true);
    for (row, text) in [
        (1, "=POWER(2,10)"),
        (2, "=POWER(1E150,2)"),
        (3, "=EXP(1)"),
        (4, "=EXP(709)"),
        (5, "=EXP(-1000)"),
        (6, "=POWER(1E-200,2)"),
        (7, "=EXP(-740)"),
        (8, "=POWER(-4,0.5)"),
        (9, "=EXP(1/0)"),
        (10, "=POWER(1/0,2)"),
        (11, "=POWER(2,1/0)"),
        (12, "=HSTACK(EXP(1000),4,1/0,POWER(1E200,2))"),
        (13, "=IFERROR(POWER(1E200,2),77)"),
        (14, "=POWER(1E-160,2)"),
        (15, "=POWER(-1E200,3)"),
    ] {
        formula(&mut engine, row, 1, text);
    }
    engine.evaluate_all().unwrap();
    for (row, expected) in [
        (1, 1024.0),
        (2, 1E150_f64.powf(2.0)),
        (3, 1_f64.exp()),
        (4, 709_f64.exp()),
        (5, 0.0),
        (6, 0.0),
        (7, (-740_f64).exp()),
        (13, 77.0),
        (14, 1E-160_f64.powf(2.0)),
    ] {
        assert_eq!(
            engine.get_cell_value("S", row, 1),
            Some(LiteralValue::Number(expected))
        );
    }
    assert_num(engine.get_cell_value("S", 8, 1).unwrap());
    assert_num(engine.get_cell_value("S", 15, 1).unwrap());
    for row in 9..=11 {
        match engine.get_cell_value("S", row, 1).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Div),
            other => panic!("expected #DIV/0!, got {other:?}"),
        }
    }
    assert_num(engine.get_cell_value("S", 12, 1).unwrap());
    assert_eq!(
        engine.get_cell_value("S", 12, 2),
        Some(LiteralValue::Number(4.0))
    );
    match engine.get_cell_value("S", 12, 3).unwrap() {
        LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Div),
        other => panic!("expected #DIV/0!, got {other:?}"),
    }
    assert_num(engine.get_cell_value("S", 12, 4).unwrap());
}
