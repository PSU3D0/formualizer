use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn serial_eval_config() -> EvalConfig {
    EvalConfig {
        enable_parallel: false,
        ..Default::default()
    }
}

#[test]
fn implicit_intersection_column_vector_selects_by_row() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(42.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 5, 2, parse("=@A1:A10").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn implicit_intersection_row_vector_selects_by_column() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(7.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 3, 3, parse("=@A1:E1").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 3),
        Some(LiteralValue::Number(7.0))
    );
}

#[test]
fn implicit_intersection_2d_selects_by_row_and_col_cross_sheet() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 3, LiteralValue::Number(123.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet2", 5, 3, parse("=@Sheet1!A1:E10").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet2", 5, 3),
        Some(LiteralValue::Number(123.0))
    );
}

#[test]
fn implicit_intersection_out_of_bounds_is_value_error() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(42.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 20, 2, parse("=@A1:A10").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 20, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.to_string(), "#VALUE!"),
        other => panic!("expected #VALUE!, got {other:?}"),
    }
}

#[test]
fn implicit_intersection_suppresses_spill_from_array_function() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=@SEQUENCE(2,2)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(1.0))
    );

    // No spill should occur.
    assert_eq!(engine.get_cell_value("Sheet1", 1, 5), None);
    assert_eq!(engine.get_cell_value("Sheet1", 2, 4), None);
}

#[test]
fn implicit_intersection_against_spilled_values_requires_at_for_scalar() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    // A1 spills a 3x1 vector: A1:A3 = 1,2,3
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();

    // B2 uses @ to pick the intersecting element (A2)
    engine
        .set_cell_formula("Sheet1", 2, 2, parse("=@A1:A3").unwrap())
        .unwrap();

    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(2.0))
    );

    // B2 should be scalar (no spill).
    assert_eq!(engine.get_cell_value("Sheet1", 3, 2), None);
}

#[test]
fn implicit_intersection_whole_row_uses_the_formula_column() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    engine
        .set_cell_value("Sheet1", 21, 1, LiteralValue::Text("id".into()))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 21, 7, LiteralValue::Text("Y".into()))
        .unwrap();
    // G2 reads G21; H2 reads H21, past the row's last used cell (blank).
    engine
        .set_cell_formula("Sheet1", 2, 7, parse("=@21:21").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 8, parse("=@21:21=\"Y\"").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 7),
        Some(LiteralValue::Text("Y".into()))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 8),
        Some(LiteralValue::Boolean(false))
    );
}

#[test]
fn implicit_intersection_whole_column_uses_the_formula_row() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    engine
        .set_cell_value("Sheet2", 4, 1, LiteralValue::Number(9.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 4, 3, parse("=@Sheet2!A:A*2").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 3),
        Some(LiteralValue::Number(18.0))
    );
}

#[test]
fn implicit_intersection_in_a_relative_formula_family_uses_each_placement() {
    // One relative shape down a column: each placement intersects its own
    // window, including rows past the first placement's window.
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    for r in 1..=40 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    for r in 5..30 {
        engine
            .set_cell_formula(
                "Sheet1",
                r,
                2,
                parse(format!("=@A{}:A{}", r - 1, r + 5)).unwrap(),
            )
            .unwrap();
    }
    let _ = engine.evaluate_all().unwrap();
    for r in 5..30 {
        assert_eq!(
            engine.get_cell_value("Sheet1", r, 2),
            Some(LiteralValue::Number(r as f64)),
            "B{r}"
        );
    }
}

#[test]
fn single_column_vector_selects_by_row() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(42.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 5, 2, parse("=SINGLE(A1:A10)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(42.0))
    );
}

#[test]
fn single_xlfn_row_vector_selects_by_column() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(7.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 3, 3, parse("=_xlfn.SINGLE(A1:E1)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 3),
        Some(LiteralValue::Number(7.0))
    );
}

#[test]
fn single_out_of_bounds_is_value_error() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Number(42.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 20, 2, parse("=SINGLE(A1:A10)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 20, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.to_string(), "#VALUE!"),
        other => panic!("expected #VALUE!, got {other:?}"),
    }
}

#[test]
fn single_suppresses_spill_from_array_function() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());

    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=SINGLE(SEQUENCE(2,2))").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(1.0))
    );

    // No spill should occur.
    assert_eq!(engine.get_cell_value("Sheet1", 1, 5), None);
    assert_eq!(engine.get_cell_value("Sheet1", 2, 4), None);
}

#[test]
fn single_in_a_relative_formula_family_uses_each_placement() {
    // Same as the `@` family test above, written as SINGLE.
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    for r in 1..=40 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    for r in 5..30 {
        engine
            .set_cell_formula(
                "Sheet1",
                r,
                2,
                parse(format!("=SINGLE(A{}:A{})", r - 1, r + 5)).unwrap(),
            )
            .unwrap();
    }
    let _ = engine.evaluate_all().unwrap();
    for r in 5..30 {
        assert_eq!(
            engine.get_cell_value("Sheet1", r, 2),
            Some(LiteralValue::Number(r as f64)),
            "B{r}"
        );
    }
}

#[derive(Debug)]
struct FailCancelled;
impl crate::function::Function for FailCancelled {
    fn caps(&self) -> crate::function::FnCaps {
        crate::function::FnCaps::PURE
    }
    fn name(&self) -> &'static str {
        "FAIL_CANCELLED"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
        Err(formualizer_common::ExcelError::new(
            formualizer_common::ExcelErrorKind::Cancelled,
        ))
    }
}

#[test]
fn single_propagates_an_operand_failure_as_at_does() {
    crate::builtins::load_builtins();
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(FailCancelled));
    let interp = wb.interpreter();
    let at = interp.evaluate_ast(&parse("=@FAIL_CANCELLED()").unwrap());
    let single = interp.evaluate_ast(&parse("=SINGLE(FAIL_CANCELLED())").unwrap());
    assert_eq!(
        at.unwrap_err().kind,
        formualizer_common::ExcelErrorKind::Cancelled
    );
    assert_eq!(
        single.unwrap_err().kind,
        formualizer_common::ExcelErrorKind::Cancelled
    );
}

/// Returns `A1:A10` as a reference and counts its calls on both paths.
#[derive(Debug)]
struct CountedRef(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl CountedRef {
    fn reference() -> formualizer_parse::parser::ReferenceType {
        formualizer_parse::parser::ReferenceType::range(None, Some(1), Some(1), Some(10), Some(1))
    }
}
impl crate::function::Function for CountedRef {
    fn caps(&self) -> crate::function::FnCaps {
        crate::function::FnCaps::PURE | crate::function::FnCaps::RETURNS_REFERENCE
    }
    fn name(&self) -> &'static str {
        "COUNTED_REF"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn eval_reference<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Option<Result<formualizer_parse::parser::ReferenceType, formualizer_common::ExcelError>>
    {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(Ok(Self::reference()))
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        ctx: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(crate::traits::CalcValue::Range(ctx.resolve_range_view(
            &Self::reference(),
            ctx.current_sheet(),
        )?))
    }
}

#[test]
fn single_evaluates_a_reference_operand_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    crate::builtins::load_builtins();

    let calls = std::sync::Arc::new(AtomicUsize::new(0));
    let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountedRef(calls.clone())));
    let mut engine = Engine::new(wb, serial_eval_config());
    for r in 1..=10 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 5, 2, parse("=SINGLE(COUNTED_REF())").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn single_respects_let_locals() {
    // A value-bound local is not a workbook name; a reference-bound local
    // intersects the formula's row.
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    for r in 1..=10 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=LET(x,7,SINGLE(x))").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 5, 2, parse("=LET(r,A1:A10,SINGLE(r))").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(7.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(5.0))
    );
}

#[test]
fn implicit_intersection_respects_let_locals() {
    // `@` on a local uses the local, as SINGLE does: a value is reduced like
    // `@expr`, a reference intersects the formula's row.
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    for r in 1..=10 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=LET(x,7,@x)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 5, 2, parse("=LET(r,A1:A10,@r)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=LET(a,SEQUENCE(3),@a)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 5, parse("=LET(x,1,LET(X,7,@x))").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 6, parse("=LET(f,LAMBDA(v,v),@f)").unwrap())
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(7.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(1.0))
    );
    // No spill should occur.
    assert_eq!(engine.get_cell_value("Sheet1", 2, 4), None);
    // The inner local wins, whatever its case.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 5),
        Some(LiteralValue::Number(7.0))
    );
    match engine.get_cell_value("Sheet1", 1, 6) {
        Some(LiteralValue::Error(e)) => {
            assert_eq!(e.kind, formualizer_common::ExcelErrorKind::Calc)
        }
        other => panic!("expected #CALC!, got {other:?}"),
    }
}

#[test]
fn implicit_intersection_respects_lambda_parameters() {
    // LAMBDA bodies run on the AST path; `@` and SINGLE agree there too.
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    for r in 1..=10 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=LET(f,LAMBDA(v,@v),f(7))").unwrap())
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            4,
            parse("=LET(f,LAMBDA(v,SINGLE(v)),f(7))").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            6,
            2,
            parse("=LET(f,LAMBDA(rng,@rng),f(A1:A10))").unwrap(),
        )
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            6,
            3,
            parse("=LET(f,LAMBDA(rng,SINGLE(rng)),f(A1:A10))").unwrap(),
        )
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    for (row, col, expected) in [(1, 3, 7.0), (1, 4, 7.0), (6, 2, 6.0), (6, 3, 6.0)] {
        assert_eq!(
            engine.get_cell_value("Sheet1", row, col),
            Some(LiteralValue::Number(expected)),
            "R{row}C{col}"
        );
    }
}

#[test]
fn implicit_intersection_local_shadows_a_workbook_name() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    use crate::reference::{CellRef, Coord, RangeRef};

    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    for r in 1..=10 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    let sheet1 = engine.sheet_id("Sheet1").unwrap();
    engine
        .define_name(
            "x",
            NamedDefinition::Literal(LiteralValue::Number(42.0)),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .define_name(
            "K",
            NamedDefinition::Range(RangeRef::new(
                CellRef::new(sheet1, Coord::from_excel(1, 1, true, true)),
                CellRef::new(sheet1, Coord::from_excel(10, 1, true, true)),
            )),
            NameScope::Workbook,
        )
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=LET(x,7,@x)").unwrap())
        .unwrap();
    // `alias` holds the workbook name `K`, not the later local `K`.
    engine
        .set_cell_formula("Sheet1", 5, 2, parse("=LET(alias,K,K,7,@alias)").unwrap())
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            5,
            3,
            parse("=LET(alias,K,K,7,SINGLE(alias))").unwrap(),
        )
        .unwrap();
    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(7.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 2),
        Some(LiteralValue::Number(5.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 5, 3),
        Some(LiteralValue::Number(5.0))
    );
}

#[test]
fn implicit_intersection_of_a_local_in_a_relative_formula_family_uses_each_placement() {
    // Same as the `@` family test above, with the window bound to a LET local
    // (arena path) and to a LAMBDA parameter (AST path).
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, serial_eval_config());
    for r in 1..=40 {
        engine
            .set_cell_value("Sheet1", r, 1, LiteralValue::Number(r as f64))
            .unwrap();
    }
    for r in 5..30 {
        engine
            .set_cell_formula(
                "Sheet1",
                r,
                2,
                parse(format!("=LET(r,A{}:A{},@r)", r - 1, r + 5)).unwrap(),
            )
            .unwrap();
        engine
            .set_cell_formula(
                "Sheet1",
                r,
                3,
                parse(format!("=LET(f,LAMBDA(rng,@rng),f(A{}:A{}))", r - 1, r + 5)).unwrap(),
            )
            .unwrap();
    }
    let _ = engine.evaluate_all().unwrap();
    for r in 5..30 {
        assert_eq!(
            engine.get_cell_value("Sheet1", r, 2),
            Some(LiteralValue::Number(r as f64)),
            "B{r}"
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", r, 3),
            Some(LiteralValue::Number(r as f64)),
            "C{r}"
        );
    }
}
