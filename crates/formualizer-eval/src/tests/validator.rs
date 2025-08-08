use crate::args::{ArgSchema, CoercionPolicy, ShapeKind, ValidationOptions, validate_and_prepare};
use crate::test_workbook::TestWorkbook;
use crate::traits::ArgumentHandle;
use formualizer_common::{ExcelErrorKind, LiteralValue};

fn lit(v: LiteralValue) -> formualizer_core::parser::ASTNode {
    formualizer_core::parser::ASTNode::new(formualizer_core::parser::ASTNodeType::Literal(v), None)
}

#[test]
fn validator_enforces_min_args_and_max_when_not_variadic() {
    let wb = TestWorkbook::new();
    let ctx = wb.interpreter();
    let a0 = lit(LiteralValue::Number(1.0));
    let a1 = lit(LiteralValue::Number(2.0));
    let args: Vec<ArgumentHandle> = vec![&a0, &a1]
        .into_iter()
        .map(|n| ArgumentHandle::new(n, &ctx))
        .collect();

    // Schema expects one scalar number
    let mut s = ArgSchema::number_lenient_scalar();
    s.coercion = CoercionPolicy::NumberLenientText;
    let schema = vec![s];

    // Too many args → #VALUE! is enforced in dispatch, not in validator; validator should accept
    let res = validate_and_prepare(
        &args,
        &schema,
        ctx.context,
        ValidationOptions { warn_only: false },
    );
    assert!(
        res.is_ok(),
        "validator should not enforce max; dispatch enforces max"
    );
}

#[test]
fn schema_scalar_allows_scalar_in_range_position_fallback() {
    let wb = TestWorkbook::new();
    let ctx = wb.interpreter();
    let a0 = lit(LiteralValue::Number(6.0));
    let args: Vec<ArgumentHandle> = vec![&a0]
        .into_iter()
        .map(|n| ArgumentHandle::new(n, &ctx))
        .collect();

    let mut s = ArgSchema::number_lenient_scalar();
    s.shape = ShapeKind::Range; // function expects a range, but we provide a scalar
    let schema = vec![s];

    let out = validate_and_prepare(
        &args,
        &schema,
        ctx.context,
        ValidationOptions { warn_only: false },
    )
    .unwrap();
    assert_eq!(out.items.len(), 1);
    match &out.items[0] {
        crate::args::PreparedArg::Value(v) => assert_eq!(v.as_ref(), &LiteralValue::Number(6.0)),
        _ => panic!("expected scalar fallback for range-shaped arg"),
    }
}

#[test]
fn number_lenient_text_coercion_accepts_numeric_text() {
    let wb = TestWorkbook::new();
    let ctx = wb.interpreter();
    let a0 = lit(LiteralValue::Text("42".into()));
    let args: Vec<ArgumentHandle> = vec![&a0]
        .into_iter()
        .map(|n| ArgumentHandle::new(n, &ctx))
        .collect();

    let s = ArgSchema::number_lenient_scalar();
    let schema = vec![s];

    let out = validate_and_prepare(
        &args,
        &schema,
        ctx.context,
        ValidationOptions { warn_only: false },
    )
    .unwrap();
    assert_eq!(out.items.len(), 1);
    // We currently do not coerce into numbers in PreparedArg::Value; ensure validator allows it
    match &out.items[0] {
        crate::args::PreparedArg::Value(v) => assert!(matches!(v.as_ref(), LiteralValue::Text(_))),
        _ => panic!("expected value"),
    }
}

#[test]
fn by_ref_accepts_ast_reference() {
    let wb = TestWorkbook::new();
    let ctx = wb.interpreter();
    let a0 = formualizer_core::parser::ASTNode::new(
        formualizer_core::parser::ASTNodeType::Reference {
            original: "A1".to_string(),
            reference: formualizer_core::parser::ReferenceType::Cell {
                sheet: None,
                row: 1,
                col: 1,
            },
        },
        None,
    );
    let args: Vec<ArgumentHandle> = vec![&a0]
        .into_iter()
        .map(|n| ArgumentHandle::new(n, &ctx))
        .collect();

    let mut s = ArgSchema::any();
    s.by_ref = true;
    let schema = vec![s];

    let out = validate_and_prepare(
        &args,
        &schema,
        ctx.context,
        ValidationOptions { warn_only: false },
    )
    .unwrap();
    assert_eq!(out.items.len(), 1);
    match &out.items[0] {
        crate::args::PreparedArg::Reference(r) => match r {
            formualizer_core::parser::ReferenceType::Cell { row, col, .. } => {
                assert_eq!((*row, *col), (1, 1));
            }
            _ => panic!("expected cell reference"),
        },
        _ => panic!("expected reference"),
    }
}
