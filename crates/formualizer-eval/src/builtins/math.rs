use crate::traits::{ArgumentHandle, EvaluationContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::excel_fn;

/* ─────────────────────────── SUM() ──────────────────────────── */

#[excel_fn(name = "SUM", min = 1, variadic, arg_types = "any")]
pub fn sum_fn(
    args: &[ArgumentHandle],
    _ctx: &dyn EvaluationContext,
) -> Result<LiteralValue, ExcelError> {
    if args.is_empty() {
        return Ok(LiteralValue::Error(
            ExcelError::from_error_string("#VALUE!")
                .with_message("SUM expects at least one argument".to_string()),
        ));
    }

    let mut total = 0.0;
    for h in args {
        match h.value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            LiteralValue::Array(arr) => {
                for row in arr {
                    for v in row {
                        total += coerce_num(v)?;
                    }
                }
            }
            v => total += coerce_num(v)?,
        }
    }
    Ok(LiteralValue::Number(total))
}

fn coerce_num(v: &LiteralValue) -> Result<f64, ExcelError> {
    match v {
        LiteralValue::Number(n) => Ok(*n),
        LiteralValue::Int(i) => Ok(*i as f64),
        LiteralValue::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        _ => Err(ExcelError::from_error_string("#VALUE!")
            .with_message(format!("Cannot convert {:?} to number", v))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::with_fns;
    use formualizer_core::LiteralValue;

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn test_sum_basic() {
        let wb = TestWorkbook::new().with_fns(with_fns![__FnSUM]);
        let ctx = interp(&wb);

        // Test basic SUM functionality by creating ArgumentHandles manually
        let dummy_ast_1 = formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(1.0)),
            None,
        );
        let dummy_ast_2 = formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(2.0)),
            None,
        );
        let dummy_ast_3 = formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(3.0)),
            None,
        );

        let args = vec![
            ArgumentHandle::new(&dummy_ast_1, &ctx),
            ArgumentHandle::new(&dummy_ast_2, &ctx),
            ArgumentHandle::new(&dummy_ast_3, &ctx),
        ];

        let sum_fn = ctx.context.get_function("", "SUM").unwrap();
        let result = sum_fn.eval(&args, ctx.context).unwrap();
        assert_eq!(result, LiteralValue::Number(6.0));
    }
}

pub fn register_builtins() {
    crate::function_registry::register(std::sync::Arc::new(__FnSUM));
}
