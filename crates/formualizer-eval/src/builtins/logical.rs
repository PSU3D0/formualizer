// crates/formualizer-eval/src/builtin_logical.rs
// --------------------------------------------------
// First batch of very simple logical functions implemented with the
// `#[excel_fn]` attribute.

use crate::traits::{ArgumentHandle, EvaluationContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::excel_fn;

/* ─────────────────────────── TRUE() ─────────────────────────────── */

#[excel_fn(name = "TRUE", min = 0)]
pub fn true_fn(
    _args: &[ArgumentHandle],
    _ctx: &dyn EvaluationContext,
) -> Result<LiteralValue, ExcelError> {
    Ok(LiteralValue::Boolean(true))
}

/* ─────────────────────────── FALSE() ────────────────────────────── */

#[excel_fn(name = "FALSE", min = 0)]
pub fn false_fn(
    _args: &[ArgumentHandle],
    _ctx: &dyn EvaluationContext,
) -> Result<LiteralValue, ExcelError> {
    Ok(LiteralValue::Boolean(false))
}

/* ─────────────────────────── AND() ──────────────────────────────── */

#[excel_fn(name = "AND", min = 1, variadic, arg_types = "any")]
pub fn and_fn(
    args: &[ArgumentHandle],
    _ctx: &dyn EvaluationContext,
) -> Result<LiteralValue, ExcelError> {
    for h in args {
        let v = h.value()?;
        match v.as_ref() {
            // Blank treated as FALSE (Excel behaviour)
            LiteralValue::Empty => return Ok(LiteralValue::Boolean(false)),
            // Error propagates immediately
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            // Boolean – use as–is
            LiteralValue::Boolean(b) => {
                if !*b {
                    return Ok(LiteralValue::Boolean(false));
                }
            }
            // Numbers – zero = FALSE, non‑zero = TRUE
            LiteralValue::Number(n) if *n == 0.0 => return Ok(LiteralValue::Boolean(false)),
            LiteralValue::Number(_) => {}
            LiteralValue::Int(i) if *i == 0 => return Ok(LiteralValue::Boolean(false)),
            LiteralValue::Int(_) => {}
            // Anything else → #VALUE!
            _ => {
                return Ok(LiteralValue::Error(ExcelError::from_error_string(
                    "#VALUE!",
                )));
            }
        }
    }
    Ok(LiteralValue::Boolean(true))
}

/* ─────────────────────────── OR() ───────────────────────────────── */

#[excel_fn(name = "OR", min = 1, variadic, arg_types = "any")]
pub fn or_fn(
    args: &[ArgumentHandle],
    _ctx: &dyn EvaluationContext,
) -> Result<LiteralValue, ExcelError> {
    let mut found_true = false;

    for h in args {
        let v = h.value()?;
        match v.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            LiteralValue::Boolean(b) => {
                if *b {
                    found_true = true
                }
            }
            LiteralValue::Number(n) => {
                if *n != 0.0 {
                    found_true = true
                }
            }
            LiteralValue::Int(i) => {
                if *i != 0 {
                    found_true = true
                }
            }
            LiteralValue::Empty => {} // ignored
            _ => {
                return Ok(LiteralValue::Error(ExcelError::from_error_string(
                    "#VALUE!",
                )));
            }
        }
    }

    Ok(LiteralValue::Boolean(found_true))
}

/* ─────────────────────────── tests ─────────────────────────────── */

#[cfg(test)]
mod tests {

    use formualizer_core::LiteralValue;

    use crate::traits::ArgumentHandle;
    use crate::with_fns;
    use crate::{interpreter::Interpreter, test_workbook::TestWorkbook};

    use crate::builtins::logical::{__FnAND, __FnFALSE, __FnOR, __FnTRUE};

    fn interp() -> Interpreter {
        let wb = TestWorkbook::new().with_fns(with_fns![__FnTRUE, __FnFALSE, __FnAND, __FnOR,]);

        wb.interpreter()
    }

    #[test]
    fn test_true_false() {
        let ctx = interp();
        let t = ctx.context.get_function("", "TRUE").unwrap();
        assert_eq!(
            t.eval(&[], ctx.context.as_ref()).unwrap(),
            LiteralValue::Boolean(true)
        );

        let f = ctx.context.get_function("", "FALSE").unwrap();
        assert_eq!(
            f.eval(&[], ctx.context.as_ref()).unwrap(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn test_and_or() {
        let ctx = interp();

        let and = ctx.context.get_function("", "AND").unwrap();
        let or = ctx.context.get_function("", "OR").unwrap();
        // Build ArgumentHandles manually: TRUE, 1, FALSE
        let dummy_ast = formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Boolean(true)),
            None,
        );
        let dummy_ast_false = formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Boolean(false)),
            None,
        );
        let dummy_ast_one = formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Int(1)),
            None,
        );
        let hs = vec![
            ArgumentHandle::new(&dummy_ast, &ctx),
            ArgumentHandle::new(&dummy_ast_one, &ctx),
        ];
        assert_eq!(
            and.eval(&hs, ctx.context.as_ref()).unwrap(),
            LiteralValue::Boolean(true)
        );

        let hs2 = vec![
            ArgumentHandle::new(&dummy_ast_false, &ctx),
            ArgumentHandle::new(&dummy_ast_one, &ctx),
        ];
        assert_eq!(
            and.eval(&hs2, ctx.context.as_ref()).unwrap(),
            LiteralValue::Boolean(false)
        );
        assert_eq!(
            or.eval(&hs2, ctx.context.as_ref()).unwrap(),
            LiteralValue::Boolean(true)
        );
    }
}
