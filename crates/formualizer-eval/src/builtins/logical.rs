// crates/formualizer-eval/src/builtins/logical.rs

use super::utils::ARG_ANY_ONE;
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, EvaluationContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/* ─────────────────────────── TRUE() ─────────────────────────────── */

#[derive(Debug)]
pub struct TrueFn;

impl Function for TrueFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "TRUE"
    }
    fn min_args(&self) -> usize {
        0
    }

    fn eval_scalar<'a, 'b>(
        &self,
        _args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        Ok(LiteralValue::Boolean(true))
    }
}

/* ─────────────────────────── FALSE() ────────────────────────────── */

#[derive(Debug)]
pub struct FalseFn;

impl Function for FalseFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "FALSE"
    }
    fn min_args(&self) -> usize {
        0
    }

    fn eval_scalar<'a, 'b>(
        &self,
        _args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        Ok(LiteralValue::Boolean(false))
    }
}

/* ─────────────────────────── AND() ──────────────────────────────── */

#[derive(Debug)]
pub struct AndFn;

impl Function for AndFn {
    func_caps!(PURE, REDUCTION, BOOL_ONLY);

    fn name(&self) -> &'static str {
        "AND"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
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
}

/* ─────────────────────────── OR() ───────────────────────────────── */

#[derive(Debug)]
pub struct OrFn;

impl Function for OrFn {
    func_caps!(PURE, REDUCTION, BOOL_ONLY);

    fn name(&self) -> &'static str {
        "OR"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }

    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
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
}

/* ─────────────────────────── IF() ───────────────────────────────── */

#[derive(Debug)]
pub struct IfFn;

impl Function for IfFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "IF"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }

    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 || args.len() > 3 {
            return Ok(LiteralValue::Error(
                ExcelError::from_error_string("#VALUE!")
                    .with_message(format!("IF expects 2 or 3 arguments, got {}", args.len())),
            ));
        }

        let condition = args[0].value()?;
        let b = match condition.as_ref() {
            LiteralValue::Boolean(b) => *b,
            LiteralValue::Number(n) => *n != 0.0,
            LiteralValue::Int(i) => *i != 0,
            _ => {
                return Ok(LiteralValue::Error(ExcelError::from_error_string(
                    "#VALUE!",
                )));
            }
        };

        if b {
            args[1].value().map(|cow| cow.into_owned())
        } else if let Some(arg) = args.get(2) {
            arg.value().map(|cow| cow.into_owned())
        } else {
            Ok(LiteralValue::Boolean(false))
        }
    }
}

/* ─────────────────────────── tests ─────────────────────────────── */

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::ArgumentHandle;
    use crate::{interpreter::Interpreter, test_workbook::TestWorkbook};
    use formualizer_core::LiteralValue;

    fn interp(wb: &TestWorkbook) -> Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn test_true_false() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(TrueFn))
            .with_function(std::sync::Arc::new(FalseFn));

        let ctx = interp(&wb);
        let t = ctx.context.get_function("", "TRUE").unwrap();
        assert_eq!(
            t.eval_scalar(&[], ctx.context).unwrap(),
            LiteralValue::Boolean(true)
        );

        let f = ctx.context.get_function("", "FALSE").unwrap();
        assert_eq!(
            f.eval_scalar(&[], ctx.context).unwrap(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn test_and_or() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(AndFn))
            .with_function(std::sync::Arc::new(OrFn));
        let ctx = interp(&wb);

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
            and.eval_scalar(&hs, ctx.context).unwrap(),
            LiteralValue::Boolean(true)
        );

        let hs2 = vec![
            ArgumentHandle::new(&dummy_ast_false, &ctx),
            ArgumentHandle::new(&dummy_ast_one, &ctx),
        ];
        assert_eq!(
            and.eval_scalar(&hs2, ctx.context).unwrap(),
            LiteralValue::Boolean(false)
        );
        assert_eq!(
            or.eval_scalar(&hs2, ctx.context).unwrap(),
            LiteralValue::Boolean(true)
        );
    }
}

pub fn register_builtins() {
    crate::function_registry::register_function(std::sync::Arc::new(TrueFn));
    crate::function_registry::register_function(std::sync::Arc::new(FalseFn));
    crate::function_registry::register_function(std::sync::Arc::new(AndFn));
    crate::function_registry::register_function(std::sync::Arc::new(OrFn));
    crate::function_registry::register_function(std::sync::Arc::new(IfFn));
}
