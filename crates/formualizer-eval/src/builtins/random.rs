//! Volatile functions like RAND, RANDBETWEEN.
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;
use rand::Rng;

#[derive(Debug)]
pub struct RandFn;

impl Function for RandFn {
    func_caps!(VOLATILE);

    fn name(&self) -> &'static str {
        "RAND"
    }
    fn min_args(&self) -> usize {
        0
    }

    fn eval_scalar<'a, 'b>(
        &self,
        _args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let mut rng = ctx.rng_for_current(self.function_salt());
        Ok(LiteralValue::Number(rng.gen_range(0.0..1.0)))
    }
}

pub fn register_builtins() {
    crate::function_registry::register_function(std::sync::Arc::new(RandFn));
    crate::function_registry::register_function(std::sync::Arc::new(RandBetweenFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::DefaultFunctionContext;
    use crate::{interpreter::Interpreter, test_workbook::TestWorkbook};
    use formualizer_core::LiteralValue;

    fn interp(wb: &TestWorkbook) -> Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn test_rand_caps() {
        let rand_fn = RandFn;
        let caps = rand_fn.caps();

        // Check that VOLATILE is set
        assert!(caps.contains(crate::function::FnCaps::VOLATILE));

        // Check that PURE is not set (volatile functions are not pure)
        assert!(!caps.contains(crate::function::FnCaps::PURE));
    }

    #[test]
    fn test_rand() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RandFn));
        let ctx = interp(&wb);

        let f = ctx.context.get_function("", "RAND").unwrap();
        let fctx = DefaultFunctionContext::new(ctx.context, None);
        let result = f.eval_scalar(&[], &fctx).unwrap();
        match result {
            LiteralValue::Number(n) => assert!((0.0..1.0).contains(&n)),
            _ => panic!("Expected a number"),
        }
    }

    #[test]
    fn test_randbetween_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RandBetweenFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "RANDBETWEEN").unwrap();
        let fctx = DefaultFunctionContext::new(ctx.context, None);
        // Build two scalar args 1 and 3
        let lo = formualizer_core::ASTNode::new(
            formualizer_core::ASTNodeType::Literal(LiteralValue::Int(1)),
            None,
        );
        let hi = formualizer_core::ASTNode::new(
            formualizer_core::ASTNodeType::Literal(LiteralValue::Int(3)),
            None,
        );
        let args = vec![
            ArgumentHandle::new(&lo, &ctx),
            ArgumentHandle::new(&hi, &ctx),
        ];
        let v = f.eval_scalar(&args, &fctx).unwrap();
        match v {
            LiteralValue::Int(n) => assert!((1..=3).contains(&n)),
            _ => panic!("Expected Int"),
        }
    }
}

#[derive(Debug)]
pub struct RandBetweenFn;

impl Function for RandBetweenFn {
    func_caps!(VOLATILE);

    fn name(&self) -> &'static str {
        "RANDBETWEEN"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &crate::builtins::utils::ARG_NUM_LENIENT_TWO[..]
    }

    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        // Evaluate bounds as integers
        let lo_v = args[0].value()?.into_owned();
        let hi_v = args[1].value()?.into_owned();
        let lo = match lo_v {
            LiteralValue::Int(n) => n,
            LiteralValue::Number(n) => n as i64,
            _ => 0,
        };
        let hi = match hi_v {
            LiteralValue::Int(n) => n,
            LiteralValue::Number(n) => n as i64,
            _ => 0,
        };
        if hi < lo {
            return Err(ExcelError::new(formualizer_common::ExcelErrorKind::Num)
                .with_message("RANDBETWEEN: hi < lo".to_string()));
        }
        let mut rng = ctx.rng_for_current(self.function_salt());
        let n = if (hi - lo) as u64 == u64::MAX {
            lo
        } else {
            rng.gen_range(lo..=hi)
        };
        Ok(LiteralValue::Int(n))
    }
}
