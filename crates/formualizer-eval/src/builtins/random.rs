//! Volatile functions like RAND, RANDBETWEEN.
use crate::function::Function;
use crate::traits::{ArgumentHandle, EvaluationContext};
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
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        Ok(LiteralValue::Number(rand::thread_rng().gen_range(0.0..1.0)))
    }
}

pub fn register_builtins() {
    crate::function_registry::register_function(std::sync::Arc::new(RandFn));
}

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
        let result = f.eval_scalar(&[], ctx.context).unwrap();
        match result {
            LiteralValue::Number(n) => assert!(n >= 0.0 && n < 1.0),
            _ => panic!("Expected a number"),
        }
    }
}
