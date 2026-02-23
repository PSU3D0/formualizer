//! Volatile functions like RAND, RANDBETWEEN.
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;
use rand::Rng;

#[derive(Debug)]
pub struct RandFn;

/// Returns a uniformly distributed pseudo-random number in the interval `[0, 1)`.
///
/// `RAND` is volatile and recalculates whenever dependent formulas recalculate.
///
/// # Remarks
/// - The result is always greater than or equal to `0` and strictly less than `1`.
/// - Because the function is volatile, repeated evaluations can return different values.
/// - The engine seeds randomness per evaluation context for reproducible execution flows.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "RAND stays within bounds"
/// formula: "=LET(n, RAND(), AND(n>=0, n<1))"
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Derived integer bucket from RAND"
/// formula: "=LET(n, INT(RAND()*10), AND(n>=0, n<=9))"
/// expected: true
/// ```
///
/// ```yaml,docs
/// related:
///   - RANDBETWEEN
///   - LET
///   - INT
/// faq:
///   - q: "Can RAND return the same value on every recalculation?"
///     a: "Not by default. RAND is volatile, so recalculation can produce a different sample each time."
///   - q: "If RAND is used twice in one formula, do both uses share one sample?"
///     a: "Only if you bind it once (for example with LET). Two separate RAND calls are two separate draws."
///   - q: "Why does RAND look deterministic in some engine runs?"
///     a: "Randomness is seeded per evaluation context, which keeps a run reproducible while still treating RAND as volatile across recalculations."
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: RAND
/// Type: RandFn
/// Min args: 0
/// Max args: 0
/// Variadic: false
/// Signature: RAND()
/// Arg schema: []
/// Caps: VOLATILE
/// [formualizer-docgen:schema:end]
impl Function for RandFn {
    func_caps!(VOLATILE);

    fn name(&self) -> &'static str {
        "RAND"
    }
    fn min_args(&self) -> usize {
        0
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut rng = ctx.rng_for_current(self.function_salt());
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            rng.gen_range(0.0..1.0),
        )))
    }
}

pub fn register_builtins() {
    crate::function_registry::register_function(std::sync::Arc::new(RandFn));
    crate::function_registry::register_function(std::sync::Arc::new(RandBetweenFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{interpreter::Interpreter, test_workbook::TestWorkbook};
    use formualizer_parse::LiteralValue;

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
        let fctx = ctx.function_context(None);
        let args: Vec<ArgumentHandle<'_, '_>> = Vec::new();
        let result = f.dispatch(&args, &fctx).unwrap().into_literal();
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
        let fctx = ctx.function_context(None);
        // Build two scalar args 1 and 3
        let lo = formualizer_parse::ASTNode::new(
            formualizer_parse::ASTNodeType::Literal(LiteralValue::Int(1)),
            None,
        );
        let hi = formualizer_parse::ASTNode::new(
            formualizer_parse::ASTNodeType::Literal(LiteralValue::Int(3)),
            None,
        );
        let args = vec![
            ArgumentHandle::new(&lo, &ctx),
            ArgumentHandle::new(&hi, &ctx),
        ];
        let v = f.dispatch(&args, &fctx).unwrap().into_literal();
        match v {
            LiteralValue::Int(n) => assert!((1..=3).contains(&n)),
            _ => panic!("Expected Int"),
        }
    }
}

#[derive(Debug)]
pub struct RandBetweenFn;

/// Returns a random integer between two inclusive bounds.
///
/// `RANDBETWEEN` evaluates both bounds, then samples an integer in `[low, high]`.
///
/// # Remarks
/// - Bounds are coerced to numbers and truncated to integers.
/// - If `high < low`, the function returns `#NUM!`.
/// - The function is volatile and may return a different integer each recalculation.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Value always falls inside the requested interval"
/// formula: "=LET(n, RANDBETWEEN(1, 3), AND(n>=1, n<=3, INT(n)=n))"
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Equal bounds produce a fixed value"
/// formula: "=RANDBETWEEN(7, 7)"
/// expected: 7
/// ```
///
/// ```yaml,sandbox
/// title: "Upper bound below lower bound is invalid"
/// formula: "=RANDBETWEEN(5, 1)"
/// expected: "#NUM!"
/// ```
///
/// ```yaml,docs
/// related:
///   - RAND
///   - INT
///   - LET
/// faq:
///   - q: "Are both bounds included in RANDBETWEEN?"
///     a: "Yes. RANDBETWEEN samples an integer in the closed interval [low, high]."
///   - q: "What happens with decimal bounds like RANDBETWEEN(1.9, 4.2)?"
///     a: "Bounds are truncated to integers before sampling, so this behaves like RANDBETWEEN(1, 4)."
///   - q: "Is RANDBETWEEN deterministic?"
///     a: "It is volatile, so results can change on recalculation, though a single evaluation context uses seeded randomness for reproducible execution."
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: RANDBETWEEN
/// Type: RandBetweenFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: RANDBETWEEN(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: VOLATILE
/// [formualizer-docgen:schema:end]
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

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // Evaluate bounds as integers
        let lo_v = args[0].value()?.into_literal();
        let hi_v = args[1].value()?.into_literal();
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
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(n)))
    }
}
