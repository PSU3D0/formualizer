use super::super::utils::{ARG_NUM_LENIENT_ONE, ARG_NUM_LENIENT_TWO, coerce_num};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

#[derive(Debug)]
pub struct FactFn;
/// Returns the factorial of a non-negative integer.
///
/// `FACT` truncates fractional inputs toward zero before computing the factorial.
///
/// # Remarks
/// - Non-numeric values that cannot be coerced return `#VALUE!`.
/// - Negative inputs return `#NUM!`.
/// - Results above `170!` overflow Excel-compatible limits and return `#NUM!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Basic factorial"
/// formula: "=FACT(5)"
/// expected: 120
/// ```
///
/// ```yaml,sandbox
/// title: "Fractional input is truncated"
/// formula: "=FACT(5.9)"
/// expected: 120
/// ```
///
/// ```yaml,sandbox
/// title: "Negative input returns numeric error"
/// formula: "=FACT(-1)"
/// expected: "#NUM!"
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: FACT
/// Type: FactFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: FACT(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FactFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FACT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let v = args[0].value()?.into_literal();
        let n = match v {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_num(&other)?,
        };

        // Excel truncates to integer
        let n = n.trunc() as i64;

        if n < 0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Factorial calculation (Excel supports up to 170!)
        if n > 170 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let mut result = 1.0_f64;
        for i in 2..=(n as u64) {
            result *= i as f64;
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(result)))
    }
}

#[derive(Debug)]
pub struct GcdFn;
/// Returns the greatest common divisor of one or more integers.
///
/// `GCD` truncates each argument toward zero before calculating the divisor.
///
/// # Remarks
/// - Inputs must be between `0` and `9.99999999e9` after truncation, or `#NUM!` is returned.
/// - Negative values return `#NUM!`.
/// - Any argument error propagates immediately.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Greatest common divisor of two numbers"
/// formula: "=GCD(24, 36)"
/// expected: 12
/// ```
///
/// ```yaml,sandbox
/// title: "Variadic and fractional arguments"
/// formula: "=GCD(18.9, 6, 30)"
/// expected: 6
/// ```
///
/// ```yaml,sandbox
/// title: "Negative values are invalid"
/// formula: "=GCD(-2, 4)"
/// expected: "#NUM!"
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: GCD
/// Type: GcdFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: GCD(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for GcdFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "GCD"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        fn gcd(a: u64, b: u64) -> u64 {
            if b == 0 { a } else { gcd(b, a % b) }
        }

        let mut result: Option<u64> = None;

        for arg in args {
            let v = arg.value()?.into_literal();
            let n = match v {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                other => coerce_num(&other)?,
            };

            // Excel truncates and requires non-negative
            let n = n.trunc();
            if !(0.0..=9.99999999e9).contains(&n) {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }
            let n = n as u64;

            result = Some(match result {
                None => n,
                Some(r) => gcd(r, n),
            });
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(
            result.unwrap_or(0) as f64
        )))
    }
}

#[derive(Debug)]
pub struct LcmFn;
/// Returns the least common multiple of one or more integers.
///
/// `LCM` truncates fractional arguments toward zero and combines values iteratively.
///
/// # Remarks
/// - Inputs must be non-negative and within the supported Excel-compatible range.
/// - If any input is `0`, the resulting least common multiple is `0`.
/// - Any argument error propagates immediately.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Least common multiple for two integers"
/// formula: "=LCM(4, 6)"
/// expected: 12
/// ```
///
/// ```yaml,sandbox
/// title: "Fractional values are truncated"
/// formula: "=LCM(6.8, 8.2)"
/// expected: 24
/// ```
///
/// ```yaml,sandbox
/// title: "Negative values return numeric error"
/// formula: "=LCM(-3, 6)"
/// expected: "#NUM!"
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: LCM
/// Type: LcmFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: LCM(arg1: number@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for LcmFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LCM"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        fn gcd(a: u64, b: u64) -> u64 {
            if b == 0 { a } else { gcd(b, a % b) }
        }
        fn lcm(a: u64, b: u64) -> u64 {
            if a == 0 || b == 0 {
                0
            } else {
                (a / gcd(a, b)) * b
            }
        }

        let mut result: Option<u64> = None;

        for arg in args {
            let v = arg.value()?.into_literal();
            let n = match v {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                other => coerce_num(&other)?,
            };

            let n = n.trunc();
            if !(0.0..=9.99999999e9).contains(&n) {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }
            let n = n as u64;

            result = Some(match result {
                None => n,
                Some(r) => lcm(r, n),
            });
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(
            result.unwrap_or(0) as f64
        )))
    }
}

#[derive(Debug)]
pub struct CombinFn;
/// Returns the number of combinations for selecting `k` items from `n`.
///
/// `COMBIN` evaluates `n` choose `k` using truncated integer inputs.
///
/// # Remarks
/// - Fractional inputs are truncated toward zero before evaluation.
/// - If `n < 0`, `k < 0`, or `k > n`, the function returns `#NUM!`.
/// - Argument errors propagate directly.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Basic combinations"
/// formula: "=COMBIN(5, 2)"
/// expected: 10
/// ```
///
/// ```yaml,sandbox
/// title: "Fractional arguments are truncated"
/// formula: "=COMBIN(6.9, 3.2)"
/// expected: 20
/// ```
///
/// ```yaml,sandbox
/// title: "Invalid k returns numeric error"
/// formula: "=COMBIN(3, 5)"
/// expected: "#NUM!"
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: COMBIN
/// Type: CombinFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: COMBIN(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for CombinFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "COMBIN"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Check minimum required arguments
        if args.len() < 2 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let n_val = args[0].value()?.into_literal();
        let k_val = args[1].value()?.into_literal();

        let n = match n_val {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_num(&other)?,
        };
        let k = match k_val {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_num(&other)?,
        };

        let n = n.trunc() as i64;
        let k = k.trunc() as i64;

        if n < 0 || k < 0 || k > n {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Calculate C(n, k) = n! / (k! * (n-k)!)
        // Use the more efficient formula: C(n, k) = product of (n-i)/(i+1) for i in 0..k
        let k = k.min(n - k) as u64; // Use symmetry for efficiency
        let n = n as u64;

        let mut result = 1.0_f64;
        for i in 0..k {
            result = result * (n - i) as f64 / (i + 1) as f64;
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(result.round())))
    }
}

#[derive(Debug)]
pub struct PermutFn;
/// Returns the number of permutations for selecting and ordering `k` items from `n`.
///
/// `PERMUT` computes `n!/(n-k)!` after truncating both inputs toward zero.
///
/// # Remarks
/// - Fractional inputs are truncated to integers.
/// - If `n < 0`, `k < 0`, or `k > n`, the function returns `#NUM!`.
/// - Argument errors propagate directly.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Basic permutations"
/// formula: "=PERMUT(5, 2)"
/// expected: 20
/// ```
///
/// ```yaml,sandbox
/// title: "Fractional arguments are truncated"
/// formula: "=PERMUT(7.9, 3.1)"
/// expected: 210
/// ```
///
/// ```yaml,sandbox
/// title: "Out-of-range k returns numeric error"
/// formula: "=PERMUT(4, 6)"
/// expected: "#NUM!"
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: PERMUT
/// Type: PermutFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: PERMUT(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for PermutFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PERMUT"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Check minimum required arguments
        if args.len() < 2 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let n_val = args[0].value()?.into_literal();
        let k_val = args[1].value()?.into_literal();

        let n = match n_val {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_num(&other)?,
        };
        let k = match k_val {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_num(&other)?,
        };

        let n = n.trunc() as i64;
        let k = k.trunc() as i64;

        if n < 0 || k < 0 || k > n {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // P(n, k) = n! / (n-k)! = n * (n-1) * ... * (n-k+1)
        let mut result = 1.0_f64;
        for i in 0..k {
            result *= (n - i) as f64;
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(result)))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(FactFn));
    crate::function_registry::register_function(Arc::new(GcdFn));
    crate::function_registry::register_function(Arc::new(LcmFn));
    crate::function_registry::register_function(Arc::new(CombinFn));
    crate::function_registry::register_function(Arc::new(PermutFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn fact_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(FactFn));
        let ctx = interp(&wb);
        let n = lit(LiteralValue::Number(5.0));
        let f = ctx.context.get_function("", "FACT").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(120.0)
        );
    }

    #[test]
    fn fact_zero() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(FactFn));
        let ctx = interp(&wb);
        let n = lit(LiteralValue::Number(0.0));
        let f = ctx.context.get_function("", "FACT").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(1.0)
        );
    }

    #[test]
    fn gcd_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(GcdFn));
        let ctx = interp(&wb);
        let a = lit(LiteralValue::Number(12.0));
        let b = lit(LiteralValue::Number(18.0));
        let f = ctx.context.get_function("", "GCD").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(6.0)
        );
    }

    #[test]
    fn lcm_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(LcmFn));
        let ctx = interp(&wb);
        let a = lit(LiteralValue::Number(4.0));
        let b = lit(LiteralValue::Number(6.0));
        let f = ctx.context.get_function("", "LCM").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(12.0)
        );
    }

    #[test]
    fn combin_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CombinFn));
        let ctx = interp(&wb);
        let n = lit(LiteralValue::Number(5.0));
        let k = lit(LiteralValue::Number(2.0));
        let f = ctx.context.get_function("", "COMBIN").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx), ArgumentHandle::new(&k, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(10.0)
        );
    }

    #[test]
    fn permut_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(PermutFn));
        let ctx = interp(&wb);
        let n = lit(LiteralValue::Number(5.0));
        let k = lit(LiteralValue::Number(2.0));
        let f = ctx.context.get_function("", "PERMUT").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx), ArgumentHandle::new(&k, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Number(20.0)
        );
    }
}
