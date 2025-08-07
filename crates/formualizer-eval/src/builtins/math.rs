use super::utils::{
    ARG_ANY_ONE, ARG_NUM_LENIENT_ONE, ARG_NUM_LENIENT_TWO, ARG_RANGE_NUM_LENIENT_ONE,
    EPSILON_NEAR_ZERO, binary_numeric_args, coerce_num, unary_numeric_arg,
};
use crate::args::ArgSchema;
use crate::function::{FnFoldCtx, Function};
use crate::traits::{ArgumentHandle, EvaluationContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;
use std::f64::consts::PI;

/* ─────────────────────────── SUM() ──────────────────────────── */

#[derive(Debug)]
pub struct SumFn;

impl Function for SumFn {
    func_caps!(PURE, REDUCTION, NUMERIC_ONLY, STREAM_OK);

    fn name(&self) -> &'static str {
        "SUM"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }

    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let mut total = 0.0;
        for arg in args {
            // Try to get a range/stream first. If that fails, fall back to a single value.
            if let Ok(storage) = arg.range_storage() {
                for value_cow in storage.to_iterator() {
                    total += coerce_num(value_cow.as_ref())?;
                }
            } else {
                // Fallback for arguments that are not ranges but might be single values or errors.
                match arg.value()?.as_ref() {
                    LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
                    v => total += coerce_num(v)?,
                }
            }
        }
        Ok(LiteralValue::Number(total))
    }

    fn eval_fold(&self, f: &mut dyn FnFoldCtx) -> Option<Result<LiteralValue, ExcelError>> {
        let mut acc = 0.0f64;
        // Stream numeric chunks using the fold context. Use a moderate default chunk size.
        let mut cb = |chunk: crate::stripes::NumericChunk| -> Result<(), ExcelError> {
            for &n in chunk.data {
                acc += n;
            }
            Ok(())
        };
        if let Err(e) = f.for_each_numeric_chunk(4096, &mut cb) {
            return Some(Ok(LiteralValue::Error(e)));
        }
        let out = LiteralValue::Number(acc);
        f.write_result(out.clone());
        Some(Ok(out))
    }
}

/* ─────────────────────────── COUNT() ──────────────────────────── */

#[derive(Debug)]
pub struct CountFn;

impl Function for CountFn {
    func_caps!(PURE, REDUCTION, NUMERIC_ONLY, STREAM_OK);

    fn name(&self) -> &'static str {
        "COUNT"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }

    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let mut count: i64 = 0;
        for arg in args {
            if let Ok(storage) = arg.range_storage() {
                for value_cow in storage.to_iterator() {
                    if coerce_num(value_cow.as_ref()).is_ok() {
                        count += 1;
                    }
                }
            } else {
                match arg.value()?.as_ref() {
                    LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
                    v => {
                        if coerce_num(v).is_ok() {
                            count += 1;
                        }
                    }
                }
            }
        }
        Ok(LiteralValue::Number(count as f64))
    }

    fn eval_fold(&self, f: &mut dyn FnFoldCtx) -> Option<Result<LiteralValue, ExcelError>> {
        let mut cnt: i64 = 0;
        let mut cb = |chunk: crate::stripes::NumericChunk| -> Result<(), ExcelError> {
            cnt += chunk.data.len() as i64;
            Ok(())
        };
        if let Err(e) = f.for_each_numeric_chunk(4096, &mut cb) {
            return Some(Ok(LiteralValue::Error(e)));
        }
        let out = LiteralValue::Number(cnt as f64);
        f.write_result(out.clone());
        Some(Ok(out))
    }
}

/* ─────────────────────────── AVERAGE() ──────────────────────────── */

#[derive(Debug)]
pub struct AverageFn;

impl Function for AverageFn {
    func_caps!(PURE, REDUCTION, NUMERIC_ONLY, STREAM_OK);

    fn name(&self) -> &'static str {
        "AVERAGE"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }

    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let mut sum = 0.0f64;
        let mut cnt: i64 = 0;
        for arg in args {
            if let Ok(storage) = arg.range_storage() {
                for value_cow in storage.to_iterator() {
                    if let Ok(n) = coerce_num(value_cow.as_ref()) {
                        sum += n;
                        cnt += 1;
                    }
                }
            } else {
                match arg.value()?.as_ref() {
                    LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
                    v => {
                        if let Ok(n) = coerce_num(v) {
                            sum += n;
                            cnt += 1;
                        }
                    }
                }
            }
        }
        if cnt == 0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        Ok(LiteralValue::Number(sum / (cnt as f64)))
    }

    fn eval_fold(&self, f: &mut dyn FnFoldCtx) -> Option<Result<LiteralValue, ExcelError>> {
        let mut sum = 0.0f64;
        let mut cnt: i64 = 0;
        let mut cb = |chunk: crate::stripes::NumericChunk| -> Result<(), ExcelError> {
            for &n in chunk.data {
                sum += n;
                cnt += 1;
            }
            Ok(())
        };
        if let Err(e) = f.for_each_numeric_chunk(4096, &mut cb) {
            return Some(Ok(LiteralValue::Error(e)));
        }
        if cnt == 0 {
            let e = ExcelError::from_error_string("#DIV/0!");
            f.write_result(LiteralValue::Error(e.clone()));
            return Some(Ok(LiteralValue::Error(e)));
        }
        let out = LiteralValue::Number(sum / (cnt as f64));
        f.write_result(out.clone());
        Some(Ok(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_core::LiteralValue;

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn test_sum_caps() {
        let sum_fn = SumFn;
        let caps = sum_fn.caps();

        // Check that the expected capabilities are set
        assert!(caps.contains(crate::function::FnCaps::PURE));
        assert!(caps.contains(crate::function::FnCaps::REDUCTION));
        assert!(caps.contains(crate::function::FnCaps::NUMERIC_ONLY));
        assert!(caps.contains(crate::function::FnCaps::STREAM_OK));

        // Check that other caps are not set
        assert!(!caps.contains(crate::function::FnCaps::VOLATILE));
        assert!(!caps.contains(crate::function::FnCaps::ELEMENTWISE));
    }

    #[test]
    fn test_sum_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumFn));
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
        let result = sum_fn.dispatch(&args, ctx.context).unwrap();
        assert_eq!(result, LiteralValue::Number(6.0));
    }
}

#[cfg(test)]
mod tests_count {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    use formualizer_core::parser::ASTNode;
    use formualizer_core::parser::ASTNodeType;

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn count_numbers_ignores_text() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountFn));
        let ctx = interp(&wb);
        // COUNT({1,2,"x",3}) => 3
        let arr = LiteralValue::Array(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(2),
            LiteralValue::Text("x".into()),
            LiteralValue::Int(3),
        ]]);
        let node = ASTNode::new(ASTNodeType::Literal(arr), None);
        let args = vec![ArgumentHandle::new(&node, &ctx)];
        let f = ctx.context.get_function("", "COUNT").unwrap();
        assert_eq!(
            f.dispatch(&args, ctx.context).unwrap(),
            LiteralValue::Number(3.0)
        );
    }

    #[test]
    fn count_multiple_args_and_scalars() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountFn));
        let ctx = interp(&wb);
        let n1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(10)), None);
        let n2 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("n".into())), None);
        let arr = LiteralValue::Array(vec![vec![LiteralValue::Int(1), LiteralValue::Int(2)]]);
        let a = ASTNode::new(ASTNodeType::Literal(arr), None);
        let args = vec![
            ArgumentHandle::new(&a, &ctx),
            ArgumentHandle::new(&n1, &ctx),
            ArgumentHandle::new(&n2, &ctx),
        ];
        let f = ctx.context.get_function("", "COUNT").unwrap();
        // Two from array + scalar 10 = 3
        assert_eq!(
            f.dispatch(&args, ctx.context).unwrap(),
            LiteralValue::Number(3.0)
        );
    }

    #[test]
    fn count_direct_error_argument_propagates() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountFn));
        let ctx = interp(&wb);
        let err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            ))),
            None,
        );
        let args = vec![ArgumentHandle::new(&err, &ctx)];
        let f = ctx.context.get_function("", "COUNT").unwrap();
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[cfg(test)]
mod tests_average {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    use formualizer_core::parser::ASTNode;
    use formualizer_core::parser::ASTNodeType;

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn average_basic_numbers() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageFn));
        let ctx = interp(&wb);
        let arr = LiteralValue::Array(vec![vec![
            LiteralValue::Int(2),
            LiteralValue::Int(4),
            LiteralValue::Int(6),
        ]]);
        let node = ASTNode::new(ASTNodeType::Literal(arr), None);
        let args = vec![ArgumentHandle::new(&node, &ctx)];
        let f = ctx.context.get_function("", "AVERAGE").unwrap();
        assert_eq!(
            f.dispatch(&args, ctx.context).unwrap(),
            LiteralValue::Number(4.0)
        );
    }

    #[test]
    fn average_mixed_with_text() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageFn));
        let ctx = interp(&wb);
        let arr = LiteralValue::Array(vec![vec![
            LiteralValue::Int(2),
            LiteralValue::Text("x".into()),
            LiteralValue::Int(6),
        ]]);
        let node = ASTNode::new(ASTNodeType::Literal(arr), None);
        let args = vec![ArgumentHandle::new(&node, &ctx)];
        let f = ctx.context.get_function("", "AVERAGE").unwrap();
        // average of 2 and 6 = 4
        assert_eq!(
            f.dispatch(&args, ctx.context).unwrap(),
            LiteralValue::Number(4.0)
        );
    }

    #[test]
    fn average_no_numeric_div0() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageFn));
        let ctx = interp(&wb);
        let arr = LiteralValue::Array(vec![vec![
            LiteralValue::Text("a".into()),
            LiteralValue::Text("b".into()),
        ]]);
        let node = ASTNode::new(ASTNodeType::Literal(arr), None);
        let args = vec![ArgumentHandle::new(&node, &ctx)];
        let f = ctx.context.get_function("", "AVERAGE").unwrap();
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("expected #DIV/0!, got {v:?}"),
        }
    }

    #[test]
    fn average_direct_error_argument_propagates() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageFn));
        let ctx = interp(&wb);
        let err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            ))),
            None,
        );
        let args = vec![ArgumentHandle::new(&err, &ctx)];
        let f = ctx.context.get_function("", "AVERAGE").unwrap();
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("unexpected {v:?}"),
        }
    }
}

pub fn register_builtins() {
    crate::function_registry::register_function(std::sync::Arc::new(SumFn));
    crate::function_registry::register_function(std::sync::Arc::new(CountFn));
    crate::function_registry::register_function(std::sync::Arc::new(AverageFn));
    // --- Trigonometry: circular ---
    crate::function_registry::register_function(std::sync::Arc::new(SinFn));
    crate::function_registry::register_function(std::sync::Arc::new(CosFn));
    crate::function_registry::register_function(std::sync::Arc::new(TanFn));
    crate::function_registry::register_function(std::sync::Arc::new(AsinFn));
    crate::function_registry::register_function(std::sync::Arc::new(AcosFn));
    crate::function_registry::register_function(std::sync::Arc::new(AtanFn));
    crate::function_registry::register_function(std::sync::Arc::new(Atan2Fn));
    crate::function_registry::register_function(std::sync::Arc::new(SecFn));
    crate::function_registry::register_function(std::sync::Arc::new(CscFn));
    crate::function_registry::register_function(std::sync::Arc::new(CotFn));
    crate::function_registry::register_function(std::sync::Arc::new(AcotFn));

    // --- Trigonometry: hyperbolic ---
    crate::function_registry::register_function(std::sync::Arc::new(SinhFn));
    crate::function_registry::register_function(std::sync::Arc::new(CoshFn));
    crate::function_registry::register_function(std::sync::Arc::new(TanhFn));
    crate::function_registry::register_function(std::sync::Arc::new(AsinhFn));
    crate::function_registry::register_function(std::sync::Arc::new(AcoshFn));
    crate::function_registry::register_function(std::sync::Arc::new(AtanhFn));
    crate::function_registry::register_function(std::sync::Arc::new(SechFn));
    crate::function_registry::register_function(std::sync::Arc::new(CschFn));
    crate::function_registry::register_function(std::sync::Arc::new(CothFn));

    // --- Angle conversion and constants ---
    crate::function_registry::register_function(std::sync::Arc::new(RadiansFn));
    crate::function_registry::register_function(std::sync::Arc::new(DegreesFn));
    crate::function_registry::register_function(std::sync::Arc::new(PiFn));
}

/* ─────────────────────────── TRIG: circular ────────────────────────── */

#[derive(Debug)]
pub struct SinFn;
impl Function for SinFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "SIN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.sin()))
    }
}

#[cfg(test)]
mod tests_sin {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "{a} !~= {b}");
    }

    #[test]
    fn test_sin_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SinFn));
        let ctx = interp(&wb);
        let sin = ctx.context.get_function("", "SIN").unwrap();
        let a0 = make_num_ast(PI / 2.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match sin.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 1.0),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct CosFn;
impl Function for CosFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "COS"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.cos()))
    }
}

#[cfg(test)]
mod tests_cos {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_cos_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CosFn));
        let ctx = interp(&wb);
        let cos = ctx.context.get_function("", "COS").unwrap();
        let a0 = make_num_ast(0.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match cos.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 1.0),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct TanFn;
impl Function for TanFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "TAN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.tan()))
    }
}

#[cfg(test)]
mod tests_tan {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_tan_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TanFn));
        let ctx = interp(&wb);
        let tan = ctx.context.get_function("", "TAN").unwrap();
        let a0 = make_num_ast(PI / 4.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match tan.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 1.0),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct AsinFn;
impl Function for AsinFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "ASIN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        if x < -1.0 || x > 1.0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string("#NUM!")));
        }
        Ok(LiteralValue::Number(x.asin()))
    }
}

#[cfg(test)]
mod tests_asin {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_asin_basic_and_domain() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AsinFn));
        let ctx = interp(&wb);
        let asin = ctx.context.get_function("", "ASIN").unwrap();
        // valid
        let a0 = make_num_ast(0.5);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match asin.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (0.5f64).asin()),
            v => panic!("unexpected {v:?}"),
        }
        // invalid domain
        let a1 = make_num_ast(2.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match asin.dispatch(&args2, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#NUM!"),
            v => panic!("expected error, got {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct AcosFn;
impl Function for AcosFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "ACOS"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        if x < -1.0 || x > 1.0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string("#NUM!")));
        }
        Ok(LiteralValue::Number(x.acos()))
    }
}

#[cfg(test)]
mod tests_acos {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_acos_basic_and_domain() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AcosFn));
        let ctx = interp(&wb);
        let acos = ctx.context.get_function("", "ACOS").unwrap();
        let a0 = make_num_ast(0.5);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match acos.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (0.5f64).acos()),
            v => panic!("unexpected {v:?}"),
        }
        let a1 = make_num_ast(-2.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match acos.dispatch(&args2, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#NUM!"),
            v => panic!("expected error, got {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct AtanFn;
impl Function for AtanFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "ATAN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.atan()))
    }
}

#[cfg(test)]
mod tests_atan {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_atan_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AtanFn));
        let ctx = interp(&wb);
        let atan = ctx.context.get_function("", "ATAN").unwrap();
        let a0 = make_num_ast(1.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match atan.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (1.0f64).atan()),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct Atan2Fn;
impl Function for Atan2Fn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "ATAN2"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_TWO[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let (x, y) = binary_numeric_args(args)?; // Excel: ATAN2(x_num, y_num)
        if x == 0.0 && y == 0.0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        Ok(LiteralValue::Number(y.atan2(x)))
    }
}

#[cfg(test)]
mod tests_atan2 {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_atan2_basic_and_zero_zero() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(Atan2Fn));
        let ctx = interp(&wb);
        let atan2 = ctx.context.get_function("", "ATAN2").unwrap();
        // ATAN2(1,1) = pi/4
        let a0 = make_num_ast(1.0);
        let a1 = make_num_ast(1.0);
        let args = vec![
            ArgumentHandle::new(&a0, &ctx),
            ArgumentHandle::new(&a1, &ctx),
        ];
        match atan2.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, PI / 4.0),
            v => panic!("unexpected {v:?}"),
        }
        // ATAN2(0,0) => #DIV/0!
        let b0 = make_num_ast(0.0);
        let b1 = make_num_ast(0.0);
        let args2 = vec![
            ArgumentHandle::new(&b0, &ctx),
            ArgumentHandle::new(&b1, &ctx),
        ];
        match atan2.dispatch(&args2, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("expected error, got {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct SecFn;
impl Function for SecFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "SEC"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        let c = x.cos();
        if c.abs() < EPSILON_NEAR_ZERO {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        Ok(LiteralValue::Number(1.0 / c))
    }
}

#[cfg(test)]
mod tests_sec {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_sec_basic_and_div0() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SecFn));
        let ctx = interp(&wb);
        let sec = ctx.context.get_function("", "SEC").unwrap();
        let a0 = make_num_ast(0.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match sec.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 1.0),
            v => panic!("unexpected {v:?}"),
        }
        let a1 = make_num_ast(PI / 2.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match sec.dispatch(&args2, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            LiteralValue::Number(n) => assert!(n.abs() > 1e12), // near singularity
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct CscFn;
impl Function for CscFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "CSC"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        let s = x.sin();
        if s.abs() < EPSILON_NEAR_ZERO {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        Ok(LiteralValue::Number(1.0 / s))
    }
}

#[cfg(test)]
mod tests_csc {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_csc_basic_and_div0() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CscFn));
        let ctx = interp(&wb);
        let csc = ctx.context.get_function("", "CSC").unwrap();
        let a0 = make_num_ast(PI / 2.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match csc.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 1.0),
            v => panic!("unexpected {v:?}"),
        }
        let a1 = make_num_ast(0.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match csc.dispatch(&args2, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("expected error, got {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct CotFn;
impl Function for CotFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "COT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        let t = x.tan();
        if t.abs() < EPSILON_NEAR_ZERO {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        Ok(LiteralValue::Number(1.0 / t))
    }
}

#[cfg(test)]
mod tests_cot {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_cot_basic_and_div0() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CotFn));
        let ctx = interp(&wb);
        let cot = ctx.context.get_function("", "COT").unwrap();
        let a0 = make_num_ast(PI / 4.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match cot.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 1.0),
            v => panic!("unexpected {v:?}"),
        }
        let a1 = make_num_ast(0.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match cot.dispatch(&args2, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("expected error, got {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct AcotFn;
impl Function for AcotFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "ACOT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        let result = if x == 0.0 {
            PI / 2.0
        } else if x > 0.0 {
            (1.0 / x).atan()
        } else {
            (1.0 / x).atan() + PI
        };
        Ok(LiteralValue::Number(result))
    }
}

#[cfg(test)]
mod tests_acot {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_acot_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AcotFn));
        let ctx = interp(&wb);
        let acot = ctx.context.get_function("", "ACOT").unwrap();
        let a0 = make_num_ast(2.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match acot.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 0.4636476090008061),
            v => panic!("unexpected {v:?}"),
        }
    }
}

/* ─────────────────────────── TRIG: hyperbolic ──────────────────────── */

#[derive(Debug)]
pub struct SinhFn;
impl Function for SinhFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "SINH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.sinh()))
    }
}

#[cfg(test)]
mod tests_sinh {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_sinh_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SinhFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "SINH").unwrap();
        let a0 = make_num_ast(1.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (1.0f64).sinh()),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct CoshFn;
impl Function for CoshFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "COSH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.cosh()))
    }
}

#[cfg(test)]
mod tests_cosh {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_cosh_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CoshFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "COSH").unwrap();
        let a0 = make_num_ast(1.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (1.0f64).cosh()),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct TanhFn;
impl Function for TanhFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "TANH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.tanh()))
    }
}

#[cfg(test)]
mod tests_tanh {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_tanh_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TanhFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "TANH").unwrap();
        let a0 = make_num_ast(0.5);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (0.5f64).tanh()),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct AsinhFn;
impl Function for AsinhFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "ASINH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_NUM_LENIENT_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.asinh()))
    }
}

#[cfg(test)]
mod tests_asinh {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_asinh_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AsinhFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ASINH").unwrap();
        let a0 = make_num_ast(1.5);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (1.5f64).asinh()),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct AcoshFn;
impl Function for AcoshFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "ACOSH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        if x < 1.0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string("#NUM!")));
        }
        Ok(LiteralValue::Number(x.acosh()))
    }
}

#[cfg(test)]
mod tests_acosh {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    #[test]
    fn test_acosh_basic_and_domain() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AcoshFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ACOSH").unwrap();
        let a0 = make_num_ast(1.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        assert_eq!(
            f.dispatch(&args, ctx.context).unwrap(),
            LiteralValue::Number(0.0)
        );
        let a1 = make_num_ast(0.5);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match f.dispatch(&args2, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#NUM!"),
            v => panic!("expected error, got {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct AtanhFn;
impl Function for AtanhFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "ATANH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        if x <= -1.0 || x >= 1.0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string("#NUM!")));
        }
        Ok(LiteralValue::Number(x.atanh()))
    }
}

#[cfg(test)]
mod tests_atanh {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_atanh_basic_and_domain() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AtanhFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ATANH").unwrap();
        let a0 = make_num_ast(0.5);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (0.5f64).atanh()),
            v => panic!("unexpected {v:?}"),
        }
        let a1 = make_num_ast(1.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match f.dispatch(&args2, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#NUM!"),
            v => panic!("expected error, got {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct SechFn;
impl Function for SechFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "SECH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(1.0 / x.cosh()))
    }
}

#[cfg(test)]
mod tests_sech {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_sech_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SechFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "SECH").unwrap();
        let a0 = make_num_ast(0.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 1.0),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct CschFn;
impl Function for CschFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "CSCH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        let s = x.sinh();
        if s == 0.0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        Ok(LiteralValue::Number(1.0 / s))
    }
}

#[cfg(test)]
mod tests_csch {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    #[test]
    fn test_csch_div0() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CschFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "CSCH").unwrap();
        let a0 = make_num_ast(0.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("expected error, got {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct CothFn;
impl Function for CothFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "COTH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        let s = x.sinh();
        if s.abs() < EPSILON_NEAR_ZERO {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        Ok(LiteralValue::Number(x.cosh() / s))
    }
}

#[cfg(test)]
mod tests_coth {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    #[test]
    fn test_coth_div0() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CothFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "COTH").unwrap();
        let a0 = make_num_ast(0.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("expected error, got {v:?}"),
        }
    }
}

/* ───────────────────── Angle conversion & constant ─────────────────── */

#[derive(Debug)]
pub struct RadiansFn;
impl Function for RadiansFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "RADIANS"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let deg = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(deg * PI / 180.0))
    }
}

#[cfg(test)]
mod tests_radians {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_radians_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RadiansFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "RADIANS").unwrap();
        let a0 = make_num_ast(180.0);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, PI),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct DegreesFn;
impl Function for DegreesFn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "DEGREES"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        let rad = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(rad * 180.0 / PI))
    }
}

#[cfg(test)]
mod tests_degrees {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn make_num_ast(n: f64) -> formualizer_core::parser::ASTNode {
        formualizer_core::parser::ASTNode::new(
            formualizer_core::parser::ASTNodeType::Literal(LiteralValue::Number(n)),
            None,
        )
    }
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9);
    }
    #[test]
    fn test_degrees_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(DegreesFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "DEGREES").unwrap();
        let a0 = make_num_ast(PI);
        let args = vec![ArgumentHandle::new(&a0, &ctx)];
        match f.dispatch(&args, ctx.context).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 180.0),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct PiFn;
impl Function for PiFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PI"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn eval_scalar<'a, 'b>(
        &self,
        _args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        Ok(LiteralValue::Number(PI))
    }
}

#[cfg(test)]
mod tests_pi {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_core::LiteralValue;
    #[test]
    fn test_pi_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(PiFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "PI").unwrap();
        assert_eq!(
            f.eval_scalar(&[], ctx.context).unwrap(),
            LiteralValue::Number(PI)
        );
    }
}
