use super::utils::{
    ARG_ANY_ONE, ARG_NUM_LENIENT_ONE, ARG_NUM_LENIENT_TWO, ARG_RANGE_NUM_LENIENT_ONE,
    EPSILON_NEAR_ZERO, binary_numeric_args, coerce_num, unary_numeric_arg,
};
use crate::args::ArgSchema;
use crate::function::{FnFoldCtx, Function};
use crate::traits::{ArgumentHandle, FunctionContext};
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
        _ctx: &dyn FunctionContext,
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
        _ctx: &dyn FunctionContext,
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
        _ctx: &dyn FunctionContext,
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
        let fctx = crate::traits::DefaultFunctionContext::new(ctx.context, None);

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
        let result = sum_fn.dispatch(&args, &fctx).unwrap();
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
        let fctx = crate::traits::DefaultFunctionContext::new(ctx.context, None);
        assert_eq!(f.dispatch(&args, &fctx).unwrap(), LiteralValue::Number(3.0));
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
        let fctx = crate::traits::DefaultFunctionContext::new(ctx.context, None);
        assert_eq!(f.dispatch(&args, &fctx).unwrap(), LiteralValue::Number(3.0));
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
        let fctx = crate::traits::DefaultFunctionContext::new(ctx.context, None);
        match f.dispatch(&args, &fctx).unwrap() {
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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
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
        let fctx = crate::traits::DefaultFunctionContext::new(ctx.context, None);
        match f.dispatch(&args, &fctx).unwrap() {
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
        let fctx = crate::traits::DefaultFunctionContext::new(ctx.context, None);
        match f.dispatch(&args, &fctx).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("unexpected {v:?}"),
        }
    }
}

pub fn register_builtins() {
    crate::function_registry::register_function(std::sync::Arc::new(SumFn));
    crate::function_registry::register_function(std::sync::Arc::new(CountFn));
    crate::function_registry::register_function(std::sync::Arc::new(AverageFn));
    crate::function_registry::register_function(std::sync::Arc::new(AverageIfsFn));
    // --- Trigonometry: circular ---
    crate::function_registry::register_function(std::sync::Arc::new(SinFn));
    crate::function_registry::register_function(std::sync::Arc::new(CosFn));
    crate::function_registry::register_function(std::sync::Arc::new(TanFn));
    // A few elementwise numeric funcs are wired for map path; extend as needed
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

/* ─────────────────────────── AVERAGEIFS() ──────────────────────────── */

#[derive(Debug)]
pub struct AverageIfsFn;

impl Function for AverageIfsFn {
    func_caps!(PURE, WINDOWED, STREAM_OK);

    fn name(&self) -> &'static str {
        "AVERAGEIFS"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        // [avg_range, criteria_range1, criteria1, criteria_range2, criteria2, ...]
        // Use Criteria coercion for criteria values; ranges are accepted as Range/Array shape.
        // For simplicity, accept Any and do parsing inside.
        &super::utils::ARG_ANY_ONE[..]
    }

    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        // Fallback scalar path: expand everything and compute like Excel
        // Build avg iterator and paired criteria.
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }

        // Extract avg_range dims/iterator
        let (mut avg_iter, dims): (
            Box<dyn Iterator<Item = LiteralValue>>,
            Option<(usize, usize)>,
        ) = if let Ok(storage) = args[0].range_storage() {
            let d = storage.dims();
            (
                Box::new(storage.to_iterator().map(|c| c.into_owned())),
                Some(d),
            )
        } else {
            let v = args[0].value()?.into_owned();
            (
                Box::new(std::iter::once(v)) as Box<dyn Iterator<Item = LiteralValue>>,
                None,
            )
        };

        // Prepare criteria pairs as iterators or broadcast scalars
        let mut crit_iters: Vec<Box<dyn Iterator<Item = LiteralValue>>> = Vec::new();
        let mut preds: Vec<crate::args::CriteriaPredicate> = Vec::new();
        for i in (1..args.len()).step_by(2) {
            // range
            if let Ok(storage) = args[i].range_storage() {
                let d = storage.dims();
                if let Some(d0) = dims {
                    if d0 != d {
                        return Ok(LiteralValue::Error(ExcelError::from_error_string(
                            "#VALUE!",
                        )));
                    }
                }
                crit_iters.push(Box::new(storage.to_iterator().map(|c| c.into_owned())));
            } else {
                let v = args[i].value()?.into_owned();
                let count = dims.map(|(r, c)| r * c).unwrap_or(1);
                crit_iters.push(Box::new(std::iter::repeat(v).take(count)));
            }
            // criteria
            let cval = args[i + 1].value()?.into_owned();
            let pred = crate::args::parse_criteria(&cval)?;
            preds.push(pred);
        }

        let mut sum = 0.0f64;
        let mut cnt = 0i64;
        let mut idx = 0usize;
        loop {
            let a = match avg_iter.next() {
                Some(v) => v,
                None => break,
            };
            let mut ok = true;
            for (j, it) in crit_iters.iter_mut().enumerate() {
                let v = it.next().unwrap_or(LiteralValue::Empty);
                if !criteria_match(&preds[j], &v) {
                    ok = false;
                    break;
                }
            }
            if ok {
                if let Ok(n) = super::utils::coerce_num(&a) {
                    sum += n;
                    cnt += 1;
                }
            }
            idx += 1;
        }
        if cnt == 0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        Ok(LiteralValue::Number(sum / cnt as f64))
    }

    fn eval_window<'a, 'b>(
        &self,
        w: &mut crate::window_ctx::SimpleWindowCtx<'a, 'b>,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        // Use width=1, step=1 windows: iterate aligned positions across avg_range and each criteria_range
        let mut sum = 0.0f64;
        let mut cnt = 0i64;
        // Build predicates from criteria arguments
        if w.args.len() < 3 || (w.args.len() - 1) % 2 != 0 {
            return Some(Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            ))));
        }
        let mut preds: Vec<crate::args::CriteriaPredicate> = Vec::new();
        for i in (1..w.args.len()).step_by(2) {
            let cval = match w.args[i + 1].value() {
                Ok(v) => v.into_owned(),
                Err(e) => return Some(Ok(LiteralValue::Error(e))),
            };
            match crate::args::parse_criteria(&cval) {
                Ok(p) => preds.push(p),
                Err(e) => return Some(Ok(LiteralValue::Error(e))),
            }
        }
        let res = w.for_each_window(|cells| {
            // cells[0] = avg value; cells[1],cells[3],.. = criteria_ranges; criteria at even indices after that
            let avg_v = &cells[0];
            let mut ok = true;
            let mut pj = 0usize;
            for j in (1..cells.len()).step_by(2) {
                if !criteria_match(&preds[pj], &cells[j]) {
                    ok = false;
                    break;
                }
                pj += 1;
            }
            if ok {
                if let Ok(n) = super::utils::coerce_num(avg_v) {
                    sum += n;
                    cnt += 1;
                }
            }
            Ok(())
        });
        if let Err(e) = res {
            return Some(Ok(LiteralValue::Error(e)));
        }
        if cnt == 0 {
            return Some(Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            ))));
        }
        Some(Ok(LiteralValue::Number(sum / cnt as f64)))
    }
}

fn criteria_match(pred: &crate::args::CriteriaPredicate, v: &LiteralValue) -> bool {
    use crate::args::CriteriaPredicate as P;
    match pred {
        P::Eq(t) => values_equal_invariant(t, v),
        P::Ne(t) => !values_equal_invariant(t, v),
        P::Gt(n) => value_to_number(v).map(|x| x > *n).unwrap_or(false),
        P::Ge(n) => value_to_number(v).map(|x| x >= *n).unwrap_or(false),
        P::Lt(n) => value_to_number(v).map(|x| x < *n).unwrap_or(false),
        P::Le(n) => value_to_number(v).map(|x| x <= *n).unwrap_or(false),
        P::TextLike {
            pattern,
            case_insensitive,
        } => text_like_match(pattern, *case_insensitive, v),
        P::IsBlank => matches!(v, LiteralValue::Empty),
        P::IsNumber => value_to_number(v).is_ok(),
        P::IsText => matches!(v, LiteralValue::Text(_)),
        P::IsLogical => matches!(v, LiteralValue::Boolean(_)),
    }
}

fn value_to_number(v: &LiteralValue) -> Result<f64, ExcelError> {
    crate::coercion::to_number_lenient(v)
}
fn values_equal_invariant(a: &LiteralValue, b: &LiteralValue) -> bool {
    match (a, b) {
        (LiteralValue::Number(x), LiteralValue::Number(y)) => (x - y).abs() < 1e-12,
        (LiteralValue::Int(x), LiteralValue::Int(y)) => x == y,
        (LiteralValue::Boolean(x), LiteralValue::Boolean(y)) => x == y,
        (LiteralValue::Text(x), LiteralValue::Text(y)) => x.eq_ignore_ascii_case(y),
        (LiteralValue::Empty, LiteralValue::Empty) => true,
        (LiteralValue::Number(x), _) => value_to_number(b)
            .map(|y| (x - y).abs() < 1e-12)
            .unwrap_or(false),
        (_, LiteralValue::Number(_)) => values_equal_invariant(b, a),
        _ => false,
    }
}

fn text_like_match(pattern: &str, case_insensitive: bool, v: &LiteralValue) -> bool {
    let s = match v {
        LiteralValue::Text(t) => t.clone(),
        LiteralValue::Number(n) => n.to_string(),
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Boolean(b) => {
            if *b {
                "TRUE".into()
            } else {
                "FALSE".into()
            }
        }
        LiteralValue::Empty => String::new(),
        _ => return false,
    };
    let (pat, text) = if case_insensitive {
        (pattern.to_ascii_lowercase(), s.to_ascii_lowercase())
    } else {
        (pattern.to_string(), s)
    };
    wildcard_match(&pat, &text)
}

fn wildcard_match(pat: &str, text: &str) -> bool {
    // Simple glob-like matcher for * and ?
    fn helper(p: &[u8], t: &[u8]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        match p[0] {
            b'*' => {
                // match zero or more
                for i in 0..=t.len() {
                    if helper(&p[1..], &t[i..]) {
                        return true;
                    }
                }
                false
            }
            b'?' => {
                if t.is_empty() {
                    false
                } else {
                    helper(&p[1..], &t[1..])
                }
            }
            ch => {
                if t.first().copied() == Some(ch) {
                    helper(&p[1..], &t[1..])
                } else {
                    false
                }
            }
        }
    }
    helper(pat.as_bytes(), text.as_bytes())
}

#[cfg(test)]
mod tests_averageifs {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::parser::{ASTNode, ASTNodeType};

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn averageifs_basic_numeric() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        // avg_range {1,2,3,4}, criteria_range {0,1,0,1}, criteria ">0"
        let avg_arr = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(1),
                LiteralValue::Int(2),
                LiteralValue::Int(3),
                LiteralValue::Int(4),
            ]])),
            None,
        );
        let crit_rng = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(0),
                LiteralValue::Int(1),
                LiteralValue::Int(0),
                LiteralValue::Int(1),
            ]])),
            None,
        );
        let crit = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text(">0".into())), None);
        let args = vec![
            ArgumentHandle::new(&avg_arr, &ctx),
            ArgumentHandle::new(&crit_rng, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        let out = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert_eq!(out, LiteralValue::Number((2.0 + 4.0) / 2.0));
    }

    #[test]
    fn averageifs_div0_when_no_match() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        let avg_arr = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(1),
                LiteralValue::Int(2),
            ]])),
            None,
        );
        let crit_rng = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(0),
                LiteralValue::Int(0),
            ]])),
            None,
        );
        let crit = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text(">0".into())), None);
        let args = vec![
            ArgumentHandle::new(&avg_arr, &ctx),
            ArgumentHandle::new(&crit_rng, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("expected div0, got {v:?}"),
        }
    }

    #[test]
    fn averageifs_mismatched_shapes_value_error() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        // avg 1x4, crit range 1x3 -> #VALUE!
        let avg_arr = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(1),
                LiteralValue::Int(2),
                LiteralValue::Int(3),
                LiteralValue::Int(4),
            ]])),
            None,
        );
        let crit_rng = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(0),
                LiteralValue::Int(1),
                LiteralValue::Int(0),
            ]])),
            None,
        );
        let crit = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text(">0".into())), None);
        let args = vec![
            ArgumentHandle::new(&avg_arr, &ctx),
            ArgumentHandle::new(&crit_rng, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#VALUE!"),
            v => panic!("expected value error, got {v:?}"),
        }
    }

    #[test]
    fn averageifs_text_like_and_equality() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        // avg {10,20,30} with crit texts {"alpha","beta","alphabet"} and pattern "al*"
        let avg = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(10),
                LiteralValue::Int(20),
                LiteralValue::Int(30),
            ]])),
            None,
        );
        let crit_rng = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Text("alpha".into()),
                LiteralValue::Text("beta".into()),
                LiteralValue::Text("alphabet".into()),
            ]])),
            None,
        );
        let pat = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("al*".into())), None);
        let args = vec![
            ArgumentHandle::new(&avg, &ctx),
            ArgumentHandle::new(&crit_rng, &ctx),
            ArgumentHandle::new(&pat, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        // matches alpha and alphabet => (10+30)/2 = 20
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(20.0)
        );
    }

    #[test]
    fn averageifs_scalar_criteria_broadcasts() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        let avg = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(1),
                LiteralValue::Int(2),
                LiteralValue::Int(3),
            ]])),
            None,
        );
        let crit_rng = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(1),
                LiteralValue::Int(1),
                LiteralValue::Int(1),
            ]])),
            None,
        );
        let crit = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("=1".into())), None);
        let args = vec![
            ArgumentHandle::new(&avg, &ctx),
            ArgumentHandle::new(&crit_rng, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        // all match => average of 1,2,3 = 2
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(2.0)
        );
    }

    #[test]
    fn averageifs_eval_window_parity_with_dispatch() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        let avg = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(2),
                LiteralValue::Int(4),
                LiteralValue::Int(6),
                LiteralValue::Int(8),
            ]])),
            None,
        );
        let crit_rng = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(0),
                LiteralValue::Int(1),
                LiteralValue::Int(0),
                LiteralValue::Int(1),
            ]])),
            None,
        );
        let crit = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text(">0".into())), None);
        let args = vec![
            ArgumentHandle::new(&avg, &ctx),
            ArgumentHandle::new(&crit_rng, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        let disp = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        // Call eval_window directly
        let fctx = ctx.function_context(None);
        let mut wctx = crate::window_ctx::SimpleWindowCtx::new(
            &args,
            &fctx,
            crate::window_ctx::WindowSpec::default(),
        );
        let win = f.eval_window(&mut wctx).unwrap().unwrap();
        assert_eq!(disp, win);
    }

    #[test]
    fn averageifs_multiple_criteria_pairs() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        // avg [10,20,30,40], city [Bellevue,Issaquah,Bellevue,Issaquah], beds [2,3,4,5]
        let avg = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(10),
                LiteralValue::Int(20),
                LiteralValue::Int(30),
                LiteralValue::Int(40),
            ]])),
            None,
        );
        let city = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Text("Bellevue".into()),
                LiteralValue::Text("Issaquah".into()),
                LiteralValue::Text("Bellevue".into()),
                LiteralValue::Text("Issaquah".into()),
            ]])),
            None,
        );
        let beds = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(2),
                LiteralValue::Int(3),
                LiteralValue::Int(4),
                LiteralValue::Int(5),
            ]])),
            None,
        );
        let c_city = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Text("Bellevue".into())),
            None,
        );
        let c_beds = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text(">=4".into())), None);
        let args = vec![
            ArgumentHandle::new(&avg, &ctx),
            ArgumentHandle::new(&city, &ctx),
            ArgumentHandle::new(&c_city, &ctx),
            ArgumentHandle::new(&beds, &ctx),
            ArgumentHandle::new(&c_beds, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        // Matches entries 3: avg 30 → 30
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(30.0)
        );
    }

    #[test]
    fn averageifs_error_in_criteria_range_does_not_crash_and_never_matches() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        let avg = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(1),
                LiteralValue::Int(2),
            ]])),
            None,
        );
        let crit_rng = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Error(ExcelError::from_error_string("#N/A")),
                LiteralValue::Int(1),
            ]])),
            None,
        );
        let crit = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("=1".into())), None);
        let args = vec![
            ArgumentHandle::new(&avg, &ctx),
            ArgumentHandle::new(&crit_rng, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        // Only second matches -> average 2
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(2.0)
        );
    }

    #[test]
    fn averageifs_case_insensitive_text_matching() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        let avg = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(5),
                LiteralValue::Int(10),
            ]])),
            None,
        );
        let crit_rng = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Text("ALPHA".into()),
                LiteralValue::Text("alpha".into()),
            ]])),
            None,
        );
        let crit = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Text("alpha".into())),
            None,
        );
        let args = vec![
            ArgumentHandle::new(&avg, &ctx),
            ArgumentHandle::new(&crit_rng, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        // both match case-insensitively -> average 7.5
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(7.5)
        );
    }
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
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.sin()))
    }

    fn eval_map(
        &self,
        m: &mut dyn crate::function::FnMapCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        if !m.is_array_context() {
            return None;
        }
        let mut closure = |n: f64| Ok(LiteralValue::Number(n.sin()));
        if let Err(e) = m.map_unary_numeric(&mut closure) {
            return Some(Ok(LiteralValue::Error(e)));
        }
        Some(Ok(m.finalize()))
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
        match sin.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.cos()))
    }

    fn eval_map(
        &self,
        m: &mut dyn crate::function::FnMapCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        if !m.is_array_context() {
            return None;
        }
        let mut closure = |n: f64| Ok(LiteralValue::Number(n.cos()));
        if let Err(e) = m.map_unary_numeric(&mut closure) {
            return Some(Ok(LiteralValue::Error(e)));
        }
        Some(Ok(m.finalize()))
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
        match cos.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        Ok(LiteralValue::Number(x.tan()))
    }

    fn eval_map(
        &self,
        m: &mut dyn crate::function::FnMapCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        if !m.is_array_context() {
            return None;
        }
        let mut closure = |n: f64| Ok(LiteralValue::Number(n.tan()));
        if let Err(e) = m.map_unary_numeric(&mut closure) {
            return Some(Ok(LiteralValue::Error(e)));
        }
        Some(Ok(m.finalize()))
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
        match tan.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        if !(-1.0..=1.0).contains(&x) {
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
        match asin.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (0.5f64).asin()),
            v => panic!("unexpected {v:?}"),
        }
        // invalid domain
        let a1 = make_num_ast(2.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match asin.dispatch(&args2, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let x = unary_numeric_arg(args)?;
        if !(-1.0..=1.0).contains(&x) {
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
        match acos.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (0.5f64).acos()),
            v => panic!("unexpected {v:?}"),
        }
        let a1 = make_num_ast(-2.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match acos.dispatch(&args2, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match atan.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (1.0f64).atan()),
            v => panic!("unexpected {v:?}"),
        }
    }
}

#[derive(Debug)]
pub struct Atan2Fn;
impl Function for Atan2Fn {
    func_caps!(PURE, ELEMENTWISE, NUMERIC_ONLY);
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
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let (x, y) = binary_numeric_args(args)?; // Excel: ATAN2(x_num, y_num)
        if x == 0.0 && y == 0.0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        Ok(LiteralValue::Number(y.atan2(x)))
    }

    fn eval_map(
        &self,
        m: &mut dyn crate::function::FnMapCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        if !m.is_array_context() {
            return None;
        }
        let mut closure = |x: f64, y: f64| {
            if x == 0.0 && y == 0.0 {
                Ok(LiteralValue::Error(ExcelError::from_error_string(
                    "#DIV/0!",
                )))
            } else {
                Ok(LiteralValue::Number(y.atan2(x)))
            }
        };
        if let Err(e) = m.map_binary_numeric(&mut closure) {
            return Some(Ok(LiteralValue::Error(e)));
        }
        Some(Ok(m.finalize()))
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
        match atan2.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        match atan2.dispatch(&args2, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match sec.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 1.0),
            v => panic!("unexpected {v:?}"),
        }
        let a1 = make_num_ast(PI / 2.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match sec.dispatch(&args2, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match csc.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 1.0),
            v => panic!("unexpected {v:?}"),
        }
        let a1 = make_num_ast(0.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match csc.dispatch(&args2, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match cot.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Number(n) => assert_close(n, 1.0),
            v => panic!("unexpected {v:?}"),
        }
        let a1 = make_num_ast(0.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match cot.dispatch(&args2, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match acot.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        let fctx = crate::traits::DefaultFunctionContext::new(ctx.context, None);
        match f.dispatch(&args, &fctx).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(0.0)
        );
        let a1 = make_num_ast(0.5);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match f.dispatch(&args2, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Number(n) => assert_close(n, (0.5f64).atanh()),
            v => panic!("unexpected {v:?}"),
        }
        let a1 = make_num_ast(1.0);
        let args2 = vec![ArgumentHandle::new(&a1, &ctx)];
        match f.dispatch(&args2, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
        _ctx: &dyn FunctionContext,
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
            f.eval_scalar(&[], &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(PI)
        );
    }
}
