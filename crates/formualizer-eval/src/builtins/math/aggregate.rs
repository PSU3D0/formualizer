use super::super::utils::{ARG_RANGE_NUM_LENIENT_ONE, coerce_num};
use crate::args::ArgSchema;
use crate::function::{FnFoldCtx, Function};
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/* ─────────────────────────── SUM() ──────────────────────────── */

#[derive(Debug)]
pub struct SumFn;

impl Function for SumFn {
    func_caps!(PURE, REDUCTION, NUMERIC_ONLY, STREAM_OK, PARALLEL_ARGS);

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
            // Try to get a range/view first. If that fails, fall back to a single value.
            if let Ok(view) = arg.range_view() {
                view.for_each_cell(&mut |v| {
                    match v {
                        LiteralValue::Error(e) => return Err(e.clone()),
                        _ => {
                            if let Ok(n) = coerce_num(v) {
                                total += n;
                            }
                        }
                    }
                    Ok(())
                })?;
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
            if let Ok(view) = arg.range_view() {
                view.for_each_cell(&mut |v| {
                    if !matches!(v, LiteralValue::Empty) && coerce_num(v).is_ok() {
                        count += 1;
                    }
                    Ok(())
                })?;
            } else {
                match arg.value()?.as_ref() {
                    LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
                    v => {
                        if !matches!(v, LiteralValue::Empty) && coerce_num(v).is_ok() {
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
            // Empty cells are excluded at packing time; all values here are numerics
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
            if let Ok(view) = arg.range_view() {
                view.for_each_cell(&mut |v| {
                    if let Ok(n) = coerce_num(v) {
                        sum += n;
                        cnt += 1;
                    }
                    Ok(())
                })?;
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
            let e = ExcelError::new_div();
            f.write_result(LiteralValue::Error(e.clone()));
            return Some(Ok(LiteralValue::Error(e)));
        }
        let out = LiteralValue::Number(sum / (cnt as f64));
        f.write_result(out.clone());
        Some(Ok(out))
    }
}

/* ──────────────────────── SUMPRODUCT() ───────────────────────── */

#[derive(Debug)]
pub struct SumProductFn;

impl Function for SumProductFn {
    // Pure reduction over arrays; uses broadcasting and lenient coercion
    func_caps!(PURE, REDUCTION);

    fn name(&self) -> &'static str {
        "SUMPRODUCT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        // Accept ranges or scalars; numeric lenient coercion
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }

    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        use crate::broadcast::{broadcast_shape, project_index};

        if args.is_empty() {
            return Ok(LiteralValue::Number(0.0));
        }

        // Helper: materialize an argument to a 2D array of LiteralValue
        let to_array = |ah: &ArgumentHandle| -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
            if let Ok(rv) = ah.range_view() {
                let mut rows: Vec<Vec<LiteralValue>> = Vec::new();
                rv.for_each_row(&mut |row| {
                    rows.push(row.to_vec());
                    Ok(())
                })?;
                Ok(rows)
            } else {
                let v = ah.value()?;
                Ok(match v.as_ref() {
                    LiteralValue::Array(arr) => arr.clone(),
                    other => vec![vec![other.clone()]],
                })
            }
        };

        // Collect arrays and shapes
        let mut arrays: Vec<Vec<Vec<LiteralValue>>> = Vec::with_capacity(args.len());
        let mut shapes: Vec<(usize, usize)> = Vec::with_capacity(args.len());
        for a in args.iter() {
            let arr = to_array(a)?;
            let shape = (arr.len(), arr.first().map(|r| r.len()).unwrap_or(0));
            arrays.push(arr);
            shapes.push(shape);
        }

        // Compute broadcast target shape across all args
        let target = match broadcast_shape(&shapes) {
            Ok(s) => s,
            Err(_) => {
                return Ok(LiteralValue::Error(ExcelError::new_value()));
            }
        };

        // Iterate target shape, multiply coerced values across args, sum total
        let mut total = 0.0f64;
        for r in 0..target.0 {
            for c in 0..target.1 {
                let mut prod = 1.0f64;
                for (arr, &shape) in arrays.iter().zip(shapes.iter()) {
                    let (rr, cc) = project_index((r, c), shape);
                    let lv = arr
                        .get(rr)
                        .and_then(|row| row.get(cc))
                        .cloned()
                        .unwrap_or(LiteralValue::Empty);
                    match lv {
                        LiteralValue::Error(e) => return Ok(LiteralValue::Error(e)),
                        _ => match super::super::utils::coerce_num(&lv) {
                            Ok(n) => {
                                prod *= n;
                            }
                            Err(_) => {
                                // Non-numeric -> treated as 0 in SUMPRODUCT
                                prod *= 0.0;
                            }
                        },
                    }
                }
                total += prod;
            }
        }
        Ok(LiteralValue::Number(total))
    }
}

#[cfg(test)]
mod tests_sumproduct {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_core::LiteralValue;
    use formualizer_core::parser::{ASTNode, ASTNodeType};

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    fn arr(vals: Vec<Vec<LiteralValue>>) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Array(vals)), None)
    }

    fn num(n: f64) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(n)), None)
    }

    #[test]
    fn sumproduct_basic_pairwise() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumProductFn));
        let ctx = interp(&wb);
        // {1,2,3} * {4,5,6} = 1*4 + 2*5 + 3*6 = 32
        let a = arr(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(2),
            LiteralValue::Int(3),
        ]]);
        let b = arr(vec![vec![
            LiteralValue::Int(4),
            LiteralValue::Int(5),
            LiteralValue::Int(6),
        ]]);
        let args = vec![ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)];
        let f = ctx.context.get_function("", "SUMPRODUCT").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(32.0)
        );
    }

    #[test]
    fn sumproduct_variadic_three_arrays() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumProductFn));
        let ctx = interp(&wb);
        // {1,2} * {3,4} * {2,2} = (1*3*2) + (2*4*2) = 6 + 16 = 22
        let a = arr(vec![vec![LiteralValue::Int(1), LiteralValue::Int(2)]]);
        let b = arr(vec![vec![LiteralValue::Int(3), LiteralValue::Int(4)]]);
        let c = arr(vec![vec![LiteralValue::Int(2), LiteralValue::Int(2)]]);
        let args = vec![
            ArgumentHandle::new(&a, &ctx),
            ArgumentHandle::new(&b, &ctx),
            ArgumentHandle::new(&c, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMPRODUCT").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(22.0)
        );
    }

    #[test]
    fn sumproduct_broadcast_scalar_over_array() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumProductFn));
        let ctx = interp(&wb);
        // {1,2,3} * 10 => (1*10 + 2*10 + 3*10) = 60
        let a = arr(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(2),
            LiteralValue::Int(3),
        ]]);
        let s = num(10.0);
        let args = vec![ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&s, &ctx)];
        let f = ctx.context.get_function("", "SUMPRODUCT").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(60.0)
        );
    }

    #[test]
    fn sumproduct_2d_arrays_broadcast_rows_cols() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumProductFn));
        let ctx = interp(&wb);
        // A is 2x2, B is 1x2 -> broadcast B across rows
        // A = [[1,2],[3,4]], B = [[10,20]]
        // sum = 1*10 + 2*20 + 3*10 + 4*20 = 10 + 40 + 30 + 80 = 160
        let a = arr(vec![
            vec![LiteralValue::Int(1), LiteralValue::Int(2)],
            vec![LiteralValue::Int(3), LiteralValue::Int(4)],
        ]);
        let b = arr(vec![vec![LiteralValue::Int(10), LiteralValue::Int(20)]]);
        let args = vec![ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)];
        let f = ctx.context.get_function("", "SUMPRODUCT").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(160.0)
        );
    }

    #[test]
    fn sumproduct_non_numeric_treated_as_zero() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumProductFn));
        let ctx = interp(&wb);
        // {1,"x",3} * {1,1,1} => 1*1 + 0*1 + 3*1 = 4
        let a = arr(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Text("x".into()),
            LiteralValue::Int(3),
        ]]);
        let b = arr(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(1),
            LiteralValue::Int(1),
        ]]);
        let args = vec![ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)];
        let f = ctx.context.get_function("", "SUMPRODUCT").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(4.0)
        );
    }

    #[test]
    fn sumproduct_error_in_input_propagates() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumProductFn));
        let ctx = interp(&wb);
        let a = arr(vec![vec![LiteralValue::Int(1), LiteralValue::Int(2)]]);
        let e = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::new_na())),
            None,
        );
        let args = vec![ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&e, &ctx)];
        let f = ctx.context.get_function("", "SUMPRODUCT").unwrap();
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Error(err) => assert_eq!(err, "#N/A"),
            v => panic!("expected error, got {v:?}"),
        }
    }

    #[test]
    fn sumproduct_incompatible_shapes_value_error() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumProductFn));
        let ctx = interp(&wb);
        // 1x3 and 1x2 -> #VALUE!
        let a = arr(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(2),
            LiteralValue::Int(3),
        ]]);
        let b = arr(vec![vec![LiteralValue::Int(4), LiteralValue::Int(5)]]);
        let args = vec![ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)];
        let f = ctx.context.get_function("", "SUMPRODUCT").unwrap();
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Error(e) => assert_eq!(e, "#VALUE!"),
            v => panic!("expected value error, got {v:?}"),
        }
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
    crate::function_registry::register_function(std::sync::Arc::new(SumProductFn));
    crate::function_registry::register_function(std::sync::Arc::new(SumFn));
    crate::function_registry::register_function(std::sync::Arc::new(CountFn));
    crate::function_registry::register_function(std::sync::Arc::new(AverageFn));
}
