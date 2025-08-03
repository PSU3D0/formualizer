use crate::traits::{ArgumentHandle, EvaluationContext, Function};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::excel_fn;
use rayon::prelude::*;
use std::sync::atomic::AtomicBool;

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
    for arg in args {
        // Try to get a range/stream first. If that fails, fall back to a single value.
        if let Ok(storage) = arg.range_storage() {
            for value_cow in storage.into_iter() {
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

fn coerce_num(v: &LiteralValue) -> Result<f64, ExcelError> {
    match v {
        LiteralValue::Number(n) => Ok(*n),
        LiteralValue::Int(i) => Ok(*i as f64),
        LiteralValue::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        _ => Err(ExcelError::from_error_string("#VALUE!")
            .with_message(format!("Cannot convert {v:?} to number"))),
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

/* ─────────────────────── PARALLEL_SUM() Example ──────────────────────── */

/// Example parallel SUM function demonstrating try_parallel_eval hook
#[derive(Debug)]
pub struct ParallelSumFn;

impl Function for ParallelSumFn {
    fn name(&self) -> &'static str {
        "PARALLEL_SUM"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn variadic(&self) -> bool {
        true
    }

    fn eval<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn EvaluationContext,
    ) -> Result<LiteralValue, ExcelError> {
        // Fallback sequential implementation
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

    fn try_parallel_eval<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn EvaluationContext,
        cancel_flag: &AtomicBool,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        use std::sync::atomic::Ordering;

        // Only use parallel evaluation if we have a thread pool
        let thread_pool = ctx.thread_pool()?;

        // Check cancellation before starting
        if cancel_flag.load(Ordering::Relaxed) {
            return Some(Err(ExcelError::new(
                formualizer_common::ExcelErrorKind::Cancelled,
            )
            .with_message("Parallel SUM cancelled before execution".to_string())));
        }

        // Use the thread pool for parallel evaluation
        let result =
            thread_pool.install(|| {
                args.par_iter()
                    .map(|h| -> Result<f64, ExcelError> {
                        // Check cancellation periodically
                        if cancel_flag.load(Ordering::Relaxed) {
                            return Err(ExcelError::new(
                                formualizer_common::ExcelErrorKind::Cancelled,
                            )
                            .with_message("Parallel SUM cancelled during execution".to_string()));
                        }

                        match h.value()?.as_ref() {
                            LiteralValue::Error(e) => Err(e.clone()),
                            LiteralValue::Array(arr) => {
                                let mut subtotal = 0.0;
                                for row in arr {
                                    for v in row {
                                        subtotal += coerce_num(v)?;
                                    }
                                }
                                Ok(subtotal)
                            }
                            v => coerce_num(v),
                        }
                    })
                    .try_reduce(|| 0.0, |a, b| Ok(a + b))
            });

        Some(result.map(LiteralValue::Number))
    }
}

pub fn register_builtins() {
    crate::function_registry::register(std::sync::Arc::new(__FnSUM));
    crate::function_registry::register(std::sync::Arc::new(ParallelSumFn));
}
