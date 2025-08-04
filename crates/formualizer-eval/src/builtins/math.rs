use crate::function::{FnFoldCtx, Function};
use crate::traits::{ArgumentHandle, EvaluationContext};
use formualizer_common::{ArgKind, ArgSpec, ExcelError, LiteralValue};
use formualizer_macros::func_caps;

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
    fn arg_schema(&self) -> &'static [ArgSpec] {
        static SCHEMA: &[ArgSpec] = &[ArgSpec::new(ArgKind::Any)];
        SCHEMA
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
        let mut acc = 0.0;

        // Use numeric_stripes for efficient iteration
        for stripe in f.numeric_stripes() {
            for v in stripe.head {
                acc += match v {
                    LiteralValue::Number(n) => *n,
                    LiteralValue::Int(i) => *i as f64,
                    LiteralValue::Boolean(b) => {
                        if *b {
                            1.0
                        } else {
                            0.0
                        }
                    }
                    LiteralValue::Empty => 0.0,
                    LiteralValue::Error(e) => return Some(Ok(LiteralValue::Error(e.clone()))),
                    _ => 0.0, // Text and other non-numeric values are ignored like Excel
                };
            }
        }

        f.write_result(LiteralValue::Number(acc));
        Some(Ok(LiteralValue::Number(acc)))
    }
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

pub fn register_builtins() {
    crate::function_registry::register_function(std::sync::Arc::new(SumFn));
}
