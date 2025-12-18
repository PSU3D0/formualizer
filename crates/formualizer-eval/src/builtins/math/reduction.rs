use super::super::utils::{ARG_RANGE_NUM_LENIENT_ONE, coerce_num};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use arrow_array::Array;
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

#[derive(Debug)]
pub struct MinFn; // MIN(...)
impl Function for MinFn {
    func_caps!(PURE, REDUCTION, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "MIN"
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut mv: Option<f64> = None;
        for a in args {
            if let Ok(view) = a.range_view() {
                // Propagate errors from range first
                for res in view.errors_slices() {
                    let (_, _, err_cols) = res?;
                    for col in err_cols {
                        if col.null_count() < col.len() {
                            for i in 0..col.len() {
                                if !col.is_null(i) {
                                    return Ok(crate::traits::CalcValue::Scalar(
                                        LiteralValue::Error(ExcelError::new(
                                            crate::arrow_store::unmap_error_code(col.value(i)),
                                        )),
                                    ));
                                }
                            }
                        }
                    }
                }

                for res in view.numbers_slices() {
                    let (_, _, num_cols) = res?;
                    for col in num_cols {
                        if let Some(n) = arrow::compute::kernels::aggregate::min(col.as_ref()) {
                            mv = Some(mv.map(|m| m.min(n)).unwrap_or(n));
                        }
                    }
                }
            } else {
                let v = a.value()?.into_literal();
                match v {
                    LiteralValue::Error(e) => {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                    }
                    other => {
                        if let Ok(n) = coerce_num(&other) {
                            mv = Some(mv.map(|m| m.min(n)).unwrap_or(n));
                        }
                    }
                }
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            mv.unwrap_or(0.0),
        )))
    }
}

#[derive(Debug)]
pub struct MaxFn; // MAX(...)
impl Function for MaxFn {
    func_caps!(PURE, REDUCTION, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "MAX"
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
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut mv: Option<f64> = None;
        for a in args {
            if let Ok(view) = a.range_view() {
                // Propagate errors from range first
                for res in view.errors_slices() {
                    let (_, _, err_cols) = res?;
                    for col in err_cols {
                        if col.null_count() < col.len() {
                            for i in 0..col.len() {
                                if !col.is_null(i) {
                                    return Ok(crate::traits::CalcValue::Scalar(
                                        LiteralValue::Error(ExcelError::new(
                                            crate::arrow_store::unmap_error_code(col.value(i)),
                                        )),
                                    ));
                                }
                            }
                        }
                    }
                }

                for res in view.numbers_slices() {
                    let (_, _, num_cols) = res?;
                    for col in num_cols {
                        if let Some(n) = arrow::compute::kernels::aggregate::max(col.as_ref()) {
                            mv = Some(mv.map(|m| m.max(n)).unwrap_or(n));
                        }
                    }
                }
            } else {
                let v = a.value()?.into_literal();
                match v {
                    LiteralValue::Error(e) => {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                    }
                    other => {
                        if let Ok(n) = coerce_num(&other) {
                            mv = Some(mv.map(|m| m.max(n)).unwrap_or(n));
                        }
                    }
                }
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            mv.unwrap_or(0.0),
        )))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(MinFn));
    crate::function_registry::register_function(Arc::new(MaxFn));
}

#[cfg(test)]
mod tests_min_max {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn min_basic_array_and_scalar() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MinFn));
        let ctx = interp(&wb);
        let arr = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(5),
                LiteralValue::Int(2),
                LiteralValue::Int(9),
            ]])),
            None,
        );
        let extra = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let f = ctx.context.get_function("", "MIN").unwrap();
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&arr, &ctx),
                    ArgumentHandle::new(&extra, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Number(1.0));
    }

    #[test]
    fn max_basic_with_text_ignored() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MaxFn));
        let ctx = interp(&wb);
        let arr = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(5),
                LiteralValue::Text("x".into()),
                LiteralValue::Int(9),
            ]])),
            None,
        );
        let f = ctx.context.get_function("", "MAX").unwrap();
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&arr, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Number(9.0));
    }

    #[test]
    fn min_error_propagates() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MinFn));
        let ctx = interp(&wb);
        let err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::new_na())),
            None,
        );
        let one = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let f = ctx.context.get_function("", "MIN").unwrap();
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&err, &ctx),
                    ArgumentHandle::new(&one, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        match out {
            LiteralValue::Error(e) => assert_eq!(e, "#N/A"),
            v => panic!("expected error got {v:?}"),
        }
    }

    #[test]
    fn max_error_propagates() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MaxFn));
        let ctx = interp(&wb);
        let err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            ))),
            None,
        );
        let one = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let f = ctx.context.get_function("", "MAX").unwrap();
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&one, &ctx),
                    ArgumentHandle::new(&err, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        match out {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            v => panic!("expected error got {v:?}"),
        }
    }
}
