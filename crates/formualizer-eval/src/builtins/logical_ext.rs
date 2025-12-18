use super::utils::ARG_ANY_ONE;
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/* Additional logical & error-handling functions: NOT, XOR, IFERROR, IFNA, IFS */

#[derive(Debug)]
pub struct NotFn;
impl Function for NotFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NOT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        let b = match v {
            LiteralValue::Boolean(b) => !b,
            LiteralValue::Number(n) => n == 0.0,
            LiteralValue::Int(i) => i == 0,
            LiteralValue::Empty => true,
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(b)))
    }
}

#[derive(Debug)]
pub struct XorFn;
impl Function for XorFn {
    func_caps!(PURE, REDUCTION, BOOL_ONLY);
    fn name(&self) -> &'static str {
        "XOR"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut true_count = 0usize;
        let mut first_error: Option<LiteralValue> = None;
        for a in args {
            if let Ok(view) = a.range_view() {
                let mut err: Option<LiteralValue> = None;
                view.for_each_cell(&mut |val| {
                    match val {
                        LiteralValue::Boolean(b) => {
                            if *b {
                                true_count += 1;
                            }
                        }
                        LiteralValue::Number(n) => {
                            if *n != 0.0 {
                                true_count += 1;
                            }
                        }
                        LiteralValue::Int(i) => {
                            if *i != 0 {
                                true_count += 1;
                            }
                        }
                        LiteralValue::Empty => {}
                        LiteralValue::Error(_) => {
                            if first_error.is_none() {
                                err = Some(val.clone());
                            }
                        }
                        _ => {
                            if first_error.is_none() {
                                err = Some(LiteralValue::Error(ExcelError::from_error_string(
                                    "#VALUE!",
                                )));
                            }
                        }
                    }
                    Ok(())
                })?;
                if first_error.is_none() {
                    first_error = err;
                }
            } else {
                let v = a.value()?.into_literal();
                match v {
                    LiteralValue::Boolean(b) => {
                        if b {
                            true_count += 1;
                        }
                    }
                    LiteralValue::Number(n) => {
                        if n != 0.0 {
                            true_count += 1;
                        }
                    }
                    LiteralValue::Int(i) => {
                        if i != 0 {
                            true_count += 1;
                        }
                    }
                    LiteralValue::Empty => {}
                    LiteralValue::Error(e) => {
                        if first_error.is_none() {
                            first_error = Some(LiteralValue::Error(e));
                        }
                    }
                    _ => {
                        if first_error.is_none() {
                            first_error = Some(LiteralValue::Error(ExcelError::from_error_string(
                                "#VALUE!",
                            )));
                        }
                    }
                }
            }
        }
        if let Some(err) = first_error {
            return Ok(crate::traits::CalcValue::Scalar(err));
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            true_count % 2 == 1,
        )))
    }
}

#[derive(Debug)]
pub struct IfErrorFn; // IFERROR(value, fallback)
impl Function for IfErrorFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IFERROR"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        false
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        // value, fallback (any scalar)
        static TWO: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::any(), ArgSchema::any()]);
        &TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        match v {
            LiteralValue::Error(_) => args[1].value(),
            other => Ok(crate::traits::CalcValue::Scalar(other)),
        }
    }
}

#[derive(Debug)]
pub struct IfNaFn; // IFNA(value, fallback)
impl Function for IfNaFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IFNA"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        false
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static TWO: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::any(), ArgSchema::any()]);
        &TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let v = args[0].value()?.into_literal();
        match v {
            LiteralValue::Error(ref e) if e.kind == formualizer_common::ExcelErrorKind::Na => {
                args[1].value()
            }
            other => Ok(crate::traits::CalcValue::Scalar(other)),
        }
    }
}

#[derive(Debug)]
pub struct IfsFn; // IFS(cond1, val1, cond2, val2, ...)
impl Function for IfsFn {
    func_caps!(PURE, SHORT_CIRCUIT);
    fn name(&self) -> &'static str {
        "IFS"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 || !args.len().is_multiple_of(2) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        for pair in args.chunks(2) {
            let cond = pair[0].value()?.into_literal();
            let is_true = match cond {
                LiteralValue::Boolean(b) => b,
                LiteralValue::Number(n) => n != 0.0,
                LiteralValue::Int(i) => i != 0,
                LiteralValue::Empty => false,
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::from_error_string("#VALUE!"),
                    )));
                }
            };
            if is_true {
                return pair[1].value();
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_na(),
        )))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(NotFn));
    crate::function_registry::register_function(Arc::new(XorFn));
    crate::function_registry::register_function(Arc::new(IfErrorFn));
    crate::function_registry::register_function(Arc::new(IfNaFn));
    crate::function_registry::register_function(Arc::new(IfsFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn not_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NotFn));
        let ctx = interp(&wb);
        let t = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(true)), None);
        let args = vec![ArgumentHandle::new(&t, &ctx)];
        let f = ctx.context.get_function("", "NOT").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn xor_range_and_scalars() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(XorFn));
        let ctx = interp(&wb);
        let arr = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Int(1),
                LiteralValue::Int(0),
                LiteralValue::Int(2),
            ]])),
            None,
        );
        let zero = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(0)), None);
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&zero, &ctx),
        ];
        let f = ctx.context.get_function("", "XOR").unwrap();
        // 1,true,true -> 2 trues => even => FALSE
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn iferror_fallback() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IfErrorFn));
        let ctx = interp(&wb);
        let err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            ))),
            None,
        );
        let fb = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None);
        let args = vec![
            ArgumentHandle::new(&err, &ctx),
            ArgumentHandle::new(&fb, &ctx),
        ];
        let f = ctx.context.get_function("", "IFERROR").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(5)
        );
    }

    #[test]
    fn iferror_passthrough_non_error() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IfErrorFn));
        let ctx = interp(&wb);
        let val = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(11)), None);
        let fb = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None);
        let args = vec![
            ArgumentHandle::new(&val, &ctx),
            ArgumentHandle::new(&fb, &ctx),
        ];
        let f = ctx.context.get_function("", "IFERROR").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(11)
        );
    }

    #[test]
    fn ifna_only_handles_na() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IfNaFn));
        let ctx = interp(&wb);
        let na = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::new_na())),
            None,
        );
        let other_err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::new_value())),
            None,
        );
        let fb = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(7)), None);
        let args_na = vec![
            ArgumentHandle::new(&na, &ctx),
            ArgumentHandle::new(&fb, &ctx),
        ];
        let args_val = vec![
            ArgumentHandle::new(&other_err, &ctx),
            ArgumentHandle::new(&fb, &ctx),
        ];
        let f = ctx.context.get_function("", "IFNA").unwrap();
        assert_eq!(
            f.dispatch(&args_na, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(7)
        );
        match f
            .dispatch(&args_val, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#VALUE!"),
            _ => panic!(),
        }
    }

    #[test]
    fn ifna_value_passthrough() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IfNaFn));
        let ctx = interp(&wb);
        let val = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(22)), None);
        let fb = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(9)), None);
        let args = vec![
            ArgumentHandle::new(&val, &ctx),
            ArgumentHandle::new(&fb, &ctx),
        ];
        let f = ctx.context.get_function("", "IFNA").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(22)
        );
    }

    #[test]
    fn ifs_short_circuits() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IfsFn));
        let ctx = interp(&wb);
        let cond_true = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(true)), None);
        let val1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(9)), None);
        let cond_false = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(false)), None);
        let val2 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let args = vec![
            ArgumentHandle::new(&cond_true, &ctx),
            ArgumentHandle::new(&val1, &ctx),
            ArgumentHandle::new(&cond_false, &ctx),
            ArgumentHandle::new(&val2, &ctx),
        ];
        let f = ctx.context.get_function("", "IFS").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(9)
        );
    }

    #[test]
    fn ifs_no_match_returns_na_error() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IfsFn));
        let ctx = interp(&wb);
        let cond_false1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(false)), None);
        let val1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(9)), None);
        let cond_false2 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(false)), None);
        let val2 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let args = vec![
            ArgumentHandle::new(&cond_false1, &ctx),
            ArgumentHandle::new(&val1, &ctx),
            ArgumentHandle::new(&cond_false2, &ctx),
            ArgumentHandle::new(&val2, &ctx),
        ];
        let f = ctx.context.get_function("", "IFS").unwrap();
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#N/A"),
            other => panic!("expected #N/A got {other:?}"),
        }
    }

    #[test]
    fn not_number_zero_and_nonzero() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(NotFn));
        let ctx = interp(&wb);
        let zero = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(0)), None);
        let one = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let f = ctx.context.get_function("", "NOT").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&zero, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&one, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn xor_error_propagation() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(XorFn));
        let ctx = interp(&wb);
        let err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::new_value())),
            None,
        );
        let one = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let f = ctx.context.get_function("", "XOR").unwrap();
        match f
            .dispatch(
                &[
                    ArgumentHandle::new(&err, &ctx),
                    ArgumentHandle::new(&one, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#VALUE!"),
            _ => panic!("expected value error"),
        }
    }
}
