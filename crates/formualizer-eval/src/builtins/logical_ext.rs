use super::utils::ARG_ANY_ONE;
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/* Additional logical & error-handling functions: NOT, XOR, IFERROR, IFNA, IFS */

#[derive(Debug)]
pub struct NotFn;
/// Reverses the logical value of its argument.
///
/// `NOT` converts the input to a logical value, then flips it.
///
/// # Remarks
/// - Numbers are coerced (`0` -> TRUE after inversion, non-zero -> FALSE after inversion).
/// - Blank values are treated as FALSE, so `NOT(blank)` returns TRUE.
/// - Text and other non-coercible values return `#VALUE!`.
/// - Errors are propagated unchanged.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Invert boolean"
/// formula: '=NOT(TRUE)'
/// expected: false
/// ```
///
/// ```yaml,sandbox
/// title: "Invert numeric truthiness"
/// formula: '=NOT(0)'
/// expected: true
/// ```
/// [formualizer-docgen:schema:start]
/// Name: NOT
/// Type: NotFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: NOT(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
/// Returns TRUE when an odd number of arguments evaluate to TRUE.
///
/// `XOR` aggregates all values and checks parity of truthy inputs.
///
/// # Remarks
/// - Booleans and numbers are accepted (`0` is FALSE, non-zero is TRUE).
/// - Blank values are ignored.
/// - Text and other non-coercible values produce `#VALUE!`.
/// - If no coercion error occurs first, encountered formula errors are propagated.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Odd count of TRUE values"
/// formula: '=XOR(TRUE, FALSE, TRUE, TRUE)'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Text input triggers VALUE error"
/// formula: '=XOR(1, "x")'
/// expected: "#VALUE!"
/// ```
/// [formualizer-docgen:schema:start]
/// Name: XOR
/// Type: XorFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: XOR(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, BOOL_ONLY
/// [formualizer-docgen:schema:end]
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
/// Returns a fallback when the first expression evaluates to any error.
///
/// `IFERROR(value, value_if_error)` is useful for user-friendly error handling.
///
/// # Remarks
/// - Any error kind in the first argument triggers the fallback branch.
/// - Non-error results pass through unchanged.
/// - Evaluation failures surfaced as interpreter errors are also caught.
/// - Exactly two arguments are required; other arities return `#VALUE!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Replace division error"
/// formula: '=IFERROR(1/0, "n/a")'
/// expected: "n/a"
/// ```
///
/// ```yaml,sandbox
/// title: "Pass through non-error"
/// formula: '=IFERROR(42, 0)'
/// expected: 42
/// ```
/// [formualizer-docgen:schema:start]
/// Name: IFERROR
/// Type: IfErrorFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: IFERROR(arg1: any@scalar, arg2: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
        match args[0].value() {
            Ok(cv) => match cv.into_literal() {
                LiteralValue::Error(_) => args[1].value(),
                other => Ok(crate::traits::CalcValue::Scalar(other)),
            },
            Err(_) => args[1].value(),
        }
    }
}

#[derive(Debug)]
pub struct IfNaFn; // IFNA(value, fallback)
/// Returns a fallback only when the first expression is `#N/A`.
///
/// `IFNA(value, value_if_na)` is narrower than `IFERROR`.
///
/// # Remarks
/// - Only `#N/A` triggers fallback.
/// - Other error kinds are returned unchanged.
/// - Non-error results pass through unchanged.
/// - Exactly two arguments are required; other arities return `#VALUE!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Catch N/A"
/// formula: '=IFNA(NA(), "missing")'
/// expected: "missing"
/// ```
///
/// ```yaml,sandbox
/// title: "Do not catch other errors"
/// formula: '=IFNA(1/0, "missing")'
/// expected: "#DIV/0!"
/// ```
/// [formualizer-docgen:schema:start]
/// Name: IFNA
/// Type: IfNaFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: IFNA(arg1: any@scalar, arg2: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
/// Returns the value for the first TRUE condition in condition-value pairs.
///
/// `IFS(cond1, value1, cond2, value2, ...)` evaluates left to right and short-circuits.
///
/// # Remarks
/// - Arguments must be provided as pairs; odd argument counts return `#VALUE!`.
/// - Conditions accept booleans and numbers (`0` FALSE, non-zero TRUE); blank is FALSE.
/// - Text conditions return `#VALUE!`; error conditions propagate.
/// - If no condition is TRUE, returns `#N/A`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "First matching condition wins"
/// formula: '=IFS(2<1, "a", 3>2, "b", TRUE, "c")'
/// expected: "b"
/// ```
///
/// ```yaml,sandbox
/// title: "No conditions matched"
/// formula: '=IFS(FALSE, 1, 0, 2)'
/// expected: "#N/A"
/// ```
/// [formualizer-docgen:schema:start]
/// Name: IFS
/// Type: IfsFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: IFS(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
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

    #[derive(Debug)]
    struct ThrowNameFn;

    impl Function for ThrowNameFn {
        func_caps!(PURE);

        fn name(&self) -> &'static str {
            "THROWNAME"
        }

        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
            Err(ExcelError::new_name())
        }
    }

    #[test]
    fn iferror_catches_evaluation_errors_returned_as_err() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(IfErrorFn))
            .with_function(std::sync::Arc::new(ThrowNameFn));
        let ctx = interp(&wb);

        let throw = ASTNode::new(
            ASTNodeType::Function {
                name: "THROWNAME".to_string(),
                args: vec![],
            },
            None,
        );
        let fallback = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(42)), None);

        let args = vec![
            ArgumentHandle::new(&throw, &ctx),
            ArgumentHandle::new(&fallback, &ctx),
        ];
        let f = ctx.context.get_function("", "IFERROR").unwrap();

        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(42)
        );
    }
}
