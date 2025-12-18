// crates/formualizer-eval/src/builtins/logical.rs

use super::utils::ARG_ANY_ONE;
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/* ─────────────────────────── TRUE() ─────────────────────────────── */

#[derive(Debug)]
pub struct TrueFn;

impl Function for TrueFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "TRUE"
    }
    fn min_args(&self) -> usize {
        0
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            true,
        )))
    }
}

/* ─────────────────────────── FALSE() ────────────────────────────── */

#[derive(Debug)]
pub struct FalseFn;

impl Function for FalseFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "FALSE"
    }
    fn min_args(&self) -> usize {
        0
    }

    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            false,
        )))
    }
}

/* ─────────────────────────── AND() ──────────────────────────────── */

#[derive(Debug)]
pub struct AndFn;

impl Function for AndFn {
    func_caps!(PURE, REDUCTION, BOOL_ONLY, SHORT_CIRCUIT);

    fn name(&self) -> &'static str {
        "AND"
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
        let mut first_error: Option<LiteralValue> = None;
        for h in args {
            let it = h.lazy_values_owned()?;
            for v in it {
                match v {
                    LiteralValue::Error(_) => {
                        if first_error.is_none() {
                            first_error = Some(v);
                        }
                    }
                    LiteralValue::Empty => {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                            false,
                        )));
                    }
                    LiteralValue::Boolean(b) => {
                        if !b {
                            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                                false,
                            )));
                        }
                    }
                    LiteralValue::Number(n) => {
                        if n == 0.0 {
                            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                                false,
                            )));
                        }
                    }
                    LiteralValue::Int(i) => {
                        if i == 0 {
                            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                                false,
                            )));
                        }
                    }
                    _ => {
                        // Non-coercible (e.g., Text) → #VALUE! candidate with message
                        if first_error.is_none() {
                            first_error =
                                Some(LiteralValue::Error(ExcelError::new_value().with_message(
                                    "AND expects logical/numeric inputs; text is not coercible",
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
            true,
        )))
    }
}

/* ─────────────────────────── OR() ───────────────────────────────── */

#[derive(Debug)]
pub struct OrFn;

impl Function for OrFn {
    func_caps!(PURE, REDUCTION, BOOL_ONLY, SHORT_CIRCUIT);

    fn name(&self) -> &'static str {
        "OR"
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
        let mut first_error: Option<LiteralValue> = None;
        for h in args {
            let it = h.lazy_values_owned()?;
            for v in it {
                match v {
                    LiteralValue::Error(_) => {
                        if first_error.is_none() {
                            first_error = Some(v);
                        }
                    }
                    LiteralValue::Empty => {
                        // ignored
                    }
                    LiteralValue::Boolean(b) => {
                        if b {
                            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                                true,
                            )));
                        }
                    }
                    LiteralValue::Number(n) => {
                        if n != 0.0 {
                            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                                true,
                            )));
                        }
                    }
                    LiteralValue::Int(i) => {
                        if i != 0 {
                            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                                true,
                            )));
                        }
                    }
                    _ => {
                        // Non-coercible → #VALUE! candidate with message
                        if first_error.is_none() {
                            first_error =
                                Some(LiteralValue::Error(ExcelError::new_value().with_message(
                                    "OR expects logical/numeric inputs; text is not coercible",
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
            false,
        )))
    }
}

/* ─────────────────────────── IF() ───────────────────────────────── */

#[derive(Debug)]
pub struct IfFn;

impl Function for IfFn {
    func_caps!(PURE, SHORT_CIRCUIT);

    fn name(&self) -> &'static str {
        "IF"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        // Single variadic any schema so we can enforce precise 2 or 3 arity inside eval()
        static ONE: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| vec![ArgSchema::any()]);
        &ONE[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 || args.len() > 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value()
                    .with_message(format!("IF expects 2 or 3 arguments, got {}", args.len())),
            )));
        }

        let condition = args[0].value()?.into_literal();
        let b = match condition {
            LiteralValue::Boolean(b) => b,
            LiteralValue::Number(n) => n != 0.0,
            LiteralValue::Int(i) => i != 0,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message("IF condition must be boolean or number"),
                )));
            }
        };

        if b {
            args[1].value()
        } else if let Some(arg) = args.get(2) {
            arg.value()
        } else {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                false,
            )))
        }
    }
}

pub fn register_builtins() {
    crate::function_registry::register_function(std::sync::Arc::new(TrueFn));
    crate::function_registry::register_function(std::sync::Arc::new(FalseFn));
    crate::function_registry::register_function(std::sync::Arc::new(AndFn));
    crate::function_registry::register_function(std::sync::Arc::new(OrFn));
    crate::function_registry::register_function(std::sync::Arc::new(IfFn));
}

/* ─────────────────────────── tests ─────────────────────────────── */

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::ArgumentHandle;
    use crate::{interpreter::Interpreter, test_workbook::TestWorkbook};
    use formualizer_parse::LiteralValue;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct CountFn(Arc<AtomicUsize>);
    impl Function for CountFn {
        func_caps!(PURE);
        fn name(&self) -> &'static str {
            "COUNTING"
        }
        fn min_args(&self) -> usize {
            0
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                true,
            )))
        }
    }

    #[derive(Debug)]
    struct ErrorFn(Arc<AtomicUsize>);
    impl Function for ErrorFn {
        func_caps!(PURE);
        fn name(&self) -> &'static str {
            "ERRORFN"
        }
        fn min_args(&self) -> usize {
            0
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )))
        }
    }

    fn interp(wb: &TestWorkbook) -> Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn test_true_false() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(TrueFn))
            .with_function(std::sync::Arc::new(FalseFn));

        let ctx = interp(&wb);
        let t = ctx.context.get_function("", "TRUE").unwrap();
        let fctx = ctx.function_context(None);
        assert_eq!(
            t.eval(&[], &fctx).unwrap().into_literal(),
            LiteralValue::Boolean(true)
        );

        let f = ctx.context.get_function("", "FALSE").unwrap();
        assert_eq!(
            f.eval(&[], &fctx).unwrap().into_literal(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn test_and_or() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(AndFn))
            .with_function(std::sync::Arc::new(OrFn));
        let ctx = interp(&wb);
        let fctx = ctx.function_context(None);

        let and = ctx.context.get_function("", "AND").unwrap();
        let or = ctx.context.get_function("", "OR").unwrap();
        // Build ArgumentHandles manually: TRUE, 1, FALSE
        let dummy_ast = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Boolean(true)),
            None,
        );
        let dummy_ast_false = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Boolean(false)),
            None,
        );
        let dummy_ast_one = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Int(1)),
            None,
        );
        let hs = vec![
            ArgumentHandle::new(&dummy_ast, &ctx),
            ArgumentHandle::new(&dummy_ast_one, &ctx),
        ];
        assert_eq!(
            and.eval(&hs, &fctx).unwrap().into_literal(),
            LiteralValue::Boolean(true)
        );

        let hs2 = vec![
            ArgumentHandle::new(&dummy_ast_false, &ctx),
            ArgumentHandle::new(&dummy_ast_one, &ctx),
        ];
        assert_eq!(
            and.eval(&hs2, &fctx).unwrap().into_literal(),
            LiteralValue::Boolean(false)
        );
        assert_eq!(
            or.eval(&hs2, &fctx).unwrap().into_literal(),
            LiteralValue::Boolean(true)
        );
    }

    #[test]
    fn and_short_circuits_on_false_without_evaluating_rest() {
        let counter = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new()
            .with_function(Arc::new(AndFn))
            .with_function(Arc::new(CountFn(counter.clone())));
        let ctx = interp(&wb);
        let fctx = ctx.function_context(None);
        let and = ctx.context.get_function("", "AND").unwrap();

        // Build args: FALSE, COUNTING()
        let a_false = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Boolean(false)),
            None,
        );
        let counting_call = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Function {
                name: "COUNTING".into(),
                args: vec![],
            },
            None,
        );
        let hs = vec![
            ArgumentHandle::new(&a_false, &ctx),
            ArgumentHandle::new(&counting_call, &ctx),
        ];
        let out = and.eval(&hs, &fctx).unwrap().into_literal();
        assert_eq!(out, LiteralValue::Boolean(false));
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "COUNTING should not be evaluated"
        );
    }

    #[test]
    fn or_short_circuits_on_true_without_evaluating_rest() {
        let counter = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new()
            .with_function(Arc::new(OrFn))
            .with_function(Arc::new(CountFn(counter.clone())));
        let ctx = interp(&wb);
        let fctx = ctx.function_context(None);
        let or = ctx.context.get_function("", "OR").unwrap();

        // Build args: TRUE, COUNTING()
        let a_true = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Boolean(true)),
            None,
        );
        let counting_call = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Function {
                name: "COUNTING".into(),
                args: vec![],
            },
            None,
        );
        let hs = vec![
            ArgumentHandle::new(&a_true, &ctx),
            ArgumentHandle::new(&counting_call, &ctx),
        ];
        let out = or.eval(&hs, &fctx).unwrap().into_literal();
        assert_eq!(out, LiteralValue::Boolean(true));
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "COUNTING should not be evaluated"
        );
    }

    #[test]
    fn or_range_arg_short_circuits_on_first_true_before_evaluating_next_arg() {
        let counter = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new()
            .with_function(Arc::new(OrFn))
            .with_function(Arc::new(CountFn(counter.clone())));
        let ctx = interp(&wb);
        let fctx = ctx.function_context(None);
        let or = ctx.context.get_function("", "OR").unwrap();

        // First arg is an array literal with first element 1 (truey), then zeros.
        let arr = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Array(vec![
                vec![formualizer_parse::parser::ASTNode::new(
                    formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Int(1)),
                    None,
                )],
                vec![formualizer_parse::parser::ASTNode::new(
                    formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Int(0)),
                    None,
                )],
            ]),
            None,
        );
        let counting_call = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Function {
                name: "COUNTING".into(),
                args: vec![],
            },
            None,
        );
        let hs = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&counting_call, &ctx),
        ];
        let out = or.eval(&hs, &fctx).unwrap().into_literal();
        assert_eq!(out, LiteralValue::Boolean(true));
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "COUNTING should not be evaluated"
        );
    }

    #[test]
    fn and_returns_first_error_when_no_decisive_false() {
        let err_counter = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new()
            .with_function(Arc::new(AndFn))
            .with_function(Arc::new(ErrorFn(err_counter.clone())));
        let ctx = interp(&wb);
        let fctx = ctx.function_context(None);
        let and = ctx.context.get_function("", "AND").unwrap();

        // AND(1, ERRORFN(), 1) => #VALUE!
        let one = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Int(1)),
            None,
        );
        let errcall = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Function {
                name: "ERRORFN".into(),
                args: vec![],
            },
            None,
        );
        let hs = vec![
            ArgumentHandle::new(&one, &ctx),
            ArgumentHandle::new(&errcall, &ctx),
            ArgumentHandle::new(&one, &ctx),
        ];
        let out = and.eval(&hs, &fctx).unwrap().into_literal();
        match out {
            LiteralValue::Error(e) => assert_eq!(e.to_string(), "#VALUE!"),
            _ => panic!("Expected error"),
        }
        assert_eq!(
            err_counter.load(Ordering::SeqCst),
            1,
            "ERRORFN should be evaluated once"
        );
    }

    #[test]
    fn or_does_not_evaluate_error_after_true() {
        let err_counter = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new()
            .with_function(Arc::new(OrFn))
            .with_function(Arc::new(ErrorFn(err_counter.clone())));
        let ctx = interp(&wb);
        let fctx = ctx.function_context(None);
        let or = ctx.context.get_function("", "OR").unwrap();

        // OR(TRUE, ERRORFN()) => TRUE and ERRORFN not evaluated
        let a_true = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Boolean(true)),
            None,
        );
        let errcall = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Function {
                name: "ERRORFN".into(),
                args: vec![],
            },
            None,
        );
        let hs = vec![
            ArgumentHandle::new(&a_true, &ctx),
            ArgumentHandle::new(&errcall, &ctx),
        ];
        let out = or.eval(&hs, &fctx).unwrap().into_literal();
        assert_eq!(out, LiteralValue::Boolean(true));
        assert_eq!(
            err_counter.load(Ordering::SeqCst),
            0,
            "ERRORFN should not be evaluated"
        );
    }
}
