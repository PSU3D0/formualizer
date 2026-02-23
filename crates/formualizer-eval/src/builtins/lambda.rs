use crate::function::{FnCaps, Function};
use crate::interpreter::{LocalBinding, LocalEnv};
use crate::traits::{ArgumentHandle, CalcValue, CustomCallable, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
use std::collections::HashSet;
use std::sync::Arc;

fn value_error(msg: impl Into<String>) -> ExcelError {
    ExcelError::new(ExcelErrorKind::Value).with_message(msg.into())
}

fn local_name_from_ast(node: &ASTNode) -> Result<String, ExcelError> {
    match &node.node_type {
        ASTNodeType::Reference {
            reference: ReferenceType::NamedRange(name),
            ..
        } => Ok(name.clone()),
        _ => Err(value_error("Expected a local name identifier")),
    }
}

fn binding_from_calc_value(cv: CalcValue<'_>) -> LocalBinding {
    match cv {
        CalcValue::Scalar(v) => LocalBinding::Value(v),
        CalcValue::Range(rv) => {
            let (rows, cols) = rv.dims();
            if rows == 1 && cols == 1 {
                LocalBinding::Value(rv.get_cell(0, 0))
            } else {
                let mut data = Vec::with_capacity(rows);
                let _ = rv.for_each_row(&mut |row| {
                    data.push(row.to_vec());
                    Ok(())
                });
                LocalBinding::Value(LiteralValue::Array(data))
            }
        }
        CalcValue::Callable(c) => LocalBinding::Callable(c),
    }
}

#[derive(Debug)]
pub struct LetFn;

/// Binds local names to values and evaluates a final expression with those bindings.
///
/// `LET` introduces lexical variables using name/value pairs, then returns the last expression.
///
/// # Remarks
/// - Arguments must be provided as `name, value` pairs followed by one final calculation expression.
/// - Names are resolved as local identifiers and can shadow workbook-level names.
/// - Bindings are evaluated left-to-right, so later values can reference earlier bindings.
/// - Invalid names or malformed arity return `#VALUE!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Bind intermediate values"
/// formula: "=LET(rate,0.08,price,125,price*(1+rate))"
/// expected: 135
/// ```
///
/// ```yaml,sandbox
/// title: "Use LET with range calculations"
/// grid:
///   A1: 10
///   A2: 4
/// formula: "=LET(total,SUM(A1:A2),total*2)"
/// expected: 28
/// ```
///
/// ```yaml,sandbox
/// title: "Nested LET supports shadowing"
/// formula: "=LET(x,2,LET(x,5,x)+x)"
/// expected: 7
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: LET
/// Type: LetFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: LET(<schema unavailable>)
/// Arg schema: <unavailable: arg_schema panicked>
/// Caps: PURE, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
impl Function for LetFn {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE | FnCaps::SHORT_CIRCUIT
    }

    fn name(&self) -> &'static str {
        "LET"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn variadic(&self) -> bool {
        true
    }

    fn dispatch<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        self.eval(args, ctx)
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Ok(CalcValue::Scalar(LiteralValue::Error(value_error(
                "LET expects name/value pairs followed by a final expression",
            ))));
        }

        let mut env: LocalEnv = args[0].current_env();

        for pair_idx in (0..args.len() - 1).step_by(2) {
            let name = match local_name_from_ast(args[pair_idx].ast()) {
                Ok(name) => name,
                Err(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            };

            let bound = args[pair_idx + 1].value_with_env(env.clone())?;
            env = env.with_binding(&name, binding_from_calc_value(bound));
        }

        args[args.len() - 1].value_with_env(env)
    }
}

#[derive(Clone)]
struct LambdaClosure {
    params: Vec<String>,
    body: ASTNode,
    captured_env: LocalEnv,
}

impl CustomCallable for LambdaClosure {
    fn arity(&self) -> usize {
        self.params.len()
    }

    fn invoke<'ctx>(
        &self,
        interp: &crate::interpreter::Interpreter<'ctx>,
        args: &[LiteralValue],
    ) -> Result<CalcValue<'ctx>, ExcelError> {
        if args.len() != self.arity() {
            return Ok(CalcValue::Scalar(LiteralValue::Error(value_error(
                format!(
                    "LAMBDA expected {} argument(s), got {}",
                    self.arity(),
                    args.len()
                ),
            ))));
        }

        let mut env = self.captured_env.clone();
        for (name, value) in self.params.iter().zip(args.iter()) {
            env = env.with_binding(name, LocalBinding::Value(value.clone()));
        }

        let scoped = interp.with_local_env(env);
        scoped.evaluate_ast(&self.body)
    }
}

#[derive(Debug)]
pub struct LambdaFn;

/// Creates an anonymous callable that can be invoked with spreadsheet arguments.
///
/// `LAMBDA` captures its defining local scope and returns a reusable function value.
///
/// # Remarks
/// - All arguments except the last are parameter names; the last argument is the body expression.
/// - Parameter names must be unique (case-insensitive), or `#VALUE!` is returned.
/// - Invocation arity must exactly match the declared parameter count.
/// - Returning an uninvoked lambda as a final cell value yields a `#CALC!` in evaluation.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Inline lambda invocation"
/// formula: "=LAMBDA(x,x+1)(41)"
/// expected: 42
/// ```
///
/// ```yaml,sandbox
/// title: "Lambda captures outer LET bindings"
/// formula: "=LET(k,10,addk,LAMBDA(n,n+k),addk(5))"
/// expected: 15
/// ```
///
/// ```yaml,sandbox
/// title: "Duplicate parameter names are invalid"
/// formula: "=LAMBDA(x,x,x+1)"
/// expected: "#VALUE!"
/// ```
///
/// [formualizer-docgen:schema:start]
/// Name: LAMBDA
/// Type: LambdaFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: LAMBDA(<schema unavailable>)
/// Arg schema: <unavailable: arg_schema panicked>
/// Caps: PURE, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
impl Function for LambdaFn {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE | FnCaps::SHORT_CIRCUIT
    }

    fn name(&self) -> &'static str {
        "LAMBDA"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn variadic(&self) -> bool {
        true
    }

    fn dispatch<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        self.eval(args, ctx)
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        if args.is_empty() {
            return Ok(CalcValue::Scalar(LiteralValue::Error(value_error(
                "LAMBDA requires at least a calculation expression",
            ))));
        }

        let mut params = Vec::new();
        let mut seen = HashSet::new();
        for arg in &args[..args.len() - 1] {
            let name = match local_name_from_ast(arg.ast()) {
                Ok(name) => name,
                Err(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            };
            let key = name.to_ascii_uppercase();
            if !seen.insert(key) {
                return Ok(CalcValue::Scalar(LiteralValue::Error(value_error(
                    "LAMBDA parameter names must be unique",
                ))));
            }
            params.push(name);
        }

        let closure = LambdaClosure {
            params,
            body: args[args.len() - 1].ast().clone(),
            captured_env: args[0].current_env(),
        };

        Ok(CalcValue::Callable(Arc::new(closure)))
    }
}

pub fn register_builtins() {
    crate::function_registry::register_function(Arc::new(LetFn));
    crate::function_registry::register_function(Arc::new(LambdaFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::parse;

    fn test_wb() -> TestWorkbook {
        TestWorkbook::new()
            .with_function(Arc::new(LetFn))
            .with_function(Arc::new(LambdaFn))
    }

    fn eval(src: &str) -> LiteralValue {
        eval_result(src).expect("eval")
    }

    fn eval_result(src: &str) -> Result<LiteralValue, ExcelError> {
        eval_result_with_wb(src, test_wb())
    }

    fn eval_with_wb(src: &str, wb: TestWorkbook) -> LiteralValue {
        eval_result_with_wb(src, wb).expect("eval")
    }

    fn eval_result_with_wb(src: &str, wb: TestWorkbook) -> Result<LiteralValue, ExcelError> {
        let interp = wb.interpreter();
        let ast = parse(src).expect("parse");
        interp.evaluate_ast(&ast).map(|v| v.into_literal())
    }

    #[test]
    fn let_binds_values() {
        assert_eq!(eval("=LET(x,2,x+3)"), LiteralValue::Number(5.0));
    }

    #[test]
    fn let_nested_shadowing() {
        assert_eq!(eval("=LET(x,2,LET(x,5,x)+x)"), LiteralValue::Number(7.0));
    }

    #[test]
    fn lambda_can_be_bound_and_invoked() {
        assert_eq!(
            eval("=LET(inc,LAMBDA(n,n+1),inc(41))"),
            LiteralValue::Number(42.0)
        );
    }

    #[test]
    fn lambda_closure_captures_outer_bindings() {
        assert_eq!(
            eval("=LET(k,10,addk,LAMBDA(n,n+k),addk(5))"),
            LiteralValue::Number(15.0)
        );
    }

    #[test]
    fn lambda_arity_errors() {
        let v = eval("=LET(inc,LAMBDA(n,n+1),inc(1,2))");
        match v {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Value),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn lambda_value_requires_invocation() {
        let v = eval("=LAMBDA(x,x+1)");
        match v {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Calc),
            other => panic!("expected #CALC!, got {other:?}"),
        }
    }

    #[test]
    fn let_rejects_non_identifier_name() {
        let v = eval("=LET(A1,2,A1)");
        match v {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Value),
            other => panic!("expected #VALUE!, got {other:?}"),
        }
    }

    #[test]
    fn lambda_rejects_duplicate_params() {
        let v = eval("=LAMBDA(x,x,x+1)");
        match v {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Value),
            other => panic!("expected #VALUE!, got {other:?}"),
        }
    }

    #[test]
    fn let_and_lambda_names_are_case_insensitive() {
        assert_eq!(eval("=LET(x,1,X+1)"), LiteralValue::Number(2.0));
        assert_eq!(
            eval("=LET(F,LAMBDA(n,n+1),f(1))"),
            LiteralValue::Number(2.0)
        );
    }

    #[test]
    fn let_shadows_workbook_named_range() {
        let wb = test_wb().with_named_range("x", vec![vec![LiteralValue::Number(100.0)]]);
        assert_eq!(eval_with_wb("=LET(X,1,x+1)", wb), LiteralValue::Number(2.0));
    }

    #[test]
    fn lambda_param_shadows_outer_scope() {
        assert_eq!(
            eval("=LET(n,5,f,LAMBDA(n,n+1),f(10))"),
            LiteralValue::Number(11.0)
        );
    }

    #[test]
    fn lambda_closure_snapshot_semantics() {
        assert_eq!(
            eval("=LET(k,1,f,LAMBDA(x,x+k),k,2,f(0))"),
            LiteralValue::Number(1.0)
        );
    }

    #[test]
    fn let_undefined_symbol_before_binding_errors() {
        let err = eval_result("=LET(x,y,y,2,x)").expect_err("expected #NAME?");
        assert_eq!(err.kind, ExcelErrorKind::Name);
    }

    #[test]
    fn non_invoked_lambda_in_let_is_calc_error() {
        let v = eval("=LET(f,LAMBDA(x,x+1),f)");
        match v {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Calc),
            other => panic!("expected #CALC!, got {other:?}"),
        }
    }
}
