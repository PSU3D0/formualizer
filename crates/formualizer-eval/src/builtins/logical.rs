// crates/formualizer-eval/src/builtins/logical.rs

use super::utils::ARG_ANY_ONE;
use crate::args::ArgSchema;
use crate::function::{Function, FunctionResolution, resolution_to_reference};
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

/* ─────────────────────────── TRUE() ─────────────────────────────── */

#[derive(Debug)]
pub struct TrueFn;
/// Returns the logical constant TRUE.
///
/// Use `TRUE()` when you want an explicit boolean value in formulas.
///
/// # Remarks
/// - `TRUE` takes no arguments and always returns the boolean value `TRUE`.
/// - No coercion or evaluation side effects are involved.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Return TRUE directly"
/// formula: '=TRUE()'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Use TRUE in branching"
/// formula: '=IF(TRUE(), "yes", "no")'
/// expected: "yes"
/// ```
///
/// ```yaml,docs
/// related:
///   - FALSE
///   - IF
///   - AND
/// faq:
///   - q: "Can TRUE accept arguments?"
///     a: "No. TRUE takes zero arguments and always returns the boolean constant TRUE."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: TRUE
/// Type: TrueFn
/// Min args: 0
/// Max args: 0
/// Variadic: false
/// Signature: TRUE()
/// Arg schema: []
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
/// Returns the logical constant FALSE.
///
/// Use `FALSE()` when you want an explicit boolean false value in formulas.
///
/// # Remarks
/// - `FALSE` takes no arguments and always returns the boolean value `FALSE`.
/// - No coercion or evaluation side effects are involved.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Return FALSE directly"
/// formula: '=FALSE()'
/// expected: false
/// ```
///
/// ```yaml,sandbox
/// title: "Use FALSE in branching"
/// formula: '=IF(FALSE(), "yes", "no")'
/// expected: "no"
/// ```
///
/// ```yaml,docs
/// related:
///   - TRUE
///   - IF
///   - OR
/// faq:
///   - q: "Can FALSE accept arguments?"
///     a: "No. FALSE takes zero arguments and always returns the boolean constant FALSE."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: FALSE
/// Type: FalseFn
/// Min args: 0
/// Max args: 0
/// Variadic: false
/// Signature: FALSE()
/// Arg schema: []
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
/// Returns TRUE only when all supplied values evaluate to TRUE.
///
/// `AND` evaluates arguments left to right and short-circuits on a decisive `FALSE`.
///
/// # Remarks
/// - Booleans and numbers are accepted (`0` is FALSE, non-zero is TRUE).
/// - Blank values are treated as FALSE.
/// - Text and other non-coercible values yield `#VALUE!` unless a prior FALSE short-circuits.
/// - If no decisive FALSE is found, the first encountered error is returned.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "All truthy inputs"
/// formula: '=AND(TRUE, 1, 5)'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Text input causes VALUE error"
/// formula: '=AND(TRUE, "x")'
/// expected: "#VALUE!"
/// ```
///
/// ```yaml,docs
/// related:
///   - OR
///   - NOT
///   - XOR
/// faq:
///   - q: "What happens with blanks and text in AND?"
///     a: "Blank values evaluate as FALSE; non-coercible text yields #VALUE! unless a prior FALSE short-circuits."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: AND
/// Type: AndFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: AND(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, BOOL_ONLY, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
impl Function for AndFn {
    fn family_kernel(&self) -> Option<crate::function::FamilyKernel> {
        Some(crate::function::FamilyKernel::And)
    }
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
/// Returns TRUE when any supplied value evaluates to TRUE.
///
/// `OR` evaluates arguments left to right and short-circuits on a decisive `TRUE`.
///
/// # Remarks
/// - Booleans and numbers are accepted (`0` is FALSE, non-zero is TRUE).
/// - Blank values are ignored.
/// - Text and other non-coercible values yield `#VALUE!` if no prior TRUE short-circuits.
/// - If no TRUE is found, the first encountered error is returned.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "One truthy value makes OR true"
/// formula: '=OR(FALSE, 0, 2)'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "No true values and text input"
/// formula: '=OR(FALSE, "x")'
/// expected: "#VALUE!"
/// ```
///
/// ```yaml,docs
/// related:
///   - AND
///   - NOT
///   - XOR
/// faq:
///   - q: "How does OR treat blanks and text?"
///     a: "Blanks are ignored; non-coercible text returns #VALUE! unless a prior TRUE already short-circuits."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: OR
/// Type: OrFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: OR(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, BOOL_ONLY, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
impl Function for OrFn {
    fn family_kernel(&self) -> Option<crate::function::FamilyKernel> {
        Some(crate::function::FamilyKernel::Or)
    }
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
/// Returns one value when a condition is TRUE and another when FALSE.
///
/// `IF(condition, value_if_true, [value_if_false])` supports two or three arguments.
///
/// # Remarks
/// - Condition coercion: booleans are used directly, numbers use `0` as FALSE and non-zero as TRUE.
/// - A blank condition is treated as FALSE.
/// - Text or other non-numeric/non-boolean conditions return `#VALUE!`.
/// - With only two arguments, the FALSE branch defaults to logical `FALSE`.
/// - Array and range conditions select elementwise. Scalar branches and singleton
///   axes broadcast; incompatible non-singleton dimensions return `#VALUE!`.
/// - Each needed branch evaluates once; an unused branch is not evaluated and
///   does not contribute to the result shape. Condition errors remain positional.
/// - Generated arrays use the shared size cap (`#NUM!`); cancellation and
///   resource failures abort evaluation rather than becoming array elements.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Numeric condition"
/// formula: '=IF(2, "yes", "no")'
/// expected: "yes"
/// ```
///
/// ```yaml,sandbox
/// title: "Two-argument IF defaults false branch"
/// formula: '=IF(0, 10)'
/// expected: false
/// ```
///
/// ```yaml,docs
/// related:
///   - IFS
///   - IFERROR
///   - IFNA
/// faq:
///   - q: "What is returned when IF has only two arguments and condition is FALSE?"
///     a: "The false branch defaults to logical FALSE when value_if_false is omitted."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: IF
/// Type: IfFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: IF(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, RETURNS_REFERENCE, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
impl Function for IfFn {
    fn propagate_format(
        &self,
        result: &crate::traits::CalcValue<'_>,
    ) -> Option<crate::format::FormatId> {
        result.format_id()
    }

    func_caps!(PURE, SHORT_CIRCUIT, RETURNS_REFERENCE, MAY_SPILL);

    fn family_kernel(&self) -> Option<crate::function::FamilyKernel> {
        Some(crate::function::FamilyKernel::If)
    }

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

    fn eval_reference<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Option<Result<formualizer_parse::parser::ReferenceType, ExcelError>> {
        match try_resolve_if_reference_or_value(args) {
            Ok(Some(result)) => resolution_to_reference(Ok(result)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }

    fn resolve_reference_or_value<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
        value_fallback: &dyn Fn() -> Result<crate::traits::CalcValue<'b>, ExcelError>,
    ) -> Result<FunctionResolution<'b>, ExcelError> {
        match try_resolve_if_reference_or_value(args)? {
            Some(result) => Ok(result),
            None => value_fallback().map(FunctionResolution::Value),
        }
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

        let condition = match args[0].value()? {
            crate::traits::CalcValue::Range(view) => {
                return eval_array_if(args, _ctx, crate::traits::CalcValue::Range(view));
            }
            other => other.into_literal(),
        };
        let b = match condition {
            LiteralValue::Array(rows) => {
                return eval_array_if(
                    args,
                    _ctx,
                    crate::traits::CalcValue::Scalar(LiteralValue::Array(rows)),
                );
            }
            LiteralValue::Boolean(b) => b,
            LiteralValue::Number(n) => n != 0.0,
            LiteralValue::Int(i) => i != 0,
            LiteralValue::Empty => false,
            LiteralValue::Error(error) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(error)));
            }
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

fn try_resolve_if_reference_or_value<'b>(
    args: &[ArgumentHandle<'_, 'b>],
) -> Result<Option<FunctionResolution<'b>>, ExcelError> {
    if args.len() < 2 || args.len() > 3 {
        return Ok(Some(FunctionResolution::Value(
            crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value()
                    .with_message(format!("IF expects 2 or 3 arguments, got {}", args.len())),
            )),
        )));
    }
    let condition = args[0].value()?;
    // An array condition selects values elementwise, never a single reference.
    // Keep ranges borrowed rather than materializing them to discover this.
    if matches!(condition, crate::traits::CalcValue::Range(_)) {
        return Ok(None);
    }
    let selected = match condition.into_literal() {
        LiteralValue::Boolean(value) => value,
        LiteralValue::Number(value) => value != 0.0,
        LiteralValue::Int(value) => value != 0,
        LiteralValue::Empty => false,
        LiteralValue::Error(error) => {
            return Ok(Some(FunctionResolution::Value(
                crate::traits::CalcValue::Scalar(LiteralValue::Error(error)),
            )));
        }
        LiteralValue::Array(_) => return Ok(None),
        _ => {
            return Ok(Some(FunctionResolution::Value(
                crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message("IF condition must be boolean or number"),
                )),
            )));
        }
    };
    if selected {
        args[1].resolve_reference_or_value().map(Some)
    } else if let Some(arg) = args.get(2) {
        arg.resolve_reference_or_value().map(Some)
    } else {
        Ok(Some(FunctionResolution::Value(
            crate::traits::CalcValue::Scalar(LiteralValue::Boolean(false)),
        )))
    }
}

/// Array-only IF path. Keeping this out of line leaves the scalar coercion,
/// reference selection and family kernel independent of materialization.
#[inline(never)]
fn eval_array_if<'b>(
    args: &[ArgumentHandle<'_, 'b>],
    ctx: &dyn FunctionContext<'b>,
    condition: crate::traits::CalcValue<'b>,
) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
    use super::utils::{CancelPoll, Grid, materialized_shape_too_large};
    use crate::broadcast::{broadcast_shape, project_index};
    use crate::traits::CalcValue;

    fn grid(value: CalcValue<'_>) -> Grid<'_> {
        match value {
            CalcValue::Range(view) => Grid::Range(view),
            CalcValue::Scalar(LiteralValue::Array(rows))
            | CalcValue::AnnotatedScalar(LiteralValue::Array(rows), _) => Grid::Array(rows),
            other => Grid::Scalar(other.into_literal()),
        }
    }
    fn truth(cell: LiteralValue) -> Result<bool, ExcelError> {
        match cell {
            LiteralValue::Boolean(b) => Ok(b),
            LiteralValue::Number(n) => Ok(n != 0.0),
            LiteralValue::Int(n) => Ok(n != 0),
            LiteralValue::Empty => Ok(false),
            LiteralValue::Error(error) => Err(error),
            _ => {
                Err(ExcelError::new_value().with_message("IF condition must be boolean or number"))
            }
        }
    }
    let token = ctx.cancellation_token();
    let is_cancelled = || {
        token
            .as_ref()
            .is_some_and(crate::engine::CancelToken::is_cancelled)
    };
    let mut poll = CancelPoll::new(&is_cancelled);
    let condition = grid(condition);
    let condition_shape = condition.shape();
    poll.advance(0)?;
    if let Some(error) = materialized_shape_too_large(condition_shape) {
        return Ok(CalcValue::Scalar(LiteralValue::Error(error)));
    }
    // Scan without allocating a mask. Invalid conditions select neither arm.
    // A view stays a view; no owned copy is needed for probing or selection.
    let (mut needs_true, mut needs_false) = (false, false);
    'scan: for r in 0..condition_shape.0 {
        for c in 0..condition_shape.1 {
            poll.advance(1)?;
            match truth(condition.get(r, c)) {
                Ok(true) => needs_true = true,
                Ok(false) => needs_false = true,
                Err(error) if super::logical_ext::is_live_fault(&error) => return Err(error),
                Err(_) => {}
            }
            if needs_true && needs_false {
                break 'scan;
            }
        }
    }
    // An unused branch contributes only a singleton shape, and is not evaluated.
    let yes = if needs_true {
        grid(args[1].value()?)
    } else {
        Grid::Scalar(LiteralValue::Empty)
    };
    let no = if needs_false {
        match args.get(2) {
            Some(arg) => grid(arg.value()?),
            None => Grid::Scalar(LiteralValue::Boolean(false)),
        }
    } else {
        Grid::Scalar(LiteralValue::Empty)
    };
    // Branch evaluation may have cancelled since the scan's last poll.
    let mut poll = CancelPoll::new(&is_cancelled);
    poll.advance(0)?;
    let yes_shape = yes.shape();
    let no_shape = no.shape();
    let shape = match broadcast_shape(&[condition_shape, yes_shape, no_shape]) {
        Ok(shape) => shape,
        Err(error) => return Ok(CalcValue::Scalar(LiteralValue::Error(error))),
    };
    if let Some(error) = materialized_shape_too_large(shape) {
        return Ok(CalcValue::Scalar(LiteralValue::Error(error)));
    }
    let mut output = Vec::with_capacity(shape.0);
    for r in 0..shape.0 {
        let mut row = Vec::with_capacity(shape.1);
        for c in 0..shape.1 {
            poll.advance(1)?;
            let (cr, cc) = project_index((r, c), condition_shape);
            let selected = match truth(condition.get(cr, cc)) {
                Ok(true) => {
                    let (r, c) = project_index((r, c), yes_shape);
                    yes.get(r, c)
                }
                Ok(false) => {
                    let (r, c) = project_index((r, c), no_shape);
                    no.get(r, c)
                }
                Err(error) if super::logical_ext::is_live_fault(&error) => return Err(error),
                Err(error) => LiteralValue::Error(error),
            };
            if let LiteralValue::Error(ref error) = selected {
                if super::logical_ext::is_live_fault(error) {
                    return Err(error.clone());
                }
            }
            row.push(selected);
        }
        output.push(row);
    }
    Ok(CalcValue::Scalar(LiteralValue::Array(output)))
}

pub fn register_builtins() {
    crate::function_registry::register_builtin(std::sync::Arc::new(TrueFn));
    crate::function_registry::register_builtin(std::sync::Arc::new(FalseFn));
    crate::function_registry::register_builtin(std::sync::Arc::new(AndFn));
    crate::function_registry::register_builtin(std::sync::Arc::new(OrFn));
    crate::function_registry::register_builtin(std::sync::Arc::new(IfFn));
}

/* ─────────────────────────── tests ─────────────────────────────── */

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{CycleConfig, CycleDetection, CyclePolicy, Engine, EvalConfig};
    use crate::traits::ArgumentHandle;
    use crate::{interpreter::Interpreter, test_workbook::TestWorkbook};
    use formualizer_common::ExcelErrorKind;
    use formualizer_parse::{LiteralValue, parser::Parser, parser::parse};
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

    fn evaluate_formula(formula: &str, wb: &TestWorkbook) -> LiteralValue {
        let mut parser = Parser::new(formula).expect("parser");
        let ast = parser.parse().expect("parse");
        wb.interpreter()
            .evaluate_ast(&ast)
            .expect("evaluate")
            .into_literal()
    }

    fn assert_error_kind(value: LiteralValue, kind: ExcelErrorKind) {
        assert!(
            matches!(value, LiteralValue::Error(ref error) if error.kind == kind),
            "expected {kind:?}, got {value:?}"
        );
    }

    #[test]
    fn array_if_truthiness_and_broadcast() {
        crate::builtins::load_builtins();
        let wb = TestWorkbook::new();
        for (formula, expected) in [
            ("=IF({TRUE;FALSE;2}, {10;20;30}, 0)", "[[10], [0], [30]]"),
            ("=IF({TRUE;FALSE}, {10,20}, 0)", "[[10, 20], [0, 0]]"),
            ("=IF({TRUE;FALSE}, 7)", "[[7], [FALSE]]"),
            ("=IF({TRUE;FALSE}, IF({FALSE;TRUE}, 1, 2), 0)", "[[2], [0]]"),
        ] {
            let actual = evaluate_formula(formula, &wb);
            fn norm(value: &LiteralValue) -> String {
                match value {
                    LiteralValue::Array(rows) => format!(
                        "[{}]",
                        rows.iter()
                            .map(|row| format!(
                                "[{}]",
                                row.iter().map(norm).collect::<Vec<_>>().join(", ")
                            ))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                    LiteralValue::Boolean(b) => b.to_string().to_uppercase(),
                    other => other.to_string(),
                }
            }
            assert_eq!(norm(&actual), expected, "{formula}: {actual:?}");
        }
    }

    #[test]
    fn array_if_blank_integer_and_invalid_conditions() {
        use formualizer_parse::parser::{ASTNode, ASTNodeType};
        let wb = TestWorkbook::new();
        let interp = wb.interpreter();
        let condition = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Empty,
                LiteralValue::Int(0),
                LiteralValue::Int(-2),
                LiteralValue::Text("bad".into()),
                LiteralValue::Error(ExcelError::new_na()),
            ]])),
            None,
        );
        let branch = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(7)), None);
        let args = [
            ArgumentHandle::new(&condition, &interp),
            ArgumentHandle::new(&branch, &interp),
        ];
        let actual = IfFn
            .eval(&args, &interp.function_context(None))
            .unwrap()
            .into_literal();
        let LiteralValue::Array(rows) = actual else {
            panic!("{actual:?}")
        };
        assert_eq!(rows[0][0], LiteralValue::Boolean(false));
        assert_eq!(rows[0][1], LiteralValue::Boolean(false));
        assert_eq!(rows[0][2], LiteralValue::Int(7));
        assert_error_kind(rows[0][3].clone(), ExcelErrorKind::Value);
        assert_error_kind(rows[0][4].clone(), ExcelErrorKind::Na);
    }

    #[test]
    fn array_if_only_evaluates_selected_branches_once() {
        let counter = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new()
            .with_function(Arc::new(IfFn))
            .with_function(Arc::new(CountFn(counter.clone())));
        let result = evaluate_formula("=IF({TRUE;TRUE}, 7, COUNTING())", &wb);
        assert!(matches!(result, LiteralValue::Array(_)), "{result:?}");
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        let result = evaluate_formula("=IF({TRUE;FALSE;TRUE}, COUNTING(), COUNTING())", &wb);
        assert!(matches!(result, LiteralValue::Array(_)), "{result:?}");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
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

    #[test]
    fn if_treats_empty_condition_as_false() {
        let wb = TestWorkbook::new().with_function(Arc::new(IfFn));
        let ctx = interp(&wb);
        let fctx = ctx.function_context(None);
        let iff = ctx.context.get_function("", "IF").unwrap();

        let cond_empty = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Empty),
            None,
        );
        let when_true = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Int(10)),
            None,
        );
        let when_false = formualizer_parse::parser::ASTNode::new(
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Int(20)),
            None,
        );

        let args = vec![
            ArgumentHandle::new(&cond_empty, &ctx),
            ArgumentHandle::new(&when_true, &ctx),
            ArgumentHandle::new(&when_false, &ctx),
        ];

        assert_eq!(
            iff.eval(&args, &fctx).unwrap().into_literal(),
            LiteralValue::Int(20)
        );
    }

    #[test]
    fn if_propagates_condition_error_kind() {
        let wb = TestWorkbook::new()
            .with_function(Arc::new(IfFn))
            .with_function(Arc::new(crate::builtins::info::NaFn));

        assert_error_kind(evaluate_formula("=IF(NA()=0,0,1)", &wb), ExcelErrorKind::Na);
        assert_error_kind(evaluate_formula("=IF(1/0>1,1,2)", &wb), ExcelErrorKind::Div);
    }

    #[test]
    fn if_errored_condition_records_no_arm_edges() {
        let config = EvalConfig::default().with_cycle(CycleConfig {
            detection: CycleDetection::Runtime,
            policy: CyclePolicy::Error,
        });
        let mut engine = Engine::new(TestWorkbook::new(), config);
        engine
            .set_cell_formula(
                "Sheet1",
                1,
                1,
                parse("=IF(NA()=0,INDEX(Q1:Q100,50),0)").expect("parse A1"),
            )
            .expect("set A1");
        engine
            .set_cell_formula("Sheet1", 50, 17, parse("=A1").expect("parse Q50"))
            .expect("set Q50");

        engine.evaluate_all().expect("evaluate");

        assert_error_kind(
            engine.get_cell_value("Sheet1", 1, 1).expect("A1 value"),
            ExcelErrorKind::Na,
        );
        assert!(
            !matches!(
                engine.get_cell_value("Sheet1", 50, 17),
                Some(LiteralValue::Error(error)) if error.kind == ExcelErrorKind::Circ
            ),
            "Q50 must not be circular when the IF condition errors"
        );
        assert_eq!(engine.last_cycle_telemetry().live_cycles_witnessed, 0);
    }

    #[test]
    fn if_text_condition_is_value_error() {
        let wb = TestWorkbook::new().with_function(Arc::new(IfFn));
        assert_error_kind(
            evaluate_formula("=IF(\"abc\",1,2)", &wb),
            ExcelErrorKind::Value,
        );
    }
}
