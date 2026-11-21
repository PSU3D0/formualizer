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
/// Use `TRUE` for the logical value true in formulas.
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
/// Use `FALSE` for the logical value false in formulas.
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
/// `AND` evaluates every argument left to right, as Excel does: a `FALSE`
/// does not hide a later error.
///
/// # Remarks
/// - Booleans and numbers are accepted (`0` is FALSE, non-zero is TRUE).
/// - Text and blank cells inside a reference or array are ignored.
/// - A direct text argument yields `#VALUE!`.
/// - The first error in argument order (ranges scanned row by row) is returned.
/// - With no logical values at all, the result is `#VALUE!`.
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
///     a: "Text and blank cells in a reference or array are ignored; a direct text argument yields #VALUE!. If nothing logical remains, AND returns #VALUE!."
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
        let mut all_true = true;
        let outcome = scan_logical_args(args, "AND", |b, _| all_true &= b)?;
        Ok(crate::traits::CalcValue::Scalar(
            outcome.unwrap_or(LiteralValue::Boolean(all_true)),
        ))
    }
}

/* ─────────────────────────── OR() ───────────────────────────────── */

#[derive(Debug)]
pub struct OrFn;
/// Returns TRUE when any supplied value evaluates to TRUE.
///
/// `OR` evaluates every argument left to right, as Excel does: a `TRUE`
/// does not hide a later error.
///
/// # Remarks
/// - Booleans and numbers are accepted (`0` is FALSE, non-zero is TRUE).
/// - Text and blank cells inside a reference or array are ignored.
/// - A direct text argument yields `#VALUE!`.
/// - The first error in argument order (ranges scanned row by row) is returned.
/// - With no logical values at all, the result is `#VALUE!`.
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
///     a: "Text and blank cells in a reference or array are ignored; a direct text argument returns #VALUE!. If nothing logical remains, OR returns #VALUE!."
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
        let mut any_true = false;
        let outcome = scan_logical_args(args, "OR", |b, _| any_true |= b)?;
        Ok(crate::traits::CalcValue::Scalar(
            outcome.unwrap_or(LiteralValue::Boolean(any_true)),
        ))
    }
}

/// Walk the arguments of `AND`/`OR`/`XOR` the way Excel does, handing each
/// logical value to `on_logical` in argument order.
///
/// Every argument is evaluated (Excel does not short-circuit these
/// functions), and ranges and arrays are scanned in row-major order:
/// - Booleans and numbers are logical values (`0` is FALSE).
/// - Text and blank cells inside a reference or array are ignored.
/// - A direct text argument is `#VALUE!`.
/// - The first error in argument order is the result.
///
/// Returns `Some(error)` when the result is an error (the first error, or
/// `#VALUE!` when no logical value was seen) and `None` when the caller's
/// accumulated logical result stands. Cancellation and resource faults
/// abort rather than becoming the result.
pub(crate) fn scan_logical_args(
    args: &[ArgumentHandle<'_, '_>],
    name: &'static str,
    mut on_logical: impl FnMut(bool, u64),
) -> Result<Option<LiteralValue>, ExcelError> {
    use crate::traits::{CalcValue, ResolvedArgument};

    let mut first_error: Option<ExcelError> = None;
    let mut seen_logical = false;
    for arg in args {
        let resolved = match arg.resolve_once() {
            Ok(resolved) => resolved,
            Err(error) if super::logical_ext::is_live_fault(&error) => return Err(error),
            Err(error) => ResolvedArgument::ReferenceError(error),
        };
        match resolved {
            ResolvedArgument::Range(view) => {
                let mut visit = |cell: &LiteralValue, count: u64| {
                    if count == 0 {
                        return;
                    }
                    match cell {
                        LiteralValue::Boolean(b) => {
                            seen_logical = true;
                            on_logical(*b, count);
                        }
                        LiteralValue::Number(n) => {
                            seen_logical = true;
                            on_logical(*n != 0.0, count);
                        }
                        LiteralValue::Int(i) => {
                            seen_logical = true;
                            on_logical(*i != 0, count);
                        }
                        LiteralValue::Error(error) if first_error.is_none() => {
                            first_error = Some(error.clone());
                        }
                        // Text, blanks and other non-logical cells are ignored.
                        _ => {}
                    }
                };
                let (rows, cols) = view.dims();
                let (ar, ac) = view.active_dims();
                let tail_rows = (rows - ar) as u64;
                let tail_cols = (cols - ac) as u64;
                if tail_cols == 0 {
                    view.core_sub_view().for_each_cell(&mut |cell| {
                        visit(cell, 1);
                        Ok(())
                    })?;
                } else {
                    let mut since_poll = 4096usize;
                    for r in 0..ar {
                        for c in 0..ac {
                            if since_poll >= 4096 {
                                since_poll = 0;
                                if view.criteria_cache_cancelled() {
                                    return Err(ExcelError::new(
                                        formualizer_common::ExcelErrorKind::Cancelled,
                                    ));
                                }
                            }
                            since_poll += 1;
                            let cell = view.get_cell(r, c);
                            visit(&cell, 1);
                        }
                        if since_poll >= 4096 {
                            since_poll = 0;
                            if view.criteria_cache_cancelled() {
                                return Err(ExcelError::new(
                                    formualizer_common::ExcelErrorKind::Cancelled,
                                ));
                            }
                        }
                        since_poll += 1;
                        let tail_cell = view.right_tail_cell(r);
                        visit(&tail_cell, tail_cols);
                    }
                }
                if tail_rows > 0 {
                    for c in 0..ac {
                        let tail_cell = view.bottom_tail_cell(c);
                        visit(&tail_cell, tail_rows);
                    }
                    if tail_cols > 0 {
                        let corner = view.corner_tail_cell();
                        visit(&corner, tail_rows.saturating_mul(tail_cols));
                    }
                }
            }
            ResolvedArgument::ReferenceError(error) => {
                if super::logical_ext::is_live_fault(&error) {
                    return Err(error);
                }
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            ResolvedArgument::Value(value) => {
                let value = match value {
                    CalcValue::Scalar(v) | CalcValue::AnnotatedScalar(v, _) => v,
                    // `resolve_once` folds ranges into `ResolvedArgument::Range`.
                    CalcValue::Range(_) | CalcValue::Callable(_) => {
                        LiteralValue::Error(ExcelError::new_value())
                    }
                };
                match value {
                    LiteralValue::Boolean(b) => {
                        seen_logical = true;
                        on_logical(b, 1);
                    }
                    LiteralValue::Number(n) => {
                        seen_logical = true;
                        on_logical(n != 0.0, 1);
                    }
                    LiteralValue::Int(i) => {
                        seen_logical = true;
                        on_logical(i != 0, 1);
                    }
                    LiteralValue::Empty => {}
                    LiteralValue::Error(error) => {
                        if super::logical_ext::is_live_fault(&error) {
                            return Err(error);
                        }
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    _ => {
                        if first_error.is_none() {
                            first_error = Some(ExcelError::new_value().with_message(format!(
                                "{name} expects logical/numeric inputs; text is not coercible"
                            )));
                        }
                    }
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Ok(Some(LiteralValue::Error(error)));
    }
    if !seen_logical {
        return Ok(Some(LiteralValue::Error(
            ExcelError::new_value().with_message(format!("{name} found no logical values")),
        )));
    }
    Ok(None)
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
        match try_resolve_if_reference_or_value(args, _ctx) {
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
        match try_resolve_if_reference_or_value(args, _ctx)? {
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
            report_if_selected_function_reference(&args[1], _ctx);
            args[1].value()
        } else if let Some(arg) = args.get(2) {
            report_if_selected_function_reference(arg, _ctx);
            arg.value()
        } else {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
                false,
            )))
        }
    }
}

/// Record only the actually selected producer boundary. The Core query is
/// metadata-only and respects real names/locals; unknown binding presence is
/// uncertainty, never absence proof. Raw Callable values are not bare
/// function references and continue through their existing value path.
fn report_if_selected_function_reference<'b>(
    arg: &ArgumentHandle<'_, 'b>,
    ctx: &dyn FunctionContext<'b>,
) {
    if arg.function_value_reference_intent().is_some() {
        ctx.report_unavailable_operation(
            crate::operation_diagnostics::UnavailableOperation::UnverifiedFunctionMode {
                name: "IF".into(),
                mode: "function_reference_selected_value",
            },
        );
    }
}

fn try_resolve_if_reference_or_value<'b>(
    args: &[ArgumentHandle<'_, 'b>],
    ctx: &dyn FunctionContext<'b>,
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
        report_if_selected_function_reference(&args[1], ctx);
        args[1].resolve_reference_or_value().map(Some)
    } else if let Some(arg) = args.get(2) {
        report_if_selected_function_reference(arg, ctx);
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
    use super::utils::{CancelPoll, materialized_shape_too_large};
    use crate::broadcast::broadcast_shape;
    use crate::engine::range_view::{TailedOperand, build_tailed_range_view, combined_active_dims};
    use crate::traits::CalcValue;

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
    let condition = TailedOperand::from_calc_value(condition);
    let condition_shape = condition.dims();
    poll.advance(0)?;
    if let Some(error) =
        materialized_shape_too_large(condition_shape, ctx.materialized_array_cells())
    {
        return Ok(CalcValue::Scalar(LiteralValue::Error(error)));
    }
    // Scan without allocating a mask, visiting only active core + uniform tails.
    let (mut needs_true, mut needs_false) = (false, false);
    let (cond_ar, cond_ac) = condition.active_dims();
    'scan: for r in 0..cond_ar {
        for c in 0..cond_ac {
            poll.advance(1)?;
            match truth(condition.get_core(r, c)) {
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
    if !(needs_true && needs_false)
        && let TailedOperand::Range(ref rv) = condition
    {
        rv.for_each_tail_entry(|cell, _| {
            if needs_true && needs_false {
                return Ok(());
            }
            poll.advance(1)?;
            match truth(cell.clone()) {
                Ok(true) => needs_true = true,
                Ok(false) => needs_false = true,
                Err(error) if super::logical_ext::is_live_fault(&error) => return Err(error),
                Err(_) => {}
            }
            Ok(())
        })?;
    }
    // An unused branch contributes only a singleton shape, and is not evaluated.
    let yes = if needs_true {
        report_if_selected_function_reference(&args[1], ctx);
        TailedOperand::from_calc_value(args[1].value()?)
    } else {
        TailedOperand::Scalar(LiteralValue::Empty)
    };
    let no = if needs_false {
        match args.get(2) {
            Some(arg) => {
                report_if_selected_function_reference(arg, ctx);
                TailedOperand::from_calc_value(arg.value()?)
            }
            None => TailedOperand::Scalar(LiteralValue::Boolean(false)),
        }
    } else {
        TailedOperand::Scalar(LiteralValue::Empty)
    };
    // Branch evaluation may have cancelled since the scan's last poll.
    let mut poll = CancelPoll::new(&is_cancelled);
    poll.advance(0)?;
    let yes_shape = yes.dims();
    let no_shape = no.dims();
    let shape = match broadcast_shape(&[condition_shape, yes_shape, no_shape]) {
        Ok(shape) => shape,
        Err(error) => return Ok(CalcValue::Scalar(LiteralValue::Error(error))),
    };
    if let Some(error) = materialized_shape_too_large(shape, ctx.materialized_array_cells()) {
        return Ok(CalcValue::Scalar(LiteralValue::Error(error)));
    }

    fn select_cell(
        cond_val: LiteralValue,
        yes_val: impl FnOnce() -> LiteralValue,
        no_val: impl FnOnce() -> LiteralValue,
    ) -> Result<LiteralValue, ExcelError> {
        let selected = match truth(cond_val) {
            Ok(true) => yes_val(),
            Ok(false) => no_val(),
            Err(error) if super::logical_ext::is_live_fault(&error) => return Err(error),
            Err(error) => LiteralValue::Error(error),
        };
        if let LiteralValue::Error(ref error) = selected
            && super::logical_ext::is_live_fault(error)
        {
            return Err(error.clone());
        }
        Ok(selected)
    }

    let active = combined_active_dims(shape, &[&condition, &yes, &no]);
    if active != shape {
        let view = build_tailed_range_view(
            shape,
            active,
            ctx.date_system(),
            token,
            ctx.materialized_array_cells(),
            |pos| match pos {
                crate::engine::range_view::TailPos::Core(r, c) => select_cell(
                    condition.get_core(r, c),
                    || yes.get_core(r, c),
                    || no.get_core(r, c),
                ),
                crate::engine::range_view::TailPos::Right(r) => select_cell(
                    condition.get_right_tail(r),
                    || yes.get_right_tail(r),
                    || no.get_right_tail(r),
                ),
                crate::engine::range_view::TailPos::Bottom(c) => select_cell(
                    condition.get_bottom_tail(c),
                    || yes.get_bottom_tail(c),
                    || no.get_bottom_tail(c),
                ),
                crate::engine::range_view::TailPos::Corner => select_cell(
                    condition.get_corner_tail(),
                    || yes.get_corner_tail(),
                    || no.get_corner_tail(),
                ),
            },
        )?;
        return Ok(CalcValue::Range(view));
    }

    let mut output = Vec::with_capacity(shape.0);
    for r in 0..shape.0 {
        let mut row = Vec::with_capacity(shape.1);
        for c in 0..shape.1 {
            poll.advance(1)?;
            row.push(select_cell(
                condition.get_core(r, c),
                || yes.get_core(r, c),
                || no.get_core(r, c),
            )?);
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
    fn and_evaluates_every_argument_after_a_false() {
        let counter = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new()
            .with_function(Arc::new(AndFn))
            .with_function(Arc::new(CountFn(counter.clone())));
        let ctx = interp(&wb);
        let fctx = ctx.function_context(None);
        let and = ctx.context.get_function("", "AND").unwrap();

        // Build args: FALSE, COUNTING() (COUNTING returns TRUE)
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
            1,
            "Excel evaluates every argument: COUNTING runs once"
        );
    }

    #[test]
    fn or_evaluates_every_argument_after_a_true() {
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
            1,
            "Excel evaluates every argument: COUNTING runs once"
        );
    }

    #[test]
    fn or_range_arg_true_still_evaluates_next_arg() {
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
            1,
            "Excel evaluates every argument: COUNTING runs once"
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
    fn or_returns_error_after_true() {
        let err_counter = Arc::new(AtomicUsize::new(0));
        let wb = TestWorkbook::new()
            .with_function(Arc::new(OrFn))
            .with_function(Arc::new(ErrorFn(err_counter.clone())));
        let ctx = interp(&wb);
        let fctx = ctx.function_context(None);
        let or = ctx.context.get_function("", "OR").unwrap();

        // OR(TRUE, ERRORFN()) => #VALUE!: Excel evaluates every argument
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
        assert_error_kind(out, ExcelErrorKind::Value);
        assert_eq!(
            err_counter.load(Ordering::SeqCst),
            1,
            "ERRORFN is evaluated and its error returned"
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

    fn logical_workbook() -> TestWorkbook {
        crate::builtins::load_builtins();
        TestWorkbook::new()
            .with_cell_a1("Sheet1", "A1", LiteralValue::Boolean(true))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Text("x".into()))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Empty)
            .with_cell_a1("Sheet1", "A4", LiteralValue::Empty)
            .with_cell_a1("Sheet1", "B1", LiteralValue::Boolean(false))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Error(ExcelError::new_na()))
            .with_cell_a1(
                "Sheet1",
                "B3",
                LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div)),
            )
            .with_cell_a1("Sheet1", "C1", LiteralValue::Text("y".into()))
            .with_cell_a1("Sheet1", "D1", LiteralValue::Number(0.0))
    }

    #[test]
    fn and_or_xor_return_the_first_error_in_argument_order() {
        // Excel evaluates every argument of AND/OR/XOR; a decisive FALSE (or
        // TRUE) does not hide a later error. Ranges are scanned in order.
        let wb = logical_workbook();
        for (formula, kind) in [
            ("=AND(FALSE,1/0)", ExcelErrorKind::Div),
            ("=AND(FALSE,#REF!=2003)", ExcelErrorKind::Ref),
            ("=OR(TRUE,1/0)", ExcelErrorKind::Div),
            ("=XOR(TRUE,1/0)", ExcelErrorKind::Div),
            ("=AND(1/0,NA())", ExcelErrorKind::Div),
            ("=OR(NA(),1/0)", ExcelErrorKind::Na),
            ("=XOR(NA(),1/0)", ExcelErrorKind::Na),
            ("=AND(FALSE,B1:B3)", ExcelErrorKind::Na),
            ("=OR(TRUE,B1:B3,1/0)", ExcelErrorKind::Na),
            ("=XOR(B1:B3)", ExcelErrorKind::Na),
            ("=AND(B3,B2)", ExcelErrorKind::Div),
            ("=AND(FALSE,\"x\")", ExcelErrorKind::Value),
            ("=OR(TRUE,\"x\")", ExcelErrorKind::Value),
        ] {
            assert_error_kind(evaluate_formula(formula, &wb), kind);
        }
    }

    #[test]
    fn and_or_xor_ignore_text_and_blanks_in_references_and_arrays() {
        // A2 holds text, A3 and A4 are blank.
        let wb = logical_workbook();
        for (formula, expected) in [
            ("=AND(A1:A4)", true),
            ("=AND(TRUE,A3)", true),
            ("=AND(A1:A2,D1)", false),
            ("=OR(FALSE,A2:A4)", false),
            ("=OR(A2:A4,A1)", true),
            ("=XOR(A1:A4)", true),
            ("=XOR(A1:A4,TRUE)", false),
            ("=AND({TRUE,\"x\"})", true),
            ("=OR({FALSE,\"x\"})", false),
            ("=AND(TRUE,)", false),
        ] {
            assert_eq!(
                evaluate_formula(formula, &wb),
                LiteralValue::Boolean(expected),
                "{formula}"
            );
        }
    }

    #[test]
    fn and_or_xor_without_logical_values_are_value_errors() {
        let wb = logical_workbook();
        for formula in [
            "=AND(A2:A4)",
            "=AND(A3)",
            "=OR(A3:A4)",
            "=OR(C1)",
            "=XOR(A2:A4)",
            "=AND({\"x\"})",
            "=AND(\"x\")",
            "=XOR(TRUE,\"x\")",
        ] {
            assert_error_kind(evaluate_formula(formula, &wb), ExcelErrorKind::Value);
        }
    }

    #[test]
    fn and_or_ignore_never_written_cells_on_the_engine_path() {
        let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Boolean(true))
            .expect("A1");
        for (col, formula) in [
            (2, "=AND(A1,C1)"),
            (4, "=AND(C1)"),
            (5, "=OR(C1:C5,FALSE)"),
            (6, "=IF(AND(FALSE,1/0),1,2)"),
        ] {
            engine
                .set_cell_formula("Sheet1", 1, col, parse(formula).expect("parse"))
                .expect("set formula");
        }
        engine.evaluate_all().expect("evaluate");
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 2),
            Some(LiteralValue::Boolean(true))
        );
        assert_error_kind(
            engine.get_cell_value("Sheet1", 1, 4).expect("D1"),
            ExcelErrorKind::Value,
        );
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 5),
            Some(LiteralValue::Boolean(false))
        );
        assert_error_kind(
            engine.get_cell_value("Sheet1", 1, 6).expect("F1"),
            ExcelErrorKind::Div,
        );
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
