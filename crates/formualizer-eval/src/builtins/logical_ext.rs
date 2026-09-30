use super::utils::{
    ARG_ANY_ONE, CANCEL_POLL_CELLS, CancelPoll, Grid, materialized_shape_too_large,
};
use crate::args::ArgSchema;
use crate::arrow_store::map_error_code;
use crate::broadcast::{broadcast_shape, project_index};
use crate::engine::CancelToken;
use crate::engine::range_view::RangeView;
use crate::function::{Function, FunctionResolution, resolution_to_reference};
use crate::function_contract::FunctionDependencyContract;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use arrow_array::Array as _;
use formualizer_common::{ExcelError, ExcelErrorExtra, ExcelErrorKind, LiteralValue};
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
///
/// ```yaml,docs
/// related:
///   - AND
///   - OR
///   - XOR
/// faq:
///   - q: "How does NOT treat blanks?"
///     a: "Blank is treated as FALSE first, so NOT(blank) returns TRUE."
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
    fn dependency_contract(&self, arity: usize) -> Option<FunctionDependencyContract> {
        FunctionDependencyContract::static_scalar_all_args(arity)
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
///
/// ```yaml,docs
/// related:
///   - AND
///   - OR
///   - NOT
/// faq:
///   - q: "What determines XOR's final result?"
///     a: "XOR returns TRUE when the count of truthy inputs is odd; blanks are ignored."
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
/// - Evaluation failures surfaced as interpreter errors are also caught, except
///   cancellation and resource-limit failures, which abort the evaluation
///   request instead of selecting the fallback.
/// - When the first argument is an array or range, each error element is
///   replaced by the matching element of the fallback and other elements pass
///   through. A scalar fallback applies to every error element; an array
///   fallback is paired by position using the same broadcast rule as array
///   operators (a single row or column repeats; other mismatched sizes return
///   `#VALUE!`). Errors inside the fallback are returned as-is.
/// - The fallback is evaluated at most once, and only when an error is
///   present. An array or range with no error elements is returned unchanged,
///   so a larger array fallback does not change the result's size.
/// - A replaced array result larger than 16,777,216 cells, or larger than a
///   sheet in either dimension, returns `#NUM!`.
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
///
/// ```yaml,sandbox
/// title: "Replace errors element by element"
/// grid:
///   A1: 1
///   A2: 0
///   A3: 2
/// formula: '=IFERROR(1/A1:A3, -1)'
/// expected: [[1],[-1],[0.5]]
/// ```
///
/// ```yaml,docs
/// related:
///   - IFNA
///   - IF
///   - ISERROR
/// faq:
///   - q: "Does IFERROR catch all error types?"
///     a: "An error produced while evaluating the first argument triggers the fallback. Dependency preparation, cancellation, and resource-limit failures abort the request instead of selecting the fallback."
///   - q: "How does IFERROR handle arrays?"
///     a: "Each error element of an array or range is replaced by the corresponding fallback element; clean arrays are returned unchanged without evaluating the fallback."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: IFERROR
/// Type: IfErrorFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: IFERROR(arg1: any@scalar, arg2: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
impl Function for IfErrorFn {
    fn family_kernel(&self) -> Option<crate::function::FamilyKernel> {
        Some(crate::function::FamilyKernel::IfError)
    }
    fn propagate_format(
        &self,
        result: &crate::traits::CalcValue<'_>,
    ) -> Option<crate::format::FormatId> {
        result.format_id()
    }

    // SHORT_CIRCUIT: dispatch must not eagerly evaluate the fallback arm —
    // the eval body below evaluates arg0 first and touches arg1 only when
    // arg0 produced an error (same defect class as the IF fix in #118).
    func_caps!(PURE, SHORT_CIRCUIT, MAY_SPILL);
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        error_guard(args, ctx, ErrorGuard::AnyError)
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
/// - When the first argument is an array or range, each `#N/A` element is
///   replaced by the matching element of the fallback, with the same pairing,
///   laziness, and size rules as `IFERROR`; other elements, including other
///   error kinds, pass through.
/// - Evaluation failures of the first argument are not caught.
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
///
/// ```yaml,docs
/// related:
///   - IFERROR
///   - ISNA
///   - NA
/// faq:
///   - q: "Which errors does IFNA intercept?"
///     a: "Only #N/A is intercepted; all other errors pass through unchanged, including inside arrays."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: IFNA
/// Type: IfNaFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: IFNA(arg1: any@scalar, arg2: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
impl Function for IfNaFn {
    fn propagate_format(
        &self,
        result: &crate::traits::CalcValue<'_>,
    ) -> Option<crate::format::FormatId> {
        result.format_id()
    }

    // SHORT_CIRCUIT: the fallback arm is evaluated only when arg0 is #N/A;
    // all other values/errors pass through without touching arg1.
    func_caps!(PURE, SHORT_CIRCUIT, MAY_SPILL);
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() != 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        error_guard(args, ctx, ErrorGuard::NaOnly)
    }
}

/* ───────────────────── shared IFERROR / IFNA selection ─────────────────────
 *
 * Both guards select, per element of `value`, either the element itself or
 * the corresponding element of the fallback. They differ only in which error
 * kinds are replaced and in how an `Err` from evaluating `value` is treated.
 *
 * Scalar values keep the historical behaviour exactly. For an array or range
 * value:
 * - the value is probed for a matching error (Arrow error lanes for views,
 *   a direct scan for owned arrays); a clean value is returned untouched and
 *   the fallback is never evaluated, however large it would be;
 * - otherwise the fallback is evaluated exactly once and the output shape is
 *   the operator broadcast of both shapes (`broadcast.rs`: singleton axes
 *   broadcast, incompatible axes are `#VALUE!`);
 * - the output is bounded by the shared generated-array cap (`#NUM!`)
 *   before allocation;
 * - fallback elements are used as-is, so fallback errors propagate
 *   positionally.
 *
 * Live cancellation and resource failures are request-level outcomes, not
 * spreadsheet errors: they propagate as `Err` from either argument and from
 * the scan/copy loops instead of being replaced by the fallback.
 */

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorGuard {
    /// IFERROR: every error kind selects the fallback.
    AnyError,
    /// IFNA: only `#N/A` selects the fallback.
    NaOnly,
}

impl ErrorGuard {
    #[inline]
    fn matches_kind(self, kind: ExcelErrorKind) -> bool {
        match self {
            ErrorGuard::AnyError => true,
            ErrorGuard::NaOnly => kind == ExcelErrorKind::Na,
        }
    }

    #[inline]
    fn matches(self, value: &LiteralValue) -> bool {
        matches!(value, LiteralValue::Error(e) if self.matches_kind(e.kind))
    }
}

/// Failures that describe the evaluation request rather than a cell value.
fn is_live_fault(error: &ExcelError) -> bool {
    error.kind == ExcelErrorKind::Cancelled
        || matches!(error.extra, ExcelErrorExtra::Resource { .. })
}

fn error_guard<'a, 'b, 'c>(
    args: &'c [ArgumentHandle<'a, 'b>],
    ctx: &dyn FunctionContext<'b>,
    guard: ErrorGuard,
) -> Result<CalcValue<'b>, ExcelError> {
    let value = match args[0].value() {
        Ok(value) => value,
        Err(error) if is_live_fault(&error) => return Err(error),
        // IFERROR has always caught evaluation failures of its value; IFNA
        // has always propagated them.
        Err(_) if guard == ErrorGuard::AnyError => return args[1].value(),
        Err(error) => return Err(error),
    };

    let token = ctx.cancellation_token();
    let is_cancelled = || token.as_ref().is_some_and(CancelToken::is_cancelled);

    let value_grid = match value {
        CalcValue::Scalar(LiteralValue::Error(ref e))
        | CalcValue::AnnotatedScalar(LiteralValue::Error(ref e), _)
            if guard.matches_kind(e.kind) =>
        {
            return args[1].value();
        }
        // Host functions may return an annotated array; the annotation is kept
        // on the clean passthrough and dropped from a rebuilt array, like
        // every other array result.
        CalcValue::Scalar(LiteralValue::Array(ref rows))
        | CalcValue::AnnotatedScalar(LiteralValue::Array(ref rows), _) => {
            if !array_has_match(rows, guard, &mut CancelPoll::new(&is_cancelled))? {
                return Ok(value);
            }
            let (CalcValue::Scalar(LiteralValue::Array(rows))
            | CalcValue::AnnotatedScalar(LiteralValue::Array(rows), _)) = value
            else {
                unreachable!("matched above");
            };
            Grid::Array(rows)
        }
        CalcValue::Range(ref view) => {
            let probe = match &token {
                Some(token) => view.clone().with_cancel_token(Some(token.clone())),
                None => view.clone(),
            };
            if !view_has_match(&probe, guard, &mut CancelPoll::new(&is_cancelled))? {
                return Ok(value);
            }
            Grid::Range(probe)
        }
        other => return Ok(other),
    };

    let fallback = match args[1].value() {
        Ok(fallback) => fallback,
        Err(error) if is_live_fault(&error) => return Err(error),
        Err(error) => CalcValue::Scalar(LiteralValue::Error(error)),
    };

    // A single matching element with a single fallback is the scalar case.
    let fallback_is_single = match &fallback {
        CalcValue::Scalar(LiteralValue::Array(rows))
        | CalcValue::AnnotatedScalar(LiteralValue::Array(rows), _) => {
            rows.len() == 1 && rows.first().is_some_and(|row| row.len() == 1)
        }
        CalcValue::Range(view) => view.dims() == (1, 1),
        _ => true,
    };
    if value_grid.shape() == (1, 1) && fallback_is_single {
        CancelPoll::new(&is_cancelled).advance(1)?;
        return Ok(fallback);
    }

    let fallback_grid = match fallback {
        CalcValue::Range(view) => Grid::Range(view),
        CalcValue::Scalar(LiteralValue::Array(rows))
        | CalcValue::AnnotatedScalar(LiteralValue::Array(rows), _) => Grid::Array(rows),
        other => Grid::Scalar(other.into_literal()),
    };

    match select_elementwise(
        &value_grid,
        &fallback_grid,
        guard,
        &mut CancelPoll::new(&is_cancelled),
    )? {
        Ok(rows) => Ok(CalcValue::Scalar(LiteralValue::Array(rows))),
        Err(error) => Ok(CalcValue::Scalar(LiteralValue::Error(error))),
    }
}

/// Scans an owned array for an element the guard replaces.
fn array_has_match(
    rows: &[Vec<LiteralValue>],
    guard: ErrorGuard,
    poll: &mut CancelPoll<'_>,
) -> Result<bool, ExcelError> {
    poll.advance(0)?;
    for row in rows {
        for cell in row {
            poll.advance(1)?;
            if guard.matches(cell) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Probes a view's overlay-merged Arrow error lanes for an element the guard
/// replaces, without decoding cells. The view's cancellation token is checked
/// before each bounded piece is prepared, and `poll` before each is scanned;
/// a cancelled walk is an `Err`, never a clean result.
fn view_has_match(
    view: &RangeView<'_>,
    guard: ErrorGuard,
    poll: &mut CancelPoll<'_>,
) -> Result<bool, ExcelError> {
    poll.advance(0)?;
    if view.is_empty() {
        return Ok(false);
    }
    let na_code = map_error_code(ExcelErrorKind::Na);
    // Pieces are one column by at most CANCEL_POLL_CELLS rows, so neither lane
    // preparation nor the IFNA scan runs unbounded between polls.
    view.try_for_each_error_piece(CANCEL_POLL_CELLS, &mut |lane| {
        poll.advance(lane.len())?;
        if lane.null_count() == lane.len() {
            return Ok(false);
        }
        Ok(match guard {
            ErrorGuard::AnyError => true,
            ErrorGuard::NaOnly => lane.iter().any(|code| code == Some(na_code)),
        })
    })
}

/// Builds the broadcast selection of `value` and `fallback`.
///
/// The outer `Result` carries live faults (cancellation); the inner one is a
/// spreadsheet error result (`#VALUE!` for incompatible shapes, `#NUM!` over
/// the shared generated-array cap), decided before any output allocation.
fn select_elementwise(
    value: &Grid<'_>,
    fallback: &Grid<'_>,
    guard: ErrorGuard,
    poll: &mut CancelPoll<'_>,
) -> Result<Result<Vec<Vec<LiteralValue>>, ExcelError>, ExcelError> {
    let value_shape = value.shape();
    let fallback_shape = fallback.shape();
    let shape = match broadcast_shape(&[value_shape, fallback_shape]) {
        Ok(shape) => shape,
        Err(error) => return Ok(Err(error)),
    };
    if let Some(error) = materialized_shape_too_large(shape) {
        return Ok(Err(error));
    }
    poll.advance(0)?;
    let mut out = Vec::with_capacity(shape.0);
    for r in 0..shape.0 {
        let mut row = Vec::with_capacity(shape.1);
        for c in 0..shape.1 {
            poll.advance(1)?;
            let (vr, vc) = project_index((r, c), value_shape);
            let cell = value.get(vr, vc);
            row.push(if guard.matches(&cell) {
                let (fr, fc) = project_index((r, c), fallback_shape);
                fallback.get(fr, fc)
            } else {
                cell
            });
        }
        out.push(row);
    }
    Ok(Ok(out))
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
///
/// ```yaml,docs
/// related:
///   - IF
///   - AND
///   - OR
/// faq:
///   - q: "What errors can IFS return on structure issues?"
///     a: "Odd argument counts return #VALUE!, and no TRUE condition returns #N/A."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: IFS
/// Type: IfsFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: IFS(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, RETURNS_REFERENCE, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
impl Function for IfsFn {
    fn propagate_format(
        &self,
        result: &crate::traits::CalcValue<'_>,
    ) -> Option<crate::format::FormatId> {
        result.format_id()
    }

    func_caps!(PURE, SHORT_CIRCUIT, RETURNS_REFERENCE, MAY_SPILL);
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
    fn eval_reference<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Option<Result<formualizer_parse::parser::ReferenceType, ExcelError>> {
        resolution_to_reference(resolve_ifs_reference_or_value(args, ctx))
    }
    fn resolve_reference_or_value<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
        _value_fallback: &dyn Fn() -> Result<crate::traits::CalcValue<'b>, ExcelError>,
    ) -> Result<FunctionResolution<'b>, ExcelError> {
        resolve_ifs_reference_or_value(args, ctx)
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

fn resolve_selected_non_if<'b>(
    selected: &ArgumentHandle<'_, 'b>,
    _ctx: &dyn FunctionContext<'b>,
) -> Result<FunctionResolution<'b>, ExcelError> {
    selected.resolve_reference_or_value()
}

fn value_error_resolution<'b>() -> FunctionResolution<'b> {
    FunctionResolution::Value(crate::traits::CalcValue::Scalar(LiteralValue::Error(
        ExcelError::new_value(),
    )))
}

fn resolve_ifs_reference_or_value<'b>(
    args: &[ArgumentHandle<'_, 'b>],
    ctx: &dyn FunctionContext<'b>,
) -> Result<FunctionResolution<'b>, ExcelError> {
    if args.len() < 2 || !args.len().is_multiple_of(2) {
        return Ok(value_error_resolution());
    }
    for pair in args.chunks(2) {
        let condition = pair[0].value()?.into_literal();
        let selected = match condition {
            LiteralValue::Boolean(value) => value,
            LiteralValue::Number(value) => value != 0.0,
            LiteralValue::Int(value) => value != 0,
            LiteralValue::Empty => false,
            LiteralValue::Error(error) => {
                return Ok(FunctionResolution::Value(crate::traits::CalcValue::Scalar(
                    LiteralValue::Error(error),
                )));
            }
            _ => return Ok(value_error_resolution()),
        };
        if selected {
            return resolve_selected_non_if(&pair[1], ctx);
        }
    }
    Ok(FunctionResolution::Value(crate::traits::CalcValue::Scalar(
        LiteralValue::Error(ExcelError::new_na()),
    )))
}

/// Returns the result corresponding to the first matching candidate value.
///
/// `SWITCH(expression, value1, result1, [value2, result2], ..., [default])`
/// compares `expression` against each candidate from left to right.
///
/// # Remarks
/// - Matching is case-insensitive for text values.
/// - Numeric comparisons treat `Int` and `Number` values as compatible.
/// - A blank cell on either side compares as numeric zero, so a blank selector matches a `0`
///   case; it does not match empty text (`""`) or `FALSE`. This deliberately differs from the
///   `=` operator, where a blank also equals `""` and `FALSE`: Excel's `SWITCH` was measured
///   separately and matches a blank against zero only.
/// - A blank matches only an exact zero, not a number within the `1e-12` tolerance used between
///   two numbers (`=SWITCH(Z1,1E-13,...)` does not match).
/// - A trailing unmatched argument acts as the default result.
/// - When no candidate matches and no default is supplied, returns `#N/A`.
/// - Errors in `expression` propagate immediately.
///
/// # Examples
///
/// ```excel
/// =SWITCH("gold","silver",1,"gold",2,0)
/// ```
///
/// ```yaml,sandbox
/// title: "Match a text label"
/// formula: '=SWITCH("gold","silver",1,"gold",2,0)'
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Fall back to default"
/// formula: '=SWITCH(3,1,"one",2,"two","other")'
/// expected: "other"
/// ```
///
/// ```yaml,docs
/// related:
///   - IF
///   - IFS
///   - CHOOSE
/// faq:
///   - q: "Does SWITCH compare text case-sensitively?"
///     a: "No. Text comparisons are case-insensitive in this implementation."
///   - q: "What happens when nothing matches?"
///     a: "SWITCH returns the trailing default when provided; otherwise it returns #N/A."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: SWITCH
/// Type: SwitchFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: SWITCH(arg1: any@scalar, arg2...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
#[derive(Debug)]
pub struct SwitchFn;
/// [formualizer-docgen:schema:start]
/// Name: SWITCH
/// Type: SwitchFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: SWITCH(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, SHORT_CIRCUIT
/// [formualizer-docgen:schema:end]
impl Function for SwitchFn {
    fn propagate_format(
        &self,
        result: &crate::traits::CalcValue<'_>,
    ) -> Option<crate::format::FormatId> {
        result.format_id()
    }

    func_caps!(PURE, SHORT_CIRCUIT, MAY_SPILL);
    fn name(&self) -> &'static str {
        "SWITCH"
    }
    fn min_args(&self) -> usize {
        3
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
        if args.len() < 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }
        let expr = args[0].value()?.into_literal();
        if let LiteralValue::Error(e) = &expr {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                e.clone(),
            )));
        }
        // args[1..] are value/result pairs with optional trailing default
        let rest = &args[1..];
        let has_default = rest.len() % 2 == 1;
        let pairs = if has_default {
            rest.len() - 1
        } else {
            rest.len()
        };
        for chunk in rest[..pairs].chunks(2) {
            let candidate = chunk[0].value()?.into_literal();
            if switch_values_equal(&expr, &candidate) {
                return chunk[1].value();
            }
        }
        if has_default {
            return rest.last().unwrap().value();
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_na(),
        )))
    }
}

fn switch_values_equal(a: &LiteralValue, b: &LiteralValue) -> bool {
    match (a, b) {
        (LiteralValue::Number(x), LiteralValue::Number(y)) => (x - y).abs() < 1e-12,
        (LiteralValue::Int(x), LiteralValue::Int(y)) => x == y,
        (LiteralValue::Number(x), LiteralValue::Int(y)) => (x - *y as f64).abs() < 1e-12,
        (LiteralValue::Int(x), LiteralValue::Number(y)) => (*x as f64 - y).abs() < 1e-12,
        (LiteralValue::Boolean(x), LiteralValue::Boolean(y)) => x == y,
        (LiteralValue::Text(x), LiteralValue::Text(y)) => x.eq_ignore_ascii_case(y),
        (LiteralValue::Empty, LiteralValue::Empty) => true,
        // A blank on either side is numeric zero, as in Excel: a blank
        // selector matches a `0` case and a blank case matches a `0` selector.
        // It does not match empty text or FALSE, unlike the `=` operator's
        // blank rule (Excel's SWITCH was measured separately). The test is an
        // exact zero, without the number-to-number tolerance above; `-0.0`
        // also matches (Excel stores a typed `-0` as 0).
        (LiteralValue::Empty, LiteralValue::Int(n))
        | (LiteralValue::Int(n), LiteralValue::Empty) => *n == 0,
        (LiteralValue::Empty, LiteralValue::Number(n))
        | (LiteralValue::Number(n), LiteralValue::Empty) => *n == 0.0,
        _ => false,
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_builtin(Arc::new(NotFn));
    crate::function_registry::register_builtin(Arc::new(XorFn));
    crate::function_registry::register_builtin(Arc::new(IfErrorFn));
    crate::function_registry::register_builtin(Arc::new(IfNaFn));
    crate::function_registry::register_builtin(Arc::new(IfsFn));
    crate::function_registry::register_builtin(Arc::new(SwitchFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::{ExcelErrorKind, LiteralValue};
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
    fn switch_match_and_default() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SwitchFn));
        let ctx = interp(&wb);
        let expr = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(2)), None);
        let c1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let v1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("a".into())), None);
        let c2 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(2)), None);
        let v2 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("b".into())), None);
        let f = ctx.context.get_function("", "SWITCH").unwrap();
        let args = vec![
            ArgumentHandle::new(&expr, &ctx),
            ArgumentHandle::new(&c1, &ctx),
            ArgumentHandle::new(&v1, &ctx),
            ArgumentHandle::new(&c2, &ctx),
            ArgumentHandle::new(&v2, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Text("b".into())
        );
    }

    #[test]
    fn switch_blank_equals_numeric_zero_on_either_side() {
        for zero in [
            LiteralValue::Int(0),
            LiteralValue::Number(0.0),
            LiteralValue::Number(-0.0),
        ] {
            assert!(switch_values_equal(&LiteralValue::Empty, &zero), "{zero:?}");
            assert!(switch_values_equal(&zero, &LiteralValue::Empty), "{zero:?}");
        }
    }

    #[test]
    fn switch_blank_does_not_equal_empty_text_false_or_nonzero() {
        for other in [
            LiteralValue::Text(String::new()),
            LiteralValue::Text("0".into()),
            LiteralValue::Boolean(false),
            LiteralValue::Int(1),
            LiteralValue::Number(1e-13),
        ] {
            assert!(
                !switch_values_equal(&LiteralValue::Empty, &other),
                "{other:?}"
            );
            assert!(
                !switch_values_equal(&other, &LiteralValue::Empty),
                "{other:?}"
            );
        }
    }

    #[test]
    fn switch_no_match_no_default_returns_na() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SwitchFn));
        let ctx = interp(&wb);
        let expr = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(99)), None);
        let c1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let v1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("a".into())), None);
        let f = ctx.context.get_function("", "SWITCH").unwrap();
        let args = vec![
            ArgumentHandle::new(&expr, &ctx),
            ArgumentHandle::new(&c1, &ctx),
            ArgumentHandle::new(&v1, &ctx),
        ];
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
    fn switch_with_default() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SwitchFn));
        let ctx = interp(&wb);
        let expr = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(99)), None);
        let c1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let v1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("a".into())), None);
        let def = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Text("default".into())),
            None,
        );
        let f = ctx.context.get_function("", "SWITCH").unwrap();
        let args = vec![
            ArgumentHandle::new(&expr, &ctx),
            ArgumentHandle::new(&c1, &ctx),
            ArgumentHandle::new(&v1, &ctx),
            ArgumentHandle::new(&def, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Text("default".into())
        );
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

    #[test]
    fn ifs_and_switch_propagate_condition_error() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(IfsFn))
            .with_function(std::sync::Arc::new(SwitchFn));
        let ctx = interp(&wb);
        let error = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::new_na())),
            None,
        );
        let one = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let two = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(2)), None);

        let ifs = ctx.context.get_function("", "IFS").unwrap();
        let ifs_value = ifs
            .dispatch(
                &[
                    ArgumentHandle::new(&error, &ctx),
                    ArgumentHandle::new(&one, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert!(
            matches!(ifs_value, LiteralValue::Error(ref e) if e.kind == ExcelErrorKind::Na),
            "IFS must preserve the condition error, got {ifs_value:?}"
        );

        let switch = ctx.context.get_function("", "SWITCH").unwrap();
        let switch_value = switch
            .dispatch(
                &[
                    ArgumentHandle::new(&error, &ctx),
                    ArgumentHandle::new(&one, &ctx),
                    ArgumentHandle::new(&two, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert!(
            matches!(switch_value, LiteralValue::Error(ref e) if e.kind == ExcelErrorKind::Na),
            "SWITCH must preserve the expression error, got {switch_value:?}"
        );
    }
}

#[cfg(test)]
mod error_guard_tests {
    use super::*;
    use std::cell::Cell;

    fn num(n: f64) -> LiteralValue {
        LiteralValue::Number(n)
    }

    fn error(kind: ExcelErrorKind) -> LiteralValue {
        LiteralValue::Error(ExcelError::new(kind))
    }

    #[test]
    fn shared_cap_applies_to_output_shape() {
        assert!(materialized_shape_too_large((4096, 4096)).is_none());
        assert!(materialized_shape_too_large((4096, 4097)).is_some());
        assert!(materialized_shape_too_large((1_048_576, 1)).is_none());
        assert!(materialized_shape_too_large((1_048_577, 1)).is_some());
        assert!(materialized_shape_too_large((1, 16_385)).is_some());
        assert!(materialized_shape_too_large((usize::MAX, usize::MAX)).is_some());
    }

    #[test]
    fn over_cap_selection_is_num_without_building_output() {
        // 4097 x 1 against 1 x 4097: rejected from shapes alone, and before
        // the first cancellation poll (nothing is visited).
        let polls = Cell::new(0usize);
        let is_cancelled = || {
            polls.set(polls.get() + 1);
            false
        };
        let value = Grid::Array(vec![vec![error(ExcelErrorKind::Div)]; 4097]);
        let fallback = Grid::Array(vec![vec![num(0.0); 4097]]);
        let result = select_elementwise(
            &value,
            &fallback,
            ErrorGuard::AnyError,
            &mut CancelPoll::new(&is_cancelled),
        )
        .unwrap();
        assert_eq!(result.unwrap_err().kind, ExcelErrorKind::Num);
        assert_eq!(polls.get(), 0);
    }

    #[test]
    fn incompatible_shapes_are_value_error() {
        let is_cancelled = || false;
        let value = Grid::Array(vec![vec![error(ExcelErrorKind::Na); 3]]);
        let fallback = Grid::Array(vec![vec![num(1.0), num(2.0)]]);
        let result = select_elementwise(
            &value,
            &fallback,
            ErrorGuard::NaOnly,
            &mut CancelPoll::new(&is_cancelled),
        )
        .unwrap();
        assert_eq!(result.unwrap_err().kind, ExcelErrorKind::Value);
    }

    /// Cancellation raised part-way through one wide row is observed inside
    /// that row, not only at row boundaries.
    #[test]
    fn wide_row_copy_is_interrupted_mid_row() {
        let polls = Cell::new(0usize);
        let is_cancelled = || {
            polls.set(polls.get() + 1);
            polls.get() >= 3
        };
        let mut row = vec![num(1.0); 16_384];
        row[0] = error(ExcelErrorKind::Div);
        let value = Grid::Array(vec![row]);
        let result = select_elementwise(
            &value,
            &Grid::Scalar(num(0.0)),
            ErrorGuard::AnyError,
            &mut CancelPoll::new(&is_cancelled),
        );
        assert_eq!(result.unwrap_err().kind, ExcelErrorKind::Cancelled);
        assert_eq!(polls.get(), 3);
        // The row spans several poll strides, so poll 3 lands mid-row.
        const _: () = assert!(16_384 / CANCEL_POLL_CELLS >= 3);
    }

    #[test]
    fn wide_row_scan_is_interrupted_mid_row() {
        let polls = Cell::new(0usize);
        let is_cancelled = || {
            polls.set(polls.get() + 1);
            polls.get() >= 2
        };
        // Clean row: the scan must not report "no match" once cancelled.
        let rows = vec![vec![num(1.0); 16_384]];
        let result = array_has_match(
            &rows,
            ErrorGuard::AnyError,
            &mut CancelPoll::new(&is_cancelled),
        );
        assert_eq!(result.unwrap_err().kind, ExcelErrorKind::Cancelled);
        assert_eq!(polls.get(), 2);
    }

    #[test]
    fn view_probe_reads_error_lanes_by_guard() {
        let is_cancelled = || false;
        let view = RangeView::from_owned_rows(
            vec![
                vec![num(1.0), LiteralValue::Text("x".into())],
                vec![error(ExcelErrorKind::Div), LiteralValue::Empty],
            ],
            crate::engine::DateSystem::Excel1900,
        );
        let any = view_has_match(
            &view,
            ErrorGuard::AnyError,
            &mut CancelPoll::new(&is_cancelled),
        );
        let na = view_has_match(
            &view,
            ErrorGuard::NaOnly,
            &mut CancelPoll::new(&is_cancelled),
        );
        assert!(any.unwrap());
        assert!(!na.unwrap());

        let clean = RangeView::from_owned_rows(
            vec![vec![num(1.0)], vec![LiteralValue::Boolean(true)]],
            crate::engine::DateSystem::Excel1900,
        );
        assert!(
            !view_has_match(
                &clean,
                ErrorGuard::AnyError,
                &mut CancelPoll::new(&is_cancelled)
            )
            .unwrap()
        );
    }

    /// 32,768 rows in one chunk whose error lane comes from a dense computed
    /// overlay of `#DIV/0!` (no `#N/A`), so the IFNA scan must read every lane.
    fn dense_div_sheet() -> crate::arrow_store::ArrowSheet {
        use crate::arrow_store::{IngestBuilder, OverlayFragment, OverlayValue};
        let rows = 32_768;
        let mut ingest = IngestBuilder::new("S", 1, rows, crate::engine::DateSystem::Excel1900);
        for _ in 0..rows {
            ingest.append_row(&[num(1.0)]).unwrap();
        }
        let mut sheet = ingest.finish();
        let dense = vec![OverlayValue::Error(map_error_code(ExcelErrorKind::Div)); rows];
        sheet
            .ensure_column_chunk_mut(0, 0)
            .unwrap()
            .computed_overlay
            .apply_fragment(OverlayFragment::dense_range(0, dense).unwrap());
        sheet
    }

    #[test]
    fn ifna_lane_scan_over_dense_overlay_is_polled_per_bounded_piece() {
        use crate::engine::range_view::range_work;
        let sheet = dense_div_sheet();
        let view = sheet.range_view(0, 0, 32_767, 0);

        // Clean for IFNA after scanning all eight 4096-row pieces.
        let is_cancelled = || false;
        range_work::begin();
        let found = view_has_match(
            &view,
            ErrorGuard::NaOnly,
            &mut CancelPoll::new(&is_cancelled),
        );
        let work = range_work::take();
        assert!(!found.unwrap());
        assert_eq!(work.error_pieces, 8);
        assert_eq!(work.error_piece_max_rows, CANCEL_POLL_CELLS);

        // Poll 1 is the walk start and poll 2 precedes the scan of piece 2;
        // the third poll (before scanning piece 3) reports cancellation, so
        // piece 3 is never scanned and pieces 4..8 are never prepared.
        let polls = Cell::new(0usize);
        let is_cancelled = || {
            polls.set(polls.get() + 1);
            polls.get() >= 3
        };
        range_work::begin();
        let result = view_has_match(
            &view,
            ErrorGuard::NaOnly,
            &mut CancelPoll::new(&is_cancelled),
        );
        let work = range_work::take();
        assert_eq!(result.unwrap_err().kind, ExcelErrorKind::Cancelled);
        assert_eq!(polls.get(), 3);
        assert_eq!(work.error_pieces, 3);

        // The request token attached to the view is checked before each piece
        // is prepared: cancelled while piece 2 is scanned, piece 3 is never
        // built.
        let token = CancelToken::new();
        let tokened = view.clone().with_cancel_token(Some(token.clone()));
        let polls = Cell::new(0usize);
        let is_cancelled = || {
            polls.set(polls.get() + 1);
            if polls.get() == 2 {
                token.cancel();
            }
            false
        };
        range_work::begin();
        let result = view_has_match(
            &tokened,
            ErrorGuard::NaOnly,
            &mut CancelPoll::new(&is_cancelled),
        );
        let work = range_work::take();
        assert_eq!(result.unwrap_err().kind, ExcelErrorKind::Cancelled);
        assert_eq!(work.error_pieces, 2);
    }

    #[test]
    fn view_probe_propagates_iterator_cancellation() {
        // The view's own token is honoured by segment iteration even when the
        // poll closure never reports cancellation.
        let token = CancelToken::new();
        token.cancel();
        let is_cancelled = || false;
        let view =
            RangeView::from_owned_rows(vec![vec![num(1.0)]], crate::engine::DateSystem::Excel1900)
                .with_cancel_token(Some(token));
        let result = view_has_match(
            &view,
            ErrorGuard::AnyError,
            &mut CancelPoll::new(&is_cancelled),
        );
        assert_eq!(result.unwrap_err().kind, ExcelErrorKind::Cancelled);
    }
}
