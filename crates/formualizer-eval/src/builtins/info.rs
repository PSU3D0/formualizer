use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;

use super::utils::ARG_ANY_ONE;

/* Info and type-introspection builtins for spreadsheet formulas. */

#[derive(Debug)]
pub struct IsNumberFn;
/// Returns TRUE when the value is numeric.
///
/// This includes integer, floating-point, and temporal serial-compatible values.
///
/// # Remarks
/// - Returns TRUE for `Int`, `Number`, `Date`, `DateTime`, `Time`, and `Duration`.
/// - Text that looks numeric is still text and returns FALSE.
/// - Errors are treated as non-numeric and return FALSE.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Number is numeric"
/// formula: '=ISNUMBER(42)'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Numeric text is not numeric"
/// formula: '=ISNUMBER("42")'
/// expected: false
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ISNUMBER
/// Type: IsNumberFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISNUMBER(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsNumberFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISNUMBER"
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
        let is_num = matches!(
            v,
            LiteralValue::Int(_)
                | LiteralValue::Number(_)
                | LiteralValue::Date(_)
                | LiteralValue::DateTime(_)
                | LiteralValue::Time(_)
                | LiteralValue::Duration(_)
        );
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            is_num,
        )))
    }
}

#[derive(Debug)]
pub struct IsTextFn;
/// Returns TRUE when the value is text.
///
/// # Remarks
/// - Only text literals return TRUE.
/// - Numbers, booleans, blanks, and errors return FALSE.
/// - No coercion from other types to text is performed for this check.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Detect text"
/// formula: '=ISTEXT("alpha")'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Number is not text"
/// formula: '=ISTEXT(100)'
/// expected: false
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ISTEXT
/// Type: IsTextFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISTEXT(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsTextFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISTEXT"
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
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            matches!(v, LiteralValue::Text(_)),
        )))
    }
}

#[derive(Debug)]
pub struct IsLogicalFn;
/// Returns TRUE when the value is a boolean.
///
/// # Remarks
/// - Only logical TRUE/FALSE values return TRUE.
/// - Numeric truthy/falsy values are not considered logical by this predicate.
/// - Errors return FALSE.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Boolean input"
/// formula: '=ISLOGICAL(TRUE)'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Numeric input"
/// formula: '=ISLOGICAL(1)'
/// expected: false
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ISLOGICAL
/// Type: IsLogicalFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISLOGICAL(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsLogicalFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISLOGICAL"
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
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            matches!(v, LiteralValue::Boolean(_)),
        )))
    }
}

#[derive(Debug)]
pub struct IsBlankFn;
/// Returns TRUE only for a truly empty value.
///
/// # Remarks
/// - Empty string `""` is text, not blank, so it returns FALSE.
/// - Numeric zero and FALSE are not blank.
/// - Errors return FALSE.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Reference to an empty cell"
/// formula: '=ISBLANK(A1)'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Empty string is not blank"
/// formula: '=ISBLANK("")'
/// expected: false
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ISBLANK
/// Type: IsBlankFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISBLANK(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsBlankFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISBLANK"
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
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            matches!(v, LiteralValue::Empty),
        )))
    }
}

#[derive(Debug)]
pub struct IsErrorFn; // TRUE for any error (#N/A included)
/// Returns TRUE for any error value.
///
/// # Remarks
/// - Matches all error kinds, including `#N/A`.
/// - Non-error values always return FALSE.
/// - Arity mismatch returns `#VALUE!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Division error"
/// formula: '=ISERROR(1/0)'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Normal value"
/// formula: '=ISERROR(123)'
/// expected: false
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ISERROR
/// Type: IsErrorFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISERROR(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsErrorFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISERROR"
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
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            matches!(v, LiteralValue::Error(_)),
        )))
    }
}

#[derive(Debug)]
pub struct IsErrFn; // TRUE for any error except #N/A
/// Returns TRUE for any error except `#N/A`.
///
/// # Remarks
/// - `#N/A` specifically returns FALSE.
/// - Other errors such as `#DIV/0!` or `#VALUE!` return TRUE.
/// - Non-error values return FALSE.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "DIV/0 is an error excluding N/A"
/// formula: '=ISERR(1/0)'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "N/A is excluded"
/// formula: '=ISERR(NA())'
/// expected: false
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ISERR
/// Type: IsErrFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISERR(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsErrFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISERR"
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
        let is_err = match v {
            LiteralValue::Error(e) => e.kind != ExcelErrorKind::Na,
            _ => false,
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            is_err,
        )))
    }
}

#[derive(Debug)]
pub struct IsNaFn; // TRUE only for #N/A
/// Returns TRUE only for the `#N/A` error.
///
/// # Remarks
/// - Other error kinds return FALSE.
/// - Non-error values return FALSE.
/// - Useful when `#N/A` has special business meaning.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Check for N/A"
/// formula: '=ISNA(NA())'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Other errors are not N/A"
/// formula: '=ISNA(1/0)'
/// expected: false
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ISNA
/// Type: IsNaFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISNA(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsNaFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISNA"
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
        let is_na = matches!(v, LiteralValue::Error(e) if e.kind==ExcelErrorKind::Na);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            is_na,
        )))
    }
}

#[derive(Debug)]
pub struct IsFormulaFn; // Requires provenance tracking (not yet) => always FALSE.
/// Returns whether a value originates from a formula.
///
/// Current engine metadata does not track formula provenance at this call site.
///
/// # Remarks
/// - This implementation currently returns FALSE for all inputs.
/// - Errors are not raised solely due to provenance unavailability.
/// - Arity mismatch returns `#VALUE!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Literal value"
/// formula: '=ISFORMULA(10)'
/// expected: false
/// ```
///
/// ```yaml,sandbox
/// title: "Computed value in expression context"
/// formula: '=ISFORMULA(1+1)'
/// expected: false
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ISFORMULA
/// Type: IsFormulaFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISFORMULA(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsFormulaFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISFORMULA"
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
        // Formula provenance metadata is not tracked yet, so ISFORMULA currently returns FALSE.
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            false,
        )))
    }
}

#[derive(Debug)]
pub struct TypeFn;
/// Returns an Excel TYPE code describing the value category.
///
/// # Remarks
/// - Codes: `1` number, `2` text, `4` logical, `64` array.
/// - Errors are propagated unchanged instead of returning `16`.
/// - Blank values map to numeric code `1` in this implementation.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Text type code"
/// formula: '=TYPE("abc")'
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Boolean type code"
/// formula: '=TYPE(TRUE)'
/// expected: 4
/// ```
/// [formualizer-docgen:schema:start]
/// Name: TYPE
/// Type: TypeFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: TYPE(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TypeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TYPE"
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
        let v = args[0].value()?.into_literal(); // Propagate errors directly
        if let LiteralValue::Error(e) = v {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
        }
        let code = match v {
            LiteralValue::Int(_)
            | LiteralValue::Number(_)
            | LiteralValue::Empty
            | LiteralValue::Date(_)
            | LiteralValue::DateTime(_)
            | LiteralValue::Time(_)
            | LiteralValue::Duration(_) => 1,
            LiteralValue::Text(_) => 2,
            LiteralValue::Boolean(_) => 4,
            LiteralValue::Array(_) => 64,
            LiteralValue::Error(_) => unreachable!(),
            LiteralValue::Pending => 1, // treat as blank/zero numeric; may change
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(code)))
    }
}

#[derive(Debug)]
pub struct NaFn; // NA() -> #N/A error
/// Returns the `#N/A` error value.
///
/// # Remarks
/// - `NA()` is commonly used to mark missing lookup results.
/// - The function takes no arguments.
/// - The returned value is an error and propagates through dependent formulas.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Direct N/A"
/// formula: '=NA()'
/// expected: "#N/A"
/// ```
///
/// ```yaml,sandbox
/// title: "Detect N/A"
/// formula: '=ISNA(NA())'
/// expected: true
/// ```
/// [formualizer-docgen:schema:start]
/// Name: NA
/// Type: NaFn
/// Min args: 0
/// Max args: 0
/// Variadic: false
/// Signature: NA()
/// Arg schema: []
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NaFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NA"
    }
    fn min_args(&self) -> usize {
        0
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Na),
        )))
    }
}

#[derive(Debug)]
pub struct NFn; // N(value)
/// Converts a value to its numeric representation.
///
/// # Remarks
/// - Numbers pass through unchanged; booleans convert to `1`/`0`.
/// - Text and blank values convert to `0`.
/// - Errors propagate unchanged.
/// - Temporal values are converted using serial number representation.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Boolean to number"
/// formula: '=N(TRUE)'
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Text to zero"
/// formula: '=N("hello")'
/// expected: 0
/// ```
/// [formualizer-docgen:schema:start]
/// Name: N
/// Type: NFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: N(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "N"
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
        match v {
            LiteralValue::Int(i) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(i))),
            LiteralValue::Number(n) => {
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(n)))
            }
            LiteralValue::Date(_)
            | LiteralValue::DateTime(_)
            | LiteralValue::Time(_)
            | LiteralValue::Duration(_) => {
                // Convert via serial number helper
                if let Some(serial) = v.as_serial_number() {
                    Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                        serial,
                    )))
                } else {
                    Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
                }
            }
            LiteralValue::Boolean(b) => {
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(if b {
                    1
                } else {
                    0
                })))
            }
            LiteralValue::Text(_) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0))),
            LiteralValue::Empty => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0))),
            LiteralValue::Array(_) => {
                // Array-to-scalar implicit intersection is not implemented here; returns 0.
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0)))
            }
            LiteralValue::Error(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            LiteralValue::Pending => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(0))),
        }
    }
}

#[derive(Debug)]
pub struct TFn; // T(value)
/// Returns text when input is text, otherwise returns empty text.
///
/// # Remarks
/// - Text values pass through unchanged.
/// - Errors propagate unchanged.
/// - Numbers, booleans, and blanks return an empty string.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Text passthrough"
/// formula: '=T("report")'
/// expected: "report"
/// ```
///
/// ```yaml,sandbox
/// title: "Number becomes empty text"
/// formula: '=T(99)'
/// expected: ""
/// ```
/// [formualizer-docgen:schema:start]
/// Name: T
/// Type: TFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: T(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "T"
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
        match v {
            LiteralValue::Text(s) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(s))),
            LiteralValue::Error(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            _ => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
                String::new(),
            ))),
        }
    }
}

/// ISEVEN(number) - Returns TRUE if number is even
#[derive(Debug)]
pub struct IsEvenFn;
/// Returns TRUE when a number is even.
///
/// # Remarks
/// - Numeric input is truncated toward zero before parity is checked.
/// - Booleans are coerced (`TRUE` -> 1, `FALSE` -> 0).
/// - Non-numeric text returns `#VALUE!`.
/// - Errors propagate unchanged.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Even integer"
/// formula: '=ISEVEN(6)'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Decimal truncation before parity"
/// formula: '=ISEVEN(3.9)'
/// expected: false
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ISEVEN
/// Type: IsEvenFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISEVEN(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsEvenFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISEVEN"
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
        let n = match v {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            LiteralValue::Int(i) => i as f64,
            LiteralValue::Number(n) => n,
            LiteralValue::Boolean(b) => {
                if b {
                    1.0
                } else {
                    0.0
                }
            }
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
        };
        // Excel truncates to integer first
        let n = n.trunc() as i64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            n % 2 == 0,
        )))
    }
}

/// ISODD(number) - Returns TRUE if number is odd
#[derive(Debug)]
pub struct IsOddFn;
/// Returns TRUE when a number is odd.
///
/// # Remarks
/// - Numeric input is truncated toward zero before parity is checked.
/// - Booleans are coerced (`TRUE` -> 1, `FALSE` -> 0).
/// - Non-numeric text returns `#VALUE!`.
/// - Errors propagate unchanged.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Odd integer"
/// formula: '=ISODD(7)'
/// expected: true
/// ```
///
/// ```yaml,sandbox
/// title: "Boolean coercion"
/// formula: '=ISODD(TRUE)'
/// expected: true
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ISODD
/// Type: IsOddFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ISODD(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IsOddFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ISODD"
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
        let n = match v {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            LiteralValue::Int(i) => i as f64,
            LiteralValue::Number(n) => n,
            LiteralValue::Boolean(b) => {
                if b {
                    1.0
                } else {
                    0.0
                }
            }
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
        };
        let n = n.trunc() as i64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Boolean(
            n % 2 != 0,
        )))
    }
}

/// ERROR.TYPE(error_val) - Returns a number corresponding to an error type
/// Returns:
///   1 = #NULL!
///   2 = #DIV/0!
///   3 = #VALUE!
///   4 = #REF!
///   5 = #NAME?
///   6 = #NUM!
///   7 = #N/A
///   8 = #GETTING_DATA (not commonly used)
///   #N/A if the value is not an error
///
/// NOTE: Error codes 9-13 are non-standard extensions for internal error types.
#[derive(Debug)]
pub struct ErrorTypeFn;
/// Returns the numeric code for a specific error value.
///
/// # Remarks
/// - Standard mappings include: `#NULL!`=1, `#DIV/0!`=2, `#VALUE!`=3, `#REF!`=4, `#NAME?`=5, `#NUM!`=6, `#N/A`=7.
/// - Non-error inputs return `#N/A`.
/// - Additional internal error kinds may map to extended non-standard codes.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Map DIV/0 to code"
/// formula: '=ERROR.TYPE(1/0)'
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Non-error input returns N/A"
/// formula: '=ERROR.TYPE(10)'
/// expected: "#N/A"
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ERROR.TYPE
/// Type: ErrorTypeFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ERROR.TYPE(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ErrorTypeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ERROR.TYPE"
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
        match v {
            LiteralValue::Error(e) => {
                let code = match e.kind {
                    ExcelErrorKind::Null => 1,
                    ExcelErrorKind::Div => 2,
                    ExcelErrorKind::Value => 3,
                    ExcelErrorKind::Ref => 4,
                    ExcelErrorKind::Name => 5,
                    ExcelErrorKind::Num => 6,
                    ExcelErrorKind::Na => 7,
                    ExcelErrorKind::Error => 8,
                    // Non-standard extensions (codes 9-13)
                    ExcelErrorKind::NImpl => 9,
                    ExcelErrorKind::Spill => 10,
                    ExcelErrorKind::Calc => 11,
                    ExcelErrorKind::Circ => 12,
                    ExcelErrorKind::Cancelled => 13,
                };
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(code)))
            }
            _ => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            ))),
        }
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(IsNumberFn));
    crate::function_registry::register_function(Arc::new(IsTextFn));
    crate::function_registry::register_function(Arc::new(IsLogicalFn));
    crate::function_registry::register_function(Arc::new(IsBlankFn));
    crate::function_registry::register_function(Arc::new(IsErrorFn));
    crate::function_registry::register_function(Arc::new(IsErrFn));
    crate::function_registry::register_function(Arc::new(IsNaFn));
    crate::function_registry::register_function(Arc::new(IsFormulaFn));
    crate::function_registry::register_function(Arc::new(IsEvenFn));
    crate::function_registry::register_function(Arc::new(IsOddFn));
    crate::function_registry::register_function(Arc::new(ErrorTypeFn));
    crate::function_registry::register_function(Arc::new(TypeFn));
    crate::function_registry::register_function(Arc::new(NaFn));
    crate::function_registry::register_function(Arc::new(NFn));
    crate::function_registry::register_function(Arc::new(TFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn isnumber_numeric_and_date() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IsNumberFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ISNUMBER").unwrap();
        let num = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Number(std::f64::consts::PI)),
            None,
        );
        let date = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Date(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            )),
            None,
        );
        let txt = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("x".into())), None);
        let args_num = vec![crate::traits::ArgumentHandle::new(&num, &ctx)];
        let args_date = vec![crate::traits::ArgumentHandle::new(&date, &ctx)];
        let args_txt = vec![crate::traits::ArgumentHandle::new(&txt, &ctx)];
        assert_eq!(
            f.dispatch(&args_num, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&args_date, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&args_txt, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn istest_and_isblank() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IsTextFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ISTEXT").unwrap();
        let t = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("abc".into())), None);
        let n = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(5)), None);
        let args_t = vec![crate::traits::ArgumentHandle::new(&t, &ctx)];
        let args_n = vec![crate::traits::ArgumentHandle::new(&n, &ctx)];
        assert_eq!(
            f.dispatch(&args_t, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&args_n, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(false)
        );

        // ISBLANK
        let wb2 = TestWorkbook::new().with_function(std::sync::Arc::new(IsBlankFn));
        let ctx2 = interp(&wb2);
        let f2 = ctx2.context.get_function("", "ISBLANK").unwrap();
        let blank = ASTNode::new(ASTNodeType::Literal(LiteralValue::Empty), None);
        let blank_args = vec![crate::traits::ArgumentHandle::new(&blank, &ctx2)];
        assert_eq!(
            f2.dispatch(&blank_args, &ctx2.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(true)
        );
    }

    #[test]
    fn iserror_variants() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(IsErrorFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ISERROR").unwrap();
        let err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Div))),
            None,
        );
        let ok = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let a_err = vec![crate::traits::ArgumentHandle::new(&err, &ctx)];
        let a_ok = vec![crate::traits::ArgumentHandle::new(&ok, &ctx)];
        assert_eq!(
            f.dispatch(&a_err, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            f.dispatch(&a_ok, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn type_codes_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TypeFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "TYPE").unwrap();
        let v_num = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(2.0)), None);
        let v_txt = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("hi".into())), None);
        let v_bool = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(true)), None);
        let v_err = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value))),
            None,
        );
        let v_arr = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![LiteralValue::Int(1)]])),
            None,
        );
        let a_num = vec![crate::traits::ArgumentHandle::new(&v_num, &ctx)];
        let a_txt = vec![crate::traits::ArgumentHandle::new(&v_txt, &ctx)];
        let a_bool = vec![crate::traits::ArgumentHandle::new(&v_bool, &ctx)];
        let a_err = vec![crate::traits::ArgumentHandle::new(&v_err, &ctx)];
        let a_arr = vec![crate::traits::ArgumentHandle::new(&v_arr, &ctx)];
        assert_eq!(
            f.dispatch(&a_num, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(1)
        );
        assert_eq!(
            f.dispatch(&a_txt, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(2)
        );
        assert_eq!(
            f.dispatch(&a_bool, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(4)
        );
        match f
            .dispatch(&a_err, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#VALUE!"),
            _ => panic!(),
        }
        assert_eq!(
            f.dispatch(&a_arr, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(64)
        );
    }

    #[test]
    fn na_and_n_and_t() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(NaFn))
            .with_function(std::sync::Arc::new(NFn))
            .with_function(std::sync::Arc::new(TFn));
        let ctx = wb.interpreter();
        // NA()
        let na_fn = ctx.context.get_function("", "NA").unwrap();
        match na_fn
            .eval(&[], &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#N/A"),
            _ => panic!(),
        }
        // N()
        let n_fn = ctx.context.get_function("", "N").unwrap();
        let val = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(true)), None);
        let args = vec![crate::traits::ArgumentHandle::new(&val, &ctx)];
        assert_eq!(
            n_fn.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Int(1)
        );
        // T()
        let t_fn = ctx.context.get_function("", "T").unwrap();
        let txt = ASTNode::new(ASTNodeType::Literal(LiteralValue::Text("abc".into())), None);
        let args_t = vec![crate::traits::ArgumentHandle::new(&txt, &ctx)];
        assert_eq!(
            t_fn.dispatch(&args_t, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Text("abc".into())
        );
    }
}
