//! Extended text functions: CLEAN, UNICHAR, UNICODE, TEXTBEFORE, TEXTAFTER, TEXTSPLIT, DOLLAR, FIXED

use super::super::utils::{ARG_ANY_ONE, coerce_num};
use crate::args::{ArgSchema, ShapeKind};
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ArgKind, CoercionPolicy, ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;

fn scalar_like_value(arg: &ArgumentHandle<'_, '_>) -> Result<LiteralValue, ExcelError> {
    Ok(match arg.value()? {
        CalcValue::Scalar(v) => v,
        CalcValue::Range(rv) => rv.get_cell(0, 0),
        CalcValue::Callable(_) => LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Calc).with_message("LAMBDA value must be invoked"),
        ),
    })
}

/// Coerce a LiteralValue to text
fn coerce_text(v: &LiteralValue) -> String {
    match v {
        LiteralValue::Text(s) => s.clone(),
        LiteralValue::Empty => String::new(),
        LiteralValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Number(f) => {
            let s = f.to_string();
            if s.ends_with(".0") {
                s[..s.len() - 2].to_string()
            } else {
                s
            }
        }
        other => other.to_string(),
    }
}

// ============================================================================
// CLEAN - Remove non-printable characters (ASCII 0-31)
// ============================================================================

#[derive(Debug)]
pub struct CleanFn;
/// Removes non-printable ASCII control characters from text.
///
/// # Remarks
/// - Characters with codes `0..31` are removed.
/// - Printable whitespace like regular spaces is preserved.
/// - Non-text inputs are coerced to text before cleaning.
/// - Errors are propagated unchanged.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Strip control characters"
/// formula: '=CLEAN("A"&CHAR(10)&"B")'
/// expected: "AB"
/// ```
///
/// ```yaml,sandbox
/// title: "Printable spaces remain"
/// formula: '=CLEAN("A B")'
/// expected: "A B"
/// ```
/// [formualizer-docgen:schema:start]
/// Name: CLEAN
/// Type: CleanFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: CLEAN(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for CleanFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CLEAN"
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
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let v = scalar_like_value(&args[0])?;
        let text = match v {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_text(&other),
        };

        // Remove non-printable characters (ASCII 0-31)
        let cleaned: String = text.chars().filter(|&c| c as u32 >= 32).collect();
        Ok(CalcValue::Scalar(LiteralValue::Text(cleaned)))
    }
}

// ============================================================================
// UNICHAR - Return Unicode character from code point
// ============================================================================

#[derive(Debug)]
pub struct UnicharFn;
/// Returns the Unicode character for a given code point.
///
/// # Remarks
/// - Input is truncated to an integer code point.
/// - Code point `0`, surrogate range, and values above `0x10FFFF` return `#VALUE!`.
/// - Errors are propagated unchanged.
/// - Non-numeric inputs are coerced with numeric coercion rules.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Basic Unicode code point"
/// formula: '=UNICHAR(9731)'
/// expected: "☃"
/// ```
///
/// ```yaml,sandbox
/// title: "Invalid code point"
/// formula: '=UNICHAR(0)'
/// expected: "#VALUE!"
/// ```
/// [formualizer-docgen:schema:start]
/// Name: UNICHAR
/// Type: UnicharFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: UNICHAR(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for UnicharFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "UNICHAR"
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
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let v = scalar_like_value(&args[0])?;
        let n = match v {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_num(&other)?,
        };

        let code = n.trunc() as u32;

        // Valid Unicode range (excluding surrogates)
        if code == 0 || (0xD800..=0xDFFF).contains(&code) || code > 0x10FFFF {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        match char::from_u32(code) {
            Some(c) => Ok(CalcValue::Scalar(LiteralValue::Text(c.to_string()))),
            None => Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            ))),
        }
    }
}

// ============================================================================
// UNICODE - Return Unicode code point of first character
// ============================================================================

#[derive(Debug)]
pub struct UnicodeFn;
/// Returns the Unicode code point of the first character in text.
///
/// # Remarks
/// - Only the first character is evaluated.
/// - Empty text returns `#VALUE!`.
/// - Non-text inputs are coerced to text before inspection.
/// - Errors are propagated unchanged.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Code point for letter A"
/// formula: '=UNICODE("A")'
/// expected: 65
/// ```
///
/// ```yaml,sandbox
/// title: "Code point for emoji"
/// formula: '=UNICODE("😀")'
/// expected: 128512
/// ```
/// [formualizer-docgen:schema:start]
/// Name: UNICODE
/// Type: UnicodeFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: UNICODE(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for UnicodeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "UNICODE"
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
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let v = scalar_like_value(&args[0])?;
        let text = match v {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_text(&other),
        };

        if text.is_empty() {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let code = text.chars().next().unwrap() as u32;
        Ok(CalcValue::Scalar(LiteralValue::Number(code as f64)))
    }
}

// ============================================================================
// TEXTBEFORE - Return text before a delimiter
// ============================================================================

fn arg_textbefore() -> Vec<ArgSchema> {
    vec![
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Any],
            required: true,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::None,
            max: None,
            repeating: None,
            default: None,
        },
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Any],
            required: true,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::None,
            max: None,
            repeating: None,
            default: None,
        },
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Number],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberLenientText,
            max: None,
            repeating: None,
            default: Some(LiteralValue::Number(1.0)),
        },
    ]
}

#[derive(Debug)]
pub struct TextBeforeFn;
/// Returns text that appears before a delimiter.
///
/// # Remarks
/// - Delimiter matching is case-sensitive.
/// - `instance_num` defaults to `1`; negative instances search from the end.
/// - `instance_num=0` or empty delimiter returns `#VALUE!`.
/// - If requested delimiter occurrence is not found, returns `#N/A`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Text before first delimiter"
/// formula: '=TEXTBEFORE("a-b-c", "-")'
/// expected: "a"
/// ```
///
/// ```yaml,sandbox
/// title: "Text before last delimiter"
/// formula: '=TEXTBEFORE("a-b-c", "-", -1)'
/// expected: "a-b"
/// ```
/// [formualizer-docgen:schema:start]
/// Name: TEXTBEFORE
/// Type: TextBeforeFn
/// Min args: 2
/// Max args: 3
/// Variadic: false
/// Signature: TEXTBEFORE(arg1: any@scalar, arg2: any@scalar, arg3?: number@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg3{kinds=number,required=false,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=true}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TextBeforeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TEXTBEFORE"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(arg_textbefore);
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let v1 = scalar_like_value(&args[0])?;
        let text = match v1 {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_text(&other),
        };

        let v2 = scalar_like_value(&args[1])?;
        let delimiter = match v2 {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_text(&other),
        };

        let instance = if args.len() >= 3 {
            match scalar_like_value(&args[2])? {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                other => coerce_num(&other)?.trunc() as i32,
            }
        } else {
            1
        };

        if delimiter.is_empty() {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        if instance == 0 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let result = if instance > 0 {
            // Find nth occurrence from start
            let mut pos = 0;
            let mut found_count = 0;
            for (idx, _) in text.match_indices(&delimiter) {
                found_count += 1;
                if found_count == instance {
                    pos = idx;
                    break;
                }
            }
            if found_count < instance {
                return Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new(
                    ExcelErrorKind::Na,
                ))));
            }
            text[..pos].to_string()
        } else {
            // Find nth occurrence from end
            let matches: Vec<_> = text.match_indices(&delimiter).collect();
            let idx = matches.len() as i32 + instance; // instance is negative
            if idx < 0 || idx as usize >= matches.len() {
                return Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new(
                    ExcelErrorKind::Na,
                ))));
            }
            text[..matches[idx as usize].0].to_string()
        };

        Ok(CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

// ============================================================================
// TEXTAFTER - Return text after a delimiter
// ============================================================================

#[derive(Debug)]
pub struct TextAfterFn;
/// Returns text that appears after a delimiter.
///
/// # Remarks
/// - Delimiter matching is case-sensitive.
/// - `instance_num` defaults to `1`; negative instances search from the end.
/// - `instance_num=0` or empty delimiter returns `#VALUE!`.
/// - If requested delimiter occurrence is not found, returns `#N/A`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Text after first delimiter"
/// formula: '=TEXTAFTER("a-b-c", "-")'
/// expected: "b-c"
/// ```
///
/// ```yaml,sandbox
/// title: "Text after last delimiter"
/// formula: '=TEXTAFTER("a-b-c", "-", -1)'
/// expected: "c"
/// ```
/// [formualizer-docgen:schema:start]
/// Name: TEXTAFTER
/// Type: TextAfterFn
/// Min args: 2
/// Max args: 3
/// Variadic: false
/// Signature: TEXTAFTER(arg1: any@scalar, arg2: any@scalar, arg3?: number@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg3{kinds=number,required=false,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=true}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TextAfterFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TEXTAFTER"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(arg_textbefore);
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let v1 = scalar_like_value(&args[0])?;
        let text = match v1 {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_text(&other),
        };

        let v2 = scalar_like_value(&args[1])?;
        let delimiter = match v2 {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_text(&other),
        };

        let instance = if args.len() >= 3 {
            match scalar_like_value(&args[2])? {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                other => coerce_num(&other)?.trunc() as i32,
            }
        } else {
            1
        };

        if delimiter.is_empty() {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        if instance == 0 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let result = if instance > 0 {
            // Find nth occurrence from start
            let mut end_pos = 0;
            let mut found_count = 0;
            for (idx, matched) in text.match_indices(&delimiter) {
                found_count += 1;
                if found_count == instance {
                    end_pos = idx + matched.len();
                    break;
                }
            }
            if found_count < instance {
                return Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new(
                    ExcelErrorKind::Na,
                ))));
            }
            text[end_pos..].to_string()
        } else {
            // Find nth occurrence from end
            let matches: Vec<_> = text.match_indices(&delimiter).collect();
            let idx = matches.len() as i32 + instance;
            if idx < 0 || idx as usize >= matches.len() {
                return Ok(CalcValue::Scalar(LiteralValue::Error(ExcelError::new(
                    ExcelErrorKind::Na,
                ))));
            }
            let (pos, matched) = matches[idx as usize];
            text[pos + matched.len()..].to_string()
        };

        Ok(CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

// ============================================================================
// DOLLAR - Format number as currency
// ============================================================================

fn arg_dollar() -> Vec<ArgSchema> {
    vec![
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Number],
            required: true,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberLenientText,
            max: None,
            repeating: None,
            default: None,
        },
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Number],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberLenientText,
            max: None,
            repeating: None,
            default: Some(LiteralValue::Number(2.0)),
        },
    ]
}

#[derive(Debug)]
pub struct DollarFn;
/// Formats a number as currency text.
///
/// # Remarks
/// - Default decimal places is `2` when omitted.
/// - Negative values are rendered in parentheses, such as `($1,234.00)`.
/// - Uses comma group separators and dollar symbol.
/// - Input coercion failures or propagated errors return an error.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Default currency formatting"
/// formula: '=DOLLAR(1234.5)'
/// expected: "$1,234.50"
/// ```
///
/// ```yaml,sandbox
/// title: "Negative value with zero decimals"
/// formula: '=DOLLAR(-999.4, 0)'
/// expected: "($999)"
/// ```
/// [formualizer-docgen:schema:start]
/// Name: DOLLAR
/// Type: DollarFn
/// Min args: 1
/// Max args: 2
/// Variadic: false
/// Signature: DOLLAR(arg1: number@scalar, arg2?: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=false,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=true}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for DollarFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DOLLAR"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(arg_dollar);
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let v = scalar_like_value(&args[0])?;
        let num = match v {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_num(&other)?,
        };

        let decimals = if args.len() >= 2 {
            match scalar_like_value(&args[1])? {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                other => coerce_num(&other)?.trunc() as i32,
            }
        } else {
            2
        };

        // Round to specified decimals
        let factor = 10f64.powi(decimals);
        let rounded = (num * factor).round() / factor;

        // Format with thousands separator and currency symbol
        let abs_val = rounded.abs();
        let decimals_usize = decimals.max(0) as usize;

        let formatted = if decimals >= 0 {
            format!("{:.prec$}", abs_val, prec = decimals_usize)
        } else {
            format!("{:.0}", abs_val)
        };

        // Add thousands separators
        let parts: Vec<&str> = formatted.split('.').collect();
        let int_part = parts[0];
        let dec_part = parts.get(1);

        let int_with_commas: String = int_part
            .chars()
            .rev()
            .enumerate()
            .flat_map(|(i, c)| {
                if i > 0 && i % 3 == 0 {
                    vec![',', c]
                } else {
                    vec![c]
                }
            })
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        let result = if let Some(dec) = dec_part {
            if rounded < 0.0 {
                format!("(${}.{})", int_with_commas, dec)
            } else {
                format!("${}.{}", int_with_commas, dec)
            }
        } else if rounded < 0.0 {
            format!("(${})", int_with_commas)
        } else {
            format!("${}", int_with_commas)
        };

        Ok(CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

// ============================================================================
// FIXED - Format number with fixed decimals
// ============================================================================

fn arg_fixed() -> Vec<ArgSchema> {
    vec![
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Number],
            required: true,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberLenientText,
            max: None,
            repeating: None,
            default: None,
        },
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Number],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberLenientText,
            max: None,
            repeating: None,
            default: Some(LiteralValue::Number(2.0)),
        },
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Logical],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::Logical,
            max: None,
            repeating: None,
            default: Some(LiteralValue::Boolean(false)),
        },
    ]
}

#[derive(Debug)]
pub struct FixedFn;
/// Formats a number as text with fixed decimal places.
///
/// # Remarks
/// - Default decimal places is `2` when omitted.
/// - Third argument controls comma grouping (`TRUE` disables commas).
/// - Values are rounded to the requested decimal precision.
/// - Numeric coercion failures return `#VALUE!`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Fixed with commas"
/// formula: '=FIXED(12345.678, 1, FALSE)'
/// expected: "12,345.7"
/// ```
///
/// ```yaml,sandbox
/// title: "Fixed without commas"
/// formula: '=FIXED(12345.678, 1, TRUE)'
/// expected: "12345.7"
/// ```
/// [formualizer-docgen:schema:start]
/// Name: FIXED
/// Type: FixedFn
/// Min args: 1
/// Max args: 3
/// Variadic: false
/// Signature: FIXED(arg1: number@scalar, arg2?: number@scalar, arg3?: logical@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=false,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=true}; arg3{kinds=logical,required=false,shape=scalar,by_ref=false,coercion=Logical,max=None,repeating=None,default=true}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FixedFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FIXED"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(arg_fixed);
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let v = scalar_like_value(&args[0])?;
        let num = match v {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_num(&other)?,
        };

        let decimals = if args.len() >= 2 {
            match scalar_like_value(&args[1])? {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                other => coerce_num(&other)?.trunc() as i32,
            }
        } else {
            2
        };

        let no_commas = if args.len() >= 3 {
            match scalar_like_value(&args[2])? {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Boolean(b) => b,
                other => coerce_num(&other)? != 0.0,
            }
        } else {
            false
        };

        // Round to specified decimals
        let factor = 10f64.powi(decimals);
        let rounded = (num * factor).round() / factor;

        let decimals_usize = decimals.max(0) as usize;

        let formatted = if decimals >= 0 {
            format!("{:.prec$}", rounded.abs(), prec = decimals_usize)
        } else {
            format!("{:.0}", rounded.abs())
        };

        let result = if no_commas {
            if rounded < 0.0 {
                format!("-{}", formatted)
            } else {
                formatted
            }
        } else {
            // Add thousands separators
            let parts: Vec<&str> = formatted.split('.').collect();
            let int_part = parts[0];
            let dec_part = parts.get(1);

            let int_with_commas: String = int_part
                .chars()
                .rev()
                .enumerate()
                .flat_map(|(i, c)| {
                    if i > 0 && i % 3 == 0 {
                        vec![',', c]
                    } else {
                        vec![c]
                    }
                })
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();

            if let Some(dec) = dec_part {
                if rounded < 0.0 {
                    format!("-{}.{}", int_with_commas, dec)
                } else {
                    format!("{}.{}", int_with_commas, dec)
                }
            } else if rounded < 0.0 {
                format!("-{}", int_with_commas)
            } else {
                int_with_commas
            }
        };

        Ok(CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

// ============================================================================
// Registration
// ============================================================================

pub fn register_builtins() {
    use crate::function_registry::register_function;
    use std::sync::Arc;

    register_function(Arc::new(CleanFn));
    register_function(Arc::new(UnicharFn));
    register_function(Arc::new(UnicodeFn));
    register_function(Arc::new(TextBeforeFn));
    register_function(Arc::new(TextAfterFn));
    register_function(Arc::new(DollarFn));
    register_function(Arc::new(FixedFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    fn make_text_ast(s: &str) -> ASTNode {
        ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Text(s.to_string())),
            None,
        )
    }

    fn make_num_ast(n: f64) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(n)), None)
    }

    #[test]
    fn test_clean() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CleanFn));
        let ctx = interp(&wb);
        let clean = ctx.context.get_function("", "CLEAN").unwrap();

        let input = make_text_ast("Hello\x00\x01\x1FWorld");
        let args = vec![ArgumentHandle::new(&input, &ctx)];
        match clean
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Text(s) => assert_eq!(s, "HelloWorld"),
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn test_unichar_unicode() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(UnicharFn))
            .with_function(std::sync::Arc::new(UnicodeFn));
        let ctx = interp(&wb);

        // UNICHAR
        let unichar = ctx.context.get_function("", "UNICHAR").unwrap();
        let code = make_num_ast(65.0);
        let args = vec![ArgumentHandle::new(&code, &ctx)];
        match unichar
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Text(s) => assert_eq!(s, "A"),
            v => panic!("unexpected {v:?}"),
        }

        // UNICODE
        let unicode = ctx.context.get_function("", "UNICODE").unwrap();
        let text = make_text_ast("A");
        let args = vec![ArgumentHandle::new(&text, &ctx)];
        match unicode
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(n) => assert_eq!(n, 65.0),
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn test_textbefore_textafter() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(TextBeforeFn))
            .with_function(std::sync::Arc::new(TextAfterFn));
        let ctx = interp(&wb);

        let textbefore = ctx.context.get_function("", "TEXTBEFORE").unwrap();
        let text = make_text_ast("hello-world-test");
        let delim = make_text_ast("-");
        let args = vec![
            ArgumentHandle::new(&text, &ctx),
            ArgumentHandle::new(&delim, &ctx),
        ];
        match textbefore
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Text(s) => assert_eq!(s, "hello"),
            v => panic!("unexpected {v:?}"),
        }

        let textafter = ctx.context.get_function("", "TEXTAFTER").unwrap();
        match textafter
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Text(s) => assert_eq!(s, "world-test"),
            v => panic!("unexpected {v:?}"),
        }
    }
}
