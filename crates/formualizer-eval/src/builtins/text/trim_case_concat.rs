use super::super::utils::ARG_ANY_ONE;
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;

fn scalar_like_value(arg: &ArgumentHandle<'_, '_>) -> Result<LiteralValue, ExcelError> {
    Ok(match arg.value()? {
        crate::traits::CalcValue::Scalar(v) => v,
        crate::traits::CalcValue::Range(rv) => rv.get_cell(0, 0),
        crate::traits::CalcValue::Callable(_) => LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Calc).with_message("LAMBDA value must be invoked"),
        ),
    })
}

fn to_text<'a, 'b>(a: &ArgumentHandle<'a, 'b>) -> Result<String, ExcelError> {
    let v = scalar_like_value(a)?;
    Ok(match v {
        LiteralValue::Text(s) => s,
        LiteralValue::Empty => String::new(),
        LiteralValue::Boolean(b) => {
            if b {
                "TRUE".into()
            } else {
                "FALSE".into()
            }
        }
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Number(f) => {
            let s = f.to_string();
            if s.ends_with(".0") {
                s[..s.len() - 2].into()
            } else {
                s
            }
        }
        LiteralValue::Error(e) => return Err(e),
        other => other.to_string(),
    })
}

#[derive(Debug)]
pub struct TrimFn;
/// Removes leading/trailing whitespace and collapses internal runs to single spaces.
///
/// # Remarks
/// - Leading and trailing whitespace is removed.
/// - Consecutive whitespace inside the text is collapsed to one ASCII space.
/// - Non-text inputs are coerced to text before trimming.
/// - Errors are propagated unchanged.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Normalize spacing"
/// formula: '=TRIM("  alpha   beta  ")'
/// expected: "alpha beta"
/// ```
///
/// ```yaml,sandbox
/// title: "Already clean text"
/// formula: '=TRIM("report")'
/// expected: "report"
/// ```
///
/// ```yaml,docs
/// related:
///   - CLEAN
///   - TEXTJOIN
///   - SUBSTITUTE
/// faq:
///   - q: "What whitespace does TRIM normalize?"
///     a: "It trims edges and collapses internal whitespace runs to single spaces."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: TRIM
/// Type: TrimFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: TRIM(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TrimFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TRIM"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let s = to_text(&args[0])?;
        let mut out = String::new();
        let mut prev_space = false;
        for ch in s.chars() {
            if ch.is_whitespace() {
                prev_space = true;
            } else {
                if prev_space && !out.is_empty() {
                    out.push(' ');
                }
                out.push(ch);
                prev_space = false;
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            out.trim().into(),
        )))
    }
}

#[derive(Debug)]
pub struct UpperFn;
/// Converts text to uppercase.
///
/// # Remarks
/// - Uses ASCII uppercasing semantics in this implementation.
/// - Numbers and booleans are first converted to text.
/// - Errors are propagated unchanged.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Uppercase letters"
/// formula: '=UPPER("Quarterly report")'
/// expected: "QUARTERLY REPORT"
/// ```
///
/// ```yaml,sandbox
/// title: "Number coerced to text"
/// formula: '=UPPER(123)'
/// expected: "123"
/// ```
///
/// ```yaml,docs
/// related:
///   - LOWER
///   - PROPER
///   - EXACT
/// faq:
///   - q: "Is uppercasing fully Unicode-aware?"
///     a: "This implementation uses ASCII uppercasing semantics, so non-ASCII case rules are limited."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: UPPER
/// Type: UpperFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: UPPER(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for UpperFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "UPPER"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            to_text(&args[0])?.to_ascii_uppercase(),
        )))
    }
}
#[derive(Debug)]
pub struct LowerFn;
/// Converts text to lowercase.
///
/// # Remarks
/// - Uses ASCII lowercasing semantics in this implementation.
/// - Numbers and booleans are first converted to text.
/// - Errors are propagated unchanged.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Lowercase letters"
/// formula: '=LOWER("Data PIPELINE")'
/// expected: "data pipeline"
/// ```
///
/// ```yaml,sandbox
/// title: "Boolean coerced to text"
/// formula: '=LOWER(TRUE)'
/// expected: "true"
/// ```
///
/// ```yaml,docs
/// related:
///   - UPPER
///   - PROPER
///   - EXACT
/// faq:
///   - q: "How are booleans handled by LOWER?"
///     a: "Inputs are coerced to text first, so TRUE/FALSE become lowercase string values."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: LOWER
/// Type: LowerFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: LOWER(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for LowerFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LOWER"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            to_text(&args[0])?.to_ascii_lowercase(),
        )))
    }
}
#[derive(Debug)]
pub struct ProperFn;
/// Capitalizes the first letter of each alphanumeric word.
///
/// # Remarks
/// - Word boundaries are reset by non-alphanumeric characters.
/// - Internal letters in each word are lowercased.
/// - Non-text inputs are coerced to text.
/// - Errors are propagated unchanged.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Title case simple phrase"
/// formula: '=PROPER("hello world")'
/// expected: "Hello World"
/// ```
///
/// ```yaml,sandbox
/// title: "Hyphen-separated words"
/// formula: '=PROPER("north-east REGION")'
/// expected: "North-East Region"
/// ```
///
/// ```yaml,docs
/// related:
///   - UPPER
///   - LOWER
///   - TRIM
/// faq:
///   - q: "How are word boundaries determined?"
///     a: "Any non-alphanumeric character starts a new word boundary for capitalization."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: PROPER
/// Type: ProperFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: PROPER(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ProperFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PROPER"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let s = to_text(&args[0])?;
        let mut out = String::new();
        let mut new_word = true;
        for ch in s.chars() {
            if ch.is_alphanumeric() {
                if new_word {
                    for c in ch.to_uppercase() {
                        out.push(c);
                    }
                } else {
                    for c in ch.to_lowercase() {
                        out.push(c);
                    }
                }
                new_word = false;
            } else {
                out.push(ch);
                new_word = true;
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(out)))
    }
}

// CONCAT(text1, text2, ...)
#[derive(Debug)]
pub struct ConcatFn;
/// Concatenates multiple values into one text string.
///
/// # Remarks
/// - Accepts one or more arguments.
/// - Blank values contribute an empty string.
/// - Numbers and booleans are coerced to text.
/// - Errors are propagated as soon as encountered.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Join text pieces"
/// formula: '=CONCAT("Q", 1, "-", "2026")'
/// expected: "Q1-2026"
/// ```
///
/// ```yaml,sandbox
/// title: "Concatenate with blanks"
/// formula: '=CONCAT("A", "", "B")'
/// expected: "AB"
/// ```
///
/// ```yaml,docs
/// related:
///   - CONCATENATE
///   - TEXTJOIN
///   - VALUE
/// faq:
///   - q: "Do blank arguments add separators or characters?"
///     a: "No. CONCAT appends each value directly, and blanks contribute an empty string."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: CONCAT
/// Type: ConcatFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: CONCAT(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ConcatFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CONCAT"
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
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut out = String::new();
        for a in args {
            out.push_str(&to_text(a)?);
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(out)))
    }
}
// CONCATENATE (alias semantics)
#[derive(Debug)]
pub struct ConcatenateFn;
/// Legacy alias for `CONCAT` that joins multiple values as text.
///
/// # Remarks
/// - Semantics match `CONCAT` in this implementation.
/// - Blank values contribute an empty string.
/// - Numbers and booleans are coerced to text.
/// - Errors are propagated as soon as encountered.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Legacy concatenate behavior"
/// formula: '=CONCATENATE("Jan", "-", 2026)'
/// expected: "Jan-2026"
/// ```
///
/// ```yaml,sandbox
/// title: "Boolean coercion"
/// formula: '=CONCATENATE("Flag:", TRUE)'
/// expected: "Flag:TRUE"
/// ```
///
/// ```yaml,docs
/// related:
///   - CONCAT
///   - TEXTJOIN
///   - VALUE
/// faq:
///   - q: "Is CONCATENATE behavior different from CONCAT here?"
///     a: "No. In this engine CONCATENATE uses the same join semantics as CONCAT."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: CONCATENATE
/// Type: ConcatenateFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: CONCATENATE(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ConcatenateFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CONCATENATE"
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        ConcatFn.eval(args, ctx)
    }
}

// TEXTJOIN(delimiter, ignore_empty, text1, [text2, ...])
#[derive(Debug)]
pub struct TextJoinFn;
/// Joins text values using a delimiter, with optional empty-value filtering.
///
/// `TEXTJOIN(delimiter, ignore_empty, text1, ...)` is useful for building labels and lists.
///
/// # Remarks
/// - `ignore_empty=TRUE` skips empty strings and empty cells.
/// - `ignore_empty=FALSE` includes empty items, which can produce adjacent delimiters.
/// - Delimiter and values are coerced to text.
/// - Any error in inputs propagates immediately.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Ignore empty entries"
/// formula: '=TEXTJOIN(",", TRUE, "a", "", "c")'
/// expected: "a,c"
/// ```
///
/// ```yaml,sandbox
/// title: "Keep empty entries"
/// formula: '=TEXTJOIN("-", FALSE, "a", "", "c")'
/// expected: "a--c"
/// ```
///
/// ```yaml,docs
/// related:
///   - CONCAT
///   - CONCATENATE
///   - TEXTSPLIT
/// faq:
///   - q: "What does ignore_empty change?"
///     a: "TRUE skips empty values; FALSE keeps them, which can create adjacent delimiters."
/// ```
/// [formualizer-docgen:schema:start]
/// Name: TEXTJOIN
/// Type: TextJoinFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: TEXTJOIN(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TextJoinFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "TEXTJOIN"
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
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        // Get delimiter
        let delimiter = to_text(&args[0])?;

        // Get ignore_empty flag
        let ignore_empty = match scalar_like_value(&args[1])? {
            LiteralValue::Boolean(b) => b,
            LiteralValue::Int(i) => i != 0,
            LiteralValue::Number(f) => f != 0.0,
            LiteralValue::Text(t) => t.to_uppercase() == "TRUE",
            LiteralValue::Empty => false,
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            _ => false,
        };

        // Collect text values
        let mut parts = Vec::new();
        for arg in args.iter().skip(2) {
            match scalar_like_value(arg)? {
                LiteralValue::Error(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
                LiteralValue::Empty => {
                    if !ignore_empty {
                        parts.push(String::new());
                    }
                }
                v => {
                    let s = match v {
                        LiteralValue::Text(t) => t,
                        LiteralValue::Boolean(b) => {
                            if b {
                                "TRUE".to_string()
                            } else {
                                "FALSE".to_string()
                            }
                        }
                        LiteralValue::Int(i) => i.to_string(),
                        LiteralValue::Number(f) => f.to_string(),
                        _ => v.to_string(),
                    };
                    if !ignore_empty || !s.is_empty() {
                        parts.push(s);
                    }
                }
            }
        }

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            parts.join(&delimiter),
        )))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(TrimFn));
    crate::function_registry::register_function(Arc::new(UpperFn));
    crate::function_registry::register_function(Arc::new(LowerFn));
    crate::function_registry::register_function(Arc::new(ProperFn));
    crate::function_registry::register_function(Arc::new(ConcatFn));
    crate::function_registry::register_function(Arc::new(ConcatenateFn));
    crate::function_registry::register_function(Arc::new(TextJoinFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }
    #[test]
    fn trim_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TrimFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TRIM").unwrap();
        let s = lit(LiteralValue::Text("  a   b  ".into()));
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&s, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Text("a b".into()));
    }
    #[test]
    fn concat_variants() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(ConcatFn))
            .with_function(std::sync::Arc::new(ConcatenateFn));
        let ctx = wb.interpreter();
        let c = ctx.context.get_function("", "CONCAT").unwrap();
        let ce = ctx.context.get_function("", "CONCATENATE").unwrap();
        let a = lit(LiteralValue::Text("a".into()));
        let b = lit(LiteralValue::Text("b".into()));
        assert_eq!(
            c.dispatch(
                &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Text("ab".into())
        );
        assert_eq!(
            ce.dispatch(
                &[ArgumentHandle::new(&a, &ctx), ArgumentHandle::new(&b, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Text("ab".into())
        );
    }

    #[test]
    fn textjoin_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TextJoinFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TEXTJOIN").unwrap();
        let delim = lit(LiteralValue::Text(",".into()));
        let ignore = lit(LiteralValue::Boolean(true));
        let a = lit(LiteralValue::Text("a".into()));
        let b = lit(LiteralValue::Text("b".into()));
        let c = lit(LiteralValue::Empty);
        let d = lit(LiteralValue::Text("d".into()));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&delim, &ctx),
                    ArgumentHandle::new(&ignore, &ctx),
                    ArgumentHandle::new(&a, &ctx),
                    ArgumentHandle::new(&b, &ctx),
                    ArgumentHandle::new(&c, &ctx),
                    ArgumentHandle::new(&d, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Text("a,b,d".into()));
    }

    #[test]
    fn textjoin_no_ignore() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(TextJoinFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TEXTJOIN").unwrap();
        let delim = lit(LiteralValue::Text("-".into()));
        let ignore = lit(LiteralValue::Boolean(false));
        let a = lit(LiteralValue::Text("a".into()));
        let b = lit(LiteralValue::Empty);
        let c = lit(LiteralValue::Text("c".into()));
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&delim, &ctx),
                    ArgumentHandle::new(&ignore, &ctx),
                    ArgumentHandle::new(&a, &ctx),
                    ArgumentHandle::new(&b, &ctx),
                    ArgumentHandle::new(&c, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Text("a--c".into()));
    }
}
