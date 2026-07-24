use super::super::utils::ARG_ANY_ONE;
use crate::args::{ArgSchema, ShapeKind};
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext, ResolvedArgument};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;
use std::sync::LazyLock;

static ARG_ANY_RANGE_ONE: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
    vec![{
        let mut schema = ArgSchema::any();
        schema.shape = ShapeKind::Range;
        schema
    }]
});

static TEXTJOIN_ARGS: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
    vec![ArgSchema::any(), ArgSchema::any(), {
        let mut schema = ArgSchema::any();
        schema.shape = ShapeKind::Range;
        schema.repeating = Some(1);
        schema
    }]
});

const MAX_CONCAT_RESULT_CHARS: usize = 32_767;

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
    literal_to_text(&scalar_like_value(a)?)
}

fn literal_to_text(v: &LiteralValue) -> Result<String, ExcelError> {
    Ok(match v {
        LiteralValue::Text(s) => s.clone(),
        LiteralValue::Empty => String::new(),
        LiteralValue::Boolean(b) => {
            if *b {
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
        LiteralValue::Error(e) => return Err(e.clone()),
        other => other.to_string(),
    })
}

fn legacy_scalar_value(arg: &ArgumentHandle<'_, '_>) -> Result<LiteralValue, ExcelError> {
    Ok(match arg.value()? {
        crate::traits::CalcValue::Scalar(LiteralValue::Array(rows)) => rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .unwrap_or(LiteralValue::Empty),
        crate::traits::CalcValue::Scalar(value) => value,
        crate::traits::CalcValue::Range(view) => view.get_cell(0, 0),
        crate::traits::CalcValue::Callable(_) => LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Calc).with_message("LAMBDA value must be invoked"),
        ),
    })
}

fn append_with_limit(
    out: &mut String,
    current_chars: &mut usize,
    text: &str,
) -> Result<(), ExcelError> {
    let next_chars = current_chars
        .checked_add(text.chars().count())
        .ok_or_else(ExcelError::new_value)?;
    if next_chars > MAX_CONCAT_RESULT_CHARS {
        return Err(ExcelError::new_value());
    }
    out.push_str(text);
    *current_chars = next_chars;
    Ok(())
}

fn for_each_expanded_value(
    arg: &ArgumentHandle<'_, '_>,
    visitor: &mut dyn FnMut(&LiteralValue) -> Result<(), ExcelError>,
) -> Result<(), ExcelError> {
    match arg.resolve_once()? {
        ResolvedArgument::Range(view) | ResolvedArgument::Value(CalcValue::Range(view)) => {
            view.for_each_cell(visitor)
        }
        ResolvedArgument::ReferenceError(error) => visitor(&LiteralValue::Error(error)),
        ResolvedArgument::Value(CalcValue::Scalar(LiteralValue::Array(rows))) => {
            for row in &rows {
                for value in row {
                    visitor(value)?;
                }
            }
            Ok(())
        }
        ResolvedArgument::Value(CalcValue::Scalar(value)) => visitor(&value),
        ResolvedArgument::Value(CalcValue::Callable(_)) => visitor(&LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Calc).with_message("LAMBDA value must be invoked"),
        )),
    }
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
/// - Ranges and arrays are flattened in row-major order.
/// - Blank values contribute an empty string.
/// - Numbers and booleans are coerced to text.
/// - Errors are propagated as soon as encountered.
/// - Results longer than 32,767 characters return `#VALUE!`.
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
/// Signature: CONCAT(arg1...: any@range)
/// Arg schema: arg1{kinds=any,required=true,shape=range,by_ref=false,coercion=None,max=None,repeating=None,default=false}
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
        &ARG_ANY_RANGE_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut out = String::new();
        let mut out_chars = 0;
        for arg in args {
            for_each_expanded_value(arg, &mut |value| {
                append_with_limit(&mut out, &mut out_chars, &literal_to_text(value)?)
            })?;
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(out)))
    }
}
// CONCATENATE (legacy scalar semantics)
#[derive(Debug)]
pub struct ConcatenateFn;
/// Legacy function that joins multiple scalar values as text.
///
/// # Remarks
/// - Range and array arguments use only their top-left value.
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
///     a: "Yes. CONCAT expands ranges and arrays, while legacy CONCATENATE uses only the top-left value."
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
        _: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut out = String::new();
        for arg in args {
            out.push_str(&literal_to_text(&legacy_scalar_value(arg)?)?);
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(out)))
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
/// - Text ranges and arrays are flattened in row-major order.
/// - Delimiter and values are coerced to text.
/// - Any error in inputs propagates immediately.
/// - Results longer than 32,767 characters return `#VALUE!`.
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
/// Signature: TEXTJOIN(arg1: any@scalar, arg2: any@scalar, arg3...: any@range)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg3{kinds=any,required=true,shape=range,by_ref=false,coercion=None,max=None,repeating=Some(1),default=false}
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
        &TEXTJOIN_ARGS[..]
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

        let mut out = String::new();
        let mut out_chars = 0;
        let mut has_item = false;
        for arg in args.iter().skip(2) {
            let mut cell_error = None;
            let visit_result = for_each_expanded_value(arg, &mut |value| {
                match value {
                    LiteralValue::Error(e) => {
                        cell_error = Some(e.clone());
                        return Err(e.clone());
                    }
                    LiteralValue::Empty => {
                        if !ignore_empty {
                            if has_item {
                                append_with_limit(&mut out, &mut out_chars, &delimiter)?;
                            }
                            has_item = true;
                        }
                    }
                    value => {
                        let s = match value {
                            LiteralValue::Text(t) => t.clone(),
                            LiteralValue::Boolean(b) => {
                                if *b {
                                    "TRUE".to_string()
                                } else {
                                    "FALSE".to_string()
                                }
                            }
                            LiteralValue::Int(i) => i.to_string(),
                            LiteralValue::Number(f) => f.to_string(),
                            _ => value.to_string(),
                        };
                        if !ignore_empty || !s.is_empty() {
                            if has_item {
                                append_with_limit(&mut out, &mut out_chars, &delimiter)?;
                            }
                            append_with_limit(&mut out, &mut out_chars, &s)?;
                            has_item = true;
                        }
                    }
                }
                Ok(())
            });
            if let Some(error) = cell_error {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(error)));
            }
            visit_result?;
        }

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(out)))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_builtin(Arc::new(TrimFn));
    crate::function_registry::register_builtin(Arc::new(UpperFn));
    crate::function_registry::register_builtin(Arc::new(LowerFn));
    crate::function_registry::register_builtin(Arc::new(ProperFn));
    crate::function_registry::register_builtin(Arc::new(ConcatFn));
    crate::function_registry::register_builtin(Arc::new(ConcatenateFn));
    crate::function_registry::register_builtin(Arc::new(TextJoinFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    fn range_ref(original: &str, sr: u32, sc: u32, er: u32, ec: u32) -> ASTNode {
        ASTNode::new(
            ASTNodeType::Reference {
                original: original.into(),
                reference: ReferenceType::range(None, Some(sr), Some(sc), Some(er), Some(ec)),
            },
            None,
        )
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

    #[test]
    fn concat_and_textjoin_flatten_2d_range_row_major() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(ConcatFn))
            .with_function(std::sync::Arc::new(TextJoinFn))
            .with_range(
                "Sheet1",
                1,
                1,
                vec![
                    vec![
                        LiteralValue::Text("a".into()),
                        LiteralValue::Text("b".into()),
                    ],
                    vec![
                        LiteralValue::Text("c".into()),
                        LiteralValue::Text("d".into()),
                    ],
                ],
            );
        let ctx = wb.interpreter();
        let range = range_ref("A1:B2", 1, 1, 2, 2);
        let concat = ctx.context.get_function("", "CONCAT").unwrap();
        let concat_out = concat
            .dispatch(
                &[ArgumentHandle::new(&range, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(concat_out, LiteralValue::Text("abcd".into()));

        let textjoin = ctx.context.get_function("", "TEXTJOIN").unwrap();
        let delimiter = lit(LiteralValue::Text("|".into()));
        let ignore = lit(LiteralValue::Boolean(true));
        let textjoin_out = textjoin
            .dispatch(
                &[
                    ArgumentHandle::new(&delimiter, &ctx),
                    ArgumentHandle::new(&ignore, &ctx),
                    ArgumentHandle::new(&range, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(textjoin_out, LiteralValue::Text("a|b|c|d".into()));
    }

    #[test]
    fn concat_and_textjoin_flatten_array_values_row_major() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(ConcatFn))
            .with_function(std::sync::Arc::new(TextJoinFn));
        let ctx = wb.interpreter();
        let array = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(1), LiteralValue::Int(2)],
            vec![LiteralValue::Int(3), LiteralValue::Int(4)],
        ]));

        let concat = ctx.context.get_function("", "CONCAT").unwrap();
        let concat_out = concat
            .dispatch(
                &[ArgumentHandle::new(&array, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(concat_out, LiteralValue::Text("1234".into()));

        let textjoin = ctx.context.get_function("", "TEXTJOIN").unwrap();
        let delimiter = lit(LiteralValue::Text("-".into()));
        let ignore = lit(LiteralValue::Boolean(true));
        let textjoin_out = textjoin
            .dispatch(
                &[
                    ArgumentHandle::new(&delimiter, &ctx),
                    ArgumentHandle::new(&ignore, &ctx),
                    ArgumentHandle::new(&array, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(textjoin_out, LiteralValue::Text("1-2-3-4".into()));
    }

    #[test]
    fn concatenate_uses_top_left_of_literal_arrays_in_ast_evaluation() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ConcatenateFn));
        let ctx = wb.interpreter();
        let concatenate = ctx.context.get_function("", "CONCATENATE").unwrap();
        let suffix = lit(LiteralValue::Text("!".into()));

        for (rows, expected) in [
            (
                vec![
                    vec![LiteralValue::Text("top".into()), LiteralValue::Int(2)],
                    vec![LiteralValue::Int(3)],
                ],
                "top!",
            ),
            (Vec::new(), "!"),
            (vec![Vec::new(), vec![LiteralValue::Int(9)]], "!"),
        ] {
            let array = lit(LiteralValue::Array(rows));
            let out = concatenate
                .dispatch(
                    &[
                        ArgumentHandle::new(&array, &ctx),
                        ArgumentHandle::new(&suffix, &ctx),
                    ],
                    &ctx.function_context(None),
                )
                .unwrap()
                .into_literal();
            assert_eq!(out, LiteralValue::Text(expected.into()));
        }
    }

    #[test]
    fn concat_and_textjoin_enforce_character_limit_at_boundary() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(ConcatFn))
            .with_function(std::sync::Arc::new(TextJoinFn));
        let ctx = wb.interpreter();

        let concat = ctx.context.get_function("", "CONCAT").unwrap();
        let concat_exact = lit(LiteralValue::Text("é".repeat(MAX_CONCAT_RESULT_CHARS)));
        let concat_out = concat
            .dispatch(
                &[ArgumentHandle::new(&concat_exact, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(
            concat_out,
            LiteralValue::Text("é".repeat(MAX_CONCAT_RESULT_CHARS))
        );

        let one_more = lit(LiteralValue::Text("x".into()));
        let concat_error = concat
            .dispatch(
                &[
                    ArgumentHandle::new(&concat_exact, &ctx),
                    ArgumentHandle::new(&one_more, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap_err();
        assert_eq!(concat_error, ExcelError::new_value());

        let textjoin = ctx.context.get_function("", "TEXTJOIN").unwrap();
        let delimiter = lit(LiteralValue::Text("|".into()));
        let ignore = lit(LiteralValue::Boolean(true));
        let tail = lit(LiteralValue::Text("z".into()));
        for (prefix_len, expect_error) in [(32_765, false), (32_766, true)] {
            let prefix = lit(LiteralValue::Text("x".repeat(prefix_len)));
            let result = textjoin.dispatch(
                &[
                    ArgumentHandle::new(&delimiter, &ctx),
                    ArgumentHandle::new(&ignore, &ctx),
                    ArgumentHandle::new(&prefix, &ctx),
                    ArgumentHandle::new(&tail, &ctx),
                ],
                &ctx.function_context(None),
            );
            if expect_error {
                assert_eq!(result.unwrap_err(), ExcelError::new_value());
            } else {
                let value = result.unwrap().into_literal();
                let LiteralValue::Text(text) = value else {
                    panic!("expected text result, got {value:?}");
                };
                assert_eq!(text.chars().count(), MAX_CONCAT_RESULT_CHARS);
                assert!(text.ends_with("|z"));
            }
        }
    }

    #[test]
    fn expanded_array_preserves_error_after_first_cell() {
        // An error in a non-first cell must still abort the join. Only the
        // error *kind* is asserted: materializing an array argument goes
        // through the shared Arrow-backed range view, which encodes error
        // kinds rather than their diagnostic messages, so message erasure here
        // is engine-wide behavior and not specific to CONCAT/TEXTJOIN.
        let expected = ExcelError::new(ExcelErrorKind::Ref).with_message("later cell failed");
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(ConcatFn))
            .with_function(std::sync::Arc::new(TextJoinFn));
        let ctx = wb.interpreter();
        let array = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Text("ok".into()),
            LiteralValue::Error(expected.clone()),
            LiteralValue::Text("unreached".into()),
        ]]));

        let concat = ctx.context.get_function("", "CONCAT").unwrap();
        let concat_error = concat
            .dispatch(
                &[ArgumentHandle::new(&array, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap_err();
        assert_eq!(concat_error.kind, expected.kind);

        let textjoin = ctx.context.get_function("", "TEXTJOIN").unwrap();
        let delimiter = lit(LiteralValue::Text(",".into()));
        let ignore = lit(LiteralValue::Boolean(true));
        let textjoin_out = textjoin
            .dispatch(
                &[
                    ArgumentHandle::new(&delimiter, &ctx),
                    ArgumentHandle::new(&ignore, &ctx),
                    ArgumentHandle::new(&array, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        match textjoin_out {
            LiteralValue::Error(error) => assert_eq!(error.kind, expected.kind),
            other => panic!("expected an error value, got {other:?}"),
        }
    }

    #[test]
    fn textjoin_range_blanks_obey_both_ignore_settings() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(TextJoinFn))
            .with_range(
                "Sheet1",
                1,
                1,
                vec![vec![
                    LiteralValue::Text("a".into()),
                    LiteralValue::Empty,
                    LiteralValue::Text(String::new()),
                    LiteralValue::Text("d".into()),
                ]],
            );
        let ctx = wb.interpreter();
        let textjoin = ctx.context.get_function("", "TEXTJOIN").unwrap();
        let delimiter = lit(LiteralValue::Text("-".into()));
        let range = range_ref("A1:D1", 1, 1, 1, 4);

        for (ignore_empty, expected) in [(true, "a-d"), (false, "a---d")] {
            let ignore = lit(LiteralValue::Boolean(ignore_empty));
            let out = textjoin
                .dispatch(
                    &[
                        ArgumentHandle::new(&delimiter, &ctx),
                        ArgumentHandle::new(&ignore, &ctx),
                        ArgumentHandle::new(&range, &ctx),
                    ],
                    &ctx.function_context(None),
                )
                .unwrap()
                .into_literal();
            assert_eq!(out, LiteralValue::Text(expected.into()));
        }
    }

    #[test]
    fn concatenate_keeps_legacy_top_left_range_behavior() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(ConcatenateFn))
            .with_range(
                "Sheet1",
                1,
                1,
                vec![vec![
                    LiteralValue::Text("top".into()),
                    LiteralValue::Text("ignored".into()),
                ]],
            );
        let ctx = wb.interpreter();
        let concatenate = ctx.context.get_function("", "CONCATENATE").unwrap();
        let range = range_ref("A1:B1", 1, 1, 1, 2);
        let suffix = lit(LiteralValue::Text("!".into()));
        let out = concatenate
            .dispatch(
                &[
                    ArgumentHandle::new(&range, &ctx),
                    ArgumentHandle::new(&suffix, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Text("top!".into()));
    }

    #[test]
    fn concat_textjoin_and_concatenate_publish_matching_shapes() {
        assert_eq!(ConcatFn.arg_schema()[0].shape, ShapeKind::Range);
        assert_eq!(ConcatenateFn.arg_schema()[0].shape, ShapeKind::Scalar);
        assert_eq!(TextJoinFn.arg_schema()[0].shape, ShapeKind::Scalar);
        assert_eq!(TextJoinFn.arg_schema()[1].shape, ShapeKind::Scalar);
        assert_eq!(TextJoinFn.arg_schema()[2].shape, ShapeKind::Range);
        assert_eq!(TextJoinFn.arg_schema()[2].repeating, Some(1));
    }
}
