//! Text array functions: TEXTSPLIT, VALUETOTEXT, ARRAYTOTEXT
//!
//! TEXTSPLIT: Splits text into a 2D array based on delimiters
//! VALUETOTEXT: Converts a value to text representation
//! ARRAYTOTEXT: Converts an array to text representation

use super::super::utils::collapse_if_scalar;
use crate::args::{ArgSchema, ShapeKind};
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ArgKind, CoercionPolicy, ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;

fn scalar_like_value(arg: &ArgumentHandle<'_, '_>) -> Result<LiteralValue, ExcelError> {
    Ok(match arg.value()? {
        CalcValue::Scalar(v) => v,
        CalcValue::Range(rv) => rv.get_cell(0, 0),
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

/// Get delimiters from an argument (can be single value or array)
fn get_delimiters(arg: &ArgumentHandle<'_, '_>) -> Result<Vec<String>, ExcelError> {
    let cv = arg.value()?;
    match cv {
        CalcValue::Scalar(v) => match v {
            LiteralValue::Error(e) => Err(e),
            LiteralValue::Array(arr) => {
                let mut delims = Vec::new();
                for row in arr {
                    for cell in row {
                        let s = coerce_text(&cell);
                        if !s.is_empty() {
                            delims.push(s);
                        }
                    }
                }
                Ok(delims)
            }
            other => {
                let s = coerce_text(&other);
                if s.is_empty() {
                    Ok(vec![])
                } else {
                    Ok(vec![s])
                }
            }
        },
        CalcValue::Range(rv) => {
            let mut delims = Vec::new();
            rv.for_each_cell(&mut |cell| {
                let s = coerce_text(cell);
                if !s.is_empty() {
                    delims.push(s);
                }
                Ok(())
            })?;
            Ok(delims)
        }
    }
}

// ============================================================================
// TEXTSPLIT - Split text into 2D array based on delimiters
// ============================================================================

fn arg_textsplit() -> Vec<ArgSchema> {
    vec![
        // text
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
        // col_delimiter
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
        // row_delimiter (optional)
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Any],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::None,
            max: None,
            repeating: None,
            default: None,
        },
        // ignore_empty (optional, default FALSE)
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
        // match_mode (optional, default 0)
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Number],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberLenientText,
            max: None,
            repeating: None,
            default: Some(LiteralValue::Number(0.0)),
        },
        // pad_with (optional, default #N/A)
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Any],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::None,
            max: None,
            repeating: None,
            default: Some(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na))),
        },
    ]
}

/// Split text using any of the delimiters, with optional case-insensitive matching
fn split_by_delimiters(text: &str, delimiters: &[String], case_insensitive: bool) -> Vec<String> {
    if delimiters.is_empty() {
        return vec![text.to_string()];
    }

    let working_text = if case_insensitive {
        text.to_lowercase()
    } else {
        text.to_string()
    };

    let delims_working: Vec<String> = if case_insensitive {
        delimiters.iter().map(|d| d.to_lowercase()).collect()
    } else {
        delimiters.to_vec()
    };

    let mut result = Vec::new();
    let mut current_start = 0;

    while current_start < text.len() {
        let mut earliest_match: Option<(usize, usize)> = None; // (position, delimiter_len)

        for delim in &delims_working {
            if delim.is_empty() {
                continue;
            }
            if let Some(pos) = working_text[current_start..].find(delim.as_str()) {
                let abs_pos = current_start + pos;
                match earliest_match {
                    None => earliest_match = Some((abs_pos, delim.len())),
                    Some((ep, _)) if abs_pos < ep => earliest_match = Some((abs_pos, delim.len())),
                    _ => {}
                }
            }
        }

        match earliest_match {
            Some((pos, len)) => {
                result.push(text[current_start..pos].to_string());
                current_start = pos + len;
            }
            None => {
                result.push(text[current_start..].to_string());
                break;
            }
        }
    }

    // If we ended exactly at a delimiter, add empty string at end
    if current_start == text.len() && !text.is_empty() {
        let ends_with_delim = delims_working.iter().any(|d| {
            if d.is_empty() {
                return false;
            }
            working_text.ends_with(d.as_str())
        });
        if ends_with_delim {
            result.push(String::new());
        }
    }

    result
}

#[derive(Debug)]
pub struct TextSplitFn;

impl Function for TextSplitFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "TEXTSPLIT"
    }

    fn min_args(&self) -> usize {
        2
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(arg_textsplit);
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Get text to split
        let text_val = scalar_like_value(&args[0])?;
        let text = match text_val {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_text(&other),
        };

        // Get column delimiters
        let col_delimiters = get_delimiters(&args[1])?;

        // Get optional row delimiters
        let row_delimiters = if args.len() > 2 {
            // Check if row_delimiter argument is provided and not omitted
            let val = scalar_like_value(&args[2])?;
            match val {
                LiteralValue::Empty => vec![],
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                _ => get_delimiters(&args[2])?,
            }
        } else {
            vec![]
        };

        // Get ignore_empty (default FALSE)
        let ignore_empty = if args.len() > 3 {
            match scalar_like_value(&args[3])? {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Boolean(b) => b,
                LiteralValue::Number(n) => n != 0.0,
                LiteralValue::Int(i) => i != 0,
                _ => false,
            }
        } else {
            false
        };

        // Get match_mode (default 0 = case-sensitive)
        let case_insensitive = if args.len() > 4 {
            match scalar_like_value(&args[4])? {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Number(n) => n.trunc() as i32 == 1,
                LiteralValue::Int(i) => i == 1,
                _ => false,
            }
        } else {
            false
        };

        // Get pad_with (default #N/A)
        let pad_with = if args.len() > 5 {
            scalar_like_value(&args[5])?
        } else {
            LiteralValue::Error(ExcelError::new(ExcelErrorKind::Na))
        };

        // First, split by row delimiters (if any)
        let row_parts = if row_delimiters.is_empty() {
            vec![text.clone()]
        } else {
            split_by_delimiters(&text, &row_delimiters, case_insensitive)
        };

        // Then split each row by column delimiters
        let mut rows: Vec<Vec<LiteralValue>> = Vec::new();
        let mut max_cols = 0;

        for row_text in row_parts {
            if ignore_empty && row_text.is_empty() {
                continue;
            }

            let col_parts = split_by_delimiters(&row_text, &col_delimiters, case_insensitive);

            let row: Vec<LiteralValue> = if ignore_empty {
                col_parts
                    .into_iter()
                    .filter(|s| !s.is_empty())
                    .map(LiteralValue::Text)
                    .collect()
            } else {
                col_parts.into_iter().map(LiteralValue::Text).collect()
            };

            if !row.is_empty() {
                max_cols = max_cols.max(row.len());
                rows.push(row);
            }
        }

        // Handle empty result
        if rows.is_empty() {
            return Ok(CalcValue::Scalar(LiteralValue::Text(String::new())));
        }

        // Pad rows to same width
        for row in &mut rows {
            while row.len() < max_cols {
                row.push(pad_with.clone());
            }
        }

        Ok(collapse_if_scalar(rows, ctx.date_system()))
    }
}

// ============================================================================
// VALUETOTEXT - Convert value to text representation
// ============================================================================

fn arg_valuetotext() -> Vec<ArgSchema> {
    vec![
        // value
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
        // format (optional, default 0=concise)
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Number],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberLenientText,
            max: None,
            repeating: None,
            default: Some(LiteralValue::Number(0.0)),
        },
    ]
}

/// Convert a single value to its text representation
fn value_to_text_repr(v: &LiteralValue, strict: bool) -> String {
    match v {
        LiteralValue::Text(s) => {
            if strict {
                format!("\"{}\"", s)
            } else {
                s.clone()
            }
        }
        LiteralValue::Number(n) => {
            let s = n.to_string();
            if s.ends_with(".0") {
                s[..s.len() - 2].to_string()
            } else {
                s
            }
        }
        LiteralValue::Int(i) => i.to_string(),
        LiteralValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        LiteralValue::Empty => String::new(),
        LiteralValue::Error(e) => e.to_string(),
        LiteralValue::Array(arr) => {
            // For arrays, use array syntax
            let rows: Vec<String> = arr
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| value_to_text_repr(cell, strict))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect();
            format!("{{{}}}", rows.join(";"))
        }
        LiteralValue::Date(d) => d.format("%Y-%m-%d").to_string(),
        LiteralValue::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        LiteralValue::Time(t) => t.format("%H:%M:%S").to_string(),
        LiteralValue::Duration(dur) => {
            let total_secs = dur.num_seconds();
            let hours = total_secs / 3600;
            let mins = (total_secs % 3600) / 60;
            let secs = total_secs % 60;
            format!("{}:{:02}:{:02}", hours, mins, secs)
        }
        LiteralValue::Pending => String::new(),
    }
}

#[derive(Debug)]
pub struct ValueToTextFn;

impl Function for ValueToTextFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "VALUETOTEXT"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(arg_valuetotext);
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Get value
        let value = scalar_like_value(&args[0])?;

        // Get format (0=concise, 1=strict)
        let format = if args.len() > 1 {
            match scalar_like_value(&args[1])? {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Number(n) => n.trunc() as i32,
                LiteralValue::Int(i) => i as i32,
                _ => 0,
            }
        } else {
            0
        };

        let strict = format == 1;

        // Handle error propagation for the value itself
        if let LiteralValue::Error(e) = &value {
            // In strict mode, errors become their text representation
            // In concise mode, propagate the error
            if strict {
                return Ok(CalcValue::Scalar(LiteralValue::Text(e.to_string())));
            } else {
                return Ok(CalcValue::Scalar(LiteralValue::Error(e.clone())));
            }
        }

        let result = value_to_text_repr(&value, strict);
        Ok(CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

// ============================================================================
// ARRAYTOTEXT - Convert array to text representation
// ============================================================================

fn arg_arraytotext() -> Vec<ArgSchema> {
    vec![
        // array
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Any, ArgKind::Range],
            required: true,
            by_ref: false,
            shape: ShapeKind::Range,
            coercion: CoercionPolicy::None,
            max: None,
            repeating: None,
            default: None,
        },
        // format (optional, default 0=concise)
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Number],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberLenientText,
            max: None,
            repeating: None,
            default: Some(LiteralValue::Number(0.0)),
        },
    ]
}

#[derive(Debug)]
pub struct ArrayToTextFn;

impl Function for ArrayToTextFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "ARRAYTOTEXT"
    }

    fn min_args(&self) -> usize {
        1
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(arg_arraytotext);
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Get format (0=concise, 1=strict)
        let format = if args.len() > 1 {
            match scalar_like_value(&args[1])? {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Number(n) => n.trunc() as i32,
                LiteralValue::Int(i) => i as i32,
                _ => 0,
            }
        } else {
            0
        };

        let strict = format == 1;

        // Try to get array from argument
        let rows: Vec<Vec<LiteralValue>> = if let Ok(rv) = args[0].range_view() {
            let (num_rows, num_cols) = rv.dims();
            let mut result = Vec::with_capacity(num_rows);
            for r in 0..num_rows {
                let mut row = Vec::with_capacity(num_cols);
                for c in 0..num_cols {
                    row.push(rv.get_cell(r, c));
                }
                result.push(row);
            }
            result
        } else {
            let cv = args[0].value()?;
            match cv.into_literal() {
                LiteralValue::Array(arr) => arr,
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                other => vec![vec![other]],
            }
        };

        let result = if strict {
            // Strict format: {value;value;...} with rows separated by semicolons
            // and columns by commas, with strings quoted
            let row_strs: Vec<String> = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| value_to_text_repr(cell, true))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect();
            format!("{{{}}}", row_strs.join(";"))
        } else {
            // Concise format: comma-separated values (all cells flattened)
            let all_values: Vec<String> = rows
                .iter()
                .flat_map(|row| row.iter().map(|cell| value_to_text_repr(cell, false)))
                .collect();
            all_values.join(", ")
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

    register_function(Arc::new(TextSplitFn));
    register_function(Arc::new(ValueToTextFn));
    register_function(Arc::new(ArrayToTextFn));
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    use std::sync::Arc;

    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn test_valuetotext_concise() {
        let wb = TestWorkbook::new().with_function(Arc::new(ValueToTextFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "VALUETOTEXT").unwrap();

        // Test number
        let num = lit(LiteralValue::Number(123.0));
        let args = vec![ArgumentHandle::new(&num, &ctx)];
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Text(s) => assert_eq!(s, "123"),
            v => panic!("unexpected {v:?}"),
        }

        // Test text (concise = no quotes)
        let text = lit(LiteralValue::Text("hello".to_string()));
        let args = vec![ArgumentHandle::new(&text, &ctx)];
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Text(s) => assert_eq!(s, "hello"),
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn test_valuetotext_strict() {
        let wb = TestWorkbook::new().with_function(Arc::new(ValueToTextFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "VALUETOTEXT").unwrap();

        // Test text with strict format (quotes)
        let text = lit(LiteralValue::Text("hello".to_string()));
        let format = lit(LiteralValue::Number(1.0));
        let args = vec![
            ArgumentHandle::new(&text, &ctx),
            ArgumentHandle::new(&format, &ctx),
        ];
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Text(s) => assert_eq!(s, "\"hello\""),
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn test_arraytotext_concise() {
        let wb = TestWorkbook::new().with_function(Arc::new(ArrayToTextFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ARRAYTOTEXT").unwrap();

        // Test simple array
        let arr = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Number(1.0),
            LiteralValue::Number(2.0),
            LiteralValue::Number(3.0),
        ]]));
        let args = vec![ArgumentHandle::new(&arr, &ctx)];
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Text(s) => assert_eq!(s, "1, 2, 3"),
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn test_arraytotext_strict() {
        let wb = TestWorkbook::new().with_function(Arc::new(ArrayToTextFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "ARRAYTOTEXT").unwrap();

        // Test 2D array with strict format
        let arr = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Number(1.0), LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.0), LiteralValue::Number(4.0)],
        ]));
        let format = lit(LiteralValue::Number(1.0));
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&format, &ctx),
        ];
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Text(s) => assert_eq!(s, "{1,2;3,4}"),
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn test_textsplit_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(TextSplitFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "TEXTSPLIT").unwrap();

        // Test simple split
        let text = lit(LiteralValue::Text("a,b,c".to_string()));
        let delim = lit(LiteralValue::Text(",".to_string()));
        let args = vec![
            ArgumentHandle::new(&text, &ctx),
            ArgumentHandle::new(&delim, &ctx),
        ];
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0].len(), 3);
                assert_eq!(arr[0][0], LiteralValue::Text("a".to_string()));
                assert_eq!(arr[0][1], LiteralValue::Text("b".to_string()));
                assert_eq!(arr[0][2], LiteralValue::Text("c".to_string()));
            }
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn test_textsplit_2d() {
        let wb = TestWorkbook::new().with_function(Arc::new(TextSplitFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "TEXTSPLIT").unwrap();

        // Test 2D split with row and column delimiters
        let text = lit(LiteralValue::Text("a,b;c,d".to_string()));
        let col_delim = lit(LiteralValue::Text(",".to_string()));
        let row_delim = lit(LiteralValue::Text(";".to_string()));
        let args = vec![
            ArgumentHandle::new(&text, &ctx),
            ArgumentHandle::new(&col_delim, &ctx),
            ArgumentHandle::new(&row_delim, &ctx),
        ];
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].len(), 2);
                assert_eq!(arr[0][0], LiteralValue::Text("a".to_string()));
                assert_eq!(arr[0][1], LiteralValue::Text("b".to_string()));
                assert_eq!(arr[1][0], LiteralValue::Text("c".to_string()));
                assert_eq!(arr[1][1], LiteralValue::Text("d".to_string()));
            }
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn test_textsplit_ignore_empty() {
        let wb = TestWorkbook::new().with_function(Arc::new(TextSplitFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "TEXTSPLIT").unwrap();

        // Test with consecutive delimiters and ignore_empty=TRUE
        let text = lit(LiteralValue::Text("a,,b".to_string()));
        let delim = lit(LiteralValue::Text(",".to_string()));
        let row_delim = lit(LiteralValue::Empty);
        let ignore_empty = lit(LiteralValue::Boolean(true));
        let args = vec![
            ArgumentHandle::new(&text, &ctx),
            ArgumentHandle::new(&delim, &ctx),
            ArgumentHandle::new(&row_delim, &ctx),
            ArgumentHandle::new(&ignore_empty, &ctx),
        ];
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0].len(), 2);
                assert_eq!(arr[0][0], LiteralValue::Text("a".to_string()));
                assert_eq!(arr[0][1], LiteralValue::Text("b".to_string()));
            }
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn test_textsplit_case_insensitive() {
        let wb = TestWorkbook::new().with_function(Arc::new(TextSplitFn));
        let ctx = interp(&wb);
        let f = ctx.context.get_function("", "TEXTSPLIT").unwrap();

        // Test case-insensitive matching
        let text = lit(LiteralValue::Text("aXbxc".to_string()));
        let delim = lit(LiteralValue::Text("X".to_string()));
        let row_delim = lit(LiteralValue::Empty);
        let ignore_empty = lit(LiteralValue::Boolean(false));
        let match_mode = lit(LiteralValue::Number(1.0)); // case-insensitive
        let args = vec![
            ArgumentHandle::new(&text, &ctx),
            ArgumentHandle::new(&delim, &ctx),
            ArgumentHandle::new(&row_delim, &ctx),
            ArgumentHandle::new(&ignore_empty, &ctx),
            ArgumentHandle::new(&match_mode, &ctx),
        ];
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0].len(), 3);
                assert_eq!(arr[0][0], LiteralValue::Text("a".to_string()));
                assert_eq!(arr[0][1], LiteralValue::Text("b".to_string()));
                assert_eq!(arr[0][2], LiteralValue::Text("c".to_string()));
            }
            v => panic!("unexpected {v:?}"),
        }
    }
}
