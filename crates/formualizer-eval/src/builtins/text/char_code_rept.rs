//! CHAR, CODE, REPT text functions

use super::super::utils::{ARG_ANY_ONE, ARG_ANY_TWO, coerce_num};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

fn scalar_like_value(arg: &ArgumentHandle<'_, '_>) -> Result<LiteralValue, ExcelError> {
    Ok(match arg.value()? {
        CalcValue::Scalar(v) => v,
        CalcValue::Range(rv) => rv.get_cell(0, 0),
    })
}

/// CHAR(number) - Returns the character specified by a number
/// Excel uses Windows-1252 encoding for codes 1-255
#[derive(Debug)]
pub struct CharFn;
impl Function for CharFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CHAR"
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

        let code = n.trunc() as i32;

        // Excel CHAR accepts 1-255
        if code < 1 || code > 255 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        // Windows-1252 to Unicode mapping for codes 128-159
        let unicode_char = match code as u8 {
            0x80 => '\u{20AC}', // Euro sign
            0x82 => '\u{201A}', // Single low-9 quotation mark
            0x83 => '\u{0192}', // Latin small letter f with hook
            0x84 => '\u{201E}', // Double low-9 quotation mark
            0x85 => '\u{2026}', // Horizontal ellipsis
            0x86 => '\u{2020}', // Dagger
            0x87 => '\u{2021}', // Double dagger
            0x88 => '\u{02C6}', // Modifier letter circumflex accent
            0x89 => '\u{2030}', // Per mille sign
            0x8A => '\u{0160}', // Latin capital letter S with caron
            0x8B => '\u{2039}', // Single left-pointing angle quotation mark
            0x8C => '\u{0152}', // Latin capital ligature OE
            0x8E => '\u{017D}', // Latin capital letter Z with caron
            0x91 => '\u{2018}', // Left single quotation mark
            0x92 => '\u{2019}', // Right single quotation mark
            0x93 => '\u{201C}', // Left double quotation mark
            0x94 => '\u{201D}', // Right double quotation mark
            0x95 => '\u{2022}', // Bullet
            0x96 => '\u{2013}', // En dash
            0x97 => '\u{2014}', // Em dash
            0x98 => '\u{02DC}', // Small tilde
            0x99 => '\u{2122}', // Trade mark sign
            0x9A => '\u{0161}', // Latin small letter s with caron
            0x9B => '\u{203A}', // Single right-pointing angle quotation mark
            0x9C => '\u{0153}', // Latin small ligature oe
            0x9E => '\u{017E}', // Latin small letter z with caron
            0x9F => '\u{0178}', // Latin capital letter Y with diaeresis
            0x81 | 0x8D | 0x8F | 0x90 | 0x9D => {
                // Undefined in Windows-1252, return placeholder
                '\u{FFFD}'
            }
            c => char::from(c),
        };

        Ok(CalcValue::Scalar(LiteralValue::Text(
            unicode_char.to_string(),
        )))
    }
}

/// CODE(text) - Returns a numeric code for the first character in a text string
#[derive(Debug)]
pub struct CodeFn;
impl Function for CodeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CODE"
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
        let s = match v {
            LiteralValue::Text(t) => t,
            LiteralValue::Empty => {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => other.to_string(),
        };

        if s.is_empty() {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let first_char = s.chars().next().unwrap();

        // Map Unicode back to Windows-1252 for Excel compatibility
        let code = match first_char {
            '\u{20AC}' => 0x80, // Euro sign
            '\u{201A}' => 0x82, // Single low-9 quotation mark
            '\u{0192}' => 0x83, // Latin small letter f with hook
            '\u{201E}' => 0x84, // Double low-9 quotation mark
            '\u{2026}' => 0x85, // Horizontal ellipsis
            '\u{2020}' => 0x86, // Dagger
            '\u{2021}' => 0x87, // Double dagger
            '\u{02C6}' => 0x88, // Modifier letter circumflex accent
            '\u{2030}' => 0x89, // Per mille sign
            '\u{0160}' => 0x8A, // Latin capital letter S with caron
            '\u{2039}' => 0x8B, // Single left-pointing angle quotation mark
            '\u{0152}' => 0x8C, // Latin capital ligature OE
            '\u{017D}' => 0x8E, // Latin capital letter Z with caron
            '\u{2018}' => 0x91, // Left single quotation mark
            '\u{2019}' => 0x92, // Right single quotation mark
            '\u{201C}' => 0x93, // Left double quotation mark
            '\u{201D}' => 0x94, // Right double quotation mark
            '\u{2022}' => 0x95, // Bullet
            '\u{2013}' => 0x96, // En dash
            '\u{2014}' => 0x97, // Em dash
            '\u{02DC}' => 0x98, // Small tilde
            '\u{2122}' => 0x99, // Trade mark sign
            '\u{0161}' => 0x9A, // Latin small letter s with caron
            '\u{203A}' => 0x9B, // Single right-pointing angle quotation mark
            '\u{0153}' => 0x9C, // Latin small ligature oe
            '\u{017E}' => 0x9E, // Latin small letter z with caron
            '\u{0178}' => 0x9F, // Latin capital letter Y with diaeresis
            c if (c as u32) < 256 => c as i64,
            c => c as i64, // For characters outside Windows-1252, return Unicode code point
        };

        Ok(CalcValue::Scalar(LiteralValue::Int(code)))
    }
}

/// REPT(text, number_times) - Repeats text a given number of times
#[derive(Debug)]
pub struct ReptFn;
impl Function for ReptFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "REPT"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_TWO[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let text_val = scalar_like_value(&args[0])?;
        let count_val = scalar_like_value(&args[1])?;

        let text = match text_val {
            LiteralValue::Text(t) => t,
            LiteralValue::Empty => String::new(),
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => other.to_string(),
        };

        let count = match count_val {
            LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
            other => coerce_num(&other)?,
        };

        let count = count.trunc() as i64;

        if count < 0 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        // Excel limits result to 32767 characters
        let max_result_len = 32767;
        let result_len = text.len() * (count as usize);
        if result_len > max_result_len {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let result = text.repeat(count as usize);
        Ok(CalcValue::Scalar(LiteralValue::Text(result)))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(CharFn));
    crate::function_registry::register_function(Arc::new(CodeFn));
    crate::function_registry::register_function(Arc::new(ReptFn));
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
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn char_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CharFn));
        let ctx = interp(&wb);
        let n = lit(LiteralValue::Number(65.0));
        let f = ctx.context.get_function("", "CHAR").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Text("A".to_string())
        );
    }

    #[test]
    fn code_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CodeFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Text("A".to_string()));
        let f = ctx.context.get_function("", "CODE").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&s, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Int(65)
        );
    }

    #[test]
    fn rept_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ReptFn));
        let ctx = interp(&wb);
        let s = lit(LiteralValue::Text("ab".to_string()));
        let n = lit(LiteralValue::Number(3.0));
        let f = ctx.context.get_function("", "REPT").unwrap();
        assert_eq!(
            f.dispatch(
                &[ArgumentHandle::new(&s, &ctx), ArgumentHandle::new(&n, &ctx)],
                &ctx.function_context(None)
            )
            .unwrap()
            .into_literal(),
            LiteralValue::Text("ababab".to_string())
        );
    }
}
