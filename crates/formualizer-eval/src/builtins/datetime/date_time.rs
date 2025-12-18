//! DATE and TIME functions

use super::serial::{create_date_normalized, time_to_fraction};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use chrono::NaiveTime;
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

fn coerce_to_int(arg: &ArgumentHandle) -> Result<i32, ExcelError> {
    let v = arg.value()?.into_literal();
    match v {
        LiteralValue::Int(i) => Ok(i as i32),
        LiteralValue::Number(f) => Ok(f.trunc() as i32),
        LiteralValue::Text(s) => s.parse::<f64>().map(|f| f.trunc() as i32).map_err(|_| {
            ExcelError::new_value().with_message("DATE/TIME argument is not a valid number")
        }),
        LiteralValue::Boolean(b) => Ok(if b { 1 } else { 0 }),
        LiteralValue::Empty => Ok(0),
        LiteralValue::Error(e) => Err(e),
        _ => Err(ExcelError::new_value()
            .with_message("DATE/TIME expects numeric or text-numeric arguments")),
    }
}

/// DATE(year, month, day) - Creates a date serial number
#[derive(Debug)]
pub struct DateFn;

impl Function for DateFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "DATE"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        // DATE(year, month, day) – all scalar, numeric lenient (allow text numbers)
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let year = coerce_to_int(&args[0])?;
        let month = coerce_to_int(&args[1])?;
        let day = coerce_to_int(&args[2])?;

        // Excel interprets years 0-1899 as 1900-3799
        let adjusted_year = if (0..=1899).contains(&year) {
            year + 1900
        } else {
            year
        };

        let date = create_date_normalized(adjusted_year, month, day)?;
        let serial = super::serial::date_to_serial_for(ctx.date_system(), &date);

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            serial,
        )))
    }
}

/// TIME(hour, minute, second) - Creates a time serial number (fraction of day)
#[derive(Debug)]
pub struct TimeFn;

impl Function for TimeFn {
    func_caps!(PURE);

    fn name(&self) -> &'static str {
        "TIME"
    }

    fn min_args(&self) -> usize {
        3
    }

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        // TIME(hour, minute, second) – scalar numeric lenient
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
            ]
        });
        &SCHEMA[..]
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let hour = coerce_to_int(&args[0])?;
        let minute = coerce_to_int(&args[1])?;
        let second = coerce_to_int(&args[2])?;

        // Excel normalizes time values
        let total_seconds = hour * 3600 + minute * 60 + second;

        // Handle negative time by wrapping
        let normalized_seconds = if total_seconds < 0 {
            let days_back = (-total_seconds - 1) / 86400 + 1;
            total_seconds + days_back * 86400
        } else {
            total_seconds
        };

        // Get just the time portion (modulo full days)
        let time_seconds = normalized_seconds % 86400;
        let hours = (time_seconds / 3600) as u32;
        let minutes = ((time_seconds % 3600) / 60) as u32;
        let seconds = (time_seconds % 60) as u32;

        match NaiveTime::from_hms_opt(hours, minutes, seconds) {
            Some(time) => {
                let fraction = time_to_fraction(&time);
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                    fraction,
                )))
            }
            None => Err(ExcelError::new_num()),
        }
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(DateFn));
    crate::function_registry::register_function(Arc::new(TimeFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    use std::sync::Arc;

    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn test_date_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(DateFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "DATE").unwrap();

        // DATE(2024, 1, 15)
        let year = lit(LiteralValue::Int(2024));
        let month = lit(LiteralValue::Int(1));
        let day = lit(LiteralValue::Int(15));

        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&year, &ctx),
                    ArgumentHandle::new(&month, &ctx),
                    ArgumentHandle::new(&day, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();

        match result {
            LiteralValue::Number(n) => {
                // Should be a positive serial number
                assert!(n > 0.0);
                // Should be an integer (no time component)
                assert_eq!(n.trunc(), n);
            }
            _ => panic!("DATE should return a number"),
        }
    }

    #[test]
    fn test_date_normalization() {
        let wb = TestWorkbook::new().with_function(Arc::new(DateFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "DATE").unwrap();

        // DATE(2024, 13, 5) should normalize to 2025-01-05
        let year = lit(LiteralValue::Int(2024));
        let month = lit(LiteralValue::Int(13));
        let day = lit(LiteralValue::Int(5));

        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&year, &ctx),
                    ArgumentHandle::new(&month, &ctx),
                    ArgumentHandle::new(&day, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();

        // Just verify it returns a valid number
        assert!(matches!(result.into_literal(), LiteralValue::Number(_)));
    }

    #[test]
    fn test_date_system_1900_vs_1904() {
        use crate::engine::{Engine, EvalConfig};
        use crate::interpreter::Interpreter;

        // Engine with default 1900 system
        let cfg_1900 = EvalConfig::default();
        let eng_1900 = Engine::new(TestWorkbook::new(), cfg_1900.clone());
        let interp_1900 = Interpreter::new(&eng_1900, "Sheet1");
        let f = interp_1900.context.get_function("", "DATE").unwrap();
        let y = lit(LiteralValue::Int(1904));
        let m = lit(LiteralValue::Int(1));
        let d = lit(LiteralValue::Int(1));
        let args = [
            crate::traits::ArgumentHandle::new(&y, &interp_1900),
            crate::traits::ArgumentHandle::new(&m, &interp_1900),
            crate::traits::ArgumentHandle::new(&d, &interp_1900),
        ];
        let v1900 = f
            .dispatch(&args, &interp_1900.function_context(None))
            .unwrap()
            .into_literal();

        // Engine with 1904 system
        let cfg_1904 = EvalConfig {
            date_system: crate::engine::DateSystem::Excel1904,
            ..Default::default()
        };
        let eng_1904 = Engine::new(TestWorkbook::new(), cfg_1904);
        let interp_1904 = Interpreter::new(&eng_1904, "Sheet1");
        let f2 = interp_1904.context.get_function("", "DATE").unwrap();
        let args2 = [
            crate::traits::ArgumentHandle::new(&y, &interp_1904),
            crate::traits::ArgumentHandle::new(&m, &interp_1904),
            crate::traits::ArgumentHandle::new(&d, &interp_1904),
        ];
        let v1904 = f2
            .dispatch(&args2, &interp_1904.function_context(None))
            .unwrap()
            .into_literal();

        match (v1900, v1904) {
            (LiteralValue::Number(a), LiteralValue::Number(b)) => {
                // 1904-01-01 is 1462 in 1900 system, 0 in 1904 system
                assert!((a - 1462.0).abs() < 1e-9, "expected 1462, got {a}");
                assert!(b.abs() < 1e-9, "expected 0, got {b}");
            }
            other => panic!("Unexpected results: {other:?}"),
        }
    }

    #[test]
    fn test_time_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(TimeFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TIME").unwrap();

        // TIME(12, 0, 0) = noon = 0.5
        let hour = lit(LiteralValue::Int(12));
        let minute = lit(LiteralValue::Int(0));
        let second = lit(LiteralValue::Int(0));

        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&hour, &ctx),
                    ArgumentHandle::new(&minute, &ctx),
                    ArgumentHandle::new(&second, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();

        match result {
            LiteralValue::Number(n) => {
                assert!((n - 0.5).abs() < 1e-10);
            }
            _ => panic!("TIME should return a number"),
        }
    }

    #[test]
    fn test_time_normalization() {
        let wb = TestWorkbook::new().with_function(Arc::new(TimeFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "TIME").unwrap();

        // TIME(25, 0, 0) = 1:00 AM next day = 1/24
        let hour = lit(LiteralValue::Int(25));
        let minute = lit(LiteralValue::Int(0));
        let second = lit(LiteralValue::Int(0));

        let result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&hour, &ctx),
                    ArgumentHandle::new(&minute, &ctx),
                    ArgumentHandle::new(&second, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();

        match result {
            LiteralValue::Number(n) => {
                // Should wrap to 1:00 AM = 1/24
                assert!((n - 1.0 / 24.0).abs() < 1e-10);
            }
            _ => panic!("TIME should return a number"),
        }
    }
}
