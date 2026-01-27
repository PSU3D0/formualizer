//! Depreciation functions: SLN, SYD, DB, DDB

use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

fn coerce_num(arg: &ArgumentHandle) -> Result<f64, ExcelError> {
    let v = arg.value()?.into_literal();
    match v {
        LiteralValue::Number(f) => Ok(f),
        LiteralValue::Int(i) => Ok(i as f64),
        LiteralValue::Boolean(b) => Ok(if b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        LiteralValue::Error(e) => Err(e),
        _ => Err(ExcelError::new_value()),
    }
}

/// SLN(cost, salvage, life)
/// Returns the straight-line depreciation of an asset for one period
#[derive(Debug)]
pub struct SlnFn;
impl Function for SlnFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SLN"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
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
    ) -> Result<CalcValue<'b>, ExcelError> {
        let cost = coerce_num(&args[0])?;
        let salvage = coerce_num(&args[1])?;
        let life = coerce_num(&args[2])?;

        if life == 0.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_div()),
            ));
        }

        let depreciation = (cost - salvage) / life;
        Ok(CalcValue::Scalar(LiteralValue::Number(depreciation)))
    }
}

/// SYD(cost, salvage, life, per)
/// Returns the sum-of-years' digits depreciation of an asset for a specified period
#[derive(Debug)]
pub struct SydFn;
impl Function for SydFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SYD"
    }
    fn min_args(&self) -> usize {
        4
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
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
    ) -> Result<CalcValue<'b>, ExcelError> {
        let cost = coerce_num(&args[0])?;
        let salvage = coerce_num(&args[1])?;
        let life = coerce_num(&args[2])?;
        let per = coerce_num(&args[3])?;

        if life <= 0.0 || per <= 0.0 || per > life {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Sum of years = life * (life + 1) / 2
        let sum_of_years = life * (life + 1.0) / 2.0;

        // SYD = (cost - salvage) * (life - per + 1) / sum_of_years
        let depreciation = (cost - salvage) * (life - per + 1.0) / sum_of_years;

        Ok(CalcValue::Scalar(LiteralValue::Number(depreciation)))
    }
}

/// DB(cost, salvage, life, period, [month])
/// Returns the depreciation of an asset for a specified period using the fixed-declining balance method
#[derive(Debug)]
pub struct DbFn;
impl Function for DbFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DB"
    }
    fn min_args(&self) -> usize {
        4
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
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
    ) -> Result<CalcValue<'b>, ExcelError> {
        let cost = coerce_num(&args[0])?;
        let salvage = coerce_num(&args[1])?;
        let life = coerce_num(&args[2])?;
        let period = coerce_num(&args[3])?;
        let month = if args.len() > 4 {
            coerce_num(&args[4])?
        } else {
            12.0
        };

        if life <= 0.0 || period <= 0.0 || month < 1.0 || month > 12.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let life_int = life.trunc() as i32;
        let period_int = period.trunc() as i32;

        if period_int < 1 || period_int > life_int + 1 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Calculate rate (rounded to 3 decimal places)
        let rate = if cost <= 0.0 || salvage <= 0.0 {
            1.0
        } else {
            let r = 1.0 - (salvage / cost).powf(1.0 / life);
            (r * 1000.0).round() / 1000.0
        };

        let mut total_depreciation = 0.0;
        let value = cost;

        for p in 1..=period_int {
            let depreciation = if p == 1 {
                // First period: prorated
                value * rate * month / 12.0
            } else if p == life_int + 1 {
                // Last period (if partial year): remaining value minus salvage
                (value - total_depreciation - salvage)
                    .max(0.0)
                    .min(value - total_depreciation)
            } else {
                (value - total_depreciation) * rate
            };

            if p == period_int {
                return Ok(CalcValue::Scalar(LiteralValue::Number(depreciation)));
            }

            total_depreciation += depreciation;
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(0.0)))
    }
}

/// DDB(cost, salvage, life, period, [factor])
/// Returns the depreciation of an asset for a specified period using the double-declining balance method
///
/// TODO: KNOWN ISSUES - This implementation has correctness issues that need to be fixed:
/// 1. Fractional period handling is incorrect - Excel does NOT support fractional periods
///    for DDB and returns an error. The current implementation attempts a weighted average
///    approach which doesn't match Excel behavior.
/// 2. Salvage value logic may not match Excel exactly - Excel's DDB doesn't consider salvage
///    during the per-period depreciation calculation; it only prevents cumulative depreciation
///    from exceeding (cost - salvage).
/// See merge-review/03-financial.md for full analysis.
#[derive(Debug)]
pub struct DdbFn;
impl Function for DdbFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DDB"
    }
    fn min_args(&self) -> usize {
        4
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(),
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
    ) -> Result<CalcValue<'b>, ExcelError> {
        let cost = coerce_num(&args[0])?;
        let salvage = coerce_num(&args[1])?;
        let life = coerce_num(&args[2])?;
        let period = coerce_num(&args[3])?;
        let factor = if args.len() > 4 {
            coerce_num(&args[4])?
        } else {
            2.0
        };

        if cost < 0.0 || salvage < 0.0 || life <= 0.0 || period <= 0.0 || factor <= 0.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        if period > life {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let rate = factor / life;
        let mut value = cost;
        let mut depreciation = 0.0;

        for p in 1..=(period.trunc() as i32) {
            depreciation = value * rate;
            // Don't depreciate below salvage value
            if value - depreciation < salvage {
                depreciation = (value - salvage).max(0.0);
            }
            value -= depreciation;
        }

        // TODO: Handle fractional period - this logic is incorrect and doesn't match Excel
        // Excel returns an error for non-integer periods. This weighted average approach
        // should be removed or replaced with proper error handling.
        let frac = period.fract();
        if frac > 0.0 {
            let next_depreciation = value * rate;
            let next_depreciation = if value - next_depreciation < salvage {
                (value - salvage).max(0.0)
            } else {
                next_depreciation
            };
            depreciation = depreciation * (1.0 - frac) + next_depreciation * frac;
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(depreciation)))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(SlnFn));
    crate::function_registry::register_function(Arc::new(SydFn));
    crate::function_registry::register_function(Arc::new(DbFn));
    crate::function_registry::register_function(Arc::new(DdbFn));
}
