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

/// Returns straight-line depreciation for a single period.
///
/// `SLN` spreads the depreciable amount (`cost - salvage`) evenly across `life` periods.
///
/// # Remarks
/// - Formula: `(cost - salvage) / life`.
/// - `life` must be non-zero; `life = 0` returns `#DIV/0!`.
/// - This function returns the algebraic result: if `salvage > cost`, depreciation is negative.
/// - Inputs are interpreted as scalar numeric values in matching currency/period units.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Straight-line yearly depreciation"
/// formula: "=SLN(10000, 1000, 9)"
/// expected: 1000
/// ```
///
/// ```yaml,sandbox
/// title: "Negative depreciation when salvage exceeds cost"
/// formula: "=SLN(1000, 1200, 2)"
/// expected: -100
/// ```
/// ```yaml,docs
/// related:
///   - SYD
///   - DB
///   - DDB
/// faq:
///   - q: "Can `SLN` return a negative value?"
///     a: "Yes. If `salvage > cost`, `(cost - salvage) / life` is negative."
///   - q: "What happens when `life` is zero?"
///     a: "`SLN` returns `#DIV/0!`."
/// ```
#[derive(Debug)]
pub struct SlnFn;
/// [formualizer-docgen:schema:start]
/// Name: SLN
/// Type: SlnFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: SLN(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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

/// Returns sum-of-years'-digits depreciation for a requested period.
///
/// `SYD` applies accelerated depreciation by weighting earlier periods more heavily.
///
/// # Remarks
/// - Formula: `(cost - salvage) * (life - per + 1) / (life * (life + 1) / 2)`.
/// - `life` and `per` must satisfy: `life > 0`, `per > 0`, and `per <= life`; otherwise returns `#NUM!`.
/// - The function uses the provided numeric values directly (no integer-only enforcement).
/// - Result sign follows `(cost - salvage)`: positive for typical depreciation expense, negative if `salvage > cost`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "First SYD period"
/// formula: "=SYD(10000, 1000, 5, 1)"
/// expected: 3000
/// ```
///
/// ```yaml,sandbox
/// title: "Final SYD period"
/// formula: "=SYD(10000, 1000, 5, 5)"
/// expected: 600
/// ```
/// ```yaml,docs
/// related:
///   - SLN
///   - DB
///   - DDB
/// faq:
///   - q: "Does `SYD` require integer `life` and `per`?"
///     a: "No strict integer check is enforced; it uses provided numeric values directly after domain validation."
///   - q: "Which period values are valid?"
///     a: "`per` must satisfy `0 < per <= life`, and `life` must be positive; otherwise `#NUM!` is returned."
/// ```
#[derive(Debug)]
pub struct SydFn;
/// [formualizer-docgen:schema:start]
/// Name: SYD
/// Type: SydFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: SYD(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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

/// Returns fixed-declining-balance depreciation for a specified period.
///
/// `DB` computes per-period depreciation using a declining-balance rate and an optional
/// first-year month proration.
///
/// # Remarks
/// - Parameters: `cost`, `salvage`, `life`, `period`, and optional `month` (default `12`).
/// - `month` must be in `1..=12`; `life` and `period` must be positive; invalid values return `#NUM!`.
/// - `life` and `period` are truncated to integers for period checks and iteration.
/// - The declining rate is rounded to three decimals; if `cost <= 0` or `salvage <= 0`, this implementation uses a rate of `1.0`.
/// - Returned value is the period depreciation amount (generally positive expense, but sign follows provided inputs).
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "First full-year DB period"
/// formula: "=DB(10000, 1000, 5, 1)"
/// expected: 3690
/// ```
///
/// ```yaml,sandbox
/// title: "Fractional period input is truncated"
/// formula: "=DB(10000, 1000, 5, 2.9)"
/// expected: 2328.39
/// ```
/// ```yaml,docs
/// related:
///   - DDB
///   - SYD
///   - SLN
/// faq:
///   - q: "How is `month` used in `DB`?"
///     a: "`month` prorates the first-year depreciation; if omitted it defaults to `12`."
///   - q: "Why can fractional `period` inputs behave like integers?"
///     a: "`DB` truncates `life` and `period` to integers for iteration and period bounds."
/// ```
#[derive(Debug)]
pub struct DbFn;
/// [formualizer-docgen:schema:start]
/// Name: DB
/// Type: DbFn
/// Min args: 4
/// Max args: variadic
/// Variadic: true
/// Signature: DB(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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

        if life <= 0.0 || period <= 0.0 || !(1.0..=12.0).contains(&month) {
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

/// Returns declining-balance depreciation for a period using a configurable acceleration factor.
///
/// `DDB` defaults to the double-declining method (`factor = 2`) and applies a salvage floor so
/// book value does not fall below `salvage`.
///
/// # Remarks
/// - Parameters: `cost`, `salvage`, `life`, `period`, and optional `factor` (default `2`).
/// - Input constraints: `cost >= 0`, `salvage >= 0`, `life > 0`, `period > 0`, `factor > 0`, and `period <= life`; violations return `#NUM!`.
/// - Per-period rate is `factor / life`.
/// - This implementation processes the integer part of `period` and then blends with the next period for a fractional remainder.
/// - Result is the period depreciation amount; with valid inputs above it is non-negative.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Default double-declining first period"
/// formula: "=DDB(10000, 1000, 5, 1)"
/// expected: 4000
/// ```
///
/// ```yaml,sandbox
/// title: "Using a custom factor"
/// formula: "=DDB(10000, 1000, 5, 1, 1.5)"
/// expected: 3000
/// ```
/// ```yaml,docs
/// related:
///   - DB
///   - SYD
///   - SLN
/// faq:
///   - q: "What does the optional `factor` control?"
///     a: "It sets the per-period declining rate as `factor / life`; `2` gives double-declining balance."
///   - q: "When does `DDB` return `#NUM!`?"
///     a: "Invalid non-positive inputs (`life`, `period`, `factor`), negative `cost`/`salvage`, or `period > life`."
/// ```
#[derive(Debug)]
pub struct DdbFn;
/// [formualizer-docgen:schema:start]
/// Name: DDB
/// Type: DdbFn
/// Min args: 4
/// Max args: variadic
/// Variadic: true
/// Signature: DDB(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
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
