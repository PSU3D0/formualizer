//! Time Value of Money functions: PMT, PV, FV, NPV, NPER, RATE, IPMT, PPMT, XNPV, XIRR, DOLLARDE, DOLLARFR

use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;

fn coerce_num(arg: &ArgumentHandle) -> Result<f64, ExcelError> {
    let v = arg.value()?.into_literal();
    coerce_literal_num(&v)
}

fn coerce_literal_num(v: &LiteralValue) -> Result<f64, ExcelError> {
    match v {
        LiteralValue::Number(f) => Ok(*f),
        LiteralValue::Int(i) => Ok(*i as f64),
        LiteralValue::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        LiteralValue::Error(e) => Err(e.clone()),
        _ => Err(ExcelError::new_value()),
    }
}

/// Calculates the constant payment amount for a fixed-rate annuity or loan.
///
/// Use this to solve for periodic payment size when rate, term, and present/future value
/// targets are known.
///
/// # Remarks
/// - `rate` is the interest rate per payment period (for example, annual rate / 12 for monthly payments).
/// - Cash-flow sign convention: cash paid out is negative and cash received is positive.
/// - `type = 0` means end-of-period payments; `type != 0` means beginning-of-period payments.
/// - Returns `#NUM!` when `nper` is zero.
/// - Propagates argument conversion and underlying value errors.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =PMT(0.06/12, 360, 300000)
/// result: -1798.6515754582708
/// ```
/// ```yaml,sandbox
/// formula: =PMT(0.05/4, 20, -10000, 0, 1)
/// result: 561.1890334005388
/// ```
#[derive(Debug)]
pub struct PmtFn;
/// [formualizer-docgen:schema:start]
/// Name: PMT
/// Type: PmtFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: PMT(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for PmtFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PMT"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(), // rate
                ArgSchema::number_lenient_scalar(), // nper
                ArgSchema::number_lenient_scalar(), // pv
                ArgSchema::number_lenient_scalar(), // fv (optional)
                ArgSchema::number_lenient_scalar(), // type (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let rate = coerce_num(&args[0])?;
        let nper = coerce_num(&args[1])?;
        let pv = coerce_num(&args[2])?;
        let fv = if args.len() > 3 {
            coerce_num(&args[3])?
        } else {
            0.0
        };
        let pmt_type = if args.len() > 4 {
            coerce_num(&args[4])? as i32
        } else {
            0
        };

        if nper == 0.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let pmt = if rate.abs() < 1e-10 {
            // When rate is 0, PMT = -(pv + fv) / nper
            -(pv + fv) / nper
        } else {
            // PMT = (rate * (pv * (1+rate)^nper + fv)) / ((1+rate)^nper - 1)
            // With type adjustment for beginning of period
            let factor = (1.0 + rate).powf(nper);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -(rate * (pv * factor + fv)) / ((factor - 1.0) * type_adj)
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(pmt)))
    }
}

/// Calculates present value from periodic cash flows at a fixed rate.
///
/// Use this to discount a regular payment stream and optional terminal value back to time zero.
///
/// # Remarks
/// - `rate` is the discount rate per period.
/// - Cash-flow sign convention: inflows are positive and outflows are negative.
/// - `type = 0` assumes payments at period end; `type != 0` assumes period start.
/// - When `rate` is zero, present value is computed with simple arithmetic (no discounting).
/// - Returns argument-related errors if coercion fails or an input is an error value.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =PV(0.06/12, 360, -1798.65157545827)
/// result: 299999.9999999998
/// ```
/// ```yaml,sandbox
/// formula: =PV(0, 10, -500)
/// result: 5000
/// ```
#[derive(Debug)]
pub struct PvFn;
/// [formualizer-docgen:schema:start]
/// Name: PV
/// Type: PvFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: PV(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for PvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PV"
    }
    fn min_args(&self) -> usize {
        3
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
        let rate = coerce_num(&args[0])?;
        let nper = coerce_num(&args[1])?;
        let pmt = coerce_num(&args[2])?;
        let fv = if args.len() > 3 {
            coerce_num(&args[3])?
        } else {
            0.0
        };
        let pmt_type = if args.len() > 4 {
            coerce_num(&args[4])? as i32
        } else {
            0
        };

        let pv = if rate.abs() < 1e-10 {
            -fv - pmt * nper
        } else {
            let factor = (1.0 + rate).powf(nper);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            (-fv - pmt * type_adj * (factor - 1.0) / rate) / factor
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(pv)))
    }
}

/// Calculates future value from a fixed periodic rate and payment stream.
///
/// Use this to project an ending balance after compounding a present value and periodic payments.
///
/// # Remarks
/// - `rate` is the interest rate per period.
/// - Cash-flow sign convention: payments you make are negative; receipts are positive.
/// - `type = 0` models end-of-period payments; `type != 0` models beginning-of-period payments.
/// - When `rate` is zero, result is linear (`-pv - pmt * nper`).
/// - Returns argument-related errors if coercion fails or an input is an error value.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =FV(0.04/12, 120, -200)
/// result: 29449.96094509572
/// ```
/// ```yaml,sandbox
/// formula: =FV(0, 24, -150, 1000)
/// result: 2600
/// ```
#[derive(Debug)]
pub struct FvFn;
/// [formualizer-docgen:schema:start]
/// Name: FV
/// Type: FvFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: FV(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FV"
    }
    fn min_args(&self) -> usize {
        3
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
        let rate = coerce_num(&args[0])?;
        let nper = coerce_num(&args[1])?;
        let pmt = coerce_num(&args[2])?;
        let pv = if args.len() > 3 {
            coerce_num(&args[3])?
        } else {
            0.0
        };
        let pmt_type = if args.len() > 4 {
            coerce_num(&args[4])? as i32
        } else {
            0
        };

        let fv = if rate.abs() < 1e-10 {
            -pv - pmt * nper
        } else {
            let factor = (1.0 + rate).powf(nper);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -pv * factor - pmt * type_adj * (factor - 1.0) / rate
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(fv)))
    }
}

/// Calculates net present value for equally spaced cash flows.
///
/// The first cash-flow argument is discounted one period from the present, matching spreadsheet
/// `NPV` behavior for periodic series.
///
/// # Remarks
/// - `rate` is the discount rate per period.
/// - Cash-flow sign convention: investments/outflows are negative, returns/inflows are positive.
/// - Non-numeric values are ignored; numeric values in arrays/ranges are consumed left-to-right.
/// - Embedded error values inside provided cash-flow values are propagated as errors.
/// - Returns argument coercion errors for invalid `rate` or direct scalar failures.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =NPV(0.08, 4000, 5000, 6000)
/// result: 12753.391251333636
/// ```
/// ```yaml,sandbox
/// formula: =NPV(0.10, -5000, 2000, 2500, 3000)
/// result: 1034.7653848780812
/// ```
#[derive(Debug)]
pub struct NpvFn;
/// [formualizer-docgen:schema:start]
/// Name: NPV
/// Type: NpvFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: NPV(arg1: number@scalar, arg2...: any@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NpvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NPV"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar(), ArgSchema::any()]);
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let rate = coerce_num(&args[0])?;

        let mut npv = 0.0;
        let mut period = 1;

        for arg in &args[1..] {
            let v = arg.value()?.into_literal();
            match v {
                LiteralValue::Number(n) => {
                    npv += n / (1.0 + rate).powi(period);
                    period += 1;
                }
                LiteralValue::Int(i) => {
                    npv += (i as f64) / (1.0 + rate).powi(period);
                    period += 1;
                }
                LiteralValue::Error(e) => {
                    return Ok(CalcValue::Scalar(LiteralValue::Error(e)));
                }
                LiteralValue::Array(arr) => {
                    for row in arr {
                        for cell in row {
                            match cell {
                                LiteralValue::Number(n) => {
                                    npv += n / (1.0 + rate).powi(period);
                                    period += 1;
                                }
                                LiteralValue::Int(i) => {
                                    npv += (i as f64) / (1.0 + rate).powi(period);
                                    period += 1;
                                }
                                LiteralValue::Error(e) => {
                                    return Ok(CalcValue::Scalar(LiteralValue::Error(e)));
                                }
                                _ => {} // Skip non-numeric values
                            }
                        }
                    }
                }
                _ => {} // Skip non-numeric values
            }
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(npv)))
    }
}

/// Calculates the number of periods needed to satisfy a cash-flow target.
///
/// Use this to solve term length when periodic rate, payment, and value constraints are known.
///
/// # Remarks
/// - `rate` is the interest rate per period.
/// - Cash-flow sign convention: at least one of `pmt`, `pv`, or `fv` should usually have opposite sign.
/// - `type = 0` means payments at period end; `type != 0` means period start.
/// - Returns `#NUM!` when inputs imply no finite solution (for example, invalid logarithm domain).
/// - Returns `#NUM!` when both `rate = 0` and `pmt = 0`.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =NPER(0.06/12, -1798.65157545827, 300000)
/// result: 360.00000000000045
/// ```
/// ```yaml,sandbox
/// formula: =NPER(0, -250, 5000)
/// result: 20
/// ```
#[derive(Debug)]
pub struct NperFn;
/// [formualizer-docgen:schema:start]
/// Name: NPER
/// Type: NperFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: NPER(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NperFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NPER"
    }
    fn min_args(&self) -> usize {
        3
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
        let rate = coerce_num(&args[0])?;
        let pmt = coerce_num(&args[1])?;
        let pv = coerce_num(&args[2])?;
        let fv = if args.len() > 3 {
            coerce_num(&args[3])?
        } else {
            0.0
        };
        let pmt_type = if args.len() > 4 {
            coerce_num(&args[4])? as i32
        } else {
            0
        };

        let nper = if rate.abs() < 1e-10 {
            if pmt.abs() < 1e-10 {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }
            -(pv + fv) / pmt
        } else {
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            let pmt_adj = pmt * type_adj;
            let numerator = pmt_adj - fv * rate;
            let denominator = pv * rate + pmt_adj;
            if numerator / denominator <= 0.0 {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }
            (numerator / denominator).ln() / (1.0 + rate).ln()
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(nper)))
    }
}

/// Solves for the periodic interest rate implied by annuity cash flows.
///
/// This function uses Newton-Raphson iteration and returns the per-period rate that satisfies
/// the TVM equation.
///
/// # Remarks
/// - Output is a rate per period; convert to annual terms externally if needed.
/// - Cash-flow sign convention matters for convergence: use opposite signs for borrow/repay sides.
/// - `guess` defaults to `0.1` and influences convergence speed and branch selection.
/// - `type = 0` means end-of-period payments; `type != 0` means beginning-of-period payments.
/// - Returns `#NUM!` on non-convergence, near-zero derivative, or unsatisfied numeric conditions.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =RATE(360, -1798.65157545827, 300000)
/// result: 0.005000000000000038
/// ```
/// ```yaml,sandbox
/// formula: =RATE(12, -88.84878867834166, 1000)
/// result: 0.010000000000005125
/// ```
#[derive(Debug)]
pub struct RateFn;
/// [formualizer-docgen:schema:start]
/// Name: RATE
/// Type: RateFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: RATE(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5: number@scalar, arg6...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg6{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for RateFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "RATE"
    }
    fn min_args(&self) -> usize {
        3
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
        let nper = coerce_num(&args[0])?;
        let pmt = coerce_num(&args[1])?;
        let pv = coerce_num(&args[2])?;
        let fv = if args.len() > 3 {
            coerce_num(&args[3])?
        } else {
            0.0
        };
        let pmt_type = if args.len() > 4 {
            coerce_num(&args[4])? as i32
        } else {
            0
        };
        let guess = if args.len() > 5 {
            coerce_num(&args[5])?
        } else {
            0.1
        };

        // Newton-Raphson iteration to find rate
        let mut rate = guess;
        let max_iter = 100;
        let tolerance = 1e-10;

        for _ in 0..max_iter {
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };

            if rate.abs() < 1e-10 {
                // Special case for very small rate
                let f = pv + pmt * nper + fv;
                if f.abs() < tolerance {
                    return Ok(CalcValue::Scalar(LiteralValue::Number(rate)));
                }
                rate = 0.01; // Nudge away from zero
                continue;
            }

            let factor = (1.0 + rate).powf(nper);
            let f = pv * factor + pmt * type_adj * (factor - 1.0) / rate + fv;

            // Derivative
            let factor_prime = nper * (1.0 + rate).powf(nper - 1.0);
            let df = pv * factor_prime
                + pmt * type_adj * (factor_prime / rate - (factor - 1.0) / (rate * rate));

            if df.abs() < 1e-20 {
                break;
            }

            let new_rate = rate - f / df;

            if (new_rate - rate).abs() < tolerance {
                return Ok(CalcValue::Scalar(LiteralValue::Number(new_rate)));
            }

            rate = new_rate;

            // Prevent rate from going too negative
            if rate < -0.99 {
                rate = -0.99;
            }
        }

        // If we didn't converge, return error
        Ok(CalcValue::Scalar(
            LiteralValue::Error(ExcelError::new_num()),
        ))
    }
}

/// Returns the interest-only component of a payment for a specific period.
///
/// Use this with `PMT` or `PPMT` to break a fixed payment into interest and principal pieces.
///
/// # Remarks
/// - `rate` is the interest rate per payment period.
/// - `per` is 1-based and must satisfy `1 <= per <= nper`.
/// - Cash-flow sign convention: for a positive loan principal (`pv`), interest components are typically negative.
/// - `type = 1` yields zero interest in period 1 (annuity-due first payment).
/// - Returns `#NUM!` when `per` is outside valid bounds.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =IPMT(0.06/12, 1, 360, 300000)
/// result: -1500
/// ```
/// ```yaml,sandbox
/// formula: =IPMT(0.06/12, 12, 360, 300000)
/// result: -1483.1572957145672
/// ```
#[derive(Debug)]
pub struct IpmtFn;
/// [formualizer-docgen:schema:start]
/// Name: IPMT
/// Type: IpmtFn
/// Min args: 4
/// Max args: variadic
/// Variadic: true
/// Signature: IPMT(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5: number@scalar, arg6...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg6{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IpmtFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IPMT"
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
        let rate = coerce_num(&args[0])?;
        let per = coerce_num(&args[1])?;
        let nper = coerce_num(&args[2])?;
        let pv = coerce_num(&args[3])?;
        let fv = if args.len() > 4 {
            coerce_num(&args[4])?
        } else {
            0.0
        };
        let pmt_type = if args.len() > 5 {
            coerce_num(&args[5])? as i32
        } else {
            0
        };

        if per < 1.0 || per > nper {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Calculate PMT first
        let pmt = if rate.abs() < 1e-10 {
            -(pv + fv) / nper
        } else {
            let factor = (1.0 + rate).powf(nper);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -(rate * (pv * factor + fv)) / ((factor - 1.0) * type_adj)
        };

        // Calculate FV at start of period
        let fv_at_start = if rate.abs() < 1e-10 {
            -pv - pmt * (per - 1.0)
        } else {
            let factor = (1.0 + rate).powf(per - 1.0);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -pv * factor - pmt * type_adj * (factor - 1.0) / rate
        };

        // Interest is rate * balance at start of period
        // fv_at_start is negative of balance, so ipmt = fv_at_start * rate
        let ipmt = if pmt_type != 0 && per == 1.0 {
            0.0 // No interest in first period for annuity due
        } else {
            fv_at_start * rate
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(ipmt)))
    }
}

/// Returns the principal component of a payment for a specific period.
///
/// `PPMT` is computed as `PMT - IPMT` using the same rate, timing, and sign convention.
///
/// # Remarks
/// - `rate` is the interest rate per payment period.
/// - `per` is 1-based and must satisfy `1 <= per <= nper`.
/// - Cash-flow sign convention: with a positive borrowed `pv`, principal components are usually negative.
/// - `type = 1` means beginning-of-period payments.
/// - Returns `#NUM!` when `per` is outside valid bounds.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =PPMT(0.06/12, 1, 360, 300000)
/// result: -298.6515754582708
/// ```
/// ```yaml,sandbox
/// formula: =PPMT(0.06/12, 12, 360, 300000)
/// result: -315.4942797437036
/// ```
#[derive(Debug)]
pub struct PpmtFn;
/// [formualizer-docgen:schema:start]
/// Name: PPMT
/// Type: PpmtFn
/// Min args: 4
/// Max args: variadic
/// Variadic: true
/// Signature: PPMT(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5: number@scalar, arg6...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg6{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for PpmtFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PPMT"
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
        let rate = coerce_num(&args[0])?;
        let per = coerce_num(&args[1])?;
        let nper = coerce_num(&args[2])?;
        let pv = coerce_num(&args[3])?;
        let fv = if args.len() > 4 {
            coerce_num(&args[4])?
        } else {
            0.0
        };
        let pmt_type = if args.len() > 5 {
            coerce_num(&args[5])? as i32
        } else {
            0
        };

        if per < 1.0 || per > nper {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Calculate PMT
        let pmt = if rate.abs() < 1e-10 {
            -(pv + fv) / nper
        } else {
            let factor = (1.0 + rate).powf(nper);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -(rate * (pv * factor + fv)) / ((factor - 1.0) * type_adj)
        };

        // Calculate IPMT
        let fv_at_start = if rate.abs() < 1e-10 {
            -pv - pmt * (per - 1.0)
        } else {
            let factor = (1.0 + rate).powf(per - 1.0);
            let type_adj = if pmt_type != 0 { 1.0 + rate } else { 1.0 };
            -pv * factor - pmt * type_adj * (factor - 1.0) / rate
        };

        let ipmt = if pmt_type != 0 && per == 1.0 {
            0.0
        } else {
            fv_at_start * rate
        };

        // PPMT = PMT - IPMT
        let ppmt = pmt - ipmt;

        Ok(CalcValue::Scalar(LiteralValue::Number(ppmt)))
    }
}

/// Converts a nominal annual rate into an effective annual rate.
///
/// This is useful when nominal APR is quoted with periodic compounding and you need annualized
/// yield including compounding effects.
///
/// # Remarks
/// - `nominal_rate` is annual; `npery` is compounding periods per year.
/// - `npery` is truncated to an integer before computation.
/// - Sign convention is not cash-flow based; this function transforms rate conventions only.
/// - Returns `#NUM!` when `nominal_rate <= 0` or `npery < 1`.
/// - Result formula: `(1 + nominal_rate / npery)^npery - 1`.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =EFFECT(0.12, 12)
/// result: 0.12682503013196977
/// ```
/// ```yaml,sandbox
/// formula: =EFFECT(0.08, 4)
/// result: 0.08243215999999998
/// ```
#[derive(Debug)]
pub struct EffectFn;
/// [formualizer-docgen:schema:start]
/// Name: EFFECT
/// Type: EffectFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: EFFECT(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for EffectFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "EFFECT"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
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
        let nominal_rate = coerce_num(&args[0])?;
        let npery = coerce_num(&args[1])?.trunc() as i32;

        // Validation
        if nominal_rate <= 0.0 || npery < 1 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // EFFECT = (1 + nominal_rate/npery)^npery - 1
        let effect = (1.0 + nominal_rate / npery as f64).powi(npery) - 1.0;
        Ok(CalcValue::Scalar(LiteralValue::Number(effect)))
    }
}

/// Converts an effective annual rate into a nominal annual rate.
///
/// This is the inverse-style transformation of `EFFECT` for a chosen compounding frequency.
///
/// # Remarks
/// - `effect_rate` is annual effective yield; `npery` is periods per year.
/// - `npery` is truncated to an integer before computation.
/// - Sign convention is not cash-flow based; this function converts annual rate representation.
/// - Returns `#NUM!` when `effect_rate <= 0` or `npery < 1`.
/// - Result formula: `npery * ((1 + effect_rate)^(1/npery) - 1)`.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =NOMINAL(0.12682503013196977, 12)
/// result: 0.1200000000000001
/// ```
/// ```yaml,sandbox
/// formula: =NOMINAL(0.08243216, 4)
/// result: 0.08000000000000007
/// ```
#[derive(Debug)]
pub struct NominalFn;
/// [formualizer-docgen:schema:start]
/// Name: NOMINAL
/// Type: NominalFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: NOMINAL(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NominalFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NOMINAL"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
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
        let effect_rate = coerce_num(&args[0])?;
        let npery = coerce_num(&args[1])?.trunc() as i32;

        // Validation
        if effect_rate <= 0.0 || npery < 1 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // NOMINAL = npery * ((1 + effect_rate)^(1/npery) - 1)
        let nominal = npery as f64 * ((1.0 + effect_rate).powf(1.0 / npery as f64) - 1.0);
        Ok(CalcValue::Scalar(LiteralValue::Number(nominal)))
    }
}

/// Calculates periodic internal rate of return for regularly spaced cash flows.
///
/// The function iteratively finds the per-period rate where discounted cash flows sum to zero.
///
/// # Remarks
/// - Output is a rate per cash-flow period (not automatically annualized).
/// - Cash-flow sign convention: outflows are negative and inflows are positive.
/// - Non-numeric cells in arrays/ranges are ignored; direct scalar errors are propagated.
/// - A callable value input returns `#CALC!`.
/// - Returns `#NUM!` if fewer than two numeric cash flows are available, if derivative is near zero, or if iteration does not converge.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =IRR({-10000,3000,4200,6800})
/// result: 0.16340560068898924
/// ```
/// ```yaml,sandbox
/// formula: =IRR({-5000,1200,1410,1875,1050}, 0.1)
/// result: 0.041848876015677466
/// ```
#[derive(Debug)]
pub struct IrrFn;
/// [formualizer-docgen:schema:start]
/// Name: IRR
/// Type: IrrFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: IRR(arg1: any@scalar, arg2...: number@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for IrrFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "IRR"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::any(), ArgSchema::number_lenient_scalar()]);
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Collect cash flows
        let mut cashflows = Vec::new();
        let val = args[0].value()?;
        match val {
            CalcValue::Scalar(lit) => match lit {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Array(arr) => {
                    for row in arr {
                        for cell in row {
                            if let Ok(n) = coerce_literal_num(&cell) {
                                cashflows.push(n);
                            }
                        }
                    }
                }
                other => cashflows.push(coerce_literal_num(&other)?),
            },
            CalcValue::Range(range) => {
                let (rows, cols) = range.dims();
                for r in 0..rows {
                    for c in 0..cols {
                        let cell = range.get_cell(r, c);
                        if let Ok(n) = coerce_literal_num(&cell) {
                            cashflows.push(n);
                        }
                    }
                }
            }
            CalcValue::Callable(_) => {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Calc)
                        .with_message("LAMBDA value must be invoked"),
                )));
            }
        }

        if cashflows.len() < 2 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Initial guess
        let guess = if args.len() > 1 {
            coerce_num(&args[1])?
        } else {
            0.1
        };

        // Newton-Raphson iteration to find IRR
        let mut rate = guess;
        const MAX_ITER: i32 = 100;
        const EPSILON: f64 = 1e-10;

        for _ in 0..MAX_ITER {
            let mut npv = 0.0;
            let mut d_npv = 0.0;

            for (i, &cf) in cashflows.iter().enumerate() {
                let factor = (1.0 + rate).powi(i as i32);
                npv += cf / factor;
                if i > 0 {
                    d_npv -= (i as f64) * cf / (factor * (1.0 + rate));
                }
            }

            if d_npv.abs() < EPSILON {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }

            let new_rate = rate - npv / d_npv;
            if (new_rate - rate).abs() < EPSILON {
                return Ok(CalcValue::Scalar(LiteralValue::Number(new_rate)));
            }
            rate = new_rate;
        }

        Ok(CalcValue::Scalar(
            LiteralValue::Error(ExcelError::new_num()),
        ))
    }
}

/// Calculates modified internal rate of return with separate finance and reinvest rates.
///
/// Negative cash flows are discounted at `finance_rate` and positive cash flows are compounded at
/// `reinvest_rate`, then combined into a single periodic return.
///
/// # Remarks
/// - `finance_rate` and `reinvest_rate` are both rates per cash-flow period.
/// - Cash-flow sign convention: at least one negative and one positive cash flow are required.
/// - Non-numeric cells in arrays/ranges are ignored; direct scalar errors are propagated.
/// - A callable value input returns `#CALC!`.
/// - Returns `#NUM!` for insufficient cash flows, and `#DIV/0!` when computed positive/negative legs are invalid.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =MIRR({-10000,3000,4200,6800}, 0.1, 0.12)
/// result: 0.15147133664676304
/// ```
/// ```yaml,sandbox
/// formula: =MIRR({-120000,39000,30000,21000,37000,46000}, 0.1, 0.12)
/// result: 0.1260941303659051
/// ```
#[derive(Debug)]
pub struct MirrFn;
/// [formualizer-docgen:schema:start]
/// Name: MIRR
/// Type: MirrFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: MIRR(arg1: any@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for MirrFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "MIRR"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::any(),
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
        // Collect cash flows
        let mut cashflows = Vec::new();
        let val = args[0].value()?;
        match val {
            CalcValue::Scalar(lit) => match lit {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Array(arr) => {
                    for row in arr {
                        for cell in row {
                            if let Ok(n) = coerce_literal_num(&cell) {
                                cashflows.push(n);
                            }
                        }
                    }
                }
                other => cashflows.push(coerce_literal_num(&other)?),
            },
            CalcValue::Range(range) => {
                let (rows, cols) = range.dims();
                for r in 0..rows {
                    for c in 0..cols {
                        let cell = range.get_cell(r, c);
                        if let Ok(n) = coerce_literal_num(&cell) {
                            cashflows.push(n);
                        }
                    }
                }
            }
            CalcValue::Callable(_) => {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Calc)
                        .with_message("LAMBDA value must be invoked"),
                )));
            }
        }

        let finance_rate = coerce_num(&args[1])?;
        let reinvest_rate = coerce_num(&args[2])?;

        if cashflows.len() < 2 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let n = cashflows.len() as i32;

        // Present value of negative cash flows (discounted at finance_rate)
        let mut pv_neg = 0.0;
        // Future value of positive cash flows (compounded at reinvest_rate)
        let mut fv_pos = 0.0;

        for (i, &cf) in cashflows.iter().enumerate() {
            if cf < 0.0 {
                pv_neg += cf / (1.0 + finance_rate).powi(i as i32);
            } else {
                fv_pos += cf * (1.0 + reinvest_rate).powi(n - 1 - i as i32);
            }
        }

        if pv_neg >= 0.0 || fv_pos <= 0.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_div()),
            ));
        }

        // MIRR = (FV_pos / -PV_neg)^(1/(n-1)) - 1
        let mirr = (-fv_pos / pv_neg).powf(1.0 / (n - 1) as f64) - 1.0;
        Ok(CalcValue::Scalar(LiteralValue::Number(mirr)))
    }
}

/// Returns cumulative interest paid between two inclusive payment periods.
///
/// Use this to total the interest component over a slice of an amortization schedule.
///
/// # Remarks
/// - `rate` is the interest rate per payment period.
/// - `start_period` and `end_period` are 1-based, inclusive integer periods.
/// - `type` must be `0` (end-of-period) or `1` (beginning-of-period).
/// - Sign convention follows this implementation's balance model; with positive `pv`, cumulative interest is typically positive.
/// - Returns `#NUM!` for invalid domain values (non-positive rate, invalid ranges, invalid type, or non-positive `pv`).
///
/// # Examples
/// ```yaml,sandbox
/// formula: =CUMIPMT(0.06/12, 360, 300000, 1, 12, 0)
/// result: 16929.385083045923
/// ```
/// ```yaml,sandbox
/// formula: =CUMIPMT(0.06/12, 360, 300000, 13, 24, 0)
/// result: 14681.09233746059
/// ```
#[derive(Debug)]
pub struct CumipmtFn;
/// [formualizer-docgen:schema:start]
/// Name: CUMIPMT
/// Type: CumipmtFn
/// Min args: 6
/// Max args: 6
/// Variadic: false
/// Signature: CUMIPMT(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5: number@scalar, arg6: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg6{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for CumipmtFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CUMIPMT"
    }
    fn min_args(&self) -> usize {
        6
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
        let rate = coerce_num(&args[0])?;
        let nper = coerce_num(&args[1])?.trunc() as i32;
        let pv = coerce_num(&args[2])?;
        let start = coerce_num(&args[3])?.trunc() as i32;
        let end = coerce_num(&args[4])?.trunc() as i32;
        let pay_type = coerce_num(&args[5])?.trunc() as i32;

        // Validation
        if rate <= 0.0
            || nper <= 0
            || pv <= 0.0
            || start < 1
            || end < start
            || end > nper
            || (pay_type != 0 && pay_type != 1)
        {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Calculate PMT
        let pmt = if rate == 0.0 {
            -pv / nper as f64
        } else {
            -pv * rate * (1.0 + rate).powi(nper) / ((1.0 + rate).powi(nper) - 1.0)
        };

        // Sum interest payments from start to end
        let mut cum_int = 0.0;
        let mut balance = pv;

        for period in 1..=end {
            let interest = if pay_type == 1 && period == 1 {
                0.0
            } else {
                balance * rate
            };

            if period >= start {
                cum_int += interest;
            }

            let principal = pmt - interest;
            balance += principal;
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(cum_int)))
    }
}

/// Returns cumulative principal paid between two inclusive payment periods.
///
/// Use this to measure principal reduction over a selected amortization window.
///
/// # Remarks
/// - `rate` is the interest rate per payment period.
/// - `start_period` and `end_period` are 1-based, inclusive integer periods.
/// - `type` must be `0` (end-of-period) or `1` (beginning-of-period).
/// - Sign convention follows payment direction; with positive `pv`, cumulative principal is typically negative.
/// - Returns `#NUM!` for invalid domain values (non-positive rate, invalid ranges, invalid type, or non-positive `pv`).
///
/// # Examples
/// ```yaml,sandbox
/// formula: =CUMPRINC(0.06/12, 360, 300000, 1, 12, 0)
/// result: -38513.20398854517
/// ```
/// ```yaml,sandbox
/// formula: =CUMPRINC(0.06/12, 360, 300000, 13, 24, 0)
/// result: -36264.91124295984
/// ```
#[derive(Debug)]
pub struct CumprincFn;
/// [formualizer-docgen:schema:start]
/// Name: CUMPRINC
/// Type: CumprincFn
/// Min args: 6
/// Max args: 6
/// Variadic: false
/// Signature: CUMPRINC(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5: number@scalar, arg6: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg6{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for CumprincFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CUMPRINC"
    }
    fn min_args(&self) -> usize {
        6
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
        let rate = coerce_num(&args[0])?;
        let nper = coerce_num(&args[1])?.trunc() as i32;
        let pv = coerce_num(&args[2])?;
        let start = coerce_num(&args[3])?.trunc() as i32;
        let end = coerce_num(&args[4])?.trunc() as i32;
        let pay_type = coerce_num(&args[5])?.trunc() as i32;

        // Validation
        if rate <= 0.0
            || nper <= 0
            || pv <= 0.0
            || start < 1
            || end < start
            || end > nper
            || (pay_type != 0 && pay_type != 1)
        {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Calculate PMT
        let pmt = if rate == 0.0 {
            -pv / nper as f64
        } else {
            -pv * rate * (1.0 + rate).powi(nper) / ((1.0 + rate).powi(nper) - 1.0)
        };

        // Sum principal payments from start to end
        let mut cum_princ = 0.0;
        let mut balance = pv;

        for period in 1..=end {
            let interest = if pay_type == 1 && period == 1 {
                0.0
            } else {
                balance * rate
            };

            let principal = pmt - interest;

            if period >= start {
                cum_princ += principal;
            }

            balance += principal;
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(cum_princ)))
    }
}

/// Calculates annualized net present value for irregularly dated cash flows.
///
/// Discounting uses an actual-day offset divided by 365 from the first provided date.
///
/// # Remarks
/// - `rate` is an annual discount rate.
/// - Cash-flow sign convention: outflows are negative and inflows are positive.
/// - `values` and `dates` are flattened to numeric entries; non-numeric entries are ignored.
/// - Scalar error inputs are propagated; callable inputs return `#CALC!`.
/// - Returns `#NUM!` when `values` and `dates` lengths differ or no numeric pair exists.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =XNPV(0.10, {-10000,2750,4250,3250,2750}, {0,365,730,1095,1460})
/// result: 332.4567993989465
/// ```
/// ```yaml,sandbox
/// formula: =XNPV(0.08, {-5000,1200,1800,2400}, {0,180,365,730})
/// result: -120.41078799700836
/// ```
#[derive(Debug)]
pub struct XnpvFn;
/// [formualizer-docgen:schema:start]
/// Name: XNPV
/// Type: XnpvFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: XNPV(arg1: number@scalar, arg2: any@scalar, arg3: any@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg3{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for XnpvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "XNPV"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(), // rate
                ArgSchema::any(),                   // values
                ArgSchema::any(),                   // dates
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let rate = coerce_num(&args[0])?;

        // Collect values
        let mut values = Vec::new();
        let val = args[1].value()?;
        match val {
            CalcValue::Scalar(lit) => match lit {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Array(arr) => {
                    for row in arr {
                        for cell in row {
                            if let Ok(n) = coerce_literal_num(&cell) {
                                values.push(n);
                            }
                        }
                    }
                }
                other => values.push(coerce_literal_num(&other)?),
            },
            CalcValue::Range(range) => {
                let (rows, cols) = range.dims();
                for r in 0..rows {
                    for c in 0..cols {
                        let cell = range.get_cell(r, c);
                        if let Ok(n) = coerce_literal_num(&cell) {
                            values.push(n);
                        }
                    }
                }
            }
            CalcValue::Callable(_) => {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Calc)
                        .with_message("LAMBDA value must be invoked"),
                )));
            }
        }

        // Collect dates
        let mut dates = Vec::new();
        let date_val = args[2].value()?;
        match date_val {
            CalcValue::Scalar(lit) => match lit {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Array(arr) => {
                    for row in arr {
                        for cell in row {
                            if let Ok(n) = coerce_literal_num(&cell) {
                                dates.push(n);
                            }
                        }
                    }
                }
                other => dates.push(coerce_literal_num(&other)?),
            },
            CalcValue::Range(range) => {
                let (rows, cols) = range.dims();
                for r in 0..rows {
                    for c in 0..cols {
                        let cell = range.get_cell(r, c);
                        if let Ok(n) = coerce_literal_num(&cell) {
                            dates.push(n);
                        }
                    }
                }
            }
            CalcValue::Callable(_) => {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Calc)
                        .with_message("LAMBDA value must be invoked"),
                )));
            }
        }

        // Validate that values and dates have the same length
        if values.len() != dates.len() || values.is_empty() {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Calculate XNPV: Sum of values[i] / (1 + rate)^((dates[i] - dates[0]) / 365)
        let first_date = dates[0];
        let mut xnpv = 0.0;

        for (i, &value) in values.iter().enumerate() {
            let days_from_start = dates[i] - first_date;
            let years = days_from_start / 365.0;
            xnpv += value / (1.0 + rate).powf(years);
        }

        Ok(CalcValue::Scalar(LiteralValue::Number(xnpv)))
    }
}

/// Helper function to calculate XNPV given rate, values, and dates
fn calculate_xnpv(rate: f64, values: &[f64], dates: &[f64]) -> f64 {
    if values.is_empty() || dates.is_empty() {
        return 0.0;
    }
    let first_date = dates[0];
    let mut xnpv = 0.0;
    for (i, &value) in values.iter().enumerate() {
        let days_from_start = dates[i] - first_date;
        let years = days_from_start / 365.0;
        xnpv += value / (1.0 + rate).powf(years);
    }
    xnpv
}

/// Helper function to calculate the derivative of XNPV with respect to rate
fn calculate_xnpv_derivative(rate: f64, values: &[f64], dates: &[f64]) -> f64 {
    if values.is_empty() || dates.is_empty() {
        return 0.0;
    }
    let first_date = dates[0];
    let mut d_xnpv = 0.0;
    for (i, &value) in values.iter().enumerate() {
        let days_from_start = dates[i] - first_date;
        let years = days_from_start / 365.0;
        // d/dr [value / (1+r)^years] = -years * value / (1+r)^(years+1)
        d_xnpv -= years * value / (1.0 + rate).powf(years + 1.0);
    }
    d_xnpv
}

/// Calculates annualized internal rate of return for irregularly dated cash flows.
///
/// The solver uses Newton-Raphson on `XNPV(rate, values, dates) = 0` with day-count basis 365.
///
/// # Remarks
/// - Output is an annualized rate.
/// - Cash-flow sign convention requires at least one negative and one positive value.
/// - `guess` defaults to `0.1` and can materially affect convergence.
/// - Non-numeric entries in value/date arrays are ignored; callable inputs return `#CALC!`.
/// - Returns `#NUM!` for mismatched lengths, insufficient valid points, missing sign change, derivative failure, or non-convergence.
///
/// # Examples
/// ```yaml,sandbox
/// formula: =XIRR({-10000,2750,4250,3250,2750}, {0,365,730,1095,1460})
/// result: 0.11541278310055854
/// ```
/// ```yaml,sandbox
/// formula: =XIRR({-5000,1200,1800,2400}, {0,180,365,730}, 0.1)
/// result: 0.06001829492127762
/// ```
#[derive(Debug)]
pub struct XirrFn;
/// [formualizer-docgen:schema:start]
/// Name: XIRR
/// Type: XirrFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: XIRR(arg1: any@scalar, arg2: any@scalar, arg3...: number@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for XirrFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "XIRR"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::any(),                   // values
                ArgSchema::any(),                   // dates
                ArgSchema::number_lenient_scalar(), // guess (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Collect values
        let mut values = Vec::new();
        let val = args[0].value()?;
        match val {
            CalcValue::Scalar(lit) => match lit {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Array(arr) => {
                    for row in arr {
                        for cell in row {
                            if let Ok(n) = coerce_literal_num(&cell) {
                                values.push(n);
                            }
                        }
                    }
                }
                other => values.push(coerce_literal_num(&other)?),
            },
            CalcValue::Range(range) => {
                let (rows, cols) = range.dims();
                for r in 0..rows {
                    for c in 0..cols {
                        let cell = range.get_cell(r, c);
                        if let Ok(n) = coerce_literal_num(&cell) {
                            values.push(n);
                        }
                    }
                }
            }
            CalcValue::Callable(_) => {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Calc)
                        .with_message("LAMBDA value must be invoked"),
                )));
            }
        }

        // Collect dates
        let mut dates = Vec::new();
        let date_val = args[1].value()?;
        match date_val {
            CalcValue::Scalar(lit) => match lit {
                LiteralValue::Error(e) => return Ok(CalcValue::Scalar(LiteralValue::Error(e))),
                LiteralValue::Array(arr) => {
                    for row in arr {
                        for cell in row {
                            if let Ok(n) = coerce_literal_num(&cell) {
                                dates.push(n);
                            }
                        }
                    }
                }
                other => dates.push(coerce_literal_num(&other)?),
            },
            CalcValue::Range(range) => {
                let (rows, cols) = range.dims();
                for r in 0..rows {
                    for c in 0..cols {
                        let cell = range.get_cell(r, c);
                        if let Ok(n) = coerce_literal_num(&cell) {
                            dates.push(n);
                        }
                    }
                }
            }
            CalcValue::Callable(_) => {
                return Ok(CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Calc)
                        .with_message("LAMBDA value must be invoked"),
                )));
            }
        }

        // Validate
        if values.len() != dates.len() || values.len() < 2 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Check that we have at least one positive and one negative cash flow
        let has_positive = values.iter().any(|&v| v > 0.0);
        let has_negative = values.iter().any(|&v| v < 0.0);
        if !has_positive || !has_negative {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Initial guess
        let guess = if args.len() > 2 {
            coerce_num(&args[2])?
        } else {
            0.1
        };

        // Newton-Raphson iteration to find XIRR
        let mut rate = guess;
        const MAX_ITER: i32 = 100;
        const EPSILON: f64 = 1e-10;

        for _ in 0..MAX_ITER {
            let xnpv = calculate_xnpv(rate, &values, &dates);
            let d_xnpv = calculate_xnpv_derivative(rate, &values, &dates);

            if d_xnpv.abs() < EPSILON {
                return Ok(CalcValue::Scalar(
                    LiteralValue::Error(ExcelError::new_num()),
                ));
            }

            let new_rate = rate - xnpv / d_xnpv;

            if (new_rate - rate).abs() < EPSILON {
                return Ok(CalcValue::Scalar(LiteralValue::Number(new_rate)));
            }

            rate = new_rate;

            // Prevent rate from going too negative (would make (1+rate) negative)
            if rate <= -1.0 {
                rate = -0.99;
            }
        }

        Ok(CalcValue::Scalar(
            LiteralValue::Error(ExcelError::new_num()),
        ))
    }
}

/// Converts fractional-dollar notation into a decimal dollar value.
///
/// This is commonly used for security price formats such as thirty-seconds (`fraction = 32`).
///
/// # Remarks
/// - `fraction` is truncated to an integer denominator and must be `>= 1`.
/// - Sign convention: sign is preserved (`-x` maps to `-result`).
/// - No periodic rate is involved in this conversion.
/// - Returns `#NUM!` when `fraction < 1` after truncation.
/// - Fractional parsing uses denominator digit width (`ceil(log10(fraction))`).
///
/// # Examples
/// ```yaml,sandbox
/// formula: =DOLLARDE(1.02, 16)
/// result: 1.125
/// ```
/// ```yaml,sandbox
/// formula: =DOLLARDE(-3.15, 32)
/// result: -3.46875
/// ```
#[derive(Debug)]
pub struct DollardeFn;
/// [formualizer-docgen:schema:start]
/// Name: DOLLARDE
/// Type: DollardeFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: DOLLARDE(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for DollardeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DOLLARDE"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(), // fractional_dollar
                ArgSchema::number_lenient_scalar(), // fraction
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let fractional_dollar = coerce_num(&args[0])?;
        let fraction = coerce_num(&args[1])?.trunc() as i32;

        // Validate fraction
        if fraction < 1 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Determine how many decimal places are in the fractional part
        // The fractional part represents numerator / fraction
        let sign = if fractional_dollar < 0.0 { -1.0 } else { 1.0 };
        let abs_value = fractional_dollar.abs();
        let integer_part = abs_value.trunc();
        let fractional_part = abs_value - integer_part;

        // Calculate the number of digits needed to represent the fraction denominator
        let digits = (fraction as f64).log10().ceil() as i32;
        let multiplier = 10_f64.powi(digits);

        // The fractional part is scaled by the multiplier, then divided by the fraction
        let numerator = (fractional_part * multiplier).round();
        let decimal_fraction = numerator / fraction as f64;

        let result = sign * (integer_part + decimal_fraction);
        Ok(CalcValue::Scalar(LiteralValue::Number(result)))
    }
}

/// Converts a decimal dollar value into fractional-dollar notation.
///
/// This is the inverse-style formatting helper used for quoted fractional price conventions.
///
/// # Remarks
/// - `fraction` is truncated to an integer denominator and must be `>= 1`.
/// - Sign convention: sign is preserved (`-x` maps to `-result`).
/// - No periodic rate is involved in this conversion.
/// - Returns `#NUM!` when `fraction < 1` after truncation.
/// - Fraction output is encoded by denominator digit width (`ceil(log10(fraction))`).
///
/// # Examples
/// ```yaml,sandbox
/// formula: =DOLLARFR(1.125, 16)
/// result: 1.02
/// ```
/// ```yaml,sandbox
/// formula: =DOLLARFR(-3.46875, 32)
/// result: -3.15
/// ```
#[derive(Debug)]
pub struct DollarfrFn;
/// [formualizer-docgen:schema:start]
/// Name: DOLLARFR
/// Type: DollarfrFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: DOLLARFR(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for DollarfrFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "DOLLARFR"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(), // decimal_dollar
                ArgSchema::number_lenient_scalar(), // fraction
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        let decimal_dollar = coerce_num(&args[0])?;
        let fraction = coerce_num(&args[1])?.trunc() as i32;

        // Validate fraction
        if fraction < 1 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let sign = if decimal_dollar < 0.0 { -1.0 } else { 1.0 };
        let abs_value = decimal_dollar.abs();
        let integer_part = abs_value.trunc();
        let decimal_part = abs_value - integer_part;

        // Convert decimal fraction to fractional representation
        // numerator = decimal_part * fraction
        let numerator = decimal_part * fraction as f64;

        // Calculate the number of digits needed to represent the fraction denominator
        let digits = (fraction as f64).log10().ceil() as i32;
        let divisor = 10_f64.powi(digits);

        // The fractional dollar format puts the numerator after the decimal point
        let result = sign * (integer_part + numerator / divisor);
        Ok(CalcValue::Scalar(LiteralValue::Number(result)))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(PmtFn));
    crate::function_registry::register_function(Arc::new(PvFn));
    crate::function_registry::register_function(Arc::new(FvFn));
    crate::function_registry::register_function(Arc::new(NpvFn));
    crate::function_registry::register_function(Arc::new(NperFn));
    crate::function_registry::register_function(Arc::new(RateFn));
    crate::function_registry::register_function(Arc::new(IpmtFn));
    crate::function_registry::register_function(Arc::new(PpmtFn));
    crate::function_registry::register_function(Arc::new(EffectFn));
    crate::function_registry::register_function(Arc::new(NominalFn));
    crate::function_registry::register_function(Arc::new(IrrFn));
    crate::function_registry::register_function(Arc::new(MirrFn));
    crate::function_registry::register_function(Arc::new(CumipmtFn));
    crate::function_registry::register_function(Arc::new(CumprincFn));
    crate::function_registry::register_function(Arc::new(XnpvFn));
    crate::function_registry::register_function(Arc::new(XirrFn));
    crate::function_registry::register_function(Arc::new(DollardeFn));
    crate::function_registry::register_function(Arc::new(DollarfrFn));
}
