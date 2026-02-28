//! Time Value of Money functions: PMT, PV, FV, NPV, NPER, RATE, IPMT, PPMT, XNPV, XIRR, DOLLARDE, DOLLARFR

use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
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

/// PMT(rate, nper, pv, [fv], [type])
/// Calculates the payment for a loan based on constant payments and a constant interest rate
#[derive(Debug)]
pub struct PmtFn;
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

/// PV(rate, nper, pmt, [fv], [type])
/// Calculates the present value of an investment
#[derive(Debug)]
pub struct PvFn;
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

/// FV(rate, nper, pmt, [pv], [type])
/// Calculates the future value of an investment
#[derive(Debug)]
pub struct FvFn;
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

/// NPV(rate, value1, [value2], ...)
/// Calculates the net present value of an investment
#[derive(Debug)]
pub struct NpvFn;
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

/// NPER(rate, pmt, pv, [fv], [type])
/// Calculates the number of periods for an investment
#[derive(Debug)]
pub struct NperFn;
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

/// RATE(nper, pmt, pv, [fv], [type], [guess])
/// Calculates the interest rate per period
#[derive(Debug)]
pub struct RateFn;
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

/// IPMT(rate, per, nper, pv, [fv], [type])
/// Calculates the interest payment for a given period
#[derive(Debug)]
pub struct IpmtFn;
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

/// PPMT(rate, per, nper, pv, [fv], [type])
/// Calculates the principal payment for a given period
#[derive(Debug)]
pub struct PpmtFn;
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

/// EFFECT(nominal_rate, npery) - Returns the effective annual interest rate
#[derive(Debug)]
pub struct EffectFn;
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

/// NOMINAL(effect_rate, npery) - Returns the nominal annual interest rate
#[derive(Debug)]
pub struct NominalFn;
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

/// Compute NPV at a given rate.
fn irr_npv(cashflows: &[f64], rate: f64) -> f64 {
    let mut npv = 0.0;
    for (i, &cf) in cashflows.iter().enumerate() {
        npv += cf / (1.0 + rate).powi(i as i32);
    }
    npv
}

/// Compute NPV and its derivative w.r.t. rate.
fn irr_npv_deriv(cashflows: &[f64], rate: f64) -> (f64, f64) {
    let mut npv = 0.0;
    let mut d_npv = 0.0;
    for (i, &cf) in cashflows.iter().enumerate() {
        let factor = (1.0 + rate).powi(i as i32);
        npv += cf / factor;
        if i > 0 {
            d_npv -= (i as f64) * cf / (factor * (1.0 + rate));
        }
    }
    (npv, d_npv)
}

/// Solve for IRR using Newton-Raphson with Brent's method fallback.
///
/// Strategy:
/// 1. Try Newton-Raphson from the user's guess (fast when it works).
/// 2. If Newton diverges, bracket the root by scanning probe points,
///    then use Brent's method (superlinear convergence with guaranteed
///    bracketing) to find the root precisely.
fn irr_solve(cashflows: &[f64], guess: f64) -> Option<f64> {
    const MAX_NR: usize = 100;
    const MAX_BRENT: usize = 200;
    const TOL: f64 = 1e-12;
    const MACH_EPS: f64 = f64::EPSILON;

    // --- Phase 1: Newton-Raphson from the given guess ---
    let mut rate = guess;
    for _ in 0..MAX_NR {
        let (npv, d_npv) = irr_npv_deriv(cashflows, rate);
        if d_npv.abs() < TOL {
            break; // flat derivative, fall through to Brent
        }
        let new_rate = rate - npv / d_npv;
        // Accept if converged and rate > -1 (pole at -1)
        if (new_rate - rate).abs() < TOL && new_rate > -1.0 {
            return Some(new_rate);
        }
        // If Newton shoots below -1 or to NaN/Inf, bail out
        if new_rate <= -1.0 || !new_rate.is_finite() {
            break;
        }
        rate = new_rate;
    }

    // --- Phase 2: Bracket the root, then apply Brent's method ---
    // Search for a sign change in NPV across a wide range of rates.
    let probes: &[f64] = &[
        -0.99, -0.9, -0.5, -0.1, -0.01, 0.0, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.3,
        0.5, 1.0, 2.0, 5.0, 10.0,
    ];
    let mut lo = f64::NAN;
    let mut hi = f64::NAN;
    let mut npv_lo = f64::NAN;

    for &r in probes {
        let npv = irr_npv(cashflows, r);
        if !npv.is_finite() {
            continue;
        }
        if lo.is_nan() {
            lo = r;
            npv_lo = npv;
            continue;
        }
        if npv_lo * npv < 0.0 {
            hi = r;
            break;
        }
        lo = r;
        npv_lo = npv;
    }

    if hi.is_nan() {
        return None; // no sign change found — no real IRR
    }

    // Brent's method (following scipy's brentq / Brent's zeroin algorithm).
    // Combines inverse quadratic interpolation, secant, and bisection.
    // xpre/xcur maintain the bracket; xblk is the contrapoint.
    let mut xpre = lo;
    let mut xcur = hi;
    let mut fpre = irr_npv(cashflows, xpre);
    let mut fcur = irr_npv(cashflows, xcur);

    if fpre == 0.0 {
        return Some(xpre);
    }
    if fcur == 0.0 {
        return Some(xcur);
    }

    let mut xblk = 0.0;
    let mut fblk = 0.0;
    let mut spre = 0.0;
    let mut scur = 0.0;

    for _ in 0..MAX_BRENT {
        // If xpre and xcur bracket the root, reset the contrapoint
        if fpre * fcur < 0.0 {
            xblk = xpre;
            fblk = fpre;
            spre = xcur - xpre;
            scur = spre;
        }

        // Ensure xcur is the best approximation (|fcur| <= |fblk|)
        if fblk.abs() < fcur.abs() {
            xpre = xcur;
            xcur = xblk;
            xblk = xpre;
            fpre = fcur;
            fcur = fblk;
            fblk = fpre;
        }

        let delta = (MACH_EPS * xcur.abs() + 0.5 * TOL).max(MACH_EPS);
        let sbis = (xblk - xcur) * 0.5;

        if fcur == 0.0 || sbis.abs() < delta {
            return Some(xcur);
        }

        if spre.abs() >= delta && fcur.abs() < fpre.abs() {
            // Try interpolation
            let stry = if (xpre - xblk).abs() < MACH_EPS {
                // Secant step
                -fcur * (xcur - xpre) / (fcur - fpre)
            } else {
                // Inverse quadratic interpolation
                let dpre = (fpre - fcur) / (xpre - xcur);
                let dblk = (fblk - fcur) / (xblk - xcur);
                -fcur * (fblk * dblk - fpre * dpre) / (dblk * dpre * (fblk - fpre))
            };

            // Accept if step is small enough
            if 2.0 * stry.abs() < spre.abs().min(3.0 * sbis.abs() - delta) {
                spre = scur;
                scur = stry;
            } else {
                spre = sbis;
                scur = sbis;
            }
        } else {
            // Bisection
            spre = sbis;
            scur = sbis;
        }

        xpre = xcur;
        fpre = fcur;

        if scur.abs() > delta {
            xcur += scur;
        } else {
            xcur += if sbis > 0.0 { delta } else { -delta };
        }
        fcur = irr_npv(cashflows, xcur);
    }
    Some(xcur)
}

/// IRR(values, [guess]) - Internal rate of return
#[derive(Debug)]
pub struct IrrFn;
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

        match irr_solve(&cashflows, guess) {
            Some(rate) => Ok(CalcValue::Scalar(LiteralValue::Number(rate))),
            None => Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            )),
        }
    }
}

/// MIRR(values, finance_rate, reinvest_rate) - Modified IRR
#[derive(Debug)]
pub struct MirrFn;
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
                fv_pos += cf * (1.0 + reinvest_rate).powi((n - 1 - i as i32) as i32);
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

/// CUMIPMT(rate, nper, pv, start_period, end_period, type) - Cumulative interest payment
#[derive(Debug)]
pub struct CumipmtFn;
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

/// CUMPRINC(rate, nper, pv, start_period, end_period, type) - Cumulative principal payment
#[derive(Debug)]
pub struct CumprincFn;
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

/// XNPV(rate, values, dates) - Net present value for irregular cash flows
/// Formula: Sum of values[i] / (1 + rate)^((dates[i] - dates[0]) / 365)
#[derive(Debug)]
pub struct XnpvFn;
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

/// XIRR(values, dates, [guess]) - Internal rate of return for irregular cash flows
/// Uses Newton-Raphson iteration to find rate where XNPV = 0
#[derive(Debug)]
pub struct XirrFn;
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

/// DOLLARDE(fractional_dollar, fraction) - Convert fractional dollar to decimal
/// Example: DOLLARDE(1.02, 16) = 1.125 (1 and 2/16)
#[derive(Debug)]
pub struct DollardeFn;
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

/// DOLLARFR(decimal_dollar, fraction) - Convert decimal dollar to fractional
/// Example: DOLLARFR(1.125, 16) = 1.02
#[derive(Debug)]
pub struct DollarfrFn;
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

/// RRI(nper, pv, fv) — equivalent interest rate for growth of an investment.
/// Returns (fv/pv)^(1/nper) - 1  (i.e. CAGR).
#[derive(Debug)]
pub struct RriFn;
impl Function for RriFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "RRI"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(), // nper
                ArgSchema::number_lenient_scalar(), // pv
                ArgSchema::number_lenient_scalar(), // fv
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
        let pv = coerce_num(&args[1])?;
        let fv = coerce_num(&args[2])?;

        // nper must be > 0, pv must be non-zero
        if nper <= 0.0 || pv == 0.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // If pv and fv have different signs, the ratio is negative and
        // fractional exponent would produce NaN → Excel returns #NUM!
        let ratio = fv / pv;
        if ratio < 0.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let result = ratio.powf(1.0 / nper) - 1.0;
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
    crate::function_registry::register_function(Arc::new(RriFn));
}
