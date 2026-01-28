//! Bond pricing functions: ACCRINT, ACCRINTM, PRICE, YIELD

use crate::args::ArgSchema;
use crate::builtins::datetime::serial_to_date;
use crate::function::Function;
use crate::traits::{ArgumentHandle, CalcValue, FunctionContext};
use chrono::{Datelike, NaiveDate};
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

/// Day count basis calculation
/// Returns (num_days, year_basis) for the given basis type
#[derive(Debug, Clone, Copy, PartialEq)]
enum DayCountBasis {
    UsNasd30360 = 0,   // US (NASD) 30/360
    ActualActual = 1,  // Actual/actual
    Actual360 = 2,     // Actual/360
    Actual365 = 3,     // Actual/365
    European30360 = 4, // European 30/360
}

impl DayCountBasis {
    fn from_int(basis: i32) -> Result<Self, ExcelError> {
        match basis {
            0 => Ok(DayCountBasis::UsNasd30360),
            1 => Ok(DayCountBasis::ActualActual),
            2 => Ok(DayCountBasis::Actual360),
            3 => Ok(DayCountBasis::Actual365),
            4 => Ok(DayCountBasis::European30360),
            _ => Err(ExcelError::new_num()),
        }
    }
}

/// Check if a year is a leap year
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Check if a date is the last day of the month
fn is_last_day_of_month(date: &NaiveDate) -> bool {
    let next_day = *date + chrono::Duration::days(1);
    next_day.month() != date.month()
}

/// Calculate days between two dates using the specified basis
fn days_between(start: &NaiveDate, end: &NaiveDate, basis: DayCountBasis) -> i32 {
    match basis {
        DayCountBasis::UsNasd30360 => days_30_360_us(start, end),
        DayCountBasis::ActualActual | DayCountBasis::Actual360 | DayCountBasis::Actual365 => {
            (*end - *start).num_days() as i32
        }
        DayCountBasis::European30360 => days_30_360_eu(start, end),
    }
}

/// Calculate days using US (NASD) 30/360 method
fn days_30_360_us(start: &NaiveDate, end: &NaiveDate) -> i32 {
    let mut sd = start.day() as i32;
    let sm = start.month() as i32;
    let sy = start.year();

    let mut ed = end.day() as i32;
    let em = end.month() as i32;
    let ey = end.year();

    // Adjust for last day of February
    let start_is_last_feb = sm == 2 && is_last_day_of_month(start);
    let end_is_last_feb = em == 2 && is_last_day_of_month(end);

    if start_is_last_feb && end_is_last_feb {
        ed = 30;
    }
    if start_is_last_feb {
        sd = 30;
    }
    if ed == 31 && sd >= 30 {
        ed = 30;
    }
    if sd == 31 {
        sd = 30;
    }

    (ey - sy) * 360 + (em - sm) * 30 + (ed - sd)
}

/// Calculate days using European 30/360 method
fn days_30_360_eu(start: &NaiveDate, end: &NaiveDate) -> i32 {
    let mut sd = start.day() as i32;
    let sm = start.month() as i32;
    let sy = start.year();

    let mut ed = end.day() as i32;
    let em = end.month() as i32;
    let ey = end.year();

    if sd == 31 {
        sd = 30;
    }
    if ed == 31 {
        ed = 30;
    }

    (ey - sy) * 360 + (em - sm) * 30 + (ed - sd)
}

/// Get the annual basis (denominator for year fraction)
fn annual_basis(basis: DayCountBasis, start: &NaiveDate, end: &NaiveDate) -> f64 {
    match basis {
        DayCountBasis::UsNasd30360 | DayCountBasis::European30360 => 360.0,
        DayCountBasis::Actual360 => 360.0,
        DayCountBasis::Actual365 => 365.0,
        DayCountBasis::ActualActual => {
            // Determine the average year length based on leap years in the period
            let sy = start.year();
            let ey = end.year();
            if sy == ey {
                if is_leap_year(sy) { 366.0 } else { 365.0 }
            } else {
                // Average across years
                let mut total_days = 0.0;
                let mut years = 0;
                for y in sy..=ey {
                    total_days += if is_leap_year(y) { 366.0 } else { 365.0 };
                    years += 1;
                }
                total_days / years as f64
            }
        }
    }
}

/// Calculate the year fraction between two dates
fn year_fraction(start: &NaiveDate, end: &NaiveDate, basis: DayCountBasis) -> f64 {
    let days = days_between(start, end, basis) as f64;
    let annual = annual_basis(basis, start, end);
    days / annual
}

/// Find the coupon date before settlement date
fn coupon_date_before(settlement: &NaiveDate, maturity: &NaiveDate, frequency: i32) -> NaiveDate {
    let months_between_coupons = 12 / frequency;
    let mut coupon_date = *maturity;

    // Work backwards from maturity to find the coupon date just before settlement
    while coupon_date >= *settlement {
        coupon_date = add_months(&coupon_date, -months_between_coupons);
    }
    coupon_date
}

/// Find the coupon date after settlement date
fn coupon_date_after(settlement: &NaiveDate, maturity: &NaiveDate, frequency: i32) -> NaiveDate {
    let months_between_coupons = 12 / frequency;
    let prev_coupon = coupon_date_before(settlement, maturity, frequency);
    add_months(&prev_coupon, months_between_coupons)
}

/// Add months to a date, handling end-of-month adjustments
fn add_months(date: &NaiveDate, months: i32) -> NaiveDate {
    let total_months = date.year() * 12 + date.month() as i32 - 1 + months;
    let new_year = total_months / 12;
    let new_month = (total_months % 12 + 1) as u32;

    // Try to keep the same day, but cap at month's end
    let mut new_day = date.day();
    loop {
        if let Some(d) = NaiveDate::from_ymd_opt(new_year, new_month, new_day) {
            return d;
        }
        new_day -= 1;
        if new_day == 0 {
            // Fallback - should never reach here
            return NaiveDate::from_ymd_opt(new_year, new_month, 1).unwrap();
        }
    }
}

/// Count the number of coupons remaining
fn coupons_remaining(settlement: &NaiveDate, maturity: &NaiveDate, frequency: i32) -> i32 {
    let months_between_coupons = 12 / frequency;
    let mut count = 0;
    let mut coupon_date = coupon_date_after(settlement, maturity, frequency);

    while coupon_date <= *maturity {
        count += 1;
        coupon_date = add_months(&coupon_date, months_between_coupons);
    }
    count
}

/// ACCRINT(issue, first_interest, settlement, rate, par, frequency, [basis], [calc_method])
/// Returns accrued interest for a security that pays periodic interest
#[derive(Debug)]
pub struct AccrintFn;

impl Function for AccrintFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ACCRINT"
    }
    fn min_args(&self) -> usize {
        6
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(), // issue
                ArgSchema::number_lenient_scalar(), // first_interest
                ArgSchema::number_lenient_scalar(), // settlement
                ArgSchema::number_lenient_scalar(), // rate
                ArgSchema::number_lenient_scalar(), // par
                ArgSchema::number_lenient_scalar(), // frequency
                ArgSchema::number_lenient_scalar(), // basis (optional)
                ArgSchema::number_lenient_scalar(), // calc_method (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Check minimum required arguments
        if args.len() < 6 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let issue_serial = coerce_num(&args[0])?;
        let first_interest_serial = coerce_num(&args[1])?;
        let settlement_serial = coerce_num(&args[2])?;
        let rate = coerce_num(&args[3])?;
        let par = coerce_num(&args[4])?;
        let frequency = coerce_num(&args[5])?.trunc() as i32;
        let basis_int = if args.len() > 6 {
            coerce_num(&args[6])?.trunc() as i32
        } else {
            0
        };
        let calc_method = if args.len() > 7 {
            coerce_num(&args[7])?.trunc() as i32
        } else {
            1
        };

        // Validate inputs
        if rate <= 0.0 || par <= 0.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }
        if frequency != 1 && frequency != 2 && frequency != 4 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let basis = DayCountBasis::from_int(basis_int)?;

        let issue = serial_to_date(issue_serial)?;
        let first_interest = serial_to_date(first_interest_serial)?;
        let settlement = serial_to_date(settlement_serial)?;

        // settlement must be after issue
        if settlement <= issue {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Calculate accrued interest
        // If calc_method is TRUE (or 1), calculate from issue to settlement
        // If calc_method is FALSE (or 0), calculate from last coupon to settlement
        let accrued_interest = if calc_method != 0 {
            // Calculate from issue date to settlement date
            // ACCRINT = par * rate * year_fraction(issue, settlement)
            let yf = year_fraction(&issue, &settlement, basis);
            par * rate * yf
        } else {
            // Calculate from last coupon date to settlement
            let prev_coupon = coupon_date_before(&settlement, &first_interest, frequency);
            let start_date = if prev_coupon < issue {
                issue
            } else {
                prev_coupon
            };
            let yf = year_fraction(&start_date, &settlement, basis);
            par * rate * yf
        };

        Ok(CalcValue::Scalar(LiteralValue::Number(accrued_interest)))
    }
}

/// ACCRINTM(issue, settlement, rate, par, [basis])
/// Returns accrued interest for a security that pays interest at maturity
#[derive(Debug)]
pub struct AccrintmFn;

impl Function for AccrintmFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "ACCRINTM"
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
                ArgSchema::number_lenient_scalar(), // issue
                ArgSchema::number_lenient_scalar(), // settlement
                ArgSchema::number_lenient_scalar(), // rate
                ArgSchema::number_lenient_scalar(), // par
                ArgSchema::number_lenient_scalar(), // basis (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Check minimum required arguments
        if args.len() < 4 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let issue_serial = coerce_num(&args[0])?;
        let settlement_serial = coerce_num(&args[1])?;
        let rate = coerce_num(&args[2])?;
        let par = coerce_num(&args[3])?;
        let basis_int = if args.len() > 4 {
            coerce_num(&args[4])?.trunc() as i32
        } else {
            0
        };

        // Validate inputs
        if rate <= 0.0 || par <= 0.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let basis = DayCountBasis::from_int(basis_int)?;

        let issue = serial_to_date(issue_serial)?;
        let settlement = serial_to_date(settlement_serial)?;

        // settlement must be after issue
        if settlement <= issue {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // ACCRINTM = par * rate * year_fraction(issue, settlement)
        let yf = year_fraction(&issue, &settlement, basis);
        let accrued_interest = par * rate * yf;

        Ok(CalcValue::Scalar(LiteralValue::Number(accrued_interest)))
    }
}

/// PRICE(settlement, maturity, rate, yld, redemption, frequency, [basis])
/// Returns price per $100 face value for a security that pays periodic interest
#[derive(Debug)]
pub struct PriceFn;

impl Function for PriceFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PRICE"
    }
    fn min_args(&self) -> usize {
        6
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(), // settlement
                ArgSchema::number_lenient_scalar(), // maturity
                ArgSchema::number_lenient_scalar(), // rate (coupon rate)
                ArgSchema::number_lenient_scalar(), // yld (yield)
                ArgSchema::number_lenient_scalar(), // redemption
                ArgSchema::number_lenient_scalar(), // frequency
                ArgSchema::number_lenient_scalar(), // basis (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Check minimum required arguments
        if args.len() < 6 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let settlement_serial = coerce_num(&args[0])?;
        let maturity_serial = coerce_num(&args[1])?;
        let rate = coerce_num(&args[2])?;
        let yld = coerce_num(&args[3])?;
        let redemption = coerce_num(&args[4])?;
        let frequency = coerce_num(&args[5])?.trunc() as i32;
        let basis_int = if args.len() > 6 {
            coerce_num(&args[6])?.trunc() as i32
        } else {
            0
        };

        // Validate inputs
        if rate < 0.0 || yld < 0.0 || redemption <= 0.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }
        if frequency != 1 && frequency != 2 && frequency != 4 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let basis = DayCountBasis::from_int(basis_int)?;

        let settlement = serial_to_date(settlement_serial)?;
        let maturity = serial_to_date(maturity_serial)?;

        // maturity must be after settlement
        if maturity <= settlement {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let price = calculate_price(
            &settlement,
            &maturity,
            rate,
            yld,
            redemption,
            frequency,
            basis,
        );
        Ok(CalcValue::Scalar(LiteralValue::Number(price)))
    }
}

/// Calculate bond price using standard bond pricing formula
fn calculate_price(
    settlement: &NaiveDate,
    maturity: &NaiveDate,
    rate: f64,
    yld: f64,
    redemption: f64,
    frequency: i32,
    basis: DayCountBasis,
) -> f64 {
    let n = coupons_remaining(settlement, maturity, frequency);
    let coupon = 100.0 * rate / frequency as f64;

    // Find previous and next coupon dates
    let next_coupon = coupon_date_after(settlement, maturity, frequency);
    let prev_coupon = coupon_date_before(settlement, maturity, frequency);

    // Calculate fraction of period from settlement to next coupon
    let days_to_next = days_between(settlement, &next_coupon, basis) as f64;
    let days_in_period = days_between(&prev_coupon, &next_coupon, basis) as f64;

    let dsn = if days_in_period > 0.0 {
        days_to_next / days_in_period
    } else {
        0.0
    };

    let yld_per_period = yld / frequency as f64;

    if n == 1 {
        // Short first period (single coupon remaining)
        // Price = (redemption + coupon) / (1 + dsn * yld_per_period) - (1 - dsn) * coupon
        let price = (redemption + coupon) / (1.0 + dsn * yld_per_period) - (1.0 - dsn) * coupon;
        price
    } else {
        // Multiple coupons remaining
        // Price = sum of discounted coupons + discounted redemption - accrued interest
        let discount_factor = 1.0 + yld_per_period;

        // Discount factor for first coupon (fractional period)
        let first_discount = discount_factor.powf(dsn);

        // Present value of coupon payments
        let mut pv_coupons = 0.0;
        for k in 0..n {
            let discount = first_discount * discount_factor.powi(k);
            pv_coupons += coupon / discount;
        }

        // Present value of redemption
        let pv_redemption = redemption / (first_discount * discount_factor.powi(n - 1));

        // Accrued interest (negative because we subtract it)
        let accrued = (1.0 - dsn) * coupon;

        pv_coupons + pv_redemption - accrued
    }
}

/// YIELD(settlement, maturity, rate, pr, redemption, frequency, [basis])
/// Returns yield of a security that pays periodic interest
#[derive(Debug)]
pub struct YieldFn;

impl Function for YieldFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "YIELD"
    }
    fn min_args(&self) -> usize {
        6
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                ArgSchema::number_lenient_scalar(), // settlement
                ArgSchema::number_lenient_scalar(), // maturity
                ArgSchema::number_lenient_scalar(), // rate (coupon rate)
                ArgSchema::number_lenient_scalar(), // pr (price)
                ArgSchema::number_lenient_scalar(), // redemption
                ArgSchema::number_lenient_scalar(), // frequency
                ArgSchema::number_lenient_scalar(), // basis (optional)
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        // Check minimum required arguments
        if args.len() < 6 {
            return Ok(CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value(),
            )));
        }

        let settlement_serial = coerce_num(&args[0])?;
        let maturity_serial = coerce_num(&args[1])?;
        let rate = coerce_num(&args[2])?;
        let pr = coerce_num(&args[3])?;
        let redemption = coerce_num(&args[4])?;
        let frequency = coerce_num(&args[5])?.trunc() as i32;
        let basis_int = if args.len() > 6 {
            coerce_num(&args[6])?.trunc() as i32
        } else {
            0
        };

        // Validate inputs
        if rate < 0.0 || pr <= 0.0 || redemption <= 0.0 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }
        if frequency != 1 && frequency != 2 && frequency != 4 {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        let basis = DayCountBasis::from_int(basis_int)?;

        let settlement = serial_to_date(settlement_serial)?;
        let maturity = serial_to_date(maturity_serial)?;

        // maturity must be after settlement
        if maturity <= settlement {
            return Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            ));
        }

        // Use Newton-Raphson to find yield where price = target price
        let yld = calculate_yield(
            &settlement,
            &maturity,
            rate,
            pr,
            redemption,
            frequency,
            basis,
        );

        match yld {
            Some(y) => Ok(CalcValue::Scalar(LiteralValue::Number(y))),
            None => Ok(CalcValue::Scalar(
                LiteralValue::Error(ExcelError::new_num()),
            )),
        }
    }
}

/// Calculate yield using Newton-Raphson iteration
fn calculate_yield(
    settlement: &NaiveDate,
    maturity: &NaiveDate,
    rate: f64,
    target_price: f64,
    redemption: f64,
    frequency: i32,
    basis: DayCountBasis,
) -> Option<f64> {
    const MAX_ITER: i32 = 100;
    const EPSILON: f64 = 1e-10;

    // Initial guess based on coupon rate
    let mut yld = rate;
    if yld == 0.0 {
        yld = 0.05; // Default guess if rate is 0
    }

    for _ in 0..MAX_ITER {
        let price = calculate_price(
            settlement, maturity, rate, yld, redemption, frequency, basis,
        );
        let diff = price - target_price;

        if diff.abs() < EPSILON {
            return Some(yld);
        }

        // Calculate derivative numerically
        let delta = 0.0001;
        let price_up = calculate_price(
            settlement,
            maturity,
            rate,
            yld + delta,
            redemption,
            frequency,
            basis,
        );
        let derivative = (price_up - price) / delta;

        if derivative.abs() < EPSILON {
            return None;
        }

        let new_yld = yld - diff / derivative;

        // Prevent yield from going too negative
        if new_yld < -0.99 {
            yld = -0.99;
        } else {
            yld = new_yld;
        }

        // Prevent yield from going too high
        if yld > 10.0 {
            yld = 10.0;
        }
    }

    // If close enough after max iterations, return the result
    let final_price = calculate_price(
        settlement, maturity, rate, yld, redemption, frequency, basis,
    );
    if (final_price - target_price).abs() < 0.01 {
        Some(yld)
    } else {
        None
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(AccrintFn));
    crate::function_registry::register_function(Arc::new(AccrintmFn));
    crate::function_registry::register_function(Arc::new(PriceFn));
    crate::function_registry::register_function(Arc::new(YieldFn));
}
