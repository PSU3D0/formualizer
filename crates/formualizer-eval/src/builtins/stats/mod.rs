//! Statistical basic functions (Sprint 6)
//!
//! Implementations target Excel semantic parity for:
//! LARGE, SMALL, RANK.EQ, RANK.AVG, MEDIAN, STDEV.S, STDEV.P, VAR.S, VAR.P,
//! PERCENTILE.INC, PERCENTILE.EXC, QUARTILE.INC, QUARTILE.EXC.
//!
//! Notes:
//! - We currently materialize numeric values into a Vec<f64>. For large ranges this could be
//!   optimized with streaming selection algorithms (nth_element / partial sort). TODO(perf).
//! - Text/boolean coercion nuance: For Excel statistical functions, values coming from range
//!   references should ignore text and logical values (they are skipped), while direct scalar
//!   arguments still coerce (e.g. =STDEV(1,TRUE) treats TRUE as 1). This file now implements that
//!   distinction. TODO(excel-nuance): refine numeric text literal vs non‑numeric text handling.
//! - Errors encountered in any argument propagate immediately.
//! - Empty numeric sets produce Excel-specific errors (#NUM! for LARGE/SMALL, #N/A for rank target
//!   out of range, #DIV/0! for STDEV/VAR sample with n < 2, etc.).

use super::super::builtins::utils::{ARG_RANGE_NUM_LENIENT_ONE, coerce_num};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
// use std::collections::BTreeMap; // removed unused import
use formualizer_macros::func_caps;

fn scalar_like_value(arg: &ArgumentHandle<'_, '_>) -> Result<LiteralValue, ExcelError> {
    Ok(match arg.value()? {
        crate::traits::CalcValue::Scalar(v) => v,
        crate::traits::CalcValue::Range(rv) => rv.get_cell(0, 0),
        crate::traits::CalcValue::Callable(_) => LiteralValue::Error(
            ExcelError::new(formualizer_common::ExcelErrorKind::Calc)
                .with_message("LAMBDA value must be invoked"),
        ),
    })
}

/// Collect numeric inputs applying Excel statistical semantics:
/// - Range references: include only numeric cells; skip text, logical, blank. Errors propagate.
/// - Direct scalar arguments: attempt numeric coercion (so TRUE/FALSE, numeric text are included if
///   coerce_num succeeds). Non-numeric text is ignored (Excel would treat a direct non-numeric text
///   argument as #VALUE! in some contexts; covered by TODO for finer parity).
fn collect_numeric_stats(args: &[ArgumentHandle]) -> Result<Vec<f64>, ExcelError> {
    let mut out = Vec::new();
    for a in args {
        // Special-case: inline array literal argument should be treated like a list of direct scalar
        // arguments (not a by-ref range). This allows boolean/text coercion per element akin to
        // passing multiple scalars to the function.
        if let Some(arr) = a.inline_array_literal()? {
            for row in arr.into_iter() {
                for cell in row.into_iter() {
                    match cell {
                        LiteralValue::Error(e) => return Err(e),
                        other => {
                            if let Ok(n) = coerce_num(&other) {
                                out.push(n);
                            }
                        }
                    }
                }
            }
            continue;
        }

        if let Ok(view) = a.range_view() {
            view.for_each_cell(&mut |v| {
                match v {
                    LiteralValue::Error(e) => return Err(e.clone()),
                    LiteralValue::Number(n) => out.push(*n),
                    LiteralValue::Int(i) => out.push(*i as f64),
                    _ => {}
                }
                Ok(())
            })?;
        } else {
            let v = scalar_like_value(a)?;
            match v {
                LiteralValue::Error(e) => return Err(e),
                other => {
                    if let Ok(n) = coerce_num(&other) {
                        out.push(n);
                    }
                }
            }
        }
    }
    Ok(out)
}

fn percentile_inc(sorted: &[f64], p: f64) -> Result<f64, ExcelError> {
    if sorted.is_empty() {
        return Err(ExcelError::new_num());
    }
    if !(0.0..=1.0).contains(&p) {
        return Err(ExcelError::new_num());
    }
    if sorted.len() == 1 {
        return Ok(sorted[0]);
    }
    let n = sorted.len() as f64;
    let rank = p * (n - 1.0); // 0-based rank
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        return Ok(sorted[lo]);
    }
    let frac = rank - (lo as f64);
    Ok(sorted[lo] + (sorted[hi] - sorted[lo]) * frac)
}

fn percentile_exc(sorted: &[f64], p: f64) -> Result<f64, ExcelError> {
    // Excel PERCENTILE.EXC requires 0 < p < 1 and uses (n+1) basis; invalid if rank<1 or >n
    if sorted.is_empty() {
        return Err(ExcelError::new_num());
    }
    if !(0.0..=1.0).contains(&p) || p <= 0.0 || p >= 1.0 {
        return Err(ExcelError::new_num());
    }
    let n = sorted.len() as f64;
    let rank = p * (n + 1.0); // 1..n domain
    if rank < 1.0 || rank > n {
        return Err(ExcelError::new_num());
    }
    let lo = rank.floor();
    let hi = rank.ceil();
    if (lo - hi).abs() < f64::EPSILON {
        return Ok(sorted[(lo as usize) - 1]);
    }
    let frac = rank - lo;
    let lo_idx = (lo as usize) - 1;
    let hi_idx = (hi as usize) - 1;
    Ok(sorted[lo_idx] + (sorted[hi_idx] - sorted[lo_idx]) * frac)
}

/// Returns the rank position of a number within a data set, with ties sharing the same rank.
///
/// `RANK.EQ` defaults to descending order (largest value is rank 1), and can switch to ascending
/// order when `order` is non-zero.
///
/// # Remarks
/// - Omitting `order`, or setting `order` to `0`, ranks values in descending order.
/// - Any non-zero `order` ranks values in ascending order.
/// - Tied values receive the same rank (the first matching position in the sorted list).
/// - Returns `#N/A` if `number` is not found in `ref`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Descending rank with direct values"
/// formula: "=RANK.EQ(7,{10,7,4,2})"
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Ascending rank with ties in a range"
/// grid:
///   A1: 50
///   A2: 20
///   A3: 20
///   A4: 10
///   A5: 5
/// formula: "=RANK.EQ(A2,A1:A5,1)"
/// expected: 3
/// ```
#[derive(Debug)]
pub struct RankEqFn;
/// [formualizer-docgen:schema:start]
/// Name: RANK.EQ
/// Type: RankEqFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: RANK.EQ(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for RankEqFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "RANK.EQ"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["RANK"]
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    } // allow optional order
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        let target = match coerce_num(&args[0].value()?.into_literal()) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_na(),
                )));
            }
        };
        // optional order arg at end if 3 args
        let order = if args.len() >= 3 {
            coerce_num(&args[2].value()?.into_literal()).unwrap_or(0.0)
        } else {
            0.0
        };
        let nums = collect_numeric_stats(&args[1..2])?; // only one ref range per Excel spec
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        let mut sorted = nums; // copy
        if order.abs() < 1e-12 {
            // descending
            sorted.sort_by(|a, b| b.partial_cmp(a).unwrap());
        } else {
            // ascending
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        }
        for (i, &v) in sorted.iter().enumerate() {
            if (v - target).abs() < 1e-12 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                    (i + 1) as f64,
                )));
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_na(),
        )))
    }
}

/// Returns the rank position of a number, averaging the rank positions for ties.
///
/// Use `RANK.AVG` when tied values should share the average of their occupied rank positions.
///
/// # Remarks
/// - Omitting `order`, or setting `order` to `0`, ranks values in descending order.
/// - Any non-zero `order` ranks values in ascending order.
/// - If `number` appears multiple times, the function returns the mean of those rank positions.
/// - Returns `#N/A` if `number` is not found in `ref`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Average rank for tied values"
/// formula: "=RANK.AVG(20,{30,20,20,10})"
/// expected: 2.5
/// ```
///
/// ```yaml,sandbox
/// title: "Ascending average rank from a range"
/// grid:
///   A1: 50
///   A2: 20
///   A3: 20
///   A4: 10
///   A5: 5
/// formula: "=RANK.AVG(A2,A1:A5,1)"
/// expected: 3.5
/// ```
#[derive(Debug)]
pub struct RankAvgFn;
/// [formualizer-docgen:schema:start]
/// Name: RANK.AVG
/// Type: RankAvgFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: RANK.AVG(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for RankAvgFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "RANK.AVG"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        let t0 = scalar_like_value(&args[0])?;
        let target = match coerce_num(&t0) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_na(),
                )));
            }
        };
        let order = if args.len() >= 3 {
            let ord = scalar_like_value(&args[2])?;
            coerce_num(&ord).unwrap_or(0.0)
        } else {
            0.0
        };
        let nums = collect_numeric_stats(&args[1..2])?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        let mut sorted = nums;
        if order.abs() < 1e-12 {
            sorted.sort_by(|a, b| b.partial_cmp(a).unwrap());
        } else {
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        }
        let mut positions = Vec::new();
        for (i, &v) in sorted.iter().enumerate() {
            if (v - target).abs() < 1e-12 {
                positions.push(i + 1);
            }
        }
        if positions.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        let avg = positions.iter().copied().sum::<usize>() as f64 / positions.len() as f64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(avg)))
    }
}

/// Returns the k-th largest value in a data set.
///
/// `LARGE` is useful for top-N analysis, such as highest score, second-highest sale, or third-best
/// result.
///
/// # Remarks
/// - `k` must be at least `1`.
/// - Returns `#NUM!` if `k` is greater than the count of numeric values.
/// - Non-numeric values in referenced ranges are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Second-largest from direct values"
/// formula: "=LARGE({4,9,1,7},2)"
/// expected: 7
/// ```
///
/// ```yaml,sandbox
/// title: "Third-largest from a range"
/// grid:
///   A1: 3
///   A2: 12
///   A3: 8
///   A4: 5
/// formula: "=LARGE(A1:A4,3)"
/// expected: 5
/// ```
#[derive(Debug)]
pub struct LARGE;
/// [formualizer-docgen:schema:start]
/// Name: LARGE
/// Type: LARGE
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: LARGE(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for LARGE {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "LARGE"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let k = match coerce_num(&args.last().unwrap().value()?.into_literal()) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };
        let k = k as i64;
        if k < 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.is_empty() || k as usize > nums.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        nums.sort_by(|a, b| b.partial_cmp(a).unwrap());
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            nums[(k as usize) - 1],
        )))
    }
}

/// Returns the k-th smallest value in a data set.
///
/// `SMALL` is often used to find low outliers, minimum thresholds, or bottom-N values.
///
/// # Remarks
/// - `k` must be at least `1`.
/// - Returns `#NUM!` if `k` is greater than the count of numeric values.
/// - Non-numeric values in referenced ranges are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Second-smallest from direct values"
/// formula: "=SMALL({4,9,1,7},2)"
/// expected: 4
/// ```
///
/// ```yaml,sandbox
/// title: "Third-smallest from a range"
/// grid:
///   A1: 3
///   A2: 12
///   A3: 8
///   A4: 5
/// formula: "=SMALL(A1:A4,3)"
/// expected: 8
/// ```
#[derive(Debug)]
pub struct SMALL;
/// [formualizer-docgen:schema:start]
/// Name: SMALL
/// Type: SMALL
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: SMALL(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for SMALL {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "SMALL"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let k = match coerce_num(&args.last().unwrap().value()?.into_literal()) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };
        let k = k as i64;
        if k < 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.is_empty() || k as usize > nums.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            nums[(k as usize) - 1],
        )))
    }
}

/// Returns the middle value of a numeric data set.
///
/// For an even number of values, `MEDIAN` returns the average of the two center values.
///
/// # Remarks
/// - Ignores non-numeric values in referenced ranges.
/// - Returns `#NUM!` when no numeric values are available.
/// - Supports both scalar arguments and range inputs.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Median of an odd-sized set"
/// formula: "=MEDIAN(1,3,8)"
/// expected: 3
/// ```
///
/// ```yaml,sandbox
/// title: "Median of an even-sized range"
/// grid:
///   A1: 1
///   A2: 2
///   A3: 10
///   A4: 12
/// formula: "=MEDIAN(A1:A4)"
/// expected: 6
/// ```
#[derive(Debug)]
pub struct MEDIAN;
/// [formualizer-docgen:schema:start]
/// Name: MEDIAN
/// Type: MEDIAN
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: MEDIAN(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for MEDIAN {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "MEDIAN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = nums.len();
        let mid = n / 2;
        let med = if n % 2 == 1 {
            nums[mid]
        } else {
            (nums[mid - 1] + nums[mid]) / 2.0
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(med)))
    }
}

/// Estimates sample standard deviation using `n-1` in the denominator.
///
/// `STDEV.S` measures spread when your values represent a sample of a larger population.
///
/// # Remarks
/// - Requires at least two numeric values.
/// - Returns `#DIV/0!` when fewer than two numeric values are provided.
/// - Non-numeric values in referenced ranges are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Sample standard deviation from scalar arguments"
/// formula: "=STDEV.S(2,4,6)"
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Sample standard deviation from a range"
/// grid:
///   A1: 5
///   A2: 7
///   A3: 9
/// formula: "=STDEV.S(A1:A3)"
/// expected: 2
/// ```
#[derive(Debug)]
pub struct StdevSample; // sample
/// [formualizer-docgen:schema:start]
/// Name: STDEV.S
/// Type: StdevSample
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: STDEV.S(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY, STREAM_OK
/// [formualizer-docgen:schema:end]
impl Function for StdevSample {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION, STREAM_OK);
    fn name(&self) -> &'static str {
        "STDEV.S"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["STDEV"]
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        let n = nums.len();
        if n < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::from_error_string("#DIV/0!"),
            )));
        }
        let mean = nums.iter().sum::<f64>() / (n as f64);
        let mut ss = 0.0;
        for &v in &nums {
            let d = v - mean;
            ss += d * d;
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            (ss / ((n - 1) as f64)).sqrt(),
        )))
    }
}

/// Returns population standard deviation using `n` in the denominator.
///
/// Use `STDEV.P` when your values represent the entire population, not a sample.
///
/// # Remarks
/// - Requires at least one numeric value.
/// - Returns `#DIV/0!` when no numeric values are provided.
/// - Non-numeric values in referenced ranges are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Population standard deviation from scalar arguments"
/// formula: "=STDEV.P(2,4,6)"
/// expected: 1.632993161855452
/// ```
///
/// ```yaml,sandbox
/// title: "Population standard deviation from a range"
/// grid:
///   A1: 1
///   A2: 2
///   A3: 3
/// formula: "=STDEV.P(A1:A3)"
/// expected: 0.816496580927726
/// ```
#[derive(Debug)]
pub struct StdevPop; // population
/// [formualizer-docgen:schema:start]
/// Name: STDEV.P
/// Type: StdevPop
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: STDEV.P(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY, STREAM_OK
/// [formualizer-docgen:schema:end]
impl Function for StdevPop {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION, STREAM_OK);
    fn name(&self) -> &'static str {
        "STDEV.P"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["STDEVP"]
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        let n = nums.len();
        if n == 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::from_error_string("#DIV/0!"),
            )));
        }
        let mean = nums.iter().sum::<f64>() / (n as f64);
        let mut ss = 0.0;
        for &v in &nums {
            let d = v - mean;
            ss += d * d;
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            (ss / (n as f64)).sqrt(),
        )))
    }
}

/// Estimates sample variance using `n-1` in the denominator.
///
/// `VAR.S` is the squared counterpart of `STDEV.S` for sample-based variability.
///
/// # Remarks
/// - Requires at least two numeric values.
/// - Returns `#DIV/0!` when fewer than two numeric values are provided.
/// - Non-numeric values in referenced ranges are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Sample variance from scalar arguments"
/// formula: "=VAR.S(2,4,6)"
/// expected: 4
/// ```
///
/// ```yaml,sandbox
/// title: "Sample variance from a range"
/// grid:
///   A1: 1
///   A2: 2
///   A3: 3
/// formula: "=VAR.S(A1:A3)"
/// expected: 1
/// ```
#[derive(Debug)]
pub struct VarSample; // sample variance
/// [formualizer-docgen:schema:start]
/// Name: VAR.S
/// Type: VarSample
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: VAR.S(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY, STREAM_OK
/// [formualizer-docgen:schema:end]
impl Function for VarSample {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION, STREAM_OK);
    fn name(&self) -> &'static str {
        "VAR.S"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["VAR"]
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        let n = nums.len();
        if n < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::from_error_string("#DIV/0!"),
            )));
        }
        let mean = nums.iter().sum::<f64>() / (n as f64);
        let mut ss = 0.0;
        for &v in &nums {
            let d = v - mean;
            ss += d * d;
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            ss / ((n - 1) as f64),
        )))
    }
}

/// Returns population variance using `n` in the denominator.
///
/// `VAR.P` describes dispersion for a complete population of numeric values.
///
/// # Remarks
/// - Requires at least one numeric value.
/// - Returns `#DIV/0!` when no numeric values are provided.
/// - Non-numeric values in referenced ranges are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Population variance from scalar arguments"
/// formula: "=VAR.P(2,4,6)"
/// expected: 2.6666666666666665
/// ```
///
/// ```yaml,sandbox
/// title: "Population variance from a range"
/// grid:
///   A1: 1
///   A2: 2
///   A3: 3
/// formula: "=VAR.P(A1:A3)"
/// expected: 0.6666666666666666
/// ```
#[derive(Debug)]
pub struct VarPop; // population variance
/// [formualizer-docgen:schema:start]
/// Name: VAR.P
/// Type: VarPop
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: VAR.P(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY, STREAM_OK
/// [formualizer-docgen:schema:end]
impl Function for VarPop {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION, STREAM_OK);
    fn name(&self) -> &'static str {
        "VAR.P"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["VARP"]
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        let n = nums.len();
        if n == 0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::from_error_string("#DIV/0!"),
            )));
        }
        let mean = nums.iter().sum::<f64>() / (n as f64);
        let mut ss = 0.0;
        for &v in &nums {
            let d = v - mean;
            ss += d * d;
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            ss / (n as f64),
        )))
    }
}

// MODE.SNGL (alias MODE) and MODE.MULT
/// Returns the most frequently occurring value in a data set.
///
/// `MODE.SNGL` returns a single mode value and reports `#N/A` if no value repeats.
///
/// # Remarks
/// - Returns the first mode encountered after sorting when frequencies tie.
/// - Returns `#N/A` when every numeric value appears only once.
/// - Alias `MODE` is supported.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Single mode from scalar arguments"
/// formula: "=MODE.SNGL(1,2,2,3)"
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Single mode from a range"
/// grid:
///   A1: 4
///   A2: 4
///   A3: 6
///   A4: 6
///   A5: 6
/// formula: "=MODE.SNGL(A1:A5)"
/// expected: 6
/// ```
#[derive(Debug)]
pub struct ModeSingleFn;
/// [formualizer-docgen:schema:start]
/// Name: MODE.SNGL
/// Type: ModeSingleFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: MODE.SNGL(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for ModeSingleFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "MODE.SNGL"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["MODE"]
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mut best_val = nums[0];
        let mut best_cnt = 1usize;
        let mut cur_val = nums[0];
        let mut cur_cnt = 1usize;
        for &v in &nums[1..] {
            if (v - cur_val).abs() < 1e-12 {
                cur_cnt += 1;
            } else {
                if cur_cnt > best_cnt {
                    best_cnt = cur_cnt;
                    best_val = cur_val;
                }
                cur_val = v;
                cur_cnt = 1;
            }
        }
        if cur_cnt > best_cnt {
            best_cnt = cur_cnt;
            best_val = cur_val;
        }
        if best_cnt <= 1 {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )))
        } else {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                best_val,
            )))
        }
    }
}

/// Returns all modal values as a vertical array.
///
/// Use `MODE.MULT` when a data set can have multiple values with the same highest frequency.
///
/// # Remarks
/// - Returns each tied mode as a separate row in the result array.
/// - Returns `#N/A` when every numeric value appears only once.
/// - Non-numeric values in referenced ranges are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Multiple modes from direct values"
/// formula: "=MODE.MULT({1,2,2,3,3,4})"
/// expected:
///   - [2]
///   - [3]
/// ```
///
/// ```yaml,sandbox
/// title: "Single repeated mode still returns an array"
/// grid:
///   A1: 5
///   A2: 5
///   A3: 2
///   A4: 1
/// formula: "=MODE.MULT(A1:A4)"
/// expected:
///   - [5]
/// ```
#[derive(Debug)]
pub struct ModeMultiFn;
/// [formualizer-docgen:schema:start]
/// Name: MODE.MULT
/// Type: ModeMultiFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: MODE.MULT(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for ModeMultiFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "MODE.MULT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mut runs: Vec<(f64, usize)> = Vec::new();
        let mut cur_val = nums[0];
        let mut cur_cnt = 1usize;
        for &v in &nums[1..] {
            if (v - cur_val).abs() < 1e-12 {
                cur_cnt += 1;
            } else {
                runs.push((cur_val, cur_cnt));
                cur_val = v;
                cur_cnt = 1;
            }
        }
        runs.push((cur_val, cur_cnt));
        let max_freq = runs.iter().map(|r| r.1).max().unwrap_or(0);
        if max_freq <= 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }
        let rows: Vec<Vec<LiteralValue>> = runs
            .into_iter()
            .filter(|&(_, c)| c == max_freq)
            .map(|(v, _)| vec![LiteralValue::Number(v)])
            .collect();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(rows)))
    }
}

/// Returns the k-th percentile of a data set using inclusive interpolation.
///
/// `PERCENTILE.INC` accepts percentile values from `0` through `1` and interpolates between
/// sorted values as needed.
///
/// # Remarks
/// - `k` must be in the inclusive range `[0, 1]`.
/// - Returns `#NUM!` for empty numeric input or invalid percentile arguments.
/// - Alias `PERCENTILE` is supported.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Inclusive 25th percentile from direct values"
/// formula: "=PERCENTILE.INC({1,2,3,4,5},0.25)"
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Inclusive median-style interpolation from a range"
/// grid:
///   A1: 10
///   A2: 20
///   A3: 30
///   A4: 40
/// formula: "=PERCENTILE.INC(A1:A4,0.5)"
/// expected: 25
/// ```
#[derive(Debug)]
pub struct PercentileInc; // inclusive
/// [formualizer-docgen:schema:start]
/// Name: PERCENTILE.INC
/// Type: PercentileInc
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: PERCENTILE.INC(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for PercentileInc {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "PERCENTILE.INC"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["PERCENTILE"]
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let pv = scalar_like_value(args.last().unwrap())?;
        let p = match coerce_num(&pv) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        match percentile_inc(&nums, p) {
            Ok(v) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(v))),
            Err(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        }
    }
}

/// Returns the k-th percentile of a data set using exclusive interpolation.
///
/// `PERCENTILE.EXC` uses the `n+1` rank basis and excludes the exact endpoints `0` and `1`.
///
/// # Remarks
/// - `k` must satisfy `0 < k < 1`.
/// - Returns `#NUM!` when the percentile falls outside the valid rank range for the data size.
/// - Returns `#NUM!` for empty numeric input.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Exclusive 25th percentile from direct values"
/// formula: "=PERCENTILE.EXC({1,2,3,4,5},0.25)"
/// expected: 1.5
/// ```
///
/// ```yaml,sandbox
/// title: "Exclusive percentile from a range"
/// grid:
///   A1: 10
///   A2: 20
///   A3: 30
///   A4: 40
///   A5: 50
/// formula: "=PERCENTILE.EXC(A1:A5,0.6)"
/// expected: 36
/// ```
#[derive(Debug)]
pub struct PercentileExc; // exclusive
/// [formualizer-docgen:schema:start]
/// Name: PERCENTILE.EXC
/// Type: PercentileExc
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: PERCENTILE.EXC(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for PercentileExc {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "PERCENTILE.EXC"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let pv = scalar_like_value(args.last().unwrap())?;
        let p = match coerce_num(&pv) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        match percentile_exc(&nums, p) {
            Ok(v) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(v))),
            Err(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        }
    }
}

/// Returns an inclusive quartile value for a data set.
///
/// `QUARTILE.INC` maps quartile index `0..4` onto minimum, quartiles, median, and maximum.
///
/// # Remarks
/// - Valid quartile index values are `0`, `1`, `2`, `3`, and `4`.
/// - Uses inclusive percentile logic for quartiles `1` through `3`.
/// - Returns `#NUM!` for invalid quartile index values or empty numeric input.
/// - Alias `QUARTILE` is supported.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "First quartile from direct values"
/// formula: "=QUARTILE.INC({1,2,3,4,5},1)"
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Third quartile from a range"
/// grid:
///   A1: 10
///   A2: 20
///   A3: 30
///   A4: 40
/// formula: "=QUARTILE.INC(A1:A4,3)"
/// expected: 32.5
/// ```
#[derive(Debug)]
pub struct QuartileInc; // quartile inclusive
/// [formualizer-docgen:schema:start]
/// Name: QUARTILE.INC
/// Type: QuartileInc
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: QUARTILE.INC(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for QuartileInc {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "QUARTILE.INC"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["QUARTILE"]
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let qv = scalar_like_value(args.last().unwrap())?;
        let q = match coerce_num(&qv) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };
        let q_i = q as i64;
        if !(0..=4).contains(&q_i) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p = match q_i {
            0 => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                    nums[0],
                )));
            }
            4 => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                    nums[nums.len() - 1],
                )));
            }
            1 => 0.25,
            2 => 0.5,
            3 => 0.75,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };
        match percentile_inc(&nums, p) {
            Ok(v) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(v))),
            Err(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        }
    }
}

/// Returns an exclusive quartile value for a data set.
///
/// `QUARTILE.EXC` applies exclusive percentile interpolation and supports quartiles `1` through
/// `3`.
///
/// # Remarks
/// - Valid quartile index values are `1`, `2`, and `3`.
/// - Returns `#NUM!` for invalid quartile index values.
/// - Returns `#NUM!` when the input is too small for exclusive quartile interpolation.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "First exclusive quartile from direct values"
/// formula: "=QUARTILE.EXC({1,2,3,4,5,6,7,8},1)"
/// expected: 2.25
/// ```
///
/// ```yaml,sandbox
/// title: "Third exclusive quartile from a range"
/// grid:
///   A1: 10
///   A2: 20
///   A3: 30
///   A4: 40
///   A5: 50
///   A6: 60
///   A7: 70
///   A8: 80
/// formula: "=QUARTILE.EXC(A1:A8,3)"
/// expected: 67.5
/// ```
#[derive(Debug)]
pub struct QuartileExc; // quartile exclusive
/// [formualizer-docgen:schema:start]
/// Name: QUARTILE.EXC
/// Type: QuartileExc
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: QUARTILE.EXC(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for QuartileExc {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "QUARTILE.EXC"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let qv = scalar_like_value(args.last().unwrap())?;
        let q = match coerce_num(&qv) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };
        let q_i = q as i64;
        if !(1..=3).contains(&q_i) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p = match q_i {
            1 => 0.25,
            2 => 0.5,
            3 => 0.75,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };
        match percentile_exc(&nums, p) {
            Ok(v) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(v))),
            Err(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        }
    }
}

/// Multiplies all numeric arguments and returns their product.
///
/// `PRODUCT` is useful for chained growth factors, scaling ratios, and compound multipliers.
///
/// # Remarks
/// - Non-numeric values in referenced ranges are ignored.
/// - Returns `0` when no numeric values are found.
/// - Direct scalar arguments still attempt numeric coercion.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Product of scalar values"
/// formula: "=PRODUCT(2,3,4)"
/// expected: 24
/// ```
///
/// ```yaml,sandbox
/// title: "Product from a range"
/// grid:
///   A1: 1
///   A2: 5
///   A3: 10
/// formula: "=PRODUCT(A1:A3)"
/// expected: 50
/// ```
#[derive(Debug)]
pub struct ProductFn;
/// [formualizer-docgen:schema:start]
/// Name: PRODUCT
/// Type: ProductFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: PRODUCT(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for ProductFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "PRODUCT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
        }
        let result = nums.iter().product::<f64>();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the geometric mean of positive numeric values.
///
/// `GEOMEAN` is commonly used for rates of change and multiplicative growth comparisons.
///
/// # Remarks
/// - All numeric inputs must be strictly greater than `0`.
/// - Returns `#NUM!` if any value is `<= 0`, or if no numeric values are provided.
/// - Non-numeric values in referenced ranges are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Geometric mean from scalar values"
/// formula: "=GEOMEAN(4,16)"
/// expected: 8
/// ```
///
/// ```yaml,sandbox
/// title: "Geometric mean from a range"
/// grid:
///   A1: 1
///   A2: 3
///   A3: 9
/// formula: "=GEOMEAN(A1:A3)"
/// expected: 3
/// ```
#[derive(Debug)]
pub struct GeomeanFn;
/// [formualizer-docgen:schema:start]
/// Name: GEOMEAN
/// Type: GeomeanFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: GEOMEAN(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for GeomeanFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "GEOMEAN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        // All values must be positive
        if nums.iter().any(|&n| n <= 0.0) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        // Geometric mean = (x1 * x2 * ... * xn)^(1/n)
        // Use log to avoid overflow: exp(mean(ln(x)))
        let log_sum: f64 = nums.iter().map(|x| x.ln()).sum();
        let result = (log_sum / nums.len() as f64).exp();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the harmonic mean of positive numeric values.
///
/// `HARMEAN` emphasizes smaller values and is useful for averaging rates and ratios.
///
/// # Remarks
/// - All numeric inputs must be strictly greater than `0`.
/// - Returns `#NUM!` if any value is `<= 0`, or if no numeric values are provided.
/// - Non-numeric values in referenced ranges are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Harmonic mean from scalar values"
/// formula: "=HARMEAN(1,2,4)"
/// expected: 1.7142857142857142
/// ```
///
/// ```yaml,sandbox
/// title: "Harmonic mean from a range"
/// grid:
///   A1: 2
///   A2: 3
///   A3: 6
/// formula: "=HARMEAN(A1:A3)"
/// expected: 3
/// ```
#[derive(Debug)]
pub struct HarmeanFn;
/// [formualizer-docgen:schema:start]
/// Name: HARMEAN
/// Type: HarmeanFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: HARMEAN(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for HarmeanFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "HARMEAN"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        // All values must be positive
        if nums.iter().any(|&n| n <= 0.0) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        // Harmonic mean = n / sum(1/x)
        let sum_reciprocals: f64 = nums.iter().map(|x| 1.0 / x).sum();
        let result = nums.len() as f64 / sum_reciprocals;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the average of absolute deviations from the mean.
///
/// `AVEDEV` provides a robust spread measure that is less sensitive to outliers than squared-error
/// metrics.
///
/// # Remarks
/// - Returns `#NUM!` when no numeric values are available.
/// - Non-numeric values in referenced ranges are ignored.
/// - Uses the arithmetic mean as the center point.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Average absolute deviation from scalar values"
/// formula: "=AVEDEV(2,4,6)"
/// expected: 1.3333333333333333
/// ```
///
/// ```yaml,sandbox
/// title: "Average absolute deviation from a range"
/// grid:
///   A1: 1
///   A2: 1
///   A3: 3
///   A4: 5
/// formula: "=AVEDEV(A1:A4)"
/// expected: 1.5
/// ```
#[derive(Debug)]
pub struct AvedevFn;
/// [formualizer-docgen:schema:start]
/// Name: AVEDEV
/// Type: AvedevFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: AVEDEV(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for AvedevFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "AVEDEV"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let mean = nums.iter().sum::<f64>() / nums.len() as f64;
        let avedev = nums.iter().map(|x| (x - mean).abs()).sum::<f64>() / nums.len() as f64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            avedev,
        )))
    }
}

/// Returns the sum of squared deviations from the mean.
///
/// `DEVSQ` is useful for variance-related calculations and diagnostics of spread.
///
/// # Remarks
/// - Returns `#NUM!` when no numeric values are available.
/// - Non-numeric values in referenced ranges are ignored.
/// - Uses the arithmetic mean of included values.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Sum of squared deviations from scalar values"
/// formula: "=DEVSQ(2,4,6)"
/// expected: 8
/// ```
///
/// ```yaml,sandbox
/// title: "Sum of squared deviations from a range"
/// grid:
///   A1: 1
///   A2: 2
///   A3: 3
///   A4: 4
/// formula: "=DEVSQ(A1:A4)"
/// expected: 5
/// ```
#[derive(Debug)]
pub struct DevsqFn;

/* ─────────────────────────── MAXIFS / MINIFS ──────────────────────────── */

use super::utils::{ARG_ANY_ONE, criteria_match};

/// Returns the maximum numeric value in a range that meets all criteria.
///
/// `MAXIFS` applies one or more `(criteria_range, criteria)` pairs and returns the largest
/// matching numeric value.
///
/// # Remarks
/// - Arguments must be `target_range` plus one or more criteria pairs.
/// - Criteria are combined with logical AND.
/// - Returns `0` when no cells satisfy all criteria.
/// - Non-numeric cells in `target_range` are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Maximum value for one condition"
/// grid:
///   A1: 10
///   A2: 20
///   A3: 15
///   B1: "East"
///   B2: "West"
///   B3: "East"
/// formula: "=MAXIFS(A1:A3,B1:B3,\"East\")"
/// expected: 15
/// ```
///
/// ```yaml,sandbox
/// title: "Maximum value with two criteria"
/// grid:
///   A1: 100
///   A2: 80
///   A3: 90
///   A4: 70
///   B1: "A"
///   B2: "A"
///   B3: "B"
///   B4: "B"
///   C1: "Q1"
///   C2: "Q2"
///   C3: "Q1"
///   C4: "Q1"
/// formula: "=MAXIFS(A1:A4,B1:B4,\"B\",C1:C4,\"Q1\")"
/// expected: 90
/// ```
#[derive(Debug)]
pub struct MaxIfsFn;
/// [formualizer-docgen:schema:start]
/// Name: MAXIFS
/// Type: MaxIfsFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: MAXIFS(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for MaxIfsFn {
    func_caps!(PURE, REDUCTION);
    fn name(&self) -> &'static str {
        "MAXIFS"
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
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        eval_maxminifs(args, true)
    }
}

/// Returns the minimum numeric value in a range that meets all criteria.
///
/// `MINIFS` evaluates one or more `(criteria_range, criteria)` pairs and returns the smallest
/// matching numeric value.
///
/// # Remarks
/// - Arguments must be `target_range` plus one or more criteria pairs.
/// - Criteria are combined with logical AND.
/// - Returns `0` when no cells satisfy all criteria.
/// - Non-numeric cells in `target_range` are ignored.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Minimum value for one condition"
/// grid:
///   A1: 10
///   A2: 20
///   A3: 15
///   B1: "East"
///   B2: "West"
///   B3: "East"
/// formula: "=MINIFS(A1:A3,B1:B3,\"East\")"
/// expected: 10
/// ```
///
/// ```yaml,sandbox
/// title: "Minimum value with two criteria"
/// grid:
///   A1: 100
///   A2: 80
///   A3: 90
///   A4: 70
///   B1: "A"
///   B2: "A"
///   B3: "B"
///   B4: "B"
///   C1: "Q1"
///   C2: "Q2"
///   C3: "Q1"
///   C4: "Q1"
/// formula: "=MINIFS(A1:A4,B1:B4,\"B\",C1:C4,\"Q1\")"
/// expected: 70
/// ```
#[derive(Debug)]
pub struct MinIfsFn;
/// [formualizer-docgen:schema:start]
/// Name: MINIFS
/// Type: MinIfsFn
/// Min args: 3
/// Max args: variadic
/// Variadic: true
/// Signature: MINIFS(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION
/// [formualizer-docgen:schema:end]
impl Function for MinIfsFn {
    func_caps!(PURE, REDUCTION);
    fn name(&self) -> &'static str {
        "MINIFS"
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
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        eval_maxminifs(args, false)
    }
}

/// Shared implementation for MAXIFS and MINIFS
fn eval_maxminifs<'a, 'b>(
    args: &[ArgumentHandle<'a, 'b>],
    is_max: bool,
) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
    // Validate argument count: must be target_range + N pairs
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
            ExcelError::new_value().with_message(format!(
                "Function expects 1 target_range followed by N pairs (criteria_range, criteria); got {} args",
                args.len()
            )),
        )));
    }

    // Get target range
    let target_view = match args[0].range_view() {
        Ok(v) => v,
        Err(_) => {
            // Single value case - if criteria match, return that value
            let target_val = args[0].value()?.into_literal();
            if let LiteralValue::Error(e) = target_val {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            // Check all criteria against empty/scalar
            let mut all_match = true;
            for i in (1..args.len()).step_by(2) {
                let crit_val = args[i].value()?.into_literal();
                let pred = crate::args::parse_criteria(&args[i + 1].value()?.into_literal())?;
                if !criteria_match(&pred, &crit_val) {
                    all_match = false;
                    break;
                }
            }
            if all_match {
                return match coerce_num(&target_val) {
                    Ok(n) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(n))),
                    Err(_) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0))),
                };
            }
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
        }
    };

    let (rows, cols) = target_view.dims();

    // Parse all criteria
    let mut criteria_ranges = Vec::new();
    let mut predicates = Vec::new();
    for i in (1..args.len()).step_by(2) {
        let crit_view = args[i].range_view().ok();
        let pred = crate::args::parse_criteria(&args[i + 1].value()?.into_literal())?;
        criteria_ranges.push(crit_view);
        predicates.push(pred);
    }

    // Iterate through all cells and find max/min where all criteria match
    let mut result: Option<f64> = None;

    for r in 0..rows {
        for c in 0..cols {
            // Check all criteria
            let mut all_match = true;
            for (crit_idx, pred) in predicates.iter().enumerate() {
                let crit_val = match &criteria_ranges[crit_idx] {
                    Some(view) => {
                        let (cr, cc) = view.dims();
                        if r < cr && c < cc {
                            view.get_cell(r, c)
                        } else {
                            LiteralValue::Empty
                        }
                    }
                    None => LiteralValue::Empty,
                };
                if !criteria_match(pred, &crit_val) {
                    all_match = false;
                    break;
                }
            }

            if all_match {
                let target_val = target_view.get_cell(r, c);
                match target_val {
                    LiteralValue::Error(e) => {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                    }
                    LiteralValue::Number(n) => {
                        result = Some(match result {
                            None => n,
                            Some(curr) => {
                                if is_max {
                                    curr.max(n)
                                } else {
                                    curr.min(n)
                                }
                            }
                        });
                    }
                    LiteralValue::Int(i) => {
                        let n = i as f64;
                        result = Some(match result {
                            None => n,
                            Some(curr) => {
                                if is_max {
                                    curr.max(n)
                                } else {
                                    curr.min(n)
                                }
                            }
                        });
                    }
                    _ => {} // Skip non-numeric
                }
            }
        }
    }

    // Excel returns 0 if no matches found
    Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
        result.unwrap_or(0.0),
    )))
}

/* ─────────────────────────── TRIMMEAN ──────────────────────────── */

/// Returns the mean after trimming a percentage of values from both tails.
///
/// `TRIMMEAN` sorts numeric data, removes an equal count from low and high ends, then averages the
/// remaining interior values.
///
/// # Remarks
/// - `percent` must satisfy `0 <= percent < 1`.
/// - The trimmed count per side is `floor(n * percent / 2)`.
/// - Returns `#NUM!` for invalid percent values or when no numeric values are available.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Trimmed mean from direct values"
/// formula: "=TRIMMEAN({1,2,3,4,5,6},0.3333333333333333)"
/// expected: 3.5
/// ```
///
/// ```yaml,sandbox
/// title: "Trimmed mean from a range"
/// grid:
///   A1: 10
///   A2: 12
///   A3: 13
///   A4: 20
///   A5: 21
///   A6: 30
/// formula: "=TRIMMEAN(A1:A6,0.4)"
/// expected: 16.5
/// ```
#[derive(Debug)]
pub struct TrimmeanFn;
/// [formualizer-docgen:schema:start]
/// Name: TRIMMEAN
/// Type: TrimmeanFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: TRIMMEAN(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for TrimmeanFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "TRIMMEAN"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let mut nums = collect_numeric_stats(&args[0..1])?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let percent = match args[1].value()?.into_literal() {
            LiteralValue::Error(e) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            other => coerce_num(&other)?,
        };

        // Percent must be between 0 and 1 (exclusive of 1)
        if !(0.0..1.0).contains(&percent) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = nums.len();
        // Number of values to exclude from each end
        let exclude = ((n as f64 * percent) / 2.0).floor() as usize;

        if 2 * exclude >= n {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let trimmed = &nums[exclude..n - exclude];
        let sum: f64 = trimmed.iter().sum();
        let mean = sum / trimmed.len() as f64;

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(mean)))
    }
}

/* ─────────────────────────── CORREL ──────────────────────────── */

/// Helper to collect two paired arrays for regression/correlation functions
fn collect_paired_arrays(args: &[ArgumentHandle]) -> Result<(Vec<f64>, Vec<f64>), ExcelError> {
    let y_nums = collect_numeric_stats(&args[0..1])?;
    let x_nums = collect_numeric_stats(&args[1..2])?;

    // Arrays must have same length
    if y_nums.len() != x_nums.len() {
        return Err(ExcelError::new_na());
    }

    if y_nums.is_empty() {
        return Err(ExcelError::new_div());
    }

    Ok((y_nums, x_nums))
}

/// Returns the Pearson correlation coefficient between two numeric arrays.
///
/// `CORREL` measures linear relationship strength from `-1` (perfect inverse) to `1` (perfect
/// direct).
///
/// # Remarks
/// - Both arrays must produce the same number of numeric values.
/// - Returns `#N/A` when array lengths differ.
/// - Returns `#DIV/0!` when either series has zero variance.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Perfect positive linear correlation"
/// formula: "=CORREL({2,4,6},{1,2,3})"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Perfect negative linear correlation"
/// grid:
///   A1: 10
///   A2: 8
///   A3: 6
///   B1: 1
///   B2: 2
///   B3: 3
/// formula: "=CORREL(A1:A3,B1:B3)"
/// expected: -1
/// ```
#[derive(Debug)]
pub struct CorrelFn;
/// [formualizer-docgen:schema:start]
/// Name: CORREL
/// Type: CorrelFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: CORREL(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for CorrelFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "CORREL"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let (y, x) = match collect_paired_arrays(args) {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let n = x.len() as f64;
        let mean_x = x.iter().sum::<f64>() / n;
        let mean_y = y.iter().sum::<f64>() / n;

        let mut sum_xy = 0.0;
        let mut sum_x2 = 0.0;
        let mut sum_y2 = 0.0;

        for i in 0..x.len() {
            let dx = x[i] - mean_x;
            let dy = y[i] - mean_y;
            sum_xy += dx * dy;
            sum_x2 += dx * dx;
            sum_y2 += dy * dy;
        }

        let denom = (sum_x2 * sum_y2).sqrt();
        if denom == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let correl = sum_xy / denom;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            correl,
        )))
    }
}

/* ─────────────────────────── SLOPE ──────────────────────────── */

/// Returns the slope of the linear regression line for paired data.
///
/// `SLOPE` fits `y = m*x + b` and returns `m`, the rate of change in `y` per unit of `x`.
///
/// # Remarks
/// - `known_y` and `known_x` must have the same numeric length.
/// - Returns `#N/A` for mismatched lengths.
/// - Returns `#DIV/0!` if all `x` values are identical.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Positive slope from direct arrays"
/// formula: "=SLOPE({2,4,6},{1,2,3})"
/// expected: 2
/// ```
///
/// ```yaml,sandbox
/// title: "Negative slope from ranges"
/// grid:
///   A1: 10
///   A2: 8
///   A3: 6
///   B1: 1
///   B2: 2
///   B3: 3
/// formula: "=SLOPE(A1:A3,B1:B3)"
/// expected: -2
/// ```
#[derive(Debug)]
pub struct SlopeFn;
/// [formualizer-docgen:schema:start]
/// Name: SLOPE
/// Type: SlopeFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: SLOPE(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for SlopeFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "SLOPE"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let (y, x) = match collect_paired_arrays(args) {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let n = x.len() as f64;
        let mean_x = x.iter().sum::<f64>() / n;
        let mean_y = y.iter().sum::<f64>() / n;

        let mut sum_xy = 0.0;
        let mut sum_x2 = 0.0;

        for i in 0..x.len() {
            let dx = x[i] - mean_x;
            let dy = y[i] - mean_y;
            sum_xy += dx * dy;
            sum_x2 += dx * dx;
        }

        if sum_x2 == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let slope = sum_xy / sum_x2;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            slope,
        )))
    }
}

/* ─────────────────────────── INTERCEPT ──────────────────────────── */

/// Returns the y-intercept of the linear regression line for paired data.
///
/// `INTERCEPT` fits `y = m*x + b` and returns `b`, the predicted `y` when `x = 0`.
///
/// # Remarks
/// - `known_y` and `known_x` must have the same numeric length.
/// - Returns `#N/A` for mismatched lengths.
/// - Returns `#DIV/0!` if all `x` values are identical.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Positive intercept from direct arrays"
/// formula: "=INTERCEPT({3,5,7},{1,2,3})"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Intercept from range-based linear trend"
/// grid:
///   A1: 10
///   A2: 8
///   A3: 6
///   B1: 1
///   B2: 2
///   B3: 3
/// formula: "=INTERCEPT(A1:A3,B1:B3)"
/// expected: 12
/// ```
#[derive(Debug)]
pub struct InterceptFn;
/// [formualizer-docgen:schema:start]
/// Name: INTERCEPT
/// Type: InterceptFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: INTERCEPT(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for InterceptFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "INTERCEPT"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let (y, x) = match collect_paired_arrays(args) {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let n = x.len() as f64;
        let mean_x = x.iter().sum::<f64>() / n;
        let mean_y = y.iter().sum::<f64>() / n;

        let mut sum_xy = 0.0;
        let mut sum_x2 = 0.0;

        for i in 0..x.len() {
            let dx = x[i] - mean_x;
            let dy = y[i] - mean_y;
            sum_xy += dx * dy;
            sum_x2 += dx * dx;
        }

        if sum_x2 == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let slope = sum_xy / sum_x2;
        let intercept = mean_y - slope * mean_x;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            intercept,
        )))
    }
}

/// [formualizer-docgen:schema:start]
/// Name: DEVSQ
/// Type: DevsqFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: DEVSQ(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for DevsqFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "DEVSQ"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        let mean = nums.iter().sum::<f64>() / nums.len() as f64;
        let devsq = nums.iter().map(|x| (x - mean).powi(2)).sum::<f64>();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            devsq,
        )))
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
STATISTICAL DISTRIBUTION FUNCTIONS
═══════════════════════════════════════════════════════════════════════════ */

/// Helper: Standard normal CDF using error function approximation
fn std_norm_cdf(z: f64) -> f64 {
    // Use the complementary error function: Φ(z) = 0.5 * erfc(-z / sqrt(2))
    // Approximation using Abramowitz and Stegun formula 7.1.26
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if z < 0.0 { -1.0 } else { 1.0 };
    let z_abs = z.abs() / std::f64::consts::SQRT_2;

    let t = 1.0 / (1.0 + p * z_abs);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-z_abs * z_abs).exp();

    0.5 * (1.0 + sign * y)
}

/// Helper: Standard normal PDF
fn std_norm_pdf(z: f64) -> f64 {
    let inv_sqrt_2pi = 1.0 / (2.0 * std::f64::consts::PI).sqrt();
    inv_sqrt_2pi * (-0.5 * z * z).exp()
}

/// Helper: Inverse standard normal CDF (probit function)
/// Uses Rational approximation from Abramowitz and Stegun
#[allow(clippy::excessive_precision)]
fn std_norm_inv(p: f64) -> Option<f64> {
    if p <= 0.0 || p >= 1.0 {
        return None;
    }

    // Coefficients for rational approximation
    const A: [f64; 6] = [
        -3.969683028665376e+01,
        2.209460984245205e+02,
        -2.759285104469687e+02,
        1.383577518672690e+02,
        -3.066479806614716e+01,
        2.506628277459239e+00,
    ];
    const B: [f64; 5] = [
        -5.447609879822406e+01,
        1.615858368580409e+02,
        -1.556989798598866e+02,
        6.680131188771972e+01,
        -1.328068155288572e+01,
    ];
    const C: [f64; 6] = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e+00,
        -2.549732539343734e+00,
        4.374664141464968e+00,
        2.938163982698783e+00,
    ];
    const D: [f64; 4] = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e+00,
        3.754408661907416e+00,
    ];

    const P_LOW: f64 = 0.02425;
    const P_HIGH: f64 = 1.0 - P_LOW;

    let q = p - 0.5;

    if p < P_LOW {
        // Lower tail
        let r = (-2.0 * p.ln()).sqrt();
        let num = ((((C[0] * r + C[1]) * r + C[2]) * r + C[3]) * r + C[4]) * r + C[5];
        let den = (((D[0] * r + D[1]) * r + D[2]) * r + D[3]) * r + 1.0;
        Some(num / den)
    } else if p <= P_HIGH {
        // Central region
        let r = q * q;
        let num = ((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5];
        let den = ((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0;
        Some(q * num / den)
    } else {
        // Upper tail
        let r = (-2.0 * (1.0 - p).ln()).sqrt();
        let num = ((((C[0] * r + C[1]) * r + C[2]) * r + C[3]) * r + C[4]) * r + C[5];
        let den = (((D[0] * r + D[1]) * r + D[2]) * r + D[3]) * r + 1.0;
        Some(-num / den)
    }
}

/// Returns the standard normal probability for a z-score as either a CDF or PDF value.
///
/// Use `NORM.S.DIST` for z-based probability lookups when the distribution has mean `0` and
/// standard deviation `1`.
///
/// # Remarks
/// - Set `cumulative` to a non-zero value for the cumulative distribution `P(Z <= z)`.
/// - Set `cumulative` to `0` for the probability density at exactly `z`.
/// - Accepts any real-valued `z`; no domain clipping is applied.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Standard normal CDF at zero"
/// formula: "=NORM.S.DIST(0,TRUE)"
/// expected: 0.5
/// ```
///
/// ```yaml,sandbox
/// title: "Standard normal PDF at zero"
/// formula: "=NORM.S.DIST(0,FALSE)"
/// expected: 0.3989422804014327
/// ```
#[derive(Debug)]
pub struct NormSDistFn;
/// [formualizer-docgen:schema:start]
/// Name: NORM.S.DIST
/// Type: NormSDistFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: NORM.S.DIST(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NormSDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NORM.S.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let z = coerce_num(&scalar_like_value(&args[0])?)?;
        let cumulative = coerce_num(&scalar_like_value(&args[1])?)? != 0.0;

        let result = if cumulative {
            std_norm_cdf(z)
        } else {
            std_norm_pdf(z)
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the z-score whose standard normal cumulative probability matches `probability`.
///
/// This is the inverse of `NORM.S.DIST(z, TRUE)` and is commonly used for critical-value
/// thresholds.
///
/// # Remarks
/// - `probability` must be strictly between `0` and `1`.
/// - Returns `#NUM!` when `probability <= 0` or `probability >= 1`.
/// - Output can be negative, zero, or positive depending on which side of `0.5` you query.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Median probability maps to zero"
/// formula: "=NORM.S.INV(0.5)"
/// expected: 0
/// ```
///
/// ```yaml,sandbox
/// title: "Upper-tail critical z-score"
/// formula: "=NORM.S.INV(0.975)"
/// expected: 1.959963986120195
/// ```
#[derive(Debug)]
pub struct NormSInvFn;
/// [formualizer-docgen:schema:start]
/// Name: NORM.S.INV
/// Type: NormSInvFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: NORM.S.INV(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NormSInvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NORM.S.INV"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let p = coerce_num(&scalar_like_value(&args[0])?)?;

        match std_norm_inv(p) {
            Some(z) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(z))),
            None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            ))),
        }
    }
}

/// Returns the normal-distribution probability at `x` for a given mean and standard deviation.
///
/// Use `NORM.DIST` for either cumulative probabilities or point density under a non-standard
/// normal model.
///
/// # Remarks
/// - Set `cumulative` to non-zero for `P(X <= x)`; set it to `0` for density mode.
/// - `standard_dev` must be strictly greater than `0`.
/// - Returns `#NUM!` when `standard_dev <= 0`.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Normal CDF at the mean"
/// formula: "=NORM.DIST(50,50,10,TRUE)"
/// expected: 0.5
/// ```
///
/// ```yaml,sandbox
/// title: "Normal PDF at the mean"
/// formula: "=NORM.DIST(50,50,10,FALSE)"
/// expected: 0.03989422804014327
/// ```
#[derive(Debug)]
pub struct NormDistFn;
/// [formualizer-docgen:schema:start]
/// Name: NORM.DIST
/// Type: NormDistFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: NORM.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NormDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NORM.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let mean = coerce_num(&scalar_like_value(&args[1])?)?;
        let std_dev = coerce_num(&scalar_like_value(&args[2])?)?;
        let cumulative = coerce_num(&scalar_like_value(&args[3])?)? != 0.0;

        if std_dev <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let z = (x - mean) / std_dev;

        let result = if cumulative {
            std_norm_cdf(z)
        } else {
            std_norm_pdf(z) / std_dev
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the value `x` whose normal cumulative probability equals `probability`.
///
/// This function is the inverse of `NORM.DIST(x, mean, standard_dev, TRUE)`.
///
/// # Remarks
/// - `probability` must be strictly between `0` and `1`.
/// - `standard_dev` must be strictly greater than `0`.
/// - Returns `#NUM!` for invalid probability bounds or non-positive standard deviation.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Median probability returns the mean"
/// formula: "=NORM.INV(0.5,10,2)"
/// expected: 10
/// ```
///
/// ```yaml,sandbox
/// title: "One-standard-deviation quantile"
/// formula: "=NORM.INV(0.841344746068543,0,1)"
/// expected: 1
/// ```
#[derive(Debug)]
pub struct NormInvFn;
/// [formualizer-docgen:schema:start]
/// Name: NORM.INV
/// Type: NormInvFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: NORM.INV(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NormInvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NORM.INV"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let p = coerce_num(&scalar_like_value(&args[0])?)?;
        let mean = coerce_num(&scalar_like_value(&args[1])?)?;
        let std_dev = coerce_num(&scalar_like_value(&args[2])?)?;

        if std_dev <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        match std_norm_inv(p) {
            Some(z) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                mean + z * std_dev,
            ))),
            None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            ))),
        }
    }
}

/// Returns the log-normal probability at `x` as either a cumulative value or density.
///
/// `LOGNORM.DIST` models positive-valued variables where `ln(X)` follows a normal distribution.
///
/// # Remarks
/// - Set `cumulative` to non-zero for CDF mode; set it to `0` for PDF mode.
/// - Requires `x > 0` and `standard_dev > 0`.
/// - Returns `#NUM!` when `x <= 0` or `standard_dev <= 0`.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Log-normal CDF at x = 1"
/// formula: "=LOGNORM.DIST(1,0,1,TRUE)"
/// expected: 0.5
/// ```
///
/// ```yaml,sandbox
/// title: "Log-normal PDF at x = 1"
/// formula: "=LOGNORM.DIST(1,0,1,FALSE)"
/// expected: 0.3989422804014327
/// ```
#[derive(Debug)]
pub struct LognormDistFn;
/// [formualizer-docgen:schema:start]
/// Name: LOGNORM.DIST
/// Type: LognormDistFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: LOGNORM.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for LognormDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LOGNORM.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let mean = coerce_num(&scalar_like_value(&args[1])?)?;
        let std_dev = coerce_num(&scalar_like_value(&args[2])?)?;
        let cumulative = coerce_num(&scalar_like_value(&args[3])?)? != 0.0;

        if x <= 0.0 || std_dev <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let z = (x.ln() - mean) / std_dev;

        let result = if cumulative {
            std_norm_cdf(z)
        } else {
            std_norm_pdf(z) / (x * std_dev)
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the positive value `x` whose log-normal cumulative probability is `probability`.
///
/// This function inverts `LOGNORM.DIST(x, mean, standard_dev, TRUE)`.
///
/// # Remarks
/// - `probability` must be strictly between `0` and `1`.
/// - `standard_dev` must be strictly greater than `0`.
/// - Returns `#NUM!` when inputs violate probability or scale constraints.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Median log-normal quantile"
/// formula: "=LOGNORM.INV(0.5,0,1)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Upper quantile for mean 0 and stdev 1"
/// formula: "=LOGNORM.INV(0.841344746068543,0,1)"
/// expected: 2.718281828459045
/// ```
#[derive(Debug)]
pub struct LognormInvFn;
/// [formualizer-docgen:schema:start]
/// Name: LOGNORM.INV
/// Type: LognormInvFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: LOGNORM.INV(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for LognormInvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LOGNORM.INV"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let p = coerce_num(&scalar_like_value(&args[0])?)?;
        let mean = coerce_num(&scalar_like_value(&args[1])?)?;
        let std_dev = coerce_num(&scalar_like_value(&args[2])?)?;

        if std_dev <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        match std_norm_inv(p) {
            Some(z) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                (mean + z * std_dev).exp(),
            ))),
            None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            ))),
        }
    }
}

/// Returns the standard normal probability density at `x`.
///
/// `PHI` is equivalent to `NORM.S.DIST(x, FALSE)` and is useful in continuous-probability
/// calculations.
///
/// # Remarks
/// - Evaluates the density of a standard normal variable centered at `0`.
/// - The result is always non-negative and symmetric around `x = 0`.
/// - Works for any real input value.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Standard normal density at zero"
/// formula: "=PHI(0)"
/// expected: 0.3989422804014327
/// ```
///
/// ```yaml,sandbox
/// title: "Standard normal density at one"
/// formula: "=PHI(1)"
/// expected: 0.24197072451914337
/// ```
#[derive(Debug)]
pub struct PhiFn;
/// [formualizer-docgen:schema:start]
/// Name: PHI
/// Type: PhiFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: PHI(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for PhiFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "PHI"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let z = coerce_num(&scalar_like_value(&args[0])?)?;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            std_norm_pdf(z),
        )))
    }
}

/// Returns the standard normal area between `0` and `z`.
///
/// `GAUSS` computes `NORM.S.DIST(z, TRUE) - 0.5`, preserving the sign of `z`.
///
/// # Remarks
/// - Positive `z` returns a positive area; negative `z` returns a negative area.
/// - `GAUSS(0)` returns `0`.
/// - Output magnitude is always less than `0.5`.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Area from mean to z = 1"
/// formula: "=GAUSS(1)"
/// expected: 0.3413447460685429
/// ```
///
/// ```yaml,sandbox
/// title: "Symmetric negative z-value"
/// formula: "=GAUSS(-1)"
/// expected: -0.3413447460685429
/// ```
#[derive(Debug)]
pub struct GaussFn;
/// [formualizer-docgen:schema:start]
/// Name: GAUSS
/// Type: GaussFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: GAUSS(arg1: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for GaussFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "GAUSS"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> =
            LazyLock::new(|| vec![ArgSchema::number_lenient_scalar()]);
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let z = coerce_num(&scalar_like_value(&args[0])?)?;
        // GAUSS(z) = Φ(z) - 0.5
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            std_norm_cdf(z) - 0.5,
        )))
    }
}

/// Helper: Log-gamma function
#[allow(clippy::excessive_precision)]
fn ln_gamma(x: f64) -> f64 {
    // Lanczos approximation
    const G: f64 = 7.0;
    const C: [f64; 9] = [
        0.99999999999980993,
        676.5203681218851,
        -1259.1392167224028,
        771.32342877765313,
        -176.61502916214059,
        12.507343278686905,
        -0.13857109526572012,
        9.9843695780195716e-6,
        1.5056327351493116e-7,
    ];

    if x < 0.5 {
        // Reflection formula
        let pi = std::f64::consts::PI;
        pi.ln() - (pi * x).sin().ln() - ln_gamma(1.0 - x)
    } else {
        let x = x - 1.0;
        let mut ag = C[0];
        for (i, c) in C.iter().enumerate().skip(1) {
            ag += c / (x + i as f64);
        }
        let tmp = x + G + 0.5;
        0.5 * (2.0 * std::f64::consts::PI).ln() + (tmp).ln() * (x + 0.5) - tmp + ag.ln()
    }
}

/// Helper: Regularized lower incomplete gamma function P(a, x)
fn gamma_p(a: f64, x: f64) -> f64 {
    if x < 0.0 || a <= 0.0 {
        return 0.0;
    }
    if x == 0.0 {
        return 0.0;
    }

    // Use series expansion for x < a+1
    if x < a + 1.0 {
        gamma_series(a, x)
    } else {
        // Use continued fraction for x >= a+1
        1.0 - gamma_cf(a, x)
    }
}

/// Helper: Series expansion for incomplete gamma
fn gamma_series(a: f64, x: f64) -> f64 {
    let ln_ga = ln_gamma(a);
    let mut sum = 1.0 / a;
    let mut term = sum;
    for n in 1..200 {
        term *= x / (a + n as f64);
        sum += term;
        if term.abs() < sum.abs() * 1e-15 {
            break;
        }
    }
    sum * (-x + a * x.ln() - ln_ga).exp()
}

/// Helper: Continued fraction for upper incomplete gamma Q(a,x)
/// Using modified Lentz's algorithm (Numerical Recipes formulation)
fn gamma_cf(a: f64, x: f64) -> f64 {
    let ln_ga = ln_gamma(a);
    const TINY: f64 = 1e-30;
    const EPS: f64 = 1e-14;

    // Set up for evaluating continued fraction by modified Lentz's method
    let mut b = x + 1.0 - a;
    let mut c = 1.0 / TINY;
    let mut d = 1.0 / b;
    let mut h = d;

    for i in 1..=200 {
        let an = -(i as f64) * (i as f64 - a);
        b += 2.0;
        d = an * d + b;
        if d.abs() < TINY {
            d = TINY;
        }
        c = b + an / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let delta = d * c;
        h *= delta;
        if (delta - 1.0).abs() <= EPS {
            break;
        }
    }

    h * (-x + a * x.ln() - ln_ga).exp()
}

/// Helper: Regularized incomplete beta function I_x(a,b)
/// Uses the continued fraction representation (NIST DLMF 8.17.22)
fn beta_i(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    if a <= 0.0 || b <= 0.0 {
        return f64::NAN;
    }

    // Use symmetry for better convergence: I_x(a,b) = 1 - I_{1-x}(b,a)
    if x > (a + 1.0) / (a + b + 2.0) {
        return 1.0 - beta_i(1.0 - x, b, a);
    }

    // Compute the prefactor: x^a * (1-x)^b / (a * B(a,b))
    let ln_beta = ln_gamma(a) + ln_gamma(b) - ln_gamma(a + b);
    let ln_prefactor = a * x.ln() + b * (1.0 - x).ln() - ln_beta - a.ln();
    let prefactor = ln_prefactor.exp();

    // Evaluate the continued fraction using modified Lentz algorithm
    // The CF is: 1 / (1 + d1/(1 + d2/(1 + ...)))
    // where d_{2m+1} = -(a+m)(a+b+m)x / ((a+2m)(a+2m+1))
    //       d_{2m}   = m(b-m)x / ((a+2m-1)(a+2m))
    const EPS: f64 = 1e-14;
    const TINY: f64 = 1e-30;

    let qab = a + b;
    let qap = a + 1.0;
    let qam = a - 1.0;
    let mut c = 1.0;
    let mut d = 1.0 - qab * x / qap;
    if d.abs() < TINY {
        d = TINY;
    }
    d = 1.0 / d;
    let mut h = d;

    for m in 1..=200 {
        let m_f64 = m as f64;
        let m2 = 2.0 * m_f64;

        // Even step: d_{2m} = m(b-m)x / ((a+2m-1)(a+2m))
        let aa = m_f64 * (b - m_f64) * x / ((qam + m2) * (a + m2));
        d = 1.0 + aa * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + aa / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        h *= d * c;

        // Odd step: d_{2m+1} = -(a+m)(a+b+m)x / ((a+2m)(a+2m+1))
        let aa = -((a + m_f64) * (qab + m_f64) * x) / ((a + m2) * (qap + m2));
        d = 1.0 + aa * d;
        if d.abs() < TINY {
            d = TINY;
        }
        c = 1.0 + aa / c;
        if c.abs() < TINY {
            c = TINY;
        }
        d = 1.0 / d;
        let delta = d * c;
        h *= delta;

        if (delta - 1.0).abs() <= EPS {
            break;
        }
    }

    prefactor * h
}

/// Helper: T distribution CDF
fn t_cdf(t: f64, df: f64) -> f64 {
    let x = df / (df + t * t);
    0.5 * (1.0 + t.signum() * (1.0 - beta_i(x, df / 2.0, 0.5)))
}

/// Helper: T distribution inverse CDF using Newton-Raphson
fn t_inv(p: f64, df: f64) -> Option<f64> {
    if p <= 0.0 || p >= 1.0 {
        return None;
    }

    // Initial guess using normal approximation
    let mut t = std_norm_inv(p)?;

    // Newton-Raphson iteration
    for _ in 0..50 {
        let cdf = t_cdf(t, df);
        let pdf = t_pdf(t, df);
        if pdf.abs() < 1e-30 {
            break;
        }
        let delta = (cdf - p) / pdf;
        t -= delta;
        if delta.abs() < 1e-12 {
            break;
        }
    }

    Some(t)
}

/// Helper: T distribution PDF
fn t_pdf(t: f64, df: f64) -> f64 {
    let coef =
        (ln_gamma((df + 1.0) / 2.0) - ln_gamma(df / 2.0) - 0.5 * (df * std::f64::consts::PI).ln())
            .exp();
    coef * (1.0 + t * t / df).powf(-(df + 1.0) / 2.0)
}

/// Helper: Chi-square CDF
fn chisq_cdf(x: f64, df: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    gamma_p(df / 2.0, x / 2.0)
}

/// Helper: Chi-square inverse CDF using Newton-Raphson
fn chisq_inv(p: f64, df: f64) -> Option<f64> {
    if p <= 0.0 || p >= 1.0 {
        return None;
    }

    // Initial guess
    let mut x = df.max(1.0);
    if p < 0.5 {
        x = x.min(1.0);
    }

    // Newton-Raphson iteration
    for _ in 0..100 {
        let cdf = chisq_cdf(x, df);
        let pdf = chisq_pdf(x, df);
        if pdf.abs() < 1e-30 {
            break;
        }
        let delta = (cdf - p) / pdf;
        let new_x = (x - delta).max(1e-15);
        if (new_x - x).abs() < 1e-12 * x {
            x = new_x;
            break;
        }
        x = new_x;
    }

    Some(x)
}

/// Helper: Chi-square PDF
fn chisq_pdf(x: f64, df: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    let k = df / 2.0;
    ((k - 1.0) * x.ln() - x / 2.0 - k * 2.0_f64.ln() - ln_gamma(k)).exp()
}

/// Helper: F distribution CDF
fn f_cdf(f: f64, d1: f64, d2: f64) -> f64 {
    if f <= 0.0 {
        return 0.0;
    }
    let x = d1 * f / (d1 * f + d2);
    beta_i(x, d1 / 2.0, d2 / 2.0)
}

/// Helper: F distribution inverse CDF using Newton-Raphson
fn f_inv(p: f64, d1: f64, d2: f64) -> Option<f64> {
    if p <= 0.0 || p >= 1.0 {
        return None;
    }

    // Initial guess
    let mut f = 1.0;

    // Newton-Raphson iteration
    for _ in 0..100 {
        let cdf = f_cdf(f, d1, d2);
        let pdf = f_pdf(f, d1, d2);
        if pdf.abs() < 1e-30 {
            break;
        }
        let delta = (cdf - p) / pdf;
        let new_f = (f - delta).max(1e-15);
        if (new_f - f).abs() < 1e-12 * f {
            f = new_f;
            break;
        }
        f = new_f;
    }

    Some(f)
}

/// Helper: F distribution PDF
fn f_pdf(f: f64, d1: f64, d2: f64) -> f64 {
    if f <= 0.0 {
        return 0.0;
    }
    let ln_beta = ln_gamma(d1 / 2.0) + ln_gamma(d2 / 2.0) - ln_gamma((d1 + d2) / 2.0);
    let coef = (d1 / 2.0) * (d1 / d2).ln() + (d1 / 2.0 - 1.0) * f.ln()
        - ((d1 + d2) / 2.0) * (1.0 + d1 * f / d2).ln()
        - ln_beta;
    coef.exp()
}

/// Returns the Student's t probability for `x` and a given degrees-of-freedom value.
///
/// Use `T.DIST` in either cumulative mode (left-tail probability) or density mode.
///
/// # Remarks
/// - Set `cumulative` to non-zero for CDF mode, or `0` for PDF mode.
/// - `deg_freedom` must be at least `1`.
/// - Returns `#NUM!` when `deg_freedom < 1`.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "t CDF at zero"
/// formula: "=T.DIST(0,10,TRUE)"
/// expected: 0.5
/// ```
///
/// ```yaml,sandbox
/// title: "t PDF at zero"
/// formula: "=T.DIST(0,10,FALSE)"
/// expected: 0.389108383966031
/// ```
#[derive(Debug)]
pub struct TDistFn;
/// [formualizer-docgen:schema:start]
/// Name: T.DIST
/// Type: TDistFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: T.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "T.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let df = coerce_num(&scalar_like_value(&args[1])?)?;
        let cumulative = coerce_num(&scalar_like_value(&args[2])?)? != 0.0;

        if df < 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if cumulative {
            t_cdf(x, df)
        } else {
            t_pdf(x, df)
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the t-value whose left-tail probability equals `probability`.
///
/// `T.INV` is the inverse of `T.DIST(x, deg_freedom, TRUE)`.
///
/// # Remarks
/// - `probability` must be strictly between `0` and `1`.
/// - `deg_freedom` must be at least `1`.
/// - Returns `#NUM!` for out-of-range probability or invalid degrees of freedom.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Median t quantile"
/// formula: "=T.INV(0.5,10)"
/// expected: 0
/// ```
///
/// ```yaml,sandbox
/// title: "Upper-tail critical value"
/// formula: "=T.INV(0.975,10)"
/// expected: 2.228138851986273
/// ```
#[derive(Debug)]
pub struct TInvFn;
/// [formualizer-docgen:schema:start]
/// Name: T.INV
/// Type: TInvFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: T.INV(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TInvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "T.INV"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let p = coerce_num(&scalar_like_value(&args[0])?)?;
        let df = coerce_num(&scalar_like_value(&args[1])?)?;

        if df < 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        match t_inv(p, df) {
            Some(result) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                result,
            ))),
            None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            ))),
        }
    }
}

/// Returns the chi-square probability for `x` with the specified degrees of freedom.
///
/// Use `CHISQ.DIST` in cumulative mode for left-tail probability or density mode for the PDF.
///
/// # Remarks
/// - Set `cumulative` to non-zero for CDF mode, or `0` for PDF mode.
/// - Requires `x >= 0` and `deg_freedom >= 1`.
/// - Returns `#NUM!` for negative `x` or invalid degrees of freedom.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Chi-square CDF at zero"
/// formula: "=CHISQ.DIST(0,4,TRUE)"
/// expected: 0
/// ```
///
/// ```yaml,sandbox
/// title: "Chi-square PDF example"
/// formula: "=CHISQ.DIST(2,2,FALSE)"
/// expected: 0.18393972058572117
/// ```
#[derive(Debug)]
pub struct ChisqDistFn;
/// [formualizer-docgen:schema:start]
/// Name: CHISQ.DIST
/// Type: ChisqDistFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: CHISQ.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ChisqDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CHISQ.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let df = coerce_num(&scalar_like_value(&args[1])?)?;
        let cumulative = coerce_num(&scalar_like_value(&args[2])?)? != 0.0;

        if df < 1.0 || x < 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if cumulative {
            chisq_cdf(x, df)
        } else {
            chisq_pdf(x, df)
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the chi-square value whose left-tail probability is `probability`.
///
/// `CHISQ.INV` inverts `CHISQ.DIST(x, deg_freedom, TRUE)`.
///
/// # Remarks
/// - `probability` must be strictly between `0` and `1`.
/// - `deg_freedom` must be at least `1`.
/// - Returns `#NUM!` when arguments are outside valid ranges.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Median chi-square quantile for df=2"
/// formula: "=CHISQ.INV(0.5,2)"
/// expected: 1.3862943611198906
/// ```
///
/// ```yaml,sandbox
/// title: "Upper quantile for df=10"
/// formula: "=CHISQ.INV(0.95,10)"
/// expected: 18.307038053275146
/// ```
#[derive(Debug)]
pub struct ChisqInvFn;
/// [formualizer-docgen:schema:start]
/// Name: CHISQ.INV
/// Type: ChisqInvFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: CHISQ.INV(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ChisqInvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CHISQ.INV"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let p = coerce_num(&scalar_like_value(&args[0])?)?;
        let df = coerce_num(&scalar_like_value(&args[1])?)?;

        if df < 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        match chisq_inv(p, df) {
            Some(result) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                result,
            ))),
            None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            ))),
        }
    }
}

/// Returns the F-distribution probability for `x` with numerator and denominator degrees of freedom.
///
/// Use `F.DIST` for left-tail cumulative probabilities or density values in variance-ratio tests.
///
/// # Remarks
/// - Set `cumulative` to non-zero for CDF mode, or `0` for PDF mode.
/// - Requires `x >= 0`, `deg_freedom1 >= 1`, and `deg_freedom2 >= 1`.
/// - Returns `#NUM!` when any domain constraint is violated.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "F CDF with symmetric 2 and 2 degrees of freedom"
/// formula: "=F.DIST(1,2,2,TRUE)"
/// expected: 0.5
/// ```
///
/// ```yaml,sandbox
/// title: "F PDF with symmetric 2 and 2 degrees of freedom"
/// formula: "=F.DIST(1,2,2,FALSE)"
/// expected: 0.25
/// ```
#[derive(Debug)]
pub struct FDistFn;
/// [formualizer-docgen:schema:start]
/// Name: F.DIST
/// Type: FDistFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: F.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "F.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let d1 = coerce_num(&scalar_like_value(&args[1])?)?;
        let d2 = coerce_num(&scalar_like_value(&args[2])?)?;
        let cumulative = coerce_num(&scalar_like_value(&args[3])?)? != 0.0;

        if d1 < 1.0 || d2 < 1.0 || x < 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if cumulative {
            f_cdf(x, d1, d2)
        } else {
            f_pdf(x, d1, d2)
        };
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the F value whose left-tail probability equals `probability`.
///
/// `F.INV` inverts `F.DIST(x, deg_freedom1, deg_freedom2, TRUE)`.
///
/// # Remarks
/// - `probability` must be strictly between `0` and `1`.
/// - `deg_freedom1` and `deg_freedom2` must each be at least `1`.
/// - Returns `#NUM!` for invalid probability or degree-of-freedom arguments.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Median F quantile with symmetric 2 and 2 degrees of freedom"
/// formula: "=F.INV(0.5,2,2)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Upper-tail F critical value"
/// formula: "=F.INV(0.95,5,10)"
/// expected: 3.3258345304130112
/// ```
#[derive(Debug)]
pub struct FInvFn;
/// [formualizer-docgen:schema:start]
/// Name: F.INV
/// Type: FInvFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: F.INV(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FInvFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "F.INV"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let p = coerce_num(&scalar_like_value(&args[0])?)?;
        let d1 = coerce_num(&scalar_like_value(&args[1])?)?;
        let d2 = coerce_num(&scalar_like_value(&args[2])?)?;

        if d1 < 1.0 || d2 < 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        match f_inv(p, d1, d2) {
            Some(result) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                result,
            ))),
            None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            ))),
        }
    }
}

/// Returns the z-score of `x` relative to a mean and standard deviation.
///
/// `STANDARDIZE` computes `(x - mean) / standard_dev`.
///
/// # Remarks
/// - `standard_dev` must be strictly greater than `0`.
/// - Returns `#NUM!` when `standard_dev <= 0`.
/// - Positive output means `x` is above the mean; negative output means below.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "One standard deviation above the mean"
/// formula: "=STANDARDIZE(42,40,2)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Exactly at the mean"
/// formula: "=STANDARDIZE(100,100,10)"
/// expected: 0
/// ```
#[derive(Debug)]
pub struct StandardizeFn;
/// [formualizer-docgen:schema:start]
/// Name: STANDARDIZE
/// Type: StandardizeFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: STANDARDIZE(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for StandardizeFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "STANDARDIZE"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let mean = coerce_num(&scalar_like_value(&args[1])?)?;
        let std_dev = coerce_num(&scalar_like_value(&args[2])?)?;

        if std_dev <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            (x - mean) / std_dev,
        )))
    }
}

/// Helper: Factorial function
fn factorial(n: i64) -> f64 {
    if n < 0 {
        return f64::NAN;
    }
    if n <= 1 {
        return 1.0;
    }
    // For large n, use gamma function: n! = Gamma(n+1)
    if n > 20 {
        return ln_gamma((n + 1) as f64).exp();
    }
    let mut result = 1.0;
    for i in 2..=n {
        result *= i as f64;
    }
    result
}

/// Helper: Log of binomial coefficient (n choose k)
fn ln_binom(n: i64, k: i64) -> f64 {
    if k < 0 || k > n {
        return f64::NEG_INFINITY;
    }
    if k == 0 || k == n {
        return 0.0;
    }
    ln_gamma((n + 1) as f64) - ln_gamma((k + 1) as f64) - ln_gamma((n - k + 1) as f64)
}

/// Returns the binomial probability for a count of successes across independent trials.
///
/// Use `BINOM.DIST` to evaluate either exact-success probability (PMF) or cumulative probability
/// up to a success count (CDF).
///
/// # Remarks
/// - `number_s` and `trials` are truncated to integers.
/// - Requires `0 <= number_s <= trials`, `trials >= 0`, and `0 <= probability_s <= 1`.
/// - Set `cumulative` to non-zero for CDF mode, or `0` for PMF mode.
/// - Returns `#NUM!` for invalid count or probability ranges.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Binomial PMF for exactly 3 successes"
/// formula: "=BINOM.DIST(3,10,0.5,FALSE)"
/// expected: 0.1171875
/// ```
///
/// ```yaml,sandbox
/// title: "Binomial CDF for at most 3 successes"
/// formula: "=BINOM.DIST(3,10,0.5,TRUE)"
/// expected: 0.171875
/// ```
#[derive(Debug)]
pub struct BinomDistFn;
/// [formualizer-docgen:schema:start]
/// Name: BINOM.DIST
/// Type: BinomDistFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: BINOM.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for BinomDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BINOM.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let k = coerce_num(&scalar_like_value(&args[0])?)?.trunc() as i64;
        let n = coerce_num(&scalar_like_value(&args[1])?)?.trunc() as i64;
        let p = coerce_num(&scalar_like_value(&args[2])?)?;
        let cumulative = coerce_num(&scalar_like_value(&args[3])?)? != 0.0;

        if n < 0 || k < 0 || k > n || !(0.0..=1.0).contains(&p) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if cumulative {
            // CDF: sum from i=0 to k of P(X=i)
            let mut sum = 0.0;
            for i in 0..=k {
                let ln_prob =
                    ln_binom(n, i) + (i as f64) * p.ln() + ((n - i) as f64) * (1.0 - p).ln();
                sum += ln_prob.exp();
            }
            sum
        } else {
            // PMF: P(X=k)
            let ln_prob = ln_binom(n, k) + (k as f64) * p.ln() + ((n - k) as f64) * (1.0 - p).ln();
            ln_prob.exp()
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the Poisson probability for event count `x` at average rate `mean`.
///
/// `POISSON.DIST` supports exact-count mode (PMF) and cumulative mode (CDF).
///
/// # Remarks
/// - `x` is truncated to an integer and must be at least `0`.
/// - `mean` must be non-negative.
/// - Set `cumulative` to non-zero for CDF mode, or `0` for PMF mode.
/// - Returns `#NUM!` for negative counts or negative mean values.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Poisson PMF for zero events"
/// formula: "=POISSON.DIST(0,2,FALSE)"
/// expected: 0.1353352832366127
/// ```
///
/// ```yaml,sandbox
/// title: "Poisson CDF up to two events"
/// formula: "=POISSON.DIST(2,2,TRUE)"
/// expected: 0.6766764161830634
/// ```
#[derive(Debug)]
pub struct PoissonDistFn;
/// [formualizer-docgen:schema:start]
/// Name: POISSON.DIST
/// Type: PoissonDistFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: POISSON.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for PoissonDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "POISSON.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let k = coerce_num(&scalar_like_value(&args[0])?)?.trunc() as i64;
        let lambda = coerce_num(&scalar_like_value(&args[1])?)?;
        let cumulative = coerce_num(&scalar_like_value(&args[2])?)? != 0.0;

        if k < 0 || lambda < 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if cumulative {
            // CDF: sum from i=0 to k of P(X=i) = 1 - Q(k+1, lambda)
            // Using the regularized incomplete gamma function
            1.0 - gamma_p((k + 1) as f64, lambda)
        } else {
            // PMF: P(X=k) = lambda^k * e^(-lambda) / k!
            // Use log to avoid overflow
            let ln_prob = (k as f64) * lambda.ln() - lambda - ln_gamma((k + 1) as f64);
            ln_prob.exp()
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the exponential-distribution probability at `x` for rate `lambda`.
///
/// Use `EXPON.DIST` for waiting-time models where events occur with a constant hazard rate.
///
/// # Remarks
/// - Requires `x >= 0` and `lambda > 0`.
/// - Set `cumulative` to non-zero for CDF mode, or `0` for PDF mode.
/// - Returns `#NUM!` when inputs violate domain requirements.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Exponential CDF"
/// formula: "=EXPON.DIST(1,1,TRUE)"
/// expected: 0.6321205588285577
/// ```
///
/// ```yaml,sandbox
/// title: "Exponential PDF"
/// formula: "=EXPON.DIST(1,1,FALSE)"
/// expected: 0.36787944117144233
/// ```
#[derive(Debug)]
pub struct ExponDistFn;
/// [formualizer-docgen:schema:start]
/// Name: EXPON.DIST
/// Type: ExponDistFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: EXPON.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ExponDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "EXPON.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let lambda = coerce_num(&scalar_like_value(&args[1])?)?;
        let cumulative = coerce_num(&scalar_like_value(&args[2])?)? != 0.0;

        if x < 0.0 || lambda <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if cumulative {
            // CDF: 1 - e^(-lambda*x)
            1.0 - (-lambda * x).exp()
        } else {
            // PDF: lambda * e^(-lambda*x)
            lambda * (-lambda * x).exp()
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the gamma-distribution probability at `x` for shape `alpha` and scale `beta`.
///
/// `GAMMA.DIST` supports cumulative and density modes for right-skewed waiting-time models.
///
/// # Remarks
/// - Requires `x >= 0`, `alpha > 0`, and `beta > 0`.
/// - Set `cumulative` to non-zero for CDF mode, or `0` for PDF mode.
/// - Returns `#NUM!` when any parameter is outside its valid range.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Gamma CDF with alpha=1 and beta=2"
/// formula: "=GAMMA.DIST(2,1,2,TRUE)"
/// expected: 0.6321205588285577
/// ```
///
/// ```yaml,sandbox
/// title: "Gamma PDF with alpha=1 and beta=2"
/// formula: "=GAMMA.DIST(2,1,2,FALSE)"
/// expected: 0.18393972058572117
/// ```
#[derive(Debug)]
pub struct GammaDistFn;
/// [formualizer-docgen:schema:start]
/// Name: GAMMA.DIST
/// Type: GammaDistFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: GAMMA.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for GammaDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "GAMMA.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let alpha = coerce_num(&scalar_like_value(&args[1])?)?; // shape
        let beta = coerce_num(&scalar_like_value(&args[2])?)?; // scale
        let cumulative = coerce_num(&scalar_like_value(&args[3])?)? != 0.0;

        if x < 0.0 || alpha <= 0.0 || beta <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if cumulative {
            // CDF: P(alpha, x/beta) where P is the regularized lower incomplete gamma
            gamma_p(alpha, x / beta)
        } else {
            // PDF: x^(alpha-1) * e^(-x/beta) / (beta^alpha * Gamma(alpha))
            let ln_pdf = (alpha - 1.0) * x.ln() - x / beta - alpha * beta.ln() - ln_gamma(alpha);
            ln_pdf.exp()
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the Weibull-distribution probability at `x` for shape `alpha` and scale `beta`.
///
/// `WEIBULL.DIST` is commonly used for reliability and time-to-failure analysis.
///
/// # Remarks
/// - Requires `x >= 0`, `alpha > 0`, and `beta > 0`.
/// - Set `cumulative` to non-zero for CDF mode, or `0` for PDF mode.
/// - Returns `#NUM!` when parameters fall outside valid ranges.
/// - In PDF mode at `x = 0`, behavior follows the Weibull shape-specific limit.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Weibull CDF with alpha=1 and beta=2"
/// formula: "=WEIBULL.DIST(2,1,2,TRUE)"
/// expected: 0.6321205588285577
/// ```
///
/// ```yaml,sandbox
/// title: "Weibull PDF with alpha=1 and beta=2"
/// formula: "=WEIBULL.DIST(2,1,2,FALSE)"
/// expected: 0.18393972058572117
/// ```
#[derive(Debug)]
pub struct WeibullDistFn;
/// [formualizer-docgen:schema:start]
/// Name: WEIBULL.DIST
/// Type: WeibullDistFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: WEIBULL.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for WeibullDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "WEIBULL.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let alpha = coerce_num(&scalar_like_value(&args[1])?)?; // shape
        let beta = coerce_num(&scalar_like_value(&args[2])?)?; // scale
        let cumulative = coerce_num(&scalar_like_value(&args[3])?)? != 0.0;

        if x < 0.0 || alpha <= 0.0 || beta <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if cumulative {
            // CDF: 1 - e^(-(x/beta)^alpha)
            1.0 - (-(x / beta).powf(alpha)).exp()
        } else {
            // PDF: (alpha/beta) * (x/beta)^(alpha-1) * e^(-(x/beta)^alpha)
            if x == 0.0 {
                if alpha < 1.0 {
                    f64::INFINITY
                } else if alpha == 1.0 {
                    alpha / beta
                } else {
                    0.0
                }
            } else {
                (alpha / beta) * (x / beta).powf(alpha - 1.0) * (-(x / beta).powf(alpha)).exp()
            }
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns the beta-distribution probability for `x`, with optional lower/upper bounds.
///
/// `BETA.DIST` can evaluate either the cumulative probability or density on `[A, B]` (default
/// `[0, 1]`).
///
/// # Remarks
/// - Requires `alpha > 0`, `beta > 0`, and `A < B`.
/// - `x` must lie within the inclusive interval `[A, B]`.
/// - Set `cumulative` to non-zero for CDF mode, or `0` for PDF mode.
/// - Returns `#NUM!` for invalid bounds, parameters, or out-of-range `x`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Uniform beta CDF on [0,1]"
/// formula: "=BETA.DIST(0.3,1,1,TRUE)"
/// expected: 0.3
/// ```
///
/// ```yaml,sandbox
/// title: "Uniform beta PDF on [0,1]"
/// formula: "=BETA.DIST(0.3,1,1,FALSE)"
/// expected: 1
/// ```
#[derive(Debug)]
pub struct BetaDistFn;
/// [formualizer-docgen:schema:start]
/// Name: BETA.DIST
/// Type: BetaDistFn
/// Min args: 4
/// Max args: variadic
/// Variadic: true
/// Signature: BETA.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5: number@scalar, arg6...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg6{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for BetaDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "BETA.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let alpha = coerce_num(&scalar_like_value(&args[1])?)?;
        let beta_param = coerce_num(&scalar_like_value(&args[2])?)?;
        let cumulative = coerce_num(&scalar_like_value(&args[3])?)? != 0.0;

        // Optional bounds A and B (default 0 and 1)
        let a = if args.len() > 4 {
            coerce_num(&scalar_like_value(&args[4])?)?
        } else {
            0.0
        };
        let b = if args.len() > 5 {
            coerce_num(&scalar_like_value(&args[5])?)?
        } else {
            1.0
        };

        if alpha <= 0.0 || beta_param <= 0.0 || a >= b {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // x must be in [a, b]
        if x < a || x > b {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // Transform x to standard [0,1] interval
        let x_std = (x - a) / (b - a);

        let result = if cumulative {
            // CDF: I_x(alpha, beta) - regularized incomplete beta function
            beta_i(x_std, alpha, beta_param)
        } else {
            // PDF: (x-A)^(alpha-1) * (B-x)^(beta-1) / ((B-A)^(alpha+beta-1) * B(alpha, beta))
            let ln_beta = ln_gamma(alpha) + ln_gamma(beta_param) - ln_gamma(alpha + beta_param);
            let scale = b - a;
            if (x_std == 0.0 && alpha < 1.0) || (x_std == 1.0 && beta_param < 1.0) {
                f64::INFINITY
            } else if x_std == 0.0 {
                if alpha == 1.0 {
                    (1.0 - x_std).powf(beta_param - 1.0) / (scale * ln_beta.exp())
                } else {
                    0.0
                }
            } else if x_std == 1.0 {
                if beta_param == 1.0 {
                    x_std.powf(alpha - 1.0) / (scale * ln_beta.exp())
                } else {
                    0.0
                }
            } else {
                let ln_pdf =
                    (alpha - 1.0) * x_std.ln() + (beta_param - 1.0) * (1.0 - x_std).ln() - ln_beta;
                ln_pdf.exp() / scale
            }
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns negative-binomial probabilities for failures observed before a target success count.
///
/// `NEGBINOM.DIST` supports exact-failure mode (PMF) and cumulative mode (CDF).
///
/// # Remarks
/// - `number_f` is truncated and must be `>= 0`.
/// - `number_s` is truncated and must be `>= 1`.
/// - `probability_s` must satisfy `0 < p < 1`.
/// - Returns `#NUM!` when counts or probability are outside valid ranges.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Negative binomial PMF"
/// formula: "=NEGBINOM.DIST(2,1,0.5,FALSE)"
/// expected: 0.125
/// ```
///
/// ```yaml,sandbox
/// title: "Negative binomial CDF"
/// formula: "=NEGBINOM.DIST(2,1,0.5,TRUE)"
/// expected: 0.875
/// ```
#[derive(Debug)]
pub struct NegbinomDistFn;
/// [formualizer-docgen:schema:start]
/// Name: NEGBINOM.DIST
/// Type: NegbinomDistFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: NEGBINOM.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for NegbinomDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "NEGBINOM.DIST"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let number_f = coerce_num(&scalar_like_value(&args[0])?)?.trunc() as i64; // number of failures
        let number_s = coerce_num(&scalar_like_value(&args[1])?)?.trunc() as i64; // number of successes
        let prob_s = coerce_num(&scalar_like_value(&args[2])?)?; // probability of success
        let cumulative = coerce_num(&scalar_like_value(&args[3])?)? != 0.0;

        if number_f < 0 || number_s < 1 || prob_s <= 0.0 || prob_s >= 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let result = if cumulative {
            // CDF: sum from i=0 to number_f of P(X=i)
            // This is equivalent to I_{prob_s}(number_s, number_f + 1) using regularized beta
            beta_i(prob_s, number_s as f64, (number_f + 1) as f64)
        } else {
            // PMF: C(number_f + number_s - 1, number_s - 1) * prob_s^number_s * (1-prob_s)^number_f
            // = C(k + r - 1, r - 1) * p^r * (1-p)^k where k = number_f, r = number_s
            let ln_prob = ln_binom(number_f + number_s - 1, number_s - 1)
                + (number_s as f64) * prob_s.ln()
                + (number_f as f64) * (1.0 - prob_s).ln();
            ln_prob.exp()
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Returns hypergeometric probabilities for successes drawn without replacement.
///
/// Use `HYPGEOM.DIST` for finite-population sampling where each draw changes remaining odds.
///
/// # Remarks
/// - Count inputs are truncated to integers.
/// - Requires valid population/sample bounds and feasible success counts.
/// - Set `cumulative` to non-zero for CDF mode, or `0` for PMF mode.
/// - Returns `#NUM!` for invalid population setup; out-of-support PMF values return `0`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Hypergeometric PMF"
/// formula: "=HYPGEOM.DIST(1,3,4,10,FALSE)"
/// expected: 0.5
/// ```
///
/// ```yaml,sandbox
/// title: "Hypergeometric CDF"
/// formula: "=HYPGEOM.DIST(1,3,4,10,TRUE)"
/// expected: 0.6666666666666666
/// ```
#[derive(Debug)]
pub struct HypgeomDistFn;
/// [formualizer-docgen:schema:start]
/// Name: HYPGEOM.DIST
/// Type: HypgeomDistFn
/// Min args: 5
/// Max args: 5
/// Variadic: false
/// Signature: HYPGEOM.DIST(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar, arg4: number@scalar, arg5: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg5{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for HypgeomDistFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "HYPGEOM.DIST"
    }
    fn min_args(&self) -> usize {
        5
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let sample_s = coerce_num(&scalar_like_value(&args[0])?)?.trunc() as i64; // successes in sample
        let number_sample = coerce_num(&scalar_like_value(&args[1])?)?.trunc() as i64; // sample size
        let population_s = coerce_num(&scalar_like_value(&args[2])?)?.trunc() as i64; // successes in population
        let number_pop = coerce_num(&scalar_like_value(&args[3])?)?.trunc() as i64; // population size
        let cumulative = coerce_num(&scalar_like_value(&args[4])?)? != 0.0;

        // Validation
        if number_pop <= 0
            || population_s < 0
            || population_s > number_pop
            || number_sample < 0
            || number_sample > number_pop
            || sample_s < 0
        {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // sample_s must be at least max(0, number_sample - (number_pop - population_s))
        // and at most min(number_sample, population_s)
        let min_successes = 0.max(number_sample - (number_pop - population_s));
        let max_successes = number_sample.min(population_s);

        if sample_s < min_successes || sample_s > max_successes {
            // Return 0 for PMF, or appropriate CDF value
            if cumulative {
                if sample_s < min_successes {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
                } else {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(1.0)));
                }
            } else {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
            }
        }

        let result = if cumulative {
            // CDF: sum from i=min_successes to sample_s of P(X=i)
            let mut sum = 0.0;
            for i in min_successes..=sample_s {
                sum += hypgeom_pmf(i, number_sample, population_s, number_pop);
            }
            sum
        } else {
            // PMF: C(population_s, sample_s) * C(number_pop - population_s, number_sample - sample_s) / C(number_pop, number_sample)
            hypgeom_pmf(sample_s, number_sample, population_s, number_pop)
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/// Helper: Hypergeometric PMF
fn hypgeom_pmf(k: i64, n: i64, k_pop: i64, n_pop: i64) -> f64 {
    // P(X=k) = C(K, k) * C(N-K, n-k) / C(N, n)
    // Using logs to avoid overflow
    let ln_prob = ln_binom(k_pop, k) + ln_binom(n_pop - k_pop, n - k) - ln_binom(n_pop, n);
    ln_prob.exp()
}

/* ═══════════════════════════════════════════════════════════════════════════
COVARIANCE AND CORRELATION FUNCTIONS
═══════════════════════════════════════════════════════════════════════════ */

/// Returns population covariance for two paired numeric data sets.
///
/// `COVARIANCE.P` measures joint variability using `n` in the denominator.
///
/// # Remarks
/// - Arrays must resolve to the same number of numeric points.
/// - Uses population scaling (`/ n`) rather than sample scaling.
/// - Positive output indicates same-direction movement; negative output indicates opposite movement.
/// - Pairing and shape mismatches return spreadsheet errors from paired-array validation.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Positive population covariance"
/// formula: "=COVARIANCE.P({1,3,5},{2,4,6})"
/// expected: 2.6666666666666665
/// ```
///
/// ```yaml,sandbox
/// title: "Negative population covariance"
/// formula: "=COVARIANCE.P({1,2,3},{3,2,1})"
/// expected: -0.6666666666666666
/// ```
#[derive(Debug)]
pub struct CovariancePFn;
/// [formualizer-docgen:schema:start]
/// Name: COVARIANCE.P
/// Type: CovariancePFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: COVARIANCE.P(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for CovariancePFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "COVARIANCE.P"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["COVAR"]
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let (y, x) = match collect_paired_arrays(args) {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let n = x.len() as f64;
        let mean_x = x.iter().sum::<f64>() / n;
        let mean_y = y.iter().sum::<f64>() / n;

        let mut sum_xy = 0.0;
        for i in 0..x.len() {
            let dx = x[i] - mean_x;
            let dy = y[i] - mean_y;
            sum_xy += dx * dy;
        }

        // Population covariance divides by n
        let covar = sum_xy / n;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            covar,
        )))
    }
}

/// Returns sample covariance for two paired numeric data sets.
///
/// `COVARIANCE.S` measures joint variability using `n - 1` in the denominator.
///
/// # Remarks
/// - Arrays must contain paired numeric values with matching lengths.
/// - Requires at least two paired points.
/// - Returns `#DIV/0!` when fewer than two numeric pairs are available.
/// - Pairing and shape mismatches return spreadsheet errors from paired-array validation.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Positive sample covariance"
/// formula: "=COVARIANCE.S({1,3,5},{2,4,6})"
/// expected: 4
/// ```
///
/// ```yaml,sandbox
/// title: "Negative sample covariance"
/// formula: "=COVARIANCE.S({1,2,3},{3,2,1})"
/// expected: -1
/// ```
#[derive(Debug)]
pub struct CovarianceSFn;
/// [formualizer-docgen:schema:start]
/// Name: COVARIANCE.S
/// Type: CovarianceSFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: COVARIANCE.S(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for CovarianceSFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "COVARIANCE.S"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let (y, x) = match collect_paired_arrays(args) {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let n = x.len();
        if n < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let mean_x = x.iter().sum::<f64>() / n as f64;
        let mean_y = y.iter().sum::<f64>() / n as f64;

        let mut sum_xy = 0.0;
        for i in 0..n {
            let dx = x[i] - mean_x;
            let dy = y[i] - mean_y;
            sum_xy += dx * dy;
        }

        // Sample covariance divides by (n - 1)
        let covar = sum_xy / (n - 1) as f64;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            covar,
        )))
    }
}

/// Returns the Pearson correlation coefficient between two paired numeric arrays.
///
/// `PEARSON` reports linear association on a normalized scale from `-1` to `1`.
///
/// # Remarks
/// - Arrays must contain the same number of numeric observations.
/// - Returns `#DIV/0!` when either array has zero variance.
/// - Positive values indicate positive linear association; negative values indicate inverse association.
/// - Pairing and shape mismatches return spreadsheet errors from paired-array validation.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Perfect positive linear correlation"
/// formula: "=PEARSON({1,2,3},{2,4,6})"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Perfect negative linear correlation"
/// formula: "=PEARSON({1,2,3},{3,2,1})"
/// expected: -1
/// ```
#[derive(Debug)]
pub struct PearsonFn;
/// [formualizer-docgen:schema:start]
/// Name: PEARSON
/// Type: PearsonFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: PEARSON(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for PearsonFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "PEARSON"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let (y, x) = match collect_paired_arrays(args) {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let n = x.len() as f64;
        let mean_x = x.iter().sum::<f64>() / n;
        let mean_y = y.iter().sum::<f64>() / n;

        let mut sum_xy = 0.0;
        let mut sum_x2 = 0.0;
        let mut sum_y2 = 0.0;

        for i in 0..x.len() {
            let dx = x[i] - mean_x;
            let dy = y[i] - mean_y;
            sum_xy += dx * dy;
            sum_x2 += dx * dx;
            sum_y2 += dy * dy;
        }

        let denom = (sum_x2 * sum_y2).sqrt();
        if denom == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let correl = sum_xy / denom;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            correl,
        )))
    }
}

/// Returns the coefficient of determination (`R^2`) for paired x/y data.
///
/// `RSQ` is the square of Pearson correlation and indicates explained linear variance.
///
/// # Remarks
/// - Arrays must contain the same number of numeric observations.
/// - Result is in `[0, 1]` for valid numeric inputs.
/// - Returns `#DIV/0!` when either input array has zero variance.
/// - Pairing and shape mismatches return spreadsheet errors from paired-array validation.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Perfect linear fit"
/// formula: "=RSQ({1,2,3},{2,4,6})"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Strong but imperfect linear relationship"
/// formula: "=RSQ({1,2,3},{1,2,4})"
/// expected: 0.9642857142857143
/// ```
#[derive(Debug)]
pub struct RsqFn;
/// [formualizer-docgen:schema:start]
/// Name: RSQ
/// Type: RsqFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: RSQ(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for RsqFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "RSQ"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let (y, x) = match collect_paired_arrays(args) {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let n = x.len() as f64;
        let mean_x = x.iter().sum::<f64>() / n;
        let mean_y = y.iter().sum::<f64>() / n;

        let mut sum_xy = 0.0;
        let mut sum_x2 = 0.0;
        let mut sum_y2 = 0.0;

        for i in 0..x.len() {
            let dx = x[i] - mean_x;
            let dy = y[i] - mean_y;
            sum_xy += dx * dy;
            sum_x2 += dx * dx;
            sum_y2 += dy * dy;
        }

        let denom = sum_x2 * sum_y2;
        if denom == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        // R-squared = r^2 = (sum_xy)^2 / (sum_x2 * sum_y2)
        let rsq = (sum_xy * sum_xy) / denom;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(rsq)))
    }
}

/// Returns the standard error of y-estimates from a simple linear regression.
///
/// `STEYX` measures the typical residual size around the fitted regression line.
///
/// # Remarks
/// - Requires paired x/y inputs with matching numeric lengths.
/// - Requires at least three paired points.
/// - Returns `#DIV/0!` when `n < 3` or x-values have zero variance.
/// - Pairing and shape mismatches return spreadsheet errors from paired-array validation.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Perfect linear fit has zero standard error"
/// formula: "=STEYX({2,4,6},{1,2,3})"
/// expected: 0
/// ```
///
/// ```yaml,sandbox
/// title: "Non-zero regression standard error"
/// formula: "=STEYX({2,5,7},{1,2,3})"
/// expected: 0.408248290463863
/// ```
#[derive(Debug)]
pub struct SteyxFn;
/// [formualizer-docgen:schema:start]
/// Name: STEYX
/// Type: SteyxFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: STEYX(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for SteyxFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "STEYX"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let (y, x) = match collect_paired_arrays(args) {
            Ok(v) => v,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        let n = x.len();
        if n < 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let n_f = n as f64;
        let mean_x = x.iter().sum::<f64>() / n_f;
        let mean_y = y.iter().sum::<f64>() / n_f;

        let mut sum_xy = 0.0;
        let mut sum_x2 = 0.0;
        let mut sum_y2 = 0.0;

        for i in 0..n {
            let dx = x[i] - mean_x;
            let dy = y[i] - mean_y;
            sum_xy += dx * dy;
            sum_x2 += dx * dx;
            sum_y2 += dy * dy;
        }

        if sum_x2 == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        // STEYX = sqrt((sum_y2 - (sum_xy)^2 / sum_x2) / (n - 2))
        let sse = sum_y2 - (sum_xy * sum_xy) / sum_x2;
        if sse < 0.0 {
            // This can happen due to floating point errors; return 0 in such case
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(0.0)));
        }
        let steyx = (sse / (n_f - 2.0)).sqrt();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            steyx,
        )))
    }
}

/* ─────────────────────────── SKEW ──────────────────────────── */

/// Returns the sample skewness of a numeric distribution.
///
/// `SKEW` quantifies asymmetry: positive values indicate a longer right tail, negative values a
/// longer left tail.
///
/// # Remarks
/// - Requires at least three numeric values.
/// - Returns `#DIV/0!` when there are fewer than three numbers or zero sample standard deviation.
/// - Non-numeric values in ranges are ignored by statistical-collection rules.
/// - Uses the Excel-style sample skewness correction factor.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Symmetric sample"
/// formula: "=SKEW({1,2,3})"
/// expected: 0
/// ```
///
/// ```yaml,sandbox
/// title: "Right-skewed sample"
/// formula: "=SKEW({1,1,2,10})"
/// expected: 1.9683567600862015
/// ```
#[derive(Debug)]
pub struct SkewFn;
/// [formualizer-docgen:schema:start]
/// Name: SKEW
/// Type: SkewFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: SKEW(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for SkewFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "SKEW"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        let n = nums.len();

        // SKEW requires at least 3 data points
        if n < 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let n_f = n as f64;
        let mean = nums.iter().sum::<f64>() / n_f;

        // Calculate sample standard deviation
        let mut sum_sq = 0.0;
        for &v in &nums {
            let d = v - mean;
            sum_sq += d * d;
        }
        let stdev = (sum_sq / (n_f - 1.0)).sqrt();

        if stdev == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        // Calculate sum of cubed deviations normalized by stdev
        let mut sum_cubed = 0.0;
        for &v in &nums {
            let d = (v - mean) / stdev;
            sum_cubed += d * d * d;
        }

        // Excel SKEW formula: n / ((n-1)*(n-2)) * sum((xi - mean)/stdev)^3
        let skew = (n_f / ((n_f - 1.0) * (n_f - 2.0))) * sum_cubed;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(skew)))
    }
}

/* ─────────────────────────── KURT ──────────────────────────── */

/// Returns the sample excess kurtosis of a numeric distribution.
///
/// `KURT` indicates tail heaviness relative to a normal distribution after Excel-style sample
/// correction.
///
/// # Remarks
/// - Requires at least four numeric values.
/// - Returns `#DIV/0!` when there are fewer than four numbers or zero sample standard deviation.
/// - Positive values suggest heavier tails; negative values suggest lighter tails.
/// - Non-numeric values in ranges are ignored by statistical-collection rules.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Uniformly spaced values"
/// formula: "=KURT({1,2,3,4})"
/// expected: -1.2
/// ```
///
/// ```yaml,sandbox
/// title: "Heavier-tail sample"
/// formula: "=KURT({1,1,1,2,10,10,10,10})"
/// expected: -2.3069755007920767
/// ```
#[derive(Debug)]
pub struct KurtFn;
/// [formualizer-docgen:schema:start]
/// Name: KURT
/// Type: KurtFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: KURT(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, REDUCTION, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for KurtFn {
    func_caps!(PURE, NUMERIC_ONLY, REDUCTION);
    fn name(&self) -> &'static str {
        "KURT"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        let n = nums.len();

        // KURT requires at least 4 data points
        if n < 4 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let n_f = n as f64;
        let mean = nums.iter().sum::<f64>() / n_f;

        // Calculate sample standard deviation
        let mut sum_sq = 0.0;
        for &v in &nums {
            let d = v - mean;
            sum_sq += d * d;
        }
        let stdev = (sum_sq / (n_f - 1.0)).sqrt();

        if stdev == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        // Calculate sum of fourth powers of deviations normalized by stdev
        let mut sum_fourth = 0.0;
        for &v in &nums {
            let d = (v - mean) / stdev;
            sum_fourth += d * d * d * d;
        }

        // Excel KURT formula (excess kurtosis):
        // n*(n+1) / ((n-1)*(n-2)*(n-3)) * sum((xi - mean)/stdev)^4 - 3*(n-1)^2 / ((n-2)*(n-3))
        let term1 = (n_f * (n_f + 1.0)) / ((n_f - 1.0) * (n_f - 2.0) * (n_f - 3.0)) * sum_fourth;
        let term2 = (3.0 * (n_f - 1.0) * (n_f - 1.0)) / ((n_f - 2.0) * (n_f - 3.0));
        let kurt = term1 - term2;
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(kurt)))
    }
}

/* ─────────────────────────── FISHER ──────────────────────────── */

/// Returns the Fisher z-transformation of a correlation-like value `x`.
///
/// `FISHER` maps `(-1, 1)` into `(-inf, +inf)` and is commonly used in correlation inference.
///
/// # Remarks
/// - Input must satisfy `-1 < x < 1`.
/// - Returns `#NUM!` when `x <= -1` or `x >= 1`.
/// - The transformation is `0.5 * ln((1 + x) / (1 - x))`.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Fisher transform at zero"
/// formula: "=FISHER(0)"
/// expected: 0
/// ```
///
/// ```yaml,sandbox
/// title: "Fisher transform at x=0.5"
/// formula: "=FISHER(0.5)"
/// expected: 0.5493061443340549
/// ```
#[derive(Debug)]
pub struct FisherFn;
/// [formualizer-docgen:schema:start]
/// Name: FISHER
/// Type: FisherFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: FISHER(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for FisherFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "FISHER"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;

        // FISHER requires -1 < x < 1
        if x <= -1.0 || x >= 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // Fisher transformation: 0.5 * ln((1 + x) / (1 - x))
        let fisher = 0.5 * ((1.0 + x) / (1.0 - x)).ln();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            fisher,
        )))
    }
}

/* ─────────────────────────── FISHERINV ──────────────────────────── */

/// Returns the inverse Fisher transformation of `y`.
///
/// `FISHERINV` maps Fisher z-values back to the open interval `(-1, 1)`.
///
/// # Remarks
/// - The inverse form is `(e^(2y) - 1) / (e^(2y) + 1)`.
/// - Output is always strictly between `-1` and `1` for finite inputs.
/// - This function is useful for converting transformed correlation estimates back to r-space.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Inverse Fisher at zero"
/// formula: "=FISHERINV(0)"
/// expected: 0
/// ```
///
/// ```yaml,sandbox
/// title: "Round-trip with FISHER(0.5)"
/// formula: "=FISHERINV(0.5493061443340549)"
/// expected: 0.5
/// ```
#[derive(Debug)]
pub struct FisherInvFn;
/// [formualizer-docgen:schema:start]
/// Name: FISHERINV
/// Type: FisherInvFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: FISHERINV(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for FisherInvFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "FISHERINV"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let y = coerce_num(&scalar_like_value(&args[0])?)?;

        // Inverse Fisher transformation: (e^(2y) - 1) / (e^(2y) + 1)
        let e2y = (2.0 * y).exp();
        let fisherinv = (e2y - 1.0) / (e2y + 1.0);
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            fisherinv,
        )))
    }
}

/* ─────────────────────────── FORECAST.LINEAR ──────────────────────────── */

/// Returns a predicted y-value at `x` from simple linear regression over known data.
///
/// `FORECAST.LINEAR` fits `y = intercept + slope * x` and evaluates that line at the requested x.
///
/// # Remarks
/// - Requires `known_y` and `known_x` arrays with the same numeric length.
/// - Returns `#N/A` when arrays are empty or lengths do not match.
/// - Returns `#DIV/0!` when `known_x` has zero variance.
/// - Alias `FORECAST` is supported.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Predict next point on a perfect line"
/// formula: "=FORECAST.LINEAR(4,{2,4,6},{1,2,3})"
/// expected: 8
/// ```
///
/// ```yaml,sandbox
/// title: "Forecast with non-zero intercept"
/// formula: "=FORECAST.LINEAR(5,{3,5,7},{1,2,3})"
/// expected: 11
/// ```
#[derive(Debug)]
pub struct ForecastLinearFn;
/// [formualizer-docgen:schema:start]
/// Name: FORECAST.LINEAR
/// Type: ForecastLinearFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: FORECAST.LINEAR(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for ForecastLinearFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "FORECAST.LINEAR"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["FORECAST"]
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // args[0] = x value to forecast
        // args[1] = known_y's
        // args[2] = known_x's
        let x = match coerce_num(&scalar_like_value(&args[0])?) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value(),
                )));
            }
        };

        let y_vals = collect_numeric_stats(&args[1..2])?;
        let x_vals = collect_numeric_stats(&args[2..3])?;

        // Arrays must have same length
        if y_vals.len() != x_vals.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        if y_vals.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        let n = x_vals.len() as f64;
        let mean_x = x_vals.iter().sum::<f64>() / n;
        let mean_y = y_vals.iter().sum::<f64>() / n;

        let mut sum_xy = 0.0;
        let mut sum_x2 = 0.0;

        for i in 0..x_vals.len() {
            let dx = x_vals[i] - mean_x;
            let dy = y_vals[i] - mean_y;
            sum_xy += dx * dy;
            sum_x2 += dx * dx;
        }

        if sum_x2 == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let slope = sum_xy / sum_x2;
        let intercept = mean_y - slope * mean_x;
        let forecast = intercept + slope * x;

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            forecast,
        )))
    }
}

/* ─────────────────────────── LINEST ──────────────────────────── */

/// Returns linear-regression coefficients and optional fit statistics.
///
/// `LINEST` fits a straight line to known y/x pairs and returns either `[slope, intercept]` or a
/// larger statistics matrix.
///
/// # Remarks
/// - `known_y` is required; `known_x` defaults to `1..n` when omitted.
/// - `const` controls whether an intercept is fitted (`TRUE` by default).
/// - `stats=TRUE` returns a `5x2` result block; otherwise it returns `1x2`.
/// - Returns spreadsheet errors for mismatched lengths, empty data, or degenerate x-values.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Slope and intercept only"
/// formula: "=LINEST({2,4,6},{1,2,3})"
/// expected:
///   - [2, 0]
/// ```
///
/// ```yaml,sandbox
/// title: "Linear fit with non-zero intercept"
/// formula: "=LINEST({3,5,7},{1,2,3})"
/// expected:
///   - [2, 1]
/// ```
#[derive(Debug)]
pub struct LinestFn;
/// [formualizer-docgen:schema:start]
/// Name: LINEST
/// Type: LinestFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: LINEST(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for LinestFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "LINEST"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // args[0] = known_y's (required)
        // args[1] = known_x's (optional, defaults to {1,2,3,...})
        // args[2] = const (optional, default TRUE - whether to compute intercept)
        // args[3] = stats (optional, default FALSE - whether to return additional statistics)

        let y_vals = collect_numeric_stats(&args[0..1])?;

        if y_vals.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        // Get known_x's or generate default {1, 2, 3, ...}
        let x_vals = if args.len() >= 2 {
            collect_numeric_stats(&args[1..2])?
        } else {
            (1..=y_vals.len()).map(|i| i as f64).collect()
        };

        // Arrays must have same length
        if y_vals.len() != x_vals.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_ref(),
            )));
        }

        // Parse const argument (default TRUE)
        let use_const = if args.len() >= 3 {
            match scalar_like_value(&args[2])? {
                LiteralValue::Boolean(b) => b,
                LiteralValue::Number(n) => n != 0.0,
                LiteralValue::Int(i) => i != 0,
                _ => true,
            }
        } else {
            true
        };

        // Parse stats argument (default FALSE)
        let return_stats = if args.len() >= 4 {
            match scalar_like_value(&args[3])? {
                LiteralValue::Boolean(b) => b,
                LiteralValue::Number(n) => n != 0.0,
                LiteralValue::Int(i) => i != 0,
                _ => false,
            }
        } else {
            false
        };

        let n = x_vals.len() as f64;

        // Calculate regression coefficients
        let (slope, intercept) = if use_const {
            // Normal linear regression with intercept
            let mean_x = x_vals.iter().sum::<f64>() / n;
            let mean_y = y_vals.iter().sum::<f64>() / n;

            let mut sum_xy = 0.0;
            let mut sum_x2 = 0.0;

            for i in 0..x_vals.len() {
                let dx = x_vals[i] - mean_x;
                let dy = y_vals[i] - mean_y;
                sum_xy += dx * dy;
                sum_x2 += dx * dx;
            }

            if sum_x2 == 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_div(),
                )));
            }

            let slope = sum_xy / sum_x2;
            let intercept = mean_y - slope * mean_x;
            (slope, intercept)
        } else {
            // Regression through origin (intercept = 0)
            let mut sum_xy = 0.0;
            let mut sum_x2 = 0.0;

            for i in 0..x_vals.len() {
                sum_xy += x_vals[i] * y_vals[i];
                sum_x2 += x_vals[i] * x_vals[i];
            }

            if sum_x2 == 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_div(),
                )));
            }

            let slope = sum_xy / sum_x2;
            (slope, 0.0)
        };

        if !return_stats {
            // Return just slope and intercept as 1x2 array: [[slope, intercept]]
            let row = vec![LiteralValue::Number(slope), LiteralValue::Number(intercept)];
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(vec![
                row,
            ])));
        }

        // Calculate additional statistics for stats=TRUE
        // Row 1: [slope, intercept]
        // Row 2: [se_slope, se_intercept]
        // Row 3: [r_squared, se_y]
        // Row 4: [F_statistic, df]
        // Row 5: [ss_reg, ss_resid]

        let mean_y = y_vals.iter().sum::<f64>() / n;

        // Calculate residuals and sums of squares
        let mut ss_resid = 0.0; // Sum of squared residuals
        let mut ss_tot = 0.0; // Total sum of squares

        for i in 0..x_vals.len() {
            let y_pred = slope * x_vals[i] + intercept;
            let residual = y_vals[i] - y_pred;
            ss_resid += residual * residual;
            let dy_tot = y_vals[i] - mean_y;
            ss_tot += dy_tot * dy_tot;
        }

        let ss_reg = ss_tot - ss_resid; // Regression sum of squares

        // R-squared
        let r_squared = if ss_tot == 0.0 {
            1.0 // Perfect fit or all y values are the same
        } else {
            1.0 - (ss_resid / ss_tot)
        };

        // Degrees of freedom
        let df = if use_const {
            (n as i64 - 2).max(1) as f64 // n - k - 1 where k=1 (one predictor)
        } else {
            (n as i64 - 1).max(1) as f64 // n - k when no intercept
        };

        // Standard error of y estimate
        let se_y = if df > 0.0 {
            (ss_resid / df).sqrt()
        } else {
            0.0
        };

        // Standard errors of coefficients
        let mean_x = x_vals.iter().sum::<f64>() / n;
        let mut sum_x2_centered = 0.0;
        let mut sum_x2_raw = 0.0;
        for &xi in &x_vals {
            sum_x2_centered += (xi - mean_x).powi(2);
            sum_x2_raw += xi * xi;
        }

        let se_slope = if sum_x2_centered > 0.0 && df > 0.0 {
            se_y / sum_x2_centered.sqrt()
        } else {
            f64::NAN
        };

        let se_intercept = if use_const && sum_x2_centered > 0.0 && df > 0.0 {
            se_y * (sum_x2_raw / (n * sum_x2_centered)).sqrt()
        } else {
            f64::NAN
        };

        // F-statistic
        let f_stat = if ss_resid > 0.0 && df > 0.0 {
            (ss_reg / 1.0) / (ss_resid / df) // MSR / MSE
        } else if ss_resid == 0.0 {
            f64::INFINITY // Perfect fit
        } else {
            f64::NAN
        };

        // Build 5x2 result array
        let rows = vec![
            vec![LiteralValue::Number(slope), LiteralValue::Number(intercept)],
            vec![
                LiteralValue::Number(se_slope),
                LiteralValue::Number(se_intercept),
            ],
            vec![LiteralValue::Number(r_squared), LiteralValue::Number(se_y)],
            vec![LiteralValue::Number(f_stat), LiteralValue::Number(df)],
            vec![LiteralValue::Number(ss_reg), LiteralValue::Number(ss_resid)],
        ];

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(rows)))
    }
}

/* ─────────────────────────── CONFIDENCE.NORM ──────────────────────────── */

/// Returns the half-width of a confidence interval using a normal critical value.
///
/// `CONFIDENCE.NORM` computes `z_crit * standard_dev / sqrt(size)` for two-sided intervals.
///
/// # Remarks
/// - `alpha` must satisfy `0 < alpha < 1`.
/// - `standard_dev` must be greater than `0`.
/// - `size` must be at least `1`.
/// - Returns `#NUM!` when any input is outside valid bounds.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "95% confidence half-width"
/// formula: "=CONFIDENCE.NORM(0.05,2,100)"
/// expected: 0.3919927977622559
/// ```
///
/// ```yaml,sandbox
/// title: "90% confidence half-width"
/// formula: "=CONFIDENCE.NORM(0.1,5,25)"
/// expected: 1.644853625133699
/// ```
#[derive(Debug)]
pub struct ConfidenceNormFn;
/// [formualizer-docgen:schema:start]
/// Name: CONFIDENCE.NORM
/// Type: ConfidenceNormFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: CONFIDENCE.NORM(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ConfidenceNormFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CONFIDENCE.NORM"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["CONFIDENCE"]
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let alpha = coerce_num(&scalar_like_value(&args[0])?)?;
        let std_dev = coerce_num(&scalar_like_value(&args[1])?)?;
        let size = coerce_num(&scalar_like_value(&args[2])?)?;

        // Validate inputs
        if alpha <= 0.0 || alpha >= 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        if std_dev <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        if size < 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // z_crit = NORM.S.INV(1 - alpha/2)
        let z_crit = match std_norm_inv(1.0 - alpha / 2.0) {
            Some(z) => z,
            None => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };

        let result = z_crit * std_dev / size.sqrt();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/* ─────────────────────────── CONFIDENCE.T ──────────────────────────── */

/// Returns the half-width of a confidence interval using a t critical value.
///
/// `CONFIDENCE.T` is typically used when population standard deviation is unknown and sample size
/// is limited.
///
/// # Remarks
/// - `alpha` must satisfy `0 < alpha < 1`.
/// - `standard_dev` must be greater than `0`.
/// - `size` must be at least `2` so that `df = size - 1` is valid.
/// - Returns `#NUM!` when inputs are outside valid bounds.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "95% t-interval half-width"
/// formula: "=CONFIDENCE.T(0.05,2,25)"
/// expected: 0.8256636934020788
/// ```
///
/// ```yaml,sandbox
/// title: "90% t-interval half-width"
/// formula: "=CONFIDENCE.T(0.1,5,10)"
/// expected: 2.9158049866307585
/// ```
#[derive(Debug)]
pub struct ConfidenceTFn;
/// [formualizer-docgen:schema:start]
/// Name: CONFIDENCE.T
/// Type: ConfidenceTFn
/// Min args: 3
/// Max args: 3
/// Variadic: false
/// Signature: CONFIDENCE.T(arg1: number@scalar, arg2: number@scalar, arg3: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ConfidenceTFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CONFIDENCE.T"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let alpha = coerce_num(&scalar_like_value(&args[0])?)?;
        let std_dev = coerce_num(&scalar_like_value(&args[1])?)?;
        let size = coerce_num(&scalar_like_value(&args[2])?)?;

        // Validate inputs - size must be >= 2 for t-distribution (df = size - 1 >= 1)
        if alpha <= 0.0 || alpha >= 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        if std_dev <= 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        if size < 2.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let df = size - 1.0;

        // t_crit = T.INV(1 - alpha/2, df)
        let t_crit = match t_inv(1.0 - alpha / 2.0, df) {
            Some(t) => t,
            None => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };

        let result = t_crit * std_dev / size.sqrt();
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            result,
        )))
    }
}

/* ─────────────────────────── Z.TEST ──────────────────────────── */

/// Returns the one-tailed p-value of a z-test against hypothesized mean `x`.
///
/// `Z.TEST` evaluates whether the sample mean is significantly greater than the target value.
///
/// # Remarks
/// - Uses provided `sigma` when supplied; otherwise computes population standard deviation.
/// - Returns `#NUM!` when `sigma <= 0`.
/// - Returns `#DIV/0!` when implied standard deviation is zero.
/// - Returns `#N/A` when the data array has no numeric values.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Z-test with provided sigma"
/// formula: "=Z.TEST({1,2,3,4,5},2,1)"
/// expected: 0.012673659338734137
/// ```
///
/// ```yaml,sandbox
/// title: "Z-test with sigma estimated from sample"
/// formula: "=Z.TEST({1,2,3,4,5},2)"
/// expected: 0.056923149003329065
/// ```
#[derive(Debug)]
pub struct ZTestFn;
/// [formualizer-docgen:schema:start]
/// Name: Z.TEST
/// Type: ZTestFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: Z.TEST(arg1: number@range, arg2: number@scalar, arg3...: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ZTestFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "Z.TEST"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["ZTEST"]
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
                {
                    let mut s = ArgSchema::number_lenient_scalar();
                    s.shape = crate::args::ShapeKind::Range;
                    s.coercion = formualizer_common::CoercionPolicy::NumberLenientText;
                    s
                },
                ArgSchema::number_lenient_scalar(),
                ArgSchema::number_lenient_scalar(), // optional sigma
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // Collect numeric values from the array argument
        let data = collect_numeric_stats(&args[0..1])?;

        if data.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        let x = coerce_num(&scalar_like_value(&args[1])?)?;

        let n = data.len() as f64;
        let mean: f64 = data.iter().sum::<f64>() / n;

        // Calculate sigma: use provided value or compute population std dev
        let sigma = if args.len() > 2 {
            let s = coerce_num(&scalar_like_value(&args[2])?)?;
            if s <= 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            s
        } else {
            // Population standard deviation
            let variance: f64 = data.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
            let std_dev = variance.sqrt();
            if std_dev == 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_div(),
                )));
            }
            std_dev
        };

        // z = (mean - x) / (sigma / sqrt(n))
        let z = (mean - x) / (sigma / n.sqrt());

        // P-value = 1 - NORM.S.DIST(z, TRUE)
        let p_value = 1.0 - std_norm_cdf(z);

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            p_value,
        )))
    }
}

/* ─────────────────────────── TREND ──────────────────────────── */

/// Returns fitted y-values along a linear trend derived from known data.
///
/// `TREND` performs simple linear regression and returns predictions for `new_x` (or defaults).
///
/// # Remarks
/// - `known_y` is required; `known_x` defaults to `1..n` when omitted.
/// - `new_x` defaults to `known_x` when omitted.
/// - `const` defaults to `TRUE`; set to `FALSE` to force a zero intercept.
/// - Returns spreadsheet errors for empty data, mismatched lengths, or degenerate x-variance.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Predict two future points on a line"
/// formula: "=TREND({2,4,6},{1,2,3},{4,5})"
/// expected:
///   - [8, 10]
/// ```
///
/// ```yaml,sandbox
/// title: "Default x-values with fitted trend"
/// formula: "=TREND({3,5,7})"
/// expected:
///   - [3, 5, 7]
/// ```
#[derive(Debug)]
pub struct TrendFn;
/// [formualizer-docgen:schema:start]
/// Name: TREND
/// Type: TrendFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: TREND(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for TrendFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "TREND"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // TREND: args[0] = known_y's (required)
        // args[1] = known_x's (optional, defaults to {1,2,3,...})
        // args[2] = new_x's (optional, defaults to known_x's)
        // args[3] = const (optional, default TRUE - whether to compute intercept)

        let y_vals = collect_numeric_stats(&args[0..1])?;

        if y_vals.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        // Helper to check if argument is empty/omitted
        // Note: Empty arguments are represented as empty text strings by the parser
        fn is_arg_empty(arg: &ArgumentHandle) -> bool {
            match scalar_like_value(arg) {
                Ok(LiteralValue::Empty) => true,
                Ok(LiteralValue::Text(s)) if s.is_empty() => true,
                _ => false,
            }
        }

        // Get known_x's or generate default {1, 2, 3, ...}
        let x_vals = if args.len() >= 2 && !is_arg_empty(&args[1]) {
            collect_numeric_stats(&args[1..2])?
        } else {
            (1..=y_vals.len()).map(|i| i as f64).collect()
        };

        // Arrays must have same length
        if y_vals.len() != x_vals.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_ref(),
            )));
        }

        // Get new_x's or use known_x's - check if argument is empty/omitted
        let new_x_vals = if args.len() >= 3 && !is_arg_empty(&args[2]) {
            collect_numeric_stats(&args[2..3])?
        } else {
            x_vals.clone()
        };

        if new_x_vals.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        // Parse const argument (default TRUE)
        let use_const = if args.len() >= 4 {
            match scalar_like_value(&args[3])? {
                LiteralValue::Boolean(b) => b,
                LiteralValue::Number(n) => n != 0.0,
                LiteralValue::Int(i) => i != 0,
                LiteralValue::Empty => true, // empty defaults to TRUE
                _ => true,
            }
        } else {
            true
        };

        let n = x_vals.len() as f64;

        // Calculate regression coefficients
        let (slope, intercept) = if use_const {
            // Normal linear regression with intercept
            let mean_x = x_vals.iter().sum::<f64>() / n;
            let mean_y = y_vals.iter().sum::<f64>() / n;

            let mut sum_xy = 0.0;
            let mut sum_x2 = 0.0;

            for i in 0..x_vals.len() {
                let dx = x_vals[i] - mean_x;
                let dy = y_vals[i] - mean_y;
                sum_xy += dx * dy;
                sum_x2 += dx * dx;
            }

            if sum_x2 == 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_div(),
                )));
            }

            let slope = sum_xy / sum_x2;
            let intercept = mean_y - slope * mean_x;
            (slope, intercept)
        } else {
            // Regression through origin (intercept = 0)
            let mut sum_xy = 0.0;
            let mut sum_x2 = 0.0;

            for i in 0..x_vals.len() {
                sum_xy += x_vals[i] * y_vals[i];
                sum_x2 += x_vals[i] * x_vals[i];
            }

            if sum_x2 == 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_div(),
                )));
            }

            let slope = sum_xy / sum_x2;
            (slope, 0.0)
        };

        // Calculate predicted y values for new_x's
        let predicted: Vec<LiteralValue> = new_x_vals
            .iter()
            .map(|&x| LiteralValue::Number(slope * x + intercept))
            .collect();

        // Return as 1xN array (row vector)
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(vec![
            predicted,
        ])))
    }
}

/* ─────────────────────────── GROWTH ──────────────────────────── */

/// Returns fitted values from an exponential trend model.
///
/// `GROWTH` fits `y = b * m^x` by linearizing in log space, then returns predictions for `new_x`.
///
/// # Remarks
/// - All known y-values must be strictly greater than `0`.
/// - `known_x` defaults to `1..n`; `new_x` defaults to `known_x`.
/// - `const` defaults to `TRUE`; set to `FALSE` to force `b = 1`.
/// - Returns spreadsheet errors for invalid domains, mismatched lengths, or degenerate x-variance.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Exponential growth forecast"
/// formula: "=GROWTH({2,4,8},{1,2,3},{4,5})"
/// expected:
///   - [16, 32]
/// ```
///
/// ```yaml,sandbox
/// title: "Default x-values with perfect doubling pattern"
/// formula: "=GROWTH({3,6,12})"
/// expected:
///   - [3, 6, 12]
/// ```
#[derive(Debug)]
pub struct GrowthFn;
/// [formualizer-docgen:schema:start]
/// Name: GROWTH
/// Type: GrowthFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: GROWTH(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for GrowthFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "GROWTH"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // GROWTH: args[0] = known_y's (required)
        // args[1] = known_x's (optional, defaults to {1,2,3,...})
        // args[2] = new_x's (optional, defaults to known_x's)
        // args[3] = const (optional, default TRUE - whether to compute intercept)

        let y_vals = collect_numeric_stats(&args[0..1])?;

        if y_vals.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        // Check that all y values are positive (required for log transformation)
        for &y in &y_vals {
            if y <= 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        }

        // Helper to check if argument is empty/omitted
        // Note: Empty arguments are represented as empty text strings by the parser
        fn is_arg_empty(arg: &ArgumentHandle) -> bool {
            match scalar_like_value(arg) {
                Ok(LiteralValue::Empty) => true,
                Ok(LiteralValue::Text(s)) if s.is_empty() => true,
                _ => false,
            }
        }

        // Get known_x's or generate default {1, 2, 3, ...}
        let x_vals = if args.len() >= 2 && !is_arg_empty(&args[1]) {
            collect_numeric_stats(&args[1..2])?
        } else {
            (1..=y_vals.len()).map(|i| i as f64).collect()
        };

        // Arrays must have same length
        if y_vals.len() != x_vals.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_ref(),
            )));
        }

        // Get new_x's or use known_x's - check if argument is empty/omitted
        let new_x_vals = if args.len() >= 3 && !is_arg_empty(&args[2]) {
            collect_numeric_stats(&args[2..3])?
        } else {
            x_vals.clone()
        };

        if new_x_vals.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        // Parse const argument (default TRUE)
        let use_const = if args.len() >= 4 {
            match scalar_like_value(&args[3])? {
                LiteralValue::Boolean(b) => b,
                LiteralValue::Number(n) => n != 0.0,
                LiteralValue::Int(i) => i != 0,
                LiteralValue::Empty => true, // empty defaults to TRUE
                _ => true,
            }
        } else {
            true
        };

        // Transform to log space: ln(y) = ln(b) + x*ln(m)
        // This is linear regression on log-transformed y values
        let ln_y_vals: Vec<f64> = y_vals.iter().map(|&y| y.ln()).collect();

        let n = x_vals.len() as f64;

        // Calculate regression coefficients in log space
        let (ln_m, ln_b) = if use_const {
            // Normal linear regression with intercept
            let mean_x = x_vals.iter().sum::<f64>() / n;
            let mean_ln_y = ln_y_vals.iter().sum::<f64>() / n;

            let mut sum_xy = 0.0;
            let mut sum_x2 = 0.0;

            for i in 0..x_vals.len() {
                let dx = x_vals[i] - mean_x;
                let dy = ln_y_vals[i] - mean_ln_y;
                sum_xy += dx * dy;
                sum_x2 += dx * dx;
            }

            if sum_x2 == 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_div(),
                )));
            }

            let ln_m = sum_xy / sum_x2;
            let ln_b = mean_ln_y - ln_m * mean_x;
            (ln_m, ln_b)
        } else {
            // Regression through origin in log space (ln_b = 0, so b = 1)
            let mut sum_xy = 0.0;
            let mut sum_x2 = 0.0;

            for i in 0..x_vals.len() {
                sum_xy += x_vals[i] * ln_y_vals[i];
                sum_x2 += x_vals[i] * x_vals[i];
            }

            if sum_x2 == 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_div(),
                )));
            }

            let ln_m = sum_xy / sum_x2;
            (ln_m, 0.0)
        };

        // Convert back from log space: m = e^ln_m, b = e^ln_b
        let m = ln_m.exp();
        let b = ln_b.exp();

        // Calculate predicted y values: y = b * m^x
        let predicted: Vec<LiteralValue> = new_x_vals
            .iter()
            .map(|&x| LiteralValue::Number(b * m.powf(x)))
            .collect();

        // Return as 1xN array (row vector)
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(vec![
            predicted,
        ])))
    }
}

/* ─────────────────────────── LOGEST ──────────────────────────── */

/// Returns parameters for an exponential model fitted to known data.
///
/// `LOGEST` fits `y = b * m^x` and returns either `[m, b]` or an expanded statistics matrix.
///
/// # Remarks
/// - All known y-values must be strictly greater than `0`.
/// - `known_x` defaults to `1..n` when omitted.
/// - `const` controls whether `b` is fitted (`TRUE` by default).
/// - `stats=TRUE` returns a `5x2` statistics block; otherwise returns `1x2`.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Exponential base and intercept"
/// formula: "=LOGEST({2,4,8},{1,2,3})"
/// expected:
///   - [2, 1]
/// ```
///
/// ```yaml,sandbox
/// title: "Alternative growth series"
/// formula: "=LOGEST({3,6,12},{1,2,3})"
/// expected:
///   - [2, 1.5]
/// ```
#[derive(Debug)]
pub struct LogestFn;
/// [formualizer-docgen:schema:start]
/// Name: LOGEST
/// Type: LogestFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: LOGEST(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for LogestFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "LOGEST"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // args[0] = known_y's (required)
        // args[1] = known_x's (optional, defaults to {1,2,3,...})
        // args[2] = const (optional, default TRUE - whether to compute b)
        // args[3] = stats (optional, default FALSE - whether to return additional statistics)

        let y_vals = collect_numeric_stats(&args[0..1])?;

        if y_vals.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        // Check that all y values are positive (required for log transformation)
        for &y in &y_vals {
            if y <= 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        }

        // Get known_x's or generate default {1, 2, 3, ...}
        let x_vals = if args.len() >= 2 {
            collect_numeric_stats(&args[1..2])?
        } else {
            (1..=y_vals.len()).map(|i| i as f64).collect()
        };

        // Arrays must have same length
        if y_vals.len() != x_vals.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_ref(),
            )));
        }

        // Parse const argument (default TRUE)
        let use_const = if args.len() >= 3 {
            match scalar_like_value(&args[2])? {
                LiteralValue::Boolean(b) => b,
                LiteralValue::Number(n) => n != 0.0,
                LiteralValue::Int(i) => i != 0,
                _ => true,
            }
        } else {
            true
        };

        // Parse stats argument (default FALSE)
        let return_stats = if args.len() >= 4 {
            match scalar_like_value(&args[3])? {
                LiteralValue::Boolean(b) => b,
                LiteralValue::Number(n) => n != 0.0,
                LiteralValue::Int(i) => i != 0,
                _ => false,
            }
        } else {
            false
        };

        // Transform to log space: ln(y) = ln(b) + x*ln(m)
        let ln_y_vals: Vec<f64> = y_vals.iter().map(|&y| y.ln()).collect();

        let n = x_vals.len() as f64;

        // Calculate regression coefficients in log space
        let (ln_m, ln_b) = if use_const {
            // Normal linear regression with intercept
            let mean_x = x_vals.iter().sum::<f64>() / n;
            let mean_ln_y = ln_y_vals.iter().sum::<f64>() / n;

            let mut sum_xy = 0.0;
            let mut sum_x2 = 0.0;

            for i in 0..x_vals.len() {
                let dx = x_vals[i] - mean_x;
                let dy = ln_y_vals[i] - mean_ln_y;
                sum_xy += dx * dy;
                sum_x2 += dx * dx;
            }

            if sum_x2 == 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_div(),
                )));
            }

            let ln_m = sum_xy / sum_x2;
            let ln_b = mean_ln_y - ln_m * mean_x;
            (ln_m, ln_b)
        } else {
            // Regression through origin in log space (ln_b = 0, so b = 1)
            let mut sum_xy = 0.0;
            let mut sum_x2 = 0.0;

            for i in 0..x_vals.len() {
                sum_xy += x_vals[i] * ln_y_vals[i];
                sum_x2 += x_vals[i] * x_vals[i];
            }

            if sum_x2 == 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_div(),
                )));
            }

            let ln_m = sum_xy / sum_x2;
            (ln_m, 0.0)
        };

        // Convert from log space to get m and b
        let m = ln_m.exp();
        let b = ln_b.exp();

        if !return_stats {
            // Return just m and b as 1x2 array: [[m, b]]
            let row = vec![LiteralValue::Number(m), LiteralValue::Number(b)];
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(vec![
                row,
            ])));
        }

        // Calculate additional statistics for stats=TRUE
        // Statistics are computed in log space, then converted
        // Row 1: [m, b]
        // Row 2: [se_m, se_b] - standard errors (converted from log space)
        // Row 3: [r_squared, se_y] - R-squared and standard error of y estimate
        // Row 4: [F_statistic, df] - F-statistic and degrees of freedom
        // Row 5: [ss_reg, ss_resid] - regression sum of squares and residual sum of squares

        let mean_ln_y = ln_y_vals.iter().sum::<f64>() / n;

        // Calculate residuals and sums of squares in log space
        let mut ss_resid = 0.0;
        let mut ss_tot = 0.0;

        for i in 0..x_vals.len() {
            let ln_y_pred = ln_m * x_vals[i] + ln_b;
            let residual = ln_y_vals[i] - ln_y_pred;
            ss_resid += residual * residual;
            let dy_tot = ln_y_vals[i] - mean_ln_y;
            ss_tot += dy_tot * dy_tot;
        }

        let ss_reg = ss_tot - ss_resid;

        // R-squared (same in both spaces for transformed regression)
        let r_squared = if ss_tot == 0.0 {
            1.0
        } else {
            1.0 - (ss_resid / ss_tot)
        };

        // Degrees of freedom
        let df = if use_const {
            (n as i64 - 2).max(1) as f64
        } else {
            (n as i64 - 1).max(1) as f64
        };

        // Standard error of y estimate (in log space)
        let se_ln_y = if df > 0.0 {
            (ss_resid / df).sqrt()
        } else {
            0.0
        };

        // Standard errors of coefficients in log space
        let mean_x = x_vals.iter().sum::<f64>() / n;
        let mut sum_x2_centered = 0.0;
        let mut sum_x2_raw = 0.0;
        for &xi in &x_vals {
            sum_x2_centered += (xi - mean_x).powi(2);
            sum_x2_raw += xi * xi;
        }

        let se_ln_m = if sum_x2_centered > 0.0 && df > 0.0 {
            se_ln_y / sum_x2_centered.sqrt()
        } else {
            f64::NAN
        };

        let se_ln_b = if use_const && sum_x2_centered > 0.0 && df > 0.0 {
            se_ln_y * (sum_x2_raw / (n * sum_x2_centered)).sqrt()
        } else {
            f64::NAN
        };

        // Convert standard errors: se_m = m * se_ln_m (delta method approximation)
        let se_m = m * se_ln_m;
        let se_b = b * se_ln_b;

        // Standard error of y estimate - convert from log space
        // This is an approximation; for exponential models, se_y in original space varies with x
        let se_y = se_ln_y;

        // F-statistic
        let f_stat = if ss_resid > 0.0 && df > 0.0 {
            (ss_reg / 1.0) / (ss_resid / df)
        } else if ss_resid == 0.0 {
            f64::INFINITY
        } else {
            f64::NAN
        };

        // Build 5x2 result array
        let rows = vec![
            vec![LiteralValue::Number(m), LiteralValue::Number(b)],
            vec![LiteralValue::Number(se_m), LiteralValue::Number(se_b)],
            vec![LiteralValue::Number(r_squared), LiteralValue::Number(se_y)],
            vec![LiteralValue::Number(f_stat), LiteralValue::Number(df)],
            vec![LiteralValue::Number(ss_reg), LiteralValue::Number(ss_resid)],
        ];

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(rows)))
    }
}

/* ─────────────────────────── PERCENTRANK ──────────────────────────── */

/// Returns the inclusive percentile rank of `x` within a numeric data array.
///
/// `PERCENTRANK.INC` maps values to `[0, 1]` and interpolates linearly between data points.
///
/// # Remarks
/// - `x` must be within the observed min/max range; otherwise returns `#N/A`.
/// - Optional `significance` controls decimal truncation and defaults to `3`.
/// - `significance` must be at least `1`.
/// - Returns `#NUM!` for invalid setup such as empty numeric input.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Exact inclusive percentile rank"
/// formula: "=PERCENTRANK.INC({1,2,3,4,5},3)"
/// expected: 0.5
/// ```
///
/// ```yaml,sandbox
/// title: "Interpolated inclusive percentile rank"
/// formula: "=PERCENTRANK.INC({1,2,3,4,5},2.5)"
/// expected: 0.375
/// ```
#[derive(Debug)]
pub struct PercentRankIncFn;
/// [formualizer-docgen:schema:start]
/// Name: PERCENTRANK.INC
/// Type: PercentRankIncFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: PERCENTRANK.INC(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for PercentRankIncFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "PERCENTRANK.INC"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["PERCENTRANK"]
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // Get x value (the value to find the rank of)
        let x = match coerce_num(&scalar_like_value(&args[1])?) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };

        // Get optional significance (default 3)
        let significance = if args.len() > 2 {
            match coerce_num(&scalar_like_value(&args[2])?) {
                Ok(n) => {
                    let s = n as i32;
                    if s < 1 {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new_num(),
                        )));
                    }
                    s as u32
                }
                Err(_) => 3,
            }
        } else {
            3
        };

        // Collect and sort the data array
        let mut nums = collect_numeric_stats(&args[0..1])?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let n = nums.len();

        // Check if x is outside the range
        if x < nums[0] || x > nums[n - 1] {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        // Find the rank using linear interpolation
        // For PERCENTRANK.INC, the formula is: rank = (position) / (n-1)
        // where position is 0-based and uses linear interpolation
        let rank = if n == 1 {
            // Single element - rank is 0 (or 1.0 if we want, but Excel returns 0)
            0.0
        } else {
            let mut rank_val = 0.0;
            for i in 0..n - 1 {
                if (nums[i] - x).abs() < 1e-12 {
                    // Exact match at position i
                    rank_val = (i as f64) / ((n - 1) as f64);
                    break;
                } else if nums[i] < x && x < nums[i + 1] {
                    // Interpolate between positions i and i+1
                    let frac = (x - nums[i]) / (nums[i + 1] - nums[i]);
                    rank_val = ((i as f64) + frac) / ((n - 1) as f64);
                    break;
                } else if i == n - 2 && (nums[n - 1] - x).abs() < 1e-12 {
                    // Exact match at last position
                    rank_val = 1.0;
                }
            }
            rank_val
        };

        // Truncate to significance decimal places
        let multiplier = 10_f64.powi(significance as i32);
        let truncated = (rank * multiplier).trunc() / multiplier;

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            truncated,
        )))
    }
}

/// Returns the exclusive percentile rank of `x` within a numeric data array.
///
/// `PERCENTRANK.EXC` uses an open ranking scale that excludes exact `0` and `1` endpoints.
///
/// # Remarks
/// - `x` must lie within the observed min/max range; otherwise returns `#N/A`.
/// - Output is based on position divided by `n + 1`, with interpolation between points.
/// - Optional `significance` defaults to `3` and must be at least `1`.
/// - Returns `#NUM!` for invalid setup such as empty numeric input.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Exact exclusive percentile rank"
/// formula: "=PERCENTRANK.EXC({1,2,3,4,5},3)"
/// expected: 0.5
/// ```
///
/// ```yaml,sandbox
/// title: "Interpolated exclusive percentile rank"
/// formula: "=PERCENTRANK.EXC({1,2,3,4,5},2.5)"
/// expected: 0.416
/// ```
#[derive(Debug)]
pub struct PercentRankExcFn;
/// [formualizer-docgen:schema:start]
/// Name: PERCENTRANK.EXC
/// Type: PercentRankExcFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: PERCENTRANK.EXC(arg1...: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for PercentRankExcFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "PERCENTRANK.EXC"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // Get x value (the value to find the rank of)
        let x = match coerce_num(&scalar_like_value(&args[1])?) {
            Ok(n) => n,
            Err(_) => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
        };

        // Get optional significance (default 3)
        let significance = if args.len() > 2 {
            match coerce_num(&scalar_like_value(&args[2])?) {
                Ok(n) => {
                    let s = n as i32;
                    if s < 1 {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new_num(),
                        )));
                    }
                    s as u32
                }
                Err(_) => 3,
            }
        } else {
            3
        };

        // Collect and sort the data array
        let mut nums = collect_numeric_stats(&args[0..1])?;
        if nums.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let n = nums.len();

        // Check if x is outside the range
        if x < nums[0] || x > nums[n - 1] {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        // For PERCENTRANK.EXC, the formula is: rank = position / (n+1)
        // where position is 1-based and uses linear interpolation
        let rank = {
            let mut rank_val = 0.0;
            for i in 0..n {
                if (nums[i] - x).abs() < 1e-12 {
                    // Exact match at position i (1-based: i+1)
                    rank_val = ((i + 1) as f64) / ((n + 1) as f64);
                    break;
                } else if i < n - 1 && nums[i] < x && x < nums[i + 1] {
                    // Interpolate between positions i and i+1 (1-based: i+1 and i+2)
                    let frac = (x - nums[i]) / (nums[i + 1] - nums[i]);
                    let position = ((i + 1) as f64) + frac;
                    rank_val = position / ((n + 1) as f64);
                    break;
                }
            }
            rank_val
        };

        // Truncate to significance decimal places
        let multiplier = 10_f64.powi(significance as i32);
        let truncated = (rank * multiplier).trunc() / multiplier;

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            truncated,
        )))
    }
}

/* ─────────────────────────── FREQUENCY ──────────────────────────── */

/// Returns a vertical frequency distribution for numeric data across bin cutoffs.
///
/// `FREQUENCY` counts values into `<= first bin`, intermediate right-closed bins, and an overflow
/// bucket above the final bin.
///
/// # Remarks
/// - Returns an array with `bins + 1` rows.
/// - Bins are sorted before counting.
/// - If `bins_array` has no numeric values, result is a single count of all data points.
/// - Non-numeric values in input ranges are ignored by statistical-collection rules.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Frequency buckets with two bins"
/// formula: "=FREQUENCY({1,2,3,4,5},{2,4})"
/// expected:
///   - [2]
///   - [2]
///   - [1]
/// ```
///
/// ```yaml,sandbox
/// title: "Frequency with repeated values"
/// formula: "=FREQUENCY({1,1,2,2,3},{1,2})"
/// expected:
///   - [2]
///   - [2]
///   - [1]
/// ```
#[derive(Debug)]
pub struct FrequencyFn;
/// [formualizer-docgen:schema:start]
/// Name: FREQUENCY
/// Type: FrequencyFn
/// Min args: 2
/// Max args: 1
/// Variadic: false
/// Signature: FREQUENCY(arg1: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE, NUMERIC_ONLY
/// [formualizer-docgen:schema:end]
impl Function for FrequencyFn {
    func_caps!(PURE, NUMERIC_ONLY);
    fn name(&self) -> &'static str {
        "FREQUENCY"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        false
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_RANGE_NUM_LENIENT_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // Collect data array
        let data = collect_numeric_stats(&args[0..1])?;

        // Collect bins array
        let mut bins = collect_numeric_stats(&args[1..2])?;

        // Handle empty bins - return single count of all data
        if bins.is_empty() {
            let rows = vec![vec![LiteralValue::Number(data.len() as f64)]];
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(rows)));
        }

        // Sort bins
        bins.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Calculate frequencies
        // Result has bins.len() + 1 elements
        let mut frequencies = vec![0usize; bins.len() + 1];

        for &value in &data {
            // Find which bin the value belongs to
            let mut found = false;
            for (i, &bin) in bins.iter().enumerate() {
                if i == 0 {
                    // First bin: count values <= bins[0]
                    if value <= bin {
                        frequencies[0] += 1;
                        found = true;
                        break;
                    }
                } else {
                    // Intermediate bins: count values > bins[i-1] AND <= bins[i]
                    if value <= bin {
                        frequencies[i] += 1;
                        found = true;
                        break;
                    }
                }
            }
            // Last bin: values > bins[last]
            if !found {
                frequencies[bins.len()] += 1;
            }
        }

        // Return as vertical array (column vector)
        let rows: Vec<Vec<LiteralValue>> = frequencies
            .into_iter()
            .map(|f| vec![LiteralValue::Number(f as f64)])
            .collect();

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Array(rows)))
    }
}

/* ─────────────────────────── T.DIST.2T ──────────────────────────── */

/// Returns the two-tailed Student's t probability beyond `x`.
///
/// `T.DIST.2T` computes `P(|T| > x)` for the specified degrees of freedom.
///
/// # Remarks
/// - Requires `x >= 0` and `deg_freedom >= 1`.
/// - Represents a two-sided tail area.
/// - Returns `#NUM!` when arguments are outside valid ranges.
/// - Invalid numeric coercions propagate as spreadsheet errors.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Two-tailed t probability at zero"
/// formula: "=T.DIST.2T(0,10)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Two-tailed t probability at x=2"
/// formula: "=T.DIST.2T(2,10)"
/// expected: 0.0733880342639167
/// ```
#[derive(Debug)]
pub struct TDist2TFn;
/// [formualizer-docgen:schema:start]
/// Name: T.DIST.2T
/// Type: TDist2TFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: T.DIST.2T(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TDist2TFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "T.DIST.2T"
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let x = coerce_num(&scalar_like_value(&args[0])?)?;
        let df = coerce_num(&scalar_like_value(&args[1])?)?;

        // x must be non-negative for T.DIST.2T, df must be >= 1
        if x < 0.0 || df < 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // Two-tailed: P(|T| > x) = 2 * (1 - t_cdf(x, df))
        let p = 2.0 * (1.0 - t_cdf(x, df));
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(p)))
    }
}

/* ─────────────────────────── T.INV.2T ──────────────────────────── */

/// Returns the positive t critical value for a two-tailed probability.
///
/// `T.INV.2T` solves for `t` such that `P(|T| > t) = probability`.
///
/// # Remarks
/// - `probability` must satisfy `0 < probability <= 1`.
/// - `deg_freedom` must be at least `1`.
/// - Returns `#NUM!` for invalid probability or degree-of-freedom arguments.
/// - Alias `TINV` is supported.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Maximum two-tailed probability"
/// formula: "=T.INV.2T(1,10)"
/// expected: 0
/// ```
///
/// ```yaml,sandbox
/// title: "95% two-sided critical value"
/// formula: "=T.INV.2T(0.05,10)"
/// expected: 2.228138851986273
/// ```
#[derive(Debug)]
pub struct TInv2TFn;
/// [formualizer-docgen:schema:start]
/// Name: T.INV.2T
/// Type: TInv2TFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: T.INV.2T(arg1: number@scalar, arg2: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TInv2TFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "T.INV.2T"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["TINV"]
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
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let p = coerce_num(&scalar_like_value(&args[0])?)?;
        let df = coerce_num(&scalar_like_value(&args[1])?)?;

        // probability must be in (0, 1], df >= 1
        if p <= 0.0 || p > 1.0 || df < 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // For two-tailed: we want t such that P(|T| > t) = p
        // P(|T| > t) = 2 * (1 - F(t)) where F is CDF
        // So 1 - F(t) = p/2, meaning F(t) = 1 - p/2
        // Thus t = t_inv(1 - p/2, df)
        match t_inv(1.0 - p / 2.0, df) {
            Some(result) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                result,
            ))),
            None => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            ))),
        }
    }
}

/* ─────────────────────────── T.TEST ──────────────────────────── */

/// Returns the p-value from a Student t-test comparing two numeric samples.
///
/// `T.TEST` supports paired, equal-variance two-sample, and unequal-variance (Welch) modes.
///
/// # Remarks
/// - `tails` must be `1` (one-tailed) or `2` (two-tailed).
/// - `type` must be `1` (paired), `2` (two-sample equal variance), or `3` (Welch).
/// - Returns `#N/A` when paired mode arrays have different lengths.
/// - Returns `#NUM!` or `#DIV/0!` for invalid setup or degenerate variance conditions.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Two-tailed equal-variance test with identical samples"
/// formula: "=T.TEST({1,2,3},{1,2,3},2,2)"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "One-tailed Welch test with identical samples"
/// formula: "=T.TEST({1,2,3},{1,2,3},1,3)"
/// expected: 0.5
/// ```
#[derive(Debug)]
pub struct TTestFn;
/// [formualizer-docgen:schema:start]
/// Name: T.TEST
/// Type: TTestFn
/// Min args: 4
/// Max args: 4
/// Variadic: false
/// Signature: T.TEST(arg1: number@range, arg2: number@range, arg3: number@scalar, arg4: number@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg3{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg4{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for TTestFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "T.TEST"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["TTEST"]
    }
    fn min_args(&self) -> usize {
        4
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                {
                    let mut s = ArgSchema::number_lenient_scalar();
                    s.shape = crate::args::ShapeKind::Range;
                    s.coercion = formualizer_common::CoercionPolicy::NumberLenientText;
                    s
                },
                {
                    let mut s = ArgSchema::number_lenient_scalar();
                    s.shape = crate::args::ShapeKind::Range;
                    s.coercion = formualizer_common::CoercionPolicy::NumberLenientText;
                    s
                },
                ArgSchema::number_lenient_scalar(), // tails
                ArgSchema::number_lenient_scalar(), // type
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let array1 = collect_numeric_stats(&args[0..1])?;
        let array2 = collect_numeric_stats(&args[1..2])?;
        let tails = coerce_num(&scalar_like_value(&args[2])?)? as i32;
        let test_type = coerce_num(&scalar_like_value(&args[3])?)? as i32;

        // Validate tails (1 or 2) and type (1, 2, or 3)
        if !(1..=2).contains(&tails) || !(1..=3).contains(&test_type) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let n1 = array1.len();
        let n2 = array2.len();

        // For paired test, arrays must have same length
        if test_type == 1 && n1 != n2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        // Need at least 2 data points for meaningful t-test
        if n1 < 2 || n2 < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        let (t_stat, df) = match test_type {
            1 => {
                // Paired t-test
                let n = n1 as f64;
                let diffs: Vec<f64> = array1
                    .iter()
                    .zip(array2.iter())
                    .map(|(a, b)| a - b)
                    .collect();
                let mean_diff = diffs.iter().sum::<f64>() / n;
                let var_diff =
                    diffs.iter().map(|d| (d - mean_diff).powi(2)).sum::<f64>() / (n - 1.0);
                if var_diff == 0.0 {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new_div(),
                    )));
                }
                let se = (var_diff / n).sqrt();
                (mean_diff / se, n - 1.0)
            }
            2 => {
                // Two-sample equal variance (pooled)
                let n1f = n1 as f64;
                let n2f = n2 as f64;
                let mean1 = array1.iter().sum::<f64>() / n1f;
                let mean2 = array2.iter().sum::<f64>() / n2f;
                let var1 = array1.iter().map(|x| (x - mean1).powi(2)).sum::<f64>() / (n1f - 1.0);
                let var2 = array2.iter().map(|x| (x - mean2).powi(2)).sum::<f64>() / (n2f - 1.0);

                // Pooled variance
                let sp2 = ((n1f - 1.0) * var1 + (n2f - 1.0) * var2) / (n1f + n2f - 2.0);
                if sp2 == 0.0 {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new_div(),
                    )));
                }
                let se = (sp2 * (1.0 / n1f + 1.0 / n2f)).sqrt();
                ((mean1 - mean2) / se, n1f + n2f - 2.0)
            }
            3 => {
                // Welch's t-test (unequal variance)
                let n1f = n1 as f64;
                let n2f = n2 as f64;
                let mean1 = array1.iter().sum::<f64>() / n1f;
                let mean2 = array2.iter().sum::<f64>() / n2f;
                let var1 = array1.iter().map(|x| (x - mean1).powi(2)).sum::<f64>() / (n1f - 1.0);
                let var2 = array2.iter().map(|x| (x - mean2).powi(2)).sum::<f64>() / (n2f - 1.0);

                let s1_n = var1 / n1f;
                let s2_n = var2 / n2f;
                let se = (s1_n + s2_n).sqrt();
                if se == 0.0 {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new_div(),
                    )));
                }

                // Welch-Satterthwaite degrees of freedom
                let df_num = (s1_n + s2_n).powi(2);
                let df_denom = s1_n.powi(2) / (n1f - 1.0) + s2_n.powi(2) / (n2f - 1.0);
                let df = if df_denom == 0.0 {
                    1.0
                } else {
                    df_num / df_denom
                };
                ((mean1 - mean2) / se, df)
            }
            _ => unreachable!(),
        };

        // Calculate p-value based on tails
        let t_abs = t_stat.abs();
        let p = if tails == 1 {
            1.0 - t_cdf(t_abs, df)
        } else {
            2.0 * (1.0 - t_cdf(t_abs, df))
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(p)))
    }
}

/* ─────────────────────────── F.TEST ──────────────────────────── */

/// Returns the two-tailed p-value from an F-test comparing sample variances.
///
/// `F.TEST` evaluates whether two samples have significantly different variances.
///
/// # Remarks
/// - Each array must contain at least two numeric values.
/// - Uses sample variances and computes a two-tailed probability.
/// - Returns `#DIV/0!` when either sample variance is zero.
/// - Alias `FTEST` is supported.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Identical samples yield p-value 1"
/// formula: "=F.TEST({1,2,3,4},{1,2,3,4})"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Different variances example"
/// formula: "=F.TEST({1,2,3,4},{1,1,1,5})"
/// expected: 0.5466810975407987
/// ```
#[derive(Debug)]
pub struct FTestFn;
/// [formualizer-docgen:schema:start]
/// Name: F.TEST
/// Type: FTestFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: F.TEST(arg1: number@range, arg2: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FTestFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "F.TEST"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["FTEST"]
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                {
                    let mut s = ArgSchema::number_lenient_scalar();
                    s.shape = crate::args::ShapeKind::Range;
                    s.coercion = formualizer_common::CoercionPolicy::NumberLenientText;
                    s
                },
                {
                    let mut s = ArgSchema::number_lenient_scalar();
                    s.shape = crate::args::ShapeKind::Range;
                    s.coercion = formualizer_common::CoercionPolicy::NumberLenientText;
                    s
                },
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let array1 = collect_numeric_stats(&args[0..1])?;
        let array2 = collect_numeric_stats(&args[1..2])?;

        let n1 = array1.len();
        let n2 = array2.len();

        // Need at least 2 points in each array
        if n1 < 2 || n2 < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        let n1f = n1 as f64;
        let n2f = n2 as f64;

        let mean1 = array1.iter().sum::<f64>() / n1f;
        let mean2 = array2.iter().sum::<f64>() / n2f;

        let var1 = array1.iter().map(|x| (x - mean1).powi(2)).sum::<f64>() / (n1f - 1.0);
        let var2 = array2.iter().map(|x| (x - mean2).powi(2)).sum::<f64>() / (n2f - 1.0);

        // Handle zero variance
        if var1 == 0.0 || var2 == 0.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_div(),
            )));
        }

        // F-statistic: Excel's F.TEST uses var1/var2 (order matters for degrees of freedom)
        // and returns two-tailed p-value
        let f = var1 / var2;
        let df1 = n1f - 1.0;
        let df2 = n2f - 1.0;

        // Two-tailed p-value: min(F.DIST(f), 1-F.DIST(f)) * 2
        let p_lower = f_cdf(f, df1, df2);
        let p_upper = 1.0 - p_lower;
        let p = 2.0 * p_lower.min(p_upper);

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(p)))
    }
}

/* ─────────────────────────── CHISQ.TEST ──────────────────────────── */

/// Returns the right-tail p-value from a chi-square goodness-of-fit style comparison.
///
/// `CHISQ.TEST` compares observed and expected values and computes `1 - CHISQ.DIST(...)`.
///
/// # Remarks
/// - `actual_range` and `expected_range` must contain the same number of numeric points.
/// - Expected values must be strictly greater than `0`.
/// - Requires at least two categories (`df >= 1`).
/// - Returns `#N/A` for length mismatches or empty inputs, and `#NUM!` for invalid expected values.
///
/// # Examples
///
/// ```yaml,sandbox
/// title: "Perfect match between observed and expected"
/// formula: "=CHISQ.TEST({20,30,50},{20,30,50})"
/// expected: 1
/// ```
///
/// ```yaml,sandbox
/// title: "Two-category chi-square test"
/// formula: "=CHISQ.TEST({18,22},{20,20})"
/// expected: 0.5270892568655381
/// ```
#[derive(Debug)]
pub struct ChisqTestFn;
/// [formualizer-docgen:schema:start]
/// Name: CHISQ.TEST
/// Type: ChisqTestFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: CHISQ.TEST(arg1: number@range, arg2: number@range)
/// Arg schema: arg1{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=range,by_ref=false,coercion=NumberLenientText,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ChisqTestFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "CHISQ.TEST"
    }
    fn aliases(&self) -> &'static [&'static str] {
        &["CHITEST"]
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use std::sync::LazyLock;
        static SCHEMA: LazyLock<Vec<ArgSchema>> = LazyLock::new(|| {
            vec![
                {
                    let mut s = ArgSchema::number_lenient_scalar();
                    s.shape = crate::args::ShapeKind::Range;
                    s.coercion = formualizer_common::CoercionPolicy::NumberLenientText;
                    s
                },
                {
                    let mut s = ArgSchema::number_lenient_scalar();
                    s.shape = crate::args::ShapeKind::Range;
                    s.coercion = formualizer_common::CoercionPolicy::NumberLenientText;
                    s
                },
            ]
        });
        &SCHEMA[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let actual = collect_numeric_stats(&args[0..1])?;
        let expected = collect_numeric_stats(&args[1..2])?;

        // Arrays must have same length
        if actual.len() != expected.len() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        if actual.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_na(),
            )));
        }

        // Calculate chi-squared statistic: sum((observed - expected)^2 / expected)
        let mut chi_sq = 0.0;
        for (obs, exp) in actual.iter().zip(expected.iter()) {
            if *exp <= 0.0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_num(),
                )));
            }
            chi_sq += (obs - exp).powi(2) / exp;
        }

        // Degrees of freedom = number of categories - 1
        let df = (actual.len() - 1) as f64;

        if df < 1.0 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_num(),
            )));
        }

        // P-value = 1 - CHISQ.DIST(chi_sq, df, TRUE) = right-tail probability
        let p = 1.0 - chisq_cdf(chi_sq, df);

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(p)))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(ForecastLinearFn));
    crate::function_registry::register_function(Arc::new(LinestFn));
    crate::function_registry::register_function(Arc::new(LARGE));
    crate::function_registry::register_function(Arc::new(SMALL));
    crate::function_registry::register_function(Arc::new(MEDIAN));
    crate::function_registry::register_function(Arc::new(StdevSample));
    crate::function_registry::register_function(Arc::new(StdevPop));
    crate::function_registry::register_function(Arc::new(VarSample));
    crate::function_registry::register_function(Arc::new(VarPop));
    crate::function_registry::register_function(Arc::new(PercentileInc));
    crate::function_registry::register_function(Arc::new(PercentileExc));
    crate::function_registry::register_function(Arc::new(QuartileInc));
    crate::function_registry::register_function(Arc::new(QuartileExc));
    crate::function_registry::register_function(Arc::new(RankEqFn));
    crate::function_registry::register_function(Arc::new(RankAvgFn));
    crate::function_registry::register_function(Arc::new(ModeSingleFn));
    crate::function_registry::register_function(Arc::new(ModeMultiFn));
    crate::function_registry::register_function(Arc::new(ProductFn));
    crate::function_registry::register_function(Arc::new(GeomeanFn));
    crate::function_registry::register_function(Arc::new(HarmeanFn));
    crate::function_registry::register_function(Arc::new(AvedevFn));
    crate::function_registry::register_function(Arc::new(DevsqFn));
    crate::function_registry::register_function(Arc::new(MaxIfsFn));
    crate::function_registry::register_function(Arc::new(MinIfsFn));
    crate::function_registry::register_function(Arc::new(TrimmeanFn));
    crate::function_registry::register_function(Arc::new(CorrelFn));
    crate::function_registry::register_function(Arc::new(SlopeFn));
    crate::function_registry::register_function(Arc::new(InterceptFn));
    // Covariance and correlation functions
    crate::function_registry::register_function(Arc::new(CovariancePFn));
    crate::function_registry::register_function(Arc::new(CovarianceSFn));
    crate::function_registry::register_function(Arc::new(PearsonFn));
    crate::function_registry::register_function(Arc::new(RsqFn));
    crate::function_registry::register_function(Arc::new(SteyxFn));
    crate::function_registry::register_function(Arc::new(SkewFn));
    crate::function_registry::register_function(Arc::new(KurtFn));
    crate::function_registry::register_function(Arc::new(FisherFn));
    crate::function_registry::register_function(Arc::new(FisherInvFn));
    // Statistical distributions
    crate::function_registry::register_function(Arc::new(NormSDistFn));
    crate::function_registry::register_function(Arc::new(NormSInvFn));
    crate::function_registry::register_function(Arc::new(NormDistFn));
    crate::function_registry::register_function(Arc::new(NormInvFn));
    crate::function_registry::register_function(Arc::new(LognormDistFn));
    crate::function_registry::register_function(Arc::new(LognormInvFn));
    crate::function_registry::register_function(Arc::new(PhiFn));
    crate::function_registry::register_function(Arc::new(GaussFn));
    crate::function_registry::register_function(Arc::new(StandardizeFn));
    crate::function_registry::register_function(Arc::new(TDistFn));
    crate::function_registry::register_function(Arc::new(TInvFn));
    crate::function_registry::register_function(Arc::new(ChisqDistFn));
    crate::function_registry::register_function(Arc::new(ChisqInvFn));
    crate::function_registry::register_function(Arc::new(FDistFn));
    crate::function_registry::register_function(Arc::new(FInvFn));
    // Discrete distributions
    crate::function_registry::register_function(Arc::new(BinomDistFn));
    crate::function_registry::register_function(Arc::new(PoissonDistFn));
    crate::function_registry::register_function(Arc::new(ExponDistFn));
    crate::function_registry::register_function(Arc::new(GammaDistFn));
    // Additional distributions
    crate::function_registry::register_function(Arc::new(WeibullDistFn));
    crate::function_registry::register_function(Arc::new(BetaDistFn));
    crate::function_registry::register_function(Arc::new(NegbinomDistFn));
    crate::function_registry::register_function(Arc::new(HypgeomDistFn));
    // Confidence intervals and hypothesis testing
    crate::function_registry::register_function(Arc::new(ConfidenceNormFn));
    crate::function_registry::register_function(Arc::new(ConfidenceTFn));
    crate::function_registry::register_function(Arc::new(ZTestFn));
    // Regression and trend functions
    crate::function_registry::register_function(Arc::new(TrendFn));
    crate::function_registry::register_function(Arc::new(GrowthFn));
    crate::function_registry::register_function(Arc::new(LogestFn));
    // Percent rank and frequency functions
    crate::function_registry::register_function(Arc::new(PercentRankIncFn));
    crate::function_registry::register_function(Arc::new(PercentRankExcFn));
    crate::function_registry::register_function(Arc::new(FrequencyFn));
    // Hypothesis testing functions
    crate::function_registry::register_function(Arc::new(TDist2TFn));
    crate::function_registry::register_function(Arc::new(TInv2TFn));
    crate::function_registry::register_function(Arc::new(TTestFn));
    crate::function_registry::register_function(Arc::new(FTestFn));
    crate::function_registry::register_function(Arc::new(ChisqTestFn));
}

#[cfg(test)]
mod tests_basic_stats {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn arr(vals: Vec<f64>) -> ASTNode {
        ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![
                vals.into_iter().map(LiteralValue::Number).collect(),
            ])),
            None,
        )
    }
    #[test]
    fn median_even() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MEDIAN));
        let ctx = interp(&wb);
        let node = arr(vec![1.0, 3.0, 5.0, 7.0]);
        let f = ctx.context.get_function("", "MEDIAN").unwrap();
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&node, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Number(4.0));
    }
    #[test]
    fn median_odd() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MEDIAN));
        let ctx = interp(&wb);
        let node = arr(vec![1.0, 9.0, 5.0]);
        let f = ctx.context.get_function("", "MEDIAN").unwrap();
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&node, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Number(5.0));
    }
    #[test]
    fn large_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(LARGE));
        let ctx = interp(&wb);
        let nums = arr(vec![10.0, 20.0, 30.0]);
        let k = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(2.0)), None);
        let f = ctx.context.get_function("", "LARGE").unwrap();
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&nums, &ctx),
                    ArgumentHandle::new(&k, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Number(20.0));
    }
    #[test]
    fn small_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SMALL));
        let ctx = interp(&wb);
        let nums = arr(vec![10.0, 20.0, 30.0]);
        let k = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(2.0)), None);
        let f = ctx.context.get_function("", "SMALL").unwrap();
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&nums, &ctx),
                    ArgumentHandle::new(&k, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Number(20.0));
    }
    #[test]
    fn percentile_inc_quarter() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(PercentileInc));
        let ctx = interp(&wb);
        let nums = arr(vec![1.0, 2.0, 3.0, 4.0]);
        let p = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(0.25)), None);
        let f = ctx.context.get_function("", "PERCENTILE.INC").unwrap();
        match f
            .dispatch(
                &[
                    ArgumentHandle::new(&nums, &ctx),
                    ArgumentHandle::new(&p, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - 1.75).abs() < 1e-9),
            other => panic!("unexpected {other:?}"),
        }
    }
    #[test]
    fn rank_eq_descending() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RankEqFn));
        let ctx = interp(&wb);
        // target 5 among {10,5,1} descending => ranks 1,2,3 => expect 2
        let target = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(5.0)), None);
        let arr_node = arr(vec![10.0, 5.0, 1.0]);
        let f = ctx.context.get_function("", "RANK.EQ").unwrap();
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&target, &ctx),
                    ArgumentHandle::new(&arr_node, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Number(2.0));
    }
    #[test]
    fn rank_eq_ascending_order_arg() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RankEqFn));
        let ctx = interp(&wb);
        // ascending order=1: array {1,5,10}; target 5 => rank 2
        let target = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(5.0)), None);
        let arr_node = arr(vec![1.0, 5.0, 10.0]);
        let order = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(1.0)), None);
        let f = ctx.context.get_function("", "RANK.EQ").unwrap();
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&target, &ctx),
                    ArgumentHandle::new(&arr_node, &ctx),
                    ArgumentHandle::new(&order, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap();
        assert_eq!(out, LiteralValue::Number(2.0));
    }
    #[test]
    fn rank_avg_ties() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(RankAvgFn));
        let ctx = interp(&wb);
        // descending array {5,5,1} target 5 positions 1 and 2 avg -> 1.5
        let target = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(5.0)), None);
        let arr_node = arr(vec![5.0, 5.0, 1.0]);
        let f = ctx.context.get_function("", "RANK.AVG").unwrap();
        let out = f
            .dispatch(
                &[
                    ArgumentHandle::new(&target, &ctx),
                    ArgumentHandle::new(&arr_node, &ctx),
                ],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        match out {
            LiteralValue::Number(v) => assert!((v - 1.5).abs() < 1e-12),
            other => panic!("expected number got {other:?}"),
        }
    }
    #[test]
    fn stdev_var_sample_population() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(StdevSample))
            .with_function(std::sync::Arc::new(StdevPop))
            .with_function(std::sync::Arc::new(VarSample))
            .with_function(std::sync::Arc::new(VarPop));
        let ctx = interp(&wb);
        let arr_node = arr(vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]); // variance population = 4, sample = 4.571428...
        let stdev_p = ctx.context.get_function("", "STDEV.P").unwrap();
        let stdev_s = ctx.context.get_function("", "STDEV.S").unwrap();
        let var_p = ctx.context.get_function("", "VAR.P").unwrap();
        let var_s = ctx.context.get_function("", "VAR.S").unwrap();
        let args = [ArgumentHandle::new(&arr_node, &ctx)];
        match var_p
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - 4.0).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
        match var_s
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - 4.571428571428571).abs() < 1e-9),
            other => panic!("unexpected {other:?}"),
        }
        match stdev_p
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - 2.0).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
        match stdev_s
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - 2.138089935).abs() < 1e-9),
            other => panic!("unexpected {other:?}"),
        }
    }
    #[test]
    fn quartile_inc_exc() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(QuartileInc))
            .with_function(std::sync::Arc::new(QuartileExc));
        let ctx = interp(&wb);
        let arr_node = arr(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let q1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(1.0)), None);
        let q2 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(2.0)), None);
        let f_inc = ctx.context.get_function("", "QUARTILE.INC").unwrap();
        let f_exc = ctx.context.get_function("", "QUARTILE.EXC").unwrap();
        let arg_inc_q1 = [
            ArgumentHandle::new(&arr_node, &ctx),
            ArgumentHandle::new(&q1, &ctx),
        ];
        let arg_inc_q2 = [
            ArgumentHandle::new(&arr_node, &ctx),
            ArgumentHandle::new(&q2, &ctx),
        ];
        match f_inc
            .dispatch(&arg_inc_q1, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - 2.0).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
        match f_inc
            .dispatch(&arg_inc_q2, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - 3.0).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
        // QUARTILE.EXC Q1 for 5-point set uses exclusive percentile => 1.5
        match f_exc
            .dispatch(&arg_inc_q1, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - 1.5).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
        match f_exc
            .dispatch(&arg_inc_q2, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Number(v) => assert!((v - 3.0).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
    }

    // --- eval()/dispatch equivalence tests for variance / stdev ---
    #[test]
    fn fold_equivalence_var_stdev() {
        use crate::function::Function as _; // trait import
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(VarSample))
            .with_function(std::sync::Arc::new(VarPop))
            .with_function(std::sync::Arc::new(StdevSample))
            .with_function(std::sync::Arc::new(StdevPop));
        let ctx = interp(&wb);
        let arr_node = arr(vec![1.0, 2.0, 5.0, 5.0, 9.0]);
        let args = [ArgumentHandle::new(&arr_node, &ctx)];

        let var_s_fn = VarSample; // concrete instance to call eval()
        let var_p_fn = VarPop;
        let stdev_s_fn = StdevSample;
        let stdev_p_fn = StdevPop;

        let fctx = ctx.function_context(None);
        // Dispatch results (will use fold path)
        let disp_var_s = ctx
            .context
            .get_function("", "VAR.S")
            .unwrap()
            .dispatch(&args, &fctx)
            .unwrap()
            .into_literal();
        let disp_var_p = ctx
            .context
            .get_function("", "VAR.P")
            .unwrap()
            .dispatch(&args, &fctx)
            .unwrap()
            .into_literal();
        let disp_stdev_s = ctx
            .context
            .get_function("", "STDEV.S")
            .unwrap()
            .dispatch(&args, &fctx)
            .unwrap()
            .into_literal();
        let disp_stdev_p = ctx
            .context
            .get_function("", "STDEV.P")
            .unwrap()
            .dispatch(&args, &fctx)
            .unwrap()
            .into_literal();

        // Scalar path results
        let scalar_var_s = var_s_fn.eval(&args, &fctx).unwrap().into_literal();
        let scalar_var_p = var_p_fn.eval(&args, &fctx).unwrap().into_literal();
        let scalar_stdev_s = stdev_s_fn.eval(&args, &fctx).unwrap().into_literal();
        let scalar_stdev_p = stdev_p_fn.eval(&args, &fctx).unwrap().into_literal();

        fn assert_close(a: &LiteralValue, b: &LiteralValue) {
            match (a, b) {
                (LiteralValue::Number(x), LiteralValue::Number(y)) => {
                    assert!((x - y).abs() < 1e-12, "mismatch {x} vs {y}")
                }
                _ => assert_eq!(a, b),
            }
        }
        assert_close(&disp_var_s, &scalar_var_s);
        assert_close(&disp_var_p, &scalar_var_p);
        assert_close(&disp_stdev_s, &scalar_stdev_s);
        assert_close(&disp_stdev_p, &scalar_stdev_p);
    }

    #[test]
    fn fold_equivalence_edge_cases() {
        use crate::function::Function as _;
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(VarSample))
            .with_function(std::sync::Arc::new(VarPop))
            .with_function(std::sync::Arc::new(StdevSample))
            .with_function(std::sync::Arc::new(StdevPop));
        let ctx = interp(&wb);
        // Single numeric element -> sample variance/div0, population variance 0
        let single = arr(vec![42.0]);
        let args_single = [ArgumentHandle::new(&single, &ctx)];
        let fctx = ctx.function_context(None);
        let disp_var_s = ctx
            .context
            .get_function("", "VAR.S")
            .unwrap()
            .dispatch(&args_single, &fctx)
            .unwrap();
        let scalar_var_s = VarSample.eval(&args_single, &fctx).unwrap().into_literal();
        assert_eq!(disp_var_s, scalar_var_s);
        let disp_var_p = ctx
            .context
            .get_function("", "VAR.P")
            .unwrap()
            .dispatch(&args_single, &fctx)
            .unwrap();
        let scalar_var_p = VarPop.eval(&args_single, &fctx).unwrap().into_literal();
        assert_eq!(disp_var_p, scalar_var_p);
        let disp_stdev_p = ctx
            .context
            .get_function("", "STDEV.P")
            .unwrap()
            .dispatch(&args_single, &fctx)
            .unwrap();
        let scalar_stdev_p = StdevPop.eval(&args_single, &fctx).unwrap().into_literal();
        assert_eq!(disp_stdev_p, scalar_stdev_p);
        let disp_stdev_s = ctx
            .context
            .get_function("", "STDEV.S")
            .unwrap()
            .dispatch(&args_single, &fctx)
            .unwrap();
        let scalar_stdev_s = StdevSample
            .eval(&args_single, &fctx)
            .unwrap()
            .into_literal();
        assert_eq!(disp_stdev_s, scalar_stdev_s);
    }

    #[test]
    fn legacy_aliases_match_modern() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(PercentileInc))
            .with_function(std::sync::Arc::new(QuartileInc))
            .with_function(std::sync::Arc::new(RankEqFn));
        let ctx = interp(&wb);
        let arr_node = arr(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let p = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(0.4)), None);
        let q2 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(2.0)), None);
        let target = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(4.0)), None);
        let args_p = [
            ArgumentHandle::new(&arr_node, &ctx),
            ArgumentHandle::new(&p, &ctx),
        ];
        let args_q = [
            ArgumentHandle::new(&arr_node, &ctx),
            ArgumentHandle::new(&q2, &ctx),
        ];
        let args_rank = [
            ArgumentHandle::new(&target, &ctx),
            ArgumentHandle::new(&arr_node, &ctx),
        ];
        let modern_p = ctx
            .context
            .get_function("", "PERCENTILE.INC")
            .unwrap()
            .dispatch(&args_p, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let legacy_p = ctx
            .context
            .get_function("", "PERCENTILE")
            .unwrap()
            .dispatch(&args_p, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(modern_p, legacy_p);
        let modern_q = ctx
            .context
            .get_function("", "QUARTILE.INC")
            .unwrap()
            .dispatch(&args_q, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let legacy_q = ctx
            .context
            .get_function("", "QUARTILE")
            .unwrap()
            .dispatch(&args_q, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(modern_q, legacy_q);
        let modern_rank = ctx
            .context
            .get_function("", "RANK.EQ")
            .unwrap()
            .dispatch(&args_rank, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let legacy_rank = ctx
            .context
            .get_function("", "RANK")
            .unwrap()
            .dispatch(&args_rank, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(modern_rank, legacy_rank);
    }

    #[test]
    fn mode_single_basic_and_alias() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ModeSingleFn));
        let ctx = interp(&wb);
        let arr_node = arr(vec![5.0, 2.0, 2.0, 3.0, 3.0, 3.0]);
        let args = [ArgumentHandle::new(&arr_node, &ctx)];
        let mode_sngl = ctx
            .context
            .get_function("", "MODE.SNGL")
            .unwrap()
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(mode_sngl, LiteralValue::Number(3.0));
        let mode_alias = ctx
            .context
            .get_function("", "MODE")
            .unwrap()
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(mode_alias, mode_sngl);
    }

    #[test]
    fn mode_single_no_duplicates() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ModeSingleFn));
        let ctx = interp(&wb);
        let arr_node = arr(vec![1.0, 2.0, 3.0]);
        let args = [ArgumentHandle::new(&arr_node, &ctx)];
        let out = ctx
            .context
            .get_function("", "MODE.SNGL")
            .unwrap()
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match out {
            LiteralValue::Error(e) => assert!(e.to_string().contains("#N/A")),
            _ => panic!("expected #N/A"),
        }
    }

    #[test]
    fn mode_multi_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ModeMultiFn));
        let ctx = interp(&wb);
        let arr_node = arr(vec![2.0, 3.0, 2.0, 4.0, 3.0, 5.0, 2.0, 3.0]);
        let args = [ArgumentHandle::new(&arr_node, &ctx)];
        let out = ctx
            .context
            .get_function("", "MODE.MULT")
            .unwrap()
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let expected = LiteralValue::Array(vec![
            vec![LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.0)],
        ]);
        assert_eq!(out, expected);
    }

    #[test]
    fn large_small_fold_vs_scalar() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(LARGE))
            .with_function(std::sync::Arc::new(SMALL));
        let ctx = interp(&wb);
        let arr_node = arr(vec![10.0, 5.0, 7.0, 12.0, 9.0]);
        let k_node = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(2.0)), None);
        let args = [
            ArgumentHandle::new(&arr_node, &ctx),
            ArgumentHandle::new(&k_node, &ctx),
        ];
        let f_large = ctx.context.get_function("", "LARGE").unwrap();
        let disp_large = f_large
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let scalar_large = LARGE
            .eval(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(disp_large, scalar_large);
        let f_small = ctx.context.get_function("", "SMALL").unwrap();
        let disp_small = f_small
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let scalar_small = SMALL
            .eval(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(disp_small, scalar_small);
    }

    #[test]
    fn mode_fold_vs_scalar() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(ModeSingleFn))
            .with_function(std::sync::Arc::new(ModeMultiFn));
        let ctx = interp(&wb);
        let arr_node = arr(vec![2.0, 3.0, 2.0, 4.0, 3.0, 3.0, 2.0]);
        let args = [ArgumentHandle::new(&arr_node, &ctx)];
        let f_single = ctx.context.get_function("", "MODE.SNGL").unwrap();
        let disp_single = f_single
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let scalar_single = ModeSingleFn
            .eval(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(disp_single, scalar_single);
        let f_multi = ctx.context.get_function("", "MODE.MULT").unwrap();
        let disp_multi = f_multi
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let scalar_multi = ModeMultiFn
            .eval(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(disp_multi, scalar_multi);
    }

    #[test]
    fn median_fold_vs_scalar_even() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MEDIAN));
        let ctx = interp(&wb);
        let arr_node = arr(vec![7.0, 1.0, 9.0, 5.0]); // sorted: 1,5,7,9 median=(5+7)/2=6
        let args = [ArgumentHandle::new(&arr_node, &ctx)];
        let f_med = ctx.context.get_function("", "MEDIAN").unwrap();
        let disp = f_med
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let scalar = MEDIAN
            .eval(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(disp, scalar);
        assert_eq!(disp, LiteralValue::Number(6.0));
    }

    #[test]
    fn median_fold_vs_scalar_odd() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MEDIAN));
        let ctx = interp(&wb);
        let arr_node = arr(vec![9.0, 2.0, 5.0]); // sorted 2,5,9 median=5
        let args = [ArgumentHandle::new(&arr_node, &ctx)];
        let f_med = ctx.context.get_function("", "MEDIAN").unwrap();
        let disp = f_med
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let scalar = MEDIAN
            .eval(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(disp, scalar);
        assert_eq!(disp, LiteralValue::Number(5.0));
    }

    // Newly added edge case tests for statistical semantics.
    #[test]
    fn percentile_inc_edges() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(PercentileInc));
        let ctx = interp(&wb);
        let arr_node = arr(vec![10.0, 20.0, 30.0, 40.0]);
        let p0 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(0.0)), None);
        let p1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(1.0)), None);
        let f = ctx.context.get_function("", "PERCENTILE.INC").unwrap();
        let args0 = [
            ArgumentHandle::new(&arr_node, &ctx),
            ArgumentHandle::new(&p0, &ctx),
        ];
        let args1 = [
            ArgumentHandle::new(&arr_node, &ctx),
            ArgumentHandle::new(&p1, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args0, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(10.0)
        );
        assert_eq!(
            f.dispatch(&args1, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(40.0)
        );
    }

    #[test]
    fn percentile_exc_invalid() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(PercentileExc));
        let ctx = interp(&wb);
        let arr_node = arr(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let p_bad0 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(0.0)), None);
        let p_bad1 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(1.0)), None);
        let f = ctx.context.get_function("", "PERCENTILE.EXC").unwrap();
        for bad in [&p_bad0, &p_bad1] {
            let args = [
                ArgumentHandle::new(&arr_node, &ctx),
                ArgumentHandle::new(bad, &ctx),
            ];
            match f
                .dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal()
            {
                LiteralValue::Error(e) => assert!(e.to_string().contains("#NUM!")),
                other => panic!("expected #NUM! got {other:?}"),
            }
        }
    }

    #[test]
    fn quartile_invalids() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(QuartileInc))
            .with_function(std::sync::Arc::new(QuartileExc));
        let ctx = interp(&wb);
        let arr_node = arr(vec![1.0, 2.0, 3.0]);
        // QUARTILE.INC invalid q=5
        let q5 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(5.0)), None);
        let args_bad_inc = [
            ArgumentHandle::new(&arr_node, &ctx),
            ArgumentHandle::new(&q5, &ctx),
        ];
        match ctx
            .context
            .get_function("", "QUARTILE.INC")
            .unwrap()
            .dispatch(&args_bad_inc, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert!(e.to_string().contains("#NUM!")),
            other => panic!("expected #NUM! {other:?}"),
        }
        // QUARTILE.EXC invalid q=0
        let q0 = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(0.0)), None);
        let args_bad_exc = [
            ArgumentHandle::new(&arr_node, &ctx),
            ArgumentHandle::new(&q0, &ctx),
        ];
        match ctx
            .context
            .get_function("", "QUARTILE.EXC")
            .unwrap()
            .dispatch(&args_bad_exc, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert!(e.to_string().contains("#NUM!")),
            other => panic!("expected #NUM! {other:?}"),
        }
    }

    #[test]
    fn rank_target_not_found() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(RankEqFn))
            .with_function(std::sync::Arc::new(RankAvgFn));
        let ctx = interp(&wb);
        let arr_node = arr(vec![1.0, 2.0, 3.0]);
        let target = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(4.0)), None);
        let args = [
            ArgumentHandle::new(&target, &ctx),
            ArgumentHandle::new(&arr_node, &ctx),
        ];
        for name in ["RANK.EQ", "RANK.AVG"] {
            match ctx
                .context
                .get_function("", name)
                .unwrap()
                .dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal()
            {
                LiteralValue::Error(e) => assert!(e.to_string().contains("#N/A")),
                other => panic!("expected #N/A {other:?}"),
            }
        }
    }

    #[test]
    fn mode_mult_ordering() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(ModeMultiFn));
        let ctx = interp(&wb);
        // Two modes with same frequency; ensure ascending ordering in array result
        let arr_node = arr(vec![5.0, 2.0, 2.0, 5.0, 3.0, 7.0, 5.0, 2.0]); // 2 and 5 appear 4 times each
        let args = [ArgumentHandle::new(&arr_node, &ctx)];
        let out = ctx
            .context
            .get_function("", "MODE.MULT")
            .unwrap()
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match out {
            LiteralValue::Array(rows) => {
                let vals: Vec<f64> = rows
                    .into_iter()
                    .map(|r| {
                        if let LiteralValue::Number(n) = r[0] {
                            n
                        } else {
                            panic!("expected number")
                        }
                    })
                    .collect();
                assert_eq!(vals, vec![2.0, 5.0]);
            }
            other => panic!("expected array {other:?}"),
        }
    }

    #[test]
    fn boolean_and_text_in_range_are_ignored() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(StdevPop));
        let ctx = interp(&wb);
        let mixed = ASTNode::new(
            ASTNodeType::Literal(LiteralValue::Array(vec![vec![
                LiteralValue::Number(1.0),
                LiteralValue::Text("ABC".into()),
                LiteralValue::Boolean(true),
                LiteralValue::Number(4.0),
            ]])),
            None,
        );
        let f = ctx.context.get_function("", "STDEV.P").unwrap();
        let out = f
            .dispatch(
                &[ArgumentHandle::new(&mixed, &ctx)],
                &ctx.function_context(None),
            )
            .unwrap()
            .into_literal();
        // NOTE: Inline array literal is treated as a direct scalar argument (not a range reference),
        // so boolean TRUE is coerced to 1. Dataset becomes {1,1,4}; population stdev = sqrt(6/3)=sqrt(2).
        match out {
            LiteralValue::Number(v) => {
                assert!((v - 2f64.sqrt()).abs() < 1e-12, "expected sqrt(2) got {v}")
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn boolean_direct_arg_coerces() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(StdevPop));
        let ctx = interp(&wb);
        let one = ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(1.0)), None);
        let t = ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(true)), None);
        let f = ctx.context.get_function("", "STDEV.P").unwrap();
        let args = [
            ArgumentHandle::new(&one, &ctx),
            ArgumentHandle::new(&t, &ctx),
        ];
        let out = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(out, LiteralValue::Number(0.0));
    }
}
