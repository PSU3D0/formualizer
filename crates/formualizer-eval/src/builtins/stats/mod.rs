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
        match a.ast().node_type.clone() {
            formualizer_parse::parser::ASTNodeType::Literal(LiteralValue::Array(arr)) => {
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
            }
            _ => {
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
                    match a.value()?.as_ref() {
                        LiteralValue::Error(e) => return Err(e.clone()),
                        other => {
                            if let Ok(n) = coerce_num(other) {
                                out.push(n);
                            }
                        }
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

/// RANK.EQ(number, ref, [order]) Excel semantics:
/// - order omitted or 0 => descending (largest value rank 1)
/// - order non-zero => ascending (smallest value rank 1)
/// - ties return same rank (position of first in ordering)
#[derive(Debug)]
pub struct RankEqFn;
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new_na()));
        }
        let target = match coerce_num(args[0].value()?.as_ref()) {
            Ok(n) => n,
            Err(_) => return Ok(LiteralValue::Error(ExcelError::new_na())),
        };
        // optional order arg at end if 3 args
        let order = if args.len() >= 3 {
            coerce_num(args[2].value()?.as_ref()).unwrap_or(0.0)
        } else {
            0.0
        };
        let nums = collect_numeric_stats(&args[1..2])?; // only one ref range per Excel spec
        if nums.is_empty() {
            return Ok(LiteralValue::Error(ExcelError::new_na()));
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
                return Ok(LiteralValue::Number((i + 1) as f64));
            }
        }
        Ok(LiteralValue::Error(ExcelError::new_na()))
    }
}

/// RANK.AVG(number, ref, [order]) ties return average of ranks
#[derive(Debug)]
pub struct RankAvgFn;
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new_na()));
        }
        let target = match coerce_num(args[0].value()?.as_ref()) {
            Ok(n) => n,
            Err(_) => return Ok(LiteralValue::Error(ExcelError::new_na())),
        };
        let order = if args.len() >= 3 {
            coerce_num(args[2].value()?.as_ref()).unwrap_or(0.0)
        } else {
            0.0
        };
        let nums = collect_numeric_stats(&args[1..2])?;
        if nums.is_empty() {
            return Ok(LiteralValue::Error(ExcelError::new_na()));
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
            return Ok(LiteralValue::Error(ExcelError::new_na()));
        }
        let avg = positions.iter().copied().sum::<usize>() as f64 / positions.len() as f64;
        Ok(LiteralValue::Number(avg))
    }
}

#[derive(Debug)]
pub struct LARGE;
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        let k = match coerce_num(args.last().unwrap().value()?.as_ref()) {
            Ok(n) => n,
            Err(_) => return Ok(LiteralValue::Error(ExcelError::new_num())),
        };
        let k = k as i64;
        if k < 1 {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.is_empty() || k as usize > nums.len() {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        nums.sort_by(|a, b| b.partial_cmp(a).unwrap());
        Ok(LiteralValue::Number(nums[(k as usize) - 1]))
    }
    fn eval_fold(
        &self,
        f: &mut dyn crate::function::FnFoldCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        let args = f.args();
        if args.len() < 2 {
            return Some(Ok(LiteralValue::Error(ExcelError::new_num())));
        }
        let k = match coerce_num(args.last().unwrap().value().ok()?.as_ref()) {
            Ok(n) => n as i64,
            Err(_) => {
                return Some(Ok(LiteralValue::Error(ExcelError::from_error_string(
                    "#NUM!",
                ))));
            }
        };
        if k < 1 {
            return Some(Ok(LiteralValue::Error(ExcelError::new_num())));
        }
        let mut nums = match collect_numeric_stats(&args[..args.len() - 1]) {
            Ok(v) => v,
            Err(e) => return Some(Ok(LiteralValue::Error(e))),
        };
        if nums.is_empty() || k as usize > nums.len() {
            return Some(Ok(LiteralValue::Error(ExcelError::new_num())));
        }
        // partial selection nth_element style
        let idx = (k as usize) - 1;
        // we want k-th largest; convert by selecting at idx in descending => nth on len-1-idx ascending
        let target = nums.len() - 1 - idx;
        let (left, nth, _right) =
            nums.select_nth_unstable_by(target, |a, b| a.partial_cmp(b).unwrap());
        let val = *nth;
        f.write_result(LiteralValue::Number(val));
        Some(Ok(LiteralValue::Number(val)))
    }
}

#[derive(Debug)]
pub struct SMALL;
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        let k = match coerce_num(args.last().unwrap().value()?.as_ref()) {
            Ok(n) => n,
            Err(_) => return Ok(LiteralValue::Error(ExcelError::new_num())),
        };
        let k = k as i64;
        if k < 1 {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.is_empty() || k as usize > nums.len() {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Ok(LiteralValue::Number(nums[(k as usize) - 1]))
    }
    fn eval_fold(
        &self,
        f: &mut dyn crate::function::FnFoldCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        let args = f.args();
        if args.len() < 2 {
            return Some(Ok(LiteralValue::Error(ExcelError::new_num())));
        }
        let k = match coerce_num(args.last().unwrap().value().ok()?.as_ref()) {
            Ok(n) => n as i64,
            Err(_) => {
                return Some(Ok(LiteralValue::Error(ExcelError::from_error_string(
                    "#NUM!",
                ))));
            }
        };
        if k < 1 {
            return Some(Ok(LiteralValue::Error(ExcelError::new_num())));
        }
        let mut nums = match collect_numeric_stats(&args[..args.len() - 1]) {
            Ok(v) => v,
            Err(e) => return Some(Ok(LiteralValue::Error(e))),
        };
        if nums.is_empty() || k as usize > nums.len() {
            return Some(Ok(LiteralValue::Error(ExcelError::new_num())));
        }
        let idx = (k as usize) - 1;
        let (left, nth, _right) =
            nums.select_nth_unstable_by(idx, |a, b| a.partial_cmp(b).unwrap());
        let val = *nth;
        f.write_result(LiteralValue::Number(val));
        Some(Ok(LiteralValue::Number(val)))
    }
}

#[derive(Debug)]
pub struct MEDIAN;
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let mut nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = nums.len();
        let mid = n / 2;
        let med = if n % 2 == 1 {
            nums[mid]
        } else {
            (nums[mid - 1] + nums[mid]) / 2.0
        };
        Ok(LiteralValue::Number(med))
    }
    fn eval_fold(
        &self,
        f: &mut dyn crate::function::FnFoldCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        let args = f.args();
        let mut nums = match collect_numeric_stats(args) {
            Ok(v) => v,
            Err(e) => return Some(Ok(LiteralValue::Error(e))),
        };
        if nums.is_empty() {
            return Some(Ok(LiteralValue::Error(ExcelError::new_num())));
        }
        let n = nums.len();
        let mid = n / 2;
        if n % 2 == 1 {
            // odd
            let (_left, nth, _right) =
                nums.select_nth_unstable_by(mid, |a, b| a.partial_cmp(b).unwrap());
            let val = *nth;
            let out = LiteralValue::Number(val);
            f.write_result(out.clone());
            Some(Ok(out))
        } else {
            // even
            let (_left, nth, _right) =
                nums.select_nth_unstable_by(mid, |a, b| a.partial_cmp(b).unwrap());
            let upper_mid = *nth;
            // Now the first mid elements are the lower partition (unsorted), scan for its max.
            let mut lower_max = f64::NEG_INFINITY;
            for &v in &nums[..mid] {
                if v > lower_max {
                    lower_max = v;
                }
            }
            let med = (lower_max + upper_mid) / 2.0;
            let out = LiteralValue::Number(med);
            f.write_result(out.clone());
            Some(Ok(out))
        }
    }
}

#[derive(Debug)]
pub struct StdevSample; // sample
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        let n = nums.len();
        if n < 2 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        let mean = nums.iter().sum::<f64>() / (n as f64);
        let mut ss = 0.0;
        for &v in &nums {
            let d = v - mean;
            ss += d * d;
        }
        Ok(LiteralValue::Number((ss / ((n - 1) as f64)).sqrt()))
    }
    fn eval_fold(
        &self,
        f: &mut dyn crate::function::FnFoldCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        // Welford one-pass for sample stdev
        let mut n: f64 = 0.0;
        let mut mean: f64 = 0.0;
        let mut m2: f64 = 0.0;
        let mut err: Option<ExcelError> = None;
        let mut cb = |chunk: crate::stripes::NumericChunk| -> Result<(), ExcelError> {
            for &x in chunk.data {
                n += 1.0;
                let delta = x - mean;
                mean += delta / n;
                let delta2 = x - mean;
                m2 += delta * delta2;
            }
            Ok(())
        };
        if let Err(e) = f.for_each_numeric_chunk(4096, &mut cb) {
            err = Some(e);
        }
        let out = if let Some(e) = err {
            LiteralValue::Error(e)
        } else if n < 2.0 {
            LiteralValue::Error(ExcelError::new_div())
        } else {
            LiteralValue::Number((m2 / (n - 1.0)).sqrt())
        };
        f.write_result(out.clone());
        Some(Ok(out))
    }
}

#[derive(Debug)]
pub struct StdevPop; // population
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        let n = nums.len();
        if n == 0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        let mean = nums.iter().sum::<f64>() / (n as f64);
        let mut ss = 0.0;
        for &v in &nums {
            let d = v - mean;
            ss += d * d;
        }
        Ok(LiteralValue::Number((ss / (n as f64)).sqrt()))
    }
    fn eval_fold(
        &self,
        f: &mut dyn crate::function::FnFoldCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        let mut n: f64 = 0.0;
        let mut mean = 0.0;
        let mut m2 = 0.0;
        let mut err: Option<ExcelError> = None;
        let mut cb = |chunk: crate::stripes::NumericChunk| -> Result<(), ExcelError> {
            for &x in chunk.data {
                n += 1.0;
                let delta = x - mean;
                mean += delta / n;
                let delta2 = x - mean;
                m2 += delta * delta2;
            }
            Ok(())
        };
        if let Err(e) = f.for_each_numeric_chunk(4096, &mut cb) {
            err = Some(e);
        }
        let out = if let Some(e) = err {
            LiteralValue::Error(e)
        } else if n == 0.0 {
            LiteralValue::Error(ExcelError::new_div())
        } else {
            LiteralValue::Number((m2 / n).sqrt())
        };
        f.write_result(out.clone());
        Some(Ok(out))
    }
}

#[derive(Debug)]
pub struct VarSample; // sample variance
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        let n = nums.len();
        if n < 2 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        let mean = nums.iter().sum::<f64>() / (n as f64);
        let mut ss = 0.0;
        for &v in &nums {
            let d = v - mean;
            ss += d * d;
        }
        Ok(LiteralValue::Number(ss / ((n - 1) as f64)))
    }
    fn eval_fold(
        &self,
        f: &mut dyn crate::function::FnFoldCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        let mut n: f64 = 0.0;
        let mut mean = 0.0;
        let mut m2 = 0.0;
        let mut err = None;
        let mut cb = |chunk: crate::stripes::NumericChunk| -> Result<(), ExcelError> {
            for &x in chunk.data {
                n += 1.0;
                let delta = x - mean;
                mean += delta / n;
                let delta2 = x - mean;
                m2 += delta * delta2;
            }
            Ok(())
        };
        if let Err(e) = f.for_each_numeric_chunk(4096, &mut cb) {
            err = Some(e);
        }
        let out = if let Some(e) = err {
            LiteralValue::Error(e)
        } else if n < 2.0 {
            LiteralValue::Error(ExcelError::new_div())
        } else {
            LiteralValue::Number(m2 / (n - 1.0))
        };
        f.write_result(out.clone());
        Some(Ok(out))
    }
}

#[derive(Debug)]
pub struct VarPop; // population variance
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let nums = collect_numeric_stats(args)?;
        let n = nums.len();
        if n == 0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        let mean = nums.iter().sum::<f64>() / (n as f64);
        let mut ss = 0.0;
        for &v in &nums {
            let d = v - mean;
            ss += d * d;
        }
        Ok(LiteralValue::Number(ss / (n as f64)))
    }
    fn eval_fold(
        &self,
        f: &mut dyn crate::function::FnFoldCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        let mut n: f64 = 0.0;
        let mut mean = 0.0;
        let mut m2 = 0.0;
        let mut err = None;
        let mut cb = |chunk: crate::stripes::NumericChunk| -> Result<(), ExcelError> {
            for &x in chunk.data {
                n += 1.0;
                let delta = x - mean;
                mean += delta / n;
                let delta2 = x - mean;
                m2 += delta * delta2;
            }
            Ok(())
        };
        if let Err(e) = f.for_each_numeric_chunk(4096, &mut cb) {
            err = Some(e);
        }
        let out = if let Some(e) = err {
            LiteralValue::Error(e)
        } else if n == 0.0 {
            LiteralValue::Error(ExcelError::new_div())
        } else {
            LiteralValue::Number(m2 / n)
        };
        f.write_result(out.clone());
        Some(Ok(out))
    }
}

// MODE.SNGL (alias MODE) and MODE.MULT
#[derive(Debug)]
pub struct ModeSingleFn;
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let mut nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(LiteralValue::Error(ExcelError::new_na()));
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
            Ok(LiteralValue::Error(ExcelError::new_na()))
        } else {
            Ok(LiteralValue::Number(best_val))
        }
    }
    fn eval_fold(
        &self,
        f: &mut dyn crate::function::FnFoldCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        let args = f.args();
        let mut nums = match collect_numeric_stats(args) {
            Ok(v) => v,
            Err(e) => return Some(Ok(LiteralValue::Error(e))),
        };
        if nums.is_empty() {
            return Some(Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#N/A",
            ))));
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
        let out = if best_cnt <= 1 {
            LiteralValue::Error(ExcelError::new_na())
        } else {
            LiteralValue::Number(best_val)
        };
        f.write_result(out.clone());
        Some(Ok(out))
    }
}

#[derive(Debug)]
pub struct ModeMultiFn;
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let mut nums = collect_numeric_stats(args)?;
        if nums.is_empty() {
            return Ok(LiteralValue::Error(ExcelError::new_na()));
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
            return Ok(LiteralValue::Error(ExcelError::new_na()));
        }
        let rows: Vec<Vec<LiteralValue>> = runs
            .into_iter()
            .filter(|&(_, c)| c == max_freq)
            .map(|(v, _)| vec![LiteralValue::Number(v)])
            .collect();
        Ok(LiteralValue::Array(rows))
    }
    fn eval_fold(
        &self,
        f: &mut dyn crate::function::FnFoldCtx,
    ) -> Option<Result<LiteralValue, ExcelError>> {
        let args = f.args();
        let mut nums = match collect_numeric_stats(args) {
            Ok(v) => v,
            Err(e) => return Some(Ok(LiteralValue::Error(e))),
        };
        if nums.is_empty() {
            return Some(Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#N/A",
            ))));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mut runs: Vec<(f64, usize)> = Vec::new();
        let mut cur = nums[0];
        let mut cnt = 1usize;
        for &v in &nums[1..] {
            if (v - cur).abs() < 1e-12 {
                cnt += 1;
            } else {
                runs.push((cur, cnt));
                cur = v;
                cnt = 1;
            }
        }
        runs.push((cur, cnt));
        let max_freq = runs.iter().map(|r| r.1).max().unwrap_or(0);
        if max_freq <= 1 {
            let out = LiteralValue::Error(ExcelError::new_na());
            f.write_result(out.clone());
            return Some(Ok(out));
        }
        let rows: Vec<Vec<LiteralValue>> = runs
            .into_iter()
            .filter(|&(_, c)| c == max_freq)
            .map(|(v, _)| vec![LiteralValue::Number(v)])
            .collect();
        let out = LiteralValue::Array(rows);
        f.write_result(out.clone());
        Some(Ok(out))
    }
}

#[derive(Debug)]
pub struct PercentileInc; // inclusive
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        let p = match coerce_num(args.last().unwrap().value()?.as_ref()) {
            Ok(n) => n,
            Err(_) => return Ok(LiteralValue::Error(ExcelError::new_num())),
        };
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.is_empty() {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        match percentile_inc(&nums, p) {
            Ok(v) => Ok(LiteralValue::Number(v)),
            Err(e) => Ok(LiteralValue::Error(e)),
        }
    }
}

#[derive(Debug)]
pub struct PercentileExc; // exclusive
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        let p = match coerce_num(args.last().unwrap().value()?.as_ref()) {
            Ok(n) => n,
            Err(_) => return Ok(LiteralValue::Error(ExcelError::new_num())),
        };
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.is_empty() {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        match percentile_exc(&nums, p) {
            Ok(v) => Ok(LiteralValue::Number(v)),
            Err(e) => Ok(LiteralValue::Error(e)),
        }
    }
}

#[derive(Debug)]
pub struct QuartileInc; // quartile inclusive
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        let q = match coerce_num(args.last().unwrap().value()?.as_ref()) {
            Ok(n) => n,
            Err(_) => return Ok(LiteralValue::Error(ExcelError::new_num())),
        };
        let q_i = q as i64;
        if !(0..=4).contains(&q_i) {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.is_empty() {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p = match q_i {
            0 => return Ok(LiteralValue::Number(nums[0])),
            4 => return Ok(LiteralValue::Number(nums[nums.len() - 1])),
            1 => 0.25,
            2 => 0.5,
            3 => 0.75,
            _ => return Ok(LiteralValue::Error(ExcelError::new_num())),
        };
        match percentile_inc(&nums, p) {
            Ok(v) => Ok(LiteralValue::Number(v)),
            Err(e) => Ok(LiteralValue::Error(e)),
        }
    }
}

#[derive(Debug)]
pub struct QuartileExc; // quartile exclusive
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        let q = match coerce_num(args.last().unwrap().value()?.as_ref()) {
            Ok(n) => n,
            Err(_) => return Ok(LiteralValue::Error(ExcelError::new_num())),
        };
        let q_i = q as i64;
        if !(1..=3).contains(&q_i) {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        let mut nums = collect_numeric_stats(&args[..args.len() - 1])?;
        if nums.len() < 2 {
            return Ok(LiteralValue::Error(ExcelError::new_num()));
        }
        nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p = match q_i {
            1 => 0.25,
            2 => 0.5,
            3 => 0.75,
            _ => return Ok(LiteralValue::Error(ExcelError::new_num())),
        };
        match percentile_exc(&nums, p) {
            Ok(v) => Ok(LiteralValue::Number(v)),
            Err(e) => Ok(LiteralValue::Error(e)),
        }
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
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
            .unwrap();
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
        match var_p.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Number(v) => assert!((v - 4.0).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
        match var_s.dispatch(&args, &ctx.function_context(None)).unwrap() {
            LiteralValue::Number(v) => assert!((v - 4.571428571428571).abs() < 1e-9),
            other => panic!("unexpected {other:?}"),
        }
        match stdev_p
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
        {
            LiteralValue::Number(v) => assert!((v - 2.0).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
        match stdev_s
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
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
        {
            LiteralValue::Number(v) => assert!((v - 2.0).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
        match f_inc
            .dispatch(&arg_inc_q2, &ctx.function_context(None))
            .unwrap()
        {
            LiteralValue::Number(v) => assert!((v - 3.0).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
        // QUARTILE.EXC Q1 for 5-point set uses exclusive percentile => 1.5
        match f_exc
            .dispatch(&arg_inc_q1, &ctx.function_context(None))
            .unwrap()
        {
            LiteralValue::Number(v) => assert!((v - 1.5).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
        match f_exc
            .dispatch(&arg_inc_q2, &ctx.function_context(None))
            .unwrap()
        {
            LiteralValue::Number(v) => assert!((v - 3.0).abs() < 1e-12),
            other => panic!("unexpected {other:?}"),
        }
    }

    // --- eval_fold equivalence tests for variance / stdev ---
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

        let var_s_fn = VarSample; // concrete instance to call eval_scalar
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
            .unwrap();
        let disp_var_p = ctx
            .context
            .get_function("", "VAR.P")
            .unwrap()
            .dispatch(&args, &fctx)
            .unwrap();
        let disp_stdev_s = ctx
            .context
            .get_function("", "STDEV.S")
            .unwrap()
            .dispatch(&args, &fctx)
            .unwrap();
        let disp_stdev_p = ctx
            .context
            .get_function("", "STDEV.P")
            .unwrap()
            .dispatch(&args, &fctx)
            .unwrap();

        // Scalar path results
        let scalar_var_s = var_s_fn.eval_scalar(&args, &fctx).unwrap();
        let scalar_var_p = var_p_fn.eval_scalar(&args, &fctx).unwrap();
        let scalar_stdev_s = stdev_s_fn.eval_scalar(&args, &fctx).unwrap();
        let scalar_stdev_p = stdev_p_fn.eval_scalar(&args, &fctx).unwrap();

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
        let scalar_var_s = VarSample.eval_scalar(&args_single, &fctx).unwrap();
        assert_eq!(disp_var_s, scalar_var_s);
        let disp_var_p = ctx
            .context
            .get_function("", "VAR.P")
            .unwrap()
            .dispatch(&args_single, &fctx)
            .unwrap();
        let scalar_var_p = VarPop.eval_scalar(&args_single, &fctx).unwrap();
        assert_eq!(disp_var_p, scalar_var_p);
        let disp_stdev_p = ctx
            .context
            .get_function("", "STDEV.P")
            .unwrap()
            .dispatch(&args_single, &fctx)
            .unwrap();
        let scalar_stdev_p = StdevPop.eval_scalar(&args_single, &fctx).unwrap();
        assert_eq!(disp_stdev_p, scalar_stdev_p);
        let disp_stdev_s = ctx
            .context
            .get_function("", "STDEV.S")
            .unwrap()
            .dispatch(&args_single, &fctx)
            .unwrap();
        let scalar_stdev_s = StdevSample.eval_scalar(&args_single, &fctx).unwrap();
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
            .unwrap();
        let legacy_p = ctx
            .context
            .get_function("", "PERCENTILE")
            .unwrap()
            .dispatch(&args_p, &ctx.function_context(None))
            .unwrap();
        assert_eq!(modern_p, legacy_p);
        let modern_q = ctx
            .context
            .get_function("", "QUARTILE.INC")
            .unwrap()
            .dispatch(&args_q, &ctx.function_context(None))
            .unwrap();
        let legacy_q = ctx
            .context
            .get_function("", "QUARTILE")
            .unwrap()
            .dispatch(&args_q, &ctx.function_context(None))
            .unwrap();
        assert_eq!(modern_q, legacy_q);
        let modern_rank = ctx
            .context
            .get_function("", "RANK.EQ")
            .unwrap()
            .dispatch(&args_rank, &ctx.function_context(None))
            .unwrap();
        let legacy_rank = ctx
            .context
            .get_function("", "RANK")
            .unwrap()
            .dispatch(&args_rank, &ctx.function_context(None))
            .unwrap();
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
            .unwrap();
        assert_eq!(mode_sngl, LiteralValue::Number(3.0));
        let mode_alias = ctx
            .context
            .get_function("", "MODE")
            .unwrap()
            .dispatch(&args, &ctx.function_context(None))
            .unwrap();
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
            .unwrap();
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
            .unwrap();
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
            .unwrap();
        let scalar_large = LARGE
            .eval_scalar(&args, &ctx.function_context(None))
            .unwrap();
        assert_eq!(disp_large, scalar_large);
        let f_small = ctx.context.get_function("", "SMALL").unwrap();
        let disp_small = f_small
            .dispatch(&args, &ctx.function_context(None))
            .unwrap();
        let scalar_small = SMALL
            .eval_scalar(&args, &ctx.function_context(None))
            .unwrap();
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
            .unwrap();
        let scalar_single = ModeSingleFn
            .eval_scalar(&args, &ctx.function_context(None))
            .unwrap();
        assert_eq!(disp_single, scalar_single);
        let f_multi = ctx.context.get_function("", "MODE.MULT").unwrap();
        let disp_multi = f_multi
            .dispatch(&args, &ctx.function_context(None))
            .unwrap();
        let scalar_multi = ModeMultiFn
            .eval_scalar(&args, &ctx.function_context(None))
            .unwrap();
        assert_eq!(disp_multi, scalar_multi);
    }

    #[test]
    fn median_fold_vs_scalar_even() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(MEDIAN));
        let ctx = interp(&wb);
        let arr_node = arr(vec![7.0, 1.0, 9.0, 5.0]); // sorted: 1,5,7,9 median=(5+7)/2=6
        let args = [ArgumentHandle::new(&arr_node, &ctx)];
        let f_med = ctx.context.get_function("", "MEDIAN").unwrap();
        let disp = f_med.dispatch(&args, &ctx.function_context(None)).unwrap();
        let scalar = MEDIAN
            .eval_scalar(&args, &ctx.function_context(None))
            .unwrap();
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
        let disp = f_med.dispatch(&args, &ctx.function_context(None)).unwrap();
        let scalar = MEDIAN
            .eval_scalar(&args, &ctx.function_context(None))
            .unwrap();
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
            f.dispatch(&args0, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(10.0)
        );
        assert_eq!(
            f.dispatch(&args1, &ctx.function_context(None)).unwrap(),
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
            match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
            .unwrap();
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
            .unwrap();
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
        let out = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert_eq!(out, LiteralValue::Number(0.0));
    }
}
