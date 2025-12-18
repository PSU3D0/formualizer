use super::super::utils::{ARG_ANY_ONE, coerce_num, criteria_match};
use crate::args::ArgSchema;
use crate::compute_prelude::{boolean, cmp, filter_array};
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use arrow::compute::kernels::aggregate::sum_array;
use arrow_array::types::Float64Type;
use arrow_array::{Array as _, BooleanArray, Float64Array};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::Cell;

    thread_local! {
        static CACHED_MASK_SLICE_FAST: Cell<usize> = const { Cell::new(0) };
        static CACHED_MASK_PAD_PARTIAL: Cell<usize> = const { Cell::new(0) };
        static CACHED_MASK_PAD_ALL_FILL: Cell<usize> = const { Cell::new(0) };
    }

    pub fn reset_cached_mask_counters() {
        CACHED_MASK_SLICE_FAST.with(|c| c.set(0));
        CACHED_MASK_PAD_PARTIAL.with(|c| c.set(0));
        CACHED_MASK_PAD_ALL_FILL.with(|c| c.set(0));
    }

    pub fn cached_mask_counters() -> (usize, usize, usize) {
        let a = CACHED_MASK_SLICE_FAST.with(|c| c.get());
        let b = CACHED_MASK_PAD_PARTIAL.with(|c| c.get());
        let d = CACHED_MASK_PAD_ALL_FILL.with(|c| c.get());
        (a, b, d)
    }

    pub(crate) fn inc_slice_fast() {
        CACHED_MASK_SLICE_FAST.with(|c| c.set(c.get() + 1));
    }
    pub(crate) fn inc_pad_partial() {
        CACHED_MASK_PAD_PARTIAL.with(|c| c.set(c.get() + 1));
    }
    pub(crate) fn inc_pad_all_fill() {
        CACHED_MASK_PAD_ALL_FILL.with(|c| c.set(c.get() + 1));
    }
}

/*
Criteria-driven aggregation functions:
  - SUMIF(range, criteria, [sum_range])
  - SUMIFS(sum_range, criteria_range1, criteria1, ...)
  - COUNTIF(range, criteria)
  - COUNTIFS(criteria_range1, criteria1, ...)
  - AVERAGEIFS(avg_range, criteria_range1, criteria1, ...)  (moved here from aggregate.rs)
  - COUNTA(value1, value2, ...)
  - COUNTBLANK(range_or_values...)

Design notes:
  * Validation of shape parity for multi-criteria aggregations (#VALUE! on mismatch).
  * Criteria parsing reused via crate::args::parse_criteria and criteria_match helper in utils.
  * Streaming optimization deferred (TODO(perf)).
*/

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregationType {
    Sum,
    Count,
    Average,
}

fn eval_if_family<'a, 'b>(
    args: &[ArgumentHandle<'a, 'b>],
    ctx: &dyn FunctionContext<'b>,
    agg_type: AggregationType,
    multi: bool,
) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
    let mut sum_view: Option<crate::engine::range_view::RangeView<'_>> = None;
    let mut sum_scalar: Option<LiteralValue> = None;
    let mut crit_specs = Vec::new();

    if !multi {
        // Single criterion: IF(range, criteria, [target_range])
        if args.len() < 2 || args.len() > 3 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new_value().with_message(format!(
                    "Function expects 2 or 3 arguments, got {}",
                    args.len()
                )),
            )));
        }
        let pred = crate::args::parse_criteria(&args[1].value()?.into_literal())?;
        let crit_rv = args[0].range_view().ok();
        let crit_val = if crit_rv.is_none() {
            Some(args[0].value()?.into_literal())
        } else {
            None
        };
        crit_specs.push((crit_rv, pred, crit_val));

        if agg_type != AggregationType::Count {
            if args.len() == 3 {
                if let Ok(v) = args[2].range_view() {
                    let crit_dims = crit_specs[0].0.as_ref().map(|v| v.dims()).unwrap_or((1, 1));
                    sum_view = Some(v.expand_to(crit_dims.0, crit_dims.1));
                } else {
                    sum_scalar = Some(args[2].value()?.into_literal());
                }
            } else {
                // Default target is criteria range
                if let Ok(v) = args[0].range_view() {
                    sum_view = Some(v);
                } else {
                    sum_scalar = Some(args[0].value()?.into_literal());
                }
            }
        }
    } else {
        // Multi criteria: IFS(target_range, crit_range1, crit1, ...) or COUNTIFS(crit_range1, crit1, ...)
        if agg_type == AggregationType::Count {
            if args.len() < 2 || !args.len().is_multiple_of(2) {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message(format!(
                        "COUNTIFS expects N pairs (criteria_range, criteria); got {} args",
                        args.len()
                    )),
                )));
            }
            for i in (0..args.len()).step_by(2) {
                let mut rv = args[i].range_view().ok();
                let mut val: Option<LiteralValue> = None;

                // Broadcast semantics: treat 1x1 criteria ranges as scalar criteria.
                if let Some(ref view) = rv {
                    let (r, c) = view.dims();
                    if r == 1 && c == 1 {
                        val = Some(view.as_1x1().unwrap_or(LiteralValue::Empty));
                        rv = None;
                    }
                }

                if val.is_none() && rv.is_none() {
                    val = Some(args[i].value()?.into_literal());
                }

                let pred = crate::args::parse_criteria(&args[i + 1].value()?.into_literal())?;
                crit_specs.push((rv, pred, val));
            }
        } else {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_value().with_message(format!(
                        "Function expects 1 target_range followed by N pairs (criteria_range, criteria); got {} args",
                        args.len()
                    )),
                )));
            }
            if let Ok(v) = args[0].range_view() {
                sum_view = Some(v);
            } else {
                sum_scalar = Some(args[0].value()?.into_literal());
            }
            for i in (1..args.len()).step_by(2) {
                let mut rv = args[i].range_view().ok();
                let mut val: Option<LiteralValue> = None;

                // Broadcast semantics: treat 1x1 criteria ranges as scalar criteria.
                if let Some(ref view) = rv {
                    let (r, c) = view.dims();
                    if r == 1 && c == 1 {
                        val = Some(view.as_1x1().unwrap_or(LiteralValue::Empty));
                        rv = None;
                    }
                }

                if val.is_none() && rv.is_none() {
                    val = Some(args[i].value()?.into_literal());
                }

                let pred = crate::args::parse_criteria(&args[i + 1].value()?.into_literal())?;
                crit_specs.push((rv, pred, val));
            }
        }
    }

    // Determine union dimensions
    let mut dims = (1usize, 1usize);
    if let Some(ref sv) = sum_view {
        dims = sv.dims();
    }
    for (rv, _, _) in &crit_specs {
        if let Some(v) = rv {
            let vd = v.dims();
            dims.0 = dims.0.max(vd.0);
            dims.1 = dims.1.max(vd.1);
        }
    }

    // Excel SUMIF rules: if target_range is given, it expands from its top-left to match criteria range dims
    // SUMIFS rules: all ranges must have same dims.
    // Our implementation will use dims as the iteration space and broadcast/pad.

    let mut total_sum = 0.0f64;
    let mut total_count = 0i64;

    // Use a driver view for chunked iteration. Prefer sum_view, else first criteria range.
    let driver = sum_view
        .as_ref()
        .or_else(|| crit_specs.iter().find_map(|(rv, _, _)| rv.as_ref()));

    if let Some(drv) = driver {
        // We can't easily iterate over union dims if they are larger than driver.
        // But for most cases they are same.
        // If driver is smaller, we'll miss some rows.
        // Actually, if it's SUMIF, we want to iterate over criteria range dims.
        let driver = if !multi && crit_specs[0].0.is_some() {
            crit_specs[0].0.as_ref().unwrap()
        } else {
            drv
        };

        for res in driver.iter_row_chunks() {
            let cs = res?;
            let row_start = cs.row_start;
            let row_len = cs.row_len;
            if row_len == 0 {
                continue;
            }

            // Get slices for all criteria and sum range
            let mut crit_num_slices = Vec::with_capacity(crit_specs.len());
            let mut crit_text_slices = Vec::with_capacity(crit_specs.len());
            for (rv, _, _) in &crit_specs {
                if let Some(v) = rv {
                    crit_num_slices.push(Some(v.slice_numbers(row_start, row_len)));
                    crit_text_slices.push(Some(v.slice_lowered_text(row_start, row_len)));
                } else {
                    crit_num_slices.push(None);
                    crit_text_slices.push(None);
                }
            }

            let sum_slices = sum_view
                .as_ref()
                .map(|v| v.slice_numbers(row_start, row_len));

            for c in 0..dims.1 {
                let mut mask_opt: Option<BooleanArray> = None;
                let mut impossible = false;

                for (j, (_, pred, scalar_val)) in crit_specs.iter().enumerate() {
                    if crit_specs[j].0.is_none() {
                        if let Some(sv) = scalar_val {
                            if !criteria_match(pred, sv) {
                                impossible = true;
                                break;
                            }
                            continue;
                        }
                        if !criteria_match(pred, &LiteralValue::Empty) {
                            impossible = true;
                            break;
                        }
                        continue;
                    }

                    // Try cache
                    let cur_cached = if let Some(ref view) = crit_specs[j].0 {
                        ctx.get_criteria_mask(view, c, pred).map(|m| {
                            let fill = criteria_match(pred, &LiteralValue::Empty);
                            let m_len = m.len();

                            // The cached mask may be shorter than the current driver's chunk
                            // (e.g., whole-column references trimmed to different used-regions).
                            // Treat out-of-bounds rows as Empty cells.
                            if row_start + row_len <= m_len {
                                #[cfg(test)]
                                test_hooks::inc_slice_fast();
                                let sl = m.slice(row_start, row_len);
                                return sl
                                    .as_any()
                                    .downcast_ref::<arrow_array::BooleanArray>()
                                    .expect("cached criteria mask slice downcast")
                                    .clone();
                            }

                            let mut bb =
                                arrow_array::builder::BooleanBuilder::with_capacity(row_len);
                            if row_start < m_len {
                                #[cfg(test)]
                                test_hooks::inc_pad_partial();
                                let take_len = row_len.min(m_len - row_start);
                                let sl = m.slice(row_start, take_len);
                                let ba = sl
                                    .as_any()
                                    .downcast_ref::<arrow_array::BooleanArray>()
                                    .expect("cached criteria mask slice downcast");
                                bb.append_array(ba);
                                bb.append_n(row_len - take_len, fill);
                            } else {
                                #[cfg(test)]
                                test_hooks::inc_pad_all_fill();
                                bb.append_n(row_len, fill);
                            }

                            bb.finish()
                        })
                    } else {
                        None
                    };

                    if let Some(cm) = cur_cached {
                        mask_opt = Some(match mask_opt {
                            None => cm,
                            Some(prev) => boolean::and_kleene(&prev, &cm).unwrap(),
                        });
                        continue;
                    }

                    // Compute mask for this chunk
                    let num_col = crit_num_slices[j]
                        .as_ref()
                        .and_then(|cols| cols.get(c).and_then(|a| a.as_ref()));
                    let text_col = crit_text_slices[j]
                        .as_ref()
                        .and_then(|cols| cols.get(c).and_then(|a| a.as_ref()));

                    let m = match (pred, num_col, text_col) {
                        (crate::args::CriteriaPredicate::Gt(n), Some(nc), _) => {
                            cmp::gt(nc.as_ref(), &Float64Array::new_scalar(*n)).unwrap()
                        }
                        (crate::args::CriteriaPredicate::Ge(n), Some(nc), _) => {
                            cmp::gt_eq(nc.as_ref(), &Float64Array::new_scalar(*n)).unwrap()
                        }
                        (crate::args::CriteriaPredicate::Lt(n), Some(nc), _) => {
                            cmp::lt(nc.as_ref(), &Float64Array::new_scalar(*n)).unwrap()
                        }
                        (crate::args::CriteriaPredicate::Le(n), Some(nc), _) => {
                            cmp::lt_eq(nc.as_ref(), &Float64Array::new_scalar(*n)).unwrap()
                        }
                        (crate::args::CriteriaPredicate::Eq(v), nc, tc) => {
                            match v {
                                LiteralValue::Number(x) => {
                                    let nx = *x;
                                    if let Some(nc) = nc {
                                        cmp::eq(nc.as_ref(), &Float64Array::new_scalar(nx)).unwrap()
                                    } else {
                                        BooleanArray::new_null(row_len)
                                    }
                                }
                                LiteralValue::Int(x) => {
                                    let nx = *x as f64;
                                    if let Some(nc) = nc {
                                        cmp::eq(nc.as_ref(), &Float64Array::new_scalar(nx)).unwrap()
                                    } else {
                                        BooleanArray::new_null(row_len)
                                    }
                                }
                                _ => {
                                    // Use fallback for text and other types to ensure Excel parity (e.g. blank matching)
                                    let mut bb =
                                        arrow_array::builder::BooleanBuilder::with_capacity(
                                            row_len,
                                        );
                                    let view = crit_specs[j].0.as_ref().unwrap();
                                    for i in 0..row_len {
                                        bb.append_value(criteria_match(
                                            pred,
                                            &view.get_cell(row_start + i, c),
                                        ));
                                    }
                                    bb.finish()
                                }
                            }
                        }
                        (crate::args::CriteriaPredicate::Ne(v), nc, tc) => match v {
                            LiteralValue::Number(x) => {
                                let nx = *x;
                                if let Some(nc) = nc {
                                    cmp::neq(nc.as_ref(), &Float64Array::new_scalar(nx)).unwrap()
                                } else {
                                    BooleanArray::from(vec![true; row_len])
                                }
                            }
                            LiteralValue::Int(x) => {
                                let nx = *x as f64;
                                if let Some(nc) = nc {
                                    cmp::neq(nc.as_ref(), &Float64Array::new_scalar(nx)).unwrap()
                                } else {
                                    BooleanArray::from(vec![true; row_len])
                                }
                            }
                            _ => {
                                let mut bb =
                                    arrow_array::builder::BooleanBuilder::with_capacity(row_len);
                                let view = crit_specs[j].0.as_ref().unwrap();
                                for i in 0..row_len {
                                    bb.append_value(criteria_match(
                                        pred,
                                        &view.get_cell(row_start + i, c),
                                    ));
                                }
                                bb.finish()
                            }
                        },
                        (crate::args::CriteriaPredicate::TextLike { .. }, _, _) => {
                            let mut bb =
                                arrow_array::builder::BooleanBuilder::with_capacity(row_len);
                            let view = crit_specs[j].0.as_ref().unwrap();
                            for i in 0..row_len {
                                bb.append_value(criteria_match(
                                    pred,
                                    &view.get_cell(row_start + i, c),
                                ));
                            }
                            bb.finish()
                        }
                        _ => {
                            // Fallback for any other case
                            let mut bb =
                                arrow_array::builder::BooleanBuilder::with_capacity(row_len);
                            if let Some(ref view) = crit_specs[j].0 {
                                for i in 0..row_len {
                                    bb.append_value(criteria_match(
                                        pred,
                                        &view.get_cell(row_start + i, c),
                                    ));
                                }
                            } else {
                                let val = scalar_val.as_ref().unwrap_or(&LiteralValue::Empty);
                                let matches = criteria_match(pred, val);
                                for _ in 0..row_len {
                                    bb.append_value(matches);
                                }
                            }
                            bb.finish()
                        }
                    };

                    mask_opt = Some(match mask_opt {
                        None => m,
                        Some(prev) => boolean::and_kleene(&prev, &m).unwrap(),
                    });
                }

                if impossible {
                    continue;
                }

                match mask_opt {
                    Some(mask) => {
                        if agg_type == AggregationType::Count {
                            total_count += (0..mask.len())
                                .filter(|&i| mask.is_valid(i) && mask.value(i))
                                .count() as i64;
                        } else {
                            let target_col = sum_slices
                                .as_ref()
                                .and_then(|cols| cols.get(c).and_then(|a| a.as_ref()));
                            if let Some(tc) = target_col {
                                let filtered = filter_array(tc.as_ref(), &mask).unwrap();
                                let f64_arr =
                                    filtered.as_any().downcast_ref::<Float64Array>().unwrap();
                                if let Some(s) = sum_array::<Float64Type, _>(f64_arr) {
                                    total_sum += s;
                                }
                                total_count += f64_arr.len() as i64 - f64_arr.null_count() as i64;
                            } else if let Some(ref s) = sum_scalar
                                && let Ok(n) = coerce_num(s)
                            {
                                let count = (0..mask.len())
                                    .filter(|&i| mask.is_valid(i) && mask.value(i))
                                    .count() as i64;
                                total_sum += n * count as f64;
                                total_count += count;
                            }
                        }
                    }
                    None => {
                        // No masks: everything matches
                        if agg_type == AggregationType::Count {
                            total_count += row_len as i64;
                        } else {
                            let target_col = sum_slices
                                .as_ref()
                                .and_then(|cols| cols.get(c).and_then(|a| a.as_ref()));
                            if let Some(tc) = target_col {
                                if let Some(s) = sum_array::<Float64Type, _>(tc.as_ref()) {
                                    total_sum += s;
                                }
                                total_count += tc.len() as i64 - tc.null_count() as i64;
                            } else if let Some(ref s) = sum_scalar
                                && let Ok(n) = coerce_num(s)
                            {
                                total_sum += n * row_len as f64;
                                total_count += row_len as i64;
                            }
                        }
                    }
                }
            }
        }
    } else {
        // Scalar driver fallback
        let mut all_match = true;
        for (_, pred, scalar_val) in &crit_specs {
            let val = scalar_val.as_ref().unwrap_or(&LiteralValue::Empty);
            if !criteria_match(pred, val) {
                all_match = false;
                break;
            }
        }
        if all_match {
            if agg_type == AggregationType::Count {
                total_count = (dims.0 * dims.1) as i64;
            } else if let Some(ref s) = sum_scalar
                && let Ok(n) = coerce_num(s)
            {
                total_sum = n * (dims.0 * dims.1) as f64;
                total_count = (dims.0 * dims.1) as i64;
            }
        }
    }

    match agg_type {
        AggregationType::Sum => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            total_sum,
        ))),
        AggregationType::Count => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            total_count as f64,
        ))),
        AggregationType::Average => {
            if total_count == 0 {
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new_div(),
                )))
            } else {
                Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
                    total_sum / total_count as f64,
                )))
            }
        }
    }
}

/* ─────────────────────────── AVERAGEIF() ──────────────────────────── */
#[derive(Debug)]
pub struct AverageIfFn;
impl Function for AverageIfFn {
    func_caps!(
        PURE,
        REDUCTION,
        WINDOWED,
        STREAM_OK,
        PARALLEL_ARGS,
        PARALLEL_CHUNKS
    );
    fn name(&self) -> &'static str {
        "AVERAGEIF"
    }
    fn min_args(&self) -> usize {
        2
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        eval_if_family(args, ctx, AggregationType::Average, false)
    }
}

/* ─────────────────────────── SUMIF() ──────────────────────────── */
#[derive(Debug)]
pub struct SumIfFn;
impl Function for SumIfFn {
    func_caps!(
        PURE,
        REDUCTION,
        WINDOWED,
        STREAM_OK,
        PARALLEL_ARGS,
        PARALLEL_CHUNKS
    );
    fn name(&self) -> &'static str {
        "SUMIF"
    }
    fn min_args(&self) -> usize {
        2
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        eval_if_family(args, ctx, AggregationType::Sum, false)
    }
}

/* ─────────────────────────── COUNTIF() ──────────────────────────── */
#[derive(Debug)]
pub struct CountIfFn;
impl Function for CountIfFn {
    func_caps!(
        PURE,
        REDUCTION,
        WINDOWED,
        STREAM_OK,
        PARALLEL_ARGS,
        PARALLEL_CHUNKS
    );
    fn name(&self) -> &'static str {
        "COUNTIF"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        false
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        eval_if_family(args, ctx, AggregationType::Count, false)
    }
}

/* ─────────────────────────── SUMIFS() ──────────────────────────── */
#[derive(Debug)]
pub struct SumIfsFn; // SUMIFS(sum_range, criteria_range1, criteria1, ...)
impl Function for SumIfsFn {
    func_caps!(
        PURE,
        REDUCTION,
        WINDOWED,
        STREAM_OK,
        PARALLEL_ARGS,
        PARALLEL_CHUNKS
    );
    fn name(&self) -> &'static str {
        "SUMIFS"
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        eval_if_family(args, ctx, AggregationType::Sum, true)
    }
}

/* ─────────────────────────── COUNTIFS() ──────────────────────────── */
#[derive(Debug)]
pub struct CountIfsFn; // COUNTIFS(criteria_range1, criteria1, ...)
impl Function for CountIfsFn {
    func_caps!(
        PURE,
        REDUCTION,
        WINDOWED,
        STREAM_OK,
        PARALLEL_ARGS,
        PARALLEL_CHUNKS
    );
    fn name(&self) -> &'static str {
        "COUNTIFS"
    }
    fn min_args(&self) -> usize {
        2
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        eval_if_family(args, ctx, AggregationType::Count, true)
    }
}

/* ─────────────────────────── AVERAGEIFS() (moved) ──────────────────────────── */
#[derive(Debug)]
pub struct AverageIfsFn;
impl Function for AverageIfsFn {
    func_caps!(
        PURE,
        REDUCTION,
        WINDOWED,
        STREAM_OK,
        PARALLEL_ARGS,
        PARALLEL_CHUNKS
    );
    fn name(&self) -> &'static str {
        "AVERAGEIFS"
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
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        eval_if_family(args, ctx, AggregationType::Average, true)
    }
}

/* ─────────────────────────── COUNTA() ──────────────────────────── */
#[derive(Debug)]
pub struct CountAFn; // counts non-empty (including empty text "")
impl Function for CountAFn {
    func_caps!(PURE, REDUCTION);
    fn name(&self) -> &'static str {
        "COUNTA"
    }
    fn min_args(&self) -> usize {
        1
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
        let mut cnt = 0i64;
        for a in args {
            if let Ok(view) = a.range_view() {
                for res in view.type_tags_slices() {
                    let (_, _, tag_cols) = res?;
                    for col in tag_cols {
                        for i in 0..col.len() {
                            if col.value(i) != crate::arrow_store::TypeTag::Empty as u8 {
                                cnt += 1;
                            }
                        }
                    }
                }
            } else {
                let v = a.value()?.into_literal();
                if !matches!(v, LiteralValue::Empty) {
                    cnt += 1;
                }
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            cnt as f64,
        )))
    }
}

/* ─────────────────────────── COUNTBLANK() ──────────────────────────── */
#[derive(Debug)]
pub struct CountBlankFn; // counts truly empty cells and empty text
impl Function for CountBlankFn {
    func_caps!(PURE, REDUCTION);
    fn name(&self) -> &'static str {
        "COUNTBLANK"
    }
    fn min_args(&self) -> usize {
        1
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
        let mut cnt = 0i64;
        for a in args {
            if let Ok(view) = a.range_view() {
                let mut tag_it = view.type_tags_slices();
                let mut text_it = view.text_slices();

                while let (Some(tag_res), Some(text_res)) = (tag_it.next(), text_it.next()) {
                    let (_, _, tag_cols) = tag_res?;
                    let (_, _, text_cols) = text_res?;

                    for (tc, xc) in tag_cols.into_iter().zip(text_cols.into_iter()) {
                        let text_arr = xc
                            .as_any()
                            .downcast_ref::<arrow_array::StringArray>()
                            .unwrap();
                        for i in 0..tc.len() {
                            let is_blank = tc.value(i) == crate::arrow_store::TypeTag::Empty as u8
                                || (tc.value(i) == crate::arrow_store::TypeTag::Text as u8
                                    && !text_arr.is_null(i)
                                    && text_arr.value(i).is_empty());
                            if is_blank {
                                cnt += 1;
                            }
                        }
                    }
                }
            } else {
                let v = a.value()?.into_literal();
                match v {
                    LiteralValue::Empty => cnt += 1,
                    LiteralValue::Text(s) if s.is_empty() => cnt += 1,
                    _ => {}
                }
            }
        }
        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Number(
            cnt as f64,
        )))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(SumIfFn));
    crate::function_registry::register_function(Arc::new(CountIfFn));
    crate::function_registry::register_function(Arc::new(AverageIfFn));
    crate::function_registry::register_function(Arc::new(SumIfsFn));
    crate::function_registry::register_function(Arc::new(CountIfsFn));
    crate::function_registry::register_function(Arc::new(AverageIfsFn));
    crate::function_registry::register_function(Arc::new(CountAFn));
    crate::function_registry::register_function(Arc::new(CountBlankFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }
    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn sumif_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumIfFn));
        let ctx = interp(&wb);
        let range = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(2),
            LiteralValue::Int(3),
        ]]));
        let crit = lit(LiteralValue::Text(">1".into()));
        let args = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMIF").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(5.0)
        );
    }

    #[test]
    fn sumif_with_sum_range() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumIfFn));
        let ctx = interp(&wb);
        let range = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(0),
            LiteralValue::Int(1),
        ]]));
        let sum_range = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(10),
            LiteralValue::Int(20),
            LiteralValue::Int(30),
        ]]));
        let crit = lit(LiteralValue::Text("=1".into()));
        let args = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&crit, &ctx),
            ArgumentHandle::new(&sum_range, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMIF").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(40.0)
        );
    }

    #[test]
    fn sumif_mismatched_ranges_now_pad_with_empty() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumIfFn));
        let ctx = interp(&wb);
        // sum_range: 2x2
        let sum = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(1), LiteralValue::Int(2)],
            vec![LiteralValue::Int(3), LiteralValue::Int(4)],
        ]));
        // criteria range: 3x2 (extra row should be ignored due to iterating sum_range dims)
        let crit_range = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(1), LiteralValue::Int(1)],
            vec![LiteralValue::Int(1), LiteralValue::Int(1)],
            vec![LiteralValue::Int(1), LiteralValue::Int(1)],
        ]));
        let crit = lit(LiteralValue::Text("=1".into()));
        let args = vec![
            ArgumentHandle::new(&crit_range, &ctx),
            ArgumentHandle::new(&crit, &ctx),
            ArgumentHandle::new(&sum, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMIF").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(10.0)
        );
    }

    #[test]
    fn countif_text_wildcard() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountIfFn));
        let ctx = interp(&wb);
        let rng = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Text("alpha".into()),
            LiteralValue::Text("beta".into()),
            LiteralValue::Text("alphabet".into()),
        ]]));
        let crit = lit(LiteralValue::Text("al*".into()));
        let args = vec![
            ArgumentHandle::new(&rng, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "COUNTIF").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(2.0)
        );
    }

    #[test]
    fn sumifs_multiple_criteria() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumIfsFn));
        let ctx = interp(&wb);
        let sum = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(10),
            LiteralValue::Int(20),
            LiteralValue::Int(30),
            LiteralValue::Int(40),
        ]]));
        let city = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Text("Bellevue".into()),
            LiteralValue::Text("Issaquah".into()),
            LiteralValue::Text("Bellevue".into()),
            LiteralValue::Text("Issaquah".into()),
        ]]));
        let beds = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(2),
            LiteralValue::Int(3),
            LiteralValue::Int(4),
            LiteralValue::Int(5),
        ]]));
        let c_city = lit(LiteralValue::Text("Bellevue".into()));
        let c_beds = lit(LiteralValue::Text(">=4".into()));
        let args = vec![
            ArgumentHandle::new(&sum, &ctx),
            ArgumentHandle::new(&city, &ctx),
            ArgumentHandle::new(&c_city, &ctx),
            ArgumentHandle::new(&beds, &ctx),
            ArgumentHandle::new(&c_beds, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMIFS").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(30.0)
        );
    }

    #[test]
    fn countifs_basic() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountIfsFn));
        let ctx = interp(&wb);
        let city = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Text("a".into()),
            LiteralValue::Text("b".into()),
            LiteralValue::Text("a".into()),
        ]]));
        let beds = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(2),
            LiteralValue::Int(3),
        ]]));
        let c_city = lit(LiteralValue::Text("a".into()));
        let c_beds = lit(LiteralValue::Text(">1".into()));
        let args = vec![
            ArgumentHandle::new(&city, &ctx),
            ArgumentHandle::new(&c_city, &ctx),
            ArgumentHandle::new(&beds, &ctx),
            ArgumentHandle::new(&c_beds, &ctx),
        ];
        let f = ctx.context.get_function("", "COUNTIFS").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(1.0)
        );
    }

    #[test]
    fn averageifs_div0() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        let avg = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(2),
        ]]));
        let crit_rng = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(0),
            LiteralValue::Int(0),
        ]]));
        let crit = lit(LiteralValue::Text(">0".into()));
        let args = vec![
            ArgumentHandle::new(&avg, &ctx),
            ArgumentHandle::new(&crit_rng, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        match f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal()
        {
            LiteralValue::Error(e) => assert_eq!(e, "#DIV/0!"),
            _ => panic!("expected div0"),
        }
    }

    #[test]
    fn counta_and_countblank() {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(CountAFn))
            .with_function(std::sync::Arc::new(CountBlankFn));
        let ctx = interp(&wb);
        let arr = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Empty,
            LiteralValue::Text("".into()),
            LiteralValue::Int(5),
        ]]));
        let args = vec![ArgumentHandle::new(&arr, &ctx)];
        let counta = ctx.context.get_function("", "COUNTA").unwrap();
        let countblank = ctx.context.get_function("", "COUNTBLANK").unwrap();
        assert_eq!(
            counta
                .dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            countblank
                .dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(2.0)
        );
    }

    // ───────── Parity tests (window vs scalar) ─────────
    #[test]
    fn sumifs_broadcasts_1x1_criteria_over_range() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumIfsFn));
        let ctx = interp(&wb);
        // sum_range: column vector [10, 20]
        let sum = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(10)],
            vec![LiteralValue::Int(20)],
        ]));
        // criteria_range: column vector ["A", "B"]
        let tags = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Text("A".into())],
            vec![LiteralValue::Text("B".into())],
        ]));
        // criteria: 1x1 array acting as scalar "A"
        let c_tag = lit(LiteralValue::Array(vec![vec![LiteralValue::Text(
            "A".into(),
        )]]));
        let args = vec![
            ArgumentHandle::new(&sum, &ctx),
            ArgumentHandle::new(&tags, &ctx),
            ArgumentHandle::new(&c_tag, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMIFS").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(10.0)
        );
    }

    #[test]
    fn countifs_broadcasts_1x1_criteria_over_row() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountIfsFn));
        let ctx = interp(&wb);
        // criteria_range: row [1,2,3,4]
        let nums = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(2),
            LiteralValue::Int(3),
            LiteralValue::Int(4),
        ]]));
        // criteria: 1x1 array ">=3"
        let crit = lit(LiteralValue::Array(vec![vec![LiteralValue::Text(
            ">=3".into(),
        )]]));
        let args = vec![
            ArgumentHandle::new(&nums, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "COUNTIFS").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Number(2.0)
        );
    }

    #[test]
    fn sumifs_empty_ranges_with_1x1_criteria_produce_zero() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumIfsFn));
        let ctx = interp(&wb);
        // Empty ranges (0x0) simulate unused whole-column resolved empty
        let empty = lit(LiteralValue::Array(Vec::new()));
        // 1x1 criteria (array)
        let crit = lit(LiteralValue::Array(vec![vec![LiteralValue::Text(
            "X".into(),
        )]]));
        let args = vec![
            ArgumentHandle::new(&empty, &ctx),
            ArgumentHandle::new(&empty, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMIFS").unwrap();
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(0.0)
        );
    }

    #[test]
    fn sumifs_mismatched_ranges_now_pad_with_empty() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumIfsFn));
        let ctx = interp(&wb);
        // sum_range: 2x2
        let sum = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(1), LiteralValue::Int(2)],
            vec![LiteralValue::Int(3), LiteralValue::Int(4)],
        ]));
        // criteria_range: 3x2 (different rows - extra row will match against padded empty values)
        let crit_range = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(1), LiteralValue::Int(1)],
            vec![LiteralValue::Int(1), LiteralValue::Int(1)],
            vec![LiteralValue::Int(1), LiteralValue::Int(1)],
        ]));
        // scalar criterion
        let crit = lit(LiteralValue::Text("=1".into()));
        let args = vec![
            ArgumentHandle::new(&sum, &ctx),
            ArgumentHandle::new(&crit_range, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMIFS").unwrap();
        // With padding, sum_range gets padded with empties for row 3
        // Rows 1-2 match criteria (all 1s), row 3 has empties which don't match =1
        // So we sum: 1 + 2 + 3 + 4 = 10
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(10.0)
        );
    }

    #[test]
    fn countifs_mismatched_ranges_pad_and_broadcast() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountIfsFn));
        let ctx = interp(&wb);
        // criteria_range1: 2x1 -> [1,1]
        let r1 = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(1)],
            vec![LiteralValue::Int(1)],
        ]));
        // criteria1: "=1"
        let c1 = lit(LiteralValue::Text("=1".into()));
        // criteria_range2: 3x1 -> [1,1,1]
        let r2 = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(1)],
            vec![LiteralValue::Int(1)],
            vec![LiteralValue::Int(1)],
        ]));
        // criteria2: "=1"
        let c2 = lit(LiteralValue::Text("=1".into()));
        let args = vec![
            ArgumentHandle::new(&r1, &ctx),
            ArgumentHandle::new(&c1, &ctx),
            ArgumentHandle::new(&r2, &ctx),
            ArgumentHandle::new(&c2, &ctx),
        ];
        let f = ctx.context.get_function("", "COUNTIFS").unwrap();
        // Union rows = 3; row3 has r1=Empty (padded), which doesn't match =1; expect 2
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(2.0)
        );
    }

    #[test]
    fn averageifs_mismatched_ranges_pad() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        // avg_range: 2x1 -> [10,20]
        let avg = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(10)],
            vec![LiteralValue::Int(20)],
        ]));
        // criteria_range: 3x1 -> [1,1,2]
        let r1 = lit(LiteralValue::Array(vec![
            vec![LiteralValue::Int(1)],
            vec![LiteralValue::Int(1)],
            vec![LiteralValue::Int(2)],
        ]));
        let c1 = lit(LiteralValue::Text("=1".into()));
        let args = vec![
            ArgumentHandle::new(&avg, &ctx),
            ArgumentHandle::new(&r1, &ctx),
            ArgumentHandle::new(&c1, &ctx),
        ];
        let f = ctx.context.get_function("", "AVERAGEIFS").unwrap();
        // Only first two rows match; expect (10+20)/2 = 15
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(15.0)
        );
    }

    #[test]
    fn criteria_scientific_notation() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumIfFn));
        let ctx = interp(&wb);
        let nums = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Number(1000.0),
            LiteralValue::Number(1500.0),
            LiteralValue::Number(999.0),
        ]]));
        let crit = lit(LiteralValue::Text(">1e3".into())); // should parse as >1000
        let args = vec![
            ArgumentHandle::new(&nums, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMIF").unwrap();
        // >1000 matches 1500 only (strict greater)
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(1500.0)
        );
    }
}
