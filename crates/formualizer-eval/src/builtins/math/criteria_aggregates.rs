use super::super::utils::{ARG_ANY_ONE, coerce_num, criteria_match};
use crate::args::ArgSchema;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ExcelError, LiteralValue};
use formualizer_macros::func_caps;

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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 2 || args.len() > 3 {
            return Ok(LiteralValue::Error(ExcelError::new_value().with_message(
                format!("SUMIF expects 2 or 3 arguments, got {}", args.len()),
            )));
        }

        let pred = crate::args::parse_criteria(args[1].value()?.as_ref())?;

        // Resolve criteria source: range view or scalar (broadcast)
        let crit_view = args[0].range_view().ok();
        let crit_scalar = if crit_view.is_none() {
            Some(args[0].value()?.into_owned())
        } else {
            None
        };

        // Resolve sum source and iteration dims
        let (sum_view_opt, sum_scalar_opt, dims) = if args.len() == 3 {
            match args[2].range_view() {
                Ok(v) => {
                    let d = v.dims();
                    (Some(v), None, d)
                }
                Err(_) => {
                    let sv = args[2].value()?.into_owned();
                    // If criteria is a range, iterate over its dims; else single cell
                    let d = crit_view.as_ref().map(|v| v.dims()).unwrap_or((1, 1));
                    (None, Some(sv), d)
                }
            }
        } else {
            // No sum_range: sum over the criteria range itself or scalar
            match args[0].range_view() {
                Ok(v) => {
                    let d = v.dims();
                    (Some(v), None, d)
                }
                Err(_) => {
                    let sv = args[0].value()?.into_owned();
                    ((None), Some(sv), (1, 1))
                }
            }
        };

        // Optimized numeric-only path when summing from a numeric-only view
        if let Some(ref sum_view) = sum_view_opt {
            if sum_view.kind_probe() == crate::engine::range_view::RangeKind::NumericOnly {
                let mut total = 0.0f64;
                for row in 0..dims.0 {
                    for col in 0..dims.1 {
                        // Criteria value (padded/broadcast)
                        let cval = if let Some(ref v) = crit_view {
                            v.get_cell(row, col)
                        } else if let Some(ref s) = crit_scalar {
                            s.clone()
                        } else {
                            LiteralValue::Empty
                        };
                        if !criteria_match(&pred, &cval) {
                            continue;
                        }
                        match sum_view.get_cell(row, col) {
                            LiteralValue::Number(n) => total += n,
                            LiteralValue::Int(i) => total += i as f64,
                            _ => {}
                        }
                    }
                }
                return Ok(LiteralValue::Number(total));
            }
        }

        // General path (mixed types, or scalar sum)
        let mut total = 0.0f64;
        for row in 0..dims.0 {
            for col in 0..dims.1 {
                let cval = if let Some(ref v) = crit_view {
                    v.get_cell(row, col)
                } else if let Some(ref s) = crit_scalar {
                    s.clone()
                } else {
                    LiteralValue::Empty
                };
                if !criteria_match(&pred, &cval) {
                    continue;
                }
                let sval = if let Some(ref v) = sum_view_opt {
                    v.get_cell(row, col)
                } else if let Some(ref s) = sum_scalar_opt {
                    s.clone()
                } else {
                    LiteralValue::Empty
                };
                if let Ok(n) = coerce_num(&sval) {
                    total += n;
                }
            }
        }
        Ok(LiteralValue::Number(total))
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() != 2 {
            return Ok(LiteralValue::Error(ExcelError::new_value().with_message(
                format!("COUNTIF expects 2 arguments, got {}", args.len()),
            )));
        }
        let pred = crate::args::parse_criteria(args[1].value()?.as_ref())?;
        let (it, _) = materialize_iter(&args[0]);
        let mut cnt = 0i64;
        for v in it {
            if criteria_match(&pred, &v) {
                cnt += 1;
            }
        }
        Ok(LiteralValue::Number(cnt as f64))
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        #[cfg(feature = "tracing")]
        let _span = tracing::info_span!("SUMIFS").entered();
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return Ok(LiteralValue::Error(
                ExcelError::new_value().with_message(format!(
                    "SUMIFS expects 1 sum_range followed by N pairs (criteria_range, criteria); got {} args",
                    args.len()
                )),
            ));
        }

        // Get the sum range as a RangeView
        let sum_view = match args[0].range_view() {
            Ok(v) => v,
            Err(_) => {
                // Scalar fallback
                let val = args[0].value()?;
                let mut total = 0.0f64;
                // For scalar, we need all criteria to be scalar and match
                for i in (1..args.len()).step_by(2) {
                    let crit_val = args[i].value()?;
                    let pred = crate::args::parse_criteria(args[i + 1].value()?.as_ref())?;
                    if !criteria_match(&pred, crit_val.as_ref()) {
                        return Ok(LiteralValue::Number(0.0));
                    }
                }
                if let Ok(n) = coerce_num(val.as_ref()) {
                    total = n;
                }
                return Ok(LiteralValue::Number(total));
            }
        };

        let mut dims = sum_view.dims();

        // Collect criteria ranges and predicates
        let mut crit_views = Vec::new();
        let mut preds = Vec::new();
        for i in (1..args.len()).step_by(2) {
            let crit_view = match args[i].range_view() {
                Ok(v) => {
                    // Check if it's a 1x1 range (should be treated as scalar)
                    if v.dims() == (1, 1) {
                        // Treat 1x1 as scalar - broadcast it
                        let scalar_val = v.get_cell(0, 0);
                        crit_views.push(None);
                        let p = crate::args::parse_criteria(args[i + 1].value()?.as_ref())?;
                        preds.push((p, Some(scalar_val)));
                        continue;
                    }
                    v
                }
                Err(_) => {
                    // Scalar criteria - we'll handle this specially
                    let val = args[i].value()?;
                    // Create a pseudo-view that will broadcast the scalar
                    // For now, we'll just store a marker and handle it in the loop
                    crit_views.push(None);
                    let p = crate::args::parse_criteria(args[i + 1].value()?.as_ref())?;
                    preds.push((p, Some(val.into_owned())));
                    continue;
                }
            };
            crit_views.push(Some(crit_view));
            let p = crate::args::parse_criteria(args[i + 1].value()?.as_ref())?;
            preds.push((p, None));
        }

        #[cfg(feature = "tracing")]
        tracing::debug!(
            rows = dims.0,
            cols = dims.1,
            criteria = preds.len(),
            "sumifs_dims"
        );

        // Check if we can use the optimized numeric path
        if sum_view.kind_probe() == crate::engine::range_view::RangeKind::NumericOnly {
            // Optimized path for numeric-only sum range
            let mut total = 0.0f64;

            // We'll iterate by rows for union dims (padding via get_cell)
            for row in 0..dims.0 {
                for col in 0..dims.1 {
                    // Check all criteria
                    let mut all_match = true;
                    for (j, (pred, scalar_val)) in preds.iter().enumerate() {
                        let crit_val = if let Some(ref view) = crit_views[j] {
                            view.get_cell(row, col)
                        } else if let Some(scalar) = scalar_val {
                            scalar.clone()
                        } else {
                            LiteralValue::Empty
                        };

                        if !criteria_match(pred, &crit_val) {
                            all_match = false;
                            break;
                        }
                    }

                    if all_match {
                        match sum_view.get_cell(row, col) {
                            LiteralValue::Number(n) => total += n,
                            LiteralValue::Int(i) => total += i as f64,
                            _ => {}
                        }
                    }
                }
            }

            return Ok(LiteralValue::Number(total));
        }

        // General path for mixed or non-numeric ranges over union dims
        let mut total = 0.0f64;
        for row in 0..dims.0 {
            for col in 0..dims.1 {
                // Check all criteria
                let mut all_match = true;
                for (j, (pred, scalar_val)) in preds.iter().enumerate() {
                    let crit_val = if let Some(ref view) = crit_views[j] {
                        view.get_cell(row, col)
                    } else if let Some(scalar) = scalar_val {
                        scalar.clone()
                    } else {
                        LiteralValue::Empty
                    };
                    if !criteria_match(pred, &crit_val) {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    let sum_val = sum_view.get_cell(row, col);
                    if let Ok(n) = coerce_num(&sum_val) {
                        total += n;
                    }
                }
            }
        }
        Ok(LiteralValue::Number(total))
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        #[cfg(feature = "tracing")]
        let _span = tracing::info_span!("COUNTIFS").entered();
        if args.len() < 2 || args.len() % 2 != 0 {
            return Ok(LiteralValue::Error(ExcelError::new_value().with_message(
                format!(
                    "COUNTIFS expects N pairs (criteria_range, criteria); got {} args",
                    args.len()
                ),
            )));
        }
        // Collect criteria as views or scalars; compute union dims
        let mut crit_views: Vec<Option<crate::engine::range_view::RangeView<'_>>> = Vec::new();
        let mut preds = Vec::new();
        let mut dims = (1usize, 1usize);
        let mut seen_any_view = false;
        for i in (0..args.len()).step_by(2) {
            let pred = crate::args::parse_criteria(args[i + 1].value()?.as_ref())?;
            match args[i].range_view() {
                Ok(v) => {
                    if v.dims() == (1, 1) {
                        let scalar = v.get_cell(0, 0);
                        crit_views.push(None);
                        preds.push((pred, Some(scalar)));
                    } else {
                        let vd = v.dims();
                        if !seen_any_view {
                            dims = vd;
                            seen_any_view = true;
                        } else {
                            if vd.0 > dims.0 {
                                dims.0 = vd.0;
                            }
                            if vd.1 > dims.1 {
                                dims.1 = vd.1;
                            }
                        }
                        crit_views.push(Some(v));
                        preds.push((pred, None));
                    }
                }
                Err(_) => {
                    let scalar = args[i].value()?.into_owned();
                    crit_views.push(None);
                    preds.push((pred, Some(scalar)));
                }
            }
        }
        let mut cnt = 0i64;
        for row in 0..dims.0 {
            for col in 0..dims.1 {
                let mut all_match = true;
                for (j, (pred, scalar_val)) in preds.iter().enumerate() {
                    let crit_val = if let Some(ref view) = crit_views[j] {
                        view.get_cell(row, col)
                    } else if let Some(sv) = scalar_val {
                        sv.clone()
                    } else {
                        LiteralValue::Empty
                    };
                    if !criteria_match(pred, &crit_val) {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    cnt += 1;
                }
            }
        }
        Ok(LiteralValue::Number(cnt as f64))
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return Ok(LiteralValue::Error(
                ExcelError::new_value().with_message(format!(
                    "AVERAGEIFS expects 1 avg_range followed by N pairs (criteria_range, criteria); got {} args",
                    args.len()
                )),
            ));
        }
        // Resolve avg range
        let avg_view = match args[0].range_view() {
            Ok(v) => v,
            Err(_) => {
                // Scalar fallback: require scalar criteria and match; else #DIV/0!
                let val = args[0].value()?;
                for i in (1..args.len()).step_by(2) {
                    let cval = args[i].value()?;
                    let pred = crate::args::parse_criteria(args[i + 1].value()?.as_ref())?;
                    if !criteria_match(&pred, cval.as_ref()) {
                        return Ok(ExcelError::new_div().into());
                    }
                }
                if let Ok(n) = coerce_num(val.as_ref()) {
                    return Ok(LiteralValue::Number(n));
                } else {
                    return Ok(ExcelError::new_div().into());
                }
            }
        };

        // Collect criteria as views or scalars; compute union dims with avg_view
        let mut dims = avg_view.dims();
        let mut crit_views: Vec<Option<crate::engine::range_view::RangeView<'_>>> = Vec::new();
        let mut preds = Vec::new();
        for i in (1..args.len()).step_by(2) {
            let pred = crate::args::parse_criteria(args[i + 1].value()?.as_ref())?;
            match args[i].range_view() {
                Ok(v) => {
                    if v.dims() == (1, 1) {
                        let scalar = v.get_cell(0, 0);
                        crit_views.push(None);
                        preds.push((pred, Some(scalar)));
                    } else {
                        // Do not expand avg_range dimensions; pad criteria to avg_range dims
                        crit_views.push(Some(v));
                        preds.push((pred, None));
                    }
                }
                Err(_) => {
                    let scalar = args[i].value()?.into_owned();
                    crit_views.push(None);
                    preds.push((pred, Some(scalar)));
                }
            }
        }

        let mut sum = 0.0f64;
        let mut cnt = 0i64;
        for row in 0..dims.0 {
            for col in 0..dims.1 {
                let mut all_match = true;
                for (j, (pred, scalar_val)) in preds.iter().enumerate() {
                    let crit_val = if let Some(ref view) = crit_views[j] {
                        view.get_cell(row, col)
                    } else if let Some(sv) = scalar_val {
                        sv.clone()
                    } else {
                        LiteralValue::Empty
                    };
                    if !criteria_match(pred, &crit_val) {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    let v = avg_view.get_cell(row, col);
                    if let Ok(n) = coerce_num(&v) {
                        sum += n;
                        cnt += 1;
                    }
                }
            }
        }
        if cnt == 0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#DIV/0!",
            )));
        }
        Ok(LiteralValue::Number(sum / cnt as f64))
    }
}

/* ─────────────────────────── COUNTA() ──────────────────────────── */
#[derive(Debug)]
pub struct CountAFn; // counts non-empty (including empty text "")
impl Function for CountAFn {
    func_caps!(PURE, REDUCTION, WINDOWED, STREAM_OK);
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let mut cnt = 0i64;
        for a in args {
            let (it, _) = materialize_iter(a);
            for v in it {
                match v {
                    LiteralValue::Empty => {}
                    _ => cnt += 1,
                }
            }
        }
        Ok(LiteralValue::Number(cnt as f64))
    }
}

/* ─────────────────────────── COUNTBLANK() ──────────────────────────── */
#[derive(Debug)]
pub struct CountBlankFn; // counts truly empty cells and empty text
impl Function for CountBlankFn {
    func_caps!(PURE, REDUCTION, WINDOWED, STREAM_OK);
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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        let mut cnt = 0i64;
        for a in args {
            let (it, _) = materialize_iter(a);
            for v in it {
                match v {
                    LiteralValue::Empty => cnt += 1,
                    LiteralValue::Text(ref s) if s.is_empty() => cnt += 1,
                    _ => {}
                }
            }
        }
        Ok(LiteralValue::Number(cnt as f64))
    }
}

// Helper: materialize an argument (range or scalar) into an iterator of values and its 2D dims representation.
fn materialize_iter<'a, 'b>(
    arg: &'a ArgumentHandle<'a, 'b>,
) -> (Box<dyn Iterator<Item = LiteralValue> + 'a>, (usize, usize)) {
    if let Ok(view) = arg.range_view() {
        let d = view.dims();
        let mut values: Vec<LiteralValue> = Vec::with_capacity(d.0 * d.1);
        // Re-resolve for borrow: the previous `view` is moved; get a fresh one
        if let Ok(rv2) = arg.range_view() {
            rv2.for_each_cell(&mut |cell| {
                values.push(cell.clone());
                Ok(())
            })
            .ok();
        }
        (Box::new(values.into_iter()), d)
    } else {
        let v = arg.value().unwrap().into_owned();
        (Box::new(std::iter::once(v)), (1, 1))
    }
}

pub fn register_builtins() {
    use std::sync::Arc;
    crate::function_registry::register_function(Arc::new(SumIfFn));
    crate::function_registry::register_function(Arc::new(CountIfFn));
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
    use formualizer_core::parser::{ASTNode, ASTNodeType};
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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
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
        match f.dispatch(&args, &ctx.function_context(None)).unwrap() {
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
            counta.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            countblank
                .dispatch(&args, &ctx.function_context(None))
                .unwrap(),
            LiteralValue::Number(2.0)
        );
    }

    // ───────── Parity tests (window vs scalar) ─────────
    #[test]
    #[ignore]
    fn sumif_window_parity() {
        let f = SumIfFn; // direct instance
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
        let fctx = ctx.function_context(None);
        let mut wctx = crate::window_ctx::SimpleWindowCtx::new(
            &args,
            &fctx,
            crate::window_ctx::WindowSpec::default(),
        );
        let window_val = f.eval_window(&mut wctx).expect("window path").unwrap();
        let scalar = f.eval_scalar(&args, &fctx).unwrap();
        assert_eq!(window_val, scalar);
    }

    #[test]
    #[ignore]
    fn sumifs_window_parity() {
        let f = SumIfsFn;
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
        let fctx = ctx.function_context(None);
        let mut wctx = crate::window_ctx::SimpleWindowCtx::new(
            &args,
            &fctx,
            crate::window_ctx::WindowSpec::default(),
        );
        let window_val = f.eval_window(&mut wctx).expect("window path").unwrap();
        let scalar = f.eval_scalar(&args, &fctx).unwrap();
        assert_eq!(window_val, scalar);
    }

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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
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
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
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
    #[ignore]
    fn countifs_window_parity() {
        let f = CountIfsFn;
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
        let fctx = ctx.function_context(None);
        let mut wctx = crate::window_ctx::SimpleWindowCtx::new(
            &args,
            &fctx,
            crate::window_ctx::WindowSpec::default(),
        );
        let window_val = f.eval_window(&mut wctx).expect("window path").unwrap();
        let scalar = f.eval_scalar(&args, &fctx).unwrap();
        assert_eq!(window_val, scalar);
    }

    #[test]
    #[ignore]
    fn averageifs_window_parity() {
        let f = AverageIfsFn;
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(AverageIfsFn));
        let ctx = interp(&wb);
        let avg = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(10),
            LiteralValue::Int(20),
            LiteralValue::Int(30),
        ]]));
        let crit_rng = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(0),
            LiteralValue::Int(1),
            LiteralValue::Int(1),
        ]]));
        let crit = lit(LiteralValue::Text(">0".into()));
        let args = vec![
            ArgumentHandle::new(&avg, &ctx),
            ArgumentHandle::new(&crit_rng, &ctx),
            ArgumentHandle::new(&crit, &ctx),
        ];
        let fctx = ctx.function_context(None);
        let mut wctx = crate::window_ctx::SimpleWindowCtx::new(
            &args,
            &fctx,
            crate::window_ctx::WindowSpec::default(),
        );
        let window_val = f.eval_window(&mut wctx).expect("window path").unwrap();
        let scalar = f.eval_scalar(&args, &fctx).unwrap();
        assert_eq!(window_val, scalar);
    }
    #[test]
    #[ignore]
    fn counta_window_parity() {
        let f = CountAFn;
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountAFn));
        let ctx = interp(&wb);
        let arr = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Empty,
            LiteralValue::Int(1),
            LiteralValue::Text("".into()),
        ]]));
        let args = vec![ArgumentHandle::new(&arr, &ctx)];
        let fctx = ctx.function_context(None);
        let mut wctx = crate::window_ctx::SimpleWindowCtx::new(
            &args,
            &fctx,
            crate::window_ctx::WindowSpec::default(),
        );
        let window_val = f.eval_window(&mut wctx).expect("window path").unwrap();
        let scalar = f.eval_scalar(&args, &fctx).unwrap();
        assert_eq!(window_val, scalar);
    }
    #[test]
    #[ignore]
    fn countblank_window_parity() {
        let f = CountBlankFn;
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountBlankFn));
        let ctx = interp(&wb);
        let arr = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Empty,
            LiteralValue::Int(1),
            LiteralValue::Text("".into()),
        ]]));
        let args = vec![ArgumentHandle::new(&arr, &ctx)];
        let fctx = ctx.function_context(None);
        let mut wctx = crate::window_ctx::SimpleWindowCtx::new(
            &args,
            &fctx,
            crate::window_ctx::WindowSpec::default(),
        );
        let window_val = f.eval_window(&mut wctx).expect("window path").unwrap();
        let scalar = f.eval_scalar(&args, &fctx).unwrap();
        assert_eq!(window_val, scalar);
    }

    // ───────── Criteria parsing edge cases ─────────
    #[test]
    fn criteria_numeric_string_vs_number() {
        // SUMIF over numeric cells with criteria expressed as text ">=2" and "=3"
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumIfFn));
        let ctx = interp(&wb);
        let range = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Number(2.0),
            LiteralValue::Int(3),
        ]]));
        let ge2 = lit(LiteralValue::Text(">=2".into()));
        let eq3 = lit(LiteralValue::Text("=3".into()));
        let args_ge2 = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&ge2, &ctx),
        ];
        let args_eq3 = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&eq3, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMIF").unwrap();
        assert_eq!(
            f.dispatch(&args_ge2, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(5.0)
        ); // 2+3
        assert_eq!(
            f.dispatch(&args_eq3, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(3.0)
        );
    }

    #[test]
    fn criteria_wildcards_patterns() {
        // COUNTIF with wildcard patterns
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountIfFn));
        let ctx = interp(&wb);
        let data = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Text("alpha".into()),
            LiteralValue::Text("alphabet".into()),
            LiteralValue::Text("alp".into()),
            LiteralValue::Text("al".into()),
            LiteralValue::Text("beta".into()),
        ]]));
        let pat_al_star = lit(LiteralValue::Text("al*".into())); // matches all starting with al
        let pat_q = lit(LiteralValue::Text("alp?".into())); // matches four-char starting alp?
        let pat_star_et = lit(LiteralValue::Text("*et".into())); // ends with et
        let f = ctx.context.get_function("", "COUNTIF").unwrap();
        let ctxf = ctx.function_context(None);
        // Current wildcard matcher is case-sensitive and non-greedy but supports * and ?; pattern 'al*' should match alpha, alphabet, alp, al (4)
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&data, &ctx),
                    ArgumentHandle::new(&pat_al_star, &ctx)
                ],
                &ctxf
            )
            .unwrap(),
            LiteralValue::Number(4.0)
        );
        // 'alp?' matches exactly four-char strings starting with 'alp'. We have 'alph' prefix inside 'alpha' but pattern must consume entire string, so only 'alp?' -> no exact 4-length match; expect 0.
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&data, &ctx),
                    ArgumentHandle::new(&pat_q, &ctx)
                ],
                &ctxf
            )
            .unwrap(),
            LiteralValue::Number(0.0)
        );
        // '*et' matches words ending with 'et' (alphabet)
        assert_eq!(
            f.dispatch(
                &[
                    ArgumentHandle::new(&data, &ctx),
                    ArgumentHandle::new(&pat_star_et, &ctx)
                ],
                &ctxf
            )
            .unwrap(),
            LiteralValue::Number(1.0)
        );
    }

    #[test]
    fn criteria_boolean_text_and_numeric_equivalence() {
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountIfFn));
        let ctx = interp(&wb);
        let data = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Boolean(true),
            LiteralValue::Boolean(false),
            LiteralValue::Text("TRUE".into()),
            LiteralValue::Int(1),
            LiteralValue::Int(0),
        ]]));
        // Criteria TRUE should match Boolean(true) only (NOT text TRUE unless equality logic coerces); we rely on current parse -> Eq(Boolean(true))
        let crit_true = lit(LiteralValue::Text("TRUE".into()));
        let args_true = vec![
            ArgumentHandle::new(&data, &ctx),
            ArgumentHandle::new(&crit_true, &ctx),
        ];
        let f = ctx.context.get_function("", "COUNTIF").unwrap();
        let res = f.dispatch(&args_true, &ctx.function_context(None)).unwrap();
        // Expect 1 match (the boolean true) because Text("TRUE") is parsed to boolean predicate Eq(Boolean(true))
        assert_eq!(res, LiteralValue::Number(1.0));
    }

    #[test]
    fn criteria_empty_and_blank() {
        // COUNTIF to distinguish blank vs non-blank using criteria "=" and "<>" patterns
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(CountIfFn));
        let ctx = interp(&wb);
        let arr = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Empty,
            LiteralValue::Text("".into()),
            LiteralValue::Text(" ".into()),
            LiteralValue::Int(0),
        ]]));
        let crit_blank = lit(LiteralValue::Text("=".into())); // equality with empty -> treated as Eq(Text("")) by parser? Actually '=' prefix branch with rhs '' -> Eq(Number?) fallback -> becomes Eq(Text(""))
        let crit_not_blank = lit(LiteralValue::Text("<>".into())); // Eq(Text("<>")) fallback due to parse path; document current semantics
        let f = ctx.context.get_function("", "COUNTIF").unwrap();
        let ctxf = ctx.function_context(None);
        let blank_result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&arr, &ctx),
                    ArgumentHandle::new(&crit_blank, &ctx),
                ],
                &ctxf,
            )
            .unwrap();
        // Current parser: '=' recognized, rhs empty -> numeric parse fails, becomes Eq(Text("")) so matches Empty? criteria_match treats Eq(Text) vs Empty -> false, so only Text("") counts.
        // After equality adjustment, '=' with empty rhs matches both true blank and empty text => expect 2.
        assert_eq!(blank_result, LiteralValue::Number(2.0));
        let not_blank_result = f
            .dispatch(
                &[
                    ArgumentHandle::new(&arr, &ctx),
                    ArgumentHandle::new(&crit_not_blank, &ctx),
                ],
                &ctxf,
            )
            .unwrap();
        // Expect 0 with current simplistic parsing (since becomes Eq(Text("<>")) none match) -> acts as regression guard; adjust if semantics improved later.
        // '<>' with empty rhs -> Ne(Text("")) now excludes both blank and empty text; counts others (space, 0) => 2.
        assert_eq!(not_blank_result, LiteralValue::Number(2.0));
    }

    #[test]
    fn criteria_non_numeric_relational_fallback() {
        // SUMIF with relational operator against non-numeric should degrade to equality on full string per parse_criteria implementation comment.
        let wb = TestWorkbook::new().with_function(std::sync::Arc::new(SumIfFn));
        let ctx = interp(&wb);
        let range = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Text("apple".into()),
            LiteralValue::Text("banana".into()),
        ]]));
        let sum_range = lit(LiteralValue::Array(vec![vec![
            LiteralValue::Int(10),
            LiteralValue::Int(20),
        ]]));
        let crit = lit(LiteralValue::Text(">apple".into())); // will parse '>' then fail numeric parse -> equality on full expression '>apple'
        let args = vec![
            ArgumentHandle::new(&range, &ctx),
            ArgumentHandle::new(&crit, &ctx),
            ArgumentHandle::new(&sum_range, &ctx),
        ];
        let f = ctx.context.get_function("", "SUMIF").unwrap();
        // No element equals the literal string '>apple'
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None)).unwrap(),
            LiteralValue::Number(0.0)
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
