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
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let pred = crate::args::parse_criteria(args[1].value()?.as_ref())?;
        let (mut crit_iter, dims) = materialize_iter(&args[0]);
        let (mut sum_iter, dims_sum) = if args.len() == 3 {
            materialize_iter(&args[2])
        } else {
            materialize_iter(&args[0])
        };
        if dims != dims_sum {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let mut total = 0.0f64;
        while let (Some(c), Some(s)) = (crit_iter.next(), sum_iter.next()) {
            if criteria_match(&pred, &c) {
                if let Ok(n) = coerce_num(&s) {
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
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
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
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let (sum_it, dims) = materialize_iter(&args[0]);
        let mut crit_iters = Vec::new();
        let mut preds = Vec::new();
        for i in (1..args.len()).step_by(2) {
            let (iter, d) = materialize_iter(&args[i]);
            if d != dims {
                return Ok(LiteralValue::Error(ExcelError::from_error_string(
                    "#VALUE!",
                )));
            }
            crit_iters.push(iter);
            let p = crate::args::parse_criteria(args[i + 1].value()?.as_ref())?;
            preds.push(p);
        }
        let crit_values: Vec<Vec<LiteralValue>> =
            crit_iters.into_iter().map(|it| it.collect()).collect();
        let sum_values: Vec<LiteralValue> = sum_it.collect();
        let len = sum_values.len();
        let mut total = 0.0f64;
        for (idx, val) in sum_values.iter().enumerate() {
            if preds
                .iter()
                .enumerate()
                .all(|(j, p)| criteria_match(p, &crit_values[j][idx]))
            {
                if let Ok(n) = coerce_num(val) {
                    total += n;
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
        if args.len() < 2 || args.len() % 2 != 0 {
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let mut crit_iters = Vec::new();
        let mut preds = Vec::new();
        let mut dims: Option<(usize, usize)> = None;
        for i in (0..args.len()).step_by(2) {
            let (iter, d) = materialize_iter(&args[i]);
            if let Some(dd) = dims {
                if dd != d {
                    return Ok(LiteralValue::Error(ExcelError::from_error_string(
                        "#VALUE!",
                    )));
                }
            } else {
                dims = Some(d);
            }
            crit_iters.push(iter);
            preds.push(crate::args::parse_criteria(args[i + 1].value()?.as_ref())?);
        }
        let crit_values: Vec<Vec<LiteralValue>> =
            crit_iters.into_iter().map(|it| it.collect()).collect();
        let len = crit_values[0].len();
        let mut cnt = 0i64;
        for (idx, _) in crit_values[0].iter().enumerate() {
            if preds
                .iter()
                .enumerate()
                .all(|(j, p)| criteria_match(p, &crit_values[j][idx]))
            {
                cnt += 1;
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
            return Ok(LiteralValue::Error(ExcelError::from_error_string(
                "#VALUE!",
            )));
        }
        let (avg_it, dims) = materialize_iter(&args[0]);
        let mut crit_iters = Vec::new();
        let mut preds = Vec::new();
        for i in (1..args.len()).step_by(2) {
            let (iter, d) = materialize_iter(&args[i]);
            if d != dims {
                return Ok(LiteralValue::Error(ExcelError::from_error_string(
                    "#VALUE!",
                )));
            }
            crit_iters.push(iter);
            preds.push(crate::args::parse_criteria(args[i + 1].value()?.as_ref())?);
        }
        let crit_values: Vec<Vec<LiteralValue>> =
            crit_iters.into_iter().map(|it| it.collect()).collect();
        let avg_values: Vec<LiteralValue> = avg_it.collect();
        let len = avg_values.len();
        let mut sum = 0.0f64;
        let mut cnt = 0i64;
        for (idx, val) in avg_values.iter().enumerate() {
            if preds
                .iter()
                .enumerate()
                .all(|(j, p)| criteria_match(p, &crit_values[j][idx]))
            {
                if let Ok(n) = coerce_num(val) {
                    sum += n;
                    cnt += 1;
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
