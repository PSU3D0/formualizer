//! Stack / concatenation dynamic array functions: HSTACK, VSTACK
//!
//! Excel semantics (baseline subset):
//! - Each function accepts 1..N arrays/ranges; scalars treated as 1x1.
//! - HSTACK: concatenate arrays horizontally (columns) aligning rows; differing row counts -> #VALUE!.
//! - VSTACK: concatenate arrays vertically (rows) aligning columns; differing column counts -> #VALUE!.
//! - Empty arguments (zero-sized ranges) are skipped; if all skipped -> empty spill.
//! - Result collapses to scalar if 1x1 after stacking (consistent with existing dynamic functions here).
//!
//! TODO(excel-nuance): Propagate first error cell wise; currently a whole argument that is an Error scalar becomes a 1x1 error block.
//! TODO(perf): Avoid intermediate full materialization by streaming row-wise/col-wise (later optimization).

use super::super::utils::collapse_if_scalar;
use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ArgKind, ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;

#[derive(Debug)]
pub struct HStackFn;
#[derive(Debug)]
pub struct VStackFn;

fn materialize_arg<'b>(
    arg: &ArgumentHandle<'_, 'b>,
    ctx: &dyn FunctionContext<'b>,
) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
    // Similar helper to dynamic.rs (avoid cyclic import). Minimal duplication; consider refactor later.
    if let Ok(r) = arg.as_reference_or_eval() {
        let mut rows: Vec<Vec<LiteralValue>> = Vec::new();
        let sheet = ctx.current_sheet();
        let rv = ctx.resolve_range_view(&r, sheet)?;
        rv.for_each_row(&mut |row| {
            rows.push(row.to_vec());
            Ok(())
        })?;
        Ok(rows)
    } else {
        let cv = arg.value()?;
        match cv.into_literal() {
            LiteralValue::Array(a) => Ok(a),
            v => Ok(vec![vec![v]]),
        }
    }
}

/// Concatenates arrays horizontally into a single spilled array.
///
/// `HSTACK` appends columns from each argument left-to-right.
///
/// # Remarks
/// - All non-empty range arguments must have the same row count.
/// - Scalar arguments are treated as 1x1 values.
/// - Mismatched row counts return `#VALUE!`.
/// - Empty inputs are skipped; if all inputs are empty, result is an empty spill.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Join two 2-row ranges side by side"
/// grid:
///   A1: 1
///   A2: 2
///   B1: 10
///   B2: 20
/// formula: '=HSTACK(A1:A2,B1:B2)'
/// expected: [[1,10],[2,20]]
/// ```
///
/// ```yaml,sandbox
/// title: "Append a scalar column"
/// grid:
///   A1: "Item"
/// formula: '=HSTACK(A1,"OK")'
/// expected: [["Item","OK"]]
/// ```
/// [formualizer-docgen:schema:start]
/// Name: HSTACK
/// Type: HStackFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: HSTACK(arg1...: range|any@range)
/// Arg schema: arg1{kinds=range|any,required=true,shape=range,by_ref=false,coercion=None,max=None,repeating=Some(1),default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for HStackFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "HSTACK"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![ArgSchema {
                kinds: smallvec::smallvec![ArgKind::Range, ArgKind::Any],
                required: true,
                by_ref: false,
                shape: ShapeKind::Range,
                coercion: CoercionPolicy::None,
                max: None,
                repeating: Some(1),
                default: None,
            }]
        });
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], ctx.date_system()),
            ));
        }

        let mut entries = Vec::with_capacity(args.len());
        let mut target_rows: Option<usize> = None;
        let mut total_cols = 0;

        for a in args {
            if let Ok(v) = a.range_view() {
                let (rows, cols) = v.dims();
                if rows == 0 || cols == 0 {
                    continue;
                }
                if let Some(tr) = target_rows {
                    if rows != tr {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Value),
                        )));
                    }
                } else {
                    target_rows = Some(rows);
                }
                total_cols += cols;
                entries.push(HStackEntry::View(v));
            } else {
                let v = a.value()?.into_literal();
                if let Some(tr) = target_rows {
                    if tr != 1 {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Value),
                        )));
                    }
                } else {
                    target_rows = Some(1);
                }
                total_cols += 1;
                entries.push(HStackEntry::Scalar(v));
            }
        }

        if entries.is_empty() {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], ctx.date_system()),
            ));
        }

        let row_count = target_rows.unwrap();
        let mut result: Vec<Vec<LiteralValue>> = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            result.push(Vec::with_capacity(total_cols));
        }

        for entry in entries {
            match entry {
                HStackEntry::View(v) => {
                    let (v_rows, v_cols) = v.dims();
                    for (r, row) in result.iter_mut().enumerate().take(v_rows) {
                        for c in 0..v_cols {
                            row.push(v.get_cell(r, c));
                        }
                    }
                }
                HStackEntry::Scalar(s) => {
                    result[0].push(s);
                }
            }
        }

        Ok(collapse_if_scalar(result, ctx.date_system()))
    }
}

enum HStackEntry<'a> {
    View(crate::engine::range_view::RangeView<'a>),
    Scalar(LiteralValue),
}

/// Concatenates arrays vertically into a single spilled array.
///
/// `VSTACK` appends rows from each argument top-to-bottom.
///
/// # Remarks
/// - All non-empty range arguments must have the same column count.
/// - Scalar arguments are treated as 1x1 values.
/// - Mismatched column counts return `#VALUE!`.
/// - Empty inputs are skipped; if all inputs are empty, result is an empty spill.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Join two 1-row ranges vertically"
/// grid:
///   A1: 1
///   B1: 10
///   A2: 2
///   B2: 20
/// formula: '=VSTACK(A1:B1,A2:B2)'
/// expected: [[1,10],[2,20]]
/// ```
///
/// ```yaml,sandbox
/// title: "Stack scalar values"
/// formula: '=VSTACK(5,9)'
/// expected: [[5],[9]]
/// ```
/// [formualizer-docgen:schema:start]
/// Name: VSTACK
/// Type: VStackFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: VSTACK(arg1...: range|any@range)
/// Arg schema: arg1{kinds=range|any,required=true,shape=range,by_ref=false,coercion=None,max=None,repeating=Some(1),default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for VStackFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "VSTACK"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![ArgSchema {
                kinds: smallvec::smallvec![ArgKind::Range, ArgKind::Any],
                required: true,
                by_ref: false,
                shape: ShapeKind::Range,
                coercion: CoercionPolicy::None,
                max: None,
                repeating: Some(1),
                default: None,
            }]
        });
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], ctx.date_system()),
            ));
        }

        let mut target_width: Option<usize> = None;
        let mut total_rows = 0;
        let mut entries = Vec::with_capacity(args.len());

        for a in args {
            if let Ok(v) = a.range_view() {
                let (rows, cols) = v.dims();
                if rows == 0 || cols == 0 {
                    continue;
                }
                if let Some(tw) = target_width {
                    if cols != tw {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Value),
                        )));
                    }
                } else {
                    target_width = Some(cols);
                }
                total_rows += rows;
                entries.push(VStackEntry::View(v));
            } else {
                let v = a.value()?.into_literal();
                if let Some(tw) = target_width {
                    if tw != 1 {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Value),
                        )));
                    }
                } else {
                    target_width = Some(1);
                }
                total_rows += 1;
                entries.push(VStackEntry::Scalar(v));
            }
        }

        if entries.is_empty() {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], ctx.date_system()),
            ));
        }

        let mut result: Vec<Vec<LiteralValue>> = Vec::with_capacity(total_rows);
        for entry in entries {
            match entry {
                VStackEntry::View(v) => {
                    let _ = v.for_each_row(&mut |row| {
                        result.push(row.to_vec());
                        Ok(())
                    });
                }
                VStackEntry::Scalar(s) => {
                    result.push(vec![s]);
                }
            }
        }

        Ok(collapse_if_scalar(result, ctx.date_system()))
    }
}

enum VStackEntry<'a> {
    View(crate::engine::range_view::RangeView<'a>),
    Scalar(LiteralValue),
}

pub fn register_builtins() {
    use crate::function_registry::register_function;
    use std::sync::Arc;
    register_function(Arc::new(HStackFn));
    register_function(Arc::new(VStackFn));
}

/* ───────────────────────── tests ───────────────────────── */
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
    use std::sync::Arc;

    fn ref_range(r: &str, sr: i32, sc: i32, er: i32, ec: i32) -> ASTNode {
        ASTNode::new(
            ASTNodeType::Reference {
                original: r.into(),
                reference: ReferenceType::range(
                    None,
                    Some(sr as u32),
                    Some(sc as u32),
                    Some(er as u32),
                    Some(ec as u32),
                ),
            },
            None,
        )
    }

    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn hstack_basic_and_mismatched_rows() {
        let wb = TestWorkbook::new().with_function(Arc::new(HStackFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "C1", LiteralValue::Int(100)); // single row range for mismatch
        let ctx = wb.interpreter();
        let left = ref_range("A1:A2", 1, 1, 2, 1);
        let right = ref_range("B1:B2", 1, 2, 2, 2);
        let f = ctx.context.get_function("", "HSTACK").unwrap();
        let args = vec![
            ArgumentHandle::new(&left, &ctx),
            ArgumentHandle::new(&right, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(
                    a[0],
                    vec![LiteralValue::Number(1.0), LiteralValue::Number(10.0)]
                );
            }
            other => panic!("expected array got {other:?}"),
        }
        // mismatch rows
        let mism = ref_range("C1:C1", 1, 3, 1, 3);
        let args_bad = vec![
            ArgumentHandle::new(&left, &ctx),
            ArgumentHandle::new(&mism, &ctx),
        ];
        let v_bad = f
            .dispatch(&args_bad, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v_bad {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Value),
            other => panic!("expected #VALUE! got {other:?}"),
        }
    }

    #[test]
    fn vstack_basic_and_mismatched_cols() {
        let wb = TestWorkbook::new().with_function(Arc::new(VStackFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "C1", LiteralValue::Int(100))
            .with_cell_a1("Sheet1", "C2", LiteralValue::Int(200));
        let ctx = wb.interpreter();
        let top = ref_range("A1:B1", 1, 1, 1, 2);
        let bottom = ref_range("A2:B2", 2, 1, 2, 2);
        let f = ctx.context.get_function("", "VSTACK").unwrap();
        let args = vec![
            ArgumentHandle::new(&top, &ctx),
            ArgumentHandle::new(&bottom, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(
                    a[0],
                    vec![LiteralValue::Number(1.0), LiteralValue::Number(10.0)]
                );
            }
            other => panic!("expected array got {other:?}"),
        }
        // mismatched width (add 3rd column row)
        let extra = ref_range("A1:C1", 1, 1, 1, 3);
        let args_bad = vec![
            ArgumentHandle::new(&top, &ctx),
            ArgumentHandle::new(&extra, &ctx),
        ];
        let v_bad = f
            .dispatch(&args_bad, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v_bad {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Value),
            other => panic!("expected #VALUE! got {other:?}"),
        }
    }

    #[test]
    fn hstack_scalar_and_array_collapse() {
        let wb = TestWorkbook::new().with_function(Arc::new(HStackFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "HSTACK").unwrap();
        let s1 = lit(LiteralValue::Int(5));
        let s2 = lit(LiteralValue::Int(6));
        let args = vec![
            ArgumentHandle::new(&s1, &ctx),
            ArgumentHandle::new(&s2, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        // 1 row x 2 cols stays as array (not scalar collapse)
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 1);
                assert_eq!(
                    a[0],
                    vec![LiteralValue::Number(5.0), LiteralValue::Number(6.0)]
                );
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn vstack_scalar_collapse_single_result() {
        let wb = TestWorkbook::new().with_function(Arc::new(VStackFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "VSTACK").unwrap();
        let lone = lit(LiteralValue::Int(9));
        let args = vec![ArgumentHandle::new(&lone, &ctx)];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Int(9));
    }
}
