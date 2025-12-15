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

use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ArgKind, ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;

#[derive(Debug)]
pub struct HStackFn;
#[derive(Debug)]
pub struct VStackFn;

fn materialize_arg(
    arg: &ArgumentHandle,
    ctx: &dyn FunctionContext,
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
        match arg.value()?.as_ref() {
            LiteralValue::Array(a) => Ok(a.clone()),
            v => Ok(vec![vec![v.clone()]]),
        }
    }
}

fn collapse_if_scalar(mut rows: Vec<Vec<LiteralValue>>) -> LiteralValue {
    if rows.len() == 1 && rows[0].len() == 1 {
        return rows.remove(0).remove(0);
    }
    LiteralValue::Array(rows)
}

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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.is_empty() {
            return Ok(LiteralValue::Array(vec![]));
        }
        let mut blocks: Vec<Vec<Vec<LiteralValue>>> = Vec::new();
        let mut target_rows: Option<usize> = None;
        for a in args {
            let rows = materialize_arg(a, ctx)?;
            if rows.is_empty() {
                continue;
            }
            if let Some(tr) = target_rows {
                if rows.len() != tr {
                    return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
                }
            } else {
                target_rows = Some(rows.len());
            }
            blocks.push(rows);
        }
        if blocks.is_empty() {
            return Ok(LiteralValue::Array(vec![]));
        }
        let row_count = target_rows.unwrap();
        // Compute total width (use first row lengths; mismatched row internal widths cause #VALUE!)
        for b in &blocks {
            // rectangular validation inside block
            let w = b[0].len();
            if b.iter().any(|r| r.len() != w) {
                return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
            }
        }
        let mut result: Vec<Vec<LiteralValue>> = Vec::with_capacity(row_count);
        for r in 0..row_count {
            result.push(Vec::new());
        }
        for b in blocks {
            for (r, row_vec) in b.into_iter().enumerate() {
                result[r].extend(row_vec);
            }
        }
        Ok(collapse_if_scalar(result))
    }
}

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
    fn eval_scalar<'a, 'b>(
        &self,
        args: &'a [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext,
    ) -> Result<LiteralValue, ExcelError> {
        if args.is_empty() {
            return Ok(LiteralValue::Array(vec![]));
        }
        let mut blocks: Vec<Vec<Vec<LiteralValue>>> = Vec::new();
        let mut target_width: Option<usize> = None;
        for a in args {
            let rows = materialize_arg(a, ctx)?;
            if rows.is_empty() {
                continue;
            }
            // Determine width (validate rectangular within block)
            let width = rows[0].len();
            if rows.iter().any(|r| r.len() != width) {
                return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
            }
            if let Some(tw) = target_width {
                if width != tw {
                    return Ok(LiteralValue::Error(ExcelError::new(ExcelErrorKind::Value)));
                }
            } else {
                target_width = Some(width);
            }
            blocks.push(rows);
        }
        if blocks.is_empty() {
            return Ok(LiteralValue::Array(vec![]));
        }
        let mut result: Vec<Vec<LiteralValue>> = Vec::new();
        for b in blocks {
            result.extend(b);
        }
        Ok(collapse_if_scalar(result))
    }
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
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(sr as u32),
                    start_col: Some(sc as u32),
                    end_row: Some(er as u32),
                    end_col: Some(ec as u32),
                },
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
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
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
        let v_bad = f.dispatch(&args_bad, &ctx.function_context(None)).unwrap();
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
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
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
        let v_bad = f.dispatch(&args_bad, &ctx.function_context(None)).unwrap();
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
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        // 1 row x 2 cols stays as array (not scalar collapse)
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 1);
                assert_eq!(a[0], vec![LiteralValue::Int(5), LiteralValue::Int(6)]);
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
        let v = f.dispatch(&args, &ctx.function_context(None)).unwrap();
        assert_eq!(v, LiteralValue::Int(9));
    }
}
