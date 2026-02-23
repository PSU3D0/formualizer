//! CHOOSE function - selects a value from a list based on an index
//!
//! Excel semantics:
//! - CHOOSE(index_num, value1, [value2], ...)
//! - index_num must be between 1 and the number of values
//! - Returns #VALUE! if index is out of range or not numeric
//! - Can return references, not just values

use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use crate::builtins::utils::collapse_if_scalar;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ArgKind, ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;

#[derive(Debug)]
pub struct ChooseFn;
#[derive(Debug)]
pub struct ChooseColsFn;
#[derive(Debug)]
pub struct ChooseRowsFn;

/// Returns one value from a list using a 1-based index.
///
/// `CHOOSE` selects from `value1, value2, ...` based on `index_num`.
///
/// # Remarks
/// - `index_num` is 1-based and is truncated to an integer.
/// - Indexes less than 1 or greater than the number of choices return `#VALUE!`.
/// - Errors in `index_num` are propagated.
/// - The selected argument is returned as-is, including non-text/non-numeric values.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Select a label"
/// formula: '=CHOOSE(2,"Low","Medium","High")'
/// expected: "Medium"
/// ```
///
/// ```yaml,sandbox
/// title: "Use numeric choices"
/// formula: '=CHOOSE(3,10,20,30,40)'
/// expected: 30
/// ```
/// [formualizer-docgen:schema:start]
/// Name: CHOOSE
/// Type: ChooseFn
/// Min args: 2
/// Max args: 2
/// Variadic: false
/// Signature: CHOOSE(arg1: number@scalar, arg2: any@scalar)
/// Arg schema: arg1{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberStrict,max=None,repeating=None,default=false}; arg2{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=Some(1),default=false}
/// Caps: PURE, LOOKUP
/// [formualizer-docgen:schema:end]
impl Function for ChooseFn {
    fn name(&self) -> &'static str {
        "CHOOSE"
    }

    fn min_args(&self) -> usize {
        2
    }

    func_caps!(PURE, LOOKUP);

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // index_num (strict numeric)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberStrict,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // value1, value2, ... (variadic, any type)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: true,
                    by_ref: false, // Could be reference but we'll unwrap value or pass through
                    shape: ShapeKind::Scalar, // Treat each choice as scalar (top-left if range)
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: Some(1), // any number of choices after index
                    default: None,
                },
            ]
        });
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Get index
        let index_val = args[0].value()?.into_literal();
        if let LiteralValue::Error(e) = index_val {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
        }

        let index = match index_val {
            LiteralValue::Number(n) => n as i64,
            LiteralValue::Int(i) => i,
            LiteralValue::Text(s) => s.parse::<f64>().map(|n| n as i64).unwrap_or(-1),
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
        };

        // Check bounds
        if index < 1 || index as usize > args.len() - 1 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Return the selected value (1-based indexing for the choice)
        let selected_arg = &args[index as usize];
        selected_arg.value()
    }
}

/* ───────────────────────── CHOOSECOLS() / CHOOSEROWS() ───────────────────────── */

fn materialize_rows_2d<'b>(
    arg: &ArgumentHandle<'_, 'b>,
    ctx: &dyn FunctionContext<'b>,
) -> Result<Vec<Vec<formualizer_common::LiteralValue>>, ExcelError> {
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
        let v = arg.value()?.into_literal();
        match v {
            LiteralValue::Array(a) => Ok(a),
            other => Ok(vec![vec![other]]),
        }
    }
}

/// Returns selected columns from an array or range.
///
/// `CHOOSECOLS` builds a new spilled array containing only the requested columns, in the
/// order provided.
///
/// # Remarks
/// - Column indexes are 1-based; negative indexes count from the end (`-1` is last column).
/// - Repeated indexes are allowed and duplicate columns in the output.
/// - Index `0` or out-of-bounds indexes return `#VALUE!`.
/// - The result spills as an array unless it collapses to a single cell.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Select first and third columns"
/// grid:
///   A1: "Name"
///   B1: "Dept"
///   C1: "Score"
///   A2: "Ana"
///   B2: "Ops"
///   C2: 91
/// formula: '=CHOOSECOLS(A1:C2,1,3)'
/// expected: [["Name","Score"],["Ana",91]]
/// ```
///
/// ```yaml,sandbox
/// title: "Select the last column with a negative index"
/// grid:
///   A1: 10
///   B1: 20
///   C1: 30
/// formula: '=CHOOSECOLS(A1:C1,-1)'
/// expected: 30
/// ```
/// [formualizer-docgen:schema:start]
/// Name: CHOOSECOLS
/// Type: ChooseColsFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: CHOOSECOLS(arg1: range|any@range, arg2...: number@scalar)
/// Arg schema: arg1{kinds=range|any,required=true,shape=range,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=Some(1),default=false}
/// Caps: PURE, LOOKUP
/// [formualizer-docgen:schema:end]
impl Function for ChooseColsFn {
    func_caps!(PURE, LOOKUP);
    fn name(&self) -> &'static str {
        "CHOOSECOLS"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // array
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range, ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // col_num1 and subsequent
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: Some(1),
                    default: None,
                },
            ]
        });
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }
        let view = args[0].range_view()?;
        let (rows, cols) = view.dims();
        if rows == 0 || cols == 0 {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        let mut indices: Vec<usize> = Vec::new();
        for a in &args[1..] {
            let v = a.value()?.into_literal();
            if let LiteralValue::Error(e) = v {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            let raw = match v {
                LiteralValue::Int(i) => i,
                LiteralValue::Number(n) => n as i64,
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Value),
                    )));
                }
            };
            if raw == 0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
            let adj = if raw > 0 {
                raw - 1
            } else {
                (cols as i64) + raw
            };
            if adj < 0 || adj as usize >= cols {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
            indices.push(adj as usize);
        }
        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(rows);
        for r in 0..rows {
            let mut new_row = Vec::with_capacity(indices.len());
            for &c in &indices {
                new_row.push(view.get_cell(r, c));
            }
            out.push(new_row);
        }

        Ok(collapse_if_scalar(out, _ctx.date_system()))
    }
}

/// Returns selected rows from an array or range.
///
/// `CHOOSEROWS` builds a new spilled array containing only the requested rows, in the
/// order provided.
///
/// # Remarks
/// - Row indexes are 1-based; negative indexes count from the end (`-1` is last row).
/// - Repeated indexes are allowed and duplicate rows in the output.
/// - Index `0` or out-of-bounds indexes return `#VALUE!`.
/// - The result spills as an array unless it collapses to a single cell.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Pick first and last rows"
/// grid:
///   A1: "Jan"
///   A2: "Feb"
///   A3: "Mar"
/// formula: '=CHOOSEROWS(A1:A3,1,-1)'
/// expected: [["Jan"],["Mar"]]
/// ```
///
/// ```yaml,sandbox
/// title: "Duplicate a row in output"
/// grid:
///   A1: 5
///   A2: 9
/// formula: '=CHOOSEROWS(A1:A2,2,2)'
/// expected: [[9],[9]]
/// ```
/// [formualizer-docgen:schema:start]
/// Name: CHOOSEROWS
/// Type: ChooseRowsFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: CHOOSEROWS(arg1: range|any@range, arg2...: number@scalar)
/// Arg schema: arg1{kinds=range|any,required=true,shape=range,by_ref=false,coercion=None,max=None,repeating=None,default=false}; arg2{kinds=number,required=true,shape=scalar,by_ref=false,coercion=NumberLenientText,max=None,repeating=Some(1),default=false}
/// Caps: PURE, LOOKUP
/// [formualizer-docgen:schema:end]
impl Function for ChooseRowsFn {
    func_caps!(PURE, LOOKUP);
    fn name(&self) -> &'static str {
        "CHOOSEROWS"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // array
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range, ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
                // row_num1...
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberLenientText,
                    max: None,
                    repeating: Some(1),
                    default: None,
                },
            ]
        });
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }
        let view = args[0].range_view()?;
        let (rows, cols) = view.dims();
        if rows == 0 || cols == 0 {
            return Ok(crate::traits::CalcValue::Range(
                crate::engine::range_view::RangeView::from_owned_rows(vec![], _ctx.date_system()),
            ));
        }

        let mut indices: Vec<usize> = Vec::new();
        for a in &args[1..] {
            let v = a.value()?.into_literal();
            if let LiteralValue::Error(e) = v {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            let raw = match v {
                LiteralValue::Int(i) => i,
                LiteralValue::Number(n) => n as i64,
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Value),
                    )));
                }
            };
            if raw == 0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
            let adj = if raw > 0 {
                raw - 1
            } else {
                (rows as i64) + raw
            };
            if adj < 0 || adj as usize >= rows {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
            indices.push(adj as usize);
        }
        let mut out: Vec<Vec<LiteralValue>> = Vec::with_capacity(indices.len());
        for &r in &indices {
            let mut row_vals = Vec::with_capacity(cols);
            for c in 0..cols {
                row_vals.push(view.get_cell(r, c));
            }
            out.push(row_vals);
        }

        Ok(collapse_if_scalar(out, _ctx.date_system()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
    use std::sync::Arc;

    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn choose_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(ChooseFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "CHOOSE").unwrap();

        // CHOOSE(2, "A", "B", "C") -> "B"
        let two = lit(LiteralValue::Int(2));
        let a = lit(LiteralValue::Text("A".into()));
        let b = lit(LiteralValue::Text("B".into()));
        let c = lit(LiteralValue::Text("C".into()));

        let args = vec![
            ArgumentHandle::new(&two, &ctx),
            ArgumentHandle::new(&a, &ctx),
            ArgumentHandle::new(&b, &ctx),
            ArgumentHandle::new(&c, &ctx),
        ];

        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Text("B".into()));
    }

    #[test]
    fn choose_numeric_values() {
        let wb = TestWorkbook::new().with_function(Arc::new(ChooseFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "CHOOSE").unwrap();

        // CHOOSE(3, 10, 20, 30, 40) -> 30
        let three = lit(LiteralValue::Int(3));
        let ten = lit(LiteralValue::Int(10));
        let twenty = lit(LiteralValue::Int(20));
        let thirty = lit(LiteralValue::Int(30));
        let forty = lit(LiteralValue::Int(40));

        let args = vec![
            ArgumentHandle::new(&three, &ctx),
            ArgumentHandle::new(&ten, &ctx),
            ArgumentHandle::new(&twenty, &ctx),
            ArgumentHandle::new(&thirty, &ctx),
            ArgumentHandle::new(&forty, &ctx),
        ];

        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(30));
    }

    #[test]
    fn choose_out_of_range() {
        let wb = TestWorkbook::new().with_function(Arc::new(ChooseFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "CHOOSE").unwrap();

        // CHOOSE(5, "A", "B", "C") -> #VALUE! (only 3 choices)
        let five = lit(LiteralValue::Int(5));
        let a = lit(LiteralValue::Text("A".into()));
        let b = lit(LiteralValue::Text("B".into()));
        let c = lit(LiteralValue::Text("C".into()));

        let args = vec![
            ArgumentHandle::new(&five, &ctx),
            ArgumentHandle::new(&a, &ctx),
            ArgumentHandle::new(&b, &ctx),
            ArgumentHandle::new(&c, &ctx),
        ];

        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(result, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));

        // CHOOSE(0, "A", "B") -> #VALUE! (index must be >= 1)
        let zero = lit(LiteralValue::Int(0));
        let args2 = vec![
            ArgumentHandle::new(&zero, &ctx),
            ArgumentHandle::new(&a, &ctx),
            ArgumentHandle::new(&b, &ctx),
        ];

        let result2 = f
            .dispatch(&args2, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(result2, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));
    }

    #[test]
    fn choose_decimal_index() {
        let wb = TestWorkbook::new().with_function(Arc::new(ChooseFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "CHOOSE").unwrap();

        // CHOOSE(2.7, "A", "B", "C") -> "B" (truncates to 2)
        let two_seven = lit(LiteralValue::Number(2.7));
        let a = lit(LiteralValue::Text("A".into()));
        let b = lit(LiteralValue::Text("B".into()));
        let c = lit(LiteralValue::Text("C".into()));

        let args = vec![
            ArgumentHandle::new(&two_seven, &ctx),
            ArgumentHandle::new(&a, &ctx),
            ArgumentHandle::new(&b, &ctx),
            ArgumentHandle::new(&c, &ctx),
        ];

        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Text("B".into()));
    }

    #[test]
    fn choose_text_index_numeric_string() {
        let wb = TestWorkbook::new().with_function(Arc::new(ChooseFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "CHOOSE").unwrap();
        let two_txt = lit(LiteralValue::Text("2".into()));
        let a = lit(LiteralValue::Text("A".into()));
        let b = lit(LiteralValue::Text("B".into()));
        let c = lit(LiteralValue::Text("C".into()));
        let args = vec![
            ArgumentHandle::new(&two_txt, &ctx),
            ArgumentHandle::new(&a, &ctx),
            ArgumentHandle::new(&b, &ctx),
            ArgumentHandle::new(&c, &ctx),
        ];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        // Current engine uses NumberStrict coercion for index: numeric text not accepted -> #VALUE!
        assert!(matches!(result, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));
    }

    #[test]
    fn choose_decimal_less_than_one() {
        let wb = TestWorkbook::new().with_function(Arc::new(ChooseFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "CHOOSE").unwrap();
        let zero_nine = lit(LiteralValue::Number(0.9));
        let a = lit(LiteralValue::Text("A".into()));
        let b = lit(LiteralValue::Text("B".into()));
        let args = vec![
            ArgumentHandle::new(&zero_nine, &ctx),
            ArgumentHandle::new(&a, &ctx),
            ArgumentHandle::new(&b, &ctx),
        ];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(result, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));
    }

    fn range(r: &str, sr: u32, sc: u32, er: u32, ec: u32) -> ASTNode {
        ASTNode::new(
            ASTNodeType::Reference {
                original: r.into(),
                reference: ReferenceType::range(None, Some(sr), Some(sc), Some(er), Some(ec)),
            },
            None,
        )
    }

    #[test]
    fn choosecols_basic_and_negative_and_duplicates() {
        let wb = TestWorkbook::new().with_function(Arc::new(ChooseColsFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "C1", LiteralValue::Int(3))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "C2", LiteralValue::Int(30));
        let ctx = wb.interpreter();
        let arr = range("A1:C2", 1, 1, 2, 3);
        let f = ctx.context.get_function("", "CHOOSECOLS").unwrap();
        let one = lit(LiteralValue::Int(1));
        let three = lit(LiteralValue::Int(3));
        let neg_one = lit(LiteralValue::Int(-1));
        // pick first & third (positive indices)
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&one, &ctx),
            ArgumentHandle::new(&three, &ctx),
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
                    vec![LiteralValue::Number(1.0), LiteralValue::Number(3.0)]
                );
            }
            other => panic!("expected array got {other:?}"),
        }
        // negative -1 -> last col only
        let args_neg = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&neg_one, &ctx),
        ];
        let v2 = f
            .dispatch(&args_neg, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v2 {
            LiteralValue::Array(a) => {
                assert_eq!(a[0], vec![LiteralValue::Number(3.0)]);
            }
            other => panic!("expected array last col got {other:?}"),
        }
        // duplicates (1,1)
        let args_dup = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&one, &ctx),
            ArgumentHandle::new(&one, &ctx),
        ];
        let v3 = f
            .dispatch(&args_dup, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v3 {
            LiteralValue::Array(a) => {
                assert_eq!(
                    a[0],
                    vec![LiteralValue::Number(1.0), LiteralValue::Number(1.0)]
                );
            }
            other => panic!("expected dup cols got {other:?}"),
        }
    }

    #[test]
    fn choosecols_out_of_range() {
        let wb = TestWorkbook::new().with_function(Arc::new(ChooseColsFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(2));
        let ctx = wb.interpreter();
        let arr = range("A1:B1", 1, 1, 1, 2);
        let f = ctx.context.get_function("", "CHOOSECOLS").unwrap();
        let three = lit(LiteralValue::Int(3));
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&three, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Value),
            other => panic!("expected #VALUE! got {other:?}"),
        }
    }

    #[test]
    fn chooserows_basic_and_negative() {
        let wb = TestWorkbook::new().with_function(Arc::new(ChooseRowsFn));
        let wb = wb
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Int(2))
            .with_cell_a1("Sheet1", "A2", LiteralValue::Int(10))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(20))
            .with_cell_a1("Sheet1", "A3", LiteralValue::Int(100))
            .with_cell_a1("Sheet1", "B3", LiteralValue::Int(200));
        let ctx = wb.interpreter();
        let arr = range("A1:B3", 1, 1, 3, 2);
        let f = ctx.context.get_function("", "CHOOSEROWS").unwrap();
        let one = lit(LiteralValue::Int(1));
        let neg_one = lit(LiteralValue::Int(-1));
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&one, &ctx),
            ArgumentHandle::new(&neg_one, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Array(a) => {
                assert_eq!(a.len(), 2);
                assert_eq!(a[0][0], LiteralValue::Number(1.0));
                assert_eq!(a[1][0], LiteralValue::Number(100.0));
            }
            other => panic!("expected array got {other:?}"),
        }
    }

    #[test]
    fn chooserows_out_of_range() {
        let wb = TestWorkbook::new().with_function(Arc::new(ChooseRowsFn));
        let wb = wb.with_cell_a1("Sheet1", "A1", LiteralValue::Int(1));
        let ctx = wb.interpreter();
        let arr = range("A1:A1", 1, 1, 1, 1);
        let f = ctx.context.get_function("", "CHOOSEROWS").unwrap();
        let two = lit(LiteralValue::Int(2));
        let args = vec![
            ArgumentHandle::new(&arr, &ctx),
            ArgumentHandle::new(&two, &ctx),
        ];
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        match v {
            LiteralValue::Error(e) => assert_eq!(e.kind, ExcelErrorKind::Value),
            other => panic!("expected #VALUE! got {other:?}"),
        }
    }
}
