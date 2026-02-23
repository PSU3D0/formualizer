//! Reference information functions: ROW, ROWS, COLUMN, COLUMNS
//!
//! Excel semantics:
//! - ROW([reference]) - Returns the row number of a reference
//! - ROWS(array) - Returns the number of rows in a reference
//! - COLUMN([reference]) - Returns the column number of a reference
//! - COLUMNS(array) - Returns the number of columns in a reference
//!
//! Without arguments, ROW and COLUMN return the current cell's position

use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ArgKind, ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_macros::func_caps;
use formualizer_parse::parser::ReferenceType;

#[derive(Debug)]
pub struct RowFn;

/// Returns the row number of a reference, or of the current cell when omitted.
///
/// `ROW` returns a 1-based row index.
///
/// # Remarks
/// - With a range argument, `ROW` returns the first row in that reference.
/// - Without arguments, it uses the row of the formula cell.
/// - Full-column references such as `A:A` return `1`.
/// - Invalid references return an error (`#REF!`/`#VALUE!` depending on context).
///
/// # Examples
/// ```yaml,sandbox
/// title: "Row of a single-cell reference"
/// formula: '=ROW(B5)'
/// expected: 5
/// ```
///
/// ```yaml,sandbox
/// title: "Row of a multi-cell range"
/// formula: '=ROW(C3:E9)'
/// expected: 3
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ROW
/// Type: RowFn
/// Min args: 0
/// Max args: 1
/// Variadic: false
/// Signature: ROW(arg1?: range@range)
/// Arg schema: arg1{kinds=range,required=false,shape=range,by_ref=true,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for RowFn {
    fn name(&self) -> &'static str {
        "ROW"
    }

    fn min_args(&self) -> usize {
        0
    }

    func_caps!(PURE);

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // Optional reference
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: false,
                    by_ref: true,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
            ]
        });
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() {
            // Return current cell's row (1-based) if available
            if let Some(cell_ref) = ctx.current_cell() {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                    cell_ref.coord.row() as i64 + 1,
                )));
            }
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Get reference
        let reference = match args[0].as_reference_or_eval() {
            Ok(r) => r,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // Extract row number from reference (1-based)
        let row_1based = match &reference {
            ReferenceType::Cell { row, .. } => *row as i64,
            ReferenceType::Range {
                start_row: Some(sr),
                ..
            } => *sr as i64,
            // Full-column references like A:A use first row
            ReferenceType::Range {
                start_row: None,
                end_row: None,
                ..
            } => 1,
            // Fallback: resolve the reference and use the view origin
            _ => match ctx.resolve_range_view(&reference, ctx.current_sheet()) {
                Ok(view) => {
                    if view.is_empty() {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Ref),
                        )));
                    }
                    view.start_row() as i64 + 1
                }
                Err(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
            },
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            row_1based,
        )))
    }
}

#[derive(Debug)]
pub struct RowsFn;

/// Returns the number of rows in a reference or array.
///
/// `ROWS` reports height, not data density.
///
/// # Remarks
/// - For a single cell reference, returns `1`.
/// - For full-column references (for example `A:A`), returns `1048576`.
/// - For array literals, returns the outer array length.
/// - Invalid references return an error.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Count rows in a contiguous range"
/// formula: '=ROWS(B2:D10)'
/// expected: 9
/// ```
///
/// ```yaml,sandbox
/// title: "Count rows in a full column reference"
/// formula: '=ROWS(A:A)'
/// expected: 1048576
/// ```
/// [formualizer-docgen:schema:start]
/// Name: ROWS
/// Type: RowsFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: ROWS(arg1: any@range)
/// Arg schema: arg1{kinds=any,required=true,shape=range,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for RowsFn {
    fn name(&self) -> &'static str {
        "ROWS"
    }

    fn min_args(&self) -> usize {
        1
    }

    func_caps!(PURE);

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // Required reference/range or array
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
            ]
        });
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        const EXCEL_MAX_ROWS: i64 = 1_048_576;

        if args.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Try to get reference first, fall back to array literal
        if let Ok(reference) = args[0].as_reference_or_eval() {
            // Calculate number of rows
            let rows = match &reference {
                ReferenceType::Cell { .. } => 1,
                ReferenceType::Range {
                    start_row: Some(sr),
                    end_row: Some(er),
                    ..
                } => {
                    if *er >= *sr {
                        (*er - *sr + 1) as i64
                    } else {
                        1
                    }
                }
                // Full-column references like A:A
                ReferenceType::Range {
                    start_row: None,
                    end_row: None,
                    ..
                } => EXCEL_MAX_ROWS,
                // Open-ended tail like A5:A
                ReferenceType::Range {
                    start_row: Some(sr),
                    end_row: None,
                    ..
                } => EXCEL_MAX_ROWS.saturating_sub(*sr as i64).saturating_add(1),
                // Open-ended head like A:A10 (treated as A1:A10)
                ReferenceType::Range {
                    start_row: None,
                    end_row: Some(er),
                    ..
                } => *er as i64,
                // Fallback for named ranges, table refs, etc.
                _ => match ctx.resolve_range_view(&reference, ctx.current_sheet()) {
                    Ok(view) => view.dims().0 as i64,
                    Err(e) => {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                    }
                },
            };
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(rows)))
        } else {
            // Handle array literal
            let v = args[0].value()?.into_literal();
            let rows = match v {
                LiteralValue::Array(arr) => arr.len() as i64,
                _ => 1,
            };
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(rows)))
        }
    }
}

#[derive(Debug)]
pub struct ColumnFn;

/// Returns the column number of a reference, or of the current cell when omitted.
///
/// `COLUMN` returns a 1-based column index (`A` = 1).
///
/// # Remarks
/// - With a range argument, `COLUMN` returns the first column in that reference.
/// - Without arguments, it uses the column of the formula cell.
/// - Full-row references such as `5:5` return `1`.
/// - Invalid references return an error (`#REF!`/`#VALUE!` depending on context).
///
/// # Examples
/// ```yaml,sandbox
/// title: "Column of a single-cell reference"
/// formula: '=COLUMN(C5)'
/// expected: 3
/// ```
///
/// ```yaml,sandbox
/// title: "Column of a range"
/// formula: '=COLUMN(B2:D4)'
/// expected: 2
/// ```
/// [formualizer-docgen:schema:start]
/// Name: COLUMN
/// Type: ColumnFn
/// Min args: 0
/// Max args: 1
/// Variadic: false
/// Signature: COLUMN(arg1?: range@range)
/// Arg schema: arg1{kinds=range,required=false,shape=range,by_ref=true,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ColumnFn {
    fn name(&self) -> &'static str {
        "COLUMN"
    }

    fn min_args(&self) -> usize {
        0
    }

    func_caps!(PURE);

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // Optional reference
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Range],
                    required: false,
                    by_ref: true,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
            ]
        });
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if args.is_empty() {
            // Return current cell's column (1-based) if available
            if let Some(cell_ref) = ctx.current_cell() {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                    cell_ref.coord.col() as i64 + 1,
                )));
            }
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Get reference
        let reference = match args[0].as_reference_or_eval() {
            Ok(r) => r,
            Err(e) => return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
        };

        // Extract column number from reference (1-based)
        let col_1based = match &reference {
            ReferenceType::Cell { col, .. } => *col as i64,
            ReferenceType::Range {
                start_col: Some(sc),
                ..
            } => *sc as i64,
            // Full-row references like 1:1 use first column
            ReferenceType::Range {
                start_col: None,
                end_col: None,
                ..
            } => 1,
            // Fallback: resolve the reference and use the view origin
            _ => match ctx.resolve_range_view(&reference, ctx.current_sheet()) {
                Ok(view) => {
                    if view.is_empty() {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Ref),
                        )));
                    }
                    view.start_col() as i64 + 1
                }
                Err(e) => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                }
            },
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            col_1based,
        )))
    }
}

#[derive(Debug)]
pub struct ColumnsFn;

/// Returns the number of columns in a reference or array.
///
/// `COLUMNS` reports width, not data density.
///
/// # Remarks
/// - For a single cell reference, returns `1`.
/// - For full-row references (for example `1:1`), returns `16384`.
/// - For array literals, returns the first row width.
/// - Invalid references return an error.
///
/// # Examples
/// ```yaml,sandbox
/// title: "Count columns in a rectangular range"
/// formula: '=COLUMNS(B2:D10)'
/// expected: 3
/// ```
///
/// ```yaml,sandbox
/// title: "Count columns in a full row reference"
/// formula: '=COLUMNS(1:1)'
/// expected: 16384
/// ```
/// [formualizer-docgen:schema:start]
/// Name: COLUMNS
/// Type: ColumnsFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: COLUMNS(arg1: any@range)
/// Arg schema: arg1{kinds=any,required=true,shape=range,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ColumnsFn {
    fn name(&self) -> &'static str {
        "COLUMNS"
    }

    fn min_args(&self) -> usize {
        1
    }

    func_caps!(PURE);

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // Required reference/range or array
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Any],
                    required: true,
                    by_ref: false,
                    shape: ShapeKind::Range,
                    coercion: CoercionPolicy::None,
                    max: None,
                    repeating: None,
                    default: None,
                },
            ]
        });
        &SCHEMA
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        const EXCEL_MAX_COLS: i64 = 16_384;

        if args.is_empty() {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Try to get reference first, fall back to array literal
        if let Ok(reference) = args[0].as_reference_or_eval() {
            // Calculate number of columns
            let cols = match &reference {
                ReferenceType::Cell { .. } => 1,
                ReferenceType::Range {
                    start_col: Some(sc),
                    end_col: Some(ec),
                    ..
                } => {
                    if *ec >= *sc {
                        (*ec - *sc + 1) as i64
                    } else {
                        1
                    }
                }
                // Full-row references like 1:1
                ReferenceType::Range {
                    start_col: None,
                    end_col: None,
                    ..
                } => EXCEL_MAX_COLS,
                // Open-ended tail where start_col is known and end_col is omitted
                ReferenceType::Range {
                    start_col: Some(sc),
                    end_col: None,
                    ..
                } => EXCEL_MAX_COLS.saturating_sub(*sc as i64).saturating_add(1),
                // Open-ended head like :F (or equivalent parsed form)
                ReferenceType::Range {
                    start_col: None,
                    end_col: Some(ec),
                    ..
                } => *ec as i64,
                // Fallback for named ranges, table refs, etc.
                _ => match ctx.resolve_range_view(&reference, ctx.current_sheet()) {
                    Ok(view) => view.dims().1 as i64,
                    Err(e) => {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
                    }
                },
            };
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(cols)))
        } else {
            // Handle array literal
            let v = args[0].value()?.into_literal();
            let cols = match v {
                LiteralValue::Array(arr) => arr.first().map(|r| r.len()).unwrap_or(0) as i64,
                _ => 1,
            };
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(cols)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::{CellRef, Coord};
    use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};
    use std::sync::Arc;

    #[test]
    fn row_with_reference() {
        let wb = TestWorkbook::new().with_function(Arc::new(RowFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ROW").unwrap();

        // ROW(B5) -> 5
        let b5_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "B5".into(),
                reference: ReferenceType::cell(None, 5, 2),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&b5_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(5));

        // ROW(A1:C3) -> 1 (first row)
        let range_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:C3".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(3), Some(3)),
            },
            None,
        );

        let args2 = vec![ArgumentHandle::new(&range_ref, &ctx)];
        let result2 = f
            .dispatch(&args2, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result2, LiteralValue::Int(1));
    }

    #[test]
    fn row_no_arg_uses_current_cell_1_based() {
        let wb = TestWorkbook::new().with_function(Arc::new(RowFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ROW").unwrap();

        let current = CellRef::new(0, Coord::from_excel(7, 4, false, false));
        let result = f
            .dispatch(&[], &ctx.function_context(Some(&current)))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(7));
    }

    #[test]
    fn row_full_column_reference_returns_first_row() {
        let wb = TestWorkbook::new().with_function(Arc::new(RowFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ROW").unwrap();

        // ROW(A:A) -> 1
        let col_range_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "A:A".into(),
                reference: ReferenceType::range(None, None, Some(1), None, Some(1)),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&col_range_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(1));
    }

    #[test]
    fn row_named_range_falls_back_to_resolved_range_view() {
        let wb = TestWorkbook::new()
            .with_named_range("MyRow", vec![vec![LiteralValue::Int(42)]])
            .with_function(Arc::new(RowFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ROW").unwrap();

        let named_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "MyRow".into(),
                reference: ReferenceType::NamedRange("MyRow".into()),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&named_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(1));
    }

    #[test]
    fn rows_function() {
        let wb = TestWorkbook::new().with_function(Arc::new(RowsFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ROWS").unwrap();

        // ROWS(A1:A5) -> 5
        let range_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:A5".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(5), Some(1)),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&range_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(5));

        // ROWS(B2:D10) -> 9
        let range_ref2 = ASTNode::new(
            ASTNodeType::Reference {
                original: "B2:D10".into(),
                reference: ReferenceType::range(None, Some(2), Some(2), Some(10), Some(4)),
            },
            None,
        );

        let args2 = vec![ArgumentHandle::new(&range_ref2, &ctx)];
        let result2 = f
            .dispatch(&args2, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result2, LiteralValue::Int(9));

        // ROWS(A1) -> 1 (single cell)
        let cell_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1".into(),
                reference: ReferenceType::cell(None, 1, 1),
            },
            None,
        );

        let args3 = vec![ArgumentHandle::new(&cell_ref, &ctx)];
        let result3 = f
            .dispatch(&args3, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result3, LiteralValue::Int(1));
    }

    #[test]
    fn rows_full_column_reference_returns_sheet_height() {
        let wb = TestWorkbook::new().with_function(Arc::new(RowsFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ROWS").unwrap();

        // ROWS(A:A) -> 1048576
        let col_range_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "A:A".into(),
                reference: ReferenceType::range(None, None, Some(1), None, Some(1)),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&col_range_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(1_048_576));
    }

    #[test]
    fn rows_named_range_falls_back_to_resolved_range_view() {
        let wb = TestWorkbook::new()
            .with_named_range(
                "MyRows",
                vec![
                    vec![LiteralValue::Int(1)],
                    vec![LiteralValue::Int(2)],
                    vec![LiteralValue::Int(3)],
                ],
            )
            .with_function(Arc::new(RowsFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ROWS").unwrap();

        let named_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "MyRows".into(),
                reference: ReferenceType::NamedRange("MyRows".into()),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&named_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(3));
    }

    #[test]
    fn column_with_reference() {
        let wb = TestWorkbook::new().with_function(Arc::new(ColumnFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "COLUMN").unwrap();

        // COLUMN(C5) -> 3
        let c5_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "C5".into(),
                reference: ReferenceType::cell(None, 5, 3),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&c5_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(3));

        // COLUMN(B2:D4) -> 2 (first column)
        let range_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "B2:D4".into(),
                reference: ReferenceType::range(None, Some(2), Some(2), Some(4), Some(4)),
            },
            None,
        );

        let args2 = vec![ArgumentHandle::new(&range_ref, &ctx)];
        let result2 = f
            .dispatch(&args2, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result2, LiteralValue::Int(2));
    }

    #[test]
    fn column_no_arg_uses_current_cell_1_based() {
        let wb = TestWorkbook::new().with_function(Arc::new(ColumnFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "COLUMN").unwrap();

        let current = CellRef::new(0, Coord::from_excel(7, 4, false, false));
        let result = f
            .dispatch(&[], &ctx.function_context(Some(&current)))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(4));
    }

    #[test]
    fn column_full_row_reference_returns_first_column() {
        let wb = TestWorkbook::new().with_function(Arc::new(ColumnFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "COLUMN").unwrap();

        // COLUMN(5:5) -> 1
        let row_range_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "5:5".into(),
                reference: ReferenceType::range(None, Some(5), None, Some(5), None),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&row_range_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(1));
    }

    #[test]
    fn column_named_range_falls_back_to_resolved_range_view() {
        let wb = TestWorkbook::new()
            .with_named_range("MyRange", vec![vec![LiteralValue::Int(42)]])
            .with_function(Arc::new(ColumnFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "COLUMN").unwrap();

        let named_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "MyRange".into(),
                reference: ReferenceType::NamedRange("MyRange".into()),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&named_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(1));
    }

    #[test]
    fn columns_function() {
        let wb = TestWorkbook::new().with_function(Arc::new(ColumnsFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "COLUMNS").unwrap();

        // COLUMNS(A1:E1) -> 5
        let range_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:E1".into(),
                reference: ReferenceType::range(None, Some(1), Some(1), Some(1), Some(5)),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&range_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(5));

        // COLUMNS(B2:D10) -> 3
        let range_ref2 = ASTNode::new(
            ASTNodeType::Reference {
                original: "B2:D10".into(),
                reference: ReferenceType::range(None, Some(2), Some(2), Some(10), Some(4)),
            },
            None,
        );

        let args2 = vec![ArgumentHandle::new(&range_ref2, &ctx)];
        let result2 = f
            .dispatch(&args2, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result2, LiteralValue::Int(3));

        // COLUMNS(A1) -> 1 (single cell)
        let cell_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1".into(),
                reference: ReferenceType::cell(None, 1, 1),
            },
            None,
        );

        let args3 = vec![ArgumentHandle::new(&cell_ref, &ctx)];
        let result3 = f
            .dispatch(&args3, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result3, LiteralValue::Int(1));
    }

    #[test]
    fn columns_full_row_reference_returns_sheet_width() {
        let wb = TestWorkbook::new().with_function(Arc::new(ColumnsFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "COLUMNS").unwrap();

        // COLUMNS(1:1) -> 16384
        let row_range_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "1:1".into(),
                reference: ReferenceType::range(None, Some(1), None, Some(1), None),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&row_range_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(16_384));
    }

    #[test]
    fn columns_named_range_falls_back_to_resolved_range_view() {
        let wb = TestWorkbook::new()
            .with_named_range(
                "MyCols",
                vec![
                    vec![
                        LiteralValue::Int(1),
                        LiteralValue::Int(2),
                        LiteralValue::Int(3),
                    ],
                    vec![
                        LiteralValue::Int(4),
                        LiteralValue::Int(5),
                        LiteralValue::Int(6),
                    ],
                ],
            )
            .with_function(Arc::new(ColumnsFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "COLUMNS").unwrap();

        let named_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "MyCols".into(),
                reference: ReferenceType::NamedRange("MyCols".into()),
            },
            None,
        );

        let args = vec![ArgumentHandle::new(&named_ref, &ctx)];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Int(3));
    }

    #[test]
    fn rows_columns_reversed_range() {
        // A5:A1 (start_row > end_row) should treat as 1 row / 1 column per current implementation fallback
        let wb = TestWorkbook::new()
            .with_function(Arc::new(RowsFn))
            .with_function(Arc::new(ColumnsFn));
        let ctx = wb.interpreter();
        let rows_f = ctx.context.get_function("", "ROWS").unwrap();
        let cols_f = ctx.context.get_function("", "COLUMNS").unwrap();
        let rev_range = ASTNode::new(
            ASTNodeType::Reference {
                original: "A5:A1".into(),
                reference: ReferenceType::range(None, Some(5), Some(1), Some(1), Some(1)),
            },
            None,
        );
        let args = vec![ArgumentHandle::new(&rev_range, &ctx)];
        let r_count = rows_f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        let c_count = cols_f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(r_count, LiteralValue::Int(1));
        assert_eq!(c_count, LiteralValue::Int(1));
    }
}
