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
            // Return current cell's row if available
            if let Some(cell_ref) = ctx.current_cell() {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                    cell_ref.coord.row() as i64,
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

        // Extract row number from reference
        let row = match &reference {
            ReferenceType::Cell { row, .. } => *row,
            ReferenceType::Range {
                start_row: Some(sr),
                ..
            } => *sr,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Ref),
                )));
            }
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            row as i64,
        )))
    }
}

#[derive(Debug)]
pub struct RowsFn;

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
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
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
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Ref),
                    )));
                }
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
            // Return current cell's column if available
            if let Some(cell_ref) = ctx.current_cell() {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
                    cell_ref.coord.col() as i64,
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

        // Extract column number from reference
        let col = match &reference {
            ReferenceType::Cell { col, .. } => *col,
            ReferenceType::Range {
                start_col: Some(sc),
                ..
            } => *sc,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Ref),
                )));
            }
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Int(
            col as i64,
        )))
    }
}

#[derive(Debug)]
pub struct ColumnsFn;

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
        _ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
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
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Ref),
                    )));
                }
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
