use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use crate::function::{FnCaps, Function};
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{ArgKind, ExcelError, ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::ReferenceType;

fn number_strict_scalar() -> ArgSchema {
    ArgSchema {
        kinds: smallvec::smallvec![ArgKind::Number],
        required: true,
        by_ref: false,
        shape: ShapeKind::Scalar,
        coercion: CoercionPolicy::NumberStrict,
        max: None,
        repeating: None,
        default: None,
    }
}

fn arg_byref_array() -> Vec<ArgSchema> {
    vec![
        // Accept both references and array literals
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
        number_strict_scalar(),
        // Column is optional for 1D arrays
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Number],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberStrict,
            max: None,
            repeating: None,
            default: None,
        },
    ]
}

fn arg_byref_reference() -> Vec<ArgSchema> {
    vec![
        ArgSchema {
            kinds: smallvec::smallvec![ArgKind::Range],
            required: true,
            by_ref: true,
            shape: ShapeKind::Range,
            coercion: CoercionPolicy::None,
            max: None,
            repeating: None,
            default: None,
        },
        number_strict_scalar(),
        number_strict_scalar(),
        ArgSchema {
            // height optional
            kinds: smallvec::smallvec![ArgKind::Number],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberStrict,
            max: None,
            repeating: None,
            default: None,
        },
        ArgSchema {
            // width optional
            kinds: smallvec::smallvec![ArgKind::Number],
            required: false,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberStrict,
            max: None,
            repeating: None,
            default: None,
        },
    ]
}

#[derive(Debug)]
pub struct IndexFn;
impl Function for IndexFn {
    fn caps(&self) -> FnCaps {
        FnCaps::PURE | FnCaps::RETURNS_REFERENCE
    }
    fn name(&self) -> &'static str {
        "INDEX"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(arg_byref_array);
        &SCHEMA
    }

    fn eval_reference<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Option<Result<ReferenceType, ExcelError>> {
        // args: array(by_ref), row, col (col optional for 1D)
        if args.len() < 2 {
            return Some(Err(ExcelError::new(ExcelErrorKind::Value)));
        }
        // Return None for array literals so eval() handles them
        let base = match args[0].as_reference_or_eval() {
            Ok(r) => r,
            Err(_) => return None,
        };
        let row = match args[1].value() {
            Ok(cv) => match cv.into_literal() {
                LiteralValue::Number(n) => n as i64,
                LiteralValue::Int(i) => i,
                _ => return Some(Err(ExcelError::new(ExcelErrorKind::Value))),
            },
            Err(e) => return Some(Err(e)),
        };
        let col = if args.len() >= 3 {
            match args[2].value() {
                Ok(cv) => match cv.into_literal() {
                    LiteralValue::Number(n) => n as i64,
                    LiteralValue::Int(i) => i,
                    _ => return Some(Err(ExcelError::new(ExcelErrorKind::Value))),
                },
                Err(e) => return Some(Err(e)),
            }
        } else {
            // TODO(phase6): Document INDEX 1D behavior when col omitted.
            1
        };

        // Only Range supported for now
        let (sheet, sr, sc, er, ec) = match base {
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => match (start_row, start_col, end_row, end_col) {
                (Some(sr), Some(sc), Some(er), Some(ec)) => (sheet, sr, sc, er, ec),
                _ => return Some(Err(ExcelError::new(ExcelErrorKind::Ref))),
            },
            ReferenceType::Cell {
                sheet, row, col, ..
            } => (sheet, row, col, row, col),
            _ => return Some(Err(ExcelError::new(ExcelErrorKind::Ref))),
        };

        // 1-based indexing per Excel
        if row <= 0 || col <= 0 {
            return Some(Err(ExcelError::new(ExcelErrorKind::Ref)));
        }
        let r = sr + (row as u32) - 1;
        let c = sc + (col as u32) - 1;
        if r > er || c > ec {
            return Some(Err(ExcelError::new(ExcelErrorKind::Ref)));
        }

        Some(Ok(ReferenceType::cell(sheet, r, c)))
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        // First try to handle as a reference
        if let Some(result) = self.eval_reference(args, ctx) {
            match result {
                Ok(r) => {
                    // Materialize to value
                    let current_sheet = ctx.current_sheet();
                    match ctx.resolve_range_view(&r, current_sheet) {
                        Ok(rv) => {
                            let (rows, cols) = rv.dims();
                            if rows == 1 && cols == 1 {
                                Ok(crate::traits::CalcValue::Scalar(
                                    rv.as_1x1().unwrap_or(LiteralValue::Empty),
                                ))
                            } else {
                                Ok(crate::traits::CalcValue::Range(rv))
                            }
                        }
                        Err(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
                    }
                }
                Err(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            }
        } else {
            // Handle array literal
            if args.len() < 2 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
            let v = args[0].value()?.into_literal();
            let table: Vec<Vec<LiteralValue>> = match v {
                LiteralValue::Array(rows) => rows,
                other => vec![vec![other]],
            };
            let index = match args[1].value()?.into_literal() {
                LiteralValue::Number(n) => n as i64,
                LiteralValue::Int(i) => i,
                _ => {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Value),
                    )));
                }
            };

            // Determine if this is a 1D array (single row or single column)
            let is_single_row = table.len() == 1;
            let is_single_col = table.iter().all(|r| r.len() == 1);

            // For 1D arrays with 2 args, index is position in the array
            if args.len() == 2 && (is_single_row || is_single_col) {
                // TODO(phase6): Document INDEX 1D behavior when col omitted.
                if index <= 0 {
                    return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                        ExcelError::new(ExcelErrorKind::Ref),
                    )));
                }
                let idx = (index - 1) as usize;
                let val = if is_single_row {
                    table[0].get(idx).cloned()
                } else {
                    table.get(idx).and_then(|r| r.first()).cloned()
                };
                return Ok(crate::traits::CalcValue::Scalar(val.unwrap_or_else(|| {
                    LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref))
                })));
            }

            // 2D array or 3 arguments: use row and col indexing
            let row = index as usize;
            let col = if args.len() >= 3 {
                match args[2].value()?.into_literal() {
                    LiteralValue::Number(n) => n as usize,
                    LiteralValue::Int(i) => i as usize,
                    _ => {
                        return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                            ExcelError::new(ExcelErrorKind::Value),
                        )));
                    }
                }
            } else {
                1
            };

            // 1-based indexing
            if row == 0 || col == 0 {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Ref),
                )));
            }
            let val = table
                .get(row - 1)
                .and_then(|r| r.get(col - 1))
                .cloned()
                .unwrap_or_else(|| LiteralValue::Error(ExcelError::new(ExcelErrorKind::Ref)));
            Ok(crate::traits::CalcValue::Scalar(val))
        }
    }
}

#[derive(Debug)]
pub struct OffsetFn;
impl Function for OffsetFn {
    fn caps(&self) -> FnCaps {
        // OFFSET is volatile in Excel semantics
        FnCaps::PURE | FnCaps::RETURNS_REFERENCE | FnCaps::VOLATILE
    }
    fn name(&self) -> &'static str {
        "OFFSET"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(arg_byref_reference);
        &SCHEMA
    }

    fn eval_reference<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        _ctx: &dyn FunctionContext<'b>,
    ) -> Option<Result<ReferenceType, ExcelError>> {
        if args.len() < 3 {
            return Some(Err(ExcelError::new(ExcelErrorKind::Value)));
        }
        let base = match args[0].as_reference_or_eval() {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };
        let dr = match args[1].value() {
            Ok(cv) => match cv.into_literal() {
                LiteralValue::Number(n) => n as i64,
                LiteralValue::Int(i) => i,
                _ => return Some(Err(ExcelError::new(ExcelErrorKind::Value))),
            },
            Err(e) => return Some(Err(e)),
        };
        let dc = match args[2].value() {
            Ok(cv) => match cv.into_literal() {
                LiteralValue::Number(n) => n as i64,
                LiteralValue::Int(i) => i,
                _ => return Some(Err(ExcelError::new(ExcelErrorKind::Value))),
            },
            Err(e) => return Some(Err(e)),
        };

        let (sheet, sr, sc, er, ec) = match base {
            ReferenceType::Range {
                sheet,
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => match (start_row, start_col, end_row, end_col) {
                (Some(sr), Some(sc), Some(er), Some(ec)) => (sheet, sr, sc, er, ec),
                _ => return Some(Err(ExcelError::new(ExcelErrorKind::Ref))),
            },
            ReferenceType::Cell {
                sheet, row, col, ..
            } => (sheet, row, col, row, col),
            _ => return Some(Err(ExcelError::new(ExcelErrorKind::Ref))),
        };

        let nsr = (sr as i64) + dr;
        let nsc = (sc as i64) + dc;
        let height = if args.len() >= 4 {
            match args[3].value() {
                Ok(cv) => match cv.into_literal() {
                    LiteralValue::Number(n) => n as i64,
                    LiteralValue::Int(i) => i,
                    _ => return Some(Err(ExcelError::new(ExcelErrorKind::Value))),
                },
                Err(e) => return Some(Err(e)),
            }
        } else {
            (er as i64) - (sr as i64) + 1
        };
        let width = if args.len() >= 5 {
            match args[4].value() {
                Ok(cv) => match cv.into_literal() {
                    LiteralValue::Number(n) => n as i64,
                    LiteralValue::Int(i) => i,
                    _ => return Some(Err(ExcelError::new(ExcelErrorKind::Value))),
                },
                Err(e) => return Some(Err(e)),
            }
        } else {
            (ec as i64) - (sc as i64) + 1
        };

        if nsr <= 0 || nsc <= 0 || height <= 0 || width <= 0 {
            return Some(Err(ExcelError::new(ExcelErrorKind::Ref)));
        }
        let ner = nsr + height - 1;
        let nec = nsc + width - 1;

        if height == 1 && width == 1 {
            Some(Ok(ReferenceType::cell(sheet, nsr as u32, nsc as u32)))
        } else {
            Some(Ok(ReferenceType::range(
                sheet,
                Some(nsr as u32),
                Some(nsc as u32),
                Some(ner as u32),
                Some(nec as u32),
            )))
        }
    }

    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        if let Some(Ok(r)) = self.eval_reference(args, ctx) {
            let current_sheet = ctx.current_sheet();
            match ctx.resolve_range_view(&r, current_sheet) {
                Ok(rv) => {
                    let (rows, cols) = rv.dims();
                    if rows == 1 && cols == 1 {
                        Ok(crate::traits::CalcValue::Scalar(
                            rv.as_1x1().unwrap_or(LiteralValue::Empty),
                        ))
                    } else {
                        Ok(crate::traits::CalcValue::Range(rv))
                    }
                }
                Err(e) => Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e))),
            }
        } else {
            Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Ref),
            )))
        }
    }
}

pub fn register_builtins() {
    crate::function_registry::register_function(std::sync::Arc::new(IndexFn));
    crate::function_registry::register_function(std::sync::Arc::new(OffsetFn));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use crate::traits::ArgumentHandle;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};

    fn interp(wb: &TestWorkbook) -> crate::interpreter::Interpreter<'_> {
        wb.interpreter()
    }

    #[test]
    fn index_returns_reference_and_materializes_in_value_context() {
        let wb = TestWorkbook::new()
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(42))
            .with_function(std::sync::Arc::new(IndexFn));
        let ctx = interp(&wb);

        // Build INDEX(A1:C3,2,2) expecting B2
        let array_ref = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1:C3".into(),
                reference: ReferenceType::Range {
                    sheet: None,
                    start_row: Some(1),
                    start_col: Some(1),
                    end_row: Some(3),
                    end_col: Some(3),
                    start_row_abs: false,
                    start_col_abs: false,
                    end_row_abs: false,
                    end_col_abs: false,
                },
            },
            None,
        );
        let row = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(2)), None);
        let col = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(2)), None);
        let call = ASTNode::new(
            ASTNodeType::Function {
                name: "INDEX".into(),
                args: vec![array_ref.clone(), row.clone(), col.clone()],
            },
            None,
        );

        // Reference context
        let r = ctx.evaluate_ast_as_reference(&call).expect("ref ok");
        match r {
            ReferenceType::Cell { row, col, .. } => {
                assert_eq!((row, col), (2, 2));
            }
            _ => panic!(),
        }

        // Value context (scalar materialization)
        let args = vec![
            ArgumentHandle::new(&array_ref, &ctx),
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&col, &ctx),
        ];
        let f = ctx.context.get_function("", "INDEX").unwrap();
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Number(42.0));
    }

    #[test]
    fn offset_returns_reference_and_materializes() {
        let wb = TestWorkbook::new()
            .with_cell_a1("Sheet1", "A1", LiteralValue::Int(1))
            .with_cell_a1("Sheet1", "B2", LiteralValue::Int(5))
            .with_function(std::sync::Arc::new(OffsetFn));
        let ctx = interp(&wb);

        let base = ASTNode::new(
            ASTNodeType::Reference {
                original: "A1".into(),
                reference: ReferenceType::Cell {
                    sheet: None,
                    row: 1,
                    col: 1,
                    row_abs: false,
                    col_abs: false,
                },
            },
            None,
        );
        let dr = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let dc = ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(1)), None);
        let call = ASTNode::new(
            ASTNodeType::Function {
                name: "OFFSET".into(),
                args: vec![base.clone(), dr.clone(), dc.clone()],
            },
            None,
        );

        let r = ctx.evaluate_ast_as_reference(&call).expect("ref ok");
        match r {
            ReferenceType::Cell { row, col, .. } => assert_eq!((row, col), (2, 2)),
            _ => panic!(),
        }

        let args = vec![
            ArgumentHandle::new(&base, &ctx),
            ArgumentHandle::new(&dr, &ctx),
            ArgumentHandle::new(&dc, &ctx),
        ];
        let f = ctx.context.get_function("", "OFFSET").unwrap();
        let v = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(v, LiteralValue::Number(5.0));
    }
}
