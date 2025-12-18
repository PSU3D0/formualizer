//! ADDRESS function - creates a cell reference as text
//!
//! Excel semantics:
//! - ADDRESS(row_num, column_num, [abs_num], [a1], [sheet_text])
//! - abs_num: 1 = absolute ($A$1), 2 = abs row (A$1), 3 = abs col ($A1), 4 = relative (A1)
//! - a1: TRUE = A1 style, FALSE = R1C1 style
//! - sheet_text: optional sheet name to include

use crate::args::{ArgSchema, CoercionPolicy, ShapeKind};
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::{
    ArgKind, ExcelError, ExcelErrorKind, LiteralValue, col_letters_from_1based,
};
use formualizer_macros::func_caps;

/// Convert a column number to Excel letter notation (1 = A, 27 = AA, etc.)
fn column_to_letters(col: u32) -> String {
    col_letters_from_1based(col).unwrap_or_default()
}

#[derive(Debug)]
pub struct AddressFn;

impl Function for AddressFn {
    fn name(&self) -> &'static str {
        "ADDRESS"
    }

    fn min_args(&self) -> usize {
        2
    }

    func_caps!(PURE);

    fn arg_schema(&self) -> &'static [ArgSchema] {
        use once_cell::sync::Lazy;
        static SCHEMA: Lazy<Vec<ArgSchema>> = Lazy::new(|| {
            vec![
                // row_num (required, strict number)
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
                // column_num (required, strict number)
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
                // abs_num (optional, default 1)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Number],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::NumberStrict,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Int(1)),
                },
                // a1 (optional, default TRUE)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Logical],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
                    coercion: CoercionPolicy::Logical,
                    max: None,
                    repeating: None,
                    default: Some(LiteralValue::Boolean(true)),
                },
                // sheet_text (optional)
                ArgSchema {
                    kinds: smallvec::smallvec![ArgKind::Text],
                    required: false,
                    by_ref: false,
                    shape: ShapeKind::Scalar,
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
        if args.len() < 2 {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Get row number
        let row_val = args[0].value()?.into_literal();
        if let LiteralValue::Error(e) = row_val {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
        }
        let row = match row_val {
            LiteralValue::Number(n) => n as i64,
            LiteralValue::Int(i) => i,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
        };

        if !(1..=1_048_576).contains(&row) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Get column number
        let col_val = args[1].value()?.into_literal();
        if let LiteralValue::Error(e) = col_val {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
        }
        let col = match col_val {
            LiteralValue::Number(n) => n as i64,
            LiteralValue::Int(i) => i,
            _ => {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                    ExcelError::new(ExcelErrorKind::Value),
                )));
            }
        };

        if !(1..=16384).contains(&col) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Get abs_num (default 1 = absolute)
        let abs_num = if args.len() > 2 {
            let abs_val = args[2].value()?.into_literal();
            if let LiteralValue::Error(e) = abs_val {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            match abs_val {
                LiteralValue::Number(n) => n as i64,
                LiteralValue::Int(i) => i,
                _ => 1,
            }
        } else {
            1
        };

        if !(1..=4).contains(&abs_num) {
            return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(
                ExcelError::new(ExcelErrorKind::Value),
            )));
        }

        // Get a1 (default TRUE = A1 notation)
        let a1_style = if args.len() > 3 {
            let a1_val = args[3].value()?.into_literal();
            if let LiteralValue::Error(e) = a1_val {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            match a1_val {
                LiteralValue::Boolean(b) => b,
                _ => true,
            }
        } else {
            true
        };

        // Get sheet name (optional)
        let sheet_name = if args.len() > 4 {
            let sheet_val = args[4].value()?.into_literal();
            if let LiteralValue::Error(e) = sheet_val {
                return Ok(crate::traits::CalcValue::Scalar(LiteralValue::Error(e)));
            }
            match sheet_val {
                LiteralValue::Text(s) => Some(s),
                _ => None,
            }
        } else {
            None
        };

        // Build the address
        let address = if a1_style {
            // A1 notation
            let col_letters = column_to_letters(col as u32);
            let (col_abs, row_abs) = match abs_num {
                1 => (true, true),   // $A$1
                2 => (false, true),  // A$1
                3 => (true, false),  // $A1
                4 => (false, false), // A1
                _ => (true, true),
            };

            let col_str = if col_abs {
                format!("${col_letters}")
            } else {
                col_letters
            };
            let row_str = if row_abs {
                format!("${row}")
            } else {
                row.to_string()
            };
            format!("{col_str}{row_str}")
        } else {
            // R1C1 notation
            let (col_abs, row_abs) = match abs_num {
                1 => (true, true),
                2 => (false, true),
                3 => (true, false),
                4 => (false, false),
                _ => (true, true),
            };

            let row_str = if row_abs {
                format!("R{row}")
            } else {
                format!("R[{row}]")
            };
            let col_str = if col_abs {
                format!("C{col}")
            } else {
                format!("C[{col}]")
            };
            format!("{row_str}{col_str}")
        };

        // Add sheet name if provided
        let final_address = if let Some(sheet) = sheet_name {
            // Quote sheet name if it contains spaces or special characters
            if sheet.contains(' ') || sheet.contains('!') || sheet.contains('\'') {
                format!("'{}'!{address}", sheet.replace('\'', "''"))
            } else {
                format!("{sheet}!{address}")
            }
        } else {
            address
        };

        Ok(crate::traits::CalcValue::Scalar(LiteralValue::Text(
            final_address,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_workbook::TestWorkbook;
    use formualizer_parse::parser::{ASTNode, ASTNodeType};
    use std::sync::Arc;

    fn lit(v: LiteralValue) -> ASTNode {
        ASTNode::new(ASTNodeType::Literal(v), None)
    }

    #[test]
    fn test_column_to_letters() {
        assert_eq!(column_to_letters(1), "A");
        assert_eq!(column_to_letters(26), "Z");
        assert_eq!(column_to_letters(27), "AA");
        assert_eq!(column_to_letters(52), "AZ");
        assert_eq!(column_to_letters(53), "BA");
        assert_eq!(column_to_letters(702), "ZZ");
        assert_eq!(column_to_letters(703), "AAA");
    }

    #[test]
    fn address_basic() {
        let wb = TestWorkbook::new().with_function(Arc::new(AddressFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ADDRESS").unwrap();

        // ADDRESS(2, 3) -> "$C$2" (default absolute)
        let two = lit(LiteralValue::Int(2));
        let three = lit(LiteralValue::Int(3));

        let args = vec![
            ArgumentHandle::new(&two, &ctx),
            ArgumentHandle::new(&three, &ctx),
        ];

        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert_eq!(result, LiteralValue::Text("$C$2".into()));
    }

    #[test]
    fn address_abs_variations() {
        let wb = TestWorkbook::new().with_function(Arc::new(AddressFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ADDRESS").unwrap();

        let row = lit(LiteralValue::Int(5));
        let col = lit(LiteralValue::Int(4)); // Column D

        // abs_num = 1: $D$5
        let abs1 = lit(LiteralValue::Int(1));
        let args1 = vec![
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&col, &ctx),
            ArgumentHandle::new(&abs1, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args1, &ctx.function_context(None)).unwrap(),
            LiteralValue::Text("$D$5".into())
        );

        // abs_num = 2: D$5
        let abs2 = lit(LiteralValue::Int(2));
        let args2 = vec![
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&col, &ctx),
            ArgumentHandle::new(&abs2, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args2, &ctx.function_context(None)).unwrap(),
            LiteralValue::Text("D$5".into())
        );

        // abs_num = 3: $D5
        let abs3 = lit(LiteralValue::Int(3));
        let args3 = vec![
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&col, &ctx),
            ArgumentHandle::new(&abs3, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args3, &ctx.function_context(None)).unwrap(),
            LiteralValue::Text("$D5".into())
        );

        // abs_num = 4: D5
        let abs4 = lit(LiteralValue::Int(4));
        let args4 = vec![
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&col, &ctx),
            ArgumentHandle::new(&abs4, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args4, &ctx.function_context(None)).unwrap(),
            LiteralValue::Text("D5".into())
        );
    }

    #[test]
    fn address_with_sheet() {
        let wb = TestWorkbook::new().with_function(Arc::new(AddressFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ADDRESS").unwrap();

        let row = lit(LiteralValue::Int(1));
        let col = lit(LiteralValue::Int(1));
        let abs_num = lit(LiteralValue::Int(1));
        let a1_style = lit(LiteralValue::Boolean(true));

        // Simple sheet name
        let sheet1 = lit(LiteralValue::Text("Sheet1".into()));
        let args1 = vec![
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&col, &ctx),
            ArgumentHandle::new(&abs_num, &ctx),
            ArgumentHandle::new(&a1_style, &ctx),
            ArgumentHandle::new(&sheet1, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args1, &ctx.function_context(None)).unwrap(),
            LiteralValue::Text("Sheet1!$A$1".into())
        );

        // Sheet name with spaces (needs quoting)
        let sheet2 = lit(LiteralValue::Text("My Sheet".into()));
        let args2 = vec![
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&col, &ctx),
            ArgumentHandle::new(&abs_num, &ctx),
            ArgumentHandle::new(&a1_style, &ctx),
            ArgumentHandle::new(&sheet2, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args2, &ctx.function_context(None)).unwrap(),
            LiteralValue::Text("'My Sheet'!$A$1".into())
        );
    }

    #[test]
    fn address_r1c1_style() {
        let wb = TestWorkbook::new().with_function(Arc::new(AddressFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ADDRESS").unwrap();

        let row = lit(LiteralValue::Int(5));
        let col = lit(LiteralValue::Int(3));
        let abs1 = lit(LiteralValue::Int(1));
        let r1c1 = lit(LiteralValue::Boolean(false));

        // R1C1 absolute
        let args = vec![
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&col, &ctx),
            ArgumentHandle::new(&abs1, &ctx),
            ArgumentHandle::new(&r1c1, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args, &ctx.function_context(None))
                .unwrap()
                .into_literal(),
            LiteralValue::Text("R5C3".into())
        );

        // R1C1 relative
        let abs4 = lit(LiteralValue::Int(4));
        let args2 = vec![
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&col, &ctx),
            ArgumentHandle::new(&abs4, &ctx),
            ArgumentHandle::new(&r1c1, &ctx),
        ];
        assert_eq!(
            f.dispatch(&args2, &ctx.function_context(None)).unwrap(),
            LiteralValue::Text("R[5]C[3]".into())
        );
    }

    #[test]
    fn address_edge_cases() {
        let wb = TestWorkbook::new().with_function(Arc::new(AddressFn));
        let ctx = wb.interpreter();
        let f = ctx.context.get_function("", "ADDRESS").unwrap();

        // Row too large
        let big_row = lit(LiteralValue::Int(1_048_577));
        let col = lit(LiteralValue::Int(1));
        let args = vec![
            ArgumentHandle::new(&big_row, &ctx),
            ArgumentHandle::new(&col, &ctx),
        ];
        let result = f
            .dispatch(&args, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(result, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));

        // Column too large
        let row = lit(LiteralValue::Int(1));
        let big_col = lit(LiteralValue::Int(16385));
        let args2 = vec![
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&big_col, &ctx),
        ];
        let result2 = f
            .dispatch(&args2, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(result2, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));

        // Invalid abs_num
        let abs5 = lit(LiteralValue::Int(5));
        let normal_col = lit(LiteralValue::Int(1));
        let args3 = vec![
            ArgumentHandle::new(&row, &ctx),
            ArgumentHandle::new(&normal_col, &ctx),
            ArgumentHandle::new(&abs5, &ctx),
        ];
        let result3 = f
            .dispatch(&args3, &ctx.function_context(None))
            .unwrap()
            .into_literal();
        assert!(matches!(result3, LiteralValue::Error(e) if e.kind == ExcelErrorKind::Value));
    }
}
