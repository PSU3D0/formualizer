#[cfg(test)]
mod tests {
    use crate::test_workbook::TestWorkbook;
    use formualizer_common::error::{ExcelError, ExcelErrorKind};
    use formualizer_parse::LiteralValue;
    use formualizer_parse::Tokenizer;
    use formualizer_parse::parser::Parser;
    use std::sync::Arc;

    /// Helper function to parse and evaluate a formula.
    fn evaluate_formula(formula: &str, wb: &TestWorkbook) -> Result<LiteralValue, ExcelError> {
        let tokenizer = Tokenizer::new(formula).unwrap();
        let mut parser = Parser::new(tokenizer.items, false);
        let ast = parser
            .parse()
            .map_err(|e| ExcelError::new(ExcelErrorKind::Error).with_message(e.message.clone()))?;

        let interpreter = wb.interpreter();

        let cv = interpreter.evaluate_ast(&ast)?;
        if formula.contains('{') {
            Ok(match cv {
                crate::traits::CalcValue::Scalar(v) => v,
                crate::traits::CalcValue::Range(rv) => {
                    let (rows, _cols) = rv.dims();
                    let mut data = Vec::with_capacity(rows);
                    let _ = rv.for_each_row(&mut |row| {
                        data.push(row.to_vec());
                        Ok(())
                    });
                    LiteralValue::Array(data)
                }
            })
        } else {
            Ok(cv.into_literal())
        }
    }

    fn create_workbook() -> TestWorkbook {
        use std::sync::Arc;
        TestWorkbook::new()
            .with_function(Arc::new(crate::builtins::math::SumFn))
            .with_function(Arc::new(crate::builtins::logical::IfFn))
            .with_function(Arc::new(crate::builtins::logical::AndFn))
            .with_function(Arc::new(crate::builtins::logical::TrueFn))
            .with_function(Arc::new(crate::builtins::logical::FalseFn))
    }

    #[test]
    fn range_duplicate_sum_is_correct() {
        // Prepare a small range and sum it twice; internal caching is an engine concern.
        let wb = TestWorkbook::new()
            .with_function(Arc::new(crate::builtins::math::SumFn))
            .with_cell("Sheet1", 1, 1, LiteralValue::Int(1))
            .with_cell("Sheet1", 1, 2, LiteralValue::Int(2))
            .with_cell("Sheet1", 2, 1, LiteralValue::Int(3))
            .with_cell("Sheet1", 2, 2, LiteralValue::Int(4));
        let tokenizer = Tokenizer::new("=SUM(A1:B2, A1:B2)").unwrap();
        let mut parser = Parser::new(tokenizer.items, false);
        let ast = parser.parse().unwrap();
        let interp = wb.interpreter();
        let res = interp.evaluate_ast(&ast).unwrap().into_literal();
        assert_eq!(res, LiteralValue::Number(20.0));
    }

    #[test]
    fn test_basic_arithmetic() {
        let wb = create_workbook();
        // Basic arithmetic
        assert_eq!(
            evaluate_formula("=1+2", &wb).unwrap(),
            LiteralValue::Number(3.0)
        );
        assert_eq!(
            evaluate_formula("=3-1", &wb).unwrap(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            evaluate_formula("=2*3", &wb).unwrap(),
            LiteralValue::Number(6.0)
        );
        assert_eq!(
            evaluate_formula("=6/2", &wb).unwrap(),
            LiteralValue::Number(3.0)
        );
        assert_eq!(
            evaluate_formula("=2^3", &wb).unwrap(),
            LiteralValue::Number(8.0)
        );

        // Order of operations
        assert_eq!(
            evaluate_formula("=1+2*3", &wb).unwrap(),
            LiteralValue::Number(7.0)
        );
        assert_eq!(
            evaluate_formula("=(1+2)*3", &wb).unwrap(),
            LiteralValue::Number(9.0)
        );
        assert_eq!(
            evaluate_formula("=2^3+1", &wb).unwrap(),
            LiteralValue::Number(9.0)
        );
        assert_eq!(
            evaluate_formula("=2^(3+1)", &wb).unwrap(),
            LiteralValue::Number(16.0)
        );
    }

    #[test]
    fn test_unary_operators() {
        let wb = create_workbook();
        // Unary operators
        assert_eq!(
            evaluate_formula("=-5", &wb).unwrap(),
            LiteralValue::Number(-5.0)
        );
        assert_eq!(
            evaluate_formula("=+5", &wb).unwrap(),
            LiteralValue::Number(5.0)
        );
        assert_eq!(
            evaluate_formula("=--5", &wb).unwrap(),
            LiteralValue::Number(5.0)
        );
        assert_eq!(
            evaluate_formula("=-(-5)", &wb).unwrap(),
            LiteralValue::Number(5.0)
        );

        // Percentage operator
        assert_eq!(
            evaluate_formula("=50%", &wb).unwrap(),
            LiteralValue::Number(0.5)
        );
        assert_eq!(
            evaluate_formula("=100%+20%", &wb).unwrap(),
            LiteralValue::Number(1.2)
        );
    }

    #[test]
    fn test_value_coercion() {
        let wb = create_workbook();
        // Boolean to number coercion
        assert_eq!(
            evaluate_formula("=TRUE+1", &wb).unwrap(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            evaluate_formula("=FALSE+1", &wb).unwrap(),
            LiteralValue::Number(1.0)
        );

        // Text to number coercion
        assert_eq!(
            evaluate_formula("=\"5\"+2", &wb).unwrap(),
            LiteralValue::Number(7.0)
        );

        // Number to boolean coercion in logical contexts
        assert_eq!(
            evaluate_formula("=IF(1, \"Yes\", \"No\")", &wb).unwrap(),
            LiteralValue::Text("Yes".to_string())
        );
        assert_eq!(
            evaluate_formula("=IF(0, \"Yes\", \"No\")", &wb).unwrap(),
            LiteralValue::Text("No".to_string())
        );
    }

    #[test]
    fn test_string_concatenation() {
        let wb = create_workbook();
        // String concatenation
        assert_eq!(
            evaluate_formula("=\"Hello\"&\" \"&\"World\"", &wb).unwrap(),
            LiteralValue::Text("Hello World".to_string())
        );

        // Number to string coercion in concatenation
        assert_eq!(
            evaluate_formula("=\"LiteralValue: \"&123", &wb).unwrap(),
            LiteralValue::Text("LiteralValue: 123".to_string())
        );

        // Boolean to string coercion in concatenation
        assert_eq!(
            evaluate_formula("=\"Is true: \"&TRUE", &wb).unwrap(),
            LiteralValue::Text("Is true: TRUE".to_string())
        );
    }

    #[test]
    fn test_comparisons() {
        let wb = create_workbook();
        // Equal and not equal
        assert_eq!(
            evaluate_formula("=1=1", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1<>1", &wb).unwrap(),
            LiteralValue::Boolean(false)
        );
        assert_eq!(
            evaluate_formula("=1=2", &wb).unwrap(),
            LiteralValue::Boolean(false)
        );
        assert_eq!(
            evaluate_formula("=1<>2", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );

        // Greater than, less than
        assert_eq!(
            evaluate_formula("=2>1", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1<2", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1>2", &wb).unwrap(),
            LiteralValue::Boolean(false)
        );
        assert_eq!(
            evaluate_formula("=2<1", &wb).unwrap(),
            LiteralValue::Boolean(false)
        );

        // Greater than or equal, less than or equal
        assert_eq!(
            evaluate_formula("=2>=1", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1<=2", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1>=1", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1<=1", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );

        // Text comparisons
        assert_eq!(
            evaluate_formula("=\"a\"=\"a\"", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=\"a\"=\"A\"", &wb).unwrap(),
            LiteralValue::Boolean(true)
        ); // Case-insensitive
        assert_eq!(
            evaluate_formula("=\"a\"<\"b\"", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=\"b\">\"a\"", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );

        // Mixed type comparisons
        assert_eq!(
            evaluate_formula("=\"5\"=5", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=TRUE=1", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
    }

    #[test]
    fn test_function_calls() {
        let wb = create_workbook();
        assert_eq!(
            evaluate_formula("=SUM(1,2,3)", &wb).unwrap(),
            LiteralValue::Number(6.0)
        );

        // Function with array argument
        assert_eq!(
            evaluate_formula("=SUM({1,2,3;4,5,6})", &wb).unwrap(),
            LiteralValue::Number(21.0)
        );

        // Nested function calls
        assert_eq!(
            evaluate_formula("=IF(SUM(1,2)>0, \"Positive\", \"Negative\")", &wb).unwrap(),
            LiteralValue::Text("Positive".to_string())
        );

        // Function with boolean logic
        assert_eq!(
            evaluate_formula("=AND(TRUE, TRUE)", &wb).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=AND(TRUE, FALSE)", &wb).unwrap(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn test_cell_references() {
        let wb = create_workbook()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(5.0))
            .with_cell("Sheet1", 1, 2, LiteralValue::Number(10.0))
            .with_cell("Sheet1", 1, 3, LiteralValue::Text("Hello".to_string()));

        // Basic cell references
        assert_eq!(
            evaluate_formula("=A1", &wb).unwrap(),
            LiteralValue::Number(5.0)
        );
        assert_eq!(
            evaluate_formula("=A1+B1", &wb).unwrap(),
            LiteralValue::Number(15.0)
        );
        assert_eq!(
            evaluate_formula("=C1&\" World\"", &wb).unwrap(),
            LiteralValue::Text("Hello World".to_string())
        );

        // Reference in function
        assert_eq!(
            evaluate_formula("=SUM(A1,B1)", &wb).unwrap(),
            LiteralValue::Number(15.0)
        );
    }

    #[test]
    fn test_range_references() {
        let wb = create_workbook()
            .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
            .with_cell("Sheet1", 1, 2, LiteralValue::Number(2.0))
            .with_cell("Sheet1", 2, 1, LiteralValue::Number(3.0))
            .with_cell("Sheet1", 2, 2, LiteralValue::Number(4.0));

        // Sum of range
        assert_eq!(
            evaluate_formula("=SUM(A1:B2)", &wb).unwrap(),
            LiteralValue::Number(10.0)
        );
    }

    #[test]
    fn test_named_ranges() {
        let wb = create_workbook()
            .with_named_range(
                "MyRange",
                vec![
                    vec![LiteralValue::Number(10.0), LiteralValue::Number(20.0)],
                    vec![LiteralValue::Number(30.0), LiteralValue::Number(40.0)],
                ],
            )
            .with_cell("MyRange", 1, 1, LiteralValue::Number(100.0));

        // Use named range
        assert_eq!(
            evaluate_formula("=SUM(MyRange)", &wb).unwrap(),
            LiteralValue::Number(100.0)
        );
    }

    #[test]
    fn test_array_operations() {
        let wb = create_workbook();
        // Create an array
        let result = evaluate_formula("={1,2,3;4,5,6}", &wb).unwrap();
        if let LiteralValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0].len(), 3);
            assert_eq!(arr[0][0], LiteralValue::Number(1.0));
            assert_eq!(arr[1][2], LiteralValue::Number(6.0));
        } else {
            panic!("Expected array result");
        }

        // Array arithmetic
        let result = evaluate_formula("={1,2,3}+{4,5,6}", &wb).unwrap();
        if let LiteralValue::Array(arr) = result {
            assert_eq!(arr[0][0], LiteralValue::Number(5.0));
            assert_eq!(arr[0][1], LiteralValue::Number(7.0));
            assert_eq!(arr[0][2], LiteralValue::Number(9.0));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_complex_formulas() {
        let wb = create_workbook()
            .with_cell_a1("Sheet1", "A1", LiteralValue::Number(10.0))
            .with_cell_a1("Sheet1", "B1", LiteralValue::Number(5.0))
            .with_cell_a1("Sheet1", "C1", LiteralValue::Boolean(true));
        // Complex formula with multiple operations and functions
        assert_eq!(
            evaluate_formula("=IF(A1>B1, SUM(A1, B1)/(A1-B1), \"A1 <= B1\")", &wb).unwrap(),
            LiteralValue::Number(3.0)
        );

        // Formula with nested IF and boolean logic
        assert_eq!(
            evaluate_formula(
                "=IF(AND(A1>0, B1>0, C1), \"All positive\", \"Not all positive\")",
                &wb
            )
            .unwrap(),
            LiteralValue::Text("All positive".to_string())
        );
    }

    #[test]
    fn test_array_mismatched_dimensions() {
        let wb = create_workbook();
        // {1,2} is a 1x2 array and {3} is a 1x1 array.
        // Expected: broadcasting {3} across both columns => [[1+3, 2+3]] = [[4, 5]]
        let result = evaluate_formula("={1,2}+{3}", &wb).unwrap();
        let expected = LiteralValue::Array(vec![vec![
            LiteralValue::Number(4.0),
            LiteralValue::Number(5.0),
        ]]);
        assert_eq!(result, expected);
    }

    #[test]
    fn interpreter_broadcasts_comparisons() {
        let wb = create_workbook();
        // {1,2} = {1;2} => 2x2 booleans
        match evaluate_formula("={1,2}={1;2}", &wb).unwrap() {
            LiteralValue::Array(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
                assert_eq!(rows[0][0], LiteralValue::Boolean(true));
                assert_eq!(rows[0][1], LiteralValue::Boolean(false));
                assert_eq!(rows[1][0], LiteralValue::Boolean(false));
                assert_eq!(rows[1][1], LiteralValue::Boolean(true));
            }
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn interpreter_broadcasts_per_cell_errors() {
        let wb = create_workbook();
        // {1,0} ^ {-1;0.5} => per-cell #DIV/0! where 0^-1; others numeric
        match evaluate_formula("={1,0}^{-1;0.5}", &wb).unwrap() {
            LiteralValue::Array(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].len(), 2);
                // 1^-1 = 1; 0^-1 is treated as #NUM! by current semantics
                assert_eq!(rows[0][0], LiteralValue::Number(1.0));
                match &rows[0][1] {
                    LiteralValue::Error(e) => assert_eq!(e, "#NUM!"),
                    v => panic!("expected num error, got {v:?}"),
                }
                // 1^0.5 = 1; 0^0.5 = 0
                assert_eq!(rows[1][0], LiteralValue::Number(1.0));
                assert_eq!(rows[1][1], LiteralValue::Number(0.0));
            }
            v => panic!("unexpected {v:?}"),
        }
    }

    #[test]
    fn test_unary_operator_on_array() {
        let wb = create_workbook();
        let result = evaluate_formula("=-({1,-2,3})", &wb).unwrap();
        let expected = LiteralValue::Array(vec![vec![
            LiteralValue::Number(-1.0),
            LiteralValue::Number(2.0),
            LiteralValue::Number(-3.0),
        ]]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_percentage_operator_on_array() {
        let wb = create_workbook();
        let result = evaluate_formula("=({50,100}%)", &wb).unwrap();
        let expected = LiteralValue::Array(vec![vec![
            LiteralValue::Number(0.5),
            LiteralValue::Number(1.0),
        ]]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_exponentiation_error() {
        let wb = create_workbook();
        // Negative base with fractional exponent should yield a #NUM! error.
        if let LiteralValue::Error(ref e) = evaluate_formula("=(-4)^(0.5)", &wb).unwrap() {
            assert_eq!(e, "#NUM!");
        } else {
            panic!("Expected error result");
        }
    }

    #[test]
    fn test_zero_power_zero() {
        let wb = create_workbook();
        let result = evaluate_formula("=0^0", &wb).unwrap();
        assert_eq!(result, LiteralValue::Number(1.0));
    }

    #[test]
    fn test_division_array_scalar() {
        let wb = create_workbook();
        let result = evaluate_formula("={10,20}/10", &wb).unwrap();
        let expected = LiteralValue::Array(vec![vec![
            LiteralValue::Number(1.0),
            LiteralValue::Number(2.0),
        ]]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_division_scalar_array() {
        let wb = create_workbook();
        let result = evaluate_formula("=10/{2,0}", &wb).unwrap();
        if let LiteralValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0].len(), 2);
            assert_eq!(arr[0][0], LiteralValue::Number(5.0));
            if let LiteralValue::Error(ref e) = arr[0][1] {
                assert_eq!(e, "#DIV/0!");
            } else {
                panic!("Expected #DIV/0! error");
            }
        } else {
            panic!("Expected an array result");
        }
    }

    #[test]
    fn test_error_propagation_in_array() {
        let wb = create_workbook();
        let result = evaluate_formula("={\"abc\",5}+1", &wb).unwrap();
        if let LiteralValue::Array(arr) = result {
            assert_eq!(arr.len(), 1);
            assert_eq!(arr[0].len(), 2);
            if let LiteralValue::Error(ref e) = arr[0][0] {
                assert_eq!(e, "#VALUE!");
            } else {
                panic!("Expected error for non-coercible text");
            }
            assert_eq!(arr[0][1], LiteralValue::Number(6.0));
        } else {
            panic!("Expected an array result");
        }
    }

    #[test]
    fn test_invalid_reference() {
        let wb = create_workbook();
        let result = evaluate_formula("=Z999", &wb).unwrap();
        if let LiteralValue::Error(ref e) = result {
            assert_eq!(e, "#REF!");
        } else {
            panic!("Expected error for invalid cell reference");
        }
    }

    #[test]
    fn test_sum_function_argument_count() {
        let wb = create_workbook();
        // SUM() with no arguments returns 0 (Excel behavior)
        let result = evaluate_formula("=SUM()", &wb).unwrap();
        assert_eq!(result, LiteralValue::Number(0.0));
    }

    #[test]
    fn test_if_function_argument_count() {
        let wb = create_workbook();
        // IF expects at most 3 arguments.
        let result = evaluate_formula("=IF(TRUE,1,2,3,4)", &wb).unwrap();
        if let LiteralValue::Error(ref e) = result {
            // expected should mention "at most 3"
            assert!(
                e.message
                    .clone()
                    .unwrap()
                    .contains("expects 2 or 3 arguments, got 5")
            );
        } else {
            panic!("Expected wrong argument count error for IF");
        }
    }

    #[test]
    #[ignore]
    fn test_named_range_not_found() {
        let wb = create_workbook();
        let result = evaluate_formula("=SUM(NonExistentNamedRange)", &wb).unwrap();
        if let LiteralValue::Error(ref e) = result {
            assert_eq!(e, "#NAME?");
        } else {
            panic!("Expected error for non-existent named range");
        }
    }

    #[test]
    fn test_incompatible_types() {
        let wb = create_workbook();
        // Subtracting a number from a text string (not using concatenation) should yield #VALUE!
        let result = evaluate_formula("=\"text\"-1", &wb).unwrap();
        if let LiteralValue::Error(ref e) = result {
            assert_eq!(e, "#VALUE!");
        } else {
            panic!("Expected #VALUE! error for incompatible types");
        }
    }

    #[test]
    fn test_mixed_precedence_concatenation() {
        let wb = create_workbook();
        // Concatenation (&) has lower precedence than addition.
        // So "=\"A\"&1+2" should evaluate as "A" & (1+2) => "A3"
        let result = evaluate_formula("=\"A\"&1+2", &wb).unwrap();
        assert_eq!(result, LiteralValue::Text("A3".to_string()));
    }

    #[test]
    fn test_binary_ops_with_int_and_number() {
        fn unwrap_1x1(value: LiteralValue) -> LiteralValue {
            match value {
                LiteralValue::Array(arr) => {
                    if arr.len() == 1 && arr[0].len() == 1 {
                        arr[0][0].clone()
                    } else {
                        panic!("Expected 1x1 array result");
                    }
                }
                other => other,
            }
        }

        let wb = create_workbook();

        let v = unwrap_1x1(evaluate_formula("={1}+{2.5}", &wb).unwrap());
        assert_eq!(v, LiteralValue::Number(3.5));

        // Test Number + Int
        let v = unwrap_1x1(evaluate_formula("={2.5}+{1}", &wb).unwrap());
        assert_eq!(v, LiteralValue::Number(3.5));

        // Test Int - Number
        let v = unwrap_1x1(evaluate_formula("={5}-{2.5}", &wb).unwrap());
        assert_eq!(v, LiteralValue::Number(2.5));

        // Test Int * Number
        let v = unwrap_1x1(evaluate_formula("={3}*{1.5}", &wb).unwrap());
        assert_eq!(v, LiteralValue::Number(4.5));

        // Test Int / Number
        let v = unwrap_1x1(evaluate_formula("={6}/{2.5}", &wb).unwrap());
        assert_eq!(v, LiteralValue::Number(2.4));

        // Test Int ^ Number
        let v = unwrap_1x1(evaluate_formula("={2}^{2.5}", &wb).unwrap());
        if let LiteralValue::Number(n) = v {
            // Due to floating point precision issues, we compare with an epsilon
            assert!((n - 5.65685424949238).abs() < 0.000000001);
        } else {
            panic!("Expected numeric result");
        }
    }
}
