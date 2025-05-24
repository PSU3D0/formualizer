use formualizer_macros::excel_fn;

use crate::traits::{ArgumentHandle, EvaluationContext};

use formualizer_common::{
    LiteralValue,
    error::{ExcelError, ExcelErrorKind},
};

/* ─────────────── SUM(A, …) ─────────────── */
#[excel_fn(name = "SUM", min = 1, variadic, arg_types = "any")]
fn test_sum_fn(
    args: &[ArgumentHandle],
    _ctx: &dyn EvaluationContext,
) -> Result<LiteralValue, ExcelError> {
    if args.is_empty() {
        return Ok(LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Value)
                .with_message("SUM expects at least one argument".to_string()),
        ));
    }

    let total = 0.0;
    let mut total = 0.0;
    for h in args {
        match h.value()?.as_ref() {
            LiteralValue::Error(e) => return Ok(LiteralValue::Error(e.clone())),
            LiteralValue::Array(arr) => {
                for row in arr {
                    for v in row {
                        total += coerce_num(v)?;
                    }
                }
            }
            v => total += coerce_num(v)?,
        }
    }
    Ok(LiteralValue::Number(total))
}

fn coerce_num(v: &LiteralValue) -> Result<f64, ExcelError> {
    match v {
        LiteralValue::Number(n) => Ok(*n),
        LiteralValue::Int(i) => Ok(*i as f64),
        LiteralValue::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
        LiteralValue::Empty => Ok(0.0),
        _ => Err(ExcelError::new(ExcelErrorKind::Value)
            .with_message(format!("Cannot convert {:?} to number", v))),
    }
}

/* ─────────────── IF(cond, then, else) ─────────────── */
#[excel_fn(name = "IF", min = 2, variadic)]
fn test_if_fn(
    args: &[ArgumentHandle],
    ctx: &dyn EvaluationContext,
) -> Result<LiteralValue, ExcelError> {
    if args.len() < 2 || args.len() > 3 {
        return Ok(LiteralValue::Error(
            ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("IF expects 2 or 3 arguments, got {}", args.len())),
        ));
    }
    let cond = args[0].value()?;
    let truthy = cond.is_truthy();
    let branch = if truthy {
        &args[1]
    } else {
        args.get(2).unwrap_or(&args[1])
    };
    branch.value().map(|v| v.into_owned())
}

#[cfg(test)]
mod tests {
    use formualizer_core::LiteralValue;

    use crate::test_workbook::TestWorkbook;
    use crate::with_fns;
    use formualizer_common::error::{ExcelError, ExcelErrorKind};
    use formualizer_core::parser::Parser;
    use formualizer_core::tokenizer::Tokenizer;
    

    use super::{__FnIF, __FnSUM};
    use crate::builtins::logical::{__FnAND, __FnFALSE, __FnOR, __FnTRUE};

    /// Helper function to parse and evaluate a formula.
    fn evaluate_formula(formula: &str, wb: TestWorkbook) -> Result<LiteralValue, ExcelError> {
        let tokenizer = Tokenizer::new(formula).unwrap();
        let mut parser = Parser::new(tokenizer.items, false);
        let ast = parser
            .parse()
            .map_err(|e| ExcelError::new(ExcelErrorKind::Error).with_message(e.message.clone()))?;

        let interpreter = wb.interpreter();
        
        interpreter.evaluate_ast(&ast)
    }

    fn create_workbook() -> TestWorkbook {
        
        TestWorkbook::new().with_fns(with_fns![
            __FnSUM, __FnIF, __FnAND, __FnOR, __FnFALSE, __FnTRUE
        ])
    }

    #[test]
    fn test_basic_arithmetic() {
        // Basic arithmetic
        assert_eq!(
            evaluate_formula("=1+2", create_workbook()).unwrap(),
            LiteralValue::Number(3.0)
        );
        assert_eq!(
            evaluate_formula("=3-1", create_workbook()).unwrap(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            evaluate_formula("=2*3", create_workbook()).unwrap(),
            LiteralValue::Number(6.0)
        );
        assert_eq!(
            evaluate_formula("=6/2", create_workbook()).unwrap(),
            LiteralValue::Number(3.0)
        );
        assert_eq!(
            evaluate_formula("=2^3", create_workbook()).unwrap(),
            LiteralValue::Number(8.0)
        );

        // Order of operations
        assert_eq!(
            evaluate_formula("=1+2*3", create_workbook()).unwrap(),
            LiteralValue::Number(7.0)
        );
        assert_eq!(
            evaluate_formula("=(1+2)*3", create_workbook()).unwrap(),
            LiteralValue::Number(9.0)
        );
        assert_eq!(
            evaluate_formula("=2^3+1", create_workbook()).unwrap(),
            LiteralValue::Number(9.0)
        );
        assert_eq!(
            evaluate_formula("=2^(3+1)", create_workbook()).unwrap(),
            LiteralValue::Number(16.0)
        );
    }

    #[test]
    fn test_unary_operators() {
        // Unary operators
        assert_eq!(
            evaluate_formula("=-5", create_workbook()).unwrap(),
            LiteralValue::Number(-5.0)
        );
        assert_eq!(
            evaluate_formula("=+5", create_workbook()).unwrap(),
            LiteralValue::Number(5.0)
        );
        assert_eq!(
            evaluate_formula("=--5", create_workbook()).unwrap(),
            LiteralValue::Number(5.0)
        );
        assert_eq!(
            evaluate_formula("=-(-5)", create_workbook()).unwrap(),
            LiteralValue::Number(5.0)
        );

        // Percentage operator
        assert_eq!(
            evaluate_formula("=50%", create_workbook()).unwrap(),
            LiteralValue::Number(0.5)
        );
        assert_eq!(
            evaluate_formula("=100%+20%", create_workbook()).unwrap(),
            LiteralValue::Number(1.2)
        );
    }

    #[test]
    fn test_value_coercion() {
        // Boolean to number coercion
        assert_eq!(
            evaluate_formula("=TRUE+1", create_workbook()).unwrap(),
            LiteralValue::Number(2.0)
        );
        assert_eq!(
            evaluate_formula("=FALSE+1", create_workbook()).unwrap(),
            LiteralValue::Number(1.0)
        );

        // Text to number coercion
        assert_eq!(
            evaluate_formula("=\"5\"+2", create_workbook()).unwrap(),
            LiteralValue::Number(7.0)
        );

        // Number to boolean coercion in logical contexts
        assert_eq!(
            evaluate_formula("=IF(1, \"Yes\", \"No\")", create_workbook()).unwrap(),
            LiteralValue::Text("Yes".to_string())
        );
        assert_eq!(
            evaluate_formula("=IF(0, \"Yes\", \"No\")", create_workbook()).unwrap(),
            LiteralValue::Text("No".to_string())
        );
    }

    #[test]
    fn test_string_concatenation() {
        // String concatenation
        assert_eq!(
            evaluate_formula("=\"Hello\"&\" \"&\"World\"", create_workbook()).unwrap(),
            LiteralValue::Text("Hello World".to_string())
        );

        // Number to string coercion in concatenation
        assert_eq!(
            evaluate_formula("=\"LiteralValue: \"&123", create_workbook()).unwrap(),
            LiteralValue::Text("LiteralValue: 123".to_string())
        );

        // Boolean to string coercion in concatenation
        assert_eq!(
            evaluate_formula("=\"Is true: \"&TRUE", create_workbook()).unwrap(),
            LiteralValue::Text("Is true: TRUE".to_string())
        );
    }

    #[test]
    fn test_comparisons() {
        // Equal and not equal
        assert_eq!(
            evaluate_formula("=1=1", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1<>1", create_workbook()).unwrap(),
            LiteralValue::Boolean(false)
        );
        assert_eq!(
            evaluate_formula("=1=2", create_workbook()).unwrap(),
            LiteralValue::Boolean(false)
        );
        assert_eq!(
            evaluate_formula("=1<>2", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );

        // Greater than, less than
        assert_eq!(
            evaluate_formula("=2>1", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1<2", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1>2", create_workbook()).unwrap(),
            LiteralValue::Boolean(false)
        );
        assert_eq!(
            evaluate_formula("=2<1", create_workbook()).unwrap(),
            LiteralValue::Boolean(false)
        );

        // Greater than or equal, less than or equal
        assert_eq!(
            evaluate_formula("=2>=1", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1<=2", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1>=1", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=1<=1", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );

        // Text comparisons
        assert_eq!(
            evaluate_formula("=\"a\"=\"a\"", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=\"a\"=\"A\"", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        ); // Case-insensitive
        assert_eq!(
            evaluate_formula("=\"a\"<\"b\"", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=\"b\">\"a\"", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );

        // Mixed type comparisons
        assert_eq!(
            evaluate_formula("=\"5\"=5", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=TRUE=1", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
    }

    #[test]
    fn test_function_calls() {
        assert_eq!(
            evaluate_formula("=SUM(1,2,3)", create_workbook()).unwrap(),
            LiteralValue::Number(6.0)
        );

        // Function with array argument
        assert_eq!(
            evaluate_formula("=SUM({1,2,3;4,5,6})", create_workbook()).unwrap(),
            LiteralValue::Number(21.0)
        );

        // Nested function calls
        assert_eq!(
            evaluate_formula(
                "=IF(SUM(1,2)>0, \"Positive\", \"Negative\")",
                create_workbook()
            )
            .unwrap(),
            LiteralValue::Text("Positive".to_string())
        );

        // Function with boolean logic
        assert_eq!(
            evaluate_formula("=AND(TRUE, TRUE)", create_workbook()).unwrap(),
            LiteralValue::Boolean(true)
        );
        assert_eq!(
            evaluate_formula("=AND(TRUE, FALSE)", create_workbook()).unwrap(),
            LiteralValue::Boolean(false)
        );
    }

    #[test]
    fn test_cell_references() {
        fn create_workbook_with_cell_references() -> TestWorkbook {
            create_workbook()
                .with_cell("Sheet1", 1, 1, LiteralValue::Number(5.0))
                .with_cell("Sheet1", 1, 2, LiteralValue::Number(10.0))
                .with_cell("Sheet1", 1, 3, LiteralValue::Text("Hello".to_string()))
        }

        // Basic cell references
        assert_eq!(
            evaluate_formula("=A1", create_workbook_with_cell_references()).unwrap(),
            LiteralValue::Number(5.0)
        );
        assert_eq!(
            evaluate_formula("=A1+B1", create_workbook_with_cell_references()).unwrap(),
            LiteralValue::Number(15.0)
        );
        assert_eq!(
            evaluate_formula("=C1&\" World\"", create_workbook_with_cell_references()).unwrap(),
            LiteralValue::Text("Hello World".to_string())
        );

        // Reference in function
        assert_eq!(
            evaluate_formula("=SUM(A1,B1)", create_workbook_with_cell_references()).unwrap(),
            LiteralValue::Number(15.0)
        );
    }

    #[test]
    fn test_range_references() {
        fn create_workbook_with_range_references() -> TestWorkbook {
            create_workbook()
                .with_cell("Sheet1", 1, 1, LiteralValue::Number(1.0))
                .with_cell("Sheet1", 1, 2, LiteralValue::Number(2.0))
                .with_cell("Sheet1", 2, 1, LiteralValue::Number(3.0))
                .with_cell("Sheet1", 2, 2, LiteralValue::Number(4.0))
        }

        // Sum of range
        assert_eq!(
            evaluate_formula("=SUM(A1:B2)", create_workbook_with_range_references()).unwrap(),
            LiteralValue::Number(10.0)
        );
    }

    #[test]
    fn test_named_ranges() {
        fn create_workbook_with_named_ranges() -> TestWorkbook {
            create_workbook()
                .with_named_range(
                    "MyRange",
                    vec![
                        vec![LiteralValue::Number(10.0), LiteralValue::Number(20.0)],
                        vec![LiteralValue::Number(30.0), LiteralValue::Number(40.0)],
                    ],
                )
                .with_cell("MyRange", 1, 1, LiteralValue::Number(100.0))
        }

        // Use named range
        assert_eq!(
            evaluate_formula("=SUM(MyRange)", create_workbook_with_named_ranges()).unwrap(),
            LiteralValue::Number(100.0)
        );
    }

    #[test]
    fn test_array_operations() {
        // Create an array
        let result = evaluate_formula("={1,2,3;4,5,6}", create_workbook()).unwrap();
        if let LiteralValue::Array(arr) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0].len(), 3);
            assert_eq!(arr[0][0], LiteralValue::Number(1.0));
            assert_eq!(arr[1][2], LiteralValue::Number(6.0));
        } else {
            panic!("Expected array result");
        }

        // Array arithmetic
        let result = evaluate_formula("={1,2,3}+{4,5,6}", create_workbook()).unwrap();
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
        fn create_workbook_with_complex_formulas() -> TestWorkbook {
            create_workbook()
                .with_cell_a1("Sheet1", "A1", LiteralValue::Number(10.0))
                .with_cell_a1("Sheet1", "B1", LiteralValue::Number(5.0))
                .with_cell_a1("Sheet1", "C1", LiteralValue::Boolean(true))
        }
        // Complex formula with multiple operations and functions
        assert_eq!(
            evaluate_formula(
                "=IF(A1>B1, SUM(A1, B1)/(A1-B1), \"A1 <= B1\")",
                create_workbook_with_complex_formulas()
            )
            .unwrap(),
            LiteralValue::Number(3.0)
        );

        // Formula with nested IF and boolean logic
        assert_eq!(
            evaluate_formula(
                "=IF(AND(A1>0, B1>0, C1), \"All positive\", \"Not all positive\")",
                create_workbook_with_complex_formulas()
            )
            .unwrap(),
            LiteralValue::Text("All positive".to_string())
        );
    }

    #[test]
    fn test_array_mismatched_dimensions() {
        // {1,2} is a 1x2 array and {3} is a 1x1 array.
        // Expected: elementwise addition with missing values treated as Empty (coerced to 0).
        // => [[1+3, 2+0]] = [[4, 2]]
        let result = evaluate_formula("={1,2}+{3}", create_workbook()).unwrap();
        let expected = LiteralValue::Array(vec![vec![
            LiteralValue::Number(4.0),
            LiteralValue::Number(2.0),
        ]]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_unary_operator_on_array() {
        let result = evaluate_formula("=-({1,-2,3})", create_workbook()).unwrap();
        let expected = LiteralValue::Array(vec![vec![
            LiteralValue::Number(-1.0),
            LiteralValue::Number(2.0),
            LiteralValue::Number(-3.0),
        ]]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_percentage_operator_on_array() {
        let result = evaluate_formula("=({50,100}%)", create_workbook()).unwrap();
        let expected = LiteralValue::Array(vec![vec![
            LiteralValue::Number(0.5),
            LiteralValue::Number(1.0),
        ]]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_exponentiation_error() {
        // Negative base with fractional exponent should yield a #NUM! error.
        if let LiteralValue::Error(ref e) =
            evaluate_formula("=(-4)^(0.5)", create_workbook()).unwrap()
        {
            assert_eq!(e, "#NUM!");
        } else {
            panic!("Expected error result");
        }
    }

    #[test]
    fn test_zero_power_zero() {
        let result = evaluate_formula("=0^0", create_workbook()).unwrap();
        assert_eq!(result, LiteralValue::Number(1.0));
    }

    #[test]
    fn test_division_array_scalar() {
        let result = evaluate_formula("={10,20}/10", create_workbook()).unwrap();
        let expected = LiteralValue::Array(vec![vec![
            LiteralValue::Number(1.0),
            LiteralValue::Number(2.0),
        ]]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_division_scalar_array() {
        let result = evaluate_formula("=10/{2,0}", create_workbook()).unwrap();
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
        let result = evaluate_formula("={\"abc\",5}+1", create_workbook()).unwrap();
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
        let result = evaluate_formula("=Z999", create_workbook()).unwrap();
        if let LiteralValue::Error(ref e) = result {
            assert_eq!(e, "#REF!");
        } else {
            panic!("Expected error for invalid cell reference");
        }
    }

    #[test]
    fn test_sum_function_argument_count() {
        // SUM expects at least one argument.
        let result = evaluate_formula("=SUM()", create_workbook()).unwrap();
        if let LiteralValue::Error(ref e) = result {
            assert!(e.message.clone().unwrap().contains("at least"));
        } else {
            panic!("Expected wrong argument count error");
        }
    }

    #[test]
    fn test_if_function_argument_count() {
        // IF expects at most 3 arguments.
        let result = evaluate_formula("=IF(TRUE,1,2,3,4)", create_workbook()).unwrap();
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
    fn test_named_range_not_found() {
        let result = evaluate_formula("=SUM(NonExistentNamedRange)", create_workbook()).unwrap();
        if let LiteralValue::Error(ref e) = result {
            assert_eq!(e, "#NAME?");
        } else {
            panic!("Expected error for non-existent named range");
        }
    }

    #[test]
    fn test_incompatible_types() {
        // Subtracting a number from a text string (not using concatenation) should yield #VALUE!
        let result = evaluate_formula("=\"text\"-1", create_workbook()).unwrap();
        if let LiteralValue::Error(ref e) = result {
            assert_eq!(e, "#VALUE!");
        } else {
            panic!("Expected #VALUE! error for incompatible types");
        }
    }

    #[test]
    fn test_mixed_precedence_concatenation() {
        // Concatenation (&) has lower precedence than addition.
        // So "=\"A\"&1+2" should evaluate as "A" & (1+2) => "A3"
        let result = evaluate_formula("=\"A\"&1+2", create_workbook()).unwrap();
        assert_eq!(result, LiteralValue::Text("A3".to_string()));
    }

    #[test]
    fn test_binary_ops_with_int_and_number() {
        let result = evaluate_formula("={1}+{2.5}", create_workbook()).unwrap();
        if let LiteralValue::Array(arr) = result {
            assert_eq!(arr[0][0], LiteralValue::Number(3.5));
        } else {
            panic!("Expected array result");
        }

        // Test Number + Int
        let result = evaluate_formula("={2.5}+{1}", create_workbook()).unwrap();
        if let LiteralValue::Array(arr) = result {
            assert_eq!(arr[0][0], LiteralValue::Number(3.5));
        } else {
            panic!("Expected array result");
        }

        // Test Int - Number
        let result = evaluate_formula("={5}-{2.5}", create_workbook()).unwrap();
        if let LiteralValue::Array(arr) = result {
            assert_eq!(arr[0][0], LiteralValue::Number(2.5));
        } else {
            panic!("Expected array result");
        }

        // Test Int * Number
        let result = evaluate_formula("={3}*{1.5}", create_workbook()).unwrap();
        if let LiteralValue::Array(arr) = result {
            assert_eq!(arr[0][0], LiteralValue::Number(4.5));
        } else {
            panic!("Expected array result");
        }

        // Test Int / Number
        let result = evaluate_formula("={6}/{2.5}", create_workbook()).unwrap();
        if let LiteralValue::Array(arr) = result {
            assert_eq!(arr[0][0], LiteralValue::Number(2.4));
        } else {
            panic!("Expected array result");
        }

        // Test Int ^ Number
        let result = evaluate_formula("={2}^{2.5}", create_workbook()).unwrap();
        if let LiteralValue::Array(arr) = result {
            if let LiteralValue::Number(n) = arr[0][0] {
                // Due to floating point precision issues, we compare with an epsilon
                assert!((n - 5.65685424949238).abs() < 0.000000001);
            } else {
                panic!("Expected number result in array");
            }
        } else {
            panic!("Expected array result");
        }
    }
}
