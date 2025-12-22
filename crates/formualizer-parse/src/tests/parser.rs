#[cfg(test)]
mod tests {
    use crate::FormulaDialect;
    use crate::tokenizer::Tokenizer;
    use formualizer_common::{ExcelError, LiteralValue};

    use crate::parser::{ASTNode, ASTNodeType, Parser, ParserError, ReferenceType};
    use crate::parser::{CollectPolicy, RefView};

    // Helper function to parse a formula
    fn parse_formula(formula: &str) -> Result<ASTNode, ParserError> {
        let tokenizer = Tokenizer::new(formula).map_err(|e| ParserError {
            message: e.to_string(),
            position: Some(e.pos),
        })?;
        let mut parser = Parser::new(tokenizer.items, false);
        parser.parse()
    }

    fn parse_formula_with_dialect(
        formula: &str,
        dialect: FormulaDialect,
    ) -> Result<ASTNode, ParserError> {
        let tokenizer = Tokenizer::new_with_dialect(formula, dialect).map_err(|e| ParserError {
            message: e.to_string(),
            position: Some(e.pos),
        })?;
        let mut parser = Parser::new_with_dialect(tokenizer.items, false, dialect);
        parser.parse()
    }

    #[test]
    fn parser_try_from_formula_is_fallible() {
        let err = match Parser::try_from_formula("=\"unterminated") {
            Ok(_) => panic!("expected tokenizer error"),
            Err(err) => err,
        };
        assert!(err.message.contains("Reached end"));
    }

    // Helper function to check if a formula contains a range reference with expected properties
    fn check_range_in_formula(formula: &str, range_check: impl Fn(&ReferenceType) -> bool) -> bool {
        let ast = parse_formula(formula).unwrap();
        let deps = ast.get_dependencies();

        deps.iter().any(|ref_type| match ref_type {
            ReferenceType::Range { .. } => range_check(ref_type),
            _ => false,
        })
    }

    #[test]
    fn test_contains_volatile_with_classifier() {
        let tokenizer = Tokenizer::new("=RAND()+A1").unwrap();
        let mut parser = Parser::new(tokenizer.items, false).with_volatility_classifier(|name| {
            name.eq_ignore_ascii_case("RAND")
                || name.eq_ignore_ascii_case("NOW")
                || name.eq_ignore_ascii_case("TODAY")
        });
        let ast = parser.parse().unwrap();
        assert!(ast.contains_volatile());

        let tokenizer = Tokenizer::new("=SUM(1,2,3)").unwrap();
        let mut parser = Parser::new(tokenizer.items, false)
            .with_volatility_classifier(|name| name.eq_ignore_ascii_case("RAND"));
        let ast = parser.parse().unwrap();
        assert!(!ast.contains_volatile());
    }

    #[test]
    fn test_refs_iterator_and_visitor_basic() {
        let ast = parse_formula("=A1 + SUM(B2:C3, NamedRange, Table1[Col])").unwrap();

        // Iterator should find references in stable order (left-to-right depth-first)
        let refs: Vec<RefView> = ast.refs().collect();
        assert!(!refs.is_empty());

        // Expect first is A1 cell
        match refs.first().unwrap() {
            RefView::Cell {
                sheet, row, col, ..
            } => {
                assert!(sheet.is_none());
                assert_eq!((*row, *col), (1, 1));
            }
            _ => panic!("expected first ref to be a Cell"),
        }

        // Visitor should hit same count
        let mut count = 0;
        ast.visit_refs(|_| count += 1);
        assert_eq!(count, refs.len());
    }

    #[test]
    fn test_collect_references_policy_no_expand() {
        let ast = parse_formula("=SUM(B2:C3)").unwrap();
        let policy = CollectPolicy {
            expand_small_ranges: false,
            range_expansion_limit: 0,
            include_names: true,
        };
        let refs = ast.collect_references(&policy);
        assert_eq!(refs.len(), 1);
        match &refs[0] {
            ReferenceType::Range {
                start_row,
                start_col,
                end_row,
                end_col,
                ..
            } => {
                assert_eq!(
                    (*start_row, *start_col, *end_row, *end_col),
                    (Some(2), Some(2), Some(3), Some(3))
                );
            }
            _ => panic!("expected a Range"),
        }
    }

    #[test]
    fn test_collect_references_policy_expand_small_range() {
        let ast = parse_formula("=SUM(B2:C3)").unwrap();
        let policy = CollectPolicy {
            expand_small_ranges: true,
            range_expansion_limit: 16,
            include_names: true,
        };
        let refs = ast.collect_references(&policy);
        // B2:C3 is 4 cells
        assert_eq!(refs.len(), 4);
        // Ensure cells include B2 and C3
        let mut have_b2 = false;
        let mut have_c3 = false;
        for r in refs {
            match r {
                ReferenceType::Cell { row, col, .. } if row == 2 && col == 2 => have_b2 = true,
                ReferenceType::Cell { row, col, .. } if row == 3 && col == 3 => have_c3 = true,
                _ => {}
            }
        }
        assert!(have_b2 && have_c3);
    }

    #[test]
    fn test_collect_references_policy_exclude_names() {
        let ast = parse_formula("=NamedRef + A1").unwrap();
        let policy = CollectPolicy {
            expand_small_ranges: false,
            range_expansion_limit: 0,
            include_names: false,
        };
        let refs = ast.collect_references(&policy);
        // Should only include A1
        assert_eq!(refs.len(), 1);
        match &refs[0] {
            ReferenceType::Cell { row, col, .. } => assert_eq!((*row, *col), (1, 1)),
            _ => panic!("expected a Cell ref"),
        }
    }

    #[test]
    fn test_parse_openformula_cell_reference() {
        let ast = parse_formula_with_dialect("=SUM([.A1])", FormulaDialect::OpenFormula).unwrap();

        let (name, args) = match &ast.node_type {
            ASTNodeType::Function { name, args } => (name, args),
            _ => panic!("expected Function node"),
        };

        assert_eq!(name, "SUM");
        assert_eq!(args.len(), 1);

        match &args[0].node_type {
            ASTNodeType::Reference { reference, .. } => {
                assert_eq!(reference, &ReferenceType::cell(None, 1, 1));
            }
            other => panic!("expected Reference argument, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_openformula_sheet_range() {
        let ast =
            parse_formula_with_dialect("=SUM([Sheet One.A1:.B2])", FormulaDialect::OpenFormula)
                .unwrap();

        let args = match &ast.node_type {
            ASTNodeType::Function { name, args } => {
                assert_eq!(name, "SUM");
                args
            }
            _ => panic!("expected Function node"),
        };

        assert_eq!(args.len(), 1);

        match &args[0].node_type {
            ASTNodeType::Reference { reference, .. } => {
                assert_eq!(
                    reference,
                    &ReferenceType::range(
                        Some("Sheet One".to_string()),
                        Some(1),
                        Some(1),
                        Some(2),
                        Some(2),
                    )
                );
            }
            other => panic!("expected range reference, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_simple_formula() {
        let ast = parse_formula("=A1+B2").unwrap();

        if let ASTNodeType::BinaryOp { op, left, right } = ast.node_type {
            assert_eq!(op, "+");

            if let ASTNodeType::Reference { reference, .. } = left.node_type {
                assert_eq!(reference, ReferenceType::cell(None, 1, 1));
            } else {
                panic!("Expected Reference node for left operand");
            }

            if let ASTNodeType::Reference { reference, .. } = right.node_type {
                assert_eq!(reference, ReferenceType::cell(None, 2, 2));
            } else {
                panic!("Expected Reference node for right operand");
            }
        } else {
            panic!("Expected BinaryOp node");
        }
    }

    #[test]
    fn test_parse_function_call() {
        let ast = parse_formula("=SUM(A1:B2)").unwrap();

        println!("AST: {ast:?}");

        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "SUM");
            assert_eq!(args.len(), 1);

            if let ASTNodeType::Reference {
                original,
                reference,
            } = &args[0].node_type
            {
                assert_eq!(original, "A1:B2");
                assert_eq!(
                    reference,
                    &ReferenceType::range(None, Some(1), Some(1), Some(2), Some(2))
                );
            } else {
                panic!("Expected Reference node for function argument");
            }
        } else {
            panic!("Expected Function node");
        }
    }

    #[test]
    fn test_operator_precedence() {
        let ast = parse_formula("=A1+B2*C3").unwrap();

        if let ASTNodeType::BinaryOp {
            op: op1,
            left: left1,
            right: right1,
        } = ast.node_type
        {
            assert_eq!(op1, "+");

            if let ASTNodeType::Reference { reference, .. } = left1.node_type {
                assert_eq!(reference, ReferenceType::cell(None, 1, 1));
            } else {
                panic!("Expected Reference node for left operand of +");
            }

            if let ASTNodeType::BinaryOp {
                op: op2,
                left: left2,
                right: right2,
            } = right1.node_type
            {
                assert_eq!(op2, "*");

                if let ASTNodeType::Reference { reference, .. } = left2.node_type {
                    assert_eq!(reference, ReferenceType::cell(None, 2, 2));
                } else {
                    panic!("Expected Reference node for left operand of *");
                }

                if let ASTNodeType::Reference { reference, .. } = right2.node_type {
                    assert_eq!(reference, ReferenceType::cell(None, 3, 3));
                } else {
                    panic!("Expected Reference node for right operand of *");
                }
            } else {
                panic!("Expected BinaryOp node for right operand of +");
            }
        } else {
            panic!("Expected BinaryOp node");
        }
    }

    #[test]
    fn test_parentheses() {
        let ast = parse_formula("=(A1+B2)*C3").unwrap();

        if let ASTNodeType::BinaryOp { op, left, right } = ast.node_type {
            assert_eq!(op, "*");

            if let ASTNodeType::BinaryOp { op: inner_op, .. } = left.node_type {
                assert_eq!(inner_op, "+");
            } else {
                panic!("Expected BinaryOp node for left operand");
            }

            if let ASTNodeType::Reference { reference, .. } = right.node_type {
                assert_eq!(reference, ReferenceType::cell(None, 3, 3));
            } else {
                panic!("Expected Reference node for right operand");
            }
        } else {
            panic!("Expected BinaryOp node");
        }
    }

    #[test]
    fn test_function_multiple_args() {
        let ast = parse_formula("=IF(A1>0,B1,C1)").unwrap();

        println!("AST: {ast:?}");

        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "IF");
            assert_eq!(args.len(), 3);

            // Check first argument (condition)
            if let ASTNodeType::BinaryOp { op, .. } = &args[0].node_type {
                assert_eq!(op, ">");
            } else {
                panic!("Expected BinaryOp node for first argument");
            }

            // Check second and third arguments (true/false results)
            if let ASTNodeType::Reference { reference, .. } = &args[1].node_type {
                assert_eq!(reference, &ReferenceType::cell(None, 1, 2));
            } else {
                panic!("Expected Reference node for second argument");
            }

            if let ASTNodeType::Reference { reference, .. } = &args[2].node_type {
                assert_eq!(reference, &ReferenceType::cell(None, 1, 3));
            } else {
                panic!("Expected Reference node for third argument");
            }
        } else {
            panic!("Expected Function node");
        }
    }

    #[test]
    fn test_functions_with_optional_arguments() {
        // Test with all arguments provided
        let ast = parse_formula("=VLOOKUP(A1,B1:C10,2,FALSE)").unwrap();
        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "VLOOKUP");
            assert_eq!(args.len(), 4);
        } else {
            panic!("Expected Function node");
        }

        // Test with missing optional argument
        let ast = parse_formula("=VLOOKUP(A1,B1:C10,2)").unwrap();
        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "VLOOKUP");
            assert_eq!(args.len(), 3);
        } else {
            panic!("Expected Function node");
        }

        // Test with multiple optional arguments - some specified, some not
        let ast = parse_formula("=IFERROR(A1/B1,)").unwrap();
        if let ASTNodeType::Function { name, args } = &ast.node_type {
            assert_eq!(name, "IFERROR");
            assert_eq!(args.len(), 2);
            // Second argument should be an empty string
            if let ASTNodeType::Literal(LiteralValue::Text(text)) = &args[1].node_type {
                assert_eq!(text, "");
            } else {
                panic!("Expected empty text literal for omitted argument");
            }
        } else {
            panic!("Expected Function node");
        }

        // Test skipping middle arguments
        let ast = parse_formula("=IF(A1>0,,C1)").unwrap();
        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "IF");
            assert_eq!(args.len(), 3);
            // Middle argument should be empty
            if let ASTNodeType::Literal(LiteralValue::Text(text)) = &args[1].node_type {
                assert_eq!(text, "");
            } else {
                panic!("Expected empty text literal for omitted middle argument");
            }
        } else {
            panic!("Expected Function node");
        }

        // Test with multiple trailing empty arguments
        let ast = parse_formula("=IF(A1>0,,)").unwrap();
        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "IF");
            assert_eq!(args.len(), 3);
            // Both optional arguments should be empty
            if let ASTNodeType::Literal(LiteralValue::Text(text)) = &args[1].node_type {
                assert_eq!(text, "");
            } else {
                panic!("Expected empty text literal for second argument");
            }
            if let ASTNodeType::Literal(LiteralValue::Text(text)) = &args[2].node_type {
                assert_eq!(text, "");
            } else {
                panic!("Expected empty text literal for third argument");
            }
        } else {
            panic!("Expected Function node");
        }

        // Test with complex empty arguments combination
        let ast = parse_formula("=CHOOSE(1,A1,,C1,,E1)").unwrap();
        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "CHOOSE");
            assert_eq!(args.len(), 6);
            // Check the empty arguments (3rd and 5th)
            if let ASTNodeType::Literal(LiteralValue::Text(text)) = &args[2].node_type {
                assert_eq!(text, "");
            } else {
                panic!("Expected empty text literal for third argument");
            }
            if let ASTNodeType::Literal(LiteralValue::Text(text)) = &args[4].node_type {
                assert_eq!(text, "");
            } else {
                panic!("Expected empty text literal for fifth argument");
            }
        } else {
            panic!("Expected Function node");
        }
    }

    #[test]
    fn test_nested_functions() {
        let ast = parse_formula("=IF(SUM(A1:A10)>100,MAX(B1:B10),0)").unwrap();

        println!("AST: {ast:?}");

        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "IF");
            assert_eq!(args.len(), 3);

            // Check first argument (SUM(...) > 100)
            if let ASTNodeType::BinaryOp { op, left, .. } = &args[0].node_type {
                assert_eq!(op, ">");

                if let ASTNodeType::Function {
                    name: inner_name, ..
                } = &left.node_type
                {
                    assert_eq!(inner_name, "SUM");
                } else {
                    panic!("Expected Function node for left side of comparison");
                }
            } else {
                panic!("Expected BinaryOp node for first argument");
            }

            // Check second argument (MAX(...))
            if let ASTNodeType::Function {
                name: inner_name, ..
            } = &args[1].node_type
            {
                assert_eq!(inner_name, "MAX");
            } else {
                panic!("Expected Function node for second argument");
            }

            // Check third argument (0)
            if let ASTNodeType::Literal(LiteralValue::Number(num)) = &args[2].node_type {
                assert_eq!(*num, 0.0);
            } else {
                panic!("Expected Number literal for third argument");
            }
        } else {
            panic!("Expected Function node");
        }
    }

    #[test]
    fn test_unary_operators() {
        let ast = parse_formula("=-A1").unwrap();

        if let ASTNodeType::UnaryOp { op, expr } = ast.node_type {
            assert_eq!(op, "-");

            if let ASTNodeType::Reference { reference, .. } = expr.node_type {
                assert_eq!(reference, ReferenceType::cell(None, 1, 1));
            } else {
                panic!("Expected Reference node for operand");
            }
        } else {
            panic!("Expected UnaryOp node");
        }
    }

    #[test]
    fn test_double_unary_operator() {
        let ast = parse_formula("=--A1").unwrap();

        if let ASTNodeType::UnaryOp { op, expr: _ } = ast.node_type {
            assert_eq!(op, "-");
        }
    }

    #[test]
    fn test_infinite_range_formulas() {
        // Column-wise infinite range (A:A)
        let formula = "=SUM(A:A)";
        let ast = parse_formula(formula).unwrap();

        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "SUM");
            assert_eq!(args.len(), 1);

            if let ASTNodeType::Reference { reference, .. } = &args[0].node_type {
                if let ReferenceType::Range {
                    start_col,
                    end_col,
                    start_row,
                    end_row,
                    ..
                } = reference
                {
                    assert_eq!(*start_col, Some(1));
                    assert_eq!(*end_col, Some(1));
                    assert_eq!(*start_row, None);
                    assert_eq!(*end_row, None);
                } else {
                    panic!("Expected Range reference");
                }
            } else {
                panic!("Expected Reference node");
            }
        } else {
            panic!("Expected Function node");
        }

        // Row-wise infinite range (1:1)
        let formula = "=SUM(1:1)";
        let ast = parse_formula(formula).unwrap();

        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "SUM");
            assert_eq!(args.len(), 1);

            if let ASTNodeType::Reference { reference, .. } = &args[0].node_type {
                if let ReferenceType::Range {
                    start_col,
                    end_col,
                    start_row,
                    end_row,
                    ..
                } = reference
                {
                    assert_eq!(*start_col, None);
                    assert_eq!(*end_col, None);
                    assert_eq!(*start_row, Some(1));
                    assert_eq!(*end_row, Some(1));
                } else {
                    panic!("Expected Range reference");
                }
            } else {
                panic!("Expected Reference node");
            }
        } else {
            panic!("Expected Function node");
        }

        // Partially bounded range (A1:A)
        let formula = "=SUM(A1:A)";
        assert!(check_range_in_formula(formula, |r| {
            if let ReferenceType::Range {
                start_col,
                end_col,
                start_row,
                end_row,
                ..
            } = r
            {
                return *start_col == Some(1)
                    && *end_col == Some(1)
                    && *start_row == Some(1)
                    && end_row.is_none();
            }
            false
        }));

        // Partially bounded range (A:A10)
        let formula = "=SUM(A:A10)";
        assert!(check_range_in_formula(formula, |r| {
            if let ReferenceType::Range {
                start_col,
                end_col,
                start_row,
                end_row,
                ..
            } = r
            {
                return *start_col == Some(1)
                    && *end_col == Some(1)
                    && start_row.is_none()
                    && *end_row == Some(10);
            }
            false
        }));

        // Sheet reference with infinite range
        let formula = "=SUM(Sheet1!A:A)";
        assert!(check_range_in_formula(formula, |r| {
            if let ReferenceType::Range {
                sheet,
                start_col,
                end_col,
                start_row,
                end_row,
                ..
            } = r
            {
                return sheet.as_ref().is_some_and(|s| s == "Sheet1")
                    && *start_col == Some(1)
                    && *end_col == Some(1)
                    && start_row.is_none()
                    && end_row.is_none();
            }
            false
        }));
    }

    #[test]
    fn test_array_literal() {
        let ast = parse_formula("={1,2;3,4}").unwrap();

        if let ASTNodeType::Array(rows) = ast.node_type {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].len(), 2);
            assert_eq!(rows[1].len(), 2);

            // Check values in the array
            if let ASTNodeType::Literal(LiteralValue::Number(num)) = &rows[0][0].node_type {
                assert_eq!(*num, 1.0);
            } else {
                panic!("Expected Number literal for [0][0]");
            }

            if let ASTNodeType::Literal(LiteralValue::Number(num)) = &rows[1][1].node_type {
                assert_eq!(*num, 4.0);
            } else {
                panic!("Expected Number literal for [1][1]");
            }
        } else {
            panic!("Expected Array node");
        }
    }

    #[test]
    fn test_complex_formula() {
        let ast = parse_formula("=IF(AND(A1>0,B1<10),SUM(C1:C10)/COUNT(C1:C10),\"N/A\")").unwrap();

        println!("AST: {ast:?}");

        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "IF");
            assert_eq!(args.len(), 3);

            // Check first argument (AND(...))
            if let ASTNodeType::Function {
                name: inner_name, ..
            } = &args[0].node_type
            {
                assert_eq!(inner_name, "AND");
            } else {
                panic!("Expected Function node for first argument");
            }

            // Check second argument (SUM(...)/COUNT(...))
            if let ASTNodeType::BinaryOp { op, .. } = &args[1].node_type {
                assert_eq!(op, "/");
            } else {
                panic!("Expected BinaryOp node for second argument");
            }

            // Check third argument ("N/A")
            if let ASTNodeType::Literal(LiteralValue::Text(text)) = &args[2].node_type {
                assert_eq!(text, "N/A");
            } else {
                panic!("Expected Text literal for third argument");
            }
        } else {
            panic!("Expected Function node");
        }
    }

    #[test]
    fn test_error_handling() {
        let result = parse_formula("=SUM(A1:B2");
        assert!(result.is_err());

        let result = parse_formula("=A1+");
        assert!(result.is_err());
    }

    #[test]
    fn test_whitespace_handling() {
        let ast = parse_formula("= A1 + B2 ").unwrap();

        if let ASTNodeType::BinaryOp { op, left, right } = ast.node_type {
            assert_eq!(op, "+");

            if let ASTNodeType::Reference { reference, .. } = left.node_type {
                assert_eq!(reference, ReferenceType::cell(None, 1, 1));
            } else {
                panic!("Expected Reference node for left operand");
            }

            if let ASTNodeType::Reference { reference, .. } = right.node_type {
                assert_eq!(reference, ReferenceType::cell(None, 2, 2));
            } else {
                panic!("Expected Reference node for right operand");
            }
        } else {
            panic!("Expected BinaryOp node");
        }
    }

    #[test]
    fn test_string_literals() {
        let ast = parse_formula("=\"Hello\"").unwrap();

        if let ASTNodeType::Literal(LiteralValue::Text(text)) = ast.node_type {
            assert_eq!(text, "Hello");
        } else {
            panic!("Expected Text literal");
        }

        // Test string with escaped quotes
        let ast = parse_formula("=\"Hello\"\"World\"").unwrap();

        if let ASTNodeType::Literal(LiteralValue::Text(text)) = ast.node_type {
            assert_eq!(text, "Hello\"World");
        } else {
            panic!("Expected Text literal");
        }
    }

    #[test]
    fn test_boolean_literals() {
        let ast = parse_formula("=TRUE").unwrap();

        if let ASTNodeType::Literal(LiteralValue::Boolean(value)) = ast.node_type {
            assert!(value);
        } else {
            panic!("Expected Boolean literal");
        }

        let ast = parse_formula("=FALSE").unwrap();

        if let ASTNodeType::Literal(LiteralValue::Boolean(value)) = ast.node_type {
            assert!(!value);
        } else {
            panic!("Expected Boolean literal");
        }
    }

    #[test]
    fn test_error_literals() {
        let ast = parse_formula("=#DIV/0!").unwrap();

        if let ASTNodeType::Literal(LiteralValue::Error(error)) = ast.node_type {
            assert_eq!(error, ExcelError::new_div());
        } else {
            panic!("Expected Error literal");
        }
    }

    #[test]
    fn test_empty_function_arguments() {
        // Parsing a function call with an empty argument list.
        let ast = parse_formula("=SUM()").unwrap();
        if let ASTNodeType::Function { name, args } = ast.node_type {
            assert_eq!(name, "SUM");
            assert_eq!(args.len(), 0);
        } else {
            panic!("Expected a Function node");
        }
    }
}

#[cfg(test)]
mod fingerprint_tests {
    use formualizer_common::LiteralValue;

    use crate::tokenizer::*;

    use crate::parser::{ASTNode, ASTNodeType};

    #[test]
    fn test_fingerprint_whitespace_insensitive() {
        // Test that formulas with different whitespace have the same fingerprint
        let f1 = "=SUM(a1, 2)";
        let f2 = "=  SUM( A1 ,2 )"; // diff whitespace/casing

        let fp1 = crate::parser::parse(f1).unwrap().fingerprint();
        let fp2 = crate::parser::parse(f2).unwrap().fingerprint();

        assert_eq!(
            fp1, fp2,
            "Formulas with different whitespace should have the same fingerprint"
        );

        // Different values should have different fingerprints
        let fp3 = crate::parser::parse("=SUM(A1,3)").unwrap().fingerprint();
        assert_ne!(
            fp1, fp3,
            "Formulas with different values should have different fingerprints"
        );
    }

    #[test]
    fn test_fingerprint_case_insensitivity() {
        // Test that formulas with different casing have the same fingerprint
        let f1 = "=sum(a1)";
        let f2 = "=SUM(A1)";

        let fp1 = crate::parser::parse(f1).unwrap().fingerprint();
        let fp2 = crate::parser::parse(f2).unwrap().fingerprint();

        assert_eq!(
            fp1, fp2,
            "Formulas with different casing should have the same fingerprint"
        );
    }

    #[test]
    fn test_fingerprint_different_structure() {
        // Test that formulas with different structure have different fingerprints
        let f1 = "=SUM(A1,B1)";
        let f2 = "=SUM(A1+B1)";

        let fp1 = crate::parser::parse(f1).unwrap().fingerprint();
        let fp2 = crate::parser::parse(f2).unwrap().fingerprint();

        assert_ne!(
            fp1, fp2,
            "Formulas with different structure should have different fingerprints"
        );
    }

    #[test]
    fn test_fingerprint_ignores_source_token() {
        // Create two identical ASTNodes but with different source_token values
        let value = LiteralValue::Number(42.0);
        let node_type = ASTNodeType::Literal(value);

        let token1 = Token::new("42".to_string(), TokenType::Operand, TokenSubType::Number);
        let token2 = Token::new("42.0".to_string(), TokenType::Operand, TokenSubType::Number);

        let node1 = ASTNode::new(node_type.clone(), Some(token1));
        let node2 = ASTNode::new(node_type, Some(token2));

        assert_eq!(
            node1.fingerprint(),
            node2.fingerprint(),
            "Fingerprints should be equal for nodes with same structure but different source_token"
        );
    }

    #[test]
    fn test_fingerprint_deterministic() {
        // Test that the fingerprint is deterministic across calls
        let formula = "=SUM(A1:B10)/COUNT(A1:B10)";
        let ast = crate::parser::parse(formula).unwrap();

        let fp1 = ast.fingerprint();
        let fp2 = ast.fingerprint();

        assert_eq!(
            fp1, fp2,
            "Fingerprint should be deterministic for the same AST"
        );
    }

    #[test]
    fn test_fingerprint_complex_formula() {
        // Test with a more complex formula
        let f1 = "=IF(AND(A1>0,B1<10),SUM(C1:C10)/COUNT(C1:C10),\"N/A\")";
        let f2 = "=IF(AND(A1>0,B1<10),SUM(C1:C10)/COUNT(C1:C10),\"N/A\")";

        let fp1 = crate::parser::parse(f1).unwrap().fingerprint();
        let fp2 = crate::parser::parse(f2).unwrap().fingerprint();

        assert_eq!(
            fp1, fp2,
            "Identical complex formulas should have the same fingerprint"
        );

        // Slightly different formula
        let f3 = "=IF(AND(A1>0,B1<=10),SUM(C1:C10)/COUNT(C1:C10),\"N/A\")";
        let fp3 = crate::parser::parse(f3).unwrap().fingerprint();

        assert_ne!(
            fp1, fp3,
            "Different complex formulas should have different fingerprints"
        );
    }

    #[test]
    fn test_validation_requirements() {
        // Test the specific validation example from the requirements
        let f1 = "=SUM(a1, 2)";
        let f2 = "=  SUM( A1 ,2 )"; // diff whitespace/casing
        let fp1 = crate::parser::parse(f1).unwrap().fingerprint();
        let fp2 = crate::parser::parse(f2).unwrap().fingerprint();
        assert_eq!(
            fp1, fp2,
            "Formulas with different whitespace and casing should have the same fingerprint"
        );

        let fp3 = crate::parser::parse("=SUM(A1,3)").unwrap().fingerprint();
        assert_ne!(
            fp1, fp3,
            "Formulas with different values should have different fingerprints"
        );
    }
}

#[cfg(test)]
mod normalise_tests {
    use crate::parser::normalise_reference;

    #[test]
    fn test_normalise_cell_references() {
        // Test normalizing cell references
        assert_eq!(normalise_reference("a1").unwrap(), "A1");
        assert_eq!(normalise_reference("$a$1").unwrap(), "$A$1");
        assert_eq!(normalise_reference("$A$1").unwrap(), "$A$1");
        assert_eq!(normalise_reference("Sheet1!$b$2").unwrap(), "Sheet1!$B$2");
        assert_eq!(normalise_reference("'Sheet1'!$b$2").unwrap(), "Sheet1!$B$2");
        assert_eq!(
            normalise_reference("'my sheet'!$b$2").unwrap(),
            "'my sheet'!$B$2"
        );
    }

    #[test]
    fn test_normalise_range_references() {
        // Test normalizing range references
        assert_eq!(normalise_reference("a1:b2").unwrap(), "A1:B2");
        assert_eq!(normalise_reference("$a$1:$b$2").unwrap(), "$A$1:$B$2");
        assert_eq!(
            normalise_reference("Sheet1!$a$1:$b$2").unwrap(),
            "Sheet1!$A$1:$B$2"
        );
        assert_eq!(
            normalise_reference("'my sheet'!$a$1:$b$2").unwrap(),
            "'my sheet'!$A$1:$B$2"
        );
        assert_eq!(normalise_reference("$a:$a").unwrap(), "$A:$A");
        assert_eq!(normalise_reference("$1:$1").unwrap(), "$1:$1");
    }

    #[test]
    fn test_normalise_table_references() {
        // Test normalizing table references
        assert_eq!(
            normalise_reference("Table1[Column1]").unwrap(),
            "Table1[Column1]"
        );
        assert_eq!(
            normalise_reference("Table1[ Column1 ]").unwrap(),
            "Table1[Column1]"
        );
        assert_eq!(
            normalise_reference("Table1[Column1:Column2]").unwrap(),
            "Table1[Column1:Column2]"
        );
        assert_eq!(
            normalise_reference("Table1[ Column1 : Column2 ]").unwrap(),
            "Table1[Column1:Column2]"
        );
        // Special items should remain unchanged
        assert_eq!(
            normalise_reference("Table1[#Headers]").unwrap(),
            "Table1[#Headers]"
        );
    }

    #[test]
    fn test_normalise_named_ranges() {
        // Named ranges should remain unchanged
        assert_eq!(normalise_reference("SalesData").unwrap(), "SalesData");
    }

    #[test]
    fn test_validation_examples() {
        // These are the examples given in the validation section
        assert_eq!(normalise_reference("a1").unwrap(), "A1");
        assert_eq!(
            normalise_reference("'my sheet'!$b$2").unwrap(),
            "'my sheet'!$B$2"
        );
        assert_eq!(normalise_reference("A:A").unwrap(), "A:A");
        assert_eq!(
            normalise_reference("Table1[ column ]").unwrap(),
            "Table1[column]"
        );
    }
}

#[cfg(test)]
mod reference_tests {
    use crate::parser::ReferenceType;
    use crate::parser::*;
    use crate::tokenizer::Tokenizer;

    #[test]
    fn test_cell_reference_parsing() {
        // Simple cell reference
        let reference = "A1";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(ref_type, ReferenceType::cell(None, 1, 1));

        // Cell reference with sheet
        let reference = "Sheet1!B2";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::cell(Some("Sheet1".to_string()), 2, 2)
        );

        // Cell reference with quoted sheet name
        let reference = "'Sheet 1'!C3";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::cell(Some("Sheet 1".to_string()), 3, 3)
        );

        // Cell reference with absolute reference
        let reference = "$D$4";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::cell_with_abs(None, 4, 4, true, true)
        );
    }

    #[test]
    fn test_range_reference_parsing() {
        // Simple range
        let reference = "A1:B2";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(None, Some(1), Some(1), Some(2), Some(2))
        );

        // Range with sheet
        let reference = "Sheet1!C3:D4";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(
                Some("Sheet1".to_string()),
                Some(3),
                Some(3),
                Some(4),
                Some(4),
            )
        );

        // Range with quoted sheet name
        let reference = "'Sheet 1'!E5:F6";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(
                Some("Sheet 1".to_string()),
                Some(5),
                Some(5),
                Some(6),
                Some(6),
            )
        );

        // Range with absolute references
        let reference = "$G$7:$H$8";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range_with_abs(
                None,
                Some(7),
                Some(7),
                Some(8),
                Some(8),
                true,
                true,
                true,
                true
            )
        );
    }

    #[test]
    fn test_infinite_range_parsing() {
        // Infinite column range (A:A)
        let reference = "A:A";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(None, None, Some(1), None, Some(1))
        );

        // Infinite row range (1:1)
        let reference = "1:1";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(None, Some(1), None, Some(1), None)
        );

        // Row range with sheet
        let reference = "Sheet1!3:4";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(Some("Sheet1".to_string()), Some(3), None, Some(4), None)
        );

        // Column range with sheet
        let reference = "Sheet1!C:D";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(Some("Sheet1".to_string()), None, Some(3), None, Some(4))
        );

        // Infinite column range with sheet (Sheet1!A:A)
        let reference = "Sheet1!A:A";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(Some("Sheet1".to_string()), None, Some(1), None, Some(1))
        );

        // Range with column-only to column-only (A:B)
        let reference = "A:B";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(None, None, Some(1), None, Some(2))
        );

        // Range with row-only to row-only (1:5)
        let reference = "1:5";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(None, Some(1), None, Some(5), None)
        );

        // Range with bounded start, unbounded end (A1:A)
        let reference = "A1:A";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(None, Some(1), Some(1), None, Some(1))
        );

        // Range with unbounded start, bounded end (A:A10)
        let reference = "A:A10";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::range(None, None, Some(1), Some(10), Some(1))
        );
    }

    #[test]
    fn test_range_to_string() {
        // Test to_string representation for normal ranges
        let range = ReferenceType::range(None, Some(1), Some(1), Some(2), Some(2));
        assert_eq!(range.to_excel_string(), "A1:B2");

        // Test to_string for infinite column range
        let range = ReferenceType::range(None, None, Some(1), None, Some(1));
        assert_eq!(range.to_excel_string(), "A:A");

        // Test to_string for infinite row range
        let range = ReferenceType::range(None, Some(1), None, Some(1), None);
        assert_eq!(range.to_excel_string(), "1:1");

        // Test to_string for partially infinite range (A1:A)
        let range = ReferenceType::range(None, Some(1), Some(1), None, Some(1));
        assert_eq!(range.to_excel_string(), "A1:A");

        // Test to_string for partially infinite range with sheet
        let range =
            ReferenceType::range(Some("Sheet1".to_string()), None, Some(1), Some(10), Some(1));
        assert_eq!(range.to_excel_string(), "Sheet1!A:A10");
    }

    #[test]
    fn test_table_reference_parsing() {
        // Table reference
        let reference = "Table1[Column1]";
        let ref_type = ReferenceType::from_string(reference).unwrap();

        // Check that we get a table reference with the correct name and column
        if let ReferenceType::Table(table_ref) = ref_type {
            assert_eq!(table_ref.name, "Table1");

            if let Some(TableSpecifier::Column(column)) = table_ref.specifier {
                assert_eq!(column, "Column1");
            } else {
                panic!("Expected Column specifier");
            }
        } else {
            panic!("Expected Table reference");
        }
    }

    #[test]
    fn test_external_workbook_reference_parsing() {
        let ref_type = ReferenceType::from_string("[33]Sheet1!$B:$B").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "[33]Sheet1!$B:$B".to_string(),
                book: ExternalBookRef::Token("[33]".to_string()),
                sheet: "Sheet1".to_string(),
                kind: ExternalRefKind::range_with_abs(
                    None,
                    Some(2),
                    None,
                    Some(2),
                    false,
                    true,
                    false,
                    true,
                ),
            })
        );

        let ref_type = ReferenceType::from_string("'[My Book.xlsx]Sheet1'!A1").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "'[My Book.xlsx]Sheet1'!A1".to_string(),
                book: ExternalBookRef::Token("[My Book.xlsx]".to_string()),
                sheet: "Sheet1".to_string(),
                kind: ExternalRefKind::cell(1, 1),
            })
        );
    }

    #[test]
    fn test_external_workbook_reference_paths_and_urls() {
        let ref_type = ReferenceType::from_string("'[C:\\Users\\me\\Book.xlsx]Sheet1'!A1").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "'[C:\\Users\\me\\Book.xlsx]Sheet1'!A1".to_string(),
                book: ExternalBookRef::Token("[C:\\Users\\me\\Book.xlsx]".to_string()),
                sheet: "Sheet1".to_string(),
                kind: ExternalRefKind::cell(1, 1),
            })
        );

        let ref_type = ReferenceType::from_string("'C:\\Users\\me\\[Book.xlsx]Sheet1'!A1").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "'C:\\Users\\me\\[Book.xlsx]Sheet1'!A1".to_string(),
                book: ExternalBookRef::Token("C:\\Users\\me\\[Book.xlsx]".to_string()),
                sheet: "Sheet1".to_string(),
                kind: ExternalRefKind::cell(1, 1),
            })
        );

        let ref_type =
            ReferenceType::from_string("[\\\\server\\share\\Book.xlsx]Sheet1!A1").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "[\\\\server\\share\\Book.xlsx]Sheet1!A1".to_string(),
                book: ExternalBookRef::Token("[\\\\server\\share\\Book.xlsx]".to_string()),
                sheet: "Sheet1".to_string(),
                kind: ExternalRefKind::cell(1, 1),
            })
        );

        let ref_type =
            ReferenceType::from_string("'[https://example.com/Book.xlsx]Sheet1'!1:3").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "'[https://example.com/Book.xlsx]Sheet1'!1:3".to_string(),
                book: ExternalBookRef::Token("[https://example.com/Book.xlsx]".to_string()),
                sheet: "Sheet1".to_string(),
                kind: ExternalRefKind::range(Some(1), None, Some(3), None),
            })
        );

        // Sheet names containing ']' but no '[' should not be treated as external.
        let ref_type = ReferenceType::from_string("'foo]bar'!A1").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::cell(Some("foo]bar".to_string()), 1, 1)
        );
    }

    #[test]
    fn test_external_workbook_sheet_names_with_spaces() {
        let ref_type = ReferenceType::from_string("'[Book.xlsx]My Sheet'!$A$1").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "'[Book.xlsx]My Sheet'!$A$1".to_string(),
                book: ExternalBookRef::Token("[Book.xlsx]".to_string()),
                sheet: "My Sheet".to_string(),
                kind: ExternalRefKind::cell_with_abs(1, 1, true, true),
            })
        );
    }

    #[test]
    fn test_external_workbook_unix_style_paths() {
        let ref_type = ReferenceType::from_string("'/tmp/[Book.xlsx]Sheet1'!A1").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "'/tmp/[Book.xlsx]Sheet1'!A1".to_string(),
                book: ExternalBookRef::Token("/tmp/[Book.xlsx]".to_string()),
                sheet: "Sheet1".to_string(),
                kind: ExternalRefKind::cell(1, 1),
            })
        );
    }

    #[test]
    fn test_external_workbook_sheet_name_can_contain_close_bracket() {
        let ref_type =
            ReferenceType::from_string("'C:\\Users\\me\\[Book.xlsx]S]heet1'!A1").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "'C:\\Users\\me\\[Book.xlsx]S]heet1'!A1".to_string(),
                book: ExternalBookRef::Token("C:\\Users\\me\\[Book.xlsx]".to_string()),
                sheet: "S]heet1".to_string(),
                kind: ExternalRefKind::cell(1, 1),
            })
        );
    }

    #[test]
    fn test_external_workbook_token_and_sheet_name_allow_escaped_quotes() {
        let ref_type = ReferenceType::from_string("'[O''Reilly.xlsx]Sheet1'!A1").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "'[O''Reilly.xlsx]Sheet1'!A1".to_string(),
                book: ExternalBookRef::Token("[O'Reilly.xlsx]".to_string()),
                sheet: "Sheet1".to_string(),
                kind: ExternalRefKind::cell(1, 1),
            })
        );

        let ref_type = ReferenceType::from_string("'[Book.xlsx]Bob''s Sheet'!A1").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::External(ExternalReference {
                raw: "'[Book.xlsx]Bob''s Sheet'!A1".to_string(),
                book: ExternalBookRef::Token("[Book.xlsx]".to_string()),
                sheet: "Bob's Sheet".to_string(),
                kind: ExternalRefKind::cell(1, 1),
            })
        );
    }

    #[test]
    fn test_sheet_scoped_table_reference_is_not_external() {
        let ref_type = ReferenceType::from_string("Sheet1!Table1[Column1]").unwrap();
        assert_eq!(
            ref_type,
            ReferenceType::Table(TableReference {
                name: "Table1".to_string(),
                specifier: Some(TableSpecifier::Column("Column1".to_string())),
            })
        );
    }

    #[test]
    fn test_named_range_parsing() {
        // Named range
        let reference = "SalesData";
        let ref_type = ReferenceType::from_string(reference).unwrap();
        assert_eq!(ref_type, ReferenceType::NamedRange(reference.to_string()));
    }

    #[test]
    fn test_column_to_number() {
        assert_eq!(ReferenceType::column_to_number("A").unwrap(), 1);
        assert_eq!(ReferenceType::column_to_number("Z").unwrap(), 26);
        assert_eq!(ReferenceType::column_to_number("AA").unwrap(), 27);
        assert_eq!(ReferenceType::column_to_number("AB").unwrap(), 28);
        assert_eq!(ReferenceType::column_to_number("BA").unwrap(), 53);
        assert_eq!(ReferenceType::column_to_number("ZZ").unwrap(), 702);
        assert_eq!(ReferenceType::column_to_number("AAA").unwrap(), 703);
    }

    #[test]
    fn test_number_to_column() {
        assert_eq!(ReferenceType::number_to_column(1), "A");
        assert_eq!(ReferenceType::number_to_column(26), "Z");
        assert_eq!(ReferenceType::number_to_column(27), "AA");
        assert_eq!(ReferenceType::number_to_column(28), "AB");
        assert_eq!(ReferenceType::number_to_column(53), "BA");
        assert_eq!(ReferenceType::number_to_column(702), "ZZ");
        assert_eq!(ReferenceType::number_to_column(703), "AAA");
    }

    #[test]
    fn test_get_dependencies() {
        // Parse a formula and check its dependencies
        let formula = "=A1+B1*SUM(C1:D2)";
        let tokenizer = Tokenizer::new(formula).unwrap();
        let mut parser = Parser::new(tokenizer.items, false);
        let ast = parser.parse().unwrap();

        let dependencies = ast.get_dependencies();

        // We expect three dependencies: A1, B1, and C1:D2
        assert_eq!(dependencies.len(), 3);

        let deps: Vec<ReferenceType> = dependencies.into_iter().cloned().collect();

        assert!(deps.contains(&ReferenceType::cell(None, 1, 1))); // A1
        assert!(deps.contains(&ReferenceType::cell(None, 1, 2))); // B1
        assert!(deps.contains(&ReferenceType::range(
            None,
            Some(1),
            Some(3),
            Some(2),
            Some(4)
        ))); // C1:D2
    }

    #[test]
    fn test_get_dependency_strings() {
        // Parse a formula and check its dependency strings
        let formula = "=A1+B1*SUM(C1:D2)";
        let tokenizer = Tokenizer::new(formula).unwrap();
        let mut parser = Parser::new(tokenizer.items, false);
        let ast = parser.parse().unwrap();

        let dependencies = ast.get_dependency_strings();

        // We expect three dependencies: A1, B1, and C1:D2
        assert_eq!(dependencies.len(), 3);
        assert!(dependencies.contains(&"A1".to_string()));
        assert!(dependencies.contains(&"B1".to_string()));
        assert!(dependencies.contains(&"C1:D2".to_string()));
    }

    #[test]
    fn test_complex_formula_dependencies() {
        let formula = "=IF(SUM(Sheet1!A1:A10)>100,MAX(Table1[Amount]),MIN('Data Sheet'!B1:B5))";
        let tokenizer = Tokenizer::new(formula).unwrap();
        let mut parser = Parser::new(tokenizer.items, false);
        let ast = parser.parse().unwrap();

        let dependencies = ast.get_dependency_strings();
        println!("Dependencies: {dependencies:?}");

        assert_eq!(dependencies.len(), 3);
        assert!(dependencies.contains(&"Sheet1!A1:A10".to_string()));
        assert!(dependencies.contains(&"Table1[Amount]".to_string()));
        assert!(dependencies.contains(&"'Data Sheet'!B1:B5".to_string()));
    }

    #[test]
    fn test_xlfn_function_parsing() {
        let formula = "=_xlfn.XLOOKUP(J7, 'GI XWALK'!$Q:$Q,'GI XWALK'!$R:$R,,0)";
        let tokenizer = Tokenizer::new(formula).unwrap();
        println!("tokenizer: {:?}", tokenizer.items);
        let mut parser = Parser::new(tokenizer.items, false);
        let ast = parser.parse().unwrap();
        println!("ast: {ast:?}");
    }

    #[test]
    fn test_dual_bracket_structured_reference_parsing() {
        let formula = "=EffortDB[[#All],[NPI]:[JMG Group]]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        println!("tokenizer: {:?}", tokenizer.items);
        let mut parser = Parser::new(tokenizer.items, false);
        let ast = parser.parse().unwrap();
        println!("ast: {ast:?}");

        // When the formula is tokenized and parsed, the equals sign is removed,
        // so we compare against the formula without the equals sign
        if let ASTNodeType::Reference {
            original,
            reference,
        } = &ast.node_type
        {
            assert_eq!(original, &"EffortDB[[#All],[NPI]:[JMG Group]]".to_string());

            // Check that reference is a Table type with the correct name
            if let ReferenceType::Table(table_ref) = reference {
                assert_eq!(table_ref.name, "EffortDB");

                // Check that the specifier is correctly parsed
                // (in this case, it should be a Column since we're not fully
                // parsing the complex specifier yet)
                assert!(table_ref.specifier.is_some());
            } else {
                panic!("Expected Table reference");
            }
        } else {
            panic!("Expected Reference node");
        }
    }

    #[test]
    fn test_table_reference_with_simple_column() {
        // Test a simple table reference with just a column
        let reference = "Table1[Column1]";
        let ref_type = ReferenceType::from_string(reference).unwrap();

        if let ReferenceType::Table(table_ref) = ref_type {
            assert_eq!(table_ref.name, "Table1");

            if let Some(specifier) = table_ref.specifier {
                match specifier {
                    TableSpecifier::Column(column) => {
                        assert_eq!(column, "Column1");
                    }
                    _ => panic!("Expected Column specifier"),
                }
            } else {
                panic!("Expected specifier to be Some");
            }
        } else {
            panic!("Expected Table reference");
        }
    }

    #[test]
    fn test_table_reference_with_column_range() {
        // Test a table reference with a column range
        let reference = "Table1[Column1:Column2]";
        let ref_type = ReferenceType::from_string(reference).unwrap();

        if let ReferenceType::Table(table_ref) = ref_type {
            assert_eq!(table_ref.name, "Table1");

            if let Some(specifier) = table_ref.specifier {
                match specifier {
                    TableSpecifier::ColumnRange(start, end) => {
                        assert_eq!(start, "Column1");
                        assert_eq!(end, "Column2");
                    }
                    _ => panic!("Expected ColumnRange specifier"),
                }
            } else {
                panic!("Expected specifier to be Some");
            }
        } else {
            panic!("Expected Table reference");
        }
    }

    #[test]
    fn test_table_reference_with_special_item() {
        // Test a table reference with a special item
        let reference = "Table1[#Headers]";
        let ref_type = ReferenceType::from_string(reference).unwrap();

        if let ReferenceType::Table(table_ref) = ref_type {
            assert_eq!(table_ref.name, "Table1");

            if let Some(specifier) = table_ref.specifier {
                match specifier {
                    TableSpecifier::SpecialItem(item) => {
                        assert_eq!(item, SpecialItem::Headers);
                    }
                    _ => panic!("Expected SpecialItem specifier"),
                }
            } else {
                panic!("Expected specifier to be Some");
            }
        } else {
            panic!("Expected Table reference");
        }
    }

    #[test]
    fn test_single_bracket_structured_reference_parsing() {
        let formula = "=EffortDB[#All]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        println!("tokenizer: {:?}", tokenizer.items);
        let mut parser = Parser::new(tokenizer.items, false);
        let ast = parser.parse().unwrap();
        println!("ast: {ast:?}");
    }

    #[test]
    fn test_table_reference_without_specifier() {
        // Test a table reference without any specifier (entire table)
        let reference = "Table1";
        let ref_type = ReferenceType::from_string(reference).unwrap();

        // After our column name length limit (max 3 chars), "Table1" can't be parsed as a cell
        // reference anymore (Table is 5 chars). It should be treated as a named range.
        if let ReferenceType::NamedRange(name) = ref_type {
            assert_eq!(name, "Table1");
        } else {
            panic!("Expected NamedRange, got: {ref_type:?}");
        }
    }

    #[test]
    fn test_table_item_with_column_reference() {
        // Test a table reference with an item specifier and column
        let reference = "Table1[[#Data],[Column1]]";
        let ref_type = ReferenceType::from_string(reference).unwrap();

        if let ReferenceType::Table(table_ref) = ref_type {
            assert_eq!(table_ref.name, "Table1");

            // Currently our implementation doesn't fully parse complex specifiers,
            // but we should at least verify it's parsed as a table reference
            assert!(table_ref.specifier.is_some());

            // Note: In the future, we should enhance this to properly parse
            // complex structured references and verify the exact specifier
        } else {
            panic!("Expected Table reference");
        }
    }

    #[test]
    fn test_table_this_row_with_column_reference() {
        // Test a table reference with this row specifier and column
        let reference = "Table1[[@],[Column1]]";
        let ref_type = ReferenceType::from_string(reference).unwrap();

        if let ReferenceType::Table(table_ref) = ref_type {
            assert_eq!(table_ref.name, "Table1");

            // Currently our implementation doesn't fully parse complex specifiers,
            // but we should at least verify it's parsed as a table reference
            assert!(table_ref.specifier.is_some());

            // Note: In the future, we should enhance this to properly parse
            // complex structured references and verify the exact specifier
        } else {
            panic!("Expected Table reference");
        }
    }

    #[test]
    fn test_table_multiple_item_specifiers() {
        // Test a table reference with multiple item specifiers
        let reference = "Table1[[#Headers],[#Data]]";
        let ref_type = ReferenceType::from_string(reference).unwrap();

        if let ReferenceType::Table(table_ref) = ref_type {
            assert_eq!(table_ref.name, "Table1");

            // Currently our implementation doesn't fully parse complex specifiers,
            // but we should at least verify it's parsed as a table reference
            assert!(table_ref.specifier.is_some());

            // Note: In the future, we should enhance this to properly parse
            // complex structured references and verify the exact specifier
        } else {
            panic!("Expected Table reference");
        }
    }

    // Note: The following tests are for future functionality and currently only validate
    // that the existing parsing mechanism doesn't break on these formats.

    #[test]
    fn test_table_reference_with_spill() {
        // Test a table reference with spill operator
        // Currently our implementation doesn't support spill operators (#),
        // which is why we're seeing an error. This test confirms the current behavior.
        let formula = "=Table1[#Data]#";
        let tokenizer_result = Tokenizer::new(formula);

        // Verify that the current implementation rejects the spill operator
        assert!(tokenizer_result.is_err());

        // Note: In the future, we should enhance parsing to support spill operators
        // for dynamic array formulas and structured references
    }

    #[test]
    fn test_table_intersection() {
        // Test table intersection reference
        // Currently our implementation doesn't properly handle table intersections,
        // so this test just verifies current behavior
        let formula = "=Table1[@] Table2[#All]";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Just verify the tokenizer doesn't crash
        assert!(!tokenizer.items.is_empty());

        // Note: In the future, this should be enhanced to properly parse
        // table intersections and verify they're handled correctly
    }

    #[test]
    fn structured_combination_roundtrip_prints_nested_brackets() {
        use crate::parser::{ReferenceType, SpecialItem, TableReference, TableSpecifier};
        let s = "Table1[[#Headers],[#Data]]";
        let r = ReferenceType::from_string(s).expect("parse ok");
        // Expect canonical nested-bracket printing
        assert_eq!(r.to_string(), s);
        // Also sanity-check structure
        match r {
            ReferenceType::Table(TableReference {
                name,
                specifier: Some(TableSpecifier::Combination(parts)),
            }) => {
                assert_eq!(name, "Table1");
                assert!(
                    parts
                        .iter()
                        .any(|p| matches!(**p, TableSpecifier::SpecialItem(SpecialItem::Headers)))
                );
                assert!(
                    parts
                        .iter()
                        .any(|p| matches!(**p, TableSpecifier::SpecialItem(SpecialItem::Data)))
                );
            }
            _ => panic!("expected table combination"),
        }
    }

    #[test]
    fn structured_combination_dedupes_duplicate_specials() {
        use crate::parser::{ReferenceType, SpecialItem, TableReference, TableSpecifier};
        // Input with duplicates of specials
        let s = "Table1[[#Data],[#Data],[#Totals],[#Totals]]";
        let r = ReferenceType::from_string(s).expect("parse ok");
        // Our Display prints each special once when building Combination
        // Note: order may follow detection order (#Data, then #Totals)
        assert_eq!(r.to_string(), "Table1[[#Data],[#Totals]]");
        if let ReferenceType::Table(TableReference {
            specifier: Some(TableSpecifier::Combination(parts)),
            ..
        }) = r
        {
            let has_data = parts
                .iter()
                .any(|p| matches!(**p, TableSpecifier::SpecialItem(SpecialItem::Data)));
            let has_totals = parts
                .iter()
                .any(|p| matches!(**p, TableSpecifier::SpecialItem(SpecialItem::Totals)));
            assert!(has_data && has_totals);
            // Ensure duplicates were not kept
            assert_eq!(parts.len(), 2);
        } else {
            panic!("expected table combination");
        }
    }
}

#[cfg(test)]
mod sheet_ref_tests {
    use crate::parser::ReferenceType;
    use formualizer_common::{AxisBound, SheetLocator, SheetRef};

    #[test]
    fn parse_sheet_ref_preserves_abs_flags() {
        let r = ReferenceType::parse_sheet_ref("$A$1").unwrap();
        match r {
            SheetRef::Cell(cell) => {
                assert!(matches!(cell.sheet, SheetLocator::Current));
                assert_eq!(cell.coord.row(), 0);
                assert_eq!(cell.coord.col(), 0);
                assert!(cell.coord.row_abs());
                assert!(cell.coord.col_abs());
            }
            _ => panic!("expected cell"),
        }

        let r = ReferenceType::parse_sheet_ref("Sheet1!A$1").unwrap();
        match r {
            SheetRef::Cell(cell) => {
                assert_eq!(cell.sheet.name(), Some("Sheet1"));
                assert!(cell.coord.row_abs());
                assert!(!cell.coord.col_abs());
            }
            _ => panic!("expected cell"),
        }
    }

    #[test]
    fn parse_sheet_ref_supports_open_ended_ranges() {
        let r = ReferenceType::parse_sheet_ref("$A:$B").unwrap();
        match r {
            SheetRef::Range(range) => {
                assert!(range.start_row.is_none());
                assert!(range.end_row.is_none());
                assert_eq!(range.start_col.unwrap().index, 0);
                assert!(range.start_col.unwrap().abs);
                assert_eq!(range.end_col.unwrap().index, 1);
                assert!(range.end_col.unwrap().abs);
            }
            _ => panic!("expected range"),
        }

        let r = ReferenceType::parse_sheet_ref("1:$3").unwrap();
        match r {
            SheetRef::Range(range) => {
                assert!(range.start_col.is_none());
                assert!(range.end_col.is_none());
                let sr = range.start_row.unwrap();
                let er = range.end_row.unwrap();
                assert_eq!(sr.index, 0);
                assert!(!sr.abs);
                assert_eq!(er.index, 2);
                assert!(er.abs);
            }
            _ => panic!("expected range"),
        }

        let r = ReferenceType::parse_sheet_ref("A1:A").unwrap();
        match r {
            SheetRef::Range(range) => {
                assert_eq!(range.start_row.unwrap().index, 0);
                assert_eq!(range.start_col.unwrap().index, 0);
                assert!(range.end_row.is_none());
                assert_eq!(range.end_col.unwrap().index, 0);
            }
            _ => panic!("expected range"),
        }
    }

    #[test]
    fn parse_sheet_ref_allows_external_workbook_prefix() {
        let r = ReferenceType::parse_sheet_ref("[33]Sheet1!$B:$B").unwrap();
        match r {
            SheetRef::Range(range) => {
                assert_eq!(range.sheet.name(), Some("[33]Sheet1"));
                assert!(range.start_row.is_none());
                assert!(range.end_row.is_none());
                let sc = range.start_col.unwrap();
                let ec = range.end_col.unwrap();
                assert_eq!(sc.index, 1);
                assert!(sc.abs);
                assert_eq!(ec.index, 1);
                assert!(ec.abs);
            }
            _ => panic!("expected range"),
        }
    }

    #[test]
    fn to_sheet_ref_lossy_defaults_to_relative() {
        let rt = ReferenceType::cell(None, 1, 1);
        let sr = rt.to_sheet_ref_lossy().unwrap();
        match sr {
            SheetRef::Cell(cell) => {
                assert!(!cell.coord.row_abs());
                assert!(!cell.coord.col_abs());
                assert!(matches!(cell.sheet, SheetLocator::Current));
            }
            _ => panic!("expected cell"),
        }

        let rt = ReferenceType::range(Some("Sheet1".to_string()), None, Some(1), None, Some(1));
        let sr = rt.to_sheet_ref_lossy().unwrap();
        match sr {
            SheetRef::Range(range) => {
                assert_eq!(range.sheet.name(), Some("Sheet1"));
                assert!(range.start_row.is_none());
                assert_eq!(range.start_col, Some(AxisBound::new(0, false)));
                assert_eq!(range.end_col, Some(AxisBound::new(0, false)));
            }
            _ => panic!("expected range"),
        }
    }
}

#[cfg(test)]
mod semantics_regressions {
    use crate::parser::{ASTNodeType, Parser, ReferenceType};
    use crate::tokenizer::Tokenizer;

    #[test]
    fn exponent_is_right_associative() {
        let t = Tokenizer::new("=2^3^2").unwrap();
        let mut p = Parser::new(t.items, false);
        let ast = p.parse().unwrap();

        match ast.node_type {
            ASTNodeType::BinaryOp { op, left: _, right } => {
                assert_eq!(op, "^");
                // Expected: 2^(3^2)
                match right.node_type {
                    ASTNodeType::BinaryOp { op: op2, .. } => assert_eq!(op2, "^"),
                    other => panic!("expected right child to be exponent, got {other:?}"),
                }
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn unary_minus_binds_less_tightly_than_exponent() {
        let t = Tokenizer::new("=-2^2").unwrap();
        let mut p = Parser::new(t.items, false);
        let ast = p.parse().unwrap();

        // Expected: -(2^2)
        match ast.node_type {
            ASTNodeType::UnaryOp { op, expr } => {
                assert_eq!(op, "-");
                match expr.node_type {
                    ASTNodeType::BinaryOp { op: op2, .. } => assert_eq!(op2, "^"),
                    other => panic!("expected exponent under unary, got {other:?}"),
                }
            }
            other => panic!("expected UnaryOp, got {other:?}"),
        }
    }

    #[test]
    fn quoted_sheet_name_allows_escaped_single_quote() {
        let r = ReferenceType::from_string("'Bob''s Sheet'!A1").unwrap();
        assert_eq!(
            r,
            ReferenceType::cell(Some("Bob's Sheet".to_string()), 1, 1)
        );
    }
}
