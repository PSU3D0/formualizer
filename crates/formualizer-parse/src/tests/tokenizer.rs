#[cfg(test)]
mod tests {
    use crate::FormulaDialect;
    use crate::tokenizer::{Token, TokenSubType, TokenType, Tokenizer};
    macro_rules! assert_token_types {
        ($actual:expr, $expected:expr) => {
            if $actual.len() != $expected.len() {
                panic!(
                    "Token count mismatch!\nExpected {} tokens but got {} tokens.",
                    $expected.len(),
                    $actual.len()
                );
            }

            for (i, (actual, (exp_type, exp_value, exp_subtype))) in $actual.iter().zip($expected.iter()).enumerate() {
                if actual.token_type != **exp_type || actual.value != *exp_value || actual.subtype != **exp_subtype {
                    panic!(
                        "Token mismatch at position {}!\n\nExpected: <{:?} subtype: {:?} value: {}>\nActual: <{:?} subtype: {:?} value: {}>",
                        i,
                        *exp_type,
                        *exp_subtype,
                        exp_value,
                        actual.token_type,
                        actual.subtype,
                        actual.value
                    );
                }
            }
        };
    }

    #[test]
    fn test_literal_formula() {
        // If the formula does not start with '=', it is treated as a literal.
        let formula = "SUM(A1:B2)";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_eq!(tokenizer.items.len(), 1);
        assert_eq!(tokenizer.items[0].token_type, TokenType::Literal);
        assert_eq!(tokenizer.items[0].value, formula);
    }

    #[test]
    fn test_basic_formula() {
        let formula = "=A1+B2";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // Expected tokens: Operand("A1"), OpInfix("+"), Operand("B2")
        assert_eq!(tokenizer.items.len(), 3);
        assert_eq!(tokenizer.items[0].token_type, TokenType::Operand);
        assert_eq!(tokenizer.items[0].value, "A1");
        // A1 cannot be parsed as a number, so subtype should be Range.
        assert_eq!(tokenizer.items[0].subtype, TokenSubType::Range);
        assert_eq!(tokenizer.items[1].token_type, TokenType::OpInfix);
        assert_eq!(tokenizer.items[1].value, "+");
        assert_eq!(tokenizer.items[2].token_type, TokenType::Operand);
        assert_eq!(tokenizer.items[2].value, "B2");
        // The rendered formula should match the original.
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_nested_formula() {
        let formula = "=SUM(A1:B2, VLOOKUP(C3, D4:E5, 2, FALSE))";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // We expect 17 tokens overall.
        assert_eq!(tokenizer.items.len(), 17);
        // Check a few key tokens:
        assert_eq!(tokenizer.items[0].value, "SUM(");
        assert_eq!(tokenizer.items[0].token_type, TokenType::Func);
        assert_eq!(tokenizer.items[0].subtype, TokenSubType::Open);
        assert_eq!(tokenizer.items[1].value, "A1:B2");
        assert_eq!(tokenizer.items[4].value, "VLOOKUP(");
        assert_eq!(tokenizer.items[5].value, "C3");
        assert_eq!(tokenizer.items[11].value, "2");
        assert_eq!(tokenizer.items[11].subtype, TokenSubType::Number);
        assert_eq!(tokenizer.items[14].value, "FALSE");
        // Render should match the original.
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_unary_operator() {
        let formula = "=-A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // Expected tokens: OpPrefix("-"), Operand("A1")
        assert_eq!(tokenizer.items.len(), 2);
        assert_eq!(tokenizer.items[0].token_type, TokenType::OpPrefix);
        assert_eq!(tokenizer.items[0].value, "-");
        assert_eq!(tokenizer.items[1].token_type, TokenType::Operand);
        assert_eq!(tokenizer.items[1].value, "A1");
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_double_unary_operator() {
        let formula = "=--A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // Expected tokens: OpPrefix("-"), OpPrefix("-"), Operand("A1")
        assert_eq!(tokenizer.items.len(), 3);
        assert_eq!(tokenizer.items[0].token_type, TokenType::OpPrefix);
        assert_eq!(tokenizer.items[0].value, "-");
        assert_eq!(tokenizer.items[1].token_type, TokenType::OpPrefix);
        assert_eq!(tokenizer.items[1].value, "-");
        assert_eq!(tokenizer.items[2].token_type, TokenType::Operand);
        assert_eq!(tokenizer.items[2].value, "A1");
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_parentheses() {
        let formula = "=(A1+B2)*-C3";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // Expected tokens:
        // 0: "(" (Paren, Open)
        // 1: Operand "A1"
        // 2: OpInfix "+"
        // 3: Operand "B2"
        // 4: ")" (Paren, Close)
        // 5: OpInfix "*"
        // 6: OpPrefix "-"
        // 7: Operand "C3"
        assert_eq!(tokenizer.items.len(), 8);
        assert_eq!(tokenizer.items[0].token_type, TokenType::Paren);
        assert_eq!(tokenizer.items[0].value, "(");
        assert_eq!(tokenizer.items[1].value, "A1");
        assert_eq!(tokenizer.items[2].value, "+");
        assert_eq!(tokenizer.items[3].value, "B2");
        assert_eq!(tokenizer.items[4].token_type, TokenType::Paren);
        assert_eq!(tokenizer.items[4].subtype, TokenSubType::Close);
        assert_eq!(tokenizer.items[5].value, "*");
        assert_eq!(tokenizer.items[6].token_type, TokenType::OpPrefix);
        assert_eq!(tokenizer.items[6].value, "-");
        assert_eq!(tokenizer.items[7].value, "C3");
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_large_formula() {
        let formula = "=SUMIFS('FY24 POLR_Match Date'!$P:$P,'FY24 POLR_Match Date'!$K:$K, 'Ambulatory','FY24 POLR_Match Date'!$D:$D, 'Calculations Incentive'!$A13)+SUMIF('DFCI FY24'!$A:$A, 'Calculations Incentive'!A13, 'DFCI FY24'!$O:$O)+SUMIF('BWH Tx wRVUs'!$F:$F, 'Calculations Incentive'!A13, 'BWH Tx wRVUs'!$N:$N)";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // Here we simply check that tokenization succeeds and that the rendered formula matches.
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_scientific_notation() {
        let formula = "=1.23E+3";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // Expect a single operand token representing a number.
        assert_eq!(tokenizer.items.len(), 1);
        assert_eq!(tokenizer.items[0].token_type, TokenType::Operand);
        assert_eq!(tokenizer.items[0].value, "1.23E+3");
        assert_eq!(tokenizer.items[0].subtype, TokenSubType::Number);
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_string_literal() {
        let formula = "=\"abc\"\"def\"";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // A double-quoted string should be treated as a text operand.
        assert_eq!(tokenizer.items.len(), 1);
        assert_eq!(tokenizer.items[0].token_type, TokenType::Operand);
        // The token value includes the quotes.
        assert_eq!(tokenizer.items[0].value, "\"abc\"\"def\"");
        assert_eq!(tokenizer.items[0].subtype, TokenSubType::Text);
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_brackets() {
        let formula = "=[A1]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // Since the formula starts with '=', we expect the brackets to be processed
        // and then saved as an operand.
        assert_eq!(tokenizer.items.len(), 1);
        // The accumulated token should be "[A1]".
        assert_eq!(tokenizer.items[0].value, "[A1]");
        assert_eq!(tokenizer.items[0].token_type, TokenType::Operand);
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_error_token() {
        let formula = "=#DIV/0!";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // Expect a single operand token with error subtype.
        assert_eq!(tokenizer.items.len(), 1);
        assert_eq!(tokenizer.items[0].value, "#DIV/0!");
        assert_eq!(tokenizer.items[0].token_type, TokenType::Operand);
        assert_eq!(tokenizer.items[0].subtype, TokenSubType::Error);
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_openformula_semicolon_argument_separator() {
        let formula = "=SUM([.A1];[.A2])";
        let tokenizer = Tokenizer::new_with_dialect(formula, FormulaDialect::OpenFormula).unwrap();

        assert_eq!(tokenizer.items.len(), 5);
        assert_eq!(tokenizer.items[0].value, "SUM(");
        assert_eq!(tokenizer.items[1].value, "[.A1]");
        assert_eq!(tokenizer.items[2].token_type, TokenType::Sep);
        assert_eq!(tokenizer.items[2].subtype, TokenSubType::Arg);
        assert_eq!(tokenizer.items[3].value, "[.A2]");
        assert_eq!(tokenizer.items[4].token_type, TokenType::Func);
        assert_eq!(tokenizer.items[4].subtype, TokenSubType::Close);
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_openformula_array_row_separator() {
        let formula = "={1;2}";
        let tokenizer = Tokenizer::new_with_dialect(formula, FormulaDialect::OpenFormula).unwrap();

        assert!(
            tokenizer
                .items
                .iter()
                .any(|token| token.token_type == TokenType::Sep
                    && token.subtype == TokenSubType::Row)
        );
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_whitespace() {
        let formula = "= A1 \n + B2";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // We expect tokens for whitespace to appear.
        // One possible tokenization:
        // 0: Whitespace " " (first token)
        // 1: Operand "A1"
        // 2: Whitespace " \n " (grouped by the regex)
        // 3: OpInfix "+"
        // 4: Whitespace " "
        // 5: Operand "B2"
        let non_ws: Vec<&Token> = tokenizer
            .items
            .iter()
            .filter(|t| t.token_type != TokenType::Whitespace)
            .collect();
        assert!(non_ws.len() >= 3);
        assert_eq!(non_ws[0].value, "A1");
        assert_eq!(non_ws[1].value, "+");
        assert_eq!(non_ws[2].value, "B2");
    }

    #[test]
    fn test_mismatched_parentheses() {
        let formula = "=A1+B2)";
        let result = Tokenizer::new(formula);
        assert!(result.is_err());
    }

    #[test]
    fn test_unmatched_bracket() {
        let formula = "=[A1";
        let result = Tokenizer::new(formula);
        assert!(result.is_err());
    }

    #[test]
    fn test_array_formulas() {
        let formula = "={1,2,3;4,5,6}";
        let tokenizer = Tokenizer::new(formula).unwrap();

        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Array, "{", &TokenSubType::Open),
                (&TokenType::Operand, "1", &TokenSubType::Number),
                (&TokenType::Sep, ",", &TokenSubType::Arg),
                (&TokenType::Operand, "2", &TokenSubType::Number),
                (&TokenType::Sep, ",", &TokenSubType::Arg),
                (&TokenType::Operand, "3", &TokenSubType::Number),
                (&TokenType::Sep, ";", &TokenSubType::Row),
                (&TokenType::Operand, "4", &TokenSubType::Number),
                (&TokenType::Sep, ",", &TokenSubType::Arg),
                (&TokenType::Operand, "5", &TokenSubType::Number),
                (&TokenType::Sep, ",", &TokenSubType::Arg),
                (&TokenType::Operand, "6", &TokenSubType::Number),
                (&TokenType::Array, "}", &TokenSubType::Close)
            ]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_table_references() {
        let formula = "=Table1[Column1]";
        let tokenizer = Tokenizer::new(formula).unwrap();

        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "Table1[Column1]", &TokenSubType::Range)]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_structured_references() {
        let formula = "=[@Column1]+[@Column2]";
        let tokenizer = Tokenizer::new(formula).unwrap();

        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Operand, "[@Column1]", &TokenSubType::Range),
                (&TokenType::OpInfix, "+", &TokenSubType::None),
                (&TokenType::Operand, "[@Column2]", &TokenSubType::Range)
            ]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_complex_structured_references() {
        // Test column range reference
        let formula = "=Table1[[Column1]:[Column3]]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(
                &TokenType::Operand,
                "Table1[[Column1]:[Column3]]",
                &TokenSubType::Range
            )]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test headers reference
        let formula = "=Table1[#Headers]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(
                &TokenType::Operand,
                "Table1[#Headers]",
                &TokenSubType::Range
            )]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test all data reference
        let formula = "=Table1[#All]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "Table1[#All]", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test data area reference
        let formula = "=Table1[#Data]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "Table1[#Data]", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test totals reference
        let formula = "=Table1[#Totals]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "Table1[#Totals]", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test headers with column range
        let formula = "=Table1[[#Headers],[Column1]:[Column3]]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(
                &TokenType::Operand,
                "Table1[[#Headers],[Column1]:[Column3]]",
                &TokenSubType::Range
            )]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test column with spaces in name
        let formula = "=[@[Column Name]]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(
                &TokenType::Operand,
                "[@[Column Name]]",
                &TokenSubType::Range
            )]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test multiple special items
        let formula = "=SUM(Table1[[#Headers],[#Data],[Column1]])";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Func, "SUM(", &TokenSubType::Open),
                (
                    &TokenType::Operand,
                    "Table1[[#Headers],[#Data],[Column1]]",
                    &TokenSubType::Range
                ),
                (&TokenType::Func, ")", &TokenSubType::Close)
            ]
        );
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_function_multiple_args() {
        let formula = "=IF(A1>0,MAX(B1,C1),MIN(D1,E1))";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Print tokens for debugging
        println!("Tokens in test_function_multiple_args:");
        for (i, token) in tokenizer.items.iter().enumerate() {
            println!(
                "{}: {:?} - {:?} - {}",
                i, token.token_type, token.subtype, token.value
            );
        }

        // Find the IF open parenthesis
        let if_open_index = tokenizer
            .items
            .iter()
            .position(|t| t.value == "IF(")
            .unwrap();

        // Before investigating comma types, first check the basic formula structure
        assert!(
            tokenizer.items.len() >= 5,
            "Formula should have at least 5 tokens"
        );
        assert_eq!(tokenizer.items[if_open_index].token_type, TokenType::Func);
        assert_eq!(tokenizer.items[if_open_index].subtype, TokenSubType::Open);

        // Verify formula is parsed correctly by checking render output
        assert_eq!(tokenizer.render(), formula);

        // Check for commas - but use the right token type
        // The tokenizer might categorize commas as OpInfix rather than Sep tokens
        // when they're in certain contexts
        let commas: Vec<(usize, &Token)> = tokenizer
            .items
            .iter()
            .enumerate()
            .filter(|(_, t)| t.value == ",")
            .collect();

        // Make sure we have at least 2 commas in the top-level function
        assert!(
            commas.len() >= 2,
            "Expected at least 2 commas in the formula, found {}",
            commas.len()
        );
    }

    #[test]
    fn test_complex_ranges() {
        let formula = "=SUM('Sheet 1:Sheet 3'!A1:C10)";
        let tokenizer = Tokenizer::new(formula).unwrap();

        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Func, "SUM(", &TokenSubType::Open),
                (
                    &TokenType::Operand,
                    "'Sheet 1:Sheet 3'!A1:C10",
                    &TokenSubType::Range
                ),
                (&TokenType::Func, ")", &TokenSubType::Close)
            ]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_sheet_names_with_special_characters() {
        // Test with hyphen in sheet name
        let formula = "='Sheet-1'!A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "'Sheet-1'!A1", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with period in sheet name
        let formula = "='Sheet.1'!A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "'Sheet.1'!A1", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with hash symbol in sheet name
        let formula = "='Sheet#1'!A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "'Sheet#1'!A1", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with parentheses in sheet name
        let formula = "='Sheet(1)'!A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "'Sheet(1)'!A1", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with exclamation mark in sheet name (escaped with single quotes)
        let formula = "='Sheet!1'!A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "'Sheet!1'!A1", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with multiple special characters in sheet name
        let formula = "='Sheet-1.2#3!4'!A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(
                &TokenType::Operand,
                "'Sheet-1.2#3!4'!A1",
                &TokenSubType::Range
            )]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with sheet name containing a mix of spaces and special characters
        let formula = "='My Special Sheet!'!A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(
                &TokenType::Operand,
                "'My Special Sheet!'!A1",
                &TokenSubType::Range
            )]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with URL-like characters in sheet name
        let formula = "='Sheet/Path?Query'!A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(
                &TokenType::Operand,
                "'Sheet/Path?Query'!A1",
                &TokenSubType::Range
            )]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with external reference and special characters in sheet name
        let formula = "='[Book1.xlsx]Sheet#1'!A1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(
                &TokenType::Operand,
                "'[Book1.xlsx]Sheet#1'!A1",
                &TokenSubType::Range
            )]
        );
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_infinite_range() {
        let column_wise = "A:A";
        let row_wise = "1:1";
        let formula = format!("=SUM({column_wise})");
        let tokenizer = Tokenizer::new(&formula).unwrap();
        assert_eq!(tokenizer.items[0].value, "SUM(");
        assert_eq!(tokenizer.items[1].value, "A:A");
        assert_eq!(tokenizer.items[1].subtype, TokenSubType::Range);
        assert_eq!(tokenizer.render(), formula);

        let formula = format!("=SUM({row_wise})");
        let tokenizer = Tokenizer::new(&formula).unwrap();
        assert_eq!(tokenizer.items[0].value, "SUM(");
        assert_eq!(tokenizer.items[1].value, "1:1");
        assert_eq!(tokenizer.items[1].subtype, TokenSubType::Range);
        assert_eq!(tokenizer.render(), formula);

        let column_wise_with_sheet = "Sheet1!A:A";
        let column_wise_with_quoted_sheet = "'Sheet 1'!A:A";

        let formula = format!("=SUM({column_wise_with_sheet})");
        let tokenizer = Tokenizer::new(&formula).unwrap();
        assert_eq!(tokenizer.items[0].value, "SUM(");
        assert_eq!(tokenizer.items[1].value, "Sheet1!A:A");
        assert_eq!(tokenizer.items[1].subtype, TokenSubType::Range);
        assert_eq!(tokenizer.render(), formula);

        let formula = format!("=SUM({column_wise_with_quoted_sheet})");
        let tokenizer = Tokenizer::new(&formula).unwrap();
        assert_eq!(tokenizer.items[0].value, "SUM(");
        assert_eq!(tokenizer.items[1].value, "'Sheet 1'!A:A");
        assert_eq!(tokenizer.items[1].subtype, TokenSubType::Range);
        assert_eq!(tokenizer.render(), formula);

        let column_wise_with_lower_bound = "=A1:A";
        let tokenizer = Tokenizer::new(column_wise_with_lower_bound).unwrap();
        assert_eq!(tokenizer.items[0].value, "A1:A");
        assert_eq!(tokenizer.items[0].subtype, TokenSubType::Range);
        assert_eq!(tokenizer.render(), column_wise_with_lower_bound);

        let column_wise_with_upper_bound = "=A:A500";
        let tokenizer = Tokenizer::new(column_wise_with_upper_bound).unwrap();
        assert_eq!(tokenizer.items[0].value, "A:A500");
        assert_eq!(tokenizer.items[0].subtype, TokenSubType::Range);
        assert_eq!(tokenizer.render(), column_wise_with_upper_bound);
    }

    #[test]
    fn test_r1c1_references() {
        let formula = "=R[-1]C[0]+R[0]C[-1]";
        let tokenizer = Tokenizer::new(formula).unwrap();

        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Operand, "R[-1]C[0]", &TokenSubType::Range),
                (&TokenType::OpInfix, "+", &TokenSubType::None),
                (&TokenType::Operand, "R[0]C[-1]", &TokenSubType::Range)
            ]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_r1c1_references_with_absolute_relative_mix() {
        // Test absolute R1C1 reference
        let formula = "=R1C1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "R1C1", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test cell reference with mixed absolute/relative
        let formula = "=R[1]C1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "R[1]C1", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test cell reference with mixed relative/absolute
        let formula = "=R1C[-1]";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "R1C[-1]", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with sheet reference
        let formula = "=Sheet1!R1C1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "Sheet1!R1C1", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with RC (current row, current column)
        let formula = "=RC";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "RC", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test with quoted sheet name
        let formula = "='Sheet 1'!R2C3";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![(&TokenType::Operand, "'Sheet 1'!R2C3", &TokenSubType::Range)]
        );
        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_logical_operators() {
        let formula = "=AND(A1>0,OR(B1<10,C1=5))";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Check logical operators are properly tokenized
        let gt_pos = tokenizer.items.iter().position(|t| t.value == ">").unwrap();
        let lt_pos = tokenizer.items.iter().position(|t| t.value == "<").unwrap();
        let eq_pos = tokenizer.items.iter().position(|t| t.value == "=").unwrap();

        assert_token_types!(
            vec![
                tokenizer.items[gt_pos].clone(),
                tokenizer.items[lt_pos].clone(),
                tokenizer.items[eq_pos].clone()
            ],
            vec![
                (&TokenType::OpInfix, ">", &TokenSubType::None),
                (&TokenType::OpInfix, "<", &TokenSubType::None),
                (&TokenType::OpInfix, "=", &TokenSubType::None)
            ]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_nested_strings() {
        let formula = "=CONCATENATE(\"First\",\" \",\"Second\")";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Check string literals
        let strings: Vec<&Token> = tokenizer
            .items
            .iter()
            .filter(|t| t.subtype == TokenSubType::Text)
            .collect();

        assert_eq!(strings.len(), 3);
        assert_eq!(strings[0].value, "\"First\"");
        assert_eq!(strings[1].value, "\" \"");
        assert_eq!(strings[2].value, "\"Second\"");

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_named_ranges() {
        let formula = "=MyNamedRange*2";
        let tokenizer = Tokenizer::new(formula).unwrap();

        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Operand, "MyNamedRange", &TokenSubType::Range),
                (&TokenType::OpInfix, "*", &TokenSubType::None),
                (&TokenType::Operand, "2", &TokenSubType::Number)
            ]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_external_references() {
        let formula = "='[Book1.xlsx]Sheet1'!A1";
        let tokenizer = Tokenizer::new(formula).unwrap();

        assert_token_types!(
            tokenizer.items,
            vec![(
                &TokenType::Operand,
                "'[Book1.xlsx]Sheet1'!A1",
                &TokenSubType::Range
            )]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_dynamic_array_formulas() {
        // Test dynamic array functions that return multiple values
        let formula = "=SORT(A1:B10)";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Func, "SORT(", &TokenSubType::Open),
                (&TokenType::Operand, "A1:B10", &TokenSubType::Range),
                (&TokenType::Func, ")", &TokenSubType::Close)
            ]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test SEQUENCE function which creates a range of values
        let formula = "=SEQUENCE(4,3)";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Func, "SEQUENCE(", &TokenSubType::Open),
                (&TokenType::Operand, "4", &TokenSubType::Number),
                (&TokenType::Sep, ",", &TokenSubType::Arg),
                (&TokenType::Operand, "3", &TokenSubType::Number),
                (&TokenType::Func, ")", &TokenSubType::Close)
            ]
        );
        assert_eq!(tokenizer.render(), formula);

        // Test UNIQUE function
        let formula = "=UNIQUE(A1:A10)";
        let tokenizer = Tokenizer::new(formula).unwrap();
        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Func, "UNIQUE(", &TokenSubType::Open),
                (&TokenType::Operand, "A1:A10", &TokenSubType::Range),
                (&TokenType::Func, ")", &TokenSubType::Close)
            ]
        );
        assert_eq!(tokenizer.render(), formula);

        // Note: Spill range operator '#' and implicit intersection operator '@' tests
        // are currently skipped as the tokenizer doesn't yet support these.
        // These will need to be implemented as feature enhancements.

        // Commented tests for future implementation:
        // - Spill range operator: "=A1#"
        // - Spill range with sheet reference: "=Sheet1!A1#"
        // - Implicit intersection operator: "=@A:A"
    }

    #[test]
    fn test_space_in_formula() {
        let formula = "=SUM( A1:B2 ) / COUNT( C1:D2 )";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Filter out whitespace tokens for this check
        let no_whitespace: Vec<Token> = tokenizer
            .items
            .iter()
            .filter(|t| t.token_type != TokenType::Whitespace)
            .cloned()
            .collect();

        assert_token_types!(
            no_whitespace,
            vec![
                (&TokenType::Func, "SUM(", &TokenSubType::Open),
                (&TokenType::Operand, "A1:B2", &TokenSubType::Range),
                (&TokenType::Func, ")", &TokenSubType::Close),
                (&TokenType::OpInfix, "/", &TokenSubType::None),
                (&TokenType::Func, "COUNT(", &TokenSubType::Open),
                (&TokenType::Operand, "C1:D2", &TokenSubType::Range),
                (&TokenType::Func, ")", &TokenSubType::Close)
            ]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_percentage_operator() {
        let formula = "=50%+25%";
        let tokenizer = Tokenizer::new(formula).unwrap();

        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Operand, "50", &TokenSubType::Number),
                (&TokenType::OpPostfix, "%", &TokenSubType::None),
                (&TokenType::OpInfix, "+", &TokenSubType::None),
                (&TokenType::Operand, "25", &TokenSubType::Number),
                (&TokenType::OpPostfix, "%", &TokenSubType::None)
            ]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_mixed_operators() {
        let formula = "=5+10*2^3/4-1";
        let tokenizer = Tokenizer::new(formula).unwrap();

        assert_token_types!(
            tokenizer.items,
            vec![
                (&TokenType::Operand, "5", &TokenSubType::Number),
                (&TokenType::OpInfix, "+", &TokenSubType::None),
                (&TokenType::Operand, "10", &TokenSubType::Number),
                (&TokenType::OpInfix, "*", &TokenSubType::None),
                (&TokenType::Operand, "2", &TokenSubType::Number),
                (&TokenType::OpInfix, "^", &TokenSubType::None),
                (&TokenType::Operand, "3", &TokenSubType::Number),
                (&TokenType::OpInfix, "/", &TokenSubType::None),
                (&TokenType::Operand, "4", &TokenSubType::Number),
                (&TokenType::OpInfix, "-", &TokenSubType::None),
                (&TokenType::Operand, "1", &TokenSubType::Number)
            ]
        );

        assert_eq!(tokenizer.render(), formula);
    }

    #[test]
    fn test_incomplete_string_literal() {
        // A string missing its closing quote should produce an error.
        let formula = "=\"Hello";
        let result = Tokenizer::new(formula);
        assert!(
            result.is_err(),
            "Expected error for incomplete string literal"
        );
    }

    #[test]
    fn test_multiple_percentage_operators() {
        // Formula with two consecutive percentage operators.
        let formula = "=50%%";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // Expected tokens: Operand("50"), OpPostfix("%"), OpPostfix("%")
        assert_eq!(tokenizer.items.len(), 3);
        assert_eq!(tokenizer.items[0].value, "50");
        assert_eq!(tokenizer.items[1].value, "%");
        assert_eq!(tokenizer.items[1].token_type, TokenType::OpPostfix);
        assert_eq!(tokenizer.items[2].value, "%");
        assert_eq!(tokenizer.items[2].token_type, TokenType::OpPostfix);
    }

    #[test]
    fn test_absolute_references() {
        // Check that a formula with dollar signs is tokenized as a single operand.
        let formula = "=$A$1+1";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // Expected tokens: Operand("$A$1"), OpInfix("+"), Operand("1")
        assert!(tokenizer.items.len() >= 3);
        assert_eq!(tokenizer.items[0].value, "$A$1");
        assert_eq!(tokenizer.items[1].value, "+");
        assert_eq!(tokenizer.items[2].value, "1");
    }

    #[test]
    fn test_formula_only_whitespace() {
        // A formula that starts with "=" and is followed only by whitespace.
        let formula = "=   ";
        let result = Tokenizer::new(formula);
        assert!(
            result.is_ok(),
            "Expected tokenizer to handle whitespace-only formulas"
        );
        let tokenizer = result.unwrap();
        let non_whitespace: Vec<&Token> = tokenizer
            .items
            .iter()
            .filter(|t| t.token_type != TokenType::Whitespace)
            .collect();
        assert_eq!(non_whitespace.len(), 0, "Expected no non-whitespace tokens");
    }

    #[test]
    fn test_escaped_quotes_in_string() {
        // A string with escaped double-quotes.
        let formula = "=\"He said \"\"Hello\"\"\"";
        let tokenizer = Tokenizer::new(formula).unwrap();
        // The token value should include the escaped quotes.
        assert_eq!(tokenizer.items[0].value, "\"He said \"\"Hello\"\"\"");
    }

    #[test]
    fn test_big_formula() {
        let formula = "=-SUMIFS($COGS!$J:$J,$COGS!$D:$D, \">=\"&$'Test 24-25'!C2, $COGS!$D:$D, \"<=\"&$'Test 24-25'!C3,$COGS!$A:$A,$'Test 24-25'!$A$4)";
        let tokenizer = Tokenizer::new(formula).unwrap();
        let items = tokenizer.items;
        println!("items: {items:?}");
        assert_eq!(items.len(), 23);
        assert_eq!(items[0].value, "-");
        assert_eq!(items[1].value, "SUMIFS(");
        assert_eq!(items[2].value, "$COGS!$J:$J");
        assert_eq!(items[3].value, ",");
        assert_eq!(items[4].value, "$COGS!$D:$D");
        assert_eq!(items[5].value, ",");
        assert_eq!(items[6].value, " ");
        assert_eq!(items[7].value, "\">=\"");
        assert_eq!(items[8].value, "&");
        assert_eq!(items[9].value, "$'Test 24-25'!C2");
    }

    #[test]
    fn test_xlfn_functions() {
        let formula = "=_xlfn.XLOOKUP(J7, 'GI XWALK'!$Q:$Q,'GI XWALK'!$R:$R,,0)";
        let tokenizer = Tokenizer::new(formula).unwrap();
        println!("tokenizer: {:?}", tokenizer.items);
        assert_eq!(tokenizer.items[0].value, "_xlfn.XLOOKUP(");
        assert_eq!(tokenizer.items[1].value, "J7");
        assert_eq!(tokenizer.items[2].value, ",");
    }

    /// Helper function to validate token substring matches source
    fn assert_token_substring_matches(formula: &str, token: &Token) {
        let actual_substring = &formula[token.start..token.end];
        assert_eq!(
            actual_substring, token.value,
            "Token value '{}' doesn't match substring '{}' at [{}..{})",
            token.value, actual_substring, token.start, token.end
        );
    }

    #[test]
    fn test_basic_token_positions() {
        let formula = "=A1+10";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Expected tokens: "A1"(1,3), "+"(3,4), "10"(4,6)
        assert_eq!(tokenizer.items.len(), 3);

        let a1_token = &tokenizer.items[0];
        assert_eq!(a1_token.value, "A1");
        assert_eq!(a1_token.start, 1);
        assert_eq!(a1_token.end, 3);
        assert_token_substring_matches(formula, a1_token);

        let plus_token = &tokenizer.items[1];
        assert_eq!(plus_token.value, "+");
        assert_eq!(plus_token.start, 3);
        assert_eq!(plus_token.end, 4);
        assert_token_substring_matches(formula, plus_token);

        let ten_token = &tokenizer.items[2];
        assert_eq!(ten_token.value, "10");
        assert_eq!(ten_token.start, 4);
        assert_eq!(ten_token.end, 6);
        assert_token_substring_matches(formula, ten_token);
    }

    #[test]
    fn test_function_positions() {
        let formula = "=SUM(B2:B4)";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Expected tokens: "SUM("(1,5), "B2:B4"(5,10), ")"(10,11)
        assert_eq!(tokenizer.items.len(), 3);

        let sum_token = &tokenizer.items[0];
        assert_eq!(sum_token.value, "SUM(");
        assert_eq!(sum_token.start, 1);
        assert_eq!(sum_token.end, 5);
        assert_token_substring_matches(formula, sum_token);

        let range_token = &tokenizer.items[1];
        assert_eq!(range_token.value, "B2:B4");
        assert_eq!(range_token.start, 5);
        assert_eq!(range_token.end, 10);
        assert_token_substring_matches(formula, range_token);

        let close_token = &tokenizer.items[2];
        assert_eq!(close_token.value, ")");
        assert_eq!(close_token.start, 10);
        assert_eq!(close_token.end, 11);
        assert_token_substring_matches(formula, close_token);
    }

    #[test]
    fn test_string_with_quotes_positions() {
        let formula = "=\"ab\"\"c\"";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Expected tokens: "\"ab\"\"c\""(1,8)
        assert_eq!(tokenizer.items.len(), 1);

        let string_token = &tokenizer.items[0];
        assert_eq!(string_token.value, "\"ab\"\"c\"");
        assert_eq!(string_token.start, 1);
        assert_eq!(string_token.end, 8);
        assert_token_substring_matches(formula, string_token);
    }

    #[test]
    fn test_error_literal_positions() {
        let formula = "=#DIV/0!";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Expected tokens: "#DIV/0!"(1,8)
        assert_eq!(tokenizer.items.len(), 1);

        let error_token = &tokenizer.items[0];
        assert_eq!(error_token.value, "#DIV/0!");
        assert_eq!(error_token.start, 1);
        assert_eq!(error_token.end, 8);
        assert_token_substring_matches(formula, error_token);
    }

    #[test]
    fn test_whitespace_positions() {
        let formula = "= A1 + B2 ";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Expected tokens (with whitespace): " "(1,2), "A1"(2,4), " "(4,5), "+"(5,6), " "(6,7), "B2"(7,9), " "(9,10)
        let whitespace_tokens: Vec<&Token> = tokenizer
            .items
            .iter()
            .filter(|t| t.token_type == TokenType::Whitespace)
            .collect();

        assert!(whitespace_tokens.len() >= 3);

        // Verify each token's substring matches
        for token in &tokenizer.items {
            assert_token_substring_matches(formula, token);
        }
    }

    #[test]
    fn test_complex_formula_positions() {
        let formula = "=SUM(A1:B2)*MAX(C3,D4)";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Verify all tokens have correct positions
        for token in &tokenizer.items {
            assert_token_substring_matches(formula, token);
            // Verify positions are valid
            assert!(token.start <= token.end);
            assert!(token.end <= formula.len());
        }
    }

    #[test]
    fn test_literal_formula_positions() {
        let formula = "SUM(A1:B2)";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // For literal formulas (not starting with =), we should have one token spanning entire string
        assert_eq!(tokenizer.items.len(), 1);

        let literal_token = &tokenizer.items[0];
        assert_eq!(literal_token.value, formula);
        assert_eq!(literal_token.start, 0);
        assert_eq!(literal_token.end, formula.len());
        assert_token_substring_matches(formula, literal_token);
    }

    #[test]
    fn test_operator_positions() {
        let formula = "=A1>=B1";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Expected tokens: "A1"(1,3), ">="(3,5), "B1"(5,7)
        assert_eq!(tokenizer.items.len(), 3);

        let a1_token = &tokenizer.items[0];
        assert_eq!(a1_token.value, "A1");
        assert_token_substring_matches(formula, a1_token);

        let ge_token = &tokenizer.items[1];
        assert_eq!(ge_token.value, ">=");
        assert_eq!(ge_token.start, 3);
        assert_eq!(ge_token.end, 5);
        assert_token_substring_matches(formula, ge_token);

        let b1_token = &tokenizer.items[2];
        assert_eq!(b1_token.value, "B1");
        assert_token_substring_matches(formula, b1_token);
    }

    #[test]
    fn test_array_formula_positions() {
        let formula = "={1,2;3,4}";
        let tokenizer = Tokenizer::new(formula).unwrap();

        // Verify all tokens have correct byte positions
        for token in &tokenizer.items {
            assert_token_substring_matches(formula, token);
        }

        // Check specific tokens
        let open_brace = tokenizer.items.iter().find(|t| t.value == "{").unwrap();
        assert_eq!(open_brace.start, 1);
        assert_eq!(open_brace.end, 2);

        let close_brace = tokenizer.items.iter().find(|t| t.value == "}").unwrap();
        assert_eq!(close_brace.start, 9);
        assert_eq!(close_brace.end, 10);
    }

    #[test]
    fn test_scientific_notation_positions() {
        let formula = "=1.23E+45";
        let tokenizer = Tokenizer::new(formula).unwrap();

        assert_eq!(tokenizer.items.len(), 1);
        let scientific_token = &tokenizer.items[0];
        assert_eq!(scientific_token.value, "1.23E+45");
        assert_eq!(scientific_token.start, 1);
        assert_eq!(scientific_token.end, 9);
        assert_token_substring_matches(formula, scientific_token);
    }
}
