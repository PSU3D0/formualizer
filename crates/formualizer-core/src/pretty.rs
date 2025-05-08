use crate::parser::{ASTNode, ASTNodeType, Parser, ParserError};
use crate::tokenizer::Tokenizer;

/// Pretty-prints an AST node according to canonical formatting rules.
///
/// Rules:
/// - All functions upper-case, no spaces before '('
/// - Commas followed by single space; no space before ','
/// - Binary operators surrounded by single spaces
/// - No superfluous parentheses (keeps semantics)
/// - References printed via .normalise()
/// - Array literals: {1, 2; 3, 4}
pub fn pretty_print(ast: &ASTNode) -> String {
    match &ast.node_type {
        ASTNodeType::Literal(value) => {
            format!("{}", value)
        }
        ASTNodeType::Reference { reference, .. } => reference.normalise(),
        ASTNodeType::UnaryOp { op, expr } => {
            format!("{}{}", op, pretty_print(expr))
        }
        ASTNodeType::BinaryOp { op, left, right } => {
            // Special handling for range operator ':'
            if op == ":" {
                format!("{}:{}", pretty_print(left), pretty_print(right))
            } else {
                format!("{} {} {}", pretty_print(left), op, pretty_print(right))
            }
        }
        ASTNodeType::Function { name, args } => {
            let args_str = args
                .iter()
                .map(pretty_print)
                .collect::<Vec<String>>()
                .join(", ");

            format!("{}({})", name.to_uppercase(), args_str)
        }
        ASTNodeType::Array(rows) => {
            let rows_str = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(pretty_print)
                        .collect::<Vec<String>>()
                        .join(", ")
                })
                .collect::<Vec<String>>()
                .join("; ");

            format!("{{{}}}", rows_str)
        }
    }
}

/// Tokenizes and parses a formula, then pretty-prints it.
///
/// Returns a Result with the pretty-printed formula or a parser error.
pub fn pretty_parse_render(formula: &str) -> Result<String, ParserError> {
    // Handle empty formula case
    if formula.is_empty() {
        return Ok(String::new());
    }

    // If formula doesn't start with '=', add it before parsing and remove it after
    let needs_equals = !formula.starts_with('=');
    let formula_to_parse = if needs_equals {
        format!("={}", formula)
    } else {
        formula.to_string()
    };

    // Tokenize, parse, and pretty-print
    let tokenizer = match Tokenizer::new(&formula_to_parse) {
        Ok(t) => t,
        Err(e) => {
            return Err(ParserError {
                message: format!("Tokenizer error: {}", e.message),
                position: None,
            });
        }
    };

    let mut parser = Parser::new(tokenizer.items, false);
    let ast = parser.parse()?;

    // Format the result with '=' prefix
    let pretty_printed = pretty_print(&ast);

    // Return the result with appropriate '=' prefix
    if needs_equals {
        Ok(pretty_printed)
    } else {
        Ok(format!("={}", pretty_printed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pretty_print_validation() {
        let original = "= sum(  a1 ,2 ) ";
        let pretty = pretty_parse_render(original).unwrap();
        assert_eq!(pretty, "=SUM(A1, 2)");

        let round = pretty_parse_render(&pretty).unwrap();
        assert_eq!(pretty, round); // idempotent
    }

    #[test]
    fn test_ast_canonicalization() {
        // Test that our pretty printer produces canonical form
        let formula = "=sum(  a1, b2  )";
        let pretty = pretty_parse_render(formula).unwrap();

        // Check that the pretty printed version is canonicalized
        assert_eq!(pretty, "=SUM(A1, B2)");

        // Test round-trip consistency
        let repretty = pretty_parse_render(&pretty).unwrap();
        assert_eq!(pretty, repretty);
    }

    #[test]
    fn test_pretty_print_operators() {
        let formula = "=a1+b2*3";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "=A1 + B2 * 3");

        let formula = "=a1 + b2 *     3";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "=A1 + B2 * 3");
    }

    #[test]
    fn test_pretty_print_function_nesting() {
        let formula = "=if(a1>0, sum(b1:b10), average(c1:c10))";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "=IF(A1 > 0, SUM(B1:B10), AVERAGE(C1:C10))");
    }

    #[test]
    fn test_pretty_print_arrays() {
        let formula = "={1,2;3,4}";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "={1, 2; 3, 4}");

        let formula = "={1, 2; 3, 4}";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "={1, 2; 3, 4}");
    }

    #[test]
    fn test_pretty_print_references() {
        let formula = "=Sheet1!$a$1:$b$2";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "=Sheet1!A1:B2");

        let formula = "='My Sheet'!a1";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "='My Sheet'!A1");
    }
}
