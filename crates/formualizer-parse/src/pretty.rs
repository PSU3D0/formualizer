use crate::parser::{ASTNode, ASTNodeType, ParserError, parse};
use crate::tokenizer::Associativity;

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
    pretty_print_node(ast)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

fn infix_info(op: &str) -> (u8, Associativity) {
    match op {
        ":" | " " | "," => (8, Associativity::Left),
        "^" => (6, Associativity::Right),
        "*" | "/" => (4, Associativity::Left),
        "+" | "-" => (3, Associativity::Left),
        "&" => (2, Associativity::Left),
        "=" | "<" | ">" | "<=" | ">=" | "<>" => (1, Associativity::Left),
        _ => (0, Associativity::Left),
    }
}

fn unary_precedence(op: &str) -> u8 {
    if op == "%" {
        7
    } else {
        // Prefix unary.
        5
    }
}

fn node_precedence(ast: &ASTNode) -> u8 {
    match &ast.node_type {
        ASTNodeType::BinaryOp { op, .. } => infix_info(op).0,
        ASTNodeType::UnaryOp { op, .. } => unary_precedence(op),
        // Treat everything else as an atom.
        _ => 9,
    }
}

fn child_needs_parens(
    child: &ASTNode,
    parent_op: &str,
    parent_prec: u8,
    parent_assoc: Associativity,
    side: Side,
) -> bool {
    let child_prec = node_precedence(child);
    if child_prec < parent_prec {
        return true;
    }
    if child_prec > parent_prec {
        return false;
    }

    // Same precedence: associativity and mixed operators matter.
    match side {
        Side::Left => {
            if parent_assoc == Associativity::Right {
                // Right-assoc ops (e.g. '^'): parenthesize left child if it could re-associate.
                matches!(child.node_type, ASTNodeType::BinaryOp { .. })
            } else {
                false
            }
        }
        Side::Right => {
            if parent_assoc == Associativity::Left {
                if let ASTNodeType::BinaryOp { op: child_op, .. } = &child.node_type {
                    if child_op != parent_op {
                        return true;
                    }

                    // Even with same op, some operators are not associative.
                    if parent_op == "-" || parent_op == "/" {
                        return true;
                    }
                }
                false
            } else {
                // Right-assoc ops: parenthesize if mixing ops at same precedence.
                if let ASTNodeType::BinaryOp { op: child_op, .. } = &child.node_type {
                    return child_op != parent_op;
                }
                false
            }
        }
    }
}

fn unary_operand_needs_parens(unary_op: &str, operand: &ASTNode) -> bool {
    match unary_op {
        "%" => matches!(operand.node_type, ASTNodeType::BinaryOp { .. }),
        _ => {
            let operand_prec = node_precedence(operand);
            operand_prec < unary_precedence(unary_op)
                && matches!(operand.node_type, ASTNodeType::BinaryOp { .. })
        }
    }
}

fn pretty_child(
    child: &ASTNode,
    parent_op: &str,
    parent_prec: u8,
    parent_assoc: Associativity,
    side: Side,
) -> String {
    let s = pretty_print_node(child);
    if child_needs_parens(child, parent_op, parent_prec, parent_assoc, side) {
        format!("({s})")
    } else {
        s
    }
}

fn pretty_print_node(ast: &ASTNode) -> String {
    match &ast.node_type {
        ASTNodeType::Literal(value) => match value {
            // Quote and escape text literals to preserve Excel semantics
            crate::LiteralValue::Text(s) => {
                let escaped = s.replace('"', "\"\"");
                format!("\"{escaped}\"")
            }
            _ => format!("{value}"),
        },
        ASTNodeType::Reference { reference, .. } => reference.normalise(),
        ASTNodeType::UnaryOp { op, expr } => {
            let inner = pretty_print_node(expr);
            let inner = if unary_operand_needs_parens(op, expr) {
                format!("({inner})")
            } else {
                inner
            };

            if op == "%" {
                format!("{inner}%")
            } else {
                format!("{op}{inner}")
            }
        }
        ASTNodeType::BinaryOp { op, left, right } => {
            let (prec, assoc) = infix_info(op);
            let left_s = pretty_child(left, op, prec, assoc, Side::Left);
            let right_s = pretty_child(right, op, prec, assoc, Side::Right);

            // Special handling for range operator ':'
            if op == ":" {
                format!("{left_s}:{right_s}")
            } else {
                format!("{left_s} {op} {right_s}")
            }
        }
        ASTNodeType::Function { name, args } => {
            let args_str = args
                .iter()
                .map(pretty_print_node)
                .collect::<Vec<String>>()
                .join(", ");

            format!("{}({})", name.to_uppercase(), args_str)
        }
        ASTNodeType::Array(rows) => {
            let rows_str = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(pretty_print_node)
                        .collect::<Vec<String>>()
                        .join(", ")
                })
                .collect::<Vec<String>>()
                .join("; ");

            format!("{{{rows_str}}}")
        }
    }
}

/// Produce a canonical Excel formula string for an AST, prefixed with '='.
///
/// This is the single entry-point that UI layers should use when displaying
/// a formula reconstructed from an AST.
pub fn canonical_formula(ast: &ASTNode) -> String {
    format!("={}", pretty_print(ast))
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
        format!("={formula}")
    } else {
        formula.to_string()
    };

    // Parse and pretty-print
    let ast = parse(&formula_to_parse)?;

    // Format the result with '=' prefix
    let pretty_printed = pretty_print(&ast);

    // Return the result with appropriate '=' prefix
    if needs_equals {
        Ok(pretty_printed)
    } else {
        Ok(format!("={pretty_printed}"))
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
    fn test_pretty_print_inserts_parentheses_when_needed() {
        let formula = "=(a1+b2)*c3";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "=(A1 + B2) * C3");
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
        assert_eq!(pretty, "=Sheet1!$A$1:$B$2");

        let formula = "='My Sheet'!a1";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "='My Sheet'!A1");
    }

    #[test]
    fn test_pretty_print_text_literals_in_functions() {
        // Should preserve quotes around text literals
        let formula = "=SUMIFS(A:A, B:B, \"*Parking*\")";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "=SUMIFS(A:A, B:B, \"*Parking*\")");
    }

    #[test]
    fn test_pretty_print_text_concatenation_and_escaping() {
        // Operators as text must stay quoted, and spacing around '&' is canonical
        let formula = "=\">=\"&DATE(2024,1,1)";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "=\">=\" & DATE(2024, 1, 1)");

        // Embedded quotes should be doubled
        let formula = "=\"He said \"\"Hi\"\"\"";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "=\"He said \"\"Hi\"\"\"");
    }

    #[test]
    fn test_pretty_print_text_in_arrays() {
        let formula = "={\"A\", \"B\"; \"C\", \"D\"}";
        let pretty = pretty_parse_render(formula).unwrap();
        assert_eq!(pretty, "={\"A\", \"B\"; \"C\", \"D\"}");
    }
}
