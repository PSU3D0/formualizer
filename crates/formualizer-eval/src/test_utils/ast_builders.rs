#![cfg(test)]

use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType};

pub fn make_num_ast(n: f64) -> ASTNode {
    ASTNode::new(ASTNodeType::Literal(LiteralValue::Number(n)), None)
}

pub fn make_int_ast(i: i64) -> ASTNode {
    ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(i)), None)
}

pub fn make_bool_ast(b: bool) -> ASTNode {
    ASTNode::new(ASTNodeType::Literal(LiteralValue::Boolean(b)), None)
}

pub fn make_text_ast(s: &str) -> ASTNode {
    ASTNode::new(
        ASTNodeType::Literal(LiteralValue::Text(s.to_string())),
        None,
    )
}

pub fn make_array_ast(rows: Vec<Vec<ASTNode>>) -> ASTNode {
    ASTNode::new(ASTNodeType::Array(rows), None)
}
