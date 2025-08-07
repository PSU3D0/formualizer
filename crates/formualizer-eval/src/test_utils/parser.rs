#![cfg(test)]

use formualizer_core::parser::ASTNode;

pub fn parse_ast(src: &str) -> ASTNode {
    formualizer_core::parse(src).expect("failed to parse formula")
}
