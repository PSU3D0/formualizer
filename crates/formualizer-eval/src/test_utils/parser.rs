#![cfg(test)]

use formualizer_parse::parser::ASTNode;

pub fn parse_ast(src: &str) -> ASTNode {
    formualizer_parse::parse(src).expect("failed to parse formula")
}
