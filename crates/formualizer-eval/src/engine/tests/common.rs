//! Common test helpers
use formualizer_core::parser::{ASTNode, ASTNodeType, ReferenceType};

pub fn create_cell_ref_ast(sheet: Option<&str>, row: u32, col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{}C{}", row, col),
            reference: ReferenceType::Cell {
                sheet: sheet.map(|s| s.to_string()),
                row,
                col,
            },
        },
        source_token: None,
    }
}

pub fn create_binary_op_ast(left: ASTNode, right: ASTNode, op: &str) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: op.to_string(),
            left: Box::new(left),
            right: Box::new(right),
        },
        source_token: None,
    }
}
