//! Common test helpers
use crate::engine::{Vertex, VertexId};
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

/// Helper to find a vertex by ID in the vertices array
/// Since Vertex doesn't store its ID, we need to reconstruct based on position
pub fn find_vertex_by_id<'a>(vertices: &'a [Vertex], id: VertexId) -> Option<&'a Vertex> {
    // Vertices are stored sequentially starting from FIRST_NORMAL_VERTEX
    use crate::engine::vertex_store::FIRST_NORMAL_VERTEX;
    let index = id.0.checked_sub(FIRST_NORMAL_VERTEX)? as usize;
    vertices.get(index)
}

/// Helper to get vertex at position, asserting it exists
pub fn get_vertex_at<'a>(vertices: &'a [Vertex], index: usize) -> &'a Vertex {
    vertices.get(index).expect(&format!(
        "Expected vertex at index {} but vertices only has {} elements",
        index,
        vertices.len()
    ))
}

/// Helper to get all vertex IDs from a graph in creation order
pub fn get_vertex_ids_in_order(graph: &crate::engine::DependencyGraph) -> Vec<VertexId> {
    let mut vertex_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    vertex_ids.sort(); // VertexIds are created sequentially, so sorting gives creation order
    vertex_ids
}
