//! Common test helpers
use crate::engine::{EvalConfig, VertexId};
use crate::{CellRef, Coord, SheetId};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

pub fn create_cell_ref_ast(sheet: Option<&str>, row: u32, col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{row}C{col}"),
            reference: ReferenceType::cell(sheet.map(|s| s.to_string()), row, col),
        },
        source_token: None,
        contains_volatile: false,
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
        contains_volatile: false,
    }
}

/// Build an absolute cell ref from Excel 1-based coords.
pub fn abs_cell_ref(sheet_id: SheetId, row: u32, col: u32) -> CellRef {
    CellRef::new(sheet_id, Coord::from_excel(row, col, true, true))
}

/// Helper to get all vertex IDs from a graph in creation order
pub fn get_vertex_ids_in_order(graph: &crate::engine::DependencyGraph) -> Vec<VertexId> {
    let mut vertex_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    vertex_ids.sort(); // VertexIds are created sequentially, so sorting gives creation order
    vertex_ids
}

pub fn arrow_eval_config() -> EvalConfig {
    EvalConfig {
        arrow_storage_enabled: true,
        delta_overlay_enabled: true,
        write_formula_overlay_enabled: true,
        ..Default::default()
    }
}

pub fn eval_config_with_range_limit(limit: usize) -> EvalConfig {
    EvalConfig::default().with_range_expansion_limit(limit)
}
