use super::common::{abs_cell_ref, get_vertex_ids_in_order};
use crate::engine::{DependencyGraph, Scheduler, VertexId};
use formualizer_common::{LiteralValue, parse_a1_1based};
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

/// Helper to create a cell reference AST node
fn create_cell_ref_ast(sheet: Option<&str>, row: u32, col: u32, original: &str) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: original.to_string(),
            reference: ReferenceType::cell(sheet.map(|s| s.to_string()), row, col),
        },
        source_token: None,
        contains_volatile: false,
    }
}

/// Helper to create a binary operation AST (A + B, etc.)
fn create_binary_op_ast(left_ref: &str, right_ref: &str, op: &str) -> ASTNode {
    let (left_row, left_col) = parse_cell_ref(left_ref);

    let right_node = if let Ok(num) = right_ref.parse::<i64>() {
        ASTNode {
            node_type: ASTNodeType::Literal(LiteralValue::Int(num)),
            source_token: None,
            contains_volatile: false,
        }
    } else {
        let (right_row, right_col) = parse_cell_ref(right_ref);
        create_cell_ref_ast(None, right_row, right_col, right_ref)
    };

    ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: op.to_string(),
            left: Box::new(create_cell_ref_ast(None, left_row, left_col, left_ref)),
            right: Box::new(right_node),
        },
        source_token: None,
        contains_volatile: false,
    }
}

/// Helper to parse "A1" -> (1, 1), "B2" -> (2, 2), etc.
fn parse_cell_ref(cell_ref: &str) -> (u32, u32) {
    parse_a1_1based(cell_ref)
        .map(|(row, col, _, _)| (row, col))
        .unwrap_or((1, 1))
}

/// Create a test graph with realistic formulas and return vertex IDs by cell reference
fn create_test_graph_with_formulas()
-> (DependencyGraph, std::collections::HashMap<String, VertexId>) {
    let mut graph = DependencyGraph::new();

    // Create some base values
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap(); // A1 = 10
    graph
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Int(20))
        .unwrap(); // B1 = 20
    graph
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Int(30))
        .unwrap(); // C1 = 30

    // Create formulas
    // A2 = A1 * 2
    let a2_ast = create_binary_op_ast("A1", "2", "*");
    graph.set_cell_formula("Sheet1", 2, 1, a2_ast).unwrap();

    // B2 = A2 + B1
    let b2_ast = create_binary_op_ast("A2", "B1", "+");
    graph.set_cell_formula("Sheet1", 2, 2, b2_ast).unwrap();

    // C2 = B2 + C1
    let c2_ast = create_binary_op_ast("B2", "C1", "+");
    graph.set_cell_formula("Sheet1", 2, 3, c2_ast).unwrap();

    // Build cell mapping
    let mut cell_map = std::collections::HashMap::new();
    for (addr, &vertex_id) in graph.cell_to_vertex() {
        let cell_ref = format!(
            "{}{}",
            char::from_u32('A' as u32 + addr.coord.col() - 1).unwrap(),
            addr.coord.row()
        );
        cell_map.insert(cell_ref, vertex_id);
    }

    (graph, cell_map)
}

#[test]
fn test_tarjan_simple_graph() {
    let (graph, cell_map) = create_test_graph_with_formulas();
    let scheduler = Scheduler::new(&graph);

    // Get all vertices for SCC analysis
    let all_vertices: Vec<VertexId> = cell_map.values().copied().collect();

    // Run Tarjan's algorithm
    let sccs = scheduler.tarjan_scc(&all_vertices).unwrap();

    // In a simple acyclic graph, each vertex should be its own SCC
    assert_eq!(sccs.len(), all_vertices.len());

    // Each SCC should contain exactly one vertex
    for scc in &sccs {
        assert_eq!(scc.len(), 1);
    }

    // Verify all vertices are included exactly once
    let mut found_vertices = std::collections::HashSet::new();
    for scc in &sccs {
        for &vertex_id in scc {
            assert!(
                found_vertices.insert(vertex_id),
                "Vertex {vertex_id:?} found multiple times"
            );
        }
    }
    assert_eq!(found_vertices.len(), all_vertices.len());
}

#[test]
fn test_tarjan_cycle_detection() {
    let mut graph = DependencyGraph::new();

    // Create a cycle: A1 → B1 → C1 → A1
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
        .unwrap(); // A1 starts as value

    // A1 = B1 + 1
    let a1_ast = create_binary_op_ast("B1", "1", "+");
    graph.set_cell_formula("Sheet1", 1, 1, a1_ast).unwrap(); // Now A1 is a formula

    // B1 = C1 * 2
    let b1_ast = create_binary_op_ast("C1", "2", "*");
    graph.set_cell_formula("Sheet1", 1, 2, b1_ast).unwrap();

    // C1 = A1 - 1 (closes the cycle)
    let c1_ast = create_binary_op_ast("A1", "1", "-");
    graph.set_cell_formula("Sheet1", 1, 3, c1_ast).unwrap();

    let scheduler = Scheduler::new(&graph);

    // Get all vertices from the graph (should be 3: A1, B1, C1)
    let all_vertices: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    assert_eq!(all_vertices.len(), 3, "Expected 3 vertices in the graph");

    let sccs = scheduler.tarjan_scc(&all_vertices).unwrap();

    // Should find one SCC containing all three vertices
    assert_eq!(sccs.len(), 1);
    assert_eq!(sccs[0].len(), 3);

    // Verify all three vertices are in the same SCC
    let scc_set: std::collections::HashSet<VertexId> = sccs[0].iter().copied().collect();
    for &vertex_id in &all_vertices {
        assert!(
            scc_set.contains(&vertex_id),
            "Vertex {vertex_id:?} not found in cycle SCC"
        );
    }
}

#[test]
fn test_tarjan_self_loops() {
    let mut graph = DependencyGraph::new();

    // Create A1 = 5 (no cycle)
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(5))
        .unwrap();

    // Create a self-referencing formula B1 = B1.
    // The immediate check in `set_cell_formula` will catch this.
    let b1_ast = create_cell_ref_ast(None, 1, 2, "B1");
    let result = graph.set_cell_formula("Sheet1", 1, 2, b1_ast);
    assert!(result.is_err()); // Should be caught by immediate check

    // To test Tarjan's ability to handle self-loops, we need to construct it manually
    // or create a more complex cycle that bypasses the immediate check.
    // Let's create C1 = D1 and D1 = C1, which is a 2-node cycle, but Tarjan should still
    // correctly identify the components.

    let c1_ast = create_cell_ref_ast(None, 1, 4, "D1"); // C1 = D1
    graph.set_cell_formula("Sheet1", 1, 3, c1_ast).unwrap();

    let d1_ast = create_cell_ref_ast(None, 1, 3, "C1"); // D1 = C1
    graph.set_cell_formula("Sheet1", 1, 4, d1_ast).unwrap();

    let scheduler = Scheduler::new(&graph);
    let all_vertices: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();

    let sccs = scheduler.tarjan_scc(&all_vertices).unwrap();

    // Should find:
    // - One SCC with just A1
    // - One SCC with {C1, D1}
    // - One SCC with the original B1, which is now just a placeholder
    assert_eq!(sccs.len(), 3);

    let scc_with_cycle = sccs.iter().find(|scc| scc.len() == 2).unwrap();
    let scc_singles: Vec<_> = sccs.iter().filter(|scc| scc.len() == 1).collect();
    assert_eq!(scc_singles.len(), 2);

    let a1_id = *graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 1)).unwrap();
    assert!(scc_singles.iter().any(|scc| scc[0] == a1_id));

    let c1_id = *graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 3)).unwrap();
    let d1_id = *graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 4)).unwrap();
    assert!(scc_with_cycle.contains(&c1_id));
    assert!(scc_with_cycle.contains(&d1_id));
}

#[test]
fn test_tarjan_complex_graph() {
    let mut graph = DependencyGraph::new();

    // Create a more complex graph with multiple cycles:
    // Cycle 1: A1 → B1 → A1
    // Cycle 2: C1 → D1 → E1 → C1
    // Acyclic: F1 → G1, F1 → H1
    // Bridge: B1 → C1 (connects the cycles)

    // Cycle 1: A1 ↔ B1
    let a1_ast = create_cell_ref_ast(None, 1, 2, "B1"); // A1 = B1
    graph.set_cell_formula("Sheet1", 1, 1, a1_ast).unwrap();

    let b1_ast = create_binary_op_ast("A1", "C1", "+"); // B1 = A1 + C1 (also connects to cycle 2)
    graph.set_cell_formula("Sheet1", 1, 2, b1_ast).unwrap();

    // Cycle 2: C1 → D1 → E1 → C1
    let c1_ast = create_cell_ref_ast(None, 1, 4, "D1"); // C1 = D1
    graph.set_cell_formula("Sheet1", 1, 3, c1_ast).unwrap();

    let d1_ast = create_cell_ref_ast(None, 1, 5, "E1"); // D1 = E1
    graph.set_cell_formula("Sheet1", 1, 4, d1_ast).unwrap();

    let e1_ast = create_cell_ref_ast(None, 1, 3, "C1"); // E1 = C1
    graph.set_cell_formula("Sheet1", 1, 5, e1_ast).unwrap();

    // Acyclic part: F1 → G1, F1 → H1
    graph
        .set_cell_value("Sheet1", 1, 6, LiteralValue::Int(10))
        .unwrap(); // F1 = 10

    let g1_ast = create_cell_ref_ast(None, 1, 6, "F1"); // G1 = F1
    graph.set_cell_formula("Sheet1", 1, 7, g1_ast).unwrap();

    let h1_ast = create_cell_ref_ast(None, 1, 6, "F1"); // H1 = F1
    graph.set_cell_formula("Sheet1", 1, 8, h1_ast).unwrap();

    let scheduler = Scheduler::new(&graph);
    let all_vertex_ids = get_vertex_ids_in_order(&graph);

    let sccs = scheduler.tarjan_scc(&all_vertex_ids).unwrap();

    // Should find:
    // - SCC 1: {A1, B1} (2-vertex cycle)
    // - SCC 2: {C1, D1, E1} (3-vertex cycle)
    // - SCC 3: {F1} (single vertex, value)
    // - SCC 4: {G1} (single vertex, depends on F1)
    // - SCC 5: {H1} (single vertex, depends on F1)

    assert_eq!(sccs.len(), 5);

    // Find the cycles by size
    let mut scc_sizes: Vec<usize> = sccs.iter().map(|scc| scc.len()).collect();
    scc_sizes.sort();

    // Should have: [1, 1, 1, 2, 3] (three single vertices, one 2-cycle, one 3-cycle)
    assert_eq!(scc_sizes, vec![1, 1, 1, 2, 3]);
}

#[test]
fn test_tarjan_empty_input() {
    let graph = DependencyGraph::new();
    let scheduler = Scheduler::new(&graph);

    let sccs = scheduler.tarjan_scc(&[]).unwrap();
    assert!(sccs.is_empty());
}

#[test]
fn test_tarjan_single_vertex() {
    let mut graph = DependencyGraph::new();
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(42))
        .unwrap();

    let scheduler = Scheduler::new(&graph);
    let vertices: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();

    let sccs = scheduler.tarjan_scc(&vertices).unwrap();

    assert_eq!(sccs.len(), 1);
    assert_eq!(sccs[0].len(), 1);
    assert_eq!(sccs[0][0], vertices[0]);
}

/// Property test: Verify SCC partitioning correctness
#[test]
fn test_scc_partitioning_properties() {
    let (graph, cell_map) = create_test_graph_with_formulas();
    let scheduler = Scheduler::new(&graph);

    let all_vertices: Vec<VertexId> = cell_map.values().copied().collect();
    let sccs = scheduler.tarjan_scc(&all_vertices).unwrap();

    // Property 1: Every vertex appears in exactly one SCC
    let mut vertex_count = std::collections::HashMap::new();
    for scc in &sccs {
        for &vertex_id in scc {
            *vertex_count.entry(vertex_id).or_insert(0) += 1;
        }
    }

    for &vertex_id in &all_vertices {
        assert_eq!(
            vertex_count.get(&vertex_id),
            Some(&1),
            "Vertex {vertex_id:?} should appear exactly once"
        );
    }

    // Property 2: Total vertex count is preserved
    let total_in_sccs: usize = sccs.iter().map(|scc| scc.len()).sum();
    assert_eq!(total_in_sccs, all_vertices.len());

    // Property 3: No empty SCCs
    for scc in &sccs {
        assert!(!scc.is_empty(), "SCC should not be empty");
    }

    // Property 4: In an acyclic graph, every SCC has exactly one vertex
    for scc in &sccs {
        assert_eq!(
            scc.len(),
            1,
            "In acyclic graph, each SCC should have one vertex"
        );
    }
}
