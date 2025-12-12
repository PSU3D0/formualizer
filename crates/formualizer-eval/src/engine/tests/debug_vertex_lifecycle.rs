use crate::engine::{DependencyGraph, VertexEditor};
use crate::reference::{CellRef, Coord};
use formualizer_common::LiteralValue;
use formualizer_parse::parse;

fn cell_ref(sheet_id: u16, row: u32, col: u32) -> CellRef {
    CellRef {
        sheet_id,
        coord: Coord::from_excel(row, col, true, true),
    }
}

fn lit_num(value: f64) -> LiteralValue {
    LiteralValue::Number(value)
}

#[test]
fn debug_dependency_creation() {
    let mut graph = DependencyGraph::new();
    let mut editor = VertexEditor::new(&mut graph);

    // Create A1 with value (note: A1 = row 1, col 1 in Excel's 1-based indexing)
    println!("Creating A1 with value 10.0");
    let a1 = editor.set_cell_value(cell_ref(0, 1, 1), lit_num(10.0));
    println!("A1 vertex ID: {a1:?}");

    // Drop editor and recreate to verify mapping
    drop(editor);

    // Check cell_to_vertex mapping
    let a1_addr = cell_ref(0, 1, 1);
    let a1_from_graph = graph.get_vertex_for_cell(&a1_addr);
    println!("A1 from get_vertex_for_cell: {a1_from_graph:?}");
    println!("A1 CellRef: {a1_addr:?}");

    // Check what the full cell_to_vertex map contains
    let all_cells = graph.cell_to_vertex();
    for (cell, vid) in all_cells.iter() {
        println!("  Cell {cell:?} -> Vertex {vid:?}");
    }

    let mut editor = VertexEditor::new(&mut graph);

    // Create B1 with formula =A1*2
    println!("Creating B1 with formula =A1*2");
    let formula = parse("=A1*2").unwrap();

    // Debug the parsed formula to see what reference it creates
    println!("Parsed formula: {formula:?}");

    let b1 = editor.set_cell_formula(cell_ref(0, 1, 2), formula); // B1 = row 1, col 2
    println!("B1 vertex ID: {b1:?}");

    // Check cell mapping after formula creation
    drop(editor);
    println!("\nCell mapping after B1 creation:");
    let all_cells = graph.cell_to_vertex();
    for (cell, vid) in all_cells.iter() {
        println!("  Cell {cell:?} -> Vertex {vid:?}");
    }
    let editor = VertexEditor::new(&mut graph);

    // Drop editor to ensure changes are committed
    drop(editor);

    // Check if dependency was created
    println!("Checking dependencies...");
    let a1_deps = graph.get_dependencies(a1);
    println!("A1 dependencies (should be empty): {a1_deps:?}");

    let b1_deps = graph.get_dependencies(b1);
    println!("B1 dependencies (should contain A1): {b1_deps:?}");

    // Force rebuild if needed
    println!("Delta size before rebuild: {}", graph.edges_delta_size());
    if graph.edges_delta_size() > 0 {
        graph.rebuild_edges();
        println!("Rebuilt edges");
    }

    // Check dependents
    let a1_dependents = graph.get_dependents(a1);
    println!("A1 dependents (should contain B1): {a1_dependents:?}");

    assert_eq!(b1_deps.len(), 1, "B1 should depend on A1");
    assert_eq!(b1_deps[0], a1, "B1's dependency should be A1");
    assert_eq!(a1_dependents.len(), 1, "A1 should have 1 dependent");
    assert_eq!(a1_dependents[0], b1, "A1's dependent should be B1");
}
