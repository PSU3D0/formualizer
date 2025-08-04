use crate::engine::graph::{DependencyGraph, NameScope, NamedDefinition};
use crate::reference::{CellRef, Coord, RangeRef};
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_core::parser::parse;

/// Helper to create a literal number value
fn lit_num(value: f64) -> LiteralValue {
    LiteralValue::Number(value)
}

#[test]
fn test_named_range_basic() {
    let mut graph = DependencyGraph::new();

    // Define a named cell "Total" pointing to E10 (row 10, col 5 in 1-based)
    let definition = NamedDefinition::Cell(
        CellRef::new(0, Coord::new(9, 4, true, true)), // 0-based: row 9, col 4
    );

    graph
        .define_name("Total", definition, NameScope::Workbook)
        .unwrap();

    // Set value in the referenced cell
    graph
        .set_cell_value("Sheet1", 9, 4, lit_num(100.0))
        .unwrap();

    // Use in formula
    let ast = parse("=Total*2").unwrap();
    let result = graph.set_cell_formula("Sheet1", 0, 0, ast).unwrap();

    // Verify dependency was created
    assert!(!result.affected_vertices.is_empty());
}

#[test]
fn test_named_range_resolution() {
    let mut graph = DependencyGraph::new();

    // Define workbook-scoped name
    let wb_def = NamedDefinition::Cell(CellRef::new(0, Coord::new(0, 0, true, true)));
    graph
        .define_name("GlobalName", wb_def, NameScope::Workbook)
        .unwrap();

    // Define sheet-scoped name with same name
    let sheet_def = NamedDefinition::Cell(CellRef::new(0, Coord::new(1, 1, true, true)));
    graph
        .define_name("GlobalName", sheet_def, NameScope::Sheet(0))
        .unwrap();

    // Sheet scope should take precedence
    let resolved = graph.resolve_name("GlobalName", 0).unwrap();
    match resolved {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(cell_ref.coord.row, 1);
            assert_eq!(cell_ref.coord.col, 1);
        }
        _ => panic!("Expected Cell definition"),
    }
}

#[test]
fn test_named_range_for_range() {
    let mut graph = DependencyGraph::new();

    // Define a range A1:C3
    let start = CellRef::new(0, Coord::new(0, 0, true, true));
    let end = CellRef::new(0, Coord::new(2, 2, true, true));
    let range_def = NamedDefinition::Range(RangeRef::new(start, end));

    graph
        .define_name("DataRange", range_def, NameScope::Workbook)
        .unwrap();

    // Use in formula - should expand to dependencies
    let ast = parse("=SUM(DataRange)").unwrap();
    let result = graph.set_cell_formula("Sheet1", 5, 5, ast).unwrap();

    // Should have created placeholders for the range
    assert!(result.created_placeholders.len() > 0);
}

#[test]
fn test_invalid_name_rejected() {
    let mut graph = DependencyGraph::new();

    // Try to create a name that looks like a cell reference
    let def = NamedDefinition::Cell(CellRef::new(0, Coord::new(0, 0, true, true)));

    let result = graph.define_name("A1", def.clone(), NameScope::Workbook);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err().kind, ExcelErrorKind::Name));

    // Try to create a name with invalid characters
    let result = graph.define_name("My Name", def.clone(), NameScope::Workbook);
    assert!(result.is_err());

    // Valid names should work
    assert!(
        graph
            .define_name("MyName", def.clone(), NameScope::Workbook)
            .is_ok()
    );
    assert!(
        graph
            .define_name("_Name", def.clone(), NameScope::Sheet(0))
            .is_ok()
    );
    assert!(
        graph
            .define_name("Name.Value", def, NameScope::Sheet(0))
            .is_ok()
    );
}

#[test]
fn test_duplicate_name_error() {
    let mut graph = DependencyGraph::new();

    let def = NamedDefinition::Cell(CellRef::new(0, Coord::new(0, 0, true, true)));

    // First definition should succeed
    graph
        .define_name("MyName", def.clone(), NameScope::Workbook)
        .unwrap();

    // Duplicate in same scope should fail
    let result = graph.define_name("MyName", def.clone(), NameScope::Workbook);
    assert!(result.is_err());

    // Same name in different scope should succeed
    assert!(
        graph
            .define_name("MyName", def, NameScope::Sheet(0))
            .is_ok()
    );
}

#[test]
fn test_undefined_name_error() {
    let mut graph = DependencyGraph::new();

    // Try to use undefined name in formula
    let ast = parse("=UndefinedName*2").unwrap();
    let result = graph.set_cell_formula("Sheet1", 0, 0, ast);

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err().kind, ExcelErrorKind::Name));
}

#[test]
fn test_update_named_range() {
    let mut graph = DependencyGraph::new();

    // Define initial name
    let def1 = NamedDefinition::Cell(CellRef::new(0, Coord::new(0, 0, true, true)));
    graph
        .define_name("MyCell", def1, NameScope::Workbook)
        .unwrap();

    // Set value in A1
    graph.set_cell_value("Sheet1", 0, 0, lit_num(10.0)).unwrap();

    // Create formula using the name
    let ast = parse("=MyCell+5").unwrap();
    graph.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    // Update the name to point to B1
    let def2 = NamedDefinition::Cell(CellRef::new(0, Coord::new(0, 1, true, true)));
    graph
        .update_name("MyCell", def2, NameScope::Workbook)
        .unwrap();

    // The formula should now be marked dirty
    // We'll just verify that the update succeeded for now
    // In a real test, we'd check that evaluation picks up the new reference
}

#[test]
fn test_delete_named_range() {
    let mut graph = DependencyGraph::new();

    // Define a name
    let def = NamedDefinition::Cell(CellRef::new(0, Coord::new(0, 0, true, true)));
    graph
        .define_name("TempName", def, NameScope::Workbook)
        .unwrap();

    // Use in formula
    let ast = parse("=TempName").unwrap();
    let vertex_id = graph
        .set_cell_formula("Sheet1", 1, 1, ast)
        .unwrap()
        .affected_vertices[0];

    // Delete the name
    graph.delete_name("TempName", NameScope::Workbook).unwrap();

    // The formula should be marked dirty (will error on next eval)
    assert!(graph.is_dirty(vertex_id));
}

#[test]
fn test_named_formula() {
    let mut graph = DependencyGraph::new();

    // Set up some values
    graph.set_cell_value("Sheet1", 0, 0, lit_num(10.0)).unwrap();
    graph.set_cell_value("Sheet1", 0, 1, lit_num(20.0)).unwrap();

    // Define a named formula
    let formula_ast = parse("=A1+B1").unwrap();
    let def = NamedDefinition::Formula {
        ast: formula_ast,
        dependencies: Vec::new(), // Will be computed
        range_deps: Vec::new(),
    };

    graph
        .define_name("Total", def, NameScope::Workbook)
        .unwrap();

    // Use the named formula in another formula
    let ast = parse("=Total*2").unwrap();
    let result = graph.set_cell_formula("Sheet1", 2, 2, ast);

    // Should succeed and create dependencies
    assert!(result.is_ok());
}

#[test]
fn test_circular_reference_through_names() {
    let mut graph = DependencyGraph::new();

    // Create a simpler circular reference through named ranges
    // Define NamedRange1 pointing to A1
    graph
        .define_name(
            "NamedRange1",
            NamedDefinition::Cell(CellRef::new(0, Coord::new(0, 0, true, true))),
            NameScope::Workbook,
        )
        .unwrap();

    // A1 = NamedRange1 (self-reference through name)
    let ast1 = parse("=NamedRange1").unwrap();
    let result = graph.set_cell_formula("Sheet1", 0, 0, ast1);

    // Should detect the self-reference cycle
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err().kind, ExcelErrorKind::Circ));
}

#[test]
fn test_name_scope_precedence() {
    let mut graph = DependencyGraph::new();

    // Set values in different cells
    graph
        .set_cell_value("Sheet1", 0, 0, lit_num(100.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 1, 1, lit_num(200.0))
        .unwrap();

    // Define workbook-scoped name pointing to A1
    let wb_def = NamedDefinition::Cell(CellRef::new(0, Coord::new(0, 0, true, true)));
    graph
        .define_name("Value", wb_def, NameScope::Workbook)
        .unwrap();

    // Define sheet-scoped name with same name pointing to B2
    let sheet_def = NamedDefinition::Cell(CellRef::new(0, Coord::new(1, 1, true, true)));
    graph
        .define_name("Value", sheet_def, NameScope::Sheet(0))
        .unwrap();

    // Formula in Sheet1 should use sheet-scoped name (B2 = 200)
    let ast = parse("=Value").unwrap();
    let result = graph.set_cell_formula("Sheet1", 2, 2, ast);
    assert!(result.is_ok());

    // The formula should depend on B2, not A1
    // This would be verified during evaluation
}

#[test]
fn test_large_named_range_compression() {
    let mut graph = DependencyGraph::new();

    // Define a large range that should be kept compressed
    let start = CellRef::new(0, Coord::new(0, 0, true, true));
    let end = CellRef::new(0, Coord::new(999, 99, true, true)); // 1000x100 range
    let range_def = NamedDefinition::Range(RangeRef::new(start, end));

    graph
        .define_name("BigData", range_def, NameScope::Workbook)
        .unwrap();

    // Use in formula
    let ast = parse("=SUM(BigData)").unwrap();
    let result = graph.set_cell_formula("Sheet1", 1000, 0, ast).unwrap();

    // Should not create individual placeholders for such a large range
    // (depends on config.range_expansion_limit)
    assert!(result.created_placeholders.len() < 100000);
}
