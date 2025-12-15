use crate::engine::graph::DependencyGraph;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::vertex::VertexKind;
use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::parse;
use rustc_hash::FxHashSet;

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
    let result = graph.set_cell_formula("Sheet1", 1, 1, ast).unwrap();

    // Verify dependency was created
    assert!(!result.affected_vertices.is_empty());
}

#[test]
fn named_range_vertex_defaults_to_scalar_kind() {
    let mut graph = DependencyGraph::new();
    graph.set_cell_value("Sheet1", 1, 1, lit_num(5.0)).unwrap();

    let definition = NamedDefinition::Cell(CellRef::new(0, Coord::new(0, 0, true, true)));
    graph
        .define_name("ScalarName", definition, NameScope::Workbook)
        .unwrap();

    let (_name, named_range) = graph
        .named_ranges_iter()
        .find(|(n, _)| n.as_str() == "ScalarName")
        .expect("named range should exist");

    assert_eq!(
        graph.get_vertex_kind(named_range.vertex),
        VertexKind::NamedScalar
    );
}

#[test]
fn named_range_range_allocates_array_vertex() {
    let mut graph = DependencyGraph::new();
    let start = CellRef::new(0, Coord::new(0, 0, true, true));
    let end = CellRef::new(0, Coord::new(1, 1, true, true));
    let range_def = NamedDefinition::Range(RangeRef::new(start, end));

    graph
        .define_name("RangeName", range_def, NameScope::Workbook)
        .unwrap();

    let (_name, named_range) = graph
        .named_ranges_iter()
        .find(|(n, _)| n.as_str() == "RangeName")
        .expect("range name should exist");

    assert_eq!(
        graph.get_vertex_kind(named_range.vertex),
        VertexKind::NamedArray
    );
}

#[test]
fn named_range_dirty_propagation_reaches_formula() {
    let mut graph = DependencyGraph::new();
    graph.set_cell_value("Sheet1", 1, 1, lit_num(5.0)).unwrap();

    let base_ref = CellRef::new(0, Coord::new(0, 0, true, true));
    graph
        .define_name(
            "Input",
            NamedDefinition::Cell(base_ref),
            NameScope::Workbook,
        )
        .unwrap();

    let ast = parse("=Input + 1").unwrap();
    let formula_summary = graph.set_cell_formula("Sheet1", 2, 1, ast).unwrap();
    assert!(
        formula_summary.affected_vertices.contains(
            &graph
                .get_vertex_for_cell(&CellRef::new(0, Coord::new(1, 0, true, true)))
                .unwrap()
        )
    );

    let name_vertex = graph
        .named_ranges_iter()
        .find(|(n, _)| n.as_str() == "Input")
        .unwrap()
        .1
        .vertex;
    let formula_vertex = graph
        .get_vertex_for_cell(&CellRef::new(0, Coord::new(1, 0, true, true)))
        .unwrap();

    let summary = graph.set_cell_value("Sheet1", 1, 1, lit_num(7.0)).unwrap();
    let affected: FxHashSet<_> = summary.affected_vertices.into_iter().collect();
    assert!(
        affected.contains(&name_vertex),
        "expected name vertex to be dirtied when base cell edits"
    );
    assert!(
        affected.contains(&formula_vertex),
        "expected formula dependent to be dirtied when name changes"
    );
}

#[test]
fn named_range_eval_mutation_propagates() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    let sheet_id = engine.sheet_id_mut("Sheet1");
    let input_ref = CellRef::new(sheet_id, Coord::new(0, 0, true, true));
    engine
        .define_name(
            "InputValue",
            NamedDefinition::Cell(input_ref),
            NameScope::Workbook,
        )
        .unwrap();

    let formula_ast = parse("=InputValue*2").unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, formula_ast)
        .unwrap();

    engine.evaluate_all().unwrap();
    let initial = engine
        .get_cell_value("Sheet1", 2, 1)
        .expect("initial output");

    assert!(matches!(initial, LiteralValue::Number(n) if (n - 20.0).abs() < 1e-9));

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(25.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    let updated = engine
        .get_cell_value("Sheet1", 2, 1)
        .expect("updated output");
    assert!(matches!(updated, LiteralValue::Number(n) if (n - 50.0).abs() < 1e-9));
}

#[test]
fn engine_get_cell_value_handles_named_range_formula() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    let sheet_id = engine.graph.sheet_id_mut("Sheet1");
    let input_ref = CellRef::new(sheet_id, Coord::new(0, 0, true, true));
    engine
        .graph
        .define_name(
            "InputValue",
            NamedDefinition::Cell(input_ref),
            NameScope::Workbook,
        )
        .unwrap();

    let formula_ast = parse("=InputValue*2").unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, formula_ast)
        .unwrap();

    engine.evaluate_all().unwrap();

    let via_engine = engine
        .get_cell_value("Sheet1", 1, 2)
        .expect("engine should surface formula result");
    assert!(matches!(via_engine, LiteralValue::Number(n) if (n - 20.0).abs() < 1e-9));

    let via_graph = engine
        .graph
        .get_cell_value("Sheet1", 1, 2)
        .expect("graph should have formula value");
    assert!(matches!(via_graph, LiteralValue::Number(n) if (n - 20.0).abs() < 1e-9));

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(25.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    let updated_engine = engine
        .get_cell_value("Sheet1", 1, 2)
        .expect("engine should reflect updated named range");
    assert!(matches!(updated_engine, LiteralValue::Number(n) if (n - 50.0).abs() < 1e-9));

    let updated_graph = engine
        .graph
        .get_cell_value("Sheet1", 1, 2)
        .expect("graph should reflect updated named range");
    assert!(matches!(updated_graph, LiteralValue::Number(n) if (n - 50.0).abs() < 1e-9));
}

#[test]
fn named_range_chain_propagates_dirty() {
    let mut graph = DependencyGraph::new();
    graph.set_cell_value("Sheet1", 1, 1, lit_num(2.0)).unwrap();

    let inner_ref = CellRef::new(0, Coord::new(0, 0, true, true));
    graph
        .define_name(
            "Inner",
            NamedDefinition::Cell(inner_ref),
            NameScope::Workbook,
        )
        .unwrap();

    let outer_ast = parse("=Inner * 2").unwrap();
    graph
        .define_name(
            "Outer",
            NamedDefinition::Formula {
                ast: outer_ast,
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();

    let sum_ast = parse("=Outer + 3").unwrap();
    graph.set_cell_formula("Sheet1", 2, 1, sum_ast).unwrap();

    let inner_vertex = graph
        .named_ranges_iter()
        .find(|(n, _)| n.as_str() == "Inner")
        .unwrap()
        .1
        .vertex;
    let outer_vertex = graph
        .named_ranges_iter()
        .find(|(n, _)| n.as_str() == "Outer")
        .unwrap()
        .1
        .vertex;
    let formula_vertex = graph
        .get_vertex_for_cell(&CellRef::new(0, Coord::new(1, 0, true, true)))
        .unwrap();

    let summary = graph.set_cell_value("Sheet1", 1, 1, lit_num(6.0)).unwrap();
    let affected: FxHashSet<_> = summary.affected_vertices.into_iter().collect();

    assert!(affected.contains(&inner_vertex));
    assert!(affected.contains(&outer_vertex));
    assert!(affected.contains(&formula_vertex));
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
            assert_eq!(cell_ref.coord.row(), 1);
            assert_eq!(cell_ref.coord.col(), 1);
        }
        _ => panic!("Expected Cell definition"),
    }
}

#[test]
fn test_named_range_for_range() {
    let mut graph = DependencyGraph::new();

    // Define a range A1:C3
    let start = CellRef::new(0, Coord::new(0, 0, true, true));
    let end = CellRef::new(0, Coord::from_excel(2, 2, true, true));
    let range_def = NamedDefinition::Range(RangeRef::new(start, end));

    graph
        .define_name("DataRange", range_def, NameScope::Workbook)
        .unwrap();

    // Use in formula - should expand to dependencies
    let ast = parse("=SUM(DataRange)").unwrap();
    let result = graph.set_cell_formula("Sheet1", 5, 5, ast).unwrap();

    // Should have created placeholders for the range
    assert!(!result.created_placeholders.is_empty());
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
    let result = graph.set_cell_formula("Sheet1", 1, 1, ast);

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
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();

    // Create formula using the name
    let ast = parse("=MyCell+5").unwrap();
    graph.set_cell_formula("Sheet1", 1, 3, ast).unwrap();

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
        .set_cell_formula("Sheet1", 1, 2, ast)
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
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph.set_cell_value("Sheet1", 1, 2, lit_num(20.0)).unwrap();

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
    let result = graph.set_cell_formula("Sheet1", 1, 1, ast1);

    // Should detect the self-reference cycle
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err().kind, ExcelErrorKind::Circ));
}

#[test]
fn test_name_scope_precedence() {
    let mut graph = DependencyGraph::new();

    // Set values in different cells
    graph
        .set_cell_value("Sheet1", 1, 1, lit_num(100.0))
        .unwrap();
    graph
        .set_cell_value("Sheet1", 2, 2, lit_num(200.0))
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
    let result = graph.set_cell_formula("Sheet1", 2, 3, ast);
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

// ============ Phase 2: Structural Operations Tests ============

#[test]
fn test_named_range_insert_rows() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Define a named cell at B5 (row 4, col 1 in 0-based) with relative references
    let def = NamedDefinition::Cell(CellRef::new(0, Coord::new(4, 1, false, false)));
    graph
        .define_name("Target", def, NameScope::Workbook)
        .unwrap();

    // Insert 2 rows before row 3
    let op = ShiftOperation::InsertRows {
        sheet_id: 0,
        before: 3,
        count: 2,
    };

    graph.adjust_named_ranges(&op).unwrap();

    // Check that Target now points to B7 (row 6, col 1)
    let adjusted = graph.resolve_name("Target", 0).unwrap();
    match adjusted {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(cell_ref.coord.row(), 6, "Row should shift from 4 to 6");
            assert_eq!(cell_ref.coord.col(), 1, "Column should remain 1");
        }
        _ => panic!("Expected Cell definition"),
    }
}

#[test]
fn test_named_range_delete_rows() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Define a named cell at B10 (row 9, col 1 in 0-based) with relative references
    let def = NamedDefinition::Cell(CellRef::new(0, Coord::new(9, 1, false, false)));
    graph
        .define_name("Target", def, NameScope::Workbook)
        .unwrap();

    // Delete 3 rows starting at row 2
    let op = ShiftOperation::DeleteRows {
        sheet_id: 0,
        start: 2,
        count: 3,
    };

    graph.adjust_named_ranges(&op).unwrap();

    // Check that Target now points to B7 (row 6, col 1)
    let adjusted = graph.resolve_name("Target", 0).unwrap();
    match adjusted {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(cell_ref.coord.row(), 6, "Row should shift from 9 to 6");
            assert_eq!(cell_ref.coord.col(), 1, "Column should remain 1");
        }
        _ => panic!("Expected Cell definition"),
    }
}

#[test]
fn test_named_range_insert_columns() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Define a named cell at E3 (row 2, col 4 in 0-based) with relative references
    let def = NamedDefinition::Cell(CellRef::new(0, Coord::new(2, 4, false, false)));
    graph
        .define_name("Target", def, NameScope::Workbook)
        .unwrap();

    // Insert 2 columns before column 2
    let op = ShiftOperation::InsertColumns {
        sheet_id: 0,
        before: 2,
        count: 2,
    };

    graph.adjust_named_ranges(&op).unwrap();

    // Check that Target now points to G3 (row 2, col 6)
    let adjusted = graph.resolve_name("Target", 0).unwrap();
    match adjusted {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(cell_ref.coord.row(), 2, "Row should remain 2");
            assert_eq!(cell_ref.coord.col(), 6, "Column should shift from 4 to 6");
        }
        _ => panic!("Expected Cell definition"),
    }
}

#[test]
fn test_named_range_delete_columns() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Define a named cell at J3 (row 2, col 9 in 0-based) with relative references
    let def = NamedDefinition::Cell(CellRef::new(0, Coord::new(2, 9, false, false)));
    graph
        .define_name("Target", def, NameScope::Workbook)
        .unwrap();

    // Delete 3 columns starting at column 4
    let op = ShiftOperation::DeleteColumns {
        sheet_id: 0,
        start: 4,
        count: 3,
    };

    graph.adjust_named_ranges(&op).unwrap();

    // Check that Target now points to G3 (row 2, col 6)
    let adjusted = graph.resolve_name("Target", 0).unwrap();
    match adjusted {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(cell_ref.coord.row(), 2, "Row should remain 2");
            assert_eq!(cell_ref.coord.col(), 6, "Column should shift from 9 to 6");
        }
        _ => panic!("Expected Cell definition"),
    }
}

#[test]
fn test_named_range_adjustment() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Define a range B2:D4 (0-based: row 1-3, col 1-3) with relative references
    let start = CellRef::new(0, Coord::new(1, 1, false, false));
    let end = CellRef::new(0, Coord::new(3, 3, false, false));
    let range_def = NamedDefinition::Range(RangeRef::new(start, end));

    graph
        .define_name("DataRange", range_def, NameScope::Workbook)
        .unwrap();

    // Insert a row before row 2
    let op = ShiftOperation::InsertRows {
        sheet_id: 0,
        before: 2,
        count: 1,
    };

    graph.adjust_named_ranges(&op).unwrap();

    // Check that range adjusted to B2:D5 (row 1-4, col 1-3)
    let adjusted = graph.resolve_name("DataRange", 0).unwrap();
    match adjusted {
        NamedDefinition::Range(range_ref) => {
            assert_eq!(range_ref.start.coord.row(), 1, "Start row should remain 1");
            assert_eq!(
                range_ref.end.coord.row(),
                4,
                "End row should shift from 3 to 4"
            );
            assert_eq!(range_ref.start.coord.col(), 1, "Start col should remain 1");
            assert_eq!(range_ref.end.coord.col(), 3, "End col should remain 3");
        }
        _ => panic!("Expected Range definition"),
    }
}

#[test]
fn test_named_formula_adjustment() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Define a named formula that references A1+B1
    let formula_ast = parse("=A1+B1").unwrap();
    let def = NamedDefinition::Formula {
        ast: formula_ast,
        dependencies: Vec::new(),
        range_deps: Vec::new(),
    };

    graph
        .define_name("Total", def, NameScope::Workbook)
        .unwrap();

    // Insert a row at the beginning
    let op = ShiftOperation::InsertRows {
        sheet_id: 0,
        before: 0,
        count: 1,
    };

    graph.adjust_named_ranges(&op).unwrap();

    // Check that formula now references A2+B2
    let adjusted = graph.resolve_name("Total", 0).unwrap();
    match adjusted {
        NamedDefinition::Formula { ast, .. } => {
            // The AST should have been adjusted
            // This would be verified by checking the AST structure
            // For now we just verify it's still a formula
            assert!(matches!(
                ast.node_type,
                formualizer_parse::parser::ASTNodeType::BinaryOp { .. }
            ));
        }
        _ => panic!("Expected Formula definition"),
    }
}

#[test]
fn test_named_range_delete_causes_ref_error() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Define a named cell at B3 (row 2, col 1 in 0-based) with relative references
    let def = NamedDefinition::Cell(CellRef::new(0, Coord::new(2, 1, false, false)));
    graph
        .define_name("Target", def, NameScope::Workbook)
        .unwrap();

    // Delete the row containing the named cell
    let op = ShiftOperation::DeleteRows {
        sheet_id: 0,
        start: 2,
        count: 1,
    };

    // Adjusting should fail with REF error
    let result = graph.adjust_named_ranges(&op);
    assert!(result.is_err());
    if let Err(e) = result {
        assert!(matches!(e.kind, ExcelErrorKind::Ref));
    }
}

// ============ Tests for Absolute vs Relative References ============

#[test]
fn test_absolute_references_dont_move() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Define a named cell at $B$5 (absolute reference)
    let def = NamedDefinition::Cell(CellRef::new(0, Coord::new(4, 1, true, true)));
    graph
        .define_name("AbsoluteTarget", def, NameScope::Workbook)
        .unwrap();

    // Insert rows before row 3
    let op = ShiftOperation::InsertRows {
        sheet_id: 0,
        before: 3,
        count: 2,
    };

    graph.adjust_named_ranges(&op).unwrap();

    // Check that AbsoluteTarget still points to B5 (row 4, col 1)
    let adjusted = graph.resolve_name("AbsoluteTarget", 0).unwrap();
    match adjusted {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(cell_ref.coord.row(), 4, "Absolute row should not change");
            assert_eq!(cell_ref.coord.col(), 1, "Absolute column should not change");
            assert!(cell_ref.coord.row_abs(), "Should still be absolute row");
            assert!(cell_ref.coord.col_abs(), "Should still be absolute column");
        }
        _ => panic!("Expected Cell definition"),
    }
}

#[test]
fn test_mixed_references_partial_adjustment() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Test $B5 (absolute column, relative row)
    let def1 = NamedDefinition::Cell(CellRef::new(0, Coord::new(4, 1, false, true)));
    graph
        .define_name("MixedRef1", def1, NameScope::Workbook)
        .unwrap();

    // Test B$5 (relative column, absolute row)
    let def2 = NamedDefinition::Cell(CellRef::new(0, Coord::new(4, 1, true, false)));
    graph
        .define_name("MixedRef2", def2, NameScope::Workbook)
        .unwrap();

    // Insert 2 rows before row 3
    let op = ShiftOperation::InsertRows {
        sheet_id: 0,
        before: 3,
        count: 2,
    };

    graph.adjust_named_ranges(&op).unwrap();

    // Check $B5 - row should move, column should not
    let mixed1 = graph.resolve_name("MixedRef1", 0).unwrap();
    match mixed1 {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(
                cell_ref.coord.row(),
                6,
                "Relative row should shift from 4 to 6"
            );
            assert_eq!(cell_ref.coord.col(), 1, "Absolute column should remain 1");
            assert!(!cell_ref.coord.row_abs(), "Row should be relative");
            assert!(cell_ref.coord.col_abs(), "Column should be absolute");
        }
        _ => panic!("Expected Cell definition"),
    }

    // Check B$5 - row should not move, column stays same
    let mixed2 = graph.resolve_name("MixedRef2", 0).unwrap();
    match mixed2 {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(cell_ref.coord.row(), 4, "Absolute row should remain 4");
            assert_eq!(cell_ref.coord.col(), 1, "Column should remain 1");
            assert!(cell_ref.coord.row_abs(), "Row should be absolute");
            assert!(!cell_ref.coord.col_abs(), "Column should be relative");
        }
        _ => panic!("Expected Cell definition"),
    }
}

#[test]
fn test_mixed_references_column_operations() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Test $E3 (absolute column, relative row)
    let def1 = NamedDefinition::Cell(CellRef::new(0, Coord::new(2, 4, false, true)));
    graph
        .define_name("ColMixed1", def1, NameScope::Workbook)
        .unwrap();

    // Test E$3 (relative column, absolute row)
    let def2 = NamedDefinition::Cell(CellRef::new(0, Coord::new(2, 4, true, false)));
    graph
        .define_name("ColMixed2", def2, NameScope::Workbook)
        .unwrap();

    // Insert 2 columns before column 2
    let op = ShiftOperation::InsertColumns {
        sheet_id: 0,
        before: 2,
        count: 2,
    };

    graph.adjust_named_ranges(&op).unwrap();

    // Check $E3 - column should not move
    let mixed1 = graph.resolve_name("ColMixed1", 0).unwrap();
    match mixed1 {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(cell_ref.coord.row(), 2, "Row should remain 2");
            assert_eq!(cell_ref.coord.col(), 4, "Absolute column should remain 4");
            assert!(!cell_ref.coord.row_abs(), "Row should be relative");
            assert!(cell_ref.coord.col_abs(), "Column should be absolute");
        }
        _ => panic!("Expected Cell definition"),
    }

    // Check E$3 - column should move, row should not
    let mixed2 = graph.resolve_name("ColMixed2", 0).unwrap();
    match mixed2 {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(cell_ref.coord.row(), 2, "Absolute row should remain 2");
            assert_eq!(
                cell_ref.coord.col(),
                6,
                "Relative column should shift from 4 to 6"
            );
            assert!(cell_ref.coord.row_abs(), "Row should be absolute");
            assert!(!cell_ref.coord.col_abs(), "Column should be relative");
        }
        _ => panic!("Expected Cell definition"),
    }
}

#[test]
fn test_range_with_mixed_references() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Define a range $B$2:D4 (absolute start, relative end)
    let start = CellRef::new(0, Coord::new(1, 1, true, true)); // $B$2
    let end = CellRef::new(0, Coord::new(3, 3, false, false)); // D4
    let range_def = NamedDefinition::Range(RangeRef::new(start, end));

    graph
        .define_name("MixedRange", range_def, NameScope::Workbook)
        .unwrap();

    // Insert a row before row 2
    let op = ShiftOperation::InsertRows {
        sheet_id: 0,
        before: 2,
        count: 1,
    };

    graph.adjust_named_ranges(&op).unwrap();

    // Check that range adjusted correctly
    let adjusted = graph.resolve_name("MixedRange", 0).unwrap();
    match adjusted {
        NamedDefinition::Range(range_ref) => {
            // Start ($B$2) should not move
            assert_eq!(
                range_ref.start.coord.row(),
                1,
                "Absolute start row should remain 1"
            );
            assert_eq!(
                range_ref.start.coord.col(),
                1,
                "Absolute start col should remain 1"
            );

            // End (D4) should move to D5
            assert_eq!(
                range_ref.end.coord.row(),
                4,
                "Relative end row should shift from 3 to 4"
            );
            assert_eq!(range_ref.end.coord.col(), 3, "End col should remain 3");
        }
        _ => panic!("Expected Range definition"),
    }
}

#[test]
fn test_absolute_ref_deleted_no_error() {
    use crate::engine::graph::editor::reference_adjuster::ShiftOperation;

    let mut graph = DependencyGraph::new();

    // Define an absolute reference at $B$3
    let def = NamedDefinition::Cell(CellRef::new(0, Coord::new(2, 1, true, true)));
    graph
        .define_name("AbsoluteRef", def, NameScope::Workbook)
        .unwrap();

    // Delete rows that would include row 3 if it were relative
    let op = ShiftOperation::DeleteRows {
        sheet_id: 0,
        start: 2,
        count: 1,
    };

    // Absolute references don't adjust, so they don't get deleted
    // The reference remains valid even though the row it points to is deleted
    let result = graph.adjust_named_ranges(&op);
    assert!(
        result.is_ok(),
        "Absolute references should not cause errors when their row is deleted"
    );

    // The reference should still point to row 2 (which is now what was row 3)
    let adjusted = graph.resolve_name("AbsoluteRef", 0).unwrap();
    match adjusted {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(
                cell_ref.coord.row(),
                2,
                "Absolute reference should not change"
            );
            assert_eq!(cell_ref.coord.col(), 1, "Column should not change");
        }
        _ => panic!("Expected Cell definition"),
    }
}

// ============ Phase 3: VertexEditor Integration Tests ============

#[test]
fn test_vertex_editor_define_name_for_cell() {
    use crate::engine::graph::editor::VertexEditor;

    let mut graph = DependencyGraph::new();

    // Set a value in the cell we'll name
    graph
        .set_cell_value("Sheet1", 5, 2, lit_num(100.0))
        .unwrap();

    // Use VertexEditor to define a name
    {
        let mut editor = VertexEditor::new(&mut graph);
        editor
            .define_name_for_cell("TotalSales", "Sheet1", 5, 2, NameScope::Workbook)
            .expect("Should define name successfully");
    }

    // Verify the name was defined
    let resolved = graph.resolve_name("TotalSales", 0).unwrap();
    match resolved {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(cell_ref.coord.row(), 4);
            assert_eq!(cell_ref.coord.col(), 1);
        }
        _ => panic!("Expected Cell definition"),
    }

    // Use the name in a formula
    let ast = parse("=TotalSales*1.1").unwrap();
    let result = graph.set_cell_formula("Sheet1", 10, 10, ast);
    assert!(result.is_ok());
}

#[test]
fn test_vertex_editor_define_name_for_range() {
    use crate::engine::graph::editor::VertexEditor;

    let mut graph = DependencyGraph::new();

    // Use VertexEditor to define a range name
    {
        let mut editor = VertexEditor::new(&mut graph);
        editor
            .define_name_for_range(
                "SalesData",
                "Sheet1",
                1,
                1, // A1
                10,
                3, // C10
                NameScope::Workbook,
            )
            .expect("Should define range name successfully");
    }

    // Verify the range was defined
    let resolved = graph.resolve_name("SalesData", 0).unwrap();
    match resolved {
        NamedDefinition::Range(range_ref) => {
            assert_eq!(range_ref.start.coord.row(), 0);
            assert_eq!(range_ref.start.coord.col(), 0);
            assert_eq!(range_ref.end.coord.row(), 9);
            assert_eq!(range_ref.end.coord.col(), 2);
        }
        _ => panic!("Expected Range definition"),
    }
}

#[test]
fn test_vertex_editor_sheet_scoped_names() {
    use crate::engine::graph::editor::VertexEditor;

    let mut graph = DependencyGraph::new();

    {
        let mut editor = VertexEditor::new(&mut graph);

        // Define workbook-scoped name
        editor
            .define_name_for_cell("Value", "Sheet1", 1, 1, NameScope::Workbook)
            .expect("Should define workbook name");

        // Define sheet-scoped name with same name
        editor
            .define_name_for_cell("Value", "Sheet1", 2, 2, NameScope::Sheet(0))
            .expect("Should define sheet name");
    }

    // Sheet scope should take precedence
    let resolved = graph.resolve_name("Value", 0).unwrap();
    match resolved {
        NamedDefinition::Cell(cell_ref) => {
            assert_eq!(
                cell_ref.coord.row(),
                1,
                "Sheet-scoped name should take precedence"
            );
            assert_eq!(cell_ref.coord.col(), 1);
        }
        _ => panic!("Expected Cell definition"),
    }
}

#[test]
fn test_vertex_editor_update_name() {
    use crate::engine::graph::editor::VertexEditor;

    let mut graph = DependencyGraph::new();

    // Set values in cells
    graph.set_cell_value("Sheet1", 1, 1, lit_num(10.0)).unwrap();
    graph.set_cell_value("Sheet1", 2, 2, lit_num(20.0)).unwrap();

    // Define initial name
    {
        let mut editor = VertexEditor::new(&mut graph);
        editor
            .define_name_for_cell("Target", "Sheet1", 1, 1, NameScope::Workbook)
            .expect("Should define name");
    }

    // Create formula using the name
    let ast = parse("=Target+5").unwrap();
    let vertex_id = graph
        .set_cell_formula("Sheet1", 3, 3, ast)
        .unwrap()
        .affected_vertices[0];

    // Update the name to point to a different cell
    {
        let mut editor = VertexEditor::new(&mut graph);
        let new_def = NamedDefinition::Cell(CellRef::new(0, Coord::from_excel(2, 2, true, true)));
        editor
            .update_name("Target", new_def, NameScope::Workbook)
            .expect("Should update name");
    }

    // The formula should now be marked dirty
    assert!(graph.is_dirty(vertex_id));
}

#[test]
fn test_vertex_editor_delete_name() {
    use crate::engine::graph::editor::VertexEditor;

    let mut graph = DependencyGraph::new();

    // Define a name
    {
        let mut editor = VertexEditor::new(&mut graph);
        editor
            .define_name_for_cell("TempName", "Sheet1", 1, 1, NameScope::Workbook)
            .expect("Should define name");
    }

    // Use in formula
    let ast = parse("=TempName").unwrap();
    let vertex_id = graph
        .set_cell_formula("Sheet1", 2, 2, ast)
        .unwrap()
        .affected_vertices[0];

    // Delete the name
    {
        let mut editor = VertexEditor::new(&mut graph);
        editor
            .delete_name("TempName", NameScope::Workbook)
            .expect("Should delete name");
    }

    // The formula should be marked dirty (will error on next eval)
    assert!(graph.is_dirty(vertex_id));

    // Verify the name is gone
    assert!(graph.resolve_name("TempName", 0).is_none());
}

#[test]
fn test_vertex_editor_invalid_sheet_name() {
    use crate::engine::graph::editor::VertexEditor;

    let mut graph = DependencyGraph::new();
    let mut editor = VertexEditor::new(&mut graph);

    // Try to define a name for a non-existent sheet
    let result =
        editor.define_name_for_cell("MyName", "NonExistentSheet", 1, 1, NameScope::Workbook);

    assert!(result.is_err());
    if let Err(e) = result {
        match e {
            crate::engine::graph::editor::EditorError::InvalidName { name, reason } => {
                assert_eq!(name, "NonExistentSheet");
                assert!(reason.contains("not found"));
            }
            _ => panic!("Expected InvalidName error"),
        }
    }
}

#[test]
fn test_vertex_editor_structural_operations_with_names() {
    use crate::engine::graph::editor::VertexEditor;

    let mut graph = DependencyGraph::new();

    // Define a named range that will be affected by structural operations
    {
        let mut editor = VertexEditor::new(&mut graph);
        // Manually create range with relative references so it will move
        let start = CellRef::new(0, Coord::new(3, 1, false, false)); // relative refs
        let end = CellRef::new(0, Coord::new(5, 1, false, false)); // relative refs
        let range_def = NamedDefinition::Range(RangeRef::new(start, end));
        editor
            .define_name("MyData", range_def, NameScope::Workbook)
            .expect("Should define range name");

        // Insert rows before the range
        editor.insert_rows(0, 2, 1).expect("Should insert row");
    }

    // Verify the range was adjusted
    let resolved = graph.resolve_name("MyData", 0).unwrap();
    match resolved {
        NamedDefinition::Range(range_ref) => {
            // Range should have shifted down by 1 row
            assert_eq!(
                range_ref.start.coord.row(),
                4,
                "Start should shift from 3 to 4"
            );
            assert_eq!(range_ref.end.coord.row(), 6, "End should shift from 5 to 6");
        }
        _ => panic!("Expected Range definition"),
    }
}

#[test]
fn test_vertex_editor_change_log() {
    let mut graph = DependencyGraph::new();
    let mut log = crate::engine::graph::editor::change_log::ChangeLog::new();

    {
        let mut editor =
            crate::engine::graph::editor::VertexEditor::with_logger(&mut graph, &mut log);

        // Perform operations
        editor
            .define_name_for_cell("Name1", "Sheet1", 1, 1, NameScope::Workbook)
            .expect("Should define name");

        let new_def = NamedDefinition::Cell(CellRef::new(0, Coord::from_excel(2, 2, true, true)));
        editor
            .update_name("Name1", new_def, NameScope::Workbook)
            .expect("Should update name");

        editor
            .delete_name("Name1", NameScope::Workbook)
            .expect("Should delete name");
    }

    // Check change log
    let changes = log.events();
    assert_eq!(changes.len(), 3, "Should have 3 change events");

    // Verify event types
    use crate::engine::graph::editor::change_log::ChangeEvent;
    assert!(matches!(&changes[0], ChangeEvent::DefineName { .. }));
    assert!(matches!(&changes[1], ChangeEvent::UpdateName { .. }));
    assert!(matches!(&changes[2], ChangeEvent::DeleteName { .. }));
}
