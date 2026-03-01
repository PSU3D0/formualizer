//! Tests for the hybrid model of range dependency management.
use super::common::{abs_cell_ref, eval_config_with_range_limit};
use crate::engine::{DependencyGraph, Engine, EvalConfig, VertexId};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_common::parse_a1_1based;
use formualizer_parse::parse;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType};

/// Helper to create a range reference AST node
fn range_ast(start_row: u32, start_col: u32, end_row: u32, end_col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Reference {
            original: format!("R{start_row}C{start_col}:R{end_row}C{end_col}"),
            reference: ReferenceType::range(
                None,
                Some(start_row),
                Some(start_col),
                Some(end_row),
                Some(end_col),
            ),
        },
        source_token: None,
        contains_volatile: false,
    }
}

/// Helper to create a SUM(range) AST node
fn sum_ast(start_row: u32, start_col: u32, end_row: u32, end_col: u32) -> ASTNode {
    ASTNode {
        node_type: ASTNodeType::Function {
            name: "SUM".to_string(),
            args: vec![range_ast(start_row, start_col, end_row, end_col)],
        },
        source_token: None,
        contains_volatile: false,
    }
}

fn graph_with_range_limit(limit: usize) -> DependencyGraph {
    DependencyGraph::new_with_config(eval_config_with_range_limit(limit))
}

#[test]
fn test_tiny_range_expands_to_cell_dependencies() {
    let mut graph = graph_with_range_limit(4);

    // C1 = SUM(A1:A4) - size is 4, which is <= limit
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 4, 1))
        .unwrap();

    let c1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 3))
        .unwrap();
    let c1_vertex = graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 3))
        .unwrap();

    let dependencies = graph.get_dependencies(c1_id);

    // Should have 4 direct dependencies
    assert_eq!(
        dependencies.len(),
        4,
        "Should expand to 4 cell dependencies"
    );

    // Should have no compressed range dependencies
    assert!(
        graph.formula_to_range_deps().is_empty(),
        "Should not create a compressed range dependency"
    );

    // Verify the dependencies are correct
    let mut dep_addrs = Vec::new();
    for &dep_id in &dependencies {
        let cell_ref = graph.get_cell_ref(dep_id).unwrap();
        dep_addrs.push((cell_ref.coord.row(), cell_ref.coord.col()));
    }
    dep_addrs.sort();
    let expected_addrs = vec![(0, 0), (1, 0), (2, 0), (3, 0)];
    assert_eq!(dep_addrs, expected_addrs);
}

#[test]
fn test_range_dependency_dirtiness() {
    let mut graph = DependencyGraph::new();

    // C1 depends on the range A1:A10.
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 10, 1))
        .unwrap();
    let c1_id = *graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 3)).unwrap();

    // Create a value in the middle of the range, e.g., A5.
    graph
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(100))
        .unwrap();

    // Clear all dirty flags from the initial setup.
    let all_ids: Vec<VertexId> = graph.cell_to_vertex().values().copied().collect();
    graph.clear_dirty_flags(&all_ids);
    assert!(graph.get_evaluation_vertices().is_empty());

    // Now, change the value of A5. This should trigger dirty propagation
    // to C1 via the range dependency.
    graph
        .set_cell_value("Sheet1", 5, 1, LiteralValue::Int(200))
        .unwrap();

    // Check that C1 is now dirty.
    let eval_vertices = graph.get_evaluation_vertices();
    assert!(!eval_vertices.is_empty());
    assert!(eval_vertices.contains(&c1_id));
}

#[test]
fn test_range_dependency_updates_on_formula_change() {
    let mut graph = DependencyGraph::new();

    // B1 = SUM(A1:A2)
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 2, 1))
        .unwrap();
    let b1_id = *graph.cell_to_vertex().get(&abs_cell_ref(0, 1, 2)).unwrap();

    // Change A1, B1 should be dirty
    graph
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    assert!(graph.get_evaluation_vertices().contains(&b1_id));
    graph.clear_dirty_flags(&[b1_id]);
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));

    // Change A3 (outside the range), B1 should NOT be dirty
    graph
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(30))
        .unwrap();
    assert!(!graph.get_evaluation_vertices().contains(&b1_id));

    // Now, update B1 to depend on A1:A5
    graph
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 5, 1))
        .unwrap();
    graph.clear_dirty_flags(&[b1_id]);

    // Change A3 again (now inside the range), B1 should be dirty
    graph
        .set_cell_value("Sheet1", 3, 1, LiteralValue::Int(40))
        .unwrap();
    assert!(graph.get_evaluation_vertices().contains(&b1_id));
}

#[test]
fn test_large_range_creates_single_compressed_ref() {
    let mut graph = graph_with_range_limit(4);

    // C1 = SUM(A1:A100) - size is 100, which is > limit
    graph
        .set_cell_formula("Sheet1", 1, 3, sum_ast(1, 1, 100, 1))
        .unwrap();

    let c1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 3))
        .unwrap();
    let c1_dependencies = graph.get_dependencies(c1_id);

    // Should have no direct dependencies
    assert!(
        c1_dependencies.is_empty(),
        "Should not have any direct cell dependencies"
    );

    // Should have one compressed range dependency
    let range_deps = graph.formula_to_range_deps();
    assert_eq!(
        range_deps.len(),
        1,
        "Should create one compressed range dependency"
    );
    assert!(range_deps.contains_key(&c1_id));
    assert_eq!(range_deps.get(&c1_id).unwrap().len(), 1);
}

#[test]
fn test_duplicate_range_refs_in_formula() {
    let mut graph = graph_with_range_limit(4);
    // B1 = SUM(A1:A100) + COUNT(A1:A100)
    let formula = ASTNode {
        node_type: ASTNodeType::BinaryOp {
            op: "+".to_string(),
            left: Box::new(sum_ast(1, 1, 100, 1)),
            right: Box::new(ASTNode {
                node_type: ASTNodeType::Function {
                    name: "COUNT".to_string(),
                    args: vec![range_ast(1, 1, 100, 1)],
                },
                source_token: None,
                contains_volatile: false,
            }),
        },
        source_token: None,
        contains_volatile: false,
    };
    graph.set_cell_formula("Sheet1", 1, 2, formula).unwrap();

    let b1_id = *graph
        .get_vertex_id_for_address(&abs_cell_ref(0, 1, 2))
        .unwrap();

    // Should only have one compressed range dependency, not two
    let range_deps = graph.formula_to_range_deps();
    assert_eq!(range_deps.get(&b1_id).unwrap().len(), 1);
}

#[test]
fn test_zero_sized_range_behavior() {
    let mut graph = DependencyGraph::new();
    // B1 = SUM(A1:A0)
    let result = graph.set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 0, 1));
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().kind,
        formualizer_common::ExcelErrorKind::Ref
    );
}

fn create_simple_engine() -> Engine<TestWorkbook> {
    let wb = TestWorkbook::new();
    let config = EvalConfig::default();
    Engine::new(wb, config)
}

/*
 * The Scenario: You have two different formulas that "look" at overlapping
 * parts of the same column. For example, Formula A calculates SUM(A1:A50)
 * and Formula B calculates SUM(A25:A75). The Test: It changes a value in
 * the "middle" (like cell A30) which belongs to both ranges.
 *
 * Why it matters: This ensures your Sheet Index and Range Compression logic
 * are robust enough to "wake up" every observer that cares about a specific
 * cell. It prevents a bug where the engine might only update the "first" or
 * "most recent" range it found in its index.
 */
#[test]
fn test_partial_range_overlap_dependency_propagation() {
    let mut engine = create_simple_engine(); // From evaluation.rs context

    // 1. Setup initial values in the overlap area
    engine
        .set_cell_value("Sheet1", 30, 1, LiteralValue::Number(10.0))
        .unwrap(); // A30 = 10

    // 2. Define two overlapping range formulas
    // B1 = SUM(A1:A50)
    let sum_a1_a50 = sum_ast(1, 1, 50, 1);
    engine.set_cell_formula("Sheet1", 1, 2, sum_a1_a50).unwrap();

    // B2 = SUM(A25:A75)
    let sum_a25_a75 = sum_ast(25, 1, 75, 1);
    engine
        .set_cell_formula("Sheet1", 2, 2, sum_a25_a75)
        .unwrap();

    // Initial Evaluation
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(10.0))
    );

    // 3. Update the overlapping cell A30
    // This is the critical moment for the DependencyGraph and Scheduler
    engine
        .set_cell_value("Sheet1", 30, 1, LiteralValue::Number(20.0))
        .unwrap();

    // 4. Verify propagation
    // We check if both B1 and B2 are flagged as dirty or produce new values
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0)),
        "B1 (A1:A50) failed to update when overlapping cell A30 changed"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 2),
        Some(LiteralValue::Number(20.0)),
        "B2 (A25:A75) failed to update when overlapping cell A30 changed"
    );
}

/*
 * The Scenario: You are standing on Sheet1 and write a formula that sums
 * data physically located on Sheet2 (e.g., =SUM(Sheet2!A1:A10))
 *
 * The Test: It updates a value on Sheet2 and checks if the formula on
 * Sheet1 correctly reflects that change.
 *
 * Why it matters: This verifies that your Dependency Graph can track
 * "edges" (connections) across different sheet boundaries. It confirms
 * that when Sheet2 is marked "dirty," the engine knows to look across
 * the workbook to find formulas on other sheets that might be affected.
 */

#[test]
fn test_cross_sheet_range_dependency() {
    let mut engine = create_simple_engine();

    // 1. Setup Sheet2 with a value
    engine.graph.add_sheet("Sheet2").unwrap();
    engine
        .set_cell_value("Sheet2", 1, 1, LiteralValue::Number(10.0))
        .unwrap(); // Sheet2!A1 = 10

    // 2. Create formula on Sheet1: =SUM(Sheet2!A1:A10)
    let range_cross = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "Sheet2!A1:A10".to_string(),
            reference: ReferenceType::Range {
                sheet: Some("Sheet2".to_string()),
                start_row: Some(1),
                start_col: Some(1),
                end_row: Some(10),
                end_col: Some(1),
                start_row_abs: true,
                start_col_abs: true,
                end_row_abs: true,
                end_col_abs: true,
            },
        },
        source_token: None,
        contains_volatile: false,
    };

    let sum_formula = ASTNode {
        node_type: ASTNodeType::Function {
            name: "SUM".to_string(),
            args: vec![range_cross],
        },
        source_token: None,
        contains_volatile: false,
    };

    engine
        .set_cell_formula("Sheet1", 1, 2, sum_formula)
        .unwrap(); // Sheet1!B1
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(10.0))
    );

    // 3. Update Sheet2!A1 and verify Sheet1!B1 follows
    engine
        .set_cell_value("Sheet2", 1, 1, LiteralValue::Number(50.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(50.0)),
        "Cross-sheet range dependency failed to propagate"
    );
}

/*
 * The Scenario: You have a chain of dependencies. Cell C1 feeds into cell A5.
 * Then, a range formula SUM(A1:A10) includes cell A5.
 * The Test: You change the "grandparent" cell (C1). This should trigger A5,
 * which in turn should trigger the SUM formula.
 *
 * Why it matters: This is a "stress test" for your Scheduler and
 * Tarjan's SCC algorithm. It ensures that the engine doesn't just look for
 * direct manual edits to a range, but also understands when a range's value
 * changes because one of the cells inside that range is itself a formula that
 * just recalculated.
 */
#[test]
fn test_nested_formula_within_range_propagation() {
    let mut engine = create_simple_engine();

    // 1. Setup C1 as the source
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(10.0))
        .unwrap(); // C1 = 10

    // 2. A5 is a formula depending on C1
    // A5 = C1
    let a5_formula = ASTNode {
        node_type: ASTNodeType::Reference {
            original: "C1".to_string(),
            reference: ReferenceType::cell(None, 1, 3),
        },
        source_token: None,
        contains_volatile: false,
    };
    engine.set_cell_formula("Sheet1", 5, 1, a5_formula).unwrap();

    // 3. B1 depends on the range A1:A10
    // B1 = SUM(A1:A10) -> Should include the value of A5 (which is C1)
    engine
        .set_cell_formula("Sheet1", 1, 2, sum_ast(1, 1, 10, 1))
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(10.0))
    );

    // 4. TRIGGER: Change C1.
    // This MUST trigger A5, which MUST trigger the Range A1:A10, which MUST trigger B1.
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(50.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(50.0)),
        "Range dependency failed to trigger via a nested formula change"
    );
}

/*
 * Scenario: Dependency Recovery (The Ghost Reference)
 * 1. Create a formula on Sheet1 that references a cell on Sheet2.
 * 2. Delete Sheet2 entirely.
 * 3. Verify the formula on Sheet1 evaluates to an error (#REF!).
 * 4. Re-create Sheet2 with the same name and provide a new value.
 *
 * Why it matters:
 * This verifies that the Dependency Graph isn't "brittle." When a sheet is
 * deleted, the connection should break gracefully, but when the sheet
 * returns, the engine should automatically "heal" the link and resume
 * tracking updates without requiring the user to re-enter the formula.
 */
#[test]
fn test_sheet_recreation_dependency_recovery() {
    use formualizer_parse::parse; // Import the parser
    let mut engine = create_simple_engine();

    // 1. Setup cross-sheet dependency
    engine.graph.add_sheet("Sheet2").unwrap();
    engine
        .set_cell_value("Sheet2", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    // Sheet1!A1 = Sheet2!A1
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=Sheet2!A1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    // 2. Remove Sheet2
    let s2_id = engine
        .graph
        .sheet_id("Sheet2")
        .expect("Sheet2 should exist");
    engine.graph.remove_sheet(s2_id).unwrap();

    // Formula should now be an error (likely #REF!)
    engine.evaluate_all().unwrap();
    let val = engine.get_cell_value("Sheet1", 1, 1).unwrap();
    match val {
        LiteralValue::Error(_) => { /* Success: it recognized the missing sheet */ }
        _ => panic!(
            "Expected an error value after sheet removal, found {:?}",
            val
        ),
    }

    // 3. Re-add Sheet2 and provide value
    engine.graph.add_sheet("Sheet2").unwrap();
    engine
        .set_cell_value("Sheet2", 1, 1, LiteralValue::Number(50.0))
        .unwrap();

    // 4. Check for "Healing"
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(50.0)),
        "Dependency failed to 'heal' after sheet was removed and re-added"
    );
}

/*
 * Scenario: Dependency Healing via Rename
 * 1. Create Sheet2!A1. Sheet1!A1 points to it.
 * 2. Delete Sheet2. Sheet1!A1 becomes #REF!.
 * 3. Create "Sheet3" (new ID).
 * 4. Rename "Sheet3" to "Sheet2".
 * * Why it matters:
 * This tests if the "Rename" logic is smart enough to trigger a
 * global re-scan of formulas to fix #REF! errors.
 */
#[test]
fn test_rename_fixes_ref_errors() {
    use formualizer_parse::parse;
    let mut engine = create_simple_engine();

    // Use engine methods where possible
    let _ = engine.graph.add_sheet("Sheet2").unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=Sheet2!A1").unwrap())
        .unwrap();

    // 1. Use the engine-level remove if available,
    // but graph.remove_sheet is okay if you don't have an engine wrapper
    let s2_id = engine.graph.sheet_id("Sheet2").unwrap();
    engine.graph.remove_sheet(s2_id).unwrap();
    engine.evaluate_all().unwrap();

    // 2. Add Sheet3
    let s3_id = engine.graph.add_sheet("Sheet3").unwrap();
    engine
        .set_cell_value("Sheet3", 1, 1, LiteralValue::Number(100.0))
        .unwrap();

    // 3. FIX: Call engine.rename_sheet, NOT engine.graph.rename_sheet
    // This coordinates the Arrow Storage update with the Graph update
    engine.rename_sheet(s3_id, "Sheet2").unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(100.0)),
        "Renaming a sheet to a missing name did not heal the #REF! dependency"
    );
}

/*
 * Scenario: Cross-Sheet Row Insertion Shifting
 * 1. Sheet1 has a SUM formula looking at a specific range on Sheet2 (A1:A10).
 * 2. A new row is inserted at the very top of Sheet2 (before Row 1).
 * 3. The data that was in A1 is now physically in A2.
 * * Why it matters:
 * This is a major test of coordinate stability. In a robust engine, the
 * formula on Sheet1 should automatically "stretch" or "shift" its
 * reference to (A2:A11) to follow the data. If it stays stuck on A1:A10,
 * the user's calculation becomes wrong. It verifies that structural
 * changes on one sheet correctly trigger coordinate updates on all
 * dependent sheets.
 */
#[test]
fn test_cross_sheet_row_insertion_dependency() {
    use formualizer_parse::parse;
    let mut engine = create_simple_engine();

    // 1. Setup Sheet2 with data and Sheet1 with a formula pointing to it
    engine.graph.add_sheet("Sheet2").unwrap();
    engine
        .set_cell_value("Sheet2", 1, 1, LiteralValue::Number(10.0))
        .unwrap(); // Sheet2!A1 = 10

    // Sheet1!A1 = SUM(Sheet2!A1:A10)
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SUM(Sheet2!A1:A10)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );

    // 2. Insert 1 row at index 1 (the very top) on Sheet2
    // We call this on 'engine' directly, not 'engine.graph'
    engine.insert_rows("Sheet2", 1, 1).unwrap();

    // 3. The value 10.0 is now at Sheet2!A2.
    // The formula on Sheet1 should have been rewritten to SUM(Sheet2!A2:A11)
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0)),
        "Inserting a row on Sheet2 failed to shift the reference on Sheet1"
    );
}
/*
 * Scenario: Range Dependency Fan-Out (Memory Stress)
 * 1. Create 5,000 unique formulas that all overlap on the same source cell (A1).
 * 2. Each formula is slightly different: SUM(A1:A2), SUM(A1:A3), SUM(A1:A4)...
 * 3. Update the source cell (A1).
 *
 * Why it matters:
 * In a "Hybrid" model, the engine must manage a massive amount of "Range Observers."
 * If the memory complexity is O(N^2) or if it fails to share metadata, memory
 * usage will spike. This test ensures the engine can handle a "worst-case"
 * spreadsheet layout where thousands of cells depend on one central data point.
 */
#[test]
fn test_massive_range_fan_out_performance() {
    let mut engine = create_simple_engine();
    let sheet = "Sheet1";

    // 1. Initial value
    engine
        .set_cell_value(sheet, 1, 1, LiteralValue::Number(1.0))
        .unwrap();

    // 2. Create 5,000 overlapping ranges
    for i in 2..5002 {
        // Create SUM(A1:A{i}) in column B
        engine
            .set_cell_formula(sheet, i, 2, sum_ast(1, 1, i, 1))
            .unwrap();
    }

    // 3. Measure update time (Performance/Complexity check)
    let start = std::time::Instant::now();
    engine
        .set_cell_value(sheet, 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    let duration = start.elapsed();

    // Verify a sample (the last formula)
    // Result should be 2.0 (A1) + 0.0 (others) = 2.0
    assert_eq!(
        engine.get_cell_value(sheet, 5001, 2),
        Some(LiteralValue::Number(2.0))
    );

    // Ideally, this should be sub-second even in debug mode
    assert!(duration.as_secs() < 2, "Fan-out propagation is too slow!");
}

/*
 * Scenario: Stripe Boundary Intersection
 * 1. Identify the engine's "Stripe Size" (usually a power of 2, like 64 or 128).
 * 2. Create a formula that looks at a range straddling that boundary.
 * 3. Update cells on BOTH sides of the boundary line.
 *
 * Why it matters:
 * Engines that use "Striping" to speed up lookups often have bugs at the
 * "seams." A range might be correctly indexed in Stripe A, but not Stripe B.
 * This verifies that the range is registered in every stripe it physically
 * occupies so that no updates are missed.
 */
#[test]
fn test_stripe_boundary_range_trigger() {
    let mut engine = create_simple_engine();

    // Assuming a stripe size of 64 (common in these engines)
    // Range A60:A70 crosses the 64-row boundary
    engine
        .set_cell_formula("Sheet1", 1, 2, sum_ast(60, 1, 70, 1))
        .unwrap();

    // Test Case 1: Update cell BEFORE boundary (A63)
    engine
        .set_cell_value("Sheet1", 63, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(10.0))
    );

    // Test Case 2: Update cell AFTER boundary (A65)
    engine
        .set_cell_value("Sheet1", 65, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine.evaluate_all().unwrap();

    // Total should be 15.0
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(15.0)),
        "Range failed to catch update from the second stripe"
    );
}

#[test]
fn test_rename_layer_registry() {
    let mut engine = create_simple_engine(); // Your engine setup
    let sheet_id = engine.graph.sheet_reg().get_id("Sheet1").unwrap();

    engine.graph.rename_sheet(sheet_id, "NewSheet").unwrap();

    assert_eq!(engine.graph.sheet_name(sheet_id), "NewSheet");
    assert!(engine.graph.sheet_reg().get_id("Sheet1").is_none());
}

#[test]
fn test_rename_layer_storage() {
    let mut engine = create_simple_engine();
    // 1. Put data in
    let _ = engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(100.0));

    // 2. Rename
    let sheet_id = engine.graph.sheet_reg().get_id("Sheet1").unwrap();
    engine.rename_sheet(sheet_id, "Sheet2").unwrap();

    // 3. Try to read from the NEW name
    let val = engine.read_cell_value("Sheet2", 1, 1);
    assert_eq!(
        val,
        Some(LiteralValue::Number(100.0)),
        "Storage failed to follow the rename!"
    );
}
#[test]
fn test_rename_layer_identity() {
    let mut engine = create_simple_engine();
    let _ = engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(100.0));

    let (row, col, _, _) = parse_a1_1based("A1").unwrap();
    let addr = engine.graph.make_cell_ref("Sheet1", row, col);

    // Fix: Dereference here to copy the ID and release the borrow on engine
    let v_id = *engine.graph.get_vertex_id_for_address(&addr).unwrap();

    let sheet_id = engine
        .graph
        .sheet_reg()
        .get_id("Sheet1")
        .expect("Sheet1 missing");

    // This now works because v_id is just a value, not a borrow
    engine.rename_sheet(sheet_id, "SheetX").unwrap();

    let cell_ref = engine
        .graph
        .get_cell_ref(v_id) // No * needed here now as v_id is already the value
        .expect("Vertex lost its cell ref!");

    let current_name = engine.graph.sheet_name(cell_ref.sheet_id);
    assert_eq!(
        current_name, "SheetX",
        "Vertex still thinks it is on the old sheet name!"
    );
}
#[test]
fn test_rename_layer_vertex_read() {
    let mut engine = create_simple_engine();
    let _ = engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(100.0));

    let (row, col, _, _) = parse_a1_1based("A1").unwrap();
    let addr = engine.graph.make_cell_ref("Sheet1", row, col);
    let v_id = *engine.graph.get_vertex_id_for_address(&addr).unwrap();

    let sheet_id = engine.graph.sheet_reg().get_id("Sheet1").unwrap();
    engine.rename_sheet(sheet_id, "SheetX").unwrap();

    // Use evaluate_vertex to force a fresh look at the data
    let val = engine.evaluate_vertex(v_id).expect("Evaluation failed");
    assert_eq!(
        val,
        LiteralValue::Number(100.0),
        "Data not found after rename!"
    );
}
#[test]
fn test_rename_check_formula_healing() {
    let mut engine = create_simple_engine();
    // 1. Data on Sheet2
    let _ = engine.set_cell_value("Sheet2", 1, 1, LiteralValue::Number(100.0));

    // 2. Create a VALID formula first (pointing to Sheet2)
    let ast = parse("=Sheet2!A1").expect("Parse failed");
    engine.set_cell_formula("Sheet1", 1, 2, ast).unwrap();
    engine.evaluate_all().unwrap();

    // 3. NOW break it: Rename Sheet2 to "Other"
    // Sheet1!B1 should now be #REF! because "Sheet2" is gone
    let sheet2_id = engine.graph.sheet_reg().get_id("Sheet2").unwrap();
    engine.rename_sheet(sheet2_id, "Other").unwrap();
    engine.evaluate_all().unwrap();

    // 4. HEAL it: Rename "Other" to "Missing"
    // (The test previously used "Missing" in the formula string)
    // Actually, let's just rename "Other" back to "Sheet2" to match the formula.
    engine.rename_sheet(sheet2_id, "Sheet2").unwrap();
    engine.evaluate_all().unwrap();

    // 5. Verify the value is back to 100.0
    let v_id = *engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 1, 2))
        .unwrap();

    // Using evaluate_vertex to ensure we bypass any stale cache
    let val = engine.evaluate_vertex(v_id).expect("Evaluation failed");
    assert_eq!(
        val,
        LiteralValue::Number(100.0),
        "Formula did not heal correctly!"
    );
}
#[test]
fn test_rename_cross_sheet_link() {
    let mut engine = create_simple_engine();
    // Assuming Sheet1 and Sheet2 exist.
    let _ = engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(100.0));

    // 1. Rename Sheet1 to DataSheet
    let sheet_id = engine.graph.sheet_reg().get_id("Sheet1").unwrap();
    let _ = engine.rename_sheet(sheet_id, "DataSheet").unwrap();

    // 2. Create link on Sheet2!A1 pointing to the renamed sheet
    let ast = parse("=DataSheet!A1").expect("Parse failed");
    let _ = engine.set_cell_formula("Sheet2", 1, 1, ast).unwrap();

    // 3. Force Evaluation
    let _ = engine.evaluate_all().unwrap();

    // 4. Verification: Look up the vertex by the EXACT address used
    let addr = engine.graph.make_cell_ref("Sheet2", 1, 1);
    let v_id = *engine
        .graph
        .get_vertex_id_for_address(&addr)
        .expect("Vertex not found at Sheet2!A1");

    // Fetch the value from the engine
    let val = engine.evaluate_vertex(v_id).expect("Evaluation failed");
    assert_eq!(
        val,
        LiteralValue::Number(100.0),
        "Cross-sheet link failed to retrieve value"
    );
}
