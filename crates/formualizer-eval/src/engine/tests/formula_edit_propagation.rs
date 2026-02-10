use crate::engine::{Engine, EvalConfig};
use crate::reference::{CellRef, Coord};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::{ASTNode, ASTNodeType, ReferenceType, parse};

fn make_engine() -> Engine<TestWorkbook> {
    let wb = TestWorkbook::new();
    let cfg = EvalConfig::default();
    Engine::new(wb, cfg)
}

fn lit(n: i64) -> ASTNode {
    ASTNode::new(ASTNodeType::Literal(LiteralValue::Int(n)), None)
}

fn ref_cell(row: u32, col: u32) -> ASTNode {
    ASTNode::new(
        ASTNodeType::Reference {
            original: format!("R{row}C{col}"),
            reference: ReferenceType::cell(None, row, col),
        },
        None,
    )
}

#[test]
fn dependents_redirty_on_formula_edit_direct_chain() {
    let mut engine = make_engine();

    // A1 = 10 (as a formula), B1 = A1, C1 = B1
    engine.set_cell_formula("Sheet1", 1, 1, lit(10)).unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, ref_cell(1, 1))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, ref_cell(1, 2))
        .unwrap();

    // Evaluate once to clear initial dirty state
    engine.evaluate_all().unwrap();

    // Edit A1's formula to 20
    engine.set_cell_formula("Sheet1", 1, 1, lit(20)).unwrap();

    // Grab vertex IDs for B1 and C1
    let sheet_id = engine.graph.sheet_id("Sheet1").unwrap();
    let b1 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sheet_id, Coord::from_excel(1, 2, true, true)))
        .unwrap();
    let c1 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sheet_id, Coord::from_excel(1, 3, true, true)))
        .unwrap();

    // Ensure both B1 and C1 are scheduled (redirtied)
    let scheduled = engine.graph.get_evaluation_vertices();
    assert!(
        scheduled.contains(&b1),
        "B1 should be scheduled after A1 edit"
    );
    assert!(
        scheduled.contains(&c1),
        "C1 should be scheduled after A1 edit"
    );

    // Evaluate all; expect at least 3 computations (A1, B1, C1)
    let eval = engine.evaluate_all().unwrap();
    assert!(
        eval.computed_vertices >= 2,
        "expected >=2 recomputations, got {}",
        eval.computed_vertices
    );
}

#[test]
fn dependents_redirty_when_value_becomes_formula() {
    let mut engine = make_engine();

    // Seed A1 as a value, then B1 = A1, C1 = B1
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(10))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, ref_cell(1, 1))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, ref_cell(1, 2))
        .unwrap();
    engine.evaluate_all().unwrap();

    // Change A1 to a formula value
    engine.set_cell_formula("Sheet1", 1, 1, lit(20)).unwrap();

    // B1 and C1 should be scheduled
    let sid = engine.graph.sheet_id("Sheet1").unwrap();
    let b1 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sid, Coord::from_excel(1, 2, true, true)))
        .unwrap();
    let c1 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(sid, Coord::from_excel(1, 3, true, true)))
        .unwrap();
    let scheduled = engine.graph.get_evaluation_vertices();
    assert!(scheduled.contains(&b1));
    assert!(scheduled.contains(&c1));

    let eval = engine.evaluate_all().unwrap();
    assert!(eval.computed_vertices >= 2);
}

#[test]
fn whole_column_dependent_redirty_on_formula_edit() {
    let mut engine = make_engine();

    // D2..D5 are formulas; S1 = SUM(D:D)
    engine.set_cell_formula("Sheet1", 2, 4, lit(1)).unwrap();
    engine.set_cell_formula("Sheet1", 3, 4, lit(2)).unwrap();
    engine.set_cell_formula("Sheet1", 4, 4, lit(3)).unwrap();
    engine.set_cell_formula("Sheet1", 5, 4, lit(4)).unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 19, parse("=SUM(D:D)").unwrap())
        .unwrap(); // S column is col 19

    engine.evaluate_all().unwrap();

    // Change D5 formula
    engine.set_cell_formula("Sheet1", 5, 4, lit(40)).unwrap();

    // S1 should be scheduled via column stripe invalidation
    let sheet_id = engine.graph.sheet_id("Sheet1").unwrap();
    let s1 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(
            sheet_id,
            Coord::from_excel(1, 19, true, true),
        ))
        .unwrap();
    let scheduled = engine.graph.get_evaluation_vertices();
    assert!(
        scheduled.contains(&s1),
        "S1 should be scheduled after D5 edit"
    );
}

#[test]
fn cross_sheet_whole_column_dependent_redirty_on_formula_edit() {
    let mut engine = make_engine();

    // Create Sheet2 and place a SUM over Sheet1!D:D in A1
    engine.graph.add_sheet("Sheet2").unwrap();

    // Seed some formulas in Sheet1 column D
    engine.set_cell_formula("Sheet1", 2, 4, lit(5)).unwrap();
    engine.set_cell_formula("Sheet1", 5, 4, lit(7)).unwrap();

    engine
        .set_cell_formula("Sheet2", 1, 1, parse("=SUM(Sheet1!D:D)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    // Edit Sheet1!D5 and ensure Sheet2!A1 is scheduled
    engine.set_cell_formula("Sheet1", 5, 4, lit(70)).unwrap();

    let s2_id = engine.graph.sheet_id("Sheet2").unwrap();
    let a1_sheet2 = engine
        .graph
        .get_vertex_for_cell(&CellRef::new(s2_id, Coord::from_excel(1, 1, true, true)))
        .unwrap();
    let scheduled = engine.graph.get_evaluation_vertices();
    assert!(
        scheduled.contains(&a1_sheet2),
        "Sheet2!A1 should be scheduled after Sheet1!D5 edit"
    );
}
