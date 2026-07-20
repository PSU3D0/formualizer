//! Ticket 611: Structural invalidation in canonical (Arrow-truth) mode.
//!
//! These tests ensure structural operations that induce #REF! correctly:
//! - preserve invalid references as ordinary AST error literals where an expression survives;
//! - propagate dirtying so downstream dependents recompute;
//! - mirror results into Arrow truth so `Engine::get_cell_value` reflects #REF!.

use crate::engine::eval::Engine;
use crate::test_workbook::TestWorkbook;
use formualizer_common::ExcelErrorKind;
use formualizer_parse::LiteralValue;
use formualizer_parse::parser::parse;
use formualizer_parse::pretty::canonical_formula;

use super::common::{abs_cell_ref, arrow_eval_config};

#[test]
fn canonical_remove_sheet_marks_ref_and_propagates_to_downstream_dependents() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    let data_id = engine.add_sheet("Data").unwrap();
    engine
        .set_cell_value("Data", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    // Sheet1!B1 = Data!A1 * 2
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=Data!A1*2").unwrap())
        .unwrap();
    // Sheet1!C1 = B1 + 1 (downstream dependent)
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=B1+1").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(21.0))
    );

    let sid = engine.graph.sheet_id("Sheet1").unwrap();
    let b1_vid = engine
        .graph
        .get_vertex_for_cell(&abs_cell_ref(sid, 1, 2))
        .unwrap();

    engine.remove_sheet(data_id).unwrap();

    // (a) structural invalidation is recorded
    assert!(engine.graph.is_ref_error(b1_vid));

    engine.evaluate_all().unwrap();

    // (b) Arrow-truth values updated after evaluation
    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        other => panic!("expected Sheet1!B1 to be #REF!, got {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 1, 3) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        other => panic!("expected Sheet1!C1 to be #REF! (propagated), got {other:?}"),
    }
}

#[test]
fn canonical_delete_column_marks_ref_and_propagates_to_downstream_dependents() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    // C1 = A1 * 2
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=A1*2").unwrap())
        .unwrap();
    // D1 = C1 + 1
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=C1+1").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(20.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(21.0))
    );

    // Delete column A; C1 moves to B1.
    engine.delete_columns("Sheet1", 1, 1).unwrap();
    // The original A1 value is deleted.
    assert_eq!(engine.get_cell_value("Sheet1", 1, 1), None);
    engine.evaluate_all().unwrap();

    // (b) Arrow-truth values updated after evaluation (including downstream dependents)
    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        other => panic!("expected Sheet1!B1 to be #REF!, got {other:?}"),
    }
    match engine.get_cell_value("Sheet1", 1, 3) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        other => panic!("expected Sheet1!C1 to be #REF! (propagated), got {other:?}"),
    }
}

#[test]
fn canonical_rename_sheet_rewrites_sheet_locator_and_recomputes() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    let sheet2_id = engine.add_sheet("Sheet2").unwrap();
    engine
        .set_cell_value("Sheet2", 1, 1, LiteralValue::Number(5.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=Sheet2!A1+10").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(15.0))
    );

    engine.rename_sheet(sheet2_id, "DataSheet").unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(15.0))
    );

    // Lock down that the stored AST was rewritten from Sheet2 -> DataSheet.
    let (ast_opt, _) = engine.get_cell("Sheet1", 2, 1).expect("cell exists");
    let ast = ast_opt.expect("formula exists");
    let f = canonical_formula(&ast);
    assert!(
        f.contains("DataSheet!A1"),
        "expected rewritten formula to reference DataSheet, got: {f}"
    );
    assert!(
        !f.contains("Sheet2!A1"),
        "expected old sheet name removed from formula, got: {f}"
    );
}

#[test]
fn canonical_insert_columns_shifts_values_and_formulas_and_rewrites_references() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(2.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=A1+B1").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(3.0))
    );

    // Insert a column before A (1-based API).
    engine.insert_columns("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();

    // Old values shift right: A1->B1, B1->C1.
    assert_eq!(engine.get_cell_value("Sheet1", 1, 1), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(2.0))
    );

    // Formula cell shifts right: C1->D1 and should still compute 3.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(3.0))
    );

    // Also lock down that the formula was rewritten to follow the moved cells.
    let (ast_opt, _) = engine.get_cell("Sheet1", 1, 4).expect("D1 exists");
    let ast = ast_opt.expect("D1 has formula");
    let f = canonical_formula(&ast);
    assert!(
        f.contains("B1") && f.contains("C1"),
        "expected formula to reference B1 and C1 after insert, got: {f}"
    );
    assert!(
        !f.contains("A1"),
        "expected old reference A1 to be rewritten, got: {f}"
    );
}

#[test]
fn canonical_insert_rows_shifts_values_and_formulas_and_rewrites_references() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=A1+A2").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(30.0))
    );

    // Insert a row before row 1.
    engine.insert_rows("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();

    // Values shift down: A1->A2, A2->A3.
    assert_eq!(engine.get_cell_value("Sheet1", 1, 1), None);
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(20.0))
    );

    // Formula shifts down: A3->A4 and should still compute 30 by referencing A2+A3.
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(30.0))
    );
    let (ast_opt, _) = engine.get_cell("Sheet1", 4, 1).expect("A4 exists");
    let ast = ast_opt.expect("A4 has formula");
    let f = canonical_formula(&ast);
    assert!(
        f.contains("A2") && f.contains("A3"),
        "expected formula to reference A2 and A3 after insert, got: {f}"
    );
    assert!(
        !f.contains("A1+A2") && !f.contains("A1"),
        "expected old references to be rewritten, got: {f}"
    );
}

#[test]
fn canonical_insert_rows_moves_fully_absolute_reference_with_its_target() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    // A2 = $A$1. After inserting a row before row 1, the referenced value
    // physically moves to A2 and the formula cell shifts to A3. Policy
    // pinned on issue #168: absolute references TRACK structural
    // inserts/deletes (the `$` pins copy/fill relocation only), so the
    // formula must now reference $A$2 and keep computing 99.
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(99.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 2, 1, parse("=$A$1").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(99.0))
    );

    engine.insert_rows("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();

    // Old value moved to A2.
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(99.0))
    );

    // Formula moved to A3 and its absolute reference tracked the target.
    let (ast_opt, _) = engine.get_cell("Sheet1", 3, 1).expect("A3 exists");
    let ast = ast_opt.expect("A3 has formula");
    let f = canonical_formula(&ast);
    assert!(
        f.contains("$A$2"),
        "expected absolute ref to track its moved target, got: {f}"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(99.0))
    );
}

#[test]
fn canonical_delete_columns_shifts_range_reference_and_preserves_result() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    // Put values in B1:C1 and sum them from D1.
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(2.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=SUM(B1:C1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(3.0))
    );

    // Delete column A; range should shift left, and formula cell should shift to C1.
    engine.delete_columns("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(3.0))
    );
    let (ast_opt, _) = engine.get_cell("Sheet1", 1, 3).expect("C1 exists");
    let ast = ast_opt.expect("C1 has formula");
    let f = canonical_formula(&ast);
    assert!(
        f.contains("SUM") && f.contains("A1") && f.contains("B1"),
        "expected SUM range to shift to A1:B1, got: {f}"
    );
}

#[test]
fn canonical_delete_columns_contracts_range_when_deleted_inside() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(1.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(2.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(3.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 4, parse("=SUM(A1:C1)").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 4),
        Some(LiteralValue::Number(6.0))
    );

    // Delete column B (1-based). This removes the middle cell of the SUM range.
    engine.delete_columns("Sheet1", 2, 1).unwrap();
    engine.evaluate_all().unwrap();

    // Formula cell shifts left: D1 -> C1. Range should contract and sum (1 + 3) = 4.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(4.0))
    );
    let (ast_opt, _) = engine.get_cell("Sheet1", 1, 3).expect("C1 exists");
    let ast = ast_opt.expect("C1 has formula");
    let f = canonical_formula(&ast);
    assert!(
        f.contains("SUM") && f.contains("A1") && f.contains("B1"),
        "expected SUM range to contract to A1:B1, got: {f}"
    );
}

#[test]
fn canonical_delete_rows_creates_ref_and_propagates_downstream() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(20.0))
        .unwrap();
    // A3 depends on A1 + A2.
    engine
        .set_cell_formula("Sheet1", 3, 1, parse("=A1+A2").unwrap())
        .unwrap();
    // B3 depends on A3 (downstream).
    engine
        .set_cell_formula("Sheet1", 3, 2, parse("=A3+1").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(30.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 2),
        Some(LiteralValue::Number(31.0))
    );

    // Delete row 1. Formula that referenced A1 should become #REF!.
    engine.delete_rows("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();

    // Old A2 moved to A1.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );

    // Formula moved from A3->A2 and should now be #REF!.
    match engine.get_cell_value("Sheet1", 2, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        other => panic!("expected Sheet1!A2 to be #REF!, got {other:?}"),
    }

    // Downstream dependent moved from B3->B2 and should also be #REF!.
    match engine.get_cell_value("Sheet1", 2, 2) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Ref),
        other => panic!("expected Sheet1!B2 to be #REF! (propagated), got {other:?}"),
    }
}

#[test]
fn structural_ref_literal_preserves_lazy_error_semantics_and_round_trips() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 3, parse("=IF(FALSE,A1,5)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 4, 3, parse("=IFERROR(A1,7)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 3, 4, parse("=C3+1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    engine.delete_rows("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 3),
        Some(LiteralValue::Number(5.0)),
        "an invalid reference in an unselected IF branch must not poison the formula"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 3),
        Some(LiteralValue::Number(7.0)),
        "IFERROR must catch the structurally synthesized #REF! literal"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 4),
        Some(LiteralValue::Number(6.0)),
        "downstream formulas must recompute from the lazy result"
    );

    for (row, expected) in [(2, "=IF(false, #REF!, 5)"), (3, "=IFERROR(#REF!, 7)")] {
        let (ast, _) = engine
            .get_cell("Sheet1", row, 3)
            .expect("formula cell exists");
        let ast = ast.expect("formula AST exists");
        let rendered = canonical_formula(&ast);
        assert_eq!(rendered, expected);
        assert_eq!(
            canonical_formula(&parse(&rendered).expect("rendered #REF! formula must reparse")),
            rendered
        );
    }
}

#[test]
fn structural_adjustment_is_sheet_local_and_ref_is_a_valid_sheet_name() {
    let cfg = arrow_eval_config();
    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine.add_sheet("Other").unwrap();
    engine.add_sheet("#REF").unwrap();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Other", 1, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_value("#REF", 1, 1, LiteralValue::Number(30.0))
        .unwrap();
    engine
        .set_cell_formula("Other", 2, 2, parse("=A1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Other", 3, 2, parse("=Sheet1!A1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Other", 4, 2, parse("='#REF'!A1").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();

    engine.delete_rows("Sheet1", 1, 1).unwrap();
    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Other", 2, 2),
        Some(LiteralValue::Number(20.0)),
        "unqualified references on another sheet must remain local"
    );
    match engine.get_cell_value("Other", 3, 2) {
        Some(LiteralValue::Error(error)) => assert_eq!(error.kind, ExcelErrorKind::Ref),
        other => panic!("expected the matching qualified reference to become #REF!, got {other:?}"),
    }
    assert_eq!(
        engine.get_cell_value("Other", 4, 2),
        Some(LiteralValue::Number(30.0)),
        "a real sheet named #REF must remain addressable"
    );

    let formula = |row| {
        let (ast, _) = engine
            .get_cell("Other", row, 2)
            .expect("formula cell exists");
        canonical_formula(&ast.expect("formula AST exists"))
    };
    assert_eq!(formula(2), "=A1");
    assert_eq!(formula(3), "=#REF!");
    assert_eq!(formula(4), "='#REF'!A1");
}
