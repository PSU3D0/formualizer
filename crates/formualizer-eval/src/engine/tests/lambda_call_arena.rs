//! Postfix calls (`LAMBDA(x,x+1)(B1)`) survive the arena round trip.
//!
//! The arena used to store a call as a `#N/IMPL!` literal, so the stored
//! formula lost its callee and arguments: the reconstructed tree had no
//! references while the parsed tree read `B1`. Evaluation is still
//! unimplemented for calls on both paths.

use crate::engine::{Engine, EvalConfig, FormulaPlaneMode};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::parser::{ASTNodeType, parse};
use formualizer_parse::pretty::canonical_formula;

const FORMULA: &str = "=LAMBDA(x,x+1)(B1)";

fn engine(mode: FormulaPlaneMode) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig::default().with_formula_plane_mode(mode),
    );
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(2.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse(FORMULA).unwrap())
        .unwrap();
    engine
}

fn stored_formula(engine: &Engine<TestWorkbook>, row: u32, col: u32) -> String {
    let (ast, _) = engine.get_cell("Sheet1", row, col).expect("cell exists");
    let ast = ast.expect("formula cell");
    assert!(
        matches!(ast.node_type, ASTNodeType::Call { .. }),
        "stored formula must reconstruct as a call, got {:?}",
        ast.node_type
    );
    canonical_formula(&ast)
}

fn assert_not_implemented(value: Option<LiteralValue>) {
    match value {
        Some(LiteralValue::Error(error)) => assert_eq!(error.kind, ExcelErrorKind::NImpl),
        other => panic!("expected #N/IMPL!, got {other:?}"),
    }
}

#[test]
fn call_formula_reconstructs_from_the_arena() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut engine = engine(mode);
        assert_eq!(
            stored_formula(&engine, 1, 1),
            canonical_formula(&parse(FORMULA).unwrap()),
            "{mode:?}"
        );
        engine.evaluate_all().unwrap();
        assert_not_implemented(engine.get_cell_value("Sheet1", 1, 1));
    }
}

#[test]
fn call_formula_references_follow_structural_edits() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut engine = engine(mode);
        engine.evaluate_all().unwrap();
        engine.insert_rows("Sheet1", 1, 1).unwrap();
        assert_eq!(
            stored_formula(&engine, 2, 1),
            canonical_formula(&parse("=LAMBDA(x,x+1)(B2)").unwrap()),
            "{mode:?}"
        );
        engine.evaluate_all().unwrap();
        assert_not_implemented(engine.get_cell_value("Sheet1", 2, 1));
    }
}
