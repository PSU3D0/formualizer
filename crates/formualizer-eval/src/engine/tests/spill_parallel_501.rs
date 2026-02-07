use crate::engine::{eval::Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::LiteralValue;
use formualizer_parse::parser::parse;

fn parallel_eval_config() -> EvalConfig {
    EvalConfig {
        enable_parallel: true,
        ..Default::default()
    }
}

fn read_range(
    engine: &Engine<TestWorkbook>,
    sheet: &str,
    sr: u32,
    sc: u32,
    er: u32,
    ec: u32,
) -> Vec<Vec<LiteralValue>> {
    let mut out = Vec::new();
    for r in sr..=er {
        let mut row = Vec::new();
        for c in sc..=ec {
            row.push(
                engine
                    .get_cell_value(sheet, r, c)
                    .unwrap_or(LiteralValue::Empty),
            );
        }
        out.push(row);
    }
    out
}

#[test]
fn parallel_spill_projects_children_sequence() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, parallel_eval_config());

    // Ensure we actually exercise the parallel layer path by creating a layer with >1 vertex.
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();

    let _ = engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(1.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(3.0))
    );
}

#[test]
fn parallel_spill_conflict_produces_spill_error() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, parallel_eval_config());

    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Text("X".into()))
        .unwrap();
    // Ensure parallel evaluation path is taken.
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=2").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();

    let _ = engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(e)) => assert_eq!(e, "#SPILL!"),
        v => panic!("expected #SPILL! at A1, got {v:?}"),
    }
    // Conflicting cell is preserved; spill children are not projected.
    assert_eq!(
        engine.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Text("X".into()))
    );
    assert_eq!(engine.get_cell_value("Sheet1", 3, 1), None);

    let a1 = engine.graph.make_cell_ref("Sheet1", 1, 1);
    let anchor_vid = engine
        .graph
        .get_vertex_for_cell(&a1)
        .expect("vertex id for A1");
    assert!(engine.graph.spill_cells_for_anchor(anchor_vid).is_none());
}

#[test]
fn parallel_spill_visible_via_range_reads() {
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, parallel_eval_config());

    // Ensure parallel layer path.
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=99").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(3,1)").unwrap())
        .unwrap();

    let _ = engine.evaluate_all().unwrap();

    let vals = read_range(&engine, "Sheet1", 1, 1, 3, 1);
    assert_eq!(
        vals,
        vec![
            vec![LiteralValue::Number(1.0)],
            vec![LiteralValue::Number(2.0)],
            vec![LiteralValue::Number(3.0)],
        ]
    );
}
