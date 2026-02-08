//! Ticket 607: in Arrow-canonical mode, dependency-graph value caching must be disabled.

use crate::engine::eval::Engine;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::EvalConfig;
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use formualizer_parse::LiteralValue;

use super::common::{abs_cell_ref, arrow_eval_config};

#[test]
fn canonical_mode_disables_graph_value_cache_for_cells_and_formulas() {
    let cfg: EvalConfig = arrow_eval_config();

    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert!(!engine.graph.value_cache_enabled());
    assert_eq!(engine.graph.debug_graph_value_read_attempts(), 0);

    // Public reads remain correct.
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(20.0))
    );

    // Graph cache reads return None for cell/formula vertices in canonical mode.
    let sid = engine.graph.sheet_id("Sheet1").unwrap();
    let a1 = abs_cell_ref(sid, 1, 1);
    let b1 = abs_cell_ref(sid, 1, 2);
    let a1_vid = engine.graph.get_vertex_for_cell(&a1).unwrap();
    let b1_vid = engine.graph.get_vertex_for_cell(&b1).unwrap();
    assert_eq!(engine.graph.get_value(a1_vid), None);
    assert_eq!(engine.graph.get_value(b1_vid), None);
    assert_eq!(engine.graph.get_cell_value("Sheet1", 1, 1), None);
    assert_eq!(engine.graph.get_cell_value("Sheet1", 1, 2), None);
}

#[test]
fn canonical_eval_does_not_read_graph_cell_values_with_named_formula() {
    let cfg: EvalConfig = arrow_eval_config();

    let mut engine = Engine::new(TestWorkbook::default(), cfg);
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();

    engine
        .define_name(
            "N",
            NamedDefinition::Formula {
                ast: parse("=A1*3").unwrap(),
                dependencies: Vec::new(),
                range_deps: Vec::new(),
            },
            NameScope::Workbook,
        )
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=N+1").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(engine.graph.debug_graph_value_read_attempts(), 0);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(31.0))
    );
}
