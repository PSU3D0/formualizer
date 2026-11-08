//! Corrupt only the private registry to test the query's boundary guarantee.
//! This fixture does not claim a native edit can create this inconsistent state.
use super::*;
use crate::engine::{
    Engine, EvalConfig,
    affected_closure::{AffectedRoot, ClosureLimits, UnknownReason},
};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;

#[test]
fn current_read_closure_displaced_spill_geometry_is_explicitly_unknown() {
    #[cfg(not(target_arch = "wasm32"))]
    {
        const CASE: &str = "engine::graph::malformed_spill_query_tests::current_read_closure_displaced_spill_geometry_is_explicitly_unknown";
        if std::env::var("FZ_DISPLACED_SPILL_QUERY_CHILD")
            .ok()
            .as_deref()
            != Some(CASE)
        {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", CASE, "--nocapture"])
                .env("FZ_DISPLACED_SPILL_QUERY_CHILD", CASE)
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
    }
    crate::builtins::load_builtins();
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            arrow_storage_enabled: true,
            delta_overlay_enabled: true,
            write_formula_overlay_enabled: true,
            ..Default::default()
        },
    );
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(3.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=INDIRECT(\"A1:A2\")").unwrap())
        .unwrap();
    engine.evaluate_all_for_snapshot(None).unwrap();
    let anchor_cell = engine.graph.make_cell_ref("Sheet1", 1, 2);
    let anchor = engine.graph.get_vertex_for_cell(&anchor_cell).unwrap();
    assert!(engine.graph.is_spill_anchor(anchor));
    let displaced = vec![
        engine.graph.make_cell_ref("Sheet1", 10, 8),
        engine.graph.make_cell_ref("Sheet1", 11, 8),
    ];
    engine.graph.spill_anchor_to_cells.insert(anchor, displaced);
    let result = engine
        .current_read_affected_closure(
            &[AffectedRoot::CommittedSpill(anchor)],
            ClosureLimits {
                max_work: 100_000,
                max_output_items: 1000,
                max_scratch_bytes: 2 << 20,
            },
        )
        .unwrap();
    assert!(
        result.unlocalized_spill_geometry,
        "displaced registry must not certify geometry: {result:?}"
    );
    assert!(
        result
            .unknowns
            .iter()
            .any(|fact| fact.vertex == Some(anchor)
                && fact.reason == UnknownReason::UnavailableSpillGeometry)
    );
    assert!(
        !result
            .witnessed
            .iter()
            .any(|region| region.rect.contains(9, 7))
    );
}
