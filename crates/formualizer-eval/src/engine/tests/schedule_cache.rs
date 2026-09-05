use super::common::{create_binary_op_ast, create_cell_ref_ast};
use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelError, LiteralValue};

fn telemetry_config() -> EvalConfig {
    EvalConfig {
        enable_virtual_dep_telemetry: true,
        ..EvalConfig::default()
    }
}

fn make_engine() -> Engine<TestWorkbook> {
    Engine::new(TestWorkbook::new(), telemetry_config())
}

fn chain_ast(row: u32) -> formualizer_parse::ASTNode {
    create_binary_op_ast(
        create_cell_ref_ast(None, row - 1, 1),
        create_cell_ref_ast(None, row - 1, 1),
        "+",
    )
}

#[test]
fn schedule_cache_hits_on_repeated_value_only_chain_recalc() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))?;
    engine.set_cell_formula("Sheet1", 2, 1, chain_ast(2))?;
    engine.set_cell_formula("Sheet1", 3, 1, chain_ast(3))?;
    engine.set_cell_formula("Sheet1", 4, 1, chain_ast(4))?;

    engine.evaluate_all()?;

    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))?;
    engine.evaluate_all()?;

    let telemetry = engine.last_virtual_dep_telemetry().clone();
    assert_eq!(telemetry.schedule_cache_hits, 1);
    assert_eq!(telemetry.schedule_cache_misses, 0);
    assert_eq!(telemetry.reused_schedule_vertices_total, 3);
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(16.0))
    );
    Ok(())
}

#[test]
fn schedule_cache_invalidates_after_formula_edit() -> Result<(), ExcelError> {
    let mut engine = make_engine();
    engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))?;
    engine.set_cell_formula("Sheet1", 2, 1, chain_ast(2))?;
    engine.set_cell_formula("Sheet1", 3, 1, chain_ast(3))?;

    engine.evaluate_all()?;

    let replacement = create_binary_op_ast(
        create_cell_ref_ast(None, 2, 1),
        create_cell_ref_ast(None, 1, 1),
        "+",
    );
    engine.set_cell_formula("Sheet1", 3, 1, replacement)?;
    engine.evaluate_all()?;

    let telemetry = engine.last_virtual_dep_telemetry().clone();
    assert_eq!(telemetry.schedule_cache_hits, 0);
    assert_eq!(telemetry.schedule_cache_misses, 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 3, 1),
        Some(LiteralValue::Number(3.0))
    );
    Ok(())
}

fn run_probe_in_subprocess(test_name: &str) -> bool {
    // Registry-mutating tests can legitimately invalidate our cache mid-probe.
    // Isolate the stable-topology experiment rather than suppressing invalidation.
    const CHILD_ENV: &str = "FZ_RECALC_REUSE_SCHEDULE_PROBE_CHILD";
    if std::env::var_os(CHILD_ENV).is_some() {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(format!("engine::tests::schedule_cache::{test_name}"))
        .args(["--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    true
}

fn additive_chains(depth: u32, branches: u32) -> Engine<TestWorkbook> {
    use formualizer_parse::parser::parse;
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            formula_plane_mode: crate::engine::FormulaPlaneMode::Off,
            ..telemetry_config()
        },
    );
    for branch in 0..branches {
        let input_col = branch * 2 + 1;
        let formula_col = input_col + 1;
        engine
            .set_cell_value("Sheet1", 1, input_col, LiteralValue::Int(1))
            .unwrap();
        let input = char::from(b'A' + (input_col - 1) as u8);
        let output = char::from(b'A' + (formula_col - 1) as u8);
        for row in 1..=depth {
            let formula = if row == 1 {
                format!("={input}1+1")
            } else {
                format!("={output}{}+1", row - 1)
            };
            engine
                .set_cell_formula("Sheet1", row, formula_col, parse(formula).unwrap())
                .unwrap();
        }
    }
    engine
}

#[test]
fn schedule_cache_probe_separates_cold_build_warm_reuse_and_noop() {
    if run_probe_in_subprocess("schedule_cache_probe_separates_cold_build_warm_reuse_and_noop") {
        return;
    }
    for depth in [1, 32, 256] {
        let mut engine = additive_chains(depth, 1);
        engine.reset_recalc_reuse_probe();
        assert_eq!(
            engine.evaluate_all().unwrap().computed_vertices,
            depth as usize
        );
        let cold = engine.recalc_reuse_probe();
        assert_eq!(cold.schedule_builds, 1);
        assert_eq!(cold.schedule_cache_misses, 1);
        assert_eq!(cold.schedule_shared_handles, 1);
        assert_eq!(cold.schedule_deep_clones, 0);
        assert!(cold.schedule_retained_bytes > 0);

        engine.reset_recalc_reuse_probe();
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
            .unwrap();
        assert_eq!(
            engine.evaluate_all().unwrap().computed_vertices,
            depth as usize
        );
        let warm = engine.recalc_reuse_probe();
        assert_eq!(warm.schedule_cache_hits, 1);
        assert_eq!(warm.schedule_builds, 0);
        assert_eq!(warm.schedule_shared_handles, 1);
        assert_eq!(warm.schedule_deep_clones, 0);
        assert_eq!(warm.schedule_deep_clone_buffers, 0);
        assert_eq!(warm.demand_builds, 0);
        assert_eq!(
            engine.get_cell_value("Sheet1", depth, 2),
            Some(LiteralValue::Number(f64::from(depth) + 2.0))
        );

        engine.reset_recalc_reuse_probe();
        assert_eq!(engine.evaluate_all().unwrap().computed_vertices, 0);
        assert_eq!(engine.recalc_reuse_probe().schedule_requests, 0);
    }
}

#[test]
fn schedule_cache_probe_alternating_candidates_remain_misses() {
    if run_probe_in_subprocess("schedule_cache_probe_alternating_candidates_remain_misses") {
        return;
    }
    let mut engine = additive_chains(32, 2);
    engine.evaluate_all().unwrap();
    for (col, value) in [(1, 2), (3, 3), (1, 4), (3, 5)] {
        engine
            .set_cell_value("Sheet1", 1, col, LiteralValue::Int(value))
            .unwrap();
        engine.reset_recalc_reuse_probe();
        assert_eq!(engine.evaluate_all().unwrap().computed_vertices, 32);
        let probe = engine.recalc_reuse_probe();
        assert_eq!(probe.schedule_cache_hits, 0);
        assert_eq!(probe.schedule_cache_misses, 1);
        assert_eq!(probe.schedule_builds, 1);
        assert_eq!(
            engine.get_cell_value("Sheet1", 32, col + 1),
            Some(LiteralValue::Number(value as f64 + 32.0))
        );
    }
}

#[test]
fn schedule_cache_shares_immutable_payload_and_releases_invalidated_entry() {
    if run_probe_in_subprocess(
        "schedule_cache_shares_immutable_payload_and_releases_invalidated_entry",
    ) {
        return;
    }
    use formualizer_parse::parser::parse;
    use std::sync::Arc;
    let mut engine = additive_chains(32, 1);
    engine.evaluate_all().unwrap();
    let first = engine.cached_static_schedule_for_test().unwrap();
    assert_eq!(first.units.capacity(), first.units.len());
    assert_eq!(first.layers.capacity(), first.layers.len());
    assert_eq!(first.cycles.capacity(), first.cycles.len());
    for layer in &first.layers {
        assert_eq!(layer.vertices.capacity(), layer.vertices.len());
    }
    let weak = Arc::downgrade(&first);
    let units = first.units.clone();
    let layers = first
        .layers
        .iter()
        .map(|layer| layer.vertices.clone())
        .collect::<Vec<_>>();
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(2))
        .unwrap();
    engine.evaluate_all().unwrap();
    let hit = engine.cached_static_schedule_for_test().unwrap();
    assert!(Arc::ptr_eq(&first, &hit));
    assert_eq!(first.units, units);
    assert_eq!(
        first
            .layers
            .iter()
            .map(|layer| layer.vertices.clone())
            .collect::<Vec<_>>(),
        layers
    );
    assert_eq!(Arc::strong_count(&first), 3);

    engine
        .set_cell_formula("Sheet1", 32, 2, parse("=B31+2").unwrap())
        .unwrap();
    assert!(engine.cached_static_schedule_for_test().is_none());
    assert_eq!(Arc::strong_count(&first), 2);
    engine.evaluate_all().unwrap();
    let replacement = engine.cached_static_schedule_for_test().unwrap();
    assert!(!Arc::ptr_eq(&first, &replacement));
    assert_eq!(first.units, units);
    drop(hit);
    drop(first);
    assert!(weak.upgrade().is_none());
}

#[test]
fn schedule_cache_shared_cycle_payload_preserves_cycle_results() {
    if run_probe_in_subprocess("schedule_cache_shared_cycle_payload_preserves_cycle_results") {
        return;
    }
    use formualizer_parse::parser::parse;
    use std::sync::Arc;
    let mut engine = make_engine();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=C1+1").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=B1+1").unwrap())
        .unwrap();
    let vertices = engine.evaluation_vertices();
    let cold = engine.evaluate_all().unwrap();
    let first = engine.cached_static_schedule_for_test().unwrap();
    assert_eq!(first.cycles.len(), 1);
    let values = [
        engine.get_cell_value("Sheet1", 1, 2),
        engine.get_cell_value("Sheet1", 1, 3),
    ];
    for vertex in vertices {
        engine.graph.set_dirty(vertex, true);
    }
    engine.reset_recalc_reuse_probe();
    let warm = engine.evaluate_all().unwrap();
    assert_eq!(cold.cycle_errors, warm.cycle_errors);
    assert_eq!(engine.recalc_reuse_probe().schedule_cache_hits, 1);
    assert!(Arc::ptr_eq(
        &first,
        &engine.cached_static_schedule_for_test().unwrap()
    ));
    assert_eq!(
        [
            engine.get_cell_value("Sheet1", 1, 2),
            engine.get_cell_value("Sheet1", 1, 3)
        ],
        values
    );
}

#[test]
fn schedule_cache_shared_handles_do_not_expand_dynamic_or_range_eligibility() {
    if run_probe_in_subprocess(
        "schedule_cache_shared_handles_do_not_expand_dynamic_or_range_eligibility",
    ) {
        return;
    }
    use formualizer_parse::parser::parse;
    for formula in ["=INDIRECT(\"A1\")", "=SUM(A1:A32)"] {
        let mut engine = Engine::new(
            TestWorkbook::new(),
            EvalConfig {
                enable_parallel: false,
                formula_plane_mode: crate::engine::FormulaPlaneMode::Off,
                range_expansion_limit: 1,
                ..telemetry_config()
            },
        );
        engine
            .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(1))
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 1, 2, parse(formula).unwrap())
            .unwrap();
        for value in [2, 3] {
            engine
                .set_cell_value("Sheet1", 1, 1, LiteralValue::Int(value))
                .unwrap();
            engine.reset_recalc_reuse_probe();
            engine.evaluate_all().unwrap();
            let probe = engine.recalc_reuse_probe();
            assert!(probe.schedule_cache_ineligible > 0);
            assert_eq!(probe.schedule_shared_handles, 0);
            assert!(engine.cached_static_schedule_for_test().is_none());
            assert!(
                matches!(engine.get_cell_value("Sheet1", 1, 2), Some(LiteralValue::Number(n)) if n == value as f64)
                    || engine.get_cell_value("Sheet1", 1, 2) == Some(LiteralValue::Int(value))
            );
        }
    }
}
