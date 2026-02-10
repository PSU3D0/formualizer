use crate::engine::{EvalConfig, eval::Engine};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::LiteralValue;
use formualizer_parse::parser::parse;

/// Ticket 602: canonical mode compacts computed overlays into base arrays instead of panicking.
#[test]
fn canonical_mode_compacts_on_budget_cap() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.max_overlay_memory_bytes = Some(512); // tiny cap
    let mut engine = Engine::new(wb, cfg);

    for r in 1..=500 {
        engine
            .set_cell_formula("Sheet1", r, 1, parse("=1").unwrap())
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUM(A1:A500)").unwrap())
        .unwrap();

    // Must not panic — compaction handles the budget
    engine.evaluate_all().unwrap();
    let _ = engine.evaluate_all().unwrap();

    // SUM correct via Arrow-truth path
    assert_eq!(
        engine.read_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(500.0))
    );

    // Canonical mode must NOT flip to force_materialize
    assert!(
        !engine.force_materialize_range_views,
        "canonical mode must not set force_materialize_range_views"
    );
}

/// Ticket 602: overlay memory stays bounded across repeated evaluations in canonical mode.
#[test]
fn canonical_mode_overlay_usage_bounded() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.max_overlay_memory_bytes = Some(2048);
    let mut engine = Engine::new(wb, cfg);

    for r in 1..=2000 {
        engine
            .set_cell_formula("Sheet1", r, 1, parse("=1").unwrap())
            .unwrap();
    }

    for _ in 0..3 {
        engine.evaluate_all().unwrap();
        assert!(
            engine.overlay_memory_usage() <= 2048,
            "overlay memory {} exceeded cap 2048",
            engine.overlay_memory_usage()
        );
    }
}
