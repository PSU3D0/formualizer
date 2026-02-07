//! Test-only gate: run a small canonical-mode smoke subset when enabled via env.
//!
//! Usage:
//!   FZ_TEST_FORCE_ARROW_CANONICAL=1 cargo test -p formualizer-eval arrow_canonical_env_607

use crate::engine::eval::Engine;
use crate::engine::EvalConfig;
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use formualizer_parse::LiteralValue;

#[test]
fn canonical_env_smoke_subset() {
    let enabled = std::env::var("FZ_TEST_FORCE_ARROW_CANONICAL")
        .ok()
        .is_some_and(|v| v != "0");
    if !enabled {
        return;
    }

    let mut cfg = EvalConfig::default();
    cfg.arrow_canonical_values = true;

    let mut engine = Engine::new(TestWorkbook::default(), cfg);

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1+40").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();

    assert!(!engine.graph.value_cache_enabled());
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(42.0))
    );
}
