use crate::engine::{eval::Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;
use formualizer_parse::LiteralValue;

/// Helper: evaluate under both arrow_canonical_values modes and return (false_result, true_result)
/// for a given cell.
fn eval_both_modes<F>(setup: F, sheet: &str, row: u32, col: u32) -> (Option<LiteralValue>, Option<LiteralValue>)
where
    F: Fn(&mut Engine<TestWorkbook>),
{
    // Non-canonical (graph-truth) mode
    let wb = TestWorkbook::new();
    let cfg = EvalConfig {
        arrow_canonical_values: false,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(wb, cfg);
    setup(&mut engine);
    engine.evaluate_all().unwrap();
    let _ = engine.evaluate_all().unwrap(); // second pass for spill dependents
    let val_false = engine.read_cell_value(sheet, row, col);

    // Canonical (Arrow-truth) mode
    let wb = TestWorkbook::new();
    let cfg = EvalConfig {
        arrow_canonical_values: true,
        ..EvalConfig::default()
    };
    let mut engine = Engine::new(wb, cfg);
    setup(&mut engine);
    engine.evaluate_all().unwrap();
    let _ = engine.evaluate_all().unwrap();
    let val_true = engine.read_cell_value(sheet, row, col);

    (val_false, val_true)
}

#[test]
fn mode_matrix_scalar_formulas() {
    let setup = |engine: &mut Engine<TestWorkbook>| {
        engine.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0)).unwrap();
        engine.set_cell_formula("Sheet1", 1, 2, parse("=A1*2").unwrap()).unwrap();
        engine.set_cell_formula("Sheet1", 1, 3, parse("=SUM(A1:B1)").unwrap()).unwrap();
    };

    // A1 = 10
    let (vf, vt) = eval_both_modes(setup, "Sheet1", 1, 1);
    assert_eq!(vf, Some(LiteralValue::Number(10.0)), "A1 graph-truth");
    assert_eq!(vt, Some(LiteralValue::Number(10.0)), "A1 arrow-truth");

    // B1 = A1*2 = 20
    let (vf, vt) = eval_both_modes(setup, "Sheet1", 1, 2);
    assert_eq!(vf, Some(LiteralValue::Number(20.0)), "B1 graph-truth");
    assert_eq!(vt, Some(LiteralValue::Number(20.0)), "B1 arrow-truth");

    // C1 = SUM(A1:B1) = 30
    let (vf, vt) = eval_both_modes(setup, "Sheet1", 1, 3);
    assert_eq!(vf, Some(LiteralValue::Number(30.0)), "C1 graph-truth");
    assert_eq!(vt, Some(LiteralValue::Number(30.0)), "C1 arrow-truth");
}

#[test]
fn mode_matrix_spill() {
    let setup = |engine: &mut Engine<TestWorkbook>| {
        engine
            .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(5,1)").unwrap())
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 1, 2, parse("=SUM(A1:A5)").unwrap())
            .unwrap();
    };

    // Spill values: A1=1, A2=2, ..., A5=5
    for r in 1..=5u32 {
        let (vf, vt) = eval_both_modes(setup, "Sheet1", r, 1);
        assert_eq!(
            vf,
            Some(LiteralValue::Number(r as f64)),
            "A{r} graph-truth"
        );
        assert_eq!(
            vt,
            Some(LiteralValue::Number(r as f64)),
            "A{r} arrow-truth"
        );
    }

    // B1 = SUM(A1:A5) = 15
    let (vf, vt) = eval_both_modes(setup, "Sheet1", 1, 2);
    assert_eq!(vf, Some(LiteralValue::Number(15.0)), "B1 graph-truth");
    assert_eq!(vt, Some(LiteralValue::Number(15.0)), "B1 arrow-truth");
}

#[test]
fn guardrail_staleness_fallback() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.arrow_canonical_values = false;
    cfg.max_overlay_memory_bytes = Some(512); // tiny cap
    let mut engine = Engine::new(wb, cfg);

    // Create enough formula cells to exceed the budget
    for r in 1..=500 {
        engine
            .set_cell_formula("Sheet1", r, 1, parse("=1").unwrap())
            .unwrap();
    }
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=SUM(A1:A500)").unwrap())
        .unwrap();

    engine.evaluate_all().unwrap();
    let _ = engine.evaluate_all().unwrap(); // second pass

    // SUM must still be correct even after budget-triggered fallback
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(500.0))
    );

    // The engine should have force_materialize_range_views set
    assert!(
        engine.force_materialize_range_views,
        "force_materialize_range_views must be set when overlay mirroring is disabled"
    );
}

#[test]
#[should_panic(expected = "arrow_canonical_values=true requires correctness-preserving budget")]
fn canonical_mode_panics_on_budget_clear() {
    let wb = TestWorkbook::new();
    let mut cfg = EvalConfig::default();
    cfg.arrow_canonical_values = true;
    cfg.max_overlay_memory_bytes = Some(512); // tiny cap to trigger budget clearing
    let mut engine = Engine::new(wb, cfg);

    // Create enough formula cells to exceed the budget
    for r in 1..=500 {
        engine
            .set_cell_formula("Sheet1", r, 1, parse("=1").unwrap())
            .unwrap();
    }

    // This should panic because canonical mode + budget clear is unsupported
    engine.evaluate_all().unwrap();
}
