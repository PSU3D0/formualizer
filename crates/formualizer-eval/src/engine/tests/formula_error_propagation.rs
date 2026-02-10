use crate::engine::{Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};

#[test]
fn formula_ref_propagates_error_after_bulk_ingest() {
    // Mimic formualizer-workbook loader behavior:
    // - stage formula text
    // - build graph via bulk ingest
    // - evaluate
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // D8 = 1/0 -> #DIV/0!
    engine.stage_formula_text("Sheet1", 8, 4, "=1/0".to_string());
    // E8 = D8 -> must propagate #DIV/0!
    engine.stage_formula_text("Sheet1", 8, 5, "=D8".to_string());

    engine.build_graph_all().expect("staged formulas build");

    // Sanity: formulas exist as formulas (ingestion must not drop them)
    let (ast_d8, _v_d8) = engine.get_cell("Sheet1", 8, 4).expect("D8 present");
    assert!(ast_d8.is_some(), "D8 should have a formula AST");
    let (ast_e8, _v_e8) = engine.get_cell("Sheet1", 8, 5).expect("E8 present");
    assert!(ast_e8.is_some(), "E8 should have a formula AST");

    engine.evaluate_all().expect("evaluation succeeds");

    match engine.get_cell_value("Sheet1", 8, 4) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Div),
        other => panic!("D8 expected #DIV/0!, got {other:?}"),
    }

    match engine.get_cell_value("Sheet1", 8, 5) {
        Some(LiteralValue::Error(e)) => assert_eq!(e.kind, ExcelErrorKind::Div),
        other => panic!("E8 expected #DIV/0!, got {other:?}"),
    }
}

#[test]
fn rri_basic_cagr_calculation() {
    // RRI(nper, pv, fv) = (fv/pv)^(1/nper) - 1
    // Excel: =RRI(10, 1000, 2000) ≈ 0.07177 (CAGR for doubling in 10 periods)
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // A1 = RRI(10, 1000, 2000)
    engine.stage_formula_text("Sheet1", 1, 1, "=RRI(10, 1000, 2000)".to_string());
    // B1 = _XLFN.RRI(10, 1000, 2000) — verify prefix stripping works
    engine.stage_formula_text("Sheet1", 1, 2, "=_XLFN.RRI(10, 1000, 2000)".to_string());

    engine.build_graph_all().expect("staged formulas build");
    engine.evaluate_all().expect("evaluation succeeds");

    let expected = (2000.0_f64 / 1000.0).powf(1.0 / 10.0) - 1.0;

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Number(n)) => {
            assert!((n - expected).abs() < 1e-10, "A1 expected {expected}, got {n}");
        }
        other => panic!("A1 expected Number({expected}), got {other:?}"),
    }

    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Number(n)) => {
            assert!((n - expected).abs() < 1e-10, "B1 expected {expected}, got {n}");
        }
        other => panic!("B1 expected Number({expected}), got {other:?}"),
    }
}

#[test]
fn iferror_catches_name_error_from_unknown_function() {
    // IFERROR must catch ALL error types, including #NAME? from unknown functions.
    // In Excel, =IFERROR(NONEXISTENT_FUNC(), "fallback") returns "fallback".
    // Bug: the Rust `?` operator in IfErrorFn::eval propagates Err(ExcelError)
    // before the match can catch it, so #NAME? errors bypass IFERROR.
    let wb = TestWorkbook::new();
    let mut engine = Engine::new(wb, EvalConfig::default());

    // A1 = IFERROR(NONEXISTENT_FUNC(), "fallback")
    engine.stage_formula_text("Sheet1", 1, 1, "=IFERROR(NONEXISTENT_FUNC(), \"fallback\")".to_string());
    // B1 = IFERROR(1/0, "div_caught") — sanity check that IFERROR catches #DIV/0!
    engine.stage_formula_text("Sheet1", 1, 2, "=IFERROR(1/0, \"div_caught\")".to_string());

    engine.build_graph_all().expect("staged formulas build");
    engine.evaluate_all().expect("evaluation succeeds");

    // A1: IFERROR should catch #NAME? and return fallback
    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Text(s)) => assert_eq!(s, "fallback"),
        other => panic!("A1 expected \"fallback\", got {other:?}"),
    }

    // B1: IFERROR should catch #DIV/0! and return fallback
    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Text(s)) => assert_eq!(s, "div_caught"),
        other => panic!("B1 expected \"div_caught\", got {other:?}"),
    }
}
