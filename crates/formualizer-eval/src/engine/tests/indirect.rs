use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::{ChangeLog, Engine, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::{ExcelErrorKind, LiteralValue};
use formualizer_parse::ASTNode;
use formualizer_parse::parser::parse as parse_formula;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

fn parse(formula: &str) -> ASTNode {
    parse_formula(formula).expect("valid formula")
}

fn telemetry_config() -> EvalConfig {
    EvalConfig::default().with_virtual_dep_telemetry(true)
}

#[test]
fn indirect_simple_lookup_and_update() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(42.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=INDIRECT(\"A1\")"))
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(42.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(100.0))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(100.0))
    );
}

#[test]
fn indirect_retarget_via_ref_text_edit() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("A1".to_string()))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=INDIRECT(B1)"))
        .unwrap();

    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(10.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("A2".to_string()))
        .unwrap();
    engine.evaluate_all().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(20.0))
    );
}

#[test]
fn indirect_invalid_ref_returns_ref_error() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=INDIRECT(\"NOT_A_REF\")"))
        .unwrap();

    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(err)) => assert_eq!(err.kind, ExcelErrorKind::Ref),
        other => panic!("expected #REF! error, got {other:?}"),
    }
}

#[test]
fn indirect_retarget_parity_across_full_recalc_entrypoints() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("A1".to_string()))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=INDIRECT(B1)"))
        .unwrap();

    engine.evaluate_all().unwrap();

    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("A2".to_string()))
        .unwrap();
    let (_res, _delta) = engine.evaluate_all_with_delta().unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(20.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("A1".to_string()))
        .unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    engine.evaluate_all_cancellable(cancel).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(10.0))
    );

    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("A2".to_string()))
        .unwrap();
    let mut log = ChangeLog::new();
    engine.evaluate_all_logged(&mut log).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(20.0))
    );
}

#[test]
fn indirect_supports_quoted_sheet_and_ranges() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Data Sheet", 1, 1, LiteralValue::Number(2.0))
        .unwrap();
    engine
        .set_cell_value("Data Sheet", 2, 1, LiteralValue::Number(3.0))
        .unwrap();
    engine
        .set_cell_value("Data Sheet", 3, 1, LiteralValue::Number(5.0))
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=INDIRECT(\"'Data Sheet'!A1\")"))
        .unwrap();
    engine
        .set_cell_formula(
            "Sheet1",
            1,
            2,
            parse("=SUM(INDIRECT(\"'Data Sheet'!A1:A3\"))"),
        )
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 2),
        Some(LiteralValue::Number(10.0))
    );
}

#[test]
fn indirect_supports_named_ranges_and_maps_missing_name_to_ref() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .define_name(
            "MyValue",
            NamedDefinition::Literal(LiteralValue::Number(77.0)),
            NameScope::Workbook,
        )
        .unwrap();

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=INDIRECT(\"MyValue\")"))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=INDIRECT(\"UnknownName\")"))
        .unwrap();

    engine.evaluate_all().unwrap();

    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(77.0))
    );
    match engine.get_cell_value("Sheet1", 1, 2) {
        Some(LiteralValue::Error(err)) => assert_eq!(err.kind, ExcelErrorKind::Ref),
        other => panic!("expected #REF! error, got {other:?}"),
    }
}

#[test]
fn indirect_a1_false_returns_not_implemented_error() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=INDIRECT(\"R1C1\",FALSE)"))
        .unwrap();

    engine.evaluate_all().unwrap();

    match engine.get_cell_value("Sheet1", 1, 1) {
        Some(LiteralValue::Error(err)) => assert_eq!(err.kind, ExcelErrorKind::NImpl),
        other => panic!("expected #NIMPL error, got {other:?}"),
    }
}

#[test]
fn recalc_plan_with_indirect_falls_back_to_dynamic_recalc() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("A1".to_string()))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=INDIRECT(B1)"))
        .unwrap();

    let plan = engine.build_recalc_plan().unwrap();
    assert!(plan.has_dynamic_refs());

    engine.evaluate_all().unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("A2".to_string()))
        .unwrap();

    let before = engine.virtual_dep_fallback_activations();
    engine.evaluate_recalc_plan(&plan).unwrap();
    let after = engine.virtual_dep_fallback_activations();

    assert_eq!(after, before + 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(20.0))
    );
}

#[test]
fn virtual_dep_telemetry_reports_convergence_for_indirect_retarget() {
    let mut engine = Engine::new(TestWorkbook::new(), telemetry_config());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 2, 1, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("A1".to_string()))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=INDIRECT(B1)"))
        .unwrap();

    engine.evaluate_all().unwrap();
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Text("A2".to_string()))
        .unwrap();
    engine.evaluate_all().unwrap();

    let t = engine.last_virtual_dep_telemetry();
    assert!(t.candidate_vertices_total > 0);
    assert!(t.schedule_virtual_passes > 0 || t.schedule_static_passes > 0);
    assert_eq!(t.bailout_reason, Some("converged"));
}

#[test]
fn virtual_dep_telemetry_static_workbook_has_no_dynamic_changes() {
    let mut engine = Engine::new(TestWorkbook::new(), telemetry_config());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1+1"))
        .unwrap();

    engine.evaluate_all().unwrap();
    let t = engine.last_virtual_dep_telemetry();
    assert!(t.candidate_vertices_total > 0);
    assert_eq!(t.changed_vdeps_total, 0);
}

#[test]
fn virtual_dep_telemetry_disabled_by_default() {
    let mut engine = Engine::new(TestWorkbook::new(), EvalConfig::default());

    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(5.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 2, parse("=A1+1"))
        .unwrap();

    engine.evaluate_all().unwrap();
    let t = engine.last_virtual_dep_telemetry();
    assert_eq!(t.candidate_vertices_total, 0);
    assert_eq!(t.schedule_virtual_passes + t.schedule_static_passes, 0);
}
