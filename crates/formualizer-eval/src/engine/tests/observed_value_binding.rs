use super::*;
use crate::engine::{CancelToken, EvalConfig};
use crate::test_workbook::TestWorkbook;
use formualizer_common::ExcelErrorKind;
use formualizer_parse::parser::parse;

fn setup_dynamic() -> (Engine<TestWorkbook>, VertexId) {
    crate::builtins::load_builtins();
    let mut engine = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            arrow_storage_enabled: true,
            delta_overlay_enabled: true,
            write_formula_overlay_enabled: true,
            ..EvalConfig::default()
        },
    );
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 3, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=INDIRECT(\"B1\")").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    let cell = engine.graph.make_cell_ref("Sheet1", 1, 1);
    let vertex = engine.graph.get_vertex_id_for_address(&cell).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert!(
        engine
            .graph
            .authority_host()
            .committed_observed(vertex)
            .is_some()
    );
    (engine, vertex)
}

#[test]
fn observed_value_binding_is_not_published_by_effect_planning() {
    let (mut engine, vertex) = setup_dynamic();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=INDIRECT(\"C1\")").unwrap())
        .unwrap();
    assert!(
        engine
            .graph
            .authority_host()
            .committed_observed(vertex)
            .is_none()
    );
    engine.freshness.armed = true;
    let cell = engine.graph.make_cell_ref("Sheet1", 1, 1);
    let result = engine
        .freshness_evaluate_recorded(
            vertex,
            "Sheet1",
            cell,
            engine.graph.formula_view(vertex).unwrap(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(result, LiteralValue::Number(20.0));
    let effects = engine.plan_vertex_effects(vertex, result).unwrap();
    assert_ne!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert!(
        engine
            .graph
            .authority_host()
            .committed_observed(vertex)
            .is_none(),
        "staged reads must not certify an unwritten result"
    );
    for effect in effects {
        engine.apply_effect(&effect, None, None).unwrap();
    }
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );
    let reads = engine
        .graph
        .authority_host()
        .committed_observed(vertex)
        .unwrap();
    assert!(reads.contains(&(0, 0, 2, 0, 2)), "{reads:?}");
}

#[test]
fn observed_value_binding_is_retired_by_unrecorded_result_writes() {
    let mut failures = Vec::new();
    for by_ref in [false, true] {
        let (mut engine, vertex) = setup_dynamic();
        assert!(
            !engine.graph.value_cache_enabled(),
            "canonical fast return must be tested"
        );
        let routing = engine
            .graph
            .authority_host()
            .observed(vertex)
            .unwrap()
            .to_vec();
        let revision = engine.graph.authority_host().rev_dyn();
        let proof_revision = engine.graph.authority_host().observed_value_revision();
        let value = LiteralValue::Number(999.0);
        if by_ref {
            engine.graph.update_vertex_value_ref(vertex, &value);
        } else {
            engine.graph.update_vertex_value(vertex, value);
        }
        assert_eq!(
            engine.graph.authority_host().observed(vertex),
            Some(routing.as_slice())
        );
        assert_eq!(engine.graph.authority_host().rev_dyn(), revision);
        assert!(engine.graph.authority_host().observed_value_revision() > proof_revision);
        if engine
            .graph
            .authority_host()
            .committed_observed(vertex)
            .is_some()
        {
            failures.push(format!(
                "by_ref={by_ref}: old reads cannot attest a generic replacement"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn observed_value_binding_failed_outer_request_cannot_retain_attestation() {
    let (mut engine, vertex) = setup_dynamic();
    let cancel = CancelToken::new();
    cancel.cancel();
    let error = engine.evaluate_all_cancellable(cancel).unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    assert!(
        engine
            .graph
            .authority_host()
            .committed_observed(vertex)
            .is_none(),
        "failed outer request must leave dynamic result trust unknown"
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
}

#[test]
fn observed_value_binding_named_result_writes_retire_non_grid_records() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    for by_ref in [false, true] {
        let (mut engine, _) = setup_dynamic();
        engine
            .define_name(
                "TrackedAlias",
                NamedDefinition::Formula {
                    ast: parse("=INDIRECT(\"C1\")").unwrap(),
                    dependencies: vec![],
                    range_deps: vec![],
                },
                NameScope::Workbook,
            )
            .unwrap();
        engine
            .set_cell_formula("Sheet1", 2, 1, parse("=TrackedAlias").unwrap())
            .unwrap();
        engine.evaluate_all().unwrap();
        let vertex = engine.graph.named_ranges_iter().next().unwrap().1.vertex;
        assert!(
            engine
                .graph
                .authority_host()
                .committed_observed(vertex)
                .is_some()
        );
        let value = LiteralValue::Number(999.0);
        if by_ref {
            engine.graph.update_vertex_value_ref(vertex, &value);
        } else {
            engine.graph.update_vertex_value(vertex, value);
        }
        assert!(
            engine
                .graph
                .authority_host()
                .committed_observed(vertex)
                .is_none(),
            "by_ref={by_ref}"
        );
    }
}

#[test]
fn observed_value_binding_spill_installs_reads_after_final_anchor_write() {
    let (mut engine, _) = setup_dynamic();
    engine
        .set_cell_value("Sheet1", 1, 4, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_value("Sheet1", 1, 5, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 4, 1, parse("=INDIRECT(\"C1\")").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    let cell = engine.graph.make_cell_ref("Sheet1", 4, 1);
    let vertex = engine.graph.get_vertex_id_for_address(&cell).unwrap();
    engine
        .set_cell_formula("Sheet1", 4, 1, parse("=INDIRECT(\"D1:E1\")").unwrap())
        .unwrap();
    engine.freshness.armed = true;
    let result = engine
        .freshness_evaluate_recorded(
            vertex,
            "Sheet1",
            cell,
            engine.graph.formula_view(vertex).unwrap(),
        )
        .unwrap()
        .unwrap();
    assert!(matches!(result, LiteralValue::Array(_)));
    let effects = engine.plan_vertex_effects(vertex, result).unwrap();
    assert!(
        engine
            .graph
            .authority_host()
            .committed_observed(vertex)
            .is_none()
    );
    for effect in effects {
        engine.apply_effect(&effect, None, None).unwrap();
    }
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 2),
        Some(LiteralValue::Number(20.0))
    );
    let reads = engine
        .graph
        .authority_host()
        .committed_observed(vertex)
        .unwrap();
    assert!(reads.contains(&(0, 0, 3, 0, 4)), "{reads:?}");
}

#[test]
fn observed_value_binding_current_empty_reads_are_distinct_from_unknown() {
    let (mut engine, _) = setup_dynamic();
    engine
        .set_cell_formula(
            "Sheet1",
            4,
            1,
            parse("=IF(FALSE,INDIRECT(\"D1\"),7)").unwrap(),
        )
        .unwrap();
    engine.evaluate_all().unwrap();
    let vertex = engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 4, 1))
        .unwrap();
    assert!(engine.graph.is_dynamic(vertex));
    assert_eq!(
        engine.graph.authority_host().committed_observed(vertex),
        Some([].as_slice())
    );
    assert_eq!(
        engine.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(7.0))
    );
}

struct CancelAfterRead {
    cancel: CancelToken,
}
impl crate::function::Function for CancelAfterRead {
    fn name(&self) -> &'static str {
        "CANCEL_AFTER_READ"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn caps(&self) -> crate::function::FnCaps {
        crate::function::FnCaps::VOLATILE
    }
    fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
        static SCHEMA: std::sync::LazyLock<Vec<crate::args::ArgSchema>> =
            std::sync::LazyLock::new(|| vec![crate::args::ArgSchema::any()]);
        &SCHEMA
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
        _: &dyn crate::traits::FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        let value = args[0].value()?;
        self.cancel.cancel();
        Ok(value)
    }
}
#[test]
fn observed_value_binding_failure_after_partial_writes_discards_attestation() {
    crate::builtins::load_builtins();
    let cancel = CancelToken::new();
    let workbook = TestWorkbook::new().with_function(std::sync::Arc::new(CancelAfterRead {
        cancel: cancel.clone(),
    }));
    let mut engine = Engine::new(
        workbook,
        EvalConfig {
            enable_parallel: false,
            arrow_storage_enabled: true,
            delta_overlay_enabled: true,
            write_formula_overlay_enabled: true,
            ..Default::default()
        },
    );
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(10.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=INDIRECT(\"B1\")").unwrap())
        .unwrap();
    engine.evaluate_all().unwrap();
    let vertex = engine
        .graph
        .get_vertex_id_for_address(&engine.graph.make_cell_ref("Sheet1", 1, 1))
        .unwrap();
    assert!(
        engine
            .graph
            .authority_host()
            .committed_observed(vertex)
            .is_some()
    );
    engine
        .set_cell_value("Sheet1", 1, 2, LiteralValue::Number(20.0))
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=CANCEL_AFTER_READ(A1)").unwrap())
        .unwrap();
    let error = engine.evaluate_all_cancellable(cancel).unwrap_err();
    assert_eq!(error.kind, ExcelErrorKind::Cancelled);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0)),
        "the dynamic result must actually commit before this failure"
    );
    assert!(
        engine
            .graph
            .authority_host()
            .committed_observed(vertex)
            .is_none()
    );
    assert!(engine.graph.is_dirty(vertex));
}
