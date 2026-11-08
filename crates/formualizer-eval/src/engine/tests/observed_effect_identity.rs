use super::*;
use crate::test_workbook::TestWorkbook;
use formualizer_parse::parser::parse;

fn setup() -> (Engine<TestWorkbook>, VertexId) {
    crate::builtins::load_builtins();
    let mut e = Engine::new(
        TestWorkbook::new(),
        EvalConfig {
            enable_parallel: false,
            arrow_storage_enabled: true,
            delta_overlay_enabled: true,
            write_formula_overlay_enabled: true,
            ..Default::default()
        },
    );
    e.set_cell_value("Sheet1", 1, 2, LiteralValue::Number(10.0))
        .unwrap();
    e.set_cell_value("Sheet1", 1, 3, LiteralValue::Number(20.0))
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 1, parse("=INDIRECT(\"B1\")").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let v = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 1))
        .unwrap();
    assert!(e.graph.authority_host().committed_observed(v).is_some());
    (e, v)
}
fn arm(e: &mut Engine<TestWorkbook>) {
    e.freshness_begin_request();
    e.freshness_begin_pass(&super::super::scheduler::Schedule {
        units: vec![],
        cycles: vec![],
        layers: vec![],
    });
}
fn record_c(e: &mut Engine<TestWorkbook>, v: VertexId) -> LiteralValue {
    e.set_cell_formula("Sheet1", 1, 1, parse("=INDIRECT(\"C1\")").unwrap())
        .unwrap();
    arm(e);
    e.freshness_evaluate_recorded(
        v,
        "Sheet1",
        e.graph.make_cell_ref("Sheet1", 1, 1),
        e.graph.formula_view(v).unwrap(),
    )
    .unwrap()
    .unwrap()
}
#[test]
fn observed_effect_buffered_write_must_wait_for_install() {
    let (mut e, v) = setup();
    let result = record_c(&mut e, v);
    let effects = e.plan_vertex_effects(v, result).unwrap();
    let mut buffer = ComputedWriteBuffer::default();
    for effect in effects {
        e.apply_effect_with_computed_writes(&effect, None, None, Some(&mut buffer))
            .unwrap();
    }
    assert!(!buffer.is_empty());
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert!(
        e.graph.authority_host().committed_observed(v).is_none(),
        "new C1 reads attest old B1 value before flush"
    );
}
#[test]
fn observed_effect_pending_reads_must_not_attest_generic_effect() {
    let (mut e, v) = setup();
    let _ = record_c(&mut e, v);
    e.apply_effect(
        &Effect::WriteCell {
            vertex_id: v,
            value: LiteralValue::Number(999.0),
        },
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(999.0))
    );
    assert!(
        e.graph.authority_host().committed_observed(v).is_none(),
        "unrelated WriteCell consumed C1 evaluation evidence"
    );
}
#[test]
fn observed_effect_ref_error_write_must_withdraw_named_proof() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let (mut e, _) = setup();
    e.define_name(
        "TrackedAlias",
        NamedDefinition::Formula {
            ast: parse("=INDIRECT(\"C1\")").unwrap(),
            dependencies: vec![],
            range_deps: vec![],
        },
        NameScope::Workbook,
    )
    .unwrap();
    e.set_cell_formula("Sheet1", 2, 1, parse("=TrackedAlias").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let v = e.graph.named_ranges_iter().next().unwrap().1.vertex;
    assert_eq!(e.graph.get_value(v), Some(LiteralValue::Number(20.0)));
    assert!(e.graph.authority_host().committed_observed(v).is_some());
    e.graph.mark_as_ref_error(v);
    assert!(
        matches!(e.graph.get_value(v),Some(LiteralValue::Error(ref err)) if err.kind==ExcelErrorKind::Ref)
    );
    assert!(
        e.graph.authority_host().committed_observed(v).is_none(),
        "raw named #REF replacement retains old proof"
    );
}
#[test]
fn observed_effect_native_unchanged_reads_keep_relation_revision() {
    let (mut e, v) = setup();
    e.create_evaluation_schedule(&[v]).unwrap();
    assert!(
        e.create_evaluation_schedule(&[v])
            .unwrap()
            .2
            .schedule_cache_hit
    );
    let rev = e.graph.authority_host().rev_dyn();
    let reads = e.graph.authority_host().observed(v).unwrap().to_vec();
    e.graph.set_dirty(v, true);
    e.evaluate_all().unwrap();
    assert_eq!(e.graph.authority_host().rev_dyn(), rev);
    assert_eq!(e.graph.authority_host().observed(v), Some(reads.as_slice()));
    assert!(e.graph.authority_host().committed_observed(v).is_some());
    assert!(
        e.create_evaluation_schedule(&[v])
            .unwrap()
            .2
            .schedule_cache_hit
    );
}

#[test]
fn observed_effect_cancel_must_preserve_routing_and_cache_identity() {
    let (mut e, v) = setup();
    e.create_evaluation_schedule(&[v]).unwrap();
    assert!(
        e.create_evaluation_schedule(&[v])
            .unwrap()
            .2
            .schedule_cache_hit
    );
    let rev = e.graph.authority_host().rev_dyn();
    let routing = e.graph.authority_host().observed(v).unwrap().to_vec();
    let cancel = crate::engine::CancelToken::new();
    cancel.cancel();
    assert!(e.evaluate_all_cancellable(cancel).is_err());
    assert!(e.graph.authority_host().committed_observed(v).is_none());
    assert_eq!(
        e.graph.authority_host().observed(v),
        Some(routing.as_slice()),
        "cancellation erases unchanged routing"
    );
    assert_eq!(e.graph.authority_host().rev_dyn(), rev);
    assert!(
        e.create_evaluation_schedule(&[v])
            .unwrap()
            .2
            .schedule_cache_hit
    );
}
#[test]
fn observed_effect_final_reconciliation_failure_discards_proof() {
    let (mut e, v) = setup();
    let footprint = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1));
    let result = e.observe_evaluation_resource_request(EvaluationRequestKind::Full, |inner| {
        inner.graph.set_dirty(v, true);
        inner.evaluate_all()?;
        assert!(inner.graph.authority_host().committed_observed(v).is_some());
        let mut budgets = crate::engine::EvaluationBudgets::default();
        budgets.retained.total_bytes = Some(0);
        inner.active_resource_ledger = Some(ResourceLedger::new(Some(999), budgets));
        inner.source_cache_accounted = 0;
        inner
            .source_cache_footprints
            .push(std::sync::Arc::downgrade(&footprint));
        Ok(())
    });
    assert!(result.is_err(), "exit reconciliation must actually fail");
    assert!(e.graph.authority_host().committed_observed(v).is_none());
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
}
#[test]
fn observed_effect_formula_to_literal_retires_record() {
    let (mut e, v) = setup();
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Number(7.0))
        .unwrap();
    assert!(e.graph.authority_host().observed(v).is_none());
    assert!(e.graph.authority_host().committed_observed(v).is_none());
}
#[test]
fn observed_effect_row_shift_retires_old_identity() {
    let (mut e, v) = setup();
    e.insert_rows("Sheet1", 1, 1).unwrap();
    assert!(e.graph.authority_host().committed_observed(v).is_none());
    assert!(e.graph.authority_host().observed(v).is_none());
}
#[test]
fn observed_effect_name_rebinding_retires_reader_proof() {
    use crate::engine::named_range::{NameScope, NamedDefinition};
    let (mut e, _) = setup();
    e.define_name(
        "Locator",
        NamedDefinition::Literal(LiteralValue::Text("B1".into())),
        NameScope::Workbook,
    )
    .unwrap();
    e.set_cell_formula("Sheet1", 2, 1, parse("=INDIRECT(Locator)").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let v = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 2, 1))
        .unwrap();
    assert!(e.graph.authority_host().committed_observed(v).is_some());
    e.update_name(
        "Locator",
        NamedDefinition::Literal(LiteralValue::Text("C1".into())),
        NameScope::Workbook,
    )
    .unwrap();
    assert!(e.graph.authority_host().committed_observed(v).is_none());
    e.evaluate_all().unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 2, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert!(e.graph.authority_host().committed_observed(v).is_some());
}
#[test]
fn observed_effect_scc_error_stamp_retires_proof() {
    let (mut e, v) = setup();
    let value = LiteralValue::Error(ExcelError::new(ExcelErrorKind::Circ));
    e.stamp_cycle_error(v, &value, None);
    assert_eq!(e.get_cell_value("Sheet1", 1, 1), Some(value));
    assert!(e.graph.authority_host().committed_observed(v).is_none());
}

#[test]
fn observed_effect_failed_structural_rebuild_must_retire_old_identity() {
    let (mut e, v) = setup();
    e.graph.authority_host_mut().store.budget.scratch = Some(0);
    let outcome = e.insert_rows("Sheet1", 1, 1);
    assert!(
        matches!(
            e.graph.authority_host().state,
            crate::engine::authority::host::HostState::Failed(_)
        ),
        "must actually reach failed authority: {outcome:?}"
    );
    assert_eq!(e.graph.get_cell_ref(v).unwrap().coord.row(), 1);
    assert!(
        e.graph.authority_host().committed_observed(v).is_none(),
        "failed rebuild retains B1 proof on moved formula/value"
    );
}

#[test]
fn observed_effect_spill_clear_must_withdraw_anchor_proof() {
    let (mut e, _) = setup();
    e.set_cell_formula("Sheet1", 4, 1, parse("=INDIRECT(\"B1:C1\")").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let v = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 4, 1))
        .unwrap();
    assert!(e.graph.is_spill_anchor(v));
    assert_eq!(
        e.get_cell_value("Sheet1", 4, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert!(e.graph.authority_host().committed_observed(v).is_some());
    e.apply_effect(&Effect::SpillClear { anchor_vertex: v }, None, None)
        .unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 4, 1)
            .unwrap_or(LiteralValue::Empty),
        LiteralValue::Empty
    );
    assert!(
        e.graph.authority_host().committed_observed(v).is_none(),
        "spill clear writes Empty to anchor but retains previous array proof"
    );
}
#[test]
fn observed_effect_yielding_spill_must_not_steal_pending_reader_evidence() {
    let (mut e, _) = setup();
    for row in 1..=3 {
        e.set_cell_value("Sheet1", row, 4, LiteralValue::Number((row * 10) as f64))
            .unwrap();
    }
    for col in 5..=7 {
        e.set_cell_value("Sheet1", 1, col, LiteralValue::Number(col as f64))
            .unwrap();
    }
    // Retire setup A1 so its B1 text read does not form a dependency on the later anchor.
    e.set_cell_value("Sheet1", 1, 1, LiteralValue::Empty)
        .unwrap();
    e.set_cell_formula("Sheet1", 1, 2, parse("=INDIRECT(\"D1:D3\")").unwrap())
        .unwrap();
    e.evaluate_all().unwrap();
    let other = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 1, 2))
        .unwrap();
    assert!(e.graph.is_spill_anchor(other));
    e.set_cell_formula("Sheet1", 2, 1, parse("=INDIRECT(\"E1:G1\")").unwrap())
        .unwrap();
    let winner = e
        .graph
        .get_vertex_id_for_address(&e.graph.make_cell_ref("Sheet1", 2, 1))
        .unwrap();
    arm(&mut e);
    // Simulate the real parallel group's compute-before-plan order.
    let _pending_other = e
        .freshness_evaluate_recorded(
            other,
            "Sheet1",
            e.graph.make_cell_ref("Sheet1", 1, 2),
            e.graph.formula_view(other).unwrap(),
        )
        .unwrap()
        .unwrap();
    let result = e
        .freshness_evaluate_recorded(
            winner,
            "Sheet1",
            e.graph.make_cell_ref("Sheet1", 2, 1),
            e.graph.formula_view(winner).unwrap(),
        )
        .unwrap()
        .unwrap();
    let effects = e.plan_vertex_effects(winner, result).unwrap();
    assert!(effects.iter().any(|effect|matches!(effect,Effect::WriteCell{vertex_id,value:LiteralValue::Error(err)} if *vertex_id==other && err.kind==ExcelErrorKind::Spill)),"must produce native yielding effect: {effects:?}");
    for effect in effects {
        e.apply_effect(&effect, None, None).unwrap();
    }
    assert!(
        matches!(e.get_cell_value("Sheet1",1,2),Some(LiteralValue::Error(ref err)) if err.kind==ExcelErrorKind::Spill)
    );
    assert!(
        e.graph.authority_host().committed_observed(other).is_none(),
        "winner's synthetic #SPILL write consumed loser's pending normal-value reads"
    );
}

#[test]
fn observed_effect_flush_installs_paired_reads_and_replay_is_one_use() {
    let (mut e, v) = setup();
    let result = record_c(&mut e, v);
    let effects = e.plan_vertex_effects(v, result).unwrap();
    let mut buffer = ComputedWriteBuffer::default();
    for effect in &effects {
        e.apply_effect_with_computed_writes(effect, None, None, Some(&mut buffer))
            .unwrap();
    }
    assert!(e.graph.authority_host().committed_observed(v).is_none());
    e.flush_computed_write_buffer(&mut buffer).unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );
    let sheet = e.graph.make_cell_ref("Sheet1", 1, 1).sheet_id;
    assert_eq!(
        e.graph.authority_host().committed_observed(v),
        Some([(sheet, 0, 2, 0, 2)].as_slice())
    );
    for effect in &effects {
        e.apply_effect(effect, None, None).unwrap();
    }
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert!(
        e.graph.authority_host().committed_observed(v).is_none(),
        "replaying a consumed proof must not reattest"
    );
}

#[test]
fn observed_effect_buffer_overwrites_use_actual_coalescing_winner() {
    use crate::arrow_store::OverlayValue;
    for kind in 0..3 {
        let (mut e, v) = setup();
        let result = record_c(&mut e, v);
        let effects = e.plan_vertex_effects(v, result).unwrap();
        let mut buffer = ComputedWriteBuffer::default();
        for effect in &effects {
            e.apply_effect_with_computed_writes(effect, None, None, Some(&mut buffer))
                .unwrap();
        }
        let sheet = e.graph.make_cell_ref("Sheet1", 1, 1).sheet_id;
        match kind {
            0 => buffer.push_cell(sheet, 0, 0, OverlayValue::Number(999.0)),
            1 => buffer.push_rect(sheet, 0, 0, vec![vec![OverlayValue::Number(999.0)]]),
            _ => buffer.push_column_run(sheet, 0, 0, vec![(OverlayValue::Number(999.0), None)]),
        }
        e.flush_computed_write_buffer(&mut buffer).unwrap();
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(999.0))
        );
        assert!(
            e.graph.authority_host().committed_observed(v).is_none(),
            "write kind {kind} left the losing result's proof"
        );
    }
}

#[test]
fn observed_effect_recorded_write_can_win_over_an_earlier_generic_write() {
    use crate::arrow_store::OverlayValue;
    let (mut e, v) = setup();
    let result = record_c(&mut e, v);
    let effects = e.plan_vertex_effects(v, result).unwrap();
    let sheet = e.graph.make_cell_ref("Sheet1", 1, 1).sheet_id;
    let mut buffer = ComputedWriteBuffer::default();
    buffer.push_cell(sheet, 0, 0, OverlayValue::Number(999.0));
    for effect in &effects {
        e.apply_effect_with_computed_writes(effect, None, None, Some(&mut buffer))
            .unwrap();
    }
    e.flush_computed_write_buffer(&mut buffer).unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert!(e.graph.authority_host().committed_observed(v).is_some());
}

#[test]
fn observed_effect_discarded_buffer_cannot_promote() {
    let (mut e, v) = setup();
    let result = record_c(&mut e, v);
    let effects = e.plan_vertex_effects(v, result).unwrap();
    let mut buffer = ComputedWriteBuffer::default();
    for effect in &effects {
        e.apply_effect_with_computed_writes(effect, None, None, Some(&mut buffer))
            .unwrap();
    }
    buffer.clear();
    e.flush_computed_write_buffer(&mut buffer).unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert!(e.graph.authority_host().committed_observed(v).is_none());
}

#[test]
fn observed_effect_retained_result_cannot_attest_another_pass_or_engine() {
    let (mut e, v) = setup();
    let result = record_c(&mut e, v);
    let effects = e.plan_vertex_effects(v, result).unwrap();
    arm(&mut e);
    for effect in &effects {
        e.apply_effect(effect, None, None).unwrap();
    }
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert!(e.graph.authority_host().committed_observed(v).is_none());

    let (mut other, other_v) = setup();
    assert_eq!(
        v, other_v,
        "control must use the same numeric vertex identity"
    );
    arm(&mut other);
    for effect in &effects {
        other.apply_effect(effect, None, None).unwrap();
    }
    assert_eq!(
        other.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert!(
        other
            .graph
            .authority_host()
            .committed_observed(other_v)
            .is_none()
    );
}

#[test]
fn observed_effect_automatic_flush_promotes_only_after_installation() {
    let (mut e, v) = setup();
    let result = record_c(&mut e, v);
    let effects = e.plan_vertex_effects(v, result).unwrap();
    e.config.max_overlay_memory_bytes = Some(1);
    let mut buffer = ComputedWriteBuffer::default();
    for effect in &effects {
        e.apply_effect_with_computed_writes(effect, None, None, Some(&mut buffer))
            .unwrap();
    }
    assert!(
        buffer.is_empty(),
        "the cap must actually force automatic flush"
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert!(e.graph.authority_host().committed_observed(v).is_some());
}

#[test]
fn observed_effect_failed_flush_and_request_cannot_promote_on_later_retry() {
    let (mut e, v) = setup();
    let mut buffer = ComputedWriteBuffer::default();
    let result = e.observe_evaluation_resource_request(EvaluationRequestKind::Full, |inner| {
        let result = record_c(inner, v);
        let effects = inner.plan_vertex_effects(v, result)?;
        for effect in &effects {
            inner.apply_effect_with_computed_writes(effect, None, None, Some(&mut buffer))?;
        }
        inner.fail_evaluation_commit_preflight_once_for_test = true;
        inner.flush_computed_write_buffer(&mut buffer)
    });
    assert!(
        result.is_err(),
        "the actual commit preflight must reject the flush"
    );
    assert!(
        !buffer.is_empty(),
        "failed preflight must not consume pending writes"
    );
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(10.0))
    );
    assert!(e.graph.authority_host().committed_observed(v).is_none());
    e.flush_computed_write_buffer(&mut buffer).unwrap();
    assert_eq!(
        e.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(20.0))
    );
    assert!(
        e.graph.authority_host().committed_observed(v).is_none(),
        "a failed request's read token cannot attest a retried write"
    );
}
