use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use formualizer_common::{ExcelErrorExtra, LiteralValue, ResourceExhaustionReason};
use formualizer_parse::parser::parse;

use crate::engine::eval::classify_mixed_topology_incomplete;
use crate::engine::resource_ledger::{resolve_resource_profile, split_legacy_memory_bytes};
use crate::engine::{
    AdmissionResourceBudget, ChangeLog, DeadlineResourceBudget, Engine, EvalConfig,
    EvaluationBudgets, EvaluationIncompleteReason, EvaluationResourceProfile,
    EvaluationResourceProfileKind, FormulaIngestBatch, FormulaIngestRecord, FormulaPlaneMode,
    FormulaPlaneTopologyCacheOutcome, LegacyResourceConfigDisposition, ResourceEnvelope,
    ResourceLedger, RetainedResourceBudget, ScratchResourceBudget, WorkResourceBudget,
};
use crate::formula_plane::scheduler::MixedTopologyCompileStats;
use crate::test_workbook::TestWorkbook;

fn formula_engine(
    mode: FormulaPlaneMode,
    profile: EvaluationResourceProfile,
) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default()
            .with_formula_plane_mode(mode)
            .with_resource_profile(profile),
    );
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=1+1").unwrap())
        .unwrap();
    engine
}

fn resource_reason(error: &formualizer_common::ExcelError) -> Option<ResourceExhaustionReason> {
    match &error.extra {
        ExcelErrorExtra::Resource { detail } => Some(detail.reason),
        _ => None,
    }
}

#[test]
fn compatibility_and_legacy_mapping_are_explicit_and_deterministic() {
    let resolved =
        resolve_resource_profile(&EvaluationResourceProfile::Compatibility, None, None, None);
    assert_eq!(resolved.kind, EvaluationResourceProfileKind::Compatibility);
    assert_eq!(resolved.budgets, EvaluationBudgets::default());
    assert!(resolved.diagnostic.is_none());

    assert_eq!(split_legacy_memory_bytes(9), (5, 4));
    let resolved = resolve_resource_profile(
        &EvaluationResourceProfile::Compatibility,
        Some(7),
        Some(3),
        Some(Duration::from_millis(25)),
    );
    assert_eq!(resolved.kind, EvaluationResourceProfileKind::Custom);
    assert_eq!(resolved.budgets.admission.graph_vertex_hard_limit, Some(7));
    assert_eq!(
        resolved.budgets.retained.total_bytes,
        Some(3 * 1024 * 1024 / 2)
    );
    assert_eq!(
        resolved.budgets.scratch.total_bytes,
        Some(3 * 1024 * 1024 / 2)
    );
    assert_eq!(
        resolved.budgets.deadline.max_elapsed,
        Some(Duration::from_millis(25))
    );
    let diagnostic = resolved.diagnostic.unwrap();
    assert_eq!(
        diagnostic.disposition,
        LegacyResourceConfigDisposition::MappedToCustom
    );
    assert!(diagnostic.graph_admission_activation_deferred_to_c2);

    let custom = EvaluationBudgets {
        work: WorkResourceBudget {
            max_work_units: Some(11),
        },
        ..EvaluationBudgets::default()
    };
    let resolved = resolve_resource_profile(
        &EvaluationResourceProfile::Custom(custom.clone()),
        Some(1),
        Some(1),
        Some(Duration::ZERO),
    );
    assert_eq!(resolved.budgets, custom);
    assert_eq!(
        resolved.diagnostic.unwrap().disposition,
        LegacyResourceConfigDisposition::IgnoredByExplicitProfile
    );
}

#[test]
fn ledger_reservations_release_overflow_and_work_are_checked() {
    let budgets = EvaluationBudgets {
        retained: RetainedResourceBudget {
            total_bytes: Some(10),
            ..RetainedResourceBudget::default()
        },
        scratch: ScratchResourceBudget {
            total_bytes: Some(8),
            ..ScratchResourceBudget::default()
        },
        work: WorkResourceBudget {
            max_work_units: Some(3),
        },
        ..EvaluationBudgets::default()
    };
    let mut ledger = ResourceLedger::new(Some(41), budgets);
    ledger.reserve_retained(7).unwrap();
    ledger.release_retained(2).unwrap();
    assert!(ledger.reserve_retained(6).is_err());
    ledger.reserve_scratch(8).unwrap();
    ledger.release_scratch(8).unwrap();
    assert!(ledger.release_scratch(1).is_err());
    ledger.charge_work(3).unwrap();
    let error = ledger.charge_work(1).unwrap_err();
    let excel = error.into_excel_error();
    assert_eq!(
        resource_reason(&excel),
        Some(ResourceExhaustionReason::WorkUnits)
    );
    let snapshot = ledger.snapshot();
    assert_eq!(snapshot.retained_peak, 7);
    assert_eq!(snapshot.scratch_peak, 8);
    assert_eq!(snapshot.work_charged, 3);
}

#[test]
fn fake_clock_deadline_is_monotonic_and_typed() {
    let now_ns = Arc::new(AtomicU64::new(0));
    let clock_value = Arc::clone(&now_ns);
    let budgets = EvaluationBudgets {
        deadline: DeadlineResourceBudget {
            max_elapsed: Some(Duration::from_nanos(10)),
        },
        ..EvaluationBudgets::default()
    };
    let mut ledger = ResourceLedger::with_test_elapsed_clock(
        Some(9),
        budgets,
        Arc::new(move || Duration::from_nanos(clock_value.load(Ordering::SeqCst))),
    );
    ledger.checkpoint_deadline().unwrap();
    now_ns.store(10, Ordering::SeqCst);
    let excel = ledger.checkpoint_deadline().unwrap_err().into_excel_error();
    assert_eq!(
        resource_reason(&excel),
        Some(ResourceExhaustionReason::Deadline)
    );
    assert_eq!(ledger.snapshot().deadline_checkpoints, 2);
}

#[test]
fn work_and_deadline_errors_are_common_across_modes() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let work_profile = EvaluationResourceProfile::Custom(EvaluationBudgets {
            work: WorkResourceBudget {
                max_work_units: Some(0),
            },
            ..EvaluationBudgets::default()
        });
        let error = formula_engine(mode, work_profile)
            .evaluate_all()
            .unwrap_err();
        assert_eq!(
            resource_reason(&error),
            Some(ResourceExhaustionReason::WorkUnits)
        );

        let deadline_profile = EvaluationResourceProfile::Custom(EvaluationBudgets {
            deadline: DeadlineResourceBudget {
                max_elapsed: Some(Duration::ZERO),
            },
            ..EvaluationBudgets::default()
        });
        let error = formula_engine(mode, deadline_profile)
            .evaluate_all()
            .unwrap_err();
        assert_eq!(
            resource_reason(&error),
            Some(ResourceExhaustionReason::Deadline)
        );
    }
}

#[test]
fn default_compatibility_preserves_acceptance_and_reports_ledger() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut engine = formula_engine(mode, EvaluationResourceProfile::Compatibility);
        engine.evaluate_all().unwrap();
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(2.0))
        );
        let request = engine.last_evaluation_resource_request_stats().unwrap();
        assert_eq!(
            request.ledger.profile,
            EvaluationResourceProfileKind::Compatibility
        );
        assert_eq!(request.ledger.work_limit, None);
        assert!(request.ledger.deadline_checkpoints > 0);
        assert_eq!(request.ledger.exhaustion, None);
    }
}

#[test]
fn deferred_preparation_work_and_deadline_failures_are_retry_safe() {
    for (profile, expected) in [
        (
            EvaluationResourceProfile::Custom(EvaluationBudgets {
                work: WorkResourceBudget {
                    max_work_units: Some(0),
                },
                ..EvaluationBudgets::default()
            }),
            ResourceExhaustionReason::WorkUnits,
        ),
        (
            EvaluationResourceProfile::Custom(EvaluationBudgets {
                deadline: DeadlineResourceBudget {
                    max_elapsed: Some(Duration::ZERO),
                },
                ..EvaluationBudgets::default()
            }),
            ResourceExhaustionReason::Deadline,
        ),
    ] {
        let mut config = EvalConfig::default().with_resource_profile(profile);
        config.defer_graph_building = true;
        let mut engine = Engine::new(TestWorkbook::default(), config);
        let _ = engine.add_sheet("Sheet1");
        engine.stage_formula_text("Sheet1", 1, 2, "=A1+1".to_string());
        let before = engine.baseline_stats();
        let error = engine.evaluate_cell("Sheet1", 1, 2).unwrap_err();
        assert_eq!(resource_reason(&error), Some(expected));
        assert_eq!(engine.staged_formula_count(), 1);
        let after = engine.baseline_stats();
        assert_eq!(after.graph_vertex_count, before.graph_vertex_count);
        assert_eq!(after.graph_edge_count, before.graph_edge_count);
    }
}

#[test]
fn structural_topology_incompleteness_is_not_mislabeled_as_a_cap() {
    assert_eq!(
        classify_mixed_topology_incomplete(&MixedTopologyCompileStats::default()),
        EvaluationIncompleteReason::FormulaPlaneTopologySemanticStructural
    );
}

#[test]
fn graph_caps_are_declarative_across_staged_and_generic_paths_in_all_modes() {
    fn prepared(
        mode: FormulaPlaneMode,
        profile: EvaluationResourceProfile,
    ) -> Engine<TestWorkbook> {
        let mut config = EvalConfig::default()
            .with_formula_plane_mode(mode)
            .with_resource_profile(profile);
        config.defer_graph_building = true;
        let mut engine = Engine::new(TestWorkbook::default(), config);
        let _ = engine.add_sheet("Sheet1");
        let _ = engine.add_sheet("Other");
        engine.stage_formula_text("Sheet1", 1, 2, "=A1+1".to_string());
        engine.stage_formula_text("Other", 1, 2, "=A1+2".to_string());
        engine
            .build_graph_for_sheets(["Sheet1"])
            .expect("C1a profiles must not change selected staged acceptance");
        assert_eq!(engine.staged_formula_count(), 1);
        engine
            .build_graph_all()
            .expect("C1a profiles must not change prepare-all acceptance");
        engine
            .evaluate_all()
            .expect("generic evaluation checkpoints must not fail after staging");
        engine
    }

    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let baseline = prepared(mode, EvaluationResourceProfile::Compatibility);
        let capped = prepared(
            mode,
            EvaluationResourceProfile::Custom(EvaluationBudgets {
                admission: AdmissionResourceBudget {
                    graph_vertex_hard_limit: Some(0),
                    graph_edge_hard_limit: Some(0),
                    ..AdmissionResourceBudget::default()
                },
                ..EvaluationBudgets::default()
            }),
        );
        let baseline_stats = baseline.baseline_stats();
        let capped_stats = capped.baseline_stats();
        assert_eq!(
            capped_stats.graph_vertex_count,
            baseline_stats.graph_vertex_count
        );
        assert_eq!(
            capped_stats.graph_edge_count,
            baseline_stats.graph_edge_count
        );
        assert_eq!(
            capped_stats.formula_plane_active_span_count,
            baseline_stats.formula_plane_active_span_count
        );
        assert_eq!(
            capped.get_cell_value("Sheet1", 1, 2),
            baseline.get_cell_value("Sheet1", 1, 2)
        );
        assert_eq!(
            capped.get_cell_value("Other", 1, 2),
            baseline.get_cell_value("Other", 1, 2)
        );
        assert_eq!(
            capped
                .last_evaluation_resource_request_stats()
                .unwrap()
                .ledger
                .exhaustion,
            None
        );
    }
}

#[test]
fn authoritative_staged_spans_do_not_charge_hypothetical_legacy_vertices() {
    let mut config = EvalConfig::default()
        .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
        .with_resource_profile(EvaluationResourceProfile::Custom(EvaluationBudgets {
            admission: AdmissionResourceBudget {
                graph_vertex_hard_limit: Some(0),
                graph_edge_hard_limit: Some(0),
                ..AdmissionResourceBudget::default()
            },
            ..EvaluationBudgets::default()
        }));
    config.defer_graph_building = true;
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let _ = engine.add_sheet("Sheet1");
    for row in 1..=100 {
        engine.stage_formula_text("Sheet1", row, 2, format!("=A{row}*2"));
    }
    engine.build_graph_all().unwrap();
    assert_eq!(engine.staged_formula_count(), 0);
    assert_eq!(engine.baseline_stats().graph_vertex_count, 0);
    assert_eq!(engine.baseline_stats().graph_edge_count, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
}

#[test]
fn c1a_retained_and_scratch_budgets_are_observational_without_new_skip_or_demotion() {
    for budgets in [
        EvaluationBudgets {
            scratch: ScratchResourceBudget {
                total_bytes: Some(0),
                ..ScratchResourceBudget::default()
            },
            ..EvaluationBudgets::default()
        },
        EvaluationBudgets {
            retained: RetainedResourceBudget {
                total_bytes: Some(0),
                ..RetainedResourceBudget::default()
            },
            ..EvaluationBudgets::default()
        },
    ] {
        let profile = EvaluationResourceProfile::Custom(budgets);
        let mut engine = Engine::new(
            TestWorkbook::default(),
            EvalConfig::default()
                .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
                .with_resource_profile(profile),
        );
        let mut records = Vec::new();
        for row in 1..=100 {
            let formula = format!("=A{row}+1");
            let ast = parse(&formula).unwrap();
            let ast_id = engine.intern_formula_ast(&ast);
            records.push(FormulaIngestRecord::new(
                row,
                2,
                ast_id,
                Some(formula.into()),
            ));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
            .unwrap();
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
        engine.evaluate_all().unwrap();
        assert!(engine.mixed_topology_cache_present_for_test());
        assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
        assert_eq!(
            engine.get_cell_value("Sheet1", 100, 2),
            Some(LiteralValue::Number(1.0))
        );
    }
}

#[test]
fn span_work_exhaustion_happens_before_value_or_dirty_acknowledgement() {
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
            .with_resource_profile(EvaluationResourceProfile::Custom(EvaluationBudgets {
                work: WorkResourceBudget {
                    max_work_units: Some(1),
                },
                ..EvaluationBudgets::default()
            })),
    );
    let mut records = Vec::new();
    for row in 1..=4 {
        let ast = parse(format!("=A{row}+1")).unwrap();
        let ast_id = engine.intern_formula_ast(&ast);
        records.push(FormulaIngestRecord::new(row, 2, ast_id, None));
    }
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
        .unwrap();
    let before = engine.baseline_stats();
    let before_value = engine.get_cell_value("Sheet1", 4, 2);

    let error = engine.evaluate_all().unwrap_err();
    assert_eq!(
        resource_reason(&error),
        Some(ResourceExhaustionReason::WorkUnits)
    );
    assert_eq!(engine.get_cell_value("Sheet1", 4, 2), before_value);
    let after = engine.baseline_stats();
    assert_eq!(
        after.formula_plane_dirty_pending_events,
        before.formula_plane_dirty_pending_events
    );
}

#[test]
fn existing_materialization_guard_is_typed_without_changing_error_kind() {
    let mut config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    config.max_formula_plane_cache_candidates = 0;
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut limits = engine.workbook_load_limits().clone();
    limits.max_formula_plane_fallback_cells = 1;
    engine.set_workbook_load_limits(limits);

    let mut records = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        let formula = format!("=A{row}+1");
        let ast_id = engine.intern_formula_ast(&parse(&formula).unwrap());
        records.push(FormulaIngestRecord::new(row, 2, ast_id, None));
    }
    let tail_id = engine.intern_formula_ast(&parse("=B100+1").unwrap());
    records.push(FormulaIngestRecord::new(1, 3, tail_id, None));
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
        .unwrap();
    let before = engine.baseline_stats();
    let error = engine.evaluate_all().unwrap_err();
    assert_eq!(error.kind, formualizer_common::ExcelErrorKind::NImpl);
    assert_eq!(
        resource_reason(&error),
        Some(ResourceExhaustionReason::MaterializationCells)
    );
    let after = engine.baseline_stats();
    assert_eq!(after.graph_vertex_count, before.graph_vertex_count);
    assert_eq!(after.graph_edge_count, before.graph_edge_count);
    assert_eq!(after.formula_plane_active_span_count, 1);
    assert_eq!(
        after.formula_plane_dirty_pending_events,
        before.formula_plane_dirty_pending_events
    );
}

#[test]
fn skipped_topology_is_typed_atomic_and_never_cached() {
    let mut config = EvalConfig::default()
        .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
        .with_resource_profile(EvaluationResourceProfile::Custom(EvaluationBudgets {
            admission: AdmissionResourceBudget {
                materialization_cells: Some(0),
                ..AdmissionResourceBudget::default()
            },
            ..EvaluationBudgets::default()
        }));
    config.max_formula_plane_cache_candidates = 0;
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut records = Vec::new();
    for row in 1..=100 {
        engine
            .set_cell_value("Sheet1", row, 1, LiteralValue::Number(row as f64))
            .unwrap();
        let formula = format!("=A{row}*2");
        let ast = parse(&formula).unwrap();
        let ast_id = engine.intern_formula_ast(&ast);
        records.push(FormulaIngestRecord::new(
            row,
            2,
            ast_id,
            Some(formula.into()),
        ));
    }
    let tail = parse("=B100+1").unwrap();
    let tail_id = engine.intern_formula_ast(&tail);
    records.push(FormulaIngestRecord::new(
        1,
        3,
        tail_id,
        Some("=B100+1".into()),
    ));
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
        .unwrap();
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    engine.evaluate_all().unwrap();
    let request = engine.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(
        request.topology.cache_outcome,
        FormulaPlaneTopologyCacheOutcome::SkippedOverflow
    );
    assert_eq!(
        request.topology.incomplete_reason,
        Some(EvaluationIncompleteReason::FormulaPlaneTopologyCandidates)
    );
    assert_eq!(request.topology.cache_skip_events, 1);
    assert!(!engine.mixed_topology_cache_present_for_test());
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 0);
    assert_eq!(
        engine.get_cell_value("Sheet1", 100, 2),
        Some(LiteralValue::Number(200.0))
    );
}

#[test]
fn evaluate_vertex_max_work_zero_matches_all_modes_without_publication() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let profile = EvaluationResourceProfile::Custom(EvaluationBudgets {
            work: WorkResourceBudget {
                max_work_units: Some(0),
            },
            ..EvaluationBudgets::default()
        });
        let mut engine = formula_engine(mode, profile);
        let address = engine.graph.make_cell_ref("Sheet1", 1, 1);
        let vertex = *engine
            .graph
            .get_vertex_id_for_address(&address)
            .expect("formula vertex");
        let before = engine.get_cell_value("Sheet1", 1, 1);

        let error = engine.evaluate_vertex(vertex).unwrap_err();
        assert_eq!(
            resource_reason(&error),
            Some(ResourceExhaustionReason::WorkUnits)
        );
        assert_eq!(engine.get_cell_value("Sheet1", 1, 1), before);
    }
}

#[test]
fn explicit_profiles_ignore_legacy_vertex_limit_at_the_shared_demotion_seam() {
    fn demote(profile: EvaluationResourceProfile) -> Result<(), String> {
        let mut config = EvalConfig::default()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
            .with_resource_profile(profile);
        config.max_vertices = Some(0);
        let mut engine = Engine::new(TestWorkbook::default(), config);
        let mut formulas = Vec::new();
        for row in 1..=100 {
            let formula = format!("=A{row}*2");
            let ast = parse(&formula).unwrap();
            let ast_id = engine.intern_formula_ast(&ast);
            formulas.push(FormulaIngestRecord::new(
                row,
                2,
                ast_id,
                Some(Arc::<str>::from(formula)),
            ));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", formulas)])
            .unwrap();
        let refs = engine.graph.formula_authority().active_span_refs();
        let prepared = engine
            .prepare_formula_span_demotion(&refs)
            .map_err(|error| error.to_string())?;
        engine
            .commit_prepared_formula_span_demotion(prepared)
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    let mut constrained = ResourceEnvelope::finance_balanced();
    constrained.max_work_units = 1_000_000;
    for profile in [
        EvaluationResourceProfile::FinanceBalanced,
        EvaluationResourceProfile::Constrained(constrained),
        EvaluationResourceProfile::Custom(EvaluationBudgets::default()),
    ] {
        demote(profile).expect(
            "capacity, cycle, name, and structural routes share an explicit-profile demotion seam",
        );
    }

    let error = demote(EvaluationResourceProfile::Compatibility).unwrap_err();
    assert!(error.contains("resource") || error.contains("vertex"));
}

#[test]
fn logged_resource_failures_close_groups_and_keep_dirty_retryable() {
    use crate::engine::graph::editor::change_log::ChangeEvent;

    let work_limited = EvaluationResourceProfile::Custom(EvaluationBudgets {
        work: WorkResourceBudget {
            max_work_units: Some(1),
        },
        ..EvaluationBudgets::default()
    });
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_resource_profile(work_limited),
    );
    engine
        .set_cell_formula("Sheet1", 1, 1, parse("=SEQUENCE(2,1)").unwrap())
        .unwrap();
    engine
        .set_cell_formula("Sheet1", 1, 3, parse("=A1+1").unwrap())
        .unwrap();
    let mut log = ChangeLog::new();
    let error = engine.evaluate_all_logged(&mut log).unwrap_err();
    assert_eq!(
        resource_reason(&error),
        Some(ResourceExhaustionReason::WorkUnits)
    );
    assert_eq!(log.compound_depth(), 0);
    assert!(matches!(
        log.events().first(),
        Some(ChangeEvent::CompoundStart { .. })
    ));
    assert!(
        log.events()
            .iter()
            .any(|event| matches!(event, ChangeEvent::SpillCommitted { .. }))
    );
    assert!(matches!(
        log.events().last(),
        Some(ChangeEvent::CompoundEnd { .. })
    ));
    assert_eq!(log.meta(0).unwrap().1, log.meta(log.len() - 1).unwrap().1);

    engine.set_evaluation_resource_profile_for_test(EvaluationResourceProfile::Compatibility);
    engine.evaluate_all_logged(&mut log).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(log.compound_depth(), 0);

    let deadline_limited = EvaluationResourceProfile::Custom(EvaluationBudgets {
        deadline: DeadlineResourceBudget {
            max_elapsed: Some(Duration::ZERO),
        },
        ..EvaluationBudgets::default()
    });
    let mut engine = formula_engine(FormulaPlaneMode::Off, deadline_limited);
    let mut log = ChangeLog::new();
    let error = engine.evaluate_all_logged(&mut log).unwrap_err();
    assert_eq!(
        resource_reason(&error),
        Some(ResourceExhaustionReason::Deadline)
    );
    assert_eq!(log.compound_depth(), 0);
    engine.set_evaluation_resource_profile_for_test(EvaluationResourceProfile::Compatibility);
    engine.evaluate_all_logged(&mut log).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 1),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(log.compound_depth(), 0);
    assert!(matches!(
        log.events().first(),
        Some(ChangeEvent::CompoundStart { .. })
    ));
    assert!(matches!(
        log.events().last(),
        Some(ChangeEvent::CompoundEnd { .. })
    ));
}
