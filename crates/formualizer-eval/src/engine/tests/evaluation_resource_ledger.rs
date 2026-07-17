use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use formualizer_common::{ExcelErrorExtra, LiteralValue, ResourceExhaustionReason};
use formualizer_parse::parser::parse;

use crate::engine::eval::classify_mixed_topology_incomplete;
use crate::engine::named_range::{NameScope, NamedDefinition};
use crate::engine::resource_ledger::{resolve_evaluation_budgets, split_legacy_memory_bytes};
use crate::engine::{
    AdmissionResourceBudget, ChangeLog, DeadlineResourceBudget, DiskScratchPolicy, Engine,
    EvalConfig, EvaluationBudgets, EvaluationIncompleteReason, FormulaIngestBatch,
    FormulaIngestRecord, FormulaPlaneMode, FormulaPlaneTopologyCacheOutcome,
    LegacyResourceConfigDisposition, ResourceEnvelope, ResourceLedger, RetainedResourceBudget,
    ScratchResourceBudget, VertexId, VertexKind, WorkResourceBudget,
};
use crate::formula_plane::scheduler::MixedTopologyCompileStats;
use crate::reference::{CellRef, Coord, RangeRef};
use crate::test_workbook::TestWorkbook;

fn formula_engine(mode: FormulaPlaneMode, budgets: EvaluationBudgets) -> Engine<TestWorkbook> {
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default()
            .with_formula_plane_mode(mode)
            .with_evaluation_budgets(budgets),
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
fn explicit_and_legacy_budgets_merge_at_field_level_deterministically() {
    let resolved = resolve_evaluation_budgets(&EvaluationBudgets::default(), None, None, None);
    assert_eq!(resolved.budgets, EvaluationBudgets::default());
    assert!(resolved.diagnostic.is_none());

    assert_eq!(split_legacy_memory_bytes(9), (5, 4));
    let explicit = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_vertex_hard_limit: Some(99),
            ..AdmissionResourceBudget::default()
        },
        retained: RetainedResourceBudget {
            total_bytes: Some(42),
            ..RetainedResourceBudget::default()
        },
        work: WorkResourceBudget {
            max_work_units: Some(11),
        },
        ..EvaluationBudgets::default()
    };
    let resolved =
        resolve_evaluation_budgets(&explicit, Some(7), Some(3), Some(Duration::from_millis(25)));
    assert_eq!(resolved.budgets.admission.graph_vertex_hard_limit, Some(99));
    assert_eq!(resolved.budgets.retained.total_bytes, Some(42));
    assert_eq!(
        resolved.budgets.scratch.total_bytes,
        Some(3 * 1024 * 1024 / 2)
    );
    assert_eq!(
        resolved.budgets.deadline.max_elapsed,
        Some(Duration::from_millis(25))
    );
    assert_eq!(resolved.budgets.work.max_work_units, Some(11));

    let diagnostic = resolved.diagnostic.unwrap();
    assert_eq!(
        diagnostic.max_vertices,
        LegacyResourceConfigDisposition::IgnoredByExplicitBudget
    );
    assert_eq!(
        diagnostic.max_memory_mb_retained,
        LegacyResourceConfigDisposition::IgnoredByExplicitBudget
    );
    assert_eq!(
        diagnostic.max_memory_mb_scratch,
        LegacyResourceConfigDisposition::Mapped
    );
    assert_eq!(
        diagnostic.max_eval_time,
        LegacyResourceConfigDisposition::Mapped
    );
    assert!(!diagnostic.graph_admission_activation_deferred_to_c2);
}

#[test]
fn aggregate_envelope_derives_budgets_without_selecting_a_mode() {
    let envelope = ResourceEnvelope {
        retained_bytes: 800,
        request_scratch_bytes: 200,
        materialized_graph_bytes: 1_000,
        max_work_units: 77,
        deadline: Some(Duration::from_millis(12)),
        max_threads: 3,
        disk_scratch: DiskScratchPolicy::MemoryOnly,
    };
    let budgets = envelope.to_budgets();

    assert_eq!(budgets.retained.total_bytes, Some(800));
    assert_eq!(budgets.retained.mixed_cache_bytes, Some(60));
    assert_eq!(budgets.retained.lookup_cache_bytes, Some(40));
    assert_eq!(budgets.scratch.total_bytes, Some(200));
    assert_eq!(budgets.scratch.schedule_discovery_bytes, Some(100));
    assert_eq!(budgets.scratch.graph_source_bytes, Some(70));
    assert_eq!(budgets.scratch.spill_overlay_bytes, Some(30));
    assert_eq!(
        budgets.scratch.disk_scratch_policy,
        Some(DiskScratchPolicy::MemoryOnly)
    );
    let native_budgets = ResourceEnvelope {
        disk_scratch: DiskScratchPolicy::NativeTemporary,
        ..envelope
    }
    .to_budgets();
    assert_eq!(
        native_budgets.scratch.disk_scratch_policy,
        Some(DiskScratchPolicy::NativeTemporary)
    );
    assert_ne!(
        budgets.scratch.disk_scratch_policy,
        native_budgets.scratch.disk_scratch_policy
    );
    assert_eq!(budgets.admission.materialized_graph_bytes, Some(1_000));
    assert_eq!(budgets.work.max_work_units, Some(77));
    assert_eq!(
        budgets.deadline.max_elapsed,
        Some(Duration::from_millis(12))
    );
    assert_eq!(budgets.optimization.max_threads, Some(3));

    let large_budgets = ResourceEnvelope {
        retained_bytes: u64::MAX,
        ..envelope
    }
    .to_budgets();
    let derived_limit = (u64::MAX / 8).saturating_mul(60) / 100 / 64;
    let clamped_limit = usize::try_from(derived_limit).unwrap_or(usize::MAX);
    assert_eq!(
        large_budgets.optimization.mixed_cache_candidates,
        Some(clamped_limit)
    );
    assert_eq!(
        large_budgets.optimization.mixed_cache_edges,
        Some(clamped_limit)
    );
}

#[test]
fn evaluate_vertex_all_unset_preserves_non_formula_compatibility() {
    let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
    engine
        .set_cell_value("Sheet1", 1, 1, LiteralValue::Number(42.0))
        .unwrap();
    let cell = engine.graph.make_cell_ref("Sheet1", 1, 1);
    let cell_vertex = *engine
        .graph
        .get_vertex_id_for_address(&cell)
        .expect("literal cell vertex");
    engine
        .graph
        .update_vertex_value(cell_vertex, LiteralValue::Number(-1.0));
    let graph_value_before = engine.vertex_value(cell_vertex);

    assert_eq!(
        engine.evaluate_vertex(cell_vertex).unwrap(),
        LiteralValue::Number(42.0)
    );
    assert_eq!(
        engine.vertex_value(cell_vertex),
        graph_value_before,
        "direct literal reads must not publish through the formula effects path"
    );

    let error = engine.evaluate_vertex(VertexId::new(u32::MAX)).unwrap_err();
    assert_eq!(error.kind, formualizer_common::ExcelErrorKind::Ref);

    let range = RangeRef::new(
        CellRef::new(0, Coord::new(0, 0, true, true)),
        CellRef::new(0, Coord::new(0, 0, true, true)),
    );
    engine
        .define_name(
            "ScalarRangeMismatch",
            NamedDefinition::Range(range),
            NameScope::Workbook,
        )
        .unwrap();
    let named_vertex = engine
        .graph
        .named_ranges_iter()
        .find(|(name, _)| name.as_str() == "ScalarRangeMismatch")
        .map(|(_, named)| named.vertex)
        .expect("named range vertex");
    engine.graph.set_kind(named_vertex, VertexKind::NamedScalar);

    let error = engine.evaluate_vertex(named_vertex).unwrap_err();
    assert_eq!(error.kind, formualizer_common::ExcelErrorKind::Value);
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
fn mixed_cache_accounting_is_idempotent_and_replaces_retained_ownership() {
    let mut ledger = ResourceLedger::new(
        Some(42),
        EvaluationBudgets {
            retained: RetainedResourceBudget {
                total_bytes: Some(10),
                mixed_cache_bytes: Some(10),
                ..RetainedResourceBudget::default()
            },
            ..EvaluationBudgets::default()
        },
    );
    ledger.account_mixed_cache(6).unwrap();
    ledger.account_mixed_cache(6).unwrap();
    assert_eq!(ledger.snapshot().retained_current, 6);
    assert_eq!(ledger.snapshot().retained_peak, 6);
    ledger.account_mixed_cache(4).unwrap();
    assert_eq!(ledger.snapshot().retained_current, 4);
    assert_eq!(ledger.snapshot().retained_peak, 6);
    ledger.account_mixed_cache(0).unwrap();
    assert_eq!(ledger.snapshot().retained_current, 0);
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
        let work_budgets = EvaluationBudgets {
            work: WorkResourceBudget {
                max_work_units: Some(0),
            },
            ..EvaluationBudgets::default()
        };
        let error = formula_engine(mode, work_budgets)
            .evaluate_all()
            .unwrap_err();
        assert_eq!(
            resource_reason(&error),
            Some(ResourceExhaustionReason::WorkUnits)
        );

        let deadline_budgets = EvaluationBudgets {
            deadline: DeadlineResourceBudget {
                max_elapsed: Some(Duration::ZERO),
            },
            ..EvaluationBudgets::default()
        };
        let error = formula_engine(mode, deadline_budgets)
            .evaluate_all()
            .unwrap_err();
        assert_eq!(
            resource_reason(&error),
            Some(ResourceExhaustionReason::Deadline)
        );
    }
}

#[test]
fn default_unset_budgets_preserve_acceptance_and_report_ledger() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut engine = formula_engine(mode, EvaluationBudgets::default());
        engine.evaluate_all().unwrap();
        assert_eq!(
            engine.get_cell_value("Sheet1", 1, 1),
            Some(LiteralValue::Number(2.0))
        );
        let request = *engine.last_evaluation_resource_request_stats().unwrap();
        assert_eq!(request.ledger.work_limit, None);
        assert!(request.ledger.deadline_checkpoints > 0);
        assert_eq!(request.ledger.exhaustion, None);
    }
}

#[test]
fn deferred_preparation_work_and_deadline_failures_are_retry_safe() {
    for (budgets, expected) in [
        (
            EvaluationBudgets {
                work: WorkResourceBudget {
                    max_work_units: Some(0),
                },
                ..EvaluationBudgets::default()
            },
            ResourceExhaustionReason::WorkUnits,
        ),
        (
            EvaluationBudgets {
                deadline: DeadlineResourceBudget {
                    max_elapsed: Some(Duration::ZERO),
                },
                ..EvaluationBudgets::default()
            },
            ResourceExhaustionReason::Deadline,
        ),
    ] {
        let mut config = EvalConfig::default().with_evaluation_budgets(budgets);
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
fn graph_caps_are_authoritative_and_atomic_across_staged_modes() {
    for mode in [
        FormulaPlaneMode::Off,
        FormulaPlaneMode::Shadow,
        FormulaPlaneMode::AuthoritativeExperimental,
    ] {
        let mut config = EvalConfig::default()
            .with_formula_plane_mode(mode)
            .with_evaluation_budgets(EvaluationBudgets {
                admission: AdmissionResourceBudget {
                    graph_vertex_hard_limit: Some(0),
                    graph_edge_hard_limit: Some(0),
                    ..AdmissionResourceBudget::default()
                },
                ..EvaluationBudgets::default()
            });
        config.defer_graph_building = true;
        let mut engine = Engine::new(TestWorkbook::default(), config);
        let _ = engine.add_sheet("Sheet1");
        engine.stage_formula_text("Sheet1", 1, 2, "=A1+1".to_string());
        let before = engine.baseline_stats();
        let error = engine.build_graph_all().unwrap_err();
        assert_eq!(
            resource_reason(&error),
            Some(ResourceExhaustionReason::GraphVertices)
        );
        assert_eq!(
            engine.baseline_stats().graph_vertex_count,
            before.graph_vertex_count
        );
        assert_eq!(
            engine.baseline_stats().graph_edge_count,
            before.graph_edge_count
        );
        assert_eq!(engine.staged_formula_count(), 1);
    }
}

#[test]
fn authoritative_staged_spans_do_not_charge_hypothetical_legacy_vertices() {
    let mut config = EvalConfig::default()
        .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
        .with_evaluation_budgets(EvaluationBudgets {
            admission: AdmissionResourceBudget {
                graph_vertex_hard_limit: Some(0),
                graph_edge_hard_limit: Some(0),
                ..AdmissionResourceBudget::default()
            },
            ..EvaluationBudgets::default()
        });
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
fn c1b_activates_only_mixed_cache_and_schedule_discovery_memory() {
    let build = |budgets| {
        let mut engine = Engine::new(
            TestWorkbook::default(),
            EvalConfig::default()
                .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
                .with_evaluation_budgets(budgets),
        );
        let mut records = Vec::new();
        for row in 1..=100 {
            let formula = format!("=A{row}+1");
            let ast_id = engine.intern_formula_ast(&parse(&formula).unwrap());
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
        engine
    };

    let mut retained = build(EvaluationBudgets {
        retained: RetainedResourceBudget {
            total_bytes: Some(0),
            mixed_cache_bytes: Some(0),
            ..RetainedResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    });
    retained.evaluate_all().unwrap();
    assert!(!retained.mixed_topology_cache_present_for_test());
    assert_eq!(retained.baseline_stats().formula_plane_active_span_count, 1);

    let mut scratch = build(EvaluationBudgets {
        scratch: ScratchResourceBudget {
            total_bytes: Some(0),
            schedule_discovery_bytes: Some(0),
            ..ScratchResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    });
    assert_eq!(scratch.mixed_topology_index_builds_for_test(), 0);
    let error = scratch.evaluate_all().unwrap_err();
    assert_eq!(
        resource_reason(&error),
        Some(ResourceExhaustionReason::ScratchMemory)
    );
    assert_eq!(
        scratch.mixed_topology_index_builds_for_test(),
        0,
        "index scratch preflight must fail before temporary indexes are constructed",
    );
    let request = scratch.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(request.ledger.scratch_current, 0);
    assert_eq!(request.ledger.scratch_peak, 0);
    assert_eq!(scratch.baseline_stats().formula_plane_active_span_count, 1);
    assert!(scratch.baseline_stats().formula_plane_dirty_pending_events > 0);
}

#[test]
fn exact_schedule_work_error_releases_reserved_scratch() {
    let build = |work_limit| {
        let mut config = EvalConfig::default()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
            .with_evaluation_budgets(EvaluationBudgets {
                work: WorkResourceBudget {
                    max_work_units: work_limit,
                },
                ..EvaluationBudgets::default()
            });
        config.max_formula_plane_cache_candidates = 0;
        let mut engine = Engine::new(TestWorkbook::default(), config);
        let mut records = Vec::new();
        for row in 1..=100 {
            let formula = format!("=A{row}+1");
            let ast_id = engine.intern_formula_ast(&parse(&formula).unwrap());
            records.push(FormulaIngestRecord::new(row, 2, ast_id, None));
        }
        for col in 3..=12 {
            let tail = engine.intern_formula_ast(&parse("=B32+1").unwrap());
            records.push(FormulaIngestRecord::new(1, col, tail, None));
        }
        engine
            .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
            .unwrap();
        engine
    };

    let mut unlimited = build(None);
    unlimited.evaluate_all().unwrap();
    let total_work = unlimited
        .last_evaluation_resource_request_stats()
        .unwrap()
        .ledger
        .work_charged;
    let mut witnessed = None;
    for limit in 0..total_work {
        let mut engine = build(Some(limit));
        let result = engine.evaluate_all();
        let request = *engine.last_evaluation_resource_request_stats().unwrap();
        if result.is_err()
            && request.topology.exact_pass_count == 0
            && request.ledger.scratch_peak > 0
        {
            witnessed = Some(request);
            break;
        }
    }
    let request = witnessed.expect("a work limit must fail inside exact schedule construction");
    assert_eq!(request.ledger.scratch_current, 0);
    assert_eq!(
        request.ledger.exhaustion,
        Some(ResourceExhaustionReason::WorkUnits)
    );
}

#[test]
fn span_work_exhaustion_happens_before_value_or_dirty_acknowledgement() {
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
            .with_evaluation_budgets(EvaluationBudgets {
                work: WorkResourceBudget {
                    max_work_units: Some(1),
                },
                ..EvaluationBudgets::default()
            }),
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
fn cache_overflow_does_not_charge_existing_materialization_guard() {
    let mut config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
    config.max_formula_plane_cache_candidates = 0;
    let mut engine = Engine::new(TestWorkbook::default(), config);
    let mut limits = engine.workbook_load_limits().clone();
    limits.max_formula_plane_fallback_cells = 0;
    engine.set_workbook_load_limits(limits);
    let mut records = Vec::new();
    for row in 1..=100 {
        let formula = format!("=A{row}+1");
        let ast_id = engine.intern_formula_ast(&parse(&formula).unwrap());
        records.push(FormulaIngestRecord::new(row, 2, ast_id, None));
    }
    let tail_id = engine.intern_formula_ast(&parse("=B100+1").unwrap());
    records.push(FormulaIngestRecord::new(1, 3, tail_id, None));
    engine
        .ingest_formula_batches(vec![FormulaIngestBatch::new("Sheet1", records)])
        .unwrap();

    engine.evaluate_all().unwrap();
    let request = engine.last_evaluation_resource_request_stats().unwrap();
    assert_eq!(request.fallback_materialized_cells, 0);
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(2.0))
    );
}

#[test]
fn skipped_topology_is_typed_atomic_and_never_cached() {
    let mut config =
        EvalConfig::default().with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental);
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
    engine.set_evaluation_budgets_for_test(EvaluationBudgets {
        admission: AdmissionResourceBudget {
            materialization_cells: Some(0),
            ..AdmissionResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    });
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
    assert_eq!(engine.mixed_topology_index_builds_for_test(), 1);
    assert_eq!(request.ledger.scratch_current, 0);
    assert!(request.ledger.scratch_peak > 0);
    assert!(!engine.mixed_topology_cache_present_for_test());
    assert_eq!(engine.baseline_stats().formula_plane_active_span_count, 1);
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
        let budgets = EvaluationBudgets {
            work: WorkResourceBudget {
                max_work_units: Some(0),
            },
            ..EvaluationBudgets::default()
        };
        let mut engine = formula_engine(mode, budgets);
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
fn explicit_vertex_budget_ignores_legacy_limit_at_shared_demotion_seam() {
    fn demote(budgets: EvaluationBudgets) -> Result<(), String> {
        let mut config = EvalConfig::default()
            .with_formula_plane_mode(FormulaPlaneMode::AuthoritativeExperimental)
            .with_evaluation_budgets(budgets);
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

    let explicit = EvaluationBudgets {
        admission: AdmissionResourceBudget {
            graph_vertex_hard_limit: Some(1_000_000),
            ..AdmissionResourceBudget::default()
        },
        ..EvaluationBudgets::default()
    };
    demote(explicit).expect("the explicit vertex budget wins over the legacy field");

    let error = demote(EvaluationBudgets::default()).unwrap_err();
    assert!(error.contains("resource") || error.contains("vertex"));
}

#[test]
fn logged_resource_failures_close_groups_and_keep_dirty_retryable() {
    use crate::engine::graph::editor::change_log::ChangeEvent;

    let work_limited = EvaluationBudgets {
        work: WorkResourceBudget {
            max_work_units: Some(1),
        },
        ..EvaluationBudgets::default()
    };
    let mut engine = Engine::new(
        TestWorkbook::default(),
        EvalConfig::default().with_evaluation_budgets(work_limited),
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

    engine.set_evaluation_budgets_for_test(EvaluationBudgets::default());
    engine.evaluate_all_logged(&mut log).unwrap();
    assert_eq!(
        engine.get_cell_value("Sheet1", 1, 3),
        Some(LiteralValue::Number(2.0))
    );
    assert_eq!(log.compound_depth(), 0);

    let deadline_limited = EvaluationBudgets {
        deadline: DeadlineResourceBudget {
            max_elapsed: Some(Duration::ZERO),
        },
        ..EvaluationBudgets::default()
    };
    let mut engine = formula_engine(FormulaPlaneMode::Off, deadline_limited);
    let mut log = ChangeLog::new();
    let error = engine.evaluate_all_logged(&mut log).unwrap_err();
    assert_eq!(
        resource_reason(&error),
        Some(ResourceExhaustionReason::Deadline)
    );
    assert_eq!(log.compound_depth(), 0);
    engine.set_evaluation_budgets_for_test(EvaluationBudgets::default());
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
